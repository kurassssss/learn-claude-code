"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║           NEXUS PRIME  –  SWING TRADING ENGINE  v3.0                           ║
║           Moduł średnioterminowy · Dźwignia x10 · Multi-Exchange               ║
║           High-Confidence Only · Ensemble Signal Architecture                  ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Architektura:
  ┌─────────────────────────────────────────────────────────────────┐
  │  DataFetcher  →  FeatureBuilder  →  EnsembleScorer              │
  │       ↓                                                          │
  │  ConfidenceGate (≥0.87)  →  PositionSizer  →  TradeExecutor    │
  │       ↓                                                          │
  │  RiskManager (SL/TP/Trailing)  →  PortfolioGuard               │
  └─────────────────────────────────────────────────────────────────┘

Warstwy sygnałów:
  1. Trend          – EMA stack, ADX, Ichimoku Cloud
  2. Momentum       – RSI multi-TF, MACD histogram, Stoch-RSI, Williams %R
  3. Volume         – OBV slope, VWAP deviation, CVD delta
  4. Market Struct  – HH/HL detekcja, Order Blocks, Fair Value Gaps
  5. Volatility     – ATR regime, Bollinger squeeze, Keltner breakout
  6. Funding/OI     – Funding rate bias, Open Interest delta
  7. Cross-TF Agree – Spójność sygnałów 1h / 4h / 1d

Giełdy: Binance Futures, Bybit, OKX, Kraken Futures, BitMEX
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np

# ccxt.async_support wymagane: pip install ccxt numpy pandas ta-lib
try:
    import ccxt.async_support as ccxt_async
    import ccxt
except ImportError:
    raise ImportError("Zainstaluj: pip install ccxt numpy")

# ──────────────────────────────────────────────────────────────────────────────
# KONFIGURACJA GLOBALNA
# ──────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # ── Giełdy ──────────────────────────────────────────────────────────────
    "exchanges": {
        "binance": {
            "api_key": "",
            "secret": "",
            "options": {"defaultType": "future"},
            "sandbox": False,
        },
        "bybit": {
            "api_key": "",
            "secret": "",
            "options": {"defaultType": "linear"},
            "sandbox": False,
        },
        "okx": {
            "api_key": "",
            "secret": "",
            "password": "",          # OKX wymaga passphrase
            "sandbox": False,
        },
        "kraken": {
            "api_key": "",
            "secret": "",
            "sandbox": False,
        },
        "bitmex": {
            "api_key": "",
            "secret": "",
            "sandbox": True,         # BitMEX – zaczynamy sandbox
        },
    },

    # ── Trading ─────────────────────────────────────────────────────────────
    "leverage": 10,                     # Dźwignia x10
    "capital_min_pct": 0.10,           # Min 10% kapitału na trade
    "capital_max_pct": 0.25,           # Max 25% kapitału na trade
    "confidence_threshold": 0.87,      # Minimalne confidence do wejścia
    "confidence_max": 0.97,            # Powyżej tego – max pozycja
    "max_concurrent_positions": 6,     # Maks. równoległych pozycji
    "max_positions_per_exchange": 2,   # Maks. na jedną giełdę

    # ── Timeframes ───────────────────────────────────────────────────────────
    "primary_tf": "4h",                # Główny timeframe
    "confirmation_tf": "1d",          # Potwierdzenie trendu
    "entry_tf": "1h",                  # Timeframe wejścia

    # ── Risk Management ──────────────────────────────────────────────────────
    "sl_atr_multiplier": 1.8,          # SL = entry - 1.8 * ATR(14)
    "tp_rr_ratio": 2.8,               # TP = entry + SL_dist * 2.8
    "trailing_activation_rr": 1.5,    # Trailing stop aktywny po RR 1.5
    "trailing_atr_multiplier": 1.2,   # Trailing = highest - 1.2 * ATR
    "max_daily_loss_pct": 0.04,       # Max dzienna strata 4% kapitału
    "max_drawdown_pct": 0.12,         # Circuit breaker przy 12% DD

    # ── Symbole ─────────────────────────────────────────────────────────────
    "watchlist": [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "BNB/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
        "ARB/USDT:USDT", "OP/USDT:USDT",  "MATIC/USDT:USDT",
        "DOGE/USDT:USDT","SUI/USDT:USDT",  "INJ/USDT:USDT",
    ],

    # ── System ───────────────────────────────────────────────────────────────
    "scan_interval_sec": 900,          # Skan co 15 minut
    "db_path": "nexus_swing.db",
    "log_level": "INFO",
    "dry_run": True,                   # USTAW False do live tradingu!
}

# ──────────────────────────────────────────────────────────────────────────────
# LOGGER
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, CONFIG["log_level"]),
    format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("NexusSwing")


# ──────────────────────────────────────────────────────────────────────────────
# TYPY / ENUMERACJE
# ──────────────────────────────────────────────────────────────────────────────

class SignalDirection(Enum):
    LONG  = auto()
    SHORT = auto()
    FLAT  = auto()

class PositionState(Enum):
    OPEN      = "open"
    TRAILING  = "trailing"
    CLOSED    = "closed"
    CANCELLED = "cancelled"

@dataclass
class OHLCV:
    """Znormalizowana świeca."""
    timestamp: int          # ms
    open:  float
    high:  float
    low:   float
    close: float
    volume: float

@dataclass
class LayerScore:
    """Wynik jednej warstwy sygnałowej (0.0 – 1.0)."""
    name:      str
    score:     float          # 0.0 = bearish, 0.5 = neutral, 1.0 = bullish
    weight:    float          # waga w ensemble
    direction: SignalDirection
    meta:      Dict[str, Any] = field(default_factory=dict)

@dataclass
class TradeSignal:
    """Gotowy sygnał wejścia."""
    exchange:    str
    symbol:      str
    direction:   SignalDirection
    confidence:  float            # 0.0 – 1.0
    entry_price: float
    sl_price:    float
    tp_price:    float
    atr:         float
    capital_pct: float            # ile % kapitału alokować
    layers:      List[LayerScore] = field(default_factory=list)
    timestamp:   int = field(default_factory=lambda: int(time.time() * 1000))

@dataclass
class Position:
    """Aktywna pozycja."""
    id:             str
    exchange:       str
    symbol:         str
    direction:      SignalDirection
    entry_price:    float
    sl_price:       float
    tp_price:       float
    trailing_stop:  Optional[float]
    size_contracts: float
    leverage:       int
    capital_used:   float
    state:          PositionState = PositionState.OPEN
    highest_price:  float = 0.0
    lowest_price:   float = float("inf")
    pnl_usd:        float = 0.0
    open_time:      int   = field(default_factory=lambda: int(time.time() * 1000))
    close_time:     Optional[int] = None
    close_price:    Optional[float] = None
    close_reason:   Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# WSKAŹNIKI TECHNICZNE (czyste numpy – bez zewnętrznych zależności)
# ──────────────────────────────────────────────────────────────────────────────

class Indicators:
    """Wszystkie wskaźniki techniczne implementowane na czystych tablicach numpy."""

    @staticmethod
    def ema(arr: np.ndarray, period: int) -> np.ndarray:
        """Exponential Moving Average."""
        out = np.full_like(arr, np.nan)
        if len(arr) < period:
            return out
        alpha = 2.0 / (period + 1)
        out[period - 1] = arr[:period].mean()
        for i in range(period, len(arr)):
            out[i] = arr[i] * alpha + out[i - 1] * (1 - alpha)
        return out

    @staticmethod
    def sma(arr: np.ndarray, period: int) -> np.ndarray:
        out = np.full_like(arr, np.nan)
        for i in range(period - 1, len(arr)):
            out[i] = arr[i - period + 1:i + 1].mean()
        return out

    @staticmethod
    def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
            period: int = 14) -> np.ndarray:
        """Average True Range."""
        n = len(high)
        tr = np.zeros(n)
        tr[0] = high[0] - low[0]
        for i in range(1, n):
            tr[i] = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i]  - close[i - 1]),
            )
        return Indicators.ema(tr, period)

    @staticmethod
    def rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
        """Relative Strength Index."""
        delta = np.diff(close)
        gains = np.where(delta > 0, delta, 0.0)
        losses = np.where(delta < 0, -delta, 0.0)
        avg_g = Indicators.ema(gains, period)
        avg_l = Indicators.ema(losses, period)
        rs = np.where(avg_l == 0, np.inf, avg_g / avg_l)
        rsi_arr = 100 - (100 / (1 + rs))
        return np.concatenate([[np.nan], rsi_arr])

    @staticmethod
    def macd(close: np.ndarray,
             fast: int = 12, slow: int = 26, signal: int = 9
             ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """MACD line, Signal line, Histogram."""
        ema_fast = Indicators.ema(close, fast)
        ema_slow = Indicators.ema(close, slow)
        macd_line = ema_fast - ema_slow
        sig_line  = Indicators.ema(macd_line, signal)
        histogram = macd_line - sig_line
        return macd_line, sig_line, histogram

    @staticmethod
    def stoch_rsi(close: np.ndarray, rsi_p: int = 14,
                  stoch_p: int = 14, smooth_k: int = 3,
                  smooth_d: int = 3) -> Tuple[np.ndarray, np.ndarray]:
        """Stochastic RSI."""
        rsi_vals = Indicators.rsi(close, rsi_p)
        k_arr = np.full_like(rsi_vals, np.nan)
        for i in range(stoch_p - 1, len(rsi_vals)):
            window = rsi_vals[i - stoch_p + 1: i + 1]
            lo, hi = np.nanmin(window), np.nanmax(window)
            k_arr[i] = (rsi_vals[i] - lo) / (hi - lo) * 100 if hi != lo else 50.0
        k_smooth = Indicators.sma(np.nan_to_num(k_arr), smooth_k)
        d_smooth = Indicators.sma(k_smooth, smooth_d)
        return k_smooth, d_smooth

    @staticmethod
    def bollinger(close: np.ndarray, period: int = 20,
                  std_mul: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Bollinger Bands: upper, mid, lower."""
        mid = Indicators.sma(close, period)
        std = np.full_like(close, np.nan)
        for i in range(period - 1, len(close)):
            std[i] = close[i - period + 1:i + 1].std()
        upper = mid + std_mul * std
        lower = mid - std_mul * std
        return upper, mid, lower

    @staticmethod
    def keltner(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                period: int = 20, atr_mul: float = 1.5
                ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Keltner Channels."""
        mid  = Indicators.ema(close, period)
        atr_ = Indicators.atr(high, low, close, period)
        return mid + atr_mul * atr_, mid, mid - atr_mul * atr_

    @staticmethod
    def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray,
            period: int = 14) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ADX + DI+ + DI-."""
        n = len(high)
        dm_plus  = np.zeros(n)
        dm_minus = np.zeros(n)
        tr_arr   = np.zeros(n)
        for i in range(1, n):
            h_diff = high[i]  - high[i - 1]
            l_diff = low[i - 1] - low[i]
            dm_plus[i]  = h_diff if h_diff > l_diff and h_diff > 0 else 0
            dm_minus[i] = l_diff if l_diff > h_diff and l_diff > 0 else 0
            tr_arr[i] = max(
                high[i] - low[i],
                abs(high[i]  - close[i - 1]),
                abs(low[i]   - close[i - 1]),
            )
        atr14   = Indicators.ema(tr_arr, period)
        di_plus  = 100 * Indicators.ema(dm_plus, period)  / (atr14 + 1e-10)
        di_minus = 100 * Indicators.ema(dm_minus, period) / (atr14 + 1e-10)
        dx = 100 * np.abs(di_plus - di_minus) / (di_plus + di_minus + 1e-10)
        adx_line = Indicators.ema(dx, period)
        return adx_line, di_plus, di_minus

    @staticmethod
    def williams_r(high: np.ndarray, low: np.ndarray,
                   close: np.ndarray, period: int = 14) -> np.ndarray:
        """Williams %R."""
        out = np.full_like(close, np.nan)
        for i in range(period - 1, len(close)):
            hh = high[i - period + 1:i + 1].max()
            ll = low[i  - period + 1:i + 1].min()
            out[i] = -100 * (hh - close[i]) / (hh - ll + 1e-10)
        return out

    @staticmethod
    def obv(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        """On-Balance Volume."""
        out = np.zeros(len(close))
        for i in range(1, len(close)):
            if close[i] > close[i - 1]:
                out[i] = out[i - 1] + volume[i]
            elif close[i] < close[i - 1]:
                out[i] = out[i - 1] - volume[i]
            else:
                out[i] = out[i - 1]
        return out

    @staticmethod
    def vwap_deviation(high: np.ndarray, low: np.ndarray,
                       close: np.ndarray, volume: np.ndarray,
                       period: int = 20) -> np.ndarray:
        """Odchylenie od VWAP (rolling)."""
        typical = (high + low + close) / 3
        out = np.full_like(close, np.nan)
        for i in range(period - 1, len(close)):
            tp_w = typical[i - period + 1:i + 1]
            vol  = volume[i  - period + 1:i + 1]
            vwap = (tp_w * vol).sum() / (vol.sum() + 1e-10)
            out[i] = (close[i] - vwap) / (vwap + 1e-10) * 100
        return out

    @staticmethod
    def ichimoku(high: np.ndarray, low: np.ndarray,
                 tenkan_p: int = 9, kijun_p: int = 26,
                 senkou_b_p: int = 52) -> Dict[str, np.ndarray]:
        """Ichimoku Cloud."""
        def midpoint(arr_h, arr_l, p):
            out = np.full(len(arr_h), np.nan)
            for i in range(p - 1, len(arr_h)):
                out[i] = (arr_h[i-p+1:i+1].max() + arr_l[i-p+1:i+1].min()) / 2
            return out

        tenkan  = midpoint(high, low, tenkan_p)
        kijun   = midpoint(high, low, kijun_p)
        senkou_a = (tenkan + kijun) / 2
        senkou_b = midpoint(high, low, senkou_b_p)
        chikou  = np.concatenate([high[kijun_p:], np.full(kijun_p, np.nan)])
        return {
            "tenkan":   tenkan,
            "kijun":    kijun,
            "senkou_a": senkou_a,
            "senkou_b": senkou_b,
            "chikou":   chikou,
        }

    @staticmethod
    def detect_swing_structure(high: np.ndarray, low: np.ndarray,
                                lookback: int = 5) -> Dict[str, bool]:
        """
        Detekcja struktury rynkowej: Higher Highs / Higher Lows / BOS.
        Zwraca słownik z flagami bullish/bearish struktury.
        """
        if len(high) < lookback * 3:
            return {"hh_hl": False, "lh_ll": False, "bos_up": False, "bos_dn": False}

        def pivot_highs(arr, lb):
            pivots = []
            for i in range(lb, len(arr) - lb):
                if arr[i] == arr[i - lb:i + lb + 1].max():
                    pivots.append((i, arr[i]))
            return pivots

        def pivot_lows(arr, lb):
            pivots = []
            for i in range(lb, len(arr) - lb):
                if arr[i] == arr[i - lb:i + lb + 1].min():
                    pivots.append((i, arr[i]))
            return pivots

        ph = pivot_highs(high, lookback)
        pl = pivot_lows(low, lookback)

        hh_hl = False
        lh_ll = False
        bos_up = False
        bos_dn = False

        if len(ph) >= 2:
            hh_hl = ph[-1][1] > ph[-2][1]
            lh_ll = ph[-1][1] < ph[-2][1]
        if len(pl) >= 2:
            hh_hl = hh_hl and pl[-1][1] > pl[-2][1]
            lh_ll = lh_ll and pl[-1][1] < pl[-2][1]

        # Break of Structure
        if len(ph) >= 1 and high[-1] > ph[-1][1]:
            bos_up = True
        if len(pl) >= 1 and low[-1] < pl[-1][1]:
            bos_dn = True

        return {"hh_hl": hh_hl, "lh_ll": lh_ll, "bos_up": bos_up, "bos_dn": bos_dn}

    @staticmethod
    def detect_fair_value_gaps(high: np.ndarray, low: np.ndarray,
                                close: np.ndarray, lookback: int = 50
                                ) -> Tuple[List[Tuple], List[Tuple]]:
        """
        Fair Value Gaps (FVG) – bullish i bearish.
        Bullish FVG: low[i+1] > high[i-1] (luka w górę)
        Bearish FVG: high[i+1] < low[i-1] (luka w dół)
        """
        bullish_fvg = []
        bearish_fvg = []
        start = max(1, len(high) - lookback)
        for i in range(start, len(high) - 1):
            if low[i + 1] > high[i - 1]:        # bullish gap
                bullish_fvg.append((i, high[i - 1], low[i + 1]))
            if high[i + 1] < low[i - 1]:        # bearish gap
                bearish_fvg.append((i, high[i - 1], low[i + 1]))
        return bullish_fvg, bearish_fvg


# ──────────────────────────────────────────────────────────────────────────────
# WARSTWY SYGNAŁÓW
# ──────────────────────────────────────────────────────────────────────────────

class SignalLayers:
    """
    Siedem niezależnych warstw analizy rynku.
    Każda zwraca LayerScore z oceną 0.0 (silnie bearish) do 1.0 (silnie bullish).
    """

    I = Indicators()    # alias

    # ── 1. Trend ──────────────────────────────────────────────────────────────
    @staticmethod
    def trend_layer(candles: List[OHLCV]) -> LayerScore:
        """
        EMA 20/50/200 stack + ADX + Ichimoku – ocena siły i kierunku trendu.
        """
        c = np.array([x.close  for x in candles])
        h = np.array([x.high   for x in candles])
        l = np.array([x.low    for x in candles])
        i = Indicators

        ema20  = i.ema(c, 20)
        ema50  = i.ema(c, 50)
        ema200 = i.ema(c, 200)
        adx, di_p, di_m = i.adx(h, l, c)
        ichi = i.ichimoku(h, l)

        score_parts = []
        meta = {}

        # EMA stack: 20 > 50 > 200 = bullish
        if not np.isnan(ema200[-1]):
            stack_bull = (ema20[-1] > ema50[-1] > ema200[-1])
            stack_bear = (ema20[-1] < ema50[-1] < ema200[-1])
            score_parts.append(1.0 if stack_bull else (0.0 if stack_bear else 0.5))
            meta["ema_stack_bull"] = bool(stack_bull)

        # Cena powyżej EMA200
        if not np.isnan(ema200[-1]):
            above_200 = c[-1] > ema200[-1]
            score_parts.append(0.85 if above_200 else 0.15)
            meta["above_ema200"] = bool(above_200)

        # ADX siła trendu + kierunek
        if not np.isnan(adx[-1]):
            trend_strong = adx[-1] > 25
            bull_adx = di_p[-1] > di_m[-1]
            if trend_strong:
                score_parts.append(0.9 if bull_adx else 0.1)
            else:
                score_parts.append(0.5)   # Słaby trend = neutralny
            meta["adx"] = round(adx[-1], 2)
            meta["di_plus"] = round(di_p[-1], 2)
            meta["di_minus"] = round(di_m[-1], 2)

        # Ichimoku: cena ponad chmurą
        if not np.isnan(ichi["senkou_a"][-1]) and not np.isnan(ichi["senkou_b"][-1]):
            cloud_top = max(ichi["senkou_a"][-1], ichi["senkou_b"][-1])
            cloud_bot = min(ichi["senkou_a"][-1], ichi["senkou_b"][-1])
            above_cloud = c[-1] > cloud_top
            below_cloud = c[-1] < cloud_bot
            score_parts.append(1.0 if above_cloud else (0.0 if below_cloud else 0.5))
            meta["above_cloud"] = bool(above_cloud)

        # Tenkan powyżej Kijun
        if not np.isnan(ichi["tenkan"][-1]) and not np.isnan(ichi["kijun"][-1]):
            tk_bull = ichi["tenkan"][-1] > ichi["kijun"][-1]
            score_parts.append(0.8 if tk_bull else 0.2)

        if not score_parts:
            return LayerScore("TREND", 0.5, 0.20, SignalDirection.FLAT, meta)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("TREND", score, 0.22, direction, meta)

    # ── 2. Momentum ───────────────────────────────────────────────────────────
    @staticmethod
    def momentum_layer(candles: List[OHLCV]) -> LayerScore:
        """
        RSI + MACD + Stoch-RSI + Williams %R – pęd cenowy.
        """
        c = np.array([x.close for x in candles])
        h = np.array([x.high  for x in candles])
        l = np.array([x.low   for x in candles])
        i = Indicators

        rsi14 = i.rsi(c, 14)
        _, _, macd_hist = i.macd(c)
        k_stoch, d_stoch = i.stoch_rsi(c)
        wpr = i.williams_r(h, l, c, 14)

        score_parts = []
        meta = {}

        # RSI – unikamy skrajności; ideał to 55-75 dla bullish
        if not np.isnan(rsi14[-1]):
            r = rsi14[-1]
            meta["rsi"] = round(r, 2)
            if 55 <= r <= 75:
                score_parts.append(0.90)
            elif 45 <= r < 55:
                score_parts.append(0.55)
            elif r > 75:
                score_parts.append(0.35)   # Wykupienie – ryzyko
            elif 30 <= r < 45:
                score_parts.append(0.30)
            else:                           # Wyprzedanie – okazja dla bull
                score_parts.append(0.65)

        # MACD histogram rosnący i powyżej zera
        if not np.isnan(macd_hist[-1]) and not np.isnan(macd_hist[-2]):
            hist_pos    = macd_hist[-1] > 0
            hist_rising = macd_hist[-1] > macd_hist[-2]
            if hist_pos and hist_rising:
                score_parts.append(0.95)
            elif hist_pos:
                score_parts.append(0.70)
            elif not hist_pos and hist_rising:
                score_parts.append(0.40)   # Negatywny ale rośnie
            else:
                score_parts.append(0.10)
            meta["macd_hist"] = round(float(macd_hist[-1]), 6)

        # Stoch-RSI – k > d i k rosnące
        if not np.isnan(k_stoch[-1]) and not np.isnan(d_stoch[-1]):
            k, d = k_stoch[-1], d_stoch[-1]
            cross_bull = k > d and k_stoch[-2] <= d_stoch[-2]
            if cross_bull:
                score_parts.append(0.95)
            elif k > d:
                score_parts.append(0.75 if k < 80 else 0.45)
            else:
                score_parts.append(0.25)
            meta["stoch_k"] = round(float(k), 2)

        # Williams %R – bullish zone (-20 to -50)
        if not np.isnan(wpr[-1]):
            w = wpr[-1]
            meta["williams_r"] = round(float(w), 2)
            if -50 <= w <= -20:
                score_parts.append(0.85)
            elif w > -20:
                score_parts.append(0.30)   # Wykupienie
            elif w < -80:
                score_parts.append(0.70)   # Wyprzedanie = potencjalna reversal
            else:
                score_parts.append(0.50)

        if not score_parts:
            return LayerScore("MOMENTUM", 0.5, 0.20, SignalDirection.FLAT, meta)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("MOMENTUM", score, 0.20, direction, meta)

    # ── 3. Volume ─────────────────────────────────────────────────────────────
    @staticmethod
    def volume_layer(candles: List[OHLCV]) -> LayerScore:
        """
        OBV slope + VWAP deviation + Volume spike analysis.
        """
        c = np.array([x.close  for x in candles])
        h = np.array([x.high   for x in candles])
        l = np.array([x.low    for x in candles])
        v = np.array([x.volume for x in candles])
        i = Indicators

        obv_arr  = i.obv(c, v)
        vwap_dev = i.vwap_deviation(h, l, c, v, 20)

        score_parts = []
        meta = {}

        # OBV slope – rosnące OBV przy rosnącej cenie = potwierdzenie
        if len(obv_arr) >= 10:
            obv_slope = (obv_arr[-1] - obv_arr[-10]) / (abs(obv_arr[-10]) + 1e-10)
            price_slope = (c[-1] - c[-10]) / (c[-10] + 1e-10)
            # Dywergencja
            converge = (obv_slope > 0 and price_slope > 0) or \
                       (obv_slope < 0 and price_slope < 0)
            if converge and obv_slope > 0:
                score_parts.append(0.90)   # Bullish potwierdzenie
            elif converge and obv_slope < 0:
                score_parts.append(0.10)   # Bearish potwierdzenie
            elif obv_slope > 0 and price_slope < 0:
                score_parts.append(0.80)   # Bullish dywergencja OBV
            else:
                score_parts.append(0.25)   # Bearish dywergencja
            meta["obv_slope"] = round(float(obv_slope), 4)
            meta["price_slope_10"] = round(float(price_slope * 100), 2)

        # VWAP deviation: lekko powyżej VWAP = bullish momentum
        if not np.isnan(vwap_dev[-1]):
            dev = vwap_dev[-1]
            meta["vwap_dev_pct"] = round(float(dev), 3)
            if 0.3 <= dev <= 3.0:
                score_parts.append(0.85)   # Bullish ale nie za bardzo
            elif dev > 3.0:
                score_parts.append(0.40)   # Za bardzo powyżej VWAP
            elif -1.0 <= dev < 0.3:
                score_parts.append(0.55)   # Neutralne
            else:
                score_parts.append(0.30)   # Poniżej VWAP

        # Volume spike: ostatnia świeca vs średnia 20
        if len(v) >= 20:
            avg_vol = v[-21:-1].mean()
            vol_ratio = v[-1] / (avg_vol + 1e-10)
            meta["vol_ratio"] = round(float(vol_ratio), 2)
            if vol_ratio > 2.0 and c[-1] > c[-2]:
                score_parts.append(0.95)   # Duży wolumen + wzrost ceny
            elif vol_ratio > 1.5:
                score_parts.append(0.70)
            elif vol_ratio < 0.5:
                score_parts.append(0.45)   # Niski wolumen = brak przekonania
            else:
                score_parts.append(0.55)

        if not score_parts:
            return LayerScore("VOLUME", 0.5, 0.15, SignalDirection.FLAT, meta)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("VOLUME", score, 0.15, direction, meta)

    # ── 4. Struktura rynkowa ──────────────────────────────────────────────────
    @staticmethod
    def market_structure_layer(candles: List[OHLCV]) -> LayerScore:
        """
        Higher Highs/Lows + Break of Structure + Fair Value Gaps.
        """
        h = np.array([x.high  for x in candles])
        l = np.array([x.low   for x in candles])
        c = np.array([x.close for x in candles])

        struct = Indicators.detect_swing_structure(h, l)
        bull_fvg, bear_fvg = Indicators.detect_fair_value_gaps(h, l, c)

        score_parts = []
        meta = {}

        # HH + HL = bullish struktury
        if struct["hh_hl"]:
            score_parts.append(0.95)
            meta["structure"] = "HH+HL (bullish)"
        elif struct["lh_ll"]:
            score_parts.append(0.05)
            meta["structure"] = "LH+LL (bearish)"
        else:
            score_parts.append(0.50)
            meta["structure"] = "Mixed"

        # Break of Structure w górę
        if struct["bos_up"]:
            score_parts.append(0.90)
        elif struct["bos_dn"]:
            score_parts.append(0.10)
        else:
            score_parts.append(0.50)

        # FVG: niedawne bullish FVG świadczą o presji kupujących
        recent_bull_fvg = [f for f in bull_fvg if f[0] > len(c) - 20]
        recent_bear_fvg = [f for f in bear_fvg if f[0] > len(c) - 20]
        meta["recent_bull_fvg"] = len(recent_bull_fvg)
        meta["recent_bear_fvg"] = len(recent_bear_fvg)
        if recent_bull_fvg and not recent_bear_fvg:
            score_parts.append(0.80)
        elif recent_bear_fvg and not recent_bull_fvg:
            score_parts.append(0.20)
        else:
            score_parts.append(0.50)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("MARKET_STRUCT", score, 0.18, direction, meta)

    # ── 5. Volatility Regime ──────────────────────────────────────────────────
    @staticmethod
    def volatility_layer(candles: List[OHLCV]) -> LayerScore:
        """
        ATR regime + Bollinger squeeze release + Keltner breakout.
        Szukamy momentu ekspansji zmienności po kompresji.
        """
        c = np.array([x.close for x in candles])
        h = np.array([x.high  for x in candles])
        l = np.array([x.low   for x in candles])
        i = Indicators

        atr14  = i.atr(h, l, c, 14)
        atr_ma = i.ema(atr14, 50)

        bb_up, bb_mid, bb_lo = i.bollinger(c, 20)
        kc_up, kc_mid, kc_lo = i.keltner(h, l, c, 20)

        score_parts = []
        meta = {}

        # ATR względem średniej – rosnący = ekspansja
        if not np.isnan(atr14[-1]) and not np.isnan(atr_ma[-1]):
            atr_ratio = atr14[-1] / (atr_ma[-1] + 1e-10)
            meta["atr_ratio"] = round(float(atr_ratio), 3)
            if 1.0 <= atr_ratio <= 2.0:
                score_parts.append(0.80)  # Zdrowa ekspansja
            elif atr_ratio > 2.0:
                score_parts.append(0.50)  # Zbyt wysoka zmienność – ryzyko
            elif atr_ratio < 0.6:
                score_parts.append(0.65)  # Kompresja – możliwe przebudzenie
            else:
                score_parts.append(0.55)

        # Bollinger squeeze: BB wewnątrz KC = kompresja → potencjał wybuchu
        if not np.isnan(bb_up[-1]) and not np.isnan(kc_up[-1]):
            squeeze = (bb_up[-1] < kc_up[-1]) and (bb_lo[-1] > kc_lo[-1])
            # Squeeze release w górę
            prev_squeeze = (bb_up[-2] < kc_up[-2]) and (bb_lo[-2] > kc_lo[-2])
            if prev_squeeze and not squeeze:     # Właśnie uwolniony
                direction_up = c[-1] > bb_mid[-1]
                score_parts.append(0.92 if direction_up else 0.08)
                meta["squeeze_release"] = "UP" if direction_up else "DOWN"
            elif squeeze:
                score_parts.append(0.60)         # W ścisku – czekamy
                meta["squeeze"] = True
            else:
                score_parts.append(0.55)

        # Cena powyżej górnego KC = silny breakout
        if not np.isnan(kc_up[-1]):
            if c[-1] > kc_up[-1]:
                score_parts.append(0.85)
                meta["keltner_breakout"] = "UP"
            elif c[-1] < kc_lo[-1]:
                score_parts.append(0.15)
                meta["keltner_breakout"] = "DOWN"
            else:
                score_parts.append(0.55)

        if not score_parts:
            return LayerScore("VOLATILITY", 0.5, 0.10, SignalDirection.FLAT, meta)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("VOLATILITY", score, 0.10, direction, meta)

    # ── 6. Funding / Open Interest ────────────────────────────────────────────
    @staticmethod
    def funding_layer(funding_rate: float, oi_delta_pct: float) -> LayerScore:
        """
        Funding rate + OI delta – sentyment rynku futures.
        Negatywny funding przy rosnącym OI = strong bullish signal.
        """
        score_parts = []
        meta = {
            "funding_rate_pct": round(funding_rate * 100, 5),
            "oi_delta_pct": round(oi_delta_pct, 3),
        }

        # Funding rate: ujemny = short side płaci = bullish bias
        if funding_rate < -0.0003:
            score_parts.append(0.95)   # Bardzo bullish
        elif funding_rate < 0:
            score_parts.append(0.75)
        elif funding_rate < 0.0003:
            score_parts.append(0.55)   # Neutralne
        elif funding_rate < 0.001:
            score_parts.append(0.40)   # Lekko bearish
        else:
            score_parts.append(0.15)   # Zbyt wysoki funding = crowded longs

        # OI delta: rosnące OI + wzrost ceny = nowe pieniądze w longach
        if oi_delta_pct > 3.0:
            score_parts.append(0.85)
        elif oi_delta_pct > 0.5:
            score_parts.append(0.65)
        elif oi_delta_pct > -0.5:
            score_parts.append(0.50)
        elif oi_delta_pct < -3.0:
            score_parts.append(0.20)   # Masowe zamykanie
        else:
            score_parts.append(0.35)

        score = float(np.mean(score_parts))
        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("FUNDING_OI", score, 0.08, direction, meta)

    # ── 7. Cross-Timeframe Agreement ─────────────────────────────────────────
    @staticmethod
    def cross_tf_layer(
        scores_1h: float,
        scores_4h: float,
        scores_1d: float,
    ) -> LayerScore:
        """
        Spójność sygnałów między timeframe'ami.
        Wszystkie trzy zgodnie bullish = maksymalna pewność.
        """
        scores = [scores_1h, scores_4h, scores_1d]
        bull = sum(1 for s in scores if s > 0.60)
        bear = sum(1 for s in scores if s < 0.40)
        meta = {
            "score_1h": round(scores_1h, 3),
            "score_4h": round(scores_4h, 3),
            "score_1d": round(scores_1d, 3),
        }

        # Wagujemy wyższe TF bardziej
        weighted = scores_1h * 0.25 + scores_4h * 0.35 + scores_1d * 0.40

        # Bonus za pełną zgodność
        if bull == 3:
            score = min(0.98, weighted * 1.12)
            meta["agreement"] = "FULL_BULL"
        elif bear == 3:
            score = max(0.02, weighted * 0.88)
            meta["agreement"] = "FULL_BEAR"
        elif bull == 2:
            score = weighted
            meta["agreement"] = "MAJORITY_BULL"
        elif bear == 2:
            score = weighted
            meta["agreement"] = "MAJORITY_BEAR"
        else:
            score = 0.50
            meta["agreement"] = "MIXED"

        direction = (SignalDirection.LONG if score > 0.55
                     else SignalDirection.SHORT if score < 0.45
                     else SignalDirection.FLAT)
        return LayerScore("CROSS_TF", score, 0.07, direction, meta)


# ──────────────────────────────────────────────────────────────────────────────
# ENSEMBLE SCORER
# ──────────────────────────────────────────────────────────────────────────────

class EnsembleScorer:
    """
    Łączy wszystkie warstwy sygnałów w jeden wynik confidence.
    
    Confidence = średnia ważona wyników warstw.
    Dodatkowe filtry: consensus check, minimum agreement check.
    """

    @staticmethod
    def compute(layers: List[LayerScore]) -> Tuple[float, SignalDirection]:
        """
        Zwraca (confidence: float, direction: SignalDirection).
        
        Jeśli warstwy nie zgadzają się co do kierunku → FLAT.
        """
        if not layers:
            return 0.5, SignalDirection.FLAT

        total_weight = sum(l.weight for l in layers)
        weighted_score = sum(l.score * l.weight for l in layers) / total_weight

        # Policz ile warstw jest bullish / bearish (non-neutral)
        bull_layers = [l for l in layers if l.direction == SignalDirection.LONG]
        bear_layers = [l for l in layers if l.direction == SignalDirection.SHORT]

        # Wymagamy minimum 5 z 7 warstw w tym samym kierunku
        if len(bull_layers) >= 5:
            direction = SignalDirection.LONG
        elif len(bear_layers) >= 5:
            direction = SignalDirection.SHORT
        else:
            direction = SignalDirection.FLAT
            weighted_score = 0.5   # Reset do neutralnego

        # Zbieżność: odległość od 0.5 musi być wystarczająca
        if direction != SignalDirection.FLAT:
            if direction == SignalDirection.LONG and weighted_score < 0.70:
                direction = SignalDirection.FLAT
                weighted_score = 0.5
            elif direction == SignalDirection.SHORT and weighted_score > 0.30:
                direction = SignalDirection.FLAT
                weighted_score = 0.5

        # Normalizuj confidence do [0.5, 1.0] dla LONG / [0.0, 0.5] dla SHORT
        confidence = weighted_score
        return confidence, direction


# ──────────────────────────────────────────────────────────────────────────────
# POSITION SIZER  (Kelly + Fixed Fraction Hybrid)
# ──────────────────────────────────────────────────────────────────────────────

class PositionSizer:
    """
    Oblicza wielkość pozycji na podstawie confidence i dostępnego kapitału.
    
    Logika:
      - Confidence [threshold, max] mapuje liniowo na [capital_min, capital_max]
      - Kelly fraction jako górne ograniczenie bezpieczeństwa
      - Uwzględnia dźwignię przy obliczaniu ryzyka w USD
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg

    def compute_capital_pct(self, confidence: float) -> float:
        """Liniowe mapowanie confidence → alokacja kapitału."""
        lo_c  = self.cfg["confidence_threshold"]
        hi_c  = self.cfg["confidence_max"]
        lo_k  = self.cfg["capital_min_pct"]
        hi_k  = self.cfg["capital_max_pct"]

        if confidence <= lo_c:
            return 0.0
        if confidence >= hi_c:
            return hi_k

        ratio = (confidence - lo_c) / (hi_c - lo_c)
        return lo_k + ratio * (hi_k - lo_k)

    def compute_contracts(
        self,
        capital_usd: float,
        capital_pct: float,
        entry_price: float,
        sl_price: float,
        leverage: int,
        contract_size: float = 1.0,
    ) -> Tuple[float, float, float]:
        """
        Zwraca (n_contracts, allocated_usd, risk_usd).
        
        Risk per trade = strata SL / dźwignia.
        Kontrakt = wartość notional / leverage.
        """
        allocated_usd = capital_usd * capital_pct

        # Odległość SL w %
        sl_dist_pct = abs(entry_price - sl_price) / entry_price

        # Bez dźwigni: ryzyko % = capital * sl_dist
        # Z dźwignią x10: pozycja notional = allocated_usd * leverage
        notional_value = allocated_usd * leverage

        # Ilość kontraktów (dla kontraktów kwotowanych w USDT)
        n_contracts = notional_value / (entry_price * contract_size)

        # Risk w USD = strata gdyby SL trafił
        risk_usd = notional_value * sl_dist_pct

        return n_contracts, allocated_usd, risk_usd

    def kelly_fraction(self, win_rate: float, rr_ratio: float) -> float:
        """
        Kelly Criterion: f* = (p * b - q) / b
        gdzie b = RR ratio, p = win_rate, q = 1 - p
        """
        q = 1.0 - win_rate
        f_star = (win_rate * rr_ratio - q) / rr_ratio
        # Używamy 25% Kelly jako bezpieczną frakcję
        return max(0.0, min(f_star * 0.25, 0.25))


# ──────────────────────────────────────────────────────────────────────────────
# RISK MANAGER
# ──────────────────────────────────────────────────────────────────────────────

class RiskManager:
    """
    Zarządza poziomami SL/TP oraz trailing stop.
    Pilnuje limitu dziennej straty i circuit breakera.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.daily_pnl_usd: float = 0.0
        self.daily_reset_ts: float = time.time()
        self.peak_capital: float = 0.0

    def compute_sl_tp(
        self,
        entry: float,
        atr: float,
        direction: SignalDirection,
    ) -> Tuple[float, float]:
        """ATR-bazowany SL + RR-bazowany TP."""
        sl_dist = atr * self.cfg["sl_atr_multiplier"]
        tp_dist = sl_dist * self.cfg["tp_rr_ratio"]

        if direction == SignalDirection.LONG:
            sl = entry - sl_dist
            tp = entry + tp_dist
        else:
            sl = entry + sl_dist
            tp = entry - tp_dist

        return sl, tp

    def update_trailing_stop(self, pos: Position, current_price: float) -> Position:
        """
        Aktualizuje trailing stop jeśli pozycja jest w zysku.
        Aktywuje się po osiągnięciu trailing_activation_rr.
        """
        if pos.direction == SignalDirection.LONG:
            pos.highest_price = max(pos.highest_price, current_price)
            profit_dist = pos.highest_price - pos.entry_price
            sl_dist     = pos.entry_price - pos.sl_price

            if profit_dist >= sl_dist * self.cfg["trailing_activation_rr"]:
                atr = (pos.entry_price - pos.sl_price) / self.cfg["sl_atr_multiplier"]
                new_trail = pos.highest_price - atr * self.cfg["trailing_atr_multiplier"]
                if pos.trailing_stop is None or new_trail > pos.trailing_stop:
                    pos.trailing_stop = new_trail
                    pos.state = PositionState.TRAILING
                    log.info(
                        f"[{pos.exchange}] Trailing stop aktywny: "
                        f"{pos.symbol} → {new_trail:.4f}"
                    )
        else:
            pos.lowest_price = min(pos.lowest_price, current_price)
            profit_dist = pos.entry_price - pos.lowest_price
            sl_dist     = pos.sl_price - pos.entry_price

            if profit_dist >= sl_dist * self.cfg["trailing_activation_rr"]:
                atr = (pos.sl_price - pos.entry_price) / self.cfg["sl_atr_multiplier"]
                new_trail = pos.lowest_price + atr * self.cfg["trailing_atr_multiplier"]
                if pos.trailing_stop is None or new_trail < pos.trailing_stop:
                    pos.trailing_stop = new_trail
                    pos.state = PositionState.TRAILING

        return pos

    def should_close(
        self, pos: Position, current_price: float
    ) -> Tuple[bool, str]:
        """Sprawdza czy pozycję należy zamknąć."""
        if pos.direction == SignalDirection.LONG:
            if current_price <= pos.sl_price:
                return True, "SL_HIT"
            if current_price >= pos.tp_price:
                return True, "TP_HIT"
            if pos.trailing_stop and current_price <= pos.trailing_stop:
                return True, "TRAILING_STOP"
        else:
            if current_price >= pos.sl_price:
                return True, "SL_HIT"
            if current_price <= pos.tp_price:
                return True, "TP_HIT"
            if pos.trailing_stop and current_price >= pos.trailing_stop:
                return True, "TRAILING_STOP"
        return False, ""

    def check_daily_risk(self, capital_usd: float) -> bool:
        """
        Zwraca False jeśli dzienny limit straty przekroczony
        lub nastąpił circuit breaker max drawdown.
        """
        now = time.time()
        if now - self.daily_reset_ts > 86400:
            self.daily_pnl_usd = 0.0
            self.daily_reset_ts = now

        daily_loss_limit = capital_usd * self.cfg["max_daily_loss_pct"]
        if self.daily_pnl_usd < -daily_loss_limit:
            log.warning(f"❌ Dzienny limit strat przekroczony: {self.daily_pnl_usd:.2f} USD")
            return False

        if self.peak_capital > 0:
            drawdown = (self.peak_capital - capital_usd) / self.peak_capital
            if drawdown > self.cfg["max_drawdown_pct"]:
                log.error(
                    f"🔴 CIRCUIT BREAKER – Drawdown {drawdown:.1%} "
                    f"przekracza limit {self.cfg['max_drawdown_pct']:.1%}"
                )
                return False

        self.peak_capital = max(self.peak_capital, capital_usd)
        return True

    def register_pnl(self, pnl_usd: float):
        self.daily_pnl_usd += pnl_usd


# ──────────────────────────────────────────────────────────────────────────────
# DATA FETCHER
# ──────────────────────────────────────────────────────────────────────────────

class DataFetcher:
    """
    Pobiera dane OHLCV, funding rates i Open Interest z giełd async.
    Cache z TTL żeby nie przekraczać limitów API.
    """

    def __init__(self, exchange_id: str, exchange_cfg: dict):
        self.exchange_id = exchange_id
        exchange_class = getattr(ccxt_async, exchange_id)
        self.exchange: ccxt_async.Exchange = exchange_class({
            "apiKey":    exchange_cfg.get("api_key", ""),
            "secret":    exchange_cfg.get("secret", ""),
            "password":  exchange_cfg.get("password", ""),
            **exchange_cfg.get("options", {}),
        })
        if exchange_cfg.get("sandbox"):
            self.exchange.set_sandbox_mode(True)

        self._cache: Dict[str, Tuple[float, Any]] = {}  # klucz → (ts, dane)
        self._cache_ttl = 300  # 5 min

    def _cache_key(self, *args) -> str:
        return "|".join(str(a) for a in args)

    def _from_cache(self, key: str) -> Optional[Any]:
        if key in self._cache:
            ts, data = self._cache[key]
            if time.time() - ts < self._cache_ttl:
                return data
        return None

    def _to_cache(self, key: str, data: Any):
        self._cache[key] = (time.time(), data)

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, limit: int = 300
    ) -> List[OHLCV]:
        key = self._cache_key("ohlcv", self.exchange_id, symbol, timeframe)
        cached = self._from_cache(key)
        if cached is not None:
            return cached

        try:
            raw = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            candles = [OHLCV(*row[:6]) for row in raw]
            self._to_cache(key, candles)
            return candles
        except Exception as e:
            log.warning(f"[{self.exchange_id}] OHLCV {symbol}/{timeframe}: {e}")
            return []

    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        key = self._cache_key("ticker", self.exchange_id, symbol)
        cached = self._from_cache(key)
        if cached is not None:
            return cached
        try:
            t = await self.exchange.fetch_ticker(symbol)
            self._to_cache(key, t)
            return t
        except Exception as e:
            log.warning(f"[{self.exchange_id}] Ticker {symbol}: {e}")
            return None

    async def fetch_funding_rate(self, symbol: str) -> float:
        """Zwraca aktualny funding rate (float). 0.0 przy błędzie."""
        try:
            if hasattr(self.exchange, "fetch_funding_rate"):
                fr = await self.exchange.fetch_funding_rate(symbol)
                return float(fr.get("fundingRate", 0.0))
        except Exception:
            pass
        return 0.0

    async def fetch_open_interest_delta(self, symbol: str) -> float:
        """
        Oblicza zmianę OI (%) z ostatnich 24h.
        Zwraca 0.0 jeśli brak danych.
        """
        try:
            if hasattr(self.exchange, "fetch_open_interest_history"):
                history = await self.exchange.fetch_open_interest_history(
                    symbol, "1h", limit=25
                )
                if len(history) >= 2:
                    oi_now  = history[-1].get("openInterestValue", 0)
                    oi_prev = history[-24].get("openInterestValue", 0)
                    if oi_prev:
                        return (oi_now - oi_prev) / oi_prev * 100
        except Exception:
            pass
        return 0.0

    async def fetch_balance(self) -> float:
        """Zwraca dostępny USDT/USD balance futures."""
        try:
            balance = await self.exchange.fetch_balance({"type": "future"})
            for key in ["USDT", "USD", "BUSD"]:
                if key in balance.get("free", {}):
                    return float(balance["free"][key])
        except Exception as e:
            log.warning(f"[{self.exchange_id}] Balance: {e}")
        return 0.0

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Ustawia dźwignię dla symbolu."""
        try:
            await self.exchange.set_leverage(leverage, symbol)
            return True
        except Exception as e:
            log.warning(f"[{self.exchange_id}] Leverage {symbol}: {e}")
            return False

    async def close(self):
        await self.exchange.close()


# ──────────────────────────────────────────────────────────────────────────────
# TRADE EXECUTOR
# ──────────────────────────────────────────────────────────────────────────────

class TradeExecutor:
    """
    Wysyła zlecenia na giełdę z retry logiką i slippage kontrolą.
    W trybie dry_run tylko symuluje.
    """

    def __init__(self, fetcher: DataFetcher, cfg: dict):
        self.fetcher = fetcher
        self.cfg     = cfg
        self.dry_run = cfg["dry_run"]

    async def open_position(
        self, signal: TradeSignal, capital_usd: float
    ) -> Optional[Position]:
        """
        Otwiera pozycję:
        1. Ustaw dźwignię
        2. Wyślij market order
        3. Wyślij SL i TP jako stop orders
        """
        ex_id  = signal.exchange
        symbol = signal.symbol
        side   = "buy" if signal.direction == SignalDirection.LONG else "sell"

        sizer   = PositionSizer(self.cfg)
        n_contracts, allocated_usd, risk_usd = sizer.compute_contracts(
            capital_usd, signal.capital_pct,
            signal.entry_price, signal.sl_price, CONFIG["leverage"]
        )

        log.info(
            f"[{ex_id}] 🟢 SIGNAL {side.upper()} {symbol} | "
            f"confidence={signal.confidence:.3f} | "
            f"size={n_contracts:.4f} | "
            f"entry={signal.entry_price:.4f} | "
            f"SL={signal.sl_price:.4f} | TP={signal.tp_price:.4f} | "
            f"risk={risk_usd:.2f}$"
        )

        pos_id = f"{ex_id}_{symbol}_{int(time.time())}"

        if self.dry_run:
            log.info(f"[DRY RUN] Pozycja SYMULOWANA: {pos_id}")
            return Position(
                id=pos_id,
                exchange=ex_id,
                symbol=symbol,
                direction=signal.direction,
                entry_price=signal.entry_price,
                sl_price=signal.sl_price,
                tp_price=signal.tp_price,
                trailing_stop=None,
                size_contracts=n_contracts,
                leverage=self.cfg["leverage"],
                capital_used=allocated_usd,
                highest_price=signal.entry_price,
                lowest_price=signal.entry_price,
            )

        # ── Live trading ─────────────────────────────────────────────────────
        exchange = self.fetcher.exchange

        # Ustaw dźwignię
        await self.fetcher.set_leverage(symbol, self.cfg["leverage"])

        # Market entry
        for attempt in range(3):
            try:
                order = await exchange.create_order(
                    symbol, "market", side, n_contracts,
                    params={"reduceOnly": False}
                )
                entry_price = float(order.get("average") or order.get("price", signal.entry_price))
                break
            except Exception as e:
                log.warning(f"[{ex_id}] Order attempt {attempt+1}: {e}")
                await asyncio.sleep(2)
        else:
            log.error(f"[{ex_id}] Nie udało się złożyć zlecenia: {symbol}")
            return None

        # SL order
        sl_side = "sell" if signal.direction == SignalDirection.LONG else "buy"
        try:
            await exchange.create_order(
                symbol, "stop_market", sl_side, n_contracts,
                params={"stopPrice": signal.sl_price, "reduceOnly": True}
            )
        except Exception as e:
            log.warning(f"[{ex_id}] SL order: {e}")

        # TP order
        try:
            await exchange.create_order(
                symbol, "take_profit_market", sl_side, n_contracts,
                params={"stopPrice": signal.tp_price, "reduceOnly": True}
            )
        except Exception as e:
            log.warning(f"[{ex_id}] TP order: {e}")

        return Position(
            id=pos_id,
            exchange=ex_id,
            symbol=symbol,
            direction=signal.direction,
            entry_price=entry_price,
            sl_price=signal.sl_price,
            tp_price=signal.tp_price,
            trailing_stop=None,
            size_contracts=n_contracts,
            leverage=self.cfg["leverage"],
            capital_used=allocated_usd,
            highest_price=entry_price,
            lowest_price=entry_price,
        )

    async def close_position(
        self, pos: Position, reason: str, current_price: float
    ) -> float:
        """Zamyka pozycję. Zwraca PnL w USD."""
        log.info(
            f"[{pos.exchange}] 🔴 CLOSE {pos.symbol} | "
            f"reason={reason} | entry={pos.entry_price:.4f} | "
            f"exit={current_price:.4f}"
        )

        if pos.direction == SignalDirection.LONG:
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - current_price) / pos.entry_price

        pnl_usd = pos.capital_used * pnl_pct * pos.leverage

        pos.pnl_usd    = pnl_usd
        pos.close_time  = int(time.time() * 1000)
        pos.close_price = current_price
        pos.close_reason = reason
        pos.state = PositionState.CLOSED

        if not self.dry_run:
            ex = self.fetcher.exchange
            side = "sell" if pos.direction == SignalDirection.LONG else "buy"
            try:
                await ex.create_order(
                    pos.symbol, "market", side, pos.size_contracts,
                    params={"reduceOnly": True}
                )
            except Exception as e:
                log.error(f"[{pos.exchange}] Close order: {e}")

        log.info(
            f"[{pos.exchange}] PnL: {pnl_usd:+.2f} USD "
            f"({pnl_pct*100:+.2f}% × {pos.leverage}x)"
        )
        return pnl_usd


# ──────────────────────────────────────────────────────────────────────────────
# PORTFOLIO GUARD
# ──────────────────────────────────────────────────────────────────────────────

class PortfolioGuard:
    """
    Sprawdza limity portfelowe przed otwarciem nowej pozycji:
    - Max concurrent positions globalnie
    - Max per exchange
    - Brak duplikatów tego samego symbolu
    - Korelacja – nie trzymamy zbyt skorelowanych pozycji jednocześnie
    """

    # Grupy aktywów o wysokiej korelacji
    CORRELATION_GROUPS = [
        {"BTC/USDT:USDT", "ETH/USDT:USDT"},          # Layer 1 majors
        {"SOL/USDT:USDT", "AVAX/USDT:USDT"},          # Alt L1
        {"ARB/USDT:USDT", "OP/USDT:USDT"},             # L2 ecosystem
    ]
    MAX_PER_CORR_GROUP = 1

    def __init__(self, cfg: dict):
        self.cfg = cfg

    def can_open(
        self, signal: TradeSignal, open_positions: List[Position]
    ) -> Tuple[bool, str]:
        active = [p for p in open_positions if p.state != PositionState.CLOSED]

        # Limit globalny
        if len(active) >= self.cfg["max_concurrent_positions"]:
            return False, f"Max pozycji ({self.cfg['max_concurrent_positions']}) osiągnięte"

        # Limit per exchange
        ex_count = sum(1 for p in active if p.exchange == signal.exchange)
        if ex_count >= self.cfg["max_positions_per_exchange"]:
            return False, f"Max pozycji na {signal.exchange} osiągnięte"

        # Duplikat symbolu
        for p in active:
            if p.symbol == signal.symbol and p.exchange == signal.exchange:
                return False, f"Pozycja {signal.symbol} już otwarta na {signal.exchange}"

        # Korelacja
        open_symbols = {p.symbol for p in active}
        for group in self.CORRELATION_GROUPS:
            if signal.symbol in group:
                overlap = open_symbols & group
                if len(overlap) >= self.MAX_PER_CORR_GROUP:
                    return False, f"Korelacja: już mamy {overlap} z tej grupy"

        return True, "OK"


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────────────────────────────────────

class Database:
    """SQLite persistence dla pozycji i logów transakcji."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id            TEXT PRIMARY KEY,
                exchange      TEXT,
                symbol        TEXT,
                direction     TEXT,
                entry_price   REAL,
                sl_price      REAL,
                tp_price      REAL,
                trailing_stop REAL,
                size          REAL,
                leverage      INTEGER,
                capital_used  REAL,
                state         TEXT,
                pnl_usd       REAL,
                open_time     INTEGER,
                close_time    INTEGER,
                close_price   REAL,
                close_reason  TEXT,
                confidence    REAL,
                layers_json   TEXT
            );
            CREATE TABLE IF NOT EXISTS scan_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        INTEGER,
                exchange  TEXT,
                symbol    TEXT,
                tf        TEXT,
                confidence REAL,
                direction  TEXT,
                action     TEXT
            );
        """)
        self.conn.commit()

    def save_position(self, pos: Position, confidence: float = 0.0,
                      layers_json: str = "{}"):
        self.conn.execute("""
            INSERT OR REPLACE INTO positions VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            pos.id, pos.exchange, pos.symbol, pos.direction.name,
            pos.entry_price, pos.sl_price, pos.tp_price, pos.trailing_stop,
            pos.size_contracts, pos.leverage, pos.capital_used, pos.state.value,
            pos.pnl_usd, pos.open_time, pos.close_time, pos.close_price,
            pos.close_reason, confidence, layers_json,
        ))
        self.conn.commit()

    def log_scan(self, exchange: str, symbol: str, tf: str,
                 confidence: float, direction: str, action: str):
        self.conn.execute(
            "INSERT INTO scan_log (ts,exchange,symbol,tf,confidence,direction,action) "
            "VALUES (?,?,?,?,?,?,?)",
            (int(time.time() * 1000), exchange, symbol, tf, confidence, direction, action)
        )
        self.conn.commit()

    def load_open_positions(self) -> List[Dict]:
        cur = self.conn.execute(
            "SELECT * FROM positions WHERE state NOT IN ('closed','cancelled')"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# GŁÓWNA KLASA SILNIKA
# ──────────────────────────────────────────────────────────────────────────────

class NexusSwingEngine:
    """
    Główny orchestrator.
    
    Cykl życia:
      1. Inicjalizacja fetcher'ów per giełda
      2. Co scan_interval: dla każdej giełdy × symbolu × TF
         a. Pobierz OHLCV (1h, 4h, 1d)
         b. Oblicz 7 warstw sygnałów per TF
         c. Compute ensemble confidence
         d. Jeśli confidence ≥ threshold → generuj TradeSignal
         e. PortfolioGuard → jeśli OK → TradeExecutor.open_position
      3. Co ~60s: dla każdej aktywnej pozycji
         a. Pobierz aktualną cenę
         b. RiskManager.update_trailing_stop
         c. RiskManager.should_close → jeśli tak → zamknij
    """

    def __init__(self, cfg: dict = CONFIG):
        self.cfg = cfg
        self.db  = Database(cfg["db_path"])

        self.fetchers:   Dict[str, DataFetcher]  = {}
        self.executors:  Dict[str, TradeExecutor] = {}
        self.positions:  List[Position] = []
        self.risk_mgr    = RiskManager(cfg)
        self.portfolio   = PortfolioGuard(cfg)
        self.sizer       = PositionSizer(cfg)
        self.scorer      = EnsembleScorer()
        self._layers     = SignalLayers()
        self._running    = False

    async def _init_exchanges(self):
        """Inicjalizuje połączenia z giełdami."""
        for ex_id, ex_cfg in self.cfg["exchanges"].items():
            if not ex_cfg.get("api_key"):
                log.debug(f"[{ex_id}] Brak klucza API – pomijam")
                continue
            try:
                fetcher = DataFetcher(ex_id, ex_cfg)
                executor = TradeExecutor(fetcher, self.cfg)
                self.fetchers[ex_id]  = fetcher
                self.executors[ex_id] = executor
                log.info(f"[{ex_id}] ✅ Połączony")
            except Exception as e:
                log.warning(f"[{ex_id}] Init error: {e}")

    def _candles_to_tf_score(
        self, candles_1h: List[OHLCV], candles_4h: List[OHLCV], candles_1d: List[OHLCV],
        funding: float, oi_delta: float
    ) -> Tuple[float, float, float, List[LayerScore], float]:
        """
        Oblicza ensemble score dla danego zestawu świec.
        Zwraca (confidence, tf_score_1h, tf_score_4h, tf_score_1d, layers).
        """

        def single_tf_scores(candles: List[OHLCV], min_len: int = 60
                              ) -> Tuple[float, List[LayerScore]]:
            if len(candles) < min_len:
                return 0.5, []
            layers = [
                SignalLayers.trend_layer(candles),
                SignalLayers.momentum_layer(candles),
                SignalLayers.volume_layer(candles),
                SignalLayers.market_structure_layer(candles),
                SignalLayers.volatility_layer(candles),
            ]
            total_w = sum(l.weight for l in layers)
            sc = sum(l.score * l.weight for l in layers) / total_w
            return sc, layers

        sc_1h, layers_1h = single_tf_scores(candles_1h)
        sc_4h, layers_4h = single_tf_scores(candles_4h)
        sc_1d, layers_1d = single_tf_scores(candles_1d)

        funding_layer = SignalLayers.funding_layer(funding, oi_delta)
        ctf_layer     = SignalLayers.cross_tf_layer(sc_1h, sc_4h, sc_1d)

        # Pełny ensemble dla 4h jako primary
        all_layers = layers_4h + [funding_layer, ctf_layer]
        confidence, direction = self.scorer.compute(all_layers)

        return confidence, sc_1h, sc_4h, sc_1d, all_layers, direction

    async def _analyze_symbol(
        self,
        exchange_id: str,
        symbol: str,
        fetcher: DataFetcher,
    ) -> Optional[TradeSignal]:
        """
        Pełna analiza symbolu na jednej giełdzie.
        Zwraca TradeSignal lub None.
        """
        # Pobierz świece równolegle
        results = await asyncio.gather(
            fetcher.fetch_ohlcv(symbol, "1h", 300),
            fetcher.fetch_ohlcv(symbol, "4h", 300),
            fetcher.fetch_ohlcv(symbol, "1d", 300),
            fetcher.fetch_funding_rate(symbol),
            fetcher.fetch_open_interest_delta(symbol),
            return_exceptions=True,
        )

        candles_1h, candles_4h, candles_1d, funding, oi_delta = [
            r if not isinstance(r, Exception) else ([] if i < 3 else 0.0)
            for i, r in enumerate(results)
        ]

        if len(candles_4h) < 60:
            return None

        # Ensemble scoring
        confidence, sc_1h, sc_4h, sc_1d, layers, direction = \
            self._candles_to_tf_score(
                candles_1h, candles_4h, candles_1d,
                float(funding), float(oi_delta)
            )

        self.db.log_scan(
            exchange_id, symbol, "4h", confidence, direction.name,
            "SIGNAL" if confidence >= self.cfg["confidence_threshold"] else "SKIP"
        )

        if direction == SignalDirection.FLAT:
            return None
        if confidence < self.cfg["confidence_threshold"]:
            log.debug(f"[{exchange_id}] {symbol} confidence={confidence:.3f} < threshold – skip")
            return None

        # Oblicz ATR dla SL/TP
        h4_high  = np.array([c.high  for c in candles_4h])
        h4_low   = np.array([c.low   for c in candles_4h])
        h4_close = np.array([c.close for c in candles_4h])
        atr14    = Indicators.atr(h4_high, h4_low, h4_close, 14)
        atr      = float(atr14[-1]) if not np.isnan(atr14[-1]) else h4_close[-1] * 0.02

        entry = h4_close[-1]
        risk_mgr  = self.risk_mgr
        sl, tp = risk_mgr.compute_sl_tp(entry, atr, direction)

        capital_pct = self.sizer.compute_capital_pct(confidence)

        signal = TradeSignal(
            exchange    = exchange_id,
            symbol      = symbol,
            direction   = direction,
            confidence  = confidence,
            entry_price = entry,
            sl_price    = sl,
            tp_price    = tp,
            atr         = atr,
            capital_pct = capital_pct,
            layers      = layers,
        )

        log.info(
            f"[{exchange_id}] ✨ HIGH-CONFIDENCE SIGNAL │ "
            f"{symbol} {direction.name} │ "
            f"confidence={confidence:.3f} │ "
            f"capital={capital_pct:.1%} │ "
            f"SL/TP={sl:.4f}/{tp:.4f}"
        )
        for l in layers:
            log.debug(f"  └─ [{l.name:15s}] score={l.score:.3f} w={l.weight:.2f} → {l.direction.name}")

        return signal

    async def _scan_loop(self):
        """Główna pętla skanowania sygnałów."""
        log.info("🔍 Scan loop start")
        while self._running:
            try:
                await self._run_scan()
            except Exception as e:
                log.error(f"Scan loop error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(self.cfg["scan_interval_sec"])

    async def _run_scan(self):
        """Jeden cykl skanowania wszystkich giełd i symboli."""
        tasks = []
        for ex_id, fetcher in self.fetchers.items():
            for symbol in self.cfg["watchlist"]:
                tasks.append(self._analyze_symbol(ex_id, symbol, fetcher))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        signals = [r for r in results if isinstance(r, TradeSignal)]
        # Sortuj od najwyższego confidence
        signals.sort(key=lambda s: s.confidence, reverse=True)

        for signal in signals:
            await self._try_enter(signal)

    async def _try_enter(self, signal: TradeSignal):
        """Sprawdza wszystkie warunki i otwiera pozycję jeśli zielone światło."""
        fetcher   = self.fetchers.get(signal.exchange)
        executor  = self.executors.get(signal.exchange)
        if not fetcher or not executor:
            return

        # Pobierz balance
        capital = await fetcher.fetch_balance()
        if capital <= 0:
            log.warning(f"[{signal.exchange}] Brak balance")
            return

        # Risk manager – circuit breaker
        if not self.risk_mgr.check_daily_risk(capital):
            return

        # Portfolio guard
        can_open, reason = self.portfolio.can_open(signal, self.positions)
        if not can_open:
            log.debug(f"[{signal.exchange}] PortfolioGuard: {reason}")
            return

        pos = await executor.open_position(signal, capital)
        if pos:
            self.positions.append(pos)
            self.db.save_position(
                pos, signal.confidence,
                json.dumps([asdict(l) for l in signal.layers], default=str)
            )

    async def _monitor_loop(self):
        """Monitoruje aktywne pozycje i zarządza SL/TP/trailing."""
        log.info("👁  Monitor loop start")
        while self._running:
            try:
                await self._run_monitor()
            except Exception as e:
                log.error(f"Monitor error: {e}")
            await asyncio.sleep(60)

    async def _run_monitor(self):
        """Jeden cykl monitorowania pozycji."""
        active = [p for p in self.positions if p.state != PositionState.CLOSED]
        if not active:
            return

        for pos in active:
            fetcher  = self.fetchers.get(pos.exchange)
            executor = self.executors.get(pos.exchange)
            if not fetcher or not executor:
                continue

            ticker = await fetcher.fetch_ticker(pos.symbol)
            if not ticker:
                continue

            current_price = float(ticker.get("last") or ticker.get("close", 0))
            if current_price <= 0:
                continue

            # Zaktualizuj trailing stop
            pos = self.risk_mgr.update_trailing_stop(pos, current_price)

            # Sprawdź czy zamknąć
            should_close, reason = self.risk_mgr.should_close(pos, current_price)
            if should_close:
                pnl = await executor.close_position(pos, reason, current_price)
                self.risk_mgr.register_pnl(pnl)
                self.db.save_position(pos)
            else:
                # Zaktualizuj PnL unrealized
                if pos.direction == SignalDirection.LONG:
                    pnl_pct = (current_price - pos.entry_price) / pos.entry_price
                else:
                    pnl_pct = (pos.entry_price - current_price) / pos.entry_price
                pos.pnl_usd = pos.capital_used * pnl_pct * pos.leverage
                self.db.save_position(pos)

    async def print_status(self):
        """Wypisuje status portfela."""
        active = [p for p in self.positions if p.state != PositionState.CLOSED]
        closed = [p for p in self.positions if p.state == PositionState.CLOSED]
        total_pnl = sum(p.pnl_usd for p in closed)

        print("\n" + "═" * 70)
        print(f"  NEXUS SWING ENGINE │ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print("═" * 70)
        print(f"  Aktywne pozycje : {len(active)}/{self.cfg['max_concurrent_positions']}")
        print(f"  Zamknięte       : {len(closed)}")
        print(f"  Total PnL       : {total_pnl:+.2f} USD")
        print(f"  Dzienny PnL     : {self.risk_mgr.daily_pnl_usd:+.2f} USD")
        print(f"  Dry Run         : {self.cfg['dry_run']}")
        print("─" * 70)
        for p in active:
            dir_emoji = "🟢" if p.direction == SignalDirection.LONG else "🔴"
            trail_str = f" 📍{p.trailing_stop:.4f}" if p.trailing_stop else ""
            print(
                f"  {dir_emoji} [{p.exchange:8s}] {p.symbol:20s} "
                f"entry={p.entry_price:.4f} "
                f"PnL={p.pnl_usd:+.2f}$ "
                f"[{p.state.value}]{trail_str}"
            )
        print("═" * 70 + "\n")

    async def run(self):
        """Start silnika – uruchamia scan i monitor jako równoległe taski."""
        log.info("🚀 NexusSwingEngine START")
        if self.cfg["dry_run"]:
            log.warning("⚠️  DRY RUN MODE – transakcje nie są wykonywane live!")

        await self._init_exchanges()

        if not self.fetchers:
            log.error("Brak skonfigurowanych giełd z kluczami API. Ustaw api_key w CONFIG.")
            return

        self._running = True
        await asyncio.gather(
            self._scan_loop(),
            self._monitor_loop(),
        )

    async def stop(self):
        """Graceful shutdown."""
        log.info("🛑 Zatrzymuję silnik...")
        self._running = False
        for fetcher in self.fetchers.values():
            await fetcher.close()
        self.db.conn.close()
        log.info("✅ Silnik zatrzymany")


# ──────────────────────────────────────────────────────────────────────────────
# BACKTESTER  (uproszczony – pełny w osobnym module)
# ──────────────────────────────────────────────────────────────────────────────

class SwingBacktester:
    """
    Szybki backtester do walidacji parametrów sygnałów.
    Używa historycznych OHLCV z ccxt (synchronicznie).
    """

    def __init__(self, cfg: dict = CONFIG):
        self.cfg = cfg

    def fetch_historical(
        self, exchange_id: str, symbol: str, tf: str, limit: int = 1000
    ) -> List[OHLCV]:
        ex = getattr(ccxt, exchange_id)()
        raw = ex.fetch_ohlcv(symbol, tf, limit=limit)
        return [OHLCV(*r[:6]) for r in raw]

    def run(
        self,
        exchange_id: str = "binance",
        symbol: str = "BTC/USDT",
        limit: int = 500,
    ) -> Dict[str, Any]:
        """
        Uruchom backtest na historycznych danych.
        Zwraca metryki: win_rate, avg_rr, sharpe, max_dd.
        """
        candles = self.fetch_historical(exchange_id, symbol, "4h", limit)
        candles_1h = self.fetch_historical(exchange_id, symbol, "1h", limit * 4)
        candles_1d = self.fetch_historical(exchange_id, symbol, "1d", limit // 6)

        trades = []
        window = 200  # Minimalne okno analizy

        for i in range(window, len(candles) - 20):
            c4h = candles[:i]
            c1h = candles_1h[:i * 4] if len(candles_1h) > i * 4 else candles_1h
            c1d_end = max(1, i // 6)
            c1d = candles_1d[:c1d_end] if len(candles_1d) > c1d_end else candles_1d

            if not c1h or not c1d:
                continue

            try:
                confidence, sc_1h, sc_4h, sc_1d, layers, direction = \
                    NexusSwingEngine(self.cfg)._candles_to_tf_score(
                        c1h, c4h, c1d, 0.0, 0.0
                    )
            except Exception:
                continue

            if confidence < self.cfg["confidence_threshold"]:
                continue
            if direction == SignalDirection.FLAT:
                continue

            entry = c4h[-1].close
            h_arr = np.array([c.high  for c in c4h])
            l_arr = np.array([c.low   for c in c4h])
            cl_arr= np.array([c.close for c in c4h])
            atr   = Indicators.atr(h_arr, l_arr, cl_arr, 14)[-1]
            if np.isnan(atr):
                continue

            rm = RiskManager(self.cfg)
            sl, tp = rm.compute_sl_tp(entry, atr, direction)

            # Symuluj wynik na kolejnych 20 świecach
            result = "timeout"
            for j in range(i, min(i + 20, len(candles))):
                fut = candles[j]
                if direction == SignalDirection.LONG:
                    if fut.low  <= sl: result = "sl"; break
                    if fut.high >= tp: result = "tp"; break
                else:
                    if fut.high >= sl: result = "sl"; break
                    if fut.low  <= tp: result = "tp"; break

            sl_dist = abs(entry - sl)
            tp_dist = abs(entry - tp)
            rr = tp_dist / sl_dist if sl_dist > 0 else 0

            trades.append({
                "idx":        i,
                "confidence": confidence,
                "direction":  direction.name,
                "result":     result,
                "rr_ratio":   rr,
                "win":        result == "tp",
            })

        if not trades:
            return {"trades": 0, "win_rate": 0, "avg_rr": 0}

        wins    = [t for t in trades if t["win"]]
        losses  = [t for t in trades if not t["win"] and t["result"] == "sl"]
        win_rate = len(wins) / len(trades)

        # Simulate equity curve
        equity = [1.0]
        cap_pct = self.cfg["capital_min_pct"]
        for t in trades:
            if t["win"]:
                gain = cap_pct * t["rr_ratio"] * self.cfg["leverage"]
                equity.append(equity[-1] * (1 + gain))
            else:
                loss = cap_pct * self.cfg["leverage"]
                equity.append(equity[-1] * (1 - loss))

        eq = np.array(equity)
        peak = np.maximum.accumulate(eq)
        dd   = ((peak - eq) / peak).max()

        # Uproszczony Sharpe (zakładamy 0% rfr)
        returns = np.diff(eq) / eq[:-1]
        sharpe = (returns.mean() / (returns.std() + 1e-10)) * math.sqrt(252 * 6)

        result_dict = {
            "symbol":          symbol,
            "total_trades":    len(trades),
            "wins":            len(wins),
            "losses":          len(losses),
            "timeouts":        len(trades) - len(wins) - len(losses),
            "win_rate":        round(win_rate, 4),
            "avg_rr":          round(np.mean([t["rr_ratio"] for t in trades]), 3),
            "max_drawdown":    round(float(dd), 4),
            "sharpe_ratio":    round(float(sharpe), 3),
            "final_equity":    round(float(eq[-1]), 4),
            "avg_confidence":  round(np.mean([t["confidence"] for t in trades]), 4),
        }

        log.info(
            f"📊 BACKTEST {symbol} │ "
            f"Trades={result_dict['total_trades']} │ "
            f"WR={result_dict['win_rate']:.1%} │ "
            f"Sharpe={result_dict['sharpe_ratio']:.2f} │ "
            f"MaxDD={result_dict['max_drawdown']:.1%} │ "
            f"Equity={result_dict['final_equity']:.2f}x"
        )
        return result_dict


# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    """
    Użycie:
      1. Uzupełnij CONFIG['exchanges'] o klucze API
      2. Ustaw CONFIG['dry_run'] = False dla live tradingu
      3. Uruchom: python nexus_swing_engine.py
    """
    engine = NexusSwingEngine(CONFIG)

    # Status co 10 minut
    async def status_printer():
        while True:
            await asyncio.sleep(600)
            await engine.print_status()

    try:
        await asyncio.gather(
            engine.run(),
            status_printer(),
        )
    except KeyboardInterrupt:
        await engine.stop()


if __name__ == "__main__":
    # ── Quick backtest mode (bez API) ────────────────────────────────────────
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        bt = SwingBacktester(CONFIG)
        symbols = CONFIG["watchlist"][:4]   # Pierwsze 4 symbole
        for sym in symbols:
            sym_clean = sym.replace(":USDT", "").replace(":USD", "")
            try:
                result = bt.run("binance", sym_clean, limit=600)
                print(json.dumps(result, indent=2))
            except Exception as e:
                print(f"Backtest {sym_clean}: {e}")
    else:
        asyncio.run(main())
