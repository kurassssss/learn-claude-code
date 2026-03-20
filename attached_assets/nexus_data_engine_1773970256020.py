"""
╔═══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                   ║
║   ███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗                                   ║
║   ████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝                                   ║
║   ██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗                                   ║
║   ██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║                                   ║
║   ██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║                                   ║
║   ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝                                   ║
║                                                                                   ║
║   ██████╗ ███╗   ███╗███╗   ██╗██╗██████╗ ██╗   ██╗███████╗                    ║
║  ██╔═══██╗████╗ ████║████╗  ██║██║██╔══██╗██║   ██║██╔════╝                    ║
║  ██║   ██║██╔████╔██║██╔██╗ ██║██║██████╔╝██║   ██║███████╗                    ║
║  ██║   ██║██║╚██╔╝██║██║╚██╗██║██║██╔══██╗██║   ██║╚════██║                    ║
║  ╚██████╔╝██║ ╚═╝ ██║██║ ╚████║██║██████╔╝╚██████╔╝███████║                    ║
║   ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═══╝╚═╝╚═════╝  ╚═════╝ ╚══════╝                    ║
║                                                                                   ║
║   D A T A   E N G I N E   v3.0  —  U N I V E R S A L   M A R K E T   B U S     ║
║                                                                                   ║
║   Sources (ALL simultaneous, async):                                             ║
║   • 15+ Crypto Exchanges via CCXT  (spot + futures, all pairs)                  ║
║   • WebSocket tick streams         (Kraken, Bybit, Binance, OKX…)              ║
║   • Order Books L2/L3              (depth, imbalance, spread)                   ║
║   • Funding Rates + Open Interest  (perps + futures)                            ║
║   • Liquidation feeds              (heatmaps, cascades)                          ║
║   • On-Chain Bitcoin metrics       (hash rate, mempool, SOPR, NVT)              ║
║   • Fear & Greed Index             (CNN + Alternative.me)                        ║
║   • Macro Markets                  (indices, VIX, DXY, gold, oil via yfinance)  ║
║   • News & Sentiment               (CryptoCompare, RSS, NLP scoring)            ║
║   • Google Trends                  (crypto search interest)                      ║
║   • Technical Indicators           (pandas-ta: 130+ indicators on live data)    ║
║   • Volatility Surface             (implied vol, GARCH, realized vol)           ║
║   • Correlation Matrix             (rolling cross-asset correlations)           ║
║   • Portfolio Risk Metrics         (empyrical: Sharpe, Sortino, VaR, CVaR)     ║
║                                                                                   ║
║   Dispatch:                                                                       ║
║   • Pub/Sub event bus (asyncio)    → any number of subscribers                  ║
║   • Redis pub/sub                  → inter-process / distributed                ║
║   • FastAPI WebSocket broadcast    → dashboards, front-ends                     ║
║   • Direct callback injection      → NEXUS / Kraken Ultra / any agent           ║
║   • SQLite + InfluxDB persistence  → time-series storage                        ║
║   • Parquet snapshots              → backtesting datasets                       ║
║                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ── stdlib ────────────────────────────────────────────────────────────────────
import asyncio
import collections
import contextlib
import copy
import csv
import gzip
import hashlib
import io
import json
import logging
import math
import os
import queue
import random
import re
import signal
import sqlite3
import sys
import tempfile
import textwrap
import time
import traceback
import uuid
import weakref
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List,
    Optional, Set, Tuple, Union,
)

import numpy as np

# ── third-party (graceful optional imports) ───────────────────────────────────
def _try(name, pip_hint=""):
    try:
        return __import__(name)
    except ImportError:
        return None


ccxt        = _try("ccxt")
pd          = _try("pandas")
ta          = _try("pandas_ta")  # pandas-ta
aiohttp     = _try("aiohttp")
websockets  = _try("websockets")
httpx_mod   = _try("httpx")
yf          = _try("yfinance")
redis_mod   = _try("redis")
influx_mod  = _try("influxdb_client")
empyrical   = _try("empyrical")
arch_mod    = _try("arch")
pyarrow     = _try("pyarrow")
scipy_mod   = _try("scipy")
sklearn_mod = _try("sklearn")
loguru_mod  = _try("loguru")
rich_mod    = _try("rich")
fastapi_mod = _try("fastapi")
uvicorn_mod = _try("uvicorn")

# ── logger setup ──────────────────────────────────────────────────────────────
if loguru_mod:
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format=(
        "<green>{time:HH:mm:ss.SSS}</green> │ "
        "<level>{level:<8}</level> │ "
        "<cyan>{name:<22}</cyan> │ {message}"
    ), level="DEBUG", colorize=True)
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s │ %(name)-22s │ %(levelname)-8s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("NEXUS.DATA")

# ── constants ─────────────────────────────────────────────────────────────────
DATA_DIR = Path(os.getenv("NEXUS_DATA_DIR", "nexus_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR = DATA_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)

UTC = timezone.utc
_TS  = lambda: datetime.now(UTC).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
#  DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class DataType(Enum):
    TICKER          = "ticker"
    OHLCV           = "ohlcv"
    ORDERBOOK       = "orderbook"
    TRADE           = "trade"
    FUNDING_RATE    = "funding_rate"
    OPEN_INTEREST   = "open_interest"
    LIQUIDATION     = "liquidation"
    ON_CHAIN        = "on_chain"
    MACRO           = "macro"
    SENTIMENT       = "sentiment"
    NEWS            = "news"
    FEAR_GREED      = "fear_greed"
    INDICATOR       = "indicator"
    VOLATILITY      = "volatility"
    CORRELATION     = "correlation"
    RISK_METRICS    = "risk_metrics"
    PORTFOLIO       = "portfolio"
    SYSTEM          = "system"


@dataclass
class MarketEvent:
    """Universal envelope for every data point flowing through the bus."""
    id:         str            = field(default_factory=lambda: str(uuid.uuid4())[:8])
    type:       DataType       = DataType.TICKER
    source:     str            = ""          # exchange / data-provider name
    symbol:     str            = ""          # e.g. BTC/USDT
    timestamp:  str            = field(default_factory=_TS)
    data:       Dict[str, Any] = field(default_factory=dict)
    tags:       Dict[str, str] = field(default_factory=dict)
    latency_ms: float          = 0.0

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["type"] = self.type.value
        return d

    @classmethod
    def from_dict(cls, d: Dict) -> "MarketEvent":
        d2 = dict(d)
        d2["type"] = DataType(d2.get("type", "ticker"))
        return cls(**d2)


@dataclass
class OHLCVBar:
    symbol:     str
    exchange:   str
    timeframe:  str
    timestamp:  int     # unix ms
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    extra:      Dict    = field(default_factory=dict)

    def to_row(self) -> List:
        return [self.timestamp, self.open, self.high, self.low,
                self.close, self.volume]


@dataclass
class OrderBook:
    symbol:     str
    exchange:   str
    timestamp:  int
    bids:       List[List[float]]   # [[price, amount], ...]
    asks:       List[List[float]]
    spread:     float  = 0.0
    mid:        float  = 0.0
    imbalance:  float  = 0.0   # (bid_vol - ask_vol) / (bid_vol + ask_vol)
    depth_bid:  float  = 0.0   # total bid volume top-20
    depth_ask:  float  = 0.0


@dataclass
class TickerSnapshot:
    symbol:         str
    exchange:       str
    timestamp:      int
    bid:            float
    ask:            float
    last:           float
    volume_24h:     float
    change_24h_pct: float
    high_24h:       float
    low_24h:        float
    funding_rate:   Optional[float] = None
    open_interest:  Optional[float] = None
    mark_price:     Optional[float] = None
    index_price:    Optional[float] = None


# ═══════════════════════════════════════════════════════════════════════════════
#  ASYNC EVENT BUS  (pub/sub, typed, multi-subscriber)
# ═══════════════════════════════════════════════════════════════════════════════

class EventBus:
    """
    Central async pub/sub bus. Subscribers register interest in DataType(s).
    All dispatch is non-blocking; slow subscribers get dropped messages (ring-buffer).
    """

    def __init__(self, buffer_size: int = 2048):
        self._subs: Dict[DataType, List[asyncio.Queue]] = defaultdict(list)
        self._wild_subs: List[asyncio.Queue] = []          # subscribe to ALL
        self._buffer_size = buffer_size
        self._total_published = 0
        self._total_dropped = 0
        self._lock = asyncio.Lock()

    async def subscribe(self, *types: DataType) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._buffer_size)
        async with self._lock:
            if not types:
                self._wild_subs.append(q)
            else:
                for t in types:
                    self._subs[t].append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue):
        async with self._lock:
            self._wild_subs = [s for s in self._wild_subs if s is not q]
            for subs in self._subs.values():
                while q in subs:
                    subs.remove(q)

    async def publish(self, event: MarketEvent):
        self._total_published += 1
        targets = list(self._subs.get(event.type, [])) + self._wild_subs
        for q in targets:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # drop oldest, insert newest
                with suppress(asyncio.QueueEmpty):
                    q.get_nowait()
                q.put_nowait(event)
                self._total_dropped += 1

    def stats(self) -> Dict:
        return {
            "published": self._total_published,
            "dropped":   self._total_dropped,
            "subscribers": sum(len(v) for v in self._subs.values()) + len(self._wild_subs),
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  PERSISTENT STORE  (SQLite for tick data + Parquet for OHLCV bulk)
# ═══════════════════════════════════════════════════════════════════════════════

class DataStore:
    """Thread-safe SQLite store with Parquet snapshots for bulk OHLCV."""

    def __init__(self, path: Path = DATA_DIR / "nexus_market_data.db"):
        self.path = path
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()
        self._write_queue: deque = deque(maxlen=50_000)

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tickers (
                id          TEXT PRIMARY KEY,
                symbol      TEXT,
                exchange    TEXT,
                bid         REAL, ask REAL, last REAL,
                volume_24h  REAL, change_pct REAL,
                funding_rate REAL, open_interest REAL,
                ts          INTEGER,
                raw         TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tick_sym ON tickers(symbol, ts);

            CREATE TABLE IF NOT EXISTS ohlcv (
                id          TEXT PRIMARY KEY,
                symbol      TEXT,
                exchange    TEXT,
                timeframe   TEXT,
                ts          INTEGER,
                open        REAL, high REAL, low REAL, close REAL, volume REAL
            );
            CREATE INDEX IF NOT EXISTS idx_ohlcv ON ohlcv(symbol, exchange, timeframe, ts);

            CREATE TABLE IF NOT EXISTS orderbooks (
                id          TEXT PRIMARY KEY,
                symbol      TEXT, exchange TEXT, ts INTEGER,
                spread      REAL, mid REAL, imbalance REAL,
                depth_bid   REAL, depth_ask REAL,
                raw         TEXT
            );

            CREATE TABLE IF NOT EXISTS macro (
                id          TEXT PRIMARY KEY,
                symbol      TEXT, source TEXT, ts INTEGER,
                price       REAL, change_pct REAL, extra TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_macro ON macro(symbol, ts);

            CREATE TABLE IF NOT EXISTS sentiment (
                id          TEXT PRIMARY KEY,
                source      TEXT, ts INTEGER,
                fear_greed  INTEGER, sentiment_score REAL,
                headline    TEXT, extra TEXT
            );

            CREATE TABLE IF NOT EXISTS on_chain (
                id          TEXT PRIMARY KEY,
                metric      TEXT, ts INTEGER,
                value       REAL, extra TEXT
            );

            CREATE TABLE IF NOT EXISTS indicators (
                id          TEXT PRIMARY KEY,
                symbol      TEXT, exchange TEXT, timeframe TEXT,
                ts          INTEGER,
                indicators  TEXT
            );
        """)
        self.conn.commit()

    # ── write helpers ──────────────────────────────────────────────────────────

    def store_ticker(self, t: TickerSnapshot):
        self.conn.execute(
            "INSERT OR REPLACE INTO tickers VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4())[:8], t.symbol, t.exchange, t.bid, t.ask, t.last,
             t.volume_24h, t.change_24h_pct, t.funding_rate, t.open_interest,
             t.timestamp, json.dumps(asdict(t)))
        )
        self.conn.commit()

    def store_ohlcv_batch(self, bars: List[OHLCVBar]):
        self.conn.executemany(
            "INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(str(uuid.uuid4())[:8], b.symbol, b.exchange, b.timeframe,
              b.timestamp, b.open, b.high, b.low, b.close, b.volume)
             for b in bars]
        )
        self.conn.commit()

    def store_orderbook(self, ob: OrderBook):
        self.conn.execute(
            "INSERT OR REPLACE INTO orderbooks VALUES (?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4())[:8], ob.symbol, ob.exchange, ob.timestamp,
             ob.spread, ob.mid, ob.imbalance, ob.depth_bid, ob.depth_ask,
             json.dumps({"bids": ob.bids[:20], "asks": ob.asks[:20]}))
        )
        self.conn.commit()

    def store_macro(self, symbol: str, source: str, price: float,
                    change_pct: float, extra: Dict = {}):
        self.conn.execute(
            "INSERT OR REPLACE INTO macro VALUES (?,?,?,?,?,?,?)",
            (str(uuid.uuid4())[:8], symbol, source,
             int(datetime.now(UTC).timestamp() * 1000),
             price, change_pct, json.dumps(extra))
        )
        self.conn.commit()

    def store_sentiment(self, source: str, fear_greed: int,
                        score: float, headline: str = "", extra: Dict = {}):
        self.conn.execute(
            "INSERT OR REPLACE INTO sentiment VALUES (?,?,?,?,?,?,?)",
            (str(uuid.uuid4())[:8], source,
             int(datetime.now(UTC).timestamp() * 1000),
             fear_greed, score, headline, json.dumps(extra))
        )
        self.conn.commit()

    def store_on_chain(self, metric: str, value: float, extra: Dict = {}):
        self.conn.execute(
            "INSERT OR REPLACE INTO on_chain VALUES (?,?,?,?,?)",
            (str(uuid.uuid4())[:8], metric,
             int(datetime.now(UTC).timestamp() * 1000),
             value, json.dumps(extra))
        )
        self.conn.commit()

    def store_indicators(self, symbol: str, exchange: str, tf: str,
                         ts: int, ind: Dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO indicators VALUES (?,?,?,?,?,?)",
            (str(uuid.uuid4())[:8], symbol, exchange, tf, ts, json.dumps(ind))
        )
        self.conn.commit()

    # ── read helpers ──────────────────────────────────────────────────────────

    def get_ohlcv(self, symbol: str, exchange: str, tf: str,
                  limit: int = 500) -> Optional[Any]:
        if pd is None:
            return None
        rows = self.conn.execute(
            "SELECT ts, open, high, low, close, volume FROM ohlcv "
            "WHERE symbol=? AND exchange=? AND timeframe=? "
            "ORDER BY ts DESC LIMIT ?",
            (symbol, exchange, tf, limit)
        ).fetchall()
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    def get_latest_tickers(self, limit: int = 200) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT symbol, exchange, last, change_pct, volume_24h, funding_rate, ts "
            "FROM tickers ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [{"symbol": r[0], "exchange": r[1], "last": r[2],
                 "change_pct": r[3], "volume_24h": r[4],
                 "funding_rate": r[5], "ts": r[6]} for r in rows]

    def get_fear_greed_history(self, limit: int = 30) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT ts, fear_greed, sentiment_score FROM sentiment "
            "ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [{"ts": r[0], "fear_greed": r[1], "score": r[2]} for r in rows]

    def save_parquet(self, symbol: str, exchange: str, tf: str):
        """Export OHLCV to Parquet for backtesting."""
        if pyarrow is None or pd is None:
            return
        df = self.get_ohlcv(symbol, exchange, tf, limit=100_000)
        if df is None or len(df) == 0:
            return
        fname = PARQUET_DIR / f"{exchange}_{symbol.replace('/','_')}_{tf}.parquet"
        df.to_parquet(str(fname), engine="pyarrow", compression="snappy")
        logger.info(f"💾 Parquet saved: {fname.name} ({len(df)} bars)")

    def load_parquet(self, symbol: str, exchange: str, tf: str) -> Optional[Any]:
        if pd is None:
            return None
        fname = PARQUET_DIR / f"{exchange}_{symbol.replace('/','_')}_{tf}.parquet"
        if not fname.exists():
            return None
        return pd.read_parquet(str(fname))


# ═══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATOR COMPUTER  (pandas-ta, 130+ indicators)
# ═══════════════════════════════════════════════════════════════════════════════

class IndicatorEngine:
    """
    Computes 130+ technical indicators via pandas-ta on a DataFrame.
    Falls back to manual numpy implementations when pandas-ta not available.
    """

    # Indicator groups to compute
    STRATEGY_PARAMS = dict(
        sma_fast=20, sma_slow=50, ema_fast=12, ema_slow=26, ema_signal=9,
        rsi_period=14, bb_period=20, bb_std=2.0, atr_period=14,
        stoch_k=14, stoch_d=3, adx_period=14, cci_period=20,
        willr_period=14, macd_fast=12, macd_slow=26, macd_signal=9,
        roc_period=10, momentum_period=10, obv_period=20,
    )

    @classmethod
    def compute(cls, df: Any, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Returns dict of indicator name → latest value (float or dict)."""
        if pd is None or df is None or len(df) < 30:
            return {}

        p = {**cls.STRATEGY_PARAMS, **(params or {})}
        result: Dict[str, Any] = {}

        try:
            close = df["close"]
            high  = df["high"]
            low   = df["low"]
            vol   = df["volume"]

            if ta is not None:
                # Use pandas-ta strategy (compute all at once)
                _df = df.copy()
                # Core trend
                _df.ta.sma(length=p["sma_fast"], append=True)
                _df.ta.sma(length=p["sma_slow"], append=True)
                _df.ta.ema(length=p["ema_fast"], append=True)
                _df.ta.ema(length=p["ema_slow"], append=True)
                # Oscillators
                _df.ta.rsi(length=p["rsi_period"], append=True)
                _df.ta.macd(fast=p["macd_fast"], slow=p["macd_slow"],
                            signal=p["macd_signal"], append=True)
                _df.ta.stoch(k=p["stoch_k"], d=p["stoch_d"], append=True)
                _df.ta.adx(length=p["adx_period"], append=True)
                _df.ta.cci(length=p["cci_period"], append=True)
                _df.ta.willr(length=p["willr_period"], append=True)
                # Volatility
                _df.ta.bbands(length=p["bb_period"], std=p["bb_std"], append=True)
                _df.ta.atr(length=p["atr_period"], append=True)
                _df.ta.kc(append=True)   # Keltner Channel
                # Volume
                _df.ta.obv(append=True)
                _df.ta.vwap(append=True)
                _df.ta.mfi(length=14, append=True)
                # Momentum
                _df.ta.roc(length=p["roc_period"], append=True)
                _df.ta.mom(length=p["momentum_period"], append=True)
                _df.ta.tsi(append=True)
                _df.ta.uo(append=True)    # Ultimate Oscillator
                _df.ta.ao(append=True)    # Awesome Oscillator
                # Trend
                _df.ta.aroon(length=14, append=True)
                _df.ta.dpo(length=20, append=True)
                _df.ta.vortex(append=True)
                _df.ta.psar(append=True)  # Parabolic SAR

                # Extract last row of all new columns
                new_cols = [c for c in _df.columns if c not in df.columns]
                last = _df.iloc[-1]
                for col in new_cols:
                    v = last[col]
                    if pd.notna(v):
                        result[col] = round(float(v), 6)

            else:
                # Fallback: manual numpy implementations
                c = close.values.astype(float)

                def _sma(arr, n):
                    return float(np.mean(arr[-n:])) if len(arr) >= n else float("nan")

                def _ema(arr, n):
                    k = 2 / (n + 1)
                    e = arr[0]
                    for x in arr[1:]:
                        e = x * k + e * (1 - k)
                    return float(e)

                def _rsi(arr, n):
                    d = np.diff(arr)
                    g = np.where(d > 0, d, 0.0)
                    l_ = np.where(d < 0, -d, 0.0)
                    ag = np.mean(g[-n:]) + 1e-10
                    al = np.mean(l_[-n:]) + 1e-10
                    return 100 - 100 / (1 + ag / al)

                result["SMA_20"]  = _sma(c, p["sma_fast"])
                result["SMA_50"]  = _sma(c, p["sma_slow"])
                result["EMA_12"]  = _ema(c[-50:], p["ema_fast"])
                result["EMA_26"]  = _ema(c[-50:], p["ema_slow"])
                result["RSI_14"]  = _rsi(c[-50:], p["rsi_period"])
                result["ROC_10"]  = float((c[-1] / (c[-p["roc_period"]] + 1e-10) - 1) * 100)
                result["MOM_10"]  = float(c[-1] - c[-p["momentum_period"]])

                h, l = high.values.astype(float), low.values.astype(float)
                tr = np.maximum(h[1:] - l[1:],
                     np.maximum(np.abs(h[1:] - c[:-1]),
                                np.abs(l[1:] - c[:-1])))
                result["ATR_14"] = float(np.mean(tr[-p["atr_period"]:]))

                # Bollinger Bands
                bb_c = c[-p["bb_period"]:]
                bb_m = float(np.mean(bb_c))
                bb_s = float(np.std(bb_c)) * p["bb_std"]
                result["BBL_20"] = bb_m - bb_s
                result["BBM_20"] = bb_m
                result["BBU_20"] = bb_m + bb_s
                result["BBB_20"] = float((2 * bb_s / (bb_m + 1e-10)) * 100)

            # Derived signals (always computed)
            sma_fast = result.get(f"SMA_{p['sma_fast']}", result.get("SMA_20"))
            sma_slow = result.get(f"SMA_{p['sma_slow']}", result.get("SMA_50"))
            ema_fast = result.get(f"EMA_{p['ema_fast']}", result.get("EMA_12"))
            ema_slow = result.get(f"EMA_{p['ema_slow']}", result.get("EMA_26"))
            rsi      = result.get(f"RSI_{p['rsi_period']}", result.get("RSI_14"))
            atr      = result.get(f"ATRr_{p['atr_period']}", result.get("ATR_14"))

            last_close = float(close.iloc[-1])
            result["last_close"]        = last_close
            result["sma_cross_up"]      = int(sma_fast > sma_slow) if sma_fast and sma_slow else 0
            result["ema_cross_up"]      = int(ema_fast > ema_slow) if ema_fast and ema_slow else 0
            result["rsi_oversold"]      = int(rsi < 30) if rsi else 0
            result["rsi_overbought"]    = int(rsi > 70) if rsi else 0
            result["atr_pct"]           = float(atr / last_close * 100) if atr and last_close else 0
            result["price_vs_sma50"]    = float((last_close / sma_slow - 1) * 100) if sma_slow else 0

        except Exception as e:
            logger.warning(f"IndicatorEngine error: {e}")

        return result

    @classmethod
    def compute_volatility(cls, close_series: Any, windows: List[int] = [7, 14, 30]) -> Dict:
        """Realized volatility + GARCH fit."""
        result: Dict[str, Any] = {}
        if pd is None:
            return result
        try:
            c = np.array(close_series, dtype=float)
            log_ret = np.diff(np.log(c + 1e-12))
            for w in windows:
                if len(log_ret) >= w:
                    rv = float(np.std(log_ret[-w:]) * math.sqrt(365 * 24) * 100)
                    result[f"realized_vol_{w}d"] = rv

            # GARCH(1,1) if arch available
            if arch_mod is not None and len(log_ret) >= 100:
                try:
                    from arch import arch_model
                    am = arch_model(log_ret[-500:] * 100, vol="Garch", p=1, q=1, dist="Normal")
                    res = am.fit(disp="off", show_warning=False)
                    forecast = res.forecast(horizon=1)
                    result["garch_vol_1d"] = float(math.sqrt(forecast.variance.values[-1, 0]))
                    result["garch_omega"]  = float(res.params.get("omega", 0))
                    result["garch_alpha"]  = float(res.params.get("alpha[1]", 0))
                    result["garch_beta"]   = float(res.params.get("beta[1]", 0))
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Volatility compute error: {e}")
        return result

    @classmethod
    def compute_risk_metrics(cls, returns: Any) -> Dict:
        """Portfolio-level risk metrics via empyrical."""
        result: Dict[str, Any] = {}
        if empyrical is None or pd is None:
            return result
        try:
            import empyrical as em
            r = np.array(returns, dtype=float)
            result["sharpe"]    = float(em.sharpe_ratio(r))
            result["sortino"]   = float(em.sortino_ratio(r))
            result["max_dd"]    = float(em.max_drawdown(r) * 100)
            result["calmar"]    = float(em.calmar_ratio(r))
            result["omega"]     = float(em.omega_ratio(r))
            result["var_95"]    = float(em.value_at_risk(r, cutoff=0.05) * 100)
            result["cvar_95"]   = float(em.conditional_value_at_risk(r, cutoff=0.05) * 100)
            result["ann_return"]= float(em.annual_return(r) * 100)
            result["ann_vol"]   = float(em.annual_volatility(r) * 100)
        except Exception as e:
            logger.warning(f"Risk metrics error: {e}")
        return result


# ═══════════════════════════════════════════════════════════════════════════════
#  EXCHANGE COLLECTOR  (CCXT — 15+ exchanges, REST polling)
# ═══════════════════════════════════════════════════════════════════════════════

class ExchangeCollector:
    """
    Polls 15+ exchanges for tickers, OHLCV, order books, funding rates,
    open interest, and liquidations using CCXT.
    """

    # Priority exchanges — each has unique data we want
    EXCHANGE_CONFIGS = {
        "kraken": {
            "pro": False, "futures_id": "krakenperps",
            "markets": ["spot", "futures"],
            "priority": 1, "max_symbols": 200,
        },
        "bybit": {
            "pro": False,
            "markets": ["spot", "linear", "inverse"],
            "priority": 1, "max_symbols": 300,
        },
        "binance": {
            "pro": False,
            "markets": ["spot", "futures"],
            "priority": 1, "max_symbols": 500,
        },
        "okx": {
            "pro": False,
            "markets": ["spot", "swap", "futures"],
            "priority": 2, "max_symbols": 400,
        },
        "coinbasepro": {
            "pro": False, "markets": ["spot"],
            "priority": 2, "max_symbols": 200,
        },
        "huobi": {
            "pro": False, "markets": ["spot", "swap"],
            "priority": 2, "max_symbols": 300,
        },
        "kucoin": {
            "pro": False, "markets": ["spot", "futures"],
            "priority": 2, "max_symbols": 300,
        },
        "gate": {
            "pro": False, "markets": ["spot", "futures"],
            "priority": 3, "max_symbols": 500,
        },
        "bitget": {
            "pro": False, "markets": ["spot", "swap"],
            "priority": 3, "max_symbols": 300,
        },
        "mexc": {
            "pro": False, "markets": ["spot", "swap"],
            "priority": 3, "max_symbols": 400,
        },
        "bitmex": {
            "pro": False, "markets": ["swap", "futures"],
            "priority": 3, "max_symbols": 50,
        },
        "deribit": {
            "pro": False, "markets": ["option", "futures"],
            "priority": 3, "max_symbols": 50,
        },
        "phemex": {
            "pro": False, "markets": ["spot", "swap"],
            "priority": 4, "max_symbols": 100,
        },
        "bitfinex": {
            "pro": False, "markets": ["spot"],
            "priority": 4, "max_symbols": 100,
        },
        "poloniex": {
            "pro": False, "markets": ["spot"],
            "priority": 4, "max_symbols": 100,
        },
    }

    TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]
    TOP_QUOTE_CURRENCIES = ["USDT", "USD", "BTC", "ETH", "USDC", "BUSD", "EUR"]

    def __init__(self, bus: EventBus, store: DataStore,
                 enabled_exchanges: Optional[List[str]] = None,
                 api_keys: Optional[Dict[str, Dict]] = None):
        self.bus  = bus
        self.store = store
        self.api_keys = api_keys or {}
        self.exchanges: Dict[str, Any]  = {}
        self.symbol_universe: Dict[str, List[str]] = {}  # ex_id → [symbols]
        self._active_exchanges: List[str] = enabled_exchanges or list(self.EXCHANGE_CONFIGS.keys())

    # ── init ───────────────────────────────────────────────────────────────────

    async def init_exchanges(self):
        """Instantiate CCXT exchange objects (non-blocking)."""
        if ccxt is None:
            logger.warning("ccxt not installed — ExchangeCollector disabled")
            return

        for ex_id in self._active_exchanges:
            try:
                cls = getattr(ccxt, ex_id, None)
                if cls is None:
                    continue
                creds = self.api_keys.get(ex_id, {})
                config = {
                    "timeout": 30_000,
                    "enableRateLimit": True,
                    "rateLimit": 200,
                    **creds,
                }
                exchange = cls(config)
                await asyncio.get_event_loop().run_in_executor(
                    None, exchange.load_markets
                )
                self.exchanges[ex_id] = exchange
                logger.info(f"✅ Exchange loaded: {ex_id} "
                            f"({len(exchange.symbols)} symbols)")
            except Exception as e:
                logger.warning(f"⚠️  {ex_id} init failed: {e}")

    def _top_symbols(self, exchange: Any, max_n: int = 100) -> List[str]:
        """Return top-N most liquid symbols (filter by USDT/USD quote)."""
        syms = []
        for sym, mkt in exchange.markets.items():
            if mkt.get("quote") in self.TOP_QUOTE_CURRENCIES and mkt.get("active", True):
                syms.append(sym)
        # prefer USDT
        usdt = [s for s in syms if "/USDT" in s]
        other = [s for s in syms if s not in usdt]
        return (usdt + other)[:max_n]

    # ── REST polling ───────────────────────────────────────────────────────────

    async def fetch_all_tickers(self, ex_id: str) -> int:
        """Fetch all tickers from one exchange, publish to bus, store."""
        ex = self.exchanges.get(ex_id)
        if not ex:
            return 0
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(None, ex.fetch_tickers)
            ts_now = int(datetime.now(UTC).timestamp() * 1000)
            count = 0
            for sym, t in raw.items():
                try:
                    ticker = TickerSnapshot(
                        symbol         = sym,
                        exchange       = ex_id,
                        timestamp      = int(t.get("timestamp") or ts_now),
                        bid            = float(t.get("bid") or 0),
                        ask            = float(t.get("ask") or 0),
                        last           = float(t.get("last") or t.get("close") or 0),
                        volume_24h     = float(t.get("quoteVolume") or t.get("baseVolume") or 0),
                        change_24h_pct = float(t.get("percentage") or 0),
                        high_24h       = float(t.get("high") or 0),
                        low_24h        = float(t.get("low") or 0),
                        funding_rate   = float(t.get("info", {}).get("fundingRate", 0) or 0),
                        open_interest  = float(t.get("openInterest") or 0),
                        mark_price     = float(t.get("markPrice") or 0),
                        index_price    = float(t.get("indexPrice") or 0),
                    )
                    if ticker.last <= 0:
                        continue
                    self.store.store_ticker(ticker)
                    event = MarketEvent(
                        type=DataType.TICKER, source=ex_id, symbol=sym,
                        data=asdict(ticker), latency_ms=0,
                    )
                    await self.bus.publish(event)
                    count += 1
                except Exception:
                    pass
            logger.debug(f"  {ex_id}: {count} tickers published")
            return count
        except Exception as e:
            logger.warning(f"fetch_all_tickers {ex_id}: {e}")
            return 0

    async def fetch_ohlcv(self, ex_id: str, symbol: str,
                          timeframe: str = "1h", limit: int = 500) -> List[OHLCVBar]:
        ex = self.exchanges.get(ex_id)
        if not ex or not ex.has.get("fetchOHLCV"):
            return []
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: ex.fetch_ohlcv(symbol, timeframe, limit=limit)
            )
            bars = []
            for r in raw:
                b = OHLCVBar(
                    symbol=symbol, exchange=ex_id, timeframe=timeframe,
                    timestamp=int(r[0]), open=float(r[1]), high=float(r[2]),
                    low=float(r[3]), close=float(r[4]), volume=float(r[5]),
                )
                bars.append(b)
            if bars:
                self.store.store_ohlcv_batch(bars)
                # Compute indicators on the fetched batch
                if pd is not None and len(bars) >= 50:
                    df = pd.DataFrame(
                        [b.to_row() for b in bars],
                        columns=["timestamp","open","high","low","close","volume"]
                    )
                    ind = IndicatorEngine.compute(df)
                    if ind:
                        self.store.store_indicators(
                            symbol, ex_id, timeframe, bars[-1].timestamp, ind
                        )
                        await self.bus.publish(MarketEvent(
                            type=DataType.INDICATOR, source=ex_id, symbol=symbol,
                            data={"timeframe": timeframe, **ind}
                        ))
            return bars
        except Exception as e:
            logger.debug(f"fetch_ohlcv {ex_id} {symbol}: {e}")
            return []

    async def fetch_orderbook(self, ex_id: str, symbol: str, depth: int = 20) -> Optional[OrderBook]:
        ex = self.exchanges.get(ex_id)
        if not ex or not ex.has.get("fetchOrderBook"):
            return None
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: ex.fetch_order_book(symbol, depth)
            )
            bids = raw.get("bids", [])[:depth]
            asks = raw.get("asks", [])[:depth]
            bid_top = float(bids[0][0]) if bids else 0
            ask_top = float(asks[0][0]) if asks else 0
            mid = (bid_top + ask_top) / 2 if bid_top and ask_top else 0
            spread = (ask_top - bid_top) if bid_top and ask_top else 0
            bid_vol = sum(float(b[1]) for b in bids)
            ask_vol = sum(float(a[1]) for a in asks)
            imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-10)
            ob = OrderBook(
                symbol=symbol, exchange=ex_id,
                timestamp=int(raw.get("timestamp") or
                              datetime.now(UTC).timestamp() * 1000),
                bids=bids, asks=asks,
                spread=spread, mid=mid,
                imbalance=float(imbalance),
                depth_bid=float(bid_vol), depth_ask=float(ask_vol),
            )
            self.store.store_orderbook(ob)
            await self.bus.publish(MarketEvent(
                type=DataType.ORDERBOOK, source=ex_id, symbol=symbol,
                data={
                    "spread": spread, "mid": mid,
                    "imbalance": float(imbalance),
                    "depth_bid": float(bid_vol),
                    "depth_ask": float(ask_vol),
                    "best_bid": bid_top, "best_ask": ask_top,
                    "bids_top5": bids[:5], "asks_top5": asks[:5],
                }
            ))
            return ob
        except Exception as e:
            logger.debug(f"fetch_orderbook {ex_id} {symbol}: {e}")
            return None

    async def fetch_funding_rates(self, ex_id: str) -> Dict[str, float]:
        ex = self.exchanges.get(ex_id)
        if not ex or not ex.has.get("fetchFundingRates"):
            return {}
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(None, ex.fetch_funding_rates)
            result = {}
            for sym, d in raw.items():
                fr = d.get("fundingRate")
                if fr is not None:
                    result[sym] = float(fr)
                    await self.bus.publish(MarketEvent(
                        type=DataType.FUNDING_RATE, source=ex_id, symbol=sym,
                        data={
                            "funding_rate": float(fr),
                            "next_funding_time": d.get("nextFundingDatetime"),
                            "funding_rate_8h_annualized": float(fr) * 3 * 365 * 100,
                        }
                    ))
            return result
        except Exception as e:
            logger.debug(f"funding_rates {ex_id}: {e}")
            return {}

    async def fetch_open_interest(self, ex_id: str, symbol: str) -> Optional[float]:
        ex = self.exchanges.get(ex_id)
        if not ex or not ex.has.get("fetchOpenInterest"):
            return None
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(None, lambda: ex.fetch_open_interest(symbol))
            oi = float(raw.get("openInterestAmount") or raw.get("openInterest") or 0)
            await self.bus.publish(MarketEvent(
                type=DataType.OPEN_INTEREST, source=ex_id, symbol=symbol,
                data={"open_interest": oi}
            ))
            return oi
        except Exception:
            return None

    async def fetch_liquidations(self, ex_id: str, symbol: str) -> List[Dict]:
        ex = self.exchanges.get(ex_id)
        if not ex or not ex.has.get("fetchLiquidations"):
            return []
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(None, lambda: ex.fetch_liquidations(symbol))
            liq = []
            for l in raw[:50]:
                d = {
                    "symbol": symbol, "exchange": ex_id,
                    "timestamp": l.get("timestamp"),
                    "side": l.get("side"),
                    "amount": float(l.get("amount") or 0),
                    "price": float(l.get("price") or 0),
                }
                liq.append(d)
                await self.bus.publish(MarketEvent(
                    type=DataType.LIQUIDATION, source=ex_id, symbol=symbol,
                    data=d
                ))
            return liq
        except Exception:
            return []

    # ── continuous polling loops ───────────────────────────────────────────────

    async def poll_tickers_loop(self, interval_sec: float = 10.0):
        """Continuously poll all exchange tickers."""
        while True:
            tasks = [self.fetch_all_tickers(ex_id) for ex_id in self.exchanges]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            total = sum(r for r in results if isinstance(r, int))
            logger.info(f"🔄 Ticker poll: {total} tickers from {len(self.exchanges)} exchanges")
            await asyncio.sleep(interval_sec)

    async def poll_ohlcv_loop(self, symbols_per_exchange: int = 20,
                               timeframes: Optional[List[str]] = None,
                               interval_sec: float = 60.0):
        """Periodically refresh OHLCV for top symbols on each exchange."""
        tfs = timeframes or ["1m", "5m", "1h"]
        while True:
            for ex_id, ex in self.exchanges.items():
                top_syms = self._top_symbols(ex, symbols_per_exchange)
                for sym in top_syms[:symbols_per_exchange]:
                    for tf in tfs:
                        await self.fetch_ohlcv(ex_id, sym, tf, limit=200)
                        await asyncio.sleep(0.05)
            logger.info(f"✅ OHLCV refresh done ({len(self.exchanges)} exchanges)")
            await asyncio.sleep(interval_sec)

    async def poll_orderbook_loop(self, symbols: Optional[List[str]] = None,
                                   interval_sec: float = 5.0):
        """Poll order books for priority symbols."""
        watch_syms = symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT"]
        while True:
            for ex_id in list(self.exchanges.keys())[:5]:  # top 5 exchanges
                for sym in watch_syms:
                    if sym in self.exchanges[ex_id].symbols:
                        await self.fetch_orderbook(ex_id, sym)
                        await asyncio.sleep(0.1)
            await asyncio.sleep(interval_sec)

    async def poll_funding_loop(self, interval_sec: float = 3600.0):
        """Poll funding rates (every hour)."""
        while True:
            for ex_id in self.exchanges:
                await self.fetch_funding_rates(ex_id)
            await asyncio.sleep(interval_sec)

    async def download_full_history(self, symbol: str, exchange: str = "kraken",
                                    timeframe: str = "1h", days: int = 365) -> List[OHLCVBar]:
        """Download full historical OHLCV for a symbol."""
        ex = self.exchanges.get(exchange)
        if not ex:
            return []
        logger.info(f"📥 Downloading {days}d history: {exchange} {symbol} {timeframe}")
        all_bars: List[OHLCVBar] = []
        since = int((datetime.now(UTC) - timedelta(days=days)).timestamp() * 1000)
        limit = 1000

        while True:
            try:
                loop = asyncio.get_event_loop()
                raw = await loop.run_in_executor(
                    None, lambda: ex.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
                )
                if not raw:
                    break
                for r in raw:
                    all_bars.append(OHLCVBar(
                        symbol=symbol, exchange=exchange, timeframe=timeframe,
                        timestamp=int(r[0]), open=float(r[1]), high=float(r[2]),
                        low=float(r[3]), close=float(r[4]), volume=float(r[5])
                    ))
                if len(raw) < limit:
                    break
                since = raw[-1][0] + 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"History download error: {e}")
                break

        if all_bars:
            self.store.store_ohlcv_batch(all_bars)
            self.store.save_parquet(symbol, exchange, timeframe)
            logger.info(f"✅ Downloaded {len(all_bars)} bars for {exchange}/{symbol}/{timeframe}")
        return all_bars


# ═══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET STREAM ENGINE  (real-time ticks, trades, books)
# ═══════════════════════════════════════════════════════════════════════════════

class WebSocketEngine:
    """
    Manages persistent WebSocket connections to multiple exchanges.
    Auto-reconnects with exponential back-off.
    """

    WS_ENDPOINTS = {
        "kraken_spot": {
            "url": "wss://ws.kraken.com",
            "subscribe": lambda syms: json.dumps({
                "event": "subscribe",
                "pair": [s.replace("/", "") for s in syms[:50]],
                "subscription": {"name": "ticker"}
            }),
            "parse": "_parse_kraken_spot",
        },
        "bybit_linear": {
            "url": "wss://stream.bybit.com/v5/public/linear",
            "subscribe": lambda syms: json.dumps({
                "op": "subscribe",
                "args": [f"tickers.{s.replace('/','')}" for s in syms[:50]]
            }),
            "parse": "_parse_bybit",
        },
        "binance_spot": {
            "url": "wss://stream.binance.com:9443/ws",
            "subscribe": lambda syms: json.dumps({
                "method": "SUBSCRIBE",
                "params": [f"{s.replace('/','').lower()}@ticker" for s in syms[:50]],
                "id": 1
            }),
            "parse": "_parse_binance",
        },
        "okx_spot": {
            "url": "wss://ws.okx.com:8443/ws/v5/public",
            "subscribe": lambda syms: json.dumps({
                "op": "subscribe",
                "args": [{"channel": "tickers", "instId": s.replace("/USDT", "-USDT")}
                         for s in syms[:50]]
            }),
            "parse": "_parse_okx",
        },
    }

    def __init__(self, bus: EventBus, symbols: Optional[List[str]] = None):
        self.bus = bus
        self.symbols = symbols or [
            "BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "ADA/USDT",
            "DOGE/USDT", "MATIC/USDT", "DOT/USDT", "AVAX/USDT", "LINK/USDT",
            "LTC/USDT", "UNI/USDT", "ATOM/USDT", "ETC/USDT", "BCH/USDT",
            "FIL/USDT", "TRX/USDT", "XLM/USDT", "NEAR/USDT", "APT/USDT",
        ]
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._reconnect_delays: Dict[str, float] = defaultdict(lambda: 1.0)

    async def _connect_stream(self, name: str, config: Dict):
        """Single WebSocket stream with auto-reconnect."""
        parser = getattr(self, config["parse"], self._parse_generic)
        while self._running:
            delay = self._reconnect_delays[name]
            try:
                if websockets is None:
                    await asyncio.sleep(60)
                    continue
                logger.info(f"🔌 WS connecting: {name}")
                async with websockets.connect(
                    config["url"],
                    ping_interval=20, ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    # Subscribe
                    sub_msg = config["subscribe"](self.symbols)
                    await ws.send(sub_msg)
                    self._reconnect_delays[name] = 1.0   # reset on success
                    logger.info(f"✅ WS connected: {name}")

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            event = parser(name, msg)
                            if event:
                                await self.bus.publish(event)
                        except Exception as e:
                            logger.debug(f"WS parse error [{name}]: {e}")

            except Exception as e:
                logger.warning(f"⚠️  WS {name} disconnected: {e}. "
                               f"Reconnecting in {delay:.0f}s")
                self._reconnect_delays[name] = min(delay * 2, 60.0)
                await asyncio.sleep(delay)

    # ── parsers ───────────────────────────────────────────────────────────────

    def _parse_kraken_spot(self, source: str, msg: Any) -> Optional[MarketEvent]:
        if not isinstance(msg, list) or len(msg) < 4:
            return None
        data, channel, pair = msg[1], msg[2], msg[3]
        if channel != "ticker":
            return None
        try:
            return MarketEvent(
                type=DataType.TICKER, source="kraken_ws", symbol=pair,
                data={
                    "bid":       float(data["b"][0]),
                    "ask":       float(data["a"][0]),
                    "last":      float(data["c"][0]),
                    "volume_24": float(data["v"][1]),
                    "high_24":   float(data["h"][1]),
                    "low_24":    float(data["l"][1]),
                    "open_24":   float(data["o"][1]),
                }
            )
        except Exception:
            return None

    def _parse_bybit(self, source: str, msg: Any) -> Optional[MarketEvent]:
        if msg.get("topic", "").startswith("tickers."):
            d = msg.get("data", {})
            sym = msg["topic"].replace("tickers.", "").replace("USDT", "/USDT")
            try:
                return MarketEvent(
                    type=DataType.TICKER, source="bybit_ws", symbol=sym,
                    data={
                        "last":          float(d.get("lastPrice", 0)),
                        "bid":           float(d.get("bid1Price", 0)),
                        "ask":           float(d.get("ask1Price", 0)),
                        "volume_24":     float(d.get("volume24h", 0)),
                        "change_24_pct": float(d.get("price24hPcnt", 0)) * 100,
                        "funding_rate":  float(d.get("fundingRate", 0)),
                        "open_interest": float(d.get("openInterest", 0)),
                        "mark_price":    float(d.get("markPrice", 0)),
                    }
                )
            except Exception:
                return None
        return None

    def _parse_binance(self, source: str, msg: Any) -> Optional[MarketEvent]:
        if "e" not in msg or msg["e"] != "24hrTicker":
            return None
        sym = msg.get("s", "").replace("USDT", "/USDT")
        try:
            return MarketEvent(
                type=DataType.TICKER, source="binance_ws", symbol=sym,
                data={
                    "last":          float(msg.get("c", 0)),
                    "bid":           float(msg.get("b", 0)),
                    "ask":           float(msg.get("a", 0)),
                    "open":          float(msg.get("o", 0)),
                    "high":          float(msg.get("h", 0)),
                    "low":           float(msg.get("l", 0)),
                    "volume_24":     float(msg.get("q", 0)),
                    "change_24_pct": float(msg.get("P", 0)),
                    "n_trades":      int(msg.get("n", 0)),
                }
            )
        except Exception:
            return None

    def _parse_okx(self, source: str, msg: Any) -> Optional[MarketEvent]:
        arg = msg.get("arg", {})
        data_list = msg.get("data", [])
        if not data_list or arg.get("channel") != "tickers":
            return None
        d = data_list[0]
        sym = d.get("instId", "").replace("-USDT", "/USDT")
        try:
            return MarketEvent(
                type=DataType.TICKER, source="okx_ws", symbol=sym,
                data={
                    "last":      float(d.get("last", 0)),
                    "bid":       float(d.get("bidPx", 0)),
                    "ask":       float(d.get("askPx", 0)),
                    "volume_24": float(d.get("vol24h", 0)),
                    "open_24":   float(d.get("open24h", 0)),
                    "high_24":   float(d.get("high24h", 0)),
                    "low_24":    float(d.get("low24h", 0)),
                    "oi":        float(d.get("openInterest", 0)),
                }
            )
        except Exception:
            return None

    def _parse_generic(self, source: str, msg: Any) -> Optional[MarketEvent]:
        return None

    # ── start / stop ───────────────────────────────────────────────────────────

    async def start(self, streams: Optional[List[str]] = None):
        self._running = True
        targets = streams or list(self.WS_ENDPOINTS.keys())
        for name in targets:
            cfg = self.WS_ENDPOINTS[name]
            task = asyncio.create_task(self._connect_stream(name, cfg), name=name)
            self._tasks.append(task)
        logger.info(f"🚀 WebSocket engine started: {len(self._tasks)} streams")

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()


# ═══════════════════════════════════════════════════════════════════════════════
#  MACRO & TRADITIONAL MARKET COLLECTOR  (yfinance)
# ═══════════════════════════════════════════════════════════════════════════════

class MacroCollector:
    """
    Collects traditional finance data: indices, commodities, FX, volatility.
    Uses yfinance for clean, free access.
    """

    WATCH_LIST: Dict[str, str] = {
        # Equity indices
        "^GSPC":  "S&P 500",
        "^IXIC":  "Nasdaq",
        "^DJI":   "Dow Jones",
        "^RUT":   "Russell 2000",
        "^FTSE":  "FTSE 100",
        "^GDAXI": "DAX",
        "^N225":  "Nikkei 225",
        # Volatility
        "^VIX":   "VIX",
        "^VXN":   "VXN (Nasdaq Vol)",
        # Commodities
        "GC=F":   "Gold Futures",
        "SI=F":   "Silver Futures",
        "CL=F":   "Crude Oil WTI",
        "BZ=F":   "Brent Crude",
        "NG=F":   "Natural Gas",
        "ZC=F":   "Corn",
        "ZW=F":   "Wheat",
        # FX / DXY
        "DX-Y.NYB": "DXY (USD Index)",
        "EURUSD=X":  "EUR/USD",
        "GBPUSD=X":  "GBP/USD",
        "USDJPY=X":  "USD/JPY",
        "USDCNY=X":  "USD/CNY",
        # Bonds
        "^TNX": "10Y Treasury Yield",
        "^TYX": "30Y Treasury Yield",
        "^FVX": "5Y Treasury Yield",
        "^IRX": "13W T-Bill",
        # Crypto ETFs
        "GBTC":  "Grayscale BTC Trust",
        "BITO":  "ProShares Bitcoin ETF",
        "ETHE":  "Grayscale ETH Trust",
    }

    def __init__(self, bus: EventBus, store: DataStore):
        self.bus = bus
        self.store = store

    async def fetch_snapshot(self) -> Dict[str, Dict]:
        """Fetch latest prices for all macro instruments."""
        if yf is None:
            logger.warning("yfinance not installed — MacroCollector disabled")
            return {}

        tickers_str = " ".join(self.WATCH_LIST.keys())
        result: Dict[str, Dict] = {}
        try:
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None, lambda: yf.download(
                    tickers_str, period="2d", interval="1h",
                    auto_adjust=True, progress=False, threads=True
                )
            )

            for ticker, label in self.WATCH_LIST.items():
                try:
                    if "Close" in data.columns:
                        if hasattr(data["Close"], "columns"):
                            series = data["Close"][ticker].dropna()
                        else:
                            series = data["Close"].dropna()
                    else:
                        continue

                    if len(series) < 2:
                        continue

                    last     = float(series.iloc[-1])
                    prev     = float(series.iloc[-2])
                    chg_pct  = (last / prev - 1) * 100 if prev else 0
                    result[ticker] = {"label": label, "last": last, "change_pct": chg_pct}

                    self.store.store_macro(ticker, "yfinance", last, chg_pct,
                                          {"label": label})
                    await self.bus.publish(MarketEvent(
                        type=DataType.MACRO, source="yfinance", symbol=ticker,
                        data={"label": label, "last": last, "change_pct": chg_pct}
                    ))
                except Exception:
                    pass

            logger.info(f"📊 Macro snapshot: {len(result)}/{len(self.WATCH_LIST)} fetched")
        except Exception as e:
            logger.warning(f"MacroCollector error: {e}")
        return result

    async def fetch_history(self, ticker: str, period: str = "1y",
                            interval: str = "1d") -> Optional[Any]:
        if yf is None or pd is None:
            return None
        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None, lambda: yf.download(
                    ticker, period=period, interval=interval,
                    auto_adjust=True, progress=False
                )
            )
            if df is not None and len(df) > 0:
                logger.info(f"📥 Macro history: {ticker} {len(df)} bars")
            return df
        except Exception as e:
            logger.warning(f"Macro history {ticker}: {e}")
            return None

    async def poll_loop(self, interval_sec: float = 300.0):
        """Poll macro data every 5 minutes."""
        while True:
            await self.fetch_snapshot()
            await asyncio.sleep(interval_sec)


# ═══════════════════════════════════════════════════════════════════════════════
#  SENTIMENT & ON-CHAIN COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class SentimentCollector:
    """
    Fear & Greed Index, on-chain Bitcoin metrics, news sentiment.
    Uses free public APIs only (no key required).
    """

    APIS = {
        "fear_greed":  "https://api.alternative.me/fng/?limit=7",
        "btc_dominance":"https://api.coinpaprika.com/v1/global",
        "coinmarketcap_global": "https://api.coingecko.com/api/v3/global",
        "btc_blockchain": "https://blockchain.info/stats?format=json",
        "crypto_news":  "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=30",
        "btc_mempool":  "https://mempool.space/api/mempool",
        "btc_fees":     "https://mempool.space/api/v1/fees/recommended",
        "btc_price_on_chain": "https://blockchain.info/ticker",
        "eth_gas":      "https://api.etherscan.io/api?module=gastracker&action=gasoracle",
        "top_coins":    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
                        "&order=market_cap_desc&per_page=100&page=1",
        "defi_tvl":     "https://api.llama.fi/protocols",
        "trending":     "https://api.coingecko.com/api/v3/search/trending",
    }

    def __init__(self, bus: EventBus, store: DataStore):
        self.bus = bus
        self.store = store
        self._session: Optional[Any] = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            if aiohttp is None:
                return None
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "NEXUS-DATA-ENGINE/3.0"}
            )
        return self._session

    async def _fetch_json(self, url: str) -> Optional[Dict]:
        session = await self._get_session()
        if session is None:
            # Fallback to httpx
            if httpx_mod is None:
                return None
            try:
                import httpx
                async with httpx.AsyncClient(timeout=15) as c:
                    r = await c.get(url)
                    return r.json()
            except Exception:
                return None
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
        except Exception as e:
            logger.debug(f"HTTP fetch error {url}: {e}")
        return None

    async def fetch_fear_greed(self) -> Dict:
        """Fetch Fear & Greed Index from alternative.me."""
        data = await self._fetch_json(self.APIS["fear_greed"])
        if not data:
            return {}
        try:
            latest = data["data"][0]
            value  = int(latest["value"])
            label  = latest["value_classification"]
            result = {"value": value, "label": label, "timestamp": latest.get("timestamp")}
            self.store.store_sentiment("alternative.me", value, value / 100, label)
            await self.bus.publish(MarketEvent(
                type=DataType.FEAR_GREED, source="alternative.me", symbol="CRYPTO",
                data=result
            ))
            logger.info(f"😱 Fear & Greed: {value} ({label})")
            return result
        except Exception as e:
            logger.debug(f"Fear & Greed parse: {e}")
            return {}

    async def fetch_global_market(self) -> Dict:
        """Fetch global crypto market data from CoinGecko."""
        data = await self._fetch_json(self.APIS["coinmarketcap_global"])
        if not data:
            return {}
        try:
            d = data.get("data", data)
            result = {
                "total_market_cap_usd":    d.get("total_market_cap", {}).get("usd", 0),
                "total_volume_24h_usd":    d.get("total_volume", {}).get("usd", 0),
                "btc_dominance_pct":       d.get("market_cap_percentage", {}).get("btc", 0),
                "eth_dominance_pct":       d.get("market_cap_percentage", {}).get("eth", 0),
                "active_cryptocurrencies": d.get("active_cryptocurrencies", 0),
                "defi_market_cap":         d.get("defi_market_cap", 0),
                "defi_volume_24h":         d.get("defi_24h_vol", 0),
                "defi_to_eth_ratio":       d.get("defi_to_eth_ratio", 0),
            }
            await self.bus.publish(MarketEvent(
                type=DataType.SENTIMENT, source="coingecko", symbol="GLOBAL",
                data=result
            ))
            return result
        except Exception:
            return {}

    async def fetch_top_100_coins(self) -> List[Dict]:
        """Fetch top 100 coins by market cap."""
        data = await self._fetch_json(self.APIS["top_coins"])
        if not data or not isinstance(data, list):
            return []
        coins = []
        for coin in data[:100]:
            c = {
                "id":            coin.get("id"),
                "symbol":        coin.get("symbol", "").upper(),
                "name":          coin.get("name"),
                "price":         float(coin.get("current_price") or 0),
                "market_cap":    float(coin.get("market_cap") or 0),
                "volume_24h":    float(coin.get("total_volume") or 0),
                "change_24h":    float(coin.get("price_change_percentage_24h") or 0),
                "change_7d":     float(coin.get("price_change_percentage_7d_in_currency") or 0),
                "ath":           float(coin.get("ath") or 0),
                "ath_change":    float(coin.get("ath_change_percentage") or 0),
                "rank":          int(coin.get("market_cap_rank") or 0),
            }
            coins.append(c)
            await self.bus.publish(MarketEvent(
                type=DataType.TICKER, source="coingecko",
                symbol=f"{c['symbol']}/USD",
                data=c
            ))
        logger.info(f"📊 Top 100 coins fetched from CoinGecko")
        return coins

    async def fetch_trending(self) -> List[Dict]:
        """Fetch trending coins."""
        data = await self._fetch_json(self.APIS["trending"])
        if not data:
            return []
        try:
            coins = [c["item"] for c in data.get("coins", [])]
            result = [{"name": c["name"], "symbol": c["symbol"],
                       "rank": c.get("market_cap_rank")} for c in coins]
            await self.bus.publish(MarketEvent(
                type=DataType.SENTIMENT, source="coingecko_trending",
                symbol="TRENDING", data={"trending": result}
            ))
            return result
        except Exception:
            return []

    async def fetch_btc_on_chain(self) -> Dict:
        """Bitcoin blockchain metrics."""
        data = await self._fetch_json(self.APIS["btc_blockchain"])
        if not data:
            return {}
        try:
            result = {
                "hash_rate_gh":     float(data.get("hash_rate", 0)),
                "difficulty":       float(data.get("difficulty", 0)),
                "n_tx_per_block":   float(data.get("n_tx_per_block", 0)),
                "minutes_between_blocks": float(data.get("minutes_between_blocks", 0)),
                "n_tx":             int(data.get("n_tx", 0)),
                "total_fees_btc":   float(data.get("total_fees_btc", 0)),
                "miners_revenue":   float(data.get("miners_revenue_usd", 0)),
                "trade_volume":     float(data.get("trade_volume_usd", 0)),
                "market_price_usd": float(data.get("market_price_usd", 0)),
            }
            for k, v in result.items():
                self.store.store_on_chain(k, v)
            await self.bus.publish(MarketEvent(
                type=DataType.ON_CHAIN, source="blockchain.info",
                symbol="BTC", data=result
            ))
            return result
        except Exception:
            return {}

    async def fetch_mempool(self) -> Dict:
        """Bitcoin mempool stats."""
        data = await self._fetch_json(self.APIS["btc_mempool"])
        fees  = await self._fetch_json(self.APIS["btc_fees"])
        result: Dict[str, Any] = {}
        if data:
            result.update({
                "mempool_count":  int(data.get("count", 0)),
                "mempool_vsize":  int(data.get("vsize", 0)),
                "mempool_total_fee_sat": int(data.get("total_fee", 0)),
            })
        if fees:
            result.update({
                "fee_fastest_sat":  int(fees.get("fastestFee", 0)),
                "fee_half_hour_sat":int(fees.get("halfHourFee", 0)),
                "fee_hour_sat":     int(fees.get("hourFee", 0)),
                "fee_economy_sat":  int(fees.get("economyFee", 0)),
            })
        if result:
            await self.bus.publish(MarketEvent(
                type=DataType.ON_CHAIN, source="mempool.space",
                symbol="BTC", data=result
            ))
        return result

    async def fetch_defi_tvl(self) -> Dict:
        """Top DeFi protocols by TVL."""
        data = await self._fetch_json(self.APIS["defi_tvl"])
        if not data or not isinstance(data, list):
            return {}
        top = sorted(data, key=lambda x: x.get("tvl", 0), reverse=True)[:20]
        result = {
            "top_protocols": [
                {"name": p.get("name"), "tvl": float(p.get("tvl", 0)),
                 "chain": p.get("chain"), "category": p.get("category")}
                for p in top
            ],
            "total_tvl": sum(float(p.get("tvl", 0)) for p in data),
        }
        await self.bus.publish(MarketEvent(
            type=DataType.SENTIMENT, source="defillama",
            symbol="DEFI", data=result
        ))
        return result

    async def fetch_crypto_news(self) -> List[Dict]:
        """Latest crypto news + basic sentiment scoring."""
        data = await self._fetch_json(self.APIS["crypto_news"])
        if not data:
            return []
        articles = data.get("Data", [])[:30]
        scored = []
        positive_words = {"bull", "surge", "rally", "gain", "breakout", "ath",
                          "adoption", "partnership", "launch", "upgrade", "approve"}
        negative_words = {"bear", "crash", "drop", "hack", "ban", "sell", "short",
                          "liquidation", "fear", "dump", "fraud", "scam"}
        for art in articles:
            title = art.get("title", "").lower()
            pos = sum(1 for w in positive_words if w in title)
            neg = sum(1 for w in negative_words if w in title)
            score = (pos - neg) / (pos + neg + 1)
            item = {
                "title":    art.get("title", ""),
                "source":   art.get("source", ""),
                "url":      art.get("url", ""),
                "published": art.get("published_on", 0),
                "sentiment_score": round(score, 3),
                "sentiment_label": "positive" if score > 0.1 else (
                    "negative" if score < -0.1 else "neutral"),
                "tags":     art.get("tags", ""),
            }
            scored.append(item)
            self.store.store_sentiment(
                "cryptocompare_news", 50 + int(score * 50),
                score, item["title"][:200]
            )
        await self.bus.publish(MarketEvent(
            type=DataType.NEWS, source="cryptocompare",
            symbol="CRYPTO", data={"articles": scored[:10], "count": len(scored)}
        ))
        avg_score = sum(s["sentiment_score"] for s in scored) / max(len(scored), 1)
        logger.info(f"📰 News: {len(scored)} articles | avg sentiment: {avg_score:+.3f}")
        return scored

    async def fetch_eth_gas(self) -> Dict:
        """Ethereum gas prices (free Etherscan API)."""
        data = await self._fetch_json(self.APIS["eth_gas"])
        if not data or data.get("status") != "1":
            return {}
        r = data.get("result", {})
        result = {
            "safe_gas_gwei":    int(r.get("SafeGasPrice", 0)),
            "propose_gas_gwei": int(r.get("ProposeGasPrice", 0)),
            "fast_gas_gwei":    int(r.get("FastGasPrice", 0)),
            "suggest_base_fee": float(r.get("suggestBaseFee", 0)),
        }
        self.store.store_on_chain("eth_fast_gas", result["fast_gas_gwei"])
        await self.bus.publish(MarketEvent(
            type=DataType.ON_CHAIN, source="etherscan",
            symbol="ETH", data=result
        ))
        return result

    # ── polling loops ─────────────────────────────────────────────────────────

    async def poll_loop(self, interval_sec: float = 300.0):
        """Poll all sentiment/on-chain sources every 5 minutes."""
        while True:
            tasks = [
                self.fetch_fear_greed(),
                self.fetch_global_market(),
                self.fetch_btc_on_chain(),
                self.fetch_mempool(),
                self.fetch_crypto_news(),
                self.fetch_eth_gas(),
                self.fetch_defi_tvl(),
                self.fetch_trending(),
                self.fetch_top_100_coins(),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                logger.debug(f"Sentiment poll errors: {len(errors)}")
            await asyncio.sleep(interval_sec)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  CORRELATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class CorrelationEngine:
    """
    Computes rolling cross-asset correlation matrices.
    Detects regime changes via correlation breakdowns.
    """

    def __init__(self, bus: EventBus, store: DataStore,
                 window: int = 168):   # 168 hours = 7 days
        self.bus = bus
        self.store = store
        self.window = window
        self._price_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    async def on_ticker(self, event: MarketEvent):
        """Feed incoming tickers into price buffers."""
        last = event.data.get("last", 0)
        if last > 0:
            self._price_buffers[event.symbol].append(last)

    async def compute_and_publish(self, symbols: Optional[List[str]] = None):
        """Compute correlation matrix for requested symbols."""
        if pd is None:
            return
        if symbols is None:
            # Use symbols with enough data
            symbols = [s for s, buf in self._price_buffers.items()
                       if len(buf) >= self.window // 2][:30]
        if len(symbols) < 2:
            return
        try:
            prices = {}
            for sym in symbols:
                buf = list(self._price_buffers[sym])
                if len(buf) >= 10:
                    prices[sym] = buf
            if len(prices) < 2:
                return

            # Align lengths
            min_len = min(len(v) for v in prices.values())
            df = pd.DataFrame({k: v[-min_len:] for k, v in prices.items()})
            returns = df.pct_change().dropna()
            corr = returns.corr()

            # Find top correlations
            pairs = []
            for i, a in enumerate(symbols):
                for j, b in enumerate(symbols):
                    if i < j and a in corr.columns and b in corr.columns:
                        c = float(corr.loc[a, b])
                        if not math.isnan(c):
                            pairs.append({"a": a, "b": b, "correlation": round(c, 4)})
            pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

            await self.bus.publish(MarketEvent(
                type=DataType.CORRELATION, source="nexus_corr",
                symbol="MATRIX",
                data={
                    "symbols":       symbols[:20],
                    "top_pairs":     pairs[:20],
                    "window_hours":  self.window,
                    "n_symbols":     len(symbols),
                }
            ))
        except Exception as e:
            logger.debug(f"Correlation compute error: {e}")

    async def poll_loop(self, interval_sec: float = 600.0):
        while True:
            await self.compute_and_publish()
            await asyncio.sleep(interval_sec)


# ═══════════════════════════════════════════════════════════════════════════════
#  REDIS BRIDGE  (distributed pub/sub for multi-process agent dispatch)
# ═══════════════════════════════════════════════════════════════════════════════

class RedisBridge:
    """
    Mirrors the local EventBus to Redis pub/sub channels.
    Enables multiple agent processes to subscribe to the data stream.
    Channel naming: nexus:<data_type>:<symbol>
    """

    def __init__(self, bus: EventBus, host: str = "localhost",
                 port: int = 6379, db: int = 0):
        self.bus = bus
        self.host, self.port, self.db = host, port, db
        self._redis = None
        self._available = redis_mod is not None

    def _get_redis(self):
        if not self._available:
            return None
        if self._redis is None:
            try:
                import redis
                self._redis = redis.Redis(
                    host=self.host, port=self.port, db=self.db,
                    socket_timeout=2, socket_connect_timeout=2,
                    decode_responses=True,
                )
                self._redis.ping()
                logger.info(f"✅ Redis connected: {self.host}:{self.port}")
            except Exception as e:
                logger.warning(f"Redis unavailable: {e}")
                self._redis = None
        return self._redis

    async def bridge_loop(self):
        """Subscribe to ALL bus events and mirror to Redis."""
        q = await self.bus.subscribe()
        r = self._get_redis()
        while True:
            try:
                event: MarketEvent = await asyncio.wait_for(q.get(), timeout=5.0)
                if r:
                    channel = f"nexus:{event.type.value}:{event.symbol}"
                    r.publish(channel, json.dumps(event.to_dict()))
                    # Also store latest in hash
                    r.hset(f"nexus:latest:{event.symbol}",
                           mapping={event.type.value: json.dumps(event.data)})
                    r.expire(f"nexus:latest:{event.symbol}", 3600)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.debug(f"Redis bridge error: {e}")
                r = self._get_redis()   # reconnect

    async def get_latest(self, symbol: str) -> Dict:
        r = self._get_redis()
        if not r:
            return {}
        try:
            raw = r.hgetall(f"nexus:latest:{symbol}")
            return {k: json.loads(v) for k, v in raw.items()}
        except Exception:
            return {}


# ═══════════════════════════════════════════════════════════════════════════════
#  AGENT DISPATCHER  (routes data to NEXUS / Kraken Ultra / any agent callback)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class AgentEndpoint:
    name:       str
    callback:   Callable[[MarketEvent], Awaitable[None]]
    types:      Set[DataType]   = field(default_factory=set)  # empty = all
    symbols:    Set[str]        = field(default_factory=set)  # empty = all
    min_interval_ms: float      = 0.0     # rate-limit per agent
    _last_called: float         = field(default=0.0, repr=False)


class AgentDispatcher:
    """
    Routes MarketEvents to registered agent callbacks.
    Supports type filtering, symbol filtering, and per-agent rate-limiting.
    """

    def __init__(self, bus: EventBus):
        self.bus = bus
        self._agents: Dict[str, AgentEndpoint] = {}
        self._stats: Dict[str, int] = defaultdict(int)

    def register(self, name: str, callback: Callable,
                 types: Optional[List[DataType]] = None,
                 symbols: Optional[List[str]] = None,
                 min_interval_ms: float = 0.0):
        ep = AgentEndpoint(
            name=name,
            callback=callback,
            types=set(types or []),
            symbols=set(symbols or []),
            min_interval_ms=min_interval_ms,
        )
        self._agents[name] = ep
        logger.info(f"🤖 Agent registered: {name} "
                    f"(types={len(ep.types) or 'ALL'}, "
                    f"symbols={len(ep.symbols) or 'ALL'})")

    def unregister(self, name: str):
        self._agents.pop(name, None)

    async def dispatch_loop(self):
        """Listen to bus and dispatch to all registered agents."""
        q = await self.bus.subscribe()
        while True:
            try:
                event: MarketEvent = await asyncio.wait_for(q.get(), timeout=10.0)
                for name, agent in list(self._agents.items()):
                    # Type filter
                    if agent.types and event.type not in agent.types:
                        continue
                    # Symbol filter
                    if agent.symbols and event.symbol not in agent.symbols:
                        continue
                    # Rate limit
                    now = time.monotonic() * 1000
                    if (now - agent._last_called) < agent.min_interval_ms:
                        continue
                    agent._last_called = now
                    # Dispatch (fire-and-forget with error isolation)
                    try:
                        asyncio.create_task(
                            agent.callback(event),
                            name=f"dispatch_{name}_{event.type.value}"
                        )
                        self._stats[name] += 1
                    except Exception as e:
                        logger.warning(f"Dispatch error [{name}]: {e}")
            except asyncio.TimeoutError:
                pass

    def stats(self) -> Dict:
        return dict(self._stats)


# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI BROADCAST SERVER  (WebSocket + REST for dashboards / frontends)
# ═══════════════════════════════════════════════════════════════════════════════

def create_api_server(engine: "NexusDataEngine") -> Any:
    if fastapi_mod is None:
        return None
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse

    app = FastAPI(title="NEXUS Data Engine API", version="3.0.0")
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["*"], allow_headers=["*"])

    _ws_clients: List[WebSocket] = []

    @app.websocket("/ws")
    async def ws_broadcast(websocket: WebSocket):
        """Stream all market events to browser/dashboard clients."""
        await websocket.accept()
        _ws_clients.append(websocket)
        q = await engine.bus.subscribe()
        try:
            while True:
                event = await asyncio.wait_for(q.get(), timeout=30.0)
                await websocket.send_text(json.dumps(event.to_dict()))
        except (WebSocketDisconnect, asyncio.TimeoutError, Exception):
            pass
        finally:
            await engine.bus.unsubscribe(q)
            if websocket in _ws_clients:
                _ws_clients.remove(websocket)

    @app.websocket("/ws/{symbol}")
    async def ws_symbol(websocket: WebSocket, symbol: str):
        """Stream events for a specific symbol."""
        await websocket.accept()
        q = await engine.bus.subscribe(DataType.TICKER, DataType.ORDERBOOK,
                                       DataType.INDICATOR)
        try:
            while True:
                event = await asyncio.wait_for(q.get(), timeout=30.0)
                if event.symbol == symbol or not symbol:
                    await websocket.send_text(json.dumps(event.to_dict()))
        except Exception:
            pass
        finally:
            await engine.bus.unsubscribe(q)

    @app.get("/")
    def root():
        return {
            "status": "NEXUS Data Engine v3.0 — ONLINE",
            "bus_stats": engine.bus.stats(),
            "dispatcher_stats": engine.dispatcher.stats(),
            "exchanges_loaded": list(engine.exchange_collector.exchanges.keys()),
        }

    @app.get("/tickers")
    def tickers(limit: int = 100):
        return engine.store.get_latest_tickers(limit)

    @app.get("/ohlcv/{exchange}/{symbol}/{timeframe}")
    def ohlcv(exchange: str, symbol: str, timeframe: str, limit: int = 500):
        df = engine.store.get_ohlcv(symbol.replace("_", "/"), exchange, timeframe, limit)
        if df is None:
            return JSONResponse({"error": "no data"}, status_code=404)
        return df.to_dict(orient="records")

    @app.get("/sentiment/fear_greed")
    def fear_greed():
        return engine.store.get_fear_greed_history(30)

    @app.get("/exchanges")
    def exchanges():
        return {
            "loaded": list(engine.exchange_collector.exchanges.keys()),
            "configured": list(engine.exchange_collector.EXCHANGE_CONFIGS.keys()),
        }

    @app.post("/subscribe/ohlcv")
    async def subscribe_ohlcv(exchange: str, symbol: str,
                               timeframe: str = "1h", limit: int = 500):
        bars = await engine.exchange_collector.fetch_ohlcv(
            exchange, symbol, timeframe, limit
        )
        return {"bars": len(bars), "symbol": symbol, "exchange": exchange}

    @app.post("/download_history")
    async def download_history(exchange: str, symbol: str,
                                timeframe: str = "1h", days: int = 90):
        bars = await engine.exchange_collector.download_full_history(
            symbol, exchange, timeframe, days
        )
        return {"downloaded": len(bars)}

    @app.get("/macro")
    async def macro():
        return await engine.macro_collector.fetch_snapshot()

    @app.get("/on_chain")
    async def on_chain():
        rows = engine.store.conn.execute(
            "SELECT metric, value, ts FROM on_chain ORDER BY ts DESC LIMIT 100"
        ).fetchall()
        return [{"metric": r[0], "value": r[1], "ts": r[2]} for r in rows]

    @app.get("/correlation")
    async def correlation():
        await engine.correlation_engine.compute_and_publish()
        return {"status": "computed and published"}

    @app.get("/redis/latest/{symbol}")
    async def redis_latest(symbol: str):
        return await engine.redis_bridge.get_latest(symbol)

    @app.get("/indicators/{exchange}/{symbol}/{timeframe}")
    def indicators(exchange: str, symbol: str, timeframe: str):
        row = engine.store.conn.execute(
            "SELECT indicators FROM indicators WHERE symbol=? AND exchange=? "
            "AND timeframe=? ORDER BY ts DESC LIMIT 1",
            (symbol.replace("_", "/"), exchange, timeframe)
        ).fetchone()
        if row:
            return json.loads(row[0])
        return JSONResponse({"error": "no indicators"}, status_code=404)

    @app.get("/bus/stats")
    def bus_stats():
        return engine.bus.stats()

    return app


# ═══════════════════════════════════════════════════════════════════════════════
#  NEXUS DATA ENGINE  — Central Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

class NexusDataEngine:
    """
    The single entry point. Starts all collectors, wires them to the event bus,
    and dispatches to registered agent callbacks.

    Usage:
        engine = NexusDataEngine(api_keys={...})
        engine.register_agent("my_bot", my_callback)
        await engine.start()
    """

    def __init__(
        self,
        api_keys:           Optional[Dict[str, Dict]] = None,
        enabled_exchanges:  Optional[List[str]]       = None,
        ws_symbols:         Optional[List[str]]       = None,
        redis_host:         str   = "localhost",
        redis_port:         int   = 6379,
        ticker_interval:    float = 10.0,
        ohlcv_interval:     float = 60.0,
        sentiment_interval: float = 300.0,
        macro_interval:     float = 300.0,
        enable_ws:          bool  = True,
        enable_redis:       bool  = True,
        enable_api:         bool  = True,
        api_port:           int   = 9000,
    ):
        self.api_keys           = api_keys or self._load_api_keys_from_env()
        self.ticker_interval    = ticker_interval
        self.ohlcv_interval     = ohlcv_interval
        self.sentiment_interval = sentiment_interval
        self.macro_interval     = macro_interval
        self.enable_ws          = enable_ws
        self.enable_redis       = enable_redis
        self.enable_api         = enable_api
        self.api_port           = api_port

        # Core components
        self.bus                 = EventBus(buffer_size=4096)
        self.store               = DataStore()
        self.exchange_collector  = ExchangeCollector(
            self.bus, self.store, enabled_exchanges, self.api_keys
        )
        self.ws_engine           = WebSocketEngine(self.bus, ws_symbols)
        self.macro_collector     = MacroCollector(self.bus, self.store)
        self.sentiment_collector = SentimentCollector(self.bus, self.store)
        self.indicator_engine    = IndicatorEngine()
        self.correlation_engine  = CorrelationEngine(self.bus, self.store)
        self.redis_bridge        = RedisBridge(self.bus, redis_host, redis_port)
        self.dispatcher          = AgentDispatcher(self.bus)

        self._tasks: List[asyncio.Task] = []
        self._running = False

        logger.info("🌌 NexusDataEngine v3.0 initialised")

    @staticmethod
    def _load_api_keys_from_env() -> Dict[str, Dict]:
        """Load exchange API keys from environment variables."""
        keys: Dict[str, Dict] = {}
        for ex in ["kraken", "bybit", "binance", "okx", "kucoin", "coinbasepro"]:
            key_var    = f"{ex.upper()}_API_KEY"
            secret_var = f"{ex.upper()}_SECRET"
            passphrase = f"{ex.upper()}_PASSPHRASE"
            if os.getenv(key_var):
                keys[ex] = {
                    "apiKey": os.getenv(key_var),
                    "secret": os.getenv(secret_var, ""),
                }
                if os.getenv(passphrase):
                    keys[ex]["password"] = os.getenv(passphrase)
        return keys

    # ── agent registration (public API) ───────────────────────────────────────

    def register_agent(self, name: str, callback: Callable,
                       types: Optional[List[DataType]] = None,
                       symbols: Optional[List[str]]    = None,
                       min_interval_ms: float           = 0.0):
        """
        Register any agent/strategy/function to receive market events.
        callback signature: async def my_handler(event: MarketEvent) -> None

        Examples:
            engine.register_agent("nexus_evolution", nexus_on_data)
            engine.register_agent("kraken_ultra", kraken_on_ticker,
                                  types=[DataType.TICKER],
                                  symbols=["BTC/USDT", "ETH/USDT"])
        """
        self.dispatcher.register(name, callback, types, symbols, min_interval_ms)

    def unregister_agent(self, name: str):
        self.dispatcher.unregister(name)

    # ── start / stop ───────────────────────────────────────────────────────────

    async def start(self, quick: bool = False):
        """
        Start all data collection pipelines.
        quick=True: skip OHLCV/history downloads (faster startup for testing).
        """
        self._running = True
        logger.info("🚀 Starting NEXUS Data Engine...")

        # Init exchanges
        await self.exchange_collector.init_exchanges()

        # Define all concurrent tasks
        task_defs = [
            # Exchange REST polling
            ("poll_tickers",    self.exchange_collector.poll_tickers_loop(self.ticker_interval)),
            ("poll_funding",    self.exchange_collector.poll_funding_loop(3600)),
            ("poll_orderbook",  self.exchange_collector.poll_orderbook_loop(interval_sec=5)),
            # Sentiment / on-chain
            ("poll_sentiment",  self.sentiment_collector.poll_loop(self.sentiment_interval)),
            # Macro
            ("poll_macro",      self.macro_collector.poll_loop(self.macro_interval)),
            # Correlation
            ("poll_corr",       self.correlation_engine.poll_loop(600)),
            # Dispatcher
            ("dispatcher",      self.dispatcher.dispatch_loop()),
        ]

        if not quick:
            task_defs.append(
                ("poll_ohlcv", self.exchange_collector.poll_ohlcv_loop(
                    symbols_per_exchange=30,
                    timeframes=["1m", "5m", "15m", "1h"],
                    interval_sec=self.ohlcv_interval,
                ))
            )

        if self.enable_ws:
            task_defs.append(("ws_engine", self.ws_engine.start()))

        if self.enable_redis:
            task_defs.append(("redis_bridge", self.redis_bridge.bridge_loop()))

        # Correlation feeds from tickers
        ticker_q = await self.bus.subscribe(DataType.TICKER)

        async def _corr_feed():
            while True:
                ev = await ticker_q.get()
                await self.correlation_engine.on_ticker(ev)

        task_defs.append(("corr_feed", _corr_feed()))

        # FastAPI server
        if self.enable_api and fastapi_mod is not None and uvicorn_mod is not None:
            app = create_api_server(self)
            import uvicorn
            config = uvicorn.Config(app, host="0.0.0.0", port=self.api_port,
                                    log_level="warning")
            server = uvicorn.Server(config)
            task_defs.append(("api_server", server.serve()))

        # Launch all tasks
        for name, coro in task_defs:
            t = asyncio.create_task(coro, name=name)
            self._tasks.append(t)
            logger.info(f"  ▶ Task started: {name}")

        logger.info(f"✅ NEXUS Data Engine ONLINE — {len(self._tasks)} tasks running")

        # Initial fast fetches (don't block)
        asyncio.create_task(self._initial_snapshot())

    async def _initial_snapshot(self):
        """Immediately fetch a first snapshot of all sources."""
        await asyncio.sleep(2)   # let exchanges init
        await asyncio.gather(
            self.sentiment_collector.fetch_fear_greed(),
            self.sentiment_collector.fetch_global_market(),
            self.sentiment_collector.fetch_top_100_coins(),
            self.macro_collector.fetch_snapshot(),
            return_exceptions=True
        )

    async def stop(self):
        self._running = False
        await self.ws_engine.stop()
        await self.sentiment_collector.close()
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        logger.info("🛑 NEXUS Data Engine stopped")

    async def run_forever(self):
        """Block forever, handling Ctrl+C gracefully."""
        await self.start()
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ── convenience API ───────────────────────────────────────────────────────

    async def get_snapshot(self, symbol: str = "BTC/USDT",
                           exchange: str = "kraken") -> Dict:
        """Get a complete data snapshot for one symbol."""
        ticker_rows = self.store.get_latest_tickers(500)
        ticker = next((t for t in ticker_rows
                       if t["symbol"] == symbol and t["exchange"] == exchange), {})
        ohlcv_1h = self.store.get_ohlcv(symbol, exchange, "1h", 200)
        ind = {}
        if ohlcv_1h is not None and len(ohlcv_1h) >= 50:
            ind = self.indicator_engine.compute(ohlcv_1h)
            vol = self.indicator_engine.compute_volatility(ohlcv_1h["close"])
            ind.update(vol)
        fear_greed = self.store.get_fear_greed_history(1)
        redis_data = await self.redis_bridge.get_latest(symbol)

        return {
            "symbol":     symbol,
            "exchange":   exchange,
            "ticker":     ticker,
            "indicators": ind,
            "fear_greed": fear_greed[0] if fear_greed else None,
            "redis":      redis_data,
            "timestamp":  _TS(),
        }

    async def get_full_universe(self) -> Dict[str, List[str]]:
        """Return all available symbols per exchange."""
        return {
            ex_id: ex.symbols[:200]
            for ex_id, ex in self.exchange_collector.exchanges.items()
        }

    async def download_dataset(self, symbols: List[str],
                                exchange: str = "kraken",
                                timeframe: str = "1h",
                                days: int = 90) -> Dict[str, int]:
        """Bulk download historical data for a list of symbols."""
        results = {}
        for sym in symbols:
            bars = await self.exchange_collector.download_full_history(
                sym, exchange, timeframe, days
            )
            results[sym] = len(bars)
            self.store.save_parquet(sym, exchange, timeframe)
            await asyncio.sleep(0.5)   # be polite to the exchange
        return results


# ═══════════════════════════════════════════════════════════════════════════════
#  INTEGRATION HELPERS  (pre-built callbacks for NEXUS / Kraken Ultra)
# ═══════════════════════════════════════════════════════════════════════════════

class NexusIntegration:
    """
    Pre-built integration callbacks.
    Connects NexusDataEngine to your agent systems with a single line.
    """

    @staticmethod
    def make_nexus_callback(nexus_engine: Any) -> Callable:
        """
        Create a callback that feeds MarketEvents into a NexusGenesisEngine.
        The nexus engine evaluator will use live data instead of synthetic GBM.
        """
        async def callback(event: MarketEvent):
            try:
                # Feed ticker data into nexus evaluator's price buffer
                if event.type == DataType.TICKER and hasattr(nexus_engine, "evaluator"):
                    nexus_engine.evaluator._last_live_price = event.data.get("last", 0)

                # Feed indicator signals to current best genome's evaluation
                if event.type == DataType.INDICATOR and hasattr(nexus_engine, "best_genome"):
                    bg = nexus_engine.best_genome
                    if bg is not None:
                        sma_cross = event.data.get("sma_cross_up", 0)
                        rsi = event.data.get("RSI_14", 50)
                        ema_bull = event.data.get("ema_cross_up", 0)
                        signal = sma_cross and rsi < bg.genes.get("rsi_overbought", 70) and ema_bull
                        if signal:
                            logger.debug(f"🧬 Nexus signal: BUY {event.symbol} "
                                         f"(RSI={rsi:.1f})")

                # Feed fear & greed to risk modulation
                if event.type == DataType.FEAR_GREED:
                    fg = event.data.get("value", 50)
                    if hasattr(nexus_engine, "best_genome") and nexus_engine.best_genome:
                        if fg < 20:   # Extreme fear → reduce risk
                            nexus_engine.best_genome.genes["risk_percent"] = min(
                                nexus_engine.best_genome.genes.get("risk_percent", 1.0),
                                0.5
                            )
                        elif fg > 80:  # Extreme greed → tighten stops
                            nexus_engine.best_genome.genes["take_profit_pct"] = max(
                                nexus_engine.best_genome.genes.get("take_profit_pct", 2.0),
                                1.5
                            )
            except Exception as e:
                logger.debug(f"NexusCallback error: {e}")

        return callback

    @staticmethod
    def make_kraken_ultra_callback(strategy_fn: Callable) -> Callable:
        """
        Create a callback that routes tickers + indicators to a
        Kraken Ultra strategy function.
        strategy_fn signature: (symbol, price, indicators, orderbook) -> Optional[str] (BUY/SELL/None)
        """
        _last_ob: Dict[str, Dict] = {}
        _last_ind: Dict[str, Dict] = {}

        async def callback(event: MarketEvent):
            if event.type == DataType.ORDERBOOK:
                _last_ob[event.symbol] = event.data
            elif event.type == DataType.INDICATOR:
                _last_ind[event.symbol] = event.data
            elif event.type == DataType.TICKER:
                price = event.data.get("last", 0)
                if price > 0:
                    decision = strategy_fn(
                        event.symbol, price,
                        _last_ind.get(event.symbol, {}),
                        _last_ob.get(event.symbol, {}),
                    )
                    if decision:
                        logger.info(f"🦑 Kraken Ultra signal: {decision} "
                                    f"{event.symbol} @ {price}")

        return callback

    @staticmethod
    def make_logging_agent(name: str = "LOGGER") -> Callable:
        """Simple logging agent to monitor data flow."""
        _count: Dict[str, int] = defaultdict(int)
        _last_log = [time.time()]

        async def callback(event: MarketEvent):
            _count[event.type.value] += 1
            now = time.time()
            if now - _last_log[0] > 30:
                total = sum(_count.values())
                logger.info(f"📡 [{name}] 30s stats: "
                            + " | ".join(f"{k}={v}" for k, v in sorted(_count.items())))
                _count.clear()
                _last_log[0] = now

        return callback


# ═══════════════════════════════════════════════════════════════════════════════
#  DEMO / ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def demo():
    """
    Full demo: start data engine, register a logging agent,
    run for 60 seconds, print snapshot.
    """
    logger.info("=" * 70)
    logger.info("  NEXUS OMNIBUS DATA ENGINE v3.0 — DEMO")
    logger.info("=" * 70)

    # Example: load API keys from environment
    # export KRAKEN_API_KEY=xxx KRAKEN_SECRET=xxx
    engine = NexusDataEngine(
        enabled_exchanges=["kraken", "bybit", "binance"],
        ws_symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT"],
        ticker_interval=15.0,
        sentiment_interval=120.0,
        macro_interval=300.0,
        enable_ws=True,
        enable_redis=False,   # set True if Redis is available
        enable_api=True,
        api_port=9000,
    )

    # Register a logging agent (no-op, just counts events)
    engine.register_agent("monitor", NexusIntegration.make_logging_agent("MONITOR"))

    # Example: register a custom strategy callback
    async def my_strategy(event: MarketEvent):
        if event.type == DataType.TICKER and event.symbol == "BTC/USDT":
            price = event.data.get("last", 0)
            if price > 0:
                logger.debug(f"BTC/USDT: ${price:,.2f} via {event.source}")

    engine.register_agent(
        "btc_watcher", my_strategy,
        types=[DataType.TICKER], symbols=["BTC/USDT"],
        min_interval_ms=1000,
    )

    # Start (quick mode = skip OHLCV batch polling)
    await engine.start(quick=True)

    logger.info("⏱  Running for 60 seconds... (Ctrl+C to stop)")
    logger.info(f"📡 REST API: http://localhost:{engine.api_port}")
    logger.info(f"📡 WS feed:  ws://localhost:{engine.api_port}/ws")
    logger.info(f"📡 BTC feed: ws://localhost:{engine.api_port}/ws/BTC%2FUSDT")

    await asyncio.sleep(30)

    # Print snapshot after data flows in
    logger.info("\n📊 SNAPSHOT:")
    snap = await engine.get_snapshot("BTC/USDT", "kraken")
    logger.info(json.dumps(snap, indent=2, default=str))

    logger.info("\n🌐 Bus stats: " + json.dumps(engine.bus.stats()))
    logger.info("🤖 Dispatch stats: " + json.dumps(engine.dispatcher.stats()))

    # Universe
    universe = await engine.get_full_universe()
    total_syms = sum(len(v) for v in universe.values())
    logger.info(f"\n🪐 Symbol universe: {total_syms} symbols across "
                f"{len(universe)} exchanges")
    for ex, syms in universe.items():
        logger.info(f"  {ex}: {len(syms)} symbols")

    await engine.stop()
    logger.info("✅ Demo complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NEXUS Data Engine v3.0")
    parser.add_argument("--mode", choices=["demo", "server", "download"],
                        default="demo")
    parser.add_argument("--exchanges", nargs="+",
                        default=["kraken", "bybit", "binance"])
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--symbol", type=str, default="BTC/USDT")
    parser.add_argument("--exchange", type=str, default="kraken")
    parser.add_argument("--timeframe", type=str, default="1h")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--no-ws", action="store_true")
    parser.add_argument("--no-api", action="store_true")
    parser.add_argument("--redis", action="store_true")
    args = parser.parse_args()

    if args.mode == "demo":
        asyncio.run(demo())

    elif args.mode == "server":
        async def run_server():
            engine = NexusDataEngine(
                enabled_exchanges=args.exchanges,
                enable_ws=not args.no_ws,
                enable_api=not args.no_api,
                enable_redis=args.redis,
                api_port=args.port,
            )
            engine.register_agent(
                "monitor", NexusIntegration.make_logging_agent()
            )
            await engine.run_forever()
        asyncio.run(run_server())

    elif args.mode == "download":
        async def run_download():
            engine = NexusDataEngine(
                enabled_exchanges=[args.exchange],
                enable_ws=False, enable_api=False, enable_redis=False,
            )
            await engine.exchange_collector.init_exchanges()
            bars = await engine.exchange_collector.download_full_history(
                args.symbol, args.exchange, args.timeframe, args.days
            )
            logger.info(f"✅ Downloaded {len(bars)} bars → Parquet saved")
        asyncio.run(run_download())
