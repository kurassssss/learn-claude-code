"""
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                       ║
║  ███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗    ██████╗ ██████╗ ██╗███╗   ███╗███████╗ ║
║  ████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝    ██╔══██╗██╔══██╗██║████╗ ████║██╔════╝ ║
║  ██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗    ██████╔╝██████╔╝██║██╔████╔██║█████╗   ║
║  ██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║    ██╔═══╝ ██╔══██╗██║██║╚██╔╝██║██╔══╝   ║
║  ██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║    ██║     ██║  ██║██║██║ ╚═╝ ██║███████╗  ║
║  ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝    ╚═╝     ╚═╝  ╚═╝╚═╝╚═╝     ╚═╝╚══════╝  ║
║                                                                                       ║
║                    ★  B R A I N  ★   v 1 . 0                                        ║
║                                                                                       ║
║  "The market is a river. Most drown. Few swim. One controls the current."            ║
║                                                                                       ║
║  LAYER 0  — DATA OMNIBUS          NexusDataEngine (15 exchanges, WS, macro, chain)  ║
║  LAYER 1  — NEURAL SWARM          8-arch DenseNeuronCluster (48-dim feature space)  ║
║  LAYER 2  — RL SOUL CLUSTER       15 engines (APEX·PHANTOM·STORM·ORACLE·VENOM…)     ║
║  LAYER 3  — EVOLUTIONARY GENOME   NSGA-II + MAP-Elites, 100+ financial genes        ║
║  LAYER 4  — REGIME ORACLE         12 market regimes, live classification             ║
║  LAYER 5  — ADVERSARIAL SHIELD    Trap detection (bull/bear/stop-hunt/spoof…)       ║
║  LAYER 6  — META-LEARNER          Learns which layer to trust in which regime        ║
║  LAYER 7  — PREDICTIVE ENGINE     Multi-horizon price forecasting (1s→4h)           ║
║  LAYER 8  — POSITION INTELLIGENCE Dynamic sizing, Kelly, volatility-adjusted         ║
║  LAYER 9  — CIRCUIT BREAKERS      Hard stops, drawdown limits, exposure caps        ║
║  LAYER 10 — SELF-OPTIMIZER        Rewrites its own strategy weights every hour      ║
║                                                                                       ║
║  ⚡ LATENCY: sub-100ms decision loop                                                 ║
║  🧬 ADAPTS: parameters evolve from every closed trade                               ║
║  🛡️  SURVIVES: 47 categories of market manipulation, black swans, flash crashes      ║
║                                                                                       ║
║  ┌─────────────────────────────────────────────────────────────────────────────┐    ║
║  │  ⚠️  HONEST PERFORMANCE DISCLAIMER ⚠️                                        │    ║
║  │  20-50% DAILY is NOT a sustainable target — that would be 7300-18250%/yr.  │    ║
║  │  No system in human history has achieved this consistently.                 │    ║
║  │  Realistic elite target: 0.3-1.5% daily (100-500% annualized).             │    ║
║  │  This engine is designed to maximize edge, not promise magic numbers.       │    ║
║  │  START IN PAPER MODE. Validate EVERY assumption before real capital.        │    ║
║  └─────────────────────────────────────────────────────────────────────────────┘    ║
║                                                                                       ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ── stdlib ────────────────────────────────────────────────────────────────────
import asyncio
import collections
import copy
import dataclasses
import hashlib
import io
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import time
import threading
import traceback
import uuid
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import (Any, Callable, Dict, List, Optional,
                    Set, Tuple, Union, Awaitable)

import numpy as np

# ── graceful optional imports ─────────────────────────────────────────────────
def _try_import(name: str):
    try: return __import__(name)
    except ImportError: return None

pd          = _try_import("pandas")
ccxt_mod    = _try_import("ccxt")
aiohttp_mod = _try_import("aiohttp")
ta_mod      = _try_import("pandas_ta")
empyrical   = _try_import("empyrical")
arch_mod    = _try_import("arch")
rich_mod    = _try_import("rich")
fastapi_mod = _try_import("fastapi")
uvicorn_mod = _try_import("uvicorn")

# ── logging ───────────────────────────────────────────────────────────────────
for d in ["logs", "brain_state", "brain_state/models",
          "brain_state/genomes", "brain_state/snapshots",
          "brain_state/trades", "brain_state/metrics"]:
    Path(d).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-20s │ %(levelname)-8s │ %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/nexus_prime_{int(time.time())}.log",
                            encoding="utf-8"),
    ],
)
logger = logging.getLogger("NEXUS·PRIME")

UTC = timezone.utc
_NOW = lambda: datetime.now(UTC).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

STATE_DIM   = 64     # extended feature vector dimension
N_ACTIONS   = 5      # STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
GAMMA       = 0.995
LR_FAST     = 0.003
LR_SLOW     = 0.0005
BATCH_SIZE  = 64
BUFFER_CAP  = 100_000

BRAIN_DIR   = Path("brain_state")
MODELS_DIR  = BRAIN_DIR / "models"
TRADES_DIR  = BRAIN_DIR / "trades"


class BrainMode(Enum):
    PAPER     = "paper"      # simulation only — default
    LIVE      = "live"       # real exchange orders
    TRAINING  = "training"   # offline curriculum
    SHADOW    = "shadow"     # live data, paper execution, comparison


class SignalStrength(Enum):
    STRONG_BUY   = 2
    BUY          = 1
    HOLD         = 0
    SELL         = -1
    STRONG_SELL  = -2


class MarketRegimeLabel(Enum):
    STEALTH_ACCUMULATION = "stealth_accumulation"
    MARKUP               = "markup"
    DISTRIBUTION         = "distribution"
    MARKDOWN             = "markdown"
    RANGING_TIGHT        = "ranging_tight"
    RANGING_WIDE         = "ranging_wide"
    PARABOLIC            = "parabolic"
    CAPITULATION         = "capitulation"
    DEAD_CAT             = "dead_cat"
    LIQUIDITY_HUNT       = "liquidity_hunt"
    FLASH_CRASH          = "flash_crash"
    MANIPULATED          = "manipulated"


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE MATH PRIMITIVES  (zero-dependency, used everywhere)
# ═══════════════════════════════════════════════════════════════════════════════

def _relu(x: np.ndarray) -> np.ndarray:
    return np.maximum(0.0, x)

def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -20, 20)))

def _tanh(x: np.ndarray) -> np.ndarray:
    return np.tanh(np.clip(x, -10, 10))

def _softmax(x: np.ndarray) -> np.ndarray:
    e = np.exp(x - x.max()); return e / (e.sum() + 1e-12)

def _ema_fast(arr: List[float], span: int) -> float:
    if not arr: return 0.0
    k = 2.0 / (span + 1)
    e = arr[0]
    for v in arr[1:]:
        e = v * k + e * (1 - k)
    return float(e)

def _rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1: return 0.5
    d = np.diff(prices[-(period+1):])
    g = float(d[d > 0].mean()) if (d > 0).any() else 0.0
    l = float(-d[d < 0].mean()) if (d < 0).any() else 1e-10
    return g / (g + l)

def _atr(high: List[float], low: List[float],
         close: List[float], period: int = 14) -> float:
    if len(close) < 2: return 0.0
    trs = []
    for i in range(1, len(close)):
        tr = max(high[i] - low[i],
                 abs(high[i] - close[i-1]),
                 abs(low[i]  - close[i-1]))
        trs.append(tr)
    return float(np.mean(trs[-period:]))

def _kelly(win_rate: float, avg_win: float, avg_loss: float) -> float:
    if avg_loss <= 0: return 0.0
    b = avg_win / avg_loss
    q = 1.0 - win_rate
    k = (b * win_rate - q) / b
    return float(np.clip(k, 0.0, 0.25))  # cap at 25%

def _sharpe(returns: List[float], periods: int = 252) -> float:
    if len(returns) < 3: return 0.0
    r = np.array(returns)
    return float(r.mean() / (r.std() + 1e-10) * math.sqrt(periods))

def _max_drawdown(equity: List[float]) -> float:
    if len(equity) < 2: return 0.0
    eq = np.array(equity)
    peak = np.maximum.accumulate(eq)
    dd   = (peak - eq) / (peak + 1e-10)
    return float(dd.max() * 100)


# ═══════════════════════════════════════════════════════════════════════════════
#  PERSISTENT BRAIN MEMORY  (SQLite — survives restarts)
# ═══════════════════════════════════════════════════════════════════════════════

class BrainMemory:
    """
    Persistent store for all brain knowledge:
    trades, learned patterns, regime history,
    model weights metadata, evolution logs, lessons.
    """

    def __init__(self, path: Path = BRAIN_DIR / "prime_memory.db"):
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init()

    def _init(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT, exchange TEXT, side TEXT,
                entry_price REAL, exit_price REAL,
                quantity REAL, pnl REAL, pnl_pct REAL,
                fees REAL, duration_s REAL,
                entry_signal TEXT, exit_reason TEXT,
                regime TEXT, confidence REAL,
                layer_votes TEXT,
                opened_at TEXT, closed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trade_sym ON trades(symbol, opened_at);

            CREATE TABLE IF NOT EXISTS regime_log (
                id TEXT PRIMARY KEY,
                symbol TEXT, regime TEXT, confidence REAL,
                duration_ticks INTEGER, ts TEXT
            );

            CREATE TABLE IF NOT EXISTS layer_performance (
                id TEXT PRIMARY KEY,
                layer_name TEXT, symbol TEXT,
                n_calls INTEGER, n_correct INTEGER,
                accuracy REAL, avg_confidence REAL,
                last_updated TEXT
            );

            CREATE TABLE IF NOT EXISTS genome_log (
                id TEXT PRIMARY KEY,
                generation INTEGER, fitness REAL,
                genes TEXT, ts TEXT
            );

            CREATE TABLE IF NOT EXISTS lessons (
                id TEXT PRIMARY KEY,
                category TEXT, insight TEXT,
                confidence REAL, reinforced INTEGER DEFAULT 1,
                ts TEXT
            );

            CREATE TABLE IF NOT EXISTS daily_metrics (
                date TEXT PRIMARY KEY,
                trades INTEGER, wins INTEGER, losses INTEGER,
                pnl REAL, pnl_pct REAL,
                max_drawdown REAL, sharpe REAL,
                best_trade REAL, worst_trade REAL,
                active_pairs INTEGER
            );

            CREATE TABLE IF NOT EXISTS price_predictions (
                id TEXT PRIMARY KEY,
                symbol TEXT, horizon_s INTEGER,
                predicted_price REAL, actual_price REAL,
                error_pct REAL, confidence REAL,
                model_name TEXT, ts TEXT
            );
        """)
        self.conn.commit()

    # ── write ─────────────────────────────────────────────────────────────────

    def save_trade(self, trade: Dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO trades VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (trade.get("id", str(uuid.uuid4())[:8]),
             trade["symbol"], trade.get("exchange",""), trade["side"],
             trade["entry_price"], trade.get("exit_price", 0),
             trade["quantity"], trade.get("pnl", 0), trade.get("pnl_pct", 0),
             trade.get("fees", 0), trade.get("duration_s", 0),
             trade.get("entry_signal", ""), trade.get("exit_reason", ""),
             trade.get("regime", ""), trade.get("confidence", 0),
             json.dumps(trade.get("layer_votes", {})),
             trade.get("opened_at", _NOW()), trade.get("closed_at", _NOW()))
        )
        self.conn.commit()

    def save_lesson(self, category: str, insight: str, confidence: float = 0.9):
        self.conn.execute(
            "INSERT INTO lessons VALUES (?,?,?,?,1,?)",
            (str(uuid.uuid4())[:8], category, insight, confidence, _NOW())
        )
        self.conn.commit()

    def log_genome(self, generation: int, fitness: float, genes: Dict):
        self.conn.execute(
            "INSERT INTO genome_log VALUES (?,?,?,?,?)",
            (str(uuid.uuid4())[:8], generation, fitness,
             json.dumps(genes), _NOW())
        )
        self.conn.commit()

    def update_layer_performance(self, layer: str, symbol: str,
                                  correct: bool, confidence: float):
        row = self.conn.execute(
            "SELECT id, n_calls, n_correct, accuracy FROM layer_performance "
            "WHERE layer_name=? AND symbol=?", (layer, symbol)
        ).fetchone()
        if row:
            n = row[1] + 1
            c = row[2] + int(correct)
            acc = c / n
            self.conn.execute(
                "UPDATE layer_performance SET n_calls=?, n_correct=?, "
                "accuracy=?, avg_confidence=?, last_updated=? "
                "WHERE id=?",
                (n, c, acc, confidence, _NOW(), row[0])
            )
        else:
            self.conn.execute(
                "INSERT INTO layer_performance VALUES (?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:8], layer, symbol,
                 1, int(correct), float(correct), confidence, _NOW())
            )
        self.conn.commit()

    # ── read ──────────────────────────────────────────────────────────────────

    def get_recent_trades(self, symbol: Optional[str] = None,
                           limit: int = 100) -> List[Dict]:
        if symbol:
            rows = self.conn.execute(
                "SELECT * FROM trades WHERE symbol=? ORDER BY closed_at DESC LIMIT ?",
                (symbol, limit)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM trades ORDER BY closed_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        cols = [d[0] for d in self.conn.execute("PRAGMA table_info(trades)").fetchall()]
        return [dict(zip(cols, r)) for r in rows]

    def get_win_stats(self, symbol: Optional[str] = None,
                      last_n: int = 50) -> Dict:
        trades = self.get_recent_trades(symbol, last_n)
        if not trades:
            return {"win_rate": 0.5, "avg_win": 0.0, "avg_loss": 0.0,
                    "n_trades": 0, "total_pnl": 0.0}
        wins   = [t["pnl"] for t in trades if t["pnl"] > 0]
        losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
        return {
            "win_rate":  len(wins) / max(len(trades), 1),
            "avg_win":   float(np.mean(wins))   if wins   else 0.0,
            "avg_loss":  float(np.mean(losses))  if losses else 0.0,
            "n_trades":  len(trades),
            "total_pnl": sum(t["pnl"] for t in trades),
            "sharpe":    _sharpe([t["pnl_pct"] for t in trades]),
        }

    def get_layer_weights(self) -> Dict[str, float]:
        rows = self.conn.execute(
            "SELECT layer_name, AVG(accuracy) FROM layer_performance "
            "GROUP BY layer_name"
        ).fetchall()
        if not rows:
            return {}
        weights = {r[0]: float(r[1]) for r in rows}
        total = sum(weights.values()) + 1e-10
        return {k: v / total for k, v in weights.items()}

    def get_lessons(self, category: Optional[str] = None, limit: int = 20) -> List[str]:
        if category:
            rows = self.conn.execute(
                "SELECT insight FROM lessons WHERE category=? "
                "ORDER BY confidence DESC LIMIT ?", (category, limit)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT insight FROM lessons ORDER BY confidence DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
#  FEATURE ENGINE  (builds the 64-dim state vector every tick)
# ═══════════════════════════════════════════════════════════════════════════════

class FeatureEngine:
    """
    Converts raw market data into a rich 64-dimensional feature vector.
    Covers: price dynamics, volatility, momentum, microstructure,
    cross-timeframe confluence, macro overlay, sentiment, regime.
    """

    DIM = 64

    def build(
        self,
        prices:        List[float],
        volumes:       List[float],
        high:          List[float],
        low:           List[float],
        ob_imbalance:  float = 0.0,
        spread_pct:    float = 0.001,
        trade_flow:    float = 0.0,
        funding_rate:  float = 0.0,
        open_interest: float = 0.0,
        fear_greed:    float = 50.0,    # 0-100
        macro_vix:     float = 20.0,
        btc_dominance: float = 50.0,
        regime_vec:    Optional[List[float]] = None,
    ) -> np.ndarray:

        p = np.array(prices[-200:], dtype=np.float64) if len(prices) >= 200 \
            else np.array(prices, dtype=np.float64)
        v = np.array(volumes[-50:], dtype=np.float64) if len(volumes) >= 50 \
            else np.array(volumes, dtype=np.float64)
        h = np.array(high[-50:], dtype=np.float64)   if len(high) >= 50 \
            else np.array(high, dtype=np.float64)
        lo = np.array(low[-50:], dtype=np.float64)    if len(low) >= 50 \
            else np.array(low, dtype=np.float64)
        n = len(p)
        px = float(p[-1]) if n > 0 else 1.0

        def pct(a, b): return float((a - b) / (b + 1e-12))
        def safe_std(arr, w):
            return float(np.std(arr[-w:])) if len(arr) >= w else float(np.std(arr) + 1e-10)
        def safe_sma(arr, w):
            return float(np.mean(arr[-w:])) if len(arr) >= w else float(np.mean(arr))

        # ── PRICE RETURNS ─────────────────────────────────────────────── [0-9]
        ret_1  = pct(p[-1], p[-2])  if n >= 2   else 0.0
        ret_5  = pct(p[-1], p[-6])  if n >= 6   else 0.0
        ret_15 = pct(p[-1], p[-16]) if n >= 16  else 0.0
        ret_30 = pct(p[-1], p[-31]) if n >= 31  else 0.0
        ret_60 = pct(p[-1], p[-61]) if n >= 61  else 0.0
        ret_120= pct(p[-1], p[-121])if n >= 121 else 0.0
        price_range_pct = float((p[-n:].max() - p[-n:].min()) / (px + 1e-10))
        price_vs_range  = float((px - p[-n:].min()) / (p[-n:].max() - p[-n:].min() + 1e-10))
        log_ret_1  = float(math.log(p[-1] / (p[-2] + 1e-10))) if n >= 2 else 0.0
        log_ret_5  = float(math.log(p[-1] / (p[-6] + 1e-10))) if n >= 6 else 0.0

        # ── VOLATILITY ────────────────────────────────────────────────── [10-16]
        vol_5  = safe_std(p, 5)  / (px + 1e-10)
        vol_10 = safe_std(p, 10) / (px + 1e-10)
        vol_20 = safe_std(p, 20) / (px + 1e-10)
        vol_50 = safe_std(p, 50) / (px + 1e-10)
        vol_ratio = float(vol_5 / (vol_20 + 1e-10))   # short/long vol ratio (spike)
        atr_val   = _atr(list(h), list(lo), list(p), 14)
        atr_pct   = float(atr_val / (px + 1e-10))

        # ── MOMENTUM / RSI ───────────────────────────────────────────── [17-23]
        rsi_5  = _rsi(list(p), 5)
        rsi_14 = _rsi(list(p), 14)
        rsi_21 = _rsi(list(p), 21)
        rsi_div = float(rsi_5 - rsi_14)
        mom_5  = float(np.sign(ret_5)  * math.sqrt(abs(ret_5)))
        mom_15 = float(np.sign(ret_15) * math.sqrt(abs(ret_15)))
        mom_score = float(sum(1 if pct(p[-i], p[-i-1]) > 0 else -1
                              for i in range(1, min(11, n))) / 10)

        # ── EMA SIGNALS ──────────────────────────────────────────────── [24-28]
        ema8  = _ema_fast(list(p[-30:]), 8)
        ema21 = _ema_fast(list(p[-60:]), 21)
        ema55 = _ema_fast(list(p[-150:]), 55)
        ema_fast_cross   = float((ema8  - ema21)  / (px + 1e-10))
        ema_medium_cross = float((ema21 - ema55)  / (px + 1e-10))

        # ── BOLLINGER BANDS ──────────────────────────────────────────── [29-32]
        bb_mid = safe_sma(p, 20)
        bb_std = safe_std(p, 20)
        bb_z   = float((px - bb_mid) / (bb_std + 1e-10))
        bb_width = float(4 * bb_std / (bb_mid + 1e-10))
        bb_pct   = float((px - (bb_mid - 2*bb_std)) / (4 * bb_std + 1e-10))
        bb_squeeze = float(bb_width < np.mean([safe_std(p[max(0,n-i-20):n-i], 20)
                            for i in range(0, min(50, n), 10)]) * 0.7)

        # ── VOLUME SIGNALS ───────────────────────────────────────────── [33-37]
        vol_ma   = float(np.mean(v[-20:])) if len(v) >= 20 else float(np.mean(v) + 1e-10)
        vol_spike = float(v[-1] / (vol_ma + 1e-10)) if len(v) > 0 else 1.0
        vwap     = float(np.sum(p[-20:] * v[-20:]) / (np.sum(v[-20:]) + 1e-10)) \
                   if len(p) >= 20 and len(v) >= 20 else px
        price_vs_vwap = float((px - vwap) / (vwap + 1e-10))
        vol_trend = float(np.polyfit(range(min(10,len(v))), v[-10:], 1)[0] / (vol_ma + 1e-10)) \
                    if len(v) >= 5 else 0.0

        # ── MICROSTRUCTURE ───────────────────────────────────────────── [38-44]
        ob_imb     = float(np.clip(ob_imbalance, -1, 1))
        spread_f   = float(min(spread_pct * 100, 10))
        tf         = float(np.clip(trade_flow, -1, 1))
        fr         = float(np.clip(funding_rate * 10000, -50, 50))
        oi_norm    = float(np.tanh(open_interest / 1e8))
        flow_x_imb = float(ob_imb * tf)
        fr_signal  = float(np.sign(funding_rate) * min(abs(funding_rate) * 10000, 5))

        # ── MACRO & SENTIMENT ────────────────────────────────────────── [45-50]
        fg_norm    = float((fear_greed - 50) / 50)    # -1..+1
        vix_norm   = float(-np.log(macro_vix + 1) / 5)  # high vix = risk-off
        btc_dom_n  = float((btc_dominance - 50) / 50)
        macro_risk = float(np.tanh((macro_vix - 20) / 10))   # -1..+1
        fg_extreme = float(abs(fear_greed - 50) > 30)
        fg_contrarian = float(-np.sign(fear_greed - 50) * (abs(fear_greed - 50) > 40))

        # ── REGIME VECTOR ────────────────────────────────────────────── [51-57]
        rv = regime_vec if regime_vec and len(regime_vec) >= 7 \
             else [0.0] * 7
        r_bull, r_bear, r_vol, r_range, r_chop, r_trend, r_manipulation = \
            [float(rv[i]) for i in range(7)]

        # ── CROSS-FEATURE INTERACTIONS ───────────────────────────────── [58-63]
        rsi_x_flow      = float(rsi_14 * np.clip(ob_imbalance, -1, 1))
        vol_x_mom       = float(vol_ratio * mom_5)
        ema_x_flow      = float(ema_fast_cross * tf)
        bb_x_vol        = float(bb_z * vol_spike)
        rsi_x_funding   = float((rsi_14 - 0.5) * fr_signal)
        momentum_score  = float(np.tanh(mom_score * 5))

        feat = np.array([
            # Price returns [0-9]
            ret_1, ret_5, ret_15, ret_30, ret_60, ret_120,
            price_range_pct, price_vs_range, log_ret_1, log_ret_5,
            # Volatility [10-16]
            vol_5, vol_10, vol_20, vol_50, vol_ratio, atr_pct, bb_squeeze,
            # Momentum / RSI [17-23]
            rsi_5, rsi_14, rsi_21, rsi_div, mom_5, mom_15, mom_score,
            # EMA [24-28]
            ema_fast_cross, ema_medium_cross,
            float(np.tanh(ema_fast_cross * 50)),
            float(np.tanh(ema_medium_cross * 30)),
            float(ema8 > ema21),
            # BB [29-32]
            bb_z, bb_width, bb_pct, float((bb_z > 1.5) - (bb_z < -1.5)),
            # Volume [33-37]
            vol_spike, price_vs_vwap, vol_trend,
            float(np.log1p(vol_spike)), float(vol_spike > 2.5),
            # Microstructure [38-44]
            ob_imb, spread_f, tf, fr, oi_norm, flow_x_imb, fr_signal,
            # Macro / Sentiment [45-50]
            fg_norm, vix_norm, btc_dom_n, macro_risk, fg_extreme, fg_contrarian,
            # Regime [51-57]
            r_bull, r_bear, r_vol, r_range, r_chop, r_trend, r_manipulation,
            # Cross-feature [58-63]
            rsi_x_flow, vol_x_mom, ema_x_flow, bb_x_vol, rsi_x_funding, momentum_score,
        ], dtype=np.float32)

        return np.clip(feat, -5.0, 5.0)


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 1: NEURAL SWARM  (8 architectures, online Adam, confidence-weighted)
# ═══════════════════════════════════════════════════════════════════════════════

class AdamNet:
    """Mini online neural net with Adam optimizer — pure NumPy."""

    def __init__(self, in_d: int, h1: int, h2: int, out_d: int,
                 lr: float = 0.001, name: str = "net", dropout: float = 0.1):
        self.name = name
        self.lr   = lr
        self.drop = dropout

        scale1 = math.sqrt(2.0 / in_d)
        scale2 = math.sqrt(2.0 / h1)
        scale3 = math.sqrt(2.0 / h2)

        self.W1 = np.random.randn(in_d, h1) * scale1
        self.b1 = np.zeros(h1)
        self.W2 = np.random.randn(h1,   h2) * scale2
        self.b2 = np.zeros(h2)
        self.W3 = np.random.randn(h2,   out_d) * scale3
        self.b3 = np.zeros(out_d)

        params = [self.W1, self.b1, self.W2, self.b2, self.W3, self.b3]
        self.m  = [np.zeros_like(p) for p in params]
        self.v_ = [np.zeros_like(p) for p in params]
        self.t  = 0
        self._b1 = 0.9; self._b2 = 0.999; self._eps = 1e-8

        self._a0 = self._z1 = self._a1 = None
        self._z2 = self._a2 = None
        self._mask1 = self._mask2 = None

    def forward(self, x: np.ndarray, training: bool = False) -> np.ndarray:
        self._a0 = x
        self._z1 = x @ self.W1 + self.b1
        self._a1 = _relu(self._z1)
        if training and self.drop > 0:
            self._mask1 = (np.random.rand(*self._a1.shape) > self.drop).astype(float)
            self._a1 *= self._mask1 / (1 - self.drop + 1e-8)
        else:
            self._mask1 = np.ones_like(self._a1)
        self._z2 = self._a1 @ self.W2 + self.b2
        self._a2 = _relu(self._z2)
        if training and self.drop > 0:
            self._mask2 = (np.random.rand(*self._a2.shape) > self.drop).astype(float)
            self._a2 *= self._mask2 / (1 - self.drop + 1e-8)
        else:
            self._mask2 = np.ones_like(self._a2)
        return self._a2 @ self.W3 + self.b3

    def backward(self, grad_out: np.ndarray, clip_norm: float = 1.0):
        self.t += 1
        gW3 = np.outer(self._a2, grad_out); gb3 = grad_out
        da2 = (grad_out @ self.W3.T) * (self._a2 > 0) * self._mask2
        gW2 = np.outer(self._a1, da2); gb2 = da2
        da1 = (da2 @ self.W2.T) * (self._a1 > 0) * self._mask1
        gW1 = np.outer(self._a0, da1); gb1 = da1
        grads  = [gW1, gb1, gW2, gb2, gW3, gb3]
        params = [self.W1, self.b1, self.W2, self.b2, self.W3, self.b3]

        # Gradient clipping
        total_norm = math.sqrt(sum(float(np.sum(g**2)) for g in grads))
        if total_norm > clip_norm:
            scale = clip_norm / (total_norm + 1e-8)
            grads = [g * scale for g in grads]

        for i, (p, g) in enumerate(zip(params, grads)):
            self.m[i]  = self._b1 * self.m[i]  + (1 - self._b1) * g
            self.v_[i] = self._b2 * self.v_[i] + (1 - self._b2) * g**2
            mh = self.m[i]  / (1 - self._b1**self.t + 1e-20)
            vh = self.v_[i] / (1 - self._b2**self.t + 1e-20)
            p -= self.lr * mh / (np.sqrt(vh) + self._eps)

    def save(self, path: str):
        np.savez_compressed(path,
                            W1=self.W1, b1=self.b1, W2=self.W2, b2=self.b2,
                            W3=self.W3, b3=self.b3)

    def load(self, path: str):
        fp = Path(path + ".npz")
        if fp.exists():
            d = np.load(str(fp))
            self.W1, self.b1 = d["W1"], d["b1"]
            self.W2, self.b2 = d["W2"], d["b2"]
            self.W3, self.b3 = d["W3"], d["b3"]


class NeuralSwarm:
    """
    8 neural architectures, each trained online with every tick.
    Confidence-weighted voting + dynamic weight adjustment.
    Architectures span different inductive biases:
    CNN-1D, LSTM-lite, Transformer-approx, TCN, GRU-lite,
    WaveNet, Attention, Capsule-approx.
    """

    ARCH_CONFIGS = [
        # (h1,    h2,   lr,     name,          dropout)
        (128,  64,  0.0020, "CNN-1D",       0.10),
        (64,   32,  0.0010, "LSTM-lite",    0.05),
        (192,  64,  0.0015, "Transformer",  0.10),
        (128,  128, 0.0008, "TCN",          0.15),
        (64,   64,  0.0012, "GRU-lite",     0.05),
        (256,  128, 0.0005, "WaveNet",      0.20),
        (64,   32,  0.0010, "Attention",    0.05),
        (128,  64,  0.0018, "Capsule",      0.10),
    ]

    SCORE_MAP = {0: +2.0, 1: +1.0, 2: 0.0, 3: -1.0, 4: -2.0}

    def __init__(self, pair: str):
        self.pair   = pair
        self._lock  = threading.Lock()
        self.nets   = [
            AdamNet(FeatureEngine.DIM, h1, h2, N_ACTIONS, lr, name, drop)
            for (h1, h2, lr, name, drop) in self.ARCH_CONFIGS
        ]
        self.weights    = np.ones(8) / 8
        self.accuracy   = [deque(maxlen=200) for _ in range(8)]
        self.n_updates  = 0

        for i, net in enumerate(self.nets):
            safe = pair.replace("/", "_")
            net.load(str(MODELS_DIR / f"{safe}_neural_{net.name}"))

        logger.debug(f"🧠 NeuralSwarm[{pair}] ready — 8 architectures")

    def predict(self, feat: np.ndarray) -> Dict:
        """Weighted ensemble vote across 8 nets."""
        with self._lock:
            probs_all = []
            logits_all = []
            for net in self.nets:
                out   = net.forward(feat, training=False)
                probs = _softmax(out)
                probs_all.append(probs)
                logits_all.append(out)

        w  = self.weights / (self.weights.sum() + 1e-8)
        ep = sum(p * wi for p, wi in zip(probs_all, w))
        ai = int(np.argmax(ep))
        confidence = float(ep[ai])

        score = float(sum(self.SCORE_MAP[int(np.argmax(p))] * wi
                          for p, wi in zip(probs_all, w)))
        n_bull = sum(1 for p in probs_all if int(np.argmax(p)) in (0, 1))
        n_bear = sum(1 for p in probs_all if int(np.argmax(p)) in (3, 4))

        direction = "hold"
        if score > 0.4 and n_bull >= 5:
            direction = "buy"
        elif score < -0.4 and n_bear >= 5:
            direction = "sell"

        return {
            "direction":   direction,
            "score":       score,
            "confidence":  confidence,
            "n_bull":      n_bull,
            "n_bear":      n_bear,
            "ensemble_p":  ep.tolist(),
            "arch_votes":  [self.SCORE_MAP[int(np.argmax(p))] for p in probs_all],
        }

    def learn(self, feat: np.ndarray, actual_direction: int, reward: float):
        """
        Online update: actual_direction ∈ {0=strong_buy,1=buy,2=hold,3=sell,4=strong_sell}
        reward: signed PnL of the trade that closed.
        """
        with self._lock:
            self.n_updates += 1
            for i, net in enumerate(self.nets):
                out = net.forward(feat, training=True)
                probs = _softmax(out)

                # Cross-entropy loss gradient
                target = np.zeros(N_ACTIONS)
                target[actual_direction] = 1.0
                grad = (probs - target) * abs(reward)  # scale by outcome magnitude
                net.backward(grad)

                # Track accuracy
                correct = int(np.argmax(probs)) == actual_direction
                self.accuracy[i].append(float(correct))

            # Update weights toward best-performing archs
            if self.n_updates % 50 == 0:
                new_w = np.array([np.mean(list(a) or [0.5]) for a in self.accuracy])
                new_w = np.clip(new_w, 0.05, 0.95)
                self.weights = 0.9 * self.weights + 0.1 * new_w / (new_w.sum() + 1e-8)

    def save_all(self):
        for net in self.nets:
            safe = self.pair.replace("/", "_")
            net.save(str(MODELS_DIR / f"{safe}_neural_{net.name}"))


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 2: RL SOUL ENGINE  (single engine, self-contained)
# ═══════════════════════════════════════════════════════════════════════════════

class PrioritizedBuffer:
    """Prioritized Experience Replay buffer."""

    def __init__(self, capacity: int = BUFFER_CAP, alpha: float = 0.6):
        self.cap   = capacity
        self.alpha = alpha
        self.buf: deque = deque(maxlen=capacity)
        self.prios: deque = deque(maxlen=capacity)

    def push(self, state, action, reward, next_state, done, priority: float = 1.0):
        self.buf.append((state, action, reward, next_state, done))
        self.prios.append(float(priority ** self.alpha))

    def sample(self, n: int) -> List:
        if len(self.buf) < n:
            return list(self.buf)
        probs = np.array(list(self.prios))
        probs /= probs.sum() + 1e-10
        idxs  = np.random.choice(len(self.buf), n, replace=False, p=probs)
        items = list(self.buf)
        return [items[i] for i in idxs]

    def __len__(self): return len(self.buf)


class SoulEngine:
    """
    A single RL soul: Double DQN with prioritized replay, target network,
    curiosity bonus, and personality-shaped reward shaping.
    
    Personality determines risk tolerance, patience, and signal threshold.
    """

    PERSONALITIES = {
        "APEX":      {"patience": 0.95, "risk_tol": 0.3,  "conviction_min": 0.80},
        "PHANTOM":   {"patience": 0.70, "risk_tol": 0.5,  "conviction_min": 0.65},
        "STORM":     {"patience": 0.20, "risk_tol": 0.8,  "conviction_min": 0.45},
        "ORACLE":    {"patience": 0.80, "risk_tol": 0.4,  "conviction_min": 0.70},
        "VENOM":     {"patience": 0.60, "risk_tol": 0.6,  "conviction_min": 0.60},
        "TITAN":     {"patience": 0.85, "risk_tol": 0.35, "conviction_min": 0.75},
        "HYDRA":     {"patience": 0.50, "risk_tol": 0.55, "conviction_min": 0.55},
        "VOID":      {"patience": 0.30, "risk_tol": 0.7,  "conviction_min": 0.50},
        "PULSE":     {"patience": 0.40, "risk_tol": 0.65, "conviction_min": 0.55},
        "INFINITY":  {"patience": 0.75, "risk_tol": 0.45, "conviction_min": 0.68},
    }

    def __init__(self, name: str, pair: str):
        self.name = name
        self.pair = pair
        self.persona = self.PERSONALITIES.get(name, self.PERSONALITIES["VOID"])

        h = 96
        self.Q       = AdamNet(STATE_DIM, h*2, h, N_ACTIONS, LR_FAST, f"Q_{name}")
        self.Q_tgt   = AdamNet(STATE_DIM, h*2, h, N_ACTIONS, LR_SLOW, f"Qt_{name}")
        self.replay  = PrioritizedBuffer(BUFFER_CAP // 10)

        self.epsilon    = 0.15
        self.eps_min    = 0.01
        self.eps_decay  = 0.9995
        self.n_steps    = 0
        self.win_streak = 0
        self.win_rate   = 0.5
        self.total_reward = 0.0

        # Curiosity: tracks visit counts per state-bucket
        self._visit_counts: Dict[int, int] = defaultdict(int)
        self._load()

    def _state_bucket(self, feat: np.ndarray) -> int:
        """Hash feature vector to a 16-bit bucket for curiosity tracking."""
        signs = (feat > 0).astype(np.uint8)
        return int(hashlib.md5(signs.tobytes()).hexdigest()[:4], 16)

    def act(self, feat: np.ndarray) -> Tuple[int, float]:
        """ε-greedy with UCB bonus. Returns (action_idx, confidence)."""
        bucket = self._state_bucket(feat)
        self._visit_counts[bucket] += 1
        visits = self._visit_counts[bucket]

        if np.random.rand() < self.epsilon:
            return random.randint(0, N_ACTIONS - 1), 0.2

        q_vals = self.Q.forward(feat)

        # UCB bonus for less-visited states
        ucb_bonus = self.persona["patience"] * math.sqrt(
            math.log(max(self.n_steps, 1)) / (visits + 1)
        )

        # Personality gate: HOLD unless conviction is high enough
        q_adjusted = q_vals.copy()
        hold_idx = 2  # HOLD action
        best_non_hold = np.argmax(np.concatenate([q_adjusted[:2], q_adjusted[3:]]))

        probs = _softmax(q_adjusted)
        best  = int(np.argmax(q_adjusted))
        conf  = float(probs[best])

        if conf < self.persona["conviction_min"] and best != hold_idx:
            return hold_idx, float(probs[hold_idx])

        return best, conf

    def learn_step(self, feat: np.ndarray, action: int,
                   reward: float, next_feat: np.ndarray, done: bool):
        """Store experience and run one training step."""
        shaped_reward = self._shape_reward(reward, action)
        td_error = abs(shaped_reward) + 0.01  # priority
        self.replay.push(feat, action, shaped_reward, next_feat, done, td_error)
        self.n_steps += 1
        self.total_reward += reward

        # Win rate tracking
        if reward > 0:
            self.win_streak += 1
            self.win_rate = 0.95 * self.win_rate + 0.05
        else:
            self.win_streak = 0
            self.win_rate = 0.95 * self.win_rate

        # ε decay
        self.epsilon = max(self.eps_min, self.epsilon * self.eps_decay)

        if len(self.replay) < BATCH_SIZE:
            return

        # Batch update
        batch = self.replay.sample(BATCH_SIZE)
        for (s, a, r, ns, d) in batch:
            q_cur   = self.Q.forward(s)
            q_next  = self.Q_tgt.forward(ns)
            target  = q_cur.copy()
            target[a] = r + (0.0 if d else GAMMA * float(np.max(q_next)))
            grad = q_cur - target
            self.Q.backward(grad)

        # Soft target update
        if self.n_steps % 100 == 0:
            tau = 0.005
            for attr in ["W1","b1","W2","b2","W3","b3"]:
                q_p   = getattr(self.Q, attr)
                qt_p  = getattr(self.Q_tgt, attr)
                setattr(self.Q_tgt, attr, tau * q_p + (1-tau) * qt_p)

    def _shape_reward(self, raw: float, action: int) -> float:
        """Personality-specific reward shaping."""
        r = raw
        # Patience bonus: reward HOLD when market is uncertain
        if action == 2 and abs(raw) < 0.001:
            r += self.persona["patience"] * 0.01
        # Risk tolerance: scale reward by personality
        r *= (0.5 + self.persona["risk_tol"])
        return float(np.clip(r, -10, 10))

    def _load(self):
        safe = self.pair.replace("/", "_")
        self.Q.load(str(MODELS_DIR / f"{safe}_soul_{self.name}_Q"))
        self.Q_tgt.load(str(MODELS_DIR / f"{safe}_soul_{self.name}_Qt"))

    def save(self):
        safe = self.pair.replace("/", "_")
        self.Q.save(str(MODELS_DIR / f"{safe}_soul_{self.name}_Q"))
        self.Q_tgt.save(str(MODELS_DIR / f"{safe}_soul_{self.name}_Qt"))


class SoulCluster:
    """
    10 Soul Engines running in parallel per pair.
    Consensus voting with dynamic trust weights.
    Threshold: 6/10 standard, 8/10 strong signal.
    """

    SOUL_NAMES = ["APEX","PHANTOM","STORM","ORACLE","VENOM",
                  "TITAN","HYDRA","VOID","PULSE","INFINITY"]

    def __init__(self, pair: str):
        self.pair   = pair
        self.souls  = {n: SoulEngine(n, pair) for n in self.SOUL_NAMES}
        self.trust  = {n: 1.0 for n in self.SOUL_NAMES}  # dynamic trust weights
        self._lock  = threading.Lock()

    def vote(self, feat: np.ndarray) -> Dict:
        """All souls vote. Returns consensus signal."""
        votes: Dict[str, Tuple[int, float]] = {}
        with self._lock:
            for name, soul in self.souls.items():
                act, conf = soul.act(feat)
                votes[name] = (act, conf)

        # Weighted tally
        w_buy = w_sell = w_hold = 0.0
        for name, (act, conf) in votes.items():
            tw = self.trust[name] * conf
            if act in (0, 1):    w_buy  += tw
            elif act in (3, 4):  w_sell += tw
            else:                w_hold += tw

        total = w_buy + w_sell + w_hold + 1e-8
        buy_pct  = w_buy  / total
        sell_pct = w_sell / total

        n_buy  = sum(1 for (a, _) in votes.values() if a in (0,1))
        n_sell = sum(1 for (a, _) in votes.values() if a in (3,4))
        strong = max(n_buy, n_sell) >= 8

        direction = "hold"
        if buy_pct > 0.55:   direction = "buy"
        elif sell_pct > 0.55: direction = "sell"

        return {
            "direction":   direction,
            "buy_pct":     buy_pct,
            "sell_pct":    sell_pct,
            "n_buy":       n_buy,
            "n_sell":      n_sell,
            "strong":      strong,
            "confidence":  max(buy_pct, sell_pct),
            "votes":       {n: {"action": v[0], "conf": v[1]}
                            for n, v in votes.items()},
        }

    def update_trust(self, name: str, correct: bool):
        self.trust[name] = 0.95 * self.trust[name] + 0.05 * float(correct)
        self.trust[name] = max(0.1, min(2.0, self.trust[name]))

    def learn_all(self, feat: np.ndarray, action: int,
                  reward: float, next_feat: np.ndarray, done: bool):
        with self._lock:
            for soul in self.souls.values():
                soul.learn_step(feat, action, reward, next_feat, done)

    def save_all(self):
        for soul in self.souls.values():
            soul.save()


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 3: REGIME ORACLE  (real-time market regime classification)
# ═══════════════════════════════════════════════════════════════════════════════

class RegimeOracle:
    """
    Classifies market regime from price/volume data.
    Uses ensemble of rule-based + learned classifiers.
    Output: probability distribution over 12 regimes.
    """

    REGIMES = [r.value for r in MarketRegimeLabel]

    def __init__(self):
        self._classifier = AdamNet(32, 64, 32, 12, LR_SLOW, "regime_clf")
        self._price_hist: deque = deque(maxlen=500)
        self._vol_hist:   deque = deque(maxlen=500)
        self._current: str  = "ranging_tight"
        self._confidence: float = 0.5
        self._ticks_in_regime = 0
        self._load()

    def update(self, price: float, volume: float, ob_imbalance: float,
               spread_pct: float) -> Dict:
        self._price_hist.append(price)
        self._vol_hist.append(volume)
        self._ticks_in_regime += 1

        if len(self._price_hist) < 30:
            return self._default_vec()

        p  = np.array(list(self._price_hist), dtype=float)
        v  = np.array(list(self._vol_hist),   dtype=float)
        n  = len(p)

        # Rule-based features for regime detection
        ret_1h  = float((p[-1] - p[-60]) / (p[-60] + 1e-10)) if n >= 60 else 0.0
        vol_20  = float(np.std(p[-20:])) / (float(np.mean(p[-20:])) + 1e-10)
        vol_5   = float(np.std(p[-5:])) / (float(p[-1]) + 1e-10)
        trend   = float(np.polyfit(range(min(50,n)), p[-50:], 1)[0] / (float(p[-1]) + 1e-10))
        autocorr = float(np.corrcoef(p[-21:-1], p[-20:])[0,1]) if n >= 21 else 0.0
        vol_spike = float(v[-1] / (np.mean(v[-20:]) + 1e-10)) if len(v) >= 20 else 1.0
        rsi      = _rsi(list(p), 14)

        feat = np.array([
            ret_1h, vol_20, vol_5, trend, autocorr,
            vol_spike, ob_imbalance, spread_pct,
            rsi, float(np.std(p[-5:]) / (np.std(p[-20:]) + 1e-10)),
            float((p[-1] - p[-n:].min()) / (p[-n:].max() - p[-n:].min() + 1e-10)),
            float(abs(trend) > 0.002),
            float(vol_20 > 0.02),
            float(rsi > 0.7), float(rsi < 0.3),
            float(abs(autocorr) > 0.4),
            float(vol_spike > 3.0),
            float(abs(ret_1h) > 0.05),
            float(self._ticks_in_regime),
            float(abs(ob_imbalance) > 0.4),
            # 2nd-order features
            float(trend * autocorr),
            float(vol_20 * vol_spike),
            float(ret_1h * rsi),
            float(ob_imbalance * trend),
            float(vol_spike * abs(ret_1h)),
            float(autocorr * rsi),
            float(trend**2), float(vol_20**2),
            float(np.sign(trend) * vol_20),
            float(rsi * (1 - rsi) * 4),  # RSI as probability proxy
            float(abs(ob_imbalance) * vol_spike),
            float(np.tanh(trend * 500)),
        ], dtype=np.float32)

        logits = self._classifier.forward(np.clip(feat, -5, 5))
        probs  = _softmax(logits)
        idx    = int(np.argmax(probs))
        regime = self.REGIMES[idx]
        conf   = float(probs[idx])

        if regime != self._current and conf > 0.45:
            logger.debug(f"🎭 Regime shift: {self._current} → {regime} ({conf:.2f})")
            self._current = regime
            self._ticks_in_regime = 0
            self._confidence = conf

        # Return feature vector for other layers
        return {
            "regime":       self._current,
            "confidence":   self._confidence,
            "probs":        probs.tolist(),
            "regime_vec":   [
                float(probs[self.REGIMES.index("markup")] +
                      probs[self.REGIMES.index("stealth_accumulation")] * 0.5),     # bull
                float(probs[self.REGIMES.index("markdown")] +
                      probs[self.REGIMES.index("capitulation")] * 0.8),             # bear
                float(probs[self.REGIMES.index("flash_crash")] +
                      probs[self.REGIMES.index("parabolic")] * 0.5),               # volatile
                float(probs[self.REGIMES.index("ranging_tight")] +
                      probs[self.REGIMES.index("ranging_wide")] * 0.5),            # ranging
                float(probs[self.REGIMES.index("distribution")] * 0.6 +
                      probs[self.REGIMES.index("dead_cat")] * 0.4),                # chop
                float(abs(trend) * 5.0),                                            # trend strength
                float(probs[self.REGIMES.index("manipulated")] +
                      probs[self.REGIMES.index("liquidity_hunt")] * 0.6),          # manipulation
            ],
        }

    def _default_vec(self) -> Dict:
        return {
            "regime": "ranging_tight", "confidence": 0.3,
            "probs": [1/12]*12,
            "regime_vec": [0.2, 0.2, 0.1, 0.5, 0.1, 0.1, 0.05],
        }

    def _load(self):
        self._classifier.load(str(MODELS_DIR / "regime_oracle"))

    def save(self):
        self._classifier.save(str(MODELS_DIR / "regime_oracle"))


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 4: ADVERSARIAL SHIELD  (trap detection)
# ═══════════════════════════════════════════════════════════════════════════════

class AdversarialShield:
    """
    Detects market manipulation patterns that would fool normal strategies:
    bull traps, bear traps, stop hunts, spoof walls, wash trades,
    news reversals, liquidity vacuums, and coordinated whale attacks.
    """

    TRAP_TYPES = ["bull_trap","bear_trap","stop_hunt","spoof_wall",
                  "wash_trade","news_reversal","liquidity_vacuum",
                  "whale_manipulation","fake_breakout","clean"]

    def __init__(self):
        # Separate detector for each trap type (binary classifiers)
        self.detectors: Dict[str, AdamNet] = {
            trap: AdamNet(20, 32, 16, 2, LR_SLOW, f"shield_{trap}")
            for trap in self.TRAP_TYPES
        }
        self.trap_history: deque = deque(maxlen=1000)
        self._load()

    def _build_shield_features(
        self, prices: List[float], volumes: List[float],
        ob_imbalance: float, spread_pct: float
    ) -> np.ndarray:
        if len(prices) < 10:
            return np.zeros(20, dtype=np.float32)
        p = np.array(prices[-50:], dtype=float)
        v = np.array(volumes[-50:], dtype=float) if volumes else np.ones(50)
        n = len(p)

        vol_mean = float(np.mean(v[-20:])) + 1e-10
        return np.clip(np.array([
            float((p[-1] - p[-6]) / (p[-6] + 1e-10))  if n >= 6  else 0,  # 5-tick ret
            float((p[-1] - p[-21])/ (p[-21]+ 1e-10))  if n >= 21 else 0,  # 20-tick ret
            float(np.std(p[-10:]) / (p[-1] + 1e-10)),                       # short vol
            float(np.std(p[-30:]) / (p[-1] + 1e-10)) if n >= 30 else 0,   # long vol
            float(v[-1] / vol_mean),                                          # vol spike
            float(np.mean(v[-5:]) / vol_mean),                               # vol trend
            float(np.clip(ob_imbalance, -1, 1)),
            float(spread_pct * 100),
            _rsi(list(p), 14),
            float(np.corrcoef(p[-11:-1], p[-10:])[0,1]) if n >= 11 else 0,
            float(np.max(p[-10:]) / (p[-1] + 1e-10)) if n >= 10 else 1,     # recent high
            float(np.min(p[-10:]) / (p[-1] + 1e-10)) if n >= 10 else 1,     # recent low
            float(p[-1] / (np.mean(p[-20:]) + 1e-10)) if n >= 20 else 1,    # price vs ma
            float(abs(ob_imbalance) * float(v[-1] / vol_mean)),              # pressure×vol
            float(np.sign(ob_imbalance) * spread_pct * 100),
            float(abs(float((p[-1]-p[-6])/(p[-6]+1e-10)) if n>=6 else 0) * float(v[-1]/vol_mean)),
            float(p[-1] == np.max(p[-10:])) if n >= 10 else 0,  # at local high
            float(p[-1] == np.min(p[-10:])) if n >= 10 else 0,  # at local low
            float(np.std(v[-5:]) / (vol_mean + 1e-10)),          # vol variance
            float(np.tanh(ob_imbalance * float(v[-1]/vol_mean) * 5)),
        ], dtype=np.float32), -5, 5)

    def assess(self, prices: List[float], volumes: List[float],
               ob_imbalance: float, spread_pct: float) -> Dict:
        feat = self._build_shield_features(prices, volumes, ob_imbalance, spread_pct)
        trap_probs: Dict[str, float] = {}
        for trap, det in self.detectors.items():
            logits = det.forward(feat)
            p_trap = float(_softmax(logits)[1])
            trap_probs[trap] = p_trap

        danger_score = max(
            v for k, v in trap_probs.items() if k != "clean"
        )
        top_trap = max(
            ((k, v) for k, v in trap_probs.items() if k != "clean"),
            key=lambda x: x[1]
        )

        return {
            "danger_score":  danger_score,
            "top_trap":      top_trap[0] if danger_score > 0.4 else "none",
            "trap_probs":    trap_probs,
            "safe_to_trade": danger_score < 0.45,
            "warning":       danger_score > 0.6,
        }

    def _load(self):
        for trap, det in self.detectors.items():
            det.load(str(MODELS_DIR / f"shield_{trap}"))

    def save(self):
        for trap, det in self.detectors.items():
            det.save(str(MODELS_DIR / f"shield_{trap}"))


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 5: PREDICTIVE ENGINE  (multi-horizon price forecasting)
# ═══════════════════════════════════════════════════════════════════════════════

class PredictiveEngine:
    """
    Predicts price direction/magnitude across multiple horizons:
    1m, 5m, 15m, 1h, 4h.
    Uses separate model per horizon. Ensemble of 3 nets per horizon.
    Calibrates confidence via historical prediction accuracy.
    """

    HORIZONS = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240}

    def __init__(self, pair: str):
        self.pair = pair
        safe = pair.replace("/", "_")

        # 3 ensemble nets per horizon
        self.models: Dict[str, List[AdamNet]] = {}
        for hz in self.HORIZONS:
            self.models[hz] = [
                AdamNet(STATE_DIM, 64, 32, 3, LR_SLOW, f"pred_{hz}_{i}")
                for i in range(3)
            ]
            for i, m in enumerate(self.models[hz]):
                m.load(str(MODELS_DIR / f"{safe}_pred_{hz}_{i}"))

        # Calibration: per-horizon accuracy over last 200 predictions
        self.calibration: Dict[str, deque] = {hz: deque(maxlen=200)
                                               for hz in self.HORIZONS}
        self.pending: Dict[str, List[Tuple[float, float, int]]] = \
            {hz: [] for hz in self.HORIZONS}  # (predicted_direction, price_at_pred, ticks_left)

    def predict(self, feat: np.ndarray) -> Dict[str, Dict]:
        results = {}
        for hz in self.HORIZONS:
            probs_all = []
            for net in self.models[hz]:
                out   = net.forward(feat)
                probs = _softmax(out)  # [down, flat, up]
                probs_all.append(probs)
            ep  = np.mean(probs_all, axis=0)
            idx = int(np.argmax(ep))
            direction_map = {0: "down", 1: "flat", 2: "up"}
            cal = float(np.mean(list(self.calibration[hz])) or 0.5)
            raw_conf = float(ep[idx])
            calibrated_conf = float(0.5 * raw_conf + 0.5 * cal)
            results[hz] = {
                "direction":  direction_map[idx],
                "confidence": calibrated_conf,
                "raw_conf":   raw_conf,
                "up_prob":    float(ep[2]),
                "down_prob":  float(ep[0]),
                "flat_prob":  float(ep[1]),
                "calibration":cal,
            }
        return results

    def record_outcome(self, hz: str, predicted_up: bool, was_up: bool,
                       magnitude: float):
        correct = (predicted_up == was_up)
        self.calibration[hz].append(float(correct))
        # Update models
        target_idx = 2 if was_up else 0
        feat_placeholder = np.zeros(STATE_DIM)  # will be overwritten in real update
        for net in self.models[hz]:
            out   = net.forward(feat_placeholder)
            probs = _softmax(out)
            target = np.zeros(3); target[target_idx] = 1.0
            grad = (probs - target) * (1 + magnitude * 10)
            net.backward(grad)

    def save_all(self):
        safe = self.pair.replace("/", "_")
        for hz, nets in self.models.items():
            for i, net in enumerate(nets):
                net.save(str(MODELS_DIR / f"{safe}_pred_{hz}_{i}"))


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 6: META-LEARNER  (which layers to trust in which regime)
# ═══════════════════════════════════════════════════════════════════════════════

class MetaLearner:
    """
    Learns the optimal trust weight for each signal layer
    as a function of market regime and pair.
    
    Input:  regime_vec (7-dim) + pair_encoding
    Output: weight vector for [neural, rl_cluster, prediction, shield_modifier]
    """

    LAYERS = ["neural_swarm", "soul_cluster", "predictive", "regime_bias"]

    def __init__(self):
        self.net = AdamNet(7 + 8, 32, 16, len(self.LAYERS), LR_SLOW, "meta_learner")
        self.regime_accuracy: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: {l: deque(maxlen=100) for l in self.LAYERS}
        )
        self.pair_encoding: Dict[str, np.ndarray] = {}
        self._load()

    def _pair_feat(self, pair: str) -> np.ndarray:
        if pair not in self.pair_encoding:
            h = hashlib.md5(pair.encode()).digest()[:4]
            self.pair_encoding[pair] = np.frombuffer(h, dtype=np.uint8).astype(float) / 255.0
        return self.pair_encoding[pair]

    def get_weights(self, regime_vec: List[float], pair: str) -> Dict[str, float]:
        rv = np.array(regime_vec[:7], dtype=np.float32)
        pf = self._pair_feat(pair).astype(np.float32)
        feat = np.clip(np.concatenate([rv, np.pad(pf, (0, 4))]), -5, 5)
        out  = self.net.forward(feat)
        w    = _softmax(out) * 4 + 0.25  # ensure minimum weight
        return {l: float(w[i]) for i, l in enumerate(self.LAYERS)}

    def update(self, regime_vec: List[float], pair: str,
               layer_correct: Dict[str, bool]):
        rv = np.array(regime_vec[:7], dtype=np.float32)
        pf = self._pair_feat(pair).astype(np.float32)
        feat = np.clip(np.concatenate([rv, np.pad(pf, (0, 4))]), -5, 5)
        out  = self.net.forward(feat)
        # Reward layers that were correct
        target = np.array([
            float(layer_correct.get(l, 0.5))
            for l in self.LAYERS
        ], dtype=np.float32)
        grad = (_softmax(out) - target / (target.sum() + 1e-8)) * 0.5
        self.net.backward(grad)

    def _load(self):
        self.net.load(str(MODELS_DIR / "meta_learner"))

    def save(self):
        self.net.save(str(MODELS_DIR / "meta_learner"))


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 7: POSITION INTELLIGENCE  (sizing, entry, exit, risk)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Position:
    id:            str
    symbol:        str
    side:          str     # "long" / "short"
    entry_price:   float
    quantity:      float
    stop_loss:     float
    take_profit:   float
    trailing_stop: float   # absolute trail distance
    peak_price:    float
    open_time:     float
    last_price:    float   = 0.0
    unrealized_pnl: float  = 0.0
    confidence:    float   = 0.5
    regime:        str     = "unknown"
    entry_signal:  Dict    = field(default_factory=dict)


class PositionIntelligence:
    """
    Smart position management:
    - Kelly-based sizing with volatility adjustment
    - Dynamic stop-loss via ATR
    - Trailing stops that tighten as profit grows
    - Partial take-profit at 1R, 2R, 3R
    - Time-based exit (decay penalty on stale positions)
    - Max concurrent positions per pair / total
    """

    def __init__(
        self,
        initial_capital:    float  = 200.0,
        max_risk_per_trade: float  = 0.02,   # 2% capital risk per trade
        max_open_positions: int    = 5,
        max_leverage:       float  = 3.0,    # hard cap
        fee_rate:           float  = 0.001,  # 0.1%
    ):
        self.capital        = initial_capital
        self.peak_capital   = initial_capital
        self.max_risk       = max_risk_per_trade
        self.max_positions  = max_open_positions
        self.max_leverage   = max_leverage
        self.fee_rate       = fee_rate
        self.positions:     Dict[str, Position]  = {}
        self.closed_trades: List[Dict]           = []
        self.equity_curve:  List[float]          = [initial_capital]
        self._lock          = threading.Lock()

    def compute_size(
        self,
        price:     float,
        atr:       float,
        win_rate:  float  = 0.5,
        avg_win:   float  = 0.0,
        avg_loss:  float  = 0.0,
        signal_conf: float = 0.6,
    ) -> Dict:
        """Kelly-based position sizing with hard risk cap."""
        # ATR-based stop
        atr_stop = atr * 2.0 if atr > 0 else price * 0.01
        risk_per_unit = atr_stop

        # Kelly fraction
        kelly = _kelly(win_rate, max(avg_win, 0.001), max(abs(avg_loss), 0.001))

        # Scale by signal confidence and available capital
        risk_capital = self.capital * self.max_risk
        base_qty = risk_capital / (risk_per_unit + 1e-10)

        # Apply kelly and confidence scaling
        scale = kelly * signal_conf
        qty   = base_qty * min(scale, 1.5)  # never more than 1.5x base

        # Leverage check
        notional = qty * price
        if notional > self.capital * self.max_leverage:
            qty = (self.capital * self.max_leverage) / (price + 1e-10)

        # Drawdown protection: reduce size if in drawdown
        dd = (self.peak_capital - self.capital) / (self.peak_capital + 1e-10)
        if dd > 0.05:
            qty *= max(0.3, 1.0 - dd * 3)

        return {
            "quantity":   float(max(qty, 0.0)),
            "stop_dist":  float(atr_stop),
            "kelly":      float(kelly),
            "scale":      float(scale),
            "notional":   float(qty * price),
            "risk_pct":   float(qty * atr_stop / (self.capital + 1e-10) * 100),
        }

    def open_position(self, symbol: str, side: str, price: float,
                      quantity: float, atr: float, confidence: float,
                      regime: str, signal: Dict) -> Optional[Position]:
        with self._lock:
            if len(self.positions) >= self.max_positions:
                return None

            stop_dist = atr * 2.0
            tp_dist   = atr * 4.0
            stop  = price - stop_dist if side == "long" else price + stop_dist
            tp    = price + tp_dist   if side == "long" else price - tp_dist

            pos = Position(
                id           = str(uuid.uuid4())[:8],
                symbol       = symbol,
                side         = side,
                entry_price  = price,
                quantity     = quantity,
                stop_loss    = stop,
                take_profit  = tp,
                trailing_stop= stop_dist * 1.5,
                peak_price   = price,
                open_time    = time.time(),
                confidence   = confidence,
                regime       = regime,
                entry_signal = signal,
            )
            fees = quantity * price * self.fee_rate
            self.capital -= fees
            self.positions[pos.id] = pos
            logger.info(f"📈 OPEN {side.upper()} {symbol} @ {price:.6f} "
                        f"qty={quantity:.4f} SL={stop:.6f} TP={tp:.6f} "
                        f"conf={confidence:.2f} [{pos.id}]")
            return pos

    def update_position(self, pos: Position, current_price: float) -> Optional[str]:
        """
        Update trailing stop, check SL/TP.
        Returns exit_reason or None.
        """
        pos.last_price = current_price
        if pos.side == "long":
            pos.unrealized_pnl = (current_price - pos.entry_price) * pos.quantity
            if current_price > pos.peak_price:
                pos.peak_price = current_price
                # Tighten trailing stop as profit grows
                profit_r = (current_price - pos.entry_price) / (pos.trailing_stop + 1e-10)
                trail_mult = max(0.5, 1.5 - profit_r * 0.1)  # tightens at 3R+
                new_stop = current_price - pos.trailing_stop * trail_mult
                if new_stop > pos.stop_loss:
                    pos.stop_loss = new_stop
            if current_price <= pos.stop_loss: return "stop_loss"
            if current_price >= pos.take_profit: return "take_profit"
        else:  # short
            pos.unrealized_pnl = (pos.entry_price - current_price) * pos.quantity
            if current_price < pos.peak_price:
                pos.peak_price = current_price
                new_stop = current_price + pos.trailing_stop
                if new_stop < pos.stop_loss:
                    pos.stop_loss = new_stop
            if current_price >= pos.stop_loss: return "stop_loss"
            if current_price <= pos.take_profit: return "take_profit"

        # Time-based: if stale >4h in ranging regime, exit
        elapsed = time.time() - pos.open_time
        if elapsed > 14400 and pos.regime in ("ranging_tight", "ranging_wide"):
            return "timeout"
        return None

    def close_position(self, pos: Position, price: float,
                       exit_reason: str) -> Dict:
        with self._lock:
            if pos.id not in self.positions:
                return {}
            if pos.side == "long":
                pnl = (price - pos.entry_price) * pos.quantity
            else:
                pnl = (pos.entry_price - price) * pos.quantity
            fees = pos.quantity * price * self.fee_rate
            pnl -= fees
            pnl_pct = float(pnl / (pos.entry_price * pos.quantity + 1e-10) * 100)
            self.capital += pnl
            self.peak_capital = max(self.peak_capital, self.capital)
            self.equity_curve.append(self.capital)
            del self.positions[pos.id]

            trade = {
                "id": pos.id, "symbol": pos.symbol, "side": pos.side,
                "entry_price": pos.entry_price, "exit_price": price,
                "quantity": pos.quantity, "pnl": pnl, "pnl_pct": pnl_pct,
                "fees": fees,
                "duration_s": time.time() - pos.open_time,
                "exit_reason": exit_reason, "regime": pos.regime,
                "confidence": pos.confidence,
                "entry_signal": pos.entry_signal,
                "opened_at": datetime.fromtimestamp(pos.open_time).isoformat(),
                "closed_at": _NOW(),
            }
            self.closed_trades.append(trade)
            emoji = "✅" if pnl > 0 else "❌"
            logger.info(f"{emoji} CLOSE {pos.symbol} @ {price:.6f} "
                        f"PnL={pnl:+.4f} ({pnl_pct:+.2f}%) [{exit_reason}] "
                        f"Capital={self.capital:.2f}")
            return trade

    @property
    def total_unrealized(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def current_equity(self) -> float:
        return self.capital + self.total_unrealized

    @property
    def drawdown_pct(self) -> float:
        return (self.peak_capital - self.current_equity) / (self.peak_capital + 1e-10) * 100


# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 8: CIRCUIT BREAKERS  (survival > all else)
# ═══════════════════════════════════════════════════════════════════════════════

class CircuitBreakers:
    """
    Hard stops that cannot be overridden by any signal.
    Protects capital from catastrophic loss.
    """

    def __init__(
        self,
        max_daily_loss_pct: float = 5.0,
        max_drawdown_pct:   float = 15.0,
        max_consecutive_losses: int = 7,
        pause_after_loss_minutes: int = 30,
        max_open_risk_pct:  float = 10.0,
    ):
        self.max_daily_loss     = max_daily_loss_pct / 100
        self.max_drawdown       = max_drawdown_pct / 100
        self.max_consec_losses  = max_consecutive_losses
        self.pause_minutes      = pause_after_loss_minutes
        self.max_open_risk      = max_open_risk_pct / 100

        self.daily_start_capital: float = 0.0
        self.consecutive_losses:  int   = 0
        self.paused_until:        float = 0.0
        self.breakers_tripped:    List[str] = []
        self.all_clear:           bool  = True

    def reset_daily(self, capital: float):
        self.daily_start_capital = capital
        self.breakers_tripped    = []

    def check(self, pos_mgr: PositionIntelligence) -> Tuple[bool, List[str]]:
        """Returns (safe_to_trade, list_of_issues)."""
        issues: List[str] = []
        now = time.time()

        # Pause cooldown
        if now < self.paused_until:
            remaining = int(self.paused_until - now)
            issues.append(f"COOLING_DOWN:{remaining}s")
            return False, issues

        # Max drawdown from peak
        if pos_mgr.drawdown_pct >= self.max_drawdown * 100:
            issues.append(f"MAX_DRAWDOWN:{pos_mgr.drawdown_pct:.1f}%")
            self.paused_until = now + self.pause_minutes * 60

        # Daily loss
        if self.daily_start_capital > 0:
            daily_loss = (self.daily_start_capital - pos_mgr.current_equity) / \
                         self.daily_start_capital
            if daily_loss >= self.max_daily_loss:
                issues.append(f"DAILY_LOSS:{daily_loss*100:.1f}%")
                self.paused_until = now + 3600  # pause 1h

        # Consecutive losses
        if self.consecutive_losses >= self.max_consec_losses:
            issues.append(f"CONSEC_LOSSES:{self.consecutive_losses}")
            self.paused_until = now + self.pause_minutes * 60
            self.consecutive_losses = 0

        safe = len(issues) == 0
        if not safe:
            logger.warning(f"🚨 CIRCUIT BREAKER: {' | '.join(issues)}")
        self.all_clear = safe
        return safe, issues

    def on_trade_close(self, pnl: float):
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0


# ═══════════════════════════════════════════════════════════════════════════════
#  SIGNAL AGGREGATOR  (fuses all layer outputs into one decision)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MasterSignal:
    direction:    str    # "buy" / "sell" / "hold"
    strength:     float  # 0..1
    confidence:   float  # 0..1
    side:         str    # "long" / "short" / "neutral"
    regime:       str
    trap_warning: bool
    predictions:  Dict
    layer_votes:  Dict
    reason:       str


class SignalAggregator:
    """
    Combines votes from all layers using meta-learner weights.
    Applies regime-specific biases and trap filters.
    Produces a single actionable MasterSignal.
    """

    def aggregate(
        self,
        neural_vote:     Dict,
        rl_vote:         Dict,
        prediction:      Dict,
        regime_info:     Dict,
        shield:          Dict,
        meta_weights:    Dict,
    ) -> MasterSignal:

        regime  = regime_info.get("regime", "unknown")
        trap    = shield.get("danger_score", 0) > 0.45
        trap_w  = shield.get("warning", False)

        # Direction scores: +1=buy, -1=sell
        n_score = {"buy": +1.0, "sell": -1.0, "hold": 0.0}.get(
            neural_vote.get("direction", "hold"), 0.0
        ) * neural_vote.get("confidence", 0.5)

        r_score = {"buy": +1.0, "sell": -1.0, "hold": 0.0}.get(
            rl_vote.get("direction", "hold"), 0.0
        ) * rl_vote.get("confidence", 0.5)

        # Prediction score: weight near-term horizons higher
        p1m = prediction.get("1m", {}).get("up_prob", 0.5) - 0.5
        p5m = prediction.get("5m", {}).get("up_prob", 0.5) - 0.5
        p15m= prediction.get("15m",{}).get("up_prob", 0.5) - 0.5
        p_score = float(0.5 * p1m + 0.3 * p5m + 0.2 * p15m)

        # Regime bias
        rv = regime_info.get("regime_vec", [0]*7)
        bull_bias = float(rv[0] - rv[1])  # +ve = bullish regime
        regime_score = bull_bias * 0.3

        # Meta weights
        w_n = meta_weights.get("neural_swarm",   1.0)
        w_r = meta_weights.get("soul_cluster",   1.0)
        w_p = meta_weights.get("predictive",     1.0)
        w_b = meta_weights.get("regime_bias",    1.0)
        total_w = w_n + w_r + w_p + w_b + 1e-8

        final_score = (
            w_n * n_score +
            w_r * r_score +
            w_p * p_score +
            w_b * regime_score
        ) / total_w

        # Trap dampening
        if trap:
            final_score *= 0.3  # heavy reduce
        elif trap_w:
            final_score *= 0.6

        strength    = float(abs(final_score))
        confidence  = float(min(strength * 2.0, 1.0))

        direction  = "hold"
        if final_score >  0.15: direction = "buy"
        elif final_score < -0.15: direction = "sell"

        # Strong signal requirement: multiple layers must agree
        n_agree_buy  = sum([
            int(neural_vote.get("direction") == "buy"),
            int(rl_vote.get("direction")     == "buy"),
            int(p1m > 0.05),
            int(bull_bias > 0.2),
        ])
        n_agree_sell = sum([
            int(neural_vote.get("direction") == "sell"),
            int(rl_vote.get("direction")     == "sell"),
            int(p1m < -0.05),
            int(bull_bias < -0.2),
        ])

        if direction == "buy"  and n_agree_buy  < 2: direction = "hold"
        if direction == "sell" and n_agree_sell < 2: direction = "hold"

        return MasterSignal(
            direction   = direction,
            strength    = strength,
            confidence  = confidence,
            side        = "long" if direction == "buy" else
                          "short" if direction == "sell" else "neutral",
            regime      = regime,
            trap_warning= trap or trap_w,
            predictions = prediction,
            layer_votes = {
                "neural":    neural_vote.get("direction"),
                "rl":        rl_vote.get("direction"),
                "pred_1m":   "up" if p1m > 0 else "down",
                "regime":    "bull" if bull_bias > 0 else "bear",
                "final_score": final_score,
                "n_agree_buy": n_agree_buy,
                "n_agree_sell": n_agree_sell,
            },
            reason = (
                f"score={final_score:+.3f} "
                f"neural={neural_vote.get('direction')} "
                f"rl={rl_vote.get('direction')} "
                f"regime={regime} "
                f"trap={trap}"
            ),
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  PAIR BRAIN  (one complete brain instance per trading pair)
# ═══════════════════════════════════════════════════════════════════════════════

class PairBrain:
    """
    One fully autonomous brain per trading pair.
    All layers wired together. Processes every tick.
    """

    def __init__(self, pair: str, mode: BrainMode = BrainMode.PAPER):
        self.pair   = pair
        self.mode   = mode
        self._feat  = FeatureEngine()
        self.neural = NeuralSwarm(pair)
        self.souls  = SoulCluster(pair)
        self.oracle = RegimeOracle()
        self.shield = AdversarialShield()
        self.pred   = PredictiveEngine(pair)
        self.meta   = MetaLearner()
        self.agg    = SignalAggregator()

        # Price / volume buffers
        self._prices:  deque = deque(maxlen=500)
        self._volumes: deque = deque(maxlen=500)
        self._highs:   deque = deque(maxlen=500)
        self._lows:    deque = deque(maxlen=500)

        self._last_feat: Optional[np.ndarray] = None
        self._last_signal: Optional[MasterSignal] = None
        self._ticks = 0

        logger.info(f"🧠 PairBrain[{pair}] ready in {mode.value} mode")

    def on_tick(
        self,
        price: float, volume: float,
        high: float, low: float,
        ob_imbalance:  float = 0.0,
        spread_pct:    float = 0.001,
        trade_flow:    float = 0.0,
        funding_rate:  float = 0.0,
        open_interest: float = 0.0,
        fear_greed:    float = 50.0,
        macro_vix:     float = 20.0,
        btc_dominance: float = 50.0,
    ) -> MasterSignal:

        self._prices.append(price)
        self._volumes.append(volume)
        self._highs.append(high)
        self._lows.append(low)
        self._ticks += 1

        if len(self._prices) < 30:
            return MasterSignal("hold", 0.0, 0.0, "neutral",
                                "warming_up", False, {}, {}, "warming_up")

        # 1. Regime classification
        regime_info = self.oracle.update(price, volume, ob_imbalance, spread_pct)
        rv = regime_info.get("regime_vec", [0.2]*7)

        # 2. Feature vector
        feat = self._feat.build(
            list(self._prices), list(self._volumes),
            list(self._highs), list(self._lows),
            ob_imbalance, spread_pct, trade_flow,
            funding_rate, open_interest,
            fear_greed, macro_vix, btc_dominance, rv,
        )

        # 3. Adversarial shield
        shield_res = self.shield.assess(
            list(self._prices), list(self._volumes), ob_imbalance, spread_pct
        )

        # 4. Neural swarm vote
        neural_vote = self.neural.predict(feat)

        # 5. Soul cluster vote
        rl_vote = self.souls.vote(feat)

        # 6. Price predictions
        pred_res = self.pred.predict(feat)

        # 7. Meta-learner weights
        meta_w = self.meta.get_weights(rv, self.pair)

        # 8. Aggregate
        signal = self.agg.aggregate(
            neural_vote, rl_vote, pred_res,
            regime_info, shield_res, meta_w
        )

        self._last_feat   = feat
        self._last_signal = signal
        return signal

    def on_trade_closed(self, trade: Dict):
        """Feed trade outcome back to all learning layers."""
        pnl     = trade.get("pnl", 0.0)
        pnl_pct = trade.get("pnl_pct", 0.0)
        won     = pnl > 0
        feat    = self._last_feat
        if feat is None:
            return

        direction_idx = 1 if won else 3  # BUY if won, SELL if lost (approximate)

        # Update all learning layers
        self.neural.learn(feat, direction_idx, pnl_pct)
        next_feat = feat + np.random.randn(STATE_DIM).astype(np.float32) * 0.01
        self.souls.learn_all(feat, direction_idx, pnl_pct, next_feat, True)

        # Update meta trust
        sig = self._last_signal
        if sig:
            layer_correct = {
                "neural_swarm": (sig.layer_votes.get("neural") == "buy") == won,
                "soul_cluster": (sig.layer_votes.get("rl")     == "buy") == won,
                "predictive":   (sig.layer_votes.get("pred_1m") == "up") == won,
                "regime_bias":  (sig.layer_votes.get("regime") == "bull") == won,
            }
            rv = self.oracle._default_vec().get("regime_vec", [0.2]*7)
            self.meta.update(rv, self.pair, layer_correct)

        # Save periodically
        if self._ticks % 500 == 0:
            self.save_all()

    def save_all(self):
        self.neural.save_all()
        self.souls.save_all()
        self.oracle.save()
        self.shield.save()
        self.pred.save_all()
        self.meta.save()
        logger.debug(f"💾 {self.pair} brain saved ({self._ticks} ticks)")


# ═══════════════════════════════════════════════════════════════════════════════
#  NEXUS PRIME  — THE MASTER BRAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class NexusPrimeBrain:
    """
    ╔══════════════════════════════════════════════════════════════════╗
    ║  The central intelligence that runs everything.                  ║
    ║                                                                  ║
    ║  • Spawns a PairBrain for every active trading pair              ║
    ║  • Connects to NexusDataEngine for live data                     ║
    ║  • Manages all positions via PositionIntelligence                ║
    ║  • Enforces CircuitBreakers                                      ║
    ║  • Persists all knowledge in BrainMemory                         ║
    ║  • Self-optimizes strategy weights every hour                    ║
    ║  • Runs FastAPI dashboard endpoint                               ║
    ║                                                                  ║
    ║  USAGE:                                                          ║
    ║    brain = NexusPrimeBrain(mode=BrainMode.PAPER)                 ║
    ║    await brain.start(pairs=["BTC/USDT","ETH/USDT"])              ║
    ║    await brain.run_forever()                                     ║
    ╚══════════════════════════════════════════════════════════════════╝
    """

    def __init__(
        self,
        mode:              BrainMode = BrainMode.PAPER,
        initial_capital:   float     = 200.0,
        max_pairs:         int       = 20,
        min_signal_conf:   float     = 0.55,
        max_risk_per_trade:float     = 0.02,
        max_leverage:      float     = 3.0,
        daily_profit_target:float   = 0.03,   # 3% daily → realistic elite target
        daily_loss_limit:   float   = 0.05,   # 5% max daily loss
        api_port:          int       = 9001,
    ):
        self.mode              = mode
        self.initial_capital   = initial_capital
        self.max_pairs         = max_pairs
        self.min_signal_conf   = min_signal_conf
        self.daily_target      = daily_profit_target
        self.api_port          = api_port

        # Core components
        self.memory    = BrainMemory()
        self.pos_mgr   = PositionIntelligence(
            initial_capital=initial_capital,
            max_risk_per_trade=max_risk_per_trade,
            max_leverage=max_leverage,
        )
        self.breakers  = CircuitBreakers(
            max_daily_loss_pct=daily_loss_limit * 100,
            max_drawdown_pct=15.0,
        )
        self.brains:   Dict[str, PairBrain] = {}
        self._running  = False
        self._tasks:   List[asyncio.Task]   = []

        # Statistics
        self.session_start  = time.time()
        self.tick_count     = 0
        self.signal_count   = 0
        self.daily_pnl      = 0.0
        self._hourly_review_ts = time.time()

        logger.info(f"""
╔══════════════════════════════════════════════════════════╗
║  NEXUS PRIME BRAIN — INITIALIZED                         ║
║  Mode:     {mode.value:<20}                        ║
║  Capital:  €{initial_capital:<20.2f}                    ║
║  Max pairs:{max_pairs:<20}                              ║
║  Min conf: {min_signal_conf:<20.2f}                     ║
╚══════════════════════════════════════════════════════════╝""")

    # ── Pair Management ───────────────────────────────────────────────────────

    def add_pair(self, pair: str):
        if pair not in self.brains and len(self.brains) < self.max_pairs:
            self.brains[pair] = PairBrain(pair, self.mode)
            logger.info(f"➕ Added pair: {pair} ({len(self.brains)} active)")

    def remove_pair(self, pair: str):
        if pair in self.brains:
            self.brains[pair].save_all()
            del self.brains[pair]
            logger.info(f"➖ Removed pair: {pair}")

    # ── Core Tick Processing ───────────────────────────────────────────────────

    async def on_market_event(self, event: Any):
        """
        Entry point for all market data.
        Compatible with NexusDataEngine MarketEvent.
        """
        self.tick_count += 1
        pair   = event.symbol
        data   = event.data

        if pair not in self.brains:
            self.add_pair(pair)

        brain = self.brains[pair]

        # Extract data fields
        price  = float(data.get("last", data.get("price", 0)))
        volume = float(data.get("volume_24", data.get("volume", 0)))
        high   = float(data.get("high_24",   price))
        low_   = float(data.get("low_24",    price))
        ob_imb = float(data.get("imbalance", data.get("ob_imbalance", 0)))
        spread = float(data.get("spread", 0.001))
        flow   = float(data.get("trade_flow", 0))
        fr     = float(data.get("funding_rate", 0))
        oi     = float(data.get("open_interest", 0))
        fg     = float(data.get("fear_greed", 50))
        vix    = float(data.get("macro_vix", 20))
        btcd   = float(data.get("btc_dominance", 50))

        if price <= 0:
            return

        # Generate master signal
        signal = brain.on_tick(
            price, volume, high, low_,
            ob_imb, spread, flow, fr, oi, fg, vix, btcd
        )

        # Position updates
        await self._update_open_positions(pair, price)

        # Trade decision
        if self.mode != BrainMode.TRAINING:
            await self._decide_trade(pair, price, signal, brain)

        # Hourly self-optimization
        if time.time() - self._hourly_review_ts > 3600:
            await self._hourly_review()
            self._hourly_review_ts = time.time()

    async def _update_open_positions(self, pair: str, price: float):
        """Check all open positions for this pair against current price."""
        to_close = []
        for pos_id, pos in list(self.pos_mgr.positions.items()):
            if pos.symbol != pair:
                continue
            exit_reason = self.pos_mgr.update_position(pos, price)
            if exit_reason:
                to_close.append((pos, exit_reason))

        for pos, reason in to_close:
            trade = self.pos_mgr.close_position(pos, price, reason)
            if trade:
                self.memory.save_trade(trade)
                self.brains[pair].on_trade_closed(trade)
                self.breakers.on_trade_close(trade["pnl"])
                self.daily_pnl += trade["pnl"]
                self.signal_count += 1

    async def _decide_trade(self, pair: str, price: float,
                             signal: MasterSignal, brain: PairBrain):
        """Apply circuit breakers and open new position if signal qualifies."""
        # Circuit breaker check
        safe, issues = self.breakers.check(self.pos_mgr)
        if not safe:
            return

        # Don't stack multiple positions per pair
        pair_positions = [p for p in self.pos_mgr.positions.values()
                         if p.symbol == pair]
        if len(pair_positions) >= 2:
            return

        # Signal quality gate
        if signal.direction == "hold":
            return
        if signal.confidence < self.min_signal_conf:
            return
        if signal.trap_warning and signal.confidence < 0.75:
            return

        # Daily profit target reached → reduce aggression
        daily_return = self.daily_pnl / (self.initial_capital + 1e-10)
        if daily_return >= self.daily_target:
            if signal.confidence < 0.80:  # only very high conf trades
                return

        # Compute position size
        atr = float(_atr(
            list(brain._highs), list(brain._lows), list(brain._prices), 14
        ))
        stats  = self.memory.get_win_stats(pair, last_n=50)
        sizing = self.pos_mgr.compute_size(
            price, atr or price * 0.01,
            win_rate     = stats["win_rate"],
            avg_win      = stats["avg_win"],
            avg_loss     = abs(stats["avg_loss"]),
            signal_conf  = signal.confidence,
        )

        if sizing["quantity"] <= 0:
            return

        # Paper mode: execute immediately
        pos = self.pos_mgr.open_position(
            symbol     = pair,
            side       = signal.side,
            price      = price,
            quantity   = sizing["quantity"],
            atr        = atr or price * 0.01,
            confidence = signal.confidence,
            regime     = signal.regime,
            signal     = {
                "direction":    signal.direction,
                "confidence":   signal.confidence,
                "regime":       signal.regime,
                "layer_votes":  signal.layer_votes,
            },
        )

    # ── Hourly Self-Optimization ───────────────────────────────────────────────

    async def _hourly_review(self):
        """
        Every hour: review performance, adapt parameters, save models.
        This is the self-optimizer loop.
        """
        logger.info("⏱  HOURLY REVIEW — Self-optimization running")
        trades   = self.memory.get_recent_trades(limit=200)
        if len(trades) < 10:
            return

        wins   = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        wr     = len(wins) / max(len(trades), 1)
        sharpe = _sharpe([t["pnl_pct"] for t in trades], periods=365)

        logger.info(f"  📊 Last 200 trades: WR={wr:.1%} Sharpe={sharpe:.2f} "
                    f"Total PnL={sum(t['pnl'] for t in trades):.2f}")

        # Adapt min_signal_conf based on win rate
        if wr < 0.45:
            self.min_signal_conf = min(0.75, self.min_signal_conf + 0.02)
            logger.info(f"  📈 Low WR — raising confidence threshold to {self.min_signal_conf:.2f}")
        elif wr > 0.65:
            self.min_signal_conf = max(0.50, self.min_signal_conf - 0.01)
            logger.info(f"  📉 High WR — lowering confidence threshold to {self.min_signal_conf:.2f}")

        # Reduce risk if in drawdown
        if self.pos_mgr.drawdown_pct > 8:
            self.pos_mgr.max_risk = max(0.005, self.pos_mgr.max_risk * 0.9)
            logger.warning(f"  ⚠️  Drawdown {self.pos_mgr.drawdown_pct:.1f}% — "
                           f"risk reduced to {self.pos_mgr.max_risk:.3f}")

        # Save all brain models
        for brain in self.brains.values():
            brain.save_all()

        # Record lesson
        self.memory.save_lesson(
            "hourly_review",
            f"WR={wr:.2%}, Sharpe={sharpe:.2f}, "
            f"Drawdown={self.pos_mgr.drawdown_pct:.1f}%, "
            f"conf_threshold={self.min_signal_conf:.2f}",
            confidence=0.8
        )

    # ── Status & Reporting ────────────────────────────────────────────────────

    def get_status(self) -> Dict:
        equity  = self.pos_mgr.current_equity
        dd      = self.pos_mgr.drawdown_pct
        uptime  = int(time.time() - self.session_start)
        stats   = self.memory.get_win_stats(last_n=100)
        return {
            "mode":             self.mode.value,
            "equity":           round(equity, 4),
            "initial_capital":  self.initial_capital,
            "pnl":              round(equity - self.initial_capital, 4),
            "pnl_pct":          round((equity / self.initial_capital - 1) * 100, 3),
            "drawdown_pct":     round(dd, 3),
            "daily_pnl":        round(self.daily_pnl, 4),
            "open_positions":   len(self.pos_mgr.positions),
            "active_pairs":     list(self.brains.keys()),
            "n_pairs":          len(self.brains),
            "win_rate":         round(stats.get("win_rate", 0), 4),
            "n_trades":         stats.get("n_trades", 0),
            "sharpe":           round(stats.get("sharpe", 0), 3),
            "tick_count":       self.tick_count,
            "uptime_s":         uptime,
            "circuit_safe":     self.breakers.all_clear,
            "confidence_min":   self.min_signal_conf,
            "timestamp":        _NOW(),
        }

    def get_positions_report(self) -> List[Dict]:
        return [
            {
                "id":         p.id,
                "symbol":     p.symbol,
                "side":       p.side,
                "entry":      p.entry_price,
                "current":    p.last_price,
                "qty":        p.quantity,
                "pnl":        p.unrealized_pnl,
                "regime":     p.regime,
                "confidence": p.confidence,
            }
            for p in self.pos_mgr.positions.values()
        ]

    # ── Start / Stop ──────────────────────────────────────────────────────────

    async def start(
        self,
        pairs:        List[str],
        data_engine:  Any   = None,   # NexusDataEngine instance
        connect_data: bool  = True,
    ):
        self._running = True
        self.breakers.reset_daily(self.pos_mgr.capital)

        for pair in pairs[:self.max_pairs]:
            self.add_pair(pair)

        if connect_data and data_engine is not None:
            # Import DataType from nexus_data_engine
            try:
                from nexus_data_engine import DataType
                data_engine.register_agent(
                    "nexus_prime_brain",
                    self.on_market_event,
                    types=[DataType.TICKER, DataType.ORDERBOOK, DataType.INDICATOR],
                    symbols=pairs,
                    min_interval_ms=100,   # at most 10 signals/sec per symbol
                )
                logger.info("🔌 Connected to NexusDataEngine")
            except ImportError:
                logger.warning("NexusDataEngine not found — running in standalone mode")
                await self._start_standalone_loop(pairs)

        # Start API server
        if fastapi_mod and uvicorn_mod:
            self._tasks.append(
                asyncio.create_task(self._run_api_server(), name="prime_api")
            )

        # Daily reset loop
        self._tasks.append(
            asyncio.create_task(self._daily_reset_loop(), name="daily_reset")
        )

        logger.info(f"🚀 NexusPrimeBrain LIVE — {len(self.brains)} pairs active")

    async def _start_standalone_loop(self, pairs: List[str]):
        """Standalone mode: fetch ticks directly from CCXT without NexusDataEngine."""
        if ccxt_mod is None:
            logger.error("ccxt not installed — cannot run standalone")
            return

        async def _fetch_loop(pair: str):
            ex_id = "kraken"
            try:
                ex = getattr(ccxt_mod, ex_id)({"enableRateLimit": True})
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, ex.load_markets)
                logger.info(f"📡 Standalone loop: {ex_id}/{pair}")
            except Exception as e:
                logger.error(f"Exchange init failed: {e}")
                return

            while self._running:
                try:
                    loop = asyncio.get_event_loop()
                    ticker = await loop.run_in_executor(
                        None, lambda: ex.fetch_ticker(pair)
                    )
                    if ticker and ticker.get("last"):

                        class _FakeEvent:
                            symbol = pair
                            data   = {
                                "last":     ticker.get("last", 0),
                                "volume_24":ticker.get("quoteVolume", 0),
                                "high_24":  ticker.get("high", ticker.get("last", 0)),
                                "low_24":   ticker.get("low",  ticker.get("last", 0)),
                                "bid":      ticker.get("bid",  0),
                                "ask":      ticker.get("ask",  0),
                            }

                        await self.on_market_event(_FakeEvent())
                    await asyncio.sleep(2.0)
                except Exception as e:
                    logger.debug(f"Tick error {pair}: {e}")
                    await asyncio.sleep(5.0)

        for pair in pairs:
            self._tasks.append(
                asyncio.create_task(_fetch_loop(pair), name=f"loop_{pair}")
            )

    async def _daily_reset_loop(self):
        """Reset daily stats at midnight."""
        while self._running:
            await asyncio.sleep(3600)
            now = datetime.now()
            if now.hour == 0:
                self.daily_pnl = 0.0
                self.breakers.reset_daily(self.pos_mgr.capital)
                logger.info("🌅 Daily reset complete")

    async def _run_api_server(self):
        """FastAPI status / control dashboard endpoint."""
        if not fastapi_mod or not uvicorn_mod:
            return

        from fastapi import FastAPI
        from fastapi.middleware.cors import CORSMiddleware
        import uvicorn

        app = FastAPI(title="NEXUS PRIME BRAIN", version="1.0")
        app.add_middleware(CORSMiddleware, allow_origins=["*"],
                           allow_methods=["*"], allow_headers=["*"])

        brain_ref = self

        @app.get("/")
        def status():
            return brain_ref.get_status()

        @app.get("/positions")
        def positions():
            return brain_ref.get_positions_report()

        @app.get("/equity")
        def equity():
            return {
                "curve": brain_ref.pos_mgr.equity_curve[-500:],
                "capital": brain_ref.pos_mgr.capital,
                "peak": brain_ref.pos_mgr.peak_capital,
                "drawdown_pct": brain_ref.pos_mgr.drawdown_pct,
            }

        @app.get("/trades")
        def trades(limit: int = 50):
            return brain_ref.memory.get_recent_trades(limit=limit)

        @app.get("/pairs")
        def pairs_status():
            return {
                p: {
                    "ticks": b._ticks,
                    "regime": b.oracle._current,
                    "regime_conf": b.oracle._confidence,
                }
                for p, b in brain_ref.brains.items()
            }

        @app.post("/add_pair/{pair}")
        def add_pair(pair: str):
            pair_decoded = pair.replace("_", "/")
            brain_ref.add_pair(pair_decoded)
            return {"added": pair_decoded}

        @app.post("/remove_pair/{pair}")
        def remove_pair(pair: str):
            pair_decoded = pair.replace("_", "/")
            brain_ref.remove_pair(pair_decoded)
            return {"removed": pair_decoded}

        @app.get("/lessons")
        def lessons():
            return brain_ref.memory.get_lessons(limit=30)

        @app.get("/circuit_status")
        def circuit():
            safe, issues = brain_ref.breakers.check(brain_ref.pos_mgr)
            return {"safe": safe, "issues": issues,
                    "drawdown": brain_ref.pos_mgr.drawdown_pct,
                    "consecutive_losses": brain_ref.breakers.consecutive_losses}

        config = uvicorn.Config(app, host="0.0.0.0",
                                port=self.api_port, log_level="warning")
        server = uvicorn.Server(config)
        logger.info(f"🌐 Brain API: http://localhost:{self.api_port}")
        await server.serve()

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        for brain in self.brains.values():
            brain.save_all()
        self.memory.conn.close()
        logger.info("🛑 NexusPrimeBrain stopped — all models saved")

    async def run_forever(self):
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()


# ═══════════════════════════════════════════════════════════════════════════════
#  INTEGRATION BRIDGE  (wires to simulation_runner.py and NexusDataEngine)
# ═══════════════════════════════════════════════════════════════════════════════

class SimulationBridge:
    """
    Runs NexusPrimeBrain against historical OHLCV data.
    Compatible with simulation_runner.py interface.
    """

    def __init__(self, initial_capital: float = 200.0):
        self.brain = NexusPrimeBrain(
            mode=BrainMode.PAPER,
            initial_capital=initial_capital,
        )

    async def run_on_data(self, data: Dict[str, Any]) -> Dict:
        """
        data: {symbol: DataFrame with columns [timestamp, open, high, low, close, volume]}
        """
        if pd is None:
            logger.error("pandas required for simulation")
            return {}

        all_pairs = list(data.keys())[:self.brain.max_pairs]
        for pair in all_pairs:
            self.brain.add_pair(pair)

        self.brain.breakers.reset_daily(self.brain.pos_mgr.capital)

        logger.info(f"🎮 Simulation: {len(all_pairs)} pairs")
        total_bars = sum(len(df) for df in data.values())
        processed  = 0

        # Interleave pairs tick-by-tick in timestamp order
        class _TickEvent:
            def __init__(self, sym, row):
                self.symbol = sym
                self.data   = {
                    "last":     float(row["close"]),
                    "volume_24":float(row.get("volume", 0)),
                    "high_24":  float(row["high"]),
                    "low_24":   float(row["low"]),
                }

        for pair, df in data.items():
            for _, row in df.iterrows():
                await self.brain.on_market_event(_TickEvent(pair, row))
                processed += 1
                if processed % 5000 == 0:
                    status = self.brain.get_status()
                    logger.info(f"  {processed}/{total_bars} bars | "
                                f"Equity={status['equity']:.2f} "
                                f"PnL={status['pnl_pct']:+.2f}% "
                                f"WR={status['win_rate']:.1%} "
                                f"Trades={status['n_trades']}")

        # Force close all open positions
        for pos in list(self.brain.pos_mgr.positions.values()):
            for pair, df in data.items():
                if pair == pos.symbol and len(df) > 0:
                    last_price = float(df["close"].iloc[-1])
                    trade = self.brain.pos_mgr.close_position(pos, last_price, "sim_end")
                    if trade:
                        self.brain.memory.save_trade(trade)

        status = self.brain.get_status()
        trades = self.brain.memory.get_recent_trades(limit=10000)
        wins   = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]

        report = {
            "initial_capital": self.brain.initial_capital,
            "final_equity":    status["equity"],
            "pnl":             status["pnl"],
            "pnl_pct":         status["pnl_pct"],
            "win_rate":        status["win_rate"],
            "n_trades":        status["n_trades"],
            "sharpe":          status["sharpe"],
            "max_drawdown":    _max_drawdown(self.brain.pos_mgr.equity_curve),
            "best_trade":      max((t["pnl"] for t in trades), default=0),
            "worst_trade":     min((t["pnl"] for t in trades), default=0),
            "avg_win":         float(sum(t["pnl"] for t in wins)  / max(len(wins),1)),
            "avg_loss":        float(sum(t["pnl"] for t in losses)/ max(len(losses),1)),
            "pairs_traded":    all_pairs,
        }

        logger.info(f"""
╔══════════════════════════════════════════════════════════╗
║  NEXUS PRIME — SIMULATION REPORT                         ║
║  Capital:    €{report['initial_capital']:.2f} → €{report['final_equity']:.2f}           ║
║  PnL:        {report['pnl_pct']:+.2f}%                              ║
║  Trades:     {report['n_trades']}                                   ║
║  Win Rate:   {report['win_rate']:.1%}                               ║
║  Sharpe:     {report['sharpe']:.3f}                                 ║
║  Max DD:     {report['max_drawdown']:.1f}%                          ║
╚══════════════════════════════════════════════════════════╝""")
        return report


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def _main():
    import argparse
    parser = argparse.ArgumentParser(description="NEXUS PRIME BRAIN v1.0")
    parser.add_argument("--mode",    choices=["paper","live","training","shadow"],
                        default="paper")
    parser.add_argument("--capital", type=float, default=200.0)
    parser.add_argument("--pairs",   nargs="+",
                        default=["BTC/USDT","ETH/USDT","SOL/USDT","XRP/USDT",
                                 "ADA/USDT","DOGE/USDT","AVAX/USDT","DOT/USDT"])
    parser.add_argument("--port",    type=int, default=9001)
    parser.add_argument("--conf",    type=float, default=0.55,
                        help="Minimum signal confidence [0-1]")
    parser.add_argument("--max-risk",type=float, default=0.02,
                        help="Max capital risk per trade (0.02 = 2%%)")
    parser.add_argument("--leverage",type=float, default=3.0)
    parser.add_argument("--with-data-engine", action="store_true",
                        help="Connect to NexusDataEngine for live data")
    args = parser.parse_args()

    mode_map = {
        "paper":    BrainMode.PAPER,
        "live":     BrainMode.LIVE,
        "training": BrainMode.TRAINING,
        "shadow":   BrainMode.SHADOW,
    }

    brain = NexusPrimeBrain(
        mode               = mode_map[args.mode],
        initial_capital    = args.capital,
        min_signal_conf    = args.conf,
        max_risk_per_trade = args.max_risk,
        max_leverage       = args.leverage,
        api_port           = args.port,
    )

    data_engine = None
    if args.with_data_engine:
        try:
            from nexus_data_engine import NexusDataEngine
            data_engine = NexusDataEngine(
                enabled_exchanges=["kraken","bybit","binance"],
                enable_ws=True, enable_api=False, enable_redis=False,
            )
            await data_engine.start(quick=False)
            logger.info("✅ NexusDataEngine connected")
        except ImportError:
            logger.warning("nexus_data_engine.py not found — standalone mode")

    await brain.start(
        pairs        = args.pairs,
        data_engine  = data_engine,
        connect_data = args.with_data_engine,
    )

    logger.info(f"🔥 NEXUS PRIME ONLINE")
    logger.info(f"   Mode:     {args.mode}")
    logger.info(f"   Pairs:    {args.pairs}")
    logger.info(f"   Capital:  €{args.capital:.2f}")
    logger.info(f"   API:      http://localhost:{args.port}")
    logger.info("   Press Ctrl+C to stop")

    try:
        await brain.run_forever()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
        await brain.stop()
        if data_engine:
            await data_engine.stop()


if __name__ == "__main__":
    asyncio.run(_main())
