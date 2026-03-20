"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                      ║
║  ███████╗██╗    ██╗ █████╗ ██████╗ ███╗   ███╗                                    ║
║  ██╔════╝██║    ██║██╔══██╗██╔══██╗████╗ ████║                                    ║
║  ███████╗██║ █╗ ██║███████║██████╔╝██╔████╔██║                                    ║
║  ╚════██║██║███╗██║██╔══██║██╔══██╗██║╚██╔╝██║                                    ║
║  ███████║╚███╔███╔╝██║  ██║██║  ██║██║ ╚═╝ ██║                                    ║
║  ╚══════╝ ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝                                    ║
║                                                                                      ║
║  ██╗  ██╗██╗██╗    ██╗    ██████╗  ██████╗ ████████╗███████╗                      ║
║  ██║ ██╔╝██║██║    ██║    ██╔══██╗██╔═══██╗╚══██╔══╝██╔════╝                      ║
║  █████╔╝ ██║██║    ██║    ██████╔╝██║   ██║   ██║   ███████╗                      ║
║  ██╔═██╗ ██║██║    ██║    ██╔══██╗██║   ██║   ██║   ╚════██║                      ║
║  ██║  ██╗██║███████╗███████╗██████╔╝╚██████╔╝   ██║   ███████║                    ║
║  ╚═╝  ╚═╝╚═╝╚══════╝╚══════╝╚═════╝  ╚═════╝   ╚═╝   ╚══════╝                    ║
║                                                                                      ║
║  S C A L P   S W A R M   v 1 . 0  —  1 0 0 0   A U T O N O M O U S   B O T S     ║
║                                                                                      ║
║  Architecture:                                                                       ║
║  ● PairRegistry      — discovers ALL tradeable pairs across 15 exchanges            ║
║  ● PairAuction       — scores pairs by volatility/spread/volume, assigns 1-per-bot ║
║  ● ScalpBot ×1000    — each owns one pair forever, hunts 0.1% moves long/short     ║
║  ● MicroSignalEngine — sub-100ms tick-level signal: OB imbalance + tape + EMA      ║
║  ● NanoExecutor      — order lifecycle: entry → SL/TP → exit in <50ms logic        ║
║  ● SwarmIntelligence — bots share learned thresholds via shared memory ring        ║
║  ● SwarmOrchestrator — asyncio event loop, 1000 concurrent coroutines              ║
║  ● SwarmDashboard    — live terminal + FastAPI /ws broadcast                        ║
║                                                                                      ║
║  Target: 200 trades/bot/day × 0.1% avg = 20% gross/bot/day                        ║
║  After fees (0.05% maker × 2): net ~18% gross — still needs REAL validation        ║
║                                                                                      ║
║  ┌──────────────────────────────────────────────────────────────────────────────┐  ║
║  │  ⚠️  HONEST NOTE: 200 trades/day at 0.1% target is aggressive scalping.     │  ║
║  │  Exchange rate limits, slippage, and spread will reduce real returns.        │  ║
║  │  Always run PAPER mode first. Validate pair selection before going live.     │  ║
║  └──────────────────────────────────────────────────────────────────────────────┘  ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ── stdlib ─────────────────────────────────────────────────────────────────────
import asyncio
import collections
import dataclasses
import hashlib
import json
import logging
import math
import multiprocessing
import os
import random
import sqlite3
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np

# ── optional ──────────────────────────────────────────────────────────────────
def _try(n):
    try: return __import__(n)
    except ImportError: return None

ccxt_mod    = _try("ccxt")
aiohttp_mod = _try("aiohttp")
fastapi_mod = _try("fastapi")
uvicorn_mod = _try("uvicorn")
rich_mod    = _try("rich")

# ── logging ───────────────────────────────────────────────────────────────────
for d in ["swarm_data", "swarm_data/bots", "swarm_data/trades", "swarm_data/db", "logs"]:
    Path(d).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-16s │ %(levelname)-7s │ %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/swarm_{int(time.time())}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("NEXUS·SWARM")

UTC = timezone.utc
_NOW = lambda: datetime.now(UTC).isoformat()
_MS  = lambda: int(time.monotonic() * 1000)


# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

TARGET_BOTS         = 1000
TARGET_PROFIT_PCT   = 0.10          # 0.1% per trade gross
FEE_RATE            = 0.0005        # 0.05% maker fee per side (Bybit/OKX)
NET_TARGET_PCT      = TARGET_PROFIT_PCT / 100 - FEE_RATE * 2   # ~0.0%... tight
STOP_LOSS_PCT       = 0.0008        # 0.08% stop loss
TAKE_PROFIT_PCT     = 0.0010        # 0.10% take profit
MAX_HOLD_SECONDS    = 90            # force-exit stale positions
TICK_INTERVAL_MS    = 250           # poll interval per bot (4× per second)
SHARED_RING_SIZE    = 2048          # shared signal memory slots
SWARM_DATA          = Path("swarm_data")
DB_PATH             = SWARM_DATA / "db" / "swarm.db"


# ══════════════════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

class BotStatus(Enum):
    IDLE      = "idle"
    SCANNING  = "scanning"
    LONG      = "long"
    SHORT     = "short"
    CLOSING   = "closing"
    PAUSED    = "paused"
    DEAD      = "dead"


@dataclass
class PairScore:
    symbol:         str
    exchange:       str
    score:          float        # composite suitability score
    spread_pct:     float        # average spread %
    vol_1h_pct:     float        # hourly volatility %
    volume_24h:     float        # USD volume
    ticks_per_hour: float        # estimated tick frequency
    min_qty:        float        # minimum order size
    price:          float        # current price


@dataclass
class BotTrade:
    bot_id:       int
    symbol:       str
    exchange:     str
    side:         str            # "long" / "short"
    entry_price:  float
    exit_price:   float  = 0.0
    qty:          float  = 0.0
    pnl:          float  = 0.0
    pnl_pct:      float  = 0.0
    fees:         float  = 0.0
    duration_ms:  int    = 0
    exit_reason:  str    = ""
    signal_score: float  = 0.0
    entry_ts:     int    = field(default_factory=_MS)
    exit_ts:      int    = 0


@dataclass
class BotState:
    bot_id:         int
    symbol:         str
    exchange:       str
    status:         BotStatus = BotStatus.IDLE
    capital:        float     = 0.0
    n_trades:       int       = 0
    n_wins:         int       = 0
    total_pnl:      float     = 0.0
    daily_pnl:      float     = 0.0
    win_rate:       float     = 0.5
    last_price:     float     = 0.0
    position_side:  str       = ""
    position_entry: float     = 0.0
    position_qty:   float     = 0.0
    position_ts:    int       = 0
    consecutive_losses: int   = 0
    paused_until:   float     = 0.0
    last_signal:    float     = 0.0   # signal score
    ticks_today:    int       = 0


# ══════════════════════════════════════════════════════════════════════════════
#  SWARM DATABASE  (single SQLite, WAL mode, handles 1000 writers)
# ══════════════════════════════════════════════════════════════════════════════

class SwarmDB:
    """
    Lightweight SQLite store for all 1000 bots.
    Batch-writes every 500ms to avoid lock contention.
    """

    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self._conn = sqlite3.connect(str(path), check_same_thread=False,
                                     timeout=10.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA cache_size=10000")
        self._lock = threading.Lock()
        self._write_queue: List[Tuple] = []
        self._init()

    def _init(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS bots (
                bot_id INTEGER PRIMARY KEY,
                symbol TEXT, exchange TEXT,
                n_trades INTEGER DEFAULT 0,
                n_wins INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                daily_pnl REAL DEFAULT 0,
                win_rate REAL DEFAULT 0.5,
                capital REAL DEFAULT 0,
                status TEXT DEFAULT 'idle',
                last_updated TEXT
            );
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                bot_id INTEGER, symbol TEXT, exchange TEXT,
                side TEXT, entry_price REAL, exit_price REAL,
                qty REAL, pnl REAL, pnl_pct REAL, fees REAL,
                duration_ms INTEGER, exit_reason TEXT,
                signal_score REAL, entry_ts INTEGER, exit_ts INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_trades_bot ON trades(bot_id, entry_ts);
            CREATE INDEX IF NOT EXISTS idx_trades_ts  ON trades(entry_ts);
            CREATE TABLE IF NOT EXISTS swarm_metrics (
                ts INTEGER PRIMARY KEY,
                total_bots INTEGER, active_bots INTEGER,
                trades_last_min INTEGER, pnl_last_min REAL,
                total_pnl REAL, total_trades INTEGER,
                win_rate REAL
            );
            CREATE TABLE IF NOT EXISTS pair_registry (
                symbol TEXT, exchange TEXT,
                score REAL, spread_pct REAL, vol_1h_pct REAL,
                volume_24h REAL, assigned_bot INTEGER DEFAULT -1,
                last_scored TEXT,
                PRIMARY KEY (symbol, exchange)
            );
        """)
        self._conn.commit()

    def upsert_bot(self, s: BotState):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO bots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (s.bot_id, s.symbol, s.exchange, s.n_trades, s.n_wins,
                 s.total_pnl, s.daily_pnl, s.win_rate, s.capital,
                 s.status.value, _NOW())
            )

    def flush_bots(self, states: List[BotState]):
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO bots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [(s.bot_id, s.symbol, s.exchange, s.n_trades, s.n_wins,
                  s.total_pnl, s.daily_pnl, s.win_rate, s.capital,
                  s.status.value, _NOW()) for s in states]
            )
            self._conn.commit()

    def save_trade(self, t: BotTrade):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:8], t.bot_id, t.symbol, t.exchange,
                 t.side, t.entry_price, t.exit_price, t.qty,
                 t.pnl, t.pnl_pct, t.fees, t.duration_ms,
                 t.exit_reason, t.signal_score, t.entry_ts, t.exit_ts)
            )

    def batch_save_trades(self, trades: List[BotTrade]):
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(str(uuid.uuid4())[:8], t.bot_id, t.symbol, t.exchange,
                  t.side, t.entry_price, t.exit_price, t.qty,
                  t.pnl, t.pnl_pct, t.fees, t.duration_ms,
                  t.exit_reason, t.signal_score, t.entry_ts, t.exit_ts)
                 for t in trades]
            )
            self._conn.commit()

    def log_swarm_metrics(self, m: Dict):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO swarm_metrics VALUES (?,?,?,?,?,?,?,?)",
                (int(time.time()), m["total_bots"], m["active_bots"],
                 m["trades_last_min"], m["pnl_last_min"],
                 m["total_pnl"], m["total_trades"], m["win_rate"])
            )
            self._conn.commit()

    def get_swarm_summary(self) -> Dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*), SUM(n_trades), SUM(total_pnl), AVG(win_rate) FROM bots"
            ).fetchone()
        return {
            "bots": row[0] or 0, "trades": row[1] or 0,
            "total_pnl": row[2] or 0.0, "avg_win_rate": row[3] or 0.0,
        }

    def get_top_bots(self, n: int = 20) -> List[Dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT bot_id, symbol, exchange, total_pnl, n_trades, win_rate "
                "FROM bots ORDER BY total_pnl DESC LIMIT ?", (n,)
            ).fetchall()
        return [{"bot_id": r[0], "symbol": r[1], "exchange": r[2],
                 "pnl": r[3], "trades": r[4], "wr": r[5]} for r in rows]

    def get_recent_trades(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT bot_id, symbol, side, pnl, pnl_pct, duration_ms, exit_reason "
                "FROM trades ORDER BY exit_ts DESC LIMIT ?", (limit,)
            ).fetchall()
        return [{"bot": r[0], "sym": r[1], "side": r[2],
                 "pnl": r[3], "pnl_pct": r[4],
                 "dur_ms": r[5], "reason": r[6]} for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
#  PAIR REGISTRY  (discovers and scores all available pairs)
# ══════════════════════════════════════════════════════════════════════════════

class PairRegistry:
    """
    Discovers ALL tradeable perpetual/spot pairs across configured exchanges.
    Scores each pair for scalping suitability:
      • High hourly volatility (need 0.1% moves)
      • Low spread (< 0.05% ideal)
      • High volume (liquidity for fast fills)
      • Good tick frequency
    Returns ranked list — top N assigned 1-per-bot.
    """

    EXCHANGES = ["bybit", "binance", "okx", "gate", "mexc",
                 "bitget", "kucoin", "huobi", "bitmex", "phemex"]
    QUOTE_CURRENCIES = ["USDT", "USD", "USDC"]
    MIN_VOLUME_24H = 100_000       # USD
    MAX_SPREAD_PCT = 0.05          # 0.05%
    MIN_VOL_1H_PCT = 0.05          # need at least 0.05% hourly vol

    def __init__(self, api_keys: Optional[Dict] = None,
                 exchanges: Optional[List[str]] = None):
        self.api_keys  = api_keys or {}
        self._exchange_ids = exchanges or self.EXCHANGES
        self._loaded: Dict[str, Any] = {}
        self._scores: List[PairScore] = []
        self._assigned: Set[str] = set()   # "exchange:symbol" strings

    async def load_exchanges(self) -> int:
        if ccxt_mod is None:
            log.warning("ccxt not installed — using synthetic pairs")
            return 0
        loaded = 0
        for ex_id in self._exchange_ids:
            try:
                cls = getattr(ccxt_mod, ex_id, None)
                if cls is None:
                    continue
                creds = self.api_keys.get(ex_id, {})
                ex = cls({"enableRateLimit": True, "timeout": 15_000, **creds})
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, ex.load_markets)
                self._loaded[ex_id] = ex
                loaded += 1
                log.info(f"  📡 {ex_id}: {len(ex.symbols)} symbols loaded")
            except Exception as e:
                log.debug(f"  ⚠️  {ex_id}: {e}")
        return loaded

    async def score_pairs(self) -> List[PairScore]:
        """Score all pairs across all loaded exchanges."""
        log.info("🔍 Scoring pairs for scalping suitability...")
        scores: List[PairScore] = []

        for ex_id, ex in self._loaded.items():
            try:
                loop = asyncio.get_event_loop()
                # Fetch tickers for all symbols at once
                raw_tickers = await loop.run_in_executor(None, ex.fetch_tickers)
            except Exception as e:
                log.debug(f"fetch_tickers {ex_id}: {e}")
                continue

            for sym, t in raw_tickers.items():
                # Filter: must have USDT/USD quote
                mkt = ex.markets.get(sym, {})
                if mkt.get("quote") not in self.QUOTE_CURRENCIES:
                    continue
                if not mkt.get("active", True):
                    continue

                bid  = float(t.get("bid")  or 0)
                ask  = float(t.get("ask")  or 0)
                last = float(t.get("last") or t.get("close") or 0)
                vol  = float(t.get("quoteVolume") or t.get("baseVolume") or 0)
                hi   = float(t.get("high") or last)
                lo   = float(t.get("low")  or last)

                if last <= 0 or bid <= 0 or ask <= 0:
                    continue
                if vol < self.MIN_VOLUME_24H:
                    continue

                spread_pct = float((ask - bid) / ((ask + bid) / 2 + 1e-10) * 100)
                if spread_pct > self.MAX_SPREAD_PCT:
                    continue

                # Estimate hourly volatility from 24h range
                vol_24h_pct = float((hi - lo) / (last + 1e-10) * 100)
                vol_1h_pct  = vol_24h_pct / 4   # rough: 24h vol / 4 = hourly estimate
                if vol_1h_pct < self.MIN_VOL_1H_PCT:
                    continue

                # Composite score: we want vol HIGH, spread LOW, volume HIGH
                score = (
                    0.45 * min(vol_1h_pct / 0.5, 1.0)     +  # volatility score
                    0.30 * (1.0 - spread_pct / self.MAX_SPREAD_PCT) +  # spread score
                    0.25 * min(math.log10(vol + 1) / 9, 1.0)  # volume score
                )

                min_qty = float(mkt.get("limits", {}).get("amount", {}).get("min", 1.0) or 1.0)

                scores.append(PairScore(
                    symbol=sym, exchange=ex_id, score=score,
                    spread_pct=spread_pct, vol_1h_pct=vol_1h_pct,
                    volume_24h=vol, ticks_per_hour=max(vol / (last + 1e-10) * 10, 60),
                    min_qty=min_qty, price=last,
                ))

        scores.sort(key=lambda x: x.score, reverse=True)
        self._scores = scores
        log.info(f"✅ Scored {len(scores)} pairs across "
                 f"{len(self._loaded)} exchanges")
        return scores

    def assign_pairs(self, n_bots: int) -> List[PairScore]:
        """
        Assign unique pairs to bots. Each (exchange, symbol) used at most once.
        Returns list of length n_bots (padded with synthetic if not enough real).
        """
        assigned: List[PairScore] = []
        seen: Set[str] = set()
        for ps in self._scores:
            key = f"{ps.exchange}:{ps.symbol}"
            if key not in seen:
                seen.add(key)
                assigned.append(ps)
                if len(assigned) >= n_bots:
                    break

        # Pad with synthetic pairs if we have fewer than n_bots real pairs
        if len(assigned) < n_bots:
            log.warning(f"Only {len(assigned)} real pairs — padding with synthetic")
            assigned.extend(self._synthetic_pairs(n_bots - len(assigned), len(assigned)))

        log.info(f"🎯 Pair assignment: {len(assigned)} unique pairs for {n_bots} bots")
        return assigned[:n_bots]

    @staticmethod
    def _synthetic_pairs(n: int, offset: int = 0) -> List[PairScore]:
        """Generate synthetic pairs for paper/testing when real exchanges unavailable."""
        base_coins = [
            "BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","DOT","LINK","LTC",
            "UNI","ATOM","ETC","BCH","FIL","TRX","XLM","NEAR","APT","ARB",
            "OP","MATIC","SAND","MANA","AXS","ENJ","CHZ","GALA","IMX","BLUR",
            "1INCH","AAVE","COMP","MKR","SNX","CRV","BAL","YFI","SUSHI","FXS",
            "LUNA","UST","FTT","SRM","RAY","SBF","DYDX","PERP","RBN","GMX",
        ]
        exchanges = ["bybit", "binance", "okx", "gate", "mexc",
                     "bitget", "kucoin", "huobi"]
        result = []
        for i in range(n):
            coin = base_coins[(offset + i) % len(base_coins)]
            suffix = (offset + i) // len(base_coins)
            sym = f"{coin}{'2' if suffix else ''}/USDT"
            ex  = exchanges[i % len(exchanges)]
            result.append(PairScore(
                symbol=sym, exchange=ex, score=0.5,
                spread_pct=0.02, vol_1h_pct=0.15,
                volume_24h=5_000_000, ticks_per_hour=300,
                min_qty=1.0, price=random.uniform(0.1, 50000),
            ))
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  MICRO SIGNAL ENGINE  (sub-100ms decision, per-bot)
# ══════════════════════════════════════════════════════════════════════════════

class MicroSignalEngine:
    """
    Ultra-fast signal generator optimised for scalping 0.1% moves.
    
    Signal sources (all computed from tick buffer, no heavy indicators):
    1. Order-book imbalance (bid vol vs ask vol) — primary
    2. Price momentum (EMA crossover, very short periods)
    3. Trade flow delta (buy volume vs sell volume)
    4. Microstructure: spread tightening / widening
    5. Candlestick body ratio (direction of last N ticks)
    
    Output: signal ∈ [-1, +1], direction = long/short/hold
    """

    # Tunable thresholds (adapted per bot via SwarmIntelligence)
    ENTRY_THRESHOLD  = 0.25    # |signal| > this → enter
    OB_WEIGHT        = 0.40
    MOMENTUM_WEIGHT  = 0.30
    FLOW_WEIGHT      = 0.20
    SPREAD_WEIGHT    = 0.10

    def __init__(self, bot_id: int):
        self.bot_id = bot_id
        # Tick-level buffers
        self._prices:  deque = deque(maxlen=120)   # 120 ticks ≈ 30s at 250ms
        self._bids:    deque = deque(maxlen=20)
        self._asks:    deque = deque(maxlen=20)
        self._spreads: deque = deque(maxlen=20)
        self._buy_vol: deque = deque(maxlen=20)
        self._sell_vol:deque = deque(maxlen=20)
        self._timestamps: deque = deque(maxlen=120)

        # Per-bot adaptive thresholds (updated by SwarmIntelligence)
        self.entry_threshold = self.ENTRY_THRESHOLD
        self._ema_fast = 0.0
        self._ema_slow = 0.0
        self._ema_init = False
        self._last_signal = 0.0

    def update(self, price: float, bid: float, ask: float,
               bid_vol: float, ask_vol: float,
               trade_side: str = "") -> float:
        """
        Feed one tick. Returns signal score ∈ [-1, +1].
        Positive = long signal. Negative = short signal.
        """
        self._prices.append(price)
        self._bids.append(bid)
        self._asks.append(ask)
        self._timestamps.append(_MS())

        spread = (ask - bid) / ((ask + bid) / 2 + 1e-10)
        self._spreads.append(spread)

        bv = float(bid_vol) if bid_vol > 0 else 0.0
        av = float(ask_vol) if ask_vol > 0 else 0.0
        self._buy_vol.append(bv if trade_side == "buy"  else 0.0)
        self._sell_vol.append(av if trade_side == "sell" else 0.0)

        if len(self._prices) < 10:
            return 0.0

        p = list(self._prices)

        # ── 1. Order Book Imbalance ──────────────────────────────────────────
        total_vol = bv + av + 1e-10
        ob_imbalance = float((bv - av) / total_vol)   # ∈ [-1, +1]

        # ── 2. Micro-Momentum (EMA cross on ticks) ───────────────────────────
        k8  = 2 / (8  + 1)
        k21 = 2 / (21 + 1)
        if not self._ema_init:
            self._ema_fast = price
            self._ema_slow = price
            self._ema_init = True
        self._ema_fast = price * k8  + self._ema_fast * (1 - k8)
        self._ema_slow = price * k21 + self._ema_slow * (1 - k21)
        ema_signal = float(
            (self._ema_fast - self._ema_slow) / (price + 1e-10) * 1000
        )
        ema_norm = float(math.tanh(ema_signal))

        # ── 3. Trade Flow ─────────────────────────────────────────────────────
        total_buy  = sum(self._buy_vol)  + 1e-10
        total_sell = sum(self._sell_vol) + 1e-10
        flow_ratio = float((total_buy - total_sell) / (total_buy + total_sell))

        # ── 4. Spread Signal (tightening = activity incoming) ─────────────────
        if len(self._spreads) >= 5:
            sp_now  = float(np.mean(list(self._spreads)[-3:]))
            sp_prev = float(np.mean(list(self._spreads)[-8:-3]))
            spread_change = float((sp_prev - sp_now) / (sp_prev + 1e-10))
            spread_signal = float(math.tanh(spread_change * 20))
        else:
            spread_signal = 0.0

        # ── 5. Tick Body Direction ────────────────────────────────────────────
        n = min(10, len(p))
        up_ticks   = sum(1 for i in range(-n+1, 0) if p[i] > p[i-1])
        down_ticks = sum(1 for i in range(-n+1, 0) if p[i] < p[i-1])
        direction_bias = float((up_ticks - down_ticks) / (n - 1 + 1e-10))

        # ── Composite signal ──────────────────────────────────────────────────
        raw = (
            self.OB_WEIGHT       * ob_imbalance  +
            self.MOMENTUM_WEIGHT * ema_norm       +
            self.FLOW_WEIGHT     * flow_ratio     +
            self.SPREAD_WEIGHT   * spread_signal
        )
        # Blend with tick direction
        signal = float(0.8 * raw + 0.2 * direction_bias)
        signal = float(np.clip(signal, -1.0, 1.0))
        self._last_signal = signal
        return signal

    def should_enter(self, signal: float) -> Tuple[bool, str]:
        """Returns (enter, side)."""
        if abs(signal) < self.entry_threshold:
            return False, ""
        side = "long" if signal > 0 else "short"
        return True, side

    def get_momentum(self) -> float:
        """Quick momentum check for exit timing."""
        if len(self._prices) < 5:
            return 0.0
        p = list(self._prices)
        return float((p[-1] - p[-5]) / (p[-5] + 1e-10))


# ══════════════════════════════════════════════════════════════════════════════
#  NANO EXECUTOR  (position lifecycle per bot, pure paper logic)
# ══════════════════════════════════════════════════════════════════════════════

class NanoExecutor:
    """
    Manages the open/close lifecycle for one bot's position.
    Sub-millisecond logic (pure Python, no I/O).
    
    For LIVE mode: wraps CCXT order placement calls.
    For PAPER mode: simulates fills with spread-adjusted prices.
    """

    def __init__(self, bot_id: int, symbol: str, exchange: str,
                 capital: float, fee_rate: float = FEE_RATE,
                 paper: bool = True):
        self.bot_id   = bot_id
        self.symbol   = symbol
        self.exchange = exchange
        self.capital  = capital
        self.fee_rate = fee_rate
        self.paper    = paper

        # Current position
        self.open:          bool  = False
        self.side:          str   = ""
        self.entry_price:   float = 0.0
        self.qty:           float = 0.0
        self.entry_ts:      int   = 0
        self.stop_loss:     float = 0.0
        self.take_profit:   float = 0.0
        self.signal_score:  float = 0.0

        # Session stats
        self.session_pnl:    float = 0.0
        self.session_trades: int   = 0
        self.session_wins:   int   = 0

        # Trade history (last 50)
        self._trade_hist: deque = deque(maxlen=50)

        # CCXT exchange instance (for live mode)
        self._ex = None

    def enter(self, side: str, price: float, signal_score: float,
              spread_pct: float = 0.0) -> Optional[BotTrade]:
        """Open a position. Returns trade object or None."""
        if self.open:
            return None

        # Spread-adjusted fill price (paper)
        half_spread = price * spread_pct / 2
        if self.paper:
            fill = price + half_spread if side == "long" else price - half_spread
        else:
            fill = price  # live: actual fill comes from exchange

        # Position size: use % of capital
        risk_capital = self.capital * 0.95   # use 95% per trade
        qty = risk_capital / (fill + 1e-10)

        # SL / TP
        if side == "long":
            sl = fill * (1 - STOP_LOSS_PCT)
            tp = fill * (1 + TAKE_PROFIT_PCT)
        else:
            sl = fill * (1 + STOP_LOSS_PCT)
            tp = fill * (1 - TAKE_PROFIT_PCT)

        fees = qty * fill * self.fee_rate
        self.capital -= fees   # entry fee

        self.open         = True
        self.side         = side
        self.entry_price  = fill
        self.qty          = qty
        self.entry_ts     = _MS()
        self.stop_loss    = sl
        self.take_profit  = tp
        self.signal_score = signal_score

        return BotTrade(
            bot_id=self.bot_id, symbol=self.symbol, exchange=self.exchange,
            side=side, entry_price=fill, qty=qty,
            signal_score=signal_score, entry_ts=self.entry_ts,
        )

    def should_exit(self, price: float) -> Tuple[bool, str]:
        """Check all exit conditions. Returns (exit, reason)."""
        if not self.open:
            return False, ""

        # SL / TP
        if self.side == "long":
            if price <= self.stop_loss:   return True, "stop_loss"
            if price >= self.take_profit: return True, "take_profit"
        else:
            if price >= self.stop_loss:   return True, "stop_loss"
            if price <= self.take_profit: return True, "take_profit"

        # Time-based: max hold
        if (_MS() - self.entry_ts) > MAX_HOLD_SECONDS * 1000:
            return True, "timeout"

        return False, ""

    def exit(self, price: float, reason: str,
             spread_pct: float = 0.0) -> Optional[BotTrade]:
        """Close position. Returns completed trade."""
        if not self.open:
            return None

        # Spread-adjusted exit
        half_spread = price * spread_pct / 2
        if self.paper:
            fill = price - half_spread if self.side == "long" \
                   else price + half_spread
        else:
            fill = price

        if self.side == "long":
            pnl = (fill - self.entry_price) * self.qty
        else:
            pnl = (self.entry_price - fill) * self.qty

        fees = self.qty * fill * self.fee_rate
        pnl -= fees
        pnl_pct = float(pnl / (self.entry_price * self.qty + 1e-10) * 100)

        self.capital += pnl
        self.session_pnl    += pnl
        self.session_trades += 1
        if pnl > 0:
            self.session_wins += 1

        trade = BotTrade(
            bot_id=self.bot_id, symbol=self.symbol, exchange=self.exchange,
            side=self.side, entry_price=self.entry_price, exit_price=fill,
            qty=self.qty, pnl=pnl, pnl_pct=pnl_pct, fees=fees,
            duration_ms=_MS() - self.entry_ts, exit_reason=reason,
            signal_score=self.signal_score, entry_ts=self.entry_ts,
            exit_ts=_MS(),
        )

        self._trade_hist.append(trade)
        self.open = False
        return trade

    @property
    def win_rate(self) -> float:
        return self.session_wins / max(self.session_trades, 1)


# ══════════════════════════════════════════════════════════════════════════════
#  SWARM INTELLIGENCE  (shared learning ring between all bots)
# ══════════════════════════════════════════════════════════════════════════════

class SwarmIntelligence:
    """
    Shared ring buffer — all bots write their last outcome, all bots read
    the aggregate. Implements collective learning:
    
    - Winning bots lower their entry threshold (be more aggressive)
    - Losing bots raise their entry threshold (be more selective)
    - Symbol-level learning: if many bots lose on a symbol type, 
      threshold for that symbol class increases
    - Periodically computes global win rate and broadcasts adaptation signal
    """

    def __init__(self, n_bots: int = TARGET_BOTS):
        self.n_bots = n_bots
        self._ring: deque = deque(maxlen=SHARED_RING_SIZE)
        self._lock  = threading.Lock()

        # Shared threshold adaptation
        self._global_win_rate: float = 0.50
        self._global_threshold: float = 0.25
        self._threshold_by_bot: np.ndarray = np.full(n_bots, 0.25, dtype=np.float32)
        self._wins_by_bot:   np.ndarray = np.zeros(n_bots, dtype=np.int32)
        self._trades_by_bot: np.ndarray = np.zeros(n_bots, dtype=np.int32)

        # Volatility tracking per exchange (to focus on the best ones)
        self._exchange_scores: Dict[str, float] = defaultdict(lambda: 0.5)

    def report_outcome(self, bot_id: int, won: bool,
                       pnl_pct: float, signal_score: float,
                       exchange: str):
        with self._lock:
            if bot_id < self.n_bots:
                self._trades_by_bot[bot_id] += 1
                if won:
                    self._wins_by_bot[bot_id] += 1

                # Adapt this bot's threshold
                t = self._trades_by_bot[bot_id]
                w = self._wins_by_bot[bot_id]
                bot_wr = w / max(t, 1)
                current = float(self._threshold_by_bot[bot_id])
                if bot_wr > 0.60:
                    new_t = max(0.15, current - 0.002)
                elif bot_wr < 0.40:
                    new_t = min(0.50, current + 0.005)
                else:
                    new_t = current
                self._threshold_by_bot[bot_id] = new_t

                # Exchange scoring
                self._exchange_scores[exchange] = (
                    0.95 * self._exchange_scores[exchange] + 0.05 * float(won)
                )

            self._ring.append({
                "bot_id": bot_id, "won": won, "pnl_pct": pnl_pct,
                "score": signal_score, "ex": exchange,
                "ts": _MS(),
            })

    def get_bot_threshold(self, bot_id: int) -> float:
        if bot_id < len(self._threshold_by_bot):
            return float(self._threshold_by_bot[bot_id])
        return 0.25

    def update_global_stats(self):
        """Called every 30s. Computes global win rate and adapts global threshold."""
        with self._lock:
            recent = list(self._ring)[-500:]
        if not recent:
            return
        wins  = sum(1 for r in recent if r["won"])
        total = len(recent)
        self._global_win_rate = wins / total
        # Global threshold: converge toward regime that maintains ~55% WR
        if self._global_win_rate < 0.45:
            self._global_threshold = min(0.45, self._global_threshold + 0.005)
        elif self._global_win_rate > 0.65:
            self._global_threshold = max(0.15, self._global_threshold - 0.002)

    def get_best_exchange(self) -> str:
        with self._lock:
            if not self._exchange_scores:
                return "bybit"
            return max(self._exchange_scores, key=self._exchange_scores.get)

    @property
    def global_win_rate(self) -> float:
        return self._global_win_rate

    @property
    def global_threshold(self) -> float:
        return self._global_threshold


# ══════════════════════════════════════════════════════════════════════════════
#  SCALPBOT  (one bot = one pair, runs forever)
# ══════════════════════════════════════════════════════════════════════════════

class ScalpBot:
    """
    The atomic unit of the swarm. One bot, one pair, autonomous forever.
    
    Loop (every TICK_INTERVAL_MS):
      1. Fetch latest ticker (or receive from data bus)
      2. Update MicroSignalEngine
      3. If in position: check SL/TP/timeout → exit if triggered
      4. If not in position: check signal → enter if strong
      5. Report outcome to SwarmIntelligence
      6. Update BotState
    
    Paper mode: simulates all fills internally (no exchange calls).
    Live mode:  places real orders via CCXT.
    """

    def __init__(
        self,
        bot_id:       int,
        pair_score:   PairScore,
        capital:      float,
        swarm_intel:  SwarmIntelligence,
        db:           SwarmDB,
        paper:        bool = True,
        ex_instance:  Any  = None,
    ):
        self.bot_id   = bot_id
        self.ps       = pair_score
        self.symbol   = pair_score.symbol
        self.exchange = pair_score.exchange
        self.paper    = paper

        self.signal   = MicroSignalEngine(bot_id)
        self.executor = NanoExecutor(
            bot_id, self.symbol, self.exchange, capital,
            fee_rate=FEE_RATE, paper=paper
        )
        self.intel    = swarm_intel
        self.db       = db
        self._ex      = ex_instance   # CCXT exchange object (for live)

        # State
        self.state = BotState(
            bot_id=bot_id, symbol=self.symbol, exchange=self.exchange,
            capital=capital, status=BotStatus.IDLE,
        )

        # Performance tracking
        self._trades_buffer: List[BotTrade] = []
        self._ticks = 0
        self._last_db_flush = time.time()
        self._paused_until  = 0.0
        self._consec_losses = 0
        self._running = True

        # Current tick data
        self._price   = pair_score.price or 1.0
        self._bid     = self._price * 0.9999
        self._ask     = self._price * 1.0001
        self._bid_vol = 0.0
        self._ask_vol = 0.0

    # ── Tick Feed ──────────────────────────────────────────────────────────────

    def feed_tick(self, price: float, bid: float, ask: float,
                  bid_vol: float = 0.0, ask_vol: float = 0.0,
                  trade_side: str = ""):
        """External data feed — called by orchestrator or data engine."""
        self._price   = price
        self._bid     = bid     if bid   > 0 else price * 0.9999
        self._ask     = ask     if ask   > 0 else price * 1.0001
        self._bid_vol = bid_vol
        self._ask_vol = ask_vol

    async def _fetch_tick_live(self):
        """Fetch tick from exchange (live mode)."""
        if self._ex is None or ccxt_mod is None:
            return
        try:
            loop = asyncio.get_event_loop()
            t = await loop.run_in_executor(
                None, lambda: self._ex.fetch_ticker(self.symbol)
            )
            if t and t.get("last"):
                self.feed_tick(
                    float(t.get("last",  self._price)),
                    float(t.get("bid",   self._price * 0.9999)),
                    float(t.get("ask",   self._price * 1.0001)),
                )
        except Exception:
            pass

    # ── Core Bot Loop ─────────────────────────────────────────────────────────

    async def run(self):
        """Main bot event loop."""
        log.debug(f"🤖 Bot {self.bot_id:04d} start — {self.symbol} [{self.exchange}]")

        while self._running:
            try:
                tick_start = _MS()

                # Pause check
                if time.time() < self._paused_until:
                    await asyncio.sleep(TICK_INTERVAL_MS / 1000)
                    continue

                # Fetch live tick if in live mode
                if not self.paper:
                    await self._fetch_tick_live()

                price = self._price

                # Synthetic price for paper mode (Geometric Brownian Motion)
                if self.paper:
                    price = self._simulate_price(price)

                spread_pct = float((self._ask - self._bid) / (price + 1e-10))

                # Update signal engine
                sig_score = self.signal.update(
                    price, self._bid, self._ask,
                    self._bid_vol, self._ask_vol,
                )

                # Adapt threshold from swarm intelligence
                self.signal.entry_threshold = self.intel.get_bot_threshold(self.bot_id)

                # Position management
                if self.executor.open:
                    # Check exit conditions
                    exit_now, reason = self.executor.should_exit(price)

                    # Also exit on momentum reversal
                    if not exit_now:
                        mom = self.signal.get_momentum()
                        if (self.executor.side == "long"  and sig_score < -0.30 and mom < 0) or \
                           (self.executor.side == "short" and sig_score >  0.30 and mom > 0):
                            exit_now, reason = True, "signal_reversal"

                    if exit_now:
                        trade = self.executor.exit(price, reason, spread_pct)
                        if trade:
                            await self._on_trade_closed(trade)

                else:
                    # Entry logic
                    enter, side = self.signal.should_enter(sig_score)
                    if enter:
                        trade = self.executor.enter(side, price, sig_score, spread_pct)
                        if trade:
                            self.state.status = (BotStatus.LONG if side == "long"
                                                 else BotStatus.SHORT)
                            self.state.position_side  = side
                            self.state.position_entry = price
                            self.state.position_ts    = _MS()

                # Update state
                self.state.last_price  = price
                self.state.last_signal = sig_score
                self.state.capital     = self.executor.capital
                self.state.n_trades    = self.executor.session_trades
                self.state.n_wins      = self.executor.session_wins
                self.state.win_rate    = self.executor.win_rate
                self.state.total_pnl   = self.executor.session_pnl
                self.state.ticks_today += 1
                if not self.executor.open:
                    self.state.status = BotStatus.SCANNING

                self._ticks += 1

                # Periodic DB flush (every 5s)
                if time.time() - self._last_db_flush > 5.0:
                    await self._flush_to_db()

                # Tick timing: sleep remainder of interval
                elapsed = _MS() - tick_start
                sleep_ms = max(0, TICK_INTERVAL_MS - elapsed)
                await asyncio.sleep(sleep_ms / 1000)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Bot {self.bot_id} error: {e}")
                await asyncio.sleep(1.0)

        self.state.status = BotStatus.DEAD

    def _simulate_price(self, price: float) -> float:
        """
        Simulate realistic tick price for paper mode.
        Uses GBM with volatility calibrated to pair's historical vol.
        """
        dt = TICK_INTERVAL_MS / (1000 * 86400)   # fraction of day
        sigma = self.ps.vol_1h_pct / 100 * math.sqrt(24)   # daily vol
        drift = 0.0   # no drift at tick level
        shock = np.random.randn() * sigma * math.sqrt(dt)
        # Occasional microstructure jump (bid-ask bounce)
        if random.random() < 0.02:
            shock += random.choice([-1, 1]) * self.ps.spread_pct / 100 * 0.5
        new_price = price * math.exp(drift * dt + shock)
        new_price = max(new_price, price * 0.90)  # floor at -10% from current
        # Update bid/ask
        half = new_price * self.ps.spread_pct / 100 / 2
        self._bid = new_price - half
        self._ask = new_price + half
        # Simulate order book imbalance
        self._bid_vol = abs(np.random.randn()) * 1000
        self._ask_vol = abs(np.random.randn()) * 1000
        return new_price

    async def _on_trade_closed(self, trade: BotTrade):
        """Process a closed trade."""
        won = trade.pnl > 0
        if won:
            self._consec_losses = 0
        else:
            self._consec_losses += 1

        # Pause after 5 consecutive losses
        if self._consec_losses >= 5:
            pause_s = 120  # 2 minute cooldown
            self._paused_until = time.time() + pause_s
            self._consec_losses = 0
            self.state.status = BotStatus.PAUSED
            log.debug(f"Bot {self.bot_id:04d} paused {pause_s}s (5 consec losses)")

        # Report to swarm
        self.intel.report_outcome(
            self.bot_id, won, trade.pnl_pct,
            trade.signal_score, self.exchange
        )

        # Buffer trade for batch DB write
        self._trades_buffer.append(trade)
        self.state.daily_pnl += trade.pnl

    async def _flush_to_db(self):
        """Batch-write buffered data to DB."""
        if self._trades_buffer:
            self.db.batch_save_trades(self._trades_buffer)
            self._trades_buffer.clear()
        self.db.upsert_bot(self.state)
        self._last_db_flush = time.time()

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
#  SWARM ORCHESTRATOR  (manages all 1000 bots)
# ══════════════════════════════════════════════════════════════════════════════

class SwarmOrchestrator:
    """
    Creates, starts, monitors, and restarts all 1000 ScalpBots.
    
    Key responsibilities:
    - Pair assignment (unique pair per bot, no changes)
    - Asyncio task pool with restart-on-crash
    - Shared capital pool management
    - Global circuit breaker (halt all bots on catastrophic loss)
    - Live metrics aggregation every second
    - Periodic swarm intelligence update (every 30s)
    - REST + WebSocket dashboard
    """

    def __init__(
        self,
        n_bots:         int   = TARGET_BOTS,
        capital_per_bot:float = 0.20,    # €0.20 per bot = €200 total for 1000 bots
        paper:          bool  = True,
        api_keys:       Optional[Dict] = None,
        exchanges:      Optional[List[str]] = None,
        api_port:       int   = 9002,
        max_global_drawdown_pct: float = 20.0,
    ):
        self.n_bots         = n_bots
        self.capital_per_bot= capital_per_bot
        self.total_capital  = capital_per_bot * n_bots
        self.paper          = paper
        self.api_keys       = api_keys or {}
        self.exchanges_cfg  = exchanges
        self.api_port       = api_port
        self.max_global_dd  = max_global_drawdown_pct / 100

        self.db       = SwarmDB()
        self.intel    = SwarmIntelligence(n_bots)
        self.registry = PairRegistry(api_keys, exchanges)

        self.bots:      Dict[int, ScalpBot]       = {}
        self.tasks:     Dict[int, asyncio.Task]   = {}
        self._pairs:    List[PairScore]           = []
        self._ex_pool:  Dict[str, Any]            = {}

        self._running = False
        self._start_ts = time.time()

        # Metrics (updated every second)
        self._metrics_ring: deque = deque(maxlen=3600)  # 1h of 1s snapshots
        self._trades_last_min: int = 0
        self._pnl_last_min: float = 0.0
        self._global_halt = False

        # WebSocket broadcast clients
        self._ws_clients: Set[Any] = set()

        log.info(f"""
╔══════════════════════════════════════════════════════════════╗
║  SWARM ORCHESTRATOR — INITIALIZED                            ║
║  Bots:     {n_bots:<10}  Paper: {str(paper):<8}              ║
║  Capital:  €{capital_per_bot:.2f}/bot = €{self.total_capital:.2f} total    ║
╚══════════════════════════════════════════════════════════════╝""")

    # ── Init ──────────────────────────────────────────────────────────────────

    async def _load_exchanges(self):
        """Load CCXT exchange instances for live mode."""
        if ccxt_mod is None or self.paper:
            return
        ex_ids = self.exchanges_cfg or ["bybit", "binance", "okx"]
        for ex_id in ex_ids:
            try:
                cls = getattr(ccxt_mod, ex_id)
                creds = self.api_keys.get(ex_id, {})
                ex = cls({"enableRateLimit": True, "timeout": 10_000, **creds})
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, ex.load_markets)
                self._ex_pool[ex_id] = ex
                log.info(f"  ✅ Exchange loaded: {ex_id}")
            except Exception as e:
                log.warning(f"  ⚠️  {ex_id}: {e}")

    async def _discover_pairs(self):
        """Run pair discovery and scoring."""
        log.info("🔭 Discovering pairs...")
        if not self.paper:
            await self.registry.load_exchanges()
            await self.registry.score_pairs()
        self._pairs = self.registry.assign_pairs(self.n_bots)

    def _create_bot(self, bot_id: int) -> ScalpBot:
        ps = self._pairs[bot_id % len(self._pairs)]
        ex_instance = self._ex_pool.get(ps.exchange) if not self.paper else None
        return ScalpBot(
            bot_id=bot_id, pair_score=ps,
            capital=self.capital_per_bot,
            swarm_intel=self.intel, db=self.db,
            paper=self.paper, ex_instance=ex_instance,
        )

    # ── Start ─────────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        log.info("🚀 Starting Swarm Orchestrator...")

        await self._load_exchanges()
        await self._discover_pairs()

        # Create all bots
        log.info(f"🤖 Spawning {self.n_bots} bots...")
        for i in range(self.n_bots):
            self.bots[i] = self._create_bot(i)

        # Launch all bot tasks
        log.info(f"⚡ Launching {self.n_bots} asyncio tasks...")
        for bot_id, bot in self.bots.items():
            task = asyncio.create_task(bot.run(), name=f"bot_{bot_id:04d}")
            self.tasks[bot_id] = task

        # Background tasks
        asyncio.create_task(self._metrics_loop(),     name="metrics_loop")
        asyncio.create_task(self._watchdog_loop(),    name="watchdog_loop")
        asyncio.create_task(self._intel_update_loop(),name="intel_loop")
        asyncio.create_task(self._daily_reset_loop(), name="daily_reset")

        # FastAPI
        if fastapi_mod and uvicorn_mod:
            asyncio.create_task(self._api_server(), name="swarm_api")

        log.info(f"✅ SWARM ONLINE — {self.n_bots} bots hunting on "
                 f"{len(set(b.symbol for b in self.bots.values()))} unique pairs")

    # ── Background Loops ──────────────────────────────────────────────────────

    async def _metrics_loop(self):
        """Aggregate and log metrics every second."""
        last_trade_count = 0
        last_pnl = 0.0

        while self._running:
            await asyncio.sleep(1.0)

            # Fast aggregate (no DB, read from bot states in memory)
            active = sum(1 for b in self.bots.values()
                        if b.state.status not in (BotStatus.DEAD, BotStatus.PAUSED))
            total_trades = sum(b.executor.session_trades for b in self.bots.values())
            total_pnl    = sum(b.executor.session_pnl    for b in self.bots.values())
            total_wins   = sum(b.executor.session_wins   for b in self.bots.values())
            total_capital= sum(b.executor.capital        for b in self.bots.values())
            in_position  = sum(1 for b in self.bots.values() if b.executor.open)

            trades_delta = total_trades - last_trade_count
            pnl_delta    = total_pnl    - last_pnl
            last_trade_count = total_trades
            last_pnl         = total_pnl

            win_rate = total_wins / max(total_trades, 1)
            pnl_pct  = (total_capital - self.total_capital) / (self.total_capital + 1e-10) * 100

            m = {
                "ts":               int(time.time()),
                "total_bots":       self.n_bots,
                "active_bots":      active,
                "in_position":      in_position,
                "total_trades":     total_trades,
                "total_wins":       total_wins,
                "win_rate":         round(win_rate, 4),
                "total_pnl":        round(total_pnl, 4),
                "pnl_pct":          round(pnl_pct, 3),
                "total_capital":    round(total_capital, 4),
                "trades_last_sec":  trades_delta,
                "pnl_last_sec":     round(pnl_delta, 4),
                "trades_per_day_est": trades_delta * 86400,
                "global_threshold": round(self.intel.global_threshold, 3),
                "global_wr":        round(self.intel.global_win_rate, 3),
                "uptime_s":         int(time.time() - self._start_ts),
            }
            self._metrics_ring.append(m)

            # Broadcast to WS clients
            if self._ws_clients:
                msg = json.dumps(m)
                dead = set()
                for ws in self._ws_clients:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        dead.add(ws)
                self._ws_clients -= dead

            # Global circuit breaker
            total_loss_pct = (self.total_capital - total_capital) / (self.total_capital + 1e-10)
            if total_loss_pct >= self.max_global_dd and not self._global_halt:
                self._global_halt = True
                log.warning(f"🚨 GLOBAL HALT — drawdown {total_loss_pct*100:.1f}%")
                for bot in self.bots.values():
                    bot.stop()

            # Print every 10s
            if int(time.time()) % 10 == 0:
                log.info(
                    f"📊 SWARM │ "
                    f"Active={active:4d} │ "
                    f"InPos={in_position:4d} │ "
                    f"Trades={total_trades:7d} │ "
                    f"WR={win_rate:.1%} │ "
                    f"PnL={pnl_pct:+.3f}% │ "
                    f"Capital=€{total_capital:.2f} │ "
                    f"T/s={trades_delta}"
                )

    async def _watchdog_loop(self):
        """Restart crashed bots every 5 seconds."""
        while self._running:
            await asyncio.sleep(5.0)
            if self._global_halt:
                continue
            for bot_id, task in list(self.tasks.items()):
                if task.done():
                    exc = task.exception()
                    if exc:
                        log.debug(f"Bot {bot_id:04d} crashed: {exc} — restarting")
                    # Recreate bot with preserved capital
                    old_capital = self.bots[bot_id].executor.capital
                    new_bot = self._create_bot(bot_id)
                    new_bot.executor.capital = max(old_capital, self.capital_per_bot * 0.1)
                    new_bot.executor.session_pnl    = self.bots[bot_id].executor.session_pnl
                    new_bot.executor.session_trades = self.bots[bot_id].executor.session_trades
                    new_bot.executor.session_wins   = self.bots[bot_id].executor.session_wins
                    self.bots[bot_id] = new_bot
                    new_task = asyncio.create_task(new_bot.run(), name=f"bot_{bot_id:04d}")
                    self.tasks[bot_id] = new_task

    async def _intel_update_loop(self):
        """Update swarm intelligence every 30 seconds."""
        while self._running:
            await asyncio.sleep(30.0)
            self.intel.update_global_stats()
            # Batch DB flush
            states = [b.state for b in self.bots.values()]
            self.db.flush_bots(states)

    async def _daily_reset_loop(self):
        """Reset daily PnL counters at midnight."""
        while self._running:
            await asyncio.sleep(3600)
            now = datetime.now()
            if now.hour == 0:
                for bot in self.bots.values():
                    bot.state.daily_pnl = 0
                    bot.state.ticks_today = 0
                log.info("🌅 Daily stats reset")

    # ── REST + WebSocket API ──────────────────────────────────────────────────

    async def _api_server(self):
        if not fastapi_mod or not uvicorn_mod:
            return

        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.middleware.cors import CORSMiddleware
        import uvicorn

        app = FastAPI(title="NEXUS SWARM API", version="1.0")
        app.add_middleware(CORSMiddleware, allow_origins=["*"],
                           allow_methods=["*"], allow_headers=["*"])
        orch = self

        @app.get("/")
        def root():
            m = orch._metrics_ring[-1] if orch._metrics_ring else {}
            return {**m, "mode": "paper" if orch.paper else "live",
                    "pairs": len(set(b.symbol for b in orch.bots.values()))}

        @app.get("/metrics/history")
        def metrics_history(minutes: int = 5):
            return list(orch._metrics_ring)[-(minutes*60):]

        @app.get("/bots/top")
        def top_bots(n: int = 50):
            return orch.db.get_top_bots(n)

        @app.get("/bots/{bot_id}")
        def bot_detail(bot_id: int):
            bot = orch.bots.get(bot_id)
            if not bot: return {"error": "not found"}
            return {
                "bot_id":   bot_id,
                "symbol":   bot.symbol,
                "exchange": bot.exchange,
                "status":   bot.state.status.value,
                "capital":  bot.executor.capital,
                "trades":   bot.executor.session_trades,
                "win_rate": bot.executor.win_rate,
                "pnl":      bot.executor.session_pnl,
                "in_pos":   bot.executor.open,
                "pos_side": bot.executor.side if bot.executor.open else "",
                "threshold":bot.signal.entry_threshold,
            }

        @app.get("/bots")
        def all_bots_summary(page: int = 0, size: int = 100):
            start = page * size
            end   = start + size
            bot_list = [
                {
                    "id":  bid,
                    "sym": b.symbol,
                    "ex":  b.exchange,
                    "pnl": round(b.executor.session_pnl, 4),
                    "n":   b.executor.session_trades,
                    "wr":  round(b.executor.win_rate, 3),
                    "st":  b.state.status.value,
                    "cap": round(b.executor.capital, 4),
                }
                for bid, b in list(orch.bots.items())[start:end]
            ]
            return {"page": page, "size": size, "total": orch.n_bots,
                    "bots": bot_list}

        @app.get("/trades/recent")
        def recent_trades(limit: int = 100):
            return orch.db.get_recent_trades(limit)

        @app.get("/swarm/intelligence")
        def swarm_intel():
            return {
                "global_win_rate":  orch.intel.global_win_rate,
                "global_threshold": orch.intel.global_threshold,
                "best_exchange":    orch.intel.get_best_exchange(),
                "exchange_scores":  dict(orch.intel._exchange_scores),
            }

        @app.get("/pairs")
        def pairs_list():
            pair_stats: Dict[str, Dict] = defaultdict(lambda: {
                "bots": 0, "trades": 0, "pnl": 0.0, "wr": 0.0
            })
            for b in orch.bots.values():
                key = f"{b.exchange}:{b.symbol}"
                pair_stats[key]["bots"]   += 1
                pair_stats[key]["trades"] += b.executor.session_trades
                pair_stats[key]["pnl"]    += b.executor.session_pnl
            return dict(pair_stats)

        @app.get("/circuit")
        def circuit():
            return {
                "global_halt":    orch._global_halt,
                "max_dd_pct":     orch.max_global_dd * 100,
                "current_capital":sum(b.executor.capital for b in orch.bots.values()),
                "initial_capital":orch.total_capital,
            }

        @app.post("/halt")
        def halt():
            orch._global_halt = True
            for bot in orch.bots.values():
                bot.stop()
            return {"halted": True}

        @app.post("/resume")
        def resume():
            orch._global_halt = False
            return {"halted": False}

        @app.websocket("/ws")
        async def ws_stream(websocket: WebSocket):
            await websocket.accept()
            orch._ws_clients.add(websocket)
            try:
                while True:
                    await asyncio.sleep(30)
            except WebSocketDisconnect:
                pass
            finally:
                orch._ws_clients.discard(websocket)

        config = uvicorn.Config(app, host="0.0.0.0",
                                port=self.api_port, log_level="warning")
        server = uvicorn.Server(config)
        log.info(f"🌐 Swarm API: http://localhost:{self.api_port}")
        log.info(f"   Metrics WS: ws://localhost:{self.api_port}/ws")
        await server.serve()

    # ── Stop ──────────────────────────────────────────────────────────────────

    async def stop(self):
        self._running = False
        for bot in self.bots.values():
            bot.stop()
        for task in self.tasks.values():
            task.cancel()
        # Final DB flush
        states = [b.state for b in self.bots.values()]
        self.db.flush_bots(states)
        self.db._conn.close()
        log.info("🛑 Swarm stopped — all bots saved")

    async def run_forever(self):
        await self.start()
        try:
            await asyncio.gather(
                *list(self.tasks.values()), return_exceptions=True
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    def print_status(self):
        """Print a summary table to terminal."""
        if not self._metrics_ring:
            return
        m = self._metrics_ring[-1]
        top = self.db.get_top_bots(5)

        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  NEXUS SWARM — LIVE STATUS                                       ║
╠══════════════════════════════════════════════════════════════════╣
║  Active bots  : {m.get('active_bots',0):>6} / {self.n_bots}                           ║
║  In position  : {m.get('in_position',0):>6}                                    ║
║  Total trades : {m.get('total_trades',0):>10}                              ║
║  Win rate     : {m.get('win_rate',0)*100:>6.1f}%                                  ║
║  Total PnL    : €{m.get('total_pnl',0):>+10.4f}  ({m.get('pnl_pct',0):+.3f}%)          ║
║  Capital      : €{m.get('total_capital',0):>10.4f}                              ║
║  Trades/sec   : {m.get('trades_last_sec',0):>6}  est {m.get('trades_per_day_est',0):>8,.0f}/day     ║
║  Global WR    : {m.get('global_wr',0)*100:>6.1f}%  threshold={m.get('global_threshold',0):.3f}     ║
╠══════════════════════════════════════════════════════════════════╣
║  TOP 5 BOTS:                                                     ║""")
        for r in top:
            print(f"║  #{r['bot_id']:04d} {r['symbol']:<20} PnL={r['pnl']:+.4f}  T={r['trades']}  WR={r['wr']:.0%}  ║")
        print("╚══════════════════════════════════════════════════════════════════╝")


# ══════════════════════════════════════════════════════════════════════════════
#  INTEGRATION BRIDGE  (plug into NexusPrimeBrain / NexusDataEngine)
# ══════════════════════════════════════════════════════════════════════════════

class SwarmDataBridge:
    """
    Feeds NexusDataEngine market events to the relevant ScalpBots.
    Routes each ticker event to the bot(s) assigned to that symbol.
    """

    def __init__(self, orchestrator: SwarmOrchestrator):
        self.orch = orchestrator
        # Build symbol → [bot_ids] lookup
        self._sym_to_bots: Dict[str, List[int]] = defaultdict(list)
        for bot_id, bot in orchestrator.bots.items():
            self._sym_to_bots[bot.symbol].append(bot_id)

    async def on_market_event(self, event: Any):
        """Compatible with NexusDataEngine MarketEvent."""
        symbol = event.symbol
        data   = event.data
        price  = float(data.get("last", 0))
        bid    = float(data.get("bid", data.get("best_bid", price * 0.9999)))
        ask    = float(data.get("ask", data.get("best_ask", price * 1.0001)))
        bid_vol= float(data.get("depth_bid", data.get("bid_vol", 0)))
        ask_vol= float(data.get("depth_ask", data.get("ask_vol", 0)))

        if price <= 0:
            return

        # Feed to all bots assigned to this symbol
        for bot_id in self._sym_to_bots.get(symbol, []):
            bot = self.orch.bots.get(bot_id)
            if bot:
                bot.feed_tick(price, bid, ask, bid_vol, ask_vol)

    def register_with_engine(self, data_engine: Any):
        """Register this bridge as an agent on NexusDataEngine."""
        try:
            from nexus_data_engine import DataType
            symbols = list(self._sym_to_bots.keys())
            data_engine.register_agent(
                "swarm_data_bridge",
                self.on_market_event,
                types=[DataType.TICKER, DataType.ORDERBOOK],
                symbols=symbols[:500],   # top 500 symbols
                min_interval_ms=50,       # 20 updates/sec per symbol max
            )
            log.info(f"🔌 SwarmDataBridge connected to NexusDataEngine "
                     f"({len(symbols)} symbols)")
        except ImportError:
            log.warning("nexus_data_engine not found — bots use synthetic prices")


# ══════════════════════════════════════════════════════════════════════════════
#  PERFORMANCE ANALYSER
# ══════════════════════════════════════════════════════════════════════════════

class SwarmAnalyser:
    """
    Deep performance analysis of the swarm.
    Identifies best/worst performing bots, regime preferences,
    optimal thresholds per exchange, and projections.
    """

    def __init__(self, db: SwarmDB):
        self.db = db

    def full_report(self) -> Dict:
        summary = self.db.get_swarm_summary()
        top     = self.db.get_top_bots(20)
        recent  = self.db.get_recent_trades(500)

        if not recent:
            return {"summary": summary, "top_bots": top, "message": "no trades yet"}

        wins    = [t for t in recent if t["pnl"] > 0]
        losses  = [t for t in recent if t["pnl"] <= 0]
        win_rate= len(wins) / max(len(recent), 1)

        avg_win_pct = float(sum(t["pnl_pct"] for t in wins)   / max(len(wins), 1))
        avg_los_pct = float(sum(t["pnl_pct"] for t in losses) / max(len(losses), 1))
        avg_dur_ms  = float(sum(t["dur_ms"]  for t in recent) / max(len(recent), 1))
        avg_dur_s   = avg_dur_ms / 1000

        # Exit reason distribution
        reasons: Dict[str, int] = defaultdict(int)
        for t in recent:
            reasons[t.get("reason", "?")] += 1

        # Estimated daily projection
        trades_per_24h_est = (len(recent) / max(avg_dur_s, 1)) * 86400 / max(len(self.db.get_top_bots(1000)), 1)
        proj_daily_pct = (avg_win_pct * win_rate + avg_los_pct * (1 - win_rate)) * trades_per_24h_est

        return {
            "summary":          summary,
            "top_bots":         top[:10],
            "win_rate":         round(win_rate, 4),
            "avg_win_pct":      round(avg_win_pct, 4),
            "avg_loss_pct":     round(avg_los_pct, 4),
            "avg_hold_s":       round(avg_dur_s, 1),
            "exit_reasons":     dict(reasons),
            "est_trades_per_bot_per_day": round(trades_per_24h_est, 1),
            "est_gross_pnl_pct_per_day":  round(proj_daily_pct, 3),
            "fee_drag_per_trade_pct":      round(FEE_RATE * 2 * 100, 4),
        }

    def print_report(self):
        r = self.full_report()
        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  NEXUS SWARM — PERFORMANCE ANALYSIS                                  ║
╠══════════════════════════════════════════════════════════════════════╣
║  Total bots:          {r['summary'].get('bots', 0):<10}                        ║
║  Total trades:        {r['summary'].get('trades', 0):<10}                        ║
║  Win rate:            {r.get('win_rate', 0)*100:>6.1f}%                             ║
║  Avg win per trade:   {r.get('avg_win_pct', 0):>+8.4f}%                           ║
║  Avg loss per trade:  {r.get('avg_loss_pct', 0):>+8.4f}%                          ║
║  Avg hold time:       {r.get('avg_hold_s', 0):>8.1f}s                           ║
║  Est trades/bot/day:  {r.get('est_trades_per_bot_per_day', 0):>8.1f}                           ║
║  Est gross PnL/day:   {r.get('est_gross_pnl_pct_per_day', 0):>+8.3f}%                          ║
║  Fee drag/trade:      {r.get('fee_drag_per_trade_pct', 0):>8.4f}%                           ║
╠══════════════════════════════════════════════════════════════════════╣
║  EXIT REASONS:                                                       ║""")
        for reason, count in sorted(r.get("exit_reasons", {}).items()):
            print(f"║    {reason:<20} {count:>6}                                    ║")
        print("╚══════════════════════════════════════════════════════════════════════╝")


# ══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def _main():
    import argparse
    parser = argparse.ArgumentParser(description="NEXUS SWARM KILL BOTS v1.0")
    parser.add_argument("--bots",     type=int,   default=TARGET_BOTS,
                        help=f"Number of bots (default {TARGET_BOTS})")
    parser.add_argument("--capital",  type=float, default=0.20,
                        help="Capital per bot in EUR (default 0.20)")
    parser.add_argument("--mode",     choices=["paper","live"], default="paper")
    parser.add_argument("--port",     type=int,   default=9002)
    parser.add_argument("--max-dd",   type=float, default=20.0,
                        help="Max global drawdown %% before halt (default 20)")
    parser.add_argument("--duration", type=int,   default=0,
                        help="Run for N seconds then stop (0=infinite)")
    parser.add_argument("--report",   action="store_true",
                        help="Print performance report and exit")
    args = parser.parse_args()

    if args.report:
        db = SwarmDB()
        analyser = SwarmAnalyser(db)
        analyser.print_report()
        return

    paper = (args.mode == "paper")

    log.info(f"""
╔═══════════════════════════════════════════════════════════╗
║       NEXUS SWARM KILL BOTS — STARTING                    ║
║  Bots:    {args.bots:<10}  Mode: {args.mode:<8}              ║
║  Capital: €{args.capital:.2f}/bot = €{args.capital*args.bots:.2f} total         ║
║  API:     http://localhost:{args.port}                    ║
╚═══════════════════════════════════════════════════════════╝""")

    orch = SwarmOrchestrator(
        n_bots=args.bots,
        capital_per_bot=args.capital,
        paper=paper,
        api_port=args.port,
        max_global_drawdown_pct=args.max_dd,
    )

    # Optionally connect to NexusDataEngine for real ticks
    data_engine = None
    if not paper:
        try:
            from nexus_data_engine import NexusDataEngine
            data_engine = NexusDataEngine(
                enabled_exchanges=["bybit","binance","okx"],
                enable_ws=True, enable_api=False, enable_redis=False,
            )
        except ImportError:
            pass

    await orch.start()

    if data_engine:
        bridge = SwarmDataBridge(orch)
        bridge.register_with_engine(data_engine)
        await data_engine.start(quick=True)

    if args.duration > 0:
        log.info(f"⏱  Running for {args.duration}s...")
        await asyncio.sleep(args.duration)
        await orch.stop()
        SwarmAnalyser(orch.db).print_report()
    else:
        log.info("🔥 SWARM RUNNING — Ctrl+C to stop")
        log.info(f"   Dashboard: http://localhost:{args.port}")
        try:
            await orch.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            await orch.stop()
            if data_engine:
                await data_engine.stop()
            SwarmAnalyser(orch.db).print_report()


if __name__ == "__main__":
    asyncio.run(_main())
