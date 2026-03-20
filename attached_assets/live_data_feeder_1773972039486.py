"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║           NEXUS PRIME  –  LIVE DATA FEEDER  v2.0                               ║
║           Multi-Exchange · Zero API Keys · Public Endpoints Only               ║
║           REST + WebSocket · Real-Time OHLCV / Orderbook / Trades              ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Dane publiczne – NIE wymagają klucza API:
  ✅ OHLCV (świece) – wszystkie timeframes
  ✅ Ticker (last price, 24h vol, bid/ask)
  ✅ Orderbook (depth L2)
  ✅ Recent trades (tape)
  ✅ Funding rates (Binance, Bybit, OKX, BitMEX)
  ✅ Open Interest (Binance, Bybit, OKX)
  ✅ WebSocket streams (real-time ticki)

Klucz API potrzebny TYLKO do:
  ❌ Składania zleceń
  ❌ Sprawdzania salda
  ❌ Historii transakcji konta

Giełdy obsługiwane (publiczne dane):
  Binance Futures, Bybit, OKX, Kraken, Bitfinex,
  Gate.io, MEXC, Huobi/HTX, KuCoin, Deribit,
  BitMEX, Phemex, CoinEx, BingX, Bitget
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import random
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Callable, Tuple, Any, Set
from pathlib import Path

import numpy as np

try:
    import ccxt.async_support as ccxt_async
    import ccxt
except ImportError:
    raise ImportError("pip install ccxt")

# ──────────────────────────────────────────────────────────────────────────────
# LOGGER
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("LiveFeeder")


# ──────────────────────────────────────────────────────────────────────────────
# KONFIGURACJA GIEŁD (WSZYSTKIE PUBLICZNE – ZERO API KEY)
# ──────────────────────────────────────────────────────────────────────────────

# Każda giełda z typem rynku futures/spot i priorytetem (niższy = szybszy)
EXCHANGE_PROFILES = {
    # ── Futures/Perpetuals ────────────────────────────────────────────────────
    "binanceusdm": {
        "label":    "Binance Futures",
        "options":  {"defaultType": "future"},
        "priority": 1,
        "has_funding":   True,
        "has_oi":        True,
        "has_ws":        True,
        "rate_limit_ms": 50,    # ~20 req/s publiczny limit
    },
    "bybit": {
        "label":    "Bybit",
        "options":  {"defaultType": "linear"},
        "priority": 1,
        "has_funding":   True,
        "has_oi":        True,
        "has_ws":        True,
        "rate_limit_ms": 50,
    },
    "okx": {
        "label":    "OKX",
        "options":  {"defaultType": "swap"},
        "priority": 1,
        "has_funding":   True,
        "has_oi":        True,
        "has_ws":        True,
        "rate_limit_ms": 100,
    },
    "bitmex": {
        "label":    "BitMEX",
        "options":  {},
        "priority": 2,
        "has_funding":   True,
        "has_oi":        True,
        "has_ws":        True,
        "rate_limit_ms": 200,
    },
    "phemex": {
        "label":    "Phemex",
        "options":  {"defaultType": "swap"},
        "priority": 2,
        "has_funding":   True,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 200,
    },
    "bitget": {
        "label":    "Bitget",
        "options":  {"defaultType": "swap"},
        "priority": 2,
        "has_funding":   True,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 200,
    },
    "gate": {
        "label":    "Gate.io Futures",
        "options":  {"defaultType": "future"},
        "priority": 3,
        "has_funding":   True,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 300,
    },
    "mexc": {
        "label":    "MEXC Futures",
        "options":  {"defaultType": "swap"},
        "priority": 3,
        "has_funding":   True,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 200,
    },
    # ── Spot (jako backup/cross-reference) ───────────────────────────────────
    "kraken": {
        "label":    "Kraken Spot",
        "options":  {},
        "priority": 2,
        "has_funding":   False,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 500,
    },
    "bitfinex2": {
        "label":    "Bitfinex",
        "options":  {},
        "priority": 3,
        "has_funding":   False,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 1000,
    },
    "kucoin": {
        "label":    "KuCoin",
        "options":  {},
        "priority": 3,
        "has_funding":   False,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 333,
    },
    "huobi": {
        "label":    "HTX (Huobi)",
        "options":  {},
        "priority": 3,
        "has_funding":   False,
        "has_oi":        False,
        "has_ws":        False,
        "rate_limit_ms": 200,
    },
}

# Watchlist – symbole które obserwujemy (format ccxt unified)
DEFAULT_WATCHLIST = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "BNB/USDT:USDT",
    "AVAX/USDT:USDT",
    "LINK/USDT:USDT",
    "ARB/USDT:USDT",
    "OP/USDT:USDT",
    "DOGE/USDT:USDT",
    "SUI/USDT:USDT",
]

# Fallback spot symbole dla giełd bez perpetuals
SPOT_FALLBACK = {
    "BTC/USDT:USDT": "BTC/USDT",
    "ETH/USDT:USDT": "ETH/USDT",
    "SOL/USDT:USDT": "SOL/USDT",
    "BNB/USDT:USDT": "BNB/USDT",
    "AVAX/USDT:USDT": "AVAX/USDT",
    "LINK/USDT:USDT": "LINK/USDT",
    "ARB/USDT:USDT":  "ARB/USDT",
    "OP/USDT:USDT":   "OP/USDT",
    "DOGE/USDT:USDT": "DOGE/USDT",
    "SUI/USDT:USDT":  "SUI/USDT",
}


# ──────────────────────────────────────────────────────────────────────────────
# TYPY DANYCH
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class LiveTick:
    """Jeden tick agregowanych danych z giełd."""
    timestamp:      int           # ms epoch
    exchange:       str
    symbol:         str
    price:          float
    bid:            float
    ask:            float
    spread_pct:     float
    volume_24h:     float
    price_change_pct: float       # 24h
    funding_rate:   float         # 0.0 jeśli spot
    open_interest:  float         # 0.0 jeśli brak
    ob_imbalance:   float         # (bid_vol - ask_vol) / total_vol, -1..+1
    trades_buy_vol: float         # ostatnie N transakcji – strona kupna
    trades_sell_vol: float

    @property
    def spread_usd(self) -> float:
        return self.ask - self.bid

    @property
    def cvd_delta(self) -> float:
        """Cumulative Volume Delta approximation."""
        total = self.trades_buy_vol + self.trades_sell_vol
        return (self.trades_buy_vol - self.trades_sell_vol) / (total + 1e-10)


@dataclass
class AggregatedMarketState:
    """
    Zagregowany stan rynku dla jednego symbolu
    z wszystkich dostępnych giełd.
    """
    symbol:          str
    timestamp:       int
    n_exchanges:     int

    # Cena – mediana z wszystkich giełd (odporność na outliers)
    price_median:    float
    price_min:       float
    price_max:       float
    price_spread_ex: float   # spread między giełdami (arbitraż sygnał)

    # Wolumen łączny 24h ze wszystkich giełd
    total_volume_24h: float

    # Funding – średnia ważona wolumenem
    avg_funding_rate: float
    funding_dispersion: float  # odchylenie std funding między giełdami

    # OI łączne
    total_oi:        float
    oi_delta_1h:     float    # zmiana OI w ostatniej godzinie

    # Order book imbalance – mean z giełd (weighted by volume)
    avg_ob_imbalance: float

    # CVD łączne
    cumulative_cvd:  float

    # Ticki per giełda (do szczegółowej analizy)
    ticks_by_exchange: Dict[str, LiveTick] = field(default_factory=dict)

    def to_rl_state_dict(self) -> Dict[str, float]:
        """
        Konwertuje do formatu zgodnego z RLState z rl_engines.py.
        Używane do podania jako input do silników RL.
        """
        price_norm = (self.price_median - self.price_min) / \
                     (self.price_max - self.price_min + 1e-10)
        return {
            "price_median":       self.price_median,
            "price_spread_ex":    self.price_spread_ex,
            "total_volume_24h":   self.total_volume_24h,
            "avg_funding_rate":   self.avg_funding_rate,
            "funding_dispersion": self.funding_dispersion,
            "total_oi":           self.total_oi,
            "oi_delta_1h":        self.oi_delta_1h,
            "avg_ob_imbalance":   self.avg_ob_imbalance,
            "cumulative_cvd":     self.cumulative_cvd,
            "n_exchanges":        float(self.n_exchanges),
        }


@dataclass
class LiveCandle:
    """Świeca w czasie rzeczywistym."""
    exchange:   str
    symbol:     str
    timeframe:  str
    timestamp:  int
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    is_closed:  bool = False    # False = świeca jeszcze otwarta


# ──────────────────────────────────────────────────────────────────────────────
# SINGLE EXCHANGE CONNECTOR
# ──────────────────────────────────────────────────────────────────────────────

class ExchangeConnector:
    """
    Połączenie z jedną giełdą – tylko publiczne endpointy.
    Zarządza rate limiting, retry logiką i health check.
    """

    def __init__(self, exchange_id: str, profile: dict):
        self.exchange_id = exchange_id
        self.profile     = profile
        self.label       = profile["label"]
        self._rl_ms      = profile["rate_limit_ms"]
        self._last_call  = 0.0
        self._errors     = 0
        self._healthy    = True

        # Inicjalizuj ccxt bez kluczy API
        exchange_class = getattr(ccxt_async, exchange_id, None)
        if exchange_class is None:
            raise ValueError(f"ccxt nie zna giełdy: {exchange_id}")

        self.ex: ccxt_async.Exchange = exchange_class({
            **profile.get("options", {}),
            "enableRateLimit": True,      # ccxt wbudowany rate limiter
            "timeout": 10_000,            # 10s timeout
        })

        # Cache dostępnych symboli
        self._available_symbols: Optional[Set[str]] = None
        self._symbol_map: Dict[str, str] = {}   # unified → exchange format

    async def _rate_limit(self):
        """Własny rate limiter ponad ccxt."""
        elapsed = (time.time() - self._last_call) * 1000
        if elapsed < self._rl_ms:
            await asyncio.sleep((self._rl_ms - elapsed) / 1000)
        self._last_call = time.time()

    async def load_markets(self) -> bool:
        """Pobiera listę dostępnych symboli."""
        try:
            await self._rate_limit()
            markets = await self.ex.load_markets()
            self._available_symbols = set(markets.keys())
            log.info(f"[{self.label}] ✅ {len(self._available_symbols)} symboli")
            self._errors = 0
            return True
        except Exception as e:
            log.warning(f"[{self.label}] load_markets: {e}")
            self._errors += 1
            return False

    def resolve_symbol(self, unified: str) -> Optional[str]:
        """
        Mapuje unified symbol (BTC/USDT:USDT) na format giełdy.
        Fallback do spot jeśli futures niedostępne.
        """
        if self._available_symbols is None:
            return unified

        # Próbuj bezpośrednio
        if unified in self._available_symbols:
            return unified

        # Próbuj spot fallback
        spot = SPOT_FALLBACK.get(unified)
        if spot and spot in self._available_symbols:
            return spot

        # Próbuj BTC/USDT:USDT → BTC/USDT (strip perpetual marker)
        clean = unified.split(":")[0]
        if clean in self._available_symbols:
            return clean

        return None

    async def fetch_ticker(self, symbol: str) -> Optional[dict]:
        sym = self.resolve_symbol(symbol)
        if not sym:
            return None
        try:
            await self._rate_limit()
            t = await self.ex.fetch_ticker(sym)
            self._errors = 0
            return t
        except Exception as e:
            self._errors += 1
            if self._errors > 5:
                self._healthy = False
            return None

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1h", limit: int = 300
    ) -> List[List]:
        sym = self.resolve_symbol(symbol)
        if not sym:
            return []
        try:
            await self._rate_limit()
            data = await self.ex.fetch_ohlcv(sym, timeframe, limit=limit)
            self._errors = 0
            return data
        except Exception as e:
            log.debug(f"[{self.label}] OHLCV {symbol}/{timeframe}: {e}")
            self._errors += 1
            return []

    async def fetch_orderbook(
        self, symbol: str, depth: int = 20
    ) -> Optional[dict]:
        sym = self.resolve_symbol(symbol)
        if not sym:
            return None
        try:
            await self._rate_limit()
            ob = await self.ex.fetch_order_book(sym, depth)
            self._errors = 0
            return ob
        except Exception as e:
            log.debug(f"[{self.label}] Orderbook {symbol}: {e}")
            return None

    async def fetch_trades(
        self, symbol: str, limit: int = 50
    ) -> List[dict]:
        sym = self.resolve_symbol(symbol)
        if not sym:
            return []
        try:
            await self._rate_limit()
            trades = await self.ex.fetch_trades(sym, limit=limit)
            self._errors = 0
            return trades
        except Exception as e:
            log.debug(f"[{self.label}] Trades {symbol}: {e}")
            return []

    async def fetch_funding_rate(self, symbol: str) -> float:
        if not self.profile.get("has_funding"):
            return 0.0
        sym = self.resolve_symbol(symbol)
        if not sym:
            return 0.0
        try:
            await self._rate_limit()
            if hasattr(self.ex, "fetch_funding_rate"):
                fr = await self.ex.fetch_funding_rate(sym)
                return float(fr.get("fundingRate") or 0.0)
        except Exception:
            pass
        return 0.0

    async def fetch_open_interest(self, symbol: str) -> float:
        if not self.profile.get("has_oi"):
            return 0.0
        sym = self.resolve_symbol(symbol)
        if not sym:
            return 0.0
        try:
            await self._rate_limit()
            if hasattr(self.ex, "fetch_open_interest"):
                oi = await self.ex.fetch_open_interest(sym)
                return float(oi.get("openInterestValue") or oi.get("openInterest") or 0.0)
        except Exception:
            pass
        return 0.0

    async def close(self):
        try:
            await self.ex.close()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# MULTI-EXCHANGE DATA AGGREGATOR
# ──────────────────────────────────────────────────────────────────────────────

class MultiExchangeFeeder:
    """
    Pobiera dane live ze WSZYSTKICH dostępnych giełd równolegle.

    Architektura:
      ┌─────────────────────────────────────────────────────────┐
      │  ExchangeConnector × N  (async, rate-limited, retry)    │
      │           ↓                                              │
      │  DataAggregator  (median, weighted avg, cross-ex arb)   │
      │           ↓                                              │
      │  AggregatedMarketState  per symbol                       │
      │           ↓                                              │
      │  Callbacks → TrainingPipeline / SignalEngine / Logger    │
      └─────────────────────────────────────────────────────────┘
    """

    def __init__(
        self,
        watchlist:     List[str] = DEFAULT_WATCHLIST,
        exchanges:     Optional[List[str]] = None,   # None = wszystkie
        poll_interval: float = 15.0,                  # sekund między skanami
        ohlcv_tf:      str   = "1h",
        ohlcv_limit:   int   = 300,
    ):
        self.watchlist     = watchlist
        self.poll_interval = poll_interval
        self.ohlcv_tf      = ohlcv_tf
        self.ohlcv_limit   = ohlcv_limit
        self._running      = False

        # Inicjalizuj connectory
        use_exchanges = exchanges or list(EXCHANGE_PROFILES.keys())
        self.connectors: Dict[str, ExchangeConnector] = {}
        for ex_id in use_exchanges:
            if ex_id in EXCHANGE_PROFILES:
                try:
                    self.connectors[ex_id] = ExchangeConnector(
                        ex_id, EXCHANGE_PROFILES[ex_id]
                    )
                except Exception as e:
                    log.warning(f"[{ex_id}] Init error: {e}")

        # Stan danych
        self._latest_ticks:  Dict[str, Dict[str, LiveTick]] = defaultdict(dict)
        # symbol → exchange → tick
        self._ohlcv_cache:   Dict[str, Dict[str, List]] = defaultdict(dict)
        # symbol → exchange → [[ts,o,h,l,c,v], ...]
        self._oi_history:    Dict[str, deque] = defaultdict(lambda: deque(maxlen=60))
        # symbol → deque[(ts, oi)]
        self._agg_state:     Dict[str, AggregatedMarketState] = {}

        # Callbacki – wywoływane po każdej aktualizacji
        self._on_tick_callbacks: List[Callable] = []
        self._on_ohlcv_callbacks: List[Callable] = []

        log.info(
            f"MultiExchangeFeeder init: "
            f"{len(self.connectors)} giełd, "
            f"{len(watchlist)} symboli"
        )

    def on_tick(self, callback: Callable[[AggregatedMarketState], None]):
        """Rejestruje callback wywoływany po każdym cyklu agregacji."""
        self._on_tick_callbacks.append(callback)

    def on_ohlcv(self, callback: Callable[[str, str, List[List]], None]):
        """Rejestruje callback (exchange, symbol, candles) po pobraniu OHLCV."""
        self._on_ohlcv_callbacks.append(callback)

    # ── Init ──────────────────────────────────────────────────────────────────

    async def _init_all(self):
        """Ładuje markets dla wszystkich giełd równolegle."""
        log.info("📡 Inicjalizacja połączeń z giełdami...")
        tasks = [c.load_markets() for c in self.connectors.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        healthy = sum(1 for r in results if r is True)
        log.info(f"✅ {healthy}/{len(self.connectors)} giełd aktywnych")

    # ── Tick Fetcher ──────────────────────────────────────────────────────────

    async def _fetch_ticker_from_exchange(
        self, connector: ExchangeConnector, symbol: str
    ) -> Optional[LiveTick]:
        """Pobiera pełny tick z jednej giełdy dla jednego symbolu."""
        ticker = await connector.fetch_ticker(symbol)
        if not ticker:
            return None

        # Orderbook imbalance (opcjonalne – szybkie)
        ob_imbalance = 0.0
        ob = await connector.fetch_orderbook(symbol, depth=10)
        if ob:
            bid_vol = sum(v for _, v in ob.get("bids", [])[:5])
            ask_vol = sum(v for _, v in ob.get("asks", [])[:5])
            total   = bid_vol + ask_vol
            if total > 0:
                ob_imbalance = (bid_vol - ask_vol) / total

        # Recent trades CVD
        buy_vol = sell_vol = 0.0
        trades = await connector.fetch_trades(symbol, limit=50)
        for t in trades:
            v = float(t.get("amount") or 0)
            if t.get("side") == "buy":
                buy_vol += v
            elif t.get("side") == "sell":
                sell_vol += v

        # Funding + OI (równolegle)
        funding, oi = await asyncio.gather(
            connector.fetch_funding_rate(symbol),
            connector.fetch_open_interest(symbol),
            return_exceptions=True,
        )
        funding = float(funding) if isinstance(funding, (int, float)) else 0.0
        oi      = float(oi)      if isinstance(oi, (int, float))      else 0.0

        price    = float(ticker.get("last")  or ticker.get("close") or 0)
        bid      = float(ticker.get("bid")   or price)
        ask      = float(ticker.get("ask")   or price)
        vol_24h  = float(ticker.get("quoteVolume") or ticker.get("baseVolume") or 0)
        chg_pct  = float(ticker.get("percentage")  or 0)
        spread   = (ask - bid) / (price + 1e-10) * 100 if price > 0 else 0.0

        if price <= 0:
            return None

        return LiveTick(
            timestamp       = int(time.time() * 1000),
            exchange        = connector.exchange_id,
            symbol          = symbol,
            price           = price,
            bid             = bid,
            ask             = ask,
            spread_pct      = spread,
            volume_24h      = vol_24h,
            price_change_pct = chg_pct,
            funding_rate    = funding,
            open_interest   = oi,
            ob_imbalance    = ob_imbalance,
            trades_buy_vol  = buy_vol,
            trades_sell_vol = sell_vol,
        )

    async def _fetch_all_tickers(self, symbol: str):
        """Pobiera ticker dla jednego symbolu ze wszystkich giełd równolegle."""
        healthy_connectors = [
            c for c in self.connectors.values() if c._healthy
        ]
        tasks = [
            self._fetch_ticker_from_exchange(c, symbol)
            for c in healthy_connectors
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for connector, result in zip(healthy_connectors, results):
            if isinstance(result, LiveTick):
                self._latest_ticks[symbol][connector.exchange_id] = result
                # Zapisz OI historię
                if result.open_interest > 0:
                    self._oi_history[symbol].append(
                        (result.timestamp, result.open_interest)
                    )

    # ── OHLCV Fetcher ─────────────────────────────────────────────────────────

    async def _fetch_ohlcv_from_exchange(
        self,
        connector: ExchangeConnector,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> Tuple[str, str, List]:
        data = await connector.fetch_ohlcv(symbol, timeframe, limit)
        return connector.exchange_id, symbol, data

    async def fetch_ohlcv_all_exchanges(
        self,
        symbol:    str,
        timeframe: str = "1h",
        limit:     int = 300,
    ) -> Dict[str, List[List]]:
        """
        Pobiera OHLCV dla jednego symbolu ze wszystkich giełd równolegle.
        Zwraca {exchange_id: [[ts, o, h, l, c, v], ...]}
        """
        healthy = [c for c in self.connectors.values() if c._healthy]
        tasks   = [
            self._fetch_ohlcv_from_exchange(c, symbol, timeframe, limit)
            for c in healthy
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ohlcv_map = {}
        for result in results:
            if isinstance(result, tuple):
                ex_id, sym, data = result
                if data and len(data) > 10:
                    ohlcv_map[ex_id] = data
                    self._ohlcv_cache[symbol][ex_id] = data
                    for cb in self._on_ohlcv_callbacks:
                        try:
                            await cb(ex_id, symbol, data)
                        except Exception as e:
                            log.debug(f"OHLCV callback error: {e}")

        return ohlcv_map

    async def fetch_ohlcv_best_exchange(
        self,
        symbol:    str,
        timeframe: str = "1h",
        limit:     int = 300,
    ) -> Optional[Tuple[str, List[List]]]:
        """
        Pobiera OHLCV z najlepszej dostępnej giełdy (priorytet 1 → 3).
        Używaj gdy potrzebujesz tylko jednego źródła.
        """
        sorted_connectors = sorted(
            [c for c in self.connectors.values() if c._healthy],
            key=lambda c: EXCHANGE_PROFILES[c.exchange_id]["priority"]
        )
        for connector in sorted_connectors:
            data = await connector.fetch_ohlcv(symbol, timeframe, limit)
            if data and len(data) > 50:
                return connector.exchange_id, data

        return None

    # ── Aggregacja ────────────────────────────────────────────────────────────

    def _aggregate(self, symbol: str) -> Optional[AggregatedMarketState]:
        """
        Agreguje ticki z wszystkich giełd dla jednego symbolu.
        Używa mediany dla ceny (odporność na outliers/pumpy na 1 giełdzie).
        """
        ticks = self._latest_ticks.get(symbol, {})
        if not ticks:
            return None

        # Filtruj stare ticki (>30s)
        now_ms  = int(time.time() * 1000)
        fresh   = {ex: t for ex, t in ticks.items()
                   if now_ms - t.timestamp < 30_000}
        if not fresh:
            return None

        prices   = [t.price      for t in fresh.values()]
        volumes  = [t.volume_24h for t in fresh.values()]
        fundings = [t.funding_rate for t in fresh.values() if t.funding_rate != 0]
        ois      = [t.open_interest for t in fresh.values() if t.open_interest > 0]
        obs      = [t.ob_imbalance  for t in fresh.values()]
        cvds     = [t.cvd_delta      for t in fresh.values()]

        # Cena – mediana (nie średnia – odporność na outliers)
        price_median = float(np.median(prices))
        price_min    = float(min(prices))
        price_max    = float(max(prices))
        # Spread między giełdami jako % mediany
        price_spread_ex = (price_max - price_min) / (price_median + 1e-10) * 100

        # Wolumen łączny
        total_vol = sum(volumes)

        # Funding ważony wolumenem
        if fundings and volumes:
            vol_arr = np.array([t.volume_24h for t in fresh.values()
                                if t.funding_rate != 0])
            fr_arr  = np.array(fundings)
            avg_funding = float(np.average(fr_arr, weights=vol_arr + 1e-10)
                                if len(vol_arr) == len(fr_arr) else np.mean(fr_arr))
            fund_dispersion = float(np.std(fr_arr)) if len(fr_arr) > 1 else 0.0
        else:
            avg_funding = 0.0
            fund_dispersion = 0.0

        # OI total
        total_oi = sum(ois)

        # OI delta z historii
        oi_hist = list(self._oi_history.get(symbol, []))
        oi_delta_1h = 0.0
        if len(oi_hist) >= 2:
            ts_1h_ago = now_ms - 3_600_000
            old_oi = next((v for ts, v in reversed(oi_hist) if ts <= ts_1h_ago), None)
            if old_oi and old_oi > 0 and total_oi > 0:
                oi_delta_1h = (total_oi - old_oi) / old_oi * 100

        # OB imbalance ważony wolumenem
        if volumes and obs:
            avg_ob = float(np.average(obs, weights=np.array(volumes) + 1e-10))
        else:
            avg_ob = float(np.mean(obs)) if obs else 0.0

        # CVD agregat
        cum_cvd = float(np.mean(cvds)) if cvds else 0.0

        return AggregatedMarketState(
            symbol            = symbol,
            timestamp         = now_ms,
            n_exchanges       = len(fresh),
            price_median      = price_median,
            price_min         = price_min,
            price_max         = price_max,
            price_spread_ex   = price_spread_ex,
            total_volume_24h  = total_vol,
            avg_funding_rate  = avg_funding,
            funding_dispersion = fund_dispersion,
            total_oi          = total_oi,
            oi_delta_1h       = oi_delta_1h,
            avg_ob_imbalance  = avg_ob,
            cumulative_cvd    = cum_cvd,
            ticks_by_exchange = fresh,
        )

    # ── Główna pętla ──────────────────────────────────────────────────────────

    async def _poll_cycle(self):
        """Jeden cykl pobierania danych dla wszystkich symboli."""
        # Pobierz ticki dla wszystkich symboli równolegle
        ticker_tasks = [
            self._fetch_all_tickers(symbol)
            for symbol in self.watchlist
        ]
        await asyncio.gather(*ticker_tasks, return_exceptions=True)

        # Agreguj i wywołaj callbacki
        for symbol in self.watchlist:
            state = self._aggregate(symbol)
            if state:
                self._agg_state[symbol] = state
                for cb in self._on_tick_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(cb):
                            await cb(state)
                        else:
                            cb(state)
                    except Exception as e:
                        log.debug(f"Tick callback error: {e}")

    async def _ohlcv_refresh_loop(self, interval_sec: int = 300):
        """
        Co N sekund odświeża OHLCV dla wszystkich symboli ze wszystkich giełd.
        Domyślnie co 5 minut dla 1h świec.
        """
        while self._running:
            for symbol in self.watchlist:
                await self.fetch_ohlcv_all_exchanges(
                    symbol, self.ohlcv_tf, self.ohlcv_limit
                )
                await asyncio.sleep(0.5)   # throttle między symbolami
            await asyncio.sleep(interval_sec)

    async def _health_monitor(self):
        """Co minutę sprawdza stan giełd i próbuje wznowić martwe."""
        while self._running:
            await asyncio.sleep(60)
            dead = [c for c in self.connectors.values()
                    if not c._healthy and c._errors > 0]
            for connector in dead:
                log.info(f"[{connector.label}] 🔄 Próba wznowienia...")
                ok = await connector.load_markets()
                if ok:
                    connector._healthy = True
                    connector._errors  = 0
                    log.info(f"[{connector.label}] ✅ Wznowiony")

    async def run(self):
        """Start feedera."""
        log.info("🚀 MultiExchangeFeeder START")
        await self._init_all()

        # Pierwsze pobranie OHLCV
        log.info("📊 Pierwsze pobranie OHLCV...")
        for symbol in self.watchlist:
            await self.fetch_ohlcv_all_exchanges(
                symbol, self.ohlcv_tf, self.ohlcv_limit
            )
            await asyncio.sleep(0.2)

        self._running = True
        log.info(
            f"✅ Feeder aktywny – poll co {self.poll_interval}s, "
            f"OHLCV refresh co 5min"
        )

        await asyncio.gather(
            self._poll_main(),
            self._ohlcv_refresh_loop(300),
            self._health_monitor(),
        )

    async def _poll_main(self):
        while self._running:
            t0 = time.time()
            await self._poll_cycle()
            elapsed = time.time() - t0
            sleep_t = max(0, self.poll_interval - elapsed)
            log.debug(f"Poll cycle: {elapsed:.1f}s → sleep {sleep_t:.1f}s")
            await asyncio.sleep(sleep_t)

    async def stop(self):
        self._running = False
        for c in self.connectors.values():
            await c.close()
        log.info("🛑 Feeder zatrzymany")

    # ── Pomocnicze gettery ────────────────────────────────────────────────────

    def get_state(self, symbol: str) -> Optional[AggregatedMarketState]:
        """Zwraca ostatni zagregowany stan dla symbolu."""
        return self._agg_state.get(symbol)

    def get_all_states(self) -> Dict[str, AggregatedMarketState]:
        """Zwraca ostatnie stany dla wszystkich symboli."""
        return dict(self._agg_state)

    def get_ohlcv(
        self, symbol: str, exchange_id: Optional[str] = None
    ) -> Optional[List[List]]:
        """
        Zwraca OHLCV z cache.
        Jeśli exchange_id=None zwraca z giełdy o najwyższym priorytecie.
        """
        cache = self._ohlcv_cache.get(symbol, {})
        if not cache:
            return None
        if exchange_id:
            return cache.get(exchange_id)

        # Zwróć z giełdy priorytet 1
        for ex_id in sorted(cache, key=lambda x: EXCHANGE_PROFILES.get(x, {}).get("priority", 99)):
            if cache[ex_id]:
                return cache[ex_id]
        return None

    def get_best_price(self, symbol: str) -> Tuple[float, str]:
        """
        Zwraca (najlepsza_cena, giełda).
        Best price = mediana z wszystkich giełd.
        """
        state = self._agg_state.get(symbol)
        if state:
            return state.price_median, f"{state.n_exchanges} exchanges"
        return 0.0, "no_data"

    def print_market_overview(self):
        """Wypisuje przegląd rynku."""
        print("\n" + "═" * 80)
        print(f"  LIVE MARKET OVERVIEW  │  {time.strftime('%H:%M:%S')}  │  "
              f"{len([c for c in self.connectors.values() if c._healthy])} giełd aktywnych")
        print("═" * 80)
        print(f"  {'Symbol':<22} {'Price':>12} {'Ex#':>4} {'Funding':>10} "
              f"{'OI':>14} {'OB Imb':>8} {'Ex.Spread':>10}")
        print("─" * 80)
        for sym in self.watchlist:
            s = self._agg_state.get(sym)
            if s:
                funding_str = f"{s.avg_funding_rate*100:+.4f}%"
                oi_str = f"${s.total_oi/1e9:.2f}B" if s.total_oi > 1e9 else \
                         f"${s.total_oi/1e6:.1f}M" if s.total_oi > 0 else "N/A"
                spread_str  = f"{s.price_spread_ex:.3f}%"
                ob_str = f"{s.avg_ob_imbalance:+.3f}"
                print(
                    f"  {sym:<22} ${s.price_median:>11,.2f} "
                    f"{s.n_exchanges:>4} "
                    f"{funding_str:>10} "
                    f"{oi_str:>14} "
                    f"{ob_str:>8} "
                    f"{spread_str:>10}"
                )
        print("═" * 80 + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# ADAPTER DLA ULTIMATE TRAINING – zastępuje MarketSimulator
# ──────────────────────────────────────────────────────────────────────────────

class LiveDataTrainingAdapter:
    """
    Adapter który podłącza LiveFeeder pod ultimate_training.py.

    Zamienia live dane na RLState kompatybilny z rl_engines.py/v2.py
    i karmi nimi silniki RL w czasie rzeczywistym.

    Użycie:
        adapter = LiveDataTrainingAdapter(feeder)
        await adapter.start_live_training(orchestrators)
    """

    def __init__(
        self,
        feeder:        MultiExchangeFeeder,
        history_len:   int   = 200,   # ile ticków historii dla wskaźników
        min_exchanges: int   = 2,     # minimalna liczba giełd do akceptacji danych
    ):
        self.feeder        = feeder
        self.history_len   = history_len
        self.min_exchanges = min_exchanges

        # Historia cen per symbol dla obliczania wskaźników
        self._price_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=history_len)
        )
        self._vol_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=history_len)
        )
        self._funding_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=50)
        )

    def _build_rl_state(self, state: AggregatedMarketState) -> Optional[dict]:
        """
        Buduje słownik RLState z agregowanego stanu rynku.
        Kompatybilny z polem RLState z rl_engines.py.
        """
        sym = state.symbol

        # Dodaj do historii
        self._price_history[sym].append(state.price_median)
        self._funding_history[sym].append(state.avg_funding_rate)

        prices   = np.array(self._price_history[sym])
        fundings = np.array(self._funding_history[sym])

        if len(prices) < 20:
            return None   # Za mało historii

        # ── Oblicz wskaźniki z historii cen ─────────────────────────────────

        # Zmiany ceny
        ret_1  = (prices[-1] - prices[-2])  / (prices[-2]  + 1e-10) if len(prices) >= 2  else 0
        ret_5  = (prices[-1] - prices[-6])  / (prices[-6]  + 1e-10) if len(prices) >= 6  else 0
        ret_15 = (prices[-1] - prices[-16]) / (prices[-16] + 1e-10) if len(prices) >= 16 else 0

        # RSI (uproszczony)
        if len(prices) >= 14:
            deltas = np.diff(prices[-15:])
            gains  = deltas[deltas > 0].mean() if (deltas > 0).any() else 0
            losses = -deltas[deltas < 0].mean() if (deltas < 0).any() else 1e-10
            rsi    = gains / (gains + losses)
        else:
            rsi = 0.5

        # Volatility
        if len(prices) >= 10:
            vol_1m = float(np.std(np.diff(prices[-11:]) / (prices[-11:-1] + 1e-10)))
        else:
            vol_1m = 0.001

        # EMA cross (9 vs 21)
        if len(prices) >= 21:
            alpha9  = 2 / (9  + 1)
            alpha21 = 2 / (21 + 1)
            ema9  = prices[-9:].mean()
            ema21 = prices[-21:].mean()
            ema_cross = (ema9 - ema21) / (prices[-1] + 1e-10)
        else:
            ema_cross = 0.0

        # Bollinger position (0=lower band, 0.5=mid, 1=upper)
        if len(prices) >= 20:
            bb_mid = prices[-20:].mean()
            bb_std = prices[-20:].std()
            bb_pos = (prices[-1] - (bb_mid - 2*bb_std)) / (4*bb_std + 1e-10)
            bb_pos = float(np.clip(bb_pos, 0, 1))
        else:
            bb_pos = 0.5

        # Funding trend
        avg_funding = float(fundings.mean()) if len(fundings) > 0 else 0.0

        # Regime detection z ceny i zmienności
        vol_ma = float(np.std(np.diff(prices) / (prices[:-1] + 1e-10)) * np.sqrt(252))
        regime_volatile = 1.0 if vol_1m > 0.003 else 0.0
        regime_bull     = 1.0 if ret_5 > 0.003 else 0.0
        regime_bear     = 1.0 if ret_5 < -0.003 else 0.0

        return {
            # Pola zgodne z RLState z rl_engines.py
            "price_change_1m":  float(ret_1),
            "price_change_5m":  float(ret_5),
            "rsi_14":           float(rsi),
            "rsi_5":            float(np.clip(rsi * 1.1, 0, 1)),
            "bb_position":      float(bb_pos),
            "ob_imbalance":     float(state.avg_ob_imbalance),
            "momentum_score":   float(ret_15),
            "trade_flow":       float(state.cumulative_cvd),
            "ema_cross":        float(ema_cross * 100),
            "volatility_1m":    float(vol_1m),
            "regime_bull":      regime_bull,
            "regime_bear":      regime_bear,
            "regime_volatile":  regime_volatile,
            # Dodatkowe pola live
            "funding_rate":     float(avg_funding),
            "oi_delta_1h":      float(state.oi_delta_1h),
            "ex_price_spread":  float(state.price_spread_ex),
            "funding_disp":     float(state.funding_dispersion),
            "n_exchanges":      float(state.n_exchanges),
        }

    async def start_live_training(
        self,
        orchestrators: Dict,   # {symbol: RLOrchestrator}
        max_ticks:     Optional[int] = None,
        log_interval:  int = 100,
    ):
        """
        Karmi silniki RL live danymi rynkowymi.
        Działa jako zamiennik GauntletTrainer.run_level() ale na danych live.
        """
        log.info("🔴 LIVE TRAINING START – dane rzeczywiste ze wszystkich giełd")
        tick_count   = 0
        reward_cache = defaultdict(float)   # poprzednia cena dla nagrody

        def on_market_update(state: AggregatedMarketState):
            nonlocal tick_count

            if state.n_exchanges < self.min_exchanges:
                return   # Za mało giełd – dane niewiarygodne

            sym = state.symbol
            rl_state_dict = self._build_rl_state(state)
            if not rl_state_dict:
                return

            orch = orchestrators.get(sym)
            if not orch:
                return

            # Buduj RLState (import z rl_engines)
            try:
                from rl_engines import RLState
                rl_state = RLState(**{
                    k: v for k, v in rl_state_dict.items()
                    if k in RLState.__dataclass_fields__
                })
            except Exception:
                return

            # Decyzja silnika
            decision = orch.decide(rl_state)

            # Nagroda = faktyczna zmiana ceny od ostatniego ticka
            prev_price = reward_cache.get(sym, state.price_median)
            price_ret  = (state.price_median - prev_price) / (prev_price + 1e-10)
            reward_cache[sym] = state.price_median

            # Nagroda w zależności od podjętej decyzji
            direction = decision.get("direction", "hold")
            if direction == "buy":
                reward = price_ret * 100
            elif direction == "sell":
                reward = -price_ret * 100
            else:
                reward = 0.0

            orch.record_outcome(reward, price_ret * 0.01, rl_state)
            tick_count += 1

            if tick_count % log_interval == 0:
                log.info(
                    f"[LIVE TRAIN] Tick #{tick_count:,} │ "
                    f"{sym} @ ${state.price_median:,.2f} │ "
                    f"ex={state.n_exchanges} │ "
                    f"decision={direction}"
                )

            if max_ticks and tick_count >= max_ticks:
                log.info(f"✅ Live training limit osiągnięty: {tick_count} ticków")
                return

        # Zarejestruj callback
        self.feeder.on_tick(on_market_update)
        log.info(f"📡 Live training aktywny dla {len(orchestrators)} symboli")


# ──────────────────────────────────────────────────────────────────────────────
# ARBITRAGE DETECTOR (bonus – analiza spreadu między giełdami)
# ──────────────────────────────────────────────────────────────────────────────

class CrossExchangeArbitrageDetector:
    """
    Wykrywa sytuacje gdy ta sama para ma istotnie różną cenę na różnych giełdach.
    Sygnał informacyjny – nie do wykonania (latency arbitrage wymaga co-location).
    Przydatny jako dodatkowy feature dla RL.
    """

    def __init__(self, threshold_pct: float = 0.15):
        self.threshold = threshold_pct   # % różnicy ceny
        self.arb_log: deque = deque(maxlen=1000)

    def check(self, state: AggregatedMarketState) -> Optional[dict]:
        if state.n_exchanges < 2:
            return None
        if state.price_spread_ex < self.threshold:
            return None

        # Znajdź giełdy z najwyższą i najniższą ceną
        ticks  = state.ticks_by_exchange
        hi_ex  = max(ticks, key=lambda e: ticks[e].price)
        lo_ex  = min(ticks, key=lambda e: ticks[e].price)
        hi_p   = ticks[hi_ex].price
        lo_p   = ticks[lo_ex].price
        spread = (hi_p - lo_p) / lo_p * 100

        event = {
            "ts":         state.timestamp,
            "symbol":     state.symbol,
            "spread_pct": round(spread, 4),
            "high_ex":    hi_ex,
            "high_price": hi_p,
            "low_ex":     lo_ex,
            "low_price":  lo_p,
        }
        self.arb_log.append(event)
        return event


# ──────────────────────────────────────────────────────────────────────────────
# DEMO / STANDALONE
# ──────────────────────────────────────────────────────────────────────────────

async def demo_live_feed(
    exchanges: Optional[List[str]] = None,
    symbols:   Optional[List[str]] = None,
    duration:  int = 120,   # sekund
):
    """
    Demo – pobiera live dane z giełd i wyświetla przegląd rynku.
    Nie wymaga żadnego API key.
    
    Użycie:
        python live_data_feeder.py
        python live_data_feeder.py --fast      # tylko Binance + Bybit
        python live_data_feeder.py --symbols BTC/USDT:USDT ETH/USDT:USDT
    """
    watch = symbols or DEFAULT_WATCHLIST[:5]  # Demo: pierwsze 5

    feeder = MultiExchangeFeeder(
        watchlist     = watch,
        exchanges     = exchanges,
        poll_interval = 15.0,
    )

    arb_detector = CrossExchangeArbitrageDetector(threshold_pct=0.10)

    def on_update(state: AggregatedMarketState):
        arb = arb_detector.check(state)
        if arb:
            log.warning(
                f"⚡ ARB SIGNAL │ {state.symbol} │ "
                f"{arb['low_ex']} ${arb['low_price']:,.2f} → "
                f"{arb['high_ex']} ${arb['high_price']:,.2f} │ "
                f"spread={arb['spread_pct']:.3f}%"
            )

    feeder.on_tick(on_update)

    # Stop po N sekundach
    async def auto_stop():
        await asyncio.sleep(duration)
        log.info(f"Demo zakończone po {duration}s")
        feeder.print_market_overview()
        await feeder.stop()

    await asyncio.gather(
        feeder.run(),
        auto_stop(),
    )


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Live Multi-Exchange Data Feeder – zero API keys"
    )
    parser.add_argument(
        "--exchanges", nargs="+",
        help="Giełdy (domyślnie wszystkie). Np: --exchanges binanceusdm bybit okx"
    )
    parser.add_argument(
        "--symbols", nargs="+",
        help="Symbole (domyślnie top 5). Np: --symbols BTC/USDT:USDT ETH/USDT:USDT"
    )
    parser.add_argument(
        "--duration", type=int, default=300,
        help="Czas demo w sekundach (domyślnie 300)"
    )
    parser.add_argument(
        "--fast", action="store_true",
        help="Tylko Binance + Bybit (szybszy start)"
    )
    args = parser.parse_args()

    fast_exchanges = ["binanceusdm", "bybit"] if args.fast else args.exchanges

    asyncio.run(demo_live_feed(
        exchanges = fast_exchanges,
        symbols   = args.symbols,
        duration  = args.duration,
    ))
