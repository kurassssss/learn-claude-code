"""
QUANTUM TRADER — Exchange Strategy Optimizer
=============================================
Każda giełda dostaje WŁASNE zoptymalizowane parametry strategii.

Binance != Bybit != OKX != KuCoin

Różnice między giełdami:
  • Opłaty: Binance 0.04%, Bybit 0.06%, OKX 0.05%
  • Płynność: Binance 10× większa niż Bybit
  • Funding: co 8h vs co 4h vs co 1h
  • Orderbook: Binance głębszy, mniej spoofingu
  • Latencja: różna per giełda per region
  • Anomalie: każda giełda ma unikalne wzorce cenowe
  • Limity: różne min/max pozycji
  • Dźwignia: różne dostępne poziomy

System uczy się tego wszystkiego i dostosowuje:
  • TP/SL per giełda per para
  • Rozmiar pozycji per giełda (uwzględnia płynność)
  • Timing wejść per giełda (uwzględnia anomalie)
  • Strategię (scalp/swing/funding arb) per giełda
  • Confidence threshold per giełda
"""

import numpy as np
import logging
import time
import math
import json
import os
import threading
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger("quantum.strategy_optimizer")


# ─── EXCHANGE-SPECIFIC PARAMETER SET ─────────────────────────────────────────

@dataclass
class ExchangeStrategyParams:
    """
    Pełny zestaw parametrów strategii zoptymalizowany dla konkretnej giełdy.
    Każda giełda + para ma swój własny zestaw.
    """
    exchange: str
    symbol:   str

    # Entry
    min_confidence:   float = 0.65
    signal_blend:     Dict[str, float] = field(default_factory=lambda: {
        "technical": 0.30, "cyclic": 0.20, "orderbook": 0.25,
        "lstm": 0.10, "regime": 0.10, "micro": 0.05,
    })

    # Exit
    take_profit_pct:  float = 1.00   # % zysk do zamknięcia
    stop_loss_pct:    float = 0.40
    trailing_trigger: float = 0.60   # aktywuj trailing po X%
    trailing_step:    float = 0.25
    max_hold_min:     float = 120.0

    # Sizing
    base_size_usd:    float = 100.0
    max_size_usd:     float = 500.0
    use_maker:        bool  = False   # limit orders = maker fee

    # Timing
    preferred_hours:  List[int] = field(default_factory=lambda: list(range(24)))
    avoid_hours:      List[int] = field(default_factory=list)

    # Exchange quirks
    min_order_usd:    float = 5.0
    leverage:         int   = 3
    fee_maker:        float = 0.0002
    fee_taker:        float = 0.0006

    # Strategy type
    primary_strategy: str = "SCALP"   # SCALP / SWING / FUNDING_ARB / MEAN_REV

    # Auto-tuning state
    n_tuning_iters:   int   = 0
    last_tuned:       float = 0.0
    fitness:          float = 0.0

    def effective_tp(self) -> float:
        """TP po opłatach round-trip."""
        rt_fee = (self.fee_taker if not self.use_maker else self.fee_maker) * 2
        return self.take_profit_pct + rt_fee * 100

    def net_profit_per_trade(self, win_rate: float) -> float:
        """Oczekiwany zysk netto na transakcję."""
        rt_fee = (self.fee_taker if not self.use_maker else self.fee_maker) * 2 * 100
        avg_win  = self.take_profit_pct - rt_fee
        avg_loss = -(self.stop_loss_pct + rt_fee)
        return win_rate * avg_win + (1 - win_rate) * avg_loss

    def to_dict(self) -> Dict:
        d = {k: v for k, v in self.__dict__.items()}
        return d


# ─── ONLINE PARAMETER TUNER ───────────────────────────────────────────────────

class OnlineTuner:
    """
    Dostosowuje parametry strategii online na podstawie wyników.
    Używa Gaussian Process Bandit (UCB) do eksploracji przestrzeni parametrów.

    Per każda para na każdej giełdzie:
      - Śledzi historię wyników z różnymi parametrami
      - Proponuje nowe parametry do przetestowania
      - Konwerguje do optymalnych wartości
    """

    def __init__(self, symbol: str, exchange: str):
        self.symbol   = symbol
        self.exchange = exchange

        # Historia (parametry → wynik)
        self.history: List[Tuple[Dict, float]] = []   # [(params, reward)]
        self.n_exploit = 0
        self.n_explore = 0

        # Current best
        self.best_params: Optional[Dict] = None
        self.best_reward: float = -np.inf

        self._lock = threading.Lock()

    def _param_space(self) -> Dict[str, Tuple]:
        """Przestrzeń parametrów do eksploracji."""
        return {
            "take_profit_pct":  (0.5,  3.0),
            "stop_loss_pct":    (0.2,  1.5),
            "min_confidence":   (0.55, 0.85),
            "trailing_trigger": (0.3,  1.0),
            "trailing_step":    (0.15, 0.5),
            "max_hold_min":     (30,   240),
        }

    def suggest(self, base_params: ExchangeStrategyParams) -> Dict:
        """
        Zaproponuj nowy zestaw parametrów do przetestowania.
        UCB1: balance exploration vs exploitation.
        """
        space = self._param_space()
        total = len(self.history)

        # Pierwsze 10 iteracji: random exploration
        if total < 10:
            self.n_explore += 1
            return {
                k: float(np.random.uniform(lo, hi))
                for k, (lo, hi) in space.items()
            }

        # UCB: wybierz parametry z najlepszym UCB score
        # Uproszenie: perturbacja wokół najlepszych znanych parametrów
        if self.best_params and np.random.random() < 0.7:
            # Exploit: perturbuj najlepsze
            self.n_exploit += 1
            result = dict(self.best_params)
            param_to_perturb = np.random.choice(list(space.keys()))
            lo, hi = space[param_to_perturb]
            current = result.get(param_to_perturb, (lo + hi) / 2)
            noise = (hi - lo) * 0.1 * np.random.randn()
            result[param_to_perturb] = float(np.clip(current + noise, lo, hi))
            return result
        else:
            # Explore: random
            self.n_explore += 1
            return {k: float(np.random.uniform(lo, hi)) for k, (lo, hi) in space.items()}

    def record(self, params: Dict, reward: float) -> None:
        """Zapisz wynik testu parametrów."""
        with self._lock:
            self.history.append((params, reward))
            if reward > self.best_reward:
                self.best_reward  = reward
                self.best_params  = dict(params)

    def get_best(self) -> Optional[Dict]:
        return self.best_params

    def convergence_score(self) -> float:
        """0 = brak konwergencji, 1 = w pełni skonwergowany."""
        if len(self.history) < 5:
            return 0.0
        recent_rewards = [r for _, r in self.history[-10:]]
        if not recent_rewards:
            return 0.0
        std = np.std(recent_rewards)
        return float(np.clip(1 - std / 0.5, 0, 1))


# ─── FUNDING RATE ARBITRAGE STRATEGY ─────────────────────────────────────────

class FundingArbStrategy:
    """
    Strategia arbitrażu funding rate.

    Gdy funding rate jest ekstremalny:
    - Funding > 0.05%: longi przepłacają → SHORT perpetual + LONG spot hedge
    - Funding < -0.02%: shorty przepłacają → LONG perpetual + SHORT futures

    Na niektórych giełdach (Bybit, OKX) funding co 8h = 3x dziennie.
    Przy funding 0.08% × 3 = 0.24%/dzień netto po opłatach ≈ +87%/rok

    Wykrywamy anomalie funding i exploitujemy je przed resetem.
    """

    def __init__(self, exchange: str):
        self.exchange   = exchange
        self.funding_history: Dict[str, deque] = {}
        self.active_arb: Dict[str, Dict] = {}

    def update_funding(self, symbol: str, rate: float, next_reset_ts: float = 0) -> Optional[Dict]:
        """
        Aktualizuj funding rate i sprawdź czy jest okazja do arb.
        """
        if symbol not in self.funding_history:
            self.funding_history[symbol] = deque(maxlen=200)
        self.funding_history[symbol].append({"rate": rate, "ts": time.time()})

        # Analiza
        rates = [h["rate"] for h in self.funding_history[symbol]]
        avg   = np.mean(rates[-20:]) if len(rates) >= 5 else rate
        std   = np.std(rates[-20:])  if len(rates) >= 5 else 0.001

        # Extreme funding = okazja
        is_extreme = abs(rate) > 0.04  # > 0.04% per period

        if not is_extreme:
            return None

        direction = "SHORT" if rate > 0 else "LONG"  # przeciwnie do tych co płacą

        # Ile zbierzemy w następnym resetcie
        time_to_reset = next_reset_ts - time.time() if next_reset_ts > time.time() else 28800  # 8h
        collection_annualized = abs(rate) * (86400 / time_to_reset) * 365 * 100

        return {
            "symbol":         symbol,
            "direction":      direction,
            "funding_rate":   rate,
            "annualized_pct": round(collection_annualized, 2),
            "time_to_reset_min": round(time_to_reset / 60, 1),
            "strategy":       "FUNDING_ARB",
            "confidence":     min(0.95, 0.6 + abs(rate) * 5),  # pewność rośnie z magnitude
        }

    def get_best_arb_opportunities(self) -> List[Dict]:
        """Zwróć najlepsze aktualne okazje funding arb."""
        opps = []
        for sym in self.funding_history:
            hist = list(self.funding_history[sym])
            if not hist:
                continue
            latest = hist[-1]["rate"]
            if abs(latest) > 0.03:
                opp = self.update_funding(sym, latest)
                if opp:
                    opps.append(opp)
        return sorted(opps, key=lambda x: -abs(x["funding_rate"]))


# ─── LATENCY ARBITRAGE DETECTOR ──────────────────────────────────────────────

class LatencyArbDetector:
    """
    Wykrywa okazje do arbitrażu cenowego między giełdami.

    Gdy cena na Binance zmienia się, Bybit często lag'uje o 100-500ms.
    W tym oknie można wejść na Bybit zanim cena się wyrówna.

    Strategia:
    1. Śledź ceny na wszystkich giełdach w czasie rzeczywistym
    2. Gdy divergencja > threshold → sygnał na giełdzie z opóźnieniem
    3. Zamknij gdy ceny się wyrównają (zwykle <30s)
    """

    def __init__(self):
        self.prices: Dict[str, Dict[str, deque]] = {}  # symbol → exchange → prices
        self.arb_events: deque = deque(maxlen=1000)
        self.min_divergence_pct = 0.05  # min 0.05% różnica cen
        self._lock = threading.Lock()

    def update(self, symbol: str, exchange: str, price: float) -> Optional[Dict]:
        """Zaktualizuj cenę i sprawdź czy jest arb."""
        with self._lock:
            if symbol not in self.prices:
                self.prices[symbol] = {}
            if exchange not in self.prices[symbol]:
                self.prices[symbol][exchange] = deque(maxlen=50)
            self.prices[symbol][exchange].append({"price": price, "ts": time.time()})

            exchanges = list(self.prices[symbol].keys())
            if len(exchanges) < 2:
                return None

            # Znajdź max/min cen na różnych giełdach
            current_prices = {}
            for ex in exchanges:
                hist = self.prices[symbol][ex]
                if hist and time.time() - hist[-1]["ts"] < 5:  # świeże dane (< 5s)
                    current_prices[ex] = hist[-1]["price"]

            if len(current_prices) < 2:
                return None

            max_ex    = max(current_prices, key=current_prices.get)
            min_ex    = min(current_prices, key=current_prices.get)
            max_price = current_prices[max_ex]
            min_price = current_prices[min_ex]

            divergence = (max_price - min_price) / min_price

            if divergence >= self.min_divergence_pct / 100:
                event = {
                    "symbol":      symbol,
                    "buy_exchange": min_ex,   # kup na tańszej
                    "sell_exchange": max_ex,  # sprzedaj na droższej
                    "divergence_pct": divergence * 100,
                    "buy_price":   min_price,
                    "sell_price":  max_price,
                    "ts":          time.time(),
                }
                self.arb_events.append(event)
                return event

        return None

    def get_active_opportunities(self, min_divergence: float = 0.05) -> List[Dict]:
        cutoff = time.time() - 10  # ostatnie 10 sekund
        return [e for e in self.arb_events if e["ts"] > cutoff and e["divergence_pct"] >= min_divergence]


# ─── EXCHANGE STRATEGY OPTIMIZER ─────────────────────────────────────────────

class ExchangeStrategyOptimizer:
    """
    Centralny optimizer strategii per giełda.

    Zarządza:
    - Parametrami per (exchange, symbol)
    - Online tuningiem przez Gaussian Bandit
    - Strategią Funding Arb
    - Latency Arb
    - Wyuczonymi anomaliami

    Przepływ:
    1. get_params(exchange, symbol) → dostaje zoptymalizowane parametry
    2. record_result(exchange, symbol, result) → uczy się
    3. Co N transakcji: tune_params() → aktualizuje parametry
    """

    def __init__(self, save_dir: str = "logs/strategy_optimizer"):
        self.save_dir = save_dir
        os.makedirs(save_dir, exist_ok=True)

        # Parametry per (exchange, symbol)
        self.params:   Dict[str, ExchangeStrategyParams] = {}
        # Tunery per (exchange, symbol)
        self.tuners:   Dict[str, OnlineTuner] = {}
        # Funding arb per exchange
        self.funding:  Dict[str, FundingArbStrategy] = {}
        # Latency arb (cross-exchange)
        self.latency   = LatencyArbDetector()

        # Wyniki per (exchange, symbol)
        self.results:  Dict[str, deque] = {}

        self._lock = threading.Lock()
        self._last_save = 0.0
        self._load()

        logger.info("📊 Strategy Optimizer initialized")

    def _key(self, exchange: str, symbol: str) -> str:
        return f"{exchange}::{symbol}"

    # ── PARAMETER MANAGEMENT ──────────────────────────────────────────────────

    def get_params(self, exchange: str, symbol: str) -> ExchangeStrategyParams:
        """Pobierz zoptymalizowane parametry dla tej pary na tej giełdzie."""
        key = self._key(exchange, symbol)

        with self._lock:
            if key not in self.params:
                self.params[key] = self._create_default_params(exchange, symbol)
            return self.params[key]

    def _create_default_params(self, exchange: str, symbol: str) -> ExchangeStrategyParams:
        """Twórz domyślne parametry dostosowane do giełdy."""
        p = ExchangeStrategyParams(exchange=exchange, symbol=symbol)

        # Exchange-specific defaults
        if exchange == "binance":
            p.fee_maker       = 0.0002   # z BNB discount
            p.fee_taker       = 0.0004
            p.take_profit_pct = 0.90     # mniejszy TP = więcej transakcji przy płynności
            p.stop_loss_pct   = 0.35
            p.min_confidence  = 0.62
            p.max_size_usd    = 500.0
            p.leverage        = 3
            p.primary_strategy = "SCALP"

        elif exchange == "bybit":
            p.fee_maker       = 0.0001   # maker rebate
            p.fee_taker       = 0.0006
            p.use_maker       = True     # Bybit opłaca się jako maker
            p.take_profit_pct = 1.10
            p.stop_loss_pct   = 0.45
            p.min_confidence  = 0.65
            p.max_size_usd    = 400.0
            p.leverage        = 5        # Bybit pozwala więcej
            p.primary_strategy = "SCALP"

        elif exchange == "okx":
            p.fee_maker       = 0.0002
            p.fee_taker       = 0.0005
            p.take_profit_pct = 1.00
            p.stop_loss_pct   = 0.40
            p.min_confidence  = 0.64
            p.max_size_usd    = 350.0
            p.leverage        = 3
            p.primary_strategy = "SCALP"

        elif exchange == "kucoin":
            p.fee_maker       = 0.0002
            p.fee_taker       = 0.0006
            p.take_profit_pct = 1.20     # więcej płynności potrzeba
            p.stop_loss_pct   = 0.50
            p.min_confidence  = 0.68
            p.max_size_usd    = 200.0
            p.leverage        = 3
            p.primary_strategy = "SWING"

        elif exchange == "gate":
            p.fee_maker       = 0.0002
            p.fee_taker       = 0.0004
            p.take_profit_pct = 1.30
            p.stop_loss_pct   = 0.55
            p.min_confidence  = 0.70
            p.max_size_usd    = 150.0
            p.primary_strategy = "SWING"

        # Symbol-specific adjustments
        base = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")

        if base in ["BTC", "ETH"]:
            # Mniejszy TP dla bardzo płynnych par
            p.take_profit_pct *= 0.85
            p.max_size_usd    *= 1.5
            p.min_confidence  -= 0.03

        elif base in ["DOGE", "SHIB", "PEPE", "WIF", "BONK"]:
            # Meme coins: wyższy TP, mniejszy rozmiar, większa pewność
            p.take_profit_pct *= 1.3
            p.stop_loss_pct   *= 0.8   # szybszy SL
            p.max_size_usd    *= 0.5
            p.min_confidence  += 0.05
            p.max_hold_min    = 60.0   # krótszy hold

        elif base in ["SOL", "AVAX", "NEAR", "APT", "SUI"]:
            # High-volatility L1: standardowo
            p.take_profit_pct *= 1.1
            p.max_size_usd    *= 0.9

        return p

    # ── RESULT RECORDING & TUNING ─────────────────────────────────────────────

    def record_result(
        self,
        exchange:   str,
        symbol:     str,
        pnl_pct:    float,
        fee_pct:    float,
        hold_min:   float,
        confidence: float,
        direction:  str,
        exit_reason: str,
    ) -> None:
        """Zapisz wynik transakcji i triggeruj tuning."""
        key = self._key(exchange, symbol)

        with self._lock:
            if key not in self.results:
                self.results[key] = deque(maxlen=500)

            result = {
                "pnl":        pnl_pct,
                "fee":        fee_pct,
                "net":        pnl_pct - fee_pct,
                "hold_min":   hold_min,
                "confidence": confidence,
                "direction":  direction,
                "exit":       exit_reason,
                "ts":         time.time(),
            }
            self.results[key].append(result)

        # Tune co 10 transakcji
        n = len(self.results.get(key, []))
        if n >= 10 and n % 10 == 0:
            self._tune_params(exchange, symbol)

        # Auto-save co 50 transakcji
        if n % 50 == 0:
            self.save()

    def _tune_params(self, exchange: str, symbol: str) -> None:
        """
        Dostrój parametry strategii na podstawie historii wyników.
        Używa OnlineTuner + reguł heurystycznych.
        """
        key = self._key(exchange, symbol)
        results = list(self.results.get(key, []))

        if len(results) < 10:
            return

        p = self.params[key]

        # Analiza wyników
        pnls       = [r["net"] for r in results[-50:]]
        wins       = [r for r in results[-50:] if r["pnl"] > 0]
        losses     = [r for r in results[-50:] if r["pnl"] <= 0]
        win_rate   = len(wins) / max(len(results[-50:]), 1)
        avg_win    = np.mean([r["pnl"] for r in wins])    if wins    else 0
        avg_loss   = np.mean([r["pnl"] for r in losses])  if losses  else 0
        avg_hold   = np.mean([r["hold_min"] for r in results[-50:]])

        # Timeouty = sygnał że TP za wysoki lub SL za mały
        timeouts   = [r for r in results[-50:] if r["exit"] == "TIMEOUT"]
        timeout_pct = len(timeouts) / max(len(results[-50:]), 1)

        # ── Reguły automatycznego dostrajania ─────────────────────────────

        tuned = False

        # Za dużo timeoutów (>30%) → zmniejsz TP lub wydłuż max_hold
        if timeout_pct > 0.30:
            if p.take_profit_pct > 0.6:
                p.take_profit_pct = max(0.6, p.take_profit_pct * 0.92)
                tuned = True
            elif p.max_hold_min < 180:
                p.max_hold_min = min(240, p.max_hold_min * 1.15)
                tuned = True

        # Wysoki win rate ale mały zysk → podnieś TP
        if win_rate > 0.70 and avg_win < p.take_profit_pct * 0.7:
            p.take_profit_pct = min(3.0, p.take_profit_pct * 1.08)
            tuned = True

        # Niski win rate (<45%) → podnieś min_confidence lub zmniejsz TP
        if win_rate < 0.45 and len(results) >= 20:
            if p.min_confidence < 0.80:
                p.min_confidence = min(0.80, p.min_confidence + 0.02)
                tuned = True
            if p.stop_loss_pct > 0.25:
                p.stop_loss_pct = max(0.25, p.stop_loss_pct * 0.95)
                tuned = True

        # Duże straty = SL za szeroki → zaciśnij
        if avg_loss < -p.stop_loss_pct * 1.3 and losses:
            p.stop_loss_pct = max(0.20, p.stop_loss_pct * 0.90)
            tuned = True

        # Online tuner: zaproponuj i przetestuj nowy zestaw
        tuner_key = key
        if tuner_key not in self.tuners:
            self.tuners[tuner_key] = OnlineTuner(symbol, exchange)

        tuner = self.tuners[tuner_key]
        reward = np.mean(pnls) * win_rate * math.sqrt(len(results[-50:]))
        tuner.record(p.to_dict(), reward)

        # Co 20 iteracji: użyj sugestii tunera
        if p.n_tuning_iters % 20 == 0 and p.n_tuning_iters > 0:
            suggestion = tuner.suggest(p)
            if suggestion:
                for param, value in suggestion.items():
                    if hasattr(p, param):
                        setattr(p, param, value)
                        tuned = True

        p.n_tuning_iters += 1
        p.last_tuned      = time.time()
        p.fitness         = float(np.mean(pnls)) if pnls else 0

        if tuned:
            logger.info(
                f"⚙️ [{exchange}] {symbol}: parametry dostrojone "
                f"(TP={p.take_profit_pct:.2f}% SL={p.stop_loss_pct:.2f}% "
                f"conf={p.min_confidence:.2f} wr={win_rate:.1%})"
            )

    # ── FUNDING ARB ──────────────────────────────────────────────────────────

    def get_funding_arb(self, exchange: str) -> List[Dict]:
        """Pobierz aktualne okazje funding arb dla giełdy."""
        if exchange not in self.funding:
            self.funding[exchange] = FundingArbStrategy(exchange)
        return self.funding[exchange].get_best_arb_opportunities()

    def update_funding_rate(self, exchange: str, symbol: str, rate: float) -> Optional[Dict]:
        if exchange not in self.funding:
            self.funding[exchange] = FundingArbStrategy(exchange)
        return self.funding[exchange].update_funding(symbol, rate)

    # ── LATENCY ARB ──────────────────────────────────────────────────────────

    def update_price_cross_exchange(self, symbol: str, exchange: str, price: float) -> Optional[Dict]:
        """Aktualizuj cenę cross-exchange i szukaj arb."""
        return self.latency.update(symbol, exchange, price)

    def get_latency_arb(self) -> List[Dict]:
        return self.latency.get_active_opportunities()

    # ── EXCHANGE COMPARISON ───────────────────────────────────────────────────

    def get_best_exchange_for_symbol(self, symbol: str, exchanges: List[str]) -> Tuple[str, Dict]:
        """
        Znajdź najlepszą giełdę dla danego symbolu.
        Uwzględnia: opłaty, płynność, wyniki historyczne.
        """
        if not exchanges:
            return exchanges[0] if exchanges else "binance", {}

        scores = {}
        for ex in exchanges:
            p = self.get_params(ex, symbol)
            key = self._key(ex, symbol)
            results = list(self.results.get(key, []))

            if results:
                avg_net = np.mean([r["net"] for r in results[-20:]])
                win_rate = sum(1 for r in results[-20:] if r["pnl"] > 0) / max(len(results[-20:]), 1)
            else:
                avg_net  = 0.0
                win_rate = 0.5

            # Score: niższe opłaty + lepsze wyniki
            fee_score = (0.001 - p.fee_taker) * 100       # mniejsze opłaty = lepiej
            perf_score = avg_net * win_rate * 10           # wyniki historyczne

            scores[ex] = fee_score + perf_score

        best_ex = max(scores, key=scores.get)
        return best_ex, {
            "scores": {ex: round(s, 4) for ex, s in scores.items()},
            "winner": best_ex,
            "params": self.get_params(best_ex, symbol).to_dict(),
        }

    # ── DIAGNOSTICS ──────────────────────────────────────────────────────────

    def get_exchange_report(self, exchange: str) -> Dict:
        """Raport optymalizacji dla giełdy."""
        exchange_results = {
            k.split("::")[1]: list(v)
            for k, v in self.results.items()
            if k.startswith(f"{exchange}::")
        }

        report = {
            "exchange":        exchange,
            "tracked_symbols": len(exchange_results),
            "total_trades":    sum(len(r) for r in exchange_results.values()),
            "symbols": {},
        }

        for sym, results in exchange_results.items():
            if not results:
                continue
            pnls = [r["net"] for r in results[-50:]]
            report["symbols"][sym] = {
                "trades":   len(results),
                "win_rate": round(sum(1 for r in results[-20:] if r["pnl"] > 0) / max(len(results[-20:]), 1), 3),
                "avg_net":  round(np.mean(pnls), 4) if pnls else 0,
                "fitness":  round(self.params.get(self._key(exchange, sym), ExchangeStrategyParams(exchange, sym)).fitness, 4),
                "tuning_iters": self.params.get(self._key(exchange, sym), ExchangeStrategyParams(exchange, sym)).n_tuning_iters,
            }

        # Posortuj po avg_net
        report["top_symbols"] = sorted(
            report["symbols"].items(),
            key=lambda x: -x[1].get("avg_net", 0)
        )[:10]

        return report

    def get_global_report(self) -> Dict:
        exchanges = set(k.split("::")[0] for k in self.params.keys())
        return {ex: self.get_exchange_report(ex) for ex in exchanges}

    # ── PERSISTENCE ──────────────────────────────────────────────────────────

    def save(self) -> None:
        path = os.path.join(self.save_dir, "strategy_optimizer.json")
        try:
            data = {}
            for key, p in self.params.items():
                data[key] = {
                    "take_profit_pct":  p.take_profit_pct,
                    "stop_loss_pct":    p.stop_loss_pct,
                    "min_confidence":   p.min_confidence,
                    "trailing_trigger": p.trailing_trigger,
                    "trailing_step":    p.trailing_step,
                    "max_hold_min":     p.max_hold_min,
                    "base_size_usd":    p.base_size_usd,
                    "max_size_usd":     p.max_size_usd,
                    "use_maker":        p.use_maker,
                    "leverage":         p.leverage,
                    "fee_maker":        p.fee_maker,
                    "fee_taker":        p.fee_taker,
                    "primary_strategy": p.primary_strategy,
                    "n_tuning_iters":   p.n_tuning_iters,
                    "fitness":          p.fitness,
                    "preferred_hours":  p.preferred_hours,
                    "avoid_hours":      p.avoid_hours,
                    "signal_blend":     p.signal_blend,
                }
            # Results summary (not full history)
            results_summary = {
                k: {
                    "n": len(v),
                    "avg_net": round(np.mean([r["net"] for r in list(v)[-50:]]), 4) if v else 0,
                    "win_rate": round(sum(1 for r in list(v)[-20:] if r["pnl"] > 0) / max(len(list(v)[-20:]), 1), 3) if v else 0,
                }
                for k, v in self.results.items()
            }
            with open(path, "w") as f:
                json.dump({"params": data, "results_summary": results_summary}, f, indent=2)
        except Exception as e:
            logger.debug(f"Optimizer save failed: {e}")

    def _load(self) -> None:
        path = os.path.join(self.save_dir, "strategy_optimizer.json")
        if not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)

            for key, pd in data.get("params", {}).items():
                parts = key.split("::")
                if len(parts) != 2:
                    continue
                exchange, symbol = parts
                p = self._create_default_params(exchange, symbol)
                for attr, val in pd.items():
                    if hasattr(p, attr):
                        setattr(p, attr, val)
                self.params[key] = p

            n = len(self.params)
            logger.info(f"✅ Strategy optimizer wczytany: {n} zestawów parametrów")
        except Exception as e:
            logger.debug(f"Optimizer load failed: {e}")
