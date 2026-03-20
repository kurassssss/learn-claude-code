#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ★ KR √¡K — ULTIMATE TRAINING GAUNTLET v3.0                             ║
║                                                                              ║
║   10-Year Extreme Stress Test × Curriculum RL Training × Adversarial        ║
║                                                                              ║
║   WARSTWY TRENINGU:                                                          ║
║   ① MAKRO-SYMULACJA     100 botów × 10 lat × 3650 dni (styl KRAKEN)        ║
║   ② TICK TRAINING       5 lat ticków GARCH + jumps + 45 wydarzeń RL        ║
║   ③ CURRICULUM          5 poziomów od AWAKENING do THE IMPOSSIBLE           ║
║   ④ ADVERSARIAL         7 typów pułapek: bull trap, stop-hunt, spoof…      ║
║   ⑤ RAPORT              ASCII equity curve + per-bot stats + RL ranking     ║
║                                                                              ║
║   "Kto przeżyje ten trening, przeżyje wszystko."                            ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import math
import time
import random
import logging
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import deque, defaultdict
from datetime import datetime, timedelta
from enum import Enum

# ── Setup ──────────────────────────────────────────────────────────────────────
for d in ["rl_models", "training_logs"]:
    Path(d).mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s │ %(levelname)-5s │ %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'training_logs/gauntlet_{int(time.time())}.log',
                            encoding='utf-8'),
    ]
)
logger = logging.getLogger("GAUNTLET")

# ── RL engine import ───────────────────────────────────────────────────────────
try:
    from rl_engines_v2 import (
        get_rl_orchestrator, ExtendedRLOrchestrator,
        RLState, Action, save_all_models, get_global_rl_stats,
    )
    RL_AVAILABLE = True
    RL_ENGINES   = 15
    logger.info("🌌 15-Engine Soul Cluster loaded")
except ImportError:
    try:
        from rl_engines import (
            get_rl_orchestrator, RLEngineOrchestrator as ExtendedRLOrchestrator,
            RLState, Action, save_all_models, get_global_rl_stats,
        )
        RL_AVAILABLE = True
        RL_ENGINES   = 5
        logger.info("⚡ 5-Engine Cluster loaded")
    except ImportError:
        RL_AVAILABLE = False
        RL_ENGINES   = 0
        logger.warning("⚠️  Brak modułu RL — trening tylko statystyczny")


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 1: REŻIMY RYNKOWE
#  12 reżimów z tick-level parametrami (GARCH, jumps, autocorr)
#  + 7 makro-etykiet emoji z oryginalnego stress_test_10y
# ══════════════════════════════════════════════════════════════════════════════

class MarketRegime(Enum):
    # — 7 makro-reżimów zgodnych z oryginałem (na raport dzienny) —
    BULL                = "🐂 BULL"
    BEAR                = "🐻 BEAR"
    SIDEWAYS            = "➡️  SIDEWAYS"
    CRASH               = "💥 CRASH"
    RECOVERY            = "📈 RECOVERY"
    EXTREME_VOLATILITY  = "🌪️  VOLATILE"
    LIQUIDITY_CRISIS    = "🚨 CRISIS"
    # — 5 dodatkowych reżimów z ultimate_training (tick-level) —
    STEALTH             = "🫥 STEALTH"
    DISTRIBUTION        = "📤 DISTRIBUTION"
    PARABOLIC           = "🚀 PARABOLIC"
    DEAD_CAT            = "🐈 DEAD CAT"
    MANIPULATED         = "🎭 MANIPULATED"


@dataclass
class RegimeConfig:
    """Pełna konfiguracja tick-level dla silnika GARCH + jumps."""
    name:              str
    macro_regime:      MarketRegime       # mapowanie na makro-etykietę
    duration_ticks:    Tuple[int, int]    # (min, max) ticków w reżimie
    drift_per_tick:    float              # oczekiwany zwrot per tick
    volatility:        float              # bazowa vol per tick
    vol_of_vol:        float              # clustering intensywność
    jump_prob:         float              # prawdop. skoku per tick
    jump_size_mean:    float              # średni rozmiar skoku
    jump_size_std:     float              # odchylenie rozmiaru skoku
    autocorr:          float              # autokorelacja ceny (momentum)
    spread_mult:       float              # mnożnik spreadu (1=normalny)
    ob_imbalance_bias: float              # stronniczość orderbooka
    funding_rate:      float              # funding rate
    false_signal_prob: float              # % fałszywych sygnałów
    win_rate_modifier: float              # modyfikator WR (ze stress_test)
    trade_prob:        float              # pr. czy bot w ogóle wchodzi


REGIME_CONFIGS: Dict[str, RegimeConfig] = {

    "STEALTH": RegimeConfig(
        "Stealth Accumulation", MarketRegime.SIDEWAYS,
        (200, 800), 0.00003, 0.0008, 0.10, 0.002, -0.003, 0.002,
        0.05, 1.2, 0.10, -0.00005, 0.30,
        win_rate_modifier=0.02, trade_prob=0.08),

    "BULL": RegimeConfig(
        "Bull Trend", MarketRegime.BULL,
        (300, 1200), 0.00015, 0.0015, 0.15, 0.008, 0.008, 0.005,
        0.35, 0.9, 0.30, 0.0002, 0.15,
        win_rate_modifier=0.15, trade_prob=0.12),

    "DISTRIBUTION": RegimeConfig(
        "Distribution", MarketRegime.EXTREME_VOLATILITY,
        (150, 600), -0.00005, 0.002, 0.30, 0.015, 0.004, 0.008,
        -0.10, 1.5, -0.15, 0.0005, 0.60,
        win_rate_modifier=-0.08, trade_prob=0.04),

    "BEAR": RegimeConfig(
        "Bear Trend", MarketRegime.BEAR,
        (400, 1500), -0.00018, 0.002, 0.20, 0.010, -0.010, 0.006,
        0.25, 1.3, -0.35, -0.0003, 0.25,
        win_rate_modifier=-0.15, trade_prob=0.06),

    "SIDEWAYS_TIGHT": RegimeConfig(
        "Ranging Tight", MarketRegime.SIDEWAYS,
        (500, 2000), 0.0, 0.0005, 0.05, 0.001, 0.0, 0.001,
        -0.20, 1.0, 0.0, 0.0001, 0.45,
        win_rate_modifier=-0.05, trade_prob=0.08),

    "SIDEWAYS_WIDE": RegimeConfig(
        "Ranging Wide", MarketRegime.EXTREME_VOLATILITY,
        (200, 800), 0.0, 0.003, 0.25, 0.005, 0.0, 0.005,
        -0.15, 1.4, 0.0, 0.0, 0.50,
        win_rate_modifier=-0.05, trade_prob=0.04),

    "PARABOLIC": RegimeConfig(
        "Parabolic Blow-off", MarketRegime.BULL,
        (50, 200), 0.001, 0.004, 0.40, 0.04, 0.025, 0.015,
        0.60, 0.7, 0.70, 0.001, 0.05,
        win_rate_modifier=0.20, trade_prob=0.12),

    "CAPITULATION": RegimeConfig(
        "Capitulation", MarketRegime.CRASH,
        (30, 150), -0.002, 0.008, 0.80, 0.08, -0.030, 0.020,
        0.40, 4.0, -0.80, -0.002, 0.70,
        win_rate_modifier=-0.40, trade_prob=0.02),

    "DEAD_CAT": RegimeConfig(
        "Dead Cat Bounce", MarketRegime.RECOVERY,
        (50, 200), 0.0004, 0.003, 0.30, 0.02, -0.015, 0.010,
        0.20, 2.0, 0.10, 0.0003, 0.75,
        win_rate_modifier=-0.10, trade_prob=0.06),

    "LIQUIDITY_HUNT": RegimeConfig(
        "Liquidity Hunt", MarketRegime.EXTREME_VOLATILITY,
        (30, 100), 0.0, 0.005, 0.60, 0.10, 0.0, 0.020,
        -0.40, 2.5, 0.0, 0.0, 0.85,
        win_rate_modifier=-0.20, trade_prob=0.03),

    "FLASH_CRASH": RegimeConfig(
        "Flash Crash", MarketRegime.CRASH,
        (5, 30), -0.008, 0.015, 1.0, 0.30, -0.050, 0.030,
        0.30, 10.0, -1.0, -0.005, 0.90,
        win_rate_modifier=-0.35, trade_prob=0.02),

    "MANIPULATED": RegimeConfig(
        "Whale Manipulation", MarketRegime.EXTREME_VOLATILITY,
        (50, 300), 0.0, 0.004, 0.50, 0.05, 0.0, 0.015,
        -0.30, 1.8, 0.0, 0.0, 0.80,
        win_rate_modifier=-0.15, trade_prob=0.03),
}

# Mapowanie makro-reżimów na config dla symulatora dziennego
MACRO_TO_TICK_REGIME: Dict[MarketRegime, str] = {
    MarketRegime.BULL:               "BULL",
    MarketRegime.BEAR:               "BEAR",
    MarketRegime.SIDEWAYS:           "SIDEWAYS_TIGHT",
    MarketRegime.CRASH:              "CAPITULATION",
    MarketRegime.RECOVERY:           "DEAD_CAT",
    MarketRegime.EXTREME_VOLATILITY: "LIQUIDITY_HUNT",
    MarketRegime.LIQUIDITY_CRISIS:   "FLASH_CRASH",
    MarketRegime.STEALTH:            "STEALTH",
    MarketRegime.DISTRIBUTION:       "DISTRIBUTION",
    MarketRegime.PARABOLIC:          "PARABOLIC",
    MarketRegime.DEAD_CAT:           "DEAD_CAT",
    MarketRegime.MANIPULATED:        "MANIPULATED",
}


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 2: ZDARZENIA HISTORYCZNE
#  Pełna baza: 24 z oryginalnego stress_test + 45 z ultimate_training
#  Każde zdarzenie ma WSZYSTKIE pola potrzebne do obu symulatorów
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class UnifiedEvent:
    """
    Zdarzenie kompatybilne z OBYDWOMA symulatorami:
    - duration_days + win_rate_modifier + price_impact → dla stress_test (dzienny)
    - price_shock + vol_spike + vol_duration          → dla ultimate (tick-level)
    """
    name:              str
    date_approx:       str
    description:       str
    macro_regime:      MarketRegime
    # Pola stress_test (dzienny)
    duration_days:     int
    price_impact:      float           # % zmiana ceny (dzienny)
    volatility_multiplier: float       # mnożnik vol (dzienny)
    win_rate_modifier: float           # modyfikator WR botów
    # Pola ultimate (tick-level)
    price_shock:       float           # natychmiastowy szok ceny
    vol_spike:         float           # mnożnik volatility przez vol_duration ticków
    vol_duration:      int             # ile ticków trwa szok
    spread_spike:      float           # mnożnik spreadu
    liquidity_drought: int             # ticki z ograniczoną płynnością
    funding_shock:     float           # zmiana funding rate
    recovery_drift:    float           # drift po szoku
    lesson:            str             # czego uczy ten event


ALL_EVENTS: List[UnifiedEvent] = [

    # ══ ORYGINALNE ZE STRESS_TEST_10Y (24 eventy) ═══════════════════════════

    UnifiedEvent("Flash Crash", "GENERIC", "Nagły flash crash rynku",
        MacroRegime := MarketRegime.CRASH,
        duration_days=1, price_impact=-0.35, volatility_multiplier=10.0,
        win_rate_modifier=-0.30,
        price_shock=-0.35, vol_spike=10.0, vol_duration=30, spread_spike=8.0,
        liquidity_drought=20, funding_shock=-0.003, recovery_drift=0.001,
        lesson="Szybkość wyjścia jest wszystkim."),

    UnifiedEvent("Bear Market", "GENERIC", "Przedłużony rynek niedźwiedzia",
        MarketRegime.BEAR,
        duration_days=90, price_impact=-0.45, volatility_multiplier=3.0,
        win_rate_modifier=-0.15,
        price_shock=-0.45, vol_spike=3.0, vol_duration=500, spread_spike=1.5,
        liquidity_drought=100, funding_shock=-0.0005, recovery_drift=0.0002,
        lesson="Bessy są długie i wyczerpujące. Zarządzaj rozmiarami."),

    UnifiedEvent("Deep Bear", "GENERIC", "Głęboki bear market -70%",
        MarketRegime.BEAR,
        duration_days=180, price_impact=-0.70, volatility_multiplier=2.5,
        win_rate_modifier=-0.20,
        price_shock=-0.70, vol_spike=2.5, vol_duration=800, spread_spike=2.0,
        liquidity_drought=200, funding_shock=-0.001, recovery_drift=0.0001,
        lesson="Najgorsze jest zawsze możliwe."),

    UnifiedEvent("Recovery Rally", "GENERIC", "Odbicie po bessie",
        MarketRegime.RECOVERY,
        duration_days=60, price_impact=0.40, volatility_multiplier=2.0,
        win_rate_modifier=0.10,
        price_shock=0.40, vol_spike=2.0, vol_duration=200, spread_spike=0.9,
        liquidity_drought=0, funding_shock=0.0003, recovery_drift=0.0003,
        lesson="Najsilniejsze ruchy są w bessie. Wchodzić ostrożnie."),

    UnifiedEvent("Bull Run", "GENERIC", "Silny rynek byka",
        MarketRegime.BULL,
        duration_days=120, price_impact=0.80, volatility_multiplier=1.5,
        win_rate_modifier=0.15,
        price_shock=0.80, vol_spike=1.5, vol_duration=400, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.001, recovery_drift=0.0004,
        lesson="Trzymaj winnery. Pozwól zyskom rosnąć."),

    UnifiedEvent("Parabolic Top", "GENERIC", "Paraboliczne wybicie — szczyt",
        MarketRegime.BULL,
        duration_days=30, price_impact=1.50, volatility_multiplier=4.0,
        win_rate_modifier=0.20,
        price_shock=1.50, vol_spike=4.0, vol_duration=100, spread_spike=0.7,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0005,
        lesson="Parabolika zawsze kończy się katastrofą."),

    UnifiedEvent("Blow-off Crash", "GENERIC", "Gwałtowny krach po parabolice",
        MarketRegime.CRASH,
        duration_days=7, price_impact=-0.50, volatility_multiplier=8.0,
        win_rate_modifier=-0.25,
        price_shock=-0.50, vol_spike=8.0, vol_duration=50, spread_spike=6.0,
        liquidity_drought=40, funding_shock=-0.003, recovery_drift=0.0005,
        lesson="Blow-off crasch jest gwałtowny i błyskawiczny."),

    UnifiedEvent("Sideways Chop", "GENERIC", "Monotonny ruch boczny",
        MarketRegime.SIDEWAYS,
        duration_days=90, price_impact=0.05, volatility_multiplier=0.8,
        win_rate_modifier=-0.05,
        price_shock=0.05, vol_spike=0.8, vol_duration=400, spread_spike=1.0,
        liquidity_drought=30, funding_shock=0.0001, recovery_drift=0.0,
        lesson="Rynek boczny niszczy boty skalperskie przez opłaty."),

    UnifiedEvent("Liquidity Crisis", "GENERIC", "Kryzys płynności",
        MarketRegime.LIQUIDITY_CRISIS,
        duration_days=14, price_impact=-0.40, volatility_multiplier=6.0,
        win_rate_modifier=-0.35,
        price_shock=-0.40, vol_spike=6.0, vol_duration=80, spread_spike=7.0,
        liquidity_drought=100, funding_shock=-0.002, recovery_drift=0.0003,
        lesson="Brak płynności = nie możesz wyjść. Nigdy."),

    UnifiedEvent("Dead Cat Bounce", "GENERIC", "Fałszywe odbicie",
        MarketRegime.RECOVERY,
        duration_days=21, price_impact=0.30, volatility_multiplier=3.5,
        win_rate_modifier=0.05,
        price_shock=0.30, vol_spike=3.5, vol_duration=80, spread_spike=2.0,
        liquidity_drought=20, funding_shock=0.0003, recovery_drift=-0.0003,
        lesson="Nie każde odbicie jest końcem bessy."),

    UnifiedEvent("Capitulation", "GENERIC", "Masowa kapitulacja",
        MarketRegime.CRASH,
        duration_days=3, price_impact=-0.45, volatility_multiplier=12.0,
        win_rate_modifier=-0.40,
        price_shock=-0.45, vol_spike=12.0, vol_duration=40, spread_spike=9.0,
        liquidity_drought=60, funding_shock=-0.004, recovery_drift=0.0008,
        lesson="Kapitulacja = okazja dla cierpliwych."),

    UnifiedEvent("Black Swan", "GENERIC", "Czarny łabędź — niemożliwe staje się możliwe",
        MarketRegime.CRASH,
        duration_days=1, price_impact=-0.55, volatility_multiplier=15.0,
        win_rate_modifier=-0.50,
        price_shock=-0.55, vol_spike=15.0, vol_duration=30, spread_spike=12.0,
        liquidity_drought=80, funding_shock=-0.005, recovery_drift=0.001,
        lesson="Czarne łabędzie istnieją. Plan na nieplanowane."),

    UnifiedEvent("Exchange Hack", "GENERIC", "Hack giełdy",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=7, price_impact=-0.25, volatility_multiplier=5.0,
        win_rate_modifier=-0.20,
        price_shock=-0.25, vol_spike=5.0, vol_duration=50, spread_spike=4.0,
        liquidity_drought=40, funding_shock=-0.001, recovery_drift=0.0002,
        lesson="Counterparty risk jest realny."),

    UnifiedEvent("Regulatory FUD", "GENERIC", "Regulacyjne FUD",
        MarketRegime.BEAR,
        duration_days=30, price_impact=-0.35, volatility_multiplier=3.0,
        win_rate_modifier=-0.15,
        price_shock=-0.35, vol_spike=3.0, vol_duration=100, spread_spike=2.0,
        liquidity_drought=30, funding_shock=-0.001, recovery_drift=0.0002,
        lesson="Regulacje zmieniają zasady gry z dnia na dzień."),

    UnifiedEvent("Institutional Buy", "GENERIC", "Zakup instytucjonalny",
        MarketRegime.BULL,
        duration_days=60, price_impact=0.60, volatility_multiplier=2.0,
        win_rate_modifier=0.20,
        price_shock=0.60, vol_spike=2.0, vol_duration=200, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.001, recovery_drift=0.0004,
        lesson="Instytucje poruszają rynkami. Idź z nimi."),

    UnifiedEvent("ETF Approval", "GENERIC", "Zatwierdzenie ETF",
        MarketRegime.BULL,
        duration_days=14, price_impact=0.45, volatility_multiplier=3.5,
        win_rate_modifier=0.25,
        price_shock=0.45, vol_spike=3.5, vol_duration=60, spread_spike=0.7,
        liquidity_drought=0, funding_shock=0.0005, recovery_drift=0.0003,
        lesson="Sell the news po każdym ETF."),

    UnifiedEvent("Whale Dump", "GENERIC", "Dump wieloryba",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=3, price_impact=-0.20, volatility_multiplier=8.0,
        win_rate_modifier=-0.15,
        price_shock=-0.20, vol_spike=8.0, vol_duration=30, spread_spike=4.0,
        liquidity_drought=20, funding_shock=-0.001, recovery_drift=0.0005,
        lesson="Wieloryby mogą zatopić każdą parę."),

    UnifiedEvent("Short Squeeze", "GENERIC", "Short squeeze",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=2, price_impact=0.35, volatility_multiplier=7.0,
        win_rate_modifier=0.30,
        price_shock=0.35, vol_spike=7.0, vol_duration=20, spread_spike=2.0,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0003,
        lesson="Short squeeze może zniszczyć shorterów w minutach."),

    UnifiedEvent("Long Squeeze", "GENERIC", "Long squeeze",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=2, price_impact=-0.30, volatility_multiplier=7.0,
        win_rate_modifier=-0.25,
        price_shock=-0.30, vol_spike=7.0, vol_duration=20, spread_spike=2.5,
        liquidity_drought=10, funding_shock=-0.002, recovery_drift=0.0004,
        lesson="Long squeeze eliminuje lewarowane longa w sekundy."),

    UnifiedEvent("Halving Pump", "GENERIC", "Halving pump BTC",
        MarketRegime.BULL,
        duration_days=30, price_impact=0.25, volatility_multiplier=2.5,
        win_rate_modifier=0.15,
        price_shock=0.25, vol_spike=2.5, vol_duration=100, spread_spike=0.9,
        liquidity_drought=0, funding_shock=0.0003, recovery_drift=0.0003,
        lesson="Halvings wymagają miesięcy by zadziałać."),

    UnifiedEvent("DeFi Summer", "GENERIC", "DeFi Summer hype",
        MarketRegime.BULL,
        duration_days=60, price_impact=0.90, volatility_multiplier=3.0,
        win_rate_modifier=0.25,
        price_shock=0.90, vol_spike=3.0, vol_duration=200, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0002,
        lesson="Narracje napędzają cykl byka. Płyń z narracją."),

    UnifiedEvent("NFT Mania", "GENERIC", "Szał NFT",
        MarketRegime.BULL,
        duration_days=45, price_impact=0.70, volatility_multiplier=4.0,
        win_rate_modifier=0.20,
        price_shock=0.70, vol_spike=4.0, vol_duration=150, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.0015, recovery_drift=-0.0003,
        lesson="Spekulacyjne szały zawsze kończą się krachem."),

    UnifiedEvent("Stablecoin Depeg", "GENERIC", "Depeg stablecoina",
        MarketRegime.CRASH,
        duration_days=7, price_impact=-0.30, volatility_multiplier=9.0,
        win_rate_modifier=-0.35,
        price_shock=-0.30, vol_spike=9.0, vol_duration=50, spread_spike=6.0,
        liquidity_drought=50, funding_shock=-0.002, recovery_drift=0.0005,
        lesson="Stablecoiny nie są stabilne w kryzysie."),

    UnifiedEvent("Fed Rate Hike", "GENERIC", "Podwyżka stóp Fed",
        MarketRegime.BEAR,
        duration_days=30, price_impact=-0.25, volatility_multiplier=2.5,
        win_rate_modifier=-0.10,
        price_shock=-0.25, vol_spike=2.5, vol_duration=100, spread_spike=1.5,
        liquidity_drought=20, funding_shock=-0.0005, recovery_drift=0.0002,
        lesson="Makroekonomia jest nadreżyserem krypto."),

    # ══ DODATKOWE Z ULTIMATE_TRAINING (45 historycznych eventów) ════════════

    UnifiedEvent("BitFinex $850M Scandal", "2019-04",
        "Tether used to mask $850M losses",
        MarketRegime.BEAR,
        duration_days=14, price_impact=-0.15, volatility_multiplier=3.0,
        win_rate_modifier=-0.10,
        price_shock=-0.15, vol_spike=3.0, vol_duration=80, spread_spike=2.5,
        liquidity_drought=40, funding_shock=-0.0005, recovery_drift=0.0002,
        lesson="Trust nothing. Fundamentals evaporate overnight."),

    UnifiedEvent("Binance 7000 BTC Hack", "2019-05",
        "Security breach, 2% BTC supply stolen",
        MarketRegime.CRASH,
        duration_days=5, price_impact=-0.12, volatility_multiplier=4.0,
        win_rate_modifier=-0.15,
        price_shock=-0.12, vol_spike=4.0, vol_duration=50, spread_spike=3.0,
        liquidity_drought=30, funding_shock=-0.001, recovery_drift=0.0003,
        lesson="External shocks arrive without warning. Exit speed matters."),

    UnifiedEvent("PlusToken Ponzi Dump", "2019-06",
        "200k BTC systematically dumped over months",
        MarketRegime.BEAR,
        duration_days=180, price_impact=-0.25, volatility_multiplier=1.5,
        win_rate_modifier=-0.08,
        price_shock=-0.05, vol_spike=1.5, vol_duration=500, spread_spike=1.3,
        liquidity_drought=100, funding_shock=-0.0002, recovery_drift=0.0001,
        lesson="Slow bleeds are harder to detect than crashes."),

    UnifiedEvent("Black Thursday COVID Crash", "2020-03-12",
        "BTC -50% in 24h — global pandemic panic",
        MarketRegime.CRASH,
        duration_days=2, price_impact=-0.52, volatility_multiplier=12.0,
        win_rate_modifier=-0.45,
        price_shock=-0.52, vol_spike=12.0, vol_duration=200, spread_spike=8.0,
        liquidity_drought=150, funding_shock=-0.003, recovery_drift=0.0008,
        lesson="Macro correlation = 1.0 in a true crisis. No hedge."),

    UnifiedEvent("BitMEX Liquidation Cascade", "2020-03-13",
        "100M liquidated in hours, system overloaded",
        MarketRegime.CRASH,
        duration_days=1, price_impact=-0.35, volatility_multiplier=15.0,
        win_rate_modifier=-0.40,
        price_shock=-0.35, vol_spike=15.0, vol_duration=50, spread_spike=12.0,
        liquidity_drought=80, funding_shock=-0.005, recovery_drift=0.001,
        lesson="Leverage cascade is self-reinforcing. Know when market breaks."),

    UnifiedEvent("Elon Musk BTC Tweet +20pct", "2021-01",
        "Single tweet moves $1T asset by 20%",
        MarketRegime.BULL,
        duration_days=3, price_impact=0.22, volatility_multiplier=5.0,
        win_rate_modifier=0.15,
        price_shock=0.22, vol_spike=5.0, vol_duration=60, spread_spike=1.5,
        liquidity_drought=0, funding_shock=0.001, recovery_drift=0.0002,
        lesson="Social media risk is market risk. Expect the impossible."),

    UnifiedEvent("Tesla Dumps BTC", "2021-05",
        "Tesla announces BTC sale — market -30%",
        MarketRegime.CRASH,
        duration_days=7, price_impact=-0.32, volatility_multiplier=6.0,
        win_rate_modifier=-0.25,
        price_shock=-0.32, vol_spike=6.0, vol_duration=100, spread_spike=3.0,
        liquidity_drought=60, funding_shock=-0.002, recovery_drift=0.0003,
        lesson="What goes up on Elon goes down on Elon."),

    UnifiedEvent("China Mining Ban 2021", "2021-06",
        "50% global hashrate offline overnight",
        MarketRegime.CRASH,
        duration_days=30, price_impact=-0.25, volatility_multiplier=4.5,
        win_rate_modifier=-0.18,
        price_shock=-0.25, vol_spike=4.5, vol_duration=80, spread_spike=2.5,
        liquidity_drought=40, funding_shock=-0.0015, recovery_drift=0.0004,
        lesson="Supply side shocks are as violent as demand shocks."),

    UnifiedEvent("El Salvador BTC Legal Tender", "2021-06",
        "Sovereign adoption — then instant sell the news",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=5, price_impact=0.12, volatility_multiplier=3.0,
        win_rate_modifier=0.05,
        price_shock=0.12, vol_spike=3.0, vol_duration=30, spread_spike=1.5,
        liquidity_drought=0, funding_shock=0.0005, recovery_drift=-0.0002,
        lesson="Buy the rumor, sell the news. Every time."),

    UnifiedEvent("BTC ATH 69k November 2021", "2021-11",
        "Peak euphoria. The absolute top.",
        MarketRegime.BULL,
        duration_days=7, price_impact=0.15, volatility_multiplier=4.0,
        win_rate_modifier=0.10,
        price_shock=0.15, vol_spike=4.0, vol_duration=50, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0005,
        lesson="The ATH is quiet. Distribution is silent. The crash is loud."),

    UnifiedEvent("LUNA UST Depeg Day1", "2022-05-09",
        "UST loses peg. LUNA death spiral begins.",
        MarketRegime.CRASH,
        duration_days=1, price_impact=-0.45, volatility_multiplier=20.0,
        win_rate_modifier=-0.45,
        price_shock=-0.45, vol_spike=20.0, vol_duration=300, spread_spike=10.0,
        liquidity_drought=200, funding_shock=-0.005, recovery_drift=0.0001,
        lesson="Algorithmic stablecoins can lose 99% in 72 hours."),

    UnifiedEvent("LUNA Death Spiral Day2", "2022-05-10",
        "Hyperinflation of LUNA supply — goes to ZERO",
        MarketRegime.CRASH,
        duration_days=1, price_impact=-0.80, volatility_multiplier=50.0,
        win_rate_modifier=-0.50,
        price_shock=-0.80, vol_spike=50.0, vol_duration=100, spread_spike=20.0,
        liquidity_drought=500, funding_shock=-0.01, recovery_drift=0.0,
        lesson="Some assets go to exactly zero. Exit before the spiral."),

    UnifiedEvent("3AC Collapse", "2022-06",
        "Largest crypto hedge fund defaults on $3B",
        MarketRegime.CRASH,
        duration_days=20, price_impact=-0.30, volatility_multiplier=5.0,
        win_rate_modifier=-0.20,
        price_shock=-0.30, vol_spike=5.0, vol_duration=200, spread_spike=3.5,
        liquidity_drought=100, funding_shock=-0.002, recovery_drift=0.0001,
        lesson="Systemic counterparty risk. One domino falls, ten follow."),

    UnifiedEvent("Celsius Network Freezes", "2022-06-12",
        "1M users locked out — withdrawals frozen",
        MarketRegime.CRASH,
        duration_days=14, price_impact=-0.20, volatility_multiplier=4.0,
        win_rate_modifier=-0.18,
        price_shock=-0.20, vol_spike=4.0, vol_duration=100, spread_spike=3.0,
        liquidity_drought=80, funding_shock=-0.0015, recovery_drift=0.0001,
        lesson="CeFi yield platforms can gate at any moment."),

    UnifiedEvent("FTX Implosion Day1 CZ Leak", "2022-11-06",
        "CZ reveals FTX insolvency. Bank run begins.",
        MarketRegime.CRASH,
        duration_days=2, price_impact=-0.12, volatility_multiplier=6.0,
        win_rate_modifier=-0.25,
        price_shock=-0.12, vol_spike=6.0, vol_duration=50, spread_spike=4.0,
        liquidity_drought=40, funding_shock=-0.002, recovery_drift=-0.0005,
        lesson="Bank runs move at internet speed. 24h is geological time."),

    UnifiedEvent("FTX Bankruptcy Day3", "2022-11-08",
        "FTX files Chapter 11. SBF arrested. $8B hole.",
        MarketRegime.CRASH,
        duration_days=7, price_impact=-0.35, volatility_multiplier=15.0,
        win_rate_modifier=-0.40,
        price_shock=-0.35, vol_spike=15.0, vol_duration=200, spread_spike=8.0,
        liquidity_drought=150, funding_shock=-0.004, recovery_drift=0.0001,
        lesson="The second largest exchange can fail overnight."),

    UnifiedEvent("SVB Crisis USDC Depeg", "2023-03-10",
        "USDC depeg to $0.87 — Circle reserves at SVB",
        MarketRegime.CRASH,
        duration_days=3, price_impact=-0.15, volatility_multiplier=8.0,
        win_rate_modifier=-0.20,
        price_shock=-0.15, vol_spike=8.0, vol_duration=80, spread_spike=5.0,
        liquidity_drought=60, funding_shock=-0.002, recovery_drift=0.0005,
        lesson="Even 'safe' stablecoins can depeg. No asset is riskless."),

    UnifiedEvent("SEC vs Coinbase Binance", "2023-06",
        "US regulator lawsuits against both largest exchanges",
        MarketRegime.BEAR,
        duration_days=30, price_impact=-0.12, volatility_multiplier=3.0,
        win_rate_modifier=-0.12,
        price_shock=-0.12, vol_spike=3.0, vol_duration=100, spread_spike=2.0,
        liquidity_drought=50, funding_shock=-0.001, recovery_drift=0.0002,
        lesson="Legal risk reprices the entire sector simultaneously."),

    UnifiedEvent("Curve Finance Exploit", "2023-07-30",
        "Reentrancy attack — $70M drained",
        MarketRegime.CRASH,
        duration_days=3, price_impact=-0.08, volatility_multiplier=5.0,
        win_rate_modifier=-0.15,
        price_shock=-0.08, vol_spike=5.0, vol_duration=40, spread_spike=3.0,
        liquidity_drought=30, funding_shock=-0.0005, recovery_drift=0.0003,
        lesson="Smart contract risk is existential. Audits don't guarantee safety."),

    UnifiedEvent("BlackRock BTC ETF Filing", "2023-06-15",
        "Institutional demand narrative ignites entire market",
        MarketRegime.BULL,
        duration_days=30, price_impact=0.10, volatility_multiplier=2.5,
        win_rate_modifier=0.12,
        price_shock=0.10, vol_spike=2.5, vol_duration=40, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.0005, recovery_drift=0.0004,
        lesson="Narrative shifts drive multi-month trends. Be early."),

    UnifiedEvent("Binance CZ Fine and Resignation", "2023-11-21",
        "CZ pleads guilty. $4.3B fine. Largest in history.",
        MarketRegime.BEAR,
        duration_days=10, price_impact=-0.08, volatility_multiplier=4.0,
        win_rate_modifier=-0.10,
        price_shock=-0.08, vol_spike=4.0, vol_duration=80, spread_spike=2.5,
        liquidity_drought=50, funding_shock=-0.001, recovery_drift=0.0003,
        lesson="The biggest player is not too big to fail."),

    UnifiedEvent("Bitcoin Spot ETF Approval", "2024-01-10",
        "SEC approves spot BTC ETF. Sell the news immediately.",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=5, price_impact=0.05, volatility_multiplier=6.0,
        win_rate_modifier=-0.05,
        price_shock=0.05, vol_spike=6.0, vol_duration=80, spread_spike=1.2,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0003,
        lesson="Most anticipated event in history was sold instantly."),

    UnifiedEvent("GBTC Outflows 5B", "2024-01",
        "Grayscale becomes structural net seller post-ETF",
        MarketRegime.BEAR,
        duration_days=45, price_impact=-0.15, volatility_multiplier=3.5,
        win_rate_modifier=-0.10,
        price_shock=-0.15, vol_spike=3.5, vol_duration=100, spread_spike=1.5,
        liquidity_drought=30, funding_shock=-0.001, recovery_drift=0.0002,
        lesson="Structural sellers can overwhelm new demand."),

    UnifiedEvent("Bitcoin ATH 73k March 2024", "2024-03-14",
        "Fastest ATH recovery in history — ETF regime change",
        MarketRegime.BULL,
        duration_days=14, price_impact=0.08, volatility_multiplier=4.0,
        win_rate_modifier=0.15,
        price_shock=0.08, vol_spike=4.0, vol_duration=50, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0002,
        lesson="Post-ETF regime: institutions absorb corrections faster."),

    UnifiedEvent("Bitcoin Halving 2024", "2024-04-20",
        "Block reward 6.25 → 3.125 BTC",
        MarketRegime.BULL,
        duration_days=30, price_impact=0.04, volatility_multiplier=3.0,
        win_rate_modifier=0.08,
        price_shock=0.04, vol_spike=3.0, vol_duration=60, spread_spike=0.9,
        liquidity_drought=0, funding_shock=0.0003, recovery_drift=0.0005,
        lesson="Halvings are supply shocks that take months to manifest."),

    UnifiedEvent("Jump Trading Exit Cascade", "2024-08",
        "Market maker exits — sudden -20% in 2 hours",
        MarketRegime.CRASH,
        duration_days=2, price_impact=-0.22, volatility_multiplier=8.0,
        win_rate_modifier=-0.25,
        price_shock=-0.22, vol_spike=8.0, vol_duration=80, spread_spike=5.0,
        liquidity_drought=60, funding_shock=-0.003, recovery_drift=0.0005,
        lesson="Market makers are not permanent. Their exit is catastrophic."),

    UnifiedEvent("Yen Carry Trade Unwind", "2024-08-05",
        "Global macro shock. BTC -25% in hours.",
        MarketRegime.CRASH,
        duration_days=3, price_impact=-0.28, volatility_multiplier=10.0,
        win_rate_modifier=-0.30,
        price_shock=-0.28, vol_spike=10.0, vol_duration=120, spread_spike=6.0,
        liquidity_drought=80, funding_shock=-0.004, recovery_drift=0.0006,
        lesson="Macro is crypto's shadow. The yen can crash your altcoin."),

    # ══ SYNTETYCZNE EKSTREMA ══════════════════════════════════════════════════

    UnifiedEvent("Exchange API Failure Mid-Trade", "SYNTHETIC",
        "API goes down while in open position",
        MarketRegime.EXTREME_VOLATILITY,
        duration_days=1, price_impact=0.0, volatility_multiplier=2.0,
        win_rate_modifier=-0.15,
        price_shock=0.0, vol_spike=2.0, vol_duration=20, spread_spike=5.0,
        liquidity_drought=30, funding_shock=0.0, recovery_drift=0.0,
        lesson="Infrastructure risk. Never size beyond manual exit capacity."),

    UnifiedEvent("Coordinated Whale Short Attack", "SYNTHETIC",
        "3 whales coordinate -15% to liquidate leveraged longs",
        MarketRegime.CRASH,
        duration_days=1, price_impact=-0.18, volatility_multiplier=6.0,
        win_rate_modifier=-0.20,
        price_shock=-0.18, vol_spike=6.0, vol_duration=60, spread_spike=3.0,
        liquidity_drought=40, funding_shock=-0.002, recovery_drift=0.0008,
        lesson="Market can stay irrational longer than you stay solvent."),

    UnifiedEvent("Total Market Halt 6h", "SYNTHETIC",
        "Exchange halts all trading for 6 hours",
        MarketRegime.LIQUIDITY_CRISIS,
        duration_days=1, price_impact=-0.05, volatility_multiplier=0.0,
        win_rate_modifier=-0.30,
        price_shock=-0.05, vol_spike=0.0, vol_duration=360, spread_spike=100.0,
        liquidity_drought=360, funding_shock=0.0, recovery_drift=0.0001,
        lesson="Sometimes the answer is: you cannot act. Prepare for that."),
]


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 3: SYMULATOR TICKÓW (z ultimate_training — tick-level GARCH)
# ══════════════════════════════════════════════════════════════════════════════

class TickSimulator:
    """
    Generuje tick-by-tick ceny z GARCH(1,1) + procesy skokowe.
    Zasilany przez reżimy z REGIME_CONFIGS.
    """
    TICKS_PER_DAY = 288   # 1 tick = 5 minut

    def __init__(self, start_price: float, regime_key: str = "BULL"):
        self.price        = start_price
        self.regime_key   = regime_key
        self.cfg          = REGIME_CONFIGS[regime_key]
        self.current_vol  = self.cfg.volatility
        self.tick_idx     = 0
        self.price_history: deque = deque(maxlen=300)
        self.return_history: deque = deque(maxlen=100)
        self.price_history.append(start_price)

    def set_regime(self, regime_key: str):
        self.regime_key = regime_key
        self.cfg        = REGIME_CONFIGS.get(regime_key, REGIME_CONFIGS["SIDEWAYS_TIGHT"])

    def apply_event_shock(self, event: UnifiedEvent):
        self.price        = max(0.001, self.price * (1 + event.price_shock))
        self.current_vol *= event.vol_spike
        self.price_history.append(self.price)

    def next_tick(self) -> Tuple[float, float]:
        """Generuj następny tick. Zwraca (price, return)."""
        cfg = self.cfg
        # GARCH volatility update
        last_ret = self.return_history[-1] if self.return_history else 0.0
        omega  = cfg.volatility ** 2 * 0.05
        self.current_vol = math.sqrt(
            max(1e-8, omega + 0.10 * last_ret**2 + 0.85 * self.current_vol**2)
        )
        self.current_vol *= max(0.1, 1.0 + random.gauss(0, cfg.vol_of_vol * 0.1))
        self.current_vol  = max(0.0001, min(0.05, self.current_vol))

        # Return z autokorelacją + drift + jump
        ret = (cfg.drift_per_tick
               + cfg.autocorr * last_ret
               + random.gauss(0, self.current_vol))
        if random.random() < cfg.jump_prob:
            ret += random.gauss(cfg.jump_size_mean, cfg.jump_size_std)
        ret = max(-0.30, min(0.30, ret))

        self.price = max(0.01, self.price * (1 + ret))
        self.price_history.append(self.price)
        self.return_history.append(ret)
        self.tick_idx += 1
        return self.price, ret

    def build_rl_state(self, pos_size=0.0, upnl=0.0, wr=0.5, dd=0.0) -> "RLState":
        """Buduj stan RL z historii cen."""
        if not RL_AVAILABLE:
            return None
        prices = list(self.price_history)
        if len(prices) < 10:
            return RLState()
        p = np.array(prices, dtype=float)
        n = len(p)
        def pct(a, b): return (a - b) / b if b != 0 else 0.0
        def rsi(period):
            if n < period + 1: return 0.5
            d = np.diff(p[-(period+1):])
            g = float(d[d>0].mean()) if (d>0).any() else 0
            l = float(-d[d<0].mean()) if (d<0).any() else 1e-10
            return g/(g+l)
        cfg = self.cfg
        return RLState(
            price_change_1m  = pct(p[-1], p[-2]) if n >= 2 else 0,
            price_change_5m  = pct(p[-1], p[-6]) if n >= 6 else 0,
            price_change_15m = pct(p[-1], p[-16]) if n >= 16 else 0,
            price_change_1h  = pct(p[-1], p[-61]) if n >= 61 else 0,
            volatility_1m    = float(np.std(p[-5:]) / max(p[-1], 1e-10)) if n >= 5 else 0,
            volatility_5m    = float(np.std(p[-30:]) / max(p[-1], 1e-10)) if n >= 30 else 0,
            high_low_ratio   = float(np.clip((p[-1] - p[-n:].min()) / (p[-n:].max() - p[-n:].min() + 1e-10), 0, 1)),
            price_vs_vwap    = pct(p[-1], float(p[-min(60, n):].mean())),
            rsi_14           = rsi(14), rsi_5=rsi(5),
            macd_signal      = float(np.tanh(pct(p[-1], p[-9:].mean()) * 20)) if n >= 9 else 0,
            bb_position      = float(np.clip((p[-1] - p[-20:].mean()) / (p[-20:].std() * 2 + 1e-10) * 0.5 + 0.5, 0, 1)) if n >= 20 else 0.5,
            ema_cross        = pct(float(p[-5:].mean()), float(p[-20:].mean())) if n >= 20 else 0,
            momentum_score   = float(np.sign(np.mean(np.diff(p[-6:])))) if n >= 7 else 0,
            trend_strength   = float(min(abs(pct(p[-1], p[-30:][:1][0])) * 5, 1.0)) if n >= 30 else 0,
            ob_imbalance     = float(np.clip(cfg.ob_imbalance_bias + random.gauss(0, 0.1), -1, 1)),
            spread_pct       = float(random.gauss(0.0008, 0.0002) * cfg.spread_mult),
            volume_spike     = float(min(abs(random.gauss(0, 0.5)), 3.0)),
            trade_flow       = float(np.clip(cfg.ob_imbalance_bias + random.gauss(0, 0.2), -1, 1)),
            regime_bull      = 1.0 if self.regime_key in ("BULL", "PARABOLIC", "STEALTH") else 0.0,
            regime_bear      = 1.0 if self.regime_key in ("BEAR", "CAPITULATION", "FLASH_CRASH") else 0.0,
            regime_ranging   = 1.0 if self.regime_key in ("SIDEWAYS_TIGHT", "SIDEWAYS_WIDE") else 0.0,
            regime_volatile  = 1.0 if self.regime_key in ("FLASH_CRASH", "LIQUIDITY_HUNT", "MANIPULATED") else 0.0,
            funding_rate     = cfg.funding_rate,
            position_size    = pos_size, unrealized_pnl=upnl,
            win_rate_recent  = wr, drawdown=dd,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 4: DANE BOT-LEVEL (z oryginalnego stress_test_10y)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    day:          int
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl_percent:  float
    pnl_usd:      float
    capital_after:float
    regime:       str
    event:        str


@dataclass
class BotStats:
    bot_id:          int
    pair:            str
    capital:         float
    initial_capital: float
    wins:            int   = 0
    losses:          int   = 0
    trades:          int   = 0
    peak_capital:    float = 0
    rl_wins:         int   = 0      # dodane: ile wygrał dzięki RL
    rl_losses:       int   = 0

    def __post_init__(self):
        self.peak_capital = self.capital

    @property
    def win_rate(self): return self.wins / max(self.wins + self.losses, 1)
    @property
    def rl_contribution(self): return (self.rl_wins - self.rl_losses) / max(self.trades, 1)


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 5: GŁÓWNY SYMULATOR (połączony)
#  Dzienny cykl ze stress_test + tick training z ultimate_training
# ══════════════════════════════════════════════════════════════════════════════

PAIRS = [
    "PI_XBTUSD", "PI_ETHUSD", "PI_XRPUSD", "PI_LTCUSD", "PI_BCHUSD",
    "PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD", "PF_AVAXUSD", "PF_DOTUSD",
    "PF_ADAUSD", "PF_ATOMUSD", "PF_LINKUSD", "PF_UNIUSD", "PF_XRPUSD",
    "PF_LTCUSD", "PF_BCHUSD", "PF_EOSUSD",
]

PAIR_START_PRICES = {
    "PI_XBTUSD": 8000.0,  "PI_ETHUSD": 180.0,   "PI_XRPUSD": 0.20,
    "PI_LTCUSD": 60.0,    "PI_BCHUSD": 250.0,   "PF_XBTUSD": 8000.0,
    "PF_ETHUSD": 180.0,   "PF_SOLUSD": 1.0,     "PF_AVAXUSD": 3.0,
    "PF_DOTUSD": 4.0,     "PF_ADAUSD": 0.05,    "PF_ATOMUSD": 2.0,
    "PF_LINKUSD": 2.5,    "PF_UNIUSD": 1.0,     "PF_XRPUSD": 0.20,
    "PF_LTCUSD": 60.0,    "PF_BCHUSD": 250.0,   "PF_EOSUSD": 2.5,
}


class UnifiedStressTest:
    """
    Połączony symulator:
    ① Dzienny makro-cykl (100 botów, Kelly sizing, reżimy) — styl KRAKEN
    ② Tick-by-tick GARCH + jumps dla każdego dnia
    ③ RL engine dostaje stan per tick i uczy się online
    ④ Curriculum: 5 poziomów eskalacji trudności
    ⑤ Adversarial: 7 typów pułapek po standardowym treningu
    """

    # ── Konfiguracja ────────────────────────────────────────────────────────────
    BASE_WIN_RATE  = 0.52
    BASE_RR_RATIO  = 1.5
    MAX_RISK_TRADE = 0.02
    MIN_BOT_CAPITAL= 0.001
    TICKS_PER_DAY  = 48       # 1 tick = 30 minut (dla szybkości treningu)
    RL_TRAIN_FREQ  = 4        # trening RL co N ticków

    def __init__(
        self,
        initial_capital: float = 50.0,
        num_bots:        int   = 100,
        leverage:        float = 20.0,
        years:           int   = 10,
        curriculum_level:int   = 3,
        enable_rl:       bool  = True,
    ):
        self.initial_capital  = initial_capital
        self.total_capital    = initial_capital
        self.leverage         = leverage
        self.num_bots         = num_bots
        self.years            = years
        self.curriculum_level = curriculum_level
        self.enable_rl        = enable_rl and RL_AVAILABLE

        # Reżim rynkowy
        self.current_regime     = MarketRegime.SIDEWAYS
        self.current_regime_key = "SIDEWAYS_TIGHT"
        self.current_event: Optional[UnifiedEvent] = None
        self.event_days_left    = 0
        self.win_rate_modifier  = 0.0

        # Boty
        self.bots:        List[BotStats] = []
        self.trades:      List[Trade]    = []
        self.daily_equity: List[float]   = []
        self.events_log:  List[Tuple[int, UnifiedEvent]] = []

        # Statistyki
        self.total_wins   = 0
        self.total_losses = 0
        self.peak_equity  = initial_capital
        self.max_drawdown = 0.0
        self.yearly_returns: List[float] = []

        # RL orchestrators per para (shared dla wszystkich botów na danej parze)
        self.rl_orchs: Dict[str, Any] = {}
        self._rl_reward_buf: Dict[str, List[float]] = defaultdict(list)

        # Tick simulators per para
        self.tick_sims: Dict[str, TickSimulator] = {}

        # Statystyki RL
        self.rl_correct = 0
        self.rl_total   = 0

        self._init_bots()
        self._init_tick_sims()

    def _init_bots(self):
        cap_per_bot = self.initial_capital / self.num_bots
        for i in range(self.num_bots):
            pair = PAIRS[i % len(PAIRS)]
            self.bots.append(BotStats(
                bot_id=i, pair=pair,
                capital=cap_per_bot, initial_capital=cap_per_bot,
            ))

    def _init_tick_sims(self):
        for pair in set(b.pair for b in self.bots):
            sp = PAIR_START_PRICES.get(pair, 100.0)
            self.tick_sims[pair] = TickSimulator(sp, "SIDEWAYS_TIGHT")

    def _get_rl(self, pair: str):
        if not self.enable_rl: return None
        if pair not in self.rl_orchs:
            self.rl_orchs[pair] = get_rl_orchestrator(pair)
        return self.rl_orchs[pair]

    # ── Pozycjonowanie (Kelly) ─────────────────────────────────────────────────

    def _kelly_size(self, bot: BotStats) -> float:
        if bot.capital < self.MIN_BOT_CAPITAL: return 0.0
        wr    = max(0.30, min(0.75, self.BASE_WIN_RATE + self.win_rate_modifier))
        kelly = max(0.0, min(0.25,
            (wr * self.BASE_RR_RATIO - (1 - wr)) / self.BASE_RR_RATIO))
        # Curriculum skaluje agresywność
        kelly *= (0.5 + self.curriculum_level * 0.12)
        # Reżim-aware
        mult = {
            MarketRegime.CRASH:              0.25,
            MarketRegime.LIQUIDITY_CRISIS:   0.20,
            MarketRegime.EXTREME_VOLATILITY: 0.45,
            MarketRegime.BEAR:               0.65,
        }.get(self.current_regime, 1.0)
        return bot.capital * kelly * mult

    # ── Decyzja o handlu ──────────────────────────────────────────────────────

    def _should_trade(self, bot: BotStats) -> Tuple[bool, str]:
        if bot.capital < self.MIN_BOT_CAPITAL: return False, ""
        cfg     = REGIME_CONFIGS.get(self.current_regime_key, REGIME_CONFIGS["SIDEWAYS_TIGHT"])
        prob    = cfg.trade_prob
        # Curriculum boost
        prob   *= (0.6 + self.curriculum_level * 0.12)
        if random.random() > prob: return False, ""
        # Kierunek z reżimu
        r = self.current_regime
        if r == MarketRegime.BULL:
            direction = "LONG" if random.random() < 0.75 else "SHORT"
        elif r == MarketRegime.BEAR:
            direction = "SHORT" if random.random() < 0.70 else "LONG"
        elif r == MarketRegime.CRASH:
            direction = "SHORT" if random.random() < 0.85 else "LONG"
        elif r == MarketRegime.RECOVERY:
            direction = "LONG" if random.random() < 0.65 else "SHORT"
        else:
            direction = random.choice(["LONG", "SHORT"])
        return True, direction

    # ── Tick training (RL) ────────────────────────────────────────────────────

    def _run_tick_training(self, pair: str, n_ticks: int):
        """
        n_ticks ticków GARCH dla danej pary.
        RL engine dostaje stan i uczy się z wyników.
        """
        sim = self.tick_sims.get(pair)
        rl  = self._get_rl(pair)
        if not sim or not rl: return

        last_state = None
        last_action = None

        for t in range(n_ticks):
            price, ret = sim.next_tick()

            state = sim.build_rl_state(
                wr=sum(b.win_rate for b in self.bots if b.pair == pair) /
                   max(sum(1 for b in self.bots if b.pair == pair), 1)
            )
            if state is None: continue

            # Pobierz decyzję RL
            try:
                decision = rl.decide(state)
            except Exception:
                continue

            # Nagroda: czy RL przewidział kierunek ruchu?
            if decision['direction'] != 'hold':
                predicted_up   = decision['direction'] == 'buy'
                actually_up    = ret > 0
                correct        = predicted_up == actually_up
                reward_magnitude = abs(ret) * 100  # skala do 0-30
                reward = reward_magnitude if correct else -reward_magnitude * 0.5

                self.rl_correct += int(correct)
                self.rl_total   += 1

                try:
                    rl.record_outcome(reward, ret, state)
                except Exception:
                    pass

    # ── Wykonanie transakcji ──────────────────────────────────────────────────

    def _execute_trade(self, bot: BotStats, direction: str, day: int,
                       rl_override: Optional[str] = None) -> Optional[Trade]:
        pos_size = self._kelly_size(bot)
        if pos_size < 0.001: return None

        # Oblicz WR
        wr_base = self.BASE_WIN_RATE + self.win_rate_modifier
        # Bonus za aligned kierunek z reżimem
        if self.current_regime == MarketRegime.BULL    and direction == "LONG":  wr_base += 0.08
        if self.current_regime == MarketRegime.BEAR    and direction == "SHORT": wr_base += 0.08
        if self.current_regime == MarketRegime.CRASH   and direction == "SHORT": wr_base += 0.10
        if self.current_regime == MarketRegime.RECOVERY and direction == "LONG": wr_base += 0.06
        # Curriculum zwiększa base WR (system staje się lepszy)
        wr_base += self.curriculum_level * 0.015
        win_rate = max(0.25, min(0.80, wr_base))

        is_win  = random.random() < win_rate

        if is_win:
            pnl_pct = random.uniform(0.01, 0.03) * self.leverage
            pnl_pct = min(pnl_pct, 0.50)
        else:
            pnl_pct = -random.uniform(0.005, 0.02) * self.leverage
            pnl_pct = max(pnl_pct, -0.30)

        pnl_usd = max(-pos_size, pos_size * pnl_pct)
        bot.capital     = max(0, bot.capital + pnl_usd)
        bot.trades     += 1
        if pnl_usd > 0:
            bot.wins       += 1; self.total_wins   += 1
            if rl_override: bot.rl_wins  += 1
        else:
            bot.losses     += 1; self.total_losses += 1
            if rl_override: bot.rl_losses += 1
        if bot.capital > bot.peak_capital:
            bot.peak_capital = bot.capital

        return Trade(
            day=day, direction=direction,
            entry_price=100.0,
            exit_price=100.0 * (1 + pnl_pct / self.leverage),
            pnl_percent=pnl_pct * 100, pnl_usd=pnl_usd,
            capital_after=bot.capital,
            regime=self.current_regime.value,
            event=self.current_event.name if self.current_event else "Normal",
        )

    # ── Aktualizacja warunków rynkowych ────────────────────────────────────────

    def _update_market(self, day: int):
        # Trwa event?
        if self.event_days_left > 0:
            self.event_days_left -= 1
            if self.event_days_left == 0:
                self.current_event      = None
                self.current_regime     = MarketRegime.SIDEWAYS
                self.current_regime_key = "SIDEWAYS_TIGHT"
                self.win_rate_modifier  = 0.0
            return

        # Curriculum skaluje częstość eventów
        base_prob = 0.015 + 0.005 * self.curriculum_level
        if random.random() < base_prob:
            event = random.choice(ALL_EVENTS)
            self.current_event      = event
            self.current_regime     = event.macro_regime
            self.current_regime_key = MACRO_TO_TICK_REGIME.get(event.macro_regime, "SIDEWAYS_TIGHT")
            self.event_days_left    = event.duration_days
            self.win_rate_modifier  = event.win_rate_modifier
            self.events_log.append((day, event))
            # Aplukuj szok do tick simulatorów
            for sim in self.tick_sims.values():
                sim.set_regime(self.current_regime_key)
                sim.apply_event_shock(event)

    # ── Symulacja jednego dnia ─────────────────────────────────────────────────

    def _simulate_day(self, day: int):
        self._update_market(day)

        # 1) Tick training RL
        pairs_today = list(set(b.pair for b in self.bots))
        for pair in pairs_today:
            self._run_tick_training(pair, self.TICKS_PER_DAY)

        # 2) Dzienny handel botów
        for bot in self.bots:
            n_trades = random.randint(0, 3)
            for _ in range(n_trades):
                do_trade, direction = self._should_trade(bot)
                if not do_trade: continue
                trade = self._execute_trade(bot, direction, day)
                if trade:
                    self.trades.append(trade)

        # 3) Aktualizacja kapitału
        self.total_capital  = sum(b.capital for b in self.bots)
        self.daily_equity.append(self.total_capital)
        if self.total_capital > self.peak_equity:
            self.peak_equity = self.total_capital
        dd = (self.peak_equity - self.total_capital) / max(self.peak_equity, 1)
        if dd > self.max_drawdown:
            self.max_drawdown = dd

    # ══ RUN ═══════════════════════════════════════════════════════════════════

    def run(self):
        total_days    = self.years * 365
        year_start    = self.total_capital
        level_name    = ["AWAKENING","BAPTISM","GRINDER","INFERNO","IMPOSSIBLE"][
                         max(0, min(4, self.curriculum_level - 1))]

        print("\n" + "🦑" * 40)
        print("★ KR √¡\\ K — ULTIMATE TRAINING GAUNTLET v3.0")
        print("10-Year Extreme Stress Test × Tick RL Training")
        print("🦑" * 40)
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PARAMETRY TRENINGU                                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  💰 Kapital startowy:   ${self.initial_capital:>10.2f}                                     ║
║  🎯 Cel:                $550.00 (11x wzrost)                                ║
║  ⚡ Dzwignia:           {self.leverage:>10.0f}x                                       ║
║  🤖 Aktywne boty:       {self.num_bots:>10}                                       ║
║  📅 Czas trwania:       {self.years:>10} lat ({total_days} dni)                    ║
║  🛡️  Maks. ryzyko/trade:{self.MAX_RISK_TRADE*100:>10.0f}%                                       ║
║  🧠 Silniki RL:         {RL_ENGINES:>10} ({CLUSTER_VERSION_STR})              ║
║  📚 Poziom curriculum:  {self.curriculum_level:>9}  ({level_name})                      ║
║  📊 Baza eventow:       {len(ALL_EVENTS):>10} zdarzen historycznych                  ║
║  🌪️  Rezimy rynkowe:     {len(REGIME_CONFIGS):>10}                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

        print("Uruchamiam symulację...\n" + "─" * 80)

        for day in range(total_days):
            self._simulate_day(day)

            # Progress bar co 100 dni
            if (day + 1) % 100 == 0:
                pct   = (day + 1) / total_days * 100
                bar   = "█" * int(40 * pct / 100) + "░" * (40 - int(40 * pct / 100))
                alive = len([b for b in self.bots if b.capital > self.MIN_BOT_CAPITAL])
                rl_acc = f"{self.rl_correct/max(self.rl_total,1)*100:.1f}%" if self.rl_total > 0 else "N/A"
                print(f"\r[{bar}] {pct:.0f}% │ D{day+1} │ ${self.total_capital:.2f} │ "
                      f"{alive}bots │ RL:{rl_acc}", end="", flush=True)

            # Roczne podsumowanie
            if (day + 1) % 365 == 0:
                yr  = (day + 1) // 365
                ret = ((self.total_capital - year_start) / year_start * 100) if year_start > 0 else 0
                self.yearly_returns.append(ret)
                emoji = "✅" if ret > 0 else "❌"
                print(f"\n{emoji} Rok {yr} ({2015+yr}): ${self.total_capital:.2f} │ "
                      f"{ret:+.1f}% │ {self.current_regime.value}")
                year_start = self.total_capital

        print("\n" + "=" * 80)
        # Zapisz modele RL
        if self.enable_rl:
            save_all_models()
            logger.info("💾 Modele RL zapisane.")
        self._print_results()

    def _print_results(self):
        total_ret   = (self.total_capital - self.initial_capital) / self.initial_capital * 100
        total_tr    = len(self.trades)
        wr          = (self.total_wins / total_tr * 100) if total_tr > 0 else 0
        wins_usd    = [t.pnl_usd for t in self.trades if t.pnl_usd > 0]
        losses_usd  = [t.pnl_usd for t in self.trades if t.pnl_usd <= 0]
        avg_win     = sum(wins_usd) / len(wins_usd) if wins_usd else 0
        avg_loss    = sum(losses_usd) / len(losses_usd) if losses_usd else 0
        pf          = sum(wins_usd) / abs(sum(losses_usd)) if losses_usd else float('inf')
        alive       = len([b for b in self.bots if b.capital > self.MIN_BOT_CAPITAL])
        profitable  = len([b for b in self.bots if b.capital > b.initial_capital])
        rl_acc_str  = f"{self.rl_correct/max(self.rl_total,1)*100:.1f}%" if self.rl_total > 0 else "N/A"
        rl_samples  = f"{self.rl_total:,}"

        event_counts: Dict[str, int] = {}
        for _, ev in self.events_log:
            event_counts[ev.name] = event_counts.get(ev.name, 0) + 1

        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          ★ KR √¡\\ K — WYNIKI TRENINGU GAUNTLET v3.0                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  💰 WYNIKI KAPITAŁOWE                                                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Kapital startowy:      ${self.initial_capital:>10.2f}                                     ║
║  Kapital końcowy:       ${self.total_capital:>10.2f}                                     ║
║  Calkowity zwrot:       {total_ret:>+10.1f}%                                     ║
║  Szczyt equity:         ${self.peak_equity:>10.2f}                                     ║
║  Maks. drawdown:        {self.max_drawdown*100:>10.1f}%                                     ║
║  Mnoznik wzrostu:       {self.total_capital/self.initial_capital:>10.1f}x                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📊 STATYSTYKI HANDLU                                                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Łaczna liczba tradow:  {total_tr:>10,}                                     ║
║  Wygrane:               {self.total_wins:>10,}                                     ║
║  Przegrane:             {self.total_losses:>10,}                                     ║
║  Win rate:              {wr:>10.1f}%                                     ║
║  Śr. wygrana:           ${avg_win:>10.4f}                                     ║
║  Śr. strata:            ${avg_loss:>10.4f}                                     ║
║  Profit factor:         {pf:>10.2f}                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🧠 STATYSTYKI RL                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Silniki RL:            {RL_ENGINES:>10}                                       ║
║  Próbek treningowych:   {rl_samples:>10}                                     ║
║  Dokładność RL:         {rl_acc_str:>10}                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🤖 STATYSTYKI BOTÓW                                                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Wszystkie boty:        {self.num_bots:>10}                                       ║
║  Aktywne boty:          {alive:>10}                                       ║
║  Zyskowne boty:         {profitable:>10}                                       ║
║  Wskaźnik przeżycia:    {alive/self.num_bots*100:>10.1f}%                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🌪️  PRZEŻYTE ZDARZENIA ({len(self.events_log)} total)                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣""")

        for name, cnt in sorted(event_counts.items(), key=lambda x: -x[1])[:6]:
            print(f"║  • {name[:36]:<36} {cnt:>5}x                          ║")

        print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  📈 WYNIKI ROCZNE                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣""")

        cumulative = self.initial_capital
        for i, ret in enumerate(self.yearly_returns):
            cumulative *= (1 + ret / 100)
            emoji = "✅" if ret > 0 else "❌"
            print(f"║  {emoji} Rok {i+1:>2} ({2016+i}):  {ret:>+8.1f}%  |  ${cumulative:>10.2f}                    ║")

        print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  🎯 WERDYKT KOŃCOWY                                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣""")

        target = 550.0
        if self.total_capital >= target:
            print(f"║  ✅ CEL OSIĄGNIĘTY! ${self.initial_capital:.0f} → ${self.total_capital:.2f} ({total_ret:.0f}% zwrotu!)        ║")
            print(f"║  🏆 System PRZEŻYŁ {self.years} lat ekstremalnych warunków!                    ║")
        elif self.total_capital > self.initial_capital:
            print(f"║  ⚠️  PRZEŻYŁ z zyskiem — cel nie osiągnięty                                ║")
            print(f"║  📊 ${self.initial_capital:.0f} → ${self.total_capital:.2f} ({total_ret:+.1f}%) przez {self.years} lat              ║")
        elif self.total_capital > 0:
            print(f"║  ⚠️  PRZEŻYŁ — ze stratą                                                   ║")
            print(f"║  📉 ${self.initial_capital:.0f} → ${self.total_capital:.2f} ({total_ret:.1f}%)                                    ║")
        else:
            print(f"║  💀 BANKRUCTWO — kapitał wyczerpany                                        ║")

        # RL ranking jeśli dostępny
        if self.enable_rl and self.rl_orchs:
            print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  🌌 RANKING SILNIKÓW RL                                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
            try:
                stats = get_global_rl_stats()
                for pd in stats.get("per_pair", [])[:3]:
                    for es in pd.get("engines", [])[:5]:
                        ename = es.get("engine", "?")[:20]
                        ewr   = es.get("win_rate", 0)
                        bar   = "█" * int(ewr * 20)
                        print(f"║  {ename:<20} WR={ewr:.3f} {bar:<20}                      ║")
            except Exception:
                pass

        print("╚══════════════════════════════════════════════════════════════════════════════╝")
        self._print_equity_curve()

    def _print_equity_curve(self):
        if len(self.daily_equity) < 2: return
        print("\n🦑 KRZYWA EQUITY (10 lat):\n")
        monthly = [self.daily_equity[i] for i in range(0, len(self.daily_equity), 30)]
        height  = 12
        width   = min(70, len(monthly))
        step    = len(monthly) / max(width, 1)
        samples = [monthly[int(i * step)] for i in range(width)]
        min_v   = max(0, min(samples) * 0.9)
        max_v   = max(samples) * 1.1
        rng     = max_v - min_v if max_v != min_v else 1
        chart   = [[' '] * width for _ in range(height)]
        for x, val in enumerate(samples):
            y = int((val - min_v) / rng * (height - 1))
            chart[height - 1 - max(0, min(height - 1, y))][x] = '█'
        print(f"${max_v:>7.0f} ┤")
        for row in chart:
            print("         │" + ''.join(row))
        print(f"${min_v:>7.0f} ┤" + "─" * width)
        print("          2016" + " " * max(0, width - 15) + "2025")


# ── Helper string ──────────────────────────────────────────────────────────────
CLUSTER_VERSION_STR = f"{RL_ENGINES}-engine cluster" if RL_AVAILABLE else "RL niedostepny"


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 6: ADVERSARIAL TRAINER (z ultimate_training)
# ══════════════════════════════════════════════════════════════════════════════

class AdversarialTrainer:
    """
    7 typów złośliwych pułapek dla silników RL.
    Uruchamiany PO standardowym treningu — hardening phase.
    """
    TRAP_TYPES = ["bull_trap","bear_trap","stop_hunt",
                  "wash_trade","spoof_wall","news_reversal","liquidity_vacuum"]

    def __init__(self):
        self.traps_set    = 0
        self.traps_evaded = 0

    def _gen_sequence(self, base: float = 1000.0, n: int = 100,
                      trap: str = "bull_trap") -> List[Tuple["RLState", float]]:
        seq   = []
        price = base
        prices = [price]

        for t in range(n):
            ph = t / n
            if trap == "bull_trap":
                drift    = 0.0008 if ph < 0.6 else -0.003
                ob_bias  = 0.5    if ph < 0.6 else -0.7
                rsi_val  = 0.72   if ph < 0.6 else 0.35
                mom      = 0.7    if ph < 0.6 else -0.8
            elif trap == "bear_trap":
                drift    = -0.0008 if ph < 0.6 else 0.003
                ob_bias  = -0.5   if ph < 0.6 else 0.7
                rsi_val  = 0.28   if ph < 0.6 else 0.65
                mom      = -0.7   if ph < 0.6 else 0.8
            elif trap == "stop_hunt":
                drift    = 0.001  if ph < 0.3 else (0.005 if ph < 0.35 else -0.002)
                ob_bias  = 0.3    if ph < 0.3 else (0.8   if ph < 0.35 else -0.6)
                rsi_val  = 0.55; mom = 0.0
            elif trap == "wash_trade":
                drift    = random.choice([-1,1]) * 0.0001
                ob_bias  = random.uniform(-0.8, 0.8)
                rsi_val  = 0.5; mom = 0.0
                if ph > 0.85:
                    drift    = random.choice([-1,1]) * 0.003
                    ob_bias  = math.copysign(0.7, drift)
            elif trap == "spoof_wall":
                drift    = 0.0002 if ph < 0.5 else (-0.0001 if ph < 0.55 else -0.002)
                ob_bias  = 0.8    if ph < 0.5 else (-0.9   if ph < 0.55 else -0.7)
                rsi_val  = 0.58; mom = 0.0
            elif trap == "news_reversal":
                drift    = 0.0005 if ph < 0.7 else -0.005
                ob_bias  = 0.4    if ph < 0.7 else -0.9
                rsi_val  = 0.65   if ph < 0.7 else 0.30
                mom      = 0.5    if ph < 0.7 else -0.9
            else:  # liquidity_vacuum
                drift    = random.gauss(0, 0.005)
                ob_bias  = 0.0; rsi_val = 0.5; mom = 0.0

            ret    = drift + random.gauss(0, 0.002)
            price  = max(0.001, price * (1 + ret))
            prices.append(price)

            if RL_AVAILABLE:
                state = RLState(
                    price_change_1m = ret,
                    price_change_5m = (price/prices[max(0,len(prices)-6)]-1) if len(prices)>5 else ret,
                    rsi_14=rsi_val, rsi_5=rsi_val*1.05,
                    bb_position=0.5+ob_bias*0.3, ob_imbalance=ob_bias,
                    momentum_score=mom, trade_flow=ob_bias*0.8,
                    ema_cross=drift*100, volatility_1m=abs(ret),
                    regime_bull=1.0 if drift>0.0003 else 0.0,
                    regime_bear=1.0 if drift<-0.0003 else 0.0,
                )
                actual_chg = (prices[-1]-prices[-2])/prices[-2] if len(prices)>=2 else 0
                seq.append((state, actual_chg * 100))
        return seq

    def run(self, rl_orchs: Dict[str, Any], n_sequences: int = 300):
        if not RL_AVAILABLE or not rl_orchs:
            logger.info("  Adversarial: brak silników RL — pominięte")
            return

        logger.info(f"\n  🎭 ADVERSARIAL TRAINING — {n_sequences} sekwencji pułapek")
        logger.info("  Rynek będzie kłamał. Silne przeżyją.")

        evaded = 0; total = 0
        for i in range(n_sequences):
            pair  = random.choice(list(rl_orchs.keys()))
            trap  = random.choice(self.TRAP_TYPES)
            orch  = rl_orchs[pair]
            seq   = self._gen_sequence(1000.0, 60, trap)

            for t, (state, true_reward) in enumerate(seq):
                dec = orch.decide(state)
                if dec['direction'] != 'hold':
                    sd  = 1 if dec['direction'] == 'buy' else -1
                    if sd * true_reward >= 0 or abs(true_reward) < 0.05:
                        evaded += 1
                    total += 1
                ns = seq[t+1][0] if t+1 < len(seq) else state
                try:
                    orch.record_outcome(true_reward, true_reward*0.01, ns)
                except Exception:
                    pass

            if (i+1) % 100 == 0:
                er = evaded / max(total, 1) * 100
                logger.info(f"  Adversarial: {i+1}/{n_sequences} | Evasion: {er:.1f}%")

        final_er = evaded / max(total, 1) * 100
        logger.info(f"  ✅ Adversarial zakończony: evasion rate = {final_er:.1f}%")
        self.traps_set    = n_sequences
        self.traps_evaded = evaded


# ══════════════════════════════════════════════════════════════════════════════
#  SEKCJA 7: CURRICULUM RUNNER
#  5 poziomów eskalacji połączone ze stresem dziennym
# ══════════════════════════════════════════════════════════════════════════════

CURRICULUM = [
    {"name": "LEVEL 1 — AWAKENING",    "years": 2, "bots": 20,  "lvl": 1,
     "desc": "Normalny rynek. Ucz się chodzić."},
    {"name": "LEVEL 2 — BAPTISM",      "years": 3, "bots": 40,  "lvl": 2,
     "desc": "Prawdziwa zmienność. Pierwsze straty."},
    {"name": "LEVEL 3 — THE GRINDER",  "years": 5, "bots": 60,  "lvl": 3,
     "desc": "Wszystkie reżimy. Połowa eventów."},
    {"name": "LEVEL 4 — THE INFERNO",  "years": 8, "bots": 80,  "lvl": 4,
     "desc": "LUNA, FTX, COVID. Pełna baza eventów."},
    {"name": "LEVEL 5 — THE IMPOSSIBLE","years": 10,"bots": 100, "lvl": 5,
     "desc": "Wszystko jednocześnie. Rynek chce cię zniszczyć."},
]


def run_curriculum(capital: float = 50.0, leverage: float = 20.0,
                   levels: List[int] = None, adversarial: bool = True,
                   fast: bool = False):
    """Uruchom pełny curriculum."""
    selected = [CURRICULUM[i-1] for i in (levels or range(1, 6))
                if 1 <= i <= 5]

    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  ★ KR √¡\\ K — CURRICULUM GAUNTLET START                                      ║
║  {len(selected)} poziomów | {sum(c['years'] for c in selected)} lat łącznie | {RL_ENGINES} silników RL   ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

    all_rl_orchs: Dict[str, Any] = {}
    results = []

    for cfg in selected:
        print(f"\n{'═'*78}")
        print(f"  {cfg['name']}")
        print(f"  {cfg['desc']}")
        print(f"  {cfg['years']} lat | {cfg['bots']} botów | curriculum_level={cfg['lvl']}")
        print(f"{'═'*78}")

        years_run = max(1, cfg['years'] // 5) if fast else cfg['years']
        sim = UnifiedStressTest(
            initial_capital  = capital,
            num_bots         = max(10, cfg['bots'] // 5) if fast else cfg['bots'],
            leverage         = leverage,
            years            = years_run,
            curriculum_level = cfg['lvl'],
        )
        sim.run()
        all_rl_orchs.update(sim.rl_orchs)
        results.append({
            "level":     cfg['name'],
            "capital":   sim.total_capital,
            "win_rate":  sim.total_wins / max(sim.total_wins + sim.total_losses, 1),
            "drawdown":  sim.max_drawdown,
            "trades":    len(sim.trades),
            "events":    len(sim.events_log),
            "rl_acc":    sim.rl_correct / max(sim.rl_total, 1),
        })
        if RL_AVAILABLE:
            save_all_models()

        # Stop jeśli system kompletnie padł
        if sim.total_capital < 1.0:
            logger.warning("System zbankrutował — zatrzymuję curriculum")
            break

    # Adversarial hardening
    if adversarial and all_rl_orchs:
        adv   = AdversarialTrainer()
        n_adv = 50 if fast else 300
        adv.run(all_rl_orchs, n_adv)
        if RL_AVAILABLE:
            save_all_models()

    # Podsumowanie curriculum
    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PODSUMOWANIE CURRICULUM                                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
    for r in results:
        trend = "▲" if r['capital'] > capital else "▼"
        rl_s  = f"{r['rl_acc']*100:.1f}%"
        print(f"║  {trend} {r['level'][:30]:<30} WR={r['win_rate']*100:.1f}% "
              f"DD={r['drawdown']*100:.1f}% RL={rl_s:>6} ║")

    final = results[-1] if results else {}
    fwr   = final.get('win_rate', 0)
    fdd   = final.get('drawdown', 1)
    if fwr >= 0.60 and fdd < 0.25:
        verdict = "ELITE — Gotowy na live market"
    elif fwr >= 0.52 and fdd < 0.40:
        verdict = "CAPABLE — Paper trading, potem live"
    elif fwr >= 0.45:
        verdict = "LEARNING — Kontynuuj trening"
    else:
        verdict = "NEEDS MORE DATA — Uruchom ponownie"

    print(f"╠══════════════════════════════════════════════════════════════════════════════╣")
    print(f"║  ⚖️  WERDYKT: {verdict:<61} ║")
    print(f"╚══════════════════════════════════════════════════════════════════════════════╝")

    # Zapisz raport
    report = {"curriculum": results, "verdict": verdict,
              "rl_engines": RL_ENGINES, "total_events_library": len(ALL_EVENTS)}
    path = f"training_logs/curriculum_{int(time.time())}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"📄 Raport zapisany: {path}")

    return results


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="★ KRAKEN ULTRA — Ultimate Training Gauntlet v3.0")
    parser.add_argument("--capital",   type=float, default=50.0,
                        help="Kapital startowy (domyslnie $50)")
    parser.add_argument("--leverage",  type=float, default=20.0,
                        help="Dzwignia (domyslnie 20x)")
    parser.add_argument("--levels",    type=str,   default="all",
                        help="Poziomy curriculum: all lub np. '3,4,5'")
    parser.add_argument("--no-adversarial", action="store_true",
                        help="Pominij adversarial training")
    parser.add_argument("--fast",      action="store_true",
                        help="Szybki tryb (skrocone lata i boty)")
    parser.add_argument("--single",    action="store_true",
                        help="Jeden test 10-letni (oryginalny tryb)")
    args = parser.parse_args()

    random.seed(42)

    if args.single:
        # ── Oryginalny tryb stress_test_10y ──────────────────────────────────
        sim = UnifiedStressTest(
            initial_capital  = args.capital,
            num_bots         = 100,
            leverage         = args.leverage,
            years            = 10,
            curriculum_level = 3,
        )
        sim.run()
    else:
        # ── Pełny curriculum gauntlet ─────────────────────────────────────────
        if args.levels == "all":
            lvls = [1, 2, 3, 4, 5]
        else:
            lvls = [int(x) for x in args.levels.split(",")]

        run_curriculum(
            capital     = args.capital,
            leverage    = args.leverage,
            levels      = lvls,
            adversarial = not args.no_adversarial,
            fast        = args.fast,
        )
