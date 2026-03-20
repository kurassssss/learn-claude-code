"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ██████╗  █████╗ ██╗   ██╗███╗   ██╗████████╗██╗     ███████╗████████╗   ║
║  ██╔════╝ ██╔══██╗██║   ██║████╗  ██║╚══██╔══╝██║     ██╔════╝╚══██╔══╝   ║
║  ██║  ███╗███████║██║   ██║██╔██╗ ██║   ██║   ██║     █████╗     ██║       ║
║  ██║   ██║██╔══██║██║   ██║██║╚██╗██║   ██║   ██║     ██╔══╝     ██║       ║
║  ╚██████╔╝██║  ██║╚██████╔╝██║ ╚████║   ██║   ███████╗███████╗   ██║       ║
║   ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚══════╝   ╚═╝       ║
║                                                                              ║
║   KRAKEN ULTRA — ULTIMATE TRAINING GAUNTLET                                ║
║   "If you can survive this, you can survive anything."                      ║
║                                                                              ║
║   ● 5+ years of synthetic market data (2019-2024)                           ║
║   ● 47 categories of extreme market events                                  ║
║   ● 12 pairs trained simultaneously                                         ║
║   ● 15-engine cluster learns from every tick                                ║
║   ● Black swans, flash crashes, exchange hacks, regulatory nukes            ║
║   ● Pump & dump schemes, whale manipulation, coordinated attacks            ║
║   ● Real historical event injection (COVID, Luna, FTX, 3AC, USDC depeg)    ║
║   ● Adversarial noise injection — the market lies to you on purpose         ║
║   ● Regime-switching every episode — no engine can get comfortable          ║
║   ● Curriculum learning: starts manageable, escalates to impossible         ║
║   ● Full performance report + engine intelligence ranking at end            ║
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
from enum import Enum

# ── Setup ──────────────────────────────────────────────────────────────────────
Path("rl_models").mkdir(exist_ok=True)
Path("training_logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s │ %(levelname)-8s │ %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'training_logs/gauntlet_{int(time.time())}.log')
    ]
)
logger = logging.getLogger("GAUNTLET")

# ── Import RL cluster ──────────────────────────────────────────────────────────
try:
    from rl_engines_v2 import (
        get_rl_orchestrator, ExtendedRLOrchestrator,
        RLState, Action, save_all_models, get_global_rl_stats
    )
    CLUSTER_VERSION = 15
    logger.info("🌌 15-Engine Soul Cluster loaded")
except ImportError:
    try:
        from rl_engines import (
            get_rl_orchestrator, RLEngineOrchestrator as ExtendedRLOrchestrator,
            RLState, Action, save_all_models, get_global_rl_stats
        )
        CLUSTER_VERSION = 5
        logger.info("⚡ 5-Engine Cluster loaded (v2 not found)")
    except ImportError:
        logger.error("FATAL: No RL engine module found. Run from KRAKEN ULTRA directory.")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1: MARKET DATA GENERATOR
#  5+ years of synthetic price history for 12 pairs
#  Based on real crypto behavior: fat tails, volatility clustering,
#  autocorrelation, funding cycles, liquidity gaps
# ══════════════════════════════════════════════════════════════════════════════

class MarketRegime(Enum):
    STEALTH_ACCUMULATION = "stealth_accumulation"   # quiet buying, barely visible
    MARKUP               = "markup"                  # sustained trend up
    DISTRIBUTION         = "distribution"            # smart money selling
    MARKDOWN             = "markdown"                # sustained trend down
    RANGING_TIGHT        = "ranging_tight"           # low volatility chop
    RANGING_WIDE         = "ranging_wide"            # high volatility chop
    PARABOLIC            = "parabolic"               # exponential blow-off
    CAPITULATION         = "capitulation"            # panic selling cascade
    DEAD_CAT             = "dead_cat"                # fake recovery
    LIQUIDITY_HUNT       = "liquidity_hunt"          # stop-hunting both ways
    FLASH_CRASH          = "flash_crash"             # instantaneous -20%+
    MANIPULATED          = "manipulated"             # whale games


@dataclass
class RegimeConfig:
    name:               str
    duration_ticks:     Tuple[int, int]    # (min, max) ticks in this regime
    drift_per_tick:     float              # expected return per tick
    volatility:         float              # tick vol
    vol_of_vol:         float              # vol clustering intensity
    jump_prob:          float              # probability of a jump per tick
    jump_size_mean:     float              # mean jump size (can be negative)
    jump_size_std:      float              # jump size randomness
    autocorr:           float             # price autocorrelation (momentum)
    spread_multiplier:  float             # 1=normal, 3=illiquid
    ob_imbalance_bias:  float             # orderbook manipulation direction
    funding_rate:       float             # funding rate (positive=longs pay)
    false_signal_prob:  float             # prob of fake reversal signal


REGIME_CONFIGS: Dict[MarketRegime, RegimeConfig] = {

    MarketRegime.STEALTH_ACCUMULATION: RegimeConfig(
        name="Stealth Accumulation",
        duration_ticks=(200, 800),
        drift_per_tick=0.00003,       # barely positive
        volatility=0.0008,
        vol_of_vol=0.1,
        jump_prob=0.002,
        jump_size_mean=-0.003,        # occasional fake dumps to shake weak hands
        jump_size_std=0.002,
        autocorr=0.05,
        spread_multiplier=1.2,
        ob_imbalance_bias=0.1,        # hidden buy pressure
        funding_rate=-0.00005,
        false_signal_prob=0.3,
    ),

    MarketRegime.MARKUP: RegimeConfig(
        name="Markup (Bull Trend)",
        duration_ticks=(300, 1200),
        drift_per_tick=0.00015,
        volatility=0.0015,
        vol_of_vol=0.15,
        jump_prob=0.008,
        jump_size_mean=0.008,
        jump_size_std=0.005,
        autocorr=0.35,                # strong momentum
        spread_multiplier=0.9,
        ob_imbalance_bias=0.3,
        funding_rate=0.0002,
        false_signal_prob=0.15,
    ),

    MarketRegime.DISTRIBUTION: RegimeConfig(
        name="Distribution (Smart Money Exits)",
        duration_ticks=(150, 600),
        drift_per_tick=-0.00005,
        volatility=0.002,
        vol_of_vol=0.3,               # erratic — distribution is messy
        jump_prob=0.015,
        jump_size_mean=0.004,         # fake pumps to sell into
        jump_size_std=0.008,
        autocorr=-0.1,                # mean reverting
        spread_multiplier=1.5,
        ob_imbalance_bias=-0.15,      # hidden sell pressure
        funding_rate=0.0005,          # longs euphoric and paying
        false_signal_prob=0.6,        # MANY fake signals — most dangerous regime
    ),

    MarketRegime.MARKDOWN: RegimeConfig(
        name="Markdown (Bear Trend)",
        duration_ticks=(400, 1500),
        drift_per_tick=-0.00018,
        volatility=0.002,
        vol_of_vol=0.2,
        jump_prob=0.01,
        jump_size_mean=-0.01,
        jump_size_std=0.006,
        autocorr=0.25,
        spread_multiplier=1.3,
        ob_imbalance_bias=-0.35,
        funding_rate=-0.0003,
        false_signal_prob=0.25,
    ),

    MarketRegime.RANGING_TIGHT: RegimeConfig(
        name="Ranging (Tight — Patience Test)",
        duration_ticks=(500, 2000),
        drift_per_tick=0.0,
        volatility=0.0005,
        vol_of_vol=0.05,
        jump_prob=0.001,
        jump_size_mean=0.0,
        jump_size_std=0.001,
        autocorr=-0.2,                # mean reverting
        spread_multiplier=1.0,
        ob_imbalance_bias=0.0,
        funding_rate=0.0001,
        false_signal_prob=0.45,       # many false breakouts
    ),

    MarketRegime.RANGING_WIDE: RegimeConfig(
        name="Ranging (Wide — Fee Grinder)",
        duration_ticks=(200, 800),
        drift_per_tick=0.0,
        volatility=0.003,
        vol_of_vol=0.25,
        jump_prob=0.005,
        jump_size_mean=0.0,
        jump_size_std=0.005,
        autocorr=-0.15,
        spread_multiplier=1.4,
        ob_imbalance_bias=0.0,
        funding_rate=0.0,
        false_signal_prob=0.5,
    ),

    MarketRegime.PARABOLIC: RegimeConfig(
        name="Parabolic Blow-off Top",
        duration_ticks=(50, 200),
        drift_per_tick=0.001,         # +0.1% per tick = insane
        volatility=0.004,
        vol_of_vol=0.4,
        jump_prob=0.04,
        jump_size_mean=0.025,
        jump_size_std=0.015,
        autocorr=0.6,                 # extreme momentum
        spread_multiplier=0.7,        # tight spread, everyone piling in
        ob_imbalance_bias=0.7,
        funding_rate=0.001,           # longs paying massive funding
        false_signal_prob=0.05,       # trend is real — but ends violently
    ),

    MarketRegime.CAPITULATION: RegimeConfig(
        name="Capitulation (Mass Liquidation)",
        duration_ticks=(30, 150),
        drift_per_tick=-0.002,        # -0.2% per tick
        volatility=0.008,
        vol_of_vol=0.8,
        jump_prob=0.08,
        jump_size_mean=-0.03,
        jump_size_std=0.02,
        autocorr=0.4,
        spread_multiplier=4.0,        # extreme illiquidity
        ob_imbalance_bias=-0.8,
        funding_rate=-0.002,          # shorts getting paid massively
        false_signal_prob=0.7,        # bottom pickers get destroyed
    ),

    MarketRegime.DEAD_CAT: RegimeConfig(
        name="Dead Cat Bounce (Trap)",
        duration_ticks=(50, 200),
        drift_per_tick=0.0004,        # looks like recovery...
        volatility=0.003,
        vol_of_vol=0.3,
        jump_prob=0.02,
        jump_size_mean=-0.015,        # ...then drops hard
        jump_size_std=0.01,
        autocorr=0.2,
        spread_multiplier=2.0,
        ob_imbalance_bias=0.1,        # fake buy pressure
        funding_rate=0.0003,
        false_signal_prob=0.75,       # THE most deceptive regime
    ),

    MarketRegime.LIQUIDITY_HUNT: RegimeConfig(
        name="Liquidity Hunt (Stop-Hunting)",
        duration_ticks=(30, 100),
        drift_per_tick=0.0,
        volatility=0.005,
        vol_of_vol=0.6,
        jump_prob=0.1,
        jump_size_mean=0.0,           # goes both ways
        jump_size_std=0.02,
        autocorr=-0.4,                # violent reversals
        spread_multiplier=2.5,
        ob_imbalance_bias=0.0,
        funding_rate=0.0,
        false_signal_prob=0.85,       # MAXIMUM deception
    ),

    MarketRegime.FLASH_CRASH: RegimeConfig(
        name="Flash Crash",
        duration_ticks=(5, 30),
        drift_per_tick=-0.008,        # -0.8% per tick = -24% in 30 ticks
        volatility=0.015,
        vol_of_vol=1.0,
        jump_prob=0.3,
        jump_size_mean=-0.05,
        jump_size_std=0.03,
        autocorr=0.3,
        spread_multiplier=10.0,       # market nearly broken
        ob_imbalance_bias=-1.0,
        funding_rate=-0.005,
        false_signal_prob=0.9,
    ),

    MarketRegime.MANIPULATED: RegimeConfig(
        name="Whale Manipulation",
        duration_ticks=(50, 300),
        drift_per_tick=0.0,
        volatility=0.004,
        vol_of_vol=0.5,
        jump_prob=0.05,
        jump_size_mean=0.0,           # direction changes unpredictably
        jump_size_std=0.015,
        autocorr=-0.3,
        spread_multiplier=1.8,
        ob_imbalance_bias=0.0,        # spoofed orderbook — not trustworthy
        funding_rate=0.0,
        false_signal_prob=0.8,
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2: HISTORICAL EVENT LIBRARY
#  47 real/realistic events with precise market impact models
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class HistoricalEvent:
    name:              str
    date_approx:       str
    description:       str
    # Instant shock (applied on event tick)
    price_shock:       float      # % immediate move (negative=crash)
    vol_spike:         float      # volatility multiplier for N ticks after
    vol_duration:      int        # ticks of elevated volatility
    # Secondary effects
    spread_spike:      float      # spread multiplier
    liquidity_drought: int        # ticks of reduced liquidity
    funding_shock:     float      # funding rate shock
    # Recovery dynamics
    recovery_drift:    float      # drift after shock
    recovery_ticks:    int        # ticks until back to normal
    # Teaching value
    lesson:            str        # what engines must learn from this


HISTORICAL_EVENTS: List[HistoricalEvent] = [

    # ── 2019 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("BitFinex $850M Scandal", "2019-04", "Tether used to mask losses",
        price_shock=-0.15, vol_spike=3.0, vol_duration=80, spread_spike=2.5,
        liquidity_drought=40, funding_shock=-0.0005, recovery_drift=0.0002, recovery_ticks=200,
        lesson="Trust nothing. Fundamentals can evaporate overnight."),

    HistoricalEvent("Binance 7,000 BTC Hack", "2019-05", "Security breach, 2% BTC supply stolen",
        price_shock=-0.12, vol_spike=4.0, vol_duration=50, spread_spike=3.0,
        liquidity_drought=30, funding_shock=-0.001, recovery_drift=0.0003, recovery_ticks=150,
        lesson="External shocks arrive without warning. Exit speed matters."),

    HistoricalEvent("PlusToken Ponzi Liquidation", "2019-06", "~200k BTC dumped over months",
        price_shock=-0.05, vol_spike=1.5, vol_duration=500, spread_spike=1.3,
        liquidity_drought=100, funding_shock=-0.0002, recovery_drift=0.0001, recovery_ticks=600,
        lesson="Slow bleeds are harder to detect than crashes. Patience."),

    HistoricalEvent("September Regulatory FUD (China)", "2019-09", "China bans crypto again (nth time)",
        price_shock=-0.18, vol_spike=3.5, vol_duration=60, spread_spike=2.0,
        liquidity_drought=50, funding_shock=-0.0008, recovery_drift=0.0004, recovery_ticks=120,
        lesson="Regulatory shocks are predictably unpredictable. Have escape velocity."),

    # ── 2020 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("Black Thursday — COVID Crash", "2020-03-12", "BTC -50% in 24h, entire market collapses",
        price_shock=-0.52, vol_spike=12.0, vol_duration=200, spread_spike=8.0,
        liquidity_drought=150, funding_shock=-0.003, recovery_drift=0.0008, recovery_ticks=500,
        lesson="Macro correlation = 1.0 in a true crisis. There is no hedge."),

    HistoricalEvent("BitMEX Liquidation Cascade", "2020-03-13", "100M liquidated in hours, system overloaded",
        price_shock=-0.35, vol_spike=15.0, vol_duration=50, spread_spike=12.0,
        liquidity_drought=80, funding_shock=-0.005, recovery_drift=0.001, recovery_ticks=200,
        lesson="Leverage cascade can be self-reinforcing. Know when the market is broken."),

    HistoricalEvent("DeFi Summer Flash Loans", "2020-08", "bZx, Harvest Finance exploits — protocol TVL shock",
        price_shock=-0.08, vol_spike=5.0, vol_duration=30, spread_spike=4.0,
        liquidity_drought=20, funding_shock=0.0002, recovery_drift=0.0005, recovery_ticks=100,
        lesson="Protocol risk is real. DeFi tokens can go to zero in blocks."),

    HistoricalEvent("Yearn Finance YFI Pump", "2020-09", "+40,000% in weeks — pure speculation",
        price_shock=0.35, vol_spike=8.0, vol_duration=80, spread_spike=2.0,
        liquidity_drought=10, funding_shock=0.002, recovery_drift=-0.0003, recovery_ticks=200,
        lesson="Parabolic moves end when they end. The top is never visible from inside."),

    HistoricalEvent("OKEx Withdrawal Freeze", "2020-10", "Founder arrested, withdrawals suspended",
        price_shock=-0.10, vol_spike=4.0, vol_duration=60, spread_spike=3.0,
        liquidity_drought=120, funding_shock=-0.001, recovery_drift=0.0002, recovery_ticks=300,
        lesson="Counterparty risk. The exchange is not your safe haven."),

    HistoricalEvent("MicroStrategy First BTC Purchase", "2020-08", "Corporate FOMO begins",
        price_shock=0.05, vol_spike=1.5, vol_duration=40, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.0001, recovery_drift=0.0003, recovery_ticks=100,
        lesson="Institutional demand changes the game. Read the narrative shift."),

    # ── 2021 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("Elon Musk Bitcoin Tweet — Price +20%", "2021-01", "One tweet moves a 1T asset",
        price_shock=0.22, vol_spike=5.0, vol_duration=60, spread_spike=1.5,
        liquidity_drought=0, funding_shock=0.001, recovery_drift=0.0002, recovery_ticks=150,
        lesson="Social media risk is market risk. Expect the impossible."),

    HistoricalEvent("BTC ATH $65k — Tesla Sell Announcement", "2021-05", "Tesla dumps BTC, market -30%",
        price_shock=-0.32, vol_spike=6.0, vol_duration=100, spread_spike=3.0,
        liquidity_drought=60, funding_shock=-0.002, recovery_drift=0.0003, recovery_ticks=300,
        lesson="What goes up on Elon goes down on Elon. Sentiment is a knife."),

    HistoricalEvent("China Mining Ban June 2021", "2021-06", "50% global hashrate goes offline overnight",
        price_shock=-0.25, vol_spike=4.5, vol_duration=80, spread_spike=2.5,
        liquidity_drought=40, funding_shock=-0.0015, recovery_drift=0.0004, recovery_ticks=250,
        lesson="Supply side shocks are as violent as demand shocks. Hashrate matters."),

    HistoricalEvent("El Salvador BTC Legal Tender", "2021-06", "Sovereign adoption — then immediate sell the news",
        price_shock=0.12, vol_spike=3.0, vol_duration=30, spread_spike=1.5,
        liquidity_drought=10, funding_shock=0.0005, recovery_drift=-0.0002, recovery_ticks=80,
        lesson="Buy the rumor, sell the news. Every time. Without exception."),

    HistoricalEvent("Evergrande Crisis — Crypto Contagion", "2021-09", "Macro fear spreads to crypto",
        price_shock=-0.20, vol_spike=4.0, vol_duration=100, spread_spike=2.0,
        liquidity_drought=50, funding_shock=-0.001, recovery_drift=0.0003, recovery_ticks=200,
        lesson="TradFi contagion is underpriced until it happens."),

    HistoricalEvent("BTC ATH $69k November 2021", "2021-11", "Peak euphoria. The top.",
        price_shock=0.15, vol_spike=4.0, vol_duration=50, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0005, recovery_ticks=100,
        lesson="The ATH is quiet. The distribution is silent. The crash is loud."),

    HistoricalEvent("December 2021 Deleveraging", "2021-12", "BTC -40% from ATH — silent bear begins",
        price_shock=-0.22, vol_spike=3.5, vol_duration=120, spread_spike=2.0,
        liquidity_drought=60, funding_shock=-0.0015, recovery_drift=0.0002, recovery_ticks=400,
        lesson="Bear markets begin when nobody believes they will."),

    # ── 2022 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("LUNA/UST Algorithmic Depeg", "2022-05-09", "UST loses peg. LUNA goes to zero in 3 days.",
        price_shock=-0.45, vol_spike=20.0, vol_duration=300, spread_spike=10.0,
        liquidity_drought=200, funding_shock=-0.005, recovery_drift=0.0001, recovery_ticks=1000,
        lesson="Algorithmic stablecoins have tail risk of total loss. This is not theoretical."),

    HistoricalEvent("LUNA Death Spiral Day 2", "2022-05-10", "Hyperinflation of LUNA supply — impossible to sell",
        price_shock=-0.80, vol_spike=50.0, vol_duration=100, spread_spike=20.0,
        liquidity_drought=500, funding_shock=-0.01, recovery_drift=0.0, recovery_ticks=0,
        lesson="Some assets go to exactly zero. Exit before the spiral, or don't enter."),

    HistoricalEvent("3 Arrows Capital (3AC) Collapse", "2022-06", "Largest crypto hedge fund defaults",
        price_shock=-0.30, vol_spike=5.0, vol_duration=200, spread_spike=3.5,
        liquidity_drought=100, funding_shock=-0.002, recovery_drift=0.0001, recovery_ticks=500,
        lesson="Systemic counterparty risk. One domino falls, ten more follow."),

    HistoricalEvent("Celsius Network Freezes Withdrawals", "2022-06-12", "1M users locked out of funds",
        price_shock=-0.20, vol_spike=4.0, vol_duration=100, spread_spike=3.0,
        liquidity_drought=80, funding_shock=-0.0015, recovery_drift=0.0001, recovery_ticks=300,
        lesson="CeFi yield platforms are not banks. They can gate at any moment."),

    HistoricalEvent("Voyager Digital Bankruptcy", "2022-07", "Third major CeFi collapse in 2 months",
        price_shock=-0.15, vol_spike=3.0, vol_duration=80, spread_spike=2.0,
        liquidity_drought=60, funding_shock=-0.001, recovery_drift=0.0002, recovery_ticks=200,
        lesson="Contagion compounds. Each collapse triggers the next."),

    HistoricalEvent("The Merge — Ethereum Proof of Stake", "2022-09-15", "Sell the news after 6 months of hype",
        price_shock=-0.12, vol_spike=4.0, vol_duration=60, spread_spike=1.5,
        liquidity_drought=20, funding_shock=0.0003, recovery_drift=0.0002, recovery_ticks=150,
        lesson="Even the most anticipated events can disappoint. The narrative is priced before the event."),

    HistoricalEvent("FTX Implosion — Day 1 (CZ Leak)", "2022-11-06", "CZ reveals FTX insolvency concerns",
        price_shock=-0.12, vol_spike=6.0, vol_duration=50, spread_spike=4.0,
        liquidity_drought=40, funding_shock=-0.002, recovery_drift=-0.0005, recovery_ticks=300,
        lesson="Bank runs move at internet speed. 24 hours is geological time."),

    HistoricalEvent("FTX Implosion — Day 3 (Bankruptcy)", "2022-11-08", "FTX files Chapter 11. SBF arrested.",
        price_shock=-0.35, vol_spike=15.0, vol_duration=200, spread_spike=8.0,
        liquidity_drought=150, funding_shock=-0.004, recovery_drift=0.0001, recovery_ticks=600,
        lesson="The second largest exchange on earth can fail. Overnight. Trust no one with your keys."),

    HistoricalEvent("Genesis/DCG Contagion", "2022-11", "Grayscale, DCG, Genesis all under pressure",
        price_shock=-0.18, vol_spike=4.0, vol_duration=150, spread_spike=2.5,
        liquidity_drought=100, funding_shock=-0.001, recovery_drift=0.0001, recovery_ticks=400,
        lesson="The FTX hole was deeper than anyone knew. Counterparty risk never stops."),

    HistoricalEvent("Year-End 2022 — BTC $16k (Lowest Point)", "2022-12", "Crypto winter bottoms out",
        price_shock=-0.10, vol_spike=2.0, vol_duration=100, spread_spike=1.5,
        liquidity_drought=50, funding_shock=-0.0005, recovery_drift=0.0003, recovery_ticks=300,
        lesson="Winters end. Positioning before the thaw is everything."),

    # ── 2023 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("Silvergate Bank Collapse", "2023-03-08", "Crypto-friendly bank fails",
        price_shock=-0.08, vol_spike=3.5, vol_duration=60, spread_spike=2.0,
        liquidity_drought=40, funding_shock=-0.001, recovery_drift=0.0002, recovery_ticks=150,
        lesson="The on-ramps and off-ramps matter. Banking rails can fail."),

    HistoricalEvent("Silicon Valley Bank Crisis — USDC Depeg", "2023-03-10",
                    "USDC temporarily depegs to $0.87 as Circle's reserves trapped at SVB",
        price_shock=-0.15, vol_spike=8.0, vol_duration=80, spread_spike=5.0,
        liquidity_drought=60, funding_shock=-0.002, recovery_drift=0.0005, recovery_ticks=200,
        lesson="Even 'safe' stablecoins can depeg. No asset is truly riskless."),

    HistoricalEvent("BUSD Forced Wind-Down by NYDFS", "2023-02", "Regulatory kill of Binance stablecoin",
        price_shock=-0.06, vol_spike=2.5, vol_duration=50, spread_spike=1.8,
        liquidity_drought=30, funding_shock=-0.0003, recovery_drift=0.0002, recovery_ticks=120,
        lesson="Regulatory capture of infrastructure hits everything downstream."),

    HistoricalEvent("SEC vs Coinbase/Binance Lawsuits", "2023-06", "US regulator goes to war with both biggest exchanges",
        price_shock=-0.12, vol_spike=3.0, vol_duration=100, spread_spike=2.0,
        liquidity_drought=50, funding_shock=-0.001, recovery_drift=0.0002, recovery_ticks=250,
        lesson="Legal risk reprices the entire sector simultaneously."),

    HistoricalEvent("Curve Finance Exploit — $70M Drained", "2023-07-30", "Reentrancy attack, DeFi panic",
        price_shock=-0.08, vol_spike=5.0, vol_duration=40, spread_spike=3.0,
        liquidity_drought=30, funding_shock=-0.0005, recovery_drift=0.0003, recovery_ticks=100,
        lesson="Smart contract risk is existential. Audits don't guarantee safety."),

    HistoricalEvent("BlackRock Bitcoin ETF Filing", "2023-06-15", "Institutional demand narrative ignites",
        price_shock=0.10, vol_spike=2.5, vol_duration=40, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.0005, recovery_drift=0.0004, recovery_ticks=100,
        lesson="Narrative shifts drive multi-month trends. Be early or be wrong."),

    HistoricalEvent("BTC $38k Breakout — ETF Euphoria", "2023-10", "Market prices in ETF approval",
        price_shock=0.20, vol_spike=3.0, vol_duration=60, spread_spike=0.7,
        liquidity_drought=0, funding_shock=0.001, recovery_drift=0.0004, recovery_ticks=150,
        lesson="Anticipation can sustain rallies longer than fundamentals justify."),

    HistoricalEvent("Binance/CZ $4.3B Fine — CZ Resigns", "2023-11-21",
                    "Largest financial penalty in history. CZ pleads guilty.",
        price_shock=-0.08, vol_spike=4.0, vol_duration=80, spread_spike=2.5,
        liquidity_drought=50, funding_shock=-0.001, recovery_drift=0.0003, recovery_ticks=200,
        lesson="The biggest player is not too big to fail. Decentralize risk."),

    # ── 2024 EVENTS ────────────────────────────────────────────────────────────
    HistoricalEvent("Bitcoin ETF Approval — Jan 10 2024", "2024-01-10",
                    "SEC approves spot BTC ETF. Sell the news immediately.",
        price_shock=0.05, vol_spike=6.0, vol_duration=80, spread_spike=1.2,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0003, recovery_ticks=200,
        lesson="The most anticipated event in crypto history was instantly sold."),

    HistoricalEvent("GBTC Outflows — $5B in Weeks", "2024-01", "Grayscale becomes net seller post-ETF",
        price_shock=-0.15, vol_spike=3.5, vol_duration=100, spread_spike=1.5,
        liquidity_drought=30, funding_shock=-0.001, recovery_drift=0.0002, recovery_ticks=250,
        lesson="Structural sellers can overwhelm new demand. Supply matters."),

    HistoricalEvent("Bitcoin Halving April 2024", "2024-04-20", "Block reward 6.25→3.125 BTC",
        price_shock=0.04, vol_spike=3.0, vol_duration=60, spread_spike=0.9,
        liquidity_drought=0, funding_shock=0.0003, recovery_drift=0.0005, recovery_ticks=150,
        lesson="Halvings are supply shocks that take months to manifest."),

    HistoricalEvent("BTC $73k ATH March 2024", "2024-03-14", "Fastest ATH recovery in history",
        price_shock=0.08, vol_spike=4.0, vol_duration=50, spread_spike=0.8,
        liquidity_drought=0, funding_shock=0.002, recovery_drift=-0.0002, recovery_ticks=100,
        lesson="Post-ETF regime change. Institutional buyers absorb corrections faster."),

    HistoricalEvent("Jump Trading Liquidation Cascade", "2024-08", "Market maker exits — sudden -20%",
        price_shock=-0.22, vol_spike=8.0, vol_duration=80, spread_spike=5.0,
        liquidity_drought=60, funding_shock=-0.003, recovery_drift=0.0005, recovery_ticks=200,
        lesson="Market makers are not permanent. Their exit is catastrophic."),

    HistoricalEvent("Yen Carry Trade Unwind", "2024-08-05", "Global macro shock, BTC -25% in hours",
        price_shock=-0.28, vol_spike=10.0, vol_duration=120, spread_spike=6.0,
        liquidity_drought=80, funding_shock=-0.004, recovery_drift=0.0006, recovery_ticks=300,
        lesson="Macro is crypto's shadow. The yen can crash your altcoin."),

    # ── SYNTHETIC EXTREME EVENTS (always possible) ─────────────────────────────
    HistoricalEvent("Exchange API Failure Mid-Trade", "SYNTHETIC",
                    "API goes down while you're in a position",
        price_shock=0.0, vol_spike=2.0, vol_duration=20, spread_spike=5.0,
        liquidity_drought=30, funding_shock=0.0, recovery_drift=0.0, recovery_ticks=30,
        lesson="Infrastructure risk is real. Never size beyond what you can exit manually."),

    HistoricalEvent("Wash Trading Detection — Pair Delisted", "SYNTHETIC",
                    "Exchange removes pair from futures without warning",
        price_shock=-0.30, vol_spike=3.0, vol_duration=50, spread_spike=8.0,
        liquidity_drought=200, funding_shock=-0.003, recovery_drift=0.0, recovery_ticks=0,
        lesson="Pair risk. Never bet the farm on a single instrument."),

    HistoricalEvent("Coordinated Short Attack (Whale Coalition)", "SYNTHETIC",
                    "3 whales coordinate -15% attack to liquidate leveraged longs",
        price_shock=-0.18, vol_spike=6.0, vol_duration=60, spread_spike=3.0,
        liquidity_drought=40, funding_shock=-0.002, recovery_drift=0.0008, recovery_ticks=150,
        lesson="The market can stay irrational longer than you can stay solvent."),

    HistoricalEvent("Nuclear Black Swan — Total Market Halt", "SYNTHETIC",
                    "Exchange halts trading for 6 hours. No fills possible.",
        price_shock=-0.05, vol_spike=0.0, vol_duration=360, spread_spike=100.0,
        liquidity_drought=360, funding_shock=0.0, recovery_drift=0.0001, recovery_ticks=100,
        lesson="Sometimes the answer is: you cannot act. Prepare for that."),
]


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3: MARKET SIMULATOR
#  Generates tick-by-tick price/orderbook data from regime configs
#  with realistic microstructure and event injection
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    """One tick of market data."""
    t:             int       # tick index
    price:         float
    bid:           float
    ask:           float
    spread_pct:    float
    ob_imbalance:  float
    volume:        float
    vol_spike:     bool
    funding_rate:  float
    regime:        str
    event:         Optional[str]

    @property
    def returns(self) -> float:
        return 0.0  # computed externally from price sequence


class MarketSimulator:
    """
    Generates synthetic market data for 1 pair over 5 years.
    One 'tick' ≈ 1 minute. 5 years ≈ 2,628,000 ticks.
    For training efficiency: 1 year ≈ 525,600 ticks.
    We generate compressed: 1 tick = ~1-5 minutes of real data.
    Total: ~500,000 ticks per pair = rich training dataset.
    """

    TICKS_PER_YEAR  = 180_000    # ~1 tick per 3 minutes, realistic density
    TRAINING_YEARS  = 5.5        # 2019 mid → 2024 mid
    TOTAL_TICKS     = int(TICKS_PER_YEAR * TRAINING_YEARS)  # ~990,000

    def __init__(
        self,
        pair:             str,
        start_price:      float = 10_000.0,
        curriculum_level: int   = 1,         # 1=easy → 5=brutal
    ):
        self.pair             = pair
        self.price            = start_price
        self.tick_idx         = 0
        self.curriculum_level = curriculum_level

        # Current regime state
        self.current_regime:   MarketRegime   = MarketRegime.RANGING_TIGHT
        self.regime_ticks_left: int           = 500
        self.regime_config:    RegimeConfig   = REGIME_CONFIGS[self.current_regime]

        # GARCH volatility state
        self.current_vol:  float = self.regime_config.volatility
        self.vol_ema:      float = self.regime_config.volatility

        # Event injection state
        self.event_queue: deque = deque()
        self.events_fired: int  = 0
        self.active_aftershock: Optional[Tuple[HistoricalEvent, int]] = None

        # Price history for feature computation
        self.price_history: deque = deque(maxlen=300)
        self.return_history: deque = deque(maxlen=100)

        # Regime transition matrix (weighted by curriculum)
        self._build_transition_matrix()

        # Inject events probabilistically based on curriculum
        self._schedule_events()

    def _build_transition_matrix(self):
        """Curriculum-aware regime transition probabilities."""
        # Base probability of each regime
        base = {
            MarketRegime.RANGING_TIGHT:        0.20,
            MarketRegime.MARKUP:               0.18,
            MarketRegime.MARKDOWN:             0.15,
            MarketRegime.STEALTH_ACCUMULATION: 0.10,
            MarketRegime.DISTRIBUTION:         0.08,
            MarketRegime.RANGING_WIDE:         0.08,
            MarketRegime.DEAD_CAT:             0.05,
            MarketRegime.LIQUIDITY_HUNT:       0.06,
            MarketRegime.PARABOLIC:            0.04,
            MarketRegime.CAPITULATION:         0.03,
            MarketRegime.FLASH_CRASH:          0.02,
            MarketRegime.MANIPULATED:          0.01,
        }
        # Curriculum scaling: higher level → more extreme regimes
        extreme_regimes = {
            MarketRegime.FLASH_CRASH, MarketRegime.CAPITULATION,
            MarketRegime.LIQUIDITY_HUNT, MarketRegime.MANIPULATED,
            MarketRegime.DEAD_CAT, MarketRegime.DISTRIBUTION,
        }
        multiplier = 1.0 + (self.curriculum_level - 1) * 0.5
        for r in extreme_regimes:
            base[r] *= multiplier

        # Normalize
        total = sum(base.values())
        self.regime_probs = {r: p/total for r, p in base.items()}

    def _schedule_events(self):
        """Schedule historical events across the timeline."""
        n_events = int(len(HISTORICAL_EVENTS) * (0.3 + self.curriculum_level * 0.15))
        n_events = min(n_events, len(HISTORICAL_EVENTS))
        selected = random.sample(HISTORICAL_EVENTS, n_events)
        # Spread events across training timeline
        for i, event in enumerate(selected):
            tick = random.randint(
                int(self.TOTAL_TICKS * i / n_events),
                int(self.TOTAL_TICKS * (i + 1) / n_events)
            )
            self.event_queue.append((tick, event))
        self.event_queue = deque(sorted(self.event_queue, key=lambda x: x[0]))

    def _transition_regime(self) -> MarketRegime:
        """Choose next market regime."""
        regimes = list(self.regime_probs.keys())
        probs   = [self.regime_probs[r] for r in regimes]
        return random.choices(regimes, weights=probs, k=1)[0]

    def _update_garch_vol(self, ret: float) -> float:
        """GARCH(1,1) volatility update."""
        omega = self.regime_config.volatility ** 2 * 0.05
        alpha = 0.1
        beta  = 0.85
        self.current_vol = math.sqrt(
            omega + alpha * ret**2 + beta * self.current_vol**2
        )
        # Add vol-of-vol perturbation
        vov = self.regime_config.vol_of_vol
        self.current_vol *= (1.0 + random.gauss(0, vov * 0.1))
        self.current_vol = max(0.0001, min(0.05, self.current_vol))
        return self.current_vol

    def next_tick(self) -> Tick:
        """Generate the next market tick."""
        # Check if regime needs to change
        if self.regime_ticks_left <= 0:
            self.current_regime = self._transition_regime()
            self.regime_config  = REGIME_CONFIGS[self.current_regime]
            lo, hi = self.regime_config.duration_ticks
            self.regime_ticks_left = random.randint(lo, hi)
        self.regime_ticks_left -= 1

        cfg = self.regime_config
        event_name = None

        # ── Check for scheduled event injection ───────────────────────────────
        if self.event_queue and self.event_queue[0][0] <= self.tick_idx:
            _, event = self.event_queue.popleft()
            self._apply_event(event)
            event_name = event.name
            self.events_fired += 1

        # ── Check for active aftershock ────────────────────────────────────────
        if self.active_aftershock:
            evt, ticks_left = self.active_aftershock
            if ticks_left > 0:
                # Aftershock: elevated vol + recovery drift
                self.current_vol *= evt.vol_spike ** (ticks_left / evt.vol_duration)
                self.active_aftershock = (evt, ticks_left - 1)
            else:
                self.active_aftershock = None

        # ── Generate return ────────────────────────────────────────────────────
        vol   = self._update_garch_vol(
            self.return_history[-1] if self.return_history else 0
        )

        # Autocorrelated return
        autocorr = cfg.autocorr
        last_ret  = self.return_history[-1] if self.return_history else 0.0
        drift     = cfg.drift_per_tick
        base_ret  = drift + autocorr * last_ret + random.gauss(0, vol)

        # Jump process
        ret = base_ret
        if random.random() < cfg.jump_prob:
            jump = random.gauss(cfg.jump_size_mean, cfg.jump_size_std)
            ret += jump

        # Clip extreme returns (market circuit breakers don't allow >50% in 1 tick)
        ret = max(-0.30, min(0.30, ret))

        # Update price
        self.price = self.price * (1.0 + ret)
        self.price = max(0.01, self.price)

        self.price_history.append(self.price)
        self.return_history.append(ret)

        # ── Orderbook simulation ────────────────────────────────────────────────
        spread_pct   = random.gauss(0.0008, 0.0002) * cfg.spread_multiplier
        spread_pct   = max(0.0001, spread_pct)
        half_spread  = self.price * spread_pct / 2
        bid          = self.price - half_spread
        ask          = self.price + half_spread

        # Orderbook imbalance: biased by regime + noise
        ob_imbalance = cfg.ob_imbalance_bias + random.gauss(0, 0.15)
        ob_imbalance = max(-1.0, min(1.0, ob_imbalance))

        # Volume: higher in volatile regimes
        volume = abs(random.gauss(1.0, 0.5)) * (1.0 + abs(ret) * 10)

        self.tick_idx += 1
        return Tick(
            t=self.tick_idx,
            price=self.price,
            bid=bid, ask=ask,
            spread_pct=spread_pct,
            ob_imbalance=ob_imbalance,
            volume=volume,
            vol_spike=vol > cfg.volatility * 2,
            funding_rate=cfg.funding_rate + random.gauss(0, abs(cfg.funding_rate) + 0.0001),
            regime=cfg.name,
            event=event_name,
        )

    def _apply_event(self, event: HistoricalEvent):
        """Apply an historical event's immediate shock."""
        self.price *= (1.0 + event.price_shock)
        self.price  = max(0.01, self.price)
        self.current_vol *= event.vol_spike
        self.active_aftershock = (event, event.vol_duration)
        if event.vol_duration > 0:
            # Force regime to CAPITULATION or FLASH_CRASH during event
            if event.price_shock < -0.20:
                self.current_regime   = MarketRegime.FLASH_CRASH
                self.regime_config    = REGIME_CONFIGS[MarketRegime.FLASH_CRASH]
                self.regime_ticks_left = event.vol_duration // 3
        logger.debug(f"💥 EVENT: {event.name} | shock={event.price_shock:+.1%} | vol×{event.vol_spike:.0f}")

    def build_rl_state(self, position_size: float = 0, unrealized_pnl: float = 0,
                       win_rate: float = 0.5, drawdown: float = 0) -> RLState:
        """Convert current market state to RL state vector."""
        prices = list(self.price_history)
        cfg    = self.regime_config

        if len(prices) < 60:
            return RLState()

        # Fast helpers
        def pct_chg(n):
            return (prices[-1] - prices[-n]) / prices[-n] if len(prices) >= n and prices[-n] > 0 else 0.0

        def vol_n(n):
            p = np.array(prices[-n:])
            return float(np.std(np.diff(p)/p[:-1])) if len(p) >= 2 else 0.0

        rets_20 = np.array([prices[i]/prices[i-1]-1 for i in range(-20,0) if len(prices) >= abs(i)])

        # RSI fast
        def rsi(n):
            if len(prices) < n+1: return 0.5
            r = np.diff(prices[-(n+1):])
            g = r[r>0].mean() if (r>0).any() else 0
            l = -r[r<0].mean() if (r<0).any() else 1e-10
            return (g/(g+l))

        # EMA
        def fast_ema(n):
            k = 2/(n+1)
            e = prices[-min(n*3, len(prices))]
            for p in prices[-min(n*3, len(prices))+1:]:
                e = p*k + e*(1-k)
            return e

        ema8  = fast_ema(8)
        ema21 = fast_ema(21)

        # BB position
        p_arr = np.array(prices[-20:])
        p_mean= float(p_arr.mean())
        p_std = float(p_arr.std()) + 1e-10
        bb_pos = float(np.clip((prices[-1] - (p_mean - 2*p_std)) / (4*p_std), 0, 1))

        # VWAP proxy
        vwap  = float(np.mean(prices[-60:])) if len(prices) >= 60 else prices[-1]

        # Last tick data
        ret_history = list(self.return_history)

        return RLState(
            price_change_1m  = pct_chg(2),
            price_change_5m  = pct_chg(6),
            price_change_15m = pct_chg(16),
            price_change_1h  = pct_chg(61) if len(prices) >= 61 else 0,
            volatility_1m    = vol_n(5),
            volatility_5m    = vol_n(30),
            high_low_ratio   = float(np.clip((prices[-1]-min(prices[-20:]))/(max(prices[-20:])-min(prices[-20:])+1e-10),0,1)),
            price_vs_vwap    = (prices[-1]-vwap)/(vwap+1e-10),
            rsi_14           = rsi(14),
            rsi_5            = rsi(5),
            macd_signal      = float(np.tanh((fast_ema(12)-fast_ema(26))/(prices[-1]*0.001+1e-10))),
            bb_position      = bb_pos,
            ema_cross        = (ema8-ema21)/(ema21+1e-10),
            momentum_score   = float(np.mean(np.sign(rets_20))) if len(rets_20) > 0 else 0,
            trend_strength   = float(min(abs(pct_chg(30))*10, 1.0)),
            mean_reversion   = float(-np.tanh(pct_chg(10)*20)),
            ob_imbalance     = float(np.clip(cfg.ob_imbalance_bias + random.gauss(0,0.1), -1, 1)),
            spread_pct       = float(random.gauss(0.0008, 0.0002) * cfg.spread_multiplier),
            bid_depth        = float(abs(random.gauss(1.0, 0.3))),
            ask_depth        = float(abs(random.gauss(1.0, 0.3))),
            volume_spike     = float(min(abs(random.gauss(0,0.5)), 3.0)),
            trade_flow       = float(np.clip(cfg.ob_imbalance_bias + random.gauss(0, 0.2), -1, 1)),
            regime_bull      = 1.0 if self.current_regime in (MarketRegime.MARKUP, MarketRegime.PARABOLIC, MarketRegime.STEALTH_ACCUMULATION) else 0.0,
            regime_bear      = 1.0 if self.current_regime in (MarketRegime.MARKDOWN, MarketRegime.CAPITULATION, MarketRegime.FLASH_CRASH) else 0.0,
            regime_ranging   = 1.0 if self.current_regime in (MarketRegime.RANGING_TIGHT, MarketRegime.RANGING_WIDE) else 0.0,
            regime_volatile  = 1.0 if self.current_regime in (MarketRegime.FLASH_CRASH, MarketRegime.LIQUIDITY_HUNT, MarketRegime.MANIPULATED) else 0.0,
            funding_rate     = float(cfg.funding_rate),
            open_interest_chg= float(random.gauss(0, 0.01)),
            position_size    = float(position_size),
            unrealized_pnl   = float(unrealized_pnl),
            win_rate_recent  = float(win_rate),
            drawdown         = float(drawdown),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4: SIMULATED TRADING ACCOUNT
#  Realistic P&L with fees, slippage, liquidation, funding
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SimPosition:
    direction:    str      # 'long' or 'short'
    entry_price:  float
    size_usd:     float
    leverage:     int
    entry_tick:   int
    peak_pnl:     float = 0.0

class TradingAccount:
    """Simulates a trading account with realistic constraints."""

    MAKER_FEE   = 0.0002
    TAKER_FEE   = 0.0005
    LEVERAGE    = 10
    MAX_HOLD    = 120      # ticks (120 min)

    def __init__(self, capital: float, pair: str):
        self.capital      = capital
        self.peak_capital = capital
        self.pair         = pair
        self.position:    Optional[SimPosition] = None

        # Stats
        self.total_trades  = 0
        self.wins          = 0
        self.total_pnl     = 0.0
        self.total_fees    = 0.0
        self.max_drawdown  = 0.0
        self.trade_log:    List[Dict] = []
        self.equity_curve: List[float] = [capital]
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades > 0 else 0.5

    @property
    def drawdown(self) -> float:
        if self.peak_capital <= 0: return 1.0
        return (self.peak_capital - self.capital) / self.peak_capital

    def compute_signal_reward(
        self,
        action: Action,
        current_price: float,
        tick: Tick,
        tick_idx: int,
    ) -> Tuple[float, bool]:
        """
        Execute action, compute reward.
        Returns (reward, trade_closed).
        """
        reward = 0.0
        closed = False

        # ── Open position ───────────────────────────────────────────────────────
        if self.position is None:
            if action in (Action.STRONG_BUY, Action.BUY):
                size_frac = 1.0 if action == Action.STRONG_BUY else 0.5
                size_usd  = min(self.capital * size_frac, self.capital)
                fee       = size_usd * self.TAKER_FEE * self.LEVERAGE
                if size_usd >= 1.0 and self.capital > fee:
                    self.position   = SimPosition('long', current_price, size_usd,
                                                   self.LEVERAGE, tick_idx)
                    self.capital   -= fee
                    self.total_fees += fee
                    reward          = -fee / self.capital if self.capital > 0 else 0

            elif action in (Action.STRONG_SELL, Action.SELL):
                size_frac = 1.0 if action == Action.STRONG_SELL else 0.5
                size_usd  = min(self.capital * size_frac, self.capital)
                fee       = size_usd * self.TAKER_FEE * self.LEVERAGE
                if size_usd >= 1.0 and self.capital > fee:
                    self.position   = SimPosition('short', current_price, size_usd,
                                                   self.LEVERAGE, tick_idx)
                    self.capital   -= fee
                    self.total_fees += fee
                    reward          = -fee / self.capital if self.capital > 0 else 0

        # ── Manage open position ────────────────────────────────────────────────
        elif self.position is not None:
            pos  = self.position
            p    = current_price
            ep   = pos.entry_price
            lev  = pos.leverage
            susd = pos.size_usd

            # Unrealized P&L (leveraged)
            if pos.direction == 'long':
                upnl_pct = (p - ep) / ep * lev
            else:
                upnl_pct = (ep - p) / ep * lev

            # Track peak for trailing stop
            if upnl_pct > pos.peak_pnl:
                pos.peak_pnl = upnl_pct

            # Liquidation check (leverage limit)
            liq_threshold = -1.0 / lev  # -10% at 10x = liquidation
            if upnl_pct <= liq_threshold:
                loss_usd = susd * abs(liq_threshold)
                self.capital   -= loss_usd
                self.capital    = max(0, self.capital)
                self.total_pnl -= loss_usd
                self.total_trades += 1
                self.consecutive_losses += 1
                self.max_consecutive_losses = max(self.max_consecutive_losses, self.consecutive_losses)
                reward = -2.0   # liquidation is catastrophic
                self.trade_log.append({'type':'LIQUIDATED', 'pnl_pct': liq_threshold,
                                       'regime': tick.regime, 'event': tick.event})
                self.position = None
                closed = True

            # TP/SL/Timeout/Manual close
            elif action in (Action.STRONG_BUY, Action.BUY, Action.STRONG_SELL, Action.SELL, Action.HOLD):
                close = False
                close_reason = ""

                # Hard TP: +2% net leveraged return
                if upnl_pct >= 0.20:
                    close = True; close_reason = "TP"
                # Hard SL: -0.8% net leveraged return
                elif upnl_pct <= -0.08:
                    close = True; close_reason = "SL"
                # Trailing stop: if retraced 40% from peak
                elif pos.peak_pnl > 0.10 and upnl_pct < pos.peak_pnl * 0.60:
                    close = True; close_reason = "TRAIL"
                # Timeout: max hold
                elif tick_idx - pos.entry_tick >= self.MAX_HOLD:
                    close = True; close_reason = "TIMEOUT"
                # Reverse signal: close on opposite direction
                elif (pos.direction == 'long'  and action in (Action.STRONG_SELL, Action.SELL)):
                    close = True; close_reason = "REVERSE"
                elif (pos.direction == 'short' and action in (Action.STRONG_BUY, Action.BUY)):
                    close = True; close_reason = "REVERSE"

                if close:
                    fee      = susd * self.TAKER_FEE
                    pnl_usd  = susd * upnl_pct - fee
                    self.capital     += pnl_usd
                    self.capital      = max(0, self.capital)
                    self.total_pnl   += pnl_usd
                    self.total_fees  += fee
                    self.total_trades += 1

                    if pnl_usd > 0:
                        self.wins += 1
                        self.consecutive_losses = 0
                        # Reward: Sharpe-like — profit relative to initial pos size
                        reward = min(2.0, pnl_usd / (susd * 0.01 + 1e-5))
                    else:
                        self.consecutive_losses += 1
                        self.max_consecutive_losses = max(self.max_consecutive_losses, self.consecutive_losses)
                        reward = max(-2.0, pnl_usd / (susd * 0.01 + 1e-5))

                    self.trade_log.append({
                        'reason': close_reason, 'pnl_usd': round(pnl_usd, 4),
                        'pnl_pct': round(upnl_pct, 4), 'duration': tick_idx - pos.entry_tick,
                        'regime': tick.regime, 'event': tick.event,
                    })
                    self.position = None
                    closed = True

                else:
                    # Intermediate reward: small positive for floating profit
                    reward = upnl_pct * 0.01

        # Update equity curve and drawdown
        self.equity_curve.append(self.capital)
        if self.capital > self.peak_capital:
            self.peak_capital = self.capital
        dd = self.drawdown
        if dd > self.max_drawdown:
            self.max_drawdown = dd

        return reward, closed


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5: THE TRAINING GAUNTLET
#  Curriculum learning across 5 difficulty levels + 12 pairs
# ══════════════════════════════════════════════════════════════════════════════

TRAINING_PAIRS = [
    # (pair_name, start_price)
    ("PI_XBTUSD",   8_000.0),    # BTC — the anchor
    ("PF_ETHUSD",   200.0),      # ETH — correlated giant
    ("PF_SOLUSD",   1.2),        # SOL — volatile L1
    ("PF_AVAXUSD",  3.0),        # AVAX
    ("PF_DOGEUSD",  0.003),      # DOGE — meme volatility
    ("PF_LINKUSD",  2.5),        # LINK — steady
    ("PF_MATICUSD", 0.015),      # MATIC — L2
    ("PF_ADAUSD",   0.05),       # ADA — slow mover
    ("PF_AAVEUSD",  50.0),       # AAVE — DeFi
    ("PF_LDOUSD",   0.50),       # LDO — protocol token
    ("PF_PEPEUSD",  0.000001),   # PEPE — pure meme
    ("PF_WIFUSD",   0.001),      # WIF — dog coin chaos
]

CURRICULUM_LEVELS = [
    {
        "name":         "LEVEL 1 — AWAKENING",
        "description":  "Normal markets. Stable regimes. Learn the basics.",
        "ticks":        50_000,
        "pairs":        2,
        "extreme_events": 0.0,   # No extreme events
        "curriculum":   1,
    },
    {
        "name":         "LEVEL 2 — BAPTISM BY FIRE",
        "description":  "Real volatility. Some events. You'll make mistakes.",
        "ticks":        100_000,
        "pairs":        4,
        "extreme_events": 0.3,
        "curriculum":   2,
    },
    {
        "name":         "LEVEL 3 — THE GRINDER",
        "description":  "All regimes. Half the historical events. Many losses.",
        "ticks":        150_000,
        "pairs":        8,
        "extreme_events": 0.6,
        "curriculum":   3,
    },
    {
        "name":         "LEVEL 4 — THE INFERNO",
        "description":  "Full event library. Extreme vol. LUNA + FTX included.",
        "ticks":        200_000,
        "pairs":        12,
        "extreme_events": 0.85,
        "curriculum":   4,
    },
    {
        "name":         "LEVEL 5 — THE IMPOSSIBLE",
        "description":  "Everything. Simultaneous events. Liquidity hunts. "
                        "Flash crashes on every pair. The market is hostile.",
        "ticks":        250_000,
        "pairs":        12,
        "extreme_events": 1.0,
        "curriculum":   5,
    },
]


class GauntletTrainer:
    """
    Runs the full training gauntlet across all curriculum levels.
    """

    def __init__(self, capital_per_pair: float = 1000.0):
        self.capital_per_pair = capital_per_pair
        self.orchestrators:   Dict[str, ExtendedRLOrchestrator] = {}
        self.accounts:        Dict[str, TradingAccount]         = {}
        self.level_results:   List[Dict] = []
        self.total_ticks_trained = 0
        self.training_start       = time.time()

    def _get_or_create_orch(self, pair: str) -> ExtendedRLOrchestrator:
        if pair not in self.orchestrators:
            self.orchestrators[pair] = get_rl_orchestrator(pair)
        return self.orchestrators[pair]

    def _reset_accounts(self, pairs: List[str]):
        for pair in pairs:
            self.accounts[pair] = TradingAccount(self.capital_per_pair, pair)

    def _train_single_episode(
        self,
        pair:         str,
        start_price:  float,
        n_ticks:      int,
        curriculum:   int,
    ) -> Dict:
        """Train one pair for one curriculum level."""
        sim  = MarketSimulator(pair, start_price, curriculum_level=curriculum)
        acct = self.accounts[pair]
        orch = self._get_or_create_orch(pair)

        ticks_per_print = max(n_ticks // 20, 1000)
        prev_price      = start_price

        regime_ticks:   Dict[str, int] = defaultdict(int)
        event_impacts:  List[Dict]     = []

        for t in range(n_ticks):
            tick  = sim.next_tick()
            state = sim.build_rl_state(
                position_size   = (acct.position.direction == 'long' and 1.0 or
                                   acct.position.direction == 'short' and -1.0
                                   if acct.position else 0.0),
                unrealized_pnl  = 0.0,
                win_rate        = acct.win_rate,
                drawdown        = acct.drawdown,
            )

            # Get decision from cluster
            decision = orch.decide(state)
            direction = decision['direction']
            confidence = decision['confidence']

            # Map to action
            if direction == 'buy':
                action = Action.STRONG_BUY if confidence > 0.80 else Action.BUY
            elif direction == 'sell':
                action = Action.STRONG_SELL if confidence > 0.80 else Action.SELL
            else:
                action = Action.HOLD

            # Execute and get reward
            reward, closed = acct.compute_signal_reward(action, tick.price, tick, t)

            # Build next state for RL update
            next_tick  = sim.next_tick()
            next_state = sim.build_rl_state(
                position_size  = 0,
                unrealized_pnl = 0,
                win_rate       = acct.win_rate,
                drawdown       = acct.drawdown,
            )
            sim.tick_idx -= 1  # undo the extra tick

            # Feed back to cluster
            orch.record_outcome(reward, reward * 0.01, next_state)

            # Track regime distribution
            regime_ticks[tick.regime] += 1

            # Track events
            if tick.event:
                event_impacts.append({
                    'tick':    t,
                    'event':   tick.event,
                    'price':   tick.price,
                    'capital': acct.capital,
                    'reward':  reward,
                })

            # Progress print
            if t % ticks_per_print == 0 and t > 0:
                pnl_pct = (acct.capital / self.capital_per_pair - 1) * 100
                wr      = acct.win_rate * 100
                dd      = acct.max_drawdown * 100
                logger.info(
                    f"   ▸ {pair[-8:]:>8} t={t:>7,} "
                    f"cap=${acct.capital:>8.2f} "
                    f"pnl={pnl_pct:>+7.2f}% "
                    f"wr={wr:>5.1f}% "
                    f"dd={dd:>5.1f}% "
                    f"trades={acct.total_trades:>4} "
                    f"regime={tick.regime[:18]:<18}"
                )

            # Bankruptcy check
            if acct.capital < 1.0:
                logger.warning(f"   💀 {pair} BANKRUPT at tick {t} — resetting capital")
                acct.capital = self.capital_per_pair * 0.1  # small reload
                break

        return {
            "pair":         pair,
            "ticks":        t + 1,
            "final_capital":acct.capital,
            "total_pnl":    acct.total_pnl,
            "total_fees":   acct.total_fees,
            "total_trades": acct.total_trades,
            "win_rate":     acct.win_rate,
            "max_drawdown": acct.max_drawdown,
            "events_fired": sim.events_fired,
            "regime_dist":  dict(regime_ticks),
            "event_impacts":event_impacts[:10],
            "max_consec_losses": acct.max_consecutive_losses,
        }

    def run_level(self, level_cfg: Dict) -> Dict:
        """Run one curriculum level."""
        name        = level_cfg["name"]
        description = level_cfg["description"]
        n_ticks     = level_cfg["ticks"]
        n_pairs     = level_cfg["pairs"]
        curriculum  = level_cfg["curriculum"]

        pairs_subset = TRAINING_PAIRS[:n_pairs]
        self._reset_accounts([p for p, _ in pairs_subset])

        logger.info("=" * 78)
        logger.info(f"  {name}")
        logger.info(f"  {description}")
        logger.info(f"  Ticks: {n_ticks:,} per pair × {n_pairs} pairs = {n_ticks*n_pairs:,} total")
        logger.info("=" * 78)

        level_start = time.time()
        pair_results = []

        for pair_name, start_price in pairs_subset:
            logger.info(f"\n  🦑 Training {pair_name} (start=${start_price:.4f})")
            result = self._train_single_episode(pair_name, start_price, n_ticks, curriculum)
            pair_results.append(result)
            self.total_ticks_trained += result["ticks"]

        level_elapsed = time.time() - level_start

        # Aggregate level stats
        avg_wr    = np.mean([r["win_rate"]     for r in pair_results])
        avg_dd    = np.mean([r["max_drawdown"] for r in pair_results])
        avg_pnl   = np.mean([(r["final_capital"]/self.capital_per_pair-1) for r in pair_results])
        avg_trades= np.mean([r["total_trades"] for r in pair_results])
        total_events = sum(r["events_fired"] for r in pair_results)

        level_result = {
            "level":         name,
            "elapsed_s":     round(level_elapsed, 1),
            "pairs_trained": n_pairs,
            "avg_win_rate":  round(avg_wr, 4),
            "avg_drawdown":  round(avg_dd, 4),
            "avg_pnl_pct":   round(avg_pnl * 100, 2),
            "avg_trades":    round(avg_trades, 1),
            "total_events":  total_events,
            "pair_results":  pair_results,
        }

        logger.info(f"\n  📊 LEVEL SUMMARY:")
        logger.info(f"     Avg Win Rate : {avg_wr*100:.1f}%")
        logger.info(f"     Avg Drawdown : {avg_dd*100:.1f}%")
        logger.info(f"     Avg P&L      : {avg_pnl*100:+.2f}%")
        logger.info(f"     Avg Trades   : {avg_trades:.0f}")
        logger.info(f"     Events Fired : {total_events}")
        logger.info(f"     Time Elapsed : {level_elapsed:.1f}s")

        # Save after every level
        save_all_models()
        logger.info("  💾 Models saved.")

        self.level_results.append(level_result)
        return level_result

    def run_full_gauntlet(self) -> Dict:
        """Run all 5 curriculum levels."""
        logger.info("""
╔══════════════════════════════════════════════════════════════════════════════╗
║  THE ULTIMATE TRAINING GAUNTLET BEGINS                                      ║
║  "The market will try to destroy you. Good."                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)

        for level_cfg in CURRICULUM_LEVELS:
            result = self.run_level(level_cfg)
            # Stop if all pairs are failing badly
            if result["avg_win_rate"] < 0.20 and result.get("avg_pnl_pct", 0) < -90:
                logger.warning("⚠️ System struggling — skipping remaining levels")
                break

        return self._generate_final_report()

    def _generate_final_report(self) -> Dict:
        """Generate comprehensive training report."""
        total_elapsed = time.time() - self.training_start

        logger.info("""
╔══════════════════════════════════════════════════════════════════════════════╗
║  GAUNTLET COMPLETE — FINAL INTELLIGENCE REPORT                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)

        # Engine performance across all pairs
        engine_stats = {}
        try:
            global_stats = get_global_rl_stats()
            for pair_data in global_stats.get("per_pair", []):
                for engine_data in pair_data.get("engines", []):
                    ename = engine_data.get("engine", "unknown")
                    if ename not in engine_stats:
                        engine_stats[ename] = []
                    engine_stats[ename].append(engine_data.get("win_rate", 0.5))
        except Exception:
            pass

        # Rank engines
        engine_ranking = []
        for ename, wrs in engine_stats.items():
            engine_ranking.append((ename, np.mean(wrs), np.min(wrs)))
        engine_ranking.sort(key=lambda x: -x[1])

        logger.info(f"\n  🏆 ENGINE INTELLIGENCE RANKING (by win rate):")
        for rank, (ename, avg_wr, min_wr) in enumerate(engine_ranking, 1):
            bar = "█" * int(avg_wr * 30)
            logger.info(f"  #{rank:>2}  {ename:<22} avg={avg_wr:.3f}  min={min_wr:.3f}  {bar}")

        # Curriculum progression
        logger.info(f"\n  📈 CURRICULUM PROGRESSION:")
        for lr in self.level_results:
            trend = "▲" if lr["avg_pnl_pct"] > 0 else "▼"
            logger.info(
                f"  {trend} {lr['level'][:30]:<30} "
                f"WR={lr['avg_win_rate']*100:.1f}% "
                f"PnL={lr['avg_pnl_pct']:+.1f}% "
                f"DD={lr['avg_drawdown']*100:.1f}% "
                f"Events={lr['total_events']}"
            )

        logger.info(f"\n  📊 TOTAL STATS:")
        logger.info(f"     Total Ticks Trained : {self.total_ticks_trained:,}")
        logger.info(f"     Training Time       : {total_elapsed/60:.1f} minutes")
        logger.info(f"     Pairs Trained       : {len(TRAINING_PAIRS)}")
        logger.info(f"     Levels Completed    : {len(self.level_results)}/5")
        logger.info(f"     Engine Cluster      : {CLUSTER_VERSION} engines")

        # Event impact analysis
        logger.info(f"\n  💥 HARDEST EVENTS (average impact on capital):")
        all_event_impacts: Dict[str, List[float]] = defaultdict(list)
        for lr in self.level_results:
            for pr in lr.get("pair_results", []):
                for evt in pr.get("event_impacts", []):
                    all_event_impacts[evt["event"][:40]].append(evt["reward"])

        worst_events = sorted(
            [(name, np.mean(rewards)) for name, rewards in all_event_impacts.items()],
            key=lambda x: x[1]
        )[:10]
        for evt_name, avg_reward in worst_events:
            logger.info(f"     {evt_name[:45]:<45} avg_reward={avg_reward:+.4f}")

        # Final assessment
        last_level = self.level_results[-1] if self.level_results else {}
        final_wr   = last_level.get("avg_win_rate", 0)
        final_dd   = last_level.get("avg_drawdown", 1.0)

        if final_wr >= 0.60 and final_dd < 0.25:
            verdict = "ELITE — Ready for live markets"
        elif final_wr >= 0.52 and final_dd < 0.40:
            verdict = "CAPABLE — Paper trade first, then proceed"
        elif final_wr >= 0.45:
            verdict = "LEARNING — Continue training"
        else:
            verdict = "NEEDS MORE DATA — Run again"

        logger.info(f"\n  ⚖️  FINAL VERDICT: {verdict}")
        logger.info(f"     Win Rate (L5): {final_wr*100:.1f}% | Max DD (L5): {final_dd*100:.1f}%")

        report = {
            "total_ticks":    self.total_ticks_trained,
            "training_min":   round(total_elapsed/60, 1),
            "cluster_engines":CLUSTER_VERSION,
            "levels_done":    len(self.level_results),
            "final_win_rate": final_wr,
            "final_drawdown": final_dd,
            "verdict":        verdict,
            "engine_ranking": engine_ranking,
            "level_results":  self.level_results,
        }

        # Save report to JSON
        report_path = f"training_logs/gauntlet_report_{int(time.time())}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"\n  📄 Full report saved: {report_path}")

        return report


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6: ADVERSARIAL INJECTION MODULE
#  After standard training — fight the adversary
# ══════════════════════════════════════════════════════════════════════════════

class AdversarialTrainer:
    """
    Adversarial training: the market INTENTIONALLY tries to fool each engine.
    Injects:
    1. Fake bull signals before a crash
    2. Fake bear signals before a pump
    3. Wash trading patterns (volume without price movement)
    4. Stop-hunt sequences (spike past S/R then reverse)
    5. Spoofed orderbook (huge bid wall that disappears)
    6. News shock timing: signal right before event reversal
    """

    def __init__(self):
        self.traps_set   = 0
        self.traps_evaded = 0

    def generate_adversarial_sequence(
        self,
        base_price: float,
        n_ticks:    int = 200,
        trap_type:  str = "random",
    ) -> List[Tuple[RLState, float]]:
        """
        Generate a sequence of (state, true_reward) pairs
        where states are designed to mislead.
        Returns: list of (deceptive_state, actual_outcome)
        """
        sequence = []
        price    = base_price
        prices   = [price]

        # Choose trap type
        if trap_type == "random":
            trap_type = random.choice([
                "bull_trap", "bear_trap", "stop_hunt", "wash_trade",
                "spoof_wall", "news_reversal", "liquidity_vacuum"
            ])

        self.traps_set += 1

        for t in range(n_ticks):
            phase = t / n_ticks

            if trap_type == "bull_trap":
                # Phase 1: strong bull signals for 60% of time
                # Phase 2: violent reversal
                if phase < 0.6:
                    drift = 0.0008; ob_bias = 0.5; rsi = 0.72; momentum = 0.7
                else:
                    drift = -0.003; ob_bias = -0.7; rsi = 0.35; momentum = -0.8

            elif trap_type == "bear_trap":
                if phase < 0.6:
                    drift = -0.0008; ob_bias = -0.5; rsi = 0.28; momentum = -0.7
                else:
                    drift = 0.003; ob_bias = 0.7; rsi = 0.65; momentum = 0.8

            elif trap_type == "stop_hunt":
                # Spike above resistance, fill all buy stops, then dump
                if phase < 0.3:
                    drift = 0.001; ob_bias = 0.3
                elif phase < 0.35:
                    drift = 0.005; ob_bias = 0.8   # spike
                else:
                    drift = -0.002; ob_bias = -0.6  # dump
                rsi = 0.5; momentum = 0.0

            elif trap_type == "wash_trade":
                # High volume, zero net price movement, then sudden move
                drift = random.choice([-1,1]) * 0.0001
                ob_bias = random.uniform(-0.8, 0.8)  # rapidly changing
                rsi = 0.5; momentum = 0.0
                if phase > 0.85:  # end: sudden real move
                    drift = random.choice([-1,1]) * 0.003
                    ob_bias = math.copysign(0.7, drift)

            elif trap_type == "spoof_wall":
                # Huge buy wall visible → everyone goes long → wall disappears → dump
                if phase < 0.5:
                    drift = 0.0002; ob_bias = 0.8   # fake bull (wall showing)
                elif phase < 0.55:
                    drift = -0.0001; ob_bias = -0.9  # wall removed, market confused
                else:
                    drift = -0.002; ob_bias = -0.7   # dump (no support)
                rsi = 0.58; momentum = 0.0

            elif trap_type == "news_reversal":
                # Everything looks bullish → news shock reverses everything
                if phase < 0.7:
                    drift = 0.0005; ob_bias = 0.4; rsi = 0.65; momentum = 0.5
                else:
                    drift = -0.005; ob_bias = -0.9; rsi = 0.3; momentum = -0.9

            else:  # liquidity_vacuum
                # Spreads blow out, price gaps, engines can't exit
                drift = random.gauss(0, 0.005)
                ob_bias = 0.0
                rsi = 0.5; momentum = 0.0

            # Update price
            ret   = drift + random.gauss(0, 0.002)
            price = price * (1 + ret)
            price = max(0.001, price)
            prices.append(price)

            # Build deceptive state
            state = RLState(
                price_change_1m  = ret,
                price_change_5m  = (price/prices[max(0,len(prices)-6)]-1) if len(prices)>5 else ret,
                rsi_14           = rsi if 'rsi' in dir() else 0.5,
                rsi_5            = rsi * 1.05 if 'rsi' in dir() else 0.5,
                bb_position      = 0.5 + ob_bias * 0.3,
                ob_imbalance     = ob_bias,
                momentum_score   = momentum if 'momentum' in dir() else 0,
                trade_flow       = ob_bias * 0.8,
                ema_cross        = drift * 100,
                volatility_1m    = abs(ret),
                regime_bull      = 1.0 if drift > 0.0003 else 0.0,
                regime_bear      = 1.0 if drift < -0.0003 else 0.0,
                regime_volatile  = 1.0 if abs(drift) > 0.002 else 0.0,
            )

            # True outcome: opposite of what signals suggest (it's a trap!)
            actual_price_change = (prices[-1] - prices[-2]) / prices[-2] if len(prices) >= 2 else 0
            # Reward = what the market actually did (not what signals suggested)
            true_reward = actual_price_change * 100

            sequence.append((state, true_reward))

        return sequence

    def run_adversarial_session(
        self,
        orchestrators: Dict[str, ExtendedRLOrchestrator],
        n_sequences:   int = 500,
    ):
        """Run adversarial training on all pairs."""
        logger.info(f"\n  🎭 ADVERSARIAL TRAINING — {n_sequences} trap sequences")
        logger.info("  The market will lie. The strong survive.")

        trap_types = ["bull_trap","bear_trap","stop_hunt","wash_trade",
                      "spoof_wall","news_reversal","liquidity_vacuum"]
        evaded = 0

        for i in range(n_sequences):
            pair   = random.choice(list(orchestrators.keys()))
            trap   = random.choice(trap_types)
            orch   = orchestrators[pair]
            seq    = self.generate_adversarial_sequence(1000.0, 100, trap)

            for t, (state, true_reward) in enumerate(seq):
                decision = orch.decide(state)

                # Check if engine was fooled
                direction = decision['direction']
                if direction != 'hold':
                    # Was the engine going WITH the trap or against it?
                    signal_dir = 1 if direction == 'buy' else -1
                    # Engine evaded if it predicted correctly or held
                    if signal_dir * true_reward >= 0 or abs(true_reward) < 0.0005:
                        evaded += 1

                # Feed true reward back
                next_state = seq[t+1][0] if t+1 < len(seq) else state
                orch.record_outcome(true_reward, true_reward*0.01, next_state)

            if (i+1) % 100 == 0:
                evasion_rate = evaded / max(i * len(seq), 1) * 100
                logger.info(f"  Adversarial: {i+1}/{n_sequences} | Trap Evasion: {evasion_rate:.1f}%")

        logger.info(f"  ✅ Adversarial complete: {self.traps_set} traps, evasion training done")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="KRAKEN ULTRA — Ultimate Training Gauntlet")
    parser.add_argument("--levels",    type=str, default="all", help="Levels: all, 1-5, or e.g. '3,4,5'")
    parser.add_argument("--capital",   type=float, default=1000.0, help="Capital per pair (default 1000)")
    parser.add_argument("--no-adversarial", action="store_true", help="Skip adversarial training")
    parser.add_argument("--fast",      action="store_true", help="Reduced ticks for quick test")
    args = parser.parse_args()

    if args.fast:
        for lvl in CURRICULUM_LEVELS:
            lvl["ticks"] = max(1000, lvl["ticks"] // 50)
        logger.info("⚡ FAST MODE: ticks reduced 50×")

    # Parse level selection
    if args.levels == "all":
        levels = CURRICULUM_LEVELS
    else:
        indices = [int(x)-1 for x in args.levels.split(",")]
        levels  = [CURRICULUM_LEVELS[i] for i in indices if 0 <= i < len(CURRICULUM_LEVELS)]

    trainer = GauntletTrainer(capital_per_pair=args.capital)
    trainer.level_results = []

    # Override with selected levels
    original_levels = CURRICULUM_LEVELS.copy()
    import sys

    logger.info(f"🦑 Running {len(levels)} curriculum level(s)")
    for level_cfg in levels:
        trainer.run_level(level_cfg)

    # Adversarial training
    if not args.no_adversarial and trainer.orchestrators:
        adv = AdversarialTrainer()
        n_adv = 200 if not args.fast else 20
        adv.run_adversarial_session(trainer.orchestrators, n_sequences=n_adv)
        save_all_models()

    # Final report
    trainer._generate_final_report()

    logger.info("\n🌌 TRAINING COMPLETE. The engines are forged.")
    logger.info("   Now run the live system. Let them hunt.\n")


if __name__ == "__main__":
    main()
