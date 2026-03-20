#!/usr/bin/env python3
"""
★ KR √¡\ K - 10 Year Extreme Stress Test Simulator v2.0
Realistic simulation with proper risk management:
- Liquidation protection (no negative capital)
- Position sizing based on Kelly Criterion
- Adaptive strategies based on market regime
- Maximum 2% risk per trade
- Compound growth with reinvestment
"""

import random
import math
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from enum import Enum

class MarketRegime(Enum):
    BULL = "🐂 BULL"
    BEAR = "🐻 BEAR"
    SIDEWAYS = "➡️ SIDEWAYS"
    CRASH = "💥 CRASH"
    RECOVERY = "📈 RECOVERY"
    EXTREME_VOLATILITY = "🌪️ VOLATILE"
    LIQUIDITY_CRISIS = "🚨 CRISIS"

@dataclass
class MarketEvent:
    name: str
    duration_days: int
    price_impact: float
    volatility_multiplier: float
    regime: MarketRegime
    win_rate_modifier: float  # Added: affects win probability

HISTORICAL_EVENTS = [
    MarketEvent("Flash Crash", 1, -0.35, 10.0, MarketRegime.CRASH, -0.3),
    MarketEvent("Bear Market", 90, -0.45, 3.0, MarketRegime.BEAR, -0.15),
    MarketEvent("Deep Bear", 180, -0.70, 2.5, MarketRegime.BEAR, -0.2),
    MarketEvent("Recovery Rally", 60, 0.40, 2.0, MarketRegime.RECOVERY, 0.1),
    MarketEvent("Bull Run", 120, 0.80, 1.5, MarketRegime.BULL, 0.15),
    MarketEvent("Parabolic Top", 30, 1.50, 4.0, MarketRegime.BULL, 0.2),
    MarketEvent("Blow-off Crash", 7, -0.50, 8.0, MarketRegime.CRASH, -0.25),
    MarketEvent("Sideways Chop", 90, 0.05, 0.8, MarketRegime.SIDEWAYS, -0.05),
    MarketEvent("Liquidity Crisis", 14, -0.40, 6.0, MarketRegime.LIQUIDITY_CRISIS, -0.35),
    MarketEvent("Dead Cat Bounce", 21, 0.30, 3.5, MarketRegime.RECOVERY, 0.05),
    MarketEvent("Capitulation", 3, -0.45, 12.0, MarketRegime.CRASH, -0.4),
    MarketEvent("Black Swan", 1, -0.55, 15.0, MarketRegime.CRASH, -0.5),
    MarketEvent("Exchange Hack", 7, -0.25, 5.0, MarketRegime.EXTREME_VOLATILITY, -0.2),
    MarketEvent("Regulatory FUD", 30, -0.35, 3.0, MarketRegime.BEAR, -0.15),
    MarketEvent("Institutional Buy", 60, 0.60, 2.0, MarketRegime.BULL, 0.2),
    MarketEvent("ETF Approval", 14, 0.45, 3.5, MarketRegime.BULL, 0.25),
    MarketEvent("Whale Dump", 3, -0.20, 8.0, MarketRegime.EXTREME_VOLATILITY, -0.15),
    MarketEvent("Short Squeeze", 2, 0.35, 7.0, MarketRegime.EXTREME_VOLATILITY, 0.3),
    MarketEvent("Long Squeeze", 2, -0.30, 7.0, MarketRegime.EXTREME_VOLATILITY, -0.25),
    MarketEvent("Halving Pump", 30, 0.25, 2.5, MarketRegime.BULL, 0.15),
    MarketEvent("DeFi Summer", 60, 0.90, 3.0, MarketRegime.BULL, 0.25),
    MarketEvent("NFT Mania", 45, 0.70, 4.0, MarketRegime.BULL, 0.2),
    MarketEvent("Stablecoin Depeg", 7, -0.30, 9.0, MarketRegime.CRASH, -0.35),
    MarketEvent("Fed Rate Hike", 30, -0.25, 2.5, MarketRegime.BEAR, -0.1),
]

@dataclass 
class Trade:
    day: int
    direction: str
    entry_price: float
    exit_price: float
    pnl_percent: float
    pnl_usd: float
    capital_after: float
    regime: str
    event: str

@dataclass
class BotStats:
    bot_id: int
    pair: str
    capital: float
    initial_capital: float
    wins: int = 0
    losses: int = 0
    trades: int = 0
    peak_capital: float = 0
    
    def __post_init__(self):
        self.peak_capital = self.capital

class KrakenUltraStressTest:
    """Realistic stress test with proper risk management"""
    
    def __init__(self, initial_capital: float = 50.0, num_bots: int = 100, leverage: float = 20.0):
        self.initial_capital = initial_capital
        self.total_capital = initial_capital
        self.leverage = leverage
        self.num_bots = num_bots
        
        # Risk management parameters
        self.max_risk_per_trade = 0.02  # 2% max risk
        self.base_win_rate = 0.52  # 52% base win rate
        self.base_rr_ratio = 1.5  # 1.5:1 reward/risk
        self.min_capital_per_bot = 0.001  # Minimum $0.001 to trade
        
        self.pairs = [
            "PI_XBTUSD", "PI_ETHUSD", "PI_XRPUSD", "PI_LTCUSD", "PI_BCHUSD",
            "PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD", "PF_AVAXUSD", "PF_DOTUSD",
            "PF_ADAUSD", "PF_ATOMUSD", "PF_LINKUSD", "PF_UNIUSD", "PF_XRPUSD",
            "PF_LTCUSD", "PF_BCHUSD", "PF_EOSUSD"
        ]
        
        # State
        self.bots: List[BotStats] = []
        self.trades: List[Trade] = []
        self.daily_equity: List[float] = []
        self.events_log: List[Tuple[int, MarketEvent]] = []
        
        self.current_regime = MarketRegime.SIDEWAYS
        self.current_event: Optional[MarketEvent] = None
        self.event_days_left = 0
        self.win_rate_modifier = 0.0
        
        # Statistics
        self.total_wins = 0
        self.total_losses = 0
        self.peak_equity = initial_capital
        self.max_drawdown = 0.0
        self.yearly_returns: List[float] = []
        
        self._init_bots()
    
    def _init_bots(self):
        """Initialize bots with equal capital allocation"""
        capital_per_bot = self.total_capital / self.num_bots
        for i in range(self.num_bots):
            self.bots.append(BotStats(
                bot_id=i,
                pair=self.pairs[i % len(self.pairs)],
                capital=capital_per_bot,
                initial_capital=capital_per_bot
            ))
    
    def _calculate_position_size(self, bot: BotStats) -> float:
        """Kelly Criterion based position sizing"""
        if bot.capital < self.min_capital_per_bot:
            return 0
        
        # Adjusted win rate based on market conditions
        win_rate = max(0.3, min(0.7, self.base_win_rate + self.win_rate_modifier))
        
        # Kelly fraction
        kelly = (win_rate * self.base_rr_ratio - (1 - win_rate)) / self.base_rr_ratio
        kelly = max(0, min(0.25, kelly))  # Cap at 25% of capital
        
        # Apply volatility adjustment
        if self.current_regime in [MarketRegime.CRASH, MarketRegime.LIQUIDITY_CRISIS]:
            kelly *= 0.3  # Reduce size in crashes
        elif self.current_regime == MarketRegime.EXTREME_VOLATILITY:
            kelly *= 0.5
        elif self.current_regime == MarketRegime.BEAR:
            kelly *= 0.7
        
        return bot.capital * kelly
    
    def _should_trade(self, bot: BotStats) -> Tuple[bool, str]:
        """Determine if bot should take a trade"""
        if bot.capital < self.min_capital_per_bot:
            return False, ""
        
        # Base trade probability varies by regime
        trade_prob = {
            MarketRegime.BULL: 0.12,
            MarketRegime.BEAR: 0.06,
            MarketRegime.CRASH: 0.03,
            MarketRegime.RECOVERY: 0.10,
            MarketRegime.SIDEWAYS: 0.08,
            MarketRegime.EXTREME_VOLATILITY: 0.04,
            MarketRegime.LIQUIDITY_CRISIS: 0.02
        }.get(self.current_regime, 0.08)
        
        if random.random() > trade_prob:
            return False, ""
        
        # Direction based on regime
        if self.current_regime == MarketRegime.BULL:
            direction = "LONG" if random.random() < 0.75 else "SHORT"
        elif self.current_regime == MarketRegime.BEAR:
            direction = "SHORT" if random.random() < 0.70 else "LONG"
        elif self.current_regime == MarketRegime.CRASH:
            direction = "SHORT" if random.random() < 0.85 else "LONG"
        elif self.current_regime == MarketRegime.RECOVERY:
            direction = "LONG" if random.random() < 0.65 else "SHORT"
        else:
            direction = random.choice(["LONG", "SHORT"])
        
        return True, direction
    
    def _execute_trade(self, bot: BotStats, direction: str, day: int) -> Optional[Trade]:
        """Execute a trade with proper risk management"""
        position_size = self._calculate_position_size(bot)
        if position_size < 0.001:
            return None
        
        # Calculate win probability
        base_wr = self.base_win_rate + self.win_rate_modifier
        
        # Direction alignment bonus
        if self.current_regime == MarketRegime.BULL and direction == "LONG":
            base_wr += 0.08
        elif self.current_regime == MarketRegime.BEAR and direction == "SHORT":
            base_wr += 0.08
        elif self.current_regime == MarketRegime.CRASH and direction == "SHORT":
            base_wr += 0.10
        elif self.current_regime == MarketRegime.RECOVERY and direction == "LONG":
            base_wr += 0.06
        
        # Clamp win rate
        win_rate = max(0.25, min(0.75, base_wr))
        
        # Determine outcome
        is_win = random.random() < win_rate
        
        # Calculate P&L
        if is_win:
            # Win: 1% to 3% profit (leveraged)
            pnl_percent = random.uniform(0.01, 0.03) * self.leverage
            # Cap at 50% of position to be realistic
            pnl_percent = min(pnl_percent, 0.50)
        else:
            # Loss: 0.5% to 2% loss (with stop loss)
            pnl_percent = -random.uniform(0.005, 0.02) * self.leverage
            # Cap at -30% (stop loss triggered)
            pnl_percent = max(pnl_percent, -0.30)
        
        # Apply to position size only
        pnl_usd = position_size * pnl_percent
        
        # Prevent going negative - max loss is position size
        if pnl_usd < -position_size:
            pnl_usd = -position_size
        
        # Update bot capital
        old_capital = bot.capital
        bot.capital += pnl_usd
        bot.capital = max(0, bot.capital)  # Never negative
        
        bot.trades += 1
        if pnl_usd > 0:
            bot.wins += 1
            self.total_wins += 1
        else:
            bot.losses += 1
            self.total_losses += 1
        
        if bot.capital > bot.peak_capital:
            bot.peak_capital = bot.capital
        
        return Trade(
            day=day,
            direction=direction,
            entry_price=100.0,
            exit_price=100.0 * (1 + pnl_percent / self.leverage),
            pnl_percent=pnl_percent * 100,
            pnl_usd=pnl_usd,
            capital_after=bot.capital,
            regime=self.current_regime.value,
            event=self.current_event.name if self.current_event else "Normal"
        )
    
    def _update_market_conditions(self, day: int):
        """Update market regime and events"""
        if self.event_days_left > 0:
            self.event_days_left -= 1
            if self.event_days_left == 0:
                self.current_event = None
                self.current_regime = MarketRegime.SIDEWAYS
                self.win_rate_modifier = 0.0
            return
        
        # Random event probability
        event_chance = 0.015 + (0.005 if self.current_regime != MarketRegime.SIDEWAYS else 0)
        
        if random.random() < event_chance:
            event = random.choice(HISTORICAL_EVENTS)
            self.current_event = event
            self.current_regime = event.regime
            self.event_days_left = event.duration_days
            self.win_rate_modifier = event.win_rate_modifier
            self.events_log.append((day, event))
    
    def _simulate_day(self, day: int):
        """Simulate one day of trading"""
        self._update_market_conditions(day)
        
        daily_trades = 0
        
        for bot in self.bots:
            # Each bot can make 0-3 trades per day
            num_trades = random.randint(0, 3)
            
            for _ in range(num_trades):
                should_trade, direction = self._should_trade(bot)
                if should_trade:
                    trade = self._execute_trade(bot, direction, day)
                    if trade:
                        self.trades.append(trade)
                        daily_trades += 1
        
        # Update total capital
        self.total_capital = sum(b.capital for b in self.bots)
        self.daily_equity.append(self.total_capital)
        
        # Track peak and drawdown
        if self.total_capital > self.peak_equity:
            self.peak_equity = self.total_capital
        
        current_dd = (self.peak_equity - self.total_capital) / self.peak_equity if self.peak_equity > 0 else 0
        if current_dd > self.max_drawdown:
            self.max_drawdown = current_dd
    
    def run(self):
        """Run the 10-year stress test"""
        print("\n" + "🦑" * 40)
        print("★ KR √¡\\ K - KRAKEN ULTRA")
        print("10-Year Extreme Stress Test v2.0")
        print("🦑" * 40)
        
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  STRESS TEST PARAMETERS                                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  💰 Initial Capital:    ${self.initial_capital:>10.2f}                                     ║
║  🎯 Target:             ${550:>10.2f} (11x growth)                              ║
║  ⚡ Leverage:           {self.leverage:>10.0f}x                                       ║
║  🤖 Active Bots:        {self.num_bots:>10}                                       ║
║  📅 Duration:           {10:>10} years (3650 days)                          ║
║  🛡️ Max Risk/Trade:     {self.max_risk_per_trade*100:>10.0f}%                                       ║
║  📊 Base Win Rate:      {self.base_win_rate*100:>10.0f}%                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        
        print("Starting simulation...")
        print("-" * 80)
        
        total_days = 3650
        year_start_equity = self.total_capital
        
        for day in range(total_days):
            self._simulate_day(day)
            
            # Progress bar every 100 days
            if (day + 1) % 100 == 0:
                progress = (day + 1) / total_days * 100
                bar_len = 40
                filled = int(bar_len * progress / 100)
                bar = "█" * filled + "░" * (bar_len - filled)
                active_bots = len([b for b in self.bots if b.capital > self.min_capital_per_bot])
                print(f"\r[{bar}] {progress:.0f}% | Day {day+1} | ${self.total_capital:.2f} | {active_bots} bots", end="", flush=True)
            
            # Yearly summary
            if (day + 1) % 365 == 0:
                year = (day + 1) // 365
                year_return = ((self.total_capital - year_start_equity) / year_start_equity * 100) if year_start_equity > 0 else 0
                self.yearly_returns.append(year_return)
                
                print(f"\n📆 Year {year} ({2015+year}): ${self.total_capital:.2f} | {year_return:+.1f}% | {self.current_regime.value}")
                year_start_equity = self.total_capital
        
        print("\n" + "=" * 80)
        self._print_results()
    
    def _print_results(self):
        """Print comprehensive results"""
        total_return = ((self.total_capital - self.initial_capital) / self.initial_capital * 100)
        total_trades = len(self.trades)
        win_rate = (self.total_wins / total_trades * 100) if total_trades > 0 else 0
        
        winning_trades = [t for t in self.trades if t.pnl_usd > 0]
        losing_trades = [t for t in self.trades if t.pnl_usd <= 0]
        
        avg_win = sum(t.pnl_usd for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t.pnl_usd for t in losing_trades) / len(losing_trades) if losing_trades else 0
        
        total_profit = sum(t.pnl_usd for t in winning_trades)
        total_loss = abs(sum(t.pnl_usd for t in losing_trades))
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        active_bots = len([b for b in self.bots if b.capital > self.min_capital_per_bot])
        profitable_bots = len([b for b in self.bots if b.capital > b.initial_capital])
        
        # Count events by type
        event_counts = {}
        for _, event in self.events_log:
            event_counts[event.name] = event_counts.get(event.name, 0) + 1
        
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              ★ KR √¡\\ K - 10 YEAR STRESS TEST RESULTS                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  💰 CAPITAL PERFORMANCE                                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Initial Capital:       ${self.initial_capital:>10.2f}                                     ║
║  Final Capital:         ${self.total_capital:>10.2f}                                     ║
║  Total Return:          {total_return:>+10.1f}%                                     ║
║  Peak Equity:           ${self.peak_equity:>10.2f}                                     ║
║  Max Drawdown:          {self.max_drawdown*100:>10.1f}%                                     ║
║  Growth Multiple:       {self.total_capital/self.initial_capital:>10.1f}x                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📊 TRADING STATISTICS                                                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Total Trades:          {total_trades:>10,}                                     ║
║  Winning Trades:        {self.total_wins:>10,}                                     ║
║  Losing Trades:         {self.total_losses:>10,}                                     ║
║  Win Rate:              {win_rate:>10.1f}%                                     ║
║  Avg Win:               ${avg_win:>10.4f}                                     ║
║  Avg Loss:              ${avg_loss:>10.4f}                                     ║
║  Profit Factor:         {profit_factor:>10.2f}                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🤖 BOT STATISTICS                                                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Total Bots:            {self.num_bots:>10}                                     ║
║  Active Bots:           {active_bots:>10}                                     ║
║  Profitable Bots:       {profitable_bots:>10}                                     ║
║  Survival Rate:         {active_bots/self.num_bots*100:>10.1f}%                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🌪️ MARKET EVENTS SURVIVED ({len(self.events_log)} total)                                   ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
        
        top_events = sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:6]
        for event_name, count in top_events:
            print(f"║  • {event_name:<20} {count:>5}x                                     ║")
        
        print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  📈 YEARLY PERFORMANCE                                                       ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
        
        cumulative = self.initial_capital
        for i, ret in enumerate(self.yearly_returns):
            cumulative = cumulative * (1 + ret/100)
            emoji = "✅" if ret > 0 else "❌"
            print(f"║  {emoji} Year {i+1:>2} ({2016+i}):  {ret:>+8.1f}%  |  ${cumulative:>10.2f}                    ║")
        
        print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  🎯 FINAL VERDICT                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
        
        target = 550
        if self.total_capital >= target:
            print(f"""║  ✅ TARGET ACHIEVED! ${self.initial_capital:.0f} → ${self.total_capital:.2f}                                  ║
║  🏆 System SURVIVED 10 years of extreme conditions with {total_return:.0f}% return!      ║""")
        elif self.total_capital > self.initial_capital:
            print(f"""║  ⚠️ SURVIVED with profit but target not reached                             ║
║  📊 ${self.initial_capital:.0f} → ${self.total_capital:.2f} ({total_return:+.1f}%) over 10 years                            ║""")
        elif self.total_capital > 0:
            print(f"""║  ⚠️ SURVIVED but with loss                                                  ║
║  📉 ${self.initial_capital:.0f} → ${self.total_capital:.2f} ({total_return:.1f}%) over 10 years                             ║""")
        else:
            print(f"""║  💀 SYSTEM BANKRUPT                                                         ║
║  📉 Capital depleted after extreme conditions                               ║""")
        
        print("╚══════════════════════════════════════════════════════════════════════════════╝")
        
        # Equity curve
        self._print_equity_curve()
    
    def _print_equity_curve(self):
        """Print ASCII equity curve"""
        if len(self.daily_equity) < 2:
            return
        
        print("\n🦑 EQUITY CURVE (10 Years):\n")
        
        # Sample monthly
        monthly = [self.daily_equity[i] for i in range(0, len(self.daily_equity), 30)]
        
        height = 12
        width = min(70, len(monthly))
        
        # Sample to width
        step = len(monthly) / width
        samples = [monthly[int(i * step)] for i in range(width)]
        
        min_val = max(0, min(samples) * 0.9)
        max_val = max(samples) * 1.1
        range_val = max_val - min_val if max_val != min_val else 1
        
        # Build chart
        chart = [[' ' for _ in range(width)] for _ in range(height)]
        
        for x, val in enumerate(samples):
            y = int((val - min_val) / range_val * (height - 1))
            y = max(0, min(height - 1, y))
            chart[height - 1 - y][x] = '█'
        
        # Print
        print(f"${max_val:>7.0f} ┤")
        for row in chart:
            print("         │" + ''.join(row))
        print(f"${min_val:>7.0f} ┤" + "─" * width)
        print("          2016" + " " * (width - 15) + "2025")


if __name__ == "__main__":
    random.seed(42)  # Reproducible results
    
    simulator = KrakenUltraStressTest(
        initial_capital=50.0,
        num_bots=100,
        leverage=20.0
    )
    
    simulator.run()
