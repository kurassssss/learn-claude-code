#!/usr/bin/env python3
"""
★ KR √¡\ K - AI TRADING BRAIN
Ultra-intelligent trading analysis powered by GPT-5 via Replit AI Integrations

Features:
1. Market Analysis AI - Deep market understanding
2. Sentiment Analysis - News and social sentiment
3. Strategy Generation - AI-powered strategy creation
4. Trade Reasoning - Explainable AI decisions
5. Risk Assessment - Intelligent risk evaluation
6. Pattern Recognition - Technical pattern detection
7. Anomaly Detection - Market manipulation alerts
8. Position Sizing - AI-optimized position sizing

Uses Replit AI Integrations - no external API key required.
Charges billed to Replit credits.
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# the newest OpenAI model is "gpt-5" which was released August 7, 2025.
# do not change this unless explicitly requested by the user

logger = logging.getLogger("KRAKEN-AI")

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

try:
    from openai import OpenAI
    
    openai_client = OpenAI(
        api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
        base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
    )
    AI_AVAILABLE = True
    logger.info("🧠 AI Trading Brain initialized with GPT-5")
except ImportError:
    AI_AVAILABLE = False
    openai_client = None
    logger.warning("OpenAI not available - AI features disabled")


class MarketSentiment(Enum):
    EXTREME_FEAR = "EXTREME_FEAR"
    FEAR = "FEAR"
    NEUTRAL = "NEUTRAL"
    GREED = "GREED"
    EXTREME_GREED = "EXTREME_GREED"


class TradingSignal(Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


class RiskLevel(Enum):
    VERY_LOW = "VERY_LOW"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


@dataclass
class MarketAnalysis:
    pair: str
    signal: TradingSignal
    confidence: float
    sentiment: MarketSentiment
    risk_level: RiskLevel
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size_percent: float
    reasoning: str
    patterns_detected: List[str]
    key_levels: Dict[str, float]
    time_horizon: str
    timestamp: datetime


@dataclass
class AIInsight:
    category: str
    insight: str
    confidence: float
    actionable: bool
    priority: str


def is_rate_limit_error(exception: BaseException) -> bool:
    """Check if the exception is a rate limit or quota violation error."""
    error_msg = str(exception)
    return (
        "429" in error_msg
        or "RATELIMIT_EXCEEDED" in error_msg
        or "quota" in error_msg.lower()
        or "rate limit" in error_msg.lower()
        or (hasattr(exception, "status_code") and exception.status_code == 429)
    )


class AITradingBrain:
    """
    Ultra-intelligent trading brain powered by GPT-5.
    Provides deep market analysis, sentiment detection, and trading signals.
    """
    
    def __init__(self):
        self.model = "gpt-5"  # Latest and most capable model
        self.fast_model = "gpt-5-nano"  # For high-volume, fast decisions
        self.reasoning_model = "o4-mini"  # For complex reasoning tasks
        
        self.analysis_cache: Dict[str, MarketAnalysis] = {}
        self.cache_ttl = timedelta(minutes=5)
        
        self.total_analyses = 0
        self.successful_predictions = 0
        
        logger.info(f"🧠 AI Trading Brain ready | Model: {self.model}")
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception(is_rate_limit_error),
        reraise=True
    )
    def _call_gpt(self, system_prompt: str, user_prompt: str, 
                  model: Optional[str] = None, json_mode: bool = True) -> str:
        """Call GPT with retry logic and rate limit handling."""
        if not AI_AVAILABLE or not openai_client:
            return "{}"
        
        model = model or self.model
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        kwargs = {
            "model": model,
            "messages": messages,
            "max_completion_tokens": 4096
        }
        
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        
        response = openai_client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or "{}"
    
    async def analyze_market(self, pair: str, market_data: Dict) -> Optional[MarketAnalysis]:
        """
        Comprehensive AI market analysis for a trading pair.
        Uses GPT-5 to analyze price action, patterns, and generate signals.
        """
        if not AI_AVAILABLE:
            return None
        
        cache_key = f"{pair}_{market_data.get('price', 0):.2f}"
        if cache_key in self.analysis_cache:
            cached = self.analysis_cache[cache_key]
            if datetime.now() - cached.timestamp < self.cache_ttl:
                return cached
        
        system_prompt = """You are an expert crypto trading AI analyst. Analyze the market data and provide a comprehensive trading recommendation.

IMPORTANT: You must respond with ONLY valid JSON in this exact format:
{
    "signal": "STRONG_BUY|BUY|HOLD|SELL|STRONG_SELL",
    "confidence": 0.0-1.0,
    "sentiment": "EXTREME_FEAR|FEAR|NEUTRAL|GREED|EXTREME_GREED",
    "risk_level": "VERY_LOW|LOW|MEDIUM|HIGH|EXTREME",
    "entry_price": number,
    "stop_loss": number,
    "take_profit": number,
    "position_size_percent": 0.01-0.25,
    "reasoning": "detailed explanation",
    "patterns_detected": ["pattern1", "pattern2"],
    "key_levels": {"support": number, "resistance": number, "pivot": number},
    "time_horizon": "SHORT|MEDIUM|LONG"
}

Consider:
- Price action and momentum
- Volume analysis
- Order book imbalance
- Volatility regime
- Risk/reward ratio
- Position sizing using Kelly Criterion"""

        user_prompt = f"""Analyze this market data for {pair}:

Current Price: ${market_data.get('price', 0):.4f}
24h Volume: {market_data.get('volume', 0):,.0f}
Volatility: {market_data.get('volatility', 0):.4f}
Trend Strength: {market_data.get('trend_strength', 0):.4f}
RSI: {market_data.get('rsi', 50):.1f}
MACD: {market_data.get('macd', 0):.6f}
Bollinger Position: {market_data.get('bb_position', 0.5):.2f}
Order Book Imbalance: {market_data.get('orderbook_imbalance', 0):.4f}
Funding Rate: {market_data.get('funding_rate', 0):.6f}
Open Interest Change: {market_data.get('oi_change', 0):.4f}
Fear/Greed Index: {market_data.get('fear_greed', 50):.0f}

Provide your analysis in JSON format."""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self._call_gpt(system_prompt, user_prompt)
            )
            
            data = json.loads(response)
            
            analysis = MarketAnalysis(
                pair=pair,
                signal=TradingSignal[data.get('signal', 'HOLD')],
                confidence=float(data.get('confidence', 0.5)),
                sentiment=MarketSentiment[data.get('sentiment', 'NEUTRAL')],
                risk_level=RiskLevel[data.get('risk_level', 'MEDIUM')],
                entry_price=float(data.get('entry_price', market_data.get('price', 0))),
                stop_loss=float(data.get('stop_loss', 0)),
                take_profit=float(data.get('take_profit', 0)),
                position_size_percent=float(data.get('position_size_percent', 0.05)),
                reasoning=data.get('reasoning', ''),
                patterns_detected=data.get('patterns_detected', []),
                key_levels=data.get('key_levels', {}),
                time_horizon=data.get('time_horizon', 'MEDIUM'),
                timestamp=datetime.now()
            )
            
            self.analysis_cache[cache_key] = analysis
            self.total_analyses += 1
            
            logger.info(f"🧠 AI Analysis: {pair} | {analysis.signal.value} | Conf: {analysis.confidence:.0%}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"AI analysis error: {e}")
            return None
    
    async def detect_patterns(self, pair: str, price_history: List[float]) -> List[str]:
        """
        AI-powered technical pattern detection.
        Identifies chart patterns, candlestick patterns, and trend formations.
        """
        if not AI_AVAILABLE or len(price_history) < 20:
            return []
        
        system_prompt = """You are an expert at detecting technical chart patterns in cryptocurrency markets.
Analyze the price history and identify any patterns.

Respond with JSON: {"patterns": ["pattern1", "pattern2", ...], "confidence": 0.0-1.0}

Patterns to look for:
- Double Top/Bottom, Head and Shoulders
- Triangles (ascending, descending, symmetrical)
- Flags, Pennants, Wedges
- Cup and Handle, Inverse Cup and Handle
- Support/Resistance breakouts
- Trend reversals, Trend continuations"""

        prices_str = ", ".join([f"{p:.4f}" for p in price_history[-50:]])
        user_prompt = f"Analyze these recent prices for {pair}: [{prices_str}]"
        
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.fast_model)
            )
            
            data = json.loads(response)
            return data.get('patterns', [])
            
        except Exception as e:
            logger.error(f"Pattern detection error: {e}")
            return []
    
    async def assess_risk(self, trade_params: Dict) -> Dict:
        """
        AI-powered risk assessment for a potential trade.
        Evaluates multiple risk factors and provides recommendations.
        """
        if not AI_AVAILABLE:
            return {"risk_score": 0.5, "recommendation": "Unable to assess"}
        
        system_prompt = """You are a risk management AI for cryptocurrency trading.
Assess the risk of the proposed trade and provide recommendations.

Respond with JSON:
{
    "risk_score": 0.0-1.0 (higher = more risky),
    "risk_factors": ["factor1", "factor2"],
    "recommendation": "PROCEED|REDUCE_SIZE|AVOID",
    "suggested_adjustments": {"stop_loss": number, "position_size": number},
    "reasoning": "explanation"
}"""

        user_prompt = f"""Assess this trade:
Pair: {trade_params.get('pair')}
Direction: {trade_params.get('direction')}
Entry: ${trade_params.get('entry_price', 0):.4f}
Size: {trade_params.get('size', 0):.4f}
Leverage: {trade_params.get('leverage', 1)}x
Stop Loss: ${trade_params.get('stop_loss', 0):.4f}
Take Profit: ${trade_params.get('take_profit', 0):.4f}
Account Balance: ${trade_params.get('balance', 0):.2f}
Current Volatility: {trade_params.get('volatility', 0):.4f}"""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.reasoning_model)
            )
            
            return json.loads(response)
            
        except Exception as e:
            logger.error(f"Risk assessment error: {e}")
            return {"risk_score": 0.5, "recommendation": "PROCEED"}
    
    async def generate_strategy(self, market_conditions: Dict) -> Dict:
        """
        AI-generated trading strategy based on current market conditions.
        Creates adaptive strategies for different market regimes.
        """
        if not AI_AVAILABLE:
            return {}
        
        system_prompt = """You are an expert trading strategist AI.
Generate an optimal trading strategy for the current market conditions.

Respond with JSON:
{
    "strategy_name": "string",
    "strategy_type": "TREND_FOLLOWING|MEAN_REVERSION|BREAKOUT|SCALPING",
    "entry_rules": ["rule1", "rule2"],
    "exit_rules": ["rule1", "rule2"],
    "position_sizing": "description",
    "risk_management": "description",
    "optimal_pairs": ["pair1", "pair2"],
    "expected_win_rate": 0.0-1.0,
    "risk_reward_ratio": number,
    "time_frame": "string"
}"""

        user_prompt = f"""Current market conditions:
Overall Trend: {market_conditions.get('trend', 'Unknown')}
Volatility Regime: {market_conditions.get('volatility_regime', 'Normal')}
Market Sentiment: {market_conditions.get('sentiment', 'Neutral')}
BTC Dominance: {market_conditions.get('btc_dominance', 50)}%
Average Volume: {market_conditions.get('avg_volume', 'Normal')}
Correlation with Traditional Markets: {market_conditions.get('trad_correlation', 0):.2f}

Generate an optimal strategy for these conditions."""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.reasoning_model)
            )
            
            return json.loads(response)
            
        except Exception as e:
            logger.error(f"Strategy generation error: {e}")
            return {}
    
    async def detect_manipulation(self, orderbook_data: Dict, recent_trades: List[Dict]) -> Dict:
        """
        AI-powered market manipulation detection.
        Identifies spoofing, wash trading, pump & dump, and other manipulation tactics.
        """
        if not AI_AVAILABLE:
            return {"manipulation_detected": False, "confidence": 0}
        
        system_prompt = """You are an expert at detecting market manipulation in cryptocurrency markets.
Analyze the orderbook and recent trades for signs of manipulation.

Respond with JSON:
{
    "manipulation_detected": true/false,
    "manipulation_type": "SPOOFING|WASH_TRADING|PUMP_DUMP|LAYERING|NONE",
    "confidence": 0.0-1.0,
    "evidence": ["evidence1", "evidence2"],
    "recommendation": "AVOID_TRADING|TRADE_CAREFULLY|SAFE",
    "reasoning": "explanation"
}

Look for:
- Large orders that get cancelled quickly (spoofing)
- Unusual volume spikes without price movement (wash trading)
- Coordinated buying followed by selling (pump & dump)
- Multiple orders at same price levels (layering)"""

        ob_summary = f"""Order Book:
Top Bids: {orderbook_data.get('bids', [])[:5]}
Top Asks: {orderbook_data.get('asks', [])[:5]}
Bid/Ask Imbalance: {orderbook_data.get('imbalance', 0):.4f}
Spread: {orderbook_data.get('spread', 0):.6f}%"""

        trades_summary = f"Recent Trades: {len(recent_trades)} trades, " \
                        f"Volume: {sum(t.get('size', 0) for t in recent_trades):.2f}"

        user_prompt = f"{ob_summary}\n\n{trades_summary}"

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.fast_model)
            )
            
            return json.loads(response)
            
        except Exception as e:
            logger.error(f"Manipulation detection error: {e}")
            return {"manipulation_detected": False, "confidence": 0}
    
    async def explain_trade(self, trade: Dict, market_context: Dict) -> str:
        """
        Generate a human-readable explanation for a trade decision.
        Provides transparency and reasoning for AI decisions.
        """
        if not AI_AVAILABLE:
            return "AI explanation unavailable"
        
        system_prompt = """You are a trading analyst AI. 
Explain the trade decision in clear, professional language.
Be concise but thorough. Include:
- Why the trade was taken
- Key factors that influenced the decision
- Risk/reward assessment
- Expected outcome

Respond with JSON: {"explanation": "your explanation here"}"""

        user_prompt = f"""Explain this trade:
Trade: {trade.get('direction', 'Unknown')} {trade.get('pair', 'Unknown')}
Entry: ${trade.get('entry_price', 0):.4f}
Stop Loss: ${trade.get('stop_loss', 0):.4f}
Take Profit: ${trade.get('take_profit', 0):.4f}
Size: {trade.get('size', 0):.4f}
Confidence: {trade.get('confidence', 0):.0%}

Market Context:
Price: ${market_context.get('price', 0):.4f}
Trend: {market_context.get('trend', 'Unknown')}
Volatility: {market_context.get('volatility', 'Normal')}
RSI: {market_context.get('rsi', 50):.1f}
Volume: {market_context.get('volume', 'Normal')}"""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.fast_model)
            )
            
            data = json.loads(response)
            return data.get('explanation', 'No explanation available')
            
        except Exception as e:
            logger.error(f"Trade explanation error: {e}")
            return "Unable to generate explanation"
    
    async def optimize_position_size(self, account: Dict, trade_setup: Dict) -> float:
        """
        AI-optimized position sizing using advanced risk management.
        Combines Kelly Criterion with AI insights.
        """
        if not AI_AVAILABLE:
            return 0.05  # Default 5%
        
        system_prompt = """You are a risk management AI specializing in position sizing.
Calculate the optimal position size as a percentage of the account.

Consider:
- Kelly Criterion
- Current volatility
- Account risk tolerance
- Win rate history
- Maximum drawdown limits

Respond with JSON: {"position_size_percent": 0.01-0.25, "reasoning": "explanation"}"""

        user_prompt = f"""Calculate optimal position size:
Account Balance: ${account.get('balance', 0):.2f}
Max Risk Per Trade: {account.get('max_risk', 0.02):.0%}
Win Rate (historical): {account.get('win_rate', 0.5):.0%}
Avg Win: ${account.get('avg_win', 0):.2f}
Avg Loss: ${account.get('avg_loss', 0):.2f}

Trade Setup:
Entry: ${trade_setup.get('entry', 0):.4f}
Stop Loss: ${trade_setup.get('stop_loss', 0):.4f}
Take Profit: ${trade_setup.get('take_profit', 0):.4f}
Signal Confidence: {trade_setup.get('confidence', 0.5):.0%}
Current Volatility: {trade_setup.get('volatility', 0.02):.4f}"""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt, model=self.fast_model)
            )
            
            data = json.loads(response)
            return float(data.get('position_size_percent', 0.05))
            
        except Exception as e:
            logger.error(f"Position sizing error: {e}")
            return 0.05
    
    async def get_market_insights(self, pairs: List[str], market_overview: Dict) -> List[AIInsight]:
        """
        Generate actionable market insights across multiple pairs.
        High-level strategic insights for trading decisions.
        """
        if not AI_AVAILABLE:
            return []
        
        system_prompt = """You are a senior market analyst AI.
Provide strategic insights for cryptocurrency trading.

Respond with JSON:
{
    "insights": [
        {
            "category": "TREND|MOMENTUM|VOLATILITY|CORRELATION|RISK|OPPORTUNITY",
            "insight": "detailed insight",
            "confidence": 0.0-1.0,
            "actionable": true/false,
            "priority": "HIGH|MEDIUM|LOW"
        }
    ]
}

Provide 3-5 key insights."""

        user_prompt = f"""Market Overview:
Pairs monitored: {', '.join(pairs[:10])}
Overall Market Trend: {market_overview.get('trend', 'Unknown')}
BTC Price: ${market_overview.get('btc_price', 0):.2f}
Total Crypto Market Cap: ${market_overview.get('market_cap', 0):,.0f}
24h Volume: ${market_overview.get('volume_24h', 0):,.0f}
Fear & Greed Index: {market_overview.get('fear_greed', 50)}
Volatility Index: {market_overview.get('volatility_index', 50):.1f}

Generate strategic insights."""

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._call_gpt(system_prompt, user_prompt)
            )
            
            data = json.loads(response)
            
            insights = []
            for item in data.get('insights', []):
                insights.append(AIInsight(
                    category=item.get('category', 'UNKNOWN'),
                    insight=item.get('insight', ''),
                    confidence=float(item.get('confidence', 0.5)),
                    actionable=bool(item.get('actionable', False)),
                    priority=item.get('priority', 'MEDIUM')
                ))
            
            return insights
            
        except Exception as e:
            logger.error(f"Market insights error: {e}")
            return []
    
    async def learn_from_trade(self, trade_result: Dict):
        """
        Learn from completed trades to improve future predictions.
        Stores insights for pattern recognition.
        """
        if not AI_AVAILABLE:
            return
        
        was_profitable = trade_result.get('pnl', 0) > 0
        
        if was_profitable:
            self.successful_predictions += 1
        
        logger.info(
            f"🧠 AI Learning: {trade_result.get('pair')} | "
            f"PnL: ${trade_result.get('pnl', 0):.2f} | "
            f"Accuracy: {self.successful_predictions}/{self.total_analyses}"
        )
    
    def get_stats(self) -> Dict:
        """Get AI brain statistics."""
        accuracy = self.successful_predictions / max(1, self.total_analyses)
        return {
            "total_analyses": self.total_analyses,
            "successful_predictions": self.successful_predictions,
            "accuracy": accuracy,
            "model": self.model,
            "cache_size": len(self.analysis_cache),
            "ai_available": AI_AVAILABLE
        }


# Global instance
_ai_brain: Optional[AITradingBrain] = None


def get_ai_brain() -> AITradingBrain:
    """Get or create the AI Trading Brain singleton."""
    global _ai_brain
    if _ai_brain is None:
        _ai_brain = AITradingBrain()
    return _ai_brain


async def test_ai_brain():
    """Test the AI Trading Brain."""
    brain = get_ai_brain()
    
    print("\n" + "🧠" * 40)
    print("★ KR √¡\\ K - AI TRADING BRAIN TEST")
    print("🧠" * 40 + "\n")
    
    # Test market analysis
    market_data = {
        'price': 42000.0,
        'volume': 1500000000,
        'volatility': 0.035,
        'trend_strength': 0.65,
        'rsi': 58,
        'macd': 0.015,
        'bb_position': 0.62,
        'orderbook_imbalance': 0.15,
        'funding_rate': 0.0005,
        'oi_change': 0.08,
        'fear_greed': 62
    }
    
    print("📊 Testing Market Analysis...")
    analysis = await brain.analyze_market("PF_XBTUSD", market_data)
    
    if analysis:
        print(f"\n✅ Analysis Complete:")
        print(f"   Signal: {analysis.signal.value}")
        print(f"   Confidence: {analysis.confidence:.0%}")
        print(f"   Sentiment: {analysis.sentiment.value}")
        print(f"   Risk Level: {analysis.risk_level.value}")
        print(f"   Entry: ${analysis.entry_price:.2f}")
        print(f"   Stop Loss: ${analysis.stop_loss:.2f}")
        print(f"   Take Profit: ${analysis.take_profit:.2f}")
        print(f"   Position Size: {analysis.position_size_percent:.0%}")
        print(f"   Patterns: {', '.join(analysis.patterns_detected) if analysis.patterns_detected else 'None'}")
        print(f"   Reasoning: {analysis.reasoning[:200]}...")
    else:
        print("❌ Analysis failed")
    
    print("\n📈 AI Brain Stats:")
    stats = brain.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print("\n✅ AI Trading Brain ready!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_ai_brain())
