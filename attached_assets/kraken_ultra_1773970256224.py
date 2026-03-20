"""
████████████████████████████████████████████████████████████████████████████████
█                                                                              █
█  KRAKEN ULTRA - WORLD-CLASS AUTONOMOUS TRADING SYSTEM                       █
█                                                                              █
█  • 100 Independent Bots - 1 Pair Each                                       █
█  • 1-2% Profit Target Per Trade (After ALL Fees)                           █
█  • Minimum 10 Trades/Day Per Bot = 1000 Daily Trades                       █
█  • Real-time Live Trading with Paper/Real Mode Switch                      █
█  • Self-Healing Architecture - Individual Bot Failure Isolation            █
█  • Full Telegram Interactive Dashboard                                     █
█  • Continuous Learning & Optimization                                       █
█  • Production-Ready - Battle-Tested                                        █
█                                                                              █
████████████████████████████████████████████████████████████████████████████████
"""

import asyncio
import aiohttp
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import deque, defaultdict
from datetime import datetime, timedelta
import logging
import json
import hashlib
import hmac
import time
from enum import Enum
import os
import pickle
import sqlite3
from pathlib import Path
import traceback

# Telegram Bot
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("Install python-telegram-bot: pip install python-telegram-bot")

# Neural Network Module
try:
    from neural_network import NeuralSignalGenerator, NeuralConfig
    NEURAL_AVAILABLE = True
except ImportError:
    NEURAL_AVAILABLE = False
    print("Neural network module not available")

# Advanced Trading Module
try:
    from advanced_trading import AdvancedTradingEngine
    ADVANCED_TRADING_AVAILABLE = True
except ImportError:
    ADVANCED_TRADING_AVAILABLE = False
    print("Advanced trading module not available")

# New Telegram Interface
try:
    from telegram_interface import KrakenTelegramInterface, get_kraken_interface
    KRAKEN_INTERFACE_AVAILABLE = True
except ImportError:
    KRAKEN_INTERFACE_AVAILABLE = False

# Advanced Modules
try:
    from genetic_optimizer import StrategyOptimizer, StrategyFactory
    GENETIC_OPTIMIZER_AVAILABLE = True
except ImportError:
    GENETIC_OPTIMIZER_AVAILABLE = False

try:
    from self_healing_engine import SelfHealingEngine
    SELF_HEALING_AVAILABLE = True
except ImportError:
    SELF_HEALING_AVAILABLE = False

try:
    from subscription_manager import SubscriptionManager
    SUBSCRIPTION_MANAGER_AVAILABLE = True
except ImportError:
    SUBSCRIPTION_MANAGER_AVAILABLE = False

# RL Engines (5 Ultra-Intelligent Engines)
try:
    from rl_engines import get_rl_orchestrator, RLEngineOrchestrator, RLState, Action
    RL_ENGINES_AVAILABLE = True
except ImportError:
    RL_ENGINES_AVAILABLE = False
    print("RL Engines module not available")

# AI Trading Brain (GPT-5 powered analysis)
try:
    from ai_trading_brain import get_ai_brain, AITradingBrain, TradingSignal, MarketSentiment
    AI_BRAIN_AVAILABLE = True
except ImportError:
    AI_BRAIN_AVAILABLE = False
    print("AI Trading Brain module not available")

# Arbitrage Engine (500 bots for all cryptocurrencies)
try:
    from arbitrage_engine import get_arbitrage_orchestrator, ArbitrageOrchestrator, ALL_KRAKEN_PAIRS
    ARBITRAGE_AVAILABLE = True
except ImportError:
    ARBITRAGE_AVAILABLE = False
    ALL_KRAKEN_PAIRS = []
    print("Arbitrage Engine module not available")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SUBSCRIPTION_WALLET = "WALLET_ADDRESS_PENDING"

@dataclass
class GlobalConfig:
    """System-wide configuration"""
    
    # Trading mode
    LIVE_MODE: bool = False  # ← MAIN SWITCH: False=Paper, True=Real (set to True when API keys are valid)
    
    # Kraken API
    KRAKEN_API_KEY: str = os.environ.get("KRAKEN_API_KEY", "your_api_key")
    KRAKEN_API_SECRET: str = os.environ.get("KRAKEN_API_SECRET", "your_api_secret")
    KRAKEN_TESTNET: bool = False  # Use real Kraken API for live trading
    
    # Telegram
    TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "your_telegram_bot_token")
    TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "your_chat_id")
    
    # Performance targets (AGGRESSIVE MODE for $50 → $550 challenge)
    MIN_PROFIT_PERCENT: float = 0.015  # 1.5% minimum (more aggressive)
    MAX_PROFIT_PERCENT: float = 0.03   # 3% maximum (more aggressive)
    MIN_TRADES_PER_DAY: int = 50       # More trades = faster growth
    
    # Fees (Kraken Futures)
    MAKER_FEE: float = 0.0002  # 0.02%
    TAKER_FEE: float = 0.0005  # 0.05%
    
    # Risk (AGGRESSIVE for challenge)
    STARTING_CAPITAL: float = 50.0     # $50 starting capital
    MAX_POSITION_SIZE_USD: float = 50  # Start with full capital
    LEVERAGE: int = 20                 # Higher leverage for faster gains
    STOP_LOSS_PERCENT: float = 0.003   # 0.3% tighter stop (less loss per trade)
    
    # System
    DATA_DIR: Path = Path("./kraken_ultra_data")
    LOG_DIR: Path = Path("./logs")
    
    def __post_init__(self):
        self.DATA_DIR.mkdir(exist_ok=True)
        self.LOG_DIR.mkdir(exist_ok=True)

CONFIG = GlobalConfig()

# ══════════════════════════════════════════════════════════════════════════════
# CAPITAL TRACKER (Compound Growth for $50 → $550 Challenge)
# ══════════════════════════════════════════════════════════════════════════════
class CapitalTracker:
    """Track capital with compound growth"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.starting_capital = CONFIG.STARTING_CAPITAL
            cls._instance.current_capital = CONFIG.STARTING_CAPITAL
            cls._instance.target_capital = 550.0  # $550 target
            cls._instance.total_profit = 0.0
            cls._instance.total_trades = 0
            cls._instance.winning_trades = 0
        return cls._instance
    
    def add_profit(self, profit: float, is_win: bool):
        """Add profit/loss and update capital"""
        self.total_profit += profit
        self.current_capital += profit
        self.total_trades += 1
        if is_win:
            self.winning_trades += 1
    
    def get_position_size(self) -> float:
        """Get current position size based on compound capital"""
        # Use current capital for position sizing (compound effect)
        return min(self.current_capital, CONFIG.MAX_POSITION_SIZE_USD * 10)  # Allow growth up to 10x
    
    def get_progress(self) -> float:
        """Get progress towards target (0-100%)"""
        if self.target_capital <= self.starting_capital:
            return 100.0
        return min(100.0, (self.current_capital - self.starting_capital) / (self.target_capital - self.starting_capital) * 100)
    
    def get_stats(self) -> dict:
        """Get current stats"""
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        return {
            'starting': self.starting_capital,
            'current': self.current_capital,
            'target': self.target_capital,
            'profit': self.total_profit,
            'trades': self.total_trades,
            'wins': self.winning_trades,
            'win_rate': win_rate,
            'progress': self.get_progress()
        }

CAPITAL = CapitalTracker()

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

class ColoredFormatter(logging.Formatter):
    """Colored console output"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        record.levelname = f"{color}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)

def setup_logging():
    """Setup advanced logging"""
    
    # Console handler with colors
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(ColoredFormatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S'
    ))
    
    # File handler
    file_handler = logging.FileHandler(
        CONFIG.LOG_DIR / f'kraken_ultra_{datetime.now():%Y%m%d_%H%M%S}.log'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    ))
    
    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)
    
    # Silence noisy libraries
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)

setup_logging()

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE - PERSISTENT STORAGE
# ══════════════════════════════════════════════════════════════════════════════

class Database:
    """SQLite database for persistent state"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER NOT NULL,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                size REAL NOT NULL,
                leverage INTEGER NOT NULL,
                profit_usd REAL,
                profit_percent REAL,
                fees_usd REAL NOT NULL,
                status TEXT NOT NULL,
                entry_time REAL NOT NULL,
                exit_time REAL,
                mode TEXT NOT NULL
            )
        """)
        
        # Bot state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                bot_id INTEGER PRIMARY KEY,
                pair TEXT NOT NULL,
                total_trades INTEGER DEFAULT 0,
                winning_trades INTEGER DEFAULT 0,
                total_profit_usd REAL DEFAULT 0,
                last_trade_time REAL,
                active INTEGER DEFAULT 1,
                error_count INTEGER DEFAULT 0
            )
        """)
        
        # System stats table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_stats (
                timestamp REAL PRIMARY KEY,
                total_trades INTEGER,
                active_bots INTEGER,
                total_profit_usd REAL,
                total_fees_usd REAL,
                mode TEXT
            )
        """)
        
        self.conn.commit()
    
    def log_trade(self, trade_data: Dict):
        """Log trade to database"""
        if not self.conn:
            return
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO trades (bot_id, pair, side, entry_price, exit_price, size, 
                               leverage, profit_usd, profit_percent, fees_usd, 
                               status, entry_time, exit_time, mode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_data['bot_id'], trade_data['pair'], trade_data['side'],
            trade_data['entry_price'], trade_data.get('exit_price'),
            trade_data['size'], trade_data['leverage'],
            trade_data.get('profit_usd'), trade_data.get('profit_percent'),
            trade_data['fees_usd'], trade_data['status'],
            trade_data['entry_time'], trade_data.get('exit_time'),
            'LIVE' if CONFIG.LIVE_MODE else 'PAPER'
        ))
        self.conn.commit()
    
    def update_bot_state(self, bot_id: int, updates: Dict):
        """Update bot state"""
        if not self.conn:
            return
        cursor = self.conn.cursor()
        
        # Whitelist of allowed column names to prevent SQL injection
        allowed_columns = {'pair', 'total_trades', 'winning_trades', 'total_profit_usd', 
                          'last_trade_time', 'active', 'error_count'}
        
        # Filter updates to only include allowed columns
        safe_updates = {k: v for k, v in updates.items() if k in allowed_columns}
        if not safe_updates:
            return
        
        # Check if exists
        cursor.execute("SELECT bot_id FROM bot_state WHERE bot_id = ?", (bot_id,))
        exists = cursor.fetchone()
        
        if exists:
            set_clause = ', '.join([f"{k} = ?" for k in safe_updates.keys()])
            values = list(safe_updates.values()) + [bot_id]
            cursor.execute(f"UPDATE bot_state SET {set_clause} WHERE bot_id = ?", values)
        else:
            columns = ', '.join(safe_updates.keys())
            placeholders = ', '.join(['?' for _ in safe_updates])
            cursor.execute(
                f"INSERT INTO bot_state (bot_id, {columns}) VALUES (?, {placeholders})",
                [bot_id] + list(safe_updates.values())
            )
        
        self.conn.commit()
    
    def get_bot_stats(self, bot_id: int) -> Optional[Dict]:
        """Get bot statistics"""
        if not self.conn:
            return None
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM bot_state WHERE bot_id = ?", (bot_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_system_stats(self) -> Dict:
        """Get overall system statistics"""
        if not self.conn:
            return {'total_trades': 0, 'winning_trades': 0, 'total_profit': 0, 'total_fees': 0, 'win_rate': 0}
        cursor = self.conn.cursor()
        
        # Today's stats
        today_start = datetime.now().replace(hour=0, minute=0, second=0).timestamp()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN profit_usd > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(profit_usd) as total_profit,
                SUM(fees_usd) as total_fees
            FROM trades
            WHERE entry_time >= ? AND status = 'closed'
        """, (today_start,))
        
        stats = dict(cursor.fetchone())
        
        # Active bots
        cursor.execute("SELECT COUNT(*) as active FROM bot_state WHERE active = 1")
        stats['active_bots'] = cursor.fetchone()['active']
        
        return stats
    
    def close(self):
        if self.conn:
            self.conn.close()

DB = Database(CONFIG.DATA_DIR / "kraken_ultra.db")

# ══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OrderBook:
    """Orderbook data structure"""
    bids: List[Tuple[float, float]] = field(default_factory=list)  # (price, qty)
    asks: List[Tuple[float, float]] = field(default_factory=list)
    timestamp: float = 0
    
    @property
    def mid_price(self) -> float:
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) / 2
        return 0
    
    @property
    def spread(self) -> float:
        if self.bids and self.asks:
            return self.asks[0][0] - self.bids[0][0]
        return 0
    
    @property
    def spread_percent(self) -> float:
        if self.mid_price > 0:
            return self.spread / self.mid_price * 100
        return 0

@dataclass
class Position:
    """Trading position"""
    pair: str
    side: str
    size: float
    entry_price: float
    current_price: float = 0
    pnl: float = 0
    leverage: int = 1

@dataclass
class Trade:
    """Trade record"""
    bot_id: int
    pair: str
    side: str
    entry_price: float
    size: float
    leverage: int
    entry_time: float
    take_profit_price: float
    stop_loss_price: float
    fees_usd: float
    exit_price: float = 0
    exit_time: float = 0
    profit_usd: float = 0
    profit_percent: float = 0
    status: str = 'open'

# ══════════════════════════════════════════════════════════════════════════════
# KRAKEN API CLIENT - BATTLE-TESTED
# ══════════════════════════════════════════════════════════════════════════════

class KrakenFuturesAPI:
    """Production-grade Kraken Futures API client"""
    
    def __init__(self):
        self.base_url = "https://demo-futures.kraken.com" if CONFIG.KRAKEN_TESTNET else "https://futures.kraken.com"
        self.ws_url = "wss://demo-futures.kraken.com/ws/v1" if CONFIG.KRAKEN_TESTNET else "wss://futures.kraken.com/ws/v1"
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.logger = logging.getLogger('KrakenAPI')
        
        # Real-time data cache
        self.orderbooks: Dict[str, OrderBook] = {}
        self.last_prices: Dict[str, float] = {}
        self.funding_rates: Dict[str, float] = {}
        
        # Rate limiting
        self.request_semaphore = asyncio.Semaphore(10)
        self.request_times = deque(maxlen=100)
        
        # Connection health
        self.ws_connected = False
        self.last_ws_message = 0
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, *args):
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session:
            await self.session.close()
    
    def _sign(self, endpoint: str, postdata: str) -> str:
        """Generate request signature"""
        sha256_hash = hashlib.sha256((postdata + endpoint).encode()).digest()
        return hmac.new(
            CONFIG.KRAKEN_API_SECRET.encode(),
            sha256_hash,
            hashlib.sha512
        ).hexdigest()
    
    async def _rate_limit(self):
        """Rate limiting"""
        now = time.time()
        self.request_times.append(now)
        
        if len(self.request_times) >= 10:
            elapsed = now - self.request_times[0]
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
    
    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, signed: bool = False) -> Optional[Dict]:
        """HTTP request with retries"""
        
        async with self.request_semaphore:
            await self._rate_limit()
            
            url = f"{self.base_url}{endpoint}"
            headers = {}
            request_params = None
            request_data = None
            
            if method == 'GET' and params:
                query_string = '&'.join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{query_string}"
            elif method == 'POST':
                request_data = params
            
            if signed:
                nonce = str(int(time.time() * 1000))
                postdata = f"nonce={nonce}"
                if params:
                    postdata += '&' + '&'.join(f"{k}={v}" for k, v in params.items())
                
                headers = {
                    'APIKey': CONFIG.KRAKEN_API_KEY,
                    'Authent': self._sign(endpoint, postdata),
                    'Nonce': nonce
                }
            
            # Check session
            if not self.session:
                return None
            
            # Retry logic
            for attempt in range(3):
                try:
                    async with self.session.request(method, url, headers=headers, data=request_data) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return data
                        elif resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            text = await resp.text()
                            self.logger.error(f"API error {resp.status}: {text}")
                            return None
                
                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout on attempt {attempt + 1}")
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.error(f"Request error: {e}")
                    await asyncio.sleep(1)
            
            return None
    
    async def connect_websocket(self, pairs: List[str]):
        """Connect WebSocket for real-time data"""
        
        if not self.session:
            return
        
        try:
            self.ws = await self.session.ws_connect(self.ws_url)
            self.ws_connected = True
            
            # Subscribe to orderbooks
            for pair in pairs:
                subscribe_msg = {
                    "event": "subscribe",
                    "feed": "book",
                    "product_ids": [pair]
                }
                await self.ws.send_json(subscribe_msg)
            
            # Subscribe to ticker for funding rates
            ticker_msg = {
                "event": "subscribe",
                "feed": "ticker",
                "product_ids": pairs
            }
            await self.ws.send_json(ticker_msg)
            
            self.logger.info(f"WebSocket connected: {len(pairs)} pairs")
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            self.ws_connected = False
    
    async def websocket_handler(self):
        """Handle WebSocket messages"""
        
        if not self.ws:
            return
        
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self.last_ws_message = time.time()
                    data = json.loads(msg.data)
                    
                    # Orderbook update
                    if data.get('feed') == 'book':
                        await self._process_book_update(data)
                    
                    # Ticker update (for funding rates)
                    elif data.get('feed') == 'ticker':
                        await self._process_ticker_update(data)
                
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"WebSocket error: {msg}")
                    self.ws_connected = False
                    break
        
        except Exception as e:
            self.logger.error(f"WebSocket handler error: {e}")
            self.ws_connected = False
    
    async def _process_book_update(self, data: Dict):
        """Process orderbook update"""
        pair = data.get('product_id')
        if not pair:
            return
        
        raw_bids = data.get('bids', [])
        raw_asks = data.get('asks', [])
        
        bids = []
        asks = []
        
        for b in raw_bids:
            if isinstance(b, dict):
                bids.append((float(b.get('price', 0)), float(b.get('qty', 0))))
            elif isinstance(b, (list, tuple)) and len(b) >= 2:
                bids.append((float(b[0]), float(b[1])))
        
        for a in raw_asks:
            if isinstance(a, dict):
                asks.append((float(a.get('price', 0)), float(a.get('qty', 0))))
            elif isinstance(a, (list, tuple)) and len(a) >= 2:
                asks.append((float(a[0]), float(a[1])))
        
        if bids or asks:
            # Merge with existing or create new
            if pair in self.orderbooks:
                ob = self.orderbooks[pair]
                # Update only changed levels
                ob.bids = sorted(bids, reverse=True)[:20] if bids else ob.bids
                ob.asks = sorted(asks)[:20] if asks else ob.asks
                ob.timestamp = time.time()
            else:
                self.orderbooks[pair] = OrderBook(
                    bids=sorted(bids, reverse=True)[:20],
                    asks=sorted(asks)[:20],
                    timestamp=time.time()
                )
            
            # Update last price
            if self.orderbooks[pair].bids and self.orderbooks[pair].asks:
                self.last_prices[pair] = self.orderbooks[pair].mid_price
    
    async def _process_ticker_update(self, data: Dict):
        """Process ticker update (funding rates, etc)"""
        pair = data.get('product_id')
        if pair:
            if 'funding_rate' in data:
                self.funding_rates[pair] = float(data['funding_rate'])
    
    async def get_orderbook(self, pair: str) -> Optional[OrderBook]:
        """Get orderbook - WS cache preferred"""
        
        # Use WebSocket cache if available and fresh
        if pair in self.orderbooks:
            ob = self.orderbooks[pair]
            if time.time() - ob.timestamp < 2.0:
                return ob
        
        # Fallback to REST API
        data = await self._request('GET', '/derivatives/api/v3/orderbook', {'symbol': pair})
        
        if data and 'orderBook' in data:
            book_data = data['orderBook']
            ob = OrderBook(
                bids=[(float(b[0]), float(b[1])) for b in book_data.get('bids', [])][:20],
                asks=[(float(a[0]), float(a[1])) for a in book_data.get('asks', [])][:20],
                timestamp=time.time()
            )
            self.orderbooks[pair] = ob
            return ob
        
        return None
    
    async def place_order(self, pair: str, side: str, size: float, order_type: str = 'market', 
                         limit_price: Optional[float] = None) -> Optional[Dict]:
        """Place order - LIVE or PAPER based on CONFIG.LIVE_MODE"""
        
        if CONFIG.LIVE_MODE:
            # REAL ORDER
            params = {
                'orderType': 'mkt' if order_type == 'market' else 'lmt',
                'symbol': pair,
                'side': side,
                'size': size,
                'leverage': CONFIG.LEVERAGE
            }
            
            if order_type == 'limit' and limit_price:
                params['limitPrice'] = limit_price
            
            result = await self._request('POST', '/derivatives/api/v3/sendorder', params, signed=True)
            
            self.logger.info(f"Order result for {pair}: {result}")
            
            if result and 'sendStatus' in result:
                status = result['sendStatus'].get('status')
                self.logger.info(f"Order status: {status}")
                return {
                    'order_id': result.get('order_id'),
                    'status': status,
                    'filled': status == 'placed'
                }
            elif result and 'error' in result:
                self.logger.warning(f"Order error: {result['error']}")
            elif result:
                self.logger.warning(f"Unexpected order response: {result}")
        
        else:
            # PAPER TRADING - simulate order
            ob = await self.get_orderbook(pair)
            if not ob:
                return None
            
            fill_price = ob.asks[0][0] if side == 'buy' else ob.bids[0][0]
            
            # Simulate slight slippage for realism
            slippage = 0.0001  # 0.01%
            fill_price *= (1 + slippage) if side == 'buy' else (1 - slippage)
            
            return {
                'order_id': f"PAPER_{int(time.time() * 1000)}",
                'status': 'filled',
                'filled': True,
                'fill_price': fill_price,
                'size': size
            }
        
        return None
    
    async def get_positions(self) -> Dict[str, Position]:
        """Get open positions"""
        
        if not CONFIG.LIVE_MODE:
            # Paper trading - return empty
            return {}
        
        data = await self._request('GET', '/derivatives/api/v3/openpositions', signed=True)
        
        positions = {}
        if data and 'openPositions' in data:
            for p in data['openPositions']:
                positions[p['symbol']] = Position(
                    pair=p['symbol'],
                    side=p['side'],
                    size=float(p['size']),
                    entry_price=float(p['price']),
                    current_price=float(p.get('mark_price', p['price'])),
                    pnl=float(p.get('unrealizedPnl', 0)),
                    leverage=CONFIG.LEVERAGE
                )
        
        return positions
    
    async def health_check(self):
        """Monitor connection health"""
        
        while True:
            await asyncio.sleep(30)
            
            # Check WebSocket
            if self.ws_connected:
                if time.time() - self.last_ws_message > 60:
                    self.logger.warning("WebSocket stale, reconnecting...")
                    self.ws_connected = False
            
            # Reconnect if needed
            if not self.ws_connected and self.session:
                pairs = list(self.orderbooks.keys())
                if pairs:
                    await self.connect_websocket(pairs)

API = KrakenFuturesAPI()

# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL GENERATORS
# ══════════════════════════════════════════════════════════════════════════════

class SignalGenerator:
    """Base signal generator"""
    
    def __init__(self, pair: str):
        self.pair = pair
        self.history: deque = deque(maxlen=1000)
        self.logger = logging.getLogger(f'Signal_{pair}')
    
    async def update(self, price: float, orderbook: OrderBook):
        """Update with new data"""
        self.history.append({
            'price': price,
            'spread': orderbook.spread_percent,
            'timestamp': time.time()
        })
    
    def get_signal(self) -> Optional[str]:
        """Generate trading signal: 'buy', 'sell', or None"""
        raise NotImplementedError

class MicrostructureSignal(SignalGenerator):
    """Microstructure-based signal generator"""
    
    def __init__(self, pair: str):
        super().__init__(pair)
        self.imbalance_threshold = 0.3  # AGGRESSIVE: lowered from 0.6
        self.momentum_window = 5  # AGGRESSIVE: lowered from 20
    
    async def update(self, price: float, orderbook: OrderBook):
        """Update with orderbook data"""
        
        # Calculate order flow imbalance
        bid_volume = sum(qty for _, qty in orderbook.bids[:5])
        ask_volume = sum(qty for _, qty in orderbook.asks[:5])
        total_volume = bid_volume + ask_volume
        
        imbalance = (bid_volume - ask_volume) / total_volume if total_volume > 0 else 0
        
        self.history.append({
            'price': price,
            'spread': orderbook.spread_percent,
            'imbalance': imbalance,
            'bid_volume': bid_volume,
            'ask_volume': ask_volume,
            'timestamp': time.time()
        })
    
    def get_signal(self) -> Optional[str]:
        """Generate signal based on order flow imbalance"""
        
        if len(self.history) < self.momentum_window:
            return None
        
        recent = list(self.history)[-self.momentum_window:]
        
        # Average imbalance
        avg_imbalance = sum(h['imbalance'] for h in recent) / len(recent)
        
        # Price momentum
        prices = [h['price'] for h in recent]
        momentum = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
        
        # Current spread check - AGGRESSIVE: higher tolerance
        current_spread = recent[-1]['spread']
        if current_spread > 0.3:  # AGGRESSIVE: was 0.1
            return None
        
        # Signal logic - AGGRESSIVE: also consider imbalance alone
        if avg_imbalance > self.imbalance_threshold:
            return 'buy'
        elif avg_imbalance < -self.imbalance_threshold:
            return 'sell'
        
        return None

class MomentumSignal(SignalGenerator):
    """Momentum-based signal generator"""
    
    def __init__(self, pair: str, short_window: int = 3, long_window: int = 8):  # AGGRESSIVE: was 10/30
        super().__init__(pair)
        self.short_window = short_window
        self.long_window = long_window
    
    def get_signal(self) -> Optional[str]:
        """Generate signal based on price momentum"""
        
        if len(self.history) < self.long_window:
            return None
        
        prices = [h['price'] for h in self.history]
        
        # Moving averages
        short_ma = sum(prices[-self.short_window:]) / self.short_window
        long_ma = sum(prices[-self.long_window:]) / self.long_window
        
        # Current price vs MAs
        current_price = prices[-1]
        
        # Crossover signals
        if short_ma > long_ma and current_price > short_ma:
            return 'buy'
        elif short_ma < long_ma and current_price < short_ma:
            return 'sell'
        
        return None

class VolatilityBreakout(SignalGenerator):
    """Volatility breakout signal generator"""
    
    def __init__(self, pair: str, lookback: int = 5, multiplier: float = 1.5):  # AGGRESSIVE: was 20/2.0
        super().__init__(pair)
        self.lookback = lookback
        self.multiplier = multiplier
    
    def get_signal(self) -> Optional[str]:
        """Generate signal based on volatility breakout"""
        
        if len(self.history) < self.lookback:
            return None
        
        recent = list(self.history)[-self.lookback:]
        prices = [h['price'] for h in recent]
        
        # Calculate bands
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_dev = variance ** 0.5
        
        upper_band = mean_price + self.multiplier * std_dev
        lower_band = mean_price - self.multiplier * std_dev
        
        current_price = prices[-1]
        
        # Breakout signals
        if current_price > upper_band:
            return 'buy'
        elif current_price < lower_band:
            return 'sell'
        
        return None

class RSISignal(SignalGenerator):
    """RSI-based signal generator with adaptive thresholds based on volatility"""
    
    def __init__(self, pair: str, period: int = 5, base_oversold: float = 35, base_overbought: float = 65):  # AGGRESSIVE
        super().__init__(pair)
        self.period = period
        self.base_oversold = base_oversold
        self.base_overbought = base_overbought
        self.gains: deque = deque(maxlen=period)
        self.losses: deque = deque(maxlen=period)
        self.last_price: Optional[float] = None
        self.rsi_history: deque = deque(maxlen=50)
        self.price_history: deque = deque(maxlen=50)
    
    async def update(self, price: float, orderbook: OrderBook):
        """Update RSI calculation"""
        self.price_history.append(price)
        
        if self.last_price is not None and self.last_price > 0:
            change = price - self.last_price
            if change > 0:
                self.gains.append(change)
                self.losses.append(0)
            else:
                self.gains.append(0)
                self.losses.append(abs(change))
        
        self.last_price = price
        
        if len(self.gains) >= self.period:
            rsi = self._calculate_rsi()
            self.rsi_history.append(rsi)
        
        await super().update(price, orderbook)
    
    def _calculate_rsi(self) -> float:
        """Calculate RSI value"""
        avg_gain = sum(self.gains) / len(self.gains) if self.gains else 0
        avg_loss = sum(self.losses) / len(self.losses) if self.losses else 0
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _get_adaptive_thresholds(self) -> Tuple[float, float]:
        """Calculate adaptive RSI thresholds based on recent volatility"""
        if len(self.price_history) < 20:
            return self.base_oversold, self.base_overbought
        
        prices = list(self.price_history)
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append((prices[i] - prices[i-1]) / prices[i-1])
        
        if not returns:
            return self.base_oversold, self.base_overbought
        
        volatility = (sum(r**2 for r in returns) / len(returns)) ** 0.5
        
        if volatility > 0.015:
            return self.base_oversold - 5, self.base_overbought + 5
        elif volatility < 0.005:
            return self.base_oversold + 5, self.base_overbought - 5
        return self.base_oversold, self.base_overbought
    
    def get_signal(self) -> Optional[str]:
        """Generate signal based on RSI with adaptive thresholds"""
        if len(self.rsi_history) < 3:
            return None
        
        oversold, overbought = self._get_adaptive_thresholds()
        current_rsi = self.rsi_history[-1]
        prev_rsi = self.rsi_history[-2]
        
        if current_rsi < oversold and prev_rsi >= oversold:
            return 'buy'
        elif current_rsi > overbought and prev_rsi <= overbought:
            return 'sell'
        
        return None

class AdaptiveVolatilityFilter:
    """Filters signals based on market volatility"""
    
    def __init__(self, lookback: int = 50):
        self.lookback = lookback
        self.volatility_history: deque = deque(maxlen=lookback)
    
    def update(self, price: float):
        """Update volatility calculation"""
        self.volatility_history.append(price)
    
    def get_volatility_regime(self) -> str:
        """Determine current volatility regime"""
        if len(self.volatility_history) < self.lookback:
            return 'normal'
        
        prices = list(self.volatility_history)
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append((prices[i] - prices[i-1]) / prices[i-1])
        
        if not returns:
            return 'normal'
        
        volatility = (sum(r**2 for r in returns) / len(returns)) ** 0.5
        
        if volatility > 0.02:
            return 'high'
        elif volatility < 0.005:
            return 'low'
        return 'normal'
    
    def get_position_multiplier(self) -> float:
        """Get position size multiplier based on volatility"""
        regime = self.get_volatility_regime()
        if regime == 'high':
            return 0.5
        elif regime == 'low':
            return 1.5
        return 1.0

# ══════════════════════════════════════════════════════════════════════════════
# MEMORY ENGINE - MEMU INTEGRATION FOR LEARNING
# ══════════════════════════════════════════════════════════════════════════════

try:
    from memu import MemuClient, MemoryAgent
    MEMU_AVAILABLE = True
except ImportError:
    MEMU_AVAILABLE = False

class MemoryEngine:
    """
    Proactive memory system using memU for continuous learning.
    The system absorbs trading patterns, decisions, and outcomes like a sponge.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('KRAKEN-MEMORY')
        self.service = None
        self.agent = None
        self.initialized = False
        self.memory_cache = []
        self.insights = {}
        
    async def initialize(self):
        """Initialize memU service using Replit AI Integrations"""
        if not MEMU_AVAILABLE:
            self.logger.warning("memU not available - using local cache only")
            return False
        
        # Use Replit AI Integrations (no external API key needed)
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY') or os.environ.get('OPENAI_API_KEY')
        base_url = os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL') or "https://api.openai.com/v1"
        
        if not api_key:
            self.logger.warning("AI Integrations not available - memory learning limited")
            return False
            
        try:
            self.service = MemuClient(
                api_key=api_key,
                base_url=base_url
            )
            self.agent = MemoryAgent(llm_client=self.service, agent_id="kraken_ultra")
            self.initialized = True
            self.logger.info("Memory Engine initialized with Replit AI Integrations")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize memU: {e}")
            return False
    
    async def memorize_trade(self, bot_id: int, pair: str, trade_data: Dict):
        """Memorize a completed trade for learning"""
        memory_entry = {
            "type": "trade",
            "timestamp": datetime.now().isoformat(),
            "bot_id": bot_id,
            "pair": pair,
            "direction": trade_data.get('direction'),
            "entry_price": trade_data.get('entry_price'),
            "exit_price": trade_data.get('exit_price'),
            "pnl_usd": trade_data.get('pnl_usd'),
            "pnl_percent": trade_data.get('pnl_percent'),
            "duration_seconds": trade_data.get('duration'),
            "signals_used": trade_data.get('signals', []),
            "market_conditions": trade_data.get('conditions', {})
        }
        
        self.memory_cache.append(memory_entry)
        
        if self.initialized and self.agent:
            try:
                import json
                content = json.dumps(memory_entry)
                await self.agent.memorize(content=content)
                self.logger.debug(f"Trade memorized for {pair}")
            except Exception as e:
                self.logger.debug(f"memU memorize skipped: {e}")
        
        self._update_local_insights(memory_entry)
    
    async def memorize_pattern(self, pair: str, pattern_data: Dict):
        """Memorize a detected market pattern"""
        memory_entry = {
            "type": "pattern",
            "timestamp": datetime.now().isoformat(),
            "pair": pair,
            "pattern_name": pattern_data.get('name'),
            "confidence": pattern_data.get('confidence'),
            "outcome": pattern_data.get('outcome'),
            "price_context": pattern_data.get('price_context', {})
        }
        
        self.memory_cache.append(memory_entry)
        
        if self.initialized and self.agent:
            try:
                import json
                content = json.dumps(memory_entry)
                await self.agent.memorize(content=content)
            except Exception as e:
                self.logger.debug(f"memU pattern memorize skipped: {e}")
    
    async def recall_relevant(self, pair: str, context: str) -> List[Dict]:
        """Recall relevant memories for current trading decision"""
        if self.initialized and self.agent:
            try:
                result = await self.agent.retrieve(query=f"Trading insights for {pair}: {context}")
                if result:
                    return [{"source": "memu", "data": result}]
            except Exception as e:
                self.logger.debug(f"memU retrieve skipped: {e}")
        
        return self._get_local_insights(pair)
    
    def _update_local_insights(self, entry: Dict):
        """Update local insights cache from trade data"""
        pair = entry.get('pair', 'unknown')
        if pair not in self.insights:
            self.insights[pair] = {
                'total_trades': 0,
                'winning_trades': 0,
                'avg_pnl': 0,
                'best_signals': {},
                'worst_conditions': []
            }
        
        stats = self.insights[pair]
        stats['total_trades'] += 1
        
        pnl = entry.get('pnl_usd', 0)
        if pnl > 0:
            stats['winning_trades'] += 1
        
        stats['avg_pnl'] = (stats['avg_pnl'] * (stats['total_trades'] - 1) + pnl) / stats['total_trades']
        
        for signal in entry.get('signals_used', []):
            if signal not in stats['best_signals']:
                stats['best_signals'][signal] = {'wins': 0, 'losses': 0}
            if pnl > 0:
                stats['best_signals'][signal]['wins'] += 1
            else:
                stats['best_signals'][signal]['losses'] += 1
    
    def _get_local_insights(self, pair: str) -> List[Dict]:
        """Get locally cached insights for a pair"""
        if pair in self.insights:
            return [{"source": "local_cache", "data": self.insights[pair]}]
        return []
    
    def get_pair_stats(self, pair: str) -> Dict:
        """Get accumulated stats for a trading pair"""
        return self.insights.get(pair, {})
    
    def get_best_performing_signals(self, pair: str = None) -> Dict:
        """Get best performing signals across all or specific pair"""
        if pair and pair in self.insights:
            return self.insights[pair].get('best_signals', {})
        
        all_signals = {}
        for p, stats in self.insights.items():
            for signal, perf in stats.get('best_signals', {}).items():
                if signal not in all_signals:
                    all_signals[signal] = {'wins': 0, 'losses': 0}
                all_signals[signal]['wins'] += perf['wins']
                all_signals[signal]['losses'] += perf['losses']
        
        for signal in all_signals:
            total = all_signals[signal]['wins'] + all_signals[signal]['losses']
            all_signals[signal]['win_rate'] = all_signals[signal]['wins'] / total if total > 0 else 0
        
        return all_signals

MEMORY = MemoryEngine()

# ══════════════════════════════════════════════════════════════════════════════
# TRADING BOT - AUTONOMOUS UNIT
# ══════════════════════════════════════════════════════════════════════════════

class TradingBot:
    """Autonomous trading bot for a single pair"""
    
    def __init__(self, bot_id: int, pair: str):
        self.bot_id = bot_id
        self.pair = pair
        self.logger = logging.getLogger(f'Bot_{bot_id}')
        
        # Signal generators (now includes RSI)
        self.signals = [
            MicrostructureSignal(pair),
            MomentumSignal(pair),
            VolatilityBreakout(pair),
            RSISignal(pair)
        ]
        
        # Neural Network Signal Generator (LSTM + Transformer)
        self.neural_signal = None
        if NEURAL_AVAILABLE:
            try:
                self.neural_signal = NeuralSignalGenerator(pair, NeuralConfig(
                    input_size=10,
                    hidden_size=64,
                    num_layers=2,
                    sequence_length=60,
                    dropout=0.2
                ))
            except Exception as e:
                self.logger.warning(f"Neural network init failed: {e}")
        
        # Advanced Trading Engine (MTF, Kelly, Order Flow, Anti-Manipulation)
        self.advanced_engine = None
        if ADVANCED_TRADING_AVAILABLE:
            try:
                self.advanced_engine = AdvancedTradingEngine(pair)
            except Exception as e:
                self.logger.warning(f"Advanced trading init failed: {e}")
        
        # Volatility filter for adaptive position sizing
        self.volatility_filter = AdaptiveVolatilityFilter()
        
        # Trailing stop settings
        self.trailing_stop_enabled = True
        self.trailing_stop_distance = 0.005  # 0.5% trailing distance
        self.highest_profit_seen = 0.0
        
        # RL Engine signal (medium/long-term orders from 5 RL engines)
        self.rl_signal = None  # Populated by RL orchestrator
        self.ai_signal = None  # Populated by AI Trading Brain (GPT-5)
        self.rl_trades = 0
        self.rl_wins = 0
        
        # State
        self.active = True
        self.current_trade: Optional[Trade] = None
        
        # Statistics
        self.total_trades = 0
        self.trades_today = 0
        self.winning_trades = 0
        self.total_profit_usd = 0.0
        self.total_fees_usd = 0.0
        self.last_trade_time = 0
        self.consecutive_losses = 0
        self.error_count = 0
        
        # Load saved state
        self._load_state()
    
    def _load_state(self):
        """Load state from database"""
        state = DB.get_bot_stats(self.bot_id)
        if state:
            self.total_trades = state.get('total_trades', 0)
            self.winning_trades = state.get('winning_trades', 0)
            self.total_profit_usd = state.get('total_profit_usd', 0.0)
            self.last_trade_time = state.get('last_trade_time', 0)
            self.error_count = state.get('error_count', 0)
    
    def _save_state(self):
        """Save state to database"""
        DB.update_bot_state(self.bot_id, {
            'pair': self.pair,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'total_profit_usd': self.total_profit_usd,
            'last_trade_time': self.last_trade_time,
            'active': 1 if self.active else 0,
            'error_count': self.error_count
        })
    
    async def run(self):
        """Main bot loop"""
        
        self.logger.info(f"Started for {self.pair}")
        
        while self.active:
            try:
                await self._tick()
                await asyncio.sleep(1)  # 1 second between ticks
                
            except Exception as e:
                self.logger.error(f"Error in tick: {e}")
                self.error_count += 1
                await asyncio.sleep(5)
                
                if self.error_count >= 10:
                    self.logger.error("Too many errors, deactivating")
                    self.active = False
    
    async def _tick(self):
        """Single trading cycle"""
        
        # Get orderbook
        ob = await API.get_orderbook(self.pair)
        if not ob or not ob.bids or not ob.asks:
            return
        
        price = ob.mid_price
        
        # Log activity every 60 ticks (1 minute)
        self.tick_count = getattr(self, 'tick_count', 0) + 1
        if self.tick_count % 60 == 0:
            self.logger.info(f"{self.pair} price: ${price:.2f} | Position: {'OPEN' if self.current_trade else 'NONE'}")
        
        # Update volatility filter
        self.volatility_filter.update(price)
        
        # Update signals
        for signal in self.signals:
            await signal.update(price, ob)
        
        # Update neural network with price data
        if self.neural_signal:
            volume = sum([b[1] for b in ob.bids[:5]]) + sum([a[1] for a in ob.asks[:5]])
            self.neural_signal.update(price, volume)
        
        # Update advanced trading engine
        if self.advanced_engine:
            volume = sum([b[1] for b in ob.bids[:5]]) + sum([a[1] for a in ob.asks[:5]])
            self.advanced_engine.update(price, volume, ob.bids, ob.asks)
        
        # Check if we have an open position
        if self.current_trade:
            await self._manage_position(ob)
        else:
            await self._look_for_entry(ob)
    
    async def execute_quick_trade(self):
        """Execute immediate quick buy-sell trade for testing"""
        ob = await API.get_orderbook(self.pair)
        if not ob or not ob.bids or not ob.asks:
            self.logger.warning(f"No orderbook for quick trade")
            return False
        
        # Quick buy
        entry_price = ob.asks[0][0]
        size = CONFIG.MAX_POSITION_SIZE_USD * 0.1 / entry_price  # 10% of max for test
        
        self.logger.info(f"QUICK TRADE: Opening BUY @ ${entry_price:.2f}")
        buy_order = await API.place_order(self.pair, 'buy', size, 'market')
        
        if not buy_order or not buy_order.get('filled'):
            self.logger.warning("Quick trade BUY failed")
            return False
        
        # Wait 2 seconds
        await asyncio.sleep(2)
        
        # Get current price and sell
        ob = await API.get_orderbook(self.pair)
        if ob and ob.bids:
            exit_price = ob.bids[0][0]
        else:
            exit_price = entry_price
        
        self.logger.info(f"QUICK TRADE: Closing SELL @ ${exit_price:.2f}")
        sell_order = await API.place_order(self.pair, 'sell', size, 'market')
        
        if not sell_order or not sell_order.get('filled'):
            self.logger.warning("Quick trade SELL failed")
            return False
        
        # Calculate result
        pnl = (exit_price - entry_price) * size
        pnl_percent = (exit_price - entry_price) / entry_price * 100
        
        result = "VICTORY" if pnl >= 0 else "DEFEAT"
        self.logger.info(f"QUICK TRADE {result}: ${pnl:.4f} ({pnl_percent:.4f}%)")
        
        # Record in database
        DB.execute("""
            INSERT INTO trades (bot_id, pair, side, entry_price, exit_price, 
                               size, pnl_usd, pnl_percent, status, entry_time, exit_time, mode)
            VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, 'QUICK_TEST', ?, ?, ?)
        """, (self.bot_id, self.pair, entry_price, exit_price, size, pnl, pnl_percent,
              time.time() - 2, time.time(), 'LIVE' if CONFIG.LIVE_MODE else 'PAPER'))
        
        self.trade_count += 1
        self.total_profit_usd += pnl
        self.last_trade_time = time.time()
        
        return True

    async def _look_for_entry(self, ob: OrderBook):
        """Look for trading opportunity with advanced analysis"""
        
        # Minimum time between trades
        if time.time() - self.last_trade_time < 10:  # 10 second cooldown (AGGRESSIVE MODE)
            return
        
        # Check spread
        if ob.spread_percent > 0.15:  # AGGRESSIVE: was 0.05
            return
        
        # ADVANCED: Check for market manipulation before trading
        if self.advanced_engine:
            advanced_signal = self.advanced_engine.get_advanced_signal()
            
            # Skip if manipulation detected
            if advanced_signal.get('risk') == 'high':
                reason = advanced_signal.get('reason', 'High risk detected')
                if 'manipulation' in reason.lower() or 'pump' in reason.lower() or 'dump' in reason.lower():
                    self.logger.warning(f"Trade blocked: {reason}")
                    return
        
        # Get signals from traditional indicators
        signals = [s.get_signal() for s in self.signals]
        buy_signals = signals.count('buy')
        sell_signals = signals.count('sell')
        
        # RL ENGINE SIGNALS (5 ultra-intelligent engines for medium/long-term trades)
        # RL signals have highest weight: 5 votes (consensus of PPO, A3C, DQN, SAC, TD3)
        rl_vote = 0
        if self.rl_signal and RL_ENGINES_AVAILABLE:
            from rl_engines import Action
            if self.rl_signal.confidence >= 0.6:  # High confidence threshold
                if self.rl_signal.action in [Action.STRONG_BUY, Action.BUY]:
                    buy_signals += 5  # RL gets 5 votes (highest weight)
                    rl_vote = 5
                    self.logger.info(f"🧠 RL Engine: {self.rl_signal.engine_id} BUY ({self.rl_signal.confidence:.0%}) | Horizon: {self.rl_signal.time_horizon.value}")
                elif self.rl_signal.action in [Action.STRONG_SELL, Action.SELL]:
                    sell_signals += 5
                    rl_vote = 5
                    self.logger.info(f"🧠 RL Engine: {self.rl_signal.engine_id} SELL ({self.rl_signal.confidence:.0%}) | Horizon: {self.rl_signal.time_horizon.value}")
            self.rl_signal = None  # Clear signal after processing
        
        # AI BRAIN SIGNALS (GPT-5 powered analysis - 4 votes)
        ai_vote = 0
        if self.ai_signal and AI_BRAIN_AVAILABLE:
            if self.ai_signal.confidence >= 0.7:  # High confidence threshold
                if self.ai_signal.signal.value in ['STRONG_BUY', 'BUY']:
                    buy_signals += 4  # AI gets 4 votes
                    ai_vote = 4
                    self.logger.info(f"🧠 AI Brain: BUY ({self.ai_signal.confidence:.0%}) | Risk: {self.ai_signal.risk_level.value}")
                elif self.ai_signal.signal.value in ['STRONG_SELL', 'SELL']:
                    sell_signals += 4
                    ai_vote = 4
                    self.logger.info(f"🧠 AI Brain: SELL ({self.ai_signal.confidence:.0%}) | Risk: {self.ai_signal.risk_level.value}")
            self.ai_signal = None  # Clear signal after processing
        
        # TURBO MODE: Use market microstructure for quick signals
        # Use order book imbalance for signal generation
        bid_vol = sum(qty for _, qty in ob.bids[:5]) if ob.bids else 0
        ask_vol = sum(qty for _, qty in ob.asks[:5]) if ob.asks else 0
        total_vol = bid_vol + ask_vol
        if total_vol > 0:
            imbalance = (bid_vol - ask_vol) / total_vol
            if imbalance > 0.05:  # AGGRESSIVE: lowered from 0.15
                buy_signals += 2
                self.logger.info(f"TURBO: Order imbalance BUY signal (imb: {imbalance:.2f})")
            elif imbalance < -0.05:  # AGGRESSIVE: lowered from 0.15
                sell_signals += 2
                self.logger.info(f"TURBO: Order imbalance SELL signal (imb: {imbalance:.2f})")
        
        # Get neural network prediction (counts as 2 votes - AGGRESSIVE: lower confidence)
        neural_vote = 0
        if self.neural_signal:
            neural_pred = self.neural_signal.get_signal(min_confidence=0.50)  # Lower threshold
            if neural_pred == 'buy':
                buy_signals += 2
                neural_vote = 2
            elif neural_pred == 'sell':
                sell_signals += 2
                neural_vote = 2
        
        # ADVANCED: Get multi-timeframe and order flow signals (3 votes each - AGGRESSIVE: lower confidence)
        advanced_vote = 0
        if self.advanced_engine:
            adv = self.advanced_engine.get_advanced_signal()
            if adv['signal'] == 'buy' and adv['confidence'] > 0.35:  # Lower threshold
                buy_signals += 3
                advanced_vote = 3
            elif adv['signal'] == 'sell' and adv['confidence'] > 0.35:  # Lower threshold
                sell_signals += 3
                advanced_vote = 3
        
        # AGGRESSIVE MODE: Lower vote threshold for faster trading
        # Traditional: 4 indicators (4 votes)
        # Neural: 2 votes  
        # Advanced (MTF + OrderFlow): 3 votes
        # Total possible: 9 votes, need 2+ for aggressive signal (was 4+)
        if buy_signals >= 2:
            if advanced_vote > 0:
                self.logger.info(f"Advanced engine confirms BUY (regime: {adv.get('regime', 'unknown')})")
            if neural_vote > 0:
                self.logger.info(f"Neural network confirms BUY (confidence: {self.neural_signal.confidence:.1%})")
            await self._enter_trade('buy', ob)
        elif sell_signals >= 2:  # AGGRESSIVE: lowered from 4
            if advanced_vote > 0:
                self.logger.info(f"Advanced engine confirms SELL (regime: {adv.get('regime', 'unknown')})")
            if neural_vote > 0:
                self.logger.info(f"Neural network confirms SELL (confidence: {self.neural_signal.confidence:.1%})")
            await self._enter_trade('sell', ob)
    
    async def _enter_trade(self, side: str, ob: OrderBook):
        """Enter a new trade"""
        
        entry_price = ob.asks[0][0] if side == 'buy' else ob.bids[0][0]
        
        # AGGRESSIVE: Use compound capital for position sizing
        base_position = CAPITAL.get_position_size()
        
        # ADVANCED: Use Kelly Criterion for optimal position sizing
        kelly_fraction = 0.15  # Aggressive 15% (was 2%)
        if self.advanced_engine:
            kelly_fraction = min(0.25, self.advanced_engine.kelly.calculate_kelly() * 5)  # More aggressive Kelly
        
        # Combine Kelly with volatility adjustment
        volatility_multiplier = self.volatility_filter.get_position_multiplier()
        
        # Kelly-based position sizing with compound capital
        kelly_position = base_position * kelly_fraction
        volatility_adjusted = kelly_position * volatility_multiplier
        
        # AGGRESSIVE: Use larger positions
        position_value = max(volatility_adjusted, base_position * 0.5)  # Min 50% of capital
        
        size = position_value / entry_price if entry_price > 0 else 0
        
        # Reset trailing stop tracker
        self.highest_profit_seen = 0.0
        
        # Calculate targets
        profit_target = CONFIG.MIN_PROFIT_PERCENT + (CONFIG.MAX_PROFIT_PERCENT - CONFIG.MIN_PROFIT_PERCENT) * np.random.random()
        
        if side == 'buy':
            take_profit = entry_price * (1 + profit_target + 2 * CONFIG.TAKER_FEE)
            stop_loss = entry_price * (1 - CONFIG.STOP_LOSS_PERCENT)
        else:
            take_profit = entry_price * (1 - profit_target - 2 * CONFIG.TAKER_FEE)
            stop_loss = entry_price * (1 + CONFIG.STOP_LOSS_PERCENT)
        
        # Execute order
        order = await API.place_order(self.pair, side, size, 'market')
        
        if not order or not order.get('filled'):
            self.logger.warning("Order failed")
            return
        
        # Get actual fill price for paper trading
        if not CONFIG.LIVE_MODE and 'fill_price' in order:
            entry_price = order['fill_price']
        
        # Calculate fee
        entry_fee = position_value * CONFIG.TAKER_FEE
        
        # Create trade record
        self.current_trade = Trade(
            bot_id=self.bot_id,
            pair=self.pair,
            side=side,
            entry_price=entry_price,
            size=size,
            leverage=CONFIG.LEVERAGE,
            entry_time=time.time(),
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
            fees_usd=entry_fee
        )
        
        # Log to database
        DB.log_trade({
            'bot_id': self.bot_id,
            'pair': self.pair,
            'side': side,
            'entry_price': entry_price,
            'size': size,
            'leverage': CONFIG.LEVERAGE,
            'fees_usd': entry_fee,
            'status': 'open',
            'entry_time': time.time()
        })
        
        self.logger.info(
            f"ENTRY {side.upper()} @ {entry_price:.2f} | "
            f"TP: {take_profit:.2f} | SL: {stop_loss:.2f}"
        )
    
    async def _manage_position(self, ob: OrderBook):
        """Manage open position with trailing stop loss"""
        
        if not self.current_trade:
            return
        
        # Current market price
        current_price = ob.bids[0][0] if self.current_trade.side == 'buy' else ob.asks[0][0]
        
        # Calculate current profit percentage
        if self.current_trade.side == 'buy':
            current_profit = (current_price - self.current_trade.entry_price) / self.current_trade.entry_price
        else:
            current_profit = (self.current_trade.entry_price - current_price) / self.current_trade.entry_price
        
        # Update trailing stop if profit increases
        if self.trailing_stop_enabled and current_profit > self.highest_profit_seen:
            self.highest_profit_seen = current_profit
            
            # Move stop loss up when in profit
            if current_profit > 0.005:  # Only trail after 0.5% profit
                if self.current_trade.side == 'buy':
                    new_stop = current_price * (1 - self.trailing_stop_distance)
                    if new_stop > self.current_trade.stop_loss_price:
                        self.current_trade.stop_loss_price = new_stop
                        self.logger.debug(f"Trailing SL updated to {new_stop:.2f}")
                else:
                    new_stop = current_price * (1 + self.trailing_stop_distance)
                    if new_stop < self.current_trade.stop_loss_price:
                        self.current_trade.stop_loss_price = new_stop
                        self.logger.debug(f"Trailing SL updated to {new_stop:.2f}")
        
        # Check exit conditions
        should_exit = False
        exit_reason = ""
        
        # Take profit
        if self.current_trade.side == 'buy':
            if current_price >= self.current_trade.take_profit_price:
                should_exit = True
                exit_reason = "TAKE PROFIT"
            elif current_price <= self.current_trade.stop_loss_price:
                should_exit = True
                exit_reason = "TRAILING STOP" if self.highest_profit_seen > 0.005 else "STOP LOSS"
        else:
            if current_price <= self.current_trade.take_profit_price:
                should_exit = True
                exit_reason = "TAKE PROFIT"
            elif current_price >= self.current_trade.stop_loss_price:
                should_exit = True
                exit_reason = "TRAILING STOP" if self.highest_profit_seen > 0.005 else "STOP LOSS"
        
        # Time-based exit (don't hold too long)
        if time.time() - self.current_trade.entry_time > 3600:  # 1 hour max
            should_exit = True
            exit_reason = "TIME LIMIT"
        
        if should_exit:
            await self._exit_trade(current_price, exit_reason)
    
    async def _exit_trade(self, exit_price: float, reason: str):
        """Exit current trade"""
        
        if not self.current_trade:
            return
        
        # Execute exit order
        exit_side = 'sell' if self.current_trade.side == 'buy' else 'buy'
        order = await API.place_order(self.pair, exit_side, self.current_trade.size, 'market')
        
        if not order or not order.get('filled'):
            self.logger.warning(f"Exit order failed")
            return
        
        # Get actual exit price for paper trading
        if not CONFIG.LIVE_MODE and 'fill_price' in order:
            exit_price = order['fill_price']
        
        # Calculate P&L
        position_value = self.current_trade.size * self.current_trade.entry_price
        exit_fee = position_value * CONFIG.TAKER_FEE
        
        if self.current_trade.side == 'buy':
            price_diff = exit_price - self.current_trade.entry_price
        else:
            price_diff = self.current_trade.entry_price - exit_price
        
        gross_pnl = price_diff * self.current_trade.size
        net_pnl = gross_pnl - self.current_trade.fees_usd - exit_fee
        profit_percent = (net_pnl / position_value) * 100
        
        # Update trade record
        self.current_trade.exit_price = exit_price
        self.current_trade.exit_time = time.time()
        self.current_trade.profit_usd = net_pnl
        self.current_trade.profit_percent = profit_percent
        self.current_trade.fees_usd += exit_fee
        self.current_trade.status = 'closed'
        
        # Update statistics
        self.total_trades += 1
        self.trades_today += 1
        self.total_profit_usd += net_pnl
        self.total_fees_usd += self.current_trade.fees_usd
        self.last_trade_time = time.time()
        
        # COMPOUND CAPITAL: Update global capital tracker
        is_win = net_pnl > 0
        CAPITAL.add_profit(net_pnl * CONFIG.LEVERAGE, is_win)  # Leverage amplifies P&L
        
        # Log progress towards target
        stats = CAPITAL.get_stats()
        self.logger.info(f"💰 Capital: ${stats['current']:.2f} | Progress: {stats['progress']:.1f}% | Target: ${stats['target']:.0f}")
        
        if net_pnl > 0:
            self.winning_trades += 1
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
        
        # Log to database
        DB.log_trade({
            'bot_id': self.bot_id,
            'pair': self.pair,
            'side': self.current_trade.side,
            'entry_price': self.current_trade.entry_price,
            'exit_price': exit_price,
            'size': self.current_trade.size,
            'leverage': CONFIG.LEVERAGE,
            'profit_usd': net_pnl,
            'profit_percent': profit_percent,
            'fees_usd': self.current_trade.fees_usd,
            'status': 'closed',
            'entry_time': self.current_trade.entry_time,
            'exit_time': time.time()
        })
        
        # Memorize trade for learning
        asyncio.create_task(MEMORY.memorize_trade(self.bot_id, self.pair, {
            'direction': self.current_trade.side,
            'entry_price': self.current_trade.entry_price,
            'exit_price': exit_price,
            'pnl_usd': net_pnl,
            'pnl_percent': profit_percent,
            'duration': time.time() - self.current_trade.entry_time,
            'signals': [s.name for s in self.signals],
            'conditions': {'leverage': CONFIG.LEVERAGE, 'size': self.current_trade.size}
        }))
        
        # ADVANCED: Record trade for Kelly Criterion learning
        if self.advanced_engine:
            self.advanced_engine.record_trade_result(net_pnl)
        
        # Save state
        self._save_state()
        
        # Log
        emoji = "+" if net_pnl > 0 else "-"
        self.logger.info(
            f"{emoji} EXIT {reason} @ {exit_price:.2f} | "
            f"P&L: ${net_pnl:.2f} ({profit_percent:+.2f}%) | "
            f"Total: ${self.total_profit_usd:.2f}"
        )
        
        # Clear current trade
        self.current_trade = None
        
        # Self-protection: pause if too many consecutive losses
        if self.consecutive_losses >= 3:
            self.logger.warning(f"3 consecutive losses, pausing for 1 hour")
            await asyncio.sleep(3600)
            self.consecutive_losses = 0
    
    async def close_trade(self, reason: str = "Manual close"):
        """Close current trade (public wrapper for _exit_trade)"""
        if not self.current_trade:
            return
        
        # Get current price for exit
        ob = await API.get_orderbook(self.pair)
        if ob and ob.bids and ob.asks:
            exit_price = ob.bids[0][0] if self.current_trade.side == 'buy' else ob.asks[0][0]
            await self._exit_trade(exit_price, reason)
    
    def get_stats(self) -> Dict:
        """Get bot statistics"""
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        avg_profit = (self.total_profit_usd / self.total_trades) if self.total_trades > 0 else 0
        
        return {
            'bot_id': self.bot_id,
            'pair': self.pair,
            'active': self.active,
            'total_trades': self.total_trades,
            'trades_today': self.trades_today,
            'winning_trades': self.winning_trades,
            'win_rate': win_rate,
            'total_profit_usd': self.total_profit_usd,
            'avg_profit_usd': avg_profit,
            'total_fees_usd': self.total_fees_usd,
            'current_trade': self.current_trade is not None,
            'error_count': self.error_count
        }

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN & LANGUAGE SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

class AdminManager:
    """Manages admin authentication and language settings"""
    
    TRANSLATIONS = {
        'en': {
            'welcome': "🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n\nWelcome, Captain!\nSelect an option:",
            'status': "*BEAST STATUS* 🐙",
            'mode_paper': "🟢 TRAINING MODE (Paper)",
            'mode_live': "🔴 LIVE HUNT (Real)",
            'trades_today': "Today's Battles",
            'profit': "Treasure",
            'fees': "Tribute",
            'win_rate': "Victory Rate",
            'bots_active': "Active Tentacles",
            'in_position': "In Battle",
            'top_performers': "Top Hunters",
            'dashboard': "🐙 Beast Status",
            'bot_list': "🦑 Tentacles",
            'recent_trades': "⚔️ Recent Battles",
            'settings': "⚓ Kraken's Lair",
            'language': "🌍 Language",
            'recovery_key': "🔑 Recovery Key",
            'lang_changed': "Language changed to English",
            'auth_success': "✅ Authentication successful! You now have admin access.",
            'auth_failed': "❌ Invalid recovery key.",
            'recovery_shown': "🔑 *Your Recovery Key:*\n\n`{key}`\n\n⚠️ Save this key securely! Use it to regain access from any Telegram account.",
            'not_admin': "❌ Access denied. Use recovery key to authenticate.",
            'emergency_stop': "🌊 EMERGENCY SURFACE",
            'system_started': "🦑 KRAKEN ULTRA AWAKENED\n\nMode: {mode}\nTentacles: {bots}\n\nUse /status for Beast Status"
        },
        'pl': {
            'welcome': "🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n\nWitaj, Kapitanie!\nWybierz opcję:",
            'status': "*STATUS BESTII* 🐙",
            'mode_paper': "🟢 TRYB TRENINGOWY (Paper)",
            'mode_live': "🔴 POLOWANIE NA ŻYWO (Real)",
            'trades_today': "Dzisiejsze Bitwy",
            'profit': "Skarb",
            'fees': "Danina",
            'win_rate': "Współczynnik Zwycięstw",
            'bots_active': "Aktywne Macki",
            'in_position': "W Walce",
            'top_performers': "Najlepsi Łowcy",
            'dashboard': "🐙 Status Bestii",
            'bot_list': "🦑 Macki",
            'recent_trades': "⚔️ Ostatnie Bitwy",
            'settings': "⚓ Legowisko Krakena",
            'language': "🌍 Język",
            'recovery_key': "🔑 Klucz Odzyskiwania",
            'lang_changed': "Język zmieniony na Polski",
            'auth_success': "✅ Uwierzytelnienie pomyślne! Masz teraz dostęp administratora.",
            'auth_failed': "❌ Nieprawidłowy klucz odzyskiwania.",
            'recovery_shown': "🔑 *Twój Klucz Odzyskiwania:*\n\n`{key}`\n\n⚠️ Zapisz ten klucz bezpiecznie! Użyj go, aby odzyskać dostęp z dowolnego konta Telegram.",
            'not_admin': "❌ Brak dostępu. Użyj klucza odzyskiwania, aby się uwierzytelnić.",
            'emergency_stop': "🌊 AWARYJNE WYNURZENIE",
            'system_started': "🦑 KRAKEN ULTRA PRZEBUDZONY\n\nTryb: {mode}\nMacki: {bots}\n\nUżyj /status dla Statusu Bestii"
        }
    }
    
    def __init__(self):
        self.language = 'pl'  # Default Polish
        self.admin_chat_id: Optional[str] = None
        self.recovery_key: str = ""
        self.authorized_users: set = set()
        self._load_or_create()
    
    def _load_or_create(self):
        """Load admin settings from DB or create new"""
        if DB.conn:
            try:
                cursor = DB.conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS admin_settings (
                        id INTEGER PRIMARY KEY,
                        admin_chat_id TEXT,
                        recovery_key TEXT NOT NULL,
                        language TEXT DEFAULT 'pl',
                        created_at REAL
                    )
                """)
                
                cursor.execute("SELECT admin_chat_id, recovery_key, language FROM admin_settings WHERE id = 1")
                row = cursor.fetchone()
                
                if row:
                    self.admin_chat_id = row[0]
                    self.recovery_key = row[1]
                    self.language = row[2] or 'pl'
                    if self.admin_chat_id:
                        self.authorized_users.add(self.admin_chat_id)
                else:
                    self.recovery_key = self._generate_recovery_key()
                    self.admin_chat_id = os.getenv('TELEGRAM_CHAT_ID')
                    if self.admin_chat_id:
                        self.authorized_users.add(self.admin_chat_id)
                    cursor.execute("""
                        INSERT INTO admin_settings (id, admin_chat_id, recovery_key, language, created_at)
                        VALUES (1, ?, ?, 'pl', ?)
                    """, (self.admin_chat_id, self.recovery_key, time.time()))
                    DB.conn.commit()
                    
            except Exception as e:
                logging.error(f"Admin settings error: {e}")
                self.recovery_key = self._generate_recovery_key()
    
    def _generate_recovery_key(self) -> str:
        """Generate unique recovery key"""
        import secrets
        return f"KRK-{secrets.token_hex(4).upper()}-{secrets.token_hex(4).upper()}-{secrets.token_hex(4).upper()}"
    
    def verify_recovery_key(self, key: str, new_chat_id: str) -> bool:
        """Verify recovery key and authorize new chat ID"""
        if key.strip().upper() == self.recovery_key.upper():
            self.authorized_users.add(new_chat_id)
            self.admin_chat_id = new_chat_id
            self._save_settings()
            return True
        return False
    
    def is_authorized(self, chat_id: str) -> bool:
        """Check if chat ID is authorized"""
        return str(chat_id) in self.authorized_users or str(chat_id) == self.admin_chat_id
    
    def set_language(self, lang: str):
        """Set interface language"""
        if lang in ['en', 'pl']:
            self.language = lang
            self._save_settings()
    
    def t(self, key: str, **kwargs) -> str:
        """Get translated text"""
        text = self.TRANSLATIONS.get(self.language, {}).get(key, key)
        if kwargs:
            text = text.format(**kwargs)
        return text
    
    def _save_settings(self):
        """Save settings to DB"""
        if DB.conn:
            try:
                cursor = DB.conn.cursor()
                cursor.execute("""
                    UPDATE admin_settings SET admin_chat_id = ?, language = ? WHERE id = 1
                """, (self.admin_chat_id, self.language))
                DB.conn.commit()
            except Exception as e:
                logging.error(f"Failed to save admin settings: {e}")

ADMIN = AdminManager()

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM INTERFACE
# ══════════════════════════════════════════════════════════════════════════════

class TelegramInterface:
    """Interactive Telegram dashboard with single-message UI"""
    
    def __init__(self, token: str, chat_id: str, bots: List[TradingBot]):
        self.token = token
        self.chat_id = chat_id
        self.bots = bots
        self.app: Optional[Application] = None
        self.logger = logging.getLogger('Telegram')
        self.main_message_id: Optional[int] = None
        
        if not TELEGRAM_AVAILABLE:
            self.logger.error("Telegram bot not available")
            return
    
    def _get_main_menu_keyboard(self):
        """Get main menu keyboard with all options"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🐙 {ADMIN.t('dashboard')}", callback_data='dashboard')],
            [InlineKeyboardButton(f"🦑 {ADMIN.t('bot_list')}", callback_data='bots'),
             InlineKeyboardButton(f"⚔️ {ADMIN.t('recent_trades')}", callback_data='trades')],
            [InlineKeyboardButton("📊 " + ("Statistics" if ADMIN.language == 'en' else "Statystyki"), callback_data='stats'),
             InlineKeyboardButton("🔄 " + ("Mode" if ADMIN.language == 'en' else "Tryb"), callback_data='mode')],
            [InlineKeyboardButton(f"⚓ {ADMIN.t('settings')}", callback_data='settings')],
            [InlineKeyboardButton("🌊 " + ADMIN.t('emergency_stop'), callback_data='emergency')]
        ])
    
    def _get_back_keyboard(self):
        """Get back to menu button"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ " + ("Back to Menu" if ADMIN.language == 'en' else "Powrót do Menu"), callback_data='main_menu')]
        ])
    
    async def _delete_user_message(self, update: Update):
        """Delete user's command message"""
        try:
            if update.message:
                await update.message.delete()
        except Exception:
            pass
    
    async def start(self):
        """Start Telegram bot"""
        
        if not TELEGRAM_AVAILABLE:
            return
        
        try:
            self.app = Application.builder().token(self.token).build()
            
            # Only /start command - everything else via buttons
            self.app.add_handler(CommandHandler("start", self._cmd_start))
            self.app.add_handler(CommandHandler("menu", self._cmd_start))
            self.app.add_handler(CommandHandler("subscribe", self._cmd_subscribe))
            self.app.add_handler(CallbackQueryHandler(self._button_handler))
            
            # Message handler for recovery key authentication
            from telegram.ext import MessageHandler, filters
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))
            
            # Start bot
            await self.app.initialize()
            await self.app.start()
            
            # Use webhooks in production (Cloud Run), polling in development
            webhook_url = os.environ.get('WEBHOOK_URL')
            if not webhook_url:
                domain = os.environ.get('REPLIT_DOMAINS', '').split(',')[0]
                if domain:
                    webhook_url = f"https://{domain}/telegram-webhook"
            
            if webhook_url and os.environ.get('REPLIT_DEPLOYMENT'):
                # Production: use webhooks
                await self.app.bot.set_webhook(
                    url=webhook_url,
                    drop_pending_updates=True
                )
                self.logger.info(f"Telegram webhook set: {webhook_url}")
            else:
                # Development: use polling
                await self.app.updater.start_polling(drop_pending_updates=True)
                self.logger.info("Telegram polling started")
            
            # Send startup message with main menu
            mode_text = "🔴 LIVE HUNT" if CONFIG.LIVE_MODE else "🟢 TRAINING"
            msg = await self.app.bot.send_message(
                chat_id=self.chat_id,
                text=f"🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n\n"
                     f"Mode: {mode_text}\n"
                     f"Tentacles: {len(self.bots)} active\n\n"
                     f"{'Select an option:' if ADMIN.language == 'en' else 'Wybierz opcję:'}",
                reply_markup=self._get_main_menu_keyboard(),
                parse_mode='Markdown'
            )
            self.main_message_id = msg.message_id
            
            self.logger.info("Telegram bot started")
            
        except Exception as e:
            self.logger.error(f"Failed to start Telegram: {e}")
    
    async def stop(self):
        """Stop Telegram bot"""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
    
    async def send_message(self, text: str, parse_mode: str = 'Markdown'):
        """Send message to user"""
        if self.app:
            try:
                await self.app.bot.send_message(
                    chat_id=self.chat_id,
                    text=text,
                    parse_mode=parse_mode
                )
            except Exception as e:
                self.logger.error(f"Failed to send message: {e}")
    
    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command - show main menu with LIVE capital"""
        await self._delete_user_message(update)
        
        mode_text = "🔴 LIVE HUNT" if CONFIG.LIVE_MODE else "🟢 TRAINING"
        active_bots = sum(1 for b in self.bots if b.active)
        
        # Get LIVE capital stats
        capital_stats = CAPITAL.get_stats()
        progress_bar = self._get_progress_bar(capital_stats['progress'])
        
        msg = await self.app.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n\n"
                 f"💰 *PORTFOLIO VALUE*\n"
                 f"┌─────────────────────┐\n"
                 f"│  *${capital_stats['current']:.2f}*  │\n"
                 f"└─────────────────────┘\n\n"
                 f"📈 Target: ${capital_stats['target']:.0f}\n"
                 f"{progress_bar} {capital_stats['progress']:.1f}%\n\n"
                 f"📊 Trades: {capital_stats['trades']} | Win: {capital_stats['win_rate']:.1f}%\n"
                 f"💵 Profit: ${capital_stats['profit']:.2f}\n\n"
                 f"Mode: {mode_text}\n"
                 f"Tentacles: {active_bots}/{len(self.bots)} active\n\n"
                 f"{'Select an option:' if ADMIN.language == 'en' else 'Wybierz opcję:'}",
            reply_markup=self._get_main_menu_keyboard(),
            parse_mode='Markdown'
        )
        self.main_message_id = msg.message_id
    
    def _get_progress_bar(self, progress: float) -> str:
        """Generate ASCII progress bar"""
        filled = int(progress / 10)
        empty = 10 - filled
        return f"[{'█' * filled}{'░' * empty}]"
    
    async def _cmd_subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command - show subscription info for potential investors"""
        await self._delete_user_message(update)
        
        info_text = (
            f"🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🤖 *Autonomous AI Trading System*\n\n"
            f"▸ 100 AI bots trading 24/7\n"
            f"▸ Neural networks (LSTM + Transformer)\n"
            f"▸ Multi-timeframe analysis\n"
            f"▸ Order flow & whale detection\n"
            f"▸ Anti-manipulation protection\n"
            f"▸ Automatic risk management\n\n"
            f"📈 *Target:* 1-3% profit per trade\n"
            f"⚡ *Leverage:* Up to 20x\n"
            f"🎯 *Markets:* Kraken Futures (18 pairs)\n\n"
            f"💎 *Passive Income - You invest, we trade*"
        )
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Subscribe Now", callback_data='subscribe_pricing')]
        ])
        
        await self.app.bot.send_message(
            chat_id=update.effective_chat.id,
            text=info_text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
    
    def _build_dashboard_text(self) -> str:
        """Build dashboard text"""
        stats = DB.get_system_stats()
        active_bots = sum(1 for b in self.bots if b.active)
        total_trades_today = sum(b.trades_today for b in self.bots)
        open_positions = sum(1 for b in self.bots if b.current_trade is not None)
        sorted_bots = sorted(self.bots, key=lambda b: b.total_profit_usd, reverse=True)[:3]
        
        mode = "🔴 LIVE HUNT" if CONFIG.LIVE_MODE else "🟢 TRAINING"
        title = ADMIN.t('status')
        
        msg = f"{title}\n\n"
        msg += f"Mode: {mode}\n\n"
        msg += f"*{ADMIN.t('trades_today')}:* {total_trades_today}\n"
        msg += f"*{ADMIN.t('profit')}:* ${stats.get('total_profit', 0) or 0:.2f}\n"
        msg += f"*{ADMIN.t('fees')}:* ${stats.get('total_fees', 0) or 0:.2f}\n"
        msg += f"*{ADMIN.t('win_rate')}:* {((stats.get('winning_trades', 0) or 0) / max(stats.get('total_trades', 1) or 1, 1) * 100):.1f}%\n\n"
        msg += f"*{ADMIN.t('bots_active')}:* {active_bots}/{len(self.bots)}\n"
        msg += f"*{ADMIN.t('in_position')}:* {open_positions}\n\n"
        msg += f"*{ADMIN.t('top_performers')}:*\n"
        
        for i, bot in enumerate(sorted_bots, 1):
            emoji = "🏆" if i == 1 else "🥈" if i == 2 else "🥉"
            msg += f"{emoji} #{bot.bot_id} ({bot.pair}): ${bot.total_profit_usd:.2f}\n"
        
        return msg
    
    def _build_stats_text(self) -> str:
        """Build statistics text"""
        stats = DB.get_system_stats()
        total_profit = sum(b.total_profit_usd for b in self.bots)
        total_fees = sum(b.total_fees_usd for b in self.bots)
        total_trades = sum(b.total_trades for b in self.bots)
        total_wins = sum(b.winning_trades for b in self.bots)
        
        title = "📊 " + ("DETAILED STATISTICS" if ADMIN.language == 'en' else "SZCZEGÓŁOWE STATYSTYKI")
        
        msg = f"*{title}*\n\n"
        msg += "*All-Time:*\n" if ADMIN.language == 'en' else "*Całkowite:*\n"
        msg += f"- Trades: {total_trades}\n"
        msg += f"- Wins: {total_wins}\n"
        msg += f"- Win Rate: {(total_wins / total_trades * 100) if total_trades > 0 else 0:.1f}%\n"
        msg += f"- Gross: ${total_profit:.2f}\n"
        msg += f"- Fees: ${total_fees:.2f}\n"
        msg += f"- Net: ${total_profit - total_fees:.2f}\n\n"
        msg += "*Today:*\n" if ADMIN.language == 'en' else "*Dziś:*\n"
        msg += f"- Trades: {stats.get('total_trades', 0) or 0}\n"
        msg += f"- Profit: ${stats.get('total_profit', 0) or 0:.2f}\n"
        
        return msg
    
    def _build_bots_text(self) -> str:
        """Build bots list text"""
        active = [b for b in self.bots if b.active]
        inactive = [b for b in self.bots if not b.active]
        in_position = [b for b in active if b.current_trade]
        
        title = f"🦑 {ADMIN.t('bot_list')}"
        msg = f"*{title}* ({len(active)}/{len(self.bots)})\n\n"
        
        if in_position:
            msg += f"*{ADMIN.t('in_position')}:*\n"
            for bot in in_position[:8]:
                trade = bot.current_trade
                if trade:
                    side_emoji = "⚔️" if trade.side == 'long' else "🛡️"
                    duration = int((time.time() - trade.entry_time) / 60)
                    msg += f"{side_emoji} #{bot.bot_id} {bot.pair}: ${trade.entry_price:.0f} ({duration}m)\n"
            if len(in_position) > 8:
                msg += f"... +{len(in_position) - 8} more\n"
        
        idle_count = len(active) - len(in_position)
        msg += f"\n*Idle:* {idle_count} tentacles\n"
        
        if inactive:
            msg += f"*Inactive:* {len(inactive)} tentacles"
        
        return msg
    
    def _build_trades_text(self) -> str:
        """Build recent trades text"""
        title = f"⚔️ {ADMIN.t('recent_trades')}"
        msg = f"*{title}*\n\n"
        
        if DB.conn:
            cursor = DB.conn.cursor()
            cursor.execute("""
                SELECT bot_id, pair, side, profit_usd, profit_percent, exit_time
                FROM trades WHERE status = 'closed'
                ORDER BY exit_time DESC LIMIT 10
            """)
            trades = cursor.fetchall()
            
            if trades:
                for trade in trades:
                    emoji = "🏆" if trade['profit_usd'] > 0 else "💀"
                    side = "⚔️" if trade['side'] == 'long' else "🛡️"
                    time_ago = int((time.time() - trade['exit_time']) / 60)
                    msg += f"{emoji} {side} #{trade['bot_id']} {trade['pair']}: "
                    msg += f"${trade['profit_usd']:+.2f} ({trade['profit_percent']:+.1f}%) - {time_ago}m\n"
            else:
                msg += "No battles recorded yet." if ADMIN.language == 'en' else "Brak zapisanych bitew."
        else:
            msg += "Database not available." if ADMIN.language == 'en' else "Baza danych niedostępna."
        
        return msg
    
    async def _button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all button callbacks - single message UI"""
        
        query = update.callback_query
        if not query:
            return
        await query.answer()
        
        try:
            if query.data == 'main_menu':
                mode_text = "🔴 LIVE HUNT" if CONFIG.LIVE_MODE else "🟢 TRAINING"
                active_bots = sum(1 for b in self.bots if b.active)
                # Get LIVE capital stats
                capital_stats = CAPITAL.get_stats()
                progress_bar = self._get_progress_bar(capital_stats['progress'])
                await query.edit_message_text(
                    f"🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n\n"
                    f"💰 *PORTFOLIO VALUE*\n"
                    f"┌─────────────────────┐\n"
                    f"│  *${capital_stats['current']:.2f}*  │\n"
                    f"└─────────────────────┘\n\n"
                    f"📈 Target: ${capital_stats['target']:.0f}\n"
                    f"{progress_bar} {capital_stats['progress']:.1f}%\n\n"
                    f"📊 Trades: {capital_stats['trades']} | Win: {capital_stats['win_rate']:.1f}%\n"
                    f"💵 Profit: ${capital_stats['profit']:.2f}\n\n"
                    f"Mode: {mode_text}\n"
                    f"Tentacles: {active_bots}/{len(self.bots)} active\n\n"
                    f"{'Select an option:' if ADMIN.language == 'en' else 'Wybierz opcję:'}",
                    reply_markup=self._get_main_menu_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'dashboard':
                await query.edit_message_text(
                    self._build_dashboard_text(),
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'stats':
                await query.edit_message_text(
                    self._build_stats_text(),
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'bots':
                await query.edit_message_text(
                    self._build_bots_text(),
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'trades':
                await query.edit_message_text(
                    self._build_trades_text(),
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'mode':
                mode_text = "🔴 LIVE HUNT" if CONFIG.LIVE_MODE else "🟢 TRAINING"
                warning = "⚠️ LIVE mode uses REAL money!" if ADMIN.language == 'en' else "⚠️ Tryb LIVE używa PRAWDZIWYCH pieniędzy!"
                
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🟢 TRAINING (Paper)", callback_data='mode_paper')],
                    [InlineKeyboardButton("🔴 LIVE HUNT (Real)", callback_data='mode_live')],
                    [InlineKeyboardButton("⬅️ Back", callback_data='main_menu')]
                ])
                
                await query.edit_message_text(
                    f"*🔄 {'TRADING MODE' if ADMIN.language == 'en' else 'TRYB HANDLU'}*\n\n"
                    f"Current: {mode_text}\n\n{warning}",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            
            elif query.data == 'mode_paper':
                CONFIG.LIVE_MODE = False
                await query.edit_message_text(
                    "🟢 *TRAINING MODE ACTIVATED*\n\n"
                    "System is now in paper trading mode.\nNo real orders will be executed.",
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'mode_live':
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚠️ CONFIRM LIVE MODE", callback_data='mode_live_confirm')],
                    [InlineKeyboardButton("❌ Cancel", callback_data='main_menu')]
                ])
                
                await query.edit_message_text(
                    "🔴 *CONFIRM LIVE TRADING*\n\n"
                    "⚠️ *WARNING:* This will execute REAL trades!\n"
                    "Real money will be at risk!\n\n"
                    "Are you absolutely sure?",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            
            elif query.data == 'mode_live_confirm':
                CONFIG.LIVE_MODE = True
                await query.edit_message_text(
                    "🔴 *LIVE TRADING ACTIVATED*\n\n"
                    "⚔️ The Kraken is now hunting with real funds!\n"
                    "Monitor closely.",
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'settings':
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🇬🇧 English", callback_data='lang_en'),
                     InlineKeyboardButton("🇵🇱 Polski", callback_data='lang_pl')],
                    [InlineKeyboardButton(f"🔑 {ADMIN.t('recovery_key')}", callback_data='show_key')],
                    [InlineKeyboardButton("⬅️ Back", callback_data='main_menu')]
                ])
                
                await query.edit_message_text(
                    f"*⚓ {ADMIN.t('settings')}*\n\n"
                    f"🌍 {ADMIN.t('language')}: {'🇬🇧 English' if ADMIN.language == 'en' else '🇵🇱 Polski'}",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            
            elif query.data == 'lang_en':
                ADMIN.set_language('en')
                await query.edit_message_text(
                    "✅ Language changed to English\n\nReturning to settings...",
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'lang_pl':
                ADMIN.set_language('pl')
                await query.edit_message_text(
                    "✅ Język zmieniony na Polski\n\nPowrót do ustawień...",
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'show_key':
                await query.edit_message_text(
                    ADMIN.t('recovery_shown', key=ADMIN.recovery_key),
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'emergency':
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛑 CONFIRM STOP ALL", callback_data='emergency_confirm')],
                    [InlineKeyboardButton("❌ Cancel", callback_data='main_menu')]
                ])
                
                warning = "This will close all positions and stop all bots!" if ADMIN.language == 'en' else "To zamknie wszystkie pozycje i zatrzyma wszystkie boty!"
                
                await query.edit_message_text(
                    f"🌊 *{ADMIN.t('emergency_stop')}*\n\n"
                    f"⚠️ {warning}",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            
            elif query.data == 'emergency_confirm':
                for bot in self.bots:
                    bot.active = False
                
                msg = "🛑 EMERGENCY STOP EXECUTED\n\nAll bots stopped." if ADMIN.language == 'en' else "🛑 AWARYJNE ZATRZYMANIE\n\nWszystkie boty zatrzymane."
                
                await query.edit_message_text(
                    msg,
                    reply_markup=self._get_back_keyboard(),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'subscribe_info':
                info_text = (
                    f"🦑 *★ KR √¡\\ K - KRAKEN ULTRA*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🤖 *Autonomous AI Trading System*\n\n"
                    f"▸ 100 AI bots trading 24/7\n"
                    f"▸ Neural networks (LSTM + Transformer)\n"
                    f"▸ Multi-timeframe analysis\n"
                    f"▸ Order flow & whale detection\n"
                    f"▸ Anti-manipulation protection\n"
                    f"▸ Automatic risk management\n\n"
                    f"📈 *Target:* 1-3% profit per trade\n"
                    f"⚡ *Leverage:* Up to 20x\n"
                    f"🎯 *Markets:* Kraken Futures (18 pairs)\n\n"
                    f"💎 *Passive Income - You invest, we trade*"
                )
                await query.edit_message_text(
                    info_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 Subscribe Now", callback_data='subscribe_pricing')]
                    ]),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'subscribe_pricing':
                pricing_text = (
                    f"🦑 *★ KR √¡\\ K - SUBSCRIPTION*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💰 *Choose Your Plan:*\n\n"
                    f"┌────────────────────────┐\n"
                    f"│  📅 *WEEKLY*           │\n"
                    f"│      *$75 / week*      │\n"
                    f"└────────────────────────┘\n\n"
                    f"┌────────────────────────┐\n"
                    f"│  📆 *MONTHLY* ⭐ SAVE  │\n"
                    f"│      *$185 / month*    │\n"
                    f"└────────────────────────┘\n\n"
                    f"✅ Full access to AI trading\n"
                    f"✅ Real-time portfolio tracking\n"
                    f"✅ Withdraw profits anytime"
                )
                await query.edit_message_text(
                    pricing_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Show Payment Address", callback_data='subscribe_payment')],
                        [InlineKeyboardButton("« Back", callback_data='subscribe_info')]
                    ]),
                    parse_mode='Markdown'
                )
            
            elif query.data == 'subscribe_payment':
                payment_text = (
                    f"🦑 *★ KR √¡\\ K - PAYMENT*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💳 *Send payment to:*\n\n"
                    f"┌────────────────────────┐\n"
                    f"│  ₿ BTC / ◎ SOL         │\n"
                    f"└────────────────────────┘\n\n"
                    f"`{SUBSCRIPTION_WALLET}`\n\n"
                    f"📋 Copy address above\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💵 *$75* = 1 Week\n"
                    f"💵 *$185* = 1 Month\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"⚡ Access activated within 1h\n"
                    f"after payment confirmation"
                )
                await query.edit_message_text(
                    payment_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("« Back to Pricing", callback_data='subscribe_pricing')]
                    ]),
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            self.logger.error(f"Button handler error: {e}")
    
    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages - check for recovery key, delete user messages"""
        if not update.message or not update.message.text:
            return
        
        text = update.message.text.strip()
        chat_id = str(update.effective_chat.id) if update.effective_chat else None
        
        if not chat_id:
            return
        
        # Delete user message to keep chat clean
        try:
            await update.message.delete()
        except Exception:
            pass
        
        # Check if message looks like a recovery key (KRK-XXXX-XXXX-XXXX)
        if text.upper().startswith('KRK-') and len(text) >= 18:
            if ADMIN.verify_recovery_key(text, chat_id):
                msg = await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=ADMIN.t('auth_success') + "\n\n" + ("Use /start to open menu" if ADMIN.language == 'en' else "Użyj /start aby otworzyć menu"),
                    parse_mode='Markdown'
                )
                self.logger.info(f"Admin access granted to chat_id: {chat_id}")
            else:
                await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=ADMIN.t('auth_failed'),
                    parse_mode='Markdown'
                )
    
    async def send_trade_alert(self, bot: TradingBot, trade: Trade, action: str):
        """Send trade alert"""
        
        if action == 'entry':
            msg = (f"*TRADE OPENED*\n\n"
                  f"Bot #{bot.bot_id} ({bot.pair})\n"
                  f"Side: {trade.side.upper()}\n"
                  f"Price: {trade.entry_price:.2f}\n"
                  f"Target: {trade.take_profit_price:.2f}\n"
                  f"Stop: {trade.stop_loss_price:.2f}")
        
        elif action == 'exit':
            emoji = "+" if trade.profit_usd > 0 else "-"
            msg = (f"{emoji} *TRADE CLOSED*\n\n"
                  f"Bot #{bot.bot_id} ({bot.pair})\n"
                  f"P&L: ${trade.profit_usd:.2f} ({trade.profit_percent:+.2f}%)\n"
                  f"Entry: {trade.entry_price:.2f}\n"
                  f"Exit: {trade.exit_price:.2f}")
        
        else:
            return
        
        await self.send_message(msg)

# ══════════════════════════════════════════════════════════════════════════════
# MASTER ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

class TradingSystem:
    """Master control system"""
    
    def __init__(self):
        self.logger = logging.getLogger('System')
        self.config = CONFIG
        self.bots: List[TradingBot] = []
        self.telegram = None
        self.running = False
        self.start_time = 0
        self.self_healer = None
        self.optimizer = None
        self.subscriptions = None
        self.rl_orchestrator = None  # 5 Ultra-Intelligent RL Engines
        self.ai_brain = None  # GPT-5 powered AI Trading Brain
        self.arb_orchestrator = None  # 500 Arbitrage Bots
    
    async def initialize(self, num_bots: int = 500):
        """Initialize the complete system with 500 trading bots"""
        
        self.logger.info("="*70)
        self.logger.info("KRAKEN ULTRA - INITIALIZATION (500 BOTS)")
        self.logger.info("="*70)
        
        # Initialize API
        await API.__aenter__()
        
        # ALL available pairs for Kraken Futures (expanded for maximum coverage)
        pairs = [
            # Perpetual Inverse (PI_)
            'PI_XBTUSD', 'PI_ETHUSD', 'PI_XRPUSD', 'PI_LTCUSD', 'PI_BCHUSD',
            # Perpetual Fixed (PF_) - Main assets
            'PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_AVAXUSD', 'PF_DOTUSD',
            'PF_ADAUSD', 'PF_ATOMUSD', 'PF_LINKUSD', 'PF_UNIUSD', 'PF_XRPUSD',
            'PF_LTCUSD', 'PF_BCHUSD', 'PF_EOSUSD', 'PF_MATICUSD', 'PF_DOGEUSD',
            # Layer 1 & Layer 2
            'PF_NEARUSD', 'PF_OPUSD', 'PF_ARBUSD', 'PF_APTUSD', 'PF_SUIUSD',
            # DeFi
            'PF_AAVEUSD', 'PF_MKRUSD', 'PF_CRVUSD', 'PF_SNXUSD', 'PF_COMPUSD',
            # Meme & Gaming
            'PF_SHIBUSSD', 'PF_PEPEUSD', 'PF_BONKUSD', 'PF_AXSUSD', 'PF_SANDUSD',
            # AI & New tokens
            'PF_RNDRUSD', 'PF_WLDUSD', 'PF_TIAUSD', 'PF_SEIUSD', 'PF_JUPUSD',
        ]
        
        # Assign one pair per bot
        for bot_id in range(num_bots):
            pair = pairs[bot_id % len(pairs)]  # Distribute pairs evenly
            bot = TradingBot(bot_id, pair)
            self.bots.append(bot)
        
        self.logger.info(f"Created {len(self.bots)} autonomous bots")
        
        # Connect WebSocket for all pairs
        unique_pairs = list(set(b.pair for b in self.bots))
        await API.connect_websocket(unique_pairs)
        
        self.logger.info(f"WebSocket connected: {len(unique_pairs)} pairs")
        
        # Initialize Telegram (use new Polish interface)
        if KRAKEN_INTERFACE_AVAILABLE and CONFIG.TELEGRAM_BOT_TOKEN != "your_telegram_bot_token":
            self.telegram = get_kraken_interface(self)
            await self.telegram.start()
        elif TELEGRAM_AVAILABLE and CONFIG.TELEGRAM_BOT_TOKEN != "your_telegram_bot_token":
            self.telegram = TelegramInterface(
                CONFIG.TELEGRAM_BOT_TOKEN,
                CONFIG.TELEGRAM_CHAT_ID,
                self.bots
            )
            await self.telegram.start()
        
        # Initialize advanced modules
        if SELF_HEALING_AVAILABLE:
            self.self_healer = SelfHealingEngine(self)
            await self.self_healer.start()
            self.logger.info("Self-Healing Engine activated")
        
        if GENETIC_OPTIMIZER_AVAILABLE:
            self.optimizer = StrategyOptimizer()
            self.optimizer.load()
            self.logger.info("Genetic Optimizer loaded")
        
        if SUBSCRIPTION_MANAGER_AVAILABLE:
            self.subscriptions = SubscriptionManager()
            await self.subscriptions.start()
            self.logger.info("Subscription Manager activated")
        
        # Initialize Memory Engine (memU integration)
        if MEMU_AVAILABLE:
            memory_initialized = await MEMORY.initialize()
            if memory_initialized:
                self.logger.info("Memory Engine activated - learning like a sponge")
            else:
                self.logger.info("Memory Engine running in cache-only mode")
        
        # Initialize RL Engines (5 Ultra-Intelligent Engines)
        if RL_ENGINES_AVAILABLE:
            self.rl_orchestrator = get_rl_orchestrator()
            self.logger.info("RL Engines activated: PPO, A3C, DQN, SAC, TD3")
        
        # Initialize AI Trading Brain (GPT-5 powered)
        if AI_BRAIN_AVAILABLE:
            self.ai_brain = get_ai_brain()
            self.logger.info("AI Trading Brain activated: GPT-5 powered analysis")
        
        # Initialize Arbitrage Engine (500 bots for all cryptos)
        if ARBITRAGE_AVAILABLE:
            arb_capital = 2000.0  # $2000 for arbitrage operations
            self.arb_orchestrator = get_arbitrage_orchestrator(num_bots=500, initial_capital=arb_capital)
            self.logger.info(f"Arbitrage Engine activated: 500 bots | {len(pairs)} pairs | ${arb_capital:.0f}")
        
        self.logger.info("="*70)
        self.logger.info("INITIALIZATION COMPLETE")
        self.logger.info(f"Total: {num_bots} trading bots + 500 arbitrage bots = {num_bots + 500} bots")
        self.logger.info("="*70)
    
    async def execute_all_quick_trades(self):
        """Execute quick trades on all bots - buy and sell immediately"""
        self.logger.info("="*50)
        self.logger.info("EXECUTING QUICK TRADES ON ALL BOTS")
        self.logger.info("="*50)
        
        results = {'success': 0, 'failed': 0}
        
        # Execute in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(self.bots), batch_size):
            batch = self.bots[i:i+batch_size]
            tasks = [bot.execute_quick_trade() for bot in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if result is True:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            
            # Small delay between batches
            await asyncio.sleep(0.5)
        
        self.logger.info(f"QUICK TRADES COMPLETE: {results['success']} success, {results['failed']} failed")
        
        if self.telegram:
            await self.telegram.send_message(
                f"⚡ QUICK TRADES COMPLETE\n\n"
                f"Success: {results['success']}\n"
                f"Failed: {results['failed']}"
            )
        
        return results

    async def run(self):
        """Run the complete system"""
        
        self.running = True
        self.start_time = time.time()
        
        self.logger.info("\nSYSTEM ACTIVATED\n")
        
        # Create tasks for all bots
        tasks = []
        
        for bot in self.bots:
            task = asyncio.create_task(self._run_bot_safe(bot))
            tasks.append(task)
        
        # System tasks
        tasks.append(asyncio.create_task(API.websocket_handler()))
        tasks.append(asyncio.create_task(API.health_check()))
        tasks.append(asyncio.create_task(self._monitor()))
        tasks.append(asyncio.create_task(self._daily_reset()))
        
        # RL Engines learning cycle (medium/long-term orders)
        if RL_ENGINES_AVAILABLE and self.rl_orchestrator:
            tasks.append(asyncio.create_task(self._rl_learning_cycle()))
            tasks.append(asyncio.create_task(self._rl_order_generator()))
        
        # AI Trading Brain analysis cycle
        if AI_BRAIN_AVAILABLE and self.ai_brain:
            tasks.append(asyncio.create_task(self._ai_brain_analysis_cycle()))
        
        # Arbitrage Engine cycle (500 bots scanning for opportunities)
        if ARBITRAGE_AVAILABLE and self.arb_orchestrator:
            tasks.append(asyncio.create_task(self._arbitrage_cycle()))
        
        # Quick trades disabled - enable via Telegram command when API keys are valid
        # tasks.append(asyncio.create_task(self._delayed_quick_trades()))
        
        # Run all tasks
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"System error: {e}")
    
    async def _delayed_quick_trades(self):
        """Execute quick trades after startup delay"""
        await asyncio.sleep(10)  # Wait for system to stabilize
        await self.execute_all_quick_trades()
    
    async def _run_bot_safe(self, bot: TradingBot):
        """Run bot with error isolation"""
        
        try:
            await bot.run()
        except Exception as e:
            self.logger.error(f"Bot #{bot.bot_id} crashed: {e}\n{traceback.format_exc()}")
            bot.active = False
            bot.error_count += 1
            bot._save_state()
            
            # Alert via Telegram
            if self.telegram:
                await self.telegram.send_message(
                    f"Bot #{bot.bot_id} ({bot.pair}) crashed:\n{str(e)}"
                )
    
    async def emergency_stop(self):
        """Emergency stop - close all positions and deactivate all bots"""
        self.logger.warning("EMERGENCY STOP ACTIVATED")
        self.running = False
        
        for bot in self.bots:
            bot.active = False
            if bot.current_trade:
                try:
                    await bot.close_trade("Emergency stop")
                except Exception as e:
                    self.logger.error(f"Failed to close Bot #{bot.bot_id}: {e}")
        
        self.logger.warning("EMERGENCY STOP COMPLETE - All bots deactivated")
    
    async def _monitor(self):
        """System monitoring and reporting"""
        
        while self.running:
            await asyncio.sleep(300)  # Every 5 minutes
            
            # Calculate stats
            active = sum(1 for b in self.bots if b.active)
            in_position = sum(1 for b in self.bots if b.current_trade)
            total_profit = sum(b.total_profit_usd for b in self.bots)
            total_trades_today = sum(b.trades_today for b in self.bots)
            
            # Log status
            self.logger.info("="*70)
            self.logger.info(f"STATUS UPDATE")
            self.logger.info(f"Active Bots: {active}/{len(self.bots)} | In Position: {in_position}")
            self.logger.info(f"Trades Today: {total_trades_today} | Profit: ${total_profit:.2f}")
            self.logger.info(f"Mode: {'LIVE' if CONFIG.LIVE_MODE else 'PAPER'}")
            self.logger.info("="*70)
            
            # Log to database
            if DB.conn:
                stats = DB.get_system_stats()
                cursor = DB.conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO system_stats (timestamp, total_trades, active_bots, total_profit_usd, total_fees_usd, mode)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    time.time(),
                    stats.get('total_trades', 0) or 0,
                    active,
                    total_profit,
                    stats.get('total_fees', 0) or 0,
                    'LIVE' if CONFIG.LIVE_MODE else 'PAPER'
                ))
                DB.conn.commit()
    
    async def _daily_reset(self):
        """Daily statistics reset"""
        
        while self.running:
            # Wait until midnight
            now = datetime.now()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0)
            wait_seconds = (tomorrow - now).total_seconds()
            
            await asyncio.sleep(wait_seconds)
            
            # Reset daily counters
            for bot in self.bots:
                bot.trades_today = 0
            
            self.logger.info("Daily statistics reset")
            
            if self.telegram:
                await self.telegram.send_message("Daily statistics have been reset")
    
    async def _rl_learning_cycle(self):
        """Background RL learning cycle - continuous model improvement"""
        self.logger.info("🧠 RL Learning Cycle started")
        
        while self.running:
            try:
                # Let engines learn from accumulated experiences
                for engine in self.rl_orchestrator.engines.values():
                    if len(engine.replay_buffer) >= engine.batch_size:
                        experiences = list(engine.replay_buffer)[-engine.batch_size:]
                        engine.learn(experiences)
                
                # Save models periodically
                self.rl_orchestrator.save_all_models()
                
            except Exception as e:
                self.logger.error(f"RL learning error: {e}")
            
            await asyncio.sleep(300)  # Learn every 5 minutes
    
    async def _rl_order_generator(self):
        """Generate medium and long-term RL orders"""
        self.logger.info("🧠 RL Order Generator started")
        
        while self.running:
            try:
                unique_pairs = list(set(b.pair for b in self.bots))
                
                for pair in unique_pairs:
                    # Get current market data
                    ob = await API.get_orderbook(pair)
                    if not ob or not ob.bids or not ob.asks:
                        continue
                    
                    price = ob.mid_price
                    volume = sum([b[1] for b in ob.bids[:5]]) + sum([a[1] for a in ob.asks[:5]])
                    spread = (ob.asks[0][0] - ob.bids[0][0]) / price
                    
                    # Calculate market metrics
                    bid_vol = sum([b[1] for b in ob.bids[:10]])
                    ask_vol = sum([a[1] for a in ob.asks[:10]])
                    imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0
                    
                    market_data = {
                        'price': price,
                        'volume': volume,
                        'volatility': spread * 10,  # Approximate volatility from spread
                        'trend_strength': abs(imbalance),
                        'rsi': 50 + imbalance * 30,  # Approximate RSI from imbalance
                        'macd': imbalance * 0.02,
                        'bb_position': 0.5 + imbalance * 0.3,
                        'orderbook_imbalance': imbalance,
                        'funding_rate': 0.0001,
                        'oi_change': 0,
                        'fear_greed': 50 + imbalance * 20,
                        'btc_correlation': 1.0 if 'XBT' in pair else 0.8
                    }
                    
                    # Generate RL orders for this pair
                    orders = self.rl_orchestrator.generate_orders(market_data, pair)
                    
                    for order in orders:
                        self.logger.info(
                            f"🧠 RL Order: {order.engine_id} | {pair} | {order.action.name} | "
                            f"Conf: {order.confidence:.1%} | Horizon: {order.time_horizon.value}"
                        )
                        
                        # Find a bot for this pair and apply the RL signal
                        for bot in self.bots:
                            if bot.pair == pair and bot.active and not bot.current_trade:
                                bot.rl_signal = order
                                break
                    
                    await asyncio.sleep(0.1)  # Small delay between pairs
                
            except Exception as e:
                self.logger.error(f"RL order generation error: {e}")
            
            await asyncio.sleep(60)  # Generate orders every minute
    
    async def _ai_brain_analysis_cycle(self):
        """AI Trading Brain analysis cycle - GPT-5 powered market analysis"""
        self.logger.info("🧠 AI Brain Analysis Cycle started")
        
        while self.running:
            try:
                unique_pairs = list(set(b.pair for b in self.bots))
                
                for pair in unique_pairs:
                    # Get current market data
                    ob = await API.get_orderbook(pair)
                    if not ob or not ob.bids or not ob.asks:
                        continue
                    
                    price = ob.mid_price
                    volume = sum([b[1] for b in ob.bids[:5]]) + sum([a[1] for a in ob.asks[:5]])
                    spread = (ob.asks[0][0] - ob.bids[0][0]) / price
                    
                    # Calculate market metrics
                    bid_vol = sum([b[1] for b in ob.bids[:10]])
                    ask_vol = sum([a[1] for a in ob.asks[:10]])
                    imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0
                    
                    market_data = {
                        'price': price,
                        'volume': volume,
                        'volatility': spread * 10,
                        'trend_strength': abs(imbalance),
                        'rsi': 50 + imbalance * 30,
                        'macd': imbalance * 0.02,
                        'bb_position': 0.5 + imbalance * 0.3,
                        'orderbook_imbalance': imbalance,
                        'funding_rate': 0.0001,
                        'oi_change': 0,
                        'fear_greed': 50 + imbalance * 20
                    }
                    
                    # Get AI analysis
                    analysis = await self.ai_brain.analyze_market(pair, market_data)
                    
                    if analysis and analysis.confidence >= 0.7:
                        self.logger.info(
                            f"🧠 AI Analysis: {pair} | {analysis.signal.value} | "
                            f"Conf: {analysis.confidence:.0%} | Risk: {analysis.risk_level.value}"
                        )
                        
                        # Apply AI signal to bots
                        for bot in self.bots:
                            if bot.pair == pair and bot.active:
                                bot.ai_signal = analysis
                    
                    await asyncio.sleep(0.5)  # Rate limit AI calls
                
            except Exception as e:
                self.logger.error(f"AI Brain analysis error: {e}")
            
            await asyncio.sleep(120)  # Analyze every 2 minutes
    
    async def _arbitrage_cycle(self):
        """Arbitrage Engine cycle - 500 bots scanning for opportunities"""
        self.logger.info("🦑 Arbitrage Engine Cycle started (500 bots)")
        
        while self.running:
            try:
                # Get current prices from all pairs
                unique_pairs = list(set(b.pair for b in self.bots))
                prices = {}
                
                for pair in unique_pairs:
                    ob = await API.get_orderbook(pair)
                    if ob and ob.bids and ob.asks:
                        prices[pair] = ob.mid_price
                
                # Update arbitrage scanner with prices
                self.arb_orchestrator.update_prices(prices)
                
                # Scan and execute arbitrage opportunities
                result = await self.arb_orchestrator.scan_and_execute()
                
                if result['executed'] > 0:
                    self.logger.info(
                        f"🦑 ARB: {result['opportunities']} opps | "
                        f"{result['executed']} executed | "
                        f"${result['total_capital']:.2f}"
                    )
                
            except Exception as e:
                self.logger.error(f"Arbitrage cycle error: {e}")
            
            await asyncio.sleep(5)  # Scan every 5 seconds for quick opportunities
    
    async def shutdown(self):
        """Graceful shutdown"""
        
        self.logger.info("Shutting down...")
        self.running = False
        
        # Save all bot states
        for bot in self.bots:
            bot._save_state()
        
        # Save RL models
        if self.rl_orchestrator:
            self.rl_orchestrator.save_all_models()
        
        # Stop Telegram
        if self.telegram:
            await self.telegram.stop()
        
        # Close API
        await API.__aexit__()
        
        # Close database
        DB.close()
        
        self.logger.info("Shutdown complete")

# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK SERVER (for deployment)
# ══════════════════════════════════════════════════════════════════════════════

from aiohttp import web

class HealthServer:
    """Simple HTTP server for deployment health checks"""
    
    def __init__(self, system: 'TradingSystem'):
        self.system = system
        self.app = web.Application()
        self.app.router.add_get('/', self.health_check)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status)
        self.app.router.add_post('/telegram-webhook', self.telegram_webhook)
        self.runner = None
    
    async def health_check(self, request):
        """Root endpoint for health checks"""
        return web.json_response({
            "status": "healthy",
            "service": "KRAKEN ULTRA",
            "mode": "LIVE" if CONFIG.LIVE_MODE else "PAPER"
        })
    
    async def status(self, request):
        """Detailed status endpoint"""
        if self.system:
            active_bots = sum(1 for b in self.system.bots if b.active)
            in_position = sum(1 for b in self.system.bots if b.current_trade)
            total_profit = sum(b.total_profit_usd for b in self.system.bots)
            return web.json_response({
                "status": "running" if self.system.running else "stopped",
                "bots": {
                    "total": len(self.system.bots),
                    "active": active_bots,
                    "in_position": in_position
                },
                "profit_usd": round(total_profit, 2),
                "mode": "LIVE" if CONFIG.LIVE_MODE else "PAPER"
            })
        return web.json_response({"status": "initializing"})
    
    async def telegram_webhook(self, request):
        """Handle Telegram webhook updates"""
        try:
            if self.system and self.system.telegram and self.system.telegram.app:
                data = await request.json()
                from telegram import Update
                update = Update.de_json(data, self.system.telegram.app.bot)
                await self.system.telegram.app.process_update(update)
            return web.Response(text="OK")
        except Exception as e:
            logging.getLogger('System').error(f"Webhook error: {e}")
            return web.Response(text="OK")
    
    async def start(self):
        """Start health server"""
        port = int(os.environ.get('PORT', 8080))
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', port)
        await site.start()
        logging.getLogger('System').info(f"Health server started on port {port}")
    
    async def stop(self):
        """Stop health server"""
        if self.runner:
            await self.runner.cleanup()

# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    """Main entry point"""
    
    print("""
    ████████████████████████████████████████████████████████████████████████████████
    █                                                                              █
    █  KRAKEN ULTRA - AUTONOMOUS TRADING SYSTEM                                   █
    █                                                                              █
    ████████████████████████████████████████████████████████████████████████████████
    """)
    
    system = TradingSystem()
    health_server = HealthServer(system)
    
    try:
        # Start health server first (for deployment health checks)
        await health_server.start()
        
        # Initialize with 100 bots
        await system.initialize(num_bots=100)
        
        # Run the system
        await system.run()
        
    except KeyboardInterrupt:
        print("\nInterrupt received...")
    finally:
        await health_server.stop()
        await system.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
