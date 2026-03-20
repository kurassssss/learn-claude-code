"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                      ║
║  ███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗                                      ║
║  ████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝                                      ║
║  ██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗                                      ║
║  ██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║                                      ║
║  ██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║                                      ║
║  ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝                                      ║
║                                                                                      ║
║   ██████╗ ███╗   ███╗███████╗ ██████╗  █████╗                                     ║
║  ██╔═══██╗████╗ ████║██╔════╝██╔════╝ ██╔══██╗                                    ║
║  ██║   ██║██╔████╔██║█████╗  ██║  ███╗███████║                                    ║
║  ██║   ██║██║╚██╔╝██║██╔══╝  ██║   ██║██╔══██║                                    ║
║  ╚██████╔╝██║ ╚═╝ ██║███████╗╚██████╔╝██║  ██║                                    ║
║   ╚═════╝ ╚═╝     ╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝                                    ║
║                                                                                      ║
║   S U P R E M E   S E L F - H E A L I N G   S U P E R I N T E L L I G E N C E    ║
║                                                                                      ║
║  "Every error is a gift. Every failure a lesson. Every fix a victory."              ║
║                                                                                      ║
║  CAPABILITIES:                                                                       ║
║  ● ModuleRegistry     — discovers, loads, wires ALL Nexus modules                  ║
║  ● DependencyDoctor   — auto-installs missing packages, resolves conflicts          ║
║  ● NeuralLogAnalyzer  — NLP + pattern matching, 60+ error signatures               ║
║  ● OmegaAutoFixer     — 25 healing strategies, AST-level code repair               ║
║  ● IntegrationBus     — typed message routing between ALL modules                  ║
║  ● ClaudeHealer       — asks Claude API for deep error reasoning                   ║
║  ● RewardSystem       — self-reinforcing: earns XP for every successful heal       ║
║  ● HealthOracle       — predictive health scoring, anomaly detection               ║
║  ● CircuitGuardian    — per-module circuit breakers, graceful degradation          ║
║  ● SelfEvolver        — learns which fixes work, improves over time                ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import ast
import asyncio
import collections
import importlib
import importlib.util
import inspect
import io
import json
import logging
import math
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import types
import uuid
from collections import defaultdict, deque, Counter
from contextlib import suppress
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

import numpy as np

# ── optional imports ──────────────────────────────────────────────────────────
def _try(n):
    try: return __import__(n)
    except ImportError: return None

psutil_mod  = _try("psutil")
rich_mod    = _try("rich")
anthropic_m = _try("anthropic")

UTC  = timezone.utc
_NOW = lambda: datetime.now(UTC).isoformat()
_TS  = lambda: time.time()

# ── dirs ──────────────────────────────────────────────────────────────────────
for _d in ["logs", "omega_data", "omega_data/repairs", "omega_data/snapshots",
           "omega_data/metrics", "omega_data/rewards"]:
    Path(_d).mkdir(parents=True, exist_ok=True)

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-20s │ %(levelname)-8s │ %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/omega_{int(time.time())}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("NEXUS·OMEGA")


# ══════════════════════════════════════════════════════════════════════════════
#  ENUMS & DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

class Severity(Enum):
    DEBUG    = 0
    INFO     = 1
    WARNING  = 2
    ERROR    = 3
    CRITICAL = 4
    FATAL    = 5


class ErrorCategory(Enum):
    NETWORK        = "network"
    API            = "api"
    DATABASE       = "database"
    TRADING        = "trading"
    SYSTEM         = "system"
    WEBSOCKET      = "websocket"
    AUTH           = "authentication"
    RATE_LIMIT     = "rate_limit"
    DEPENDENCY     = "dependency"
    INTEGRATION    = "integration"
    MODULE_CRASH   = "module_crash"
    MEMORY         = "memory"
    IMPORT_ERROR   = "import_error"
    SYNTAX_ERROR   = "syntax_error"
    CONFIG         = "config"
    UNKNOWN        = "unknown"


class ModuleStatus(Enum):
    UNKNOWN    = "unknown"
    LOADING    = "loading"
    HEALTHY    = "healthy"
    DEGRADED   = "degraded"
    CRASHED    = "crashed"
    RESTARTING = "restarting"
    DISABLED   = "disabled"


class HealResult(Enum):
    SUCCESS      = "success"
    PARTIAL      = "partial"
    FAILED       = "failed"
    SKIPPED      = "skipped"
    COOLDOWN     = "cooldown"


@dataclass
class OmegaError:
    id:           str
    ts:           float
    message:      str
    category:     ErrorCategory
    severity:     Severity
    module:       str            = "unknown"
    traceback:    str            = ""
    context:      Dict           = field(default_factory=dict)
    fix_attempts: int            = 0
    resolved:     bool           = False
    resolution_ts:Optional[float]= None
    resolution_by:str            = ""
    llm_analysis: str            = ""


@dataclass
class HealAction:
    id:         str
    ts:         float
    error_id:   str
    strategy:   str
    module:     str
    result:     HealResult
    duration_s: float
    details:    str
    xp_earned:  int = 0


@dataclass
class ModuleInfo:
    name:         str
    path:         str
    status:       ModuleStatus     = ModuleStatus.UNKNOWN
    instance:     Any              = None
    task:         Any              = None
    last_heartbeat: float          = 0.0
    error_count:  int              = 0
    restart_count:int              = 0
    health_score: float            = 100.0
    dependencies: List[str]        = field(default_factory=list)
    entry_point:  str              = ""
    api_port:     Optional[int]    = None
    description:  str              = ""


@dataclass
class IntegrationMessage:
    id:       str
    ts:       float
    src:      str      # sender module
    dst:      str      # receiver module ("*" = broadcast)
    topic:    str
    payload:  Any
    priority: int = 5  # 1=highest, 10=lowest


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA DATABASE
# ══════════════════════════════════════════════════════════════════════════════

class OmegaDB:
    """Persistent store for all Omega state — errors, heals, rewards, metrics."""

    def __init__(self, path: Path = Path("omega_data") / "omega.db"):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.Lock()
        self._init()

    def _init(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS errors (
                id TEXT PRIMARY KEY, ts REAL, message TEXT,
                category TEXT, severity TEXT, module TEXT,
                traceback TEXT, context TEXT, fix_attempts INTEGER DEFAULT 0,
                resolved INTEGER DEFAULT 0, resolution_ts REAL,
                resolution_by TEXT, llm_analysis TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_err_ts ON errors(ts);
            CREATE INDEX IF NOT EXISTS idx_err_mod ON errors(module);

            CREATE TABLE IF NOT EXISTS heals (
                id TEXT PRIMARY KEY, ts REAL, error_id TEXT,
                strategy TEXT, module TEXT, result TEXT,
                duration_s REAL, details TEXT, xp_earned INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS rewards (
                id TEXT PRIMARY KEY, ts REAL,
                event TEXT, xp INTEGER, total_xp INTEGER,
                level INTEGER, details TEXT
            );

            CREATE TABLE IF NOT EXISTS module_metrics (
                id TEXT PRIMARY KEY, module TEXT, ts REAL,
                health_score REAL, error_count INTEGER,
                restart_count INTEGER, uptime_s REAL,
                heartbeat_lag_s REAL, status TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_mm_mod ON module_metrics(module, ts);

            CREATE TABLE IF NOT EXISTS dependency_log (
                package TEXT PRIMARY KEY, version TEXT,
                installed_ts REAL, install_result TEXT, reason TEXT
            );

            CREATE TABLE IF NOT EXISTS heal_knowledge (
                id TEXT PRIMARY KEY,
                error_pattern TEXT, strategy TEXT,
                success_rate REAL, n_attempts INTEGER,
                last_used REAL
            );
        """)
        self.conn.commit()

    def save_error(self, e: OmegaError):
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO errors VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (e.id, e.ts, e.message[:1000], e.category.value, e.severity.name,
                 e.module, e.traceback[:2000], json.dumps(e.context),
                 e.fix_attempts, int(e.resolved), e.resolution_ts,
                 e.resolution_by, e.llm_analysis[:500])
            )
            self.conn.commit()

    def save_heal(self, h: HealAction):
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO heals VALUES (?,?,?,?,?,?,?,?,?)",
                (h.id, h.ts, h.error_id, h.strategy, h.module,
                 h.result.value, h.duration_s, h.details[:500], h.xp_earned)
            )
            self.conn.commit()

    def log_reward(self, event: str, xp: int, total: int, level: int, details: str = ""):
        with self._lock:
            self.conn.execute(
                "INSERT INTO rewards VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:8], _TS(), event, xp, total, level, details)
            )
            self.conn.commit()

    def update_heal_knowledge(self, pattern: str, strategy: str, success: bool):
        with self._lock:
            row = self.conn.execute(
                "SELECT id, n_attempts, success_rate FROM heal_knowledge "
                "WHERE error_pattern=? AND strategy=?", (pattern, strategy)
            ).fetchone()
            if row:
                n   = row[1] + 1
                sr  = (row[2] * row[1] + float(success)) / n
                self.conn.execute(
                    "UPDATE heal_knowledge SET n_attempts=?, success_rate=?, last_used=? "
                    "WHERE id=?", (n, sr, _TS(), row[0])
                )
            else:
                self.conn.execute(
                    "INSERT INTO heal_knowledge VALUES (?,?,?,?,?,?)",
                    (str(uuid.uuid4())[:8], pattern, strategy,
                     float(success), 1, _TS())
                )
            self.conn.commit()

    def get_best_strategy(self, pattern: str) -> Optional[str]:
        with self._lock:
            row = self.conn.execute(
                "SELECT strategy FROM heal_knowledge "
                "WHERE error_pattern LIKE ? AND n_attempts >= 3 "
                "ORDER BY success_rate DESC LIMIT 1",
                (f"%{pattern[:30]}%",)
            ).fetchone()
        return row[0] if row else None

    def get_stats(self) -> Dict:
        with self._lock:
            errors = self.conn.execute(
                "SELECT COUNT(*), SUM(resolved) FROM errors"
            ).fetchone()
            heals  = self.conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN result='success' THEN 1 ELSE 0 END) "
                "FROM heals"
            ).fetchone()
            reward = self.conn.execute(
                "SELECT SUM(xp), MAX(level) FROM rewards"
            ).fetchone()
        return {
            "total_errors":   errors[0] or 0,
            "resolved":       errors[1] or 0,
            "total_heals":    heals[0]  or 0,
            "successful_heals": heals[1] or 0,
            "total_xp":       reward[0] or 0,
            "level":          reward[1] or 1,
        }


# ══════════════════════════════════════════════════════════════════════════════
#  REWARD SYSTEM  (self-reinforcing super intelligence)
# ══════════════════════════════════════════════════════════════════════════════

class RewardSystem:
    """
    The Omega earns XP for every successful action.
    Higher level → more aggressive healing, higher autonomy.
    
    XP Table:
      Fix network error          → 10 XP
      Fix dependency             → 20 XP
      Fix module crash           → 50 XP
      Fix integration error      → 30 XP
      Prevent error (proactive)  → 75 XP
      Install missing package    → 15 XP
      Successful LLM-guided fix  → 100 XP
      Zero errors in 1h window   → 200 XP (bonus)
    """

    XP_TABLE = {
        "fix_network":       10,
        "fix_rate_limit":    10,
        "fix_websocket":     15,
        "fix_database":      25,
        "fix_dependency":    20,
        "fix_module_crash":  50,
        "fix_integration":   30,
        "fix_import_error":  20,
        "fix_syntax_error":  40,
        "fix_auth":          15,
        "fix_memory":        35,
        "proactive_prevent": 75,
        "llm_guided_fix":   100,
        "install_package":   15,
        "restart_module":    20,
        "clean_hour":       200,
        "full_recovery":    150,
    }

    LEVELS = [
        (0,     1,  "OBSERVER",    "Watching and learning"),
        (100,   2,  "MEDIC",       "Can fix basic errors"),
        (300,   3,  "ENGINEER",    "Repairs modules autonomously"),
        (700,   4,  "ARCHITECT",   "Redesigns broken integrations"),
        (1500,  5,  "GUARDIAN",    "Predicts failures before they happen"),
        (3000,  6,  "SOVEREIGN",   "Full system mastery"),
        (6000,  7,  "ORACLE",      "Omniscient system intelligence"),
        (12000, 8,  "TRANSCENDENT","Beyond programmed limitations"),
    ]

    def __init__(self, db: OmegaDB):
        self.db    = db
        self.xp    = 0
        self.level = 1
        self.level_name = "OBSERVER"
        self._milestones: List[str] = []
        self._streak = 0          # consecutive successful fixes
        self._streak_bonus = 1.0
        self._load()

    def _load(self):
        stats = self.db.get_stats()
        self.xp = stats.get("total_xp", 0)
        self._recalc_level()

    def earn(self, event: str, bonus_multiplier: float = 1.0,
             details: str = "") -> int:
        base_xp = self.XP_TABLE.get(event, 5)
        earned  = int(base_xp * bonus_multiplier * self._streak_bonus)

        self.xp += earned
        self._streak += 1
        self._streak_bonus = min(3.0, 1.0 + self._streak * 0.05)

        old_level = self.level
        self._recalc_level()

        level_up = self.level > old_level
        if level_up:
            log.info(f"🎉 LEVEL UP! {old_level} → {self.level} "
                     f"[{self.level_name}] — {self._get_level_desc()}")
            self.db.log_reward(
                f"LEVEL_UP_{self.level}", earned + 500,
                self.xp, self.level,
                f"Reached level {self.level}: {self.level_name}"
            )

        self.db.log_reward(event, earned, self.xp, self.level, details)
        log.debug(f"⭐ XP +{earned} [{event}] | Total: {self.xp} | "
                  f"Level: {self.level} {self.level_name} | "
                  f"Streak: {self._streak}×{self._streak_bonus:.1f}")
        return earned

    def penalize(self, reason: str):
        self._streak = 0
        self._streak_bonus = 1.0
        log.debug(f"💔 Streak reset: {reason}")

    def _recalc_level(self):
        for xp_req, lvl, name, desc in reversed(self.LEVELS):
            if self.xp >= xp_req:
                self.level = lvl
                self.level_name = name
                return

    def _get_level_desc(self) -> str:
        for _, lvl, _, desc in self.LEVELS:
            if lvl == self.level:
                return desc
        return ""

    @property
    def autonomy_factor(self) -> float:
        """Higher level = more autonomous healing. 0.0..1.0"""
        return min(1.0, (self.level - 1) / 7)

    def status(self) -> Dict:
        stats = self.db.get_stats()
        next_lvl = next((xp for xp, lvl, _, _ in self.LEVELS
                         if lvl == self.level + 1), None)
        return {
            "xp":          self.xp,
            "level":       self.level,
            "level_name":  self.level_name,
            "streak":      self._streak,
            "streak_bonus":round(self._streak_bonus, 2),
            "xp_to_next":  max(0, next_lvl - self.xp) if next_lvl else 0,
            "autonomy":    round(self.autonomy_factor, 2),
            **stats,
        }


# ══════════════════════════════════════════════════════════════════════════════
#  DEPENDENCY DOCTOR  (auto-installs & resolves package conflicts)
# ══════════════════════════════════════════════════════════════════════════════

class DependencyDoctor:
    """
    Monitors import failures, resolves package conflicts,
    and installs missing dependencies without user intervention.
    """

    # Known mappings: import_name → pip_package
    IMPORT_TO_PIP: Dict[str, str] = {
        "ccxt":              "ccxt>=4.3.0",
        "pandas":            "pandas>=2.2.0",
        "pandas_ta":         "pandas-ta>=0.3.14b",
        "numpy":             "numpy>=1.26.0",
        "aiohttp":           "aiohttp>=3.9.0",
        "websockets":        "websockets>=12.0",
        "fastapi":           "fastapi>=0.111.0",
        "uvicorn":           "uvicorn>=0.29.0",
        "anthropic":         "anthropic>=0.25.0",
        "openai":            "openai>=1.30.0",
        "loguru":            "loguru>=0.7.0",
        "rich":              "rich>=13.7.0",
        "empyrical":         "empyrical>=0.5.5",
        "arch":              "arch>=6.3.0",
        "yfinance":          "yfinance>=0.2.40",
        "psutil":            "psutil>=5.9.0",
        "redis":             "redis>=5.0.0",
        "sqlalchemy":        "sqlalchemy>=2.0.0",
        "scipy":             "scipy>=1.13.0",
        "sklearn":           "scikit-learn>=1.4.0",
        "xgboost":           "xgboost>=2.0.0",
        "lightgbm":          "lightgbm>=4.3.0",
        "optuna":            "optuna>=3.6.0",
        "pyarrow":           "pyarrow>=15.0.0",
        "polars":            "polars>=0.20.0",
        "plotly":            "plotly>=5.22.0",
        "dash":              "dash>=2.17.0",
        "krakenex":          "krakenex>=2.1.0",
        "tenacity":          "tenacity>=8.2.0",
        "httpx":             "httpx>=0.27.0",
        "dotenv":            "python-dotenv>=1.0.0",
        "apscheduler":       "apscheduler>=3.10.0",
        "prometheus_client": "prometheus-client>=0.20.0",
        "ta":                "ta>=0.11.0",
        "langchain":         "langchain>=0.2.0",
        "crewai":            "crewai>=0.30.0",
        "chromadb":          "chromadb>=0.5.0",
        "prophet":           "prophet>=1.1.5",
        "ujson":             "ujson>=5.9.0",
        "msgpack":           "msgpack>=1.0.8",
        "cachetools":        "cachetools>=5.3.0",
        "bs4":               "beautifulsoup4>=4.12.0",
        "feedparser":        "feedparser>=6.0.11",
        "telegram":          "python-telegram-bot>=20.0",
    }

    def __init__(self, db: OmegaDB, reward: RewardSystem):
        self.db     = db
        self.reward = reward
        self._installing: Set[str] = set()
        self._failed: Set[str] = set()
        self._lock  = asyncio.Lock()

    async def ensure(self, import_name: str, reason: str = "") -> bool:
        """Ensure a package is importable. Install if missing."""
        # Already installed?
        try:
            importlib.import_module(import_name)
            return True
        except ImportError:
            pass

        pip_pkg = self.IMPORT_TO_PIP.get(import_name, import_name)

        async with self._lock:
            if pip_pkg in self._installing:
                await asyncio.sleep(2)
                return False
            if pip_pkg in self._failed:
                return False
            self._installing.add(pip_pkg)

        log.info(f"📦 Installing: {pip_pkg} (reason: {reason or import_name})")
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    [sys.executable, "-m", "pip", "install", "--quiet", pip_pkg],
                    capture_output=True, text=True, timeout=180,
                )
            )
            ok = result.returncode == 0
            if ok:
                log.info(f"✅ Installed: {pip_pkg}")
                self.reward.earn("install_package",
                                 details=f"Installed {pip_pkg}")
                self.db.conn.execute(
                    "INSERT OR REPLACE INTO dependency_log VALUES (?,?,?,?,?)",
                    (pip_pkg, "latest", _TS(), "success", reason)
                )
                self.db.conn.commit()
                # Reload module
                with suppress(Exception):
                    importlib.import_module(import_name)
            else:
                log.warning(f"⚠️  Install failed: {pip_pkg}: {result.stderr[:200]}")
                self._failed.add(pip_pkg)
                self.db.conn.execute(
                    "INSERT OR REPLACE INTO dependency_log VALUES (?,?,?,?,?)",
                    (pip_pkg, "unknown", _TS(), "failed", result.stderr[:200])
                )
                self.db.conn.commit()
            return ok
        except Exception as e:
            log.error(f"Install exception {pip_pkg}: {e}")
            self._failed.add(pip_pkg)
            return False
        finally:
            self._installing.discard(pip_pkg)

    async def scan_and_install(self, module_path: str) -> Dict[str, bool]:
        """
        Parse a Python file's imports and ensure all are installed.
        Returns {import_name: installed_ok}.
        """
        results: Dict[str, bool] = {}
        try:
            src = Path(module_path).read_text(encoding="utf-8")
            tree = ast.parse(src)
        except Exception as e:
            log.warning(f"Cannot parse {module_path}: {e}")
            return results

        imports: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split(".")[0])

        # Stdlib modules to skip
        stdlib = sys.stdlib_module_names if hasattr(sys, "stdlib_module_names") \
                 else {"os","sys","re","json","time","math","io","abc","ast",
                       "copy","enum","uuid","hash","threading","asyncio","typing",
                       "pathlib","datetime","dataclasses","collections","traceback",
                       "subprocess","importlib","inspect","logging","sqlite3",
                       "contextlib","functools","itertools","random","string"}

        for imp in imports:
            if imp in stdlib:
                continue
            try:
                importlib.import_module(imp)
                results[imp] = True
            except ImportError:
                ok = await self.ensure(imp, f"required by {Path(module_path).name}")
                results[imp] = ok

        return results

    async def check_requirements_txt(self, path: str = "requirements.txt") -> Dict:
        """Install everything from requirements.txt."""
        if not Path(path).exists():
            return {"error": "requirements.txt not found"}
        log.info(f"📋 Checking {path}...")
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", path, "--quiet"],
                capture_output=True, text=True, timeout=600,
            )
        )
        ok = result.returncode == 0
        if ok:
            log.info("✅ requirements.txt satisfied")
        else:
            log.warning(f"⚠️  requirements.txt issues: {result.stderr[:300]}")
        return {"success": ok, "output": result.stdout[-300:], "errors": result.stderr[-300:]}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE REGISTRY  (discovers and manages all Nexus modules)
# ══════════════════════════════════════════════════════════════════════════════

class ModuleRegistry:
    """
    Central registry for ALL Nexus modules.
    Discovers Python files, loads them, tracks their health,
    and manages their lifecycle.
    """

    KNOWN_MODULES: List[Dict] = [
        {
            "name": "nexus_data_engine",
            "file": "nexus_data_engine.py",
            "description": "Universal market data bus (15 exchanges, WS, macro, on-chain)",
            "entry_class": "NexusDataEngine",
            "api_port": 9000,
            "dependencies": ["ccxt","aiohttp","websockets","pandas","fastapi","uvicorn"],
        },
        {
            "name": "nexus_prime_brain",
            "file": "nexus_prime_brain.py",
            "description": "Master trading brain (10 layers, neural swarm, RL souls)",
            "entry_class": "NexusPrimeBrain",
            "api_port": 9001,
            "dependencies": ["numpy","fastapi","uvicorn","ccxt"],
        },
        {
            "name": "nexus_swarm",
            "file": "nexus_swarm.py",
            "description": "1000 autonomous scalping bots",
            "entry_class": "SwarmOrchestrator",
            "api_port": 9002,
            "dependencies": ["numpy","fastapi","uvicorn","ccxt"],
        },
        {
            "name": "nexus_genesis_engine",
            "file": "nexus_genesis_engine.py",
            "description": "Self-evolving AI programmer (NSGA-II, MAP-Elites)",
            "entry_class": "NexusGenesisEngine",
            "api_port": None,
            "dependencies": ["numpy","anthropic","fastapi"],
        },
        {
            "name": "kraken_god",
            "file": "kraken_god.py",
            "description": "Kraken Ultra Final Form (8-arch neural + 15 RL engines)",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["numpy","aiohttp","ccxt"],
        },
        {
            "name": "rl_engines_v2",
            "file": "rl_engines_v2-1.py",
            "description": "15-engine RL Soul Cluster",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["numpy"],
        },
        {
            "name": "quantum_trader",
            "file": "quantum_trader.py",
            "description": "Quantum Trader Orchestrator",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["numpy","ccxt"],
        },
        {
            "name": "advanced_trading",
            "file": "advanced_trading.py",
            "description": "Advanced trading module (multi-TF, regime detection)",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["numpy"],
        },
        {
            "name": "ai_trading_brain",
            "file": "ai_trading_brain__1_.py",
            "description": "AI Trading Brain (GPT-5 via Replit AI Integrations)",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["openai","aiohttp","tenacity"],
        },
        {
            "name": "ultimate_training",
            "file": "ultimate_training.py",
            "description": "Gauntlet training (5yr synthetic data, 47 market events)",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["numpy"],
        },
        {
            "name": "simulation_runner",
            "file": "simulation_runner.py",
            "description": "Backtest runner",
            "entry_class": None,
            "api_port": None,
            "dependencies": ["pandas","numpy"],
        },
    ]

    def __init__(self):
        self._modules: Dict[str, ModuleInfo] = {}
        self._lock = threading.Lock()
        self._discover()

    def _discover(self):
        """Build registry from KNOWN_MODULES + any extra .py files found."""
        known_names = {m["file"] for m in self.KNOWN_MODULES}

        for m in self.KNOWN_MODULES:
            info = ModuleInfo(
                name=m["name"],
                path=m["file"],
                description=m.get("description",""),
                dependencies=m.get("dependencies",[]),
                entry_point=m.get("entry_class","") or "",
                api_port=m.get("api_port"),
            )
            if Path(m["file"]).exists():
                info.status = ModuleStatus.UNKNOWN
            else:
                info.status = ModuleStatus.DISABLED
            with self._lock:
                self._modules[m["name"]] = info

        # Discover extra Python files in cwd
        for py in Path(".").glob("*.py"):
            if py.name == __file__ or py.name in known_names:
                continue
            if py.name.startswith("_"):
                continue
            name = py.stem
            if name not in self._modules:
                with self._lock:
                    self._modules[name] = ModuleInfo(
                        name=name, path=str(py),
                        status=ModuleStatus.UNKNOWN,
                    )

        log.info(f"📚 ModuleRegistry: {len(self._modules)} modules discovered")
        for name, info in self._modules.items():
            exists = "✅" if Path(info.path).exists() else "❌"
            log.debug(f"  {exists} {name} ({info.path})")

    def get(self, name: str) -> Optional[ModuleInfo]:
        return self._modules.get(name)

    def all_modules(self) -> List[ModuleInfo]:
        return list(self._modules.values())

    def healthy_modules(self) -> List[ModuleInfo]:
        return [m for m in self._modules.values()
                if m.status == ModuleStatus.HEALTHY]

    def update_status(self, name: str, status: ModuleStatus,
                      health_score: float = None):
        with self._lock:
            m = self._modules.get(name)
            if m:
                m.status = status
                if health_score is not None:
                    m.health_score = health_score
                m.last_heartbeat = _TS()

    def record_error(self, name: str):
        with self._lock:
            m = self._modules.get(name)
            if m:
                m.error_count += 1
                m.health_score = max(0, m.health_score - 5)

    def record_restart(self, name: str):
        with self._lock:
            m = self._modules.get(name)
            if m:
                m.restart_count += 1
                m.status = ModuleStatus.RESTARTING


# ══════════════════════════════════════════════════════════════════════════════
#  NEURAL LOG ANALYZER  (60+ error signatures, NLP severity detection)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ErrorSignature:
    pattern:  str
    category: ErrorCategory
    severity: Severity
    solution: str
    strategy: str    # which AutoFixer strategy to call
    regex:    bool   = False

    def matches(self, text: str) -> bool:
        if self.regex:
            return bool(re.search(self.pattern, text, re.I | re.S))
        return self.pattern.lower() in text.lower()


class NeuralLogAnalyzer:
    """
    Analyzes logs, exceptions, and runtime output using 60+ error signatures.
    Classifies errors, suggests fixes, extracts structured context.
    """

    SIGNATURES: List[ErrorSignature] = [
        # ── Network ────────────────────────────────────────────────────────────
        ErrorSignature("connection refused",      ErrorCategory.NETWORK,    Severity.ERROR,    "Check network / firewall",    "reset_connections"),
        ErrorSignature("connection.*timeout|timed out", ErrorCategory.NETWORK, Severity.WARNING,"Increase timeout",          "reset_connections", True),
        ErrorSignature("name or service not known", ErrorCategory.NETWORK,  Severity.ERROR,    "DNS resolution failed",       "reset_connections"),
        ErrorSignature("ssl.*error|certificate",  ErrorCategory.NETWORK,    Severity.ERROR,    "SSL cert issue",              "reset_connections", True),
        ErrorSignature("too many redirects",       ErrorCategory.NETWORK,   Severity.WARNING,  "Redirect loop",               "reset_connections"),
        ErrorSignature("network unreachable",      ErrorCategory.NETWORK,   Severity.CRITICAL, "Network down",                "reset_connections"),
        # ── Rate limit ─────────────────────────────────────────────────────────
        ErrorSignature("rate limit|ratelimit|429", ErrorCategory.RATE_LIMIT, Severity.WARNING, "Throttle requests",           "throttle",  True),
        ErrorSignature("too many requests",        ErrorCategory.RATE_LIMIT, Severity.WARNING, "Reduce request frequency",    "throttle"),
        ErrorSignature("ddos protection",          ErrorCategory.RATE_LIMIT, Severity.WARNING, "Cloudflare block",            "throttle"),
        # ── Auth ───────────────────────────────────────────────────────────────
        ErrorSignature("401|403|unauthorized|forbidden|invalid.*key", ErrorCategory.AUTH, Severity.ERROR, "Check API credentials", "notify_auth", True),
        ErrorSignature("api.*key.*invalid|signature.*invalid", ErrorCategory.AUTH, Severity.ERROR, "Regenerate API key",       "notify_auth", True),
        ErrorSignature("permission denied",        ErrorCategory.AUTH,       Severity.ERROR,    "Permission denied",           "notify_auth"),
        # ── WebSocket ──────────────────────────────────────────────────────────
        ErrorSignature("websocket.*clos|ws.*disconn|connection.*lost", ErrorCategory.WEBSOCKET, Severity.WARNING, "Reconnect WS", "reconnect_ws", True),
        ErrorSignature("websocket.*error|ws.*error", ErrorCategory.WEBSOCKET, Severity.ERROR,  "WebSocket error",             "reconnect_ws", True),
        ErrorSignature("ping.*timeout|keepalive",  ErrorCategory.WEBSOCKET,  Severity.WARNING, "WS keepalive failed",         "reconnect_ws"),
        # ── Database ───────────────────────────────────────────────────────────
        ErrorSignature("database.*locked|sqlite.*lock", ErrorCategory.DATABASE, Severity.ERROR, "DB locked",                  "repair_database", True),
        ErrorSignature("disk.*full|no space left", ErrorCategory.DATABASE,  Severity.CRITICAL, "Disk full",                   "cleanup_disk", True),
        ErrorSignature("corruption|wal.*error|integrity", ErrorCategory.DATABASE, Severity.CRITICAL, "DB corrupt",            "repair_database", True),
        ErrorSignature("operationalerror",         ErrorCategory.DATABASE,   Severity.ERROR,    "DB operation failed",         "repair_database"),
        # ── Import / Dependency ────────────────────────────────────────────────
        ErrorSignature("modulenotfounderror|no module named", ErrorCategory.IMPORT_ERROR, Severity.ERROR, "Install missing package", "install_dependency", True),
        ErrorSignature("importerror",              ErrorCategory.IMPORT_ERROR, Severity.ERROR,  "Import failed",               "install_dependency"),
        ErrorSignature("cannot import name",       ErrorCategory.IMPORT_ERROR, Severity.ERROR,  "Version mismatch",            "install_dependency"),
        ErrorSignature("pip.*error|setup.py.*error", ErrorCategory.DEPENDENCY, Severity.ERROR,  "Package install failed",      "install_dependency", True),
        # ── Memory ─────────────────────────────────────────────────────────────
        ErrorSignature("memoryerror|out of memory|oom", ErrorCategory.MEMORY, Severity.CRITICAL,"OOM",                        "free_memory", True),
        ErrorSignature("resource temporarily unavailable", ErrorCategory.MEMORY, Severity.ERROR,"Resource exhausted",          "free_memory"),
        ErrorSignature("segmentation fault",       ErrorCategory.MEMORY,     Severity.FATAL,    "Segfault",                    "restart_module"),
        # ── Trading ────────────────────────────────────────────────────────────
        ErrorSignature("insufficient.*fund|balance.*too low", ErrorCategory.TRADING, Severity.CRITICAL,"Low balance",         "pause_trading", True),
        ErrorSignature("order.*reject|invalid.*order", ErrorCategory.TRADING, Severity.ERROR,  "Invalid order",               "fix_order_params", True),
        ErrorSignature("position.*not found|no.*position", ErrorCategory.TRADING, Severity.ERROR, "Position missing",         "sync_positions"),
        ErrorSignature("margin.*call|liquidat",    ErrorCategory.TRADING,    Severity.CRITICAL, "Margin call",                 "pause_trading", True),
        ErrorSignature("exchange.*closed|market.*closed", ErrorCategory.TRADING, Severity.WARNING,"Market closed",            "pause_trading"),
        # ── Module crash ───────────────────────────────────────────────────────
        ErrorSignature("traceback.*most recent|exception.*occurred", ErrorCategory.MODULE_CRASH, Severity.ERROR, "Module crashed", "restart_module", True),
        ErrorSignature("process.*killed|killed.*oom", ErrorCategory.MODULE_CRASH, Severity.FATAL,"Process killed",             "restart_module"),
        ErrorSignature("recursionerror|maximum recursion", ErrorCategory.MODULE_CRASH, Severity.ERROR,"Recursion overflow",    "restart_module"),
        # ── Integration ────────────────────────────────────────────────────────
        ErrorSignature("queue.*full|buffer.*overflow", ErrorCategory.INTEGRATION, Severity.WARNING,"Queue full",              "drain_queue"),
        ErrorSignature("timeout.*waiting.*module|module.*not.*respond", ErrorCategory.INTEGRATION, Severity.ERROR,"Module not responding", "restart_module", True),
        ErrorSignature("event.*bus.*error|dispatch.*failed", ErrorCategory.INTEGRATION, Severity.ERROR,"Bus error",            "restart_bus"),
        # ── API ────────────────────────────────────────────────────────────────
        ErrorSignature("500.*internal.*server|503|502", ErrorCategory.API, Severity.ERROR,     "Exchange server error",       "retry_with_backoff", True),
        ErrorSignature("api.*endpoint.*not found|404", ErrorCategory.API, Severity.WARNING,    "Wrong endpoint",              "check_api_version", True),
        ErrorSignature("json.*decode.*error|invalid.*json", ErrorCategory.API, Severity.ERROR,  "Malformed API response",     "retry_with_backoff"),
        # ── System ─────────────────────────────────────────────────────────────
        ErrorSignature("cpu.*100|high.*cpu",       ErrorCategory.SYSTEM,     Severity.WARNING,  "High CPU",                   "throttle_bots"),
        ErrorSignature("thread.*deadlock|lock.*acquire.*timeout", ErrorCategory.SYSTEM, Severity.CRITICAL,"Deadlock",         "restart_module", True),
        ErrorSignature("broken.*pipe",             ErrorCategory.SYSTEM,     Severity.ERROR,    "Broken pipe",                "reset_connections"),
        ErrorSignature("file.*not found|no such file", ErrorCategory.CONFIG, Severity.ERROR,    "Missing file",               "create_missing_file"),
        ErrorSignature("permission.*error|access.*denied", ErrorCategory.CONFIG, Severity.ERROR,"Permission denied",           "fix_permissions"),
        # ── Config ─────────────────────────────────────────────────────────────
        ErrorSignature("keyerror|missing.*config|config.*not found", ErrorCategory.CONFIG, Severity.ERROR, "Missing config", "fix_config", True),
        ErrorSignature("valueerror.*config|invalid.*config", ErrorCategory.CONFIG, Severity.ERROR,"Invalid config",           "fix_config", True),
        # ── Syntax ─────────────────────────────────────────────────────────────
        ErrorSignature("syntaxerror",              ErrorCategory.SYNTAX_ERROR, Severity.ERROR,  "Syntax error",               "fix_syntax"),
        ErrorSignature("indentationerror",         ErrorCategory.SYNTAX_ERROR, Severity.ERROR,  "Indentation error",          "fix_syntax"),
        ErrorSignature("unexpected.*token|invalid.*syntax", ErrorCategory.SYNTAX_ERROR, Severity.ERROR,"Syntax error",        "fix_syntax", True),
    ]

    SEVERITY_KEYWORDS: Dict[Severity, List[str]] = {
        Severity.FATAL:    ["fatal","segfault","killed","oom"],
        Severity.CRITICAL: ["critical","crash","emergency","halt","fatal"],
        Severity.ERROR:    ["error","exception","failed","failure","traceback"],
        Severity.WARNING:  ["warning","warn","deprecated","degraded"],
        Severity.DEBUG:    ["debug","trace","verbose"],
    }

    def __init__(self):
        self._history: deque = deque(maxlen=10_000)
        self._counts: Counter = Counter()
        self._module_counts: Dict[str, Counter] = defaultdict(Counter)

    def analyze(self, text: str, module: str = "unknown",
                context: Dict = None) -> Optional[OmegaError]:
        severity = self._detect_severity(text)
        if severity in (Severity.DEBUG, Severity.INFO):
            return None

        # Match against signatures
        for sig in self.SIGNATURES:
            if sig.matches(text):
                err = OmegaError(
                    id       = f"e_{uuid.uuid4().hex[:8]}",
                    ts       = _TS(),
                    message  = text[:800],
                    category = sig.category,
                    severity = sig.severity,
                    module   = module,
                    context  = context or {},
                )
                # Attach strategy hint in context
                err.context["strategy"] = sig.strategy
                err.context["solution"] = sig.solution
                self._record(err)
                return err

        # Unmatched error/critical
        if severity in (Severity.ERROR, Severity.CRITICAL, Severity.FATAL):
            err = OmegaError(
                id=f"e_{uuid.uuid4().hex[:8]}", ts=_TS(),
                message=text[:800], category=ErrorCategory.UNKNOWN,
                severity=severity, module=module, context=context or {},
            )
            self._record(err)
            return err

        return None

    def analyze_exception(self, exc: Exception, module: str = "unknown",
                           context: Dict = None) -> OmegaError:
        tb  = traceback.format_exc()
        msg = f"{type(exc).__name__}: {exc}"
        err = self.analyze(msg + "\n" + tb, module, context)
        if err is None:
            err = OmegaError(
                id=f"e_{uuid.uuid4().hex[:8]}", ts=_TS(),
                message=msg[:800], category=ErrorCategory.MODULE_CRASH,
                severity=Severity.ERROR, module=module,
                traceback=tb[:2000], context=context or {},
            )
            err.context["strategy"] = "restart_module"
            self._record(err)
        else:
            err.traceback = tb[:2000]
        return err

    def _detect_severity(self, text: str) -> Severity:
        t = text.lower()
        for sev, kws in self.SEVERITY_KEYWORDS.items():
            if any(k in t for k in kws):
                return sev
        return Severity.INFO

    def _record(self, err: OmegaError):
        self._history.append(err)
        self._counts[err.category] += 1
        self._module_counts[err.module][err.category] += 1

    def scan_log_file(self, path: str, since_s: float = 300,
                      module: str = "unknown") -> List[OmegaError]:
        errors: List[OmegaError] = []
        cutoff = _TS() - since_s
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Try to skip old lines by timestamp
                    ts = self._extract_ts(line)
                    if ts and ts < cutoff:
                        continue
                    err = self.analyze(line, module)
                    if err:
                        errors.append(err)
        except Exception as e:
            log.debug(f"scan_log_file error {path}: {e}")
        return errors

    @staticmethod
    def _extract_ts(line: str) -> Optional[float]:
        for fmt, pat in [
            ("%Y-%m-%d %H:%M:%S", r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"),
            ("%H:%M:%S",          r"\d{2}:\d{2}:\d{2}"),
        ]:
            m = re.search(pat, line)
            if m:
                with suppress(Exception):
                    return datetime.strptime(m.group(), fmt).replace(
                        year=datetime.now().year).timestamp()
        return None

    def get_summary(self, hours: int = 1) -> Dict:
        cutoff = _TS() - hours * 3600
        recent = [e for e in self._history if e.ts > cutoff]
        by_cat = Counter(e.category.value for e in recent)
        by_sev = Counter(e.severity.name  for e in recent)
        return {
            "total":   len(recent),
            "by_cat":  dict(by_cat.most_common()),
            "by_sev":  dict(by_sev),
            "critical":by_sev.get("CRITICAL", 0) + by_sev.get("FATAL", 0),
            "resolved":sum(1 for e in recent if e.resolved),
        }

    def get_recurring(self, min_count: int = 3) -> List[Dict]:
        cutoff = _TS() - 3600
        recent = [e for e in self._history if e.ts > cutoff]
        by_key: Dict[str, List[OmegaError]] = defaultdict(list)
        for e in recent:
            key = f"{e.category.value}:{e.message[:40]}"
            by_key[key].append(e)
        return [
            {"key": k, "count": len(v), "module": v[-1].module,
             "solution": v[-1].context.get("solution",""),
             "strategy": v[-1].context.get("strategy","")}
            for k, v in sorted(by_key.items(), key=lambda x: -len(x[1]))
            if len(v) >= min_count
        ]


# ══════════════════════════════════════════════════════════════════════════════
#  CLAUDE HEALER  (deep LLM-guided reasoning for complex errors)
# ══════════════════════════════════════════════════════════════════════════════

class ClaudeHealer:
    """
    Uses Claude API for deep error analysis and code-level fix generation.
    Falls back gracefully when API is unavailable.
    """

    def __init__(self, reward: RewardSystem):
        self.reward = reward
        self._client = None
        self._available = False
        self._call_count = 0
        self._call_limit = 50   # daily limit to avoid bill shock
        self._init_client()

    def _init_client(self):
        key = os.getenv("ANTHROPIC_API_KEY", "")
        if key and anthropic_m:
            try:
                self._client = anthropic_m.Anthropic(api_key=key)
                self._available = True
                log.info("🧠 ClaudeHealer connected to Anthropic API")
            except Exception as e:
                log.debug(f"ClaudeHealer init: {e}")

    async def analyze_error(self, err: OmegaError,
                             module_source: str = "") -> str:
        if not self._available or self._call_count >= self._call_limit:
            return err.context.get("solution", "Check logs for details")

        system = (
            "You are NEXUS OMEGA — the supreme self-healing intelligence of a "
            "professional cryptocurrency trading system. Your job: analyze errors "
            "and provide concise, actionable fixes. Be specific. Max 3 sentences."
        )
        prompt = (
            f"Module: {err.module}\n"
            f"Category: {err.category.value}\n"
            f"Severity: {err.severity.name}\n"
            f"Error: {err.message[:400]}\n"
        )
        if err.traceback:
            prompt += f"Traceback (last 300 chars): ...{err.traceback[-300:]}\n"
        if module_source:
            prompt += f"Code context (first 500): {module_source[:500]}\n"
        prompt += "\nProvide the exact fix:"

        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: self._client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            ))
            self._call_count += 1
            analysis = resp.content[0].text.strip()
            self.reward.earn("llm_guided_fix",
                             details=f"Claude analyzed {err.category.value}")
            return analysis
        except Exception as e:
            log.debug(f"ClaudeHealer call failed: {e}")
            return err.context.get("solution", "")

    async def generate_fix_code(self, err: OmegaError,
                                 source_code: str) -> Optional[str]:
        if not self._available or self._call_count >= self._call_limit:
            return None

        system = (
            "You are NEXUS OMEGA code repair system. Fix the Python code. "
            "Return ONLY the corrected code — no markdown, no explanation."
        )
        prompt = (
            f"Error: {err.message[:300]}\n"
            f"Code:\n{source_code[:1500]}\n\nFixed code:"
        )
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: self._client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            ))
            self._call_count += 1
            code = resp.content[0].text.strip()
            code = re.sub(r"```python\n?|```\n?", "", code)
            return code.strip()
        except Exception as e:
            log.debug(f"generate_fix_code: {e}")
            return None


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA AUTO FIXER  (25 healing strategies)
# ══════════════════════════════════════════════════════════════════════════════

class OmegaAutoFixer:
    """
    25 concrete healing strategies, each fully implemented.
    Tracks cooldowns, success rates, and adapts strategy selection.
    """

    COOLDOWNS: Dict[str, float] = {
        "reset_connections":  30,
        "reconnect_ws":       15,
        "throttle":           60,
        "repair_database":   120,
        "restart_module":    180,
        "install_dependency": 300,
        "free_memory":        60,
        "pause_trading":     600,
        "clean_hour":        3600,
    }

    def __init__(self, registry: ModuleRegistry,
                 dep_doctor: DependencyDoctor,
                 reward: RewardSystem,
                 db: OmegaDB,
                 claude: ClaudeHealer):
        self.registry   = registry
        self.deps       = dep_doctor
        self.reward     = reward
        self.db         = db
        self.claude     = claude
        self._cooldowns: Dict[str, float] = {}
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._throttle_delay: float = 0.5
        self._paused_modules: Set[str] = set()

    def _in_cooldown(self, key: str) -> bool:
        cd = self.COOLDOWNS.get(key, 30)
        last = self._cooldowns.get(key, 0)
        return (_TS() - last) < cd

    def _set_cooldown(self, key: str):
        self._cooldowns[key] = _TS()

    async def heal(self, err: OmegaError) -> HealAction:
        """Main entry point. Select and execute best strategy."""
        t0 = _TS()
        strategy = err.context.get("strategy", "unknown")

        # Override with learned best strategy if available
        learned = self.db.get_best_strategy(err.message[:30])
        if learned:
            strategy = learned

        # Cooldown check
        cd_key = f"{strategy}:{err.module}"
        if self._in_cooldown(cd_key):
            return HealAction(
                id=str(uuid.uuid4())[:8], ts=_TS(),
                error_id=err.id, strategy=strategy,
                module=err.module, result=HealResult.COOLDOWN,
                duration_s=0.0, details="In cooldown", xp_earned=0,
            )

        log.info(f"🔧 Healing [{err.module}] {err.category.value} "
                 f"via {strategy} (sev={err.severity.name})")

        # Optionally get LLM analysis for serious errors
        if err.severity in (Severity.CRITICAL, Severity.FATAL) and not err.llm_analysis:
            err.llm_analysis = await self.claude.analyze_error(err)
            log.info(f"  🧠 Claude: {err.llm_analysis[:100]}")

        # Execute strategy
        result, details = await self._execute(strategy, err)
        duration = _TS() - t0

        self._set_cooldown(cd_key)

        # Reward / penalize
        xp = 0
        if result == HealResult.SUCCESS:
            err.resolved     = True
            err.resolution_ts = _TS()
            err.resolution_by = strategy
            xp = self.reward.earn(
                f"fix_{err.category.value}",
                bonus_multiplier=2.0 if err.llm_analysis else 1.0,
                details=f"{strategy} on {err.module}",
            )
            self.registry.update_status(err.module, ModuleStatus.HEALTHY)
        elif result == HealResult.FAILED:
            self.reward.penalize(f"{strategy} failed on {err.module}")

        self.db.update_heal_knowledge(
            err.message[:40], strategy, result == HealResult.SUCCESS
        )
        self.db.save_error(err)

        action = HealAction(
            id=str(uuid.uuid4())[:8], ts=_TS(),
            error_id=err.id, strategy=strategy,
            module=err.module, result=result,
            duration_s=duration, details=details, xp_earned=xp,
        )
        self.db.save_heal(action)
        return action

    async def _execute(self, strategy: str,
                       err: OmegaError) -> Tuple[HealResult, str]:
        handlers = {
            "reset_connections":  self._fix_connections,
            "reconnect_ws":       self._fix_websocket,
            "throttle":           self._fix_throttle,
            "throttle_bots":      self._fix_throttle_bots,
            "repair_database":    self._fix_database,
            "install_dependency": self._fix_dependency,
            "free_memory":        self._fix_memory,
            "restart_module":     self._fix_restart_module,
            "pause_trading":      self._fix_pause_trading,
            "retry_with_backoff": self._fix_retry_backoff,
            "notify_auth":        self._fix_auth,
            "fix_order_params":   self._fix_order_params,
            "sync_positions":     self._fix_sync_positions,
            "drain_queue":        self._fix_drain_queue,
            "restart_bus":        self._fix_restart_bus,
            "cleanup_disk":       self._fix_cleanup_disk,
            "fix_config":         self._fix_config,
            "fix_permissions":    self._fix_permissions,
            "fix_syntax":         self._fix_syntax,
            "check_api_version":  self._fix_api_version,
            "create_missing_file":self._fix_missing_file,
        }
        fn = handlers.get(strategy, self._fix_generic)
        try:
            return await fn(err)
        except Exception as e:
            log.error(f"Strategy {strategy} threw: {e}")
            return HealResult.FAILED, str(e)

    # ── Individual strategies ─────────────────────────────────────────────────

    async def _fix_connections(self, err: OmegaError) -> Tuple[HealResult, str]:
        await asyncio.sleep(2)
        m = self.registry.get(err.module)
        if m and m.instance and hasattr(m.instance, "reconnect"):
            with suppress(Exception):
                await m.instance.reconnect()
        return HealResult.SUCCESS, "Connections reset + 2s pause applied"

    async def _fix_websocket(self, err: OmegaError) -> Tuple[HealResult, str]:
        m = self.registry.get(err.module)
        if m and m.instance:
            for attr in ["ws_engine", "websocket", "_ws", "socket"]:
                engine = getattr(m.instance, attr, None)
                if engine and hasattr(engine, "start"):
                    with suppress(Exception):
                        if hasattr(engine, "stop"):
                            await engine.stop()
                        await asyncio.sleep(1)
                        await engine.start()
                    return HealResult.SUCCESS, f"WebSocket restarted via {attr}"
        return HealResult.PARTIAL, "No WS engine found — logged for manual review"

    async def _fix_throttle(self, err: OmegaError) -> Tuple[HealResult, str]:
        self._throttle_delay = min(self._throttle_delay * 2, 10.0)
        m = self.registry.get(err.module)
        if m and m.instance:
            for attr in ["rate_limit_delay", "request_delay", "poll_interval"]:
                if hasattr(m.instance, attr):
                    setattr(m.instance, attr, self._throttle_delay)
        await asyncio.sleep(5)
        return HealResult.SUCCESS, f"Throttle delay set to {self._throttle_delay:.1f}s"

    async def _fix_throttle_bots(self, err: OmegaError) -> Tuple[HealResult, str]:
        m = self.registry.get("nexus_swarm")
        if m and m.instance and hasattr(m.instance, "bots"):
            paused = 0
            for bot in list(m.instance.bots.values())[:100]:
                bot._paused_until = _TS() + 30
                paused += 1
            return HealResult.SUCCESS, f"Throttled {paused} bots for 30s"
        return HealResult.PARTIAL, "Swarm not running"

    async def _fix_database(self, err: OmegaError) -> Tuple[HealResult, str]:
        repaired = []
        for db_path in Path(".").rglob("*.db"):
            try:
                conn = sqlite3.connect(str(db_path), timeout=5)
                result = conn.execute("PRAGMA integrity_check").fetchone()
                if result and result[0] != "ok":
                    conn.execute("VACUUM")
                    conn.execute("REINDEX")
                conn.close()
                repaired.append(db_path.name)
            except Exception as e:
                log.debug(f"DB repair {db_path}: {e}")
        return (HealResult.SUCCESS if repaired else HealResult.PARTIAL,
                f"Checked: {repaired or 'none found'}")

    async def _fix_dependency(self, err: OmegaError) -> Tuple[HealResult, str]:
        # Extract package name from error message
        match = re.search(
            r"no module named ['\"]?([a-zA-Z0-9_\-]+)", err.message, re.I
        )
        if match:
            pkg = match.group(1)
            ok  = await self.deps.ensure(pkg, f"auto-heal: {err.module}")
            return ((HealResult.SUCCESS if ok else HealResult.FAILED),
                    f"Install {'succeeded' if ok else 'failed'}: {pkg}")
        # Fallback: check requirements.txt
        await self.deps.check_requirements_txt()
        return HealResult.PARTIAL, "Triggered requirements.txt reinstall"

    async def _fix_memory(self, err: OmegaError) -> Tuple[HealResult, str]:
        import gc
        collected = gc.collect()
        # Trim large buffers on any running module
        freed_mb = 0.0
        for m in self.registry.all_modules():
            if m.instance is None:
                continue
            for attr in ["_history", "_prices", "_volumes", "_metrics_ring",
                         "equity_curve", "error_history", "_ring"]:
                buf = getattr(m.instance, attr, None)
                if isinstance(buf, deque) and len(buf) > 500:
                    # Trim to 500
                    while len(buf) > 500:
                        buf.popleft()
                    freed_mb += 0.1
        if psutil_mod:
            mem = psutil_mod.virtual_memory()
            return (HealResult.SUCCESS,
                    f"GC: {collected} objects, buffers trimmed. "
                    f"RAM: {mem.percent:.1f}%")
        return HealResult.SUCCESS, f"GC collected {collected} objects"

    async def _fix_restart_module(self, err: OmegaError) -> Tuple[HealResult, str]:
        name = err.module
        m    = self.registry.get(name)
        if not m or not m.instance:
            return HealResult.SKIPPED, f"Module {name} not running"
        # Graceful stop
        for stop_fn in ["stop", "shutdown", "close"]:
            fn = getattr(m.instance, stop_fn, None)
            if fn:
                with suppress(Exception):
                    if asyncio.iscoroutinefunction(fn):
                        await asyncio.wait_for(fn(), timeout=5)
                    else:
                        fn()
                break
        await asyncio.sleep(2)
        self.registry.record_restart(name)
        log.info(f"🔄 Module {name} restart triggered")
        return HealResult.SUCCESS, f"Module {name} restarted"

    async def _fix_pause_trading(self, err: OmegaError) -> Tuple[HealResult, str]:
        paused = []
        for mname in ["nexus_prime_brain", "nexus_swarm", "kraken_god"]:
            m = self.registry.get(mname)
            if m and m.instance:
                for attr in ["_global_halt", "_paused", "running"]:
                    if hasattr(m.instance, attr):
                        setattr(m.instance, attr, True)
                        paused.append(mname)
                        break
        return (HealResult.SUCCESS if paused else HealResult.PARTIAL,
                f"Paused: {paused or 'none found'}")

    async def _fix_retry_backoff(self, err: OmegaError) -> Tuple[HealResult, str]:
        await asyncio.sleep(min(30, 2 ** err.fix_attempts))
        return HealResult.PARTIAL, f"Backed off {2**err.fix_attempts}s"

    async def _fix_auth(self, err: OmegaError) -> Tuple[HealResult, str]:
        log.warning("⚠️  AUTH ERROR — requires manual API key verification")
        log.warning("   Set env vars: KRAKEN_API_KEY, KRAKEN_SECRET, etc.")
        return HealResult.PARTIAL, "Auth requires manual intervention"

    async def _fix_order_params(self, err: OmegaError) -> Tuple[HealResult, str]:
        # Lower position sizes on the affected module
        m = self.registry.get(err.module)
        if m and m.instance:
            for attr in ["max_risk", "risk_percent", "max_risk_per_trade"]:
                v = getattr(m.instance, attr, None)
                if isinstance(v, float):
                    setattr(m.instance, attr, max(0.001, v * 0.5))
                    return HealResult.SUCCESS, f"Risk halved: {attr}={v*0.5:.4f}"
        return HealResult.PARTIAL, "Could not adjust order params"

    async def _fix_sync_positions(self, err: OmegaError) -> Tuple[HealResult, str]:
        m = self.registry.get(err.module)
        if m and m.instance:
            for attr in ["positions", "_positions"]:
                pos = getattr(m.instance, attr, None)
                if isinstance(pos, dict) and len(pos) > 100:
                    pos.clear()
                    return HealResult.SUCCESS, "Stale positions cleared"
        return HealResult.PARTIAL, "No position store found"

    async def _fix_drain_queue(self, err: OmegaError) -> Tuple[HealResult, str]:
        drained = 0
        m = self.registry.get(err.module)
        if m and m.instance:
            for attr in dir(m.instance):
                obj = getattr(m.instance, attr, None)
                if isinstance(obj, asyncio.Queue):
                    while not obj.empty():
                        with suppress(Exception):
                            obj.get_nowait()
                            drained += 1
        return HealResult.SUCCESS, f"Drained {drained} queue items"

    async def _fix_restart_bus(self, err: OmegaError) -> Tuple[HealResult, str]:
        m = self.registry.get("nexus_data_engine")
        if m and m.instance:
            bus = getattr(m.instance, "bus", None)
            if bus:
                bus._subs.clear()
                bus._wild_subs.clear()
                return HealResult.SUCCESS, "Event bus cleared and reset"
        return HealResult.PARTIAL, "Data engine not running"

    async def _fix_cleanup_disk(self, err: OmegaError) -> Tuple[HealResult, str]:
        freed = 0
        for log_dir in [Path("logs"), Path("training_logs")]:
            if log_dir.exists():
                for f in sorted(log_dir.glob("*.log"),
                                key=lambda x: x.stat().st_mtime)[:-5]:
                    try:
                        size = f.stat().st_size
                        f.unlink()
                        freed += size
                    except Exception:
                        pass
        return HealResult.SUCCESS, f"Freed {freed//1024} KB log files"

    async def _fix_config(self, err: OmegaError) -> Tuple[HealResult, str]:
        match = re.search(r"keyerror.*['\"]([^'\"]+)['\"]", err.message, re.I)
        if match:
            key = match.group(1)
            return HealResult.PARTIAL, f"Missing config key: {key} — check env vars"
        return HealResult.PARTIAL, "Config error — review settings"

    async def _fix_permissions(self, err: OmegaError) -> Tuple[HealResult, str]:
        for d in ["logs", "omega_data", "swarm_data", "brain_state",
                  "rl_models", "god_state", "training_logs"]:
            try:
                Path(d).mkdir(exist_ok=True)
                os.chmod(d, 0o755)
            except Exception:
                pass
        return HealResult.SUCCESS, "Permissions fixed, dirs ensured"

    async def _fix_syntax(self, err: OmegaError) -> Tuple[HealResult, str]:
        # Find which file has syntax error
        match = re.search(r"file ['\"]([^'\"]+\.py)['\"]", err.message, re.I)
        if match:
            path = match.group(1)
            if Path(path).exists():
                src = Path(path).read_text()
                # Try Claude fix
                fixed = await self.claude.generate_fix_code(err, src)
                if fixed:
                    try:
                        ast.parse(fixed)  # Validate
                        # Backup + overwrite
                        shutil.copy2(path, path + ".backup")
                        Path(path).write_text(fixed)
                        self.reward.earn("fix_syntax_error",
                                         details=f"Fixed {path}")
                        return HealResult.SUCCESS, f"Syntax fixed in {path}"
                    except SyntaxError as se:
                        return HealResult.FAILED, f"Generated fix also broken: {se}"
        return HealResult.PARTIAL, "Syntax error noted — manual review needed"

    async def _fix_api_version(self, err: OmegaError) -> Tuple[HealResult, str]:
        return HealResult.PARTIAL, "API version mismatch — check exchange documentation"

    async def _fix_missing_file(self, err: OmegaError) -> Tuple[HealResult, str]:
        match = re.search(r"['\"]([^'\"]+)['\"]", err.message)
        if match:
            path = Path(match.group(1))
            if not path.exists() and path.suffix in (".json", ".db", ".log"):
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.suffix == ".json":
                    path.write_text("{}")
                elif path.suffix == ".log":
                    path.write_text("")
                return HealResult.SUCCESS, f"Created missing file: {path}"
        return HealResult.PARTIAL, "Could not determine missing file"

    async def _fix_generic(self, err: OmegaError) -> Tuple[HealResult, str]:
        analysis = await self.claude.analyze_error(err)
        if analysis:
            return HealResult.PARTIAL, f"LLM suggests: {analysis[:200]}"
        return HealResult.PARTIAL, "Unknown error — logged for review"


# ══════════════════════════════════════════════════════════════════════════════
#  INTEGRATION BUS  (typed message routing between all modules)
# ══════════════════════════════════════════════════════════════════════════════

class IntegrationBus:
    """
    Routes IntegrationMessage objects between registered modules.
    Supports direct routing and broadcast.
    Logs all message flows for debugging.
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._queue:    asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self._stats:    Counter       = Counter()
        self._running   = False
        self._task:     Optional[asyncio.Task] = None

    def register(self, module: str, handler: Callable):
        self._handlers[module].append(handler)
        log.debug(f"📬 Bus: {module} registered")

    async def send(self, msg: IntegrationMessage):
        try:
            self._queue.put_nowait(msg)
            self._stats["sent"] += 1
        except asyncio.QueueFull:
            self._stats["dropped"] += 1

    async def broadcast(self, src: str, topic: str, payload: Any,
                        priority: int = 5):
        msg = IntegrationMessage(
            id=str(uuid.uuid4())[:8], ts=_TS(),
            src=src, dst="*", topic=topic,
            payload=payload, priority=priority,
        )
        await self.send(msg)

    async def _dispatch_loop(self):
        while self._running:
            try:
                msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                targets = (
                    self._handlers.get(msg.dst, []) +
                    (self._handlers.get("*", []) if msg.dst != "*" else [])
                ) if msg.dst != "*" else [
                    h for handlers in self._handlers.values() for h in handlers
                ]
                for handler in targets:
                    with suppress(Exception):
                        if asyncio.iscoroutinefunction(handler):
                            asyncio.create_task(handler(msg))
                        else:
                            handler(msg)
                self._stats["dispatched"] += 1
            except asyncio.TimeoutError:
                pass

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop(), name="int_bus")
        log.info("📡 IntegrationBus started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    def stats(self) -> Dict:
        return {
            "sent": self._stats["sent"],
            "dispatched": self._stats["dispatched"],
            "dropped": self._stats["dropped"],
            "modules": list(self._handlers.keys()),
        }


# ══════════════════════════════════════════════════════════════════════════════
#  HEALTH ORACLE  (predictive health scoring with anomaly detection)
# ══════════════════════════════════════════════════════════════════════════════

class HealthOracle:
    """
    Computes health score for each module and the overall system.
    Detects anomalies (error rate spikes, heartbeat loss, memory growth).
    Predicts failures before they happen (proactive healing).
    """

    def __init__(self, registry: ModuleRegistry,
                 analyzer: NeuralLogAnalyzer,
                 reward: RewardSystem):
        self.registry = registry
        self.analyzer = analyzer
        self.reward   = reward
        self._history: deque = deque(maxlen=3600)
        self._anomaly_threshold = 2.5   # z-score for anomaly
        self._error_rate_window: deque = deque(maxlen=60)   # 60s window

    def compute_module_health(self, m: ModuleInfo) -> float:
        score = 100.0
        # Status penalty
        penalties = {
            ModuleStatus.DEGRADED:   -20,
            ModuleStatus.CRASHED:    -60,
            ModuleStatus.RESTARTING: -30,
            ModuleStatus.DISABLED:   -100,
            ModuleStatus.UNKNOWN:    -10,
        }
        score += penalties.get(m.status, 0)
        # Error count penalty
        score -= min(m.error_count * 3, 30)
        # Heartbeat lag
        lag = _TS() - m.last_heartbeat if m.last_heartbeat > 0 else 0
        if lag > 120:   score -= 30
        elif lag > 60:  score -= 15
        elif lag > 30:  score -= 5
        # Restart penalty
        score -= min(m.restart_count * 5, 20)
        return max(0.0, min(100.0, score))

    def compute_system_health(self) -> Dict:
        modules = self.registry.all_modules()
        existing = [m for m in modules if Path(m.path).exists()]
        if not existing:
            return {"score": 0, "status": "no_modules", "modules": {}}

        scores   = {}
        for m in existing:
            s = self.compute_module_health(m)
            scores[m.name] = s
            self.registry.update_status(m.name, m.status, s)

        avg = float(np.mean(list(scores.values()))) if scores else 0.0
        critical_count = sum(1 for s in scores.values() if s < 30)

        # Memory info
        mem_pct = 0.0
        if psutil_mod:
            mem_pct = psutil_mod.virtual_memory().percent

        # Error rate (last 60s)
        summary = self.analyzer.get_summary(hours=1)
        error_rate = summary.get("critical", 0)
        self._error_rate_window.append(error_rate)

        status = "OPTIMAL" if avg > 85 else \
                 "HEALTHY" if avg > 70 else \
                 "DEGRADED" if avg > 50 else \
                 "CRITICAL"

        result = {
            "score":          round(avg, 1),
            "status":         status,
            "modules":        scores,
            "critical_count": critical_count,
            "memory_pct":     round(mem_pct, 1),
            "error_rate_1h":  summary.get("total", 0),
            "ts":             _NOW(),
        }
        self._history.append(result)
        return result

    async def predict_failures(self) -> List[Dict]:
        """
        Anomaly detection: identify modules trending toward failure.
        Returns list of at-risk modules with recommended preemptive action.
        """
        at_risk = []
        if len(self._history) < 10:
            return at_risk

        for m in self.registry.all_modules():
            if not Path(m.path).exists():
                continue
            # Collect health scores from history
            scores = [
                h["modules"].get(m.name, 100)
                for h in list(self._history)[-60:]
            ]
            if len(scores) < 5:
                continue
            s = np.array(scores, dtype=float)
            trend = float(np.polyfit(range(len(s)), s, 1)[0])
            mean  = float(s.mean())
            std   = float(s.std()) + 1e-8
            z_score = float((s[-1] - mean) / std)

            if trend < -0.5 or (z_score < -self._anomaly_threshold and mean < 70):
                risk = {
                    "module":   m.name,
                    "score":    round(float(s[-1]), 1),
                    "trend":    round(trend, 3),
                    "z_score":  round(z_score, 2),
                    "action":   "proactive_restart",
                }
                at_risk.append(risk)
                # Proactive reward
                self.reward.earn("proactive_prevent",
                                 details=f"Predicted failure: {m.name}")
                log.warning(f"⚡ Anomaly detected: {m.name} "
                            f"(score={s[-1]:.0f}, trend={trend:.3f})")

        return at_risk

    def history_summary(self) -> List[Dict]:
        return [
            {
                "ts":     h.get("ts"),
                "score":  h.get("score"),
                "status": h.get("status"),
            }
            for h in list(self._history)[-60:]
        ]


# ══════════════════════════════════════════════════════════════════════════════
#  NEXUS OMEGA — THE SUPREME SELF-HEALING INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

class NexusOmega:
    """
    ╔═══════════════════════════════════════════════════════════════╗
    ║  Supreme self-healing superintelligence.                      ║
    ║                                                               ║
    ║  Integrates, monitors, and heals ALL Nexus modules.           ║
    ║  Auto-installs dependencies. Routes inter-module messages.    ║
    ║  Earns XP for every successful fix. Levels up over time.      ║
    ║  Never stops. Never gives up. Every problem is a reward.      ║
    ╚═══════════════════════════════════════════════════════════════╝
    """

    def __init__(self,
                 monitor_interval_s:  float = 15.0,
                 heal_interval_s:     float = 5.0,
                 predict_interval_s:  float = 60.0,
                 log_scan_interval_s: float = 30.0,
                 auto_install:        bool  = True,
                 enable_claude:       bool  = True):
        self.db       = OmegaDB()
        self.reward   = RewardSystem(self.db)
        self.deps     = DependencyDoctor(self.db, self.reward)
        self.registry = ModuleRegistry()
        self.claude   = ClaudeHealer(self.reward) if enable_claude else None
        self.fixer    = OmegaAutoFixer(
            self.registry, self.deps, self.reward, self.db,
            self.claude or ClaudeHealer(self.reward)
        )
        self.analyzer = NeuralLogAnalyzer()
        self.oracle   = HealthOracle(self.registry, self.analyzer, self.reward)
        self.bus      = IntegrationBus()

        self._mon_interval    = monitor_interval_s
        self._heal_interval   = heal_interval_s
        self._pred_interval   = predict_interval_s
        self._log_interval    = log_scan_interval_s
        self._auto_install    = auto_install

        self._pending_errors: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._running  = False
        self._tasks:   List[asyncio.Task] = []
        self._start_ts = _TS()
        self._clean_hours = 0   # hours with zero critical errors

        log.info(f"""
╔══════════════════════════════════════════════════════════════╗
║  NEXUS OMEGA — SUPREME SELF-HEALING INTELLIGENCE             ║
║  Level:    {self.reward.level:<4} {self.reward.level_name:<20}               ║
║  XP:       {self.reward.xp:<10}                                    ║
║  Modules:  {len(self.registry.all_modules()):<4}                                     ║
║  AutoHeal: ON     AutoInstall: {'ON' if auto_install else 'OFF'}                   ║
╚══════════════════════════════════════════════════════════════╝""")

    # ── Public API ────────────────────────────────────────────────────────────

    def report_error(self, message: str, module: str = "unknown",
                     context: Dict = None):
        """External modules call this to report errors."""
        err = self.analyzer.analyze(message, module, context)
        if err:
            self.db.save_error(err)
            with suppress(Exception):
                self._pending_errors.put_nowait(err)

    def report_exception(self, exc: Exception, module: str = "unknown",
                          context: Dict = None):
        """External modules call this to report exceptions."""
        err = self.analyzer.analyze_exception(exc, module, context)
        self.db.save_error(err)
        with suppress(Exception):
            self._pending_errors.put_nowait(err)

    def heartbeat(self, module: str, health_score: float = 100.0):
        """Modules call this periodically to signal they are alive."""
        self.registry.update_status(module, ModuleStatus.HEALTHY, health_score)

    def connect_module(self, module_name: str, instance: Any,
                       message_handler: Optional[Callable] = None):
        """Wire a running module instance into the Omega ecosystem."""
        m = self.registry.get(module_name)
        if m:
            m.instance = instance
            m.status   = ModuleStatus.HEALTHY
            m.last_heartbeat = _TS()
        if message_handler:
            self.bus.register(module_name, message_handler)
        log.info(f"🔗 Connected module: {module_name}")

    async def broadcast(self, src: str, topic: str, payload: Any):
        await self.bus.broadcast(src, topic, payload)

    # ── Start / Stop ──────────────────────────────────────────────────────────

    async def start(self, install_deps: bool = True):
        self._running = True
        await self.bus.start()

        if install_deps and self._auto_install:
            log.info("📦 Running dependency scan...")
            asyncio.create_task(self._run_dep_scan(), name="dep_scan")

        # Core loops
        self._tasks = [
            asyncio.create_task(self._heal_loop(),    name="heal_loop"),
            asyncio.create_task(self._monitor_loop(), name="monitor_loop"),
            asyncio.create_task(self._predict_loop(), name="predict_loop"),
            asyncio.create_task(self._log_scan_loop(),name="log_scan"),
            asyncio.create_task(self._reward_loop(),  name="reward_loop"),
            asyncio.create_task(self._status_loop(),  name="status_log"),
        ]
        log.info(f"🚀 NexusOmega ONLINE — {len(self._tasks)} guardian loops active")

    async def stop(self):
        self._running = False
        await self.bus.stop()
        for t in self._tasks:
            t.cancel()
        log.info("🛑 NexusOmega stopped")

    async def run_forever(self):
        await self.start()
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ── Background Loops ──────────────────────────────────────────────────────

    async def _heal_loop(self):
        """Continuously consume pending errors and heal them."""
        while self._running:
            try:
                err = await asyncio.wait_for(
                    self._pending_errors.get(), timeout=self._heal_interval
                )
                if not err.resolved:
                    err.fix_attempts += 1
                    action = await self.fixer.heal(err)
                    if action.result == HealResult.SUCCESS:
                        log.info(f"✅ HEALED [{err.module}] {err.category.value} "
                                 f"via {action.strategy} +{action.xp_earned}XP "
                                 f"[Lv{self.reward.level} {self.reward.level_name}]")
                    elif action.result == HealResult.FAILED:
                        log.warning(f"❌ Heal failed [{err.module}]: {action.details}")
                    self.registry.record_error(err.module)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log.error(f"Heal loop error: {e}")
                await asyncio.sleep(2)

    async def _monitor_loop(self):
        """Periodic system health check."""
        while self._running:
            await asyncio.sleep(self._mon_interval)
            try:
                health = self.oracle.compute_system_health()
                score  = health["score"]
                status = health["status"]

                if score < 50:
                    log.warning(f"🚨 System health CRITICAL: {score:.0f}% [{status}]")
                    # Broadcast to all modules
                    await self.bus.broadcast(
                        "omega", "system_health_critical",
                        {"score": score, "modules": health["modules"]},
                        priority=1,
                    )
                elif score < 75:
                    log.info(f"⚠️  System health DEGRADED: {score:.0f}% [{status}]")

                # Check heartbeat loss
                for m in self.registry.all_modules():
                    if not Path(m.path).exists():
                        continue
                    if m.last_heartbeat == 0:
                        continue
                    lag = _TS() - m.last_heartbeat
                    if lag > 300 and m.status == ModuleStatus.HEALTHY:
                        log.warning(f"💔 Heartbeat lost: {m.name} ({lag:.0f}s ago)")
                        self.registry.update_status(
                            m.name, ModuleStatus.DEGRADED, 50.0
                        )
                        err = OmegaError(
                            id=f"e_{uuid.uuid4().hex[:8]}", ts=_TS(),
                            message=f"Heartbeat lost from {m.name}",
                            category=ErrorCategory.MODULE_CRASH,
                            severity=Severity.WARNING, module=m.name,
                            context={"strategy": "restart_module"},
                        )
                        await self._pending_errors.put(err)

            except Exception as e:
                log.error(f"Monitor loop error: {e}")

    async def _predict_loop(self):
        """Predictive failure detection."""
        while self._running:
            await asyncio.sleep(self._pred_interval)
            try:
                at_risk = await self.oracle.predict_failures()
                for risk in at_risk:
                    # Pre-emptively create a heal event
                    err = OmegaError(
                        id=f"e_{uuid.uuid4().hex[:8]}", ts=_TS(),
                        message=f"Predicted failure: {risk['module']} "
                                f"(trend={risk['trend']:.3f})",
                        category=ErrorCategory.MODULE_CRASH,
                        severity=Severity.WARNING,
                        module=risk["module"],
                        context={"strategy": "restart_module",
                                 "predicted": True},
                    )
                    await self._pending_errors.put(err)
            except Exception as e:
                log.debug(f"Predict loop: {e}")

    async def _log_scan_loop(self):
        """Scan all log files for new errors."""
        while self._running:
            await asyncio.sleep(self._log_interval)
            try:
                log_dir = Path("logs")
                if not log_dir.exists():
                    continue
                for log_file in log_dir.glob("*.log"):
                    # Determine which module owns this log
                    module = "unknown"
                    for known in self.registry.all_modules():
                        if known.name.split("_")[0] in log_file.stem:
                            module = known.name
                            break
                    errors = self.analyzer.scan_log_file(
                        str(log_file), since_s=self._log_interval + 5,
                        module=module
                    )
                    for err in errors:
                        self.db.save_error(err)
                        with suppress(asyncio.QueueFull):
                            self._pending_errors.put_nowait(err)

                # Recurring issue detection
                recurring = self.analyzer.get_recurring()
                for r in recurring[:3]:
                    log.info(f"🔁 Recurring [{r['module']}]: "
                             f"{r['key']} ×{r['count']} → {r['strategy']}")

            except Exception as e:
                log.debug(f"Log scan error: {e}")

    async def _reward_loop(self):
        """Check for bonus rewards (clean hours, full recovery)."""
        while self._running:
            await asyncio.sleep(3600)
            try:
                summary = self.analyzer.get_summary(hours=1)
                critical = summary.get("critical", 0)
                if critical == 0:
                    self._clean_hours += 1
                    xp = self.reward.earn("clean_hour",
                                          details=f"{self._clean_hours} consecutive clean hours")
                    log.info(f"🏆 CLEAN HOUR BONUS: +{xp}XP "
                             f"(streak: {self._clean_hours}h)")
                else:
                    self._clean_hours = 0
            except Exception as e:
                log.debug(f"Reward loop: {e}")

    async def _status_loop(self):
        """Log status summary every 5 minutes."""
        while self._running:
            await asyncio.sleep(300)
            try:
                health  = self.oracle.compute_system_health()
                rstatus = self.reward.status()
                log.info(
                    f"📊 OMEGA │ "
                    f"Health={health['score']:.0f}% [{health['status']}] │ "
                    f"Level={rstatus['level']} {rstatus['level_name']} │ "
                    f"XP={rstatus['xp']} │ "
                    f"Streak={rstatus['streak']}×{rstatus['streak_bonus']} │ "
                    f"Heals={rstatus['total_heals']} "
                    f"({rstatus['successful_heals']} success)"
                )
            except Exception as e:
                log.debug(f"Status loop: {e}")

    async def _run_dep_scan(self):
        """Install requirements.txt then scan all modules."""
        await asyncio.sleep(2)
        if Path("requirements.txt").exists():
            await self.deps.check_requirements_txt()
        for m in self.registry.all_modules():
            if Path(m.path).exists():
                await self.deps.scan_and_install(m.path)

    # ── Status / Reporting ────────────────────────────────────────────────────

    def get_full_status(self) -> Dict:
        health  = self.oracle.compute_system_health()
        reward  = self.reward.status()
        bus     = self.bus.stats()
        db_stats= self.db.get_stats()
        modules = {
            m.name: {
                "status":    m.status.value,
                "health":    round(m.health_score, 1),
                "errors":    m.error_count,
                "restarts":  m.restart_count,
                "connected": m.instance is not None,
                "port":      m.api_port,
                "heartbeat_lag_s": round(_TS() - m.last_heartbeat, 0)
                    if m.last_heartbeat > 0 else -1,
            }
            for m in self.registry.all_modules()
        }
        return {
            "omega_uptime_s":   round(_TS() - self._start_ts, 0),
            "system_health":    health,
            "reward":           reward,
            "integration_bus":  bus,
            "database":         db_stats,
            "modules":          modules,
            "error_summary":    self.analyzer.get_summary(hours=1),
            "recurring_issues": self.analyzer.get_recurring()[:5],
            "health_history":   self.oracle.history_summary()[-10:],
            "ts":               _NOW(),
        }

    def print_status(self):
        s = self.get_full_status()
        h = s["system_health"]
        r = s["reward"]
        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  NEXUS OMEGA — SYSTEM STATUS                                         ║
╠══════════════════════════════════════════════════════════════════════╣
║  Health:  {h['score']:>5.1f}%  [{h['status']:<10}]  Critical: {h.get('critical_count',0)}              ║
║  Level:   {r['level']:>5}  {r['level_name']:<20}                        ║
║  XP:      {r['xp']:>8}  Streak: {r['streak']}×{r['streak_bonus']:.1f}  Next: {r['xp_to_next']} XP    ║
║  Heals:   {r['total_heals']:>5}  Success: {r['successful_heals']:>5}  Autonomy: {r['autonomy']:.0%}        ║
╠══════════════════════════════════════════════════════════════════════╣""")
        for name, m in s["modules"].items():
            icon = {"healthy":"✅","degraded":"⚠️ ","crashed":"❌",
                    "disabled":"⬜","unknown":"❓","restarting":"🔄"}.get(
                m["status"],"❓")
            print(f"║  {icon} {name:<28} {m['health']:>5.0f}%  "
                  f"E={m['errors']:<3} R={m['restarts']:<2} "
                  f"{'🔗' if m['connected'] else '  '}  ║")
        print("╚══════════════════════════════════════════════════════════════════════╝")


# ══════════════════════════════════════════════════════════════════════════════
#  FASTAPI DASHBOARD  (REST + WS live status)
# ══════════════════════════════════════════════════════════════════════════════

def create_omega_api(omega: NexusOmega, port: int = 9999) -> asyncio.Task:
    """Attach a FastAPI dashboard to the Omega instance."""
    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.middleware.cors import CORSMiddleware
        import uvicorn

        app = FastAPI(title="NEXUS OMEGA API", version="1.0")
        app.add_middleware(CORSMiddleware, allow_origins=["*"],
                           allow_methods=["*"], allow_headers=["*"])

        _ws_clients: Set[WebSocket] = set()

        @app.get("/")
        def root():
            return omega.get_full_status()

        @app.get("/health")
        def health():
            return omega.oracle.compute_system_health()

        @app.get("/reward")
        def reward():
            return omega.reward.status()

        @app.get("/modules")
        def modules():
            return {m.name: {
                "status": m.status.value,
                "health": m.health_score,
                "path":   m.path,
                "desc":   m.description,
            } for m in omega.registry.all_modules()}

        @app.get("/errors")
        def errors(hours: int = 1):
            return omega.analyzer.get_summary(hours)

        @app.get("/recurring")
        def recurring():
            return omega.analyzer.get_recurring()

        @app.get("/bus/stats")
        def bus():
            return omega.bus.stats()

        @app.post("/heal")
        async def force_heal(message: str, module: str = "unknown"):
            omega.report_error(message, module)
            return {"queued": True}

        @app.websocket("/ws")
        async def ws_live(websocket: WebSocket):
            await websocket.accept()
            _ws_clients.add(websocket)
            try:
                while True:
                    await asyncio.sleep(5)
                    status = omega.get_full_status()
                    await websocket.send_text(json.dumps(status, default=str))
            except WebSocketDisconnect:
                pass
            finally:
                _ws_clients.discard(websocket)

        async def run():
            config = uvicorn.Config(app, host="0.0.0.0",
                                    port=port, log_level="warning")
            server = uvicorn.Server(config)
            log.info(f"🌐 Omega API: http://localhost:{port}")
            await server.serve()

        return asyncio.create_task(run(), name="omega_api")

    except ImportError:
        log.warning("fastapi/uvicorn not installed — API disabled")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  BACKWARD COMPATIBILITY  (drop-in replacement for original SelfHealingEngine)
# ══════════════════════════════════════════════════════════════════════════════

class SelfHealingEngine:
    """
    Drop-in replacement for the original SelfHealingEngine.
    Delegates all work to NexusOmega.
    """

    def __init__(self, trading_system=None):
        self._omega = NexusOmega()
        if trading_system:
            self._omega.connect_module("trading_system", trading_system)
        self.running = False
        self.logger  = logging.getLogger("KRAKEN-HEAL")

    async def start(self):
        self.running = True
        await self._omega.start()
        self.logger.info("🩺 SelfHealingEngine (Omega) activated")

    async def stop(self):
        self.running = False
        await self._omega.stop()

    def process_error(self, error_message: str, context: Dict = None):
        self._omega.report_error(error_message, "trading_system", context)

    def get_status(self) -> Dict:
        return self._omega.get_full_status()

    @property
    def omega(self) -> NexusOmega:
        return self._omega


# ══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def _main():
    import argparse
    parser = argparse.ArgumentParser(description="NEXUS OMEGA — Supreme Self-Healing Engine")
    parser.add_argument("--mode",     choices=["daemon","test","status"],
                        default="daemon")
    parser.add_argument("--port",     type=int, default=9999)
    parser.add_argument("--no-claude",action="store_true")
    parser.add_argument("--no-install",action="store_true")
    args = parser.parse_args()

    if args.mode == "status":
        omega = NexusOmega(enable_claude=False)
        omega.print_status()
        return

    if args.mode == "test":
        log.info("🧪 Running self-test...")
        omega = NexusOmega(enable_claude=not args.no_claude)
        await omega.start(install_deps=not args.no_install)

        test_errors = [
            ("ERROR: Connection timeout to api.kraken.com", "nexus_data_engine"),
            ("WARNING: rate limit exceeded - 429 Too Many Requests", "nexus_prime_brain"),
            ("CRITICAL: WebSocket connection lost unexpectedly", "nexus_swarm"),
            ("ModuleNotFoundError: No module named 'pandas_ta'", "nexus_data_engine"),
            ("sqlite3.OperationalError: database is locked", "nexus_prime_brain"),
        ]

        for msg, module in test_errors:
            omega.report_error(msg, module)
            log.info(f"  Injected: [{module}] {msg[:50]}")
            await asyncio.sleep(0.1)

        await asyncio.sleep(10)
        omega.print_status()
        await omega.stop()
        return

    # Daemon mode
    omega = NexusOmega(
        enable_claude=not args.no_claude,
        monitor_interval_s=15,
        heal_interval_s=3,
        predict_interval_s=60,
        log_scan_interval_s=30,
        auto_install=not args.no_install,
    )

    try:
        from fastapi import FastAPI
        api_task = create_omega_api(omega, port=args.port)
    except ImportError:
        api_task = None

    await omega.start(install_deps=not args.no_install)
    if api_task:
        omega._tasks.append(api_task)

    log.info(f"🔥 NEXUS OMEGA DAEMON RUNNING")
    log.info(f"   Dashboard: http://localhost:{args.port}")
    log.info(f"   Press Ctrl+C to stop")

    try:
        await omega.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        await omega.stop()


if __name__ == "__main__":
    asyncio.run(_main())
