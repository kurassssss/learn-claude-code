"""
╔═══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                   ║
║  ██████╗ ███╗   ███╗███████╗ ██████╗  █████╗                                    ║
║  ██╔═══██╗████╗ ████║██╔════╝██╔════╝ ██╔══██╗                                   ║
║  ██║   ██║██╔████╔██║█████╗  ██║  ███╗███████║                                   ║
║  ██║   ██║██║╚██╔╝██║██╔══╝  ██║   ██║██╔══██║                                   ║
║  ╚██████╔╝██║ ╚═╝ ██║███████╗╚██████╔╝██║  ██║                                   ║
║   ╚═════╝ ╚═╝     ╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝                                   ║
║                                                                                   ║
║  ████████╗███████╗██╗     ███████╗ ██████╗ ██████╗  █████╗ ███╗   ███╗          ║
║     ██║   ██╔════╝██║     ██╔════╝██╔════╝ ██╔══██╗██╔══██╗████╗ ████║          ║
║     ██║   █████╗  ██║     █████╗  ██║  ███╗██████╔╝███████║██╔████╔██║          ║
║     ██║   ██╔══╝  ██║     ██╔══╝  ██║   ██║██╔══██╗██╔══██║██║╚██╔╝██║          ║
║     ██║   ███████╗███████╗███████╗╚██████╔╝██║  ██║██║  ██║██║ ╚═╝ ██║          ║
║     ╚═╝   ╚══════╝╚══════╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝          ║
║                                                                                   ║
║  ██████╗ ██████╗ ██╗██████╗  ██████╗ ███████╗                                   ║
║  ██╔══██╗██╔══██╗██║██╔══██╗██╔════╝ ██╔════╝                                   ║
║  ██████╔╝██████╔╝██║██║  ██║██║  ███╗█████╗                                     ║
║  ██╔══██╗██╔══██╗██║██║  ██║██║   ██║██╔══╝                                     ║
║  ██████╔╝██║  ██║██║██████╔╝╚██████╔╝███████╗                                   ║
║  ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝  ╚═════╝ ╚══════╝                                   ║
║                                                                                   ║
║  OMEGA × TELEGRAM BRIDGE — Głęboka integracja self-healing z interfejsem         ║
║                                                                                   ║
║  MOŻLIWOŚCI:                                                                      ║
║  ● NexusOmegaAgent   — Agent AI z dostępem do wszystkich plików systemu          ║
║  ● FileSystemOps     — Czyta, analizuje i edytuje pliki .py pod nadzorem Omega   ║
║  ● LiveEventPusher   — Push Telegram gdy Omega wykryje zdarzenie w systemie      ║
║  ● CodeQualityRanker — Rankuje boty wg jakości kodu, strategii, PnL             ║
║  ● StrategyEditor    — Edytuje strategie przez Claude API + Omega zatwierdzenie  ║
║  ● OmegaEventBus     — Subskrybuje IntegrationBus → push do użytkownika          ║
║                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import os
import re
import shutil
import threading
import time
import traceback
import uuid
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ── opcjonalne importy telegram ───────────────────────────────────────────────
try:
    from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.error import TelegramError
    TELEGRAM_OK = True
except ImportError:
    TELEGRAM_OK = False

# ── opcjonalne importy Anthropic ──────────────────────────────────────────────
try:
    import anthropic as _anthropic_sdk
    CLAUDE_OK = True
except ImportError:
    CLAUDE_OK = False

UTC  = timezone.utc
_NOW = lambda: datetime.now(UTC)
_TS  = lambda: time.time()
log  = logging.getLogger("OMEGA·TG·BRIDGE")

# Upewnij się że katalogi istnieją
Path("tg_data").mkdir(exist_ok=True)
Path("omega_data").mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  KATALOG WSZYSTKICH PLIKÓW SYSTEMU (ze zrzutów ekranu)
# ══════════════════════════════════════════════════════════════════════════════

# Wszystkie pliki .py widoczne w projekcie Replit
SYSTEM_FILES: Dict[str, Dict] = {
    # Boty tradingowe (priorytetowe)
    "arbitrage_engine":     {"file": "arbitrage_engine.py",     "type": "bot",    "desc": "Silnik arbitrażu multi-giełdowego"},
    "kraken_god":           {"file": "kraken_god.py",           "type": "bot",    "desc": "Główny bot Kraken Futures"},
    "kraken_ultra":         {"file": "kraken_ultra.py",         "type": "bot",    "desc": "Kraken Ultra orchestrator"},
    "council_trading_engine":{"file":"council_trading_engine.py","type": "bot",   "desc": "Council trading – głosowanie strategii"},
    "nexus_prime_brain":    {"file": "nexus_prime_brain.py",    "type": "brain",  "desc": "Główny mózg decyzyjny Nexus"},
    "nexus_swarm":          {"file": "nexus_swarm.py",          "type": "swarm",  "desc": "Rój 1000 botów"},
    "rl_engines":           {"file": "rl_engines.py",           "type": "bot",    "desc": "15-silnikowy klaster RL"},
    "neural_network":       {"file": "neural_network.py",       "type": "ml",     "desc": "Sieć neuronowa predykcji"},
    # Silniki wspomagające
    "nexus_data_engine":    {"file": "nexus_data_engine.py",    "type": "engine", "desc": "Zbieranie i przetwarzanie danych"},
    "nexus_genesis_engine": {"file": "nexus_genesis_engine.py", "type": "engine", "desc": "Ewolucja strategii (MAP-Elites)"},
    "self_healing_engine":  {"file": "self_healing_engine.py",  "type": "omega",  "desc": "Self-healing superinteligencja"},
    "genetic_optimizer":    {"file": "genetic_optimizer.py",    "type": "ml",     "desc": "Optymalizator genetyczny"},
    # Backtesting / trening
    "backtester":           {"file": "backtester.py",           "type": "test",   "desc": "Backtester historyczny"},
    "simulation_runner":    {"file": "simulation_runner.py",    "type": "test",   "desc": "Runner symulacji"},
    "run_backtest_integration":{"file":"run_backtest_integration.py","type":"test","desc":"Integracja backtestu"},
    "merged_gauntlet":      {"file": "merged_gauntlet.py",      "type": "test",   "desc": "Gauntlet – 5-letni syntetyczny trening"},
    # Infrastruktura
    "bybit_fetcher":        {"file": "bybit_fetcher.py",        "type": "infra",  "desc": "Konektor Bybit"},
    "launch_system":        {"file": "launch_system.py",        "type": "infra",  "desc": "System startowy"},
    "main":                 {"file": "main.py",                 "type": "infra",  "desc": "Punkt wejścia systemu"},
    "subscriber_interface": {"file": "subscriber_interface.py", "type": "infra",  "desc": "Interfejs subskrypcji"},
    "subscription_manager": {"file": "subscription_manager.py", "type": "infra",  "desc": "Zarządzanie subskrypcjami"},
    "telegram_interface":   {"file": "telegram_interface.py",   "type": "infra",  "desc": "Interfejs Telegram"},
    "telegram_stats_provider":{"file":"telegram_stats_provider.py","type":"infra","desc":"Dostawca statystyk Telegram"},
}

# Foldery danych
SYSTEM_DIRS = [
    "brain_state", "data", "exchange", "knowledge_base",
    "kraken_ultra_data", "logs", "ml", "neural_models",
    "omega_data", "risk", "rl_models", "tg_data", "memu",
]

# Typy plików z emoji
TYPE_EMOJI = {
    "bot":    "🤖",
    "brain":  "🧠",
    "swarm":  "🐝",
    "ml":     "🧬",
    "engine": "⚙️",
    "omega":  "🩺",
    "test":   "🧪",
    "infra":  "🔩",
}


# ══════════════════════════════════════════════════════════════════════════════
#  FILE SYSTEM OPERATIONS  (pod nadzorem Omega)
# ══════════════════════════════════════════════════════════════════════════════

class FileSystemOps:
    """
    Bezpieczne operacje na plikach systemu.
    Każda modyfikacja: backup → edit → walidacja AST → restore jeśli błąd.
    """

    BACKUP_DIR = Path("omega_data/file_backups")

    def __init__(self):
        self.BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    def read_file(self, path: str) -> Optional[str]:
        """Wczytaj zawartość pliku."""
        p = Path(path)
        if not p.exists():
            return None
        try:
            return p.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            log.warning(f"read_file {path}: {exc}")
            return None

    def read_file_chunk(self, path: str, start_line: int = 1,
                        end_line: int = 100) -> Tuple[str, int]:
        """Wczytaj fragment pliku (start_line–end_line). Zwraca (tekst, total_lines)."""
        content = self.read_file(path)
        if not content:
            return ("", 0)
        lines = content.splitlines()
        total = len(lines)
        chunk = lines[max(0, start_line - 1):end_line]
        return ("\n".join(chunk), total)

    def list_files(self, type_filter: Optional[str] = None) -> List[Dict]:
        """Lista plików systemu z metadanymi."""
        result = []
        for name, meta in SYSTEM_FILES.items():
            p = Path(meta["file"])
            if type_filter and meta["type"] != type_filter:
                continue
            size = p.stat().st_size if p.exists() else 0
            lines = 0
            if p.exists():
                try:
                    lines = sum(1 for _ in p.open("r", encoding="utf-8",
                                                    errors="replace"))
                except Exception:
                    pass
            result.append({
                "name":    name,
                "file":    meta["file"],
                "type":    meta["type"],
                "desc":    meta["desc"],
                "exists":  p.exists(),
                "size_kb": round(size / 1024, 1),
                "lines":   lines,
                "emoji":   TYPE_EMOJI.get(meta["type"], "📄"),
            })
        return result

    def analyze_code_quality(self, path: str) -> Dict:
        """
        Analiza jakości kodu Python:
        - liczba klas, funkcji, linii
        - złożoność cyklomatyczna (uproszczona)
        - obecność docstringów, type hints
        - wykrycie wzorców strategii tradingowych
        """
        content = self.read_file(path)
        if not content:
            return {"error": "file not found", "score": 0}

        try:
            tree = ast.parse(content)
        except SyntaxError as exc:
            return {"error": f"SyntaxError: {exc}", "score": 0}

        classes   = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        functions = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        async_fns = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)]
        all_fns   = functions + async_fns

        # Docstringi
        docstrings = sum(
            1 for node in all_fns
            if (ast.get_docstring(node) or "")
        )
        doc_ratio = docstrings / max(len(all_fns), 1)

        # Type hints
        typed = sum(1 for fn in all_fns if fn.returns is not None)
        typed_ratio = typed / max(len(all_fns), 1)

        # Złożoność cyklomatyczna
        branches = sum(
            1 for node in ast.walk(tree)
            if isinstance(node, (ast.If, ast.While, ast.For,
                                  ast.ExceptHandler, ast.With))
        )
        cyclomatic = 1 + branches

        # Wzorce tradingowe
        content_lower = content.lower()
        trading_patterns = {
            "strategy":    any(kw in content_lower for kw in ["strategy", "signal", "entry", "exit"]),
            "risk_mgmt":   any(kw in content_lower for kw in ["stop_loss", "take_profit", "sl", "tp", "risk"]),
            "position":    any(kw in content_lower for kw in ["position", "order", "trade", "pnl"]),
            "async_io":    bool(async_fns),
            "error_handling": "try:" in content or "except" in content,
            "logging":     "log" in content_lower or "logger" in content_lower,
        }
        trading_score = sum(trading_patterns.values()) / len(trading_patterns) * 100

        # Finalna ocena (0–100)
        lines = content.count("\n") + 1
        score = min(100, (
            doc_ratio * 20 +
            typed_ratio * 15 +
            (min(cyclomatic, 50) / 50) * 15 +   # złożoność = dobre
            trading_score * 0.3 +
            min(lines / 50, 1.0) * 20            # objętość kodu
        ))

        return {
            "path":          path,
            "lines":         lines,
            "classes":       len(classes),
            "functions":     len(all_fns),
            "docstring_pct": round(doc_ratio * 100, 1),
            "typed_pct":     round(typed_ratio * 100, 1),
            "cyclomatic":    cyclomatic,
            "trading_patterns": trading_patterns,
            "trading_score": round(trading_score, 1),
            "score":         round(score, 1),
        }

    def backup_file(self, path: str) -> Optional[str]:
        """Utwórz kopię zapasową pliku przed edycją."""
        p = Path(path)
        if not p.exists():
            return None
        ts = int(_TS())
        backup = self.BACKUP_DIR / f"{p.stem}_{ts}{p.suffix}"
        shutil.copy2(str(p), str(backup))
        log.info(f"💾 Backup: {path} → {backup}")
        return str(backup)

    def apply_patch(self, path: str, old_code: str, new_code: str,
                    description: str = "") -> Dict:
        """
        Bezpieczna zamiana fragmentu kodu:
        1. Backup oryginału
        2. Zamiana old_code → new_code
        3. Walidacja AST nowej wersji
        4. Rollback jeśli błąd składni
        """
        original = self.read_file(path)
        if original is None:
            return {"ok": False, "error": "Plik nie istnieje"}

        if old_code not in original:
            return {"ok": False, "error": "Fragment kodu nie znaleziony w pliku"}

        backup = self.backup_file(path)
        patched = original.replace(old_code, new_code, 1)

        # Walidacja składni
        try:
            ast.parse(patched)
        except SyntaxError as exc:
            return {"ok": False, "error": f"Błąd składni: {exc}",
                    "backup": backup}

        # Zapis
        try:
            Path(path).write_text(patched, encoding="utf-8")
            log.info(f"✏️  Patch zastosowany: {path} [{description}]")
            return {
                "ok":          True,
                "path":        path,
                "backup":      backup,
                "description": description,
                "lines_before": original.count("\n") + 1,
                "lines_after":  patched.count("\n") + 1,
            }
        except Exception as exc:
            # Przywróć backup
            if backup:
                shutil.copy2(backup, path)
            return {"ok": False, "error": str(exc)}

    def rollback(self, path: str) -> bool:
        """Przywróć ostatnią kopię zapasową."""
        backups = sorted(
            self.BACKUP_DIR.glob(f"{Path(path).stem}_*{Path(path).suffix}"),
            reverse=True,
        )
        if not backups:
            return False
        shutil.copy2(str(backups[0]), path)
        log.info(f"⏪ Rollback: {path} ← {backups[0]}")
        return True


# ══════════════════════════════════════════════════════════════════════════════
#  CODE QUALITY RANKER  (rankuje boty wg jakości kodu + PnL)
# ══════════════════════════════════════════════════════════════════════════════

class CodeQualityRanker:
    """
    Rankuje pliki botów według wielokryterialnej oceny:
    jakość kodu + wzorce tradingowe + rozmiar + dostępność.
    """

    def __init__(self, fs: FileSystemOps):
        self.fs = fs
        self._cache: Dict[str, Dict] = {}
        self._cache_ts: float = 0.0

    def rank_bots(self, top_n: int = 10,
                  type_filter: Optional[str] = None) -> List[Dict]:
        """Zwróć top N botów posortowanych według jakości."""
        now = _TS()
        if now - self._cache_ts > 300:  # 5 minut cache
            self._cache = {}
            self._cache_ts = now

        results = []
        for name, meta in SYSTEM_FILES.items():
            if type_filter and meta["type"] not in type_filter:
                continue
            if not Path(meta["file"]).exists():
                continue

            if name not in self._cache:
                self._cache[name] = self.fs.analyze_code_quality(meta["file"])

            analysis = self._cache[name]
            if analysis.get("error"):
                continue

            results.append({
                "rank":        0,
                "name":        name,
                "file":        meta["file"],
                "type":        meta["type"],
                "desc":        meta["desc"],
                "emoji":       TYPE_EMOJI.get(meta["type"], "📄"),
                "score":       analysis["score"],
                "lines":       analysis["lines"],
                "functions":   analysis["functions"],
                "cyclomatic":  analysis["cyclomatic"],
                "trading_score": analysis["trading_score"],
                "docstring_pct": analysis["docstring_pct"],
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        for i, r in enumerate(results, 1):
            r["rank"] = i

        return results[:top_n]

    def get_analysis_text(self, name: str) -> str:
        """Tekstowy opis analizy konkretnego pliku."""
        meta = SYSTEM_FILES.get(name)
        if not meta:
            return f"❌ Nieznany moduł: {name}"

        a = self.fs.analyze_code_quality(meta["file"])
        if a.get("error"):
            return f"❌ Błąd analizy {name}: {a['error']}"

        tp = a["trading_patterns"]
        patterns_txt = "\n".join(
            f"  {'✅' if v else '❌'} {k}"
            for k, v in tp.items()
        )
        return (
            f"📊 *Analiza: {meta['file']}*\n\n"
            f"Ocena ogólna:   *{a['score']:.1f}/100*\n"
            f"Linie kodu:     *{a['lines']:,}*\n"
            f"Funkcje:        *{a['functions']}*\n"
            f"Klasy:          *{a['classes']}*\n"
            f"Złożoność:      *{a['cyclomatic']}*\n"
            f"Docstringi:     *{a['docstring_pct']:.0f}%*\n"
            f"Type hints:     *{a['typed_pct']:.0f}%*\n"
            f"Trading score:  *{a['trading_score']:.0f}%*\n\n"
            f"*Wzorce tradingowe:*\n{patterns_txt}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA TELEGRAM AGENT  (AI agent z pełnym dostępem do systemu)
# ══════════════════════════════════════════════════════════════════════════════

class NexusOmegaAgent:
    """
    Claude-powered agent z dostępem do:
    - Wszystkich plików .py systemu (czytanie + edycja)
    - Danych Omega (health, heals, XP, błędy)
    - KrakenUltraAggregator (portfel, transakcje)
    - Możliwości edycji strategii przez patch

    Przykłady możliwości:
    - "pokaż 10 najlepiej napisanych botów" → analiza AST + ranking
    - "pokaż kod kraken_god.py" → zwraca kod w kawałkach
    - "edytuj stop_loss na 1.5%" → patch → Omega waliduje → Telegram notify
    - "co się ostatnio naprawiło?" → historia healów z Omega DB
    """

    MAX_CODE_CHUNK = 3500  # znaki na jedną wiadomość Telegram

    def __init__(self, fs: FileSystemOps, ranker: CodeQualityRanker,
                 agg=None, omega=None):
        self.fs      = fs
        self.ranker  = ranker
        self.agg     = agg     # KrakenUltraAggregator
        self.omega   = omega   # NexusOmega instance
        self._cli    = None
        self._hist:  Dict[int, List[Dict]] = defaultdict(list)
        self._daily_calls = 0
        self._daily_limit = 200
        self._last_reset  = _TS()

        if CLAUDE_OK and os.getenv("ANTHROPIC_API_KEY"):
            try:
                self._cli = _anthropic_sdk.Anthropic(
                    api_key=os.getenv("ANTHROPIC_API_KEY")
                )
                log.info("🤖 NexusOmegaAgent: Claude AI aktywny")
            except Exception as exc:
                log.warning(f"Claude init error: {exc}")

    def _reset_daily_if_needed(self):
        now = _TS()
        if now - self._last_reset > 86400:
            self._daily_calls = 0
            self._last_reset  = now

    def _build_system_prompt(self) -> str:
        """Buduje system prompt z aktualnym stanem systemu."""
        # Pliki dostępne
        file_list = "\n".join(
            f"  - {meta['file']} ({meta['desc']})"
            for meta in SYSTEM_FILES.values()
            if Path(meta["file"]).exists()
        )

        # Stan Omega
        omega_state = "Omega niedostępna"
        if self.omega:
            try:
                s = self.omega.get_full_status()
                h = s.get("system_health", {})
                r = s.get("reward", {})
                omega_state = (
                    f"Health: {h.get('score', 0):.0f}% [{h.get('status', '?')}] | "
                    f"Level: {r.get('level', 1)} {r.get('level_name', '?')} | "
                    f"XP: {r.get('xp', 0)} | "
                    f"Heals: {r.get('total_heals', 0)}"
                )
            except Exception:
                pass

        # Stan portfela
        portfolio_state = "Aggregator niedostępny"
        if self.agg:
            try:
                p = self.agg.get_portfolio()
                portfolio_state = (
                    f"Balans: ${p['total']:.2f} | "
                    f"Dziennie: ${p['daily_pnl']:+.4f} | "
                    f"WR: {p['win_rate']:.1f}%"
                )
            except Exception:
                pass

        return f"""Jesteś NEXUS OMEGA AGENT — superinteligentnym asystentem systemu tradingowego.
Odpowiadaj TYLKO po polsku. Jesteś ekspertem od algorytmicznego tradingu, Pythona, kryptowalut i architektury systemów.

TWOJE MOŻLIWOŚCI:
- Czytasz i analizujesz dowolny plik .py systemu
- Możesz zaproponować edycje kodu (podaj exact patch: stary kod i nowy kod)
- Rozumiesz architekturę wszystkich modułów
- Interpretujesz dane Omega, błędy, historię healów
- Oceniasz jakość kodu i rankingi botów
- Sugerujesz optymalizacje strategii tradingowych

AKTUALNY STAN SYSTEMU:
Omega: {omega_state}
Portfel: {portfolio_state}

DOSTĘPNE PLIKI:
{file_list}

FORMAT ODPOWIEDZI:
- Używaj emoji dla czytelności
- Do edycji kodu podawaj: <<<PATCH: plik.py>>> następnie STARY_KOD:...NOWY_KOD:...
- Odpowiadaj zwięźle, max 800 tokenów
- Gdy pytanie dotyczy kodu, pytaj czy wysłać kod fragmentami"""

    async def chat(self, chat_id: int, message: str,
                   push_callback: Optional[Callable] = None) -> str:
        """
        Główna metoda chatu.
        push_callback: async fn(text) → dla wieloczęściowych odpowiedzi.
        """
        self._reset_daily_if_needed()

        if self._daily_calls >= self._daily_limit:
            return "⚠️ Dzienny limit zapytań AI wyczerpany."

        # Rozpoznaj intencje specjalne (bez Claude)
        special = await self._handle_special_intent(message, chat_id, push_callback)
        if special is not None:
            return special

        if not self._cli:
            return (
                "⚠️ Claude API niedostępny. Ustaw `ANTHROPIC_API_KEY`.\n\n"
                + self._offline_status()
            )

        hist = self._hist[chat_id]
        hist.append({"role": "user", "content": message})
        if len(hist) > 30:
            hist[:] = hist[-30:]

        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._cli.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1000,
                    system=self._build_system_prompt(),
                    messages=hist,
                )
            )
            self._daily_calls += 1
            answer = resp.content[0].text
            hist.append({"role": "assistant", "content": answer})

            # Wykryj patch w odpowiedzi i zaaplikuj
            patch_result = await self._auto_apply_patches(answer, chat_id)
            if patch_result:
                answer += f"\n\n{patch_result}"

            return answer

        except Exception as exc:
            log.warning(f"Agent chat error: {exc}")
            return f"⚠️ Błąd AI: {str(exc)[:100]}\n\n{self._offline_status()}"

    async def _handle_special_intent(self, message: str, chat_id: int,
                                      push_callback) -> Optional[str]:
        """
        Obsługuje komendy bez modelu AI (szybsze + tańsze):
        - top boty / ranking
        - pokaż kod X
        - historia healów
        - status pliku X
        """
        msg_lower = message.lower().strip()

        # ── TOP BOTY ──────────────────────────────────────────────────────────
        m = re.search(r"(?:top|najlep\w+)\s+(\d+)\s+bot", msg_lower)
        if m or re.search(r"ranking\s+bot|najlep\w+\s+bot", msg_lower):
            n = int(m.group(1)) if m else 10
            return self._render_top_bots(n)

        # ── POKAŻ KOD ─────────────────────────────────────────────────────────
        show = re.search(
            r"(?:pokaż|wyświetl|poka[żz]|show|kod|code)\s+(\w[\w_\.]+)", msg_lower
        )
        if show:
            name = show.group(1).replace(".py", "")
            return await self._send_file_code(name, push_callback)

        # ── LISTA PLIKÓW ──────────────────────────────────────────────────────
        if re.search(r"lista\s+plik|wszystkie\s+plik|files?\s+list", msg_lower):
            return self._render_file_list()

        # ── HISTORIA HEALÓW ───────────────────────────────────────────────────
        if re.search(r"historia\s+heal|napraw\w*\s+historia|ostatnie\s+heal", msg_lower):
            return self._render_heal_history()

        # ── ANALIZA PLIKU ─────────────────────────────────────────────────────
        an = re.search(
            r"(?:analiz\w+|oceń|ocena|quality|jakość)\s+(\w[\w_\.]+)", msg_lower
        )
        if an:
            name = an.group(1).replace(".py", "")
            meta = SYSTEM_FILES.get(name)
            if meta:
                return self.ranker.get_analysis_text(name)

        # ── STATUS OMEGA ──────────────────────────────────────────────────────
        if re.search(r"status\s+omega|omega\s+status|stan\s+systemu", msg_lower):
            return self._render_omega_status()

        # ── ROLLBACK ──────────────────────────────────────────────────────────
        rb = re.search(r"rollback\s+(\w[\w_\.]+)", msg_lower)
        if rb:
            name = rb.group(1).replace(".py", "")
            meta = SYSTEM_FILES.get(name)
            if meta:
                ok = self.fs.rollback(meta["file"])
                if ok:
                    return f"⏪ Rollback *{meta['file']}* — przywrócono ostatnią kopię!"
                return f"❌ Brak kopii zapasowej dla *{meta['file']}*"

        return None  # przekaż do Claude

    def _render_top_bots(self, n: int) -> str:
        bots = self.ranker.rank_bots(top_n=n)
        if not bots:
            return "❌ Brak dostępnych plików botów do analizy."

        lines = [f"🏆 *TOP {n} BOTÓW — RANKING KODU*\n"]
        for b in bots:
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(b["rank"], f"#{b['rank']}")
            lines.append(
                f"{medal} {b['emoji']} *{b['file']}*\n"
                f"   Ocena: `{b['score']:.1f}/100` | "
                f"Linie: {b['lines']:,} | "
                f"Fnc: {b['functions']} | "
                f"Trading: {b['trading_score']:.0f}%\n"
                f"   _{b['desc']}_\n"
            )
        lines.append(
            f"\n_Wyślij `pokaż [nazwa]` aby zobaczyć kod "
            f"lub `analiza [nazwa]` dla szczegółów_"
        )
        return "\n".join(lines)

    async def _send_file_code(self, name: str,
                               push_callback: Optional[Callable]) -> str:
        """Wysyła kod pliku w kawałkach."""
        meta = SYSTEM_FILES.get(name)
        if not meta:
            # Szukaj po fragmencie nazwy
            matches = [k for k in SYSTEM_FILES if name in k]
            if not matches:
                return f"❌ Nieznany plik: `{name}`"
            meta = SYSTEM_FILES[matches[0]]
            name = matches[0]

        content = self.fs.read_file(meta["file"])
        if content is None:
            return f"❌ Plik `{meta['file']}` nie istnieje w projekcie."

        total = len(content)
        total_lines = content.count("\n") + 1
        header = (
            f"📄 *{meta['file']}*\n"
            f"_{meta['desc']}_\n"
            f"Linie: {total_lines:,} | Rozmiar: {total/1024:.1f}KB\n"
            f"{'─' * 25}"
        )

        # Podziel na kawałki i wyślij przez push_callback jeśli dostępny
        chunks = []
        for i in range(0, len(content), self.MAX_CODE_CHUNK):
            chunk = content[i:i + self.MAX_CODE_CHUNK]
            chunks.append(f"```python\n{chunk}\n```")

        if push_callback and len(chunks) > 1:
            await push_callback(header)
            for j, chunk in enumerate(chunks, 1):
                await push_callback(
                    f"_Część {j}/{len(chunks)}_\n{chunk}"
                )
            return f"✅ Wysłano kod `{meta['file']}` w {len(chunks)} częściach."

        if len(chunks) == 1:
            return f"{header}\n\n{chunks[0]}"

        # Zwróć tylko pierwszą część z info
        return (
            f"{header}\n\n{chunks[0]}\n\n"
            f"_...plik ma jeszcze {len(chunks)-1} częsci. "
            f"Wysyłam przez push._"
        )

    def _render_file_list(self) -> str:
        files = self.fs.list_files()
        by_type: Dict[str, List] = defaultdict(list)
        for f in files:
            by_type[f["type"]].append(f)

        lines = ["📁 *WSZYSTKIE PLIKI SYSTEMU*\n"]
        for t, flist in sorted(by_type.items()):
            emoji = TYPE_EMOJI.get(t, "📄")
            lines.append(f"\n*{emoji} {t.upper()}*")
            for f in flist:
                status = "✅" if f["exists"] else "❌"
                lines.append(
                    f"  {status} `{f['file']}` "
                    f"_{f['lines']:,} ln_ — {f['desc']}"
                )
        return "\n".join(lines)

    def _render_heal_history(self) -> str:
        if not self.omega:
            return "⚠️ Omega nie jest podłączona."
        try:
            summary = self.omega.analyzer.get_summary(hours=24)
            recurring = self.omega.analyzer.get_recurring()[:5]
            r = self.omega.reward.status()

            lines = [
                f"🩺 *HISTORIA HEALÓW (24h)*\n",
                f"Łącznie błędów: *{summary.get('total', 0)}*",
                f"Krytycznych:    *{summary.get('critical', 0)}*",
                f"Naprawionych:   *{r.get('successful_heals', 0)}*",
                f"Łącznie healów: *{r.get('total_heals', 0)}*\n",
            ]
            if recurring:
                lines.append("*🔁 Powtarzające się problemy:*")
                for r_item in recurring:
                    lines.append(
                        f"  ● `{r_item.get('key', '?')[:40]}` "
                        f"×{r_item.get('count', 0)} | "
                        f"Strategia: {r_item.get('strategy', '?')}"
                    )
            else:
                lines.append("✅ Brak powtarzających się problemów")
            return "\n".join(lines)
        except Exception as exc:
            return f"❌ Błąd pobierania historii: {exc}"

    def _render_omega_status(self) -> str:
        if not self.omega:
            return "⚠️ Omega nie jest podłączona."
        try:
            s = self.omega.get_full_status()
            h = s.get("system_health", {})
            r = s.get("reward", {})
            mods = s.get("modules", {})

            lines = [f"🩺 *NEXUS OMEGA — PEŁNY STATUS*\n"]
            lines.append(
                f"Health:   *{h.get('score', 0):.0f}%* [{h.get('status', '?')}]\n"
                f"Level:    *{r.get('level', 1)} — {r.get('level_name', '?')}*\n"
                f"XP:       *{r.get('xp', 0):,}* (+{r.get('xp_to_next', 0)} do awansu)\n"
                f"Heals:    *{r.get('total_heals', 0)}* udanych: {r.get('successful_heals', 0)}\n"
                f"Autonomia: *{r.get('autonomy', 0):.0%}*\n"
            )
            if mods:
                lines.append("*Moduły:*")
                for mod_name, info in list(mods.items())[:8]:
                    icon = {"healthy": "🟢", "degraded": "🟡",
                            "crashed": "🔴", "disabled": "⚫"}.get(
                        info.get("status", ""), "❓"
                    )
                    lines.append(
                        f"  {icon} `{mod_name}` "
                        f"{info.get('health', 0):.0f}% "
                        f"E:{info.get('errors', 0)}"
                    )
            return "\n".join(lines)
        except Exception as exc:
            return f"❌ Błąd statusu Omega: {exc}"

    async def _auto_apply_patches(self, answer: str,
                                   chat_id: int) -> Optional[str]:
        """
        Wykrywa w odpowiedzi AI bloki PATCH i stosuje je automatycznie.
        Format: <<<PATCH: plik.py>>>
                STARY_KOD: ...
                NOWY_KOD: ...
        """
        patch_re = re.compile(
            r"<<<PATCH:\s*([\w_\.]+)>>>\s*"
            r"STARY_KOD:\s*```(?:python)?\s*(.*?)```\s*"
            r"NOWY_KOD:\s*```(?:python)?\s*(.*?)```",
            re.DOTALL,
        )
        results = []
        for m in patch_re.finditer(answer):
            filename, old_code, new_code = m.group(1), m.group(2), m.group(3)
            result = self.fs.apply_patch(filename, old_code.strip(),
                                          new_code.strip(),
                                          description=f"AI patch chat_id={chat_id}")
            if result["ok"]:
                results.append(
                    f"✅ Patch zastosowany: `{filename}` "
                    f"({result['lines_before']}→{result['lines_after']} ln)"
                )
                # Powiadom Omega o zmianie
                if self.omega:
                    self.omega.report_error(
                        f"File patched by AI agent: {filename}",
                        "omega_telegram_bridge",
                        {"type": "ai_patch", "file": filename},
                    )
            else:
                results.append(
                    f"⚠️ Patch nieudany dla `{filename}`: {result['error']}"
                )
        return "\n".join(results) if results else None

    def _offline_status(self) -> str:
        files_ok = sum(1 for m in SYSTEM_FILES.values()
                       if Path(m["file"]).exists())
        return (
            f"*Stan systemu:*\n"
            f"Pliki dostępne: {files_ok}/{len(SYSTEM_FILES)}\n"
            f"Omega: {'✅ aktywna' if self.omega else '❌ offline'}\n"
            f"Agg: {'✅ aktywny' if self.agg else '❌ offline'}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA EVENT PUSHER  (subskrybuje IntegrationBus → push Telegram)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PushFilter:
    """Filtr zdarzeń do push na Telegram."""
    heal_events:      bool = True    # Udane naprawy
    critical_errors:  bool = True    # Błędy krytyczne
    module_crashes:   bool = True    # Crash modułu
    file_changes:     bool = True    # Zmiany plików
    level_ups:        bool = True    # Awans poziomu Omega
    health_critical:  bool = True    # Zdrowie systemu < 50%
    strategy_changes: bool = True    # Zmiany strategii


class OmegaEventPusher:
    """
    Subskrybuje zdarzenia z NexusOmega (IntegrationBus + polling)
    i push-uje powiadomienia do użytkowników Telegram.

    Nasłuchuje na:
    - system_health_critical → alert zdrowie systemu
    - module_crashed         → alert crash modułu
    - heal_success           → info o naprawie
    - file_changed           → info o zmianie pliku
    - level_up               → awans Omega
    """

    POLL_INTERVAL = 10.0  # sekund

    def __init__(self, omega=None, bot_token: Optional[str] = None):
        self.omega      = omega
        self._token     = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self._bot:      Optional[Bot] = None
        self._subs:     Set[int] = self._load_subs()  # chat_ids
        self._filters:  Dict[int, PushFilter] = defaultdict(PushFilter)
        self._running   = False
        self._task:     Optional[asyncio.Task] = None
        self._lock      = asyncio.Lock()

        # Stan poprzedni dla wykrywania zmian
        self._prev_health:  float = 100.0
        self._prev_level:   int   = 1
        self._prev_heals:   int   = 0
        self._file_mtimes:  Dict[str, float] = {}

        if self._token and TELEGRAM_OK:
            self._bot = Bot(token=self._token)
            log.info("📲 OmegaEventPusher: Bot Telegram aktywny")

    def _load_subs(self) -> Set[int]:
        p = Path("tg_data/omega_push_subs.json")
        if p.exists():
            try:
                return set(json.loads(p.read_text()))
            except Exception:
                pass
        return set()

    def _save_subs(self):
        Path("tg_data/omega_push_subs.json").write_text(
            json.dumps(list(self._subs)), encoding="utf-8"
        )

    def subscribe(self, chat_id: int,
                  filters: Optional[PushFilter] = None):
        """Subskrybuj push dla danego chat_id."""
        self._subs.add(chat_id)
        if filters:
            self._filters[chat_id] = filters
        self._save_subs()
        log.info(f"📲 Push sub: {chat_id}")

    def unsubscribe(self, chat_id: int):
        self._subs.discard(chat_id)
        self._save_subs()

    async def push(self, text: str, level: str = "info",
                   keyboard=None):
        """Push wiadomości do wszystkich subskrybentów."""
        if not self._bot or not self._subs:
            return
        markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        for chat_id in list(self._subs):
            try:
                await self._bot.send_message(
                    chat_id=chat_id,
                    text=text[:4090],
                    parse_mode="Markdown",
                    reply_markup=markup,
                )
            except TelegramError as exc:
                log.warning(f"Push failed [{chat_id}]: {exc}")
                if "blocked" in str(exc).lower():
                    self.unsubscribe(chat_id)

    async def start(self):
        """Uruchom pętlę push."""
        if not self._bot:
            log.warning("📲 OmegaEventPusher: brak tokenu Telegram")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._poll_loop(), name="omega_push_loop"
        )
        # Subskrybuj IntegrationBus jeśli Omega dostępna
        if self.omega and hasattr(self.omega, "bus"):
            self.omega.bus.subscribe("*", self._on_bus_event)
            log.info("📡 OmegaEventPusher: subskrypcja IntegrationBus")
        log.info("📲 OmegaEventPusher: start")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def _on_bus_event(self, msg):
        """Handler zdarzeń z IntegrationBus."""
        topic = getattr(msg, "topic", "")

        if topic == "system_health_critical":
            score = msg.payload.get("score", 0)
            await self.push(
                f"🚨 *ZDROWIE SYSTEMU KRYTYCZNE*\n"
                f"Score: {score:.0f}% — natychmiastowa uwaga wymagana!\n"
                f"Sprawdź ekran Omega w /menu",
                level="critical",
            )

        elif topic == "heal_success":
            mod  = msg.payload.get("module", "?")
            strat = msg.payload.get("strategy", "?")
            xp   = msg.payload.get("xp", 0)
            await self.push(
                f"✅ *NAPRAWA UDANA*\n"
                f"Moduł: `{mod}`\n"
                f"Strategia: `{strat}`\n"
                f"+{xp} XP",
            )

        elif topic == "module_crashed":
            mod = msg.payload.get("module", "?")
            err = msg.payload.get("error", "?")[:100]
            await self.push(
                f"💥 *CRASH MODUŁU*\n"
                f"Moduł: `{mod}`\n"
                f"Błąd: `{err}`\n"
                f"Omega pracuje nad naprawą...",
                level="critical",
            )

        elif topic == "level_up":
            lvl  = msg.payload.get("level", "?")
            name = msg.payload.get("name", "?")
            await self.push(
                f"🆙 *AWANS OMEGA!*\n"
                f"Nowy poziom: *{lvl} — {name}*\n"
                f"System staje się coraz bardziej autonomiczny!",
            )

        elif topic in ("ai_patch", "file_changed", "strategy_changed"):
            fname = msg.payload.get("file", "?")
            desc  = msg.payload.get("description", "edycja kodu")
            await self.push(
                f"✏️ *ZMIANA PLIKU*\n"
                f"Plik: `{fname}`\n"
                f"Operacja: {desc}",
            )

    async def _poll_loop(self):
        """Polling zmian stanu Omega."""
        while self._running:
            try:
                await self._check_omega_changes()
                await self._check_file_changes()
            except Exception as exc:
                log.debug(f"PollLoop error: {exc}")
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _check_omega_changes(self):
        if not self.omega:
            return
        try:
            s = self.omega.get_full_status()
            h = s.get("system_health", {})
            r = s.get("reward", {})

            current_health = h.get("score", 100.0)
            current_level  = r.get("level", 1)
            current_heals  = r.get("total_heals", 0)

            # Awans poziomu
            if current_level > self._prev_level:
                await self.push(
                    f"🆙 *OMEGA AWANSOWAŁA!*\n"
                    f"Poziom {self._prev_level} → *{current_level}*\n"
                    f"Nowy tytuł: *{r.get('level_name', '?')}*"
                )
            self._prev_level = current_level

            # Zdrowie krytyczne (próg 50%)
            if current_health < 50 and self._prev_health >= 50:
                await self.push(
                    f"🚨 *ZDROWIE SYSTEMU SPADŁO PONIŻEJ 50%*\n"
                    f"Aktualnie: {current_health:.0f}%\n"
                    f"Status: {h.get('status', '?')}"
                )
            # Powrót do zdrowia
            elif current_health >= 75 and self._prev_health < 50:
                await self.push(
                    f"✅ *SYSTEM ODZYSKAŁ ZDROWIE*\n"
                    f"Aktualnie: {current_health:.0f}%"
                )
            self._prev_health = current_health

            # Nowe heale
            if current_heals > self._prev_heals:
                diff = current_heals - self._prev_heals
                if diff >= 5:  # batch notify co 5 healów
                    await self.push(
                        f"🩺 *OMEGA NAPRAWIŁA {diff} BŁĘDÓW*\n"
                        f"Łącznie napraw: {current_heals}"
                    )
            self._prev_heals = current_heals

        except Exception as exc:
            log.debug(f"_check_omega_changes: {exc}")

    async def _check_file_changes(self):
        """Wykrywa zmiany w plikach .py systemu."""
        for name, meta in SYSTEM_FILES.items():
            p = Path(meta["file"])
            if not p.exists():
                continue
            try:
                mtime = p.stat().st_mtime
                prev  = self._file_mtimes.get(name, 0)
                if prev and mtime > prev:
                    await self.push(
                        f"✏️ *PLIK ZMIENIONY*\n"
                        f"Plik: `{meta['file']}`\n"
                        f"_{meta['desc']}_\n"
                        f"Zmiana: {datetime.fromtimestamp(mtime).strftime('%H:%M:%S')}"
                    )
                self._file_mtimes[name] = mtime
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
#  SCREEN RENDERER EXTENSION  (dodatkowe ekrany dla interfejsu Telegram)
# ══════════════════════════════════════════════════════════════════════════════

class OmegaScreenExtension:
    """
    Rozszerza ScreenRenderer o nowe ekrany:
    - Ranking botów
    - Lista plików systemu
    - Historia healów
    - Edytor strategii
    """

    LOGO = "🩺 *NEXUS OMEGA BRIDGE*\n━━━━━━━━━━━━━━━━━━━━━━━\n"

    def __init__(self, fs: FileSystemOps, ranker: CodeQualityRanker,
                 agent: NexusOmegaAgent, pusher: OmegaEventPusher):
        self.fs     = fs
        self.ranker = ranker
        self.agent  = agent
        self.pusher = pusher

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime("%H:%M:%S")

    # ── Ranking botów ──────────────────────────────────────────────────────────

    def render_bot_ranking(self, top_n: int = 10) -> tuple:
        bots = self.ranker.rank_bots(top_n=top_n)
        txt  = f"{self.LOGO}*🏆 RANKING BOTÓW — TOP {top_n}*\n\n"
        for b in bots:
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(b["rank"], f"*{b['rank']}.*")
            bar_width = max(0, min(10, int(b["score"] / 10)))
            bar = "█" * bar_width + "░" * (10 - bar_width)
            txt += (
                f"{medal} {b['emoji']} *{b['name']}*\n"
                f"├ Ocena:   `{b['score']:.1f}/100` {bar}\n"
                f"├ Linie:   {b['lines']:,} | Fnc: {b['functions']}\n"
                f"├ Trading: {b['trading_score']:.0f}% | "
                f"Złożoność: {b['cyclomatic']}\n"
                f"└ _{b['desc']}_\n\n"
            )
        txt += f"_⏰ {self._ts()} — pisz `analiza [nazwa]` dla szczegółów_"
        kb = [
            [
                InlineKeyboardButton("🔄 Odśwież",    callback_data="ext:ranking"),
                InlineKeyboardButton("📁 Pliki",      callback_data="ext:files"),
            ],
            [
                InlineKeyboardButton("💬 Agent AI",   callback_data="s:chat"),
                InlineKeyboardButton("◀️ Menu",       callback_data="s:menu"),
            ],
        ]
        return txt, kb

    # ── Lista plików ───────────────────────────────────────────────────────────

    def render_file_list(self) -> tuple:
        files = self.fs.list_files()
        by_type: Dict[str, List] = defaultdict(list)
        for f in files:
            by_type[f["type"]].append(f)

        txt  = f"{self.LOGO}*📁 PLIKI SYSTEMU*\n\n"
        total_ok = sum(1 for f in files if f["exists"])
        txt += f"Dostępne: *{total_ok}/{len(files)}*\n\n"

        for t in sorted(by_type):
            flist = by_type[t]
            emoji = TYPE_EMOJI.get(t, "📄")
            txt += f"*{emoji} {t.upper()}*\n"
            for f in flist:
                status = "🟢" if f["exists"] else "🔴"
                txt += (
                    f"  {status} `{f['file']}` "
                    f"_{f['size_kb']}KB · {f['lines']:,}ln_\n"
                )
            txt += "\n"
        txt += f"_⏰ {self._ts()}_"
        kb = [
            [
                InlineKeyboardButton("🏆 Ranking",    callback_data="ext:ranking"),
                InlineKeyboardButton("🔄 Odśwież",    callback_data="ext:files"),
            ],
            [InlineKeyboardButton("◀️ Menu", callback_data="s:menu")],
        ]
        return txt, kb

    # ── Historia healów ────────────────────────────────────────────────────────

    def render_heal_history(self) -> tuple:
        txt = f"{self.LOGO}*🩺 HISTORIA NAPRAW OMEGA*\n\n"
        omega = self.agent.omega
        if not omega:
            txt += "_Omega nie jest podłączona._"
        else:
            try:
                summary  = omega.analyzer.get_summary(hours=24)
                recurring = omega.analyzer.get_recurring()[:5]
                r        = omega.reward.status()
                health   = omega.oracle.compute_system_health()

                txt += (
                    f"*STATYSTYKI 24h*\n"
                    f"├ Błędów łącznie:   *{summary.get('total', 0)}*\n"
                    f"├ Krytycznych:      *{summary.get('critical', 0)}*\n"
                    f"├ Naprawionych:     *{r.get('successful_heals', 0)}*\n"
                    f"├ Zdrowia systemu:  *{health.get('score', 0):.0f}%*\n"
                    f"└ Status:           *{health.get('status', '?')}*\n\n"
                )

                if recurring:
                    txt += "*🔁 POWRACAJĄCE PROBLEMY*\n"
                    for ri in recurring[:5]:
                        txt += (
                            f"  ● `{ri.get('key', '?')[:35]}` "
                            f"×{ri.get('count', 0)} | "
                            f"{ri.get('strategy', '?')}\n"
                        )
                else:
                    txt += "✅ Brak powtarzających się problemów\n"

            except Exception as exc:
                txt += f"❌ Błąd: {exc}\n"

        txt += f"\n_⏰ {self._ts()}_"
        kb = [
            [
                InlineKeyboardButton("🔄 Odśwież", callback_data="ext:heals"),
                InlineKeyboardButton("🩺 Omega",   callback_data="s:omega"),
            ],
            [InlineKeyboardButton("◀️ Menu", callback_data="s:menu")],
        ]
        return txt, kb

    # ── Push ustawienia ────────────────────────────────────────────────────────

    def render_push_settings(self, chat_id: int) -> tuple:
        is_sub = chat_id in self.pusher._subs
        filters = self.pusher._filters.get(chat_id, PushFilter())

        status = "🔔 AKTYWNE" if is_sub else "🔕 WYŁĄCZONE"
        txt = (
            f"{self.LOGO}"
            f"*📲 USTAWIENIA PUSH*\n\n"
            f"Status: *{status}*\n\n"
            f"*Typy powiadomień:*\n"
            f"{'✅' if filters.heal_events else '❌'} Naprawy Omega\n"
            f"{'✅' if filters.critical_errors else '❌'} Błędy krytyczne\n"
            f"{'✅' if filters.module_crashes else '❌'} Crashe modułów\n"
            f"{'✅' if filters.file_changes else '❌'} Zmiany plików\n"
            f"{'✅' if filters.level_ups else '❌'} Awanse Omega\n"
            f"{'✅' if filters.health_critical else '❌'} Zdrowie < 50%\n"
            f"{'✅' if filters.strategy_changes else '❌'} Zmiany strategii\n\n"
            f"_⏰ {self._ts()}_"
        )
        sub_btn = ("🔕 Wyłącz push" if is_sub else "🔔 Włącz push")
        kb = [
            [InlineKeyboardButton(sub_btn, callback_data="ext:toggle_push")],
            [
                InlineKeyboardButton("🏆 Ranking",   callback_data="ext:ranking"),
                InlineKeyboardButton("📁 Pliki",     callback_data="ext:files"),
            ],
            [InlineKeyboardButton("◀️ Menu", callback_data="s:menu")],
        ]
        return txt, kb


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA TELEGRAM BRIDGE — GŁÓWNA KLASA INTEGRACJI
# ══════════════════════════════════════════════════════════════════════════════

class OmegaTelegramBridge:
    """
    Główna klasa integracji:
    - Wstrzykuje się w NexusTelegramInterface
    - Zastępuje NexusAIAssistant własnym agentem z dostępem do plików
    - Dodaje nowe ekrany do ScreenRenderer
    - Uruchamia OmegaEventPusher
    - Rejestruje handlery callbacków

    Użycie:
        from omega_telegram_bridge import OmegaTelegramBridge

        bridge = OmegaTelegramBridge(omega=my_omega, agg=my_agg)
        await bridge.inject(telegram_interface)
        await bridge.start()
    """

    def __init__(self, omega=None, agg=None,
                 bot_token: Optional[str] = None):
        self.omega = omega
        self.agg   = agg
        self.fs    = FileSystemOps()
        self.ranker= CodeQualityRanker(self.fs)
        self.agent = NexusOmegaAgent(self.fs, self.ranker, agg, omega)
        self.pusher= OmegaEventPusher(omega, bot_token)
        self.screen= OmegaScreenExtension(
            self.fs, self.ranker, self.agent, self.pusher
        )
        self._iface = None  # NexusTelegramInterface
        log.info("🌉 OmegaTelegramBridge: zainicjalizowany")

    def inject(self, telegram_interface):
        """
        Wstrzyknij bridge do istniejącego NexusTelegramInterface.
        Zastępuje ai_assistant i rozszerza handlery callbacków.
        """
        self._iface = telegram_interface

        # Zastąp asystenta AI
        telegram_interface._ai_assistant = _BridgedAssistant(
            self.agent, telegram_interface
        )

        # Podłącz Omega do agregatora Telegram (jeśli NexusAggregator)
        if self.omega and hasattr(telegram_interface, "agg"):
            telegram_interface.agg._modules["self_healing_engine"] = self.omega

        # Zarejestruj ekrany rozszerzone
        orig_handler = getattr(telegram_interface, "_handle_callback", None)
        if orig_handler:
            telegram_interface._handle_callback = self._make_callback_handler(
                orig_handler
            )

        log.info("🌉 OmegaTelegramBridge: wstrzyknięty do NexusTelegramInterface")
        return self

    def _make_callback_handler(self, original):
        """Wrap oryginalnego handlera callbacków o nowe ekrany ext:*."""
        bridge = self

        async def wrapped(update, context):
            query = update.callback_query
            if query and query.data.startswith("ext:"):
                await query.answer()
                chat_id = query.message.chat_id
                bot     = context.bot
                action  = query.data[4:]

                if action == "ranking":
                    txt, kb = bridge.screen.render_bot_ranking()
                elif action == "files":
                    txt, kb = bridge.screen.render_file_list()
                elif action == "heals":
                    txt, kb = bridge.screen.render_heal_history()
                elif action == "push_settings":
                    txt, kb = bridge.screen.render_push_settings(chat_id)
                elif action == "toggle_push":
                    if chat_id in bridge.pusher._subs:
                        bridge.pusher.unsubscribe(chat_id)
                        txt = "🔕 Push wyłączony."
                    else:
                        bridge.pusher.subscribe(chat_id)
                        txt = "🔔 Push włączony! Będziesz otrzymywać powiadomienia."
                    kb  = [[InlineKeyboardButton("◀️ Wróć", callback_data="ext:push_settings")]]
                else:
                    txt, kb = "❓ Nieznana komenda ext:", []

                markup = InlineKeyboardMarkup(kb) if kb else None
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text=txt[:4090], reply_markup=markup,
                        parse_mode="Markdown",
                    )
                except Exception:
                    await bot.send_message(
                        chat_id=chat_id, text=txt[:4090],
                        reply_markup=markup, parse_mode="Markdown",
                    )
                return

            # Fallback do oryginalnego
            await original(update, context)

        return wrapped

    async def start(self):
        """Uruchom OmegaEventPusher."""
        await self.pusher.start()
        log.info("🌉 OmegaTelegramBridge: start")

    async def stop(self):
        await self.pusher.stop()

    def get_extra_menu_buttons(self) -> List:
        """Przyciski do dodania w menu Telegram."""
        return [
            [
                InlineKeyboardButton("🏆 Ranking botów", callback_data="ext:ranking"),
                InlineKeyboardButton("📁 Pliki systemu", callback_data="ext:files"),
            ],
            [
                InlineKeyboardButton("🩺 Historia heal", callback_data="ext:heals"),
                InlineKeyboardButton("📲 Push notify",   callback_data="ext:push_settings"),
            ],
        ]


# ══════════════════════════════════════════════════════════════════════════════
#  BRIDGED ASSISTANT  (wraps NexusAIAssistant z NexusOmegaAgent)
# ══════════════════════════════════════════════════════════════════════════════

class _BridgedAssistant:
    """
    Drop-in replacement dla NexusAIAssistant.
    Deleguje do NexusOmegaAgent który ma pełny dostęp do plików.
    """

    def __init__(self, agent: NexusOmegaAgent, iface):
        self.agent = agent
        self._iface = iface

    async def chat(self, chat_id: int, message: str) -> str:
        # Utwórz push_callback który wysyła wiadomości przez interfejs
        async def push_callback(text: str):
            bot = getattr(self._iface, "_bot", None)
            if not bot and hasattr(self._iface, "_app"):
                bot = self._iface._app.bot
            if bot:
                with suppress(Exception):
                    await bot.send_message(
                        chat_id=chat_id,
                        text=text[:4090],
                        parse_mode="Markdown",
                    )

        return await self.agent.chat(chat_id, message, push_callback)


# ══════════════════════════════════════════════════════════════════════════════
#  INTEGRATION HELPERS & FACTORY
# ══════════════════════════════════════════════════════════════════════════════

_bridge_instance: Optional[OmegaTelegramBridge] = None


def get_bridge(omega=None, agg=None) -> OmegaTelegramBridge:
    """Singleton OmegaTelegramBridge."""
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = OmegaTelegramBridge(omega=omega, agg=agg)
    return _bridge_instance


def setup_full_integration(
    telegram_interface,
    omega=None,
    agg=None,
    bot_token: Optional[str] = None,
) -> OmegaTelegramBridge:
    """
    Jednorazowe pełne podpięcie wszystkich komponentów.

    Użycie w main.py / launch_system.py:

        from omega_telegram_bridge import setup_full_integration
        from self_healing_engine import SelfHealingEngine
        from telegram_stats_provider import get_kraken_aggregator
        from telegram_interface import get_interface

        omega  = SelfHealingEngine()
        agg    = get_kraken_aggregator()
        iface  = get_interface()

        bridge = setup_full_integration(
            telegram_interface=iface,
            omega=omega.omega,   # NexusOmega instance
            agg=agg,
        )

        async def main():
            await omega.start()
            await bridge.start()
            await iface.run_forever()

        asyncio.run(main())
    """
    bridge = OmegaTelegramBridge(omega=omega, agg=agg, bot_token=bot_token)
    bridge.inject(telegram_interface)

    # Rozszerz menu o przyciski bridge'a
    orig_render = getattr(telegram_interface.renderer, "render_menu", None)
    if orig_render:
        extra_btns = bridge.get_extra_menu_buttons()

        def extended_menu():
            txt, kb = orig_render()
            # Wstaw nowe przyciski przed ostatnim wierszem (czat/ustawienia)
            kb = kb[:-1] + extra_btns + [kb[-1]]
            return txt, kb

        telegram_interface.renderer.render_menu = extended_menu

    log.info("🌉 OmegaTelegramBridge: pełna integracja zakończona")
    return bridge
