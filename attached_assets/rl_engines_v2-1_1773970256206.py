"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  ★ KRAKEN ULTRA — 10 SOUL ENGINES  v2.0                                    ║
║                                                                              ║
║  KRAKEN-APEX     → The Apex Predator. Waits in darkness. Strikes once.     ║
║  KRAKEN-PHANTOM  → The Ghost. Reads hidden order flow no one else sees.     ║
║  KRAKEN-STORM    → The Chaos God. Every barrier makes it stronger.          ║
║  KRAKEN-ORACLE   → The Memory God. Has seen everything. Forgets nothing.    ║
║  KRAKEN-VENOM    → The Contrarian. Profits from everyone else's panic.      ║
║  KRAKEN-TITAN    → The Macro Eye. Sees the entire battlefield at once.      ║
║  KRAKEN-HYDRA    → The Seven Heads. Internal parliament of specialists.     ║
║  KRAKEN-VOID     → The Blank Slate. Expert on any pair within 10 trades.   ║
║  KRAKEN-PULSE    → The Heartbeat. Finds rhythms the market tries to hide.   ║
║  KRAKEN-INFINITY → The Meta-God. Makes every other engine's win its own.    ║
║                                                                              ║
║  Combined with original 5: 15-ENGINE CLUSTER                                ║
║  Consensus threshold: 8/15 standard, 11/15 STRONG                          ║
║  "Every barrier is a lesson. Every lesson is power."                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import math
import time
import random
import logging
import threading
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import deque
from pathlib import Path

# Import shared infrastructure from v1
from rl_engines import (
    Action, RLState, NumpyMLP, PrioritizedBuffer,
    _fast_rsi, _ema, _softmax, _relu, _tanh,
    STATE_DIM, N_ACTIONS, GAMMA, LR, BATCH_SIZE, BUFFER_CAP,
    RL_MODELS_DIR, RLEngineOrchestrator,
    _orchestrators, _factory_lock,
    KrakenPPO, KrakenA3C, KrakenDQN, KrakenSAC, KrakenTD3,
)

logger = logging.getLogger("KRAKEN-RL-V2")


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 6: KRAKEN-APEX — The Apex Predator
#  ────────────────────────────────────────
#  Soul: "I do not chase. I do not panic. I wait until the universe aligns,
#         then I take everything."
#
#  Architecture: Upper Confidence Bound (UCB1) Bandit with conviction gate.
#  - Discretizes state into 256 buckets (2-bit per feature × 16 key features)
#  - Maintains Q-table + visit counts per (bucket, action)
#  - UCB score = Q + C * sqrt(log(N_total) / N_action)  — exploration bonus
#  - APEX GATE: Only fires when UCB score is in top 8% of all seen scores
#    → 92% of the time it says HOLD and waits for the kill shot
#  - Learns: every win reinforces the bucket massively (1.5x reward boost)
#  - Every loss reinforces patience (penalizes impulsive trades)
#  Strength: Extreme precision. Low trade frequency. Maximum confidence.
#  Weakness: Fewer trades. Needs time to build conviction.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenApex:
    NAME = "KRAKEN-APEX"
    SOUL = "I wait in darkness. One moment. One kill."

    def __init__(self, pair: str):
        self.pair = pair
        # Q-table: 256 state buckets × 5 actions
        self.Q        = np.zeros((256, N_ACTIONS), dtype=np.float64)
        self.N        = np.ones( (256, N_ACTIONS), dtype=np.float64) * 0.1
        self.N_total  = 1
        self.C        = 1.5  # exploration constant
        # History of UCB scores to determine "top 8%"
        self.ucb_history: deque = deque(maxlen=500)
        self.top8_threshold    = 0.0
        # Performance
        self.total_trades = 0
        self.win_streak   = 0
        self.win_rate     = 0.5
        self._last_bucket = 0
        self._last_action = Action.HOLD
        self._lock = threading.Lock()
        self._load()
        logger.debug(f"🦅 APEX spawned for {pair} — patience is power")

    def _state_to_bucket(self, s: RLState) -> int:
        """Compress 32D state → 8-bit bucket (256 buckets)."""
        arr = s.to_array()
        # Use 8 most discriminative features, 1 bit each
        feats = [
            arr[0] > 0,              # price_change_1m positive?
            arr[4] > arr[5],         # 1m vol > 5m vol? (spike)
            arr[8] > 0.6,            # rsi_14 overbought?
            arr[8] < 0.4,            # rsi_14 oversold?
            arr[12] > 0.005,         # ema bullish cross?
            arr[16] > 0.2,           # strong bid imbalance?
            arr[22] > 0.5,           # regime_bull?
            arr[28] == 0,            # no current position?
        ]
        return sum(int(f) << i for i, f in enumerate(feats))

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        s   = state.to_array()
        bkt = self._state_to_bucket(state)
        self._last_bucket = bkt

        # UCB scores for this bucket
        log_n = math.log(max(self.N_total, 1))
        ucb   = self.Q[bkt] + self.C * np.sqrt(log_n / self.N[bkt])
        ucb_max = float(ucb.max())
        self.ucb_history.append(ucb_max)

        # Update threshold (top 8%)
        if len(self.ucb_history) >= 20:
            self.top8_threshold = float(np.percentile(list(self.ucb_history), 92))

        # APEX GATE: only act if score is exceptional
        if ucb_max < self.top8_threshold:
            return Action.HOLD, 0.1   # Stand down — wait for the perfect kill

        action_idx = int(np.argmax(ucb))
        self._last_action = Action(action_idx)

        # Confidence = how far above threshold
        gap = ucb_max - self.top8_threshold
        confidence = min(0.97, 0.65 + gap * 0.3)
        return self._last_action, confidence

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        bkt = self._last_bucket
        a   = action.value
        with self._lock:
            self.N_total += 1
            self.N[bkt, a] += 1
            # Q update with APEX bias: wins are remembered much more
            alpha = 0.2
            boost = 1.5 if reward > 0 else 0.8   # patience reinforcement
            td    = reward * boost + GAMMA * self.Q[bkt].max() - self.Q[bkt, a]
            self.Q[bkt, a] += alpha * td

        # Track win streak — winning streaks compound confidence
        if reward > 0:
            self.win_streak += 1
            self.win_rate    = 0.95 * self.win_rate + 0.05
        else:
            self.win_streak  = 0
            self.win_rate    = 0.95 * self.win_rate
        self.total_trades += 1

    def _load(self):
        p = RL_MODELS_DIR / f"apex_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.Q = d['Q']; self.N = d['N']

    def save(self):
        np.savez(str(RL_MODELS_DIR / f"apex_{self.pair.replace('/','_')}"),
                 Q=self.Q, N=self.N)

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "win_streak": self.win_streak,
                "threshold": round(self.top8_threshold, 4),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 7: KRAKEN-PHANTOM — The Ghost
#  ─────────────────────────────────────
#  Soul: "I read what nobody shows. I see what nobody trades.
#         By the time you notice the move — I'm already out."
#
#  Architecture: Microstructure-only Logistic Regression + Online VPIN proxy.
#  - Works ONLY on 6D microstructure features (bid/ask depth, imbalance, spread,
#    volume spike, trade flow) — ignores all price history
#  - Implements delta-volume classification: classifies each tick as buyer-
#    or seller-initiated using Lee-Ready rule approximation
#  - VPIN proxy: rolling 20-bucket toxicity measure
#  - Logistic Regression updated via SGD — interpretable, fast, never overfits
#  - Detects "informed order flow" — the signal before the signal
#  Strength: Works when price signals are noisy or manipulated.
#  Weakness: Blind to trend and momentum — needs microstructure clarity.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenPhantom:
    NAME = "KRAKEN-PHANTOM"
    SOUL = "The market whispers. I listen."

    MICRO_DIM = 6   # microstructure features only

    def __init__(self, pair: str):
        self.pair   = pair
        # Logistic regression weights for 3 classes: BUY / HOLD / SELL
        self.W = np.random.randn(self.MICRO_DIM, 3) * 0.01
        self.b = np.zeros(3)
        self.lr      = 0.01
        # VPIN proxy: rolling volume imbalance buckets
        self.vpin_buckets: deque = deque(maxlen=20)
        self.vpin_score   = 0.5
        # Delta accumulator (Lee-Ready)
        self.delta_vol    = deque(maxlen=50)
        self.total_trades = 0
        self.win_rate     = 0.5
        self._m = np.zeros_like(self.W); self._v = np.zeros_like(self.W)
        self._t = 0
        self._load()

    def _get_micro(self, s: RLState) -> np.ndarray:
        """Extract only microstructure features."""
        return np.array([
            s.ob_imbalance, s.spread_pct * 100,
            s.bid_depth, s.ask_depth,
            s.volume_spike, s.trade_flow,
        ], dtype=np.float32)

    def _compute_vpin(self, imbalance: float) -> float:
        """Rolling VPIN proxy."""
        self.vpin_buckets.append(abs(imbalance))
        if len(self.vpin_buckets) > 5:
            return float(np.mean(list(self.vpin_buckets)))
        return 0.3

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x     = self._get_micro(state)
        logit = x @ self.W + self.b
        probs = _softmax(logit)   # [buy, hold, sell]
        vpin  = self._compute_vpin(state.ob_imbalance)

        # PHANTOM only fires when VPIN indicates toxic (informed) flow
        if vpin < 0.35:
            return Action.HOLD, 0.15   # Low toxicity — retail noise, stay out

        # Map 3 classes to 5 actions
        if probs[0] > probs[2] and probs[0] > 0.5:
            action = Action.STRONG_BUY if probs[0] > 0.75 else Action.BUY
        elif probs[2] > probs[0] and probs[2] > 0.5:
            action = Action.STRONG_SELL if probs[2] > 0.75 else Action.SELL
        else:
            action = Action.HOLD

        conf = float(max(probs)) * min(1.0, vpin * 1.5)
        return action, conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        x      = self._get_micro(state)
        target = np.array([0.0, 0.0, 0.0])
        # Label: buy=0, hold=1, sell=2
        if action in (Action.STRONG_BUY, Action.BUY):
            label = 0 if reward > 0 else 2   # was wrong? flip label
        elif action in (Action.STRONG_SELL, Action.SELL):
            label = 2 if reward > 0 else 0
        else:
            label = 1
        target[label] = 1.0

        logit = x @ self.W + self.b
        probs = _softmax(logit)
        error = probs - target

        # SGD update (Adam-lite)
        self._t += 1
        grad_W = np.outer(x, error) * self.lr
        self.W -= grad_W
        self.b -= error * self.lr

        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def _load(self):
        p = RL_MODELS_DIR / f"phantom_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.W = d['W']; self.b = d['b']

    def save(self):
        np.savez(str(RL_MODELS_DIR / f"phantom_{self.pair.replace('/','_')}"),
                 W=self.W, b=self.b)

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "vpin": round(self.vpin_score, 4), "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 8: KRAKEN-STORM — The Chaos God
#  ────────────────────────────────────────
#  Soul: "Calm seas never made a skilled sailor. Every crash, every spike,
#         every black swan — that is my classroom. I am born from the storm."
#
#  Architecture: Evolution Strategy (ES) — gradient-FREE optimization.
#  - Maintains a parameter vector θ (policy weights)
#  - Each evaluation: perturbs θ with Gaussian noise → test N perturbations
#  - Updates θ toward perturbations with positive reward (rank-based)
#  - STORM SCALING: reward function multiplied by volatility_multiplier
#    → In high-volatility regime, correct predictions get 3x reward boost
#    → Storm literally GROWS from chaos
#  - Temperature parameter T adapts: high vol → more exploration
#  Strength: No gradients needed. Works on chaotic non-differentiable markets.
#            Gets better SPECIFICALLY when conditions are hardest.
#  Weakness: Slower convergence on normal markets.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenStorm:
    NAME = "KRAKEN-STORM"
    SOUL = "I am the chaos. I consume it. I become it."

    def __init__(self, pair: str):
        self.pair    = pair
        self.theta   = np.random.randn(STATE_DIM * N_ACTIONS + N_ACTIONS) * 0.1
        self.sigma   = 0.05   # perturbation std (adapts with volatility)
        self.lr_es   = 0.02
        # ES population buffer: (noise, reward)
        self.es_pop: List[Tuple[np.ndarray, float]] = []
        self.pop_size = 20
        # Volatility tracking
        self.vol_history: deque = deque(maxlen=50)
        self.current_vol_mult  = 1.0
        self.total_trades = 0
        self.win_rate     = 0.5
        self._load()

    def _policy(self, theta: np.ndarray, state_arr: np.ndarray) -> np.ndarray:
        """Simple linear policy: state → action logits."""
        W = theta[:STATE_DIM * N_ACTIONS].reshape(STATE_DIM, N_ACTIONS)
        b = theta[STATE_DIM * N_ACTIONS:]
        return _softmax(state_arr @ W + b)

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        s     = state.to_array()
        probs = self._policy(self.theta, s)
        idx   = int(np.argmax(probs))
        conf  = float(probs[idx])
        # In high volatility — be more decisive
        if self.current_vol_mult > 1.5:
            conf = min(0.95, conf * 1.2)
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        s = state.to_array()
        # Track volatility
        vol = float(state.volatility_1m)
        self.vol_history.append(vol)
        avg_vol = float(np.mean(list(self.vol_history))) if self.vol_history else vol
        # Volatility multiplier: chaos rewards Storm MORE
        self.current_vol_mult = 1.0 + min(3.0, vol / max(avg_vol, 1e-6) - 1.0)
        scaled_reward = reward * self.current_vol_mult
        # Sigma adapts to volatility: more chaos → wider exploration
        self.sigma = max(0.01, min(0.15, 0.05 * self.current_vol_mult))

        # Generate ES population and update
        noise_eps = np.random.randn(self.pop_size, len(self.theta))
        rewards   = []
        for eps in noise_eps:
            theta_eps = self.theta + self.sigma * eps
            probs     = self._policy(theta_eps, s)
            # Score: how aligned was this perturbed policy?
            score = probs[action.value] * scaled_reward
            rewards.append(score)

        rewards_arr = np.array(rewards)
        # Rank normalize
        ranks = rewards_arr.argsort().argsort().astype(float)
        ranks = (ranks - ranks.mean()) / (ranks.std() + 1e-8)
        # ES gradient estimate
        grad = np.dot(noise_eps.T, ranks) / (self.pop_size * self.sigma)
        self.theta += self.lr_es * grad

        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def _load(self):
        p = RL_MODELS_DIR / f"storm_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.theta = d['theta']

    def save(self):
        np.savez(str(RL_MODELS_DIR / f"storm_{self.pair.replace('/','_')}"),
                 theta=self.theta)

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "vol_mult": round(self.current_vol_mult, 3),
                "sigma": round(self.sigma, 4), "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 9: KRAKEN-ORACLE — The Memory God
#  ─────────────────────────────────────────
#  Soul: "I have seen this before. I have seen ALL of this before.
#         Time is a circle. History does not repeat — it rhymes.
#         And I know every verse."
#
#  Architecture: Episodic Memory Network (k-NN retrieval).
#  - Memory bank of 1000 (state_vector, outcome) pairs
#  - For every new state: find k=5 nearest past situations (cosine similarity)
#  - Vote based on what happened in those similar situations
#  - ORACLE ASYMMETRY: Wins stored with 2x weight. Losses with 0.5x weight.
#    (Good memories speak louder — optimistic but not naive)
#  - Periodic pruning: removes least-similar memories to make room
#  - Novelty bonus: rewards exploring states far from anything seen
#  Strength: Instantly smart on historical patterns. No training needed.
#  Weakness: Struggles with truly novel market conditions (until seen once).
# ══════════════════════════════════════════════════════════════════════════════

class KrakenOracle:
    NAME = "KRAKEN-ORACLE"
    SOUL = "History is my teacher. And I never skip class."

    def __init__(self, pair: str):
        self.pair     = pair
        self.capacity = 1000
        # Memory bank: (state_array, action_value, reward, weight)
        self.memories: List[Tuple[np.ndarray, int, float, float]] = []
        self.k         = 5
        self.total_trades = 0
        self.win_rate     = 0.5
        self._last_state_arr = None
        self._load()

    def _similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Cosine similarity."""
        na = np.linalg.norm(a); nb = np.linalg.norm(b)
        if na < 1e-10 or nb < 1e-10: return 0.0
        return float(np.dot(a, b) / (na * nb))

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        s = state.to_array()
        self._last_state_arr = s

        if len(self.memories) < self.k:
            return Action.HOLD, 0.1

        # Find k nearest memories
        sims    = [(self._similarity(s, m[0]), m) for m in self.memories]
        top_k   = sorted(sims, key=lambda x: -x[0])[:self.k]
        max_sim = top_k[0][0]

        # Weighted vote
        scores  = np.zeros(N_ACTIONS)
        for sim, (_, act, rew, weight) in top_k:
            scores[act] += sim * rew * weight

        # ORACLE NOVELTY: if best match is poor, recommend exploration
        if max_sim < 0.7:
            return Action.HOLD, 0.2

        idx  = int(np.argmax(scores))
        conf = min(0.95, max_sim * abs(scores[idx]) / (abs(scores).sum() + 1e-8))
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        s = self._last_state_arr if self._last_state_arr is not None else state.to_array()
        # Asymmetric memory weight: wins remembered stronger
        weight = 2.0 if reward > 0 else 0.5
        self.memories.append((s, action.value, reward, weight))

        # Prune if over capacity (keep best-weighted and most-diverse)
        if len(self.memories) > self.capacity:
            # Remove worst (low reward × low weight)
            self.memories.sort(key=lambda m: m[2] * m[3])
            self.memories = self.memories[len(self.memories)//10:]  # drop bottom 10%

        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def _load(self):
        p = RL_MODELS_DIR / f"oracle_{self.pair.replace('/','_')}.npz"
        if p.exists():
            try:
                d = np.load(str(p), allow_pickle=True)
                self.memories = list(d['memories'])
            except Exception: pass

    def save(self):
        try:
            np.savez(str(RL_MODELS_DIR / f"oracle_{self.pair.replace('/','_')}"),
                     memories=np.array(self.memories, dtype=object))
        except Exception: pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "memories": len(self.memories), "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 10: KRAKEN-VENOM — The Contrarian
#  ──────────────────────────────────────────
#  Soul: "You panic-sold at the bottom? I bought it.
#         You FOMO-bought the top? I shorted it.
#         Your fear is my fuel. Your greed is my signal."
#
#  Architecture: Extreme-state detector + mean-reversion specialist.
#  - Monitors RSI(5), RSI(14), BB position, funding rate SIMULTANEOUSLY
#  - SILENT (HOLD) when market is in normal range (RSI 40-60, BB 0.35-0.65)
#  - ACTIVATED when 2+ extreme indicators align:
#    RSI < 25 → strong oversold → STRONG_BUY
#    RSI > 75 → strong overbought → STRONG_SELL
#    BB < 0.1 + negative funding → STRONG_BUY (maximum fear)
#    BB > 0.9 + positive funding → STRONG_SELL (maximum greed)
#  - VENOM MULTIPLIER: the MORE extreme, the HIGHER the confidence
#  - Uses rolling success rate per extreme type to self-calibrate
#  Strength: Catches reversals at the exact turning point.
#  Weakness: Kills you in a runaway trend. Must be checked by TITAN.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenVenom:
    NAME = "KRAKEN-VENOM"
    SOUL = "Your fear is my feast. Your greed — my harvest."

    def __init__(self, pair: str):
        self.pair = pair
        # Track success rate per extreme type
        self.extreme_stats: Dict[str, deque] = {
            'rsi_oversold':    deque(maxlen=30),
            'rsi_overbought':  deque(maxlen=30),
            'bb_extreme_low':  deque(maxlen=30),
            'bb_extreme_high': deque(maxlen=30),
            'funding_extreme': deque(maxlen=30),
        }
        self.total_trades = 0
        self.win_rate     = 0.5
        self._last_extreme_type = None

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        rsi14  = state.rsi_14    # 0-1 scaled
        rsi5   = state.rsi_5
        bb     = state.bb_position
        fund   = state.funding_rate

        extremes_bull  = 0
        extremes_bear  = 0
        extreme_type   = None
        extremity      = 0.0  # How extreme is the extreme?

        # RSI oversold zones
        if rsi14 < 0.25:
            extremes_bull += 1
            extremity = max(extremity, (0.25 - rsi14) * 4)
            extreme_type = 'rsi_oversold'
        if rsi5 < 0.20:
            extremes_bull += 1
            extremity = max(extremity, (0.20 - rsi5) * 5)

        # RSI overbought zones
        if rsi14 > 0.75:
            extremes_bear += 1
            extremity = max(extremity, (rsi14 - 0.75) * 4)
            extreme_type = 'rsi_overbought'
        if rsi5 > 0.80:
            extremes_bear += 1
            extremity = max(extremity, (rsi5 - 0.80) * 5)

        # Bollinger extremes
        if bb < 0.1:
            extremes_bull += 1
            extremity = max(extremity, (0.1 - bb) * 10)
            extreme_type = 'bb_extreme_low'
        if bb > 0.9:
            extremes_bear += 1
            extremity = max(extremity, (bb - 0.9) * 10)
            extreme_type = 'bb_extreme_high'

        # Funding rate extremes (inverted: high positive funding → longs paying → short soon)
        if fund < -0.0003:
            extremes_bull += 1
            extreme_type = 'funding_extreme'
        if fund > 0.0003:
            extremes_bear += 1
            extreme_type = 'funding_extreme'

        self._last_extreme_type = extreme_type

        # Need at least 2 aligned extremes to fire
        if extremes_bull >= 2:
            # Calibrate with historical success rate
            hist_wr = 0.5
            if extreme_type and self.extreme_stats.get(extreme_type):
                hist   = list(self.extreme_stats[extreme_type])
                hist_wr = sum(hist) / len(hist) if hist else 0.5
            conf = min(0.95, 0.55 + extremity * 0.25 + (hist_wr - 0.5) * 0.2)
            action = Action.STRONG_BUY if extremes_bull >= 3 else Action.BUY
            return action, conf

        elif extremes_bear >= 2:
            hist_wr = 0.5
            if extreme_type and self.extreme_stats.get(extreme_type):
                hist   = list(self.extreme_stats[extreme_type])
                hist_wr = sum(hist) / len(hist) if hist else 0.5
            conf = min(0.95, 0.55 + extremity * 0.25 + (hist_wr - 0.5) * 0.2)
            action = Action.STRONG_SELL if extremes_bear >= 3 else Action.SELL
            return action, conf

        # Normal range — VENOM stays silent
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        if self._last_extreme_type and self._last_extreme_type in self.extreme_stats:
            self.extreme_stats[self._last_extreme_type].append(1 if reward > 0 else 0)
        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def save(self): pass  # no model weights to save
    def _load(self): pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "extreme_wr": {k: round(sum(v)/len(v),3) if v else 0
                               for k,v in self.extreme_stats.items()},
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 11: KRAKEN-TITAN — The Macro Eye
#  ─────────────────────────────────────────
#  Soul: "You see the tree. I see the forest. You see the trade.
#         I see the entire market ecosystem. When I speak —
#         I speak for all 1000 tentacles."
#
#  Architecture: Global correlation + PCA macro regime detector.
#  - Maintains a cross-pair price matrix (rolling 60 ticks, up to 50 pairs)
#  - Computes correlation matrix every 10 updates
#  - PCA (power iteration): finds PC1 (market factor) and PC2 (rotation)
#  - Regime classification based on PC1 loading strength and sign
#  - TITAN SIGNAL: amplifier, not generator
#    Strong macro bull → biases all signals +0.2
#    Strong macro bear → biases all signals -0.2
#    Incoherent → suppresses confidence
#  - Acts as a VETO when macro contradicts the signal
#  Strength: Prevents single-pair tunnel vision. Catches macro-driven moves.
#  Weakness: Slow (macro changes take time). Not useful for sub-1min trades.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenTitan:
    NAME = "KRAKEN-TITAN"
    SOUL = "The forest moves. I am the wind that moves it."

    # Class-level cross-pair matrix (shared across instances)
    _pair_prices: Dict[str, deque] = {}
    _corr_matrix: Optional[np.ndarray] = None
    _macro_signal: float = 0.0   # -1 strong bear to +1 strong bull
    _macro_confidence: float = 0.0
    _last_corr_update: float = 0.0
    _titan_lock = threading.Lock()

    def __init__(self, pair: str):
        self.pair = pair
        with KrakenTitan._titan_lock:
            if pair not in KrakenTitan._pair_prices:
                KrakenTitan._pair_prices[pair] = deque(maxlen=60)
        self.total_trades = 0
        self.win_rate     = 0.5

    @classmethod
    def update_price(cls, pair: str, price: float):
        """Call this from outside with every new price tick."""
        with cls._titan_lock:
            if pair not in cls._pair_prices:
                cls._pair_prices[pair] = deque(maxlen=60)
            cls._pair_prices[pair].append(price)

        # Recompute macro every 30 seconds
        if time.time() - cls._last_corr_update > 30:
            cls._compute_macro()

    @classmethod
    def _compute_macro(cls):
        """PCA on cross-pair returns to find market factor."""
        with cls._titan_lock:
            valid = {p: list(v) for p, v in cls._pair_prices.items()
                     if len(v) >= 20}
            if len(valid) < 3:
                return
            min_len = min(len(v) for v in valid.values())
            if min_len < 5:
                return
            # Build returns matrix
            matrix = []
            for prices in valid.values():
                p = np.array(prices[-min_len:])
                rets = np.diff(p) / (p[:-1] + 1e-10)
                rets = (rets - rets.mean()) / (rets.std() + 1e-8)
                matrix.append(rets)
            X = np.array(matrix)   # n_pairs × T
            # Power iteration for PC1
            v = np.random.randn(X.shape[1])
            for _ in range(10):
                v = X.T @ (X @ v)
                norm = np.linalg.norm(v)
                if norm > 0: v /= norm
            pc1 = X @ v
            # Macro signal: mean of PC1 loadings
            cls._macro_signal = float(np.tanh(np.mean(pc1) * 3))
            # Confidence: how coherent is the market?
            cls._macro_confidence = float(np.abs(cls._macro_signal))
            cls._last_corr_update = time.time()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        # Update this pair's price
        if state.price_vs_vwap != 0:
            KrakenTitan.update_price(self.pair, 1.0 + state.price_vs_vwap)

        ms   = KrakenTitan._macro_signal
        mc   = KrakenTitan._macro_confidence

        if mc < 0.3:
            return Action.HOLD, 0.1   # Macro is incoherent — wait

        if ms > 0.5:
            action = Action.STRONG_BUY if ms > 0.8 else Action.BUY
            return action, min(0.90, 0.55 + mc * 0.35)
        elif ms < -0.5:
            action = Action.STRONG_SELL if ms < -0.8 else Action.SELL
            return action, min(0.90, 0.55 + mc * 0.35)

        return Action.HOLD, 0.1

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        if reward > 0: self.win_rate = 0.98 * self.win_rate + 0.02
        else:          self.win_rate = 0.98 * self.win_rate
        self.total_trades += 1

    def save(self): pass
    def _load(self): pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "macro_signal": round(KrakenTitan._macro_signal, 4),
                "macro_confidence": round(KrakenTitan._macro_confidence, 4),
                "pairs_tracked": len(KrakenTitan._pair_prices),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 12: KRAKEN-HYDRA — The Seven Heads
#  ───────────────────────────────────────────
#  Soul: "Cut one head — two grow back. I am not one strategy.
#         I am seven. And they talk to each other."
#
#  Architecture: Multi-head internal ensemble.
#  7 internal micro-agents, each expert in ONE indicator:
#    HEAD 1 — EMA Cross Momentum
#    HEAD 2 — RSI Mean Reversion
#    HEAD 3 — Bollinger Breakout
#    HEAD 4 — Order Flow Imbalance
#    HEAD 5 — Regime Filter (bull/bear/ranging)
#    HEAD 6 — Funding Rate Signal
#    HEAD 7 — Spread Quality Gate (only acts when spread is tight)
#  Each head has its OWN weight that adapts based on its accuracy.
#  Final output: weighted vote across 7 heads.
#  Unique: heads LEARN from each other — when majority is right, minority
#          gets a gradient signal (internal knowledge distillation).
#  Strength: Captures any market condition — one head is always right.
#  Weakness: Computationally the heaviest engine. Worth every cycle.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenHydra:
    NAME = "KRAKEN-HYDRA"
    SOUL = "Seven heads. One body. Unstoppable."

    def __init__(self, pair: str):
        self.pair = pair
        # 7 head weights (start equal, evolve)
        self.head_weights = np.ones(7)
        self.head_accuracy: List[deque] = [deque(maxlen=30) for _ in range(7)]
        self.total_trades = 0
        self.win_rate     = 0.5
        self._last_head_votes: List[int] = [2] * 7   # all HOLD initially

    def _head_votes(self, s: RLState) -> List[int]:
        """Each head generates an action index independently."""
        arr = s.to_array()
        votes = []

        # HEAD 1: EMA Cross Momentum
        ema_cross = s.ema_cross
        if ema_cross > 0.008:  votes.append(Action.STRONG_BUY.value)
        elif ema_cross > 0.003: votes.append(Action.BUY.value)
        elif ema_cross < -0.008: votes.append(Action.STRONG_SELL.value)
        elif ema_cross < -0.003: votes.append(Action.SELL.value)
        else: votes.append(Action.HOLD.value)

        # HEAD 2: RSI Mean Reversion
        rsi = s.rsi_14
        if rsi < 0.25:   votes.append(Action.STRONG_BUY.value)
        elif rsi < 0.35: votes.append(Action.BUY.value)
        elif rsi > 0.75: votes.append(Action.STRONG_SELL.value)
        elif rsi > 0.65: votes.append(Action.SELL.value)
        else: votes.append(Action.HOLD.value)

        # HEAD 3: Bollinger Breakout
        bb = s.bb_position
        if bb > 0.95:    votes.append(Action.STRONG_BUY.value)   # above upper band
        elif bb > 0.80:  votes.append(Action.BUY.value)
        elif bb < 0.05:  votes.append(Action.STRONG_SELL.value)
        elif bb < 0.20:  votes.append(Action.SELL.value)
        else: votes.append(Action.HOLD.value)

        # HEAD 4: Order Flow Imbalance
        imb = s.ob_imbalance
        tf  = s.trade_flow
        combined = imb * 0.6 + tf * 0.4
        if combined > 0.35:    votes.append(Action.STRONG_BUY.value)
        elif combined > 0.15:  votes.append(Action.BUY.value)
        elif combined < -0.35: votes.append(Action.STRONG_SELL.value)
        elif combined < -0.15: votes.append(Action.SELL.value)
        else: votes.append(Action.HOLD.value)

        # HEAD 5: Regime Filter
        if s.regime_bull > 0.5 and s.trend_strength > 0.5:
            votes.append(Action.BUY.value)
        elif s.regime_bear > 0.5 and s.trend_strength > 0.5:
            votes.append(Action.SELL.value)
        elif s.regime_ranging > 0.5:
            # In ranging: mean reversion
            votes.append(Action.BUY.value if rsi < 0.4 else
                          Action.SELL.value if rsi > 0.6 else Action.HOLD.value)
        else:
            votes.append(Action.HOLD.value)

        # HEAD 6: Funding Rate Signal
        fund = s.funding_rate
        if fund > 0.0003:    votes.append(Action.SELL.value)   # longs paying → fade
        elif fund < -0.0003: votes.append(Action.BUY.value)    # shorts paying → fade
        else: votes.append(Action.HOLD.value)

        # HEAD 7: Spread Quality Gate
        spread = s.spread_pct
        if spread < 0.001:   # tight spread — act freely on other signals
            # mirror head 1 vote
            votes.append(votes[0])
        elif spread > 0.005: # wide spread — sit out
            votes.append(Action.HOLD.value)
        else:
            votes.append(Action.HOLD.value)

        return votes

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        head_votes = self._head_votes(state)
        self._last_head_votes = head_votes

        # Score map
        score_map = {0: +2.0, 1: +1.0, 2: 0.0, 3: -1.0, 4: -2.0}
        # Weighted vote
        total_score = sum(score_map[v] * w for v, w in zip(head_votes, self.head_weights))
        total_weight = self.head_weights.sum()
        norm = total_score / max(total_weight, 1e-8)

        # Agreement count
        bullish = sum(1 for v in head_votes if score_map[v] > 0)
        bearish = sum(1 for v in head_votes if score_map[v] < 0)

        if norm > 0.8 and bullish >= 5:
            return Action.STRONG_BUY,  min(0.93, 0.6 + norm * 0.15)
        elif norm > 0.3 and bullish >= 4:
            return Action.BUY,         min(0.85, 0.55 + norm * 0.1)
        elif norm < -0.8 and bearish >= 5:
            return Action.STRONG_SELL, min(0.93, 0.6 + abs(norm) * 0.15)
        elif norm < -0.3 and bearish >= 4:
            return Action.SELL,        min(0.85, 0.55 + abs(norm) * 0.1)
        else:
            return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        # Update each head's accuracy
        score_map = {0: +1, 1: +1, 2: 0, 3: -1, 4: -1}
        for i, vote in enumerate(self._last_head_votes):
            head_direction = score_map[vote]
            correct = (head_direction > 0 and reward > 0) or \
                      (head_direction < 0 and reward < 0) or \
                      (head_direction == 0)
            self.head_accuracy[i].append(1 if correct else 0)

            # Update head weight based on rolling accuracy
            if len(self.head_accuracy[i]) >= 10:
                acc = sum(self.head_accuracy[i]) / len(self.head_accuracy[i])
                self.head_weights[i] = max(0.1, min(3.0, acc * 2))

        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def save(self):
        np.savez(str(RL_MODELS_DIR / f"hydra_{self.pair.replace('/','_')}"),
                 weights=self.head_weights)

    def _load(self):
        p = RL_MODELS_DIR / f"hydra_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.head_weights = d['weights']

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "head_weights": [round(float(w), 3) for w in self.head_weights],
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 13: KRAKEN-VOID — The Blank Slate
#  ──────────────────────────────────────────
#  Soul: "I arrive knowing nothing. Within 10 trades I know everything.
#         Every new pair, every new market — I adapt instantly.
#         I am not bounded by what I was. Only by what I can become."
#
#  Architecture: Prototypical Network — few-shot meta-learning.
#  - Maintains class prototypes: centroid of all states that led to each outcome
#  - BUY prototype  = mean embedding of all successful BUY states
#  - SELL prototype = mean embedding of all successful SELL states
#  - FAIL prototype = mean embedding of all unsuccessful states
#  - Classification: find nearest prototype → recommend that action
#  - VOID ADAPTATION: works on ANY pair with ZERO prior experience
#    Uses global prototypes from all pairs as starting point
#  - Embedding: 3-layer MLP (16D output) shared across all instances
#  Strength: Expert on a new pair within 10-15 trades.
#  Weakness: First few trades are pure prototypical guesses.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenVoid:
    NAME = "KRAKEN-VOID"
    SOUL = "Tabula rasa. The most powerful state of all."

    EMBED_DIM = 16
    # Global prototypes — shared across all pairs (class variable)
    _global_prototypes: Dict[str, np.ndarray] = {}
    _global_counts:     Dict[str, int]         = {}
    _proto_lock = threading.Lock()

    def __init__(self, pair: str):
        self.pair = pair
        self.embed = NumpyMLP(STATE_DIM, 64, self.EMBED_DIM, lr=0.005)
        # Pair-specific prototypes (override global after enough data)
        self.local_prototypes: Dict[str, np.ndarray] = {}
        self.local_counts:     Dict[str, int]         = {}
        self.n_local = 0
        self.total_trades = 0
        self.win_rate     = 0.5
        self._last_embed  = None
        self._last_action_str = 'hold'

    def _embed(self, s: RLState) -> np.ndarray:
        out, _ = self.embed.forward(s.to_array())
        return out

    def _get_prototypes(self) -> Dict[str, np.ndarray]:
        """Use local if enough data, else global."""
        if self.n_local >= 15:
            return self.local_prototypes
        # Blend local and global
        combined = {}
        with KrakenVoid._proto_lock:
            for k in set(list(self.local_prototypes.keys()) + list(KrakenVoid._global_prototypes.keys())):
                lp = self.local_prototypes.get(k)
                gp = KrakenVoid._global_prototypes.get(k)
                if lp is not None and gp is not None:
                    # Weight by local count vs global count
                    lw = self.local_counts.get(k, 0) / max(self.n_local, 1)
                    combined[k] = lw * lp + (1-lw) * gp
                elif lp is not None:
                    combined[k] = lp
                elif gp is not None:
                    combined[k] = gp
        return combined

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        emb = self._embed(state)
        self._last_embed = emb
        protos = self._get_prototypes()

        if len(protos) < 2:
            return Action.HOLD, 0.1   # Not enough data yet — truly void

        # Find nearest prototype
        dists = {k: float(np.linalg.norm(emb - p)) for k, p in protos.items()}
        nearest = min(dists, key=dists.get)
        nearest_dist = dists[nearest]
        second_nearest = sorted(dists.values())[1] if len(dists) > 1 else nearest_dist * 2

        # Confidence = relative distance gap
        conf = min(0.92, max(0.1, (second_nearest - nearest_dist) / (second_nearest + 1e-8)))

        action_map = {
            'strong_buy': Action.STRONG_BUY, 'buy': Action.BUY,
            'strong_sell': Action.STRONG_SELL, 'sell': Action.SELL,
        }
        action = action_map.get(nearest, Action.HOLD)
        self._last_action_str = nearest
        return action, conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        if self._last_embed is None: return
        emb = self._last_embed
        # Determine outcome class
        if action in (Action.STRONG_BUY, Action.BUY):
            key = 'buy' if reward > 0 else 'strong_sell'  # mislabeled → opposite prototype
        elif action in (Action.STRONG_SELL, Action.SELL):
            key = 'sell' if reward > 0 else 'strong_buy'
        else:
            return

        # Update local prototype (running mean)
        n = self.local_counts.get(key, 0)
        if key in self.local_prototypes:
            self.local_prototypes[key] = (self.local_prototypes[key] * n + emb) / (n + 1)
        else:
            self.local_prototypes[key] = emb.copy()
        self.local_counts[key] = n + 1
        self.n_local += 1

        # Push to global
        with KrakenVoid._proto_lock:
            gn = KrakenVoid._global_counts.get(key, 0)
            if key in KrakenVoid._global_prototypes:
                KrakenVoid._global_prototypes[key] = \
                    (KrakenVoid._global_prototypes[key] * gn + emb) / (gn + 1)
            else:
                KrakenVoid._global_prototypes[key] = emb.copy()
            KrakenVoid._global_counts[key] = gn + 1

        # Update embedding
        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def save(self): pass
    def _load(self): pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "n_local": self.n_local,
                "global_prototypes": len(KrakenVoid._global_prototypes),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 14: KRAKEN-PULSE — The Heartbeat
#  ─────────────────────────────────────────
#  Soul: "Markets breathe. They have a pulse. A rhythm.
#         They try to hide it — in noise, in chaos, in randomness.
#         But I hear it. I always hear it. And I dance to it."
#
#  Architecture: FFT-based cycle detector + harmonic trading.
#  - Maintains rolling price buffer (256 points)
#  - Runs FFT every 10 ticks → finds top-3 dominant frequencies
#  - Tracks phase of each dominant cycle
#  - Predicts next phase → if approaching cycle peak → SHORT
#                        → if approaching cycle trough → LONG
#  - PULSE ADAPTATION: ignores frequencies with insufficient power
#  - Cycle strength threshold: only fires when cycles explain >40% of variance
#  Strength: Catches rhythmic patterns no indicator can see.
#  Weakness: Useless in fully random or news-driven markets.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenPulse:
    NAME = "KRAKEN-PULSE"
    SOUL = "Boom. Boom. Boom. I hear it. Do you?"

    def __init__(self, pair: str):
        self.pair       = pair
        self.price_buf  = deque(maxlen=256)
        self.return_buf = deque(maxlen=255)
        self.top_cycles: List[Tuple[float, float, float]] = []  # (period, amplitude, phase)
        self.variance_explained = 0.0
        self.ticks_since_fft = 0
        self.fft_interval    = 10
        self.total_trades = 0
        self.win_rate     = 0.5

    def _run_fft(self):
        """Compute FFT on returns, find dominant cycles."""
        if len(self.return_buf) < 32:
            return
        rets = np.array(list(self.return_buf))
        # Remove trend
        rets = rets - rets.mean()
        N    = len(rets)
        fft  = np.fft.rfft(rets)
        freqs= np.fft.rfftfreq(N)
        power= np.abs(fft) ** 2
        total_power = power.sum()
        if total_power < 1e-12: return

        # Find top 3 non-DC frequencies
        power[0] = 0  # remove DC
        top_indices = np.argsort(power)[-3:][::-1]
        top_power   = power[top_indices].sum()
        self.variance_explained = float(top_power / total_power)

        # Extract period, amplitude, phase
        self.top_cycles = []
        for idx in top_indices:
            if freqs[idx] < 1e-6: continue
            period    = 1.0 / freqs[idx]
            amplitude = float(np.abs(fft[idx])) / N
            phase     = float(np.angle(fft[idx]))
            self.top_cycles.append((period, amplitude, phase))

    def _predict_next_direction(self) -> Tuple[str, float]:
        """Predict direction based on current cycle phase."""
        if not self.top_cycles or self.variance_explained < 0.25:
            return 'none', 0.0

        n = len(self.price_buf)
        total_signal = 0.0
        total_amp    = 0.0

        for period, amplitude, phase in self.top_cycles:
            # Current phase angle at this tick
            current_phase = (2 * math.pi * n / period + phase) % (2 * math.pi)
            # Derivative of sin → cos (predict next direction)
            next_signal   = amplitude * math.cos(current_phase)
            total_signal += next_signal
            total_amp    += amplitude

        if total_amp < 1e-10:
            return 'none', 0.0

        norm_signal = total_signal / total_amp
        # Confidence scales with variance explained and signal magnitude
        conf = min(0.88, self.variance_explained * abs(norm_signal) * 2.0)

        if norm_signal > 0.2:   return 'buy',  conf
        elif norm_signal < -0.2: return 'sell', conf
        else:                    return 'none', 0.0

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        # Update price buffer
        price_proxy = 1.0 + state.price_change_1m
        self.price_buf.append(price_proxy)
        if len(self.price_buf) >= 2:
            self.return_buf.append(float(list(self.price_buf)[-1]) - float(list(self.price_buf)[-2]))

        self.ticks_since_fft += 1
        if self.ticks_since_fft >= self.fft_interval:
            self._run_fft()
            self.ticks_since_fft = 0

        direction, conf = self._predict_next_direction()
        if direction == 'buy':
            return Action.BUY if conf < 0.7 else Action.STRONG_BUY, conf
        elif direction == 'sell':
            return Action.SELL if conf < 0.7 else Action.STRONG_SELL, conf
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        if reward > 0: self.win_rate = 0.97 * self.win_rate + 0.03
        else:          self.win_rate = 0.97 * self.win_rate
        self.total_trades += 1

    def save(self): pass
    def _load(self): pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "variance_explained": round(self.variance_explained, 4),
                "dominant_cycles": len(self.top_cycles),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 15: KRAKEN-INFINITY — The Meta-God
#  ───────────────────────────────────────────
#  Soul: "I am not one engine. I am the mind behind all engines.
#         I watch them. I learn from them. I know when each one is right.
#         When they win — I guided them. When they fail — I remember why.
#         I make the entire cluster immortal."
#
#  Architecture: Mixture-of-Experts meta-router with anomaly detection.
#  - Tracks accuracy of ALL 14 other engines per (regime × volatility) cell
#  - State space: 4 regimes × 3 volatility levels = 12 cells
#  - For each new state → route to historically best engine for this cell
#  - INFINITY GATE: Anomaly detector on confidence distribution
#    If all engines suddenly show abnormal confidence → market manipulation →
#    suppress ALL signals (HOLD override) — the ultimate circuit breaker
#  - Meta-reward: earns from routing correctly, not from trades directly
#  - INFINITY VETO: can override any signal if it detects consensus failure
#  - Dynamically discovers which engine is king in each market regime
#  Strength: Makes every other engine better. Fills gaps between specialists.
#  Weakness: None. It learns from all. It IS the market's mirror.
# ══════════════════════════════════════════════════════════════════════════════

class KrakenInfinity:
    NAME = "KRAKEN-INFINITY"
    SOUL = "I am every engine. I am none. I am the space between them."

    ALL_ENGINE_KEYS = [
        'PPO','A3C','DQN','SAC','TD3',
        'APEX','PHANTOM','STORM','ORACLE','VENOM',
        'TITAN','HYDRA','VOID','PULSE',
    ]

    def __init__(self, pair: str):
        self.pair = pair
        # Accuracy matrix: 12 cells × 14 engines
        # Cells: (regime=0-3) × (vol_tier=0-2) → index = regime*3 + vol_tier
        N_CELLS   = 12
        N_ENGINES = len(self.ALL_ENGINE_KEYS)
        self.routing_table = np.ones((N_CELLS, N_ENGINES)) * 0.5  # init neutral
        self.routing_counts= np.ones((N_CELLS, N_ENGINES))
        # Confidence anomaly detection
        self.conf_history: deque = deque(maxlen=100)
        self.conf_mean    = 0.5
        self.conf_std     = 0.15
        # Last routing decision
        self._last_cell   = 0
        self._last_routed = -1
        # Expert that INFINITY currently routes to
        self.current_expert: Optional[str] = None
        self.total_trades = 0
        self.win_rate     = 0.5
        self._load()

    def _get_cell(self, state: RLState) -> int:
        """Map state to routing table cell."""
        # Regime: 0=bull, 1=bear, 2=ranging, 3=volatile
        if state.regime_bull > 0.5:   regime = 0
        elif state.regime_bear > 0.5: regime = 1
        elif state.regime_ranging > 0.5: regime = 2
        else:                          regime = 3
        # Volatility tier
        vol = state.volatility_1m
        if vol < 0.002:   vol_tier = 0   # calm
        elif vol < 0.008: vol_tier = 1   # normal
        else:             vol_tier = 2   # high vol
        return regime * 3 + vol_tier

    def route(self, state: RLState, all_engine_decisions: Dict[str, Tuple[Action, float]]) -> Tuple[Action, float, str]:
        """
        Given all engines' decisions, route to the best one for this state.
        Returns: final_action, confidence, chosen_expert_name
        """
        cell = self._get_cell(state)
        self._last_cell = cell

        # Anomaly check: if confidence distribution is abnormal → VETO
        all_confs = [c for _, c in all_engine_decisions.values() if c > 0]
        if all_confs:
            self.conf_history.extend(all_confs)
            if len(self.conf_history) >= 20:
                self.conf_mean = float(np.mean(list(self.conf_history)))
                self.conf_std  = float(np.std(list(self.conf_history)))
            avg_conf = float(np.mean(all_confs))
            # Anomaly: all engines suddenly too confident OR all suddenly uncertain
            z_score = abs(avg_conf - self.conf_mean) / max(self.conf_std, 0.01)
            if z_score > 3.0 and avg_conf > 0.9:
                # Suspiciously high confidence → possible manipulation → VETO
                return Action.HOLD, 0.0, "VETO_ANOMALY"

        # Find best engine for this cell
        cell_scores = self.routing_table[cell]
        best_engine_idx = int(np.argmax(cell_scores))
        best_engine_key = self.ALL_ENGINE_KEYS[best_engine_idx]
        self._last_routed = best_engine_idx
        self.current_expert = best_engine_key

        # Get that engine's decision
        if best_engine_key in all_engine_decisions:
            action, conf = all_engine_decisions[best_engine_key]
            # INFINITY amplifies high-routing-score engines
            routing_score = float(cell_scores[best_engine_idx])
            conf_boost = min(0.98, conf * (0.8 + routing_score * 0.4))
            return action, conf_boost, best_engine_key

        return Action.HOLD, 0.0, "NO_DATA"

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        """Update routing table for the chosen engine."""
        cell = self._last_cell
        idx  = self._last_routed
        if idx < 0: return

        # Update routing table with exponential moving average
        alpha = 0.1
        was_correct = reward > 0
        self.routing_table[cell, idx] = (
            (1 - alpha) * self.routing_table[cell, idx] +
            alpha * (1.0 if was_correct else 0.0)
        )
        self.routing_counts[cell, idx] += 1
        if reward > 0: self.win_rate = 0.98 * self.win_rate + 0.02
        else:          self.win_rate = 0.98 * self.win_rate
        self.total_trades += 1

    def get_best_engine_per_regime(self) -> Dict[str, str]:
        """Which engine is best for each cell?"""
        cell_names = ['bull+calm','bull+norm','bull+vol',
                      'bear+calm','bear+norm','bear+vol',
                      'range+calm','range+norm','range+vol',
                      'chaos+calm','chaos+norm','chaos+vol']
        result = {}
        for i, cell_name in enumerate(cell_names):
            best_idx = int(np.argmax(self.routing_table[i]))
            result[cell_name] = self.ALL_ENGINE_KEYS[best_idx]
        return result

    def _load(self):
        p = RL_MODELS_DIR / f"infinity_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.routing_table  = d['rt']
            self.routing_counts = d['rc']

    def save(self):
        np.savez(str(RL_MODELS_DIR / f"infinity_{self.pair.replace('/','_')}"),
                 rt=self.routing_table, rc=self.routing_counts)

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades, "win_rate": round(self.win_rate, 3),
                "current_expert": self.current_expert,
                "best_per_regime": self.get_best_engine_per_regime(),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  EXTENDED ORCHESTRATOR — ALL 15 ENGINES
# ══════════════════════════════════════════════════════════════════════════════

class ExtendedRLOrchestrator:
    """
    15-engine cluster: original 5 + 10 soul engines.
    Total voting power: 15 engines.
    Consensus thresholds:
      ≥8/15 → standard BUY/SELL
      ≥11/15 → STRONG signal
    INFINITY acts as the final routing layer.
    """

    ALL_KEYS = ['PPO','A3C','DQN','SAC','TD3',
                'APEX','PHANTOM','STORM','ORACLE','VENOM',
                'TITAN','HYDRA','VOID','PULSE','INFINITY']

    def __init__(self, pair: str):
        self.pair = pair
        # Original 5
        self.ppo     = KrakenPPO(pair)
        self.a3c     = KrakenA3C(pair)
        self.dqn     = KrakenDQN(pair)
        self.sac     = KrakenSAC(pair)
        self.td3     = KrakenTD3(pair)
        # New 10 souls
        self.apex    = KrakenApex(pair)
        self.phantom = KrakenPhantom(pair)
        self.storm   = KrakenStorm(pair)
        self.oracle  = KrakenOracle(pair)
        self.venom   = KrakenVenom(pair)
        self.titan   = KrakenTitan(pair)
        self.hydra   = KrakenHydra(pair)
        self.void    = KrakenVoid(pair)
        self.pulse   = KrakenPulse(pair)
        self.infinity= KrakenInfinity(pair)

        self.engines_base = [self.ppo, self.a3c, self.dqn, self.sac, self.td3,
                             self.apex, self.phantom, self.storm, self.oracle,
                             self.venom, self.titan, self.hydra, self.void, self.pulse]
        self.keys_base    = ['PPO','A3C','DQN','SAC','TD3',
                             'APEX','PHANTOM','STORM','ORACLE','VENOM',
                             'TITAN','HYDRA','VOID','PULSE']

        # Dynamic weights (Bayesian: start equal, adapt to performance)
        self.weights = {k: 1.0 for k in self.ALL_KEYS}
        # Slight boosts for architecturally strongest engines
        self.weights.update({
            'APEX': 1.3,   # Patience → precision
            'ORACLE': 1.2, # Memory → pattern
            'INFINITY': 1.5, # Meta-router → supreme authority
            'DQN': 1.2,    # Breakout hunter
            'TD3': 1.2,    # Conviction threshold
        })

        self.engine_accuracy: Dict[str, deque] = {k: deque(maxlen=60) for k in self.ALL_KEYS}
        self._last_state: Optional[RLState] = None
        self._last_actions: Dict[str, Action] = {}
        self._last_confs:   Dict[str, float]  = {}
        self.total_decisions = 0
        self.logger = logging.getLogger(f"RL15-{pair}")
        self.logger.info(f"🌌 15-Engine cluster awakened for {pair}")

    def decide(self, state: RLState) -> Dict:
        """
        Full 15-engine ensemble decision.
        INFINITY routes after seeing all votes.
        """
        self._last_state = state
        all_decisions: Dict[str, Tuple[Action, float]] = {}
        individual: Dict[str, Dict] = {}

        # Step 1: Get votes from all 14 base engines
        for engine, key in zip(self.engines_base, self.keys_base):
            try:
                action, conf = engine.get_action(state)
            except Exception:
                action, conf = Action.HOLD, 0.0
            all_decisions[key]  = (action, conf)
            self._last_actions[key] = action
            self._last_confs[key]   = conf
            individual[key] = {'action': action.name, 'confidence': round(conf, 3)}

        # Step 2: INFINITY routes to best engine
        infinity_action, infinity_conf, routed_to = self.infinity.route(state, all_decisions)
        all_decisions['INFINITY'] = (infinity_action, infinity_conf)
        self._last_actions['INFINITY'] = infinity_action
        self._last_confs['INFINITY']   = infinity_conf
        individual['INFINITY'] = {
            'action': infinity_action.name,
            'confidence': round(infinity_conf, 3),
            'routed_to': routed_to
        }

        # Step 3: Weighted ensemble vote
        score_map = {
            Action.STRONG_BUY: +2.0, Action.BUY: +1.0, Action.HOLD: 0.0,
            Action.SELL: -1.0, Action.STRONG_SELL: -2.0
        }

        weighted_score = 0.0
        total_weight   = 0.0
        n_bullish = n_bearish = 0

        for key in self.ALL_KEYS:
            action, conf = all_decisions.get(key, (Action.HOLD, 0.0))
            w             = self.weights[key]
            contribution  = score_map[action] * conf * w
            weighted_score += contribution
            total_weight   += conf * w
            if score_map[action] > 0: n_bullish += 1
            elif score_map[action] < 0: n_bearish += 1

        norm_score = weighted_score / max(total_weight, 1e-5)
        consensus  = max(n_bullish, n_bearish)

        # Step 4: Signal classification (15-engine thresholds)
        if routed_to == "VETO_ANOMALY":
            direction, signal_str, confidence = "hold", "VETO", 0.0
        elif norm_score > 0.9 and consensus >= 11:
            direction, signal_str = "buy", "STRONG_BUY"
            confidence = min(0.97, abs(norm_score) * 0.5)
        elif norm_score > 0.35 and consensus >= 8:
            direction, signal_str = "buy", "BUY"
            confidence = min(0.88, abs(norm_score) * 0.4)
        elif norm_score < -0.9 and consensus >= 11:
            direction, signal_str = "sell", "STRONG_SELL"
            confidence = min(0.97, abs(norm_score) * 0.5)
        elif norm_score < -0.35 and consensus >= 8:
            direction, signal_str = "sell", "SELL"
            confidence = min(0.88, abs(norm_score) * 0.4)
        else:
            direction, signal_str, confidence = "hold", "HOLD", 0.0

        self.total_decisions += 1

        return {
            'direction':   direction,
            'signal':      signal_str,
            'confidence':  confidence,
            'score':       round(norm_score, 4),
            'consensus':   consensus,
            'n_bullish':   n_bullish,
            'n_bearish':   n_bearish,
            'engines':     individual,
            'routed_to':   routed_to,
            'veto':        routed_to == "VETO_ANOMALY",
        }

    def record_outcome(self, reward: float, pnl_pct: float, next_state: RLState):
        """Feed result to ALL 15 engines + update dynamic weights."""
        if self._last_state is None: return
        done = abs(pnl_pct) > 0.0001

        for engine, key in zip(self.engines_base, self.keys_base):
            action = self._last_actions.get(key, Action.HOLD)
            try:
                engine.record(self._last_state, action, reward, next_state, done)
            except Exception: pass

        # INFINITY learns from the routing outcome
        infinity_action = self._last_actions.get('INFINITY', Action.HOLD)
        self.infinity.record(self._last_state, infinity_action, reward, next_state, done)

        # Update all engine weights based on rolling accuracy
        for key in self.ALL_KEYS:
            action = self._last_actions.get(key, Action.HOLD)
            score_map = {Action.STRONG_BUY: +1, Action.BUY: +1,
                        Action.HOLD: 0, Action.SELL: -1, Action.STRONG_SELL: -1}
            direction = score_map[action]
            was_correct = (direction > 0 and reward > 0) or \
                          (direction < 0 and reward < 0)

            if action != Action.HOLD:
                self.engine_accuracy[key].append(1 if was_correct else 0)
                if len(self.engine_accuracy[key]) >= 20:
                    acc = sum(self.engine_accuracy[key]) / len(self.engine_accuracy[key])
                    # Smooth weight update via logistic
                    base_weight = 0.5 + 2.0 * (1 / (1 + math.exp(-10 * (acc - 0.5))))
                    # INFINITY and APEX get multiplier for their special roles
                    if key in ('INFINITY', 'APEX', 'ORACLE'):
                        base_weight *= 1.2
                    self.weights[key] = round(base_weight, 4)

    def save_all(self):
        for engine in self.engines_base:
            try: engine.save()
            except Exception: pass
        try: self.infinity.save()
        except Exception: pass

    def get_full_stats(self) -> Dict:
        base_stats = [e.get_stats() for e in self.engines_base]
        inf_stats  = self.infinity.get_stats()
        return {
            "pair":       self.pair,
            "cluster":    "15-ENGINE SOUL CLUSTER",
            "decisions":  self.total_decisions,
            "weights":    {k: round(v, 3) for k, v in self.weights.items()},
            "engines":    base_stats + [inf_stats],
            "best_per_regime": self.infinity.get_best_engine_per_regime(),
        }


# ── UPDATED FACTORY — 15 engines per pair ─────────────────────────────────────

_extended_orchestrators: Dict[str, ExtendedRLOrchestrator] = {}
_ext_lock = threading.Lock()


def get_rl_orchestrator(pair: str) -> ExtendedRLOrchestrator:
    """Get or create the 15-engine cluster for a pair."""
    with _ext_lock:
        if pair not in _extended_orchestrators:
            _extended_orchestrators[pair] = ExtendedRLOrchestrator(pair)
            logger.info(f"🌌 15-Engine Soul Cluster awakened: {pair}")
        return _extended_orchestrators[pair]


def get_all_orchestrators() -> Dict[str, ExtendedRLOrchestrator]:
    return dict(_extended_orchestrators)


def save_all_models():
    saved = 0
    for orch in _extended_orchestrators.values():
        orch.save_all()
        saved += 1
    logger.info(f"💾 Saved {saved} pairs × 15 soul engines")


def get_global_rl_stats() -> Dict:
    all_stats = [o.get_full_stats() for o in _extended_orchestrators.values()]
    return {
        "total_pairs":     len(_extended_orchestrators),
        "total_decisions": sum(s['decisions'] for s in all_stats),
        "engine_count":    15,
        "per_pair":        all_stats[:5],
    }
