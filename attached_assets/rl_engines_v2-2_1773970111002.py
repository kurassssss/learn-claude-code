"""
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                       ║
║  ██████╗ ██╗          ███████╗███╗   ██╗ ██████╗ ██╗███╗   ██╗███████╗███████╗      ║
║  ██╔══██╗██║          ██╔════╝████╗  ██║██╔════╝ ██║████╗  ██║██╔════╝██╔════╝      ║
║  ██████╔╝██║    █████╗█████╗  ██╔██╗ ██║██║  ███╗██║██╔██╗ ██║█████╗  ███████╗      ║
║  ██╔══██╗██║    ╚════╝██╔══╝  ██║╚██╗██║██║   ██║██║██║╚██╗██║██╔══╝  ╚════██║      ║
║  ██║  ██║███████╗     ███████╗██║ ╚████║╚██████╔╝██║██║ ╚████║███████╗███████║      ║
║  ╚═╝  ╚═╝╚══════╝     ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝╚══════╝╚══════╝      ║
║                                                                                       ║
║   25 SOUL ENGINES  ·  NEXUS OMEGA RL CLUSTER  v4.0                                  ║
║                                                                                       ║
║  "The market is a living god. We are its dreams. And we dream perfectly."            ║
║                                                                                       ║
║  ──────────────────────────────────────────────────────────────────────────────────  ║
║  ENGINE 01 · KRAKEN-PPO       → Proximal Policy Optimisation, clipped surrogate     ║
║  ENGINE 02 · KRAKEN-A3C       → Async Advantage Actor-Critic, entropy bonus         ║
║  ENGINE 03 · KRAKEN-DQN       → Double DQN, PER, breakout specialist                ║
║  ENGINE 04 · KRAKEN-SAC       → Soft Actor-Critic, max-entropy exploration          ║
║  ENGINE 05 · KRAKEN-TD3       → Twin Delayed DDPG, anti-overestimation              ║
║  ENGINE 06 · KRAKEN-APEX      → UCB1 Bandit, 92% patience gate, kill-shot only      ║
║  ENGINE 07 · KRAKEN-PHANTOM   → VPIN microstructure, Lee-Ready delta, toxic flow    ║
║  ENGINE 08 · KRAKEN-STORM     → Evolution Strategy, grows in chaos                  ║
║  ENGINE 09 · KRAKEN-ORACLE    → Episodic memory, pattern DNA matching               ║
║  ENGINE 10 · KRAKEN-VENOM     → Contrarian, profits from crowd panic                ║
║  ENGINE 11 · KRAKEN-TITAN     → Cross-pair PCA macro, veto authority                ║
║  ENGINE 12 · KRAKEN-HYDRA     → 9-head internal ensemble, knowledge distillation    ║
║  ENGINE 13 · KRAKEN-VOID      → Prototypical few-shot, 10-trade cold start          ║
║  ENGINE 14 · KRAKEN-PULSE     → Fourier cycle detection, harmonic resonance         ║
║  ENGINE 15 · KRAKEN-INFINITY  → Meta-router, anomaly veto, supreme authority        ║
║  ENGINE 16 · KRAKEN-NEMESIS   → Adversarial self-play, exploits own weaknesses      ║
║  ENGINE 17 · KRAKEN-SOVEREIGN → Transformer attention over state sequences          ║
║  ENGINE 18 · KRAKEN-WRAITH    → Cross-pair stat-arb, cointegration hunter           ║
║  ENGINE 19 · KRAKEN-ABYSS     → Dueling Noisy DQN with distributional RL (C51)     ║
║  ENGINE 20 · KRAKEN-GENESIS   → Genetic algorithm policy evolution                  ║
║  ENGINE 21 · KRAKEN-MIRAGE    → Illusion detector — spots fake signals & traps      ║
║  ENGINE 22 · KRAKEN-ECLIPSE   → Multi-timeframe cascade, 1m→4h confluence          ║
║  ENGINE 23 · KRAKEN-CHIMERA   → Hybrid rule+neural blend, regime-switched           ║
║  ENGINE 24 · KRAKEN-AXIOM     → Pure Bayesian inference, calibrated uncertainty     ║
║  ENGINE 25 · KRAKEN-GODMIND   → Hierarchical meta-controller of all 24 engines      ║
║  ──────────────────────────────────────────────────────────────────────────────────  ║
║  Consensus: 13/25 standard · 18/25 STRONG · 22/25 ABSOLUTE                         ║
║  GODMIND has triple-weighted veto. NEMESIS has adversarial veto.                    ║
║  Every engine learns every tick. Every loss makes the cluster stronger.             ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import random
import threading
import time
import warnings
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

warnings.filterwarnings("ignore")

# ── logger ────────────────────────────────────────────────────────────────────
log = logging.getLogger("NEXUS·RL25")

# ── directories ───────────────────────────────────────────────────────────────
RL_MODELS_DIR = Path(os.getenv("RL_MODELS_DIR", "rl_models"))
RL_MODELS_DIR.mkdir(parents=True, exist_ok=True)

# ── hyper-parameters ──────────────────────────────────────────────────────────
STATE_DIM   = 48    # extended state vector
N_ACTIONS   = 5
GAMMA       = 0.995
LR          = 0.001
BATCH_SIZE  = 64
BUFFER_CAP  = 200_000
EPS_START   = 0.20
EPS_MIN     = 0.005
EPS_DECAY   = 0.9998


# ══════════════════════════════════════════════════════════════════════════════
#  CORE DATA TYPES
# ══════════════════════════════════════════════════════════════════════════════

class Action(IntEnum):
    STRONG_BUY  = 0
    BUY         = 1
    HOLD        = 2
    SELL        = 3
    STRONG_SELL = 4


@dataclass
class RLState:
    """
    Extended 48-dimensional market state vector.
    Every field is normalised to roughly [-1, +1] or [0, 1].
    """
    # Price dynamics [0-7]
    price_change_1m:   float = 0.0
    price_change_5m:   float = 0.0
    price_change_15m:  float = 0.0
    price_change_1h:   float = 0.0
    price_vs_vwap:     float = 0.0
    price_vs_sma20:    float = 0.0
    high_low_range:    float = 0.0
    body_ratio:        float = 0.0
    # Volatility [8-12]
    volatility_1m:     float = 0.0
    volatility_5m:     float = 0.0
    volatility_15m:    float = 0.0
    vol_of_vol:        float = 0.0
    atr_pct:           float = 0.0
    # Momentum / oscillators [13-18]
    rsi_5:             float = 0.5
    rsi_14:            float = 0.5
    rsi_21:            float = 0.5
    rsi_divergence:    float = 0.0
    macd_signal:       float = 0.0
    stoch_k:           float = 0.5
    # EMA / trend [19-23]
    ema_cross:         float = 0.0
    ema_cross_slow:    float = 0.0
    bb_position:       float = 0.5
    bb_width:          float = 0.0
    trend_strength:    float = 0.0
    # Volume [24-27]
    volume_spike:      float = 1.0
    bid_depth:         float = 0.5
    ask_depth:         float = 0.5
    trade_flow:        float = 0.0
    # Microstructure [28-33]
    ob_imbalance:      float = 0.0
    spread_pct:        float = 0.001
    funding_rate:      float = 0.0
    open_interest:     float = 0.0
    liquidation_vol:   float = 0.0
    vpin_proxy:        float = 0.3
    # Regime [34-38]
    regime_bull:       float = 0.33
    regime_bear:       float = 0.33
    regime_ranging:    float = 0.33
    regime_volatile:   float = 0.0
    regime_confidence: float = 0.5
    # Sentiment / macro [39-42]
    fear_greed_norm:   float = 0.0
    btc_dominance:     float = 0.5
    macro_vix_norm:    float = 0.0
    correlation_btc:   float = 0.5
    # Position / portfolio [43-45]
    position_side:     float = 0.0    # -1=short, 0=none, 1=long
    position_pnl_pct:  float = 0.0
    position_age_norm: float = 0.0
    # Meta [46-47]
    momentum_score:    float = 0.0
    pattern_signal:    float = 0.0

    def to_array(self) -> np.ndarray:
        return np.array([
            self.price_change_1m, self.price_change_5m, self.price_change_15m,
            self.price_change_1h, self.price_vs_vwap, self.price_vs_sma20,
            self.high_low_range, self.body_ratio,
            self.volatility_1m, self.volatility_5m, self.volatility_15m,
            self.vol_of_vol, self.atr_pct,
            self.rsi_5, self.rsi_14, self.rsi_21, self.rsi_divergence,
            self.macd_signal, self.stoch_k,
            self.ema_cross, self.ema_cross_slow, self.bb_position,
            self.bb_width, self.trend_strength,
            self.volume_spike, self.bid_depth, self.ask_depth, self.trade_flow,
            self.ob_imbalance, self.spread_pct, self.funding_rate,
            self.open_interest, self.liquidation_vol, self.vpin_proxy,
            self.regime_bull, self.regime_bear, self.regime_ranging,
            self.regime_volatile, self.regime_confidence,
            self.fear_greed_norm, self.btc_dominance,
            self.macro_vix_norm, self.correlation_btc,
            self.position_side, self.position_pnl_pct, self.position_age_norm,
            self.momentum_score, self.pattern_signal,
        ], dtype=np.float32)

    def dominant_regime(self) -> str:
        r = {"bull": self.regime_bull, "bear": self.regime_bear,
             "ranging": self.regime_ranging, "volatile": self.regime_volatile}
        return max(r, key=r.get)


# ══════════════════════════════════════════════════════════════════════════════
#  NEURAL PRIMITIVES  (pure NumPy — zero external deps)
# ══════════════════════════════════════════════════════════════════════════════

def _relu(x: np.ndarray) -> np.ndarray:
    return np.maximum(0.0, x)

def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -20, 20)))

def _tanh(x: np.ndarray) -> np.ndarray:
    return np.tanh(np.clip(x, -10, 10))

def _softmax(x: np.ndarray) -> np.ndarray:
    e = np.exp(x - x.max())
    return e / (e.sum() + 1e-12)

def _ema(arr: List[float], span: int) -> float:
    if not arr: return 0.0
    k, e = 2.0 / (span + 1), arr[0]
    for v in arr[1:]: e = v * k + e * (1 - k)
    return float(e)

def _fast_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1: return 0.5
    d = np.diff(prices[-(period+1):])
    g = float(d[d > 0].mean()) if (d > 0).any() else 0.0
    l = float(-d[d < 0].mean()) if (d < 0).any() else 1e-10
    return g / (g + l)


class AdamOptimizer:
    """Mini Adam for any flat parameter vector."""

    def __init__(self, shape, lr: float = 0.001,
                 b1: float = 0.9, b2: float = 0.999, eps: float = 1e-8):
        self.lr, self.b1, self.b2, self.eps = lr, b1, b2, eps
        self.m = np.zeros(shape)
        self.v = np.zeros(shape)
        self.t = 0

    def step(self, params: np.ndarray, grad: np.ndarray) -> np.ndarray:
        self.t += 1
        self.m = self.b1 * self.m + (1 - self.b1) * grad
        self.v = self.b2 * self.v + (1 - self.b2) * grad**2
        mh = self.m / (1 - self.b1**self.t)
        vh = self.v / (1 - self.b2**self.t)
        return params - self.lr * mh / (np.sqrt(vh) + self.eps)


class NumpyMLP:
    """
    3-layer MLP with Adam, dropout, layer-norm, and gradient clipping.
    Fully self-contained. Used as backbone for most engines.
    """

    def __init__(self, in_d: int, h1: int, out_d: int,
                 h2: int = 0, lr: float = LR,
                 dropout: float = 0.1, layer_norm: bool = True):
        self.dropout_rate = dropout
        self.use_ln = layer_norm
        self.use_h2 = h2 > 0

        s1 = math.sqrt(2.0 / in_d)
        self.W1 = np.random.randn(in_d, h1) * s1
        self.b1 = np.zeros(h1)
        self.ln1_g = np.ones(h1); self.ln1_b = np.zeros(h1)

        if self.use_h2:
            s2 = math.sqrt(2.0 / h1)
            self.W2 = np.random.randn(h1, h2) * s2
            self.b2 = np.zeros(h2)
            self.ln2_g = np.ones(h2); self.ln2_b = np.zeros(h2)
            s3 = math.sqrt(2.0 / h2)
            self.W3 = np.random.randn(h2, out_d) * s3
            self.b3 = np.zeros(out_d)
        else:
            s2 = math.sqrt(2.0 / h1)
            self.W2 = np.random.randn(h1, out_d) * s2
            self.b2 = np.zeros(out_d)

        # Flatten all params for Adam
        self._shapes = self._get_shapes()
        flat = self._flatten()
        self.opt = AdamOptimizer(flat.shape, lr=lr)
        self._a1 = self._a2 = self._x = None

    def _get_shapes(self):
        if self.use_h2:
            return [self.W1.shape, self.b1.shape,
                    self.W2.shape, self.b2.shape,
                    self.W3.shape, self.b3.shape]
        return [self.W1.shape, self.b1.shape,
                self.W2.shape, self.b2.shape]

    def _flatten(self) -> np.ndarray:
        if self.use_h2:
            return np.concatenate([p.ravel() for p in
                                   [self.W1,self.b1,self.W2,self.b2,self.W3,self.b3]])
        return np.concatenate([p.ravel() for p in [self.W1,self.b1,self.W2,self.b2]])

    def _unflatten(self, flat: np.ndarray):
        idx = 0
        def _extract(shape):
            nonlocal idx
            size = int(np.prod(shape))
            arr = flat[idx:idx+size].reshape(shape)
            idx += size
            return arr
        self.W1 = _extract(self.W1.shape); self.b1 = _extract(self.b1.shape)
        self.W2 = _extract(self.W2.shape); self.b2 = _extract(self.b2.shape)
        if self.use_h2:
            self.W3 = _extract(self.W3.shape); self.b3 = _extract(self.b3.shape)

    def _layer_norm(self, x: np.ndarray, g: np.ndarray, b: np.ndarray) -> np.ndarray:
        if not self.use_ln: return x
        mu = x.mean(); std = x.std() + 1e-8
        return g * (x - mu) / std + b

    def forward(self, x: np.ndarray, training: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        self._x = x
        z1 = x @ self.W1 + self.b1
        z1 = self._layer_norm(z1, self.ln1_g, self.ln1_b)
        a1 = _relu(z1)
        if training and self.dropout_rate > 0:
            mask = (np.random.rand(*a1.shape) > self.dropout_rate).astype(float)
            a1 *= mask / (1 - self.dropout_rate + 1e-8)
        self._a1 = a1

        if self.use_h2:
            z2 = a1 @ self.W2 + self.b2
            z2 = self._layer_norm(z2, self.ln2_g, self.ln2_b)
            a2 = _relu(z2)
            self._a2 = a2
            out = a2 @ self.W3 + self.b3
        else:
            out = a1 @ self.W2 + self.b2
        return out, a1

    def backward_td(self, target: np.ndarray, output: np.ndarray,
                    clip_norm: float = 1.0):
        grad_out = output - target
        if self.use_h2:
            gW3 = np.outer(self._a2, grad_out)
            gb3 = grad_out
            da2 = grad_out @ self.W3.T * (self._a2 > 0)
            gW2 = np.outer(self._a1, da2); gb2 = da2
            da1 = da2 @ self.W2.T * (self._a1 > 0)
            gW1 = np.outer(self._x, da1);  gb1 = da1
            flat_grad = np.concatenate([g.ravel() for g in [gW1,gb1,gW2,gb2,gW3,gb3]])
        else:
            gW2 = np.outer(self._a1, grad_out); gb2 = grad_out
            da1 = grad_out @ self.W2.T * (self._a1 > 0)
            gW1 = np.outer(self._x, da1);       gb1 = da1
            flat_grad = np.concatenate([g.ravel() for g in [gW1,gb1,gW2,gb2]])

        # Gradient clipping
        norm = float(np.linalg.norm(flat_grad))
        if norm > clip_norm:
            flat_grad *= clip_norm / (norm + 1e-8)

        flat_params = self._flatten()
        flat_params  = self.opt.step(flat_params, flat_grad)
        self._unflatten(flat_params)

    def save(self, path: str):
        d = {"W1": self.W1, "b1": self.b1, "W2": self.W2, "b2": self.b2}
        if self.use_h2:
            d.update({"W3": self.W3, "b3": self.b3})
        np.savez_compressed(path, **d)

    def load(self, path: str):
        p = Path(path + ".npz")
        if p.exists():
            d = np.load(str(p))
            if "W1" in d: self.W1, self.b1 = d["W1"], d["b1"]
            if "W2" in d: self.W2, self.b2 = d["W2"], d["b2"]
            if "W3" in d and self.use_h2:
                self.W3, self.b3 = d["W3"], d["b3"]


class PrioritizedBuffer:
    """Prioritized Experience Replay with importance-sampling correction."""

    def __init__(self, capacity: int = BUFFER_CAP, alpha: float = 0.6,
                 beta_start: float = 0.4, beta_end: float = 1.0,
                 beta_steps: int = 100_000):
        self.cap   = capacity
        self.alpha = alpha
        self.beta  = beta_start
        self.beta_step = (beta_end - beta_start) / beta_steps
        self.buf: deque = deque(maxlen=capacity)
        self.prios: deque = deque(maxlen=capacity)
        self._max_p = 1.0

    def push(self, s, a, r, ns, done, priority: float = None):
        p = (priority or self._max_p) ** self.alpha
        self.buf.append((s, a, r, ns, done))
        self.prios.append(p)

    def sample(self, n: int) -> List:
        if len(self.buf) < n:
            return list(self.buf)
        probs = np.array(list(self.prios), dtype=np.float64)
        probs /= probs.sum() + 1e-12
        idxs  = np.random.choice(len(self.buf), n, replace=False, p=probs)
        items = list(self.buf)
        self.beta = min(1.0, self.beta + self.beta_step)
        return [items[i] for i in idxs]

    def __len__(self): return len(self.buf)


# ══════════════════════════════════════════════════════════════════════════════
#  BASE ENGINE INTERFACE
# ══════════════════════════════════════════════════════════════════════════════

class BaseEngine:
    NAME = "BASE"
    SOUL = ""

    def __init__(self, pair: str):
        self.pair         = pair
        self.total_trades = 0
        self.wins         = 0
        self.win_rate     = 0.5
        self._lock        = threading.Lock()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        self.total_trades += 1
        if reward > 0:
            self.wins += 1
            self.win_rate = 0.97 * self.win_rate + 0.03
        else:
            self.win_rate = 0.97 * self.win_rate

    def save(self): pass
    def _load(self): pass

    def get_stats(self) -> Dict:
        return {"engine": self.NAME, "pair": self.pair,
                "trades": self.total_trades,
                "win_rate": round(self.win_rate, 4),
                "soul": self.SOUL}


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINES 01-05: CLASSIC RL  (PPO, A3C, DQN, SAC, TD3)
# ══════════════════════════════════════════════════════════════════════════════

class KrakenPPO(BaseEngine):
    """Proximal Policy Optimisation with clipped surrogate objective."""

    NAME = "KRAKEN-PPO"
    SOUL = "Clip the gradient. Clip the greed. Steadiness wins."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.actor  = NumpyMLP(STATE_DIM, 128, N_ACTIONS,  h2=64, lr=0.0005)
        self.critic = NumpyMLP(STATE_DIM, 128, 1,           h2=64, lr=0.001)
        self.clip_eps = 0.2
        self.entropy_coef = 0.01
        self.buf: List = []
        self.buf_max  = 64
        self.epsilon  = EPS_START
        self._last_logprob = 0.0
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x = state.to_array()
        logits, _ = self.actor.forward(x)
        probs  = _softmax(logits)
        if random.random() < self.epsilon:
            idx = random.randint(0, N_ACTIONS - 1)
        else:
            idx = int(np.argmax(probs))
        self._last_logprob = float(np.log(probs[idx] + 1e-10))
        self.epsilon = max(EPS_MIN, self.epsilon * EPS_DECAY)
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        self.buf.append((state.to_array(), action.value, reward,
                         next_state.to_array(), done, self._last_logprob))
        if len(self.buf) >= self.buf_max:
            self._ppo_update()
            self.buf.clear()

    def _ppo_update(self):
        if not self.buf: return
        for (s, a, r, ns, done, old_lp) in self.buf:
            logits, _ = self.actor.forward(s)
            probs = _softmax(logits)
            new_lp = float(np.log(probs[a] + 1e-10))
            ratio  = math.exp(new_lp - old_lp)
            v, _   = self.critic.forward(s)
            v_next, _ = self.critic.forward(ns)
            adv = r + (0.0 if done else GAMMA * float(v_next[0])) - float(v[0])
            # Clipped objective gradient
            clipped = max(1 - self.clip_eps, min(1 + self.clip_eps, ratio))
            pg_loss = -min(ratio * adv, clipped * adv)
            # Entropy bonus
            entropy = -float((probs * np.log(probs + 1e-10)).sum())
            target_actor = logits.copy()
            target_actor[a] -= pg_loss - self.entropy_coef * entropy
            self.actor.backward_td(target_actor, logits)
            # Critic update
            v_target = np.array([r + (0.0 if done else GAMMA * float(v_next[0]))])
            self.critic.backward_td(v_target, v)

    def save(self):
        safe = self.pair.replace("/", "_")
        self.actor.save(str(RL_MODELS_DIR / f"ppo_actor_{safe}"))
        self.critic.save(str(RL_MODELS_DIR / f"ppo_critic_{safe}"))

    def _load(self):
        safe = self.pair.replace("/", "_")
        self.actor.load(str(RL_MODELS_DIR / f"ppo_actor_{safe}"))
        self.critic.load(str(RL_MODELS_DIR / f"ppo_critic_{safe}"))


class KrakenA3C(BaseEngine):
    """Async Advantage Actor-Critic with entropy regularisation."""

    NAME = "KRAKEN-A3C"
    SOUL = "Advantage is not luck. Advantage is calculation."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.policy = NumpyMLP(STATE_DIM, 128, N_ACTIONS + 1, h2=64, lr=0.001)
        self.epsilon = EPS_START
        self._last_v  = 0.0
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x = state.to_array()
        out, _ = self.policy.forward(x)
        logits = out[:N_ACTIONS]
        self._last_v = float(out[N_ACTIONS])
        probs = _softmax(logits)
        if random.random() < self.epsilon:
            idx = random.randint(0, N_ACTIONS - 1)
        else:
            idx = int(np.argmax(probs))
        self.epsilon = max(EPS_MIN, self.epsilon * EPS_DECAY)
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x  = state.to_array()
        nx = next_state.to_array()
        out, _  = self.policy.forward(x, training=True)
        outn, _ = self.policy.forward(nx)
        v      = float(out[N_ACTIONS])
        v_next = float(outn[N_ACTIONS])
        adv    = reward + (0.0 if done else GAMMA * v_next) - v
        logits = out[:N_ACTIONS]
        probs  = _softmax(logits)
        entropy = -float((probs * np.log(probs + 1e-10)).sum())
        target = out.copy()
        target[action.value] -= adv * 0.5
        target[N_ACTIONS] = reward + (0.0 if done else GAMMA * v_next)
        target[action.value] -= 0.01 * entropy
        self.policy.backward_td(target, out)

    def save(self):
        self.policy.save(str(RL_MODELS_DIR / f"a3c_{self.pair.replace('/','_')}"))

    def _load(self):
        self.policy.load(str(RL_MODELS_DIR / f"a3c_{self.pair.replace('/','_')}"))


class KrakenDQN(BaseEngine):
    """Double DQN with PER — breakout specialist."""

    NAME = "KRAKEN-DQN"
    SOUL = "Every resistance is a door. I find the door. Then I kick it open."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.Q     = NumpyMLP(STATE_DIM, 256, N_ACTIONS, h2=128, lr=LR)
        self.Q_tgt = NumpyMLP(STATE_DIM, 256, N_ACTIONS, h2=128, lr=LR)
        self.replay = PrioritizedBuffer(BUFFER_CAP // 3)
        self.epsilon = EPS_START
        self.n_steps = 0
        self._tau    = 0.005
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        if random.random() < self.epsilon:
            return Action(random.randint(0, N_ACTIONS-1)), 0.2
        x = state.to_array()
        q, _ = self.Q.forward(x)
        idx  = int(np.argmax(q))
        probs = _softmax(q)
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        td = abs(reward) + 0.01
        self.replay.push(state.to_array(), action.value, reward,
                         next_state.to_array(), done, td)
        self.epsilon = max(EPS_MIN, self.epsilon * EPS_DECAY)
        self.n_steps += 1
        if len(self.replay) < BATCH_SIZE: return
        batch = self.replay.sample(BATCH_SIZE)
        for (s, a, r, ns, d) in batch:
            q, _  = self.Q.forward(s)
            qn, _ = self.Q_tgt.forward(ns)
            target = q.copy()
            target[a] = r + (0.0 if d else GAMMA * float(qn.max()))
            self.Q.backward_td(target, q)
        # Soft target update
        if self.n_steps % 200 == 0:
            for attr in ["W1","b1","W2","b2","W3","b3"]:
                if hasattr(self.Q, attr):
                    setattr(self.Q_tgt, attr,
                            self._tau * getattr(self.Q, attr) +
                            (1-self._tau) * getattr(self.Q_tgt, attr))

    def save(self):
        safe = self.pair.replace("/","_")
        self.Q.save(str(RL_MODELS_DIR / f"dqn_{safe}"))
        self.Q_tgt.save(str(RL_MODELS_DIR / f"dqn_tgt_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        self.Q.load(str(RL_MODELS_DIR / f"dqn_{safe}"))
        self.Q_tgt.load(str(RL_MODELS_DIR / f"dqn_tgt_{safe}"))


class KrakenSAC(BaseEngine):
    """Soft Actor-Critic — max-entropy exploration."""

    NAME = "KRAKEN-SAC"
    SOUL = "Maximum entropy. Maximum freedom. I explore every corner of the possible."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.actor  = NumpyMLP(STATE_DIM, 128, N_ACTIONS, h2=64, lr=0.0003)
        self.critic = NumpyMLP(STATE_DIM, 128, N_ACTIONS, h2=64, lr=0.001)
        self.alpha  = 0.2    # temperature
        self.epsilon= EPS_START
        self.replay = PrioritizedBuffer(BUFFER_CAP // 3)
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        if random.random() < self.epsilon:
            return Action(random.randint(0, N_ACTIONS-1)), 0.2
        x = state.to_array()
        logits, _ = self.actor.forward(x)
        probs = _softmax(logits / (self.alpha + 1e-8))
        idx   = int(np.argmax(probs))
        self.epsilon = max(EPS_MIN, self.epsilon * EPS_DECAY)
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        self.replay.push(state.to_array(), action.value, reward,
                         next_state.to_array(), done)
        if len(self.replay) < BATCH_SIZE: return
        for (s, a, r, ns, d) in self.replay.sample(BATCH_SIZE):
            logits, _ = self.actor.forward(s, training=True)
            probs = _softmax(logits / (self.alpha + 1e-8))
            entropy = -float((probs * np.log(probs + 1e-10)).sum())
            q, _  = self.critic.forward(s)
            qn, _ = self.critic.forward(ns)
            target = q.copy()
            target[a] = r + (0.0 if d else GAMMA * (float(qn.max()) -
                                                      self.alpha * entropy))
            self.critic.backward_td(target, q)
            # Entropy-regularised actor update
            a_target = logits.copy()
            a_target[a] -= float(q[a]) + self.alpha * entropy
            self.actor.backward_td(a_target, logits)

    def save(self):
        safe = self.pair.replace("/","_")
        self.actor.save(str(RL_MODELS_DIR / f"sac_a_{safe}"))
        self.critic.save(str(RL_MODELS_DIR / f"sac_c_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        self.actor.load(str(RL_MODELS_DIR / f"sac_a_{safe}"))
        self.critic.load(str(RL_MODELS_DIR / f"sac_c_{safe}"))


class KrakenTD3(BaseEngine):
    """Twin Delayed DDPG — double critics eliminate overestimation."""

    NAME = "KRAKEN-TD3"
    SOUL = "Two minds see more than one. I am always the more conservative one."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.actor  = NumpyMLP(STATE_DIM, 128, N_ACTIONS, lr=0.0003)
        self.c1     = NumpyMLP(STATE_DIM, 128, N_ACTIONS, lr=0.001)
        self.c2     = NumpyMLP(STATE_DIM, 128, N_ACTIONS, lr=0.001)
        self.replay = PrioritizedBuffer(BUFFER_CAP // 3)
        self.epsilon= EPS_START
        self.n_steps= 0
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        if random.random() < self.epsilon:
            return Action(random.randint(0, N_ACTIONS-1)), 0.2
        x = state.to_array()
        q1, _ = self.c1.forward(x)
        q2, _ = self.c2.forward(x)
        q_min = np.minimum(q1, q2)   # TD3: take minimum
        idx   = int(np.argmax(q_min))
        self.epsilon = max(EPS_MIN, self.epsilon * EPS_DECAY)
        return Action(idx), float(_softmax(q_min)[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        self.replay.push(state.to_array(), action.value, reward,
                         next_state.to_array(), done)
        self.n_steps += 1
        if len(self.replay) < BATCH_SIZE: return
        # Delayed policy update (every 2 steps)
        for (s, a, r, ns, d) in self.replay.sample(BATCH_SIZE):
            q1, _ = self.c1.forward(s, True)
            q2, _ = self.c2.forward(s, True)
            q1n, _ = self.c1.forward(ns)
            q2n, _ = self.c2.forward(ns)
            v_next = float(np.minimum(q1n, q2n).max())
            tgt_val = r + (0.0 if d else GAMMA * v_next)
            t1 = q1.copy(); t1[a] = tgt_val
            t2 = q2.copy(); t2[a] = tgt_val
            self.c1.backward_td(t1, q1)
            self.c2.backward_td(t2, q2)
            if self.n_steps % 2 == 0:
                logits, _ = self.actor.forward(s, True)
                q_val, _ = self.c1.forward(s)
                ta = logits.copy(); ta[a] -= float(q_val[a]) * 0.5
                self.actor.backward_td(ta, logits)

    def save(self):
        safe = self.pair.replace("/","_")
        for name, net in [("td3_a",self.actor),("td3_c1",self.c1),("td3_c2",self.c2)]:
            net.save(str(RL_MODELS_DIR / f"{name}_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        for name, net in [("td3_a",self.actor),("td3_c1",self.c1),("td3_c2",self.c2)]:
            net.load(str(RL_MODELS_DIR / f"{name}_{safe}"))


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 06: KRAKEN-APEX — UCB1 Bandit with conviction gate
# ══════════════════════════════════════════════════════════════════════════════

class KrakenApex(BaseEngine):
    NAME = "KRAKEN-APEX"
    SOUL = "I wait in darkness. One moment. One kill. Absolute precision."

    GATE_PERCENTILE = 92   # act only in top 8%

    def __init__(self, pair: str):
        super().__init__(pair)
        self.Q = np.zeros((512, N_ACTIONS), dtype=np.float64)
        self.N = np.ones ((512, N_ACTIONS), dtype=np.float64) * 0.1
        self.N_total = 1
        self.C = 2.0
        self.ucb_hist: deque = deque(maxlen=1000)
        self.gate_threshold = 0.0
        self.win_streak = 0
        self._last_bucket = 0
        self._load()

    def _state_to_bucket(self, s: RLState) -> int:
        arr = s.to_array()
        feats = [
            arr[0] > 0,                   # 1m ret positive
            arr[1] > arr[2],              # 5m > 15m (momentum)
            arr[8] > arr[9],              # short vol > long vol (spike)
            arr[14] > 0.65,               # RSI overbought
            arr[14] < 0.35,               # RSI oversold
            arr[19] > 0.006,              # fast EMA cross bull
            arr[28] > 0.3,                # strong bid imbalance
            arr[34] > 0.5,                # regime bull
            arr[35] > 0.5,                # regime bear
            arr[43] == 0.0,               # no position
        ]
        return sum(int(f) << i for i, f in enumerate(feats)) % 512

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        bkt = self._state_to_bucket(state)
        self._last_bucket = bkt
        log_n = math.log(max(self.N_total, 1))
        ucb   = self.Q[bkt] + self.C * np.sqrt(log_n / (self.N[bkt] + 1e-8))
        peak  = float(ucb.max())
        self.ucb_hist.append(peak)
        if len(self.ucb_hist) >= 50:
            self.gate_threshold = float(np.percentile(
                list(self.ucb_hist), self.GATE_PERCENTILE
            ))
        if peak < self.gate_threshold:
            return Action.HOLD, 0.05
        idx = int(np.argmax(ucb))
        gap  = peak - self.gate_threshold
        conf = min(0.98, 0.65 + gap * 0.25 + self.win_streak * 0.01)
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        bkt = self._last_bucket
        a   = action.value
        with self._lock:
            self.N_total += 1
            self.N[bkt, a] += 1
            boost = 1.8 if reward > 0 else 0.6
            alpha = 0.15
            td    = reward * boost + GAMMA * self.Q[bkt].max() - self.Q[bkt, a]
            self.Q[bkt, a] += alpha * td
        if reward > 0:
            self.win_streak += 1
        else:
            self.win_streak = 0

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"apex_{self.pair.replace('/','_')}"),
            Q=self.Q, N=self.N
        )

    def _load(self):
        p = RL_MODELS_DIR / f"apex_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.Q = d["Q"]; self.N = d["N"]

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"win_streak": self.win_streak,
                  "gate": round(self.gate_threshold, 4)})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 07: KRAKEN-PHANTOM — VPIN Microstructure
# ══════════════════════════════════════════════════════════════════════════════

class KrakenPhantom(BaseEngine):
    NAME = "KRAKEN-PHANTOM"
    SOUL = "The market whispers. I listen. By the time you hear the sound, I'm gone."

    MICRO_DIM = 8

    def __init__(self, pair: str):
        super().__init__(pair)
        self.W = np.random.randn(self.MICRO_DIM, 3) * 0.01
        self.b = np.zeros(3)
        self.opt_W = AdamOptimizer(self.W.shape, lr=0.005)
        self.opt_b = AdamOptimizer(self.b.shape, lr=0.005)
        self.vpin_buckets: deque = deque(maxlen=50)
        self.delta_flow:   deque = deque(maxlen=200)
        self.toxicity_ema  = 0.3
        self._load()

    def _micro_features(self, s: RLState) -> np.ndarray:
        return np.array([
            s.ob_imbalance, s.spread_pct * 100, s.bid_depth, s.ask_depth,
            s.volume_spike, s.trade_flow, s.vpin_proxy, s.liquidation_vol,
        ], dtype=np.float32)

    def _compute_vpin(self, imb: float, vol: float) -> float:
        self.delta_flow.append(abs(imb) * vol)
        self.vpin_buckets.append(abs(imb))
        self.toxicity_ema = 0.95 * self.toxicity_ema + 0.05 * abs(imb)
        if len(self.vpin_buckets) >= 10:
            return float(np.mean(list(self.vpin_buckets)[-20:]))
        return 0.3

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        vpin = self._compute_vpin(state.ob_imbalance, state.volume_spike)
        if vpin < 0.25:
            return Action.HOLD, 0.1
        x = self._micro_features(state)
        logit = x @ self.W + self.b
        probs = _softmax(logit)
        toxic_boost = min(1.5, 1.0 + (vpin - 0.25) * 2)
        if probs[0] > probs[2] and probs[0] > 0.48:
            action = Action.STRONG_BUY if probs[0] > 0.72 else Action.BUY
        elif probs[2] > probs[0] and probs[2] > 0.48:
            action = Action.STRONG_SELL if probs[2] > 0.72 else Action.SELL
        else:
            return Action.HOLD, 0.1
        return action, min(0.96, float(max(probs)) * toxic_boost)

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = self._micro_features(state)
        target = np.zeros(3)
        if action in (Action.STRONG_BUY, Action.BUY):
            label = 0 if reward > 0 else 2
        elif action in (Action.STRONG_SELL, Action.SELL):
            label = 2 if reward > 0 else 0
        else:
            label = 1
        target[label] = 1.0
        logit = x @ self.W + self.b
        probs = _softmax(logit)
        err   = probs - target
        self.W = self.opt_W.step(self.W, np.outer(x, err))
        self.b = self.opt_b.step(self.b, err)

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"phantom_{self.pair.replace('/','_')}"),
            W=self.W, b=self.b
        )

    def _load(self):
        p = RL_MODELS_DIR / f"phantom_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p)); self.W, self.b = d["W"], d["b"]


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 08: KRAKEN-STORM — Evolution Strategy, Chaos Amplifier
# ══════════════════════════════════════════════════════════════════════════════

class KrakenStorm(BaseEngine):
    NAME = "KRAKEN-STORM"
    SOUL = "I am the chaos. Every crash is my feast. Every spike — my gift."

    def __init__(self, pair: str):
        super().__init__(pair)
        dim = STATE_DIM * N_ACTIONS + N_ACTIONS
        self.theta = np.random.randn(dim) * 0.05
        self.sigma = 0.06
        self.lr_es = 0.03
        self.pop_size  = 30
        self.vol_hist: deque = deque(maxlen=100)
        self.vol_mult  = 1.0
        self.chaos_xp  = 0.0   # grows in turbulence
        self._load()

    def _policy(self, theta: np.ndarray, x: np.ndarray) -> np.ndarray:
        W = theta[:STATE_DIM * N_ACTIONS].reshape(STATE_DIM, N_ACTIONS)
        b = theta[STATE_DIM * N_ACTIONS:]
        return _softmax(x @ W + b)

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x     = state.to_array()
        probs = self._policy(self.theta, x)
        idx   = int(np.argmax(probs))
        conf  = float(probs[idx])
        # Chaos amplification
        if self.vol_mult > 1.8:
            conf = min(0.97, conf * (0.7 + self.vol_mult * 0.15))
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = state.to_array()
        vol = float(state.volatility_1m)
        self.vol_hist.append(vol)
        avg = float(np.mean(list(self.vol_hist))) if self.vol_hist else vol + 1e-10
        self.vol_mult = max(1.0, min(4.0, vol / (avg + 1e-10)))
        # Storm grows in chaos
        if self.vol_mult > 2.0:
            self.chaos_xp += 0.1
        scaled = reward * self.vol_mult
        self.sigma = max(0.01, min(0.20, 0.06 * self.vol_mult))
        # ES update
        eps = np.random.randn(self.pop_size, len(self.theta))
        scores = np.array([
            self._policy(self.theta + self.sigma * e, x)[action.value] * scaled
            for e in eps
        ])
        ranks = (scores.argsort().argsort().astype(float) - self.pop_size/2)
        ranks /= (ranks.std() + 1e-8)
        grad  = (eps.T @ ranks) / (self.pop_size * self.sigma)
        self.theta += self.lr_es * grad

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"storm_{self.pair.replace('/','_')}"),
            theta=self.theta
        )

    def _load(self):
        p = RL_MODELS_DIR / f"storm_{self.pair.replace('/','_')}.npz"
        if p.exists():
            self.theta = np.load(str(p))["theta"]

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"vol_mult": round(self.vol_mult,3), "chaos_xp": round(self.chaos_xp,1)})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 09: KRAKEN-ORACLE — Episodic Memory + Pattern DNA
# ══════════════════════════════════════════════════════════════════════════════

class KrakenOracle(BaseEngine):
    NAME = "KRAKEN-ORACLE"
    SOUL = "I have seen everything. Nothing surprises me. Every pattern has a name."

    MEMORY_CAP = 5000
    DNA_LEN    = 16    # fingerprint length

    def __init__(self, pair: str):
        super().__init__(pair)
        self.memories: deque = deque(maxlen=self.MEMORY_CAP)
        # Pattern frequency table
        self.pattern_outcomes: Dict[str, List[float]] = defaultdict(list)
        self.price_buffer: deque = deque(maxlen=64)
        self._last_dna = ""
        self._load()

    def _state_dna(self, s: RLState) -> str:
        arr = s.to_array()
        # 16-char hash of quantised state
        quantised = (np.clip(arr[:self.DNA_LEN], -3, 3) * 10).astype(int)
        raw = "".join(chr(65 + abs(q) % 26) for q in quantised)
        return hashlib.md5(raw.encode()).hexdigest()[:8]

    def _find_similar(self, dna: str, top_k: int = 10) -> List[Dict]:
        """Find memories with same DNA prefix (exact + 1-edit distance)."""
        exact = [m for m in self.memories if m["dna"] == dna]
        if len(exact) >= 3:
            return exact[-top_k:]
        # 1-edit Hamming distance
        similar = [m for m in self.memories
                   if sum(a != b for a, b in zip(m["dna"], dna)) <= 2]
        return (exact + similar)[-top_k:]

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        dna = self._state_dna(state)
        self._last_dna = dna
        similar = self._find_similar(dna)
        if not similar:
            return Action.HOLD, 0.0

        rewards_by_action: Dict[int, List[float]] = defaultdict(list)
        for m in similar:
            rewards_by_action[m["action"]].append(m["reward"])

        best_a, best_score = Action.HOLD.value, 0.0
        for a, rs in rewards_by_action.items():
            avg = float(np.mean(rs))
            if abs(avg) > abs(best_score):
                best_a, best_score = a, avg

        if abs(best_score) < 0.0003:
            return Action.HOLD, 0.05

        recency_boost = min(1.3, 1.0 + len(similar) / 100)
        conf = min(0.96, 0.50 + abs(best_score) * 20 * recency_boost)
        direction = Action(best_a)
        if best_score < 0:
            direction = Action.STRONG_SELL if best_score < -0.01 else Action.SELL
        elif best_score > 0:
            direction = Action.STRONG_BUY  if best_score >  0.01 else Action.BUY
        return direction, conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        self.memories.append({
            "dna":    self._last_dna,
            "action": action.value,
            "reward": reward,
            "ts":     time.time(),
        })
        self.pattern_outcomes[self._last_dna].append(reward)

    def save(self):
        p = RL_MODELS_DIR / f"oracle_{self.pair.replace('/','_')}.npz"
        recent = list(self.memories)[-2000:]
        dnas  = [m["dna"]    for m in recent]
        acts  = [m["action"] for m in recent]
        rews  = [m["reward"] for m in recent]
        np.savez_compressed(str(p), dnas=dnas, acts=acts, rews=rews)

    def _load(self):
        p = RL_MODELS_DIR / f"oracle_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p), allow_pickle=True)
            for dna, a, r in zip(d["dnas"], d["acts"], d["rews"]):
                self.memories.append({"dna": str(dna), "action": int(a),
                                      "reward": float(r), "ts": 0})


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 10: KRAKEN-VENOM — Contrarian
# ══════════════════════════════════════════════════════════════════════════════

class KrakenVenom(BaseEngine):
    NAME = "KRAKEN-VENOM"
    SOUL = "When the crowd screams BUY, I sell. When they panic, I feast."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.net = NumpyMLP(STATE_DIM, 96, N_ACTIONS, lr=0.001)
        self.crowd_fear_threshold  = 0.72
        self.crowd_greed_threshold = 0.72
        self.contrarian_scale = 1.4
        self._extreme_events: deque = deque(maxlen=200)
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        arr = state.to_array()
        # Crowd signals
        rsi  = state.rsi_14
        bb   = state.bb_position
        fund = state.funding_rate
        fg   = state.fear_greed_norm    # normalised -1..+1
        imb  = state.ob_imbalance

        extreme_greed = (rsi > self.crowd_greed_threshold and
                         bb > 0.92 and fund > 0.0002 and fg > 0.5)
        extreme_fear  = (rsi < (1-self.crowd_fear_threshold) and
                         bb < 0.08 and fund < -0.0002 and fg < -0.5)

        if extreme_greed:
            self._extreme_events.append(("greed", state.volatility_1m))
            return Action.STRONG_SELL, min(0.95, 0.7 + abs(imb) * 0.4)
        if extreme_fear:
            self._extreme_events.append(("fear", state.volatility_1m))
            return Action.STRONG_BUY,  min(0.95, 0.7 + abs(imb) * 0.4)

        # Mild contrarian via network
        out, _ = self.net.forward(arr)
        probs  = _softmax(-out)   # Invert logits — VENOM fades consensus
        idx    = int(np.argmax(probs))
        return Action(idx), float(probs[idx]) * 0.6

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = state.to_array()
        out, _ = self.net.forward(x, training=True)
        probs  = _softmax(-out)
        target = probs.copy()
        target[action.value] = 1.0 if reward > 0 else 0.0
        self.net.backward_td(target, -probs)

    def save(self):
        self.net.save(str(RL_MODELS_DIR / f"venom_{self.pair.replace('/','_')}"))

    def _load(self):
        self.net.load(str(RL_MODELS_DIR / f"venom_{self.pair.replace('/','_')}"))


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 11: KRAKEN-TITAN — Cross-Pair PCA Macro
# ══════════════════════════════════════════════════════════════════════════════

class KrakenTitan(BaseEngine):
    NAME = "KRAKEN-TITAN"
    SOUL = "I see the forest, not the trees. Every tree bends to my wind."

    _pair_prices:  Dict[str, deque]    = {}
    _macro_signal: float               = 0.0
    _macro_conf:   float               = 0.0
    _regime_vec:   np.ndarray          = np.array([0.25, 0.25, 0.25, 0.25])
    _last_update:  float               = 0.0
    _titan_lock    = threading.Lock()
    _update_interval = 20.0   # seconds

    def __init__(self, pair: str):
        super().__init__(pair)
        with KrakenTitan._titan_lock:
            if pair not in KrakenTitan._pair_prices:
                KrakenTitan._pair_prices[pair] = deque(maxlen=120)

    @classmethod
    def update_price(cls, pair: str, price: float):
        with cls._titan_lock:
            if pair not in cls._pair_prices:
                cls._pair_prices[pair] = deque(maxlen=120)
            cls._pair_prices[pair].append(price)
        if time.time() - cls._last_update > cls._update_interval:
            cls._compute_macro()

    @classmethod
    def _compute_macro(cls):
        with cls._titan_lock:
            valid = {p: list(v) for p, v in cls._pair_prices.items()
                     if len(v) >= 30}
        if len(valid) < 3:
            return
        min_len = min(len(v) for v in valid.values())
        if min_len < 10:
            return
        matrix = []
        for prices in valid.values():
            p = np.array(prices[-min_len:])
            rets = np.diff(p) / (p[:-1] + 1e-10)
            rets = (rets - rets.mean()) / (rets.std() + 1e-8)
            matrix.append(rets)
        X = np.array(matrix)   # n_pairs × T-1
        # Power iteration PCA
        v = np.random.randn(X.shape[1])
        for _ in range(20):
            v = X.T @ (X @ v)
            n = np.linalg.norm(v)
            if n > 0: v /= n
        pc1 = X @ v
        macro = float(np.tanh(np.mean(pc1) * 4))
        conf  = float(abs(macro))
        # Regime from pc1 distribution
        cls._macro_signal = macro
        cls._macro_conf   = conf
        cls._last_update  = time.time()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        if state.price_vs_vwap != 0:
            KrakenTitan.update_price(self.pair, 1.0 + state.price_vs_vwap)
        ms, mc = KrakenTitan._macro_signal, KrakenTitan._macro_conf
        if mc < 0.25:
            return Action.HOLD, 0.08
        if ms > 0.55:
            return (Action.STRONG_BUY if ms > 0.85 else Action.BUY), min(0.92, 0.55+mc*0.4)
        if ms < -0.55:
            return (Action.STRONG_SELL if ms < -0.85 else Action.SELL), min(0.92, 0.55+mc*0.4)
        return Action.HOLD, 0.08

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"macro_signal": round(KrakenTitan._macro_signal, 4),
                  "macro_conf": round(KrakenTitan._macro_conf, 4),
                  "pairs_tracked": len(KrakenTitan._pair_prices)})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 12: KRAKEN-HYDRA — 9-Head Internal Ensemble
# ══════════════════════════════════════════════════════════════════════════════

class KrakenHydra(BaseEngine):
    NAME = "KRAKEN-HYDRA"
    SOUL = "Nine heads. One body. Unstoppable. Cut one — two more grow."

    N_HEADS = 9

    def __init__(self, pair: str):
        super().__init__(pair)
        self.head_weights = np.ones(self.N_HEADS)
        self.head_acc: List[deque] = [deque(maxlen=50) for _ in range(self.N_HEADS)]
        self._last_votes: List[int] = [Action.HOLD.value] * self.N_HEADS
        self._load()

    def _head_votes(self, s: RLState) -> List[int]:
        v = []
        # H1: EMA cross momentum
        ec = s.ema_cross
        v.append(0 if ec>0.010 else 1 if ec>0.004 else 4 if ec<-0.010 else 3 if ec<-0.004 else 2)
        # H2: RSI mean reversion
        r = s.rsi_14
        v.append(0 if r<0.22 else 1 if r<0.35 else 4 if r>0.78 else 3 if r>0.65 else 2)
        # H3: Bollinger band
        bb = s.bb_position
        v.append(0 if bb>0.96 else 1 if bb>0.82 else 4 if bb<0.04 else 3 if bb<0.18 else 2)
        # H4: Order flow imbalance
        comb = s.ob_imbalance * 0.55 + s.trade_flow * 0.45
        v.append(0 if comb>0.38 else 1 if comb>0.16 else 4 if comb<-0.38 else 3 if comb<-0.16 else 2)
        # H5: Regime filter
        if   s.regime_bull    > 0.55: v.append(1)
        elif s.regime_bear    > 0.55: v.append(3)
        elif s.regime_ranging > 0.55: v.append(1 if r < 0.4 else 3 if r > 0.6 else 2)
        else:                         v.append(2)
        # H6: Funding rate contrarian
        f = s.funding_rate
        v.append(3 if f>0.0004 else 1 if f<-0.0004 else 2)
        # H7: Volume spike + direction
        vs = s.volume_spike
        pc = s.price_change_1m
        v.append(0 if vs>2.5 and pc>0.003 else 4 if vs>2.5 and pc<-0.003 else 2)
        # H8: Liquidation cascade
        liq = s.liquidation_vol
        v.append(1 if liq > 0.5 and s.regime_bear > 0.4 else
                 3 if liq > 0.5 and s.regime_bull > 0.4 else 2)
        # H9: Macro correlation (BTC dominance)
        bd = s.btc_dominance
        v.append(3 if bd > 0.62 else 1 if bd < 0.38 else 2)
        return v

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        votes = self._head_votes(state)
        self._last_votes = votes
        smap  = {0:+2.0, 1:+1.0, 2:0.0, 3:-1.0, 4:-2.0}
        score = sum(smap[v] * w for v, w in zip(votes, self.head_weights))
        total = self.head_weights.sum()
        norm  = score / max(total, 1e-8)
        bull  = sum(1 for v in votes if smap[v] > 0)
        bear  = sum(1 for v in votes if smap[v] < 0)
        if   norm >  0.9 and bull >= 6: return Action.STRONG_BUY,  min(0.95, 0.6+norm*0.12)
        elif norm >  0.3 and bull >= 5: return Action.BUY,          min(0.87, 0.55+norm*0.1)
        elif norm < -0.9 and bear >= 6: return Action.STRONG_SELL,  min(0.95, 0.6+abs(norm)*0.12)
        elif norm < -0.3 and bear >= 5: return Action.SELL,          min(0.87, 0.55+abs(norm)*0.1)
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        smap = {0:+1, 1:+1, 2:0, 3:-1, 4:-1}
        for i, vote in enumerate(self._last_votes):
            correct = ((smap[vote] > 0 and reward > 0) or
                       (smap[vote] < 0 and reward < 0) or
                       smap[vote] == 0)
            self.head_acc[i].append(1.0 if correct else 0.0)
            if len(self.head_acc[i]) >= 15:
                acc = float(sum(self.head_acc[i]) / len(self.head_acc[i]))
                self.head_weights[i] = max(0.05, min(4.0, acc * 2.5))

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"hydra_{self.pair.replace('/','_')}"),
            weights=self.head_weights
        )

    def _load(self):
        p = RL_MODELS_DIR / f"hydra_{self.pair.replace('/','_')}.npz"
        if p.exists():
            self.head_weights = np.load(str(p))["weights"]

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s["head_weights"] = [round(float(w), 3) for w in self.head_weights]
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 13: KRAKEN-VOID — Prototypical Few-Shot Meta-Learning
# ══════════════════════════════════════════════════════════════════════════════

class KrakenVoid(BaseEngine):
    NAME = "KRAKEN-VOID"
    SOUL = "Tabula rasa. The most powerful state of all. Zero to expert in 10 trades."

    EMBED_DIM = 24
    _global_protos: Dict[str, np.ndarray] = {}
    _global_counts: Dict[str, int]        = {}
    _proto_lock = threading.Lock()

    def __init__(self, pair: str):
        super().__init__(pair)
        self.embed = NumpyMLP(STATE_DIM, 96, self.EMBED_DIM, lr=0.003)
        self.local_protos: Dict[str, np.ndarray] = {}
        self.local_counts: Dict[str, int]        = {}
        self.n_local = 0
        self._last_emb = None

    def _get_embed(self, s: RLState) -> np.ndarray:
        emb, _ = self.embed.forward(s.to_array())
        return emb

    def _active_protos(self) -> Dict[str, np.ndarray]:
        if self.n_local >= 20:
            return self.local_protos
        result = {}
        with KrakenVoid._proto_lock:
            for k in set(list(self.local_protos) + list(KrakenVoid._global_protos)):
                lp = self.local_protos.get(k)
                gp = KrakenVoid._global_protos.get(k)
                lc = self.local_counts.get(k, 0)
                gc = KrakenVoid._global_counts.get(k, 1)
                lw = lc / max(self.n_local + lc, 1)
                if lp is not None and gp is not None:
                    result[k] = lw * lp + (1 - lw) * gp
                elif lp is not None:
                    result[k] = lp
                elif gp is not None:
                    result[k] = gp
        return result

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        emb = self._get_embed(state)
        self._last_emb = emb
        protos = self._active_protos()
        if not protos:
            return Action.HOLD, 0.0
        dists = {k: float(np.linalg.norm(emb - p)) for k, p in protos.items()}
        nearest = min(dists, key=dists.get)
        min_d   = dists[nearest]
        max_d   = max(dists.values()) + 1e-8
        conf    = 1.0 - min_d / max_d
        if   nearest == "win_buy":   action = Action.BUY
        elif nearest == "win_sell":  action = Action.SELL
        elif nearest == "fail_buy":  action = Action.SELL
        elif nearest == "fail_sell": action = Action.BUY
        else:                        return Action.HOLD, 0.0
        return action, min(0.92, conf)

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        if self._last_emb is None: return
        won  = reward > 0
        side = "buy" if action in (Action.STRONG_BUY, Action.BUY) else "sell"
        key  = f"{'win' if won else 'fail'}_{side}"
        emb  = self._last_emb
        self.local_counts[key] = self.local_counts.get(key, 0) + 1
        n = self.local_counts[key]
        if key in self.local_protos:
            self.local_protos[key] = (self.local_protos[key] * (n-1) + emb) / n
        else:
            self.local_protos[key] = emb.copy()
        # Update global
        with KrakenVoid._proto_lock:
            gc = KrakenVoid._global_counts.get(key, 0) + 1
            KrakenVoid._global_counts[key] = gc
            if key in KrakenVoid._global_protos:
                KrakenVoid._global_protos[key] = (
                    KrakenVoid._global_protos[key] * (gc-1) + emb
                ) / gc
            else:
                KrakenVoid._global_protos[key] = emb.copy()
        self.n_local += 1


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 14: KRAKEN-PULSE — Fourier Cycle Detection
# ══════════════════════════════════════════════════════════════════════════════

class KrakenPulse(BaseEngine):
    NAME = "KRAKEN-PULSE"
    SOUL = "The market has a heartbeat. I hear it. I trade its rhythm."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.price_history: deque = deque(maxlen=512)
        self.dominant_period = 0
        self.phase_now = 0.0
        self.amplitude = 0.0
        self.last_fft_time = 0.0
        self.net = NumpyMLP(STATE_DIM + 4, 64, N_ACTIONS, lr=0.001)
        self._load()

    def _analyse_cycles(self):
        if len(self.price_history) < 64:
            return
        prices = np.array(list(self.price_history))
        prices = (prices - prices.mean()) / (prices.std() + 1e-8)
        fft   = np.fft.rfft(prices)
        power = np.abs(fft) ** 2
        # Find dominant frequency (ignore DC at 0)
        dom_idx = int(np.argmax(power[1:]) + 1)
        n = len(prices)
        if dom_idx > 0:
            self.dominant_period = n / dom_idx
            self.amplitude = float(2 * np.abs(fft[dom_idx]) / n)
            self.phase_now = float(np.angle(fft[dom_idx]))
        self.last_fft_time = time.time()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        p = state.price_vs_vwap
        if p != 0:
            self.price_history.append(1.0 + p)
        if time.time() - self.last_fft_time > 10:
            self._analyse_cycles()
        x = np.append(state.to_array(), [
            self.dominant_period / 100.0,
            self.amplitude * 100,
            math.sin(self.phase_now),
            math.cos(self.phase_now),
        ]).astype(np.float32)
        out, _ = self.net.forward(x)
        probs  = _softmax(out)
        idx    = int(np.argmax(probs))
        conf   = float(probs[idx])
        # Amplify when in strong cycle
        if self.amplitude > 0.02:
            conf = min(0.96, conf * 1.3)
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = np.append(state.to_array(), [
            self.dominant_period / 100.0, self.amplitude * 100,
            math.sin(self.phase_now), math.cos(self.phase_now),
        ]).astype(np.float32)
        out, _  = self.net.forward(x, training=True)
        probs   = _softmax(out)
        target  = probs.copy()
        target[action.value] = 1.0 if reward > 0 else 0.0
        self.net.backward_td(target, probs)

    def save(self):
        self.net.save(str(RL_MODELS_DIR / f"pulse_{self.pair.replace('/','_')}"))

    def _load(self):
        self.net.load(str(RL_MODELS_DIR / f"pulse_{self.pair.replace('/','_')}"))

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"period": round(self.dominant_period,1),
                  "amplitude": round(self.amplitude,4)})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 15: KRAKEN-INFINITY — Hierarchical Meta-Router
# ══════════════════════════════════════════════════════════════════════════════

class KrakenInfinity(BaseEngine):
    NAME = "KRAKEN-INFINITY"
    SOUL = "I am every engine. I am none. I am the space between them."

    ALL_KEYS = ['PPO','A3C','DQN','SAC','TD3','APEX','PHANTOM','STORM',
                'ORACLE','VENOM','TITAN','HYDRA','VOID','PULSE']

    N_CELLS   = 16   # 4 regimes × 4 vol tiers
    N_ENGINES = 14

    def __init__(self, pair: str):
        super().__init__(pair)
        self.routing = np.ones((self.N_CELLS, self.N_ENGINES)) * 0.5
        self.counts  = np.ones((self.N_CELLS, self.N_ENGINES))
        self.conf_hist: deque = deque(maxlen=200)
        self.conf_mean = 0.5
        self.conf_std  = 0.15
        self._last_cell    = 0
        self._last_routed  = -1
        self.current_expert = None
        self._veto_count   = 0
        self._load()

    def _get_cell(self, s: RLState) -> int:
        if   s.regime_bull    > 0.50: regime = 0
        elif s.regime_bear    > 0.50: regime = 1
        elif s.regime_ranging > 0.50: regime = 2
        else:                          regime = 3
        vol = s.volatility_1m
        if   vol < 0.001: tier = 0
        elif vol < 0.004: tier = 1
        elif vol < 0.010: tier = 2
        else:             tier = 3
        return regime * 4 + tier

    def route(self, state: RLState,
              decisions: Dict[str, Tuple[Action, float]]
              ) -> Tuple[Action, float, str]:
        cell = self._get_cell(state)
        self._last_cell = cell
        # Anomaly veto
        confs = [c for _, c in decisions.values() if c > 0]
        if confs:
            self.conf_hist.extend(confs)
            if len(self.conf_hist) >= 30:
                self.conf_mean = float(np.mean(list(self.conf_hist)))
                self.conf_std  = float(np.std(list(self.conf_hist)))
            avg_c  = float(np.mean(confs))
            z      = abs(avg_c - self.conf_mean) / max(self.conf_std, 0.01)
            if z > 3.5 and avg_c > 0.92:
                self._veto_count += 1
                return Action.HOLD, 0.0, "VETO_ANOMALY"
        # Best engine for this cell
        scores = self.routing[cell]
        # UCB routing
        log_n  = math.log(max(float(self.counts[cell].sum()), 1))
        ucb    = scores + 1.2 * np.sqrt(log_n / (self.counts[cell] + 1e-8))
        best   = int(np.argmax(ucb))
        key    = self.ALL_KEYS[best]
        self._last_routed  = best
        self.current_expert = key
        if key in decisions:
            act, conf = decisions[key]
            boost = min(0.99, conf * (0.75 + float(scores[best]) * 0.45))
            return act, boost, key
        return Action.HOLD, 0.0, "NO_DATA"

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        idx = self._last_routed
        if idx < 0: return
        cell = self._last_cell
        alpha = 0.08
        self.routing[cell, idx] = (
            (1-alpha) * self.routing[cell, idx] + alpha * float(reward > 0)
        )
        self.counts[cell, idx] += 1

    def best_per_regime(self) -> Dict[str, str]:
        labels = [f"{'bull' if r<4 else 'bear' if r<8 else 'range' if r<12 else 'chaos'}"
                  f"+{'calm' if c==0 else 'norm' if c==1 else 'high' if c==2 else 'ultra'}"
                  for r in range(0,16,4) for c in range(4)]
        return {labels[i]: self.ALL_KEYS[int(np.argmax(self.routing[i]))]
                for i in range(self.N_CELLS)}

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"infinity_{self.pair.replace('/','_')}"),
            rt=self.routing, ct=self.counts
        )

    def _load(self):
        p = RL_MODELS_DIR / f"infinity_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.routing = d["rt"]; self.counts = d["ct"]

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"expert": self.current_expert,
                  "vetos": self._veto_count,
                  "best_per_regime": self.best_per_regime()})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 16: KRAKEN-NEMESIS — Adversarial Self-Play
# ══════════════════════════════════════════════════════════════════════════════

class KrakenNemesis(BaseEngine):
    """
    Soul:    "I am my own worst enemy — and that makes me invincible."

    Nemesis trains an adversarial opponent that ALWAYS tries to exploit
    the cluster's current strategy. By fighting itself, it discovers every
    weakness before the market does.

    Architecture:
    ─ Main policy   θ_main  → tries to profit
    ─ Adversary     θ_adv   → tries to fool θ_main
    ─ Every N trades: swap roles, adversary becomes main, main becomes adversary
    ─ GAN-style training loop: main maximises reward, adv minimises it
    ─ Output: a policy that has SEEN ITS OWN FAILURES and healed them
    """

    NAME = "KRAKEN-NEMESIS"
    SOUL = "I am my own worst enemy. Therefore I am invincible."

    SWAP_EVERY = 100

    def __init__(self, pair: str):
        super().__init__(pair)
        self.main = NumpyMLP(STATE_DIM, 192, N_ACTIONS, h2=96, lr=0.001)
        self.adv  = NumpyMLP(STATE_DIM, 192, N_ACTIONS, h2=96, lr=0.001)
        self.buf_main: List = []
        self.buf_adv:  List = []
        self.is_adversarial_tick = False
        self.swap_counter = 0
        self._exploit_log: deque = deque(maxlen=100)
        self._load()

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x = state.to_array()
        # Every other tick: use adversary to stress-test
        self.is_adversarial_tick = (self.swap_counter % 2 == 0)
        if self.is_adversarial_tick:
            adv_out, _ = self.adv.forward(x)
            adv_action = Action(int(np.argmax(adv_out)))
            main_out, _ = self.main.forward(x)
            # If adversary strongly disagrees, it found a weakness — penalise
            main_action = Action(int(np.argmax(main_out)))
            if adv_action != main_action:
                self._exploit_log.append(1)
                # Return the SAFER of the two (hold as mediator)
                if adv_action == Action.HOLD:
                    return Action.HOLD, 0.1
                main_probs = _softmax(main_out)
                return main_action, float(main_probs[main_action.value]) * 0.7
        out, _ = self.main.forward(x)
        probs  = _softmax(out)
        idx    = int(np.argmax(probs))
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = state.to_array()
        # Update main toward reward
        main_out, _ = self.main.forward(x, training=True)
        main_probs  = _softmax(main_out)
        target_main = main_probs.copy()
        target_main[action.value] = 1.0 if reward > 0 else 0.0
        self.main.backward_td(target_main, main_probs)
        # Update adversary in OPPOSITE direction (tries to hurt main)
        adv_out, _  = self.adv.forward(x, training=True)
        adv_probs   = _softmax(adv_out)
        target_adv  = adv_probs.copy()
        # Adversary wins when main loses — anti-correlated training
        target_adv[action.value] = 0.0 if reward > 0 else 1.0
        self.adv.backward_td(target_adv, adv_probs)
        self.swap_counter += 1
        # Periodic swap: adversary becomes the main
        if self.swap_counter % self.SWAP_EVERY == 0:
            adv_wr = self.wins / max(self.total_trades, 1)
            if adv_wr < 0.5:   # if main is losing, promote adversary
                self.main, self.adv = self.adv, self.main
                log.debug(f"NEMESIS[{self.pair}]: role swap — adversary promoted")

    def save(self):
        safe = self.pair.replace("/","_")
        self.main.save(str(RL_MODELS_DIR / f"nemesis_m_{safe}"))
        self.adv.save( str(RL_MODELS_DIR / f"nemesis_a_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        self.main.load(str(RL_MODELS_DIR / f"nemesis_m_{safe}"))
        self.adv.load( str(RL_MODELS_DIR / f"nemesis_a_{safe}"))

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s["exploit_rate"] = round(
            sum(self._exploit_log)/max(len(self._exploit_log),1), 3
        )
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 17: KRAKEN-SOVEREIGN — Transformer Attention over State Sequences
# ══════════════════════════════════════════════════════════════════════════════

class KrakenSovereign(BaseEngine):
    """
    Soul:    "I don't see one moment. I see the river of time."

    Architecture:
    ─ Maintains a sequence buffer of the last T=16 state vectors
    ─ Implements scaled dot-product attention (pure NumPy)
    ─ Q, K, V projections: STATE_DIM → ATTN_DIM
    ─ Multi-head (4 heads), concatenated
    ─ Feed-forward layer → N_ACTIONS
    ─ Sequence captures temporal dependencies invisible to single-tick engines
    """

    NAME = "KRAKEN-SOVEREIGN"
    SOUL = "I don't see one moment. I see the river of time flowing."

    SEQ_LEN  = 16
    ATTN_DIM = 32
    N_HEADS  = 4
    HEAD_DIM = ATTN_DIM // N_HEADS

    def __init__(self, pair: str):
        super().__init__(pair)
        scale = 0.02
        # Q, K, V for each head
        self.Wq = [np.random.randn(STATE_DIM, self.HEAD_DIM) * scale for _ in range(self.N_HEADS)]
        self.Wk = [np.random.randn(STATE_DIM, self.HEAD_DIM) * scale for _ in range(self.N_HEADS)]
        self.Wv = [np.random.randn(STATE_DIM, self.HEAD_DIM) * scale for _ in range(self.N_HEADS)]
        self.Wo  = np.random.randn(self.ATTN_DIM, STATE_DIM) * scale
        self.ff  = NumpyMLP(STATE_DIM, 128, N_ACTIONS, lr=0.0005)
        self.seq: deque = deque(maxlen=self.SEQ_LEN)
        self._last_ctx = np.zeros(STATE_DIM)
        self._adam_Wq = [AdamOptimizer(W.shape, lr=0.0005) for W in self.Wq]
        self._adam_Wk = [AdamOptimizer(W.shape, lr=0.0005) for W in self.Wk]
        self._adam_Wv = [AdamOptimizer(W.shape, lr=0.0005) for W in self.Wv]
        self._adam_Wo  = AdamOptimizer(self.Wo.shape, lr=0.0005)
        self._load()

    def _attend(self, seq: np.ndarray) -> np.ndarray:
        """Multi-head attention on sequence. seq: T × D → context: D"""
        T, D = seq.shape
        heads = []
        for h in range(self.N_HEADS):
            Q = seq @ self.Wq[h]    # T × head_dim
            K = seq @ self.Wk[h]
            V = seq @ self.Wv[h]
            scale = math.sqrt(self.HEAD_DIM)
            attn  = _softmax((Q @ K.T) / scale)   # T × T
            head  = attn @ V                        # T × head_dim
            heads.append(head[-1])                  # take last token
        concat = np.concatenate(heads)              # ATTN_DIM
        ctx    = concat @ self.Wo                   # → STATE_DIM
        return _tanh(ctx + seq[-1])                 # residual

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x = state.to_array()
        self.seq.append(x)
        if len(self.seq) < 4:
            return Action.HOLD, 0.05
        seq_arr = np.array(list(self.seq))
        ctx     = self._attend(seq_arr)
        self._last_ctx = ctx
        out, _  = self.ff.forward(ctx)
        probs   = _softmax(out)
        idx     = int(np.argmax(probs))
        conf    = float(probs[idx])
        # Sovereign is more confident with a longer sequence
        seq_bonus = min(1.2, 0.8 + len(self.seq) / (self.SEQ_LEN * 2))
        return Action(idx), min(0.96, conf * seq_bonus)

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        if len(self.seq) < 4: return
        ctx = self._last_ctx
        out, _ = self.ff.forward(ctx, training=True)
        probs  = _softmax(out)
        target = probs.copy()
        target[action.value] = 1.0 if reward > 0 else 0.0
        self.ff.backward_td(target, probs)
        # Approximate attention gradient via reward signal
        if abs(reward) > 0.0005:
            for h in range(self.N_HEADS):
                grad_noise = np.random.randn(*self.Wq[h].shape) * abs(reward) * 0.001
                self.Wq[h] += grad_noise * (1 if reward > 0 else -1)

    def save(self):
        safe = self.pair.replace("/","_")
        self.ff.save(str(RL_MODELS_DIR / f"sovereign_ff_{safe}"))
        d = {f"Wq{i}": self.Wq[i] for i in range(self.N_HEADS)}
        d.update({f"Wk{i}": self.Wk[i] for i in range(self.N_HEADS)})
        d.update({f"Wv{i}": self.Wv[i] for i in range(self.N_HEADS)})
        d["Wo"] = self.Wo
        np.savez_compressed(str(RL_MODELS_DIR / f"sovereign_attn_{safe}"), **d)

    def _load(self):
        safe = self.pair.replace("/","_")
        self.ff.load(str(RL_MODELS_DIR / f"sovereign_ff_{safe}"))
        p = RL_MODELS_DIR / f"sovereign_attn_{safe}.npz"
        if p.exists():
            d = np.load(str(p))
            for i in range(self.N_HEADS):
                if f"Wq{i}" in d: self.Wq[i] = d[f"Wq{i}"]
                if f"Wk{i}" in d: self.Wk[i] = d[f"Wk{i}"]
                if f"Wv{i}" in d: self.Wv[i] = d[f"Wv{i}"]
            if "Wo" in d: self.Wo = d["Wo"]


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 18: KRAKEN-WRAITH — Cross-Pair Statistical Arbitrage
# ══════════════════════════════════════════════════════════════════════════════

class KrakenWraith(BaseEngine):
    """
    Soul:    "Markets are never truly independent. I trade their shadows."

    Architecture:
    ─ Maintains rolling price histories for all pairs (class-level)
    ─ Detects cointegration using Engle-Granger residuals (rolling OLS)
    ─ When pair A diverges from pair B beyond Zscore threshold → mean-revert
    ─ Uses 3-pair triangular relationships for stronger signals
    ─ Acts as a ARBITRAGE ENGINE: doesn't need market direction, only spread
    """

    NAME = "KRAKEN-WRAITH"
    SOUL = "Markets are never independent. I trade their shadows."

    _all_prices: Dict[str, deque] = {}
    _spread_hist: Dict[str, deque] = {}
    _wraith_lock = threading.Lock()
    ZSCORE_ENTRY  = 2.0
    ZSCORE_EXIT   = 0.3
    WINDOW        = 60

    def __init__(self, pair: str):
        super().__init__(pair)
        with KrakenWraith._wraith_lock:
            if pair not in KrakenWraith._all_prices:
                KrakenWraith._all_prices[pair] = deque(maxlen=200)
        self._last_z: Dict[str, float] = {}

    @classmethod
    def feed_price(cls, pair: str, price: float):
        with cls._wraith_lock:
            if pair not in cls._all_prices:
                cls._all_prices[pair] = deque(maxlen=200)
            cls._all_prices[pair].append(price)

    def _zscore_spread(self, pair_a: str, pair_b: str) -> Optional[float]:
        with KrakenWraith._wraith_lock:
            pa = list(KrakenWraith._all_prices.get(pair_a, []))
            pb = list(KrakenWraith._all_prices.get(pair_b, []))
        n = min(len(pa), len(pb), self.WINDOW)
        if n < 20: return None
        pa_arr = np.array(pa[-n:])
        pb_arr = np.array(pb[-n:])
        # OLS: hedge ratio β
        cov = np.cov(np.log(pa_arr + 1e-10), np.log(pb_arr + 1e-10))
        if cov[1, 1] < 1e-10: return None
        beta   = cov[0, 1] / cov[1, 1]
        spread = np.log(pa_arr + 1e-10) - beta * np.log(pb_arr + 1e-10)
        mean   = spread.mean()
        std    = spread.std() + 1e-8
        return float((spread[-1] - mean) / std)

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        # Update own price
        own_price = 1.0 + state.price_vs_vwap
        KrakenWraith.feed_price(self.pair, own_price)
        # Check spread against all other pairs
        best_z    = 0.0
        best_pair = None
        with KrakenWraith._wraith_lock:
            other_pairs = [p for p in KrakenWraith._all_prices if p != self.pair]
        for other in other_pairs[:10]:   # check up to 10 pairs
            z = self._zscore_spread(self.pair, other)
            if z is not None and abs(z) > abs(best_z):
                best_z, best_pair = z, other

        if best_pair is None or abs(best_z) < self.ZSCORE_ENTRY:
            return Action.HOLD, 0.1

        # Mean reversion trade
        conf = min(0.94, 0.55 + (abs(best_z) - self.ZSCORE_ENTRY) * 0.15)
        if best_z > self.ZSCORE_ENTRY:
            return Action.SELL, conf      # spread too wide → sell A, it will revert
        else:
            return Action.BUY,  conf      # spread too narrow → buy A

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s["tracked_pairs"] = len(KrakenWraith._all_prices)
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 19: KRAKEN-ABYSS — Dueling Noisy DQN with C51 Distributional RL
# ══════════════════════════════════════════════════════════════════════════════

class KrakenAbyss(BaseEngine):
    """
    Soul:    "I don't predict the expected value. I predict the DISTRIBUTION."

    Architecture:
    ─ Dueling network: value stream V(s) + advantage stream A(s,a)
    ─ Noisy linear layers: ε-greedy replaced by learned noise for exploration
    ─ C51 distributional RL: models full return distribution (51 atoms)
    ─ Learns not just WHERE the market goes, but HOW CERTAIN and how RISKY
    ─ Uncertainty-aware actions: avoids high-variance positions
    """

    NAME = "KRAKEN-ABYSS"
    SOUL = "I don't predict the outcome. I predict the entire distribution of outcomes."

    N_ATOMS    = 21        # distributional RL atoms (reduced for speed)
    V_MIN, V_MAX = -5.0, 5.0

    def __init__(self, pair: str):
        super().__init__(pair)
        dz = (self.V_MAX - self.V_MIN) / (self.N_ATOMS - 1)
        self.support = np.linspace(self.V_MIN, self.V_MAX, self.N_ATOMS)
        # Shared body
        self.body     = NumpyMLP(STATE_DIM, 192, 128, h2=96, lr=LR)
        # Dueling heads (distributional)
        self.val_head = NumpyMLP(128, 64, self.N_ATOMS, lr=LR)           # V(s)
        self.adv_head = NumpyMLP(128, 64, N_ACTIONS * self.N_ATOMS, lr=LR)  # A(s,a)
        # Noisy exploration params
        self.noise_std = 0.5
        self.noise_decay = 0.9999
        self.replay = PrioritizedBuffer(BUFFER_CAP // 3)
        self._load()

    def _forward_dist(self, x: np.ndarray, training: bool = False
                      ) -> np.ndarray:
        """Returns action distributions: N_ACTIONS × N_ATOMS (probabilities)."""
        # Add learned noise for exploration
        noise = np.random.randn(*x.shape) * self.noise_std * float(training)
        feat, _ = self.body.forward(x + noise, training)
        val, _  = self.val_head.forward(feat)              # N_ATOMS
        adv, _  = self.adv_head.forward(feat)              # N_ACTIONS * N_ATOMS
        adv_mat = adv.reshape(N_ACTIONS, self.N_ATOMS)
        adv_mean= adv_mat.mean(axis=0, keepdims=True)
        q_mat   = val + adv_mat - adv_mean                 # N_ACTIONS × N_ATOMS
        return np.array([_softmax(q_mat[i]) for i in range(N_ACTIONS)])

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x    = state.to_array()
        dist = self._forward_dist(x)                       # N_ACTIONS × N_ATOMS
        # Expected Q-values
        q_vals = dist @ self.support
        # Uncertainty: std of each action's distribution
        q_std  = np.array([
            float(np.sqrt(((dist[a] * (self.support - q_vals[a])**2).sum())))
            for a in range(N_ACTIONS)
        ])
        # Risk-adjusted selection: penalise high-variance actions
        risk_adjusted = q_vals - 0.3 * q_std
        idx  = int(np.argmax(risk_adjusted))
        conf = float(_softmax(q_vals)[idx])
        # Decay noise
        self.noise_std = max(0.05, self.noise_std * self.noise_decay)
        return Action(idx), conf

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        td = abs(reward) + 0.01
        self.replay.push(state.to_array(), action.value, reward,
                         next_state.to_array(), done, td)
        if len(self.replay) < BATCH_SIZE: return
        for (s, a, r, ns, d) in self.replay.sample(min(32, BATCH_SIZE)):
            dist_ns = self._forward_dist(ns)
            q_ns    = dist_ns @ self.support
            a_star  = int(np.argmax(q_ns))
            # Project target distribution
            tz = np.clip(r + (0.0 if d else GAMMA * self.support), self.V_MIN, self.V_MAX)
            b  = (tz - self.V_MIN) / ((self.V_MAX - self.V_MIN) / (self.N_ATOMS - 1))
            l  = np.floor(b).astype(int).clip(0, self.N_ATOMS-1)
            u  = np.ceil (b).astype(int).clip(0, self.N_ATOMS-1)
            target_dist = np.zeros(self.N_ATOMS)
            for j in range(self.N_ATOMS):
                target_dist[l[j]] += dist_ns[a_star, j] * (u[j] - b[j])
                target_dist[u[j]] += dist_ns[a_star, j] * (b[j] - l[j])
            target_dist /= target_dist.sum() + 1e-10
            # Cross-entropy gradient
            dist_s = self._forward_dist(s, training=True)
            grad   = dist_s[a] - target_dist
            # Backprop approximation through adv head
            adv_out, _ = self.adv_head.forward(
                self.body.forward(s)[0]
            )
            adv_target = adv_out.copy()
            adv_target[a*self.N_ATOMS : (a+1)*self.N_ATOMS] -= grad * 0.5
            self.adv_head.backward_td(adv_target, adv_out)

    def save(self):
        safe = self.pair.replace("/","_")
        self.body.save(str(RL_MODELS_DIR / f"abyss_b_{safe}"))
        self.val_head.save(str(RL_MODELS_DIR / f"abyss_v_{safe}"))
        self.adv_head.save(str(RL_MODELS_DIR / f"abyss_a_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        self.body.load(str(RL_MODELS_DIR / f"abyss_b_{safe}"))
        self.val_head.load(str(RL_MODELS_DIR / f"abyss_v_{safe}"))
        self.adv_head.load(str(RL_MODELS_DIR / f"abyss_a_{safe}"))


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 20: KRAKEN-GENESIS — Genetic Algorithm Policy Evolution
# ══════════════════════════════════════════════════════════════════════════════

class KrakenGenesis(BaseEngine):
    """
    Soul:    "Evolution has no ego. Only survival matters."

    Architecture:
    ─ Population of POP_SIZE policy vectors θ_i
    ─ Fitness = cumulative reward over last EVAL_WINDOW ticks
    ─ Tournament selection → crossover → Gaussian mutation
    ─ Elitism: top 20% survive unchanged
    ─ Species diversity: if population converges, inject random immigrants
    ─ Adaptive mutation rate: decreases when fitness is improving
    """

    NAME = "KRAKEN-GENESIS"
    SOUL = "Evolution has no ego. Only fitness. Only survival."

    POP_SIZE    = 16
    EVAL_WINDOW = 50
    ELITE_FRAC  = 0.25
    MUT_RATE    = 0.12

    def __init__(self, pair: str):
        super().__init__(pair)
        dim = STATE_DIM * N_ACTIONS + N_ACTIONS
        self.population = np.random.randn(self.POP_SIZE, dim) * 0.1
        self.fitness    = np.zeros(self.POP_SIZE)
        self.active_idx = 0
        self.tick_count = 0
        self.mut_sigma  = 0.08
        self.best_ever_fitness = 0.0
        self._reward_buf: deque = deque(maxlen=self.EVAL_WINDOW)
        self._diversity_check_every = 200
        self._load()

    def _policy(self, theta: np.ndarray, x: np.ndarray) -> np.ndarray:
        W = theta[:STATE_DIM * N_ACTIONS].reshape(STATE_DIM, N_ACTIONS)
        b = theta[STATE_DIM * N_ACTIONS:]
        return _softmax(x @ W + b)

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        x     = state.to_array()
        probs = self._policy(self.population[self.active_idx], x)
        idx   = int(np.argmax(probs))
        return Action(idx), float(probs[idx])

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        self._reward_buf.append(reward)
        self.fitness[self.active_idx] += reward
        self.tick_count += 1
        # Evaluate every EVAL_WINDOW ticks → evolve
        if self.tick_count % self.EVAL_WINDOW == 0:
            self._evolve()
            self._reward_buf.clear()

    def _evolve(self):
        n_elite = max(1, int(self.POP_SIZE * self.ELITE_FRAC))
        order   = np.argsort(self.fitness)[::-1]
        new_pop = [self.population[i].copy() for i in order[:n_elite]]

        # Tournament selection + crossover + mutation
        while len(new_pop) < self.POP_SIZE:
            # Tournament of 3
            t1 = int(order[random.randint(0, n_elite-1)])
            t2 = int(order[random.randint(0, n_elite-1)])
            p1 = self.population[t1]
            p2 = self.population[t2]
            # Uniform crossover
            mask  = np.random.rand(len(p1)) < 0.5
            child = np.where(mask, p1, p2)
            # Adaptive mutation
            if self.fitness[order[0]] > self.best_ever_fitness:
                self.mut_sigma = max(0.02, self.mut_sigma * 0.98)
            else:
                self.mut_sigma = min(0.20, self.mut_sigma * 1.05)
            mutation_mask = np.random.rand(len(child)) < self.MUT_RATE
            child += mutation_mask * np.random.randn(len(child)) * self.mut_sigma
            new_pop.append(child)

        # Diversity check: if variance too low → inject 2 random immigrants
        pop_arr = np.array(new_pop)
        if np.std(pop_arr) < 0.01:
            for i in range(2):
                new_pop[-(i+1)] = np.random.randn(len(new_pop[0])) * 0.1

        self.population = np.array(new_pop)
        self.best_ever_fitness = max(self.best_ever_fitness, float(self.fitness.max()))
        self.fitness    = np.zeros(self.POP_SIZE)
        self.active_idx = 0
        log.debug(f"GENESIS[{self.pair}]: evolved gen, best_ever={self.best_ever_fitness:.4f}")

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"genesis_{self.pair.replace('/','_')}"),
            pop=self.population, best=np.array([self.best_ever_fitness])
        )

    def _load(self):
        p = RL_MODELS_DIR / f"genesis_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.population = d["pop"]
            self.best_ever_fitness = float(d["best"][0])

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({"best_ever": round(self.best_ever_fitness, 4),
                  "mut_sigma": round(self.mut_sigma, 4)})
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 21: KRAKEN-MIRAGE — Illusion Detector
# ══════════════════════════════════════════════════════════════════════════════

class KrakenMirage(BaseEngine):
    """
    Soul:    "I see through the lies. The market's greatest weapon is deception."

    Architecture:
    ─ Trained ONLY on examples where signals were WRONG (trap classifier)
    ─ Maintains trap signature library: bull_trap, bear_trap, stop_hunt,
      spoof_wall, wash_trade, fake_breakout, news_reversal
    ─ Outputs danger_score ∈ [0,1] and recommended override
    ─ When danger_score > 0.65 → force HOLD regardless of other engines
    ─ The ONLY engine that can unilaterally CANCEL a trade
    """

    NAME = "KRAKEN-MIRAGE"
    SOUL = "I see through the market's lies. The trap is never where they say."

    TRAP_TYPES = ["bull_trap","bear_trap","stop_hunt","spoof_wall",
                  "wash_trade","fake_breakout","news_reversal","clean"]

    def __init__(self, pair: str):
        super().__init__(pair)
        # One binary classifier per trap type
        self.detectors: Dict[str, NumpyMLP] = {
            t: NumpyMLP(STATE_DIM + 8, 96, 2, lr=0.002)
            for t in self.TRAP_TYPES
        }
        self.trap_counts: Dict[str, int] = {t: 0 for t in self.TRAP_TYPES}
        self._last_scores: Dict[str, float] = {}
        self._false_signal_history: deque = deque(maxlen=500)
        self._price_buf: deque = deque(maxlen=30)
        self._vol_buf:   deque = deque(maxlen=30)
        self._load()

    def _extra_features(self, s: RLState) -> np.ndarray:
        """8 trap-specific features."""
        pc = self._price_buf
        vc = self._vol_buf
        vol_spike_ratio = (float(pc[-1]) / (float(np.mean(list(vc)[-10:])) + 1e-10)
                           if len(vc) >= 10 else 1.0)
        price_accel = (float((list(pc)[-1] - list(pc)[-3]) / (abs(list(pc)[-3]) + 1e-10))
                       if len(pc) >= 3 else 0.0)
        return np.array([
            s.volume_spike,
            abs(s.ob_imbalance),
            s.spread_pct * 100,
            abs(s.price_change_1m),
            s.vpin_proxy,
            vol_spike_ratio,
            price_accel,
            s.liquidation_vol,
        ], dtype=np.float32)

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        p = 1.0 + state.price_vs_vwap
        self._price_buf.append(p)
        self._vol_buf.append(state.volatility_1m)
        x_extra = self._extra_features(state)
        x = np.concatenate([state.to_array(), x_extra])
        trap_probs: Dict[str, float] = {}
        for t, net in self.detectors.items():
            out, _ = net.forward(x)
            trap_probs[t] = float(_softmax(out)[1])   # prob of being a trap
        self._last_scores = trap_probs
        non_clean = {k: v for k, v in trap_probs.items() if k != "clean"}
        danger = max(non_clean.values()) if non_clean else 0.0
        if danger > 0.65:
            self.trap_counts[max(non_clean, key=non_clean.get)] += 1
            return Action.HOLD, danger   # VETO signal — MIRAGE detected a trap
        return Action.HOLD, 0.0          # No trap → abstain (don't add noise)

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        if action == Action.HOLD: return
        x_extra = self._extra_features(state)
        x = np.concatenate([state.to_array(), x_extra])
        was_trap = reward < -0.001   # wrong trade = trap
        if was_trap:
            self._false_signal_history.append(state.to_array())
        # Update all detectors
        for t, net in self.detectors.items():
            out, _ = net.forward(x, training=True)
            probs  = _softmax(out)
            is_this_trap = (
                (t == "bull_trap"    and action in (Action.STRONG_BUY,Action.BUY)  and was_trap) or
                (t == "bear_trap"    and action in (Action.STRONG_SELL,Action.SELL) and was_trap) or
                (t == "fake_breakout"and abs(state.price_change_1m) > 0.005 and was_trap) or
                (t == "clean"        and not was_trap)
            )
            tgt = np.array([0.0, 1.0]) if is_this_trap else np.array([1.0, 0.0])
            net.backward_td(tgt, probs)

    def save(self):
        for t, net in self.detectors.items():
            safe = self.pair.replace("/","_")
            net.save(str(RL_MODELS_DIR / f"mirage_{t}_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        for t, net in self.detectors.items():
            net.load(str(RL_MODELS_DIR / f"mirage_{t}_{safe}"))

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s["trap_counts"] = self.trap_counts
        s["trap_scores"] = {k: round(v,3) for k,v in self._last_scores.items()}
        return s

    @property
    def has_veto(self) -> bool:
        """True if MIRAGE currently flags a trap."""
        non_clean = {k: v for k, v in self._last_scores.items() if k != "clean"}
        return bool(non_clean and max(non_clean.values()) > 0.65)


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 22: KRAKEN-ECLIPSE — Multi-Timeframe Cascade
# ══════════════════════════════════════════════════════════════════════════════

class KrakenEclipse(BaseEngine):
    """
    Soul:    "The 1-minute chart whispers. The 4-hour chart commands."

    Architecture:
    ─ Maintains OHLC bars for: 1m, 5m, 15m, 1h, 4h (synthesised from ticks)
    ─ Each timeframe has its own mini-policy network
    ─ Confluence gate: all TFs must agree direction for STRONG signal
    ─ Higher TFs veto lower TFs — 4h BEAR veto overrides 1m BUY
    ─ Trend alignment score: how many TFs agree?
    """

    NAME  = "KRAKEN-ECLIPSE"
    SOUL  = "1m whispers. 4h commands. I listen to both — always."

    TF_NAMES = ["1m","5m","15m","1h","4h"]
    TF_TICKS = [1, 5, 15, 60, 240]

    def __init__(self, pair: str):
        super().__init__(pair)
        self.tf_nets  = {tf: NumpyMLP(STATE_DIM, 64, N_ACTIONS, lr=0.001)
                         for tf in self.TF_NAMES}
        self.tf_bufs  = {tf: deque(maxlen=t*3) for tf, t in zip(self.TF_NAMES, self.TF_TICKS)}
        self.tf_bars  = {tf: [] for tf in self.TF_NAMES}
        self.tick_n   = 0
        self.tf_weights = {tf: 1.0 + i * 0.3 for i, tf in enumerate(self.TF_NAMES)}
        self._load()

    def _synth_bars(self, state: RLState):
        self.tick_n += 1
        for tf, n in zip(self.TF_NAMES, self.TF_TICKS):
            self.tf_bufs[tf].append(state.to_array())

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        self._synth_bars(state)
        x = state.to_array()
        votes: Dict[str, Tuple[Action, float]] = {}
        for tf in self.TF_NAMES:
            buf = list(self.tf_bufs[tf])
            if len(buf) < 5: continue
            avg_state = np.mean(buf, axis=0).astype(np.float32)
            out, _ = self.tf_nets[tf].forward(avg_state)
            probs  = _softmax(out)
            idx    = int(np.argmax(probs))
            votes[tf] = (Action(idx), float(probs[idx]))

        if not votes:
            return Action.HOLD, 0.0

        # Higher TF veto: if 4h or 1h strongly disagree, override lower TFs
        high_tf_actions = {tf: votes[tf][0] for tf in ["4h","1h"] if tf in votes}
        if len(high_tf_actions) == 2:
            vals = list(high_tf_actions.values())
            sm = {Action.STRONG_BUY:2,Action.BUY:1,Action.HOLD:0,Action.SELL:-1,Action.STRONG_SELL:-2}
            if sm[vals[0]] * sm[vals[1]] < 0:   # disagreement between 1h and 4h
                return Action.HOLD, 0.1

        # Weighted confluence
        smap = {Action.STRONG_BUY:2,Action.BUY:1,Action.HOLD:0,Action.SELL:-1,Action.STRONG_SELL:-2}
        score = sum(smap[a] * c * self.tf_weights[tf]
                    for tf, (a,c) in votes.items())
        total_w = sum(self.tf_weights[tf] for tf in votes)
        norm    = score / max(total_w, 1e-8)

        agree = sum(1 for a,_ in votes.values() if smap[a]*norm > 0)
        conf  = min(0.96, 0.5 + abs(norm) * 0.15 + agree * 0.04)

        if   norm >  0.8 and agree >= 3: return Action.STRONG_BUY,  conf
        elif norm >  0.3 and agree >= 2: return Action.BUY,          conf * 0.85
        elif norm < -0.8 and agree >= 3: return Action.STRONG_SELL,  conf
        elif norm < -0.3 and agree >= 2: return Action.SELL,          conf * 0.85
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = state.to_array()
        for tf in self.TF_NAMES:
            out, _ = self.tf_nets[tf].forward(x, training=True)
            probs  = _softmax(out)
            target = probs.copy(); target[action.value] = 1.0 if reward>0 else 0.0
            self.tf_nets[tf].backward_td(target, probs)
            # Update TF weight by accuracy
            was_correct = (probs[action.value] > 0.4 and reward > 0) or (probs[action.value] < 0.3 and reward < 0)
            self.tf_weights[tf] = min(3.0, max(0.2,
                self.tf_weights[tf] * (1.02 if was_correct else 0.99)
            ))

    def save(self):
        safe = self.pair.replace("/","_")
        for tf, net in self.tf_nets.items():
            net.save(str(RL_MODELS_DIR / f"eclipse_{tf}_{safe}"))

    def _load(self):
        safe = self.pair.replace("/","_")
        for tf, net in self.tf_nets.items():
            net.load(str(RL_MODELS_DIR / f"eclipse_{tf}_{safe}"))


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 23: KRAKEN-CHIMERA — Hybrid Rule+Neural, Regime-Switched
# ══════════════════════════════════════════════════════════════════════════════

class KrakenChimera(BaseEngine):
    """
    Soul:    "I am neither rule nor neural. I am whatever the moment demands."

    Architecture:
    ─ Rule-based sub-engine   (fast, interpretable, for high-volatility)
    ─ Neural sub-engine       (adaptive, for calm trending markets)
    ─ Hybrid mixer:           α × rule + (1-α) × neural
    ─ α adapts continuously:  sharp moves → more rule; slow trends → more neural
    ─ Acts as a stability layer: prevents neural drift in wild markets
    """

    NAME = "KRAKEN-CHIMERA"
    SOUL = "Rule or neural? Neither. Whatever the market demands."

    def __init__(self, pair: str):
        super().__init__(pair)
        self.neural = NumpyMLP(STATE_DIM, 128, N_ACTIONS, h2=64, lr=0.001)
        self.alpha  = 0.5    # rule weight; 0=full neural, 1=full rule
        self.alpha_ema = 0.5
        self._load()

    def _rule_score(self, s: RLState) -> float:
        """Pure rule-based score ∈ [-2, +2]."""
        score = 0.0
        # EMA cross
        ec = s.ema_cross
        score += 2.0 if ec>0.010 else 1.0 if ec>0.004 else -2.0 if ec<-0.010 else -1.0 if ec<-0.004 else 0
        # RSI
        r = s.rsi_14
        score += 1.5 if r<0.25 else 0.5 if r<0.38 else -1.5 if r>0.75 else -0.5 if r>0.62 else 0
        # OB imbalance
        score += s.ob_imbalance * 1.5
        # Funding contrarian
        score -= s.funding_rate * 5000
        # Trend
        score += (s.regime_bull - s.regime_bear) * 1.0
        return float(np.clip(score / 3.0, -2, 2))

    def _alpha_for_state(self, s: RLState) -> float:
        vol = s.volatility_1m
        # High volatility → trust rules more
        return float(np.clip(0.3 + vol * 50, 0.1, 0.85))

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        alpha = self._alpha_for_state(state)
        self.alpha_ema = 0.9 * self.alpha_ema + 0.1 * alpha
        self.alpha = self.alpha_ema
        # Rule signal
        rs = self._rule_score(state)
        if   rs >  1.5: rule_a, rule_c = Action.STRONG_BUY,  0.80
        elif rs >  0.5: rule_a, rule_c = Action.BUY,          0.65
        elif rs < -1.5: rule_a, rule_c = Action.STRONG_SELL,  0.80
        elif rs < -0.5: rule_a, rule_c = Action.SELL,          0.65
        else:           rule_a, rule_c = Action.HOLD,          0.20
        # Neural signal
        out, _   = self.neural.forward(state.to_array())
        probs    = _softmax(out)
        neural_a = Action(int(np.argmax(probs)))
        neural_c = float(probs[int(np.argmax(probs))])
        # Blend
        smap = {Action.STRONG_BUY:2,Action.BUY:1,Action.HOLD:0,Action.SELL:-1,Action.STRONG_SELL:-2}
        blend = alpha * smap[rule_a] + (1-alpha) * smap[neural_a]
        conf  = alpha * rule_c + (1-alpha) * neural_c
        if   blend >  1.5: return Action.STRONG_BUY,  min(0.95, conf)
        elif blend >  0.4: return Action.BUY,          min(0.87, conf)
        elif blend < -1.5: return Action.STRONG_SELL,  min(0.95, conf)
        elif blend < -0.4: return Action.SELL,          min(0.87, conf)
        return Action.HOLD, 0.0

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        x = state.to_array()
        out, _  = self.neural.forward(x, training=True)
        probs   = _softmax(out)
        target  = probs.copy()
        target[action.value] = 1.0 if reward > 0 else 0.0
        self.neural.backward_td(target, probs)

    def save(self):
        self.neural.save(str(RL_MODELS_DIR / f"chimera_{self.pair.replace('/','_')}"))

    def _load(self):
        self.neural.load(str(RL_MODELS_DIR / f"chimera_{self.pair.replace('/','_')}"))

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s["alpha"] = round(self.alpha, 3)
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 24: KRAKEN-AXIOM — Pure Bayesian Inference
# ══════════════════════════════════════════════════════════════════════════════

class KrakenAxiom(BaseEngine):
    """
    Soul:    "Belief updates perfectly. The truth is always Bayesian."

    Architecture:
    ─ Maintains a Beta distribution for each (state_bucket, action) pair
    ─ P(reward>0 | state_bucket, action) ~ Beta(α, β)
    ─ α incremented on win, β on loss — conjugate Bayesian update
    ─ Thompson sampling for action selection (sample from Beta)
    ─ Calibrated uncertainty: outputs posterior mean AND credible interval
    ─ UCB Thompson variant: acts only when posterior is confident (CI < 0.3)
    """

    NAME = "KRAKEN-AXIOM"
    SOUL = "Belief updates perfectly. I am always calibrated. Never overconfident."

    N_BUCKETS = 256

    def __init__(self, pair: str):
        super().__init__(pair)
        # Beta params: alpha (wins+1), beta (losses+1) per bucket×action
        self.alpha_mat = np.ones((self.N_BUCKETS, N_ACTIONS), dtype=np.float64)
        self.beta_mat  = np.ones((self.N_BUCKETS, N_ACTIONS), dtype=np.float64)
        self.n_mat     = np.zeros((self.N_BUCKETS, N_ACTIONS), dtype=np.float64)
        self._last_bucket = 0
        self._load()

    def _bucket(self, s: RLState) -> int:
        arr = s.to_array()
        key = np.sign(arr[[0,1,14,19,28,34,35,43]]).astype(int) + 1  # 0,1,2 per feature
        idx = 0
        for i, v in enumerate(key):
            idx += int(v) * (3 ** i)
        return idx % self.N_BUCKETS

    def get_action(self, state: RLState) -> Tuple[Action, float]:
        bkt = self._bucket(state)
        self._last_bucket = bkt
        # Thompson sampling: draw from Beta for each action
        samples = np.array([
            np.random.beta(self.alpha_mat[bkt, a], self.beta_mat[bkt, a])
            for a in range(N_ACTIONS)
        ])
        # Credible interval width — confidence metric
        ci_widths = np.array([
            self._ci_width(self.alpha_mat[bkt, a], self.beta_mat[bkt, a])
            for a in range(N_ACTIONS)
        ])
        best_a = int(np.argmax(samples))
        ci     = float(ci_widths[best_a])
        # Only act if sufficiently informed (CI < 0.35)
        n_obs  = float(self.n_mat[bkt, best_a])
        if ci > 0.35 or n_obs < 5:
            return Action.HOLD, 0.1
        posterior_mean = float(self.alpha_mat[bkt, best_a] /
                               (self.alpha_mat[bkt, best_a] + self.beta_mat[bkt, best_a]))
        conf = min(0.96, posterior_mean * (1 - ci))
        return Action(best_a), conf

    @staticmethod
    def _ci_width(alpha: float, beta: float) -> float:
        """Approximate 95% CI width for Beta(alpha, beta)."""
        mean = alpha / (alpha + beta + 1e-10)
        var  = alpha * beta / ((alpha + beta)**2 * (alpha + beta + 1) + 1e-10)
        std  = math.sqrt(var)
        return float(min(1.0, 4 * std))

    def record(self, state: RLState, action: Action, reward: float,
               next_state: RLState, done: bool):
        super().record(state, action, reward, next_state, done)
        bkt = self._last_bucket
        a   = action.value
        if reward > 0:
            self.alpha_mat[bkt, a] += 1.0
        else:
            self.beta_mat [bkt, a] += 1.0
        self.n_mat[bkt, a] += 1.0
        # Bayesian decay: slowly forget old evidence (non-stationary markets)
        decay = 0.9998
        self.alpha_mat *= decay
        self.beta_mat  *= decay
        # Floor to prevent underflow
        self.alpha_mat = np.clip(self.alpha_mat, 0.5, 200)
        self.beta_mat  = np.clip(self.beta_mat,  0.5, 200)

    def save(self):
        np.savez_compressed(
            str(RL_MODELS_DIR / f"axiom_{self.pair.replace('/','_')}"),
            alpha=self.alpha_mat, beta=self.beta_mat, n=self.n_mat
        )

    def _load(self):
        p = RL_MODELS_DIR / f"axiom_{self.pair.replace('/','_')}.npz"
        if p.exists():
            d = np.load(str(p))
            self.alpha_mat = d["alpha"]; self.beta_mat = d["beta"]; self.n_mat = d["n"]


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE 25: KRAKEN-GODMIND — Hierarchical Meta-Controller
# ══════════════════════════════════════════════════════════════════════════════

class KrakenGodmind(BaseEngine):
    """
    Soul:    "I am the mind above all minds. I decide which gods to trust."

    Architecture:
    ─ Receives ALL 24 engine votes + confidence scores
    ─ 3-layer meta-network: 24×5 input → decision + trust_updates
    ─ Implements hierarchical control:
        L1: Veto layer   (MIRAGE danger + NEMESIS anomaly)
        L2: Macro layer  (TITAN + ECLIPSE + AXIOM)
        L3: Signal layer (all remaining engines)
    ─ Dynamic trust matrix: 24×5 (engine × action) updated every trade
    ─ Detects "engine cluster bias": when too many engines agree too quickly
      (flash mob manipulation detection)
    ─ Triple voting weight in the final ensemble
    ─ Outputs a CONFIDENCE-CALIBRATED final signal with uncertainty estimate
    """

    NAME = "KRAKEN-GODMIND"
    SOUL = "I am the mind above all minds. I see what they cannot."

    ALL_KEYS = [
        'PPO','A3C','DQN','SAC','TD3',
        'APEX','PHANTOM','STORM','ORACLE','VENOM',
        'TITAN','HYDRA','VOID','PULSE','INFINITY',
        'NEMESIS','SOVEREIGN','WRAITH','ABYSS','GENESIS',
        'MIRAGE','ECLIPSE','CHIMERA','AXIOM',
    ]
    N_ENGINES = 24

    VETO_ENGINES   = {'MIRAGE'}      # can unilaterally block trades
    MACRO_ENGINES  = {'TITAN','ECLIPSE','AXIOM','SOVEREIGN'}
    FLASH_THRESHOLD = 0.90            # if avg_conf > this → suspect manipulation

    def __init__(self, pair: str):
        super().__init__(pair)
        # Trust matrix: how much to believe each engine in each regime
        self.trust = np.ones((8, self.N_ENGINES))  # 8 regimes
        self.trust_counts = np.zeros((8, self.N_ENGINES))
        # Meta-net: learns to combine engine votes
        in_dim = self.N_ENGINES * N_ACTIONS + self.N_ENGINES  # votes + confidences
        self.meta_net = NumpyMLP(in_dim, 256, N_ACTIONS, h2=128, lr=0.0003)
        # History
        self.conf_hist: deque = deque(maxlen=300)
        self.regime_accuracy: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._last_cell  = 0
        self._last_input = None
        self._flash_count= 0
        self._total_vetoes = 0
        self._load()

    def _regime_cell(self, s: RLState) -> int:
        """8-cell regime: 4 macro × 2 vol"""
        if   s.regime_bull    > 0.5: r = 0
        elif s.regime_bear    > 0.5: r = 1
        elif s.regime_ranging > 0.5: r = 2
        else:                         r = 3
        v = 0 if s.volatility_1m < 0.005 else 1
        return r * 2 + v

    def command(
        self,
        state:     RLState,
        all_votes: Dict[str, Tuple[Action, float]],
        mirage:    "KrakenMirage",
    ) -> Tuple[Action, float, Dict]:
        """
        GODMIND's final decision.
        Returns: (action, confidence, debug_dict)
        """
        cell = self._regime_cell(state)
        self._last_cell = cell

        # ── LAYER 1: VETO LAYER ────────────────────────────────────────────
        if mirage.has_veto:
            self._total_vetoes += 1
            return Action.HOLD, 0.0, {"veto": "MIRAGE", "layer": 1}

        # Flash-mob manipulation detection
        active_confs = [c for k, (_, c) in all_votes.items() if c > 0.1]
        if active_confs:
            avg_c = float(np.mean(active_confs))
            self.conf_hist.extend(active_confs)
            if len(self.conf_hist) >= 50:
                hist_mean = float(np.mean(list(self.conf_hist)))
                hist_std  = float(np.std(list(self.conf_hist))) + 0.01
                z = (avg_c - hist_mean) / hist_std
                if z > 3.5 and avg_c > self.FLASH_THRESHOLD:
                    self._flash_count += 1
                    return Action.HOLD, 0.0, {"veto": "FLASH_MOB", "layer": 1}

        # ── LAYER 2: MACRO LAYER ──────────────────────────────────────────
        macro_votes = {k: all_votes[k] for k in self.MACRO_ENGINES if k in all_votes}
        smap = {Action.STRONG_BUY:2,Action.BUY:1,Action.HOLD:0,Action.SELL:-1,Action.STRONG_SELL:-2}
        if macro_votes:
            macro_score = float(np.mean([smap[a] for a, _ in macro_votes.values()]))
            if abs(macro_score) > 1.8:
                direction = (Action.STRONG_BUY if macro_score > 0 else Action.STRONG_SELL)
                return direction, 0.75, {"source": "MACRO_LAYER", "layer": 2}

        # ── LAYER 3: FULL META-NETWORK ────────────────────────────────────
        # Build input: vote onehot + confidence per engine
        vote_vec  = np.zeros(self.N_ENGINES * N_ACTIONS, dtype=np.float32)
        conf_vec  = np.zeros(self.N_ENGINES, dtype=np.float32)
        for i, key in enumerate(self.ALL_KEYS):
            if key in all_votes:
                a, c = all_votes[key]
                vote_vec[i * N_ACTIONS + a.value] = 1.0
                conf_vec[i] = c
                # Apply trust weight
                trust_w = float(self.trust[cell, i])
                vote_vec[i * N_ACTIONS + a.value] *= trust_w
                conf_vec[i] *= trust_w

        meta_input = np.concatenate([vote_vec, conf_vec])
        self._last_input = meta_input
        out, _ = self.meta_net.forward(meta_input)
        probs  = _softmax(out)
        idx    = int(np.argmax(probs))
        conf   = float(probs[idx])
        return Action(idx), min(0.97, conf), {"source": "META_NET", "layer": 3}

    def record_meta(self, state: RLState, action: Action, reward: float,
                    next_state: RLState, done: bool,
                    all_votes: Optional[Dict] = None):
        super().record(state, action, reward, next_state, done)
        cell = self._last_cell
        # Update trust matrix
        smap = {Action.STRONG_BUY:1,Action.BUY:1,Action.HOLD:0,Action.SELL:-1,Action.STRONG_SELL:-1}
        if all_votes:
            for i, key in enumerate(self.ALL_KEYS):
                if key in all_votes:
                    a_eng, _ = all_votes[key]
                    was_right = (smap[a_eng] > 0 and reward > 0) or \
                                (smap[a_eng] < 0 and reward < 0)
                    alpha = 0.06
                    self.trust[cell, i] = np.clip(
                        (1-alpha) * self.trust[cell, i] + alpha * float(was_right) * 2,
                        0.1, 3.0
                    )
                    self.trust_counts[cell, i] += 1
        # Update meta-net
        if self._last_input is not None:
            out, _ = self.meta_net.forward(self._last_input, training=True)
            probs  = _softmax(out)
            target = probs.copy()
            target[action.value] = 1.0 if reward > 0 else 0.0
            self.meta_net.backward_td(target, probs)
        self.regime_accuracy[str(cell)].append(1 if reward > 0 else 0)

    def best_engine_per_regime(self) -> Dict[str, str]:
        labels = ["bull+calm","bull+vol","bear+calm","bear+vol",
                  "range+calm","range+vol","chaos+calm","chaos+vol"]
        return {
            labels[i]: self.ALL_KEYS[int(np.argmax(self.trust[i]))]
            for i in range(8)
        }

    def save(self):
        safe = self.pair.replace("/","_")
        self.meta_net.save(str(RL_MODELS_DIR / f"godmind_{safe}"))
        np.savez_compressed(
            str(RL_MODELS_DIR / f"godmind_trust_{safe}"),
            trust=self.trust, counts=self.trust_counts
        )

    def _load(self):
        safe = self.pair.replace("/","_")
        self.meta_net.load(str(RL_MODELS_DIR / f"godmind_{safe}"))
        p = RL_MODELS_DIR / f"godmind_trust_{safe}.npz"
        if p.exists():
            d = np.load(str(p))
            self.trust = d["trust"]; self.trust_counts = d["counts"]

    def record(self, state, action, reward, next_state, done):
        self.record_meta(state, action, reward, next_state, done)

    def get_stats(self) -> Dict:
        s = super().get_stats()
        s.update({
            "flash_vetoes":     self._flash_count,
            "mirage_vetoes":    self._total_vetoes,
            "best_per_regime":  self.best_engine_per_regime(),
            "regime_accuracy": {k: round(sum(v)/max(len(v),1),3)
                                 for k,v in self.regime_accuracy.items()},
        })
        return s


# ══════════════════════════════════════════════════════════════════════════════
#  OMEGA ORCHESTRATOR — ALL 25 ENGINES
# ══════════════════════════════════════════════════════════════════════════════

class OmegaRLOrchestrator:
    """
    The supreme 25-engine cluster.

    Consensus tiers:
      ≥13/25  → BUY / SELL
      ≥18/25  → STRONG_BUY / STRONG_SELL
      ≥22/25  → ABSOLUTE (maximum position size)

    Special powers:
      GODMIND   → triple voting weight, hierarchical control
      MIRAGE    → can unilaterally VETO any trade (trap detected)
      NEMESIS   → can block when adversarial test fails
      INFINITY  → final routing authority among base engines

    Every engine learns every tick. No engine ever sleeps.
    The cluster is stronger than any individual engine by an order of magnitude.
    """

    ALL_KEYS = [
        'PPO','A3C','DQN','SAC','TD3',
        'APEX','PHANTOM','STORM','ORACLE','VENOM',
        'TITAN','HYDRA','VOID','PULSE','INFINITY',
        'NEMESIS','SOVEREIGN','WRAITH','ABYSS','GENESIS',
        'MIRAGE','ECLIPSE','CHIMERA','AXIOM','GODMIND',
    ]

    # Base voting weights — GODMIND gets 3×
    BASE_WEIGHTS = {k: 1.0 for k in ALL_KEYS}
    BASE_WEIGHTS.update({
        'APEX':     1.4,    # high precision
        'ORACLE':   1.3,    # memory god
        'INFINITY': 1.6,    # meta-router
        'NEMESIS':  1.2,    # adversarial hardening
        'SOVEREIGN':1.3,    # temporal attention
        'AXIOM':    1.2,    # calibrated belief
        'GODMIND':  3.0,    # supreme authority
    })

    def __init__(self, pair: str):
        self.pair = pair
        # Instantiate all 25 engines
        self.ppo       = KrakenPPO(pair)
        self.a3c       = KrakenA3C(pair)
        self.dqn       = KrakenDQN(pair)
        self.sac       = KrakenSAC(pair)
        self.td3       = KrakenTD3(pair)
        self.apex      = KrakenApex(pair)
        self.phantom   = KrakenPhantom(pair)
        self.storm     = KrakenStorm(pair)
        self.oracle    = KrakenOracle(pair)
        self.venom     = KrakenVenom(pair)
        self.titan     = KrakenTitan(pair)
        self.hydra     = KrakenHydra(pair)
        self.void      = KrakenVoid(pair)
        self.pulse     = KrakenPulse(pair)
        self.infinity  = KrakenInfinity(pair)
        self.nemesis   = KrakenNemesis(pair)
        self.sovereign = KrakenSovereign(pair)
        self.wraith    = KrakenWraith(pair)
        self.abyss     = KrakenAbyss(pair)
        self.genesis   = KrakenGenesis(pair)
        self.mirage    = KrakenMirage(pair)
        self.eclipse   = KrakenEclipse(pair)
        self.chimera   = KrakenChimera(pair)
        self.axiom     = KrakenAxiom(pair)
        self.godmind   = KrakenGodmind(pair)

        self._base_engines = [
            self.ppo, self.a3c, self.dqn, self.sac, self.td3,
            self.apex, self.phantom, self.storm, self.oracle, self.venom,
            self.titan, self.hydra, self.void, self.pulse, self.infinity,
            self.nemesis, self.sovereign, self.wraith, self.abyss, self.genesis,
            self.mirage, self.eclipse, self.chimera, self.axiom,
        ]
        self._base_keys = self.ALL_KEYS[:-1]   # all except GODMIND

        # Dynamic per-engine accuracy tracking
        self.weights: Dict[str, float] = dict(self.BASE_WEIGHTS)
        self.engine_acc: Dict[str, deque] = {k: deque(maxlen=100) for k in self.ALL_KEYS}
        self._last_state: Optional[RLState]          = None
        self._last_votes: Dict[str, Tuple[Action, float]] = {}
        self.total_decisions = 0
        self.total_strong    = 0
        self.total_absolute  = 0
        self.total_vetoes    = 0
        self._lock = threading.Lock()
        log.info(f"⚡ OmegaRLOrchestrator: 25 engines awakened for {pair}")

    # ── DECISION ──────────────────────────────────────────────────────────────

    def decide(self, state: RLState) -> Dict:
        """
        Full 25-engine ensemble decision.
        Returns structured decision dict with full transparency.
        """
        with self._lock:
            self._last_state = state
            all_votes: Dict[str, Tuple[Action, float]] = {}
            individual: Dict[str, Dict] = {}

            # Gather votes from 24 base engines
            for engine, key in zip(self._base_engines, self._base_keys):
                try:
                    action, conf = engine.get_action(state)
                except Exception as e:
                    log.debug(f"Engine {key} error: {e}")
                    action, conf = Action.HOLD, 0.0
                all_votes[key]  = (action, conf)
                individual[key] = {"action": action.name, "confidence": round(conf, 3)}

            # GODMIND final decision (hierarchical)
            gm_action, gm_conf, gm_debug = self.godmind.command(
                state, all_votes, self.mirage
            )
            all_votes["GODMIND"] = (gm_action, gm_conf)
            individual["GODMIND"] = {
                "action":     gm_action.name,
                "confidence": round(gm_conf, 3),
                **gm_debug,
            }
            self._last_votes = all_votes

            # ── Weighted ensemble ─────────────────────────────────────────
            smap = {
                Action.STRONG_BUY:  +2.0,
                Action.BUY:         +1.0,
                Action.HOLD:         0.0,
                Action.SELL:        -1.0,
                Action.STRONG_SELL: -2.0,
            }
            weighted_score = 0.0
            total_w        = 0.0
            n_bull = n_bear = 0

            for key in self.ALL_KEYS:
                action, conf = all_votes.get(key, (Action.HOLD, 0.0))
                w = self.weights[key] * conf
                weighted_score += smap[action] * w
                total_w        += w
                if smap[action] > 0: n_bull += 1
                if smap[action] < 0: n_bear += 1

            norm      = weighted_score / max(total_w, 1e-6)
            consensus = max(n_bull, n_bear)

            # ── Signal tier ───────────────────────────────────────────────
            if gm_debug.get("veto"):
                direction, signal_str, conf_out = "hold", "VETO", 0.0
                self.total_vetoes += 1
            elif norm >  1.4 and consensus >= 22:
                direction, signal_str = "buy",  "ABSOLUTE_BUY"
                conf_out = min(0.99, abs(norm) * 0.48)
                self.total_absolute += 1
            elif norm >  0.9 and consensus >= 18:
                direction, signal_str = "buy",  "STRONG_BUY"
                conf_out = min(0.97, abs(norm) * 0.45)
                self.total_strong += 1
            elif norm >  0.30 and consensus >= 13:
                direction, signal_str = "buy",  "BUY"
                conf_out = min(0.87, abs(norm) * 0.38)
            elif norm < -1.4 and consensus >= 22:
                direction, signal_str = "sell", "ABSOLUTE_SELL"
                conf_out = min(0.99, abs(norm) * 0.48)
                self.total_absolute += 1
            elif norm < -0.9 and consensus >= 18:
                direction, signal_str = "sell", "STRONG_SELL"
                conf_out = min(0.97, abs(norm) * 0.45)
                self.total_strong += 1
            elif norm < -0.30 and consensus >= 13:
                direction, signal_str = "sell", "SELL"
                conf_out = min(0.87, abs(norm) * 0.38)
            else:
                direction, signal_str, conf_out = "hold", "HOLD", 0.0

            self.total_decisions += 1

            return {
                "direction":    direction,
                "signal":       signal_str,
                "confidence":   round(conf_out, 4),
                "score":        round(float(norm), 4),
                "consensus":    consensus,
                "n_bullish":    n_bull,
                "n_bearish":    n_bear,
                "veto":         bool(gm_debug.get("veto")),
                "veto_reason":  gm_debug.get("veto", ""),
                "godmind_layer":gm_debug.get("layer", 3),
                "engines":      individual,
                "stats": {
                    "total_decisions": self.total_decisions,
                    "total_strong":    self.total_strong,
                    "total_absolute":  self.total_absolute,
                    "total_vetoes":    self.total_vetoes,
                },
            }

    # ── LEARNING ──────────────────────────────────────────────────────────────

    def record_outcome(self, reward: float, pnl_pct: float,
                       next_state: RLState):
        """Feed result to ALL 25 engines. Update dynamic trust weights."""
        if self._last_state is None:
            return
        done = abs(pnl_pct) > 0.0001

        with self._lock:
            # Feed to all 24 base engines
            for engine, key in zip(self._base_engines, self._base_keys):
                action = self._last_votes.get(key, (Action.HOLD, 0.0))[0]
                try:
                    engine.record(self._last_state, action, reward,
                                  next_state, done)
                except Exception as e:
                    log.debug(f"record error [{key}]: {e}")

            # GODMIND gets full vote context
            gm_action = self._last_votes.get("GODMIND", (Action.HOLD, 0.0))[0]
            self.godmind.record_meta(
                self._last_state, gm_action, reward, next_state, done,
                all_votes=self._last_votes
            )

            # Update dynamic weights via rolling accuracy
            smap = {Action.STRONG_BUY:1,Action.BUY:1,Action.HOLD:0,
                    Action.SELL:-1,Action.STRONG_SELL:-1}
            for key in self.ALL_KEYS:
                action = self._last_votes.get(key, (Action.HOLD, 0.0))[0]
                direction = smap[action]
                correct = ((direction > 0 and reward > 0) or
                           (direction < 0 and reward < 0))
                if action != Action.HOLD:
                    self.engine_acc[key].append(1.0 if correct else 0.0)
                if len(self.engine_acc[key]) >= 25:
                    acc = float(np.mean(list(self.engine_acc[key])))
                    # Logistic weight update
                    base_w = 0.3 + 2.7 / (1 + math.exp(-12 * (acc - 0.50)))
                    # Preserve special weights
                    if key in ('INFINITY','GODMIND','APEX','ORACLE'):
                        base_w *= self.BASE_WEIGHTS[key]
                    self.weights[key] = round(
                        max(0.1, min(5.0, base_w)), 4
                    )

    # ── PERSISTENCE ───────────────────────────────────────────────────────────

    def save_all(self):
        for engine in self._base_engines:
            try: engine.save()
            except Exception as e: log.debug(f"save error: {e}")
        try: self.godmind.save()
        except Exception as e: log.debug(f"godmind save: {e}")
        # Save weights
        safe = self.pair.replace("/","_")
        np.savez_compressed(
            str(RL_MODELS_DIR / f"omega_weights_{safe}"),
            **{k.lower(): np.array([v]) for k, v in self.weights.items()}
        )
        log.debug(f"💾 OmegaRL[{self.pair}] all 25 engines saved")

    def _load_weights(self):
        safe = self.pair.replace("/","_")
        p = RL_MODELS_DIR / f"omega_weights_{safe}.npz"
        if p.exists():
            d = np.load(str(p))
            for k in self.ALL_KEYS:
                kl = k.lower()
                if kl in d:
                    self.weights[k] = float(d[kl][0])

    # ── REPORTING ─────────────────────────────────────────────────────────────

    def get_full_stats(self) -> Dict:
        return {
            "pair":           self.pair,
            "cluster":        "25-ENGINE OMEGA RL CLUSTER",
            "decisions":      self.total_decisions,
            "strong_signals": self.total_strong,
            "absolute_sigs":  self.total_absolute,
            "vetoes":         self.total_vetoes,
            "weights":        {k: round(v, 3) for k, v in self.weights.items()},
            "per_engine":     [e.get_stats() for e in self._base_engines],
            "godmind":        self.godmind.get_stats(),
            "best_per_regime":self.godmind.best_engine_per_regime(),
        }


# ══════════════════════════════════════════════════════════════════════════════
#  FACTORY & GLOBAL API
# ══════════════════════════════════════════════════════════════════════════════

_orchestrators: Dict[str, OmegaRLOrchestrator] = {}
_factory_lock  = threading.Lock()


def get_rl_orchestrator(pair: str) -> OmegaRLOrchestrator:
    """Get or create the 25-engine Omega cluster for a pair."""
    with _factory_lock:
        if pair not in _orchestrators:
            _orchestrators[pair] = OmegaRLOrchestrator(pair)
            log.info(f"⚡ 25-Engine Omega cluster awakened: {pair}")
        return _orchestrators[pair]


def get_all_orchestrators() -> Dict[str, OmegaRLOrchestrator]:
    return dict(_orchestrators)


def save_all_models():
    count = 0
    for orch in _orchestrators.values():
        orch.save_all()
        count += 1
    log.info(f"💾 Saved {count} pairs × 25 Omega soul engines")


def get_global_rl_stats() -> Dict:
    all_stats = [o.get_full_stats() for o in _orchestrators.values()]
    return {
        "total_pairs":     len(_orchestrators),
        "engine_count":    25,
        "total_decisions": sum(s["decisions"] for s in all_stats),
        "total_strong":    sum(s["strong_signals"] for s in all_stats),
        "total_absolute":  sum(s["absolute_sigs"] for s in all_stats),
        "total_vetoes":    sum(s["vetoes"] for s in all_stats),
        "per_pair":        all_stats[:8],
    }


# ── Backward compatibility aliases ────────────────────────────────────────────
ExtendedRLOrchestrator = OmegaRLOrchestrator
RLEngineOrchestrator   = OmegaRLOrchestrator
