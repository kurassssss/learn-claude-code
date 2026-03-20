import { Router } from "express";
import { db } from "@workspace/db";
import {
  enginesTable, botsTable, mutationsTable, eliteStrategiesTable,
  errorsTable, healActionsTable, telegramConfigTable
} from "@workspace/db";
import { eq, desc, and, sql } from "drizzle-orm";
import { v4 as uuid } from "uuid";

export const swarmRouter = Router();

const ENGINE_DEFS = [
  { n: 1,  name: "KRAKEN-PPO",       type: "PPO",         desc: "Proximal Policy Optimisation, clipped surrogate",             veto: false, vetoW: 1.0  },
  { n: 2,  name: "KRAKEN-A3C",       type: "A3C",         desc: "Async Advantage Actor-Critic, entropy bonus",                 veto: false, vetoW: 1.0  },
  { n: 3,  name: "KRAKEN-DQN",       type: "DQN",         desc: "Double DQN, PER, breakout specialist",                       veto: false, vetoW: 1.0  },
  { n: 4,  name: "KRAKEN-SAC",       type: "SAC",         desc: "Soft Actor-Critic, max-entropy exploration",                  veto: false, vetoW: 1.0  },
  { n: 5,  name: "KRAKEN-TD3",       type: "TD3",         desc: "Twin Delayed DDPG, anti-overestimation",                     veto: false, vetoW: 1.0  },
  { n: 6,  name: "KRAKEN-APEX",      type: "BANDIT",      desc: "UCB1 Bandit, 92% patience gate, kill-shot only",             veto: false, vetoW: 1.0  },
  { n: 7,  name: "KRAKEN-PHANTOM",   type: "MICROSTRUCTURE", desc: "VPIN microstructure, Lee-Ready delta, toxic flow",         veto: false, vetoW: 1.0  },
  { n: 8,  name: "KRAKEN-STORM",     type: "ES",          desc: "Evolution Strategy, grows in chaos",                         veto: false, vetoW: 1.0  },
  { n: 9,  name: "KRAKEN-ORACLE",    type: "EPISODIC",    desc: "Episodic memory, pattern DNA matching",                      veto: false, vetoW: 1.0  },
  { n: 10, name: "KRAKEN-VENOM",     type: "CONTRARIAN",  desc: "Contrarian, profits from crowd panic",                       veto: false, vetoW: 1.0  },
  { n: 11, name: "KRAKEN-TITAN",     type: "MACRO",       desc: "Cross-pair PCA macro, veto authority",                       veto: true,  vetoW: 2.0  },
  { n: 12, name: "KRAKEN-HYDRA",     type: "ENSEMBLE",    desc: "9-head internal ensemble, knowledge distillation",           veto: false, vetoW: 1.0  },
  { n: 13, name: "KRAKEN-VOID",      type: "FEW-SHOT",    desc: "Prototypical few-shot, 10-trade cold start",                 veto: false, vetoW: 1.0  },
  { n: 14, name: "KRAKEN-PULSE",     type: "FOURIER",     desc: "Fourier cycle detection, harmonic resonance",                veto: false, vetoW: 1.0  },
  { n: 15, name: "KRAKEN-INFINITY",  type: "META",        desc: "Meta-router, anomaly veto, supreme authority",               veto: true,  vetoW: 2.0  },
  { n: 16, name: "KRAKEN-NEMESIS",   type: "ADVERSARIAL", desc: "Adversarial self-play, exploits own weaknesses",             veto: true,  vetoW: 2.0  },
  { n: 17, name: "KRAKEN-SOVEREIGN", type: "TRANSFORMER", desc: "Transformer attention over state sequences",                 veto: false, vetoW: 1.0  },
  { n: 18, name: "KRAKEN-WRAITH",    type: "STAT-ARB",    desc: "Cross-pair stat-arb, cointegration hunter",                  veto: false, vetoW: 1.0  },
  { n: 19, name: "KRAKEN-ABYSS",     type: "DUELING",     desc: "Dueling Noisy DQN with distributional RL (C51)",             veto: false, vetoW: 1.0  },
  { n: 20, name: "KRAKEN-GENESIS",   type: "GENETIC",     desc: "Genetic algorithm policy evolution",                         veto: false, vetoW: 1.0  },
  { n: 21, name: "KRAKEN-MIRAGE",    type: "DETECTOR",    desc: "Illusion detector — spots fake signals & traps",             veto: false, vetoW: 1.0  },
  { n: 22, name: "KRAKEN-ECLIPSE",   type: "MTF",         desc: "Multi-timeframe cascade, 1m→4h confluence",                  veto: false, vetoW: 1.0  },
  { n: 23, name: "KRAKEN-CHIMERA",   type: "HYBRID",      desc: "Hybrid rule+neural blend, regime-switched",                  veto: false, vetoW: 1.0  },
  { n: 24, name: "KRAKEN-AXIOM",     type: "BAYESIAN",    desc: "Pure Bayesian inference, calibrated uncertainty",            veto: false, vetoW: 1.0  },
  { n: 25, name: "KRAKEN-GODMIND",   type: "HIERARCHICAL",desc: "Hierarchical meta-controller of all 24 engines",             veto: true,  vetoW: 3.0  },
];

const BOT_STRATEGIES = [
  "PPO-Scalp", "SAC-Momentum", "DQN-Breakout", "A3C-Trend",
  "TD3-MeanRev", "APEX-KillShot", "PHANTOM-Microstr", "STORM-Chaos",
  "ORACLE-Pattern", "VENOM-Contrarian", "TITAN-Macro", "HYDRA-Ensemble",
  "VOID-FewShot", "PULSE-Fourier", "INFINITY-Meta", "NEMESIS-Adversarial",
  "SOVEREIGN-Attn", "WRAITH-StatArb", "ABYSS-Distrib", "GENESIS-Genetic",
];

async function seedEngines() {
  const existing = await db.select().from(enginesTable).limit(1);
  if (existing.length > 0) return;

  const actions = ["HOLD", "BUY", "SELL", "STRONG_BUY", "STRONG_SELL"];
  for (const e of ENGINE_DEFS) {
    await db.insert(enginesTable).values({
      id: `engine-${e.n.toString().padStart(2, "0")}`,
      engineNumber: e.n,
      name: e.name,
      type: e.type,
      description: e.desc,
      status: Math.random() > 0.1 ? "active" : "training",
      winRate: 0.45 + Math.random() * 0.2,
      totalReward: (Math.random() - 0.3) * 500,
      epsilon: 0.005 + Math.random() * 0.15,
      learningRate: 0.0005 + Math.random() * 0.002,
      gamma: 0.99 + Math.random() * 0.009,
      batchSize: [32, 64, 128][Math.floor(Math.random() * 3)],
      bufferCap: 100000 + Math.floor(Math.random() * 100000),
      entropyBonus: Math.random() * 0.05,
      explorationRate: 0.01 + Math.random() * 0.15,
      lastAction: actions[Math.floor(Math.random() * actions.length)],
      tradesCount: Math.floor(Math.random() * 5000),
      hasVeto: e.veto,
      vetoWeight: e.vetoW,
    }).onConflictDoNothing();
  }
}

async function seedBots() {
  const existing = await db.select().from(botsTable).limit(1);
  if (existing.length > 0) return;

  const statuses: Array<"active"|"idle"|"crashed"|"healing"> = ["active", "active", "active", "idle", "crashed", "healing"];
  for (let i = 1; i <= 1000; i++) {
    const pnl = (Math.random() - 0.35) * 200;
    await db.insert(botsTable).values({
      id: `bot-${i.toString().padStart(4, "0")}`,
      name: `NEXUS-BOT-${i.toString().padStart(4, "0")}`,
      status: statuses[Math.floor(Math.random() * statuses.length)],
      pnl,
      winRate: 0.4 + Math.random() * 0.25,
      trades: Math.floor(Math.random() * 500),
      strategy: BOT_STRATEGIES[Math.floor(Math.random() * BOT_STRATEGIES.length)],
      healthScore: 60 + Math.random() * 40,
      generation: Math.floor(1 + Math.random() * 10),
      fitness: Math.random(),
    }).onConflictDoNothing();
  }
}

async function seedElites() {
  const existing = await db.select().from(eliteStrategiesTable).limit(1);
  if (existing.length > 0) return;

  const regimes = ["bull", "bear", "ranging", "volatile", "crash"];
  const niches = ["high-vol", "low-vol", "trending", "mean-rev", "breakout"];
  for (let i = 0; i < 20; i++) {
    await db.insert(eliteStrategiesTable).values({
      id: uuid(),
      niche: niches[i % niches.length],
      regime: regimes[i % regimes.length],
      fitness: 0.6 + Math.random() * 0.4,
      params: {
        learningRate: 0.0005 + Math.random() * 0.002,
        gamma: 0.99 + Math.random() * 0.009,
        epsilon: 0.005 + Math.random() * 0.1,
        entropyBonus: Math.random() * 0.05,
      },
      timesSelected: Math.floor(Math.random() * 100),
    }).onConflictDoNothing();
  }
}

async function seedErrors() {
  const existing = await db.select().from(errorsTable).limit(1);
  if (existing.length > 0) return;

  const categories = ["network", "api", "trading", "system", "module_crash"];
  const severities = ["WARNING", "ERROR", "CRITICAL"];
  const modules = ["nexus_swarm", "kraken_god", "rl_engines", "self_healing_engine", "nexus_prime_brain"];
  const msgs = [
    "WebSocket disconnected unexpectedly",
    "Rate limit exceeded on Bybit API",
    "NaN value detected in RL state vector",
    "Module crashed after 3 retries",
    "Memory pressure: buffer at 95% capacity",
    "Order rejected: insufficient margin",
    "Consensus timeout: engines not responding",
  ];

  for (let i = 0; i < 30; i++) {
    const resolved = Math.random() > 0.4;
    await db.insert(errorsTable).values({
      id: uuid(),
      ts: Date.now() / 1000 - Math.random() * 86400,
      message: msgs[Math.floor(Math.random() * msgs.length)],
      category: categories[Math.floor(Math.random() * categories.length)],
      severity: severities[Math.floor(Math.random() * severities.length)],
      module: modules[Math.floor(Math.random() * modules.length)],
      resolved,
      fixAttempts: Math.floor(Math.random() * 4),
      resolutionBy: resolved ? "OmegaAutoFixer" : "",
    }).onConflictDoNothing();
  }
}

async function seedTelegram() {
  const existing = await db.select().from(telegramConfigTable).limit(1);
  if (existing.length > 0) return;

  await db.insert(telegramConfigTable).values({
    enabled: false,
    chatId: "",
    botToken: "",
    notifyOnConsensus: true,
    notifyOnError: true,
    notifyOnMutation: false,
    notifyOnHeal: true,
    minSeverity: "WARNING",
  });
}

// Seed on first request
let seeded = false;
async function ensureSeeded() {
  if (seeded) return;
  seeded = true;
  await Promise.all([seedEngines(), seedBots(), seedElites(), seedErrors(), seedTelegram()]);
}

// ── Swarm Status ──────────────────────────────────────────────────────────────

swarmRouter.get("/swarm/status", async (req, res) => {
  await ensureSeeded();
  const bots = await db.select().from(botsTable);
  const totalPnl = bots.reduce((s, b) => s + b.pnl, 0);
  const totalTrades = bots.reduce((s, b) => s + b.trades, 0);
  const totalWins = bots.reduce((s, b) => s + Math.round(b.trades * b.winRate), 0);

  res.json({
    totalBots: bots.length,
    activeBots: bots.filter(b => b.status === "active").length,
    idleBots: bots.filter(b => b.status === "idle").length,
    crashedBots: bots.filter(b => b.status === "crashed").length,
    healingBots: bots.filter(b => b.status === "healing").length,
    totalPnl: Math.round(totalPnl * 100) / 100,
    winRate: totalTrades > 0 ? Math.round((totalWins / totalTrades) * 10000) / 100 : 0,
    uptime: process.uptime(),
    consensusThreshold: 13,
    activeEngines: 25,
    healthScore: 85 + Math.random() * 10,
    xpLevel: 7,
    totalXp: 14250,
    lastUpdate: new Date().toISOString(),
  });
});

// ── Consensus ─────────────────────────────────────────────────────────────────

swarmRouter.get("/swarm/consensus", async (req, res) => {
  await ensureSeeded();
  const engines = await db.select().from(enginesTable).orderBy(enginesTable.engineNumber);
  const actions = ["STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"];
  const actionWeights = [0.15, 0.25, 0.35, 0.15, 0.10];

  const pick = () => {
    const r = Math.random();
    let acc = 0;
    for (let i = 0; i < actions.length; i++) {
      acc += actionWeights[i];
      if (r < acc) return actions[i];
    }
    return "HOLD";
  };

  const engineVotes = engines.map(e => ({
    engineId: e.id,
    engineName: e.name,
    vote: pick(),
    confidence: 0.4 + Math.random() * 0.55,
    weight: e.vetoWeight,
  }));

  const voteCounts: Record<string, number> = {};
  for (const v of engineVotes) {
    voteCounts[v.vote] = (voteCounts[v.vote] || 0) + v.weight;
  }
  const winAction = Object.entries(voteCounts).sort((a, b) => b[1] - a[1])[0][0];
  const totalWeight = Object.values(voteCounts).reduce((s, v) => s + v, 0);

  res.json({
    action: winAction,
    votes: Math.round(voteCounts[winAction] || 0),
    requiredVotes: 13,
    confidence: Math.round(((voteCounts[winAction] || 0) / totalWeight) * 1000) / 10,
    dominantRegime: ["bull", "bear", "ranging", "volatile"][Math.floor(Math.random() * 4)],
    engineVotes,
    timestamp: new Date().toISOString(),
  });
});

// ── Bots ──────────────────────────────────────────────────────────────────────

swarmRouter.get("/swarm/bots", async (req, res) => {
  await ensureSeeded();
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const offset = Number(req.query.offset) || 0;
  const status = req.query.status as string | undefined;

  let query = db.select().from(botsTable);
  const bots = await (status
    ? db.select().from(botsTable).where(eq(botsTable.status, status)).limit(limit).offset(offset)
    : db.select().from(botsTable).limit(limit).offset(offset));

  const total = (await db.select({ count: sql<number>`count(*)` }).from(botsTable))[0].count;

  res.json({
    bots: bots.map(b => ({
      id: b.id,
      name: b.name,
      status: b.status,
      pnl: Math.round(b.pnl * 100) / 100,
      winRate: Math.round(b.winRate * 10000) / 100,
      trades: b.trades,
      strategy: b.strategy,
      healthScore: Math.round(b.healthScore * 10) / 10,
      generation: b.generation,
      fitness: Math.round(b.fitness * 10000) / 10000,
      lastActivity: b.lastActivity.toISOString(),
    })),
    total: Number(total),
    offset,
    limit,
  });
});

// ── Engines ───────────────────────────────────────────────────────────────────

swarmRouter.get("/engines", async (req, res) => {
  await ensureSeeded();
  const engines = await db.select().from(enginesTable).orderBy(enginesTable.engineNumber);
  res.json({
    engines: engines.map(e => ({
      ...e,
      lastActionTs: e.lastActionTs.toISOString(),
      createdAt: undefined,
      updatedAt: undefined,
    })),
    total: engines.length,
  });
});

swarmRouter.get("/engines/:engineId", async (req, res) => {
  await ensureSeeded();
  const [engine] = await db.select().from(enginesTable).where(eq(enginesTable.id, req.params.engineId));
  if (!engine) { res.status(404).json({ error: "Not found" }); return; }
  res.json({ ...engine, lastActionTs: engine.lastActionTs.toISOString(), createdAt: undefined, updatedAt: undefined });
});

swarmRouter.patch("/engines/:engineId", async (req, res) => {
  await ensureSeeded();
  const body = req.body;
  const update: Partial<typeof enginesTable.$inferInsert> = {};
  if (body.learningRate !== undefined) update.learningRate = body.learningRate;
  if (body.gamma !== undefined) update.gamma = body.gamma;
  if (body.epsilon !== undefined) update.epsilon = body.epsilon;
  if (body.batchSize !== undefined) update.batchSize = body.batchSize;
  if (body.bufferCap !== undefined) update.bufferCap = body.bufferCap;
  if (body.entropyBonus !== undefined) update.entropyBonus = body.entropyBonus;
  if (body.explorationRate !== undefined) update.explorationRate = body.explorationRate;

  await db.update(enginesTable).set(update).where(eq(enginesTable.id, req.params.engineId));
  const [updated] = await db.select().from(enginesTable).where(eq(enginesTable.id, req.params.engineId));
  if (!updated) { res.status(404).json({ error: "Not found" }); return; }
  res.json({ ...updated, lastActionTs: updated.lastActionTs.toISOString(), createdAt: undefined, updatedAt: undefined });
});

swarmRouter.post("/engines/:engineId/reset", async (req, res) => {
  await ensureSeeded();
  const def = ENGINE_DEFS.find(e => `engine-${e.n.toString().padStart(2, "0")}` === req.params.engineId);
  if (!def) { res.status(404).json({ error: "Not found" }); return; }

  await db.update(enginesTable).set({
    epsilon: 0.20,
    learningRate: 0.001,
    gamma: 0.995,
    batchSize: 64,
    bufferCap: 200000,
    entropyBonus: 0.01,
    explorationRate: 0.2,
    status: "idle",
    totalReward: 0,
    tradesCount: 0,
  }).where(eq(enginesTable.id, req.params.engineId));

  const [updated] = await db.select().from(enginesTable).where(eq(enginesTable.id, req.params.engineId));
  if (!updated) { res.status(404).json({ error: "Not found" }); return; }
  res.json({ ...updated, lastActionTs: updated.lastActionTs.toISOString(), createdAt: undefined, updatedAt: undefined });
});

// ── Mutations ─────────────────────────────────────────────────────────────────

swarmRouter.post("/mutations/run", async (req, res) => {
  await ensureSeeded();
  const { strategy = "genetic", mutationRate = 0.1, targetEngines } = req.body;

  const engines = targetEngines?.length
    ? await db.select().from(enginesTable).where(sql`${enginesTable.id} = ANY(${targetEngines})`)
    : await db.select().from(enginesTable);

  const details: Array<object> = [];
  let improved = 0, degraded = 0, unchanged = 0;

  for (const e of engines) {
    if (Math.random() > mutationRate * 2) { unchanged++; continue; }

    const params = ["learningRate", "gamma", "epsilon", "entropyBonus", "explorationRate"];
    const param = params[Math.floor(Math.random() * params.length)];
    const oldVal = (e as any)[param] as number;
    const delta = (Math.random() - 0.5) * oldVal * 0.3;
    const newVal = Math.max(1e-6, oldVal + delta);
    const fitnessDelta = (Math.random() - 0.45) * 0.1;
    const accepted = fitnessDelta > 0 || Math.random() < 0.2;

    if (accepted) {
      await db.update(enginesTable).set({ [param]: newVal }).where(eq(enginesTable.id, e.id));
      if (fitnessDelta > 0) improved++; else degraded++;
    } else unchanged++;

    details.push({ engineId: e.id, engineName: e.name, paramName: param, oldValue: oldVal, newValue: accepted ? newVal : oldVal, fitnessDelta: Math.round(fitnessDelta * 10000) / 10000, accepted });
  }

  const bestDelta = details.reduce((max: number, d: any) => d.fitnessDelta > max ? d.fitnessDelta : max, -Infinity);
  const avgDelta = details.length ? details.reduce((s: number, d: any) => s + d.fitnessDelta, 0) / details.length : 0;

  const [mut] = await db.insert(mutationsTable).values({
    id: uuid(),
    strategy,
    mutationsApplied: improved + degraded,
    improved,
    degraded,
    unchanged,
    bestFitnessDelta: bestDelta === -Infinity ? 0 : bestDelta,
    averageFitnessDelta: avgDelta,
    details,
  }).returning();

  res.json({
    id: mut.id,
    strategy,
    mutationsApplied: improved + degraded,
    improved,
    degraded,
    unchanged,
    bestFitnessDelta: mut.bestFitnessDelta,
    averageFitnessDelta: mut.averageFitnessDelta,
    timestamp: mut.createdAt.toISOString(),
    details,
  });
});

swarmRouter.get("/mutations/history", async (req, res) => {
  await ensureSeeded();
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const muts = await db.select().from(mutationsTable).orderBy(desc(mutationsTable.createdAt)).limit(limit);
  const total = (await db.select({ count: sql<number>`count(*)` }).from(mutationsTable))[0].count;

  res.json({
    mutations: muts.map(m => ({
      id: m.id,
      strategy: m.strategy,
      mutationsApplied: m.mutationsApplied,
      improved: m.improved,
      degraded: m.degraded,
      unchanged: m.unchanged,
      bestFitnessDelta: m.bestFitnessDelta,
      averageFitnessDelta: m.averageFitnessDelta,
      timestamp: m.createdAt.toISOString(),
      details: m.details,
    })),
    total: Number(total),
  });
});

swarmRouter.get("/mutations/elite", async (req, res) => {
  await ensureSeeded();
  const elites = await db.select().from(eliteStrategiesTable).orderBy(desc(eliteStrategiesTable.fitness));
  res.json({
    elites: elites.map(e => ({
      id: e.id,
      niche: e.niche,
      regime: e.regime,
      fitness: e.fitness,
      params: e.params,
      discoveredAt: e.discoveredAt.toISOString(),
      timesSelected: e.timesSelected,
    })),
    total: elites.length,
  });
});

// ── Healing ───────────────────────────────────────────────────────────────────

swarmRouter.get("/healing/status", async (req, res) => {
  await ensureSeeded();
  const errors = await db.select().from(errorsTable);
  const heals = await db.select().from(healActionsTable);
  const resolved = errors.filter(e => e.resolved).length;
  const successful = heals.filter(h => h.result === "SUCCESS").length;

  res.json({
    xpLevel: 7,
    totalXp: 14250,
    xpToNextLevel: 750,
    totalErrors: errors.length,
    resolvedErrors: resolved,
    unresolvedErrors: errors.length - resolved,
    totalHeals: heals.length,
    successfulHeals: successful,
    healSuccessRate: heals.length > 0 ? Math.round((successful / heals.length) * 10000) / 100 : 0,
    activeCircuitBreakers: Math.floor(Math.random() * 3),
    modulesMonitored: 22,
    lastHealTs: heals.length > 0 ? new Date(heals[heals.length - 1].ts * 1000).toISOString() : new Date().toISOString(),
  });
});

swarmRouter.get("/healing/errors", async (req, res) => {
  await ensureSeeded();
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const resolvedFilter = req.query.resolved;
  const errors = resolvedFilter !== undefined
    ? await db.select().from(errorsTable).where(eq(errorsTable.resolved, resolvedFilter === "true")).orderBy(desc(errorsTable.createdAt)).limit(limit)
    : await db.select().from(errorsTable).orderBy(desc(errorsTable.createdAt)).limit(limit);
  const total = (await db.select({ count: sql<number>`count(*)` }).from(errorsTable))[0].count;

  res.json({
    errors: errors.map(e => ({
      id: e.id,
      ts: e.ts,
      message: e.message,
      category: e.category,
      severity: e.severity,
      module: e.module,
      resolved: e.resolved,
      fixAttempts: e.fixAttempts,
      resolutionBy: e.resolutionBy,
    })),
    total: Number(total),
  });
});

swarmRouter.get("/healing/actions", async (req, res) => {
  await ensureSeeded();
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const actions = await db.select().from(healActionsTable).orderBy(desc(healActionsTable.createdAt)).limit(limit);
  const total = (await db.select({ count: sql<number>`count(*)` }).from(healActionsTable))[0].count;

  res.json({
    actions: actions.map(a => ({
      id: a.id,
      ts: a.ts,
      errorId: a.errorId,
      strategy: a.strategy,
      module: a.module,
      result: a.result,
      durationS: a.durationS,
      details: a.details,
      xpEarned: a.xpEarned,
    })),
    total: Number(total),
  });
});

swarmRouter.post("/healing/trigger", async (req, res) => {
  await ensureSeeded();
  const { module: moduleName, errorId, strategy = "OmegaAutoFixer" } = req.body;

  const strategies = ["AST_REPAIR", "MODULE_RESTART", "DEPENDENCY_INJECT", "CONFIG_RESET", "ROLLBACK"];
  const chosenStrategy = strategy !== "OmegaAutoFixer" ? strategy : strategies[Math.floor(Math.random() * strategies.length)];
  const results = ["SUCCESS", "SUCCESS", "SUCCESS", "PARTIAL", "FAILED"];
  const result = results[Math.floor(Math.random() * results.length)];
  const xpEarned = result === "SUCCESS" ? 50 + Math.floor(Math.random() * 50) : result === "PARTIAL" ? 10 : 0;

  const eId = errorId || (await db.select().from(errorsTable).orderBy(desc(errorsTable.createdAt)).limit(1))[0]?.id || uuid();

  const [action] = await db.insert(healActionsTable).values({
    id: uuid(),
    ts: Date.now() / 1000,
    errorId: eId,
    strategy: chosenStrategy,
    module: moduleName,
    result,
    durationS: 0.5 + Math.random() * 2.5,
    details: `${chosenStrategy} applied to ${moduleName}: ${result}`,
    xpEarned,
  }).returning();

  if (result === "SUCCESS" && errorId) {
    await db.update(errorsTable).set({ resolved: true, resolutionBy: "OmegaAutoFixer" }).where(eq(errorsTable.id, errorId));
  }

  res.json({
    result: action.result,
    strategy: action.strategy,
    xpEarned: action.xpEarned,
    details: action.details,
  });
});

// ── Telegram ──────────────────────────────────────────────────────────────────

swarmRouter.get("/telegram/config", async (req, res) => {
  await ensureSeeded();
  const [config] = await db.select().from(telegramConfigTable).limit(1);
  res.json({
    enabled: config.enabled,
    chatId: config.chatId,
    botToken: config.botToken ? "****" : "",
    notifyOnConsensus: config.notifyOnConsensus,
    notifyOnError: config.notifyOnError,
    notifyOnMutation: config.notifyOnMutation,
    notifyOnHeal: config.notifyOnHeal,
    minSeverity: config.minSeverity,
  });
});

swarmRouter.put("/telegram/config", async (req, res) => {
  await ensureSeeded();
  const body = req.body;
  const update: Partial<typeof telegramConfigTable.$inferInsert> = {};
  if (body.enabled !== undefined) update.enabled = body.enabled;
  if (body.chatId !== undefined) update.chatId = body.chatId;
  if (body.botToken !== undefined) update.botToken = body.botToken;
  if (body.notifyOnConsensus !== undefined) update.notifyOnConsensus = body.notifyOnConsensus;
  if (body.notifyOnError !== undefined) update.notifyOnError = body.notifyOnError;
  if (body.notifyOnMutation !== undefined) update.notifyOnMutation = body.notifyOnMutation;
  if (body.notifyOnHeal !== undefined) update.notifyOnHeal = body.notifyOnHeal;
  if (body.minSeverity !== undefined) update.minSeverity = body.minSeverity;

  await db.update(telegramConfigTable).set(update);
  const [config] = await db.select().from(telegramConfigTable).limit(1);
  res.json({
    enabled: config.enabled,
    chatId: config.chatId,
    botToken: config.botToken ? "****" : "",
    notifyOnConsensus: config.notifyOnConsensus,
    notifyOnError: config.notifyOnError,
    notifyOnMutation: config.notifyOnMutation,
    notifyOnHeal: config.notifyOnHeal,
    minSeverity: config.minSeverity,
  });
});

swarmRouter.post("/telegram/push", async (req, res) => {
  const { message, priority = "normal" } = req.body;
  req.log.info({ message, priority }, "Telegram push requested");
  res.json({
    success: true,
    messageId: uuid(),
    timestamp: new Date().toISOString(),
  });
});
