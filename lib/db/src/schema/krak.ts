import { pgTable, text, real, integer, boolean, serial, jsonb, timestamp } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

export const enginesTable = pgTable("engines", {
  id: text("id").primaryKey(),
  engineNumber: integer("engine_number").notNull(),
  name: text("name").notNull(),
  type: text("type").notNull(),
  description: text("description").notNull().default(""),
  status: text("status").notNull().default("idle"),
  winRate: real("win_rate").notNull().default(0),
  totalReward: real("total_reward").notNull().default(0),
  epsilon: real("epsilon").notNull().default(0.2),
  learningRate: real("learning_rate").notNull().default(0.001),
  gamma: real("gamma").notNull().default(0.995),
  batchSize: integer("batch_size").notNull().default(64),
  bufferCap: integer("buffer_cap").notNull().default(200000),
  entropyBonus: real("entropy_bonus").notNull().default(0.01),
  explorationRate: real("exploration_rate").notNull().default(0.2),
  lastAction: text("last_action").notNull().default("HOLD"),
  lastActionTs: timestamp("last_action_ts").notNull().defaultNow(),
  tradesCount: integer("trades_count").notNull().default(0),
  hasVeto: boolean("has_veto").notNull().default(false),
  vetoWeight: real("veto_weight").notNull().default(1.0),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const botsTable = pgTable("bots", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  status: text("status").notNull().default("idle"),
  pnl: real("pnl").notNull().default(0),
  winRate: real("win_rate").notNull().default(0),
  trades: integer("trades").notNull().default(0),
  strategy: text("strategy").notNull().default("neutral"),
  healthScore: real("health_score").notNull().default(100),
  generation: integer("generation").notNull().default(1),
  fitness: real("fitness").notNull().default(0),
  lastActivity: timestamp("last_activity").notNull().defaultNow(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const mutationsTable = pgTable("mutations", {
  id: text("id").primaryKey(),
  strategy: text("strategy").notNull(),
  mutationsApplied: integer("mutations_applied").notNull().default(0),
  improved: integer("improved").notNull().default(0),
  degraded: integer("degraded").notNull().default(0),
  unchanged: integer("unchanged").notNull().default(0),
  bestFitnessDelta: real("best_fitness_delta").notNull().default(0),
  averageFitnessDelta: real("average_fitness_delta").notNull().default(0),
  details: jsonb("details").notNull().default([]),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const eliteStrategiesTable = pgTable("elite_strategies", {
  id: text("id").primaryKey(),
  niche: text("niche").notNull(),
  regime: text("regime").notNull(),
  fitness: real("fitness").notNull().default(0),
  params: jsonb("params").notNull().default({}),
  discoveredAt: timestamp("discovered_at").notNull().defaultNow(),
  timesSelected: integer("times_selected").notNull().default(0),
});

export const errorsTable = pgTable("omega_errors", {
  id: text("id").primaryKey(),
  ts: real("ts").notNull(),
  message: text("message").notNull(),
  category: text("category").notNull(),
  severity: text("severity").notNull(),
  module: text("module").notNull().default("unknown"),
  resolved: boolean("resolved").notNull().default(false),
  fixAttempts: integer("fix_attempts").notNull().default(0),
  resolutionBy: text("resolution_by").notNull().default(""),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const healActionsTable = pgTable("heal_actions", {
  id: text("id").primaryKey(),
  ts: real("ts").notNull(),
  errorId: text("error_id").notNull(),
  strategy: text("strategy").notNull(),
  module: text("module").notNull(),
  result: text("result").notNull(),
  durationS: real("duration_s").notNull().default(0),
  details: text("details").notNull().default(""),
  xpEarned: integer("xp_earned").notNull().default(0),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const telegramConfigTable = pgTable("telegram_config", {
  id: serial("id").primaryKey(),
  enabled: boolean("enabled").notNull().default(false),
  chatId: text("chat_id").notNull().default(""),
  botToken: text("bot_token").notNull().default(""),
  notifyOnConsensus: boolean("notify_on_consensus").notNull().default(true),
  notifyOnError: boolean("notify_on_error").notNull().default(true),
  notifyOnMutation: boolean("notify_on_mutation").notNull().default(false),
  notifyOnHeal: boolean("notify_on_heal").notNull().default(true),
  minSeverity: text("min_severity").notNull().default("WARNING"),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const liveTicksTable = pgTable("live_ticks", {
  id: serial("id").primaryKey(),
  symbol: text("symbol").notNull().unique(),
  priceMedian: real("price_median").notNull().default(0),
  priceMin: real("price_min").notNull().default(0),
  priceMax: real("price_max").notNull().default(0),
  priceSpreadEx: real("price_spread_ex").notNull().default(0),
  priceChange24h: real("price_change_24h").notNull().default(0),
  totalVolume24h: real("total_volume_24h").notNull().default(0),
  avgFundingRate: real("avg_funding_rate").notNull().default(0),
  totalOi: real("total_oi").notNull().default(0),
  oiDelta1h: real("oi_delta_1h").notNull().default(0),
  avgObImbalance: real("avg_ob_imbalance").notNull().default(0),
  cumulativeCvd: real("cumulative_cvd").notNull().default(0),
  nExchanges: integer("n_exchanges").notNull().default(0),
  rawTicks: jsonb("raw_ticks").notNull().default({}),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const systemConfigTable = pgTable("system_config", {
  key: text("key").primaryKey(),
  value: text("value").notNull().default(""),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const paperTradesTable = pgTable("paper_trades", {
  id: text("id").primaryKey(),
  symbol: text("symbol").notNull(),
  side: text("side").notNull(),
  quantity: real("quantity").notNull().default(0),
  entryPrice: real("entry_price").notNull().default(0),
  exitPrice: real("exit_price").notNull().default(0),
  pnl: real("pnl").notNull().default(0),
  pnlPct: real("pnl_pct").notNull().default(0),
  status: text("status").notNull().default("open"),
  engine: text("engine").notNull().default("GODMIND"),
  openedAt: timestamp("opened_at").notNull().defaultNow(),
  closedAt: timestamp("closed_at"),
});

export const swarmMetricsTable = pgTable("swarm_metrics", {
  id: serial("id").primaryKey(),
  totalPnl: real("total_pnl").notNull().default(0),
  winRate: real("win_rate").notNull().default(0),
  totalXp: integer("total_xp").notNull().default(0),
  xpLevel: integer("xp_level").notNull().default(1),
  recordedAt: timestamp("recorded_at").notNull().defaultNow(),
});

export const insertEngineSchema = createInsertSchema(enginesTable);
export type InsertEngine = z.infer<typeof insertEngineSchema>;
export type Engine = typeof enginesTable.$inferSelect;

export const insertBotSchema = createInsertSchema(botsTable);
export type InsertBot = z.infer<typeof insertBotSchema>;
export type Bot = typeof botsTable.$inferSelect;

export const insertMutationSchema = createInsertSchema(mutationsTable);
export type InsertMutation = z.infer<typeof insertMutationSchema>;
export type Mutation = typeof mutationsTable.$inferSelect;

export const insertEliteStrategySchema = createInsertSchema(eliteStrategiesTable);
export type InsertEliteStrategy = z.infer<typeof insertEliteStrategySchema>;
export type EliteStrategy = typeof eliteStrategiesTable.$inferSelect;

export const insertErrorSchema = createInsertSchema(errorsTable);
export type InsertError = z.infer<typeof insertErrorSchema>;
export type OmegaError = typeof errorsTable.$inferSelect;

export const insertHealActionSchema = createInsertSchema(healActionsTable);
export type InsertHealAction = z.infer<typeof insertHealActionSchema>;
export type HealAction = typeof healActionsTable.$inferSelect;

export const insertLiveTickSchema = createInsertSchema(liveTicksTable);
export type InsertLiveTick = z.infer<typeof insertLiveTickSchema>;
export type LiveTick = typeof liveTicksTable.$inferSelect;

export const insertPaperTradeSchema = createInsertSchema(paperTradesTable);
export type InsertPaperTrade = z.infer<typeof insertPaperTradeSchema>;
export type PaperTrade = typeof paperTradesTable.$inferSelect;

export type SystemConfig = typeof systemConfigTable.$inferSelect;
