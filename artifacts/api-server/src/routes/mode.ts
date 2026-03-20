import { Router } from "express";
import { db, liveTicksTable, systemConfigTable } from "@workspace/db";
import { getSystemMode, setSystemMode } from "../services/market-feeder";
import { getPaperSummary } from "../services/paper-trading";
import { sendTelegramMessage } from "../services/telegram-bot";
import { desc } from "drizzle-orm";

export const modeRouter = Router();

modeRouter.get("/mode", async (req, res) => {
  const mode = await getSystemMode();
  const ticks = await db.select().from(liveTicksTable).limit(10);
  res.json({
    mode,
    feederActive: ticks.length > 0,
    lastTick: ticks[0]?.updatedAt ?? null,
    symbolsTracked: ticks.length,
    telegramConnected: !!(process.env.TELEGRAM_BOT_API_KEY),
  });
});

modeRouter.post("/mode/toggle", async (req, res) => {
  const current = await getSystemMode();
  const next = current === "paper" ? "live" : "paper";
  await setSystemMode(next);

  const chatId = req.body.chatId;
  if (chatId || true) {
    await sendTelegramMessage(
      `🔄 <b>Mode switched</b>\n${current.toUpperCase()} → <b>${next.toUpperCase()}</b>`
    ).catch(() => {});
  }

  res.json({ mode: next, previous: current });
});

modeRouter.post("/mode/set", async (req, res) => {
  const { mode } = req.body;
  if (mode !== "paper" && mode !== "live") {
    return res.status(400).json({ error: "mode must be 'paper' or 'live'" });
  }
  await setSystemMode(mode);
  res.json({ mode });
});

modeRouter.get("/market/live", async (req, res) => {
  const ticks = await db.select().from(liveTicksTable).limit(20);
  res.json(ticks);
});

modeRouter.get("/paper/summary", async (req, res) => {
  const summary = await getPaperSummary();
  res.json(summary);
});

modeRouter.post("/telegram/notify", async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: "message required" });
  const sent = await sendTelegramMessage(message);
  res.json({ sent });
});
