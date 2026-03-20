import { db, liveTicksTable, paperTradesTable, enginesTable } from "@workspace/db";
import { eq, desc, sql } from "drizzle-orm";
import { v4 as uuid } from "uuid";
import { logger } from "../lib/logger";
import { getSystemMode, getSystemConfig } from "./market-feeder";
import { sendTelegramMessage } from "./telegram-bot";

const INITIAL_CAPITAL = 100_000;
const POSITION_SIZE_PCT = 0.02;
const MAX_OPEN_POSITIONS = 5;
const TRADE_SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT"];

let running = false;

async function getConsensusAction(): Promise<{ action: string; confidence: number }> {
  try {
    const engines = await db.select({ lastAction: enginesTable.lastAction, vetoWeight: enginesTable.vetoWeight })
      .from(enginesTable);

    const votes: Record<string, number> = {};
    let totalWeight = 0;
    for (const e of engines) {
      const w = e.vetoWeight;
      votes[e.lastAction] = (votes[e.lastAction] ?? 0) + w;
      totalWeight += w;
    }

    const topAction = Object.entries(votes).sort((a, b) => b[1] - a[1])[0];
    if (!topAction) return { action: "HOLD", confidence: 50 };

    const confidence = totalWeight > 0 ? (topAction[1] / totalWeight * 100) : 50;
    return { action: topAction[0], confidence };
  } catch {
    return { action: "HOLD", confidence: 50 };
  }
}

async function getLivePrice(symbol: string): Promise<number | null> {
  const rows = await db.select({ price: liveTicksTable.priceMedian })
    .from(liveTicksTable)
    .where(eq(liveTicksTable.symbol, symbol))
    .limit(1);
  return rows[0]?.price ?? null;
}

async function getOpenPosition(symbol: string): Promise<any | null> {
  const rows = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.symbol, symbol))
    .where(eq(paperTradesTable.status, "open"))
    .limit(1);
  return rows[0] ?? null;
}

async function getOpenCount(): Promise<number> {
  const rows = await db.select({ count: sql<number>`count(*)` })
    .from(paperTradesTable)
    .where(eq(paperTradesTable.status, "open"));
  return Number(rows[0]?.count ?? 0);
}

async function openPosition(symbol: string, side: "BUY" | "SELL", price: number) {
  const posValue = INITIAL_CAPITAL * POSITION_SIZE_PCT;
  const quantity = posValue / price;
  const id = `pt-${uuid()}`;

  await db.insert(paperTradesTable).values({
    id, symbol, side, quantity,
    entryPrice: price,
    exitPrice: 0,
    pnl: 0,
    pnlPct: 0,
    status: "open",
    engine: "GODMIND",
    openedAt: new Date(),
  });

  const sym = symbol.replace("/USDT:USDT", "");
  logger.info({ symbol, side, price, quantity }, "Paper position opened");
  const chatId = await getSystemConfig("telegram_chat_id");
  if (chatId) {
    await sendTelegramMessage(
      `📄 <b>PAPER TRADE OPENED</b>\n${side === "BUY" ? "🟢" : "🔴"} ${sym} ${side}\n@ $${price.toFixed(2)}\nQty: ${quantity.toFixed(6)}\nValue: $${posValue.toFixed(2)}`,
      chatId
    );
  }
}

async function closePosition(pos: any, currentPrice: number) {
  const sym = pos.symbol;
  const entryPrice = pos.entryPrice;
  let pnl = 0;
  let pnlPct = 0;

  if (pos.side === "BUY") {
    pnl = (currentPrice - entryPrice) * pos.quantity;
    pnlPct = (currentPrice - entryPrice) / entryPrice * 100;
  } else {
    pnl = (entryPrice - currentPrice) * pos.quantity;
    pnlPct = (entryPrice - currentPrice) / entryPrice * 100;
  }

  await db.update(paperTradesTable)
    .set({ status: "closed", exitPrice: currentPrice, pnl, pnlPct, closedAt: new Date() })
    .where(eq(paperTradesTable.id, pos.id));

  const symShort = sym.replace("/USDT:USDT", "");
  const pnlStr = pnl >= 0 ? `+$${pnl.toFixed(2)}` : `-$${Math.abs(pnl).toFixed(2)}`;
  logger.info({ symbol: sym, pnl, pnlPct }, "Paper position closed");
  const chatId = await getSystemConfig("telegram_chat_id");
  if (chatId) {
    await sendTelegramMessage(
      `📄 <b>PAPER TRADE CLOSED</b>\n${pnl >= 0 ? "✅" : "❌"} ${symShort}\n@ $${currentPrice.toFixed(2)}\nPnL: ${pnlStr} (${pnlPct.toFixed(2)}%)`,
      chatId
    );
  }
}

async function updateOpenPnL() {
  const openPositions = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "open"))
    .limit(20);

  for (const pos of openPositions) {
    const currentPrice = await getLivePrice(pos.symbol);
    if (!currentPrice) continue;
    let pnl = 0;
    let pnlPct = 0;
    if (pos.side === "BUY") {
      pnl = (currentPrice - pos.entryPrice) * pos.quantity;
      pnlPct = (currentPrice - pos.entryPrice) / pos.entryPrice * 100;
    } else {
      pnl = (pos.entryPrice - currentPrice) * pos.quantity;
      pnlPct = (pos.entryPrice - currentPrice) / pos.entryPrice * 100;
    }
    await db.update(paperTradesTable)
      .set({ pnl, pnlPct })
      .where(eq(paperTradesTable.id, pos.id));
  }
}

async function tradingCycle() {
  const mode = await getSystemMode();
  const { action, confidence } = await getConsensusAction();

  await updateOpenPnL();

  if (confidence < 55) return;

  const openCount = await getOpenCount();

  for (const symbol of TRADE_SYMBOLS) {
    const price = await getLivePrice(symbol);
    if (!price) continue;

    const openPos = await getOpenPosition(symbol);

    if (action.includes("BUY") || action === "STRONG_BUY") {
      if (!openPos || openPos.side === "SELL") {
        if (openPos?.side === "SELL") await closePosition(openPos, price);
        if (openCount < MAX_OPEN_POSITIONS) {
          await openPosition(symbol, "BUY", price);
        }
      }
    } else if (action.includes("SELL") || action === "STRONG_SELL") {
      if (openPos?.side === "BUY") {
        await closePosition(openPos, price);
      }
    } else if (action === "HOLD" && openPos) {
      const pnlPct = openPos.pnlPct ?? 0;
      if (pnlPct >= 3.0 || pnlPct <= -2.0) {
        await closePosition(openPos, price);
      }
    }
  }
}

export async function startPaperTrading() {
  if (running) return;
  running = true;
  logger.info("Paper trading engine starting");

  const loop = async () => {
    while (running) {
      try {
        await tradingCycle();
      } catch (e: any) {
        logger.error({ msg: e?.message }, "Paper trading error");
      }
      await new Promise(r => setTimeout(r, 30_000));
    }
  };
  loop();
}

export async function getPaperSummary() {
  const open = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "open"))
    .orderBy(desc(paperTradesTable.openedAt));

  const closed = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "closed"))
    .orderBy(desc(paperTradesTable.closedAt))
    .limit(50);

  const realizedPnl = closed.reduce((s, t) => s + t.pnl, 0);
  const unrealizedPnl = open.reduce((s, t) => s + t.pnl, 0);
  const wins = closed.filter(t => t.pnl > 0).length;
  const winRate = closed.length > 0 ? wins / closed.length : 0;

  return {
    openPositions: open,
    recentClosed: closed.slice(0, 20),
    realizedPnl,
    unrealizedPnl,
    totalPnl: realizedPnl + unrealizedPnl,
    totalTrades: closed.length,
    winRate,
    initialCapital: INITIAL_CAPITAL,
    currentCapital: INITIAL_CAPITAL + realizedPnl + unrealizedPnl,
  };
}
