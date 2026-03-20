import { logger } from "../lib/logger";
import { getSystemMode, setSystemMode, getSystemConfig, setSystemConfig } from "./market-feeder";
import { db, liveTicksTable, paperTradesTable } from "@workspace/db";
import { eq, desc, sql } from "drizzle-orm";

const BOT_TOKEN = process.env.TELEGRAM_BOT_API_KEY ?? "";
const BASE_URL = `https://api.telegram.org/bot${BOT_TOKEN}`;

let running = false;
let offset = 0;

async function tgGet(method: string, params: Record<string, any> = {}): Promise<any> {
  const url = new URL(`${BASE_URL}/${method}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)));
  try {
    const r = await fetch(url.toString(), { signal: AbortSignal.timeout(35000) });
    return await r.json();
  } catch { return null; }
}

async function tgPost(method: string, body: Record<string, any>): Promise<any> {
  try {
    const r = await fetch(`${BASE_URL}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10000),
    });
    return await r.json();
  } catch { return null; }
}

export async function sendTelegramMessage(text: string, chatId?: string): Promise<boolean> {
  const id = chatId ?? await getSystemConfig("telegram_chat_id");
  if (!id || !BOT_TOKEN) return false;
  const r = await tgPost("sendMessage", { chat_id: id, text, parse_mode: "HTML" });
  return r?.ok === true;
}

async function getQuickStatus(): Promise<string> {
  const mode = await getSystemMode();
  const ticks = await db.select().from(liveTicksTable).limit(5);
  const positions = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "open")).limit(10);
  const closed = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "closed")).limit(100);

  const totalPnl = closed.reduce((s, t) => s + t.pnl, 0)
                 + positions.reduce((s, t) => s + t.pnl, 0);

  let lines = [
    `🤖 <b>KRAK SWARM OS – STATUS</b>`,
    ``,
    `⚡ Mode: <b>${mode === "live" ? "🔴 LIVE" : "📄 PAPER"}</b>`,
    `💰 Net PnL: <b>${totalPnl >= 0 ? "+" : ""}$${totalPnl.toFixed(2)}</b>`,
    `📊 Open Positions: ${positions.length}`,
    `📈 Closed Trades: ${closed.length}`,
    ``,
    `<b>Market Prices:</b>`,
  ];

  for (const tick of ticks) {
    const sym = tick.symbol.replace("/USDT:USDT", "");
    const chg = tick.priceChange24h ?? 0;
    const arrow = chg >= 0 ? "▲" : "▼";
    lines.push(`  ${sym}: $${tick.priceMedian.toLocaleString("en-US", { maximumFractionDigits: 2 })} ${arrow}${Math.abs(chg).toFixed(2)}%`);
  }

  return lines.join("\n");
}

async function getPositionsText(): Promise<string> {
  const positions = await db.select().from(paperTradesTable)
    .where(eq(paperTradesTable.status, "open"))
    .orderBy(desc(paperTradesTable.openedAt))
    .limit(10);

  if (!positions.length) return "📋 No open positions";

  const lines = ["📋 <b>Open Positions:</b>", ""];
  for (const p of positions) {
    const sym = p.symbol.replace("/USDT:USDT", "");
    const pnlStr = p.pnl >= 0 ? `+$${p.pnl.toFixed(2)}` : `-$${Math.abs(p.pnl).toFixed(2)}`;
    lines.push(`${p.side === "BUY" ? "🟢" : "🔴"} ${sym} ${p.side} @${p.entryPrice.toFixed(2)} | PnL: ${pnlStr}`);
  }
  return lines.join("\n");
}

async function handleCommand(text: string, chatId: string, from: string) {
  const cmd = text.split(" ")[0].toLowerCase();

  await setSystemConfig("telegram_chat_id", chatId);

  switch (cmd) {
    case "/start":
    case "/help":
      await sendTelegramMessage([
        `👾 <b>KRAK Swarm Intelligence – Online</b>`,
        ``,
        `Commands:`,
        `/status – System status + prices`,
        `/mode – Current trading mode`,
        `/toggle – Switch Paper ↔ Live`,
        `/positions – Open paper positions`,
        `/pnl – P&L summary`,
        `/help – This menu`,
      ].join("\n"), chatId);
      break;

    case "/status":
      await sendTelegramMessage(await getQuickStatus(), chatId);
      break;

    case "/mode": {
      const m = await getSystemMode();
      await sendTelegramMessage(`⚡ Current mode: <b>${m === "live" ? "🔴 LIVE" : "📄 PAPER"}</b>`, chatId);
      break;
    }

    case "/toggle": {
      const current = await getSystemMode();
      const next = current === "paper" ? "live" : "paper";
      await setSystemMode(next);
      await sendTelegramMessage(
        `🔄 Mode switched to: <b>${next === "live" ? "🔴 LIVE" : "📄 PAPER"}</b>`,
        chatId
      );
      break;
    }

    case "/positions":
      await sendTelegramMessage(await getPositionsText(), chatId);
      break;

    case "/pnl": {
      const closed = await db.select().from(paperTradesTable)
        .where(eq(paperTradesTable.status, "closed")).limit(1000);
      const open = await db.select().from(paperTradesTable)
        .where(eq(paperTradesTable.status, "open")).limit(100);

      const realizedPnl = closed.reduce((s, t) => s + t.pnl, 0);
      const unrealizedPnl = open.reduce((s, t) => s + t.pnl, 0);
      const wins = closed.filter(t => t.pnl > 0).length;
      const winRate = closed.length > 0 ? (wins / closed.length * 100).toFixed(1) : "0.0";

      await sendTelegramMessage([
        `💰 <b>P&L Report</b>`,
        ``,
        `Realized: ${realizedPnl >= 0 ? "+" : ""}$${realizedPnl.toFixed(2)}`,
        `Unrealized: ${unrealizedPnl >= 0 ? "+" : ""}$${unrealizedPnl.toFixed(2)}`,
        `Total: ${(realizedPnl + unrealizedPnl) >= 0 ? "+" : ""}$${(realizedPnl + unrealizedPnl).toFixed(2)}`,
        `Win Rate: ${winRate}% (${wins}/${closed.length})`,
      ].join("\n"), chatId);
      break;
    }

    default:
      await sendTelegramMessage("❓ Unknown command. Use /help", chatId);
  }
}

async function pollUpdates() {
  const resp = await tgGet("getUpdates", { offset, timeout: 30, allowed_updates: ["message"] });
  if (!resp?.ok || !resp.result) return;

  for (const update of resp.result) {
    offset = Math.max(offset, update.update_id + 1);
    const msg = update.message;
    if (!msg?.text) continue;
    const chatId = String(msg.chat.id);
    const from = msg.from?.username ?? msg.from?.first_name ?? "user";
    logger.info({ chatId, text: msg.text }, "Telegram message received");
    await handleCommand(msg.text.trim(), chatId, from);
  }
}

export async function startTelegramBot() {
  if (!BOT_TOKEN) {
    logger.warn("TELEGRAM_BOT_API_KEY not set – Telegram bot disabled");
    return;
  }
  if (running) return;
  running = true;
  logger.info("Telegram bot starting");

  const info = await tgGet("getMe");
  if (info?.ok) {
    logger.info({ username: info.result.username }, "Telegram bot connected");
  }

  const loop = async () => {
    while (running) {
      try {
        await pollUpdates();
      } catch (e: any) {
        logger.error({ msg: e?.message }, "Telegram poll error");
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  };
  loop();
}

export async function notifyTelegram(event: "consensus" | "error" | "mutation" | "heal", payload: any) {
  const chatId = await getSystemConfig("telegram_chat_id");
  if (!chatId) return;

  let text = "";
  switch (event) {
    case "consensus":
      text = `⚡ <b>GODMIND CONSENSUS</b>\nAction: <b>${payload.action}</b>\nConfidence: ${payload.confidence?.toFixed(1)}%\nRegime: ${payload.regime}`;
      break;
    case "error":
      text = `🚨 <b>SYSTEM ERROR</b>\n${payload.category}: ${payload.message}\nSeverity: ${payload.severity}`;
      break;
    case "mutation":
      text = `🧬 <b>MUTATION CYCLE</b>\nStrategy: ${payload.strategy}\nImproved: ${payload.improved} bots`;
      break;
    case "heal":
      text = `🛡️ <b>HEAL ACTION</b>\nModule: ${payload.module}\nResult: ${payload.result}\nXP: +${payload.xpEarned}`;
      break;
  }
  if (text) await sendTelegramMessage(text, chatId);
}
