import ccxt from "ccxt";
import { db, liveTicksTable, systemConfigTable } from "@workspace/db";
import { eq, sql } from "drizzle-orm";
import { logger } from "../lib/logger";

export const WATCHLIST = [
  "BTC/USDT:USDT",
  "ETH/USDT:USDT",
  "SOL/USDT:USDT",
  "BNB/USDT:USDT",
  "AVAX/USDT:USDT",
];

export const WATCHLIST_SPOT_FALLBACK: Record<string, string> = {
  "BTC/USDT:USDT": "BTC/USDT",
  "ETH/USDT:USDT": "ETH/USDT",
  "SOL/USDT:USDT": "SOL/USDT",
  "BNB/USDT:USDT": "BNB/USDT",
  "AVAX/USDT:USDT": "AVAX/USDT",
};

interface RawTick {
  exchange: string;
  price: number;
  bid: number;
  ask: number;
  spreadPct: number;
  volume24h: number;
  priceChange24h: number;
  fundingRate: number;
  openInterest: number;
  obImbalance: number;
  cvdDelta: number;
}

interface AggState {
  symbol: string;
  priceMedian: number;
  priceMin: number;
  priceMax: number;
  priceSpreadEx: number;
  priceChange24h: number;
  totalVolume24h: number;
  avgFundingRate: number;
  totalOi: number;
  oiDelta1h: number;
  avgObImbalance: number;
  cumulativeCvd: number;
  nExchanges: number;
  ticks: Record<string, RawTick>;
}

let exchanges: Record<string, any> = {};
let marketsFetched: Record<string, boolean> = {};
let running = false;
let oiHistory: Record<string, Array<[number, number]>> = {};

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function weightedAvg(values: number[], weights: number[]): number {
  if (!values.length) return 0;
  const totalW = weights.reduce((a, b) => a + b, 0);
  if (totalW === 0) return values.reduce((a, b) => a + b, 0) / values.length;
  return values.reduce((sum, v, i) => sum + v * weights[i], 0) / totalW;
}

export async function initExchanges() {
  const exchangeDefs = [
    { id: "binanceusdm", opts: { defaultType: "future" } },
    { id: "bybit",       opts: { defaultType: "linear" } },
    { id: "okx",         opts: { defaultType: "swap"   } },
    { id: "kraken",      opts: {} },
  ];

  for (const def of exchangeDefs) {
    try {
      const ExClass = (ccxt as any)[def.id];
      if (!ExClass) continue;
      const ex = new ExClass({ ...def.opts, enableRateLimit: true, timeout: 10000 });
      exchanges[def.id] = ex;
    } catch (e) {
      logger.warn({ exchange: def.id }, "Exchange init failed");
    }
  }
  logger.info({ count: Object.keys(exchanges).length }, "Exchanges initialized");
}

async function loadMarkets(exId: string): Promise<boolean> {
  if (marketsFetched[exId]) return true;
  try {
    await exchanges[exId].loadMarkets();
    marketsFetched[exId] = true;
    return true;
  } catch (e) {
    return false;
  }
}

function resolveSymbol(exId: string, unified: string): string | null {
  const ex = exchanges[exId];
  if (!ex || !ex.markets) return null;
  if (ex.markets[unified]) return unified;
  const spot = WATCHLIST_SPOT_FALLBACK[unified];
  if (spot && ex.markets[spot]) return spot;
  const clean = unified.split(":")[0];
  if (ex.markets[clean]) return clean;
  return null;
}

async function fetchTickFromExchange(exId: string, symbol: string): Promise<RawTick | null> {
  try {
    const sym = resolveSymbol(exId, symbol);
    if (!sym) return null;
    const ex = exchanges[exId];
    const ticker = await ex.fetchTicker(sym);
    const price   = Number(ticker.last ?? ticker.close ?? 0);
    const bid     = Number(ticker.bid ?? price);
    const ask     = Number(ticker.ask ?? price);
    const vol24h  = Number(ticker.quoteVolume ?? ticker.baseVolume ?? 0);
    const chg     = Number(ticker.percentage ?? 0);
    const spread  = price > 0 ? (ask - bid) / price * 100 : 0;

    let fundingRate = 0;
    let openInterest = 0;
    let obImbalance = 0;
    let cvdDelta = 0;

    try {
      const obData = await ex.fetchOrderBook(sym, 10);
      const bidsVol = obData.bids.slice(0, 5).reduce((s: number, [, v]: [number, number]) => s + v, 0);
      const asksVol = obData.asks.slice(0, 5).reduce((s: number, [, v]: [number, number]) => s + v, 0);
      const total = bidsVol + asksVol;
      if (total > 0) obImbalance = (bidsVol - asksVol) / total;
    } catch (_) {}

    try {
      const trades = await ex.fetchTrades(sym, undefined, 50);
      let buyVol = 0, sellVol = 0;
      for (const t of trades) {
        const v = Number(t.amount ?? 0);
        if (t.side === "buy") buyVol += v;
        else if (t.side === "sell") sellVol += v;
      }
      const total = buyVol + sellVol;
      cvdDelta = total > 0 ? (buyVol - sellVol) / total : 0;
    } catch (_) {}

    try {
      if (ex.has?.fetchFundingRate) {
        const fr = await ex.fetchFundingRate(sym);
        fundingRate = Number(fr?.fundingRate ?? 0);
      }
    } catch (_) {}

    try {
      if (ex.has?.fetchOpenInterest) {
        const oi = await ex.fetchOpenInterest(sym);
        openInterest = Number(oi?.openInterestValue ?? oi?.openInterest ?? 0);
      }
    } catch (_) {}

    if (price <= 0) return null;
    return { exchange: exId, price, bid, ask, spreadPct: spread, volume24h: vol24h, priceChange24h: chg, fundingRate, openInterest, obImbalance, cvdDelta };
  } catch (e) {
    return null;
  }
}

async function aggregateSymbol(symbol: string): Promise<AggState | null> {
  const tasks = Object.keys(exchanges).map(exId => fetchTickFromExchange(exId, symbol));
  const results = await Promise.allSettled(tasks);

  const ticks: Record<string, RawTick> = {};
  const exIds = Object.keys(exchanges);
  results.forEach((r, i) => {
    if (r.status === "fulfilled" && r.value) {
      ticks[exIds[i]] = r.value;
    }
  });

  if (!Object.keys(ticks).length) return null;

  const tickArr = Object.values(ticks);
  const prices    = tickArr.map(t => t.price);
  const volumes   = tickArr.map(t => t.volume24h);
  const fundings  = tickArr.filter(t => t.fundingRate !== 0).map(t => t.fundingRate);
  const fundVols  = tickArr.filter(t => t.fundingRate !== 0).map(t => t.volume24h);
  const ois       = tickArr.filter(t => t.openInterest > 0).map(t => t.openInterest);
  const obs       = tickArr.map(t => t.obImbalance);
  const cvds      = tickArr.map(t => t.cvdDelta);
  const changes   = tickArr.map(t => t.priceChange24h);

  const priceMedian = median(prices);
  const priceMin    = Math.min(...prices);
  const priceMax    = Math.max(...prices);
  const priceSpreadEx = priceMedian > 0 ? (priceMax - priceMin) / priceMedian * 100 : 0;
  const totalVolume = volumes.reduce((a, b) => a + b, 0);
  const avgFunding  = weightedAvg(fundings, fundVols);
  const totalOi     = ois.reduce((a, b) => a + b, 0);
  const avgOb       = weightedAvg(obs, volumes);
  const cumCvd      = cvds.length ? cvds.reduce((a, b) => a + b, 0) / cvds.length : 0;
  const avgChange   = changes.length ? changes.reduce((a, b) => a + b, 0) / changes.length : 0;

  const nowMs = Date.now();
  if (!oiHistory[symbol]) oiHistory[symbol] = [];
  if (totalOi > 0) {
    oiHistory[symbol].push([nowMs, totalOi]);
    if (oiHistory[symbol].length > 60) oiHistory[symbol].shift();
  }

  let oiDelta = 0;
  const hist = oiHistory[symbol];
  if (hist.length >= 2) {
    const ago1h = nowMs - 3_600_000;
    const old = hist.find(([ts]) => ts <= ago1h);
    if (old && old[1] > 0 && totalOi > 0) {
      oiDelta = (totalOi - old[1]) / old[1] * 100;
    }
  }

  return {
    symbol, priceMedian, priceMin, priceMax, priceSpreadEx,
    priceChange24h: avgChange,
    totalVolume24h: totalVolume, avgFundingRate: avgFunding,
    totalOi, oiDelta1h: oiDelta, avgObImbalance: avgOb,
    cumulativeCvd: cumCvd, nExchanges: tickArr.length, ticks,
  };
}

async function saveTick(state: AggState) {
  try {
    await db.insert(liveTicksTable).values({
      symbol:         state.symbol,
      priceMedian:    state.priceMedian,
      priceMin:       state.priceMin,
      priceMax:       state.priceMax,
      priceSpreadEx:  state.priceSpreadEx,
      priceChange24h: state.priceChange24h,
      totalVolume24h: state.totalVolume24h,
      avgFundingRate: state.avgFundingRate,
      totalOi:        state.totalOi,
      oiDelta1h:      state.oiDelta1h,
      avgObImbalance: state.avgObImbalance,
      cumulativeCvd:  state.cumulativeCvd,
      nExchanges:     state.nExchanges,
      rawTicks:       state.ticks as any,
      updatedAt:      new Date(),
    }).onConflictDoUpdate({
      target: liveTicksTable.symbol,
      set: {
        priceMedian:    state.priceMedian,
        priceMin:       state.priceMin,
        priceMax:       state.priceMax,
        priceSpreadEx:  state.priceSpreadEx,
        priceChange24h: state.priceChange24h,
        totalVolume24h: state.totalVolume24h,
        avgFundingRate: state.avgFundingRate,
        totalOi:        state.totalOi,
        oiDelta1h:      state.oiDelta1h,
        avgObImbalance: state.avgObImbalance,
        cumulativeCvd:  state.cumulativeCvd,
        nExchanges:     state.nExchanges,
        rawTicks:       state.ticks as any,
        updatedAt:      new Date(),
      },
    });
  } catch (e: any) {
    logger.error({ msg: e?.message }, "Failed to save tick");
  }
}

async function pollCycle() {
  logger.info("Market feeder poll cycle start");
  for (const exId of Object.keys(exchanges)) {
    await loadMarkets(exId).catch(() => {});
  }

  const tasks = WATCHLIST.map(sym => aggregateSymbol(sym));
  const results = await Promise.allSettled(tasks);
  let saved = 0;
  for (const r of results) {
    if (r.status === "fulfilled" && r.value) {
      await saveTick(r.value);
      saved++;
    }
  }
  logger.info({ saved }, "Market feeder poll cycle done");
}

export async function startMarketFeeder() {
  if (running) return;
  running = true;
  logger.info("Market feeder starting");

  const loop = async () => {
    while (running) {
      try {
        await pollCycle();
      } catch (e: any) {
        logger.error({ msg: e?.message }, "Market feeder error");
      }
      await new Promise(r => setTimeout(r, 15_000));
    }
  };

  loop();
}

export function stopMarketFeeder() {
  running = false;
}

export async function getSystemMode(): Promise<"paper" | "live"> {
  try {
    const rows = await db.select().from(systemConfigTable).where(eq(systemConfigTable.key, "mode")).limit(1);
    return (rows[0]?.value as "paper" | "live") ?? "paper";
  } catch {
    return "paper";
  }
}

export async function setSystemMode(mode: "paper" | "live") {
  await db.insert(systemConfigTable).values({ key: "mode", value: mode, updatedAt: new Date() })
    .onConflictDoUpdate({ target: systemConfigTable.key, set: { value: mode, updatedAt: new Date() } });
}

export async function getSystemConfig(key: string): Promise<string | null> {
  try {
    const rows = await db.select().from(systemConfigTable).where(eq(systemConfigTable.key, key)).limit(1);
    return rows[0]?.value ?? null;
  } catch {
    return null;
  }
}

export async function setSystemConfig(key: string, value: string) {
  await db.insert(systemConfigTable).values({ key, value, updatedAt: new Date() })
    .onConflictDoUpdate({ target: systemConfigTable.key, set: { value, updatedAt: new Date() } });
}
