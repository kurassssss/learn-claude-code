# KRAK Swarm Intelligence

## Overview

Ultra-advanced swarm intelligence trading system dashboard with 25 RL engines, 1000 autonomous bots, self-healing engine, parameter mutation, and Telegram bridge.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Frontend**: React + Vite, TailwindCSS, shadcn/ui, Recharts, Framer Motion

## Architecture

### 25 RL Engine Cluster
Engines 01–25: PPO, A3C, DQN, SAC, TD3, APEX, PHANTOM, STORM, ORACLE, VENOM, TITAN, HYDRA, VOID, PULSE, INFINITY, NEMESIS, SOVEREIGN, WRAITH, ABYSS, GENESIS, MIRAGE, ECLIPSE, CHIMERA, AXIOM, GODMIND

- GODMIND has triple-weighted veto authority
- NEMESIS and TITAN have adversarial/macro veto
- Consensus threshold: 13/25 standard, 18/25 STRONG, 22/25 ABSOLUTE

### 1000 Autonomous Bots (Swarm)
Each bot tracks: PnL, win rate, trades, strategy, health score, generation, fitness

### Self-Healing Engine (Omega)
- XP-based reward system
- 25 healing strategies
- AST-level code repair
- Circuit breakers per module
- Error tracking with severity levels

### Parameter Mutation System
- Genetic algorithm mutations
- MAP-Elites quality-diversity search
- Elite strategy archive (20 niches × regimes)
- Mutation history tracking

### Telegram Bridge
- Config: bot token + chat ID
- Notification toggles: consensus, errors, mutations, heals
- Manual message push

## Structure

```text
artifacts/
├── api-server/          # Express API server (all KRAK routes)
│   └── src/routes/
│       ├── health.ts    # GET /api/healthz
│       └── swarm.ts     # All KRAK API endpoints
└── krak-swarm/          # React + Vite frontend dashboard
    └── src/pages/
        ├── dashboard.tsx   # Swarm overview + consensus
        ├── engines.tsx     # 25 RL engines management
        ├── bots.tsx        # 1000 bot listing
        ├── mutations.tsx   # Genetic mutations + MAP-Elites
        ├── healing.tsx     # Self-healing status
        └── telegram.tsx    # Telegram bridge config
lib/
├── api-spec/openapi.yaml   # Full OpenAPI spec for all KRAK APIs
├── api-client-react/       # Generated React Query hooks
├── api-zod/                # Generated Zod schemas
└── db/src/schema/krak.ts   # All DB tables (engines, bots, mutations, errors, etc.)
```

## API Endpoints

- `GET /api/swarm/status` — overall swarm metrics
- `GET /api/swarm/consensus` — current engine consensus (STRONG_BUY/SELL/etc)
- `GET /api/swarm/bots` — paginated bot list with filters
- `GET/PATCH /api/engines/:id` — engine details + parameter mutation
- `POST /api/engines/:id/reset` — reset engine to defaults
- `POST /api/mutations/run` — trigger genetic/MAP-Elites mutation cycle
- `GET /api/mutations/history` — mutation history
- `GET /api/mutations/elite` — MAP-Elites best strategies
- `GET /api/healing/status` — XP level, heal rates, module health
- `GET /api/healing/errors` — error log with severity
- `GET /api/healing/actions` — heal action log
- `POST /api/healing/trigger` — manually trigger heal
- `GET/PUT /api/telegram/config` — Telegram bridge config
- `POST /api/telegram/push` — push message to Telegram

## Database Schema

Tables: `engines`, `bots`, `mutations`, `elite_strategies`, `omega_errors`, `heal_actions`, `telegram_config`, `swarm_metrics`

## Dev Commands

- `pnpm --filter @workspace/api-server run dev` — API server
- `pnpm --filter @workspace/krak-swarm run dev` — Frontend
- `pnpm --filter @workspace/api-spec run codegen` — Regenerate API types
- `pnpm --filter @workspace/db run push` — Push DB schema changes
