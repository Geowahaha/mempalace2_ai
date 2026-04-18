# CLAUDE.md — dexter\_pro\_v3\_fixed

# ============================================================

# SAVE TO: D:\\dexter\_pro\_v3\_fixed\\dexter\_pro\_v3\_fixed\\CLAUDE.md

# Auto-loaded by Claude Code every session. Keep updated.

# ============================================================

## What This System Is

Dexter Pro is a fully autonomous multi-strategy AI trading system.
It is NOT a simple signal bot. It has:

* Multiple concurrent strategy families running in parallel (swarm model)
* A neural reasoning brain that evaluates market context per signal
* A self-learning layer that adapts confidence thresholds from live performance
* A trading manager that orchestrates all families + guards simultaneously
* A position manager that actively defends open trades in real-time
* A canary system that probes experimental strategies with minimal risk
* Live execution on cTrader and MT5 with real money

Owner: mrgeo | Bangkok (UTC+7) | Windows dev machine

## Full Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     MARKET DATA LAYER                        │
│  data/ — candles, ticks, OHLCV feeds (Binance, cTrader)     │
│  market/ — session detection, regime classification          │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    ANALYSIS LAYER                            │
│  analysis/ — behavioral, structural, regime, sentiment       │
│  agent/ — chart state router, signal context builder         │
│  data/reports/chart\_state\_memory\_report.json                 │
│         └── accumulated band/session/pattern memory          │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                  NEURAL BRAIN LAYER                          │
│  AI providers: Gemini 2.0 Flash (primary)                    │
│                OpenRouter (fallback, multi-model routing)    │
│                Ollama local (qwen3:1.5b / llama3.2:3b)       │
│  learning/live\_profile\_autopilot.py — confidence band logic  │
│  learning/ — neural scoring, probability estimation          │
│  Purpose: enrich raw signals with market reasoning context   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                SELF-LEARNING LAYER                           │
│  learning/ — winner logic, performance scoring               │
│  • Winner logic: historical session/band/side/symbol perf    │
│    → applies confidence bonus (+2.0) or penalty (-2.0/-4.5) │
│  • Chart state memory: accumulates band samples over time    │
│    → gates first\_sample\_mode and high\_confidence\_bridge      │
│  • Live profile autopilot: tunes confidence bands per setup  │
│  • Family promotion/demotion by win rate + PnL               │
│  • Crypto weekend scorecard: weekly performance analysis     │
│  System improves itself continuously from live trade results │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                  SCHEDULER / SIGNAL ROUTER                   │
│  scheduler.py — THE core brain of the system                 │
│  • Runs all scan intervals (XAU 300s, stocks 1800s, etc.)   │
│  • Routes each signal through regime guard → pattern gate    │
│    → confidence gate → direction guard → family selector     │
│  • Manages: behavioral, scalp, scheduled, canary, swarm      │
│  • US open smart monitor, XAU shock mode, mood-stop logic    │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│               TRADING MANAGER LAYER                          │
│  data/runtime/trading\_manager\_state.json                     │
│  • xau\_execution\_directive — master XAU bias                 │
│  • xau\_order\_care — monitors pending orders for staleness    │
│  • xau\_micro\_regime — short-window regime refresh            │
│  • xau\_cluster\_loss\_guard — pauses after cluster losses      │
│  • xau\_opportunity\_sidecar — tracks follow-on opportunities  │
│  • Swarm sampling — multiple families evaluated per signal   │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
┌────────▼────────┐             ┌────────▼────────┐
│  STANDARD       │             │  CANARY SYSTEM   │
│  STRATEGIES     │             │  (probe layer)   │
│                 │             │                  │
│ XAU behavioral  │             │ Low-risk parallel│
│ XAU scheduled   │             │ trades that test │
│ Stocks scanner  │             │ strategy families│
│                 │             │ before scaling up│
└────────┬────────┘             └────────┬────────┘
         │                               │
         └───────────────┬───────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│              STRATEGY FAMILIES (all run in parallel)         │
│                                                              │
│  PRIMARY CANARY FAMILIES (tested, running live):            │
│  xau\_scalp\_pullback\_limit    CTRADER\_RISK=2.5 USD           │
│  btc\_weekday\_lob\_momentum    CTRADER\_RISK=1.1 USD           │
│                                                              │
│  EXPERIMENTAL FAMILIES (canary probe, lower risk):          │
│  xau\_scalp\_tick\_depth\_filter (TDF)  RISK=0.75 USD           │
│  xau\_scalp\_microtrend\_follow\_up (MFU)  RISK=0.65 USD        │
│  xau\_scalp\_flow\_short\_sidecar (FSS)  RISK=0.45 USD          │
│  xau\_scalp\_failed\_fade\_follow\_stop (FFFS)  RISK=0.75 USD    │
│  xau\_scalp\_range\_repair (RR)  RISK=0.75 USD                 │
│  eth\_weekday\_overlap\_probe  RISK=0.35 USD                    │
│                                                              │
│  Each family has: allowed patterns, sessions, direction      │
│  guards, confidence thresholds, and its own variant limit   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│               EXECUTION LAYER                                │
│  execution/ — order placement, position sizing, risk check   │
│  api/ — cTrader OpenAPI + MT5 + Binance/Bybit wrappers       │
│                                                              │
│  Guards (all must pass before any order fires):              │
│  • check\_risk() — max positions, max USD at risk             │
│  • Direction guard — CTRADER\_BLOCK\_OPPOSITE\_DIRECTION=1      │
│  • Per-family/symbol/direction limits                        │
│  • Pending order limits per symbol                           │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│               POSITION MANAGER                               │
│  CTRADER\_POSITION\_MANAGER\_ENABLED=1                          │
│  • close\_at\_planned\_target — honor TP at entry time          │
│  • invalid\_TP repair — fixes broken TP after entry           │
│  • breakout/pullback repair TP logic                         │
│  • XAU active defense — real-time adverse flow detection     │
│    Uses: bar\_volume\_proxy, delta\_proxy, adverse\_drift        │
│    Can: tighten stop, close early, lock profit               │
│  • BE (breakeven) trigger after TP1 partial close            │
│  • Scheduled canary no-follow logic                          │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│              NOTIFICATION + MONITORING                       │
│  notifier/ — Telegram bot @mrgeon8n\_bot                      │
│  • Per-signal alerts (async, non-blocking)                   │
│  • US open session kickoff + quality reports every 15min     │
│  • Mood-stop alerts when opportunity breadth weakens         │
│  • XAU shock mode alerts on major news events                │
│  • Crypto weekend scorecard (Sundays UTC 00:15)              │
│  moltworker/ — background workers, health checks             │
│  ops/ — watchdog (dexter\_monitor\_watchdog.ps1)               │
└─────────────────────────────────────────────────────────────┘
```

## Directory Map

```
dexter\_pro\_v3\_fixed/
  agent/          ← chart state router, signal context builder
  analysis/       ← behavioral, structural, regime, sentiment analysis
  api/            ← broker wrappers (cTrader OpenAPI, MT5, Binance, Bybit)
  data/
    ctrader\_openapi.db        ← execution journal + all live trade records
    runtime/                  ← live JSON state (trading\_manager\_state etc.)
    reports/                  ← chart\_state\_memory\_report, autopilot reports
  docs/           ← audit checklists, session logs, prompt library
  execution/      ← order placement, position sizing, risk check, PM
  learning/       ← winner logic, neural scoring, autopilot, band tracking
  logs/           ← runtime logs (read-only reference)
  market/         ← session detection (asian/london/ny), regime classifier
  moltworker/     ← background workers, scheduled jobs
  notifier/       ← Telegram formatting, alert dispatch
  openclaw\_skills/← autonomous agent skill modules
  ops/            ← watchdog, health check, deployment scripts
  runtime/        ← live canary state, monitor logs, watchdog state
  scanners/       ← XAU scalp scanner, BTC scanner, ETH scanner, stocks
  store/          ← trade journal, persistent state, performance history
  tests/          ← unit + integration tests (pytest)
  scheduler.py    ← CORE: all scan loops + complete signal routing logic
  config.py       ← CORE: typed config object parsing all .env.local keys
  main.py         ← entry point, process orchestration
```

## Parent Directory

```
D:\\dexter\_pro\_v3\_fixed\\
  dexter\_pro\_v3\_fixed\\        ← project root (you are here)
  \_audit\\                     ← audit logs, read-only
  \_refs\\                      ← reference documents
  learning\\                   ← ML artifacts (shared)
  docs\\
    dexter\_claude\_prompts.py  ← Claude Code prompt templates
    dexter\_file\_guide.py      ← /add cheat sheet per task type
```

## Critical Files

|File|Purpose|
|-|-|
|`scheduler.py`|Core — all routing, all scan loops, all family selectors|
|`config.py`|All env vars parsed to typed object — source of truth|
|`.env.local`|Raw thresholds 1200+ lines — never hardcode from here|
|`main.py`|Entry point, process start|
|`execution/`|Order placement + position manager|
|`api/`|All broker wrappers (cTrader OpenAPI, MT5, Binance, Bybit)|
|`docs/AGENT_HANDOFF_XAU_GATE_ENTRY_TEMPLATE.md`|XAU gate journal stamps, entry template + M1 bias chain, VM aggregate checklist — **mission next steps**|
|`docs/AGENT_SYNC_BOARD.md`|**Bilateral agent coordination** — read/update at session start; owner checks progress here (no middleman for routine status)|

## Session Startup

```
/add scheduler.py
/add config.py
/add data/runtime/trading_manager_state.json
/add docs/AGENT_HANDOFF_XAU_GATE_ENTRY_TEMPLATE.md
/add docs/AGENT_SYNC_BOARD.md
```

**Agent coordination:** open **`docs/AGENT_SYNC_BOARD.md`** first — refresh **Quick status** + **Owner — latest** + append **Activity log**; peer agents monitor the same file.

**XAU mission (sequenced):** open `docs/AGENT_HANDOFF_XAU_GATE_ENTRY_TEMPLATE.md` **§4.1** (phases **A→E**) and **§5**; execute in order.

Optional: *"Continue from 2026-04 handoff. What is current system state?"*

## Never Ask Me About

* Basic Python, installing packages, general coding concepts
* Anything not related to this trading system's live behavior

**Focus:** multi-family trading logic, AI reasoning, self-learning adaptation, position management, live-market safety, signal routing bugs.

* SSH keys: `C:\Users\mrgeo\.ssh\`
* `.gitignore` must cover: `.env.local`, `*.key`, `*.pem`, `*.pub`, `*.zip`

## Trading Philosophy

See `docs/TRADING_PHILOSOPHY.md` for the full EliteQuantTrader profile.

**Actionable principles already integrated into code:**
- Sniper entries via multi-layer confluence: Entry Sharpness Score (8 microstructure features, 5 dimensions, 0-100 composite)
- Order flow confirmation at entry: delta_proxy, tick_up_ratio, depth_imbalance, depth_absorption_rate
- Dynamic entry type routing: knife→block, caution→limit+risk reduction, sharp→promote to stop
- Self-evolving: winner logic, chart state memory, live profile autopilot, family promotion/demotion
- Multi-agent: openclaw/ Conductor + Risk/Perf/Regime/Opt agents

**Future edge layers (from philosophy, not yet implemented):**
- Volume Profile (HVN/LVN/POC) for structural SL/TP placement
- DOM-driven adaptive trailing (liquidity shift detection)
- Statistical regime detection (HMM/clustering beyond rule-based day_type)

## Recent Changes

### 2026-04-04 — Entry Sharpness Score + sweep reversal sharpness guard

`analysis/entry_sharpness.py` (new), `scheduler.py`, `config.py`, `learning/live_profile_autopilot.py`. 8 deep microstructure features + composite scorer integrated into PB knife guard, entry router (knife/caution/sharp bands), RR knife guard, sweep reversal gate. 16 new `XAU_ENTRY_SHARPNESS_*` config keys. Full observability via raw_scores. 51 tests (41 unit + 10 integration). Branch: `deploy-xau-family-canary`, deployed to VM.

### 2026-04-03 — XAUUSD session filter fix + MTF bypass + holiday guard

Session filter bug (`session_sig not in set` → `_session_signature_matches`), all-unknown MTF bypass, aligned_side override, FFFS explicit skip, FSS/FLS conf gate, entry template scaffold (disabled), 5 BTC LOB test fixes. Commit `0cf600f`.

### 2026-04-02 — Post-SL sweep reversal re-entry

`_check_post_sl_reversal_signal` — M1 wick detection, market re-entry via main cTrader lane. Holiday guard for tests. Commits `f3b8c92`, `53939b0`, `cf72eea`, `7a45e84`.

### 2026-03-31 — News guard + PSC family + RR/canary fixes

Scheduled news guard (T1 kill PRE45/POST30, T2 size x0.50), PSC canary (Pre-London sweep+cont), RR conf gate removal. Commits `ce0dc65`, `672206e`, `3aa7461`.

## Current Branch State (update every session)

Branch: `deploy-xau-family-canary` — active development branch, deployed to VM.
Verify with `git branch -a`.
