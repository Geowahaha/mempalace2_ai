# 🏛️ HANDOFF.md — Mempalace2 AI

> **To the next agent:** Read this file first. Understand the project. Update this file before you leave.

---

## Project Overview

**Mempalace2 AI** is an intelligent multi-agent trading system for **XAUUSD (Gold)**. Architecture inspired by Claude Code's source deep dive (50-lesson course at markdown.engineering/learn-claude-code).

## Architecture

```
COORDINATOR (orchestrator)
  ├── MarketScanner → scans markets, detects 7 setup types
  ├── Analyst → multi-timeframe confirmation, optimal Entry/TP1/TP2/TP3/SL
  ├── RiskManager → Kelly sizing, portfolio heat, circuit breakers
  └── Executor → trailing stops, TP monitoring, auto-close

TOOLS
  ├── market_data (XAUUSD via OANDA/MT5, synthetic fallback)
  ├── technical_analysis (EMA, RSI, MACD, ATR, Supertrend, BB, ADX, Stochastic)
  └── risk_engine (Kelly Criterion, R:R enforcement, Sharpe-adjusted entry)

STRATEGIES
  ├── ATR-based dynamic levels
  ├── Structure-based (support/resistance)
  ├── Fibonacci extensions
  └── Ensemble optimizer (picks best automatically)
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Python (not TS/Node) | Easier for quant/trading ecosystem (numpy, pandas, scipy) |
| XAUUSD focus | Gold is highly liquid, clear S/R levels, good ATR for TP/SL |
| OANDA as primary exchange | Best API for forex/gold, free practice account |
| Kelly Criterion (25% fractional) | Conservative position sizing, prevents overbetting |
| Min 2:1 R:R enforced | Every trade must have reward ≥ 2× risk |
| 6% max portfolio heat | Never risk more than 6% total equity at once |
| Multi-timeframe confirmation | Signals must agree across 15m/1h/4h/1d |

## Risk Model

- **Position sizing:** Kelly Criterion (fractional 25%)
- **Stop Loss:** ATR × 1.5 (adjusted for volatility regime) + structure
- **Take Profits:** TP1 = 2R, TP2 = 3.5R, TP3 = 5R
- **Trailing stop:** Moves to breakeven after TP1, then trails at 2× ATR
- **Circuit breaker:** 3% daily loss halts trading
- **Max drawdown:** 15% triggers system shutdown

## Setup Detection Patterns (7 types)

1. **EMA Crossover** — Fast/slow EMA cross
2. **RSI Oversold/Overbought** — RSI < 30 or > 70
3. **MACD Momentum Shift** — Histogram turning
4. **Supertrend Direction Flip** — Bullish/bearish flip
5. **Bollinger Band Squeeze Breakout** — Low BB width + direction
6. **Support/Resistance Test** — Price near key level (within 0.5 ATR)
7. **Trend Alignment** — All indicators agree on direction

## How to Run

```bash
cd mempalace2_ai
pip install -r requirements.txt
python -m mempalace2_ai --config config/settings.yaml
```

## Environment Variables

```bash
EXCHANGE_API_KEY=your_key_here
EXCHANGE_API_SECRET=your_secret_here
```

## File Structure

```
mempalace2_ai/
├── __main__.py          # Entry point (boot → pipeline → dashboard)
├── core/
│   ├── boot.py          # 3-phase boot (config → agents → tasks)
│   ├── state.py         # GlobalState singleton (trade signals, portfolio)
│   └── task.py          # Task lifecycle (pending→running→completed/failed)
├── agents/
│   ├── coordinator.py   # Top orchestrator, message routing, dashboard
│   ├── market_scanner.py# Scans & detects setups (7 patterns)
│   ├── analyst.py       # Deep analysis, MTF confirmation, signal gen
│   ├── risk_manager.py  # Kelly sizing, heat check, approve/reject
│   └── executor.py      # Execute trades, trailing stops, TP/SL monitor
├── tools/
│   ├── base.py          # Tool interface & registry
│   ├── market_data.py   # OHLCV fetching (OANDA + synthetic)
│   ├── technical.py     # Full indicator suite (15+ indicators)
│   ├── risk_engine.py   # Position sizing & validation
│   └── registry.py      # Registers all tools
├── strategies/
│   └── optimizer.py     # ATR/Structure/Fibonacci/Ensemble optimizers
├── config/
│   ├── settings.py      # AppConfig dataclasses
│   └── settings.yaml    # Default config (XAUUSD, risk params)
└── requirements.txt
```

---

## How It Works (Full Pipeline)

The system runs as a **continuous pipeline** with 5 agents working together:

```
START
  │
  ▼
┌─────────────────────────────────────────────┐
│              BOOT SEQUENCE                   │
│  Phase 1: Load config (symbols, risk, API)  │
│  Phase 2: Init agents + register tools      │
│  Phase 3: Start task manager                │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│            MARKET SCANNER (loop)             │
│  Every 60s, for each symbol:                │
│    1. Fetch OHLCV candlestick data          │
│    2. Run 15+ technical indicators          │
│    3. Check for 7 setup patterns            │
│    4. If setup found → send to Analyst      │
│                                              │
│  Detects: EMA cross, RSI OB/OS, MACD shift, │
│  Supertrend flip, BB squeeze, S/R test,     │
│  full trend alignment                        │
└──────────────────┬──────────────────────────┘
                   │ setup found
                   ▼
┌─────────────────────────────────────────────┐
│              ANALYST (deep dive)             │
│  1. Multi-timeframe check (15m/1h/4h/1d)    │
│  2. Find support/resistance levels          │
│  3. Calculate ATR (volatility)              │
│  4. Optimize Entry/TP1/TP2/TP3/SL           │
│     using ensemble of 3 strategies:         │
│     - ATR-based dynamic levels              │
│     - Structure (support/resistance)        │
│     - Fibonacci extensions                  │
│  5. Score confidence (0-100%)               │
│  6. Generate TradeSignal → send to Risk Mgr │
└──────────────────┬──────────────────────────┘
                   │ signal generated
                   ▼
┌─────────────────────────────────────────────┐
│           RISK MANAGER (gatekeeper)          │
│  Checks, in order:                          │
│  ✗ R:R < 2:1 → REJECT                      │
│  ✗ Portfolio heat > 6% → REJECT             │
│  ✗ Max 5 open trades → REJECT               │
│  ✗ Daily loss ≥ 3% → REJECT (circuit break) │
│  ✓ Pass all → calculate position size:      │
│      Kelly Criterion (25% fractional)       │
│  ✓ Approved → send to Executor              │
└──────────────────┬──────────────────────────┘
                   │ approved
                   ▼
┌─────────────────────────────────────────────┐
│            EXECUTOR (manage trade)           │
│  1. Place order (entry price)               │
│  2. Update portfolio state                  │
│  3. Start monitoring loop (every 5s):       │
│     - Check if SL hit → close, log loss     │
│     - Check if TP1 hit → move SL to B/E     │
│     - Check if TP2 hit → trail stop         │
│     - Check if TP3 hit → close, log profit  │
│  4. Update trailing stop (2× ATR)           │
│  5. Track P&L, win rate, drawdown           │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
              POSITION CLOSED
              Update portfolio stats
              Return to Scanner loop
```

### Concrete Example: XAUUSD Long Trade

```
Scanner detects: "Supertrend flipped bullish, RSI at 28 (oversold)"
                          │
                          ▼
Analyst says: "4H and 1D also bullish. Entry=3200, ATR=18"
  Ensemble optimizer picks Structure strategy:
    Entry:  3200.00
    SL:     3194.50  (below support, 1.5 ATR)
    TP1:    3218.00  (2.0 R, next resistance)
    TP2:    3236.00  (3.5 R)
    TP3:    3272.00  (5.0 R)
  Confidence: 78%
                          │
                          ▼
Risk Manager: "R:R = 3.27 ✓ | Kelly says 3.2% position ✓ | 
               Portfolio heat = 2.1% (< 6%) ✓ | Open trades = 2 (< 5) ✓"
  → APPROVED, size = $320 (3.2% of $10,000)
                          │
                          ▼
Executor: "Buy 0.10 lots XAUUSD @ 3200.00"
  Monitor:
    Price hits 3218 → TP1 hit, SL moves to 3200 (breakeven)
    Price hits 3236 → TP2 hit, trailing stop starts
    Price hits 3272 → TP3 hit, CLOSE
  Result: +$72 (+2.25% on position)
```

### What You See When Running

```
15:00:00 │ mempalace2.boot              │ INFO  │ ════════════════════════════════
15:00:00 │ mempalace2.boot              │ INFO  │   MEMPALACE2 AI — BOOT SEQUENCE
15:00:00 │ mempalace2.boot              │ INFO  │ ════════════════════════════════
15:00:00 │ mempalace2.boot              │ INFO  │   ✓ Phase 1: Config loaded (12ms)
15:00:01 │ mempalace2.boot              │ INFO  │   ✓ Phase 2: Agents & Tools ready (340ms)
15:00:01 │ mempalace2.boot              │ INFO  │   ✓ Phase 3: Task system active (5ms)
15:00:01 │ mempalace2.boot              │ INFO  │   BOOT COMPLETE — 357ms
15:00:01 │ mempalace2.agents.scanner    │ INFO  │ 🔍 XAUUSD: 2 setup(s) detected
15:00:02 │ mempalace2.agents.analyst    │ INFO  │ 📊 Analyzing: XAUUSD long [supertrend_flip_bullish]
15:00:03 │ mempalace2.agents.analyst    │ INFO  │ 📈 Signal: XAUUSD long Conf=78% R:R=3.3 Entry=3200
15:00:03 │ mempalace2.agents.risk_mgr   │ INFO  │ 🛡️ Risk check: XAUUSD long R:R=3.27
15:00:03 │ mempalace2.agents.risk_mgr   │ INFO  │ ✅ APPROVED: XAUUSD Size=3.2% R:R=3.27 EV=0.042
15:00:04 │ mempalace2.agents.executor   │ INFO  │ ⚡ Executing: XAUUSD long Entry=3200 SL=3194.5
15:00:04 │ mempalace2.agents.executor   │ INFO  │ ✅ Executed: XAUUSD Qty=0.10 Value=$320 Risk=1.1%
15:05:12 │ mempalace2.agents.executor   │ INFO  │ 🎯 TP1 hit: XAUUSD long @ 3218.00 (+0.56%)
15:05:12 │ mempalace2.agents.executor   │ INFO  │   → SL moved to breakeven: 3200.00
```

---

## What Works

- ✅ Boot pipeline (3-phase)
- ✅ Tool system (registry, interface, safe execution)
- ✅ Task lifecycle management
- ✅ Agent communication (message passing via coordinator)
- ✅ Technical analysis suite (15+ indicators)
- ✅ 7 setup detection patterns
- ✅ Multi-timeframe confirmation
- ✅ Kelly Criterion position sizing
- ✅ ATR-based dynamic TP/SL
- ✅ Risk validation pipeline
- ✅ Portfolio state tracking
- ✅ Position monitoring with trailing stops

## What Needs Work

- 🔲 Real exchange API integration (currently uses synthetic data fallback)
- 🔲 WebSocket live price feed
- 🔲 Backtesting engine with historical data
- 🔲 Web dashboard / API server
- 🔲 Telegram/Discord alerts for signals
- 🔲 ML model for signal confidence scoring
- 🔲 Correlation-aware position sizing
- 🔲 Database for trade history persistence
- 🔲 Unit tests

## Git

- **Repo:** https://github.com/Geowahaha/mempalace2_ai.git
- **Branch:** main
- **Auth:** Use personal access token (revoked old one, create fresh)
- **Push:** `git remote set-url origin https://USERNAME:TOKEN@github.com/Geowahaha/mempalace2_ai.git`

---

## 📝 Agent Notes (append below)

<!-- Each agent that works on this project should add a note here before leaving -->

### Session: 2026-04-15 (Initial Build)
- **Agent:** MiMo (first agent)
- **What I did:** Built entire system from scratch — 28 files, 3300+ lines
- **Context:** User linked Claude Code source deep dive course, wanted trading system inspired by it
- **XAUUSD focus:** User specified Gold trading, not crypto
- **Key insight:** Architecture mirrors Claude Code's tool/task/agent system but adapted for finance
- **Next priorities:** Real API integration, backtesting, dashboard

---

## 🚀 Next Instructions: Going Live

### Current State vs Live Trading

| Component | Status | What's Needed |
|-----------|--------|---------------|
| Trading logic | ✅ Done | — |
| Exchange API | 🔲 Synthetic only | Real broker connection |
| Live prices | 🔲 Fake | WebSocket feed |
| Order execution | 🔲 Simulated | Real order placement |
| Server | 🔲 Local only | 24/7 hosted machine |

### 1. Broker / Exchange Account

For XAUUSD (Gold), best options:

| Broker | API | Cost | Notes |
|--------|-----|------|-------|
| **OANDA** | REST + Streaming | Free practice, $0 commission | Best for gold forex, easy API |
| **IC Markets** | via MT5/MT4 | Low spreads | Needs MetaTrader bridge |
| **Pepperstone** | via cTrader API | Low spreads | Good API |
| **FXCM** | REST | Free demo | Simple but limited |

**Recommendation: OANDA** — ccxt support already in code, free practice account, clean API.

### 2. Server (24/7 Uptime)

The system needs to run continuously. Options:

| Option | Cost/mo | Setup |
|--------|---------|-------|
| **Your PC** + keep it on | $0 | Easiest but unreliable |
| **VPS (Vultr/DigitalOcean)** | $6-12 | Copy code, run it |
| **AWS EC2** | $8-15 | More complex |
| **Railway.app** | $5-20 | Push to GitHub, auto-deploys |

**Recommendation: Vultr $6/mo VPS** — Ubuntu, SSH in, `python -m mempalace2_ai`

### 3. Code Changes Needed for Live

```
Needed:
├── Live WebSocket price feed (instead of polling)
├── Real order placement (OANDA REST API)
├── Database (SQLite) for trade history
├── Telegram bot for alerts
├── Graceful shutdown / restart handling
└── Error recovery (reconnect on API failure)
```

### 4. Estimated Timeline

```
Phase 1: OANDA account + API keys           → 30 min (sign up, get keys)
Phase 2: Real exchange integration           → 2-3 hours (code it)
Phase 3: WebSocket live feed                 → 1-2 hours
Phase 4: Database + alerts                   → 1-2 hours
Phase 5: VPS setup + deploy                  → 1 hour
                                         Total: ~1 day
```

### 5. Before Going Live — MUST DO

1. **Paper trade first** — OANDA free practice account, zero risk
2. **Backtest** — run on 6 months of historical data
3. **Start small** — $500-1000 real money, 0.5% max position
4. **Monitor for 2 weeks** — watch every trade, check logs

### 6. Next Agent Tasks (Priority Order)

1. Set up OANDA practice account, integrate real API
2. Build WebSocket live price feed
3. Add SQLite database for trade history
4. Add Telegram/Discord alert bot
5. Build backtesting engine with historical XAUUSD data
6. Add web dashboard (FastAPI + simple frontend)
7. VPS deployment scripts
8. Paper trade for 2 weeks before going live with real money
