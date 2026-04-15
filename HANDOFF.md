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
