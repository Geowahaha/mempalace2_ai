# 🏛️ Mempalace2 AI — Intelligent Multi-Agent Trading System

> Architecture inspired by Claude Code's multi-agent tool system.
> Redesigned for autonomous trading with optimal Entry/TP/SL analysis.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  COORDINATOR AGENT                    │
│         (Orchestrates all sub-agents + tools)         │
├──────────┬──────────┬──────────┬─────────────────────┤
│ Market   │ Analyst  │ Risk     │ Executor            │
│ Scanner  │ Agent    │ Manager  │ Agent               │
│ Agent    │          │ Agent    │                     │
├──────────┴──────────┴──────────┴─────────────────────┤
│                    TOOL SYSTEM                        │
│  MarketData · Technical · RiskEngine · Portfolio      │
├──────────────────────────────────────────────────────┤
│                    TASK SYSTEM                        │
│  Scan → Analyze → Validate → Execute → Monitor       │
└──────────────────────────────────────────────────────┘
```

## Core Design Principles

1. **Multi-Agent Coordination** — Each agent has a specialized role
2. **Risk-First** — Every trade passes through the Risk Manager before execution
3. **Optimal Entry/TP/SL** — AI-driven analysis for max profit, min risk
4. **Task Lifecycle** — Every scan/analysis/trade tracked as a managed task
5. **Streaming Real-Time** — Market data flows through the pipeline continuously

## Modules

| Module | Description |
|--------|-------------|
| `core/` | Boot sequence, state management, task lifecycle |
| `agents/` | Coordinator, Scanner, Analyst, Risk Manager, Executor |
| `tools/` | Market data, technical analysis, risk engine, portfolio |
| `strategies/` | Entry/TP/SL optimization algorithms |
| `analysis/` | Signal generation, multi-timeframe analysis |
| `config/` | Settings, exchange configs, risk parameters |

## Quick Start

```bash
pip install -r requirements.txt
python -m mempalace2_ai --config config/settings.yaml
```

## Risk Model

Every trade is evaluated on:
- **Risk/Reward Ratio** — Minimum 2:1 enforced
- **Kelly Criterion** — Position sizing for optimal growth
- **Sharpe-Adjusted Entry** — Entry price optimized for expected return/volatility
- **Dynamic TP/SL** — Adapted to volatility regime (ATR-based)
- **Portfolio Heat** — Max 6% total portfolio risk at any time
