# 🏛️ HANDOFF.md — Mempalace2 AI (Hermes Integration)

> **To the next agent:** Read this file first. Understand the project. Update this file before you leave.

---

## Project Overview

**Mempalace2 AI** is an intelligent multi-agent trading system for **XAUUSD (Gold)**, upgraded with hermes-agent's self-improving agent framework.

**Current Status:** ✅ **FULLY WIRED AND TESTED.** All integration work is complete. The self-improving loop is live.

---

## What's Done (All Sessions)

### ✅ Session 1: 2026-04-15 — Foundation
- SQLite State Store with FTS5 (`enhanced/state_store.py`)
- Trade Memory System (`memory/store.py`)

### ✅ Session 2: 2026-04-15/16 — Hermes Integration (All 4 Phases)
- **Phase 1:** Trajectory Logger, Skill Framework, Skill Manager, 3 Trading Skills
- **Phase 2:** Context Engine, Prompt Builder, Scheduler/Reporter
- **Phase 3:** Enhanced Boot Pipeline, Entry Point, Requirements
- **Phase 4:** Enhanced Tool Registry (6 toolsets, circuit breaker)

### ✅ Session 3: 2026-04-16 — Wiring + Tests + Delegate (ALL COMPLETE)

| Task | Commit | What was done |
|------|--------|---------------|
| Wire agents | `98e0d9d` | Memory + skills injected into analyst; trajectory flows scanner→analyst→risk→executor; pattern/lesson/skill storage on trade close; circuit breaker wiring; context engine ticks |
| Wire registry | `98e0d9d` | EnhancedToolRegistry with 6 learning tools (store_pattern, recall_patterns, store_lesson, log_trajectory, list_skills, match_skills) |
| Delegate agent | `4ccdb59` | `agents/delegate.py` — parallel multi-symbol subagents, DELEGATE_BLOCKED_TOOLS, MAX_DEPTH=1, concurrency=3 |
| Integration tests | `c283d19` | `tests/test_integration.py` — 32 tests, all passing |
| Bugfix | `c283d19` | `skills/base.py` — string→list normalization, None-safe `in` checks |
| E2E verification | `e76ab88` | Boot verified: 6 agents, 3 skills, 6 tools, 5 reports |
| Handoff update | `e76ab88` | This file |

---

## Current System State

### Agents (6 active)
| Agent | File | Status | Key wiring |
|-------|------|--------|------------|
| Coordinator | `agents/coordinator.py` | ✅ | Orchestrates all agents, enhanced dashboard |
| Market Scanner | `agents/market_scanner.py` | ✅ | `context_engine.tick_scan()` + trajectory start on scan |
| Analyst | `agents/analyst.py` | ✅ | `memory.build_context_for_analysis()` + `skills.build_skills_context_block()` injected |
| Risk Manager | `agents/risk_manager.py` | ✅ | Trajectory steps on approve/reject, circuit breaker trip |
| Executor | `agents/executor.py` | ✅ | `memory.store_trade_pattern()`, `store_lesson()`, `skills.update_skill_from_trade()`, `state_store.record_trade()`, trajectory finalize |
| Delegate | `agents/delegate.py` | ✅ | Parallel multi-symbol scan, isolated analysis, depth-limited |

### Enhanced Components (all built + wired)
- **StateStore** (`enhanced/state_store.py`) — SQLite + FTS5, WAL mode, ACID
- **TradeMemory** (`memory/store.py`) — pattern recall, lesson learning, `<trade-memory>` fencing
- **SkillsManager** (`skills/manager.py`) — 3 skills, auto-creation, EMA win rate updates
- **TrajectoryLogger** (`trajectories/logger.py`) — JSONL + ShareGPT, per-decision logging
- **ContextEngine** (`enhanced/context_engine.py`) — token budget, scan ticks, compression
- **PromptBuilder** (`enhanced/prompt_builder.py`) — dynamic prompts with memory injection
- **Scheduler** (`scheduler/reporter.py`) — 5 periodic reports, async loop
- **EnhancedToolRegistry** (`enhanced/tools/trading_registry.py`) — 6 toolsets, circuit breaker

### Trading Skills (3 discovered)
1. `supertrend-reversal` — Catch supertrend direction flips with RSI confirmation
2. `bb-squeeze-breakout` — Trade Bollinger Band squeeze breakouts with volume
3. `ema-crossover-mtf` — EMA crossover with multi-timeframe alignment

### Tests
- `tests/test_integration.py` — **32 tests, all passing**
- Covers: StateStore, TradeMemory, SkillManager, TrajectoryLogger, ContextEngine, EnhancedToolRegistry, EnhancedBoot, DelegateAgent, EndToEndPipeline

---

## Self-Improving Loop (Active)

```
1. Market scan → detect setup → start trajectory + context_engine.tick
2. Analyst → recall similar patterns (memory) → match skills → inject context
3. RiskManager → validate → trajectory step
4. Executor → enter trade → trajectory records decision
5. Trade closes → store pattern + lesson → update skill win rate → finalize trajectory
6. Next scan → updated memory/skills improve future decisions
7. Scheduler → periodic performance reports
```

---

## How to Run

```bash
cd mempalace2_ai

# Run tests
python3 -m unittest tests.test_integration -v

# Boot enhanced mode (from parent of mempalace2_ai/)
cd /root/.openclaw/workspace
python3 -c "
import sys; sys.path.insert(0, 'mempalace2_ai')
import asyncio
from enhanced.boot import EnhancedBootPipeline
async def main():
    pipeline = EnhancedBootPipeline(db_path='/tmp/test.db', trajectory_dir='/tmp/traj/')
    state = await pipeline.boot()
    print(f'Agents: {list(state.agents.keys())}')
    print(f'Skills: {len(state.skills_manager._skills)}')
    await state.scheduler.stop()
asyncio.run(main())
"

# Live trading (requires API keys in config/settings.yaml)
python3 -m mempalace2_ai --enhanced --symbols XAUUSD --log-level DEBUG
```

---

## What's Left (Optional Enhancements)

Everything functional is done. These are nice-to-haves:

| Priority | Enhancement | Notes |
|----------|-------------|-------|
| Low | Live API integration | Needs exchange API keys in `config/settings.yaml` |
| Low | Backtest mode | `__main__.py` has `--backtest` flag stub |
| Low | More trading skills | Add SKILL.md files in `skills/trading/` |
| Low | Web dashboard | Current dashboard is log-based |
| Low | Skill auto-creation from losses | Partially implemented in `skills/manager.py` |

---

## File Structure

```
mempalace2_ai/
├── __main__.py                    # Entry point (--enhanced, --symbols, --log-level)
├── __init__.py
├── .gitignore
├── core/
│   ├── boot.py                    # Original boot (unchanged)
│   ├── state.py                   # GlobalState + TradeSignal + ActiveTrade + PortfolioState
│   └── task.py                    # Task lifecycle
├── agents/
│   ├── coordinator.py             # Orchestrator — 5 sub-agents
│   ├── market_scanner.py          # Scan loop — context engine tick + trajectory start
│   ├── analyst.py                 # Analysis — memory + skills injection
│   ├── risk_manager.py            # Risk gate — trajectory steps + circuit breaker
│   ├── executor.py                # Execution — pattern/lesson/skill storage on close
│   └── delegate.py                # Parallel multi-symbol subagents
├── tools/
│   ├── base.py, registry.py       # Tool framework
│   ├── market_data.py             # OHLCV fetching
│   ├── technical.py               # 15+ indicators
│   └── risk_engine.py             # Kelly sizing, position calc
├── strategies/
│   └── optimizer.py               # ATR/Structure/Fibonacci/Ensemble
├── config/
│   ├── settings.py                # AppConfig dataclasses
│   └── settings.yaml              # Default config
├── enhanced/
│   ├── __init__.py
│   ├── state_store.py             # SQLite + FTS5
│   ├── context_engine.py          # Token budget management
│   ├── prompt_builder.py          # Dynamic prompt assembly
│   ├── boot.py                    # 3-phase enhanced boot
│   └── tools/
│       └── trading_registry.py    # 6 toolsets + circuit breaker
├── memory/
│   ├── __init__.py
│   └── store.py                   # Patterns, lessons, recall
├── trajectories/
│   ├── __init__.py
│   └── logger.py                  # JSONL + ShareGPT logging
├── skills/
│   ├── __init__.py
│   ├── base.py                    # SKILL.md framework
│   ├── manager.py                 # Auto-creation + self-learning
│   └── trading/
│       ├── supertrend-reversal/SKILL.md
│       ├── bb-squeeze-breakout/SKILL.md
│       └── ema-crossover-mtf/SKILL.md
├── scheduler/
│   ├── __init__.py
│   └── reporter.py                # 5 periodic reports
├── tests/
│   └── test_integration.py        # 32 tests, all passing
├── requirements.txt
├── HANDOFF.md                     # This file
└── README.md
```

---

## Git History

```
e76ab88 docs: update HANDOFF.md — all tasks complete
4ccdb59 feat: add delegate agent for parallel multi-symbol analysis (Task 3)
c283d19 feat: integration tests + skills bugfix (Task 4)
98e0d9d feat: wire hermes components into agent loop (Task 1 & 2)
c14aa4a docs: update HANDOFF.md with full status and next-agent instructions
eb7614f feat: add enhanced tool registry with toolsets and risk gating (Phase 4)
61dbfb6 feat: add enhanced boot pipeline, update entry point and requirements (Phase 3)
4abd42f feat: add context engine, prompt builder, and scheduler (Phase 2)
e6af4aa feat: add trajectory logger and skill framework (Phase 1)
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| SQLite over JSONL | FTS5 search, ACID transactions, concurrent reads |
| Fenced memory blocks | `<trade-memory>` tags prevent model confusion with market data |
| agentskills.io format | Portable, discoverable, progressive disclosure |
| Trajectory JSONL | ShareGPT format for fine-tuning data |
| WAL mode SQLite | Concurrent readers + single writer |
| Keep original agents | Incremental upgrade — don't break working system |
| `--enhanced` opt-in | Standard boot still works without hermes |

---

_Last updated: 2026-04-16 01:37 GMT+8_
