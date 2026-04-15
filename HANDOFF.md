# 🏛️ HANDOFF.md — Mempalace2 AI (Hermes Integration Branch)

> **To the next agent:** Read this file first. Understand the project. Update this file before you leave.

---

## Project Overview

**Mempalace2 AI** is an intelligent multi-agent trading system for **XAUUSD (Gold)**, now upgraded with hermes-agent's self-improving agent framework.

**Current Phase:** Core hermes-agent integration is **COMPLETE**. All 4 phases built and pushed. Next work is **integration wiring + testing + agent refinement**.

## What's Done

### ✅ Session 1: 2026-04-15 (Foundation)

| Component | File | Status |
|-----------|------|--------|
| SQLite State Store (FTS5) | `enhanced/state_store.py` | ✅ 550+ lines, full schema, migrations, FTS5 search, analytics |
| Trade Memory System | `memory/store.py` | ✅ Pattern recall, lesson learning, context building |
| Package inits | `enhanced/__init__.py`, `memory/__init__.py` | ✅ |

### ✅ Session 2: 2026-04-15/16 (Hermes Integration — All 4 Phases)

| Component | File | Phase | Status |
|-----------|------|-------|--------|
| Trajectory Logger | `trajectories/logger.py` | 1 | ✅ JSONL + ShareGPT format, convert_scratchpad_to_think() |
| Skill Base Framework | `skills/base.py` | 1 | ✅ SKILL.md parsing, SkillEntry, condition matching |
| Skill Manager | `skills/manager.py` | 1 | ✅ Auto-creation from trades, self-learning loop, EMA updates |
| Trading Skills ×3 | `skills/trading/*/SKILL.md` | 1 | ✅ supertrend-reversal, bb-squeeze-breakout, ema-crossover-mtf |
| Context Engine | `enhanced/context_engine.py` | 2 | ✅ ABC + TradingContextCompressor, threshold management |
| Prompt Builder | `enhanced/prompt_builder.py` | 2 | ✅ Dynamic prompt assembly, injection defense, fenced blocks |
| Scheduler/Reporter | `scheduler/reporter.py` | 2 | ✅ 5 periodic reports, delivery modes, async loop |
| Enhanced Boot | `enhanced/boot.py` | 3 | ✅ 3-phase boot with all hermes components |
| Entry Point Update | `__main__.py` | 3 | ✅ --enhanced, --memory-db, --trajectory-out flags |
| Requirements | `requirements.txt` | 3 | ✅ Added openai, jinja2, croniter |
| Enhanced Tool Registry | `enhanced/tools/trading_registry.py` | 4 | ✅ 6 toolsets, circuit breaker, LLM schema export |
| .gitignore | `.gitignore` | 3 | ✅ Standard Python gitignore |

---

## ✅ DONE — Session 3: 2026-04-16 (Wiring + Tests + Delegate)

| Task | Commit | What |
|------|--------|------|
| Task 1 — Wire agents | `98e0d9d` | Memory + skills in analyst, trajectory flow scanner→analyst→risk→executor, pattern/lesson storage on close, circuit breaker, context engine ticks |
| Task 2 — Wire registry | `98e0d9d` | EnhancedToolRegistry with 6 learning tools |
| Task 3 — Delegate agent | `c283d19`+ | `agents/delegate.py` — parallel multi-symbol subagents, isolated analysis, depth-limited |
| Task 4 — Integration tests | `c283d19` | 29 tests, all passing |
| Bugfix | `c283d19` | `skills/base.py` — string→list normalization for conditions, None-safe checks |

## 🔲 REMAINING — What the Next Agent Must Do

These are the integration, wiring, and testing tasks to make the system fully functional:

### Task 1: Wire hermes components into existing agents (HIGH PRIORITY)

The enhanced components exist but the **original agents don't use them yet**. You need to inject memory, skills, and trajectory logging into the agent decision loop.

**Files to modify:**

1. **`agents/coordinator.py`** — After analysis, call:
   ```python
   state.memory.build_context_for_analysis(symbol, setup_type, direction)
   state.skills_manager.match_skills(context)
   state.trajectory_logger.add_step(tid, "analysis", {...})
   ```

2. **`agents/analyst.py`** — Before generating analysis, inject:
   ```python
   context_block = state.memory.build_context_for_analysis(...)
   skills_block = state.skills_manager.build_skills_context_block(context)
   ```

3. **`agents/executor.py`** — After trade close, call:
   ```python
   state.memory.store_trade_pattern(symbol, setup, direction, conditions, outcome)
   state.memory.store_lesson(...)
   state.skills_manager.update_skill_from_trade(skill_name, won, pnl_pct, rr)
   state.state_store.record_trade(session_id, trade_dict)
   state.trajectory_logger.finalize(tid, "executed", outcome)
   ```

4. **`agents/risk_manager.py`** — On circuit breaker trip:
   ```python
   # If using enhanced registry:
   state.enhanced_registry.set_circuit_breaker(True)
   ```

5. **`agents/market_scanner.py`** — After each scan:
   ```python
   state.context_engine.tick_scan()
   if state.context_engine.should_compress():
       # Compress context
   ```

### Task 2: Update Enhanced Boot to wire registry (HIGH PRIORITY)

**File: `enhanced/boot.py`** — In `_phase_agents_enhanced()`, after creating all components:

```python
# Wire learning tools into enhanced registry
from enhanced.tools.trading_registry import (
    EnhancedToolRegistry, make_memory_tools, make_trajectory_tools, make_skill_tools
)

enhanced_registry = EnhancedToolRegistry(base_registry=base_tool_registry)
enhanced_registry.register_from_base()

# Register learning tools
for name, handler in make_memory_tools(self.state.memory).items():
    enhanced_registry.register(name, handler, toolset="learning")
for name, handler in make_trajectory_tools(self.state.trajectory_logger).items():
    enhanced_registry.register(name, handler, toolset="learning")
for name, handler in make_skill_tools(self.state.skills_manager).items():
    enhanced_registry.register(name, handler, toolset="learning")

self.state.enhanced_registry = enhanced_registry
```

### ✅ Task 3: Add Subagent / Delegate System — DONE

**File: `agents/delegate.py`** — Adapted from `hermes-agent/tools/delegate_tool.py`

- ✅ Parallel multi-symbol scanning with `delegate_parallel_scan()`
- ✅ Isolated analysis subagents (no trade execution, no risk override)
- ✅ `DELEGATE_BLOCKED_TOOLS` — children can't execute trades or set circuit breakers
- ✅ `MAX_DEPTH=1` — no recursive delegation
- ✅ Concurrency capped at `_MAX_CONCURRENT_CHILDREN=3`
- ✅ Trajectory logging per delegation
- ✅ Memory + skills read-only access for context
- ✅ Wired into CoordinatorAgent as sub-agent

### ✅ Task 4: Integration Testing — DONE

**File: `tests/test_integration.py`** — 29 tests, all passing

```python
# Test the full enhanced pipeline:
# 1. Boot with --enhanced
# 2. Verify StateStore creates tables
# 3. Verify skills are discovered
# 4. Simulate a trade → verify memory stores pattern
# 5. Simulate trade close → verify skill win rate updates
# 6. Verify trajectory JSONL is written
# 7. Verify scheduler reports generate
# 8. Verify context engine compresses after N scans
```

### Task 5: End-to-End Test Run (MEDIUM PRIORITY)

```bash
cd /root/.openclaw/workspace/mempalace2_ai
python -m mempalace2_ai --enhanced --symbols XAUUSD --log-level DEBUG
```

Verify:
- Boot completes with all "✓" components
- Skills discovered (should be 3)
- Market scans trigger trajectory logging
- Memory system recalls patterns
- Scheduler generates periodic reports
- Graceful shutdown flushes trajectories + ends session

---

## How to Continue

```bash
cd /root/.openclaw/workspace/mempalace2_ai

# 1. Understand the architecture
cat HANDOFF.md                    # This file
ls -la enhanced/ memory/ trajectories/ skills/ scheduler/

# 2. Read the existing agents to understand where to inject
cat agents/coordinator.py         # Orchestrator — main injection point
cat agents/analyst.py             # Analysis agent — inject memory + skills
cat agents/executor.py            # Trade executor — store patterns + lessons
cat agents/market_scanner.py      # Scanner — context engine ticks

# 3. Read the enhanced components you'll wire in
cat enhanced/state_store.py       # SQLite state store
cat memory/store.py               # Trade memory
cat skills/manager.py             # Skill manager
cat trajectories/logger.py        # Trajectory logger
cat enhanced/context_engine.py    # Context compressor
cat enhanced/prompt_builder.py    # Prompt builder
cat scheduler/reporter.py         # Periodic reports
cat enhanced/tools/trading_registry.py  # Enhanced tool registry

# 4. Start with Task 1: Wire agents (highest impact)
# 5. Then Task 2: Wire registry in boot
# 6. Then Task 4: Write integration tests
# 7. Then Task 5: End-to-end test run

# After each task:
git add -A && git commit -m "feat: [description]" && git push
```

---

## Architecture (Current State)

```
COORDINATOR (orchestrator) ← NEEDS WIRING
  ├── MarketScanner → scans markets, detects 7 setup types
  │     └── [MISSING] context_engine.tick_scan() after each scan
  ├── Analyst → multi-timeframe + memory recall + skill matching
  │     └── [MISSING] inject memory.build_context_for_analysis() + skills.match_skills()
  ├── RiskManager → Kelly sizing, portfolio heat, circuit breakers
  │     └── [MISSING] enhanced_registry.set_circuit_breaker() on trip
  └── Executor → trailing stops, TP monitoring, auto-close
        └── [MISSING] memory.store_trade_pattern() + skills.update_skill_from_trade()

ENHANCED LAYER (hermes-agent integration) ← ALL BUILT
  ├── StateStore (SQLite + FTS5) → persistent trade/signal/pattern history ✅
  ├── TradeMemory → pattern recall, lesson learning, context building ✅
  ├── SkillsManager → self-improving trading skills (agentskills.io) ✅
  ├── TrajectoryLogger → decision logging for fine-tuning ✅
  ├── ContextEngine → token budget management for long sessions ✅
  ├── Scheduler → periodic reports, alerts, market snapshots ✅
  ├── PromptBuilder → dynamic system prompts with memory injection ✅
  └── EnhancedToolRegistry → 6 toolsets, circuit breaker, LLM schemas ✅

DATA FLOW (after wiring):
  Scan → [context_engine.tick] → Analyst → [memory.recall + skills.match]
  → RiskManager → [trajectory_logger.add_step] → Executor
  → [memory.store + skills.update + trajectory_logger.finalize]
  → [scheduler generates periodic reports]
```

## File Structure (Current)

```
mempalace2_ai/
├── __main__.py                    # ✅ --enhanced flag, enhanced dashboard
├── __init__.py
├── .gitignore                     # ✅
├── core/
│   ├── boot.py                    # Original boot (unchanged)
│   ├── state.py                   # GlobalState
│   └── task.py                    # Task lifecycle
├── agents/
│   ├── coordinator.py             # ⚠️ NEEDS wiring
│   ├── market_scanner.py          # ⚠️ NEEDS wiring
│   ├── analyst.py                 # ⚠️ NEEDS wiring
│   ├── risk_manager.py            # ⚠️ NEEDS wiring
│   └── executor.py                # ⚠️ NEEDS wiring
├── tools/
│   ├── base.py                    # Original tool interface
│   ├── registry.py                # Original registry
│   ├── market_data.py             # OHLCV fetching
│   ├── technical.py               # 15+ indicators
│   └── risk_engine.py             # Position sizing
├── strategies/
│   └── optimizer.py               # ATR/Structure/Fibonacci/Ensemble
├── config/
│   ├── settings.py                # AppConfig dataclasses
│   └── settings.yaml              # Default config
├── enhanced/                      # ✅ ALL DONE
│   ├── __init__.py
│   ├── state_store.py             # ✅ SQLite + FTS5 state store
│   ├── context_engine.py          # ✅ Token budget management
│   ├── prompt_builder.py          # ✅ Dynamic prompt assembly
│   ├── boot.py                    # ✅ 3-phase enhanced boot
│   └── tools/
│       └── trading_registry.py    # ✅ 6 toolsets + circuit breaker
├── memory/                        # ✅ ALL DONE
│   ├── __init__.py
│   └── store.py                   # ✅ Patterns, lessons, recall
├── trajectories/                  # ✅ ALL DONE
│   ├── __init__.py
│   └── logger.py                  # ✅ JSONL + ShareGPT logging
├── skills/                        # ✅ ALL DONE
│   ├── __init__.py
│   ├── base.py                    # ✅ SKILL.md framework
│   ├── manager.py                 # ✅ Auto-creation + self-learning
│   └── trading/
│       ├── __init__.py
│       ├── supertrend-reversal/SKILL.md
│       ├── bb-squeeze-breakout/SKILL.md
│       └── ema-crossover-mtf/SKILL.md
├── scheduler/                     # ✅ ALL DONE
│   ├── __init__.py
│   └── reporter.py                # ✅ 5 periodic reports
├── tests/
│   └── __init__.py
└── requirements.txt               # ✅ Updated
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| SQLite over JSONL | hermes-agent pattern — FTS5 search, ACID transactions, concurrent reads |
| Fenced memory blocks | hermes pattern — `<trade-memory>` tags prevent model confusion |
| agentskills.io format | hermes standard — portable, discoverable, progressive disclosure |
| Trajectory JSONL | hermes pattern — ShareGPT format for fine-tuning data |
| WAL mode SQLite | hermes pattern — concurrent readers + single writer |
| Keep original agents | Incremental upgrade — don't break working system |
| Enhanced boot as opt-in | `--enhanced` flag — standard boot still works without hermes |

## References

- **hermes-agent repo:** https://github.com/NousResearch/hermes-agent
- **Key hermes files studied:**
  - `agent/trajectory.py` → adapted to `trajectories/logger.py`
  - `tools/skills_tool.py` → adapted to `skills/base.py` + `skills/manager.py`
  - `agent/context_engine.py` → adapted to `enhanced/context_engine.py`
  - `agent/prompt_builder.py` → adapted to `enhanced/prompt_builder.py`
  - `tools/registry.py` → adapted to `enhanced/tools/trading_registry.py`

---

## 📝 Agent Notes

### Session 1: 2026-04-15 (Foundation)
- Created enhanced state store (SQLite+FTS5) and trade memory system
- Files: `enhanced/state_store.py`, `memory/store.py`, package inits

### Session 2: 2026-04-15/16 (Full Hermes Integration)
- Built all 4 phases of hermes-agent integration in one session
- 13 new files, ~30,000 lines of adapted code
- All committed and pushed to main
- **Key insight:** The building blocks are done. The remaining work is WIRING — connecting the hermes components into the existing agent decision loop. This is the difference between "components exist" and "system is self-improving."

### How the Self-Improving Loop Works (after wiring):
1. **Market scan** → detect setup → log to trajectory
2. **Analysis** → recall similar patterns from memory → match skills → build context
3. **Risk check** → size position → validate against circuit breaker
4. **Execute** → enter trade → trajectory records decision
5. **Trade closes** → store outcome as pattern → update skill win rate → log lesson
6. **Next scan** → updated memory/skills improve future decisions
7. **Periodic reports** → summarize performance, skill stats, memory growth

---

_Last updated: 2026-04-16 00:50 GMT+8 by MiMo via OpenClaw_
