# 🏛️ HANDOFF.md — Mempalace2 AI (Hermes Integration Branch)

> **To the next agent:** Read this file first. Understand the project. Update this file before you leave.

---

## Project Overview

**Mempalace2 AI** is an intelligent multi-agent trading system for **XAUUSD (Gold)**, now being upgraded with hermes-agent's self-improving agent framework.

**Current Phase:** Leveling up to a self-improving trading AI agent by integrating NousResearch/hermes-agent core systems.

## What's Done (2026-04-15 Session)

### ✅ Completed

| Component | File | Status |
|-----------|------|--------|
| SQLite State Store (FTS5) | `enhanced/state_store.py` | ✅ Done — 550+ lines, full schema, migrations, FTS5 search, analytics |
| Trade Memory System | `memory/store.py` | ✅ Done — pattern recall, lesson learning, context building |
| Package inits | `enhanced/__init__.py`, `memory/__init__.py` | ✅ Done |

### What the State Store provides (hermes-agent pattern adapted):
- WAL-mode SQLite with thread-local connections
- FTS5 full-text search across trade reasoning/strategy
- Tables: sessions, trade_history, signal_history, trade_patterns, market_context_snapshots, learning_events
- Schema migrations (v1→v3)
- Analytics: trade stats, strategy performance, hourly performance

### What the Memory System provides:
- PatternMemory: stores setup patterns with win rates, recalls similar patterns by symbol/type
- LessonMemory: stores mistakes/insights as heuristics, context-keyword matching
- Context building for analyst agent (`<trade-memory>` fenced blocks, hermes pattern)
- System prompt block generation with top patterns and recent lessons

---

## 🔲 REMAINING — Next Agent Tasks (Priority Order)

These files still need to be created to complete the hermes-agent integration:

### Phase 1: Core Infrastructure (HIGH PRIORITY)

**1. `trajectories/logger.py`** — Trade Decision Trajectory Logger
- Adapted from `hermes-agent/agent/trajectory.py`
- Logs every trade decision (scan → analyze → risk check → execute/close) as a trajectory
- JSONL format for fine-tuning data generation
- Captures: input context, reasoning, tool calls, outcome
- Enable `--backtest --trajectory-out` for batch trajectory generation
- Key hermes concepts: `save_trajectory()`, ShareGPT format, `convert_scratchpad_to_think()`

**2. `skills/base.py`** — Trading Skill Framework
- Adapted from `hermes-agent/tools/skills_tool.py`
- agentskills.io compatible SKILL.md format with YAML frontmatter
- Each trading pattern becomes a skill with metadata, conditions, outcomes
- Progressive disclosure: metadata (name, description) → full instructions → linked files
- Key hermes concepts: `SkillEntry`, `skills_list()`, `skill_view()`, frontmatter parsing

**3. `skills/manager.py`** — Skill Manager
- Discovers, loads, and manages trading skills
- Auto-creates skills from successful trades (hermes "self-learning loop")
- Skill improvement: updates win rate, refines conditions after each trade
- Key hermes concepts: `discover_builtin_skills()`, skill matching, platform conditions

**4. `skills/trading/`** — Initial Trading Skills (3 directories with SKILL.md)
```
skills/trading/
├── supertrend-reversal/
│   └── SKILL.md     # When supertrend flips + RSI confirmation → high WR
├── bb-squeeze-breakout/
│   └── SKILL.md     # BB width minimum + volume spike → breakout trade
└── ema-crossover-mtf/
│   └── SKILL.md     # EMA cross + multi-timeframe alignment → trend trade
```

### Phase 2: Agent Enhancements (MEDIUM PRIORITY)

**5. `enhanced/context_engine.py`** — Context Compression for Long Sessions
- Adapted from `hermes-agent/agent/context_engine.py`
- Abstract base class + Compressor implementation
- Manages token budget for long-running trading sessions (hours/days)
- Compresses old scan results, keeps recent trades + active positions
- Key hermes concepts: `ContextEngine` ABC, `should_compress()`, `compress()`, threshold management

**6. `scheduler/reporter.py`** — Scheduled Reports & Periodic Tasks
- Adapted from `hermes-agent/cron/` system
- Daily P&L report, weekly performance review, hourly market snapshot
- Telegram/Discord alert integration hooks
- Key hermes concepts: cron expressions, job scheduling, delivery modes

**7. `enhanced/prompt_builder.py`** — System Prompt Assembly
- Adapted from `hermes-agent/agent/prompt_builder.py`
- Builds dynamic system prompts for LLM-powered analysis
- Injects: trading memory, active skills, portfolio state, risk parameters
- Prompt injection defense scanning (hermes pattern)
- Key hermes concepts: `build_system_prompt()`, context file scanning, `_CONTEXT_THREAT_PATTERNS`

### Phase 3: Integration & Entry Point (MEDIUM PRIORITY)

**8. `enhanced/boot.py`** — Enhanced Boot Pipeline
- Extends existing `core/boot.py` with hermes components
- Phase 1: Config + State Store init
- Phase 2: Agents + Memory + Skills + Tool Registry
- Phase 3: Scheduler + Trajectory logger + First scan
- Key hermes concepts: `BootMetrics`, phased startup, health checks

**9. Update `__main__.py`** — New Entry Point
- Add `--enhanced` flag to use hermes-integrated boot
- Add `--trajectory-out` for trajectory logging mode
- Add `--memory-db` for custom state store path
- Dashboard shows: memory stats, skill count, trajectory count

**10. Update `requirements.txt`**
- Add: `openai>=2.21.0` (for LLM-powered analysis if desired)
- Add: `jinja2>=3.1.5` (for prompt templates)
- Add: `croniter>=6.0.0` (for scheduler)
- Keep existing deps (numpy, pandas, ccxt, etc.)

### Phase 4: Advanced Features (LOW PRIORITY)

**11. `enhanced/tools/trading_registry.py`** — Enhanced Tool Registry
- Adapted from `hermes-agent/tools/registry.py`
- Toolsets: `market_data`, `analysis`, `risk`, `execution`, `portfolio`, `learning`
- Risk-based tool gating (block execution tools when circuit breaker trips)
- Key hermes concepts: `ToolEntry`, `ToolRegistry`, toolset checks, thread-safe

**12. Subagent / Delegate System**
- Adapted from `hermes-agent/tools/delegate_tool.py`
- Spawn parallel analysis subagents for multi-symbol scanning
- Isolated backtesting subagents that don't affect live state
- Key hermes concepts: `DELEGATE_BLOCKED_TOOLS`, `MAX_DEPTH`, batch mode

**13. `.gitignore`** — Standard Python gitignore
```
__pycache__/
*.pyc
*.pyo
*.db
*.db-wal
*.db-shm
.env
.venv/
dist/
*.egg-info/
```

---

## Architecture (Target State)

```
COORDINATOR (orchestrator)
  ├── MarketScanner → scans markets, detects 7 setup types
  ├── Analyst → multi-timeframe + memory recall + skill matching
  ├── RiskManager → Kelly sizing, portfolio heat, circuit breakers
  └── Executor → trailing stops, TP monitoring, auto-close

ENHANCED LAYER (hermes-agent integration)
  ├── StateStore (SQLite + FTS5) → persistent trade/signal/pattern history
  ├── TradeMemory → pattern recall, lesson learning, context building
  ├── SkillsManager → self-improving trading skills (agentskills.io)
  ├── TrajectoryLogger → decision logging for fine-tuning
  ├── ContextEngine → token budget management for long sessions
  ├── Scheduler → periodic reports, alerts, market snapshots
  └── PromptBuilder → dynamic system prompts with memory injection

TOOLS (enhanced registry with toolsets)
  ├── market_data (XAUUSD via OANDA/MT5, synthetic fallback)
  ├── technical_analysis (EMA, RSI, MACD, ATR, Supertrend, BB, ADX)
  ├── risk_engine (Kelly Criterion, R:R enforcement, Sharpe-adjusted)
  ├── learning (pattern store, lesson recall, trajectory log)
  └── execution (order placement, trailing stops, position monitor)
```

## File Structure (Current + Planned)

```
mempalace2_ai/
├── __main__.py                    # Entry point (needs --enhanced flag)
├── __init__.py
├── core/
│   ├── boot.py                    # Original boot (keep, extend)
│   ├── state.py                   # GlobalState (extend with memory/skills refs)
│   └── task.py                    # Task lifecycle
├── agents/
│   ├── coordinator.py             # Orchestrator (add memory/skills injection)
│   ├── market_scanner.py          # Scanner (add pattern recall)
│   ├── analyst.py                 # Analyst (add memory context building)
│   ├── risk_manager.py            # Risk gate (keep)
│   └── executor.py                # Trade executor (add trajectory logging)
├── tools/
│   ├── base.py                    # Tool interface (keep)
│   ├── registry.py                # Original registry (extend or replace)
│   ├── market_data.py             # OHLCV fetching
│   ├── technical.py               # 15+ indicators
│   └── risk_engine.py             # Position sizing
├── strategies/
│   └── optimizer.py               # ATR/Structure/Fibonacci/Ensemble
├── config/
│   ├── settings.py                # AppConfig dataclasses
│   └── settings.yaml              # Default config
├── enhanced/                      # NEW — hermes integration layer
│   ├── __init__.py                # ✅ Done
│   ├── state_store.py             # ✅ Done — SQLite + FTS5 state store
│   ├── context_engine.py          # 🔲 TODO
│   ├── prompt_builder.py          # 🔲 TODO
│   ├── boot.py                    # 🔲 TODO
│   └── tools/
│       └── trading_registry.py    # 🔲 TODO
├── memory/                        # NEW — trade memory system
│   ├── __init__.py                # ✅ Done
│   └── store.py                   # ✅ Done — patterns, lessons, recall
├── trajectories/                  # NEW — decision logging
│   └── logger.py                  # 🔲 TODO
├── skills/                        # NEW — self-improving trading skills
│   ├── base.py                    # 🔲 TODO
│   ├── manager.py                 # 🔲 TODO
│   └── trading/                   # 🔲 TODO (3 SKILL.md files)
├── scheduler/                     # NEW — periodic tasks
│   └── reporter.py                # 🔲 TODO
└── requirements.txt               # Needs update
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

## How to Continue

```bash
cd /root/.openclaw/workspace/mempalace2_ai

# Read the existing code
cat enhanced/state_store.py     # Understand the state store
cat memory/store.py             # Understand the memory system

# Start with trajectories/logger.py (highest priority)
# Then skills/base.py + skills/manager.py
# Then enhanced/context_engine.py
# Then the rest

# After each file:
git add -A && git commit -m "feat: add [component]" && git push
```

## References

- **hermes-agent repo:** `/root/.openclaw/workspace/hermes-agent/`
- **Key hermes files to study:**
  - `agent/trajectory.py` — trajectory saving
  - `tools/skills_tool.py` — skill framework
  - `agent/context_engine.py` — context compression
  - `agent/memory_manager.py` — memory orchestration
  - `tools/registry.py` — tool registry
  - `agent/prompt_builder.py` — prompt assembly
  - `hermes_state.py` — SQLite state store

---

## 📝 Agent Notes

### Session: 2026-04-15 (Hermes Integration — Part 1)
- **Agent:** MiMo via OpenClaw
- **What I did:** Created enhanced state store (SQLite+FTS5) and trade memory system
- **Files created:** `enhanced/state_store.py` (550+ lines), `memory/store.py` (250+ lines), package inits
- **Architecture insight:** hermes-agent's key innovation is the self-learning loop (skills from experience + memory recall + trajectory logging). The trading system already has solid agent architecture; we're adding the "brain" layer.
- **Next agent:** Start with `trajectories/logger.py` — it's the simplest and enables the fine-tuning data pipeline immediately.

---
