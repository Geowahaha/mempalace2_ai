# Mempalace Trader Architecture

## Repo shape

- `trading_ai/` holds the Python package and all trading logic.
- `scripts/` holds Windows-first bootstrap and local test-server commands.
- `docs/` holds operational notes for running beside Dexter.
- `data/` and `logs/` are runtime-only and must stay outside Dexter.

## Runtime layers

1. `trading_ai.main`
   Runs the continuous learning loop, risk gates, execution service, and runtime recovery.
2. `trading_ai.api`
   Exposes local test-server endpoints for status, memory search, wake-up context, and taxonomy.
3. `trading_ai.integrations.ctrader_dexter_worker`
   Bridges into Dexter's worker as a subprocess. Dexter stays a separate repo and process boundary.
4. `trading_ai.core.skillbook` + `trading_ai.core.self_improvement`
   Hermes-inspired procedural memory loop: recall skills before decisions, distill new skills after closed trades.

## Memory design

The memory system keeps raw trade journals in Chroma and adds MemPalace-style metadata:

- `wing`: trading universe partition, currently `symbol:<symbol>`
- `hall`: session partition, currently `session:<session>`
- `room`: strategy/setup partition, usually `strategy_key` or `setup:trend:volatility`

This gives three memory access levels:

1. Weighted semantic recall from similar trade outcomes.
2. Wake-up context built from top winning and losing journals for the current symbol/session.
3. Taxonomy inspection for debugging what the engine has actually learned.

The monitor API now also exposes a trading intelligence layer:

- winner rooms
- danger rooms
- opportunity rooms
- anti-pattern rooms
- confidence calibration buckets
- lane scoreboard
- promotion pipeline hints
- room guardrails for the live loop
- operator note halls (`facts`, `discoveries`, `advice`, `preferences`)
- tunnel candidates across rooms
- a daily analyst brief for asynchronous MiMo review
- procedural skill documents and skill-context recall

See [TRADING_MEMORY_V2.md](TRADING_MEMORY_V2.md) for the target full-power MemPalace design.
See [HERMES_SELF_IMPROVEMENT.md](HERMES_SELF_IMPROVEMENT.md) for the new self-improving agent loop.

## Local test server

Recommended local test mode on this PC:

1. `scripts/bootstrap.ps1`
2. `scripts/start-api.ps1`
3. `scripts/start-loop.ps1`

`start-loop.ps1` defaults to dry-run. Keep live flags off until account auth and quote routing are verified again.
