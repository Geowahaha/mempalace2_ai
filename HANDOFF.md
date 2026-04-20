# HANDOFF.md — Deep Audit Results (2026-04-21)

## What This Is

An autonomous XAUUSD (Gold) trading bot called **mempalace2_ai**. Uses LLM (GPT-4o-mini / MiMo / Ollama) as core decision engine, ChromaDB vector memory for trade recall, pattern recognition, strategy evolution, self-improvement, shadow probes, and position management. Runs in a continuous ~30s loop.

## What Was Done

Full codebase audit — read all 261 Python files, focused on strategy logic, risk management, position management, memory engine, execution, and the main learning loop. Identified **19 issues** across bugs, dangerous behaviors, and architectural problems.

## Top 5 — Fix Immediately

### 1. Paper Broker Spread Is 5x Too Tight
**File:** `trading_ai/core/execution.py` — `PaperBroker`
**Problem:** Spread = `$0.05`. Real XAUUSD spread = `$0.20-0.50`. Every pattern, win rate, and strategy learned in paper mode is based on fantasy conditions. The entire memory database is contaminated.
**Fix:** Make spread configurable via Settings. Default `paper_spread_usd = 0.25`.

### 2. Shadow Probes Leak Into Live Pattern Book
**File:** `trading_ai/main.py` — `process_shadow_close()`
**Problem:** Shadow probes (dry-run fake trades) call `pattern_book.append_closed_trade()` — the same book used for live entry gates. Lucky shadow wins inflate `pattern_win_rate`, which unlocks real money trades based on fake data.
**Fix:** Maintain separate `PatternBook` instances: `pattern_book_live` (real trades only) and `pattern_book_combined` (for analytics). Live entry gates use `live` only.

### 3. First Net Loss Kills Strategy Forever
**File:** `trading_ai/core/strategy_evolution.py` — `_refresh_score_and_active()`
**Problem:** `disable = (st.trades >= 10 and wr < 0.45) or (st.total_profit < 0)`. A strategy with 9 wins and 1 loss where the loss exceeds cumulative wins → permanently disabled. One bad day kills a strategy for life.
**Fix:** Require minimum sample before total_profit matters: `(st.trades >= 15 and st.total_profit < 0)`.

### 4. Trailing Stop Doesn't Work When Position Is In Loss
**File:** `trading_ai/core/position_manager.py` — `evaluate_open_position()`
**Problem:** Trailing stop only fires when `unrealized_return_pct > 0`. If position dips from +0.3% to -0.05%, the trail is ignored. Profit protection disappears exactly when you need it.
**Fix:** Remove `unrealized_return_pct > 0` guard. Trail level already encodes whether you're protecting profit.

### 5. Loss-Streak Safety Override Fires on 3 Trades at 54% WR
**File:** `trading_ai/main.py` — `_loss_streak_override_payload()`
**Problem:** 3 shadow probe trades at 54% win rate can bypass the consecutive-loss entry block. 3 trades is noise. You're disabling circuit breakers based on statistical static.
**Fix:** Raise defaults: `loss_streak_override_min_shadow_trades = 8`, `loss_streak_override_min_shadow_win_rate = 0.60`.

---

## All 19 Issues Found

### Bugs (Code Errors)
1. **PaperBroker spread** — $0.05 vs real $0.25 — see #1 above
2. **Shadow probe data leak** — fake trades enter live pattern book — see #2 above
3. **Strategy killed by single net loss** — `total_profit < 0` disables regardless of sample — see #3
4. **Trailing stop broken in loss** — only fires when profitable — see #4
5. **`_entry_skill_stats` corrupt win rate** — when `strategy_state` has wins but zero trades, produces 500% win rate. File: `position_manager.py`
6. **Broker volume reconciliation fragile** — hardcoded divisor `scale * 100` assumes specific cTrader unit convention. File: `main.py` — `_reconcile_open_positions_from_broker()`
7. **`_confidence_bucket` boundary overlap** — value 0.2 belongs to both "0.0-0.2" and "0.2-0.4" logically. File: `memory.py`
8. **Shadow probe pattern contamination** — same as #2 but also affects `MemoryEngine` store. File: `main.py` — `process_shadow_close()`

### Stupid Strategies (Bad Logic)
9. **Adaptive confidence floor is backwards** — lowers floor in strong trends (easier to chase), raises in quiet markets (misses value). File: `agent.py` — `_confidence_floor_for_features()`
10. **Neutral PnL threshold = 0.0001** — 0.01% return counts as "win". Round-trip spread cost determines most classifications. File: `config.py`
11. **Entry override is complexity theater** — 7 nested conditions with tight thresholds that almost never trigger. Hundreds of lines for ~0 additional trades. File: `position_manager.py` — `evaluate_entry_hold_override()`
12. **Self-improvement uses temperature 0.0** — identical inputs always produce identical skill text, can't learn nuances. File: `self_improvement.py` — `_llm_review()`
13. **Portfolio intelligence disabled dead code** — entire fusion system built but defaults to off. Untested in production. File: `config.py`
14. **LLM is sole alpha source** — no deterministic signal layer. The LLM reads feature labels and guesses. File: `agent.py` — `TradingAgent.decide()`
15. **Weekly lane decisions on 3 trades** — `weekly_lane_min_trades=3` is statistically meaningless. File: `config.py`
16. **Loss-streak override on noise** — see #5 above
17. **`PerformanceTracker.win_rate` excludes neutrals** — inflates win rate. File: `performance.py`

### Architectural Issues
18. **`_seed_price_history_from_monitor` assumes line order** — doesn't sort by timestamp, can produce scrambled history. File: `main.py`
19. **`_normalized_rows` full ChromaDB scan** — fetches all records every time, doesn't scale. File: `memory.py`

---

## What's Actually Good (Growth Potential)

1. **Memory palace architecture** — ChromaDB + semantic search + metadata filtering is the right approach. After 200+ clean trades, this will surface real patterns.
2. **Self-improvement loop** — observe → critique → distill → reuse is the correct mental model. Tracks overconfident losses vs underconfident wins.
3. **Multi-layer safety gates** — no single broken layer can blow up the account. Requires consensus.
4. **Shadow probes concept** — brilliant idea to measure "am I being too conservative?" Just needs data isolation.
5. **Strategy evolution pipeline** — candidate → shadow → live → retired with aging decay and capital weighting. This is how professional quant firms manage strategies.

## Recommended Fix Order

```
Week 1:  #1 (spread), #2 (shadow leak), #3 (strategy kill) — stop bleeding
Week 2:  #4 (trailing stop), #5 (override threshold), #6 (volume) — stop corruption
Week 3:  #9 (confidence floor), #10 (neutral threshold), #14 (signal layer) — fix strategy
Week 4:  #11-#13, #15-#17 — clean up complexity
Week 5:  #18-#19 — hygiene and debuggability
```

## Key Files

| File | Role |
|------|------|
| `trading_ai/main.py` | Main learning loop (~1800 lines) |
| `trading_ai/core/agent.py` | LLM trading agent + decision logic |
| `trading_ai/core/position_manager.py` | Entry assessment + open position management |
| `trading_ai/core/execution.py` | Broker abstraction + PaperBroker |
| `trading_ai/core/memory.py` | ChromaDB vector memory engine |
| `trading_ai/core/strategy_evolution.py` | Strategy registry + lane evolution |
| `trading_ai/core/strategy.py` | RiskManager (consecutive losses, max trades) |
| `trading_ai/core/patterns.py` | Pattern bucketing + win rate gates |
| `trading_ai/core/self_improvement.py` | Hermes-style skill distillation |
| `trading_ai/core/portfolio_intelligence.py` | Multi-source vote fusion (disabled) |
| `trading_ai/core/skillbook.py` | Procedural memory + team brief |
| `trading_ai/config.py` | All settings (~500 lines) |
| `tools/risk_engine.py` | Kelly Criterion + ATR TP/SL (separate system) |

---

## Context

This audit was done by reading the full codebase (364 files, 261 Python files). No code changes were made. The repo was cloned from `https://github.com/Geowahaha/mempalace2_ai.git` for analysis.
