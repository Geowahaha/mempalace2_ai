# ============================================================
#  DEXTER PRO — Claude Code Prompt Library
#  SAVE TO: D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\docs\dexter_claude_prompts.py
#  Copy-paste prompts directly into Claude Code terminal.
#  Use /clear between unrelated tasks to save tokens.
# ============================================================


# ────────────────────────────────────────────────────────────
# STEP 0 — SEED MEMORY.md (run once per new machine / fresh install)
# ────────────────────────────────────────────────────────────

SEED_MEMORY = """
/memory
Please save the following to project memory:

## Last session: 2026-03-20 FSS fix

### What was fixed
scalp_xauusd:fss:canary was silently skipped on run 20260320021436-000003
confidence=82.0, delta_proxy=0.1391, bar_volume_proxy=1.0 (flow was strong)

### Root cause 1 — scheduler.py
FSS allowed_patterns=SCALP_FLOW_FORCE too narrow.
Signal was "Behavioral Sweep-Retest + Liquidity Continuation"
FIX: Pattern bridge added for that signal on continuation desk only.

### Root cause 2 — config.py + scheduler.py
Signal band=80+ but FSS asian memory had only lower bands.
FIX: high_confidence_bridge — 80+ borrows 70-79.9 context.
New key: XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE
Flag written to raw_scores for audit.

### Live state
107 tests passing. Monitor PID 20028 since 09:35:57 ICT 2026-03-20.
Watching for: scalp_xauusd:fss:canary as sell_stop on next similar setup.

### Key files right now
scheduler.py, config.py, tests/test_scheduler_watchlist.py,
data/ctrader_openapi.db (execution_journal for fss:canary entries)

### Do NOT touch without regression test
FSS pattern bridge + high_confidence_bridge logic
"""


# ────────────────────────────────────────────────────────────
# SESSION START — run every time you open Claude Code
# ────────────────────────────────────────────────────────────

SESSION_START = """
/add scheduler.py
/add config.py
/add data/runtime/trading_manager_state.json

Continue from FSS fix session 2026-03-20.
1. Has scalp_xauusd:fss:canary fired as sell_stop since PID 20028 started?
   Check execution_journal WHERE source LIKE '%fss%' ORDER BY id DESC LIMIT 5
2. What is current xau_execution_directive?
3. Any errors in runtime/monitor.stderr.log in last 30 lines?
"""


# ────────────────────────────────────────────────────────────
# FSS MONITORING — check if fix is working
# ────────────────────────────────────────────────────────────

FSS_CHECK = """
/add data/ctrader_openapi.db
/add scheduler.py

Run this query against execution_journal:
SELECT id, created_utc, source, status, direction, entry_type, signal_run_id
FROM execution_journal
WHERE source LIKE '%fss%'
ORDER BY id DESC LIMIT 10

If no fss entries found since 2026-03-20 09:35:57:
1. Show me the FSS pattern matcher function in scheduler.py
   (search _xau_flow_short_signal_pattern_matches)
2. Is there a log line that shows FSS being evaluated vs skipped?
3. What session/setup would be the next eligible trigger?
"""

FSS_DEEP_AUDIT = """
/add scheduler.py
/add config.py
/add data/reports/chart_state_memory_report.json

Audit the full FSS signal path end-to-end:
1. _xau_flow_short_signal_pattern_matches — what patterns now accepted?
2. first_sample_mode — exactly when does high_confidence_bridge activate?
3. From chart_state_memory_report, how many asian SHORT states have band 80+?
   How many have band 70-79.9 that the bridge would use?
4. Is there any path where FSS passes pattern + bridge but still gets blocked
   by direction guard or family variant limit?

Show code references for each answer.
"""


# ────────────────────────────────────────────────────────────
# SENIOR DEV REVIEW — use one module at a time
# ────────────────────────────────────────────────────────────

REVIEW_EXECUTION = """
/add execution/
/add api/

Review execution/ and api/ as a staff engineer on a production trading system.
Focus ONLY on:
1. Race conditions — can two orders fire simultaneously for same symbol?
2. Broker API timeouts — what happens if cTrader call hangs >5s?
3. Position state sync — can local state drift from broker reality?
4. Missing awaits — any sync calls inside async functions?
5. Exception swallowing — bare except/pass hiding broker errors?

For each issue:
- File + line number
- P&L impact in live trading
- Exact fix with code

Rewrite the riskiest function with full defensive coding.
Do NOT comment on style — only live-trading safety.
"""

REVIEW_POSITION_MANAGER = """
/add runtime/
/add execution/
/add api/

Review position manager and state tracking as a quant systems engineer.
1. Single source of truth for position.size — local or broker? Where can they diverge?
2. After partial close — is remaining size correctly updated everywhere?
3. On system restart — how is live position state recovered from broker?
4. Can a position be orphaned (open at broker, unknown to system)?
5. Is canary position state completely isolated from main?

Show code paths for each. Flag any path where state can be wrong silently.
"""

REVIEW_RISK_GUARDS = """
/add execution/
/add config.py

Map every code path that leads to order placement.
For each path confirm:
- Does it call check_risk() / risk validation?
- Does it respect CTRADER_MAX_POSITIONS_PER_SYMBOL?
- Does it respect CTRADER_BLOCK_OPPOSITE_DIRECTION?
- Does it respect per-family limits?

Draw the full decision tree as ASCII.
Highlight any path where order CAN fire without passing all guards.
"""

REVIEW_SCANNER = """
/add scanners/
/add agent/

Review signal generation as a trading systems engineer.
1. Can a scanner produce duplicate signals within SCALPING_DUPLICATE_SUPPRESS_SEC?
2. What happens when Gemini returns malformed JSON — crash or graceful fallback?
3. Can confidence scores be pushed beyond intended caps by winner_logic bonuses?
4. Can XAU_FORCE mode bypass MIN_SIGNAL_CONFIDENCE? Show the exact condition.

File + function names for each answer.
"""

REVIEW_FSS_SCHEDULER = """
/add scheduler.py
/add config.py

Review all FSS-related code in scheduler.py as a trading systems engineer.
Search for: _xau_flow_short, flow_short_sidecar, high_confidence_bridge

1. Is the pattern bridge for "Behavioral Sweep-Retest + Liquidity Continuation"
   narrow enough? Could it fire on a weak continuation setup?
2. Is high_confidence_bridge properly guarded — any path where it fires
   on a setup that genuinely shouldn't get FSS?
3. Is there a variant limit path that could silently drop FSS even when
   pattern + bridge both pass?
4. Is the raw_scores.high_confidence_bridge flag being written consistently?

Show the 2026-03-20 patch diff and assess if it's tight enough for live use.
"""


# ────────────────────────────────────────────────────────────
# DEBUG SURGEON — paste error + code into template below
# ────────────────────────────────────────────────────────────

DEBUG_TEMPLATE = """
Bug: [DESCRIBE WHAT WENT WRONG]
Observed: [WHAT SYSTEM DID]
Expected: [WHAT IT SHOULD HAVE DONE]
When: [MARKET CONDITIONS / SESSION / TIME ICT]

Error log:
[PASTE LOG]

Relevant code:
[PASTE FUNCTION]

Tell me:
1. Root cause — WHY is the logic wrong (not just what failed)
2. Under what exact market condition does this trigger?
3. How to write a unit test reproducing this WITHOUT live market
4. Permanent fix — not a patch, fix the invariant
5. What other code in the same module has the same bug class?
"""

DEBUG_FSS_NOT_FIRING = """
Bug: scalp_xauusd:fss:canary not firing on [DESCRIBE SETUP]
Session: [asian/london/new_york]
Signal confidence: [X]
Pattern: [PATTERN NAME]
Run ID: [signal_run_id]

/add scheduler.py
/add config.py
/add data/ctrader_openapi.db

Check in order:
1. Does signal pattern match FSS allowed_patterns including the 2026-03-20 bridge?
   Run _xau_flow_short_signal_pattern_matches against this signal mentally.
2. Does confidence band qualify for first_sample_mode or high_confidence_bridge?
3. Is FSS blocked by direction guard or family variant limit?
   Check CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL and CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION
4. Is there a log line showing FSS was evaluated but rejected vs never evaluated?

Show the exact gate that blocked FSS and the fix.
"""

DEBUG_POSITION_DRIFT = """
Bug: Position shows [direction] in system but broker shows different state.
After: [rapid open/close / restart / network drop]

/add runtime/
/add api/
/add store/

Log:
[LOG]

Broker response:
[JSON]

1. Full state machine: open → close for this position type
2. Every place where local state is written vs broker queried
3. Where they can desync without detection
4. Add defensive assertions to catch this BEFORE next order fires
"""

DEBUG_ORDER_SILENT_FAIL = """
Bug: Order placed (got order_id) but never filled. No error logged.
Symbol: [XAUUSD/BTC/ETH]
Type: [market/limit/stop]
Time: [HH:MM ICT]

/add execution/
/add api/
/add runtime/

Log:
[LOG]

1. Was order rejected silently by broker?
2. Is there timeout/polling for pending orders? Is it running?
3. Can CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL cause silent no-op?
4. Add explicit order status polling with Telegram timeout alert.
"""


# ────────────────────────────────────────────────────────────
# IMPROVE — add new features safely
# ────────────────────────────────────────────────────────────

IMPROVE_MONITORING = """
/add runtime/
/add notifier/
/add store/

Add daily performance summary to Telegram at [TIME] ICT.
Include:
- Open positions: symbol, direction, entry, current R, age (minutes)
- Today closed: wins/losses, total PnL USD, best/worst trade
- Strategy family breakdown: which families fired and how
- Active guards: direction guards or regime guards currently blocking

Requirements:
- Must not block trading loop (async send)
- Works even if no trades today (graceful empty state)
- Mobile-readable on Telegram (no wide tables)
- Bangkok time (UTC+7) throughout

Show implementation plan first, then code.
"""

IMPROVE_FSS_REJECTION_LOG = """
/add scheduler.py
/add notifier/

When FSS is evaluated but rejected, log WHY in human-readable form.

Target output:
"FSS SKIP run_id=X: pattern=Behavioral Sweep-Retest not in allowed_patterns.
 confidence=74, session=asian, h1=bearish.
 Would have been: sell_stop if pattern matched."

Requirements:
- Goes to logs/ always
- Goes to Telegram only if TELEGRAM_BROADCAST_SIGNALS=1
- Must NOT slow scan cycle — write async or after scan completes
- Include high_confidence_bridge eligibility in the skip reason

Show where in scheduler.py to hook this without touching execution logic.
"""

IMPROVE_ENV_VALIDATION = """
/add config.py
/add .env.local

Add startup validation that checks:
1. All required keys present (no silent None that crashes mid-trade)
2. FSS thresholds internally consistent:
   FIRST_SAMPLE_MIN_CONFIDENCE < SAMPLE_MIN_CONFIDENCE < MIN_CONFIDENCE
3. Risk USD per family sums don't exceed account risk ceiling
4. Direction guard RECENT_SEC is not set to 0 accidentally

Raise on startup with clear error message — fail fast before live trading starts.
"""


# ────────────────────────────────────────────────────────────
# ENV CONFIG AUDIT
# ────────────────────────────────────────────────────────────

ENV_AUDIT = """
/add .env.local
/add config.py

Audit current config for:
1. FSS thresholds: FIRST_SAMPLE_MIN_CONFIDENCE=68, SAMPLE_MIN_CONFIDENCE=72
   Is the bridge gap (68 vs 80+ bridge) too wide? Could it fire on marginal 80+ setups?

2. Canary max risk: PERSISTENT_CANARY_CTRADER_RISK_USD=2.5
   EXPERIMENTAL_FAMILY_MAX_VARIANTS=3, EXPERIMENTAL_FAMILY_CTRADER_RISK_USD=0.75
   Max simultaneous USD at risk from canary + experimental combined?

3. XAU Force mode: SCALPING_XAU_FORCE_MIN_CONFIDENCE=56
   Below MIN_SIGNAL_CONFIDENCE=70. What guards prevent over-trading?

4. Direction guard: CTRADER_DIRECTION_GUARD_RECENT_SEC=900
   Is 15 minutes long enough after a loss before allowing same direction?

Show the math and flag any setting that looks misconfigured for live trading.
"""


# ────────────────────────────────────────────────────────────
# QUICK ONE-LINERS
# ────────────────────────────────────────────────────────────

QUICK_ASYNC_CHECK = """
/add [FILE]
Scan for: missing await, sync calls inside async, tight loops without
asyncio.sleep(0), exception handlers swallowing broker errors silently.
Show line numbers only. No explanations unless asked.
"""

QUICK_WRITE_TEST = """
/add [FILE]
Write pytest unit test for [FUNCTION].
Test: happy path, broker timeout, malformed broker response.
Mock the broker API — no live connection required.
"""

QUICK_EXPLAIN_LOG = """
I see this in runtime/monitor.stderr.log:
[PASTE LINE]

What does this mean in context of a live trading agent?
Is this an error to fix or expected behavior?
If error: which file/function is the source?
"""

QUICK_FSS_JOURNAL_CHECK = """
/add data/ctrader_openapi.db
Show last 10 entries from execution_journal where source LIKE '%fss%'.
Format: id | created_utc | source | status | direction | entry_type | entry
If none: when was the last scan cycle that evaluated FSS? Check runtime logs.
"""
