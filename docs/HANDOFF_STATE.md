# Mempalace Trader Handoff State

Last updated: 2026-04-11 11:38 Asia/Bangkok

Read this file first when continuing the project in a new chat/session. It is intentionally written without API keys, tokens, passwords, account logins, private keys, or local export data.

## Restart prompt for a new chat

```text
Continue Mempalace Trader from D:\Mempalac_AI. Read docs/HANDOFF_STATE.md first, then read README.md, docs/ARCHITECTURE.md, docs/TRADING_MEMORY_V2.md, and trading_ai/docs/PARALLEL_WITH_DEXTER.md as needed. Check git status/log, local .env without printing secrets, runtime_state, logs, current processes, and dashboard status. Do not modify or stop Dexter. Preserve secret hygiene. Current design uses local Ollama qwen2.5:1.5b primary, qwen2.5:0.5b and gemma3:1b-it-qat fallback, DRY_RUN=false demo trading, Dexter worker execution, pre-LLM hard filters, MemPalace-style memory wings/halls/rooms, and optional daily MiMo analyst. Continue safely from current state.
```

## Current repository state

- Repo path: `D:\Mempalac_AI`
- Remote: `https://github.com/Geowahaha/Mempalace_trader.git`
- Branch: `main`
- Latest pushed commits:
  - `2196a60 Add read-only Dexter edge auditor`
  - `0ffe6d3 Add Mempalace handoff state`
  - `aa3750f Add safe Mempalac stop script`
  - `30aceed Initial Mempalace production trading engine`
- The local untracked file `start_stop ระบบ Mempalace ai.txt` is user-local and should not be staged unless explicitly requested.
- Git ignore rules intentionally exclude `.env`, `.env.*`, `.venv`, `data`, `logs`, `reports`, Chroma/db files, account exports, ssh keys, and local notes.

## Latest Dexter XAU audit and patch

- New local audit report: `D:\Mempalac_AI\reports\DEXTER_XAU_GIVEBACK_AUDIT_2026-04-10.md`
- Statement reviewed: `D:\dexter_pro_v3_fixed\cT_9900897_2026-04-10_23-29.xlsx`
- Finding: XAU giveback is real. From 91 XAU closed positions with usable tick coverage, 42 losers had first moved profitable by more than `0.5` XAU points, and 26 losers had first moved profitable by at least `2.0` XAU points.
- Finding: 2026-04-10 no-order symptom is mostly operational, not signal absence. Dexter journal shows `account_auth_failed: Invalid access token` for scheduled XAU entries and `family_position_cap:XAUUSD:xau_scalp_microtrend:1` for many scalp entries.
- Dexter patch applied in `D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed`:
  - Fibo short quarantine and golden-pocket/momentum quality gates were added earlier in this session.
  - Source-direction quarantine/protected lane governance was added earlier in this session.
  - XAU active-defense profit-seeking guard was added: if a XAU position is already profitable by configured R, active defense tightens/protects instead of closing, and TP trimming is suppressed while profit-seeking is active.
- Verification passed:
  - `python -m py_compile config.py execution\ctrader_executor.py scanners\fibo_advance.py`
  - `python -m pytest tests\test_trading_manager_agent.py tests\test_trading_team.py -q`
  - Result: `31 passed, 1 warning`
- Operational blocker remains: Dexter needs valid cTrader OpenAPI auth before broker sync/execution can work again. Patch is code-ready but running Dexter process must be restarted/redeployed deliberately after auth is fixed.

## Dexter VM deployment state

- Important correction: the real Dexter trading service runs on the Oracle VM, not on this PC.
- Local PC Dexter is now safety-only:
  - `CTRADER_DRY_RUN=1`
  - `MT5_DRY_RUN=1`
  - local `main.py monitor` process was stopped to avoid duplicate execution against the VM.
- VM app path: `/opt/dexter_pro`
- VM service: `dexter-monitor` via `systemd`
- VM branch now loaded: `deploy-xau-family-canary`
- VM commit now loaded: `778d846` (`Harden XAU fibo and profit governance`)
- VM auth note:
  - the older note that VM auth had already been fixed should now be treated as stale
  - later checks in this session showed the same cTrader auth family failing with invalid/revoked token behavior
  - do not assume the VM is broker-synced until it is rechecked explicitly with fresh tokens
- VM verification notes:
  - `py_compile` for patched files passed on VM.
  - VM venv does not currently have `pytest`, so full pytest was not run on VM.
  - Local PC pytest passed before deploy: `31 passed, 1 warning`.
- VM source-control note:
  - Branch was pushed to GitHub remote `dexter` as `deploy-xau-family-canary`.
  - The workflow auto-deploys only `main`; this branch deploy was manually checked out on VM for controlled rollout.
  - Consider merging to `main` only after observing VM behavior and deciding the patch is stable.

## Current runtime state

- As of `2026-04-11 11:38 Asia/Bangkok`, local Mempalace stack is running again on this PC.
- Current API/dashboard port is `8080` (not `8091`).
- Config loading was corrected on `2026-04-11`: [config.py](D:/Mempalac_AI/trading_ai/config.py) now loads the repo-root `.env` as the canonical settings source.
- Reason: local root `.env` and `trading_ai/.env` had diverged token pairs; the old setup could silently keep using stale worker credentials.
- Worker interpreter resolution was also hardened:
  - [ctrader_dexter_worker.py](D:/Mempalac_AI/trading_ai/integrations/ctrader_dexter_worker.py) now auto-discovers `trading_ai\.venv\Scripts\python.exe` when Dexter local has no `.venv`
- Current environment defaults loaded from local `.env`:
  - `dry_run=True`
  - `live_execution_enabled=False`
  - `ctrader_dexter_worker=True`
  - `quote_source=auto`
- Live runtime overrides applied by startup script:
  - `DRY_RUN=false`
  - `LIVE_EXECUTION_ENABLED=true`
  - `LLM_PROVIDER=local`
  - `CTRADER_QUOTE_SOURCE=dexter_reference`
- Current `runtime_state.json` path: `D:\Mempalac_AI\data\runtime_state.json`
- Current saved runtime state shows one prior live-demo position:
  - `open_position=BUY XAUUSD 0.01`
  - `trades_executed=0`
  - `consecutive_losses=0`
  - `halted=false`
- Current broker health on this PC:
  - resolved on `2026-04-11` after running a full production OAuth authorization-code exchange against `http://localhost:5000/callback`
  - fresh tokens were written into both:
    - `D:\Mempalac_AI\.env`
    - `D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\.env.local`
  - verified:
    - Mempalace `accounts` => `ok`
    - Mempalace `health` => `ok`, `environment=demo`, configured `account_login=9969200`
    - Dexter read-only worker probes also pass with the updated env
- Runtime verification after auth fix and restart:
  - `/status` => `ok`
  - `/broker/health` => `ok`, `account_id=46945293`, `environment=demo`
  - `/runtime/state` => `ok`
  - loop log restores the open XAUUSD demo position and continues to HOLD on `ASIA + RANGE + LOW volatility`
- Local code is ahead of runtime:
  - multi-position runtime state support is patched
  - same-side add-on entries are patched
  - opposite-side signal now closes all tracked same-symbol positions before opening the reverse leg
  - equity-based max-total-lot cap is patched
  - XAU and fibo scanners now fail open when capture is genuinely unavailable instead of translating missing capture into `low_tick_velocity:0.000`
  - XAU shadow backtest now falls back to `ctrader_spot_ticks` when candle DB exists but the requested time window is empty
  - verification passed on code only: `python -m py_compile ...`
  - logic test passed in repo venv: two BUY positions can coexist, and a later SELL closes both BUYs before opening one SELL
  - targeted Dexter verification passed locally:
    - `python -m pytest tests\test_scanner_capture_passthrough.py -q`
    - `python -m pytest tests\test_live_profile_autopilot.py -q -k "latest_capture_feature_snapshot_filters_rows_by_symbol_within_run or run_xau_shadow_backtest_falls_back_to_spot_ticks_when_candle_window_is_empty"`
  - note: full `tests\test_live_profile_autopilot.py` still has unrelated existing failures not caused by this patch
- These local code changes are active again in the local Mempalace trading loop.
- Current observed loop behavior after restart:
  - broker auth/execution path works
  - API dashboard works on `http://127.0.0.1:8080/dashboard`
  - loop is currently returning `HOLD` because the pre-LLM hard filter sees `ASIA + RANGE + LOW volatility`, not because of auth failure
- cTrader reconcile volume note:
  - broker reconcile returns raw `volume=100` for the currently open Mempalace XAUUSD 0.01-lot demo position
  - Mempalace startup reconcile now converts this back into lot-sized runtime units before restoring positions
  - targeted test added:
    - `D:\Mempalac_AI\tests\test_ctrader_reconcile.py`
    - verification passed with stdlib unittest because repo `.venv` currently does not include `pytest`
      - `D:\Mempalac_AI\.venv\Scripts\python.exe -m unittest D:\Mempalac_AI\tests\test_ctrader_reconcile.py -v`
      - `D:\Mempalac_AI\.venv\Scripts\python.exe -m py_compile D:\Mempalac_AI\trading_ai\main.py D:\Mempalac_AI\trading_ai\config.py D:\Mempalac_AI\tests\test_ctrader_reconcile.py`
- Runtime-state correction note:
  - root runtime state at `D:\Mempalac_AI\data\runtime_state.json` had temporarily drifted to `volume=1.0` during the first broker-reconcile patch
  - it was corrected from the legacy snapshot at `D:\Mempalac_AI\data\mempalac\runtime_state.json`
  - a backup of the wrong-scale file exists at `D:\Mempalac_AI\data\runtime_state.json.wrongscale.bak`

Process IDs change after restart. Do not rely on old PIDs except for immediate same-session debugging.

## Start, stop, and monitor commands

Start the full local demo-live stack:

```powershell
cd D:\Mempalac_AI
.\scripts\start-demo-live-stack.ps1 -Interval 30
```

Stop Mempalace API and trading loop only. This does not stop Dexter and does not stop Ollama:

```powershell
cd D:\Mempalac_AI
.\scripts\stop-demo-live-stack.ps1
```

Stop only the trading loop and keep the API/dashboard if needed:

```powershell
cd D:\Mempalac_AI
.\scripts\stop-demo-live-stack.ps1 -KeepApi
```

Check dashboard/API status:

```powershell
cd D:\Mempalac_AI
.\scripts\check-status.ps1
```

Tail logs:

```powershell
cd D:\Mempalac_AI
.\scripts\tail-logs.ps1
```

Direct status check:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/status" -TimeoutSec 10
```

Direct Ollama health check:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -TimeoutSec 5
```

## Reboot behavior

If the PC is shut down or restarted, the current Mempalace PowerShell/Python processes stop. They will not automatically resume unless a Windows Scheduled Task or service wrapper is created later.

After reboot:

1. Make sure Ollama background server is running.
2. Confirm `Invoke-RestMethod http://127.0.0.1:11434/api/tags` returns local models.
3. Run `.\scripts\start-demo-live-stack.ps1 -Interval 30`.
4. Check dashboard and logs.

The Ollama desktop app window can be closed. Mempalace needs `ollama.exe serve`, not the UI window.

## LLM design

- Primary local model: `qwen2.5:1.5b`
- Fallback local models: `qwen2.5:0.5b`, `gemma3:1b-it-qat`
- Local endpoint: `http://127.0.0.1:11434/v1`
- Reasoning from tests:
  - `qwen2.5:1.5b` passed controlled BUY/SELL/HOLD probe after prompt cleanup.
  - `qwen2.5:0.5b` was faster but failed 2/3 controlled cases, so it is fallback only.
  - Gemma local was slower on this PC and sometimes produced schema/action issues, so it is last fallback.

Cost note:

- Local Ollama inference costs no OpenAI/MiMo/API credits.
- cTrader API calls do not spend LLM credits.
- MiMo Pro should remain optional and limited to the daily analyst job unless explicitly enabled for live decisions.
- Exact Codex/chat credit usage is not visible from this repo. Creating/reading this handoff file is small, but the platform/account billing page is the only source of exact credit cost.

## Trading/execution design

- Current runtime is demo-live, not dry run:
  - `DRY_RUN=false`
  - `LIVE_EXECUTION_ENABLED=true`
- Execution path is via the Dexter worker integration from Mempalace, but Dexter repo itself must not be modified by Mempalace work.
- Runtime config now reads the repo-root `.env`; keeping secrets only in `trading_ai/.env` is no longer sufficient.
- Market quote source currently uses the Dexter-reference path instead of the failing native cTrader capture route.
- Native cTrader route still has unresolved support issue:
  - observed `app_auth_failed`
  - observed `CANT_ROUTE_REQUEST`
  - support/debug bundle should be sent to cTrader/OpenAPI support separately if needed.
- Current local blocker is different from the old route issue:
  - the worker now reaches application auth, but account/accounts auth fails because all available access/refresh token pairs appear invalid or revoked
  - the next required operator action is to mint a fresh cTrader OpenAPI OAuth token pair for the same app/client credentials, then retest `accounts` and `health`
- Do not print or commit `.env` contents, OpenAPI tokens, refresh tokens, passwords, or account identifiers.

## Tick-capture and shadow-resolution patch state

- Root cause of false `low_tick_velocity:0.000` rejects:
  - `latest_capture_feature_snapshot()` returns `status=capture_missing` when there is no recent run inside the lookback window
  - some scanner paths treated missing features as zeros, which could falsely block entries
- Patch applied in local Dexter repo:
  - [xauusd.py](D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/scanners/xauusd.py)
  - [fibo_advance.py](D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/scanners/fibo_advance.py)
  - scanners now return `micro_capture_unavailable:*` passthrough instead of blocking on zeroed capture metrics when capture is missing
- Root cause of shadow resolver backlog:
  - `run_xau_shadow_backtest()` only used `ctrader_spot_ticks` when the whole candle DB file was missing
  - if the candle DB existed but the requested window had no rows, the resolver skipped the shadow row instead of falling back
- Patch applied in local Dexter repo:
  - [live_profile_autopilot.py](D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/learning/live_profile_autopilot.py)
  - `_fetch_bars()` now falls back per-query to `ctrader_spot_ticks` when candle rows are empty
- New targeted tests:
  - [test_scanner_capture_passthrough.py](D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/tests/test_scanner_capture_passthrough.py)
  - [test_live_profile_autopilot.py](D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/tests/test_live_profile_autopilot.py)

## NeuralBrain note

- `rng undefined` is not currently reproduced in the checked local code path.
- Current observations:
  - `learning/neural_brain.py` and `learning/symbol_neural_brain.py` define RNG objects before use
  - local mission reports show successful training runs
- No NeuralBrain patch was applied in this round because there is not yet a reproducible failing path.

## Risk and decision rules

Current safety posture is intentionally conservative:

- `pre_llm_hard_filter` runs before the LLM for obvious no-trade regimes.
- Guaranteed HOLD before LLM when:
  - volatility is `LOW`
  - trend is `RANGE`
  - structure is consolidation
  - risk loss streak block is active
- After LLM, hard filter still blocks:
  - BUY against DOWN trend
  - SELL against UP trend
  - LOW volatility
  - RANGE trend
  - consolidation
  - configured loss streak block
- The loop does not force trades just because it is demo. The goal is to avoid poisoning memory with low-quality entries.

## MemPalace memory design

The project uses MemPalace ideas as the trading memory architecture:

- Raw verbatim journals should be preserved, not compressed into summaries too early.
- Memory is structured with wings, halls, and rooms.
- Memory is used as wake-up context before decisions, not only as an archive.
- First-class memory areas:
  - `symbol:*`
  - `execution`
  - `research`
  - `risk`
- Important hall types:
  - `hall_events`
  - `hall_discoveries`
  - `hall_advice`
  - `hall_facts`
  - `hall_preferences`
- Important intelligence outputs:
  - winner rooms
  - danger rooms
  - confidence calibration
  - lane scoreboard
  - shadow to live promotion hints

## Daily MiMo analyst design

MiMo Pro is intended as a once-per-day analyst, not the hot trading-loop policy head.

Expected role:

- Read the daily brief.
- Summarize winner rooms and danger rooms.
- Detect confidence drift.
- Suggest promote/demote actions.
- Store advice into memory.
- Do not directly mutate live policy without review.

Relevant scripts:

```powershell
.\scripts\get-daily-analyst-packet.ps1
.\scripts\run-daily-analyst.ps1
```

## Dexter edge audit state

Mempalace now includes a read-only Dexter auditor. It is designed to help Dexter without interfering with it:

- Code: `D:\Mempalac_AI\trading_ai\dexter_edge_audit.py`
- Runner: `D:\Mempalac_AI\scripts\run-dexter-edge-audit.ps1`
- Console entry point: `mempalace-dexter-edge-audit`
- Local report path: `D:\Mempalac_AI\reports\DEXTER_EDGE_AUDIT.md`
- `reports/` is ignored and should not be pushed because it contains private trading performance and edge evidence.

Run the current audit again:

```powershell
cd D:\Mempalac_AI
.\scripts\run-dexter-edge-audit.ps1 -TradeExport "D:\path\to\ctrader_trade_export.xlsx" -MaxEnvKeys 360
```

The auditor reads:

- Dexter cTrader SQLite DB in read-only mode
- Dexter backtest SQLite DB in read-only mode
- Optional cTrader XLSX trade export
- Dexter `.env.local` in sanitized mode only

Sanitization rules:

- Do not print tokens, passwords, client secrets, account identifiers, private keys, Telegram IDs, webhook URLs, or raw API payloads.
- Only trading behavior keys are shown, such as risk, lot/volume, fibo, XAU, BTC, canary, gates, thresholds, sessions, and live/demo flags.
- Do not copy full `.env.local` into chat or commits.

Latest audit generated locally at `D:\Mempalac_AI\reports\DEXTER_EDGE_AUDIT.md`.

Important findings from the latest audit:

- Broker-side export had 277 closed trades, total PnL about `+112.16 USD`, win rate about `38.6%`, and max drawdown about `-155.20 USD`.
- `XAUUSD buy` in the export was weak: 181 trades, about `-53.83 USD`, win rate about `34.3%`.
- `XAUUSD sell` in the export was strong: 17 trades, about `+89.96 USD`, win rate about `58.8%`.
- `BTCUSD buy` in the export was strong: 27 trades, about `+72.29 USD`, win rate about `66.7%`.
- Bad export days: `2026-04-07` about `-132.16 USD`, `2026-04-09` about `-55.82 USD`.
- Good export day: `2026-04-08` about `+110.78 USD`.
- Dexter DB shows the strongest immediate red flag is `fibo_xauusd` short: 22 trades, about `-111.25 USD`, win rate `0.0%`.
- Dexter DB shows `fibo_xauusd` long was positive, so do not kill the whole fibo family blindly. Quarantine/review the short side first.
- BTC quantity `0.05` appears explained by Dexter `.env.local` value `CTRADER_DEFAULT_VOLUME_SYMBOL_OVERRIDES=BTCUSD=5`; the worker uses `fixed_volume` before risk-based sizing.
- This means BTC volume increase was not proven to be AI auto-learning. It was likely a fixed symbol override that happened to perform well in that sample.

Safe Dexter improvement plan:

- Keep Dexter as executor and proven strategy host.
- Use Mempalace as read-only memory/evidence/governance first.
- Protect winner lanes before removing bad lanes.
- Candidate protect lanes include `xauusd_scheduled:canary` short/long, `scalp_xauusd:fss:canary` long, and `scalp_btcusd:canary` short.
- Candidate quarantine lanes include `fibo_xauusd` short, weak XAU canary base lanes, and selected `pb/bs/td` lanes only after human approval.
- Do not patch Dexter live logic until the user explicitly approves a branch/rollback plan.

## Current behavior observed

Recent live-demo loop behavior:

- The system is running and repeatedly evaluating XAUUSD.
- It has not opened a current position.
- Recent decisions are HOLD.
- Many HOLD decisions are correct because of `RANGE`, `LOW`, or consolidation hard filters.
- Some directional `UP_MEDIUM` or `DOWN_MEDIUM` regimes reached the LLM, but qwen still chose HOLD because pattern/memory evidence was thin.
- Current memory has notes but no closed trade rows in the current Chroma store, so PatternBook has not yet learned real win/loss statistics.

## Unresolved issues / next engineering tasks

1. Local qwen sometimes writes reasons like `risk_state can_trade is false` even when runtime risk is not halted. Add stricter decision validation or include compact risk JSON in a harder-to-misread format.
2. The model often chooses HOLD in clear UP/DOWN MEDIUM regimes because memory/pattern sample size is thin. Decide whether bootstrap exploration should allow very small demo orders after structure and risk gates pass.
3. Native cTrader quote/capture route still needs cTrader support response for `app_auth_failed / CANT_ROUTE_REQUEST`.
4. Build a Windows Scheduled Task or service wrapper if this PC should auto-resume Mempalace after reboot.
5. For VM deployment, keep Mempalace and Dexter as separate repos/processes/config paths. Do not share `data`, logs, runtime state, or strategy memory.
6. Add a dashboard panel showing:
   - current pre-LLM veto reason
   - whether LLM was called this cycle
   - model used
   - memory room guard outcome
   - current open position state
7. Add explicit broker position reconciliation so startup can detect externally-opened demo positions, not only positions opened and persisted by Mempalace.
8. Consider a shadow-trade mode for candidate lanes: record would-have-traded signals without broker execution until enough memory exists.
9. Add a Mempalace daily Dexter audit summary that stores `winner rooms`, `danger rooms`, and BTC/XAU sizing observations into MemPalace memory without changing Dexter.
10. If approved later, implement a Dexter-side hard gate for `fibo_xauusd` short only, preserving `fibo_xauusd` long and all currently profitable lanes.

## Safety rules for future agents

- Do not modify, reset, stop, or deploy Dexter unless explicitly requested.
- Do not read secrets aloud or paste `.env` contents into chat.
- Do not commit `.env`, token files, logs, Chroma DBs, account export spreadsheets, private keys, or local runtime data.
- Before commit, run:

```powershell
git status --short --ignored
git diff --cached --check
```

- Before push, scan staged files for known secrets and account identifiers.
- If changing execution logic, verify with demo first and check `runtime_state.json` for open positions.
