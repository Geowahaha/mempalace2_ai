# Running Mempalac parallel to Dexter (independent trading stacks)

**Goal:** Two separate systems on the same machine or VM without sharing trading state, repos, or configuration. This document is the contract for git and runtime separation so Dexter is not overwritten or accidentally merged with Mempalac.

## 1. Git: always two repositories

- **Dexter:** `dexter_pro_v3_fixed` (or worktrees under `.claude/worktrees/...`). Own branches, own commit history.
- **Mempalac:** this repo (`Mempalac_AI/trading_ai` or your clone path). Own branches, own history.

**Do not** copy Mempalac source into Dexter as a subtree merge, or Dexter into Mempalac, unless you deliberately want a single monorepo. The default safe pattern is **two remotes, two clones, no cross-repo merges**.

## 2. Trading isolation (required)

| Topic | Dexter | Mempalac |
|--------|--------|----------|
| cTrader target | `CTRADER_ACCOUNT_ID` / Open API identity in Dexter `.env.local` | Numeric **`CTRADER_ACCOUNT_ID`** = **ctidTraderAccountId** for the account Mempalac should trade. The visible cTrader login is not the same as the Open API account id. |
| Config file | Dexter `dexter_pro_v3_fixed/.env.local` | Mempalac `.env` in **this** repo root. Never point Mempalac tooling at Dexter `.env.local` for its own strategy memory paths. |
| Journal / SQLite | Dexter `data/ctrader_openapi.db` and `data/runtime/*` | Keep Mempalac **`DATA_DIR`** (and Chroma / memory paths) under Mempalac only. Do not open Dexter DB files read/write from Mempalac. |

If both systems trade the **same** cTrader account, orders and risk are **not** independent. Use **different** accounts for parallel stacks unless you intend shared capital.

## 3. Optional: execution via Dexter one-shot worker

`integrations/ctrader_dexter_worker.py` (`CTraderDexterWorkerBroker`) runs Dexter’s `ops/ctrader_execute_once.py` as a subprocess with a JSON payload that includes **`account_id`** from Mempalac’s **`CTRADER_ACCOUNT_ID`** (see `execute_trade`).

- **Working directory** for the worker is the Dexter repo root resolved from the worker script path.
- The worker process may load **Dexter’s** OAuth tokens from **Dexter’s** `.env.local`. That is expected for API access; the **order** still must target Mempalac’s `account_id` in the payload. After any Dexter upgrade, confirm `ctrader_execute_once.py` still honors payload `account_id` for the target account.

**Token refresh:** If both a long-running Dexter process and another process use the **same** Spotware OAuth refresh token and both refresh, you can see invalid-token races. Safer patterns: (a) only Dexter holds the token and Mempalac uses **only** the worker path for live orders, or (b) separate OAuth applications / refresh tokens per system.

Relevant Mempalac env keys (see `config.py`):
`CTRADER_DEXTER_WORKER`, `CTRADER_ACCOUNT_ID`, `CTRADER_WORKER_SCRIPT`, `CTRADER_WORKER_PYTHON`, `CTRADER_WORKER_TIMEOUT_SEC`, `CTRADER_WORKER_VOLUME_SCALE`.

### Optional: Mempalac as Dexter family-lane signal source

If you want **Dexter** to own trade execution while Mempalac provides direction/confidence as a lane:

- In Mempalac `.env`:
  - `DEXTER_FAMILY_EXPORT_ENABLED=1`
  - `DEXTER_FAMILY_EXPORT_PATH=D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/data/runtime/mempalace_family_signal.json`
  - `DEXTER_FAMILY_EXPORT_BASE_SOURCE=scalp_xauusd`
- In Dexter `.env.local`:
  - `MEMPALACE_FAMILY_ENABLED=1`
  - `MEMPALACE_FAMILY_SIGNAL_PATH=D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/data/runtime/mempalace_family_signal.json`
  - `PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED=1`

Notes:
- Existing Dexter families stay untouched; this lane is **off by default**.
- Mempalac writes BUY/SELL/HOLD payloads every loop; Dexter only takes fresh payloads (`MEMPALACE_FAMILY_SIGNAL_MAX_AGE_SEC`).
- HOLD payload means no mempalace family execution on Dexter.

## 4. Resource and port isolation

- **CPU / RAM:** Two loops plus Chroma can stress a small VM. Watch Dexter scan latency if Mempalac runs heavy backtests on the same host.
- **Ports:** If either stack exposes HTTP or metrics, use different ports. No requirement for Dexter to open ports for Mempalac unless you add integrations explicitly.

## 5. What does *not* change Dexter

- Committing only in the **Mempalac** repo.
- Setting Mempalac `.env` and `DATA_DIR` under Mempalac.
- Using the worker with a **distinct** `CTRADER_ACCOUNT_ID` from Dexter’s live account.

Avoid automation that edits Dexter `.env.local`, replaces Dexter `data/`, or rebases Dexter branches from Mempalac history.

## 6. Quick checklist before go-live

- [ ] Mempalac `CTRADER_ACCOUNT_ID` is the correct **numeric** Open API account id for the intended account.
- [ ] Mempalac `DATA_DIR` (and Chroma path) is not under Dexter `data/`.
- [ ] Dexter continues to use its existing `.env.local` and account for production Dexter trades.
- [ ] `DRY_RUN` / paper mode gates are understood before enabling real demo or live volume on Mempalac.
- [ ] If using OAuth in both stacks directly, refresh-token strategy is decided (single worker path vs separate apps).

For implementation details of the worker bridge, see `integrations/ctrader_dexter_worker.py`.
