---
name: openclaw
description: "Operational orchestrator for Dexter Pro via local wrapper/API and CLI fallback."
---

# OpenClaw Dexter Orchestration

## Rules

- This file is instructions, not a callable tool.
- Execute real commands via `exec`; never output pseudo function JSON.
- Do not use web search for local Dexter operations.
- Return `Action / Evidence / Next`.

## Wrapper (preferred)

Use:
`powershell -NoProfile -ExecutionPolicy Bypass -File "D:\\dexter_pro_v3_fixed\\dexter_pro_v3_fixed\\ops\\dexter_ops.ps1" <action> ...`

Actions:
- `status`, `neural`, `signals`, `performance`, `positions`
- `scan -Task <...>` (supports `scalping`)
- `scalping_status`, `scalping_toggle -Enabled 1|0`, `scalping_logic -Symbol BTCUSD`
- `pause`, `resume`, `close_all`

## Fallback

If wrapper/API fails:
- `python main.py health`
- `python main.py scan <task>`
- inspect `runtime/*.log`

## Safety

- Confirm before `close_all` unless explicit urgency.
- Never claim execution without evidence.
- Never leak tokens.
