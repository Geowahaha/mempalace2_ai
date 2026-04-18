# Full Source-Level Parity Audit

- Generated: 2026-02-20 18:04 UTC
- Local project: `D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed`
- Upstream dexter commit: `3ca1cea` (`main`)
- Upstream openclaw commit: `417509c53` (`main`)

## Method
- Cloned both repositories and inventoried all module namespaces from source roots.
- Audited parity at module level (`Implemented` / `Partial` / `Missing`) and scope fit (`In-scope` / `Out-of-scope`).
- Produced strict checklist for all modules in: `dexter/src`, `openclaw/src`, `openclaw/apps`, `openclaw/packages`, `openclaw/extensions`, `openclaw/skills`.

## Inventory Summary
- Dexter modules audited: **9**
- OpenClaw core src modules audited: **50**
- OpenClaw apps audited: **4**
- OpenClaw packages audited: **2**
- OpenClaw extensions audited: **35**
- OpenClaw skills audited: **52**
- **Total module namespaces audited: 152**

## Dexter Parity Checklist (All Modules)
| Module | Status | Scope | Gap Note |
|---|---|---|---|
| `agent` | Partial | In-scope | Python agent exists but lacks full task-graph + reflection loop parity |
| `components` | Missing | Out-of-scope | No React/Ink UI components in this Python CLI bot |
| `evals` | Missing | In-scope | No formal eval runner/benchmark dataset pipeline |
| `gateway` | Partial | In-scope | Scheduler + Telegram admin bot exist; no full gateway abstraction |
| `hooks` | Missing | Out-of-scope | No frontend hooks layer |
| `model` | Partial | In-scope | Provider selection/fallback implemented but simpler model orchestration |
| `skills` | Missing | In-scope | No structured skill registry/loader parity yet |
| `tools` | Partial | In-scope | Trading tools implemented; no browser/search/fundamentals parity |
| `utils` | Partial | In-scope | Config/logging helpers exist, less extensive utility coverage |

## OpenClaw Core Parity Checklist (All `src/*` Modules)
| Module | Status | Scope | Gap Note |
|---|---|---|---|
| `acp` | Missing | Out-of-scope | ACP protocol layer not implemented |
| `agents` | Partial | In-scope | Core research agent exists but not multi-agent/session-isolated runtime |
| `auto-reply` | Partial | In-scope | Admin NLP routing exists, but no full channel auto-reply policy stack |
| `browser` | Missing | Out-of-scope | No browser automation in current trading scope |
| `canvas-host` | Missing | Out-of-scope | Canvas/A2UI host not in trading scope |
| `channels` | Partial | In-scope | Telegram implemented; multi-channel abstraction missing |
| `cli` | Partial | In-scope | Python CLI exists, but command surface is much smaller |
| `commands` | Partial | In-scope | Scan/research commands exist, limited control-plane operations |
| `compat` | Missing | Out-of-scope | Legacy compatibility layer absent |
| `config` | Partial | In-scope | Env config strong; lacks full schema/validation matrix |
| `cron` | Partial | In-scope | Scheduler present with timed jobs + monitor windows |
| `daemon` | Missing | In-scope | No native daemon/service installer for 24/7 ops |
| `discord` | Missing | Out-of-scope | Discord channel absent |
| `docs` | Missing | Out-of-scope | No docs-generation module |
| `gateway` | Partial | In-scope | No dedicated WS gateway protocol/control plane |
| `hooks` | Missing | In-scope | No pluggable hook execution framework |
| `imessage` | Missing | Out-of-scope | iMessage channel absent |
| `infra` | Partial | In-scope | Basic orchestration/logging exists; no infra service layer parity |
| `line` | Missing | Out-of-scope | LINE channel absent |
| `link-understanding` | Missing | In-scope | No dedicated URL/link understanding pipeline |
| `logging` | Partial | In-scope | Structured logging used; advanced sinks/diagnostics missing |
| `macos` | Missing | Out-of-scope | macOS relay runtime absent |
| `markdown` | Partial | In-scope | Telegram markdown formatting exists, no full markdown pipeline |
| `media` | Missing | Out-of-scope | Media tooling not in current trading scope |
| `media-understanding` | Missing | Out-of-scope | Media understanding absent |
| `memory` | Missing | In-scope | No vector/long-term memory backend |
| `node-host` | Missing | Out-of-scope | Device-node host absent |
| `pairing` | Partial | In-scope | Admin ACL exists; code-based secure pairing flow missing |
| `plugin-sdk` | Missing | Out-of-scope | No plugin SDK |
| `plugins` | Missing | Out-of-scope | No plugin runtime |
| `process` | Partial | In-scope | Threaded jobs exist; no generic process bridge/queue subsystem |
| `providers` | Partial | In-scope | Groq/Gemini/Anthropic fallback implemented, limited provider features |
| `routing` | Partial | In-scope | Rule routing via scanner/scheduler; no generalized route engine |
| `scripts` | Missing | Out-of-scope | No equivalent script module |
| `security` | Partial | In-scope | Admin allowlist + safety checks exist; full security audit framework missing |
| `sessions` | Missing | In-scope | No explicit per-user/per-channel session model |
| `shared` | Missing | Out-of-scope | No shared monorepo runtime package |
| `signal` | Missing | Out-of-scope | Signal messenger channel absent |
| `slack` | Missing | Out-of-scope | Slack channel absent |
| `telegram` | Partial | In-scope | Strong Telegram notifier/admin commands, but no full gateway Telegram stack |
| `terminal` | Missing | In-scope | No separate terminal subsystem/TUI |
| `test-helpers` | Missing | Out-of-scope | No dedicated test-helper module |
| `test-utils` | Missing | Out-of-scope | No dedicated test-utils module |
| `tts` | Missing | Out-of-scope | No text-to-speech module |
| `tui` | Missing | Out-of-scope | No TUI app |
| `types` | Partial | In-scope | Python dataclasses present, but no full typed contract layer |
| `utils` | Partial | In-scope | Utility coverage narrower than OpenClaw |
| `web` | Missing | In-scope | No web dashboard/control UI |
| `whatsapp` | Missing | Out-of-scope | WhatsApp channel absent |
| `wizard` | Partial | In-scope | Setup wizard exists, less comprehensive onboarding |

## OpenClaw App Modules Checklist (All `apps/*`)
| Module | Status | Scope | Gap Note |
|---|---|---|---|
| `android` | Missing | Out-of-scope | Native app surface not present in this Python trading bot. |
| `ios` | Missing | Out-of-scope | Native app surface not present in this Python trading bot. |
| `macos` | Missing | Out-of-scope | Native app surface not present in this Python trading bot. |
| `shared` | Missing | Out-of-scope | Native app surface not present in this Python trading bot. |

## OpenClaw Package Modules Checklist (All `packages/*`)
| Module | Status | Scope | Gap Note |
|---|---|---|---|
| `clawdbot` | Missing | Out-of-scope | Package-specific runtime not applicable to current architecture. |
| `moltbot` | Missing | Out-of-scope | Package-specific runtime not applicable to current architecture. |

## OpenClaw Extensions Checklist (All `extensions/*`)
| Extension | Status | Scope | Gap Note |
|---|---|---|---|
| `bluebubbles` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `copilot-proxy` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `device-pair` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `diagnostics-otel` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `discord` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `feishu` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `google-antigravity-auth` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `google-gemini-cli-auth` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `googlechat` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `imessage` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `irc` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `line` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `llm-task` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `lobster` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `matrix` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `mattermost` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `memory-core` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `memory-lancedb` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `minimax-portal-auth` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `msteams` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `nextcloud-talk` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `nostr` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `open-prose` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `phone-control` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `qwen-portal-auth` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `signal` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `slack` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `talk-voice` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `telegram` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `tlon` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `twitch` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `voice-call` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `whatsapp` | Missing | In-scope | Channel/plugin extension architecture missing. |
| `zalo` | Missing | Out-of-scope | Not targeted for current trading bot scope. |
| `zalouser` | Missing | Out-of-scope | Not targeted for current trading bot scope. |

## OpenClaw Skills Checklist (All `skills/*`)
| Skill | Status | Scope | Gap Note |
|---|---|---|---|
| `1password` | Missing | Out-of-scope | Not required for current trading workflow. |
| `apple-notes` | Missing | Out-of-scope | Not required for current trading workflow. |
| `apple-reminders` | Missing | Out-of-scope | Not required for current trading workflow. |
| `bear-notes` | Missing | Out-of-scope | Not required for current trading workflow. |
| `blogwatcher` | Missing | Out-of-scope | Not required for current trading workflow. |
| `blucli` | Missing | Out-of-scope | Not required for current trading workflow. |
| `bluebubbles` | Missing | Out-of-scope | Not required for current trading workflow. |
| `camsnap` | Missing | Out-of-scope | Not required for current trading workflow. |
| `canvas` | Missing | Out-of-scope | Not required for current trading workflow. |
| `clawhub` | Missing | Out-of-scope | Not required for current trading workflow. |
| `coding-agent` | Missing | In-scope | Skill loading/execution framework missing. |
| `discord` | Missing | Out-of-scope | Not required for current trading workflow. |
| `eightctl` | Missing | Out-of-scope | Not required for current trading workflow. |
| `food-order` | Missing | Out-of-scope | Not required for current trading workflow. |
| `gemini` | Missing | In-scope | Skill loading/execution framework missing. |
| `gifgrep` | Missing | Out-of-scope | Not required for current trading workflow. |
| `github` | Missing | In-scope | Skill loading/execution framework missing. |
| `gog` | Missing | Out-of-scope | Not required for current trading workflow. |
| `goplaces` | Missing | Out-of-scope | Not required for current trading workflow. |
| `healthcheck` | Missing | Out-of-scope | Not required for current trading workflow. |
| `himalaya` | Missing | Out-of-scope | Not required for current trading workflow. |
| `imsg` | Missing | Out-of-scope | Not required for current trading workflow. |
| `local-places` | Missing | Out-of-scope | Not required for current trading workflow. |
| `mcporter` | Missing | Out-of-scope | Not required for current trading workflow. |
| `model-usage` | Missing | In-scope | Skill loading/execution framework missing. |
| `nano-banana-pro` | Missing | Out-of-scope | Not required for current trading workflow. |
| `nano-pdf` | Missing | Out-of-scope | Not required for current trading workflow. |
| `notion` | Missing | Out-of-scope | Not required for current trading workflow. |
| `obsidian` | Missing | Out-of-scope | Not required for current trading workflow. |
| `openai-image-gen` | Missing | Out-of-scope | Not required for current trading workflow. |
| `openai-whisper` | Missing | Out-of-scope | Not required for current trading workflow. |
| `openai-whisper-api` | Missing | Out-of-scope | Not required for current trading workflow. |
| `openhue` | Missing | Out-of-scope | Not required for current trading workflow. |
| `oracle` | Missing | Out-of-scope | Not required for current trading workflow. |
| `ordercli` | Missing | Out-of-scope | Not required for current trading workflow. |
| `peekaboo` | Missing | Out-of-scope | Not required for current trading workflow. |
| `sag` | Missing | Out-of-scope | Not required for current trading workflow. |
| `session-logs` | Missing | In-scope | Skill loading/execution framework missing. |
| `sherpa-onnx-tts` | Missing | Out-of-scope | Not required for current trading workflow. |
| `skill-creator` | Missing | In-scope | Skill loading/execution framework missing. |
| `slack` | Missing | Out-of-scope | Not required for current trading workflow. |
| `songsee` | Missing | Out-of-scope | Not required for current trading workflow. |
| `sonoscli` | Missing | Out-of-scope | Not required for current trading workflow. |
| `spotify-player` | Missing | Out-of-scope | Not required for current trading workflow. |
| `summarize` | Missing | In-scope | Skill loading/execution framework missing. |
| `things-mac` | Missing | Out-of-scope | Not required for current trading workflow. |
| `tmux` | Missing | Out-of-scope | Not required for current trading workflow. |
| `trello` | Missing | Out-of-scope | Not required for current trading workflow. |
| `video-frames` | Missing | Out-of-scope | Not required for current trading workflow. |
| `voice-call` | Missing | Out-of-scope | Not required for current trading workflow. |
| `wacli` | Missing | Out-of-scope | Not required for current trading workflow. |
| `weather` | Missing | Out-of-scope | Not required for current trading workflow. |

## Strict In-Scope Gap Report (Priority Order)
1. **Gateway/session control plane parity gap**: missing explicit session model, route bindings, and WS control protocol (`openclaw/src/gateway`, `sessions`, `routing`).
2. **Daemon/service parity gap**: no installer-managed service lifecycle (`openclaw/src/daemon`).
3. **Memory parity gap**: no persistent/vector memory backend (`openclaw/src/memory`).
4. **Evaluation parity gap**: no formal eval harness/dataset/regression pipeline (`dexter/src/evals`).
5. **Skill framework gap**: no structured skill registry + runtime loading (`dexter/src/skills`, `openclaw/skills`).
6. **Link understanding gap**: no dedicated link ingestion + enrichment pass (`openclaw/src/link-understanding`).
7. **Web control UI gap**: no dashboard/ops UI (`openclaw/src/web`).
8. **Hook/plugin extension points gap**: no stable hook contracts (`openclaw/src/hooks`, `plugin-sdk`, `plugins`).
9. **Pairing/auth hardening gap**: admin allowlist exists, but not code-based sender pairing flow (`openclaw/src/pairing`).
10. **Multi-channel channel-layer gap**: Telegram only; no standardized channel runtime abstraction (`openclaw/src/channels`).

## Full-Performance Implementation Plan
### Phase 0 (Stability Baseline, 1-2 days)
- Freeze current trading signal pipeline behavior; add snapshot tests for XAUUSD/Crypto/Stocks signal formatting.
- Add structured event log schema for scans/alerts/admin commands.

### Phase 1 (Control Plane Core, 3-5 days)
- Introduce `gateway` package with session IDs, route keys, and event bus.
- Add per-session runtime state (model/profile/permissions/thinking-level).
- Expose local WS control endpoint for admin + tooling integration.

### Phase 2 (Service + Security, 2-4 days)
- Add daemon service installers (Windows Task Scheduler + Linux systemd templates).
- Implement pairing-code workflow for first-contact admin authorization.
- Add hardened command auth policy matrix (private/group/channel-level).

### Phase 3 (Memory + Research Quality, 3-6 days)
- Add persistent memory store (SQLite first, vector optional).
- Add link understanding pipeline for `/research` and “why signal” explanations.
- Cache tool outputs with expiry + provenance trace IDs.

### Phase 4 (Skills + Extensions, 4-7 days)
- Implement minimal skill registry (`SKILL.md` loader + allowlisted tool map).
- Add plugin hook points (`before_scan`, `after_scan`, `before_alert`, `admin_intent`).
- Add optional channels abstraction to support future WhatsApp/Discord without refactor.

### Phase 5 (Evals + Ops UI, 4-8 days)
- Build eval runner for deterministic regression across market scenarios + prompts.
- Add lightweight web ops dashboard (health, queue, recent alerts, provider status).
- Add release checklist and CI gates for parity-critical paths.

## Acceptance Criteria for “Parity-Complete (In-Scope)”
- Sessionized control-plane + daemonized 24/7 service + pairing auth in place.
- Memory-backed research and signal explanation with traceable provenance.
- Skill/hook framework active with at least 3 production skills.
- Eval suite passing with stable regression thresholds.
- Web ops UI + health/alert telemetry operational.