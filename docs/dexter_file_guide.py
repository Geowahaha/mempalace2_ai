# ============================================================
#  DEXTER PRO — File Loading Guide for Claude Code
#  SAVE TO: D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\docs\dexter_file_guide.py
#  Cheat sheet: which files to /add for each task type.
#  Rule: start with minimum, add more only if Claude asks.
# ============================================================

# ────────────────────────────────────────────────────────────
# WHERE EACH FILE LIVES
# ────────────────────────────────────────────────────────────
FILE_LOCATIONS = {
    "CLAUDE.md":                    "D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/CLAUDE.md",
    "dexter_claude_prompts.py":     "D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/docs/dexter_claude_prompts.py",
    "dexter_file_guide.py":         "D:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/docs/dexter_file_guide.py",
    "MEMORY.md (auto)":             "C:/Users/mrgeo/.claude/projects/d--dexter-pro-v3-fixed/memory/MEMORY.md",
}

# ────────────────────────────────────────────────────────────
# TASK → FILES MAPPING
# ────────────────────────────────────────────────────────────
TASK_TO_FILES = {

    # ── DAILY START ────────────────────────────────────────
    "daily_start": [
        "scheduler.py",
        "config.py",
        "data/runtime/trading_manager_state.json",
        # CLAUDE.md + MEMORY.md load automatically — don't /add them
    ],

    # ── FSS MONITORING (current focus) ────────────────────
    "fss_check": [
        "data/ctrader_openapi.db",           # check execution_journal
        "scheduler.py",                       # see pattern bridge
    ],
    "fss_deep_audit": [
        "scheduler.py",
        "config.py",
        "data/reports/chart_state_memory_report.json",
        "tests/test_scheduler_watchlist.py",
    ],
    "fss_not_firing": [
        "scheduler.py",
        "config.py",
        "data/ctrader_openapi.db",
        "data/runtime/trading_manager_state.json",
    ],

    # ── ORDER / EXECUTION BUGS ─────────────────────────────
    "order_not_firing": [
        "execution/",
        "api/",
        "config.py",
    ],
    "order_wrong_size": [
        "execution/",
        "config.py",
    ],
    "order_wrong_direction": [
        "execution/",
        "config.py",
        "scanners/",
    ],
    "order_silent_fail": [
        "execution/",
        "api/",
        "runtime/",
    ],

    # ── POSITION STATE BUGS ────────────────────────────────
    "position_drift": [
        "runtime/",
        "api/",
        "store/",
        "execution/",
    ],
    "position_not_closing": [
        "runtime/",
        "api/",
        "execution/",
    ],
    "sl_not_moving": [
        "execution/",
        "config.py",
        "api/",
    ],
    "canary_state_confusion": [
        "runtime/",
        "execution/",
        "config.py",
    ],

    # ── SIGNAL / SCANNER BUGS ──────────────────────────────
    "no_signals_firing": [
        "scanners/",
        "agent/",
        "market/",
        "config.py",
    ],
    "too_many_signals": [
        "scanners/",
        "config.py",
        "learning/",
    ],
    "wrong_confidence_score": [
        "scanners/",
        "agent/",
        "learning/",
        "analysis/",
        "config.py",
    ],
    "regime_blocking_signals": [
        "analysis/",
        "market/",
        "config.py",
    ],

    # ── SCHEDULER / ROUTING ────────────────────────────────
    "scheduler_routing_bug": [
        "scheduler.py",
        "config.py",
        "data/runtime/trading_manager_state.json",
    ],
    "canary_not_firing": [
        "runtime/",
        "execution/",
        "config.py",
        "scanners/",
    ],
    "canary_family_review": [
        "execution/",
        "config.py",
        "store/",
    ],
    "swarm_sampling_issue": [
        "scheduler.py",
        "config.py",
        "data/runtime/trading_manager_state.json",
    ],

    # ── TELEGRAM / NOTIFICATION ────────────────────────────
    "telegram_not_sending": [
        "notifier/",
        "config.py",
    ],
    "telegram_spam": [
        "notifier/",
        "config.py",
        "scanners/",
    ],

    # ── LEARNING / WINNER LOGIC ────────────────────────────
    "winner_logic_review": [
        "learning/",
        "store/",
        "config.py",
    ],
    "band_context_missing": [
        "learning/",
        "data/reports/chart_state_memory_report.json",
        "config.py",
    ],

    # ── ARCHITECTURE / NEW FEATURE ────────────────────────
    "add_new_strategy_family": [
        "scanners/",           # existing scanner as template
        "execution/",          # how families register
        "config.py",           # env pattern to follow
        "tests/",              # test pattern to follow
    ],
    "add_new_alert": [
        "notifier/",
        "config.py",
    ],
    "refactor_module": [
        "docs/",               # read FULL_PARITY_AUDIT_CHECKLIST first!
        # then add the specific module
    ],
    "add_config_key": [
        "config.py",
        ".env.local",
        # always update both together
    ],

    # ── SECURITY / OPS ─────────────────────────────────────
    "key_rotation": [
        "config.py",           # check all env key names
        "ops/",                # rotation scripts if any
        # never /add .env.local.backup-*
    ],
    "startup_issues": [
        "main.py",
        "config.py",
        "ops/dexter_monitor_watchdog.ps1",
    ],
}


# ────────────────────────────────────────────────────────────
# NEVER LOAD THESE
# ────────────────────────────────────────────────────────────
NEVER_LOAD = [
    "logs/",                          # huge, read-only, no code value
    "*.key", "*.pem", "*.pub",        # SSH keys — security
    ".env.local.backup-*",            # stale / expired keys
    "dexter_pro_v3_fixed+1.zip",      # 554MB archive
    "__pycache__/",
    ".pytest_cache/",
    "_temp_openclaw/",
    "_tmp_openclaw/",
    "temp-grok/",
    "ctr_test_out",
    "mgr_test_out",
    "sched_test_out",
]


# ────────────────────────────────────────────────────────────
# TOKEN SAVING RULES
# ────────────────────────────────────────────────────────────
TOKEN_RULES = """
1. Start with 2-3 files max. Add more only if Claude asks.
2. /clear between unrelated tasks — old context wastes tokens.
3. /compact when conversation > 20 turns — preserves key decisions.
4. For scheduler.py (large file): ask Claude to search specific
   function names with rg rather than loading the whole file.
5. For .env.local (1200 lines): /add only when auditing thresholds.
   For code tasks, config.py is enough (already parsed).
6. CLAUDE.md + MEMORY.md load automatically — never /add manually.
"""


# ────────────────────────────────────────────────────────────
# COPY-PASTE SESSION WORKFLOW
# ────────────────────────────────────────────────────────────
WORKFLOW = """
# Morning startup
cd D:\\dexter_pro_v3_fixed\\dexter_pro_v3_fixed
claude
[paste SESSION_START from dexter_claude_prompts.py]

# Debugging a specific issue
/clear
/add [files from TASK_TO_FILES above]
[paste relevant DEBUG_* prompt]

# Switching to different task
/clear
/add [different files]
[paste new prompt]

# End of day — update MEMORY.md
/memory
[summarize what was changed today and current live state]
"""
