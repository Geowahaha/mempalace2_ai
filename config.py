"""
config.py - Central configuration for Dexter Pro
Loads from .env.local first (highest priority), then falls back to .env
"""
import json
import os
from pathlib import Path
import sys
from typing import Optional
from dotenv import load_dotenv

# ── Load order: .env.local → .env ────────────────────────────────────────────
_BASE = Path(__file__).parent

# In pytest runs we avoid loading user-local `.env.local` to keep unit tests deterministic.
# Note: `PYTEST_CURRENT_TEST` may not be set yet during module import, so also detect via argv.
_is_pytest = bool((os.getenv("PYTEST_CURRENT_TEST", "") or "").strip()) or any(
    "pytest" in str(a).lower() for a in sys.argv
)

# .env.local overrides everything — this is your real config file
_env_local = _BASE / ".env.local"
if not _is_pytest:
    if _env_local.exists():
        load_dotenv(dotenv_path=_env_local, override=True)
        print("[Config] Loaded: .env.local")
    else:
        # Fallback to plain .env if present
        _env_file = _BASE / ".env"
        if _env_file.exists():
            load_dotenv(dotenv_path=_env_file, override=True)
            print("[Config] Loaded: .env (tip: rename to .env.local)")
        else:
            print("[Config] WARNING: No .env.local found - using system environment variables only")


class Config:
    # ── AI Brain ───────────────────────────────────────────────────────────────
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    GROQ_API_KEY:      str = os.getenv("GROQ_API_KEY", "")
    GEMINI_API_KEY:    str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_VERTEX_AI_API_KEY: str = os.getenv("GEMINI_VERTEX_AI_API_KEY", "")
    OPENAI_API_KEY:    str = os.getenv("OPENAI_API_KEY", "")     # optional fallback
    OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
    AI_MODEL:          str = os.getenv("AI_MODEL", "claude-sonnet-4-6")
    GROQ_MODEL:        str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    GEMINI_MODEL:      str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    GEMINI_VERTEX_MODEL: str = os.getenv("GEMINI_VERTEX_MODEL", os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite"))
    OPENROUTER_MODEL:  str = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
    AI_PROVIDER:       str = os.getenv("AI_PROVIDER", "auto")  # auto|groq|gemini|anthropic|openrouter
    DEXTER_BRIDGE_API_TOKEN: str = os.getenv("DEXTER_BRIDGE_API_TOKEN", "")

    # ── cTrader OpenAPI / Store ───────────────────────────────────────────────
    CTRADER_ENABLED: bool = os.getenv("CTRADER_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_AUTOTRADE_ENABLED: bool = os.getenv("CTRADER_AUTOTRADE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DRY_RUN: bool = os.getenv("CTRADER_DRY_RUN", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_USE_DEMO: bool = os.getenv("CTRADER_USE_DEMO", "0").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PROFILE_VERSION: str = os.getenv("CTRADER_PROFILE_VERSION", "ctrader_profile_v1")
    MISSION_STACK_VERSION: str = os.getenv("MISSION_STACK_VERSION", "mission_stack_v1")
    CTRADER_DB_PATH: str = os.getenv("CTRADER_DB_PATH", "")
    CTRADER_ACCOUNT_ID: str = os.getenv("CTRADER_ACCOUNT_ID", "")
    CTRADER_ACCOUNT_LOGIN: str = os.getenv("CTRADER_ACCOUNT_LOGIN", "")
    CTRADER_SYNC_ENABLED: bool = os.getenv("CTRADER_SYNC_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_SYNC_INTERVAL_MIN: int = int(os.getenv("CTRADER_SYNC_INTERVAL_MIN", "1"))
    CTRADER_SYNC_DEALS_LOOKBACK_HOURS: int = int(os.getenv("CTRADER_SYNC_DEALS_LOOKBACK_HOURS", "72"))
    CTRADER_EXECUTOR_TIMEOUT_SEC: int = int(os.getenv("CTRADER_EXECUTOR_TIMEOUT_SEC", "25"))
    CTRADER_HEALTHCHECK_TIMEOUT_SEC: int = int(os.getenv("CTRADER_HEALTHCHECK_TIMEOUT_SEC", "18"))
    CTRADER_ALLOWED_SOURCES: str = os.getenv("CTRADER_ALLOWED_SOURCES", "scalp_xauusd,scalp_ethusd,scalp_btcusd,xauusd_scheduled,xauusd_scheduled:winner,fibo_xauusd")
    CTRADER_ALLOWED_SYMBOLS: str = os.getenv("CTRADER_ALLOWED_SYMBOLS", "XAUUSD,ETHUSD,BTCUSD")
    CTRADER_SOURCE_PROFILE_GATE_ENABLED: bool = os.getenv("CTRADER_SOURCE_PROFILE_GATE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE: float = float(os.getenv("CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", "70.0"))
    CTRADER_XAU_SCHEDULED_ALLOWED_SESSIONS: str = os.getenv("CTRADER_XAU_SCHEDULED_ALLOWED_SESSIONS", "london|london,new_york,overlap")
    CTRADER_XAU_SCHEDULED_ALLOWED_TIMEFRAMES: str = os.getenv("CTRADER_XAU_SCHEDULED_ALLOWED_TIMEFRAMES", "1h")
    CTRADER_XAU_SCHEDULED_ALLOWED_ENTRY_TYPES: str = os.getenv("CTRADER_XAU_SCHEDULED_ALLOWED_ENTRY_TYPES", "limit")
    CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED: bool = os.getenv("CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED: bool = os.getenv("CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_SCHEDULED_CANARY_MIN_RR: float = float(os.getenv("CTRADER_SCHEDULED_CANARY_MIN_RR", "0.85"))
    CTRADER_SCHEDULED_CANARY_MIN_STOP_KEEP_RATIO: float = float(os.getenv("CTRADER_SCHEDULED_CANARY_MIN_STOP_KEEP_RATIO", "0.58"))
    CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_RETEST_ENABLED: bool = os.getenv("CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_RETEST_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_PULLBACK_RISK_RATIO: float = float(os.getenv("CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_PULLBACK_RISK_RATIO", "0.20"))
    CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_MIN_OFFSET_PCT: float = float(os.getenv("CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_MIN_OFFSET_PCT", "0.00035"))
    CTRADER_BTC_WINNER_MIN_CONFIDENCE: float = float(os.getenv("CTRADER_BTC_WINNER_MIN_CONFIDENCE", "70.0"))
    CTRADER_BTC_WINNER_MAX_CONFIDENCE: float = float(os.getenv("CTRADER_BTC_WINNER_MAX_CONFIDENCE", "75.0"))
    CTRADER_BTC_WINNER_ALLOWED_SESSIONS_WEEKEND: str = os.getenv("CTRADER_BTC_WINNER_ALLOWED_SESSIONS_WEEKEND", "london,new_york,overlap")
    CTRADER_ETH_WINNER_DIRECT_ENABLED: bool = os.getenv("CTRADER_ETH_WINNER_DIRECT_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_RISK_USD_PER_TRADE: float = float(os.getenv("CTRADER_RISK_USD_PER_TRADE", os.getenv("SIM_RISK_USD_PER_SIGNAL", "10.0")))
    CTRADER_TP_LEVEL: int = int(os.getenv("CTRADER_TP_LEVEL", "1"))
    CTRADER_DEFAULT_VOLUME: int = int(os.getenv("CTRADER_DEFAULT_VOLUME", "0"))
    CTRADER_DEFAULT_VOLUME_SYMBOL_OVERRIDES: str = os.getenv("CTRADER_DEFAULT_VOLUME_SYMBOL_OVERRIDES", "")
    CTRADER_PRICE_SANITY_ENABLED: bool = os.getenv("CTRADER_PRICE_SANITY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PRICE_SANITY_CRYPTO_MAX_DEVIATION_PCT: float = float(os.getenv("CTRADER_PRICE_SANITY_CRYPTO_MAX_DEVIATION_PCT", "0.35"))
    CTRADER_PRICE_SANITY_XAU_MAX_DEVIATION_PCT: float = float(os.getenv("CTRADER_PRICE_SANITY_XAU_MAX_DEVIATION_PCT", "0.08"))
    CTRADER_PRICE_SANITY_FX_MAX_DEVIATION_PCT: float = float(os.getenv("CTRADER_PRICE_SANITY_FX_MAX_DEVIATION_PCT", "0.03"))
    CTRADER_MARKET_ENTRY_DRIFT_GUARD_ENABLED: bool = os.getenv("CTRADER_MARKET_ENTRY_DRIFT_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_MARKET_ENTRY_MAX_DRIFT_PCT: float = float(os.getenv("CTRADER_MARKET_ENTRY_MAX_DRIFT_PCT", "0.12"))
    CTRADER_MARKET_ENTRY_MAX_DRIFT_SYMBOL_OVERRIDES: str = os.getenv("CTRADER_MARKET_ENTRY_MAX_DRIFT_SYMBOL_OVERRIDES", "")
    PERSISTENT_CANARY_ENABLED: bool = os.getenv("PERSISTENT_CANARY_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_ALLOWED_SOURCES: str = os.getenv("PERSISTENT_CANARY_ALLOWED_SOURCES", "scalp_xauusd,scalp_btcusd,scalp_ethusd,xauusd_scheduled")
    PERSISTENT_CANARY_DIRECT_ALLOWED_SOURCES: str = os.getenv("PERSISTENT_CANARY_DIRECT_ALLOWED_SOURCES", "scalp_xauusd,scalp_btcusd,scalp_ethusd,xauusd_scheduled")
    PERSISTENT_CANARY_ALLOWED_SYMBOLS: str = os.getenv("PERSISTENT_CANARY_ALLOWED_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD")
    PERSISTENT_CANARY_RUN_PARALLEL: bool = os.getenv("PERSISTENT_CANARY_RUN_PARALLEL", "1").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_MT5_ENABLED: bool = os.getenv("PERSISTENT_CANARY_MT5_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER: float = float(os.getenv("PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER", "0.20"))
    PERSISTENT_CANARY_MT5_MAGIC_OFFSET: int = int(os.getenv("PERSISTENT_CANARY_MT5_MAGIC_OFFSET", "700"))
    PERSISTENT_CANARY_MT5_SKIP_POSITION_MANAGER: bool = os.getenv("PERSISTENT_CANARY_MT5_SKIP_POSITION_MANAGER", "1").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_CTRADER_ENABLED: bool = os.getenv("PERSISTENT_CANARY_CTRADER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_CTRADER_RISK_USD: float = float(os.getenv("PERSISTENT_CANARY_CTRADER_RISK_USD", "2.5"))
    PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED: bool = os.getenv("PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_STRATEGY_FAMILIES: str = os.getenv("PERSISTENT_CANARY_STRATEGY_FAMILIES", "xau_scalp_pullback_limit,xau_scalp_breakout_stop")
    PERSISTENT_CANARY_FAMILY_MAX_VARIANTS: int = int(os.getenv("PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", "2"))
    PERSISTENT_CANARY_FAMILY_MT5_VOLUME_MULTIPLIER: float = float(os.getenv("PERSISTENT_CANARY_FAMILY_MT5_VOLUME_MULTIPLIER", "0.10"))
    PERSISTENT_CANARY_FAMILY_CTRADER_RISK_USD: float = float(os.getenv("PERSISTENT_CANARY_FAMILY_CTRADER_RISK_USD", "1.25"))
    PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED: bool = os.getenv("PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES: str = os.getenv("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES", "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar,xau_scalp_range_repair,xau_fibo_advance")

    # ── Fibonacci Advance (fibo_advance lane / xau_fibo_advance family) ────────
    FIBO_ADVANCE_ENABLED: bool = os.getenv("FIBO_ADVANCE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FIBO_ADVANCE_MIN_CONFIDENCE: float = float(os.getenv("FIBO_ADVANCE_MIN_CONFIDENCE", "62.0"))
    FIBO_ADVANCE_MIN_FIBO_SCORE: float = float(os.getenv("FIBO_ADVANCE_MIN_FIBO_SCORE", "38.0"))
    FIBO_ADVANCE_MIN_RR: float = float(os.getenv("FIBO_ADVANCE_MIN_RR", "1.2"))
    FIBO_ADVANCE_MAX_LEVEL_DIST_PCT: float = float(os.getenv("FIBO_ADVANCE_MAX_LEVEL_DIST_PCT", "0.25"))
    FIBO_ADVANCE_SWING_LOOKBACK: int = int(os.getenv("FIBO_ADVANCE_SWING_LOOKBACK", "5"))
    FIBO_ADVANCE_MIN_IMPULSE_ATR: float = float(os.getenv("FIBO_ADVANCE_MIN_IMPULSE_ATR", "1.2"))
    FIBO_ADVANCE_SL_ATR_BUFFER: float = float(os.getenv("FIBO_ADVANCE_SL_ATR_BUFFER", "0.25"))
    FIBO_ADVANCE_CTRADER_RISK_USD: float = float(os.getenv("FIBO_ADVANCE_CTRADER_RISK_USD", "1.0"))
    # Microstructure thresholds (more lenient than base scanner — institutions accumulate quietly at Fib)
    FIBO_ADVANCE_MICRO_DELTA_THR: float = float(os.getenv("FIBO_ADVANCE_MICRO_DELTA_THR", "0.30"))
    FIBO_ADVANCE_MICRO_IMB_THR: float = float(os.getenv("FIBO_ADVANCE_MICRO_IMB_THR", "0.35"))
    FIBO_ADVANCE_MICRO_VEL_THR: float = float(os.getenv("FIBO_ADVANCE_MICRO_VEL_THR", "0.05"))
    # Fibonacci Killer protection thresholds
    FIBO_ADVANCE_KILLER_ATR_MULT: float = float(os.getenv("FIBO_ADVANCE_KILLER_ATR_MULT", "1.8"))
    FIBO_ADVANCE_KILLER_DELTA_THRESHOLD: float = float(os.getenv("FIBO_ADVANCE_KILLER_DELTA_THRESHOLD", "0.40"))
    FIBO_ADVANCE_KILLER_VOL_SPIKE: float = float(os.getenv("FIBO_ADVANCE_KILLER_VOL_SPIKE", "2.5"))
    FIBO_ADVANCE_KILLER_MAX_SPREAD_EXP: float = float(os.getenv("FIBO_ADVANCE_KILLER_MAX_SPREAD_EXP", "1.25"))
    FIBO_ADVANCE_KILLER_RETRACE_VEL: float = float(os.getenv("FIBO_ADVANCE_KILLER_RETRACE_VEL", "2.0"))
    # Trend awareness (prevent April 7 disaster — strong D1 trend filter)
    FIBO_TREND_STRONG_EMA_SPREAD_PCT: float = float(os.getenv("FIBO_TREND_STRONG_EMA_SPREAD_PCT", "0.5"))
    FIBO_SESSION_DIRECTION_BIAS_PENALTY: float = float(os.getenv("FIBO_SESSION_DIRECTION_BIAS_PENALTY", "-12.0"))
    # Soft pause after consecutive losses (faster circuit breaker)
    FIBO_ADVANCE_SOFT_PAUSE_CONSEC: int = int(os.getenv("FIBO_ADVANCE_SOFT_PAUSE_CONSEC", "5"))
    FIBO_ADVANCE_SOFT_PAUSE_MIN: int = int(os.getenv("FIBO_ADVANCE_SOFT_PAUSE_MIN", "30"))
    FIBO_ADVANCE_SCAN_INTERVAL_SEC: int = int(os.getenv("FIBO_ADVANCE_SCAN_INTERVAL_SEC", "300"))
    # Institution-grade gates
    FIBO_ADVANCE_SHARPNESS_KNIFE_THR: int = int(os.getenv("FIBO_ADVANCE_SHARPNESS_KNIFE_THR", "30"))
    FIBO_ADVANCE_MAX_IMPULSE_AGE_BARS: int = int(os.getenv("FIBO_ADVANCE_MAX_IMPULSE_AGE_BARS", "40"))
    # Scout mode (H1→M15 intermediate setups — fires while waiting for Sniper)
    FIBO_SCOUT_ENABLED: bool = os.getenv("FIBO_SCOUT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FIBO_SCOUT_MIN_CONFIDENCE: float = float(os.getenv("FIBO_SCOUT_MIN_CONFIDENCE", "55.0"))
    FIBO_SCOUT_MIN_FIBO_SCORE: float = float(os.getenv("FIBO_SCOUT_MIN_FIBO_SCORE", "28.0"))
    FIBO_SCOUT_MIN_RR: float = float(os.getenv("FIBO_SCOUT_MIN_RR", "1.0"))
    FIBO_SCOUT_MAX_LEVEL_DIST_PCT: float = float(os.getenv("FIBO_SCOUT_MAX_LEVEL_DIST_PCT", "0.15"))
    FIBO_SCOUT_SL_ATR_BUFFER: float = float(os.getenv("FIBO_SCOUT_SL_ATR_BUFFER", "0.20"))
    FIBO_SCOUT_CTRADER_RISK_USD: float = float(os.getenv("FIBO_SCOUT_CTRADER_RISK_USD", "0.5"))
    FIBO_SCOUT_SHARPNESS_KNIFE_THR: int = int(os.getenv("FIBO_SCOUT_SHARPNESS_KNIFE_THR", "25"))
    FIBO_SCOUT_MAX_IMPULSE_AGE_BARS: int = int(os.getenv("FIBO_SCOUT_MAX_IMPULSE_AGE_BARS", "30"))
    # When True, entry sharpness / knife gate blocks if capture features are missing (no blind Fib entries).
    FIBO_REQUIRE_CAPTURE_FEATURES: bool = os.getenv("FIBO_REQUIRE_CAPTURE_FEATURES", "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    # Reject Fib setup when |entry−SL| exceeds cap (0 = disabled). Tightens fat structural stops.
    FIBO_ADVANCE_MAX_RISK_ATR_MULT: float = float(os.getenv("FIBO_ADVANCE_MAX_RISK_ATR_MULT", "0") or 0)
    FIBO_ADVANCE_MAX_RISK_ENTRY_PCT: float = float(os.getenv("FIBO_ADVANCE_MAX_RISK_ENTRY_PCT", "0") or 0)
    FIBO_SCOUT_MAX_RISK_ATR_MULT: float = float(os.getenv("FIBO_SCOUT_MAX_RISK_ATR_MULT", "0") or 0)
    FIBO_SCOUT_MAX_RISK_ENTRY_PCT: float = float(os.getenv("FIBO_SCOUT_MAX_RISK_ENTRY_PCT", "0") or 0)
    # Cap first TP distance in R so PM can bank sooner (0 = no cap). Long: tp1 <= entry+risk*R; short: tp1 >= entry−risk*R
    FIBO_ADVANCE_TP1_MAX_R: float = float(os.getenv("FIBO_ADVANCE_TP1_MAX_R", "0") or 0)
    FIBO_SCOUT_TP1_MAX_R: float = float(os.getenv("FIBO_SCOUT_TP1_MAX_R", "0") or 0)
    # Evidence gate: do not let Fib trade early 38.2/50% pullbacks as if they were GP entries.
    FIBO_ADVANCE_REQUIRE_GOLDEN_POCKET: bool = os.getenv("FIBO_ADVANCE_REQUIRE_GOLDEN_POCKET", "1").strip().lower() in ("1", "true", "yes", "on")
    FIBO_SCOUT_REQUIRE_GOLDEN_POCKET: bool = os.getenv("FIBO_SCOUT_REQUIRE_GOLDEN_POCKET", "1").strip().lower() in ("1", "true", "yes", "on")
    FIBO_ADVANCE_MIN_ENTRY_LEVEL_RATIO: float = float(os.getenv("FIBO_ADVANCE_MIN_ENTRY_LEVEL_RATIO", "0.618") or 0.618)
    FIBO_SCOUT_MIN_ENTRY_LEVEL_RATIO: float = float(os.getenv("FIBO_SCOUT_MIN_ENTRY_LEVEL_RATIO", "0.618") or 0.618)
    FIBO_ADVANCE_MIN_RETRACEMENT_DEPTH: float = float(os.getenv("FIBO_ADVANCE_MIN_RETRACEMENT_DEPTH", "0.618") or 0.618)
    FIBO_SCOUT_MIN_RETRACEMENT_DEPTH: float = float(os.getenv("FIBO_SCOUT_MIN_RETRACEMENT_DEPTH", "0.618") or 0.618)
    FIBO_ADVANCE_MAX_RETRACEMENT_DEPTH: float = float(os.getenv("FIBO_ADVANCE_MAX_RETRACEMENT_DEPTH", "0.786") or 0.786)
    FIBO_SCOUT_MAX_RETRACEMENT_DEPTH: float = float(os.getenv("FIBO_SCOUT_MAX_RETRACEMENT_DEPTH", "0.786") or 0.786)
    FIBO_GOLDEN_GATE_TOLERANCE: float = float(os.getenv("FIBO_GOLDEN_GATE_TOLERANCE", "0.012") or 0.012)
    FIBO_ADVANCE_MIN_IMPULSE_STRENGTH_SCORE: float = float(os.getenv("FIBO_ADVANCE_MIN_IMPULSE_STRENGTH_SCORE", "0.55") or 0.55)
    FIBO_SCOUT_MIN_IMPULSE_STRENGTH_SCORE: float = float(os.getenv("FIBO_SCOUT_MIN_IMPULSE_STRENGTH_SCORE", "0.50") or 0.50)
    FIBO_ADVANCE_MIN_MOMENTUM_SCORE: int = int(os.getenv("FIBO_ADVANCE_MIN_MOMENTUM_SCORE", "4") or 4)
    FIBO_SCOUT_MIN_MOMENTUM_SCORE: int = int(os.getenv("FIBO_SCOUT_MIN_MOMENTUM_SCORE", "4") or 4)
    FIBO_SCOUT_REQUIRE_MTF_STACKING: bool = os.getenv("FIBO_SCOUT_REQUIRE_MTF_STACKING", "1").strip().lower() in ("1", "true", "yes", "on")
    # Audit result: fibo_xauusd short is quarantined by default; long side remains available.
    FIBO_ADVANCE_SHORT_QUARANTINE_ENABLED: bool = os.getenv("FIBO_ADVANCE_SHORT_QUARANTINE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    # Fibo Position Manager — time-based profit lock (progressive SL tightening)
    FIBO_PM_TIME_LOCK_ENABLED: bool = os.getenv("FIBO_PM_TIME_LOCK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FIBO_PM_BE_AFTER_MIN: float = float(os.getenv("FIBO_PM_BE_AFTER_MIN", "20"))
    FIBO_PM_LOCK_30_AFTER_MIN: float = float(os.getenv("FIBO_PM_LOCK_30_AFTER_MIN", "45"))
    FIBO_PM_LOCK_50_AFTER_MIN: float = float(os.getenv("FIBO_PM_LOCK_50_AFTER_MIN", "90"))
    FIBO_PM_LOCK_70_AFTER_MIN: float = float(os.getenv("FIBO_PM_LOCK_70_AFTER_MIN", "150"))
    # Momentum exhaustion profit lock (lock profit when momentum dies)
    FIBO_PM_EXHAUSTION_LOCK_ENABLED: bool = os.getenv("FIBO_PM_EXHAUSTION_LOCK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_EXHAUSTION_MIN_AGE_MIN: float = float(os.getenv("CTRADER_PM_XAU_EXHAUSTION_MIN_AGE_MIN", "3.0"))
    CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DELTA: float = float(os.getenv("CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DELTA", "0.08"))
    CTRADER_PM_XAU_EXHAUSTION_MAX_VOLUME: float = float(os.getenv("CTRADER_PM_XAU_EXHAUSTION_MAX_VOLUME", "0.25"))
    CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DRIFT: float = float(os.getenv("CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DRIFT", "0.008"))
    CTRADER_PM_XAU_EXHAUSTION_MAX_REJECTION: float = float(os.getenv("CTRADER_PM_XAU_EXHAUSTION_MAX_REJECTION", "0.25"))
    CTRADER_PM_XAU_EXHAUSTION_REQUIRED_SIGNALS: int = int(os.getenv("CTRADER_PM_XAU_EXHAUSTION_REQUIRED_SIGNALS", "3"))
    # Comma-separated families that ignore Strategy Lab blocked/shadow for persistent canary candidate loading.
    # Default: XAU flow sidecars (still require pattern + chart contexts / first_sample gates in builders).
    PERSISTENT_CANARY_IGNORE_STRATEGY_LAB_BLOCK: str = os.getenv(
        "PERSISTENT_CANARY_IGNORE_STRATEGY_LAB_BLOCK",
        "xau_scalp_flow_short_sidecar,xau_scalp_flow_long_sidecar",
    )
    PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS: int = int(os.getenv("PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", "1"))
    PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_CTRADER_RISK_USD: float = float(os.getenv("PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_CTRADER_RISK_USD", "0.75"))
    BTC_WEEKDAY_LOB_ALLOWED_SESSIONS: str = os.getenv("BTC_WEEKDAY_LOB_ALLOWED_SESSIONS", "new_york|london,new_york,overlap")
    BTC_WEEKDAY_LOB_MIN_CONFIDENCE: float = float(os.getenv("BTC_WEEKDAY_LOB_MIN_CONFIDENCE", "70"))
    BTC_WEEKDAY_LOB_MAX_CONFIDENCE: float = float(os.getenv("BTC_WEEKDAY_LOB_MAX_CONFIDENCE", "74.9"))
    BTC_WEEKDAY_LOB_ALLOWED_PATTERNS: str = os.getenv("BTC_WEEKDAY_LOB_ALLOWED_PATTERNS", "OB_BOUNCE,CHOCH_ENTRY")
    BTC_WEEKDAY_LOB_ALLOW_MARKET: bool = os.getenv("BTC_WEEKDAY_LOB_ALLOW_MARKET", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER: bool = os.getenv("BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE: bool = os.getenv("BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE: float = float(os.getenv("BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE", "72.8"))
    BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB: float = float(os.getenv("BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB", "0.65"))
    BTC_WEEKDAY_LOB_CHOCH_MARKET_TO_LIMIT_ENABLED: bool = os.getenv("BTC_WEEKDAY_LOB_CHOCH_MARKET_TO_LIMIT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_WEEKDAY_LOB_CHOCH_MARKET_MAX_CONFIDENCE: float = float(os.getenv("BTC_WEEKDAY_LOB_CHOCH_MARKET_MAX_CONFIDENCE", "72.2"))
    BTC_WEEKDAY_LOB_CHOCH_MARKET_MIN_NEURAL_PROB: float = float(os.getenv("BTC_WEEKDAY_LOB_CHOCH_MARKET_MIN_NEURAL_PROB", "0.63"))
    BTC_WEEKDAY_LOB_CHOCH_LIMIT_PULLBACK_RISK_RATIO: float = float(os.getenv("BTC_WEEKDAY_LOB_CHOCH_LIMIT_PULLBACK_RISK_RATIO", "0.12"))
    BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER: float = float(os.getenv("BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", "0.70"))
    BTC_WEEKDAY_LOB_CTRADER_RISK_USD: float = float(os.getenv("BTC_WEEKDAY_LOB_CTRADER_RISK_USD", "0.9"))
    BTC_WEEKDAY_LOB_NARROW_LIVE_RISK_USD: float = float(os.getenv("BTC_WEEKDAY_LOB_NARROW_LIVE_RISK_USD", "1.1"))
    BTC_WEEKDAY_LOB_PROMOTION_MIN_RESOLVED: int = int(os.getenv("BTC_WEEKDAY_LOB_PROMOTION_MIN_RESOLVED", "4"))
    BTC_WEEKDAY_LOB_PROMOTION_MIN_WIN_RATE: float = float(os.getenv("BTC_WEEKDAY_LOB_PROMOTION_MIN_WIN_RATE", "0.55"))
    BTC_WEEKDAY_LOB_PROMOTION_MIN_PNL_USD: float = float(os.getenv("BTC_WEEKDAY_LOB_PROMOTION_MIN_PNL_USD", "1.0"))
    ETH_WEEKDAY_PROBE_ALLOWED_SESSIONS: str = os.getenv("ETH_WEEKDAY_PROBE_ALLOWED_SESSIONS", "london,new_york,overlap")
    ETH_WEEKDAY_PROBE_MIN_CONFIDENCE: float = float(os.getenv("ETH_WEEKDAY_PROBE_MIN_CONFIDENCE", "74"))
    ETH_WEEKDAY_PROBE_MAX_CONFIDENCE: float = float(os.getenv("ETH_WEEKDAY_PROBE_MAX_CONFIDENCE", "79.9"))
    ETH_WEEKDAY_PROBE_ALLOWED_PATTERNS: str = os.getenv("ETH_WEEKDAY_PROBE_ALLOWED_PATTERNS", "OB_BOUNCE")
    ETH_WEEKDAY_PROBE_ALLOW_MARKET: bool = os.getenv("ETH_WEEKDAY_PROBE_ALLOW_MARKET", "1").strip().lower() in ("1", "true", "yes", "on")
    ETH_WEEKDAY_PROBE_REQUIRE_STRONG_WINNER: bool = os.getenv("ETH_WEEKDAY_PROBE_REQUIRE_STRONG_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    ETH_WEEKDAY_PROBE_CTRADER_RISK_USD: float = float(os.getenv("ETH_WEEKDAY_PROBE_CTRADER_RISK_USD", "0.35"))

    # ── Crypto Cluster Loss Guard + Daily Cap (Phase 1 — isolated from XAU) ──
    CRYPTO_CLUSTER_LOSS_GUARD_ENABLED: bool = os.getenv("CRYPTO_CLUSTER_LOSS_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_CLUSTER_LOSS_WINDOW_HOURS: float = float(os.getenv("BTC_CLUSTER_LOSS_WINDOW_HOURS", "3.0"))
    BTC_CLUSTER_LOSS_MIN_LOSSES: int = int(os.getenv("BTC_CLUSTER_LOSS_MIN_LOSSES", "2"))
    BTC_DAILY_TRADE_CAP: int = int(os.getenv("BTC_DAILY_TRADE_CAP", "3"))
    ETH_CLUSTER_LOSS_WINDOW_HOURS: float = float(os.getenv("ETH_CLUSTER_LOSS_WINDOW_HOURS", "2.0"))
    ETH_CLUSTER_LOSS_MIN_LOSSES: int = int(os.getenv("ETH_CLUSTER_LOSS_MIN_LOSSES", "2"))
    ETH_DAILY_TRADE_CAP: int = int(os.getenv("ETH_DAILY_TRADE_CAP", "2"))

    # ── BTC Flow Short Sidecar (BFSS) — crypto clone of XAU FSS, isolated ──
    BTC_FSS_ENABLED: bool = os.getenv("BTC_FSS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_FSS_MIN_CONFIDENCE: float = float(os.getenv("BTC_FSS_MIN_CONFIDENCE", "67.0"))
    BTC_FSS_MIN_DELTA_PROXY: float = float(os.getenv("BTC_FSS_MIN_DELTA_PROXY", "0.04"))
    BTC_FSS_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("BTC_FSS_MIN_BAR_VOLUME_PROXY", "0.28"))
    BTC_FSS_TRIGGER_RISK_RATIO: float = float(os.getenv("BTC_FSS_TRIGGER_RISK_RATIO", "0.10"))
    BTC_FSS_STOP_LIFT_RATIO: float = float(os.getenv("BTC_FSS_STOP_LIFT_RATIO", "0.28"))
    BTC_FSS_CTRADER_RISK_USD: float = float(os.getenv("BTC_FSS_CTRADER_RISK_USD", "0.65"))
    BTC_FSS_LOOKBACK_SEC: int = int(os.getenv("BTC_FSS_LOOKBACK_SEC", "240"))

    # ── BTC Flow Long Sidecar (BFLS) — crypto clone of XAU FLS, isolated ──
    BTC_FLS_ENABLED: bool = os.getenv("BTC_FLS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_FLS_MIN_CONFIDENCE: float = float(os.getenv("BTC_FLS_MIN_CONFIDENCE", "67.0"))
    BTC_FLS_MIN_DELTA_PROXY: float = float(os.getenv("BTC_FLS_MIN_DELTA_PROXY", "0.04"))
    BTC_FLS_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("BTC_FLS_MIN_BAR_VOLUME_PROXY", "0.28"))
    BTC_FLS_TRIGGER_RISK_RATIO: float = float(os.getenv("BTC_FLS_TRIGGER_RISK_RATIO", "0.10"))
    BTC_FLS_STOP_LIFT_RATIO: float = float(os.getenv("BTC_FLS_STOP_LIFT_RATIO", "0.28"))
    BTC_FLS_CTRADER_RISK_USD: float = float(os.getenv("BTC_FLS_CTRADER_RISK_USD", "0.65"))
    BTC_FLS_LOOKBACK_SEC: int = int(os.getenv("BTC_FLS_LOOKBACK_SEC", "240"))

    # ── BTC Scheduled High-Confidence Session Bypass (Phase 3) ──
    BTC_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD: float = float(os.getenv("BTC_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD", "0.87"))

    # ── BTC Range Repair (BRR) — crypto clone of XAU RR, isolated ──
    BTC_RANGE_REPAIR_ENABLED: bool = os.getenv("BTC_RANGE_REPAIR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_RANGE_REPAIR_LOOKBACK_SEC: int = int(os.getenv("BTC_RANGE_REPAIR_LOOKBACK_SEC", "300"))
    BTC_RANGE_REPAIR_ALLOWED_STATES: str = os.getenv("BTC_RANGE_REPAIR_ALLOWED_STATES", "range_probe")
    BTC_RANGE_REPAIR_MAX_CONTINUATION_BIAS: float = float(os.getenv("BTC_RANGE_REPAIR_MAX_CONTINUATION_BIAS", "0.12"))
    BTC_RANGE_REPAIR_MIN_REJECTION_RATIO: float = float(os.getenv("BTC_RANGE_REPAIR_MIN_REJECTION_RATIO", "0.14"))
    BTC_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("BTC_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", "0.20"))
    BTC_RANGE_REPAIR_MAX_ABS_DELTA_PROXY: float = float(os.getenv("BTC_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", "0.14"))
    BTC_RANGE_REPAIR_ENTRY_RISK_RATIO: float = float(os.getenv("BTC_RANGE_REPAIR_ENTRY_RISK_RATIO", "0.10"))
    BTC_RANGE_REPAIR_ENTRY_ATR_RATIO: float = float(os.getenv("BTC_RANGE_REPAIR_ENTRY_ATR_RATIO", "0.04"))
    BTC_RANGE_REPAIR_STOP_KEEP_RISK_RATIO: float = float(os.getenv("BTC_RANGE_REPAIR_STOP_KEEP_RISK_RATIO", "0.75"))
    BTC_RANGE_REPAIR_TP1_RR: float = float(os.getenv("BTC_RANGE_REPAIR_TP1_RR", "0.50"))
    BTC_RANGE_REPAIR_TP2_RR: float = float(os.getenv("BTC_RANGE_REPAIR_TP2_RR", "0.85"))
    BTC_RANGE_REPAIR_TP3_RR: float = float(os.getenv("BTC_RANGE_REPAIR_TP3_RR", "1.15"))
    BTC_RANGE_REPAIR_CTRADER_RISK_USD: float = float(os.getenv("BTC_RANGE_REPAIR_CTRADER_RISK_USD", "0.55"))

    # ── XAU Toxic Hour Guard — block XAU scalp during historically losing hours ──
    XAU_TOXIC_HOUR_GUARD_ENABLED: bool = os.getenv("XAU_TOXIC_HOUR_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_TOXIC_HOURS_UTC: str = os.getenv("XAU_TOXIC_HOURS_UTC", "1")  # comma-separated UTC hours, default 01 (=08 BKK)

    # ── ADI — Adaptive Directional Intelligence ──
    ADI_ENABLED: bool = os.getenv("ADI_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    ADI_LOOKBACK_DAYS: int = int(os.getenv("ADI_LOOKBACK_DAYS", "14"))
    ADI_MAX_PENALTY: float = float(os.getenv("ADI_MAX_PENALTY", "-45.0"))
    ADI_MAX_BOOST: float = float(os.getenv("ADI_MAX_BOOST", "15.0"))
    ADI_COLD_START_PENALTY: float = float(os.getenv("ADI_COLD_START_PENALTY", "-6.0"))

    # ── XAU MRD — Microstructure Regime Detector (XAUUSD Scanner) ──
    MRD_DELTA_BIAS_THRESHOLD: float = float(os.getenv("MRD_DELTA_BIAS_THRESHOLD", "0.15"))
    MRD_DEPTH_IMBALANCE_THRESHOLD: float = float(os.getenv("MRD_DEPTH_IMBALANCE_THRESHOLD", "0.25"))
    MRD_TICK_VELOCITY_MIN: float = float(os.getenv("MRD_TICK_VELOCITY_MIN", "0.10"))
    MRD_HIGH_CONF_THRESHOLD: float = float(os.getenv("MRD_HIGH_CONF_THRESHOLD", "75.0"))
    MRD_HIGH_CONF_DELTA_RELAX: float = float(os.getenv("MRD_HIGH_CONF_DELTA_RELAX", "0.10"))

    # ── Crypto MRD — BTC Microstructure Regime Detector (Phase 4) ──
    BTC_MRD_ENABLED: bool = os.getenv("BTC_MRD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_MRD_LOOKBACK_SEC: int = int(os.getenv("BTC_MRD_LOOKBACK_SEC", "600"))
    BTC_MRD_BEARISH_DELTA_THRESHOLD: float = float(os.getenv("BTC_MRD_BEARISH_DELTA_THRESHOLD", "-0.05"))
    BTC_MRD_BULLISH_DELTA_THRESHOLD: float = float(os.getenv("BTC_MRD_BULLISH_DELTA_THRESHOLD", "0.05"))
    BTC_MRD_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("BTC_MRD_MIN_BAR_VOLUME_PROXY", "0.20"))

    # ── Crypto Weekend Trading ────────────────────────────────────────────────
    CRYPTO_WEEKEND_TRADING_ENABLED: bool = os.getenv("CRYPTO_WEEKEND_TRADING_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WEEKEND_RISK_MULTIPLIER: float = float(os.getenv("CRYPTO_WEEKEND_RISK_MULTIPLIER", "0.65"))
    CRYPTO_WEEKEND_BTC_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_WEEKEND_BTC_ALLOWED_SESSIONS", "*")
    CRYPTO_WEEKEND_ETH_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_WEEKEND_ETH_ALLOWED_SESSIONS", "*")
    CRYPTO_WEEKEND_ALLOW_NEUTRAL_WINNER: bool = os.getenv("CRYPTO_WEEKEND_ALLOW_NEUTRAL_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WEEKEND_ETH_ALLOW_SHORT: bool = os.getenv("CRYPTO_WEEKEND_ETH_ALLOW_SHORT", "1").strip().lower() in ("1", "true", "yes", "on")

    # ── Crypto Smart Families — global 24/7 mode (bypasses session gate for testing) ──
    CRYPTO_SMART_FAMILIES_24H_MODE: bool = os.getenv("CRYPTO_SMART_FAMILIES_24H_MODE", "0").strip().lower() in ("1", "true", "yes", "on")

    # ── Crypto Flow Short Sidecar (CFS) — sell_stop shorts, neural short-score gated ──
    CRYPTO_FLOW_SHORT_ENABLED: bool = os.getenv("CRYPTO_FLOW_SHORT_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_FLOW_SHORT_ALLOWED_SYMBOLS: str = os.getenv("CRYPTO_FLOW_SHORT_ALLOWED_SYMBOLS", "BTCUSD,ETHUSD")
    CRYPTO_FLOW_SHORT_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_FLOW_SHORT_ALLOWED_SESSIONS", "london|new_york|london,new_york,overlap")
    CRYPTO_FLOW_SHORT_MIN_SHORT_SCORE: float = float(os.getenv("CRYPTO_FLOW_SHORT_MIN_SHORT_SCORE", "70"))
    CRYPTO_FLOW_SHORT_RSI_MAX: float = float(os.getenv("CRYPTO_FLOW_SHORT_RSI_MAX", "45"))
    CRYPTO_FLOW_SHORT_MIN_EDGE: float = float(os.getenv("CRYPTO_FLOW_SHORT_MIN_EDGE", "30"))
    CRYPTO_FLOW_SHORT_MIN_CONFIDENCE: float = float(os.getenv("CRYPTO_FLOW_SHORT_MIN_CONFIDENCE", "68"))
    CRYPTO_FLOW_SHORT_MAX_CONFIDENCE: float = float(os.getenv("CRYPTO_FLOW_SHORT_MAX_CONFIDENCE", "85"))
    CRYPTO_FLOW_SHORT_BLOCK_SEVERE_WINNER: bool = os.getenv("CRYPTO_FLOW_SHORT_BLOCK_SEVERE_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_FLOW_SHORT_BREAK_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("CRYPTO_FLOW_SHORT_BREAK_STOP_TRIGGER_RISK_RATIO", "0.10"))
    CRYPTO_FLOW_SHORT_BREAK_STOP_STOP_LIFT_RATIO: float = float(os.getenv("CRYPTO_FLOW_SHORT_BREAK_STOP_STOP_LIFT_RATIO", "0.30"))
    CRYPTO_FLOW_SHORT_BTC_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_FLOW_SHORT_BTC_CTRADER_RISK_USD", "0.45"))
    CRYPTO_FLOW_SHORT_ETH_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_FLOW_SHORT_ETH_CTRADER_RISK_USD", "0.20"))

    # ── Crypto Flow Buy Stop (CFB) — buy_stop longs, neural long-score gated ──
    CRYPTO_FLOW_BUY_ENABLED: bool = os.getenv("CRYPTO_FLOW_BUY_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_FLOW_BUY_ALLOWED_SYMBOLS: str = os.getenv("CRYPTO_FLOW_BUY_ALLOWED_SYMBOLS", "BTCUSD,ETHUSD")
    CRYPTO_FLOW_BUY_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_FLOW_BUY_ALLOWED_SESSIONS", "london,new_york,overlap|new_york")
    CRYPTO_FLOW_BUY_MIN_LONG_SCORE: float = float(os.getenv("CRYPTO_FLOW_BUY_MIN_LONG_SCORE", "85"))
    CRYPTO_FLOW_BUY_RSI_MIN: float = float(os.getenv("CRYPTO_FLOW_BUY_RSI_MIN", "55"))
    CRYPTO_FLOW_BUY_RSI_MAX: float = float(os.getenv("CRYPTO_FLOW_BUY_RSI_MAX", "70"))
    CRYPTO_FLOW_BUY_MIN_EDGE: float = float(os.getenv("CRYPTO_FLOW_BUY_MIN_EDGE", "40"))
    CRYPTO_FLOW_BUY_MIN_CONFIDENCE: float = float(os.getenv("CRYPTO_FLOW_BUY_MIN_CONFIDENCE", "68"))
    CRYPTO_FLOW_BUY_MAX_CONFIDENCE: float = float(os.getenv("CRYPTO_FLOW_BUY_MAX_CONFIDENCE", "80"))
    CRYPTO_FLOW_BUY_REQUIRE_STRONG_WINNER: bool = os.getenv("CRYPTO_FLOW_BUY_REQUIRE_STRONG_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_FLOW_BUY_ALLOW_NEUTRAL_WINNER: bool = os.getenv("CRYPTO_FLOW_BUY_ALLOW_NEUTRAL_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_FLOW_BUY_BREAK_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("CRYPTO_FLOW_BUY_BREAK_STOP_TRIGGER_RISK_RATIO", "0.10"))
    CRYPTO_FLOW_BUY_BREAK_STOP_STOP_LIFT_RATIO: float = float(os.getenv("CRYPTO_FLOW_BUY_BREAK_STOP_STOP_LIFT_RATIO", "0.30"))
    CRYPTO_FLOW_BUY_BTC_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_FLOW_BUY_BTC_CTRADER_RISK_USD", "0.65"))
    CRYPTO_FLOW_BUY_ETH_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_FLOW_BUY_ETH_CTRADER_RISK_USD", "0.25"))

    # ── Crypto Winner Confirmed (CWC) — strong winner + high edge only ──
    CRYPTO_WINNER_CONFIRMED_ENABLED: bool = os.getenv("CRYPTO_WINNER_CONFIRMED_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WINNER_CONFIRMED_ALLOWED_SYMBOLS: str = os.getenv("CRYPTO_WINNER_CONFIRMED_ALLOWED_SYMBOLS", "BTCUSD")
    CRYPTO_WINNER_CONFIRMED_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_WINNER_CONFIRMED_ALLOWED_SESSIONS", "london,new_york,overlap")
    CRYPTO_WINNER_CONFIRMED_MIN_WIN_RATE: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_MIN_WIN_RATE", "0.62"))
    CRYPTO_WINNER_CONFIRMED_MIN_EDGE: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_MIN_EDGE", "60"))
    CRYPTO_WINNER_CONFIRMED_MIN_CONFIDENCE: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_MIN_CONFIDENCE", "70"))
    CRYPTO_WINNER_CONFIRMED_MAX_CONFIDENCE: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_MAX_CONFIDENCE", "80"))
    CRYPTO_WINNER_CONFIRMED_MIN_NEURAL_PROB: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_MIN_NEURAL_PROB", "0.62"))
    CRYPTO_WINNER_CONFIRMED_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_WINNER_CONFIRMED_CTRADER_RISK_USD", "0.90"))

    # ── Crypto Behavioral Retest (CBR) — CHOCH_ENTRY + market-to-limit conversion ──
    CRYPTO_BEHAVIORAL_RETEST_ENABLED: bool = os.getenv("CRYPTO_BEHAVIORAL_RETEST_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SYMBOLS: str = os.getenv("CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SYMBOLS", "BTCUSD,ETHUSD")
    CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SESSIONS: str = os.getenv("CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SESSIONS", "london,new_york,overlap|new_york")
    CRYPTO_BEHAVIORAL_RETEST_ALLOWED_PATTERNS: str = os.getenv("CRYPTO_BEHAVIORAL_RETEST_ALLOWED_PATTERNS", "CHOCH_ENTRY")
    CRYPTO_BEHAVIORAL_RETEST_MIN_CONFIDENCE: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_MIN_CONFIDENCE", "72"))
    CRYPTO_BEHAVIORAL_RETEST_MAX_CONFIDENCE: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_MAX_CONFIDENCE", "82"))
    CRYPTO_BEHAVIORAL_RETEST_MIN_NEURAL_PROB: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_MIN_NEURAL_PROB", "0.65"))
    CRYPTO_BEHAVIORAL_RETEST_BLOCK_SEVERE_WINNER: bool = os.getenv("CRYPTO_BEHAVIORAL_RETEST_BLOCK_SEVERE_WINNER", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_BEHAVIORAL_RETEST_PULLBACK_RISK_RATIO: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_PULLBACK_RISK_RATIO", "0.15"))
    CRYPTO_BEHAVIORAL_RETEST_BTC_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_BTC_CTRADER_RISK_USD", "0.45"))
    CRYPTO_BEHAVIORAL_RETEST_ETH_CTRADER_RISK_USD: float = float(os.getenv("CRYPTO_BEHAVIORAL_RETEST_ETH_CTRADER_RISK_USD", "0.20"))

    CTRADER_XAU_ACTIVE_FAMILIES: str = os.getenv("CTRADER_XAU_ACTIVE_FAMILIES", "xau_scalp_pullback_limit,xau_scalp_breakout_stop")
    DEXTER_MEMPALACE_FAMILY_LANE_ENABLED: bool = os.getenv("DEXTER_MEMPALACE_FAMILY_LANE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    DEXTER_MEMPALACE_FAMILY_NAME: str = os.getenv("DEXTER_MEMPALACE_FAMILY_NAME", "xau_scalp_mempalace_lane")
    DEXTER_MEMPALACE_SOURCE_TOKENS: str = os.getenv("DEXTER_MEMPALACE_SOURCE_TOKENS", "mempalace,mempalac")
    CTRADER_XAU_PRIMARY_FAMILY: str = os.getenv("CTRADER_XAU_PRIMARY_FAMILY", "")
    TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_SWARM_ACTIVE_FAMILIES: str = os.getenv(
        "TRADING_MANAGER_XAU_SWARM_ACTIVE_FAMILIES",
        "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar,xau_scalp_flow_long_sidecar,xau_scalp_failed_fade_follow_stop,xau_scalp_range_repair",
    )
    XAU_RANGE_REPAIR_ENABLED: bool = os.getenv("XAU_RANGE_REPAIR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_RANGE_REPAIR_MIN_CONFIDENCE: float = float(os.getenv("XAU_RANGE_REPAIR_MIN_CONFIDENCE", "0"))
    XAU_RANGE_REPAIR_LOOKBACK_SEC: int = int(os.getenv("XAU_RANGE_REPAIR_LOOKBACK_SEC", "300"))
    XAU_RANGE_REPAIR_ALLOWED_STATES: str = os.getenv("XAU_RANGE_REPAIR_ALLOWED_STATES", "range_probe")
    XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES: str = os.getenv("XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES", "fast_expansion,panic_spread")
    XAU_RANGE_REPAIR_MAX_CONTINUATION_BIAS: float = float(os.getenv("XAU_RANGE_REPAIR_MAX_CONTINUATION_BIAS", "0.09"))
    XAU_RANGE_REPAIR_MIN_REJECTION_RATIO: float = float(os.getenv("XAU_RANGE_REPAIR_MIN_REJECTION_RATIO", "0.16"))
    XAU_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", "0.18"))
    XAU_RANGE_REPAIR_MAX_ABS_DELTA_PROXY: float = float(os.getenv("XAU_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", "0.11"))
    XAU_RANGE_REPAIR_MAX_ABS_DEPTH_IMBALANCE: float = float(os.getenv("XAU_RANGE_REPAIR_MAX_ABS_DEPTH_IMBALANCE", "0.10"))
    XAU_RANGE_REPAIR_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_RANGE_REPAIR_MAX_SPREAD_EXPANSION", "1.10"))
    XAU_RANGE_REPAIR_MAX_SPREAD_AVG_PCT: float = float(os.getenv("XAU_RANGE_REPAIR_MAX_SPREAD_AVG_PCT", "0.0022"))
    XAU_RANGE_REPAIR_ENTRY_RISK_RATIO: float = float(os.getenv("XAU_RANGE_REPAIR_ENTRY_RISK_RATIO", "0.10"))
    XAU_RANGE_REPAIR_ENTRY_ATR_RATIO: float = float(os.getenv("XAU_RANGE_REPAIR_ENTRY_ATR_RATIO", "0.045"))
    XAU_RANGE_REPAIR_STOP_KEEP_RISK_RATIO: float = float(os.getenv("XAU_RANGE_REPAIR_STOP_KEEP_RISK_RATIO", "0.72"))
    XAU_RANGE_REPAIR_TP1_RR: float = float(os.getenv("XAU_RANGE_REPAIR_TP1_RR", "0.50"))
    XAU_RANGE_REPAIR_TP2_RR: float = float(os.getenv("XAU_RANGE_REPAIR_TP2_RR", "0.82"))
    XAU_RANGE_REPAIR_TP3_RR: float = float(os.getenv("XAU_RANGE_REPAIR_TP3_RR", "1.10"))
    XAU_RANGE_REPAIR_CTRADER_RISK_USD: float = float(os.getenv("XAU_RANGE_REPAIR_CTRADER_RISK_USD", "0.35"))
    XAU_PULLBACK_LIMIT_ENTRY_ATR: float = float(os.getenv("XAU_PULLBACK_LIMIT_ENTRY_ATR", "0.12"))
    XAU_PULLBACK_LIMIT_STOP_PAD_ATR: float = float(os.getenv("XAU_PULLBACK_LIMIT_STOP_PAD_ATR", "0.04"))
    XAU_PB_NARROW_CONTEXT_ENABLED: bool = os.getenv("XAU_PB_NARROW_CONTEXT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_PB_NARROW_CONTEXT_MIN_RESOLVED: int = int(os.getenv("XAU_PB_NARROW_CONTEXT_MIN_RESOLVED", "3"))
    XAU_PB_NARROW_CONTEXT_MIN_MEMORY_SCORE: float = float(os.getenv("XAU_PB_NARROW_CONTEXT_MIN_MEMORY_SCORE", "20"))
    XAU_PB_NARROW_CONTEXT_MAX_ROWS: int = int(os.getenv("XAU_PB_NARROW_CONTEXT_MAX_ROWS", "8"))
    XAU_PB_NARROW_CONTEXT_ALLOW_ADJACENT_CONFIDENCE: bool = os.getenv("XAU_PB_NARROW_CONTEXT_ALLOW_ADJACENT_CONFIDENCE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_PB_NARROW_CONTEXT_RELAXED_MIN_MEMORY_SCORE: float = float(os.getenv("XAU_PB_NARROW_CONTEXT_RELAXED_MIN_MEMORY_SCORE", "28"))
    XAU_PB_FALLING_KNIFE_BLOCK_ENABLED: bool = os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_PB_FALLING_KNIFE_BLOCK_DAY_TYPES: str = os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_DAY_TYPES", "repricing,fast_expansion,panic_spread")
    XAU_PB_FALLING_KNIFE_BLOCK_STATE_LABELS: str = os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_STATE_LABELS", "failed_fade_risk,panic_dislocation")
    XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_DELTA_PROXY: float = float(os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_DELTA_PROXY", "0.09"))
    XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_REFILL_SHIFT: float = float(os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_REFILL_SHIFT", "0.04"))
    XAU_PB_FALLING_KNIFE_BLOCK_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_MIN_BAR_VOLUME_PROXY", "0.42"))
    XAU_PB_FALLING_KNIFE_BLOCK_MAX_REJECTION_RATIO: float = float(os.getenv("XAU_PB_FALLING_KNIFE_BLOCK_MAX_REJECTION_RATIO", "0.18"))
    XAU_PB_CAPTURE_MICRO_RELAX_ENABLED: bool = os.getenv("XAU_PB_CAPTURE_MICRO_RELAX_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_PB_CAPTURE_MICRO_RELAX_ALLOWED_REASONS: str = os.getenv("XAU_PB_CAPTURE_MICRO_RELAX_ALLOWED_REASONS", "long_imbalance_not_supportive,long_refill_not_supportive,long_delta_not_supportive")
    XAU_PB_CAPTURE_MICRO_RELAX_RISK_MULT: float = float(os.getenv("XAU_PB_CAPTURE_MICRO_RELAX_RISK_MULT", "0.88"))
    XAU_OPENAPI_ENTRY_ROUTER_ENABLED: bool = os.getenv("XAU_OPENAPI_ENTRY_ROUTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_DAY_TYPES: str = os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_DAY_TYPES", "panic_spread")
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_STATE_LABELS: str = os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_STATE_LABELS", "failed_fade_risk,panic_dislocation")
    XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_PCT: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_PCT", "0.0023"))
    XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_EXPANSION", "1.13"))
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_DELTA_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_DELTA_PROXY", "0.09"))
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_REFILL_SHIFT: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_REFILL_SHIFT", "0.05"))
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_BAR_VOLUME_PROXY", "0.40"))
    XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MAX_REJECTION_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MAX_REJECTION_RATIO", "0.18"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_STATE_LABELS: str = os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_STATE_LABELS", "continuation_drive,breakout_drive")
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_SCORE: int = int(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_SCORE", "5"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_DELTA_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_DELTA_PROXY", "0.08"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_IMBALANCE: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_IMBALANCE", "0.025"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_REFILL_SHIFT: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_REFILL_SHIFT", "0.02"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MAX_REJECTION_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MAX_REJECTION_RATIO", "0.24"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_BAR_VOLUME_PROXY", "0.40"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STATE_LABELS: str = os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STATE_LABELS", "pullback_absorption,repricing_transition,reversal_exhaustion")
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_SCORE: int = int(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_SCORE", "4"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_REJECTION_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_REJECTION_RATIO", "0.26"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_ALIGNED_DELTA_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_ALIGNED_DELTA_PROXY", "0.07"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_ALIGNED_REFILL_SHIFT: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_ALIGNED_REFILL_SHIFT", "-0.02"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_BAR_VOLUME_PROXY", "0.20"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_SPREAD_EXPANSION", "1.08"))
    XAU_OPENAPI_ENTRY_ROUTER_SHALLOW_LIMIT_SCALE: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_SHALLOW_LIMIT_SCALE", "0.78"))
    XAU_OPENAPI_ENTRY_ROUTER_DEEP_LIMIT_SCALE: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_DEEP_LIMIT_SCALE", "1.18"))
    XAU_OPENAPI_ENTRY_ROUTER_FAST_STOP_TRIGGER_SCALE: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_FAST_STOP_TRIGGER_SCALE", "0.82"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RISK_MULTIPLIER: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RISK_MULTIPLIER", "0.88"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_TRIGGER_RISK_RATIO", "0.10"))
    XAU_OPENAPI_ENTRY_ROUTER_STOP_STOP_LIFT_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_STOP_STOP_LIFT_RATIO", "0.32"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RETEST_RISK_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RETEST_RISK_RATIO", "0.08"))
    XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STOP_PAD_RATIO: float = float(os.getenv("XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STOP_PAD_RATIO", "0.24"))
    XAU_MULTI_TF_ENTRY_GUARD_ENABLED: bool = os.getenv("XAU_MULTI_TF_ENTRY_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MULTI_TF_ENTRY_GUARD_FAMILIES: str = os.getenv(
        "XAU_MULTI_TF_ENTRY_GUARD_FAMILIES",
        "xau_scalp_tick_depth_filter,xau_scalp_flow_short_sidecar,xau_scalp_microtrend_follow_up,xau_scalp_pullback_limit,xau_scalp_breakout_stop,xau_scalp_range_repair",
    )
    XAU_MULTI_TF_ENTRY_GUARD_REQUIRE_H1_H4_ALIGN: bool = os.getenv("XAU_MULTI_TF_ENTRY_GUARD_REQUIRE_H1_H4_ALIGN", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MULTI_TF_ENTRY_GUARD_ALLOW_COUNTERTREND_CONFIRMED: bool = os.getenv("XAU_MULTI_TF_ENTRY_GUARD_ALLOW_COUNTERTREND_CONFIRMED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_CONF_FILTER_ENABLED: bool = os.getenv("SCALP_XAU_DIRECT_CONF_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_STRICT_ENABLED: bool = os.getenv("SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN: bool = os.getenv("SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_ALLOW_PARTIAL_ALIGN: bool = os.getenv("SCALP_XAU_DIRECT_MTF_ALLOW_PARTIAL_ALIGN", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONF: float = float(os.getenv("SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONF", "70.0"))
    SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR: bool = os.getenv("SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_NEUTRAL_OPEN_BUFFER_PCT: float = float(os.getenv("SCALP_XAU_DIRECT_MTF_NEUTRAL_OPEN_BUFFER_PCT", "0.00015"))
    SCALP_XAU_DIRECT_MTF_PARTIAL_FLOW_CONFIRM_ENABLED: bool = os.getenv("SCALP_XAU_DIRECT_MTF_PARTIAL_FLOW_CONFIRM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONTINUATION_BIAS: float = float(os.getenv("SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONTINUATION_BIAS", "0.10"))
    SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_DELTA_PROXY: float = float(os.getenv("SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_DELTA_PROXY", "0.08"))
    SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_BAR_VOLUME_PROXY", "0.38"))
    SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED: bool = os.getenv("SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED: bool = os.getenv("SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", "0").strip().lower() in ("1", "true", "yes", "on")
    ENTRY_TEMPLATE_CATALOG_ENABLED: bool = os.getenv("ENTRY_TEMPLATE_CATALOG_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    ENTRY_TEMPLATE_CATALOG_PATH: str = os.getenv("ENTRY_TEMPLATE_CATALOG_PATH", "").strip()
    # Optional pre-neural confidence tailwind from mined impulse capture stats (0 = off).
    ENTRY_TEMPLATE_CONF_TAILWIND_MAX: float = float(os.getenv("ENTRY_TEMPLATE_CONF_TAILWIND_MAX", "0"))
    ENTRY_TEMPLATE_CONF_TAILWIND_MIN_CAPTURE: float = float(os.getenv("ENTRY_TEMPLATE_CONF_TAILWIND_MIN_CAPTURE", "0.52"))
    ENTRY_TEMPLATE_CONF_TAILWIND_MIN_IMPULSES: int = int(os.getenv("ENTRY_TEMPLATE_CONF_TAILWIND_MIN_IMPULSES", "30"))
    ENTRY_TEMPLATE_SCANNER_BIAS_ENABLED: bool = os.getenv("ENTRY_TEMPLATE_SCANNER_BIAS_ENABLED", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    ENTRY_TEMPLATE_SCANNER_MAX_SHIFT_RISK_RATIO: float = float(os.getenv("ENTRY_TEMPLATE_SCANNER_MAX_SHIFT_RISK_RATIO", "0.22"))
    ENTRY_TEMPLATE_SCANNER_MIN_OFFSET_RISK_TO_ACT: float = float(os.getenv("ENTRY_TEMPLATE_SCANNER_MIN_OFFSET_RISK_TO_ACT", "0.04"))
    XAU_MICROTREND_FOLLOW_UP_ENABLED: bool = os.getenv("XAU_MICROTREND_FOLLOW_UP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MICROTREND_FOLLOW_UP_MIN_RESOLVED: int = int(os.getenv("XAU_MICROTREND_FOLLOW_UP_MIN_RESOLVED", "3"))
    XAU_MICROTREND_FOLLOW_UP_MIN_STATE_SCORE: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_MIN_STATE_SCORE", "18"))
    XAU_MICROTREND_FOLLOW_UP_MAX_ROWS: int = int(os.getenv("XAU_MICROTREND_FOLLOW_UP_MAX_ROWS", "6"))
    XAU_MICROTREND_FOLLOW_UP_ALLOW_ADJACENT_CONFIDENCE: bool = os.getenv("XAU_MICROTREND_FOLLOW_UP_ALLOW_ADJACENT_CONFIDENCE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MICROTREND_FOLLOW_UP_ALLOW_H1_RELAXED: bool = os.getenv("XAU_MICROTREND_FOLLOW_UP_ALLOW_H1_RELAXED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MICROTREND_FOLLOW_UP_ALLOW_COMPATIBLE_DAY_TYPE: bool = os.getenv("XAU_MICROTREND_FOLLOW_UP_ALLOW_COMPATIBLE_DAY_TYPE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MICROTREND_FOLLOW_UP_RELAXED_MIN_STATE_SCORE: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_RELAXED_MIN_STATE_SCORE", "28"))
    XAU_MICROTREND_FOLLOW_UP_RELAXED_RISK_MULTIPLIER: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_RELAXED_RISK_MULTIPLIER", "0.85"))
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MODE_ENABLED: bool = os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MODE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_RESOLVED: int = int(os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_RESOLVED", "3"))
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_STATE_SCORE: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_STATE_SCORE", "32"))
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_ALLOWED_STATES: str = os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition")
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MAX_RELAXED_BLOCKERS: int = int(os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MAX_RELAXED_BLOCKERS", "2"))
    XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_RISK_MULTIPLIER", "0.70"))
    XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS", "0.12"))
    XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY", "0.10"))
    XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY", "0.42"))
    XAU_MICROTREND_FOLLOW_UP_SHALLOW_RETEST_RISK_RATIO: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_SHALLOW_RETEST_RISK_RATIO", "0.12"))
    XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_TRIGGER_RISK_RATIO", "0.14"))
    XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_STOP_LIFT_RATIO: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_STOP_LIFT_RATIO", "0.38"))
    XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD: float = float(os.getenv("XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD", "0.65"))
    XAU_FLOW_SHORT_SIDECAR_ENABLED: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_MIN_RESOLVED: int = int(os.getenv("XAU_FLOW_SHORT_SIDECAR_MIN_RESOLVED", "3"))
    XAU_FLOW_SHORT_SIDECAR_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_MIN_STATE_SCORE", "20"))
    XAU_FLOW_SHORT_SIDECAR_MAX_ROWS: int = int(os.getenv("XAU_FLOW_SHORT_SIDECAR_MAX_ROWS", "6"))
    XAU_FLOW_SHORT_SIDECAR_ALLOWED_SESSIONS: str = os.getenv("XAU_FLOW_SHORT_SIDECAR_ALLOWED_SESSIONS", "new_york,london,new_york,overlap")
    XAU_FLOW_SHORT_SIDECAR_ALLOWED_PATTERNS: str = os.getenv("XAU_FLOW_SHORT_SIDECAR_ALLOWED_PATTERNS", "SCALP_FLOW_FORCE")
    XAU_FLOW_SHORT_SIDECAR_ALLOW_ADJACENT_CONFIDENCE: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_ALLOW_ADJACENT_CONFIDENCE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", "0.10"))
    XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", "0.08"))
    XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", "0.38"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE", "72"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE", "32"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT", "0.75"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_DELTA_PROXY_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_DELTA_PROXY_MULT", "0.75"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT", "0.90"))
    XAU_FLOW_SHORT_SIDECAR_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SAMPLE_RISK_MULTIPLIER", "0.70"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MODE_ENABLED: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", "69"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", "34"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES: str = os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition")
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE: bool = os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT", "0.68"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT", "0.68"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT", "0.82"))
    XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER", "0.55"))
    XAU_FLOW_SHORT_SIDECAR_SHALLOW_RETEST_RISK_RATIO: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_SHALLOW_RETEST_RISK_RATIO", "0.10"))
    XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO", "0.12"))
    XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO", "0.34"))
    XAU_FLOW_SHORT_SIDECAR_CTRADER_RISK_USD: float = float(os.getenv("XAU_FLOW_SHORT_SIDECAR_CTRADER_RISK_USD", "0.45"))
    XAU_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD: float = float(os.getenv("XAU_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD", "0.85"))
    XAU_FLOW_LONG_SIDECAR_ENABLED: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_MIN_RESOLVED: int = int(os.getenv("XAU_FLOW_LONG_SIDECAR_MIN_RESOLVED", "3"))
    XAU_FLOW_LONG_SIDECAR_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_MIN_STATE_SCORE", "20"))
    XAU_FLOW_LONG_SIDECAR_MAX_ROWS: int = int(os.getenv("XAU_FLOW_LONG_SIDECAR_MAX_ROWS", "6"))
    XAU_FLOW_LONG_SIDECAR_ALLOWED_SESSIONS: str = os.getenv("XAU_FLOW_LONG_SIDECAR_ALLOWED_SESSIONS", "new_york|london|overlap")
    XAU_FLOW_LONG_SIDECAR_ALLOWED_PATTERNS: str = os.getenv("XAU_FLOW_LONG_SIDECAR_ALLOWED_PATTERNS", "SCALP_FLOW_FORCE")
    XAU_FLOW_LONG_SIDECAR_ALLOW_ADJACENT_CONFIDENCE: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_ALLOW_ADJACENT_CONFIDENCE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_FORCE_STOP_ONLY: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_FORCE_STOP_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", "0.10"))
    XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", "0.08"))
    XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", "0.38"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_ENABLED: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_CONFIDENCE", "72"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_STATE_SCORE", "32"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT", "0.75"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_DELTA_PROXY_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_DELTA_PROXY_MULT", "0.75"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT", "0.90"))
    XAU_FLOW_LONG_SIDECAR_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SAMPLE_RISK_MULTIPLIER", "0.70"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MODE_ENABLED: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", "69"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", "34"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES: str = os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition")
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE: bool = os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT", "0.68"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT", "0.68"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT", "0.82"))
    XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER", "0.55"))
    XAU_FLOW_LONG_SIDECAR_SHALLOW_RETEST_RISK_RATIO: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_SHALLOW_RETEST_RISK_RATIO", "0.10"))
    XAU_FLOW_LONG_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO", "0.12"))
    XAU_FLOW_LONG_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO", "0.34"))
    XAU_FLOW_LONG_SIDECAR_CTRADER_RISK_USD: float = float(os.getenv("XAU_FLOW_LONG_SIDECAR_CTRADER_RISK_USD", "0.45"))
    XAU_BREAKOUT_STOP_TRIGGER_ATR: float = float(os.getenv("XAU_BREAKOUT_STOP_TRIGGER_ATR", "0.10"))
    XAU_BREAKOUT_STOP_STOP_LIFT_RATIO: float = float(os.getenv("XAU_BREAKOUT_STOP_STOP_LIFT_RATIO", "0.45"))
    XAU_TICK_DEPTH_FILTER_ENABLED: bool = os.getenv("XAU_TICK_DEPTH_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", "240"))
    XAU_TICK_DEPTH_FILTER_MIN_SPOTS: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_MIN_SPOTS", "6"))
    XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", "40"))
    XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", "0.0022"))
    XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", "1.12"))
    XAU_TICK_DEPTH_FILTER_LONG_MAX_IMBALANCE: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_LONG_MAX_IMBALANCE", "-0.01"))
    XAU_TICK_DEPTH_FILTER_LONG_MAX_REFILL_SHIFT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_LONG_MAX_REFILL_SHIFT", "-0.03"))
    XAU_TICK_DEPTH_FILTER_LONG_MAX_DELTA_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_LONG_MAX_DELTA_PROXY", "-0.01"))
    XAU_TICK_DEPTH_FILTER_SHORT_MIN_IMBALANCE: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_SHORT_MIN_IMBALANCE", "0.005"))
    XAU_TICK_DEPTH_FILTER_SHORT_MAX_REJECTION: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_SHORT_MAX_REJECTION", "0.20"))
    XAU_TICK_DEPTH_FILTER_SHORT_MIN_DELTA_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_SHORT_MIN_DELTA_PROXY", "0.01"))
    XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", "0.35"))
    XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", "3"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_EXPANSION: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_EXPANSION", "1.16"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_PCT: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_PCT", "0.0025"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DRIFT_PCT: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DRIFT_PCT", "0.008"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DELTA_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DELTA_PROXY", "0.14"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_MAX_REJECTION: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_MAX_REJECTION", "0.18"))
    XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_BAR_VOLUME_PROXY", "0.55"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_SPREAD_EXPANSION: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_SPREAD_EXPANSION", "1.12"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MAX_SPREAD_PCT: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MAX_SPREAD_PCT", "0.0022"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DRIFT_PCT: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DRIFT_PCT", "0.0025"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DELTA_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DELTA_PROXY", "0.10"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_BAR_VOLUME_PROXY", "0.45"))
    XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_REJECTION: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_REJECTION", "0.20"))
    XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DRIFT_PCT: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DRIFT_PCT", "0.010"))
    XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DELTA_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DELTA_PROXY", "0.08"))
    XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_BAR_VOLUME_PROXY", "0.40"))
    XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_REJECTION: float = float(os.getenv("XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_REJECTION", "0.10"))
    XAU_TICK_DEPTH_FILTER_PANIC_SPREAD_BLOCK: bool = os.getenv("XAU_TICK_DEPTH_FILTER_PANIC_SPREAD_BLOCK", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED: bool = os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA", "1"))
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE", "73"))
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY", "0.45"))
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION", "1.05"))
    XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_RISK_MULTIPLIER: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_RISK_MULTIPLIER", "0.70"))
    XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_SCORE_BONUS: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_SCORE_BONUS", "1"))
    XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", "-1.0"))
    XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MAX_SPREAD_EXPANSION_MULT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MAX_SPREAD_EXPANSION_MULT", "1.05"))
    XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_BAR_VOLUME_MULT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_BAR_VOLUME_MULT", "0.95"))
    XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_SCORE_BONUS: int = int(os.getenv("XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_SCORE_BONUS", "1"))
    XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_CONFIDENCE_DELTA: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_CONFIDENCE_DELTA", "-0.5"))
    XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MAX_SPREAD_EXPANSION_MULT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MAX_SPREAD_EXPANSION_MULT", "1.35"))
    XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_BAR_VOLUME_MULT: float = float(os.getenv("XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_BAR_VOLUME_MULT", "1.0"))
    # ── Entry Sharpness Score (deep data analytics) ──────────────────────
    XAU_ENTRY_SHARPNESS_ENABLED: bool = os.getenv("XAU_ENTRY_SHARPNESS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_ENTRY_SHARPNESS_KNIFE_THRESHOLD: int = int(os.getenv("XAU_ENTRY_SHARPNESS_KNIFE_THRESHOLD", "30"))
    XAU_ENTRY_SHARPNESS_CAUTION_THRESHOLD: int = int(os.getenv("XAU_ENTRY_SHARPNESS_CAUTION_THRESHOLD", "50"))
    XAU_ENTRY_SHARPNESS_SHARP_THRESHOLD: int = int(os.getenv("XAU_ENTRY_SHARPNESS_SHARP_THRESHOLD", "70"))
    XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD: int = int(os.getenv("XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD", "35"))
    XAU_ENTRY_SHARPNESS_RR_KNIFE_THRESHOLD: int = int(os.getenv("XAU_ENTRY_SHARPNESS_RR_KNIFE_THRESHOLD", "30"))
    XAU_ENTRY_SHARPNESS_CAUTION_RISK_MULT: float = float(os.getenv("XAU_ENTRY_SHARPNESS_CAUTION_RISK_MULT", "0.75"))
    XAU_ENTRY_SHARPNESS_SHARP_PROMOTE_MIN_CONT_SCORE: int = int(os.getenv("XAU_ENTRY_SHARPNESS_SHARP_PROMOTE_MIN_CONT_SCORE", "4"))
    XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE: float = float(os.getenv("XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE", "0.025"))
    XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION: float = float(os.getenv("XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION", "1.20"))
    XAU_ENTRY_SHARPNESS_W_MOMENTUM: float = float(os.getenv("XAU_ENTRY_SHARPNESS_W_MOMENTUM", "1.0"))
    XAU_ENTRY_SHARPNESS_W_FLOW: float = float(os.getenv("XAU_ENTRY_SHARPNESS_W_FLOW", "1.0"))
    XAU_ENTRY_SHARPNESS_W_ABSORPTION: float = float(os.getenv("XAU_ENTRY_SHARPNESS_W_ABSORPTION", "1.0"))
    XAU_ENTRY_SHARPNESS_W_STABILITY: float = float(os.getenv("XAU_ENTRY_SHARPNESS_W_STABILITY", "1.0"))
    XAU_ENTRY_SHARPNESS_W_POSITIONING: float = float(os.getenv("XAU_ENTRY_SHARPNESS_W_POSITIONING", "1.0"))
    # ── Sharpness Feedback Loop (self-improving) ────────────────────────────
    XAU_SHARPNESS_FEEDBACK_ENABLED: bool = os.getenv("XAU_SHARPNESS_FEEDBACK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_SHARPNESS_FEEDBACK_INTERVAL_MIN: int = int(os.getenv("XAU_SHARPNESS_FEEDBACK_INTERVAL_MIN", "120"))
    XAU_SHARPNESS_FEEDBACK_ON_START: bool = os.getenv("XAU_SHARPNESS_FEEDBACK_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_SHARPNESS_FEEDBACK_LOOKBACK_DAYS: int = int(os.getenv("XAU_SHARPNESS_FEEDBACK_LOOKBACK_DAYS", "14"))
    XAU_SHARPNESS_FEEDBACK_NOTIFY_TELEGRAM: bool = os.getenv("XAU_SHARPNESS_FEEDBACK_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_SHARPNESS_FEEDBACK_MIN_TRADES: int = int(os.getenv("XAU_SHARPNESS_FEEDBACK_MIN_TRADES", "10"))
    SELF_IMPROVING_SYMBOLS: str = os.getenv("SELF_IMPROVING_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD")
    # ── Sharpness Auto-Calibration ──────────────────────────────────────────
    XAU_SHARPNESS_AUTO_CALIBRATE_ENABLED: bool = os.getenv("XAU_SHARPNESS_AUTO_CALIBRATE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    XAU_SHARPNESS_AUTO_CALIBRATE_MAX_STEP: float = float(os.getenv("XAU_SHARPNESS_AUTO_CALIBRATE_MAX_STEP", "0.15"))
    XAU_SHARPNESS_AUTO_CALIBRATE_MIN_WEIGHT: float = float(os.getenv("XAU_SHARPNESS_AUTO_CALIBRATE_MIN_WEIGHT", "0.5"))
    XAU_SHARPNESS_AUTO_CALIBRATE_MAX_WEIGHT: float = float(os.getenv("XAU_SHARPNESS_AUTO_CALIBRATE_MAX_WEIGHT", "2.0"))
    # ── Family Performance Decay Detector ───────────────────────────────────
    XAU_FAMILY_DECAY_ENABLED: bool = os.getenv("XAU_FAMILY_DECAY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_FAMILY_DECAY_RECENT_TRADES: int = int(os.getenv("XAU_FAMILY_DECAY_RECENT_TRADES", "20"))
    XAU_FAMILY_DECAY_BASELINE_TRADES: int = int(os.getenv("XAU_FAMILY_DECAY_BASELINE_TRADES", "60"))
    XAU_FAMILY_DECAY_THRESHOLD: float = float(os.getenv("XAU_FAMILY_DECAY_THRESHOLD", "0.15"))
    # ── Volume Profile ──────────────────────────────────────────────────────
    XAU_VOLUME_PROFILE_ENABLED: bool = os.getenv("XAU_VOLUME_PROFILE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_VOLUME_PROFILE_HOURS_BACK: int = int(os.getenv("XAU_VOLUME_PROFILE_HOURS_BACK", "24"))
    XAU_VOLUME_PROFILE_BUCKET_TICKS: int = int(os.getenv("XAU_VOLUME_PROFILE_BUCKET_TICKS", "10"))
    XAU_VOLUME_PROFILE_VA_PCT: float = float(os.getenv("XAU_VOLUME_PROFILE_VA_PCT", "0.70"))
    XAU_VOLUME_PROFILE_INTERVAL_MIN: int = int(os.getenv("XAU_VOLUME_PROFILE_INTERVAL_MIN", "30"))
    # ── DOM Liquidity Shift Detector ────────────────────────────────────────
    XAU_DOM_LIQUIDITY_SHIFT_ENABLED: bool = os.getenv("XAU_DOM_LIQUIDITY_SHIFT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN: int = int(os.getenv("XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", "30"))
    XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS: int = int(os.getenv("XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", "6"))
    # Crypto DOM defense anti-MM-trap safeguards
    CRYPTO_DOM_DEFENSE_PROFIT_BUFFER_R: float = float(os.getenv("CRYPTO_DOM_DEFENSE_PROFIT_BUFFER_R", "0.50"))
    CRYPTO_DOM_TP_MAX_EXTENSION_R: float = float(os.getenv("CRYPTO_DOM_TP_MAX_EXTENSION_R", "3.0"))
    # ── Strategy Evolution Log ──────────────────────────────────────────────
    STRATEGY_EVOLUTION_ENABLED: bool = os.getenv("STRATEGY_EVOLUTION_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_EVOLUTION_NOTIFY_TELEGRAM: bool = os.getenv("STRATEGY_EVOLUTION_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS: int = int(os.getenv("CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", "18"))
    XAU_TD_VS_PB_COMPARE_MIN_RESOLVED: int = int(os.getenv("XAU_TD_VS_PB_COMPARE_MIN_RESOLVED", "4"))
    CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE: bool = os.getenv("CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE_MAX_TP_DEVIATION_PCT: float = float(os.getenv("CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE_MAX_TP_DEVIATION_PCT", "0.35"))
    CTRADER_POSITION_MANAGER_ENABLED: bool = os.getenv("CTRADER_POSITION_MANAGER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_CLOSE_AT_PLANNED_TARGET: bool = os.getenv("CTRADER_PM_CLOSE_AT_PLANNED_TARGET", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_INVALID_TP_CLOSE_R: float = float(os.getenv("CTRADER_PM_INVALID_TP_CLOSE_R", "0.25"))
    CTRADER_PM_INVALID_TP_REPAIR_R: float = float(os.getenv("CTRADER_PM_INVALID_TP_REPAIR_R", "0.60"))
    CTRADER_PM_INVALID_TP_BE_TRIGGER_R: float = float(os.getenv("CTRADER_PM_INVALID_TP_BE_TRIGGER_R", "0.12"))
    CTRADER_PM_INVALID_TP_BE_LOCK_R: float = float(os.getenv("CTRADER_PM_INVALID_TP_BE_LOCK_R", "0.02"))
    CTRADER_PM_BREAKOUT_REPAIR_TP_R: float = float(os.getenv("CTRADER_PM_BREAKOUT_REPAIR_TP_R", "0.55"))
    CTRADER_PM_PULLBACK_REPAIR_TP_R: float = float(os.getenv("CTRADER_PM_PULLBACK_REPAIR_TP_R", "0.75"))
    CTRADER_PM_SCHEDULED_CANARY_BE_TRIGGER_R: float = float(os.getenv("CTRADER_PM_SCHEDULED_CANARY_BE_TRIGGER_R", "0.22"))
    CTRADER_PM_SCHEDULED_CANARY_BE_LOCK_R: float = float(os.getenv("CTRADER_PM_SCHEDULED_CANARY_BE_LOCK_R", "0.03"))
    CTRADER_PM_CANARY_FAMILY_BE_TRIGGER_R: float = float(os.getenv("CTRADER_PM_CANARY_FAMILY_BE_TRIGGER_R", "0.80"))
    CTRADER_PM_CANARY_FAMILY_BE_LOCK_R: float = float(os.getenv("CTRADER_PM_CANARY_FAMILY_BE_LOCK_R", "0.05"))
    CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_AGE_MIN: int = int(os.getenv("CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_AGE_MIN", "18"))
    CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_R: float = float(os.getenv("CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_R", "0.08"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED: bool = os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_ACTIVE_DEFENSE_ALLOWED_SOURCES: str = os.getenv(
        "CTRADER_PM_XAU_ACTIVE_DEFENSE_ALLOWED_SOURCES",
        "xauusd_scheduled:canary,scalp_xauusd:winner,scalp_xauusd:pb:canary,scalp_xauusd:td:canary,scalp_xauusd:ff:canary,scalp_xauusd:mfu:canary,scalp_xauusd:fss:canary,scalp_xauusd:fls:canary,scalp_xauusd:rr:canary,xauusd_scheduled:winner",
    )
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_AGE_MIN: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_AGE_MIN", "2.0"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_BAR_VOLUME_PROXY", "0.32"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DELTA: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DELTA", "0.10"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_IMBALANCE: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_IMBALANCE", "0.08"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DRIFT_PCT: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DRIFT_PCT", "0.010"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_MAX_REJECTION: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_MAX_REJECTION", "0.20"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_SCORE: int = int(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_SCORE", "3"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_SCORE: int = int(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_SCORE", "5"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_MAX_R: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_MAX_R", "0.20"))
    # When underwater by this many R multiples and microstructure score is bad, cut before full SL (soft stop).
    CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_ENABLED: bool = os.getenv(
        "CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_R: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_R", "-0.28"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_MIN_SCORE: int = int(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_MIN_SCORE", "3"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_STOP_KEEP_R: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_STOP_KEEP_R", "0.42"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_PROFIT_LOCK_R: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_PROFIT_LOCK_R", "0.05"))
    CTRADER_PM_XAU_ACTIVE_DEFENSE_TRIM_TP_R: float = float(os.getenv("CTRADER_PM_XAU_ACTIVE_DEFENSE_TRIM_TP_R", "0.55"))
    # Profit-seeking guard: if XAU is already working, protect it with SL instead of closing/trimming TP too early.
    CTRADER_PM_XAU_PROFIT_SEEKING_ENABLED: bool = os.getenv(
        "CTRADER_PM_XAU_PROFIT_SEEKING_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_PROFIT_SEEKING_MIN_R: float = float(os.getenv("CTRADER_PM_XAU_PROFIT_SEEKING_MIN_R", "0.15"))
    CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_R: float = float(os.getenv("CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_R", "0.08"))
    CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_BUFFER_R: float = float(os.getenv("CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_BUFFER_R", "0.03"))
    # Profit retrace guard: bank profit in weak/corrective phases, but keep impulse continuation alive.
    CTRADER_PM_IMPULSE_FAMILIES: str = os.getenv(
        "CTRADER_PM_IMPULSE_FAMILIES",
        "xau_scalp_breakout_stop,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scheduled_trend,xau_scalp_mempalace_lane",
    )
    CTRADER_PM_CORRECTIVE_FAMILIES: str = os.getenv(
        "CTRADER_PM_CORRECTIVE_FAMILIES",
        "xau_scalp_pullback_limit,xau_scalp_range_repair,xau_scalp_flow_short_sidecar,xau_scalp_tick_depth_filter",
    )
    CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED: bool = os.getenv(
        "CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN", "4.0"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R", "0.30"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R", "0.22"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY", "0.22"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT", "0.004"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_DELTA_PROXY: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_DELTA_PROXY", "0.12"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_BAR_VOLUME_PROXY", "0.30"))
    CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_LOCK_R: float = float(os.getenv("CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_LOCK_R", "0.08"))
    # Sweep-recovery detector: avoid premature close when move likely is liquidity sweep + continuation.
    CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED: bool = os.getenv(
        "CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO: float = float(
        os.getenv("CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO", "0.28")
    )
    CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY: float = float(
        os.getenv("CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY", "0.30")
    )
    CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY: float = float(
        os.getenv("CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY", "0.08")
    )
    CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE: float = float(
        os.getenv("CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE", "0.06")
    )
    CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R: float = float(
        os.getenv("CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R", "0.05")
    )
    CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN: float = float(os.getenv("CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", "0.15"))
    # When trading manager xau_order_care is inactive, still allow TP extension using config defaults (capture snapshot required).
    CTRADER_PM_XAU_EXTENSION_ALLOW_WITHOUT_ORDER_CARE: bool = os.getenv(
        "CTRADER_PM_XAU_EXTENSION_ALLOW_WITHOUT_ORDER_CARE", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_POST_FILL_STOP_CLAMP_ENABLED: bool = os.getenv("CTRADER_PM_XAU_POST_FILL_STOP_CLAMP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_XAU_POST_FILL_STOP_MAX_RISK_MULT: float = float(os.getenv("CTRADER_PM_XAU_POST_FILL_STOP_MAX_RISK_MULT", "1.15"))
    CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED: bool = os.getenv("CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN: int = int(os.getenv("CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN", "20"))
    CTRADER_XAU_SHORT_LIMIT_PAUSE_LOOKBACK_MIN: int = int(os.getenv("CTRADER_XAU_SHORT_LIMIT_PAUSE_LOOKBACK_MIN", "95"))
    CTRADER_XAU_SHORT_LIMIT_PAUSE_FAMILIES: str = os.getenv(
        "CTRADER_XAU_SHORT_LIMIT_PAUSE_FAMILIES",
        "xau_scalp_microtrend,xau_scalp_tick_depth_filter",
    )
    CTRADER_XAU_PAIR_RISK_CAP_ENABLED: bool = os.getenv("CTRADER_XAU_PAIR_RISK_CAP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_XAU_PAIR_RISK_CAP_FAMILIES: str = os.getenv(
        "CTRADER_XAU_PAIR_RISK_CAP_FAMILIES",
        "xau_scalp_microtrend,xau_scalp_tick_depth_filter",
    )
    CTRADER_XAU_PAIR_RISK_MAX_USD: float = float(os.getenv("CTRADER_XAU_PAIR_RISK_MAX_USD", "3.0"))
    CTRADER_XAU_PAIR_RISK_MIN_USD: float = float(os.getenv("CTRADER_XAU_PAIR_RISK_MIN_USD", "0.15"))
    CTRADER_XAU_PAIR_RISK_PRICE_TOLERANCE: float = float(os.getenv("CTRADER_XAU_PAIR_RISK_PRICE_TOLERANCE", "0.05"))
    CTRADER_POSITION_DIRECTION_GUARD_ENABLED: bool = os.getenv("CTRADER_POSITION_DIRECTION_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_MAX_POSITIONS_PER_SYMBOL: int = int(os.getenv("CTRADER_MAX_POSITIONS_PER_SYMBOL", "3"))
    CTRADER_MAX_POSITIONS_PER_DIRECTION: int = int(os.getenv("CTRADER_MAX_POSITIONS_PER_DIRECTION", "2"))
    CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL: int = int(os.getenv("CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", "1"))
    CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION: int = int(os.getenv("CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", "1"))
    CTRADER_BLOCK_OPPOSITE_DIRECTION: bool = os.getenv("CTRADER_BLOCK_OPPOSITE_DIRECTION", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS: bool = os.getenv("CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DIRECTION_GUARD_INCLUDE_RECENT_JOURNAL: bool = os.getenv("CTRADER_DIRECTION_GUARD_INCLUDE_RECENT_JOURNAL", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DIRECTION_GUARD_RECENT_SEC: int = int(os.getenv("CTRADER_DIRECTION_GUARD_RECENT_SEC", "900"))
    CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL: int = int(os.getenv("CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", "2"))
    CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION: int = int(os.getenv("CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", "1"))
    CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL: int = int(os.getenv("CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", "1"))
    CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION: int = int(os.getenv("CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", "1"))
    # Evidence governance: block bad source+direction lanes before worker execution.
    # Format: source:direction, where source may itself contain colons. Direction can be long, short, or *.
    CTRADER_SOURCE_DIRECTION_QUARANTINE_ENABLED: bool = os.getenv(
        "CTRADER_SOURCE_DIRECTION_QUARANTINE_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    CTRADER_QUARANTINED_SOURCE_DIRECTIONS: str = os.getenv(
        "CTRADER_QUARANTINED_SOURCE_DIRECTIONS",
        ",".join(
            [
                "fibo_xauusd:short",
                "scalp_xauusd:canary:long",
                "scalp_xauusd:canary:short",
                "scalp_xauusd:bs:canary:long",
                "scalp_xauusd:bs:canary:short",
                "scalp_xauusd:pb:canary:long",
                "scalp_xauusd:pb:canary:short",
                "scalp_xauusd:td:canary:long",
                "scalp_xauusd:td:canary:short",
                "scalp_xauusd:short",
            ]
        ),
    )
    CTRADER_PROTECTED_SOURCE_DIRECTIONS: str = os.getenv(
        "CTRADER_PROTECTED_SOURCE_DIRECTIONS",
        ",".join(
            [
                "xauusd_scheduled:canary",
                "xauusd_scheduled:winner",
                "scalp_xauusd:fss:canary",
                "scalp_btcusd:canary",
            ]
        ),
    )
    TRADING_TEAM_ENABLED: bool = os.getenv("TRADING_TEAM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_TEAM_XAU_PRIORITY_TOPK: int = int(os.getenv("TRADING_TEAM_XAU_PRIORITY_TOPK", "4"))
    TRADING_TEAM_XAU_REASON_SCORE_MULT: float = float(os.getenv("TRADING_TEAM_XAU_REASON_SCORE_MULT", "18"))
    TRADING_TEAM_XAU_LIVE_EDGE_ENABLED: bool = os.getenv("TRADING_TEAM_XAU_LIVE_EDGE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_TEAM_XAU_LIVE_EDGE_MIN_RESOLVED: int = int(os.getenv("TRADING_TEAM_XAU_LIVE_EDGE_MIN_RESOLVED", "2"))
    TRADING_TEAM_XAU_LIVE_EDGE_PNL_MULT: float = float(os.getenv("TRADING_TEAM_XAU_LIVE_EDGE_PNL_MULT", "0.55"))
    TRADING_TEAM_XAU_LIVE_EDGE_WIN_RATE_MULT: float = float(os.getenv("TRADING_TEAM_XAU_LIVE_EDGE_WIN_RATE_MULT", "12.0"))
    TRADING_TEAM_XAU_LIVE_EDGE_SCORE_CAP: float = float(os.getenv("TRADING_TEAM_XAU_LIVE_EDGE_SCORE_CAP", "12.0"))
    TRADING_TEAM_XAU_PRODUCTION_MAX_FAMILIES: int = int(os.getenv("TRADING_TEAM_XAU_PRODUCTION_MAX_FAMILIES", "2"))
    TRADING_TEAM_XAU_SAMPLING_MAX_FAMILIES: int = int(os.getenv("TRADING_TEAM_XAU_SAMPLING_MAX_FAMILIES", "2"))
    TRADING_TEAM_XAU_SAMPLING_PARALLEL_LIMIT: int = int(os.getenv("TRADING_TEAM_XAU_SAMPLING_PARALLEL_LIMIT", "1"))
    TRADING_TEAM_STRATEGY_LAB_PROMOTION_SCORE_MULT: float = float(os.getenv("TRADING_TEAM_STRATEGY_LAB_PROMOTION_SCORE_MULT", "0.35"))
    TRADING_TEAM_STRATEGY_LAB_LIVE_SHADOW_SCORE_MULT: float = float(os.getenv("TRADING_TEAM_STRATEGY_LAB_LIVE_SHADOW_SCORE_MULT", "0.15"))
    TRADING_TEAM_STRATEGY_LAB_RECOVERY_SCORE_MULT: float = float(os.getenv("TRADING_TEAM_STRATEGY_LAB_RECOVERY_SCORE_MULT", "0.10"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MIN_STATE_SCORE: float = float(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MIN_STATE_SCORE", "24"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION: int = int(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION", "3"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_TREND_MAX_SAME_DIRECTION: int = int(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_TREND_MAX_SAME_DIRECTION", "3"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_REPRICING_MAX_SAME_DIRECTION: int = int(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_REPRICING_MAX_SAME_DIRECTION", "2"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_FAST_EXPANSION_MAX_SAME_DIRECTION: int = int(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_FAST_EXPANSION_MAX_SAME_DIRECTION", "2"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_PANIC_SPREAD_MAX_SAME_DIRECTION: int = int(os.getenv("TRADING_MANAGER_XAU_PARALLEL_FAMILIES_PANIC_SPREAD_MAX_SAME_DIRECTION", "1"))
    TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ALLOWED: str = os.getenv(
        "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ALLOWED",
        "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar",
    )
    TRADING_MANAGER_XAU_HEDGE_LANE_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_HEDGE_LANE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_HEDGE_LANE_MIN_STATE_SCORE: float = float(os.getenv("TRADING_MANAGER_XAU_HEDGE_LANE_MIN_STATE_SCORE", "26"))
    TRADING_MANAGER_XAU_HEDGE_LANE_ALLOWED_FAMILIES: str = os.getenv(
        "TRADING_MANAGER_XAU_HEDGE_LANE_ALLOWED_FAMILIES",
        "xau_scalp_failed_fade_follow_stop,xau_scalp_flow_short_sidecar",
    )
    TRADING_MANAGER_XAU_HEDGE_LANE_MAX_PER_SYMBOL: int = int(os.getenv("TRADING_MANAGER_XAU_HEDGE_LANE_MAX_PER_SYMBOL", "1"))
    TRADING_MANAGER_XAU_HEDGE_LANE_RISK_MULTIPLIER: float = float(os.getenv("TRADING_MANAGER_XAU_HEDGE_LANE_RISK_MULTIPLIER", "0.65"))
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_STATE_SCORE: float = float(os.getenv("TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_STATE_SCORE", "28"))
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ALLOWED_FAMILIES: str = os.getenv(
        "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ALLOWED_FAMILIES",
        "xau_scalp_failed_fade_follow_stop,xau_scalp_flow_short_sidecar",
    )
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL: int = int(os.getenv("TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL", "2"))
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER: float = float(os.getenv("TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER", "0.55"))
    TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_VULNERABLE_REVIEWS: int = int(os.getenv("TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_VULNERABLE_REVIEWS", "1"))
    TRADING_MANAGER_XAU_ORDER_CARE_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_ORDER_CARE_RECENT_REVIEW_COUNT: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RECENT_REVIEW_COUNT", "5"))
    TRADING_MANAGER_XAU_ORDER_CARE_MIN_LOSSES: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_MIN_LOSSES", "2"))
    TRADING_MANAGER_XAU_ORDER_CARE_MIN_ACTIVE_MIN: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_MIN_ACTIVE_MIN", "45"))
    TRADING_MANAGER_XAU_ORDER_CARE_ALLOWED_SOURCES: str = os.getenv(
        "TRADING_MANAGER_XAU_ORDER_CARE_ALLOWED_SOURCES",
        "xauusd_scheduled:canary,scalp_xauusd:canary,scalp_xauusd,scalp_xauusd:winner,scalp_xauusd:pb:canary,scalp_xauusd:td:canary,scalp_xauusd:ff:canary,scalp_xauusd:mfu:canary,scalp_xauusd:fss:canary,scalp_xauusd:rr:canary",
    )
    TRADING_MANAGER_XAU_ORDER_CARE_FSS_ALLOWED_SOURCES: str = os.getenv(
        "TRADING_MANAGER_XAU_ORDER_CARE_FSS_ALLOWED_SOURCES",
        "scalp_xauusd:fss:canary",
    )
    TRADING_MANAGER_XAU_ORDER_CARE_LIMIT_RETEST_ALLOWED_SOURCES: str = os.getenv(
        "TRADING_MANAGER_XAU_ORDER_CARE_LIMIT_RETEST_ALLOWED_SOURCES",
        "xauusd_scheduled:canary,scalp_xauusd:canary,scalp_xauusd,scalp_xauusd:winner,scalp_xauusd:pb:canary,scalp_xauusd:td:canary,scalp_xauusd:ff:canary,scalp_xauusd:mfu:canary",
    )
    TRADING_MANAGER_XAU_ORDER_CARE_RANGE_REPAIR_ALLOWED_SOURCES: str = os.getenv(
        "TRADING_MANAGER_XAU_ORDER_CARE_RANGE_REPAIR_ALLOWED_SOURCES",
        "scalp_xauusd:rr:canary",
    )
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TIGHTEN_SCORE: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TIGHTEN_SCORE", "2"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_SCORE: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_SCORE", "4"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_MAX_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_MAX_R", "0.12"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_STOP_KEEP_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_STOP_KEEP_R", "0.30"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_PROFIT_LOCK_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_PROFIT_LOCK_R", "0.03"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TRIM_TP_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TRIM_TP_R", "0.40"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_AGE_MIN: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_AGE_MIN", "6"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_MAX_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_MAX_R", "0.03"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_TRIGGER_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_TRIGGER_R", "0.12"))
    TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_LOCK_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_LOCK_R", "0.01"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TIGHTEN_SCORE: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TIGHTEN_SCORE", "2"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_SCORE: int = int(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_SCORE", "4"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_MAX_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_MAX_R", "0.10"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_STOP_KEEP_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_STOP_KEEP_R", "0.28"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_PROFIT_LOCK_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_PROFIT_LOCK_R", "0.02"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TRIM_TP_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TRIM_TP_R", "0.36"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_AGE_MIN: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_AGE_MIN", "5"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_MAX_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_MAX_R", "0.02"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_TRIGGER_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_TRIGGER_R", "0.10"))
    TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_LOCK_R: float = float(os.getenv("TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_LOCK_R", "0.01"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_REGIME_TRANSITION_LOOKBACK_SEC: int = int(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_LOOKBACK_SEC", "240"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_HOLD_MIN: int = int(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_HOLD_MIN", "12"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_REJECTION_RATIO: float = float(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_REJECTION_RATIO", "0.34"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_MAX_ABS_CONTINUATION_BIAS: float = float(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_MAX_ABS_CONTINUATION_BIAS", "0.055"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_BAR_VOLUME_PROXY", "0.18"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_OPPOSITE_BIAS: float = float(os.getenv("TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_OPPOSITE_BIAS", "0.03"))
    TRADING_MANAGER_XAU_REGIME_TRANSITION_RANGE_STATES: str = os.getenv(
        "TRADING_MANAGER_XAU_REGIME_TRANSITION_RANGE_STATES",
        "range_probe",
    )
    TRADING_MANAGER_XAU_REGIME_TRANSITION_LIMIT_FAMILIES: str = os.getenv(
        "TRADING_MANAGER_XAU_REGIME_TRANSITION_LIMIT_FAMILIES",
        "xau_scalp_microtrend,xau_scalp_tick_depth_filter",
    )
    TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_FAMILIES: str = os.getenv(
        "TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_FAMILIES",
        "xau_scalp_range_repair",
    )
    TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_SOURCES: str = os.getenv(
        "TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_SOURCES",
        "scalp_xauusd:rr:canary",
    )
    TRADING_MANAGER_XAU_MICRO_REGIME_REFRESH_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_MICRO_REGIME_REFRESH_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_MICRO_REGIME_WINDOW_MIN: int = int(os.getenv("TRADING_MANAGER_XAU_MICRO_REGIME_WINDOW_MIN", "12"))
    TRADING_MANAGER_XAU_MICRO_REGIME_MIN_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_MICRO_REGIME_MIN_RESOLVED", "3"))
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_WINDOW_MIN: int = int(os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_WINDOW_MIN", "12"))
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_RESOLVED", "3"))
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_LOSSES: int = int(os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_LOSSES", "2"))
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_DISTINCT_FAMILIES: int = int(os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_DISTINCT_FAMILIES", "2"))
    TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MAX_PNL_USD: float = float(os.getenv("TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MAX_PNL_USD", "-5"))
    TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PM_REPAIR_MISSING_SL_ENABLED: bool = os.getenv("CTRADER_PM_REPAIR_MISSING_SL_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_BRIDGE_URL: str = os.getenv("CTRADER_BRIDGE_URL", "http://127.0.0.1:8788")
    CTRADER_BRIDGE_TOKEN: str = os.getenv("CTRADER_BRIDGE_TOKEN", os.getenv("DEXTER_BRIDGE_API_TOKEN", ""))
    CTRADER_STORE_FEED_ENABLED: bool = os.getenv("CTRADER_STORE_FEED_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_STORE_FEED_SOURCES: str = os.getenv("CTRADER_STORE_FEED_SOURCES", "scalp_xauusd,scalp_ethusd,scalp_btcusd,xauusd_scheduled,xauusd_scheduled:winner")
    CTRADER_MARKET_CAPTURE_ENABLED: bool = os.getenv("CTRADER_MARKET_CAPTURE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_MARKET_CAPTURE_SYMBOLS: str = os.getenv("CTRADER_MARKET_CAPTURE_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD")
    CTRADER_MARKET_CAPTURE_INTERVAL_MIN: int = int(os.getenv("CTRADER_MARKET_CAPTURE_INTERVAL_MIN", "5"))
    CTRADER_MARKET_CAPTURE_DURATION_SEC: int = int(os.getenv("CTRADER_MARKET_CAPTURE_DURATION_SEC", "12"))
    CTRADER_MARKET_CAPTURE_MAX_EVENTS: int = int(os.getenv("CTRADER_MARKET_CAPTURE_MAX_EVENTS", "600"))
    CTRADER_MARKET_CAPTURE_DEPTH_LEVELS: int = int(os.getenv("CTRADER_MARKET_CAPTURE_DEPTH_LEVELS", "5"))
    CTRADER_MARKET_CAPTURE_ON_START: bool = os.getenv("CTRADER_MARKET_CAPTURE_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_MARKET_CAPTURE_ON_EXECUTE: bool = os.getenv("CTRADER_MARKET_CAPTURE_ON_EXECUTE", "1").strip().lower() in ("1", "true", "yes", "on")

    # ── Copy Trade System ──
    COPY_TRADE_ENABLED: bool = os.getenv("COPY_TRADE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    COPY_TRADE_WORKER_TIMEOUT_SEC: int = int(os.getenv("COPY_TRADE_WORKER_TIMEOUT_SEC", "25"))
    COPY_TRADE_CLOSE_FOLLOW_ENABLED: bool = os.getenv("COPY_TRADE_CLOSE_FOLLOW_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    COPY_TRADE_PROTECTION_FOLLOW_ENABLED: bool = os.getenv("COPY_TRADE_PROTECTION_FOLLOW_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    COPY_TRADE_LATENCY_WARN_MS: int = int(os.getenv("COPY_TRADE_LATENCY_WARN_MS", "5000"))
    COPY_TRADE_CLOSE_FOLLOW_TIMEOUT_SEC: int = int(os.getenv("COPY_TRADE_CLOSE_FOLLOW_TIMEOUT_SEC", "18"))
    COPY_TRADE_PROTECTION_FOLLOW_TIMEOUT_SEC: int = int(os.getenv("COPY_TRADE_PROTECTION_FOLLOW_TIMEOUT_SEC", "18"))
    COPY_TRADE_CLOSE_EVENT_DEDUPE_SEC: int = int(os.getenv("COPY_TRADE_CLOSE_EVENT_DEDUPE_SEC", "90"))
    CTRADER_MARKET_CAPTURE_ON_EXECUTE_DURATION_SEC: int = int(os.getenv("CTRADER_MARKET_CAPTURE_ON_EXECUTE_DURATION_SEC", "6"))
    CTRADER_MARKET_CAPTURE_ON_EXECUTE_MAX_EVENTS: int = int(os.getenv("CTRADER_MARKET_CAPTURE_ON_EXECUTE_MAX_EVENTS", "240"))
    CTRADER_PENDING_ORDER_SWEEP_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_SWEEP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_SWEEP_ON_SYNC: bool = os.getenv("CTRADER_PENDING_ORDER_SWEEP_ON_SYNC", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_GRACE_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_GRACE_MIN", "5"))
    CTRADER_PENDING_ORDER_TTL_DEFAULT_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_DEFAULT_MIN", "120"))
    CTRADER_PENDING_ORDER_TTL_XAU_SCALP_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_XAU_SCALP_MIN", "45"))
    CTRADER_PENDING_ORDER_TTL_XAU_PULLBACK_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_XAU_PULLBACK_MIN", "45"))
    CTRADER_PENDING_ORDER_TTL_XAU_BREAKOUT_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_XAU_BREAKOUT_MIN", "15"))
    CTRADER_PENDING_ORDER_TTL_XAU_SCHEDULED_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_XAU_SCHEDULED_MIN", "240"))
    CTRADER_PENDING_ORDER_TTL_CRYPTO_WINNER_MIN: int = int(os.getenv("CTRADER_PENDING_ORDER_TTL_CRYPTO_WINNER_MIN", "180"))
    CTRADER_PENDING_ORDER_MAX_PER_SOURCE_SYMBOL: int = int(os.getenv("CTRADER_PENDING_ORDER_MAX_PER_SOURCE_SYMBOL", "3"))
    CTRADER_PENDING_ORDER_MAX_PER_SYMBOL: int = int(os.getenv("CTRADER_PENDING_ORDER_MAX_PER_SYMBOL", "2"))
    CTRADER_PENDING_ORDER_CANCEL_DISABLED_SOURCE: bool = os.getenv("CTRADER_PENDING_ORDER_CANCEL_DISABLED_SOURCE", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_CANCEL_DISABLED_FAMILY: bool = os.getenv("CTRADER_PENDING_ORDER_CANCEL_DISABLED_FAMILY", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ALLOWED_FAMILIES: str = os.getenv(
        "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ALLOWED_FAMILIES",
        "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter",
    )
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC: int = int(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", "75"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R: float = float(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", "0.18"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_STEP_R: float = float(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_STEP_R", "0.22"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_BUFFER_R: float = float(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_BUFFER_R", "0.16"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MAX_COUNT: int = int(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MAX_COUNT", "2"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_COOLDOWN_SEC: int = int(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_COOLDOWN_SEC", "75"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_MID_DRIFT_PCT: float = float(os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_MID_DRIFT_PCT", "0.006"))
    CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE: bool = os.getenv("CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_MAX_DISTANCE_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_MAX_DISTANCE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_AGE_SEC: int = int(os.getenv("CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_AGE_SEC", "120"))
    CTRADER_PENDING_ORDER_MAX_DISTANCE_R: float = float(os.getenv("CTRADER_PENDING_ORDER_MAX_DISTANCE_R", "1.45"))
    CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_R: float = float(os.getenv("CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_R", "0.65"))
    CTRADER_PENDING_ORDER_MAX_DISTANCE_MAX_R: float = float(os.getenv("CTRADER_PENDING_ORDER_MAX_DISTANCE_MAX_R", "1.75"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_FOLLOW_STOP_ALLOWED_FAMILIES: str = os.getenv(
        "CTRADER_PENDING_ORDER_FOLLOW_STOP_ALLOWED_FAMILIES",
        "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter",
    )
    CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", "0.34"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", "0.012"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", "0.02"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", "0.12"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", "0.12"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", "0.35"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", "0.10"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", "0.58"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", "0.88"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", "0.50"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE", "74"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT", "0.88"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT", "0.75"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT", "0.75"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT", "0.90"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT", "0.70"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_MIN_CONFIDENCE: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_MIN_CONFIDENCE", "75"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_TRIGGER_R_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_TRIGGER_R_MULT", "0.82"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_IMBALANCE_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_IMBALANCE_MULT", "0.65"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_DELTA_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_DELTA_MULT", "0.65"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_BAR_VOLUME_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_BAR_VOLUME_MULT", "1.05"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_REJECTION_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_REJECTION_MULT", "1.10"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_RISK_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_RISK_MULT", "0.55"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_ENABLED: bool = os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", "-1.0"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_TRIGGER_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_TRIGGER_MULT", "0.90"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_IMBALANCE_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_IMBALANCE_MULT", "0.90"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_DELTA_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_DELTA_MULT", "0.90"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_BAR_VOLUME_MULT: float = float(os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_BAR_VOLUME_MULT", "0.95"))
    CTRADER_PENDING_ORDER_FOLLOW_STOP_PANIC_SPREAD_DISABLE: bool = os.getenv("CTRADER_PENDING_ORDER_FOLLOW_STOP_PANIC_SPREAD_DISABLE", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_OPENAPI_CLIENT_ID: str = os.getenv("CTRADER_OPENAPI_CLIENT_ID", os.getenv("OpenAPI_ClientID", "")).strip()
    CTRADER_OPENAPI_CLIENT_SECRET: str = os.getenv("CTRADER_OPENAPI_CLIENT_SECRET", os.getenv("OpenAPI_Secreat", os.getenv("OpenAPI_Secret", ""))).strip()
    # Optional override for protobuf TCP host (default: demo.ctraderapi.com / live.ctraderapi.com). See Spotware proxy docs.
    CTRADER_OPENAPI_PROTOBUF_HOST: str = os.getenv("CTRADER_OPENAPI_PROTOBUF_HOST", "").strip()
    CTRADER_OPENAPI_PROTOBUF_PORT: int = int(os.getenv("CTRADER_OPENAPI_PROTOBUF_PORT", "5035") or "5035")
    CTRADER_OPENAPI_REDIRECT_URI: str = os.getenv("CTRADER_OPENAPI_REDIRECT_URI", "http://localhost")
    # Token resolution: token_manager handles priority (persisted state > env).
    # Legacy fallback keys (OpenAPI_Access_token_API_key) removed — they pointed
    # to revoked tokens after key rotation. Use CTRADER_OPENAPI_* keys only.
    CTRADER_OPENAPI_ACCESS_TOKEN: str = os.getenv("CTRADER_OPENAPI_ACCESS_TOKEN", "").strip()
    CTRADER_OPENAPI_REFRESH_TOKEN: str = os.getenv("CTRADER_OPENAPI_REFRESH_TOKEN", "").strip()
    CTRADER_USER_ID_JSON: str = os.getenv("CTRADER_USER_ID_JSON", os.getenv("Ctrader_UserID", ""))
    CTRADER_ACCOUNTS_JSON: str = os.getenv("CTRADER_ACCOUNTS_JSON", os.getenv("Ctrader_accounts", ""))
    MT5_READINESS_CHECK_ON_START: bool = os.getenv("MT5_READINESS_CHECK_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_READINESS_CHECK_TIME_UTC: str = os.getenv("MT5_READINESS_CHECK_TIME_UTC", "00:02")

    # ── Telegram ───────────────────────────────────────────────────────────────
    TELEGRAM_BOT_TOKEN: str = os.getenv(
        "TELEGRAM_BOT_TOKEN",
        "8536612154:AAGMbUo2mH45TSyWV1Eq22NX_-M_ZnlnPwA"   # @mrgeon8n_bot default
    )
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_ADMIN_IDS: str = os.getenv("TELEGRAM_ADMIN_IDS", "")  # comma-separated user IDs
    MONITOR_DISABLE_TELEGRAM: bool = os.getenv("MONITOR_DISABLE_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    TELEGRAM_BROADCAST_SIGNALS: bool = os.getenv("TELEGRAM_BROADCAST_SIGNALS", "1").strip().lower() in ("1", "true", "yes", "on")
    TELEGRAM_AUTO_BLOCK_UNREACHABLE: bool = os.getenv("TELEGRAM_AUTO_BLOCK_UNREACHABLE", "1").strip().lower() in ("1", "true", "yes", "on")

    # ── Crypto Exchange ────────────────────────────────────────────────────────
    CRYPTO_EXCHANGE:  str = os.getenv("CRYPTO_EXCHANGE", "binance")
    BINANCE_API_KEY:  str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_SECRET:   str = os.getenv("BINANCE_SECRET", "")
    BYBIT_API_KEY:    str = os.getenv("BYBIT_API_KEY", "")
    BYBIT_SECRET:     str = os.getenv("BYBIT_SECRET", "")

    # ── Scanner Intervals (seconds) ────────────────────────────────────────────
    CRYPTO_SCAN_INTERVAL: int = int(os.getenv("CRYPTO_SCAN_INTERVAL", "300"))    # 5 min
    XAUUSD_SCAN_INTERVAL: int = int(os.getenv("XAUUSD_SCAN_INTERVAL", "900"))    # 15 min
    FX_SCAN_INTERVAL:     int = int(os.getenv("FX_SCAN_INTERVAL", "300"))        # 5 min
    STOCK_SCAN_INTERVAL:  int = int(os.getenv("STOCK_SCAN_INTERVAL",  "1800"))   # 30 min
    US_OPEN_SMART_INTERVAL_MIN: int = int(os.getenv("US_OPEN_SMART_INTERVAL_MIN", "10"))
    US_OPEN_SMART_PREMARKET_LEAD_MIN: int = int(os.getenv("US_OPEN_SMART_PREMARKET_LEAD_MIN", "60"))
    US_OPEN_SMART_POST_OPEN_MAX_MIN: int = int(os.getenv("US_OPEN_SMART_POST_OPEN_MAX_MIN", "120"))
    US_OPEN_SMART_ALWAYS_REPORT: bool = os.getenv("US_OPEN_SMART_ALWAYS_REPORT", "0").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SMART_NO_OPP_PING_MIN: int = int(os.getenv("US_OPEN_SMART_NO_OPP_PING_MIN", "15"))
    US_OPEN_SESSION_CHECKIN_ENABLED: bool = os.getenv("US_OPEN_SESSION_CHECKIN_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_QUALITY_REPORT_INTERVAL_MIN: int = int(os.getenv("US_OPEN_QUALITY_REPORT_INTERVAL_MIN", "15"))
    SIGNAL_MONITOR_AUTO_PUSH_ENABLED: bool = os.getenv("SIGNAL_MONITOR_AUTO_PUSH_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SIGNAL_MONITOR_AUTO_PUSH_INTERVAL_MIN: int = int(os.getenv("SIGNAL_MONITOR_AUTO_PUSH_INTERVAL_MIN", "15"))
    SIGNAL_MONITOR_AUTO_PUSH_SYMBOLS: str = os.getenv("SIGNAL_MONITOR_AUTO_PUSH_SYMBOLS", "XAUUSD,ETHUSD")
    SIGNAL_MONITOR_AUTO_PUSH_WINDOW_MODE: str = os.getenv("SIGNAL_MONITOR_AUTO_PUSH_WINDOW_MODE", "today")
    SIGNAL_MONITOR_AUTO_PUSH_DAYS: int = int(os.getenv("SIGNAL_MONITOR_AUTO_PUSH_DAYS", "1"))
    SIGNAL_MONITOR_AUTO_PUSH_ON_START: bool = os.getenv("SIGNAL_MONITOR_AUTO_PUSH_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_LANE_SCORECARD_ENABLED: bool = os.getenv("MT5_LANE_SCORECARD_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_LANE_SCORECARD_LOOKBACK_DAYS: int = int(os.getenv("MT5_LANE_SCORECARD_LOOKBACK_DAYS", "1"))
    MT5_LANE_SCORECARD_NOTIFY_TELEGRAM: bool = os.getenv("MT5_LANE_SCORECARD_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_LANE_SCORECARD_TIME_UTC: str = os.getenv("MT5_LANE_SCORECARD_TIME_UTC", "00:10")
    MT5_LANE_SCORECARD_ON_START: bool = os.getenv("MT5_LANE_SCORECARD_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WEEKEND_SCORECARD_ENABLED: bool = os.getenv("CRYPTO_WEEKEND_SCORECARD_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WEEKEND_SCORECARD_LOOKBACK_DAYS: int = int(os.getenv("CRYPTO_WEEKEND_SCORECARD_LOOKBACK_DAYS", "14"))
    CRYPTO_WEEKEND_SCORECARD_NOTIFY_TELEGRAM: bool = os.getenv("CRYPTO_WEEKEND_SCORECARD_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_WEEKEND_SCORECARD_TIME_UTC: str = os.getenv("CRYPTO_WEEKEND_SCORECARD_TIME_UTC", "00:15")
    CRYPTO_WEEKEND_SCORECARD_ON_START: bool = os.getenv("CRYPTO_WEEKEND_SCORECARD_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    WINNER_MISSION_REPORT_ENABLED: bool = os.getenv("WINNER_MISSION_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    WINNER_MISSION_REPORT_LOOKBACK_DAYS: int = int(os.getenv("WINNER_MISSION_REPORT_LOOKBACK_DAYS", "14"))
    WINNER_MISSION_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("WINNER_MISSION_REPORT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    WINNER_MISSION_REPORT_TIME_UTC: str = os.getenv("WINNER_MISSION_REPORT_TIME_UTC", "00:20")
    WINNER_MISSION_REPORT_ON_START: bool = os.getenv("WINNER_MISSION_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    MISSED_OPPORTUNITY_AUDIT_ENABLED: bool = os.getenv("MISSED_OPPORTUNITY_AUDIT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MISSED_OPPORTUNITY_AUDIT_LOOKBACK_DAYS: int = int(os.getenv("MISSED_OPPORTUNITY_AUDIT_LOOKBACK_DAYS", "14"))
    MISSED_OPPORTUNITY_AUDIT_NOTIFY_TELEGRAM: bool = os.getenv("MISSED_OPPORTUNITY_AUDIT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    MISSED_OPPORTUNITY_AUDIT_TIME_UTC: str = os.getenv("MISSED_OPPORTUNITY_AUDIT_TIME_UTC", "00:25")
    MISSED_OPPORTUNITY_AUDIT_ON_START: bool = os.getenv("MISSED_OPPORTUNITY_AUDIT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    AUTO_APPLY_LIVE_PROFILE_ENABLED: bool = os.getenv("AUTO_APPLY_LIVE_PROFILE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    AUTO_APPLY_LIVE_PROFILE_TIME_UTC: str = os.getenv("AUTO_APPLY_LIVE_PROFILE_TIME_UTC", "00:30")
    AUTO_APPLY_LIVE_PROFILE_ON_START: bool = os.getenv("AUTO_APPLY_LIVE_PROFILE_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    AUTO_APPLY_LIVE_PROFILE_NOTIFY_TELEGRAM: bool = os.getenv("AUTO_APPLY_LIVE_PROFILE_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV: bool = os.getenv("AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", "1").strip().lower() in ("1", "true", "yes", "on")
    AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE: int = int(os.getenv("AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", "6"))
    AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE: float = float(os.getenv("AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE", "0.60"))
    AUTO_APPLY_LIVE_PROFILE_MIN_PNL_USD: float = float(os.getenv("AUTO_APPLY_LIVE_PROFILE_MIN_PNL_USD", "0.0"))
    AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_RESOLVED: int = int(os.getenv("AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_RESOLVED", "4"))
    AUTO_APPLY_LIVE_PROFILE_MAX_WAIT_MIN: float = float(os.getenv("AUTO_APPLY_LIVE_PROFILE_MAX_WAIT_MIN", "20.0"))
    AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MAX_NET_LOSS_USD: float = float(os.getenv("AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MAX_NET_LOSS_USD", "-20.0"))
    AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_WIN_RATE: float = float(os.getenv("AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_WIN_RATE", "0.40"))
    AUTO_APPLY_LIVE_PROFILE_ENV_BACKUP_KEEP: int = int(os.getenv("AUTO_APPLY_LIVE_PROFILE_ENV_BACKUP_KEEP", "20"))
    AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN: float = float(os.getenv("AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", "68.0"))
    AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX: float = float(os.getenv("AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", "80.0"))
    AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MIN: float = float(os.getenv("AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MIN", "70.0"))
    AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MAX: float = float(os.getenv("AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MAX", "82.0"))
    AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MIN: float = float(os.getenv("AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MIN", "72.0"))
    AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX: float = float(os.getenv("AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX", "84.0"))
    CANARY_POST_TRADE_AUDIT_ENABLED: bool = os.getenv("CANARY_POST_TRADE_AUDIT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CANARY_POST_TRADE_AUDIT_LOOKBACK_DAYS: int = int(os.getenv("CANARY_POST_TRADE_AUDIT_LOOKBACK_DAYS", "14"))
    CANARY_POST_TRADE_AUDIT_INTERVAL_MIN: int = int(os.getenv("CANARY_POST_TRADE_AUDIT_INTERVAL_MIN", "3"))
    CANARY_POST_TRADE_AUDIT_ON_START: bool = os.getenv("CANARY_POST_TRADE_AUDIT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CANARY_POST_TRADE_AUDIT_NOTIFY_TELEGRAM: bool = os.getenv("CANARY_POST_TRADE_AUDIT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    CANARY_POST_TRADE_AUDIT_MILESTONES: str = os.getenv("CANARY_POST_TRADE_AUDIT_MILESTONES", "3,5")
    CTRADER_DATA_INTEGRITY_REPORT_ENABLED: bool = os.getenv("CTRADER_DATA_INTEGRITY_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS: int = int(os.getenv("CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS", "180"))
    CTRADER_DATA_INTEGRITY_REPORT_INTERVAL_MIN: int = int(os.getenv("CTRADER_DATA_INTEGRITY_REPORT_INTERVAL_MIN", "30"))
    CTRADER_DATA_INTEGRITY_REPORT_ON_START: bool = os.getenv("CTRADER_DATA_INTEGRITY_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_DATA_INTEGRITY_REPORT_REPAIR_ON_RUN: bool = os.getenv("CTRADER_DATA_INTEGRITY_REPORT_REPAIR_ON_RUN", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_REPORT_ENABLED: bool = os.getenv("XAU_DIRECT_LANE_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS: int = int(os.getenv("XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS", "72"))
    XAU_DIRECT_LANE_REPORT_INTERVAL_MIN: int = int(os.getenv("XAU_DIRECT_LANE_REPORT_INTERVAL_MIN", "60"))
    XAU_DIRECT_LANE_REPORT_ON_START: bool = os.getenv("XAU_DIRECT_LANE_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("XAU_DIRECT_LANE_REPORT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_AUTO_TUNE_ENABLED: bool = os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS: int = int(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS", "24"))
    XAU_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN: int = int(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN", "120"))
    XAU_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED: int = int(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED", "4"))
    XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", "0.42"))
    XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_PNL_USD: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_PNL_USD", "-4.0"))
    XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE", "0.60"))
    XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_PNL_USD: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_PNL_USD", "4.0"))
    XAU_DIRECT_LANE_AUTO_TUNE_TARGET_FILL_RATE: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_TARGET_FILL_RATE", "0.55"))
    XAU_DIRECT_LANE_AUTO_TUNE_CONF_STEP: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_CONF_STEP", "0.5"))
    XAU_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR", "70.0"))
    XAU_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL: float = float(os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL", "78.0"))
    XAU_DIRECT_LANE_AUTO_TUNE_ON_START: bool = os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_AUTO_TUNE_NOTIFY_TELEGRAM: bool = os.getenv("XAU_DIRECT_LANE_AUTO_TUNE_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    # Parameter Trial Sandbox — POC gate before any auto-tune change goes live
    XAU_DIRECT_LANE_TRIAL_ENABLED: bool = os.getenv("XAU_DIRECT_LANE_TRIAL_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_TRIAL_BT_LOOKBACK_HOURS: int = int(os.getenv("XAU_DIRECT_LANE_TRIAL_BT_LOOKBACK_HOURS", "72"))
    XAU_DIRECT_LANE_TRIAL_BT_MIN_INCREMENTAL: int = int(os.getenv("XAU_DIRECT_LANE_TRIAL_BT_MIN_INCREMENTAL", "3"))
    XAU_DIRECT_LANE_TRIAL_BT_MIN_WIN_RATE: float = float(os.getenv("XAU_DIRECT_LANE_TRIAL_BT_MIN_WIN_RATE", "0.55"))
    XAU_DIRECT_LANE_TRIAL_BT_INTERVAL_MIN: int = int(os.getenv("XAU_DIRECT_LANE_TRIAL_BT_INTERVAL_MIN", "15"))
    XAU_DIRECT_LANE_TRIAL_BT_ON_START: bool = os.getenv("XAU_DIRECT_LANE_TRIAL_BT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_TRIAL_NOTIFY_TELEGRAM: bool = os.getenv("XAU_DIRECT_LANE_TRIAL_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_DIRECT_LANE_TRIAL_MAX_PENDING: int = int(os.getenv("XAU_DIRECT_LANE_TRIAL_MAX_PENDING", "3"))
    # Shadow backtest — simulate blocked XAU direct lane signals against candle history
    XAU_FAMILY_CANARY_GATE_JOURNAL_ENABLED: bool = os.getenv(
        "XAU_FAMILY_CANARY_GATE_JOURNAL_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    XAU_SHADOW_BACKTEST_ENABLED: bool = os.getenv("XAU_SHADOW_BACKTEST_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_SHADOW_BACKTEST_RESOLVE_HOURS: float = float(os.getenv("XAU_SHADOW_BACKTEST_RESOLVE_HOURS", "4.0"))
    XAU_SHADOW_BACKTEST_MIN_SAMPLE: int = int(os.getenv("XAU_SHADOW_BACKTEST_MIN_SAMPLE", "5"))
    XAU_SHADOW_BACKTEST_INTERVAL_MIN: int = int(os.getenv("XAU_SHADOW_BACKTEST_INTERVAL_MIN", "30"))
    XAU_SHADOW_BACKTEST_ON_START: bool = os.getenv("XAU_SHADOW_BACKTEST_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    # BTC direct lane auto-tune — self-tune BFSS/BFLS confidence from live fills
    BTC_DIRECT_LANE_AUTO_TUNE_ENABLED: bool = os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS: int = int(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS", "48"))
    BTC_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED: int = int(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED", "3"))
    BTC_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN: int = int(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN", "120"))
    BTC_DIRECT_LANE_AUTO_TUNE_ON_START: bool = os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    BTC_DIRECT_LANE_AUTO_TUNE_CONF_STEP: float = float(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_CONF_STEP", "0.5"))
    BTC_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE: float = float(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", "0.42"))
    BTC_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE: float = float(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE", "0.62"))
    BTC_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR: float = float(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR", "63.0"))
    BTC_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL: float = float(os.getenv("BTC_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL", "74.0"))
    # ── Conductor / Multi-Agent ──────────────────────────────────────────────
    CONDUCTOR_ENABLED: bool = os.getenv("CONDUCTOR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CONDUCTOR_INTERVAL_MIN: int = int(os.getenv("CONDUCTOR_INTERVAL_MIN", "30"))
    CONDUCTOR_ON_START: bool = os.getenv("CONDUCTOR_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CONDUCTOR_OPPORTUNITY_FOLLOW_ENABLED: bool = os.getenv("CONDUCTOR_OPPORTUNITY_FOLLOW_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CONDUCTOR_FOLLOW_MIN_BEHAVIORAL_WR: float = float(os.getenv("CONDUCTOR_FOLLOW_MIN_BEHAVIORAL_WR", "0.60") or "0.60")
    CONDUCTOR_FOLLOW_MIN_RESOLVED: int = int(os.getenv("CONDUCTOR_FOLLOW_MIN_RESOLVED", "4") or "4")
    CONDUCTOR_FOLLOW_EXPIRE_MIN: int = int(os.getenv("CONDUCTOR_FOLLOW_EXPIRE_MIN", "90") or "90")
    OPENCLAW_GATEWAY_URL: str = os.getenv("OPENCLAW_GATEWAY_URL", "")
    OPENCLAW_VERSION_GUARD_ENABLED: bool = os.getenv("OPENCLAW_VERSION_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    OPENCLAW_VERSION_GUARD_INTERVAL_MIN: int = int(os.getenv("OPENCLAW_VERSION_GUARD_INTERVAL_MIN", "240") or "240")
    # ── Qwen / DashScope (free tier) ─────────────────────────────────────────
    QWEN_API_KEY: str = os.getenv("QWEN_API_KEY", "")
    QWEN_BASE_URL: str = os.getenv("QWEN_BASE_URL", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1")
    QWEN_MODEL: str = os.getenv("QWEN_MODEL", "qwen-plus")  # conductor uses this; /ask uses qwen-turbo directly
    QWEN_PLUS_MONTHLY_BUDGET: int = int(os.getenv("QWEN_PLUS_MONTHLY_BUDGET", "900000") or "900000")
    QWEN_TURBO_MONTHLY_BUDGET: int = int(os.getenv("QWEN_TURBO_MONTHLY_BUDGET", "900000") or "900000")
    STRATEGY_LAB_REPORT_ENABLED: bool = os.getenv("STRATEGY_LAB_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_LAB_REPORT_INTERVAL_MIN: int = int(os.getenv("STRATEGY_LAB_REPORT_INTERVAL_MIN", "15"))
    STRATEGY_LAB_REPORT_ON_START: bool = os.getenv("STRATEGY_LAB_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_LAB_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("STRATEGY_LAB_REPORT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_LAB_TEAM_ENABLED: bool = os.getenv("STRATEGY_LAB_TEAM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_LAB_TEAM_TOPK: int = int(os.getenv("STRATEGY_LAB_TEAM_TOPK", "5"))
    STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE: float = float(os.getenv("STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE", "8"))
    STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED: int = int(os.getenv("STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED", "4"))
    STRATEGY_LAB_TEAM_RECOVERY_ENABLED: bool = os.getenv("STRATEGY_LAB_TEAM_RECOVERY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_LAB_TEAM_RECOVERY_TOPK: int = int(os.getenv("STRATEGY_LAB_TEAM_RECOVERY_TOPK", "3"))
    STRATEGY_LAB_FORCE_RECOVERY_FAMILIES: str = os.getenv("STRATEGY_LAB_FORCE_RECOVERY_FAMILIES", "")
    STRATEGY_GENERATOR_ENABLED: bool = os.getenv("STRATEGY_GENERATOR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_GENERATOR_MIN_SAMPLE: int = int(os.getenv("STRATEGY_GENERATOR_MIN_SAMPLE", "6"))
    STRATEGY_WALK_FORWARD_WINDOWS_DAYS: str = os.getenv("STRATEGY_WALK_FORWARD_WINDOWS_DAYS", "3,7,14")
    STRATEGY_PROMOTION_ENABLED: bool = os.getenv("STRATEGY_PROMOTION_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_PROMOTION_MIN_SAMPLE: int = int(os.getenv("STRATEGY_PROMOTION_MIN_SAMPLE", "8"))
    STRATEGY_PROMOTION_MIN_WIN_RATE_EDGE: float = float(os.getenv("STRATEGY_PROMOTION_MIN_WIN_RATE_EDGE", "0.02"))
    STRATEGY_PROMOTION_MIN_PNL_EDGE_USD: float = float(os.getenv("STRATEGY_PROMOTION_MIN_PNL_EDGE_USD", "0.0"))
    STRATEGY_PROMOTION_MIN_SCORE_EDGE: float = float(os.getenv("STRATEGY_PROMOTION_MIN_SCORE_EDGE", "0.5"))
    STRATEGY_PROMOTION_REQUIRE_POSITIVE_SCORE: bool = os.getenv("STRATEGY_PROMOTION_REQUIRE_POSITIVE_SCORE", "1").strip().lower() in ("1", "true", "yes", "on")
    STRATEGY_PROMOTION_MIN_DSR: float = float(os.getenv("STRATEGY_PROMOTION_MIN_DSR", "0.0"))
    STRATEGY_PROMOTION_MAX_DD_USD: float = float(os.getenv("STRATEGY_PROMOTION_MAX_DD_USD", "35.0"))
    STRATEGY_PROMOTION_PURGE_HOURS: int = int(os.getenv("STRATEGY_PROMOTION_PURGE_HOURS", "6"))
    STRATEGY_PROMOTION_STAGED_MAX_UNCERTAINTY: float = float(os.getenv("STRATEGY_PROMOTION_STAGED_MAX_UNCERTAINTY", "0.60"))
    FAMILY_CALIBRATION_REPORT_ENABLED: bool = os.getenv("FAMILY_CALIBRATION_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FAMILY_CALIBRATION_REPORT_LOOKBACK_DAYS: int = int(os.getenv("FAMILY_CALIBRATION_REPORT_LOOKBACK_DAYS", "21"))
    FAMILY_CALIBRATION_REPORT_INTERVAL_MIN: int = int(os.getenv("FAMILY_CALIBRATION_REPORT_INTERVAL_MIN", "15"))
    FAMILY_CALIBRATION_REPORT_ON_START: bool = os.getenv("FAMILY_CALIBRATION_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    FAMILY_CALIBRATION_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("FAMILY_CALIBRATION_REPORT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    FAMILY_CALIBRATION_PRIOR_STRENGTH: float = float(os.getenv("FAMILY_CALIBRATION_PRIOR_STRENGTH", "6.0"))
    RECENT_WIN_CLUSTER_LOOKBACK_HOURS: int = int(os.getenv("RECENT_WIN_CLUSTER_LOOKBACK_HOURS", "8"))
    RECENT_WIN_CLUSTER_MIN_RESOLVED: int = int(os.getenv("RECENT_WIN_CLUSTER_MIN_RESOLVED", "2"))
    RECENT_WIN_CLUSTER_MAX_HOLD_MIN: int = int(os.getenv("RECENT_WIN_CLUSTER_MAX_HOLD_MIN", "45"))
    RECENT_WIN_CLUSTER_ROUTER_BONUS_CAP: float = float(os.getenv("RECENT_WIN_CLUSTER_ROUTER_BONUS_CAP", "6.0"))
    RECENT_WIN_CLUSTER_ROUTER_BONUS_MULT: float = float(os.getenv("RECENT_WIN_CLUSTER_ROUTER_BONUS_MULT", "1.0"))
    CHART_STATE_ROUTER_BONUS_ENABLED: bool = os.getenv("CHART_STATE_ROUTER_BONUS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CHART_STATE_ROUTER_BONUS_CAP: float = float(os.getenv("CHART_STATE_ROUTER_BONUS_CAP", "4.0"))
    CHART_STATE_ROUTER_BONUS_MULT: float = float(os.getenv("CHART_STATE_ROUTER_BONUS_MULT", "0.12"))
    CHART_STATE_ROUTER_MIN_RESOLVED: int = int(os.getenv("CHART_STATE_ROUTER_MIN_RESOLVED", "3"))
    CHART_STATE_ROUTER_FOLLOW_UP_ONLY: bool = os.getenv("CHART_STATE_ROUTER_FOLLOW_UP_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    # Min resolved trades per bucket to mark follow_up_candidate in chart_state_memory_report (FSS/FLS context loader).
    # Default 1 allows first profitable continuation bucket to qualify; still requires pnl>0, wr>=0.57, allowed state_label.
    CHART_STATE_MEMORY_FOLLOW_UP_MIN_RESOLVED: int = int(os.getenv("CHART_STATE_MEMORY_FOLLOW_UP_MIN_RESOLVED", "1"))
    WINNER_MEMORY_LIBRARY_LOOKBACK_DAYS: int = int(os.getenv("WINNER_MEMORY_LIBRARY_LOOKBACK_DAYS", "21"))
    WINNER_MEMORY_LIBRARY_MIN_RESOLVED: int = int(os.getenv("WINNER_MEMORY_LIBRARY_MIN_RESOLVED", "3"))
    WINNER_MEMORY_LIBRARY_MIN_WIN_RATE: float = float(os.getenv("WINNER_MEMORY_LIBRARY_MIN_WIN_RATE", "0.60"))
    EXTERNAL_MODEL_PRIOR_LIBRARY_ENABLED: bool = os.getenv("EXTERNAL_MODEL_PRIOR_LIBRARY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    EXTERNAL_MODEL_PRIOR_ROUTER_ENABLED: bool = os.getenv("EXTERNAL_MODEL_PRIOR_ROUTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    EXTERNAL_MODEL_PRIOR_ROUTER_BONUS_CAP: float = float(os.getenv("EXTERNAL_MODEL_PRIOR_ROUTER_BONUS_CAP", "3.0"))
    EXTERNAL_MODEL_PRIOR_ROUTER_MULT: float = float(os.getenv("EXTERNAL_MODEL_PRIOR_ROUTER_MULT", "1.0"))
    UNCERTAINTY_ROUTER_ENABLED: bool = os.getenv("UNCERTAINTY_ROUTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    UNCERTAINTY_ROUTER_MIN_SAMPLE: int = int(os.getenv("UNCERTAINTY_ROUTER_MIN_SAMPLE", "6"))
    UNCERTAINTY_ROUTER_PENALTY_MULT: float = float(os.getenv("UNCERTAINTY_ROUTER_PENALTY_MULT", "18.0"))
    FAMILY_UNCERTAINTY_ROUTER_ENABLED: bool = os.getenv("FAMILY_UNCERTAINTY_ROUTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FAMILY_UNCERTAINTY_CAPTURE_MIN_SAMPLE: int = int(os.getenv("FAMILY_UNCERTAINTY_CAPTURE_MIN_SAMPLE", "10"))
    FAMILY_UNCERTAINTY_POSITIVE_BONUS: float = float(os.getenv("FAMILY_UNCERTAINTY_POSITIVE_BONUS", "0.04"))
    FAMILY_UNCERTAINTY_NEGATIVE_PENALTY: float = float(os.getenv("FAMILY_UNCERTAINTY_NEGATIVE_PENALTY", "0.08"))
    FAMILY_UNCERTAINTY_PRIOR_BONUS: float = float(os.getenv("FAMILY_UNCERTAINTY_PRIOR_BONUS", "0.03"))
    CTRADER_TICK_DEPTH_REPLAY_LAB_ENABLED: bool = os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_TICK_DEPTH_REPLAY_LAB_LOOKBACK_DAYS: int = int(os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_LOOKBACK_DAYS", "7"))
    CTRADER_TICK_DEPTH_REPLAY_LAB_INTERVAL_MIN: int = int(os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_INTERVAL_MIN", "20"))
    CTRADER_TICK_DEPTH_REPLAY_LAB_REPLAY_WINDOW_SEC: int = int(os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_REPLAY_WINDOW_SEC", "45"))
    CTRADER_TICK_DEPTH_REPLAY_LAB_ON_START: bool = os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_TICK_DEPTH_REPLAY_LAB_NOTIFY_TELEGRAM: bool = os.getenv("CTRADER_TICK_DEPTH_REPLAY_LAB_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    MISSION_PROGRESS_REPORT_ENABLED: bool = os.getenv("MISSION_PROGRESS_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MISSION_PROGRESS_REPORT_INTERVAL_MIN: int = int(os.getenv("MISSION_PROGRESS_REPORT_INTERVAL_MIN", "10"))
    MISSION_PROGRESS_REPORT_ON_START: bool = os.getenv("MISSION_PROGRESS_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    MISSION_PROGRESS_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("MISSION_PROGRESS_REPORT_NOTIFY_TELEGRAM", "0").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_REPORT_ENABLED: bool = os.getenv("TRADING_MANAGER_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_REPORT_INTERVAL_MIN: int = int(os.getenv("TRADING_MANAGER_REPORT_INTERVAL_MIN", "15"))
    TRADING_MANAGER_REPORT_ON_START: bool = os.getenv("TRADING_MANAGER_REPORT_ON_START", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_REPORT_NOTIFY_TELEGRAM: bool = os.getenv("TRADING_MANAGER_REPORT_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_REPORT_LOOKBACK_HOURS: int = int(os.getenv("TRADING_MANAGER_REPORT_LOOKBACK_HOURS", "24"))
    TRADING_MANAGER_REPORT_TIMEZONE: str = os.getenv("TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok")
    TRADING_MANAGER_REPORT_SYMBOLS: str = os.getenv("TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD")
    TRADING_MANAGER_EVENT_WINDOW_MIN: int = int(os.getenv("TRADING_MANAGER_EVENT_WINDOW_MIN", "20"))
    TRADING_MANAGER_EVENT_PAD_MIN: int = int(os.getenv("TRADING_MANAGER_EVENT_PAD_MIN", "12"))
    TRADING_MANAGER_EVENT_MIN_DROP_PCT: float = float(os.getenv("TRADING_MANAGER_EVENT_MIN_DROP_PCT", "0.30"))
    TRADING_MANAGER_AUTO_TUNE_ENABLED: bool = os.getenv("TRADING_MANAGER_AUTO_TUNE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_AUTO_TUNE_PERSIST_ENV: bool = os.getenv("TRADING_MANAGER_AUTO_TUNE_PERSIST_ENV", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_AUTO_ROUTING_ENABLED: bool = os.getenv("TRADING_MANAGER_AUTO_ROUTING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV: bool = os.getenv("TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_REASON_MEMORY_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_PERSIST_ENV: bool = os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_PERSIST_ENV", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_REASON_MEMORY_LOOKBACK_DAYS: int = int(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_LOOKBACK_DAYS", os.getenv("NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS", "120")))
    TRADING_MANAGER_XAU_REASON_MEMORY_MIN_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_MIN_RESOLVED", os.getenv("NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED", "8")))
    TRADING_MANAGER_XAU_REASON_MEMORY_MIN_MATCHED_TAGS: int = int(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_MIN_MATCHED_TAGS", "2"))
    TRADING_MANAGER_XAU_REASON_MEMORY_MIN_ABS_SCORE: float = float(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_MIN_ABS_SCORE", "0.10"))
    TRADING_MANAGER_XAU_REASON_MEMORY_CONFIDENCE_SCORE_MULT: float = float(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_CONFIDENCE_SCORE_MULT", "4.0"))
    TRADING_MANAGER_XAU_REASON_MEMORY_MAX_ABS_DELTA: float = float(os.getenv("TRADING_MANAGER_XAU_REASON_MEMORY_MAX_ABS_DELTA", "2.5"))
    TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED: bool = os.getenv("TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_OPPORTUNITY_FEED_TOPK: int = int(os.getenv("TRADING_MANAGER_OPPORTUNITY_FEED_TOPK", "3"))
    TRADING_MANAGER_OPPORTUNITY_FEED_MIN_STATE_SCORE: float = float(os.getenv("TRADING_MANAGER_OPPORTUNITY_FEED_MIN_STATE_SCORE", "24"))
    TRADING_MANAGER_OPPORTUNITY_FEED_MIN_RESOLVED: int = int(os.getenv("TRADING_MANAGER_OPPORTUNITY_FEED_MIN_RESOLVED", "2"))
    TRADING_MANAGER_MACRO_LOOKBACK_HOURS: int = int(os.getenv("TRADING_MANAGER_MACRO_LOOKBACK_HOURS", "24"))
    TRADING_MANAGER_MACRO_MIN_SCORE: int = int(os.getenv("TRADING_MANAGER_MACRO_MIN_SCORE", "8"))
    TRADING_MANAGER_CALENDAR_LOOKAHEAD_HOURS: int = int(os.getenv("TRADING_MANAGER_CALENDAR_LOOKAHEAD_HOURS", "8"))
    TRADING_MANAGER_CALENDAR_MAX_EVENTS: int = int(os.getenv("TRADING_MANAGER_CALENDAR_MAX_EVENTS", "6"))
    TRADING_MANAGER_PRE_EVENT_FREEZE_MIN: int = int(os.getenv("TRADING_MANAGER_PRE_EVENT_FREEZE_MIN", "20"))
    TRADING_MANAGER_XAU_SHOCK_SIZE_MULT: float = float(os.getenv("TRADING_MANAGER_XAU_SHOCK_SIZE_MULT", "0.25"))
    TRADING_MANAGER_XAU_SHOCK_TP1_RR: float = float(os.getenv("TRADING_MANAGER_XAU_SHOCK_TP1_RR", "0.45"))
    TRADING_MANAGER_XAU_SHOCK_TP2_RR: float = float(os.getenv("TRADING_MANAGER_XAU_SHOCK_TP2_RR", "0.75"))
    TRADING_MANAGER_XAU_SHOCK_TP3_RR: float = float(os.getenv("TRADING_MANAGER_XAU_SHOCK_TP3_RR", "1.05"))
    TRADING_MANAGER_XAU_PRE_EVENT_SIZE_MULT: float = float(os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_SIZE_MULT", "0.30"))
    TRADING_MANAGER_XAU_PRE_EVENT_TP1_RR: float = float(os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_TP1_RR", "0.50"))
    TRADING_MANAGER_XAU_PRE_EVENT_TP2_RR: float = float(os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_TP2_RR", "0.85"))
    TRADING_MANAGER_XAU_PRE_EVENT_TP3_RR: float = float(os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_TP3_RR", "1.20"))
    TRADING_MANAGER_XAU_PRE_EVENT_PRIMARY_FAMILY: str = os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_PRIMARY_FAMILY", "xau_scalp_pullback_limit")
    TRADING_MANAGER_XAU_PRE_EVENT_ACTIVE_FAMILIES: str = os.getenv("TRADING_MANAGER_XAU_PRE_EVENT_ACTIVE_FAMILIES", "xau_scalp_pullback_limit")
    TRADING_MANAGER_XAU_SHOCK_PRIMARY_FAMILY: str = os.getenv("TRADING_MANAGER_XAU_SHOCK_PRIMARY_FAMILY", "xau_scalp_pullback_limit")
    TRADING_MANAGER_XAU_SHOCK_ACTIVE_FAMILIES: str = os.getenv("TRADING_MANAGER_XAU_SHOCK_ACTIVE_FAMILIES", "xau_scalp_pullback_limit")
    TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_RESOLVED", "3"))
    TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_PNL_USD: float = float(os.getenv("TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_PNL_USD", "0"))
    TRADING_MANAGER_XAU_PB_DEMOTE_ENABLED: bool = os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_PB_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_PB_RESOLVED", "12"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MAX_PB_PNL_USD: float = float(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MAX_PB_PNL_USD", "-10"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_RESOLVED", "6"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_PNL_USD: float = float(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_PNL_USD", "10"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_WIN_RATE: float = float(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_WIN_RATE", "0.60"))
    TRADING_MANAGER_XAU_PB_DEMOTE_USE_CALIBRATION_FALLBACK: bool = os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_USE_CALIBRATION_FALLBACK", "1").strip().lower() in ("1", "true", "yes", "on")
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_RESOLVED: int = int(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_RESOLVED", "20"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_PNL_USD: float = float(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_PNL_USD", "40"))
    TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_WIN_RATE: float = float(os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_WIN_RATE", "0.72"))
    TRADING_MANAGER_XAU_PB_DEMOTE_PRIMARY_FAMILY: str = os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_PRIMARY_FAMILY", "xau_scalp_pullback_limit")
    TRADING_MANAGER_XAU_PB_DEMOTE_ACTIVE_FAMILIES: str = os.getenv("TRADING_MANAGER_XAU_PB_DEMOTE_ACTIVE_FAMILIES", "xau_scalp_pullback_limit")
    US_OPEN_MOOD_STOP_ENABLED: bool = os.getenv("US_OPEN_MOOD_STOP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_MOOD_CHECK_START_MIN: int = int(os.getenv("US_OPEN_MOOD_CHECK_START_MIN", "45"))
    US_OPEN_MOOD_WEAK_CYCLES_TO_STOP: int = int(os.getenv("US_OPEN_MOOD_WEAK_CYCLES_TO_STOP", "3"))
    US_OPEN_SYMBOL_ALERT_COOLDOWN_MIN: int = int(os.getenv("US_OPEN_SYMBOL_ALERT_COOLDOWN_MIN", "20"))
    US_OPEN_RECORD_NEW_SYMBOLS_ONLY: bool = os.getenv("US_OPEN_RECORD_NEW_SYMBOLS_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_MACRO_FREEZE_ENABLED: bool = os.getenv("US_OPEN_MACRO_FREEZE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_MACRO_FREEZE_MIN_SCORE: int = int(os.getenv("US_OPEN_MACRO_FREEZE_MIN_SCORE", "8"))
    US_OPEN_MACRO_FREEZE_MAX_AGE_MIN: int = int(os.getenv("US_OPEN_MACRO_FREEZE_MAX_AGE_MIN", "45"))
    US_OPEN_MACRO_FREEZE_PRIORITY_ONLY: bool = os.getenv("US_OPEN_MACRO_FREEZE_PRIORITY_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_MACRO_FREEZE_MIN_SOURCE_QUALITY: float = float(os.getenv("US_OPEN_MACRO_FREEZE_MIN_SOURCE_QUALITY", "0.70"))
    US_OPEN_CIRCUIT_BREAKER_ENABLED: bool = os.getenv("US_OPEN_CIRCUIT_BREAKER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_CIRCUIT_BREAKER_CHECK_START_MIN: int = int(os.getenv("US_OPEN_CIRCUIT_BREAKER_CHECK_START_MIN", "30"))
    US_OPEN_CIRCUIT_BREAKER_MIN_RESOLVED: int = int(os.getenv("US_OPEN_CIRCUIT_BREAKER_MIN_RESOLVED", "8"))
    US_OPEN_CIRCUIT_BREAKER_MAX_WIN_RATE: float = float(os.getenv("US_OPEN_CIRCUIT_BREAKER_MAX_WIN_RATE", "25"))
    US_OPEN_CIRCUIT_BREAKER_MIN_SL: int = int(os.getenv("US_OPEN_CIRCUIT_BREAKER_MIN_SL", "4"))
    US_OPEN_CIRCUIT_BREAKER_MAX_AVG_R: float = float(os.getenv("US_OPEN_CIRCUIT_BREAKER_MAX_AVG_R", "-0.50"))
    US_OPEN_SETUP_WEIGHTING_ENABLED: bool = os.getenv("US_OPEN_SETUP_WEIGHTING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SETUP_STATS_MIN_RESOLVED: int = int(os.getenv("US_OPEN_SETUP_STATS_MIN_RESOLVED", "6"))
    US_OPEN_SETUP_POOR_WR: float = float(os.getenv("US_OPEN_SETUP_POOR_WR", "35"))
    US_OPEN_SETUP_POOR_NET_R: float = float(os.getenv("US_OPEN_SETUP_POOR_NET_R", "-1.0"))
    US_OPEN_SETUP_HARD_BLOCK_ENABLED: bool = os.getenv("US_OPEN_SETUP_HARD_BLOCK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SETUP_HARD_BLOCK_MIN_RESOLVED: int = int(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MIN_RESOLVED", "10"))
    US_OPEN_SETUP_HARD_BLOCK_MAX_WR: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_WR", "8"))
    US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R", "-2.5"))
    US_OPEN_SETUP_BOOST_ENABLED: bool = os.getenv("US_OPEN_SETUP_BOOST_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SETUP_BOOST_MIN_RESOLVED: int = int(os.getenv("US_OPEN_SETUP_BOOST_MIN_RESOLVED", "8"))
    US_OPEN_SETUP_BOOST_MIN_WR: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_WR", "58"))
    US_OPEN_SETUP_BOOST_MIN_NET_R: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_NET_R", "0.8"))
    US_OPEN_SETUP_MAX_PENALTY_CHOCH: float = float(os.getenv("US_OPEN_SETUP_MAX_PENALTY_CHOCH", "8.0"))
    US_OPEN_SETUP_MAX_PENALTY_BB_SQUEEZE: float = float(os.getenv("US_OPEN_SETUP_MAX_PENALTY_BB_SQUEEZE", "6.0"))
    US_OPEN_SETUP_MAX_PENALTY_OB_BOUNCE: float = float(os.getenv("US_OPEN_SETUP_MAX_PENALTY_OB_BOUNCE", "3.0"))
    US_OPEN_SETUP_MAX_BOOST_OB_BOUNCE: float = float(os.getenv("US_OPEN_SETUP_MAX_BOOST_OB_BOUNCE", "2.0"))
    US_OPEN_SYMBOL_SESSION_LOSS_CAP_ENABLED: bool = os.getenv("US_OPEN_SYMBOL_SESSION_LOSS_CAP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SYMBOL_SESSION_LOSS_CAP_MIN_RESOLVED: int = int(os.getenv("US_OPEN_SYMBOL_SESSION_LOSS_CAP_MIN_RESOLVED", "4"))
    US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_NEG_R: float = float(os.getenv("US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_NEG_R", "-2.0"))
    US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_LOSSES: int = int(os.getenv("US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_LOSSES", "4"))
    US_OPEN_SETUP_POOR_WR_CORE: float = float(os.getenv("US_OPEN_SETUP_POOR_WR_CORE", str(US_OPEN_SETUP_POOR_WR)))
    US_OPEN_SETUP_POOR_WR_LATE: float = float(os.getenv("US_OPEN_SETUP_POOR_WR_LATE", str(US_OPEN_SETUP_POOR_WR)))
    US_OPEN_SETUP_POOR_NET_R_CORE: float = float(os.getenv("US_OPEN_SETUP_POOR_NET_R_CORE", str(US_OPEN_SETUP_POOR_NET_R)))
    US_OPEN_SETUP_POOR_NET_R_LATE: float = float(os.getenv("US_OPEN_SETUP_POOR_NET_R_LATE", str(US_OPEN_SETUP_POOR_NET_R)))
    US_OPEN_SETUP_HARD_BLOCK_MAX_WR_CORE: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_WR_CORE", str(US_OPEN_SETUP_HARD_BLOCK_MAX_WR)))
    US_OPEN_SETUP_HARD_BLOCK_MAX_WR_LATE: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_WR_LATE", str(US_OPEN_SETUP_HARD_BLOCK_MAX_WR)))
    US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R_CORE: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R_CORE", str(US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R)))
    US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R_LATE: float = float(os.getenv("US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R_LATE", str(US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R)))
    US_OPEN_SETUP_BOOST_MIN_WR_CORE: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_WR_CORE", str(US_OPEN_SETUP_BOOST_MIN_WR)))
    US_OPEN_SETUP_BOOST_MIN_WR_LATE: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_WR_LATE", str(US_OPEN_SETUP_BOOST_MIN_WR)))
    US_OPEN_SETUP_BOOST_MIN_NET_R_CORE: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_NET_R_CORE", str(US_OPEN_SETUP_BOOST_MIN_NET_R)))
    US_OPEN_SETUP_BOOST_MIN_NET_R_LATE: float = float(os.getenv("US_OPEN_SETUP_BOOST_MIN_NET_R_LATE", str(US_OPEN_SETUP_BOOST_MIN_NET_R)))
    US_OPEN_SYMBOL_RECOVERY_ENABLED: bool = os.getenv("US_OPEN_SYMBOL_RECOVERY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    US_OPEN_SYMBOL_RECOVERY_MIN_CONFIDENCE: float = float(os.getenv("US_OPEN_SYMBOL_RECOVERY_MIN_CONFIDENCE", "72"))
    US_OPEN_SYMBOL_RECOVERY_MIN_VOL_RATIO: float = float(os.getenv("US_OPEN_SYMBOL_RECOVERY_MIN_VOL_RATIO", "1.15"))
    US_OPEN_SYMBOL_RECOVERY_MIN_SETUP_WR: float = float(os.getenv("US_OPEN_SYMBOL_RECOVERY_MIN_SETUP_WR", "0.58"))
    US_OPEN_SYMBOL_RECOVERY_MIN_RANK_SCORE: float = float(os.getenv("US_OPEN_SYMBOL_RECOVERY_MIN_RANK_SCORE", "55"))
    US_OPEN_SYMBOL_RECOVERY_MAX_PER_SYMBOL: int = int(os.getenv("US_OPEN_SYMBOL_RECOVERY_MAX_PER_SYMBOL", "1"))
    US_OPEN_SYMBOL_RECOVERY_COOLDOWN_MIN: int = int(os.getenv("US_OPEN_SYMBOL_RECOVERY_COOLDOWN_MIN", "25"))
    XAUUSD_ALERT_COOLDOWN_SEC: int = int(os.getenv("XAUUSD_ALERT_COOLDOWN_SEC", "600"))
    XAUUSD_SCAN_STATUS_NOTIFY_WHEN_AUTO_MONITOR: bool = os.getenv("XAUUSD_SCAN_STATUS_NOTIFY_WHEN_AUTO_MONITOR", "0").strip().lower() in ("1", "true", "yes", "on")
    XAU_EVENT_SHOCK_MODE_ENABLED: bool = os.getenv("XAU_EVENT_SHOCK_MODE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_EVENT_SHOCK_LOOKBACK_HOURS: int = int(os.getenv("XAU_EVENT_SHOCK_LOOKBACK_HOURS", "6"))
    XAU_EVENT_SHOCK_MAX_AGE_MIN: int = int(os.getenv("XAU_EVENT_SHOCK_MAX_AGE_MIN", "180"))
    XAU_EVENT_SHOCK_MIN_SCORE: int = int(os.getenv("XAU_EVENT_SHOCK_MIN_SCORE", "8"))
    XAU_EVENT_SHOCK_MIN_SOURCE_QUALITY: float = float(os.getenv("XAU_EVENT_SHOCK_MIN_SOURCE_QUALITY", "0.75"))
    XAU_EVENT_SHOCK_SIZE_MULT: float = float(os.getenv("XAU_EVENT_SHOCK_SIZE_MULT", "0.45"))
    XAU_EVENT_SHOCK_TP1_RR: float = float(os.getenv("XAU_EVENT_SHOCK_TP1_RR", "0.70"))
    XAU_EVENT_SHOCK_TP2_RR: float = float(os.getenv("XAU_EVENT_SHOCK_TP2_RR", "1.10"))
    XAU_EVENT_SHOCK_TP3_RR: float = float(os.getenv("XAU_EVENT_SHOCK_TP3_RR", "1.60"))
    XAU_EVENT_SHOCK_KILL_SWITCH_SCORE: int = int(os.getenv("XAU_EVENT_SHOCK_KILL_SWITCH_SCORE", "12"))
    XAU_EVENT_SHOCK_KILL_SWITCH_CONFIRMED_ONLY: bool = os.getenv("XAU_EVENT_SHOCK_KILL_SWITCH_CONFIRMED_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_EVENT_SHOCK_KILL_SWITCH_THEMES: str = os.getenv("XAU_EVENT_SHOCK_KILL_SWITCH_THEMES", "GEOPOLITICS,OIL_ENERGY_SHOCK,TARIFF_TRADE")
    # Pre-London Sweep Continuation (PSC) canary family
    XAU_PSC_ENABLED: bool = os.getenv("XAU_PSC_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    XAU_PSC_CTRADER_RISK_USD: float = float(os.getenv("XAU_PSC_CTRADER_RISK_USD", "0.75"))
    XAU_PSC_PRE_LONDON_START_UTC: float = float(os.getenv("XAU_PSC_PRE_LONDON_START_UTC", "22.0"))
    XAU_PSC_PRE_LONDON_END_UTC: float = float(os.getenv("XAU_PSC_PRE_LONDON_END_UTC", "2.5"))
    XAU_PSC_ASIAN_RANGE_MIN: float = float(os.getenv("XAU_PSC_ASIAN_RANGE_MIN", "8.0"))
    XAU_PSC_ASIAN_RANGE_MAX: float = float(os.getenv("XAU_PSC_ASIAN_RANGE_MAX", "60.0"))
    XAU_PSC_SWEEP_MIN_DEPTH: float = float(os.getenv("XAU_PSC_SWEEP_MIN_DEPTH", "5.0"))
    XAU_PSC_SWEEP_MAX_DEPTH: float = float(os.getenv("XAU_PSC_SWEEP_MAX_DEPTH", "30.0"))
    XAU_PSC_RECOVERY_MAX_BARS: int = int(os.getenv("XAU_PSC_RECOVERY_MAX_BARS", "8"))
    XAU_PSC_NO_CHASE_MAX_PIPS: float = float(os.getenv("XAU_PSC_NO_CHASE_MAX_PIPS", "20.0"))
    XAU_PSC_LOOKBACK_SEC: int = int(os.getenv("XAU_PSC_LOOKBACK_SEC", "300"))
    XAU_PSC_MIN_SIGNED_DELTA: float = float(os.getenv("XAU_PSC_MIN_SIGNED_DELTA", "0.02"))
    XAU_PSC_MIN_TICK_UP_RATIO: float = float(os.getenv("XAU_PSC_MIN_TICK_UP_RATIO", "0.48"))
    XAU_PSC_ENTRY_BUFFER: float = float(os.getenv("XAU_PSC_ENTRY_BUFFER", "1.5"))
    XAU_PSC_SL_BUFFER: float = float(os.getenv("XAU_PSC_SL_BUFFER", "3.0"))
    XAU_PSC_TP1_RR: float = float(os.getenv("XAU_PSC_TP1_RR", "0.55"))
    XAU_PSC_TP2_RR: float = float(os.getenv("XAU_PSC_TP2_RR", "1.10"))
    XAU_PSC_TP3_RR: float = float(os.getenv("XAU_PSC_TP3_RR", "1.80"))
    # Scheduled news guard — pre/post calendar event blocking (NFP, FOMC, CPI, etc.)
    XAU_SCHEDULED_NEWS_GUARD_ENABLED: bool = os.getenv("XAU_SCHEDULED_NEWS_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_SCHEDULED_NEWS_GUARD_PRE_MIN: int = int(os.getenv("XAU_SCHEDULED_NEWS_GUARD_PRE_MIN", "30"))
    XAU_SCHEDULED_NEWS_GUARD_POST_MIN: int = int(os.getenv("XAU_SCHEDULED_NEWS_GUARD_POST_MIN", "15"))
    XAU_SCHEDULED_NEWS_GUARD_TIER1_PRE_MIN: int = int(os.getenv("XAU_SCHEDULED_NEWS_GUARD_TIER1_PRE_MIN", "45"))
    XAU_SCHEDULED_NEWS_GUARD_TIER1_POST_MIN: int = int(os.getenv("XAU_SCHEDULED_NEWS_GUARD_TIER1_POST_MIN", "30"))
    XAU_SCHEDULED_NEWS_GUARD_TIER1_EVENTS: str = os.getenv("XAU_SCHEDULED_NEWS_GUARD_TIER1_EVENTS", "")
    XAU_SCHEDULED_NEWS_GUARD_SIZE_MULT: float = float(os.getenv("XAU_SCHEDULED_NEWS_GUARD_SIZE_MULT", "0.50"))
    XAUUSD_SMART_TRAP_GUARD_ENABLED: bool = os.getenv("XAUUSD_SMART_TRAP_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_TRAP_NEAR_ROUND_ATR: float = float(os.getenv("XAUUSD_TRAP_NEAR_ROUND_ATR", "0.35"))
    XAUUSD_TRAP_NO_CHASE_EMA21_ATR: float = float(os.getenv("XAUUSD_TRAP_NO_CHASE_EMA21_ATR", "1.00"))
    XAUUSD_TRAP_NO_CHASE_BB_PCT: float = float(os.getenv("XAUUSD_TRAP_NO_CHASE_BB_PCT", "0.92"))
    XAUUSD_TRAP_REJECTION_WICK_RATIO: float = float(os.getenv("XAUUSD_TRAP_REJECTION_WICK_RATIO", "0.45"))
    XAUUSD_TRAP_REJECTION_M5_LOOKBACK: int = int(os.getenv("XAUUSD_TRAP_REJECTION_M5_LOOKBACK", "36"))
    XAUUSD_TRAP_REJECTION_RECENT_BARS: int = int(os.getenv("XAUUSD_TRAP_REJECTION_RECENT_BARS", "4"))
    XAUUSD_TRAP_EVENT_WINDOW_MIN: int = int(os.getenv("XAUUSD_TRAP_EVENT_WINDOW_MIN", "30"))
    XAUUSD_TRAP_PENALTY_ROUND_RES: float = float(os.getenv("XAUUSD_TRAP_PENALTY_ROUND_RES", "8"))
    XAUUSD_TRAP_PENALTY_NO_CHASE: float = float(os.getenv("XAUUSD_TRAP_PENALTY_NO_CHASE", "10"))
    XAUUSD_TRAP_PENALTY_SWEEP: float = float(os.getenv("XAUUSD_TRAP_PENALTY_SWEEP", "18"))
    XAUUSD_TRAP_PENALTY_EVENT: float = float(os.getenv("XAUUSD_TRAP_PENALTY_EVENT", "12"))
    XAUUSD_TRAP_BLOCK_ON_SWEEP: bool = os.getenv("XAUUSD_TRAP_BLOCK_ON_SWEEP", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_NEWS_FREEZE_ENABLED: bool = os.getenv("XAUUSD_NEWS_FREEZE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_NEWS_FREEZE_WINDOW_MIN: int = int(os.getenv("XAUUSD_NEWS_FREEZE_WINDOW_MIN", "20"))
    XAUUSD_TRAP_BLOCK_ON_NEWS_FREEZE: bool = os.getenv("XAUUSD_TRAP_BLOCK_ON_NEWS_FREEZE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_GUARD_TRANSITION_ALERT_ENABLED: bool = os.getenv("XAU_GUARD_TRANSITION_ALERT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_GUARD_TRANSITION_WATCH_INTERVAL_SEC: int = int(os.getenv("XAU_GUARD_TRANSITION_WATCH_INTERVAL_SEC", "30"))
    XAUUSD_LIQUIDITY_MAP_ENABLED: bool = os.getenv("XAUUSD_LIQUIDITY_MAP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_LIQUIDITY_VP_BINS: int = int(os.getenv("XAUUSD_LIQUIDITY_VP_BINS", "24"))
    XAUUSD_LIQUIDITY_VP_LOOKBACK_H1: int = int(os.getenv("XAUUSD_LIQUIDITY_VP_LOOKBACK_H1", "120"))
    XAUUSD_TRAP_DXY_SHOCK_PCT_15M: float = float(os.getenv("XAUUSD_TRAP_DXY_SHOCK_PCT_15M", "0.18"))
    XAUUSD_TRAP_TNX_SHOCK_BPS_15M: float = float(os.getenv("XAUUSD_TRAP_TNX_SHOCK_BPS_15M", "2.0"))
    XAUUSD_TRAP_PENALTY_MACRO_SHOCK: float = float(os.getenv("XAUUSD_TRAP_PENALTY_MACRO_SHOCK", "10"))
    XAUUSD_TRAP_PENALTY_SWEEP_PROB: float = float(os.getenv("XAUUSD_TRAP_PENALTY_SWEEP_PROB", "8"))
    XAUUSD_TRAP_SWEEP_PROB_BLOCK_SCORE: int = int(os.getenv("XAUUSD_TRAP_SWEEP_PROB_BLOCK_SCORE", "78"))
    XAUUSD_BEHAVIORAL_FALLBACK_ENABLED: bool = os.getenv("XAUUSD_BEHAVIORAL_FALLBACK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_MIN_CONFIDENCE: float = float(os.getenv("XAUUSD_BEHAVIORAL_MIN_CONFIDENCE", "62.0"))
    XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_LONG: float = float(os.getenv("XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_LONG", os.getenv("XAUUSD_BEHAVIORAL_MIN_CONFIDENCE", "62.0")))
    XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_SHORT: float = float(os.getenv("XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_SHORT", os.getenv("XAUUSD_BEHAVIORAL_MIN_CONFIDENCE", "62.0")))
    XAUUSD_BEHAVIORAL_BALANCE_SIDE_THRESHOLDS: bool = os.getenv("XAUUSD_BEHAVIORAL_BALANCE_SIDE_THRESHOLDS", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_MIN_EDGE: float = float(os.getenv("XAUUSD_BEHAVIORAL_MIN_EDGE", "4.0"))
    XAUUSD_BEHAVIORAL_REQUIRE_SWEEP_TRIGGER: bool = os.getenv("XAUUSD_BEHAVIORAL_REQUIRE_SWEEP_TRIGGER", "0").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_TRIGGER_BB_PCTL_MAX: float = float(os.getenv("XAUUSD_BEHAVIORAL_TRIGGER_BB_PCTL_MAX", "0.25"))
    XAUUSD_BEHAVIORAL_TRIGGER_TR_ATR_MAX: float = float(os.getenv("XAUUSD_BEHAVIORAL_TRIGGER_TR_ATR_MAX", "0.85"))
    XAUUSD_BEHAVIORAL_EDGE_TRIGGER_ENABLED: bool = os.getenv("XAUUSD_BEHAVIORAL_EDGE_TRIGGER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_EDGE_TRIGGER_CONFIDENCE: float = float(os.getenv("XAUUSD_BEHAVIORAL_EDGE_TRIGGER_CONFIDENCE", "68.0"))
    XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_EDGE: float = float(os.getenv("XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_EDGE", "3.0"))
    XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_TREND_VOTES: int = int(os.getenv("XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_TREND_VOTES", "1"))
    XAUUSD_BEHAVIORAL_MIN_RR: float = float(os.getenv("XAUUSD_BEHAVIORAL_MIN_RR", "2.0"))
    XAUUSD_BEHAVIORAL_ENTRY_BUFFER_ATR_M5: float = float(os.getenv("XAUUSD_BEHAVIORAL_ENTRY_BUFFER_ATR_M5", "0.10"))
    XAUUSD_REGIME_GUARD_ENABLED: bool = os.getenv("XAUUSD_REGIME_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_REGIME_GUARD_REQUIRE_STRUCTURE_ALIGN: bool = os.getenv("XAUUSD_REGIME_GUARD_REQUIRE_STRUCTURE_ALIGN", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_REVERSAL_TRIGGER_ENABLED: bool = os.getenv("XAUUSD_BEHAVIORAL_REVERSAL_TRIGGER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_BEHAVIORAL_REVERSAL_MIN_EXTENSION_ATR: float = float(os.getenv("XAUUSD_BEHAVIORAL_REVERSAL_MIN_EXTENSION_ATR", "0.45"))
    XAUUSD_BEHAVIORAL_REVERSAL_RSI_LONG_MIN: float = float(os.getenv("XAUUSD_BEHAVIORAL_REVERSAL_RSI_LONG_MIN", "49.5"))
    XAUUSD_BEHAVIORAL_REVERSAL_RSI_SHORT_MAX: float = float(os.getenv("XAUUSD_BEHAVIORAL_REVERSAL_RSI_SHORT_MAX", "50.5"))

    # ── XAUUSD 1M/5M Behavior Scalping ───────────────────────────────────────
    XAUUSD_SCALP_ENABLED: bool = os.getenv("XAUUSD_SCALP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_SCALP_SCAN_INTERVAL: int = int(os.getenv("XAUUSD_SCALP_SCAN_INTERVAL", "60"))     # seconds (every 1 min)
    XAUUSD_SCALP_MIN_CONFIDENCE: float = float(os.getenv("XAUUSD_SCALP_MIN_CONFIDENCE", "58.0"))
    XAUUSD_SCALP_REQUIRE_KILL_ZONE: bool = os.getenv("XAUUSD_SCALP_REQUIRE_KILL_ZONE", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_SCALP_REQUIRE_M1_TRIGGER: bool = os.getenv("XAUUSD_SCALP_REQUIRE_M1_TRIGGER", "1").strip().lower() in ("1", "true", "yes", "on")
    XAUUSD_SCALP_ALERT_COOLDOWN_SEC: int = int(os.getenv("XAUUSD_SCALP_ALERT_COOLDOWN_SEC", "300"))  # 5 min cooldown
    XAUUSD_SCALP_FVG_LOOKBACK: int = int(os.getenv("XAUUSD_SCALP_FVG_LOOKBACK", "25"))

    # ── Signal Simulator ──────────────────────────────────────────────────────
    SIM_ENABLED: bool = os.getenv("SIM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SIM_MAX_PENDING_HOURS: float = float(os.getenv("SIM_MAX_PENDING_HOURS", "24.0"))
    SIM_CHECK_INTERVAL_SEC: int = int(os.getenv("SIM_CHECK_INTERVAL_SEC", "60"))
    SIM_RISK_USD_PER_SIGNAL: float = float(os.getenv("SIM_RISK_USD_PER_SIGNAL", "10.0"))
    SIGNAL_OUTCOME_NOTIFY_ENABLED: bool = os.getenv("SIGNAL_OUTCOME_NOTIFY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SIGNAL_OUTCOME_INITIAL_BALANCE_USD: float = float(os.getenv("SIGNAL_OUTCOME_INITIAL_BALANCE_USD", "1000.0"))

    # ── MT5 Execution Bridge (RPyC -> Windows MT5 host) ─────────────────────
    MT5_ENABLED: bool = os.getenv("MT5_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_DRY_RUN: bool = os.getenv("MT5_DRY_RUN", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_HOST: str = os.getenv("MT5_HOST", "127.0.0.1")
    MT5_PORT: int = int(os.getenv("MT5_PORT", "18812"))
    MT5_MAGIC: int = int(os.getenv("MT5_MAGIC", "770100"))
    MT5_DEVIATION: int = int(os.getenv("MT5_DEVIATION", "20"))
    MT5_LOT_SIZE: float = float(os.getenv("MT5_LOT_SIZE", "0.01"))
    MT5_MIN_SIGNAL_CONFIDENCE: int = int(os.getenv("MT5_MIN_SIGNAL_CONFIDENCE", "75"))
    MT5_MIN_SIGNAL_CONFIDENCE_FX: float = float(os.getenv("MT5_MIN_SIGNAL_CONFIDENCE_FX", os.getenv("MT5_MIN_SIGNAL_CONFIDENCE", "75")))
    MT5_MIN_SIGNAL_CONFIDENCE_SYMBOL_OVERRIDES: str = os.getenv("MT5_MIN_SIGNAL_CONFIDENCE_SYMBOL_OVERRIDES", "")
    MT5_MAX_SIGNALS_PER_SCAN: int = int(os.getenv("MT5_MAX_SIGNALS_PER_SCAN", "1"))
    MT5_MAX_ATTEMPTS_PER_SCAN: int = int(os.getenv("MT5_MAX_ATTEMPTS_PER_SCAN", "3"))
    MT5_MAX_OPEN_POSITIONS: int = int(os.getenv("MT5_MAX_OPEN_POSITIONS", "5"))
    MT5_MAX_POSITIONS_PER_SYMBOL: int = int(os.getenv("MT5_MAX_POSITIONS_PER_SYMBOL", "1"))
    MT5_MAX_MARGIN_USAGE_PCT: float = float(os.getenv("MT5_MAX_MARGIN_USAGE_PCT", "35"))
    MT5_MAX_MARGIN_USAGE_PCT_FX: float = float(os.getenv("MT5_MAX_MARGIN_USAGE_PCT_FX", os.getenv("MT5_MAX_MARGIN_USAGE_PCT", "35")))
    MT5_MAX_MARGIN_USAGE_PCT_SYMBOL_OVERRIDES: str = os.getenv("MT5_MAX_MARGIN_USAGE_PCT_SYMBOL_OVERRIDES", "")
    MT5_RISK_MULTIPLIER_SYMBOL_OVERRIDES: str = os.getenv("MT5_RISK_MULTIPLIER_SYMBOL_OVERRIDES", "")
    MT5_RISK_MULTIPLIER_MIN_SYMBOL_OVERRIDES: str = os.getenv("MT5_RISK_MULTIPLIER_MIN_SYMBOL_OVERRIDES", "")
    MT5_RISK_MULTIPLIER_MAX_SYMBOL_OVERRIDES: str = os.getenv("MT5_RISK_MULTIPLIER_MAX_SYMBOL_OVERRIDES", "")
    MT5_CANARY_FORCE_SYMBOL_OVERRIDES: str = os.getenv("MT5_CANARY_FORCE_SYMBOL_OVERRIDES", "")
    MT5_PENDING_ENTRY_ENABLED: bool = os.getenv("MT5_PENDING_ENTRY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PENDING_ENTRY_DEFAULT_MODE: str = os.getenv("MT5_PENDING_ENTRY_DEFAULT_MODE", "auto")
    MT5_PENDING_ENTRY_MODE_SYMBOL_OVERRIDES: str = os.getenv("MT5_PENDING_ENTRY_MODE_SYMBOL_OVERRIDES", "")
    MT5_PENDING_ENTRY_MIN_ADV_ATR: float = float(os.getenv("MT5_PENDING_ENTRY_MIN_ADV_ATR", "0.08"))
    MT5_PENDING_ENTRY_MAX_DIST_ATR: float = float(os.getenv("MT5_PENDING_ENTRY_MAX_DIST_ATR", "1.25"))
    MT5_PENDING_ENTRY_MIN_ADV_ATR_SYMBOL_OVERRIDES: str = os.getenv("MT5_PENDING_ENTRY_MIN_ADV_ATR_SYMBOL_OVERRIDES", "")
    MT5_PENDING_ENTRY_MAX_DIST_ATR_SYMBOL_OVERRIDES: str = os.getenv("MT5_PENDING_ENTRY_MAX_DIST_ATR_SYMBOL_OVERRIDES", "")
    MT5_MIN_FREE_MARGIN_AFTER_TRADE: float = float(os.getenv("MT5_MIN_FREE_MARGIN_AFTER_TRADE", "1"))
    MT5_COMMENT_PREFIX: str = os.getenv("MT5_COMMENT_PREFIX", "DEXTER")
    MT5_NOTIFY_EXECUTED: bool = os.getenv("MT5_NOTIFY_EXECUTED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_NOTIFY_FAILED: bool = os.getenv("MT5_NOTIFY_FAILED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_ENABLED: bool = os.getenv("MT5_BYPASS_TEST_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_SOURCES: str = os.getenv("MT5_BYPASS_TEST_SOURCES", "scalp_xauusd")
    MT5_BYPASS_TEST_SYMBOLS: str = os.getenv("MT5_BYPASS_TEST_SYMBOLS", "XAUUSD")
    MT5_BYPASS_TEST_SOURCE_SUFFIX: str = os.getenv("MT5_BYPASS_TEST_SOURCE_SUFFIX", "bypass")
    MT5_BYPASS_TEST_SKIP_NEURAL_FILTER: bool = os.getenv("MT5_BYPASS_TEST_SKIP_NEURAL_FILTER", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_SKIP_RISK_GOVERNOR: bool = os.getenv("MT5_BYPASS_TEST_SKIP_RISK_GOVERNOR", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_SKIP_MT5_CONFIDENCE: bool = os.getenv("MT5_BYPASS_TEST_SKIP_MT5_CONFIDENCE", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_IGNORE_OPEN_POSITIONS: bool = os.getenv("MT5_BYPASS_TEST_IGNORE_OPEN_POSITIONS", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_MAGIC_OFFSET: int = int(os.getenv("MT5_BYPASS_TEST_MAGIC_OFFSET", "500"))
    MT5_BYPASS_TEST_QUICK_TP_ENABLED: bool = os.getenv("MT5_BYPASS_TEST_QUICK_TP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_QUICK_TP_NOTIFY_TELEGRAM: bool = os.getenv("MT5_BYPASS_TEST_QUICK_TP_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_BYPASS_TEST_QUICK_TP_BALANCE_PCT: float = float(os.getenv("MT5_BYPASS_TEST_QUICK_TP_BALANCE_PCT", "1.0"))
    MT5_BYPASS_TEST_QUICK_TP_MIN_USD: float = float(os.getenv("MT5_BYPASS_TEST_QUICK_TP_MIN_USD", "1.0"))
    MT5_BYPASS_TEST_QUICK_TP_INTERVAL_SEC: int = int(os.getenv("MT5_BYPASS_TEST_QUICK_TP_INTERVAL_SEC", "20"))
    MT5_BEST_LANE_ENABLED: bool = os.getenv("MT5_BEST_LANE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_BEST_LANE_TAG: str = os.getenv("MT5_BEST_LANE_TAG", "winner")
    MT5_BEST_LANE_SOURCES: str = os.getenv("MT5_BEST_LANE_SOURCES", "xauusd,scalp_xauusd")
    MT5_BEST_LANE_SYMBOLS: str = os.getenv("MT5_BEST_LANE_SYMBOLS", "XAUUSD")
    MT5_BEST_LANE_MIN_CONFIDENCE: float = float(os.getenv("MT5_BEST_LANE_MIN_CONFIDENCE", "72.0"))
    MT5_BEST_LANE_MIN_CONFIDENCE_SYMBOL_OVERRIDES: str = os.getenv("MT5_BEST_LANE_MIN_CONFIDENCE_SYMBOL_OVERRIDES", "")
    MT5_XAU_SCHEDULED_LIVE_ENABLED: bool = os.getenv("MT5_XAU_SCHEDULED_LIVE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY: bool = os.getenv("MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE: float = float(os.getenv("MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", "78.0"))
    MT5_XAU_SCHEDULED_LIVE_SESSIONS: str = os.getenv("MT5_XAU_SCHEDULED_LIVE_SESSIONS", "new_york")
    MT5_XAU_SCHEDULED_LIVE_TIMEFRAMES: str = os.getenv("MT5_XAU_SCHEDULED_LIVE_TIMEFRAMES", "1h")
    MT5_XAU_SCHEDULED_LIVE_NOTIFY_REJECTED: bool = os.getenv("MT5_XAU_SCHEDULED_LIVE_NOTIFY_REJECTED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_SCALP_XAU_LIVE_FILTER_ENABLED: bool = os.getenv("MT5_SCALP_XAU_LIVE_FILTER_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_SCALP_XAU_LIVE_SESSIONS: str = os.getenv("MT5_SCALP_XAU_LIVE_SESSIONS", "new_york|london,new_york,overlap")
    MT5_SCALP_XAU_LIVE_CONF_MIN: float = float(os.getenv("MT5_SCALP_XAU_LIVE_CONF_MIN", "72.0"))
    MT5_SCALP_XAU_LIVE_CONF_MAX: float = float(os.getenv("MT5_SCALP_XAU_LIVE_CONF_MAX", "75.0"))
    MT5_EXECUTE_XAUUSD: bool = os.getenv("MT5_EXECUTE_XAUUSD", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_EXECUTE_CRYPTO: bool = os.getenv("MT5_EXECUTE_CRYPTO", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_EXECUTE_FX: bool = os.getenv("MT5_EXECUTE_FX", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_EXECUTE_STOCKS: bool = os.getenv("MT5_EXECUTE_STOCKS", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_AVOID_DUPLICATE_DIRECTION: bool = os.getenv("MT5_AVOID_DUPLICATE_DIRECTION", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_MICRO_MODE_ENABLED: bool = os.getenv("MT5_MICRO_MODE_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_MICRO_SINGLE_POSITION_ONLY: bool = os.getenv("MT5_MICRO_SINGLE_POSITION_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_POSITION_LIMITS_BOT_ONLY: bool = os.getenv("MT5_POSITION_LIMITS_BOT_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_MICRO_MAX_SPREAD_PCT: float = float(os.getenv("MT5_MICRO_MAX_SPREAD_PCT", "0.15"))
    MT5_MICRO_WHITELIST_LEARNER_ENABLED: bool = os.getenv("MT5_MICRO_WHITELIST_LEARNER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_MICRO_WHITELIST_PATH: str = os.getenv("MT5_MICRO_WHITELIST_PATH", "")
    MT5_MICRO_WHITELIST_TTL_HOURS: int = int(os.getenv("MT5_MICRO_WHITELIST_TTL_HOURS", "24"))
    MT5_MICRO_BALANCE_BUCKET_USD: float = float(os.getenv("MT5_MICRO_BALANCE_BUCKET_USD", "2.0"))
    MT5_AUTOPILOT_ENABLED: bool = os.getenv("MT5_AUTOPILOT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_AUTOPILOT_DB_PATH: str = os.getenv("MT5_AUTOPILOT_DB_PATH", "")
    MT5_AUTOPILOT_SYNC_INTERVAL_MIN: int = int(os.getenv("MT5_AUTOPILOT_SYNC_INTERVAL_MIN", "15"))
    MT5_AUTOPILOT_GATE_CACHE_SEC: int = int(os.getenv("MT5_AUTOPILOT_GATE_CACHE_SEC", "5"))
    MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD: float = float(os.getenv("MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD", "2.0"))
    MT5_RISK_GOV_DAILY_LOSS_LIMIT_PCT: float = float(os.getenv("MT5_RISK_GOV_DAILY_LOSS_LIMIT_PCT", "15.0"))
    MT5_RISK_GOV_MAX_CONSECUTIVE_LOSSES: int = int(os.getenv("MT5_RISK_GOV_MAX_CONSECUTIVE_LOSSES", "2"))
    MT5_RISK_GOV_LOSS_COOLDOWN_MIN: int = int(os.getenv("MT5_RISK_GOV_LOSS_COOLDOWN_MIN", "30"))
    MT5_RISK_GOV_MAX_REJECTIONS_1H: int = int(os.getenv("MT5_RISK_GOV_MAX_REJECTIONS_1H", "5"))
    MT5_RISK_GOV_LANE_AWARE_ENABLED: bool = os.getenv("MT5_RISK_GOV_LANE_AWARE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD_LANE_OVERRIDES: str = os.getenv("MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD_LANE_OVERRIDES", "")
    MT5_RISK_GOV_DAILY_LOSS_LIMIT_PCT_LANE_OVERRIDES: str = os.getenv("MT5_RISK_GOV_DAILY_LOSS_LIMIT_PCT_LANE_OVERRIDES", "")
    MT5_RISK_GOV_MAX_CONSECUTIVE_LOSSES_LANE_OVERRIDES: str = os.getenv("MT5_RISK_GOV_MAX_CONSECUTIVE_LOSSES_LANE_OVERRIDES", "")
    MT5_RISK_GOV_LOSS_COOLDOWN_MIN_LANE_OVERRIDES: str = os.getenv("MT5_RISK_GOV_LOSS_COOLDOWN_MIN_LANE_OVERRIDES", "")
    MT5_RISK_GOV_MAX_REJECTIONS_1H_LANE_OVERRIDES: str = os.getenv("MT5_RISK_GOV_MAX_REJECTIONS_1H_LANE_OVERRIDES", "")
    MT5_PRE_CLOSE_FLATTEN_ENABLED: bool = os.getenv("MT5_PRE_CLOSE_FLATTEN_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_PRE_CLOSE_FLATTEN_CHECK_INTERVAL_MIN: int = int(os.getenv("MT5_PRE_CLOSE_FLATTEN_CHECK_INTERVAL_MIN", "5"))
    MT5_PRE_CLOSE_FLATTEN_FRI_ONLY: bool = os.getenv("MT5_PRE_CLOSE_FLATTEN_FRI_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PRE_CLOSE_FLATTEN_NY_HOUR: int = int(os.getenv("MT5_PRE_CLOSE_FLATTEN_NY_HOUR", "16"))
    MT5_PRE_CLOSE_FLATTEN_NY_MINUTE: int = int(os.getenv("MT5_PRE_CLOSE_FLATTEN_NY_MINUTE", "50"))
    MT5_PRE_CLOSE_FLATTEN_WINDOW_MIN: int = int(os.getenv("MT5_PRE_CLOSE_FLATTEN_WINDOW_MIN", "20"))
    MT5_PRE_CLOSE_FLATTEN_EXCLUDE_SYMBOLS: str = os.getenv("MT5_PRE_CLOSE_FLATTEN_EXCLUDE_SYMBOLS", "BTCUSD,ETHUSD")
    MT5_PRE_CLOSE_FLATTEN_INCLUDE_SYMBOLS: str = os.getenv("MT5_PRE_CLOSE_FLATTEN_INCLUDE_SYMBOLS", "")
    MT5_REPEAT_ERROR_GUARD_ENABLED: bool = os.getenv("MT5_REPEAT_ERROR_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_REPEAT_ERROR_GUARD_WINDOW_MIN: int = int(os.getenv("MT5_REPEAT_ERROR_GUARD_WINDOW_MIN", "30"))
    MT5_REPEAT_ERROR_GUARD_MAX_HITS: int = int(os.getenv("MT5_REPEAT_ERROR_GUARD_MAX_HITS", "2"))
    MT5_REPEAT_ERROR_GUARD_LOCK_MIN: int = int(os.getenv("MT5_REPEAT_ERROR_GUARD_LOCK_MIN", "45"))
    MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED: bool = os.getenv("MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_REPEAT_ERROR_GUARD_PATH: str = os.getenv("MT5_REPEAT_ERROR_GUARD_PATH", "")
    MT5_ORCHESTRATOR_DB_PATH: str = os.getenv("MT5_ORCHESTRATOR_DB_PATH", "")
    MT5_WF_TRAIN_DAYS: int = int(os.getenv("MT5_WF_TRAIN_DAYS", "30"))
    MT5_WF_FORWARD_DAYS: int = int(os.getenv("MT5_WF_FORWARD_DAYS", "7"))
    MT5_WF_MIN_TRAIN_TRADES: int = int(os.getenv("MT5_WF_MIN_TRAIN_TRADES", "8"))
    MT5_WF_MIN_FORWARD_TRADES: int = int(os.getenv("MT5_WF_MIN_FORWARD_TRADES", "5"))
    MT5_WF_MIN_FORWARD_WIN_RATE: float = float(os.getenv("MT5_WF_MIN_FORWARD_WIN_RATE", "0.45"))
    MT5_WF_MAX_FORWARD_MAE: float = float(os.getenv("MT5_WF_MAX_FORWARD_MAE", "0.45"))
    MT5_ADAPTIVE_SIZING_ENABLED: bool = os.getenv("MT5_ADAPTIVE_SIZING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_SIZING_MIN_MULT: float = float(os.getenv("MT5_ADAPTIVE_SIZING_MIN_MULT", "0.25"))
    MT5_ADAPTIVE_SIZING_MAX_MULT: float = float(os.getenv("MT5_ADAPTIVE_SIZING_MAX_MULT", "1.00"))
    MT5_ADAPTIVE_SIZING_CANARY_MULT: float = float(os.getenv("MT5_ADAPTIVE_SIZING_CANARY_MULT", "0.35"))
    MT5_ADAPTIVE_SIZING_TARGET_WIN_RATE: float = float(os.getenv("MT5_ADAPTIVE_SIZING_TARGET_WIN_RATE", "0.52"))
    MT5_ADAPTIVE_SIZING_TARGET_MAE: float = float(os.getenv("MT5_ADAPTIVE_SIZING_TARGET_MAE", "0.35"))
    MT5_ADAPTIVE_EXECUTION_ENABLED: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_LOOKBACK_DAYS: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_LOOKBACK_DAYS", "45"))
    MT5_ADAPTIVE_EXECUTION_MIN_SYMBOL_TRADES: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_MIN_SYMBOL_TRADES", "6"))
    MT5_ADAPTIVE_EXECUTION_RR_MIN: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_RR_MIN", "1.2"))
    MT5_ADAPTIVE_EXECUTION_RR_MAX: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_RR_MAX", "2.8"))
    MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MIN: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MIN", "0.85"))
    MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MAX: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MAX", "1.35"))
    MT5_ADAPTIVE_EXECUTION_SIZE_MIN: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_SIZE_MIN", "0.70"))
    MT5_ADAPTIVE_EXECUTION_SIZE_MAX: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_SIZE_MAX", "1.10"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_ENABLED: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_LOOKBACK_DAYS: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_LOOKBACK_DAYS", "30"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TRADES: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TRADES", "10"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TOKEN_SAMPLES: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TOKEN_SAMPLES", "5"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_RR_ADJ: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_RR_ADJ", "0.12"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_SIZE_ADJ: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_SIZE_ADJ", "0.22"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_TARGET_WIN_RATE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_TARGET_WIN_RATE", "0.50"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_SOURCE_STRICT: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_SOURCE_STRICT", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_USE_VOLUME_PROFILE: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_USE_VOLUME_PROFILE", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_EDGE_SOFT_BLOCK: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_EDGE_SOFT_BLOCK", "-0.42"))
    MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_SOFT_SIZE_CAP: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_SOFT_SIZE_CAP", "0.62"))
    MT5_ADAPTIVE_EXECUTION_DIRECTIONAL_BIAS_ENABLED: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTIONAL_BIAS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_LOOKBACK_DAYS: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_LOOKBACK_DAYS", "30"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_MIN_SAMPLES: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_MIN_SAMPLES", "4"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_TARGET_WIN_RATE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_TARGET_WIN_RATE", "0.45"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_RR_PENALTY: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_RR_PENALTY", "0.18"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_SIZE_PENALTY: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_SIZE_PENALTY", "0.35"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_ENABLED: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_SOURCES: str = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_SOURCES", "scalp_xauusd")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MIN_SAMPLES: int = int(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MIN_SAMPLES", "4"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_WIN_RATE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_WIN_RATE", "0.35"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_AVG_PNL: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_AVG_PNL", "-5.0"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_CONFIDENCE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_CONFIDENCE", "84"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_NEURAL_PROB: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_NEURAL_PROB", "0.70"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_HOURS_UTC: str = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_HOURS_UTC", "12-20")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_OUTSIDE_WINDOW_SOFT_ONLY: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_OUTSIDE_WINDOW_SOFT_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_RR_PENALTY: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_RR_PENALTY", "0.10"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_SIZE_PENALTY: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_SIZE_PENALTY", "0.22"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_NEWS_RESCUE_ENABLED: bool = os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_NEWS_RESCUE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MAX_SHOCK_SCORE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MAX_SHOCK_SCORE", "7.0"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_CONFIDENCE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_CONFIDENCE", "78"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_BEHAVIOR_EDGE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_BEHAVIOR_EDGE", "0.12"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_CONFIDENCE: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_CONFIDENCE", "80"))
    MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_NEURAL_PROB: float = float(os.getenv("MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_NEURAL_PROB", "0.64"))
    MT5_EXIT_DYNAMIC_TP_SPREAD_PAD: bool = os.getenv("MT5_EXIT_DYNAMIC_TP_SPREAD_PAD", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_EXIT_DYNAMIC_TP_COMM_PIPS: float = float(os.getenv("MT5_EXIT_DYNAMIC_TP_COMM_PIPS", "0.5"))
    MT5_POSITION_MANAGER_ENABLED: bool = os.getenv("MT5_POSITION_MANAGER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_POSITION_MANAGER_DB_PATH: str = os.getenv("MT5_POSITION_MANAGER_DB_PATH", "")
    MT5_POSITION_MANAGER_INTERVAL_MIN: int = int(os.getenv("MT5_POSITION_MANAGER_INTERVAL_MIN", "1"))
    MT5_PM_MANAGE_ENABLED: bool = os.getenv("MT5_PM_MANAGE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_MANAGE_MANUAL_POSITIONS: bool = os.getenv("MT5_PM_MANAGE_MANUAL_POSITIONS", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_BREAK_EVEN_R: float = float(os.getenv("MT5_PM_BREAK_EVEN_R", "0.8"))
    MT5_PM_TRAIL_START_R: float = float(os.getenv("MT5_PM_TRAIL_START_R", "1.2"))
    MT5_PM_TRAIL_GAP_R: float = float(os.getenv("MT5_PM_TRAIL_GAP_R", "0.6"))
    MT5_PM_TRAIL_DYNAMIC_ENABLED: bool = os.getenv("MT5_PM_TRAIL_DYNAMIC_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_TRAIL_DYNAMIC_STEP_R: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_STEP_R", "0.8"))
    MT5_PM_TRAIL_DYNAMIC_TIGHTEN_PCT_PER_STEP: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_TIGHTEN_PCT_PER_STEP", "0.12"))
    MT5_PM_TRAIL_DYNAMIC_MAX_TIGHTEN_PCT: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_MAX_TIGHTEN_PCT", "0.35"))
    MT5_PM_TRAIL_DYNAMIC_SPREAD_WIDEN_PCT: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_SPREAD_WIDEN_PCT", "0.18"))
    MT5_PM_TRAIL_DYNAMIC_MAX_WIDEN_PCT: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_MAX_WIDEN_PCT", "0.24"))
    MT5_PM_TRAIL_DYNAMIC_YOUNG_AGE_MIN: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_YOUNG_AGE_MIN", "6"))
    MT5_PM_TRAIL_DYNAMIC_YOUNG_WIDEN_PCT: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_YOUNG_WIDEN_PCT", "0.10"))
    MT5_PM_TRAIL_DYNAMIC_MIN_GAP_R: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_MIN_GAP_R", "0.28"))
    MT5_PM_TRAIL_DYNAMIC_MAX_GAP_R: float = float(os.getenv("MT5_PM_TRAIL_DYNAMIC_MAX_GAP_R", "1.10"))
    MT5_PM_PARTIAL_TP_R: float = float(os.getenv("MT5_PM_PARTIAL_TP_R", "1.0"))
    MT5_PM_PARTIAL_CLOSE_PCT: float = float(os.getenv("MT5_PM_PARTIAL_CLOSE_PCT", "0.5"))
    MT5_PM_MIN_PARTIAL_VOLUME: float = float(os.getenv("MT5_PM_MIN_PARTIAL_VOLUME", "0.01"))
    MT5_PM_FORCE_BE_AFTER_PARTIAL: bool = os.getenv("MT5_PM_FORCE_BE_AFTER_PARTIAL", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_FORCE_BE_AFTER_PARTIAL_BUFFER_R: float = float(os.getenv("MT5_PM_FORCE_BE_AFTER_PARTIAL_BUFFER_R", "0.05"))
    MT5_PM_TIME_STOP_MIN: int = int(os.getenv("MT5_PM_TIME_STOP_MIN", "120"))
    MT5_PM_TIME_STOP_FLAT_R: float = float(os.getenv("MT5_PM_TIME_STOP_FLAT_R", "0.25"))
    MT5_PM_MAX_ACTIONS_PER_CYCLE: int = int(os.getenv("MT5_PM_MAX_ACTIONS_PER_CYCLE", "3"))
    MT5_PM_NOTIFY_ACTIONS: bool = os.getenv("MT5_PM_NOTIFY_ACTIONS", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_EARLY_RISK_PROTECT_ENABLED: bool = os.getenv("MT5_PM_EARLY_RISK_PROTECT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_EARLY_RISK_TRIGGER_R: float = float(os.getenv("MT5_PM_EARLY_RISK_TRIGGER_R", "-0.80"))
    MT5_PM_EARLY_RISK_SL_R: float = float(os.getenv("MT5_PM_EARLY_RISK_SL_R", "-0.92"))
    MT5_PM_EARLY_RISK_BUFFER_R: float = float(os.getenv("MT5_PM_EARLY_RISK_BUFFER_R", "0.05"))
    MT5_PM_SPREAD_SPIKE_PROTECT_ENABLED: bool = os.getenv("MT5_PM_SPREAD_SPIKE_PROTECT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_SPREAD_SPIKE_PCT: float = float(os.getenv("MT5_PM_SPREAD_SPIKE_PCT", "0.18"))
    MT5_PM_ADAPTIVE_ENABLED: bool = os.getenv("MT5_PM_ADAPTIVE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_ADAPTIVE_LOOKBACK_DAYS: int = int(os.getenv("MT5_PM_ADAPTIVE_LOOKBACK_DAYS", "45"))
    MT5_PM_ADAPTIVE_MIN_SYMBOL_TRADES: int = int(os.getenv("MT5_PM_ADAPTIVE_MIN_SYMBOL_TRADES", "6"))
    MT5_PM_LEARNING_ENABLED: bool = os.getenv("MT5_PM_LEARNING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_PM_LEARNING_LOOKBACK_DAYS: int = int(os.getenv("MT5_PM_LEARNING_LOOKBACK_DAYS", "60"))
    MT5_PM_LEARNING_MIN_ACTIONS: int = int(os.getenv("MT5_PM_LEARNING_MIN_ACTIONS", "8"))
    MT5_PM_LEARNING_SYNC_HOURS: int = int(os.getenv("MT5_PM_LEARNING_SYNC_HOURS", "168"))
    MT5_PM_LEARNING_MAX_CLOSED_ROWS: int = int(os.getenv("MT5_PM_LEARNING_MAX_CLOSED_ROWS", "400"))
    MT5_SYMBOL_MAP: str = os.getenv("MT5_SYMBOL_MAP", "")  # e.g. XAUUSD=XAUUSDm,BTC/USDT=BTCUSD
    MT5_ALLOW_SYMBOLS: str = os.getenv("MT5_ALLOW_SYMBOLS", "")  # comma-separated broker symbols
    MT5_BLOCK_SYMBOLS: str = os.getenv("MT5_BLOCK_SYMBOLS", "")  # comma-separated broker symbols

    # ── Entry Confirmation Gates (M5 + HTF/LTF) ───────────────────────────────
    MT5_M5_CONFIRM_ENABLED: bool = os.getenv("MT5_M5_CONFIRM_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_M5_CONFIRM_APPLY_TO: str = os.getenv("MT5_M5_CONFIRM_APPLY_TO", "fx,gold")
    MT5_M5_CONFIRM_MAX_ATR_DIST: float = float(os.getenv("MT5_M5_CONFIRM_MAX_ATR_DIST", "1.0"))
    MT5_M5_CONFIRM_STRICT: bool = os.getenv("MT5_M5_CONFIRM_STRICT", "0").strip().lower() in ("1", "true", "yes", "on")

    MT5_HTF_LTF_FILTER_ENABLED: bool = os.getenv("MT5_HTF_LTF_FILTER_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_HTF_LTF_FILTER_APPLY_TO: str = os.getenv("MT5_HTF_LTF_FILTER_APPLY_TO", "fx,gold")
    MT5_HTF_LTF_HARD_BLOCK: bool = os.getenv("MT5_HTF_LTF_HARD_BLOCK", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_HTF_LTF_SOFT_SIZE_PENALTY: float = float(os.getenv("MT5_HTF_LTF_SOFT_SIZE_PENALTY", "0.80"))

    # ── Smart Limit Order Timeout & Invalidations ─────────────────────────────
    MT5_LIMIT_ENTRY_ENABLED: bool = os.getenv("MT5_LIMIT_ENTRY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_LIMIT_ADAPTIVE_EXITS_SIZE_ONLY: bool = os.getenv("MT5_LIMIT_ADAPTIVE_EXITS_SIZE_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK: bool = os.getenv("MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", "0").strip().lower() in ("1", "true", "yes", "on")
    MT5_LIMIT_FALLBACK_MIN_CONFIDENCE: float = float(os.getenv("MT5_LIMIT_FALLBACK_MIN_CONFIDENCE", "82.0"))
    MT5_LIMIT_FALLBACK_MAX_SPREAD_PCT: float = float(os.getenv("MT5_LIMIT_FALLBACK_MAX_SPREAD_PCT", "0.03"))
    MT5_LIMIT_FALLBACK_MAX_SLIPPAGE_ATR: float = float(os.getenv("MT5_LIMIT_FALLBACK_MAX_SLIPPAGE_ATR", "0.20"))
    MT5_LIMIT_TIMEOUT_MINS: int = int(os.getenv("MT5_LIMIT_TIMEOUT_MINS", "60"))
    MT5_LIMIT_CANCEL_ON_TP_FRONT_RUN: bool = os.getenv("MT5_LIMIT_CANCEL_ON_TP_FRONT_RUN", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_LIMIT_CANCEL_ON_SL_BREAK: bool = os.getenv("MT5_LIMIT_CANCEL_ON_SL_BREAK", "1").strip().lower() in ("1", "true", "yes", "on")


    # ── Signal Thresholds ──────────────────────────────────────────────────────
    MIN_SIGNAL_CONFIDENCE:  int = int(os.getenv("MIN_SIGNAL_CONFIDENCE", "63"))
    STOCK_MIN_CONFIDENCE:   int = int(os.getenv("STOCK_MIN_CONFIDENCE",  "70"))
    STOCK_MAX_RESULTS:      int = int(os.getenv("STOCK_MAX_RESULTS",     "5"))
    TOP_COINS_COUNT:        int = int(os.getenv("TOP_COINS_COUNT",       "50"))
    CRYPTO_AUTO_FOCUS_ONLY: bool = os.getenv("CRYPTO_AUTO_FOCUS_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_AUTO_FOCUS_SYMBOLS: str = os.getenv("CRYPTO_AUTO_FOCUS_SYMBOLS", "BTC/USDT,ETH/USDT,BTCUSD,ETHUSD")
    CRYPTO_AUTO_FOCUS_NO_SIGNAL_REPORT: bool = os.getenv("CRYPTO_AUTO_FOCUS_NO_SIGNAL_REPORT", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_AUTO_FOCUS_NO_SIGNAL_INTERVAL_MIN: int = int(os.getenv("CRYPTO_AUTO_FOCUS_NO_SIGNAL_INTERVAL_MIN", "5"))
    FX_TOP_N:             int = int(os.getenv("FX_TOP_N", "5"))
    FX_MIN_CONFIDENCE:    int = int(os.getenv("FX_MIN_CONFIDENCE", str(MIN_SIGNAL_CONFIDENCE)))
    STOCK_MIN_VOL_RATIO:    float = float(os.getenv("STOCK_MIN_VOL_RATIO", "0.9"))
    STOCK_MIN_EDGE:         float = float(os.getenv("STOCK_MIN_EDGE", "15"))
    STOCK_MIN_MOMENTUM_RSI: float = float(os.getenv("STOCK_MIN_MOMENTUM_RSI", "53"))
    US_OPEN_MIN_VOL_RATIO:  float = float(os.getenv("US_OPEN_MIN_VOL_RATIO", "1.0"))
    US_OPEN_MIN_DOLLAR_VOLUME: float = float(os.getenv("US_OPEN_MIN_DOLLAR_VOLUME", "30000000"))
    WATCHLIST_MIN_VOL_RATIO: float = float(os.getenv("WATCHLIST_MIN_VOL_RATIO", "0.5"))
    WATCHLIST_MAX_RESULTS: int = int(os.getenv("WATCHLIST_MAX_RESULTS", "5"))
    WATCHLIST_MIN_CONFIDENCE: int = int(
        os.getenv("WATCHLIST_MIN_CONFIDENCE", str(STOCK_MIN_CONFIDENCE))
    )
    VI_TOP_N: int = int(os.getenv("VI_TOP_N", "10"))
    VI_MAX_CANDIDATES: int = int(os.getenv("VI_MAX_CANDIDATES", "20"))
    VI_MIN_CONFIDENCE: int = int(os.getenv("VI_MIN_CONFIDENCE", "76"))
    VI_MIN_VOL_RATIO: float = float(os.getenv("VI_MIN_VOL_RATIO", "1.0"))
    VI_MIN_DOLLAR_VOLUME: float = float(os.getenv("VI_MIN_DOLLAR_VOLUME", "25000000"))
    VI_MAX_PE_RATIO: float = float(os.getenv("VI_MAX_PE_RATIO", "25"))
    VI_LONG_ONLY: bool = os.getenv("VI_LONG_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    VI_MIN_SETUP_WIN_RATE: float = float(os.getenv("VI_MIN_SETUP_WIN_RATE", "0.56"))
    VI_RSI_MIN: float = float(os.getenv("VI_RSI_MIN", "54"))
    VI_RSI_MAX: float = float(os.getenv("VI_RSI_MAX", "67"))
    VI_REQUIRE_QUALITY_SCORE: int = int(os.getenv("VI_REQUIRE_QUALITY_SCORE", "2"))
    TH_VI_MIN_CONFIDENCE: int = int(os.getenv("TH_VI_MIN_CONFIDENCE", "70"))
    TH_VI_MIN_VOL_RATIO: float = float(os.getenv("TH_VI_MIN_VOL_RATIO", "0.8"))
    TH_VI_MIN_DOLLAR_VOLUME: float = float(os.getenv("TH_VI_MIN_DOLLAR_VOLUME", "8000000"))
    TH_VI_MIN_SETUP_WIN_RATE: float = float(os.getenv("TH_VI_MIN_SETUP_WIN_RATE", "0.53"))
    TH_VI_RSI_MIN: float = float(os.getenv("TH_VI_RSI_MIN", "50"))
    TH_VI_RSI_MAX: float = float(os.getenv("TH_VI_RSI_MAX", "74"))
    TH_VI_REQUIRE_QUALITY_SCORE: int = int(os.getenv("TH_VI_REQUIRE_QUALITY_SCORE", "1"))
    TH_VI_LONG_ONLY: bool = os.getenv("TH_VI_LONG_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    STOCK_INFO_CACHE_TTL_SEC: int = int(os.getenv("STOCK_INFO_CACHE_TTL_SEC", "21600"))
    STOCK_MARKETS: list = ["US", "EU", "ASIA", "THAILAND"]

    # ── Economic Calendar Alerts ──────────────────────────────────────────────
    ECON_CALENDAR_ENABLED: bool = os.getenv("ECON_CALENDAR_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    ECON_CALENDAR_FEED_URL: str = os.getenv(
        "ECON_CALENDAR_FEED_URL",
        "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    )
    ECON_CALENDAR_CACHE_TTL_SEC: int = int(os.getenv("ECON_CALENDAR_CACHE_TTL_SEC", "300"))
    ECON_CALENDAR_CHECK_INTERVAL_MIN: int = int(os.getenv("ECON_CALENDAR_CHECK_INTERVAL_MIN", "5"))
    ECON_CALENDAR_LOOKAHEAD_HOURS: int = int(os.getenv("ECON_CALENDAR_LOOKAHEAD_HOURS", "24"))
    ECON_CALENDAR_MIN_IMPACT: str = os.getenv("ECON_CALENDAR_MIN_IMPACT", "high").strip().lower()
    ECON_ALERT_WINDOWS: str = os.getenv("ECON_ALERT_WINDOWS", "60,15")
    ECON_ALERT_TOLERANCE_MIN: int = int(os.getenv("ECON_ALERT_TOLERANCE_MIN", "3"))
    ECON_ALERT_CURRENCIES: str = os.getenv("ECON_ALERT_CURRENCIES", "USD,EUR,GBP,JPY,CAD,AUD,NZD,CHF")

    # ── Macro Headline Risk Watch ─────────────────────────────────────────────
    MACRO_NEWS_ENABLED: bool = os.getenv("MACRO_NEWS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_NEWS_FEED_URL: str = os.getenv(
        "MACRO_NEWS_FEED_URL",
        "https://news.google.com/rss/search?q=(Trump+OR+tariff+OR+Fed+OR+FOMC+OR+CPI+OR+NFP+OR+war+OR+missile+OR+oil+OR+crude+OR+OPEC+OR+sanctions+OR+geopolitical)+when:1d&hl=en-US&gl=US&ceid=US:en",
    )
    MACRO_NEWS_CACHE_TTL_SEC: int = int(os.getenv("MACRO_NEWS_CACHE_TTL_SEC", "300"))
    MACRO_NEWS_CHECK_INTERVAL_MIN: int = int(os.getenv("MACRO_NEWS_CHECK_INTERVAL_MIN", "30"))
    MACRO_NEWS_LOOKBACK_HOURS: int = int(os.getenv("MACRO_NEWS_LOOKBACK_HOURS", "24"))
    MACRO_NEWS_MIN_SCORE: int = int(os.getenv("MACRO_NEWS_MIN_SCORE", "8"))
    MACRO_NEWS_ALERT_MAX_AGE_MIN: int = int(os.getenv("MACRO_NEWS_ALERT_MAX_AGE_MIN", "240"))
    MACRO_NEWS_MAX_ALERTS_PER_RUN: int = int(os.getenv("MACRO_NEWS_MAX_ALERTS_PER_RUN", "2"))
    MACRO_NEWS_REQUIRE_PRIORITY_THEME: bool = os.getenv("MACRO_NEWS_REQUIRE_PRIORITY_THEME", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_NEWS_SOURCE_QUALITY_OVERRIDES: str = os.getenv(
        "MACRO_NEWS_SOURCE_QUALITY_OVERRIDES",
        "REUTERS=0.96,BLOOMBERG=0.95,WSJ=0.93,CNBC=0.84,FXSTREET=0.83,FOREXLIVE=0.79,INVESTINGCOM=0.72,MARKETWATCH=0.74,YAHOOFINANCE=0.66",
    )
    MACRO_NEWS_TRUSTED_MIN_QUALITY: float = float(os.getenv("MACRO_NEWS_TRUSTED_MIN_QUALITY", "0.80"))
    MACRO_NEWS_RUMOR_FILTER_ENABLED: bool = os.getenv("MACRO_NEWS_RUMOR_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN: int = int(os.getenv("MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN", "120"))
    MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE: int = int(os.getenv("MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE", "8"))
    MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY: float = float(os.getenv("MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY", "0.75"))
    MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN_XAUUSD: int = int(os.getenv("MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN_XAUUSD", os.getenv("MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN", "120")))
    MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE_XAUUSD: int = int(os.getenv("MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE_XAUUSD", os.getenv("MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE", "8")))
    MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY_XAUUSD: float = float(os.getenv("MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY_XAUUSD", os.getenv("MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY", "0.75")))
    MACRO_NEWS_RUMOR_SCORE_PENALTY: float = float(os.getenv("MACRO_NEWS_RUMOR_SCORE_PENALTY", "2.0"))
    MACRO_NEWS_UNVERIFIED_SCORE_PENALTY: float = float(os.getenv("MACRO_NEWS_UNVERIFIED_SCORE_PENALTY", "1.0"))
    MACRO_NEWS_CONFIRMED_SCORE_BONUS: float = float(os.getenv("MACRO_NEWS_CONFIRMED_SCORE_BONUS", "0.8"))
    MACRO_IMPACT_TRACKER_ENABLED: bool = os.getenv("MACRO_IMPACT_TRACKER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_IMPACT_TRACKER_DB_PATH: str = os.getenv("MACRO_IMPACT_TRACKER_DB_PATH", "")
    MACRO_IMPACT_TRACKER_SYNC_INTERVAL_MIN: int = int(os.getenv("MACRO_IMPACT_TRACKER_SYNC_INTERVAL_MIN", "15"))
    MACRO_IMPACT_TRACKER_LOOKBACK_HOURS: int = int(os.getenv("MACRO_IMPACT_TRACKER_LOOKBACK_HOURS", "72"))
    MACRO_IMPACT_TRACKER_MIN_SCORE: int = int(os.getenv("MACRO_IMPACT_TRACKER_MIN_SCORE", "5"))
    MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC: int = int(os.getenv("MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC", "20"))
    MACRO_REPORT_DEFAULT_HOURS: int = int(os.getenv("MACRO_REPORT_DEFAULT_HOURS", "24"))
    MACRO_REPORT_MAX_HEADLINES: int = int(os.getenv("MACRO_REPORT_MAX_HEADLINES", "5"))
    MACRO_ADAPTIVE_WEIGHTING_ENABLED: bool = os.getenv("MACRO_ADAPTIVE_WEIGHTING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES: int = int(os.getenv("MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES", "3"))
    MACRO_ADAPTIVE_WEIGHT_MIN_MULT: float = float(os.getenv("MACRO_ADAPTIVE_WEIGHT_MIN_MULT", "0.80"))
    MACRO_ADAPTIVE_WEIGHT_MAX_MULT: float = float(os.getenv("MACRO_ADAPTIVE_WEIGHT_MAX_MULT", "1.25"))
    MACRO_ADAPTIVE_WEIGHT_UPDATE_HOURS: int = int(os.getenv("MACRO_ADAPTIVE_WEIGHT_UPDATE_HOURS", "168"))  # 7d
    MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED: bool = os.getenv("MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MACRO_ALERT_ADAPTIVE_MIN_SAMPLES: int = int(os.getenv("MACRO_ALERT_ADAPTIVE_MIN_SAMPLES", "3"))
    MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT: float = float(os.getenv("MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT", "0.90"))
    MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE: float = float(os.getenv("MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE", "65"))
    MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR: int = int(os.getenv("MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR", "10"))
    MACRO_WEIGHTS_DEFAULT_TOP: int = int(os.getenv("MACRO_WEIGHTS_DEFAULT_TOP", "8"))

    # ── Risk Management ────────────────────────────────────────────────────────
    DEFAULT_RISK_PERCENT: float = float(os.getenv("DEFAULT_RISK_PERCENT", "1.0"))
    DEFAULT_RR_RATIO:     float = float(os.getenv("DEFAULT_RR_RATIO",     "2.5"))

    # ── Timeframes — XAUUSD ───────────────────────────────────────────────────
    XAUUSD_TREND_TF:     str = os.getenv("XAUUSD_TREND_TF",     "1d")
    XAUUSD_STRUCTURE_TF: str = os.getenv("XAUUSD_STRUCTURE_TF", "4h")
    XAUUSD_ENTRY_TF:     str = os.getenv("XAUUSD_ENTRY_TF",     "1h")

    # ── Timeframes — Crypto ───────────────────────────────────────────────────
    CRYPTO_TREND_TF: str = os.getenv("CRYPTO_TREND_TF", "4h")
    CRYPTO_ENTRY_TF: str = os.getenv("CRYPTO_ENTRY_TF", "1h")
    FX_TREND_TF: str = os.getenv("FX_TREND_TF", "4h")
    FX_ENTRY_TF: str = os.getenv("FX_ENTRY_TF", "1h")
    # ── Scalping (separate pipeline, does not alter default signal flow) ────
    SCALPING_ENABLED: bool = os.getenv("SCALPING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_SYMBOLS: str = os.getenv("SCALPING_SYMBOLS", "XAUUSD,ETHUSD")
    SCALPING_ETH_SYMBOL: str = os.getenv("SCALPING_ETH_SYMBOL", "ETH/USDT")
    SCALPING_BTC_SYMBOL: str = os.getenv("SCALPING_BTC_SYMBOL", "BTC/USDT")
    SCALPING_SCAN_INTERVAL_SEC: int = int(os.getenv("SCALPING_SCAN_INTERVAL_SEC", "300"))
    SCALPING_ENTRY_TF: str = os.getenv("SCALPING_ENTRY_TF", "5m")
    SCALPING_M1_TRIGGER_TF: str = os.getenv("SCALPING_M1_TRIGGER_TF", "1m")
    SCALPING_XAU_TREND_TF: str = os.getenv("SCALPING_XAU_TREND_TF", "1h")
    SCALPING_XAU_STRUCTURE_TF: str = os.getenv("SCALPING_XAU_STRUCTURE_TF", "15m")
    SCALPING_CRYPTO_TREND_TF: str = os.getenv("SCALPING_CRYPTO_TREND_TF", "15m")
    SCALPING_MIN_CONFIDENCE: float = float(os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70")))
    SCALPING_ETH_MIN_CONFIDENCE: float = float(os.getenv("SCALPING_ETH_MIN_CONFIDENCE", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70"))))
    SCALPING_BTC_MIN_CONFIDENCE: float = float(os.getenv("SCALPING_BTC_MIN_CONFIDENCE", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70"))))
    SCALPING_ETH_MIN_CONFIDENCE_WEEKEND: float = float(os.getenv("SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", os.getenv("SCALPING_ETH_MIN_CONFIDENCE", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70")))))
    SCALPING_BTC_MIN_CONFIDENCE_WEEKEND: float = float(os.getenv("SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", os.getenv("SCALPING_BTC_MIN_CONFIDENCE", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70")))))
    SCALPING_ETH_ALLOWED_SESSIONS: str = os.getenv("SCALPING_ETH_ALLOWED_SESSIONS", "")
    SCALPING_BTC_ALLOWED_SESSIONS: str = os.getenv("SCALPING_BTC_ALLOWED_SESSIONS", "")
    SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND: str = os.getenv("SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND", "")
    SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND: str = os.getenv("SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND", "")
    SCALPING_MIN_CONFIDENCE_XAU_LONG: float = float(os.getenv("SCALPING_MIN_CONFIDENCE_XAU_LONG", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70"))))
    SCALPING_MIN_CONFIDENCE_XAU_SHORT: float = float(os.getenv("SCALPING_MIN_CONFIDENCE_XAU_SHORT", os.getenv("SCALPING_MIN_CONFIDENCE_XAU_LONG", os.getenv("SCALPING_MIN_CONFIDENCE", os.getenv("MIN_SIGNAL_CONFIDENCE", "70")))))
    SCALPING_XAU_BALANCE_SIDE_THRESHOLDS: bool = os.getenv("SCALPING_XAU_BALANCE_SIDE_THRESHOLDS", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_REGIME_GUARD_ENABLED: bool = os.getenv("SCALPING_XAU_REGIME_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_REGIME_GUARD_TREND_TF: str = os.getenv("SCALPING_XAU_REGIME_GUARD_TREND_TF", "1d")
    SCALPING_XAU_REGIME_GUARD_STRUCTURE_TF: str = os.getenv("SCALPING_XAU_REGIME_GUARD_STRUCTURE_TF", "4h")
    SCALPING_XAU_EXIT_RETUNE_ENABLED: bool = os.getenv("SCALPING_XAU_EXIT_RETUNE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_TP1_RR: float = float(os.getenv("SCALPING_XAU_TP1_RR", "0.9"))
    SCALPING_XAU_TP2_RR: float = float(os.getenv("SCALPING_XAU_TP2_RR", "1.35"))
    SCALPING_XAU_TP3_RR: float = float(os.getenv("SCALPING_XAU_TP3_RR", "1.9"))
    SCALPING_XAU_SL_MAX_ATR: float = float(os.getenv("SCALPING_XAU_SL_MAX_ATR", "1.25"))
    SCALPING_XAU_TP2_MAX_ATR: float = float(os.getenv("SCALPING_XAU_TP2_MAX_ATR", "1.8"))
    SCALPING_XAU_TP3_MAX_ATR: float = float(os.getenv("SCALPING_XAU_TP3_MAX_ATR", "2.6"))
    SCALPING_XAU_FORCE_EVERY_SCAN: bool = os.getenv("SCALPING_XAU_FORCE_EVERY_SCAN", "0").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_FORCE_CONFIDENCE_BASE: float = float(os.getenv("SCALPING_XAU_FORCE_CONFIDENCE_BASE", "58.0"))
    SCALPING_XAU_FORCE_MIN_CONFIDENCE: float = float(os.getenv("SCALPING_XAU_FORCE_MIN_CONFIDENCE", "56.0"))
    SCALPING_XAU_FORCE_SL_ATR: float = float(os.getenv("SCALPING_XAU_FORCE_SL_ATR", "0.60"))
    SCALPING_XAU_FORCE_TP1_RR: float = float(os.getenv("SCALPING_XAU_FORCE_TP1_RR", "0.55"))
    SCALPING_XAU_FORCE_TP2_RR: float = float(os.getenv("SCALPING_XAU_FORCE_TP2_RR", "0.90"))
    SCALPING_XAU_FORCE_TP3_RR: float = float(os.getenv("SCALPING_XAU_FORCE_TP3_RR", "1.25"))
    SCALPING_XAU_FORCE_LOOKBACK_BARS: int = int(os.getenv("SCALPING_XAU_FORCE_LOOKBACK_BARS", "220"))
    SCALPING_XAU_FORCE_USE_M1_MICRO: bool = os.getenv("SCALPING_XAU_FORCE_USE_M1_MICRO", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_FORCE_ENTRY_ADV_ATR_M1: float = float(os.getenv("SCALPING_XAU_FORCE_ENTRY_ADV_ATR_M1", "0.18"))
    SCALPING_XAU_FORCE_BLOCK_REASONS: str = os.getenv(
        "SCALPING_XAU_FORCE_BLOCK_REASONS",
        "no_signal,base_scanner_no_signal,no_direction_passed_threshold,m1_short_not_confirmed,m1_long_not_confirmed,m1_not_confirmed",
    )
    SCALPING_XAU_FORCE_REQUIRE_COUNTERTREND_CONFIRMED_LONG: bool = os.getenv(
        "SCALPING_XAU_FORCE_REQUIRE_COUNTERTREND_CONFIRMED_LONG",
        "1",
    ).strip().lower() in ("1", "true", "yes", "on")
    XAU_COUNTERTREND_LONG_REQUIRE_CONFIRMED: bool = os.getenv(
        "XAU_COUNTERTREND_LONG_REQUIRE_CONFIRMED",
        "1",
    ).strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_FORCE_ENTRY_MAX_DIST_ATR_M1: float = float(os.getenv("SCALPING_XAU_FORCE_ENTRY_MAX_DIST_ATR_M1", "0.45"))
    SCALPING_XAU_FORCE_RSI_LONG_MIN: float = float(os.getenv("SCALPING_XAU_FORCE_RSI_LONG_MIN", "51.0"))
    SCALPING_XAU_FORCE_RSI_SHORT_MAX: float = float(os.getenv("SCALPING_XAU_FORCE_RSI_SHORT_MAX", "49.0"))
    SCALPING_CRYPTO_WINNER_LOGIC_ENABLED: bool = os.getenv("SCALPING_CRYPTO_WINNER_LOGIC_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_CRYPTO_WINNER_CACHE_SEC: int = int(os.getenv("SCALPING_CRYPTO_WINNER_CACHE_SEC", "180"))
    SCALPING_CRYPTO_WINNER_LOOKBACK_DAYS: int = int(os.getenv("SCALPING_CRYPTO_WINNER_LOOKBACK_DAYS", "21"))
    SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_BAND_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_BAND_SAMPLES", "3"))
    SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_SAMPLES", "4"))
    SCALPING_CRYPTO_WINNER_MIN_SESSION_BAND_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SESSION_BAND_SAMPLES", "4"))
    SCALPING_CRYPTO_WINNER_MIN_SIDE_BAND_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SIDE_BAND_SAMPLES", "4"))
    SCALPING_CRYPTO_WINNER_MIN_SIDE_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SIDE_SAMPLES", "5"))
    SCALPING_CRYPTO_WINNER_MIN_SESSION_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_SESSION_SAMPLES", "3"))
    SCALPING_CRYPTO_WINNER_MIN_BAND_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_BAND_SAMPLES", "4"))
    SCALPING_CRYPTO_WINNER_MIN_OVERALL_SAMPLES: int = int(os.getenv("SCALPING_CRYPTO_WINNER_MIN_OVERALL_SAMPLES", "8"))
    SCALPING_CRYPTO_WINNER_STRONG_WR: float = float(os.getenv("SCALPING_CRYPTO_WINNER_STRONG_WR", "0.62"))
    SCALPING_CRYPTO_WINNER_WEAK_WR: float = float(os.getenv("SCALPING_CRYPTO_WINNER_WEAK_WR", "0.50"))
    SCALPING_CRYPTO_WINNER_SEVERE_WR: float = float(os.getenv("SCALPING_CRYPTO_WINNER_SEVERE_WR", "0.40"))
    SCALPING_CRYPTO_WINNER_STRONG_AVG_PNL: float = float(os.getenv("SCALPING_CRYPTO_WINNER_STRONG_AVG_PNL", "0.0"))
    SCALPING_CRYPTO_WINNER_WEAK_AVG_PNL: float = float(os.getenv("SCALPING_CRYPTO_WINNER_WEAK_AVG_PNL", "0.0"))
    SCALPING_CRYPTO_WINNER_SEVERE_AVG_PNL: float = float(os.getenv("SCALPING_CRYPTO_WINNER_SEVERE_AVG_PNL", "-8.0"))
    SCALPING_CRYPTO_WINNER_CONF_BONUS: float = float(os.getenv("SCALPING_CRYPTO_WINNER_CONF_BONUS", "2.0"))
    SCALPING_CRYPTO_WINNER_CONF_PENALTY_WEAK: float = float(os.getenv("SCALPING_CRYPTO_WINNER_CONF_PENALTY_WEAK", "2.0"))
    SCALPING_CRYPTO_WINNER_CONF_PENALTY_SEVERE: float = float(os.getenv("SCALPING_CRYPTO_WINNER_CONF_PENALTY_SEVERE", "4.5"))
    SCALPING_CRYPTO_WINNER_CONF_MIN: float = float(os.getenv("SCALPING_CRYPTO_WINNER_CONF_MIN", "55.0"))
    SCALPING_CRYPTO_WINNER_CONF_MAX: float = float(os.getenv("SCALPING_CRYPTO_WINNER_CONF_MAX", "90.0"))
    SCALPING_CRYPTO_WINNER_HARD_BLOCK_SEVERE: bool = os.getenv("SCALPING_CRYPTO_WINNER_HARD_BLOCK_SEVERE", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_CRYPTO_WINNER_HARD_BLOCK_MIN_CONF: float = float(os.getenv("SCALPING_CRYPTO_WINNER_HARD_BLOCK_MIN_CONF", "76.0"))

    # ── Crypto Winner Exit Retuning (ported from XAU) ─────────────────────────
    SCALPING_CRYPTO_WINNER_RISK_MULT_STRONG: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RISK_MULT_STRONG", "1.00"))
    SCALPING_CRYPTO_WINNER_RISK_MULT_WEAK: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RISK_MULT_WEAK", "0.85"))
    SCALPING_CRYPTO_WINNER_RISK_MULT_SEVERE: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RISK_MULT_SEVERE", "0.75"))
    SCALPING_CRYPTO_WINNER_RR_MULT_STRONG: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RR_MULT_STRONG", "1.05"))
    SCALPING_CRYPTO_WINNER_RR_MULT_WEAK: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RR_MULT_WEAK", "0.88"))
    SCALPING_CRYPTO_WINNER_RR_MULT_SEVERE: float = float(os.getenv("SCALPING_CRYPTO_WINNER_RR_MULT_SEVERE", "0.78"))

    # ── Crypto Performance Tracker ────────────────────────────────────────────
    CRYPTO_PERFORMANCE_TRACKER_ENABLED: bool = os.getenv("CRYPTO_PERFORMANCE_TRACKER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_PERFORMANCE_TRACKER_LOOKBACK_DAYS: int = int(os.getenv("CRYPTO_PERFORMANCE_TRACKER_LOOKBACK_DAYS", "21"))
    CRYPTO_PERFORMANCE_TRACKER_REPORT_PATH: str = os.getenv("CRYPTO_PERFORMANCE_TRACKER_REPORT_PATH", "data/reports/crypto_performance_tracker.json")
    SCALPING_XAU_WINNER_LOGIC_ENABLED: bool = os.getenv("SCALPING_XAU_WINNER_LOGIC_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_WINNER_CACHE_SEC: int = int(os.getenv("SCALPING_XAU_WINNER_CACHE_SEC", "180"))
    SCALPING_XAU_WINNER_LOOKBACK_DAYS: int = int(os.getenv("SCALPING_XAU_WINNER_LOOKBACK_DAYS", "14"))
    SCALPING_XAU_WINNER_MIN_SAMPLES: int = int(os.getenv("SCALPING_XAU_WINNER_MIN_SAMPLES", "0"))
    SCALPING_XAU_WINNER_MIN_SIDE_SESSION_SAMPLES: int = int(os.getenv("SCALPING_XAU_WINNER_MIN_SIDE_SESSION_SAMPLES", "6"))
    SCALPING_XAU_WINNER_MIN_SIDE_SAMPLES: int = int(os.getenv("SCALPING_XAU_WINNER_MIN_SIDE_SAMPLES", "12"))
    SCALPING_XAU_WINNER_MIN_SESSION_SAMPLES: int = int(os.getenv("SCALPING_XAU_WINNER_MIN_SESSION_SAMPLES", "10"))
    SCALPING_XAU_WINNER_MIN_OVERALL_SAMPLES: int = int(os.getenv("SCALPING_XAU_WINNER_MIN_OVERALL_SAMPLES", "20"))
    SCALPING_XAU_WINNER_STRONG_WR: float = float(os.getenv("SCALPING_XAU_WINNER_STRONG_WR", "0.60"))
    SCALPING_XAU_WINNER_WEAK_WR: float = float(os.getenv("SCALPING_XAU_WINNER_WEAK_WR", "0.50"))
    SCALPING_XAU_WINNER_SEVERE_WR: float = float(os.getenv("SCALPING_XAU_WINNER_SEVERE_WR", "0.40"))
    SCALPING_XAU_WINNER_STRONG_AVG_PNL: float = float(os.getenv("SCALPING_XAU_WINNER_STRONG_AVG_PNL", "0.0"))
    SCALPING_XAU_WINNER_WEAK_AVG_PNL: float = float(os.getenv("SCALPING_XAU_WINNER_WEAK_AVG_PNL", "0.0"))
    SCALPING_XAU_WINNER_SEVERE_AVG_PNL: float = float(os.getenv("SCALPING_XAU_WINNER_SEVERE_AVG_PNL", "-3.0"))
    SCALPING_XAU_WINNER_CONF_BONUS: float = float(os.getenv("SCALPING_XAU_WINNER_CONF_BONUS", "2.0"))
    SCALPING_XAU_WINNER_CONF_PENALTY_WEAK: float = float(os.getenv("SCALPING_XAU_WINNER_CONF_PENALTY_WEAK", "2.2"))
    SCALPING_XAU_WINNER_CONF_PENALTY_SEVERE: float = float(os.getenv("SCALPING_XAU_WINNER_CONF_PENALTY_SEVERE", "4.8"))
    SCALPING_XAU_WINNER_CONF_MIN: float = float(os.getenv("SCALPING_XAU_WINNER_CONF_MIN", "50.0"))
    SCALPING_XAU_WINNER_CONF_MAX: float = float(os.getenv("SCALPING_XAU_WINNER_CONF_MAX", "90.0"))
    SCALPING_XAU_WINNER_RISK_MULT_STRONG: float = float(os.getenv("SCALPING_XAU_WINNER_RISK_MULT_STRONG", "1.00"))
    SCALPING_XAU_WINNER_RISK_MULT_WEAK: float = float(os.getenv("SCALPING_XAU_WINNER_RISK_MULT_WEAK", "0.90"))
    SCALPING_XAU_WINNER_RISK_MULT_SEVERE: float = float(os.getenv("SCALPING_XAU_WINNER_RISK_MULT_SEVERE", "0.82"))
    SCALPING_XAU_WINNER_RR_MULT_STRONG: float = float(os.getenv("SCALPING_XAU_WINNER_RR_MULT_STRONG", "1.03"))
    SCALPING_XAU_WINNER_RR_MULT_WEAK: float = float(os.getenv("SCALPING_XAU_WINNER_RR_MULT_WEAK", "0.90"))
    SCALPING_XAU_WINNER_RR_MULT_SEVERE: float = float(os.getenv("SCALPING_XAU_WINNER_RR_MULT_SEVERE", "0.82"))
    SCALPING_XAU_WINNER_HARD_BLOCK_SEVERE: bool = os.getenv("SCALPING_XAU_WINNER_HARD_BLOCK_SEVERE", "0").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_XAU_WINNER_HARD_BLOCK_MIN_CONF: float = float(os.getenv("SCALPING_XAU_WINNER_HARD_BLOCK_MIN_CONF", "58.0"))
    SCALPING_M1_TRIGGER_LOOKBACK_BARS: int = int(os.getenv("SCALPING_M1_TRIGGER_LOOKBACK_BARS", "120"))
    SCALPING_M1_TRIGGER_BREAKOUT_BARS: int = int(os.getenv("SCALPING_M1_TRIGGER_BREAKOUT_BARS", "3"))
    SCALPING_M1_TRIGGER_RSI_LONG_MIN: float = float(os.getenv("SCALPING_M1_TRIGGER_RSI_LONG_MIN", "52"))
    SCALPING_M1_TRIGGER_RSI_LONG_MAX: float = float(os.getenv("SCALPING_M1_TRIGGER_RSI_LONG_MAX", "70"))
    SCALPING_M1_TRIGGER_RSI_SHORT_MAX: float = float(os.getenv("SCALPING_M1_TRIGGER_RSI_SHORT_MAX", "48"))
    SCALPING_M1_TRIGGER_REFHIGH_BUFFER_MULT_LONG: float = float(os.getenv("SCALPING_M1_TRIGGER_REFHIGH_BUFFER_MULT_LONG", "1.00"))
    SCALPING_M1_TRIGGER_REFLOW_BUFFER_MULT_SHORT: float = float(os.getenv("SCALPING_M1_TRIGGER_REFLOW_BUFFER_MULT_SHORT", "1.25"))
    # ── Crypto-specific M1 trigger overrides (wider RSI bands for crypto volatility) ──
    SCALPING_CRYPTO_M1_RSI_LONG_MIN: float = float(os.getenv("SCALPING_CRYPTO_M1_RSI_LONG_MIN", "48"))
    SCALPING_CRYPTO_M1_RSI_LONG_MAX: float = float(os.getenv("SCALPING_CRYPTO_M1_RSI_LONG_MAX", "75"))
    SCALPING_CRYPTO_M1_RSI_SHORT_MAX: float = float(os.getenv("SCALPING_CRYPTO_M1_RSI_SHORT_MAX", "52"))
    SCALPING_CRYPTO_M1_BREAKOUT_BARS: int = int(os.getenv("SCALPING_CRYPTO_M1_BREAKOUT_BARS", "5"))
    SCALPING_ALERT_COOLDOWN_SEC: int = int(os.getenv("SCALPING_ALERT_COOLDOWN_SEC", "120"))
    SCALPING_DUPLICATE_SUPPRESS_SEC: int = int(os.getenv("SCALPING_DUPLICATE_SUPPRESS_SEC", "1800"))
    SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED: bool = os.getenv("SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    XAU_HOLIDAY_GUARD_ENABLED: bool = os.getenv("XAU_HOLIDAY_GUARD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    # ── Post-SL sweep reversal re-entry ──────────────────────────────────────
    POST_SL_REVERSAL_ENABLED: bool = os.getenv("POST_SL_REVERSAL_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    POST_SL_REVERSAL_MIN_WICK_RATIO: float = float(os.getenv("POST_SL_REVERSAL_MIN_WICK_RATIO", "0.55"))
    POST_SL_REVERSAL_MIN_SWEEP_PIPS: float = float(os.getenv("POST_SL_REVERSAL_MIN_SWEEP_PIPS", "3.0"))
    POST_SL_REVERSAL_CONFIDENCE: float = float(os.getenv("POST_SL_REVERSAL_CONFIDENCE", "74.0"))
    POST_SL_REVERSAL_SL_BUFFER_ATR: float = float(os.getenv("POST_SL_REVERSAL_SL_BUFFER_ATR", "0.20"))
    POST_SL_REVERSAL_TP1_R: float = float(os.getenv("POST_SL_REVERSAL_TP1_R", "1.5"))
    POST_SL_REVERSAL_TP2_R: float = float(os.getenv("POST_SL_REVERSAL_TP2_R", "2.5"))
    POST_SL_REVERSAL_TP3_R: float = float(os.getenv("POST_SL_REVERSAL_TP3_R", "3.5"))
    POST_SL_REVERSAL_COOLDOWN_SECONDS: float = float(os.getenv("POST_SL_REVERSAL_COOLDOWN_SECONDS", "300"))
    POST_SL_REVERSAL_BYPASS_CONF_BAND: bool = os.getenv("POST_SL_REVERSAL_BYPASS_CONF_BAND", "1").strip().lower() in ("1", "true", "yes", "on")
    POST_SL_REVERSAL_BYPASS_MTF: bool = os.getenv("POST_SL_REVERSAL_BYPASS_MTF", "0").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_NOTIFY_TELEGRAM: bool = os.getenv("SCALPING_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_EXECUTE_MT5: bool = os.getenv("SCALPING_EXECUTE_MT5", "1").strip().lower() in ("1", "true", "yes", "on")
    SCALPING_CLOSE_TIMEOUT_MIN: int = int(os.getenv("SCALPING_CLOSE_TIMEOUT_MIN", "35"))
    SCALPING_SIM_MAX_PENDING_MIN: int = int(os.getenv("SCALPING_SIM_MAX_PENDING_MIN", "120"))
    SCALPING_TIMEOUT_CHECK_INTERVAL_MIN: int = int(os.getenv("SCALPING_TIMEOUT_CHECK_INTERVAL_MIN", "1"))
    SCALPING_NET_LOG_ENABLED: bool = os.getenv("SCALPING_NET_LOG_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    FX_SCANNER_MT5_TRADABLE_ONLY: bool = os.getenv("FX_SCANNER_MT5_TRADABLE_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    FX_MAJOR_SYMBOLS: str = os.getenv(
        "FX_MAJOR_SYMBOLS",
        "EURUSD,GBPUSD,USDJPY,AUDUSD,NZDUSD,USDCAD,USDCHF",
    )
    CRYPTO_SNIPER_EXCLUDE_FIAT_BASES: bool = os.getenv("CRYPTO_SNIPER_EXCLUDE_FIAT_BASES", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_SNIPER_EXCLUDE_STABLE_BASES: bool = os.getenv("CRYPTO_SNIPER_EXCLUDE_STABLE_BASES", "1").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_SNIPER_MT5_TRADABLE_ONLY: bool = os.getenv("CRYPTO_SNIPER_MT5_TRADABLE_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
    STOCK_SCANNER_MT5_TRADABLE_ONLY: bool = os.getenv("STOCK_SCANNER_MT5_TRADABLE_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")
    CRYPTO_SNIPER_EXCLUDE_BASES: str = os.getenv(
        "CRYPTO_SNIPER_EXCLUDE_BASES",
        "USDT,USDC,FDUSD,TUSD,BUSD,USDP,DAI,USDD,PYUSD,EUR,GBP,JPY,AUD,TRY,BRL,RUB,NGN,UAH,ZAR",
    )

    # ── Stock Scanner Yahoo Cleanup ──────────────────────────────────────────
    STOCK_YF_BAD_SYMBOL_CACHE_TTL_SEC: int = int(os.getenv("STOCK_YF_BAD_SYMBOL_CACHE_TTL_SEC", "86400"))
    STOCK_YF_EMPTY_FAILS_TO_BLACKLIST: int = int(os.getenv("STOCK_YF_EMPTY_FAILS_TO_BLACKLIST", "2"))
    STOCK_YF_SYMBOL_ALIAS_MAP: str = os.getenv("STOCK_YF_SYMBOL_ALIAS_MAP", "")  # e.g. DTAC.BK=TRUE.BK

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    US_OPEN_SMART_MONITOR: bool = os.getenv("US_OPEN_SMART_MONITOR", "1").strip() in ("1", "true", "yes", "on")
    ADMIN_AI_INTENT_ENABLED: bool = os.getenv("ADMIN_AI_INTENT_ENABLED", "1").strip() in ("1", "true", "yes", "on")
    ACCESS_DB_PATH: str = os.getenv("ACCESS_DB_PATH", "")
    TRIAL_DAYS: int = int(os.getenv("TRIAL_DAYS", "7"))
    TRIAL_NO_AI_ALL: bool = os.getenv("TRIAL_NO_AI_ALL", "0").strip().lower() in ("1", "true", "yes", "on")
    PLAN_TRIAL_DAILY_LIMIT: int = int(os.getenv("PLAN_TRIAL_DAILY_LIMIT", "12"))
    TRIAL_CRYPTO_SYMBOLS: str = os.getenv("TRIAL_CRYPTO_SYMBOLS", "BTC/USDT,ETH/USDT,BTCUSD,ETHUSD")
    PLAN_A_DAILY_LIMIT: int = int(os.getenv("PLAN_A_DAILY_LIMIT", "30"))
    PLAN_B_DAILY_LIMIT: int = int(os.getenv("PLAN_B_DAILY_LIMIT", "120"))
    PLAN_C_DAILY_LIMIT: int = int(os.getenv("PLAN_C_DAILY_LIMIT", "500"))
    BILLING_ENABLED: bool = os.getenv("BILLING_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    BILLING_AUTOSTART_IN_MONITOR: bool = os.getenv("BILLING_AUTOSTART_IN_MONITOR", "1").strip().lower() in ("1", "true", "yes", "on")
    BILLING_WEBHOOK_HOST: str = os.getenv("BILLING_WEBHOOK_HOST", "0.0.0.0")
    BILLING_WEBHOOK_PORT: int = int(os.getenv("BILLING_WEBHOOK_PORT", "8787"))
    BILLING_DEFAULT_PLAN: str = os.getenv("BILLING_DEFAULT_PLAN", "b").strip().lower()
    BILLING_DEFAULT_DAYS: int = int(os.getenv("BILLING_DEFAULT_DAYS", "30"))
    BILLING_UPGRADE_URL: str = os.getenv("BILLING_UPGRADE_URL", "")
    STRIPE_ENABLED: bool = os.getenv("STRIPE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    STRIPE_SECRET_KEY: str = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_SIGNATURE_TOLERANCE_SEC: int = int(os.getenv("STRIPE_SIGNATURE_TOLERANCE_SEC", "300"))
    STRIPE_PRICE_PLAN_MAP: str = os.getenv("STRIPE_PRICE_PLAN_MAP", "")
    STRIPE_PRICE_ID_A: str = os.getenv("STRIPE_PRICE_ID_A", "")
    STRIPE_PRICE_ID_B: str = os.getenv("STRIPE_PRICE_ID_B", "")
    STRIPE_PRICE_ID_C: str = os.getenv("STRIPE_PRICE_ID_C", "")
    STRIPE_CHECKOUT_SUCCESS_URL: str = os.getenv("STRIPE_CHECKOUT_SUCCESS_URL", "")
    STRIPE_CHECKOUT_CANCEL_URL: str = os.getenv("STRIPE_CHECKOUT_CANCEL_URL", "")
    BILLING_CURRENCY: str = os.getenv("BILLING_CURRENCY", "usd").strip().lower()
    BILLING_PRICE_A_CENTS: int = int(os.getenv("BILLING_PRICE_A_CENTS", "1900"))
    BILLING_PRICE_B_CENTS: int = int(os.getenv("BILLING_PRICE_B_CENTS", "4900"))
    BILLING_PRICE_C_CENTS: int = int(os.getenv("BILLING_PRICE_C_CENTS", "12900"))
    BILLING_PLAN_DAYS_A: int = int(os.getenv("BILLING_PLAN_DAYS_A", "30"))
    BILLING_PLAN_DAYS_B: int = int(os.getenv("BILLING_PLAN_DAYS_B", "30"))
    BILLING_PLAN_DAYS_C: int = int(os.getenv("BILLING_PLAN_DAYS_C", "90"))
    PROMPTPAY_ENABLED: bool = os.getenv("PROMPTPAY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    PROMPTPAY_WEBHOOK_SECRET: str = os.getenv("PROMPTPAY_WEBHOOK_SECRET", "")
    PROMPTPAY_SIGNATURE_HEADER: str = os.getenv("PROMPTPAY_SIGNATURE_HEADER", "X-PromptPay-Signature")
    PROMPTPAY_REQUIRE_SIGNATURE: bool = os.getenv("PROMPTPAY_REQUIRE_SIGNATURE", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_ENABLED: bool = os.getenv("NEURAL_BRAIN_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_SYNC_DAYS: int = int(os.getenv("NEURAL_BRAIN_SYNC_DAYS", "120"))
    NEURAL_BRAIN_SYNC_INTERVAL_MIN: int = int(os.getenv("NEURAL_BRAIN_SYNC_INTERVAL_MIN", "15"))
    NEURAL_BRAIN_AUTO_TRAIN: bool = os.getenv("NEURAL_BRAIN_AUTO_TRAIN", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_MIN_SAMPLES: int = int(os.getenv("NEURAL_BRAIN_MIN_SAMPLES", "30"))
    NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES: int = int(os.getenv("NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES", "10"))
    NEURAL_BRAIN_EPOCHS: int = int(os.getenv("NEURAL_BRAIN_EPOCHS", "300"))
    NEURAL_BRAIN_HIDDEN_UNITS: int = int(os.getenv("NEURAL_BRAIN_HIDDEN_UNITS", "12"))
    NEURAL_BRAIN_LR: float = float(os.getenv("NEURAL_BRAIN_LR", "0.05"))
    NEURAL_BRAIN_EXECUTION_FILTER: bool = os.getenv("NEURAL_BRAIN_EXECUTION_FILTER", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_MIN_PROB: float = float(os.getenv("NEURAL_BRAIN_MIN_PROB", "0.55"))
    NEURAL_BRAIN_MIN_PROB_FX: float = float(os.getenv("NEURAL_BRAIN_MIN_PROB_FX", os.getenv("NEURAL_BRAIN_MIN_PROB", "0.55")))
    NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES: str = os.getenv("NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES", "")
    NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW_SYMBOL_OVERRIDES: str = os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW_SYMBOL_OVERRIDES", "")
    NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH_SYMBOL_OVERRIDES: str = os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH_SYMBOL_OVERRIDES", "")
    NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY_SYMBOL_OVERRIDES: str = os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY_SYMBOL_OVERRIDES", "")
    NEURAL_BRAIN_FX_SOFT_FILTER_ENABLED: bool = os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW", "0.43"))
    NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH", "0.48"))
    NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY", "4.0"))
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_BAND_ENABLED: bool = os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_BAND_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_LOOKBACK_DAYS: int = int(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_LOOKBACK_DAYS", "60"))
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MIN_RESOLVED: int = int(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MIN_RESOLVED", "8"))
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_BLEND: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_BLEND", "0.50"))
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MIN_LOW: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MIN_LOW", "0.35"))
    NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MAX_HIGH: float = float(os.getenv("NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_MAX_HIGH", "0.52"))
    MT5_FX_CONF_SOFT_FILTER_ENABLED: bool = os.getenv("MT5_FX_CONF_SOFT_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_FX_CONF_SOFT_FILTER_BAND_PTS: float = float(os.getenv("MT5_FX_CONF_SOFT_FILTER_BAND_PTS", "6.0"))
    MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY: float = float(os.getenv("MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY", "0.35"))
    MT5_FX_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES: str = os.getenv("MT5_FX_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES", "")
    MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES: str = os.getenv("MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES", "")
    MT5_CRYPTO_CONF_SOFT_FILTER_ENABLED: bool = os.getenv("MT5_CRYPTO_CONF_SOFT_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS: float = float(os.getenv("MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS", "4.0"))
    MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY: float = float(os.getenv("MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY", "0.25"))
    MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES: str = os.getenv("MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES", "")
    MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES: str = os.getenv("MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES", "")
    MT5_FX_CONF_SOFT_FILTER_LEARNED_BAND_ENABLED: bool = os.getenv("MT5_FX_CONF_SOFT_FILTER_LEARNED_BAND_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    MT5_FX_CONF_SOFT_FILTER_LEARNED_LOOKBACK_DAYS: int = int(os.getenv("MT5_FX_CONF_SOFT_FILTER_LEARNED_LOOKBACK_DAYS", "60"))
    MT5_FX_CONF_SOFT_FILTER_LEARNED_MIN_RESOLVED: int = int(os.getenv("MT5_FX_CONF_SOFT_FILTER_LEARNED_MIN_RESOLVED", "8"))
    MT5_FX_CONF_SOFT_FILTER_LEARNED_BLEND: float = float(os.getenv("MT5_FX_CONF_SOFT_FILTER_LEARNED_BLEND", "0.50"))
    MT5_FX_CONF_SOFT_FILTER_MIN_LOW: float = float(os.getenv("MT5_FX_CONF_SOFT_FILTER_MIN_LOW", "55"))
    MT5_FX_CONF_SOFT_FILTER_MIN_GAP: float = float(os.getenv("MT5_FX_CONF_SOFT_FILTER_MIN_GAP", "1.0"))
    MT5_EXEC_REASONS_DELTA_MARKER_UTC: str = os.getenv("MT5_EXEC_REASONS_DELTA_MARKER_UTC", "")
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_ENABLED: bool = os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_LOOKBACK_DAYS: int = int(os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_LOOKBACK_DAYS", "60"))
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MIN_RESOLVED: int = int(os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MIN_RESOLVED", "8"))
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MIN_PROB: float = float(os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MIN_PROB", "0.40"))
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MAX_PROB: float = float(os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_MAX_PROB", "0.55"))
    NEURAL_BRAIN_FX_LEARNED_THRESHOLD_BLEND: float = float(os.getenv("NEURAL_BRAIN_FX_LEARNED_THRESHOLD_BLEND", "0.50"))
    NEURAL_BRAIN_FILTER_MIN_SAMPLES: int = int(os.getenv("NEURAL_BRAIN_FILTER_MIN_SAMPLES", "60"))
    NEURAL_BRAIN_FILTER_MIN_VAL_ACC: float = float(os.getenv("NEURAL_BRAIN_FILTER_MIN_VAL_ACC", "0.52"))
    NEURAL_BRAIN_FILTER_MAX_MODEL_AGE_HOURS: int = int(os.getenv("NEURAL_BRAIN_FILTER_MAX_MODEL_AGE_HOURS", "720"))
    NEURAL_BRAIN_SOFT_ADJUST: bool = os.getenv("NEURAL_BRAIN_SOFT_ADJUST", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_SOFT_ADJUST_WEIGHT: float = float(os.getenv("NEURAL_BRAIN_SOFT_ADJUST_WEIGHT", "0.35"))
    NEURAL_BRAIN_SOFT_ADJUST_MAX_DELTA: float = float(os.getenv("NEURAL_BRAIN_SOFT_ADJUST_MAX_DELTA", "8.0"))
    NEURAL_BRAIN_REASON_STUDY_ENABLED: bool = os.getenv("NEURAL_BRAIN_REASON_STUDY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS: int = int(os.getenv("NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS", os.getenv("NEURAL_BRAIN_SYNC_DAYS", "120")))
    NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED: int = int(os.getenv("NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED", "8"))
    NEURAL_BRAIN_REASON_STUDY_WEIGHT: float = float(os.getenv("NEURAL_BRAIN_REASON_STUDY_WEIGHT", "0.20"))
    NEURAL_BRAIN_REASON_STUDY_MAX_DELTA: float = float(os.getenv("NEURAL_BRAIN_REASON_STUDY_MAX_DELTA", "4.0"))
    NEURAL_BRAIN_REASON_STUDY_CACHE_SEC: int = int(os.getenv("NEURAL_BRAIN_REASON_STUDY_CACHE_SEC", "120"))
    SIGNAL_FEEDBACK_ENABLED: bool = os.getenv("SIGNAL_FEEDBACK_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS: int = int(os.getenv("NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS", "400"))
    NEURAL_BRAIN_PSEUDO_LABEL_ENABLED: bool = os.getenv("NEURAL_BRAIN_PSEUDO_LABEL_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_BRAIN_PSEUDO_LABEL_MIN_HOURS: float = float(os.getenv("NEURAL_BRAIN_PSEUDO_LABEL_MIN_HOURS", "2"))
    NEURAL_BRAIN_PSEUDO_LABEL_MIN_ABS_R: float = float(os.getenv("NEURAL_BRAIN_PSEUDO_LABEL_MIN_ABS_R", "0.25"))
    NEURAL_MISSION_AUTO_ENABLED: bool = os.getenv("NEURAL_MISSION_AUTO_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_INTERVAL_MIN: int = int(os.getenv("NEURAL_MISSION_INTERVAL_MIN", "60"))
    NEURAL_MISSION_SYMBOLS: str = os.getenv("NEURAL_MISSION_SYMBOLS", "XAUUSD,ETHUSD,BTCUSD,GBPUSD")
    NEURAL_MISSION_ITERATIONS_PER_CYCLE: int = int(os.getenv("NEURAL_MISSION_ITERATIONS_PER_CYCLE", "1"))
    NEURAL_MISSION_TARGET_WIN_RATE: float = float(os.getenv("NEURAL_MISSION_TARGET_WIN_RATE", "58.0"))
    NEURAL_MISSION_TARGET_PROFIT_FACTOR: float = float(os.getenv("NEURAL_MISSION_TARGET_PROFIT_FACTOR", "1.2"))
    NEURAL_MISSION_MIN_TRADES: int = int(os.getenv("NEURAL_MISSION_MIN_TRADES", "12"))
    NEURAL_MISSION_NOTIFY_TELEGRAM: bool = os.getenv("NEURAL_MISSION_NOTIFY_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_NOTIFY_EACH_ITERATION: bool = os.getenv("NEURAL_MISSION_NOTIFY_EACH_ITERATION", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_APPLY_POLICY_DRAFT: bool = os.getenv("NEURAL_MISSION_APPLY_POLICY_DRAFT", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_AUTO_ALLOWLIST_ENABLED: bool = os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_AUTO_ALLOWLIST_MIN_TRADES: int = int(os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_MIN_TRADES", "12"))
    NEURAL_MISSION_AUTO_ALLOWLIST_MIN_WIN_RATE: float = float(os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_MIN_WIN_RATE", "58.0"))
    NEURAL_MISSION_AUTO_ALLOWLIST_MIN_PROFIT_FACTOR: float = float(os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_MIN_PROFIT_FACTOR", "1.2"))
    NEURAL_MISSION_AUTO_ALLOWLIST_MIN_NET_PNL: float = float(os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_MIN_NET_PNL", "0.0"))
    NEURAL_MISSION_AUTO_ALLOWLIST_MAX_ADD_PER_CYCLE: int = int(os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_MAX_ADD_PER_CYCLE", "3"))
    NEURAL_MISSION_AUTO_ALLOWLIST_PERSIST_ENV: bool = os.getenv("NEURAL_MISSION_AUTO_ALLOWLIST_PERSIST_ENV", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_MISSION_ENV_BACKUP_KEEP: int = int(os.getenv("NEURAL_MISSION_ENV_BACKUP_KEEP", "20"))
    NEURAL_MISSION_INCLUDE_EXPERIMENTAL_LANES: bool = os.getenv("NEURAL_MISSION_INCLUDE_EXPERIMENTAL_LANES", "0").strip().lower() in ("1", "true", "yes", "on")
    # Neural gate learning/calibration loop (decision logging + shadow feedback + canary policy)
    NEURAL_GATE_LEARNING_ENABLED: bool = os.getenv("NEURAL_GATE_LEARNING_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_LEARNING_LOOKBACK_HOURS: int = int(os.getenv("NEURAL_GATE_LEARNING_LOOKBACK_HOURS", "168"))
    NEURAL_GATE_LEARNING_DB_PATH: str = os.getenv("NEURAL_GATE_LEARNING_DB_PATH", "data/neural_gate_learning.db")
    NEURAL_GATE_LEARNING_REPORT_EACH_CYCLE: bool = os.getenv("NEURAL_GATE_LEARNING_REPORT_EACH_CYCLE", "1").strip().lower() in ("1", "true", "yes", "on")
    # Neural gate learning loop reads only cTrader OpenAPI execution_journal (journal_id = row_id + offset).
    NEURAL_GATE_CTRADER_SYNC_ENABLED: bool = os.getenv("NEURAL_GATE_CTRADER_SYNC_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_JOURNAL_ID_CTRADER_OFFSET: int = int(os.getenv("NEURAL_GATE_JOURNAL_ID_CTRADER_OFFSET", "1000000000"))
    # Pre-dispatch microstructure snapshot from local OpenAPI SQLite (ticks/depth), merged into signal.raw_scores.
    CTRADER_EXEC_FEATURE_PACK_ENABLED: bool = os.getenv("CTRADER_EXEC_FEATURE_PACK_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    CTRADER_EXEC_FEATURE_LOOKBACK_SEC: int = int(os.getenv("CTRADER_EXEC_FEATURE_LOOKBACK_SEC", "32"))
    CTRADER_EXEC_FEATURE_MAX_TICKS: int = int(os.getenv("CTRADER_EXEC_FEATURE_MAX_TICKS", "24"))
    NEURAL_GATE_SHADOW_MATCH_WINDOW_SEC: int = int(os.getenv("NEURAL_GATE_SHADOW_MATCH_WINDOW_SEC", "300"))
    NEURAL_GATE_SHADOW_MATCH_ENTRY_TOL: float = float(os.getenv("NEURAL_GATE_SHADOW_MATCH_ENTRY_TOL", "0.20"))
    NEURAL_GATE_SHADOW_WEIGHT: float = float(os.getenv("NEURAL_GATE_SHADOW_WEIGHT", "0.35"))
    NEURAL_GATE_CANARY_ENABLED: bool = os.getenv("NEURAL_GATE_CANARY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_CANARY_POLICY_PATH: str = os.getenv("NEURAL_GATE_CANARY_POLICY_PATH", "data/runtime/neural_gate_canary_policy.json")
    NEURAL_GATE_CANARY_POLICY_TTL_SEC: int = int(os.getenv("NEURAL_GATE_CANARY_POLICY_TTL_SEC", "900"))
    NEURAL_GATE_CANARY_SYMBOL: str = os.getenv("NEURAL_GATE_CANARY_SYMBOL", "XAUUSD")
    NEURAL_GATE_CANARY_SOURCE: str = os.getenv("NEURAL_GATE_CANARY_SOURCE", "scalp_xauusd")
    NEURAL_GATE_CANARY_SOURCES: str = os.getenv("NEURAL_GATE_CANARY_SOURCES", os.getenv("NEURAL_GATE_CANARY_SOURCE", "scalp_xauusd"))
    NEURAL_GATE_CANARY_LOOKBACK_DAYS: int = int(os.getenv("NEURAL_GATE_CANARY_LOOKBACK_DAYS", "7"))
    NEURAL_GATE_CANARY_MIN_CONFIDENCE: float = float(os.getenv("NEURAL_GATE_CANARY_MIN_CONFIDENCE", "80"))
    NEURAL_GATE_CANARY_REQUIRE_FORCE_MODE: bool = os.getenv("NEURAL_GATE_CANARY_REQUIRE_FORCE_MODE", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_CANARY_LOWER_GAP: float = float(os.getenv("NEURAL_GATE_CANARY_LOWER_GAP", "0.03"))
    NEURAL_GATE_CANARY_ALLOW_LOW_FLOOR: float = float(os.getenv("NEURAL_GATE_CANARY_ALLOW_LOW_FLOOR", "0.55"))
    NEURAL_GATE_CANARY_FIXED_ALLOW_LOW: float = float(os.getenv("NEURAL_GATE_CANARY_FIXED_ALLOW_LOW", "0.0"))
    NEURAL_GATE_CANARY_MIN_SHADOW_SAMPLES: int = int(os.getenv("NEURAL_GATE_CANARY_MIN_SHADOW_SAMPLES", "12"))
    NEURAL_GATE_CANARY_TARGET_WR: float = float(os.getenv("NEURAL_GATE_CANARY_TARGET_WR", "0.58"))
    NEURAL_GATE_CANARY_MIN_NET_PNL: float = float(os.getenv("NEURAL_GATE_CANARY_MIN_NET_PNL", "0.0"))
    NEURAL_GATE_CANARY_VOLUME_CAP: float = float(os.getenv("NEURAL_GATE_CANARY_VOLUME_CAP", "0.25"))
    NEURAL_GATE_CANARY_MAX_POSITIONS_PER_SYMBOL: int = int(os.getenv("NEURAL_GATE_CANARY_MAX_POSITIONS_PER_SYMBOL", "1"))
    NEURAL_GATE_CANARY_TRIPWIRE_WINDOW_HOURS: int = int(os.getenv("NEURAL_GATE_CANARY_TRIPWIRE_WINDOW_HOURS", "24"))
    NEURAL_GATE_CANARY_TRIPWIRE_MAX_CONSEC_LOSSES: int = int(os.getenv("NEURAL_GATE_CANARY_TRIPWIRE_MAX_CONSEC_LOSSES", "2"))
    NEURAL_GATE_CANARY_TRIPWIRE_MAX_NET_LOSS_USD: float = float(os.getenv("NEURAL_GATE_CANARY_TRIPWIRE_MAX_NET_LOSS_USD", "20"))
    NEURAL_GATE_CANARY_TRIPWIRE_MAX_REJECTION_RATE: float = float(os.getenv("NEURAL_GATE_CANARY_TRIPWIRE_MAX_REJECTION_RATE", "0.30"))
    NEURAL_GATE_CANARY_AUTO_STEP_ENABLED: bool = os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_CANARY_AUTO_STEP_LOOKBACK_HOURS: int = int(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_LOOKBACK_HOURS", "6"))
    NEURAL_GATE_CANARY_AUTO_STEP_MIN_ELIGIBLE: int = int(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_MIN_ELIGIBLE", "8"))
    NEURAL_GATE_CANARY_AUTO_STEP_TARGET_FILL_RATE: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_TARGET_FILL_RATE", "0.20"))
    NEURAL_GATE_CANARY_AUTO_STEP_MIN_BLOCK_RATE: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_MIN_BLOCK_RATE", "0.40"))
    NEURAL_GATE_CANARY_AUTO_STEP_DOWN_STEP: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_DOWN_STEP", "0.01"))
    NEURAL_GATE_CANARY_AUTO_STEP_MIN_ALLOW_LOW: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_MIN_ALLOW_LOW", "0.53"))
    NEURAL_GATE_CANARY_AUTO_STEP_VOLUME_CAP_STEP: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_VOLUME_CAP_STEP", "0.02"))
    NEURAL_GATE_CANARY_AUTO_STEP_MIN_VOLUME_CAP: float = float(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_MIN_VOLUME_CAP", "0.12"))
    NEURAL_GATE_CANARY_AUTO_STEP_COOLDOWN_MIN: int = int(os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_COOLDOWN_MIN", "30"))
    NEURAL_GATE_CANARY_AUTO_STEP_REQUIRE_ACTIVE_SCOPE: bool = os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_REQUIRE_ACTIVE_SCOPE", "1").strip().lower() in ("1", "true", "yes", "on")
    NEURAL_GATE_CANARY_AUTO_STEP_STOP_ON_TRIPWIRE: bool = os.getenv("NEURAL_GATE_CANARY_AUTO_STEP_STOP_ON_TRIPWIRE", "1").strip().lower() in ("1", "true", "yes", "on")

    # ── Internal Mappings ─────────────────────────────────────────────────────
    TF_TO_CCXT: dict = {
        "1m": "1m",  "5m": "5m",  "15m": "15m", "30m": "30m",
        "1h": "1h",  "4h": "4h",  "1d": "1d",   "1w": "1w",
    }
    TF_TO_YFINANCE: dict = {
        "1m": "1m",  "5m": "5m",  "15m": "15m", "30m": "30m",
        "1h": "1h",  "4h": "1h",  "1d": "1d",   "1w": "1wk",
    }

    # ── Priority Crypto Pairs (always included in scans) ──────────────────────
    PRIORITY_PAIRS: list = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT",  "SOL/USDT",  "XRP/USDT",
        "AVAX/USDT","DOGE/USDT","ADA/USDT",  "DOT/USDT",  "LINK/USDT",
        "POL/USDT", "LTC/USDT","BCH/USDT",   "UNI/USDT",  "ATOM/USDT",
    ]

    # ── Trading Sessions (UTC) ────────────────────────────────────────────────
    SESSIONS: dict = {
        "asian":    {"start": "00:00", "end": "08:00"},
        "london":   {"start": "07:00", "end": "16:00"},
        "new_york": {"start": "12:00", "end": "21:00"},
        "overlap":  {"start": "12:00", "end": "16:00"},
    }

    @classmethod
    def validate(cls) -> list:
        """Return list of missing critical config keys."""
        missing = []
        if not cls.has_any_ai_key():
            missing.append(
                "AI KEY MISSING  ← set GROQ_API_KEY or GEMINI_API_KEY or GEMINI_VERTEX_AI_API_KEY or ANTHROPIC_API_KEY"
            )
        if not cls.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID  ← add your Telegram Chat ID")
        return missing

    @classmethod
    def summary(cls) -> str:
        """One-line config summary for startup log."""
        tg = "✅" if cls.TELEGRAM_BOT_TOKEN and cls.TELEGRAM_CHAT_ID else "⚠️ Chat ID missing"
        provider = cls.resolve_ai_provider()
        ai = f"✅ {provider}" if provider != "none" else "❌ Missing"
        return (
            f"AI={ai} | TG={tg} | "
            f"Exchange={cls.CRYPTO_EXCHANGE.upper()} | "
            f"Confidence≥{cls.MIN_SIGNAL_CONFIDENCE}%"
        )

    @classmethod
    def has_any_ai_key(cls) -> bool:
        return bool(cls.GROQ_API_KEY or cls.has_gemini_key() or cls.ANTHROPIC_API_KEY or cls.OPENROUTER_API_KEY)

    @classmethod
    def has_gemini_key(cls) -> bool:
        return bool(cls.GEMINI_VERTEX_AI_API_KEY or cls.GEMINI_API_KEY)

    @classmethod
    def gemini_mode(cls) -> str:
        if cls.GEMINI_VERTEX_AI_API_KEY:
            return "vertex"
        if cls.GEMINI_API_KEY:
            return "direct"
        return "none"

    @classmethod
    def gemini_model(cls) -> str:
        # Vertex and direct Gemini can use different model defaults.
        if cls.gemini_mode() == "vertex":
            return cls.GEMINI_VERTEX_MODEL
        return cls.GEMINI_MODEL

    @classmethod
    def resolve_ai_provider(cls) -> str:
        """Resolve active AI provider using preferred order."""
        pref = (cls.AI_PROVIDER or "auto").strip().lower()
        if pref in ("groq", "gemini", "anthropic", "openrouter"):
            if pref == "groq" and cls.GROQ_API_KEY:
                return "groq"
            if pref == "gemini" and cls.has_gemini_key():
                return "gemini"
            if pref == "anthropic" and cls.ANTHROPIC_API_KEY:
                return "anthropic"
            if pref == "openrouter" and cls.OPENROUTER_API_KEY:
                return "openrouter"
        if cls.GROQ_API_KEY:
            return "groq"
        if cls.has_gemini_key():
            return "gemini"
        if cls.ANTHROPIC_API_KEY:
            return "anthropic"
        if cls.OPENROUTER_API_KEY:
            return "openrouter"
        return "none"

    @classmethod
    def model_for_provider(cls, provider: str) -> str:
        provider = (provider or "").lower()
        if provider == "groq":
            return cls.GROQ_MODEL
        if provider == "gemini":
            return cls.gemini_model()
        if provider == "openrouter":
            return cls.OPENROUTER_MODEL
        return cls.AI_MODEL

    @classmethod
    def get_admin_ids(cls) -> set[int]:
        """Return Telegram admin user IDs allowed to run bot commands."""
        ids: set[int] = set()
        raw = (cls.TELEGRAM_ADMIN_IDS or "").strip()
        if raw:
            for part in raw.split(","):
                part = part.strip()
                if part and part.lstrip("-").isdigit():
                    ids.add(int(part))
        # Fallback for private-chat deployments where chat ID equals user ID.
        if cls.TELEGRAM_CHAT_ID and cls.TELEGRAM_CHAT_ID.lstrip("-").isdigit():
            ids.add(int(cls.TELEGRAM_CHAT_ID))
        return ids

    @classmethod
    def _parse_symbol_set(cls, raw: str) -> set[str]:
        values: set[str] = set()
        for part in (raw or "").split(","):
            item = part.strip().upper()
            if item:
                values.add(item)
        return values

    @classmethod
    def _parse_lower_set(cls, raw: str, *, separators: tuple[str, ...] = (",",)) -> set[str]:
        parts = [str(raw or "")]
        for sep in separators:
            next_parts: list[str] = []
            for chunk in parts:
                next_parts.extend(str(chunk or "").split(sep))
            parts = next_parts
        out: set[str] = set()
        for part in parts:
            item = str(part or "").strip().lower().replace(" ", "_")
            if item:
                out.add(item)
        return out

    @classmethod
    def _parse_source_direction_set(cls, raw: str) -> set[tuple[str, str]]:
        out: set[tuple[str, str]] = set()
        for part in str(raw or "").split(","):
            item = str(part or "").strip().lower().replace(" ", "_")
            if not item:
                continue
            if item in {"*", "all"}:
                out.add(("*", "*"))
                continue
            if ":" not in item:
                out.add((item, "*"))
                continue
            source, direction = item.rsplit(":", 1)
            source = source.strip()
            direction = direction.strip()
            if direction not in {"long", "short", "buy", "sell", "*", "all"}:
                source = item
                direction = "*"
            if direction == "buy":
                direction = "long"
            elif direction == "sell":
                direction = "short"
            elif direction == "all":
                direction = "*"
            if source:
                out.add((source, direction or "*"))
        return out

    @classmethod
    def _parse_signature_set(cls, raw: str) -> set[str]:
        out: set[str] = set()
        for chunk in str(raw or "").split("|"):
            tokens = [
                str(part or "").strip().lower().replace(" ", "_")
                for part in str(chunk or "").split(",")
                if str(part or "").strip()
            ]
            if tokens:
                out.add(",".join(tokens))
        return out

    @classmethod
    def _parse_upper_map(cls, raw: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for chunk in (raw or "").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            left, right = item.split("=", 1)
            k = left.strip().upper()
            v = right.strip().upper()
            if k and v:
                out[k] = v
        return out

    @classmethod
    def _parse_float_map(cls, raw: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for chunk in (raw or "").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            left, right = item.split("=", 1)
            k = left.strip().upper()
            if not k:
                continue
            try:
                v = float(right.strip())
            except Exception:
                continue
            out[k] = v
        return out

    @classmethod
    def _parse_int_map(cls, raw: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for chunk in (raw or "").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            left, right = item.split("=", 1)
            k = left.strip().upper()
            if not k:
                continue
            try:
                v = int(float(right.strip()))
            except Exception:
                continue
            out[k] = v
        return out

    @classmethod
    def _parse_bool_or_auto_map(cls, raw: str) -> dict[str, Optional[bool]]:
        out: dict[str, Optional[bool]] = {}
        for chunk in (raw or "").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            left, right = item.split("=", 1)
            k = left.strip().upper()
            if not k:
                continue
            v = right.strip().lower()
            if v in {"auto", "none", "default", ""}:
                out[k] = None
            elif v in {"1", "true", "yes", "on"}:
                out[k] = True
            elif v in {"0", "false", "no", "off"}:
                out[k] = False
        return out

    @classmethod
    def _parse_int_list(cls, raw: str) -> list[int]:
        out: list[int] = []
        for part in (raw or "").split(","):
            item = part.strip()
            if not item:
                continue
            try:
                out.append(int(item))
            except Exception:
                continue
        return out

    @classmethod
    def _parse_json_value(cls, raw: str):
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    @classmethod
    def get_mt5_allow_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_ALLOW_SYMBOLS)

    @classmethod
    def get_ctrader_allowed_sources(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_ALLOWED_SOURCES)

    @classmethod
    def get_ctrader_quarantined_source_directions(cls) -> set[tuple[str, str]]:
        return cls._parse_source_direction_set(cls.CTRADER_QUARANTINED_SOURCE_DIRECTIONS)

    @classmethod
    def get_ctrader_protected_source_directions(cls) -> set[tuple[str, str]]:
        return cls._parse_source_direction_set(cls.CTRADER_PROTECTED_SOURCE_DIRECTIONS)

    @classmethod
    def get_ctrader_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CTRADER_ALLOWED_SYMBOLS)

    @classmethod
    def get_ctrader_xau_scheduled_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.CTRADER_XAU_SCHEDULED_ALLOWED_SESSIONS)

    @classmethod
    def get_ctrader_xau_scheduled_allowed_timeframes(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_XAU_SCHEDULED_ALLOWED_TIMEFRAMES)

    @classmethod
    def get_ctrader_xau_scheduled_allowed_entry_types(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_XAU_SCHEDULED_ALLOWED_ENTRY_TYPES)

    @classmethod
    def get_ctrader_btc_winner_allowed_sessions_weekend(cls) -> set[str]:
        return cls._parse_signature_set(cls.CTRADER_BTC_WINNER_ALLOWED_SESSIONS_WEEKEND)

    @classmethod
    def get_persistent_canary_allowed_sources(cls) -> set[str]:
        return cls._parse_lower_set(cls.PERSISTENT_CANARY_ALLOWED_SOURCES)

    @classmethod
    def get_persistent_canary_direct_allowed_sources(cls) -> set[str]:
        return cls._parse_lower_set(cls.PERSISTENT_CANARY_DIRECT_ALLOWED_SOURCES)

    @classmethod
    def get_persistent_canary_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.PERSISTENT_CANARY_ALLOWED_SYMBOLS)

    @classmethod
    def get_persistent_canary_strategy_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.PERSISTENT_CANARY_STRATEGY_FAMILIES)

    @classmethod
    def get_persistent_canary_experimental_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES)

    @classmethod
    def get_btc_weekday_lob_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.BTC_WEEKDAY_LOB_ALLOWED_SESSIONS)

    @classmethod
    def get_btc_weekday_lob_allowed_patterns(cls) -> set[str]:
        return cls._parse_lower_set(cls.BTC_WEEKDAY_LOB_ALLOWED_PATTERNS)

    @classmethod
    def get_eth_weekday_probe_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.ETH_WEEKDAY_PROBE_ALLOWED_SESSIONS)

    @classmethod
    def get_eth_weekday_probe_allowed_patterns(cls) -> set[str]:
        return cls._parse_lower_set(cls.ETH_WEEKDAY_PROBE_ALLOWED_PATTERNS)

    @classmethod
    def get_crypto_weekend_btc_allowed_sessions(cls) -> set[str]:
        raw = cls.CRYPTO_WEEKEND_BTC_ALLOWED_SESSIONS.strip()
        if raw == "*":
            return {"*"}
        return cls._parse_signature_set(raw)

    @classmethod
    def get_crypto_weekend_eth_allowed_sessions(cls) -> set[str]:
        raw = cls.CRYPTO_WEEKEND_ETH_ALLOWED_SESSIONS.strip()
        if raw == "*":
            return {"*"}
        return cls._parse_signature_set(raw)

    @classmethod
    def get_crypto_flow_short_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_FLOW_SHORT_ALLOWED_SYMBOLS)

    @classmethod
    def get_crypto_flow_short_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.CRYPTO_FLOW_SHORT_ALLOWED_SESSIONS)

    @classmethod
    def get_crypto_flow_buy_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_FLOW_BUY_ALLOWED_SYMBOLS)

    @classmethod
    def get_crypto_flow_buy_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.CRYPTO_FLOW_BUY_ALLOWED_SESSIONS)

    @classmethod
    def get_crypto_winner_confirmed_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_WINNER_CONFIRMED_ALLOWED_SYMBOLS)

    @classmethod
    def get_crypto_winner_confirmed_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.CRYPTO_WINNER_CONFIRMED_ALLOWED_SESSIONS)

    @classmethod
    def get_crypto_behavioral_retest_allowed_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SYMBOLS)

    @classmethod
    def get_crypto_behavioral_retest_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.CRYPTO_BEHAVIORAL_RETEST_ALLOWED_SESSIONS)

    @classmethod
    def get_crypto_behavioral_retest_allowed_patterns(cls) -> set[str]:
        return cls._parse_lower_set(cls.CRYPTO_BEHAVIORAL_RETEST_ALLOWED_PATTERNS)

    @classmethod
    def get_ctrader_xau_active_families(cls) -> set[str]:
        families = cls._parse_lower_set(cls.CTRADER_XAU_ACTIVE_FAMILIES)
        if bool(getattr(cls, "DEXTER_MEMPALACE_FAMILY_LANE_ENABLED", False)):
            fam = str(getattr(cls, "DEXTER_MEMPALACE_FAMILY_NAME", "") or "").strip().lower()
            if fam:
                families.add(fam)
        return families

    @classmethod
    def get_dexter_mempalace_source_tokens(cls) -> set[str]:
        return cls._parse_lower_set(cls.DEXTER_MEMPALACE_SOURCE_TOKENS)

    @classmethod
    def get_ctrader_pm_impulse_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_PM_IMPULSE_FAMILIES)

    @classmethod
    def get_ctrader_pm_corrective_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_PM_CORRECTIVE_FAMILIES)

    @classmethod
    def get_ctrader_market_capture_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CTRADER_MARKET_CAPTURE_SYMBOLS)

    @classmethod
    def get_ctrader_pending_order_dynamic_reprice_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ALLOWED_FAMILIES)

    @classmethod
    def get_ctrader_pending_order_follow_stop_families(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_PENDING_ORDER_FOLLOW_STOP_ALLOWED_FAMILIES)

    @classmethod
    def get_ctrader_store_feed_sources(cls) -> set[str]:
        return cls._parse_lower_set(cls.CTRADER_STORE_FEED_SOURCES)

    @classmethod
    def get_ctrader_default_volume_symbol_overrides(cls) -> dict[str, int]:
        return cls._parse_int_map(cls.CTRADER_DEFAULT_VOLUME_SYMBOL_OVERRIDES)

    @classmethod
    def get_ctrader_market_entry_max_drift_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.CTRADER_MARKET_ENTRY_MAX_DRIFT_SYMBOL_OVERRIDES)

    @classmethod
    def get_ctrader_user_payload(cls) -> dict:
        payload = cls._parse_json_value(cls.CTRADER_USER_ID_JSON)
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def get_ctrader_accounts_payload(cls) -> dict:
        payload = cls._parse_json_value(cls.CTRADER_ACCOUNTS_JSON)
        if isinstance(payload, list):
            return {"data": payload}
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def get_ctrader_accounts(cls) -> list[dict]:
        payload = cls.get_ctrader_accounts_payload()
        rows = []
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                rows = payload.get("data", [])
            elif isinstance(payload.get("ctidTraderAccount"), list):
                rows = payload.get("ctidTraderAccount", [])
            elif isinstance(payload.get("accounts"), list):
                rows = payload.get("accounts", [])
        out: list[dict] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            normalized = dict(row or {})
            if "accountId" not in normalized and "ctidTraderAccountId" in normalized:
                normalized["accountId"] = normalized.get("ctidTraderAccountId")
            if "accountNumber" not in normalized and "traderLogin" in normalized:
                normalized["accountNumber"] = normalized.get("traderLogin")
            if "live" not in normalized and "isLive" in normalized:
                normalized["live"] = normalized.get("isLive")
            out.append(normalized)
        return out

    @classmethod
    def find_ctrader_account(cls, preferred: str = "", use_demo: Optional[bool] = None) -> Optional[dict]:
        token = str(preferred or "").strip()
        rows = cls.get_ctrader_accounts()
        if not rows:
            return None
        if token:
            for row in rows:
                for key in ("accountId", "accountNumber", "traderLogin"):
                    val = str(row.get(key, "") or "").strip()
                    if val and val == token:
                        return dict(row)
        filtered = rows
        if use_demo is not None:
            target_live = not bool(use_demo)
            scoped = [row for row in rows if bool(row.get("live", False)) == target_live]
            if scoped:
                filtered = scoped
        preferred_rows: list[dict] = []
        preferred_rows.extend(
            row for row in filtered
            if str(row.get("accountStatus", "ACTIVE") or "ACTIVE").upper() == "ACTIVE"
            and str(row.get("depositCurrency", "") or "").upper() == "USD"
            and (not bool(row.get("deleted", False)))
        )
        preferred_rows.extend(
            row for row in filtered
            if str(row.get("accountStatus", "ACTIVE") or "ACTIVE").upper() == "ACTIVE"
            and (not bool(row.get("deleted", False)))
        )
        preferred_rows.extend(row for row in filtered if not bool(row.get("deleted", False)))
        preferred_rows.extend(rows)
        seen: set[str] = set()
        for row in preferred_rows:
            key = str(row.get("accountId", "") or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            return dict(row)
        return None

    @classmethod
    def get_mt5_bypass_test_sources(cls) -> set[str]:
        return {str(v).strip().lower() for v in cls._parse_symbol_set(cls.MT5_BYPASS_TEST_SOURCES)}

    @classmethod
    def get_mt5_bypass_test_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_BYPASS_TEST_SYMBOLS)

    @classmethod
    def get_mt5_best_lane_sources(cls) -> set[str]:
        return {str(v).strip().lower() for v in cls._parse_symbol_set(cls.MT5_BEST_LANE_SOURCES)}

    @classmethod
    def get_mt5_best_lane_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_BEST_LANE_SYMBOLS)

    @classmethod
    def get_mt5_best_lane_min_confidence_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_BEST_LANE_MIN_CONFIDENCE_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_xau_scheduled_live_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.MT5_XAU_SCHEDULED_LIVE_SESSIONS)

    @classmethod
    def get_mt5_xau_scheduled_live_timeframes(cls) -> set[str]:
        return cls._parse_lower_set(cls.MT5_XAU_SCHEDULED_LIVE_TIMEFRAMES)

    @classmethod
    def get_mt5_scalp_xau_live_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.MT5_SCALP_XAU_LIVE_SESSIONS)

    @classmethod
    def get_mt5_preclose_flatten_exclude_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_PRE_CLOSE_FLATTEN_EXCLUDE_SYMBOLS)

    @classmethod
    def get_mt5_preclose_flatten_include_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_PRE_CLOSE_FLATTEN_INCLUDE_SYMBOLS)

    @classmethod
    def get_xau_event_shock_kill_switch_themes(cls) -> set[str]:
        return cls._parse_symbol_set(cls.XAU_EVENT_SHOCK_KILL_SWITCH_THEMES)

    @classmethod
    def get_trial_crypto_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.TRIAL_CRYPTO_SYMBOLS)

    @classmethod
    def get_canary_post_trade_audit_milestones(cls) -> list[int]:
        vals = [v for v in cls._parse_int_list(cls.CANARY_POST_TRADE_AUDIT_MILESTONES) if int(v) > 0]
        return sorted(list(dict.fromkeys(vals))) or [3, 5]

    @classmethod
    def get_strategy_walk_forward_windows_days(cls) -> list[int]:
        vals = [v for v in cls._parse_int_list(cls.STRATEGY_WALK_FORWARD_WINDOWS_DAYS) if int(v) > 0]
        vals = sorted(list(dict.fromkeys(vals)))
        return vals or [3, 7, 14]

    @classmethod
    def get_fx_major_symbols(cls) -> list[str]:
        vals = sorted(cls._parse_symbol_set(cls.FX_MAJOR_SYMBOLS))
        return vals or ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF"]

    @classmethod
    def get_crypto_auto_focus_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_AUTO_FOCUS_SYMBOLS)

    @classmethod
    def get_crypto_sniper_exclude_bases(cls) -> set[str]:
        return cls._parse_symbol_set(cls.CRYPTO_SNIPER_EXCLUDE_BASES)

    @classmethod
    def get_scalping_symbols(cls) -> set[str]:
        out = cls._parse_symbol_set(cls.SCALPING_SYMBOLS)
        return out or {"XAUUSD", "ETHUSD"}

    @classmethod
    def get_scalping_eth_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.SCALPING_ETH_ALLOWED_SESSIONS)

    @classmethod
    def get_scalping_eth_allowed_sessions_weekend(cls) -> set[str]:
        return cls._parse_signature_set(cls.SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND)

    @classmethod
    def get_scalping_btc_allowed_sessions(cls) -> set[str]:
        return cls._parse_signature_set(cls.SCALPING_BTC_ALLOWED_SESSIONS)

    @classmethod
    def get_scalping_btc_allowed_sessions_weekend(cls) -> set[str]:
        return cls._parse_signature_set(cls.SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND)

    @classmethod
    def get_signal_monitor_auto_symbols(cls) -> list[str]:
        alias = {
            "GOLD": "XAUUSD",
            "XAU": "XAUUSD",
            "XAUUSD": "XAUUSD",
            "ETH": "ETHUSD",
            "ETHUSD": "ETHUSD",
            "ETHUSDT": "ETHUSD",
            "ETH/USDT": "ETHUSD",
            "BTC": "BTCUSD",
            "BTCUSD": "BTCUSD",
            "BTCUSDT": "BTCUSD",
            "BTC/USDT": "BTCUSD",
        }
        out: list[str] = []
        seen: set[str] = set()
        for part in (cls.SIGNAL_MONITOR_AUTO_PUSH_SYMBOLS or "").split(","):
            token = str(part or "").strip().upper().replace(" ", "")
            if not token:
                continue
            mapped = alias.get(token, token)
            if mapped in seen:
                continue
            seen.add(mapped)
            out.append(mapped)
        return out or ["XAUUSD"]

    @classmethod
    def get_signal_monitor_auto_window_mode(cls) -> str:
        token = str(cls.SIGNAL_MONITOR_AUTO_PUSH_WINDOW_MODE or "today").strip().lower().replace("-", "_").replace(" ", "_")
        if token in {"today", "yesterday", "this_week", "this_month", "rolling_days"}:
            return token
        return "today"

    @classmethod
    def scalping_symbol_enabled(cls, symbol: str) -> bool:
        return str(symbol or "").strip().upper() in cls.get_scalping_symbols()

    @classmethod
    def get_econ_alert_windows(cls) -> list[int]:
        windows = [x for x in cls._parse_int_list(cls.ECON_ALERT_WINDOWS) if x > 0]
        return sorted(set(windows), reverse=True) or [60, 15]

    @classmethod
    def get_econ_alert_currencies(cls) -> set[str]:
        return cls._parse_symbol_set(cls.ECON_ALERT_CURRENCIES)

    @classmethod
    def get_mt5_block_symbols(cls) -> set[str]:
        return cls._parse_symbol_set(cls.MT5_BLOCK_SYMBOLS)

    @classmethod
    def get_macro_news_source_quality_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MACRO_NEWS_SOURCE_QUALITY_OVERRIDES)

    @classmethod
    def get_mt5_min_conf_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_MIN_SIGNAL_CONFIDENCE_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_margin_usage_pct_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_MAX_MARGIN_USAGE_PCT_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_risk_multiplier_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_RISK_MULTIPLIER_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_risk_multiplier_min_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_RISK_MULTIPLIER_MIN_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_risk_multiplier_max_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_RISK_MULTIPLIER_MAX_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_canary_force_symbol_overrides(cls) -> dict[str, Optional[bool]]:
        return cls._parse_bool_or_auto_map(cls.MT5_CANARY_FORCE_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_risk_gov_daily_loss_limit_usd_lane_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD_LANE_OVERRIDES)

    @classmethod
    def get_mt5_risk_gov_daily_loss_limit_pct_lane_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_RISK_GOV_DAILY_LOSS_LIMIT_PCT_LANE_OVERRIDES)

    @classmethod
    def get_mt5_risk_gov_max_consecutive_losses_lane_overrides(cls) -> dict[str, int]:
        return cls._parse_int_map(cls.MT5_RISK_GOV_MAX_CONSECUTIVE_LOSSES_LANE_OVERRIDES)

    @classmethod
    def get_mt5_risk_gov_loss_cooldown_min_lane_overrides(cls) -> dict[str, int]:
        return cls._parse_int_map(cls.MT5_RISK_GOV_LOSS_COOLDOWN_MIN_LANE_OVERRIDES)

    @classmethod
    def get_mt5_risk_gov_max_rejections_1h_lane_overrides(cls) -> dict[str, int]:
        return cls._parse_int_map(cls.MT5_RISK_GOV_MAX_REJECTIONS_1H_LANE_OVERRIDES)

    @classmethod
    def get_mt5_symbol_map(cls) -> dict[str, str]:
        """
        Parse MT5 symbol map from env string:
          XAUUSD=XAUUSDm,BTC/USDT=BTCUSD,ETH/USDT=ETHUSD
        """
        return cls._parse_upper_map(cls.MT5_SYMBOL_MAP)

    @classmethod
    def get_stock_yf_symbol_alias_map(cls) -> dict[str, str]:
        return cls._parse_upper_map(cls.STOCK_YF_SYMBOL_ALIAS_MAP)

    @classmethod
    def get_neural_min_prob_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES)

    @classmethod
    def get_neural_gate_canary_sources(cls) -> list[str]:
        raw = str(cls.NEURAL_GATE_CANARY_SOURCES or cls.NEURAL_GATE_CANARY_SOURCE or "scalp_xauusd")
        out: list[str] = []
        seen: set[str] = set()
        for part in raw.split(","):
            token = str(part or "").strip().lower()
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
        if out:
            return out
        fallback = str(cls.NEURAL_GATE_CANARY_SOURCE or "scalp_xauusd").strip().lower()
        return [fallback] if fallback else ["scalp_xauusd"]

    @classmethod
    def get_neural_fx_soft_filter_band_low_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW_SYMBOL_OVERRIDES)

    @classmethod
    def get_neural_fx_soft_filter_band_high_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH_SYMBOL_OVERRIDES)

    @classmethod
    def get_neural_fx_soft_filter_max_penalty_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_fx_conf_soft_filter_band_pts_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_FX_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_fx_conf_soft_filter_max_penalty_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_crypto_conf_soft_filter_band_pts_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS_SYMBOL_OVERRIDES)

    @classmethod
    def get_mt5_crypto_conf_soft_filter_max_penalty_symbol_overrides(cls) -> dict[str, float]:
        return cls._parse_float_map(cls.MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY_SYMBOL_OVERRIDES)

    @classmethod
    def get_exec_reasons_delta_marker_utc(cls) -> str:
        return str(cls.MT5_EXEC_REASONS_DELTA_MARKER_UTC or "").strip()

    @classmethod
    def get_stripe_price_plan_map(cls) -> dict[str, tuple[str, int]]:
        """
        Parse STRIPE_PRICE_PLAN_MAP from env:
          price_abc=a:30,price_def=b:30,price_xyz=c:90
        """
        parsed: dict[str, tuple[str, int]] = {}
        raw = (cls.STRIPE_PRICE_PLAN_MAP or "").strip()
        if not raw:
            return parsed
        for chunk in raw.split(","):
            chunk = chunk.strip()
            if not chunk or "=" not in chunk:
                continue
            price_id, plan_days = chunk.split("=", 1)
            pid = price_id.strip()
            rhs = plan_days.strip().lower()
            if not pid or ":" not in rhs:
                continue
            plan, days_txt = rhs.split(":", 1)
            plan = plan.strip().lower()
            days_txt = days_txt.strip()
            if plan not in {"trial", "a", "b", "c"}:
                continue
            if not days_txt.isdigit():
                continue
            parsed[pid] = (plan, max(1, int(days_txt)))
        return parsed

    @classmethod
    def get_stripe_plan_price_ids(cls) -> dict[str, str]:
        mapping: dict[str, str] = {}
        if cls.STRIPE_PRICE_ID_A:
            mapping["a"] = cls.STRIPE_PRICE_ID_A.strip()
        if cls.STRIPE_PRICE_ID_B:
            mapping["b"] = cls.STRIPE_PRICE_ID_B.strip()
        if cls.STRIPE_PRICE_ID_C:
            mapping["c"] = cls.STRIPE_PRICE_ID_C.strip()
        return mapping

    @classmethod
    def get_plan_days_map(cls) -> dict[str, int]:
        return {
            "a": max(1, int(cls.BILLING_PLAN_DAYS_A)),
            "b": max(1, int(cls.BILLING_PLAN_DAYS_B)),
            "c": max(1, int(cls.BILLING_PLAN_DAYS_C)),
        }

    @classmethod
    def get_plan_price_cents_map(cls) -> dict[str, int]:
        return {
            "a": int(cls.BILLING_PRICE_A_CENTS),
            "b": int(cls.BILLING_PRICE_B_CENTS),
            "c": int(cls.BILLING_PRICE_C_CENTS),
        }


config = Config()
