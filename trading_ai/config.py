from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Literal, Optional

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_PACKAGE_ROOT = Path(__file__).resolve().parent
_ROOT_ENV_PATH = _PROJECT_ROOT / ".env"
_PACKAGE_ENV_PATH = _PACKAGE_ROOT / ".env"


def config_env_paths() -> tuple[Path, ...]:
    """
    Legacy root `.env` is kept as a fallback, but `trading_ai/.env` is the active
    source of truth for local operator workflows and token refresh updates.
    """
    if _PACKAGE_ENV_PATH.is_file():
        return (_PACKAGE_ENV_PATH,)
    return (_ROOT_ENV_PATH, _PACKAGE_ENV_PATH)


class LLMProviderName(str, Enum):
    OPENAI = "openai"
    MIMO = "mimo"
    LOCAL = "local"


class Settings(BaseSettings):
    """Environment-driven configuration. Prefer `trading_ai/.env` or exported vars."""

    model_config = SettingsConfigDict(
        env_file=tuple(str(path) for path in config_env_paths()),
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    # --- Runtime ---
    instance_name: str = Field(default="mempalac", validation_alias="INSTANCE_NAME")
    symbol: str = Field(default="XAUUSD", description="Default trading symbol")
    dry_run: bool = Field(default=True, validation_alias="DRY_RUN")
    live_execution_enabled: bool = Field(
        default=False,
        validation_alias="LIVE_EXECUTION_ENABLED",
        description="Explicit safety gate: required in addition to DRY_RUN=false before live orders are allowed.",
    )
    loop_interval_sec: float = Field(default=30.0, validation_alias="LOOP_INTERVAL_SEC")
    data_dir: Path = Field(default=Path("./data"), validation_alias="DATA_DIR")
    runtime_state_path: Path = Field(
        default=Path("./data/runtime_state.json"),
        validation_alias="RUNTIME_STATE_PATH",
        description="Crash-recovery state for the active Mempalac process.",
    )
    dexter_family_export_enabled: bool = Field(
        default=False,
        validation_alias="DEXTER_FAMILY_EXPORT_ENABLED",
        description="Publish latest Mempalac BUY/SELL/HOLD decision into Dexter runtime for family-lane execution.",
    )
    dexter_family_export_path: Path = Field(
        default=Path("../dexter_pro_v3_fixed/dexter_pro_v3_fixed/data/runtime/mempalace_family_signal.json"),
        validation_alias="DEXTER_FAMILY_EXPORT_PATH",
        description="JSON handoff file consumed by Dexter mempalace family adapter.",
    )
    dexter_family_export_base_source: str = Field(
        default="scalp_xauusd",
        validation_alias="DEXTER_FAMILY_EXPORT_BASE_SOURCE",
        description="Dexter base source token for mempalace family handoff (ex: scalp_xauusd).",
    )
    dexter_family_export_family: str = Field(
        default="xau_scalp_mempalace_lane",
        validation_alias="DEXTER_FAMILY_EXPORT_FAMILY",
        description="Dexter family token mapped to mempalace lane.",
    )
    dexter_family_export_strategy_id: str = Field(
        default="xau_scalp_mempalace_lane_v1",
        validation_alias="DEXTER_FAMILY_EXPORT_STRATEGY_ID",
        description="Strategy id tag exported for Dexter journal/report attribution.",
    )
    strategy_registry_path: Path = Field(
        default=Path("./data/strategy_registry.json"),
        validation_alias="STRATEGY_REGISTRY_PATH",
        description="JSON persistence for StrategyRegistry (runtime evolution).",
    )

    # --- LLM ---
    llm_provider: LLMProviderName = Field(
        default=LLMProviderName.OPENAI, validation_alias="LLM_PROVIDER"
    )
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", validation_alias="OPENAI_MODEL")
    openai_base_url: Optional[str] = Field(default=None, validation_alias="OPENAI_BASE_URL")
    openai_fallback_models: str = Field(
        default="",
        validation_alias="OPENAI_FALLBACK_MODELS",
        description="Comma-separated failover models for the same OpenAI-compatible endpoint.",
    )

    mimo_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("MIMO_API_KEY", "MIMO_API"),
    )
    mimo_base_url: str = Field(
        default="https://api.xiaomimimo.com/v1",
        validation_alias="MIMO_BASE_URL",
    )
    mimo_model: str = Field(default="mimo-v2-pro", validation_alias="MIMO_MODEL")

    local_base_url: str = Field(
        default="http://127.0.0.1:11434/v1", validation_alias="LOCAL_LLM_BASE_URL"
    )
    local_model: str = Field(default="qwen2.5", validation_alias="LOCAL_MODEL_NAME")
    local_api_key: str = Field(default="ollama", validation_alias="LOCAL_API_KEY")
    local_keep_alive: str = Field(default="5m", validation_alias="LOCAL_KEEP_ALIVE")
    local_num_ctx: Optional[int] = Field(default=None, validation_alias="LOCAL_NUM_CTX")
    local_think: bool = Field(
        default=False,
        validation_alias="LOCAL_THINK",
        description="Enable Ollama thinking traces for local models when supported.",
    )
    local_fallback_models: str = Field(
        default="",
        validation_alias="LOCAL_FALLBACK_MODELS",
        description="Comma-separated failover models for the same local OpenAI-compatible endpoint.",
    )

    llm_timeout_sec: float = Field(default=120.0, validation_alias="LLM_TIMEOUT_SEC")
    llm_max_retries: int = Field(default=4, validation_alias="LLM_MAX_RETRIES")
    llm_max_tokens: int = Field(default=256, validation_alias="LLM_MAX_TOKENS")
    llm_fallback_enabled: bool = Field(
        default=True,
        validation_alias="LLM_FALLBACK_ENABLED",
        description="Use a conservative heuristic fallback when the configured LLM is unavailable.",
    )
    llm_failover_failure_threshold: int = Field(
        default=2,
        validation_alias="LLM_FAILOVER_FAILURE_THRESHOLD",
        ge=1,
        le=10,
        description="Open per-model circuit breaker after this many consecutive failover errors.",
    )
    llm_failover_cooldown_sec: float = Field(
        default=20.0,
        validation_alias="LLM_FAILOVER_COOLDOWN_SEC",
        ge=0.0,
        le=600.0,
        description="Seconds to skip a failing model before retrying it in the chain.",
    )
    llm_failover_runtime_path: Path = Field(
        default=Path("./data/llm_failover_runtime.json"),
        validation_alias="LLM_FAILOVER_RUNTIME_PATH",
        description="Runtime telemetry snapshot for failover chain health and latency diagnostics.",
    )

    # --- Memory ---
    memory_backend: Literal["chroma", "mempalace_chroma"] = Field(
        default="chroma", validation_alias="MEMORY_BACKEND"
    )
    chroma_path: Path = Field(
        default=Path("./data/chroma_trading"),
        validation_alias="CHROMA_PATH",
    )
    mempalace_chroma_path: Optional[Path] = Field(
        default=None,
        validation_alias="MEMPALACE_CHROMA_PATH",
        description="If set, uses existing MemPalace Chroma directory (read/write with care).",
    )
    memory_collection: str = Field(
        default="trading_experiences", validation_alias="MEMORY_COLLECTION"
    )
    recall_top_k: int = Field(default=8, validation_alias="RECALL_TOP_K")
    memory_score_weight: float = Field(
        default=0.35,
        validation_alias="MEMORY_SCORE_WEIGHT",
        description="Blend: final_rank = (1-w)*similarity + w*normalized_memory_score",
    )
    memory_wakeup_top_k: int = Field(
        default=6,
        validation_alias="MEMORY_WAKEUP_TOP_K",
        description="MemPalace-style wake-up context size for each decision cycle.",
    )
    memory_note_top_k: int = Field(
        default=6,
        validation_alias="MEMORY_NOTE_TOP_K",
        description="Maximum non-trade palace notes injected into wake-up context.",
    )
    memory_room_guard_enabled: bool = Field(
        default=True,
        validation_alias="MEMORY_ROOM_GUARD_ENABLED",
        description="Use winner/danger/anti-pattern room intelligence in the live decision loop.",
    )
    memory_room_guard_block_anti: bool = Field(
        default=True,
        validation_alias="MEMORY_ROOM_GUARD_BLOCK_ANTI",
        description="Force HOLD when the current room is classified as an anti-pattern.",
    )
    self_improvement_enabled: bool = Field(
        default=True,
        validation_alias="SELF_IMPROVEMENT_ENABLED",
        description="After each closed trade, distill a reusable procedural skill document.",
    )
    self_improvement_store_notes: bool = Field(
        default=True,
        validation_alias="SELF_IMPROVEMENT_STORE_NOTES",
        description="Mirror distilled skills into MemPalace notes for wake-up context and inspection.",
    )
    self_improvement_model_name: Optional[str] = Field(
        default=None,
        validation_alias="SELF_IMPROVEMENT_MODEL_NAME",
        description="Optional dedicated model for the Hermes-style self-improvement loop.",
    )
    self_improvement_timeout_sec: Optional[float] = Field(
        default=None,
        validation_alias="SELF_IMPROVEMENT_TIMEOUT_SEC",
        description="Optional timeout override for post-trade self-improvement reviews.",
    )
    self_improvement_max_tokens: Optional[int] = Field(
        default=None,
        validation_alias="SELF_IMPROVEMENT_MAX_TOKENS",
        description="Optional max-token override for post-trade skill distillation.",
    )
    self_improvement_local_num_ctx: Optional[int] = Field(
        default=512,
        validation_alias="SELF_IMPROVEMENT_LOCAL_NUM_CTX",
        description="Optional Ollama num_ctx override for the self-improvement model.",
    )
    self_improvement_local_keep_alive: str = Field(
        default="0s",
        validation_alias="SELF_IMPROVEMENT_LOCAL_KEEP_ALIVE",
        description="How long the self-improvement model should stay loaded after a review.",
    )
    self_improvement_local_think: bool = Field(
        default=False,
        validation_alias="SELF_IMPROVEMENT_LOCAL_THINK",
        description="Enable Ollama thinking traces for the self-improvement model.",
    )
    agent_team_enabled: bool = Field(
        default=True,
        validation_alias="AGENT_TEAM_ENABLED",
        description="Inject a strategist / memory / risk / evolution team brief into the decision prompt.",
    )
    skillbook_dir: Path = Field(
        default=Path("./data/skills"),
        validation_alias="SKILLBOOK_DIR",
        description="Markdown skill documents written by the self-improvement loop.",
    )
    skillbook_index_path: Path = Field(
        default=Path("./data/skillbook_index.json"),
        validation_alias="SKILLBOOK_INDEX_PATH",
        description="Fast index for procedural skill recall.",
    )
    skill_recall_top_k: int = Field(
        default=3,
        validation_alias="SKILL_RECALL_TOP_K",
        ge=1,
        le=12,
        description="Top-K skill documents injected back into the prompt each cycle.",
    )
    skillbook_max_evidence: int = Field(
        default=8,
        validation_alias="SKILLBOOK_MAX_EVIDENCE",
        ge=2,
        le=40,
        description="Recent evidence rows retained per skill document.",
    )
    shadow_probe_enabled: bool = Field(
        default=True,
        validation_alias="SHADOW_PROBE_ENABLED",
        description="In backtest/paper, keep learning with dry-run shadow probes when real entry is blocked.",
    )
    shadow_probe_volume_fraction: float = Field(
        default=0.25,
        validation_alias="SHADOW_PROBE_VOLUME_FRACTION",
        ge=0.05,
        le=1.0,
        description="Fraction of DEFAULT_VOLUME used by dry-run shadow probes.",
    )
    shadow_probe_min_confidence: float = Field(
        default=0.58,
        validation_alias="SHADOW_PROBE_MIN_CONFIDENCE",
        ge=0.0,
        le=1.0,
        description="Minimum confidence required before a blocked signal can open a shadow probe.",
    )
    soft_gate_new_lane_enabled: bool = Field(
        default=True,
        validation_alias="SOFT_GATE_NEW_LANE_ENABLED",
        description="Downgrade some new-lane blockers from hard HOLD to confidence penalty.",
    )
    soft_gate_new_lane_max_trades: int = Field(
        default=3,
        validation_alias="SOFT_GATE_NEW_LANE_MAX_TRADES",
        ge=1,
        le=20,
        description="Treat lanes below this trade count as immature and eligible for soft-gate handling.",
    )
    soft_gate_confidence_penalty: float = Field(
        default=0.08,
        validation_alias="SOFT_GATE_CONFIDENCE_PENALTY",
        ge=0.0,
        le=0.5,
        description="Confidence penalty applied when a blocker is softened instead of forcing HOLD.",
    )
    soft_gate_min_confidence: float = Field(
        default=0.58,
        validation_alias="SOFT_GATE_MIN_CONFIDENCE",
        ge=0.0,
        le=1.0,
        description="Lower confidence floor used for probationary new-lane trades.",
    )
    probation_trade_volume_fraction: float = Field(
        default=0.35,
        validation_alias="PROBATION_TRADE_VOLUME_FRACTION",
        ge=0.05,
        le=1.0,
        description="Actual trade size fraction used for probationary soft-gated lanes to conserve risk budget.",
    )
    loss_streak_override_enabled: bool = Field(
        default=True,
        validation_alias="LOSS_STREAK_OVERRIDE_ENABLED",
        description="Allow promoted shadow/skill lanes to re-open real entries after an entry loss-streak block.",
    )
    loss_streak_override_min_shadow_trades: int = Field(
        default=3,
        validation_alias="LOSS_STREAK_OVERRIDE_MIN_SHADOW_TRADES",
        ge=1,
        le=20,
        description="Minimum closed shadow probes required before a lane can soften the entry loss-streak block.",
    )
    loss_streak_override_min_shadow_win_rate: float = Field(
        default=0.54,
        validation_alias="LOSS_STREAK_OVERRIDE_MIN_SHADOW_WIN_RATE",
        ge=0.0,
        le=1.0,
        description="Minimum shadow win-rate required before a promoted lane can soften the loss-streak block.",
    )
    loss_streak_override_min_skill_trades: int = Field(
        default=3,
        validation_alias="LOSS_STREAK_OVERRIDE_MIN_SKILL_TRADES",
        ge=1,
        le=20,
        description="Minimum skill evidence count before skill-only loss-streak overrides are allowed.",
    )
    loss_streak_override_min_skill_edge: float = Field(
        default=0.1,
        validation_alias="LOSS_STREAK_OVERRIDE_MIN_SKILL_EDGE",
        ge=-1.0,
        le=2.0,
        description="Minimum risk-adjusted skill edge required for skill-only loss-streak overrides.",
    )
    loss_streak_override_confidence_penalty: float = Field(
        default=0.06,
        validation_alias="LOSS_STREAK_OVERRIDE_CONFIDENCE_PENALTY",
        ge=0.0,
        le=0.5,
        description="Confidence penalty applied when re-opening a lane through loss-streak override.",
    )
    position_manager_enabled: bool = Field(
        default=True,
        validation_alias="POSITION_MANAGER_ENABLED",
        description="Continuously assess open positions with momentum/statistical signals and close when risk dominates opportunity.",
    )
    position_monitor_path: Path = Field(
        default=Path("./data/position_monitor.json"),
        validation_alias="POSITION_MONITOR_PATH",
        description="Latest real-time monitoring snapshot for entry assessment and open-position management.",
    )
    position_monitor_history_path: Path = Field(
        default=Path("./data/position_monitor_history.ndjson"),
        validation_alias="POSITION_MONITOR_HISTORY_PATH",
        description="Append-only monitoring history for post-trade analysis and operator review.",
    )
    weekly_lane_learning_enabled: bool = Field(
        default=True,
        validation_alias="WEEKLY_LANE_LEARNING_ENABLED",
        description="Learn from this week's good/bad lanes and blocked opportunities, then adapt live decisions conservatively.",
    )
    weekly_lane_refresh_sec: int = Field(
        default=300,
        validation_alias="WEEKLY_LANE_REFRESH_SEC",
        ge=30,
        le=86_400,
        description="How often to refresh weekly lane profile from memory + monitor + Dexter deal history.",
    )
    weekly_lane_profile_path: Path = Field(
        default=Path("./data/weekly_lane_profile.json"),
        validation_alias="WEEKLY_LANE_PROFILE_PATH",
        description="Latest weekly lane-learning profile persisted for audit and troubleshooting.",
    )
    weekly_lane_dexter_db_path: Optional[Path] = Field(
        default=Path("../dexter_pro_v3_fixed/dexter_pro_v3_fixed/data/ctrader_openapi.db"),
        validation_alias="WEEKLY_LANE_DEXTER_DB_PATH",
        description="Optional Dexter ctrader_openapi.db source for this-week family-lane deal history.",
    )
    weekly_lane_min_trades: int = Field(
        default=3,
        validation_alias="WEEKLY_LANE_MIN_TRADES",
        ge=1,
        le=100,
        description="Minimum weekly closed trades before a lane is classified as good/bad.",
    )
    weekly_lane_good_win_rate: float = Field(
        default=0.58,
        validation_alias="WEEKLY_LANE_GOOD_WIN_RATE",
        ge=0.0,
        le=1.0,
    )
    weekly_lane_bad_loss_rate: float = Field(
        default=0.6,
        validation_alias="WEEKLY_LANE_BAD_LOSS_RATE",
        ge=0.0,
        le=1.0,
    )
    weekly_lane_bad_pnl_threshold: float = Field(
        default=0.0,
        validation_alias="WEEKLY_LANE_BAD_PNL_THRESHOLD",
        description="Treat lane as bad when weekly pnl_sum falls below this threshold.",
    )
    weekly_lane_monitor_lookahead_steps: int = Field(
        default=8,
        validation_alias="WEEKLY_LANE_MONITOR_LOOKAHEAD_STEPS",
        ge=2,
        le=120,
        description="Forward monitor steps used to estimate missed opportunities vs prevented bad blocks.",
    )
    weekly_lane_monitor_move_threshold_pct: float = Field(
        default=0.0008,
        validation_alias="WEEKLY_LANE_MONITOR_MOVE_THRESHOLD_PCT",
        ge=0.00005,
        le=0.05,
        description="Minimum favorable/adverse move (pct) to classify a blocked signal outcome.",
    )
    weekly_lane_block_bad_lanes: bool = Field(
        default=True,
        validation_alias="WEEKLY_LANE_BLOCK_BAD_LANES",
        description="Force HOLD on lanes classified as bad this week.",
    )
    weekly_lane_confidence_boost: float = Field(
        default=0.04,
        validation_alias="WEEKLY_LANE_CONFIDENCE_BOOST",
        ge=0.0,
        le=0.4,
        description="Confidence bonus for lanes classified as good this week.",
    )
    weekly_lane_confidence_penalty: float = Field(
        default=0.1,
        validation_alias="WEEKLY_LANE_CONFIDENCE_PENALTY",
        ge=0.0,
        le=0.5,
        description="Confidence penalty for lanes classified as bad when hard block is disabled.",
    )
    weekly_lane_probe_override_enabled: bool = Field(
        default=True,
        validation_alias="WEEKLY_LANE_PROBE_OVERRIDE_ENABLED",
        description="Allow low-size probe entry when blocker evidence missed profitable moves this week.",
    )
    weekly_lane_probe_min_support: int = Field(
        default=2,
        validation_alias="WEEKLY_LANE_PROBE_MIN_SUPPORT",
        ge=1,
        le=20,
        description="Minimum weekly opportunity support required before blocker-to-probe override can trigger.",
    )
    weekly_lane_probe_override_confidence: float = Field(
        default=0.67,
        validation_alias="WEEKLY_LANE_PROBE_OVERRIDE_CONFIDENCE",
        ge=0.0,
        le=1.0,
        description="Confidence assigned when a weekly lane probe override converts HOLD into entry.",
    )
    weekly_lane_probe_volume_fraction: float = Field(
        default=0.35,
        validation_alias="WEEKLY_LANE_PROBE_VOLUME_FRACTION",
        ge=0.05,
        le=1.0,
        description="Fraction of DEFAULT_VOLUME used for weekly lane probe overrides.",
    )
    position_manager_max_hold_minutes: int = Field(
        default=240,
        validation_alias="POSITION_MANAGER_MAX_HOLD_MINUTES",
        ge=15,
        le=1440,
        description="Maximum preferred holding window before opportunity decay begins to force exits.",
    )
    position_manager_min_expected_move_pct: float = Field(
        default=0.00035,
        validation_alias="POSITION_MANAGER_MIN_EXPECTED_MOVE_PCT",
        ge=0.00001,
        le=0.02,
        description="Minimum expected move percent used to avoid TP/SL collapsing in low-noise conditions.",
    )
    position_manager_tp_vol_multiplier: float = Field(
        default=1.35,
        validation_alias="POSITION_MANAGER_TP_VOL_MULTIPLIER",
        ge=0.4,
        le=5.0,
        description="Base take-profit distance multiplier over realized move expectation.",
    )
    position_manager_sl_vol_multiplier: float = Field(
        default=0.95,
        validation_alias="POSITION_MANAGER_SL_VOL_MULTIPLIER",
        ge=0.2,
        le=3.0,
        description="Base stop-loss distance multiplier over realized move expectation.",
    )
    position_manager_trail_trigger_fraction: float = Field(
        default=0.55,
        validation_alias="POSITION_MANAGER_TRAIL_TRIGGER_FRACTION",
        ge=0.1,
        le=1.0,
        description="When unrealized progress reaches this fraction of TP distance, activate a trailing protect level.",
    )
    position_manager_risk_close_threshold: float = Field(
        default=0.64,
        validation_alias="POSITION_MANAGER_RISK_CLOSE_THRESHOLD",
        ge=0.4,
        le=1.0,
        description="Close the position early when estimated live risk exceeds this threshold and opportunity fades.",
    )

    # --- Risk ---
    max_trades_per_session: int = Field(
        default=50, validation_alias="MAX_TRADES_PER_SESSION"
    )
    max_consecutive_losses: int = Field(
        default=5, validation_alias="MAX_CONSECUTIVE_LOSSES"
    )
    neutral_pnl_threshold: float = Field(
        default=1e-4,
        validation_alias="NEUTRAL_PNL_THRESHOLD",
        description="Relative return below this (absolute) counts as neutral score.",
    )
    min_trade_confidence: float = Field(
        default=0.65,
        validation_alias="MIN_TRADE_CONFIDENCE",
        description="Below this confidence, action is forced to HOLD after LLM response.",
    )
    adaptive_confidence_floor_enabled: bool = Field(
        default=True,
        validation_alias="ADAPTIVE_CONFIDENCE_FLOOR_ENABLED",
        description=(
            "Dynamically loosen/tighten confidence floor based on live regime to reduce missed impulses "
            "while staying conservative in low-quality structure."
        ),
    )
    adaptive_confidence_floor_delta_strong: float = Field(
        default=0.05,
        ge=0.0,
        le=0.30,
        validation_alias="ADAPTIVE_CONFIDENCE_FLOOR_DELTA_STRONG",
        description="How much to lower confidence floor in strong directional conditions.",
    )
    adaptive_confidence_floor_delta_weak: float = Field(
        default=0.05,
        ge=0.0,
        le=0.30,
        validation_alias="ADAPTIVE_CONFIDENCE_FLOOR_DELTA_WEAK",
        description="How much to raise confidence floor in weak/ranging conditions.",
    )
    adaptive_confidence_floor_min: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        validation_alias="ADAPTIVE_CONFIDENCE_FLOOR_MIN",
        description="Lower bound for dynamic confidence floor.",
    )
    adaptive_confidence_floor_max: float = Field(
        default=0.80,
        ge=0.0,
        le=1.0,
        validation_alias="ADAPTIVE_CONFIDENCE_FLOOR_MAX",
        description="Upper bound for dynamic confidence floor.",
    )
    entry_loss_streak_block: int = Field(
        default=3,
        validation_alias="ENTRY_LOSS_STREAK_BLOCK",
        description="Block new entries when consecutive_losses >= this value (still below full risk halt).",
    )
    price_history_max: int = Field(
        default=96,
        validation_alias="PRICE_HISTORY_MAX",
        description="Rolling mids retained for feature extraction.",
    )
    similar_trades_top_k: int = Field(
        default=5,
        validation_alias="SIMILAR_TRADES_TOP_K",
        description="Top-K similar memories for the agent prompt.",
    )

    pattern_min_win_rate: float = Field(
        default=0.55,
        validation_alias="PATTERN_MIN_WIN_RATE",
        description="Hard block entries when historical pattern win_rate is below this.",
    )
    pattern_min_sample_size: int = Field(
        default=5,
        validation_alias="PATTERN_MIN_SAMPLE_SIZE",
        description="Hard block entries when pattern bucket has fewer closed trades.",
    )
    pattern_boost_min_win_rate: float = Field(
        default=0.65,
        validation_alias="PATTERN_BOOST_MIN_WIN_RATE",
    )
    pattern_boost_min_sample: int = Field(
        default=10,
        validation_alias="PATTERN_BOOST_MIN_SAMPLE",
    )
    pattern_confidence_boost_delta: float = Field(
        default=0.1,
        validation_alias="PATTERN_CONFIDENCE_BOOST_DELTA",
    )
    pattern_confidence_cap: float = Field(
        default=0.95,
        validation_alias="PATTERN_CONFIDENCE_CAP",
    )
    pattern_gate_strict: bool = Field(
        default=False,
        validation_alias="PATTERN_GATE_STRICT",
        description=(
            "If True: block entries when no historical pattern bucket exists (cold start never trades). "
            "If False: allow unknown buckets so the first positions can fire; existing buckets still "
            "respect min win-rate and sample size."
        ),
    )

    hard_filter_min_closes: int = Field(
        default=12,
        ge=0,
        validation_alias="HARD_FILTER_MIN_CLOSES",
        description=(
            "Require at least this many closes in features before volatility/trend hard-vetoes apply. "
            "Paper/prime loop starts with RANGE/LOW — set 0 to always apply vetoes."
        ),
    )

    # --- cTrader (names aligned with Dexter Pro — paste from .env.local) ---
    ctrader_client_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "CTRADER_OPENAPI_CLIENT_ID",
            "CTRADER_CLIENT_ID",
            "OpenAPI_ClientID",
            "Client ID",
        ),
    )
    ctrader_client_secret: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "CTRADER_OPENAPI_CLIENT_SECRET",
            "CTRADER_CLIENT_SECRET",
            "OpenAPI_Secreat",
            "OpenAPI_Secret",
            "Secret",
        ),
    )
    ctrader_redirect_uri: str = Field(
        default="http://localhost",
        validation_alias=AliasChoices(
            "CTRADER_OPENAPI_REDIRECT_URI",
        ),
    )
    ctrader_access_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "CTRADER_OPENAPI_ACCESS_TOKEN",
            "CTRADER_ACCESS_TOKEN",
            "OpenAPI_Access_token_API_key",
            "OpenAPI_Access_token_API_key2",
            "OpenAPI_Access_token_API_key3",
            "new_Accesstoken",
            "new_Access_token",
        ),
    )
    ctrader_refresh_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "CTRADER_OPENAPI_REFRESH_TOKEN",
            "CTRADER_REFRESH_TOKEN",
            "OpenAPI_Refresh_token_API_key",
            "OpenAPI_Refresh_token_API_key2",
            "OpenAPI_Refresh_token_API_key3",
            "new_Refresh_token",
        ),
    )
    ctrader_account_id: Optional[str] = Field(default=None, validation_alias="CTRADER_ACCOUNT_ID")
    ctrader_account_login: Optional[str] = Field(
        default=None, validation_alias="CTRADER_ACCOUNT_LOGIN"
    )
    ctrader_demo: bool = Field(
        default=True,
        validation_alias=AliasChoices("CTRADER_USE_DEMO", "CTRADER_DEMO"),
    )
    ctrader_enabled: bool = Field(default=False, validation_alias="CTRADER_ENABLED")

    ctrader_dexter_worker: bool = Field(
        default=False,
        validation_alias="CTRADER_DEXTER_WORKER",
        description=(
            "True: route orders through Dexter ops/ctrader_execute_once.py (real Open API). "
            "Quotes for the AI loop stay on PaperBroker unless you add another feed."
        ),
    )
    ctrader_worker_script: Optional[Path] = Field(
        default=None,
        validation_alias="CTRADER_WORKER_SCRIPT",
        description="Path to dexter_pro_v3_fixed/ops/ctrader_execute_once.py",
    )
    ctrader_worker_python: Optional[str] = Field(
        default=None,
        validation_alias="CTRADER_WORKER_PYTHON",
        description="Python exe with ctrader_open_api + Twisted (use Dexter venv if needed).",
    )
    ctrader_worker_timeout_sec: int = Field(
        default=120,
        ge=20,
        le=600,
        validation_alias="CTRADER_WORKER_TIMEOUT_SEC",
    )
    ctrader_quote_source: Literal["auto", "paper", "dexter_capture", "dexter_reference"] = Field(
        default="auto",
        validation_alias="CTRADER_QUOTE_SOURCE",
        description=(
            "Quote source for the loop when using Dexter worker. "
            "'auto' prefers Dexter capture_market then Dexter-style reference quote; "
            "'paper' keeps synthetic quotes."
        ),
    )
    ctrader_quote_cache_ttl_sec: float = Field(
        default=2.0,
        ge=0.0,
        le=60.0,
        validation_alias="CTRADER_QUOTE_CACHE_TTL_SEC",
    )
    ctrader_capture_duration_sec: int = Field(
        default=3,
        ge=1,
        le=15,
        validation_alias="CTRADER_CAPTURE_DURATION_SEC",
    )
    ctrader_reference_quote_fallback_enabled: bool = Field(
        default=True,
        validation_alias="CTRADER_REFERENCE_QUOTE_FALLBACK_ENABLED",
        description=(
            "Fallback to Dexter-style external reference quote when cTrader capture_market is unavailable. "
            "For XAUUSD this uses Stooq spot CSV without importing Dexter or touching Dexter state."
        ),
    )
    ctrader_reference_quote_spread: float = Field(
        default=0.10,
        ge=0.0,
        le=10.0,
        validation_alias="CTRADER_REFERENCE_QUOTE_SPREAD",
        description="Synthetic bid/ask spread around Dexter-style reference mid when only mid price is available.",
    )
    ctrader_reference_quote_timeout_sec: float = Field(
        default=8.0,
        ge=1.0,
        le=30.0,
        validation_alias="CTRADER_REFERENCE_QUOTE_TIMEOUT_SEC",
    )
    ctrader_worker_volume_scale: int = Field(
        default=100,
        ge=1,
        le=10_000_000,
        validation_alias="CTRADER_WORKER_VOLUME_SCALE",
        description=(
            "Worker payload scale: fixed_volume=int(round(DEFAULT_VOLUME*scale)). "
            "With scale=100, 0.01 lot is sent to the Dexter worker as 1. "
            "cTrader reconcile responses may report that same position as raw volume 100."
        ),
    )

    default_volume: float = Field(default=0.01, validation_alias="DEFAULT_VOLUME")
    pyramiding_enabled: bool = Field(
        default=True,
        validation_alias="PYRAMIDING_ENABLED",
        description="Allow same-side add-on entries when exposure remains below the equity-based cap.",
    )
    pyramid_max_positions_per_side: int = Field(
        default=3,
        ge=1,
        le=20,
        validation_alias="PYRAMID_MAX_POSITIONS_PER_SIDE",
    )
    pyramid_add_min_confidence: float = Field(
        default=0.70,
        ge=0.0,
        le=1.0,
        validation_alias="PYRAMID_ADD_MIN_CONFIDENCE",
    )
    risk_equity_fallback_usd: float = Field(
        default=1000.0,
        gt=0.0,
        validation_alias="RISK_EQUITY_FALLBACK_USD",
        description="Used for exposure caps when broker balance/equity probe is unavailable.",
    )
    risk_max_lot_per_1000_equity: float = Field(
        default=0.03,
        gt=0.0,
        validation_alias="RISK_MAX_LOT_PER_1000_EQUITY",
        description="Equity-scaled total exposure cap. Example: 0.03 at 1000 USD allows 0.03 total lots.",
    )
    risk_max_total_lot_per_symbol: float = Field(
        default=0.03,
        gt=0.0,
        validation_alias="RISK_MAX_TOTAL_LOT_PER_SYMBOL",
        description="Hard total same-symbol exposure cap after equity scaling.",
    )
    risk_min_order_lot: float = Field(
        default=0.01,
        gt=0.0,
        validation_alias="RISK_MIN_ORDER_LOT",
    )
    entry_override_enabled: bool = Field(
        default=True,
        validation_alias="ENTRY_OVERRIDE_ENABLED",
        description="Allow a reduced-size entry when the LLM freezes on HOLD but objective setup quality is strong.",
    )
    entry_override_min_opportunity: float = Field(
        default=0.67,
        ge=0.0,
        le=1.0,
        validation_alias="ENTRY_OVERRIDE_MIN_OPPORTUNITY",
    )
    entry_override_max_risk: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        validation_alias="ENTRY_OVERRIDE_MAX_RISK",
    )
    entry_override_min_edge: float = Field(
        default=0.16,
        ge=-1.0,
        le=1.0,
        validation_alias="ENTRY_OVERRIDE_MIN_EDGE",
        description="Minimum opportunity minus risk required before a HOLD can be promoted into a probationary entry.",
    )
    entry_override_confidence: float = Field(
        default=0.67,
        ge=0.0,
        le=1.0,
        validation_alias="ENTRY_OVERRIDE_CONFIDENCE",
        description="Confidence assigned to probationary entry overrides before normal exposure caps apply.",
    )

    # --- Strategy evolution v2 (GPT-style; off by default — cuts frequency & adds gates) ---
    strategy_evolution_v2_enabled: bool = Field(
        default=False,
        validation_alias="STRATEGY_EVOLUTION_V2_ENABLED",
        description="When True: global top-N gate, capital weighting, per-loop ranking decay.",
    )
    strategy_global_top_n: int = Field(
        default=10,
        validation_alias="STRATEGY_GLOBAL_TOP_N",
        description="Mature strategies must rank in top N by ranking_score (exploration bypass below threshold).",
    )
    strategy_exploration_max_trades: int = Field(
        default=5,
        validation_alias="STRATEGY_EXPLORATION_MAX_TRADES",
        description="Bypass global rank filter while trades < this (per strategy key).",
    )
    strategy_aging_enabled: bool = Field(
        default=False,
        validation_alias="STRATEGY_AGING_ENABLED",
    )
    strategy_aging_factor: float = Field(
        default=0.995,
        validation_alias="STRATEGY_AGING_FACTOR",
        ge=0.5,
        lt=1.0,
        description="Per-loop multiplier on ranking_score; 1.0 disables (handled as skip in code).",
    )
    strategy_capital_weighting_enabled: bool = Field(
        default=False,
        validation_alias="STRATEGY_CAPITAL_WEIGHTING_ENABLED",
    )
    strategy_capital_pool: int = Field(
        default=5,
        validation_alias="STRATEGY_CAPITAL_POOL",
        ge=1,
        description="Top-K strategies by ranking_score for volume share denominator.",
    )
    strategy_capital_mult_min: float = Field(
        default=0.25,
        validation_alias="STRATEGY_CAPITAL_MULT_MIN",
    )
    strategy_capital_mult_max: float = Field(
        default=2.0,
        validation_alias="STRATEGY_CAPITAL_MULT_MAX",
    )

    # --- Portfolio intelligence (GPT-style fusion; off by default) ---
    portfolio_intelligence_enabled: bool = Field(
        default=False,
        validation_alias="PORTFOLIO_INTELLIGENCE_ENABLED",
        description="When True: fuse LLM + memory + structure (can add extra HOLDs).",
    )
    portfolio_weight_llm: float = Field(
        default=1.0,
        validation_alias="PORTFOLIO_WEIGHT_LLM",
        ge=0.0,
    )
    portfolio_weight_memory: float = Field(
        default=0.55,
        validation_alias="PORTFOLIO_WEIGHT_MEMORY",
        ge=0.0,
    )
    portfolio_weight_structure: float = Field(
        default=0.35,
        validation_alias="PORTFOLIO_WEIGHT_STRUCTURE",
        ge=0.0,
    )
    portfolio_tie_margin: float = Field(
        default=0.08,
        validation_alias="PORTFOLIO_TIE_MARGIN",
        ge=0.0,
        le=0.45,
        description="Required relative edge of buy_mass vs sell_mass to take a side.",
    )
    portfolio_llm_anchor_confidence: float = Field(
        default=0.0,
        validation_alias="PORTFOLIO_LLM_ANCHOR_CONFIDENCE",
        ge=0.0,
        le=1.0,
        description="If >0 and LLM confidence >= this, fusion cannot flip direction (only align/HOLD).",
    )

    # --- Correlation engine (GPT-style; requires portfolio fusion to matter) ---
    correlation_engine_enabled: bool = Field(
        default=False,
        validation_alias="CORRELATION_ENGINE_ENABLED",
        description="When True + portfolio on: penalize redundant strategy_keys in fusion.",
    )
    strategy_correlation_path: Path = Field(
        default=Path("./data/strategy_correlation.json"),
        validation_alias="STRATEGY_CORRELATION_PATH",
    )
    correlation_max_history: int = Field(
        default=100,
        validation_alias="CORRELATION_MAX_HISTORY",
        ge=5,
        le=5000,
    )
    correlation_min_samples: int = Field(
        default=10,
        validation_alias="CORRELATION_MIN_SAMPLES",
        ge=5,
        le=500,
        description="Minimum overlapping closes for matrix + penalty/diversity.",
    )
    correlation_penalty_mid: float = Field(
        default=0.3,
        validation_alias="CORRELATION_PENALTY_MID",
        ge=0.0,
        le=1.0,
        description="Extra penalty mass when Pearson r > CORRELATION_PENALTY_MID_THRESHOLD.",
    )
    correlation_penalty_high: float = Field(
        default=0.5,
        validation_alias="CORRELATION_PENALTY_HIGH",
        ge=0.0,
        le=1.0,
        description="Extra penalty when r > CORRELATION_PENALTY_HIGH_THRESHOLD.",
    )
    correlation_penalty_mid_threshold: float = Field(
        default=0.8,
        validation_alias="CORRELATION_PENALTY_MID_THRESHOLD",
        ge=0.0,
        lt=1.0,
    )
    correlation_penalty_high_threshold: float = Field(
        default=0.9,
        validation_alias="CORRELATION_PENALTY_HIGH_THRESHOLD",
        ge=0.0,
        lt=1.0,
    )
    correlation_max_penalty: float = Field(
        default=0.7,
        validation_alias="CORRELATION_MAX_PENALTY",
        ge=0.0,
        le=1.0,
    )
    correlation_diversity_bonus: float = Field(
        default=0.1,
        validation_alias="CORRELATION_DIVERSITY_BONUS",
        ge=0.0,
        le=0.5,
    )
    correlation_diversity_threshold: float = Field(
        default=0.2,
        validation_alias="CORRELATION_DIVERSITY_THRESHOLD",
        ge=0.0,
        lt=1.0,
        description="Bonus only if r < this vs all eligible peers.",
    )

    # --- Performance monitor (measurement only; safe to leave on) ---
    performance_monitor_enabled: bool = Field(
        default=True,
        validation_alias="PERFORMANCE_MONITOR_ENABLED",
    )
    performance_log_interval: int = Field(
        default=50,
        validation_alias="PERFORMANCE_LOG_INTERVAL",
        ge=1,
        le=1_000_000,
        description="Every N loop cycles print [PERFORMANCE SUMMARY].",
    )
    performance_alert_max_drawdown: float = Field(
        default=0.0,
        validation_alias="PERFORMANCE_ALERT_MAX_DRAWDOWN",
        ge=0.0,
        description="If >0, WARN when max drawdown (PnL units) exceeds this.",
    )
    performance_alert_selectivity_min: float = Field(
        default=0.03,
        validation_alias="PERFORMANCE_ALERT_SELECTIVITY_MIN",
        ge=0.0,
        le=1.0,
        description="WARN when opens/LLM-intents below this after min intents.",
    )
    performance_alert_min_llm_intents: int = Field(
        default=40,
        validation_alias="PERFORMANCE_ALERT_MIN_LLM_INTENTS",
        ge=5,
        le=500_000,
    )
    performance_stage_telemetry_enabled: bool = Field(
        default=True,
        validation_alias="PERFORMANCE_STAGE_TELEMETRY_ENABLED",
        description="Emit per-stage cycle timings to isolate real latency bottlenecks.",
    )
    performance_stage_warn_ms: float = Field(
        default=1200.0,
        validation_alias="PERFORMANCE_STAGE_WARN_MS",
        ge=1.0,
        le=120_000.0,
        description="Warn when any individual stage exceeds this threshold (ms).",
    )
    performance_cycle_warn_ms: float = Field(
        default=8000.0,
        validation_alias="PERFORMANCE_CYCLE_WARN_MS",
        ge=1.0,
        le=300_000.0,
        description="Warn when full cycle time exceeds this threshold (ms).",
    )
    performance_stage_log_every_cycle: bool = Field(
        default=False,
        validation_alias="PERFORMANCE_STAGE_LOG_EVERY_CYCLE",
        description="If true, emit per-stage timings every cycle at INFO level (not only slow warnings).",
    )

    # --- API server (optional) ---
    api_host: str = Field(default="0.0.0.0", validation_alias="API_HOST")
    api_port: int = Field(default=8080, validation_alias="API_PORT")

    @model_validator(mode="after")
    def _normalize_paths(self) -> "Settings":
        base = self.data_dir.expanduser()
        if not base.is_absolute():
            base = base.resolve()
        self.data_dir = base
        self.runtime_state_path = _resolve_path(self.runtime_state_path, base)
        self.dexter_family_export_path = _resolve_external_path(self.dexter_family_export_path)
        self.strategy_registry_path = _resolve_path(self.strategy_registry_path, base)
        self.strategy_correlation_path = _resolve_path(self.strategy_correlation_path, base)
        self.chroma_path = _resolve_path(self.chroma_path, base)
        self.llm_failover_runtime_path = _resolve_path(self.llm_failover_runtime_path, base)
        self.weekly_lane_profile_path = _resolve_path(self.weekly_lane_profile_path, base)
        self.skillbook_dir = _resolve_path(self.skillbook_dir, base)
        self.skillbook_index_path = _resolve_path(self.skillbook_index_path, base)
        if self.mempalace_chroma_path is not None:
            self.mempalace_chroma_path = _resolve_external_path(self.mempalace_chroma_path)
        if self.ctrader_worker_script is not None:
            self.ctrader_worker_script = _resolve_external_path(self.ctrader_worker_script)
        if self.weekly_lane_dexter_db_path is not None:
            self.weekly_lane_dexter_db_path = _resolve_external_path(self.weekly_lane_dexter_db_path)
        return self


def _resolve_external_path(path: Path) -> Path:
    p = Path(path).expanduser()
    return p.resolve() if p.is_absolute() else p.resolve()


def _resolve_path(path: Path, data_dir: Path) -> Path:
    p = Path(path).expanduser()
    if p.is_absolute():
        return p
    parts = p.parts
    if parts[:1] == ("data",):
        return (data_dir / Path(*parts[1:])).resolve()
    return (data_dir / p).resolve()


def load_settings() -> Settings:
    return Settings(_env_file=tuple(str(path) for path in config_env_paths()))


def memory_persist_path(settings: Settings) -> Path:
    if settings.memory_backend == "mempalace_chroma" and settings.mempalace_chroma_path:
        return Path(settings.mempalace_chroma_path)
    return Path(settings.chroma_path)
