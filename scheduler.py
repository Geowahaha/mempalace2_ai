"""
scheduler.py - Background Task Scheduler
Runs XAUUSD and crypto scans on configured intervals.
Sends Telegram alerts when signals are found.
Respects session timing - more aggressive scans during active sessions.
"""
import copy
import os
import re
import time
import logging
import threading
import json
import sqlite3
from datetime import datetime, timezone, timedelta, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import schedule

from config import config
from scanners.xauusd import xauusd_scanner
# DISABLED: non-cTrader scanners removed to reduce VM memory
# All trading uses cTrader OpenAPI only (XAUUSD, BTCUSD, ETHUSD)
# from scanners.crypto_sniper import crypto_sniper
# from scanners.fx_major_scanner import fx_major_scanner
# from scanners.stock_scanner import stock_scanner
from scanners.scalping_scanner import scalping_scanner
from scanners.fibo_advance import FiboAdvanceScanner
fibo_advance_scanner = FiboAdvanceScanner()
from notifier.telegram_bot import notifier
from market.data_fetcher import session_manager, xauusd_provider
from market.economic_calendar import economic_calendar
from market.macro_news import macro_news
from market.macro_impact_tracker import macro_impact_tracker
from execution.mt5_executor import mt5_executor, MT5ExecutionResult
from execution.ctrader_executor import ctrader_executor
from learning.neural_brain import neural_brain
from learning.mt5_autopilot_core import mt5_autopilot_core
from learning.mt5_orchestrator import mt5_orchestrator
from learning.mt5_position_manager import mt5_position_manager
from learning.mt5_limit_manager import mt5_limit_manager
from learning.neural_gate_learning_loop import neural_gate_learning_loop
from learning.scalping_runtime import scalping_timeout_manager
from learning.scalping_forward import scalping_forward_analyzer
from learning.entry_template_catalog import apply_entry_template_conf_tailwind, apply_entry_template_hints
from learning.live_profile_autopilot import (
    live_profile_autopilot,
    _confidence_band as live_profile_confidence_band,
    _classify_chart_state as live_profile_classify_chart_state,
)
from learning.adaptive_directional_intelligence import adi as adaptive_di
from learning.trading_manager_agent import trading_manager_agent
from learning.strategy_lab_team import strategy_lab_team_agent
from learning.trading_team import trading_team_agent
from api.report_store import report_store
from api.scalp_signal_store import scalp_store, ScalpSignalRecord
from notifier.access_control import access_manager

logger = logging.getLogger(__name__)


class DexterScheduler:
    """
    Background scheduler that runs scans at configured intervals
    and dispatches Telegram alerts for qualified signals.
    """

    def __init__(self):
        self.running = False
        self._thread: threading.Thread = None
        self._last_signal_symbols: set = set()
        self._last_us_open_plan_date: str = ""
        self._us_open_last_symbols: list[str] = []
        self._us_open_last_sent_ts: float = 0.0
        self._us_open_last_noopp_sent_ts: float = 0.0
        self._us_open_session_checkin_day: str = ""
        self._us_open_quality_last_sent_ts: float = 0.0
        self._us_open_quality_last_key: str = ""
        self._us_open_mood_day: str = ""
        self._us_open_mood_weak_cycles: int = 0
        self._us_open_mood_stop_triggered: bool = False
        self._us_open_mood_stop_reason: str = ""
        self._last_xauusd_alert_ts: float = 0.0
        self._last_xauusd_direction: str = ""
        self._last_xauusd_entry: float = 0.0
        self._last_xauusd_atr: float = 0.0
        self._last_xauusd_signal_snapshot: dict = {}
        self._last_signal_feedback_report_date: str = ""
        self._last_neural_filter_not_ready_log_ts: float = 0.0
        self._last_neural_soft_adjust_skip_log_ts: float = 0.0
        self._last_neural_gate_policy_log_ts: float = 0.0
        self._econ_alert_sent: dict[str, float] = {}
        self._macro_alert_sent: dict[str, float] = {}
        self._xau_guard_transition_state: dict = {}
        self._last_macro_impact_sync_log_ts: float = 0.0
        self._last_crypto_focus_no_signal_ts: float = 0.0
        self._us_open_symbol_alert_ts: dict[str, float] = {}
        self._us_open_circuit_day: str = ""
        self._us_open_circuit_triggered: bool = False
        self._us_open_circuit_reason: str = ""
        self._us_open_quality_guard_cache_ts: float = 0.0
        self._us_open_quality_guard_cache: dict = {}
        self._us_open_quality_guard_last_diag: dict[str, dict] = {"plan": {}, "monitor": {}}
        self._us_open_quality_guard_last_diag_ts: float = 0.0
        self._us_open_symbol_recovery_state: dict[str, dict] = {}
        self._last_scalping_alert_ts: dict[str, float] = {}
        self._last_scalping_signal_fp: dict[str, dict] = {}
        self._last_scalping_timeout_mt5_warn_ts: float = 0.0
        self._mt5_preclose_flatten_day: str = ""
        self._neural_mission_cycle_lock = threading.Lock()
        self._neural_mission_thread = None
        self._mt5_repeat_guard_lock = threading.Lock()
        self._mt5_repeat_guard_state = {"version": 1, "symbols": {}}
        self._mt5_repeat_guard_last_save_ts: float = 0.0
        self._last_bypass_tp_diag_ts: float = 0.0
        self._post_sl_reversal_last_fired_ts: float = 0.0
        self._signal_trace_lock = threading.Lock()
        self._signal_trace_seq: int = 0
        cfg_guard_path = str(getattr(config, "MT5_REPEAT_ERROR_GUARD_PATH", "") or "").strip()
        default_guard_path = Path(__file__).resolve().parent / "data" / "runtime" / "mt5_repeat_error_guard.json"
        self._mt5_repeat_guard_path = Path(cfg_guard_path) if cfg_guard_path else default_guard_path
        if bool(getattr(config, "MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", False)) and (not self._is_pytest_runtime()):
            try:
                self._mt5_repeat_guard_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            self._load_mt5_repeat_guard_state()
            try:
                self._cleanup_mt5_repeat_guard_state()
            except Exception:
                pass

    @staticmethod
    def _now_ts() -> float:
        return float(time.time())

    @staticmethod
    def _signal_trace_meta(signal) -> dict:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        run_no = 0
        try:
            run_no = int(raw.get("signal_run_no", 0) or 0)
        except Exception:
            run_no = 0
        run_id = str(raw.get("signal_run_id", "") or "").strip()
        trace_tag = str(raw.get("signal_trace_tag", "") or "").strip()
        if not trace_tag:
            if run_no > 0:
                trace_tag = f"R{run_no:06d}"
            elif run_id:
                trace_tag = str(run_id)[-12:]
            else:
                trace_tag = "-"
        return {
            "run_no": int(run_no),
            "run_id": run_id,
            "tag": trace_tag,
        }

    def _ensure_signal_trace(self, signal, source: str = "") -> dict:
        if signal is None:
            return {"run_no": 0, "run_id": "", "tag": "-"}
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        run_id = str(raw.get("signal_run_id", "") or "").strip()
        run_no = 0
        try:
            run_no = int(raw.get("signal_run_no", 0) or 0)
        except Exception:
            run_no = 0

        if (not run_id) or (run_no <= 0):
            with self._signal_trace_lock:
                self._signal_trace_seq += 1
                run_no = int(self._signal_trace_seq)
            run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{run_no:06d}"
            raw["signal_run_no"] = int(run_no)
            raw["signal_run_id"] = str(run_id)
            raw["signal_trace_tag"] = f"R{run_no:06d}"
            raw["signal_created_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            raw["signal_source_first_seen"] = str(source or "")
        raw["signal_source_last"] = str(source or raw.get("signal_source_last", "") or "")
        try:
            signal.raw_scores = raw
            setattr(signal, "signal_run_no", int(run_no))
            setattr(signal, "signal_run_id", str(run_id))
            setattr(signal, "signal_trace_tag", str(raw.get("signal_trace_tag", "")))
        except Exception:
            pass
        return self._signal_trace_meta(signal)

    def _send_signal_with_trace(self, signal, source: str) -> bool:
        self._ensure_signal_trace(signal, source=source)
        return bool(notifier.send_signal(signal))

    @staticmethod
    def _normalized_signature(raw: str) -> str:
        tokens = [
            str(part or "").strip().lower().replace(" ", "_")
            for part in str(raw or "").split(",")
            if str(part or "").strip()
        ]
        return ",".join(tokens)

    @staticmethod
    def _signal_session_signature(signal) -> str:
        try:
            raw = str(getattr(signal, "session", "") or "")
        except Exception:
            raw = ""
        return DexterScheduler._normalized_signature(raw)

    @staticmethod
    def _signal_timeframe_token(signal) -> str:
        try:
            raw = str(getattr(signal, "timeframe", "") or "")
        except Exception:
            raw = ""
        return str(raw).strip().lower().replace(" ", "")

    @staticmethod
    def _signal_confidence_band(signal) -> str:
        try:
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        return str(live_profile_confidence_band(conf) or "")

    @staticmethod
    def _confidence_band_adjacent(left: str, right: str) -> bool:
        l = str(left or "").strip()
        r = str(right or "").strip()
        if not l or not r or l == r:
            return False
        order = ["<70", "70-74.9", "75-79.9", "80+"]
        try:
            return abs(order.index(l) - order.index(r)) == 1
        except ValueError:
            return False

    @staticmethod
    def _signal_h1_trend_token(signal) -> str:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
        token = str(
            raw.get("signal_h1_trend")
            or raw.get("scalp_force_trend_h1")
            or raw.get("trend_h1")
            or raw.get("h1_trend")
            or mtf.get("h1_trend")
            or ""
        ).strip().lower()
        return token or "unknown"

    @staticmethod
    def _signal_h4_trend_token(signal) -> str:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
        scaling_trigger = dict(raw.get("scalping_trigger") or {})
        regime_guard = dict(scaling_trigger.get("regime_guard") or {})
        token = str(
            raw.get("signal_h4_trend")
            or raw.get("scalp_force_trend_h4")
            or raw.get("trend_h4")
            or raw.get("h4_trend")
            or mtf.get("h4_trend")
            or regime_guard.get("structure")
            or ""
        ).strip().lower()
        return token or "unknown"

    @staticmethod
    def _signal_d1_trend_token(signal) -> str:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
        token = str(
            raw.get("signal_d1_trend")
            or raw.get("trend_d1")
            or raw.get("d1_trend")
            or mtf.get("d1_trend")
            or ""
        ).strip().lower()
        return token or "unknown"

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _apply_adi_modifier(self, signal, source: str) -> dict:
        """Apply Adaptive Directional Intelligence confidence modifier.

        Evaluates 5 dimensions (empirical, technical, flow, temporal, cross-family)
        and adjusts signal.confidence.  Never blocks — only modifies confidence
        so existing gates make the final decision.
        """
        if not bool(getattr(config, "ADI_ENABLED", True)):
            return {}
        if self._is_pytest_runtime():
            return {}
        try:
            sym = str(getattr(signal, "symbol", "") or "").strip().upper()
            direction = str(getattr(signal, "direction", "") or "").strip().lower()
            if not sym or direction not in {"long", "short"}:
                return {}

            raw = dict(getattr(signal, "raw_scores", {}) or {})

            # --- Build trend_context from signal's existing raw_scores ---
            trend_context = {
                "d1": self._signal_d1_trend_token(signal),
                "h4": self._signal_h4_trend_token(signal),
                "h1": self._signal_h1_trend_token(signal),
            }

            # --- Build flow_features from raw_scores or fresh snapshot ---
            flow_features = None
            # Check if features already present (from scalp pipeline / fibo)
            for key in ("capture_features", "entry_sharpness_features", "micro_features"):
                if isinstance(raw.get(key), dict) and raw[key].get("delta_proxy") is not None:
                    flow_features = raw[key]
                    break
            # Check top-level raw_scores keys
            if flow_features is None and raw.get("delta_proxy") is not None:
                flow_features = {
                    "delta_proxy": raw.get("delta_proxy", 0.0),
                    "depth_imbalance": raw.get("depth_imbalance", 0.0),
                    "bar_volume_proxy": raw.get("bar_volume_proxy", 0.0),
                    "tick_up_ratio": raw.get("tick_up_ratio", 0.5),
                    "spots_count": raw.get("spots_count", 0),
                }
            # Fallback: fetch fresh snapshot for XAU
            if flow_features is None and "XAU" in sym:
                try:
                    snap = live_profile_autopilot.latest_capture_feature_snapshot(
                        symbol="XAUUSD", lookback_sec=120,
                        direction=direction,
                        confidence=float(getattr(signal, "confidence", 70) or 70),
                    )
                    if isinstance(snap, dict) and snap.get("ok"):
                        flow_features = dict(snap.get("features") or snap.get("gate", {}).get("features") or {})
                except Exception:
                    pass

            # --- Session info ---
            session_info = None
            try:
                sig_session = str(getattr(signal, "session", "") or "").strip().lower()
                if sig_session:
                    session_info = {"active_sessions": [sig_session]}
            except Exception:
                pass

            # --- Evaluate ---
            conf_before = float(getattr(signal, "confidence", 0.0) or 0.0)
            result = adaptive_di.evaluate(
                source=str(source or ""),
                direction=direction,
                symbol=sym,
                confidence=conf_before,
                trend_context=trend_context,
                flow_features=flow_features,
                session_info=session_info,
            )

            modifier = float(result.get("modifier", 0.0) or 0.0)
            if modifier == 0.0:
                # ADI is neutral — still check Hermes skill modifier
                hermes_mod_z = 0.0
                hermes_detail_z = {}
                try:
                    from learning.hermes_loop import improvement_loop as _hermes_z
                    sig_sess = str(getattr(signal, "session", "") or "").strip().lower()
                    hermes_mod_z, hermes_detail_z = _hermes_z.get_skill_modifier(
                        source=str(source or ""), direction=direction, session=sig_sess,
                    )
                    if hermes_mod_z != 0.0:
                        new_c = round(max(0.0, min(99.9, conf_before + hermes_mod_z)), 1)
                        signal.confidence = new_c
                except Exception:
                    pass
                raw["adi_modifier"] = 0.0
                raw["adi_recommendation"] = str(result.get("recommendation", ""))
                raw["hermes_modifier"] = round(hermes_mod_z, 1)
                raw["hermes_detail"] = hermes_detail_z
                signal.raw_scores = raw
                return result

            # Apply modifier to confidence
            new_conf = round(max(0.0, min(99.9, conf_before + modifier)), 1)
            signal.confidence = new_conf

            # ── Hermes skill modifier (compounds on ADI) ──
            hermes_mod = 0.0
            hermes_detail = {}
            try:
                from learning.hermes_loop import improvement_loop as _hermes
                sig_session = str(getattr(signal, "session", "") or "").strip().lower()
                hermes_mod, hermes_detail = _hermes.get_skill_modifier(
                    source=str(source or ""), direction=direction, session=sig_session,
                )
                if hermes_mod != 0.0:
                    new_conf = round(max(0.0, min(99.9, new_conf + hermes_mod)), 1)
                    signal.confidence = new_conf
            except Exception:
                pass

            # Record full audit trail in raw_scores
            raw["adi_modifier"] = round(modifier, 1)
            raw["adi_conf_before"] = round(conf_before, 1)
            raw["adi_conf_after"] = round(new_conf, 1)
            raw["adi_recommendation"] = str(result.get("recommendation", ""))
            raw["adi_divergence"] = bool(result.get("divergence_flag", False))
            raw["adi_catastrophic"] = bool(result.get("catastrophic_flag", False))
            raw["adi_dimensions"] = result.get("dimensions", {})
            raw["hermes_modifier"] = round(hermes_mod, 1)
            raw["hermes_detail"] = hermes_detail
            signal.raw_scores = raw

            # Log for observability
            tag = str(raw.get("signal_trace_tag", ""))
            hermes_tag = f" hermes:{hermes_mod:+.1f}" if hermes_mod else ""
            logger.info(
                "[ADI] %s %s %s | conf:%.1f→%.1f (adi:%+.1f%s) | %s%s",
                tag, sym, direction.upper(),
                conf_before, new_conf, modifier, hermes_tag,
                result.get("recommendation", ""),
                " ⚠DIVERGENCE" if result.get("divergence_flag") else "",
            )

            # Append to signal warnings/reasons for notification visibility
            if modifier <= -10:
                warn = f"ADI penalty {modifier:+.1f} ({result.get('recommendation', '')})"
                if hasattr(signal, "warnings") and isinstance(signal.warnings, list) and warn not in signal.warnings:
                    signal.warnings.append(warn)
            elif modifier >= 5:
                reason = f"ADI boost {modifier:+.1f} ({result.get('recommendation', '')})"
                if hasattr(signal, "reasons") and isinstance(signal.reasons, list) and reason not in signal.reasons:
                    signal.reasons.append(reason)

            return result
        except Exception as e:
            logger.debug("[ADI] apply error: %s", e)
            return {}

    @staticmethod
    def _trend_from_open_last(open_price: float, last_price: float, neutral_buffer_pct: float) -> str:
        if open_price <= 0 or last_price <= 0:
            return "unknown"
        threshold = abs(open_price) * max(0.0, float(neutral_buffer_pct))
        delta = float(last_price) - float(open_price)
        if abs(delta) <= threshold:
            return "neutral"
        return "bullish" if delta > 0 else "bearish"

    def _signal_effective_tf_trend_token(self, signal, *, tf_token: str, fallback_token: str) -> tuple[str, dict]:
        mode = "closed"
        buffer_hit = False
        snap = {}
        if not bool(getattr(config, "SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR", True)):
            return str(fallback_token or "unknown"), {"mode": mode, "buffer_hit": buffer_hit}
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            snap = dict(raw.get("xau_multi_tf_snapshot") or {})
        except Exception:
            raw = {}
            snap = {}
        tf = str(tf_token or "").strip().lower()
        provider_tf = {"d1": "1d", "h1": "1h", "h4": "4h"}.get(tf, tf)
        open_px = self._safe_float(snap.get(f"{tf}_open", 0.0), 0.0)
        last_px = self._safe_float(snap.get(f"{tf}_last", 0.0), 0.0)
        if open_px <= 0 or last_px <= 0:
            # In unit tests, avoid fetching live provider data when snapshot open/last is missing.
            if not bool(self._is_pytest_runtime()):
                try:
                    df = xauusd_provider.fetch(provider_tf, bars=3)
                    if df is not None and not getattr(df, "empty", True):
                        open_px = self._safe_float(df["open"].iloc[-1], 0.0)
                        last_px = self._safe_float(df["close"].iloc[-1], 0.0)
                except Exception:
                    pass
        if open_px > 0 and last_px > 0:
            mode = "intrabar"
            buffer_pct = self._safe_float(getattr(config, "SCALP_XAU_DIRECT_MTF_NEUTRAL_OPEN_BUFFER_PCT", 0.00015), 0.00015)
            intrabar = self._trend_from_open_last(open_px, last_px, buffer_pct)
            buffer_hit = intrabar == "neutral"
            if intrabar in {"bullish", "bearish"}:
                return intrabar, {
                    "mode": mode,
                    "buffer_hit": buffer_hit,
                    "open": open_px,
                    "last": last_px,
                }
        return str(fallback_token or "unknown"), {"mode": mode, "buffer_hit": buffer_hit, "open": open_px, "last": last_px}

    @staticmethod
    def _signal_countertrend_confirmed(signal) -> bool:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        trigger = dict(raw.get("scalping_trigger") or {})
        mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
        return bool(
            raw.get("countertrend_confirmed")
            or raw.get("xau_mtf_countertrend_confirmed")
            or trigger.get("countertrend_confirmed")
            or mtf.get("countertrend_confirmed")
        )

    @staticmethod
    def _signal_direction_token(signal) -> str:
        try:
            direction = str(getattr(signal, "direction", "") or "").strip().lower()
        except Exception:
            direction = ""
        if direction == "buy":
            return "long"
        if direction == "sell":
            return "short"
        return direction

    @staticmethod
    def _signal_raw_scores(signal) -> dict:
        try:
            return dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            return {}

    @staticmethod
    def _signal_float_value(signal, field: str) -> float:
        try:
            return float(getattr(signal, field, 0.0) or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _json_safe_copy(payload):
        try:
            return json.loads(json.dumps(payload or {}, ensure_ascii=True, default=str))
        except Exception:
            return {}

    @staticmethod
    def _normalize_reason_token(raw: str) -> str:
        return str(raw or "").strip().lower().replace(" ", "_")

    @staticmethod
    def _xau_source_desk(source: str, family: str = "") -> str:
        source_token = str(source or "").strip().lower()
        family_token = str(family or "").strip().lower()
        if family_token == "xau_scalp_range_repair" or ":rr:" in source_token or "range_repair" in source_token:
            return "range_repair"
        if family_token == "xau_scalp_flow_short_sidecar" or ":fss:" in source_token or "flow_short_sidecar" in source_token:
            return "fss_confirmation"
        if source_token.startswith("scalp_xauusd") or source_token.startswith("xauusd_scheduled"):
            return "limit_retest"
        return ""

    @classmethod
    def _extract_xau_chart_state_tags(cls, raw_scores: dict) -> dict:
        raw = dict(raw_scores or {})
        direct_state = {
            "state_label": str(raw.get("state_label") or raw.get("chart_state_state_label") or raw.get("xau_state_label") or "").strip().lower(),
            "day_type": str(raw.get("day_type") or raw.get("chart_state_day_type") or raw.get("xau_day_type") or "").strip().lower(),
            "follow_up_plan": str(raw.get("follow_up_plan") or raw.get("chart_state_follow_up_plan") or raw.get("xau_follow_up_plan") or "").strip().lower(),
        }
        if any(direct_state.values()):
            return direct_state
        nested_candidates = [
            dict(raw.get("chart_state_follow_up") or {}),
            dict(raw.get("chart_state_flow_short_sidecar") or {}),
            dict((raw.get("xau_openapi_entry_router") or {}).get("chart_state") or {}),
            dict(raw.get("pb_falling_knife_block_chart_state") or {}),
        ]
        for candidate in nested_candidates:
            state_label = str(candidate.get("state_label") or "").strip().lower()
            day_type = str(candidate.get("day_type") or "").strip().lower()
            follow_up_plan = str(candidate.get("follow_up_plan") or "").strip().lower()
            if state_label or day_type or follow_up_plan:
                return {
                    "state_label": state_label,
                    "day_type": day_type,
                    "follow_up_plan": follow_up_plan,
                }
        return {"state_label": "", "day_type": "", "follow_up_plan": ""}

    @classmethod
    def _apply_xau_observability_tags(
        cls,
        raw_scores: dict,
        *,
        source: str = "",
        family: str = "",
        chart_state: dict | None = None,
        follow_up_plan: str = "",
    ) -> dict:
        raw = dict(raw_scores or {})
        state_payload = dict(chart_state or {})
        extracted = cls._extract_xau_chart_state_tags(raw)
        state_label = str(state_payload.get("state_label") or extracted.get("state_label") or "").strip().lower()
        day_type = str(state_payload.get("day_type") or extracted.get("day_type") or "").strip().lower()
        follow_plan = str(follow_up_plan or extracted.get("follow_up_plan") or "").strip().lower()
        if state_label:
            raw["state_label"] = state_label
            raw["chart_state_state_label"] = state_label
            raw["xau_state_label"] = state_label
        if day_type:
            raw["day_type"] = day_type
            raw["chart_state_day_type"] = day_type
            raw["xau_day_type"] = day_type
        raw["follow_up_plan"] = follow_plan
        raw["chart_state_follow_up_plan"] = follow_plan
        raw["xau_follow_up_plan"] = follow_plan
        desk = cls._xau_source_desk(source, family)
        if desk:
            raw["xau_routing_desk"] = desk
        scenario_parts = [str(part or "").strip().lower() for part in (raw.get("direction"), state_label, day_type) if str(part or "").strip()]
        if scenario_parts:
            raw["xau_scenario_key"] = ":".join(scenario_parts)
        return raw

    def _xau_forced_style_guard(self, signal, *, source: str, runtime_state: dict | None = None) -> tuple[bool, str]:
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        if symbol != "XAUUSD":
            return True, "not_xau"
        raw = self._signal_raw_scores(signal)
        trigger = dict(raw.get("scalping_trigger") or {})
        direction = self._signal_direction_token(signal)
        family = str(
            live_profile_autopilot._strategy_family_for_source(symbol, str(source or "").strip().lower())
            or raw.get("strategy_family")
            or raw.get("family")
            or ""
        ).strip().lower()
        raw = self._apply_xau_observability_tags(raw, source=source, family=family)
        force_mode = bool(
            raw.get("forced_mode")
            or raw.get("scalp_force_mode")
            or trigger.get("forced_mode")
        )
        force_reason_tokens = {
            self._normalize_reason_token(raw.get("forced_from_status")),
            self._normalize_reason_token(raw.get("forced_from_reason")),
            self._normalize_reason_token(trigger.get("forced_from_status")),
            self._normalize_reason_token(trigger.get("forced_from_reason")),
            self._normalize_reason_token(trigger.get("xau_diag_status")),
        }
        block_tokens = {
            self._normalize_reason_token(token)
            for token in str(getattr(config, "SCALPING_XAU_FORCE_BLOCK_REASONS", "") or "").split(",")
            if str(token or "").strip()
        }
        shock_state = dict(((runtime_state or {}).get("xau_shock_profile") or {}))
        shock_active = (
            str(shock_state.get("status") or "").strip().lower() == "active"
            and str(shock_state.get("mode") or "").strip().lower() == "shock_protect"
        )
        if force_mode and direction == "short" and any(token and token in block_tokens for token in force_reason_tokens):
            raw["xau_forced_style_block"] = True
            raw["xau_forced_style_block_reason"] = "forced_continuation_not_allowed"
            raw["xau_forced_style_block_tokens"] = sorted([token for token in force_reason_tokens if token])
            raw["xau_forced_style_block_shock_protect"] = bool(shock_active)
            try:
                signal.raw_scores = raw
            except Exception:
                pass
            return False, "xau_forced_continuation_block"
        if bool(getattr(config, "XAU_COUNTERTREND_LONG_REQUIRE_CONFIRMED", True)) and direction == "long":
            mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
            strict_side = str(mtf.get("strict_aligned_side") or "").strip().lower()
            aligned_side = str(mtf.get("aligned_side") or "").strip().lower()
            countertrend_confirmed = self._signal_countertrend_confirmed(signal)
            adverse_side = strict_side or aligned_side
            if adverse_side == "short" and not countertrend_confirmed:
                raw["xau_countertrend_long_block"] = True
                raw["xau_countertrend_long_block_reason"] = "countertrend_confirmed_required"
                raw["xau_countertrend_long_aligned_side"] = adverse_side
                try:
                    signal.raw_scores = raw
                except Exception:
                    pass
                return False, "xau_countertrend_long_unconfirmed"
        try:
            signal.raw_scores = raw
        except Exception:
            pass
        return True, "xau_style_guard_pass"

    def _scalp_xau_direct_mtf_guard(self, signal) -> dict:
        if signal is None:
            return {"allowed": False, "reason": "missing_signal"}
        if str(getattr(signal, "symbol", "") or "").strip().upper() != "XAUUSD":
            return {"allowed": True, "reason": "not_xau"}
        if not bool(getattr(config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True)):
            return {"allowed": True, "reason": "disabled"}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction == "buy":
            direction = "long"
        elif direction == "sell":
            direction = "short"
        d1_base = self._signal_d1_trend_token(signal)
        h1_base = self._signal_h1_trend_token(signal)
        h4_base = self._signal_h4_trend_token(signal)
        d1_trend, d1_meta = self._signal_effective_tf_trend_token(signal, tf_token="d1", fallback_token=d1_base)
        h1_trend, h1_meta = self._signal_effective_tf_trend_token(signal, tf_token="h1", fallback_token=h1_base)
        h4_trend, h4_meta = self._signal_effective_tf_trend_token(signal, tf_token="h4", fallback_token=h4_base)
        if d1_trend == "unknown" and h1_trend == "unknown" and h4_trend == "unknown":
            # No deterministic MTF trend info available; allow to avoid false blocks.
            countertrend_confirmed = self._signal_countertrend_confirmed(signal)
            return {
                "allowed": True,
                "reason": "missing_mtf_trends_allow",
                "direction": direction,
                "d1_trend": d1_trend,
                "h1_trend": h1_trend,
                "h4_trend": h4_trend,
                "aligned_side": "",
                "countertrend_confirmed": countertrend_confirmed,
                "xau_mtf_mode": "closed",
                "xau_mtf_open_buffer_hit": False,
                "xau_mtf_flow_confirmed": False,
                "xau_mtf_flow_snapshot": {
                    "continuation_bias_abs": 0.0,
                    "delta_proxy_abs": 0.0,
                    "bar_volume_proxy": 0.0,
                },
            }
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
        aligned_side = str(mtf.get("strict_aligned_side") or "").strip().lower()
        # If multi-tf snapshot provides a conflicting strict aligned_side, trust the
        # computed effective trends when all three TFs agree (deterministic for unit tests).
        if d1_trend == "bullish" and h1_trend == "bullish" and h4_trend == "bullish":
            aligned_side = "long"
        elif d1_trend == "bearish" and h1_trend == "bearish" and h4_trend == "bearish":
            aligned_side = "short"
        elif not aligned_side:
            # Default: only accept mtf alignment when it isn't set.
            aligned_side = ""
        countertrend_confirmed = self._signal_countertrend_confirmed(signal)
        require_align = bool(getattr(config, "SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN", True))
        signal_conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        result = {
            "allowed": True,
            "reason": "aligned_or_neutral",
            "direction": direction,
            "d1_trend": d1_trend,
            "h1_trend": h1_trend,
            "h4_trend": h4_trend,
            "aligned_side": aligned_side,
            "countertrend_confirmed": countertrend_confirmed,
            "xau_mtf_mode": "intrabar" if "intrabar" in {str(d1_meta.get("mode")), str(h1_meta.get("mode")), str(h4_meta.get("mode"))} else "closed",
            "xau_mtf_open_buffer_hit": bool(d1_meta.get("buffer_hit") or h1_meta.get("buffer_hit") or h4_meta.get("buffer_hit")),
        }
        continuation_bias = abs(self._safe_float(raw.get("continuation_bias", 0.0), 0.0))
        delta_proxy = abs(self._safe_float(raw.get("delta_proxy", 0.0), 0.0))
        bar_volume_proxy = self._safe_float(raw.get("bar_volume_proxy", 0.0), 0.0)
        min_cb = self._safe_float(getattr(config, "SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONTINUATION_BIAS", 0.10), 0.10)
        min_dp = self._safe_float(getattr(config, "SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_DELTA_PROXY", 0.08), 0.08)
        min_bv = self._safe_float(getattr(config, "SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_BAR_VOLUME_PROXY", 0.38), 0.38)
        flow_confirmed = continuation_bias >= min_cb and delta_proxy >= min_dp and bar_volume_proxy >= min_bv
        result["xau_mtf_flow_confirmed"] = bool(flow_confirmed)
        result["xau_mtf_flow_snapshot"] = {
            "continuation_bias_abs": continuation_bias,
            "delta_proxy_abs": delta_proxy,
            "bar_volume_proxy": bar_volume_proxy,
        }
        if require_align and not aligned_side:
            partial_align = bool(getattr(config, "SCALP_XAU_DIRECT_MTF_ALLOW_PARTIAL_ALIGN", True))
            partial_min_conf = float(getattr(config, "SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONF", 70.0) or 70.0)
            if partial_align and direction in {"long", "short"}:
                trend_map = {"bullish": "long", "bearish": "short"}
                support_count = sum(
                    1 for t in (d1_trend, h4_trend, h1_trend) if trend_map.get(t) == direction
                )
                result["mtf_support_count"] = support_count
                if support_count >= 2 and signal_conf >= partial_min_conf:
                    if (
                        direction == "short"
                        and bool(getattr(config, "SCALP_XAU_DIRECT_MTF_PARTIAL_FLOW_CONFIRM_ENABLED", True))
                        and not (flow_confirmed or countertrend_confirmed)
                    ):
                        result["allowed"] = False
                        result["reason"] = "partial_align_no_flow_confirm"
                        return result
                    result["reason"] = f"partial_2of3_aligned:{support_count}/3_conf={signal_conf:.1f}"
                    if (
                        direction == "short"
                        and support_count >= 3
                        and bool(flow_confirmed)
                        and bool(getattr(config, "SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED", True))
                    ):
                        result["xau_fss_sell_routing_hint"] = True
                        raw["xau_fss_sell_routing_hint"] = True
                        raw["xau_fss_sell_routing_reason"] = "intrabar_3of3_bearish_flow_confirmed"
                    raw["xau_mtf_mode"] = result["xau_mtf_mode"]
                    raw["xau_mtf_open_buffer_hit"] = result["xau_mtf_open_buffer_hit"]
                    raw["xau_mtf_support_count"] = support_count
                    raw["xau_mtf_flow_confirmed"] = bool(flow_confirmed)
                    try:
                        signal.raw_scores = raw
                    except Exception:
                        pass
                    return result
            result["allowed"] = False
            result["reason"] = "d1_h4_h1_not_aligned"
            return result
        if aligned_side and direction in {"long", "short"} and direction != aligned_side:
            if bool(getattr(config, "SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", False)) and countertrend_confirmed:
                result["reason"] = f"countertrend_confirmed:{direction}_vs_{aligned_side}"
                return result
            result["allowed"] = False
            result["reason"] = f"d1_h4_h1_block:{direction}_vs_{aligned_side}"
            return result
        if (
            aligned_side == "short"
            and direction == "short"
            and bool(flow_confirmed)
            and bool(getattr(config, "SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED", True))
        ):
            result["xau_fss_sell_routing_hint"] = True
            raw["xau_fss_sell_routing_hint"] = True
            raw["xau_fss_sell_routing_reason"] = "d1_h4_h1_bearish_flow_confirmed"
        raw["xau_mtf_mode"] = result["xau_mtf_mode"]
        raw["xau_mtf_open_buffer_hit"] = result["xau_mtf_open_buffer_hit"]
        raw["xau_mtf_flow_confirmed"] = bool(flow_confirmed)
        try:
            signal.raw_scores = raw
        except Exception:
            pass
        result["reason"] = "d1_h4_h1_aligned"
        return result

    def _xau_multi_tf_entry_guard(self, signal, *, family: str) -> dict:
        if not bool(getattr(config, "XAU_MULTI_TF_ENTRY_GUARD_ENABLED", True)):
            return {}
        if str(getattr(signal, "symbol", "") or "").strip().upper() != "XAUUSD":
            return {}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return {}
        families = self._parse_lower_csv(
            getattr(
                config,
                "XAU_MULTI_TF_ENTRY_GUARD_FAMILIES",
                "xau_scalp_tick_depth_filter,xau_scalp_flow_short_sidecar,xau_scalp_microtrend_follow_up,xau_scalp_pullback_limit,xau_scalp_breakout_stop,xau_scalp_range_repair",
            )
        )
        family_token = str(family or "").strip().lower()
        if families and family_token not in families and "all" not in families and "*" not in families:
            return {}
        h1_trend = self._signal_h1_trend_token(signal)
        h4_trend = self._signal_h4_trend_token(signal)
        aligned_side = ""
        if h1_trend == "bullish" and h4_trend == "bullish":
            aligned_side = "long"
        elif h1_trend == "bearish" and h4_trend == "bearish":
            aligned_side = "short"
        countertrend_confirmed = self._signal_countertrend_confirmed(signal)
        require_align = bool(getattr(config, "XAU_MULTI_TF_ENTRY_GUARD_REQUIRE_H1_H4_ALIGN", True))
        if require_align and not aligned_side:
            return {
                "blocked": False,
                "family": family_token,
                "h1_trend": h1_trend,
                "h4_trend": h4_trend,
                "aligned_side": "",
                "countertrend_confirmed": countertrend_confirmed,
                "reason": "mtf_not_aligned",
            }
        if aligned_side and direction != aligned_side:
            if bool(getattr(config, "XAU_MULTI_TF_ENTRY_GUARD_ALLOW_COUNTERTREND_CONFIRMED", True)) and countertrend_confirmed:
                return {
                    "blocked": False,
                    "family": family_token,
                    "h1_trend": h1_trend,
                    "h4_trend": h4_trend,
                    "aligned_side": aligned_side,
                    "countertrend_confirmed": True,
                    "reason": f"countertrend_confirmed:{direction}_vs_{aligned_side}",
                }
            return {
                "blocked": True,
                "family": family_token,
                "h1_trend": h1_trend,
                "h4_trend": h4_trend,
                "aligned_side": aligned_side,
                "countertrend_confirmed": countertrend_confirmed,
                "reason": f"mtf_block:{direction}_vs_{aligned_side}",
            }
        return {
            "blocked": False,
            "family": family_token,
            "h1_trend": h1_trend,
            "h4_trend": h4_trend,
            "aligned_side": aligned_side,
            "countertrend_confirmed": countertrend_confirmed,
            "reason": "aligned_or_neutral",
        }

    @staticmethod
    def _signature_tokens(signature: str) -> set[str]:
        return {
            str(part or "").strip().lower().replace(" ", "_")
            for part in str(signature or "").split(",")
            if str(part or "").strip()
        }

    @classmethod
    def _session_signature_matches(cls, session_sig: str, allowed_sessions: set[str]) -> bool:
        if not allowed_sessions:
            return True
        sig = str(session_sig or "").strip().lower()
        if not sig:
            return False
        sig_tokens = cls._signature_tokens(sig)
        for allowed in set(allowed_sessions or set()):
            allowed_sig = str(allowed or "").strip().lower()
            if not allowed_sig:
                continue
            if allowed_sig == sig:
                return True
            allowed_tokens = cls._signature_tokens(allowed_sig)
            if allowed_tokens and allowed_tokens.issubset(sig_tokens):
                return True
        return False

    @staticmethod
    def _timeframe_parts(token: str) -> set[str]:
        raw = str(token or "").strip().lower().replace(" ", "")
        if not raw:
            return set()
        parts = re.split(r"[+/|]", raw)
        return {str(part or "").strip() for part in parts if str(part or "").strip()}

    @classmethod
    def _timeframe_matches(cls, timeframe_token: str, allowed_tfs: set[str]) -> bool:
        if not allowed_tfs:
            return True
        tf = str(timeframe_token or "").strip().lower()
        if not tf:
            return False
        tf_parts = cls._timeframe_parts(tf)
        for allowed in set(allowed_tfs or set()):
            allowed_tf = str(allowed or "").strip().lower()
            if not allowed_tf:
                continue
            if allowed_tf == tf:
                return True
            if allowed_tf in tf_parts:
                return True
            allowed_parts = cls._timeframe_parts(allowed_tf)
            if allowed_parts and allowed_parts.issubset(tf_parts):
                return True
        return False

    def _record_xau_scheduled_live_rejection(
        self,
        signal,
        *,
        lane_source: str,
        reason: str,
        conf: float,
        min_conf: float,
        session_sig: str,
        timeframe_token: str,
        allowed_sessions: set[str],
        allowed_tfs: set[str],
    ) -> bool:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        raw.update(
            {
                "mt5_xau_scheduled_live": True,
                "mt5_xau_scheduled_live_rejected": True,
                "mt5_xau_scheduled_live_reject_reason": str(reason or ""),
                "mt5_xau_scheduled_live_session": str(session_sig or ""),
                "mt5_xau_scheduled_live_timeframe": str(timeframe_token or ""),
                "mt5_xau_scheduled_live_allowed_sessions": "|".join(sorted(list(allowed_sessions or set()))),
                "mt5_xau_scheduled_live_allowed_timeframes": ",".join(sorted(list(allowed_tfs or set()))),
                "mt5_xau_scheduled_live_confidence": round(float(conf or 0.0), 2),
                "mt5_xau_scheduled_live_min_confidence": round(float(min_conf or 0.0), 2),
            }
        )
        try:
            signal.raw_scores = raw
        except Exception:
            pass
        skipped = MT5ExecutionResult(
            ok=False,
            status="skipped",
            message=f"winner filter reject: {str(reason or '').strip()}",
            signal_symbol=str(getattr(signal, "symbol", "") or ""),
        )
        self._handle_mt5_result(signal, skipped, source=lane_source)
        if bool(getattr(config, "MT5_XAU_SCHEDULED_LIVE_NOTIFY_REJECTED", True)):
            try:
                notifier.send_mt5_execution_update(signal, skipped, source=lane_source)
            except Exception:
                logger.debug("[MT5][XAU_SCHEDULED] rejection notify skipped", exc_info=True)
        return True

    def _dispatch_mt5_lane_signal(
        self,
        signal,
        lane_source: str,
        *,
        meta: dict | None = None,
        strict_limit: bool = True,
    ) -> bool:
        if signal is None:
            return False
        lane_signal = copy.deepcopy(signal)
        try:
            raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
            if strict_limit:
                raw["mt5_limit_allow_market_fallback"] = False
            for key, value in dict(meta or {}).items():
                raw[key] = value
            lane_signal.raw_scores = raw
        except Exception:
            pass
        self._maybe_execute_mt5_signal(lane_signal, source=lane_source)
        return True

    def _maybe_execute_xau_scheduled_live(self, signal, scan_source: str) -> bool:
        if signal is None:
            return False
        if str(scan_source or "").strip().lower() != "scheduled":
            return False
        if not bool(getattr(config, "MT5_XAU_SCHEDULED_LIVE_ENABLED", False)):
            return False

        session_sig = self._signal_session_signature(signal)
        timeframe_token = self._signal_timeframe_token(signal)
        allowed_sessions = set(config.get_mt5_xau_scheduled_live_sessions() or set())
        allowed_tfs = set(config.get_mt5_xau_scheduled_live_timeframes() or set())
        try:
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        min_conf = max(0.0, float(getattr(config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0) or 78.0))
        canary_only = bool(getattr(config, "MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY", True))
        base_source = "xauusd_scheduled"
        lane_tag = str(getattr(config, "MT5_BEST_LANE_TAG", "winner") or "winner").strip().lower() or "winner"
        lane_source = f"{base_source}:{lane_tag}" if canary_only else base_source
        if conf < min_conf:
            logger.info(
                "[MT5][XAU_SCHEDULED] reject source=%s conf=%.1f < min=%.1f session=%s tf=%s",
                lane_source,
                conf,
                min_conf,
                session_sig or "-",
                timeframe_token or "-",
            )
            return self._record_xau_scheduled_live_rejection(
                signal,
                lane_source=lane_source,
                reason=f"confidence {conf:.1f} < min {min_conf:.1f}",
                conf=conf,
                min_conf=min_conf,
                session_sig=session_sig,
                timeframe_token=timeframe_token,
                allowed_sessions=allowed_sessions,
                allowed_tfs=allowed_tfs,
            )
        if not self._session_signature_matches(session_sig, allowed_sessions):
            logger.info(
                "[MT5][XAU_SCHEDULED] reject source=%s session=%s allowed=%s tf=%s conf=%.1f",
                lane_source,
                session_sig or "-",
                "|".join(sorted(list(allowed_sessions or set()))) or "-",
                timeframe_token or "-",
                conf,
            )
            return self._record_xau_scheduled_live_rejection(
                signal,
                lane_source=lane_source,
                reason=(
                    f"session mismatch got={session_sig or '-'} "
                    f"allowed={'|'.join(sorted(list(allowed_sessions or set()))) or '-'}"
                ),
                conf=conf,
                min_conf=min_conf,
                session_sig=session_sig,
                timeframe_token=timeframe_token,
                allowed_sessions=allowed_sessions,
                allowed_tfs=allowed_tfs,
            )
        if not self._timeframe_matches(timeframe_token, allowed_tfs):
            logger.info(
                "[MT5][XAU_SCHEDULED] reject source=%s tf=%s allowed=%s session=%s conf=%.1f",
                lane_source,
                timeframe_token or "-",
                ",".join(sorted(list(allowed_tfs or set()))) or "-",
                session_sig or "-",
                conf,
            )
            return self._record_xau_scheduled_live_rejection(
                signal,
                lane_source=lane_source,
                reason=(
                    f"timeframe mismatch got={timeframe_token or '-'} "
                    f"allowed={','.join(sorted(list(allowed_tfs or set()))) or '-'}"
                ),
                conf=conf,
                min_conf=min_conf,
                session_sig=session_sig,
                timeframe_token=timeframe_token,
                allowed_sessions=allowed_sessions,
                allowed_tfs=allowed_tfs,
            )
        if canary_only:
            meta = {
                "mt5_best_lane": True,
                "mt5_best_lane_source": str(lane_source),
                "mt5_best_lane_min_confidence": float(min_conf),
                "mt5_xau_scheduled_live": True,
                "mt5_xau_scheduled_live_mode": "canary_only",
                "mt5_xau_scheduled_live_session": session_sig,
                "mt5_xau_scheduled_live_timeframe": timeframe_token,
            }
            logger.info(
                "[MT5][XAU_SCHEDULED] dispatch canary lane source=%s conf=%.1f session=%s tf=%s",
                lane_source,
                conf,
                session_sig or "-",
                timeframe_token or "-",
            )
            return self._dispatch_mt5_lane_signal(signal, lane_source, meta=meta, strict_limit=True)

        meta = {
            "mt5_xau_scheduled_live": True,
            "mt5_xau_scheduled_live_mode": "main",
            "mt5_xau_scheduled_live_session": session_sig,
            "mt5_xau_scheduled_live_timeframe": timeframe_token,
        }
        logger.info(
            "[MT5][XAU_SCHEDULED] dispatch main lane source=%s conf=%.1f session=%s tf=%s",
            base_source,
            conf,
            session_sig or "-",
            timeframe_token or "-",
        )
        return self._dispatch_mt5_lane_signal(signal, base_source, meta=meta, strict_limit=True)

    def _allow_scalp_xau_live_mt5(self, signal, source: str) -> tuple[bool, str]:
        src = str(source or "").strip().lower()
        if src in {"scalp_ethusd", "scalp_btcusd"}:
            if not bool(getattr(config, "MT5_EXECUTE_CRYPTO", False)):
                return False, "crypto_live_disabled"
            return True, "crypto_live_enabled"
        if src not in {"scalp_xauusd", "scalp_xauusd:winner"}:
            return True, "not_xau_scalp"
        if bool(getattr(config, "XAU_HOLIDAY_GUARD_ENABLED", True)):
            if session_manager.is_xauusd_holiday():
                return False, "xauusd_market_holiday"
        if not session_manager.is_xauusd_market_open():
            return False, "xauusd_market_closed"
        if bool(getattr(config, "XAU_TOXIC_HOUR_GUARD_ENABLED", True)):
            try:
                toxic_hours = {int(h.strip()) for h in str(getattr(config, "XAU_TOXIC_HOURS_UTC", "1") or "1").split(",") if h.strip().isdigit()}
                utc_hour = datetime.now(timezone.utc).hour
                if utc_hour in toxic_hours:
                    return False, f"xau_toxic_hour_utc:{utc_hour}"
            except Exception:
                pass
        if bool(getattr(config, "MT5_SCALP_XAU_LIVE_FILTER_ENABLED", False)):
            session_sig = self._signal_session_signature(signal)
            allowed_sessions = set(config.get_mt5_scalp_xau_live_sessions() or set())
            if allowed_sessions and not self._session_signature_matches(session_sig, allowed_sessions):
                return False, f"session_not_allowed:{session_sig or '-'}"
        _is_sweep_reversal = bool((dict(getattr(signal, "raw_scores", {}) or {})).get("sweep_reversal"))
        if bool(getattr(config, "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED", True)) and not (
            _is_sweep_reversal and bool(getattr(config, "POST_SL_REVERSAL_BYPASS_CONF_BAND", True))
        ):
            try:
                conf = float(getattr(signal, "confidence", 0.0) or 0.0)
            except Exception:
                conf = 0.0
            conf_min = float(getattr(config, "MT5_SCALP_XAU_LIVE_CONF_MIN", 72.0) or 72.0)
            conf_max = float(getattr(config, "MT5_SCALP_XAU_LIVE_CONF_MAX", 75.0) or 75.0)
            if conf < conf_min:
                return False, f"conf_below_live_band:{conf:.1f}<{conf_min:.1f}"
            if conf >= conf_max:
                return False, f"conf_above_live_band:{conf:.1f}>={conf_max:.1f}"
        if _is_sweep_reversal and bool(getattr(config, "POST_SL_REVERSAL_BYPASS_MTF", False)):
            return True, "live_band_pass_sweep_reversal"
        mtf_guard = self._scalp_xau_direct_mtf_guard(signal)
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["xau_direct_lane_mtf_guard"] = dict(mtf_guard or {})
            signal.raw_scores = raw
        except Exception:
            pass
        if not bool((mtf_guard or {}).get("allowed")):
            return False, str((mtf_guard or {}).get("reason") or "d1_h4_h1_blocked")
        # Winner long in partial 2/3 mode can still catch falling knives when flow is weak.
        # Require flow confirmation for winner longs unless countertrend is explicitly confirmed.
        if src == "scalp_xauusd:winner":
            guard_reason = str((mtf_guard or {}).get("reason") or "")
            guard_flow_confirmed = bool((mtf_guard or {}).get("xau_mtf_flow_confirmed"))
            guard_countertrend = bool((mtf_guard or {}).get("countertrend_confirmed"))
            direction = str((mtf_guard or {}).get("direction") or self._signal_direction_token(signal) or "").strip().lower()
            if direction == "long" and guard_reason.startswith("partial_2of3_aligned:") and not (guard_flow_confirmed or guard_countertrend):
                return False, "winner_partial_long_no_flow_confirm"

            # During manager transition mode, do not allow ANY winner entries.
            # Range transition = market structure shifting — winner regime from
            # previous state is stale, both limit and market orders are unsafe.
            try:
                runtime_state = self._load_trading_routing_runtime_state()
                transition = self._active_xau_regime_transition(runtime_state)
                directive = self._active_xau_execution_directive(runtime_state)
                mode = str((transition or {}).get("mode") or "").strip().lower()
                if mode == "live_range_transition_limit_pause":
                    return False, f"winner_paused_by_transition:{mode}"
                # Also respect execution directive blocked_families for winner lane
                if directive:
                    blocked_families = {str(f or "").strip().lower() for f in list(directive.get("blocked_families") or []) if str(f or "").strip()}
                    blocked_sources = {str(s or "").strip().lower() for s in list(directive.get("blocked_sources") or []) if str(s or "").strip()}
                    if "scalp_xauusd:winner" in blocked_sources or "xau_scalp_microtrend" in blocked_families:
                        return False, f"winner_blocked_by_directive:{str(directive.get('mode') or 'directive')}"
            except Exception:
                pass
        return True, "live_band_pass"

    def _allow_ctrader_source_profile(self, signal, source: str) -> tuple[bool, str]:
        if signal is None:
            return False, "missing_signal"
        if not bool(getattr(config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True)):
            return True, "disabled"
        src = str(source or "").strip().lower()
        base_source = src.split(":", 1)[0]
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        session_sig = self._signal_session_signature(signal)
        timeframe_token = self._signal_timeframe_token(signal)
        try:
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        try:
            entry_type = str(getattr(signal, "entry_type", "") or "").strip().lower()
        except Exception:
            entry_type = ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()

        runtime_state = self._load_trading_routing_runtime_state()
        xau_regime_transition = self._active_xau_regime_transition(runtime_state)
        xau_execution_directive = self._active_xau_execution_directive(runtime_state)
        if symbol == "XAUUSD" and xau_regime_transition:
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
            except Exception:
                raw = {}
            raw["xau_manager_regime_transition"] = {
                "mode": str(xau_regime_transition.get("mode") or ""),
                "reason": str(xau_regime_transition.get("reason") or ""),
                "support_state": str(xau_regime_transition.get("support_state") or ""),
                "current_side": str(xau_regime_transition.get("current_side") or ""),
                "state_label": str(xau_regime_transition.get("state_label") or ""),
                "day_type": str(xau_regime_transition.get("day_type") or ""),
                "blocked_families": list(xau_regime_transition.get("blocked_families") or []),
                "preferred_families": list(xau_regime_transition.get("preferred_families") or []),
                "hold_until_utc": str(xau_regime_transition.get("hold_until_utc") or ""),
                "snapshot_run_id": str(xau_regime_transition.get("snapshot_run_id") or ""),
            }
            signal.raw_scores = raw
        if symbol == "XAUUSD" and xau_execution_directive:
            family = str(
                live_profile_autopilot._strategy_family_for_source(symbol, src)
                or ""
            ).strip().lower()
            blocked_direction = str(xau_execution_directive.get("blocked_direction") or "").strip().lower()
            blocked_entry_types = {
                str(item or "").strip().lower()
                for item in list(xau_execution_directive.get("blocked_entry_types") or [])
                if str(item or "").strip()
            }
            blocked_families = {
                str(item or "").strip().lower()
                for item in list(xau_execution_directive.get("blocked_families") or [])
                if str(item or "").strip()
            }
            blocked_sources = {
                str(item or "").strip().lower()
                for item in list(xau_execution_directive.get("blocked_sources") or [])
                if str(item or "").strip()
            }
            preferred_families = {
                str(item or "").strip().lower()
                for item in list(xau_execution_directive.get("preferred_families") or [])
                if str(item or "").strip()
            }
            preferred_sources = {
                str(item or "").strip().lower()
                for item in list(xau_execution_directive.get("preferred_sources") or [])
                if str(item or "").strip()
            }
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
            except Exception:
                raw = {}
            raw["xau_manager_execution_directive"] = {
                "mode": str(xau_execution_directive.get("mode") or ""),
                "reason": str(xau_execution_directive.get("reason") or ""),
                "support_state": str(xau_execution_directive.get("support_state") or ""),
                "blocked_families": sorted(list(blocked_families)),
                "preferred_families": sorted(list(preferred_families)),
                "pause_until_utc": str(xau_execution_directive.get("pause_until_utc") or ""),
                "trigger_run_id": str(xau_execution_directive.get("trigger_run_id") or ""),
            }
            if family and (family in preferred_families or src in preferred_sources):
                raw["xau_manager_directive_priority"] = "preferred_family"
            signal.raw_scores = raw
            if (
                blocked_direction
                and direction == blocked_direction
                and entry_type in blocked_entry_types
                and ((family and family in blocked_families) or src in blocked_sources)
            ):
                raw["xau_manager_directive_block"] = True
                raw["xau_manager_directive_block_reason"] = str(xau_execution_directive.get("reason") or "")
                signal.raw_scores = raw
                return False, f"xau_manager_directive_block:{str(xau_execution_directive.get('mode') or 'directive')}:{family or src}"

        if symbol == "XAUUSD":
            allowed_style, style_reason = self._xau_forced_style_guard(signal, source=src, runtime_state=runtime_state)
            if not allowed_style:
                return False, style_reason

        if base_source == "xauusd_scheduled":
            min_conf = max(0.0, float(getattr(config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0) or 70.0))
            allowed_sessions = set(config.get_ctrader_xau_scheduled_allowed_sessions() or set())
            allowed_tfs = set(config.get_ctrader_xau_scheduled_allowed_timeframes() or set())
            allowed_entry_types = set(config.get_ctrader_xau_scheduled_allowed_entry_types() or set())
            if conf < min_conf:
                return False, f"xau_scheduled_conf_below:{conf:.1f}<{min_conf:.1f}"
            try:
                _np_bypass = float((dict(getattr(signal, "raw_scores", {}) or {})).get("neural_probability", 0.0) or 0.0)
                _np_threshold = float(getattr(config, "XAU_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD", 0.85) or 0.85)
                if _np_bypass >= _np_threshold:
                    return True, f"xau_scheduled_high_conf_session_bypass:np={_np_bypass:.2f}"
            except Exception:
                pass
            if allowed_sessions and (not self._session_signature_matches(session_sig, allowed_sessions)):
                return False, f"xau_scheduled_session_not_allowed:{session_sig or '-'}"
            if allowed_tfs and (not self._timeframe_matches(timeframe_token, allowed_tfs)):
                return False, f"xau_scheduled_timeframe_not_allowed:{timeframe_token or '-'}"
            if allowed_entry_types and entry_type not in allowed_entry_types:
                return False, f"xau_scheduled_entry_type_not_allowed:{entry_type or '-'}"
            if bool(getattr(config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", True)):
                mtf_guard = self._scalp_xau_direct_mtf_guard(signal)
                mtf_reason = str(mtf_guard.get("reason") or "")
                if not bool(mtf_guard.get("allowed", True)) and "d1_h4_h1_block" in mtf_reason:
                    return False, f"xau_scheduled_mtf_block:{mtf_reason}"
            return True, "xau_scheduled_profile_pass"

        if base_source == "scalp_btcusd":
            min_conf = max(0.0, float(getattr(config, "CTRADER_BTC_WINNER_MIN_CONFIDENCE", 70.0) or 70.0))
            max_conf = max(min_conf + 0.1, float(getattr(config, "CTRADER_BTC_WINNER_MAX_CONFIDENCE", 75.0) or 75.0))
            allowed_sessions = set(config.get_ctrader_btc_winner_allowed_sessions_weekend() or set())
            if conf < min_conf:
                return False, f"btc_winner_conf_below:{conf:.1f}<{min_conf:.1f}"
            if conf >= max_conf:
                return False, f"btc_winner_conf_above:{conf:.1f}>={max_conf:.1f}"
            if allowed_sessions and (not self._session_signature_matches(session_sig, allowed_sessions)):
                return False, f"btc_winner_session_not_allowed:{session_sig or '-'}"
            return True, "btc_winner_profile_pass"

        if base_source == "scalp_ethusd" and (":winner" in src or src == "scalp_ethusd"):
            if not bool(getattr(config, "CTRADER_ETH_WINNER_DIRECT_ENABLED", False)):
                return False, "eth_winner_direct_disabled"
        return True, "source_profile_pass"

    def _xau_pre_dispatch_runtime_snapshot(self, runtime_state: dict | None = None) -> dict:
        state = dict(runtime_state or self._load_trading_routing_runtime_state() or {})
        shock_state = dict((state.get("xau_shock_profile") or {}))
        regime_transition = self._active_xau_regime_transition(state)
        execution_directive = self._active_xau_execution_directive(state)
        cluster_guard = dict((state.get("xau_cluster_loss_guard") or {}))
        snap: dict = {}
        if shock_state:
            snap["xau_shock_profile"] = {
                "status": str(shock_state.get("status") or ""),
                "mode": str(shock_state.get("mode") or ""),
                "reason": str(shock_state.get("reason") or ""),
            }
        if regime_transition:
            snap["xau_regime_transition"] = {
                "status": str(regime_transition.get("status") or ""),
                "mode": str(regime_transition.get("mode") or ""),
                "reason": str(regime_transition.get("reason") or ""),
                "support_state": str(regime_transition.get("support_state") or ""),
                "state_label": str(regime_transition.get("state_label") or ""),
                "day_type": str(regime_transition.get("day_type") or ""),
                "current_side": str(regime_transition.get("current_side") or ""),
                "hold_until_utc": str(regime_transition.get("hold_until_utc") or ""),
            }
        if execution_directive:
            snap["xau_execution_directive"] = {
                "status": str(execution_directive.get("status") or ""),
                "mode": str(execution_directive.get("mode") or ""),
                "reason": str(execution_directive.get("reason") or ""),
                "support_state": str(execution_directive.get("support_state") or ""),
                "blocked_direction": str(execution_directive.get("blocked_direction") or ""),
                "blocked_entry_types": list(execution_directive.get("blocked_entry_types") or []),
                "blocked_families": list(execution_directive.get("blocked_families") or []),
                "preferred_families": list(execution_directive.get("preferred_families") or []),
                "pause_until_utc": str(execution_directive.get("pause_until_utc") or ""),
            }
        if str(cluster_guard.get("status") or "").strip().lower() == "active":
            snap["xau_cluster_loss_guard"] = {
                "status": str(cluster_guard.get("status") or ""),
                "mode": str(cluster_guard.get("mode") or ""),
                "reason": str(cluster_guard.get("reason") or ""),
                "blocked_direction": str(cluster_guard.get("blocked_direction") or ""),
            }
        return snap

    def _audit_xau_pre_dispatch_skip(
        self,
        signal,
        *,
        requested_source: str,
        dispatch_source: str,
        gate: str,
        reason: str,
        dispatch_meta: dict | None = None,
        runtime_state: dict | None = None,
    ) -> int:
        symbol = str(self._signal_symbol_key(signal) or getattr(signal, "symbol", "") or "").strip().upper()
        if not self._is_xau_symbol(symbol):
            return 0
        effective_source = str(dispatch_source or requested_source or "").strip()
        trace = self._ensure_signal_trace(signal, source=effective_source)
        raw = self._signal_raw_scores(signal)
        family = str(
            live_profile_autopilot._strategy_family_for_source(symbol, effective_source.lower())
            or raw.get("strategy_family")
            or raw.get("family")
            or ""
        ).strip().lower()
        raw = self._apply_xau_observability_tags(raw, source=effective_source, family=family)
        raw["ctrader_pre_dispatch_blocked"] = True
        raw["ctrader_pre_dispatch_gate"] = str(gate or "")
        raw["ctrader_pre_dispatch_reason"] = str(reason or "")
        raw["ctrader_pre_dispatch_requested_source"] = str(requested_source or "")
        raw["ctrader_pre_dispatch_dispatch_source"] = str(dispatch_source or "")
        raw["ctrader_pre_dispatch_trace_tag"] = str(trace.get("tag", "-") or "-")
        if dispatch_meta:
            raw["ctrader_pre_dispatch_dispatch_meta"] = dict(dispatch_meta or {})
        try:
            signal.raw_scores = raw
        except Exception:
            pass

        dispatch_meta_safe = self._json_safe_copy(dispatch_meta or {})
        raw_safe = self._json_safe_copy(raw)
        runtime_safe = self._json_safe_copy(self._xau_pre_dispatch_runtime_snapshot(runtime_state=runtime_state))
        request_payload = {
            "source": str(effective_source or requested_source or ""),
            "requested_source": str(requested_source or ""),
            "dispatch_source": str(dispatch_source or ""),
            "symbol": symbol,
            "direction": str(getattr(signal, "direction", "") or ""),
            "entry_type": str(getattr(signal, "entry_type", "") or ""),
            "confidence": self._signal_float_value(signal, "confidence"),
            "entry": self._signal_float_value(signal, "entry"),
            "stop_loss": self._signal_float_value(signal, "stop_loss"),
            "take_profit_1": self._signal_float_value(signal, "take_profit_1"),
            "take_profit_2": self._signal_float_value(signal, "take_profit_2"),
            "take_profit_3": self._signal_float_value(signal, "take_profit_3"),
            "session": str(getattr(signal, "session", "") or ""),
            "timeframe": str(getattr(signal, "timeframe", "") or ""),
            "pattern": str(getattr(signal, "pattern", "") or ""),
            "signal_run_id": str(trace.get("run_id", "") or ""),
            "signal_run_no": int(trace.get("run_no", 0) or 0),
            "raw_scores": raw_safe,
        }
        response_payload = {
            "pre_dispatch_audit": True,
            "gate": str(gate or ""),
            "reason": str(reason or ""),
            "dispatch_meta": dispatch_meta_safe,
        }
        execution_meta = {
            "pre_dispatch_audit": True,
            "requested_source": str(requested_source or ""),
            "dispatch_source": str(dispatch_source or ""),
            "dispatch_meta": dispatch_meta_safe,
            "runtime_state": runtime_safe,
            "audit_tags": [
                "xau_pre_dispatch_skip",
                f"gate:{str(gate or '').strip().lower()}",
            ],
        }
        try:
            return int(
                ctrader_executor.journal_pre_dispatch_skip(
                    signal,
                    source=effective_source or str(requested_source or ""),
                    reason=str(reason or "pre_dispatch_blocked"),
                    gate=str(gate or "pre_dispatch"),
                    request_payload=request_payload,
                    response_payload=response_payload,
                    execution_meta=execution_meta,
                )
                or 0
            )
        except Exception as e:
            logger.warning(
                "[CTRADER] pre-dispatch audit failed source=%s symbol=%s gate=%s err=%s",
                effective_source or str(requested_source or ""),
                symbol,
                str(gate or ""),
                e,
            )
            return 0

    def _maybe_execute_mt5_best_lane(self, signal, source: str) -> bool:
        """
        Optional secondary lane to follow high-confidence model signals while keeping
        the primary lane unchanged. Intended for side-by-side performance comparison.
        """
        if signal is None:
            return False
        if not bool(getattr(config, "MT5_ENABLED", False)):
            return False
        if not bool(getattr(config, "MT5_BEST_LANE_ENABLED", False)):
            return False

        src = str(source or "").strip().lower()
        lane_tag = str(getattr(config, "MT5_BEST_LANE_TAG", "winner") or "winner").strip().lower()
        if not lane_tag:
            lane_tag = "winner"
        if f":{lane_tag}" in src:
            return False

        enabled_sources = set(config.get_mt5_best_lane_sources() or set())
        if enabled_sources and (src not in enabled_sources):
            return False

        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        enabled_symbols = set(config.get_mt5_best_lane_symbols() or set())
        if enabled_symbols and symbol and (symbol not in enabled_symbols):
            return False

        try:
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        min_conf = max(0.0, float(getattr(config, "MT5_BEST_LANE_MIN_CONFIDENCE", 72.0) or 72.0))
        symbol_overrides = dict(config.get_mt5_best_lane_min_confidence_symbol_overrides() or {})
        if symbol and symbol in symbol_overrides:
            try:
                min_conf = max(0.0, float(symbol_overrides.get(symbol, min_conf)))
            except Exception:
                pass
        if conf < min_conf:
            return False

        lane_source = f"{source}:{lane_tag}"
        logger.info(
            "[MT5][BEST] dispatch lane source=%s symbol=%s conf=%.1f",
            lane_source,
            symbol or "-",
            conf,
        )
        return self._dispatch_mt5_lane_signal(
            signal,
            lane_source,
            meta={
                "mt5_best_lane": True,
                "mt5_best_lane_source": str(lane_source),
                "mt5_best_lane_min_confidence": float(min_conf),
            },
            strict_limit=True,
        )

    def _resolve_mt5_bypass_profile(self, signal, source: str) -> dict:
        profile = {
            "enabled": False,
            "source": str(source or ""),
            "suffix": "",
            "skip_neural_filter": False,
            "skip_risk_governor": False,
            "skip_mt5_confidence": False,
            "ignore_open_positions": False,
            "magic_offset": 0,
        }
        if not bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False)):
            return profile
        src_raw = str(source or "").strip().lower()
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        sources = set(config.get_mt5_bypass_test_sources() or set())
        symbols = set(config.get_mt5_bypass_test_symbols() or set())
        if sources and ("*" not in sources) and ("all" not in sources) and (src_raw not in sources):
            return profile
        if symbols and symbol and (symbol not in symbols):
            return profile
        suffix = str(getattr(config, "MT5_BYPASS_TEST_SOURCE_SUFFIX", "bypass") or "bypass").strip().lower()
        exec_source = str(source or "")
        if suffix:
            suffix_tag = f":{suffix}"
            if suffix_tag not in exec_source.lower():
                exec_source = f"{exec_source}{suffix_tag}"
        profile.update(
            {
                "enabled": True,
                "source": exec_source,
                "suffix": suffix,
                "skip_neural_filter": bool(getattr(config, "MT5_BYPASS_TEST_SKIP_NEURAL_FILTER", True)),
                "skip_risk_governor": bool(getattr(config, "MT5_BYPASS_TEST_SKIP_RISK_GOVERNOR", True)),
                "skip_mt5_confidence": bool(getattr(config, "MT5_BYPASS_TEST_SKIP_MT5_CONFIDENCE", True)),
                "ignore_open_positions": bool(getattr(config, "MT5_BYPASS_TEST_IGNORE_OPEN_POSITIONS", True)),
                "magic_offset": int(getattr(config, "MT5_BYPASS_TEST_MAGIC_OFFSET", 500) or 0),
            }
        )
        return profile

    def _ctrader_pick_dispatch_source(self, signal, source: str) -> tuple[str, dict]:
        base_source = str(source or "").strip()
        src = base_source.lower()
        meta = {
            "requested_source": base_source,
            "dispatch_source": "",
            "winner_candidate": "",
            "winner_reason": "",
        }
        if not base_source:
            meta["winner_reason"] = "missing_source"
            return "", meta
        allowed_sources = set(getattr(config, "get_ctrader_allowed_sources", lambda: set())() or set())
        if not allowed_sources:
            meta["dispatch_source"] = base_source
            meta["winner_reason"] = "allow_all"
            return base_source, meta

        # ── Standalone scanners: bypass winner routing, dispatch directly ──────
        # FiboAdvance is a self-contained scanner with its own confluence logic.
        # It does not participate in winner/regime routing — pass through directly
        # if the base source is allowed.
        _standalone_sources = {"fibo_xauusd"}
        if src in _standalone_sources and src in allowed_sources:
            meta["dispatch_source"] = base_source
            meta["winner_reason"] = "standalone_direct_pass"
            return base_source, meta

        winner_source = ""
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        if (":winner" not in src) and (":bypass" not in src):
            candidate = f"{base_source}:winner"
            candidate_key = candidate.lower()
            if candidate_key in allowed_sources:
                if src == "xauusd_scheduled":
                    session_sig = self._signal_session_signature(signal)
                    timeframe_token = self._signal_timeframe_token(signal)
                    allowed_sessions = set(config.get_mt5_xau_scheduled_live_sessions() or set())
                    allowed_tfs = set(config.get_mt5_xau_scheduled_live_timeframes() or set())
                    try:
                        conf = float(getattr(signal, "confidence", 0.0) or 0.0)
                    except Exception:
                        conf = 0.0
                    min_conf = max(0.0, float(getattr(config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0) or 78.0))
                    if conf >= min_conf and self._session_signature_matches(session_sig, allowed_sessions) and self._timeframe_matches(timeframe_token, allowed_tfs):
                        winner_source = candidate
                        meta["winner_reason"] = "xau_scheduled_live_profile"
                    else:
                        meta["winner_reason"] = "xau_scheduled_profile_not_matched"
                else:
                    regime = str(
                        raw.get("crypto_winner_logic_regime")
                        or raw.get("winner_logic_regime")
                        or ""
                    ).strip().lower()
                    if regime == "strong":
                        winner_source = candidate
                        meta["winner_reason"] = "winner_logic_strong"
                    else:
                        meta["winner_reason"] = f"winner_logic_regime:{regime or 'none'}"
                        # ETH-only mission-control gate:
                        # If winner-memory says ETH is not in "strong" regime, block the fallback to the base
                        # (non-:winner, non-:canary) lane to prevent repeating weak/losing ETH routes.
                        eth_symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
                        if (
                            eth_symbol == "ETHUSD"
                            and src.startswith("scalp_ethusd")
                            and (":canary" not in src)
                            and (":bypass" not in src)
                        ):
                            meta["winner_reason"] = f"eth_winner_memory_block:{regime or 'none'}"
                            return "", meta
            else:
                meta["winner_reason"] = "winner_source_not_allowed"
            meta["winner_candidate"] = candidate
        if winner_source:
            meta["dispatch_source"] = winner_source
            return winner_source, meta
        if src in allowed_sources:
            meta["dispatch_source"] = base_source
            if not meta["winner_reason"]:
                meta["winner_reason"] = "base_source_allowed"
            return base_source, meta
        if not meta["winner_reason"]:
            meta["winner_reason"] = "source_not_allowed"
        return "", meta

    @staticmethod
    def _mt5_lane_key_from_source(source: str) -> str:
        src = str(source or "").strip().lower()
        if not src:
            return "main"
        if ":canary" in src or src.endswith("canary"):
            return "canary"
        if ":bypass" in src or src.endswith("bypass"):
            return "bypass"
        lane_tag = str(getattr(config, "MT5_BEST_LANE_TAG", "winner") or "winner").strip().lower()
        if lane_tag and (f":{lane_tag}" in src or src == lane_tag):
            return "winner"
        return "main"

    def _apply_mt5_lane_limit_policy(self, signal, exec_source: str) -> None:
        if signal is None:
            return
        try:
            entry_type = str(getattr(signal, "entry_type", "") or "").strip().lower()
        except Exception:
            entry_type = ""
        if entry_type != "limit":
            return
        lane = self._mt5_lane_key_from_source(exec_source)
        base_source = str(exec_source or "").split(":", 1)[0].strip().lower()
        strict = False
        reason = ""
        if lane == "winner":
            strict = True
            reason = "winner_lane_strict_limit"
        elif lane in {"main", "canary"} and base_source.startswith("scalp_"):
            strict = True
            reason = "main_scalp_strict_limit" if lane == "main" else "canary_scalp_strict_limit"
        elif lane == "canary" and base_source == "xauusd_scheduled":
            strict = True
            reason = "canary_scheduled_strict_limit"
        if not strict:
            return
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["mt5_limit_allow_market_fallback"] = False
            raw["mt5_limit_policy"] = "strict_limit"
            raw["mt5_limit_policy_reason"] = str(reason)
            signal.raw_scores = raw
        except Exception:
            return

    @staticmethod
    def _strategy_family_alias(family: str) -> str:
        token = str(family or "").strip().lower()
        aliases = {
            "xau_scalp_pullback_limit": "pb",
            "xau_scalp_breakout_stop": "bs",
            "xau_scalp_tick_depth_filter": "td",
            "xau_scalp_failed_fade_follow_stop": "ff",
            "xau_scalp_microtrend_follow_up": "mfu",
            "xau_scalp_flow_short_sidecar": "fss",
            "xau_scalp_flow_long_sidecar": "fls",
            "xau_scalp_range_repair": "rr",
            "xau_scalp_prelondon_sweep_cont": "psc",
            "btc_weekday_lob_momentum": "bwl",
            "btc_scalp_flow_short_sidecar": "bfss",
            "btc_scalp_flow_long_sidecar": "bfls",
            "btc_scalp_range_repair": "brr",
            "eth_weekday_overlap_probe": "ewp",
            "crypto_flow_short": "cfs",
            "crypto_flow_buy": "cfb",
            "crypto_winner_confirmed": "cwc",
            "crypto_behavioral_retest": "cbr",
        }
        return aliases.get(token, token.replace("xau_scalp_", "")[:6])

    def _strategy_family_lane_source(self, base_source: str, family: str) -> str:
        base = str(base_source or "").strip().lower().split(":", 1)[0]
        alias = self._strategy_family_alias(family)
        return f"{base}:{alias}:canary"

    def _load_trading_manager_runtime_state(self) -> dict:
        path = Path(__file__).resolve().parent / "data" / "runtime" / "trading_manager_state.json"
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
        return {}

    def _load_trading_team_runtime_state(self) -> dict:
        path = Path(__file__).resolve().parent / "data" / "runtime" / "trading_team_state.json"
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    return {}
                if not any(
                    key in payload
                    for key in (
                        "status",
                        "symbols",
                        "opportunity_feed",
                        "xau_family_routing",
                        "xau_parallel_families",
                        "xau_order_care",
                    )
                ):
                    return {}
                return payload
        except Exception:
            return {}
        return {}

    def _load_strategy_lab_team_runtime_state(self) -> dict:
        if not bool(getattr(config, "STRATEGY_LAB_TEAM_ENABLED", True)):
            return {}
        path = Path(__file__).resolve().parent / "data" / "runtime" / "strategy_lab_team_state.json"
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    return {}
                if "status" not in payload or "symbols" not in payload:
                    return {}
                return payload
        except Exception:
            return {}
        return {}

    def _load_trading_routing_runtime_state(self) -> dict:
        if bool(getattr(config, "TRADING_TEAM_ENABLED", True)):
            team_state = self._load_trading_team_runtime_state()
            if team_state:
                return team_state
        return self._load_trading_manager_runtime_state()

    @staticmethod
    def _active_xau_execution_directive(runtime_state: dict | None) -> dict:
        state = dict(((runtime_state or {}).get("xau_execution_directive") or {}))
        if str(state.get("status") or "").strip().lower() != "active":
            return {}
        pause_until_raw = str(state.get("pause_until_utc") or "").strip()
        if pause_until_raw:
            try:
                pause_until = datetime.fromisoformat(pause_until_raw.replace("Z", "+00:00"))
                if pause_until.tzinfo is None:
                    pause_until = pause_until.replace(tzinfo=timezone.utc)
                if pause_until.astimezone(timezone.utc) <= datetime.now(timezone.utc):
                    return {}
            except Exception:
                pass
        return state

    @staticmethod
    def _active_xau_regime_transition(runtime_state: dict | None) -> dict:
        state = dict(((runtime_state or {}).get("xau_regime_transition") or {}))
        if str(state.get("status") or "").strip().lower() != "active":
            return {}
        hold_until_raw = str(state.get("hold_until_utc") or "").strip()
        if hold_until_raw:
            try:
                hold_until = datetime.fromisoformat(hold_until_raw.replace("Z", "+00:00"))
                if hold_until.tzinfo is None:
                    hold_until = hold_until.replace(tzinfo=timezone.utc)
                if hold_until.astimezone(timezone.utc) <= datetime.now(timezone.utc):
                    return {}
            except Exception:
                pass
        return state

    def _load_strategy_family_candidates(self, *, symbol: str, base_source: str) -> list[dict]:
        family_enabled = bool(getattr(config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False))
        experimental_enabled = bool(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", False))
        if not family_enabled and not experimental_enabled:
            return []
        allowed_families = set(getattr(config, "get_persistent_canary_strategy_families", lambda: set())() or set()) if family_enabled else set()
        experimental_families = set(getattr(config, "get_persistent_canary_experimental_families", lambda: set())() or set()) if experimental_enabled else set()
        standard_limit = max(0, int(getattr(config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 2) or 2))
        experimental_limit = max(0, int(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 1) or 1))
        xau_opportunity_sidecar_active = False
        manager_opportunity_priority: dict[str, float] = {}
        budget_production_families: set[str] = set()
        budget_sampling_families: set[str] = set()
        budget_live_edge_scores: dict[str, float] = {}
        runtime_state = self._load_trading_routing_runtime_state()
        xau_execution_directive = self._active_xau_execution_directive(runtime_state)
        directive_blocked_families: set[str] = set()
        directive_preferred_families: set[str] = set()
        strategy_lab_state = self._load_strategy_lab_team_runtime_state()
        symbol_token = str(symbol or "").strip().upper()
        strategy_lab_symbol = dict((strategy_lab_state.get("symbols") or {}).get(symbol_token) or {})
        strategy_lab_strategy_states = {
            str(key or "").strip(): str(value or "").strip().lower()
            for key, value in dict(strategy_lab_symbol.get("strategy_states") or {}).items()
            if str(key or "").strip()
        }
        strategy_lab_family_states = {
            str(key or "").strip().lower(): str(value or "").strip().lower()
            for key, value in dict(strategy_lab_symbol.get("family_states") or {}).items()
            if str(key or "").strip()
        }
        strategy_lab_execution_priority = {
            str(fam or "").strip().lower(): float(score or 0.0)
            for fam, score in dict(strategy_lab_symbol.get("execution_family_priority_map") or {}).items()
            if str(fam or "").strip()
        }
        strategy_lab_recovery_strategy_ids = {
            str(item or "").strip()
            for item in list(strategy_lab_symbol.get("recovery_strategy_ids") or [])
            if str(item or "").strip()
        }
        strategy_lab_recovery_families = {
            str(item or "").strip().lower()
            for item in list(strategy_lab_symbol.get("recovery_families") or [])
            if str(item or "").strip()
        }
        strategy_lab_exec_bypass = self._parse_lower_csv(
            str(getattr(config, "PERSISTENT_CANARY_IGNORE_STRATEGY_LAB_BLOCK", "") or "")
        )

        def _strategy_lab_mode(strategy_id: str, family: str) -> str:
            sid = str(strategy_id or "").strip()
            family_token = str(family or "").strip().lower()
            if sid and sid in strategy_lab_strategy_states:
                return str(strategy_lab_strategy_states.get(sid) or "")
            if family_token and family_token in strategy_lab_family_states:
                return str(strategy_lab_family_states.get(family_token) or "")
            return ""

        def _effective_strategy_lab_mode(strategy_id: str, family: str) -> str:
            mode = _strategy_lab_mode(strategy_id, family)
            sid = str(strategy_id or "").strip()
            family_token = str(family or "").strip().lower()
            if mode in {"blocked", "shadow"} and ((sid and sid in strategy_lab_recovery_strategy_ids) or (family_token and family_token in strategy_lab_recovery_families)):
                return "recovery"
            return mode

        def _strategy_lab_family_allowed(family: str) -> bool:
            ft = str(family or "").strip().lower()
            if ft and ft in strategy_lab_exec_bypass:
                return True
            return _effective_strategy_lab_mode("", family) not in {"blocked", "shadow"}

        if symbol_token == "XAUUSD":
            directive_blocked_families = {
                str(f or "").strip().lower()
                for f in list(xau_execution_directive.get("blocked_families") or [])
                if str(f or "").strip()
            }
            directive_preferred_families = {
                str(f or "").strip().lower()
                for f in list(xau_execution_directive.get("preferred_families") or [])
                if str(f or "").strip()
            }
            active_families = set(getattr(config, "get_ctrader_xau_active_families", lambda: set())() or set())
            family_routing_state = dict((runtime_state or {}).get("xau_family_routing") or {})
            team_active_families = {
                str(f or "").strip().lower()
                for f in list(family_routing_state.get("active_families") or [])
                if str(f or "").strip()
            }
            parallel_state = dict((runtime_state or {}).get("xau_parallel_families") or {})
            hedge_state = dict((runtime_state or {}).get("xau_hedge_transition") or {})
            opportunity_sidecar_state = dict((runtime_state or {}).get("xau_opportunity_sidecar") or {})
            opportunity_feed_state = dict((runtime_state or {}).get("opportunity_feed") or {})
            budget_state = dict((runtime_state or {}).get("xau_family_budget") or {})
            xau_feed = dict((opportunity_feed_state.get("symbols") or {}).get("XAUUSD") or {})
            if str(budget_state.get("status") or "").strip().lower() == "active":
                budget_production_families = {
                    str(f or "").strip().lower()
                    for f in list(budget_state.get("production_families") or [])
                    if str(f or "").strip()
                }
                budget_sampling_families = {
                    str(f or "").strip().lower()
                    for f in list(budget_state.get("sampling_families") or [])
                    if str(f or "").strip()
                }
                budget_live_edge_scores = {
                    str(fam or "").strip().lower(): float(
                        (details or {}).get("live_edge_score", 0.0) or 0.0
                    ) + float((details or {}).get("comparison_bonus", 0.0) or 0.0)
                    for fam, details in dict(budget_state.get("family_live_edge_map") or {}).items()
                    if str(fam or "").strip()
                }
            if team_active_families:
                active_families |= team_active_families
                allowed_families |= team_active_families
                experimental_families |= team_active_families
            if budget_production_families:
                active_families |= budget_production_families
                allowed_families |= budget_production_families
                standard_limit = max(
                    standard_limit,
                    max(1, int(budget_state.get("production_parallel_limit", len(budget_production_families)) or len(budget_production_families) or 1)),
                )
            if budget_sampling_families:
                experimental_families |= budget_sampling_families
                budget_sampling_limit = max(0, int(budget_state.get("sampling_parallel_limit", experimental_limit) or experimental_limit))
                budget_total_limit = len(budget_production_families) + budget_sampling_limit
                experimental_limit = min(experimental_limit, budget_total_limit) if budget_total_limit >= 0 else experimental_limit
            parallel_allowed = {
                str(f or "").strip().lower()
                for f in list(parallel_state.get("allowed_families") or [])
                if str(f or "").strip()
            } if str(parallel_state.get("status") or "") == "active" else set()
            hedge_allowed = {
                str(f or "").strip().lower()
                for f in list(hedge_state.get("allowed_families") or [])
                if str(f or "").strip()
            } if str(hedge_state.get("status") or "") == "active" else set()
            if parallel_allowed:
                active_families |= parallel_allowed
                allowed_families |= parallel_allowed
                experimental_families |= parallel_allowed
                parallel_limit = max(1, int(parallel_state.get("max_same_direction_families", 3) or 3))
                experimental_limit = max(experimental_limit, max(0, parallel_limit - 1))
            if hedge_allowed:
                experimental_families |= hedge_allowed
            if str(opportunity_sidecar_state.get("status") or "") == "active":
                xau_opportunity_sidecar_active = True
                # Inject all families that conductor set for this regime (bear=FSS, bull=FLS)
                _sidecar_families = set(opportunity_sidecar_state.get("families") or [])
                experimental_families.update(_sidecar_families)
            manager_opportunity_priority = {
                str(fam or "").strip().lower(): float(score or 0.0)
                for fam, score in dict(xau_feed.get("family_priority_map") or {}).items()
                if str(fam or "").strip()
            }
            if active_families:
                allowed_families = allowed_families & active_families if allowed_families else active_families
            if xau_opportunity_sidecar_active:
                experimental_limit = max(experimental_limit, 2)
            if manager_opportunity_priority:
                prioritized_experimental = [
                    fam for fam in list(manager_opportunity_priority.keys())
                    if str(fam or "").strip().lower() in experimental_families
                ]
                if prioritized_experimental:
                    experimental_limit = max(experimental_limit, min(3, len(prioritized_experimental)))
        if not allowed_families and not experimental_families:
            return []
        family_routing_state = dict((runtime_state or {}).get("xau_family_routing") or {})
        family_routing_mode = str(family_routing_state.get("mode") or "").strip().lower()
        primary_family = ""
        if family_routing_mode != "swarm_support_all":
            primary_family = str(
                family_routing_state.get("primary_family")
                or getattr(config, "CTRADER_XAU_PRIMARY_FAMILY", "")
                or ""
            ).strip().lower()
        report_path = Path(__file__).resolve().parent / "data" / "reports" / "strategy_lab_report.json"
        standard_candidates: list[dict] = []
        experimental_candidates: list[dict] = []
        payload = {}
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        except Exception:
            payload = {}
        base_token = str(base_source or "").strip().lower().split(":", 1)[0]
        expected_base = {
            "XAUUSD": "scalp_xauusd",
            "BTCUSD": "scalp_btcusd",
            "ETHUSD": "scalp_ethusd",
        }.get(symbol_token, "")
        for row in list((payload.get("candidates") if isinstance(payload, dict) else []) or []):
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol") or "").strip().upper()
            family = str(row.get("family") or "").strip().lower()
            strategy_id = str(row.get("strategy_id") or "").strip()
            exec_ready = bool(row.get("execution_ready", False))
            if sym != symbol_token:
                continue
            is_experimental = bool(row.get("experimental"))
            if is_experimental:
                if family not in experimental_families:
                    continue
            elif family not in allowed_families:
                continue
            if not exec_ready:
                continue
            if expected_base and base_token != expected_base:
                continue
            strategy_lab_mode = _effective_strategy_lab_mode(strategy_id, family)
            if strategy_lab_mode in {"blocked", "shadow"}:
                continue
            if strategy_lab_mode:
                row["strategy_lab_mode"] = strategy_lab_mode
            if is_experimental:
                experimental_candidates.append(dict(row))
            else:
                standard_candidates.append(dict(row))
        if symbol_token == "XAUUSD" and base_token == "scalp_xauusd":
            fallback_experimental = []
            if "xau_scalp_tick_depth_filter" in experimental_families and _strategy_lab_family_allowed("xau_scalp_tick_depth_filter"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_tick_depth_filter",
                        "strategy_id": "xau_scalp_tick_depth_filter_v1",
                        "priority": 199,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_microtrend_follow_up" in experimental_families and _strategy_lab_family_allowed("xau_scalp_microtrend_follow_up"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_microtrend_follow_up",
                        "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                        "priority": 189,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_flow_short_sidecar" in experimental_families and _strategy_lab_family_allowed("xau_scalp_flow_short_sidecar"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_flow_short_sidecar",
                        "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                        "priority": 179,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_flow_long_sidecar" in experimental_families and _strategy_lab_family_allowed("xau_scalp_flow_long_sidecar"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_flow_long_sidecar",
                        "strategy_id": "xau_scalp_flow_long_sidecar_v1",
                        "priority": 178,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_failed_fade_follow_stop" in experimental_families and _strategy_lab_family_allowed("xau_scalp_failed_fade_follow_stop"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_failed_fade_follow_stop",
                        "strategy_id": "xau_scalp_failed_fade_follow_stop_v1",
                        "priority": 169,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_range_repair" in experimental_families and _strategy_lab_family_allowed("xau_scalp_range_repair"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_range_repair",
                        "strategy_id": "xau_scalp_range_repair_v1",
                        "priority": 159,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_prelondon_sweep_cont" in experimental_families and _strategy_lab_family_allowed("xau_scalp_prelondon_sweep_cont"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_prelondon_sweep_cont",
                        "strategy_id": "xau_scalp_prelondon_sweep_cont_v1",
                        "priority": 155,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            present_experimental = {
                str(item.get("family") or "").strip().lower()
                for item in list(experimental_candidates or [])
            }
            for row in fallback_experimental:
                family = str(row.get("family") or "").strip().lower()
                if family and family not in present_experimental:
                    experimental_candidates.append(dict(row))
                    present_experimental.add(family)
        swarm_support_all = symbol_token == "XAUUSD" and family_routing_mode == "swarm_support_all"
        if standard_candidates or experimental_candidates:
            def _priority_sort_key(item: dict, *, experimental: bool) -> tuple:
                family = str(item.get("family", "") or "").strip().lower()
                strategy_lab_mode = str(item.get("strategy_lab_mode") or _effective_strategy_lab_mode(str(item.get("strategy_id") or ""), family) or "")
                strategy_lab_boost = float(strategy_lab_execution_priority.get(family, 0.0) or 0.0)
                boost = float(manager_opportunity_priority.get(family, 0.0) or 0.0)
                budget_rank = 0 if family in budget_production_families else 1 if family in budget_sampling_families else 2
                budget_live_edge = float(budget_live_edge_scores.get(family, 0.0) or 0.0)
                return (
                    budget_rank,
                    0 if budget_live_edge > 0.0 else 1,
                    -budget_live_edge,
                    0 if strategy_lab_mode == "promotable" else 1 if strategy_lab_mode == "live_shadow" else 2 if strategy_lab_mode == "recovery" else 3,
                    0 if strategy_lab_boost > 0.0 else 1,
                    -strategy_lab_boost,
                    0 if family in directive_preferred_families else 1,
                    1 if family in directive_blocked_families else 0,
                    0 if boost > 0.0 else 1,
                    -boost,
                    0 if str(item.get("family", "") or "").strip().lower() == primary_family and primary_family else 1,
                    int(item.get("priority", 999) or 999),
                    0 if experimental and xau_opportunity_sidecar_active and family in ("xau_scalp_flow_short_sidecar", "xau_scalp_flow_long_sidecar") else 1,
                    family,
                )
            standard_candidates.sort(
                key=lambda item: _priority_sort_key(item, experimental=False)
            )
            experimental_candidates.sort(
                key=lambda item: _priority_sort_key(item, experimental=True)
            )
            if xau_opportunity_sidecar_active and symbol_token == "XAUUSD":
                sidecar = next(
                    (
                        dict(item)
                        for item in list(experimental_candidates or [])
                        if str(item.get("family", "") or "").strip().lower() == "xau_scalp_flow_short_sidecar"
                    ),
                    {},
                )
                if sidecar:
                    remainder = [
                        dict(item)
                        for item in list(experimental_candidates or [])
                        if str(item.get("family", "") or "").strip().lower() != "xau_scalp_flow_short_sidecar"
                    ]
                    experimental_candidates = [sidecar] + remainder
            if swarm_support_all:
                return [*standard_candidates, *experimental_candidates]
            return standard_candidates[:standard_limit] + experimental_candidates[:experimental_limit]
        if symbol_token == "XAUUSD" and base_token == "scalp_xauusd":
            fallback_standard = []
            for family in ("xau_scalp_pullback_limit", "xau_scalp_breakout_stop"):
                if family in allowed_families and _strategy_lab_family_allowed(family):
                    fallback_standard.append(
                        {
                            "symbol": "XAUUSD",
                            "family": family,
                            "strategy_id": f"{family}_v1",
                            "priority": 99,
                            "execution_ready": True,
                        }
                    )
            fallback_standard.sort(
                key=lambda item: (
                    0 if str(item.get("family", "") or "").strip().lower() in budget_production_families else 1 if str(item.get("family", "") or "").strip().lower() in budget_sampling_families else 2,
                    -float(budget_live_edge_scores.get(str(item.get("family", "") or "").strip().lower(), 0.0) or 0.0),
                    0 if _effective_strategy_lab_mode(str(item.get("strategy_id") or ""), str(item.get("family") or "")) == "promotable" else 1 if _effective_strategy_lab_mode(str(item.get("strategy_id") or ""), str(item.get("family") or "")) == "live_shadow" else 2 if _effective_strategy_lab_mode(str(item.get("strategy_id") or ""), str(item.get("family") or "")) == "recovery" else 3,
                    0 if float(strategy_lab_execution_priority.get(str(item.get("family", "") or "").strip().lower(), 0.0) or 0.0) > 0.0 else 1,
                    -float(strategy_lab_execution_priority.get(str(item.get("family", "") or "").strip().lower(), 0.0) or 0.0),
                    0 if float(manager_opportunity_priority.get(str(item.get("family", "") or "").strip().lower(), 0.0) or 0.0) > 0.0 else 1,
                    -float(manager_opportunity_priority.get(str(item.get("family", "") or "").strip().lower(), 0.0) or 0.0),
                    0 if str(item.get("family", "") or "").strip().lower() == primary_family and primary_family else 1,
                    int(item.get("priority", 999) or 999),
                )
            )
            fallback_experimental = []
            if "xau_scalp_tick_depth_filter" in experimental_families and _strategy_lab_family_allowed("xau_scalp_tick_depth_filter"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_tick_depth_filter",
                        "strategy_id": "xau_scalp_tick_depth_filter_v1",
                        "priority": 199,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_microtrend_follow_up" in experimental_families and _strategy_lab_family_allowed("xau_scalp_microtrend_follow_up"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_microtrend_follow_up",
                        "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                        "priority": 189,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_flow_short_sidecar" in experimental_families and _strategy_lab_family_allowed("xau_scalp_flow_short_sidecar"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_flow_short_sidecar",
                        "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                        "priority": 179,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_flow_long_sidecar" in experimental_families and _strategy_lab_family_allowed("xau_scalp_flow_long_sidecar"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_flow_long_sidecar",
                        "strategy_id": "xau_scalp_flow_long_sidecar_v1",
                        "priority": 178,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "xau_scalp_failed_fade_follow_stop" in experimental_families and _strategy_lab_family_allowed("xau_scalp_failed_fade_follow_stop"):
                fallback_experimental.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_failed_fade_follow_stop",
                        "strategy_id": "xau_scalp_failed_fade_follow_stop_v1",
                        "priority": 169,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if xau_opportunity_sidecar_active:
                sidecar = next(
                    (
                        dict(item)
                        for item in list(fallback_experimental or [])
                        if str(item.get("family", "") or "").strip().lower() == "xau_scalp_flow_short_sidecar"
                    ),
                    {},
                )
                if sidecar:
                    fallback_experimental = [
                        sidecar,
                        *[
                            dict(item)
                            for item in list(fallback_experimental or [])
                            if str(item.get("family", "") or "").strip().lower() != "xau_scalp_flow_short_sidecar"
                        ],
                    ]
            if swarm_support_all:
                return [*fallback_standard, *fallback_experimental]
            return fallback_standard[:standard_limit] + fallback_experimental[:experimental_limit]
        if symbol_token == "BTCUSD" and base_token == "scalp_btcusd":
            fallback_standard = []
            if "btc_weekday_lob_momentum" in allowed_families and _strategy_lab_family_allowed("btc_weekday_lob_momentum"):
                fallback_standard.append(
                    {
                        "symbol": "BTCUSD",
                        "family": "btc_weekday_lob_momentum",
                        "strategy_id": "btcusd_weekday_lob_momentum_v1",
                        "priority": 99,
                        "execution_ready": True,
                    }
                )
            fallback_experimental = []
            if "btc_weekday_lob_momentum" in experimental_families and _strategy_lab_family_allowed("btc_weekday_lob_momentum"):
                fallback_experimental.append(
                    {
                        "symbol": "BTCUSD",
                        "family": "btc_weekday_lob_momentum",
                        "strategy_id": "btcusd_weekday_lob_momentum_v1",
                        "priority": 199,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            if "btc_scalp_flow_short_sidecar" in experimental_families and _strategy_lab_family_allowed("btc_scalp_flow_short_sidecar"):
                if bool(getattr(config, "BTC_FSS_ENABLED", True)):
                    fallback_experimental.append(
                        {
                            "symbol": "BTCUSD",
                            "family": "btc_scalp_flow_short_sidecar",
                            "strategy_id": "btcusd_flow_short_sidecar_v1",
                            "priority": 188,
                            "execution_ready": True,
                            "experimental": True,
                        }
                    )
            if "btc_scalp_flow_long_sidecar" in experimental_families and _strategy_lab_family_allowed("btc_scalp_flow_long_sidecar"):
                if bool(getattr(config, "BTC_FLS_ENABLED", True)):
                    fallback_experimental.append(
                        {
                            "symbol": "BTCUSD",
                            "family": "btc_scalp_flow_long_sidecar",
                            "strategy_id": "btcusd_flow_long_sidecar_v1",
                            "priority": 187,
                            "execution_ready": True,
                            "experimental": True,
                        }
                    )
            if "btc_scalp_range_repair" in experimental_families and _strategy_lab_family_allowed("btc_scalp_range_repair"):
                if bool(getattr(config, "BTC_RANGE_REPAIR_ENABLED", True)):
                    fallback_experimental.append(
                        {
                            "symbol": "BTCUSD",
                            "family": "btc_scalp_range_repair",
                            "strategy_id": "btcusd_range_repair_v1",
                            "priority": 186,
                            "execution_ready": True,
                            "experimental": True,
                        }
                    )
            for _cf in ("crypto_flow_short", "crypto_flow_buy", "crypto_winner_confirmed", "crypto_behavioral_retest"):
                if _cf in experimental_families and _strategy_lab_family_allowed(_cf):
                    _cf_enabled_key = _cf.upper().replace("CRYPTO_", "CRYPTO_") + "_ENABLED"
                    _cf_enabled_key = {"crypto_flow_short": "CRYPTO_FLOW_SHORT_ENABLED", "crypto_flow_buy": "CRYPTO_FLOW_BUY_ENABLED", "crypto_winner_confirmed": "CRYPTO_WINNER_CONFIRMED_ENABLED", "crypto_behavioral_retest": "CRYPTO_BEHAVIORAL_RETEST_ENABLED"}.get(_cf, "")
                    if bool(getattr(config, _cf_enabled_key, False)):
                        _allowed_sym = set(getattr(config, f"get_{_cf}_allowed_symbols", lambda: set())() or set())
                        if not _allowed_sym or "BTCUSD" in _allowed_sym:
                            fallback_experimental.append({"symbol": "BTCUSD", "family": _cf, "strategy_id": f"btcusd_{_cf}_v1", "priority": 199, "execution_ready": True, "experimental": True})
            return (
                fallback_standard[: max(0, int(getattr(config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 2) or 2))]
                + fallback_experimental[: max(0, int(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 1) or 1))]
            )
        if symbol_token == "ETHUSD" and base_token == "scalp_ethusd":
            fallback_standard = []
            if "eth_weekday_overlap_probe" in allowed_families and _strategy_lab_family_allowed("eth_weekday_overlap_probe"):
                fallback_standard.append(
                    {
                        "symbol": "ETHUSD",
                        "family": "eth_weekday_overlap_probe",
                        "strategy_id": "ethusd_weekday_overlap_probe_v1",
                        "priority": 99,
                        "execution_ready": True,
                    }
                )
            fallback_experimental = []
            if "eth_weekday_overlap_probe" in experimental_families and _strategy_lab_family_allowed("eth_weekday_overlap_probe"):
                fallback_experimental.append(
                    {
                        "symbol": "ETHUSD",
                        "family": "eth_weekday_overlap_probe",
                        "strategy_id": "ethusd_weekday_overlap_probe_v1",
                        "priority": 199,
                        "execution_ready": True,
                        "experimental": True,
                    }
                )
            for _cf in ("crypto_flow_short", "crypto_flow_buy", "crypto_behavioral_retest"):
                if _cf in experimental_families and _strategy_lab_family_allowed(_cf):
                    _cf_enabled_key = {"crypto_flow_short": "CRYPTO_FLOW_SHORT_ENABLED", "crypto_flow_buy": "CRYPTO_FLOW_BUY_ENABLED", "crypto_behavioral_retest": "CRYPTO_BEHAVIORAL_RETEST_ENABLED"}.get(_cf, "")
                    if bool(getattr(config, _cf_enabled_key, False)):
                        _allowed_sym = set(getattr(config, f"get_{_cf}_allowed_symbols", lambda: set())() or set())
                        if not _allowed_sym or "ETHUSD" in _allowed_sym:
                            fallback_experimental.append({"symbol": "ETHUSD", "family": _cf, "strategy_id": f"ethusd_{_cf}_v1", "priority": 199, "execution_ready": True, "experimental": True})
            return (
                fallback_standard[: max(0, int(getattr(config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 2) or 2))]
                + fallback_experimental[: max(0, int(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 1) or 1))]
            )
        return []

    def _load_xau_pb_narrow_contexts(self) -> list[dict]:
        if not bool(getattr(config, "XAU_PB_NARROW_CONTEXT_ENABLED", False)):
            return []
        report_path = Path(__file__).resolve().parent / "data" / "reports" / "winner_memory_library_report.json"
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        except Exception:
            payload = {}
        rows: list[dict] = []
        min_resolved = max(1, int(getattr(config, "XAU_PB_NARROW_CONTEXT_MIN_RESOLVED", 3) or 3))
        min_score = float(getattr(config, "XAU_PB_NARROW_CONTEXT_MIN_MEMORY_SCORE", 20.0) or 20.0)
        max_rows = max(1, int(getattr(config, "XAU_PB_NARROW_CONTEXT_MAX_ROWS", 8) or 8))
        for row in list((payload.get("situations") if isinstance(payload, dict) else []) or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != "XAUUSD":
                continue
            if str(row.get("family") or "").strip().lower() != "xau_scalp_pullback_limit":
                continue
            if not bool(row.get("market_beating")):
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_resolved:
                continue
            if float(row.get("memory_score", 0.0) or 0.0) < min_score:
                continue
            rows.append(
                {
                    "direction": str(row.get("direction") or "").strip().lower(),
                    "session": self._normalized_signature(str(row.get("session") or "")),
                    "timeframe": self._signal_timeframe_token(type("Obj", (), {"timeframe": str(row.get("timeframe") or "")})()),
                    "entry_type": str(row.get("entry_type") or "").strip().lower(),
                    "confidence_band": str(row.get("confidence_band") or "").strip(),
                    "h1_trend": str(row.get("h1_trend") or "").strip().lower() or "unknown",
                    "memory_score": float(row.get("memory_score", 0.0) or 0.0),
                    "resolved": int(stats.get("resolved", 0) or 0),
                }
            )
        rows.sort(key=lambda item: (float(item.get("memory_score", 0.0)), int(item.get("resolved", 0))), reverse=True)
        return rows[:max_rows]

    def _signal_matches_xau_pb_narrow_context(self, signal) -> tuple[bool, dict]:
        contexts = self._load_xau_pb_narrow_contexts()
        if not contexts:
            return False, {}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        session_sig = self._signal_session_signature(signal)
        timeframe_token = self._signal_timeframe_token(signal)
        entry_type = str(getattr(signal, "entry_type", "") or "").strip().lower() or "limit"
        conf_band = self._signal_confidence_band(signal)
        h1_trend = self._signal_h1_trend_token(signal)
        for ctx in contexts:
            if str(ctx.get("direction") or "") != direction:
                continue
            if str(ctx.get("entry_type") or "") and str(ctx.get("entry_type") or "") != entry_type:
                continue
            ctx_conf = str(ctx.get("confidence_band") or "")
            relaxed_conf_match = False
            if ctx_conf and ctx_conf != conf_band:
                if (
                    bool(getattr(config, "XAU_PB_NARROW_CONTEXT_ALLOW_ADJACENT_CONFIDENCE", True))
                    and float(ctx.get("memory_score", 0.0) or 0.0) >= float(getattr(config, "XAU_PB_NARROW_CONTEXT_RELAXED_MIN_MEMORY_SCORE", 28.0) or 28.0)
                    and self._confidence_band_adjacent(ctx_conf, conf_band)
                ):
                    relaxed_conf_match = True
                else:
                    continue
            ctx_tf = str(ctx.get("timeframe") or "")
            if ctx_tf and not self._timeframe_matches(timeframe_token, {ctx_tf}):
                continue
            ctx_session = str(ctx.get("session") or "")
            if ctx_session and not self._session_signature_matches(session_sig, {ctx_session}):
                continue
            ctx_h1 = str(ctx.get("h1_trend") or "unknown").strip().lower()
            if ctx_h1 not in {"", "unknown"} and h1_trend not in {"", "unknown"} and ctx_h1 != h1_trend:
                continue
            out = dict(ctx)
            if relaxed_conf_match:
                out["relaxed_confidence_band"] = True
                out["requested_confidence_band"] = conf_band
            return True, out
        return False, {}

    def _pb_capture_micro_relax(self, signal) -> dict:
        if not bool(getattr(config, "XAU_PB_CAPTURE_MICRO_RELAX_ENABLED", True)):
            return {}
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=str(getattr(signal, "symbol", "") or ""),
                    lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                    direction=str(getattr(signal, "direction", "") or "").strip().lower(),
                    confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return {}
        gate = dict(snapshot.get("gate") or {})
        if bool(gate.get("pass")) or bool(gate.get("canary_sample_pass")):
            return {}
        features = dict(gate.get("features") or {})
        if str(features.get("day_type") or "").strip().lower() == "panic_spread":
            return {}
        allowed = {
            str(x or "").strip()
            for x in str(getattr(config, "XAU_PB_CAPTURE_MICRO_RELAX_ALLOWED_REASONS", "") or "").split(",")
            if str(x or "").strip()
        }
        reasons = [str(x or "").strip() for x in list(gate.get("reasons") or []) if str(x or "").strip()]
        if not reasons or not allowed or any(reason not in allowed for reason in reasons):
            return {}
        return {
            "snapshot": snapshot,
            "gate": gate,
            "risk_multiplier": float(getattr(config, "XAU_PB_CAPTURE_MICRO_RELAX_RISK_MULT", 0.88) or 0.88),
            "reasons": reasons,
        }

    def _pb_capture_falling_knife_guard(self, signal) -> dict:
        if not bool(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True)):
            return {}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return {}
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=str(getattr(signal, "symbol", "") or ""),
                    lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                    direction=direction,
                    confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return {}
        gate = dict(snapshot.get("gate") or {})
        features = dict(gate.get("features") or snapshot.get("features") or {})
        if not features:
            return {}
        chart_state = dict(
            live_profile_classify_chart_state(
                direction,
                self._signal_request_context(signal),
                capture_features=features,
            )
            or {}
        )
        day_type = str(chart_state.get("day_type") or features.get("day_type") or "trend").strip().lower() or "trend"
        state_label = str(chart_state.get("state_label") or "").strip().lower()
        blocked_day_types = {
            str(token or "").strip().lower()
            for token in str(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_DAY_TYPES", "repricing,fast_expansion,panic_spread") or "").split(",")
            if str(token or "").strip()
        }
        blocked_states = {
            str(token or "").strip().lower()
            for token in str(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_STATE_LABELS", "failed_fade_risk,panic_dislocation") or "").split(",")
            if str(token or "").strip()
        }
        sign = 1.0 if direction == "long" else -1.0
        delta_proxy = float(features.get("delta_proxy", 0.0) or 0.0)
        refill_shift = float(features.get("depth_refill_shift", 0.0) or 0.0)
        rejection_ratio = float(features.get("rejection_ratio", 0.0) or 0.0)
        bar_volume_proxy = float(features.get("bar_volume_proxy", 0.0) or 0.0)
        adverse_delta = (sign * delta_proxy) <= -abs(float(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_DELTA_PROXY", 0.09) or 0.09))
        adverse_refill = (sign * refill_shift) <= -abs(float(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_REFILL_SHIFT", 0.04) or 0.04))
        high_volume = bar_volume_proxy >= float(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_BAR_VOLUME_PROXY", 0.42) or 0.42)
        low_rejection = rejection_ratio <= float(getattr(config, "XAU_PB_FALLING_KNIFE_BLOCK_MAX_REJECTION_RATIO", 0.18) or 0.18)
        state_block = state_label in blocked_states
        flow_block = day_type in blocked_day_types and adverse_delta and adverse_refill and high_volume and low_rejection
        # Sharpness-based supplementary knife detection
        sharpness_block = False
        pb_sharpness: dict = {}
        if bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            try:
                from analysis.entry_sharpness import compute_entry_sharpness_score as _compute_sharpness
                pb_sharpness = _compute_sharpness(features, direction, micro_vol_scale=float(getattr(config, "XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE", 0.025) or 0.025), max_spread_expansion=float(getattr(config, "XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION", 1.20) or 1.20))
                sharpness_block = int(pb_sharpness.get("sharpness_score", 50) or 50) < max(1, int(getattr(config, "XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD", 35) or 35))
            except Exception:
                pass
        if not state_block and not flow_block and not sharpness_block:
            return {}
        reasons: list[str] = []
        if sharpness_block:
            reasons.append(f"sharpness_knife:{pb_sharpness.get('sharpness_score', 0)}")
        if state_block:
            reasons.append(f"state:{state_label}")
        if flow_block:
            reasons.append(f"flow:{day_type}")
        reasons.extend([str(item or "").strip() for item in list(gate.get("reasons") or []) if str(item or "").strip()][:3])
        return {
            "blocked": True,
            "reason": ",".join(reasons),
            "snapshot": {
                "run_id": str(snapshot.get("run_id") or ""),
                "last_event_utc": str(snapshot.get("last_event_utc") or ""),
            },
            "chart_state": {
                "state_label": state_label,
                "day_type": day_type,
                "continuation_bias": float(chart_state.get("continuation_bias", 0.0) or 0.0),
            },
            "features": {
                "delta_proxy": round(delta_proxy, 4),
                "depth_refill_shift": round(refill_shift, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
            },
            "sharpness": dict(pb_sharpness) if pb_sharpness else {},
            "gate_reasons": [str(item or "").strip() for item in list(gate.get("reasons") or []) if str(item or "").strip()],
        }

    def _xau_openapi_entry_router(self, signal, *, family: str, preferred_entry_type: str, snapshot: dict | None = None) -> dict:
        if not bool(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True)):
            return {}
        if str(getattr(signal, "symbol", "") or "").strip().upper() != "XAUUSD":
            return {}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return {}
        snap = dict(snapshot or {})
        if not snap:
            try:
                snap = dict(
                    live_profile_autopilot.latest_capture_feature_snapshot(
                        symbol=str(getattr(signal, "symbol", "") or ""),
                        lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                        direction=direction,
                        confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                    )
                    or {}
                )
            except Exception:
                snap = {}
        if not bool(snap.get("ok")) or not bool(snap.get("run_id")):
            return {}
        gate = dict(snap.get("gate") or {})
        features = dict(gate.get("features") or snap.get("features") or {})
        if not features:
            return {}
        chart_state = dict(
            live_profile_classify_chart_state(
                direction,
                self._signal_request_context(signal),
                capture_features=features,
            )
            or {}
        )
        state_label = str(chart_state.get("state_label") or "").strip().lower()
        day_type = str(chart_state.get("day_type") or features.get("day_type") or "trend").strip().lower() or "trend"
        sign = 1.0 if direction == "long" else -1.0
        # ── Entry Sharpness Score (deep data analytics) ──────────────────
        sharpness_result: dict = {}
        sharpness_score: int = 50
        sharpness_band: str = "normal"
        _sharpness_has_data = bool(features.get("spots_count") or (features.get("delta_proxy") is not None and features.get("bar_volume_proxy") is not None))
        if _sharpness_has_data and bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            try:
                from analysis.entry_sharpness import compute_entry_sharpness_score as _compute_sharpness
                sharpness_result = _compute_sharpness(features, direction, weights={"momentum": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_MOMENTUM", 1.0) or 1.0), "flow": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_FLOW", 1.0) or 1.0), "absorption": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_ABSORPTION", 1.0) or 1.0), "stability": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_STABILITY", 1.0) or 1.0), "positioning": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_POSITIONING", 1.0) or 1.0)}, micro_vol_scale=float(getattr(config, "XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE", 0.025) or 0.025), max_spread_expansion=float(getattr(config, "XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION", 1.20) or 1.20))
                sharpness_score = int(sharpness_result.get("sharpness_score", 50) or 50)
                sharpness_band = str(sharpness_result.get("sharpness_band", "normal") or "normal")
            except Exception:
                pass
        spread_avg_pct = float(features.get("spread_avg_pct", 0.0) or 0.0)
        spread_expansion = float(features.get("spread_expansion", 1.0) or 1.0)
        delta_proxy = float(features.get("delta_proxy", 0.0) or 0.0)
        imbalance = float(features.get("depth_imbalance", 0.0) or 0.0)
        refill_shift = float(features.get("depth_refill_shift", 0.0) or 0.0)
        rejection_ratio = float(features.get("rejection_ratio", 0.0) or 0.0)
        bar_volume_proxy = float(features.get("bar_volume_proxy", 0.0) or 0.0)
        tick_up_ratio = float(features.get("tick_up_ratio", 0.0) or 0.0)
        aligned_delta = sign * delta_proxy
        aligned_imbalance = sign * imbalance
        aligned_refill = sign * refill_shift
        hostile_day_types = self._parse_lower_csv(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_DAY_TYPES", "panic_spread"))
        hostile_states = self._parse_lower_csv(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_STATE_LABELS", "failed_fade_risk,panic_dislocation"))
        stop_states = self._parse_lower_csv(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_STATE_LABELS", "continuation_drive,breakout_drive"))
        limit_states = self._parse_lower_csv(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STATE_LABELS", "pullback_absorption,repricing_transition,reversal_exhaustion"))
        max_spread_pct = float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_PCT", 0.0023) or 0.0023)
        max_spread_expansion = float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_MAX_SPREAD_EXPANSION", 1.13) or 1.13)
        continuation_score = 0
        continuation_reasons: list[str] = []
        if aligned_delta >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_DELTA_PROXY", 0.08) or 0.08):
            continuation_score += 1
            continuation_reasons.append("delta")
        if aligned_imbalance >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_IMBALANCE", 0.025) or 0.025):
            continuation_score += 1
            continuation_reasons.append("imbalance")
        if aligned_refill >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_ALIGNED_REFILL_SHIFT", 0.02) or 0.02):
            continuation_score += 1
            continuation_reasons.append("refill")
        if rejection_ratio <= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MAX_REJECTION_RATIO", 0.24) or 0.24):
            continuation_score += 1
            continuation_reasons.append("low_rejection")
        if bar_volume_proxy >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_BAR_VOLUME_PROXY", 0.40) or 0.40):
            continuation_score += 1
            continuation_reasons.append("volume")
        if spread_avg_pct <= max_spread_pct:
            continuation_score += 1
            continuation_reasons.append("spread_pct")
        if spread_expansion <= max_spread_expansion:
            continuation_score += 1
            continuation_reasons.append("spread_stable")
        if state_label in stop_states:
            continuation_score += 1
            continuation_reasons.append(f"state:{state_label}")
        absorption_score = 0
        absorption_reasons: list[str] = []
        if rejection_ratio >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_REJECTION_RATIO", 0.26) or 0.26):
            absorption_score += 1
            absorption_reasons.append("rejection")
        if aligned_delta <= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_ALIGNED_DELTA_PROXY", 0.07) or 0.07):
            absorption_score += 1
            absorption_reasons.append("delta_not_overextended")
        if aligned_refill >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_ALIGNED_REFILL_SHIFT", -0.02) or -0.02):
            absorption_score += 1
            absorption_reasons.append("refill_holding")
        if bar_volume_proxy >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_BAR_VOLUME_PROXY", 0.20) or 0.20):
            absorption_score += 1
            absorption_reasons.append("volume")
        if spread_expansion <= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MAX_SPREAD_EXPANSION", 1.08) or 1.08):
            absorption_score += 1
            absorption_reasons.append("spread_stable")
        if spread_avg_pct <= max_spread_pct:
            absorption_score += 1
            absorption_reasons.append("spread_pct")
        if state_label in limit_states:
            absorption_score += 1
            absorption_reasons.append(f"state:{state_label}")
        # Sharpness-based knife block (composite deep analytics)
        sharpness_knife = bool(sharpness_band == "knife" and sharpness_score < max(1, int(getattr(config, "XAU_ENTRY_SHARPNESS_KNIFE_THRESHOLD", 30) or 30)))
        hostile_flow = bool(
            sharpness_knife
            or day_type in hostile_day_types
            or state_label in hostile_states
            or spread_avg_pct > (max_spread_pct * 1.12)
            or spread_expansion > (max_spread_expansion * 1.08)
            or (
                aligned_delta <= -abs(float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_DELTA_PROXY", 0.09) or 0.09))
                and aligned_refill <= -abs(float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_REFILL_SHIFT", 0.05) or 0.05))
                and rejection_ratio <= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MAX_REJECTION_RATIO", 0.18) or 0.18)
                and bar_volume_proxy >= float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_BAR_VOLUME_PROXY", 0.40) or 0.40)
            )
        )
        preferred = str(preferred_entry_type or "limit").strip().lower() or "limit"
        next_entry_type = preferred
        mode = "keep_preferred"
        reasons: list[str] = []
        pull_scale = 1.0
        trigger_scale = 1.0
        risk_multiplier = 1.0
        if hostile_flow:
            if sharpness_knife:
                reasons.append(f"sharpness_knife:{sharpness_score}")
            if day_type in hostile_day_types:
                reasons.append(f"day_type:{day_type}")
            if state_label in hostile_states:
                reasons.append(f"state:{state_label}")
            if spread_avg_pct > (max_spread_pct * 1.12):
                reasons.append("spread_too_wide")
            if spread_expansion > (max_spread_expansion * 1.08):
                reasons.append("spread_expanding")
            if (
                aligned_delta <= -abs(float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_DELTA_PROXY", 0.09) or 0.09))
                and aligned_refill <= -abs(float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_HOSTILE_MIN_ADVERSE_REFILL_SHIFT", 0.05) or 0.05))
            ):
                reasons.append("adverse_flow")
            return {
                "blocked": True,
                "family": family,
                "preferred_entry_type": preferred,
                "entry_type": preferred,
                "mode": "blocked",
                "reason": ",".join(reasons[:4]),
                "reasons": reasons[:4],
                "continuation_score": int(continuation_score),
                "absorption_score": int(absorption_score),
                "snapshot": {
                    "run_id": str(snap.get("run_id") or ""),
                    "last_event_utc": str(snap.get("last_event_utc") or ""),
                },
                "chart_state": {
                    "state_label": state_label,
                    "day_type": day_type,
                    "continuation_bias": float(chart_state.get("continuation_bias", 0.0) or 0.0),
                },
                "features": {
                    "spread_avg_pct": round(spread_avg_pct, 6),
                    "spread_expansion": round(spread_expansion, 4),
                    "delta_proxy": round(delta_proxy, 4),
                    "depth_imbalance": round(imbalance, 4),
                    "depth_refill_shift": round(refill_shift, 4),
                    "rejection_ratio": round(rejection_ratio, 4),
                    "bar_volume_proxy": round(bar_volume_proxy, 4),
                    "tick_up_ratio": round(tick_up_ratio, 4),
                },
                "sharpness": dict(sharpness_result) if sharpness_result else {},
            }
        stop_min_score = max(1, int(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_MIN_SCORE", 5) or 5))
        limit_min_score = max(1, int(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_MIN_SCORE", 4) or 4))
        stop_target = "buy_stop" if direction == "long" else "sell_stop"
        if preferred == "limit":
            if continuation_score >= stop_min_score:
                next_entry_type = stop_target
                mode = "promote_to_stop"
                reasons = continuation_reasons[:5]
            elif continuation_score >= max(1, stop_min_score - 1):
                mode = "shallow_limit"
                pull_scale = max(0.50, float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_SHALLOW_LIMIT_SCALE", 0.78) or 0.78))
                reasons = continuation_reasons[:5]
            elif absorption_score >= limit_min_score:
                if aligned_delta < 0:
                    mode = "deep_limit"
                    pull_scale = max(1.0, float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_DEEP_LIMIT_SCALE", 1.18) or 1.18))
                else:
                    mode = "shallow_limit"
                    pull_scale = max(0.50, float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_SHALLOW_LIMIT_SCALE", 0.78) or 0.78))
                reasons = absorption_reasons[:5]
        elif preferred in {"buy_stop", "sell_stop"}:
            if absorption_score >= limit_min_score and continuation_score < stop_min_score:
                next_entry_type = "limit"
                mode = "downgrade_to_limit"
                risk_multiplier = max(0.25, float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RISK_MULTIPLIER", 0.88) or 0.88))
                reasons = absorption_reasons[:5]
            elif continuation_score >= stop_min_score:
                mode = "fast_stop"
                trigger_scale = max(0.50, float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_FAST_STOP_TRIGGER_SCALE", 0.82) or 0.82))
                reasons = continuation_reasons[:5]
        # ── Sharpness-based adjustments (caution / sharp) ────────────────
        if sharpness_band == "caution" and bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            if next_entry_type in {"buy_stop", "sell_stop"}:
                next_entry_type = "limit"
                mode = "sharpness_downgrade_to_limit"
                reasons = list(sharpness_result.get("sharpness_reasons") or [])[:4]
            risk_multiplier *= max(0.25, float(getattr(config, "XAU_ENTRY_SHARPNESS_CAUTION_RISK_MULT", 0.75) or 0.75))
        elif sharpness_band == "sharp" and bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            sharp_min_cont = max(1, int(getattr(config, "XAU_ENTRY_SHARPNESS_SHARP_PROMOTE_MIN_CONT_SCORE", 4) or 4))
            if next_entry_type == "limit" and continuation_score >= sharp_min_cont:
                next_entry_type = stop_target
                mode = "sharpness_promote_to_stop"
                reasons = list(sharpness_result.get("sharpness_reasons") or [])[:4] + continuation_reasons[:2]
        return {
            "blocked": False,
            "family": family,
            "preferred_entry_type": preferred,
            "entry_type": next_entry_type,
            "mode": mode,
            "reason": ",".join(reasons[:4]),
            "reasons": reasons[:5],
            "continuation_score": int(continuation_score),
            "absorption_score": int(absorption_score),
            "pull_scale": round(float(pull_scale), 4),
            "trigger_scale": round(float(trigger_scale), 4),
            "risk_multiplier": round(float(risk_multiplier), 4),
            "snapshot": {
                "run_id": str(snap.get("run_id") or ""),
                "last_event_utc": str(snap.get("last_event_utc") or ""),
            },
            "chart_state": {
                "state_label": state_label,
                "day_type": day_type,
                "continuation_bias": float(chart_state.get("continuation_bias", 0.0) or 0.0),
            },
            "features": {
                "spread_avg_pct": round(spread_avg_pct, 6),
                "spread_expansion": round(spread_expansion, 4),
                "delta_proxy": round(delta_proxy, 4),
                "depth_imbalance": round(imbalance, 4),
                "depth_refill_shift": round(refill_shift, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "tick_up_ratio": round(tick_up_ratio, 4),
            },
            "sharpness": dict(sharpness_result) if sharpness_result else {},
        }

    def _load_xau_microtrend_follow_up_contexts(self) -> list[dict]:
        if not bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_ENABLED", False)):
            return []
        report_path = Path(__file__).resolve().parent / "data" / "reports" / "chart_state_memory_report.json"
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        except Exception:
            payload = {}
        rows: list[dict] = []
        min_resolved = max(1, int(getattr(config, "XAU_MICROTREND_FOLLOW_UP_MIN_RESOLVED", 3) or 3))
        min_state_score = float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_MIN_STATE_SCORE", 18.0) or 18.0)
        max_rows = max(1, int(getattr(config, "XAU_MICROTREND_FOLLOW_UP_MAX_ROWS", 6) or 6))
        for row in list((payload.get("states") if isinstance(payload, dict) else []) or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != "XAUUSD":
                continue
            if not bool(row.get("follow_up_candidate")):
                continue
            if str(((row.get("best_family") or {}).get("family") or "")).strip().lower() != "xau_scalp_microtrend":
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_resolved:
                continue
            if float(row.get("state_score", 0.0) or 0.0) < min_state_score:
                continue
            rows.append(
                {
                    "direction": str(row.get("direction") or "").strip().lower(),
                    "session": self._normalized_signature(str(row.get("session") or "")),
                    "timeframe": self._signal_timeframe_token(type("Obj", (), {"timeframe": str(row.get("timeframe") or "")})()),
                    "confidence_band": str(row.get("confidence_band") or "").strip(),
                    "h1_trend": str(row.get("h1_trend") or "").strip().lower() or "unknown",
                    "day_type": str(row.get("day_type") or "").strip().lower() or "trend",
                    "state_label": str(row.get("state_label") or "").strip().lower(),
                    "follow_up_plan": str(row.get("follow_up_plan") or "").strip().lower(),
                    "state_score": float(row.get("state_score", 0.0) or 0.0),
                    "resolved": int(stats.get("resolved", 0) or 0),
                }
            )
        rows.sort(key=lambda item: (float(item.get("state_score", 0.0)), int(item.get("resolved", 0))), reverse=True)
        return rows[:max_rows]

    def _load_xau_flow_short_sidecar_contexts(self) -> list[dict]:
        if not bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", False)):
            return []
        report_path = Path(__file__).resolve().parent / "data" / "reports" / "chart_state_memory_report.json"
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        except Exception:
            payload = {}
        allowed_sessions = {
            self._normalized_signature(token)
            for token in str(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ALLOWED_SESSIONS", "new_york,london,new_york,overlap") or "").split("|")
            if self._normalized_signature(token)
        }
        allowed_patterns = self._xau_flow_short_allowed_pattern_tokens()
        min_resolved = max(1, int(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MIN_RESOLVED", 3) or 3))
        min_state_score = float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MIN_STATE_SCORE", 20.0) or 20.0)
        max_rows = max(1, int(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MAX_ROWS", 6) or 6))
        rows: list[dict] = []
        for row in list((payload.get("states") if isinstance(payload, dict) else []) or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != "XAUUSD":
                continue
            if not bool(row.get("follow_up_candidate")):
                continue
            if str(row.get("direction") or "").strip().lower() != "short":
                continue
            state_label = str(row.get("state_label") or "").strip().lower()
            if state_label not in {"continuation_drive", "repricing_transition"}:
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_resolved:
                continue
            if float(row.get("state_score", 0.0) or 0.0) < min_state_score:
                continue
            session = self._normalized_signature(str(row.get("session") or ""))
            if allowed_sessions and session not in allowed_sessions:
                continue
            pattern_families = {str(k or "").strip() for k in dict(row.get("pattern_families") or {}).keys() if str(k or "").strip()}
            if not self._xau_flow_short_state_pattern_matches(pattern_families, allowed_patterns):
                continue
            rows.append(
                {
                    "direction": "short",
                    "session": session,
                    "timeframe": self._signal_timeframe_token(type("Obj", (), {"timeframe": str(row.get("timeframe") or "")})()),
                    "confidence_band": str(row.get("confidence_band") or "").strip(),
                    "h1_trend": str(row.get("h1_trend") or "").strip().lower() or "unknown",
                    "day_type": str(row.get("day_type") or "").strip().lower() or "trend",
                    "state_label": state_label,
                    "follow_up_plan": str(row.get("follow_up_plan") or "").strip().lower(),
                    "state_score": float(row.get("state_score", 0.0) or 0.0),
                    "continuation_bias": float(row.get("continuation_bias", 0.0) or 0.0),
                    "resolved": int(stats.get("resolved", 0) or 0),
                    "best_family": str(((row.get("best_family") or {}).get("family") or "")).strip().lower(),
                }
            )
        rows.sort(key=lambda item: (float(item.get("state_score", 0.0)), int(item.get("resolved", 0))), reverse=True)
        return rows[:max_rows]

    def _xau_flow_short_allowed_pattern_tokens(self) -> set[str]:
        tokens = {
            str(token or "").strip().upper()
            for token in str(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ALLOWED_PATTERNS", "SCALP_FLOW_FORCE") or "").split(",")
            if str(token or "").strip()
        }
        expanded = set(tokens)
        for token in list(tokens):
            if token.startswith("SCALP_FLOW"):
                expanded.add("SCALP_FLOW")
        return expanded

    def _xau_flow_short_state_pattern_matches(self, pattern_families: set[str], allowed_patterns: set[str]) -> bool:
        if not allowed_patterns:
            return True
        normalized = {str(item or "").strip().lower() for item in pattern_families if str(item or "").strip()}
        if not normalized:
            return False
        for token in allowed_patterns:
            token_l = str(token or "").strip().lower()
            if token_l in normalized:
                return True
            if token_l.startswith("scalp_flow") and "scalp_flow" in normalized:
                return True
        return False

    def _xau_flow_short_signal_pattern_matches(self, signal_pattern: str, allowed_patterns: set[str]) -> bool:
        if not allowed_patterns:
            return True
        signal_token = str(signal_pattern or "").strip().upper()
        if not signal_token:
            return False
        if signal_token in allowed_patterns:
            return True
        if any(token in signal_token for token in allowed_patterns):
            return True
        if "SCALP_FLOW_FORCE" in signal_token and "SCALP_FLOW" in allowed_patterns:
            return True
        if {"SCALP_FLOW", "SCALP_FLOW_FORCE"} & allowed_patterns:
            if "LIQUIDITY CONTINUATION" in signal_token and any(
                token in signal_token
                for token in ("SWEEP-RETEST", "SWEEP RETEST", "SWEEP_RETEST", "BEHAVIORAL SWEEP")
            ):
                return True
        return False

    def _signal_request_context(self, signal) -> dict:
        return {
            "session": self._signal_session_signature(signal),
            "timeframe": self._signal_timeframe_token(signal),
            "entry_type": str(getattr(signal, "entry_type", "") or "").strip().lower() or "unknown",
            "pattern": str(getattr(signal, "pattern", "") or "").strip() or "unknown",
            "raw_scores": dict(getattr(signal, "raw_scores", {}) or {}),
            "payload": {
                "session": self._signal_session_signature(signal),
                "timeframe": self._signal_timeframe_token(signal),
                "entry_type": str(getattr(signal, "entry_type", "") or "").strip().lower() or "unknown",
                "pattern": str(getattr(signal, "pattern", "") or "").strip() or "unknown",
                "raw_scores": dict(getattr(signal, "raw_scores", {}) or {}),
            },
            "root": {},
        }

    @staticmethod
    def _xau_follow_up_day_type_compatible(expected: str, observed: str) -> bool:
        exp = str(expected or "").strip().lower()
        obs = str(observed or "").strip().lower()
        if not exp or not obs or exp == obs:
            return True
        compatible = {
            "trend": {"repricing", "fast_expansion"},
            "repricing": {"trend", "fast_expansion"},
            "fast_expansion": {"trend", "repricing"},
        }
        return obs in compatible.get(exp, set())

    @staticmethod
    def _parse_lower_csv(raw: str) -> set[str]:
        return {str(part or "").strip().lower() for part in str(raw or "").split(",") if str(part or "").strip()}

    def _match_xau_microtrend_follow_up_first_sample(
        self,
        *,
        contexts: list[dict],
        direction: str,
        session_sig: str,
        timeframe_token: str,
        conf_band: str,
        h1_trend: str,
        state_label: str,
        day_type: str,
        snapshot: dict,
        chart_state: dict,
    ) -> tuple[bool, dict]:
        if not bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MODE_ENABLED", False)):
            return False, {}
        allowed_states = self._parse_lower_csv(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_ALLOWED_STATES", ""))
        if allowed_states and state_label not in allowed_states:
            return False, {}
        min_resolved = max(1, int(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_RESOLVED", 3) or 3))
        min_state_score = float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_STATE_SCORE", 32.0) or 32.0)
        max_relaxed = max(0, int(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MAX_RELAXED_BLOCKERS", 2) or 2))
        best_ctx: dict = {}
        best_blockers: list[str] = []
        best_score = float("-inf")
        for ctx in list(contexts or []):
            if str(ctx.get("direction") or "") != direction:
                continue
            if int(ctx.get("resolved", 0) or 0) < min_resolved:
                continue
            ctx_score = float(ctx.get("state_score", 0.0) or 0.0)
            if ctx_score < min_state_score:
                continue
            if str(ctx.get("state_label") or "").strip().lower() != state_label:
                continue
            ctx_tf = str(ctx.get("timeframe") or "")
            if ctx_tf and not self._timeframe_matches(timeframe_token, {ctx_tf}):
                continue
            ctx_session = str(ctx.get("session") or "")
            if ctx_session and not self._session_signature_matches(session_sig, {ctx_session}):
                continue
            blockers: list[str] = []
            ctx_conf = str(ctx.get("confidence_band") or "")
            if ctx_conf and ctx_conf != conf_band:
                blockers.append("confidence_band")
            ctx_h1 = str(ctx.get("h1_trend") or "unknown").strip().lower()
            if ctx_h1 not in {"", "unknown"} and h1_trend not in {"", "unknown"} and ctx_h1 != h1_trend:
                blockers.append("h1_trend")
            ctx_day = str(ctx.get("day_type") or "").strip().lower()
            if ctx_day and ctx_day != day_type:
                if self._xau_follow_up_day_type_compatible(ctx_day, day_type):
                    blockers.append("day_type")
                else:
                    continue
            if len(blockers) > max_relaxed:
                continue
            if (ctx_score, int(ctx.get("resolved", 0) or 0)) > (best_score, int(best_ctx.get("resolved", 0) or 0)):
                best_ctx = dict(ctx)
                best_blockers = list(blockers)
                best_score = ctx_score
        if not best_ctx:
            return False, {}
        out = dict(best_ctx)
        out["chart_state"] = chart_state
        out["snapshot"] = snapshot
        out["first_sample_mode"] = True
        out["first_sample_relaxed_blockers"] = list(best_blockers)
        if "confidence_band" in best_blockers:
            out["requested_confidence_band"] = conf_band
        if "h1_trend" in best_blockers:
            out["requested_h1_trend"] = h1_trend
        if "day_type" in best_blockers:
            out["requested_day_type"] = day_type
        return True, out

    def _signal_matches_xau_microtrend_follow_up_context(self, signal) -> tuple[bool, dict]:
        contexts = self._load_xau_microtrend_follow_up_contexts()
        if not contexts:
            return False, {}
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        session_sig = self._signal_session_signature(signal)
        timeframe_token = self._signal_timeframe_token(signal)
        conf_band = self._signal_confidence_band(signal)
        h1_trend = self._signal_h1_trend_token(signal)
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=str(getattr(signal, "symbol", "") or ""),
                    lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                    direction=direction,
                    confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return False, {}
        capture_features = dict(snapshot.get("features") or (snapshot.get("gate") or {}).get("features") or {})
        chart_state = dict(
            live_profile_classify_chart_state(
                direction,
                self._signal_request_context(signal),
                capture_features=capture_features,
            )
            or {}
        )
        state_label = str(chart_state.get("state_label") or "").strip().lower()
        day_type = str(chart_state.get("day_type") or "").strip().lower()
        relaxed_min_state_score = float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_RELAXED_MIN_STATE_SCORE", 28.0) or 28.0)
        for ctx in contexts:
            if str(ctx.get("direction") or "") != direction:
                continue
            ctx_tf = str(ctx.get("timeframe") or "")
            if ctx_tf and not self._timeframe_matches(timeframe_token, {ctx_tf}):
                continue
            ctx_session = str(ctx.get("session") or "")
            if ctx_session and not self._session_signature_matches(session_sig, {ctx_session}):
                continue
            ctx_conf = str(ctx.get("confidence_band") or "")
            relaxed_conf_match = False
            if ctx_conf and ctx_conf != conf_band:
                if (
                    bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_ADJACENT_CONFIDENCE", True))
                    and self._confidence_band_adjacent(ctx_conf, conf_band)
                ):
                    relaxed_conf_match = True
                else:
                    continue
            ctx_h1 = str(ctx.get("h1_trend") or "unknown").strip().lower()
            relaxed_h1_match = False
            if ctx_h1 not in {"", "unknown"} and h1_trend not in {"", "unknown"} and ctx_h1 != h1_trend:
                if (
                    bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_H1_RELAXED", True))
                    and float(ctx.get("state_score", 0.0) or 0.0) >= relaxed_min_state_score
                    and state_label == str(ctx.get("state_label") or "").strip().lower()
                ):
                    relaxed_h1_match = True
                else:
                    continue
            if str(ctx.get("state_label") or "") and str(ctx.get("state_label") or "") != state_label:
                continue
            relaxed_day_type = False
            ctx_day_type = str(ctx.get("day_type") or "").strip().lower()
            if ctx_day_type and ctx_day_type != day_type:
                if (
                    bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_COMPATIBLE_DAY_TYPE", True))
                    and float(ctx.get("state_score", 0.0) or 0.0) >= relaxed_min_state_score
                    and self._xau_follow_up_day_type_compatible(ctx_day_type, day_type)
                ):
                    relaxed_day_type = True
                else:
                    continue
            out = dict(ctx)
            out["chart_state"] = chart_state
            out["snapshot"] = snapshot
            if relaxed_conf_match:
                out["relaxed_confidence_band"] = True
                out["requested_confidence_band"] = conf_band
            if relaxed_h1_match:
                out["relaxed_h1_trend"] = True
                out["requested_h1_trend"] = h1_trend
            if relaxed_day_type:
                out["relaxed_day_type"] = True
                out["requested_day_type"] = day_type
            return True, out
        return self._match_xau_microtrend_follow_up_first_sample(
            contexts=contexts,
            direction=direction,
            session_sig=session_sig,
            timeframe_token=timeframe_token,
            conf_band=conf_band,
            h1_trend=h1_trend,
            state_label=state_label,
            day_type=day_type,
            snapshot=snapshot,
            chart_state=chart_state,
        )

    def _apply_xau_scheduled_canary_market_to_limit_retest(self, lane_signal, *, lane_source: str) -> object:
        if lane_signal is None:
            return lane_signal
        token = str(lane_source or "").strip().lower()
        if token != "xauusd_scheduled:canary":
            return lane_signal
        if not bool(getattr(config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_RETEST_ENABLED", True)):
            return lane_signal
        entry_type = str(getattr(lane_signal, "entry_type", "") or "").strip().lower()
        if entry_type != "market":
            return lane_signal
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return lane_signal
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        tp1 = float(getattr(lane_signal, "take_profit_1", 0.0) or 0.0)
        tp2 = float(getattr(lane_signal, "take_profit_2", 0.0) or 0.0)
        tp3 = float(getattr(lane_signal, "take_profit_3", 0.0) or 0.0)
        if entry <= 0 or stop_loss <= 0:
            return lane_signal
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return lane_signal
        sign = 1.0 if direction == "long" else -1.0
        pull_ratio = max(0.05, float(getattr(config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_PULLBACK_RISK_RATIO", 0.20) or 0.20))
        min_offset_pct = max(0.0, float(getattr(config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_MIN_OFFSET_PCT", 0.00035) or 0.00035))
        retest = max(risk * pull_ratio, abs(entry) * min_offset_pct)
        new_entry = entry - (retest * sign)
        if direction == "long" and not (stop_loss < new_entry):
            return lane_signal
        if direction == "short" and not (stop_loss > new_entry):
            return lane_signal
        primary_tp = tp1 if tp1 > 0 else (tp2 if tp2 > 0 else tp3)
        if primary_tp > 0:
            if direction == "long" and not (new_entry < primary_tp):
                return lane_signal
            if direction == "short" and not (new_entry > primary_tp):
                return lane_signal
        lane_signal.entry = round(float(new_entry), 4)
        lane_signal.entry_type = "limit"
        if primary_tp > 0:
            reward = abs(primary_tp - new_entry)
            lane_signal.risk_reward = round(float(reward / max(abs(new_entry - stop_loss), 1e-9)), 2)
        try:
            raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
            raw["scheduled_canary_market_to_limit_retest"] = True
            raw["scheduled_canary_entry_before"] = round(float(entry), 4)
            raw["scheduled_canary_entry_after"] = round(float(new_entry), 4)
            raw["scheduled_canary_retest_offset"] = round(float(retest), 4)
            lane_signal.raw_scores = raw
        except Exception:
            pass
        return lane_signal

    @staticmethod
    def _signal_rr_triplet(signal) -> tuple[float, float, float]:
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        risk = abs(entry - stop)
        if risk <= 0:
            return 0.85, max(1.0, float(getattr(signal, "risk_reward", 1.24) or 1.24)), 1.8
        out: list[float] = []
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        sign = 1.0 if direction == "long" else -1.0
        for field, fallback in (("take_profit_1", 0.85), ("take_profit_2", max(1.0, float(getattr(signal, "risk_reward", 1.24) or 1.24))), ("take_profit_3", 1.8)):
            tp = float(getattr(signal, field, 0.0) or 0.0)
            rr = fallback
            if tp > 0:
                rr_raw = ((tp - entry) * sign) / max(risk, 1e-9)
                if rr_raw > 0:
                    rr = float(rr_raw)
            out.append(max(0.35, float(rr)))
        return float(out[0]), float(out[1]), float(out[2])

    def _apply_family_price_plan(self, lane_signal, *, family: str, entry: float, stop_loss: float, entry_type: str) -> object | None:
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return None
        rr1, rr2, rr3 = self._signal_rr_triplet(lane_signal)
        risk = abs(float(entry) - float(stop_loss))
        if risk <= 0:
            return None
        if direction == "long":
            tp1 = float(entry) + (risk * rr1)
            tp2 = float(entry) + (risk * rr2)
            tp3 = float(entry) + (risk * rr3)
            if not (float(stop_loss) < float(entry) < float(tp2)):
                return None
        else:
            tp1 = float(entry) - (risk * rr1)
            tp2 = float(entry) - (risk * rr2)
            tp3 = float(entry) - (risk * rr3)
            if not (float(stop_loss) > float(entry) > float(tp2)):
                return None
        lane_signal.entry = round(float(entry), 4)
        lane_signal.stop_loss = round(float(stop_loss), 4)
        lane_signal.take_profit_1 = round(float(tp1), 4)
        lane_signal.take_profit_2 = round(float(tp2), 4)
        lane_signal.take_profit_3 = round(float(tp3), 4)
        lane_signal.risk_reward = round(float(rr2), 2)
        lane_signal.entry_type = str(entry_type or "market").strip().lower()
        lane_signal.pattern = f"{str(getattr(lane_signal, 'pattern', '') or '').strip()}|{str(family or '').upper()}"[:80]
        return lane_signal

    def _apply_xau_scheduled_canary_rr_rebalance(self, lane_signal, *, lane_source: str) -> object:
        if lane_signal is None:
            return lane_signal
        token = str(lane_source or "").strip().lower()
        if token != "xauusd_scheduled:canary":
            return lane_signal
        if not bool(getattr(config, "CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED", True)):
            return lane_signal
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return lane_signal
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        tp1 = float(getattr(lane_signal, "take_profit_1", 0.0) or 0.0)
        tp2 = float(getattr(lane_signal, "take_profit_2", 0.0) or 0.0)
        tp3 = float(getattr(lane_signal, "take_profit_3", 0.0) or 0.0)
        if entry <= 0 or stop_loss <= 0:
            return lane_signal
        risk = abs(entry - stop_loss)
        primary_tp = tp1 if tp1 > 0 else (tp2 if tp2 > 0 else tp3)
        reward = abs(primary_tp - entry) if primary_tp > 0 else 0.0
        if risk <= 0 or reward <= 0:
            return lane_signal
        current_rr = reward / max(risk, 1e-9)
        min_rr = max(0.25, float(getattr(config, "CTRADER_SCHEDULED_CANARY_MIN_RR", 0.85) or 0.85))
        if current_rr >= min_rr:
            return lane_signal
        keep_ratio = min(0.95, max(0.20, float(getattr(config, "CTRADER_SCHEDULED_CANARY_MIN_STOP_KEEP_RATIO", 0.58) or 0.58)))
        target_risk = max(reward / max(min_rr, 1e-9), risk * keep_ratio)
        target_risk = min(risk, target_risk)
        if target_risk >= (risk - max(abs(entry) * 0.000001, 0.01)):
            return lane_signal
        new_stop = entry - target_risk if direction == "long" else entry + target_risk
        if direction == "long" and not (new_stop < entry):
            return lane_signal
        if direction == "short" and not (new_stop > entry):
            return lane_signal
        lane_signal.stop_loss = round(float(new_stop), 4)
        rr_display = abs(primary_tp - entry) / max(target_risk, 1e-9)
        lane_signal.risk_reward = round(float(rr_display), 2)
        try:
            raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
            raw["scheduled_canary_rr_rebalanced"] = True
            raw["scheduled_canary_rr_before"] = round(float(current_rr), 4)
            raw["scheduled_canary_rr_after"] = round(float(rr_display), 4)
            raw["scheduled_canary_stop_loss_before"] = round(float(stop_loss), 4)
            raw["scheduled_canary_stop_loss_after"] = round(float(new_stop), 4)
            raw["scheduled_canary_target_risk"] = round(float(target_risk), 4)
            raw["scheduled_canary_primary_tp"] = round(float(primary_tp), 4)
            lane_signal.raw_scores = raw
        except Exception:
            pass
        return lane_signal

    def _build_family_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None:
            return None, ""
        xau_mtf_guard = self._xau_multi_tf_entry_guard(signal, family=family) if family.startswith("xau_scalp_") else {}
        if bool(xau_mtf_guard.get("blocked")):
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
                raw["xau_multi_tf_guard_block"] = True
                raw["xau_multi_tf_guard_reason"] = str(xau_mtf_guard.get("reason") or "")
                raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                signal.raw_scores = raw
            except Exception:
                pass
            return None, ""
        if family == "xau_scalp_tick_depth_filter":
            lane_signal, lane_source = self._build_tick_depth_filter_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family == "xau_scalp_microtrend_follow_up":
            lane_signal, lane_source = self._build_xau_microtrend_follow_up_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family == "xau_scalp_flow_short_sidecar":
            lane_signal, lane_source = self._build_xau_flow_short_sidecar_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family == "xau_scalp_flow_long_sidecar":
            lane_signal, lane_source = self._build_xau_flow_long_sidecar_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family == "xau_scalp_range_repair":
            lane_signal, lane_source = self._build_xau_range_repair_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None:
                # Composite guard: block only when np < 0.50 AND MTF blocked simultaneously
                # BT result: catches exactly 1 trade (-$3.73 loser), zero winners blocked
                try:
                    _rr_np = float((dict(getattr(signal, "raw_scores", {}) or {})).get("neural_probability", 1.0) or 1.0)
                    _rr_mtf_blocked = xau_mtf_guard is not None and not bool((xau_mtf_guard or {}).get("allowed", True))
                    if _rr_np < 0.50 and _rr_mtf_blocked:
                        return None, ""
                except Exception:
                    pass
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family == "xau_scalp_prelondon_sweep_cont":
            lane_signal, lane_source = self._build_xau_psc_canary_signal(signal, base_source=base_source, candidate=candidate)
            if lane_signal is not None and xau_mtf_guard:
                try:
                    raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                    raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
                    lane_signal.raw_scores = raw
                except Exception:
                    pass
            return lane_signal, lane_source
        if family in {"btc_weekday_lob_momentum", "eth_weekday_overlap_probe"}:
            return self._build_crypto_weekday_experimental_signal(signal, base_source=base_source, candidate=candidate)
        if family == "btc_scalp_flow_short_sidecar":
            return self._build_btc_flow_short_sidecar_signal(signal, base_source=base_source, candidate=candidate)
        if family == "btc_scalp_flow_long_sidecar":
            return self._build_btc_flow_long_sidecar_signal(signal, base_source=base_source, candidate=candidate)
        if family == "btc_scalp_range_repair":
            return self._build_btc_range_repair_signal(signal, base_source=base_source, candidate=candidate)
        if family == "crypto_flow_short":
            return self._build_crypto_flow_short_signal(signal, base_source=base_source, candidate=candidate)
        if family == "crypto_flow_buy":
            return self._build_crypto_flow_buy_signal(signal, base_source=base_source, candidate=candidate)
        if family == "crypto_winner_confirmed":
            return self._build_crypto_winner_confirmed_signal(signal, base_source=base_source, candidate=candidate)
        if family == "crypto_behavioral_retest":
            return self._build_crypto_behavioral_retest_signal(signal, base_source=base_source, candidate=candidate)
        if family == "xau_scalp_failed_fade_follow_stop":
            # FF orders are spawned from cTrader follow-stop logic, not cloned here from the scanner signal.
            self._stamp_family_canary_skip(
                signal,
                family=family,
                stage="family_builder",
                reason="ff_follow_stop_executor_spawned_not_scheduler_clone",
            )
            return None, ""
        if family not in {"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}:
            return None, ""
        lane_signal = copy.deepcopy(signal)
        matched_context: dict = {}
        pb_flow_guard: dict = {}
        pb_capture_relax: dict = {}
        entry_router: dict = {}
        if family == "xau_scalp_pullback_limit" and bool(getattr(config, "XAU_PB_NARROW_CONTEXT_ENABLED", False)):
            matched, matched_context = self._signal_matches_xau_pb_narrow_context(lane_signal)
            if not matched:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="pattern_context", reason="pb_narrow_context_no_match"
                )
                return None, ""
            pb_flow_guard = self._pb_capture_falling_knife_guard(lane_signal)
            if bool(pb_flow_guard.get("blocked")):
                try:
                    raw = dict(getattr(signal, "raw_scores", {}) or {})
                    raw["pb_falling_knife_block"] = True
                    raw["pb_falling_knife_block_reason"] = str(pb_flow_guard.get("reason") or "")
                    raw["pb_falling_knife_block_snapshot"] = dict(pb_flow_guard.get("snapshot") or {})
                    raw["pb_falling_knife_block_chart_state"] = dict(pb_flow_guard.get("chart_state") or {})
                    raw["pb_falling_knife_block_features"] = dict(pb_flow_guard.get("features") or {})
                    raw["pb_falling_knife_block_sharpness"] = dict(pb_flow_guard.get("sharpness") or {})
                    signal.raw_scores = raw
                except Exception:
                    pass
                return None, ""
            pb_capture_relax = self._pb_capture_micro_relax(lane_signal)
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(lane_signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if direction not in {"long", "short"} or entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            self._stamp_family_canary_skip(
                signal, family=family, stage="signal_geometry", reason="pb_bs_invalid_entry_stop_risk"
            )
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0003)
        sign = 1.0 if direction == "long" else -1.0
        new_entry = float(entry)
        new_stop = float(stop_loss)
        entry_type = "limit"
        if family == "xau_scalp_pullback_limit":
            pull_dist = min(atr_eff * float(getattr(config, "XAU_PULLBACK_LIMIT_ENTRY_ATR", 0.12) or 0.12), base_risk * 0.65)
            pull_dist = max(base_risk * 0.18, pull_dist)
            stop_pad = min(atr_eff * float(getattr(config, "XAU_PULLBACK_LIMIT_STOP_PAD_ATR", 0.04) or 0.04), pull_dist * 0.45)
            new_entry = entry - (pull_dist * sign)
            new_stop = stop_loss - (stop_pad * sign)
            entry_type = "limit"
        elif family == "xau_scalp_breakout_stop":
            trigger = min(atr_eff * float(getattr(config, "XAU_BREAKOUT_STOP_TRIGGER_ATR", 0.10) or 0.10), base_risk * 0.55)
            trigger = max(base_risk * 0.12, trigger)
            stop_lift = trigger * float(getattr(config, "XAU_BREAKOUT_STOP_STOP_LIFT_RATIO", 0.45) or 0.45)
            new_entry = entry + (trigger * sign)
            new_stop = stop_loss + (stop_lift * sign)
            entry_type = "buy_stop" if direction == "long" else "sell_stop"
        entry_router = self._xau_openapi_entry_router(lane_signal, family=family, preferred_entry_type=entry_type)
        if bool(entry_router.get("blocked")):
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
                raw["xau_openapi_entry_router_block"] = True
                raw["xau_openapi_entry_router_block_reason"] = str(entry_router.get("reason") or "")
                raw["xau_openapi_entry_router"] = dict(entry_router)
                signal.raw_scores = raw
            except Exception:
                pass
            return None, ""
        route_entry_type = str(entry_router.get("entry_type") or entry_type).strip().lower() or entry_type
        route_mode = str(entry_router.get("mode") or "").strip().lower()
        if family == "xau_scalp_pullback_limit":
            if route_entry_type in {"buy_stop", "sell_stop"}:
                trigger = max(
                    base_risk * float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_TRIGGER_RISK_RATIO", 0.10) or 0.10),
                    atr_eff * 0.05,
                )
                trigger *= max(0.50, float(entry_router.get("trigger_scale", 1.0) or 1.0))
                stop_lift = trigger * float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_STOP_STOP_LIFT_RATIO", 0.32) or 0.32)
                new_entry = entry + (trigger * sign)
                new_stop = stop_loss + (stop_lift * sign)
                entry_type = route_entry_type
            elif route_mode in {"shallow_limit", "deep_limit"}:
                pull_scale = max(0.55, min(1.50, float(entry_router.get("pull_scale", 1.0) or 1.0)))
                pull_dist = max(base_risk * 0.14, pull_dist * pull_scale)
                stop_pad_scale = max(0.75, min(1.25, 0.92 + ((pull_scale - 1.0) * 0.35)))
                stop_pad = max(base_risk * 0.05, stop_pad * stop_pad_scale)
                new_entry = entry - (pull_dist * sign)
                new_stop = stop_loss - (stop_pad * sign)
        elif family == "xau_scalp_breakout_stop":
            if route_entry_type == "limit":
                retest = max(
                    base_risk * float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_RETEST_RISK_RATIO", 0.08) or 0.08),
                    atr_eff * 0.04,
                )
                stop_pad = retest * float(getattr(config, "XAU_OPENAPI_ENTRY_ROUTER_LIMIT_STOP_PAD_RATIO", 0.24) or 0.24)
                new_entry = entry - (retest * sign)
                new_stop = stop_loss - (stop_pad * sign)
                entry_type = "limit"
            elif route_mode == "fast_stop":
                trigger_scale = max(0.50, min(1.00, float(entry_router.get("trigger_scale", 1.0) or 1.0)))
                trigger = max(base_risk * 0.10, trigger * trigger_scale)
                stop_lift = max(base_risk * 0.04, stop_lift * max(0.72, min(1.08, 0.9 + ((trigger_scale - 1.0) * 0.30))))
                new_entry = entry + (trigger * sign)
                new_stop = stop_loss + (stop_lift * sign)
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type=entry_type)
        if shaped is None:
            self._stamp_family_canary_skip(
                signal, family=family, stage="price_plan", reason="pb_bs_apply_family_price_plan_returned_none"
            )
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_family"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["mt5_magic_offset"] = int(getattr(config, "PERSISTENT_CANARY_MT5_MAGIC_OFFSET", 700) or 0)
            raw["mt5_ignore_open_positions"] = True
            raw["mt5_extra_volume_multiplier"] = float(getattr(config, "PERSISTENT_CANARY_FAMILY_MT5_VOLUME_MULTIPLIER", 0.10) or 0.10)
            raw["ctrader_risk_usd_override"] = float(getattr(config, "PERSISTENT_CANARY_FAMILY_CTRADER_RISK_USD", 1.25) or 1.25)
            raw["mt5_limit_allow_market_fallback"] = False
            if matched_context:
                raw["pb_narrow_context"] = {
                    "session": str(matched_context.get("session") or ""),
                    "direction": str(matched_context.get("direction") or ""),
                    "timeframe": str(matched_context.get("timeframe") or ""),
                    "entry_type": str(matched_context.get("entry_type") or ""),
                    "confidence_band": str(matched_context.get("confidence_band") or ""),
                    "h1_trend": str(matched_context.get("h1_trend") or ""),
                    "memory_score": float(matched_context.get("memory_score", 0.0) or 0.0),
                }
                if bool(matched_context.get("relaxed_confidence_band")):
                    raw["pb_narrow_context"]["relaxed_confidence_band"] = True
                    raw["pb_narrow_context"]["requested_confidence_band"] = str(matched_context.get("requested_confidence_band") or "")
            if pb_capture_relax:
                raw["pb_capture_micro_relax"] = {
                    "risk_multiplier": float(pb_capture_relax.get("risk_multiplier", 1.0) or 1.0),
                    "reasons": list(pb_capture_relax.get("reasons") or []),
                    "snapshot": {
                        "run_id": str(((pb_capture_relax.get("snapshot") or {}).get("run_id") or "")),
                        "last_event_utc": str(((pb_capture_relax.get("snapshot") or {}).get("last_event_utc") or "")),
                    },
                }
                raw["ctrader_risk_usd_override"] = round(
                    max(0.1, float(raw.get("ctrader_risk_usd_override", 1.25) or 1.25) * float(pb_capture_relax.get("risk_multiplier", 1.0) or 1.0)),
                    4,
                )
            if entry_router:
                raw["xau_openapi_entry_router"] = {
                    "family": family,
                    "preferred_entry_type": str(entry_router.get("preferred_entry_type") or ""),
                    "selected_entry_type": str(entry_router.get("entry_type") or ""),
                    "mode": str(entry_router.get("mode") or ""),
                    "reason": str(entry_router.get("reason") or ""),
                    "reasons": list(entry_router.get("reasons") or []),
                    "continuation_score": int(entry_router.get("continuation_score", 0) or 0),
                    "absorption_score": int(entry_router.get("absorption_score", 0) or 0),
                    "snapshot": dict(entry_router.get("snapshot") or {}),
                    "chart_state": dict(entry_router.get("chart_state") or {}),
                    "features": dict(entry_router.get("features") or {}),
                    "sharpness": dict(entry_router.get("sharpness") or {}),
                }
                # Attach Volume Profile context if available
                if bool(getattr(config, "XAU_VOLUME_PROFILE_ENABLED", True)):
                    try:
                        _vp_sym = str(getattr(signal, "symbol", "XAUUSD") or "XAUUSD").strip().upper()
                        vp_report = dict(report_store.get_report(f"volume_profile_{_vp_sym.lower()}") or report_store.get_report("volume_profile") or {})
                        vp_data = dict(vp_report.get("vp") or {})
                        if vp_data.get("poc"):
                            from analysis.volume_profile import check_entry_vs_profile, get_tick_config
                            entry_price = float(getattr(signal, "entry", 0.0) or 0.0)
                            direction = str(getattr(signal, "direction", "") or "").strip().lower()
                            if entry_price > 0 and direction:
                                _tc = get_tick_config(_vp_sym)
                                vp_check = check_entry_vs_profile(entry_price, direction, vp_data, tick_size=float(_tc.get("tick_size", 0.01)), bucket_ticks=int(_tc.get("bucket_ticks", 10)))
                                raw["xau_openapi_entry_router"]["volume_profile"] = {"poc": float(vp_data.get("poc", 0) or 0), "va_high": float(vp_data.get("va_high", 0) or 0), "va_low": float(vp_data.get("va_low", 0) or 0), **vp_check}
                    except Exception:
                        pass
                router_risk_mult = max(0.25, float(entry_router.get("risk_multiplier", 1.0) or 1.0))
                if abs(router_risk_mult - 1.0) > 1e-9:
                    raw["ctrader_risk_usd_override"] = round(
                        max(0.1, float(raw.get("ctrader_risk_usd_override", 1.25) or 1.25) * router_risk_mult),
                        4,
                    )
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state=dict(entry_router.get("chart_state") or pb_flow_guard.get("chart_state") or {}),
                follow_up_plan=str(matched_context.get("follow_up_plan") or ""),
            )
            if xau_mtf_guard:
                raw["xau_multi_tf_guard"] = dict(xau_mtf_guard)
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    def _build_xau_flow_short_sidecar_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_flow_short_sidecar":
            return None, ""
        if not bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", False)):
            self._stamp_family_canary_skip(
                signal, family=family, stage="family_disabled", reason="XAU_FLOW_SHORT_SIDECAR_ENABLED=0"
            )
            return None, ""
        lane_signal = copy.deepcopy(signal)
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        if direction != "short":
            self._stamp_family_canary_skip(
                signal, family=family, stage="direction_gate", reason="fss_requires_short_direction"
            )
            return None, ""
        try:
            _raw_check = dict(getattr(lane_signal, "raw_scores", {}) or {})
            _behavioral_trigger = bool(_raw_check.get("behavioral_trigger"))
            _mtf_sell_hint = bool(
                bool(getattr(config, "SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED", True))
                and _raw_check.get("xau_fss_sell_routing_hint")
            )
            if _mtf_sell_hint:
                _behavioral_trigger = True
                _raw_check["xau_fss_sell_routed_by_mtf"] = True
                lane_signal.raw_scores = _raw_check
        except Exception:
            _behavioral_trigger = False
        contexts = self._load_xau_flow_short_sidecar_contexts()
        if not contexts and not _behavioral_trigger:
            self._stamp_family_canary_skip(
                signal,
                family=family,
                stage="pattern_context",
                reason="fss_no_chart_contexts_and_no_behavioral_bypass",
            )
            return None, ""
        allowed_patterns = self._xau_flow_short_allowed_pattern_tokens()
        pattern_ok = self._xau_flow_short_signal_pattern_matches(
            str(getattr(lane_signal, "pattern", "") or ""),
            allowed_patterns,
        )
        if not pattern_ok and not _behavioral_trigger:
            self._stamp_family_canary_skip(
                signal, family=family, stage="pattern_gate", reason="fss_pattern_token_not_allowed"
            )
            return None, ""
        session_sig = self._signal_session_signature(lane_signal)
        timeframe_token = self._signal_timeframe_token(lane_signal)
        conf_band = self._signal_confidence_band(lane_signal)
        h1_trend = self._signal_h1_trend_token(lane_signal)
        chart_state = live_profile_classify_chart_state(direction, self._signal_request_context(lane_signal), capture_features={})
        chart_day_type = str(chart_state.get("day_type") or "trend").strip().lower() or "trend"
        matched_context = {}
        first_sample_mode = False
        for ctx in contexts:
            if str(ctx.get("direction") or "") != direction:
                continue
            if str(ctx.get("session") or "") and not self._session_signature_matches(session_sig, {str(ctx.get("session") or "")}):
                continue
            if str(ctx.get("timeframe") or "") and not self._timeframe_matches(timeframe_token, {str(ctx.get("timeframe") or "")}):
                continue
            if str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"} and h1_trend not in {"", "unknown"} and str(ctx.get("h1_trend") or "") != h1_trend:
                continue
            ctx_conf = str(ctx.get("confidence_band") or "")
            relaxed_confidence_band = False
            if ctx_conf and ctx_conf != conf_band:
                if bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ALLOW_ADJACENT_CONFIDENCE", True)) and self._confidence_band_adjacent(ctx_conf, conf_band):
                    relaxed_confidence_band = True
                else:
                    continue
            requested_day_type = str(ctx.get("day_type") or "").strip().lower()
            relaxed_day_type = False
            if requested_day_type and requested_day_type != chart_day_type:
                if bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE", True)) and self._xau_follow_up_day_type_compatible(requested_day_type, chart_day_type):
                    relaxed_day_type = True
                else:
                    continue
            matched_context = dict(ctx)
            if relaxed_confidence_band:
                matched_context["relaxed_confidence_band"] = True
                matched_context["requested_confidence_band"] = conf_band
            if relaxed_day_type:
                matched_context["relaxed_day_type"] = True
                matched_context["requested_day_type"] = chart_day_type
            break
        if not matched_context and bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", True)):
            allowed_states = {
                str(token or "").strip().lower()
                for token in str(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition") or "").split(",")
                if str(token or "").strip()
            }
            min_state_score = float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", 34.0) or 34.0)
            min_confidence = float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0)
            signal_confidence = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            for ctx in contexts:
                if str(ctx.get("direction") or "") != direction:
                    continue
                if str(ctx.get("session") or "") and not self._session_signature_matches(session_sig, {str(ctx.get("session") or "")}):
                    continue
                if str(ctx.get("timeframe") or "") and not self._timeframe_matches(timeframe_token, {str(ctx.get("timeframe") or "")}):
                    continue
                if allowed_states and str(ctx.get("state_label") or "").strip().lower() not in allowed_states:
                    continue
                if float(ctx.get("state_score", 0.0) or 0.0) < min_state_score:
                    continue
                if signal_confidence < min_confidence:
                    continue
                ctx_conf = str(ctx.get("confidence_band") or "")
                high_confidence_bridge = False
                if ctx_conf and ctx_conf != conf_band and not self._confidence_band_adjacent(ctx_conf, conf_band):
                    high_confidence_bridge = bool(
                        bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", True))
                        and conf_band == "80+"
                        and ctx_conf == "75-79.9"
                        and signal_confidence >= max(min_confidence, 80.0)
                    )
                    if not high_confidence_bridge:
                        continue
                requested_day_type = str(ctx.get("day_type") or "").strip().lower()
                if requested_day_type and requested_day_type != chart_day_type and not self._xau_follow_up_day_type_compatible(requested_day_type, chart_day_type):
                    continue
                if (
                    str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"}
                    and h1_trend not in {"", "unknown"}
                    and str(ctx.get("h1_trend") or "") != h1_trend
                    and not bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED", True))
                ):
                    continue
                matched_context = dict(ctx)
                matched_context["first_sample_mode"] = True
                if ctx_conf and ctx_conf != conf_band:
                    matched_context["relaxed_confidence_band"] = True
                    matched_context["requested_confidence_band"] = conf_band
                    if high_confidence_bridge:
                        matched_context["high_confidence_bridge"] = True
                if requested_day_type and requested_day_type != chart_day_type:
                    matched_context["relaxed_day_type"] = True
                    matched_context["requested_day_type"] = chart_day_type
                if (
                    str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"}
                    and h1_trend not in {"", "unknown"}
                    and str(ctx.get("h1_trend") or "") != h1_trend
                ):
                    matched_context["relaxed_h1_trend"] = True
                    matched_context["requested_h1_trend"] = h1_trend
                first_sample_mode = True
                break
        # Priority #3: behavioral_trigger bypass — fire FSS even without chart_state context
        if not matched_context and _behavioral_trigger:
            signal_confidence = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            if signal_confidence >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0):
                matched_context = {
                    "direction": "short",
                    "state_label": "continuation_drive",
                    "day_type": chart_day_type,
                    "follow_up_plan": "break_stop_follow",
                    "state_score": 50.0,
                    "session": session_sig,
                    "timeframe": timeframe_token,
                    "confidence_band": conf_band,
                    "best_family": "fss",
                    "behavioral_trigger_bypass": True,
                    "first_sample_mode": True,
                }
                first_sample_mode = True
        if not matched_context and _behavioral_trigger:
            _fss_min_conf = float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0)
            _fss_sig_conf = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            if _fss_sig_conf < _fss_min_conf:
                self._stamp_family_canary_skip(
                    signal,
                    family=family,
                    stage="pattern_context",
                    reason=f"fss_behavioral_bypass_conf_below_min:{_fss_sig_conf:.1f}<{_fss_min_conf:.1f}",
                )
                return None, ""
        if not matched_context:
            self._stamp_family_canary_skip(
                signal, family=family, stage="pattern_context", reason="fss_context_no_match"
            )
            return None, ""
        snapshot = dict(
            live_profile_autopilot.latest_capture_feature_snapshot(
                symbol=str(getattr(lane_signal, "symbol", "") or ""),
                lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                direction=direction,
                confidence=float(getattr(lane_signal, "confidence", 0.0) or 0.0),
            )
            or {}
        )
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            self._stamp_family_canary_skip(
                signal, family=family, stage="feature_snapshot", reason="fss_tick_feature_snapshot_unavailable"
            )
            return None, ""
        capture_features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        continuation_bias = float(((matched_context.get("continuation_bias") or chart_state.get("continuation_bias") or 0.0) or 0.0))
        delta_proxy = float(capture_features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        if abs(continuation_bias) < 1e-9:
            # When chart-state memory lacks a stored continuation bias, use the live short-horizon
            # flow as a conservative proxy so FSS can sample real continuation setups.
            continuation_bias = max(abs(delta_proxy), abs(float(capture_features.get("depth_imbalance", 0.0) or 0.0)) * 0.5)
        # Guard B+C: behavioral_trigger bypass — require NEGATIVE delta_proxy (real selling flow)
        # positive delta_proxy = buyers dominating = macro recovery = end-of-short-trend → block FSS
        if _behavioral_trigger and delta_proxy >= 0:
            self._stamp_family_canary_skip(
                signal,
                family=family,
                stage="flow_guard",
                reason="fss_behavioral_requires_negative_delta_proxy",
            )
            return None, ""
        follow_plan = str(matched_context.get("follow_up_plan") or "").strip().lower()
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(lane_signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            self._stamp_family_canary_skip(
                signal, family=family, stage="signal_geometry", reason="fss_invalid_entry_stop_geometry"
            )
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0003)
        use_break_stop = bool(
            ("break_stop" in follow_plan or "follow" in follow_plan)
            and abs(continuation_bias) >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
            and abs(delta_proxy) >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
            and bar_volume_proxy >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
        )
        sample_mode = False
        if (
            not use_break_stop
            and bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True))
            and float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE", 72.0) or 72.0)
            and float(matched_context.get("state_score", 0.0) or 0.0)
            >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE", 32.0) or 32.0)
            and ("break_stop" in follow_plan or "follow" in follow_plan)
        ):
            use_break_stop = bool(
                abs(continuation_bias)
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
                * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT", 0.75) or 0.75)
                and abs(delta_proxy)
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
                * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_DELTA_PROXY_MULT", 0.75) or 0.75)
                and bar_volume_proxy
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
                * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT", 0.90) or 0.90)
            )
            sample_mode = bool(use_break_stop)
        if not use_break_stop and first_sample_mode and ("break_stop" in follow_plan or "follow" in follow_plan):
            # Guard A: behavioral_trigger bypass must pass FULL thresholds, never relaxed multipliers
            _fsm_cb = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT", 0.68) or 0.68)
            _fsm_dp = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT", 0.68) or 0.68)
            _fsm_bv = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT", 0.82) or 0.82)
            use_break_stop = bool(
                abs(continuation_bias)
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
                * _fsm_cb
                and abs(delta_proxy)
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
                * _fsm_dp
                and bar_volume_proxy
                >= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
                * _fsm_bv
            )
            sample_mode = bool(use_break_stop)
        if use_break_stop:
            trigger = max(
                base_risk * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO", 0.12) or 0.12),
                atr_eff * 0.05,
            )
            stop_lift = trigger * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO", 0.34) or 0.34)
            new_entry = entry - trigger
            new_stop = stop_loss - stop_lift
            next_entry_type = "sell_stop"
        else:
            if bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True)):
                return None, ""
            retest = max(
                base_risk * float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SHALLOW_RETEST_RISK_RATIO", 0.10) or 0.10),
                atr_eff * 0.045,
            )
            stop_pad = retest * 0.24
            new_entry = entry + retest
            new_stop = stop_loss + stop_pad
            next_entry_type = "limit"
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type=next_entry_type)
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_flow_short_sidecar"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["chart_state_flow_short_sidecar"] = {
                "state_label": str(matched_context.get("state_label") or ""),
                "day_type": str(matched_context.get("day_type") or ""),
                "follow_up_plan": follow_plan,
                "state_score": float(matched_context.get("state_score", 0.0) or 0.0),
                "session": str(matched_context.get("session") or ""),
                "timeframe": str(matched_context.get("timeframe") or ""),
                "direction": "short",
                "confidence_band": str(matched_context.get("confidence_band") or ""),
                "best_family": str(matched_context.get("best_family") or ""),
            }
            if bool(matched_context.get("behavioral_trigger_bypass")):
                raw["chart_state_flow_short_sidecar"]["behavioral_trigger_bypass"] = True
            if bool(matched_context.get("relaxed_confidence_band")):
                raw["chart_state_flow_short_sidecar"]["relaxed_confidence_band"] = True
            if bool(matched_context.get("relaxed_day_type")):
                raw["chart_state_flow_short_sidecar"]["relaxed_day_type"] = True
                raw["chart_state_flow_short_sidecar"]["requested_day_type"] = str(matched_context.get("requested_day_type") or "")
            if bool(matched_context.get("relaxed_h1_trend")):
                raw["chart_state_flow_short_sidecar"]["relaxed_h1_trend"] = True
                raw["chart_state_flow_short_sidecar"]["requested_h1_trend"] = str(matched_context.get("requested_h1_trend") or "")
            if bool(matched_context.get("first_sample_mode")):
                raw["chart_state_flow_short_sidecar"]["first_sample_mode"] = True
            if bool(matched_context.get("high_confidence_bridge")):
                raw["chart_state_flow_short_sidecar"]["high_confidence_bridge"] = True
            raw["chart_state_flow_short_snapshot"] = {
                "run_id": str(snapshot.get("run_id") or ""),
                "last_event_utc": str(snapshot.get("last_event_utc") or ""),
                "entry_mode": "break_stop_sample" if sample_mode else ("break_stop" if use_break_stop else "shallow_retest_limit"),
            }
            raw["mt5_ignore_open_positions"] = True
            risk_usd = float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_CTRADER_RISK_USD", 0.45) or 0.45)
            if sample_mode:
                risk_usd *= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_RISK_MULTIPLIER", 0.70) or 0.70)
                raw["chart_state_flow_short_sidecar"]["sample_mode"] = True
            if bool(matched_context.get("first_sample_mode")):
                risk_usd *= float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER", 0.55) or 0.55)
            raw["ctrader_risk_usd_override"] = round(risk_usd, 4)
            raw["mt5_limit_allow_market_fallback"] = False
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state={
                    "state_label": str(matched_context.get("state_label") or ""),
                    "day_type": str(matched_context.get("day_type") or ""),
                },
                follow_up_plan=follow_plan,
            )
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── FLS (Flow Long Sidecar) helpers ──────────────────────────────────────

    def _xau_flow_long_allowed_pattern_tokens(self) -> set[str]:
        tokens = {
            str(token or "").strip().upper()
            for token in str(getattr(config, "XAU_FLOW_LONG_SIDECAR_ALLOWED_PATTERNS", "SCALP_FLOW_FORCE") or "").split(",")
            if str(token or "").strip()
        }
        expanded = set(tokens)
        for token in list(tokens):
            if token.startswith("SCALP_FLOW"):
                expanded.add("SCALP_FLOW")
        return expanded

    def _xau_flow_long_state_pattern_matches(self, pattern_families: set[str], allowed_patterns: set[str]) -> bool:
        if not allowed_patterns:
            return True
        normalized = {str(item or "").strip().lower() for item in pattern_families if str(item or "").strip()}
        if not normalized:
            return False
        for token in allowed_patterns:
            token_l = str(token or "").strip().lower()
            if token_l in normalized:
                return True
            if token_l.startswith("scalp_flow") and "scalp_flow" in normalized:
                return True
        return False

    def _xau_flow_long_signal_pattern_matches(self, signal_pattern: str, allowed_patterns: set[str]) -> bool:
        if not allowed_patterns:
            return True
        signal_token = str(signal_pattern or "").strip().upper()
        if not signal_token:
            return False
        if signal_token in allowed_patterns:
            return True
        if any(token in signal_token for token in allowed_patterns):
            return True
        if "SCALP_FLOW_FORCE" in signal_token and "SCALP_FLOW" in allowed_patterns:
            return True
        if {"SCALP_FLOW", "SCALP_FLOW_FORCE"} & allowed_patterns:
            if "LIQUIDITY CONTINUATION" in signal_token and any(
                token in signal_token
                for token in ("SWEEP-RETEST", "SWEEP RETEST", "SWEEP_RETEST", "BEHAVIORAL SWEEP")
            ):
                return True
        return False

    def _load_xau_flow_long_sidecar_contexts(self) -> list[dict]:
        if not bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_ENABLED", False)):
            return []
        report_path = Path(__file__).resolve().parent / "data" / "reports" / "chart_state_memory_report.json"
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        except Exception:
            payload = {}
        allowed_sessions = {
            self._normalized_signature(token)
            for token in str(getattr(config, "XAU_FLOW_LONG_SIDECAR_ALLOWED_SESSIONS", "new_york|london|overlap") or "").split("|")
            if self._normalized_signature(token)
        }
        allowed_patterns = self._xau_flow_long_allowed_pattern_tokens()
        min_resolved = max(1, int(getattr(config, "XAU_FLOW_LONG_SIDECAR_MIN_RESOLVED", 3) or 3))
        min_state_score = float(getattr(config, "XAU_FLOW_LONG_SIDECAR_MIN_STATE_SCORE", 20.0) or 20.0)
        max_rows = max(1, int(getattr(config, "XAU_FLOW_LONG_SIDECAR_MAX_ROWS", 6) or 6))
        rows: list[dict] = []
        for row in list((payload.get("states") if isinstance(payload, dict) else []) or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != "XAUUSD":
                continue
            if not bool(row.get("follow_up_candidate")):
                continue
            if str(row.get("direction") or "").strip().lower() != "long":
                continue
            state_label = str(row.get("state_label") or "").strip().lower()
            if state_label not in {"continuation_drive", "repricing_transition"}:
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_resolved:
                continue
            if float(row.get("state_score", 0.0) or 0.0) < min_state_score:
                continue
            session = self._normalized_signature(str(row.get("session") or ""))
            if allowed_sessions and session not in allowed_sessions:
                continue
            pattern_families = {str(k or "").strip() for k in dict(row.get("pattern_families") or {}).keys() if str(k or "").strip()}
            if not self._xau_flow_long_state_pattern_matches(pattern_families, allowed_patterns):
                continue
            rows.append(
                {
                    "direction": "long",
                    "session": session,
                    "timeframe": self._signal_timeframe_token(type("Obj", (), {"timeframe": str(row.get("timeframe") or "")})()),
                    "confidence_band": str(row.get("confidence_band") or "").strip(),
                    "h1_trend": str(row.get("h1_trend") or "").strip().lower() or "unknown",
                    "day_type": str(row.get("day_type") or "").strip().lower() or "trend",
                    "state_label": state_label,
                    "state_score": float(row.get("state_score", 0.0) or 0.0),
                    "follow_up_plan": str(row.get("follow_up_plan") or "break_stop_follow").strip().lower(),
                    "follow_up_candidate": True,
                    "continuation_bias": float(row.get("continuation_bias", 0.0) or 0.0),
                    "best_family": str(row.get("best_family") or "").strip(),
                }
            )
            if len(rows) >= max_rows:
                break
        return rows

    def _build_xau_flow_long_sidecar_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_flow_long_sidecar":
            return None, ""
        if not bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_ENABLED", False)):
            self._stamp_family_canary_skip(
                signal, family=family, stage="family_disabled", reason="XAU_FLOW_LONG_SIDECAR_ENABLED=0"
            )
            return None, ""
        lane_signal = copy.deepcopy(signal)
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        if direction != "long":
            self._stamp_family_canary_skip(
                signal, family=family, stage="direction_gate", reason="fls_requires_long_direction"
            )
            return None, ""
        # Check if behavioral_trigger pre-qualifies this signal (Priority #3)
        try:
            _raw_check = dict(getattr(lane_signal, "raw_scores", {}) or {})
            _behavioral_trigger = bool(_raw_check.get("behavioral_trigger"))
        except Exception:
            _behavioral_trigger = False
        allowed_patterns = self._xau_flow_long_allowed_pattern_tokens()
        pattern_ok = self._xau_flow_long_signal_pattern_matches(
            str(getattr(lane_signal, "pattern", "") or ""),
            allowed_patterns,
        )
        if not pattern_ok and not _behavioral_trigger:
            self._stamp_family_canary_skip(
                signal, family=family, stage="pattern_gate", reason="fls_pattern_token_not_allowed"
            )
            return None, ""
        contexts = self._load_xau_flow_long_sidecar_contexts()
        session_sig = self._signal_session_signature(lane_signal)
        timeframe_token = self._signal_timeframe_token(lane_signal)
        conf_band = self._signal_confidence_band(lane_signal)
        h1_trend = self._signal_h1_trend_token(lane_signal)
        chart_state = live_profile_classify_chart_state(direction, self._signal_request_context(lane_signal), capture_features={})
        chart_day_type = str(chart_state.get("day_type") or "trend").strip().lower() or "trend"
        matched_context: dict = {}
        first_sample_mode = False
        for ctx in contexts:
            if str(ctx.get("direction") or "") != direction:
                continue
            if str(ctx.get("session") or "") and not self._session_signature_matches(session_sig, {str(ctx.get("session") or "")}):
                continue
            if str(ctx.get("timeframe") or "") and not self._timeframe_matches(timeframe_token, {str(ctx.get("timeframe") or "")}):
                continue
            if str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"} and h1_trend not in {"", "unknown"} and str(ctx.get("h1_trend") or "") != h1_trend:
                continue
            ctx_conf = str(ctx.get("confidence_band") or "")
            relaxed_confidence_band = False
            if ctx_conf and ctx_conf != conf_band:
                if bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_ALLOW_ADJACENT_CONFIDENCE", True)) and self._confidence_band_adjacent(ctx_conf, conf_band):
                    relaxed_confidence_band = True
                else:
                    continue
            requested_day_type = str(ctx.get("day_type") or "").strip().lower()
            relaxed_day_type = False
            if requested_day_type and requested_day_type != chart_day_type:
                if bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_ALLOW_COMPATIBLE_DAY_TYPE", True)) and self._xau_follow_up_day_type_compatible(requested_day_type, chart_day_type):
                    relaxed_day_type = True
                else:
                    continue
            matched_context = dict(ctx)
            if relaxed_confidence_band:
                matched_context["relaxed_confidence_band"] = True
                matched_context["requested_confidence_band"] = conf_band
            if relaxed_day_type:
                matched_context["relaxed_day_type"] = True
                matched_context["requested_day_type"] = chart_day_type
            break
        if not matched_context and bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", True)):
            allowed_states = {
                str(token or "").strip().lower()
                for token in str(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition") or "").split(",")
                if str(token or "").strip()
            }
            min_state_score = float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", 34.0) or 34.0)
            min_confidence = float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0)
            signal_confidence = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            for ctx in contexts:
                if str(ctx.get("direction") or "") != direction:
                    continue
                if str(ctx.get("session") or "") and not self._session_signature_matches(session_sig, {str(ctx.get("session") or "")}):
                    continue
                if str(ctx.get("timeframe") or "") and not self._timeframe_matches(timeframe_token, {str(ctx.get("timeframe") or "")}):
                    continue
                if allowed_states and str(ctx.get("state_label") or "").strip().lower() not in allowed_states:
                    continue
                if float(ctx.get("state_score", 0.0) or 0.0) < min_state_score:
                    continue
                if signal_confidence < min_confidence:
                    continue
                ctx_conf = str(ctx.get("confidence_band") or "")
                high_confidence_bridge = False
                if ctx_conf and ctx_conf != conf_band and not self._confidence_band_adjacent(ctx_conf, conf_band):
                    high_confidence_bridge = bool(
                        bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", True))
                        and conf_band == "80+"
                        and ctx_conf == "75-79.9"
                        and signal_confidence >= max(min_confidence, 80.0)
                    )
                    if not high_confidence_bridge:
                        continue
                requested_day_type = str(ctx.get("day_type") or "").strip().lower()
                if requested_day_type and requested_day_type != chart_day_type and not self._xau_follow_up_day_type_compatible(requested_day_type, chart_day_type):
                    continue
                if (
                    str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"}
                    and h1_trend not in {"", "unknown"}
                    and str(ctx.get("h1_trend") or "") != h1_trend
                    and not bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_ALLOW_H1_RELAXED", True))
                ):
                    continue
                matched_context = dict(ctx)
                matched_context["first_sample_mode"] = True
                if ctx_conf and ctx_conf != conf_band:
                    matched_context["relaxed_confidence_band"] = True
                    matched_context["requested_confidence_band"] = conf_band
                    if high_confidence_bridge:
                        matched_context["high_confidence_bridge"] = True
                if requested_day_type and requested_day_type != chart_day_type:
                    matched_context["relaxed_day_type"] = True
                    matched_context["requested_day_type"] = chart_day_type
                if (
                    str(ctx.get("h1_trend") or "unknown") not in {"", "unknown"}
                    and h1_trend not in {"", "unknown"}
                    and str(ctx.get("h1_trend") or "") != h1_trend
                ):
                    matched_context["relaxed_h1_trend"] = True
                    matched_context["requested_h1_trend"] = h1_trend
                first_sample_mode = True
                break
        # Priority #3: behavioral_trigger bypass — fire FLS even without chart_state context
        if not matched_context and _behavioral_trigger:
            signal_confidence = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            if signal_confidence >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0):
                matched_context = {
                    "direction": "long",
                    "state_label": "continuation_drive",
                    "day_type": chart_day_type,
                    "follow_up_plan": "break_stop_follow",
                    "state_score": 50.0,
                    "session": session_sig,
                    "timeframe": timeframe_token,
                    "confidence_band": conf_band,
                    "best_family": "fls",
                    "behavioral_trigger_bypass": True,
                    "first_sample_mode": True,
                }
                first_sample_mode = True
        if not matched_context and _behavioral_trigger:
            _fls_min_conf = float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 69.0) or 69.0)
            _fls_sig_conf = float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            if _fls_sig_conf < _fls_min_conf:
                self._stamp_family_canary_skip(
                    signal,
                    family=family,
                    stage="pattern_context",
                    reason=f"fls_behavioral_bypass_conf_below_min:{_fls_sig_conf:.1f}<{_fls_min_conf:.1f}",
                )
                return None, ""
        if not matched_context:
            self._stamp_family_canary_skip(
                signal,
                family=family,
                stage="pattern_context",
                reason="fls_no_chart_contexts_and_no_behavioral_bypass",
            )
            return None, ""
        snapshot = dict(
            live_profile_autopilot.latest_capture_feature_snapshot(
                symbol=str(getattr(lane_signal, "symbol", "") or ""),
                lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                direction=direction,
                confidence=float(getattr(lane_signal, "confidence", 0.0) or 0.0),
            )
            or {}
        )
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            self._stamp_family_canary_skip(
                signal, family=family, stage="feature_snapshot", reason="fls_tick_capture_snapshot_unavailable"
            )
            return None, ""
        capture_features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        continuation_bias = float(((matched_context.get("continuation_bias") or chart_state.get("continuation_bias") or 0.0) or 0.0))
        delta_proxy = float(capture_features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        if abs(continuation_bias) < 1e-9:
            continuation_bias = max(abs(delta_proxy), abs(float(capture_features.get("depth_imbalance", 0.0) or 0.0)) * 0.5)
        # Guard B+C: behavioral_trigger bypass — require POSITIVE delta_proxy (real buying flow)
        # negative/zero delta_proxy = sellers dominating = long exhaustion = end-of-long-trend → block FLS
        if _behavioral_trigger and delta_proxy <= 0:
            self._stamp_family_canary_skip(
                signal, family=family, stage="flow_guard", reason="fls_behavioral_requires_positive_delta_proxy"
            )
            return None, ""
        follow_plan = str(matched_context.get("follow_up_plan") or "").strip().lower()
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(lane_signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            self._stamp_family_canary_skip(
                signal, family=family, stage="signal_geometry", reason="fls_invalid_entry_stop_geometry"
            )
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0003)
        use_break_stop = bool(
            ("break_stop" in follow_plan or "follow" in follow_plan)
            and abs(continuation_bias) >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
            and abs(delta_proxy) >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
            and bar_volume_proxy >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
        )
        sample_mode = False
        if (
            not use_break_stop
            and bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_ENABLED", True))
            and float(getattr(lane_signal, "confidence", 0.0) or 0.0)
            >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_CONFIDENCE", 72.0) or 72.0)
            and float(matched_context.get("state_score", 0.0) or 0.0)
            >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_MIN_STATE_SCORE", 32.0) or 32.0)
            and ("break_stop" in follow_plan or "follow" in follow_plan)
        ):
            use_break_stop = bool(
                abs(continuation_bias)
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
                * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT", 0.75) or 0.75)
                and abs(delta_proxy)
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
                * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_DELTA_PROXY_MULT", 0.75) or 0.75)
                and bar_volume_proxy
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
                * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT", 0.90) or 0.90)
            )
            sample_mode = bool(use_break_stop)
        if not use_break_stop and first_sample_mode and ("break_stop" in follow_plan or "follow" in follow_plan):
            # Guard A: behavioral_trigger bypass must pass FULL thresholds, never relaxed multipliers
            _fsm_cb = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_CONTINUATION_BIAS_MULT", 0.68) or 0.68)
            _fsm_dp = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_DELTA_PROXY_MULT", 0.68) or 0.68)
            _fsm_bv = 1.0 if _behavioral_trigger else float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_BAR_VOLUME_PROXY_MULT", 0.82) or 0.82)
            use_break_stop = bool(
                abs(continuation_bias)
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10) or 0.10)
                * _fsm_cb
                and abs(delta_proxy)
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08) or 0.08)
                * _fsm_dp
                and bar_volume_proxy
                >= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38) or 0.38)
                * _fsm_bv
            )
            sample_mode = bool(use_break_stop)
        if use_break_stop:
            trigger = max(
                base_risk * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_TRIGGER_RISK_RATIO", 0.12) or 0.12),
                atr_eff * 0.05,
            )
            stop_lift = trigger * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_BREAK_STOP_STOP_LIFT_RATIO", 0.34) or 0.34)
            new_entry = entry + trigger
            new_stop = stop_loss + stop_lift
            next_entry_type = "buy_stop"
        else:
            if bool(getattr(config, "XAU_FLOW_LONG_SIDECAR_FORCE_STOP_ONLY", True)):
                self._stamp_family_canary_skip(
                    signal, family=family, stage="price_plan", reason="fls_force_stop_only_break_thresholds_not_met"
                )
                return None, ""
            retest = max(
                base_risk * float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SHALLOW_RETEST_RISK_RATIO", 0.10) or 0.10),
                atr_eff * 0.045,
            )
            stop_pad = retest * 0.24
            new_entry = entry - retest
            new_stop = stop_loss - stop_pad
            next_entry_type = "limit"
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type=next_entry_type)
        if shaped is None:
            self._stamp_family_canary_skip(
                signal, family=family, stage="price_plan", reason="fls_apply_family_price_plan_returned_none"
            )
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_flow_long_sidecar"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["chart_state_flow_long_sidecar"] = {
                "state_label": str(matched_context.get("state_label") or ""),
                "day_type": str(matched_context.get("day_type") or ""),
                "follow_up_plan": follow_plan,
                "state_score": float(matched_context.get("state_score", 0.0) or 0.0),
                "session": str(matched_context.get("session") or ""),
                "timeframe": str(matched_context.get("timeframe") or ""),
                "direction": "long",
                "confidence_band": str(matched_context.get("confidence_band") or ""),
                "best_family": str(matched_context.get("best_family") or ""),
            }
            if bool(matched_context.get("behavioral_trigger_bypass")):
                raw["chart_state_flow_long_sidecar"]["behavioral_trigger_bypass"] = True
            if bool(matched_context.get("relaxed_confidence_band")):
                raw["chart_state_flow_long_sidecar"]["relaxed_confidence_band"] = True
            if bool(matched_context.get("relaxed_day_type")):
                raw["chart_state_flow_long_sidecar"]["relaxed_day_type"] = True
                raw["chart_state_flow_long_sidecar"]["requested_day_type"] = str(matched_context.get("requested_day_type") or "")
            if bool(matched_context.get("relaxed_h1_trend")):
                raw["chart_state_flow_long_sidecar"]["relaxed_h1_trend"] = True
                raw["chart_state_flow_long_sidecar"]["requested_h1_trend"] = str(matched_context.get("requested_h1_trend") or "")
            if bool(matched_context.get("first_sample_mode")):
                raw["chart_state_flow_long_sidecar"]["first_sample_mode"] = True
            if bool(matched_context.get("high_confidence_bridge")):
                raw["chart_state_flow_long_sidecar"]["high_confidence_bridge"] = True
            raw["chart_state_flow_long_snapshot"] = {
                "run_id": str(snapshot.get("run_id") or ""),
                "last_event_utc": str(snapshot.get("last_event_utc") or ""),
                "entry_mode": "break_stop_sample" if sample_mode else ("break_stop" if use_break_stop else "shallow_retest_limit"),
            }
            raw["mt5_ignore_open_positions"] = True
            risk_usd = float(getattr(config, "XAU_FLOW_LONG_SIDECAR_CTRADER_RISK_USD", 0.45) or 0.45)
            if sample_mode:
                risk_usd *= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_SAMPLE_RISK_MULTIPLIER", 0.70) or 0.70)
                raw["chart_state_flow_long_sidecar"]["sample_mode"] = True
            if bool(matched_context.get("first_sample_mode")):
                risk_usd *= float(getattr(config, "XAU_FLOW_LONG_SIDECAR_FIRST_SAMPLE_RISK_MULTIPLIER", 0.55) or 0.55)
            raw["ctrader_risk_usd_override"] = round(risk_usd, 4)
            raw["mt5_limit_allow_market_fallback"] = False
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state={
                    "state_label": str(matched_context.get("state_label") or ""),
                    "day_type": str(matched_context.get("day_type") or ""),
                },
                follow_up_plan=follow_plan,
            )
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── end FLS ──────────────────────────────────────────────────────────────

    def _build_xau_microtrend_follow_up_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_microtrend_follow_up":
            return None, ""
        if not bool(getattr(config, "XAU_MICROTREND_FOLLOW_UP_ENABLED", False)):
            self._stamp_family_canary_skip(
                signal, family=family, stage="family_disabled", reason="XAU_MICROTREND_FOLLOW_UP_ENABLED=0"
            )
            return None, ""
        matched, matched_context = self._signal_matches_xau_microtrend_follow_up_context(signal)
        if not matched:
            self._stamp_family_canary_skip(
                signal, family=family, stage="pattern_context", reason="mfu_chart_state_context_no_match"
            )
            return None, ""
        lane_signal = copy.deepcopy(signal)
        direction = str(getattr(lane_signal, "direction", "") or "").strip().lower()
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(lane_signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if direction not in {"long", "short"} or entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            self._stamp_family_canary_skip(
                signal, family=family, stage="signal_geometry", reason="mfu_invalid_entry_stop_geometry"
            )
            return None, ""
        sign = 1.0 if direction == "long" else -1.0
        capture_features = dict(((matched_context.get("snapshot") or {}).get("features") or ((matched_context.get("snapshot") or {}).get("gate") or {}).get("features") or {}))
        continuation_bias = float(((matched_context.get("chart_state") or {}).get("continuation_bias") or 0.0) or 0.0)
        delta_proxy = float(capture_features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        follow_plan = str(matched_context.get("follow_up_plan") or "").strip().lower()
        atr_eff = max(base_risk, atr, entry * 0.0003)
        use_break_stop = bool(
            "break_stop" in follow_plan
            and abs(continuation_bias) >= float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.12) or 0.12)
            and (sign * delta_proxy) >= float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY", 0.10) or 0.10)
            and bar_volume_proxy >= float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.42) or 0.42)
        )
        if use_break_stop:
            trigger = max(
                base_risk * float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_TRIGGER_RISK_RATIO", 0.14) or 0.14),
                atr_eff * 0.06,
            )
            stop_lift = trigger * float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_STOP_LIFT_RATIO", 0.38) or 0.38)
            new_entry = entry + (trigger * sign)
            new_stop = stop_loss + (stop_lift * sign)
            entry_type = "buy_stop" if direction == "long" else "sell_stop"
        else:
            retest = max(
                base_risk * float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_SHALLOW_RETEST_RISK_RATIO", 0.12) or 0.12),
                atr_eff * 0.05,
            )
            stop_pad = retest * 0.28
            new_entry = entry - (retest * sign)
            new_stop = stop_loss - (stop_pad * sign)
            entry_type = "limit"
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type=entry_type)
        if shaped is None:
            self._stamp_family_canary_skip(
                signal, family=family, stage="price_plan", reason="mfu_apply_family_price_plan_returned_none"
            )
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_chart_state_follow_up"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["chart_state_follow_up"] = {
                "state_label": str((matched_context.get("chart_state") or {}).get("state_label") or ""),
                "day_type": str((matched_context.get("chart_state") or {}).get("day_type") or ""),
                "follow_up_plan": follow_plan,
                "state_score": float(matched_context.get("state_score", 0.0) or 0.0),
                "session": str(matched_context.get("session") or ""),
                "timeframe": str(matched_context.get("timeframe") or ""),
                "direction": str(matched_context.get("direction") or ""),
                "confidence_band": str(matched_context.get("confidence_band") or ""),
                "summary": str((matched_context.get("chart_state") or {}).get("state_label") or ""),
            }
            if bool(matched_context.get("relaxed_confidence_band")):
                raw["chart_state_follow_up"]["relaxed_confidence_band"] = True
                raw["chart_state_follow_up"]["requested_confidence_band"] = str(matched_context.get("requested_confidence_band") or "")
            if bool(matched_context.get("relaxed_h1_trend")):
                raw["chart_state_follow_up"]["relaxed_h1_trend"] = True
                raw["chart_state_follow_up"]["requested_h1_trend"] = str(matched_context.get("requested_h1_trend") or "")
            if bool(matched_context.get("relaxed_day_type")):
                raw["chart_state_follow_up"]["relaxed_day_type"] = True
                raw["chart_state_follow_up"]["requested_day_type"] = str(matched_context.get("requested_day_type") or "")
            if bool(matched_context.get("first_sample_mode")):
                raw["chart_state_follow_up"]["first_sample_mode"] = True
                raw["chart_state_follow_up"]["first_sample_relaxed_blockers"] = list(matched_context.get("first_sample_relaxed_blockers") or [])
            raw["chart_state_follow_up_snapshot"] = {
                "run_id": str(((matched_context.get("snapshot") or {}).get("run_id") or "")),
                "last_event_utc": str(((matched_context.get("snapshot") or {}).get("last_event_utc") or "")),
                "entry_mode": "break_stop" if use_break_stop else "shallow_retest_limit",
            }
            raw["mt5_ignore_open_positions"] = True
            risk_usd = float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD", 0.65) or 0.65)
            if bool(matched_context.get("relaxed_h1_trend")) or bool(matched_context.get("relaxed_day_type")):
                risk_usd *= float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_RELAXED_RISK_MULTIPLIER", 0.85) or 0.85)
            if bool(matched_context.get("first_sample_mode")):
                risk_usd *= float(getattr(config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_RISK_MULTIPLIER", 0.70) or 0.70)
            raw["ctrader_risk_usd_override"] = round(float(risk_usd), 4)
            raw["mt5_limit_allow_market_fallback"] = False
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state=dict(matched_context.get("chart_state") or {}),
                follow_up_plan=follow_plan,
            )
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    def _build_xau_range_repair_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_range_repair":
            return None, ""
        if not bool(getattr(config, "XAU_RANGE_REPAIR_ENABLED", False)):
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return None, ""
        rr_min_conf = float(getattr(config, "XAU_RANGE_REPAIR_MIN_CONFIDENCE", 0.0) or 0.0)
        if rr_min_conf > 0:
            signal_conf = float(getattr(signal, "confidence", 0.0) or 0.0)
            if signal_conf < rr_min_conf:
                return None, ""
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=str(getattr(signal, "symbol", "") or ""),
                    lookback_sec=int(getattr(config, "XAU_RANGE_REPAIR_LOOKBACK_SEC", 300) or 300),
                    direction=direction,
                    confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return None, ""
        capture_features = dict(snapshot.get("features") or (snapshot.get("gate") or {}).get("features") or {})
        chart_state = dict(
            live_profile_classify_chart_state(
                direction,
                self._signal_request_context(signal),
                capture_features=capture_features,
            )
            or {}
        )
        state_label = str(chart_state.get("state_label") or "").strip().lower()
        day_type = str(chart_state.get("day_type") or "").strip().lower()
        allowed_states = self._parse_lower_csv(getattr(config, "XAU_RANGE_REPAIR_ALLOWED_STATES", ""))
        blocked_day_types = self._parse_lower_csv(getattr(config, "XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES", ""))
        blocked_sessions = self._parse_lower_csv(getattr(config, "XAU_RANGE_REPAIR_BLOCKED_SESSIONS", ""))
        if allowed_states and state_label not in allowed_states:
            return None, ""
        if blocked_day_types and day_type in blocked_day_types:
            return None, ""
        if blocked_sessions:
            signal_session = self._normalized_signature(str(getattr(signal, "session", "") or ""))
            if any(bs and bs in signal_session for bs in blocked_sessions):
                return None, ""
        continuation_bias = abs(float(chart_state.get("continuation_bias", 0.0) or 0.0))
        rejection_ratio = float(capture_features.get("rejection_ratio", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        spread_expansion = float(capture_features.get("spread_expansion", 1.0) or 1.0)
        spread_avg_pct = abs(float(capture_features.get("spread_avg_pct", 0.0) or 0.0))
        delta_proxy = abs(float(capture_features.get("delta_proxy", 0.0) or 0.0))
        depth_imbalance = abs(float(capture_features.get("depth_imbalance", 0.0) or 0.0))
        if continuation_bias > float(getattr(config, "XAU_RANGE_REPAIR_MAX_CONTINUATION_BIAS", 0.09) or 0.09):
            return None, ""
        if rejection_ratio < float(getattr(config, "XAU_RANGE_REPAIR_MIN_REJECTION_RATIO", 0.16) or 0.16):
            return None, ""
        if bar_volume_proxy < float(getattr(config, "XAU_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", 0.18) or 0.18):
            return None, ""
        if delta_proxy > float(getattr(config, "XAU_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", 0.11) or 0.11):
            return None, ""
        if depth_imbalance > float(getattr(config, "XAU_RANGE_REPAIR_MAX_ABS_DEPTH_IMBALANCE", 0.10) or 0.10):
            return None, ""
        if spread_expansion > float(getattr(config, "XAU_RANGE_REPAIR_MAX_SPREAD_EXPANSION", 1.10) or 1.10):
            return None, ""
        if spread_avg_pct > float(getattr(config, "XAU_RANGE_REPAIR_MAX_SPREAD_AVG_PCT", 0.0022) or 0.0022):
            return None, ""
        sign = 1.0 if direction == "long" else -1.0
        # ── Falling-knife guard ─────────────────────────────────────────────────
        # The existing delta_proxy gate uses abs() so -0.09 (sellers dominating)
        # passes as easily as +0.09. For a ranging long entry that means we can
        # fire a limit while price is still cascading down — a falling knife.
        # Guard: block if raw signed delta is adverse OR tick flow is clearly down.
        if bool(getattr(config, "XAU_RANGE_REPAIR_KNIFE_GUARD_ENABLED", True)):
            signed_delta = float(capture_features.get("delta_proxy", 0.0) or 0.0)
            tick_up = float(capture_features.get("tick_up_ratio", 0.5) or 0.5)
            max_adv = float(getattr(config, "XAU_RANGE_REPAIR_KNIFE_GUARD_MAX_ADVERSE_DELTA_PROXY", 0.07) or 0.07)
            min_tick_up = float(getattr(config, "XAU_RANGE_REPAIR_KNIFE_GUARD_MIN_TICK_UP_RATIO", 0.38) or 0.38)
            adverse_delta = (sign * signed_delta) <= -max_adv
            tick_falling = (direction == "long" and tick_up < min_tick_up) or (direction == "short" and (1.0 - tick_up) < min_tick_up)
            if adverse_delta or tick_falling:
                return None, ""
        # ── Sharpness composite knife guard (catches edge cases binary checks miss) ─
        if bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            try:
                from analysis.entry_sharpness import compute_entry_sharpness_score as _compute_sharpness
                rr_sharpness = _compute_sharpness(capture_features, direction, micro_vol_scale=float(getattr(config, "XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE", 0.025) or 0.025), max_spread_expansion=float(getattr(config, "XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION", 1.20) or 1.20))
                if int(rr_sharpness.get("sharpness_score", 50) or 50) < max(1, int(getattr(config, "XAU_ENTRY_SHARPNESS_RR_KNIFE_THRESHOLD", 30) or 30)):
                    return None, ""
            except Exception:
                pass
        lane_signal = copy.deepcopy(signal)
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(lane_signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0003)
        retest = max(
            base_risk * float(getattr(config, "XAU_RANGE_REPAIR_ENTRY_RISK_RATIO", 0.10) or 0.10),
            atr_eff * float(getattr(config, "XAU_RANGE_REPAIR_ENTRY_ATR_RATIO", 0.045) or 0.045),
        )
        new_entry = entry - (retest * sign)
        new_risk = max(
            entry * 0.00005,
            base_risk * float(getattr(config, "XAU_RANGE_REPAIR_STOP_KEEP_RISK_RATIO", 0.72) or 0.72),
        )
        new_stop = new_entry - (new_risk * sign)
        lane_signal.take_profit_1 = round(float(entry + (base_risk * sign * float(getattr(config, "XAU_RANGE_REPAIR_TP1_RR", 0.50) or 0.50))), 4)
        lane_signal.take_profit_2 = round(float(entry + (base_risk * sign * float(getattr(config, "XAU_RANGE_REPAIR_TP2_RR", 0.82) or 0.82))), 4)
        lane_signal.take_profit_3 = round(float(entry + (base_risk * sign * float(getattr(config, "XAU_RANGE_REPAIR_TP3_RR", 1.10) or 1.10))), 4)
        lane_signal.risk_reward = round(float(getattr(config, "XAU_RANGE_REPAIR_TP2_RR", 0.82) or 0.82), 2)
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type="limit")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        follow_plan = str(chart_state.get("follow_up_plan") or "").strip().lower()
        if not follow_plan:
            follow_plan = "probe_repair_limit_after_exhaustion" if state_label == "reversal_exhaustion" else "fade_range_edge_with_limit_only"
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_range_repair"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["chart_state_range_repair"] = {
                "state_label": state_label,
                "day_type": day_type,
                "follow_up_plan": follow_plan,
                "continuation_bias": round(continuation_bias, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "delta_proxy_abs": round(delta_proxy, 4),
                "delta_proxy_signed": round(float(capture_features.get("delta_proxy", 0.0) or 0.0), 4),
                "tick_up_ratio": round(float(capture_features.get("tick_up_ratio", 0.5) or 0.5), 4),
                "depth_imbalance_abs": round(depth_imbalance, 4),
                "spread_expansion": round(spread_expansion, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "knife_guard_passed": True,
            }
            raw["range_repair_snapshot"] = {
                "run_id": str(snapshot.get("run_id") or ""),
                "last_event_utc": str(snapshot.get("last_event_utc") or ""),
                "entry_mode": "range_limit_repair",
            }
            raw["xau_range_repair_gate"] = {
                "allowed_states": sorted(list(allowed_states)),
                "state_label": state_label,
                "blocked_day_types": sorted(list(blocked_day_types)),
                "day_type": day_type,
            }
            raw["mt5_ignore_open_positions"] = True
            raw["ctrader_risk_usd_override"] = round(float(getattr(config, "XAU_RANGE_REPAIR_CTRADER_RISK_USD", 0.35) or 0.35), 4)
            raw["mt5_limit_allow_market_fallback"] = False
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state=chart_state,
                follow_up_plan=follow_plan,
            )
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    def _build_xau_psc_canary_signal(
        self, signal, *, base_source: str, candidate: dict
    ) -> tuple[object | None, str]:
        """
        Pre-London Sweep Continuation (PSC) canary.
        Asian range (17:00-22:00 UTC) establishes H/L.
        In the 22:00-02:30 UTC window, detect a false-break sweep below
        Asian low (long) or above Asian high (short) followed by a V-shape
        recovery, then enter on continuation toward the opposite range extreme.
        """
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_prelondon_sweep_cont":
            return None, ""
        if not bool(getattr(config, "XAU_PSC_ENABLED", False)):
            self._stamp_family_canary_skip(signal, family=family, stage="family_disabled", reason="XAU_PSC_ENABLED=0")
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            self._stamp_family_canary_skip(signal, family=family, stage="signal_geometry", reason="psc_invalid_direction")
            return None, ""
        try:
            # ── 1. Session window guard ──────────────────────────────────────
            now_utc = datetime.now(timezone.utc)
            h = now_utc.hour + now_utc.minute / 60.0
            pre_start = float(getattr(config, "XAU_PSC_PRE_LONDON_START_UTC", 22.0) or 22.0)
            pre_end = float(getattr(config, "XAU_PSC_PRE_LONDON_END_UTC", 2.5) or 2.5)
            # Window wraps midnight: valid if h >= 22.0 OR h <= 2.5
            in_window = (h >= pre_start) or (h <= pre_end)
            if not in_window:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="session_window", reason="psc_outside_pre_london_window"
                )
                return None, ""
            # ── 2. Fetch M5 bars (120 bars = 10h, enough for full Asian range) ──
            df_raw = xauusd_provider.fetch("5m", bars=120)
            if df_raw is None or df_raw.empty or len(df_raw) < 30:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="feature_snapshot", reason="psc_m5_bars_unavailable_or_short"
                )
                return None, ""
            df = df_raw.copy()
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            # ── 3. Asian range (17:00–22:00 UTC before sweep window start) ──
            today0 = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            boundary_22h = today0.replace(hour=22)
            if now_utc < boundary_22h:
                boundary_22h = boundary_22h - timedelta(days=1)
            asian_end = boundary_22h
            asian_start = asian_end - timedelta(hours=5)   # 17:00 UTC
            asian_bars = df[(df.index >= asian_start) & (df.index < asian_end)]
            if len(asian_bars) < 8:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="pattern_context", reason="psc_asian_session_bars_insufficient"
                )
                return None, ""
            asian_high = float(asian_bars["high"].max())
            asian_low = float(asian_bars["low"].min())
            asian_range = asian_high - asian_low
            rng_min = float(getattr(config, "XAU_PSC_ASIAN_RANGE_MIN", 8.0) or 8.0)
            rng_max = float(getattr(config, "XAU_PSC_ASIAN_RANGE_MAX", 45.0) or 45.0)
            if not (rng_min <= asian_range <= rng_max):
                self._stamp_family_canary_skip(
                    signal,
                    family=family,
                    stage="pattern_context",
                    reason=f"psc_asian_range_out_of_band:{asian_range:.2f}not_in[{rng_min},{rng_max}]",
                )
                return None, ""
            # ── 4. Sweep detection (bars since boundary_22h) ─────────────────
            sweep_bars_df = df[df.index >= boundary_22h]
            if len(sweep_bars_df) < 3:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="pattern_context", reason="psc_sweep_window_bars_insufficient"
                )
                return None, ""
            recovery_max_bars = int(getattr(config, "XAU_PSC_RECOVERY_MAX_BARS", 8) or 8)
            recent = (sweep_bars_df.iloc[-recovery_max_bars:] if len(sweep_bars_df) >= recovery_max_bars else sweep_bars_df)
            sw_min = float(getattr(config, "XAU_PSC_SWEEP_MIN_DEPTH", 5.0) or 5.0)
            sw_max = float(getattr(config, "XAU_PSC_SWEEP_MAX_DEPTH", 30.0) or 30.0)
            sweep_detected = False
            sweep_extreme = 0.0
            sweep_depth = 0.0
            bars_since_sweep = 0
            if direction == "long":
                candidate_low = float(recent["low"].min())
                depth_val = asian_low - candidate_low
                if sw_min <= depth_val <= sw_max:
                    sw_idx = int(recent["low"].values.argmin())
                    post = recent.iloc[sw_idx + 1:]
                    if len(post) > 0 and float(post["close"].iloc[-1]) > asian_low:
                        sweep_detected = True
                        sweep_extreme = candidate_low
                        sweep_depth = depth_val
                        bars_since_sweep = len(recent) - 1 - sw_idx
            else:
                candidate_high = float(recent["high"].max())
                depth_val = candidate_high - asian_high
                if sw_min <= depth_val <= sw_max:
                    sw_idx = int(recent["high"].values.argmax())
                    post = recent.iloc[sw_idx + 1:]
                    if len(post) > 0 and float(post["close"].iloc[-1]) < asian_high:
                        sweep_detected = True
                        sweep_extreme = candidate_high
                        sweep_depth = depth_val
                        bars_since_sweep = len(recent) - 1 - sw_idx
            if not sweep_detected:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="pattern_context", reason="psc_sweep_v_recovery_not_detected"
                )
                return None, ""
            # ── 5. No-chase guard ─────────────────────────────────────────────
            current_price = float(df["close"].iloc[-1])
            no_chase = float(getattr(config, "XAU_PSC_NO_CHASE_MAX_PIPS", 20.0) or 20.0)
            if direction == "long" and current_price > asian_low + no_chase:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="price_plan", reason="psc_no_chase_long_extended_above_asian_low"
                )
                return None, ""
            if direction == "short" and current_price < asian_high - no_chase:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="price_plan", reason="psc_no_chase_short_extended_below_asian_high"
                )
                return None, ""
            # ── 6. Momentum confirmation (must support recovery direction) ───
            try:
                snapshot = dict(
                    live_profile_autopilot.latest_capture_feature_snapshot(
                        symbol=str(getattr(signal, "symbol", "") or ""),
                        lookback_sec=int(getattr(config, "XAU_PSC_LOOKBACK_SEC", 300) or 300),
                        direction=direction,
                        confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                    ) or {}
                )
            except Exception:
                snapshot = {}
            feats = dict(snapshot.get("features") or (snapshot.get("gate") or {}).get("features") or {})
            signed_delta = float(feats.get("delta_proxy", 0.0) or 0.0)
            tick_up = float(feats.get("tick_up_ratio", 0.5) or 0.5)
            bar_vol = float(feats.get("bar_volume_proxy", 0.0) or 0.0)
            min_d = float(getattr(config, "XAU_PSC_MIN_SIGNED_DELTA", 0.02) or 0.02)
            min_t = float(getattr(config, "XAU_PSC_MIN_TICK_UP_RATIO", 0.48) or 0.48)
            if direction == "long" and signed_delta < -min_d:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="flow_guard", reason="psc_momentum_long_signed_delta_adverse"
                )
                return None, ""
            if direction == "short" and signed_delta > min_d:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="flow_guard", reason="psc_momentum_short_signed_delta_adverse"
                )
                return None, ""
            if direction == "long" and tick_up < min_t:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="flow_guard", reason="psc_momentum_long_tick_up_ratio_weak"
                )
                return None, ""
            if direction == "short" and (1.0 - tick_up) < min_t:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="flow_guard", reason="psc_momentum_short_tick_down_ratio_weak"
                )
                return None, ""
            # ── 7. Build price plan ───────────────────────────────────────────
            lane_signal = copy.deepcopy(signal)
            entry_buf = float(getattr(config, "XAU_PSC_ENTRY_BUFFER", 1.5) or 1.5)
            sl_buf = float(getattr(config, "XAU_PSC_SL_BUFFER", 3.0) or 3.0)
            tp1_rr = float(getattr(config, "XAU_PSC_TP1_RR", 0.55) or 0.55)
            tp2_rr = float(getattr(config, "XAU_PSC_TP2_RR", 1.10) or 1.10)
            tp3_rr = float(getattr(config, "XAU_PSC_TP3_RR", 1.80) or 1.80)
            if direction == "long":
                new_entry = round(asian_low + entry_buf, 4)
                new_stop = round(sweep_extreme - sl_buf, 4)
            else:
                new_entry = round(asian_high - entry_buf, 4)
                new_stop = round(sweep_extreme + sl_buf, 4)
            new_risk = abs(new_entry - new_stop)
            if new_risk <= 0 or new_entry <= 0:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="signal_geometry", reason="psc_computed_entry_stop_invalid"
                )
                return None, ""
            # Override TP targets using sweep depth as scale reference
            sign = 1.0 if direction == "long" else -1.0
            lane_signal.take_profit_1 = round(new_entry + sign * new_risk * tp1_rr, 4)
            lane_signal.take_profit_2 = round(new_entry + sign * new_risk * tp2_rr, 4)
            lane_signal.take_profit_3 = round(new_entry + sign * new_risk * tp3_rr, 4)
            lane_signal.risk_reward = round(tp2_rr, 2)
            shaped = self._apply_family_price_plan(
                lane_signal,
                family=family,
                entry=new_entry,
                stop_loss=new_stop,
                entry_type="limit",
            )
            if shaped is None:
                self._stamp_family_canary_skip(
                    signal, family=family, stage="price_plan", reason="psc_apply_family_price_plan_returned_none"
                )
                return None, ""
            lane_source = self._strategy_family_lane_source(base_source, family)
            self._ensure_signal_trace(shaped, source=lane_source)
            # ── 8. Audit trail ─────────────────────────────────────────────────
            try:
                raw = dict(getattr(shaped, "raw_scores", {}) or {})
                raw["persistent_canary_enabled"] = True
                raw["persistent_canary_family_enabled"] = True
                raw["persistent_canary_source"] = lane_source
                raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
                raw["experimental_family"] = True
                raw["mt5_canary_mode"] = True
                raw["strategy_family"] = family
                raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
                raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
                raw["strategy_family_executor"] = "scheduler_canary_psc"
                raw["strategy_family_alias"] = self._strategy_family_alias(family)
                raw["chart_state_psc"] = {
                    "asian_high": round(asian_high, 4),
                    "asian_low": round(asian_low, 4),
                    "asian_range": round(asian_range, 4),
                    "sweep_extreme": round(sweep_extreme, 4),
                    "sweep_depth": round(sweep_depth, 4),
                    "bars_since_sweep": bars_since_sweep,
                    "current_price": round(current_price, 4),
                    "signed_delta": round(signed_delta, 4),
                    "tick_up_ratio": round(tick_up, 4),
                    "bar_volume_proxy": round(bar_vol, 4),
                    "new_entry": round(new_entry, 4),
                    "new_stop": round(new_stop, 4),
                    "new_risk": round(new_risk, 4),
                    "sweep_window_start_utc": boundary_22h.isoformat(),
                    "asian_session_utc": f"{asian_start.strftime('%H:%M')}-{asian_end.strftime('%H:%M')} UTC",
                }
                raw["mt5_ignore_open_positions"] = True
                raw["ctrader_risk_usd_override"] = round(
                    float(getattr(config, "XAU_PSC_CTRADER_RISK_USD", 0.75) or 0.75), 4
                )
                raw["mt5_limit_allow_market_fallback"] = False
                raw = self._apply_xau_observability_tags(
                    raw,
                    source=lane_source,
                    family=family,
                    chart_state={
                        "state_label": "prelondon_sweep_continuation",
                        "day_type": "",
                    },
                    follow_up_plan="sweep_recovery_london_continuation",
                )
                shaped.raw_scores = raw
            except Exception:
                pass
            return shaped, lane_source
        except Exception as e:
            logger.debug("[Scheduler] PSC canary build error: %s", e)
            return None, ""

    def _build_tick_depth_filter_canary_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "xau_scalp_tick_depth_filter":
            return None, ""
        if not bool(getattr(config, "XAU_TICK_DEPTH_FILTER_ENABLED", True)):
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        snapshot = dict(
            live_profile_autopilot.latest_capture_feature_snapshot(
                symbol=str(getattr(signal, "symbol", "") or ""),
                lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                direction=direction,
                confidence=confidence,
            )
            or {}
        )
        gate = dict(snapshot.get("gate") or {})
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return None, ""
        if not bool(gate.get("pass")) and not bool(gate.get("canary_sample_pass")):
            return None, ""
        sample_mode = str(gate.get("sample_mode") or "").strip().lower()
        lane_signal = copy.deepcopy(signal)
        entry = float(getattr(lane_signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(lane_signal, "stop_loss", 0.0) or 0.0)
        entry_type = str(getattr(lane_signal, "entry_type", "") or "limit").strip().lower() or "limit"
        if entry_type not in {"limit", "buy_stop", "sell_stop"}:
            entry_type = "limit"
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=entry, stop_loss=stop_loss, entry_type=entry_type)
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            tick_chart_state = dict(
                live_profile_classify_chart_state(
                    direction,
                    self._signal_request_context(shaped),
                    capture_features=dict(gate.get("features") or snapshot.get("features") or {}),
                )
                or {}
            )
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_tick_depth_filter"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["tick_depth_filter_snapshot"] = {
                "run_id": str(snapshot.get("run_id") or ""),
                "last_event_utc": str(snapshot.get("last_event_utc") or ""),
                "gate": dict(gate),
            }
            if sample_mode:
                raw["tick_depth_filter_sample_mode"] = sample_mode
            raw["mt5_ignore_open_positions"] = True
            risk_usd = float(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_CTRADER_RISK_USD", 0.75) or 0.75)
            if bool(gate.get("canary_sample_pass")):
                risk_usd *= float(gate.get("sample_risk_multiplier", 1.0) or 1.0)
            raw["ctrader_risk_usd_override"] = round(max(0.1, risk_usd), 4)
            raw["mt5_limit_allow_market_fallback"] = False
            raw = self._apply_xau_observability_tags(
                raw,
                source=lane_source,
                family=family,
                chart_state=tick_chart_state,
                follow_up_plan="",
            )
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    def _build_crypto_weekday_experimental_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        family = str((candidate or {}).get("family") or "").strip().lower()
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        base_token = str(base_source or "").strip().lower().split(":", 1)[0]
        if signal is None or family not in {"btc_weekday_lob_momentum", "eth_weekday_overlap_probe"}:
            return None, ""
        is_weekend = datetime.now(timezone.utc).weekday() >= 5
        if is_weekend and not bool(getattr(config, "CRYPTO_WEEKEND_TRADING_ENABLED", False)):
            return None, ""
        # Phase 1: Cluster loss guard + daily cap (isolated per symbol, no XAU impact)
        _clg_blocked, _clg_reason = self._crypto_cluster_loss_check(symbol)
        if _clg_blocked:
            return None, ""
        _cap_blocked, _cap_reason = self._crypto_daily_cap_check(symbol)
        if _cap_blocked:
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return None, ""
        timeframe_token = self._signal_timeframe_token(signal)
        if timeframe_token != "5m+1m":
            return None, ""
        session_sig = self._signal_session_signature(signal)
        try:
            confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        if entry <= 0 or stop_loss <= 0 or abs(entry - stop_loss) <= 0:
            return None, ""
        entry_type = str(getattr(signal, "entry_type", "") or "market").strip().lower() or "market"
        pattern = str(getattr(signal, "pattern", "") or "").strip().lower()
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        winner_regime = str(raw.get("crypto_winner_logic_regime") or raw.get("winner_logic_regime") or "").strip().lower()
        neural_prob = float(raw.get("neural_probability", 0.0) or 0.0)
        relaxed_gate_reasons: list[str] = []
        if family == "btc_weekday_lob_momentum":
            if symbol != "BTCUSD" or base_token != "scalp_btcusd":
                return None, ""
            if direction != "long":
                return None, ""
            btc_sessions = set(config.get_crypto_weekend_btc_allowed_sessions() or set()) if is_weekend else set(config.get_btc_weekday_lob_allowed_sessions() or set())
            if "*" not in btc_sessions and not self._session_signature_matches(session_sig, btc_sessions):
                # Phase 3: high-confidence session bypass (mirror of XAU Priority #1)
                _btc_np_bypass = float(getattr(signal, "raw_scores", {}) or {} if isinstance(getattr(signal, "raw_scores", None), dict) else {})
                try:
                    _btc_np_bypass = float((dict(getattr(signal, "raw_scores", {}) or {})).get("neural_probability", 0.0) or 0.0)
                except Exception:
                    _btc_np_bypass = 0.0
                _btc_bypass_thresh = float(getattr(config, "BTC_SCHEDULED_HIGH_CONF_SESSION_BYPASS_THRESHOLD", 0.87) or 0.87)
                if _btc_np_bypass < _btc_bypass_thresh:
                    return None, ""
                relaxed_gate_reasons.append(f"btc_scheduled_high_conf_session_bypass:np={_btc_np_bypass:.2f}")
            if confidence < float(getattr(config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0) or 70.0):
                return None, ""
            if confidence > float(getattr(config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9) or 74.9):
                return None, ""
            allowed_patterns = set(config.get_btc_weekday_lob_allowed_patterns() or set())
            if allowed_patterns and ((not pattern) or pattern.lower() not in allowed_patterns):
                return None, ""
            neutral_ob_allowed = (
                bool(getattr(config, "BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE", True))
                and pattern == "ob_bounce"
                and winner_regime == "neutral"
                and confidence >= float(getattr(config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE", 72.8) or 72.8)
                and neural_prob >= float(getattr(config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB", 0.65) or 0.65)
            )
            if neutral_ob_allowed:
                relaxed_gate_reasons.append("neutral_ob_bounce")
            weekend_neutral_ok = is_weekend and bool(getattr(config, "CRYPTO_WEEKEND_ALLOW_NEUTRAL_WINNER", True)) and winner_regime == "neutral"
            if weekend_neutral_ok:
                relaxed_gate_reasons.append("weekend_neutral_winner")
            if bool(getattr(config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True)) and winner_regime != "strong" and not neutral_ob_allowed and not weekend_neutral_ok:
                return None, ""
            # Phase 4 MRD: block LOB longs when BTC macro micro-regime is bearish
            _lob_mrd_regime, _ = self._btc_mrd_check("long")
            if _lob_mrd_regime == "bearish_micro":
                return None, ""
            if entry_type == "market" and not bool(getattr(config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True)):
                return None, ""
            if "choch_entry" in pattern and entry_type != "limit":
                choch_market_reprice = (
                    bool(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_TO_LIMIT_ENABLED", True))
                    and entry_type == "market"
                    and winner_regime == "strong"
                    and confidence <= float(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MAX_CONFIDENCE", 72.2) or 72.2)
                    and neural_prob >= float(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MIN_NEURAL_PROB", 0.63) or 0.63)
                )
                if not choch_market_reprice:
                    return None, ""
                risk = abs(entry - stop_loss)
                pullback_ratio = float(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_LIMIT_PULLBACK_RISK_RATIO", 0.12) or 0.12)
                pullback = max(risk * pullback_ratio, entry * 0.00008)
                entry = entry - pullback
                entry_type = "limit"
                relaxed_gate_reasons.append("choch_market_to_limit")
            risk_usd = float(getattr(config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9) or 0.9)
            if relaxed_gate_reasons:
                risk_usd *= float(getattr(config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.70) or 0.70)
            if is_weekend:
                risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        else:
            if symbol != "ETHUSD" or base_token != "scalp_ethusd":
                return None, ""
            eth_sessions = set(config.get_crypto_weekend_eth_allowed_sessions() or set()) if is_weekend else set(config.get_eth_weekday_probe_allowed_sessions() or set())
            if "*" not in eth_sessions and not self._session_signature_matches(session_sig, eth_sessions):
                return None, ""
            if confidence < float(getattr(config, "ETH_WEEKDAY_PROBE_MIN_CONFIDENCE", 74.0) or 74.0):
                return None, ""
            if confidence > float(getattr(config, "ETH_WEEKDAY_PROBE_MAX_CONFIDENCE", 79.9) or 79.9):
                return None, ""
            allowed_patterns = set(config.get_eth_weekday_probe_allowed_patterns() or set())
            if allowed_patterns and ((not pattern) or pattern.lower() not in allowed_patterns):
                return None, ""
            eth_weekend_neutral_ok = is_weekend and bool(getattr(config, "CRYPTO_WEEKEND_ALLOW_NEUTRAL_WINNER", True)) and winner_regime == "neutral"
            if eth_weekend_neutral_ok:
                relaxed_gate_reasons.append("weekend_neutral_winner")
            if bool(getattr(config, "ETH_WEEKDAY_PROBE_REQUIRE_STRONG_WINNER", True)) and winner_regime != "strong" and not eth_weekend_neutral_ok:
                return None, ""
            if entry_type == "market" and not bool(getattr(config, "ETH_WEEKDAY_PROBE_ALLOW_MARKET", True)):
                return None, ""
            risk_usd = float(getattr(config, "ETH_WEEKDAY_PROBE_CTRADER_RISK_USD", 0.35) or 0.35)
            if is_weekend:
                risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(
            lane_signal,
            family=family,
            entry=entry,
            stop_loss=stop_loss,
            entry_type=entry_type,
        )
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_crypto_weekday"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["crypto_weekday_experimental"] = True
            raw["crypto_weekend_mode"] = is_weekend
            raw["crypto_weekend_risk_multiplier"] = float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65) if is_weekend else 1.0
            raw["crypto_weekday_regime"] = winner_regime
            raw["crypto_weekday_session"] = session_sig
            raw["crypto_weekday_pattern"] = pattern
            raw["strategy_family_relaxed_gate"] = bool(relaxed_gate_reasons)
            raw["strategy_family_relaxed_reason"] = ",".join(relaxed_gate_reasons)
            raw["mt5_ignore_open_positions"] = True
            raw["ctrader_risk_usd_override"] = risk_usd
            raw["mt5_limit_allow_market_fallback"] = False
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── Crypto Smart Family builders (CFS / CFB / CWC / CBR) ─────────────

    def _crypto_family_common_preamble(self, signal, *, base_source: str, candidate: dict, family: str) -> tuple[dict | None, str]:
        """Shared preamble for all crypto smart families. Returns (ctx_dict, "") or (None, "") on gate failure."""
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        base_token = str(base_source or "").strip().lower().split(":", 1)[0]
        if signal is None:
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return None, ""
        timeframe_token = self._signal_timeframe_token(signal)
        if timeframe_token != "5m+1m":
            return None, ""
        session_sig = self._signal_session_signature(signal)
        try:
            confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        if entry <= 0 or stop_loss <= 0 or abs(entry - stop_loss) <= 0:
            return None, ""
        entry_type = str(getattr(signal, "entry_type", "") or "market").strip().lower() or "market"
        pattern = str(getattr(signal, "pattern", "") or "").strip().lower()
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        winner_regime = str(raw.get("crypto_winner_logic_regime") or raw.get("winner_logic_regime") or "").strip().lower()
        winner_wr = float(raw.get("crypto_winner_logic_win_rate", 0.0) or 0.0)
        neural_prob = float(raw.get("neural_probability", 0.0) or 0.0)
        long_score = float(raw.get("long", 0.0) or 0.0)
        short_score = float(raw.get("short", 0.0) or 0.0)
        edge = float(raw.get("edge", 0.0) or 0.0)
        trigger = raw.get("scalping_trigger") or {}
        rsi = float(trigger.get("rsi14", 0.0) or 0.0) if isinstance(trigger, dict) else 0.0
        is_weekend = datetime.now(timezone.utc).weekday() >= 5
        return {
            "symbol": symbol, "base_token": base_token, "direction": direction,
            "session_sig": session_sig, "confidence": confidence, "entry": entry,
            "stop_loss": stop_loss, "entry_type": entry_type, "pattern": pattern,
            "raw": raw, "winner_regime": winner_regime, "winner_wr": winner_wr,
            "neural_prob": neural_prob, "long_score": long_score, "short_score": short_score,
            "edge": edge, "rsi": rsi, "is_weekend": is_weekend, "family": family,
        }, ""

    def _crypto_family_tag_and_return(self, signal, shaped, *, lane_source: str, candidate: dict, family: str, risk_usd: float, ctx: dict, extra_tags: dict | None = None) -> tuple[object | None, str]:
        """Shared post-processing for all crypto smart families."""
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(ctx.get("base_token") or "")
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = f"scheduler_canary_{family}"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["crypto_weekend_mode"] = ctx.get("is_weekend", False)
            raw["mt5_ignore_open_positions"] = True
            raw["ctrader_risk_usd_override"] = risk_usd
            raw["mt5_limit_allow_market_fallback"] = False
            raw["persistent_canary_symbol"] = ctx.get("symbol", "")
            if extra_tags:
                raw.update(extra_tags)
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── Crypto Guards: Cluster Loss + Daily Cap (Phase 1) ────────────────────

    def _crypto_cluster_loss_check(self, symbol: str) -> tuple[bool, str]:
        """Block if too many recent losses for this crypto symbol (mirror of XAU cluster loss guard)."""
        if bool(self._is_pytest_runtime()):
            return False, ""
        if not bool(getattr(config, "CRYPTO_CLUSTER_LOSS_GUARD_ENABLED", True)):
            return False, ""
        sym = str(symbol or "").strip().upper()
        if sym not in {"BTCUSD", "ETHUSD"}:
            return False, ""
        if sym == "BTCUSD":
            window_h = float(getattr(config, "BTC_CLUSTER_LOSS_WINDOW_HOURS", 3.0) or 3.0)
            min_losses = int(getattr(config, "BTC_CLUSTER_LOSS_MIN_LOSSES", 2) or 2)
        else:
            window_h = float(getattr(config, "ETH_CLUSTER_LOSS_WINDOW_HOURS", 2.0) or 2.0)
            min_losses = int(getattr(config, "ETH_CLUSTER_LOSS_MIN_LOSSES", 2) or 2)
        try:
            db_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
            db_path = Path(db_cfg) if db_cfg else (Path(__file__).resolve().parent / "data" / "ctrader_openapi.db")
            with sqlite3.connect(str(db_path), timeout=3) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM ctrader_deals WHERE symbol=? AND pnl_usd < 0 AND outcome NOT IN ('open','pending') AND execution_utc >= datetime('now', ? || ' hours')",
                    (sym, f"-{window_h:.1f}"),
                ).fetchone()
            loss_count = int((row or [0])[0] or 0)
            if loss_count >= min_losses:
                return True, f"crypto_cluster_loss_guard:{sym} {loss_count}>={min_losses} losses in {window_h:.1f}h"
        except Exception:
            pass
        return False, ""

    def _crypto_daily_cap_check(self, symbol: str) -> tuple[bool, str]:
        """Block if daily trade count for this crypto symbol exceeds cap."""
        if bool(self._is_pytest_runtime()):
            return False, ""
        sym = str(symbol or "").strip().upper()
        if sym == "BTCUSD":
            cap = int(getattr(config, "BTC_DAILY_TRADE_CAP", 3) or 3)
        elif sym == "ETHUSD":
            cap = int(getattr(config, "ETH_DAILY_TRADE_CAP", 2) or 2)
        else:
            return False, ""
        if cap <= 0:
            return False, ""
        try:
            db_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
            db_path = Path(db_cfg) if db_cfg else (Path(__file__).resolve().parent / "data" / "ctrader_openapi.db")
            with sqlite3.connect(str(db_path), timeout=3) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM ctrader_deals WHERE symbol=? AND outcome NOT IN ('open','pending') AND date(execution_utc)=date('now')",
                    (sym,),
                ).fetchone()
            count = int((row or [0])[0] or 0)
            if count >= cap:
                return True, f"crypto_daily_cap:{sym} {count}>={cap} trades today"
        except Exception:
            pass
        return False, ""

    # ── Crypto MRD — BTC Microstructure Regime Detector (Phase 4) ───────────

    def _btc_mrd_check(self, direction: str) -> tuple[str, float]:
        """Returns (regime, delta_proxy) where regime is 'neutral'|'bearish_micro'|'bullish_micro'.
        Uses a longer 600s lookback to detect macro micro-regime for BTC.
        bearish_micro → suppress longs (BFLS, LOB). bullish_micro → suppress shorts (BFSS)."""
        if not bool(getattr(config, "BTC_MRD_ENABLED", True)):
            return "neutral", 0.0
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol="BTCUSD",
                    lookback_sec=int(getattr(config, "BTC_MRD_LOOKBACK_SEC", 600) or 600),
                    direction=direction,
                    confidence=70.0,
                )
                or {}
            )
        except Exception:
            return "neutral", 0.0
        if not bool(snapshot.get("ok")):
            return "neutral", 0.0
        features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        delta_proxy = float(features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(features.get("bar_volume_proxy", 0.0) or 0.0)
        min_bv = float(getattr(config, "BTC_MRD_MIN_BAR_VOLUME_PROXY", 0.20) or 0.20)
        if bar_volume_proxy < min_bv:
            return "neutral", delta_proxy  # low volume = no regime signal
        bearish_thresh = float(getattr(config, "BTC_MRD_BEARISH_DELTA_THRESHOLD", -0.05) or -0.05)
        bullish_thresh = float(getattr(config, "BTC_MRD_BULLISH_DELTA_THRESHOLD", 0.05) or 0.05)
        if delta_proxy < bearish_thresh:
            return "bearish_micro", delta_proxy  # selling flow dominant → suppress longs
        if delta_proxy > bullish_thresh:
            return "bullish_micro", delta_proxy  # buying flow dominant → suppress shorts
        return "neutral", delta_proxy

    # ── BTC Flow Short Sidecar (BFSS) — crypto clone of XAU FSS ─────────────

    def _build_btc_flow_short_sidecar_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """BTC mirror of XAU FSS: fires sell_stop below entry when real selling flow confirmed.
        Completely isolated from XAU — separate config, separate family, no shared infra."""
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "btc_scalp_flow_short_sidecar":
            return None, ""
        if not bool(getattr(config, "BTC_FSS_ENABLED", True)):
            return None, ""
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        if symbol != "BTCUSD":
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction != "short":
            return None, ""
        confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        if confidence < float(getattr(config, "BTC_FSS_MIN_CONFIDENCE", 67.0) or 67.0):
            return None, ""
        # Phase 1 guards re-applied (BFSS skips weekday builder, call directly)
        _clg_blocked, _ = self._crypto_cluster_loss_check(symbol)
        if _clg_blocked:
            return None, ""
        _cap_blocked, _ = self._crypto_daily_cap_check(symbol)
        if _cap_blocked:
            return None, ""
        # Read behavioral trigger flag
        try:
            _raw_check = dict(getattr(signal, "raw_scores", {}) or {})
            _btc_behavioral = bool(_raw_check.get("btc_behavioral_trigger") or _raw_check.get("behavioral_trigger"))
        except Exception:
            _btc_behavioral = False
        # Get microstructure flow data for BTC
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=symbol,
                    lookback_sec=int(getattr(config, "BTC_FSS_LOOKBACK_SEC", 240) or 240),
                    direction=direction,
                    confidence=confidence,
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return None, ""
        capture_features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        delta_proxy = float(capture_features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        min_dp = float(getattr(config, "BTC_FSS_MIN_DELTA_PROXY", 0.04) or 0.04)
        min_bv = float(getattr(config, "BTC_FSS_MIN_BAR_VOLUME_PROXY", 0.28) or 0.28)
        # Guard B+C (mirror of XAU): selling flow must be NEGATIVE delta_proxy
        if delta_proxy >= 0:
            return None, ""
        # Phase 4 MRD: suppress shorts if BTC macro micro-regime is bullish
        _mrd_regime, _mrd_dp = self._btc_mrd_check("short")
        if _mrd_regime == "bullish_micro":
            return None, ""
        # For behavioral trigger: require full thresholds (Guard A equivalent)
        if abs(delta_proxy) < min_dp:
            return None, ""
        if bar_volume_proxy < min_bv:
            return None, ""
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0005)
        trigger_ratio = float(getattr(config, "BTC_FSS_TRIGGER_RISK_RATIO", 0.10) or 0.10)
        stop_lift_ratio = float(getattr(config, "BTC_FSS_STOP_LIFT_RATIO", 0.28) or 0.28)
        trigger = max(base_risk * trigger_ratio, atr_eff * 0.04)
        stop_lift = trigger * stop_lift_ratio
        new_entry = entry - trigger      # sell_stop BELOW current entry
        new_stop = stop_loss - stop_lift  # widen stop slightly for the shift
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type="sell_stop")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_btc_flow_short_sidecar"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["btc_fss_snapshot"] = {
                "delta_proxy": round(delta_proxy, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "btc_behavioral_trigger": _btc_behavioral,
                "run_id": str(snapshot.get("run_id") or ""),
                "entry_mode": "sell_stop",
                "trigger": round(trigger, 4),
                "stop_lift": round(stop_lift, 4),
            }
            raw["ctrader_risk_usd_override"] = round(float(getattr(config, "BTC_FSS_CTRADER_RISK_USD", 0.65) or 0.65), 4)
            raw["mt5_ignore_open_positions"] = True
            raw["mt5_limit_allow_market_fallback"] = False
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── BTC Flow Long Sidecar (BFLS) — crypto clone of XAU FLS ─────────────

    def _build_btc_flow_long_sidecar_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """BTC mirror of XAU FLS: fires buy_stop above entry when real buying flow confirmed.
        Completely isolated from XAU — separate config, separate family, no shared infra."""
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "btc_scalp_flow_long_sidecar":
            return None, ""
        if not bool(getattr(config, "BTC_FLS_ENABLED", True)):
            return None, ""
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        if symbol != "BTCUSD":
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction != "long":
            return None, ""
        confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        if confidence < float(getattr(config, "BTC_FLS_MIN_CONFIDENCE", 67.0) or 67.0):
            return None, ""
        # Phase 1 guards
        _clg_blocked, _ = self._crypto_cluster_loss_check(symbol)
        if _clg_blocked:
            return None, ""
        _cap_blocked, _ = self._crypto_daily_cap_check(symbol)
        if _cap_blocked:
            return None, ""
        try:
            _raw_check = dict(getattr(signal, "raw_scores", {}) or {})
            _btc_behavioral = bool(_raw_check.get("btc_behavioral_trigger") or _raw_check.get("behavioral_trigger"))
        except Exception:
            _btc_behavioral = False
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=symbol,
                    lookback_sec=int(getattr(config, "BTC_FLS_LOOKBACK_SEC", 240) or 240),
                    direction=direction,
                    confidence=confidence,
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return None, ""
        capture_features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        delta_proxy = float(capture_features.get("delta_proxy", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        min_dp = float(getattr(config, "BTC_FLS_MIN_DELTA_PROXY", 0.04) or 0.04)
        min_bv = float(getattr(config, "BTC_FLS_MIN_BAR_VOLUME_PROXY", 0.28) or 0.28)
        # Guard B+C: buying flow must be POSITIVE delta_proxy
        if delta_proxy <= 0:
            return None, ""
        # Phase 4 MRD: suppress longs if BTC macro micro-regime is bearish
        _mrd_regime, _mrd_dp = self._btc_mrd_check("long")
        if _mrd_regime == "bearish_micro":
            return None, ""
        if delta_proxy < min_dp:
            return None, ""
        if bar_volume_proxy < min_bv:
            return None, ""
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            return None, ""
        atr_eff = max(base_risk, atr, entry * 0.0005)
        trigger_ratio = float(getattr(config, "BTC_FLS_TRIGGER_RISK_RATIO", 0.10) or 0.10)
        stop_lift_ratio = float(getattr(config, "BTC_FLS_STOP_LIFT_RATIO", 0.28) or 0.28)
        trigger = max(base_risk * trigger_ratio, atr_eff * 0.04)
        stop_lift = trigger * stop_lift_ratio
        new_entry = entry + trigger      # buy_stop ABOVE current entry
        new_stop = stop_loss + stop_lift  # raise stop slightly for the shift
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type="buy_stop")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_btc_flow_long_sidecar"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["btc_fls_snapshot"] = {
                "delta_proxy": round(delta_proxy, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "btc_behavioral_trigger": _btc_behavioral,
                "run_id": str(snapshot.get("run_id") or ""),
                "entry_mode": "buy_stop",
                "trigger": round(trigger, 4),
                "stop_lift": round(stop_lift, 4),
            }
            raw["ctrader_risk_usd_override"] = round(float(getattr(config, "BTC_FLS_CTRADER_RISK_USD", 0.65) or 0.65), 4)
            raw["mt5_ignore_open_positions"] = True
            raw["mt5_limit_allow_market_fallback"] = False
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    # ── BTC Range Repair (BRR) — crypto clone of XAU RR ─────────────────────

    def _build_btc_range_repair_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """BTC mirror of XAU Range Repair: probe limit entries after BTC exhaustion/range-probe setups.
        Completely isolated from XAU — separate config BTC_RANGE_REPAIR_*, no XAU infra shared."""
        family = str((candidate or {}).get("family") or "").strip().lower()
        if signal is None or family != "btc_scalp_range_repair":
            return None, ""
        if not bool(getattr(config, "BTC_RANGE_REPAIR_ENABLED", True)):
            return None, ""
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        if symbol != "BTCUSD":
            return None, ""
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return None, ""
        # Phase 1 guards
        _clg_blocked, _ = self._crypto_cluster_loss_check(symbol)
        if _clg_blocked:
            return None, ""
        _cap_blocked, _ = self._crypto_daily_cap_check(symbol)
        if _cap_blocked:
            return None, ""
        # Phase 4 MRD: suppress direction against macro regime
        _brr_mrd_regime, _ = self._btc_mrd_check(direction)
        if direction == "long" and _brr_mrd_regime == "bearish_micro":
            return None, ""
        if direction == "short" and _brr_mrd_regime == "bullish_micro":
            return None, ""
        try:
            snapshot = dict(
                live_profile_autopilot.latest_capture_feature_snapshot(
                    symbol=symbol,
                    lookback_sec=int(getattr(config, "BTC_RANGE_REPAIR_LOOKBACK_SEC", 300) or 300),
                    direction=direction,
                    confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                )
                or {}
            )
        except Exception:
            snapshot = {}
        if not bool(snapshot.get("ok")) or not bool(snapshot.get("run_id")):
            return None, ""
        capture_features = dict((snapshot.get("features") or ((snapshot.get("gate") or {}).get("features") or {})) or {})
        chart_state = dict(
            live_profile_classify_chart_state(direction, self._signal_request_context(signal), capture_features=capture_features)
            or {}
        )
        state_label = str(chart_state.get("state_label") or "").strip().lower()
        allowed_states = {
            s.strip().lower()
            for s in str(getattr(config, "BTC_RANGE_REPAIR_ALLOWED_STATES", "reversal_exhaustion,range_probe") or "").split(",")
            if s.strip()
        }
        if allowed_states and state_label not in allowed_states:
            return None, ""
        continuation_bias = abs(float(chart_state.get("continuation_bias", 0.0) or 0.0))
        rejection_ratio = float(capture_features.get("rejection_ratio", 0.0) or 0.0)
        bar_volume_proxy = float(capture_features.get("bar_volume_proxy", 0.0) or 0.0)
        delta_proxy = abs(float(capture_features.get("delta_proxy", 0.0) or 0.0))
        if continuation_bias > float(getattr(config, "BTC_RANGE_REPAIR_MAX_CONTINUATION_BIAS", 0.12) or 0.12):
            return None, ""
        if rejection_ratio < float(getattr(config, "BTC_RANGE_REPAIR_MIN_REJECTION_RATIO", 0.14) or 0.14):
            return None, ""
        if bar_volume_proxy < float(getattr(config, "BTC_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", 0.20) or 0.20):
            return None, ""
        if delta_proxy > float(getattr(config, "BTC_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", 0.14) or 0.14):
            return None, ""
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop_loss = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        atr = abs(float(getattr(signal, "atr", 0.0) or 0.0))
        base_risk = abs(entry - stop_loss)
        if entry <= 0 or stop_loss <= 0 or base_risk <= 0:
            return None, ""
        sign = 1.0 if direction == "long" else -1.0
        atr_eff = max(base_risk, atr, entry * 0.0005)
        retest = max(
            base_risk * float(getattr(config, "BTC_RANGE_REPAIR_ENTRY_RISK_RATIO", 0.10) or 0.10),
            atr_eff * float(getattr(config, "BTC_RANGE_REPAIR_ENTRY_ATR_RATIO", 0.04) or 0.04),
        )
        new_entry = entry - (retest * sign)
        new_risk = max(entry * 0.00008, base_risk * float(getattr(config, "BTC_RANGE_REPAIR_STOP_KEEP_RISK_RATIO", 0.75) or 0.75))
        new_stop = new_entry - (new_risk * sign)
        lane_signal = copy.deepcopy(signal)
        lane_signal.take_profit_1 = round(float(entry + (base_risk * sign * float(getattr(config, "BTC_RANGE_REPAIR_TP1_RR", 0.50) or 0.50))), 2)
        lane_signal.take_profit_2 = round(float(entry + (base_risk * sign * float(getattr(config, "BTC_RANGE_REPAIR_TP2_RR", 0.85) or 0.85))), 2)
        lane_signal.take_profit_3 = round(float(entry + (base_risk * sign * float(getattr(config, "BTC_RANGE_REPAIR_TP3_RR", 1.15) or 1.15))), 2)
        lane_signal.risk_reward = round(float(getattr(config, "BTC_RANGE_REPAIR_TP2_RR", 0.85) or 0.85), 2)
        shaped = self._apply_family_price_plan(lane_signal, family=family, entry=new_entry, stop_loss=new_stop, entry_type="limit")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, family)
        self._ensure_signal_trace(shaped, source=lane_source)
        follow_plan = str(chart_state.get("follow_up_plan") or "").strip().lower() or (
            "probe_repair_limit_after_exhaustion" if state_label == "reversal_exhaustion" else "fade_range_edge_with_limit_only"
        )
        try:
            raw = dict(getattr(shaped, "raw_scores", {}) or {})
            raw["persistent_canary_enabled"] = True
            raw["persistent_canary_family_enabled"] = True
            raw["persistent_canary_source"] = lane_source
            raw["persistent_canary_base_source"] = str(base_source or "").strip().lower()
            raw["experimental_family"] = True
            raw["mt5_canary_mode"] = True
            raw["strategy_family"] = family
            raw["strategy_id"] = str((candidate or {}).get("strategy_id") or "")
            raw["strategy_family_priority"] = int((candidate or {}).get("priority", 0) or 0)
            raw["strategy_family_executor"] = "scheduler_canary_btc_range_repair"
            raw["strategy_family_alias"] = self._strategy_family_alias(family)
            raw["btc_range_repair_snapshot"] = {
                "state_label": state_label,
                "follow_up_plan": follow_plan,
                "continuation_bias": round(continuation_bias, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "delta_proxy_abs": round(delta_proxy, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "run_id": str(snapshot.get("run_id") or ""),
                "entry_mode": "range_limit_repair",
            }
            raw["ctrader_risk_usd_override"] = round(float(getattr(config, "BTC_RANGE_REPAIR_CTRADER_RISK_USD", 0.55) or 0.55), 4)
            raw["mt5_ignore_open_positions"] = True
            raw["mt5_limit_allow_market_fallback"] = False
            shaped.raw_scores = raw
        except Exception:
            pass
        return shaped, lane_source

    def _build_crypto_flow_short_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """CFS: Crypto Flow Short — sell_stop shorts gated by neural short-score + RSI."""
        ctx_result, _ = self._crypto_family_common_preamble(signal, base_source=base_source, candidate=candidate, family="crypto_flow_short")
        if ctx_result is None:
            return None, ""
        ctx = ctx_result
        if not bool(getattr(config, "CRYPTO_FLOW_SHORT_ENABLED", False)):
            return None, ""
        if ctx["direction"] != "short":
            return None, ""
        allowed_symbols = set(config.get_crypto_flow_short_allowed_symbols() or set())
        if allowed_symbols and ctx["symbol"] not in allowed_symbols:
            return None, ""
        if not getattr(config, "CRYPTO_SMART_FAMILIES_24H_MODE", False):
            allowed_sessions = set(config.get_crypto_flow_short_allowed_sessions() or set())
            if allowed_sessions and not self._session_signature_matches(ctx["session_sig"], allowed_sessions):
                return None, ""
        min_conf = float(getattr(config, "CRYPTO_FLOW_SHORT_MIN_CONFIDENCE", 68.0) or 68.0)
        max_conf = float(getattr(config, "CRYPTO_FLOW_SHORT_MAX_CONFIDENCE", 85.0) or 85.0)
        if ctx["confidence"] < min_conf or ctx["confidence"] > max_conf:
            return None, ""
        min_short = float(getattr(config, "CRYPTO_FLOW_SHORT_MIN_SHORT_SCORE", 70.0) or 70.0)
        if ctx["short_score"] < min_short:
            return None, ""
        min_edge = float(getattr(config, "CRYPTO_FLOW_SHORT_MIN_EDGE", 30.0) or 30.0)
        if ctx["edge"] < min_edge:
            return None, ""
        rsi_max = float(getattr(config, "CRYPTO_FLOW_SHORT_RSI_MAX", 45.0) or 45.0)
        if ctx["rsi"] > rsi_max or ctx["rsi"] <= 0:
            return None, ""
        if bool(getattr(config, "CRYPTO_FLOW_SHORT_BLOCK_SEVERE_WINNER", True)) and ctx["winner_regime"] == "severe":
            return None, ""
        entry = ctx["entry"]
        stop_loss = ctx["stop_loss"]
        risk = abs(entry - stop_loss)
        trigger_ratio = float(getattr(config, "CRYPTO_FLOW_SHORT_BREAK_STOP_TRIGGER_RISK_RATIO", 0.10) or 0.10)
        stop_lift_ratio = float(getattr(config, "CRYPTO_FLOW_SHORT_BREAK_STOP_STOP_LIFT_RATIO", 0.30) or 0.30)
        trigger = max(risk * trigger_ratio, entry * 0.00015)
        stop_lift = trigger * stop_lift_ratio
        new_entry = entry - trigger
        new_stop = stop_loss - stop_lift
        if new_entry <= 0 or new_stop <= 0 or new_stop <= new_entry:
            return None, ""
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family="crypto_flow_short", entry=new_entry, stop_loss=new_stop, entry_type="sell_stop")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, "crypto_flow_short")
        risk_usd = float(getattr(config, f"CRYPTO_FLOW_SHORT_{ctx['symbol'][:3]}_CTRADER_RISK_USD", 0.45) or 0.45) if ctx["symbol"] == "BTCUSD" else float(getattr(config, "CRYPTO_FLOW_SHORT_ETH_CTRADER_RISK_USD", 0.20) or 0.20)
        if ctx["is_weekend"]:
            risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        return self._crypto_family_tag_and_return(signal, shaped, lane_source=lane_source, candidate=candidate, family="crypto_flow_short", risk_usd=risk_usd, ctx=ctx, extra_tags={"crypto_flow_short_neural_gate": {"short_score": ctx["short_score"], "edge": ctx["edge"], "rsi": ctx["rsi"]}, "crypto_flow_short_entry_mode": "sell_stop"})

    def _build_crypto_flow_buy_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """CFB: Crypto Flow Buy — buy_stop longs gated by neural long-score + RSI band."""
        ctx_result, _ = self._crypto_family_common_preamble(signal, base_source=base_source, candidate=candidate, family="crypto_flow_buy")
        if ctx_result is None:
            return None, ""
        ctx = ctx_result
        if not bool(getattr(config, "CRYPTO_FLOW_BUY_ENABLED", False)):
            return None, ""
        if ctx["direction"] != "long":
            return None, ""
        allowed_symbols = set(config.get_crypto_flow_buy_allowed_symbols() or set())
        if allowed_symbols and ctx["symbol"] not in allowed_symbols:
            return None, ""
        if not getattr(config, "CRYPTO_SMART_FAMILIES_24H_MODE", False):
            allowed_sessions = set(config.get_crypto_flow_buy_allowed_sessions() or set())
            if allowed_sessions and not self._session_signature_matches(ctx["session_sig"], allowed_sessions):
                return None, ""
        min_conf = float(getattr(config, "CRYPTO_FLOW_BUY_MIN_CONFIDENCE", 68.0) or 68.0)
        max_conf = float(getattr(config, "CRYPTO_FLOW_BUY_MAX_CONFIDENCE", 80.0) or 80.0)
        if ctx["confidence"] < min_conf or ctx["confidence"] > max_conf:
            return None, ""
        min_long = float(getattr(config, "CRYPTO_FLOW_BUY_MIN_LONG_SCORE", 85.0) or 85.0)
        if ctx["long_score"] < min_long:
            return None, ""
        min_edge = float(getattr(config, "CRYPTO_FLOW_BUY_MIN_EDGE", 40.0) or 40.0)
        if ctx["edge"] < min_edge:
            return None, ""
        rsi_min = float(getattr(config, "CRYPTO_FLOW_BUY_RSI_MIN", 55.0) or 55.0)
        rsi_max = float(getattr(config, "CRYPTO_FLOW_BUY_RSI_MAX", 70.0) or 70.0)
        if ctx["rsi"] < rsi_min or ctx["rsi"] > rsi_max:
            return None, ""
        if bool(getattr(config, "CRYPTO_FLOW_BUY_REQUIRE_STRONG_WINNER", True)):
            if ctx["winner_regime"] == "strong":
                pass
            elif ctx["winner_regime"] == "neutral" and bool(getattr(config, "CRYPTO_FLOW_BUY_ALLOW_NEUTRAL_WINNER", True)):
                pass
            else:
                return None, ""
        entry = ctx["entry"]
        stop_loss = ctx["stop_loss"]
        risk = abs(entry - stop_loss)
        trigger_ratio = float(getattr(config, "CRYPTO_FLOW_BUY_BREAK_STOP_TRIGGER_RISK_RATIO", 0.10) or 0.10)
        stop_lift_ratio = float(getattr(config, "CRYPTO_FLOW_BUY_BREAK_STOP_STOP_LIFT_RATIO", 0.30) or 0.30)
        trigger = max(risk * trigger_ratio, entry * 0.00015)
        stop_lift = trigger * stop_lift_ratio
        new_entry = entry + trigger
        new_stop = stop_loss + stop_lift
        if new_entry <= 0 or new_stop <= 0 or new_stop >= new_entry:
            return None, ""
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family="crypto_flow_buy", entry=new_entry, stop_loss=new_stop, entry_type="buy_stop")
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, "crypto_flow_buy")
        risk_usd = float(getattr(config, "CRYPTO_FLOW_BUY_BTC_CTRADER_RISK_USD", 0.65) or 0.65) if ctx["symbol"] == "BTCUSD" else float(getattr(config, "CRYPTO_FLOW_BUY_ETH_CTRADER_RISK_USD", 0.25) or 0.25)
        if ctx["is_weekend"]:
            risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        return self._crypto_family_tag_and_return(signal, shaped, lane_source=lane_source, candidate=candidate, family="crypto_flow_buy", risk_usd=risk_usd, ctx=ctx, extra_tags={"crypto_flow_buy_neural_gate": {"long_score": ctx["long_score"], "edge": ctx["edge"], "rsi": ctx["rsi"]}, "crypto_flow_buy_entry_mode": "buy_stop"})

    def _build_crypto_winner_confirmed_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """CWC: Crypto Winner Confirmed — fires only on strong winner regime + high edge + neural confirmation."""
        ctx_result, _ = self._crypto_family_common_preamble(signal, base_source=base_source, candidate=candidate, family="crypto_winner_confirmed")
        if ctx_result is None:
            return None, ""
        ctx = ctx_result
        if not bool(getattr(config, "CRYPTO_WINNER_CONFIRMED_ENABLED", False)):
            return None, ""
        allowed_symbols = set(config.get_crypto_winner_confirmed_allowed_symbols() or set())
        if allowed_symbols and ctx["symbol"] not in allowed_symbols:
            return None, ""
        if not getattr(config, "CRYPTO_SMART_FAMILIES_24H_MODE", False):
            allowed_sessions = set(config.get_crypto_winner_confirmed_allowed_sessions() or set())
            if allowed_sessions and not self._session_signature_matches(ctx["session_sig"], allowed_sessions):
                return None, ""
        min_conf = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_MIN_CONFIDENCE", 70.0) or 70.0)
        max_conf = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_MAX_CONFIDENCE", 80.0) or 80.0)
        if ctx["confidence"] < min_conf or ctx["confidence"] > max_conf:
            return None, ""
        if ctx["winner_regime"] != "strong":
            return None, ""
        min_wr = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_MIN_WIN_RATE", 0.62) or 0.62)
        if ctx["winner_wr"] < min_wr:
            return None, ""
        min_edge = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_MIN_EDGE", 60.0) or 60.0)
        if ctx["edge"] < min_edge:
            return None, ""
        min_np = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_MIN_NEURAL_PROB", 0.62) or 0.62)
        if ctx["neural_prob"] < min_np:
            return None, ""
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family="crypto_winner_confirmed", entry=ctx["entry"], stop_loss=ctx["stop_loss"], entry_type=ctx["entry_type"])
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, "crypto_winner_confirmed")
        risk_usd = float(getattr(config, "CRYPTO_WINNER_CONFIRMED_CTRADER_RISK_USD", 0.90) or 0.90)
        if ctx["is_weekend"]:
            risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        return self._crypto_family_tag_and_return(signal, shaped, lane_source=lane_source, candidate=candidate, family="crypto_winner_confirmed", risk_usd=risk_usd, ctx=ctx, extra_tags={"crypto_winner_confirmed_gate": {"winner_regime": ctx["winner_regime"], "winner_wr": ctx["winner_wr"], "edge": ctx["edge"], "neural_prob": ctx["neural_prob"]}})

    def _build_crypto_behavioral_retest_signal(self, signal, *, base_source: str, candidate: dict) -> tuple[object | None, str]:
        """CBR: Crypto Behavioral Retest — CHOCH_ENTRY + market-to-limit conversion for higher RR."""
        ctx_result, _ = self._crypto_family_common_preamble(signal, base_source=base_source, candidate=candidate, family="crypto_behavioral_retest")
        if ctx_result is None:
            return None, ""
        ctx = ctx_result
        if not bool(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_ENABLED", False)):
            return None, ""
        allowed_symbols = set(config.get_crypto_behavioral_retest_allowed_symbols() or set())
        if allowed_symbols and ctx["symbol"] not in allowed_symbols:
            return None, ""
        if not getattr(config, "CRYPTO_SMART_FAMILIES_24H_MODE", False):
            allowed_sessions = set(config.get_crypto_behavioral_retest_allowed_sessions() or set())
            if allowed_sessions and not self._session_signature_matches(ctx["session_sig"], allowed_sessions):
                return None, ""
        allowed_patterns = set(config.get_crypto_behavioral_retest_allowed_patterns() or set())
        if allowed_patterns and ((not ctx["pattern"]) or ctx["pattern"].lower() not in allowed_patterns):
            return None, ""
        min_conf = float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_MIN_CONFIDENCE", 72.0) or 72.0)
        max_conf = float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_MAX_CONFIDENCE", 82.0) or 82.0)
        if ctx["confidence"] < min_conf or ctx["confidence"] > max_conf:
            return None, ""
        min_np = float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_MIN_NEURAL_PROB", 0.65) or 0.65)
        if ctx["neural_prob"] < min_np:
            return None, ""
        if bool(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_BLOCK_SEVERE_WINNER", True)) and ctx["winner_regime"] == "severe":
            return None, ""
        entry = ctx["entry"]
        stop_loss = ctx["stop_loss"]
        entry_type = ctx["entry_type"]
        converted_market = False
        if entry_type == "market":
            risk = abs(entry - stop_loss)
            pullback_ratio = float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_PULLBACK_RISK_RATIO", 0.15) or 0.15)
            pullback = max(risk * pullback_ratio, entry * 0.00010)
            if ctx["direction"] == "long":
                entry = entry - pullback
            else:
                entry = entry + pullback
            entry_type = "limit"
            converted_market = True
        if entry <= 0 or stop_loss <= 0:
            return None, ""
        lane_signal = copy.deepcopy(signal)
        shaped = self._apply_family_price_plan(lane_signal, family="crypto_behavioral_retest", entry=entry, stop_loss=stop_loss, entry_type=entry_type)
        if shaped is None:
            return None, ""
        lane_source = self._strategy_family_lane_source(base_source, "crypto_behavioral_retest")
        risk_usd = float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_BTC_CTRADER_RISK_USD", 0.45) or 0.45) if ctx["symbol"] == "BTCUSD" else float(getattr(config, "CRYPTO_BEHAVIORAL_RETEST_ETH_CTRADER_RISK_USD", 0.20) or 0.20)
        if ctx["is_weekend"]:
            risk_usd *= float(getattr(config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65) or 0.65)
        return self._crypto_family_tag_and_return(signal, shaped, lane_source=lane_source, candidate=candidate, family="crypto_behavioral_retest", risk_usd=risk_usd, ctx=ctx, extra_tags={"crypto_behavioral_retest_gate": {"pattern": ctx["pattern"], "neural_prob": ctx["neural_prob"], "winner_regime": ctx["winner_regime"]}, "crypto_behavioral_retest_market_to_limit": converted_market})

    def _persistent_canary_profile(self, signal, source: str) -> dict:
        profile = {
            "enabled": False,
            "source": "",
            "base_source": "",
            "symbol": str(getattr(signal, "symbol", "") or "").strip().upper() if signal is not None else "",
            "direct_enabled": False,
            "mt5_enabled": False,
            "ctrader_enabled": False,
            "run_parallel": False,
        }
        if signal is None or (not bool(getattr(config, "PERSISTENT_CANARY_ENABLED", False))):
            return profile
        src = str(source or "").strip()
        src_l = src.lower()
        if (not src_l) or (":bypass" in src_l) or (":canary" in src_l):
            return profile
        base_source = src_l.split(":", 1)[0].strip().lower()
        allowed_sources = set(config.get_persistent_canary_allowed_sources() or set())
        if allowed_sources and ("*" not in allowed_sources) and ("all" not in allowed_sources) and (base_source not in allowed_sources):
            return profile
        direct_allowed_sources = set(getattr(config, "get_persistent_canary_direct_allowed_sources", lambda: set())() or set())
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        allowed_symbols = set(config.get_persistent_canary_allowed_symbols() or set())
        if allowed_symbols and symbol and (symbol not in allowed_symbols):
            return profile
        mt5_symbol_enabled = bool(getattr(config, "MT5_ENABLED", False))
        if symbol == "XAUUSD":
            mt5_symbol_enabled = mt5_symbol_enabled and bool(getattr(config, "MT5_EXECUTE_XAUUSD", True))
        elif symbol in {"BTCUSD", "ETHUSD"}:
            mt5_symbol_enabled = mt5_symbol_enabled and bool(getattr(config, "MT5_EXECUTE_CRYPTO", False))
        lane_source = f"{base_source}:canary"
        profile.update(
            {
                "enabled": True,
                "source": lane_source,
                "base_source": base_source,
                "symbol": symbol,
                "direct_enabled": (not direct_allowed_sources)
                or ("*" in direct_allowed_sources)
                or ("all" in direct_allowed_sources)
                or (base_source in direct_allowed_sources),
                "mt5_enabled": bool(getattr(config, "PERSISTENT_CANARY_MT5_ENABLED", True)) and bool(mt5_symbol_enabled),
                "ctrader_enabled": bool(getattr(config, "PERSISTENT_CANARY_CTRADER_ENABLED", True))
                and bool(getattr(config, "CTRADER_ENABLED", False))
                and bool(getattr(config, "CTRADER_AUTOTRADE_ENABLED", False)),
                "run_parallel": bool(getattr(config, "PERSISTENT_CANARY_RUN_PARALLEL", True)),
            }
        )
        return profile

    def _maybe_execute_persistent_canary(self, signal, source: str) -> dict:
        profile = self._persistent_canary_profile(signal, source)
        report = {
            "enabled": bool(profile.get("enabled", False)),
            "source": str(profile.get("source", "") or ""),
            "mt5": False,
            "ctrader": False,
            "family_variants": [],
        }
        if not bool(profile.get("enabled", False)) or signal is None:
            return report
        lane_source = str(profile.get("source", "") or "")
        base_source = str(profile.get("base_source", "") or "")
        symbol = str(profile.get("symbol", "") or "")

        def _prepare_copy() -> object:
            lane_signal = copy.deepcopy(signal)
            self._ensure_signal_trace(lane_signal, source=lane_source)
            try:
                raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
                raw["persistent_canary_enabled"] = True
                raw["persistent_canary_source"] = lane_source
                raw["persistent_canary_base_source"] = base_source
                raw["persistent_canary_symbol"] = symbol
                raw["mt5_canary_mode"] = True
                raw["mt5_magic_offset"] = int(getattr(config, "PERSISTENT_CANARY_MT5_MAGIC_OFFSET", 700) or 0)
                raw["mt5_ignore_open_positions"] = bool(profile.get("run_parallel", False))
                raw["mt5_extra_volume_multiplier"] = float(getattr(config, "PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER", 0.20) or 0.20)
                raw["ctrader_risk_usd_override"] = float(getattr(config, "PERSISTENT_CANARY_CTRADER_RISK_USD", 2.5) or 2.5)
                raw["mt5_limit_allow_market_fallback"] = False
                lane_signal.raw_scores = raw
            except Exception:
                pass
            if base_source == "xauusd_scheduled":
                lane_signal = self._apply_xau_scheduled_canary_market_to_limit_retest(lane_signal, lane_source=lane_source)
                lane_signal = self._apply_xau_scheduled_canary_rr_rebalance(lane_signal, lane_source=lane_source)
            return lane_signal

        if bool(profile.get("direct_enabled", False)) and bool(profile.get("mt5_enabled", False)):
            mt5_signal = _prepare_copy()
            self._maybe_execute_mt5_signal(mt5_signal, source=lane_source)
            report["mt5"] = True
        if bool(profile.get("direct_enabled", False)) and bool(profile.get("ctrader_enabled", False)):
            _canary_xau = str(symbol or "").strip().upper() in {"XAUUSD", "GOLD"}
            _canary_holiday_blocked = (
                _canary_xau
                and bool(getattr(config, "XAU_HOLIDAY_GUARD_ENABLED", True))
                and session_manager.is_xauusd_holiday()
            )
            _canary_closed_blocked = _canary_xau and not session_manager.is_xauusd_market_open()
            if _canary_holiday_blocked:
                logger.info("[CTRADER][CANARY] skipped source=%s symbol=%s reason=xauusd_market_holiday", lane_source, symbol)
            elif _canary_closed_blocked:
                logger.info("[CTRADER][CANARY] skipped source=%s symbol=%s reason=xauusd_market_closed", lane_source, symbol)
            else:
                ctr_signal = _prepare_copy()
                try:
                    result = ctrader_executor.execute_signal(ctr_signal, source=lane_source)
                    report["ctrader"] = bool(getattr(result, "ok", False) or getattr(result, "dry_run", False))
                    logger.info(
                        "[CTRADER][CANARY] %s %s -> %s (%s) %s",
                        str(getattr(result, "status", "") or ""),
                        str(getattr(result, "signal_symbol", getattr(ctr_signal, "symbol", "")) or ""),
                        str(getattr(result, "broker_symbol", "-") or "-"),
                        lane_source,
                        str(getattr(result, "message", "") or ""),
                    )
                except Exception as e:
                    logger.warning("[CTRADER][CANARY] execute failed source=%s symbol=%s err=%s", lane_source, symbol, e)
        # Pre-load directive once so the family loop can respect blocked families/sources.
        # Canary families previously bypassed _allow_ctrader_source_profile entirely.
        # Fix: mirror the EXACT same check as _allow_ctrader_source_profile (lines 979-988):
        # block if direction+entry_type matches AND family is in blocked_families OR source
        # is in blocked_sources. This keeps behavioral_v2 (pb:canary, not in blocked lists)
        # running freely while stopping MFU/TDF canary when the directive targets them.
        _xau_directive_blocked_dir = ""
        _xau_directive_blocked_etypes: set = set()
        _xau_directive_blocked_fams: set = set()
        _xau_directive_blocked_srcs: set = set()
        if symbol == "XAUUSD":
            _rt_state = self._load_trading_routing_runtime_state()
            _directive = self._active_xau_execution_directive(_rt_state)
            if _directive:
                _xau_directive_blocked_dir = str(_directive.get("blocked_direction") or "").strip().lower()
                _xau_directive_blocked_etypes = {
                    str(t).strip().lower()
                    for t in list(_directive.get("blocked_entry_types") or [])
                    if str(t).strip()
                }
                _xau_directive_blocked_fams = {
                    str(f).strip().lower()
                    for f in list(_directive.get("blocked_families") or [])
                    if str(f).strip()
                }
                _xau_directive_blocked_srcs = {
                    str(s).strip().lower()
                    for s in list(_directive.get("blocked_sources") or [])
                    if str(s).strip()
                }
        family_candidates = self._load_strategy_family_candidates(symbol=symbol, base_source=base_source)
        for candidate in list(family_candidates or []):
            try:
                _r0 = dict(getattr(signal, "raw_scores", {}) or {})
                _r0.pop("family_canary_skip", None)
                signal.raw_scores = _r0
            except Exception:
                pass
            family_signal, family_source = self._build_family_canary_signal(signal, base_source=base_source, candidate=candidate)
            if family_signal is None or not family_source:
                if symbol == "XAUUSD":
                    st, rsn = self._classify_family_canary_build_miss(signal, candidate)
                    self._store_xau_family_canary_gate_journal(
                        signal,
                        candidate=candidate,
                        base_source=base_source,
                        lane_source="",
                        gate_stage=st,
                        reason=rsn,
                    )
                continue
            # Mirror _allow_ctrader_source_profile directive check for canary families.
            # Blocks only families/sources explicitly listed in the directive — same logic,
            # same scope. behavioral_v2 (xau_scalp_pullback_limit) is never in blocked_families.
            if symbol == "XAUUSD" and _xau_directive_blocked_dir:
                _sig_dir = str(getattr(family_signal, "direction", "") or "").strip().lower()
                _sig_etype = str(getattr(family_signal, "entry_type", "") or "").strip().lower()
                _cand_fam = str((candidate or {}).get("family") or "").strip().lower()
                if (
                    _sig_dir == _xau_directive_blocked_dir
                    and (not _xau_directive_blocked_etypes or _sig_etype in _xau_directive_blocked_etypes)
                    and ((_cand_fam and _cand_fam in _xau_directive_blocked_fams) or family_source in _xau_directive_blocked_srcs)
                ):
                    self._store_xau_family_canary_gate_journal(
                        signal,
                        candidate=candidate,
                        base_source=base_source,
                        lane_source=str(family_source or ""),
                        gate_stage="trading_manager_directive",
                        reason=(
                            f"directive_block:dir={_sig_dir}:etype={_sig_etype}"
                            f":family={_cand_fam}:src={family_source}"
                        ),
                    )
                    continue
            family_row = {
                "family": str((candidate or {}).get("family") or ""),
                "strategy_id": str((candidate or {}).get("strategy_id") or ""),
                "source": family_source,
                "mt5": False,
                "ctrader": False,
                "experimental": bool((candidate or {}).get("experimental")),
            }
            if bool(profile.get("mt5_enabled", False)) and not bool(family_row.get("experimental")):
                self._maybe_execute_mt5_signal(family_signal, source=family_source)
                family_row["mt5"] = True
            if bool(profile.get("ctrader_enabled", False)):
                try:
                    result = ctrader_executor.execute_signal(copy.deepcopy(family_signal), source=family_source)
                    family_row["ctrader"] = bool(getattr(result, "ok", False) or getattr(result, "dry_run", False))
                    logger.info(
                        "[CTRADER][CANARY][FAMILY] %s %s -> %s (%s) %s",
                        str(getattr(result, "status", "") or ""),
                        str(getattr(result, "signal_symbol", getattr(family_signal, "symbol", "")) or ""),
                        str(getattr(result, "broker_symbol", "-") or "-"),
                        family_source,
                        str(getattr(result, "message", "") or ""),
                    )
                except Exception as e:
                    logger.warning("[CTRADER][CANARY][FAMILY] execute failed source=%s symbol=%s err=%s", family_source, symbol, e)
            report["family_variants"].append(family_row)
        return report

    @staticmethod
    def _is_bypass_position_comment(comment: str, suffix: str) -> bool:
        c = str(comment or "").strip().lower()
        s = str(suffix or "").strip().lower()
        if not c or not s:
            return False
        return (f":{s}:" in c) or c.endswith(f":{s}") or (f":{s}" in c)

    @staticmethod
    def _is_pytest_runtime() -> bool:
        return bool(str(os.getenv("PYTEST_CURRENT_TEST", "") or "").strip())

    @staticmethod
    def _is_bypass_position(quote_row: dict, suffix: str, bypass_magic: int) -> bool:
        row = dict(quote_row or {})
        comment = str(row.get("comment", "") or "")
        if DexterScheduler._is_bypass_position_comment(comment, suffix):
            return True
        try:
            m = int(row.get("magic", 0) or 0)
        except Exception:
            m = 0
        if int(bypass_magic or 0) > 0 and m == int(bypass_magic):
            return True
        return False

    def _signal_symbol_key(self, signal, result: MT5ExecutionResult | None = None) -> str:
        raw = ""
        try:
            if result is not None:
                raw = str(getattr(result, "broker_symbol", "") or getattr(result, "signal_symbol", "") or "").strip().upper()
        except Exception:
            raw = ""
        if not raw:
            try:
                raw = str(getattr(signal, "symbol", "") or "").strip().upper()
            except Exception:
                raw = ""
        if raw and ("/" in raw):
            if raw.endswith("/USDT"):
                raw = f"{raw[:-5]}USD"
            raw = raw.replace("/", "")
        if result is None and raw:
            try:
                mapped = str(mt5_executor.resolve_symbol(raw) or "").strip().upper()
                if mapped:
                    raw = mapped
            except Exception:
                pass
        return raw

    @staticmethod
    def _extract_retcode_from_message(msg: str) -> int | None:
        s = str(msg or "")
        marker = "retcode="
        if marker not in s:
            return None
        tail = s.split(marker, 1)[1]
        digits = []
        for ch in tail:
            if ch.isdigit():
                digits.append(ch)
            elif digits:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    @staticmethod
    def _repeat_guard_fingerprint(result: MT5ExecutionResult) -> str:
        status = str(getattr(result, "status", "") or "").lower()
        rc = getattr(result, "retcode", None)
        code = None
        try:
            if rc is not None:
                code = int(rc)
        except Exception:
            code = None
        if code is None:
            code = DexterScheduler._extract_retcode_from_message(str(getattr(result, "message", "") or ""))
        if code is not None:
            return f"{status}:{int(code)}"
        msg = str(getattr(result, "message", "") or "").strip().lower()
        msg = " ".join(msg.split())[:80]
        return f"{status}:{msg}"

    def _repeat_guard_enabled(self) -> bool:
        return bool(getattr(config, "MT5_REPEAT_ERROR_GUARD_ENABLED", True)) and bool(getattr(config, "MT5_ENABLED", False))

    def _load_mt5_repeat_guard_state(self) -> None:
        if not bool(getattr(config, "MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", False)):
            return
        path = self._mt5_repeat_guard_path
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("symbols"), dict):
                self._mt5_repeat_guard_state = {"version": int(raw.get("version", 1) or 1), "symbols": dict(raw.get("symbols") or {})}
        except Exception as e:
            logger.debug("[MT5] repeat-guard load skipped: %s", e)

    def _save_mt5_repeat_guard_state(self) -> None:
        if not bool(getattr(config, "MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", False)):
            return
        now = self._now_ts()
        # prevent disk-thrashing while still keeping state durable enough
        if (now - self._mt5_repeat_guard_last_save_ts) < 1.0:
            return
        try:
            tmp = self._mt5_repeat_guard_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(self._mt5_repeat_guard_state, ensure_ascii=True, separators=(",", ":")),
                encoding="utf-8",
            )
            tmp.replace(self._mt5_repeat_guard_path)
            self._mt5_repeat_guard_last_save_ts = now
        except Exception as e:
            logger.debug("[MT5] repeat-guard save skipped: %s", e)

    def _repeat_guard_cleanup_row(self, row: dict, now_ts: float) -> dict:
        window_sec = max(60, int(getattr(config, "MT5_REPEAT_ERROR_GUARD_WINDOW_MIN", 30) or 30) * 60)
        events = []
        for ev in list(row.get("events", []) or []):
            ts = float((ev or {}).get("ts", 0.0) or 0.0)
            if ts <= 0:
                continue
            if (now_ts - ts) > float(window_sec):
                continue
            events.append(dict(ev or {}))
        row["events"] = events[-50:]
        lock_until = float(row.get("lock_until", 0.0) or 0.0)
        if lock_until > 0 and lock_until <= now_ts:
            row["lock_until"] = 0.0
            row["lock_reason"] = ""
        row["updated_ts"] = float(now_ts)
        return row

    def _repeat_guard_allow(self, signal, source: str) -> tuple[bool, dict]:
        if not self._repeat_guard_enabled():
            return True, {"enabled": False, "reason": "disabled"}
        sym = self._signal_symbol_key(signal)
        if not sym:
            return True, {"enabled": True, "reason": "no_symbol"}
        now_ts = self._now_ts()
        with self._mt5_repeat_guard_lock:
            symbols = self._mt5_repeat_guard_state.setdefault("symbols", {})
            row = self._repeat_guard_cleanup_row(dict(symbols.get(sym) or {}), now_ts)
            symbols[sym] = row
            lock_until = float(row.get("lock_until", 0.0) or 0.0)
            if lock_until > now_ts:
                remain = max(1.0, lock_until - now_ts)
                return False, {
                    "enabled": True,
                    "reason": "repeat_error_lock",
                    "symbol": sym,
                    "remaining_sec": round(remain, 1),
                    "source": str(source or ""),
                    "fingerprint": str(row.get("lock_fingerprint", "") or ""),
                    "lock_reason": str(row.get("lock_reason", "") or ""),
                }
        return True, {"enabled": True, "reason": "ok", "symbol": sym}

    def _repeat_guard_on_result(self, signal, result: MT5ExecutionResult, source: str) -> None:
        if not self._repeat_guard_enabled():
            return
        sym = self._signal_symbol_key(signal, result=result)
        if not sym:
            return
        status = str(getattr(result, "status", "") or "").lower()
        severe = status in {"rejected", "error", "invalid_stops"}
        now_ts = self._now_ts()
        changed = False
        lock_triggered = False
        lock_meta = {}
        with self._mt5_repeat_guard_lock:
            symbols = self._mt5_repeat_guard_state.setdefault("symbols", {})
            row = self._repeat_guard_cleanup_row(dict(symbols.get(sym) or {}), now_ts)

            if bool(getattr(result, "ok", False)):
                if row.get("events") or float(row.get("lock_until", 0.0) or 0.0) > 0:
                    row["events"] = []
                    row["lock_until"] = 0.0
                    row["lock_reason"] = ""
                    row["lock_fingerprint"] = ""
                    changed = True
            elif severe:
                fp = self._repeat_guard_fingerprint(result)
                row.setdefault("events", [])
                row["events"].append(
                    {
                        "ts": float(now_ts),
                        "status": status,
                        "fingerprint": fp,
                        "source": str(source or ""),
                    }
                )
                row = self._repeat_guard_cleanup_row(row, now_ts)
                hits = sum(1 for ev in list(row.get("events", []) or []) if str((ev or {}).get("fingerprint", "")) == fp)
                max_hits = max(1, int(getattr(config, "MT5_REPEAT_ERROR_GUARD_MAX_HITS", 2) or 2))
                if hits >= max_hits:
                    lock_min = max(1, int(getattr(config, "MT5_REPEAT_ERROR_GUARD_LOCK_MIN", 45) or 45))
                    row["lock_until"] = float(now_ts + (lock_min * 60))
                    row["lock_reason"] = f"{status} repeated {hits}x in window"
                    row["lock_fingerprint"] = fp
                    lock_triggered = True
                    lock_meta = {
                        "symbol": sym,
                        "lock_min": lock_min,
                        "hits": hits,
                        "fingerprint": fp,
                        "source": str(source or ""),
                    }
                changed = True

            symbols[sym] = row
        if changed:
            self._save_mt5_repeat_guard_state()
        if lock_triggered:
            logger.warning(
                "[MT5] repeat-error guard locked %s for %sm (hits=%s fp=%s src=%s)",
                lock_meta.get("symbol", sym),
                lock_meta.get("lock_min", "-"),
                lock_meta.get("hits", "-"),
                lock_meta.get("fingerprint", "-"),
                lock_meta.get("source", "-"),
            )

    def _cleanup_mt5_repeat_guard_state(self) -> dict:
        now_ts = self._now_ts()
        removed: list[str] = []
        expired: list[str] = []
        changed = False
        with self._mt5_repeat_guard_lock:
            symbols = dict((self._mt5_repeat_guard_state or {}).get("symbols") or {})
            cleaned_symbols: dict[str, dict] = {}
            for sym, raw_row in symbols.items():
                row_before = dict(raw_row or {})
                had_lock = float(row_before.get("lock_until", 0.0) or 0.0) > now_ts
                row = self._repeat_guard_cleanup_row(row_before, now_ts)
                if had_lock and float(row.get("lock_until", 0.0) or 0.0) <= now_ts:
                    expired.append(str(sym))
                if list(row.get("events") or []) or float(row.get("lock_until", 0.0) or 0.0) > 0:
                    cleaned_symbols[str(sym)] = row
                else:
                    removed.append(str(sym))
                if row != row_before:
                    changed = True
            if cleaned_symbols != symbols:
                self._mt5_repeat_guard_state["symbols"] = cleaned_symbols
                changed = True
        if changed:
            self._save_mt5_repeat_guard_state()
        return {
            "ok": True,
            "changed": bool(changed),
            "expired": sorted(expired),
            "removed": sorted(removed),
            "active_symbols": sorted(list((self._mt5_repeat_guard_state or {}).get("symbols", {}).keys())),
        }

    def _run_mt5_readiness_check(self) -> dict:
        if not bool(getattr(config, "MT5_ENABLED", False)):
            return {"ok": False, "status": "disabled", "message": "mt5 disabled"}
        report = {
            "ok": False,
            "status": "",
            "message": "",
            "tradable": {},
            "gate": {},
            "repeat_guard": {},
            "suggest": {},
        }
        try:
            allow_symbols = sorted(list(config.get_mt5_allow_symbols() or {"XAUUSD", "BTCUSD", "ETHUSD", "GBPUSD", "EURUSD", "USDJPY"}))
        except Exception:
            allow_symbols = ["XAUUSD", "BTCUSD", "ETHUSD", "GBPUSD", "EURUSD", "USDJPY"]
        try:
            report["repeat_guard"] = self._cleanup_mt5_repeat_guard_state()
        except Exception as e:
            report["repeat_guard"] = {"ok": False, "error": str(e)}
        try:
            tradable = mt5_executor.filter_tradable_signal_symbols(allow_symbols)
            report["tradable"] = tradable
            if list(tradable.get("unmapped") or []):
                report["suggest"] = mt5_executor.suggest_symbol_map(list(tradable.get("unmapped") or []))
        except Exception as e:
            report["tradable"] = {"ok": False, "error": str(e)}
        try:
            if hasattr(mt5_autopilot_core, "_last_gate_snapshot") and isinstance(mt5_autopilot_core._last_gate_snapshot, dict):
                mt5_autopilot_core._last_gate_snapshot.clear()
            gate = mt5_autopilot_core._compute_gate_snapshot(ttl_sec=0)
            report["gate"] = gate
        except Exception as e:
            report["gate"] = {"ok": False, "error": str(e)}
        tradable_ok = bool((report.get("tradable") or {}).get("ok"))
        unresolved = int(len(list((report.get("tradable") or {}).get("unmapped") or [])))
        gate_ok = bool((report.get("gate") or {}).get("ok"))
        daily_loss_abs = float(((report.get("gate") or {}).get("lane_metrics", {}) or {}).get("main", {}).get("daily_loss_abs", (report.get("gate") or {}).get("daily_loss_abs", 0.0)) or 0.0)
        report["ok"] = bool(tradable_ok and gate_ok and unresolved == 0)
        report["status"] = "ready" if bool(report["ok"]) else "check_required"
        report["message"] = (
            f"tradable={len(list((report.get('tradable') or {}).get('tradable') or []))}/{len(allow_symbols)} "
            f"unmapped={unresolved} main_daily_loss={daily_loss_abs:.2f}"
        )
        logger.info(
            "[MT5] readiness %s | tradable=%s/%s unmapped=%s main_daily_loss=%.2f repeat_guard_active=%s",
            report["status"],
            len(list((report.get("tradable") or {}).get("tradable") or [])),
            len(allow_symbols),
            unresolved,
            daily_loss_abs,
            len(list((report.get("repeat_guard") or {}).get("active_symbols") or [])),
        )
        return report

    def _raw_confidence(self, signal) -> float:
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            return float(raw_scores.get("confidence_pre_neural", float(getattr(signal, "confidence", 0.0))))
        except Exception:
            return float(getattr(signal, "confidence", 0.0))

    def _apply_neural_soft_adjustment(self, signal, source: str) -> dict:
        """
        Soft-adjust confidence before sending a signal.
        This never blocks the signal path.
        """
        if signal is None:
            return {"applied": False, "reason": "no_signal"}
        try:
            apply_entry_template_hints(signal)
            apply_entry_template_conf_tailwind(signal)
        except Exception:
            pass
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            if raw_scores.get("neural_confidence_adjusted"):
                return {"applied": False, "reason": "already_adjusted"}

            base_conf = float(getattr(signal, "confidence", 0.0))
            adjust = neural_brain.confidence_adjustment(signal, source=source)
            prob = adjust.get("prob")
            raw_scores["confidence_pre_neural"] = round(base_conf, 3)
            if prob is not None:
                raw_scores["neural_probability"] = round(float(prob), 4)
            raw_scores["neural_adjust_reason"] = str(adjust.get("reason", "unknown"))
            reason_study = dict(adjust.get("reason_study") or {})
            if reason_study:
                raw_scores["reason_study_applied"] = bool(reason_study.get("applied", False))
                raw_scores["reason_study_reason"] = str(reason_study.get("reason", "") or "")
                raw_scores["reason_study_delta"] = round(float(reason_study.get("delta", 0.0) or 0.0), 3)
                raw_scores["reason_study_matched_tags"] = [
                    str(item.get("tag") or "")
                    for item in list(reason_study.get("matched_tags") or [])
                    if str(item.get("tag") or "")
                ][:5]

            if adjust.get("applied"):
                adjusted = float(adjust.get("adjusted_confidence", base_conf))
                delta = float(adjust.get("delta", adjusted - base_conf))
                signal.confidence = round(adjusted, 1)
                raw_scores["confidence_post_neural"] = round(signal.confidence, 3)
                raw_scores["neural_adjust_delta"] = round(delta, 3)
                if prob is not None:
                    msg = f"🧠 Neural prob: {float(prob) * 100:.1f}%"
                    if delta >= 0.15 and msg not in signal.reasons:
                        signal.reasons.append(f"{msg} (confidence boosted)")
                    elif delta <= -0.15 and msg not in signal.warnings:
                        signal.warnings.append(f"{msg} (confidence tempered)")
                reason_delta = float(reason_study.get("delta", 0.0) or 0.0) if reason_study else 0.0
                matched_tags = list(reason_study.get("matched_tags") or []) if reason_study else []
                if abs(reason_delta) >= 0.15 and matched_tags:
                    top_tag = str(dict(matched_tags[0]).get("tag") or "")
                    if top_tag:
                        note = f"History memory: {top_tag}"
                        if reason_delta > 0 and note not in signal.reasons:
                            signal.reasons.append(f"{note} (historically strong)")
                        elif reason_delta < 0 and note not in signal.warnings:
                            signal.warnings.append(f"{note} (historically weak)")
            else:
                raw_scores["confidence_post_neural"] = round(base_conf, 3)
                now_ts = time.time()
                if (now_ts - self._last_neural_soft_adjust_skip_log_ts) >= 900:
                    logger.info(
                        "[NeuralBrain] soft-adjust skipped (%s)",
                        str(adjust.get("reason", "unknown")),
                    )
                    self._last_neural_soft_adjust_skip_log_ts = now_ts

            raw_scores["neural_confidence_adjusted"] = True
            signal.raw_scores = raw_scores
            return adjust
        except Exception as e:
            logger.debug("[NeuralBrain] soft-adjust error: %s", e)
            return {"applied": False, "reason": "exception"}

    def _neural_execution_filter_ready(self) -> tuple[bool, dict]:
        if not (config.NEURAL_BRAIN_ENABLED and config.NEURAL_BRAIN_EXECUTION_FILTER):
            return False, {"ready": False, "reason": "execution_filter_disabled"}
        try:
            state = neural_brain.execution_filter_status()
        except Exception as e:
            return False, {"ready": False, "reason": f"status_error:{e}"}

        ready = bool(state.get("ready", False))
        if not ready:
            now_ts = time.time()
            if (now_ts - self._last_neural_filter_not_ready_log_ts) >= 600:
                logger.info(
                    "[MT5] Neural execution filter armed but not ready (%s): "
                    "samples=%s/%s val_acc=%.3f/%.3f age_h=%s",
                    str(state.get("reason", "unknown")),
                    int(state.get("samples", 0) or 0),
                    int(state.get("required_samples", 0) or 0),
                    float(state.get("val_accuracy", 0.0) or 0.0),
                    float(state.get("required_val_accuracy", 0.0) or 0.0),
                    str(state.get("age_hours", "-")),
                )
                self._last_neural_filter_not_ready_log_ts = now_ts
        return ready, state

    def _symbol_override_candidates(self, signal) -> list[str]:
        candidates: list[str] = []
        try:
            sym = str(getattr(signal, "symbol", "") or "").strip().upper()
        except Exception:
            sym = ""
        if sym:
            candidates.append(sym)
            try:
                mapped = str(mt5_executor.resolve_symbol(sym) or "").strip().upper()
            except Exception:
                mapped = ""
            if mapped and mapped not in candidates:
                candidates.append(mapped)
        return candidates

    @staticmethod
    def _lookup_symbol_override(candidates: list[str], overrides: dict) -> tuple[float | None, str]:
        for idx, c in enumerate(candidates):
            if not c or c not in overrides:
                continue
            try:
                val = float(overrides[c])
            except Exception:
                continue
            if idx == 0:
                return val, f"symbol_override:{c}"
            base_sym = candidates[0] if candidates else ""
            return val, f"symbol_override_mapped:{c}<-{base_sym}"
        return None, ""

    def _neural_min_prob_for_signal(self, signal, source: str) -> tuple[float, str]:
        base = float(getattr(config, "NEURAL_BRAIN_MIN_PROB", 0.55) or 0.55)
        reason = "global"
        src = str(source or "").strip().lower()
        sym = ""
        candidates: list[str] = []
        if src == "fx":
            fx_min = float(getattr(config, "NEURAL_BRAIN_MIN_PROB_FX", base) or base)
            base = fx_min
            reason = "fx_default"
        try:
            candidates = self._symbol_override_candidates(signal)
            sym = candidates[0] if candidates else str(getattr(signal, "symbol", "") or "").strip().upper()
            overrides = config.get_neural_min_prob_symbol_overrides()
            v, why = self._lookup_symbol_override(candidates or ([sym] if sym else []), overrides)
            if v is not None:
                base = float(v)
                reason = why or reason
        except Exception:
            pass
        if src == 'fx' and sym:
            try:
                learned = mt5_autopilot_core.fx_learned_neural_threshold(sym, base_threshold=float(base))
                if bool(learned.get('applied')):
                    base = float(learned.get('threshold', base) or base)
                    reason = f"{reason}+learned:{int(learned.get('samples',0) or 0)}"
            except Exception:
                pass
        # Clamp to sane probability range
        base = max(0.0, min(0.99, float(base)))
        return base, reason

    def _attach_neural_filter_meta(self, signal, prob: float | None, min_prob: float, min_prob_reason: str, extra: dict | None = None) -> None:
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            if prob is not None:
                raw_scores["neural_probability"] = round(float(prob), 4)
            raw_scores["mt5_neural_min_prob"] = round(float(min_prob), 4)
            raw_scores["mt5_neural_min_prob_reason"] = str(min_prob_reason or "")
            if isinstance(extra, dict):
                raw_scores.update({k: v for k, v in extra.items()})
            signal.raw_scores = raw_scores
        except Exception:
            pass

    def _is_bot_like_snapshot_position(self, pos: dict) -> bool:
        try:
            pref = str(getattr(config, "MT5_COMMENT_PREFIX", "DEX") or "").strip().upper()
            comment = str((pos or {}).get("comment", "") or "").strip().upper()
            if not pref:
                return True
            if comment.startswith(pref):
                return True
            if ":" in comment:
                head = str(comment.split(":", 1)[0] or "").strip().upper()
                if head and pref.startswith(head):
                    return True
            return False
        except Exception:
            return False

    def _canary_scope_position_limit_ok(self, signal, max_positions_per_symbol: int) -> tuple[bool, str]:
        max_pos = max(1, int(max_positions_per_symbol or 1))
        try:
            snap = mt5_executor.open_positions_snapshot(
                signal_symbol=str(getattr(signal, "symbol", "") or ""),
                limit=200,
            )
            if not bool(snap.get("connected", False)):
                return True, "position_snapshot_unavailable"
            positions = list(snap.get("positions", []) or [])
            if bool(getattr(config, "MT5_POSITION_LIMITS_BOT_ONLY", False)):
                positions = [p for p in positions if self._is_bot_like_snapshot_position(p)]
            if len(positions) >= max_pos:
                return False, f"canary symbol position cap {len(positions)}/{max_pos}"
        except Exception as e:
            return True, f"position_check_error:{e}"
        return True, "ok"

    def _maybe_apply_neural_canary_override(
        self,
        signal,
        source: str,
        prob: float | None,
        min_prob: float,
        min_prob_reason: str,
    ) -> tuple[bool, dict]:
        info = {
            "applied": False,
            "allowed": False,
            "reason": "not_evaluated",
            "volume_cap": None,
            "policy_age_sec": None,
            "allow_low": None,
            "allow_high": None,
            "confidence_min": None,
            "require_force_mode": None,
        }
        if prob is None:
            info["reason"] = "no_prob"
            return False, info
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        scope = neural_gate_learning_loop.get_scope_policy(symbol, source)
        if not scope:
            info["reason"] = "no_scope_policy"
            return False, info
        info["policy_age_sec"] = int(scope.get("policy_age_sec", 0) or 0)
        if not bool(scope.get("active", False)):
            info["reason"] = str(scope.get("reason", "scope_inactive") or "scope_inactive")
            return False, info

        allow_low = float(scope.get("allow_low", min_prob) or min_prob)
        allow_high = float(scope.get("allow_high", min_prob) or min_prob)
        min_conf = float(scope.get("min_confidence", 0.0) or 0.0)
        require_force = bool(scope.get("require_force_mode", False))
        volume_cap = max(0.05, min(1.0, float(scope.get("volume_multiplier_cap", 0.25) or 0.25)))
        max_pos = max(1, int(scope.get("max_positions_per_symbol", 1) or 1))
        info.update(
            {
                "allow_low": round(allow_low, 4),
                "allow_high": round(allow_high, 4),
                "confidence_min": round(min_conf, 3),
                "require_force_mode": require_force,
                "volume_cap": round(volume_cap, 4),
            }
        )
        p = float(prob)
        conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        if p >= float(min_prob):
            info["reason"] = "above_min"
            return False, info
        if not (allow_low <= p < allow_high):
            info["reason"] = "outside_canary_band"
            return False, info
        if conf < min_conf:
            info["reason"] = f"below_canary_conf:{conf:.1f}<{min_conf:.1f}"
            return False, info
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        if require_force and (not bool(raw_scores.get("scalp_force_mode", False))):
            info["reason"] = "force_mode_required"
            return False, info
        pos_ok, pos_reason = self._canary_scope_position_limit_ok(signal, max_positions_per_symbol=max_pos)
        if not pos_ok:
            info["reason"] = pos_reason
            return False, info

        info["applied"] = True
        info["allowed"] = True
        info["reason"] = "canary_allow"
        info["base_min_prob"] = round(float(min_prob), 4)
        info["base_min_prob_reason"] = str(min_prob_reason or "")
        try:
            raw_scores["mt5_neural_canary_applied"] = True
            raw_scores["mt5_neural_canary_reason"] = str(info["reason"])
            raw_scores["mt5_neural_canary_allow_low"] = round(float(allow_low), 4)
            raw_scores["mt5_neural_canary_allow_high"] = round(float(allow_high), 4)
            raw_scores["mt5_neural_canary_volume_cap"] = round(float(volume_cap), 4)
            raw_scores["mt5_neural_canary_policy_age_sec"] = int(info["policy_age_sec"] or 0)
            signal.raw_scores = raw_scores
            if hasattr(signal, "warnings") and isinstance(signal.warnings, list):
                signal.warnings.append(
                    f"Neural canary override: p={p:.3f} in [{allow_low:.3f},{allow_high:.3f}) cap={volume_cap:.2f}"
                )
        except Exception:
            pass
        now_ts = time.time()
        if (now_ts - self._last_neural_gate_policy_log_ts) >= 300:
            logger.info(
                "[NeuralGate] canary allow %s src=%s p=%.3f base_min=%.3f band=[%.3f,%.3f) cap=%.2f age=%ss",
                symbol,
                source,
                p,
                float(min_prob),
                float(allow_low),
                float(allow_high),
                float(volume_cap),
                int(info["policy_age_sec"] or 0),
            )
            self._last_neural_gate_policy_log_ts = now_ts
        return True, info

    def _apply_neural_canary_volume_cap(self, signal, volume_multiplier: float | None, canary_info: dict | None) -> float | None:
        info = dict(canary_info or {})
        if not bool(info.get("applied", False)):
            return volume_multiplier
        cap = max(0.05, min(1.0, float(info.get("volume_cap", 0.25) or 0.25)))
        before = 1.0 if volume_multiplier is None else float(volume_multiplier)
        after = min(before, cap)
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            raw_scores["mt5_neural_canary_volume_before"] = round(float(before), 4)
            raw_scores["mt5_neural_canary_volume_after"] = round(float(after), 4)
            signal.raw_scores = raw_scores
        except Exception:
            pass
        return float(after)

    def _get_live_neural_probability(self, signal, source: str) -> tuple[float | None, dict]:
        """
        Resolve a live neural probability with symbol/family quality gating.
        Fallback order:
          1) SymbolNeuralBrain (quality-gated)
          2) Global NeuralBrain
        """
        meta = {
            "mt5_neural_prob_source": "none",
            "mt5_neural_symbol_model_source": "none",
            "mt5_neural_symbol_quality_ready": False,
            "mt5_neural_symbol_quality_reason": "n/a",
            "mt5_neural_symbol_quality_samples": 0,
            "mt5_neural_symbol_quality_required_samples": 0,
            "mt5_neural_symbol_quality_val_acc": 0.0,
            "mt5_neural_symbol_quality_required_val_acc": 0.0,
            "mt5_neural_used_global_fallback": False,
        }
        try:
            from learning.symbol_neural_brain import symbol_neural_brain

            p_sym, model_src, q = symbol_neural_brain.predict_for_signal_with_quality(
                signal,
                source=source,
                enforce_quality=True,
            )
            if isinstance(q, dict):
                meta["mt5_neural_symbol_quality_ready"] = bool(q.get("ready", False))
                meta["mt5_neural_symbol_quality_reason"] = str(q.get("reason", "unknown"))
                meta["mt5_neural_symbol_quality_samples"] = int(q.get("samples", 0) or 0)
                meta["mt5_neural_symbol_quality_required_samples"] = int(q.get("required_samples", 0) or 0)
                meta["mt5_neural_symbol_quality_val_acc"] = float(q.get("val_accuracy", 0.0) or 0.0)
                meta["mt5_neural_symbol_quality_required_val_acc"] = float(q.get("required_val_accuracy", 0.0) or 0.0)
            meta["mt5_neural_symbol_model_source"] = str(model_src or "none")
            if p_sym is not None:
                meta["mt5_neural_prob_source"] = str(model_src or "symbol_or_family")
                return float(p_sym), meta
        except Exception as e:
            meta["mt5_neural_symbol_quality_reason"] = f"symbol_quality_error:{e}"

        try:
            p_global = neural_brain.predict_probability(signal, source=source)
            if p_global is not None:
                meta["mt5_neural_used_global_fallback"] = True
                meta["mt5_neural_prob_source"] = "global"
                return float(p_global), meta
        except Exception as e:
            meta["mt5_neural_global_error"] = str(e)
        return None, meta


    def _maybe_apply_fx_neural_soft_filter(self, signal, source: str, prob: float | None, min_prob: float) -> tuple[bool, dict]:
        info = {"applied": False, "hard_block": False, "reason": "n/a", "penalty": 0.0}
        src = str(source or "").strip().lower()
        if src != 'fx':
            info['reason'] = 'not_fx'
            return False, info
        if prob is None:
            info['reason'] = 'no_prob'
            return False, info
        if not bool(getattr(config, 'NEURAL_BRAIN_FX_SOFT_FILTER_ENABLED', False)):
            info['reason'] = 'disabled'
            return False, info
        p = float(prob)
        base_low = float(getattr(config, 'NEURAL_BRAIN_FX_SOFT_FILTER_BAND_LOW', 0.43) or 0.43)
        base_high = float(getattr(config, 'NEURAL_BRAIN_FX_SOFT_FILTER_BAND_HIGH', 0.48) or 0.48)
        low = float(base_low)
        high = float(base_high)
        max_penalty = float(getattr(config, 'NEURAL_BRAIN_FX_SOFT_FILTER_MAX_CONF_PENALTY', 4.0) or 4.0)
        if high < low:
            low, high = high, low
            base_low, base_high = low, high
        sym = str(getattr(signal, 'symbol', '') or '').strip().upper()
        try:
            candidates = self._symbol_override_candidates(signal)
            low_over, low_reason = self._lookup_symbol_override(candidates, config.get_neural_fx_soft_filter_band_low_symbol_overrides())
            if low_over is not None:
                low = float(low_over)
                info['band_low_override_reason'] = low_reason
            high_over, high_reason = self._lookup_symbol_override(candidates, config.get_neural_fx_soft_filter_band_high_symbol_overrides())
            if high_over is not None:
                high = float(high_over)
                info['band_high_override_reason'] = high_reason
            pen_over, pen_reason = self._lookup_symbol_override(candidates, config.get_neural_fx_soft_filter_max_penalty_symbol_overrides())
            if pen_over is not None:
                max_penalty = float(pen_over)
                info['max_penalty_override_reason'] = pen_reason
        except Exception:
            pass
        if high < low:
            low, high = min(low, high), max(low, high)
        if sym and bool(getattr(config, 'NEURAL_BRAIN_FX_SOFT_FILTER_LEARNED_BAND_ENABLED', False)):
            try:
                learned = mt5_autopilot_core.fx_learned_neural_soft_band(
                    sym,
                    base_low=float(low),
                    base_high=float(high),
                    ref_threshold=float(min_prob),
                )
                info['learned_band_reason'] = str(learned.get('reason', ''))
                info['learned_band_samples'] = int(learned.get('samples', 0) or 0)
                if bool(learned.get('applied')):
                    low = float(learned.get('low', low) or low)
                    high = float(learned.get('high', high) or high)
                    info['learned_band_applied'] = True
                else:
                    info['learned_band_applied'] = False
            except Exception as e:
                info['learned_band_applied'] = False
                info['learned_band_reason'] = f'exception:{e}'
        if p < low:
            info['hard_block'] = True
            info['reason'] = f'below_soft_band:{low:.2f}'
            info['band_low'] = round(float(low), 4)
            info['band_high'] = round(float(high), 4)
            return False, info
        if p >= min_prob:
            info['reason'] = 'above_min'
            info['band_low'] = round(float(low), 4)
            info['band_high'] = round(float(high), 4)
            return False, info
        if p > high:
            info['reason'] = 'above_soft_band'
            info['band_low'] = round(float(low), 4)
            info['band_high'] = round(float(high), 4)
            return False, info
        # Soft band fallback: degrade confidence instead of hard blocking.
        try:
            span = max(0.0001, float(min_prob) - low)
            ratio = max(0.0, min(1.0, (float(min_prob) - p) / span))
            penalty = round(max_penalty * ratio, 2)
            old_conf = float(getattr(signal, 'confidence', 0.0) or 0.0)
            new_conf = max(0.0, round(old_conf - penalty, 1))
            signal.confidence = new_conf
            try:
                if hasattr(signal, 'warnings') and isinstance(signal.warnings, list):
                    signal.warnings.append(f'FX neural soft-filter: p={p:.2f}, conf {old_conf:.1f}->{new_conf:.1f}')
            except Exception:
                pass
            info.update({
                'applied': True,
                'reason': 'soft_penalty',
                'penalty': penalty,
                'old_conf': round(old_conf, 3),
                'new_conf': round(new_conf, 3),
                'band_low': round(low, 3),
                'band_high': round(high, 3),
                'base_band_low': round(base_low, 3),
                'base_band_high': round(base_high, 3),
            })
            logger.info('[MT5] FX neural soft filter %s p=%.2f min=%.2f conf %.1f->%.1f penalty=%.2f band=[%.2f,%.2f]%s',
                        str(getattr(signal, 'symbol', '') or '-'), p, float(min_prob), old_conf, new_conf, penalty,
                        float(low), float(high),
                        (f" learned(n={int(info.get('learned_band_samples',0) or 0)})" if info.get('learned_band_applied') else ''))
            return True, info
        except Exception as e:
            info['reason'] = f'exception:{e}'
            return False, info

    @staticmethod
    def _is_xau_symbol(symbol: str) -> bool:
        token = str(symbol or "").strip().upper()
        return ("XAU" in token) or ("GOLD" in token)

    def _check_macro_rumor_trade_guard(self, signal) -> tuple[bool, str, dict]:
        meta = {"enabled": bool(getattr(config, "MACRO_NEWS_RUMOR_FILTER_ENABLED", True)), "blocked": False}
        if not meta["enabled"]:
            return False, "disabled", meta
        try:
            lookback_h = max(1, int(getattr(config, "MACRO_NEWS_LOOKBACK_HOURS", 24) or 24))
            symbol = str(self._signal_symbol_key(signal) or getattr(signal, "symbol", "") or "").strip().upper()
            max_age_min = max(10, int(getattr(config, "MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN", 120) or 120))
            min_score = max(1, int(getattr(config, "MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE", 8) or 8))
            max_src_q = float(getattr(config, "MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY", 0.75) or 0.75)
            if self._is_xau_symbol(symbol):
                max_age_min = max(
                    10,
                    int(getattr(config, "MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN_XAUUSD", max_age_min) or max_age_min),
                )
                min_score = max(
                    1,
                    int(getattr(config, "MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE_XAUUSD", min_score) or min_score),
                )
                max_src_q = float(
                    getattr(config, "MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY_XAUUSD", max_src_q) or max_src_q
                )
            max_src_q = max(0.0, min(1.0, float(max_src_q)))
            meta.update(
                {
                    "symbol": symbol,
                    "thresholds": {
                        "max_age_min": int(max_age_min),
                        "min_score": int(min_score),
                        "max_source_quality": round(float(max_src_q), 4),
                    },
                }
            )
            now_utc = datetime.now(timezone.utc)
            heads = macro_news.high_impact_headlines(hours=min(lookback_h, 12), min_score=max(1, min_score - 2), limit=16)
            if not heads:
                return False, "clear", meta
            xau_themes = {
                "GEOPOLITICS",
                "OIL_ENERGY_SHOCK",
                "TARIFF_TRADE",
                "FED_POLICY",
                "INFLATION",
                "LABOR_GROWTH",
                "TRUMP_POLICY",
            }
            flagged = []
            for h in heads:
                age_min = max(0.0, (now_utc - getattr(h, "published_utc", now_utc)).total_seconds() / 60.0)
                if age_min > float(max_age_min):
                    continue
                state = str(getattr(h, "verification", "unverified") or "unverified").strip().lower()
                if state not in {"rumor", "mixed"}:
                    continue
                score = int(getattr(h, "score", 0) or 0)
                if score < min_score:
                    continue
                src_q = float(getattr(h, "source_quality", 0.5) or 0.5)
                if src_q > float(max_src_q):
                    continue
                themes_up = {str(t or "").strip().upper() for t in (getattr(h, "themes", []) or []) if str(t or "").strip()}
                if self._is_xau_symbol(symbol) and themes_up and not themes_up.intersection(xau_themes):
                    continue
                severity = float(score) + max(0.0, (float(max_src_q) - src_q) * 4.0) + (0.8 if state == "mixed" else 0.0)
                flagged.append((severity, score, age_min, h, themes_up))
            if not flagged:
                return False, "clear", meta
            flagged.sort(key=lambda x: (x[0], x[1], -x[2]), reverse=True)
            sev, score, age_min, top_h, themes_up = flagged[0]
            reason = (
                f"rumor_guard state={str(getattr(top_h, 'verification', 'rumor'))} "
                f"score={score} srcQ={float(getattr(top_h, 'source_quality', 0.5) or 0.5):.2f} "
                f"age={age_min:.0f}m themes={','.join(sorted(list(themes_up))[:3]) or '-'}"
            )
            meta.update(
                {
                    "blocked": True,
                    "reason": reason,
                    "headline_id": str(getattr(top_h, "headline_id", "") or ""),
                    "title": str(getattr(top_h, "title", "") or "")[:180],
                    "symbol": symbol,
                    "severity": round(float(sev), 3),
                }
            )
            return True, reason, meta
        except Exception as e:
            logger.debug("[Scheduler] macro rumor trade guard skipped: %s", e)
            return False, "error", meta

    def _xau_event_shock_state(self) -> dict:
        out = {"enabled": bool(getattr(config, "XAU_EVENT_SHOCK_MODE_ENABLED", True)), "active": False, "kill_switch": False}
        if not out["enabled"]:
            out["reason"] = "disabled"
            return out
        try:
            tm_state = dict((trading_manager_agent._load_state() or {}).get("xau_shock_profile") or {})
        except Exception:
            tm_state = {}
        try:
            lookback_h = max(1, int(getattr(config, "XAU_EVENT_SHOCK_LOOKBACK_HOURS", 6) or 6))
            max_age_min = max(10, int(getattr(config, "XAU_EVENT_SHOCK_MAX_AGE_MIN", 180) or 180))
            min_score = max(1, int(getattr(config, "XAU_EVENT_SHOCK_MIN_SCORE", 8) or 8))
            min_src_q = float(getattr(config, "XAU_EVENT_SHOCK_MIN_SOURCE_QUALITY", 0.75) or 0.75)
            kill_floor = max(min_score, int(getattr(config, "XAU_EVENT_SHOCK_KILL_SWITCH_SCORE", 12) or 12))
            kill_confirmed_only = bool(getattr(config, "XAU_EVENT_SHOCK_KILL_SWITCH_CONFIRMED_ONLY", True))
            kill_themes = {str(x or "").strip().upper() for x in (config.get_xau_event_shock_kill_switch_themes() or set())}
            sensitive_themes = {
                "GEOPOLITICS",
                "OIL_ENERGY_SHOCK",
                "TARIFF_TRADE",
                "TRUMP_POLICY",
                "FED_POLICY",
                "INFLATION",
                "LABOR_GROWTH",
            }
            now_utc = datetime.now(timezone.utc)
            heads = macro_news.high_impact_headlines(hours=lookback_h, min_score=min_score, limit=12)
            cands = []
            for h in heads:
                age_min = max(0.0, (now_utc - getattr(h, "published_utc", now_utc)).total_seconds() / 60.0)
                if age_min > float(max_age_min):
                    continue
                score = int(getattr(h, "score", 0) or 0)
                src_q = float(getattr(h, "source_quality", 0.5) or 0.5)
                if score < min_score or src_q < float(min_src_q):
                    continue
                verification = str(getattr(h, "verification", "unverified") or "unverified").strip().lower()
                if verification in {"rumor", "mixed"}:
                    continue
                themes_up = {str(t or "").strip().upper() for t in (getattr(h, "themes", []) or []) if str(t or "").strip()}
                if themes_up and not themes_up.intersection(sensitive_themes):
                    continue
                shock_score = float(score) + max(0.0, (src_q - 0.70) * 4.0) + (1.2 if verification == "confirmed" else 0.0)
                cands.append((shock_score, age_min, h, themes_up, verification, src_q, score))
            if not cands:
                if str(tm_state.get("status") or "") == "active":
                    out.update(
                        {
                            "active": True,
                            "kill_switch": False,
                            "reason": f"manager_{str(tm_state.get('mode') or 'active')}",
                            "manager_mode": str(tm_state.get("mode") or ""),
                            "manager_reason": str(tm_state.get("reason") or ""),
                        }
                    )
                    return out
                out["reason"] = "clear"
                return out
            cands.sort(key=lambda x: (x[0], -x[1]), reverse=True)
            shock_score, age_min, h, themes_up, verification, src_q, score = cands[0]
            kill_switch = bool(shock_score >= float(kill_floor))
            if kill_switch and kill_themes:
                kill_switch = bool(themes_up.intersection(kill_themes))
            if kill_switch and kill_confirmed_only:
                kill_switch = bool(verification == "confirmed")
            out.update(
                {
                    "active": True,
                    "kill_switch": bool(kill_switch),
                    "shock_score": round(float(shock_score), 3),
                    "age_min": round(float(age_min), 1),
                    "headline_id": str(getattr(h, "headline_id", "") or ""),
                    "title": str(getattr(h, "title", "") or "")[:180],
                    "themes": sorted(list(themes_up))[:5],
                    "verification": verification,
                    "source": str(getattr(h, "source", "") or ""),
                    "source_quality": round(float(src_q), 3),
                    "score": int(score),
                }
            )
            if str(tm_state.get("status") or "") == "active":
                out["manager_mode"] = str(tm_state.get("mode") or "")
                out["manager_reason"] = str(tm_state.get("reason") or "")
            out["reason"] = "kill_switch" if kill_switch else "active"
            return out
        except Exception as e:
            logger.debug("[Scheduler] XAU event-shock state skipped: %s", e)
            out["reason"] = "error"
            return out

    def _xau_scheduled_news_guard(self) -> dict:
        """
        Pre/post blocking for scheduled high-impact USD news events.
        Tier-1 (NFP/FOMC/CPI/PPI/GDP) → kill_switch=True (pre=45min / post=30min).
        Tier-2 other high-impact USD → active=True only, size reduction (pre=30min / post=15min).
        Uses economic_calendar feed (ForexFactory XML, cached 5min).
        """
        out: dict = {
            "enabled": bool(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_ENABLED", True)),
            "active": False,
            "kill_switch": False,
        }
        if not out["enabled"]:
            out["reason"] = "disabled"
            return out
        try:
            pre_min = max(5, int(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_PRE_MIN", 30) or 30))
            post_min = max(5, int(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_POST_MIN", 15) or 15))
            t1_pre_min = max(5, int(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_TIER1_PRE_MIN", 45) or 45))
            t1_post_min = max(5, int(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_TIER1_POST_MIN", 30) or 30))
            raw_t1 = str(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_TIER1_EVENTS", "") or "").strip()
            if raw_t1:
                t1_keywords: set[str] = {k.strip().lower() for k in raw_t1.split(",") if k.strip()}
            else:
                t1_keywords = {
                    "non-farm", "nonfarm", "non farm",
                    "fomc", "federal funds rate",
                    "cpi", "consumer price index",
                    "core pce", "pce price",
                }
            now_utc = datetime.now(timezone.utc)
            max_pre = max(t1_pre_min, pre_min)
            max_post = max(t1_post_min, post_min)
            all_events = economic_calendar.fetch_events()
            hits: list[tuple] = []
            for ev in all_events:
                if str(ev.currency or "").upper() != "USD":
                    continue
                if str(ev.impact or "").lower() != "high":
                    continue
                delta_min = (ev.time_utc - now_utc).total_seconds() / 60.0
                # positive = future (pre-window), negative = past (post-window)
                if delta_min > max_pre or delta_min < -max_post:
                    continue
                hits.append((ev, delta_min))
            if not hits:
                out["reason"] = "clear"
                return out
            best_t1: tuple | None = None
            best_t2: tuple | None = None
            for ev, delta_min in hits:
                title_lower = str(ev.title or "").lower()
                is_t1 = any(kw in title_lower for kw in t1_keywords)
                if is_t1:
                    if -t1_post_min <= delta_min <= t1_pre_min:
                        if best_t1 is None or abs(delta_min) < abs(best_t1[1]):
                            best_t1 = (ev, delta_min)
                else:
                    if -post_min <= delta_min <= pre_min:
                        if best_t2 is None or abs(delta_min) < abs(best_t2[1]):
                            best_t2 = (ev, delta_min)
            if best_t1:
                ev, delta_min = best_t1
                phase = "PRE" if delta_min >= 0 else "POST"
                out.update({
                    "active": True,
                    "kill_switch": True,
                    "tier": "tier1",
                    "event_title": str(ev.title or ""),
                    "event_time_utc": ev.time_utc.isoformat(),
                    "delta_min": round(delta_min, 1),
                    "phase": phase,
                    "reason": f"tier1_{phase.lower()}_event",
                })
                return out
            if best_t2:
                ev, delta_min = best_t2
                phase = "PRE" if delta_min >= 0 else "POST"
                out.update({
                    "active": True,
                    "kill_switch": False,
                    "tier": "tier2",
                    "event_title": str(ev.title or ""),
                    "event_time_utc": ev.time_utc.isoformat(),
                    "delta_min": round(delta_min, 1),
                    "phase": phase,
                    "reason": f"tier2_{phase.lower()}_event",
                })
                return out
            out["reason"] = "out_of_window"
            return out
        except Exception as e:
            logger.debug("[Scheduler] XAU scheduled news guard skipped: %s", e)
            out["reason"] = "error"
            return out

    @staticmethod
    def _xau_guard_transition_state_record(news_freeze: dict, shock: dict) -> dict:
        events = [str(item or "").strip() for item in list((news_freeze or {}).get("events") or []) if str(item or "").strip()]
        return {
            "news_freeze_active": bool((news_freeze or {}).get("active", False)),
            "news_freeze_event": str(events[0] if events else ""),
            "news_freeze_nearest_min": int((news_freeze or {}).get("nearest_min", -1) or -1),
            "news_freeze_window_min": int((news_freeze or {}).get("window_min", 0) or 0),
            "kill_switch_active": bool((shock or {}).get("kill_switch", False)),
            "kill_switch_title": str((shock or {}).get("title", "") or ""),
            "kill_switch_shock_score": float((shock or {}).get("shock_score", 0.0) or 0.0),
            "kill_switch_source": str((shock or {}).get("source", "") or ""),
            "kill_switch_verification": str((shock or {}).get("verification", "") or ""),
        }

    @staticmethod
    def _xau_guard_transition_target_ids() -> list[int]:
        ids: set[int] = set()
        try:
            ids |= set(access_manager.get_admin_ids())
        except Exception:
            ids |= set(config.get_admin_ids())
        if (not ids) and str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").lstrip("-").isdigit():
            ids.add(int(config.TELEGRAM_CHAT_ID))
        return sorted(int(cid) for cid in ids if int(cid))

    def _publish_xau_guard_transition_alert(self, payload: dict) -> dict:
        row = dict(payload or {})
        text = "XAU guard | state changed"
        try:
            text = str(notifier.format_xau_guard_transition_alert(row) or text)
        except Exception:
            logger.debug("[Scheduler] XAU guard transition format failed", exc_info=True)
        logger.info("[Scheduler] %s", text)
        sent = 0
        try:
            from notifier.admin_bot import admin_bot

            for chat_id in self._xau_guard_transition_target_ids():
                try:
                    admin_bot._send_text(int(chat_id), text)
                    sent += 1
                except Exception:
                    logger.debug(
                        "[Scheduler] XAU guard transition admin send failed chat_id=%s",
                        chat_id,
                        exc_info=True,
                    )
        except Exception:
            logger.debug("[Scheduler] XAU guard transition admin bot unavailable", exc_info=True)
        return {
            "text": text,
            "sent": sent,
        }

    def _check_post_sl_reversal_signal(self) -> bool:
        """
        Check XAUUSD M1 for sweep + reversal pattern and fire a market re-entry
        directly into the main cTrader lane if confirmed.

        Runs every ~30s inside _run_xau_guard_transition_watch.
        Governed by POST_SL_REVERSAL_COOLDOWN_SECONDS to prevent over-trading.
        Returns True if a signal was dispatched.
        """
        if not bool(getattr(config, "POST_SL_REVERSAL_ENABLED", False)):
            return False
        if bool(getattr(config, "XAU_HOLIDAY_GUARD_ENABLED", True)) and session_manager.is_xauusd_holiday():
            return False
        if not session_manager.is_xauusd_market_open():
            return False
        cooldown = float(getattr(config, "POST_SL_REVERSAL_COOLDOWN_SECONDS", 300.0) or 300.0)
        if (time.time() - self._post_sl_reversal_last_fired_ts) < cooldown:
            return False
        try:
            sweep = scalping_scanner.detect_xau_sweep_reversal()
        except Exception as e:
            logger.debug("[PostSLReversal] detect error: %s", e)
            return False
        if not bool(sweep.get("confirmed")):
            logger.debug("[PostSLReversal] no pattern: %s", sweep.get("reason", "-"))
            return False
        direction = str(sweep.get("direction") or "long").strip().lower()
        sweep_level = float(sweep.get("sweep_level") or 0.0)
        current_price = float(sweep.get("current_close") or 0.0)
        atr = float(sweep.get("atr") or 1.0)
        wick_ratio = float(sweep.get("sweep_wick_ratio") or 0.0)
        if current_price <= 0 or atr <= 0:
            return False
        # ── Sharpness guard — block sweep reversal in knife microstructure ──
        if bool(getattr(config, "XAU_ENTRY_SHARPNESS_ENABLED", True)):
            try:
                sweep_snap = dict(live_profile_autopilot.latest_capture_feature_snapshot(symbol="XAUUSD", lookback_sec=int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240), direction=direction, confidence=float(getattr(config, "POST_SL_REVERSAL_CONFIDENCE", 74.0) or 74.0)) or {})
                sweep_features = dict((sweep_snap.get("gate") or {}).get("features") or sweep_snap.get("features") or {})
                if sweep_features:
                    from analysis.entry_sharpness import compute_entry_sharpness_score as _compute_sharpness
                    sweep_sharpness = _compute_sharpness(sweep_features, direction, micro_vol_scale=float(getattr(config, "XAU_ENTRY_SHARPNESS_MICRO_VOL_SCALE", 0.025) or 0.025), max_spread_expansion=float(getattr(config, "XAU_ENTRY_SHARPNESS_MAX_SPREAD_EXPANSION", 1.20) or 1.20))
                    if int(sweep_sharpness.get("sharpness_score", 50) or 50) < max(1, int(getattr(config, "XAU_ENTRY_SHARPNESS_KNIFE_THRESHOLD", 30) or 30)):
                        logger.info("[PostSLReversal] blocked by sharpness knife score=%s", sweep_sharpness.get("sharpness_score"))
                        return False
            except Exception:
                pass
        sl_buf = atr * float(getattr(config, "POST_SL_REVERSAL_SL_BUFFER_ATR", 0.20) or 0.20)
        tp1_r = float(getattr(config, "POST_SL_REVERSAL_TP1_R", 1.5) or 1.5)
        tp2_r = float(getattr(config, "POST_SL_REVERSAL_TP2_R", 2.5) or 2.5)
        tp3_r = float(getattr(config, "POST_SL_REVERSAL_TP3_R", 3.5) or 3.5)
        conf = float(getattr(config, "POST_SL_REVERSAL_CONFIDENCE", 74.0) or 74.0)
        if direction == "long":
            sl = sweep_level - sl_buf
            risk = max(current_price - sl, 0.5)
            tp1 = current_price + risk * tp1_r
            tp2 = current_price + risk * tp2_r
            tp3 = current_price + risk * tp3_r
        else:
            sl = sweep_level + sl_buf
            risk = max(sl - current_price, 0.5)
            tp1 = current_price - risk * tp1_r
            tp2 = current_price - risk * tp2_r
            tp3 = current_price - risk * tp3_r
        from analysis.signals import TradeSignal
        sig = TradeSignal()
        sig.symbol = "XAUUSD"
        sig.direction = direction
        sig.confidence = conf
        sig.entry = current_price
        sig.entry_type = "market"
        sig.stop_loss = round(sl, 2)
        sig.take_profit_1 = round(tp1, 2)
        sig.take_profit_2 = round(tp2, 2)
        sig.take_profit_3 = round(tp3, 2)
        sig.atr = round(atr, 3)
        sig.pattern = f"SWEEP_REVERSAL_{direction.upper()}"
        sig.raw_scores = {
            "sweep_reversal": True,
            "sweep_level": round(sweep_level, 2),
            "sweep_wick_ratio": round(wick_ratio, 3),
            "post_sl_reversal_signal": True,
        }
        logger.info(
            "[PostSLReversal] sweep confirmed dir=%s sweep_level=%.2f wick=%.2f entry=%.2f sl=%.2f tp1=%.2f",
            direction, sweep_level, wick_ratio, current_price, sl, tp1,
        )
        result = self._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")
        if result is not None:
            self._post_sl_reversal_last_fired_ts = time.time()
            try:
                notifier.send_alert(
                    f"\U0001f9f2 [Sweep Reversal] {direction.upper()} XAUUSD\n"
                    f"Entry: {current_price:.2f} | SL: {sl:.2f} | TP1: {tp1:.2f}\n"
                    f"Sweep @ {sweep_level:.2f} | Wick {wick_ratio:.0%}"
                )
            except Exception:
                pass
            return True
        return False

    def _run_xau_guard_transition_watch(self, force: bool = False) -> dict:
        if (not bool(getattr(config, "XAU_GUARD_TRANSITION_ALERT_ENABLED", True))) and (not force):
            return {"enabled": False, "status": "disabled"}
        checked_utc = datetime.now(timezone.utc).isoformat()
        try:
            news_freeze = dict(xauusd_scanner._news_freeze_context() or {})
        except Exception as e:
            logger.debug("[Scheduler] XAU news-freeze state watch skipped: %s", e)
            news_freeze = {
                "enabled": bool(getattr(config, "XAUUSD_NEWS_FREEZE_ENABLED", True)),
                "active": False,
                "nearest_min": -1,
                "window_min": int(getattr(config, "XAUUSD_NEWS_FREEZE_WINDOW_MIN", 20) or 20),
                "events": [],
            }
        shock = dict(self._xau_event_shock_state() or {})
        current = self._xau_guard_transition_state_record(news_freeze, shock)
        previous = dict(self._xau_guard_transition_state or {})
        self._xau_guard_transition_state = dict(current)
        if not previous:
            return {"enabled": True, "status": "initialized", "checked_utc": checked_utc, "current": current}

        alerts = []
        if bool(previous.get("news_freeze_active", False)) != bool(current.get("news_freeze_active", False)):
            ref = current if bool(current.get("news_freeze_active", False)) else previous
            alerts.append(
                {
                    "kind": "news_freeze",
                    "action": "activated" if bool(current.get("news_freeze_active", False)) else "cleared",
                    "checked_utc": checked_utc,
                    "title": str(ref.get("news_freeze_event", "") or ""),
                    "nearest_min": int(ref.get("news_freeze_nearest_min", -1) or -1),
                    "window_min": int(ref.get("news_freeze_window_min", 0) or 0),
                }
            )
        if bool(previous.get("kill_switch_active", False)) != bool(current.get("kill_switch_active", False)):
            ref = current if bool(current.get("kill_switch_active", False)) else previous
            alerts.append(
                {
                    "kind": "kill_switch",
                    "action": "activated" if bool(current.get("kill_switch_active", False)) else "cleared",
                    "checked_utc": checked_utc,
                    "title": str(ref.get("kill_switch_title", "") or ""),
                    "shock_score": float(ref.get("kill_switch_shock_score", 0.0) or 0.0),
                    "source": str(ref.get("kill_switch_source", "") or ""),
                    "verification": str(ref.get("kill_switch_verification", "") or ""),
                }
            )
        normal_after_clear = (not bool(current.get("news_freeze_active", False))) and (not bool(current.get("kill_switch_active", False)))
        clear_alerts = [dict(item) for item in alerts if str(item.get("action") or "").strip().lower() == "cleared"]
        if normal_after_clear and clear_alerts:
            primary_clear = next((dict(item) for item in clear_alerts if str(item.get("kind") or "") == "news_freeze"), dict(clear_alerts[0]))
            primary_clear["normal_after_clear"] = True
            alerts = [dict(item) for item in alerts if str(item.get("action") or "").strip().lower() != "cleared"]
            alerts.append(primary_clear)
        dispatched = []
        for payload in list(alerts):
            row = dict(payload or {})
            if row.get("action") == "cleared":
                row["normal_after_clear"] = bool(row.get("normal_after_clear", False))
            try:
                publish_meta = self._publish_xau_guard_transition_alert(row)
            except Exception:
                logger.debug("[Scheduler] XAU guard transition alert send failed", exc_info=True)
                publish_meta = {"text": "", "sent": 0}
            row["summary_text"] = str(publish_meta.get("text", "") or "")
            row["admin_sent"] = int(publish_meta.get("sent", 0) or 0)
            dispatched.append(row)
        try:
            self._check_post_sl_reversal_signal()
        except Exception:
            logger.debug("[PostSLReversal] check error", exc_info=True)
        return {
            "enabled": True,
            "status": "alerted" if dispatched else "unchanged",
            "checked_utc": checked_utc,
            "previous": previous,
            "current": current,
            "alerts": dispatched,
        }

    def _apply_xau_event_shock_trade_controls(self, signal, source: str, volume_multiplier: float | None) -> tuple[MT5ExecutionResult | None, float | None]:
        symbol = str(self._signal_symbol_key(signal) or getattr(signal, "symbol", "") or "").strip().upper()
        if not self._is_xau_symbol(symbol):
            return None, volume_multiplier
        state = self._xau_event_shock_state()
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["xau_event_shock_enabled"] = bool(state.get("enabled", False))
            raw["xau_event_shock_active"] = bool(state.get("active", False))
            raw["xau_event_shock_kill_switch"] = bool(state.get("kill_switch", False))
            if state.get("shock_score") is not None:
                raw["xau_event_shock_score"] = float(state.get("shock_score", 0.0) or 0.0)
            if state.get("headline_id"):
                raw["xau_event_shock_headline_id"] = str(state.get("headline_id"))
            signal.raw_scores = raw
        except Exception:
            pass
        # --- Scheduled news guard: eco-calendar pre/post USD event blocking ---
        sng_state = self._xau_scheduled_news_guard()
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["xau_news_guard_enabled"] = bool(sng_state.get("enabled", False))
            raw["xau_news_guard_active"] = bool(sng_state.get("active", False))
            raw["xau_news_guard_kill_switch"] = bool(sng_state.get("kill_switch", False))
            if sng_state.get("event_title"):
                raw["xau_news_guard_event"] = str(sng_state.get("event_title") or "")[:120]
            if sng_state.get("delta_min") is not None:
                raw["xau_news_guard_delta_min"] = float(sng_state.get("delta_min") or 0.0)
            if sng_state.get("tier"):
                raw["xau_news_guard_tier"] = str(sng_state.get("tier") or "")
            signal.raw_scores = raw
        except Exception:
            pass
        if bool(sng_state.get("kill_switch", False)):
            _sng_tag = (
                f"{sng_state.get('tier','?')} {sng_state.get('phase','?')} "
                f"{float(sng_state.get('delta_min', 0.0) or 0.0):.0f}min "
                f"{str(sng_state.get('event_title', '?') or '?')[:60]!r}"
            )
            logger.info(
                "[Scheduler] XAU guard | SCHEDULED NEWS KILL | %s | source=%s",
                _sng_tag, source,
            )
            blocked = MT5ExecutionResult(
                ok=False,
                status="guard_blocked",
                message=f"xau scheduled news kill-switch ({_sng_tag})",
                signal_symbol=str(getattr(signal, "symbol", "") or ""),
            )
            return blocked, volume_multiplier
        # Early exit: neither reactive shock nor scheduled news guard is active
        if not bool(state.get("active", False)) and not bool(sng_state.get("active", False)):
            return None, volume_multiplier
        # Tier-2 only (news guard active, reactive shock inactive) → size reduction, no TP change
        if not bool(state.get("active", False)):
            _sng_mult = max(0.05, min(1.0, float(getattr(config, "XAU_SCHEDULED_NEWS_GUARD_SIZE_MULT", 0.50) or 0.50)))
            _base = 1.0 if volume_multiplier is None else float(volume_multiplier)
            _new_vol = round(_base * _sng_mult, 4)
            _sng_log = (
                f"{sng_state.get('tier','?')} {sng_state.get('phase','?')} "
                f"{float(sng_state.get('delta_min', 0.0) or 0.0):.0f}min "
                f"{str(sng_state.get('event_title', '?') or '?')[:50]!r}"
            )
            logger.info(
                "[Scheduler] XAU guard | SCHEDULED NEWS ACTIVE | size_mult=%.2f new_vol=%.4f | %s | source=%s",
                _sng_mult, _new_vol, _sng_log, source,
            )
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
                raw["xau_news_guard_size_mult"] = float(_sng_mult)
                raw["xau_news_guard_volume_after"] = float(_new_vol)
                signal.raw_scores = raw
            except Exception:
                pass
            return None, _new_vol
        state_tag = (
            f"shock={float(state.get('shock_score', 0.0) or 0.0):.2f} "
            f"srcQ={float(state.get('source_quality', 0.0) or 0.0):.2f} "
            f"ver={str(state.get('verification', '-'))}"
        )
        if bool(state.get("kill_switch", False)):
            blocked = MT5ExecutionResult(
                ok=False,
                status="guard_blocked",
                message=f"xau_event_shock kill-switch active ({state_tag})",
                signal_symbol=str(getattr(signal, "symbol", "") or ""),
            )
            return blocked, volume_multiplier
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        stop = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        risk = abs(entry - stop)
        if risk > 0:
            rr1 = max(0.30, float(getattr(config, "XAU_EVENT_SHOCK_TP1_RR", 0.70) or 0.70))
            rr2 = max(rr1 + 0.10, float(getattr(config, "XAU_EVENT_SHOCK_TP2_RR", 1.10) or 1.10))
            rr3 = max(rr2 + 0.10, float(getattr(config, "XAU_EVENT_SHOCK_TP3_RR", 1.60) or 1.60))
            direction = str(getattr(signal, "direction", "") or "").lower()
            if direction == "long":
                signal.take_profit_1 = float(entry + (risk * rr1))
                signal.take_profit_2 = float(entry + (risk * rr2))
                signal.take_profit_3 = float(entry + (risk * rr3))
            elif direction == "short":
                signal.take_profit_1 = float(entry - (risk * rr1))
                signal.take_profit_2 = float(entry - (risk * rr2))
                signal.take_profit_3 = float(entry - (risk * rr3))
            signal.risk_reward = round(float(rr2), 2)
        base_mult = 1.0 if volume_multiplier is None else float(volume_multiplier)
        shock_size_mult = max(0.05, min(1.0, float(getattr(config, "XAU_EVENT_SHOCK_SIZE_MULT", 0.45) or 0.45)))
        new_mult = round(base_mult * shock_size_mult, 4)
        try:
            if hasattr(signal, "warnings") and isinstance(signal.warnings, list):
                signal.warnings.append(f"⚠️ XAU event-shock mode active ({state_tag})")
            if hasattr(signal, "reasons") and isinstance(signal.reasons, list):
                signal.reasons.append("🛡️ Event shock control: reduced size + faster TP ladder")
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["xau_event_shock_size_mult"] = float(shock_size_mult)
            raw["xau_event_shock_volume_before"] = float(base_mult)
            raw["xau_event_shock_volume_after"] = float(new_mult)
            raw["xau_event_shock_tp_profile"] = "fast"
            raw["xau_event_shock_source"] = str(source or "")
            signal.raw_scores = raw
        except Exception:
            pass
        return None, new_mult

    def _maybe_execute_mt5_signal(self, signal, source: str) -> None:
        if not config.MT5_ENABLED:
            return
        bypass = self._resolve_mt5_bypass_profile(signal, source)
        exec_source = str(bypass.get("source") or source)
        self._ensure_signal_trace(signal, source=exec_source)
        if bool(bypass.get("enabled")):
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["mt5_bypass_test_enabled"] = True
                raw_scores["mt5_bypass_source"] = str(exec_source)
                raw_scores["mt5_bypass_skip_neural_filter"] = bool(bypass.get("skip_neural_filter", False))
                raw_scores["mt5_bypass_skip_risk_governor"] = bool(bypass.get("skip_risk_governor", False))
                raw_scores["mt5_bypass_skip_confidence"] = bool(bypass.get("skip_mt5_confidence", False))
                raw_scores["mt5_bypass_ignore_open_positions"] = bool(bypass.get("ignore_open_positions", False))
                raw_scores["mt5_bypass_magic_offset"] = int(bypass.get("magic_offset", 0) or 0)
                signal.raw_scores = raw_scores
            except Exception:
                pass
        self._apply_mt5_lane_limit_policy(signal, exec_source=exec_source)
        guard_ok, guard = self._repeat_guard_allow(signal, source=exec_source)
        if not guard_ok:
            blocked = MT5ExecutionResult(
                ok=False,
                status="skipped",
                message=(
                    f"repeat-error guard active for {str(guard.get('symbol', '?'))}: "
                    f"{str(guard.get('remaining_sec', '?'))}s remaining"
                ),
                signal_symbol=str(getattr(signal, "symbol", "") or ""),
            )
            self._handle_mt5_result(signal, blocked, source=exec_source)
            return
        rumor_block, rumor_reason, rumor_meta = self._check_macro_rumor_trade_guard(signal)
        if rumor_block:
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["macro_rumor_guard_blocked"] = True
                raw_scores["macro_rumor_guard_reason"] = str(rumor_reason or "")
                if rumor_meta.get("headline_id"):
                    raw_scores["macro_rumor_guard_headline_id"] = str(rumor_meta.get("headline_id"))
                signal.raw_scores = raw_scores
            except Exception:
                pass
            blocked = MT5ExecutionResult(
                ok=False,
                status="guard_blocked",
                message=f"macro rumor guard blocked trade: {rumor_reason}",
                signal_symbol=str(getattr(signal, "symbol", "") or ""),
            )
            self._handle_mt5_result(signal, blocked, source=exec_source)
            return
        apply_filter, _filter_state = self._neural_execution_filter_ready()
        canary_info: dict = {"applied": False, "allowed": False}
        if apply_filter and (not bool(bypass.get("skip_neural_filter", False))):
            prob, prob_meta = self._get_live_neural_probability(signal, source=source)
            min_prob, min_prob_reason = self._neural_min_prob_for_signal(signal, source)
            if prob is None:
                self._attach_neural_filter_meta(signal, prob, min_prob, min_prob_reason, extra=prob_meta)
                skipped = MT5ExecutionResult(
                    ok=False,
                    status="skipped",
                    message="neural quality gate: no qualified probability model",
                    signal_symbol=str(getattr(signal, "symbol", "") or ""),
                )
                self._handle_mt5_result(signal, skipped, source=exec_source)
                return
            soft_applied, soft_info = self._maybe_apply_fx_neural_soft_filter(signal, source, prob, min_prob)
            extra_meta = dict(prob_meta or {})
            extra_meta.update({
                'mt5_fx_soft_filter_applied': bool(soft_info.get('applied')),
                'mt5_fx_soft_filter_reason': str(soft_info.get('reason','')),
                'mt5_fx_soft_filter_penalty': float(soft_info.get('penalty',0.0) or 0.0),
            })
            canary_allowed = False
            if (prob is not None) and (prob < float(min_prob)) and (not bool(soft_info.get('applied'))):
                canary_allowed, canary_info = self._maybe_apply_neural_canary_override(
                    signal=signal,
                    source=source,
                    prob=prob,
                    min_prob=min_prob,
                    min_prob_reason=min_prob_reason,
                )
                extra_meta.update(
                    {
                        "mt5_neural_canary_applied": bool(canary_info.get("applied", False)),
                        "mt5_neural_canary_allowed": bool(canary_allowed),
                        "mt5_neural_canary_reason": str(canary_info.get("reason", "")),
                        "mt5_neural_canary_band_low": canary_info.get("allow_low"),
                        "mt5_neural_canary_band_high": canary_info.get("allow_high"),
                        "mt5_neural_canary_volume_cap": canary_info.get("volume_cap"),
                        "mt5_neural_canary_policy_age_sec": canary_info.get("policy_age_sec"),
                    }
                )
            self._attach_neural_filter_meta(signal, prob, min_prob, min_prob_reason, extra=extra_meta)
            if (prob is not None) and (prob < float(min_prob)) and (not bool(soft_info.get('applied'))) and (not canary_allowed):
                skipped = MT5ExecutionResult(
                    ok=False,
                    status="skipped",
                    message=(
                        f"neural filter: predicted win prob {prob:.2f} "
                        f"< min {float(min_prob):.2f}"
                        f" ({min_prob_reason})"
                    ),
                    signal_symbol=str(getattr(signal, "symbol", "") or ""),
                )
                self._handle_mt5_result(signal, skipped, source=exec_source)
                return
        volume_multiplier = None
        if getattr(config, "MT5_AUTOPILOT_ENABLED", True) and (not bool(bypass.get("skip_risk_governor", False))):
            plan = mt5_orchestrator.pre_trade_plan(signal, source=exec_source)
            if not plan.allow:
                blocked = MT5ExecutionResult(
                    ok=False,
                    status="guard_blocked",
                    message=str(plan.reason or "risk governor blocked"),
                    signal_symbol=str(getattr(signal, "symbol", "") or ""),
                )
                self._handle_mt5_result(signal, blocked, source=exec_source)
                return
            volume_multiplier = float(plan.risk_multiplier or 1.0)
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["mt5_risk_multiplier"] = round(volume_multiplier, 4)
                raw_scores["mt5_canary_mode"] = bool(plan.canary_mode)
                raw_scores["mt5_walkforward_reason"] = str((plan.walkforward or {}).get("reason", plan.reason))
                signal.raw_scores = raw_scores
            except Exception:
                pass
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            extra_mult = max(0.05, float(raw_scores.get("mt5_extra_volume_multiplier", 1.0) or 1.0))
        except Exception:
            extra_mult = 1.0
        volume_multiplier = self._apply_neural_canary_volume_cap(signal, volume_multiplier, canary_info)
        if abs(float(extra_mult) - 1.0) > 1e-9:
            base_mult = 1.0 if volume_multiplier is None else float(volume_multiplier)
            volume_multiplier = round(float(base_mult) * float(extra_mult), 4)
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["mt5_volume_multiplier_before_canary"] = round(float(base_mult), 4)
                raw_scores["mt5_volume_multiplier_after_canary"] = round(float(volume_multiplier), 4)
                signal.raw_scores = raw_scores
            except Exception:
                pass
        shock_blocked, volume_multiplier = self._apply_xau_event_shock_trade_controls(signal, source=source, volume_multiplier=volume_multiplier)
        if shock_blocked is not None:
            self._handle_mt5_result(signal, shock_blocked, source=exec_source)
            return
        result = mt5_executor.execute_signal(signal, source=exec_source, volume_multiplier=volume_multiplier)
        self._handle_mt5_result(signal, result, source=exec_source)

    def _handle_mt5_result(self, signal, result: MT5ExecutionResult, source: str) -> None:
        trace = self._signal_trace_meta(signal)
        lane = str(self._mt5_lane_key_from_source(source) or "main").upper()
        logger.info(
            "[MT5][%s][%s] %s %s -> %s (%s) %s",
            lane,
            str(trace.get("tag", "-")),
            result.status,
            result.signal_symbol,
            result.broker_symbol or "-",
            source,
            result.message,
        )
        try:
            self._repeat_guard_on_result(signal, result, source=source)
        except Exception as e:
            logger.debug("[MT5] repeat-error guard update skipped: %s", e)
        if not self._is_pytest_runtime():
            try:
                neural_brain.record_execution(signal, result, source=source)
            except Exception as e:
                logger.warning("[NeuralBrain] record_execution failed: %s", e)
            try:
                if getattr(config, "MT5_AUTOPILOT_ENABLED", True):
                    mt5_autopilot_core.record_execution(signal, result, source=source)
            except Exception as e:
                logger.warning("[MT5Autopilot] record_execution failed: %s", e)
        else:
            logger.debug("[MT5][TEST] persistence skipped source=%s symbol=%s", source, result.signal_symbol)

        status = str(result.status or "").lower()
        if result.ok and config.MT5_NOTIFY_EXECUTED:
            notifier.send_mt5_execution_update(signal, result, source=source)
        elif (not result.ok) and config.MT5_NOTIFY_FAILED and status in {"rejected", "error", "invalid_stops", "blocked"}:
            notifier.send_mt5_execution_update(signal, result, source=source)

    def _maybe_execute_ctrader_signal(self, signal, source: str):
        if not bool(getattr(config, "CTRADER_ENABLED", False)):
            return None
        if not bool(getattr(config, "CTRADER_AUTOTRADE_ENABLED", False)):
            return None
        self._ensure_signal_trace(signal, source=str(source or ""))
        # ── ADI: Adaptive Directional Intelligence confidence modifier ──
        try:
            self._apply_adi_modifier(signal, source=str(source or ""))
        except Exception as e:
            logger.debug("[ADI] _apply_adi_modifier failed (non-fatal): %s", e)
        dispatch_source, dispatch_meta = self._ctrader_pick_dispatch_source(signal, source)
        if not dispatch_source:
            skip_reason = str((dispatch_meta or {}).get("winner_reason", "source_not_allowed"))
            logger.info(
                "[CTRADER] skipped source=%s symbol=%s reason=%s",
                str(source or ""),
                getattr(signal, "symbol", ""),
                skip_reason,
            )
            self._audit_xau_pre_dispatch_skip(
                signal,
                requested_source=str(source or ""),
                dispatch_source="",
                gate="dispatch_source",
                reason=skip_reason,
                dispatch_meta=dispatch_meta,
            )
            return None
        if str(dispatch_source or "").strip().lower() in {"scalp_xauusd", "scalp_xauusd:winner"}:
            allow_xau, xau_reason = self._allow_scalp_xau_live_mt5(signal, source=dispatch_source)
            if not allow_xau:
                skip_reason = str(xau_reason or "xau_live_filter_blocked")
                logger.info(
                    "[CTRADER] skipped source=%s symbol=%s reason=%s",
                    str(dispatch_source or ""),
                    getattr(signal, "symbol", ""),
                    skip_reason,
                )
                self._audit_xau_pre_dispatch_skip(
                    signal,
                    requested_source=str(source or ""),
                    dispatch_source=str(dispatch_source or ""),
                    gate="xau_live_filter",
                    reason=skip_reason,
                    dispatch_meta=dispatch_meta,
                )
                return None
        allow_source, source_reason = self._allow_ctrader_source_profile(signal, dispatch_source)
        if not allow_source:
            skip_reason = str(source_reason or "source_profile_blocked")
            logger.info(
                "[CTRADER] skipped source=%s symbol=%s reason=%s",
                str(dispatch_source or ""),
                getattr(signal, "symbol", ""),
                skip_reason,
            )
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["ctrader_source_profile_blocked"] = True
                raw_scores["ctrader_source_profile_reason"] = skip_reason
                signal.raw_scores = raw_scores
            except Exception:
                pass
            self._audit_xau_pre_dispatch_skip(
                signal,
                requested_source=str(source or ""),
                dispatch_source=str(dispatch_source or ""),
                gate="source_profile",
                reason=skip_reason,
                dispatch_meta=dispatch_meta,
            )
            return None
        if dispatch_source != str(source or ""):
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["ctrader_dispatch_source"] = str(dispatch_source)
                raw_scores["ctrader_dispatch_reason"] = str((dispatch_meta or {}).get("winner_reason", "") or "")
                signal.raw_scores = raw_scores
            except Exception:
                pass
        try:
            result = ctrader_executor.execute_signal(signal, source=dispatch_source)
            logger.info(
                "[CTRADER] %s %s -> %s (%s) %s",
                str(result.status or ""),
                str(result.signal_symbol or getattr(signal, "symbol", "") or ""),
                str(result.broker_symbol or "-"),
                str(dispatch_source or ""),
                str(result.message or ""),
            )
            return result
        except Exception as e:
            logger.warning("[CTRADER] execute failed source=%s symbol=%s err=%s", dispatch_source, getattr(signal, "symbol", ""), e)
            return None

    def _maybe_execute_ctrader_batch(self, signals: list, source: str) -> list:
        if not bool(getattr(config, "CTRADER_ENABLED", False)):
            return []
        if not bool(getattr(config, "CTRADER_AUTOTRADE_ENABLED", False)):
            return []
        out = []
        for signal in list(signals or []):
            res = self._maybe_execute_ctrader_signal(signal, source=source)
            if res is not None:
                out.append(res)
        return out

    def _run_ctrader_sync(self):
        if not bool(getattr(config, "CTRADER_ENABLED", False)):
            return
        if ctrader_executor is None:
            return
        if not bool(getattr(config, "CTRADER_SYNC_ENABLED", True)):
            return
        try:
            rpt = ctrader_executor.sync_account_state(
                lookback_hours=max(1, int(getattr(config, "CTRADER_SYNC_DEALS_LOOKBACK_HOURS", 72) or 72)),
                auto_close_unsafe=bool(getattr(config, "CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE", True)),
            )
            if bool(rpt.get("ok")):
                if (
                    int(rpt.get("closed_unsafe", 0) or 0) > 0
                    or int(rpt.get("reconciled_journal", 0) or 0) > 0
                    or int(rpt.get("canceled_orders", 0) or 0) > 0
                    or int(rpt.get("amended_positions", 0) or 0) > 0
                    or int(rpt.get("closed_profit_positions", 0) or 0) > 0
                ):
                    logger.info(
                        "[CTRADER] sync ok positions=%s orders=%s deals=%s reconciled=%s canceled_orders=%s closed_unsafe=%s amended=%s closed_profit=%s",
                        rpt.get("positions", 0),
                        rpt.get("orders", 0),
                        rpt.get("deals", 0),
                        rpt.get("reconciled_journal", 0),
                        rpt.get("canceled_orders", 0),
                        rpt.get("closed_unsafe", 0),
                        rpt.get("amended_positions", 0),
                        rpt.get("closed_profit_positions", 0),
                    )
                if bool(getattr(config, "CANARY_POST_TRADE_AUDIT_ENABLED", False)):
                    try:
                        self._run_canary_post_trade_audit(force=False)
                    except Exception:
                        logger.debug("[Scheduler] cTrader sync canary audit follow-up failed", exc_info=True)
                try:
                    self._run_ct_only_watch_report(force=False)
                except Exception:
                    logger.debug("[Scheduler] cTrader sync ct-only watch follow-up failed", exc_info=True)

                # ── Feed fibo_advance trade results to circuit breaker ──────
                try:
                    self._feed_fibo_trade_results(rpt)
                except Exception:
                    logger.debug("[Scheduler] fibo trade result feed failed", exc_info=True)
            else:
                err_msg = str(rpt.get("error") or rpt.get("message") or "")
                logger.warning("[CTRADER] sync failed: %s", err_msg)
                # Auto-refresh token on auth failures
                if any(k in err_msg for k in ("Invalid access token", "Cannot route", "ACCESS_TOKEN_INVALID", "Unauthorized")):
                    try:
                        from api.ctrader_token_manager import token_manager as _tm
                        logger.info("[CTRADER] Auth failure detected — attempting token refresh")
                        new_token = _tm.try_refresh()
                        if new_token:
                            logger.info("[CTRADER] Token refreshed after sync auth failure")
                        else:
                            logger.error("[CTRADER] Token refresh failed — trading may be disrupted")
                    except Exception as refresh_err:
                        logger.debug("[CTRADER] Token refresh error: %s", refresh_err)
            if bool(getattr(config, "NEURAL_GATE_LEARNING_ENABLED", True)):
                try:
                    gate_loop = neural_gate_learning_loop.run_cycle()
                    if gate_loop.ok:
                        logger.info(
                            "[NeuralGateLoop] %s%s%s",
                            gate_loop.message,
                            (f" policy={gate_loop.policy_path}" if gate_loop.policy_path else ""),
                            (f" report={gate_loop.report_path}" if gate_loop.report_path else ""),
                        )
                    else:
                        logger.info("[NeuralGateLoop] skipped: %s", gate_loop.message)
                except Exception as e:
                    logger.warning("[NeuralGateLoop] cycle error: %s", e)
        except Exception as e:
            logger.warning("[CTRADER] sync error: %s", e)

    def _maybe_execute_mt5_batch(self, signals: list, source: str) -> None:
        if not config.MT5_ENABLED:
            return
        max_count = max(1, int(config.MT5_MAX_SIGNALS_PER_SCAN))
        max_attempts = max(max_count, int(config.MT5_MAX_ATTEMPTS_PER_SCAN))
        executed = 0
        attempted = 0
        apply_filter, _filter_state = self._neural_execution_filter_ready()
        for signal in signals:
            if executed >= max_count or attempted >= max_attempts:
                break
            bypass = self._resolve_mt5_bypass_profile(signal, source)
            exec_source = str(bypass.get("source") or source)
            self._ensure_signal_trace(signal, source=exec_source)
            if bool(bypass.get("enabled")):
                try:
                    raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                    raw_scores["mt5_bypass_test_enabled"] = True
                    raw_scores["mt5_bypass_source"] = str(exec_source)
                    raw_scores["mt5_bypass_skip_neural_filter"] = bool(bypass.get("skip_neural_filter", False))
                    raw_scores["mt5_bypass_skip_risk_governor"] = bool(bypass.get("skip_risk_governor", False))
                    raw_scores["mt5_bypass_skip_confidence"] = bool(bypass.get("skip_mt5_confidence", False))
                    raw_scores["mt5_bypass_ignore_open_positions"] = bool(bypass.get("ignore_open_positions", False))
                    raw_scores["mt5_bypass_magic_offset"] = int(bypass.get("magic_offset", 0) or 0)
                    signal.raw_scores = raw_scores
                except Exception:
                    pass
            self._apply_mt5_lane_limit_policy(signal, exec_source=exec_source)
            guard_ok, guard = self._repeat_guard_allow(signal, source=exec_source)
            if not guard_ok:
                attempted += 1
                blocked = MT5ExecutionResult(
                    ok=False,
                    status="skipped",
                    message=(
                        f"repeat-error guard active for {str(guard.get('symbol', '?'))}: "
                        f"{str(guard.get('remaining_sec', '?'))}s remaining"
                    ),
                    signal_symbol=str(getattr(signal, "symbol", "") or ""),
                )
                self._handle_mt5_result(signal, blocked, source=exec_source)
                continue
            rumor_block, rumor_reason, rumor_meta = self._check_macro_rumor_trade_guard(signal)
            if rumor_block:
                attempted += 1
                try:
                    raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                    raw_scores["macro_rumor_guard_blocked"] = True
                    raw_scores["macro_rumor_guard_reason"] = str(rumor_reason or "")
                    if rumor_meta.get("headline_id"):
                        raw_scores["macro_rumor_guard_headline_id"] = str(rumor_meta.get("headline_id"))
                    signal.raw_scores = raw_scores
                except Exception:
                    pass
                blocked = MT5ExecutionResult(
                    ok=False,
                    status="guard_blocked",
                    message=f"macro rumor guard blocked trade: {rumor_reason}",
                    signal_symbol=str(getattr(signal, "symbol", "") or ""),
                )
                self._handle_mt5_result(signal, blocked, source=exec_source)
                continue
            canary_info: dict = {"applied": False, "allowed": False}
            if apply_filter and (not bool(bypass.get("skip_neural_filter", False))):
                prob, prob_meta = self._get_live_neural_probability(signal, source=source)
                min_prob, min_prob_reason = self._neural_min_prob_for_signal(signal, source)
                if prob is None:
                    attempted += 1
                    self._attach_neural_filter_meta(signal, prob, min_prob, min_prob_reason, extra=prob_meta)
                    skipped = MT5ExecutionResult(
                        ok=False,
                        status="skipped",
                        message="neural quality gate: no qualified probability model",
                        signal_symbol=str(getattr(signal, "symbol", "") or ""),
                    )
                    self._handle_mt5_result(signal, skipped, source=exec_source)
                    continue
                soft_applied, soft_info = self._maybe_apply_fx_neural_soft_filter(signal, source, prob, min_prob)
                extra_meta = dict(prob_meta or {})
                extra_meta.update({
                    'mt5_fx_soft_filter_applied': bool(soft_info.get('applied')),
                    'mt5_fx_soft_filter_reason': str(soft_info.get('reason','')),
                    'mt5_fx_soft_filter_penalty': float(soft_info.get('penalty',0.0) or 0.0),
                })
                canary_allowed = False
                if (prob is not None) and (prob < float(min_prob)) and (not bool(soft_info.get('applied'))):
                    canary_allowed, canary_info = self._maybe_apply_neural_canary_override(
                        signal=signal,
                        source=source,
                        prob=prob,
                        min_prob=min_prob,
                        min_prob_reason=min_prob_reason,
                    )
                    extra_meta.update(
                        {
                            "mt5_neural_canary_applied": bool(canary_info.get("applied", False)),
                            "mt5_neural_canary_allowed": bool(canary_allowed),
                            "mt5_neural_canary_reason": str(canary_info.get("reason", "")),
                            "mt5_neural_canary_band_low": canary_info.get("allow_low"),
                            "mt5_neural_canary_band_high": canary_info.get("allow_high"),
                            "mt5_neural_canary_volume_cap": canary_info.get("volume_cap"),
                            "mt5_neural_canary_policy_age_sec": canary_info.get("policy_age_sec"),
                        }
                    )
                self._attach_neural_filter_meta(signal, prob, min_prob, min_prob_reason, extra=extra_meta)
                if (prob is not None) and (prob < float(min_prob)) and (not bool(soft_info.get('applied'))) and (not canary_allowed):
                    attempted += 1
                    skipped = MT5ExecutionResult(
                        ok=False,
                        status="skipped",
                        message=(
                            f"neural filter: predicted win prob {prob:.2f} "
                            f"< min {float(min_prob):.2f}"
                            f" ({min_prob_reason})"
                        ),
                        signal_symbol=str(getattr(signal, "symbol", "") or ""),
                    )
                    self._handle_mt5_result(signal, skipped, source=exec_source)
                    continue
            volume_multiplier = None
            if getattr(config, "MT5_AUTOPILOT_ENABLED", True) and (not bool(bypass.get("skip_risk_governor", False))):
                plan = mt5_orchestrator.pre_trade_plan(signal, source=exec_source)
                if not plan.allow:
                    skipped = MT5ExecutionResult(
                        ok=False,
                        status="guard_blocked",
                        message=str(plan.reason or "risk governor blocked"),
                        signal_symbol=str(getattr(signal, "symbol", "") or ""),
                    )
                    self._handle_mt5_result(signal, skipped, source=exec_source)
                    attempted += 1
                    continue
                volume_multiplier = float(plan.risk_multiplier or 1.0)
                try:
                    raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                    raw_scores["mt5_risk_multiplier"] = round(volume_multiplier, 4)
                    raw_scores["mt5_canary_mode"] = bool(plan.canary_mode)
                    raw_scores["mt5_walkforward_reason"] = str((plan.walkforward or {}).get("reason", plan.reason))
                    signal.raw_scores = raw_scores
                except Exception:
                    pass
            volume_multiplier = self._apply_neural_canary_volume_cap(signal, volume_multiplier, canary_info)
            shock_blocked, volume_multiplier = self._apply_xau_event_shock_trade_controls(signal, source=source, volume_multiplier=volume_multiplier)
            if shock_blocked is not None:
                attempted += 1
                self._handle_mt5_result(signal, shock_blocked, source=exec_source)
                continue
            result = mt5_executor.execute_signal(signal, source=exec_source, volume_multiplier=volume_multiplier)
            self._handle_mt5_result(signal, result, source=exec_source)
            consume_attempt = True
            if bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)):
                status = str(getattr(result, "status", "") or "").lower()
                msg = str(getattr(result, "message", "") or "").lower()
                if status in {"micro_filtered", "unmapped"}:
                    consume_attempt = False
                elif status == "skipped" and "margin guard" in msg:
                    consume_attempt = False
            if consume_attempt:
                attempted += 1
            if result.ok:
                executed += 1
        if attempted >= max_attempts and executed < max_count:
            logger.info(
                "[MT5] %s attempts capped: executed=%d attempted=%d max_signals=%d",
                source,
                executed,
                attempted,
                max_count,
            )

    def _run_neural_sync_train(self):
        """Sync outcomes into learning DB and optionally auto-train."""
        if not config.NEURAL_BRAIN_ENABLED:
            return
        try:
            mt5_labels = 0
            market_labels = 0

            sync = neural_brain.sync_outcomes_from_mt5(days=config.NEURAL_BRAIN_SYNC_DAYS)
            if sync.get("ok"):
                mt5_labels = int(sync.get("updated", 0) or 0)
                logger.info(
                    "[NeuralBrain] sync updated=%s closed_positions=%s",
                    sync.get("updated", 0),
                    sync.get("closed_positions", 0),
                )
            else:
                logger.warning("[NeuralBrain] mt5 sync failed: %s", sync.get("message", "unknown"))

            if config.SIGNAL_FEEDBACK_ENABLED:
                feedback = neural_brain.sync_signal_outcomes_from_market(
                    days=config.NEURAL_BRAIN_SYNC_DAYS,
                    max_records=config.NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS,
                )
                if feedback.get("ok"):
                    market_labels = int(feedback.get("resolved", 0) or 0)
                    logger.info(
                        "[NeuralBrain] feedback reviewed=%s resolved=%s pseudo=%s updated=%s",
                        feedback.get("reviewed", 0),
                        feedback.get("resolved", 0),
                        feedback.get("pseudo_labeled", 0),
                        feedback.get("updated", 0),
                    )
                else:
                    logger.warning("[NeuralBrain] feedback sync failed: %s", feedback.get("message", "unknown"))

            if config.NEURAL_BRAIN_AUTO_TRAIN:
                model = neural_brain.model_status()
                new_labels = int(mt5_labels + market_labels)
                should_train = (not model.get("available")) or (new_labels > 0)
                if should_train:
                    train_min_samples = int(config.NEURAL_BRAIN_MIN_SAMPLES)
                    if not model.get("available"):
                        bootstrap_min = max(10, int(config.NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES))
                        train_min_samples = min(train_min_samples, bootstrap_min)
                    train = neural_brain.train_backprop(
                        days=config.NEURAL_BRAIN_SYNC_DAYS,
                        min_samples=train_min_samples,
                    )
                    if train.ok:
                        logger.info(
                            "[NeuralBrain] trained samples=%d train_acc=%.2f val_acc=%.2f win_rate=%.2f (min_samples=%d)",
                            train.samples,
                            train.train_accuracy,
                            train.val_accuracy,
                            train.win_rate,
                            train_min_samples,
                        )
                    else:
                        logger.info("[NeuralBrain] train skipped: %s", train.message)
                    try:
                        from learning.symbol_neural_brain import symbol_neural_brain

                        sym_results = symbol_neural_brain.train_all(days=max(1, int(config.NEURAL_BRAIN_SYNC_DAYS)))
                        total = int(len(sym_results))
                        ok_cnt = 0
                        skip_cnt = 0
                        fail_cnt = 0
                        for _k, res in (sym_results or {}).items():
                            if bool(getattr(res, "ok", False)):
                                ok_cnt += 1
                            elif str(getattr(res, "status", "") or "") in {"not_enough_data", "disabled"}:
                                skip_cnt += 1
                            else:
                                fail_cnt += 1
                        logger.info(
                            "[SymbolBrain] auto-train total=%d ok=%d skipped=%d failed=%d",
                            total,
                            ok_cnt,
                            skip_cnt,
                            fail_cnt,
                        )
                    except Exception as e:
                        logger.warning("[SymbolBrain] auto-train error: %s", e)
                else:
                    logger.info("[NeuralBrain] no new labels; training not required")
        except Exception as e:
            logger.warning("[NeuralBrain] sync/train error: %s", e)

    def _run_mt5_autopilot_sync(self):
        """Sync MT5 closed outcomes into forward-test journal + calibration stats."""
        if not (config.MT5_ENABLED and getattr(config, "MT5_AUTOPILOT_ENABLED", True)):
            return
        try:
            report = mt5_autopilot_core.sync_outcomes_from_mt5(
                hours=max(24, int(getattr(config, "NEURAL_BRAIN_SYNC_DAYS", 120)) * 24)
            )
            if report.get("ok"):
                logger.info(
                    "[MT5Autopilot] sync closed=%s updated=%s q=%s labeled7d=%s win_rate=%.2f mae=%s",
                    report.get("closed_rows_seen", 0),
                    report.get("updated", 0),
                    report.get("history_query_mode", "-"),
                    report.get("labeled_7d", 0),
                    float(report.get("win_rate_7d", 0.0) or 0.0),
                    (f"{float(report.get('mae_7d')):.3f}" if report.get("mae_7d") is not None else "-"),
                )
            else:
                logger.info("[MT5Autopilot] sync skipped: %s", report.get("message", "unknown"))
            try:
                orch = mt5_orchestrator.sync_current_account()
                if orch.get("ok"):
                    logger.info("[MT5Orchestrator] synced current account: %s", orch.get("account_key"))
            except Exception as e:
                logger.debug("[MT5Orchestrator] sync error: %s", e)
            try:
                pm_learn = mt5_position_manager.sync_learning_outcomes(
                    hours=max(24, int(getattr(config, "MT5_PM_LEARNING_SYNC_HOURS", 168)))
                )
                if pm_learn.get("ok"):
                    logger.info(
                        "[MT5PM-Learn] closed=%s updated=%s unresolved=%s q=%s",
                        pm_learn.get("closed_rows_seen", 0),
                        pm_learn.get("updated", 0),
                        pm_learn.get("still_unresolved", 0),
                        pm_learn.get("history_query_mode", "-"),
                    )
                elif str(pm_learn.get("error") or "") not in {"disabled"}:
                    logger.debug("[MT5PM-Learn] sync skipped: %s", pm_learn.get("error", "unknown"))
            except Exception as e:
                logger.debug("[MT5PM-Learn] sync error: %s", e)
            if bool(getattr(config, "CANARY_POST_TRADE_AUDIT_ENABLED", False)):
                try:
                    self._run_canary_post_trade_audit(force=False)
                except Exception:
                    logger.debug("[Scheduler] MT5 sync canary audit follow-up failed", exc_info=True)
        except Exception as e:
            logger.warning("[MT5Autopilot] sync error: %s", e)

    def _run_mt5_position_manager(self):
        """Autonomous MT5 position management cycle (BE / trail / partial / time-stop)."""
        if not (config.MT5_ENABLED and getattr(config, "MT5_POSITION_MANAGER_ENABLED", True)):
            return
        try:
            report = mt5_position_manager.run_cycle(source="scheduler")
            if report.get("ok"):
                actions = list(report.get("actions", []) or [])
                if actions:
                    def _pm_act_label(a: dict) -> str:
                        label = f"{a.get('symbol')}:{a.get('action')}:{a.get('status')}"
                        try:
                            rc = a.get('retcode')
                            if rc is not None:
                                label += f"(retcode={int(rc)})"
                        except Exception:
                            pass
                        return label

                    logger.info(
                        "[MT5PM] positions=%s checked=%s managed=%s actions=%s",
                        report.get("positions", 0),
                        report.get("checked", 0),
                        report.get("managed", 0),
                        ", ".join(_pm_act_label(a) for a in actions[:5]),
                    )
                    if bool(getattr(config, "MT5_PM_NOTIFY_ACTIONS", True)):
                        try:
                            notifier.send_mt5_position_manager_update(report, source="scheduler")
                        except Exception as e:
                            logger.warning("[MT5PM] notify failed: %s", e)
            else:
                logger.debug("[MT5PM] cycle skipped: %s", report.get("error", "unknown"))
        except Exception as e:
            logger.warning("[MT5PM] cycle error: %s", e)

    def _run_mt5_limit_manager(self):
        """Autonomous MT5 Limit Order management cycle (timeout/front-run/stucture-break)."""
        if not (config.MT5_ENABLED and getattr(config, "MT5_LIMIT_ENTRY_ENABLED", True)):
            return
        try:
            report = mt5_limit_manager.run_cycle(source="scheduler")
            if report.get("ok"):
                actions = list(report.get("actions", []) or [])
                if actions:
                    logger.info(
                        "[MT5LimitMgr] limits_open=%s manager_actions=%s",
                        report.get("orders", 0),
                        ", ".join(f"{a.get('symbol')}:{a.get('action')}" for a in actions),
                    )
            else:
                logger.debug("[MT5LimitMgr] cycle skipped: %s", report.get("error", "unknown"))
        except Exception as e:
            logger.warning("[MT5LimitMgr] cycle error: %s", e)

    def _run_mt5_bypass_quick_tp(self):
        """
        Separate bypass-lane quick take-profit:
        close bypass-tagged positions once floating profit reaches a target
        (default: 1% of balance, with configurable USD floor).
        """
        if not (
            bool(getattr(config, "MT5_ENABLED", False))
            and bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False))
            and bool(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_ENABLED", False))
        ):
            return
        try:
            st = mt5_executor.status()
            if not bool(st.get("connected", False)):
                return {"ok": False, "reason": "mt5_disconnected"}
            balance = float(st.get("balance", 0.0) or 0.0)
            pct = max(0.0, float(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_BALANCE_PCT", 1.0) or 1.0))
            min_usd = max(0.0, float(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_MIN_USD", 1.0) or 1.0))
            target = max(min_usd, balance * (pct / 100.0))
            suffix = str(getattr(config, "MT5_BYPASS_TEST_SOURCE_SUFFIX", "bypass") or "bypass").strip().lower()
            bypass_magic = int(getattr(config, "MT5_MAGIC", 0) or 0) + int(getattr(config, "MT5_BYPASS_TEST_MAGIC_OFFSET", 0) or 0)

            snap = mt5_executor.open_positions_snapshot(limit=200)
            positions = list((snap or {}).get("positions", []) or [])
            if not positions:
                return {"ok": True, "checked": 0, "closed": 0, "failed": 0, "target_usd": float(target), "balance": float(balance)}

            checked = 0
            closed = 0
            failed = 0
            for p in positions:
                if not self._is_bypass_position(dict(p or {}), suffix, bypass_magic):
                    continue
                checked += 1
                profit = float((p or {}).get("profit", 0.0) or 0.0)
                if profit < float(target):
                    continue
                ticket = int((p or {}).get("ticket", 0) or 0)
                volume = float((p or {}).get("volume", 0.0) or 0.0)
                ptype = str((p or {}).get("type", "") or "")
                symbol = str((p or {}).get("symbol", "") or "")
                if ticket <= 0 or volume <= 0 or not symbol:
                    continue
                res = mt5_executor.close_position_partial(
                    broker_symbol=symbol,
                    position_ticket=ticket,
                    position_type=ptype,
                    position_volume=volume,
                    close_volume=volume,
                    source="bypass_quick_tp",
                )
                if bool(getattr(res, "ok", False)):
                    closed += 1
                    if bool(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_NOTIFY_TELEGRAM", True)):
                        try:
                            notifier.send_mt5_bypass_quick_tp_update(
                                symbol=symbol,
                                ticket=ticket,
                                profit_usd=profit,
                                target_usd=target,
                                balance_usd=balance,
                            )
                        except Exception as e:
                            logger.debug("[MT5BypassTP] telegram notify failed ticket=%s err=%s", ticket, e)
                else:
                    failed += 1
            if checked > 0 and (closed > 0 or failed > 0):
                logger.info(
                    "[MT5BypassTP] checked=%s closed=%s failed=%s target_usd=%.2f balance=%.2f",
                    checked,
                    closed,
                    failed,
                    float(target),
                    float(balance),
                )
            elif checked > 0:
                now_ts = float(time.time())
                if now_ts - float(self._last_bypass_tp_diag_ts or 0.0) >= 120.0:
                    self._last_bypass_tp_diag_ts = now_ts
                    logger.info(
                        "[MT5BypassTP] checked=%s no_close target_usd=%.2f balance=%.2f",
                        checked,
                        float(target),
                        float(balance),
                    )
            return {
                "ok": True,
                "checked": int(checked),
                "closed": int(closed),
                "failed": int(failed),
                "target_usd": float(target),
                "balance": float(balance),
            }
        except Exception as e:
            logger.warning("[MT5BypassTP] cycle error: %s", e)
            return {"ok": False, "reason": str(e)}

    def _run_mt5_preclose_flatten(self, force: bool = False, ny_now: datetime | None = None):
        """
        Force-close open MT5 positions before configured NY market-close cutoff.
        Default use-case: avoid positions getting stuck into weekend close.
        """
        if not (bool(getattr(config, "MT5_ENABLED", False)) and bool(getattr(config, "MT5_PRE_CLOSE_FLATTEN_ENABLED", False))):
            return {"ok": False, "skipped": True, "reason": "disabled"}
        try:
            ny_now = ny_now or datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            day_key = ny_now.strftime("%Y-%m-%d")
            fri_only = bool(getattr(config, "MT5_PRE_CLOSE_FLATTEN_FRI_ONLY", True))
            if fri_only and ny_now.weekday() != 4 and (not force):
                return {"ok": True, "skipped": True, "reason": "not_friday", "day_key": day_key}

            h = max(0, min(23, int(getattr(config, "MT5_PRE_CLOSE_FLATTEN_NY_HOUR", 16) or 16)))
            m = max(0, min(59, int(getattr(config, "MT5_PRE_CLOSE_FLATTEN_NY_MINUTE", 50) or 50)))
            window_min = max(1, int(getattr(config, "MT5_PRE_CLOSE_FLATTEN_WINDOW_MIN", 20) or 20))
            target_dt = ny_now.replace(hour=h, minute=m, second=0, microsecond=0)
            start_dt = target_dt - timedelta(minutes=window_min)
            end_dt = target_dt + timedelta(minutes=2)
            in_window = start_dt <= ny_now <= end_dt
            if (not force) and (not in_window):
                return {"ok": True, "skipped": True, "reason": "outside_window", "day_key": day_key}
            if (not force) and self._mt5_preclose_flatten_day == day_key:
                return {"ok": True, "skipped": True, "reason": "already_done_today", "day_key": day_key}

            include = set(config.get_mt5_preclose_flatten_include_symbols() or set())
            exclude = set(config.get_mt5_preclose_flatten_exclude_symbols() or set())
            snap = mt5_executor.open_positions_snapshot(limit=100)
            positions = list((snap or {}).get("positions", []) or [])
            if not positions:
                self._mt5_preclose_flatten_day = day_key
                return {
                    "ok": True,
                    "skipped": True,
                    "reason": "no_positions",
                    "day_key": day_key,
                    "checked": 0,
                    "closed": 0,
                    "failed": 0,
                }

            actions = []
            closed = 0
            failed = 0
            checked = 0
            for p in positions:
                try:
                    sym = str((p or {}).get("symbol", "") or "").upper().strip()
                    if not sym:
                        continue
                    if include and sym not in include:
                        continue
                    if sym in exclude:
                        continue
                    checked += 1
                    ticket = int((p or {}).get("ticket", 0) or 0)
                    ptype = str((p or {}).get("type", "") or "")
                    vol = float((p or {}).get("volume", 0.0) or 0.0)
                    if ticket <= 0 or vol <= 0:
                        continue
                    res = mt5_executor.close_position_partial(
                        broker_symbol=sym,
                        position_ticket=ticket,
                        position_type=ptype,
                        position_volume=vol,
                        close_volume=vol,
                        source="preclose",
                    )
                    item = {
                        "symbol": sym,
                        "ticket": ticket,
                        "volume": vol,
                        "status": str(getattr(res, "status", "") or ""),
                        "ok": bool(getattr(res, "ok", False)),
                        "message": str(getattr(res, "message", "") or ""),
                    }
                    actions.append(item)
                    if item["ok"]:
                        closed += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    actions.append({"symbol": str((p or {}).get("symbol", "") or ""), "ticket": int((p or {}).get("ticket", 0) or 0), "ok": False, "status": "error", "message": str(e)})

            self._mt5_preclose_flatten_day = day_key
            report = {
                "ok": True,
                "skipped": False,
                "day_key": day_key,
                "ny_now": ny_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "target_time": target_dt.strftime("%H:%M"),
                "checked": checked,
                "closed": closed,
                "failed": failed,
                "actions": actions[:20],
            }
            logger.info(
                "[MT5PreClose] checked=%s closed=%s failed=%s include=%s exclude=%s window=%s..%s now=%s",
                checked,
                closed,
                failed,
                (",".join(sorted(include)) if include else "-"),
                (",".join(sorted(exclude)) if exclude else "-"),
                start_dt.strftime("%H:%M"),
                end_dt.strftime("%H:%M"),
                ny_now.strftime("%H:%M"),
            )
            return report
        except Exception as e:
            logger.warning("[MT5PreClose] cycle error: %s", e)
            return {"ok": False, "skipped": False, "error": str(e)}

    @staticmethod
    def _normalize_neural_mission_symbols() -> str:
        alias = {
            "XAU": "XAUUSD",
            "XAUUSD": "XAUUSD",
            "GOLD": "XAUUSD",
            "ETH": "ETHUSD",
            "ETHUSD": "ETHUSD",
            "ETHUSDT": "ETHUSD",
            "ETH/USDT": "ETHUSD",
            "BTC": "BTCUSD",
            "BTCUSD": "BTCUSD",
            "BTCUSDT": "BTCUSD",
            "BTC/USDT": "BTCUSD",
            "GBP": "GBPUSD",
            "GBPUSD": "GBPUSD",
        }
        raw = str(getattr(config, "NEURAL_MISSION_SYMBOLS", "") or "")
        out: list[str] = []
        seen: set[str] = set()
        for part in raw.split(","):
            token = str(part or "").strip().upper().replace(" ", "")
            if not token:
                continue
            mapped = alias.get(token, token)
            if mapped in seen:
                continue
            seen.add(mapped)
            out.append(mapped)
        if not out:
            out = ["XAUUSD", "ETHUSD", "BTCUSD", "GBPUSD"]
        return ",".join(out)

    def _run_neural_mission_cycle(self, source: str = "scheduler") -> dict:
        """One mission cycle: sync/train/backtest/tune using real outcomes."""
        if not bool(getattr(config, "NEURAL_MISSION_AUTO_ENABLED", False)):
            return {"ok": False, "skipped": True, "reason": "disabled"}
        if not self._neural_mission_cycle_lock.acquire(blocking=False):
            logger.info("[NeuralMission] cycle skipped: previous cycle still running")
            return {"ok": False, "skipped": True, "reason": "in_progress"}
        start_ts = time.time()
        try:
            from learning.mt5_neural_mission import mt5_neural_mission

            days = max(1, int(getattr(config, "NEURAL_BRAIN_SYNC_DAYS", 120)))
            symbols = self._normalize_neural_mission_symbols()
            report = dict(
                mt5_neural_mission.run(
                    symbols=symbols,
                    iterations=max(1, int(getattr(config, "NEURAL_MISSION_ITERATIONS_PER_CYCLE", 1))),
                    train_days=days,
                    backtest_days=days,
                    sync_days=days,
                    target_win_rate=float(getattr(config, "NEURAL_MISSION_TARGET_WIN_RATE", 58.0)),
                    target_profit_factor=float(getattr(config, "NEURAL_MISSION_TARGET_PROFIT_FACTOR", 1.2)),
                    min_trades=max(3, int(getattr(config, "NEURAL_MISSION_MIN_TRADES", 12))),
                    apply_policy_draft=bool(getattr(config, "NEURAL_MISSION_APPLY_POLICY_DRAFT", False)),
                )
                or {}
            )
            elapsed = time.time() - start_ts
            logger.info(
                "[NeuralMission] source=%s ok=%s goal_met=%s iterations=%s symbols=%s report=%s elapsed=%.1fs",
                source,
                bool(report.get("ok", False)),
                bool(report.get("goal_met", False)),
                int(report.get("iterations_done", 0) or 0),
                ",".join(list(report.get("symbols", []) or [])),
                str(report.get("report_path", "")),
                float(elapsed),
            )
            return report
        except Exception as e:
            logger.warning("[NeuralMission] cycle error (%s): %s", source, e)
            return {"ok": False, "error": str(e), "source": source}
        finally:
            self._neural_mission_cycle_lock.release()

    def _run_neural_mission_cycle_async(self, source: str = "scheduler") -> None:
        """Run mission loop in a dedicated thread so scheduler tasks are not blocked."""
        if not bool(getattr(config, "NEURAL_MISSION_AUTO_ENABLED", False)):
            return
        if self._neural_mission_thread is not None and self._neural_mission_thread.is_alive():
            logger.info("[NeuralMission] async trigger skipped: worker still running")
            return
        self._neural_mission_thread = threading.Thread(
            target=self._run_neural_mission_cycle,
            kwargs={"source": source},
            daemon=True,
            name="NeuralMissionCycle",
        )
        self._neural_mission_thread.start()

    def _evaluate_xauusd_cooldown(self, signal) -> tuple[bool, dict]:
        """
        Evaluate duplicate-alert cooldown state for XAUUSD.
        """
        data: dict = {"reason": "ok", "elapsed_sec": 0.0, "remaining_sec": 0.0}
        now_ts = time.time()
        if self._last_xauusd_alert_ts <= 0:
            data["reason"] = "first_signal"
            return True, data

        elapsed = now_ts - self._last_xauusd_alert_ts
        data["elapsed_sec"] = float(elapsed)
        cooldown = max(60, int(config.XAUUSD_ALERT_COOLDOWN_SEC))
        data["cooldown_sec"] = float(cooldown)
        if elapsed >= cooldown:
            data["reason"] = "cooldown_elapsed"
            return True, data

        # Direction flip should always be alerted even inside cooldown.
        if signal.direction != self._last_xauusd_direction:
            data["reason"] = "direction_flip"
            return True, data

        atr_ref = max(0.01, float(signal.atr or 0), float(self._last_xauusd_atr or 0))
        move = abs(float(signal.entry) - float(self._last_xauusd_entry))
        data["atr_ref"] = float(atr_ref)
        data["price_delta"] = float(move)
        data["move_threshold"] = float(0.6 * atr_ref)
        if move >= 0.6 * atr_ref:
            data["reason"] = "meaningful_price_move"
            return True, data

        data["reason"] = "cooldown_active"
        data["remaining_sec"] = float(max(0.0, cooldown - elapsed))
        logger.info(
            "[Scheduler] XAUUSD signal suppressed by cooldown "
            f"({elapsed:.0f}s < {cooldown}s, delta={move:.2f}, atr_ref={atr_ref:.2f})"
        )
        return False, data

    def _mark_xauusd_alert_sent(self, signal) -> None:
        ts = time.time()
        self._last_xauusd_alert_ts = ts
        self._last_xauusd_direction = str(signal.direction)
        self._last_xauusd_entry = float(signal.entry)
        self._last_xauusd_atr = float(signal.atr or 0)
        self._last_xauusd_signal_snapshot = {
            'ts': ts,
            'symbol': str(getattr(signal, 'symbol', 'XAUUSD') or 'XAUUSD'),
            'direction': str(getattr(signal, 'direction', '') or ''),
            'entry': float(getattr(signal, 'entry', 0.0) or 0.0),
            'confidence': float(getattr(signal, 'confidence', 0.0) or 0.0),
        }

    def _attach_xau_previous_signal_context(self, result: dict) -> None:
        try:
            snap = dict(self._last_xauusd_signal_snapshot or {})
            if not snap:
                return
            age_sec = max(0.0, time.time() - float(snap.get('ts', 0.0) or 0.0))
            if age_sec > 3600:
                return
            out = dict(snap)
            out['age_sec'] = round(age_sec, 1)
            result['previous_signal'] = out
        except Exception:
            return

    @staticmethod
    def _should_send_xauusd_scan_status(source: str) -> bool:
        src = str(source or "").strip().lower()
        if src in {"manual", "cli", "force"}:
            return True
        auto_monitor = bool(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", False))
        allow_when_auto = bool(getattr(config, "XAUUSD_SCAN_STATUS_NOTIFY_WHEN_AUTO_MONITOR", False))
        if auto_monitor and (not allow_when_auto):
            return False
        return True

    def _run_xauusd_scan(self, force_alert: bool = False, source: str = "scheduled"):
        """Execute XAUUSD scan and send alert if signal found."""
        session_info = session_manager.get_session_info()
        now_utc = datetime.now(timezone.utc)
        result = {
            "task": "xauusd",
            "source": source,
            "forced": bool(force_alert),
            "status": "unknown",
            "signal_sent": False,
            "session_info": session_info,
            "weekend": bool(now_utc.weekday() >= 5),
            "confidence_threshold": float(config.MIN_SIGNAL_CONFIDENCE),
            "cooldown": {},
            "error": "",
        }
        try:
            if not bool(session_info.get("xauusd_market_open", True)):
                result["status"] = "market_closed"
                result["diagnostics"] = {
                    "status": "market_closed",
                    "current_price": None,
                    "unmet": ["market_closed"],
                    "notes": ["xauusd_market_closed_weekend_window"],
                }
                logger.info("[Scheduler] XAUUSD scan skipped: market closed")
                try:
                    if self._should_send_xauusd_scan_status(source):
                        notifier.send_xauusd_scan_status(result)
                except Exception:
                    logger.debug("[Scheduler] XAUUSD market-closed status send failed", exc_info=True)
                return result
            logger.info("[Scheduler] Running XAUUSD scan...")
            signal = xauusd_scanner.scan()
            if signal is None:
                result["status"] = "no_signal"
                try:
                    result["diagnostics"] = xauusd_scanner.get_last_scan_diagnostics()
                except Exception:
                    result["diagnostics"] = {}
                logger.info("[Scheduler] XAUUSD: No qualifying signal")
                try:
                    self._attach_xau_previous_signal_context(result)
                    if self._should_send_xauusd_scan_status(source):
                        notifier.send_xauusd_scan_status(result)
                except Exception:
                    logger.debug("[Scheduler] XAUUSD no-signal status send failed", exc_info=True)
                return result

            result["signal"] = {
                "symbol": str(signal.symbol),
                "direction": str(signal.direction),
                "confidence": float(signal.confidence),
                "entry": float(signal.entry),
                "stop_loss": float(signal.stop_loss),
                "take_profit_2": float(signal.take_profit_2),
                "atr": float(signal.atr or 0),
            }

            if signal.confidence < config.MIN_SIGNAL_CONFIDENCE:
                result["status"] = "below_confidence"
                try:
                    result["diagnostics"] = xauusd_scanner.get_last_scan_diagnostics()
                except Exception:
                    result["diagnostics"] = {}
                logger.info(
                    "[Scheduler] XAUUSD signal below confidence threshold (%.1f < %.1f)",
                    float(signal.confidence),
                    float(config.MIN_SIGNAL_CONFIDENCE),
                )
                try:
                    self._attach_xau_previous_signal_context(result)
                    if self._should_send_xauusd_scan_status(source):
                        notifier.send_xauusd_scan_status(result)
                except Exception:
                    logger.debug("[Scheduler] XAUUSD below-confidence status send failed", exc_info=True)
                return result

            self._apply_neural_soft_adjustment(signal, source=f"xauusd_{source}")
            result["signal"]["confidence_raw"] = self._raw_confidence(signal)
            result["signal"]["confidence_adjusted"] = float(signal.confidence)

            allow_by_cooldown, cooldown_data = self._evaluate_xauusd_cooldown(signal)
            result["cooldown"] = cooldown_data
            should_send = force_alert or allow_by_cooldown
            if should_send:
                if force_alert and not allow_by_cooldown:
                    result["status"] = "sent_manual_bypass_cooldown"
                    logger.info("[Scheduler] XAUUSD manual scan: bypassing cooldown")
                else:
                    result["status"] = "sent"
                logger.info("[Scheduler] XAUUSD signal found! Sending alert...")
                sent = self._send_signal_with_trace(signal, source=f"xauusd_{source}")
                if sent:
                    result["signal_sent"] = True
                    self._mark_xauusd_alert_sent(signal)
                    if config.SIGNAL_FEEDBACK_ENABLED:
                        neural_brain.record_signal_sent(signal, source=f"xauusd_{source}")
                    if config.MT5_EXECUTE_XAUUSD:
                        handled = self._maybe_execute_xau_scheduled_live(signal, scan_source=source)
                        if not handled:
                            self._maybe_execute_mt5_signal(signal, source="xauusd")
                            self._maybe_execute_mt5_best_lane(signal, source="xauusd")
                    self._maybe_execute_ctrader_signal(signal, source=f"xauusd_{source}")
                    self._maybe_execute_persistent_canary(signal, source=f"xauusd_{source}")
                else:
                    result["status"] = "send_failed"
            else:
                result["status"] = "cooldown_suppressed"
                result["signal_sent"] = False
                try:
                    result["diagnostics"] = xauusd_scanner.get_last_scan_diagnostics()
                except Exception:
                    result["diagnostics"] = {}
                try:
                    self._attach_xau_previous_signal_context(result)
                    if self._should_send_xauusd_scan_status(source):
                        notifier.send_xauusd_scan_status(result)
                except Exception:
                    logger.debug("[Scheduler] XAUUSD cooldown status send failed", exc_info=True)

            return result
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"[Scheduler] XAUUSD scan error: {e}", exc_info=True)
            notifier.send_error(f"XAUUSD scan failed: {str(e)[:200]}")
            return result

    def _run_crypto_scan(self, force: bool = False):
        """Execute crypto scan and send alerts for top opportunities."""
        try:
            logger.info("[Scheduler] Running Crypto Sniper scan...")
            opps = crypto_sniper.get_top_n(5)

            # Manual /scan_crypto keeps original full-report behavior.
            if force or (not config.CRYPTO_AUTO_FOCUS_ONLY):
                if not opps:
                    logger.info("[Scheduler] Crypto: No qualifying signals")
                    if force:
                        notifier.send_crypto_scan_summary([])
                    return

                new_opps = [opp for opp in opps if opp.signal.symbol not in self._last_signal_symbols]
                if new_opps:
                    for opp in new_opps:
                        self._apply_neural_soft_adjustment(opp.signal, source="crypto")

                    sent_summary = notifier.send_crypto_scan_summary(new_opps)
                    if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                        for opp in new_opps:
                            neural_brain.record_signal_sent(opp.signal, source="crypto_summary")
                    top = new_opps[0]
                    if self._raw_confidence(top.signal) >= config.MIN_SIGNAL_CONFIDENCE + 5:
                        self._send_signal_with_trace(top.signal, source="crypto")

                    if config.MT5_EXECUTE_CRYPTO:
                        self._maybe_execute_mt5_batch([opp.signal for opp in new_opps], source="crypto")
                    # cTrader execution for BTCUSD/ETHUSD is handled by
                    # scalping scanner scan_btc()/scan_eth() — not here.
                    # crypto_sniper signals use exchange symbols (BTC/USDT)
                    # which are not valid on cTrader.

                    for opp in new_opps:
                        self._last_signal_symbols.add(opp.signal.symbol)
                else:
                    logger.info("[Scheduler] Crypto: All signals already alerted recently")
                return

            focus_symbols = {s.upper() for s in config.get_crypto_auto_focus_symbols()}
            focus_alias_map = {
                "BTCUSD": {"BTCUSD", "BTC/USDT"},
                "ETHUSD": {"ETHUSD", "ETH/USDT"},
            }

            def _is_focus_symbol(sym: str) -> bool:
                su = str(sym or "").upper()
                if su in focus_symbols:
                    return True
                for aliases in focus_alias_map.values():
                    if su in aliases and (aliases & focus_symbols):
                        return True
                return False

            focus_opps = [opp for opp in (opps or []) if _is_focus_symbol(getattr(opp.signal, "symbol", ""))]
            new_focus = [opp for opp in focus_opps if opp.signal.symbol not in self._last_signal_symbols]

            if new_focus:
                for opp in new_focus:
                    self._apply_neural_soft_adjustment(opp.signal, source="crypto")

                sent_summary = notifier.send_crypto_focus_status(sorted(focus_symbols), new_focus)
                if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                    for opp in new_focus:
                        neural_brain.record_signal_sent(opp.signal, source="crypto_focus")

                for opp in new_focus:
                    if self._raw_confidence(opp.signal) >= config.MIN_SIGNAL_CONFIDENCE + 5:
                        self._send_signal_with_trace(opp.signal, source="crypto")

                if config.MT5_EXECUTE_CRYPTO:
                    self._maybe_execute_mt5_batch([opp.signal for opp in new_focus], source="crypto")
                # cTrader execution for BTCUSD/ETHUSD is handled by
                # scalping scanner scan_btc()/scan_eth() — not here.

                for opp in new_focus:
                    self._last_signal_symbols.add(opp.signal.symbol)
                return

            if focus_opps:
                logger.info("[Scheduler] Crypto focus: signals exist but already alerted recently")
                return

            logger.info("[Scheduler] Crypto focus: BTC/ETH no signal")
            if config.CRYPTO_AUTO_FOCUS_NO_SIGNAL_REPORT:
                now_ts = time.time()
                min_gap = max(1, int(config.CRYPTO_AUTO_FOCUS_NO_SIGNAL_INTERVAL_MIN)) * 60
                if (now_ts - float(self._last_crypto_focus_no_signal_ts)) >= min_gap:
                    if notifier.send_crypto_focus_status(sorted(focus_symbols), []):
                        self._last_crypto_focus_no_signal_ts = now_ts

        except Exception as e:
            logger.error(f"[Scheduler] Crypto scan error: {e}", exc_info=True)

    def _run_fx_scan(self, force: bool = False):
        """Execute FX major scan and send alerts for top opportunities."""
        try:
            logger.info("[Scheduler] Running FX Major scan...")
            opps = fx_major_scanner.get_top_n(max(1, int(getattr(config, "FX_TOP_N", 5))))
            if not opps:
                diag = fx_major_scanner.get_last_scan_diagnostics()
                if diag:
                    rr = dict(diag.get("reject_reasons", {}) or {})
                    pf = dict(diag.get("prefilter", {}) or {})
                    logger.info(
                        "[Scheduler] FX diagnostics: prefilter kept=%s/%s unmapped=%s | market_closed=%s no_entry=%s no_trend=%s no_signal=%s guard_blocked=%s exception=%s",
                        pf.get("kept", diag.get("symbols", 0)),
                        pf.get("input", diag.get("symbols_input", diag.get("symbols", 0))),
                        pf.get("unmapped", 0),
                        rr.get("market_closed", 0),
                        rr.get("no_entry_data", 0),
                        rr.get("no_trend_data", 0),
                        rr.get("no_signal", 0),
                        rr.get("guard_blocked", 0),
                        rr.get("exception", 0),
                    )
                logger.info("[Scheduler] FX: No qualifying signals")
                if force:
                    notifier.send_fx_scan_summary([])
                return

            new_opps = [opp for opp in opps if opp.signal.symbol not in self._last_signal_symbols]
            send_list = new_opps if new_opps else ([] if not force else opps)
            if not send_list:
                logger.info("[Scheduler] FX: All signals already alerted recently")
                return

            for opp in send_list:
                self._apply_neural_soft_adjustment(opp.signal, source="fx")

            sent_summary = notifier.send_fx_scan_summary(send_list)
            if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                for opp in send_list:
                    neural_brain.record_signal_sent(opp.signal, source="fx_summary")

            top = send_list[0]
            if self._raw_confidence(top.signal) >= max(int(getattr(config, "FX_MIN_CONFIDENCE", config.MIN_SIGNAL_CONFIDENCE)), int(config.MIN_SIGNAL_CONFIDENCE)) + 5:
                self._send_signal_with_trace(top.signal, source="fx")

            if bool(getattr(config, "MT5_EXECUTE_FX", False)):
                self._maybe_execute_mt5_batch([opp.signal for opp in send_list], source="fx")
            self._maybe_execute_ctrader_batch([opp.signal for opp in send_list], source="fx")

            for opp in send_list:
                self._last_signal_symbols.add(opp.signal.symbol)
        except Exception as e:
            logger.error(f"[Scheduler] FX scan error: {e}", exc_info=True)

    def _scalping_cooldown_gate(self, source: str, force: bool = False) -> tuple[bool, float]:
        if force:
            return True, 0.0
        cooldown = max(0, int(getattr(config, "SCALPING_ALERT_COOLDOWN_SEC", 120) or 120))
        if cooldown <= 0:
            return True, 0.0
        now_ts = time.time()
        last_ts = float(self._last_scalping_alert_ts.get(str(source or ""), 0.0) or 0.0)
        if last_ts <= 0:
            return True, 0.0
        elapsed = now_ts - last_ts
        if elapsed >= cooldown:
            return True, 0.0
        return False, max(0.0, cooldown - elapsed)

    @staticmethod
    def _scalping_signal_fingerprint(signal) -> str:
        if signal is None:
            return ""
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        payload = {
            "symbol": str(getattr(signal, "symbol", "") or "").strip().upper(),
            "direction": str(getattr(signal, "direction", "") or "").strip().lower(),
            "pattern": str(getattr(signal, "pattern", "") or "").strip(),
            "timeframe": str(getattr(signal, "timeframe", "") or "").strip().lower(),
            "session": str(getattr(signal, "session", "") or "").strip().lower(),
            "entry": round(float(getattr(signal, "entry", 0.0) or 0.0), 4),
            "stop_loss": round(float(getattr(signal, "stop_loss", 0.0) or 0.0), 4),
            "tp1": round(float(getattr(signal, "take_profit_1", 0.0) or 0.0), 4),
            "tp2": round(float(getattr(signal, "take_profit_2", 0.0) or 0.0), 4),
            "tp3": round(float(getattr(signal, "take_profit_3", 0.0) or 0.0), 4),
            "force_mode": bool(raw.get("scalp_force_mode", False)),
            "m5_bar_utc": str(raw.get("scalp_force_last_m5_bar_utc", "") or ""),
            "h1_bar_utc": str(raw.get("scalp_force_last_h1_bar_utc", "") or ""),
        }
        return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def _scalping_duplicate_gate(self, source: str, signal, force: bool = False) -> tuple[bool, str, str]:
        if force:
            return True, "", ""
        ttl = max(0, int(getattr(config, "SCALPING_DUPLICATE_SUPPRESS_SEC", 1800) or 1800))
        if ttl <= 0:
            return True, "", ""
        fp = self._scalping_signal_fingerprint(signal)
        if not fp:
            return True, "", ""
        now_ts = time.time()
        last = dict(self._last_scalping_signal_fp.get(str(source or ""), {}) or {})
        last_fp = str(last.get("fingerprint", "") or "")
        last_ts = float(last.get("ts", 0.0) or 0.0)
        if last_fp and last_ts > 0 and last_fp == fp:
            elapsed = now_ts - last_ts
            if elapsed < float(ttl):
                remain = max(0.0, float(ttl) - elapsed)
                return False, f"duplicate_fingerprint:{remain:.0f}s", fp
        return True, "", fp

    @staticmethod
    def _store_scalping_signal(signal, row) -> int | None:
        if signal is None:
            return None
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            trigger = dict(getattr(row, "trigger", {}) or {})
            rec = ScalpSignalRecord(
                symbol=str(getattr(signal, "symbol", getattr(row, "symbol", "XAUUSD")) or "XAUUSD").strip().upper(),
                direction=str(getattr(signal, "direction", "") or "").strip().lower(),
                scalp_type=str(getattr(config, "SCALPING_ENTRY_TF", "5m") or "5m"),
                confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                entry=float(getattr(signal, "entry", 0.0) or 0.0),
                stop_loss=float(getattr(signal, "stop_loss", 0.0) or 0.0),
                take_profit_1=float(getattr(signal, "take_profit_1", 0.0) or 0.0),
                take_profit_2=float(getattr(signal, "take_profit_2", 0.0) or 0.0),
                take_profit_3=float(getattr(signal, "take_profit_3", 0.0) or 0.0),
                risk_reward=float(getattr(signal, "risk_reward", 0.0) or 0.0),
                session=str(getattr(signal, "session", "") or ""),
                pattern=str(getattr(signal, "pattern", "") or ""),
                setup_detail={
                    "source": str(getattr(row, "source", "") or raw_scores.get("scalping_source", "")),
                    "status": str(getattr(row, "status", "") or ""),
                    "reason": str(getattr(row, "reason", "") or ""),
                    "trigger": trigger,
                    "entry_tf": str(raw_scores.get("scalping_entry_tf", getattr(config, "SCALPING_ENTRY_TF", "5m"))),
                    "trigger_tf": str(raw_scores.get("scalping_trigger_tf", getattr(config, "SCALPING_M1_TRIGGER_TF", "1m"))),
                },
                macro_shock_filter=str(raw_scores.get("macro_shock", raw_scores.get("macro_state", "")) or ""),
                kill_zone=str(raw_scores.get("kill_zone", raw_scores.get("session_zone", "")) or ""),
                sweep_detected=bool(raw_scores.get("sweep_detected", raw_scores.get("sweep"))),
                fvg_detected=bool(raw_scores.get("fvg_detected", raw_scores.get("fvg"))),
            )
            return int(scalp_store.store(rec))
        except Exception as e:
            logger.warning("[Scheduler] scalping store failed: %s", e)
            return None

    def _feed_fibo_trade_results(self, sync_report: dict) -> None:
        """
        After cTrader sync, check for newly closed fibo_xauusd trades and feed
        their PnL to the fibo_advance scanner's circuit breaker.
        """
        closed_ids = list(sync_report.get("closed_position_ids") or [])
        if not closed_ids:
            return
        # Also check for reconciled deals (closed positions that matched journal)
        if int(sync_report.get("reconciled_journal", 0) or 0) == 0 and not closed_ids:
            return

        db_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
        db_path = Path(db_cfg) if db_cfg else (Path(__file__).resolve().parent / "data" / "ctrader_openapi.db")
        if not db_path.exists():
            return

        try:
            import sqlite3
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                # Get recently closed fibo_xauusd deals (last 10 minutes)
                rows = conn.execute(
                    """
                    SELECT source, COALESCE(pnl_usd, 0.0) as pnl_usd
                      FROM ctrader_deals
                     WHERE LOWER(COALESCE(source,'')) IN ('fibo_xauusd', 'xau_fibo_advance')
                       AND status IN ('closed', 'reconciled')
                       AND closed_utc >= datetime('now', '-10 minutes')
                     ORDER BY closed_utc DESC
                    """
                ).fetchall()

                for row in rows:
                    pnl = float(row["pnl_usd"] or 0.0)
                    fibo_advance_scanner.report_trade_result(pnl)
                    logger.debug("[Scheduler] fed fibo result: pnl=%.2f", pnl)
        except Exception as e:
            logger.debug("[Scheduler] _feed_fibo_trade_results error: %s", e)

    def _run_fibo_advance_scan(self, force_alert: bool = False):
        """
        Fibonacci Advance scanner — dual-speed Sniper (H4+H1) and Scout (H1+M15).
        Runs independently on its own interval. Does NOT interfere with any existing
        scanner or family routing.  Source: fibo_xauusd / Family: xau_fibo_advance.
        """
        if not bool(getattr(config, "FIBO_ADVANCE_ENABLED", True)):
            return
        if bool(getattr(config, "XAU_TOXIC_HOUR_GUARD_ENABLED", True)):
            try:
                toxic_hours = {int(h.strip()) for h in str(getattr(config, "XAU_TOXIC_HOURS_UTC", "1") or "1").split(",") if h.strip().isdigit()}
                if datetime.now(timezone.utc).hour in toxic_hours:
                    logger.debug("[FiboAdvance] Skipping — toxic hour UTC:%d", datetime.now(timezone.utc).hour)
                    return
            except Exception:
                pass
        try:
            signal = fibo_advance_scanner.scan()
            if signal is None:
                logger.debug("[FiboAdvance:Scheduler] No signal this cycle")
                return

            source = "fibo_xauusd"
            self._ensure_signal_trace(signal, source=source)
            logger.info(
                "[FiboAdvance:Scheduler] Signal | %s | %s | conf:%.1f | entry:%.2f | pattern:%s",
                signal.direction.upper(), signal.pattern,
                signal.confidence, signal.entry,
                signal.pattern,
            )

            # Telegram notification
            try:
                self._send_signal_with_trace(signal, source=source)
            except Exception as e:
                logger.debug("[FiboAdvance:Scheduler] notify error: %s", e)

            # Neural brain recording (statistics + auto-improvement)
            try:
                if bool(getattr(config, "SIGNAL_FEEDBACK_ENABLED", False)):
                    neural_brain.record_signal_sent(signal, source=source)
            except Exception as e:
                logger.debug("[FiboAdvance:Scheduler] neural_brain error: %s", e)

            # cTrader live execution (governed by CTRADER_AUTOTRADE_ENABLED + FIBO_ADVANCE_ENABLED)
            try:
                self._maybe_execute_ctrader_signal(signal, source=source)
            except Exception as e:
                logger.debug("[FiboAdvance:Scheduler] ctrader execute error: %s", e)

            # Persistent canary — family tracking + statistics (safe, non-blocking)
            try:
                self._maybe_execute_persistent_canary(signal, source=source)
            except Exception as e:
                logger.debug("[FiboAdvance:Scheduler] canary error: %s", e)

        except Exception as e:
            logger.warning("[FiboAdvance:Scheduler] scan error: %s", e, exc_info=True)

    def _run_scalping_scan(self, force: bool = False):
        """
        Dedicated scalping pipeline (separate from default signals):
        - XAUUSD + ETH (+ BTC when enabled)
        - M5 entry + M1 trigger
        """
        report = {
            "task": "scalping",
            "ok": False,
            "enabled": bool(getattr(config, "SCALPING_ENABLED", False)),
            "forced": bool(force),
            "results": [],
            "error": "",
        }
        if not report["enabled"]:
            report["error"] = "disabled"
            return report
        try:
            runs = []
            if config.scalping_symbol_enabled("XAUUSD"):
                runs.append(scalping_scanner.scan_xauusd())
            if config.scalping_symbol_enabled("ETHUSD"):
                runs.append(scalping_scanner.scan_eth())
            if config.scalping_symbol_enabled("BTCUSD"):
                runs.append(scalping_scanner.scan_btc())

            for row in runs:
                item = {
                    "source": str(row.source),
                    "symbol": str(row.symbol),
                    "status": str(row.status),
                    "reason": str(row.reason),
                    "signal_sent": False,
                    "executed_mt5": False,
                    "executed_ctrader": False,
                    "cooldown_remaining_sec": 0.0,
                }
                signal = getattr(row, "signal", None)
                if row.status != "ready" or signal is None:
                    report["results"].append(item)
                    continue

                source = str(row.source or "")
                symbol = str(getattr(signal, "symbol", getattr(row, "symbol", "")) or getattr(row, "symbol", "") or "").strip().upper()
                if (
                    symbol == "XAUUSD"
                    and bool(getattr(config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True))
                    and (not bool(session_manager.is_xauusd_market_open()))
                ):
                    item["status"] = "market_closed"
                    item["reason"] = "xauusd_market_closed_weekend_window"
                    report["results"].append(item)
                    continue
                allow, remaining = self._scalping_cooldown_gate(source, force=force)
                item["cooldown_remaining_sec"] = round(float(remaining), 2)
                if not allow:
                    item["status"] = "cooldown_suppressed"
                    item["reason"] = f"cooldown_active:{remaining:.0f}s"
                    report["results"].append(item)
                    continue
                dup_allow, dup_reason, dup_fp = self._scalping_duplicate_gate(source, signal, force=force)
                item["duplicate_fingerprint"] = dup_fp[:96] if dup_fp else ""
                if not dup_allow:
                    item["status"] = "duplicate_suppressed"
                    item["reason"] = str(dup_reason)
                    report["results"].append(item)
                    continue

                self._apply_neural_soft_adjustment(signal, source=source)

                if bool(getattr(config, "SCALPING_NOTIFY_TELEGRAM", True)):
                    sent = self._send_signal_with_trace(signal, source=source)
                    item["signal_sent"] = bool(sent)
                    if sent:
                        stored_id = self._store_scalping_signal(signal, row)
                        if stored_id:
                            item["stored_id"] = int(stored_id)
                        self._last_scalping_alert_ts[source] = time.time()
                        if bool(getattr(config, "SIGNAL_FEEDBACK_ENABLED", True)):
                            try:
                                neural_brain.record_signal_sent(signal, source=source)
                            except Exception:
                                pass

                if bool(getattr(config, "SCALPING_EXECUTE_MT5", True)) and bool(getattr(config, "MT5_ENABLED", False)):
                    allow_live, live_reason = self._allow_scalp_xau_live_mt5(signal, source=source)
                    item["mt5_live_filter"] = str(live_reason)
                    if allow_live:
                        self._maybe_execute_mt5_signal(signal, source=source)
                        self._maybe_execute_mt5_best_lane(signal, source=source)
                        item["executed_mt5"] = True
                    elif bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)) and str(getattr(signal, "symbol", "") or "").strip().upper() == "XAUUSD":
                        try:
                            self._store_shadow_signal(signal, block_reason=str(live_reason or ""))
                        except Exception:
                            pass
                ctrader_res = self._maybe_execute_ctrader_signal(signal, source=source)
                if ctrader_res is not None:
                    item["executed_ctrader"] = bool(getattr(ctrader_res, "ok", False) or getattr(ctrader_res, "dry_run", False))
                # cTrader-only shadow support:
                # Previously, XAU shadow journal was filled only when MT5 live filter blocked
                # the signal (MT5_ENABLED path). When MT5 is disabled, we still want
                # parameter trial BT (e.g. XAU_RANGE_REPAIR_MIN_CONFIDENCE) to have data,
                # so we store signals that cTrader skipped.
                if (
                    ctrader_res is None
                    and not bool(getattr(config, "MT5_ENABLED", False))
                    and bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True))
                    and (not bool(self._is_pytest_runtime()))
                    and str(symbol or "").strip().upper() == "XAUUSD"
                ):
                    try:
                        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                        block_reason = (
                            str(raw_scores.get("ctrader_pre_dispatch_reason") or "")
                            or str(raw_scores.get("ctrader_source_profile_reason") or "")
                            or str(raw_scores.get("ctrader_dispatch_reason") or "")
                            or "ctrader_skipped"
                        )
                        self._store_shadow_signal(signal, block_reason=block_reason)
                    except Exception:
                        pass
                canary_report = self._maybe_execute_persistent_canary(signal, source=source)
                item["executed_canary_mt5"] = bool(canary_report.get("mt5", False))
                item["executed_canary_ctrader"] = bool(canary_report.get("ctrader", False))

                if dup_fp:
                    self._last_scalping_signal_fp[source] = {"fingerprint": dup_fp, "ts": time.time()}

                report["results"].append(item)

            report["ok"] = True
            ready_n = sum(1 for x in report["results"] if str(x.get("status")) == "ready")
            sent_n = sum(1 for x in report["results"] if bool(x.get("signal_sent")))
            exe_n = sum(1 for x in report["results"] if bool(x.get("executed_mt5")))
            ctr_n = sum(1 for x in report["results"] if bool(x.get("executed_ctrader")))
            logger.info(
                "[Scheduler] Scalping scan complete: ready=%s sent=%s mt5=%s ctrader=%s total=%s",
                ready_n,
                sent_n,
                exe_n,
                ctr_n,
                len(report["results"]),
            )
            return report
        except Exception as e:
            report["error"] = str(e)
            logger.error("[Scheduler] Scalping scan error: %s", e, exc_info=True)
            return report

    def _run_scalping_timeout_manager(self):
        """Fast timeout close manager for scalping-only positions."""
        if not bool(getattr(config, "SCALPING_ENABLED", False)):
            return
        if not (
            bool(getattr(config, "SCALPING_EXECUTE_MT5", True))
            and bool(getattr(config, "MT5_ENABLED", False))
        ):
            return
        try:
            rpt = scalping_timeout_manager.run_cycle(
                timeout_min=max(1, int(getattr(config, "SCALPING_CLOSE_TIMEOUT_MIN", 35) or 35))
            )
            if not rpt.get("ok"):
                err = str(rpt.get("error", "") or "")
                if err in {"", "disabled", "mt5_not_connected"}:
                    if err == "mt5_not_connected":
                        now_ts = time.time()
                        if (now_ts - self._last_scalping_timeout_mt5_warn_ts) >= 600:
                            detail = str(rpt.get("error_detail", "") or "").strip()
                            if detail:
                                logger.info("[ScalpingTimeout] skipped: mt5_not_connected (%s)", detail)
                            else:
                                logger.info("[ScalpingTimeout] skipped: mt5_not_connected")
                            self._last_scalping_timeout_mt5_warn_ts = now_ts
                    return
                logger.warning("[ScalpingTimeout] cycle skipped: %s", err)
                return
            actions = list(rpt.get("actions", []) or [])
            if actions:
                logger.info(
                    "[ScalpingTimeout] attempted=%s ok=%s eligible=%s rows=%s",
                    rpt.get("close_attempted", 0),
                    rpt.get("close_ok", 0),
                    rpt.get("eligible", 0),
                    rpt.get("rows", 0),
                )
        except Exception as e:
            logger.warning("[ScalpingTimeout] cycle error: %s", e)

    def _run_gold_overview(self):
        """Send XAUUSD market overview (morning briefing)."""
        try:
            overview = xauusd_scanner.get_market_overview()
            notifier.send_xauusd_overview(overview)
        except Exception as e:
            logger.error(f"[Scheduler] Gold overview error: {e}")

    def _run_stock_scan(self):
        """Scan all currently open stock markets and send alerts."""
        try:
            logger.info("[Scheduler] Running Global Stock scan...")
            opps_all = stock_scanner.scan_all_open_markets()
            if opps_all:
                opps = stock_scanner.filter_quality(opps_all, min_score=2)
                logger.info("[Scheduler] Stocks quality filter: %d/%d passed", len(opps), len(opps_all))
                if opps:
                    send_list = [o for o in opps if o.signal.symbol not in self._last_signal_symbols]
                    for opp in (send_list or opps):
                        self._apply_neural_soft_adjustment(opp.signal, source="stocks")
                    if send_list:
                        sent_summary = notifier.send_stock_scan_summary(send_list, market_label="OPEN MARKETS (QUALITY)")
                        if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                            for opp in send_list:
                                neural_brain.record_signal_sent(opp.signal, source="stocks_quality")
                    else:
                        logger.info("[Scheduler] Stocks: quality signals already alerted recently")
                    # Detailed signal for #1 pick
                    top = opps[0]
                    if self._raw_confidence(top.signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                        notifier.send_stock_signal(top)
                    if config.MT5_EXECUTE_STOCKS:
                        self._maybe_execute_mt5_batch(
                            [opp.signal for opp in send_list or opps],
                            source="stocks",
                        )
                    for o in opps:
                        self._last_signal_symbols.add(o.signal.symbol)
                else:
                    watchlist = stock_scanner.filter_watchlist(opps_all)[:config.WATCHLIST_MAX_RESULTS]
                    logger.info(
                        "[Scheduler] Stocks watchlist filter: %d/%d passed",
                        len(watchlist),
                        len(opps_all),
                    )
                    if watchlist:
                        for opp in watchlist:
                            self._apply_neural_soft_adjustment(opp.signal, source="stocks_watchlist")
                        logger.info("[Scheduler] Stocks: no quality signals, sending filtered watchlist snapshot")
                        sent_summary = notifier.send_stock_scan_summary(
                            watchlist,
                            market_label=f"OPEN MARKETS (WATCHLIST, vol>={config.WATCHLIST_MIN_VOL_RATIO:.1f}x)",
                        )
                        if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                            for opp in watchlist:
                                neural_brain.record_signal_sent(opp.signal, source="stocks_watchlist")
                    else:
                        logger.info("[Scheduler] Stocks: no quality/watchlist signals passed filters; skipping alert")
            else:
                logger.info("[Scheduler] Stocks: No qualifying signals")
        except Exception as e:
            logger.error(f"[Scheduler] Stock scan error: {e}", exc_info=True)

    def _run_thai_scan(self):
        """Dedicated Thailand SET50 market scan — triggered at Thai open."""
        try:
            logger.info("[Scheduler] Running Thailand SET50 scan...")
            opps = stock_scanner.scan_thailand()
            if opps:
                for opp in opps:
                    self._apply_neural_soft_adjustment(opp.signal, source="stocks_thailand")
                sent_summary = notifier.send_stock_scan_summary(opps, market_label="🇹🇭 THAILAND SET50")
                if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                    for opp in opps:
                        neural_brain.record_signal_sent(opp.signal, source="stocks_thailand")
                if self._raw_confidence(opps[0].signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                    notifier.send_stock_signal(opps[0])
            else:
                diag = stock_scanner.get_last_scan_diagnostics("SET50_TH")
                if diag:
                    up = (diag.get("reject_reasons", {}) or {})
                    logger.info(
                        "[Scheduler] Thailand diagnostics: symbols=%s market_closed=%s no_data=%s no_signal=%s exception=%s",
                        diag.get("symbols", 0),
                        up.get("market_closed", 0),
                        up.get("no_entry_data", 0),
                        up.get("no_signal", 0),
                        up.get("exception", 0),
                    )
                logger.info("[Scheduler] Thailand: No qualifying signals")
        except Exception as e:
            logger.error(f"[Scheduler] Thai scan error: {e}")

    def _run_thai_vi_stock_scan(self, force: bool = False):
        """VI-style Thailand SET50 value + trend scan (off-hours friendly)."""
        try:
            logger.info("[Scheduler] Running THAILAND VALUE+TREND scan...")
            top_n = max(3, int(getattr(config, "VI_TOP_N", 10)))
            opps = stock_scanner.scan_thailand_value_trend(top_n=top_n)
            if not opps:
                diag = stock_scanner.get_last_scan_diagnostics("TH_VI")
                if diag:
                    up = (diag.get("reject_reasons", {}) or {})
                    vf = (diag.get("vi_filter", {}) or {})
                    logger.info(
                        "[Scheduler] Thailand VI diagnostics: symbols=%s market_closed=%s no_data=%s no_signal=%s exception=%s",
                        diag.get("symbols", 0),
                        up.get("market_closed", 0),
                        up.get("no_entry_data", 0),
                        up.get("no_signal", 0),
                        up.get("exception", 0),
                    )
                    if vf:
                        logger.info(
                            "[Scheduler] Thailand VI filter diagnostics: raw=%s pass=%s long=%s fail_conf=%s fail_vol=%s fail_dv=%s fail_q=%s fail_wr=%s fail_rsi=%s fail_trend=%s fail_dir=%s",
                            vf.get("raw_opportunities", 0),
                            vf.get("base_passed", 0),
                            vf.get("after_direction", 0),
                            vf.get("fail_confidence", 0),
                            vf.get("fail_volume", 0),
                            vf.get("fail_dollar_volume", 0),
                            vf.get("fail_quality", 0),
                            vf.get("fail_setup_wr", 0),
                            vf.get("fail_rsi", 0),
                            vf.get("fail_trend", 0),
                            vf.get("fail_direction", 0),
                        )
                logger.info("[Scheduler] Thailand VI scan: No qualifying candidates")
                if force:
                    notifier.send_vi_stock_summary([], region_label="🇹🇭 THAILAND", feature_override="scan_thai_vi")
                    try:
                        diag = stock_scanner.get_last_scan_diagnostics("TH_VI")
                        vf = (diag.get("vi_filter", {}) or {}) if diag else {}
                        if vf:
                            th = vf.get("thresholds", {}) or {}
                            msg = (
                                "TH VI filter diagnostics\n"
                                f"raw={vf.get('raw_opportunities',0)} pass={vf.get('base_passed',0)} long={vf.get('after_direction',0)}\n"
                                f"fail conf={vf.get('fail_confidence',0)} q={vf.get('fail_quality',0)} vol={vf.get('fail_volume',0)} dv={vf.get('fail_dollar_volume',0)} wr={vf.get('fail_setup_wr',0)} rsi={vf.get('fail_rsi',0)} trend={vf.get('fail_trend',0)} dir={vf.get('fail_direction',0)}\n"
                                f"thresholds: conf>={th.get('min_confidence')} vol>={th.get('min_vol_ratio')} dv>={th.get('min_dollar_volume')} wr>={th.get('min_setup_win_rate')} rsi={th.get('rsi_min')}-{th.get('rsi_max')} q>={th.get('min_quality_score')}"
                            )
                            notifier._send(notifier._escape(msg), feature="scan_thai_vi")
                    except Exception:
                        pass
                return

            for opp in opps:
                self._apply_neural_soft_adjustment(opp.signal, source="stocks_thai_vi")
            report_store.save_report("thai_vi", opps)
            sent_summary = notifier.send_vi_stock_summary(opps, region_label="🇹🇭 THAILAND")
            if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                for opp in opps:
                    neural_brain.record_signal_sent(opp.signal, source="stocks_thai_vi")

            top = opps[0]
            if self._raw_confidence(top.signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                notifier.send_stock_signal(top, feature_override="scan_thai_vi")
        except Exception as e:
            logger.error(f"[Scheduler] Thailand VI scan error: {e}", exc_info=True)

    def _run_us_scan(self):
        """US market scan — triggered at NYSE open."""
        try:
            logger.info("[Scheduler] Running US market scan...")
            opps_all = stock_scanner.scan_us()
            opps = stock_scanner.filter_quality(opps_all, min_score=2)
            logger.info("[Scheduler] US quality filter: %d/%d passed", len(opps), len(opps_all))
            if opps:
                for opp in opps:
                    self._apply_neural_soft_adjustment(opp.signal, source="stocks_us")
                sent_summary = notifier.send_stock_scan_summary(opps, market_label="🇺🇸 US MARKETS")
                if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                    for opp in opps:
                        neural_brain.record_signal_sent(opp.signal, source="stocks_us")
                if self._raw_confidence(opps[0].signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                    notifier.send_stock_signal(opps[0], feature_override="scan_us_open")
                if config.MT5_EXECUTE_STOCKS:
                    self._maybe_execute_mt5_batch([opp.signal for opp in opps], source="stocks_us")
            elif opps_all:
                watchlist = stock_scanner.filter_watchlist(opps_all)[:config.WATCHLIST_MAX_RESULTS]
                logger.info(
                    "[Scheduler] US watchlist filter: %d/%d passed",
                    len(watchlist),
                    len(opps_all),
                )
                if watchlist:
                    for opp in watchlist:
                        self._apply_neural_soft_adjustment(opp.signal, source="stocks_us_watchlist")
                    sent_summary = notifier.send_stock_scan_summary(
                        watchlist,
                        market_label=f"🇺🇸 US WATCHLIST (vol>={config.WATCHLIST_MIN_VOL_RATIO:.1f}x)",
                    )
                    if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                        for opp in watchlist:
                            neural_brain.record_signal_sent(opp.signal, source="stocks_us_watchlist")
                else:
                    logger.info("[Scheduler] US: no watchlist candidates passed filters; skipping alert")
        except Exception as e:
            logger.error(f"[Scheduler] US scan error: {e}")

    def _run_us_open_daytrade(self, force: bool = False):
        """Run US open day-trade selector (top 10) in first 1-2h after NY open."""
        try:
            ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            if ny_now.weekday() >= 5:
                return
            is_premarket = ny_now.time() < dt_time(9, 30)

            # Gate by local NY open window to support DST automatically.
            if (not force) and (not ((ny_now.hour == 9 and ny_now.minute >= 30) or (ny_now.hour == 10))):
                return

            day_key = ny_now.strftime("%Y-%m-%d")
            if (not force) and self._last_us_open_plan_date == day_key:
                return

            logger.info("[Scheduler] Running US OPEN day-trade selector...")
            if force and is_premarket:
                logger.info("[Scheduler] US OPEN plan forced during pre-market; using prep mode")
            opps = stock_scanner.scan_us_open_daytrade(top_n=10, allow_premarket=bool(force and is_premarket))
            if (not force):
                macro_freeze, macro_reason = self._check_us_open_macro_freeze()
                if macro_freeze:
                    logger.info("[Scheduler] US OPEN plan: macro-freeze engaged (%s)", macro_reason)
                    return
                cb_stop, cb_reason = self._check_us_open_quality_circuit_breaker(ny_now)
                if cb_stop:
                    logger.info("[Scheduler] US OPEN plan: circuit-breaker engaged (%s)", cb_reason)
                    return
            if opps:
                for opp in opps:
                    self._apply_neural_soft_adjustment(opp.signal, source="us_open")
                opps, usq_diag = self._apply_us_open_quality_filters(opps, stage="plan")
                self._log_us_open_quality_guard_diag(usq_diag, stage="plan")
                if not opps:
                    logger.info("[Scheduler] US OPEN plan: all candidates filtered by setup/symbol quality guard")
                    return
                report_store.save_report("us_open_plan", opps)
                sent_summary = notifier.send_us_open_daytrade_summary(opps)
                if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                    for opp in opps:
                        neural_brain.record_signal_sent(opp.signal, source="us_open_plan")
                # Keep one detailed alert for the best candidate.
                if self._raw_confidence(opps[0].signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                    notifier.send_stock_signal(opps[0], feature_override="scan_us_open")
                if config.MT5_EXECUTE_STOCKS:
                    self._maybe_execute_mt5_batch([opp.signal for opp in opps], source="us_open")
                self._last_us_open_plan_date = day_key
            else:
                diag = stock_scanner.get_last_us_open_diagnostics()
                if diag:
                    strict = diag.get("strict_filter", {}) or {}
                    upstream = diag.get("upstream", {}) or {}
                    up = (upstream.get("reject_reasons", {}) or {})
                    pf = (upstream.get("prefilter", {}) or {})
                    logger.info(
                        "[Scheduler] US OPEN plan diagnostics (%s): "
                        "prefilter kept=%s/%s unmapped=%s | "
                        "upstream market_closed=%s no_data=%s no_signal=%s | "
                        "strict raw=%s pass=%s fail_conf=%s fail_vol=%s fail_dv=%s",
                        diag.get("mode", "-"),
                        pf.get("kept", upstream.get("symbols", 0)),
                        pf.get("input", upstream.get("symbols_input", upstream.get("symbols", 0))),
                        pf.get("unmapped", 0),
                        up.get("market_closed", 0),
                        up.get("no_entry_data", 0),
                        up.get("no_signal", 0),
                        strict.get("total_opportunities", 0),
                        strict.get("passed", 0),
                        strict.get("fail_confidence", 0),
                        strict.get("fail_volume", 0),
                        strict.get("fail_dollar_volume", 0),
                    )
                logger.info("[Scheduler] US OPEN plan: No qualifying signals")
        except Exception as e:
            logger.error(f"[Scheduler] US OPEN plan error: {e}", exc_info=True)

    def _in_us_open_window(self, ny_now: datetime) -> bool:
        """
        Focused monitoring window:
        - Starts before NY cash open (pre-market lead time)
        - Continues through the first configured minutes after open
        """
        try:
            lead_min = max(0, int(getattr(config, "US_OPEN_SMART_PREMARKET_LEAD_MIN", 60) or 60))
        except Exception:
            lead_min = 60
        try:
            post_open_max_min = max(30, int(getattr(config, "US_OPEN_SMART_POST_OPEN_MAX_MIN", 120) or 120))
        except Exception:
            post_open_max_min = 120
        open_dt = ny_now.replace(hour=9, minute=30, second=0, microsecond=0)
        start_dt = open_dt - timedelta(minutes=lead_min)
        end_dt = open_dt + timedelta(minutes=post_open_max_min, seconds=59)
        return start_dt <= ny_now <= end_dt

    def _reset_us_open_mood_state_if_new_day(self, ny_now: datetime) -> None:
        day_key = ny_now.strftime("%Y-%m-%d")
        if self._us_open_mood_day != day_key:
            self._us_open_mood_day = day_key
            self._us_open_mood_weak_cycles = 0
            self._us_open_mood_stop_triggered = False
            self._us_open_mood_stop_reason = ""
            self._us_open_symbol_alert_ts = {}
            self._us_open_circuit_day = day_key
            self._us_open_circuit_triggered = False
            self._us_open_circuit_reason = ""
            self._us_open_symbol_recovery_state = {}
            self._us_open_quality_guard_last_diag = {"plan": {}, "monitor": {}}
            self._us_open_quality_guard_last_diag_ts = 0.0

    def _us_open_elapsed_after_open_min(self, ny_now: datetime) -> float:
        open_dt = ny_now.replace(hour=9, minute=30, second=0, microsecond=0)
        return max(0.0, (ny_now - open_dt).total_seconds() / 60.0)

    @staticmethod
    def _median_float(values: list[float]) -> float:
        vals = sorted(float(v) for v in values if v is not None)
        if not vals:
            return 0.0
        n = len(vals)
        mid = n // 2
        if n % 2 == 1:
            return vals[mid]
        return (vals[mid - 1] + vals[mid]) / 2.0

    def _update_us_open_mood_stop(self, ny_now: datetime, opps: list) -> tuple[bool, str]:
        self._reset_us_open_mood_state_if_new_day(ny_now)
        if self._us_open_mood_stop_triggered:
            return True, (self._us_open_mood_stop_reason or "mood_stop_active")
        if not bool(getattr(config, "US_OPEN_MOOD_STOP_ENABLED", True)):
            return False, "disabled"
        if ny_now.time() < dt_time(9, 30):
            return False, "premarket"

        elapsed_min = self._us_open_elapsed_after_open_min(ny_now)
        check_start_min = max(15, int(getattr(config, "US_OPEN_MOOD_CHECK_START_MIN", 45) or 45))
        if elapsed_min < check_start_min:
            self._us_open_mood_weak_cycles = 0
            return False, "warmup"

        weak_cycles_to_stop = max(2, int(getattr(config, "US_OPEN_MOOD_WEAK_CYCLES_TO_STOP", 3) or 3))
        count = len(opps or [])
        top_conf = float(getattr(opps[0].signal, "confidence", 0.0)) if count else 0.0
        median_vol = self._median_float([getattr(o, "vol_vs_avg", 0.0) for o in (opps or [])]) if count else 0.0
        min_conf = int(getattr(config, "STOCK_MIN_CONFIDENCE", 70))
        min_vol = float(getattr(config, "US_OPEN_MIN_VOL_RATIO", 1.0))

        weak = False
        if count == 0:
            weak = True
        elif count == 1 and (top_conf < (min_conf + 3) or median_vol < min_vol):
            weak = True
        elif count <= 2 and median_vol < max(0.8, min_vol * 0.9) and top_conf < (min_conf + 2):
            weak = True

        self._us_open_mood_weak_cycles = (self._us_open_mood_weak_cycles + 1) if weak else 0
        if self._us_open_mood_weak_cycles >= weak_cycles_to_stop:
            self._us_open_mood_stop_triggered = True
            self._us_open_mood_stop_reason = (
                f"weak_breadth x{self._us_open_mood_weak_cycles} "
                f"(elapsed={elapsed_min:.0f}m count={count} top_conf={top_conf:.1f} med_vol={median_vol:.2f})"
            )
            return True, self._us_open_mood_stop_reason

        return False, (
            f"monitoring(elapsed={elapsed_min:.0f}m weak={self._us_open_mood_weak_cycles}/{weak_cycles_to_stop} "
            f"count={count} top_conf={top_conf:.1f} med_vol={median_vol:.2f})"
        )

    def _check_us_open_macro_freeze(self) -> tuple[bool, str]:
        if not bool(getattr(config, "US_OPEN_MACRO_FREEZE_ENABLED", True)):
            return False, "disabled"
        try:
            min_score = max(1, int(getattr(config, "US_OPEN_MACRO_FREEZE_MIN_SCORE", 8) or 8))
            max_age_min = max(5, int(getattr(config, "US_OPEN_MACRO_FREEZE_MAX_AGE_MIN", 45) or 45))
            priority_only = bool(getattr(config, "US_OPEN_MACRO_FREEZE_PRIORITY_ONLY", True))
            min_source_q = max(0.0, min(1.0, float(getattr(config, "US_OPEN_MACRO_FREEZE_MIN_SOURCE_QUALITY", 0.70) or 0.70)))
            heads = macro_news.high_impact_headlines(hours=4, min_score=min_score, limit=8)
            now_utc = datetime.now(timezone.utc)
            fresh = []
            for h in heads:
                age_min = max(0.0, (now_utc - h.published_utc).total_seconds() / 60.0)
                if age_min > max_age_min:
                    continue
                if priority_only and (not macro_news.is_priority_theme(h)):
                    continue
                if float(getattr(h, "source_quality", 0.5) or 0.5) < min_source_q:
                    continue
                if str(getattr(h, "verification", "unverified") or "unverified").strip().lower() in {"rumor", "mixed"}:
                    continue
                fresh.append((h, age_min))
            if not fresh:
                return False, "clear"
            h, age = sorted(fresh, key=lambda x: (x[1], -int(getattr(x[0], 'score', 0))))[0]
            themes = ",".join(list(getattr(h, "themes", []) or [])[:3]) or "-"
            src_q = float(getattr(h, "source_quality", 0.0) or 0.0)
            ver = str(getattr(h, "verification", "unverified") or "unverified")
            return True, f"macro_freeze score={int(getattr(h,'score',0))} q={src_q:.2f} ver={ver} age={age:.0f}m themes={themes}"
        except Exception as e:
            logger.debug("[Scheduler] US open macro freeze check skipped: %s", e)
            return False, "error"

    def _check_us_open_quality_circuit_breaker(self, ny_now: datetime) -> tuple[bool, str]:
        self._reset_us_open_mood_state_if_new_day(ny_now)
        if self._us_open_circuit_triggered:
            return True, (self._us_open_circuit_reason or "circuit_breaker_active")
        if not bool(getattr(config, "US_OPEN_CIRCUIT_BREAKER_ENABLED", True)):
            return False, "disabled"
        if ny_now.time() < dt_time(9, 30):
            return False, "premarket"
        elapsed_min = self._us_open_elapsed_after_open_min(ny_now)
        check_start_min = max(10, int(getattr(config, "US_OPEN_CIRCUIT_BREAKER_CHECK_START_MIN", 30) or 30))
        if elapsed_min < check_start_min:
            return False, "warmup"
        try:
            rpt = neural_brain.signal_feedback_report(days=1, source_contains="us_open")
            resolved = int(rpt.get("resolved", 0) or 0)
            wins = int(rpt.get("wins", 0) or 0)
            sl = int(rpt.get("sl", 0) or 0)
            win_rate = float(rpt.get("win_rate", 0.0) or 0.0)
            avg_r = float(rpt.get("avg_r_resolved", 0.0) or 0.0)
            min_resolved = max(3, int(getattr(config, "US_OPEN_CIRCUIT_BREAKER_MIN_RESOLVED", 8) or 8))
            max_wr = float(getattr(config, "US_OPEN_CIRCUIT_BREAKER_MAX_WIN_RATE", 25) or 25)
            min_sl = max(1, int(getattr(config, "US_OPEN_CIRCUIT_BREAKER_MIN_SL", 4) or 4))
            max_avg_r = float(getattr(config, "US_OPEN_CIRCUIT_BREAKER_MAX_AVG_R", -0.50) or -0.50)
            if resolved < min_resolved:
                return False, f"insufficient_resolved={resolved}/{min_resolved}"
            catastrophic = (wins == 0 and sl >= min_sl)
            weak = (win_rate <= max_wr and avg_r <= max_avg_r and sl >= min_sl)
            if catastrophic or weak:
                self._us_open_circuit_triggered = True
                self._us_open_circuit_reason = (
                    f"cb resolved={resolved} wins={wins} sl={sl} wr={win_rate:.1f}% avgR={avg_r:.3f}"
                )
                return True, self._us_open_circuit_reason
            return False, f"ok resolved={resolved} wr={win_rate:.1f}% avgR={avg_r:.3f}"
        except Exception as e:
            logger.debug("[Scheduler] US open circuit-breaker check skipped: %s", e)
            return False, "error"

    def _get_us_open_quality_guard_stats(self, force_refresh: bool = False) -> dict:
        """Cached US-open session stats derived from dashboard payload for runtime guard decisions."""
        try:
            now_ts = time.time()
            ttl_sec = 30.0
            if (not force_refresh) and self._us_open_quality_guard_cache and (now_ts - self._us_open_quality_guard_cache_ts) <= ttl_sec:
                return dict(self._us_open_quality_guard_cache)

            dash = neural_brain.us_open_trader_dashboard(risk_pct=1.0, start_balance=1000.0)

            def _rows_to_stats(rows: list) -> dict:
                out = {}
                for row in list(rows or []):
                    try:
                        key = str((row or {}).get("setup") or "").upper().strip()
                        if not key:
                            continue
                        out[key] = {
                            "sent": int((row or {}).get("sent", 0) or 0),
                            "resolved": int((row or {}).get("resolved", 0) or 0),
                            "wins": int((row or {}).get("wins", 0) or 0),
                            "losses": int((row or {}).get("losses", 0) or 0),
                            "win_rate": float((row or {}).get("win_rate", 0.0) or 0.0),
                            "net_r": float((row or {}).get("net_r", 0.0) or 0.0),
                        }
                    except Exception:
                        continue
                return out

            if not isinstance(dash, dict) or not bool(dash.get("ok")) or str(dash.get("status")) not in {"ok", "no_data"}:
                snap = {
                    "ok": False,
                    "status": str((dash or {}).get("status") or "error"),
                    "setup_stats": {},
                    "setup_stats_by_segment": {"core": {}, "late": {}},
                    "symbol_stats": {},
                    "segments": {},
                }
            else:
                setup_rows = list(dash.get("setup_stats_all") or dash.get("win_rate_by_setup") or [])
                setup_stats = _rows_to_stats(setup_rows)
                seg_rows = dict(dash.get("setup_stats_by_segment") or {})
                setup_stats_by_segment = {
                    "core": _rows_to_stats(seg_rows.get("core") or []),
                    "late": _rows_to_stats(seg_rows.get("late") or []),
                }

                symbol_rows = list(dash.get("symbol_stats_all") or [])
                symbol_stats = {}
                for row in symbol_rows:
                    try:
                        sym = str((row or {}).get("symbol") or "").upper().strip()
                        if not sym:
                            continue
                        symbol_stats[sym] = {
                            "sent": int((row or {}).get("sent", 0) or 0),
                            "resolved": int((row or {}).get("resolved", 0) or 0),
                            "wins": int((row or {}).get("wins", 0) or 0),
                            "losses": int((row or {}).get("losses", 0) or 0),
                            "win_rate": float((row or {}).get("win_rate", 0.0) or 0.0),
                            "net_r": float((row or {}).get("net_r", 0.0) or 0.0),
                            "pending_mark_r": float((row or {}).get("pending_mark_r", 0.0) or 0.0),
                            "session_r": float((row or {}).get("session_r", 0.0) or 0.0),
                        }
                    except Exception:
                        continue

                segs = dict(dash.get("segments") or {})
                snap = {
                    "ok": True,
                    "status": str(dash.get("status") or "ok"),
                    "ny_date": str(dash.get("ny_date") or ""),
                    "summary": dict(dash.get("summary") or {}),
                    "segments": {
                        "core": dict(segs.get("core") or {}),
                        "late": dict(segs.get("late") or {}),
                        "verdict": str(segs.get("verdict") or ""),
                    },
                    "setup_stats": setup_stats,
                    "setup_stats_by_segment": setup_stats_by_segment,
                    "symbol_stats": symbol_stats,
                }
            self._us_open_quality_guard_cache = dict(snap)
            self._us_open_quality_guard_cache_ts = now_ts
            return snap
        except Exception as e:
            return {
                "ok": False,
                "status": "error",
                "error": str(e),
                "setup_stats": {},
                "setup_stats_by_segment": {"core": {}, "late": {}},
                "symbol_stats": {},
                "segments": {},
            }

    def _base_setup_name_for_us_open(self, opp) -> str:
        try:
            setup = str(getattr(opp, "base_setup_type", "") or getattr(opp, "setup_type", "") or "").upper().strip()
            if setup.startswith("BULLISH_"):
                setup = setup[len("BULLISH_"):]
            elif setup.startswith("BEARISH_"):
                setup = setup[len("BEARISH_"):]
            return setup or "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def _current_us_open_segment_for_guard(self) -> str:
        try:
            ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            elapsed = self._us_open_elapsed_after_open_min(ny_now)
            hard_stop_min = max(30, int(getattr(config, "US_OPEN_SMART_POST_OPEN_MAX_MIN", 90) or 90))
            return "core" if elapsed <= hard_stop_min else "late"
        except Exception:
            return "core"

    def _maybe_recover_us_open_symbol(self, opp, sig, sym: str, setup: str, raw_scores: dict, blocked_reason: str) -> tuple[bool, str]:
        if not bool(getattr(config, "US_OPEN_SYMBOL_RECOVERY_ENABLED", True)):
            return False, blocked_reason
        if not str(blocked_reason or "").startswith("us_open_symbol_loss_cap"):
            return False, blocked_reason
        try:
            now_ts = time.time()
            state = dict(self._us_open_symbol_recovery_state.get(sym) or {})
            max_recovers = max(1, int(getattr(config, "US_OPEN_SYMBOL_RECOVERY_MAX_PER_SYMBOL", 1) or 1))
            cooldown_sec = max(0, int(getattr(config, "US_OPEN_SYMBOL_RECOVERY_COOLDOWN_MIN", 25) or 25)) * 60
            used = int(state.get("count", 0) or 0)
            last_ts = float(state.get("ts", 0.0) or 0.0)
            if used >= max_recovers:
                return False, blocked_reason
            if cooldown_sec > 0 and (now_ts - last_ts) < cooldown_sec:
                return False, blocked_reason

            conf = float(getattr(sig, "confidence", 0.0) or 0.0)
            vol_ratio = float(getattr(opp, "vol_vs_avg", 0.0) or 0.0)
            setup_wr = float(getattr(opp, "setup_win_rate", 0.0) or 0.0)
            rank_score = float(getattr(opp, "us_open_rank_score", 0.0) or 0.0)
            min_conf = float(getattr(config, "US_OPEN_SYMBOL_RECOVERY_MIN_CONFIDENCE", 72) or 72)
            min_vol = float(getattr(config, "US_OPEN_SYMBOL_RECOVERY_MIN_VOL_RATIO", 1.15) or 1.15)
            min_setup_wr = float(getattr(config, "US_OPEN_SYMBOL_RECOVERY_MIN_SETUP_WR", 0.58) or 0.58)
            min_rank = float(getattr(config, "US_OPEN_SYMBOL_RECOVERY_MIN_RANK_SCORE", 55) or 55)
            if conf < min_conf or vol_ratio < min_vol or setup_wr < min_setup_wr or rank_score < min_rank:
                return False, blocked_reason

            self._us_open_symbol_recovery_state[sym] = {
                "count": used + 1,
                "ts": now_ts,
                "reason": "quality_recovery",
                "setup": setup,
                "conf": round(conf, 2),
                "vol": round(vol_ratio, 3),
                "setup_wr": round(setup_wr, 3),
                "rank": round(rank_score, 2),
            }
            raw_scores["us_open_symbol_recovery"] = True
            raw_scores["us_open_symbol_recovery_prev_block"] = str(blocked_reason)
            if f"🟢 US-open symbol recovery: {sym}" not in getattr(sig, "reasons", []):
                sig.reasons.append(
                    f"🟢 US-open symbol recovery: {sym} quality override (conf {conf:.1f}%, vol {vol_ratio:.2f}x, WR {setup_wr*100:.1f}%)"
                )
            return True, ""
        except Exception:
            return False, blocked_reason

    def _log_us_open_quality_guard_diag(self, diag: dict, stage: str) -> None:
        try:
            diag_copy = dict(diag or {})
            if diag_copy:
                self._us_open_quality_guard_last_diag[str(stage or "monitor")] = diag_copy
                self._us_open_quality_guard_last_diag_ts = time.time()
            if not diag_copy:
                return
            if not any(int(diag_copy.get(k, 0) or 0) for k in ("symbol_loss_cap_blocked", "symbol_recovered", "setup_hard_blocked", "setup_post_conf_cutoff", "setup_penalized", "setup_boosted")):
                return
            logger.info(
                "[Scheduler] US OPEN quality guard (%s): symbols_blocked=%s symbol_recovered=%s setup_blocked=%s post_conf_cutoff=%s setup_penalized=%s avg_penalty=%.2f setup_boosted=%s avg_boost=%.2f source=%s seg=%s",
                stage,
                int(diag_copy.get("symbol_loss_cap_blocked", 0) or 0),
                int(diag_copy.get("symbol_recovered", 0) or 0),
                int(diag_copy.get("setup_hard_blocked", 0) or 0),
                int(diag_copy.get("setup_post_conf_cutoff", 0) or 0),
                int(diag_copy.get("setup_penalized", 0) or 0),
                float(diag_copy.get("avg_penalty", 0.0) or 0.0),
                int(diag_copy.get("setup_boosted", 0) or 0),
                float(diag_copy.get("avg_boost", 0.0) or 0.0),
                str(diag_copy.get("stats_status") or "-"),
                str(diag_copy.get("segment") or "-"),
            )
        except Exception:
            pass

    def _apply_us_open_quality_filters(self, opps: list, stage: str = "monitor") -> tuple[list, dict]:
        diag = {
            "input": len(opps or []),
            "output": len(opps or []),
            "stats_status": "disabled",
            "segment": "core",
            "symbol_loss_cap_blocked": 0,
            "symbol_recovered": 0,
            "setup_hard_blocked": 0,
            "setup_post_conf_cutoff": 0,
            "setup_penalized": 0,
            "setup_boosted": 0,
            "penalty_total": 0.0,
            "boost_total": 0.0,
        }
        if not opps:
            return [], diag

        stats = self._get_us_open_quality_guard_stats()
        diag["stats_status"] = str(stats.get("status") or ("ok" if stats.get("ok") else "error"))
        if not bool(stats.get("ok")):
            return list(opps), diag

        setup_stats = dict(stats.get("setup_stats") or {})
        setup_stats_by_segment = dict(stats.get("setup_stats_by_segment") or {})
        symbol_stats = dict(stats.get("symbol_stats") or {})
        segment = self._current_us_open_segment_for_guard()
        diag["segment"] = segment
        seg_setup_stats = dict((setup_stats_by_segment.get(segment) or {}))

        use_setup = bool(getattr(config, "US_OPEN_SETUP_WEIGHTING_ENABLED", True))
        use_symbol_cap = bool(getattr(config, "US_OPEN_SYMBOL_SESSION_LOSS_CAP_ENABLED", True))
        min_conf_cut = float(getattr(config, "STOCK_MIN_CONFIDENCE", 70) or 70)

        setup_min_resolved = max(1, int(getattr(config, "US_OPEN_SETUP_STATS_MIN_RESOLVED", 6) or 6))
        poor_wr = float(getattr(config, f"US_OPEN_SETUP_POOR_WR_{segment.upper()}", getattr(config, "US_OPEN_SETUP_POOR_WR", 35)) or getattr(config, "US_OPEN_SETUP_POOR_WR", 35))
        poor_net_r = float(getattr(config, f"US_OPEN_SETUP_POOR_NET_R_{segment.upper()}", getattr(config, "US_OPEN_SETUP_POOR_NET_R", -1.0)) or getattr(config, "US_OPEN_SETUP_POOR_NET_R", -1.0))

        hard_block_enabled = bool(getattr(config, "US_OPEN_SETUP_HARD_BLOCK_ENABLED", True))
        hard_block_min_resolved = max(setup_min_resolved, int(getattr(config, "US_OPEN_SETUP_HARD_BLOCK_MIN_RESOLVED", 10) or 10))
        hard_block_max_wr = float(getattr(config, f"US_OPEN_SETUP_HARD_BLOCK_MAX_WR_{segment.upper()}", getattr(config, "US_OPEN_SETUP_HARD_BLOCK_MAX_WR", 8)) or getattr(config, "US_OPEN_SETUP_HARD_BLOCK_MAX_WR", 8))
        hard_block_max_net_r = float(getattr(config, f"US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R_{segment.upper()}", getattr(config, "US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R", -2.5)) or getattr(config, "US_OPEN_SETUP_HARD_BLOCK_MAX_NET_R", -2.5))

        boost_enabled = bool(getattr(config, "US_OPEN_SETUP_BOOST_ENABLED", True))
        boost_min_resolved = max(1, int(getattr(config, "US_OPEN_SETUP_BOOST_MIN_RESOLVED", 8) or 8))
        boost_min_wr = float(getattr(config, f"US_OPEN_SETUP_BOOST_MIN_WR_{segment.upper()}", getattr(config, "US_OPEN_SETUP_BOOST_MIN_WR", 58)) or getattr(config, "US_OPEN_SETUP_BOOST_MIN_WR", 58))
        boost_min_net_r = float(getattr(config, f"US_OPEN_SETUP_BOOST_MIN_NET_R_{segment.upper()}", getattr(config, "US_OPEN_SETUP_BOOST_MIN_NET_R", 0.8)) or getattr(config, "US_OPEN_SETUP_BOOST_MIN_NET_R", 0.8))

        max_penalty_map = {
            "CHOCH": float(getattr(config, "US_OPEN_SETUP_MAX_PENALTY_CHOCH", 8.0) or 8.0),
            "BB_SQUEEZE": float(getattr(config, "US_OPEN_SETUP_MAX_PENALTY_BB_SQUEEZE", 6.0) or 6.0),
            "OB_BOUNCE": float(getattr(config, "US_OPEN_SETUP_MAX_PENALTY_OB_BOUNCE", 3.0) or 3.0),
        }
        max_boost_ob = float(getattr(config, "US_OPEN_SETUP_MAX_BOOST_OB_BOUNCE", 2.0) or 2.0)

        sym_cap_min_resolved = max(1, int(getattr(config, "US_OPEN_SYMBOL_SESSION_LOSS_CAP_MIN_RESOLVED", 4) or 4))
        sym_cap_max_neg_r = float(getattr(config, "US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_NEG_R", -2.0) or -2.0)
        sym_cap_max_losses = max(1, int(getattr(config, "US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_LOSSES", 4) or 4))

        filtered: list = []
        for opp in list(opps):
            sig = getattr(opp, "signal", None)
            if sig is None:
                continue
            raw_scores = dict(getattr(sig, "raw_scores", {}) or {})
            sym = str(getattr(sig, "symbol", "") or "").upper().strip()
            setup = self._base_setup_name_for_us_open(opp)

            blocked_reason = ""
            # Per-symbol session loss cap (blocks repeated weak names in same session)
            if use_symbol_cap and sym:
                srec = dict(symbol_stats.get(sym) or {})
                if srec:
                    s_res = int(srec.get("resolved", 0) or 0)
                    s_losses = int(srec.get("losses", 0) or 0)
                    s_net_r = float(srec.get("net_r", 0.0) or 0.0)
                    if s_res >= sym_cap_min_resolved and (s_losses >= sym_cap_max_losses or s_net_r <= sym_cap_max_neg_r):
                        blocked_reason = (
                            f"us_open_symbol_loss_cap {sym} res={s_res} losses={s_losses} netR={s_net_r:.3f}"
                        )
                        diag["symbol_loss_cap_blocked"] += 1
                        if blocked_reason not in sig.warnings:
                            sig.warnings.append(f"🛑 US-open symbol cap: {sym} underperforming this session")
                        recovered, blocked_reason = self._maybe_recover_us_open_symbol(opp, sig, sym, setup, raw_scores, blocked_reason)
                        if recovered:
                            diag["symbol_recovered"] += 1
                            diag["symbol_loss_cap_blocked"] = max(0, int(diag.get("symbol_loss_cap_blocked", 0) or 0) - 1)

            penalty = 0.0
            boost = 0.0
            if (not blocked_reason) and use_setup and setup in max_penalty_map:
                rec = dict(seg_setup_stats.get(setup) or setup_stats.get(setup) or {})
                if rec and int(rec.get("resolved", 0) or 0) >= setup_min_resolved:
                    resolved_n = int(rec.get("resolved", 0) or 0)
                    wr = float(rec.get("win_rate", 0.0) or 0.0)
                    net_r = float(rec.get("net_r", 0.0) or 0.0)
                    if hard_block_enabled and resolved_n >= hard_block_min_resolved and wr <= hard_block_max_wr and net_r <= hard_block_max_net_r:
                        blocked_reason = f"us_open_setup_block {setup} wr={wr:.1f}% netR={net_r:.3f} res={resolved_n}"
                        diag["setup_hard_blocked"] += 1
                        if f"🛑 US-open setup blocked: {setup}" not in sig.warnings:
                            sig.warnings.append(f"🛑 US-open setup blocked: {setup} weak today ({wr:.1f}% / {net_r:.2f}R)")
                    else:
                        if wr <= poor_wr or net_r <= poor_net_r:
                            wr_sev = max(0.0, (poor_wr - wr) / max(1.0, abs(poor_wr))) if wr <= poor_wr else 0.0
                            nr_sev = max(0.0, (poor_net_r - net_r) / max(0.25, abs(poor_net_r))) if net_r <= poor_net_r else 0.0
                            severity = max(wr_sev, nr_sev)
                            max_pen = max_penalty_map.get(setup, 0.0)
                            penalty = min(max_pen, max_pen * max(0.2, min(1.0, severity))) if max_pen > 0 else 0.0
                        elif boost_enabled and setup == "OB_BOUNCE" and resolved_n >= boost_min_resolved and wr >= boost_min_wr and net_r >= boost_min_net_r:
                            wr_gain = max(0.0, (wr - boost_min_wr) / max(5.0, 100.0 - boost_min_wr))
                            nr_gain = max(0.0, (net_r - boost_min_net_r) / max(0.5, abs(boost_min_net_r)))
                            gain = min(1.0, max(wr_gain, nr_gain))
                            boost = min(max_boost_ob, max_boost_ob * max(0.2, gain)) if max_boost_ob > 0 else 0.0

            if (not blocked_reason) and penalty > 0.0:
                before = float(getattr(sig, "confidence", 0.0) or 0.0)
                sig.confidence = round(max(0.0, before - penalty), 1)
                diag["setup_penalized"] += 1
                diag["penalty_total"] += float(penalty)
                raw_scores["us_open_setup_penalty"] = round(float(penalty), 3)
                raw_scores["us_open_setup_penalty_setup"] = setup
                raw_scores["us_open_conf_before_setup_penalty"] = round(before, 3)
                if f"⚠️ US-open setup penalty: {setup}" not in sig.warnings:
                    sig.warnings.append(f"⚠️ US-open setup penalty: {setup} underperforming today ({before:.1f}%→{sig.confidence:.1f}%)")

            if (not blocked_reason) and boost > 0.0:
                before = float(getattr(sig, "confidence", 0.0) or 0.0)
                sig.confidence = round(min(99.9, before + boost), 1)
                diag["setup_boosted"] += 1
                diag["boost_total"] += float(boost)
                raw_scores["us_open_setup_boost"] = round(float(boost), 3)
                raw_scores["us_open_setup_boost_setup"] = setup
                raw_scores["us_open_conf_before_setup_boost"] = round(before, 3)
                if f"🧠 US-open setup boost: {setup}" not in sig.reasons:
                    sig.reasons.append(f"🧠 US-open setup boost: {setup} strong today ({before:.1f}%→{sig.confidence:.1f}%)")

            if (not blocked_reason) and float(getattr(sig, "confidence", 0.0) or 0.0) < min_conf_cut:
                blocked_reason = f"us_open_post_setup_conf_cutoff conf={float(getattr(sig, 'confidence', 0.0) or 0.0):.1f}<{min_conf_cut:.1f}"
                diag["setup_post_conf_cutoff"] += 1

            if blocked_reason:
                raw_scores["us_open_quality_guard_segment"] = segment
                raw_scores["us_open_quality_guard_blocked"] = True
                raw_scores["us_open_quality_guard_reason"] = blocked_reason
                setattr(sig, "raw_scores", raw_scores)
                continue

            raw_scores["us_open_quality_guard_segment"] = segment
            raw_scores["us_open_quality_guard_blocked"] = False
            setattr(sig, "raw_scores", raw_scores)
            filtered.append(opp)

        if filtered and (diag["setup_penalized"] or diag["setup_boosted"]):
            try:
                filtered.sort(
                    key=lambda o: (o.us_open_rank_score, o.dollar_volume, o.setup_win_rate, o.signal.confidence),
                    reverse=True,
                )
            except Exception:
                pass

        diag["output"] = len(filtered)
        if diag["setup_penalized"]:
            diag["avg_penalty"] = round(diag["penalty_total"] / max(1, diag["setup_penalized"]), 2)
        else:
            diag["avg_penalty"] = 0.0
        if diag["setup_boosted"]:
            diag["avg_boost"] = round(diag["boost_total"] / max(1, diag["setup_boosted"]), 2)
        else:
            diag["avg_boost"] = 0.0
        return filtered, diag

    def get_us_open_guard_status(self) -> dict:
        """Live status snapshot for US-open guardrails (macro freeze / circuit breaker / mood-stop)."""
        try:
            ny_tz = ZoneInfo("America/New_York")
            now_utc = datetime.now(timezone.utc)
            ny_now = now_utc.astimezone(ny_tz)
            self._reset_us_open_mood_state_if_new_day(ny_now)

            open_dt = ny_now.replace(hour=9, minute=30, second=0, microsecond=0)
            lead_min = max(0, int(getattr(config, "US_OPEN_SMART_PREMARKET_LEAD_MIN", 60) or 60))
            post_open_max = max(30, int(getattr(config, "US_OPEN_SMART_POST_OPEN_MAX_MIN", 120) or 120))
            window_start = open_dt - timedelta(minutes=lead_min)
            window_end = open_dt + timedelta(minutes=post_open_max, seconds=59)
            in_window = self._in_us_open_window(ny_now)
            premarket = ny_now < open_dt
            elapsed_after_open_min = self._us_open_elapsed_after_open_min(ny_now)

            # Macro freeze details (with ETA until oldest relevant headline ages out).
            macro_enabled = bool(getattr(config, "US_OPEN_MACRO_FREEZE_ENABLED", True))
            macro_min_score = max(1, int(getattr(config, "US_OPEN_MACRO_FREEZE_MIN_SCORE", 8) or 8))
            macro_max_age_min = max(5, int(getattr(config, "US_OPEN_MACRO_FREEZE_MAX_AGE_MIN", 45) or 45))
            macro_priority_only = bool(getattr(config, "US_OPEN_MACRO_FREEZE_PRIORITY_ONLY", True))
            macro_min_source_q = max(0.0, min(1.0, float(getattr(config, "US_OPEN_MACRO_FREEZE_MIN_SOURCE_QUALITY", 0.70) or 0.70)))
            macro_active, macro_reason = self._check_us_open_macro_freeze()
            macro_release_eta_min = None
            macro_headline = ""
            try:
                heads = macro_news.high_impact_headlines(hours=4, min_score=macro_min_score, limit=8)
                fresh = []
                for h in heads:
                    age_min = max(0.0, (now_utc - h.published_utc).total_seconds() / 60.0)
                    if age_min > macro_max_age_min:
                        continue
                    if macro_priority_only and (not macro_news.is_priority_theme(h)):
                        continue
                    if float(getattr(h, "source_quality", 0.5) or 0.5) < macro_min_source_q:
                        continue
                    if str(getattr(h, "verification", "unverified") or "unverified").strip().lower() in {"rumor", "mixed"}:
                        continue
                    fresh.append((h, age_min))
                if fresh:
                    # Freeze releases when all relevant headlines age out; nearest release is oldest threshold among current fresh set.
                    rems = [max(0.0, macro_max_age_min - age) for _, age in fresh]
                    macro_release_eta_min = round(min(rems), 1)
                    h, age = sorted(fresh, key=lambda x: (x[1], -int(getattr(x[0], 'score', 0))))[0]
                    macro_headline = str(getattr(h, 'title', '') or '')[:180]
            except Exception:
                pass

            # Circuit breaker status + next release (next NY business day open, because reset is day-based).
            cb_active, cb_reason = self._check_us_open_quality_circuit_breaker(ny_now)
            cb_check_start_min = max(10, int(getattr(config, "US_OPEN_CIRCUIT_BREAKER_CHECK_START_MIN", 30) or 30))
            cb_release_eta_min = None
            cb_release_at_ny = ""
            if cb_active and str(cb_reason or '').startswith('cb '):
                nxt = open_dt + timedelta(days=1)
                while nxt.weekday() >= 5:
                    nxt += timedelta(days=1)
                nxt = nxt.replace(hour=9, minute=30, second=0, microsecond=0)
                cb_release_at_ny = nxt.strftime('%Y-%m-%d %H:%M NY')
                cb_release_eta_min = round(max(0.0, (nxt - ny_now).total_seconds() / 60.0), 1)
            elif (not cb_active) and str(cb_reason or '').lower() == 'warmup':
                cb_release_eta_min = round(max(0.0, cb_check_start_min - elapsed_after_open_min), 1)
                cb_release_at_ny = f'after +{cb_check_start_min}m from open'

            # Mood-stop status (already day-scoped, reset next session day).
            mood_active = bool(self._us_open_mood_stop_triggered)
            mood_reason = str(self._us_open_mood_stop_reason or '')
            mood_release_at_ny = ""
            if mood_active:
                nxt = open_dt + timedelta(days=1)
                while nxt.weekday() >= 5:
                    nxt += timedelta(days=1)
                nxt = nxt.replace(hour=9, minute=30, second=0, microsecond=0)
                mood_release_at_ny = nxt.strftime('%Y-%m-%d %H:%M NY')

            qg_stats = self._get_us_open_quality_guard_stats(force_refresh=False)
            qg_last_plan = dict((self._us_open_quality_guard_last_diag or {}).get('plan') or {})
            qg_last_monitor = dict((self._us_open_quality_guard_last_diag or {}).get('monitor') or {})
            qg_segment = self._current_us_open_segment_for_guard()
            qg_symbol_stats = dict((qg_stats or {}).get('symbol_stats') or {})
            cap_min_res = max(1, int(getattr(config, 'US_OPEN_SYMBOL_SESSION_LOSS_CAP_MIN_RESOLVED', 4) or 4))
            cap_max_neg_r = float(getattr(config, 'US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_NEG_R', -2.0) or -2.0)
            cap_max_losses = max(1, int(getattr(config, 'US_OPEN_SYMBOL_SESSION_LOSS_CAP_MAX_LOSSES', 4) or 4))
            capped_symbols = []
            for sym, rec in qg_symbol_stats.items():
                try:
                    s_res = int((rec or {}).get('resolved', 0) or 0)
                    s_losses = int((rec or {}).get('losses', 0) or 0)
                    s_net_r = float((rec or {}).get('net_r', 0.0) or 0.0)
                    if s_res >= cap_min_res and (s_losses >= cap_max_losses or s_net_r <= cap_max_neg_r):
                        capped_symbols.append({'symbol': sym, 'resolved': s_res, 'losses': s_losses, 'net_r': round(s_net_r, 3)})
                except Exception:
                    continue
            capped_symbols = sorted(capped_symbols, key=lambda x: (x['net_r'], -x['losses']))[:10]
            qg_summary = {
                'stats_ok': bool((qg_stats or {}).get('ok')),
                'stats_status': str((qg_stats or {}).get('status') or '-'),
                'segment': qg_segment,
                'segments_verdict': str(((qg_stats or {}).get('segments') or {}).get('verdict') or ''),
                'cache_age_sec': round(max(0.0, time.time() - float(self._us_open_quality_guard_cache_ts or 0.0)), 1) if self._us_open_quality_guard_cache_ts else None,
                'last_diag_age_sec': round(max(0.0, time.time() - float(self._us_open_quality_guard_last_diag_ts or 0.0)), 1) if self._us_open_quality_guard_last_diag_ts else None,
                'last_plan': qg_last_plan,
                'last_monitor': qg_last_monitor,
                'capped_symbols': capped_symbols,
                'recovery_state': dict(self._us_open_symbol_recovery_state or {}),
            }

            return {
                'ok': True,
                'now_utc': now_utc.strftime('%Y-%m-%d %H:%M UTC'),
                'now_ny': ny_now.strftime('%Y-%m-%d %H:%M NY'),
                'weekday_ny': ny_now.weekday(),
                'in_us_open_window': bool(in_window),
                'premarket': bool(premarket),
                'elapsed_after_open_min': round(float(elapsed_after_open_min), 1),
                'window_start_ny': window_start.strftime('%Y-%m-%d %H:%M NY'),
                'window_end_ny': window_end.strftime('%Y-%m-%d %H:%M NY'),
                'macro_freeze': {
                    'enabled': macro_enabled,
                    'active': bool(macro_active),
                    'reason': str(macro_reason or ''),
                    'min_score': macro_min_score,
                    'max_age_min': macro_max_age_min,
                    'min_source_quality': macro_min_source_q,
                    'priority_only': macro_priority_only,
                    'release_eta_min': macro_release_eta_min,
                    'headline': macro_headline,
                },
                'circuit_breaker': {
                    'enabled': bool(getattr(config, 'US_OPEN_CIRCUIT_BREAKER_ENABLED', True)),
                    'active': bool(cb_active),
                    'reason': str(cb_reason or ''),
                    'check_start_min': cb_check_start_min,
                    'release_eta_min': cb_release_eta_min,
                    'release_at_ny': cb_release_at_ny,
                },
                'mood_stop': {
                    'enabled': bool(getattr(config, 'US_OPEN_MOOD_STOP_ENABLED', True)),
                    'active': mood_active,
                    'weak_cycles': int(self._us_open_mood_weak_cycles or 0),
                    'weak_cycles_to_stop': max(2, int(getattr(config, 'US_OPEN_MOOD_WEAK_CYCLES_TO_STOP', 3) or 3)),
                    'reason': mood_reason,
                    'release_at_ny': mood_release_at_ny,
                },
                'symbol_cooldown': {
                    'enabled': int(getattr(config, 'US_OPEN_SYMBOL_ALERT_COOLDOWN_MIN', 20) or 20) > 0,
                    'cooldown_min': int(getattr(config, 'US_OPEN_SYMBOL_ALERT_COOLDOWN_MIN', 20) or 20),
                    'tracked_symbols': len(self._us_open_symbol_alert_ts or {}),
                    'record_new_only': bool(getattr(config, 'US_OPEN_RECORD_NEW_SYMBOLS_ONLY', True)),
                },
                'quality_guard': qg_summary,
            }
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def _maybe_us_open_session_checkin(self, ny_now: datetime, force: bool = False) -> None:
        if not bool(getattr(config, "US_OPEN_SESSION_CHECKIN_ENABLED", True)):
            return
        day_key = ny_now.strftime("%Y-%m-%d")
        if (not force) and self._us_open_session_checkin_day == day_key:
            return
        notifier.send_us_open_session_checkin(
            interval_min=max(3, int(config.US_OPEN_SMART_INTERVAL_MIN)),
            premarket_lead_min=max(0, int(getattr(config, "US_OPEN_SMART_PREMARKET_LEAD_MIN", 60))),
            no_opp_ping_min=max(3, int(getattr(config, "US_OPEN_SMART_NO_OPP_PING_MIN", 15))),
        )
        self._us_open_session_checkin_day = day_key

    def _maybe_send_us_open_quality_recap(self, force: bool = False) -> None:
        if not bool(getattr(config, "SIGNAL_FEEDBACK_ENABLED", True)):
            return
        try:
            interval_min = max(5, int(getattr(config, "US_OPEN_QUALITY_REPORT_INTERVAL_MIN", 15) or 15))
            now_ts = time.time()
            if (not force) and (now_ts - self._us_open_quality_last_sent_ts) < (interval_min * 60):
                return
            report = neural_brain.signal_feedback_report(days=1, source_contains="us_open")
            key = "|".join(
                str(report.get(k, 0))
                for k in ("sent", "resolved", "pending", "tp1", "tp2", "tp3", "sl")
            )
            if (not force) and self._us_open_quality_last_key == key and (now_ts - self._us_open_quality_last_sent_ts) < (interval_min * 60 * 2):
                return
            notifier.send_us_open_signal_quality_recap(report)
            self._us_open_quality_last_key = key
            self._us_open_quality_last_sent_ts = now_ts
        except Exception as e:
            logger.debug("[Scheduler] US open quality recap skipped: %s", e)

    def _run_signal_monitor_auto_push(self, force: bool = False) -> dict:
        report = {
            "task": "signal_monitor_auto_push",
            "ok": False,
            "enabled": bool(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", False)),
            "symbols": [],
            "results": [],
            "error": "",
        }
        if (not report["enabled"]) and (not force):
            report["error"] = "disabled"
            return report
        try:
            from notifier.admin_bot import admin_bot

            symbols = list(config.get_signal_monitor_auto_symbols())
            window_mode = str(config.get_signal_monitor_auto_window_mode() or "today")
            days = max(1, min(30, int(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_DAYS", 1) or 1)))
            report["symbols"] = symbols
            for symbol in symbols:
                payload = admin_bot._build_signal_monitor_payload(symbol=symbol, window_mode=window_mode, days=days)
                symbol_up = str(payload.get("symbol") or symbol).strip().upper()
                targets = access_manager.list_entitled_user_ids("signal_monitor", signal_symbol=symbol_up)
                item = {
                    "symbol": symbol_up,
                    "targets": len(targets),
                    "sent": 0,
                    "status": str(payload.get("status", "unknown") or "unknown"),
                }
                if not targets:
                    report["results"].append(item)
                    continue
                for uid in targets:
                    chat_id = int(uid)
                    try:
                        lang = str(access_manager.get_user_language_preference(chat_id) or "en").strip().lower()
                    except Exception:
                        lang = "en"
                    if lang not in {"th", "en", "de"}:
                        lang = "en"
                    text = admin_bot._format_signal_monitor_text(payload, lang=lang, chat_id=chat_id)
                    sent = notifier._send(
                        text,
                        parse_mode=None,
                        chat_id=chat_id,
                        feature="signal_monitor",
                        signal_symbol=symbol_up,
                    )
                    if sent:
                        item["sent"] = int(item["sent"]) + 1
                report["results"].append(item)

            report["ok"] = True
            sent_total = sum(int(x.get("sent", 0) or 0) for x in report["results"])
            logger.info(
                "[Scheduler] Signal monitor auto-push complete: symbols=%s sent=%s",
                len(report["results"]),
                sent_total,
            )
            return report
        except Exception as e:
            report["error"] = str(e)
            logger.error("[Scheduler] signal monitor auto-push error: %s", e, exc_info=True)
            return report

    def _build_mt5_lane_scorecard(self, lookback_days: int = 1) -> dict:
        report = {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": max(1, int(lookback_days or 1)),
            "db_path": str(getattr(mt5_autopilot_core, "db_path", "") or ""),
            "ok": False,
            "error": "",
            "summary": {},
            "lanes": {},
            "sources": [],
        }
        db_path = Path(str(report["db_path"] or "")).expanduser()
        if not db_path.exists():
            report["error"] = "mt5_autopilot_db_missing"
            return report

        since_dt = datetime.now(timezone.utc) - timedelta(days=report["lookback_days"])
        since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            with sqlite3.connect(str(db_path), timeout=15) as conn:
                rows = conn.execute(
                    """
                    SELECT COALESCE(source,''), COALESCE(signal_symbol,''), COALESCE(mt5_status,''), COALESCE(mt5_message,''),
                           COALESCE(resolved,0), outcome, COALESCE(pnl,0.0), COALESCE(canary_mode,0)
                      FROM mt5_execution_journal
                     WHERE created_at >= ?
                     ORDER BY created_at DESC, id DESC
                    """,
                    (since_iso,),
                ).fetchall()
        except Exception as e:
            report["error"] = f"db_query_error:{e}"
            return report

        def _new_bucket() -> dict:
            return {
                "sent": 0,
                "filled": 0,
                "skipped": 0,
                "guard_blocked": 0,
                "blocked": 0,
                "error": 0,
                "unmapped": 0,
                "resolved": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "canary_fills": 0,
                "_reasons": {},
            }

        lanes = {k: _new_bucket() for k in ("main", "winner", "bypass")}
        by_source: dict[str, dict] = {}
        total = _new_bucket()

        for source, signal_symbol, mt5_status, mt5_message, resolved, outcome, pnl, canary_mode in list(rows or []):
            src = str(source or "").strip()
            status = str(mt5_status or "").strip().lower() or "unknown"
            lane = self._mt5_lane_key_from_source(src)
            bucket = lanes.setdefault(lane, _new_bucket())
            source_bucket = by_source.setdefault(src or "-", _new_bucket())
            targets = (bucket, source_bucket, total)
            for tgt in targets:
                tgt["sent"] += 1
                if status in tgt:
                    tgt[status] += 1
                if status == "filled":
                    tgt["filled"] += 0  # explicit no-op; kept for readability
                if status != "filled":
                    reason = str(mt5_message or "").strip()
                    if reason:
                        reasons = tgt["_reasons"]
                        reasons[reason] = int(reasons.get(reason, 0) or 0) + 1
            if status == "filled":
                for tgt in targets:
                    tgt["filled"] += 1
                    if bool(canary_mode):
                        tgt["canary_fills"] += 1
                if int(resolved or 0) == 1:
                    pnl_v = float(pnl or 0.0)
                    for tgt in targets:
                        tgt["resolved"] += 1
                        tgt["pnl"] += pnl_v
                        if outcome == 1 or pnl_v > 0:
                            tgt["wins"] += 1
                        elif outcome == 0 or pnl_v < 0:
                            tgt["losses"] += 1

        def _finalize(bucket: dict) -> dict:
            out = {k: v for k, v in bucket.items() if not str(k).startswith("_")}
            sent = max(0, int(out.get("sent", 0) or 0))
            filled = max(0, int(out.get("filled", 0) or 0))
            wins = max(0, int(out.get("wins", 0) or 0))
            losses = max(0, int(out.get("losses", 0) or 0))
            resolved = max(0, int(out.get("resolved", 0) or 0))
            out["fill_rate_pct"] = round((filled * 100.0 / sent), 1) if sent > 0 else 0.0
            out["win_rate_pct"] = round((wins * 100.0 / (wins + losses)), 1) if (wins + losses) > 0 else 0.0
            out["pnl"] = round(float(out.get("pnl", 0.0) or 0.0), 2)
            top_reason = ""
            top_count = 0
            for reason, count in dict(bucket.get("_reasons") or {}).items():
                if int(count or 0) > top_count:
                    top_reason = str(reason)
                    top_count = int(count or 0)
            out["top_reason"] = top_reason
            out["top_reason_count"] = top_count
            out["resolved"] = resolved
            return out

        report["summary"] = _finalize(total)
        report["lanes"] = {lane: _finalize(bucket) for lane, bucket in lanes.items()}
        report["sources"] = []
        for source, bucket in sorted(
            by_source.items(),
            key=lambda kv: (
                -int((kv[1] or {}).get("resolved", 0) or 0),
                -float((kv[1] or {}).get("pnl", 0.0) or 0.0),
                str(kv[0]),
            ),
        ):
            item = _finalize(bucket)
            item["source"] = str(source)
            item["lane"] = self._mt5_lane_key_from_source(source)
            report["sources"].append(item)
        report["ok"] = True
        return report

    @staticmethod
    def _format_mt5_lane_scorecard_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"MT5 lane scorecard unavailable: {str((report or {}).get('error', 'unknown'))}"

        lookback = max(1, int((report or {}).get("lookback_days", 1) or 1))
        lanes = dict((report or {}).get("lanes") or {})
        lines = [f"MT5 LANE SCORECARD [{lookback}d]"]
        for lane in ("main", "winner", "bypass"):
            row = dict(lanes.get(lane) or {})
            lines.append(
                f"{lane}: sent {int(row.get('sent', 0) or 0)} | "
                f"fill {int(row.get('filled', 0) or 0)} ({float(row.get('fill_rate_pct', 0.0) or 0.0):.1f}%) | "
                f"WR {float(row.get('win_rate_pct', 0.0) or 0.0):.1f}% | "
                f"PnL {float(row.get('pnl', 0.0) or 0.0):+.2f}$"
            )
        top_sources = list((report or {}).get("sources") or [])[:4]
        if top_sources:
            lines.append("Top sources:")
            for row in top_sources:
                lines.append(
                    f"- {str(row.get('source') or '-')}: "
                    f"fill {int(row.get('filled', 0) or 0)} | "
                    f"resolved {int(row.get('resolved', 0) or 0)} | "
                    f"WR {float(row.get('win_rate_pct', 0.0) or 0.0):.1f}% | "
                    f"PnL {float(row.get('pnl', 0.0) or 0.0):+.2f}$"
                )
        summary = dict((report or {}).get("summary") or {})
        top_reason = str(summary.get("top_reason", "") or "").strip()
        if top_reason:
            lines.append(f"Top block: {top_reason}")
        lines.append(f"UTC: {str((report or {}).get('generated_at', '-') or '-')}")
        return "\n".join(lines)

    def _run_mt5_lane_scorecard(self, force: bool = False) -> dict:
        report = self._build_mt5_lane_scorecard(
            lookback_days=max(1, int(getattr(config, "MT5_LANE_SCORECARD_LOOKBACK_DAYS", 1) or 1))
        )
        try:
            report_store.save_report("mt5_lane_scorecard", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] MT5 lane scorecard ready: main_pnl=%s winner_pnl=%s bypass_pnl=%s",
                ((report.get("lanes") or {}).get("main") or {}).get("pnl", 0.0),
                ((report.get("lanes") or {}).get("winner") or {}).get("pnl", 0.0),
                ((report.get("lanes") or {}).get("bypass") or {}).get("pnl", 0.0),
            )
        else:
            logger.warning("[Scheduler] MT5 lane scorecard failed: %s", report.get("error"))
        if bool(getattr(config, "MT5_LANE_SCORECARD_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_mt5_lane_scorecard_text(report),
                    parse_mode=None,
                    feature="mt5_train",
                )
            except Exception:
                logger.debug("[Scheduler] MT5 lane scorecard telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_crypto_weekend_scorecard_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"CRYPTO WEEKEND SCORECARD\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "CRYPTO WEEKEND SCORECARD",
            f"Days: {int((report or {}).get('days', 0) or 0)} | Model rows: {int((report or {}).get('model_rows', 0) or 0)} | MT5 live rows: {int((report or {}).get('live_rows', 0) or 0)} | cTrader rows: {int((report or {}).get('ctrader_live_rows', 0) or 0)}",
        ]
        for row in list((report or {}).get("symbols") or [])[:2]:
            weekend = dict(row.get("weekend") or {})
            rec = dict(row.get("recommended_weekend_profile") or {})
            ctrader_live = dict(row.get("ctrader_live") or {})
            sessions = ",".join(list(rec.get("allowed_sessions") or [])[:3]) or "-"
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} weekend {int(weekend.get('resolved', 0) or 0)} "
                f"WR {float(weekend.get('win_rate', 0.0) or 0.0)*100:.1f}% "
                f"PnL {float(weekend.get('pnl_usd', 0.0) or 0.0):+.2f}$ "
                f"| cTrader {int(ctrader_live.get('resolved', 0) or 0)} "
                f"WR {float(ctrader_live.get('win_rate', 0.0) or 0.0)*100:.1f}% "
                f"PnL {float(ctrader_live.get('pnl_usd', 0.0) or 0.0):+.2f}$ "
                f"| next minConf {float(rec.get('min_confidence', 0.0) or 0.0):.1f} "
                f"| sessions {sessions}"
            )
        return "\n".join(lines)

    def _run_crypto_weekend_scorecard(self, force: bool = False) -> dict:
        report = dict(
            scalping_forward_analyzer.build_crypto_weekend_scorecard(
                days=max(1, int(getattr(config, "CRYPTO_WEEKEND_SCORECARD_LOOKBACK_DAYS", 14) or 14))
            )
            or {}
        )
        try:
            report_store.save_report("crypto_weekend_scorecard", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Crypto weekend scorecard ready: rows=%s symbols=%s",
                int(report.get("model_rows", 0) or 0),
                len(list(report.get("symbols") or [])),
            )
        else:
            logger.warning("[Scheduler] Crypto weekend scorecard failed: %s", report.get("error"))
        if bool(getattr(config, "CRYPTO_WEEKEND_SCORECARD_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_crypto_weekend_scorecard_text(report),
                    parse_mode=None,
                    feature="crypto",
                )
            except Exception:
                logger.debug("[Scheduler] Crypto weekend scorecard telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_winner_mission_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"WINNER MISSION REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "WINNER MISSION REPORT",
            f"Days: {int((report or {}).get('days', 0) or 0)}",
        ]
        for row in list((report or {}).get("symbols") or [])[:3]:
            live_total = dict(row.get("live_total") or {})
            model = dict(row.get("model") or {})
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} "
                f"| mode {str(row.get('recommended_live_mode') or '-')} "
                f"| live WR {float(live_total.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| live PnL {float(live_total.get('pnl_usd', 0.0) or 0.0):+.2f}$ "
                f"| model WR {float(model.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| bias {str(row.get('entry_bias') or '-')}"
            )
        lines.append(f"UTC: {str((report or {}).get('generated_at', '-') or '-')}")
        return "\n".join(lines)

    def _run_winner_mission_report(self, force: bool = False) -> dict:
        report = dict(
            scalping_forward_analyzer.build_winner_mission_report(
                days=max(1, int(getattr(config, "WINNER_MISSION_REPORT_LOOKBACK_DAYS", 14) or 14))
            )
            or {}
        )
        try:
            report_store.save_report("winner_mission_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Winner mission report ready: symbols=%s",
                len(list(report.get("symbols") or [])),
            )
        else:
            logger.warning("[Scheduler] Winner mission report failed: %s", report.get("error"))
        if bool(getattr(config, "WINNER_MISSION_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_winner_mission_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Winner mission report telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_missed_opportunity_audit_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"MISSED OPPORTUNITY AUDIT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        lines = [
            "MISSED OPPORTUNITY AUDIT",
            f"Days: {int((report or {}).get('days', 0) or 0)} | missed {int(summary.get('missed_rows', 0) or 0)} | allow {int(summary.get('allow_rows', 0) or 0)}",
        ]
        for row in list((report or {}).get("recommendations") or [])[:3]:
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} {str(row.get('source', '-') or '-')} "
                f"| action {str(row.get('action', '-') or '-')} "
                f"| WR {float(row.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| PnL {float(row.get('pnl_usd', 0.0) or 0.0):+.2f}$ "
                f"| canaryConf {str(row.get('proposed_canary_min_confidence', '-') or '-')}"
            )
        return "\n".join(lines)

    def _run_missed_opportunity_audit(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_missed_opportunity_audit_report(
                days=max(1, int(getattr(config, "MISSED_OPPORTUNITY_AUDIT_LOOKBACK_DAYS", 14) or 14))
            )
            or {}
        )
        try:
            report_store.save_report("missed_opportunity_audit_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Missed opportunity audit ready: missed_rows=%s positive_groups=%s",
                int(((report.get("summary") or {}).get("missed_rows", 0) or 0)),
                int(((report.get("summary") or {}).get("missed_positive_groups", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Missed opportunity audit failed: %s", report.get("error"))
        if bool(getattr(config, "MISSED_OPPORTUNITY_AUDIT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_missed_opportunity_audit_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Missed opportunity audit telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_auto_apply_live_profile_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"AUTO APPLY LIVE PROFILE\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "AUTO APPLY LIVE PROFILE",
            f"Status: {str((report or {}).get('status', '-') or '-')}",
        ]
        rollback = dict((report or {}).get("rollback") or {})
        if rollback:
            lines.append(f"Rollback: {str(rollback.get('status', '-') or '-')}")
        for key, value in list(((report or {}).get("candidate_changes") or {}).items())[:5]:
            lines.append(f"{str(key)}={str(value)}")
        return "\n".join(lines)

    def _run_auto_apply_live_profile(self, force: bool = False) -> dict:
        report = dict(live_profile_autopilot.auto_apply_live_profile() or {})
        try:
            report_store.save_report("auto_apply_live_profile_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Auto-apply live profile status=%s changes=%s promotions=%s",
                str(report.get("status", "") or ""),
                len(dict(report.get("candidate_changes") or {})),
                len(list(report.get("strategy_promotions") or [])),
            )
        else:
            logger.warning("[Scheduler] Auto-apply live profile failed: %s", report.get("error"))
        if bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_auto_apply_live_profile_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Auto-apply live profile telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_canary_post_trade_audit_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"CANARY POST-TRADE AUDIT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        lines = [
            "CANARY POST-TRADE AUDIT",
            (
                f"Status: {str((report or {}).get('status', '-') or '-')} | "
                f"closed {int(summary.get('total_canary_closed', 0) or 0)} | "
                f"BTC/ETH {int(summary.get('btc_eth_canary_closed', 0) or 0)}"
            ),
        ]
        for row in list((report or {}).get("focus_queue") or [])[:3]:
            canary = dict(row.get("canary") or {})
            control = dict(row.get("control_total") or {})
            lines.append(
                f"{str(row.get('backend', '-') or '-')} {str(row.get('symbol', '-') or '-')} {str(row.get('base_source', '-') or '-')} "
                f"| canary {int(canary.get('wins', 0) or 0)}/{int(canary.get('resolved', 0) or 0)} "
                f"{float(canary.get('pnl_usd', 0.0) or 0.0):+.2f}$ "
                f"| ctrl WR {float(control.get('win_rate', 0.0) or 0.0) * 100.0:.1f}%"
            )
        return "\n".join(lines)

    def _run_canary_post_trade_audit(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_canary_post_trade_audit_report(
                days=max(1, int(getattr(config, "CANARY_POST_TRADE_AUDIT_LOOKBACK_DAYS", 14) or 14))
            )
            or {}
        )
        try:
            report_store.save_report("canary_post_trade_audit_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Canary post-trade audit status=%s closed=%s groups=%s",
                str(report.get("status", "") or ""),
                int(((report.get("summary") or {}).get("total_canary_closed", 0) or 0)),
                int(((report.get("summary") or {}).get("groups", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Canary post-trade audit failed: %s", report.get("error"))
        if bool(getattr(config, "CANARY_POST_TRADE_AUDIT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_canary_post_trade_audit_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Canary post-trade audit telegram send failed", exc_info=True)
        if bool(report.get("ok")) and bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ENABLED", False)):
            try:
                self._run_auto_apply_live_profile(force=False)
            except Exception:
                logger.debug("[Scheduler] Canary audit auto-apply follow-up failed", exc_info=True)
        if bool(getattr(config, "STRATEGY_LAB_REPORT_ENABLED", False)):
            try:
                self._run_strategy_lab_report(force=False)
            except Exception:
                logger.debug("[Scheduler] Canary audit strategy-lab follow-up failed", exc_info=True)
        if bool(getattr(config, "MISSION_PROGRESS_REPORT_ENABLED", False)):
            try:
                self._run_mission_progress_report(force=False)
            except Exception:
                logger.debug("[Scheduler] Canary audit mission-progress follow-up failed", exc_info=True)
        return report

    def _run_ctrader_data_integrity_report(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_ctrader_data_integrity_report(
                days=max(1, int(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS", 180) or 180)),
                repair=bool(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_REPAIR_ON_RUN", True)),
            )
            or {}
        )
        try:
            report_store.save_report("ctrader_data_integrity_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            summary = dict(report.get("summary") or {})
            logger.info(
                "[Scheduler] cTrader data integrity ready: journal=%s deals=%s repaired=%s remaining=%s",
                int(summary.get("journal_rows", 0) or 0),
                int(summary.get("deal_rows", 0) or 0),
                int(summary.get("deal_rows_repaired", 0) or 0),
                int(summary.get("deal_rows_remaining_missing", 0) or 0),
            )
        else:
            logger.warning("[Scheduler] cTrader data integrity failed: %s", report.get("error"))
        return report

    @staticmethod
    def _format_xau_direct_lane_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"XAU DIRECT LANE REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        sources = dict((report or {}).get("sources") or {})
        lines = [
            "XAU DIRECT LANE REPORT",
            (
                f"Hours {int((report or {}).get('hours', 0) or 0)} "
                f"| sent {int(summary.get('sent', 0) or 0)} "
                f"| filled {int(summary.get('filled', 0) or 0)} "
                f"| resolved {int(summary.get('resolved', 0) or 0)} "
                f"| WR {float(summary.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| PnL {float(summary.get('pnl_usd', 0.0) or 0.0):+.2f}$"
            ),
        ]
        for lane in ("main", "winner"):
            row = dict(sources.get(lane) or {})
            lines.append(
                f"{lane}: sent {int(row.get('sent', 0) or 0)} | filled {int(row.get('filled', 0) or 0)} "
                f"| resolved {int(row.get('resolved', 0) or 0)} | WR {float(row.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| PnL {float(row.get('pnl_usd', 0.0) or 0.0):+.2f}$"
            )
        return "\n".join(lines)

    def _run_xau_direct_lane_report(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_xau_direct_lane_report(
                hours=max(1, int(getattr(config, "XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS", 72) or 72))
            )
            or {}
        )
        try:
            report_store.save_report("xau_direct_lane_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            summary = dict(report.get("summary") or {})
            logger.info(
                "[Scheduler] XAU direct lane report ready: sent=%s resolved=%s wr=%.3f pnl=%s",
                int(summary.get("sent", 0) or 0),
                int(summary.get("resolved", 0) or 0),
                float(summary.get("win_rate", 0.0) or 0.0),
                float(summary.get("pnl_usd", 0.0) or 0.0),
            )
        else:
            logger.warning("[Scheduler] XAU direct lane report failed: %s", report.get("error"))
        if bool(getattr(config, "XAU_DIRECT_LANE_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_xau_direct_lane_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] XAU direct lane report telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_xau_direct_lane_auto_tune_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"XAU DIRECT LANE AUTO-TUNE\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        lines = [
            "XAU DIRECT LANE AUTO-TUNE",
            (
                f"Status: {str((report or {}).get('status', '-') or '-')} "
                f"| resolved {int(summary.get('resolved', 0) or 0)} "
                f"| WR {float(summary.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| PnL {float(summary.get('pnl_usd', 0.0) or 0.0):+.2f}$"
            ),
        ]
        for key, value in dict((report or {}).get("changes") or {}).items():
            lines.append(f"{key} => {value}")
        for reason in list((report or {}).get("reasons") or [])[:2]:
            lines.append(f"reason: {str(reason)}")
        return "\n".join(lines)

    def _run_xau_direct_lane_auto_tune(self, force: bool = False) -> dict:
        report = dict(live_profile_autopilot.auto_tune_xau_direct_lane() or {})
        try:
            report_store.save_report("xau_direct_lane_auto_tune_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] XAU direct lane auto-tune status=%s changes=%s",
                str(report.get("status", "") or ""),
                len(dict(report.get("changes") or {})),
            )
        else:
            logger.warning("[Scheduler] XAU direct lane auto-tune failed: %s", report.get("error"))
        if bool(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_xau_direct_lane_auto_tune_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] XAU direct lane auto-tune telegram send failed", exc_info=True)
        return report

    def _run_btc_direct_lane_auto_tune(self, force: bool = False) -> dict:
        """Run BTC BFSS/BFLS/BRR confidence auto-tune from live fills."""
        if not bool(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        try:
            report = dict(live_profile_autopilot.auto_tune_btc_direct_lane() or {})
        except Exception as exc:
            logger.error("[Scheduler] BTC direct lane auto-tune error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}
        if bool(report.get("ok")):
            families = dict(report.get("families") or {})
            actioned = {k: v.get("status") for k, v in families.items() if "trial_proposed" in str(v.get("status") or "") or "applied" in str(v.get("status") or "")}
            if actioned:
                logger.info("[Scheduler] BTC auto-tune: %s", actioned)
            else:
                logger.debug("[Scheduler] BTC auto-tune: hold_all status=%s", report.get("status"))
        else:
            logger.warning("[Scheduler] BTC auto-tune failed: %s", report.get("error"))
        return report

    # ── Conductor / Multi-Agent Cycle ─────────────────────────────────────────

    def _run_conductor_cycle(self, force: bool = False) -> dict:
        """Run the Conductor multi-agent cycle (Performance + Regime + Optimization + Risk Guard)."""
        if not bool(getattr(config, "CONDUCTOR_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        try:
            from openclaw.conductor import run_conductor_cycle
            result = run_conductor_cycle()
            ok = bool(result.get("ok"))
            opt_findings = (result.get("results") or {}).get("optimization") or {}
            routed = list((opt_findings.get("findings") or {}).get("proposals_routed") or [])
            rg_findings = (result.get("results") or {}).get("risk_guard") or {}
            emergencies = list((rg_findings.get("findings") or {}).get("emergency_actions_taken") or [])
            if emergencies:
                logger.warning("[Scheduler] Conductor — emergency actions: %s", emergencies)
            if routed:
                logger.info("[Scheduler] Conductor — %d trial(s) proposed: %s", len(routed), routed)
            else:
                logger.debug("[Scheduler] Conductor cycle done — no proposals")
            return result
        except Exception as exc:
            logger.error("[Scheduler] Conductor cycle error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}

    # ── Parameter Trial Sandbox ──────────────────────────────────────────────

    @staticmethod
    def _format_trial_report_text(trial: dict, bt_result: dict) -> str:
        """Format 3-part trial report: found / progress / result."""
        param = str(trial.get("param") or "")
        current = str(trial.get("current_value") or "")
        proposed = str(trial.get("proposed_value") or "")
        direction = str(trial.get("direction") or "")
        reason = str(trial.get("reason") or "")
        verdict = str(bt_result.get("verdict") or "")
        verdict_reason = str(bt_result.get("verdict_reason") or "")
        inc_resolved = int(bt_result.get("incremental_resolved", 0) or 0)
        inc_wr = float(bt_result.get("incremental_win_rate", 0.0) or 0.0)
        base_wr = float(bt_result.get("baseline_win_rate", 0.0) or 0.0)
        min_wr = float(bt_result.get("min_win_rate_required", 0.55) or 0.55)
        verdict_icon = "✅" if verdict == "pass" else "❌"
        dir_icon = "🔓" if "loosen" in direction else "🔒"
        lines = [
            "╔══ PARAMETER TRIAL RESULT ══╗",
            "",
            "① FOUND",
            f"  Param: {param}",
            f"  Current: {current}  →  Proposed: {proposed}",
            f"  Signal: {direction.upper()} ({reason})",
            "",
            "② TESTED (POC Backtest)",
            f"  Incremental signals unlocked: {inc_resolved}",
            f"  Win rate at incremental band: {inc_wr:.0%}",
            f"  Baseline (allowed signals) WR: {base_wr:.0%}",
            f"  Required min WR to pass: {min_wr:.0%}",
            "",
            "③ RESULT",
            f"  Verdict: {verdict_icon} {verdict.upper()}",
            f"  Reason: {verdict_reason}",
        ]
        if verdict == "pass":
            lines += [
                "",
                f"  {dir_icon} Ready to apply: {param} = {proposed}",
                "  ⚠️  Not applied yet — awaiting confirmation",
                f"  Trial ID: {trial.get('id', '')}",
            ]
        else:
            lines += [
                "",
                "  ⛔ Change blocked — insufficient evidence",
                "  Keeping current value until more data.",
            ]
        return "\n".join(lines)

    def _run_parameter_trial_bt(self, force: bool = False) -> dict:
        """Run BT for pending trials, then send Telegram for any that passed."""
        if not bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        try:
            report = dict(live_profile_autopilot.run_parameter_trial_bt() or {})
        except Exception as exc:
            logger.error("[Scheduler] Parameter trial BT error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}
        checked = int(report.get("checked", 0) or 0)
        passed = int(report.get("passed", 0) or 0)
        failed = int(report.get("failed", 0) or 0)
        skipped = int(report.get("skipped", 0) or 0)
        if checked:
            logger.info(
                "[Scheduler] Parameter trial BT: checked=%s passed=%s failed=%s skipped=%s",
                checked, passed, failed, skipped,
            )
        # Notify Telegram for passed-but-not-yet-notified trials
        if bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_NOTIFY_TELEGRAM", True)):
            try:
                pending_notifications = live_profile_autopilot.get_pending_trial_notifications()
            except Exception:
                pending_notifications = []
            for trial in pending_notifications:
                bt_result = dict(trial.get("bt_result") or {})
                text = self._format_trial_report_text(trial, bt_result)
                sent = False
                try:
                    # Primary: sync admin_bot path (same as XAU guard — proven reliable)
                    from notifier.admin_bot import admin_bot
                    for cid in self._xau_guard_transition_target_ids():
                        try:
                            admin_bot._send_text(int(cid), text)
                            sent = True
                        except Exception:
                            pass
                except Exception as exc_ab:
                    logger.debug("[Scheduler] admin_bot trial notify error: %s", exc_ab)
                if not sent:
                    try:
                        # Fallback: async notifier path
                        sent = bool(notifier._send(text, parse_mode=None, feature="winner_mission"))
                    except Exception as exc_n:
                        logger.warning("[Scheduler] Trial notification fallback failed: %s", exc_n)
                if sent:
                    live_profile_autopilot.mark_trial_notified(str(trial.get("id") or ""))
                    logger.info("[Scheduler] Trial notification sent: %s", trial.get("id"))
                else:
                    logger.warning("[Scheduler] Trial notification FAILED (will retry): %s", trial.get("id"))
        return report

    # ── XAU shadow backtest ──────────────────────────────────────────────────

    @staticmethod
    def _stamp_family_canary_skip(signal, *, family: str, stage: str, reason: str) -> None:
        if signal is None:
            return
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["family_canary_skip"] = {
                "family": str(family or "").strip().lower(),
                "stage": str(stage or ""),
                "reason": str(reason or ""),
            }
            signal.raw_scores = raw
        except Exception:
            pass

    def _classify_family_canary_build_miss(self, signal, candidate: dict | None) -> tuple[str, str]:
        fam = str((candidate or {}).get("family") or "").strip().lower()
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        skip = raw.get("family_canary_skip")
        if isinstance(skip, dict) and str(skip.get("family") or "").strip().lower() == fam:
            return str(skip.get("stage") or "family_builder"), str(skip.get("reason") or "")
        if raw.get("xau_multi_tf_guard_block"):
            return "multi_tf_guard", str(raw.get("xau_multi_tf_guard_reason") or "")
        if raw.get("pb_falling_knife_block"):
            return "pb_falling_knife", str(raw.get("pb_falling_knife_block_reason") or "")
        if raw.get("xau_openapi_entry_router_block"):
            return "entry_router", str(raw.get("xau_openapi_entry_router_block_reason") or "")
        return "family_builder", f"{fam or 'unknown'}:build_returned_none_unstamped"

    def _family_canary_gate_raw_excerpt(self, signal) -> dict:
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        excerpt: dict = {}
        for k in (
            "xau_multi_tf_guard_reason",
            "pb_falling_knife_block_reason",
            "xau_openapi_entry_router_block_reason",
            "neural_probability",
            "neural_adjust_reason",
            "family_canary_skip",
            "ctrader_pre_dispatch_gate",
            "ctrader_pre_dispatch_reason",
        ):
            if k in raw and raw[k] is not None and raw[k] != "":
                try:
                    excerpt[k] = raw[k] if k != "family_canary_skip" else dict(raw[k])
                except Exception:
                    excerpt[k] = str(raw[k])
        return excerpt

    def _store_xau_family_canary_gate_journal(
        self,
        signal,
        *,
        candidate: dict | None,
        base_source: str,
        lane_source: str,
        gate_stage: str,
        reason: str,
    ) -> None:
        if not bool(getattr(config, "XAU_FAMILY_CANARY_GATE_JOURNAL_ENABLED", True)):
            return
        if self._is_pytest_runtime():
            return
        import json as _json
        import sqlite3 as _sqlite3
        from datetime import datetime, timezone

        db_path = Path(__file__).resolve().parent / "data" / "ctrader_openapi.db"
        if not db_path.exists():
            return
        try:
            sym = str(getattr(signal, "symbol", "XAUUSD") or "XAUUSD").strip().upper()
            if sym != "XAUUSD":
                return
            signal_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            fam = str((candidate or {}).get("family") or "").strip().lower()
            strat = str((candidate or {}).get("strategy_id") or "")
            direction = str(getattr(signal, "direction", "") or "").strip().lower()
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            np_val = raw.get("neural_probability")
            try:
                neural_p = float(np_val) if np_val is not None else None
            except Exception:
                neural_p = None
            excerpt = self._family_canary_gate_raw_excerpt(signal)
            excerpt["strategy_id"] = strat
            excerpt_json = _json.dumps(excerpt)
            with _sqlite3.connect(str(db_path), timeout=10) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS xau_family_canary_gate_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_utc TEXT NOT NULL,
                        symbol TEXT NOT NULL DEFAULT 'XAUUSD',
                        base_source TEXT NOT NULL DEFAULT '',
                        family TEXT NOT NULL DEFAULT '',
                        lane_source TEXT NOT NULL DEFAULT '',
                        gate_stage TEXT NOT NULL DEFAULT '',
                        reason TEXT NOT NULL DEFAULT '',
                        direction TEXT,
                        confidence REAL,
                        neural_probability REAL,
                        raw_scores_excerpt TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO xau_family_canary_gate_journal
                        (signal_utc, symbol, base_source, family, lane_source, gate_stage,
                         reason, direction, confidence, neural_probability, raw_scores_excerpt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_utc,
                        sym,
                        str(base_source or "").strip().lower(),
                        fam,
                        str(lane_source or "").strip().lower(),
                        str(gate_stage or ""),
                        str(reason or "")[:1024],
                        direction,
                        conf,
                        neural_p,
                        excerpt_json,
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.debug("[FamilyCanaryGate] store failed: %s", exc)

    def _store_shadow_signal(self, signal, *, block_reason: str) -> None:
        """Persist a blocked XAU direct-lane signal for shadow simulation."""
        import json as _json
        import sqlite3 as _sqlite3
        from datetime import datetime, timezone
        db_path = Path(__file__).resolve().parent / "data" / "ctrader_openapi.db"
        if not db_path.exists():
            return
        try:
            entry = float(getattr(signal, "entry", 0.0) or 0.0)
            sl = float(getattr(signal, "stop_loss", 0.0) or 0.0)
            if entry <= 0 or sl <= 0:
                return
            tp1 = float(getattr(signal, "take_profit_1", 0.0) or 0.0)
            tp2 = float(getattr(signal, "take_profit_2", 0.0) or 0.0)
            tp3 = float(getattr(signal, "take_profit_3", 0.0) or 0.0)
            direction = str(getattr(signal, "direction", "") or "").strip().lower()
            conf = float(getattr(signal, "confidence", 0.0) or 0.0)
            symbol = str(getattr(signal, "symbol", "XAUUSD") or "XAUUSD").strip().upper()
            signal_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                raw = dict(getattr(signal, "raw_scores", {}) or {})
                raw_json = _json.dumps(raw)
            except Exception:
                raw_json = "{}"
            with _sqlite3.connect(str(db_path), timeout=10) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS xau_shadow_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_utc TEXT NOT NULL,
                        symbol TEXT NOT NULL DEFAULT 'XAUUSD',
                        direction TEXT NOT NULL,
                        confidence REAL NOT NULL DEFAULT 0.0,
                        entry REAL NOT NULL DEFAULT 0.0,
                        stop_loss REAL NOT NULL DEFAULT 0.0,
                        take_profit_1 REAL NOT NULL DEFAULT 0.0,
                        take_profit_2 REAL NOT NULL DEFAULT 0.0,
                        take_profit_3 REAL NOT NULL DEFAULT 0.0,
                        block_reason TEXT NOT NULL DEFAULT '',
                        raw_scores_json TEXT NOT NULL DEFAULT '{}',
                        shadow_outcome TEXT,
                        resolved_utc TEXT,
                        shadow_pnl_rr REAL
                    )
                """)
                conn.execute("""
                    INSERT INTO xau_shadow_journal
                        (signal_utc, symbol, direction, confidence, entry, stop_loss,
                         take_profit_1, take_profit_2, take_profit_3, block_reason, raw_scores_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (signal_utc, symbol, direction, conf, entry, sl, tp1, tp2, tp3, str(block_reason or ""), raw_json))
                conn.commit()
        except Exception as exc:
            logger.debug("[Shadow] store_shadow_signal failed: %s", exc)

    def _run_xau_shadow_backtest(self, force: bool = False) -> dict:
        """Resolve pending shadow journal signals against candle history and feed auto-tune."""
        if not bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        try:
            report = dict(live_profile_autopilot.run_xau_shadow_backtest() or {})
        except Exception as exc:
            logger.error("[Scheduler] XAU shadow backtest error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}
        if bool(report.get("ok")):
            resolved = int(report.get("newly_resolved", 0) or 0)
            pending = int(report.get("pending", 0) or 0)
            logger.info(
                "[Scheduler] XAU shadow backtest: newly_resolved=%s pending=%s total_shadow=%s",
                resolved,
                pending,
                int(report.get("total", 0) or 0),
            )
        else:
            logger.warning("[Scheduler] XAU shadow backtest failed: %s", report.get("error"))
        return report

    @staticmethod
    def _format_strategy_lab_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"STRATEGY LAB REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "STRATEGY LAB REPORT",
            f"Candidates: {len(list((report or {}).get('candidates') or []))}",
        ]
        for row in list((report or {}).get("candidates") or [])[:3]:
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} | {str(row.get('strategy_id', '-') or '-')} | {str(row.get('status', '-') or '-')}"
            )
        return "\n".join(lines)

    def _run_strategy_lab_report(self, force: bool = False) -> dict:
        report = dict(live_profile_autopilot.build_strategy_lab_report() or {})
        try:
            report_store.save_report("strategy_lab_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Strategy lab report ready: candidates=%s promotable=%s families=%s",
                len(list(report.get("candidates") or [])),
                len(list(report.get("promotable_candidates") or [])),
                int(((report.get("summary") or {}).get("families_ranked", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Strategy lab report failed: %s", report.get("error"))
        if bool(getattr(config, "STRATEGY_LAB_TEAM_ENABLED", True)):
            try:
                team_report = dict(strategy_lab_team_agent.build_report(strategy_lab_report=report) or {})
                try:
                    report_store.save_report("strategy_lab_team_report", team_report)
                except Exception:
                    pass
                if bool(team_report.get("ok")):
                    logger.info(
                        "[Scheduler] Strategy lab team ready: symbols=%s promotions=%s live_shadow=%s",
                        len(list((team_report.get("symbols") or {}).keys())),
                        int(((team_report.get("summary") or {}).get("promotion_count", 0) or 0)),
                        int(((team_report.get("summary") or {}).get("live_shadow_count", 0) or 0)),
                    )
                    if bool(getattr(config, "TRADING_TEAM_ENABLED", True)):
                        fresh_team_report = dict(trading_team_agent.build_report() or {})
                        try:
                            report_store.save_report("trading_team_report", fresh_team_report)
                        except Exception:
                            pass
                else:
                    logger.warning("[Scheduler] Strategy lab team failed: %s", team_report.get("error"))
            except Exception:
                logger.debug("[Scheduler] Strategy lab team update failed", exc_info=True)
        if bool(getattr(config, "STRATEGY_LAB_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_strategy_lab_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Strategy lab report telegram send failed", exc_info=True)
        return report

    @staticmethod
    def _format_family_calibration_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"FAMILY CALIBRATION REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "FAMILY CALIBRATION REPORT",
            f"Families: {int(((report.get('summary') or {}).get('families', 0) or 0))}",
        ]
        for row in list((report or {}).get("families") or [])[:3]:
            overall = dict(row.get("overall") or {})
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} {str(row.get('family', '-') or '-')} "
                f"| WR {float(overall.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| Cal {float(row.get('calibrated_win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| U {float(row.get('uncertainty_score', 0.0) or 0.0):.2f}"
            )
        return "\n".join(lines)

    def _run_family_calibration_report(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_family_calibration_report(
                days=max(1, int(getattr(config, "FAMILY_CALIBRATION_REPORT_LOOKBACK_DAYS", 21) or 21))
            )
            or {}
        )
        try:
            report_store.save_report("family_calibration_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Family calibration ready: rows=%s families=%s",
                int(((report.get("summary") or {}).get("rows", 0) or 0)),
                int(((report.get("summary") or {}).get("families", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Family calibration failed: %s", report.get("error"))
        if bool(getattr(config, "FAMILY_CALIBRATION_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_family_calibration_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Family calibration telegram send failed", exc_info=True)
        return report

    # ── Sharpness Feedback Loop (self-improving) ────────────────────────────

    def _get_self_improving_symbols(self) -> list[str]:
        """Get list of symbols for self-improving AI features."""
        raw = str(getattr(config, "SELF_IMPROVING_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD") or "XAUUSD")
        return [s.strip().upper() for s in raw.split(",") if s.strip()]

    def _run_sharpness_feedback_report(self, force: bool = False) -> dict:
        """Run sharpness correlation + calibration + family decay report for all symbols."""
        if not bool(getattr(config, "XAU_SHARPNESS_FEEDBACK_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        import sqlite3 as _sqlite3
        db_path = Path(__file__).resolve().parent / "data" / "ctrader_openapi.db"
        if not db_path.exists():
            return {"ok": False, "status": "no_db"}
        symbols = self._get_self_improving_symbols()
        current_weights = {
            "XAU_ENTRY_SHARPNESS_W_MOMENTUM": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_MOMENTUM", 1.0) or 1.0),
            "XAU_ENTRY_SHARPNESS_W_FLOW": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_FLOW", 1.0) or 1.0),
            "XAU_ENTRY_SHARPNESS_W_ABSORPTION": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_ABSORPTION", 1.0) or 1.0),
            "XAU_ENTRY_SHARPNESS_W_STABILITY": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_STABILITY", 1.0) or 1.0),
            "XAU_ENTRY_SHARPNESS_W_POSITIONING": float(getattr(config, "XAU_ENTRY_SHARPNESS_W_POSITIONING", 1.0) or 1.0),
        }
        all_reports: dict[str, dict] = {}
        telegram_lines: list[str] = []
        try:
            from learning.sharpness_feedback import build_sharpness_feedback_report, format_sharpness_feedback_text
            with _sqlite3.connect(str(db_path), timeout=10) as conn:
                conn.row_factory = _sqlite3.Row
                for sym in symbols:
                    try:
                        report = build_sharpness_feedback_report(
                            conn,
                            days=max(1, int(getattr(config, "XAU_SHARPNESS_FEEDBACK_LOOKBACK_DAYS", 14) or 14)),
                            symbol=sym,
                            current_weights=current_weights,
                            min_trades_for_calibration=max(1, int(getattr(config, "XAU_SHARPNESS_FEEDBACK_MIN_TRADES", 10) or 10)),
                            decay_recent_trades=max(1, int(getattr(config, "XAU_FAMILY_DECAY_RECENT_TRADES", 20) or 20)),
                            decay_baseline_trades=max(1, int(getattr(config, "XAU_FAMILY_DECAY_BASELINE_TRADES", 60) or 60)),
                            decay_threshold=max(0.01, float(getattr(config, "XAU_FAMILY_DECAY_THRESHOLD", 0.15) or 0.15)),
                        )
                        all_reports[sym] = report
                        report_store.save_report(f"sharpness_feedback_report_{sym.lower()}", report)
                        summary = dict((report or {}).get("summary") or {})
                        n_trades = int(summary.get("n_trades_with_sharpness", 0) or 0)
                        if n_trades > 0:
                            logger.info(
                                "[Scheduler] Sharpness feedback %s: trades=%s composite_r=%s calibrate=%s decay_alerts=%s",
                                sym, n_trades,
                                round(float(summary.get("composite_r", 0.0) or 0.0), 4),
                                bool(summary.get("calibration_ready")),
                                int(summary.get("n_decay_alerts", 0) or 0),
                            )
                            telegram_lines.append(format_sharpness_feedback_text(report).replace("Sharpness Feedback Report", f"Sharpness [{sym}]"))
                        else:
                            logger.debug("[Scheduler] Sharpness feedback %s: no trades with sharpness", sym)
                    except Exception as exc:
                        logger.debug("[Scheduler] Sharpness feedback %s error: %s", sym, exc)
        except Exception as exc:
            logger.error("[Scheduler] Sharpness feedback report error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}
        # Save combined report for backwards compat
        xau_report = all_reports.get("XAUUSD") or next(iter(all_reports.values()), {"ok": True})
        try:
            report_store.save_report("sharpness_feedback_report", xau_report)
        except Exception:
            pass
        # Auto-calibrate from all symbols with sharpness data
        if bool(getattr(config, "XAU_SHARPNESS_AUTO_CALIBRATE_ENABLED", False)):
            for sym, sym_report in all_reports.items():
                self._apply_sharpness_auto_calibrate(sym_report, symbol=sym)
        # Telegram: combined
        if bool(getattr(config, "XAU_SHARPNESS_FEEDBACK_NOTIFY_TELEGRAM", True)) and (telegram_lines or force):
            try:
                if not telegram_lines:
                    telegram_lines = ["\U0001f4ca Sharpness Feedback Report\nNo trades with sharpness data yet."]
                notifier._send("\n\n".join(telegram_lines), parse_mode=None, feature="winner_mission")
            except Exception:
                logger.debug("[Scheduler] Sharpness feedback telegram send failed", exc_info=True)
        return {"ok": True, "symbols": list(all_reports.keys()), "reports": {k: bool(v.get("ok")) for k, v in all_reports.items()}}

    def _apply_sharpness_auto_calibrate(self, report: dict, symbol: str = "XAUUSD") -> None:
        """Apply weight recommendations from sharpness feedback if auto-calibrate is enabled."""
        calibration = dict((report or {}).get("calibration") or {})
        if not bool(calibration.get("apply")):
            return
        recommendations = list(calibration.get("recommendations") or [])
        applied = []
        for rec in recommendations:
            if str(rec.get("action", "hold") or "hold") == "hold":
                continue
            config_key = str(rec.get("config_key", "") or "")
            new_val = float(rec.get("recommended", 1.0) or 1.0)
            min_w = max(0.1, float(getattr(config, "XAU_SHARPNESS_AUTO_CALIBRATE_MIN_WEIGHT", 0.5) or 0.5))
            max_w = max(1.0, float(getattr(config, "XAU_SHARPNESS_AUTO_CALIBRATE_MAX_WEIGHT", 2.0) or 2.0))
            clamped = max(min_w, min(max_w, new_val))
            if config_key and hasattr(config, config_key):
                old_val = float(getattr(config, config_key, 1.0) or 1.0)
                if abs(clamped - old_val) > 0.001:
                    setattr(config, config_key, clamped)
                    applied.append(f"{config_key}: {old_val:.3f} -> {clamped:.3f}")
        if applied:
            logger.info("[Scheduler] Sharpness auto-calibrate [%s] applied: %s", symbol, applied)
            if bool(getattr(config, "STRATEGY_EVOLUTION_ENABLED", True)):
                try:
                    from learning.strategy_evolution import log_change
                    correlation = dict((report or {}).get("correlation") or {})
                    log_change(
                        change_type="weight_calibration",
                        description=f"[{symbol}] Sharpness weights adjusted: {', '.join(applied)}",
                        component="analysis/entry_sharpness.py",
                        metric_before={"composite_r": float((correlation.get("composite") or {}).get("r", 0) or 0)},
                        impact="pending",
                        auto=True,
                        source="sharpness_feedback",
                        metadata={"symbol": symbol, "applied": applied, "n_trades": int(correlation.get("n_trades", 0) or 0)},
                    )
                except Exception:
                    logger.debug("[Scheduler] Strategy evolution log failed", exc_info=True)

    # ── Volume Profile ──────────────────────────────────────────────────────

    def _run_volume_profile_report(self, force: bool = False) -> dict:
        """Compute session Volume Profile from M1 bars for all self-improving symbols."""
        if not bool(getattr(config, "XAU_VOLUME_PROFILE_ENABLED", True)):
            return {"ok": False, "status": "disabled"}
        import sqlite3 as _sqlite3
        db_path = Path(__file__).resolve().parent / "data" / "ctrader_openapi.db"
        if not db_path.exists():
            return {"ok": False, "status": "no_db"}
        symbols = self._get_self_improving_symbols()
        all_reports: dict[str, dict] = {}
        try:
            from analysis.volume_profile import build_session_volume_profile, get_tick_config
            with _sqlite3.connect(str(db_path), timeout=10) as conn:
                conn.row_factory = _sqlite3.Row
                for sym in symbols:
                    try:
                        tc = get_tick_config(sym)
                        report = build_session_volume_profile(
                            conn,
                            symbol=sym,
                            hours_back=max(1, int(getattr(config, "XAU_VOLUME_PROFILE_HOURS_BACK", 24) or 24)),
                            session="full",
                            tick_size=float(tc.get("tick_size", 0.01)),
                            bucket_ticks=int(tc.get("bucket_ticks", 10)),
                            va_pct=max(0.5, min(0.95, float(getattr(config, "XAU_VOLUME_PROFILE_VA_PCT", 0.70) or 0.70))),
                        )
                        all_reports[sym] = report
                        vp_data = dict(report.get("vp") or {})
                        vp_data.pop("profile", None)
                        report_store.save_report(f"volume_profile_{sym.lower()}", {**report, "vp": vp_data})
                        if bool(report.get("ok")):
                            vp = dict(report.get("vp") or {})
                            logger.info(
                                "[Scheduler] Volume profile %s: POC=%.2f VA=[%.2f,%.2f] bars=%d HVN=%d LVN=%d",
                                sym,
                                float(vp.get("poc", 0) or 0),
                                float(vp.get("va_low", 0) or 0),
                                float(vp.get("va_high", 0) or 0),
                                int(report.get("bars_used", 0) or 0),
                                len(list(vp.get("hvn_levels") or [])),
                                len(list(vp.get("lvn_levels") or [])),
                            )
                    except Exception as exc:
                        logger.debug("[Scheduler] Volume profile %s error: %s", sym, exc)
        except Exception as exc:
            logger.error("[Scheduler] Volume profile error: %s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}
        # Backwards-compat: save XAUUSD as default "volume_profile"
        if "XAUUSD" in all_reports:
            try:
                vp_data = dict(all_reports["XAUUSD"].get("vp") or {})
                vp_data.pop("profile", None)
                report_store.save_report("volume_profile", {**all_reports["XAUUSD"], "vp": vp_data})
            except Exception:
                pass
        return {"ok": True, "symbols": list(all_reports.keys()), "reports": {k: bool(v.get("ok")) for k, v in all_reports.items()}}

    # ── DOM Liquidity Shift ─────────────────────────────────────────────────

    def _get_dom_liquidity_shift(self, *, symbol: str = "XAUUSD", direction: str = "long") -> dict:
        """Compute DOM liquidity shift for position manager active defense integration.

        Returns the adverse assessment dict or empty if disabled/unavailable.
        """
        if not bool(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_ENABLED", True)):
            return {}
        import sqlite3 as _sqlite3
        db_path = Path(__file__).resolve().parent / "data" / "ctrader_openapi.db"
        if not db_path.exists():
            return {}
        try:
            from analysis.dom_liquidity_shift import analyze_dom_liquidity
            with _sqlite3.connect(str(db_path), timeout=5) as conn:
                conn.row_factory = _sqlite3.Row
                result = analyze_dom_liquidity(
                    conn,
                    symbol=symbol,
                    direction=direction,
                    lookback_min=max(5, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", 30) or 30)),
                    max_runs=max(2, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", 6) or 6)),
                )
            return result
        except Exception as exc:
            logger.debug("[Scheduler] DOM liquidity shift error: %s", exc)
            return {}

    @staticmethod
    def _format_tick_depth_replay_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"CTRADER REPLAY LAB\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        lines = [
            "CTRADER REPLAY LAB",
            (
                f"Orders {int(((report.get('summary') or {}).get('orders', 0) or 0))} "
                f"| with capture {int(((report.get('summary') or {}).get('orders_with_capture', 0) or 0))} "
                f"| families {int(((report.get('summary') or {}).get('families', 0) or 0))}"
            ),
        ]
        for row in list((report or {}).get("families") or [])[:3]:
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} {str(row.get('family', '-') or '-')} "
                f"| WR {float(row.get('win_rate', 0.0) or 0.0) * 100.0:.1f}% "
                f"| spread {float(row.get('avg_spread_pct', 0.0) or 0.0):.4f}% "
                f"| depth {float(row.get('avg_depth_imbalance', 0.0) or 0.0):+.3f}"
            )
        return "\n".join(lines)

    def _run_ctrader_tick_depth_replay_lab(self, force: bool = False) -> dict:
        report = dict(
            live_profile_autopilot.build_ctrader_tick_depth_replay_report(
                days=max(1, int(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_LOOKBACK_DAYS", 7) or 7))
            )
            or {}
        )
        try:
            report_store.save_report("ctrader_tick_depth_replay_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] cTrader replay lab ready: orders=%s captured=%s families=%s",
                int(((report.get("summary") or {}).get("orders", 0) or 0)),
                int(((report.get("summary") or {}).get("orders_with_capture", 0) or 0)),
                int(((report.get("summary") or {}).get("families", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] cTrader replay lab failed: %s", report.get("error"))
        if bool(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_tick_depth_replay_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] cTrader replay lab telegram send failed", exc_info=True)
        return report

    def _run_ctrader_market_capture(self, force: bool = False) -> dict:
        if not bool(getattr(config, "CTRADER_MARKET_CAPTURE_ENABLED", False)):
            return {"ok": False, "status": "disabled", "message": "capture disabled"}
        if ctrader_executor is None:
            return {"ok": False, "status": "executor_missing", "message": "ctrader executor missing"}
        try:
            report = dict(
                ctrader_executor.capture_market_data(
                    symbols=sorted(list(config.get_ctrader_market_capture_symbols() or set())),
                    duration_sec=max(3, int(getattr(config, "CTRADER_MARKET_CAPTURE_DURATION_SEC", 12) or 12)),
                    include_depth=True,
                    max_events=max(50, int(getattr(config, "CTRADER_MARKET_CAPTURE_MAX_EVENTS", 600) or 600)),
                    max_depth_levels=max(1, int(getattr(config, "CTRADER_MARKET_CAPTURE_DEPTH_LEVELS", 5) or 5)),
                )
                or {}
            )
        except Exception as e:
            err_str = str(e)
            logger.warning("[Scheduler] cTrader market capture failed: %s", err_str)
            if any(k in err_str for k in ("Invalid access token", "Cannot route", "ACCESS_TOKEN_INVALID")):
                try:
                    from api.ctrader_token_manager import token_manager as _tm_cap
                    _tm_cap.try_refresh()
                except Exception:
                    pass
            return {"ok": False, "status": "error", "message": err_str}
        if bool(report.get("ok")):
            spots_count = int(report.get("spots_count", 0) or len(list(report.get("spots") or [])))
            depth_count = int(report.get("depth_count", 0) or len(list(report.get("depth") or [])))
            logger.info(
                "[Scheduler] cTrader market capture ok: spots=%s depth=%s run=%s",
                spots_count,
                depth_count,
                str(report.get("run_id", "") or ""),
            )
        else:
            cap_err = str(report.get("message") or "")
            logger.warning("[Scheduler] cTrader market capture failed: %s", cap_err)
            if any(k in cap_err for k in ("Invalid access token", "Cannot route", "ACCESS_TOKEN_INVALID")):
                try:
                    from api.ctrader_token_manager import token_manager as _tm_cap2
                    _tm_cap2.try_refresh()
                except Exception:
                    pass
        if bool(report.get("ok")) and (force or bool(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_ENABLED", False))):
            try:
                self._run_ctrader_tick_depth_replay_lab(force=False)
            except Exception:
                logger.debug("[Scheduler] cTrader replay follow-up after capture failed", exc_info=True)
        return report

    @staticmethod
    def _format_mission_progress_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"MISSION PROGRESS REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        lines = [
            "MISSION PROGRESS REPORT",
            (
                f"Bundle: {str(summary.get('active_bundle_status', '-') or '-')} "
                f"| canary closed {int(summary.get('canary_closed_total', 0) or 0)} "
                f"| strategy candidates {int(summary.get('strategy_candidates', 0) or 0)} "
                f"| promotable {int(summary.get('promotable_candidates', 0) or 0)} "
                f"| winner-memory {int(summary.get('winner_memory_market_beating', 0) or 0)}"
            ),
        ]
        for row in list((report or {}).get("symbols") or [])[:3]:
            canary = dict(row.get("canary_total") or {})
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} | family {str(row.get('selected_family', '-') or '-')} "
                f"| regime {str(row.get('selected_regime', '-') or '-')} "
                f"| WR gap {str(row.get('wr_gap_to_target'))} "
                f"| sample gap {int(row.get('sample_gap_to_target', 0) or 0)} "
                f"| canary {int(canary.get('wins', 0) or 0)}/{int(canary.get('resolved', 0) or 0)}"
            )
            asian_long_memory = dict(row.get("asian_long_memory") or {})
            if asian_long_memory:
                lines.append(
                    f"  memory asian-long | {str(asian_long_memory.get('family', '-') or '-')} "
                    f"| conf {str(asian_long_memory.get('confidence_band', '-') or '-')} "
                    f"| {int(asian_long_memory.get('wins', 0) or 0)}/{int(asian_long_memory.get('resolved', 0) or 0)} "
                    f"| pnl {round(float(asian_long_memory.get('pnl_usd', 0.0) or 0.0), 2)}"
                )
            winner_memory_library = dict(row.get("winner_memory_library") or {})
            if winner_memory_library:
                stats = dict(winner_memory_library.get("stats") or {})
                lines.append(
                    f"  library beat-market | {str(winner_memory_library.get('family', '-') or '-')} "
                    f"| {str(winner_memory_library.get('session', '-') or '-')}/{str(winner_memory_library.get('direction', '-') or '-')} "
                    f"| {int(stats.get('wins', 0) or 0)}/{int(stats.get('resolved', 0) or 0)} "
                    f"| pnl {round(float(stats.get('pnl_usd', 0.0) or 0.0), 2)}"
                )
        return "\n".join(lines)

    def _run_mission_progress_report(self, force: bool = False) -> dict:
        report = dict(live_profile_autopilot.build_mission_progress_report() or {})
        try:
            report_store.save_report("mission_progress_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Mission progress report ready: active=%s candidates=%s promotable=%s",
                str(((report.get("summary") or {}).get("active_bundle_status", "") or "")),
                int(((report.get("summary") or {}).get("strategy_candidates", 0) or 0)),
                int(((report.get("summary") or {}).get("promotable_candidates", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Mission progress report failed: %s", report.get("error"))
        if bool(getattr(config, "MISSION_PROGRESS_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_mission_progress_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Mission progress report telegram send failed", exc_info=True)
        try:
            self._run_ct_only_watch_report(force=False)
        except Exception:
            logger.debug("[Scheduler] Mission progress ct-only watch follow-up failed", exc_info=True)
        return report

    @staticmethod
    def _format_trading_manager_report_text(report: dict) -> str:
        if not bool((report or {}).get("ok")):
            return f"TRADING MANAGER REPORT\nStatus: failed\nReason: {str((report or {}).get('error', '-') or '-')}"
        summary = dict((report or {}).get("summary") or {})
        lines = [
            "TRADING MANAGER REPORT",
            (
                f"Rows {int(summary.get('rows', 0) or 0)} "
                f"| abnormal excluded {int(summary.get('abnormal_excluded', 0) or 0)} "
                f"| shock symbols {int(summary.get('shock_symbols', 0) or 0)} "
                f"| open pos {int(summary.get('open_positions', 0) or 0)} "
                f"| open orders {int(summary.get('open_orders', 0) or 0)}"
            ),
        ]
        profile_apply = dict((report or {}).get("profile_apply") or {})
        if profile_apply:
            lines.append(
                f"profile {str(profile_apply.get('status', '-') or '-')} "
                f"| reason {str(profile_apply.get('reason', '-') or '-')[:90]}"
            )
        routing_apply = dict((report or {}).get("family_routing_apply") or {})
        if routing_apply:
            lines.append(
                f"routing {str(routing_apply.get('status', '-') or '-')} "
                f"| reason {str(routing_apply.get('reason', '-') or '-')[:90]}"
            )
        for row in list((report or {}).get("symbols") or [])[:3]:
            closed = dict(row.get("closed_total") or {})
            shock = dict(row.get("shock_event") or {})
            lines.append(
                f"{str(row.get('symbol', '-') or '-')} | selected {str(row.get('selected_family', '-') or '-')} "
                f"| today {int(closed.get('wins', 0) or 0)}/{int(closed.get('resolved', 0) or 0)} "
                f"| pnl {round(float(closed.get('pnl_usd', 0.0) or 0.0), 2)}"
            )
            if shock:
                losses = dict(row.get("losses_in_shock") or {})
                lines.append(
                    f"  shock {str(shock.get('shock_type', '-') or '-')} "
                    f"| move {float(shock.get('move_pct', 0.0) or 0.0):.2f}% "
                    f"| spread x{float(shock.get('spread_ratio', 0.0) or 0.0):.2f} "
                    f"| depth {float(shock.get('depth_imbalance', 0.0) or 0.0):+.3f} "
                    f"| losses {int(losses.get('losses', 0) or 0)}/{int(losses.get('resolved', 0) or 0)} "
                    f"{round(float(losses.get('pnl_usd', 0.0) or 0.0), 2)}"
                )
            shock_explanation = str(row.get("shock_explanation") or "").strip()
            if shock_explanation:
                lines.append(f"  why {shock_explanation}")
            macro_cause = dict(row.get("macro_cause") or {})
            if macro_cause:
                lines.append(
                    f"  macro {str(macro_cause.get('source', '-') or '-')} | "
                    f"{str(macro_cause.get('title', '-') or '-')[:90]}"
                )
            upcoming = list(row.get("upcoming_events") or [])
            if upcoming:
                ev = dict(upcoming[0] or {})
                lines.append(
                    f"  next {str(ev.get('title', '-') or '-')[:70]} "
                    f"| {int(ev.get('minutes_to_event', 0) or 0)}m"
                )
            family_routing = dict(row.get("family_routing_recommendations") or {})
            if family_routing:
                lines.append(
                    f"  route {str(family_routing.get('mode', '-') or '-')} "
                    f"| {str((family_routing.get('changes') or {}).get('CTRADER_XAU_PRIMARY_FAMILY', '-') or '-')}"
                )
            best = dict(row.get("best_same_situation") or {})
            if best:
                key = list(best.get("key") or [])
                lines.append(
                    f"  best same-situation | {str(key[0] if len(key) > 0 else '-')}"
                    f" | {str(key[1] if len(key) > 1 else '-')}/{str(key[2] if len(key) > 2 else '-')}"
                    f" | {int(best.get('wins', 0) or 0)}/{int(best.get('resolved', 0) or 0)}"
                    f" | pnl {round(float(best.get('pnl_usd', 0.0) or 0.0), 2)}"
                )
            for finding in list(row.get("manager_findings") or [])[:2]:
                lines.append(f"  note {str(finding or '').strip()}")
        return "\n".join(lines)

    def _run_trading_manager_report(self, force: bool = False) -> dict:
        report = dict(
            trading_manager_agent.build_report(
                hours=max(1, int(getattr(config, "TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24) or 24))
            )
            or {}
        )
        try:
            report_store.save_report("trading_manager_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            logger.info(
                "[Scheduler] Trading manager report ready: rows=%s abnormal=%s shock_symbols=%s",
                int(((report.get("summary") or {}).get("rows", 0) or 0)),
                int(((report.get("summary") or {}).get("abnormal_excluded", 0) or 0)),
                int(((report.get("summary") or {}).get("shock_symbols", 0) or 0)),
            )
        else:
            logger.warning("[Scheduler] Trading manager report failed: %s", report.get("error"))
        if bool(getattr(config, "TRADING_TEAM_ENABLED", True)):
            try:
                team_report = dict(trading_team_agent.build_report(manager_report=report) or {})
                try:
                    report_store.save_report("trading_team_report", team_report)
                except Exception:
                    pass
                if bool(team_report.get("ok")):
                    xau = dict((team_report.get("symbols") or {}).get("XAUUSD") or {})
                    execution = dict(xau.get("execution_desk") or {})
                    logger.info(
                        "[Scheduler] Trading team state ready: symbols=%s xau_primary=%s",
                        len(list((team_report.get("symbols") or {}).keys())),
                        str(execution.get("primary_family") or "-"),
                    )
                else:
                    logger.warning("[Scheduler] Trading team state failed: %s", team_report.get("error"))
            except Exception:
                logger.debug("[Scheduler] Trading team state update failed", exc_info=True)
        if bool(getattr(config, "TRADING_MANAGER_REPORT_NOTIFY_TELEGRAM", False)) and (bool(report.get("ok")) or force):
            try:
                notifier._send(
                    self._format_trading_manager_report_text(report),
                    parse_mode=None,
                    feature="winner_mission",
                )
            except Exception:
                logger.debug("[Scheduler] Trading manager report telegram send failed", exc_info=True)
        try:
            self._run_ct_only_watch_report(force=False)
        except Exception:
            logger.debug("[Scheduler] Trading manager ct-only watch follow-up failed", exc_info=True)
        return report

    def _run_ct_only_watch_report(self, force: bool = False) -> dict:
        report = dict(live_profile_autopilot.build_ct_only_watch_report() or {})
        try:
            report_store.save_report("ct_only_watch_report", report)
        except Exception:
            pass
        if bool(report.get("ok")):
            summary = dict(report.get("summary") or {})
            logger.info(
                "[Scheduler] ct-only watch: td_exec=%s td_close=%s ff_exec=%s ff_close=%s pb_demotion=%s",
                bool(summary.get("td_first_execution_detected")),
                bool(summary.get("td_first_resolved_detected")),
                bool(summary.get("ff_first_execution_detected")),
                bool(summary.get("ff_first_resolved_detected")),
                bool(summary.get("pb_demotion_applied")),
            )
        else:
            logger.warning("[Scheduler] ct-only watch report failed: %s", report.get("error"))
        return report

    def _run_us_open_smart_monitor(self, force: bool = False):
        """Close monitoring during US open: send focused updates when leadership changes."""
        if not config.US_OPEN_SMART_MONITOR:
            return
        try:
            ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            if ny_now.weekday() >= 5:
                return
            if (not force) and (not self._in_us_open_window(ny_now)):
                return
            self._reset_us_open_mood_state_if_new_day(ny_now)

            self._maybe_us_open_session_checkin(ny_now, force=force)
            logger.info("[Scheduler] Running US OPEN smart monitor...")
            premarket_mode = ny_now.time() < dt_time(9, 30)
            opps = stock_scanner.scan_us_open_daytrade(top_n=10, allow_premarket=premarket_mode)
            now_ts = time.time()
            realtime_mode = bool(getattr(config, "US_OPEN_SMART_ALWAYS_REPORT", False))
            no_opp_ping_min = max(3, int(getattr(config, "US_OPEN_SMART_NO_OPP_PING_MIN", 15) or 15))

            macro_freeze, macro_reason = self._check_us_open_macro_freeze()
            if macro_freeze and (not force):
                logger.info("[Scheduler] US OPEN smart monitor: macro-freeze engaged (%s)", macro_reason)
                if force or realtime_mode:
                    if force or (now_ts - self._us_open_last_noopp_sent_ts) >= (no_opp_ping_min * 60):
                        notifier.send_us_open_monitor_update([], periodic_ping=True)
                        self._us_open_last_noopp_sent_ts = now_ts
                self._maybe_send_us_open_quality_recap(force=force)
                return

            cb_stop, cb_reason = self._check_us_open_quality_circuit_breaker(ny_now)
            if cb_stop and (not force):
                logger.info("[Scheduler] US OPEN smart monitor: circuit-breaker engaged (%s)", cb_reason)
                if force or realtime_mode:
                    if force or (now_ts - self._us_open_last_noopp_sent_ts) >= (no_opp_ping_min * 60):
                        notifier.send_us_open_monitor_update([], periodic_ping=True)
                        self._us_open_last_noopp_sent_ts = now_ts
                self._maybe_send_us_open_quality_recap(force=force)
                return

            if not opps:
                mood_stop, mood_reason = self._update_us_open_mood_stop(ny_now, opps)
                diag = stock_scanner.get_last_us_open_diagnostics()
                if diag:
                    strict = diag.get("strict_filter", {}) or {}
                    upstream = diag.get("upstream", {}) or {}
                    up = (upstream.get("reject_reasons", {}) or {})
                    pf = (upstream.get("prefilter", {}) or {})
                    logger.info(
                        "[Scheduler] US OPEN smart monitor diagnostics (%s): "
                        "prefilter kept=%s/%s unmapped=%s | "
                        "upstream market_closed=%s no_data=%s no_signal=%s | "
                        "strict raw=%s pass=%s fail_conf=%s fail_vol=%s fail_dv=%s",
                        diag.get("mode", "-"),
                        pf.get("kept", upstream.get("symbols", 0)),
                        pf.get("input", upstream.get("symbols_input", upstream.get("symbols", 0))),
                        pf.get("unmapped", 0),
                        up.get("market_closed", 0),
                        up.get("no_entry_data", 0),
                        up.get("no_signal", 0),
                        strict.get("total_opportunities", 0),
                        strict.get("passed", 0),
                        strict.get("fail_confidence", 0),
                        strict.get("fail_volume", 0),
                        strict.get("fail_dollar_volume", 0),
                    )
                if mood_stop and (not force):
                    logger.info("[Scheduler] US OPEN smart monitor: mood-stop engaged (%s)", mood_reason)
                    self._maybe_send_us_open_quality_recap(force=force)
                    return
                logger.info("[Scheduler] US OPEN smart monitor: no opportunities")
                if force or realtime_mode:
                    if force or (now_ts - self._us_open_last_noopp_sent_ts) >= (no_opp_ping_min * 60):
                        notifier.send_us_open_monitor_update(
                            [],
                            periodic_ping=True,
                        )
                        self._us_open_last_noopp_sent_ts = now_ts
                self._maybe_send_us_open_quality_recap(force=force)
                return

            for opp in opps:
                self._apply_neural_soft_adjustment(opp.signal, source="us_open_monitor")
            opps, usq_diag = self._apply_us_open_quality_filters(opps, stage="monitor")
            self._log_us_open_quality_guard_diag(usq_diag, stage="monitor")
            if not opps:
                mood_stop, mood_reason = self._update_us_open_mood_stop(ny_now, [])
                if mood_stop and (not force):
                    logger.info("[Scheduler] US OPEN smart monitor: mood-stop engaged (%s)", mood_reason)
                    self._maybe_send_us_open_quality_recap(force=force)
                    return
                logger.info("[Scheduler] US OPEN smart monitor: all opportunities filtered by setup/symbol quality guard")
                if force or realtime_mode:
                    if force or (now_ts - self._us_open_last_noopp_sent_ts) >= (no_opp_ping_min * 60):
                        notifier.send_us_open_monitor_update([], periodic_ping=True)
                        self._us_open_last_noopp_sent_ts = now_ts
                self._maybe_send_us_open_quality_recap(force=force)
                return

            mood_stop, mood_reason = self._update_us_open_mood_stop(ny_now, opps)
            if mood_stop and (not force):
                logger.info("[Scheduler] US OPEN smart monitor: mood-stop engaged (%s)", mood_reason)
                self._maybe_send_us_open_quality_recap(force=force)
                return

            symbols = [o.signal.symbol for o in opps[:10]]
            previous = self._us_open_last_symbols
            top_changed = bool(previous and symbols and symbols[0] != previous[0])
            new_symbols = [s for s in symbols if s not in previous]
            periodic_ping_sec = (max(3, int(config.US_OPEN_SMART_INTERVAL_MIN)) * 60) if realtime_mode else (45 * 60)
            periodic_ping = (now_ts - self._us_open_last_sent_ts) >= periodic_ping_sec

            cooldown_min = max(0, int(getattr(config, "US_OPEN_SYMBOL_ALERT_COOLDOWN_MIN", 20) or 20))
            cooldown_sec = cooldown_min * 60
            fresh_opps = list(opps)
            if cooldown_sec > 0 and (not force):
                tmp = []
                for opp in opps:
                    sym = str(getattr(opp.signal, "symbol", "") or "")
                    last_ts = float(self._us_open_symbol_alert_ts.get(sym, 0.0) or 0.0)
                    if (now_ts - last_ts) >= cooldown_sec:
                        tmp.append(opp)
                fresh_opps = tmp
            fresh_symbols = [str(getattr(o.signal, "symbol", "") or "") for o in fresh_opps]
            if fresh_opps:
                report_store.save_report("us_open_monitor", fresh_opps)
            sent_any = False
            sent_opps = []
            if not previous:
                if fresh_opps:
                    sent_any = notifier.send_us_open_daytrade_summary(fresh_opps)
                    sent_opps = list(fresh_opps)
                elif realtime_mode:
                    sent_any = notifier.send_us_open_monitor_update([], periodic_ping=True)
                self._us_open_last_sent_ts = now_ts
            elif realtime_mode or top_changed or len(new_symbols) >= 2 or periodic_ping:
                if fresh_opps:
                    filtered_new_symbols = [s for s in new_symbols if s in set(fresh_symbols)]
                    sent_any = notifier.send_us_open_monitor_update(
                        fresh_opps,
                        new_symbols=filtered_new_symbols,
                        top_changed=(top_changed and bool(fresh_opps)),
                        periodic_ping=periodic_ping,
                    )
                    sent_opps = list(fresh_opps)
                elif realtime_mode or periodic_ping:
                    sent_any = notifier.send_us_open_monitor_update([], periodic_ping=True)
                self._us_open_last_sent_ts = now_ts
            if sent_any:
                for opp in sent_opps:
                    sym = str(getattr(opp.signal, "symbol", "") or "")
                    if sym:
                        self._us_open_symbol_alert_ts[sym] = now_ts
            if sent_any and config.SIGNAL_FEEDBACK_ENABLED:
                record_new_only = bool(getattr(config, "US_OPEN_RECORD_NEW_SYMBOLS_ONLY", True))
                rec_opps = sent_opps if record_new_only else opps
                for opp in rec_opps:
                    neural_brain.record_signal_sent(opp.signal, source="us_open_monitor")

            self._us_open_last_symbols = symbols
            self._maybe_send_us_open_quality_recap(force=force)
        except Exception as e:
            logger.error(f"[Scheduler] US OPEN smart monitor error: {e}", exc_info=True)

    def _run_vi_stock_scan(self):
        """VI-style US value + trend scan."""
        self._run_vi_profile_stock_scan(profile=None)

    def _run_vi_buffett_stock_scan(self):
        """Buffett-inspired VI scan (US)."""
        self._run_vi_profile_stock_scan(profile="BUFFETT")

    def _run_vi_turnaround_stock_scan(self):
        """Turnaround VI scan (US)."""
        self._run_vi_profile_stock_scan(profile="TURNAROUND")

    def _run_vi_profile_stock_scan(self, profile: str | None = None):
        try:
            prof = str(profile or "").strip().upper()
            if prof in {"BUFFETT", "TURNAROUND"}:
                logger.info("[Scheduler] Running US VI %s scan...", prof)
            else:
                logger.info("[Scheduler] Running US VALUE+TREND scan...")
            top_n = max(3, int(getattr(config, "VI_TOP_N", 10)))
            if prof == "BUFFETT":
                opps = stock_scanner.scan_us_value_trend_profile("BUFFETT", top_n=top_n)
                source_tag = "stocks_vi_buffett"
                feature_name = "scan_vi_buffett"
            elif prof == "TURNAROUND":
                opps = stock_scanner.scan_us_value_trend_profile("TURNAROUND", top_n=top_n)
                source_tag = "stocks_vi_turnaround"
                feature_name = "scan_vi_turnaround"
            else:
                opps = stock_scanner.scan_us_value_trend(top_n=top_n)
                source_tag = "stocks_vi"
                feature_name = "scan_vi"
            if not opps:
                logger.info("[Scheduler] %s: No qualifying candidates", (f"VI {prof}" if prof else "VI scan"))
                return

            for opp in opps:
                self._apply_neural_soft_adjustment(opp.signal, source=source_tag)
            report_name = f"us_vi_{prof.lower()}" if prof else "us_vi"
            report_store.save_report(report_name, opps)
            sent_summary = notifier.send_vi_stock_summary(opps, feature_override=feature_name)
            if sent_summary and config.SIGNAL_FEEDBACK_ENABLED:
                for opp in opps:
                    neural_brain.record_signal_sent(opp.signal, source=source_tag)

            top = opps[0]
            if self._raw_confidence(top.signal) >= config.STOCK_MIN_CONFIDENCE + 5:
                notifier.send_stock_signal(top, feature_override=feature_name)
        except Exception as e:
            logger.error(f"[Scheduler] VI profile scan error: {e}", exc_info=True)

    def _run_economic_calendar_alerts(self, force: bool = False):
        """Alert upcoming economic events on configured lead-time windows."""
        if (not config.ECON_CALENDAR_ENABLED) and (not force):
            return
        try:
            windows = config.get_econ_alert_windows()
            tol = max(1, int(config.ECON_ALERT_TOLERANCE_MIN))
            lookahead_min = max(windows) + tol + 2
            ccy = config.get_econ_alert_currencies()
            events = economic_calendar.upcoming_events(
                within_minutes=lookahead_min,
                min_impact=config.ECON_CALENDAR_MIN_IMPACT,
                currencies=ccy,
            )
            if not events:
                return

            window_buckets: dict[int, list] = {w: [] for w in windows}
            now_ts = time.time()

            for ev in events:
                mins_left = max(0, int(getattr(ev, "minutes_to_event", 0)))
                matches = [int(w) for w in windows if abs(mins_left - int(w)) <= tol]
                if not matches:
                    continue
                chosen = sorted(matches, key=lambda w: (abs(mins_left - w), -w))[0]
                key = f"{ev.event_id}:{int(chosen)}"
                if (not force) and (key in self._econ_alert_sent):
                    continue
                window_buckets[int(chosen)].append(ev)
                self._econ_alert_sent[key] = now_ts

            # Keep cache bounded.
            cutoff = now_ts - (72 * 3600)
            self._econ_alert_sent = {k: v for k, v in self._econ_alert_sent.items() if v >= cutoff}

            sent_total = 0
            for w in sorted(window_buckets.keys(), reverse=True):
                batch = window_buckets[w]
                if not batch:
                    continue
                if notifier.send_economic_calendar_alert(batch, window_minutes=w):
                    sent_total += len(batch)
            if sent_total:
                logger.info("[Scheduler] Economic calendar alerts sent: %d events", sent_total)
        except Exception as e:
            logger.error("[Scheduler] Economic calendar alert error: %s", e, exc_info=True)

    def _run_economic_calendar_snapshot(self):
        """Manual snapshot of upcoming calendar events."""
        try:
            hours = max(6, int(getattr(config, "ECON_CALENDAR_LOOKAHEAD_HOURS", 24)))
            events = economic_calendar.next_events(
                hours=hours,
                limit=10,
                min_impact="medium",
                currencies=config.get_econ_alert_currencies(),
            )
            notifier.send_economic_calendar_snapshot(events, lookahead_hours=hours)
        except Exception as e:
            logger.error("[Scheduler] Economic calendar snapshot error: %s", e, exc_info=True)

    def _run_macro_news_watch(self, force: bool = False):
        """Watch macro/policy headlines and send deduped high-impact alerts."""
        if (not config.MACRO_NEWS_ENABLED) and (not force):
            return
        try:
            lookback_h = max(1, int(config.MACRO_NEWS_LOOKBACK_HOURS))
            min_score = max(1, int(config.MACRO_NEWS_MIN_SCORE))
            max_age_min = max(30, int(getattr(config, "MACRO_NEWS_ALERT_MAX_AGE_MIN", 240)))
            max_per_run = max(1, int(getattr(config, "MACRO_NEWS_MAX_ALERTS_PER_RUN", 2)))
            require_priority = bool(getattr(config, "MACRO_NEWS_REQUIRE_PRIORITY_THEME", True))

            heads = macro_news.high_impact_headlines(hours=lookback_h, min_score=min_score, limit=20)
            if not heads:
                return
            fresh = []
            now_ts = time.time()
            now_utc = datetime.now(timezone.utc)
            for h in heads:
                hid = str(getattr(h, "headline_id", "") or "")
                if not hid:
                    continue
                age_min = max(0.0, (now_utc - h.published_utc).total_seconds() / 60.0)
                if age_min > float(max_age_min):
                    continue
                if require_priority and (not macro_news.is_priority_theme(h)):
                    continue
                if (not force) and (hid in self._macro_alert_sent):
                    continue
                fresh.append(h)

            if fresh:
                ranked, adapt_meta = self._rank_macro_alert_candidates(fresh, now_utc=now_utc, force=force)
                if not ranked:
                    logger.info(
                        "[Scheduler] Macro adaptive priority filtered all fresh headlines (dropped=%s)",
                        adapt_meta.get("dropped", len(fresh)),
                    )
                    return
                batch = ranked[:max_per_run]
                if not batch:
                    return
                for h in batch:
                    hid = str(getattr(h, "headline_id", "") or "")
                    if hid:
                        self._macro_alert_sent[hid] = now_ts
                # Keep dedupe store bounded.
                cutoff = now_ts - (72 * 3600)
                self._macro_alert_sent = {k: v for k, v in self._macro_alert_sent.items() if v >= cutoff}
                notifier.send_macro_news_alert(batch)
                logger.info(
                    "[Scheduler] Macro news alerts sent: %d headlines (adaptive kept=%s dropped=%s)",
                    len(batch),
                    adapt_meta.get("kept", len(batch)),
                    adapt_meta.get("dropped", 0),
                )
        except Exception as e:
            logger.error("[Scheduler] Macro news watch error: %s", e, exc_info=True)

    def _rank_macro_alert_candidates(self, headlines: list, now_utc: datetime | None = None, force: bool = False):
        """
        Phase 3: adaptive macro alert prioritization using observed theme effectiveness.
        Fail-safe: when disabled or unavailable, returns normal score/time sort.
        """
        items = list(headlines or [])
        if not items:
            return [], {"kept": 0, "dropped": 0, "adaptive": False}

        base_sorted = sorted(items, key=lambda x: (getattr(x, "score", 0), getattr(x, "published_utc", datetime.now(timezone.utc))), reverse=True)
        if (not bool(getattr(config, "MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED", True))) or force:
            return base_sorted, {"kept": len(base_sorted), "dropped": 0, "adaptive": False}

        try:
            weights = macro_news.dynamic_theme_weights_snapshot()
        except Exception:
            return base_sorted, {"kept": len(base_sorted), "dropped": 0, "adaptive": False}

        if not weights:
            return base_sorted, {"kept": len(base_sorted), "dropped": 0, "adaptive": False}

        now = now_utc or datetime.now(timezone.utc)
        min_samples = max(1, int(getattr(config, "MACRO_ALERT_ADAPTIVE_MIN_SAMPLES", getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES", 3))))
        min_mult = float(getattr(config, "MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT", "0.90"))
        skip_no_clear = float(getattr(config, "MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE", "65"))
        ultra_floor = int(getattr(config, "MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR", "10"))

        kept = []
        dropped = []
        for h in base_sorted:
            themes = list(getattr(h, "themes", []) or [])
            theme_meta = [weights.get(t) for t in themes if t in weights]
            eligible = [
                m for m in theme_meta
                if int((m or {}).get("sample_count", 0) or 0) >= min_samples
            ]

            avg_mult = 1.0
            avg_no_clear = None
            avg_confirmed = None
            if eligible:
                avg_mult = sum(float((m or {}).get("weight_mult", 1.0) or 1.0) for m in eligible) / len(eligible)
                avg_no_clear = sum(float((m or {}).get("no_clear_rate", 0.0) or 0.0) for m in eligible) / len(eligible)
                avg_confirmed = sum(float((m or {}).get("confirmed_rate", 0.0) or 0.0) for m in eligible) / len(eligible)

            age_min = max(0.0, (now - getattr(h, "published_utc", now)).total_seconds() / 60.0)
            freshness_bonus = 0.35 if age_min <= 30 else (0.15 if age_min <= 90 else 0.0)
            confirm_bonus = (float(avg_confirmed) / 100.0) * 0.75 if avg_confirmed is not None else 0.0
            no_clear_penalty = (float(avg_no_clear) / 100.0) * 0.55 if avg_no_clear is not None else 0.0
            src_q = float(getattr(h, "source_quality", 0.5) or 0.5)
            verification = str(getattr(h, "verification", "unverified") or "unverified").strip().lower()
            source_bonus = (src_q - 0.5) * 1.2
            verification_bonus = 0.7 if verification == "confirmed" else 0.0
            verification_penalty = 1.2 if verification in {"rumor", "mixed"} else 0.0
            adaptive_priority = (
                (float(getattr(h, "score", 0) or 0) * float(avg_mult))
                + freshness_bonus
                + confirm_bonus
                + source_bonus
                + verification_bonus
                - no_clear_penalty
                - verification_penalty
            )

            setattr(h, "_adaptive_priority", round(adaptive_priority, 4))
            setattr(h, "_adaptive_theme_mult", round(float(avg_mult), 4))
            setattr(h, "_adaptive_source_quality", round(float(src_q), 3))
            setattr(h, "_adaptive_verification", verification)
            if avg_no_clear is not None:
                setattr(h, "_adaptive_no_clear_rate", round(float(avg_no_clear), 1))

            weak_theme = (
                bool(eligible)
                and float(avg_mult) < min_mult
                and float(avg_no_clear or 0.0) >= skip_no_clear
                and int(getattr(h, "score", 0) or 0) < ultra_floor
            )
            weak_rumor = (
                verification in {"rumor", "mixed"}
                and float(src_q) < float(getattr(config, "MACRO_NEWS_TRUSTED_MIN_QUALITY", 0.80))
                and int(getattr(h, "score", 0) or 0) < (ultra_floor + 1)
            )
            if weak_theme or weak_rumor:
                dropped.append(h)
                continue
            kept.append(h)

        ranked = sorted(
            kept if kept else base_sorted,
            key=lambda x: (
                float(getattr(x, "_adaptive_priority", getattr(x, "score", 0) or 0)),
                float(getattr(x, "score", 0) or 0),
                getattr(x, "published_utc", now),
            ),
            reverse=True,
        )
        if dropped:
            logger.info(
                "[Scheduler] Macro adaptive priority filtered %d headline(s): %s",
                len(dropped),
                ", ".join(str(getattr(x, "headline_id", "") or "") for x in dropped[:5]),
            )
        return ranked, {"kept": len(kept), "dropped": len(dropped), "adaptive": True}

    def _run_macro_news_snapshot(self):
        """Manual macro risk snapshot."""
        try:
            lookback_h = max(1, int(config.MACRO_NEWS_LOOKBACK_HOURS))
            min_score = max(1, int(config.MACRO_NEWS_MIN_SCORE))
            heads = macro_news.high_impact_headlines(hours=lookback_h, min_score=min_score, limit=8)
            notifier.send_macro_news_snapshot(heads, lookback_hours=lookback_h)
        except Exception as e:
            logger.error("[Scheduler] Macro news snapshot error: %s", e, exc_info=True)

    def _run_macro_impact_tracker_sync(self):
        """Refresh post-news impact tracker samples for recent headlines."""
        if not bool(getattr(config, "MACRO_IMPACT_TRACKER_ENABLED", True)):
            return
        try:
            report = macro_impact_tracker.sync()
            logger.info(
                "[Scheduler] Macro impact tracker sync: headlines=%s ingested=%s sampled=%s weights_updated=%s status=%s",
                report.get("headlines", 0),
                report.get("ingested", 0),
                report.get("sampled", 0),
                report.get("weights_updated", 0),
                report.get("status", "ok"),
            )
        except Exception as e:
            logger.error("[Scheduler] Macro impact tracker sync error: %s", e, exc_info=True)

    def _run_macro_impact_report_snapshot(self):
        """Manual post-news impact report snapshot."""
        try:
            macro_impact_tracker.sync()
            hours = max(1, int(getattr(config, "MACRO_REPORT_DEFAULT_HOURS", 24)))
            min_score = max(1, int(getattr(config, "MACRO_NEWS_MIN_SCORE", 6)))
            report = macro_impact_tracker.build_report(hours=hours, min_score=min_score, limit=max(1, int(getattr(config, "MACRO_REPORT_MAX_HEADLINES", 5))))
            notifier.send_macro_impact_report(report)
        except Exception as e:
            logger.error("[Scheduler] Macro impact report snapshot error: %s", e, exc_info=True)

    def _run_macro_weights_snapshot(self, refresh: bool = False):
        """Manual snapshot of adaptive macro theme weights."""
        try:
            if refresh:
                macro_impact_tracker.refresh_adaptive_weights()
            report = macro_impact_tracker.build_weights_report(limit=max(1, int(getattr(config, "MACRO_WEIGHTS_DEFAULT_TOP", 8))))
            notifier.send_macro_weights_report(report)
        except Exception as e:
            logger.error("[Scheduler] Macro weights snapshot error: %s", e, exc_info=True)

    def _clear_signal_cache(self):
        """Clear the recently alerted symbols cache."""
        self._last_signal_symbols.clear()
        logger.info("[Scheduler] Signal cache cleared")

    def _adapt_intervals(self):
        """Dynamically adapt scan intervals based on session."""
        session_info = session_manager.get_session_info()
        if session_info["high_volatility"]:
            # More frequent during active sessions
            return {
                "xauusd": max(5 * 60, config.XAUUSD_SCAN_INTERVAL // 2),
                "crypto": max(2 * 60, config.CRYPTO_SCAN_INTERVAL // 2),
            }
        return {
            "xauusd": config.XAUUSD_SCAN_INTERVAL,
            "crypto": config.CRYPTO_SCAN_INTERVAL,
        }

    def setup_schedule(self):
        """Configure all scheduled tasks."""
        xauusd_mins = config.XAUUSD_SCAN_INTERVAL // 60
        crypto_mins = config.CRYPTO_SCAN_INTERVAL // 60
        fx_mins     = config.FX_SCAN_INTERVAL // 60
        stock_mins  = config.STOCK_SCAN_INTERVAL  // 60
        scalping_enabled = bool(getattr(config, "SCALPING_ENABLED", False))
        scalping_scan_sec = max(30, int(getattr(config, "SCALPING_SCAN_INTERVAL_SEC", 300) or 300))
        scalping_timeout_mins = max(1, int(getattr(config, "SCALPING_TIMEOUT_CHECK_INTERVAL_MIN", 1) or 1))
        scalping_timeout_enabled = scalping_enabled and bool(getattr(config, "SCALPING_EXECUTE_MT5", True)) and bool(getattr(config, "MT5_ENABLED", False))

        # ── Continuous scanners ──────────────────────────────────────────────
        schedule.every(xauusd_mins).minutes.do(self._run_xauusd_scan)

        # ── Fibonacci Advance (Sniper + Scout dual-speed) ─────────────────────
        if bool(getattr(config, "FIBO_ADVANCE_ENABLED", True)):
            fibo_interval_sec = max(60, int(getattr(config, "FIBO_ADVANCE_SCAN_INTERVAL_SEC", 300) or 300))
            if fibo_interval_sec < 60:
                schedule.every(fibo_interval_sec).seconds.do(self._run_fibo_advance_scan)
            else:
                schedule.every(fibo_interval_sec // 60).minutes.do(self._run_fibo_advance_scan)
            logger.info("[FiboAdvance] Scheduled every %ds (Sniper+Scout dual-speed)", fibo_interval_sec)
        # DISABLED: non-cTrader scans — BTC/ETH handled by scalping scanner via cTrader OpenAPI
        # schedule.every(crypto_mins).minutes.do(self._run_crypto_scan)
        # schedule.every(max(1, fx_mins)).minutes.do(self._run_fx_scan)
        # schedule.every(stock_mins).minutes.do(self._run_stock_scan)
        if scalping_enabled:
            if scalping_scan_sec < 60:
                schedule.every(scalping_scan_sec).seconds.do(self._run_scalping_scan)
            else:
                schedule.every(max(1, scalping_scan_sec // 60)).minutes.do(self._run_scalping_scan)
            if scalping_timeout_enabled:
                schedule.every(scalping_timeout_mins).minutes.do(self._run_scalping_timeout_manager)
        monitor_auto_enabled = bool(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", False))
        monitor_auto_interval_min = max(2, int(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_INTERVAL_MIN", 15) or 15))
        if monitor_auto_enabled:
            schedule.every(monitor_auto_interval_min).minutes.do(self._run_signal_monitor_auto_push)
        mt5_lane_scorecard_line = ""
        if bool(getattr(config, "MT5_LANE_SCORECARD_ENABLED", False)):
            scorecard_time = str(getattr(config, "MT5_LANE_SCORECARD_TIME_UTC", "00:10") or "00:10").strip()
            if len(scorecard_time) != 5 or ":" not in scorecard_time:
                scorecard_time = "00:10"
            schedule.every().day.at(scorecard_time).do(self._run_mt5_lane_scorecard)
            mt5_lane_scorecard_line = (
                f"  MT5 lane scorecard: daily {scorecard_time} UTC "
                f"(lookback={max(1, int(getattr(config, 'MT5_LANE_SCORECARD_LOOKBACK_DAYS', 1) or 1))}d)\n"
            )
        crypto_weekend_scorecard_line = ""
        if bool(getattr(config, "CRYPTO_WEEKEND_SCORECARD_ENABLED", False)):
            crypto_scorecard_time = str(getattr(config, "CRYPTO_WEEKEND_SCORECARD_TIME_UTC", "00:15") or "00:15").strip()
            if len(crypto_scorecard_time) != 5 or ":" not in crypto_scorecard_time:
                crypto_scorecard_time = "00:15"
            schedule.every().day.at(crypto_scorecard_time).do(self._run_crypto_weekend_scorecard)
            crypto_weekend_scorecard_line = (
                f"  Crypto weekend scorecard: daily {crypto_scorecard_time} UTC "
                f"(lookback={max(1, int(getattr(config, 'CRYPTO_WEEKEND_SCORECARD_LOOKBACK_DAYS', 14) or 14))}d)\n"
            )
        winner_mission_line = ""
        if bool(getattr(config, "WINNER_MISSION_REPORT_ENABLED", False)):
            winner_mission_time = str(getattr(config, "WINNER_MISSION_REPORT_TIME_UTC", "00:20") or "00:20").strip()
            if len(winner_mission_time) != 5 or ":" not in winner_mission_time:
                winner_mission_time = "00:20"
            schedule.every().day.at(winner_mission_time).do(self._run_winner_mission_report)
            winner_mission_line = (
                f"  Winner mission report: daily {winner_mission_time} UTC "
                f"(lookback={max(1, int(getattr(config, 'WINNER_MISSION_REPORT_LOOKBACK_DAYS', 14) or 14))}d)\n"
            )
        missed_audit_line = ""
        if bool(getattr(config, "MISSED_OPPORTUNITY_AUDIT_ENABLED", False)):
            audit_time = str(getattr(config, "MISSED_OPPORTUNITY_AUDIT_TIME_UTC", "00:25") or "00:25").strip()
            if len(audit_time) != 5 or ":" not in audit_time:
                audit_time = "00:25"
            schedule.every().day.at(audit_time).do(self._run_missed_opportunity_audit)
            missed_audit_line = (
                f"  Missed-opportunity audit: daily {audit_time} UTC "
                f"(lookback={max(1, int(getattr(config, 'MISSED_OPPORTUNITY_AUDIT_LOOKBACK_DAYS', 14) or 14))}d)\n"
            )
        auto_apply_line = ""
        if bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ENABLED", False)):
            auto_apply_time = str(getattr(config, "AUTO_APPLY_LIVE_PROFILE_TIME_UTC", "00:30") or "00:30").strip()
            if len(auto_apply_time) != 5 or ":" not in auto_apply_time:
                auto_apply_time = "00:30"
            schedule.every().day.at(auto_apply_time).do(self._run_auto_apply_live_profile)
            auto_apply_line = (
                f"  Auto-apply live profile: daily {auto_apply_time} UTC "
                f"(min_sample={max(2, int(getattr(config, 'AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE', 6) or 6))})\n"
            )
        canary_audit_line = ""
        if bool(getattr(config, "CANARY_POST_TRADE_AUDIT_ENABLED", False)):
            canary_audit_mins = max(1, int(getattr(config, "CANARY_POST_TRADE_AUDIT_INTERVAL_MIN", 3) or 3))
            schedule.every(canary_audit_mins).minutes.do(self._run_canary_post_trade_audit)
            canary_audit_line = (
                f"  Canary post-trade audit: every {canary_audit_mins}m "
                f"(lookback={max(1, int(getattr(config, 'CANARY_POST_TRADE_AUDIT_LOOKBACK_DAYS', 14) or 14))}d)\n"
            )
        ctrader_integrity_line = ""
        if bool(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_ENABLED", False)):
            integrity_mins = max(10, int(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_INTERVAL_MIN", 30) or 30))
            schedule.every(integrity_mins).minutes.do(self._run_ctrader_data_integrity_report)
            ctrader_integrity_line = (
                f"  cTrader data integrity report: every {integrity_mins}m "
                f"(lookback={max(1, int(getattr(config, 'CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS', 180) or 180))}d)\n"
            )
        xau_direct_lane_line = ""
        if bool(getattr(config, "XAU_DIRECT_LANE_REPORT_ENABLED", False)):
            xau_direct_lane_mins = max(15, int(getattr(config, "XAU_DIRECT_LANE_REPORT_INTERVAL_MIN", 60) or 60))
            schedule.every(xau_direct_lane_mins).minutes.do(self._run_xau_direct_lane_report)
            xau_direct_lane_line = (
                f"  XAU direct lane report: every {xau_direct_lane_mins}m "
                f"(lookback={max(1, int(getattr(config, 'XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS', 72) or 72))}h)\n"
            )
        xau_direct_lane_tune_line = ""
        if bool(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_ENABLED", False)):
            xau_direct_lane_tune_mins = max(60, int(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN", 120) or 120))
            schedule.every(xau_direct_lane_tune_mins).minutes.do(self._run_xau_direct_lane_auto_tune)
            xau_direct_lane_tune_line = (
                f"  XAU direct lane auto-tune: every {xau_direct_lane_tune_mins}m "
                f"(lookback={max(1, int(getattr(config, 'XAU_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS', 24) or 24))}h)\n"
            )
        xau_shadow_bt_line = ""
        if bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)):
            shadow_bt_mins = max(15, int(getattr(config, "XAU_SHADOW_BACKTEST_INTERVAL_MIN", 30) or 30))
            schedule.every(shadow_bt_mins).minutes.do(self._run_xau_shadow_backtest)
            xau_shadow_bt_line = f"  XAU shadow backtest resolver: every {shadow_bt_mins}m\n"
        param_trial_line = ""
        if bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)):
            trial_bt_mins = max(10, int(getattr(config, "XAU_DIRECT_LANE_TRIAL_BT_INTERVAL_MIN", 15) or 15))
            schedule.every(trial_bt_mins).minutes.do(self._run_parameter_trial_bt)
            param_trial_line = f"  Parameter trial sandbox BT: every {trial_bt_mins}m\n"
        btc_auto_tune_line = ""
        if bool(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_ENABLED", True)):
            btc_tune_mins = max(60, int(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_INTERVAL_MIN", 120) or 120))
            schedule.every(btc_tune_mins).minutes.do(self._run_btc_direct_lane_auto_tune)
            btc_auto_tune_line = f"  BTC direct lane auto-tune (BFSS/BFLS/BRR): every {btc_tune_mins}m\n"
        conductor_line = ""
        if bool(getattr(config, "CONDUCTOR_ENABLED", True)):
            conductor_mins = max(15, int(getattr(config, "CONDUCTOR_INTERVAL_MIN", 30) or 30))
            schedule.every(conductor_mins).minutes.do(self._run_conductor_cycle)
            conductor_line = f"  Conductor (multi-agent AI): every {conductor_mins}m\n"
        strategy_lab_line = ""
        if bool(getattr(config, "STRATEGY_LAB_REPORT_ENABLED", False)):
            strategy_lab_mins = max(5, int(getattr(config, "STRATEGY_LAB_REPORT_INTERVAL_MIN", 15) or 15))
            schedule.every(strategy_lab_mins).minutes.do(self._run_strategy_lab_report)
            strategy_lab_line = f"  Strategy lab report: every {strategy_lab_mins}m\n"
        family_calibration_line = ""
        if bool(getattr(config, "FAMILY_CALIBRATION_REPORT_ENABLED", False)):
            family_calibration_mins = max(5, int(getattr(config, "FAMILY_CALIBRATION_REPORT_INTERVAL_MIN", 15) or 15))
            schedule.every(family_calibration_mins).minutes.do(self._run_family_calibration_report)
            family_calibration_line = (
                f"  Family calibration report: every {family_calibration_mins}m "
                f"(lookback={max(1, int(getattr(config, 'FAMILY_CALIBRATION_REPORT_LOOKBACK_DAYS', 21) or 21))}d)\n"
            )
        sharpness_feedback_line = ""
        if bool(getattr(config, "XAU_SHARPNESS_FEEDBACK_ENABLED", True)):
            sharpness_fb_mins = max(30, int(getattr(config, "XAU_SHARPNESS_FEEDBACK_INTERVAL_MIN", 120) or 120))
            schedule.every(sharpness_fb_mins).minutes.do(self._run_sharpness_feedback_report)
            sharpness_feedback_line = (
                f"  Sharpness feedback loop: every {sharpness_fb_mins}m "
                f"(lookback={max(1, int(getattr(config, 'XAU_SHARPNESS_FEEDBACK_LOOKBACK_DAYS', 14) or 14))}d"
                f" auto-cal={'ON' if bool(getattr(config, 'XAU_SHARPNESS_AUTO_CALIBRATE_ENABLED', False)) else 'OFF'}"
                f" decay={'ON' if bool(getattr(config, 'XAU_FAMILY_DECAY_ENABLED', True)) else 'OFF'})\n"
            )
        volume_profile_line = ""
        if bool(getattr(config, "XAU_VOLUME_PROFILE_ENABLED", True)):
            vp_mins = max(10, int(getattr(config, "XAU_VOLUME_PROFILE_INTERVAL_MIN", 30) or 30))
            schedule.every(vp_mins).minutes.do(self._run_volume_profile_report)
            volume_profile_line = (
                f"  Volume profile: every {vp_mins}m "
                f"(lookback={max(1, int(getattr(config, 'XAU_VOLUME_PROFILE_HOURS_BACK', 24) or 24))}h"
                f" bucket={max(1, int(getattr(config, 'XAU_VOLUME_PROFILE_BUCKET_TICKS', 10) or 10))}ticks)\n"
            )
        ctrader_market_capture_line = ""
        if bool(getattr(config, "CTRADER_MARKET_CAPTURE_ENABLED", False)) and bool(getattr(config, "CTRADER_ENABLED", False)):
            capture_mins = max(1, int(getattr(config, "CTRADER_MARKET_CAPTURE_INTERVAL_MIN", 5) or 5))
            schedule.every(capture_mins).minutes.do(self._run_ctrader_market_capture)
            ctrader_market_capture_line = (
                f"  cTrader market capture: every {capture_mins}m "
                f"(duration={max(3, int(getattr(config, 'CTRADER_MARKET_CAPTURE_DURATION_SEC', 12) or 12))}s "
                f"symbols={','.join(sorted(list(config.get_ctrader_market_capture_symbols() or set())))}"
                f" depth={max(1, int(getattr(config, 'CTRADER_MARKET_CAPTURE_DEPTH_LEVELS', 5) or 5))})\n"
            )
        ctrader_replay_lab_line = ""
        if bool(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_ENABLED", False)):
            replay_mins = max(5, int(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_INTERVAL_MIN", 20) or 20))
            schedule.every(replay_mins).minutes.do(self._run_ctrader_tick_depth_replay_lab)
            ctrader_replay_lab_line = (
                f"  cTrader replay lab: every {replay_mins}m "
                f"(lookback={max(1, int(getattr(config, 'CTRADER_TICK_DEPTH_REPLAY_LAB_LOOKBACK_DAYS', 7) or 7))}d "
                f"window={max(10, int(getattr(config, 'CTRADER_TICK_DEPTH_REPLAY_LAB_REPLAY_WINDOW_SEC', 45) or 45))}s)\n"
            )
        mission_progress_line = ""
        if bool(getattr(config, "MISSION_PROGRESS_REPORT_ENABLED", False)):
            mission_progress_mins = max(3, int(getattr(config, "MISSION_PROGRESS_REPORT_INTERVAL_MIN", 10) or 10))
            schedule.every(mission_progress_mins).minutes.do(self._run_mission_progress_report)
            mission_progress_line = f"  Mission progress report: every {mission_progress_mins}m\n"
        trading_manager_line = ""
        if bool(getattr(config, "TRADING_MANAGER_REPORT_ENABLED", False)):
            trading_manager_mins = max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))
            schedule.every(trading_manager_mins).minutes.do(self._run_trading_manager_report)
            trading_manager_line = (
                f"  Trading manager report: every {trading_manager_mins}m "
                f"(lookback={max(1, int(getattr(config, 'TRADING_MANAGER_REPORT_LOOKBACK_HOURS', 24) or 24))}h)\n"
            )
        xau_guard_transition_line = ""
        if bool(getattr(config, "XAU_GUARD_TRANSITION_ALERT_ENABLED", True)):
            xau_guard_sec = max(15, int(getattr(config, "XAU_GUARD_TRANSITION_WATCH_INTERVAL_SEC", 30) or 30))
            schedule.every(xau_guard_sec).seconds.do(self._run_xau_guard_transition_watch)
            xau_guard_transition_line = f"  XAU guard transition watch: every {xau_guard_sec}s\n"
        # DISABLED: US open monitor uses stock_scanner (not cTrader)
        # schedule.every(max(3, config.US_OPEN_SMART_INTERVAL_MIN)).minutes.do(self._run_us_open_smart_monitor)
        schedule.every(max(2, int(config.ECON_CALENDAR_CHECK_INTERVAL_MIN))).minutes.do(self._run_economic_calendar_alerts)
        schedule.every(max(5, int(config.MACRO_NEWS_CHECK_INTERVAL_MIN))).minutes.do(self._run_macro_news_watch)
        if bool(getattr(config, "MACRO_IMPACT_TRACKER_ENABLED", True)):
            schedule.every(max(5, int(getattr(config, "MACRO_IMPACT_TRACKER_SYNC_INTERVAL_MIN", 15)))).minutes.do(self._run_macro_impact_tracker_sync)

        # ── Market-open triggered scans (UTC times) ──────────────────────────
        # DISABLED: stock/thai scans — cTrader OpenAPI only
        # schedule.every().day.at("03:35").do(self._run_thai_scan)
        # schedule.every().day.at("01:35").do(self._run_stock_scan)
        # schedule.every().day.at("08:05").do(self._run_stock_scan)
        # Gold overview at London open
        schedule.every().day.at("07:00").do(self._run_gold_overview)
        # DISABLED: US stock scans
        # schedule.every().day.at("13:35").do(self._run_us_open_daytrade)
        # schedule.every().day.at("14:35").do(self._run_us_open_daytrade)
        # Gold overview at NY open
        schedule.every().day.at("13:00").do(self._run_gold_overview)
        # DISABLED: US stock scans
        # schedule.every().day.at("16:00").do(self._run_stock_scan)
        # schedule.every().day.at("19:55").do(self._run_us_scan)

        # ── Maintenance ──────────────────────────────────────────────────────
        schedule.every(3).hours.do(self._clear_signal_cache)
        neural_mins = max(5, int(config.NEURAL_BRAIN_SYNC_INTERVAL_MIN))
        schedule.every(neural_mins).minutes.do(self._run_neural_sync_train)
        neural_mission_line = ""
        if bool(getattr(config, "NEURAL_MISSION_AUTO_ENABLED", False)):
            mission_mins = max(5, int(getattr(config, "NEURAL_MISSION_INTERVAL_MIN", 60)))
            schedule.every(mission_mins).minutes.do(self._run_neural_mission_cycle_async)
            mission_line_symbols = self._normalize_neural_mission_symbols()
            neural_mission_line = (
                f"  Neural mission loop: every {mission_mins}m "
                f"(symbols={mission_line_symbols}; iters={max(1, int(getattr(config, 'NEURAL_MISSION_ITERATIONS_PER_CYCLE', 1)))})\n"
            )
        mt5_autopilot_line = ""
        if bool(getattr(config, "MT5_AUTOPILOT_ENABLED", True)) and bool(getattr(config, "MT5_ENABLED", False)):
            mt5_auto_mins = max(5, int(getattr(config, "MT5_AUTOPILOT_SYNC_INTERVAL_MIN", 15)))
            schedule.every(mt5_auto_mins).minutes.do(self._run_mt5_autopilot_sync)
            mt5_autopilot_line = f"  MT5 autopilot sync: every {mt5_auto_mins}m\n"
        mt5_pm_line = ""
        if (bool(getattr(config, "MT5_POSITION_MANAGER_ENABLED", True)) or bool(getattr(config, "MT5_LIMIT_ENTRY_ENABLED", True))) and bool(getattr(config, "MT5_ENABLED", False)):
            if bool(getattr(config, "MT5_POSITION_MANAGER_ENABLED", True)):
                mt5_pm_mins = max(1, int(getattr(config, "MT5_POSITION_MANAGER_INTERVAL_MIN", 1)))
                schedule.every(mt5_pm_mins).minutes.do(self._run_mt5_position_manager)
                mt5_pm_line += f"  MT5 position manager: every {mt5_pm_mins}m\n"
            if bool(getattr(config, "MT5_LIMIT_ENTRY_ENABLED", True)):
                mt5_limit_mins = max(1, int(getattr(config, "MT5_LIMIT_ENTRY_INTERVAL_MIN", 3)))
                schedule.every(mt5_limit_mins).minutes.do(self._run_mt5_limit_manager)
                mt5_pm_line += f"  MT5 limit manager: every {mt5_limit_mins}m\n"
        mt5_bypass_tp_line = ""
        if bool(getattr(config, "MT5_ENABLED", False)) and bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False)) and bool(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_ENABLED", False)):
            tp_sec = max(5, int(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_INTERVAL_SEC", 20) or 20))
            schedule.every(tp_sec).seconds.do(self._run_mt5_bypass_quick_tp)
            mt5_bypass_tp_line = (
                f"  MT5 bypass quick-TP: every {tp_sec}s "
                f"(target={float(getattr(config, 'MT5_BYPASS_TEST_QUICK_TP_BALANCE_PCT', 1.0) or 1.0):.2f}% "
                f"min_usd={float(getattr(config, 'MT5_BYPASS_TEST_QUICK_TP_MIN_USD', 1.0) or 1.0):.2f})\n"
            )
        mt5_preclose_line = ""
        if bool(getattr(config, "MT5_ENABLED", False)) and bool(getattr(config, "MT5_PRE_CLOSE_FLATTEN_ENABLED", False)):
            preclose_mins = max(1, int(getattr(config, "MT5_PRE_CLOSE_FLATTEN_CHECK_INTERVAL_MIN", 5) or 5))
            schedule.every(preclose_mins).minutes.do(self._run_mt5_preclose_flatten)
            mt5_preclose_line = (
                f"  MT5 pre-close flatten: every {preclose_mins}m "
                f"(fri_only={bool(getattr(config, 'MT5_PRE_CLOSE_FLATTEN_FRI_ONLY', True))}; "
                f"time={int(getattr(config, 'MT5_PRE_CLOSE_FLATTEN_NY_HOUR', 16) or 16):02d}:"
                f"{int(getattr(config, 'MT5_PRE_CLOSE_FLATTEN_NY_MINUTE', 50) or 50):02d} NY)\n"
            )
        ctrader_sync_line = ""
        if bool(getattr(config, "CTRADER_ENABLED", False)) and bool(getattr(config, "CTRADER_SYNC_ENABLED", True)):
            ctrader_sync_mins = max(1, int(getattr(config, "CTRADER_SYNC_INTERVAL_MIN", 1) or 1))
            schedule.every(ctrader_sync_mins).minutes.do(self._run_ctrader_sync)
            ctrader_sync_line = f"  cTrader sync: every {ctrader_sync_mins}m\n"
        mt5_readiness_line = ""
        if bool(getattr(config, "MT5_ENABLED", False)) and bool(getattr(config, "MT5_READINESS_CHECK_ON_START", True)):
            readiness_time = str(getattr(config, "MT5_READINESS_CHECK_TIME_UTC", "00:02") or "00:02").strip() or "00:02"
            try:
                schedule.every().day.at(readiness_time).do(self._run_mt5_readiness_check)
                mt5_readiness_line = f"  MT5 readiness: daily {readiness_time} UTC\n"
            except Exception:
                mt5_readiness_line = ""
        macro_impact_line = (
            f"  Macro impact tracker sync: every {max(5, int(getattr(config, 'MACRO_IMPACT_TRACKER_SYNC_INTERVAL_MIN', 15)))}m\n"
            if bool(getattr(config, "MACRO_IMPACT_TRACKER_ENABLED", True)) else ""
        )
        scalping_line = ""
        if scalping_enabled:
            if scalping_scan_sec < 60:
                scalping_line += f"  Scalping scan: every {scalping_scan_sec}s (M5+M1)\n"
            else:
                scalping_line += f"  Scalping scan: every {max(1, scalping_scan_sec // 60)}m (M5+M1)\n"
            if scalping_timeout_enabled:
                scalping_line += (
                    f"  Scalping timeout manager: every {scalping_timeout_mins}m "
                    f"(close timeout={max(1, int(getattr(config, 'SCALPING_CLOSE_TIMEOUT_MIN', 35)))}m)\n"
                )
            else:
                scalping_line += "  Scalping timeout manager: disabled (requires SCALPING_EXECUTE_MT5=1 + MT5_ENABLED=1)\n"
        monitor_line = ""
        if monitor_auto_enabled:
            auto_symbols = ",".join(config.get_signal_monitor_auto_symbols())
            monitor_line = (
                f"  Signal monitor auto-push: every {monitor_auto_interval_min}m "
                f"(symbols={auto_symbols}; window={config.get_signal_monitor_auto_window_mode()})\n"
            )

        logger.info(
            f"[Scheduler] Jobs configured:\n"
            f"  XAUUSD:  every {xauusd_mins}m\n"
            f"  US Open Smart Monitor: every {max(3, config.US_OPEN_SMART_INTERVAL_MIN)}m "
            f"(pre-open {max(0, int(getattr(config, 'US_OPEN_SMART_PREMARKET_LEAD_MIN', 60)))}m "
            f"+ post-open {max(30, int(getattr(config, 'US_OPEN_SMART_POST_OPEN_MAX_MIN', 120)))}m"
            f"{' + mood-stop' if bool(getattr(config, 'US_OPEN_MOOD_STOP_ENABLED', True)) else ''})\n"
            f"  Economic calendar: every {max(2, int(config.ECON_CALENDAR_CHECK_INTERVAL_MIN))}m\n"
            f"  Macro headline watch: every {max(5, int(config.MACRO_NEWS_CHECK_INTERVAL_MIN))}m\n"
            f"{macro_impact_line}"
            f"{scalping_line}"
            f"{monitor_line}"
            f"{mt5_lane_scorecard_line}"
            f"{crypto_weekend_scorecard_line}"
            f"{winner_mission_line}"
            f"{missed_audit_line}"
            f"{auto_apply_line}"
            f"{canary_audit_line}"
            f"{ctrader_integrity_line}"
            f"{xau_direct_lane_line}"
            f"{xau_direct_lane_tune_line}"
            f"{xau_shadow_bt_line}"
            f"{param_trial_line}"
            f"{btc_auto_tune_line}"
            f"{conductor_line}"
            f"{strategy_lab_line}"
            f"{family_calibration_line}"
            f"{sharpness_feedback_line}"
            f"{volume_profile_line}"
            f"{ctrader_market_capture_line}"
            f"{ctrader_replay_lab_line}"
            f"{mission_progress_line}"
            f"{trading_manager_line}"
            f"{xau_guard_transition_line}"
            f"{mt5_autopilot_line}"
            f"{mt5_pm_line}"
            f"{mt5_bypass_tp_line}"
            f"{mt5_preclose_line}"
            f"{mt5_readiness_line}"
            f"{ctrader_sync_line}"
            f"{neural_mission_line}"
            f"  Thai SET50: 03:35 UTC daily\n"
            f"  US Open Plan: 13:35 & 14:35 UTC daily (DST-safe)\n"
            f"  Gold overviews: 07:00 & 13:00 UTC daily\n"
            f"  Neural sync/train: every {neural_mins}m\n"
        )

    def _run_loop(self):
        """Main scheduler loop (runs in background thread)."""
        self.setup_schedule()
        logger.info("[Scheduler] Background loop started")

        # Run initial scans on startup
        time.sleep(5)
        self._run_mt5_readiness_check()
        time.sleep(2)
        self._run_gold_overview()
        time.sleep(10)
        self._run_xauusd_scan()
        time.sleep(5)
        # DISABLED: non-cTrader startup scans
        # self._run_crypto_scan()
        # self._run_fx_scan()
        # self._run_stock_scan()
        self._run_economic_calendar_alerts()
        self._run_macro_impact_tracker_sync()
        self._run_xau_guard_transition_watch(force=True)
        self._run_mt5_autopilot_sync()
        self._run_mt5_position_manager()
        self._run_mt5_bypass_quick_tp()
        self._run_mt5_preclose_flatten()
        self._run_scalping_scan()
        self._run_scalping_timeout_manager()
        self._run_ctrader_sync()  # startup reconcile — catches deals missed during offline period
        self._run_neural_sync_train()
        self._run_neural_mission_cycle_async(source="scheduler_startup")
        if bool(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", False)) and bool(getattr(config, "SIGNAL_MONITOR_AUTO_PUSH_ON_START", True)):
            self._run_signal_monitor_auto_push(force=True)
        if bool(getattr(config, "MT5_LANE_SCORECARD_ENABLED", False)) and bool(getattr(config, "MT5_LANE_SCORECARD_ON_START", True)):
            self._run_mt5_lane_scorecard(force=True)
        if bool(getattr(config, "CRYPTO_WEEKEND_SCORECARD_ENABLED", False)) and bool(getattr(config, "CRYPTO_WEEKEND_SCORECARD_ON_START", True)):
            self._run_crypto_weekend_scorecard(force=True)
        if bool(getattr(config, "WINNER_MISSION_REPORT_ENABLED", False)) and bool(getattr(config, "WINNER_MISSION_REPORT_ON_START", True)):
            self._run_winner_mission_report(force=True)
        if bool(getattr(config, "MISSED_OPPORTUNITY_AUDIT_ENABLED", False)) and bool(getattr(config, "MISSED_OPPORTUNITY_AUDIT_ON_START", True)):
            self._run_missed_opportunity_audit(force=True)
        if bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ENABLED", False)) and bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ON_START", True)):
            self._run_auto_apply_live_profile(force=True)
        if bool(getattr(config, "CANARY_POST_TRADE_AUDIT_ENABLED", False)) and bool(getattr(config, "CANARY_POST_TRADE_AUDIT_ON_START", True)):
            self._run_canary_post_trade_audit(force=True)
        if bool(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_ENABLED", False)) and bool(getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_ON_START", True)):
            self._run_ctrader_data_integrity_report(force=True)
        if bool(getattr(config, "XAU_DIRECT_LANE_REPORT_ENABLED", False)) and bool(getattr(config, "XAU_DIRECT_LANE_REPORT_ON_START", True)):
            self._run_xau_direct_lane_report(force=True)
        if bool(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_ENABLED", False)) and bool(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_ON_START", True)):
            self._run_xau_direct_lane_auto_tune(force=True)
        if bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)) and bool(getattr(config, "XAU_SHADOW_BACKTEST_ON_START", True)):
            self._run_xau_shadow_backtest(force=True)
        if bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)) and bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_BT_ON_START", True)):
            self._run_parameter_trial_bt(force=True)
        if bool(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_ENABLED", True)) and bool(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_ON_START", True)):
            self._run_btc_direct_lane_auto_tune(force=True)
        if bool(getattr(config, "CONDUCTOR_ENABLED", True)) and bool(getattr(config, "CONDUCTOR_ON_START", True)):
            self._run_conductor_cycle(force=True)
        if bool(getattr(config, "FAMILY_CALIBRATION_REPORT_ENABLED", False)) and bool(getattr(config, "FAMILY_CALIBRATION_REPORT_ON_START", True)):
            self._run_family_calibration_report(force=True)
        if bool(getattr(config, "XAU_SHARPNESS_FEEDBACK_ENABLED", True)) and bool(getattr(config, "XAU_SHARPNESS_FEEDBACK_ON_START", True)):
            self._run_sharpness_feedback_report(force=True)
        if bool(getattr(config, "XAU_VOLUME_PROFILE_ENABLED", True)):
            self._run_volume_profile_report(force=True)
        if bool(getattr(config, "STRATEGY_LAB_REPORT_ENABLED", False)) and bool(getattr(config, "STRATEGY_LAB_REPORT_ON_START", True)):
            self._run_strategy_lab_report(force=True)
        if bool(getattr(config, "CTRADER_MARKET_CAPTURE_ENABLED", False)) and bool(getattr(config, "CTRADER_MARKET_CAPTURE_ON_START", False)):
            self._run_ctrader_market_capture(force=True)
        if bool(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_ENABLED", False)) and bool(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_ON_START", True)):
            self._run_ctrader_tick_depth_replay_lab(force=True)
        if bool(getattr(config, "MISSION_PROGRESS_REPORT_ENABLED", False)) and bool(getattr(config, "MISSION_PROGRESS_REPORT_ON_START", True)):
            self._run_mission_progress_report(force=True)
        if bool(getattr(config, "TRADING_MANAGER_REPORT_ENABLED", False)) and bool(getattr(config, "TRADING_MANAGER_REPORT_ON_START", True)):
            self._run_trading_manager_report(force=True)

        while self.running:
            schedule.run_pending()
            try:
                idle = schedule.idle_seconds()
            except Exception:
                idle = None
            sleep_sec = 5.0 if idle is None else max(1.0, min(10.0, float(idle)))
            if bool(getattr(config, "MT5_ENABLED", False)) and bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False)) and bool(getattr(config, "MT5_BYPASS_TEST_QUICK_TP_ENABLED", False)):
                sleep_sec = min(sleep_sec, 5.0)
            time.sleep(float(sleep_sec))

        logger.info("[Scheduler] Background loop stopped")

    def start(self):
        """Start the background scheduler thread."""
        if self.running:
            logger.warning("[Scheduler] Already running")
            return
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="DexterScheduler")
        self._thread.start()
        logger.info("[Scheduler] Started in background thread")

    def stop(self):
        """Stop the background scheduler."""
        self.running = False
        schedule.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("[Scheduler] Stopped")

    def run_once(self, task: str = "all"):
        """Manually trigger a single scan run (for CLI usage)."""
        results: dict = {}
        if task in ("xauusd", "gold", "all"):
            results["xauusd"] = self._run_xauusd_scan(force_alert=True, source="manual")
        # crypto scan disabled — BTC/ETH via cTrader scalping scanner
        if task in ("crypto",):
            logger.info("[Scheduler] Crypto sniper disabled — use 'scalp' for BTC/ETH via cTrader")
        if task in ("scalp", "scalping", "scalp_signals", "all"):
            results["scalping"] = self._run_scalping_scan(force=True)
        # DISABLED: fx/stocks/thai/us scans — cTrader OpenAPI only
        if task in ("fx", "forex", "stocks", "thai", "thailand", "thai_vi", "th_vi", "us", "us_open", "us_open_plan"):
            logger.info(f"[Scheduler] '{task}' scan disabled — system uses cTrader OpenAPI only")
        if task in ("us_open_monitor", "monitor_us"):
            self._run_us_open_smart_monitor(force=True)
        if task in ("overview", "all"):
            self._run_gold_overview()
        if task in ("calendar", "eco", "economic"):
            self._run_economic_calendar_snapshot()
        if task in ("macro", "macro_news"):
            self._run_macro_news_snapshot()
        if task in ("macro_report", "macro_impact", "macro_impact_report"):
            self._run_macro_impact_report_snapshot()
        if task in ("macro_weights", "macro_weight", "macro_weights_report"):
            self._run_macro_weights_snapshot()
        if task in ("signal_monitor", "signal_monitor_push", "monitor_signals"):
            results["signal_monitor"] = self._run_signal_monitor_auto_push(force=True)
        if task in ("crypto_weekend", "crypto_weekend_scorecard", "crypto_scorecard"):
            results["crypto_weekend_scorecard"] = self._run_crypto_weekend_scorecard(force=True)
        if task in ("winner_mission", "winner_mission_report", "mission_report"):
            results["winner_mission_report"] = self._run_winner_mission_report(force=True)
        if task in ("missed_audit", "missed_opportunity", "missed_opportunity_audit"):
            results["missed_opportunity_audit_report"] = self._run_missed_opportunity_audit(force=True)
        if task in ("auto_apply", "live_profile", "auto_apply_live_profile"):
            results["auto_apply_live_profile_report"] = self._run_auto_apply_live_profile(force=True)
        if task in ("canary_audit", "canary_post_trade", "canary_post_trade_audit"):
            results["canary_post_trade_audit_report"] = self._run_canary_post_trade_audit(force=True)
        if task in ("ctrader_data_integrity", "data_integrity", "integrity_report"):
            results["ctrader_data_integrity_report"] = self._run_ctrader_data_integrity_report(force=True)
        if task in ("strategy_lab", "strategy_lab_report"):
            results["strategy_lab_report"] = self._run_strategy_lab_report(force=True)
        if task in ("family_calibration", "calibration", "family_calibration_report"):
            results["family_calibration_report"] = self._run_family_calibration_report(force=True)
        if task in ("sharpness_feedback", "sharpness_report", "sharpness"):
            results["sharpness_feedback_report"] = self._run_sharpness_feedback_report(force=True)
        if task in ("volume_profile", "vp", "vp_report"):
            results["volume_profile"] = self._run_volume_profile_report(force=True)
        if task in ("dom_liquidity", "dom_shift", "liquidity_shift"):
            results["dom_liquidity"] = self._get_dom_liquidity_shift(symbol="XAUUSD", direction="long")
        if task in ("ctrader_capture", "market_capture", "ctrader_market_capture"):
            results["ctrader_market_capture"] = self._run_ctrader_market_capture(force=True)
        if task in ("ctrader_replay", "replay_lab", "ctrader_tick_depth_replay_lab"):
            results["ctrader_tick_depth_replay_report"] = self._run_ctrader_tick_depth_replay_lab(force=True)
        if task in ("mission_progress", "progress_report", "mission_progress_report"):
            results["mission_progress_report"] = self._run_mission_progress_report(force=True)
        if task in ("trading_manager", "trading_manager_report", "manager_report"):
            results["trading_manager_report"] = self._run_trading_manager_report(force=True)
        if task in ("mission", "neural_mission"):
            results["mission"] = self._run_neural_mission_cycle(source="manual")
        if task in ("vi", "value", "value_trend"):
            self._run_vi_stock_scan()
        if task in ("vi_buffett", "buffett", "value_buffett"):
            self._run_vi_buffett_stock_scan()
        if task in ("vi_turnaround", "turnaround", "value_turnaround"):
            self._run_vi_turnaround_stock_scan()
        return results


scheduler = DexterScheduler()
