from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from config import config
from market.economic_calendar import economic_calendar
from market.macro_impact_tracker import macro_impact_tracker
from market.macro_news import macro_news
from learning.live_profile_autopilot import (
    _actual_entry_from_deal,
    _classify_chart_state,
    _classify_trade_abnormality,
    _extract_request_context,
    _finalize_bucket,
    _iso,
    _iso_to_ms,
    _ms_to_iso,
    _new_bucket,
    _norm_signature,
    _norm_source,
    _norm_symbol,
    _safe_float,
    _safe_int,
    _safe_json_dict,
    _update_bucket,
    _utc_now,
    live_profile_autopilot,
)
from learning.neural_brain import neural_brain


def _avg(values: list[float]) -> float:
    vals = [float(v) for v in list(values or [])]
    return float(sum(vals) / len(vals)) if vals else 0.0


def _parse_iso_fallback(value) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


class TradingManagerAgent:
    def __init__(self, *, report_dir: str | None = None, ctrader_db_path: str | None = None):
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / "data"
        self.report_dir = Path(report_dir or (data_dir / "reports"))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.ctrader_db_path = Path(ctrader_db_path or live_profile_autopilot.ctrader_db_path)
        runtime_dir = data_dir / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = runtime_dir / "trading_manager_state.json"

    def _connect_ctrader(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.ctrader_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _report_path(self, name: str) -> Path:
        return self.report_dir / f"{name}.json"

    def _save_report(self, name: str, payload: dict) -> None:
        self._report_path(name).write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_state(self) -> dict:
        try:
            if self.state_path.exists():
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
        return {}

    def _save_state(self, payload: dict) -> None:
        self.state_path.write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _load_json(path: Path) -> dict:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

    @staticmethod
    def _version_info() -> dict:
        return live_profile_autopilot._version_info()

    @staticmethod
    def _execution_scope() -> dict:
        return live_profile_autopilot._execution_scope()

    @staticmethod
    def _current_value(key: str) -> str:
        try:
            return str(live_profile_autopilot._current_value(str(key)) or "")
        except Exception:
            return ""

    @staticmethod
    def _apply_runtime_value(key: str, value: str) -> None:
        try:
            live_profile_autopilot._apply_runtime_value(str(key), str(value))
        except Exception:
            pass

    @staticmethod
    def _persist_env_value(key: str, value: str, persist: bool) -> dict:
        if not bool(persist):
            return {"ok": True, "updated": False, "reason": "persist_disabled"}
        try:
            return live_profile_autopilot._upsert_env_key(live_profile_autopilot.env_local_path, str(key), str(value))
        except Exception as e:
            return {"ok": False, "updated": False, "reason": str(e)}

    @staticmethod
    def _parse_family_csv(raw: str | None) -> list[str]:
        out: list[str] = []
        for part in str(raw or "").split(","):
            token = str(part or "").strip().lower()
            if token and token not in out:
                out.append(token)
        return out

    @staticmethod
    def _family_from_bucket(row: dict | None) -> str:
        key = list((row or {}).get("key") or [])
        if not key:
            return ""
        return str(key[0] or "").strip().lower()

    def _allowed_xau_routing_families(self) -> list[str]:
        families = list(getattr(config, "get_persistent_canary_strategy_families", lambda: set())() or set())
        active = list(getattr(config, "get_ctrader_xau_active_families", lambda: set())() or set())
        experimental = list(getattr(config, "get_persistent_canary_experimental_families", lambda: set())() or set())
        direct = self._parse_family_csv(
            ",".join(
                [
                    str(getattr(config, "PERSISTENT_CANARY_STRATEGY_FAMILIES", "") or ""),
                    str(getattr(config, "CTRADER_XAU_ACTIVE_FAMILIES", "") or ""),
                    str(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES", "") or ""),
                    str(self._current_value("PERSISTENT_CANARY_STRATEGY_FAMILIES") or ""),
                    str(self._current_value("CTRADER_XAU_ACTIVE_FAMILIES") or ""),
                    str(self._current_value("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES") or ""),
                ]
            )
        )
        merged = self._parse_family_csv(",".join(list(families) + list(active) + list(experimental) + list(direct)))
        return merged or ["xau_scalp_pullback_limit", "xau_scalp_breakout_stop"]

    def _xau_swarm_active_families(self, allowed: list[str]) -> list[str]:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False)):
            return []
        configured = self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_SWARM_ACTIVE_FAMILIES", ""))
        return [fam for fam in configured if fam in allowed]

    @staticmethod
    def _config_family_csv(attr_name: str) -> list[str]:
        return TradingManagerAgent._parse_family_csv(str(getattr(config, attr_name, "") or ""))

    def _runtime_family_value(self, key: str, config_attr: str) -> str:
        configured = str(getattr(config, config_attr, "") or "").strip().lower()
        if configured:
            return configured
        return str(self._current_value(key) or "").strip().lower()

    def _runtime_float_value(self, key: str, config_attr: str, default: float) -> float:
        raw = str(self._current_value(key) or "").strip()
        if raw:
            try:
                return float(raw)
            except Exception:
                pass
        try:
            return float(getattr(config, config_attr, default) or default)
        except Exception:
            return float(default)

    @staticmethod
    def _reason_tag(prefix: str, value: str) -> str:
        token = neural_brain._normalize_reason_tag(str(value or ""))
        if not token:
            return ""
        return f"{str(prefix or '').strip().lower()}:{token}"

    @staticmethod
    def _dominant_row_value(rows: list[dict], key: str) -> str:
        counts: dict[str, int] = {}
        for row in list(rows or []):
            token = str((row or {}).get(key) or "").strip().lower()
            if not token or token == "unknown":
                continue
            counts[token] = int(counts.get(token, 0) or 0) + 1
        if not counts:
            return ""
        return sorted(counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]

    @staticmethod
    def _focus_symbols() -> list[str]:
        raw = str(getattr(config, "TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD") or "XAUUSD,BTCUSD,ETHUSD")
        out = []
        for part in raw.split(","):
            sym = _norm_symbol(part)
            if sym and sym not in out:
                out.append(sym)
        return out or ["XAUUSD", "BTCUSD", "ETHUSD"]

    @staticmethod
    def _tz_name() -> str:
        return str(getattr(config, "TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok") or "Asia/Bangkok").strip() or "Asia/Bangkok"

    @staticmethod
    def _safe_dt_iso(value) -> str:
        if isinstance(value, datetime):
            src = value
            if src.tzinfo is None:
                src = src.replace(tzinfo=timezone.utc)
            return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return _iso(_parse_iso_fallback(value) or _utc_now())

    def _window(self, *, hours: int) -> tuple[datetime, datetime, datetime, datetime]:
        tz_name = self._tz_name()
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        now_utc = _utc_now()
        now_local = now_utc.astimezone(tz)
        # Use a true rolling lookback window. Day-boundary truncation was
        # dropping valid rows from the last 24h and breaking manager stats.
        start_local = now_local - timedelta(hours=max(1, int(hours)))
        start_utc = start_local.astimezone(timezone.utc)
        return start_utc, now_utc, start_local, now_local

    def _load_reports(self) -> dict:
        names = [
            "mission_progress_report",
            "family_calibration_report",
            "winner_memory_library_report",
            "recent_win_cluster_memory_report",
            "ctrader_tick_depth_replay_report",
            "strategy_lab_report",
            "chart_state_memory_report",
            "ct_only_experiment_report",
        ]
        return {name: self._load_json(self._report_path(name)) for name in names}

    def _derive_xau_opportunity_sidecar_recommendation(self, *, chart_state_memory: dict, experiment_report: dict) -> dict:
        if not bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True)):
            return {}
        current_experimental = self._parse_family_csv(self._current_value("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES"))
        states = [
            dict(row)
            for row in list((chart_state_memory or {}).get("states") or [])
            if _norm_symbol(row.get("symbol")) == "XAUUSD"
            and str(row.get("direction") or "").strip().lower() == "short"
            and bool(row.get("follow_up_candidate"))
            and str(row.get("state_label") or "") in {"continuation_drive", "repricing_transition"}
        ]
        states.sort(
            key=lambda row: (
                float(row.get("state_score", 0.0) or 0.0),
                float(((row.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((row.get("stats") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        top_state = dict(states[0] if states else {})
        if not top_state:
            return {}
        if int(((top_state.get("stats") or {}).get("resolved", 0) or 0)) < max(2, int(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MIN_RESOLVED", 3) or 3)):
            return {}
        if float(top_state.get("state_score", 0.0) or 0.0) < float(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MIN_STATE_SCORE", 24.0) or 24.0):
            return {}
        source_rows = {str(row.get("source") or ""): dict(row) for row in list((experiment_report or {}).get("sources") or [])}
        fss_resolved = int((((source_rows.get("scalp_xauusd:fss:canary") or {}).get("closed_total") or {}).get("resolved", 0) or 0))
        if fss_resolved >= max(1, int(getattr(config, "XAU_FLOW_SHORT_SIDECAR_MAX_ROWS", 6) or 6)):
            return {}
        new_experimental = self._parse_family_csv(",".join(list(current_experimental) + ["xau_scalp_flow_short_sidecar"]))
        return {
            "active": True,
            "mode": "xau_short_flow_sidecar",
            "reason": f"short follow-up state {str(top_state.get('state_label') or '')} score {float(top_state.get('state_score', 0.0) or 0.0):.1f}",
            "changes": {"PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": ",".join(new_experimental)},
            "state": top_state,
        }

    def _derive_xau_reason_memory_recommendation(
        self,
        *,
        symbol_closed: list[dict],
        selected_family: str,
        best_family_today: dict,
        winner_memory_reference: dict,
    ) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_ENABLED", True))
        current_conf = self._runtime_float_value("NEURAL_GATE_CANARY_MIN_CONFIDENCE", "NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0)
        out = {
            "enabled": enabled,
            "status": "disabled" if not enabled else "unavailable",
            "active": False,
            "current_canary_min_confidence": round(current_conf, 3),
            "matched_context": [],
            "matched_tags": [],
            "family_scores": [],
            "avoid_families": [],
            "preferred_family": "",
            "reason": "",
        }
        if not enabled:
            return out

        lookback_days = max(1, int(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_LOOKBACK_DAYS", getattr(config, "NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS", 120)) or 120))
        min_resolved = max(1, int(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_MIN_RESOLVED", getattr(config, "NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED", 8)) or 8))
        min_matched_tags = max(1, int(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_MIN_MATCHED_TAGS", 2) or 2))
        min_abs_score = abs(float(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_MIN_ABS_SCORE", 0.10) or 0.10))
        score_mult = max(0.0, float(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_CONFIDENCE_SCORE_MULT", 4.0) or 4.0))
        max_abs_delta = max(0.1, float(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_MAX_ABS_DELTA", 2.5) or 2.5))
        conf_floor = float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", 68.0) or 68.0)
        conf_cap = float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", 80.0) or 80.0)

        try:
            report = dict(neural_brain.build_reason_study_report(days=lookback_days, min_resolved=min_resolved) or {})
        except Exception as exc:
            out["reason"] = f"reason study unavailable: {exc}"
            return out
        if not bool(report.get("ok")):
            out["reason"] = str(report.get("message") or report.get("status") or "reason_study_unavailable")
            return out

        tag_index = dict(report.get("tag_index") or {})
        current_primary = self._runtime_family_value("CTRADER_XAU_PRIMARY_FAMILY", "CTRADER_XAU_PRIMARY_FAMILY")
        dominant_session = self._dominant_row_value(symbol_closed, "session")
        dominant_entry = self._dominant_row_value(symbol_closed, "entry_type")
        dominant_pattern = self._dominant_row_value(symbol_closed, "pattern")

        matched_context: list[str] = []
        seen_tags: set[str] = set()

        def _append_context(prefix: str, value: str) -> None:
            tag = self._reason_tag(prefix, value)
            if tag and tag not in seen_tags:
                seen_tags.add(tag)
                matched_context.append(tag)

        _append_context("symbol", "XAUUSD")
        _append_context("family", current_primary)
        _append_context("family", selected_family)
        _append_context("family", str((best_family_today.get("key") or [""])[0] or ""))
        _append_context("family", str((winner_memory_reference or {}).get("family") or ""))
        _append_context("session", dominant_session)
        _append_context("entry", dominant_entry)
        _append_context("pattern", dominant_pattern)

        matched_rows: list[dict] = []
        for tag in matched_context:
            row = dict(tag_index.get(tag) or {})
            if row and bool(row.get("eligible")):
                matched_rows.append(row)

        family_scores: list[dict] = []
        for family in self._allowed_xau_routing_families():
            row = dict(tag_index.get(self._reason_tag("family", family)) or {})
            if row and bool(row.get("eligible")):
                family_scores.append(
                    {
                        "family": family,
                        "score": round(float(row.get("score", 0.0) or 0.0), 4),
                        "resolved": int(row.get("resolved", 0) or 0),
                        "win_rate": round(float(row.get("win_rate", 0.0) or 0.0), 4),
                        "avg_r": round(float(row.get("avg_r", 0.0) or 0.0), 4),
                    }
                )
        family_scores.sort(key=lambda item: (float(item.get("score", 0.0) or 0.0), int(item.get("resolved", 0) or 0)), reverse=True)
        out["matched_context"] = matched_context
        out["matched_tags"] = sorted(
            [dict(row) for row in matched_rows],
            key=lambda item: (abs(float(item.get("score", 0.0) or 0.0)), int(item.get("resolved", 0) or 0)),
            reverse=True,
        )[:6]
        out["family_scores"] = family_scores[:6]
        out["preferred_family"] = str((family_scores[0] if family_scores and float(family_scores[0].get("score", 0.0) or 0.0) >= min_abs_score else {}).get("family") or "")
        out["avoid_families"] = [
            str(item.get("family") or "")
            for item in list(family_scores or [])
            if float(item.get("score", 0.0) or 0.0) <= -min_abs_score
        ][:3]
        current_primary_score = 0.0
        if current_primary:
            current_row = dict(tag_index.get(self._reason_tag("family", current_primary)) or {})
            if current_row:
                current_primary_score = float(current_row.get("score", 0.0) or 0.0)
        out["current_primary_family"] = current_primary
        out["current_primary_family_score"] = round(current_primary_score, 4)
        out["report_days"] = int(report.get("days", 0) or 0)
        out["report_resolved_rows"] = int(report.get("resolved_rows", 0) or 0)

        if len(matched_rows) < min_matched_tags:
            out["status"] = "insufficient_context"
            out["reason"] = f"reason memory matched {len(matched_rows)}/{min_matched_tags} eligible tags"
            return out

        weighted_sum = 0.0
        weight_sum = 0.0
        for row in matched_rows:
            resolved = max(1.0, float(row.get("resolved", 0) or 0))
            weight = resolved ** 0.5
            weighted_sum += float(row.get("score", 0.0) or 0.0) * weight
            weight_sum += weight
        avg_score = (weighted_sum / weight_sum) if weight_sum > 0 else 0.0
        out["status"] = "monitoring"
        out["matched_count"] = len(matched_rows)
        out["avg_score"] = round(avg_score, 4)

        if abs(avg_score) < min_abs_score:
            out["reason"] = f"reason memory avg {avg_score:+.2f} below trigger {min_abs_score:.2f}"
            return out

        raw_delta = max(-max_abs_delta, min(max_abs_delta, -avg_score * score_mult))
        proposed_conf = max(conf_floor, min(conf_cap, current_conf + raw_delta))
        delta = round(proposed_conf - current_conf, 3)
        if abs(delta) < 0.05:
            out["status"] = "clamped"
            out["reason"] = "reason memory change clamped by current bounds"
            return out

        top_tags = ", ".join(
            f"{str(item.get('tag') or '')} {float(item.get('score', 0.0) or 0.0):+.2f}"
            for item in list(out["matched_tags"] or [])[:3]
        )
        reason = f"reason memory avg {avg_score:+.2f} on {len(matched_rows)} tags"
        if top_tags:
            reason = f"{reason} | {top_tags}"
        out.update(
            {
                "active": True,
                "status": "active",
                "mode": "tighten_canary_from_reason_memory" if delta > 0 else "relax_canary_from_reason_memory",
                "delta": delta,
                "proposed_canary_min_confidence": round(proposed_conf, 3),
                "changes": {
                    "NEURAL_GATE_CANARY_MIN_CONFIDENCE": f"{proposed_conf:.1f}".rstrip("0").rstrip("."),
                },
                "reason": reason,
            }
        )
        return out

    def _apply_reason_memory_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_ENABLED", True))
        persist = bool(getattr(config, "TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_PERSIST_ENV", True))
        out = {"enabled": enabled, "persist_env": persist, "status": "disabled" if not enabled else "none", "applied": {}, "reverted": {}, "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_reason_memory") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("reason_memory_recommendations") or {})
        if rec.get("active"):
            originals = dict(active.get("originals") or {})
            applied = {}
            for key, new_value in dict(rec.get("changes") or {}).items():
                old_value = self._current_value(str(key))
                originals.setdefault(str(key), str(old_value))
                if str(old_value) == str(new_value):
                    continue
                self._apply_runtime_value(str(key), str(new_value))
                applied[str(key)] = self._persist_env_value(str(key), str(new_value), persist)
            state["xau_reason_memory"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "applied_at": _iso(_utc_now()),
                "originals": originals,
                "changes": dict(rec.get("changes") or {}),
                "matched_tags": list(rec.get("matched_tags") or []),
                "avg_score": float(rec.get("avg_score", 0.0) or 0.0),
            }
            self._save_state(state)
            out.update({"status": "applied" if applied else "already_active", "applied": applied, "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            reverted = {}
            for key, old_value in dict(active.get("originals") or {}).items():
                self._apply_runtime_value(str(key), str(old_value))
                reverted[str(key)] = self._persist_env_value(str(key), str(old_value), persist)
            state["xau_reason_memory"] = {"status": "inactive", "reverted_at": _iso(_utc_now()), "originals": dict(active.get("originals") or {})}
            self._save_state(state)
            out.update({"status": "reverted", "reverted": reverted, "reason": "reason_memory_clear"})
        return out

    def _apply_opportunity_sidecar_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        persist = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", True))
        out = {"enabled": enabled, "persist_env": persist, "status": "disabled" if not enabled else "none", "applied": {}, "reverted": {}, "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_opportunity_sidecar") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("opportunity_sidecar_recommendations") or {})
        if rec.get("active"):
            originals = dict(active.get("originals") or {})
            applied = {}
            for key, new_value in dict(rec.get("changes") or {}).items():
                old_value = self._current_value(str(key))
                originals.setdefault(str(key), str(old_value))
                if str(old_value) == str(new_value):
                    continue
                self._apply_runtime_value(str(key), str(new_value))
                applied[str(key)] = self._persist_env_value(str(key), str(new_value), persist)
            state["xau_opportunity_sidecar"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "applied_at": _iso(_utc_now()),
                "originals": originals,
                "changes": dict(rec.get("changes") or {}),
            }
            self._save_state(state)
            out.update({"status": "applied" if applied else "already_active", "applied": applied, "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            reverted = {}
            for key, old_value in dict(active.get("originals") or {}).items():
                self._apply_runtime_value(str(key), str(old_value))
                reverted[str(key)] = self._persist_env_value(str(key), str(old_value), persist)
            state["xau_opportunity_sidecar"] = {"status": "inactive", "reverted_at": _iso(_utc_now()), "originals": dict(active.get("originals") or {})}
            self._save_state(state)
            out.update({"status": "reverted", "reverted": reverted, "reason": "opportunity_sidecar_clear"})
        return out

    def _derive_xau_parallel_family_recommendation(self, *, chart_state_memory: dict) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ENABLED", True)):
            return {}
        allowed = [fam for fam in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ALLOWED", "")) if fam]
        for fam in self._xau_swarm_active_families(self._allowed_xau_routing_families()):
            if fam and fam not in allowed:
                allowed.append(fam)
        if not allowed:
            return {}
        states = [
            dict(row)
            for row in list((chart_state_memory or {}).get("states") or [])
            if _norm_symbol(row.get("symbol")) == "XAUUSD"
            and bool(row.get("follow_up_candidate"))
        ]
        states.sort(
            key=lambda row: (
                float(row.get("state_score", 0.0) or 0.0),
                float(((row.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((row.get("stats") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        top_state = dict(states[0] if states else {})
        if not top_state:
            return {}
        if float(top_state.get("state_score", 0.0) or 0.0) < float(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MIN_STATE_SCORE", 24.0) or 24.0):
            return {}
        slot_budget = self._derive_xau_slot_budget(top_state)
        default_slots = max(
            2,
            int(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION", 3) or 3),
        )
        return {
            "active": True,
            "mode": "state_parallel_same_direction",
            "reason": (
                f"state {str(top_state.get('state_label') or '')} score {float(top_state.get('state_score', 0.0) or 0.0):.1f}"
                f" | {str(slot_budget.get('reason') or '')}"
            ),
            "allowed_families": allowed,
            "max_same_direction_families": int(
                slot_budget.get("max_same_direction_families", default_slots) or default_slots
            ),
            "slot_budget": slot_budget,
            "state": top_state,
        }

    def _apply_parallel_family_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": "", "applied": {}}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_parallel_families") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("parallel_family_recommendations") or {})
        if rec.get("active"):
            state["xau_parallel_families"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "allowed_families": list(rec.get("allowed_families") or []),
                "max_same_direction_families": int(rec.get("max_same_direction_families", 3) or 3),
                "slot_budget": dict(rec.get("slot_budget") or {}),
                "applied_at": _iso(_utc_now()),
            }
            self._save_state(state)
            out.update({"status": "applied" if not active or active != state["xau_parallel_families"] else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_parallel_families"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "parallel_family_clear"})
        return out

    def _derive_xau_hedge_lane_recommendation(self, *, chart_state_memory: dict, experiment_report: dict) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_ENABLED", True)):
            return {}
        allowed = [fam for fam in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_ALLOWED_FAMILIES", "")) if fam]
        if not allowed:
            return {}
        states = [
            dict(row)
            for row in list((chart_state_memory or {}).get("states") or [])
            if _norm_symbol(row.get("symbol")) == "XAUUSD"
            and bool(row.get("follow_up_candidate"))
            and str(row.get("direction") or "").strip().lower() == "short"
            and str(row.get("state_label") or "") in {"continuation_drive", "repricing_transition"}
        ]
        states.sort(
            key=lambda row: (
                float(row.get("state_score", 0.0) or 0.0),
                float(((row.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((row.get("stats") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        top_state = dict(states[0] if states else {})
        if not top_state:
            return {}
        if float(top_state.get("state_score", 0.0) or 0.0) < float(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_MIN_STATE_SCORE", 26.0) or 26.0):
            return {}
        source_rows = {str(row.get("source") or ""): dict(row) for row in list((experiment_report or {}).get("sources") or [])}
        current_rows = int((((source_rows.get("scalp_xauusd:ff:canary") or {}).get("closed_total") or {}).get("resolved", 0) or 0))
        current_rows += int((((source_rows.get("scalp_xauusd:fss:canary") or {}).get("closed_total") or {}).get("resolved", 0) or 0))
        return {
            "active": True,
            "mode": "xau_manager_hedge_transition",
            "reason": f"short transition state {str(top_state.get('state_label') or '')} score {float(top_state.get('state_score', 0.0) or 0.0):.1f}",
            "allowed_families": allowed,
            "max_per_symbol": max(1, int(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_MAX_PER_SYMBOL", 1) or 1)),
            "risk_multiplier": max(0.10, min(1.0, float(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_RISK_MULTIPLIER", 0.65) or 0.65))),
            "resolved_rows": current_rows,
            "state": top_state,
        }

    def _apply_hedge_lane_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": "", "applied": {}}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_hedge_transition") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("hedge_lane_recommendations") or {})
        if rec.get("active"):
            state["xau_hedge_transition"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "allowed_families": list(rec.get("allowed_families") or []),
                "max_per_symbol": int(rec.get("max_per_symbol", 1) or 1),
                "risk_multiplier": float(rec.get("risk_multiplier", 1.0) or 1.0),
                "applied_at": _iso(_utc_now()),
            }
            self._save_state(state)
            out.update({"status": "applied" if not active or active != state["xau_hedge_transition"] else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_hedge_transition"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "hedge_lane_clear"})
        return out

    def _derive_xau_opportunity_bypass_recommendation(
        self,
        *,
        open_positions: list[dict],
        recent_order_reviews: list[dict],
        chart_state_memory: dict,
    ) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ENABLED", False)):
            return {}
        allowed = [fam for fam in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ALLOWED_FAMILIES", "")) if fam]
        if not allowed:
            return {}
        open_longs = [
            dict(row)
            for row in list(open_positions or [])
            if str(row.get("direction") or "").strip().lower() in {"buy", "long"}
        ]
        if not open_longs:
            return {}
        states = [
            dict(row)
            for row in list((chart_state_memory or {}).get("states") or [])
            if _norm_symbol(row.get("symbol")) == "XAUUSD"
            and bool(row.get("follow_up_candidate"))
            and str(row.get("direction") or "").strip().lower() == "short"
            and str(row.get("state_label") or "") in {"continuation_drive", "repricing_transition", "failed_fade_risk"}
        ]
        states.sort(
            key=lambda row: (
                float(row.get("state_score", 0.0) or 0.0),
                float(((row.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((row.get("stats") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        top_state = dict(states[0] if states else {})
        if not top_state:
            return {}
        min_state_score = float(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_STATE_SCORE", 28.0) or 28.0)
        if float(top_state.get("state_score", 0.0) or 0.0) < min_state_score:
            return {}
        vulnerable_reviews = 0
        vulnerable_sources: set[str] = set()
        for row in list(recent_order_reviews or []):
            pnl = float(row.get("pnl_usd", 0.0) or 0.0)
            direction = str(row.get("direction") or "").strip().lower()
            diagnosis = str(row.get("diagnosis") or "").strip().lower()
            if direction != "long":
                continue
            if pnl < 0.0 and (
                "failed to extend after entry" in diagnosis
                or "insufficient follow-through after fill" in diagnosis
                or "paid up before retest confirmation" in diagnosis
                or "filled before the retest showed absorption" in diagnosis
            ):
                vulnerable_reviews += 1
                source = str(row.get("source") or "").strip().lower()
                if source:
                    vulnerable_sources.add(source)
        min_vulnerable = max(1, int(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_VULNERABLE_REVIEWS", 1) or 1))
        if vulnerable_reviews < min_vulnerable:
            return {}
        return {
            "active": True,
            "mode": "xau_opportunity_bypass",
            "reason": (
                f"open long exposure={len(open_longs)} with {vulnerable_reviews} vulnerable long reviews; "
                f"short state {str(top_state.get('state_label') or '')} score {float(top_state.get('state_score', 0.0) or 0.0):.1f}"
            ),
            "allowed_families": allowed,
            "max_per_symbol": max(1, int(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL", 2) or 2)),
            "risk_multiplier": max(0.10, min(1.0, float(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER", 0.55) or 0.55))),
            "vulnerable_review_count": vulnerable_reviews,
            "vulnerable_sources": sorted(vulnerable_sources),
            "state": top_state,
        }

    def _apply_xau_opportunity_bypass_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": "", "applied": {}}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_opportunity_bypass") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("opportunity_bypass_recommendations") or {})
        if rec.get("active"):
            new_state = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "allowed_families": list(rec.get("allowed_families") or []),
                "max_per_symbol": int(rec.get("max_per_symbol", 1) or 1),
                "risk_multiplier": float(rec.get("risk_multiplier", 1.0) or 1.0),
                "vulnerable_review_count": int(rec.get("vulnerable_review_count", 0) or 0),
                "vulnerable_sources": list(rec.get("vulnerable_sources") or []),
                "applied_at": _iso(_utc_now()),
                "state": dict(rec.get("state") or {}),
            }
            state["xau_opportunity_bypass"] = new_state
            self._save_state(state)
            out.update({"status": "applied" if active != new_state else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_opportunity_bypass"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "opportunity_bypass_clear"})
        return out

    @staticmethod
    def _order_care_profile(mode: str, *, desk: str = "") -> dict:
        token = str(mode or "").strip().lower()
        desk_token = str(desk or "").strip().lower()
        if desk_token == "range_repair":
            return {
                "min_age_min": 0.35,
                "tighten_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TIGHTEN_SCORE", 2) or 2)),
                "close_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_SCORE", 4) or 4)),
                "close_max_r": min(0.06, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_MAX_R", 0.10) or 0.10)),
                "stop_keep_r": min(0.24, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_STOP_KEEP_R", 0.28) or 0.28)),
                "profit_lock_r": min(0.02, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_PROFIT_LOCK_R", 0.02) or 0.02)),
                "trim_tp_r": min(0.24, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TRIM_TP_R", 0.36) or 0.36)),
                "no_follow_age_min": min(1.5, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_AGE_MIN", 5.0) or 5.0)),
                "no_follow_max_r": min(0.01, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_MAX_R", 0.02) or 0.02)),
                "be_trigger_r": min(0.06, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_TRIGGER_R", 0.10) or 0.10)),
                "be_lock_r": min(0.005, float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_LOCK_R", 0.01) or 0.01)),
                "extension_min_age_min": 99.0,
                "extension_min_confidence": 101.0,
                "extension_score": 99,
                "desk": "range_repair",
            }
        if token == "retest_absorption_guard":
            profile = {
                "min_age_min": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_AGE_MIN", 5.0) or 5.0),
                "tighten_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TIGHTEN_SCORE", 2) or 2)),
                "close_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_SCORE", 4) or 4)),
                "close_max_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_CLOSE_MAX_R", 0.10) or 0.10),
                "stop_keep_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_STOP_KEEP_R", 0.28) or 0.28),
                "profit_lock_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_PROFIT_LOCK_R", 0.02) or 0.02),
                "trim_tp_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_TRIM_TP_R", 0.36) or 0.36),
                "no_follow_age_min": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_AGE_MIN", 5.0) or 5.0),
                "no_follow_max_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_NO_FOLLOW_MAX_R", 0.02) or 0.02),
                "be_trigger_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_TRIGGER_R", 0.10) or 0.10),
                "be_lock_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RETEST_BE_LOCK_R", 0.01) or 0.01),
            }
            profile["desk"] = desk_token or "limit_retest"
            return profile
        profile = {
            "min_age_min": 1.0,
            "tighten_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TIGHTEN_SCORE", 2) or 2)),
            "close_score": max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_SCORE", 4) or 4)),
            "close_max_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_CLOSE_MAX_R", 0.12) or 0.12),
            "stop_keep_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_STOP_KEEP_R", 0.30) or 0.30),
            "profit_lock_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_PROFIT_LOCK_R", 0.03) or 0.03),
            "trim_tp_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_TRIM_TP_R", 0.40) or 0.40),
            "no_follow_age_min": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_AGE_MIN", 6.0) or 6.0),
            "no_follow_max_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_MAX_R", 0.03) or 0.03),
            "be_trigger_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_TRIGGER_R", 0.12) or 0.12),
            "be_lock_r": float(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_BE_LOCK_R", 0.01) or 0.01),
        }
        if desk_token == "limit_retest":
            profile["min_age_min"] = min(float(profile.get("min_age_min", 1.0) or 1.0), 0.75)
            profile["close_max_r"] = min(float(profile.get("close_max_r", 0.12) or 0.12), 0.08)
            profile["trim_tp_r"] = min(float(profile.get("trim_tp_r", 0.40) or 0.40), 0.30)
            profile["no_follow_age_min"] = min(float(profile.get("no_follow_age_min", 6.0) or 6.0), 3.0)
            profile["no_follow_max_r"] = min(float(profile.get("no_follow_max_r", 0.03) or 0.03), 0.015)
            profile["be_trigger_r"] = min(float(profile.get("be_trigger_r", 0.12) or 0.12), 0.08)
            profile["extension_min_age_min"] = max(0.15, float(getattr(config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15) or 0.15))
            profile["extension_min_confidence"] = max(76.0, float(profile.get("extension_min_confidence", 76.0) or 76.0))
            profile["extension_score"] = max(6, int(profile.get("extension_score", 6) or 6))
            profile["desk"] = "limit_retest"
            return profile
        profile["extension_min_age_min"] = max(0.15, float(getattr(config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15) or 0.15))
        profile["desk"] = desk_token or "fss_confirmation"
        return profile

    def _xau_order_care_desk_sources(self) -> dict[str, list[str]]:
        fss_sources = self._parse_family_csv(
            getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_FSS_ALLOWED_SOURCES", "scalp_xauusd:fss:canary")
        )
        limit_sources = self._parse_family_csv(
            getattr(
                config,
                "TRADING_MANAGER_XAU_ORDER_CARE_LIMIT_RETEST_ALLOWED_SOURCES",
                "xauusd_scheduled:canary,scalp_xauusd:canary,scalp_xauusd,scalp_xauusd:winner,scalp_xauusd:pb:canary,scalp_xauusd:td:canary,scalp_xauusd:ff:canary,scalp_xauusd:mfu:canary",
            )
        )
        return {
            "fss_confirmation": fss_sources or ["scalp_xauusd:fss:canary"],
            "limit_retest": limit_sources
            or [
                "xauusd_scheduled:canary",
                "scalp_xauusd:canary",
                "scalp_xauusd",
                "scalp_xauusd:winner",
                "scalp_xauusd:pb:canary",
                "scalp_xauusd:td:canary",
                "scalp_xauusd:ff:canary",
                "scalp_xauusd:mfu:canary",
            ],
            "range_repair": self._parse_family_csv(
                getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RANGE_REPAIR_ALLOWED_SOURCES", "scalp_xauusd:rr:canary")
            )
            or ["scalp_xauusd:rr:canary"],
        }
    def _derive_xau_order_care_recommendation(self, *, recent_order_reviews: list[dict]) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_ENABLED", True)):
            return {}
        review_limit = max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_RECENT_REVIEW_COUNT", 5) or 5))
        min_losses = max(1, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_MIN_LOSSES", 2) or 2))
        reviews = [dict(row) for row in list(recent_order_reviews or [])[:review_limit] if isinstance(row, dict)]
        losers = [row for row in reviews if float(row.get("pnl_usd", 0.0) or 0.0) < 0.0]
        if len(losers) < min_losses:
            return {}
        mode = ""
        reason = ""
        diag_counts = {
            "market_entry_retest_guard": 0,
            "retest_absorption_guard": 0,
            "continuation_fail_fast": 0,
        }
        for row in losers:
            diag_text = str(row.get("diagnosis") or "").lower()
            if "scheduled market entry paid up before retest confirmation" in diag_text:
                diag_counts["market_entry_retest_guard"] += 1
            if "pullback limit was filled before the retest showed absorption" in diag_text:
                diag_counts["retest_absorption_guard"] += 1
            if (
                "continuation failed to extend after entry" in diag_text
                or "insufficient follow-through after fill" in diag_text
            ):
                diag_counts["continuation_fail_fast"] += 1
        if diag_counts["market_entry_retest_guard"] >= min_losses:
            mode = "market_entry_retest_guard"
            reason = "recent scheduled canary losses paid up before retest confirmation"
        elif diag_counts["retest_absorption_guard"] >= min_losses:
            mode = "retest_absorption_guard"
            reason = "recent pullback losses filled before absorption confirmed"
        elif diag_counts["continuation_fail_fast"] >= min_losses:
            mode = "continuation_fail_fast"
            reason = "recent continuation trades failed to extend after entry"
        if not mode:
            return {}
        desk_sources = self._xau_order_care_desk_sources()
        allowed_sources = self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_ALLOWED_SOURCES", ""))
        default_sources = list(desk_sources.get("limit_retest") or []) + list(desk_sources.get("fss_confirmation") or [])
        if not allowed_sources:
            allowed_sources = list(default_sources)
        else:
            seen = {str(token or "").strip().lower() for token in allowed_sources if str(token or "").strip()}
            for token in default_sources:
                key = str(token or "").strip().lower()
                if key and key not in seen:
                    allowed_sources.append(token)
                    seen.add(key)
        desks = {
            "fss_confirmation": {
                "status": "active",
                "mode": "continuation_fail_fast",
                "allowed_sources": list(desk_sources.get("fss_confirmation") or []),
                "overrides": {
                    "allowed_sources": list(desk_sources.get("fss_confirmation") or []),
                    **self._order_care_profile("continuation_fail_fast", desk="fss_confirmation"),
                },
            },
            "limit_retest": {
                "status": "active",
                "mode": "retest_absorption_guard" if mode == "continuation_fail_fast" else mode,
                "allowed_sources": list(desk_sources.get("limit_retest") or []),
                "overrides": {
                    "allowed_sources": list(desk_sources.get("limit_retest") or []),
                    **self._order_care_profile(
                        "retest_absorption_guard" if mode == "continuation_fail_fast" else mode,
                        desk="limit_retest",
                    ),
                },
            },
            "range_repair": {
                "status": "active",
                "mode": "retest_absorption_guard",
                "allowed_sources": list(desk_sources.get("range_repair") or []),
                "overrides": {
                    "allowed_sources": list(desk_sources.get("range_repair") or []),
                    **self._order_care_profile("retest_absorption_guard", desk="range_repair"),
                },
            },
        }
        profile = self._order_care_profile(mode, desk="shared")
        return {
            "active": True,
            "mode": mode,
            "reason": reason,
            "allowed_sources": allowed_sources,
            "review_window": reviews,
            "loss_count": len(losers),
            "desks": desks,
            "overrides": {
                "allowed_sources": allowed_sources,
                **profile,
            },
        }

    def _apply_xau_order_care_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_order_care") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("order_care_recommendations") or {})
        if rec.get("active"):
            new_state = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "allowed_sources": list(rec.get("allowed_sources") or []),
                "loss_count": int(rec.get("loss_count", 0) or 0),
                "review_window": list(rec.get("review_window") or []),
                "desks": dict(rec.get("desks") or {}),
                "overrides": dict(rec.get("overrides") or {}),
                "applied_at": _iso(_utc_now()),
            }
            state["xau_order_care"] = new_state
            self._save_state(state)
            out.update({"status": "applied" if active != new_state else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            min_active_min = max(0, int(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_MIN_ACTIVE_MIN", 45) or 45))
            active_since = _parse_iso_fallback(active.get("applied_at"))
            if min_active_min > 0 and active_since is not None:
                active_age_min = max(0.0, (_utc_now() - active_since).total_seconds() / 60.0)
                if active_age_min < float(min_active_min):
                    out.update({"status": "held", "reason": f"xau_order_care_min_active_window:{round(active_age_min, 1)}<{min_active_min}"})
                    return out
            state["xau_order_care"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "xau_order_care_clear"})
        return out

    def _apply_xau_micro_regime_refresh(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        refresh = dict(xau.get("micro_regime_refresh") or {})
        if bool(refresh.get("active")):
            state["xau_micro_regime"] = {
                "status": "active",
                "state_label": str(refresh.get("state_label") or ""),
                "dominant_direction": str(refresh.get("dominant_direction") or ""),
                "window_min": int(refresh.get("window_min", 0) or 0),
                "min_resolved": int(refresh.get("min_resolved", 0) or 0),
                "dominant_bucket": dict(refresh.get("dominant_bucket") or {}),
                "applied_at": _iso(_utc_now()),
            }
            self._save_state(state)
            out.update({"status": "applied", "reason": str(refresh.get("state_label") or "")})
            return out
        if state.get("xau_micro_regime"):
            state["xau_micro_regime"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "xau_micro_regime_clear"})
        return out

    def _apply_xau_cluster_loss_guard_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_cluster_loss_guard") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("cluster_loss_guard_recommendations") or {})
        if rec.get("active"):
            new_state = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "blocked_direction": str(rec.get("blocked_direction") or ""),
                "window_min": int(rec.get("window_min", 0) or 0),
                "losses": int(rec.get("losses", 0) or 0),
                "resolved": int(rec.get("resolved", 0) or 0),
                "pnl_usd": float(rec.get("pnl_usd", 0.0) or 0.0),
                "families": list(rec.get("families") or []),
                "applied_at": _iso(_utc_now()),
            }
            state["xau_cluster_loss_guard"] = new_state
            self._save_state(state)
            out.update({"status": "applied" if active != new_state else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_cluster_loss_guard"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "xau_cluster_loss_guard_clear"})
        return out

    def _apply_xau_regime_transition_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True)) and bool(
            getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED", True)
        )
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_regime_transition") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("regime_transition_recommendations") or {})
        if bool(rec.get("active")):
            new_state = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "support_state": str(rec.get("support_state") or ""),
                "current_side": str(rec.get("current_side") or ""),
                "state_label": str(rec.get("state_label") or ""),
                "opposite_state_label": str(rec.get("opposite_state_label") or ""),
                "day_type": str(rec.get("day_type") or ""),
                "follow_up_plan": str(rec.get("follow_up_plan") or ""),
                "blocked_direction": str(rec.get("blocked_direction") or ""),
                "blocked_entry_types": list(rec.get("blocked_entry_types") or []),
                "blocked_families": list(rec.get("blocked_families") or []),
                "blocked_sources": list(rec.get("blocked_sources") or []),
                "preferred_families": list(rec.get("preferred_families") or []),
                "preferred_sources": list(rec.get("preferred_sources") or []),
                "snapshot_run_id": str(rec.get("snapshot_run_id") or ""),
                "snapshot_last_event_utc": str(rec.get("snapshot_last_event_utc") or ""),
                "snapshot_features": dict(rec.get("snapshot_features") or {}),
                "pressure": dict(rec.get("pressure") or {}),
                "hold_min": int(rec.get("hold_min", 0) or 0),
                "remaining_min": float(rec.get("remaining_min", 0.0) or 0.0),
                "hold_until_utc": str(rec.get("hold_until_utc") or ""),
                "applied_at": _iso(_utc_now()),
            }
            state["xau_regime_transition"] = new_state
            self._save_state(state)
            out.update({"status": "applied" if active != new_state else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        hold_until = _parse_iso_fallback(active.get("hold_until_utc")) if active else None
        if active and str(active.get("status") or "") == "active" and hold_until and hold_until > _utc_now():
            remain_min = round(max(0.0, (hold_until - _utc_now()).total_seconds() / 60.0), 1)
            active["remaining_min"] = remain_min
            state["xau_regime_transition"] = active
            self._save_state(state)
            out.update({"status": "held", "reason": f"xau_regime_transition_hold:{remain_min}m"})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_regime_transition"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "xau_regime_transition_clear"})
        return out

    def _apply_xau_execution_directive_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True)) and bool(
            getattr(config, "TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED", True)
        )
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_execution_directive") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("execution_directive_recommendations") or {})
        if bool(rec.get("active")):
            new_state = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "blocked_direction": str(rec.get("blocked_direction") or ""),
                "blocked_entry_types": list(rec.get("blocked_entry_types") or []),
                "blocked_families": list(rec.get("blocked_families") or []),
                "blocked_sources": list(rec.get("blocked_sources") or []),
                "preferred_families": list(rec.get("preferred_families") or []),
                "preferred_sources": list(rec.get("preferred_sources") or []),
                "support_state": str(rec.get("support_state") or ""),
                "trigger_run_id": str(rec.get("trigger_run_id") or ""),
                "pause_min": int(rec.get("pause_min", 0) or 0),
                "remaining_min": float(rec.get("remaining_min", 0.0) or 0.0),
                "pause_until_utc": str(rec.get("pause_until_utc") or ""),
                "pair_risk_cap": dict(rec.get("pair_risk_cap") or {}),
                "coach_traders": list(rec.get("coach_traders") or []),
                "trader_assignments": list(rec.get("trader_assignments") or []),
                "applied_at": _iso(_utc_now()),
            }
            state["xau_execution_directive"] = new_state
            self._save_state(state)
            out.update({"status": "applied" if active != new_state else "already_active", "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            state["xau_execution_directive"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
            self._save_state(state)
            out.update({"status": "reverted", "reason": "xau_execution_directive_clear"})
        return out

    def _collect_closed_rows(self, *, since_iso: str, symbols: list[str]) -> tuple[list[dict], dict]:
        out: list[dict] = []
        summary = {"rows_seen": 0, "abnormal_excluded": 0}
        if not self.ctrader_db_path.exists():
            return out, summary
        with closing(self._connect_ctrader()) as conn:
            rows = conn.execute(
                """
                SELECT d.execution_utc, d.source AS deal_source, COALESCE(d.symbol, '') AS deal_symbol, d.outcome, d.pnl_usd,
                       d.raw_json AS deal_raw_json,
                       j.source AS journal_source, COALESCE(j.symbol, '') AS journal_symbol, j.direction, j.confidence,
                       j.entry, j.stop_loss, j.take_profit, j.entry_type, j.request_json, j.created_utc, j.execution_meta_json
                  FROM ctrader_deals d
                  LEFT JOIN execution_journal j ON j.id = d.journal_id
                 WHERE d.execution_utc >= ?
                   AND d.has_close_detail = 1
                   AND d.outcome IN (0, 1)
                   AND COALESCE(j.symbol, d.symbol, '') IN ({placeholders})
                 ORDER BY d.execution_utc ASC, d.deal_id ASC
                """.replace("{placeholders}", ",".join("?" for _ in symbols)),
                [since_iso, *symbols],
            ).fetchall()
            summary["rows_seen"] = len(list(rows or []))
            for raw in list(rows or []):
                symbol = _norm_symbol(raw["journal_symbol"] or raw["deal_symbol"] or "")
                source = _norm_source(raw["journal_source"] or raw["deal_source"] or "")
                family = live_profile_autopilot._strategy_family_for_source(symbol, source)
                if (not symbol) or (not source) or (not family):
                    continue
                ctx = _extract_request_context(str(raw["request_json"] or "{}"))
                direction = str(raw["direction"] or (ctx.get("payload") or {}).get("direction") or "").strip().lower()
                direction = "long" if direction in {"buy", "long"} else "short" if direction in {"sell", "short"} else direction or "unknown"
                abnormal = _classify_trade_abnormality(
                    direction,
                    _actual_entry_from_deal(_safe_json_dict(str(raw["deal_raw_json"] or "{}"))),
                    _safe_float(raw["stop_loss"], 0.0),
                    _safe_float(raw["take_profit"], 0.0),
                    execution_meta=str(raw["execution_meta_json"] or "{}"),
                )
                if bool(abnormal.get("exclude_from_learning")):
                    summary["abnormal_excluded"] += 1
                    continue
                raw_scores = dict((ctx.get("raw_scores") or {}))
                out.append(
                    {
                        "closed_utc": str(raw["execution_utc"] or ""),
                        "created_utc": str(raw["created_utc"] or raw["execution_utc"] or ""),
                        "symbol": symbol,
                        "source": source,
                        "family": family,
                        "direction": direction,
                        "confidence": _safe_float(raw["confidence"], _safe_float((ctx.get("payload") or {}).get("confidence"), 0.0)),
                        "entry_type": str((raw["entry_type"] or ctx.get("entry_type") or "unknown")).strip().lower(),
                        "pnl_usd": _safe_float(raw["pnl_usd"], 0.0),
                        "outcome": _safe_int(raw["outcome"], -1),
                        "session": _norm_signature(str(ctx.get("session") or "unknown")) or "unknown",
                        "timeframe": str(ctx.get("timeframe") or "unknown"),
                        "pattern": str(ctx.get("pattern") or "unknown"),
                        "h1_trend": str(raw_scores.get("scalp_force_trend_h1") or raw_scores.get("trend_h1") or "unknown").strip().lower() or "unknown",
                        "abnormal_flags": list(abnormal.get("flags") or []),
                    }
                )
        return out, summary

    def _detect_shock(self, conn: sqlite3.Connection, *, symbol: str, since_iso: str) -> dict:
        window_min = max(5, int(getattr(config, "TRADING_MANAGER_EVENT_WINDOW_MIN", 20) or 20))
        min_drop_pct = abs(float(getattr(config, "TRADING_MANAGER_EVENT_MIN_DROP_PCT", 0.30) or 0.30))
        spot_rows = conn.execute(
            """
            SELECT event_utc, event_ts, bid, ask, spread_pct
              FROM ctrader_spot_ticks
             WHERE symbol = ?
               AND event_utc >= ?
             ORDER BY event_ts ASC, id ASC
            """,
            (symbol, since_iso),
        ).fetchall()
        mids: list[dict] = []
        spread_vals: list[float] = []
        for row in list(spot_rows or []):
            bid = _safe_float(row["bid"], 0.0)
            ask = _safe_float(row["ask"], 0.0)
            mid = ((bid + ask) / 2.0) if bid > 0 and ask > 0 else 0.0
            if mid <= 0:
                continue
            mids.append(
                {
                    "event_utc": str(row["event_utc"] or ""),
                    "event_ts": _safe_float(row["event_ts"], 0.0),
                    "mid": mid,
                    "spread_pct": _safe_float(row["spread_pct"], 0.0),
                }
            )
            spread_vals.append(_safe_float(row["spread_pct"], 0.0))
        if len(mids) < 4:
            return {}
        baseline_spread = _avg(spread_vals)
        best = None
        max_window_sec = int(window_min * 60)
        for idx, row in enumerate(mids):
            start_ts = _safe_float(row["event_ts"], 0.0)
            start_mid = _safe_float(row["mid"], 0.0)
            if start_ts <= 0 or start_mid <= 0:
                continue
            low_mid = start_mid
            low_row = row
            for nxt in mids[idx + 1 :]:
                ts = _safe_float(nxt["event_ts"], 0.0)
                if (ts - start_ts) > max_window_sec:
                    break
                mid = _safe_float(nxt["mid"], 0.0)
                if mid > 0 and mid < low_mid:
                    low_mid = mid
                    low_row = nxt
            move_pct = ((low_mid - start_mid) / start_mid) * 100.0
            if best is None or move_pct < float(best.get("move_pct", 0.0) or 0.0):
                best = {
                    "start_utc": str(row["event_utc"] or ""),
                    "end_utc": str(low_row.get("event_utc") or ""),
                    "start_ts": start_ts,
                    "end_ts": _safe_float(low_row.get("event_ts"), start_ts),
                    "start_mid": start_mid,
                    "end_mid": low_mid,
                    "move_pct": move_pct,
                }
        if not best or abs(float(best.get("move_pct", 0.0) or 0.0)) < min_drop_pct or float(best.get("move_pct", 0.0) or 0.0) >= 0.0:
            return {}
        shock_spreads = [
            float(x.get("spread_pct", 0.0) or 0.0)
            for x in mids
            if float(best["start_ts"]) <= float(x.get("event_ts", 0.0) or 0.0) <= float(best["end_ts"])
        ]
        from_iso = _ms_to_iso(int(float(best["start_ts"]) * 1000.0))
        to_iso = _ms_to_iso(int(float(best["end_ts"]) * 1000.0))
        depth_rows = conn.execute(
            """
            SELECT side, size
              FROM ctrader_depth_quotes
             WHERE symbol = ?
               AND event_utc >= ?
               AND event_utc <= ?
            """,
            (symbol, from_iso, to_iso),
        ).fetchall()
        bid_size = sum(_safe_float(x["size"], 0.0) for x in list(depth_rows or []) if str(x["side"] or "").strip().lower() == "bid")
        ask_size = sum(_safe_float(x["size"], 0.0) for x in list(depth_rows or []) if str(x["side"] or "").strip().lower() == "ask")
        total_size = bid_size + ask_size
        depth_imbalance = ((bid_size - ask_size) / total_size) if total_size > 0 else 0.0
        shock_spread = _avg(shock_spreads)
        spread_ratio = (shock_spread / baseline_spread) if baseline_spread > 0 else 0.0
        shock_type = "fast_selloff_repricing"
        if spread_ratio >= 1.25 and depth_imbalance <= -0.05:
            shock_type = "liquidity_selloff_shock"
        elif depth_imbalance <= -0.05:
            shock_type = "orderbook_sell_pressure"
        elif spread_ratio >= 1.25:
            shock_type = "spread_widening_liquidity_gap"
        return {
            "symbol": symbol,
            "shock_type": shock_type,
            "start_utc": best["start_utc"],
            "end_utc": best["end_utc"],
            "duration_min": round(max(0.0, (float(best["end_ts"]) - float(best["start_ts"])) / 60.0), 2),
            "start_mid": round(float(best["start_mid"]), 4),
            "end_mid": round(float(best["end_mid"]), 4),
            "move_pct": round(float(best["move_pct"]), 4),
            "baseline_spread_pct": round(float(baseline_spread), 5),
            "shock_spread_pct": round(float(shock_spread), 5),
            "spread_ratio": round(float(spread_ratio), 4),
            "depth_imbalance": round(float(depth_imbalance), 4),
        }

    @staticmethod
    def _shock_explanation(shock: dict) -> str:
        data = dict(shock or {})
        shock_type = str(data.get("shock_type") or "").strip()
        move_pct = float(data.get("move_pct", 0.0) or 0.0)
        spread_ratio = float(data.get("spread_ratio", 0.0) or 0.0)
        depth = float(data.get("depth_imbalance", 0.0) or 0.0)
        if shock_type == "liquidity_selloff_shock":
            return (
                f"price repriced down fast with thinner liquidity: move {move_pct:.2f}% "
                f"| spread x{spread_ratio:.2f} | depth {depth:+.3f}"
            )
        if shock_type == "orderbook_sell_pressure":
            return (
                f"sell pressure dominated the book during the drop: move {move_pct:.2f}% "
                f"| depth {depth:+.3f}"
            )
        if shock_type == "spread_widening_liquidity_gap":
            return (
                f"price drop came with spread expansion and weaker fill quality: move {move_pct:.2f}% "
                f"| spread x{spread_ratio:.2f}"
            )
        if shock_type == "fast_selloff_repricing":
            return (
                f"market repriced down aggressively without strong spread blowout: move {move_pct:.2f}% "
                f"| spread x{spread_ratio:.2f} | depth {depth:+.3f}"
            )
        return ""

    @staticmethod
    def _symbol_relevant_themes(symbol: str) -> set[str]:
        sym = _norm_symbol(symbol)
        if sym == "XAUUSD":
            return {
                "GEOPOLITICS",
                "OIL_ENERGY_SHOCK",
                "FED_POLICY",
                "INFLATION",
                "LABOR_GROWTH",
                "TARIFF_TRADE",
                "TRUMP_POLICY",
            }
        if sym in {"BTCUSD", "ETHUSD"}:
            return {
                "CRYPTO_REGULATION",
                "FED_POLICY",
                "INFLATION",
                "LABOR_GROWTH",
                "TARIFF_TRADE",
                "GEOPOLITICS",
            }
        return set()

    @staticmethod
    def _symbol_relevant_currencies(symbol: str) -> set[str]:
        sym = _norm_symbol(symbol)
        if sym in {"XAUUSD", "BTCUSD", "ETHUSD"}:
            return {"USD"}
        return {"USD"}

    @staticmethod
    def _event_market_hint(symbol: str, title: str) -> str:
        text = str(title or "").lower()
        sym = _norm_symbol(symbol)
        if any(token in text for token in ("cpi", "inflation", "ppi", "core pce", "fomc", "fed", "powell", "nfp", "nonfarm", "jobless", "unemployment")):
            if sym == "XAUUSD":
                return "USD/rates event: gold can whipsaw hard; avoid countertrend scalp close to release."
            return "USD macro event: crypto can reprice with risk sentiment and dollar/yield reaction."
        if any(token in text for token in ("opec", "crude", "oil", "energy")) and sym == "XAUUSD":
            return "Energy shock theme can spill into inflation expectations and gold repricing."
        return "High-impact release: expect spread, speed, and first-minute false breaks."

    @staticmethod
    def _serialize_upcoming_event(symbol: str, event) -> dict:
        minutes = int(getattr(event, "minutes_to_event", 0))
        title = str(getattr(event, "title", "") or "")
        return {
            "event_id": str(getattr(event, "event_id", "") or ""),
            "title": title,
            "currency": str(getattr(event, "currency", "") or ""),
            "impact": str(getattr(event, "impact", "") or ""),
            "time_utc": TradingManagerAgent._safe_dt_iso(getattr(event, "time_utc", None)),
            "minutes_to_event": minutes,
            "market_hint": TradingManagerAgent._event_market_hint(symbol, title),
        }

    def _load_macro_context(self) -> dict:
        lookback_h = max(1, int(getattr(config, "TRADING_MANAGER_MACRO_LOOKBACK_HOURS", getattr(config, "MACRO_NEWS_LOOKBACK_HOURS", 24)) or 24))
        min_score = max(1, int(getattr(config, "TRADING_MANAGER_MACRO_MIN_SCORE", getattr(config, "MACRO_NEWS_MIN_SCORE", 8)) or 8))
        cal_hours = max(1, int(getattr(config, "TRADING_MANAGER_CALENDAR_LOOKAHEAD_HOURS", getattr(config, "ECON_CALENDAR_LOOKAHEAD_HOURS", 8)) or 8))
        out = {"headlines": [], "impact_entries": [], "upcoming_events": [], "sync": {}}
        try:
            out["headlines"] = list(macro_news.high_impact_headlines(hours=lookback_h, min_score=min_score, limit=12) or [])
        except Exception:
            out["headlines"] = []
        try:
            out["sync"] = dict(macro_impact_tracker.sync(hours=max(lookback_h, int(getattr(config, "MACRO_IMPACT_TRACKER_LOOKBACK_HOURS", 72) or 72)), min_score=min_score, limit=max(8, int(getattr(config, "MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC", 20) or 20))) or {})
        except Exception:
            out["sync"] = {"ok": False}
        try:
            impact = dict(macro_impact_tracker.build_report(hours=max(lookback_h, 24), min_score=min_score, limit=8) or {})
            out["impact_entries"] = list(impact.get("entries") or [])
        except Exception:
            out["impact_entries"] = []
        try:
            out["upcoming_events"] = list(
                economic_calendar.next_events(
                    hours=cal_hours,
                    limit=max(1, int(getattr(config, "TRADING_MANAGER_CALENDAR_MAX_EVENTS", 6) or 6)),
                    min_impact=str(getattr(config, "ECON_CALENDAR_MIN_IMPACT", "high") or "high"),
                    currencies={"USD"},
                )
                or []
            )
        except Exception:
            out["upcoming_events"] = []
        return out

    def _pick_macro_cause(self, *, symbol: str, shock: dict, macro_ctx: dict) -> dict:
        sym = _norm_symbol(symbol)
        relevant_themes = self._symbol_relevant_themes(sym)
        shock_start = _parse_iso_fallback((shock or {}).get("start_utc")) if shock else None
        shock_end = _parse_iso_fallback((shock or {}).get("end_utc")) if shock else None
        best = None
        for entry in list((macro_ctx or {}).get("impact_entries") or []):
            asset = dict((entry.get("assets") or {}).get(sym) or {})
            classification = str(entry.get("classification") or "").strip().lower()
            asset_class = str(asset.get("classification") or "").strip().lower()
            title = str(entry.get("title") or "")
            themes = {str(t or "").strip().upper() for t in list(entry.get("themes") or []) if str(t or "").strip()}
            if relevant_themes and (not themes.intersection(relevant_themes)):
                continue
            published_dt = None
            pub_raw = entry.get("published_utc")
            if isinstance(pub_raw, datetime):
                published_dt = pub_raw if pub_raw.tzinfo else pub_raw.replace(tzinfo=timezone.utc)
            else:
                published_dt = _parse_iso_fallback(pub_raw)
            score = float(entry.get("score", 0) or 0)
            rank = score
            if classification in {"impact_confirmed", "impact_developing"}:
                rank += 3.0
            elif classification == "priced_in":
                rank += 1.0
            if asset_class in {"impact_confirmed", "impact_developing"}:
                rank += 2.0
            if shock_start and published_dt:
                delta_min = abs((published_dt - shock_start).total_seconds()) / 60.0
                if delta_min <= 120:
                    rank += 2.5
                elif delta_min <= 240:
                    rank += 1.0
            if shock_end and published_dt and published_dt > shock_end + timedelta(minutes=30):
                rank -= 1.5
            candidate = {
                "headline_id": str(entry.get("headline_id") or ""),
                "title": title,
                "source": str(entry.get("source") or ""),
                "published_utc": self._safe_dt_iso(published_dt),
                "score": int(entry.get("score", 0) or 0),
                "themes": sorted(list(themes)),
                "classification": str(entry.get("classification") or ""),
                "classification_human": str(entry.get("classification_human") or ""),
                "reaction_summary": str(entry.get("reaction_summary") or ""),
                "asset_classification": asset_class,
                "likely_relation": self._event_market_hint(sym, title),
                "_rank": rank,
            }
            if best is None or float(candidate["_rank"]) > float(best.get("_rank", -1e9)):
                best = candidate
        if best:
            best.pop("_rank", None)
            return best
        for headline in list((macro_ctx or {}).get("headlines") or []):
            themes = {str(t or "").strip().upper() for t in list(getattr(headline, "themes", []) or []) if str(t or "").strip()}
            if relevant_themes and (not themes.intersection(relevant_themes)):
                continue
            return {
                "headline_id": str(getattr(headline, "headline_id", "") or ""),
                "title": str(getattr(headline, "title", "") or ""),
                "source": str(getattr(headline, "source", "") or ""),
                "published_utc": self._safe_dt_iso(getattr(headline, "published_utc", None)),
                "score": int(getattr(headline, "score", 0) or 0),
                "themes": sorted(list(themes)),
                "classification": "",
                "classification_human": "",
                "reaction_summary": str(getattr(headline, "impact_hint", "") or ""),
                "asset_classification": "",
                "likely_relation": self._event_market_hint(sym, str(getattr(headline, "title", "") or "")),
            }
        return {}

    def _relevant_upcoming_events(self, *, symbol: str, macro_ctx: dict) -> list[dict]:
        currencies = self._symbol_relevant_currencies(symbol)
        out = []
        for event in list((macro_ctx or {}).get("upcoming_events") or []):
            ccy = str(getattr(event, "currency", "") or "").upper()
            if currencies and ccy not in currencies:
                continue
            out.append(self._serialize_upcoming_event(symbol, event))
        return out[:3]

    def _post_event_learning(self, *, symbol: str, macro_ctx: dict) -> list[dict]:
        sym = _norm_symbol(symbol)
        out = []
        for entry in list((macro_ctx or {}).get("impact_entries") or []):
            asset = dict((entry.get("assets") or {}).get(sym) or {})
            asset_class = str(asset.get("classification") or "").strip().lower()
            if asset_class in {"pending", "incomplete", ""}:
                continue
            out.append(
                {
                    "headline_id": str(entry.get("headline_id") or ""),
                    "title": str(entry.get("title") or ""),
                    "source": str(entry.get("source") or ""),
                    "classification_human": str(entry.get("classification_human") or ""),
                    "asset_classification_human": str(asset.get("classification_human") or ""),
                    "reaction_summary": str(entry.get("reaction_summary") or ""),
                    "themes": list(entry.get("themes") or []),
                    "published_utc": self._safe_dt_iso(entry.get("published_utc")),
                }
            )
        return out[:3]

    def _derive_upcoming_event_actions(self, *, symbol: str, upcoming_events: list[dict]) -> list[dict]:
        freeze_min = max(10, int(getattr(config, "TRADING_MANAGER_PRE_EVENT_FREEZE_MIN", 20) or 20))
        actions: list[dict] = []
        for event in list(upcoming_events or []):
            minutes = int(event.get("minutes_to_event", 0) or 0)
            title = str(event.get("title") or "")
            if minutes <= freeze_min:
                actions.append(
                    {
                        "action": "pre_event_freeze_countertrend",
                        "event_title": title,
                        "minutes_to_event": minutes,
                        "reason": str(event.get("market_hint") or ""),
                    }
                )
            elif minutes <= 60:
                actions.append(
                    {
                        "action": "pre_event_reduce_size_and_quick_tp",
                        "event_title": title,
                        "minutes_to_event": minutes,
                        "reason": str(event.get("market_hint") or ""),
                    }
                )
        return actions

    def _derive_xau_profile_recommendation(self, *, shock: dict, losses: dict, best_same_situation: dict, upcoming_events: list[dict]) -> dict:
        if not shock and not upcoming_events:
            return {}
        severe_loss = bool(int(losses.get("resolved", 0) or 0) > 0 and float(losses.get("pnl_usd", 0.0) or 0.0) < 0.0)
        freeze_min = max(10, int(getattr(config, "TRADING_MANAGER_PRE_EVENT_FREEZE_MIN", 20) or 20))
        urgent_event = any(int(ev.get("minutes_to_event", 9999) or 9999) <= freeze_min for ev in list(upcoming_events or []))
        if not severe_loss and not urgent_event:
            return {}
        no_positive_same = not bool(best_same_situation)
        if urgent_event:
            mode = "pre_event_caution"
            size_mult = float(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_SIZE_MULT", 0.30) or 0.30)
            tp1 = float(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_TP1_RR", 0.50) or 0.50)
            tp2 = float(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_TP2_RR", 0.85) or 0.85)
            tp3 = float(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_TP3_RR", 1.20) or 1.20)
            next_event = dict((upcoming_events or [{}])[0] or {})
            reason = f"upcoming event {str(next_event.get('title') or '-')[:80]} in {int(next_event.get('minutes_to_event', 0) or 0)}m"
        elif severe_loss and no_positive_same:
            mode = "shock_protect"
            size_mult = float(getattr(config, "TRADING_MANAGER_XAU_SHOCK_SIZE_MULT", 0.25) or 0.25)
            tp1 = float(getattr(config, "TRADING_MANAGER_XAU_SHOCK_TP1_RR", 0.45) or 0.45)
            tp2 = float(getattr(config, "TRADING_MANAGER_XAU_SHOCK_TP2_RR", 0.75) or 0.75)
            tp3 = float(getattr(config, "TRADING_MANAGER_XAU_SHOCK_TP3_RR", 1.05) or 1.05)
            reason = (
                f"shock losses {int(losses.get('losses', 0) or 0)}/{int(losses.get('resolved', 0) or 0)} "
                f"| pnl {float(losses.get('pnl_usd', 0.0) or 0.0):.2f}"
            )
        else:
            return {}
        changes = {
            "XAU_EVENT_SHOCK_SIZE_MULT": f"{max(0.05, min(1.0, size_mult)):.2f}".rstrip("0").rstrip("."),
            "XAU_EVENT_SHOCK_TP1_RR": f"{max(0.30, tp1):.2f}".rstrip("0").rstrip("."),
            "XAU_EVENT_SHOCK_TP2_RR": f"{max(tp1 + 0.10, tp2):.2f}".rstrip("0").rstrip("."),
            "XAU_EVENT_SHOCK_TP3_RR": f"{max(tp2 + 0.10, tp3):.2f}".rstrip("0").rstrip("."),
        }
        return {"active": True, "mode": mode, "changes": changes, "reason": reason}

    def _apply_profile_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_TUNE_ENABLED", True))
        persist = bool(getattr(config, "TRADING_MANAGER_AUTO_TUNE_PERSIST_ENV", True))
        out = {"enabled": enabled, "persist_env": persist, "status": "disabled" if not enabled else "none", "applied": {}, "reverted": {}, "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_shock_profile") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("profile_recommendations") or {})
        if rec.get("active"):
            originals = dict(active.get("originals") or {})
            applied = {}
            for key, new_value in dict(rec.get("changes") or {}).items():
                old_value = self._current_value(str(key))
                originals.setdefault(str(key), str(old_value))
                if str(old_value) == str(new_value):
                    continue
                self._apply_runtime_value(str(key), str(new_value))
                applied[str(key)] = self._persist_env_value(str(key), str(new_value), persist)
            state["xau_shock_profile"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "applied_at": _iso(_utc_now()),
                "originals": originals,
                "changes": dict(rec.get("changes") or {}),
            }
            self._save_state(state)
            out.update({"status": "applied" if applied else "already_active", "applied": applied, "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            reverted = {}
            for key, old_value in dict(active.get("originals") or {}).items():
                self._apply_runtime_value(str(key), str(old_value))
                reverted[str(key)] = self._persist_env_value(str(key), str(old_value), persist)
            state["xau_shock_profile"] = {"status": "inactive", "reverted_at": _iso(_utc_now()), "originals": dict(active.get("originals") or {})}
            self._save_state(state)
            out.update({"status": "reverted", "reverted": reverted, "reason": "shock_or_pre_event_clear"})
        return out

    def _derive_xau_family_routing_recommendation(
        self,
        *,
        selected_family: str,
        shock: dict,
        losses: dict,
        pb_source_stats: dict,
        scheduled_source_stats: dict,
        scheduled_family_calibration: dict,
        best_same_situation: dict,
        best_family_today: dict,
        winner_memory_reference: dict,
        upcoming_events: list[dict],
        post_event_learning: list[dict],
    ) -> dict:
        allowed = self._allowed_xau_routing_families()
        if not allowed:
            return {}
        swarm_enabled = bool(getattr(config, "TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False))
        swarm_active = self._xau_swarm_active_families(allowed)
        freeze_min = max(10, int(getattr(config, "TRADING_MANAGER_PRE_EVENT_FREEZE_MIN", 20) or 20))
        urgent_event = any(int(ev.get("minutes_to_event", 9999) or 9999) <= freeze_min for ev in list(upcoming_events or []))
        severe_loss = bool(int(losses.get("resolved", 0) or 0) > 0 and float(losses.get("pnl_usd", 0.0) or 0.0) < 0.0)
        preferred_same = self._family_from_bucket(best_same_situation)
        preferred_leader = self._family_from_bucket(best_family_today)
        memory_family = str((winner_memory_reference or {}).get("family") or "").strip().lower()

        def _valid_family(token: str) -> str:
            tok = str(token or "").strip().lower()
            return tok if tok in allowed else ""

        current_primary = _valid_family(self._runtime_family_value("CTRADER_XAU_PRIMARY_FAMILY", "CTRADER_XAU_PRIMARY_FAMILY")) or _valid_family(selected_family)
        current_active = self._parse_family_csv(
            ",".join(
                [
                    str(self._current_value("CTRADER_XAU_ACTIVE_FAMILIES") or ""),
                    str(getattr(config, "CTRADER_XAU_ACTIVE_FAMILIES", "") or ""),
                    ",".join(list(getattr(config, "get_ctrader_xau_active_families", lambda: set())() or set())),
                ]
            )
        )
        current_active = [fam for fam in current_active if fam in allowed] or list(allowed)

        if swarm_enabled and swarm_active:
            desired_active = [fam for fam in list(swarm_active or []) if fam in allowed] or list(current_active or allowed)
            current_active_set = {fam for fam in list(current_active or []) if fam}
            desired_active_set = {fam for fam in list(desired_active or []) if fam}
            promoted = [fam for fam in list(desired_active or []) if fam not in current_active_set]
            demoted = [fam for fam in list(current_active or []) if fam not in desired_active_set]
            return {
                "active": True,
                "mode": "swarm_support_all",
                "reason": "swarm sampling mode: support all active XAU families for broader data collection",
                "support_mode": "sample_collection",
                "swarm_sampling": True,
                "changes": {"CTRADER_XAU_ACTIVE_FAMILIES": ",".join(desired_active)},
                "promoted_families": promoted,
                "demoted_families": demoted,
                "previous_primary_family": current_primary,
                "previous_active_families": current_active,
            }

        mode = ""
        reason = ""
        primary = ""
        active: list[str] = []
        change_keys = {
            "CTRADER_XAU_PRIMARY_FAMILY",
            "CTRADER_XAU_ACTIVE_FAMILIES",
            "PERSISTENT_CANARY_STRATEGY_FAMILIES",
        }

        if urgent_event:
            mode = "pre_event_caution"
            primary = _valid_family(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_PRIMARY_FAMILY", "xau_scalp_pullback_limit"))
            active = [fam for fam in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_PRE_EVENT_ACTIVE_FAMILIES", "xau_scalp_pullback_limit")) if fam in allowed]
            if swarm_enabled and swarm_active:
                active = list(swarm_active)
            next_event = dict((upcoming_events or [{}])[0] or {})
            reason = f"upcoming event {str(next_event.get('title') or '-')[:80]} in {int(next_event.get('minutes_to_event', 0) or 0)}m"
        elif severe_loss and not preferred_same:
            mode = "shock_demote"
            primary = _valid_family(getattr(config, "TRADING_MANAGER_XAU_SHOCK_PRIMARY_FAMILY", "xau_scalp_pullback_limit"))
            active = [fam for fam in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_SHOCK_ACTIVE_FAMILIES", "xau_scalp_pullback_limit")) if fam in allowed]
            if swarm_enabled and swarm_active:
                active = list(swarm_active)
            reason = (
                f"shock losses {int(losses.get('losses', 0) or 0)}/{int(losses.get('resolved', 0) or 0)} "
                f"| pnl {float(losses.get('pnl_usd', 0.0) or 0.0):.2f}"
            )
        else:
            pb_resolved = int(pb_source_stats.get("resolved", 0) or 0)
            pb_pnl = float(pb_source_stats.get("pnl_usd", 0.0) or 0.0)
            scheduled_resolved = int(scheduled_source_stats.get("resolved", 0) or 0)
            scheduled_pnl = float(scheduled_source_stats.get("pnl_usd", 0.0) or 0.0)
            scheduled_wr = float(scheduled_source_stats.get("win_rate", 0.0) or 0.0)
            scheduled_calib = dict((scheduled_family_calibration or {}).get("overall") or {})
            scheduled_calib_resolved = int(scheduled_calib.get("resolved", 0) or 0)
            scheduled_calib_pnl = float(scheduled_calib.get("pnl_usd", 0.0) or 0.0)
            scheduled_calib_wr = float(scheduled_calib.get("win_rate", 0.0) or 0.0)
            scheduled_support_mode = ""
            scheduled_support_ok = (
                scheduled_resolved >= max(2, int(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_RESOLVED", 6) or 6))
                and scheduled_pnl >= float(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_PNL_USD", 10.0) or 10.0)
                and scheduled_wr >= float(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_WIN_RATE", 0.60) or 0.60)
            )
            if scheduled_support_ok:
                scheduled_support_mode = "live_24h"
            elif bool(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_USE_CALIBRATION_FALLBACK", True)):
                scheduled_support_ok = (
                    scheduled_calib_resolved >= max(4, int(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_RESOLVED", 20) or 20))
                    and scheduled_calib_pnl >= float(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_PNL_USD", 40.0) or 40.0)
                    and scheduled_calib_wr >= float(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_WIN_RATE", 0.72) or 0.72)
                )
                if scheduled_support_ok:
                    scheduled_support_mode = "calibration_fallback"
            pb_active = "xau_scalp_pullback_limit" in current_active or current_primary == "xau_scalp_pullback_limit"
            if (
                bool(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_ENABLED", True))
                and pb_active
                and pb_resolved >= max(2, int(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MIN_PB_RESOLVED", 12) or 12))
                and pb_pnl <= float(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_MAX_PB_PNL_USD", -10.0) or -10.0)
                and scheduled_support_ok
            ):
                mode = "scheduled_dominant_demote_pb"
                primary = _valid_family(getattr(config, "TRADING_MANAGER_XAU_PB_DEMOTE_PRIMARY_FAMILY", "xau_scalp_tick_depth_filter"))
                active = [
                    fam
                    for fam in self._parse_family_csv(
                        getattr(
                            config,
                            "TRADING_MANAGER_XAU_PB_DEMOTE_ACTIVE_FAMILIES",
                            "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop",
                        )
                    )
                    if fam in allowed
                ]
                if swarm_enabled and swarm_active:
                    active = list(swarm_active)
                if scheduled_support_mode == "calibration_fallback":
                    reason = (
                        f"pb pnl {pb_pnl:.2f} on {pb_resolved} vs scheduled calibration "
                        f"pnl {scheduled_calib_pnl:.2f} wr {scheduled_calib_wr:.2%} on {scheduled_calib_resolved}"
                    )
                else:
                    reason = (
                        f"pb pnl {pb_pnl:.2f} on {pb_resolved} vs scheduled live pnl {scheduled_pnl:.2f} "
                        f"wr {scheduled_wr:.2%} on {scheduled_resolved}"
                    )
                change_keys = {"CTRADER_XAU_PRIMARY_FAMILY", "CTRADER_XAU_ACTIVE_FAMILIES"}
            else:
                min_resolved = max(2, int(getattr(config, "TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_RESOLVED", 3) or 3))
                min_pnl = float(getattr(config, "TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_PNL_USD", 0.0) or 0.0)
                promotion_family = ""
                if preferred_same and int(best_same_situation.get("resolved", 0) or 0) >= min_resolved and float(best_same_situation.get("pnl_usd", 0.0) or 0.0) > min_pnl:
                    promotion_family = preferred_same
                    mode = "post_event_promote_same_situation"
                    reason = f"same-situation winner pnl {float(best_same_situation.get('pnl_usd', 0.0) or 0.0):.2f}"
                elif preferred_leader and int(best_family_today.get("resolved", 0) or 0) >= min_resolved and float(best_family_today.get("pnl_usd", 0.0) or 0.0) > min_pnl:
                    promotion_family = preferred_leader
                    mode = "post_event_promote_today_leader"
                    reason = f"today leader pnl {float(best_family_today.get('pnl_usd', 0.0) or 0.0):.2f}"
                elif _valid_family(memory_family) and post_event_learning:
                    promotion_family = _valid_family(memory_family)
                    mode = "post_event_memory_reuse"
                    reason = f"winner memory after event: {memory_family}"
                if promotion_family:
                    primary = promotion_family
                    active = [promotion_family]
                    if swarm_enabled and swarm_active:
                        active = list(swarm_active)

        primary = _valid_family(primary)
        active = [fam for fam in active if fam in allowed]
        if not primary:
            primary = current_primary or (current_active[0] if current_active else (allowed[0] if allowed else ""))
        if not active and primary:
            active = [primary]
        if not primary or not active:
            return {}
        if not mode:
            return {}
        if swarm_enabled:
            if current_primary:
                primary = current_primary
            change_keys.discard("CTRADER_XAU_PRIMARY_FAMILY")
            change_keys.discard("PERSISTENT_CANARY_STRATEGY_FAMILIES")
            if swarm_active:
                active = list(swarm_active)
        demoted = [fam for fam in current_active if fam not in active]
        promoted = [fam for fam in active if fam not in current_active or fam == primary]
        changes = {}
        if "CTRADER_XAU_PRIMARY_FAMILY" in change_keys:
            changes["CTRADER_XAU_PRIMARY_FAMILY"] = primary
        if "CTRADER_XAU_ACTIVE_FAMILIES" in change_keys:
            changes["CTRADER_XAU_ACTIVE_FAMILIES"] = ",".join(active)
        if "PERSISTENT_CANARY_STRATEGY_FAMILIES" in change_keys:
            changes["PERSISTENT_CANARY_STRATEGY_FAMILIES"] = ",".join(active)
        return {
            "active": True,
            "mode": mode or "manager_override",
            "reason": reason or "manager family routing adjustment",
            "support_mode": locals().get("scheduled_support_mode", ""),
            "swarm_sampling": swarm_enabled,
            "changes": changes,
            "promoted_families": promoted,
            "demoted_families": demoted,
            "previous_primary_family": current_primary,
            "previous_active_families": current_active,
        }

    def _apply_family_routing_recommendations(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_ENABLED", True))
        persist = bool(getattr(config, "TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", True))
        out = {"enabled": enabled, "persist_env": persist, "status": "disabled" if not enabled else "none", "applied": {}, "reverted": {}, "reason": ""}
        if not enabled:
            return out
        state = self._load_state()
        active = dict(state.get("xau_family_routing") or {})
        xau = next((row for row in list((report or {}).get("symbols") or []) if str(row.get("symbol") or "") == "XAUUSD"), {})
        rec = dict(xau.get("family_routing_recommendations") or {})
        if rec.get("active"):
            originals = dict(active.get("originals") or {})
            applied = {}
            for key, new_value in dict(rec.get("changes") or {}).items():
                old_value = self._current_value(str(key))
                originals.setdefault(str(key), str(old_value))
                if str(old_value) == str(new_value):
                    continue
                self._apply_runtime_value(str(key), str(new_value))
                applied[str(key)] = self._persist_env_value(str(key), str(new_value), persist)
            state["xau_family_routing"] = {
                "status": "active",
                "mode": str(rec.get("mode") or ""),
                "reason": str(rec.get("reason") or ""),
                "applied_at": _iso(_utc_now()),
                "originals": originals,
                "changes": dict(rec.get("changes") or {}),
            }
            self._save_state(state)
            out.update({"status": "applied" if applied else "already_active", "applied": applied, "reason": str(rec.get("reason") or "")})
            return out
        if active and str(active.get("status") or "") == "active":
            reverted = {}
            for key, old_value in dict(active.get("originals") or {}).items():
                self._apply_runtime_value(str(key), str(old_value))
                reverted[str(key)] = self._persist_env_value(str(key), str(old_value), persist)
            state["xau_family_routing"] = {"status": "inactive", "reverted_at": _iso(_utc_now()), "originals": dict(active.get("originals") or {})}
            self._save_state(state)
            out.update({"status": "reverted", "reverted": reverted, "reason": "family_routing_clear"})
        return out

    @staticmethod
    def _bucket_rows(rows: list[dict], key_fn) -> list[dict]:
        grouped: dict[tuple, dict] = {}
        for row in list(rows or []):
            key = key_fn(row)
            bucket = grouped.setdefault(
                tuple(key),
                {
                    "key": list(key),
                    "sources": set(),
                    "stats": _new_bucket(),
                    "directions": set(),
                    "sessions": set(),
                    "entry_types": set(),
                },
            )
            _update_bucket(bucket["stats"], _safe_float(row.get("pnl_usd"), 0.0), _safe_int(row.get("outcome"), -1))
            bucket["sources"].add(str(row.get("source") or ""))
            bucket["directions"].add(str(row.get("direction") or ""))
            bucket["sessions"].add(str(row.get("session") or ""))
            bucket["entry_types"].add(str(row.get("entry_type") or ""))
        out = []
        for bucket in list(grouped.values()):
            item = {
                "key": list(bucket.get("key") or []),
                "sources": sorted(list(bucket.get("sources") or set())),
                "directions": sorted(list(bucket.get("directions") or set())),
                "sessions": sorted(list(bucket.get("sessions") or set())),
                "entry_types": sorted(list(bucket.get("entry_types") or set())),
            }
            item.update(_finalize_bucket(bucket.get("stats") or _new_bucket()))
            out.append(item)
        out.sort(key=lambda row: (float(row.get("pnl_usd", 0.0) or 0.0), float(row.get("win_rate", 0.0) or 0.0)), reverse=True)
        return out

    @staticmethod
    def _source_bucket(rows: list[dict], *sources: str) -> dict:
        wanted = {_norm_source(src) for src in list(sources or []) if str(src or "").strip()}
        bucket = _new_bucket()
        for row in list(rows or []):
            if wanted and _norm_source(row.get("source")) not in wanted:
                continue
            _update_bucket(bucket, _safe_float(row.get("pnl_usd"), 0.0), _safe_int(row.get("outcome"), -1))
        return _finalize_bucket(bucket)

    @staticmethod
    def _symbol_states(chart_state_memory: dict, symbol: str, *, follow_up_only: bool = False) -> list[dict]:
        symbol_token = _norm_symbol(symbol)
        out: list[dict] = []
        for row in list((chart_state_memory or {}).get("states") or []):
            if not isinstance(row, dict):
                continue
            if _norm_symbol(row.get("symbol")) != symbol_token:
                continue
            if follow_up_only and not bool(row.get("follow_up_candidate")):
                continue
            out.append(dict(row))
        out.sort(
            key=lambda item: (
                1 if bool(item.get("follow_up_candidate")) else 0,
                float(item.get("state_score", 0.0) or 0.0),
                float(((item.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((item.get("stats") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        return out

    @staticmethod
    def _derive_xau_slot_budget(top_state: dict | None) -> dict:
        state = dict(top_state or {})
        day_type = str(state.get("day_type") or "trend").strip().lower() or "trend"
        budgets = {
            "trend": max(2, int(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_TREND_MAX_SAME_DIRECTION", 3) or 3)),
            "repricing": max(1, int(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_REPRICING_MAX_SAME_DIRECTION", 2) or 2)),
            "fast_expansion": max(1, int(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_FAST_EXPANSION_MAX_SAME_DIRECTION", 2) or 2)),
            "panic_spread": max(1, int(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_PANIC_SPREAD_MAX_SAME_DIRECTION", 1) or 1)),
        }
        max_same = int(budgets.get(day_type, budgets["trend"]) or budgets["trend"])
        rationale = {
            "trend": "trend day supports parallel retest/follow-up families",
            "repricing": "repricing day allows fewer concurrent families to reduce overlap noise",
            "fast_expansion": "fast expansion day keeps continuation lanes active but trims retest crowding",
            "panic_spread": "panic spread day collapses slots to the smallest safe budget",
        }
        return {
            "day_type": day_type,
            "budget_mode": "by_day_type",
            "max_same_direction_families": max_same,
            "reason": str(rationale.get(day_type) or rationale["trend"]),
        }

    @staticmethod
    def _xau_family_bias_direction(family: str) -> str:
        token = str(family or "").strip().lower()
        if token in {"xau_scalp_flow_short_sidecar", "xau_scalp_failed_fade_follow_stop"}:
            return "short"
        return ""

    def _infer_xau_live_pressure_direction(
        self,
        *,
        selected_family: str,
        micro_regime_refresh: dict | None,
        recent_order_reviews: list[dict],
        open_positions: list[dict],
        open_orders: list[dict],
        symbol_states: list[dict],
    ) -> tuple[str, dict]:
        scores = {"long": 0.0, "short": 0.0}
        evidence = {"open_positions": [], "open_orders": [], "recent_reviews": [], "memory": []}
        for row in list(open_positions or []):
            side = self._trade_direction(str(row.get("direction") or ""))
            if side in scores:
                scores[side] += 4.0
                evidence["open_positions"].append(side)
        for row in list(open_orders or []):
            side = self._trade_direction(str(row.get("direction") or ""))
            if side in scores:
                scores[side] += 3.0
                evidence["open_orders"].append(side)
        for row in list(recent_order_reviews or [])[:4]:
            side = self._trade_direction(str(row.get("direction") or ""))
            if side in scores:
                scores[side] += 1.25
                evidence["recent_reviews"].append(side)
        regime_side = self._trade_direction(str((micro_regime_refresh or {}).get("dominant_direction") or ""))
        if regime_side in scores:
            scores[regime_side] += 1.0
            evidence["memory"].append(f"micro_regime:{regime_side}")
        for row in list(symbol_states or [])[:2]:
            side = self._trade_direction(str(row.get("direction") or ""))
            if side in scores:
                scores[side] += 0.8
                evidence["memory"].append(f"chart_state:{side}")
        family_side = self._xau_family_bias_direction(selected_family)
        if family_side in scores:
            scores[family_side] += 0.5
            evidence["memory"].append(f"selected_family:{family_side}")
        ranked = sorted(scores.items(), key=lambda item: float(item[1] or 0.0), reverse=True)
        if not ranked or float(ranked[0][1] or 0.0) <= 0.0:
            return "", {"scores": scores, "evidence": evidence}
        if len(ranked) > 1 and abs(float(ranked[0][1] or 0.0) - float(ranked[1][1] or 0.0)) < 0.35:
            return "", {"scores": scores, "evidence": evidence}
        return str(ranked[0][0] or ""), {"scores": scores, "evidence": evidence}

    def _xau_regime_transition_blocked_sources(self, blocked_families: list[str]) -> list[str]:
        families = {
            str(item or "").strip().lower()
            for item in list(blocked_families or [])
            if str(item or "").strip()
        }
        if not families:
            return []
        source_universe = self._parse_family_csv(
            ",".join(
                [
                    str(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_LIMIT_RETEST_ALLOWED_SOURCES", "") or ""),
                    str(getattr(config, "TRADING_MANAGER_XAU_ORDER_CARE_ALLOWED_SOURCES", "") or ""),
                    str(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_SOURCES", "") or ""),
                ]
            )
        )
        out: list[str] = []
        for source in list(source_universe or []):
            family = str(live_profile_autopilot._strategy_family_for_source("XAUUSD", source) or "").strip().lower()
            if family in families and source not in out:
                out.append(source)
        return out

    def _derive_xau_regime_transition_recommendation(
        self,
        *,
        selected_family: str,
        micro_regime_refresh: dict,
        recent_order_reviews: list[dict],
        open_positions: list[dict],
        open_orders: list[dict],
        symbol_states: list[dict],
    ) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED", True)):
            return {}
        lookback_sec = max(60, int(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_LOOKBACK_SEC", 240) or 240))
        snapshot = dict(
            live_profile_autopilot.latest_capture_feature_snapshot(
                symbol="XAUUSD",
                lookback_sec=lookback_sec,
            )
            or {}
        )
        if not bool(snapshot.get("ok")):
            return {}
        features = dict(snapshot.get("features") or {})
        current_side, pressure = self._infer_xau_live_pressure_direction(
            selected_family=selected_family,
            micro_regime_refresh=micro_regime_refresh,
            recent_order_reviews=recent_order_reviews,
            open_positions=open_positions,
            open_orders=open_orders,
            symbol_states=symbol_states,
        )
        if current_side not in {"long", "short"}:
            return {}
        current_state = dict(_classify_chart_state(current_side, {"raw_scores": {}}, capture_features=features) or {})
        opposite_side = "long" if current_side == "short" else "short"
        opposite_state = dict(_classify_chart_state(opposite_side, {"raw_scores": {}}, capture_features=features) or {})
        current_label = str(current_state.get("state_label") or "").strip().lower()
        opposite_label = str(opposite_state.get("state_label") or "").strip().lower()
        day_type = str(features.get("day_type") or current_state.get("day_type") or "trend").strip().lower() or "trend"
        rejection_ratio = float(features.get("rejection_ratio", 0.0) or 0.0)
        bar_volume_proxy = float(features.get("bar_volume_proxy", 0.0) or 0.0)
        current_bias = float(current_state.get("continuation_bias", 0.0) or 0.0)
        opposite_bias = float(opposite_state.get("continuation_bias", 0.0) or 0.0)
        range_states = {
            str(item or "").strip().lower()
            for item in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_RANGE_STATES", ""))
            if str(item or "").strip()
        }
        continuation_states = {"continuation_drive", "breakout_drive", "repricing_transition", "pullback_absorption"}
        current_range_like = (
            current_label in range_states
            or (
                abs(current_bias) <= float(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_MAX_ABS_CONTINUATION_BIAS", 0.055) or 0.055)
                and rejection_ratio >= float(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_REJECTION_RATIO", 0.34) or 0.34)
                and bar_volume_proxy >= float(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_BAR_VOLUME_PROXY", 0.18) or 0.18)
            )
        )
        if not current_range_like:
            return {}
        top_memory = next((dict(row) for row in list(symbol_states or []) if self._trade_direction(str(row.get("direction") or "")) == current_side), {})
        memory_state = str(top_memory.get("state_label") or "").strip().lower()
        opposite_ready = opposite_label in continuation_states and opposite_bias >= float(
            getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_OPPOSITE_BIAS", 0.03) or 0.03
        )
        blocked_families = [
            token
            for token in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_LIMIT_FAMILIES", ""))
            if token
        ]
        blocked_sources = self._xau_regime_transition_blocked_sources(blocked_families)
        preferred_families: list[str] = []
        preferred_sources: list[str] = []
        if day_type not in {"fast_expansion", "panic_spread"}:
            preferred_families = [
                token
                for token in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_FAMILIES", ""))
                if token
            ]
            preferred_sources = [
                token
                for token in self._parse_family_csv(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_SOURCES", ""))
                if token
            ]
        hold_min = max(3, int(getattr(config, "TRADING_MANAGER_XAU_REGIME_TRANSITION_HOLD_MIN", 12) or 12))
        now = _utc_now()
        hold_until = now + timedelta(minutes=hold_min)
        support_state = "range_repair_lead" if preferred_families else "pause_limit_retest_only"
        mode = "live_range_transition_limit_pause"
        if opposite_ready:
            mode = f"live_{current_side}_continuation_flip"
            support_state = "countertrend_probe_ready" if preferred_families else "pause_limit_retest_only"
        reason = (
            f"live {current_side} continuation degraded to {current_label or 'range_probe'} "
            f"| day={day_type or '-'} rej={rejection_ratio:.2f} bias={current_bias:+.3f}"
        )
        if opposite_ready:
            reason += f" | opposite={opposite_side}:{opposite_label or '-'} {opposite_bias:+.3f}"
        if memory_state and memory_state != current_label:
            reason += f" | memory={memory_state}"
        coach_traders = [
            (
                f"manager regime shift: pause {current_side} limit retests for {hold_min}m "
                f"while live state is {current_label or 'range_probe'}"
            ),
        ]
        if preferred_sources:
            coach_traders.append(
                f"manager regime shift: let {preferred_sources[0]} probe rebound/sideway repair first"
            )
        trader_assignments = []
        if preferred_sources:
            trader_assignments.append(
                {
                    "source": preferred_sources[0],
                    "family": str(preferred_families[0] if preferred_families else "").strip().lower(),
                    "task": "lead_range_repair",
                    "status": "priority",
                    "reason": support_state,
                }
            )
        for source in list(blocked_sources or []):
            trader_assignments.append(
                {
                    "source": source,
                    "family": str(live_profile_autopilot._strategy_family_for_source("XAUUSD", source) or "").strip().lower(),
                    "task": f"pause_{current_side}_limit",
                    "status": "paused",
                    "reason": support_state,
                }
            )
        return {
            "active": True,
            "mode": mode,
            "reason": reason,
            "support_state": support_state,
            "current_side": current_side,
            "blocked_direction": current_side,
            "blocked_entry_types": ["limit", "patience"],
            "blocked_families": blocked_families,
            "blocked_sources": blocked_sources,
            "preferred_families": preferred_families,
            "preferred_sources": preferred_sources,
            "state_label": current_label,
            "opposite_state_label": opposite_label,
            "day_type": day_type,
            "follow_up_plan": str(current_state.get("follow_up_plan") or "").strip().lower(),
            "opposite_follow_up_plan": str(opposite_state.get("follow_up_plan") or "").strip().lower(),
            "snapshot_run_id": str(snapshot.get("run_id") or ""),
            "snapshot_last_event_utc": str(snapshot.get("last_event_utc") or ""),
            "trigger_ts": round(_iso_to_ms(_iso(now)) / 1000.0, 3),
            "hold_min": hold_min,
            "remaining_min": float(hold_min),
            "hold_until_utc": _iso(hold_until),
            "pause_min": hold_min,
            "pause_until_utc": _iso(hold_until),
            "pair_risk_cap": {
                "enabled": bool(getattr(config, "CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True)),
                "max_risk_usd": float(getattr(config, "CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0) or 3.0),
                "min_risk_usd": float(getattr(config, "CTRADER_XAU_PAIR_RISK_MIN_USD", 0.15) or 0.15),
            },
            "coach_traders": coach_traders,
            "trader_assignments": trader_assignments,
            "snapshot_features": {
                "day_type": day_type,
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
                "delta_proxy": round(float(features.get("delta_proxy", 0.0) or 0.0), 4),
                "depth_imbalance": round(float(features.get("depth_imbalance", 0.0) or 0.0), 4),
                "spread_expansion": round(float(features.get("spread_expansion", 1.0) or 1.0), 4),
            },
            "pressure": pressure,
        }

    @staticmethod
    def _trade_direction(value: str) -> str:
        token = str(value or "").strip().lower()
        if token in {"buy", "long"}:
            return "long"
        if token in {"sell", "short"}:
            return "short"
        return ""

    @staticmethod
    def _stale_ttl_min_from_journal(row: dict | sqlite3.Row | None) -> int:
        item = dict(row or {})
        text = " ".join(
            [
                str(item.get("response_json") or ""),
                str(item.get("execution_meta_json") or ""),
            ]
        ).lower()
        match = re.search(r"stale_ttl:(\d+)m", text)
        return max(0, _safe_int(match.group(1), 0)) if match else 0

    @staticmethod
    def _journal_recent_event_ts(row: dict | sqlite3.Row | None) -> float:
        item = dict(row or {})
        created_ts = _safe_float(item.get("created_ts"), 0.0)
        if created_ts <= 0.0:
            created_ts = _iso_to_ms(str(item.get("created_utc") or "")) / 1000.0
        status = str(item.get("status") or "").strip().lower()
        execution_meta = _safe_json_dict(str(item.get("execution_meta_json") or ""))
        if status == "closed":
            closed = dict(execution_meta.get("closed") or {})
            closed_ts = _iso_to_ms(str(closed.get("execution_utc") or "")) / 1000.0
            if closed_ts > 0:
                return closed_ts
        stale_ttl_min = TradingManagerAgent._stale_ttl_min_from_journal(item)
        if status in {"canceled", "filtered", "expired"} and stale_ttl_min > 0 and created_ts > 0:
            return created_ts + float(stale_ttl_min * 60)
        return created_ts

    def _derive_xau_execution_directive_recommendation(self, *, regime_transition: dict | None = None) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED", True))
        if (not enabled) or (not bool(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED", True))):
            return {}
        pause_families = {
            str(token or "").strip().lower()
            for token in self._parse_family_csv(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_FAMILIES", ""))
            if str(token or "").strip()
        }
        if not pause_families:
            return {}
        pause_min = max(1, int(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN", 20) or 20))
        lookback_min = max(
            pause_min + max(5, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_SCALP_MIN", 45) or 45)),
            int(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_LOOKBACK_MIN", 95) or 95),
        )
        cutoff_iso = _ms_to_iso(max(0, _iso_to_ms(_iso(_utc_now())) - (lookback_min * 60 * 1000)))
        best: dict = {}
        with closing(self._connect_ctrader()) as conn:
            rows = conn.execute(
                """
                SELECT created_ts, created_utc, source, status, direction, entry_type,
                       request_json, response_json, execution_meta_json
                  FROM execution_journal
                 WHERE UPPER(COALESCE(symbol,''))='XAUUSD'
                   AND COALESCE(created_utc,'')>=?
                 ORDER BY id DESC
                 LIMIT 160
                """,
                (cutoff_iso,),
            ).fetchall()
        grouped: dict[str, list[dict]] = {}
        for row in list(rows or []):
            request_json = str(row["request_json"] or "{}")
            request_payload = _safe_json_dict(request_json)
            req_ctx = _extract_request_context(request_json)
            req_payload = dict(req_ctx.get("payload") or {})
            raw_scores = dict(req_ctx.get("raw_scores") or {})
            source = _norm_source(
                row["source"]
                or request_payload.get("source")
                or req_payload.get("source")
                or ""
            )
            run_id = str(
                request_payload.get("signal_run_id")
                or req_payload.get("signal_run_id")
                or raw_scores.get("signal_run_id")
                or request_payload.get("client_order_id")
                or req_payload.get("client_order_id")
                or ""
            ).strip()
            if not source or not run_id:
                continue
            family = str(
                raw_scores.get("strategy_family")
                or raw_scores.get("family")
                or live_profile_autopilot._strategy_family_for_source("XAUUSD", source)
                or ""
            ).strip().lower()
            if not family:
                continue
            status = str(row["status"] or "").strip().lower()
            direction = self._trade_direction(row["direction"] or request_payload.get("direction") or req_payload.get("direction") or "")
            entry_type = str(row["entry_type"] or request_payload.get("entry_type") or req_payload.get("entry_type") or "").strip().lower()
            execution_meta = _safe_json_dict(str(row["execution_meta_json"] or ""))
            if bool(execution_meta.get("exclude_from_training")):
                continue
            closed = dict(execution_meta.get("closed") or {})
            grouped.setdefault(run_id, []).append(
                {
                    "run_id": run_id,
                    "source": source,
                    "family": family,
                    "status": status,
                    "direction": direction,
                    "entry_type": entry_type,
                    "pnl_usd": _safe_float(closed.get("pnl_usd"), 0.0),
                    "event_ts": self._journal_recent_event_ts(row),
                    "stale_ttl_min": self._stale_ttl_min_from_journal(row),
                }
            )

        now_ts = _iso_to_ms(_iso(_utc_now())) / 1000.0
        for run_id, items in list(grouped.items()):
            support_items = [
                item for item in list(items or [])
                if item.get("family") == "xau_scalp_flow_short_sidecar"
                and (
                    (str(item.get("status") or "") == "closed" and float(item.get("pnl_usd", 0.0) or 0.0) > 0.0)
                    or (
                        str(item.get("status") or "") in {"canceled", "filtered", "expired"}
                        and int(item.get("stale_ttl_min", 0) or 0) > 0
                    )
                )
            ]
            fail_items = [
                item for item in list(items or [])
                if str(item.get("family") or "") in pause_families
                and str(item.get("direction") or "") == "short"
                and str(item.get("entry_type") or "") in {"limit", "patience"}
                and str(item.get("status") or "") == "closed"
                and float(item.get("pnl_usd", 0.0) or 0.0) < 0.0
            ]
            if not support_items or not fail_items:
                continue
            trigger_ts = max(float(item.get("event_ts", 0.0) or 0.0) for item in support_items + fail_items)
            remain_sec = (pause_min * 60.0) - max(0.0, now_ts - trigger_ts)
            if remain_sec <= 0.0:
                continue
            if best and trigger_ts <= float(best.get("trigger_ts", 0.0) or 0.0):
                continue
            support_state = (
                "fss_win"
                if any(str(item.get("status") or "") == "closed" and float(item.get("pnl_usd", 0.0) or 0.0) > 0.0 for item in support_items)
                else "fss_stale_cancel"
            )
            blocked_families = sorted(
                {
                    str(item.get("family") or "").strip().lower()
                    for item in list(fail_items or [])
                    if str(item.get("family") or "").strip()
                }
            )
            blocked_sources = sorted(
                {
                    str(item.get("source") or "").strip().lower()
                    for item in list(fail_items or [])
                    if str(item.get("source") or "").strip()
                }
            )
            preferred_families = ["xau_scalp_flow_short_sidecar"]
            preferred_sources = ["scalp_xauusd:fss:canary"]
            coach_traders = [
                (
                    "manager directive: pause short-limit "
                    f"{','.join(blocked_families[:3])} for {round(remain_sec / 60.0, 1)}m"
                ),
                "manager directive: let scalp_xauusd:fss:canary lead confirmation shorts",
            ]
            if bool(getattr(config, "CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True)):
                coach_traders.append(
                    "manager directive: keep same-run pair risk capped at "
                    f"{float(getattr(config, 'CTRADER_XAU_PAIR_RISK_MAX_USD', 3.0) or 3.0):.2f}$"
                )
            trader_assignments = [
                {
                    "source": "scalp_xauusd:fss:canary",
                    "family": "xau_scalp_flow_short_sidecar",
                    "task": "lead_confirmation_short",
                    "status": "priority",
                    "reason": support_state,
                }
            ]
            for source in blocked_sources:
                trader_assignments.append(
                    {
                        "source": source,
                        "family": str(
                            live_profile_autopilot._strategy_family_for_source("XAUUSD", source) or ""
                        ).strip().lower(),
                        "task": "pause_short_limit",
                        "status": "paused",
                        "reason": support_state,
                    }
                )
            pair_risk_cap = {
                "enabled": bool(getattr(config, "CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True)),
                "max_risk_usd": float(getattr(config, "CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0) or 3.0),
                "min_risk_usd": float(getattr(config, "CTRADER_XAU_PAIR_RISK_MIN_USD", 0.15) or 0.15),
            }
            best = {
                "active": True,
                "mode": "family_disagreement_limit_pause",
                "reason": "confirmation short held while short-limit lanes failed on the same run",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": blocked_families,
                "blocked_sources": blocked_sources,
                "preferred_families": preferred_families,
                "preferred_sources": preferred_sources,
                "support_state": support_state,
                "trigger_run_id": run_id,
                "pause_min": pause_min,
                "remaining_min": round(remain_sec / 60.0, 1),
                "pause_until_utc": _ms_to_iso(int((trigger_ts + (pause_min * 60.0)) * 1000.0)),
                "trigger_ts": round(trigger_ts, 3),
                "pair_risk_cap": pair_risk_cap,
                "coach_traders": coach_traders,
                "trader_assignments": trader_assignments,
            }
        if best:
            return best
        transition = dict(regime_transition or {})
        if not bool(transition.get("active")):
            return {}
        return {
            "active": True,
            "mode": str(transition.get("mode") or "live_range_transition_limit_pause"),
            "reason": str(transition.get("reason") or ""),
            "blocked_direction": str(transition.get("blocked_direction") or ""),
            "blocked_entry_types": list(transition.get("blocked_entry_types") or []),
            "blocked_families": list(transition.get("blocked_families") or []),
            "blocked_sources": list(transition.get("blocked_sources") or []),
            "preferred_families": list(transition.get("preferred_families") or []),
            "preferred_sources": list(transition.get("preferred_sources") or []),
            "support_state": str(transition.get("support_state") or ""),
            "trigger_run_id": str(transition.get("snapshot_run_id") or ""),
            "pause_min": int(transition.get("pause_min", transition.get("hold_min", 0)) or 0),
            "remaining_min": float(transition.get("remaining_min", 0.0) or 0.0),
            "pause_until_utc": str(transition.get("pause_until_utc") or transition.get("hold_until_utc") or ""),
            "trigger_ts": float(transition.get("trigger_ts", 0.0) or 0.0),
            "pair_risk_cap": dict(transition.get("pair_risk_cap") or {}),
            "coach_traders": list(transition.get("coach_traders") or []),
            "trader_assignments": list(transition.get("trader_assignments") or []),
        }

    def _derive_xau_micro_regime_refresh(self, *, symbol_closed: list[dict]) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_MICRO_REGIME_REFRESH_ENABLED", True)):
            return {}
        window_min = max(3, int(getattr(config, "TRADING_MANAGER_XAU_MICRO_REGIME_WINDOW_MIN", 12) or 12))
        min_resolved = max(1, int(getattr(config, "TRADING_MANAGER_XAU_MICRO_REGIME_MIN_RESOLVED", 3) or 3))
        now_ms = _iso_to_ms(_iso(_utc_now()))
        cutoff_ms = max(0, now_ms - (window_min * 60 * 1000))
        buckets = {
            "long": {"resolved": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0, "families": set(), "sources": set()},
            "short": {"resolved": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0, "families": set(), "sources": set()},
        }
        recent_rows: list[dict] = []
        for row in list(symbol_closed or []):
            closed_ms = _iso_to_ms(str(row.get("closed_utc") or row.get("created_utc") or ""))
            if closed_ms <= 0 or closed_ms < cutoff_ms:
                continue
            side = self._trade_direction(str(row.get("direction") or ""))
            if side not in buckets:
                continue
            bucket = buckets[side]
            pnl = _safe_float(row.get("pnl_usd"), 0.0)
            outcome = _safe_int(row.get("outcome"), -1)
            bucket["resolved"] += 1
            bucket["pnl_usd"] += pnl
            if outcome == 1 or pnl > 0.0:
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1
            family = str(row.get("family") or "").strip().lower()
            source = str(row.get("source") or "").strip().lower()
            if family:
                bucket["families"].add(family)
            if source:
                bucket["sources"].add(source)
            recent_rows.append(
                {
                    "closed_utc": str(row.get("closed_utc") or ""),
                    "source": source,
                    "family": family,
                    "direction": side,
                    "pnl_usd": round(pnl, 2),
                    "outcome": outcome,
                }
            )
        dominant_direction = ""
        dominant_bucket: dict = {}
        if any(int(bucket["resolved"] or 0) >= min_resolved for bucket in buckets.values()):
            ranked = sorted(
                buckets.items(),
                key=lambda item: (
                    int(item[1]["resolved"] or 0),
                    abs(float(item[1]["pnl_usd"] or 0.0)),
                    int(item[1]["losses"] or 0),
                ),
                reverse=True,
            )
            dominant_direction, dominant_bucket = ranked[0]
        active = bool(dominant_direction and int((dominant_bucket or {}).get("resolved", 0) or 0) >= min_resolved)
        state_label = "insufficient"
        if active:
            pnl = float(dominant_bucket.get("pnl_usd", 0.0) or 0.0)
            losses = int(dominant_bucket.get("losses", 0) or 0)
            resolved = int(dominant_bucket.get("resolved", 0) or 0)
            if pnl < 0.0 and losses >= max(2, resolved // 2):
                state_label = f"{dominant_direction}_cluster_loss"
            elif pnl > 0.0:
                state_label = f"{dominant_direction}_dominant"
            else:
                state_label = f"{dominant_direction}_mixed"
        return {
            "active": active,
            "window_min": window_min,
            "min_resolved": min_resolved,
            "state_label": state_label,
            "dominant_direction": dominant_direction,
            "dominant_bucket": {
                "resolved": int((dominant_bucket or {}).get("resolved", 0) or 0),
                "wins": int((dominant_bucket or {}).get("wins", 0) or 0),
                "losses": int((dominant_bucket or {}).get("losses", 0) or 0),
                "pnl_usd": round(float((dominant_bucket or {}).get("pnl_usd", 0.0) or 0.0), 2),
                "families": sorted(list((dominant_bucket or {}).get("families") or [])),
                "sources": sorted(list((dominant_bucket or {}).get("sources") or [])),
            },
            "direction_buckets": {
                key: {
                    "resolved": int(bucket["resolved"] or 0),
                    "wins": int(bucket["wins"] or 0),
                    "losses": int(bucket["losses"] or 0),
                    "pnl_usd": round(float(bucket["pnl_usd"] or 0.0), 2),
                    "families": sorted(list(bucket["families"] or [])),
                }
                for key, bucket in buckets.items()
            },
            "recent_rows": recent_rows[:8],
        }

    def _derive_xau_cluster_loss_guard_recommendation(self, *, micro_regime_refresh: dict) -> dict:
        if not bool(getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_ENABLED", True)):
            return {}
        regime = dict(micro_regime_refresh or {})
        if not bool(regime.get("active")):
            return {}
        dominant_direction = str(regime.get("dominant_direction") or "").strip().lower()
        bucket = dict(regime.get("dominant_bucket") or {})
        if dominant_direction not in {"long", "short"}:
            return {}
        resolved = int(bucket.get("resolved", 0) or 0)
        losses = int(bucket.get("losses", 0) or 0)
        pnl = float(bucket.get("pnl_usd", 0.0) or 0.0)
        families = [str(item or "").strip().lower() for item in list(bucket.get("families") or []) if str(item or "").strip()]
        min_resolved = max(1, int(getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_RESOLVED", 3) or 3))
        min_losses = max(1, int(getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_LOSSES", 2) or 2))
        min_distinct = max(1, int(getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_DISTINCT_FAMILIES", 2) or 2))
        max_pnl = float(getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MAX_PNL_USD", -5.0) or -5.0)
        if resolved < min_resolved or losses < min_losses or len(families) < min_distinct or pnl > max_pnl:
            return {}
        return {
            "active": True,
            "mode": "same_side_cluster_loss_guard",
            "reason": (
                f"recent {dominant_direction} cluster lost {losses}/{resolved} across "
                f"{len(families)} families pnl {pnl:.2f}"
            ),
            "blocked_direction": dominant_direction,
            "window_min": int(regime.get("window_min", getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_WINDOW_MIN", 12)) or getattr(config, "TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_WINDOW_MIN", 12)),
            "losses": losses,
            "resolved": resolved,
            "pnl_usd": round(pnl, 2),
            "families": families,
        }

    def _build_recent_order_reviews(self, *, symbol: str, rows: list[dict], shock: dict | None, limit: int = 5) -> list[dict]:
        shock_start = _iso_to_ms(str((shock or {}).get("start_utc") or ""))
        shock_end = _iso_to_ms(str((shock or {}).get("end_utc") or ""))
        review_rows = sorted(
            list(rows or []),
            key=lambda row: (_iso_to_ms(str(row.get("closed_utc") or "")), _iso_to_ms(str(row.get("created_utc") or ""))),
            reverse=True,
        )
        out: list[dict] = []
        for row in review_rows[: max(1, int(limit or 5))]:
            family = str(row.get("family") or "").strip().lower()
            entry_type = str(row.get("entry_type") or "").strip().lower() or "unknown"
            direction = str(row.get("direction") or "").strip().lower() or "unknown"
            pnl = _safe_float(row.get("pnl_usd"), 0.0)
            conf = _safe_float(row.get("confidence"), 0.0)
            created_ms = _iso_to_ms(str(row.get("created_utc") or ""))
            in_shock = bool(shock_start and shock_end and shock_start <= created_ms <= shock_end)
            tags: list[str] = []
            if pnl >= 0:
                tags.append("winner")
            else:
                tags.append("loser")
            if entry_type:
                tags.append(entry_type)
            if in_shock:
                tags.append("shock_window")
            diagnosis = "trade behaved normally"
            if pnl < 0:
                if in_shock:
                    diagnosis = "entered into repricing/shock window before market stabilized"
                elif family == "xau_scheduled_trend" and entry_type == "market":
                    diagnosis = "scheduled market entry paid up before retest confirmation"
                elif family == "xau_scalp_pullback_limit":
                    diagnosis = "pullback limit was filled before the retest showed absorption"
                elif family == "xau_scalp_tick_depth_filter":
                    diagnosis = "tick/depth confirmation passed, but continuation failed to extend after entry"
                else:
                    diagnosis = "entry had insufficient follow-through after fill"
            else:
                if family == "xau_scheduled_trend":
                    diagnosis = "scheduled trend entry aligned with higher-timeframe flow and held follow-through"
                elif family == "xau_scalp_tick_depth_filter":
                    diagnosis = "tick/depth filter aligned with live order-flow and short-horizon continuation"
                elif family == "xau_scalp_flow_short_sidecar":
                    diagnosis = "continuation sidecar captured the post-break follow-through"
                elif family == "xau_scalp_microtrend_follow_up":
                    diagnosis = "microtrend follow-up entered after the chart state confirmed continuation"
                elif family == "xau_scalp_pullback_limit":
                    diagnosis = "retest entry filled after absorption and resumed in the intended direction"
            out.append(
                {
                    "closed_utc": str(row.get("closed_utc") or ""),
                    "source": str(row.get("source") or ""),
                    "family": family,
                    "direction": direction,
                    "entry_type": entry_type,
                    "pattern": str(row.get("pattern") or ""),
                    "session": str(row.get("session") or ""),
                    "confidence": round(conf, 2),
                    "pnl_usd": round(pnl, 2),
                    "tags": tags,
                    "diagnosis": diagnosis,
                }
            )
        return out

    def _derive_why_no_trade_diagnostics(
        self,
        *,
        symbol: str,
        symbol_closed: list[dict],
        open_positions: list[dict],
        open_orders: list[dict],
        chart_state_memory: dict,
        experiment_report: dict,
        opportunity_feed_symbol: dict,
        selected_family: str,
        manager_state: dict,
    ) -> dict:
        xau_swarm_support_mode = _norm_symbol(symbol) == "XAUUSD" and bool(getattr(config, "TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False))
        symbol_states = self._symbol_states(chart_state_memory, symbol, follow_up_only=False)
        top_state = dict(symbol_states[0] if symbol_states else {})
        follow_up_state = next((dict(row) for row in list(symbol_states or []) if bool(row.get("follow_up_candidate"))), {})
        source_rows = {
            _norm_source(row.get("source")): dict(row)
            for row in list((experiment_report or {}).get("sources") or [])
            if _norm_symbol(row.get("symbol")) == _norm_symbol(symbol)
        }
        active_positions = len(list(open_positions or []))
        active_orders = len(list(open_orders or []))
        likely_blockers: list[str] = []
        coaching: list[str] = []
        status = "active" if active_positions or active_orders else "idle"
        slot_budget: dict = {}
        if _norm_symbol(symbol) == "XAUUSD":
            parallel_state = dict((manager_state or {}).get("xau_parallel_families") or {})
            cluster_guard = dict((manager_state or {}).get("xau_cluster_loss_guard") or {})
            slot_budget = {
                "mode": str(parallel_state.get("mode") or ""),
                "max_same_direction_families": int(parallel_state.get("max_same_direction_families", getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION", 3)) or 3),
                "allowed_families": list(parallel_state.get("allowed_families") or []),
                "used_active_slots": len(
                    {
                        str(row.get("source") or "").strip().lower()
                        for row in list(open_positions or []) + list(open_orders or [])
                        if str(row.get("direction") or "").strip().lower() in {"buy", "sell", "long", "short"}
                    }
                ),
            }
            shock_state = dict((manager_state or {}).get("xau_shock_profile") or {})
            if str(shock_state.get("status") or "") == "active":
                shock_mode = str(shock_state.get("mode") or "shock")
                follow_dir = str(follow_up_state.get("direction") or top_state.get("direction") or "").strip().lower()
                follow_day = str(follow_up_state.get("day_type") or top_state.get("day_type") or "").strip().lower()
                short_continuation_exempt = (
                    shock_mode in {"pre_event_caution", "shock_protect"}
                    and follow_dir == "short"
                    and follow_day in {"trend", "repricing", "fast_expansion"}
                )
                if short_continuation_exempt:
                    coaching.append(f"{shock_mode} is active, but XAU short continuation remains eligible with reduced risk")
                else:
                    coaching.append(
                        f"{shock_mode} is active: avoid forced continuation and lean on confirmation/range-repair desks"
                    )
            blocked_direction = str(cluster_guard.get("blocked_direction") or "").strip().lower()
            if str(cluster_guard.get("status") or "") == "active" and blocked_direction:
                top_direction = str(top_state.get("direction") or "").strip().lower()
                if blocked_direction == top_direction and not active_positions and not active_orders:
                    likely_blockers.append(
                        f"cluster-loss guard blocks fresh {blocked_direction} pile-on after recent same-side losses"
                    )
                coaching.append(f"cluster-loss guard active for {blocked_direction} until micro-regime improves")
            if follow_up_state and not active_positions and not active_orders:
                best_family = str(((follow_up_state.get("best_family") or {}).get("family")) or "").strip().lower()
                best_family_source = {
                    "xau_scalp_microtrend": "scalp_xauusd:mfu:canary",
                    "xau_scalp_microtrend_follow_up": "scalp_xauusd:mfu:canary",
                    "xau_scalp_flow_short_sidecar": "scalp_xauusd:fss:canary",
                    "xau_scalp_tick_depth_filter": "scalp_xauusd:td:canary",
                    "xau_scalp_pullback_limit": "scalp_xauusd:pb:canary",
                }.get(best_family, "")
                best_family_resolved = int(
                    (((source_rows.get(best_family_source) or {}).get("closed_total") or {}).get("resolved", 0) or 0)
                )
                if best_family and best_family_source and best_family_resolved == 0:
                    likely_blockers.append(f"follow-up state exists but {best_family} still has no live resolved sample")
            if slot_budget and int(slot_budget.get("used_active_slots", 0) or 0) >= int(slot_budget.get("max_same_direction_families", 1) or 1):
                likely_blockers.append("family slot budget is full for the current direction")
        if not active_positions and not active_orders:
            status = "undertrading"
            if not symbol_closed:
                likely_blockers.append("no recent live closes for this symbol in the current window")
        if follow_up_state:
            coaching.append(
                f"follow-up state {str(follow_up_state.get('state_label') or '-')}"
                f" {str(follow_up_state.get('direction') or '-')}"
                f" day={str(follow_up_state.get('day_type') or '-')}"
            )
        if opportunity_feed_symbol:
            priority = list((opportunity_feed_symbol.get("priority_families") or []))
            if priority:
                if xau_swarm_support_mode:
                    coaching.append(f"manager support mode active: {', '.join(priority[:5])}")
                else:
                    coaching.append(f"manager priority now: {', '.join(priority[:3])}")
            if xau_swarm_support_mode:
                support_all = list(opportunity_feed_symbol.get("support_all_families") or [])
                if support_all:
                    coaching.append(f"manager support-all families: {', '.join(support_all[:5])}")
            elif selected_family and priority and str(priority[0] or "") != str(selected_family or ""):
                likely_blockers.append(
                    f"selected family {selected_family or '-'} lags the current manager priority {priority[0]}"
                )
        if top_state and not likely_blockers:
            likely_blockers.append(
                f"no trade fired even though top state {str(top_state.get('state_label') or '-')}"
                f" day={str(top_state.get('day_type') or '-')}"
                " suggests opportunity; execution gates likely remained too tight"
            )
        return {
            "status": status,
            "top_state": {
                "state_label": str(top_state.get("state_label") or ""),
                "direction": str(top_state.get("direction") or ""),
                "day_type": str(top_state.get("day_type") or ""),
                "state_score": round(float(top_state.get("state_score", 0.0) or 0.0), 2),
                "follow_up_candidate": bool(top_state.get("follow_up_candidate")),
            } if top_state else {},
            "slot_budget": slot_budget,
            "likely_blockers": likely_blockers[:5],
            "coaching": coaching[:4],
        }

    @staticmethod
    def _family_priority_add(target: dict[str, float], family: str, score: float) -> None:
        token = str(family or "").strip().lower()
        if not token:
            return
        try:
            val = float(score or 0.0)
        except Exception:
            val = 0.0
        if val <= 0.0:
            return
        target[token] = max(float(target.get(token, 0.0) or 0.0), val)

    def _derive_opportunity_feed(self, *, symbol_rows: list[dict], chart_state_memory: dict, experiment_report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED", True))
        out = {
            "enabled": enabled,
            "cadence_min": max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15)),
            "symbols": {},
        }
        if not enabled:
            return out
        min_state_score = float(getattr(config, "TRADING_MANAGER_OPPORTUNITY_FEED_MIN_STATE_SCORE", 24.0) or 24.0)
        min_state_resolved = max(1, int(getattr(config, "TRADING_MANAGER_OPPORTUNITY_FEED_MIN_RESOLVED", 2) or 2))
        topk = max(1, int(getattr(config, "TRADING_MANAGER_OPPORTUNITY_FEED_TOPK", 3) or 3))
        states_by_symbol: dict[str, list[dict]] = {}
        for row in list((chart_state_memory or {}).get("states") or []):
            if not isinstance(row, dict):
                continue
            symbol = _norm_symbol(row.get("symbol"))
            if not symbol:
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_state_resolved:
                continue
            if float(row.get("state_score", 0.0) or 0.0) < min_state_score:
                continue
            if not bool(row.get("profitable_state")) and float(stats.get("pnl_usd", 0.0) or 0.0) <= 0.0:
                continue
            states_by_symbol.setdefault(symbol, []).append(dict(row))
        for symbol, rows in list(states_by_symbol.items()):
            rows.sort(
                key=lambda item: (
                    float(item.get("state_score", 0.0) or 0.0),
                    float(((item.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                    int(((item.get("stats") or {}).get("resolved", 0) or 0)),
                ),
                reverse=True,
            )
        experiment_sources = {
            (_norm_symbol(row.get("symbol")), _norm_source(row.get("source"))): dict(row)
            for row in list((experiment_report or {}).get("sources") or [])
            if isinstance(row, dict)
        }
        for symbol_row in list(symbol_rows or []):
            symbol = _norm_symbol(symbol_row.get("symbol"))
            if not symbol:
                continue
            family_priority_map: dict[str, float] = {}
            agent_targets: list[dict] = []
            coaching: list[str] = []
            selected_family = str(symbol_row.get("selected_family") or "").strip().lower()
            best_family_today = dict(symbol_row.get("best_family_today") or {})
            winner_memory = dict(symbol_row.get("winner_memory_reference") or {})
            execution_directive = dict(symbol_row.get("execution_directive_recommendations") or {})
            top_states = list(states_by_symbol.get(symbol) or [])[:topk]
            xau_swarm_support_mode = symbol == "XAUUSD" and bool(getattr(config, "TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False))
            xau_swarm_families = self._xau_swarm_active_families(self._allowed_xau_routing_families()) if xau_swarm_support_mode else []

            if symbol == "XAUUSD":
                if xau_swarm_support_mode and xau_swarm_families:
                    for fam in list(xau_swarm_families or []):
                        self._family_priority_add(family_priority_map, fam, 85.0)
                    coaching.append("swarm_support_all: keep all active XAU families warm for broader sample collection")
                else:
                    self._family_priority_add(family_priority_map, "xau_scheduled_trend", 95.0)
                    coaching.append("scheduled_core every 5m: keep scanning trend continuation and retest opportunities")
            if selected_family and not xau_swarm_support_mode:
                self._family_priority_add(family_priority_map, selected_family, 82.0)
            if best_family_today and not xau_swarm_support_mode:
                best_family = self._family_from_bucket(best_family_today)
                pnl = float(best_family_today.get("pnl_usd", 0.0) or 0.0)
                resolved = int(best_family_today.get("resolved", 0) or 0)
                if best_family and resolved >= 2 and pnl > 0.0:
                    self._family_priority_add(family_priority_map, best_family, 88.0 + min(8.0, pnl / 6.0))
                    agent_targets.append(
                        {
                            "type": "today_leader",
                            "family": best_family,
                            "resolved": resolved,
                            "pnl_usd": round(pnl, 2),
                        }
                    )
                    coaching.append(f"prefer today leader {best_family} from live pnl {pnl:.2f}")
            if winner_memory:
                memory_family = str(winner_memory.get("family") or "").strip().lower()
                if memory_family and not xau_swarm_support_mode:
                    self._family_priority_add(family_priority_map, memory_family, 76.0)
                if memory_family:
                    coaching.append(
                        f"reuse winner memory {memory_family} in {str(winner_memory.get('session') or '-')} "
                        f"{str(winner_memory.get('direction') or '-')}"
                    )
            for state in top_states:
                best_family = str(((state.get("best_family") or {}).get("family")) or "").strip().lower()
                state_score = float(state.get("state_score", 0.0) or 0.0)
                stats = dict(state.get("stats") or {})
                resolved = int(stats.get("resolved", 0) or 0)
                pnl = float(stats.get("pnl_usd", 0.0) or 0.0)
                if best_family and not xau_swarm_support_mode:
                    self._family_priority_add(family_priority_map, best_family, 70.0 + min(20.0, state_score / 2.0))
                if symbol == "XAUUSD" and bool(state.get("follow_up_candidate")):
                    direction = str(state.get("direction") or "").strip().lower()
                    state_label = str(state.get("state_label") or "").strip().lower()
                    if direction == "short" and state_label == "continuation_drive" and not xau_swarm_support_mode:
                        self._family_priority_add(family_priority_map, "xau_scalp_flow_short_sidecar", 93.0)
                    if state_label in {"range_probe", "reversal_exhaustion"} and not xau_swarm_support_mode:
                        self._family_priority_add(family_priority_map, "xau_scalp_range_repair", 92.0)
                        coaching.append("range repair lane: prefer conservative limit-only repair in sideway/reversal exhaustion")
                    if not xau_swarm_support_mode:
                        self._family_priority_add(family_priority_map, "xau_scalp_microtrend_follow_up", 90.0)
                agent_targets.append(
                    {
                        "type": "chart_state",
                        "family": best_family,
                        "state_label": str(state.get("state_label") or ""),
                        "direction": str(state.get("direction") or ""),
                        "session": str(state.get("session") or ""),
                        "plan": str(state.get("follow_up_plan") or ""),
                        "state_score": round(state_score, 2),
                        "resolved": resolved,
                        "pnl_usd": round(pnl, 2),
                    }
                )
                coaching.append(
                    f"state {str(state.get('state_label') or '-')} {str(state.get('direction') or '-')} -> "
                    f"{best_family or '-'} | {str(state.get('follow_up_plan') or '-')}"
                )
            if symbol == "XAUUSD":
                for source_name, bonus_family, bonus_score in (
                    ("scalp_xauusd:td:canary", "xau_scalp_tick_depth_filter", 84.0),
                    ("scalp_xauusd:fss:canary", "xau_scalp_flow_short_sidecar", 89.0),
                    ("scalp_xauusd:mfu:canary", "xau_scalp_microtrend_follow_up", 86.0),
                    ("scalp_xauusd:rr:canary", "xau_scalp_range_repair", 87.0),
                    ("scalp_xauusd:pb:canary", "xau_scalp_pullback_limit", 74.0),
                ):
                    source_row = dict(experiment_sources.get((symbol, _norm_source(source_name))) or {})
                    closed_total = dict(source_row.get("closed_total") or {})
                    if (not xau_swarm_support_mode) and int(closed_total.get("resolved", 0) or 0) >= 2 and float(closed_total.get("pnl_usd", 0.0) or 0.0) > 0.0:
                        self._family_priority_add(family_priority_map, bonus_family, bonus_score)
            if symbol == "XAUUSD" and bool(execution_directive.get("active")):
                preferred_families = {
                    str(family or "").strip().lower()
                    for family in list(execution_directive.get("preferred_families") or [])
                    if str(family or "").strip()
                }
                blocked_families = {
                    str(family or "").strip().lower()
                    for family in list(execution_directive.get("blocked_families") or [])
                    if str(family or "").strip()
                }
                for family in list(execution_directive.get("preferred_families") or []):
                    self._family_priority_add(family_priority_map, str(family or ""), 98.0)
                for assignment in list(execution_directive.get("trader_assignments") or [])[:topk]:
                    agent_targets.append({"type": "execution_directive", **dict(assignment or {})})
                coaching.extend(list(execution_directive.get("coach_traders") or [])[:3])
            priority_rows = [
                {"family": fam, "score": round(score, 2)}
                for fam, score in sorted(family_priority_map.items(), key=lambda item: (-float(item[1]), str(item[0])))
            ]
            if symbol == "XAUUSD" and bool(execution_directive.get("active")):
                priority_rows = sorted(
                    list(priority_rows or []),
                    key=lambda row: (
                        0 if str(row.get("family") or "").strip().lower() in preferred_families else 1,
                        1 if str(row.get("family") or "").strip().lower() in blocked_families else 0,
                        -float(row.get("score", 0.0) or 0.0),
                        str(row.get("family") or ""),
                    ),
                )
            if xau_swarm_support_mode:
                priority_rows = [row for row in list(priority_rows or []) if str(row.get("family") or "") in set(xau_swarm_families)]
            else:
                priority_rows = priority_rows[:topk]
            out["symbols"][symbol] = {
                "active": bool(priority_rows),
                "cadence_min": out["cadence_min"],
                "family_priority_map": {row["family"]: row["score"] for row in priority_rows},
                "priority_families": [row["family"] for row in priority_rows],
                "support_all_families": list(xau_swarm_families) if xau_swarm_support_mode else [],
                "agent_targets": agent_targets[:topk],
                "coaching": coaching[: max(topk, 2)],
            }
        return out

    def _apply_opportunity_feed(self, report: dict) -> dict:
        enabled = bool(getattr(config, "TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED", True))
        out = {"enabled": enabled, "status": "disabled" if not enabled else "none", "symbols": 0}
        state = self._load_state()
        if not enabled:
            if state.get("opportunity_feed"):
                state["opportunity_feed"] = {"status": "inactive", "reverted_at": _iso(_utc_now())}
                self._save_state(state)
            return out
        feed = dict((report or {}).get("opportunity_feed") or {})
        symbols = dict(feed.get("symbols") or {})
        state["opportunity_feed"] = {
            "status": "active" if symbols else "idle",
            "generated_at": _iso(_utc_now()),
            "cadence_min": int(feed.get("cadence_min", 5) or 5),
            "symbols": symbols,
        }
        self._save_state(state)
        out.update({"status": "active" if symbols else "idle", "symbols": len(symbols)})
        return out

    def build_report(self, *, hours: int | None = None) -> dict:
        lookback_hours = max(1, int(hours or getattr(config, "TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24) or 24))
        symbols = self._focus_symbols()
        start_utc, now_utc, start_local, now_local = self._window(hours=lookback_hours)
        since_iso = _iso(start_utc)
        reports = self._load_reports()
        macro_ctx = self._load_macro_context()
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "window": {
                "timezone": self._tz_name(),
                "hours": lookback_hours,
                "start_local": start_local.isoformat(),
                "end_local": now_local.isoformat(),
                "start_utc": since_iso,
                "end_utc": _iso(now_utc),
            },
            "summary": {
                "rows": 0,
                "abnormal_excluded": 0,
                "symbols": len(symbols),
                "shock_symbols": 0,
                "open_positions": 0,
                "open_orders": 0,
            },
            "symbols": [],
            "actions": [],
            "refs": {
                "mission_progress_report": str(self._report_path("mission_progress_report")),
                "family_calibration_report": str(self._report_path("family_calibration_report")),
                "winner_memory_library_report": str(self._report_path("winner_memory_library_report")),
                "ctrader_tick_depth_replay_report": str(self._report_path("ctrader_tick_depth_replay_report")),
            },
            "macro_context": {
                "headline_count": len(list(macro_ctx.get("headlines") or [])),
                "impact_entries": len(list(macro_ctx.get("impact_entries") or [])),
                "upcoming_events": len(list(macro_ctx.get("upcoming_events") or [])),
                "sync": dict(macro_ctx.get("sync") or {}),
            },
            "profile_apply": {},
            "reason_memory_apply": {},
            "family_routing_apply": {},
            "opportunity_feed": {},
            "opportunity_feed_apply": {},
            "regime_transition_apply": {},
            "micro_regime_apply": {},
            "cluster_loss_guard_apply": {},
            "execution_directive_apply": {},
            "error": "",
            "parallel_family_apply": {},
            "hedge_lane_apply": {},
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            self._save_report("trading_manager_report", out)
            return out

        rows, collect_summary = self._collect_closed_rows(since_iso=since_iso, symbols=symbols)
        out["summary"]["rows"] = len(rows)
        out["summary"]["abnormal_excluded"] = int(collect_summary.get("abnormal_excluded", 0) or 0)
        family_calibration_index = {
            (str(row.get("symbol") or ""), str(row.get("family") or "")): dict(row)
            for row in list((reports.get("family_calibration_report") or {}).get("families") or [])
        }
        winner_memory_index = dict((reports.get("winner_memory_library_report") or {}).get("top_by_symbol") or {})
        mission_symbol_index = {
            str(row.get("symbol") or ""): dict(row)
            for row in list((reports.get("mission_progress_report") or {}).get("symbols") or [])
        }
        manager_state = self._load_state()

        with closing(self._connect_ctrader()) as conn:
            open_positions = conn.execute(
                f"""
                SELECT source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc
                  FROM ctrader_positions
                 WHERE is_open = 1
                   AND symbol IN ({",".join("?" for _ in symbols)})
                 ORDER BY first_seen_utc ASC, position_id ASC
                """,
                symbols,
            ).fetchall()
            open_orders = conn.execute(
                f"""
                SELECT source, symbol, direction, order_type, entry_price, stop_loss, take_profit, first_seen_utc
                  FROM ctrader_orders
                 WHERE is_open = 1
                   AND symbol IN ({",".join("?" for _ in symbols)})
                 ORDER BY first_seen_utc ASC, order_id ASC
                """,
                symbols,
            ).fetchall()
            out["summary"]["open_positions"] = len(list(open_positions or []))
            out["summary"]["open_orders"] = len(list(open_orders or []))

            symbol_rows: list[dict] = []
            manager_actions: list[dict] = []
            for symbol in symbols:
                symbol_closed = [row for row in list(rows or []) if str(row.get("symbol") or "") == symbol]
                symbol_chart_states = self._symbol_states(reports.get("chart_state_memory_report") or {}, symbol, follow_up_only=False)
                family_rows = self._bucket_rows(symbol_closed, lambda row: (str(row.get("family") or ""),))
                best_family_today = {}
                for candidate in list(family_rows or []):
                    if (
                        int(candidate.get("resolved", 0) or 0) >= 3
                        and float(candidate.get("pnl_usd", 0.0) or 0.0) > 0.0
                    ):
                        best_family_today = dict(candidate)
                        break
                worst_family_today = dict(family_rows[-1]) if family_rows else {}
                shock = self._detect_shock(conn, symbol=symbol, since_iso=since_iso)
                if shock:
                    out["summary"]["shock_symbols"] = int(out["summary"].get("shock_symbols", 0) or 0) + 1
                shock_rows = []
                if shock:
                    shock_start = _iso_to_ms(str(shock.get("start_utc") or ""))
                    shock_end = _iso_to_ms(str(shock.get("end_utc") or ""))
                    shock_pad_ms = max(1, int(getattr(config, "TRADING_MANAGER_EVENT_PAD_MIN", 12) or 12)) * 60000
                    for row in list(symbol_closed or []):
                        created_ms = _iso_to_ms(str(row.get("created_utc") or ""))
                        if (shock_start - shock_pad_ms) <= created_ms <= (shock_end + shock_pad_ms):
                            shock_rows.append(row)
                shock_family_rows = self._bucket_rows(
                    shock_rows,
                    lambda row: (
                        str(row.get("family") or ""),
                        str(row.get("direction") or ""),
                        str(row.get("entry_type") or ""),
                    ),
                )
                best_same_situation = {}
                for candidate in list(shock_family_rows or []):
                    if (
                        int(candidate.get("resolved", 0) or 0) >= 2
                        and float(candidate.get("pnl_usd", 0.0) or 0.0) > 0.0
                        and float(candidate.get("win_rate", 0.0) or 0.0) >= 0.5
                    ):
                        best_same_situation = dict(candidate)
                        break
                losses_same_situation = [row for row in list(shock_rows or []) if _safe_float(row.get("pnl_usd"), 0.0) < 0.0]
                loss_bucket = _new_bucket()
                for row in list(losses_same_situation or []):
                    _update_bucket(loss_bucket, _safe_float(row.get("pnl_usd"), 0.0), _safe_int(row.get("outcome"), -1))
                loss_summary = _finalize_bucket(loss_bucket)
                selected = dict(mission_symbol_index.get(symbol) or {})
                selected_family = str(selected.get("selected_family") or "")
                selected_regime = str(selected.get("selected_regime") or "")
                selected_family_calibration = dict(family_calibration_index.get((symbol, selected_family)) or {})
                scheduled_family_calibration = dict(family_calibration_index.get((symbol, "xau_scheduled_trend")) or {})
                top_memory = dict(winner_memory_index.get(symbol) or {})
                pb_source_stats = {}
                scheduled_source_stats = {}
                if symbol == "XAUUSD":
                    pb_source_stats = self._source_bucket(symbol_closed, "scalp_xauusd:pb:canary")
                    scheduled_source_stats = self._source_bucket(symbol_closed, "xauusd_scheduled:canary", "xauusd_scheduled:winner")
                recommended_actions: list[dict] = []
                manager_findings: list[str] = []
                shock_explanation = self._shock_explanation(shock)
                macro_cause = self._pick_macro_cause(symbol=symbol, shock=shock, macro_ctx=macro_ctx)
                upcoming_events = self._relevant_upcoming_events(symbol=symbol, macro_ctx=macro_ctx)
                upcoming_actions = self._derive_upcoming_event_actions(symbol=symbol, upcoming_events=upcoming_events)
                post_event_learning = self._post_event_learning(symbol=symbol, macro_ctx=macro_ctx)
                if shock_explanation:
                    manager_findings.append(shock_explanation)
                if int(loss_summary.get("resolved", 0) or 0) > 0:
                    manager_findings.append(
                        f"losses during shock: {int(loss_summary.get('losses', 0) or 0)}/"
                        f"{int(loss_summary.get('resolved', 0) or 0)} | pnl "
                        f"{float(loss_summary.get('pnl_usd', 0.0) or 0.0):.2f}"
                    )
                if macro_cause:
                    manager_findings.append(
                        f"probable macro cause: {str(macro_cause.get('source') or '-')}"
                        f" | {str(macro_cause.get('classification_human') or macro_cause.get('reaction_summary') or '-')[:90]}"
                    )
                if symbol == "XAUUSD" and shock:
                    negative_long = any(str(row.get("direction") or "") == "long" and _safe_float(row.get("pnl_usd"), 0.0) < 0.0 for row in list(shock_rows or []))
                    if negative_long and (
                        float(shock.get("depth_imbalance", 0.0) or 0.0) <= -0.05
                        or float(shock.get("move_pct", 0.0) or 0.0) <= -0.80
                    ):
                        recommended_actions.append(
                            {
                                "action": "block_countertrend_long_after_selloff",
                                "minutes": max(10, int(getattr(config, "TRADING_MANAGER_EVENT_WINDOW_MIN", 20) or 20)),
                                "reason": (
                                    f"{shock.get('shock_type')} move={float(shock.get('move_pct', 0.0) or 0.0):.2f}% "
                                    f"depth={float(shock.get('depth_imbalance', 0.0) or 0.0):+.3f}"
                                ),
                            }
                        )
                    if best_same_situation:
                        recommended_actions.append(
                            {
                                "action": "learn_from_best_same_situation",
                                "family": str((best_same_situation.get("key") or [""])[0] or ""),
                                "direction": str((best_same_situation.get("key") or ["", ""])[1] or ""),
                                "entry_type": str((best_same_situation.get("key") or ["", "", ""])[2] or ""),
                                "reason": f"shock pnl {float(best_same_situation.get('pnl_usd', 0.0) or 0.0):.2f}",
                            }
                        )
                        manager_findings.append(
                            f"best same-situation winner: {str((best_same_situation.get('key') or [''])[0] or '-')}"
                            f" | pnl {float(best_same_situation.get('pnl_usd', 0.0) or 0.0):.2f}"
                        )
                    elif int(loss_summary.get("resolved", 0) or 0) > 0:
                        recommended_actions.append(
                            {
                                "action": "tighten_countertrend_scalp_during_repricing_shock",
                                "minutes": max(10, int(getattr(config, "TRADING_MANAGER_EVENT_WINDOW_MIN", 20) or 20)),
                                "reason": (
                                    f"no positive same-situation winner | losses "
                                    f"{int(loss_summary.get('losses', 0) or 0)}/{int(loss_summary.get('resolved', 0) or 0)} "
                                    f"| pnl {float(loss_summary.get('pnl_usd', 0.0) or 0.0):.2f}"
                                ),
                            }
                        )
                        manager_findings.append(
                            "no positive same-situation winner found; tighten or block countertrend scalp entries during repricing shock"
                        )
                for action in list(upcoming_actions or []):
                    recommended_actions.append(dict(action))
                if upcoming_events:
                    nxt = dict(upcoming_events[0] or {})
                    manager_findings.append(
                        f"next event: {str(nxt.get('title') or '-')[:70]} in {int(nxt.get('minutes_to_event', 0) or 0)}m"
                    )
                if best_family_today and str((best_family_today.get("key") or [""])[0] or "") != selected_family:
                    recommended_actions.append(
                        {
                            "action": "prefer_today_leader_family",
                            "family": str((best_family_today.get("key") or [""])[0] or ""),
                            "reason": f"today pnl {float(best_family_today.get('pnl_usd', 0.0) or 0.0):.2f} vs selected {selected_family or '-'}",
                        }
                    )
                    manager_findings.append(
                        f"today leader family: {str((best_family_today.get('key') or [''])[0] or '-')}"
                        f" | pnl {float(best_family_today.get('pnl_usd', 0.0) or 0.0):.2f}"
                    )
                if top_memory:
                    recommended_actions.append(
                        {
                            "action": "reuse_market_beating_memory",
                            "family": str(top_memory.get("family") or ""),
                            "session": str(top_memory.get("session") or ""),
                            "direction": str(top_memory.get("direction") or ""),
                        }
                    )
                    manager_findings.append(
                        f"winner memory reference: {str(top_memory.get('family') or '-')} | "
                        f"{str(top_memory.get('session') or '-')}/{str(top_memory.get('direction') or '-')}"
                    )
                if symbol == "XAUUSD" and int(pb_source_stats.get("resolved", 0) or 0) > 0 and int(scheduled_source_stats.get("resolved", 0) or 0) > 0:
                    manager_findings.append(
                        f"pb vs scheduled: pb {float(pb_source_stats.get('pnl_usd', 0.0) or 0.0):.2f}/"
                        f"{int(pb_source_stats.get('resolved', 0) or 0)} vs scheduled "
                        f"{float(scheduled_source_stats.get('pnl_usd', 0.0) or 0.0):.2f}/"
                        f"{int(scheduled_source_stats.get('resolved', 0) or 0)}"
                    )
                profile_recommendations = {}
                reason_memory_recommendations = {}
                family_routing_recommendations = {}
                opportunity_sidecar_recommendations = {}
                parallel_family_recommendations = {}
                hedge_lane_recommendations = {}
                opportunity_bypass_recommendations = {}
                regime_transition_recommendations = {}
                micro_regime_refresh = {}
                cluster_loss_guard_recommendations = {}
                execution_directive_recommendations = {}
                recent_order_reviews = self._build_recent_order_reviews(
                    symbol=symbol,
                    rows=symbol_closed,
                    shock=shock,
                    limit=5,
                )
                symbol_open_positions = [dict(row) for row in list(open_positions or []) if str(row["symbol"] or "") == symbol]
                symbol_open_orders = [dict(row) for row in list(open_orders or []) if str(row["symbol"] or "") == symbol]
                if symbol == "XAUUSD":
                    profile_recommendations = self._derive_xau_profile_recommendation(
                        shock=shock,
                        losses=loss_summary,
                        best_same_situation=best_same_situation,
                        upcoming_events=upcoming_events,
                    )
                    reason_memory_recommendations = self._derive_xau_reason_memory_recommendation(
                        symbol_closed=symbol_closed,
                        selected_family=selected_family,
                        best_family_today=best_family_today,
                        winner_memory_reference=top_memory,
                    )
                    family_routing_recommendations = self._derive_xau_family_routing_recommendation(
                        selected_family=selected_family,
                        shock=shock,
                        losses=loss_summary,
                        pb_source_stats=pb_source_stats,
                        scheduled_source_stats=scheduled_source_stats,
                        scheduled_family_calibration=scheduled_family_calibration,
                        best_same_situation=best_same_situation,
                        best_family_today=best_family_today,
                        winner_memory_reference=top_memory,
                        upcoming_events=upcoming_events,
                        post_event_learning=post_event_learning,
                    )
                    if str(family_routing_recommendations.get("support_mode") or "") == "calibration_fallback":
                        manager_findings.append(
                            "pb demotion support came from broader scheduled calibration, not only 24h live rows"
                        )
                    if int(reason_memory_recommendations.get("matched_count", 0) or 0) > 0:
                        reason_mode = str(
                            reason_memory_recommendations.get("mode")
                            or reason_memory_recommendations.get("status")
                            or "reason_memory"
                        )
                        proposed_conf = reason_memory_recommendations.get("proposed_canary_min_confidence")
                        if proposed_conf is None:
                            manager_findings.append(
                                f"reason memory: {reason_mode} | avg {float(reason_memory_recommendations.get('avg_score', 0.0) or 0.0):+.2f}"
                            )
                        else:
                            manager_findings.append(
                                f"reason memory: {reason_mode} | avg {float(reason_memory_recommendations.get('avg_score', 0.0) or 0.0):+.2f} "
                                f"| canary {float(reason_memory_recommendations.get('current_canary_min_confidence', 0.0) or 0.0):.1f}"
                                f"->{float(proposed_conf or 0.0):.1f}"
                            )
                    opportunity_sidecar_recommendations = self._derive_xau_opportunity_sidecar_recommendation(
                        chart_state_memory=reports.get("chart_state_memory_report") or {},
                        experiment_report=reports.get("ct_only_experiment_report") or {},
                    )
                    parallel_family_recommendations = self._derive_xau_parallel_family_recommendation(
                        chart_state_memory=reports.get("chart_state_memory_report") or {},
                    )
                    hedge_lane_recommendations = self._derive_xau_hedge_lane_recommendation(
                        chart_state_memory=reports.get("chart_state_memory_report") or {},
                        experiment_report=reports.get("ct_only_experiment_report") or {},
                    )
                    micro_regime_refresh = self._derive_xau_micro_regime_refresh(symbol_closed=symbol_closed)
                    cluster_loss_guard_recommendations = self._derive_xau_cluster_loss_guard_recommendation(
                        micro_regime_refresh=micro_regime_refresh,
                    )
                    regime_transition_recommendations = self._derive_xau_regime_transition_recommendation(
                        selected_family=selected_family,
                        micro_regime_refresh=micro_regime_refresh,
                        recent_order_reviews=recent_order_reviews,
                        open_positions=symbol_open_positions,
                        open_orders=symbol_open_orders,
                        symbol_states=symbol_chart_states,
                    )
                    execution_directive_recommendations = self._derive_xau_execution_directive_recommendation(
                        regime_transition=regime_transition_recommendations,
                    )
                    if bool(regime_transition_recommendations.get("active")):
                        manager_findings.append(
                            "regime transition: "
                            f"{str(regime_transition_recommendations.get('current_side') or '-')} -> "
                            f"{str(regime_transition_recommendations.get('state_label') or '-')}"
                            f" | day={str(regime_transition_recommendations.get('day_type') or '-')}"
                        )
                        recommended_actions.append(
                            {
                                "action": "early_regime_transition_pause",
                                "blocked_direction": str(regime_transition_recommendations.get("blocked_direction") or ""),
                                "families": list(regime_transition_recommendations.get("blocked_families") or []),
                                "sources": list(regime_transition_recommendations.get("blocked_sources") or []),
                                "hold_until_utc": str(regime_transition_recommendations.get("hold_until_utc") or ""),
                                "reason": str(regime_transition_recommendations.get("reason") or ""),
                            }
                        )
                        if list(regime_transition_recommendations.get("preferred_families") or []):
                            recommended_actions.append(
                                {
                                    "action": "assign_regime_transition_lead",
                                    "families": list(regime_transition_recommendations.get("preferred_families") or []),
                                    "sources": list(regime_transition_recommendations.get("preferred_sources") or []),
                                    "reason": str(regime_transition_recommendations.get("support_state") or ""),
                                }
                            )
                    if bool(micro_regime_refresh.get("active")):
                        manager_findings.append(
                            "micro-regime: "
                            f"{str(micro_regime_refresh.get('state_label') or '-')}"
                            f" | dom={str(micro_regime_refresh.get('dominant_direction') or '-')}"
                            f" | resolved={int((dict(micro_regime_refresh.get('dominant_bucket') or {})).get('resolved', 0) or 0)}"
                        )
                    if bool(cluster_loss_guard_recommendations.get("active")):
                        manager_findings.append(
                            "cluster-loss guard: "
                            f"block {str(cluster_loss_guard_recommendations.get('blocked_direction') or '-')}"
                            f" after {int(cluster_loss_guard_recommendations.get('losses', 0) or 0)} losses"
                        )
                    if bool(execution_directive_recommendations.get("active")):
                        blocked = ",".join(list(execution_directive_recommendations.get("blocked_families") or [])[:3]) or "-"
                        lead = ",".join(list(execution_directive_recommendations.get("preferred_families") or [])[:2]) or "-"
                        manager_findings.append(
                            "execution directive: "
                            f"pause {blocked} | lead {lead} | {str(execution_directive_recommendations.get('support_state') or '-')}"
                        )
                        recommended_actions.append(
                            {
                                "action": "pause_short_limit_families",
                                "families": list(execution_directive_recommendations.get("blocked_families") or []),
                                "sources": list(execution_directive_recommendations.get("blocked_sources") or []),
                                "pause_until_utc": str(execution_directive_recommendations.get("pause_until_utc") or ""),
                                "reason": str(execution_directive_recommendations.get("reason") or ""),
                            }
                        )
                        recommended_actions.append(
                            {
                                "action": "assign_confirmation_lead",
                                "families": list(execution_directive_recommendations.get("preferred_families") or []),
                                "sources": list(execution_directive_recommendations.get("preferred_sources") or []),
                                "reason": str(execution_directive_recommendations.get("support_state") or ""),
                            }
                        )
                        if bool((dict(execution_directive_recommendations.get("pair_risk_cap") or {})).get("enabled")):
                            recommended_actions.append(
                                {
                                    "action": "cap_same_run_pair_risk",
                                    "max_risk_usd": float(
                                        (dict(execution_directive_recommendations.get("pair_risk_cap") or {})).get("max_risk_usd", 0.0) or 0.0
                                    ),
                                    "reason": "manager keeps paired canary risk compressed during disagreement pause",
                                }
                            )
                if symbol == "XAUUSD":
                    opportunity_bypass_recommendations = self._derive_xau_opportunity_bypass_recommendation(
                        open_positions=symbol_open_positions,
                        recent_order_reviews=recent_order_reviews,
                        chart_state_memory=reports.get("chart_state_memory_report") or {},
                    )
                why_no_trade_diagnostics = self._derive_why_no_trade_diagnostics(
                    symbol=symbol,
                    symbol_closed=symbol_closed,
                    open_positions=symbol_open_positions,
                    open_orders=symbol_open_orders,
                    chart_state_memory=reports.get("chart_state_memory_report") or {},
                    experiment_report=reports.get("ct_only_experiment_report") or {},
                    opportunity_feed_symbol=dict((((manager_state.get("opportunity_feed") or {}).get("symbols") or {}).get(symbol) or {})),
                    selected_family=selected_family,
                    manager_state=manager_state,
                )
                order_care_recommendations = {}
                if symbol == "XAUUSD":
                    order_care_recommendations = self._derive_xau_order_care_recommendation(
                        recent_order_reviews=recent_order_reviews,
                    )
                if recent_order_reviews:
                    latest_review = dict(recent_order_reviews[0] or {})
                    manager_findings.append(
                        f"recent order review: {str(latest_review.get('family') or '-')} "
                        f"{str(latest_review.get('direction') or '-')} {float(latest_review.get('pnl_usd', 0.0) or 0.0):.2f}"
                    )
                for blocker in list((why_no_trade_diagnostics.get("likely_blockers") or []))[:2]:
                    manager_findings.append(f"why-no-trade: {str(blocker)}")
                route_split_today = {
                    "normal": self._source_bucket(symbol_closed, "scalp_xauusd:pb:canary", "scalp_xauusd:td:canary", "scalp_xauusd:mfu:canary", "xauusd_scheduled:canary", "xauusd_scheduled:winner", "scalp_btcusd:winner", "scalp_ethusd:winner"),
                    "hedge": self._source_bucket(symbol_closed, "scalp_xauusd:fss:canary"),
                    "flip": self._source_bucket(symbol_closed, "scalp_xauusd:ff:canary"),
                }
                symbol_row = {
                    "symbol": symbol,
                    "selected_family": selected_family,
                    "selected_regime": selected_regime,
                    "selected_family_calibration": selected_family_calibration,
                    "scheduled_family_calibration": scheduled_family_calibration,
                    "closed_total": _finalize_bucket(
                        {
                            "resolved": len(symbol_closed),
                            "wins": sum(1 for row in list(symbol_closed or []) if int(row.get("outcome", -1)) == 1),
                            "losses": sum(1 for row in list(symbol_closed or []) if int(row.get("outcome", -1)) == 0),
                            "pnl_usd": sum(_safe_float(row.get("pnl_usd"), 0.0) for row in list(symbol_closed or [])),
                        }
                    ),
                    "best_family_today": best_family_today,
                    "worst_family_today": worst_family_today,
                    "shock_event": shock,
                    "shock_explanation": shock_explanation,
                    "macro_cause": macro_cause,
                    "upcoming_events": upcoming_events,
                    "post_event_learning": post_event_learning,
                    "losses_in_shock": loss_summary,
                    "best_same_situation": best_same_situation,
                    "family_leaderboard_today": family_rows[:6],
                    "shock_family_leaderboard": shock_family_rows[:6],
                    "winner_memory_reference": top_memory,
                    "pb_source_stats": pb_source_stats,
                    "scheduled_source_stats": scheduled_source_stats,
                    "selected_family_calibration": dict(family_calibration_index.get((symbol, selected_family)) or {}),
                    "open_positions": symbol_open_positions,
                    "open_orders": symbol_open_orders,
                    "profile_recommendations": profile_recommendations,
                    "reason_memory_recommendations": reason_memory_recommendations,
                    "family_routing_recommendations": family_routing_recommendations,
                    "opportunity_sidecar_recommendations": opportunity_sidecar_recommendations,
                    "parallel_family_recommendations": parallel_family_recommendations,
                    "hedge_lane_recommendations": hedge_lane_recommendations,
                    "opportunity_bypass_recommendations": opportunity_bypass_recommendations,
                    "regime_transition_recommendations": regime_transition_recommendations,
                    "micro_regime_refresh": micro_regime_refresh,
                    "cluster_loss_guard_recommendations": cluster_loss_guard_recommendations,
                    "execution_directive_recommendations": execution_directive_recommendations,
                    "order_care_recommendations": order_care_recommendations,
                    "recent_order_reviews": recent_order_reviews,
                    "why_no_trade_diagnostics": why_no_trade_diagnostics,
                    "route_split_today": route_split_today,
                    "manager_findings": manager_findings,
                    "manager_actions": recommended_actions,
                }
                symbol_rows.append(symbol_row)
                for action in list(recommended_actions or []):
                    manager_actions.append({"symbol": symbol, **action})

        symbol_rows.sort(
            key=lambda row: (
                abs(float(((row.get("shock_event") or {}).get("move_pct", 0.0) or 0.0))),
                abs(float(((row.get("losses_in_shock") or {}).get("pnl_usd", 0.0) or 0.0))),
                abs(float(((row.get("closed_total") or {}).get("pnl_usd", 0.0) or 0.0))),
            ),
            reverse=True,
        )
        out["symbols"] = symbol_rows
        out["actions"] = manager_actions[:12]
        out["opportunity_feed"] = self._derive_opportunity_feed(
            symbol_rows=symbol_rows,
            chart_state_memory=reports.get("chart_state_memory_report") or {},
            experiment_report=reports.get("ct_only_experiment_report") or {},
        )
        out["profile_apply"] = self._apply_profile_recommendations(out)
        out["reason_memory_apply"] = self._apply_reason_memory_recommendations(out)
        out["family_routing_apply"] = self._apply_family_routing_recommendations(out)
        out["opportunity_sidecar_apply"] = self._apply_opportunity_sidecar_recommendations(out)
        out["parallel_family_apply"] = self._apply_parallel_family_recommendations(out)
        out["hedge_lane_apply"] = self._apply_hedge_lane_recommendations(out)
        out["opportunity_bypass_apply"] = self._apply_xau_opportunity_bypass_recommendations(out)
        out["order_care_apply"] = self._apply_xau_order_care_recommendations(out)
        out["regime_transition_apply"] = self._apply_xau_regime_transition_recommendations(out)
        out["micro_regime_apply"] = self._apply_xau_micro_regime_refresh(out)
        out["cluster_loss_guard_apply"] = self._apply_xau_cluster_loss_guard_recommendations(out)
        out["execution_directive_apply"] = self._apply_xau_execution_directive_recommendations(out)
        out["opportunity_feed_apply"] = self._apply_opportunity_feed(out)
        self._save_report("trading_manager_report", out)
        return out


trading_manager_agent = TradingManagerAgent()
