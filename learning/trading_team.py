from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from config import config


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


class TradingTeamAgent:
    def __init__(self, *, report_dir: str | None = None, runtime_dir: str | None = None):
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / "data"
        self.report_dir = Path(report_dir or (data_dir / "reports"))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir = Path(runtime_dir or (data_dir / "runtime"))
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.runtime_dir / "trading_team_state.json"
        self.manager_state_path = self.runtime_dir / "trading_manager_state.json"
        self.manager_report_path = self.report_dir / "trading_manager_report.json"
        self.ct_experiment_report_path = self.report_dir / "ct_only_experiment_report.json"
        self.strategy_lab_state_path = self.runtime_dir / "strategy_lab_team_state.json"

    @staticmethod
    def _load_json(path: Path) -> dict:
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
        return {}

    @staticmethod
    def _save_json(path: Path, payload: dict) -> None:
        path.write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2), encoding="utf-8")

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

    @staticmethod
    def _priority_rows(score_map: dict[str, float]) -> list[dict]:
        rows = [
            {"family": str(fam or "").strip().lower(), "score": round(float(score or 0.0), 3)}
            for fam, score in dict(score_map or {}).items()
            if str(fam or "").strip()
        ]
        rows.sort(key=lambda item: (float(item.get("score", 0.0) or 0.0), str(item.get("family") or "")), reverse=True)
        return rows

    @staticmethod
    def _add_score(score_map: dict[str, float], family: str, delta: float) -> None:
        token = str(family or "").strip().lower()
        if not token:
            return
        score_map[token] = round(float(score_map.get(token, 0.0) or 0.0) + float(delta or 0.0), 4)

    def _xau_live_edge_snapshot(self, *, experiment_report: dict | None) -> dict:
        report = dict(experiment_report or {})
        if not bool(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_ENABLED", True)):
            return {"families": {}, "comparison_leaders": {}, "ok": False}
        if not bool(report.get("ok")):
            return {"families": {}, "comparison_leaders": {}, "ok": False}

        min_resolved = max(1, int(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_MIN_RESOLVED", 2) or 2))
        pnl_mult = float(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_PNL_MULT", 0.55) or 0.55)
        win_mult = float(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_WIN_RATE_MULT", 12.0) or 12.0)
        score_cap = max(1.0, float(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_SCORE_CAP", 12.0) or 12.0))
        families: dict[str, dict] = {}

        for row in list(report.get("sources") or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != "XAUUSD":
                continue
            family = str(row.get("family") or "").strip().lower()
            if not family:
                continue
            closed_total = dict(row.get("closed_total") or {})
            resolved = int(closed_total.get("resolved", 0) or 0)
            pnl_usd = float(closed_total.get("pnl_usd", 0.0) or 0.0)
            win_rate = float(closed_total.get("win_rate", 0.0) or 0.0)
            avg_pnl = float(closed_total.get("avg_pnl_usd", 0.0) or 0.0)
            sample_scale = min(1.0, float(resolved) / float(max(1, min_resolved)))
            live_edge_score = ((pnl_usd * pnl_mult) + ((win_rate - 0.5) * win_mult)) * sample_scale
            if resolved >= min_resolved:
                live_edge_score += min(2.0, float(resolved) * 0.05)
            if resolved >= min_resolved and pnl_usd <= 0.0 and avg_pnl <= 0.0:
                live_edge_score -= 2.0
            live_edge_score = max(-score_cap, min(score_cap, live_edge_score))
            families[family] = {
                "family": family,
                "source": str(row.get("source") or "").strip().lower(),
                "resolved": resolved,
                "wins": int(closed_total.get("wins", 0) or 0),
                "losses": int(closed_total.get("losses", 0) or 0),
                "win_rate": round(win_rate, 4),
                "pnl_usd": round(pnl_usd, 4),
                "avg_pnl_usd": round(avg_pnl, 4),
                "live_edge_score": round(live_edge_score, 4),
                "comparison_bonus": 0.0,
                "status": (
                    "production_candidate"
                    if resolved >= min_resolved and pnl_usd > 0.0 and win_rate >= 0.52
                    else "sampling_candidate" if resolved > 0 else "idle"
                ),
            }

        comparisons = dict(report.get("comparisons") or {})
        comparison_leaders: dict[str, str] = {}
        comparison_rules = {
            "xau_td_vs_pb_live": {
                "leader_bonus": 3.5,
                "losers": {"xau_scalp_tick_depth_filter", "xau_scalp_pullback_limit"},
                "loser_penalty": 2.0,
            },
            "xau_mfu_vs_broad_microtrend_live": {
                "leader_bonus": 2.5,
                "losers": {"xau_scalp_microtrend_follow_up", "xau_scalp_microtrend"},
                "loser_penalty": 1.0,
            },
            "xau_fss_vs_broad_microtrend_live": {
                "leader_bonus": 2.0,
                "losers": {"xau_scalp_flow_short_sidecar", "xau_scalp_microtrend"},
                "loser_penalty": 3.0,
            },
            "xau_ff_effectiveness": {
                "leader_bonus": 1.5,
                "losers": {"xau_scalp_failed_fade_follow_stop"},
                "loser_penalty": 0.0,
            },
        }
        for key, rule in list(comparison_rules.items()):
            row = dict(comparisons.get(key) or {})
            leader = str(row.get("leader") or "").strip().lower()
            if not leader:
                continue
            comparison_leaders[key] = leader
            family_row = families.setdefault(
                leader,
                {
                    "family": leader,
                    "source": "",
                    "resolved": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0.0,
                    "pnl_usd": 0.0,
                    "avg_pnl_usd": 0.0,
                    "live_edge_score": 0.0,
                    "comparison_bonus": 0.0,
                    "status": "comparison_leader",
                },
            )
            family_row["comparison_bonus"] = round(float(family_row.get("comparison_bonus", 0.0) or 0.0) + float(rule.get("leader_bonus", 0.0) or 0.0), 4)
            for loser in list(rule.get("losers") or []):
                loser_token = str(loser or "").strip().lower()
                if not loser_token or loser_token == leader:
                    continue
                loser_row = families.setdefault(
                    loser_token,
                    {
                        "family": loser_token,
                        "source": "",
                        "resolved": 0,
                        "wins": 0,
                        "losses": 0,
                        "win_rate": 0.0,
                        "pnl_usd": 0.0,
                        "avg_pnl_usd": 0.0,
                        "live_edge_score": 0.0,
                        "comparison_bonus": 0.0,
                        "status": "comparison_peer",
                    },
                )
                loser_row["comparison_bonus"] = round(float(loser_row.get("comparison_bonus", 0.0) or 0.0) - float(rule.get("loser_penalty", 0.0) or 0.0), 4)

        return {"families": families, "comparison_leaders": comparison_leaders, "ok": True}

    def _apply_xau_live_edge_allocator(self, *, score_map: dict[str, float], experiment_report: dict | None) -> tuple[list[str], dict]:
        snapshot = self._xau_live_edge_snapshot(experiment_report=experiment_report)
        families = dict(snapshot.get("families") or {})
        if not families:
            return [], {}

        coaching: list[str] = []
        for family, row in list(families.items()):
            delta = float(row.get("live_edge_score", 0.0) or 0.0) + float(row.get("comparison_bonus", 0.0) or 0.0)
            if abs(delta) >= 0.01:
                self._add_score(score_map, family, delta)

        ranked_families = [str(row.get("family") or "") for row in self._priority_rows(score_map)]
        min_resolved = max(1, int(getattr(config, "TRADING_TEAM_XAU_LIVE_EDGE_MIN_RESOLVED", 2) or 2))
        production_max = max(1, int(getattr(config, "TRADING_TEAM_XAU_PRODUCTION_MAX_FAMILIES", 2) or 2))
        sampling_max = max(0, int(getattr(config, "TRADING_TEAM_XAU_SAMPLING_MAX_FAMILIES", 2) or 2))
        sampling_parallel_limit = max(0, int(getattr(config, "TRADING_TEAM_XAU_SAMPLING_PARALLEL_LIMIT", 1) or 1))
        production_candidates = {
            str(family or "").strip().lower()
            for family, row in families.items()
            if int(row.get("resolved", 0) or 0) >= min_resolved
            and float(row.get("pnl_usd", 0.0) or 0.0) > 0.0
            and float(row.get("live_edge_score", 0.0) or 0.0) > 0.0
        }
        comparison_leaders = {
            str(family or "").strip().lower()
            for family in dict(snapshot.get("comparison_leaders") or {}).values()
            if str(family or "").strip()
        }
        production_candidates |= comparison_leaders
        production_families = [family for family in ranked_families if family in production_candidates][:production_max]
        if not production_families and ranked_families:
            production_families = ranked_families[:1]
        sampling_families = [family for family in ranked_families if family not in production_families][:sampling_max]
        if production_families:
            coaching.append(f"live edge allocator promoted {','.join(production_families)} into production")
        if sampling_families:
            coaching.append(f"live edge allocator kept {','.join(sampling_families)} in sampling")
        budget = {
            "status": "active",
            "mode": "institutional_live_edge_allocator",
            "family_live_edge_map": {
                family: {
                    "resolved": int(row.get("resolved", 0) or 0),
                    "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                    "pnl_usd": float(row.get("pnl_usd", 0.0) or 0.0),
                    "live_edge_score": float(row.get("live_edge_score", 0.0) or 0.0),
                    "comparison_bonus": float(row.get("comparison_bonus", 0.0) or 0.0),
                    "status": str(row.get("status") or ""),
                    "source": str(row.get("source") or ""),
                }
                for family, row in list(families.items())
            },
            "comparison_leaders": dict(snapshot.get("comparison_leaders") or {}),
            "production_families": production_families,
            "sampling_families": sampling_families,
            "production_parallel_limit": max(1, len(production_families) or 1),
            "sampling_parallel_limit": min(max(0, len(sampling_families)), sampling_parallel_limit),
        }
        return coaching, budget

    def _allowed_xau_families(self) -> list[str]:
        merged = self._parse_family_csv(
            ",".join(
                [
                    str(getattr(config, "PERSISTENT_CANARY_STRATEGY_FAMILIES", "") or ""),
                    str(getattr(config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES", "") or ""),
                    str(getattr(config, "CTRADER_XAU_ACTIVE_FAMILIES", "") or ""),
                    str(getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ALLOWED", "") or ""),
                    str(getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_ALLOWED_FAMILIES", "") or ""),
                    str(getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ALLOWED_FAMILIES", "") or ""),
                ]
            )
        )
        for getter_name in (
            "get_persistent_canary_strategy_families",
            "get_persistent_canary_experimental_families",
            "get_ctrader_xau_active_families",
        ):
            getter = getattr(config, getter_name, None)
            if callable(getter):
                for family in list(getter() or set()):
                    token = str(family or "").strip().lower()
                    if token and token not in merged:
                        merged.append(token)
        return merged or [
            "xau_scalp_pullback_limit",
            "xau_scalp_breakout_stop",
            "xau_scalp_tick_depth_filter",
            "xau_scalp_microtrend_follow_up",
            "xau_scalp_flow_short_sidecar",
            "xau_scalp_failed_fade_follow_stop",
            "xau_scheduled_trend",
        ]

    @staticmethod
    def _strategy_lab_desk(strategy_lab_symbol: dict | None) -> dict:
        row = dict(strategy_lab_symbol or {})
        if not row:
            return {}
        return {
            "promotion_ready": list(row.get("promotion_queue") or [])[:3],
            "live_shadow": list(row.get("live_shadow_queue") or [])[:3],
            "recovery": list(row.get("recovery_queue") or [])[:3],
            "shadow": list(row.get("shadow_queue") or [])[:3],
            "blocked_count": int((dict(row.get("summary") or {})).get("blocked_count", 0) or 0),
        }

    def _apply_strategy_lab_priority(self, *, score_map: dict[str, float], strategy_lab_symbol: dict | None) -> list[str]:
        row = dict(strategy_lab_symbol or {})
        if not row:
            return []
        coaching: list[str] = []
        promotion_mult = max(0.0, float(getattr(config, "TRADING_TEAM_STRATEGY_LAB_PROMOTION_SCORE_MULT", 0.35) or 0.35))
        live_shadow_mult = max(0.0, float(getattr(config, "TRADING_TEAM_STRATEGY_LAB_LIVE_SHADOW_SCORE_MULT", 0.15) or 0.15))
        recovery_mult = max(0.0, float(getattr(config, "TRADING_TEAM_STRATEGY_LAB_RECOVERY_SCORE_MULT", 0.10) or 0.10))
        promotion_map = {
            str(fam or "").strip().lower(): float(score or 0.0)
            for fam, score in dict(row.get("promotion_family_priority_map") or {}).items()
            if str(fam or "").strip()
        }
        live_shadow_map = {
            str(fam or "").strip().lower(): float(score or 0.0)
            for fam, score in dict(row.get("live_shadow_family_priority_map") or {}).items()
            if str(fam or "").strip()
        }
        recovery_map = {
            str(fam or "").strip().lower(): float(score or 0.0)
            for fam, score in dict(row.get("recovery_family_priority_map") or {}).items()
            if str(fam or "").strip()
        }
        for family, score in list(promotion_map.items()):
            self._add_score(score_map, family, score * promotion_mult)
        if promotion_map:
            coaching.append("strategy lab promoted proven candidates into the live desk")
        for family, score in list(live_shadow_map.items()):
            self._add_score(score_map, family, score * live_shadow_mult)
        if live_shadow_map:
            coaching.append("strategy lab probation candidates are feeding light live-shadow weight")
        for family, score in list(recovery_map.items()):
            self._add_score(score_map, family, score * recovery_mult)
        if recovery_map:
            coaching.append("strategy lab recovery desk is feeding fallback XAU probes")
        return coaching

    def _xau_family_universe(self, *, symbol_row: dict, feed_symbol: dict, strategy_lab_symbol: dict | None = None) -> list[str]:
        families = list(self._allowed_xau_families())
        for source in (
            dict(feed_symbol.get("family_priority_map") or {}),
            {item.get("family"): 1.0 for item in list((dict(symbol_row.get("reason_memory_recommendations") or {}).get("family_scores") or []))},
            dict((dict(strategy_lab_symbol or {}).get("execution_family_priority_map") or {})),
            dict((dict(strategy_lab_symbol or {}).get("promotion_family_priority_map") or {})),
            dict((dict(strategy_lab_symbol or {}).get("live_shadow_family_priority_map") or {})),
            dict((dict(strategy_lab_symbol or {}).get("recovery_family_priority_map") or {})),
        ):
            for family in list(source.keys()):
                token = str(family or "").strip().lower()
                if token and token not in families:
                    families.append(token)
        for family in (
            str(symbol_row.get("selected_family") or "").strip().lower(),
            self._family_from_bucket(symbol_row.get("best_family_today") or {}),
        ):
            if family and family not in families:
                families.append(family)
        for bucket in list(symbol_row.get("family_leaderboard_today") or [])[:8]:
            family = self._family_from_bucket(bucket)
            if family and family not in families:
                families.append(family)
        for rec_key in (
            "parallel_family_recommendations",
            "hedge_lane_recommendations",
            "opportunity_bypass_recommendations",
        ):
            rec = dict(symbol_row.get(rec_key) or {})
            for family in list(rec.get("allowed_families") or []):
                token = str(family or "").strip().lower()
                if token and token not in families:
                    families.append(token)
        sidecar = dict(symbol_row.get("opportunity_sidecar_recommendations") or {})
        if bool(sidecar.get("active")) and "xau_scalp_flow_short_sidecar" not in families:
            families.append("xau_scalp_flow_short_sidecar")
        return families

    def _score_xau_families(
        self,
        *,
        symbol_row: dict,
        feed_symbol: dict,
        families: list[str],
        strategy_lab_symbol: dict | None = None,
        experiment_report: dict | None = None,
        execution_directive: dict | None = None,
    ) -> tuple[list[dict], list[str], dict]:
        scores: dict[str, float] = {str(fam or "").strip().lower(): 0.0 for fam in list(families or []) if str(fam or "").strip()}
        coaching: list[str] = []
        reason_mult = max(1.0, float(getattr(config, "TRADING_TEAM_XAU_REASON_SCORE_MULT", 18.0) or 18.0))

        feed_map = {str(fam or "").strip().lower(): float(score or 0.0) for fam, score in dict(feed_symbol.get("family_priority_map") or {}).items() if str(fam or "").strip()}
        for family, score in list(feed_map.items()):
            self._add_score(scores, family, score)
        if feed_map:
            coaching.append("manager feed reprioritized the XAU family desk")

        selected_family = str(symbol_row.get("selected_family") or "").strip().lower()
        if selected_family:
            self._add_score(scores, selected_family, 8.0)

        best_family_today = dict(symbol_row.get("best_family_today") or {})
        best_family = self._family_from_bucket(best_family_today)
        best_pnl = float(best_family_today.get("pnl_usd", 0.0) or 0.0)
        best_resolved = int(best_family_today.get("resolved", 0) or 0)
        if best_family and best_resolved >= 2 and best_pnl > 0.0:
            self._add_score(scores, best_family, 10.0 + min(8.0, best_pnl / 4.0))
            coaching.append(f"today leader {best_family} stays boosted from live pnl {best_pnl:.2f}")

        for row in list(symbol_row.get("family_leaderboard_today") or [])[:5]:
            family = self._family_from_bucket(row)
            if not family:
                continue
            pnl = float(row.get("pnl_usd", 0.0) or 0.0)
            win_rate = float(row.get("win_rate", 0.0) or 0.0)
            resolved = int(row.get("resolved", 0) or 0)
            if resolved <= 0:
                continue
            self._add_score(scores, family, max(-4.0, min(10.0, pnl * 0.25)) + (win_rate * 4.0))

        reason_memory = dict(symbol_row.get("reason_memory_recommendations") or {})
        for row in list(reason_memory.get("family_scores") or [])[:8]:
            family = str(row.get("family") or "").strip().lower()
            if not family:
                continue
            self._add_score(scores, family, float(row.get("score", 0.0) or 0.0) * reason_mult)
        preferred_family = str(reason_memory.get("preferred_family") or "").strip().lower()
        if preferred_family:
            self._add_score(scores, preferred_family, 6.0)
        for family in list(reason_memory.get("avoid_families") or [])[:3]:
            self._add_score(scores, str(family or "").strip().lower(), -4.0)
        if int(reason_memory.get("matched_count", 0) or 0) > 0:
            coaching.append("reason-memory bias is now feeding family scores instead of blocking")

        sidecar = dict(symbol_row.get("opportunity_sidecar_recommendations") or {})
        if bool(sidecar.get("active")):
            self._add_score(scores, "xau_scalp_flow_short_sidecar", 7.0)

        family_routing = dict(symbol_row.get("family_routing_recommendations") or {})
        for family in list(family_routing.get("promoted_families") or [])[:4]:
            self._add_score(scores, str(family or "").strip().lower(), 4.0)
        for family in list(family_routing.get("demoted_families") or [])[:4]:
            self._add_score(scores, str(family or "").strip().lower(), -1.5)

        parallel = dict(symbol_row.get("parallel_family_recommendations") or {})
        if bool(parallel.get("active")):
            for family in list(parallel.get("allowed_families") or []):
                self._add_score(scores, str(family or "").strip().lower(), 3.0)
            coaching.append("parallel family desk widened same-direction support")

        hedge = dict(symbol_row.get("hedge_lane_recommendations") or {})
        if bool(hedge.get("active")):
            for family in list(hedge.get("allowed_families") or []):
                self._add_score(scores, str(family or "").strip().lower(), 2.0)

        bypass = dict(symbol_row.get("opportunity_bypass_recommendations") or {})
        if bool(bypass.get("active")):
            for family in list(bypass.get("allowed_families") or []):
                self._add_score(scores, str(family or "").strip().lower(), 2.5)

        directive = dict(execution_directive or {})
        if bool(directive.get("active")):
            for family in list(directive.get("preferred_families") or []):
                self._add_score(scores, str(family or "").strip().lower(), 12.0)
            for family in list(directive.get("blocked_families") or []):
                self._add_score(scores, str(family or "").strip().lower(), -12.0)
            coaching.extend(list(directive.get("coach_traders") or [])[:3])

        coaching.extend(self._apply_strategy_lab_priority(score_map=scores, strategy_lab_symbol=strategy_lab_symbol))
        edge_coaching, budget = self._apply_xau_live_edge_allocator(score_map=scores, experiment_report=experiment_report)
        coaching.extend(edge_coaching)
        return self._priority_rows(scores), coaching, budget

    def _build_xau_symbol_team(self, *, symbol_row: dict, feed_symbol: dict, strategy_lab_symbol: dict | None = None, experiment_report: dict | None = None) -> tuple[dict, dict]:
        families = self._xau_family_universe(symbol_row=symbol_row, feed_symbol=feed_symbol, strategy_lab_symbol=strategy_lab_symbol)
        manager_state = dict(self._load_json(self.manager_state_path) or {})
        regime_transition = dict(symbol_row.get("regime_transition_recommendations") or {})
        if not bool(regime_transition.get("active")):
            carried_regime_transition = dict(manager_state.get("xau_regime_transition") or {})
            if str(carried_regime_transition.get("status") or "") == "active":
                regime_transition = {
                    "active": True,
                    "mode": str(carried_regime_transition.get("mode") or ""),
                    "reason": str(carried_regime_transition.get("reason") or ""),
                    "support_state": str(carried_regime_transition.get("support_state") or ""),
                    "current_side": str(carried_regime_transition.get("current_side") or ""),
                    "state_label": str(carried_regime_transition.get("state_label") or ""),
                    "opposite_state_label": str(carried_regime_transition.get("opposite_state_label") or ""),
                    "day_type": str(carried_regime_transition.get("day_type") or ""),
                    "follow_up_plan": str(carried_regime_transition.get("follow_up_plan") or ""),
                    "blocked_direction": str(carried_regime_transition.get("blocked_direction") or ""),
                    "blocked_entry_types": list(carried_regime_transition.get("blocked_entry_types") or []),
                    "blocked_families": list(carried_regime_transition.get("blocked_families") or []),
                    "blocked_sources": list(carried_regime_transition.get("blocked_sources") or []),
                    "preferred_families": list(carried_regime_transition.get("preferred_families") or []),
                    "preferred_sources": list(carried_regime_transition.get("preferred_sources") or []),
                    "snapshot_run_id": str(carried_regime_transition.get("snapshot_run_id") or ""),
                    "snapshot_last_event_utc": str(carried_regime_transition.get("snapshot_last_event_utc") or ""),
                    "snapshot_features": dict(carried_regime_transition.get("snapshot_features") or {}),
                    "pressure": dict(carried_regime_transition.get("pressure") or {}),
                    "hold_min": int(carried_regime_transition.get("hold_min", 0) or 0),
                    "remaining_min": float(carried_regime_transition.get("remaining_min", 0.0) or 0.0),
                    "hold_until_utc": str(carried_regime_transition.get("hold_until_utc") or ""),
                }
        execution_directive = dict(symbol_row.get("execution_directive_recommendations") or {})
        if not bool(execution_directive.get("active")):
            carried_execution_directive = dict(manager_state.get("xau_execution_directive") or {})
            if str(carried_execution_directive.get("status") or "") == "active":
                execution_directive = {
                    "active": True,
                    "mode": str(carried_execution_directive.get("mode") or ""),
                    "reason": str(carried_execution_directive.get("reason") or ""),
                    "blocked_direction": str(carried_execution_directive.get("blocked_direction") or ""),
                    "blocked_entry_types": list(carried_execution_directive.get("blocked_entry_types") or []),
                    "blocked_families": list(carried_execution_directive.get("blocked_families") or []),
                    "blocked_sources": list(carried_execution_directive.get("blocked_sources") or []),
                    "preferred_families": list(carried_execution_directive.get("preferred_families") or []),
                    "preferred_sources": list(carried_execution_directive.get("preferred_sources") or []),
                    "support_state": str(carried_execution_directive.get("support_state") or ""),
                    "trigger_run_id": str(carried_execution_directive.get("trigger_run_id") or ""),
                    "pause_min": int(carried_execution_directive.get("pause_min", 0) or 0),
                    "remaining_min": float(carried_execution_directive.get("remaining_min", 0.0) or 0.0),
                    "pause_until_utc": str(carried_execution_directive.get("pause_until_utc") or ""),
                    "pair_risk_cap": dict(carried_execution_directive.get("pair_risk_cap") or {}),
                    "coach_traders": list(carried_execution_directive.get("coach_traders") or []),
                    "trader_assignments": list(carried_execution_directive.get("trader_assignments") or []),
                }
        ranked_rows, coaching, budget = self._score_xau_families(
            symbol_row=symbol_row,
            feed_symbol=feed_symbol,
            families=families,
            strategy_lab_symbol=strategy_lab_symbol,
            experiment_report=experiment_report,
            execution_directive=execution_directive,
        )
        priority_topk = max(1, int(getattr(config, "TRADING_TEAM_XAU_PRIORITY_TOPK", 4) or 4))
        ranked_families = [str(row.get("family") or "") for row in list(ranked_rows or []) if str(row.get("family") or "")]
        primary_family = str((ranked_rows[0] if ranked_rows else {}).get("family") or "")
        production_families = [str(fam or "") for fam in list((dict(budget or {})).get("production_families") or []) if str(fam or "")]
        if production_families and primary_family not in set(production_families):
            primary_family = str(production_families[0] or "")
            ranked_families = [primary_family] + [family for family in ranked_families if family != primary_family]
        family_priority_map = {str(row.get("family") or ""): float(row.get("score", 0.0) or 0.0) for row in list(ranked_rows or [])}
        if bool(execution_directive.get("active")):
            preferred_families = [
                str(fam or "").strip().lower()
                for fam in list(execution_directive.get("preferred_families") or [])
                if str(fam or "").strip()
            ]
            blocked_families = {
                str(fam or "").strip().lower()
                for fam in list(execution_directive.get("blocked_families") or [])
                if str(fam or "").strip()
            }
            if preferred_families:
                preferred_live = [family for family in preferred_families if family in set(ranked_families)]
                ranked_families = preferred_live + [family for family in list(ranked_families or []) if family not in set(preferred_live)]
                if preferred_live:
                    primary_family = preferred_live[0]
            if blocked_families:
                ranked_families = [
                    family for family in list(ranked_families or [])
                    if family not in blocked_families
                ] + [
                    family for family in list(ranked_families or [])
                    if family in blocked_families
                ]

        parallel = dict(symbol_row.get("parallel_family_recommendations") or {})
        support_all_families: list[str] = []
        routing_mode = "team_primary_advisory"
        if str((dict(symbol_row.get("family_routing_recommendations") or {})).get("mode") or "").strip().lower() == "swarm_support_all":
            support_all_families = [fam for fam in list(feed_symbol.get("support_all_families") or []) if fam in family_priority_map]
            routing_mode = "swarm_support_all"
        elif bool(parallel.get("active")):
            parallel_allowed = {str(fam or "").strip().lower() for fam in list(parallel.get("allowed_families") or []) if str(fam or "").strip()}
            support_all_families = [fam for fam in ranked_families if fam in parallel_allowed][: max(priority_topk, 2)]
        if not support_all_families and primary_family:
            support_all_families = ranked_families[:priority_topk]

        hedge = dict(symbol_row.get("hedge_lane_recommendations") or {})
        bypass = dict(symbol_row.get("opportunity_bypass_recommendations") or {})
        order_care = dict(symbol_row.get("order_care_recommendations") or {})
        if not bool(order_care.get("active")):
            carried_order_care = dict(manager_state.get("xau_order_care") or {})
            if str(carried_order_care.get("status") or "") == "active":
                order_care = {
                    "active": True,
                    "mode": str(carried_order_care.get("mode") or ""),
                    "reason": str(carried_order_care.get("reason") or ""),
                    "allowed_sources": list(carried_order_care.get("allowed_sources") or []),
                    "loss_count": int(carried_order_care.get("loss_count", 0) or 0),
                    "review_window": list(carried_order_care.get("review_window") or []),
                    "desks": dict(carried_order_care.get("desks") or {}),
                    "overrides": dict(carried_order_care.get("overrides") or {}),
                }
        micro_regime = dict(symbol_row.get("micro_regime_refresh") or {})
        cluster_watch = dict(symbol_row.get("cluster_loss_guard_recommendations") or {})

        symbol_team = {
            "symbol": "XAUUSD",
            "bias_desk": {
                "dominant_direction": str(micro_regime.get("dominant_direction") or "").strip().lower(),
                "state_label": str(micro_regime.get("state_label") or "").strip().lower(),
                "selected_family": str(symbol_row.get("selected_family") or "").strip().lower(),
                "preferred_family": str((dict(symbol_row.get("reason_memory_recommendations") or {})).get("preferred_family") or "").strip().lower(),
                "notes": list(symbol_row.get("manager_findings") or [])[:6],
                "manager_directive": str(execution_directive.get("reason") or ""),
                "regime_transition": {
                    "status": "active" if bool(regime_transition.get("active")) else "inactive",
                    "mode": str(regime_transition.get("mode") or ""),
                    "reason": str(regime_transition.get("reason") or ""),
                    "current_side": str(regime_transition.get("current_side") or "").strip().lower(),
                    "state_label": str(regime_transition.get("state_label") or "").strip().lower(),
                    "day_type": str(regime_transition.get("day_type") or "").strip().lower(),
                    "support_state": str(regime_transition.get("support_state") or ""),
                },
            },
            "execution_desk": {
                "primary_family": primary_family,
                "ranked_families": ranked_families[: max(priority_topk, len(support_all_families))],
                "family_priority_map": family_priority_map,
                "support_all_families": support_all_families,
                "production_families": list((dict(budget or {})).get("production_families") or []),
                "sampling_families": list((dict(budget or {})).get("sampling_families") or []),
                "parallel_limit": int(parallel.get("max_same_direction_families", max(1, len(support_all_families))) or max(1, len(support_all_families))),
                "manager_directive": dict(execution_directive) if bool(execution_directive.get("active")) else {},
            },
            "position_desk": {
                "open_positions": len(list(symbol_row.get("open_positions") or [])),
                "open_orders": len(list(symbol_row.get("open_orders") or [])),
                "order_care": dict(order_care) if bool(order_care.get("active")) else {},
                "recent_reviews": list(symbol_row.get("recent_order_reviews") or [])[:3],
            },
            "portfolio_desk": {
                "hedge_families": [str(fam or "").strip().lower() for fam in list(hedge.get("allowed_families") or []) if str(fam or "").strip()],
                "opportunity_bypass_families": [str(fam or "").strip().lower() for fam in list(bypass.get("allowed_families") or []) if str(fam or "").strip()],
                "cluster_watch": {
                    "status": "active" if bool(cluster_watch.get("active")) else "inactive",
                    "blocked_direction": str(cluster_watch.get("blocked_direction") or "").strip().lower(),
                    "losses": int(cluster_watch.get("losses", 0) or 0),
                    "enforcement": "advisory_only",
                },
            },
            "strategy_lab_desk": self._strategy_lab_desk(strategy_lab_symbol),
            "coaching": list(dict(feed_symbol).get("coaching") or [])[:4] + coaching[:4],
        }

        runtime_state = {
            "xau_family_routing": {
                "status": "active" if primary_family else "idle",
                "mode": routing_mode,
                "reason": "trading_team_family_vote",
                "primary_family": primary_family,
                "ranked_families": ranked_families[: max(priority_topk, len(support_all_families))],
                "active_families": support_all_families or ranked_families[:priority_topk],
                "family_priority_map": family_priority_map,
                "production_families": list((dict(budget or {})).get("production_families") or []),
                "sampling_families": list((dict(budget or {})).get("sampling_families") or []),
            },
            "xau_family_budget": dict(budget or {}),
            "xau_parallel_families": {
                "status": "active" if support_all_families else "inactive",
                "mode": "trading_team_parallel_support" if support_all_families else "inactive",
                "allowed_families": support_all_families,
                "max_same_direction_families": int(max(1, parallel.get("max_same_direction_families", len(support_all_families) or 1) or len(support_all_families) or 1)),
            },
            "xau_hedge_transition": {
                "status": "active" if bool(hedge.get("active")) else "inactive",
                "mode": str(hedge.get("mode") or "inactive"),
                "reason": str(hedge.get("reason") or ""),
                "allowed_families": [str(fam or "").strip().lower() for fam in list(hedge.get("allowed_families") or []) if str(fam or "").strip()],
                "max_per_symbol": int(hedge.get("max_per_symbol", 1) or 1),
                "risk_multiplier": float(hedge.get("risk_multiplier", 1.0) or 1.0),
            },
            "xau_opportunity_bypass": {
                "status": "active" if bool(bypass.get("active")) else "inactive",
                "mode": str(bypass.get("mode") or "inactive"),
                "reason": str(bypass.get("reason") or ""),
                "allowed_families": [str(fam or "").strip().lower() for fam in list(bypass.get("allowed_families") or []) if str(fam or "").strip()],
                "max_per_symbol": int(bypass.get("max_per_symbol", 1) or 1),
                "risk_multiplier": float(bypass.get("risk_multiplier", 1.0) or 1.0),
            },
            "xau_opportunity_sidecar": {
                "status": "active" if bool((dict(symbol_row.get("opportunity_sidecar_recommendations") or {})).get("active")) else "inactive",
                "mode": str((dict(symbol_row.get("opportunity_sidecar_recommendations") or {})).get("mode") or ""),
                "reason": str((dict(symbol_row.get("opportunity_sidecar_recommendations") or {})).get("reason") or ""),
            },
            "xau_order_care": {
                "status": "active" if bool(order_care.get("active")) else "inactive",
                "mode": str(order_care.get("mode") or ""),
                "reason": str(order_care.get("reason") or ""),
                "allowed_sources": list(order_care.get("allowed_sources") or []),
                "loss_count": int(order_care.get("loss_count", 0) or 0),
                "review_window": list(order_care.get("review_window") or [])[:5],
                "desks": dict(order_care.get("desks") or {}),
                "overrides": dict(order_care.get("overrides") or {}),
            },
            "xau_regime_transition": {
                "status": "active" if bool(regime_transition.get("active")) else "inactive",
                "mode": str(regime_transition.get("mode") or ""),
                "reason": str(regime_transition.get("reason") or ""),
                "support_state": str(regime_transition.get("support_state") or ""),
                "current_side": str(regime_transition.get("current_side") or ""),
                "state_label": str(regime_transition.get("state_label") or ""),
                "opposite_state_label": str(regime_transition.get("opposite_state_label") or ""),
                "day_type": str(regime_transition.get("day_type") or ""),
                "follow_up_plan": str(regime_transition.get("follow_up_plan") or ""),
                "blocked_direction": str(regime_transition.get("blocked_direction") or ""),
                "blocked_entry_types": list(regime_transition.get("blocked_entry_types") or []),
                "blocked_families": list(regime_transition.get("blocked_families") or []),
                "blocked_sources": list(regime_transition.get("blocked_sources") or []),
                "preferred_families": list(regime_transition.get("preferred_families") or []),
                "preferred_sources": list(regime_transition.get("preferred_sources") or []),
                "snapshot_run_id": str(regime_transition.get("snapshot_run_id") or ""),
                "snapshot_last_event_utc": str(regime_transition.get("snapshot_last_event_utc") or ""),
                "snapshot_features": dict(regime_transition.get("snapshot_features") or {}),
                "pressure": dict(regime_transition.get("pressure") or {}),
                "hold_min": int(regime_transition.get("hold_min", 0) or 0),
                "remaining_min": float(regime_transition.get("remaining_min", 0.0) or 0.0),
                "hold_until_utc": str(regime_transition.get("hold_until_utc") or ""),
            },
            "xau_execution_directive": {
                "status": "active" if bool(execution_directive.get("active")) else "inactive",
                "mode": str(execution_directive.get("mode") or ""),
                "reason": str(execution_directive.get("reason") or ""),
                "blocked_direction": str(execution_directive.get("blocked_direction") or ""),
                "blocked_entry_types": list(execution_directive.get("blocked_entry_types") or []),
                "blocked_families": list(execution_directive.get("blocked_families") or []),
                "blocked_sources": list(execution_directive.get("blocked_sources") or []),
                "preferred_families": list(execution_directive.get("preferred_families") or []),
                "preferred_sources": list(execution_directive.get("preferred_sources") or []),
                "support_state": str(execution_directive.get("support_state") or ""),
                "trigger_run_id": str(execution_directive.get("trigger_run_id") or ""),
                "pause_min": int(execution_directive.get("pause_min", 0) or 0),
                "remaining_min": float(execution_directive.get("remaining_min", 0.0) or 0.0),
                "pause_until_utc": str(execution_directive.get("pause_until_utc") or ""),
                "pair_risk_cap": dict(execution_directive.get("pair_risk_cap") or {}),
                "coach_traders": list(execution_directive.get("coach_traders") or [])[:4],
                "trader_assignments": list(execution_directive.get("trader_assignments") or [])[:6],
            },
            "xau_cluster_loss_watch": {
                "status": "active" if bool(cluster_watch.get("active")) else "inactive",
                "mode": str(cluster_watch.get("mode") or "advisory_only"),
                "blocked_direction": str(cluster_watch.get("blocked_direction") or "").strip().lower(),
                "losses": int(cluster_watch.get("losses", 0) or 0),
                "resolved": int(cluster_watch.get("resolved", 0) or 0),
                "pnl_usd": float(cluster_watch.get("pnl_usd", 0.0) or 0.0),
                "enforcement": "advisory_only",
            },
            "opportunity_feed_symbol": {
                "active": bool(ranked_rows),
                "cadence_min": int(feed_symbol.get("cadence_min", max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))) or max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))),
                "family_priority_map": family_priority_map,
                "priority_families": ranked_families[:priority_topk],
                "support_all_families": support_all_families,
                "agent_targets": list(feed_symbol.get("agent_targets") or [])[:priority_topk],
                "coaching": list(feed_symbol.get("coaching") or [])[: max(priority_topk, 2)],
            },
        }
        return symbol_team, runtime_state

    def _build_generic_symbol_team(self, *, symbol_row: dict, feed_symbol: dict, strategy_lab_symbol: dict | None = None) -> tuple[dict, dict]:
        priority_map = {str(fam or "").strip().lower(): float(score or 0.0) for fam, score in dict(feed_symbol.get("family_priority_map") or {}).items() if str(fam or "").strip()}
        coaching = self._apply_strategy_lab_priority(score_map=priority_map, strategy_lab_symbol=strategy_lab_symbol)
        ranked_rows = self._priority_rows(priority_map)
        ranked_families = [str(row.get("family") or "") for row in list(ranked_rows or [])]
        symbol = str(symbol_row.get("symbol") or "").strip().upper()
        symbol_team = {
            "symbol": symbol,
            "bias_desk": {
                "selected_family": str(symbol_row.get("selected_family") or "").strip().lower(),
                "notes": list(symbol_row.get("manager_findings") or [])[:4],
            },
            "execution_desk": {
                "primary_family": str((ranked_rows[0] if ranked_rows else {}).get("family") or ""),
                "ranked_families": ranked_families[:3],
                "family_priority_map": priority_map,
                "support_all_families": [],
                "parallel_limit": 1,
            },
            "position_desk": {
                "open_positions": len(list(symbol_row.get("open_positions") or [])),
                "open_orders": len(list(symbol_row.get("open_orders") or [])),
            },
            "portfolio_desk": {},
            "strategy_lab_desk": self._strategy_lab_desk(strategy_lab_symbol),
            "coaching": list(feed_symbol.get("coaching") or [])[:3] + coaching[:2],
        }
        runtime_state = {
            "opportunity_feed_symbol": {
                "active": bool(ranked_rows),
                "cadence_min": int(feed_symbol.get("cadence_min", max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))) or max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))),
                "family_priority_map": priority_map,
                "priority_families": ranked_families[:3],
                "support_all_families": [],
                "agent_targets": list(feed_symbol.get("agent_targets") or [])[:3],
                "coaching": list(feed_symbol.get("coaching") or [])[:3],
            }
        }
        return symbol_team, runtime_state

    def build_report(self, *, manager_report: dict | None = None, strategy_lab_state: dict | None = None, experiment_report: dict | None = None) -> dict:
        report = dict(manager_report or self._load_json(self.manager_report_path) or {})
        lab_state = dict(strategy_lab_state or self._load_json(self.strategy_lab_state_path) or {})
        ct_experiment = dict(experiment_report or self._load_json(self.ct_experiment_report_path) or {})
        out = {
            "ok": bool(report.get("ok")),
            "generated_at": _utc_iso(),
            "source_report": str(self.manager_report_path),
            "manager_generated_at": str(report.get("generated_at") or ""),
            "manager_summary": dict(report.get("summary") or {}),
            "strategy_lab_summary": dict(lab_state.get("summary") or {}),
            "ct_experiment_summary": dict(ct_experiment.get("summary") or {}),
            "symbols": {},
            "runtime_state": {},
            "error": "" if bool(report.get("ok")) else str(report.get("error") or "manager_report_unavailable"),
        }
        state = {
            "status": "active" if bool(report.get("ok")) else "inactive",
            "generated_at": str(out.get("generated_at") or ""),
            "source_report": str(self.manager_report_path),
            "symbols": {},
            "opportunity_feed": {"status": "idle", "symbols": {}},
            "strategy_lab": {
                "status": str(lab_state.get("status") or "inactive"),
                "generated_at": str(lab_state.get("generated_at") or ""),
                "summary": dict(lab_state.get("summary") or {}),
            },
            "ct_only_experiment": {
                "status": "active" if bool(ct_experiment.get("ok")) else "inactive",
                "generated_at": str(ct_experiment.get("generated_at") or ""),
                "summary": dict(ct_experiment.get("summary") or {}),
            },
        }
        if not bool(report.get("ok")):
            self._save_json(self.report_dir / "trading_team_report.json", out)
            self._save_json(self.state_path, state)
            return out

        feed_symbols = dict((report.get("opportunity_feed") or {}).get("symbols") or {})
        lab_symbols = dict(lab_state.get("symbols") or {})
        for row in list(report.get("symbols") or []):
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            feed_symbol = dict(feed_symbols.get(symbol) or {})
            strategy_lab_symbol = dict(lab_symbols.get(symbol) or {})
            if symbol == "XAUUSD":
                symbol_team, runtime = self._build_xau_symbol_team(
                    symbol_row=row,
                    feed_symbol=feed_symbol,
                    strategy_lab_symbol=strategy_lab_symbol,
                    experiment_report=ct_experiment,
                )
                state["xau_family_routing"] = dict(runtime.get("xau_family_routing") or {})
                state["xau_family_budget"] = dict(runtime.get("xau_family_budget") or {})
                state["xau_parallel_families"] = dict(runtime.get("xau_parallel_families") or {})
                state["xau_hedge_transition"] = dict(runtime.get("xau_hedge_transition") or {})
                state["xau_opportunity_bypass"] = dict(runtime.get("xau_opportunity_bypass") or {})
                state["xau_opportunity_sidecar"] = dict(runtime.get("xau_opportunity_sidecar") or {})
                state["xau_order_care"] = dict(runtime.get("xau_order_care") or {})
                state["xau_regime_transition"] = dict(runtime.get("xau_regime_transition") or {})
                state["xau_execution_directive"] = dict(runtime.get("xau_execution_directive") or {})
                state["xau_cluster_loss_watch"] = dict(runtime.get("xau_cluster_loss_watch") or {})
                feed_entry = dict(runtime.get("opportunity_feed_symbol") or {})
            else:
                symbol_team, runtime = self._build_generic_symbol_team(symbol_row=row, feed_symbol=feed_symbol, strategy_lab_symbol=strategy_lab_symbol)
                feed_entry = dict(runtime.get("opportunity_feed_symbol") or {})
            out["symbols"][symbol] = symbol_team
            state["symbols"][symbol] = symbol_team
            if feed_entry:
                state["opportunity_feed"]["symbols"][symbol] = feed_entry
        if dict(state["opportunity_feed"].get("symbols") or {}):
            state["opportunity_feed"]["status"] = "active"
            state["opportunity_feed"]["generated_at"] = str(out.get("generated_at") or "")
            state["opportunity_feed"]["cadence_min"] = int((report.get("opportunity_feed") or {}).get("cadence_min", max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15))) or max(5, int(getattr(config, "TRADING_MANAGER_REPORT_INTERVAL_MIN", 15) or 15)))

        out["runtime_state"] = state
        self._save_json(self.report_dir / "trading_team_report.json", out)
        self._save_json(self.state_path, state)
        return out


trading_team_agent = TradingTeamAgent()
