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


class StrategyLabTeamAgent:
    _MODE_PRIORITY = {
        "promotable": 4,
        "live_shadow": 3,
        "shadow": 2,
        "replay_only": 1,
        "blocked": 0,
    }

    def __init__(self, *, report_dir: str | None = None, runtime_dir: str | None = None):
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / "data"
        self.report_dir = Path(report_dir or (data_dir / "reports"))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir = Path(runtime_dir or (data_dir / "runtime"))
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.runtime_dir / "strategy_lab_team_state.json"
        self.strategy_lab_report_path = self.report_dir / "strategy_lab_report.json"

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
    def _candidate_resolved(row: dict) -> int:
        gate = dict((row or {}).get("promotion_gate") or {})
        try:
            resolved = int(gate.get("candidate_resolved", 0) or 0)
        except Exception:
            resolved = 0
        if resolved > 0:
            return resolved
        evidence = dict((row or {}).get("family_evidence") or {})
        total = dict(evidence.get("observed_total") or {})
        try:
            return int(total.get("resolved", 0) or 0)
        except Exception:
            return 0

    @staticmethod
    def _candidate_score(row: dict) -> float:
        if (row or {}).get("router_score") is not None:
            return _safe_float((row or {}).get("router_score"), -9999.0)
        if (row or {}).get("walk_forward_score") is not None:
            return _safe_float((row or {}).get("walk_forward_score"), -9999.0)
        gate = dict((row or {}).get("promotion_gate") or {})
        if gate.get("candidate_score") is not None:
            return _safe_float(gate.get("candidate_score"), -9999.0)
        return -9999.0

    @classmethod
    def _mode_rank(cls, mode: str) -> int:
        return int(cls._MODE_PRIORITY.get(str(mode or "").strip().lower(), -1))

    def _candidate_mode(self, row: dict) -> str:
        gate = dict((row or {}).get("promotion_gate") or {})
        status = str((row or {}).get("status") or "").strip().lower()
        symbol = str((row or {}).get("symbol") or "").strip().upper()
        execution_ready = bool((row or {}).get("execution_ready", False))
        if bool(gate.get("eligible")) and execution_ready:
            return "promotable"
        if not execution_ready:
            return "replay_only"
        score = self._candidate_score(row)
        resolved = self._candidate_resolved(row)
        min_score = float(getattr(config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE", 8.0) or 8.0)
        min_resolved = max(1, int(getattr(config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED", 4) or 4))
        blockers = [str(item or "").strip().lower() for item in list(gate.get("blockers") or []) if str(item or "").strip()]
        if any(item.startswith("positive_score") for item in blockers) or score <= 0.0:
            return "blocked"
        if symbol != "XAUUSD" and status == "blocked":
            return "blocked"
        if score >= min_score and resolved >= min_resolved:
            return "live_shadow"
        return "shadow"

    def _candidate_snapshot(self, row: dict, *, mode: str) -> dict:
        gate = dict((row or {}).get("promotion_gate") or {})
        return {
            "symbol": str((row or {}).get("symbol") or "").strip().upper(),
            "strategy_id": str((row or {}).get("strategy_id") or "").strip(),
            "family": str((row or {}).get("family") or "").strip().lower(),
            "mode": str(mode or "").strip().lower(),
            "score": round(self._candidate_score(row), 4),
            "router_score": None if (row or {}).get("router_score") is None else round(_safe_float((row or {}).get("router_score")), 4),
            "walk_forward_score": None if (row or {}).get("walk_forward_score") is None else round(_safe_float((row or {}).get("walk_forward_score")), 4),
            "resolved": self._candidate_resolved(row),
            "priority": int((row or {}).get("priority", 999) or 999),
            "experimental": bool((row or {}).get("experimental")),
            "execution_ready": bool((row or {}).get("execution_ready", False)),
            "promotion_ready": bool(gate.get("eligible")),
            "status": str((row or {}).get("status") or "").strip().lower(),
            "blockers": list(gate.get("blockers") or [])[:4],
            "proposed_overrides": dict((row or {}).get("proposed_overrides") or {}),
        }

    @staticmethod
    def _sort_rows(rows: list[dict]) -> list[dict]:
        ranked = [dict(row or {}) for row in list(rows or []) if isinstance(row, dict)]
        ranked.sort(
            key=lambda item: (
                StrategyLabTeamAgent._mode_rank(str(item.get("mode") or "")),
                float(item.get("score", -9999.0) or -9999.0),
                int(item.get("resolved", 0) or 0),
                -int(item.get("priority", 999) or 999),
                str(item.get("family") or ""),
                str(item.get("strategy_id") or ""),
            ),
            reverse=True,
        )
        return ranked

    @staticmethod
    def _family_priority_map(rows: list[dict]) -> dict[str, float]:
        out: dict[str, float] = {}
        for row in list(rows or []):
            family = str(row.get("family") or "").strip().lower()
            if not family:
                continue
            score = float(row.get("score", -9999.0) or -9999.0)
            if family not in out or score > float(out.get(family, -9999.0) or -9999.0):
                out[family] = round(score, 4)
        return out

    def _family_states(self, rows: list[dict]) -> dict[str, str]:
        out: dict[str, str] = {}
        for row in list(rows or []):
            family = str(row.get("family") or "").strip().lower()
            mode = str(row.get("mode") or "").strip().lower()
            if not family or not mode:
                continue
            current = str(out.get(family) or "")
            if self._mode_rank(mode) >= self._mode_rank(current):
                out[family] = mode
        return out

    @staticmethod
    def _recovery_blocker_fatal(blockers: list[str]) -> bool:
        fatal_prefixes = ("positive_score", "max_dd")
        for blocker in list(blockers or []):
            token = str(blocker or "").strip().lower()
            if any(token.startswith(prefix) for prefix in fatal_prefixes):
                return True
        return False

    def _build_recovery_queue(self, *, symbol: str, ranked: list[dict]) -> list[dict]:
        if str(symbol or "").strip().upper() != "XAUUSD":
            return []
        if not bool(getattr(config, "STRATEGY_LAB_TEAM_RECOVERY_ENABLED", True)):
            return []
        topk = max(1, int(getattr(config, "STRATEGY_LAB_TEAM_RECOVERY_TOPK", 3) or 3))
        # Force-inject families from config (bypasses score/blocker gates)
        force_families = {
            str(f or "").strip().lower()
            for f in str(getattr(config, "STRATEGY_LAB_FORCE_RECOVERY_FAMILIES", "") or "").split(",")
            if str(f or "").strip()
        }
        recovery: list[dict] = []
        for row in list(ranked or []):
            family = str(row.get("family") or "").strip().lower()
            mode = str(row.get("mode") or "").strip().lower()
            # Force-recovery: bypass score/blocker checks
            if family in force_families and mode in {"shadow", "blocked"} and bool(row.get("execution_ready", False)):
                enriched = dict(row)
                enriched["recovery_reason"] = "config_force_recovery"
                recovery.append(enriched)
                continue
            score = float(row.get("score", -9999.0) or -9999.0)
            blockers = [str(item or "").strip() for item in list(row.get("blockers") or []) if str(item or "").strip()]
            if mode not in {"shadow", "blocked"}:
                continue
            if not bool(row.get("execution_ready", False)):
                continue
            if score <= 0.0:
                continue
            if self._recovery_blocker_fatal(blockers):
                continue
            enriched = dict(row)
            enriched["recovery_reason"] = "lab_positive_fallback"
            recovery.append(enriched)
        return recovery[:topk]

    def _merge_force_recovery_rows(self, *, symbol: str, ranked: list[dict], recovery: list[dict]) -> list[dict]:
        """Append STRATEGY_LAB_FORCE_RECOVERY_FAMILIES even when a live_shadow desk is active (recovery desk was skipped)."""
        if str(symbol or "").strip().upper() != "XAUUSD":
            return list(recovery or [])
        force_families = {
            str(f or "").strip().lower()
            for f in str(getattr(config, "STRATEGY_LAB_FORCE_RECOVERY_FAMILIES", "") or "").split(",")
            if str(f or "").strip()
        }
        if not force_families:
            return list(recovery or [])
        have = {str(r.get("family") or "").strip().lower() for r in (recovery or []) if str(r.get("family") or "").strip()}
        out = list(recovery or [])
        for row in list(ranked or []):
            if not isinstance(row, dict):
                continue
            fam = str(row.get("family") or "").strip().lower()
            if fam not in force_families or fam in have:
                continue
            mode = str(row.get("mode") or "").strip().lower()
            if mode not in {"shadow", "blocked"}:
                continue
            if not bool(row.get("execution_ready", False)):
                continue
            enriched = dict(row)
            enriched["recovery_reason"] = "config_force_recovery"
            out.append(enriched)
            have.add(fam)
        return out

    def build_report(self, *, strategy_lab_report: dict | None = None) -> dict:
        report = dict(strategy_lab_report or self._load_json(self.strategy_lab_report_path) or {})
        out = {
            "ok": bool(report.get("ok")),
            "generated_at": _utc_iso(),
            "source_report": str(self.strategy_lab_report_path),
            "lab_generated_at": str(report.get("generated_at") or ""),
            "lab_summary": dict(report.get("summary") or {}),
            "symbols": {},
            "runtime_state": {},
            "summary": {
                "candidate_count": 0,
                "replay_ready_count": 0,
                "shadow_count": 0,
                "live_shadow_count": 0,
                "recovery_count": 0,
                "promotion_count": 0,
                "blocked_count": 0,
            },
            "error": "" if bool(report.get("ok")) else str(report.get("error") or "strategy_lab_report_unavailable"),
        }
        state = {
            "status": "active" if bool(report.get("ok")) else "inactive",
            "generated_at": str(out.get("generated_at") or ""),
            "source_report": str(self.strategy_lab_report_path),
            "lab_generated_at": str(report.get("generated_at") or ""),
            "symbols": {},
            "summary": dict(out.get("summary") or {}),
        }
        if not bool(report.get("ok")):
            self._save_json(self.report_dir / "strategy_lab_team_report.json", out)
            self._save_json(self.state_path, state)
            return out

        topk = max(1, int(getattr(config, "STRATEGY_LAB_TEAM_TOPK", 5) or 5))
        symbol_rows: dict[str, list[dict]] = {}
        for row in list(report.get("candidates") or []):
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol") or "").strip().upper()
            if not sym:
                continue
            symbol_rows.setdefault(sym, []).append(dict(row))

        summary_counts = {
            "candidate_count": 0,
            "replay_ready_count": 0,
            "shadow_count": 0,
            "live_shadow_count": 0,
            "recovery_count": 0,
            "promotion_count": 0,
            "blocked_count": 0,
        }
        for symbol in sorted(list(symbol_rows.keys())):
            rows = [dict(row or {}) for row in list(symbol_rows.get(symbol) or [])]
            snapshots: list[dict] = []
            for row in rows:
                mode = self._candidate_mode(row)
                snapshots.append(self._candidate_snapshot(row, mode=mode))
            ranked = self._sort_rows(snapshots)
            replay_queue = ranked[:topk]
            shadow_queue = [row for row in list(ranked) if str(row.get("mode") or "") == "shadow"][:topk]
            live_shadow_queue = [row for row in list(ranked) if str(row.get("mode") or "") == "live_shadow"][:topk]
            promotion_queue = [row for row in list(ranked) if str(row.get("mode") or "") == "promotable"][:topk]
            blocked_queue = [row for row in list(ranked) if str(row.get("mode") or "") == "blocked"][:topk]
            recovery_queue = []
            if not promotion_queue and not live_shadow_queue:
                recovery_queue = self._build_recovery_queue(symbol=symbol, ranked=ranked)
            recovery_queue = self._merge_force_recovery_rows(symbol=symbol, ranked=ranked, recovery=recovery_queue)

            summary = {
                "candidate_count": len(ranked),
                "replay_ready_count": len(ranked),
                "shadow_count": len([row for row in ranked if str(row.get("mode") or "") == "shadow"]),
                "live_shadow_count": len([row for row in ranked if str(row.get("mode") or "") == "live_shadow"]),
                "recovery_count": len(recovery_queue),
                "promotion_count": len([row for row in ranked if str(row.get("mode") or "") == "promotable"]),
                "blocked_count": len([row for row in ranked if str(row.get("mode") or "") == "blocked"]),
            }
            for key, value in summary.items():
                summary_counts[key] = int(summary_counts.get(key, 0) or 0) + int(value or 0)

            promotion_family_priority_map = self._family_priority_map(promotion_queue)
            live_shadow_family_priority_map = self._family_priority_map(live_shadow_queue)
            recovery_family_priority_map = self._family_priority_map(recovery_queue)
            execution_family_priority_map = dict(promotion_family_priority_map)
            for family, score in live_shadow_family_priority_map.items():
                if family not in execution_family_priority_map or float(score or -9999.0) > float(execution_family_priority_map.get(family, -9999.0) or -9999.0):
                    execution_family_priority_map[family] = round(float(score or 0.0), 4)
            for family, score in recovery_family_priority_map.items():
                if family not in execution_family_priority_map or float(score or -9999.0) > float(execution_family_priority_map.get(family, -9999.0) or -9999.0):
                    execution_family_priority_map[family] = round(float(score or 0.0), 4)
            family_states = self._family_states(ranked)
            strategy_states = {
                str(row.get("strategy_id") or "").strip(): str(row.get("mode") or "").strip().lower()
                for row in list(ranked)
                if str(row.get("strategy_id") or "").strip()
            }

            symbol_report = {
                "symbol": symbol,
                "replay_desk": {
                    "ranked_candidates": replay_queue,
                    "leader": dict(replay_queue[0]) if replay_queue else {},
                },
                "shadow_desk": {
                    "families": [str(row.get("family") or "") for row in shadow_queue],
                    "candidates": shadow_queue,
                },
                "probation_desk": {
                    "families": [str(row.get("family") or "") for row in live_shadow_queue],
                    "candidates": live_shadow_queue,
                },
                "promotion_desk": {
                    "families": [str(row.get("family") or "") for row in promotion_queue],
                    "candidates": promotion_queue,
                },
                "recovery_desk": {
                    "families": [str(row.get("family") or "") for row in recovery_queue],
                    "candidates": recovery_queue,
                    "active": bool(recovery_queue),
                },
                "blocked_desk": {
                    "count": int(summary.get("blocked_count", 0) or 0),
                    "candidates": blocked_queue,
                },
                "summary": summary,
            }
            symbol_state = {
                "summary": summary,
                "strategy_states": strategy_states,
                "family_states": family_states,
                "shadow_queue": shadow_queue,
                "live_shadow_queue": live_shadow_queue,
                "promotion_queue": promotion_queue,
                "recovery_queue": recovery_queue,
                "blocked_queue": blocked_queue,
                "shadow_strategy_ids": [str(row.get("strategy_id") or "") for row in shadow_queue if str(row.get("strategy_id") or "")],
                "live_shadow_strategy_ids": [str(row.get("strategy_id") or "") for row in live_shadow_queue if str(row.get("strategy_id") or "")],
                "approved_strategy_ids": [str(row.get("strategy_id") or "") for row in promotion_queue if str(row.get("strategy_id") or "")],
                "recovery_strategy_ids": [str(row.get("strategy_id") or "") for row in recovery_queue if str(row.get("strategy_id") or "")],
                "blocked_strategy_ids": [str(row.get("strategy_id") or "") for row in blocked_queue if str(row.get("strategy_id") or "")],
                "shadow_families": [str(row.get("family") or "") for row in shadow_queue if str(row.get("family") or "")],
                "live_shadow_families": [str(row.get("family") or "") for row in live_shadow_queue if str(row.get("family") or "")],
                "approved_families": [str(row.get("family") or "") for row in promotion_queue if str(row.get("family") or "")],
                "recovery_families": [str(row.get("family") or "") for row in recovery_queue if str(row.get("family") or "")],
                "blocked_families": [str(row.get("family") or "") for row in blocked_queue if str(row.get("family") or "")],
                "promotion_family_priority_map": promotion_family_priority_map,
                "live_shadow_family_priority_map": live_shadow_family_priority_map,
                "recovery_family_priority_map": recovery_family_priority_map,
                "execution_family_priority_map": execution_family_priority_map,
            }
            out["symbols"][symbol] = symbol_report
            state["symbols"][symbol] = symbol_state

        out["summary"] = summary_counts
        state["summary"] = summary_counts
        out["runtime_state"] = state
        self._save_json(self.report_dir / "strategy_lab_team_report.json", out)
        self._save_json(self.state_path, state)
        return out


strategy_lab_team_agent = StrategyLabTeamAgent()
