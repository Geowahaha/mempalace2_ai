"""
openclaw/agents/risk_guard_agent.py — Risk Guard Agent

Real-time safety monitor. Checks:
  - Recent cluster losses (>= 3 losses in last 60min per family)
  - Open position count vs limits
  - Drawdown velocity (rapid loss accumulation)
  - Shock mode / suspended trading state

Can emit two types of protective actions:
  1. PTS proposals (normal route — for non-urgent tuning)
  2. Emergency directives (bypass PTS — writes directly to trading_manager_state.json)
     ONLY when: >= 3 losses in 60 min AND no cluster guard already active

Emergency directive types:
  - activate_cluster_loss_guard: pauses new entries
  - tighten_confidence_emergency: immediate +2.0 conf bump for a family
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from openclaw.agents.base import AgentResult, BaseAgent

logger = logging.getLogger(__name__)

# Thresholds
_CLUSTER_LOSS_WINDOW_MIN = 60       # minutes to look back for cluster
_CLUSTER_LOSS_COUNT = 3             # losses in window = alert
_DRAWDOWN_VELOCITY_USD = -15.0      # USD lost in 60 min = velocity alert
_MAX_OPEN_POSITIONS_WARN = 6        # warn above this
_MAX_OPEN_POSITIONS_HALT = 10       # emergency halt above this


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class RiskGuardAgent(BaseAgent):
    """
    Monitors live risk state and emits protective actions.

    Findings:
      - cluster_loss_detected: bool + details per family
      - drawdown_velocity_alert: bool + usd_per_hour
      - open_positions: count
      - shock_active: bool
      - actions_taken: list of emergency actions applied
      - proposals: list of PTS proposals for non-emergency adjustments
    """

    name = "risk_guard_agent"

    def run(self, context: dict) -> AgentResult:
        try:
            from config import config
        except Exception as exc:
            return self._error(f"import config: {exc}")

        runtime_dir = Path(getattr(config, "RUNTIME_DIR", "data/runtime"))
        db_path = Path(getattr(config, "CTRADER_DB_PATH", "data/ctrader_openapi.db"))

        # ── check cluster losses ─────────────────────────────────────────────
        cluster_alerts = self._check_cluster_losses(db_path)

        # ── check drawdown velocity ──────────────────────────────────────────
        dd_alert, dd_usd_hr = self._check_drawdown_velocity(db_path)

        # ── check open positions ─────────────────────────────────────────────
        open_count = self._count_open_positions(db_path)

        # ── check shock active ───────────────────────────────────────────────
        shock_active = self._check_shock_active(runtime_dir)

        # ── check cluster guard already active ──────────────────────────────
        cluster_guard_active = self._check_cluster_guard_active(runtime_dir)

        # ── decide actions ───────────────────────────────────────────────────
        emergency_actions: list[dict] = []
        pts_proposals: list[dict] = []

        # Emergency: cluster loss >= threshold AND no guard already on
        if cluster_alerts and not cluster_guard_active and not shock_active:
            worst = max(cluster_alerts, key=lambda x: x.get("loss_count", 0))
            if worst.get("loss_count", 0) >= _CLUSTER_LOSS_COUNT:
                action = self._apply_cluster_loss_guard(runtime_dir, worst)
                if action:
                    emergency_actions.append(action)

        # Emergency: open position limit breach
        if open_count >= _MAX_OPEN_POSITIONS_HALT:
            action = self._emit_halt_directive(runtime_dir, reason=f"open_positions={open_count} >= {_MAX_OPEN_POSITIONS_HALT}")
            if action:
                emergency_actions.append(action)

        # Non-emergency: drawdown velocity → PTS proposal to tighten
        if dd_alert and not cluster_guard_active:
            try:
                from learning.live_profile_autopilot import live_profile_autopilot as lpa
                param = "XAU_DIRECT_LANE_MIN_CONFIDENCE"
                real_current = getattr(config, param, 70.0)
                proposed = round(float(real_current or 70.0) + 1.0, 1)
                ceiling = float(getattr(config, "XAU_DIRECT_LANE_TRIAL_MAX_CONF_CEIL", 77.0) or 77.0)
                if proposed <= ceiling:
                    tid = lpa._propose_parameter_trial(
                        param=param,
                        current_value=real_current,
                        proposed_value=proposed,
                        direction="tighten",
                        reason=f"[risk_guard] drawdown_velocity {dd_usd_hr:.1f} USD/hr",
                        source="risk_guard_agent",
                    )
                    if tid:
                        pts_proposals.append({"trial_id": tid, "param": param, "proposed_value": proposed})
            except Exception as exc:
                logger.debug("[risk_guard_agent] PTS proposal error: %s", exc)

        findings = {
            "cluster_alerts": cluster_alerts,
            "drawdown_velocity_alert": dd_alert,
            "drawdown_usd_per_hour": round(dd_usd_hr, 2),
            "open_positions": open_count,
            "shock_active": shock_active,
            "cluster_guard_active": cluster_guard_active,
            "emergency_actions_taken": emergency_actions,
            "pts_proposals": pts_proposals,
        }

        # Confidence = how certain we are the risk assessment is complete
        has_db = db_path.exists()
        confidence = 0.90 if has_db else 0.40

        return self._ok(findings=findings, proposals=pts_proposals, confidence=confidence)

    # ── cluster loss check ───────────────────────────────────────────────────

    def _check_cluster_losses(self, db_path: Path) -> list[dict]:
        if not db_path.exists():
            return []
        from datetime import datetime, timezone
        cutoff = _iso(_utc_now() - timedelta(minutes=_CLUSTER_LOSS_WINDOW_MIN))
        is_weekend = _utc_now().weekday() >= 5
        try:
            import sqlite3
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                # On weekends: check crypto sources only (XAU is closed)
                # On weekdays: check all sources
                if is_weekend:
                    where_extra = "AND (source LIKE '%btcusd%' OR source LIKE '%ethusd%' OR source LIKE '%btc%' OR source LIKE '%eth%')"
                else:
                    where_extra = ""
                rows = conn.execute(
                    f"""
                    SELECT source, COUNT(*) as loss_count, SUM(pnl_usd) as total_pnl
                    FROM execution_journal
                    WHERE closed_at >= ?
                      AND outcome = 0
                      {where_extra}
                    GROUP BY source
                    HAVING loss_count >= 2
                    ORDER BY loss_count DESC
                    """,
                    (cutoff,),
                ).fetchall()
                return [
                    {
                        "source": r["source"],
                        "loss_count": r["loss_count"],
                        "total_pnl": round(float(r["total_pnl"] or 0), 2),
                    }
                    for r in rows
                ]
        except Exception as exc:
            logger.debug("[risk_guard_agent] cluster loss query error: %s", exc)
            return []

    # ── drawdown velocity check ──────────────────────────────────────────────

    def _check_drawdown_velocity(self, db_path: Path) -> tuple[bool, float]:
        if not db_path.exists():
            return False, 0.0
        cutoff = _iso(_utc_now() - timedelta(minutes=_CLUSTER_LOSS_WINDOW_MIN))
        is_weekend = _utc_now().weekday() >= 5
        try:
            import sqlite3
            with sqlite3.connect(str(db_path)) as conn:
                if is_weekend:
                    where_extra = "AND (source LIKE '%btcusd%' OR source LIKE '%ethusd%' OR source LIKE '%btc%' OR source LIKE '%eth%')"
                else:
                    where_extra = ""
                row = conn.execute(
                    f"SELECT SUM(pnl_usd) as total FROM execution_journal WHERE closed_at >= ? {where_extra}",
                    (cutoff,),
                ).fetchone()
                total = float(row[0] or 0) if row else 0.0
                usd_hr = total  # already a 60-min window
                alert = usd_hr <= _DRAWDOWN_VELOCITY_USD
                return alert, usd_hr
        except Exception as exc:
            logger.debug("[risk_guard_agent] drawdown velocity error: %s", exc)
            return False, 0.0

    # ── open position count ──────────────────────────────────────────────────

    def _count_open_positions(self, db_path: Path) -> int:
        if not db_path.exists():
            return 0
        try:
            import sqlite3
            with sqlite3.connect(str(db_path)) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM execution_journal WHERE status IN ('open','pending')"
                ).fetchone()
                return int(row[0] or 0) if row else 0
        except Exception:
            return 0

    # ── shock / cluster guard state ──────────────────────────────────────────

    def _check_shock_active(self, runtime_dir: Path) -> bool:
        state = self._load_tm_state(runtime_dir)
        shock = dict(state.get("xau_shock_profile") or {})
        return str(shock.get("status", "inactive")).lower() == "active"

    def _check_cluster_guard_active(self, runtime_dir: Path) -> bool:
        state = self._load_tm_state(runtime_dir)
        guard = dict(state.get("xau_cluster_loss_guard") or {})
        return str(guard.get("status", "inactive")).lower() == "active"

    @staticmethod
    def _load_tm_state(runtime_dir: Path) -> dict:
        try:
            with open(runtime_dir / "trading_manager_state.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    # ── emergency actions ────────────────────────────────────────────────────

    def _apply_cluster_loss_guard(self, runtime_dir: Path, cluster_info: dict) -> dict | None:
        """Write xau_cluster_loss_guard → active directly to trading_manager_state.json."""
        try:
            state_path = runtime_dir / "trading_manager_state.json"
            state = self._load_tm_state(runtime_dir)
            now_iso = _iso(_utc_now())
            state["xau_cluster_loss_guard"] = {
                "status": "active",
                "activated_at": now_iso,
                "mode": "risk_guard_emergency",
                "reason": f"[risk_guard] cluster_loss: {cluster_info.get('source')} "
                          f"x{cluster_info.get('loss_count')} in 60min "
                          f"({cluster_info.get('total_pnl', 0):.1f} USD)",
                "source": cluster_info.get("source"),
                "loss_count": cluster_info.get("loss_count"),
            }
            with open(state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            logger.warning(
                "[risk_guard_agent] EMERGENCY: activated cluster_loss_guard — %s x%s",
                cluster_info.get("source"),
                cluster_info.get("loss_count"),
            )
            return {
                "action": "activate_cluster_loss_guard",
                "source": cluster_info.get("source"),
                "loss_count": cluster_info.get("loss_count"),
                "applied_at": now_iso,
            }
        except Exception as exc:
            logger.error("[risk_guard_agent] emergency write error: %s", exc)
            return None

    def _emit_halt_directive(self, runtime_dir: Path, reason: str) -> dict | None:
        """Write xau_execution_directive → halt to pause all new XAU entries."""
        try:
            state_path = runtime_dir / "trading_manager_state.json"
            state = self._load_tm_state(runtime_dir)
            now_iso = _iso(_utc_now())
            state["xau_execution_directive"] = {
                "status": "active",
                "activated_at": now_iso,
                "mode": "halt_all",
                "reason": f"[risk_guard] {reason}",
            }
            with open(state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            logger.warning("[risk_guard_agent] EMERGENCY: halt_all directive — %s", reason)
            return {
                "action": "halt_all_directive",
                "reason": reason,
                "applied_at": now_iso,
            }
        except Exception as exc:
            logger.error("[risk_guard_agent] halt directive error: %s", exc)
            return None
