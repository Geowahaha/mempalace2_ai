"""
learning/neural_gate_learning_loop.py

Continuous feedback loop for neural execution gate (cTrader OpenAPI SQLite only):
1) Sync gate decisions from execution_journal (ctrader_openapi.db, journal_id = row_id + offset).
2) Resolve outcomes from:
   - real cTrader closed deals / journal close rows (high trust)
   - shadow/counterfactual scalp outcomes (lower trust)
3) Calibrate per-scope canary policy (XAU scalp by default).
4) Publish runtime policy JSON + human-readable mission report.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(v: str) -> Optional[datetime]:
    raw = str(v or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _norm_symbol(v: str) -> str:
    return str(v or "").strip().upper()


def _prob_band(prob: float | None) -> str:
    if prob is None:
        return "none"
    p = float(prob)
    cuts = [0.55, 0.57, 0.59, 0.60, 0.62, 0.65, 0.70, 0.80, 1.01]
    if p < cuts[0]:
        return f"<{cuts[0]:.2f}"
    for lo, hi in zip(cuts[:-1], cuts[1:]):
        if lo <= p < hi:
            return f"[{lo:.2f},{hi:.2f})"
    return f">={cuts[-2]:.2f}"


def _quantile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    arr = sorted(float(v) for v in values)
    qv = max(0.0, min(1.0, float(q)))
    if len(arr) == 1:
        return arr[0]
    pos = (len(arr) - 1) * qv
    lo = int(pos)
    hi = min(lo + 1, len(arr) - 1)
    frac = pos - lo
    return arr[lo] * (1.0 - frac) + arr[hi] * frac


def _agg(rows: list[dict]) -> dict:
    rows2 = [r for r in rows if r.get("outcome") in (0, 1)]
    n = len(rows2)
    wins = sum(1 for r in rows2 if int(r.get("outcome", -1)) == 1)
    losses = sum(1 for r in rows2 if int(r.get("outcome", -1)) == 0)
    wr = round((wins / max(1, wins + losses)) * 100.0, 2) if (wins + losses) > 0 else None
    net = round(sum(_safe_float(r.get("pnl_usd"), 0.0) for r in rows2), 2)
    avg_prob = (
        round(sum(_safe_float(r.get("neural_prob"), 0.0) for r in rows2) / max(1, n), 4)
        if n > 0
        else None
    )
    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": wr,
        "net_pnl_usd": net,
        "avg_neural_prob": avg_prob,
    }


@dataclass
class LoopRun:
    ok: bool
    synced: int
    resolved_real: int
    resolved_shadow: int
    unresolved: int
    policy_active: bool
    message: str
    report_path: str = ""
    policy_path: str = ""


class NeuralGateLearningLoop:
    def __init__(self):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        runtime_dir = data_dir / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        mission_dir = data_dir / "mission_reports"
        mission_dir.mkdir(parents=True, exist_ok=True)

        self.scalp_db_path = data_dir / "scalp_signal_history.db"
        loop_cfg = str(getattr(config, "NEURAL_GATE_LEARNING_DB_PATH", "") or "").strip()
        self.loop_db_path = Path(loop_cfg) if loop_cfg else (data_dir / "neural_gate_learning.db")
        policy_cfg = str(getattr(config, "NEURAL_GATE_CANARY_POLICY_PATH", "") or "").strip()
        self.policy_path = Path(policy_cfg) if policy_cfg else (runtime_dir / "neural_gate_canary_policy.json")
        self.latest_report_path = runtime_dir / "neural_gate_loop_latest.json"
        self.mission_dir = mission_dir

        self._lock = threading.Lock()
        self._policy_cache: dict = {}
        self._policy_cache_mtime: float = 0.0
        self._init_db()

    @property
    def ctrader_db_path(self) -> Path:
        db_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
        data_dir = Path(__file__).resolve().parent.parent / "data"
        return Path(db_cfg) if db_cfg else (data_dir / "ctrader_openapi.db")

    def _ctrader_journal_offset(self) -> int:
        return max(1, int(getattr(config, "NEURAL_GATE_JOURNAL_ID_CTRADER_OFFSET", 1_000_000_000) or 1_000_000_000))

    def _ctrader_sync_eligible(self) -> bool:
        if not bool(getattr(config, "NEURAL_GATE_CTRADER_SYNC_ENABLED", True)):
            return False
        if not bool(getattr(config, "CTRADER_ENABLED", False)):
            return False
        return self.ctrader_db_path.exists()

    @property
    def enabled(self) -> bool:
        if not bool(getattr(config, "NEURAL_GATE_LEARNING_ENABLED", True)):
            return False
        return self._ctrader_sync_eligible()

    @staticmethod
    def _safe_json_dict(raw: str) -> dict:
        try:
            v = json.loads(str(raw or "") or "{}")
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}

    def _ctrader_row_created_iso(self, created_ts: float | None, created_utc: str | None) -> str:
        try:
            ts = float(created_ts or 0.0)
            if ts > 1e8:
                return _iso(datetime.fromtimestamp(ts, tz=timezone.utc))
        except Exception:
            pass
        raw = str(created_utc or "").strip()
        if raw:
            dt = _parse_iso(raw) if raw.endswith("Z") else None
            if dt is None:
                try:
                    norm = raw.replace(" ", "T", 1)
                    if not norm.endswith("Z"):
                        norm = f"{norm}Z"
                    dt = _parse_iso(norm)
                except Exception:
                    dt = None
            if dt is None and len(raw) >= 19:
                try:
                    dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    dt = None
            if dt is not None:
                return _iso(dt)
        return _iso()

    def _connect_loop(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.loop_db_path), timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _connect_scalp(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.scalp_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _neural_gate_decisions_column_names(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute("PRAGMA table_info(neural_gate_decisions)").fetchall()
        return {str(r[1]) for r in rows} if rows else set()

    def _migrate_neural_gate_exec_columns(self, conn: sqlite3.Connection) -> None:
        """Rename mt5_status/mt5_message -> exec_status/exec_message (SQLite 3.25+); rebuild if unsupported."""
        chk = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neural_gate_decisions'"
        ).fetchone()
        if not chk:
            return
        cols = self._neural_gate_decisions_column_names(conn)
        if "exec_status" in cols and "exec_message" in cols:
            return
        if "mt5_status" not in cols and "mt5_message" not in cols:
            return
        try:
            if "mt5_status" in cols and "exec_status" not in cols:
                conn.execute("ALTER TABLE neural_gate_decisions RENAME COLUMN mt5_status TO exec_status")
            cols = self._neural_gate_decisions_column_names(conn)
            if "mt5_message" in cols and "exec_message" not in cols:
                conn.execute("ALTER TABLE neural_gate_decisions RENAME COLUMN mt5_message TO exec_message")
        except sqlite3.OperationalError as e:
            logger.warning("[NeuralGateLoop] exec column rename failed (%s); rebuilding neural_gate_decisions", e)
            self._rebuild_neural_gate_decisions_exec_columns(conn)

    def _rebuild_neural_gate_decisions_exec_columns(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            DROP INDEX IF EXISTS idx_ngd_scope_time;
            DROP INDEX IF EXISTS idx_ngd_resolved;
            ALTER TABLE neural_gate_decisions RENAME TO neural_gate_decisions__old;
            CREATE TABLE neural_gate_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                journal_id INTEGER UNIQUE NOT NULL,
                created_at TEXT NOT NULL,
                account_key TEXT,
                source TEXT,
                signal_symbol TEXT,
                broker_symbol TEXT,
                direction TEXT,
                confidence REAL,
                neural_prob REAL,
                min_prob REAL,
                min_prob_reason TEXT,
                exec_status TEXT,
                exec_message TEXT,
                decision TEXT,
                decision_reason TEXT,
                entry REAL,
                stop_loss REAL,
                take_profit_2 REAL,
                force_mode INTEGER,
                m1_not_confirmed INTEGER,
                canary_applied INTEGER,
                features_json TEXT,
                resolved INTEGER NOT NULL DEFAULT 0,
                outcome_type TEXT,
                outcome INTEGER,
                outcome_label TEXT,
                pnl_usd REAL,
                outcome_ref TEXT,
                weight REAL,
                resolved_at TEXT
            );
            INSERT INTO neural_gate_decisions(
                id, journal_id, created_at, account_key, source, signal_symbol, broker_symbol, direction,
                confidence, neural_prob, min_prob, min_prob_reason, exec_status, exec_message,
                decision, decision_reason, entry, stop_loss, take_profit_2, force_mode,
                m1_not_confirmed, canary_applied, features_json, resolved, outcome_type, outcome,
                outcome_label, pnl_usd, outcome_ref, weight, resolved_at
            )
            SELECT
                id, journal_id, created_at, account_key, source, signal_symbol, broker_symbol, direction,
                confidence, neural_prob, min_prob, min_prob_reason,
                mt5_status, mt5_message,
                decision, decision_reason, entry, stop_loss, take_profit_2, force_mode,
                m1_not_confirmed, canary_applied, features_json, resolved, outcome_type, outcome,
                outcome_label, pnl_usd, outcome_ref, weight, resolved_at
            FROM neural_gate_decisions__old;
            DROP TABLE neural_gate_decisions__old;
            CREATE INDEX IF NOT EXISTS idx_ngd_scope_time ON neural_gate_decisions(source, signal_symbol, created_at);
            CREATE INDEX IF NOT EXISTS idx_ngd_resolved ON neural_gate_decisions(resolved, source, signal_symbol, created_at);
            """
        )

    def _init_db(self) -> None:
        with self._lock:
            with closing(self._connect_loop()) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS neural_gate_decisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        journal_id INTEGER UNIQUE NOT NULL,
                        created_at TEXT NOT NULL,
                        account_key TEXT,
                        source TEXT,
                        signal_symbol TEXT,
                        broker_symbol TEXT,
                        direction TEXT,
                        confidence REAL,
                        neural_prob REAL,
                        min_prob REAL,
                        min_prob_reason TEXT,
                        exec_status TEXT,
                        exec_message TEXT,
                        decision TEXT,
                        decision_reason TEXT,
                        entry REAL,
                        stop_loss REAL,
                        take_profit_2 REAL,
                        force_mode INTEGER,
                        m1_not_confirmed INTEGER,
                        canary_applied INTEGER,
                        features_json TEXT,
                        resolved INTEGER NOT NULL DEFAULT 0,
                        outcome_type TEXT,
                        outcome INTEGER,
                        outcome_label TEXT,
                        pnl_usd REAL,
                        outcome_ref TEXT,
                        weight REAL,
                        resolved_at TEXT
                    )
                    """
                )
                self._migrate_neural_gate_exec_columns(conn)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ngd_scope_time ON neural_gate_decisions(source, signal_symbol, created_at)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ngd_resolved ON neural_gate_decisions(resolved, source, signal_symbol, created_at)"
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS neural_gate_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        summary_json TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

    @staticmethod
    def _decision_from_ctrader(status: str, message: str) -> tuple[str, str]:
        st = str(status or "").strip().lower()
        msg = str(message or "").strip()
        msg_l = msg.lower()
        if "neural filter:" in msg_l or "below_neural" in msg_l:
            return "neural_block", "below_neural_min_prob"
        if st == "guard_blocked":
            return "allow", "blocked_post_neural_guard"
        if st == "skipped":
            return "other_skip", "non_neural_skip"
        if st in {
            "filled",
            "dry_run",
            "accepted",
            "rejected",
            "error",
            "invalid_stops",
            "closed",
            "canceled",
            "pending",
            "submitted",
            "reconciled_open",
        }:
            return "allow", "passed_neural_gate"
        if st in {"filtered", "disabled", "unavailable", "invalid", "blocked"}:
            return "other_skip", "non_neural_skip"
        return st or "unknown", "unknown"

    @staticmethod
    def _neural_prob_from_request_json(request_json: str) -> float | None:
        payload = NeuralGateLearningLoop._safe_json_dict(request_json)
        raw_scores = dict(payload.get("raw_scores") or {})
        for key in ("neural_probability", "mt5_neural_probability", "mt5_neural_prob"):
            if raw_scores.get(key) is not None:
                return _safe_float(raw_scores.get(key), None)
        for key in ("neural_probability", "mt5_neural_probability", "mt5_neural_prob"):
            if payload.get(key) is not None:
                return _safe_float(payload.get(key), None)
        return None

    @staticmethod
    def _extract_features(extra_json: str) -> tuple[dict, bool, bool, bool, float | None, str]:
        force_mode = False
        m1_not_confirmed = False
        canary_applied = False
        min_prob = None
        min_prob_reason = ""
        features: dict = {}
        raw_scores = {}
        try:
            extra = json.loads(str(extra_json or "")) if extra_json else {}
            raw_scores = dict((extra or {}).get("raw_scores", {}) or {})
        except Exception:
            raw_scores = {}
        if raw_scores:
            force_mode = bool(raw_scores.get("scalp_force_mode", False))
            canary_applied = bool(raw_scores.get("mt5_neural_canary_applied", False))
            trig = raw_scores.get("scalping_trigger")
            if isinstance(trig, dict):
                reason = str(trig.get("reason", "") or "").lower()
                if "m1_" in reason and "not_confirmed" in reason:
                    m1_not_confirmed = True
            if raw_scores.get("mt5_neural_min_prob") is not None:
                min_prob = _safe_float(raw_scores.get("mt5_neural_min_prob"), None)
            min_prob_reason = str(raw_scores.get("mt5_neural_min_prob_reason", "") or "")
            features = {
                "pattern": str(raw_scores.get("pattern", "") or ""),
                "session_zone": str(raw_scores.get("session_zone", "") or raw_scores.get("kill_zone", "") or ""),
                "scalp_force_mode": force_mode,
                "m1_not_confirmed": m1_not_confirmed,
                "canary_applied": canary_applied,
                "raw_scores": raw_scores,
            }
        return features, force_mode, m1_not_confirmed, canary_applied, min_prob, min_prob_reason

    def sync_decisions_from_ctrader_journal(self, lookback_hours: int = 168) -> dict:
        if not self._ctrader_sync_eligible():
            return {"ok": False, "scanned": 0, "inserted": 0, "updated": 0, "message": "ctrader_sync_not_eligible"}
        since_ts = (_utc_now() - timedelta(hours=max(24, int(lookback_hours or 168)))).timestamp()
        offset = self._ctrader_journal_offset()
        inserted = 0
        updated = 0
        scanned = 0
        with closing(sqlite3.connect(str(self.ctrader_db_path), timeout=15)) as src_conn, closing(self._connect_loop()) as dst_conn:
            src_conn.row_factory = sqlite3.Row
            rows = src_conn.execute(
                """
                SELECT id, created_ts, created_utc, source, symbol, direction, confidence,
                       entry, stop_loss, take_profit, broker_symbol, status, message,
                       request_json, account_id, dry_run
                FROM execution_journal
                WHERE created_ts >= ?
                ORDER BY id ASC
                """,
                (since_ts,),
            ).fetchall()
            scanned = len(rows)
            for r in rows:
                cid = _safe_int(r["id"], 0)
                if cid <= 0:
                    continue
                journal_id = int(offset + cid)
                decision, decision_reason = self._decision_from_ctrader(r["status"], r["message"])
                feat, force_mode, m1_not_conf, canary_applied, min_prob_rs, min_prob_reason_rs = self._extract_features(
                    str(r["request_json"] or "")
                )
                neural_prob = self._neural_prob_from_request_json(str(r["request_json"] or ""))
                min_prob = min_prob_rs
                if min_prob is None:
                    sym = _norm_symbol(r["symbol"])
                    min_prob = _safe_float(
                        (config.get_neural_min_prob_symbol_overrides() or {}).get(
                            sym, getattr(config, "NEURAL_BRAIN_MIN_PROB", 0.55)
                        ),
                        getattr(config, "NEURAL_BRAIN_MIN_PROB", 0.55),
                    )
                min_prob_reason = min_prob_reason_rs or "fallback"
                account_key = str(int(r["account_id"])) if r["account_id"] is not None else ""
                created_iso = self._ctrader_row_created_iso(r["created_ts"], str(r["created_utc"] or ""))
                take_tp2 = _safe_float(r["take_profit"], 0.0)
                payload = (
                    journal_id,
                    created_iso,
                    account_key,
                    str(r["source"] or ""),
                    str(r["symbol"] or ""),
                    str(r["broker_symbol"] or ""),
                    str(r["direction"] or ""),
                    _safe_float(r["confidence"], 0.0),
                    neural_prob,
                    _safe_float(min_prob, 0.55),
                    str(min_prob_reason or ""),
                    str(r["status"] or ""),
                    str(r["message"] or ""),
                    str(decision or ""),
                    str(decision_reason or ""),
                    _safe_float(r["entry"], 0.0),
                    _safe_float(r["stop_loss"], 0.0),
                    take_tp2,
                    1 if force_mode else 0,
                    1 if m1_not_conf else 0,
                    1 if canary_applied else 0,
                    json.dumps(feat or {}, ensure_ascii=True, separators=(",", ":")),
                )
                cur = dst_conn.execute(
                    "SELECT id FROM neural_gate_decisions WHERE journal_id=?",
                    (journal_id,),
                ).fetchone()
                if cur:
                    dst_conn.execute(
                        """
                        UPDATE neural_gate_decisions
                        SET created_at=?, account_key=?, source=?, signal_symbol=?, broker_symbol=?, direction=?,
                            confidence=?, neural_prob=?, min_prob=?, min_prob_reason=?,
                            exec_status=?, exec_message=?, decision=?, decision_reason=?,
                            entry=?, stop_loss=?, take_profit_2=?, force_mode=?, m1_not_confirmed=?,
                            canary_applied=?, features_json=?
                        WHERE journal_id=?
                        """,
                        (
                            payload[1],
                            payload[2],
                            payload[3],
                            payload[4],
                            payload[5],
                            payload[6],
                            payload[7],
                            payload[8],
                            payload[9],
                            payload[10],
                            payload[11],
                            payload[12],
                            payload[13],
                            payload[14],
                            payload[15],
                            payload[16],
                            payload[17],
                            payload[18],
                            payload[19],
                            payload[20],
                            payload[21],
                            payload[0],
                        ),
                    )
                    updated += 1
                else:
                    dst_conn.execute(
                        """
                        INSERT INTO neural_gate_decisions(
                            journal_id, created_at, account_key, source, signal_symbol, broker_symbol, direction,
                            confidence, neural_prob, min_prob, min_prob_reason, exec_status, exec_message,
                            decision, decision_reason, entry, stop_loss, take_profit_2, force_mode,
                            m1_not_confirmed, canary_applied, features_json,
                            resolved, outcome_type, outcome, outcome_label, pnl_usd, outcome_ref, weight, resolved_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            0, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                        )
                        """,
                        payload,
                    )
                    inserted += 1
            dst_conn.commit()
        return {"ok": True, "scanned": scanned, "inserted": inserted, "updated": updated}

    def _resolve_real_ctrader(self, conn: sqlite3.Connection) -> int:
        offset = self._ctrader_journal_offset()
        updated = 0
        rows = conn.execute(
            """
            SELECT d.id, d.journal_id, j.id AS ejid, j.status, j.execution_meta_json, j.response_json, j.dry_run
            FROM neural_gate_decisions d
            JOIN ct.execution_journal j ON j.id = (d.journal_id - ?)
            WHERE d.resolved=0
              AND d.decision='allow'
              AND d.journal_id >= ?
            ORDER BY d.id ASC
            """,
            (offset, offset),
        ).fetchall()
        for row in rows:
            if int(row["dry_run"] or 0) == 1:
                continue
            ejid = int(row["ejid"] or 0)
            st = str(row["status"] or "").strip().lower()
            meta = self._safe_json_dict(str(row["execution_meta_json"] or ""))
            if bool(meta.get("exclude_from_training")):
                logger.info("[NeuralGate] skip journal_id=%s exclude_from_training reason=%s", ejid, meta.get("exclude_reason", ""))
                continue
            resp = self._safe_json_dict(str(row["response_json"] or ""))
            pnl_f: float | None = None
            if st == "closed":
                closed = meta.get("closed") if isinstance(meta.get("closed"), dict) else {}
                cd = resp.get("close_deal") if isinstance(resp.get("close_deal"), dict) else {}
                if closed.get("pnl_usd") is not None:
                    pnl_f = _safe_float(closed.get("pnl_usd"), None)
                if pnl_f is None and cd.get("pnl_usd") is not None:
                    pnl_f = _safe_float(cd.get("pnl_usd"), None)
            if pnl_f is None:
                agg = conn.execute(
                    """
                    SELECT COALESCE(SUM(pnl_usd), 0) AS s, COUNT(*) AS n
                    FROM ct.ctrader_deals
                    WHERE journal_id=? AND has_close_detail=1
                    """,
                    (ejid,),
                ).fetchone()
                if agg is not None and int(agg[1] or 0) > 0:
                    pnl_f = _safe_float(agg[0], None)
            if pnl_f is None:
                continue
            pnl_f = float(pnl_f)
            if abs(pnl_f) < 1e-12:
                outcome = None
                label = "flat"
            elif pnl_f > 0:
                outcome = 1
                label = "win"
            else:
                outcome = 0
                label = "loss"
            conn.execute(
                """
                UPDATE neural_gate_decisions
                SET resolved=1, outcome_type='real_ctrader', outcome=?, outcome_label=?, pnl_usd=?,
                    outcome_ref='ctrader_execution_journal', weight=1.0, resolved_at=?
                WHERE id=?
                """,
                (outcome, label, pnl_f, _iso(), int(row["id"])),
            )
            updated += 1
        return updated

    def _match_shadow_outcome(self, scalp_conn: sqlite3.Connection, row: sqlite3.Row) -> tuple[bool, dict]:
        created_dt = _parse_iso(str(row["created_at"] or ""))
        if created_dt is None:
            return False, {}
        ts = float(created_dt.timestamp())
        symbol = _norm_symbol(row["signal_symbol"])
        direction = str(row["direction"] or "").strip().lower()
        entry = _safe_float(row["entry"], 0.0)
        if not symbol or not direction:
            return False, {}
        win_sec = max(60, int(getattr(config, "NEURAL_GATE_SHADOW_MATCH_WINDOW_SEC", 300) or 300))
        entry_tol = max(0.001, float(getattr(config, "NEURAL_GATE_SHADOW_MATCH_ENTRY_TOL", 0.20) or 0.20))
        rs = scalp_conn.execute(
            """
            SELECT id, timestamp, entry, outcome, pnl_usd
            FROM scalp_signals
            WHERE UPPER(symbol)=?
              AND LOWER(direction)=?
              AND timestamp BETWEEN ? AND ?
            ORDER BY ABS(timestamp - ?) ASC, ABS(entry - ?) ASC
            LIMIT 5
            """,
            (symbol, direction, ts - win_sec, ts + win_sec, ts, entry),
        ).fetchall()
        for sr in rs:
            de = abs(_safe_float(sr["entry"], 0.0) - entry)
            if de > entry_tol:
                continue
            out = str(sr["outcome"] or "").strip().lower()
            if out in {"", "pending"}:
                continue
            if out.startswith("tp"):
                outcome = 1
            elif out.startswith("sl"):
                outcome = 0
            else:
                continue
            return True, {
                "outcome": outcome,
                "label": out,
                "pnl_usd": _safe_float(sr["pnl_usd"], 0.0),
                "ref": f"scalp_signals:{_safe_int(sr['id'], 0)}",
            }
        return False, {}

    def resolve_outcomes(self) -> dict:
        resolved_real = 0
        resolved_shadow = 0
        unresolved = 0
        with closing(self._connect_loop()) as loop_conn:
            loop_conn.row_factory = sqlite3.Row
            if self._ctrader_sync_eligible():
                try:
                    loop_conn.execute("ATTACH DATABASE ? AS ct", (str(self.ctrader_db_path),))
                    resolved_real += self._resolve_real_ctrader(loop_conn)
                except Exception as e:
                    logger.warning("[NeuralGateLoop] ctrader attach/resolve error: %s", e)
                try:
                    loop_conn.execute("DETACH DATABASE ct")
                except Exception:
                    pass
            with closing(self._connect_scalp()) as scalp_conn:
                rows = loop_conn.execute(
                    """
                    SELECT id, journal_id, created_at, signal_symbol, direction, entry
                    FROM neural_gate_decisions
                    WHERE resolved=0
                      AND decision='neural_block'
                    ORDER BY id ASC
                    """
                ).fetchall()
                for r in rows:
                    ok, payload = self._match_shadow_outcome(scalp_conn, r)
                    if not ok:
                        continue
                    loop_conn.execute(
                        """
                        UPDATE neural_gate_decisions
                        SET resolved=1, outcome_type='shadow_counterfactual',
                            outcome=?, outcome_label=?, pnl_usd=?, outcome_ref=?, weight=?, resolved_at=?
                        WHERE id=?
                        """,
                        (
                            int(payload["outcome"]),
                            str(payload["label"]),
                            _safe_float(payload["pnl_usd"], 0.0),
                            str(payload["ref"]),
                            max(0.05, float(getattr(config, "NEURAL_GATE_SHADOW_WEIGHT", 0.35) or 0.35)),
                            _iso(),
                            int(r["id"]),
                        ),
                    )
                    resolved_shadow += 1
            unresolved = _safe_int(
                loop_conn.execute("SELECT COUNT(*) FROM neural_gate_decisions WHERE resolved=0").fetchone()[0], 0
            )
            loop_conn.commit()
        return {
            "ok": True,
            "resolved_real": resolved_real,
            "resolved_shadow": resolved_shadow,
            "unresolved": unresolved,
        }

    def _base_min_prob_for_symbol(self, symbol: str) -> float:
        sym = _norm_symbol(symbol)
        base = float(getattr(config, "NEURAL_BRAIN_MIN_PROB", 0.55) or 0.55)
        try:
            overrides = config.get_neural_min_prob_symbol_overrides()
            if sym in overrides:
                base = float(overrides[sym])
        except Exception:
            pass
        return max(0.0, min(0.99, float(base)))

    def _collect_scope_rows(self, symbol: str, source: str, lookback_days: int) -> list[dict]:
        since_iso = _iso(_utc_now() - timedelta(days=max(1, int(lookback_days or 7))))
        out: list[dict] = []
        with closing(self._connect_loop()) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, journal_id, created_at, source, signal_symbol, direction,
                       confidence, neural_prob, min_prob, decision, decision_reason, exec_status,
                       force_mode, m1_not_confirmed, canary_applied,
                       outcome_type, outcome, outcome_label, pnl_usd, weight
                FROM neural_gate_decisions
                WHERE created_at >= ?
                  AND UPPER(source)=UPPER(?)
                  AND UPPER(signal_symbol)=UPPER(?)
                  AND neural_prob IS NOT NULL
                ORDER BY id ASC
                """,
                (since_iso, source, symbol),
            ).fetchall()
            for r in rows:
                created = _parse_iso(str(r["created_at"] or ""))
                utc_h = created.astimezone(timezone.utc).strftime("%H:00") if created else "--:--"
                ict_h = (
                    created.astimezone(timezone(timedelta(hours=7))).strftime("%H:00")
                    if created
                    else "--:--"
                )
                out.append(
                    {
                        "id": _safe_int(r["id"], 0),
                        "journal_id": _safe_int(r["journal_id"], 0),
                        "created_at": str(r["created_at"] or ""),
                        "hour_utc": utc_h,
                        "hour_ict": ict_h,
                        "source": str(r["source"] or ""),
                        "signal_symbol": str(r["signal_symbol"] or ""),
                        "direction": str(r["direction"] or ""),
                        "confidence": _safe_float(r["confidence"], 0.0),
                        "neural_prob": _safe_float(r["neural_prob"], 0.0),
                        "prob_band": _prob_band(_safe_float(r["neural_prob"], 0.0)),
                        "min_prob": _safe_float(r["min_prob"], 0.0),
                        "decision": str(r["decision"] or ""),
                        "decision_reason": str(r["decision_reason"] or ""),
                        "exec_status": str(r["exec_status"] or ""),
                        "force_mode": bool(_safe_int(r["force_mode"], 0)),
                        "m1_not_confirmed": bool(_safe_int(r["m1_not_confirmed"], 0)),
                        "canary_applied": bool(_safe_int(r["canary_applied"], 0)),
                        "outcome_type": str(r["outcome_type"] or ""),
                        "outcome": (None if r["outcome"] is None else _safe_int(r["outcome"], -1)),
                        "outcome_label": str(r["outcome_label"] or ""),
                        "pnl_usd": _safe_float(r["pnl_usd"], 0.0),
                        "weight": _safe_float(r["weight"], 1.0),
                    }
                )
        return out

    def _tripwire_snapshot(self, rows: list[dict], source: str, symbol: str, lookback_hours: int) -> dict:
        since = _utc_now() - timedelta(hours=max(1, int(lookback_hours or 24)))
        recent = []
        for r in rows:
            if not bool(r.get("canary_applied")):
                continue
            dt = _parse_iso(str(r.get("created_at", "")))
            if dt is None or dt < since:
                continue
            recent.append(r)
        recent.sort(key=lambda x: str(x.get("created_at", "")))
        attempts = len(recent)
        rejects = sum(
            1
            for r in recent
            if str(r.get("exec_status", "")).lower() in {"rejected", "error", "invalid_stops"}
        )
        rejection_rate = (rejects / attempts) if attempts > 0 else 0.0
        resolved_recent = [r for r in recent if r.get("outcome") in (0, 1)]
        net = round(sum(_safe_float(r.get("pnl_usd"), 0.0) for r in resolved_recent), 2)
        cons_losses = 0
        for r in reversed(resolved_recent):
            if int(r.get("outcome", -1)) == 0:
                cons_losses += 1
            elif int(r.get("outcome", -1)) == 1:
                break
        max_cons = max(1, int(getattr(config, "NEURAL_GATE_CANARY_TRIPWIRE_MAX_CONSEC_LOSSES", 2) or 2))
        max_loss = max(1.0, float(getattr(config, "NEURAL_GATE_CANARY_TRIPWIRE_MAX_NET_LOSS_USD", 20.0) or 20.0))
        max_rej = max(0.05, float(getattr(config, "NEURAL_GATE_CANARY_TRIPWIRE_MAX_REJECTION_RATE", 0.30) or 0.30))
        triggered = (cons_losses >= max_cons) or (net <= -abs(max_loss)) or (rejection_rate > max_rej)
        return {
            "scope": f"{source}|{symbol}",
            "window_hours": max(1, int(lookback_hours or 24)),
            "attempts": attempts,
            "resolved": len(resolved_recent),
            "consecutive_losses": cons_losses,
            "net_pnl_usd": net,
            "rejections": rejects,
            "rejection_rate": round(rejection_rate, 4),
            "triggered": bool(triggered),
            "limits": {
                "max_consecutive_losses": max_cons,
                "max_net_loss_usd": float(max_loss),
                "max_rejection_rate": float(max_rej),
            },
        }

    def _previous_scope_config(self, source: str, symbol: str) -> dict:
        pol = self._load_policy_cached()
        scopes = dict(pol.get("scopes", {}) or {})
        key = f"{str(source or '').strip().lower()}|{_norm_symbol(symbol)}"
        return dict(scopes.get(key, {}) or {})

    def _canary_execution_window_stats(
        self,
        rows: list[dict],
        *,
        base_min: float,
        min_conf: float,
        require_force: bool,
        lookback_hours: int,
        prob_floor: float,
    ) -> dict:
        since = _utc_now() - timedelta(hours=max(1, int(lookback_hours or 6)))
        low = max(0.0, min(float(base_min) - 0.001, float(prob_floor)))
        high = max(low + 0.0001, float(base_min))
        eligible: list[dict] = []
        for r in rows:
            dt = _parse_iso(str(r.get("created_at", "") or ""))
            if dt is None or dt < since:
                continue
            p = _safe_float(r.get("neural_prob"), 0.0)
            if not (low <= p < high):
                continue
            conf = _safe_float(r.get("confidence"), 0.0)
            if conf < float(min_conf):
                continue
            if require_force and (not bool(r.get("force_mode"))):
                continue
            eligible.append(r)

        filled_status = {"filled", "dry_run"}
        attempt_status = filled_status | {"rejected", "error", "invalid_stops", "guard_blocked"}
        eligible_n = len(eligible)
        canary_rows = [r for r in eligible if bool(r.get("canary_applied"))]
        canary_attempts = [
            r
            for r in canary_rows
            if str(r.get("exec_status", "")).strip().lower() in attempt_status
        ]
        filled = [
            r
            for r in canary_rows
            if str(r.get("exec_status", "")).strip().lower() in filled_status
        ]
        neural_blocked = [r for r in eligible if str(r.get("decision", "")).strip().lower() == "neural_block"]
        rejected = [
            r
            for r in canary_rows
            if str(r.get("exec_status", "")).strip().lower() in {"rejected", "error", "invalid_stops"}
        ]

        fill_rate = (len(filled) / eligible_n) if eligible_n > 0 else None
        block_rate = (len(neural_blocked) / eligible_n) if eligible_n > 0 else None
        canary_rate = (len(canary_attempts) / eligible_n) if eligible_n > 0 else None
        return {
            "window_hours": max(1, int(lookback_hours or 6)),
            "prob_floor": round(float(low), 4),
            "prob_ceiling": round(float(high), 4),
            "eligible": int(eligible_n),
            "neural_blocked": int(len(neural_blocked)),
            "canary_attempts": int(len(canary_attempts)),
            "filled": int(len(filled)),
            "rejected_or_error": int(len(rejected)),
            "fill_rate": (round(float(fill_rate), 4) if fill_rate is not None else None),
            "neural_block_rate": (round(float(block_rate), 4) if block_rate is not None else None),
            "canary_attempt_rate": (round(float(canary_rate), 4) if canary_rate is not None else None),
        }

    def _auto_step_canary_scope(
        self,
        *,
        source: str,
        symbol: str,
        rows: list[dict],
        base_min: float,
        allow_low: float,
        low_floor: float,
        min_conf: float,
        require_force: bool,
        volume_cap: float,
        scope_active: bool,
        tripwire_triggered: bool,
    ) -> tuple[float, float, dict]:
        enabled = bool(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_ENABLED", True))
        lookback_hours = max(1, int(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_LOOKBACK_HOURS", 6) or 6))
        min_eligible = max(1, int(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_MIN_ELIGIBLE", 8) or 8))
        target_fill = max(
            0.0,
            min(1.0, float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_TARGET_FILL_RATE", 0.20) or 0.20)),
        )
        min_block_rate = max(
            0.0,
            min(1.0, float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_MIN_BLOCK_RATE", 0.40) or 0.40)),
        )
        down_step = max(0.001, float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_DOWN_STEP", 0.01) or 0.01))
        min_allow_cfg = float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_MIN_ALLOW_LOW", low_floor) or low_floor)
        cap_step = max(
            0.005,
            float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_VOLUME_CAP_STEP", 0.02) or 0.02),
        )
        min_cap = max(
            0.05,
            min(1.0, float(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_MIN_VOLUME_CAP", 0.12) or 0.12)),
        )
        cooldown_min = max(
            0,
            int(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_COOLDOWN_MIN", 30) or 30),
        )
        require_active = bool(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_REQUIRE_ACTIVE_SCOPE", True))
        stop_on_tripwire = bool(getattr(config, "NEURAL_GATE_CANARY_AUTO_STEP_STOP_ON_TRIPWIRE", True))

        high_cap = max(0.001, float(base_min) - 0.001)
        min_allow = max(0.0, min(high_cap, float(min_allow_cfg)))
        metric_floor = max(0.0, min(high_cap, min(float(low_floor), float(min_allow))))

        prev_scope = self._previous_scope_config(source=source, symbol=symbol)
        prev_auto = dict(prev_scope.get("auto_step", {}) or {})
        prev_allow = _safe_float(prev_scope.get("allow_low"), allow_low)
        prev_cap = _safe_float(prev_scope.get("volume_multiplier_cap"), volume_cap)

        allow_before = max(0.0, min(high_cap, float(allow_low)))
        if 0.0 < prev_allow < high_cap:
            allow_before = min(allow_before, float(prev_allow))
        cap_before = max(0.05, min(1.0, float(volume_cap)))
        if 0.05 <= prev_cap <= 1.0:
            cap_before = min(cap_before, float(prev_cap))

        exec_stats = self._canary_execution_window_stats(
            rows=rows,
            base_min=base_min,
            min_conf=min_conf,
            require_force=require_force,
            lookback_hours=lookback_hours,
            prob_floor=metric_floor,
        )
        fill_rate = exec_stats.get("fill_rate")
        block_rate = exec_stats.get("neural_block_rate")
        eligible = int(exec_stats.get("eligible", 0) or 0)

        now = _utc_now()
        last_step_at_raw = str(prev_auto.get("last_step_at", "") or "")
        last_step_at = _parse_iso(last_step_at_raw) if last_step_at_raw else None
        cooldown_ok = True
        if (last_step_at is not None) and (cooldown_min > 0):
            cooldown_ok = (now - last_step_at).total_seconds() >= float(cooldown_min * 60)

        reason = "ok"
        do_step = False
        if not enabled:
            reason = "disabled"
        elif require_active and (not scope_active):
            reason = "scope_inactive"
        elif stop_on_tripwire and bool(tripwire_triggered):
            reason = "tripwire_triggered"
        elif eligible < min_eligible:
            reason = "insufficient_eligible"
        elif fill_rate is None:
            reason = "no_fill_rate"
        elif float(fill_rate) >= target_fill:
            reason = "fill_rate_ok"
        elif block_rate is None:
            reason = "no_block_rate"
        elif float(block_rate) < min_block_rate:
            reason = "block_rate_low"
        elif (allow_before <= (min_allow + 1e-9)) and (cap_before <= (min_cap + 1e-9)):
            reason = "at_min_bounds"
        elif not cooldown_ok:
            reason = "cooldown"
        else:
            do_step = True
            reason = "low_fill_rate"

        allow_after = allow_before
        cap_after = cap_before
        applied = False
        if do_step:
            allow_after = max(float(min_allow), round(float(allow_before) - float(down_step), 4))
            cap_after = max(float(min_cap), round(float(cap_before) - float(cap_step), 4))
            applied = (allow_after < (allow_before - 1e-9)) or (cap_after < (cap_before - 1e-9))
            if not applied:
                reason = "at_min_bounds"

        if not enabled:
            allow_after = allow_before
            cap_after = cap_before

        steps_applied_prev = max(0, _safe_int(prev_auto.get("steps_applied", 0), 0))
        steps_applied_now = steps_applied_prev + (1 if applied else 0)
        info = {
            "enabled": bool(enabled),
            "applied": bool(applied),
            "action": ("step_down" if applied else "hold"),
            "reason": str(reason),
            "source": str(source or "").strip().lower(),
            "symbol": _norm_symbol(symbol),
            "lookback_hours": int(lookback_hours),
            "min_eligible": int(min_eligible),
            "target_fill_rate": round(float(target_fill), 4),
            "min_block_rate": round(float(min_block_rate), 4),
            "down_step": round(float(down_step), 4),
            "min_allow_low": round(float(min_allow), 4),
            "volume_cap_step": round(float(cap_step), 4),
            "min_volume_cap": round(float(min_cap), 4),
            "cooldown_min": int(cooldown_min),
            "cooldown_ok": bool(cooldown_ok),
            "scope_active": bool(scope_active),
            "tripwire_triggered": bool(tripwire_triggered),
            "allow_low_before": round(float(allow_before), 4),
            "allow_low_after": round(float(allow_after), 4),
            "volume_cap_before": round(float(cap_before), 4),
            "volume_cap_after": round(float(cap_after), 4),
            "steps_applied": int(steps_applied_now),
            "execution_window": dict(exec_stats or {}),
        }
        if last_step_at_raw:
            info["last_step_at_prev"] = last_step_at_raw
        if applied:
            info["last_step_at"] = _iso(now)
        elif last_step_at_raw:
            info["last_step_at"] = last_step_at_raw
        return float(allow_after), float(cap_after), info

    def calibrate_and_publish_policy(self) -> dict:
        symbol = _norm_symbol(getattr(config, "NEURAL_GATE_CANARY_SYMBOL", "XAUUSD") or "XAUUSD")
        sources = list(getattr(config, "get_neural_gate_canary_sources", lambda: [])() or [])
        if not sources:
            legacy_source = str(getattr(config, "NEURAL_GATE_CANARY_SOURCE", "scalp_xauusd") or "scalp_xauusd").strip().lower()
            sources = [legacy_source] if legacy_source else ["scalp_xauusd"]
        lookback_days = max(1, int(getattr(config, "NEURAL_GATE_CANARY_LOOKBACK_DAYS", 7) or 7))
        base_min = self._base_min_prob_for_symbol(symbol)
        min_conf = max(0.0, float(getattr(config, "NEURAL_GATE_CANARY_MIN_CONFIDENCE", 80.0) or 80.0))
        require_force = bool(getattr(config, "NEURAL_GATE_CANARY_REQUIRE_FORCE_MODE", True))
        lower_gap = max(0.005, float(getattr(config, "NEURAL_GATE_CANARY_LOWER_GAP", 0.03) or 0.03))
        low_floor = max(0.0, float(getattr(config, "NEURAL_GATE_CANARY_ALLOW_LOW_FLOOR", 0.55) or 0.55))
        lower_bound = max(low_floor, base_min - lower_gap)
        min_shadow = max(4, int(getattr(config, "NEURAL_GATE_CANARY_MIN_SHADOW_SAMPLES", 12) or 12))
        tgt_wr = max(0.0, min(1.0, float(getattr(config, "NEURAL_GATE_CANARY_TARGET_WR", 0.58) or 0.58)))
        min_net = float(getattr(config, "NEURAL_GATE_CANARY_MIN_NET_PNL", 0.0) or 0.0)
        canary_enabled = bool(getattr(config, "NEURAL_GATE_CANARY_ENABLED", True))
        volume_cap = round(max(0.05, min(1.0, float(getattr(config, "NEURAL_GATE_CANARY_VOLUME_CAP", 0.25) or 0.25))), 4)
        max_positions = max(1, int(getattr(config, "NEURAL_GATE_CANARY_MAX_POSITIONS_PER_SYMBOL", 1) or 1))

        scopes: dict[str, dict] = {}
        by_scope: dict[str, dict] = {}
        primary_source = str(sources[0]).strip().lower()
        primary_by_hour: list[dict] = []
        primary_by_band: list[dict] = []

        for source in sources:
            src = str(source or "").strip().lower()
            if not src:
                continue
            rows = self._collect_scope_rows(symbol=symbol, source=src, lookback_days=lookback_days)
            counter = [
                r
                for r in rows
                if r.get("decision") == "neural_block" and r.get("outcome_type") == "shadow_counterfactual"
            ]
            real = [
                r
                for r in rows
                if r.get("decision") == "allow" and str(r.get("outcome_type") or "") == "real_ctrader"
            ]
            candidate = [
                r
                for r in counter
                if lower_bound <= _safe_float(r.get("neural_prob"), 0.0) < base_min
                and _safe_float(r.get("confidence"), 0.0) >= min_conf
                and ((not require_force) or bool(r.get("force_mode")))
            ]
            cand_stats = _agg(candidate)
            cand_wr = (_safe_float(cand_stats.get("win_rate_pct"), 0.0) / 100.0) if cand_stats.get("win_rate_pct") else 0.0
            eligible = (
                cand_stats["n"] >= min_shadow
                and cand_wr >= tgt_wr
                and _safe_float(cand_stats["net_pnl_usd"], 0.0) >= min_net
            )
            prob_vals = [_safe_float(r.get("neural_prob"), 0.0) for r in candidate if r.get("neural_prob") is not None]
            low_q = _quantile(prob_vals, 0.25)
            allow_low = max(lower_bound, _safe_float(low_q, lower_bound))
            allow_low = min(allow_low, max(low_floor, base_min - 0.005))
            # Optional fixed canary floor to run controlled "canary-only" min prob experiments.
            fixed_allow_low = _safe_float(getattr(config, "NEURAL_GATE_CANARY_FIXED_ALLOW_LOW", 0.0), 0.0)
            if fixed_allow_low > 0.0:
                hard_cap = max(low_floor, base_min - 0.001)
                allow_low = max(low_floor, min(hard_cap, float(fixed_allow_low)))
            allow_high = float(base_min)

            tw = self._tripwire_snapshot(
                rows=rows,
                source=src,
                symbol=symbol,
                lookback_hours=max(1, int(getattr(config, "NEURAL_GATE_CANARY_TRIPWIRE_WINDOW_HOURS", 24) or 24)),
            )
            active = bool(canary_enabled and eligible and (not tw.get("triggered", False)))
            reason = "active" if active else "inactive"
            if not canary_enabled:
                reason = "disabled_by_config"
            elif tw.get("triggered", False):
                reason = "tripwire_triggered"
            elif not eligible:
                reason = "insufficient_edge_or_samples"

            scope_volume_cap = float(volume_cap)
            allow_low, scope_volume_cap, auto_step = self._auto_step_canary_scope(
                source=src,
                symbol=symbol,
                rows=rows,
                base_min=base_min,
                allow_low=allow_low,
                low_floor=low_floor,
                min_conf=min_conf,
                require_force=require_force,
                volume_cap=scope_volume_cap,
                scope_active=bool(active),
                tripwire_triggered=bool(tw.get("triggered", False)),
            )

            by_hour: dict[str, dict[str, list[dict]]] = {}
            for r in rows:
                if r.get("outcome") not in (0, 1):
                    continue
                key = f"UTC {r.get('hour_utc', '--:--')} / ICT {r.get('hour_ict', '--:--')}"
                by_hour.setdefault(key, {"counterfactual": [], "real_filled": []})
                if r.get("outcome_type") == "shadow_counterfactual":
                    by_hour[key]["counterfactual"].append(r)
                elif str(r.get("outcome_type") or "") == "real_ctrader":
                    by_hour[key]["real_filled"].append(r)

            by_hour_rows = []
            for key in sorted(by_hour.keys()):
                by_hour_rows.append(
                    {
                        "window": key,
                        "counterfactual": _agg(by_hour[key]["counterfactual"]),
                        "real_filled": _agg(by_hour[key]["real_filled"]),
                    }
                )
            bands = sorted({_prob_band(_safe_float(r.get("neural_prob"), 0.0)) for r in rows if r.get("outcome") in (0, 1)})
            by_band_rows = []
            for b in bands:
                cf = [
                    r
                    for r in counter
                    if r.get("outcome") in (0, 1) and _prob_band(_safe_float(r.get("neural_prob"), 0.0)) == b
                ]
                rf = [
                    r
                    for r in real
                    if r.get("outcome") in (0, 1) and _prob_band(_safe_float(r.get("neural_prob"), 0.0)) == b
                ]
                by_band_rows.append({"prob_band": b, "counterfactual": _agg(cf), "real_filled": _agg(rf)})

            scope_key = f"{src}|{symbol}"
            scopes[scope_key] = {
                "active": active,
                "reason": reason,
                "source": src,
                "symbol": symbol,
                "base_min_prob": round(base_min, 4),
                "allow_low": round(float(allow_low), 4),
                "allow_high": round(float(allow_high), 4),
                "min_confidence": round(float(min_conf), 3),
                "require_force_mode": bool(require_force),
                "volume_multiplier_cap": round(float(scope_volume_cap), 4),
                "max_positions_per_symbol": max_positions,
                "tripwire": tw,
                "candidate_stats": cand_stats,
                "counterfactual_stats": _agg(counter),
                "real_filled_stats": _agg(real),
                "execution_window_stats": dict(auto_step.get("execution_window", {}) or {}),
                "auto_step": auto_step,
                "sample_requirements": {
                    "min_shadow_samples": min_shadow,
                    "target_win_rate": round(tgt_wr, 4),
                    "min_net_pnl_usd": round(min_net, 2),
                },
            }
            by_scope[scope_key] = {"by_hour": by_hour_rows, "by_prob_band": by_band_rows}
            if src == primary_source:
                primary_by_hour = list(by_hour_rows)
                primary_by_band = list(by_band_rows)

        policy = {
            "version": 3,
            "updated_at": _iso(),
            "enabled": canary_enabled,
            "scope": {"source": primary_source, "symbol": symbol},
            "sources": list(sources),
            "scopes": scopes,
            "by_hour": primary_by_hour,
            "by_prob_band": primary_by_band,
            "by_scope": by_scope,
        }

        self.policy_path.parent.mkdir(parents=True, exist_ok=True)
        with self.policy_path.open("w", encoding="utf-8") as f:
            json.dump(policy, f, ensure_ascii=False, indent=2)
        self._policy_cache = dict(policy)
        try:
            self._policy_cache_mtime = self.policy_path.stat().st_mtime
        except Exception:
            self._policy_cache_mtime = 0.0
        return policy

    def _write_cycle_reports(self, payload: dict) -> str:
        self.latest_report_path.parent.mkdir(parents=True, exist_ok=True)
        with self.latest_report_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        out_path = ""
        if bool(getattr(config, "NEURAL_GATE_LEARNING_REPORT_EACH_CYCLE", True)):
            stamp = _utc_now().strftime("%Y%m%d_%H%M%S")
            p = self.mission_dir / f"neural_gate_learning_{stamp}.json"
            with p.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            out_path = str(p)
        return out_path

    def run_cycle(self) -> LoopRun:
        if not self.enabled:
            return LoopRun(False, 0, 0, 0, 0, False, "disabled", policy_path=str(self.policy_path))
        lookback_hours = max(24, int(getattr(config, "NEURAL_GATE_LEARNING_LOOKBACK_HOURS", 168) or 168))
        try:
            sync = self.sync_decisions_from_ctrader_journal(lookback_hours=lookback_hours)
            res = self.resolve_outcomes()
            policy = self.calibrate_and_publish_policy()
            scope = policy.get("scope", {}) if isinstance(policy, dict) else {}
            scope_key = f"{scope.get('source', '')}|{scope.get('symbol', '')}"
            scope_cfg = dict((policy.get("scopes", {}) or {}).get(scope_key, {}) or {})
            payload = {
                "generated_at": _iso(),
                "sync": sync,
                "resolve": res,
                "policy_scope": scope,
                "policy_scope_config": scope_cfg,
                "policy_path": str(self.policy_path),
            }
            report_path = self._write_cycle_reports(payload)
            with closing(self._connect_loop()) as conn:
                conn.execute(
                    "INSERT INTO neural_gate_runs(created_at, summary_json) VALUES (?, ?)",
                    (_iso(), json.dumps(payload, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
            sync_n = 0
            if isinstance(sync, dict) and sync.get("ok"):
                sync_n = int(sync.get("inserted", 0) or 0) + int(sync.get("updated", 0) or 0)
            msg = (
                f"ok sync={sync_n} "
                f"resolved_real={int(res.get('resolved_real', 0))} "
                f"resolved_shadow={int(res.get('resolved_shadow', 0))} "
                f"policy_active={bool(scope_cfg.get('active', False))}"
            )
            return LoopRun(
                True,
                sync_n,
                int(res.get("resolved_real", 0)),
                int(res.get("resolved_shadow", 0)),
                int(res.get("unresolved", 0)),
                bool(scope_cfg.get("active", False)),
                msg,
                report_path=report_path,
                policy_path=str(self.policy_path),
            )
        except Exception as e:
            logger.warning("[NeuralGateLoop] cycle error: %s", e, exc_info=True)
            return LoopRun(False, 0, 0, 0, 0, False, f"error:{e}", policy_path=str(self.policy_path))

    def _load_policy_cached(self) -> dict:
        path = self.policy_path
        if not path.exists():
            return {}
        try:
            mtime = path.stat().st_mtime
        except Exception:
            mtime = 0.0
        if self._policy_cache and mtime > 0 and abs(mtime - self._policy_cache_mtime) < 1e-9:
            return dict(self._policy_cache)
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            self._policy_cache = dict(data or {})
            self._policy_cache_mtime = mtime
            return dict(self._policy_cache)
        except Exception:
            return {}

    def get_scope_policy(self, signal_symbol: str, source: str) -> dict:
        pol = self._load_policy_cached()
        scopes = dict(pol.get("scopes", {}) or {})
        key = f"{str(source or '').strip().lower()}|{_norm_symbol(signal_symbol)}"
        scope = dict(scopes.get(key, {}) or {})
        if not scope:
            return {}
        ttl_sec = max(30, int(getattr(config, "NEURAL_GATE_CANARY_POLICY_TTL_SEC", 900) or 900))
        updated_at = _parse_iso(str(pol.get("updated_at", "") or ""))
        if updated_at is None:
            return {}
        age_sec = max(0.0, (_utc_now() - updated_at).total_seconds())
        if age_sec > float(ttl_sec):
            scope["active"] = False
            scope["reason"] = f"stale_policy:{int(age_sec)}s"
            scope["policy_age_sec"] = int(age_sec)
        else:
            scope["policy_age_sec"] = int(age_sec)
        return scope


neural_gate_learning_loop = NeuralGateLearningLoop()
