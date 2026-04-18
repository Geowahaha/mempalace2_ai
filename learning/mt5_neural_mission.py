"""
learning/mt5_neural_mission.py
Mission-focused neural improvement loop for selected symbols.

This module runs an iterative, bounded optimization cycle:
1) Sync outcomes (MT5 + market feedback)
2) Retrain global + per-symbol models
3) Backtest/evaluate each target symbol
4) Tune neural gating threshold + risk profile per symbol
5) Persist a structured report for audit/review

It does not promise "always win"; it provides a repeatable loop with
clear pass/fail criteria and explainable tuning outputs.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config
from learning.symbol_normalizer import canonical_symbol

logger = logging.getLogger(__name__)


DEFAULT_MISSION_SYMBOLS = ("XAUUSD", "ETHUSD", "BTCUSD", "GBPUSD")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _canonical(sym: str) -> str:
    raw = str(sym or "").strip().upper()
    if not raw:
        return ""
    return canonical_symbol(raw) or raw


def _train_result_to_dict(obj) -> dict:
    if obj is None:
        return {}
    keys = (
        "ok",
        "status",
        "message",
        "samples",
        "train_accuracy",
        "val_accuracy",
        "win_rate",
        "symbol_key",
        "feature_set",
    )
    out = {}
    for k in keys:
        if hasattr(obj, k):
            out[k] = getattr(obj, k)
    return out


class MT5NeuralMission:
    def __init__(
        self,
        *,
        signal_learning_db: Optional[str] = None,
        report_dir: Optional[str] = None,
        env_local_path: Optional[str] = None,
        neural_engine=None,
        symbol_engine=None,
        signal_store_obj=None,
    ):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        self.signal_learning_db = Path(signal_learning_db or (data_dir / "signal_learning.db"))
        self.report_dir = Path(report_dir or (data_dir / "mission_reports"))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.env_local_path = Path(env_local_path or (Path(__file__).resolve().parent.parent / ".env.local"))
        self._lock = threading.Lock()

        if neural_engine is None:
            from learning.neural_brain import neural_brain as _neural

            neural_engine = _neural
        if symbol_engine is None:
            from learning.symbol_neural_brain import symbol_neural_brain as _symbol

            symbol_engine = _symbol
        if signal_store_obj is None:
            from api.signal_store import signal_store as _store

            signal_store_obj = _store

        self.neural_engine = neural_engine
        self.symbol_engine = symbol_engine
        self.signal_store_obj = signal_store_obj

    @staticmethod
    def _notify_telegram(report: dict) -> None:
        if not bool(getattr(config, "NEURAL_MISSION_NOTIFY_TELEGRAM", True)):
            return
        try:
            from notifier.telegram_bot import notifier

            if notifier is not None and hasattr(notifier, "send_neural_mission_report"):
                notifier.send_neural_mission_report(report)
        except Exception as e:
            logger.debug("[NeuralMission] telegram notify skipped: %s", e)

    def _connect_learning_db(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.signal_learning_db), timeout=15)

    def _normalize_symbols(self, symbols) -> list[str]:
        if symbols is None:
            base = list(DEFAULT_MISSION_SYMBOLS)
        elif isinstance(symbols, str):
            base = [s.strip() for s in symbols.split(",") if str(s or "").strip()]
        else:
            base = [str(s or "").strip() for s in list(symbols or [])]
        dedup: list[str] = []
        for s in base:
            c = _canonical(s)
            if c and c not in dedup:
                dedup.append(c)
        return dedup or list(DEFAULT_MISSION_SYMBOLS)

    @staticmethod
    def _is_fx_symbol(sym: str) -> bool:
        s = _canonical(sym)
        majors = {str(x).upper() for x in (config.get_fx_major_symbols() or [])}
        if s in majors:
            return True
        return len(s) in (6, 7) and any(s.endswith(x) for x in ("USD", "JPY", "EUR", "GBP", "CHF", "AUD", "NZD", "CAD"))

    def _base_min_prob_for_symbol(self, sym: str) -> float:
        overrides = dict(config.get_neural_min_prob_symbol_overrides() or {})
        s = _canonical(sym)
        if s in overrides:
            return _safe_float(overrides.get(s), _safe_float(config.NEURAL_BRAIN_MIN_PROB, 0.55))
        if self._is_fx_symbol(s):
            return _safe_float(getattr(config, "NEURAL_BRAIN_MIN_PROB_FX", config.NEURAL_BRAIN_MIN_PROB), 0.55)
        return _safe_float(getattr(config, "NEURAL_BRAIN_MIN_PROB", 0.55), 0.55)

    @staticmethod
    def _extract_neural_prob(extra_json: str) -> Optional[float]:
        try:
            payload = json.loads(str(extra_json or "{}") or "{}")
        except Exception:
            payload = {}
        raw_scores = dict(payload.get("raw_scores", {}) or {})
        for key in ("neural_probability", "mt5_neural_probability", "mt5_neural_prob"):
            if raw_scores.get(key) is not None:
                return _safe_float(raw_scores.get(key), 0.0)
        for key in ("neural_probability", "mt5_neural_probability", "mt5_neural_prob"):
            if payload.get(key) is not None:
                return _safe_float(payload.get(key), 0.0)
        return None

    @staticmethod
    def _is_experimental_lane_source(source: str) -> bool:
        src = str(source or "").strip().lower()
        if not src:
            return False
        if ":bypass" in src or src.endswith("bypass"):
            return True
        lane_tag = str(getattr(config, "MT5_BEST_LANE_TAG", "winner") or "winner").strip().lower()
        if lane_tag and (f":{lane_tag}" in src or src == lane_tag):
            return True
        return False

    def _load_learning_rows(self, symbol: str, days: int) -> list[dict]:
        sym = _canonical(symbol)
        if not sym or not self.signal_learning_db.exists():
            return []
        since = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        query = """
            SELECT signal_symbol, broker_symbol, source, outcome, pnl, extra_json
              FROM signal_events
             WHERE resolved=1
               AND outcome IN (0, 1)
               AND COALESCE(closed_at, created_at) >= ?
               AND (
                    UPPER(COALESCE(signal_symbol,'')) = ?
                    OR UPPER(COALESCE(broker_symbol,'')) = ?
               )
             ORDER BY COALESCE(closed_at, created_at) ASC
        """
        legacy_query = """
            SELECT signal_symbol, broker_symbol, outcome, pnl, extra_json
              FROM signal_events
             WHERE resolved=1
               AND outcome IN (0, 1)
               AND COALESCE(closed_at, created_at) >= ?
               AND (
                    UPPER(COALESCE(signal_symbol,'')) = ?
                    OR UPPER(COALESCE(broker_symbol,'')) = ?
               )
             ORDER BY COALESCE(closed_at, created_at) ASC
        """
        with self._lock:
            with self._connect_learning_db() as conn:
                try:
                    rows = conn.execute(query, (since, sym, sym)).fetchall()
                except sqlite3.OperationalError:
                    rows = [
                        (r[0], r[1], "", r[2], r[3], r[4])
                        for r in conn.execute(legacy_query, (since, sym, sym)).fetchall()
                    ]
        out: list[dict] = []
        include_experimental = bool(getattr(config, "NEURAL_MISSION_INCLUDE_EXPERIMENTAL_LANES", False))
        for row in rows:
            src = str(row[2] or "")
            if (not include_experimental) and self._is_experimental_lane_source(src):
                continue
            prob = self._extract_neural_prob(str(row[5] or ""))
            out.append(
                {
                    "signal_symbol": str(row[0] or ""),
                    "broker_symbol": str(row[1] or ""),
                    "source": src,
                    "outcome": _safe_int(row[3], 0),
                    "pnl": _safe_float(row[4], 0.0),
                    "prob": (None if prob is None else max(0.0, min(1.0, float(prob)))),
                }
            )
        return out

    @staticmethod
    def _threshold_backtest(rows: list[dict], threshold: float, min_eval_trades: int) -> dict:
        thr = max(0.0, min(1.0, float(threshold)))
        kept = [r for r in rows if (r.get("prob") is None or _safe_float(r.get("prob"), 0.0) >= thr)]
        trades = len(kept)
        wins = sum(1 for r in kept if _safe_int(r.get("outcome"), 0) == 1)
        losses = max(0, trades - wins)
        win_rate = (100.0 * wins / trades) if trades > 0 else 0.0
        pnls = [_safe_float(r.get("pnl"), 0.0) for r in kept]
        net = float(sum(pnls))
        sum_pos = float(sum(x for x in pnls if x > 0))
        sum_neg = float(abs(sum(x for x in pnls if x < 0)))
        if sum_neg > 0:
            pf = sum_pos / sum_neg
        elif sum_pos > 0:
            pf = 999.0
        else:
            pf = 0.0
        shortfall = max(0, int(min_eval_trades) - trades)
        score = (
            net
            + (win_rate - 50.0) * 0.10
            + (min(pf, 3.5) - 1.0) * 2.5
            - (shortfall * 0.50)
        )
        return {
            "threshold": round(float(thr), 3),
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 2),
            "profit_factor": round(float(pf), 3),
            "net_pnl": round(net, 4),
            "score": round(float(score), 4),
        }

    def _optimize_threshold(self, rows: list[dict], base_threshold: float, min_trades: int) -> dict:
        bt = max(0.40, min(0.75, float(base_threshold)))
        candidates: set[float] = {round(bt, 2)}
        for i in range(-6, 7):
            candidates.add(round(max(0.40, min(0.75, bt + (i * 0.02))), 2))
        min_eval_trades = max(3, int(min_trades // 2))
        best = None
        for thr in sorted(candidates):
            e = self._threshold_backtest(rows, thr, min_eval_trades=min_eval_trades)
            if best is None:
                best = e
                continue
            if float(e["score"]) > float(best["score"]) + 1e-9:
                best = e
                continue
            if abs(float(e["score"]) - float(best["score"])) <= 1e-9:
                if _safe_int(e["trades"], 0) > _safe_int(best["trades"], 0):
                    best = e
                    continue
                if (
                    _safe_int(e["trades"], 0) == _safe_int(best["trades"], 0)
                    and _safe_float(e["win_rate"], 0.0) > _safe_float(best["win_rate"], 0.0)
                ):
                    best = e
        if best is None:
            best = self._threshold_backtest(rows, bt, min_eval_trades=min_eval_trades)
        best["base_threshold"] = round(bt, 3)
        return best

    def _symbol_stats(self, symbol: str, days: int) -> dict:
        start_ts = time.time() - (max(1, int(days)) * 86400.0)
        stats = dict(self.signal_store_obj.get_performance_stats_filtered(symbol=symbol, start_ts=start_ts, end_ts=None) or {})
        outcome_counts = {}
        try:
            history = list(self.signal_store_obj.get_signal_history(symbol=symbol, limit=5000) or [])
            for row in history:
                ts = _safe_float((row or {}).get("timestamp"), 0.0)
                if ts < float(start_ts):
                    continue
                outcome = str((row or {}).get("outcome", "") or "")
                if not outcome:
                    continue
                outcome_counts[outcome] = int(outcome_counts.get(outcome, 0) or 0) + 1
        except Exception:
            outcome_counts = {}
        wins = max(1, _safe_int(stats.get("wins", 0), 0))
        tp1 = int(outcome_counts.get("tp1_hit", 0))
        tp2 = int(outcome_counts.get("tp2_hit", 0))
        tp3 = int(outcome_counts.get("tp3_hit", 0))
        sl = int(outcome_counts.get("sl_hit", 0))
        stats["outcome_counts"] = outcome_counts
        stats["tp_fast_share"] = round(float(tp1 + tp2) / wins, 4)
        stats["tp3_share"] = round(float(tp3) / wins, 4)
        stats["sl_share"] = round(float(sl) / max(1, _safe_int(stats.get("completed_signals", 0), 0)), 4)
        return stats

    @staticmethod
    def _tp_sl_profile(metrics: dict, target_win_rate: float) -> dict:
        tp_fast = _safe_float(metrics.get("tp_fast_share"), 0.0)
        tp3 = _safe_float(metrics.get("tp3_share"), 0.0)
        sl_share = _safe_float(metrics.get("sl_share"), 0.0)
        wr = _safe_float(metrics.get("win_rate"), 0.0)
        if sl_share >= 0.45:
            return {
                "profile": "defensive",
                "tp_rr_bias": -0.12,
                "stop_scale_bias": 1.05,
                "note": "high SL pressure: reduce RR and widen stop slightly",
            }
        if tp3 >= 0.35 and wr >= (target_win_rate + 3.0):
            return {
                "profile": "runner",
                "tp_rr_bias": 0.10,
                "stop_scale_bias": 0.98,
                "note": "strong runner behavior: allow larger TP extension",
            }
        if tp_fast >= 0.75 and tp3 <= 0.10:
            return {
                "profile": "fast_take_profit",
                "tp_rr_bias": -0.08,
                "stop_scale_bias": 1.00,
                "note": "fast TP dominates: keep taking profits earlier",
            }
        return {
            "profile": "balanced",
            "tp_rr_bias": 0.00,
            "stop_scale_bias": 1.00,
            "note": "no strong TP/SL skew",
        }

    def _recommend_symbol_policy(
        self,
        *,
        symbol: str,
        stats: dict,
        threshold_eval: dict,
        min_trades: int,
        target_win_rate: float,
        target_profit_factor: float,
    ) -> dict:
        completed = _safe_int(stats.get("completed_signals", 0), 0)
        wr = _safe_float(stats.get("win_rate", 0.0), 0.0)
        pf = _safe_float(stats.get("profit_factor", 0.0), 0.0)
        net = _safe_float(stats.get("total_pnl_usd", 0.0), 0.0)
        pass_targets = (
            completed >= int(min_trades)
            and wr >= float(target_win_rate)
            and pf >= float(target_profit_factor)
            and net > 0.0
        )

        tuned_threshold = _safe_float(threshold_eval.get("threshold"), self._base_min_prob_for_symbol(symbol))
        tuned_threshold = max(0.40, min(0.75, tuned_threshold))

        if pass_targets and wr >= (target_win_rate + 8.0) and pf >= (target_profit_factor + 0.5):
            risk_min, risk_max, canary = 0.40, 1.05, "false"
            status = "strong_pass"
        elif pass_targets:
            risk_min, risk_max, canary = 0.30, 0.90, "auto"
            status = "pass"
        elif completed < int(min_trades):
            risk_min, risk_max, canary = 0.20, 0.40, "true"
            status = "insufficient_samples"
        else:
            risk_min, risk_max, canary = 0.20, 0.50, "true"
            status = "under_target"

        tp_sl = self._tp_sl_profile(stats, target_win_rate=float(target_win_rate))
        return {
            "symbol": _canonical(symbol),
            "status": status,
            "target_pass": bool(pass_targets),
            "neural_min_prob": round(tuned_threshold, 3),
            "risk_multiplier_min": round(float(risk_min), 3),
            "risk_multiplier_max": round(float(risk_max), 3),
            "canary_force": str(canary),
            "tp_sl_profile": tp_sl,
            "why": {
                "completed_signals": completed,
                "win_rate": round(wr, 2),
                "profit_factor": round(pf, 3),
                "net_pnl_usd": round(net, 2),
                "threshold_backtest": dict(threshold_eval or {}),
            },
        }

    @staticmethod
    def _map_to_override_line(name: str, values: dict[str, object]) -> str:
        if not values:
            return f"{name}="
        items = []
        for k in sorted(values.keys()):
            v = values.get(k)
            if v is None:
                continue
            if isinstance(v, bool):
                s = "true" if v else "false"
            elif isinstance(v, (int, float)):
                s = f"{float(v):.3f}".rstrip("0").rstrip(".")
            else:
                s = str(v).strip().lower()
            items.append(f"{k}={s}")
        return f"{name}=" + ",".join(items)

    def _build_override_bundle(self, recommendations: dict[str, dict]) -> dict:
        min_prob = {}
        risk_min = {}
        risk_max = {}
        canary = {}
        tp_sl = {}
        for sym, rec in (recommendations or {}).items():
            s = _canonical(sym)
            min_prob[s] = _safe_float(rec.get("neural_min_prob"), self._base_min_prob_for_symbol(s))
            risk_min[s] = _safe_float(rec.get("risk_multiplier_min"), 0.2)
            risk_max[s] = _safe_float(rec.get("risk_multiplier_max"), 0.5)
            canary[s] = str(rec.get("canary_force", "auto")).lower()
            tp_sl[s] = dict(rec.get("tp_sl_profile", {}) or {})
        lines = {
            "NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES": self._map_to_override_line(
                "NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES", min_prob
            ),
            "MT5_RISK_MULTIPLIER_MIN_SYMBOL_OVERRIDES": self._map_to_override_line(
                "MT5_RISK_MULTIPLIER_MIN_SYMBOL_OVERRIDES", risk_min
            ),
            "MT5_RISK_MULTIPLIER_MAX_SYMBOL_OVERRIDES": self._map_to_override_line(
                "MT5_RISK_MULTIPLIER_MAX_SYMBOL_OVERRIDES", risk_max
            ),
            "MT5_CANARY_FORCE_SYMBOL_OVERRIDES": self._map_to_override_line(
                "MT5_CANARY_FORCE_SYMBOL_OVERRIDES", canary
            ),
        }
        return {
            "maps": {
                "neural_min_prob": min_prob,
                "risk_multiplier_min": risk_min,
                "risk_multiplier_max": risk_max,
                "canary_force": canary,
                "tp_sl_profile": tp_sl,
            },
            "env_lines": lines,
        }

    @staticmethod
    def _split_symbol_csv(raw: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for part in str(raw or "").split(","):
            s = str(part or "").strip().upper()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    @staticmethod
    def _fallback_allow_symbol(symbol: str) -> str:
        s = str(symbol or "").strip().upper().replace(" ", "")
        if not s:
            return ""
        if s.endswith("/USDT"):
            return (s[:-5] + "USD").replace("/", "")
        if "/" in s:
            return s.replace("/", "")
        return s

    def _resolve_broker_symbol(self, symbol: str) -> str:
        try:
            from execution.mt5_executor import mt5_executor

            resolved = str(mt5_executor.resolve_symbol(symbol) or "").strip().upper()
            if resolved:
                return resolved
        except Exception:
            pass
        return ""

    def _apply_runtime_allow_symbols(self, symbols: list[str]) -> dict:
        try:
            from execution.mt5_executor import mt5_executor

            if hasattr(mt5_executor, "set_runtime_allow_symbols"):
                return dict(mt5_executor.set_runtime_allow_symbols(symbols) or {})
            mt5_executor._allow_symbols = {str(s or "").strip().upper() for s in symbols if str(s or "").strip()}
            return {"ok": True, "allow_count": len(mt5_executor._allow_symbols)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @staticmethod
    def _upsert_env_key(path: Path, key: str, value: str) -> dict:
        out = {
            "ok": False,
            "path": str(path),
            "key": str(key),
            "updated": False,
            "created": False,
            "line": f"{key}={value}",
            "backup_path": "",
            "error": "",
        }
        try:
            old = ""
            if path.exists():
                old = path.read_text(encoding="utf-8")
            newline = "\r\n" if "\r\n" in old else "\n"
            lines = old.splitlines()
            prefix = f"{key}="
            replaced = False
            for i, line in enumerate(lines):
                s = str(line or "").strip()
                if not s or s.startswith("#"):
                    continue
                if s.upper().startswith(prefix.upper()):
                    lines[i] = f"{key}={value}"
                    replaced = True
                    break
            if not replaced:
                if lines and str(lines[-1]).strip():
                    lines.append("")
                lines.append(f"{key}={value}")
            new_text = newline.join(lines)
            if new_text and not new_text.endswith(newline):
                new_text += newline
            changed = (new_text != old)
            if changed:
                if old:
                    try:
                        backup_dir = path.parent / "data" / "env_backups"
                        backup_dir.mkdir(parents=True, exist_ok=True)
                        ts = _utc_now().strftime("%Y%m%d_%H%M%S")
                        backup_path = backup_dir / f"{path.name}.{ts}.bak"
                        backup_path.write_text(old, encoding="utf-8")
                        out["backup_path"] = str(backup_path)
                        keep = max(5, int(getattr(config, "NEURAL_MISSION_ENV_BACKUP_KEEP", 20) or 20))
                        backups = sorted(backup_dir.glob(f"{path.name}.*.bak"), key=lambda p: p.name)
                        drop = max(0, len(backups) - keep)
                        for b in backups[:drop]:
                            try:
                                b.unlink()
                            except Exception:
                                pass
                    except Exception:
                        out["backup_path"] = ""
                path.write_text(new_text, encoding="utf-8")
            out["ok"] = True
            out["updated"] = bool(changed)
            out["created"] = bool((not replaced) and changed)
            return out
        except Exception as e:
            out["error"] = str(e)
            return out

    def _read_env_allow_symbols(self) -> list[str]:
        path = self.env_local_path
        if not path.exists():
            return sorted(config.get_mt5_allow_symbols())
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            return sorted(config.get_mt5_allow_symbols())
        key = "MT5_ALLOW_SYMBOLS="
        for line in text.splitlines():
            s = str(line or "").strip()
            if not s or s.startswith("#"):
                continue
            if s.upper().startswith(key):
                return self._split_symbol_csv(s.split("=", 1)[1] if "=" in s else "")
        return sorted(config.get_mt5_allow_symbols())

    def _auto_add_allowlist_from_backtest(
        self,
        *,
        symbol_reports: dict[str, dict],
        default_min_trades: int,
    ) -> dict:
        enabled = bool(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_ENABLED", False))
        cfg_min_trades = max(1, int(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_TRADES", default_min_trades) or default_min_trades))
        cfg_min_wr = float(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_WIN_RATE", 58.0) or 58.0)
        cfg_min_pf = float(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_PROFIT_FACTOR", 1.2) or 1.2)
        cfg_min_net = float(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_NET_PNL", 0.0) or 0.0)
        cfg_max_add = max(1, int(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_MAX_ADD_PER_CYCLE", 3) or 3))
        persist_env = bool(getattr(config, "NEURAL_MISSION_AUTO_ALLOWLIST_PERSIST_ENV", True))

        out = {
            "enabled": enabled,
            "status": "disabled",
            "criteria": {
                "min_trades": cfg_min_trades,
                "min_win_rate": round(cfg_min_wr, 3),
                "min_profit_factor": round(cfg_min_pf, 3),
                "min_net_pnl": round(cfg_min_net, 4),
                "max_add_per_cycle": cfg_max_add,
                "persist_env": persist_env,
            },
            "qualified": [],
            "added_symbols": [],
            "skipped_symbols": [],
            "existing_allow_symbols": [],
            "allow_symbols_after": [],
            "env_update": {},
            "runtime_apply": {},
            "env_line": "",
        }
        if not enabled:
            return out

        ranked: list[dict] = []
        for sym, payload in (symbol_reports or {}).items():
            s = _canonical(sym)
            ev = dict((dict(payload or {})).get("threshold_eval", {}) or {})
            trades = _safe_int(ev.get("trades"), 0)
            win_rate = _safe_float(ev.get("win_rate"), 0.0)
            profit_factor = _safe_float(ev.get("profit_factor"), 0.0)
            net_pnl = _safe_float(ev.get("net_pnl"), 0.0)
            passed = (
                trades >= cfg_min_trades
                and win_rate >= cfg_min_wr
                and profit_factor >= cfg_min_pf
                and net_pnl >= cfg_min_net
            )
            row = {
                "symbol": s,
                "trades": trades,
                "win_rate": round(win_rate, 3),
                "profit_factor": round(profit_factor, 3),
                "net_pnl": round(net_pnl, 4),
                "pass": bool(passed),
            }
            if passed:
                ranked.append(row)
            else:
                out["skipped_symbols"].append(dict(row))

        if not ranked:
            out["status"] = "no_qualified_symbols"
            out["existing_allow_symbols"] = self._read_env_allow_symbols()
            out["allow_symbols_after"] = list(out["existing_allow_symbols"])
            return out

        ranked.sort(
            key=lambda r: (
                _safe_float(r.get("net_pnl"), 0.0),
                _safe_float(r.get("profit_factor"), 0.0),
                _safe_float(r.get("win_rate"), 0.0),
                _safe_int(r.get("trades"), 0),
            ),
            reverse=True,
        )

        existing = self._read_env_allow_symbols()
        out["existing_allow_symbols"] = list(existing)
        merged = list(existing)
        merged_set = {str(x).upper() for x in merged}

        for row in ranked:
            sym = str(row.get("symbol") or "").upper()
            broker = self._resolve_broker_symbol(sym)
            target = broker or self._fallback_allow_symbol(sym)
            if not target:
                row["reason"] = "unresolved_symbol"
                out["skipped_symbols"].append(dict(row))
                continue
            row["resolved_symbol"] = target
            out["qualified"].append(dict(row))
            if target in merged_set:
                continue
            if len(out["added_symbols"]) >= cfg_max_add:
                row["reason"] = f"max_add_per_cycle:{cfg_max_add}"
                out["skipped_symbols"].append(dict(row))
                continue
            merged.append(target)
            merged_set.add(target)
            out["added_symbols"].append(target)

        out["allow_symbols_after"] = list(merged)
        if not out["added_symbols"]:
            out["status"] = "already_in_allowlist"
            return out

        allow_csv = ",".join(merged)
        out["env_line"] = f"MT5_ALLOW_SYMBOLS={allow_csv}"
        try:
            setattr(config, "MT5_ALLOW_SYMBOLS", allow_csv)
            os.environ["MT5_ALLOW_SYMBOLS"] = allow_csv
        except Exception:
            pass

        if persist_env:
            out["env_update"] = self._upsert_env_key(self.env_local_path, "MT5_ALLOW_SYMBOLS", allow_csv)
        else:
            out["env_update"] = {"ok": True, "updated": False, "reason": "persist_disabled", "path": str(self.env_local_path)}
        out["runtime_apply"] = self._apply_runtime_allow_symbols(merged)
        out["status"] = "added"
        return out

    def _save_report(self, report: dict) -> str:
        ts = _utc_now().strftime("%Y%m%d_%H%M%S")
        path = self.report_dir / f"mission_{ts}.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

    def run(
        self,
        *,
        symbols=None,
        iterations: int = 3,
        train_days: int = 180,
        backtest_days: int = 90,
        sync_days: int = 180,
        target_win_rate: float = 58.0,
        target_profit_factor: float = 1.2,
        min_trades: int = 12,
        apply_policy_draft: bool = False,
    ) -> dict:
        syms = self._normalize_symbols(symbols)
        max_iter = max(1, int(iterations))
        t_win = max(40.0, min(90.0, float(target_win_rate)))
        t_pf = max(0.8, min(3.0, float(target_profit_factor)))
        min_tr = max(3, int(min_trades))
        report = {
            "ok": True,
            "started_at": _iso(_utc_now()),
            "symbols": list(syms),
            "target": {
                "win_rate_pct": t_win,
                "profit_factor": t_pf,
                "min_trades": min_tr,
            },
            "settings": {
                "iterations": max_iter,
                "train_days": max(1, int(train_days)),
                "backtest_days": max(1, int(backtest_days)),
                "sync_days": max(1, int(sync_days)),
                "apply_policy_draft": bool(apply_policy_draft),
            },
            "iterations": [],
            "goal_met": False,
            "iterations_done": 0,
            "final": {},
            "policy_draft_result": {},
            "report_path": "",
        }

        current_thresholds = {
            s: self._base_min_prob_for_symbol(s)
            for s in syms
        }
        final_recs = {}
        final_symbol_reports = {}

        for idx in range(1, max_iter + 1):
            sync = dict(self.neural_engine.sync_outcomes_from_mt5(days=max(1, int(sync_days))) or {})
            feedback = dict(
                self.neural_engine.sync_signal_outcomes_from_market(
                    days=max(1, int(sync_days)),
                    max_records=max(50, int(getattr(config, "NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS", 400))),
                )
                or {}
            )
            model = dict(self.neural_engine.model_status() or {})
            train_min_samples = int(getattr(config, "NEURAL_BRAIN_MIN_SAMPLES", 30))
            if not bool(model.get("available", False)):
                train_min_samples = min(
                    train_min_samples,
                    max(10, int(getattr(config, "NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES", 10))),
                )
            train_global = self.neural_engine.train_backprop(
                days=max(1, int(train_days)),
                min_samples=train_min_samples,
            )
            train_global_dict = _train_result_to_dict(train_global)

            symbol_train = {}
            symbol_reports = {}
            all_pass = True
            for sym in syms:
                tr = self.symbol_engine.train_symbol(sym, days=max(1, int(train_days)))
                symbol_train[sym] = _train_result_to_dict(tr)

                stats = self._symbol_stats(sym, days=max(1, int(backtest_days)))
                learning_rows = self._load_learning_rows(sym, days=max(1, int(backtest_days)))
                threshold_eval = self._optimize_threshold(
                    learning_rows,
                    base_threshold=current_thresholds.get(sym, self._base_min_prob_for_symbol(sym)),
                    min_trades=min_tr,
                )
                rec = self._recommend_symbol_policy(
                    symbol=sym,
                    stats=stats,
                    threshold_eval=threshold_eval,
                    min_trades=min_tr,
                    target_win_rate=t_win,
                    target_profit_factor=t_pf,
                )
                current_thresholds[sym] = _safe_float(rec.get("neural_min_prob"), current_thresholds.get(sym, 0.55))
                symbol_reports[sym] = {
                    "stats": stats,
                    "learning_rows": len(learning_rows),
                    "threshold_eval": threshold_eval,
                    "recommendation": rec,
                }
                all_pass = all_pass and bool(rec.get("target_pass", False))

            final_recs = {s: dict(symbol_reports[s]["recommendation"]) for s in syms}
            final_symbol_reports = dict(symbol_reports or {})
            override_bundle = self._build_override_bundle(final_recs)
            iter_report = {
                "iteration": idx,
                "sync": sync,
                "feedback": feedback,
                "train_global": train_global_dict,
                "symbol_train": symbol_train,
                "symbols": symbol_reports,
                "override_bundle": override_bundle,
                "all_symbols_passed": bool(all_pass),
            }
            report["iterations"].append(iter_report)
            report["iterations_done"] = idx
            if bool(getattr(config, "NEURAL_MISSION_NOTIFY_EACH_ITERATION", False)):
                self._notify_telegram(
                    {
                        "ok": True,
                        "goal_met": bool(all_pass),
                        "iterations_done": idx,
                        "symbols": list(syms),
                        "target": report.get("target", {}),
                        "final": {
                            "recommendations": final_recs,
                            "override_bundle": override_bundle,
                        },
                    }
                )
            if all_pass:
                report["goal_met"] = True
                break

        final_bundle = self._build_override_bundle(final_recs)
        auto_allowlist = self._auto_add_allowlist_from_backtest(
            symbol_reports=final_symbol_reports,
            default_min_trades=min_tr,
        )
        if str(auto_allowlist.get("env_line", "")).strip():
            final_bundle["env_lines"]["MT5_ALLOW_SYMBOLS"] = str(auto_allowlist.get("env_line")).strip()
        report["final"] = {
            "recommendations": final_recs,
            "override_bundle": final_bundle,
            "auto_allowlist": auto_allowlist,
        }

        if bool(apply_policy_draft) and final_recs:
            try:
                from learning.mt5_orchestrator import mt5_orchestrator

                mins = [_safe_float(v.get("risk_multiplier_min"), 0.2) for v in final_recs.values()]
                maxs = [_safe_float(v.get("risk_multiplier_max"), 0.5) for v in final_recs.values()]
                canary_values = [str(v.get("canary_force", "auto")).lower() for v in final_recs.values()]
                if any(v == "true" for v in canary_values):
                    canary_force = True
                elif all(v == "false" for v in canary_values):
                    canary_force = False
                else:
                    canary_force = None
                draft_payload = {
                    "note": "neural_mission_generated",
                    "symbols": syms,
                    "target": report.get("target", {}),
                    "min_risk_multiplier": round(min(mins), 3) if mins else 0.2,
                    "max_risk_multiplier": round(min(maxs), 3) if maxs else 0.5,
                    "canary_force": canary_force,
                    "symbol_overrides": final_recs,
                    "override_bundle": final_bundle,
                }
                report["policy_draft_result"] = dict(
                    mt5_orchestrator.save_current_account_policy_draft(draft_payload, source="neural_mission") or {}
                )
            except Exception as e:
                report["policy_draft_result"] = {"ok": False, "error": str(e)}

        report["finished_at"] = _iso(_utc_now())
        report["report_path"] = self._save_report(report)
        self._notify_telegram(report)
        return report


mt5_neural_mission = MT5NeuralMission()
