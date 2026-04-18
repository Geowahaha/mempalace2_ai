"""
learning/scalping_forward.py

Forward-test analytics for the dedicated scalping pipeline.
Reads net-after-cost rows from mt5_scalping_net_log.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def _safe_int(v, fallback: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(fallback)


class ScalpingForwardAnalyzer:
    def __init__(self, db_path: Optional[str] = None, scalp_db_path: Optional[str] = None, ctrader_db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg or (data_dir / "mt5_autopilot.db"))
        scalp_cfg = str(getattr(config, "SCALPING_HISTORY_DB_PATH", "") or "").strip()
        self.scalp_db_path = Path(scalp_db_path or scalp_cfg or (data_dir / "scalp_signal_history.db"))
        ctrader_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
        self.ctrader_db_path = Path(ctrader_db_path or ctrader_cfg or (data_dir / "ctrader_openapi.db"))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    @staticmethod
    def _max_drawdown_from_pnl(pnls: list[float]) -> float:
        eq = 0.0
        peak = 0.0
        max_dd = 0.0
        for x in pnls:
            eq += float(x)
            peak = max(peak, eq)
            max_dd = min(max_dd, eq - peak)
        return float(max_dd)

    @staticmethod
    def _is_weekend_ts(ts: float) -> bool:
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except Exception:
            return False
        return bool(dt.weekday() >= 5)

    @staticmethod
    def _confidence_band(conf: float) -> str:
        value = _safe_float(conf, 0.0)
        if value < 70.0:
            return "<70"
        if value < 75.0:
            return "70-74.9"
        if value < 80.0:
            return "75-79.9"
        return "80+"

    @staticmethod
    def _new_bucket() -> dict:
        return {"resolved": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0}

    @classmethod
    def _update_bucket(cls, bucket: dict, pnl: float, is_win: bool, is_loss: bool) -> None:
        if not (is_win or is_loss):
            return
        bucket["resolved"] = int(bucket.get("resolved", 0) or 0) + 1
        if is_win:
            bucket["wins"] = int(bucket.get("wins", 0) or 0) + 1
        if is_loss:
            bucket["losses"] = int(bucket.get("losses", 0) or 0) + 1
        bucket["pnl_usd"] = float(bucket.get("pnl_usd", 0.0) or 0.0) + float(pnl)

    @classmethod
    def _finalize_bucket(cls, bucket: dict) -> dict:
        resolved = int(bucket.get("resolved", 0) or 0)
        wins = int(bucket.get("wins", 0) or 0)
        pnl = float(bucket.get("pnl_usd", 0.0) or 0.0)
        return {
            "resolved": resolved,
            "wins": wins,
            "losses": int(bucket.get("losses", 0) or 0),
            "win_rate": round((wins / resolved), 4) if resolved > 0 else 0.0,
            "pnl_usd": round(pnl, 4),
            "avg_pnl_usd": round((pnl / resolved), 4) if resolved > 0 else 0.0,
        }

    @classmethod
    def _merge_bucket(cls, left: dict, right: dict) -> dict:
        merged = cls._new_bucket()
        for src in (left or {}, right or {}):
            merged["resolved"] += int(src.get("resolved", 0) or 0)
            merged["wins"] += int(src.get("wins", 0) or 0)
            merged["losses"] += int(src.get("losses", 0) or 0)
            merged["pnl_usd"] += float(src.get("pnl_usd", 0.0) or 0.0)
        return merged

    def build_report(self, *, days: int = 7) -> dict:
        lookback_days = max(1, int(days or 7))
        since = _iso(_utc_now() - timedelta(days=lookback_days))
        out = {
            "ok": False,
            "db_path": str(self.db_path),
            "days": lookback_days,
            "since_utc": since,
            "rows": 0,
            "pairs": [],
            "best_pair": None,
            "error": "",
        }
        if not self.db_path.exists():
            out["error"] = "db_not_found"
            return out

        try:
            with closing(self._connect()) as conn:
                tables = {
                    str(r[0]).lower()
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                if "mt5_scalping_net_log" not in tables:
                    out["error"] = "mt5_scalping_net_log_missing"
                    return out
                rows = conn.execute(
                    """
                    SELECT canonical_symbol, source, closed_at, pnl_net_usd, outcome, duration_min, commission, swap
                      FROM mt5_scalping_net_log
                     WHERE closed_at >= ?
                     ORDER BY closed_at ASC
                    """,
                    (since,),
                ).fetchall()
        except Exception as e:
            out["error"] = f"query_error:{e}"
            return out

        if not rows:
            out["ok"] = True
            out["rows"] = 0
            return out

        buckets: dict[str, dict] = {}
        for canonical_symbol, source, closed_at, pnl_net, outcome, duration_min, commission, swap in rows:
            sym = str(canonical_symbol or "").strip().upper() or str(source or "").strip().upper() or "UNKNOWN"
            rec = buckets.setdefault(
                sym,
                {
                    "symbol": sym,
                    "source_set": set(),
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "flat": 0,
                    "pnl_list": [],
                    "duration_list": [],
                    "commission_total": 0.0,
                    "swap_total": 0.0,
                    "last_closed_at": "",
                },
            )
            rec["source_set"].add(str(source or ""))
            rec["trades"] += 1
            pnl = _safe_float(pnl_net, 0.0)
            rec["pnl_list"].append(pnl)
            rec["duration_list"].append(_safe_float(duration_min, 0.0))
            rec["commission_total"] += _safe_float(commission, 0.0)
            rec["swap_total"] += _safe_float(swap, 0.0)
            rec["last_closed_at"] = str(closed_at or rec["last_closed_at"])

            o = None if outcome is None else _safe_int(outcome, 0)
            if o == 1:
                rec["wins"] += 1
            elif o == 0:
                rec["losses"] += 1
            else:
                if pnl > 1e-12:
                    rec["wins"] += 1
                elif pnl < -1e-12:
                    rec["losses"] += 1
                else:
                    rec["flat"] += 1

        pair_rows: list[dict] = []
        for sym, rec in buckets.items():
            trades = int(rec["trades"])
            pnls = list(rec["pnl_list"] or [])
            pnls_sorted = sorted(pnls)
            median = pnls_sorted[len(pnls_sorted) // 2] if pnls_sorted else 0.0
            wins = int(rec["wins"])
            losses = int(rec["losses"])
            win_rate = (wins / trades) if trades > 0 else 0.0
            gross_win = sum(p for p in pnls if p > 0)
            gross_loss = abs(sum(p for p in pnls if p < 0))
            profit_factor = (gross_win / gross_loss) if gross_loss > 1e-12 else None
            row = {
                "symbol": sym,
                "sources": sorted(s for s in rec["source_set"] if s),
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "flat": int(rec["flat"]),
                "win_rate": round(win_rate, 4),
                "pnl_net_usd": round(sum(pnls), 4),
                "avg_net_usd": round((sum(pnls) / trades), 4) if trades > 0 else 0.0,
                "median_net_usd": round(float(median), 4),
                "max_drawdown_usd": round(self._max_drawdown_from_pnl(pnls), 4),
                "profit_factor": (None if profit_factor is None else round(float(profit_factor), 4)),
                "avg_duration_min": round((sum(rec["duration_list"]) / trades), 3) if trades > 0 else 0.0,
                "commission_total": round(float(rec["commission_total"]), 4),
                "swap_total": round(float(rec["swap_total"]), 4),
                "last_closed_at": str(rec["last_closed_at"] or ""),
            }
            pair_rows.append(row)

        pair_rows.sort(key=lambda x: (float(x.get("pnl_net_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("trades", 0))), reverse=True)
        out["ok"] = True
        out["rows"] = len(rows)
        out["pairs"] = pair_rows
        out["best_pair"] = pair_rows[0] if pair_rows else None
        return out

    def build_crypto_weekend_scorecard(self, *, days: int = 14) -> dict:
        lookback_days = max(1, int(days or 14))
        since_dt = _utc_now() - timedelta(days=lookback_days)
        since_ts = since_dt.timestamp()
        out = {
            "ok": False,
            "days": lookback_days,
            "since_utc": _iso(since_dt),
            "generated_at": _iso(_utc_now()),
            "scalp_db_path": str(self.scalp_db_path),
            "mt5_db_path": str(self.db_path),
            "ctrader_db_path": str(self.ctrader_db_path),
            "model_rows": 0,
            "live_rows": 0,
            "ctrader_live_rows": 0,
            "symbols": [],
            "recommendations": [],
            "error": "",
        }
        if not self.scalp_db_path.exists():
            out["error"] = "scalp_history_db_not_found"
            return out

        try:
            with closing(sqlite3.connect(str(self.scalp_db_path), timeout=15)) as conn:
                rows = conn.execute(
                    """
                    SELECT UPPER(symbol), session, confidence, outcome, pnl_usd, timestamp
                      FROM scalp_signals
                     WHERE UPPER(symbol) IN ('ETHUSD', 'BTCUSD')
                       AND timestamp >= ?
                       AND LOWER(outcome) IN ('tp1_hit','tp2_hit','tp3_hit','tp','win','sl_hit','sl','loss')
                     ORDER BY timestamp ASC
                    """,
                    (since_ts,),
                ).fetchall()
        except Exception as e:
            out["error"] = f"scalp_query_error:{e}"
            return out

        symbol_map: dict[str, dict] = {}
        for symbol, session, confidence, outcome, pnl_usd, ts in rows:
            sym = str(symbol or "").strip().upper()
            rec = symbol_map.setdefault(
                sym,
                {
                    "symbol": sym,
                    "model": self._new_bucket(),
                    "weekday": self._new_bucket(),
                    "weekend": self._new_bucket(),
                    "weekday_sessions": {},
                    "weekend_sessions": {},
                    "weekday_conf_bands": {},
                    "weekend_conf_bands": {},
                },
            )
            is_weekend = self._is_weekend_ts(_safe_float(ts, 0.0))
            session_key = str(session or "").strip().lower() or "off_hours"
            band = self._confidence_band(_safe_float(confidence, 0.0))
            label = str(outcome or "").strip().lower()
            is_win = label in {"tp1_hit", "tp2_hit", "tp3_hit", "tp", "win"}
            is_loss = label in {"sl_hit", "sl", "loss"}
            pnl = _safe_float(pnl_usd, 0.0)
            self._update_bucket(rec["model"], pnl, is_win, is_loss)
            segment_key = "weekend" if is_weekend else "weekday"
            self._update_bucket(rec[segment_key], pnl, is_win, is_loss)
            session_bucket = rec[f"{segment_key}_sessions"].setdefault(session_key, self._new_bucket())
            band_bucket = rec[f"{segment_key}_conf_bands"].setdefault(band, self._new_bucket())
            self._update_bucket(session_bucket, pnl, is_win, is_loss)
            self._update_bucket(band_bucket, pnl, is_win, is_loss)

        live_map: dict[str, dict] = {}
        ctrader_live_map: dict[str, dict] = {}
        if self.db_path.exists():
            try:
                with closing(self._connect()) as conn:
                    tables = {
                        str(r[0]).lower()
                        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    }
                    if "mt5_scalping_net_log" in tables:
                        live_rows = conn.execute(
                            """
                            SELECT canonical_symbol, source, closed_at, pnl_net_usd, outcome
                              FROM mt5_scalping_net_log
                             WHERE source IN ('scalp_ethusd', 'scalp_btcusd', 'scalp_ethusd:winner', 'scalp_btcusd:winner')
                               AND closed_at >= ?
                             ORDER BY closed_at ASC
                            """,
                            (_iso(since_dt),),
                        ).fetchall()
                        out["live_rows"] = len(live_rows)
                        for canonical_symbol, _source, _closed_at, pnl_net, outcome in live_rows:
                            sym = str(canonical_symbol or "").strip().upper()
                            if sym in {"ETH/USDT", "ETHUSDT"}:
                                sym = "ETHUSD"
                            elif sym in {"BTC/USDT", "BTCUSDT"}:
                                sym = "BTCUSD"
                            if sym not in {"ETHUSD", "BTCUSD"}:
                                continue
                            bucket = live_map.setdefault(sym, self._new_bucket())
                            pnl = _safe_float(pnl_net, 0.0)
                            o = None if outcome is None else _safe_int(outcome, -1)
                            is_win = o == 1 or (o not in {0, 1} and pnl > 0)
                            is_loss = o == 0 or (o not in {0, 1} and pnl < 0)
                            self._update_bucket(bucket, pnl, is_win, is_loss)
            except Exception:
                pass
        if self.ctrader_db_path.exists():
            try:
                with closing(sqlite3.connect(str(self.ctrader_db_path), timeout=15)) as conn:
                    tables = {
                        str(r[0]).lower()
                        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    }
                    if "ctrader_deals" in tables:
                        ctrader_rows = conn.execute(
                            """
                            SELECT symbol, source, execution_utc, pnl_usd, outcome
                              FROM ctrader_deals
                             WHERE source IN ('scalp_ethusd', 'scalp_btcusd', 'scalp_ethusd:winner', 'scalp_btcusd:winner')
                               AND has_close_detail = 1
                               AND journal_id IS NOT NULL
                               AND execution_utc >= ?
                             ORDER BY execution_utc ASC
                            """,
                            (_iso(since_dt),),
                        ).fetchall()
                        out["ctrader_live_rows"] = len(ctrader_rows)
                        for canonical_symbol, _source, _closed_at, pnl_net, outcome in ctrader_rows:
                            sym = str(canonical_symbol or "").strip().upper()
                            if sym not in {"ETHUSD", "BTCUSD"}:
                                continue
                            bucket = ctrader_live_map.setdefault(sym, self._new_bucket())
                            pnl = _safe_float(pnl_net, 0.0)
                            o = None if outcome is None else _safe_int(outcome, -1)
                            is_win = o == 1 or (o not in {0, 1} and pnl > 0)
                            is_loss = o == 0 or (o not in {0, 1} and pnl < 0)
                            self._update_bucket(bucket, pnl, is_win, is_loss)
            except Exception:
                pass

        symbols: list[dict] = []
        recommendations: list[dict] = []
        for sym in sorted(symbol_map.keys()):
            rec = symbol_map[sym]
            weekend = self._finalize_bucket(rec["weekend"])
            weekday = self._finalize_bucket(rec["weekday"])
            model = self._finalize_bucket(rec["model"])
            live = self._finalize_bucket(live_map.get(sym) or self._new_bucket())
            ctrader_live = self._finalize_bucket(ctrader_live_map.get(sym) or self._new_bucket())
            weekend_sessions = [
                {"session": k, **self._finalize_bucket(v)}
                for k, v in (rec["weekend_sessions"] or {}).items()
                if int((v or {}).get("resolved", 0) or 0) > 0
            ]
            weekday_sessions = [
                {"session": k, **self._finalize_bucket(v)}
                for k, v in (rec["weekday_sessions"] or {}).items()
                if int((v or {}).get("resolved", 0) or 0) > 0
            ]
            weekend_bands = [
                {"band": k, **self._finalize_bucket(v)}
                for k, v in (rec["weekend_conf_bands"] or {}).items()
                if int((v or {}).get("resolved", 0) or 0) > 0
            ]
            weekday_bands = [
                {"band": k, **self._finalize_bucket(v)}
                for k, v in (rec["weekday_conf_bands"] or {}).items()
                if int((v or {}).get("resolved", 0) or 0) > 0
            ]
            weekend_sessions.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            weekday_sessions.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            weekend_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            weekday_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)

            current_sessions = (
                sorted(config.get_scalping_eth_allowed_sessions_weekend()) if sym == "ETHUSD" else sorted(config.get_scalping_btc_allowed_sessions_weekend())
            )
            current_min_conf = float(
                getattr(
                    config,
                    "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND" if sym == "ETHUSD" else "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND",
                    0.0,
                )
                or 0.0
            )
            recommendation_source = "weekend" if int(weekend.get("resolved", 0) or 0) >= 4 else "weekday_proxy"
            session_source = weekend_sessions if recommendation_source == "weekend" else weekday_sessions
            band_source = weekend_bands if recommendation_source == "weekend" else weekday_bands
            positive_sessions = [
                str(x.get("session", "") or "")
                for x in session_source
                if int(x.get("resolved", 0) or 0) >= 2 and float(x.get("pnl_usd", 0.0) or 0.0) > 0
            ]
            positive_band = next(
                (
                    x for x in band_source
                    if int(x.get("resolved", 0) or 0) >= 3 and float(x.get("pnl_usd", 0.0) or 0.0) > 0
                ),
                None,
            )
            recommended_min_conf = current_min_conf
            if positive_band:
                band = str(positive_band.get("band", "") or "")
                if band == "70-74.9":
                    recommended_min_conf = max(current_min_conf, 70.0)
                elif band == "75-79.9":
                    recommended_min_conf = max(current_min_conf, 75.0)
                elif band == "80+":
                    recommended_min_conf = max(current_min_conf, 80.0)
            recommended_sessions = positive_sessions or current_sessions
            if recommendation_source != "weekend" and current_sessions:
                recommended_sessions = current_sessions
            row = {
                "symbol": sym,
                "model": model,
                "weekday": weekday,
                "weekend": weekend,
                "live": live,
                "ctrader_live": ctrader_live,
                "current_weekend_profile": {
                    "min_confidence": round(current_min_conf, 2),
                    "allowed_sessions": current_sessions,
                },
                "recommended_weekend_profile": {
                    "source": recommendation_source,
                    "min_confidence": round(recommended_min_conf, 2),
                    "allowed_sessions": recommended_sessions,
                },
                "top_weekend_sessions": weekend_sessions[:3],
                "top_weekday_sessions": weekday_sessions[:3],
                "top_weekend_conf_bands": weekend_bands[:3],
                "top_weekday_conf_bands": weekday_bands[:3],
                "weekday_proxy_candidate_sessions": positive_sessions,
            }
            symbols.append(row)
            recommendations.append(
                {
                    "symbol": sym,
                    "profile_source": recommendation_source,
                    "weekend_resolved": int(weekend.get("resolved", 0) or 0),
                    "recommended_min_confidence": round(recommended_min_conf, 2),
                    "recommended_sessions": recommended_sessions,
                }
            )

        symbols.sort(
            key=lambda x: (
                float(((x.get("weekend") or {}).get("pnl_usd", 0.0) or 0.0)),
                float(((x.get("weekday") or {}).get("pnl_usd", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        out["ok"] = True
        out["model_rows"] = len(rows)
        out["symbols"] = symbols
        out["recommendations"] = recommendations
        return out

    def build_winner_mission_report(self, *, days: int = 14) -> dict:
        lookback_days = max(1, int(days or 14))
        since_dt = _utc_now() - timedelta(days=lookback_days)
        since_iso = _iso(since_dt)
        out = {
            "ok": False,
            "days": lookback_days,
            "since_utc": since_iso,
            "generated_at": _iso(_utc_now()),
            "scalp_db_path": str(self.scalp_db_path),
            "mt5_db_path": str(self.db_path),
            "ctrader_db_path": str(self.ctrader_db_path),
            "symbols": [],
            "recommendations": [],
            "error": "",
        }
        tracked_symbols = ("XAUUSD", "BTCUSD", "ETHUSD")
        model_map: dict[str, dict] = {
            sym: {
                "model": self._new_bucket(),
                "sessions": {},
                "conf_bands": {},
            }
            for sym in tracked_symbols
        }
        if self.scalp_db_path.exists():
            try:
                with closing(sqlite3.connect(str(self.scalp_db_path), timeout=15)) as conn:
                    rows = conn.execute(
                        """
                        SELECT UPPER(symbol), session, confidence, outcome, pnl_usd
                          FROM scalp_signals
                         WHERE UPPER(symbol) IN ('XAUUSD', 'BTCUSD', 'ETHUSD')
                           AND timestamp >= ?
                           AND LOWER(outcome) IN ('tp1_hit','tp2_hit','tp3_hit','tp','win','sl_hit','sl','loss')
                         ORDER BY timestamp ASC
                        """,
                        (since_dt.timestamp(),),
                    ).fetchall()
                    for symbol, session, confidence, outcome, pnl_usd in rows:
                        sym = str(symbol or "").strip().upper()
                        rec = model_map.setdefault(sym, {"model": self._new_bucket(), "sessions": {}, "conf_bands": {}})
                        label = str(outcome or "").strip().lower()
                        pnl = _safe_float(pnl_usd, 0.0)
                        is_win = label in {"tp1_hit", "tp2_hit", "tp3_hit", "tp", "win"}
                        is_loss = label in {"sl_hit", "sl", "loss"}
                        session_key = str(session or "").strip().lower() or "off_hours"
                        band = self._confidence_band(_safe_float(confidence, 0.0))
                        self._update_bucket(rec["model"], pnl, is_win, is_loss)
                        self._update_bucket(rec["sessions"].setdefault(session_key, self._new_bucket()), pnl, is_win, is_loss)
                        self._update_bucket(rec["conf_bands"].setdefault(band, self._new_bucket()), pnl, is_win, is_loss)
            except Exception as e:
                out["error"] = f"model_query_error:{e}"
                return out

        mt5_lane_map: dict[str, dict] = {sym: {"main": self._new_bucket(), "winner": self._new_bucket()} for sym in tracked_symbols}
        if self.db_path.exists():
            try:
                with closing(self._connect()) as conn:
                    tables = {str(r[0]).lower() for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                    if "mt5_scalping_net_log" in tables:
                        rows = conn.execute(
                            """
                            SELECT canonical_symbol, source, pnl_net_usd, outcome
                              FROM mt5_scalping_net_log
                             WHERE closed_at >= ?
                               AND canonical_symbol IN ('XAUUSD', 'BTCUSD', 'ETHUSD')
                             ORDER BY closed_at ASC
                            """,
                            (since_iso,),
                        ).fetchall()
                        for canonical_symbol, source, pnl_net, outcome in rows:
                            sym = str(canonical_symbol or "").strip().upper()
                            lane = "winner" if ":winner" in str(source or "").lower() else "main"
                            bucket = mt5_lane_map.setdefault(sym, {"main": self._new_bucket(), "winner": self._new_bucket()})[lane]
                            pnl = _safe_float(pnl_net, 0.0)
                            o = None if outcome is None else _safe_int(outcome, -1)
                            is_win = o == 1 or (o not in {0, 1} and pnl > 0)
                            is_loss = o == 0 or (o not in {0, 1} and pnl < 0)
                            self._update_bucket(bucket, pnl, is_win, is_loss)
            except Exception:
                pass

        ctrader_lane_map: dict[str, dict] = {sym: {"main": self._new_bucket(), "winner": self._new_bucket()} for sym in tracked_symbols}
        ctrader_entry_type_map: dict[str, dict] = {sym: {"market": self._new_bucket(), "limit": self._new_bucket()} for sym in tracked_symbols}
        ctrader_live_sessions: dict[str, dict] = {sym: {} for sym in tracked_symbols}
        ctrader_live_conf_bands: dict[str, dict] = {sym: {} for sym in tracked_symbols}
        if self.ctrader_db_path.exists():
            try:
                with closing(sqlite3.connect(str(self.ctrader_db_path), timeout=15)) as conn:
                    conn.row_factory = sqlite3.Row
                    tables = {str(r[0]).lower() for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                    if "ctrader_deals" in tables and "execution_journal" in tables:
                        rows = conn.execute(
                            """
                            SELECT d.symbol,
                                   d.source,
                                   d.pnl_usd,
                                   d.outcome,
                                   j.entry_type,
                                   j.confidence,
                                   j.request_json
                              FROM ctrader_deals d
                              LEFT JOIN execution_journal j ON j.id = d.journal_id
                             WHERE d.has_close_detail = 1
                               AND d.journal_id IS NOT NULL
                               AND d.execution_utc >= ?
                               AND UPPER(COALESCE(d.symbol,'')) IN ('XAUUSD', 'BTCUSD', 'ETHUSD')
                             ORDER BY d.execution_utc ASC
                            """,
                            (since_iso,),
                        ).fetchall()
                        for row in rows:
                            sym = str(row["symbol"] or "").strip().upper()
                            lane = "winner" if ":winner" in str(row["source"] or "").lower() else "main"
                            pnl = _safe_float(row["pnl_usd"], 0.0)
                            o = None if row["outcome"] is None else _safe_int(row["outcome"], -1)
                            is_win = o == 1 or (o not in {0, 1} and pnl > 0)
                            is_loss = o == 0 or (o not in {0, 1} and pnl < 0)
                            self._update_bucket(ctrader_lane_map.setdefault(sym, {"main": self._new_bucket(), "winner": self._new_bucket()})[lane], pnl, is_win, is_loss)
                            entry_type = str(row["entry_type"] or "").strip().lower() or "market"
                            if entry_type not in {"market", "limit"}:
                                entry_type = "market"
                            self._update_bucket(ctrader_entry_type_map.setdefault(sym, {"market": self._new_bucket(), "limit": self._new_bucket()})[entry_type], pnl, is_win, is_loss)
                            req = {}
                            try:
                                raw_req = json.loads(str(row["request_json"] or "{}"))
                                req = raw_req if isinstance(raw_req, dict) else {}
                            except Exception:
                                req = {}
                            session_key = str(req.get("session", "") or "").strip().lower() or "unknown"
                            conf_band = self._confidence_band(_safe_float(row["confidence"], 0.0))
                            self._update_bucket(ctrader_live_sessions.setdefault(sym, {}).setdefault(session_key, self._new_bucket()), pnl, is_win, is_loss)
                            self._update_bucket(ctrader_live_conf_bands.setdefault(sym, {}).setdefault(conf_band, self._new_bucket()), pnl, is_win, is_loss)
            except Exception:
                pass

        recommendations: list[dict] = []
        symbols: list[dict] = []
        for sym in tracked_symbols:
            model_bucket = model_map.get(sym, {}).get("model") or self._new_bucket()
            mt5_main = mt5_lane_map.get(sym, {}).get("main") or self._new_bucket()
            mt5_winner = mt5_lane_map.get(sym, {}).get("winner") or self._new_bucket()
            ctr_main = ctrader_lane_map.get(sym, {}).get("main") or self._new_bucket()
            ctr_winner = ctrader_lane_map.get(sym, {}).get("winner") or self._new_bucket()
            live_main_raw = self._merge_bucket(mt5_main, ctr_main)
            live_winner_raw = self._merge_bucket(mt5_winner, ctr_winner)
            live_total_raw = self._merge_bucket(live_main_raw, live_winner_raw)
            model = self._finalize_bucket(model_bucket)
            mt5_live = {
                "main": self._finalize_bucket(mt5_main),
                "winner": self._finalize_bucket(mt5_winner),
                "total": self._finalize_bucket(self._merge_bucket(mt5_main, mt5_winner)),
            }
            ctrader_live = {
                "main": self._finalize_bucket(ctr_main),
                "winner": self._finalize_bucket(ctr_winner),
                "total": self._finalize_bucket(self._merge_bucket(ctr_main, ctr_winner)),
                "entry_types": {
                    k: self._finalize_bucket(v)
                    for k, v in (ctrader_entry_type_map.get(sym) or {}).items()
                },
            }
            live_total = self._finalize_bucket(live_total_raw)
            live_main = self._finalize_bucket(live_main_raw)
            live_winner = self._finalize_bucket(live_winner_raw)
            model_sessions = [
                {"session": k, **self._finalize_bucket(v)}
                for k, v in (model_map.get(sym, {}).get("sessions") or {}).items()
                if int(v.get("resolved", 0) or 0) > 0
            ]
            model_conf_bands = [
                {"band": k, **self._finalize_bucket(v)}
                for k, v in (model_map.get(sym, {}).get("conf_bands") or {}).items()
                if int(v.get("resolved", 0) or 0) > 0
            ]
            live_sessions = [
                {"session": k, **self._finalize_bucket(v)}
                for k, v in (ctrader_live_sessions.get(sym) or {}).items()
                if int(v.get("resolved", 0) or 0) > 0
            ]
            live_conf_bands = [
                {"band": k, **self._finalize_bucket(v)}
                for k, v in (ctrader_live_conf_bands.get(sym) or {}).items()
                if int(v.get("resolved", 0) or 0) > 0
            ]
            model_sessions.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            model_conf_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            live_sessions.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            live_conf_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)

            limit_stats = dict((ctrader_live.get("entry_types") or {}).get("limit") or {})
            market_stats = dict((ctrader_live.get("entry_types") or {}).get("market") or {})
            entry_bias = "pending_more_sample"
            if int(limit_stats.get("resolved", 0) or 0) >= 2 and float(limit_stats.get("pnl_usd", 0.0) or 0.0) > float(market_stats.get("pnl_usd", 0.0) or 0.0):
                entry_bias = "limit_priority"
            elif int(market_stats.get("resolved", 0) or 0) >= 2 and float(market_stats.get("pnl_usd", 0.0) or 0.0) > float(limit_stats.get("pnl_usd", 0.0) or 0.0):
                entry_bias = "market_priority"

            recommended_live_mode = "collect_sample"
            if sym in {"BTCUSD", "ETHUSD"}:
                recommended_live_mode = "winner_only"
                if int(live_winner.get("resolved", 0) or 0) >= 3 and float(live_winner.get("pnl_usd", 0.0) or 0.0) > 0:
                    recommended_live_mode = "winner_focus"
            else:
                recommended_live_mode = "scheduled_winner_plus_safe_scalp"
                if int(live_total.get("resolved", 0) or 0) >= 4 and float(live_total.get("pnl_usd", 0.0) or 0.0) < 0:
                    recommended_live_mode = "scheduled_winner_only"

            top_model_session = str((model_sessions[0] or {}).get("session", "")) if model_sessions else ""
            top_model_band = str((model_conf_bands[0] or {}).get("band", "")) if model_conf_bands else ""
            action_summary = (
                f"{recommended_live_mode}; bias={entry_bias}; "
                f"top_model_session={top_model_session or '-'}; top_model_band={top_model_band or '-'}"
            )
            if int(live_total.get("resolved", 0) or 0) < max(3, int(model.get("resolved", 0) or 0) // 4):
                action_summary += "; priority=capture_more_real_trades"
            elif float(live_total.get("pnl_usd", 0.0) or 0.0) < 0:
                action_summary += "; priority=improve_entry_quality"
            else:
                action_summary += "; priority=scale_winner_lane"

            row = {
                "symbol": sym,
                "model": model,
                "mt5_live": mt5_live,
                "ctrader_live": ctrader_live,
                "live_total": live_total,
                "live_main": live_main,
                "live_winner": live_winner,
                "entry_bias": entry_bias,
                "recommended_live_mode": recommended_live_mode,
                "action_summary": action_summary,
                "top_model_sessions": model_sessions[:3],
                "top_model_conf_bands": model_conf_bands[:3],
                "top_ctrader_sessions": live_sessions[:3],
                "top_ctrader_conf_bands": live_conf_bands[:3],
            }
            symbols.append(row)
            recommendations.append(
                {
                    "symbol": sym,
                    "recommended_live_mode": recommended_live_mode,
                    "entry_bias": entry_bias,
                    "priority": "capture_more_real_trades" if "capture_more_real_trades" in action_summary else (
                        "improve_entry_quality" if "improve_entry_quality" in action_summary else "scale_winner_lane"
                    ),
                    "top_model_session": top_model_session,
                    "top_model_conf_band": top_model_band,
                }
            )

        symbols.sort(
            key=lambda item: (
                float(((item.get("live_total") or {}).get("pnl_usd", 0.0) or 0.0)),
                float(((item.get("model") or {}).get("pnl_usd", 0.0) or 0.0)),
                str(item.get("symbol", "")),
            ),
            reverse=True,
        )
        out["ok"] = True
        out["symbols"] = symbols
        out["recommendations"] = recommendations
        return out

    def build_crypto_performance_report(self) -> dict:
        """Build dimensional performance report for BTC/ETH — crypto learning brain."""
        enabled = bool(getattr(config, "CRYPTO_PERFORMANCE_TRACKER_ENABLED", True))
        lookback = _safe_int(getattr(config, "CRYPTO_PERFORMANCE_TRACKER_LOOKBACK_DAYS", 21), 21)
        report_path_raw = str(getattr(config, "CRYPTO_PERFORMANCE_TRACKER_REPORT_PATH", "") or "").strip()
        if not report_path_raw:
            report_path_raw = "data/reports/crypto_performance_tracker.json"
        report_path = Path(__file__).resolve().parent.parent / report_path_raw

        out: dict = {
            "ok": False,
            "enabled": enabled,
            "generated_utc": _iso(_utc_now()),
            "lookback_days": lookback,
            "buckets": {},
            "recommendations": {"best_buckets": [], "avoid_buckets": []},
            "error": "",
        }
        if not enabled:
            out["error"] = "disabled"
            return out
        if not self.scalp_db_path.exists():
            out["error"] = "scalp_history_db_not_found"
            return out

        since_ts = (_utc_now() - timedelta(days=lookback)).timestamp()
        try:
            with closing(sqlite3.connect(str(self.scalp_db_path), timeout=15)) as conn:
                rows = conn.execute(
                    """
                    SELECT UPPER(symbol), session, confidence, outcome, pnl_usd, timestamp, direction
                      FROM scalp_signals
                     WHERE UPPER(symbol) IN ('ETHUSD', 'BTCUSD')
                       AND timestamp >= ?
                       AND LOWER(outcome) IN ('tp1_hit','tp2_hit','tp3_hit','tp','win','sl_hit','sl','loss')
                     ORDER BY timestamp ASC
                    """,
                    (since_ts,),
                ).fetchall()
        except Exception as e:
            out["error"] = f"query_error:{e}"
            return out

        buckets: dict[str, dict] = {}
        for row in rows:
            sym = str(row[0] or "").strip().upper()
            session_key = str(row[1] or "").strip().lower() or "off_hours"
            conf = _safe_float(row[2], 0.0)
            label = str(row[3] or "").strip().lower()
            pnl = _safe_float(row[4], 0.0)
            ts = _safe_float(row[5], 0.0)
            direction = str(row[6] or "").strip().lower() or "unknown"
            band = self._confidence_band(conf)
            day_type = "weekend" if self._is_weekend_ts(ts) else "weekday"
            is_win = label in {"tp1_hit", "tp2_hit", "tp3_hit", "tp", "win"}
            is_loss = label in {"sl_hit", "sl", "loss"}
            key = f"{sym}|{session_key}|{direction}|{band}|{day_type}"
            bucket = buckets.setdefault(key, self._new_bucket())
            self._update_bucket(bucket, pnl, is_win, is_loss)

        finalized: dict[str, dict] = {}
        best: list[dict] = []
        avoid: list[dict] = []
        for key, raw_bucket in buckets.items():
            fb = self._finalize_bucket(raw_bucket)
            finalized[key] = fb
            if fb["resolved"] >= 3 and fb["win_rate"] >= 0.55 and fb["avg_pnl_usd"] > 0:
                best.append({"bucket": key, "win_rate": fb["win_rate"], "avg_pnl": fb["avg_pnl_usd"], "samples": fb["resolved"]})
            elif fb["resolved"] >= 3 and (fb["win_rate"] < 0.40 or fb["avg_pnl_usd"] < -0.5):
                avoid.append({"bucket": key, "win_rate": fb["win_rate"], "avg_pnl": fb["avg_pnl_usd"], "samples": fb["resolved"]})

        best.sort(key=lambda x: (x["win_rate"], x["avg_pnl"]), reverse=True)
        avoid.sort(key=lambda x: (x["avg_pnl"], x["win_rate"]))
        out["buckets"] = finalized
        out["recommendations"] = {"best_buckets": best[:10], "avoid_buckets": avoid[:10]}
        out["ok"] = True

        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
        except Exception:
            pass
        return out


scalping_forward_analyzer = ScalpingForwardAnalyzer()
