from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_parse_utc(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    token = raw.replace("Z", "+00:00").replace(" ", "T")
    try:
        dt = datetime.fromisoformat(token)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def week_window_utc(now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime, datetime]:
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_utc_norm = now.astimezone(timezone.utc)
    local_now = now_utc_norm.astimezone()
    week_start_local = (local_now - timedelta(days=local_now.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    week_start_utc = week_start_local.astimezone(timezone.utc)
    return week_start_local, week_start_utc, now_utc_norm


def _new_lane_stats(lane_key: str) -> Dict[str, Any]:
    return {
        "lane_key": lane_key,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "neutrals": 0,
        "pnl_sum": 0.0,
        "win_rate": 0.0,
        "loss_rate": 0.0,
        "shadow_blocked_wins": 0,
        "shadow_blocked_losses": 0,
        "shadow_blocker_buckets": {},
        "blocked_events": 0,
        "missed_opportunities": 0,
        "prevented_bad": 0,
        "classification": "insufficient",
        "recommendation": "hold",
    }


def _finalize_rates(stats: Dict[str, Any]) -> None:
    trades = max(0, _safe_int(stats.get("trades")))
    if trades <= 0:
        stats["win_rate"] = 0.0
        stats["loss_rate"] = 0.0
        return
    stats["win_rate"] = round(_safe_int(stats.get("wins")) / float(trades), 4)
    stats["loss_rate"] = round(_safe_int(stats.get("losses")) / float(trades), 4)
    stats["pnl_sum"] = round(_safe_float(stats.get("pnl_sum")), 6)


def _normalize_lane(source: Any, lane: Any) -> str:
    src = str(source or "").strip() or "unknown_source"
    ln = str(lane or "").strip() or "main"
    return f"{src}::{ln}"


def _load_dexter_deals(
    *,
    db_path: Optional[Path],
    symbol: str,
    week_start_utc: datetime,
    now_utc: datetime,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if db_path is None:
        return [], "db_path_not_set"
    p = Path(db_path)
    if not p.exists():
        return [], f"db_not_found:{p}"

    since_iso = _iso_utc(week_start_utc)
    until_iso = _iso_utc(now_utc)
    rows: List[Dict[str, Any]] = []
    try:
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        cur = con.cursor()
        raw_rows = cur.execute(
            """
            SELECT execution_utc, source, lane, symbol, direction, pnl_usd
            FROM ctrader_deals
            WHERE symbol = ? AND execution_utc >= ? AND execution_utc <= ?
            ORDER BY execution_utc DESC
            """,
            (str(symbol), since_iso, until_iso),
        ).fetchall()
        con.close()
    except Exception as exc:
        return [], f"db_query_failed:{exc}"

    for execution_utc, source, lane, row_symbol, direction, pnl_usd in raw_rows:
        dt = _safe_parse_utc(execution_utc)
        if dt is None or dt < week_start_utc or dt > now_utc:
            continue
        rows.append(
            {
                "execution_utc": _iso_utc(dt),
                "symbol": str(row_symbol or ""),
                "lane_key": _normalize_lane(source, lane),
                "source": str(source or ""),
                "lane": str(lane or ""),
                "direction": str(direction or ""),
                "pnl": _safe_float(pnl_usd),
            }
        )
    return rows, None


def _aggregate_dexter_lanes(
    rows: Iterable[Dict[str, Any]],
    *,
    min_trades: int,
    good_win_rate: float,
    bad_loss_rate: float,
    bad_pnl_threshold: float,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        lane_key = str(row.get("lane_key") or "").strip()
        if not lane_key:
            continue
        lane = out.setdefault(lane_key, _new_lane_stats(lane_key))
        pnl = _safe_float(row.get("pnl"))
        lane["trades"] += 1
        lane["pnl_sum"] = _safe_float(lane.get("pnl_sum")) + pnl
        if pnl > 0.0:
            lane["wins"] += 1
        elif pnl < 0.0:
            lane["losses"] += 1
        else:
            lane["neutrals"] += 1

    for lane in out.values():
        _finalize_rates(lane)
        trades = _safe_int(lane.get("trades"))
        win_rate = _safe_float(lane.get("win_rate"))
        loss_rate = _safe_float(lane.get("loss_rate"))
        pnl_sum = _safe_float(lane.get("pnl_sum"))
        if trades < min_trades:
            lane["classification"] = "insufficient"
            lane["recommendation"] = "hold"
        elif loss_rate >= bad_loss_rate or pnl_sum < bad_pnl_threshold:
            lane["classification"] = "bad"
            lane["recommendation"] = "tighten_or_block"
        elif win_rate >= good_win_rate and pnl_sum > 0.0:
            lane["classification"] = "good"
            lane["recommendation"] = "promote"
        else:
            lane["classification"] = "neutral"
            lane["recommendation"] = "hold"
    return out


def _fallback_strategy_key(meta: Dict[str, Any], body: Dict[str, Any]) -> str:
    key = str(meta.get("strategy_key") or body.get("strategy_key") or "").strip()
    if key:
        return key
    setup = str(body.get("setup_tag") or meta.get("setup_tag") or "").strip() or "unknown_setup"
    feat = dict(body.get("features") or {})
    trend = str(feat.get("trend_direction") or meta.get("trend_direction") or "unknown_trend")
    vol = str(feat.get("volatility") or meta.get("volatility") or "unknown_vol")
    sess = str(feat.get("session") or meta.get("session") or "unknown_session")
    return f"{trend}*{vol}*{sess}_{setup}"


def _load_memory_strategy_lanes(
    *,
    memory: Any,
    symbol: str,
    week_start_utc: datetime,
    now_utc: datetime,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    since_ts = week_start_utc.timestamp()
    until_ts = now_utc.timestamp()
    for item in memory.list_all_structured_experiences():
        meta = dict(item.get("metadata") or {})
        created_ts = _safe_float(meta.get("created_ts"))
        if created_ts <= 0.0 or created_ts < since_ts or created_ts > until_ts:
            continue
        row_symbol = str(meta.get("symbol") or "")
        if row_symbol and str(row_symbol).upper() != str(symbol).upper():
            continue
        try:
            body = json.loads(str(item.get("document") or "{}"))
        except Exception:
            body = {}
        memory_type = str(meta.get("memory_type") or "trade_journal")
        if memory_type not in {"trade_journal", "shadow_probe"}:
            continue
        lane_key = _fallback_strategy_key(meta, body)
        lane = out.setdefault(lane_key, _new_lane_stats(lane_key))

        pnl = _safe_float(meta.get("pnl"), _safe_float((body.get("result") or {}).get("pnl")))
        score = _safe_float(meta.get("outcome_score"), _safe_float(body.get("score")))
        lane["trades"] += 1
        lane["pnl_sum"] = _safe_float(lane.get("pnl_sum")) + pnl
        if score > 0.0:
            lane["wins"] += 1
        elif score < 0.0:
            lane["losses"] += 1
        else:
            lane["neutrals"] += 1

        if memory_type == "shadow_probe":
            bucket = str(meta.get("probe_blocker_bucket") or "unknown")
            buckets = dict(lane.get("shadow_blocker_buckets") or {})
            buckets[bucket] = _safe_int(buckets.get(bucket)) + 1
            lane["shadow_blocker_buckets"] = buckets
            if score > 0.0:
                lane["shadow_blocked_wins"] = _safe_int(lane.get("shadow_blocked_wins")) + 1
            elif score < 0.0:
                lane["shadow_blocked_losses"] = _safe_int(lane.get("shadow_blocked_losses")) + 1
    for lane in out.values():
        _finalize_rates(lane)
    return out


def _load_monitor_rows(
    *,
    monitor_history_path: Path,
    symbol: str,
    week_start_utc: datetime,
    now_utc: datetime,
) -> List[Dict[str, Any]]:
    path = Path(monitor_history_path)
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        log.warning("Weekly lane learning: failed to read monitor history %s: %s", path, exc)
        return []

    for line in lines:
        raw = str(line or "").strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        row_symbol = str(row.get("symbol") or "")
        if row_symbol and row_symbol.upper() != symbol.upper():
            continue
        dt = _safe_parse_utc(row.get("updated_utc"))
        if dt is None or dt < week_start_utc or dt > now_utc:
            continue
        market = dict(row.get("market") or {})
        final_decision = dict(row.get("final_decision") or {})
        out.append(
            {
                "updated_utc": _iso_utc(dt),
                "mid": _safe_float(market.get("mid")),
                "anticipated_action": str(row.get("anticipated_action") or "").upper(),
                "anticipated_strategy_key": str(row.get("anticipated_strategy_key") or ""),
                "final_action": str(final_decision.get("action") or "").upper(),
                "final_reason": str(final_decision.get("reason") or ""),
            }
        )
    out.sort(key=lambda item: item.get("updated_utc") or "")
    return out


def _is_block_reason(reason: str) -> bool:
    text = str(reason or "")
    if not text:
        return False
    if text.startswith("pre_llm_hard_filter:loss_streak_"):
        return True
    markers = (
        "|pattern_block:",
        "|strategy_disabled:",
        "|strategy_soft_gate:",
        "|memory_guard:",
        "low_confidence_floor(",
    )
    return any(marker in text for marker in markers)


def _simulate_blocked_monitor_outcomes(
    rows: List[Dict[str, Any]],
    *,
    lookahead_steps: int,
    move_threshold_pct: float,
) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    if not rows:
        return out
    max_step = max(1, int(lookahead_steps))
    threshold = max(1e-7, float(move_threshold_pct))

    for idx, row in enumerate(rows):
        lane_key = str(row.get("anticipated_strategy_key") or "").strip()
        action = str(row.get("anticipated_action") or "").upper()
        final_action = str(row.get("final_action") or "").upper()
        reason = str(row.get("final_reason") or "")
        entry_mid = _safe_float(row.get("mid"))
        if not lane_key or entry_mid <= 0.0:
            continue
        if final_action != "HOLD" or action not in {"BUY", "SELL"}:
            continue
        if not _is_block_reason(reason):
            continue

        future_prices = [
            _safe_float(item.get("mid"))
            for item in rows[idx + 1 : idx + 1 + max_step]
            if _safe_float(item.get("mid")) > 0.0
        ]
        if not future_prices:
            continue

        if action == "BUY":
            favorable = max((price - entry_mid) for price in future_prices)
            adverse = max((entry_mid - price) for price in future_prices)
        else:
            favorable = max((entry_mid - price) for price in future_prices)
            adverse = max((price - entry_mid) for price in future_prices)

        favorable_pct = favorable / entry_mid
        adverse_pct = adverse / entry_mid
        lane = out.setdefault(
            lane_key,
            {"blocked_events": 0, "missed_opportunities": 0, "prevented_bad": 0},
        )
        lane["blocked_events"] += 1
        if favorable_pct >= threshold and favorable > adverse:
            lane["missed_opportunities"] += 1
        elif adverse_pct >= threshold and adverse > favorable:
            lane["prevented_bad"] += 1
    return out


def build_weekly_lane_profile(
    *,
    memory: Any,
    symbol: str,
    monitor_history_path: Path,
    dexter_db_path: Optional[Path],
    min_trades: int = 3,
    good_win_rate: float = 0.58,
    bad_loss_rate: float = 0.6,
    bad_pnl_threshold: float = 0.0,
    lookahead_steps: int = 8,
    move_threshold_pct: float = 0.0008,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    week_start_local, week_start_utc, now_utc_norm = week_window_utc(now_utc=now_utc)
    mem_lanes = _load_memory_strategy_lanes(
        memory=memory,
        symbol=symbol,
        week_start_utc=week_start_utc,
        now_utc=now_utc_norm,
    )
    monitor_rows = _load_monitor_rows(
        monitor_history_path=monitor_history_path,
        symbol=symbol,
        week_start_utc=week_start_utc,
        now_utc=now_utc_norm,
    )
    monitor_stats = _simulate_blocked_monitor_outcomes(
        monitor_rows,
        lookahead_steps=lookahead_steps,
        move_threshold_pct=move_threshold_pct,
    )
    for lane_key, stats in monitor_stats.items():
        lane = mem_lanes.setdefault(lane_key, _new_lane_stats(lane_key))
        lane["blocked_events"] = _safe_int(stats.get("blocked_events"))
        lane["missed_opportunities"] = _safe_int(stats.get("missed_opportunities"))
        lane["prevented_bad"] = _safe_int(stats.get("prevented_bad"))

    promote_lanes: List[str] = []
    block_lanes: List[str] = []
    probe_lanes: List[str] = []

    for lane in mem_lanes.values():
        _finalize_rates(lane)
        trades = _safe_int(lane.get("trades"))
        win_rate = _safe_float(lane.get("win_rate"))
        loss_rate = _safe_float(lane.get("loss_rate"))
        pnl_sum = _safe_float(lane.get("pnl_sum"))
        missed = _safe_int(lane.get("missed_opportunities"))
        prevented = _safe_int(lane.get("prevented_bad"))
        shadow_wins = _safe_int(lane.get("shadow_blocked_wins"))
        shadow_losses = _safe_int(lane.get("shadow_blocked_losses"))
        support = missed + shadow_wins
        caution = prevented + shadow_losses

        classification = "insufficient"
        recommendation = "hold"
        if trades >= int(min_trades):
            if loss_rate >= float(bad_loss_rate) or pnl_sum < float(bad_pnl_threshold):
                classification = "bad"
                recommendation = "tighten_or_block"
            elif win_rate >= float(good_win_rate) and pnl_sum > 0.0:
                classification = "good"
                recommendation = "promote"
            elif support > caution:
                classification = "opportunity"
                recommendation = "allow_probe_on_block"
            else:
                classification = "neutral"
        else:
            if support >= 2 and support > caution:
                classification = "opportunity"
                recommendation = "allow_probe_on_block"
            elif caution >= 2 and caution > support:
                classification = "caution"
                recommendation = "keep_blockers"

        lane["classification"] = classification
        lane["recommendation"] = recommendation
        if classification == "good":
            promote_lanes.append(str(lane.get("lane_key")))
        elif classification == "bad":
            block_lanes.append(str(lane.get("lane_key")))
        elif classification == "opportunity":
            probe_lanes.append(str(lane.get("lane_key")))

    dexter_rows, dexter_error = _load_dexter_deals(
        db_path=dexter_db_path,
        symbol=symbol,
        week_start_utc=week_start_utc,
        now_utc=now_utc_norm,
    )
    dexter_lanes = _aggregate_dexter_lanes(
        dexter_rows,
        min_trades=min_trades,
        good_win_rate=good_win_rate,
        bad_loss_rate=bad_loss_rate,
        bad_pnl_threshold=bad_pnl_threshold,
    )

    dexter_top = sorted(
        dexter_lanes.values(),
        key=lambda row: (
            -_safe_int(row.get("trades")),
            -_safe_float(row.get("pnl_sum")),
            str(row.get("lane_key") or ""),
        ),
    )[:20]
    strategy_top = sorted(
        mem_lanes.values(),
        key=lambda row: (
            -_safe_int(row.get("trades")),
            -_safe_float(row.get("pnl_sum")),
            str(row.get("lane_key") or ""),
        ),
    )[:20]

    return {
        "generated_utc": _iso_utc(now_utc_norm),
        "week_start_local": week_start_local.isoformat(),
        "week_start_utc": _iso_utc(week_start_utc),
        "symbol": str(symbol),
        "summary": {
            "mempalace_strategy_lanes": len(mem_lanes),
            "mempalace_trade_count": sum(_safe_int(item.get("trades")) for item in mem_lanes.values()),
            "monitor_blocked_events": sum(_safe_int(item.get("blocked_events")) for item in mem_lanes.values()),
            "monitor_missed_opportunities": sum(
                _safe_int(item.get("missed_opportunities")) for item in mem_lanes.values()
            ),
            "monitor_prevented_bad": sum(_safe_int(item.get("prevented_bad")) for item in mem_lanes.values()),
            "dexter_deal_count": len(dexter_rows),
            "dexter_family_lanes": len(dexter_lanes),
            "dexter_source": "ctrader_openapi.db",
            "dexter_source_error": dexter_error or "",
        },
        "promote_lanes": sorted(set(promote_lanes)),
        "block_lanes": sorted(set(block_lanes)),
        "probe_lanes": sorted(set(probe_lanes)),
        "mempalace_strategy_lanes": mem_lanes,
        "dexter_family_lanes": dexter_lanes,
        "top_mempalace_strategy_lanes": strategy_top,
        "top_dexter_family_lanes": dexter_top,
    }


def save_weekly_lane_profile(path: Path, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(target)

