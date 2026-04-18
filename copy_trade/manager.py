"""
copy_trade/manager.py — Core copy trade dispatcher.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import config
from copy_trade.accounts import FollowerAccount, account_registry

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_WORKER_PATH = _ROOT / "ops" / "ctrader_execute_once.py"


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


@dataclass
class CopyTradeResult:
    account_id: str
    label: str
    broker: str
    ok: bool
    status: str
    message: str
    order_id: Optional[int] = None
    position_id: Optional[int] = None
    deal_id: Optional[int] = None
    elapsed_ms: float = 0
    dispatch_delay_ms: float = 0
    total_lag_ms: float = 0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CopyTradeManager:
    """Dispatches executed master signals to all active followers."""

    def __init__(self):
        self.enabled: bool = bool(getattr(config, "COPY_TRADE_ENABLED", False))
        self.close_follow_enabled: bool = bool(getattr(config, "COPY_TRADE_CLOSE_FOLLOW_ENABLED", True))
        self.protection_follow_enabled: bool = bool(getattr(config, "COPY_TRADE_PROTECTION_FOLLOW_ENABLED", True))
        self._latency_warn_ms: int = max(1000, int(getattr(config, "COPY_TRADE_LATENCY_WARN_MS", 5000) or 5000))
        self._close_follow_timeout_sec: int = max(
            5,
            int(getattr(config, "COPY_TRADE_CLOSE_FOLLOW_TIMEOUT_SEC", 18) or 18),
        )
        self._protection_follow_timeout_sec: int = max(
            5,
            int(getattr(config, "COPY_TRADE_PROTECTION_FOLLOW_TIMEOUT_SEC", 18) or 18),
        )
        self._close_event_dedupe_sec: int = max(
            15,
            int(getattr(config, "COPY_TRADE_CLOSE_EVENT_DEDUPE_SEC", 90) or 90),
        )

        self._dispatch_log: list[dict] = []
        self._follow_log: list[dict] = []
        self._lock = threading.Lock()

        self._links_path = _ROOT / "data" / "copy_trade_links.json"
        self._follow_log_path = _ROOT / "data" / "copy_trade_follow_log.jsonl"
        self._position_links: dict[str, list[dict]] = {}
        self._order_links: dict[str, list[dict]] = {}
        self._recent_close_events: dict[str, float] = {}
        self._load_links()

    def dispatch(
        self,
        master_payload: dict,
        master_result: dict,
        *,
        source: str = "",
    ) -> list[CopyTradeResult]:
        """
        Dispatch a successfully executed signal to all active followers.

        Args:
            master_payload: Payload dict sent to cTrader worker by master.
            master_result: Worker result dict from master (must be ok=True).
            source: signal source string for filtering.
        """
        if not self.enabled:
            return []
        if not master_payload or not master_result:
            return []
        if not master_result.get("ok"):
            return []

        symbol = str(master_payload.get("symbol", "")).strip().upper()
        source = source or str(master_payload.get("source", ""))
        followers = account_registry.list_accounts()
        active = [
            f
            for f in followers
            if f.enabled
            and f.is_symbol_allowed(symbol)
            and f.is_source_allowed(source)
        ]
        if not active:
            return []

        master_event_ts = time.time()
        logger.info(
            "[CopyTrade] dispatching %s %s to %d followers",
            symbol,
            master_payload.get("direction", ""),
            len(active),
        )

        results: list[CopyTradeResult] = []
        threads: list[threading.Thread] = []
        for follower in active:
            t = threading.Thread(
                target=self._dispatch_one,
                args=(follower, master_payload, results, master_event_ts),
                daemon=True,
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=30)

        self._register_position_links(
            master_payload=master_payload,
            master_result=master_result,
            source=source,
            results=results,
        )
        self._log_dispatch(master_payload, master_result, results)
        return results

    def dispatch_async(
        self,
        master_payload: dict,
        master_result: dict,
        *,
        source: str = "",
    ) -> None:
        """Fire-and-forget dispatch (runs in background thread)."""
        if not self.enabled:
            return
        t = threading.Thread(
            target=self.dispatch,
            args=(master_payload, master_result),
            kwargs={"source": source},
            daemon=True,
        )
        t.start()

    def enforce_close_follow_async(
        self,
        *,
        master_position_id: int,
        master_order_id: int = 0,
        master_deal_id: int = 0,
        reason: str = "",
        master_close_utc: str = "",
    ) -> None:
        if not self.enabled or not self.close_follow_enabled:
            return
        t = threading.Thread(
            target=self.enforce_close_follow,
            kwargs={
                "master_position_id": master_position_id,
                "master_order_id": master_order_id,
                "master_deal_id": master_deal_id,
                "reason": reason,
                "master_close_utc": master_close_utc,
            },
            daemon=True,
        )
        t.start()

    def sync_protection_follow_async(
        self,
        *,
        master_position_id: int,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        reason: str = "",
    ) -> None:
        if not self.enabled or not self.protection_follow_enabled:
            return
        t = threading.Thread(
            target=self.sync_protection_follow,
            kwargs={
                "master_position_id": master_position_id,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "reason": reason,
            },
            daemon=True,
        )
        t.start()

    def enforce_close_follow(
        self,
        *,
        master_position_id: int,
        master_order_id: int = 0,
        master_deal_id: int = 0,
        reason: str = "",
        master_close_utc: str = "",
    ) -> list[CopyTradeResult]:
        """
        Mirror a master close event to linked copier positions/orders.
        """
        if not self.enabled or not self.close_follow_enabled:
            return []
        m_pos = _safe_int(master_position_id, 0)
        m_ord = _safe_int(master_order_id, 0)
        m_deal = _safe_int(master_deal_id, 0)
        if m_pos <= 0 and m_ord <= 0:
            return []

        with self._lock:
            event_key = self._close_event_key(m_pos, m_ord, m_deal, reason=reason, close_utc=master_close_utc)
            if event_key and not self._register_close_event_locked(event_key):
                return []
            map_name, map_key, links = self._resolve_links_snapshot_locked(m_pos, m_ord)

        if not links:
            return []

        action_rows: list[tuple[dict, CopyTradeResult]] = []
        for link in links:
            account_id = str(link.get("follower_account_id") or "").strip()
            follower = account_registry.get(account_id) if account_id else None
            if follower is None or not follower.enabled:
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=account_id or "unknown",
                            label=str(link.get("follower_label") or account_id or "unknown"),
                            broker=str(link.get("broker") or ""),
                            ok=False,
                            status="follower_unavailable",
                            message="follower account missing or disabled",
                        ),
                    )
                )
                continue
            if follower.broker != "ctrader":
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=follower.account_id,
                            label=follower.label,
                            broker=follower.broker,
                            ok=False,
                            status="follow_not_supported",
                            message="close-follow currently supports only ctrader followers",
                        ),
                    )
                )
                continue
            if not follower.ctrader_account_id:
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=follower.account_id,
                            label=follower.label,
                            broker=follower.broker,
                            ok=False,
                            status="missing_account_id",
                            message="ctrader_account_id not configured",
                        ),
                    )
                )
                continue

            follower_position_id = _safe_int(link.get("follower_position_id"), 0)
            follower_order_id = _safe_int(link.get("follower_order_id"), 0)
            if follower_position_id > 0:
                payload = {
                    "account_id": int(follower.ctrader_account_id),
                    "position_id": int(follower_position_id),
                    "volume": 0,
                }
                result = self._run_ctrader_worker_mode(
                    follower=follower,
                    payload=payload,
                    mode="close",
                    timeout_sec=self._close_follow_timeout_sec,
                )
            elif follower_order_id > 0:
                payload = {
                    "account_id": int(follower.ctrader_account_id),
                    "order_id": int(follower_order_id),
                }
                result = self._run_ctrader_worker_mode(
                    follower=follower,
                    payload=payload,
                    mode="cancel_order",
                    timeout_sec=self._close_follow_timeout_sec,
                )
            else:
                result = CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker=follower.broker,
                    ok=False,
                    status="missing_follower_reference",
                    message="no follower position_id/order_id link",
                )
            action_rows.append((link, result))

        with self._lock:
            self._prune_links_after_actions_locked(
                map_name=map_name,
                map_key=map_key,
                action_rows=action_rows,
                remove_statuses={
                    "closed",
                    "close_submitted",
                    "canceled",
                    "position_missing",
                    "order_missing",
                    "cancel_rejected",
                },
            )
        results = [res for _, res in action_rows]
        self._log_follow(
            action="close_follow",
            master_position_id=m_pos,
            master_order_id=m_ord,
            master_deal_id=m_deal,
            reason=reason,
            results=results,
        )
        return results

    def sync_protection_follow(
        self,
        *,
        master_position_id: int,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        reason: str = "",
    ) -> list[CopyTradeResult]:
        """
        Mirror master SL/TP amendment to linked copier positions.
        """
        if not self.enabled or not self.protection_follow_enabled:
            return []
        m_pos = _safe_int(master_position_id, 0)
        if m_pos <= 0:
            return []
        sl = _safe_float(stop_loss, 0.0)
        tp = _safe_float(take_profit, 0.0)
        if sl <= 0 and tp <= 0:
            return []

        with self._lock:
            map_name, map_key, links = self._resolve_links_snapshot_locked(m_pos, 0)
        if not links:
            return []

        action_rows: list[tuple[dict, CopyTradeResult]] = []
        for link in links:
            account_id = str(link.get("follower_account_id") or "").strip()
            follower = account_registry.get(account_id) if account_id else None
            if follower is None or not follower.enabled:
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=account_id or "unknown",
                            label=str(link.get("follower_label") or account_id or "unknown"),
                            broker=str(link.get("broker") or ""),
                            ok=False,
                            status="follower_unavailable",
                            message="follower account missing or disabled",
                        ),
                    )
                )
                continue
            if follower.broker != "ctrader":
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=follower.account_id,
                            label=follower.label,
                            broker=follower.broker,
                            ok=False,
                            status="follow_not_supported",
                            message="protection-follow currently supports only ctrader followers",
                        ),
                    )
                )
                continue
            if not follower.ctrader_account_id:
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=follower.account_id,
                            label=follower.label,
                            broker=follower.broker,
                            ok=False,
                            status="missing_account_id",
                            message="ctrader_account_id not configured",
                        ),
                    )
                )
                continue

            follower_position_id = _safe_int(link.get("follower_position_id"), 0)
            if follower_position_id <= 0:
                action_rows.append(
                    (
                        link,
                        CopyTradeResult(
                            account_id=follower.account_id,
                            label=follower.label,
                            broker=follower.broker,
                            ok=False,
                            status="missing_follower_position",
                            message="follower position_id unavailable",
                        ),
                    )
                )
                continue

            payload = {
                "account_id": int(follower.ctrader_account_id),
                "position_id": int(follower_position_id),
                "stop_loss": sl,
                "take_profit": tp,
                "trailing_stop_loss": False,
            }
            result = self._run_ctrader_worker_mode(
                follower=follower,
                payload=payload,
                mode="amend_position_sltp",
                timeout_sec=self._protection_follow_timeout_sec,
            )
            action_rows.append((link, result))

        with self._lock:
            self._prune_links_after_actions_locked(
                map_name=map_name,
                map_key=map_key,
                action_rows=action_rows,
                remove_statuses={"position_missing"},
            )
        results = [res for _, res in action_rows]
        self._log_follow(
            action="protection_follow",
            master_position_id=m_pos,
            master_order_id=0,
            master_deal_id=0,
            reason=reason,
            results=results,
            extra={"stop_loss": round(sl, 6), "take_profit": round(tp, 6)},
        )
        return results

    def _load_links(self) -> None:
        if not self._links_path.exists():
            return
        try:
            with open(self._links_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            position_links = dict(data.get("position_links") or {})
            order_links = dict(data.get("order_links") or {})
            recent_close = dict(data.get("recent_close_events") or {})
            self._position_links = {
                str(k): [dict(item or {}) for item in list(v or [])]
                for k, v in position_links.items()
                if str(k).strip()
            }
            self._order_links = {
                str(k): [dict(item or {}) for item in list(v or [])]
                for k, v in order_links.items()
                if str(k).strip()
            }
            self._recent_close_events = {
                str(k): float(v)
                for k, v in recent_close.items()
                if str(k).strip()
            }
        except Exception as e:
            logger.warning("[CopyTrade] failed to load links: %s", e)
            self._position_links = {}
            self._order_links = {}
            self._recent_close_events = {}

    def _save_links_locked(self) -> None:
        try:
            self._cleanup_close_events_locked()
            self._links_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "version": 1,
                "updated_at": _utc_now_iso(),
                "position_links": self._position_links,
                "order_links": self._order_links,
                "recent_close_events": self._recent_close_events,
            }
            tmp = self._links_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            tmp.replace(self._links_path)
        except Exception as e:
            logger.debug("[CopyTrade] failed to persist links: %s", e)

    def _close_event_key(
        self,
        master_position_id: int,
        master_order_id: int,
        master_deal_id: int,
        *,
        reason: str = "",
        close_utc: str = "",
    ) -> str:
        if int(master_deal_id or 0) > 0:
            return f"deal:{int(master_deal_id)}"
        pos = int(master_position_id or 0)
        ord_id = int(master_order_id or 0)
        if pos <= 0 and ord_id <= 0:
            return ""
        bucket = int(time.time() // max(15, self._close_event_dedupe_sec))
        return f"close:{pos}:{ord_id}:{str(reason or '').strip().lower()}:{str(close_utc or '')[:19]}:{bucket}"

    def _cleanup_close_events_locked(self) -> None:
        if not self._recent_close_events:
            return
        cutoff = time.time() - float(self._close_event_dedupe_sec * 3)
        for key in list(self._recent_close_events.keys()):
            if float(self._recent_close_events.get(key, 0.0) or 0.0) < cutoff:
                self._recent_close_events.pop(key, None)

    def _register_close_event_locked(self, event_key: str) -> bool:
        if not event_key:
            return True
        self._cleanup_close_events_locked()
        now = time.time()
        ts = float(self._recent_close_events.get(event_key, 0.0) or 0.0)
        if ts > 0 and (now - ts) < float(self._close_event_dedupe_sec):
            return False
        self._recent_close_events[event_key] = now
        self._save_links_locked()
        return True

    def _resolve_links_snapshot_locked(
        self,
        master_position_id: int,
        master_order_id: int,
    ) -> tuple[str, str, list[dict]]:
        pos_key = str(int(master_position_id or 0))
        ord_key = str(int(master_order_id or 0))
        if int(master_position_id or 0) > 0 and pos_key in self._position_links:
            return "position", pos_key, copy.deepcopy(list(self._position_links.get(pos_key) or []))
        if int(master_order_id or 0) > 0 and ord_key in self._order_links:
            return "order", ord_key, copy.deepcopy(list(self._order_links.get(ord_key) or []))
        if int(master_position_id or 0) > 0 and pos_key in self._order_links:
            return "order", pos_key, copy.deepcopy(list(self._order_links.get(pos_key) or []))
        return "", "", []

    def _prune_links_after_actions_locked(
        self,
        *,
        map_name: str,
        map_key: str,
        action_rows: list[tuple[dict, CopyTradeResult]],
        remove_statuses: set[str],
    ) -> None:
        if not map_name or not map_key:
            return
        mapping = self._position_links if map_name == "position" else self._order_links
        current = list(mapping.get(map_key) or [])
        if not current:
            return
        status_by_ref: dict[tuple[str, int, int], CopyTradeResult] = {}
        for link, result in action_rows:
            ref = (
                str(link.get("follower_account_id") or ""),
                _safe_int(link.get("follower_position_id"), 0),
                _safe_int(link.get("follower_order_id"), 0),
            )
            status_by_ref[ref] = result

        remaining: list[dict] = []
        for link in current:
            ref = (
                str(link.get("follower_account_id") or ""),
                _safe_int(link.get("follower_position_id"), 0),
                _safe_int(link.get("follower_order_id"), 0),
            )
            result = status_by_ref.get(ref)
            if result is None:
                remaining.append(link)
                continue
            status = str(result.status or "").strip().lower()
            if bool(result.ok) or status in remove_statuses:
                continue
            remaining.append(link)

        if remaining:
            mapping[map_key] = remaining
        else:
            mapping.pop(map_key, None)
        self._save_links_locked()

    def _register_position_links(
        self,
        *,
        master_payload: dict,
        master_result: dict,
        source: str,
        results: list[CopyTradeResult],
    ) -> None:
        master_position_id = _safe_int(master_result.get("position_id"), 0)
        master_order_id = _safe_int(master_result.get("order_id"), 0)
        if master_position_id <= 0 and master_order_id <= 0:
            return
        symbol = str(master_payload.get("symbol", "") or "").strip().upper()
        direction = str(master_payload.get("direction", "") or "").strip().lower()
        src = str(source or master_payload.get("source", "") or "").strip().lower()

        with self._lock:
            key = str(master_position_id if master_position_id > 0 else master_order_id)
            mapping = self._position_links if master_position_id > 0 else self._order_links
            existing = list(mapping.get(key) or [])
            for row in results:
                if not row.ok or row.broker != "ctrader":
                    continue
                follower_position_id = _safe_int(row.position_id, 0)
                follower_order_id = _safe_int(row.order_id, 0)
                if follower_position_id <= 0 and follower_order_id <= 0:
                    continue
                follower = account_registry.get(row.account_id)
                follower_ctrader_id = _safe_int(
                    (follower.ctrader_account_id if follower is not None else 0),
                    0,
                )
                link = {
                    "master_position_id": int(master_position_id),
                    "master_order_id": int(master_order_id),
                    "symbol": symbol,
                    "direction": direction,
                    "source": src,
                    "follower_account_id": str(row.account_id),
                    "follower_label": str(row.label),
                    "broker": str(row.broker),
                    "follower_ctrader_account_id": int(follower_ctrader_id),
                    "follower_position_id": int(follower_position_id),
                    "follower_order_id": int(follower_order_id),
                    "linked_utc": _utc_now_iso(),
                }
                existing = [
                    item
                    for item in existing
                    if str(item.get("follower_account_id") or "") != str(row.account_id)
                ]
                existing.append(link)
            if existing:
                mapping[key] = existing
                self._save_links_locked()

    def _dispatch_one(
        self,
        follower: FollowerAccount,
        master_payload: dict,
        results: list[CopyTradeResult],
        master_event_ts: float,
    ) -> None:
        t0 = time.time()
        dispatch_delay_ms = max(0.0, (t0 - float(master_event_ts)) * 1000.0)
        try:
            if follower.broker == "ctrader":
                result = self._execute_ctrader(follower, master_payload)
            elif follower.broker == "mt5":
                result = self._execute_mt5(follower, master_payload)
            else:
                result = CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker=follower.broker,
                    ok=False,
                    status="unsupported_broker",
                    message=f"broker '{follower.broker}' not supported",
                )
        except Exception as e:
            result = CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker=follower.broker,
                ok=False,
                status="exception",
                message=str(e),
            )

        result.dispatch_delay_ms = dispatch_delay_ms
        result.elapsed_ms = (time.time() - t0) * 1000.0
        result.total_lag_ms = max(0.0, result.dispatch_delay_ms + result.elapsed_ms)
        with self._lock:
            results.append(result)

        if result.ok:
            account_registry.update_trade_stats(follower.account_id)
            logger.info(
                "[CopyTrade] %s (%s) -> %s pos=%s order=%s lag=%.0fms worker=%.0fms",
                follower.label,
                follower.broker,
                result.status,
                result.position_id,
                result.order_id,
                result.total_lag_ms,
                result.elapsed_ms,
            )
        else:
            logger.warning(
                "[CopyTrade] %s (%s) FAILED: %s — %s lag=%.0fms worker=%.0fms",
                follower.label,
                follower.broker,
                result.status,
                result.message[:200],
                result.total_lag_ms,
                result.elapsed_ms,
            )
        if result.total_lag_ms >= float(self._latency_warn_ms):
            logger.warning(
                "[CopyTrade] latency warning %s (%s): %.0fms >= %dms",
                follower.label,
                follower.broker,
                result.total_lag_ms,
                self._latency_warn_ms,
            )

    def _execute_ctrader(
        self,
        follower: FollowerAccount,
        master_payload: dict,
    ) -> CopyTradeResult:
        """Execute on a cTrader follower account via the existing worker."""
        if not follower.ctrader_account_id:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="ctrader",
                ok=False,
                status="missing_account_id",
                message="ctrader_account_id not configured",
            )

        payload = copy.deepcopy(master_payload)
        payload["account_id"] = int(follower.ctrader_account_id)
        payload["account_reason"] = f"copy_trade:{follower.label}"

        master_risk = float(payload.get("risk_usd", 0) or 0)
        payload["risk_usd"] = round(follower.scale_risk(master_risk), 4)

        payload["label"] = (
            f"dxcp:{payload.get('symbol', '')}:"
            f"{str(payload.get('source', ''))[:12]}:"
            f"{follower.label[:8]}"
        )[:64]
        payload["comment"] = (
            f"dexter_copy|{follower.label}|{payload.get('symbol', '')}"
        )[:128]

        if follower.ctrader_access_token:
            payload["access_token_override"] = follower.ctrader_access_token

        payload.pop("raw_scores", None)
        return self._run_ctrader_worker_mode(
            follower=follower,
            payload=payload,
            mode="execute",
            timeout_sec=max(5, int(getattr(config, "COPY_TRADE_WORKER_TIMEOUT_SEC", 25) or 25)),
        )

    def _run_ctrader_worker_mode(
        self,
        *,
        follower: FollowerAccount,
        payload: dict,
        mode: str,
        timeout_sec: int,
    ) -> CopyTradeResult:
        """Run ops/ctrader_execute_once.py in a requested mode for follower account."""
        if not _WORKER_PATH.exists():
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="ctrader",
                ok=False,
                status="worker_missing",
                message=f"worker not found: {_WORKER_PATH}",
            )

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                suffix=".json",
                delete=False,
                encoding="utf-8",
            ) as fh:
                json.dump(payload, fh, ensure_ascii=True, separators=(",", ":"))
                tmp_path = fh.name

            cmd = [
                sys.executable,
                str(_WORKER_PATH),
                "--mode",
                str(mode),
                "--payload-file",
                tmp_path,
            ]
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            if follower.ctrader_access_token:
                env["CTRADER_OPENAPI_ACCESS_TOKEN"] = follower.ctrader_access_token

            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(5, int(timeout_sec)),
                cwd=str(_ROOT),
                env=env,
            )

            parsed = self._extract_json(proc.stdout)
            if parsed and parsed.get("ok"):
                return CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker="ctrader",
                    ok=True,
                    status=str(parsed.get("status", "ok")),
                    message=str(parsed.get("message", "")),
                    order_id=_safe_int(parsed.get("order_id"), 0) or None,
                    position_id=_safe_int(parsed.get("position_id"), 0) or None,
                    deal_id=_safe_int(parsed.get("deal_id"), 0) or None,
                )

            err_msg = ""
            if parsed:
                err_msg = str(parsed.get("message", parsed.get("status", "")))
            if not err_msg:
                err_msg = (proc.stderr or proc.stdout or "").strip()[-500:]
            status = str(parsed.get("status", "worker_error")) if parsed else "worker_error"
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="ctrader",
                ok=False,
                status=status,
                message=err_msg or f"exit code {proc.returncode}",
                order_id=_safe_int((parsed or {}).get("order_id"), 0) or None,
                position_id=_safe_int((parsed or {}).get("position_id"), 0) or None,
                deal_id=_safe_int((parsed or {}).get("deal_id"), 0) or None,
            )
        except subprocess.TimeoutExpired:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="ctrader",
                ok=False,
                status="timeout",
                message="worker timeout",
            )
        except Exception as e:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="ctrader",
                ok=False,
                status="exception",
                message=str(e),
            )
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    def _execute_mt5(
        self,
        follower: FollowerAccount,
        master_payload: dict,
    ) -> CopyTradeResult:
        """Execute on an MT5 follower account via RPyC bridge."""
        if not follower.mt5_login:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="mt5",
                ok=False,
                status="missing_login",
                message="mt5_login not configured",
            )

        try:
            import rpyc
        except ImportError:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="mt5",
                ok=False,
                status="rpyc_missing",
                message="rpyc not installed",
            )

        host = follower.mt5_host or str(os.getenv("MT5_HOST", "localhost"))
        port = follower.mt5_port or int(os.getenv("MT5_PORT", "18812"))
        magic = follower.mt5_magic or int(os.getenv("MT5_MAGIC", "123456"))

        symbol = str(master_payload.get("symbol", ""))
        direction = str(master_payload.get("direction", ""))
        entry = float(master_payload.get("entry", 0))
        stop_loss = float(master_payload.get("stop_loss", 0))
        take_profit = float(master_payload.get("take_profit", 0))
        entry_type = str(master_payload.get("entry_type", "market"))

        master_risk = float(master_payload.get("risk_usd", 0) or 0)
        risk_usd = follower.scale_risk(master_risk)

        try:
            conn = rpyc.connect(host, port, config={"allow_pickle": True})
            mt5 = conn.root.get_mt5()

            if not mt5.initialize():
                return CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker="mt5",
                    ok=False,
                    status="mt5_init_failed",
                    message="MT5 initialize() failed",
                )

            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                return CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker="mt5",
                    ok=False,
                    status="symbol_not_found",
                    message=f"symbol {symbol} not found on MT5",
                )

            point = symbol_info.point
            if point <= 0:
                point = 0.01

            risk_points = abs(entry - stop_loss) / point
            if risk_points <= 0:
                risk_points = 100

            tick_value = getattr(symbol_info, "trade_tick_value", 1.0) or 1.0
            volume_step = getattr(symbol_info, "volume_step", 0.01) or 0.01
            volume_min = getattr(symbol_info, "volume_min", 0.01) or 0.01
            volume_max = getattr(symbol_info, "volume_max", 100.0) or 100.0

            raw_volume = risk_usd / (risk_points * tick_value) if (risk_points * tick_value) > 0 else volume_min
            volume = max(volume_min, min(volume_max, round(raw_volume / volume_step) * volume_step))

            if direction == "long":
                order_type = mt5.ORDER_TYPE_BUY if entry_type == "market" else mt5.ORDER_TYPE_BUY_LIMIT
            else:
                order_type = mt5.ORDER_TYPE_SELL if entry_type == "market" else mt5.ORDER_TYPE_SELL_LIMIT

            price = entry if entry_type != "market" else (
                symbol_info.ask if direction == "long" else symbol_info.bid
            )

            request = {
                "action": mt5.TRADE_ACTION_DEAL if entry_type == "market" else mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "sl": stop_loss,
                "tp": take_profit,
                "magic": magic,
                "comment": f"dxcp|{follower.label}|{symbol}"[:31],
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            result = mt5.order_send(request)
            conn.close()

            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                return CopyTradeResult(
                    account_id=follower.account_id,
                    label=follower.label,
                    broker="mt5",
                    ok=True,
                    status="filled",
                    message=f"order={result.order} volume={volume}",
                    order_id=int(result.order),
                )
            retcode = result.retcode if result else -1
            comment = result.comment if result else "no result"
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="mt5",
                ok=False,
                status="mt5_rejected",
                message=f"retcode={retcode} {comment}",
            )

        except Exception as e:
            return CopyTradeResult(
                account_id=follower.account_id,
                label=follower.label,
                broker="mt5",
                ok=False,
                status="exception",
                message=str(e),
            )

    @staticmethod
    def _extract_json(text: str) -> Optional[dict]:
        for line in reversed((text or "").strip().splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    return json.loads(line)
                except Exception:
                    continue
        return None

    def _log_dispatch(
        self,
        master_payload: dict,
        master_result: dict,
        results: list[CopyTradeResult],
    ) -> None:
        latencies = [float(r.total_lag_ms) for r in results if float(r.total_lag_ms) > 0]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        entry = {
            "ts": _utc_now_iso(),
            "symbol": master_payload.get("symbol", ""),
            "direction": master_payload.get("direction", ""),
            "source": master_payload.get("source", ""),
            "master_order_id": _safe_int(master_result.get("order_id"), 0),
            "master_position_id": _safe_int(master_result.get("position_id"), 0),
            "master_risk_usd": master_payload.get("risk_usd", 0),
            "followers": len(results),
            "success": sum(1 for r in results if r.ok),
            "failed": sum(1 for r in results if not r.ok),
            "latency_ms_avg": round(avg_latency, 2) if latencies else 0.0,
            "latency_ms_max": round(max(latencies), 2) if latencies else 0.0,
            "details": [
                {
                    "account": r.account_id,
                    "label": r.label,
                    "broker": r.broker,
                    "ok": r.ok,
                    "status": r.status,
                    "order_id": r.order_id,
                    "position_id": r.position_id,
                    "deal_id": r.deal_id,
                    "dispatch_delay_ms": round(r.dispatch_delay_ms),
                    "elapsed_ms": round(r.elapsed_ms),
                    "total_lag_ms": round(r.total_lag_ms),
                }
                for r in results
            ],
        }
        with self._lock:
            self._dispatch_log.append(entry)
            if len(self._dispatch_log) > 500:
                self._dispatch_log = self._dispatch_log[-200:]

        try:
            log_path = _ROOT / "data" / "copy_trade_log.jsonl"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")
        except Exception:
            pass

    def _log_follow(
        self,
        *,
        action: str,
        master_position_id: int,
        master_order_id: int,
        master_deal_id: int,
        reason: str,
        results: list[CopyTradeResult],
        extra: Optional[dict] = None,
    ) -> None:
        entry = {
            "ts": _utc_now_iso(),
            "action": str(action or ""),
            "reason": str(reason or ""),
            "master_position_id": int(master_position_id or 0),
            "master_order_id": int(master_order_id or 0),
            "master_deal_id": int(master_deal_id or 0),
            "followers": len(results),
            "success": sum(1 for r in results if r.ok),
            "failed": sum(1 for r in results if not r.ok),
            "details": [
                {
                    "account": r.account_id,
                    "label": r.label,
                    "broker": r.broker,
                    "ok": r.ok,
                    "status": r.status,
                    "message": str(r.message or "")[:180],
                    "order_id": r.order_id,
                    "position_id": r.position_id,
                }
                for r in results
            ],
            "extra": dict(extra or {}),
        }
        with self._lock:
            self._follow_log.append(entry)
            if len(self._follow_log) > 500:
                self._follow_log = self._follow_log[-200:]

        try:
            self._follow_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._follow_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")
        except Exception:
            pass

    def get_recent_log(self, n: int = 20) -> list[dict]:
        with self._lock:
            return list(self._dispatch_log[-max(1, int(n or 20)):])

    def status_summary(self) -> dict:
        accounts = account_registry.list_accounts()
        with self._lock:
            tracked_positions = len(self._position_links)
            tracked_orders = len(self._order_links)
            follow_events = len(self._follow_log)
            recent_dispatches = len(self._dispatch_log)
        return {
            "enabled": self.enabled,
            "close_follow_enabled": self.close_follow_enabled,
            "protection_follow_enabled": self.protection_follow_enabled,
            "total_accounts": len(accounts),
            "active_accounts": sum(1 for a in accounts if a.enabled),
            "ctrader_accounts": sum(1 for a in accounts if a.broker == "ctrader" and a.enabled),
            "mt5_accounts": sum(1 for a in accounts if a.broker == "mt5" and a.enabled),
            "recent_dispatches": recent_dispatches,
            "recent_follow_events": follow_events,
            "tracked_master_positions": tracked_positions,
            "tracked_master_orders": tracked_orders,
        }

    def format_telegram_status(self) -> str:
        s = self.status_summary()
        accounts = account_registry.list_accounts()
        lines = [
            "📋 *Copy Trade Status*",
            f"Enabled: {'✅' if s['enabled'] else '❌'}",
            f"Close-follow: {'✅' if s['close_follow_enabled'] else '❌'}",
            f"Protection-follow: {'✅' if s['protection_follow_enabled'] else '❌'}",
            f"Accounts: {s['active_accounts']}/{s['total_accounts']} active",
            f"  cTrader: {s['ctrader_accounts']}",
            f"  MT5: {s['mt5_accounts']}",
            f"Tracked links: pos={s['tracked_master_positions']} order={s['tracked_master_orders']}",
            "",
        ]
        for acc in accounts:
            status = "✅" if acc.enabled else "⏸"
            lines.append(
                f"{status} *{acc.label}* ({acc.broker}) "
                f"risk={acc.risk_multiplier}x max={acc.max_risk_usd}$ "
                f"trades={acc.total_trades}"
            )
        if not accounts:
            lines.append("No follower accounts configured.")
        return "\n".join(lines)


copy_trade_manager = CopyTradeManager()
