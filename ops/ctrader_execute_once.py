"""
ops/ctrader_execute_once.py

One-shot cTrader OpenAPI worker.

Runs in a separate process so Twisted reactor and cTrader OpenAPI lifecycle
never interfere with Dexter's long-lived monitor/MT5 runtime.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from config import config  # noqa: E402
from api.ctrader_token_manager import token_manager  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ctrader_worker")
_WORKER_DEBUG = str(os.getenv("CTRADER_WORKER_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "on"}
_MARKET_DATA_PRICE_SCALE = 100000.0

try:
    from google.protobuf.json_format import MessageToDict
    from twisted.internet import defer, reactor
    from twisted.python.failure import Failure
    from ctrader_open_api import Auth, Client, EndPoints, Protobuf, TcpProtocol
    from ctrader_open_api.messages import OpenApiMessages_pb2 as pb
    from ctrader_open_api.messages import OpenApiModelMessages_pb2 as model
except Exception as import_error:  # pragma: no cover - runtime only
    print(json.dumps({
        "ok": False,
        "status": "import_error",
        "message": str(import_error),
    }, ensure_ascii=True))
    raise SystemExit(0)


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


def _normalize_symbol_key(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())


def _proto_to_dict(message) -> dict:
    try:
        return MessageToDict(message, preserving_proto_field_name=True)
    except Exception:
        return {}


def _ms_to_iso(value) -> str:
    ms = _safe_int(value, 0)
    if ms <= 0:
        return ""
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _debug(*parts) -> None:
    if not _WORKER_DEBUG:
        return
    try:
        print("[ctrader_worker_debug]", *parts, flush=True)
    except Exception:
        pass


def _iso_to_ms(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _scrub_tokens(data: dict) -> dict:
    out = dict(data or {})
    for key in ("accessToken", "refreshToken", "access_token", "refresh_token"):
        if key in out:
            out[key] = "<redacted>"
    return out


def _load_payload(path: str | None) -> dict:
    if not path:
        return {}
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _account_id_from_payload(payload: dict) -> int:
    explicit = _safe_int(payload.get("account_id"), 0)
    if explicit > 0:
        return explicit
    explicit_login = str(payload.get("account_login") or payload.get("account_number") or "").strip()
    if explicit_login:
        row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)(explicit_login, use_demo=getattr(config, "CTRADER_USE_DEMO", False))
        if isinstance(row, dict):
            account_id = _safe_int(row.get("accountId"), 0)
            if account_id > 0:
                return account_id
    raw_login = str(getattr(config, "CTRADER_ACCOUNT_LOGIN", "") or "").strip()
    if raw_login:
        row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)(raw_login, use_demo=getattr(config, "CTRADER_USE_DEMO", False))
        if isinstance(row, dict):
            account_id = _safe_int(row.get("accountId"), 0)
            if account_id > 0:
                return account_id
    raw = str(getattr(config, "CTRADER_ACCOUNT_ID", "") or "").strip()
    if raw:
        row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)(raw)
        if isinstance(row, dict):
            account_id = _safe_int(row.get("accountId"), 0)
            if account_id > 0:
                return account_id
        return _safe_int(raw, 0)
    row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)("", use_demo=getattr(config, "CTRADER_USE_DEMO", False))
    if isinstance(row, dict):
        account_id = _safe_int(row.get("accountId"), 0)
        if account_id > 0:
            return account_id
    return 0


def _resolve_host() -> tuple[str, int, str]:
    override = str(getattr(config, "CTRADER_OPENAPI_PROTOBUF_HOST", "") or "").strip()
    try:
        port = int(getattr(config, "CTRADER_OPENAPI_PROTOBUF_PORT", EndPoints.PROTOBUF_PORT) or EndPoints.PROTOBUF_PORT)
    except Exception:
        port = int(EndPoints.PROTOBUF_PORT)
    port = max(1, min(port, 65535))
    use_demo = bool(getattr(config, "CTRADER_USE_DEMO", False))
    env = "demo" if use_demo else "live"
    if override:
        return override, port, env
    if use_demo:
        return EndPoints.PROTOBUF_DEMO_HOST, int(EndPoints.PROTOBUF_PORT), "demo"
    return EndPoints.PROTOBUF_LIVE_HOST, int(EndPoints.PROTOBUF_PORT), "live"


def _access_token_candidates() -> tuple[str, str]:
    return (
        token_manager.get_access_token(),
        token_manager.get_refresh_token(),
    )


def _try_refresh_tokens() -> tuple[str, dict]:
    new_token = token_manager.try_refresh()
    if new_token:
        return new_token, {"ok": True, "status": "refreshed_via_manager"}
    return "", {"ok": False, "status": "refresh_failed", "message": "token_manager refresh failed"}


def _symbol_candidates(payload: dict) -> list[str]:
    vals: list[str] = []
    for raw in (
        payload.get("symbol"),
        payload.get("market_symbol"),
        payload.get("raw_scores", {}).get("market_symbol"),
    ):
        token = str(raw or "").strip().upper()
        if token and token not in vals:
            vals.append(token)
    return vals


def _normalize_accounts_payload(message) -> list[dict]:
    rows = list(getattr(message, "ctidTraderAccount", []) or [])
    out: list[dict] = []
    for row in rows:
        item = _proto_to_dict(row)
        normalized = {
            "accountId": _safe_int(item.get("ctidTraderAccountId"), 0),
            "accountNumber": _safe_int(item.get("traderLogin"), 0),
            "traderLogin": _safe_int(item.get("traderLogin"), 0),
            "live": bool(item.get("isLive", False)),
            "isLive": bool(item.get("isLive", False)),
            "lastClosingDealTimestamp": str(item.get("lastClosingDealTimestamp", "") or ""),
            "lastBalanceUpdateTimestamp": str(item.get("lastBalanceUpdateTimestamp", "") or ""),
        }
        if normalized["accountId"] > 0:
            out.append(normalized)
    return out


def _resolve_symbol(light_symbols, payload: dict) -> tuple[object | None, str]:
    candidates = _symbol_candidates(payload)
    normalized = {_normalize_symbol_key(x): x for x in candidates if x}
    for sym in list(light_symbols or []):
        name = str(getattr(sym, "symbolName", "") or "").strip().upper()
        key = _normalize_symbol_key(name)
        if key in normalized:
            return sym, "exact_name"
    for sym in list(light_symbols or []):
        name = str(getattr(sym, "symbolName", "") or "").strip().upper()
        desc = str(getattr(sym, "description", "") or "").strip().upper()
        key = _normalize_symbol_key(name)
        if any(tok in key for tok in normalized.keys()):
            return sym, "contains_name"
        if any(tok in _normalize_symbol_key(desc) for tok in normalized.keys()):
            return sym, "contains_desc"
    return None, "not_found"


def _build_symbol_map(rows) -> dict[int, str]:
    out: dict[int, str] = {}
    for sym in list(rows or []):
        try:
            sid = _safe_int(getattr(sym, "symbolId", 0), 0)
        except Exception:
            sid = 0
        name = str(getattr(sym, "symbolName", "") or "").strip().upper()
        if sid > 0 and name:
            out[sid] = name
    return out


def _normalize_capture_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_position(position, symbol_map: dict[int, str]) -> dict:
    raw = _proto_to_dict(position)
    trade = dict(raw.get("tradeData") or {})
    symbol_id = _safe_int(trade.get("symbolId"), _safe_int(raw.get("symbolId"), 0))
    side_token = str(trade.get("tradeSide") or raw.get("tradeSide") or "").strip().upper()
    return {
        "position_id": _safe_int(raw.get("positionId"), 0),
        "symbol_id": symbol_id,
        "symbol": str(symbol_map.get(symbol_id, "") or "").strip().upper(),
        "direction": "long" if side_token == "BUY" else ("short" if side_token == "SELL" else ""),
        "volume": _safe_int(trade.get("volume"), 0),
        "entry_price": _safe_float(raw.get("price"), 0.0),
        "stop_loss": _safe_float(raw.get("stopLoss"), 0.0),
        "take_profit": _safe_float(raw.get("takeProfit"), 0.0),
        "label": str(trade.get("label", "") or raw.get("label", "") or ""),
        "comment": str(trade.get("comment", "") or raw.get("comment", "") or ""),
        "open_timestamp_ms": _safe_int(trade.get("openTimestamp"), 0),
        "open_utc": _ms_to_iso(trade.get("openTimestamp")),
        "updated_timestamp_ms": _safe_int(raw.get("utcLastUpdateTimestamp"), 0),
        "updated_utc": _ms_to_iso(raw.get("utcLastUpdateTimestamp")),
        "status": str(raw.get("positionStatus", "") or ""),
        "swap": _safe_float(raw.get("swap"), 0.0),
        "commission": _safe_float(raw.get("commission"), 0.0),
        "used_margin": _safe_float(raw.get("usedMargin"), 0.0),
        "money_digits": _safe_int(raw.get("moneyDigits"), 2),
        "raw": raw,
    }


def _normalize_deal(deal, symbol_map: dict[int, str]) -> dict:
    raw = _proto_to_dict(deal)
    symbol_id = _safe_int(raw.get("symbolId"), 0)
    close_detail = dict(raw.get("closePositionDetail") or {})
    digits = max(0, _safe_int(close_detail.get("moneyDigits"), _safe_int(raw.get("moneyDigits"), 2)))
    scale = float(10 ** digits) if digits > 0 else 1.0
    gross_profit = _safe_float(close_detail.get("grossProfit"), 0.0) / scale
    swap = _safe_float(close_detail.get("swap"), 0.0) / scale
    commission = _safe_float(close_detail.get("commission"), 0.0) / scale
    pnl_fee = _safe_float(close_detail.get("pnlConversionFee"), 0.0) / scale
    net_pnl = gross_profit + swap + commission + pnl_fee
    side_token = str(raw.get("tradeSide") or "").strip().upper()
    return {
        "deal_id": _safe_int(raw.get("dealId"), 0),
        "order_id": _safe_int(raw.get("orderId"), 0),
        "position_id": _safe_int(raw.get("positionId"), 0),
        "symbol_id": symbol_id,
        "symbol": str(symbol_map.get(symbol_id, "") or "").strip().upper(),
        "direction": "long" if side_token == "BUY" else ("short" if side_token == "SELL" else ""),
        "volume": _safe_int(raw.get("volume"), 0),
        "filled_volume": _safe_int(raw.get("filledVolume"), 0),
        "execution_price": _safe_float(raw.get("executionPrice"), 0.0),
        "execution_timestamp_ms": _safe_int(raw.get("executionTimestamp"), 0),
        "execution_utc": _ms_to_iso(raw.get("executionTimestamp")),
        "create_timestamp_ms": _safe_int(raw.get("createTimestamp"), 0),
        "deal_status": str(raw.get("dealStatus", "") or ""),
        "gross_profit_usd": gross_profit,
        "swap_usd": swap,
        "commission_usd": commission,
        "pnl_conversion_fee_usd": pnl_fee,
        "pnl_usd": net_pnl,
        "has_close_detail": bool(close_detail),
        "entry_price": _safe_float(close_detail.get("entryPrice"), 0.0),
        "closed_volume": _safe_int(close_detail.get("closedVolume"), 0),
        "balance_after_usd": _safe_float(close_detail.get("balance"), 0.0) / scale if close_detail else 0.0,
        "raw": raw,
    }


def _quantize_volume(meta, payload: dict) -> tuple[int, dict]:
    fixed_volume = _safe_int(payload.get("fixed_volume"), 0)
    min_volume = max(1, _safe_int(getattr(meta, "minVolume", 1), 1))
    step_volume = max(1, _safe_int(getattr(meta, "stepVolume", 1), 1))
    max_volume = max(min_volume, _safe_int(getattr(meta, "maxVolume", min_volume), min_volume))
    risk_usd = max(0.0, _safe_float(payload.get("risk_usd"), 0.0))
    entry = _safe_float(payload.get("entry"), 0.0)
    stop_loss = _safe_float(payload.get("stop_loss"), 0.0)
    risk_price = abs(entry - stop_loss)
    raw_volume = 0.0
    if fixed_volume > 0:
        vol = fixed_volume
        reason = "fixed_volume"
    elif risk_usd > 0 and risk_price > 0:
        raw_volume = risk_usd / risk_price
        vol = int(raw_volume)
        reason = "approx_quote_risk"
    else:
        vol = min_volume
        reason = "min_volume_fallback"
    if vol < min_volume:
        vol = min_volume
    if step_volume > 1:
        vol = max(min_volume, int(vol // step_volume) * step_volume)
    vol = min(max_volume, max(min_volume, int(vol)))
    return int(vol), {
        "reason": reason,
        "risk_usd": round(risk_usd, 4),
        "risk_price": round(risk_price, 8),
        "min_volume": int(min_volume),
        "step_volume": int(step_volume),
        "max_volume": int(max_volume),
        "raw_volume": round(raw_volume, 6),
    }


def _relative_distance(price_a: float, price_b: float) -> int:
    distance = abs(_safe_float(price_a, 0.0) - _safe_float(price_b, 0.0))
    if distance <= 0:
        return 0
    return max(0, int(round(distance * 100000)))


def _price_digits(symbol_meta, trader) -> int:
    for cand in (
        getattr(symbol_meta, "digits", None),
        getattr(symbol_meta, "moneyDigits", None),
        getattr(trader, "moneyDigits", None),
        2,
    ):
        try:
            return max(0, int(cand))
        except Exception:
            continue
    return 2


def _quantize_price(value: float, digits: int) -> float:
    return round(_safe_float(value, 0.0), max(0, int(digits or 0)))


@defer.inlineCallbacks
def _workflow(mode: str, payload: dict):
    host, port, environment = _resolve_host()
    if mode == "accounts":
        host, port, environment = EndPoints.PROTOBUF_LIVE_HOST, int(EndPoints.PROTOBUF_PORT), "live"
    account_id = _account_id_from_payload(payload)
    client_id = str(getattr(config, "CTRADER_OPENAPI_CLIENT_ID", "") or "").strip()
    client_secret = str(getattr(config, "CTRADER_OPENAPI_CLIENT_SECRET", "") or "").strip()
    access_token, refresh_token = _access_token_candidates()

    if not client_id or not client_secret:
        defer.returnValue({
            "ok": False,
            "status": "credentials_missing",
            "message": "client id/secret missing",
            "account_id": account_id or None,
        })
        return
    if mode != "accounts" and account_id <= 0:
        defer.returnValue({
            "ok": False,
            "status": "account_missing",
            "message": "ctidTraderAccountId missing",
            "account_id": None,
        })
        return

    client = None
    refreshed_meta: dict = {}

    def _is_error(message) -> bool:
        return isinstance(message, pb.ProtoOAErrorRes)

    def _enum_name(enum_cls, value: int) -> str:
        try:
            return str(enum_cls.Name(int(value)))
        except Exception:
            return str(value)

    try:
        _debug("workflow_start", mode, "account", account_id, "env", environment)
        client = Client(host, port, TcpProtocol)
        client.startService()
        _debug("service_started")
        yield client.whenConnected(failAfterFailures=1)
        _debug("connected")

        app_msg = yield client.send(
            pb.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
            responseTimeoutInSeconds=10,
        )
        app_payload = Protobuf.extract(app_msg)
        _debug("app_auth", type(app_payload).__name__)
        if _is_error(app_payload):
            _ec = str(getattr(app_payload, "errorCode", "") or "")
            _desc = str(getattr(app_payload, "description", "") or "")
            _cred = (
                "Protobuf application auth (clientId+clientSecret) failed — before access token. "
                "Values must come from Dexter `.env.local` (CTRADER_OPENAPI_CLIENT_ID/SECRET or OpenAPI_ClientID/OpenAPI_Secreat): "
                "Mempalac `CTRADER_*` in `trading_ai/.env` is not read by this worker subprocess. "
                "Match https://openapi.ctrader.com exactly; no quotes or trailing spaces. "
                "If the app or secret was rotated, update both and refresh OAuth tokens."
            )
            _route = ""
            if _ec.upper() == "CANT_ROUTE_REQUEST" or "cannot route" in (_desc or "").lower():
                _route = (
                    " Routing: `CANT_ROUTE_REQUEST` here usually means the clientId/clientSecret pair is wrong for this app, "
                    "or protobuf routing failed. Confirm `CTRADER_USE_DEMO` matches your account type (demo vs live uses different hosts). "
                    "If you are on the correct credentials and still see this, try `CTRADER_OPENAPI_PROTOBUF_HOST` from "
                    "https://help.ctrader.com/open-api/proxies-endpoints/ (protobuf port 5035)."
                )
            defer.returnValue({
                "ok": False,
                "status": "app_auth_failed",
                "message": _desc or _ec or "application auth failed",
                "error_code": _ec,
                "account_id": int(account_id),
                "environment": environment,
                "host": str(host),
                "client_id_prefix": (client_id[:12] + "…") if len(client_id) > 12 else client_id,
                "hint": (_cred + _route).strip(),
            })
            return

        if not access_token and refresh_token:
            access_token, refreshed_meta = _try_refresh_tokens()

        if mode == "accounts":
            acct_msg = yield client.send(
                pb.ProtoOAGetAccountListByAccessTokenReq(accessToken=str(access_token or "")),
                responseTimeoutInSeconds=10,
            )
            acct_payload = Protobuf.extract(acct_msg)
            if _is_error(acct_payload):
                err_code = str(getattr(acct_payload, "errorCode", "") or "")
                if (err_code == "CH_ACCESS_TOKEN_INVALID") and refresh_token:
                    access_token, refreshed_meta = _try_refresh_tokens()
                    if access_token:
                        acct_msg = yield client.send(
                            pb.ProtoOAGetAccountListByAccessTokenReq(accessToken=str(access_token or "")),
                            responseTimeoutInSeconds=10,
                        )
                        acct_payload = Protobuf.extract(acct_msg)
                if _is_error(acct_payload):
                    defer.returnValue({
                        "ok": False,
                        "status": "accounts_failed",
                        "message": str(getattr(acct_payload, "description", "") or getattr(acct_payload, "errorCode", "account list failed")),
                        "environment": environment,
                        "token_refresh": dict(refreshed_meta or {}),
                    })
                    return
            accounts = _normalize_accounts_payload(acct_payload)
            defer.returnValue({
                "ok": True,
                "status": "accounts_loaded",
                "message": f"loaded {len(accounts)} accounts",
                "environment": environment,
                "accounts": accounts,
                "token_refresh": dict(refreshed_meta or {}),
            })
            return

        def _account_auth(token: str):
            return client.send(
                pb.ProtoOAAccountAuthReq(ctidTraderAccountId=int(account_id), accessToken=str(token or "")),
                responseTimeoutInSeconds=10,
            )

        acc_msg = yield _account_auth(access_token)
        acc_payload = Protobuf.extract(acc_msg)
        _debug("account_auth", type(acc_payload).__name__)
        if _is_error(acc_payload):
            err_code = str(getattr(acc_payload, "errorCode", "") or "")
            if (err_code == "CH_ACCESS_TOKEN_INVALID") and refresh_token:
                access_token, refreshed_meta = _try_refresh_tokens()
                if access_token:
                    acc_msg = yield _account_auth(access_token)
                    acc_payload = Protobuf.extract(acc_msg)
            if _is_error(acc_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "account_auth_failed",
                    "message": str(getattr(acc_payload, "description", "") or getattr(acc_payload, "errorCode", "account auth failed")),
                    "account_id": int(account_id),
                    "environment": environment,
                    "token_refresh": dict(refreshed_meta or {}),
                })
                return

        trader_msg = yield client.send(pb.ProtoOATraderReq(ctidTraderAccountId=int(account_id)), responseTimeoutInSeconds=10)
        trader_payload = Protobuf.extract(trader_msg)
        _debug("trader", type(trader_payload).__name__)
        reconcile_msg = yield client.send(pb.ProtoOAReconcileReq(ctidTraderAccountId=int(account_id)), responseTimeoutInSeconds=10)
        reconcile_payload = Protobuf.extract(reconcile_msg)
        _debug("reconcile", type(reconcile_payload).__name__)
        trader = getattr(trader_payload, "trader", None)

        if mode == "health":
            defer.returnValue({
                "ok": True,
                "status": "connected",
                "message": "ctrader health ok",
                "account_id": int(account_id),
                "environment": environment,
                "balance": _safe_float(getattr(trader, "balance", 0.0), 0.0),
                "money_digits": _safe_int(getattr(trader, "moneyDigits", 2), 2),
                "leverage_in_cents": _safe_int(getattr(trader, "leverageInCents", 0), 0),
                "positions": len(list(getattr(reconcile_payload, "position", []) or [])),
                "orders": len(list(getattr(reconcile_payload, "order", []) or [])),
                "token_refresh": dict(refreshed_meta or {}),
            })
            return

        if mode == "close":
            position_id = _safe_int(payload.get("position_id"), 0)
            volume = _safe_int(payload.get("volume"), 0)
            if position_id <= 0:
                defer.returnValue({
                    "ok": False,
                    "status": "position_missing",
                    "message": "position_id missing",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            close_msg = yield client.send(
                pb.ProtoOAClosePositionReq(
                    ctidTraderAccountId=int(account_id),
                    positionId=int(position_id),
                    volume=int(volume),
                ),
                responseTimeoutInSeconds=15,
            )
            close_payload = Protobuf.extract(close_msg)
            if isinstance(close_payload, pb.ProtoOAOrderErrorEvent) or _is_error(close_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "close_rejected",
                    "message": str(getattr(close_payload, "description", "") or getattr(close_payload, "errorCode", "close rejected")),
                    "account_id": int(account_id),
                    "position_id": int(position_id),
                    "environment": environment,
                    "raw": _proto_to_dict(close_payload),
                })
                return
            execution_type = _enum_name(model.ProtoOAExecutionType, getattr(close_payload, "executionType", 0))
            order = getattr(close_payload, "order", None)
            position = getattr(close_payload, "position", None)
            deal = getattr(close_payload, "deal", None)
            defer.returnValue({
                "ok": execution_type in {"ORDER_FILLED", "ORDER_PARTIAL_FILL", "ORDER_ACCEPTED"},
                "status": "closed" if execution_type in {"ORDER_FILLED", "ORDER_PARTIAL_FILL"} else "close_submitted",
                "message": f"ctrader {execution_type.lower()}",
                "account_id": int(account_id),
                "position_id": _safe_int(getattr(position, "positionId", 0), position_id) or int(position_id),
                "order_id": _safe_int(getattr(order, "orderId", 0), 0) or None,
                "deal_id": _safe_int(getattr(deal, "dealId", 0), 0) or None,
                "environment": environment,
                "execution_meta": {
                    "execution_type": execution_type,
                    "raw_execution": _proto_to_dict(close_payload),
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return

        if mode == "cancel_order":
            order_id = _safe_int(payload.get("order_id"), 0)
            if order_id <= 0:
                defer.returnValue({
                    "ok": False,
                    "status": "order_missing",
                    "message": "order_id missing",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            cancel_msg = yield client.send(
                pb.ProtoOACancelOrderReq(
                    ctidTraderAccountId=int(account_id),
                    orderId=int(order_id),
                ),
                responseTimeoutInSeconds=15,
            )
            cancel_payload = Protobuf.extract(cancel_msg)
            if isinstance(cancel_payload, pb.ProtoOAOrderErrorEvent) or _is_error(cancel_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "cancel_rejected",
                    "message": str(getattr(cancel_payload, "description", "") or getattr(cancel_payload, "errorCode", "cancel rejected")),
                    "account_id": int(account_id),
                    "order_id": int(order_id),
                    "environment": environment,
                    "raw": _proto_to_dict(cancel_payload),
                })
                return
            execution_type = _enum_name(model.ProtoOAExecutionType, getattr(cancel_payload, "executionType", 0))
            order = getattr(cancel_payload, "order", None)
            defer.returnValue({
                "ok": True,
                "status": "canceled",
                "message": (
                    f"ctrader {execution_type.lower()}"
                    if execution_type not in {"", "0"}
                    else "ctrader canceled pending order"
                ),
                "account_id": int(account_id),
                "order_id": _safe_int(getattr(order, "orderId", 0), order_id) or int(order_id),
                "environment": environment,
                "execution_meta": {
                    "execution_type": execution_type,
                    "raw_execution": _proto_to_dict(cancel_payload),
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return

        if mode == "amend_order":
            order_id = _safe_int(payload.get("order_id"), 0)
            limit_price = _safe_float(payload.get("limit_price"), 0.0)
            stop_price = _safe_float(payload.get("stop_price"), 0.0)
            stop_loss = _safe_float(payload.get("stop_loss"), 0.0)
            take_profit = _safe_float(payload.get("take_profit"), 0.0)
            volume = _safe_int(payload.get("volume"), 0)
            trailing_stop_loss = bool(payload.get("trailing_stop_loss", False))
            if order_id <= 0:
                defer.returnValue({
                    "ok": False,
                    "status": "order_missing",
                    "message": "order_id missing",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            amend_kwargs = {
                "ctidTraderAccountId": int(account_id),
                "orderId": int(order_id),
                "trailingStopLoss": bool(trailing_stop_loss),
            }
            if volume > 0:
                amend_kwargs["volume"] = int(volume)
            if limit_price > 0:
                amend_kwargs["limitPrice"] = float(limit_price)
            if stop_price > 0:
                amend_kwargs["stopPrice"] = float(stop_price)
            if stop_loss > 0:
                amend_kwargs["stopLoss"] = float(stop_loss)
            if take_profit > 0:
                amend_kwargs["takeProfit"] = float(take_profit)
            amend_msg = yield client.send(
                pb.ProtoOAAmendOrderReq(**amend_kwargs),
                responseTimeoutInSeconds=15,
            )
            amend_payload = Protobuf.extract(amend_msg)
            if isinstance(amend_payload, pb.ProtoOAOrderErrorEvent) or _is_error(amend_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "amend_order_rejected",
                    "message": str(getattr(amend_payload, "description", "") or getattr(amend_payload, "errorCode", "amend order rejected")),
                    "account_id": int(account_id),
                    "order_id": int(order_id),
                    "environment": environment,
                    "raw": _proto_to_dict(amend_payload),
                })
                return
            execution_type = _enum_name(model.ProtoOAExecutionType, getattr(amend_payload, "executionType", 0))
            order = getattr(amend_payload, "order", None)
            defer.returnValue({
                "ok": True,
                "status": "amended_order",
                "message": (
                    f"ctrader amended order {execution_type.lower()}"
                    if execution_type not in {"", "0"}
                    else "ctrader amended order"
                ),
                "account_id": int(account_id),
                "order_id": _safe_int(getattr(order, "orderId", 0), order_id) or int(order_id),
                "environment": environment,
                "execution_meta": {
                    "execution_type": execution_type,
                    "raw_execution": _proto_to_dict(amend_payload),
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return

        if mode == "amend_position_sltp":
            position_id = _safe_int(payload.get("position_id"), 0)
            stop_loss = _safe_float(payload.get("stop_loss"), 0.0)
            take_profit = _safe_float(payload.get("take_profit"), 0.0)
            trailing_stop_loss = bool(payload.get("trailing_stop_loss", False))
            if position_id <= 0:
                defer.returnValue({
                    "ok": False,
                    "status": "position_missing",
                    "message": "position_id missing",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            amend_kwargs = {
                "ctidTraderAccountId": int(account_id),
                "positionId": int(position_id),
                "trailingStopLoss": bool(trailing_stop_loss),
            }
            if stop_loss > 0:
                amend_kwargs["stopLoss"] = float(stop_loss)
            if take_profit > 0:
                amend_kwargs["takeProfit"] = float(take_profit)
            amend_msg = yield client.send(
                pb.ProtoOAAmendPositionSLTPReq(**amend_kwargs),
                responseTimeoutInSeconds=15,
            )
            amend_payload = Protobuf.extract(amend_msg)
            if isinstance(amend_payload, pb.ProtoOAOrderErrorEvent) or _is_error(amend_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "amend_rejected",
                    "message": str(getattr(amend_payload, "description", "") or getattr(amend_payload, "errorCode", "amend rejected")),
                    "account_id": int(account_id),
                    "position_id": int(position_id),
                    "environment": environment,
                    "raw": _proto_to_dict(amend_payload),
                })
                return
            execution_type = _enum_name(model.ProtoOAExecutionType, getattr(amend_payload, "executionType", 0))
            position = getattr(amend_payload, "position", None)
            defer.returnValue({
                "ok": True,
                "status": "amended",
                "message": (
                    f"ctrader amended {execution_type.lower()}"
                    if execution_type not in {"", "0"}
                    else "ctrader amended position sl/tp"
                ),
                "account_id": int(account_id),
                "position_id": _safe_int(getattr(position, "positionId", 0), position_id) or int(position_id),
                "environment": environment,
                "execution_meta": {
                    "execution_type": execution_type,
                    "raw_execution": _proto_to_dict(amend_payload),
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return

        symbols_msg = yield client.send(
            pb.ProtoOASymbolsListReq(ctidTraderAccountId=int(account_id), includeArchivedSymbols=False),
            responseTimeoutInSeconds=12,
        )
        symbols_payload = Protobuf.extract(symbols_msg)
        _debug("symbols_list", type(symbols_payload).__name__)
        light_symbols = list(getattr(symbols_payload, "symbol", []) or [])
        symbol_map = _build_symbol_map(light_symbols)

        if mode == "capture_market":
            capture_symbols = [
                _normalize_capture_symbol(sym)
                for sym in list(payload.get("symbols") or [])
                if _normalize_capture_symbol(sym)
            ]
            if not capture_symbols:
                defer.returnValue({
                    "ok": False,
                    "status": "capture_symbols_missing",
                    "message": "symbols missing",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            include_depth = bool(payload.get("include_depth", True))
            duration_sec = max(3, _safe_int(payload.get("duration_sec"), 12))
            max_events = max(50, _safe_int(payload.get("max_events"), 600))
            resolved_symbols: list[dict] = []
            symbol_ids: list[int] = []
            symbol_name_by_id: dict[int, str] = {}
            symbol_meta_by_id: dict[int, object] = {}
            seen_symbol_ids: set[int] = set()
            for token in capture_symbols:
                light_symbol, match_reason = _resolve_symbol(light_symbols, {"symbol": token, "market_symbol": token})
                if light_symbol is None:
                    continue
                symbol_id = _safe_int(getattr(light_symbol, "symbolId", 0), 0)
                symbol_name = str(getattr(light_symbol, "symbolName", "") or "").strip().upper()
                if symbol_id <= 0 or not symbol_name or symbol_id in seen_symbol_ids:
                    continue
                seen_symbol_ids.add(symbol_id)
                symbol_ids.append(symbol_id)
                symbol_name_by_id[symbol_id] = symbol_name
                resolved_symbols.append({"symbol": symbol_name, "symbol_id": symbol_id, "match": match_reason})
            if not symbol_ids:
                defer.returnValue({
                    "ok": False,
                    "status": "capture_symbols_not_found",
                    "message": f"no symbols resolved from {capture_symbols}",
                    "account_id": int(account_id),
                    "environment": environment,
                })
                return
            try:
                symbol_meta_msg = yield client.send(
                    pb.ProtoOASymbolByIdReq(
                        ctidTraderAccountId=int(account_id),
                        symbolId=[int(v) for v in symbol_ids],
                    ),
                    responseTimeoutInSeconds=4,
                )
                symbol_meta_payload = Protobuf.extract(symbol_meta_msg)
                _debug("symbol_by_id", type(symbol_meta_payload).__name__, "ids", symbol_ids)
                for meta in list(getattr(symbol_meta_payload, "symbol", []) or []):
                    sid = _safe_int(getattr(meta, "symbolId", 0), 0)
                    if sid > 0:
                        symbol_meta_by_id[sid] = meta
            except Exception:
                symbol_meta_by_id = {}

            now_ms = int(time.time() * 1000)
            spot_events: list[dict] = []
            depth_events: list[dict] = []
            total_events = 0
            used_tick_fallback = False
            max_depth_levels = max(1, _safe_int(payload.get("max_depth_levels"), 5))
            scale_by_symbol_id: dict[int, float] = {}
            for sid in list(symbol_ids or []):
                # cTrader live spot/depth events arrive in 1e-5 price units for these CFD/crypto symbols.
                # Using symbol digits here under-scales by ~1000x and corrupts replay metrics.
                scale_by_symbol_id[int(sid)] = float(_MARKET_DATA_PRICE_SCALE)

            capture_wait = defer.Deferred()
            capture_finish_scheduled = False
            protocol = yield client.whenConnected(failAfterFailures=1)

            def _maybe_finish_capture() -> None:
                nonlocal capture_finish_scheduled
                if (total_events >= max_events) and (not capture_wait.called) and (not capture_finish_scheduled):
                    capture_finish_scheduled = True
                    reactor.callLater(0, lambda: (not capture_wait.called) and capture_wait.callback(True))

            def _append_spot_event(evt) -> None:
                nonlocal total_events
                if total_events >= max_events:
                    return
                sid = _safe_int(getattr(evt, "symbolId", 0), 0)
                if sid <= 0:
                    return
                scale = float(scale_by_symbol_id.get(int(sid), 1.0) or 1.0)
                bid_raw = _safe_float(getattr(evt, "bid", 0.0), 0.0)
                ask_raw = _safe_float(getattr(evt, "ask", 0.0), 0.0)
                bid = (bid_raw / scale) if bid_raw > 0 else 0.0
                ask = (ask_raw / scale) if ask_raw > 0 else 0.0
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else (bid or ask or 0.0)
                spread = (ask - bid) if bid > 0 and ask > 0 else 0.0
                ts_ms = _safe_int(getattr(evt, "timestamp", 0), 0) or int(time.time() * 1000)
                spot_events.append(
                    {
                        "account_id": int(account_id),
                        "symbol_id": int(sid),
                        "symbol": symbol_name_by_id.get(int(sid), ""),
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "spread": spread,
                        "spread_pct": ((spread / mid) * 100.0) if mid > 0 and spread >= 0 else 0.0,
                        "event_utc": _ms_to_iso(ts_ms),
                        "event_ts": round(ts_ms / 1000.0, 3),
                    }
                )
                total_events += 1
                _maybe_finish_capture()

            def _append_depth_event(evt) -> None:
                nonlocal total_events
                sid = _safe_int(getattr(evt, "symbolId", 0), 0)
                if sid <= 0:
                    return
                scale = float(scale_by_symbol_id.get(int(sid), 1.0) or 1.0)
                event_ts_ms = int(time.time() * 1000)
                level_counts = {"bid": 0, "ask": 0}
                for quote in list(getattr(evt, "newQuotes", []) or []):
                    if total_events >= max_events:
                        break
                    size = _safe_float(getattr(quote, "size", 0.0), 0.0)
                    bid_raw = _safe_float(getattr(quote, "bid", 0.0), 0.0)
                    ask_raw = _safe_float(getattr(quote, "ask", 0.0), 0.0)
                    side = ""
                    price = 0.0
                    if bid_raw > 0:
                        side = "bid"
                        price = bid_raw / scale
                    elif ask_raw > 0:
                        side = "ask"
                        price = ask_raw / scale
                    if not side or price <= 0:
                        continue
                    level_index = int(level_counts.get(side, 0))
                    if level_index >= max_depth_levels:
                        continue
                    level_counts[side] = level_index + 1
                    depth_events.append(
                        {
                            "account_id": int(account_id),
                            "symbol_id": int(sid),
                            "symbol": symbol_name_by_id.get(int(sid), ""),
                            "quote_id": _safe_int(getattr(quote, "id", 0), 0) or None,
                            "side": side,
                            "price": price,
                            "size": size,
                            "level_index": level_index,
                            "event_utc": _ms_to_iso(event_ts_ms),
                            "event_ts": round(event_ts_ms / 1000.0, 3),
                        }
                    )
                    total_events += 1
                _maybe_finish_capture()

            def _on_market_message(_client, message) -> None:
                try:
                    payload_evt = Protobuf.extract(message)
                except Exception:
                    return
                if isinstance(payload_evt, pb.ProtoOASpotEvent):
                    _append_spot_event(payload_evt)
                elif isinstance(payload_evt, pb.ProtoOADepthEvent):
                    _append_depth_event(payload_evt)

            client.setMessageReceivedCallback(_on_market_message)
            try:
                protocol.send(
                    pb.ProtoOASubscribeSpotsReq(
                        ctidTraderAccountId=int(account_id),
                        symbolId=[int(v) for v in symbol_ids],
                        subscribeToSpotTimestamp=True,
                    ),
                    instant=True,
                )
            except Exception:
                pass
            if include_depth:
                try:
                    protocol.send(
                        pb.ProtoOASubscribeDepthQuotesReq(
                            ctidTraderAccountId=int(account_id),
                            symbolId=[int(v) for v in symbol_ids],
                        ),
                        instant=True,
                    )
                except Exception:
                    include_depth = False

            reactor.callLater(duration_sec, lambda: (not capture_wait.called) and capture_wait.callback(True))
            try:
                yield capture_wait
            except Exception:
                pass
            _debug("capture_wait_done", "spots", len(spot_events), "depth", len(depth_events), "events", total_events)

            try:
                protocol.send(
                    pb.ProtoOAUnsubscribeSpotsReq(
                        ctidTraderAccountId=int(account_id),
                        symbolId=[int(v) for v in symbol_ids],
                    ),
                    instant=True,
                )
            except Exception:
                pass
            if include_depth:
                try:
                    protocol.send(
                        pb.ProtoOAUnsubscribeDepthQuotesReq(
                            ctidTraderAccountId=int(account_id),
                            symbolId=[int(v) for v in symbol_ids],
                        ),
                        instant=True,
                    )
                except Exception:
                    pass

            # Fallback to recent tick API when the live stream stayed quiet during the capture window.
            if not spot_events:
                used_tick_fallback = True
                from_ms = max(0, now_ms - (duration_sec * 1000))
                for sid in list(symbol_ids or []):
                    if total_events >= max_events:
                        break
                    scale = float(scale_by_symbol_id.get(int(sid), 1.0) or 1.0)
                    tick_series: dict[int, dict[str, float]] = {}
                    for quote_type in (model.ProtoOAQuoteType.BID, model.ProtoOAQuoteType.ASK):
                        try:
                            tick_msg = yield client.send(
                                pb.ProtoOAGetTickDataReq(
                                    ctidTraderAccountId=int(account_id),
                                    symbolId=int(sid),
                                    type=int(quote_type),
                                    fromTimestamp=int(from_ms),
                                    toTimestamp=int(now_ms),
                                ),
                                responseTimeoutInSeconds=4,
                            )
                            tick_payload = Protobuf.extract(tick_msg)
                        except Exception:
                            continue
                        if _is_error(tick_payload):
                            continue
                        side = "bid" if int(quote_type) == int(model.ProtoOAQuoteType.BID) else "ask"
                        for tick_row in list(getattr(tick_payload, "tickData", []) or []):
                            ts_ms = _safe_int(getattr(tick_row, "timestamp", 0), 0)
                            raw_tick = _safe_float(getattr(tick_row, "tick", 0.0), 0.0)
                            if ts_ms <= 0 or raw_tick <= 0:
                                continue
                            item = tick_series.setdefault(ts_ms, {})
                            item[side] = raw_tick / scale
                    for ts_ms in sorted(tick_series.keys()):
                        item = tick_series.get(ts_ms) or {}
                        bid = _safe_float(item.get("bid"), 0.0)
                        ask = _safe_float(item.get("ask"), 0.0)
                        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else (bid or ask or 0.0)
                        spread = (ask - bid) if bid > 0 and ask > 0 else 0.0
                        spot_events.append(
                            {
                                "account_id": int(account_id),
                                "symbol_id": int(sid),
                                "symbol": symbol_name_by_id.get(sid, ""),
                                "bid": bid,
                                "ask": ask,
                                "mid": mid,
                                "spread": spread,
                                "spread_pct": ((spread / mid) * 100.0) if mid > 0 and spread >= 0 else 0.0,
                                "event_utc": _ms_to_iso(ts_ms),
                                "event_ts": round(ts_ms / 1000.0, 3),
                            }
                        )
                        total_events += 1
                        if total_events >= max_events:
                            break
            run_id = datetime.now(timezone.utc).strftime("ctcap_%Y%m%d_%H%M%S")
            _debug("capture_return", run_id, "spots", len(spot_events), "depth", len(depth_events), "fallback", used_tick_fallback)
            defer.returnValue({
                "ok": True,
                "status": "captured_live" if depth_events else ("captured_live_spot_only" if spot_events else "captured_empty"),
                "message": (
                    f"captured spots={len(spot_events)} depth={len(depth_events)} "
                    f"mode={'tick_fallback' if used_tick_fallback else 'live_subscribe'}"
                ),
                "run_id": run_id,
                "account_id": int(account_id),
                "environment": environment,
                "duration_sec": int(duration_sec),
                "include_depth": bool(include_depth),
                "symbols": resolved_symbols,
                "captured_at": _ms_to_iso(int(time.time() * 1000)),
                "spots": spot_events[:max_events],
                "depth": depth_events[:max_events],
                "token_refresh": dict(refreshed_meta or {}),
            })
            return

        if mode == "get_trendbars":
            # ── Historical OHLCV bars via ProtoOAGetTrendbarsReq ──────────
            _TF_TO_PERIOD = {
                "1m": 1, "2m": 2, "3m": 3, "4m": 4, "5m": 5,
                "10m": 6, "15m": 7, "30m": 8,
                "1h": 9, "4h": 10, "12h": 11,
                "1d": 12, "1w": 13, "1mn": 14,
            }
            tb_symbol = str(payload.get("symbol", "XAUUSD") or "XAUUSD")
            tb_tf = str(payload.get("timeframe", "5m") or "5m").lower()
            tb_period = _TF_TO_PERIOD.get(tb_tf)
            if tb_period is None:
                defer.returnValue({
                    "ok": False,
                    "status": "invalid_timeframe",
                    "message": f"unsupported timeframe: {tb_tf}, valid: {list(_TF_TO_PERIOD.keys())}",
                })
                return
            from_ms = _safe_int(payload.get("from_ms"), 0)
            to_ms = _safe_int(payload.get("to_ms"), int(time.time() * 1000))
            tb_count = max(1, min(_safe_int(payload.get("count"), 5000), 14000))
            tb_symbol_obj, tb_match = _resolve_symbol(light_symbols, {"symbol": tb_symbol, "market_symbol": tb_symbol})
            if tb_symbol_obj is None:
                defer.returnValue({
                    "ok": False,
                    "status": "symbol_not_found",
                    "message": f"symbol not found: {tb_symbol}",
                })
                return
            tb_symbol_id = _safe_int(getattr(tb_symbol_obj, "symbolId", 0), 0)
            tb_symbol_name = str(getattr(tb_symbol_obj, "symbolName", "") or "").strip()
            # Get symbol digits for price scale
            try:
                tb_meta_msg = yield client.send(
                    pb.ProtoOASymbolByIdReq(ctidTraderAccountId=int(account_id), symbolId=[int(tb_symbol_id)]),
                    responseTimeoutInSeconds=8,
                )
                tb_meta_payload = Protobuf.extract(tb_meta_msg)
                tb_meta_list = list(getattr(tb_meta_payload, "symbol", []) or [])
                tb_meta = tb_meta_list[0] if tb_meta_list else None
                tb_digits = _safe_int(getattr(tb_meta, "digits", 5), 5) if tb_meta else 5
            except Exception:
                tb_digits = 5
            # cTrader trendbar raw prices use 1e-5 rate units (same as spot events),
            # NOT the symbol's display digits.  Using symbol digits under-scales by ~1000x.
            tb_scale = float(_MARKET_DATA_PRICE_SCALE)
            _debug("get_trendbars", tb_symbol_name, "id", tb_symbol_id, "tf", tb_tf, "period", tb_period, "digits", tb_digits, "scale", tb_scale)
            try:
                tb_msg = yield client.send(
                    pb.ProtoOAGetTrendbarsReq(
                        ctidTraderAccountId=int(account_id),
                        symbolId=int(tb_symbol_id),
                        period=int(tb_period),
                        fromTimestamp=int(from_ms),
                        toTimestamp=int(to_ms),
                        count=int(tb_count),
                    ),
                    responseTimeoutInSeconds=15,
                )
            except Exception as tb_err:
                defer.returnValue({
                    "ok": False,
                    "status": "trendbar_request_failed",
                    "message": str(tb_err),
                    "symbol": tb_symbol_name,
                    "timeframe": tb_tf,
                })
                return
            tb_payload = Protobuf.extract(tb_msg)
            if _is_error(tb_payload):
                defer.returnValue({
                    "ok": False,
                    "status": "trendbar_error",
                    "message": str(getattr(tb_payload, "description", "") or ""),
                    "error_code": str(getattr(tb_payload, "errorCode", "") or ""),
                    "symbol": tb_symbol_name,
                    "timeframe": tb_tf,
                })
                return
            raw_bars = list(getattr(tb_payload, "trendbar", []) or [])
            bars = []
            for bar in raw_bars:
                low_raw = _safe_int(getattr(bar, "low", 0), 0)
                delta_open = _safe_int(getattr(bar, "deltaOpen", 0), 0)
                delta_close = _safe_int(getattr(bar, "deltaClose", 0), 0)
                delta_high = _safe_int(getattr(bar, "deltaHigh", 0), 0)
                vol = _safe_int(getattr(bar, "volume", 0), 0)
                ts_min = _safe_int(getattr(bar, "utcTimestampInMinutes", 0), 0)
                low = low_raw / tb_scale
                bars.append({
                    "ts_ms": ts_min * 60 * 1000,
                    "ts_utc": _ms_to_iso(ts_min * 60 * 1000),
                    "open": (low_raw + delta_open) / tb_scale,
                    "high": (low_raw + delta_high) / tb_scale,
                    "low": low,
                    "close": (low_raw + delta_close) / tb_scale,
                    "volume": vol,
                })
            defer.returnValue({
                "ok": True,
                "status": "trendbars_loaded",
                "symbol": tb_symbol_name,
                "symbol_id": int(tb_symbol_id),
                "timeframe": tb_tf,
                "digits": int(tb_digits),
                "bar_count": len(bars),
                "bars": bars,
                "has_more": bool(getattr(tb_payload, "hasMore", False)),
                "token_refresh": dict(refreshed_meta or {}),
            })
            return

        if mode == "reconcile":
            reconcile_msg = yield client.send(pb.ProtoOAReconcileReq(ctidTraderAccountId=int(account_id)), responseTimeoutInSeconds=10)
            reconcile_payload = Protobuf.extract(reconcile_msg)
            lookback_h = max(1, _safe_int(payload.get("lookback_hours"), 72))
            now_ms = int(time.time() * 1000)
            from_ts = _safe_int(payload.get("from_timestamp"), now_ms - (lookback_h * 3600 * 1000))
            to_ts = _safe_int(payload.get("to_timestamp"), now_ms)
            max_rows = max(10, min(_safe_int(payload.get("max_rows"), 200), 1000))
            deal_msg = yield client.send(
                pb.ProtoOADealListReq(
                    ctidTraderAccountId=int(account_id),
                    fromTimestamp=int(from_ts),
                    toTimestamp=int(to_ts),
                    maxRows=int(max_rows),
                ),
                responseTimeoutInSeconds=15,
            )
            deal_payload = Protobuf.extract(deal_msg)
            positions = [_normalize_position(x, symbol_map) for x in list(getattr(reconcile_payload, "position", []) or [])]
            orders = [_proto_to_dict(x) for x in list(getattr(reconcile_payload, "order", []) or [])]
            deals = [_normalize_deal(x, symbol_map) for x in list(getattr(deal_payload, "deal", []) or [])]
            defer.returnValue({
                "ok": True,
                "status": "reconciled",
                "message": f"positions={len(positions)} deals={len(deals)}",
                "account_id": int(account_id),
                "environment": environment,
                "positions": positions,
                "orders": orders,
                "deals": deals,
                "token_refresh": dict(refreshed_meta or {}),
            })
            return

        light_symbol, match_reason = _resolve_symbol(list(getattr(symbols_payload, "symbol", []) or []), payload)
        if light_symbol is None:
            defer.returnValue({
                "ok": False,
                "status": "symbol_not_found",
                "message": f"symbol not found for {payload.get('symbol')}",
                "signal_symbol": str(payload.get("symbol", "") or ""),
                "account_id": int(account_id),
                "environment": environment,
                "symbol_match": match_reason,
            })
            return

        symbol_id = _safe_int(getattr(light_symbol, "symbolId", 0), 0)
        symbol_name = str(getattr(light_symbol, "symbolName", "") or "").strip()
        symbol_by_id_msg = yield client.send(
            pb.ProtoOASymbolByIdReq(ctidTraderAccountId=int(account_id), symbolId=[int(symbol_id)]),
            responseTimeoutInSeconds=12,
        )
        symbol_by_id_payload = Protobuf.extract(symbol_by_id_msg)
        full_symbols = list(getattr(symbol_by_id_payload, "symbol", []) or [])
        symbol_meta = full_symbols[0] if full_symbols else None
        if symbol_meta is None:
            defer.returnValue({
                "ok": False,
                "status": "symbol_meta_missing",
                "message": f"symbol metadata missing for {symbol_name}",
                "signal_symbol": str(payload.get("symbol", "") or ""),
                "broker_symbol": symbol_name,
                "account_id": int(account_id),
                "environment": environment,
            })
            return

        volume, volume_meta = _quantize_volume(symbol_meta, payload)
        direction = str(payload.get("direction", "") or "").strip().lower()
        order_type = str(payload.get("order_type", "market") or "market").strip().lower()
        side = model.ProtoOATradeSide.BUY if direction == "long" else model.ProtoOATradeSide.SELL
        if order_type == "limit":
            order_type_proto = model.ProtoOAOrderType.LIMIT
        elif order_type == "stop":
            order_type_proto = model.ProtoOAOrderType.STOP
        else:
            order_type_proto = model.ProtoOAOrderType.MARKET
        _lbl = str(payload.get("label", "") or "").strip()[:64]
        _cid_in = str(payload.get("client_order_id", "") or "").strip()[:64]
        # API rejects empty clientOrderId; callers often set label only (e.g. Mempalac worker).
        _client_oid = (_cid_in or _lbl or f"w_{uuid.uuid4().hex[:20]}")[:64]
        if not _lbl:
            _lbl = _client_oid[:64]
        req_kwargs = {
            "ctidTraderAccountId": int(account_id),
            "symbolId": int(symbol_id),
            "orderType": order_type_proto,
            "tradeSide": side,
            "volume": int(volume),
            "comment": str(payload.get("comment", "") or "")[:128],
            "label": _lbl,
            "clientOrderId": _client_oid,
            "timeInForce": model.ProtoOATimeInForce.GOOD_TILL_CANCEL,
        }
        price_digits = _price_digits(symbol_meta, trader)
        entry_price = _quantize_price(payload.get("entry"), price_digits)
        stop_loss_price = _quantize_price(payload.get("stop_loss"), price_digits)
        take_profit_price = _quantize_price(payload.get("take_profit"), price_digits)
        if order_type == "limit":
            req_kwargs["limitPrice"] = entry_price
            if stop_loss_price > 0:
                req_kwargs["stopLoss"] = stop_loss_price
            if take_profit_price > 0:
                req_kwargs["takeProfit"] = take_profit_price
        elif order_type == "stop":
            req_kwargs["stopPrice"] = entry_price
            if stop_loss_price > 0:
                req_kwargs["stopLoss"] = stop_loss_price
            if take_profit_price > 0:
                req_kwargs["takeProfit"] = take_profit_price
        else:
            rel_stop = _relative_distance(entry_price, stop_loss_price)
            rel_take = _relative_distance(entry_price, take_profit_price)
            if rel_stop > 0:
                req_kwargs["relativeStopLoss"] = rel_stop
            if rel_take > 0:
                req_kwargs["relativeTakeProfit"] = rel_take
        exec_msg = yield client.send(pb.ProtoOANewOrderReq(**req_kwargs), responseTimeoutInSeconds=15)
        exec_payload = Protobuf.extract(exec_msg)
        if isinstance(exec_payload, pb.ProtoOAOrderErrorEvent):
            defer.returnValue({
                "ok": False,
                "status": "rejected",
                "message": str(getattr(exec_payload, "description", "") or getattr(exec_payload, "errorCode", "order rejected")),
                "signal_symbol": str(payload.get("symbol", "") or ""),
                "broker_symbol": symbol_name,
                "account_id": int(account_id),
                "volume": float(volume),
                "execution_meta": {
                    "error_code": str(getattr(exec_payload, "errorCode", "") or ""),
                    "symbol_match": match_reason,
                    "price_digits": int(price_digits),
                    "volume_meta": volume_meta,
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return
        if _is_error(exec_payload):
            defer.returnValue({
                "ok": False,
                "status": "rejected",
                "message": str(getattr(exec_payload, "description", "") or getattr(exec_payload, "errorCode", "order rejected")),
                "signal_symbol": str(payload.get("symbol", "") or ""),
                "broker_symbol": symbol_name,
                "account_id": int(account_id),
                "volume": float(volume),
                "execution_meta": {
                    "symbol_match": match_reason,
                    "price_digits": int(price_digits),
                    "volume_meta": volume_meta,
                    "token_refresh": dict(refreshed_meta or {}),
                },
            })
            return

        execution_type = _enum_name(model.ProtoOAExecutionType, getattr(exec_payload, "executionType", 0))
        order = getattr(exec_payload, "order", None)
        position = getattr(exec_payload, "position", None)
        deal = getattr(exec_payload, "deal", None)
        ok = execution_type in {"ORDER_ACCEPTED", "ORDER_FILLED", "ORDER_PARTIAL_FILL"}
        status = "filled" if execution_type in {"ORDER_FILLED", "ORDER_PARTIAL_FILL"} else ("accepted" if ok else "rejected")
        defer.returnValue({
            "ok": bool(ok),
            "status": status,
            "message": f"ctrader {execution_type.lower()}",
            "signal_symbol": str(payload.get("symbol", "") or ""),
            "broker_symbol": symbol_name,
            "account_id": int(account_id),
            "order_id": _safe_int(getattr(order, "orderId", 0), 0) or None,
            "position_id": _safe_int(getattr(position, "positionId", 0), 0) or None,
            "deal_id": _safe_int(getattr(deal, "dealId", 0), 0) or None,
            "volume": float(volume),
            "execution_meta": {
                "environment": environment,
                "execution_type": execution_type,
                "symbol_id": int(symbol_id),
                "symbol_match": match_reason,
                "symbol_name": symbol_name,
                "price_digits": int(price_digits),
                "symbol_meta": _proto_to_dict(symbol_meta),
                "volume_meta": volume_meta,
                "token_refresh": dict(refreshed_meta or {}),
                "raw_execution": _proto_to_dict(exec_payload),
            },
        })
    except Exception as e:
        logger.warning("cTrader worker failed", exc_info=True)
        defer.returnValue({
            "ok": False,
            "status": "error",
            "message": str(e),
            "account_id": int(account_id) if account_id > 0 else None,
            "environment": environment,
        })
    finally:
        if client is not None:
            try:
                client.stopService()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["health", "execute", "accounts", "reconcile", "close", "cancel_order", "amend_order", "amend_position_sltp", "capture_market", "get_trendbars"], required=True)
    parser.add_argument("--payload-file", default="")
    args = parser.parse_args()
    payload = _load_payload(str(args.payload_file or ""))
    result_box: dict = {}

    def _finish(result):
        try:
            if isinstance(result, Failure):
                result_box["data"] = {
                    "ok": False,
                    "status": "worker_failure",
                    "message": result.getErrorMessage(),
                    "failure_type": str(getattr(result.type, "__name__", "") or result.type),
                    "traceback": result.getTraceback(),
                }
            elif isinstance(result, dict):
                result_box["data"] = dict(result)
            else:
                result_box["data"] = {
                    "ok": False,
                    "status": "worker_invalid_result",
                    "message": f"unexpected worker result type: {type(result).__name__}",
                }
        finally:
            if reactor.running:
                reactor.stop()
        return result

    d = _workflow(str(args.mode or "health"), payload)
    d.addBoth(_finish)
    reactor.run()
    out = dict(result_box.get("data") or {})
    if "execution_meta" in out and isinstance(out.get("execution_meta"), dict):
        out["execution_meta"] = _scrub_tokens(out.get("execution_meta", {}))
    print(json.dumps(out, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
