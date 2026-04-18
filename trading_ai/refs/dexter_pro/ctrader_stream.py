"""
execution/ctrader_stream.py

Persistent cTrader OpenAPI streaming service.

Stays connected to cTrader, subscribes to:
  - Live trendbars (M1, M5, H1) via ProtoOASubscribeLiveTrendbarReq
  - Margin events (ProtoOAMarginCallTriggerEvent, ProtoOAMarginChangedEvent)
  - Execution events (ProtoOAExecutionEvent) for audit trail

Writes all data to SQLite (WAL mode) so scheduler/dashboard can read without
any Twisted dependency.

Usage:
    python -m execution.ctrader_stream          # foreground
    python -m execution.ctrader_stream --daemon  # background (no console)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from config import config  # noqa: E402
from market.tick_bar_engine import TickBarEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("ctrader_stream")

# ── Constants ────────────────────────────────────────────────────────────────

_DB_PATH = str(getattr(config, "CTRADER_DB_PATH", "") or (BASE / "data" / "ctrader_openapi.db"))

_TF_SUBSCRIBE = ["M1", "M5", "H1"]  # timeframes to stream

_TF_ENUM = {
    "M1": 1, "M2": 2, "M3": 3, "M4": 4, "M5": 5,
    "M10": 6, "M15": 7, "M30": 8,
    "H1": 9, "H4": 10, "H12": 11,
    "D1": 12, "W1": 13, "MN1": 14,
}

_RECONNECT_BASE_SEC = 5
_RECONNECT_MAX_SEC = 300
_HEARTBEAT_SEC = 25
_MARGIN_POLL_SEC = 30
_STATUS_UPDATE_SEC = 10

# Symbols to subscribe — resolved from broker at connect time
_SUBSCRIBE_SYMBOLS = ["XAUUSD"]

# ── Lazy imports (Twisted + protobuf) ────────────────────────────────────────

try:
    from google.protobuf.json_format import MessageToDict
    from twisted.internet import defer, reactor, task
    from twisted.python.failure import Failure
    from ctrader_open_api import Auth, Client, EndPoints, Protobuf, TcpProtocol
    from ctrader_open_api.messages import OpenApiMessages_pb2 as pb
    from ctrader_open_api.messages import OpenApiModelMessages_pb2 as model
    _HAS_DEPS = True
except ImportError as e:
    logger.error("Missing dependencies: %s", e)
    _HAS_DEPS = False


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


def _ms_to_iso(ms: int) -> str:
    if ms <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _proto_to_dict(message) -> dict:
    try:
        return MessageToDict(message, preserving_proto_field_name=True)
    except Exception:
        return {}


# ── SQLite Schema ────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stream_trendbars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    symbol_id INTEGER NOT NULL,
    tf TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    ts_utc TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(symbol, tf, ts_ms)
);

CREATE TABLE IF NOT EXISTS stream_margin (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    position_id INTEGER,
    used_margin REAL,
    margin_level_threshold REAL,
    margin_call_type TEXT,
    money_digits INTEGER DEFAULT 2,
    details_json TEXT,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS stream_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    execution_type TEXT NOT NULL,
    position_id INTEGER,
    order_id INTEGER,
    deal_id INTEGER,
    symbol TEXT,
    direction TEXT,
    volume INTEGER,
    is_server_event INTEGER DEFAULT 0,
    details_json TEXT,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS stream_status (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    connected INTEGER NOT NULL DEFAULT 0,
    last_heartbeat TEXT,
    last_trendbar TEXT,
    last_margin_check TEXT,
    subscribed_symbols TEXT,
    subscribed_tfs TEXT,
    uptime_sec REAL DEFAULT 0,
    reconnect_count INTEGER DEFAULT 0,
    total_bars_received INTEGER DEFAULT 0,
    total_margin_events INTEGER DEFAULT 0,
    total_execution_events INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO stream_status (id, connected) VALUES (1, 0);

CREATE INDEX IF NOT EXISTS idx_stream_trendbars_lookup
    ON stream_trendbars(symbol, tf, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_stream_margin_received
    ON stream_margin(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_stream_executions_received
    ON stream_executions(received_at DESC);
"""


class StreamDB:
    """Thread-safe SQLite writer for streaming data."""

    def __init__(self, db_path: str):
        self._path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._path, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()
        logger.info("StreamDB connected: %s", self._path)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def upsert_trendbar(self, symbol: str, symbol_id: int, tf: str,
                        ts_ms: int, o: float, h: float, l: float,
                        c: float, volume: int) -> None:
        if not self._conn:
            return
        try:
            self._conn.execute(
                "INSERT OR REPLACE INTO stream_trendbars "
                "(symbol, symbol_id, tf, ts_ms, ts_utc, open, high, low, close, volume, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (symbol, symbol_id, tf, ts_ms, _ms_to_iso(ts_ms),
                 o, h, l, c, volume,
                 datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            )
            self._conn.commit()
        except Exception as e:
            logger.warning("Failed to upsert trendbar: %s", e)

    def insert_margin_event(self, account_id: int, event_type: str, **kwargs) -> None:
        if not self._conn:
            return
        try:
            self._conn.execute(
                "INSERT INTO stream_margin "
                "(account_id, event_type, position_id, used_margin, "
                "margin_level_threshold, margin_call_type, money_digits, details_json, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    account_id, event_type,
                    kwargs.get("position_id"),
                    kwargs.get("used_margin"),
                    kwargs.get("margin_level_threshold"),
                    kwargs.get("margin_call_type"),
                    kwargs.get("money_digits", 2),
                    json.dumps(kwargs.get("details") or {}, default=str),
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            self._conn.commit()
        except Exception as e:
            logger.warning("Failed to insert margin event: %s", e)

    def insert_execution_event(self, account_id: int, execution_type: str,
                               **kwargs) -> None:
        if not self._conn:
            return
        try:
            self._conn.execute(
                "INSERT INTO stream_executions "
                "(account_id, execution_type, position_id, order_id, deal_id, "
                "symbol, direction, volume, is_server_event, details_json, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    account_id, execution_type,
                    kwargs.get("position_id"),
                    kwargs.get("order_id"),
                    kwargs.get("deal_id"),
                    kwargs.get("symbol"),
                    kwargs.get("direction"),
                    kwargs.get("volume"),
                    int(kwargs.get("is_server_event", False)),
                    json.dumps(kwargs.get("details") or {}, default=str),
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            self._conn.commit()
        except Exception as e:
            logger.warning("Failed to insert execution event: %s", e)

    def update_status(self, **kwargs) -> None:
        if not self._conn:
            return
        try:
            sets = []
            vals = []
            for k, v in kwargs.items():
                sets.append(f"{k} = ?")
                vals.append(v)
            sets.append("updated_at = ?")
            vals.append(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
            self._conn.execute(
                f"UPDATE stream_status SET {', '.join(sets)} WHERE id = 1",
                vals,
            )
            self._conn.commit()
        except Exception as e:
            logger.warning("Failed to update status: %s", e)


# ── Streaming Service ────────────────────────────────────────────────────────

class CTraderStreamService:
    """Long-lived Twisted service that streams cTrader data to SQLite."""

    def __init__(self):
        self.db = StreamDB(_DB_PATH)
        self.client: Optional[Client] = None
        self._account_id: int = 0
        self._access_token: str = ""
        self._symbol_map: dict[int, str] = {}  # symbol_id → name
        self._symbol_ids: list[int] = []  # subscribed symbol IDs
        self._digits_map: dict[int, int] = {}  # symbol_id → digits
        self._started_at: float = 0.0
        self._reconnect_count: int = 0
        self._reconnect_delay: float = _RECONNECT_BASE_SEC
        self._total_bars: int = 0
        self._total_margin_events: int = 0
        self._total_execution_events: int = 0
        self._heartbeat_loop: Optional[task.LoopingCall] = None
        self._status_loop: Optional[task.LoopingCall] = None
        self._margin_loop: Optional[task.LoopingCall] = None
        self._running: bool = False

        # Gate 1: Tick engines initialized internally
        self._tick_engines: dict[str, list[TickBarEngine]] = {}

    # ── Connection lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """Entry point — connect and begin streaming."""
        if not _HAS_DEPS:
            logger.error("Cannot start: missing Twisted/protobuf dependencies")
            return
        self._running = True
        self._started_at = time.time()
        self.db.connect()
        self.db.update_status(
            connected=0,
            started_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            error_message="",
        )
        self._connect()
        reactor.run()

    def _connect(self) -> None:
        """Initiate TCP connection to cTrader."""
        if not self._running:
            return
        use_demo = bool(getattr(config, "CTRADER_USE_DEMO", False))
        if use_demo:
            host, port = EndPoints.PROTOBUF_DEMO_HOST, int(EndPoints.PROTOBUF_PORT)
        else:
            host, port = EndPoints.PROTOBUF_LIVE_HOST, int(EndPoints.PROTOBUF_PORT)
        env = "demo" if use_demo else "live"
        logger.info("Connecting to cTrader %s (%s:%d)...", env, host, port)

        self.client = Client(host, port, TcpProtocol)
        self.client.startService()

        d = self.client.whenConnected(failAfterFailures=1)
        d.addCallback(self._on_connected)
        d.addErrback(self._on_connect_error)

    @defer.inlineCallbacks
    def _on_connected(self, _result) -> None:
        """TCP connected — authenticate and subscribe."""
        logger.info("TCP connected, authenticating...")
        self._reconnect_delay = _RECONNECT_BASE_SEC  # reset backoff
        self.db.update_status(error_message="authenticating...")

        client_id = str(getattr(config, "CTRADER_OPENAPI_CLIENT_ID", "") or "").strip()
        client_secret = str(getattr(config, "CTRADER_OPENAPI_CLIENT_SECRET", "") or "").strip()
        self._access_token = str(getattr(config, "CTRADER_OPENAPI_ACCESS_TOKEN", "") or "").strip()
        refresh_token = str(getattr(config, "CTRADER_OPENAPI_REFRESH_TOKEN", "") or "").strip()

        if not client_id or not client_secret:
            logger.error("Missing CTRADER_OPENAPI_CLIENT_ID/CLIENT_SECRET")
            self.db.update_status(connected=0, error_message="credentials missing")
            self._schedule_reconnect()
            return

        # App auth
        try:
            app_msg = yield self.client.send(
                pb.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
                responseTimeoutInSeconds=10,
            )
            app_payload = Protobuf.extract(app_msg)
            if isinstance(app_payload, pb.ProtoOAErrorRes):
                err = str(getattr(app_payload, "description", "") or getattr(app_payload, "errorCode", ""))
                logger.error("App auth failed: %s", err)
                self.db.update_status(connected=0, error_message=f"app auth failed: {err}")
                self._schedule_reconnect()
                return
        except Exception as e:
            logger.error("App auth exception: %s", e)
            self.db.update_status(connected=0, error_message=f"app auth error: {e}")
            self._schedule_reconnect()
            return

        # Token refresh if needed
        if not self._access_token and refresh_token:
            self._access_token = self._try_refresh(refresh_token, client_id, client_secret)
        if not self._access_token:
            logger.error("No access token available")
            self.db.update_status(connected=0, error_message="no access token")
            self._schedule_reconnect()
            return

        # Account auth
        self._account_id = self._resolve_account_id()
        if self._account_id <= 0:
            logger.error("No account ID configured")
            self.db.update_status(connected=0, error_message="no account ID")
            self._schedule_reconnect()
            return

        try:
            acc_msg = yield self.client.send(
                pb.ProtoOAAccountAuthReq(
                    ctidTraderAccountId=int(self._account_id),
                    accessToken=self._access_token,
                ),
                responseTimeoutInSeconds=10,
            )
            acc_payload = Protobuf.extract(acc_msg)
            if isinstance(acc_payload, pb.ProtoOAErrorRes):
                err_code = str(getattr(acc_payload, "errorCode", "") or "")
                # Try refresh on invalid token
                if err_code == "CH_ACCESS_TOKEN_INVALID" and refresh_token:
                    self._access_token = self._try_refresh(refresh_token, client_id, client_secret)
                    if self._access_token:
                        acc_msg = yield self.client.send(
                            pb.ProtoOAAccountAuthReq(
                                ctidTraderAccountId=int(self._account_id),
                                accessToken=self._access_token,
                            ),
                            responseTimeoutInSeconds=10,
                        )
                        acc_payload = Protobuf.extract(acc_msg)
                if isinstance(acc_payload, pb.ProtoOAErrorRes):
                    err = str(getattr(acc_payload, "description", "") or "")
                    logger.error("Account auth failed: %s", err)
                    self.db.update_status(connected=0, error_message=f"account auth failed: {err}")
                    self._schedule_reconnect()
                    return
        except Exception as e:
            logger.error("Account auth exception: %s", e)
            self.db.update_status(connected=0, error_message=f"account auth error: {e}")
            self._schedule_reconnect()
            return

        logger.info("Authenticated account %d", self._account_id)

        # Resolve symbols
        yield self._resolve_symbols()

        # Register message callback for push events
        self.client.setMessageReceivedCallback(self._on_message)

        # Subscribe to live trendbars
        yield self._subscribe_trendbars()

        # Gate 0: Depth Subscription Smoke Test
        try:
            yield self.client.send(
                pb.ProtoOASubscribeDepthQuotesReq(
                    ctidTraderAccountId=int(self._account_id),
                    symbolId=[int(s) for s in self._symbol_ids],
                ),
                responseTimeoutInSeconds=5,
            )
            logger.info("Subscribed to Depth Quotes for multi-level updates.")
        except Exception as e:
            logger.warning("Failed to subscribe depth quotes: %s", e)

        # Initialize TickBarEngines for all subscribed symbols
        for sid in self._symbol_ids:
            sym_name = self._symbol_map.get(sid, "")
            if sym_name:
                self._tick_engines[sym_name] = [
                    TickBarEngine(sym_name, 60, "time"),     # M1
                    TickBarEngine(sym_name, 100, "tick")     # 100-Tick
                ]

        # Start periodic loops
        self._start_loops()

        self.db.update_status(
            connected=1,
            error_message="",
            subscribed_symbols=",".join(self._symbol_map.get(sid, "") for sid in self._symbol_ids),
            subscribed_tfs=",".join(_TF_SUBSCRIBE),
            reconnect_count=self._reconnect_count,
        )
        logger.info("Streaming active — symbols=%s, TFs=%s",
                     [self._symbol_map.get(s, s) for s in self._symbol_ids],
                     _TF_SUBSCRIBE)

    def _on_connect_error(self, failure: Failure) -> None:
        """TCP connection failed — schedule reconnect."""
        err = failure.getErrorMessage() if failure else "unknown"
        logger.warning("Connection failed: %s", err)
        self.db.update_status(connected=0, error_message=f"connection failed: {err}")
        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        """Exponential backoff reconnect."""
        if not self._running:
            return
        self._stop_loops()
        if self.client:
            try:
                self.client.stopService()
            except Exception:
                pass
            self.client = None
        self._reconnect_count += 1
        delay = min(self._reconnect_delay, _RECONNECT_MAX_SEC)
        self._reconnect_delay = min(self._reconnect_delay * 2, _RECONNECT_MAX_SEC)
        logger.info("Reconnecting in %.0fs (attempt %d)...", delay, self._reconnect_count)
        self.db.update_status(
            connected=0,
            reconnect_count=self._reconnect_count,
            error_message=f"reconnecting in {delay:.0f}s",
        )
        reactor.callLater(delay, self._connect)

    def stop(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down stream service...")
        self._running = False
        self._stop_loops()
        self.db.update_status(connected=0, error_message="stopped")
        if self.client:
            try:
                self.client.stopService()
            except Exception:
                pass
        self.db.close()
        if reactor.running:
            reactor.stop()

    # ── Symbol resolution ────────────────────────────────────────────────

    @defer.inlineCallbacks
    def _resolve_symbols(self) -> None:
        """Get light symbol list and resolve IDs for subscription."""
        try:
            sym_msg = yield self.client.send(
                pb.ProtoOASymbolsListReq(ctidTraderAccountId=int(self._account_id)),
                responseTimeoutInSeconds=10,
            )
            sym_payload = Protobuf.extract(sym_msg)
            light_symbols = list(getattr(sym_payload, "symbol", []) or [])
        except Exception as e:
            logger.warning("Failed to load symbol list: %s", e)
            light_symbols = []

        self._symbol_map = {}
        self._symbol_ids = []
        self._digits_map = {}

        targets = [s.upper() for s in _SUBSCRIBE_SYMBOLS]
        for sym in light_symbols:
            name = str(getattr(sym, "symbolName", "") or "").strip().upper()
            sid = _safe_int(getattr(sym, "symbolId", 0), 0)
            if sid > 0 and name:
                norm = re.sub(r"[^A-Z0-9]", "", name)
                for target in targets:
                    if norm == re.sub(r"[^A-Z0-9]", "", target):
                        self._symbol_map[sid] = name
                        self._symbol_ids.append(sid)
                        break

        # Resolve digits per symbol
        if self._symbol_ids:
            try:
                meta_msg = yield self.client.send(
                    pb.ProtoOASymbolByIdReq(
                        ctidTraderAccountId=int(self._account_id),
                        symbolId=[int(s) for s in self._symbol_ids],
                    ),
                    responseTimeoutInSeconds=8,
                )
                meta_payload = Protobuf.extract(meta_msg)
                for sym_meta in list(getattr(meta_payload, "symbol", []) or []):
                    sid = _safe_int(getattr(sym_meta, "symbolId", 0), 0)
                    digits = _safe_int(getattr(sym_meta, "digits", 5), 5)
                    if sid > 0:
                        self._digits_map[sid] = digits
            except Exception as e:
                logger.warning("Failed to resolve symbol digits: %s", e)

        logger.info("Resolved %d symbols: %s", len(self._symbol_ids),
                     {sid: self._symbol_map.get(sid, "?") for sid in self._symbol_ids})

    # ── Subscriptions ────────────────────────────────────────────────────

    @defer.inlineCallbacks
    def _subscribe_trendbars(self) -> None:
        """Subscribe to live trendbars for all symbols × timeframes."""
        for sid in self._symbol_ids:
            name = self._symbol_map.get(sid, "?")
            for tf_name in _TF_SUBSCRIBE:
                period = _TF_ENUM.get(tf_name)
                if period is None:
                    continue
                try:
                    yield self.client.send(
                        pb.ProtoOASubscribeLiveTrendbarReq(
                            ctidTraderAccountId=int(self._account_id),
                            period=int(period),
                            symbolId=int(sid),
                        ),
                        responseTimeoutInSeconds=5,
                    )
                    logger.info("Subscribed: %s %s", name, tf_name)
                except Exception as e:
                    logger.warning("Failed to subscribe %s %s: %s", name, tf_name, e)

        # Also subscribe to spot prices (gives trendbar data in spot events)
        if self._symbol_ids:
            try:
                protocol = self.client.getProtocol()
                if protocol:
                    protocol.send(
                        pb.ProtoOASubscribeSpotsReq(
                            ctidTraderAccountId=int(self._account_id),
                            symbolId=[int(s) for s in self._symbol_ids],
                            subscribeToSpotTimestamp=True,
                        ),
                        instant=True,
                    )
                    logger.info("Subscribed to spot prices")
            except Exception as e:
                logger.warning("Failed to subscribe spots: %s", e)

    # ── Message handling ─────────────────────────────────────────────────

    def _on_message(self, _client, message) -> None:
        """Callback for all push messages from cTrader."""
        self._last_msg_at = time.time()
        try:
            payload = Protobuf.extract(message)
        except Exception:
            return

        if isinstance(payload, pb.ProtoOASpotEvent):
            self._handle_spot_event(payload)
        elif isinstance(payload, pb.ProtoOAExecutionEvent):
            self._handle_execution_event(payload)
        elif isinstance(payload, pb.ProtoOAMarginCallTriggerEvent):
            self._handle_margin_call_trigger(payload)
        elif isinstance(payload, pb.ProtoOAMarginChangedEvent):
            self._handle_margin_changed(payload)
        elif isinstance(payload, pb.ProtoOAMarginCallUpdateEvent):
            self._handle_margin_call_update(payload)
        elif isinstance(payload, pb.ProtoOADepthEvent):
            self._handle_depth_event(payload)

    def _handle_depth_event(self, evt) -> None:
        """Smoke test handler to log L2 multi-level updates."""
        sid = _safe_int(getattr(evt, "symbolId", 0), 0)
        symbol = self._symbol_map.get(sid, "")

        bids = getattr(evt, "newBids", [])
        asks = getattr(evt, "newAsks", [])

        # Minimal footprint log every N depth updates to prove L2 is incoming
        if hasattr(self, "_depth_count"):
            self._depth_count += 1
        else:
            self._depth_count = 1

        if self._depth_count % 500 == 1:
            logger.info("DepthEvent Smoke Test: %s — %d bid levels, %d ask levels", symbol, len(bids), len(asks))

    def _handle_spot_event(self, evt) -> None:
        """Process SpotEvent — extract live trendbar data if present."""
        sid = _safe_int(getattr(evt, "symbolId", 0), 0)
        symbol = self._symbol_map.get(sid, "")
        digits = self._digits_map.get(sid, 5)
        scale = float(10 ** digits)

        # SpotEvent.trendbar is repeated ProtoOATrendbar
        trendbars = list(getattr(evt, "trendbar", []) or [])
        for bar in trendbars:
            low_raw = _safe_int(getattr(bar, "low", 0), 0)
            delta_open = _safe_int(getattr(bar, "deltaOpen", 0), 0)
            delta_close = _safe_int(getattr(bar, "deltaClose", 0), 0)
            delta_high = _safe_int(getattr(bar, "deltaHigh", 0), 0)
            volume = _safe_int(getattr(bar, "volume", 0), 0)
            ts_min = _safe_int(getattr(bar, "utcTimestampInMinutes", 0), 0)
            period = _safe_int(getattr(bar, "period", 0), 0)

            # Map period enum back to TF name
            tf_name = ""
            for name, val in _TF_ENUM.items():
                if val == period:
                    tf_name = name
                    break
            if not tf_name or tf_name not in _TF_SUBSCRIBE:
                continue

            ts_ms = ts_min * 60 * 1000
            o = (low_raw + delta_open) / scale
            h = (low_raw + delta_high) / scale
            l = low_raw / scale
            c = (low_raw + delta_close) / scale

            self.db.upsert_trendbar(symbol, sid, tf_name, ts_ms, o, h, l, c, volume)
            self._total_bars += 1

            if self._total_bars % 50 == 1:
                logger.info("Trendbar: %s %s O=%.2f H=%.2f L=%.2f C=%.2f V=%d (total=%d)",
                            symbol, tf_name, o, h, l, c, volume, self._total_bars)

        # Feed to TickBarEngine silently (no callback logging or DB write as requested)
        bid = _safe_float(getattr(evt, "bid", 0)) / scale
        ask = _safe_float(getattr(evt, "ask", 0)) / scale
        ts_ms = _safe_int(getattr(evt, "timestamp", 0))

        if bid > 0 and ask > 0 and ts_ms > 0 and symbol in self._tick_engines:
            for engine in self._tick_engines[symbol]:
                # on_quote returns a completed bar dict if the period closes, else None
                completed = engine.on_quote(bid, ask, ts_ms)
                # The engine stores it in internally, we prove it runs without crashing.

    def _handle_execution_event(self, evt) -> None:
        """Process execution event — log to stream_executions."""
        try:
            exec_type_val = _safe_int(getattr(evt, "executionType", 0), 0)
            exec_type = str(model.ProtoOAExecutionType.Name(exec_type_val))
        except Exception:
            exec_type = str(exec_type_val)

        position = getattr(evt, "position", None)
        order = getattr(evt, "order", None)
        deal = getattr(evt, "deal", None)
        trade_data = getattr(position, "tradeData", None) if position else None

        symbol_id = _safe_int(getattr(trade_data, "symbolId", 0), 0) if trade_data else 0
        symbol = self._symbol_map.get(symbol_id, "")
        side = str(getattr(trade_data, "tradeSide", "") or "") if trade_data else ""
        direction = "long" if side == "BUY" else ("short" if side == "SELL" else "")

        self.db.insert_execution_event(
            account_id=self._account_id,
            execution_type=exec_type,
            position_id=_safe_int(getattr(position, "positionId", 0), 0) if position else None,
            order_id=_safe_int(getattr(order, "orderId", 0), 0) if order else None,
            deal_id=_safe_int(getattr(deal, "dealId", 0), 0) if deal else None,
            symbol=symbol,
            direction=direction,
            volume=_safe_int(getattr(trade_data, "volume", 0), 0) if trade_data else None,
            is_server_event=bool(getattr(evt, "isServerEvent", False)),
            details=_proto_to_dict(evt),
        )
        self._total_execution_events += 1
        logger.info("Execution: %s %s %s pos=%s",
                     exec_type, symbol, direction,
                     getattr(position, "positionId", "?") if position else "?")

    def _handle_margin_call_trigger(self, evt) -> None:
        """Margin call triggered — critical alert."""
        mc = getattr(evt, "marginCall", None)
        mc_type_val = _safe_int(getattr(mc, "marginCallType", 0), 0) if mc else 0
        try:
            mc_type = str(model.ProtoOANotificationType.Name(mc_type_val))
        except Exception:
            mc_type = str(mc_type_val)

        threshold = _safe_float(getattr(mc, "marginLevelThreshold", 0), 0)

        self.db.insert_margin_event(
            account_id=self._account_id,
            event_type="margin_call_trigger",
            margin_level_threshold=threshold,
            margin_call_type=mc_type,
            details=_proto_to_dict(evt),
        )
        self._total_margin_events += 1
        logger.critical("MARGIN CALL TRIGGER: type=%s threshold=%.2f%%", mc_type, threshold)

    def _handle_margin_changed(self, evt) -> None:
        """Margin changed on a position."""
        pos_id = _safe_int(getattr(evt, "positionId", 0), 0)
        used_margin = _safe_int(getattr(evt, "usedMargin", 0), 0)
        money_digits = _safe_int(getattr(evt, "moneyDigits", 2), 2)
        scale = float(10 ** money_digits)

        self.db.insert_margin_event(
            account_id=self._account_id,
            event_type="margin_changed",
            position_id=pos_id,
            used_margin=used_margin / scale if scale > 0 else 0,
            money_digits=money_digits,
            details=_proto_to_dict(evt),
        )
        self._total_margin_events += 1

    def _handle_margin_call_update(self, evt) -> None:
        """Margin call levels updated."""
        mc = getattr(evt, "marginCall", None)
        mc_type_val = _safe_int(getattr(mc, "marginCallType", 0), 0) if mc else 0
        try:
            mc_type = str(model.ProtoOANotificationType.Name(mc_type_val))
        except Exception:
            mc_type = str(mc_type_val)

        threshold = _safe_float(getattr(mc, "marginLevelThreshold", 0), 0)

        self.db.insert_margin_event(
            account_id=self._account_id,
            event_type="margin_call_update",
            margin_level_threshold=threshold,
            margin_call_type=mc_type,
            details=_proto_to_dict(evt),
        )
        self._total_margin_events += 1
        logger.warning("Margin call update: type=%s threshold=%.2f%%", mc_type, threshold)

    # ── Periodic tasks ───────────────────────────────────────────────────

    def _start_loops(self) -> None:
        """Start heartbeat, status updates, and margin polling."""
        self._heartbeat_loop = task.LoopingCall(self._send_heartbeat)
        self._heartbeat_loop.start(_HEARTBEAT_SEC, now=False)

        self._status_loop = task.LoopingCall(self._update_status)
        self._status_loop.start(_STATUS_UPDATE_SEC, now=False)

        self._margin_loop = task.LoopingCall(self._poll_margin)
        self._margin_loop.start(_MARGIN_POLL_SEC, now=False)

    def _stop_loops(self) -> None:
        """Stop all periodic loops."""
        for loop in (self._heartbeat_loop, self._status_loop, self._margin_loop):
            if loop and loop.running:
                try:
                    loop.stop()
                except Exception:
                    pass
        self._heartbeat_loop = None
        self._status_loop = None
        self._margin_loop = None

    def _send_heartbeat(self) -> None:
        """Send heartbeat to keep connection alive."""
        if not self.client:
            return

        now = time.time()
        # Check if we are receiving any ticks, margin updates, or cTrader heartbeats.
        # cTrader sends frequent heartbeats and spot events. If 45s pass completely silently,
        # the TCP stream is dead/half-open and must be severed.
        last_msg = getattr(self, "_last_msg_at", now)
        if now - last_msg > 45.0:
            logger.error("CRITICAL: Stream has been completely silent for 45s. Socket is dead. Force reconnect!")
            self._schedule_reconnect()
            return

        try:
            protocol = self.client.getProtocol()
            if protocol:
                protocol.send(pb.ProtoHeartbeatEvent(), instant=True)
                self.db.update_status(
                    last_heartbeat=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
        except Exception as e:
            logger.warning("Heartbeat failed: %s — reconnecting", e)
            self._schedule_reconnect()

    def _update_status(self) -> None:
        """Periodically update status row."""
        uptime = time.time() - self._started_at if self._started_at > 0 else 0
        self.db.update_status(
            uptime_sec=round(uptime, 1),
            total_bars_received=self._total_bars,
            total_margin_events=self._total_margin_events,
            total_execution_events=self._total_execution_events,
        )

    @defer.inlineCallbacks
    def _poll_margin(self) -> None:
        """Poll unrealized PnL for margin safety monitoring."""
        if not self.client or self._account_id <= 0:
            return
        try:
            pnl_msg = yield self.client.send(
                pb.ProtoOAGetPositionUnrealizedPnLReq(
                    ctidTraderAccountId=int(self._account_id),
                ),
                responseTimeoutInSeconds=5,
            )
            pnl_payload = Protobuf.extract(pnl_msg)
            if isinstance(pnl_payload, pb.ProtoOAErrorRes):
                return

            positions = list(getattr(pnl_payload, "positionUnrealizedPnL", []) or [])
            money_digits = _safe_int(getattr(pnl_payload, "moneyDigits", 2), 2)
            scale = float(10 ** money_digits)

            if positions:
                total_unrealized = sum(
                    _safe_float(getattr(p, "netUnrealizedPnL", 0), 0)
                    for p in positions
                )
                self.db.update_status(
                    last_margin_check=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
                # Log if significant unrealized loss
                total_usd = total_unrealized / scale if scale > 0 else 0
                if total_usd < -10:
                    logger.warning("Unrealized PnL: $%.2f across %d positions", total_usd, len(positions))
        except Exception as e:
            logger.debug("Margin poll error: %s", e)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _resolve_account_id(self) -> int:
        """Resolve account ID from config — same logic as ctrader_execute_once."""
        raw = str(getattr(config, "CTRADER_ACCOUNT_ID", "") or "").strip()
        if raw:
            val = _safe_int(raw, 0)
            if val > 0:
                return val
        raw_login = str(getattr(config, "CTRADER_ACCOUNT_LOGIN", "") or "").strip()
        if raw_login:
            finder = getattr(config, "find_ctrader_account", None)
            if callable(finder):
                row = finder(raw_login, use_demo=getattr(config, "CTRADER_USE_DEMO", False))
                if isinstance(row, dict):
                    val = _safe_int(row.get("accountId"), 0)
                    if val > 0:
                        return val
        return 0

    def _try_refresh(self, refresh_token: str, client_id: str, client_secret: str) -> str:
        """Try to refresh the access token."""
        redirect_uri = str(getattr(config, "CTRADER_OPENAPI_REDIRECT_URI", "http://localhost") or "http://localhost").strip()
        try:
            auth = Auth(client_id, client_secret, redirect_uri)
            refreshed = auth.refreshToken(refresh_token)
            if isinstance(refreshed, dict):
                new_token = str(refreshed.get("accessToken") or "").strip()
                if new_token:
                    logger.info("Access token refreshed successfully")
                    return new_token
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
        return ""


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="cTrader persistent streaming service")
    parser.add_argument("--daemon", action="store_true", help="Run in background mode")
    args = parser.parse_args()

    service = CTraderStreamService()

    # Register graceful shutdown
    reactor.addSystemEventTrigger("before", "shutdown", service.stop)

    logger.info("Starting cTrader stream service...")
    service.start()


if __name__ == "__main__":
    main()
