from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_ai.core.execution import (
    Action,
    Broker,
    CloseResult,
    MarketSnapshot,
    OpenPosition,
    TradeResult,
)
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class CTraderConfig:
    """Mirrors Dexter OpenAPI env surface; wire tokens into Twisted worker per `refs/dexter_pro/`."""

    client_id: str
    client_secret: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    redirect_uri: str = "http://localhost"
    demo: bool = True
    account_id: Optional[int] = None
    account_login: Optional[str] = None
    host: str = "openapi.ctrader.com"
    port: int = 5035


class CTraderProtoClient:
    """
    Thin async skeleton for cTrader Open API (TCP/protobuf).

    Production wiring:
    - Replace _connect with real Twisted/protobuf handshake per Spotware docs.
    - Map symbolId ↔ symbol name via ProtoOASymbolsListRes.
    - Subscribe via ProtoOASubscribeSpotsReq; execute via ProtoOANewOrderReq.

    This class centralizes retry policy and connection lifecycle so integration
    is a swap of _send_application_message implementations.
    """

    def __init__(self, cfg: CTraderConfig) -> None:
        self.cfg = cfg
        self._connected = False
        self._subscriptions: Dict[str, Any] = {}

    async def connect(self) -> None:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=30),
            retry=retry_if_exception_type(ConnectionError),
            reraise=True,
        ):
            with attempt:
                await self._connect_with_timeout()

    async def _connect_with_timeout(self) -> None:
        # Placeholder: real impl opens SSL socket + sends ApplicationAuthReq
        await asyncio.sleep(0.01)
        if not self.cfg.client_id or not self.cfg.client_secret:
            log.warning("cTrader credentials incomplete — staying in stub mode")
            self._connected = False
            return
        self._connected = True
        log.info("cTrader stub connect OK (replace with protobuf transport)")

    async def authenticate(self) -> None:
        await self.connect()
        if not self._connected:
            raise ConnectionError("cTrader not authenticated (missing credentials or stub)")
        # ProtoOAApplicationAuthReq / ProtoOAAccountAuthReq sequence goes here
        await asyncio.sleep(0)

    async def subscribe_spots(self, symbol_names: List[str]) -> None:
        await self.authenticate()
        for s in symbol_names:
            self._subscriptions[s] = {"subscribed_at": time.time()}
            log.info("Subscribed (stub) to spots: %s", s)

    async def get_last_tick(self, symbol: str) -> Dict[str, float]:
        """Return bid/ask/mid from last subscription or raise."""
        if symbol not in self._subscriptions:
            await self.subscribe_spots([symbol])
        # Without live feed, return sentinel — broker layer may override from paper
        raise NotImplementedError(
            "Live ticks require protobuf stream; use PaperBroker or implement _cache from spot events"
        )

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: Action,
        volume_lots: float,
        label: str,
    ) -> Dict[str, Any]:
        await self.authenticate()
        if side not in ("BUY", "SELL"):
            return {"error": "invalid_side", "side": side}
        oid = str(uuid.uuid4())
        log.info(
            "Stub market order: %s %s %.4f lots label=%s id=%s",
            side,
            symbol,
            volume_lots,
            label,
            oid,
        )
        return {
            "order_id": oid,
            "symbol": symbol,
            "side": side,
            "volume": volume_lots,
            "label": label,
            "status": "filled_stub",
        }


class CTraderBroker(Broker):
    """
    Broker adapter: cTrader Open API with graceful fallback to cached stub quotes
    when protobuf feed is not implemented yet.
    """

    def __init__(
        self,
        cfg: CTraderConfig,
        *,
        quote_fallback: Optional[Callable[[str], MarketSnapshot]] = None,
    ) -> None:
        self._cfg = cfg
        self._client = CTraderProtoClient(cfg)
        self._quote_fallback = quote_fallback
        self._last_quotes: Dict[str, MarketSnapshot] = {}

    async def get_market_data(self, symbol: str) -> MarketSnapshot:
        try:
            await self._client.subscribe_spots([symbol])
            _ = await self._client.get_last_tick(symbol)
        except NotImplementedError:
            if self._quote_fallback:
                snap = self._quote_fallback(symbol)
                self._last_quotes[symbol] = snap
                return snap
            # Minimal stub quote so the rest of the stack runs end-to-end
            mid = 2650.0
            spread = 0.25
            snap = MarketSnapshot(
                symbol=symbol,
                bid=mid - spread / 2,
                ask=mid + spread / 2,
                mid=mid,
                spread=spread,
                ts_unix=time.time(),
                extra={"venue": "ctrader_stub", "note": "Replace with live ProtoOASpotEvent"},
            )
            self._last_quotes[symbol] = snap
            return snap
        except Exception as exc:
            log.exception("cTrader get_market_data failed: %s", exc)
            raise

    async def execute_trade(
        self,
        *,
        symbol: str,
        side: Action,
        volume: float,
        decision_reason: str,
        dry_run: bool,
    ) -> TradeResult:
        if side == "HOLD":
            raise ValueError("HOLD should not reach cTrader execute_trade")
        label = f"ai_{uuid.uuid4().hex[:12]}"
        if dry_run:
            m = await self.get_market_data(symbol)
            entry = m.ask if side == "BUY" else m.bid
            return TradeResult(
                order_id=f"dry_{uuid.uuid4()}",
                symbol=symbol,
                side=side,
                volume=volume,
                entry_price=entry,
                executed=False,
                dry_run=True,
                message="dry_run ctrader",
                position_id=f"dry_{label}",
                raw_response={"reason": decision_reason, "label": label},
            )
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.4, min=0.5, max=20),
            reraise=True,
        ):
            with attempt:
                raw = await self._client.place_market_order(
                    symbol=symbol, side=side, volume_lots=volume, label=label
                )
        m = await self.get_market_data(symbol)
        entry = m.ask if side == "BUY" else m.bid
        return TradeResult(
            order_id=str(raw.get("order_id", uuid.uuid4())),
            symbol=symbol,
            side=side,
            volume=volume,
            entry_price=entry,
            executed=True,
            dry_run=False,
            message="ctrader_stub_fill",
            position_id=(
                str(raw.get("position_id") or raw.get("order_id"))
                if (raw.get("position_id") or raw.get("order_id"))
                else None
            ),
            raw_response={**raw, "reason": decision_reason},
        )

    async def close_position(
        self,
        *,
        symbol: str,
        position: OpenPosition,
        reason: str,
        dry_run: bool,
    ) -> CloseResult:
        m = await self.get_market_data(symbol)
        exit_price = m.bid if position.side == "BUY" else m.ask
        return CloseResult(
            symbol=symbol,
            side=position.side,
            volume=position.volume,
            exit_price=exit_price,
            closed=True,
            dry_run=dry_run,
            message="ctrader_stub_close",
            position_id=position.position_id,
            raw_response={"reason": reason},
        )
