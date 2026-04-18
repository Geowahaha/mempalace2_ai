"""
learning/mt5_limit_manager.py
Autonomous manager for MT5 Pending Orders (Limit Orders):
- Scans `orders_get()` for open STOP/LIMIT orders.
- Cancels if timeout elapsed (e.g. 60+ mins).
- Cancels if TP1 is hit before the entry executes (Front-run).
- Cancels if Stop Loss level is broken before the entry executes (Structure break).
"""
import time
import logging
from dataclasses import dataclass
from typing import Optional, List

from config import config
from execution.mt5_executor import mt5_executor

logger = logging.getLogger(__name__)

@dataclass
class LimitActionResult:
    ok: bool
    action: str
    message: str
    ticket: int
    symbol: str

class MT5LimitManager:
    """Monitors and manages open pending Limit orders."""
    
    def __init__(self):
        self._mt5 = mt5_executor._mt5
        self.enabled = bool(getattr(config, "MT5_LIMIT_ENTRY_ENABLED", True))

    def run_cycle(self, source: str = "limit_manager") -> dict:
        """Runs the validation logic against all open limit orders."""
        if not self.enabled or not self._mt5:
            return {"ok": False, "error": "disabled_or_no_mt5"}
            
        try:
            orders = self._mt5.orders_get()
            if not orders:
                return {"ok": True, "orders": 0, "actions": []}
                
            bot_magic = int(getattr(config, "MT5_MAGIC", 0))
            active_limits = [
                o for o in orders 
                if getattr(o, "magic", 0) == bot_magic and 
                int(getattr(o, "type", -1)) in (self._mt5.ORDER_TYPE_BUY_LIMIT, self._mt5.ORDER_TYPE_SELL_LIMIT)
            ]
            
            if not active_limits:
                return {"ok": True, "orders": 0, "actions": []}
                
            actions = []
            for order in active_limits:
                res = self._evaluate_order(order)
                if res and res.action != "none":
                    actions.append(res)
                    
            return {
                "ok": True, 
                "orders": len(active_limits), 
                "actions": [vars(a) for a in actions]
            }
            
        except Exception as e:
            logger.error("[MT5LimitMgr] Cycle failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _evaluate_order(self, order) -> Optional[LimitActionResult]:
        ticket = int(getattr(order, "ticket", 0))
        symbol = str(getattr(order, "symbol", ""))
        order_type = int(getattr(order, "type", -1))
        setup_time = int(getattr(order, "time_setup", 0)) # UNIX timestamp
        sl = float(getattr(order, "sl", 0.0))
        tp = float(getattr(order, "tp", 0.0))
        
        is_buy_limit = (order_type == self._mt5.ORDER_TYPE_BUY_LIMIT)
        
        # 1. Timeout Check
        timeout_mins = int(getattr(config, "MT5_LIMIT_TIMEOUT_MINS", 60))
        age_sec = int(time.time()) - setup_time
        if age_sec > (timeout_mins * 60):
            return self._cancel_order(ticket, symbol, "timeout", f"Order age {age_sec/60:.1f}m > {timeout_mins}m limit")

        # Get latest tick data for price invalidation checks
        tick = self._mt5.symbol_info_tick(symbol)
        if not tick:
            return None
            
        ask = float(getattr(tick, "ask", 0.0))
        bid = float(getattr(tick, "bid", 0.0))
        
        # 2. TP Front-run Check (Has price touched target before entry?)
        if bool(getattr(config, "MT5_LIMIT_CANCEL_ON_TP_FRONT_RUN", True)) and tp > 0:
            if is_buy_limit and bid >= tp:
                return self._cancel_order(ticket, symbol, "tp_frontrun", f"Price {bid} hit TP {tp} before entry")
            elif not is_buy_limit and ask <= tp:
                return self._cancel_order(ticket, symbol, "tp_frontrun", f"Price {ask} hit TP {tp} before entry")
                
        # 3. Structure Break / SL Hit Check (Has price broken structure before entry?)
        if bool(getattr(config, "MT5_LIMIT_CANCEL_ON_SL_BREAK", True)) and sl > 0:
            if is_buy_limit and bid <= sl:
                return self._cancel_order(ticket, symbol, "sl_break", f"Price {bid} broke SL {sl} structure before entry")
            elif not is_buy_limit and ask >= sl:
                return self._cancel_order(ticket, symbol, "sl_break", f"Price {ask} broke SL {sl} structure before entry")
                
        return None

    def _cancel_order(self, ticket: int, symbol: str, action: str, reason: str) -> LimitActionResult:
        request = {
            "action": self._mt5.TRADE_ACTION_REMOVE,
            "order": ticket,
        }
        
        result = self._mt5.order_send(request)
        if result and getattr(result, "retcode", -1) in (self._mt5.TRADE_RETCODE_DONE, self._mt5.TRADE_RETCODE_PLACED):
            logger.info("[MT5LimitMgr] Cancelled %s ticket %s: %s", symbol, ticket, reason)
            return LimitActionResult(True, action, reason, ticket, symbol)
        
        msg = f"Failed to cancel {symbol} ticket {ticket} ({reason})"
        logger.warning("[MT5LimitMgr] %s", msg)
        return LimitActionResult(False, "failed_cancel", msg, ticket, symbol)

mt5_limit_manager = MT5LimitManager()
