"""
learning/signal_simulator.py - Signal Outcome Simulator
=========================================================
Background thread that monitors all pending signals (both main + scalping)
and determines TP/SL hit automatically using live price data.

Runs every 60 seconds.
Sends Telegram summary when a trade completes.
Keeps scalping results separate from main signal results.
"""
from __future__ import annotations

import logging
import time
import threading
from datetime import datetime, timezone
from typing import Optional

from api.signal_store import signal_store
from api.scalp_signal_store import scalp_store
from market.data_fetcher import xauusd_provider
from config import config

logger = logging.getLogger(__name__)

# Max time to keep a trade "pending" before marking expired (hours)
MAX_PENDING_HOURS = float(getattr(config, "SIM_MAX_PENDING_HOURS", 24.0))
SCALPING_MAX_PENDING_MIN = float(getattr(config, "SCALPING_SIM_MAX_PENDING_MIN", 120.0))
# Pip value for XAUUSD (1 pip = $0.1 for standard lot, $0.001 for micro)
XAUUSD_PIP_VALUE = 0.1  # per 0.01 lot
XAUUSD_LOT = 0.01


class SignalSimulator:
    """
    Monitors pending signals and resolves them when TP/SL is hit.
    Sends summary via Telegram after each completed trade.
    """

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._notifier = None  # Set lazily to avoid circular import

    @property
    def notifier(self):
        if self._notifier is None:
            try:
                from notifier.telegram_bot import notifier as tg
                self._notifier = tg
            except Exception:
                pass
        return self._notifier

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="SignalSimulator")
        self._thread.start()
        logger.info("[Simulator] Started — monitoring pending signals every 60s")

    def stop(self) -> None:
        self._running = False

    def _loop(self) -> None:
        while self._running:
            try:
                self._check_main_signals()
                self._check_scalp_signals()
                self._notify_completed_scalp()
            except Exception as e:
                logger.error("[Simulator] Loop error: %s", e)
            time.sleep(60)

    # ─── Price Fetch ──────────────────────────────────────────────────────────

    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol."""
        if "XAU" in symbol.upper() or symbol.upper() == "GOLD":
            try:
                p = xauusd_provider.get_current_price()
                if p:
                    return float(p)
                df = xauusd_provider.fetch("1m", bars=5)
                if df is not None and not df.empty:
                    return float(df.iloc[-1]["close"])
            except Exception:
                pass
        # For other symbols, try Yahoo Finance
        try:
            import yfinance as yf
            tickers = {
                "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X",
                "USDJPY": "USDJPY=X", "AUDUSD": "AUDUSD=X",
                "XAUUSD": "GC=F", "GOLD": "GC=F",
                "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD",
            }
            yf_sym = tickers.get(symbol.upper(), symbol)
            df = yf.download(yf_sym, period="1d", interval="1m", progress=False, timeout=8)
            if df is not None and not df.empty:
                if hasattr(df.columns, "get_level_values"):
                    df.columns = df.columns.get_level_values(0)
                df.columns = [str(c).lower() for c in df.columns]
                return float(df["close"].iloc[-1])
        except Exception:
            pass
        return None

    # ─── Main Signal Checker ─────────────────────────────────────────────────

    def _check_main_signals(self) -> None:
        """Check pending main signals (from signal_history.db)."""
        try:
            from api.signal_store import signal_store
            pending = signal_store.get_all_pending() if hasattr(signal_store, "get_all_pending") else []
        except Exception:
            return

        now_ts = time.time()
        for sig in pending:
            try:
                self._resolve_main_signal(sig, now_ts)
            except Exception as e:
                logger.debug("[Simulator] Error resolving main signal %s: %s", sig.get("id"), e)

    def _resolve_main_signal(self, sig: dict, now_ts: float) -> None:
        sid = sig.get("id")
        symbol = str(sig.get("symbol", ""))
        direction = str(sig.get("direction", "")).lower()
        entry = float(sig.get("entry", 0) or 0)
        sl = float(sig.get("stop_loss", 0) or 0)
        tp1 = float(sig.get("take_profit_1", 0) or 0)
        tp2 = float(sig.get("take_profit_2", 0) or 0)
        tp3 = float(sig.get("take_profit_3", 0) or 0)
        created_ts = float(sig.get("timestamp", now_ts) or now_ts)

        # Expire check
        age_hours = (now_ts - created_ts) / 3600.0
        if age_hours > MAX_PENDING_HOURS:
            signal_store.update_outcome(
                sid, "expired", entry, 0.0, 0.0
            )
            logger.info("[Simulator] Main #%d expired (age %.1fh)", sid, age_hours)
            return

        current = self._get_current_price(symbol)
        if current is None:
            return

        outcome = self._check_hit(direction, current, entry, sl, tp1, tp2, tp3)
        if outcome is None:
            return

        exit_price = self._resolve_exit_price(outcome, current, sl, tp1, tp2, tp3)
        pnl_pips = self._calc_pips(direction, entry, exit_price, symbol)
        pnl_r = self._calc_r_multiple(direction, entry, exit_price, sl)
        risk_usd = float(getattr(config, "SIM_RISK_USD_PER_SIGNAL", 10.0) or 10.0)
        pnl_usd = round(float(pnl_r) * float(risk_usd), 2)
        signal_store.update_outcome(sid, outcome, exit_price, pnl_pips, pnl_usd)
        logger.info("[Simulator] Main #%d → %s @ %.4f (pips=%.1f)", sid, outcome, exit_price, pnl_pips)
        if bool(getattr(config, "SIGNAL_OUTCOME_NOTIFY_ENABLED", True)) and (outcome in {"tp1_hit", "tp2_hit", "tp3_hit", "sl_hit"}):
            try:
                if self.notifier is not None and hasattr(self.notifier, "send_signal_outcome_update"):
                    self.notifier.send_signal_outcome_update(
                        {
                            "id": sid,
                            "symbol": symbol,
                            "direction": direction,
                            "entry": entry,
                            "exit_price": exit_price,
                            "outcome": outcome,
                            "pnl_pips": pnl_pips,
                            "pnl_usd": pnl_usd,
                            "holding_time_minutes": round(age_hours * 60.0, 1),
                        },
                        initial_balance=float(getattr(config, "SIGNAL_OUTCOME_INITIAL_BALANCE_USD", 1000.0) or 1000.0),
                        feature="signal_monitor",
                    )
            except Exception as e:
                logger.debug("[Simulator] outcome notify error sid=%s: %s", sid, e)

    # ─── Scalp Signal Checker ─────────────────────────────────────────────────

    def _check_scalp_signals(self) -> None:
        """Check pending scalp signals (from scalp_signal_history.db)."""
        pending = scalp_store.get_pending()
        now_ts = time.time()
        for sig in pending:
            try:
                self._resolve_scalp_signal(sig, now_ts)
            except Exception as e:
                logger.debug("[Simulator] Error resolving scalp #%s: %s", sig.get("id"), e)

    def _resolve_scalp_signal(self, sig: dict, now_ts: float) -> None:
        sid = sig["id"]
        symbol = str(sig.get("symbol", "XAUUSD") or "XAUUSD")
        direction = str(sig.get("direction", "")).lower()
        entry = float(sig.get("entry", 0) or 0)
        sl = float(sig.get("stop_loss", 0) or 0)
        tp1 = float(sig.get("take_profit_1", 0) or 0)
        tp2 = float(sig.get("take_profit_2", 0) or 0)
        tp3 = float(sig.get("take_profit_3", 0) or 0)
        created_ts = float(sig.get("timestamp", now_ts) or now_ts)

        age_minutes = (now_ts - created_ts) / 60.0
        scalp_max_pending_min = max(5.0, float(getattr(config, "SCALPING_SIM_MAX_PENDING_MIN", SCALPING_MAX_PENDING_MIN) or SCALPING_MAX_PENDING_MIN))
        if age_minutes > scalp_max_pending_min:
            scalp_store.update_outcome(sid, "expired", entry, 0.0, 0.0)
            logger.info("[Simulator] Scalp #%d expired (age %.1f min)", sid, age_minutes)
            return

        current = self._get_current_price(symbol)
        if current is None:
            return

        outcome = self._check_hit(direction, current, entry, sl, tp1, tp2, tp3)
        if outcome is None:
            return

        exit_price = self._resolve_exit_price(outcome, current, sl, tp1, tp2, tp3)
        pnl_pips = self._calc_pips(direction, entry, exit_price, symbol)
        symbol_up = symbol.upper()
        if ("XAU" in symbol_up) or ("GOLD" in symbol_up):
            # XAUUSD legacy pip-value accounting (per 0.01 lot).
            pnl_usd = pnl_pips * XAUUSD_PIP_VALUE * XAUUSD_LOT / 0.01 * 1.0
        else:
            # Non-XAU scalp assets: use R-multiple * configured simulated risk.
            pnl_r = self._calc_r_multiple(direction, entry, exit_price, sl)
            risk_usd = float(getattr(config, "SIM_RISK_USD_PER_SIGNAL", 10.0) or 10.0)
            pnl_usd = round(float(pnl_r) * float(risk_usd), 2)
        scalp_store.update_outcome(sid, outcome, exit_price, pnl_pips, pnl_usd)
        logger.info("[Simulator] Scalp #%d %s → %s @ %.2f (pips=%.1f, $%.2f)",
                    sid, symbol_up, outcome, exit_price, pnl_pips, pnl_usd)

    # ─── Notify Completed Scalp Trades ────────────────────────────────────────

    def _notify_completed_scalp(self) -> None:
        """Send Telegram summary for each completed but unnotified scalp trade."""
        completed = scalp_store.get_completed_unnotified()
        for sig in completed:
            try:
                self._send_scalp_summary(sig)
                scalp_store.mark_notified(sig["id"])
            except Exception as e:
                logger.debug("[Simulator] Notify error for scalp #%s: %s", sig.get("id"), e)

    def _send_scalp_summary(self, sig: dict) -> None:
        if not self.notifier:
            return
        symbol = str(sig.get("symbol", "XAUUSD") or "XAUUSD").upper()
        outcome = sig.get("outcome", "unknown")
        direction = str(sig.get("direction", "")).upper()
        entry = float(sig.get("entry", 0))
        exit_price = float(sig.get("exit_price", 0))
        pnl_pips = float(sig.get("pnl_pips", 0))
        pnl_usd = float(sig.get("pnl_usd", 0))
        hold_min = float(sig.get("holding_time_minutes", 0))
        pattern = str(sig.get("pattern", ""))
        kill_zone = str(sig.get("kill_zone", ""))
        confidence = float(sig.get("confidence", 0))

        # Outcome emoji
        if outcome in ("tp1_hit", "tp2_hit", "tp3_hit"):
            outcome_emoji = "✅"
            result_label = f"{'TP1' if outcome=='tp1_hit' else 'TP2' if outcome=='tp2_hit' else 'TP3'} HIT"
        elif outcome == "sl_hit":
            outcome_emoji = "❌"
            result_label = "SL HIT"
        else:
            outcome_emoji = "⏱️"
            result_label = "EXPIRED"

        # PnL sign
        pnl_sign = "+" if pnl_pips >= 0 else ""

        # Get stats
        stats = scalp_store.get_stats_filtered(symbol=symbol, last_n=50)
        win_rate_str = f"{stats.get('win_rate', 0):.1f}%" if stats.get("count", 0) > 0 else "N/A"
        total_pips_str = f"{stats.get('total_pips', 0):+.1f}" if stats.get("count", 0) > 0 else "N/A"
        if ("XAU" in symbol) or ("GOLD" in symbol):
            pnl_line = f"💰 PnL: `{pnl_sign}{pnl_pips:.1f} pips` (${pnl_sign}{pnl_usd:.2f} per 0.01 lot)"
        else:
            pnl_line = f"💰 PnL: `{pnl_sign}{pnl_pips:.1f}` (${pnl_sign}{pnl_usd:.2f} sim-risk)"

        msg = (
            f"⚡ *{symbol} SCALP RESULT* {outcome_emoji}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {direction} | {result_label}\n"
            f"🎯 Entry: ${entry:.2f} → ${exit_price:.2f}\n"
            f"{pnl_line}\n"
            f"⏱️ Hold: {hold_min:.0f} min\n"
            f"🔬 Pattern: `{pattern}`\n"
            f"🕐 Zone: `{kill_zone}`\n"
            f"🧠 Conf: {confidence:.1f}%\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📈 *Scalp Stats (last 50):*\n"
            f"Win Rate: {win_rate_str} | Total: {total_pips_str} pips\n"
            f"Profit Factor: {stats.get('profit_factor', 0):.2f}"
        )

        try:
            self.notifier.send_custom_alert(
                msg,
                feature="scalp_result",
                symbol=symbol,
                parse_mode="Markdown",
            )
        except Exception as e:
            # Fallback: try generic message
            try:
                self.notifier._send_message(msg, parse_mode="Markdown")
            except Exception as e2:
                logger.debug("[Simulator] Telegram notify failed: %s / %s", e, e2)

    # ─── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _is_fx_symbol(symbol: str) -> bool:
        s = str(symbol or "").strip().upper().replace(" ", "")
        if not s:
            return False
        if "/" in s:
            s = s.replace("/", "")
        try:
            fx = {str(x).upper().replace("/", "") for x in (config.get_fx_major_symbols() or [])}
            if s in fx:
                return True
        except Exception:
            pass
        if len(s) != 6:
            return False
        base = s[:3]
        quote = s[3:]
        ccy = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"}
        return (base in ccy) and (quote in ccy)

    @staticmethod
    def _check_hit(
        direction: str,
        current: float,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
    ) -> Optional[str]:
        """
        Check if current price has hit any exit level.
        Returns outcome string or None if still open.
        """
        if direction == "long":
            if tp3 > 0 and current >= tp3:
                return "tp3_hit"
            if tp2 > 0 and current >= tp2:
                return "tp2_hit"
            if tp1 > 0 and current >= tp1:
                return "tp1_hit"
            if sl > 0 and current <= sl:
                return "sl_hit"
        elif direction == "short":
            if tp3 > 0 and current <= tp3:
                return "tp3_hit"
            if tp2 > 0 and current <= tp2:
                return "tp2_hit"
            if tp1 > 0 and current <= tp1:
                return "tp1_hit"
            if sl > 0 and current >= sl:
                return "sl_hit"
        return None

    @staticmethod
    def _resolve_exit_price(
        outcome: str,
        current: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
    ) -> float:
        """
        Convert an outcome label to the executed level price.
        In simulation we assume stop/target fills at configured levels, not at delayed poll price.
        """
        mapping = {
            "tp1_hit": tp1,
            "tp2_hit": tp2,
            "tp3_hit": tp3,
            "sl_hit": sl,
        }
        try:
            px = float(mapping.get(str(outcome or "").lower(), current))
            if px > 0:
                return px
        except Exception:
            pass
        try:
            return float(current)
        except Exception:
            return 0.0

    @staticmethod
    def _calc_pips(direction: str, entry: float, exit_price: float, symbol: str) -> float:
        """Calculate pips. For XAUUSD 1 pip = $0.01."""
        diff = exit_price - entry if direction == "long" else entry - exit_price
        sym = str(symbol or "").upper()
        if "XAU" in sym or "GOLD" in sym:
            return round(diff / 0.01, 1)  # gold pip = $0.01
        elif SignalSimulator._is_fx_symbol(sym):
            return round(diff / 0.0001, 1)  # forex pip = 0.0001
        else:
            return round(diff, 4)

    @staticmethod
    def _calc_r_multiple(direction: str, entry: float, exit_price: float, stop_loss: float) -> float:
        """
        Calculate R-multiple from entry/exit/SL.
        Returns bounded value to avoid unstable outliers in virtual PnL accounting.
        """
        try:
            e = float(entry)
            x = float(exit_price)
            sl = float(stop_loss)
            risk = abs(e - sl)
            if risk <= 1e-12:
                return 0.0
            pnl = (x - e) if str(direction or "").lower() == "long" else (e - x)
            r = pnl / risk
            if r > 6.0:
                return 6.0
            if r < -3.0:
                return -3.0
            return float(r)
        except Exception:
            return 0.0


# Singleton
signal_simulator = SignalSimulator()
