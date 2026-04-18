import threading
from typing import Optional, Literal, Dict, List

BarType = Literal["time", "tick", "volume", "range"]

class TickBarEngine:
    """
    Spotware Trend-bar-service logic ported to Python.
    Builds OHLCV bars from raw ticks using true quote timestamps.
    Thread-safe: quotes written on one thread, history readable on any.
    """

    def __init__(self, symbol: str, period_sec: int, bar_type: BarType = "time"):
        self.symbol = symbol
        self.period_sec = period_sec
        self.bar_type = bar_type
        self._lock = threading.RLock()
        self._current: Optional[Dict] = None
        self._completed: List[Dict] = []
        
        # for fixed-tick bars
        self.tick_target = period_sec if bar_type == "tick" else 0

    def _period_start(self, ts_ms: int) -> int:
        if self.bar_type == "time":
            period_ms = self.period_sec * 1000
            return (ts_ms // period_ms) * period_ms
        return ts_ms # for non-time bars, period start is just its first tick

    def _new_bar(self, mid: float, bid: float, ask: float, ts_ms: int, bar_start_ms: int) -> Dict:
        return {
            "bar_type": f"{self.bar_type}_{self.period_sec}",
            "bar_start_ms": bar_start_ms,
            "open": mid,
            "high": mid,
            "low": mid,
            "close": mid,
            "tick_count": 1,
            "volume_sum": 0.0,
            "vwap": mid,
            "spread_sum": ask - bid,
            "spread_avg": ask - bid,
            "bar_end_ms": ts_ms,
        }

    def _update(self, bar: Dict, mid: float, bid: float, ask: float, ts_ms: int) -> None:
        if mid > bar["high"]:
            bar["high"] = mid
        if mid < bar["low"]:
            bar["low"] = mid
        bar["close"] = mid
        bar["tick_count"] += 1
        # VWAP approximation (using tick count instead of real volume for spot FX/metals)
        bar["vwap"] = bar["vwap"] + (mid - bar["vwap"]) / bar["tick_count"]
        bar["spread_sum"] += (ask - bid)
        bar["spread_avg"] = bar["spread_sum"] / bar["tick_count"]
        bar["bar_end_ms"] = ts_ms

    def _finalize(self, bar: Dict, mid: float, ts_ms: int) -> Dict:
        # returns the completed bar
        # In a generic situation, the finalizing tick might actually belong to the NEXT bar,
        # but the update was skipped in on_quote so it's clean.
        return dict(bar)

    def on_quote(self, bid: float, ask: float, ts_ms: int) -> Optional[Dict]:
        """Feed a tick. Returns completed bar if the period closed."""
        mid = (bid + ask) / 2.0
        with self._lock:
            bar_start_ms = self._period_start(ts_ms)
            
            if self._current is None:
                self._current = self._new_bar(mid, bid, ask, ts_ms, bar_start_ms)
                return None
                
            if self.bar_type == "time":
                if bar_start_ms > self._current["bar_start_ms"]:
                    completed = self._finalize(self._current, mid, ts_ms)
                    self._completed.append(completed)
                    self._current = self._new_bar(mid, bid, ask, ts_ms, bar_start_ms)
                    return completed
            elif self.bar_type == "tick":
                if self._current["tick_count"] >= self.tick_target:
                    completed = self._finalize(self._current, mid, ts_ms)
                    self._completed.append(completed)
                    self._current = self._new_bar(mid, bid, ask, ts_ms, ts_ms)
                    return completed

            self._update(self._current, mid, bid, ask, ts_ms)
            return None

    def history(self, n: int = 500) -> List[Dict]:
        """Read last N completed bars."""
        with self._lock:
            return list(self._completed[-n:])
