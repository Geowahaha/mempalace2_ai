import numpy as np

class MRDScanner:
    """
    Microstructure Regime Detector (MRD).
    Calculates a dynamic Recovery Score to detect hidden macro recoveries
    using tick velocity, spread compression, VWAP momentum, and L2 imbalance.
    """
    def __init__(self):
        self.bar_history = []
        self.bids_book = {}
        self.asks_book = {}
        self.imbalance = 0.0

    def on_depth_event(self, bids: list, asks: list, ts_ms: int):
        for b in bids:
            if b['size'] == 0:
                self.bids_book.pop(b['price'], None)
            else:
                self.bids_book[b['price']] = b['size']
                
        for a in asks:
            if a['size'] == 0:
                self.asks_book.pop(a['price'], None)
            else:
                self.asks_book[a['price']] = a['size']
                
        sorted_bids = sorted(self.bids_book.items(), key=lambda x: x[0], reverse=True)[:3]
        sorted_asks = sorted(self.asks_book.items(), key=lambda x: x[0])[:3]
        
        bid_vol = sum(size for price, size in sorted_bids)
        ask_vol = sum(size for price, size in sorted_asks)
        
        total = bid_vol + ask_vol
        if total > 0:
            self.imbalance = (bid_vol - ask_vol) / total
            
    def on_tick_bar_completed(self, bar: dict):
        self.bar_history.append(bar)
        if len(self.bar_history) > 30:
            self.bar_history.pop(0)

    def compute_recovery_score(self, symbol: str) -> dict:
        if len(self.bar_history) < 15:
            return {"score": 0.0, "vwap_slope": 0.0, "velocity_ratio": 0.0, "spread_compression": 0.0, "new_low": 0.0, "imbalance": 0.0}
            
        # 1. VWAP Slope (last 10 bars)
        y = [b['vwap'] for b in self.bar_history[-10:]]
        x = range(len(y))
        slope = np.polyfit(x, y, 1)[0]
        # Normalize upward slope (assuming +1.0 pt/bar is strongly bullish)
        vwap_slope_norm = min(1.0, max(0.0, slope / 1.0))
        
        # 2. Tick Velocity Surge (Using 100-tick bars: shorter duration_ms = faster printing)
        current_dur = max(1, sum([b['bar_end_ms'] - b['bar_start_ms'] for b in self.bar_history[-3:]]) / 3)
        prior_dur = max(1, sum([b['bar_end_ms'] - b['bar_start_ms'] for b in self.bar_history[-15:-3]]) / 12)
        
        # velocity_ratio: > 1 means recent bars are printing faster
        velocity_ratio = prior_dur / current_dur
        # Normalize: if forming twice as fast (vel=2.0), score is 1.0
        vel_norm = min(1.0, max(0.0, (velocity_ratio - 1.0)))
        
        # 3. Spread Compression
        current_spread = self.bar_history[-1]['spread_avg']
        avg_prior_spread = sum(b['spread_avg'] for b in self.bar_history[-20:]) / max(1, len(self.bar_history[-20:]))
        
        spread_ratio = current_spread / avg_prior_spread if avg_prior_spread > 0 else 1.0
        # If ratio < 0.7, it's very compressed
        spread_comp_norm = min(1.0, max(0.0, (1.0 - spread_ratio) * 2.5))
        
        # 4. New Low Failure (last 5 bars)
        last_5_lows = [b['low'] for b in self.bar_history[-5:]]
        new_low_failure = 0.0
        if min(last_5_lows[-2:]) > min(last_5_lows[:3]):
            new_low_failure = 1.0
            
        # 5. Imbalance Flip
        imb_norm = max(0.0, self.imbalance)
        
        # Tuning weights: Structure is King (VWAP + New Low Failure). 
        # Noise/Micro (velocity, spread, imb) are secondary.
        score = (
            0.40 * vwap_slope_norm +
            0.15 * vel_norm +
            0.10 * spread_comp_norm +
            0.35 * new_low_failure
        )
        
        return {
            "score": score,
            "vwap_slope": vwap_slope_norm,
            "velocity_ratio": vel_norm,
            "spread_compression": spread_comp_norm,
            "new_low": new_low_failure,
            "imbalance": imb_norm
        }

    def should_suppress_short(self, symbol: str) -> tuple[bool, dict]:
        res = self.compute_recovery_score(symbol)
        # Suppress if Recovery Score > 0.65
        return res['score'] > 0.65, res
