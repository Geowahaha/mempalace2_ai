import numpy as np

class OFASScanner:
    """
    Order-Flow Asymmetry Scanner (OFAS).
    Evaluates raw tick hits and level-2 depth imbalance to confirm or suppress signals.
    """
    def __init__(self, min_imbalance=0.30, min_slope=0.015, min_tick_bias=0.65):
        self.min_imbalance = min_imbalance
        self.min_slope = min_slope
        self.min_tick_bias = min_tick_bias
        
        # State
        self.last_price = 0.0
        self.tick_history = []  # List of 'bid' or 'ask' hits
        self.imbalance_history = [] # List of float imbalances
        self.bids_book = {} # price -> size
        self.asks_book = {} # price -> size

    def on_tick(self, bid: float, ask: float, ts_ms: int):
        mid = (bid + ask) / 2.0
        
        # Simple tick direction classifier
        if self.last_price > 0:
            if mid < self.last_price:
                self.tick_history.append("bid")
            elif mid > self.last_price:
                self.tick_history.append("ask")
        self.last_price = mid
        
        # Keep last 20 hits
        if len(self.tick_history) > 20:
            self.tick_history.pop(0)

    def on_depth_event(self, bids: list, asks: list, ts_ms: int):
        """
        bids and asks are lists of dicts e.g. [{'price': X, 'size': Y}, ...]
        We maintain a cumulative order book because events are deltas.
        """
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
                
        # Sum top 3 levels from the current state of the book
        sorted_bids = sorted(self.bids_book.items(), key=lambda x: x[0], reverse=True)[:3]
        sorted_asks = sorted(self.asks_book.items(), key=lambda x: x[0])[:3]
        
        bid_vol = sum(size for price, size in sorted_bids)
        ask_vol = sum(size for price, size in sorted_asks)
        
        total = bid_vol + ask_vol
        imbalance = 0.0
        if total > 0:
            # -1.0 to 1.0 (Positive means bid dominance -> bullish)
            imbalance = (bid_vol - ask_vol) / total
            
        self.imbalance_history.append(imbalance)
        if len(self.imbalance_history) > 5:
            self.imbalance_history.pop(0)

    def should_allow_signal(self, symbol: str, direction: str) -> dict:
        """
        Returns {'allowed': bool, 'imbalance': float, 'slope': float, 'tick_bias': float}
        """
        # 1. Imbalance
        current_imb = self.imbalance_history[-1] if self.imbalance_history else 0.0
        
        # 2. Imbalance Slope (Using numpy polyfit)
        slope = 0.0
        if len(self.imbalance_history) >= 3:
            y = self.imbalance_history
            x = range(len(y))
            slope = np.polyfit(x, y, 1)[0]
            
        # 3. Tick Bias
        bid_hits = sum(1 for t in self.tick_history if t == "bid")
        ask_hits = sum(1 for t in self.tick_history if t == "ask")
        total_hits = bid_hits + ask_hits
        
        tick_bias_short = (bid_hits / total_hits) if total_hits > 0 else 0.5
        tick_bias_long = (ask_hits / total_hits) if total_hits > 0 else 0.5

        allowed = True
        if direction == "short":
            # For a short to be valid, we need seller dominance.
            # If buyers are dominant (bullish orderflow -> recovery regime), we suppress.
            # So if imbalance > 0.30 (heavy bids) -> Suppress
            # If slope > 0.015 (bids growing faster) -> Suppress
            if current_imb > self.min_imbalance or slope > self.min_slope or tick_bias_long > self.min_tick_bias:
                allowed = False

        elif direction == "long":
            if current_imb < -self.min_imbalance or slope < -self.min_slope or tick_bias_short > self.min_tick_bias:
                allowed = False

        return {
            "allowed": allowed,
            "imbalance": current_imb,
            "slope": slope,
            "tick_bias": tick_bias_short if direction == "short" else tick_bias_long
        }
