import sqlite3
import pandas as pd
from datetime import datetime
import sys
sys.path.insert(0, ".")

from market.tick_bar_engine import TickBarEngine
from scanners.mrd_scanner import MRDScanner

DB_PATH = "data/ctrader_openapi.db"
SYMBOL = "XAUUSD"

# Trade 3 (the only one with reliable context data, Trades 1 & 2 are in the data gap)
TRADE_UTC = "2026-03-23T13:05:19Z"

# Known winner to test for False Positives in the bleed window
# E.g. RR family made +$45.50 on Mar 23. Let's sample a random time 
# shortly after data restored that might have had a winning short.
# (If we don't have exact time, we'll evaluate a few intervals).

def simulate_mrd():
    mrd = MRDScanner()
    engine = TickBarEngine(SYMBOL, 100, "tick")
    
    print("=== GATE 3: MRD Offline Suppression Test ===")
    print(f"Loading ticks & depth from 09:50 UTC to {TRADE_UTC}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        from_utc = "2026-03-23T10:00:00Z"
        
        # 1. Fetch Ticks
        df_ticks = pd.read_sql_query(
            "SELECT event_ts, bid, ask FROM ctrader_spot_ticks WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC", 
            conn, params=(from_utc, TRADE_UTC))
        
        # 2. Fetch Depth
        df_depth = pd.read_sql_query(
            "SELECT event_ts, side, price, size FROM ctrader_depth_quotes WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC", 
            conn, params=(from_utc, TRADE_UTC))
            
    print(f"Loaded {len(df_ticks)} ticks and {len(df_depth)} depth rows.")
    
    # Pre-group depth by timestamp
    depth_groups = {}
    for _, row in df_depth.iterrows():
        ts = int(float(row['event_ts']) * 1000)
        if ts not in depth_groups:
            depth_groups[ts] = {'bids': [], 'asks': []}
        if row['side'] == 'bid':
            depth_groups[ts]['bids'].append({'price': row['price'], 'size': row['size']})
        else:
            depth_groups[ts]['asks'].append({'price': row['price'], 'size': row['size']})

    # Replay Loop
    last_bid, last_ask = 0.0, 0.0
    
    # To evaluate exact times
    from datetime import timezone
    target_ms = int(datetime.strptime(TRADE_UTC, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)
    
    for _, row in df_ticks.iterrows():
        b, a = float(row['bid']), float(row['ask'])
        ts_ms = int(float(row['event_ts']) * 1000)
        
        if b > 0: last_bid = b
        if a > 0: last_ask = a
        if last_bid == 0 or last_ask == 0:
            continue
            
        # Check depth events at this ts
        if ts_ms in depth_groups:
            mrd.on_depth_event(depth_groups[ts_ms]['bids'], depth_groups[ts_ms]['asks'], ts_ms)
            
        completed_bar = engine.on_quote(last_bid, last_ask, ts_ms)
        if completed_bar:
            mrd.on_tick_bar_completed(completed_bar)
            
        # Break exactly at the trade entry to capture state AT execution
        if ts_ms >= target_ms:
            break
            
    print(f"\nEvaluating SHORT entry at {TRADE_UTC}")
    should_suppress, metrics = mrd.should_suppress_short(SYMBOL)
    
    score = metrics['score']
    print(f"Recovery Score: {score:.3f}")
    print(f"  - VWAP Slope: {metrics['vwap_slope']:.3f} (Wait: check trend)")
    print(f"  - Velocity Ratio: {metrics['velocity_ratio']:.3f}")
    print(f"  - Spread Comp: {metrics['spread_compression']:.3f}")
    print(f"  - New Low Failure: {metrics['new_low']:.1f}")
    print(f"  - Imbalance: {metrics['imbalance']:.3f}")

    if should_suppress:
        print(f"\n[MRD LOG-ONLY] Suppressed SELL XAUUSD at {TRADE_UTC} | Would have saved ~$17.06")
    else:
        print(f"\n[MRD LOG-ONLY] Allowed SELL XAUUSD at {TRADE_UTC} | Did NOT block loss.")
        
    print("\n-------------------------------------------")
    if should_suppress:
        print("100% of visible Mar 23 bleed successfully blocked by MRD.")
    else:
        print("MRD failed to reach threshold (0.65). Try adjusting normalization.")
    print("-------------------------------------------")

if __name__ == "__main__":
    simulate_mrd()
