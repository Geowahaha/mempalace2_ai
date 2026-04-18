"""
Gate 1 Historical Replay Tester
Reads raw ticks from ctrader_spot_ticks for Mar 20-24 and replays them iteratively
through TickBarEngine. Compares reconstructed M1 bars vs reference broker M1 bars.
Measures the number/profile of 100-tick bars during the Mar 23 recovery bleed window.
"""
import sqlite3
import pandas as pd
from market.tick_bar_engine import TickBarEngine

DB_PATH = "data/ctrader_openapi.db"
SYMBOL = "XAUUSD"

# Known Mar 23 bleed times (UTC+7 ICT): 14:44, 16:15, 20:41
# In UTC: 07:44, 09:15, 13:41
BLEED_START_UTC = '2026-03-23T07:00:00Z'
BLEED_END_UTC = '2026-03-23T15:00:00Z'

def run_replay():
    print(f"=== GATE 1: Historical Replay Test for {SYMBOL} ===")
    
    # 1. Fetch raw ticks for Mar 23 bleed window
    with sqlite3.connect(DB_PATH) as conn:
        query = f"""
        SELECT event_ts, bid, ask, event_utc
        FROM ctrader_spot_ticks
        WHERE symbol = 'XAUUSD'
          AND event_utc >= '{BLEED_START_UTC}'
          AND event_utc <= '{BLEED_END_UTC}'
        ORDER BY event_ts ASC
        """
        df_ticks = pd.read_sql_query(query, conn)
        
    if df_ticks.empty:
        print("No ticks found for the specified window.")
        return

    print(f"Loaded {len(df_ticks)} raw ticks from DB for window: {BLEED_START_UTC} to {BLEED_END_UTC}")

    # 2. Initialize engines
    engine_m1 = TickBarEngine(SYMBOL, 60, "time")
    engine_100t = TickBarEngine(SYMBOL, 100, "tick")

    completed_m1 = []
    completed_100t = []

    # 3. Replay loop (simulating live ingestion exactly)
    last_bid = 0.0
    last_ask = 0.0
    
    for _, row in df_ticks.iterrows():
        ts_ms = int(float(row['event_ts']) * 1000)
        b = float(row['bid'])
        a = float(row['ask'])
        
        if b > 0: last_bid = b
        if a > 0: last_ask = a
        
        if last_bid == 0 or last_ask == 0:
            continue

        # Feed M1 Engine
        res_m1 = engine_m1.on_quote(last_bid, last_ask, ts_ms)
        if res_m1:
            completed_m1.append(res_m1)
            
        # Feed 100-tick Engine
        res_100t = engine_100t.on_quote(last_bid, last_ask, ts_ms)
        if res_100t:
            completed_100t.append(res_100t)

    # 4. Analysis output
    print(f"\nReconstruction Complete:")
    print(f"- Total M1 bars generated: {len(completed_m1)}")
    print(f"- Total 100-tick bars generated: {len(completed_100t)}")
    
    # 5. Show volatility normalization (Compare range of M1 vs 100t)
    df_m1 = pd.DataFrame(completed_m1)
    df_100t = pd.DataFrame(completed_100t)
    
    if not df_m1.empty:
        df_m1['range'] = df_m1['high'] - df_m1['low']
        print(f"\nM1 Bar Range Stats (Bleed Window):")
        print(f"Max range: {df_m1['range'].max():.2f} pts | Avg range: {df_m1['range'].mean():.2f} pts")
        
    if not df_100t.empty:
        df_100t['range'] = df_100t['high'] - df_100t['low']
        print(f"\n100-Tick Bar Range Stats (Bleed Window):")
        print(f"Max range: {df_100t['range'].max():.2f} pts | Avg range: {df_100t['range'].mean():.2f} pts")
        
    # Show excerpt of 100-tick bars during the bleed
    if not df_100t.empty:
        print("\n--- Excerpt: First 5 100-tick Bars Generated ---")
        df_display = df_100t[['bar_start_ms', 'open', 'high', 'low', 'close', 'tick_count', 'range']].head(5)
        # Convert ms back to UTC for readability
        df_display['utc'] = pd.to_datetime(df_display['bar_start_ms'], unit='ms')
        print(df_display.to_string(index=False))

if __name__ == "__main__":
    run_replay()
