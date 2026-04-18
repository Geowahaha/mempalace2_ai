import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.insert(0, ".")

from scanners.ofas_scanner import OFASScanner

DB_PATH = "data/ctrader_openapi.db"

# Known Mar 23 bleed times (UTC): 07:44, 09:15, 13:41
# P&L Saved estimates from the $52.11 sinkhole
trade_times = [
    {"time": "2026-03-23T07:44:00Z", "loss": 17.32},
    {"time": "2026-03-23T09:15:00Z", "loss": 17.73},
    {"time": "2026-03-23T13:41:00Z", "loss": 17.06}
]

def simulate_ofas():
    ofas = OFASScanner(min_imbalance=0.30, min_slope=0.015, min_tick_bias=0.65)
    total_saved = 0.0
    blocked_count = 0

    print("=== GATE 2: OFAS Offline Suppression Test ===")
    
    with sqlite3.connect(DB_PATH) as conn:
        for trade in trade_times:
            trade_utc = trade['time']
            # Replay 5 minutes before trade
            trade_dt = datetime.strptime(trade_utc, "%Y-%m-%dT%H:%M:%SZ")
            from_dt = trade_dt - timedelta(minutes=5)
            from_utc = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # 1. Fetch Ticks
            tick_q = "SELECT event_ts, bid, ask FROM ctrader_spot_ticks WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC"
            df_ticks = pd.read_sql_query(tick_q, conn, params=(from_utc, trade_utc))
            
            # 2. Fetch Depth
            depth_q = "SELECT event_ts, side, price, size FROM ctrader_depth_quotes WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC"
            df_depth = pd.read_sql_query(depth_q, conn, params=(from_utc, trade_utc))
            
            # 3. Process Ticks
            last_bid = 0.0
            last_ask = 0.0
            for _, row in df_ticks.iterrows():
                b = float(row['bid'])
                a = float(row['ask'])
                if b > 0: last_bid = b
                if a > 0: last_ask = a
                if last_bid > 0 and last_ask > 0:
                    ofas.on_tick(last_bid, last_ask, int(float(row['event_ts']) * 1000))
                    
            # 4. Process Depth Snapshots
            depth_events_count = 0
            if not df_depth.empty:
                for ts, group in df_depth.groupby('event_ts'):
                    bids = group[group['side'] == 'bid'][['price', 'size']].to_dict('records')
                    asks = group[group['side'] == 'ask'][['price', 'size']].to_dict('records')
                    if bids or asks:
                        ofas.on_depth_event(bids, asks, int(float(ts) * 1000))
                        depth_events_count += 1
                        
            # 5. Evaluate exactly at trade time
            result = ofas.should_allow_signal("XAUUSD", "short")
            allowed = result['allowed']
            imb = result['imbalance']
            slope = result['slope']
            t_bias = result['tick_bias']
            
            print(f"\nEvaluating SHORT entry at {trade_utc}")
            print(f"Context: {len(df_ticks)} ticks, {depth_events_count} depth changes in prior 5m.")
            print(f"Metrics: Imbalance={imb:.3f} | Slope={slope:.3f} | Bullish_Tick_Bias={t_bias:.1%}")
            
            if not allowed:
                print(f"[OFAS LOG-ONLY] Suppressed SELL XAUUSD at {trade_utc} | Would have saved ≈${trade['loss']:.2f}")
                total_saved += trade['loss']
                blocked_count += 1
            else:
                print(f"[OFAS LOG-ONLY] Allowed SELL XAUUSD at {trade_utc} | Did NOT block loss.")

    print("\n-------------------------------------------")
    print(f"SUMMARY: {blocked_count}/3 bleed trades blocked.")
    print(f"Total Blocked P&L Saved: +${total_saved:.2f}")
    if blocked_count == 3:
        print("100% of the Mar 23 recovery sinkhole successfully prevented offline.")
    print("-------------------------------------------")

if __name__ == "__main__":
    simulate_ofas()
