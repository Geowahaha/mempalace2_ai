"""Quick import & basic sanity test for Tiger Hunter changes."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    from analysis.smc import SMCAnalyzer, SMCContext, LiquidityPool, smc
    print("✓ SMC import OK")
    
    # Check LiquidityPool fields
    pool = LiquidityPool(
        pool_type="equal_highs",
        price=2650.0,
        strength=0.6,
        side="buy_side",
        touch_count=3,
        distance_atr=1.2,
    )
    assert pool.pool_type == "equal_highs"
    assert pool.touch_count == 3
    print(f"✓ LiquidityPool creation OK: {pool}")

    from analysis.signals import SignalGenerator, TradeSignal, signal_gen
    print("✓ Signals import OK")
    
    # Check new Tiger Hunter fields exist
    sig = TradeSignal(
        symbol="XAUUSD", direction="long", confidence=82.0,
        entry=2650.0, stop_loss=2640.0,
        take_profit_1=2660.0, take_profit_2=2670.0, take_profit_3=2680.0,
        risk_reward=2.0, timeframe="1h", session="london",
        trend="bullish", rsi=55.0, atr=10.0, pattern="OB Bounce",
        entry_type="limit", sl_type="anti_sweep",
        sl_reason="Anti-sweep SL behind equal_lows (3 touches)",
        tp_type="liquidity", tp_reason="TP at opposing liquidity",
        sl_liquidity_mapped=True, liquidity_pools_count=5,
    )
    d = sig.to_dict()
    assert d["sl_type"] == "anti_sweep"
    assert d["tp_type"] == "liquidity"
    assert d["sl_liquidity_mapped"] is True
    assert d["entry_type"] == "limit"
    print(f"✓ TradeSignal Tiger fields OK: sl_type={d['sl_type']}, tp_type={d['tp_type']}")

    # Test anti_sweep_sl with dummy data
    pools = [
        LiquidityPool("equal_lows", 2640.0, 0.8, "sell_side", 4, 1.0),
        LiquidityPool("equal_highs", 2670.0, 0.6, "buy_side", 2, 2.0),
    ]
    sl_price, sl_reason = smc.anti_sweep_sl(
        entry=2650.0, direction="long",
        liquidity_pools=pools, atr=10.0
    )
    assert sl_price < 2650.0, f"SL should be below entry, got {sl_price}"
    assert sl_price < 2640.0, f"SL should be BEHIND the pool at 2640, got {sl_price}"
    print(f"✓ Anti-sweep SL OK: SL={sl_price}, reason={sl_reason}")

    # Test liquidity_tp_targets
    tp_levels, tp_reason = smc.liquidity_tp_targets(
        entry=2650.0, direction="long", atr=10.0,
        liquidity_pools=pools
    )
    assert len(tp_levels) >= 1, "Should find at least 1 TP target"
    assert tp_levels[0] > 2650.0, f"TP should be above entry for long, got {tp_levels[0]}"
    print(f"✓ Liquidity TP targets OK: levels={tp_levels}, reason={tp_reason}")

    print("\n" + "=" * 50)
    print("🐯 ALL TIGER HUNTER IMPORT TESTS PASSED!")
    print("=" * 50)

if __name__ == "__main__":
    test_imports()
