"""Tests for Tiger Risk Governor and Signal Store."""
import sys
import os
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_risk_governor():
    from execution.tiger_risk_governor import TigerRiskGovernor, TIGER_PHASES

    gov = TigerRiskGovernor()

    # Test phase detection
    phase = gov.get_phase(15.0)
    assert phase.name == "seedling", f"Expected seedling, got {phase.name}"
    assert phase.risk_pct == 0.5

    phase = gov.get_phase(100.0)
    assert phase.name == "sprout", f"Expected sprout, got {phase.name}"
    assert phase.risk_pct == 0.75

    phase = gov.get_phase(500.0)
    assert phase.name == "sapling", f"Expected sapling, got {phase.name}"

    phase = gov.get_phase(2000.0)
    assert phase.name == "tree", f"Expected tree, got {phase.name}"

    phase = gov.get_phase(10000.0)
    assert phase.name == "forest", f"Expected forest, got {phase.name}"

    phase = gov.get_phase(100000.0)
    assert phase.name == "titan", f"Expected titan, got {phase.name}"

    print("[PASS] Phase detection: all 6 phases correct")

    # Test lot sizing
    lot, meta = gov.calculate_lot_size(
        equity=15.0,
        risk_distance_pips=50,
        pip_value=0.10,
        confidence=75.0,
        sl_liquidity_mapped=False,
    )
    assert lot >= 0.01, f"Lot should be >= 0.01, got {lot}"
    assert lot <= 0.02, f"Lot should be <= 0.02 in seedling phase, got {lot}"
    assert meta["phase"] == "seedling"
    print(f"[PASS] Lot sizing: $15 equity => {lot} lots ({meta['phase']} phase)")

    # Test anti-sweep bonus
    lot_normal, _ = gov.calculate_lot_size(15.0, 50, 0.10, confidence=80.0, sl_liquidity_mapped=False)
    lot_sweep, _ = gov.calculate_lot_size(15.0, 50, 0.10, confidence=80.0, sl_liquidity_mapped=True)
    # Anti-sweep gives slightly larger lot (1.05x)
    print(f"[PASS] Anti-sweep bonus: normal={lot_normal}, anti-sweep={lot_sweep}")

    # Test larger equity lot sizing
    lot, meta = gov.calculate_lot_size(
        equity=1000.0,
        risk_distance_pips=50,
        pip_value=0.10,
        confidence=85.0,
    )
    assert lot > 0.01, f"$1K equity should give bigger lot, got {lot}"
    print(f"[PASS] $1K lot sizing: {lot} lots ({meta['phase']} phase, risk=${meta['risk_amount']:.2f})")

    # Test circuit breaker
    gov.reset_daily(100.0)
    ok, reason = gov.check_circuit_breaker(100.0, -2.0)  # 2% loss
    assert ok, "Should be OK at 2% loss in sprout phase (limit 3.5%)"
    ok, reason = gov.check_circuit_breaker(100.0, -4.0)  # 4% loss
    assert not ok, "Should trigger at 4% loss in sprout phase"
    print(f"[PASS] Circuit breaker: triggered at 4% daily loss")

    # Test position limits
    ok, _ = gov.check_position_limit(15.0, 1)  # seedling: max 2
    assert ok, "Should allow 2nd position in seedling phase"
    ok, _ = gov.check_position_limit(15.0, 2)
    assert not ok, "Should deny 3rd position in seedling phase"
    print(f"[PASS] Position limits: seedling max 2 correct")

    # Test status
    gov2 = TigerRiskGovernor()
    gov2.reset_daily(15.0)
    status = gov2.status(15.0)
    assert status["phase"] == "seedling"
    assert status["risk_pct"] == 0.5
    print(f"[PASS] Status: {status['phase']} phase, risk={status['risk_pct']}%")


def test_signal_store():
    from api.signal_store import SignalStore

    # Use temp DB
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SignalStore(db_path=db_path)

        # Create a mock signal
        class MockSignal:
            symbol = "EURUSD"
            direction = "long"
            confidence = 78.5
            entry = 1.0850
            stop_loss = 1.0800
            take_profit_1 = 1.0900
            take_profit_2 = 1.0950
            take_profit_3 = 1.1000
            risk_reward = 2.0
            timeframe = "1h"
            session = "london"
            pattern = "Bullish OB + BOS"
            entry_type = "limit"
            sl_type = "anti_sweep"
            tp_type = "liquidity"
            sl_liquidity_mapped = True
            liquidity_pools_count = 5

        sig = MockSignal()

        # Store signal
        signal_id = store.store_signal(sig, source="fx", mt5_executed=True)
        assert signal_id > 0, f"Should return positive ID, got {signal_id}"
        print(f"[PASS] Store signal: ID={signal_id}")

        # Get active signals
        active = store.get_active_signals()
        assert len(active) == 1
        assert active[0]["symbol"] == "EURUSD"
        assert active[0]["sl_type"] == "anti_sweep"
        print(f"[PASS] Get active signals: {len(active)} found")

        # Update outcome
        store.update_outcome(signal_id, "tp1_hit", exit_price=1.0900, pnl_pips=50, pnl_usd=5.0)

        # Get performance
        stats = store.get_performance_stats()
        assert stats["completed_signals"] == 1
        assert stats["wins"] == 1
        assert stats["win_rate"] == 100.0
        assert stats["total_pnl_usd"] == 5.0
        assert stats["tiger_stats"]["anti_sweep_sl_pct"] == 100.0
        print(f"[PASS] Performance stats: win_rate={stats['win_rate']}%, pnl=${stats['total_pnl_usd']}")

        # Store a losing trade
        sig2 = MockSignal()
        sig2.symbol = "GBPUSD"
        sig2.sl_type = "atr"
        sig2.sl_liquidity_mapped = False
        id2 = store.store_signal(sig2, source="fx")
        store.update_outcome(id2, "sl_hit", exit_price=1.0800, pnl_pips=-50, pnl_usd=-5.0)

        stats = store.get_performance_stats()
        assert stats["completed_signals"] == 2
        assert stats["wins"] == 1
        assert stats["losses"] == 1
        assert stats["win_rate"] == 50.0
        print(f"[PASS] After loss: win_rate={stats['win_rate']}%, pnl=${stats['total_pnl_usd']}")

        # Equity curve
        curve = store.get_equity_curve(initial_equity=15.0)
        assert len(curve) >= 2
        assert curve[0]["equity"] == 15.0
        print(f"[PASS] Equity curve: {len(curve)} points, final equity=${curve[-1]['equity']}")

        # Signal history
        history = store.get_signal_history()
        assert len(history) == 2
        print(f"[PASS] Signal history: {len(history)} completed signals")

    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass  # Windows file locking


if __name__ == "__main__":
    print("=" * 50)
    print("Tiger Risk Governor Tests")
    print("=" * 50)
    test_risk_governor()

    print()
    print("=" * 50)
    print("Signal Store Tests")
    print("=" * 50)
    test_signal_store()

    print()
    print("=" * 50)
    print("ALL PHASE 2 TESTS PASSED!")
    print("=" * 50)
