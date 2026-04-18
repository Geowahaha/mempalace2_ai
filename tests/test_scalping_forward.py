from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from learning.scalping_forward import ScalpingForwardAnalyzer


def _iso(dt: datetime) -> str:
    src = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_scalping_forward_report_ranks_best_pair(tmp_path):
    db_path = tmp_path / "mt5_autopilot.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE mt5_scalping_net_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            journal_id INTEGER NOT NULL UNIQUE,
            account_key TEXT NOT NULL,
            source TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            signal_symbol TEXT,
            broker_symbol TEXT,
            position_id INTEGER,
            ticket INTEGER,
            opened_at TEXT,
            closed_at TEXT,
            duration_min REAL,
            pnl_net_usd REAL,
            gross_profit REAL,
            swap REAL,
            commission REAL,
            close_reason TEXT,
            outcome INTEGER
        )
        """
    )

    now = datetime.now(timezone.utc)
    rows = [
        # XAUUSD net +8
        (1, "scalp_xauusd", "XAUUSD", 5.0, 1, 18.0),
        (2, "scalp_xauusd", "XAUUSD", 3.0, 1, 24.0),
        # ETH net +2
        (3, "scalp_ethusd", "ETH/USDT", 6.0, 1, 16.0),
        (4, "scalp_ethusd", "ETH/USDT", -4.0, 0, 22.0),
    ]
    for jid, source, symbol, pnl, outcome, dur in rows:
        closed = now - timedelta(hours=jid)
        opened = closed - timedelta(minutes=dur)
        conn.execute(
            """
            INSERT INTO mt5_scalping_net_log(
                created_at, journal_id, account_key, source, canonical_symbol, signal_symbol,
                broker_symbol, position_id, ticket, opened_at, closed_at, duration_min, pnl_net_usd,
                gross_profit, swap, commission, close_reason, outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _iso(now),
                jid,
                "server|1",
                source,
                symbol,
                symbol,
                symbol,
                jid,
                jid,
                _iso(opened),
                _iso(closed),
                dur,
                pnl,
                pnl + 0.5,
                -0.1,
                -0.4,
                "TP" if outcome == 1 else "SL",
                outcome,
            ),
        )
    conn.commit()
    conn.close()

    analyzer = ScalpingForwardAnalyzer(db_path=str(db_path))
    report = analyzer.build_report(days=7)
    assert report["ok"] is True
    assert report["rows"] == 4
    assert report["best_pair"] is not None
    assert report["best_pair"]["symbol"] == "XAUUSD"
    assert report["best_pair"]["pnl_net_usd"] == 8.0


def test_crypto_weekend_scorecard_recommends_symbol_specific_profiles(tmp_path):
    scalp_db_path = tmp_path / "scalp_signal_history.db"
    conn = sqlite3.connect(str(scalp_db_path))
    conn.execute(
        """
        CREATE TABLE scalp_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            symbol TEXT,
            session TEXT,
            confidence REAL,
            outcome TEXT,
            pnl_usd REAL
        )
        """
    )
    weekend = datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc).timestamp()
    weekday = datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc).timestamp()
    rows = [
        (weekend, "BTCUSD", "new_york", 74.0, "tp2_hit", 12.0),
        (weekend - 60, "BTCUSD", "new_york", 73.0, "tp2_hit", 10.0),
        (weekend - 120, "BTCUSD", "london, new_york, overlap", 74.0, "tp2_hit", 11.0),
        (weekend - 180, "BTCUSD", "london, new_york, overlap", 74.0, "sl_hit", -5.0),
        (weekday, "ETHUSD", "london, new_york, overlap", 73.0, "tp2_hit", 8.0),
        (weekday - 60, "ETHUSD", "london, new_york, overlap", 74.0, "tp2_hit", 7.0),
        (weekday - 120, "ETHUSD", "new_york", 76.0, "sl_hit", -18.0),
        (weekday - 180, "ETHUSD", "new_york", 76.0, "sl_hit", -16.0),
        (weekday - 240, "ETHUSD", "new_york", 77.0, "sl_hit", -20.0),
    ]
    conn.executemany(
        "INSERT INTO scalp_signals(timestamp,symbol,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()

    analyzer = ScalpingForwardAnalyzer(db_path=str(tmp_path / "mt5_autopilot.db"), scalp_db_path=str(scalp_db_path))
    report = analyzer.build_crypto_weekend_scorecard(days=14)
    assert report["ok"] is True
    btc = next(x for x in report["symbols"] if x["symbol"] == "BTCUSD")
    eth = next(x for x in report["symbols"] if x["symbol"] == "ETHUSD")
    assert "new_york" in btc["recommended_weekend_profile"]["allowed_sessions"]
    assert "london,new_york,overlap" in eth["recommended_weekend_profile"]["allowed_sessions"]
    assert eth["recommended_weekend_profile"]["source"] == "weekday_proxy"


def test_crypto_weekend_scorecard_ignores_untracked_ctrader_rows(tmp_path):
    scalp_db_path = tmp_path / "scalp_signal_history.db"
    scalp_conn = sqlite3.connect(str(scalp_db_path))
    scalp_conn.execute(
        """
        CREATE TABLE scalp_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            symbol TEXT,
            session TEXT,
            confidence REAL,
            outcome TEXT,
            pnl_usd REAL
        )
        """
    )
    now_ts = datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc).timestamp()
    scalp_conn.execute(
        "INSERT INTO scalp_signals(timestamp,symbol,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?)",
        (now_ts, "ETHUSD", "london, new_york, overlap", 77.0, "tp2_hit", 8.0),
    )
    scalp_conn.commit()
    scalp_conn.close()

    ctrader_db_path = tmp_path / "ctrader_openapi.db"
    ctrader_conn = sqlite3.connect(str(ctrader_db_path))
    ctrader_conn.execute(
        """
        CREATE TABLE ctrader_deals (
            deal_id INTEGER PRIMARY KEY,
            account_id INTEGER,
            position_id INTEGER,
            order_id INTEGER,
            source TEXT,
            lane TEXT,
            symbol TEXT,
            broker_symbol TEXT,
            direction TEXT,
            volume REAL,
            execution_price REAL,
            gross_profit_usd REAL,
            swap_usd REAL,
            commission_usd REAL,
            pnl_conversion_fee_usd REAL,
            pnl_usd REAL,
            outcome INTEGER,
            has_close_detail INTEGER,
            signal_run_id TEXT,
            signal_run_no INTEGER,
            journal_id INTEGER,
            execution_utc TEXT,
            raw_json TEXT
        )
        """
    )
    ctrader_conn.executemany(
        """
        INSERT INTO ctrader_deals(
            deal_id, account_id, position_id, order_id, source, lane, symbol, broker_symbol,
            direction, volume, execution_price, gross_profit_usd, swap_usd, commission_usd,
            pnl_conversion_fee_usd, pnl_usd, outcome, has_close_detail, signal_run_id, signal_run_no,
            journal_id, execution_utc, raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                1, 1, 100, 200, "scalp_ethusd", "main", "ETHUSD", "ETHUSD",
                "long", 1.0, 2000.0, 9.0, 0.0, 0.0, 0.0, 9.0, 1, 1, "run", 7, 123,
                "2026-03-07T12:10:00Z", "{}"
            ),
            (
                2, 1, 101, 201, "scalp_ethusd", "main", "ETHUSD", "ETHUSD",
                "long", 1.0, 1990.0, -7.0, 0.0, 0.0, 0.0, -7.0, 0, 1, "", 8, None,
                "2026-03-07T12:20:00Z", "{}"
            ),
        ],
    )
    ctrader_conn.commit()
    ctrader_conn.close()

    analyzer = ScalpingForwardAnalyzer(
        db_path=str(tmp_path / "mt5_autopilot.db"),
        scalp_db_path=str(scalp_db_path),
        ctrader_db_path=str(ctrader_db_path),
    )
    report = analyzer.build_crypto_weekend_scorecard(days=14)
    assert report["ok"] is True
    assert report["ctrader_live_rows"] == 1
    eth = next(x for x in report["symbols"] if x["symbol"] == "ETHUSD")
    assert eth["ctrader_live"]["resolved"] == 1
    assert eth["ctrader_live"]["wins"] == 1
    assert eth["ctrader_live"]["losses"] == 0


def test_winner_mission_report_recommends_winner_only_and_limit_bias(tmp_path):
    scalp_db_path = tmp_path / "scalp_signal_history.db"
    scalp_conn = sqlite3.connect(str(scalp_db_path))
    scalp_conn.execute(
        """
        CREATE TABLE scalp_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            symbol TEXT,
            session TEXT,
            confidence REAL,
            outcome TEXT,
            pnl_usd REAL
        )
        """
    )
    now_ts = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc).timestamp()
    scalp_conn.executemany(
        "INSERT INTO scalp_signals(timestamp,symbol,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?)",
        [
            (now_ts, "BTCUSD", "new_york", 76.0, "tp2_hit", 8.0),
            (now_ts - 60, "BTCUSD", "new_york", 77.0, "tp2_hit", 7.0),
            (now_ts - 120, "XAUUSD", "london,new_york,overlap", 79.0, "tp2_hit", 6.0),
            (now_ts - 180, "XAUUSD", "london,new_york,overlap", 80.0, "tp2_hit", 5.0),
        ],
    )
    scalp_conn.commit()
    scalp_conn.close()

    ctrader_db_path = tmp_path / "ctrader_openapi.db"
    ctrader_conn = sqlite3.connect(str(ctrader_db_path))
    ctrader_conn.execute(
        """
        CREATE TABLE execution_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_ts REAL,
            created_utc TEXT,
            source TEXT,
            lane TEXT,
            symbol TEXT,
            direction TEXT,
            confidence REAL,
            entry REAL,
            stop_loss REAL,
            take_profit REAL,
            entry_type TEXT,
            dry_run INTEGER,
            account_id INTEGER,
            broker_symbol TEXT,
            volume REAL,
            status TEXT,
            message TEXT,
            order_id INTEGER,
            position_id INTEGER,
            deal_id INTEGER,
            signal_run_id TEXT,
            signal_run_no INTEGER,
            request_json TEXT,
            response_json TEXT,
            execution_meta_json TEXT
        )
        """
    )
    ctrader_conn.execute(
        """
        CREATE TABLE ctrader_deals (
            deal_id INTEGER PRIMARY KEY,
            account_id INTEGER,
            position_id INTEGER,
            order_id INTEGER,
            source TEXT,
            lane TEXT,
            symbol TEXT,
            broker_symbol TEXT,
            direction TEXT,
            volume REAL,
            execution_price REAL,
            gross_profit_usd REAL,
            swap_usd REAL,
            commission_usd REAL,
            pnl_conversion_fee_usd REAL,
            pnl_usd REAL,
            outcome INTEGER,
            has_close_detail INTEGER,
            signal_run_id TEXT,
            signal_run_no INTEGER,
            journal_id INTEGER,
            execution_utc TEXT,
            raw_json TEXT
        )
        """
    )
    ctrader_conn.executemany(
        """
        INSERT INTO execution_journal(
            id, created_ts, created_utc, source, lane, symbol, direction, confidence, entry,
            stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol, volume,
            status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
            request_json, response_json, execution_meta_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (1, 1.0, "2026-03-09 10:00:00", "scalp_btcusd", "main", "BTCUSD", "long", 74.0, 67000.0, 66800.0, 67200.0, "market", 0, 1, "BTCUSD", 1.0, "closed", "x", 101, 201, 301, "run1", 1, "{\"session\":\"new_york\"}", "{}", "{}"),
            (2, 2.0, "2026-03-09 10:05:00", "scalp_btcusd", "main", "BTCUSD", "long", 75.0, 67100.0, 66900.0, 67300.0, "market", 0, 1, "BTCUSD", 1.0, "closed", "x", 102, 202, 302, "run2", 2, "{\"session\":\"new_york\"}", "{}", "{}"),
            (3, 3.0, "2026-03-09 10:10:00", "xauusd_scheduled:winner", "winner", "XAUUSD", "long", 79.0, 5100.0, 5090.0, 5110.0, "limit", 0, 1, "XAUUSD", 1.0, "closed", "x", 103, 203, 303, "run3", 3, "{\"session\":\"london,new_york,overlap\"}", "{}", "{}"),
            (4, 4.0, "2026-03-09 10:15:00", "xauusd_scheduled:winner", "winner", "XAUUSD", "long", 80.0, 5101.0, 5091.0, 5111.0, "limit", 0, 1, "XAUUSD", 1.0, "closed", "x", 104, 204, 304, "run4", 4, "{\"session\":\"london,new_york,overlap\"}", "{}", "{}"),
        ],
    )
    ctrader_conn.executemany(
        """
        INSERT INTO ctrader_deals(
            deal_id, account_id, position_id, order_id, source, lane, symbol, broker_symbol,
            direction, volume, execution_price, gross_profit_usd, swap_usd, commission_usd,
            pnl_conversion_fee_usd, pnl_usd, outcome, has_close_detail, signal_run_id, signal_run_no,
            journal_id, execution_utc, raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (301, 1, 201, 101, "scalp_btcusd", "main", "BTCUSD", "BTCUSD", "long", 1.0, 67010.0, -2.0, 0.0, 0.0, 0.0, -2.0, 0, 1, "run1", 1, 1, "2026-03-09T10:02:00Z", "{}"),
            (302, 1, 202, 102, "scalp_btcusd", "main", "BTCUSD", "BTCUSD", "long", 1.0, 67110.0, -1.5, 0.0, 0.0, 0.0, -1.5, 0, 1, "run2", 2, 2, "2026-03-09T10:07:00Z", "{}"),
            (303, 1, 203, 103, "xauusd_scheduled:winner", "winner", "XAUUSD", "XAUUSD", "long", 1.0, 5108.0, 4.0, 0.0, 0.0, 0.0, 4.0, 1, 1, "run3", 3, 3, "2026-03-09T10:12:00Z", "{}"),
            (304, 1, 204, 104, "xauusd_scheduled:winner", "winner", "XAUUSD", "XAUUSD", "long", 1.0, 5109.0, 3.0, 0.0, 0.0, 0.0, 3.0, 1, 1, "run4", 4, 4, "2026-03-09T10:17:00Z", "{}"),
        ],
    )
    ctrader_conn.commit()
    ctrader_conn.close()

    analyzer = ScalpingForwardAnalyzer(
        db_path=str(tmp_path / "mt5_autopilot.db"),
        scalp_db_path=str(scalp_db_path),
        ctrader_db_path=str(ctrader_db_path),
    )
    report = analyzer.build_winner_mission_report(days=14)
    assert report["ok"] is True
    btc = next(x for x in report["symbols"] if x["symbol"] == "BTCUSD")
    xau = next(x for x in report["symbols"] if x["symbol"] == "XAUUSD")
    assert btc["recommended_live_mode"] == "winner_only"
    assert xau["entry_bias"] == "limit_priority"
