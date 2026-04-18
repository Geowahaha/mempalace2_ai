"""
scanners/fx_major_scanner.py - FX Major Scanner (Forex majors)
Scans major FX pairs (EURUSD, GBPUSD, USDJPY, AUDUSD, NZDUSD, USDCAD, USDCHF)
using the same signal engine as XAU/Crypto, with optional MT5 tradable prefilter.
"""
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from market.data_fetcher import fx_provider, session_manager
from analysis.technical import TechnicalAnalysis
from analysis.signals import SignalGenerator, TradeSignal
from config import config

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()
sig = SignalGenerator(min_confidence=config.FX_MIN_CONFIDENCE)


@dataclass
class FXOpportunity:
    signal: TradeSignal
    pair_group: str = "major"
    vol_vs_avg: float = 1.0
    setup_type: str = "TREND_CONT"

    @property
    def composite_score(self) -> float:
        setup_bonus = {
            "CHOCH": 10, "OB_BOUNCE": 8, "FVG_FILL": 6, "BB_SQUEEZE": 6,
            "DIVERGENCE": 5, "TREND_CONT": 3,
        }.get(self.setup_type, 0)
        vol_bonus = max(0.0, min(6.0, (float(self.vol_vs_avg or 1.0) - 1.0) * 4.0))
        return float(getattr(self.signal, 'confidence', 0.0)) + setup_bonus + vol_bonus


class FXMajorScanner:
    def __init__(self, max_workers: int = 6):
        self.max_workers = max_workers
        self.scan_count = 0
        self.total_signals = 0
        self._last_pairs: list[str] = []
        self._last_scan_diag: dict = {}

    @staticmethod
    def _detect_setup_type(signal: TradeSignal) -> str:
        pat = str(getattr(signal, 'pattern', '') or '').upper()
        if 'CHOCH' in pat:
            return 'CHOCH'
        if 'ORDER BLOCK' in pat or 'OB' in pat:
            return 'OB_BOUNCE'
        if 'FVG' in pat:
            return 'FVG_FILL'
        if 'SQUEEZE' in pat:
            return 'BB_SQUEEZE'
        if 'DIVERGENCE' in pat:
            return 'DIVERGENCE'
        return 'TREND_CONT'

    @staticmethod
    def _safe_vol_ratio(value, default: float = 1.0) -> float:
        try:
            v = float(value)
        except Exception:
            return default
        if (not math.isfinite(v)) or v <= 0:
            return default
        return v


    @staticmethod
    def _pip_size(symbol: str) -> float:
        su = str(symbol or "").upper()
        return 0.01 if su.endswith("JPY") else 0.0001

    def _apply_trade_location_guard(self, signal: TradeSignal, symbol: str, df_ta: pd.DataFrame) -> tuple[bool, dict]:
        if not bool(getattr(config, "FX_SMART_TRAP_GUARD_ENABLED", True)):
            return True, {"enabled": False}
        out = {"enabled": True, "blocked": False, "penalty": 0.0, "reasons": [], "notes": []}
        try:
            if df_ta is None or len(df_ta) < 5:
                return True, out
            last = df_ta.iloc[-1]
            o = float(last.get("open", 0) or 0)
            h = float(last.get("high", 0) or 0)
            l = float(last.get("low", 0) or 0)
            c = float(last.get("close", 0) or 0)
            if c <= 0 or h <= 0 or l <= 0:
                return True, out
            direction = str(getattr(signal, 'direction', '') or '').lower()
            if direction not in {"long", "short"}:
                return True, out
            price = float(getattr(signal, 'entry', 0) or c)
            atr = float(getattr(signal, 'atr', 0) or last.get('atr_14', 0) or 0)
            if atr <= 0:
                atr = max(self._pip_size(symbol) * 20.0, abs(price) * 0.001)
            ema21 = float(last.get('ema_21', c) or c)
            bb_pct = float(last.get('bb_pct', 0.5) or 0.5)
            pip = self._pip_size(symbol)

            no_chase_atr = float(getattr(config, 'FX_TRAP_NO_CHASE_EMA21_ATR', 0.9) or 0.9)
            no_chase_bb = float(getattr(config, 'FX_TRAP_NO_CHASE_BB_PCT', 0.88) or 0.88)
            round_pips_thr = float(getattr(config, 'FX_TRAP_ROUND_PIPS', 5.0) or 5.0)
            wick_ratio_thr = float(getattr(config, 'FX_TRAP_REJECTION_WICK_RATIO', 1.2) or 1.2)
            p_no_chase = float(getattr(config, 'FX_TRAP_PENALTY_NO_CHASE', 8.0) or 8.0)
            p_round = float(getattr(config, 'FX_TRAP_PENALTY_ROUND', 5.0) or 5.0)
            p_rej = float(getattr(config, 'FX_TRAP_PENALTY_REJECTION', 8.0) or 8.0)
            block_penalty = float(getattr(config, 'FX_TRAP_BLOCK_PENALTY', 16.0) or 16.0)

            penalty = 0.0
            ext_atr = 0.0
            if direction == 'long':
                ext_atr = (price - ema21) / max(1e-9, atr)
                if ext_atr >= no_chase_atr and bb_pct >= no_chase_bb:
                    penalty += p_no_chase
                    out['reasons'].append('no_chase')
                    out['notes'].append(f'stretched {ext_atr:.2f} ATR above EMA21')
            else:
                ext_atr = (ema21 - price) / max(1e-9, atr)
                if ext_atr >= no_chase_atr and bb_pct <= (1.0 - no_chase_bb):
                    penalty += p_no_chase
                    out['reasons'].append('no_chase')
                    out['notes'].append(f'stretched {ext_atr:.2f} ATR below EMA21')

            # Psychological round levels (00 / 50 pips)
            pips = price / max(1e-9, pip)
            dist00 = abs(pips - round(pips / 100.0) * 100.0)
            dist50 = abs(pips - (round((pips - 50.0) / 100.0) * 100.0 + 50.0))
            round_dist_pips = min(dist00, dist50)
            if round_dist_pips <= round_pips_thr:
                penalty += p_round
                out['reasons'].append('near_round')
                out['notes'].append(f'near 00/50 level ({round_dist_pips:.1f} pips)')

            body = max(abs(c - o), pip * 0.5)
            upper_wick = max(0.0, h - max(o, c))
            lower_wick = max(0.0, min(o, c) - l)
            if direction == 'long' and upper_wick / body >= wick_ratio_thr and c < (l + (h - l) * 0.6):
                penalty += p_rej
                out['reasons'].append('rejection_wick')
                out['notes'].append('upper rejection wick on trigger bar')
            if direction == 'short' and lower_wick / body >= wick_ratio_thr and c > (l + (h - l) * 0.4):
                penalty += p_rej
                out['reasons'].append('rejection_wick')
                out['notes'].append('lower rejection wick on trigger bar')

            out['penalty'] = round(float(penalty), 2)
            out['ext_atr'] = round(float(ext_atr), 3)
            out['bb_pct'] = round(float(bb_pct), 3)
            out['round_dist_pips'] = round(float(round_dist_pips), 2)

            if penalty > 0:
                try:
                    conf0 = float(getattr(signal, 'confidence', 0.0) or 0.0)
                    conf1 = max(1.0, conf0 - penalty)
                    signal.confidence = round(conf1, 1)
                    warnings = list(getattr(signal, 'warnings', []) or [])
                    if 'no_chase' in out['reasons']:
                        warnings.append(f'⚠️ FX no-chase: {out["notes"][0]}')
                    elif out['notes']:
                        warnings.append('⚠️ FX trap guard: ' + '; '.join(out['notes'][:2]))
                    signal.warnings = warnings[-8:]
                    raw = dict(getattr(signal, 'raw_scores', {}) or {})
                    raw['fx_trap_guard'] = dict(out)
                    raw['confidence_pre_fx_guard'] = round(conf0, 3)
                    raw['confidence_post_fx_guard'] = round(float(signal.confidence), 3)
                    signal.raw_scores = raw
                except Exception:
                    pass

            if penalty >= block_penalty or (('no_chase' in out['reasons']) and ('rejection_wick' in out['reasons'])):
                out['blocked'] = True
                return False, out
            return True, out
        except Exception as e:
            out['notes'].append(f'guard_exception:{e}')
            return True, out

    def analyze_single(self, symbol: str) -> Optional[FXOpportunity]:
        try:
            now_utc = datetime.now(timezone.utc)
            if now_utc.weekday() >= 5:  # weekend
                return None

            df_entry = fx_provider.fetch_ohlcv(symbol, config.FX_ENTRY_TF, bars=220)
            if df_entry is None or len(df_entry) < 50:
                return None
            df_trend = fx_provider.fetch_ohlcv(symbol, config.FX_TREND_TF, bars=140)
            if df_trend is None or len(df_trend) < 50:
                return None

            session_info = session_manager.get_session_info()
            signal = sig.score_signal(
                df_entry=df_entry,
                df_trend=df_trend,
                symbol=symbol,
                timeframe=config.FX_ENTRY_TF,
                session_info=session_info,
            )
            if signal is None:
                return None

            vol_ratio = 1.0
            try:
                df_ta = ta.add_all(df_entry.copy())
                vol_ratio = self._safe_vol_ratio(df_ta.iloc[-1].get('vol_ratio', 1.0), 1.0)
            except Exception:
                df_ta = df_entry.copy()
                vol_ratio = 1.0
            ok_guard, guard = self._apply_trade_location_guard(signal, symbol, df_ta)
            if not ok_guard:
                logger.info('[FX MAJOR] %s trap/no-chase guard blocked: %s', symbol, ','.join(list(guard.get('reasons') or [])[:3]) or 'guard')
                return None
            setup = self._detect_setup_type(signal)
            return FXOpportunity(signal=signal, vol_vs_avg=round(vol_ratio, 2), setup_type=setup)
        except Exception as e:
            logger.debug('fx analyze_single(%s): %s', symbol, e)
            return None

    def _scan_with_diag(self, symbols: list[str]) -> list[FXOpportunity]:
        reject = {"market_closed": 0, "no_entry_data": 0, "no_trend_data": 0, "no_signal": 0, "guard_blocked": 0, "exception": 0}
        opps: list[FXOpportunity] = []
        start = time.time()
        now_utc = datetime.now(timezone.utc)
        if now_utc.weekday() >= 5:
            reject["market_closed"] = len(symbols)
            self._last_scan_diag = {
                "symbols_input": len(symbols), "symbols": len(symbols), "reject_reasons": reject,
                "prefilter": {}, "elapsed": 0.0,
            }
            return []

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futs = {}
            for sym in symbols:
                futs[ex.submit(self._analyze_single_with_reason, sym)] = sym
            for fut in as_completed(futs):
                sym = futs[fut]
                try:
                    opp, reason = fut.result(timeout=45)
                    if opp is not None:
                        opps.append(opp)
                    elif reason in reject:
                        reject[reason] += 1
                    else:
                        reject["exception"] += 1
                except Exception:
                    reject["exception"] += 1
                    logger.debug('fx future error for %s', sym, exc_info=True)

        opps.sort(key=lambda o: o.composite_score, reverse=True)
        elapsed = time.time() - start
        self._last_scan_diag = {
            "symbols_input": len(symbols),
            "symbols": len(symbols),
            "reject_reasons": reject,
            "prefilter": getattr(self, '_last_prefilter', {}) or {},
            "elapsed": round(elapsed, 2),
        }
        return opps

    def _analyze_single_with_reason(self, symbol: str):
        try:
            now_utc = datetime.now(timezone.utc)
            if now_utc.weekday() >= 5:
                return None, 'market_closed'
            df_entry = fx_provider.fetch_ohlcv(symbol, config.FX_ENTRY_TF, bars=220)
            if df_entry is None or len(df_entry) < 50:
                return None, 'no_entry_data'
            df_trend = fx_provider.fetch_ohlcv(symbol, config.FX_TREND_TF, bars=140)
            if df_trend is None or len(df_trend) < 50:
                return None, 'no_trend_data'
            session_info = session_manager.get_session_info()
            signal = sig.score_signal(df_entry=df_entry, df_trend=df_trend, symbol=symbol, timeframe=config.FX_ENTRY_TF, session_info=session_info)
            if signal is None:
                return None, 'no_signal'
            try:
                df_ta = ta.add_all(df_entry.copy())
                vol_ratio = self._safe_vol_ratio(df_ta.iloc[-1].get('vol_ratio', 1.0), 1.0)
            except Exception:
                df_ta = df_entry.copy()
                vol_ratio = 1.0
            ok_guard, _guard = self._apply_trade_location_guard(signal, symbol, df_ta)
            if not ok_guard:
                return None, 'guard_blocked'
            opp = FXOpportunity(signal=signal, vol_vs_avg=round(vol_ratio, 2), setup_type=self._detect_setup_type(signal))
            return opp, None
        except Exception:
            return None, 'exception'

    def scan(self, symbols: Optional[list[str]] = None) -> list[FXOpportunity]:
        self.scan_count += 1
        if symbols is None:
            symbols = fx_provider.get_major_pairs()
        symbols = [str(s or '').upper() for s in (symbols or []) if str(s or '').strip()]
        self._last_pairs = list(symbols)
        prefilter = {"input": len(symbols), "kept": len(symbols), "unmapped": 0, "enabled": False, "connected": False, "error": ""}
        if bool(getattr(config, 'FX_SCANNER_MT5_TRADABLE_ONLY', True)) and bool(getattr(config, 'MT5_ENABLED', False)) and symbols:
            try:
                from execution.mt5_executor import mt5_executor
                filt = mt5_executor.filter_tradable_signal_symbols(symbols)
                prefilter.update({
                    "enabled": True,
                    "connected": bool(filt.get('connected', False)),
                    "error": str(filt.get('error') or ''),
                    "kept": len(filt.get('tradable', []) or []),
                    "unmapped": len(filt.get('unmapped', []) or []),
                })
                if bool(filt.get('ok')) and bool(filt.get('connected')):
                    symbols = [s for s in symbols if s in set(filt.get('tradable', []) or [])]
                    logger.info('[FX] MT5 broker-tradable filter: %s/%s pairs tradable (unmapped=%s)', len(symbols), prefilter['input'], prefilter['unmapped'])
                else:
                    logger.info('[FX] MT5 broker-tradable filter skipped: %s', prefilter['error'] or 'not connected')
            except Exception as e:
                prefilter['enabled'] = True
                prefilter['error'] = str(e)
                logger.warning('[FX] MT5 broker-tradable filter error: %s', e)
        self._last_prefilter = prefilter

        sess = session_manager.get_session_info()
        logger.info('[FX MAJOR] Scan #%s | Scanning %s pairs | Sessions: %s', self.scan_count, len(symbols), sess.get('active_sessions'))
        opps = self._scan_with_diag(symbols)
        self.total_signals += len(opps)
        logger.info('[FX MAJOR] ✅ Scan complete in %.1fs | Found %s opportunities from %s pairs', float((self._last_scan_diag or {}).get('elapsed', 0.0) or 0.0), len(opps), len(symbols))
        return opps

    def get_top_n(self, n: int = 5) -> list[FXOpportunity]:
        return self.scan()[:max(1, int(n))]

    def get_last_scan_diagnostics(self) -> dict:
        return dict(self._last_scan_diag or {})

    def get_stats(self) -> dict:
        return {
            'total_scans': self.scan_count,
            'total_signals': self.total_signals,
            'pairs_watched': len(self._last_pairs),
            'last_pairs': list(self._last_pairs),
        }


fx_major_scanner = FXMajorScanner()
