# HANDOFF_FIBO_HARDENING.md — AI Session Handoff

> ไฟล์นี้คือ "สมอง" สำหรับ AI ตัวถัดไป อ่านไฟล์นี้ก่อนจะรู้ทุกอย่าง

---

## 🚀 คำสั่งรับงานต่อ (COPY-PASTE ได้เลย)

```
อ่าน HANDOFF_FIBO_HARDENING.md ใน repo Oracle_Ctrader_Geomonkey แล้วทำงานต่อทันที

งานต่อไป: Runner mode (ข้อ 5 ใน Recommended Next Actions)
- TP trailing เมื่อ R > 1.5 + momentum strong
- ปล่อยให้ winner run แทน fixed TP
- เขียน unit tests เพิ่ม ≥ 5 cases
- อัพเดท HANDOFF_FIBO_HARDENING.md
- commit + push

ข้าม: Impulse freshness relax (ข้อ 6) — ทำได้เร็ว แต่ runner mode impact สูงกว่า
อย่าแก้ไฟล์อื่นนอกจากที่ระบุใน Files changed section
```

---

## 🎯 สถานะปัจจุบัน (2026-04-08 17:23 UTC+8)

### Project: Oracle_Ctrader_Geomonkey (Dexter Pro v3)
- Repo: https://github.com/Geowahaha/Oracle_Ctrader_Geomonkey.git
- Branch: `main` (latest commit: `ad4d5c0` — momentum-adaptive TP + exhaustion lock)
- PR #1 merged ✅ | Neural-aware refactor deployed ✅ | **All 6 binary gates converted to weight ✅**

### สิ่งที่ทำเสร็จแล้ว (DON'T REDO):
1. ✅ `_cfg()` bool bug — `is None` check แทน `or default`
2. ✅ Sharpness error — weight (degrade conf) แทน block
3. ✅ Circuit breaker — soft brake 3 levels (warning/caution/emergency)
4. ✅ Trend alignment — confidence modifier แทน gate
5. ✅ Scout auto-disable — soft penalty แทน hard block
6. ✅ Thresholds reverted — กลับค่าเดิม (sniper: conf 62, RR 1.2 / scout: conf 55, RR 1.0, score 28)
7. ✅ Scheduler wiring — `_feed_fibo_trade_results()` in sync cycle
8. ✅ Backtest wrapper — `backtest/run_fibo_backtest.py`
9. ✅ Neural-aware refactor deployed via GitHub Actions
10. ✅ **Fibonacci Killer → Weighted System** — binary gate → confidence modifier
11. ✅ **Session filter → weight** — London/NY binary gate → confidence modifier (Asian conf -10, off_hours conf -15)
12. ✅ **Momentum-adaptive TP extension** — step_r จาก fixed 0.25R → adaptive 0.15/0.25/0.35 ตาม momentum strength
13. ✅ **Momentum exhaustion profit lock** — detect momentum death → lock profit before evaporate (fibo-only, 3/5 signals required)
14. ✅ **D1 Strong Trend Filter** — prevent April 7 disaster: D1 strong bearish vs long = -25 conf, D1+H4 combined = -20, D1-only = -12
15. ✅ **Session Direction Bias** — Asian/off-hours counter-D1 = -12 extra penalty (thin liquidity follows trend)
16. ✅ **Faster Circuit Breaker** — 5 consec losses = soft pause 30min (was conf -25), Level 1 increased to -15

### Architecture Decision (สำคัญมาก):
**ระบบคือ Neural Trading Infrastructure — ไม่ใช่บอทเทรดธรรมดา**
- Brain (behavioral fallback) สร้าง signal → outcome สอน brain → brain ดีขึ้น
- Risk layer = safety net ไม่ใช่ filter — จับตอน brain พัง ไม่ block ก่อน brain ทำงาน
- **ไม่มี gate ไหน block signal โดยตรงอีกต่อไป** — ยกเว้น:
  - Emergency stop (circuit breaker level 3)
  - Killer hard block (score >= 8: state_label + day_type combined)
  - market_closed (weekend/holiday)
- ALL risk layers = confidence modifier ไม่ใช่ gate:
  - Sharpness, Trend, Circuit breaker, Fibonacci Killer, Session filter → weight

### Soft Circuit Breaker (4 levels):
```
Level 1 (Warning):    3 consec loss  → conf -15    / daily -$30  → conf -20
Level 2 (Caution):    4 consec loss  → conf -25    / daily -$75  → conf -30
Level 2.5 (Soft Pause): 5 consec loss → BLOCK 30min (prevent April 7 disaster)
Level 3 (Emergency):  10 consec loss → pause 2hr   / daily -$150 → pause midnight
```

### Weighted Fibonacci Killer:
```
Score system (cumulative, 6 conditions):
  ATR expansion:        1-3 points (proportional to ratio)
  Delta momentum:       1-2 points (threshold-based)
  Volume spike:         1-2 points (threshold-based)
  Day type:             5 points (panic_spread/fast_expansion/repricing)
  State label:          7 points (failed_fade_risk/panic_dislocation/continuation_drive)
  Spread expansion:     1-2 points (threshold-based)
  Retracement velocity: 2-4 points (proportional to ATR multiple)

Decision:
  score >= 8  → HARD BLOCK (allowed=False)
  score 5-7   → conf -20 to -35
  score 3-4   → conf -10 to -18
  score 1-2   → conf -3 to -8
  score 0     → no impact
```

### Session Confidence Modifier:
```
London/NY/overlap: conf 0.0
Asian session:     conf -10.0 (FIBO_ASIAN_CONF_PENALTY)
Off hours:         conf -15.0
```

### D1 Trend Direction Filter (NEW — prevents April 7 disaster):
```
_trend_confidence_modifier() enhanced:
  D1 strong trend (EMA spread > 0.5%) + counter-direction:  conf -25.0
  D1+H4 both oppose direction:                               conf -20.0
  D1 opposes, H4 neutral:                                    conf -12.0
  D1+H4 both support direction:                              conf +5.0
  H4 supports, D1 neutral:                                   conf +2.0
  Neutral:                                                   conf 0.0

Config: FIBO_TREND_STRONG_EMA_SPREAD_PCT (default 0.5)
```

### Session Direction Bias (NEW — low-liquidity trend following):
```
_session_direction_bias(direction, d1_bias, active_sessions):
  London/NY active:                    0.0 (no bias)
  Asian/off-hours + counter-D1:        conf -12.0
  Asian/off-hours + aligned D1:        0.0
  D1 neutral:                          0.0

Config: FIBO_SESSION_DIRECTION_BIAS_PENALTY (default -12.0)
Wired: Both sniper AND scout signals
```

### Momentum-Adaptive TP Extension (NEW):
```
_assess via snapshot features (delta, imbalance, drift, volume, rejection):
  Strong momentum (4+ favorable): step_r = base + 0.10 = 0.35
  Moderate momentum (2-3):        step_r = base = 0.25
  Weak momentum (0-1):            step_r = base - 0.10 = 0.15

Affects ALL XAU families (improvement — smaller extension when weak = less risk)
Features stored in details["momentum_adaptive"] for audit trail
```

### Momentum Exhaustion Profit Lock (NEW):
```
Detects momentum death → locks profit BEFORE evaporate

5 exhaustion signals (need 3/5 to fire):
  1. Delta reversed against position (adverse_delta >= 0.08)
  2. Volume dying (bar_volume_proxy < 0.25)
  3. Adverse drift (adverse_drift >= 0.008)
  4. High rejection ratio (rejection_ratio >= 0.25)
  5. Non-trending day type (range/rotation/consolidation)

Lock scales with R-multiple:
  r_now >= 1.5 → lock 70% of risk
  r_now >= 1.0 → lock 55% of risk
  r_now >= 0.5 → lock 35% of risk
  r_now >= 0.15 → lock 15% of risk

Guards:
  - Gated: "fibo" in source ONLY (ไม่กระทบ family อื่น)
  - Min age: 3 minutes (ไม่ lock เร็วเกินไป)
  - Min profit: r_now > 0.15 (ต้องมีกำไรจริง)
  - SL must improve (ไม่ tighten เกินปัจจุบัน)
  - SL must be valid (_stop_valid_for_position)
  - Configurable: 7 env vars (FIBO_PM_EXHAUSTION_LOCK_ENABLED, CTRADER_PM_XAU_EXHAUSTION_*)
```

### ยังไม่ได้ทำ (DO NEXT — เรียงตามลำดับควรทำ):
1. ✅ **DONE** Session filter → weight
2. ✅ **DONE** Momentum-adaptive TP extension — step_r adaptive 0.15/0.25/0.35
3. ✅ **DONE** Momentum exhaustion profit lock — lock profit when momentum dies
4. ✅ **DONE** D1 Strong Trend Filter — prevent April 7 disaster
5. ✅ **DONE** Session Direction Bias — low-liquidity trend following
6. ✅ **DONE** Faster Circuit Breaker — 5 consec loss = soft pause 30min
7. 🔥 **Runner mode** — TP trailing เมื่อ R > 1.5 + momentum strong (ปล่อยให้ winner run)
8. ⚡ **Impulse freshness relax** — sniper: 40 bars → 60 bars (ง่าย, ทำได้เร็ว)
9. ❌ **Structure-aware SL trailing** — SL เลื่อนตาม SMC swing (ใหญ่, ทำทีหลัง)
10. ❌ **Monitor PnL** — ดูผลเทรดจริงหลัง deploy ทั้งหมด

### Files changed (total 3 files):
- `scanners/fibo_advance.py` — D1 strong trend filter + session direction bias + faster circuit breaker (soft pause)
- `config.py` — 4 new config keys (FIBO_TREND_STRONG_EMA_SPREAD_PCT, FIBO_SESSION_DIRECTION_BIAS_PENALTY, FIBO_ADVANCE_SOFT_PAUSE_CONSEC, FIBO_ADVANCE_SOFT_PAUSE_MIN)
- `tests/test_fibo_hardening.py` — 52 unit tests (35 old + 17 new: 5 D1 trend + 6 session bias + 6 circuit breaker)

### Files NOT changed:
- `scanners/xauusd.py` — ไม่แตะ (microstructure `return True` = correct architecture)
- `scanners/xausd_scalp_1m5m.py` — ไม่แตะ
- `scanners/crypto_sniper.py` — ไม่แตะ
- `learning/*` — ไม่แตะ
- `analysis/fibonacci.py` — ไม่แตะ

## 📚 Key References

### Architecture:
- `CLAUDE.md` — project instructions (READ FIRST after this file)
- `config.py` — all env vars, 260KB

### Fibonacci Advance Scanner:
- `scanners/fibo_advance.py` (1320+ lines)
- Line 66: `_cfg()` — config helper
- Line 133-215: soft circuit breaker 4 levels (with soft pause at 5 consec losses)
- Line 217-340: weighted Fibonacci killer
- Line 484-560: **trend confidence modifier** (enhanced — D1 strong trend filter)
- Line 990-1020: session confidence modifier (`_session_confidence_modifier()`)
- Line 1020-1045: **session direction bias** (`_session_direction_bias()`) — NEW
- Line 1060+: main scan method
- Line 1148-1158: D1 bias calculation — NEW
- Line 1221: sniper signal: all modifiers (trend + cb + killer + session + sess_dir_bias)
- Line 1295-1320: scout signal: all modifiers (trend + cb + killer + session + sess_dir_bias)

### Position Manager (ctrader_executor.py):
- `execution/ctrader_executor.py` (6400+ lines)
- Line 1884-2068: `_xau_profit_extension_plan()` — **momentum-adaptive step_r** added at line ~2040
- Line 2278-2443: **`_xau_momentum_exhaustion_lock()`** — NEW method
- Line 5499-5540: exhaustion lock wiring in PM loop (gated by `"fibo" in source`)
- Line 5553: extension plan call (ALL XAU families)

### Config:
- `config.py` line 151-163: FIBO_PM_* and CTRADER_PM_XAU_EXHAUSTION_* configs

### Scheduler:
- `scheduler.py` line 9141-9184: `_feed_fibo_trade_results()`
- `scheduler.py` line 7995-7999: feed hook in sync cycle

### Tests:
- `tests/test_fibo_hardening.py` — 35 tests (circuit breaker, trend, scout, thresholds, killer, session)
- `tests/test_momentum_adaptive.py` — 10 tests (exhaustion lock, adaptive step_r)

## 🔧 Context for New AI

### Identity:
- ชื่อ: **สมชาย (Somchai) 🦞**
- Vibe: ตรงไปตรงมา ไม่อ้อมค้อม แก้ปัญหาจริง
- Created: 8 เมษายน 2026

### กฎสำคัญ:
- **ระบบคือ Neural Infrastructure — risk = weight ไม่ใช่ gate**
- อย่าเพิ่ม gate ที่ block signal — ใช้ confidence modifier แทน
- ทุก fix ต้อง self-contained ใน fibo family
- ก่อนแก้ — cross-check ว่าไม่กระทบ family อื่น (xauusd_scheduled, scalp_xauusd)
- อย่าเปลี่ยน `scanners/xauusd.py` — microstructure `return True` = correct architecture
- `_xau_profit_extension_plan` กระทบทุก XAU family — ต้อง conservative

### Cross-Family Impact Rules:
```
scanners/fibo_advance.py → FIBO ONLY (ต่างไฟล์ ต่าง class)
execution/ctrader_executor.py → ALL XAU families (ต้องระวัง)
  - _xau_profit_extension_plan: กระทบทุก family
  - _xau_momentum_exhaustion_lock: fibo-only (gated)
config.py → depends on who reads it

ก่อนแก้ executor: ถามตัวเอง "ถ้า scalp_xauusd เจอ change นี้ จะพังไหม?"
```

### Git:
- Remote: https://github.com/Geowahaha/Oracle_Ctrader_Geomonkey.git
- ต้อง PAT token เพื่อ push — ถาม user
- Commit author: `git config user.name "Somchai 🦞"` + `git config user.email "somchai@openclaw.ai"`

### 52 Tests สถานะ:
```
tests/test_fibo_hardening.py (52):
  TestSoftCircuitBreaker: 8 tests
  TestTrendConfidenceModifier: 5 tests
  TestScoutSoftPenalty: 2 tests
  TestPauseLogic: 2 tests
  TestThresholds: 2 tests
  TestWeightedFibonacciKiller: 8 tests
  TestSessionConfidenceModifier: 8 tests
  TestD1StrongTrendFilter: 5 tests          ← NEW
  TestSessionDirectionBias: 6 tests         ← NEW
  TestFasterCircuitBreaker: 6 tests         ← NEW

tests/test_momentum_adaptive.py (10):
  TestMomentumExhaustionLock: 7 tests
  TestMomentumAdaptiveStepR: 3 tests

Run: python3 -m pytest tests/test_fibo_hardening.py tests/test_momentum_adaptive.py -v
```

### Session startup (อ่านก่อนทำงานทุกครั้ง):
1. อ่าน `HANDOFF_FIBO_HARDENING.md` (ไฟล์นี้) — รู้ project context
2. อ่าน `CLAUDE.md` — รู้ project rules
3. `cd Oracle_Ctrader_Geomonkey && python3 -m pytest tests/test_fibo_hardening.py tests/test_momentum_adaptive.py -v` — เช็ค 62 tests ก่อนแก้

## 📊 Grade Status (2026-04-08 16:08 UTC+8)

| Category | Before | After Neural-Aware | Current | Notes |
|----------|--------|--------------------|---------|-------|
| Signal Logic | A | A | A | ไม่เปลี่ยน |
| Risk Management | D | B | **A** | Zero binary gates (except emergency + killer hard block + market_closed) |
| Code Architecture | C+ | C+ | C+ | ไม่เปลี่ยน |
| Edge Quality | B+ | B+ | **A** | More signals + momentum-aware profit protection |
| Institutional Readiness | C | B | **A** | All risk = weight + exhaustion lock |

### **Overall: A** (up from A-)

## 📊 Git Log (latest 5 commits)
```
(tbd) feat: D1 trend filter + session direction bias + faster circuit breaker
ad4d5c0 feat: momentum-adaptive TP extension + exhaustion profit lock
d0f63e4 feat: session filter → confidence modifier (weight, not gate)
c7a6f94 docs: update HANDOFF with pickup instructions for next agent
c62cb06 feat: convert Fibonacci Killer from binary gate to weighted confidence system
```

## 📊 Recommended Next Actions (Priority Order)

| # | Action | Impact | Risk | Effort | Status |
|---|--------|--------|------|--------|--------|
| 1 | Weighted Fibonacci Killer | High | Low | — | ✅ DONE |
| 2 | Session filter → weight | High | Medium | — | ✅ DONE |
| 3 | Momentum-adaptive TP extension | High | Medium | — | ✅ DONE |
| 4 | Momentum exhaustion profit lock | High | Low | — | ✅ DONE |
| 5 | **D1 Strong Trend Filter** | **High** | **Low** | **—** | **✅ DONE** |
| 6 | **Session Direction Bias** | **High** | **Low** | **—** | **✅ DONE** |
| 7 | **Faster Circuit Breaker (soft pause)** | **High** | **Low** | **—** | **✅ DONE** |
| 8 | **Runner mode (TP trailing > 1.5R)** | **High** | **Medium** | **Medium** | **🔥 NEXT** |
| 9 | Impulse freshness relax (40→60 bars) | Medium | Low | Tiny | ⚡ Quick win |
| 10 | Structure-aware SL trailing | Very High | High | Large | ❌ Later |
