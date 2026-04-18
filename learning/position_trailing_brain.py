"""
learning/position_trailing_brain.py
Neural action-value trailing stop engine.

MISSION: Use real-time microstructure (100-tick bars, order flow) to predict 
the optimal profit-lock R. 

PHASE 1: Aggressive Stepped Heuristic (Forced Live Improvement) + Neural Shadow.
PHASE 2: Online Supervised Learning (Transitioning to Neural Active).
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import uuid
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from config import config

logger = logging.getLogger(__name__)


@dataclass
class TrailingDecision:
    decision_id: str
    should_move: bool
    trail_lock_r: float
    mode: str  # "heuristic_active" or "neural_shadow"
    diagnostics: dict


class PositionTrailingBrain:
    """
    Centralized trailing stop brain.
    Moving away from hardcoded if/else toward learned continuous prediction.
    """

    def __init__(self, db_path: str | None = None, model_dir: str | None = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path or (data_dir / "signal_learning.db"))
        self.model_dir = Path(model_dir or (data_dir / "neural_models"))
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()
        
        # Neural Model: Linear Regressor (8 features)
        self.weights_path = self.model_dir / "trailing_weights_v1.npy"
        self.weights = self._load_weights()
        
        # ACTIVE LOGIC: Aggressive Stepped Heuristic (FORCED LIVE IMPROVEMENT)
        self._active_steps = [
            (1.80, 1.30, "runner_aggressive"),
            (1.20, 0.85, "major_aggressive"),
            (0.80, 0.55, "mid_aggressive"),
            (0.22, 0.20, "be_plus_aggressive"),
        ]

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _init_db(self):
        with self._lock:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trailing_decisions (
                        decision_id TEXT PRIMARY KEY,
                        position_id INTEGER,
                        symbol TEXT,
                        family TEXT,
                        created_at TEXT,

                        -- Real-time context features (X)
                        r_now REAL,
                        time_in_trade_minutes REAL,
                        vwap_slope REAL,
                        tick_velocity REAL,
                        depth_imbalance REAL,
                        vol_regime_ratio REAL,
                        session_overlap_flag REAL,
                        is_canary REAL,

                        -- Output prediction / decision (Y')
                        predicted_lock_r REAL,
                        heuristic_lock_r REAL,
                        neural_lock_r REAL,
                        decision_mode TEXT,

                        -- Retroactive audit labels (Y)
                        optimal_lock_r REAL,
                        final_trade_r REAL,
                        choked_runner INTEGER
                    )
                """)

    def _load_weights(self) -> np.ndarray:
        if self.weights_path.exists():
            try: return np.load(str(self.weights_path))
            except Exception: pass
        w = np.zeros(8)
        w[0] = 0.5 # Default bias: 50% R trailing
        w[7] = 0.01 
        return w

    def _save_weights(self):
        try: np.save(str(self.weights_path), self.weights)
        except Exception as e: logger.error(f"[PositionTrailingBrain] weights save error: {e}")

    def _get_feature_vector(self, features: dict) -> np.ndarray:
        return np.array([
            features["r_now"],
            min(features["time_in_trade_minutes"] / 120.0, 1.0),
            features["vwap_slope"] * 100.0,
            features["tick_velocity"],
            features["depth_imbalance"],
            features["vol_regime_ratio"],
            features["session_overlap_flag"],
            1.0 # Bias
        ])

    def _predict_heuristic_lock_r(self, r_now: float) -> tuple[float, str]:
        """AGGRESSIVE STEPPED RULES: Forced Profit Protection."""
        for thresh, lock, lbl in self._active_steps:
            if r_now >= thresh:
                return lock, lbl
        return 0.0, "hold"

    def _predict_neural_lock_r(self, features: dict) -> float:
        """Linear inference (Shadow)."""
        X = self._get_feature_vector(features)
        return float(np.dot(X, self.weights))

    def get_trailing_decision(self, state: dict) -> TrailingDecision:
        decision_id = str(uuid.uuid4())
        position_id = state.get("position_id", 0)
        symbol = str(state.get("symbol", "UNKNOWN"))
        family = str(state.get("family", "other"))

        r_now = float(state.get("r_now", 0.0))
        features = {
            "r_now": r_now,
            "time_in_trade_minutes": float(state.get("time_in_trade_minutes", 0.0)),
            "vwap_slope": float(state.get("vwap_slope_100t", 0.0)),
            "tick_velocity": float(state.get("tick_velocity", 0.0)),
            "depth_imbalance": float(state.get("depth_imbalance", 0.0)),
            "vol_regime_ratio": float(state.get("vol_regime_ratio", 1.0)),
            "session_overlap_flag": float(state.get("session_overlap_flag", 0.0)),
            "is_canary": 1.0 if "canary" in str(state.get("source_lane", "")) else 0.0
        }

        # 1. Active: Heuristic (Aggressive)
        heuristic_r, h_label = self._predict_heuristic_lock_r(r_now)
        
        # 2. Shadow: Neural Prediction
        neural_r = self._predict_neural_lock_r(features)
        
        mode = "heuristic_active"
        final_lock_r = heuristic_r
        should_move = bool(final_lock_r > 0)

        # 3. Log Features + Decision
        try:
            with self._lock:
                with sqlite3.connect(str(self.db_path)) as conn:
                    conn.execute("""
                        INSERT INTO trailing_decisions (
                            decision_id, position_id, symbol, family, created_at,
                            r_now, time_in_trade_minutes, vwap_slope, tick_velocity,
                            depth_imbalance, vol_regime_ratio, session_overlap_flag, is_canary,
                            predicted_lock_r, heuristic_lock_r, neural_lock_r, decision_mode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        decision_id, position_id, symbol, family, self._utc_now_iso(),
                        features["r_now"], features["time_in_trade_minutes"], 
                        features["vwap_slope"], features["tick_velocity"],
                        features["depth_imbalance"], features["vol_regime_ratio"],
                        features["session_overlap_flag"], features["is_canary"],
                        final_lock_r, heuristic_r, neural_r, mode
                    ))
        except Exception as e:
            logger.error(f"[PositionTrailingBrain] DB log error: {e}")

        # Heavy Audit Log
        logger.info(
            f"[TRAIL DECISION] {mode.upper()} | r_now={r_now:.2f} -> lock_r={final_lock_r:.2f} | "
            f"neural_pred={neural_r:.2f} | h_label={h_label}"
        )

        return TrailingDecision(
            decision_id=decision_id,
            should_move=should_move,
            trail_lock_r=final_lock_r,
            mode=mode,
            diagnostics={"h_label": h_label, "features": features}
        )

    def update_from_closed_trade(self, position_id: int, final_trade_r: float, optimal_lock_r: float, choked: bool):
        """FEEDBACK LOOP: Updates historical decisions with TRUE labels."""
        choked_int = 1 if choked else 0
        try:
            with self._lock:
                with sqlite3.connect(str(self.db_path)) as conn:
                    conn.execute("""
                        UPDATE trailing_decisions
                        SET optimal_lock_r = ?, final_trade_r = ?, choked_runner = ?
                        WHERE position_id = ? AND optimal_lock_r IS NULL
                    """, (float(optimal_lock_r), float(final_trade_r), int(choked_int), position_id))
            logger.info(f"[TRAIL FEEDBACK] Position {position_id} resolved: Final={final_trade_r:.2f}R | Optimal={optimal_lock_r:.2f}R | Choked={choked}")
        except Exception as e:
            logger.error(f"[PositionTrailingBrain] FB update error: {e}")

    def train_trailing_mlp(self, learning_rate: float = 0.01):
        """Supervised Online Learning Cycle (SGD)."""
        try:
            with self._lock:
                with sqlite3.connect(str(self.db_path)) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute("""
                        SELECT r_now, time_in_trade_minutes, vwap_slope, tick_velocity, 
                               depth_imbalance, vol_regime_ratio, session_overlap_flag,
                               optimal_lock_r, choked_runner
                        FROM trailing_decisions WHERE optimal_lock_r IS NOT NULL
                    """).fetchall()
            
            if len(rows) < 10: return
            for row in rows:
                features = {
                    "r_now": row["r_now"], "time_in_trade_minutes": row["time_in_trade_minutes"],
                    "vwap_slope": row["vwap_slope"], "tick_velocity": row["tick_velocity"],
                    "depth_imbalance": row["depth_imbalance"], "vol_regime_ratio": row["vol_regime_ratio"],
                    "session_overlap_flag": row["session_overlap_flag"]
                }
                X = self._get_feature_vector(features)
                y_pred = np.dot(X, self.weights)
                penalty = 3.0 if row["choked_runner"] else 1.0 # Force aggressive capture
                self.weights += learning_rate * penalty * (row["optimal_lock_r"] - y_pred) * X
            self._save_weights()
            logger.info(f"[PositionTrailingBrain] Training complete: n={len(rows)}")
        except Exception as e: logger.error(f"[PositionTrailingBrain] train error: {e}")

# Singleton
trailing_brain = PositionTrailingBrain()
