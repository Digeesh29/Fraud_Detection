"""
ml/inference.py
---------------
Loads all three trained models and runs ensemble prediction on live transactions.

Ensemble strategy:
    - XGBoost + Random Forest are the primary classifiers (supervised)
    - Isolation Forest is the anomaly detector (unsupervised fallback)
    - A transaction is flagged as fraud if:
        (a) Either supervised model probability >= threshold, OR
        (b) Isolation Forest flags it AND no supervised model has a confident
            "normal" score — catches novel fraud patterns not in training data

Output per transaction:
    {
        "is_fraud_predicted": bool,
        "confidence":         float,   # highest fraud probability across models
        "model_votes": {
            "random_forest":    float,  # fraud probability
            "xgboost":          float,
            "isolation_forest": bool,   # True = anomaly detected
        },
        "reason": str                   # which model(s) triggered the flag
    }

Usage:
    engine = InferenceEngine()
    result = engine.predict(feature_vector)
"""

import logging
from pathlib import Path

import joblib
import numpy as np

logger = logging.getLogger(__name__)

MODEL_DIR = Path("../models")

# Fraud probability threshold for supervised models
SUPERVISED_THRESHOLD = 0.5

# If both supervised models are below this, Isolation Forest alone won't flag it
# (reduces false positives from IF)
ISO_CONFIDENCE_GATE = 0.3


class InferenceEngine:
    """
    Loads all three models at startup and runs ensemble inference.
    Designed to be instantiated once and reused for every transaction.
    """

    def __init__(self, model_dir: str | Path = MODEL_DIR):
        self.model_dir = Path(model_dir)
        self.rf  = None
        self.xgb = None
        self.iso = None
        self._load_models()

    def _load_models(self):
        """Load all three models from disk. Raises if any are missing."""
        rf_path  = self.model_dir / "random_forest.pkl"
        xgb_path = self.model_dir / "xgboost.pkl"
        iso_path = self.model_dir / "isolation_forest.pkl"

        missing = [p for p in [rf_path, xgb_path, iso_path] if not p.exists()]
        if missing:
            raise FileNotFoundError(
                f"Missing model files: {[str(p) for p in missing]}\n"
                f"Run: python ml/train.py --csv data/paysim.csv"
            )

        logger.info("Loading models from disk ...")
        self.rf  = joblib.load(rf_path)
        self.xgb = joblib.load(xgb_path)
        self.iso = joblib.load(iso_path)
        logger.info("All models loaded ✅")

    def predict(self, features: np.ndarray) -> dict:
        """
        Run ensemble prediction on a single feature vector.

        Args:
            features: numpy array of shape (FEATURE_VECTOR_SIZE,)

        Returns:
            Prediction result dict.
        """
        X = features.reshape(1, -1)

        # ── Supervised model probabilities ─────────────────────────────────────
        rf_prob  = float(self.rf.predict_proba(X)[0][1])
        xgb_prob = float(self.xgb.predict_proba(X)[0][1])

        # ── Isolation Forest (returns -1 for anomaly, 1 for normal) ───────────
        iso_flag = self.iso.predict(X)[0] == -1  # True = anomaly

        # ── Ensemble decision ──────────────────────────────────────────────────
        rf_fraud  = rf_prob  >= SUPERVISED_THRESHOLD
        xgb_fraud = xgb_prob >= SUPERVISED_THRESHOLD

        # Isolation Forest only contributes if supervised models are uncertain
        max_supervised = max(rf_prob, xgb_prob)
        iso_contributes = iso_flag and max_supervised >= ISO_CONFIDENCE_GATE

        is_fraud = rf_fraud or xgb_fraud or iso_contributes

        # ── Confidence = highest fraud signal across all models ────────────────
        confidence = max(rf_prob, xgb_prob)
        if iso_flag:
            confidence = max(confidence, ISO_CONFIDENCE_GATE)

        # ── Reason string — which model(s) triggered ──────────────────────────
        reasons = []
        if rf_fraud:
            reasons.append(f"RandomForest({rf_prob:.2f})")
        if xgb_fraud:
            reasons.append(f"XGBoost({xgb_prob:.2f})")
        if iso_contributes:
            reasons.append("IsolationForest(anomaly)")
        reason = " + ".join(reasons) if reasons else "none"

        return {
            "is_fraud_predicted": is_fraud,
            "confidence":         round(confidence, 4),
            "model_votes": {
                "random_forest":    round(rf_prob, 4),
                "xgboost":          round(xgb_prob, 4),
                "isolation_forest": bool(iso_flag),
            },
            "reason": reason,
        }

    def predict_batch(self, feature_matrix: np.ndarray) -> list[dict]:
        """
        Run ensemble prediction on a batch of feature vectors.

        Args:
            feature_matrix: numpy array of shape (n, FEATURE_VECTOR_SIZE)

        Returns:
            List of n prediction result dicts.
        """
        return [self.predict(feature_matrix[i]) for i in range(len(feature_matrix))]
