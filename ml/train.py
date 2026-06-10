"""
ml/train.py
-----------
Trains Random Forest and XGBoost on the PaySim dataset.
Saves trained models to disk so inference.py can load them at startup.

Also trains an Isolation Forest on the same data (unsupervised baseline).
In production the Isolation Forest runs on unlabeled live data,
but we fit it on PaySim here for a warm start.

Usage:
    python ml/train.py --csv data/paysim.csv

    # Limit rows for a quick test run
    python ml/train.py --csv data/paysim.csv --max-rows 100000
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from processing.preprocessor import Preprocessor, TRANSACTION_TYPE_ENCODING
from processing.feature_engineering import FeatureEngineer, FEATURE_COLUMNS
from ml.evaluate import evaluate_model, print_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MODEL_DIR = Path("models")


# ── Data Loading ───────────────────────────────────────────────────────────────

def load_paysim(csv_path: str, max_rows: int | None = None) -> tuple[np.ndarray, np.ndarray]:
    """
    Load PaySim CSV, run through preprocessor + feature engineering,
    return (X, y) arrays ready for sklearn.
    """
    logger.info(f"Loading PaySim from {csv_path} ...")
    df = pd.read_csv(csv_path, nrows=max_rows)
    logger.info(f"Loaded {len(df):,} rows — fraud rate: {df['isFraud'].mean():.2%}")

    preprocessor = Preprocessor()
    engineer     = FeatureEngineer()

    X_rows = []
    y_rows = []
    skipped = 0

    for _, row in df.iterrows():
        # Convert PaySim row to the same dict format as our Kafka messages
        tx_raw = {
            "transaction_id":        str(row.get("step", 0)),
            "type":                  row["type"].lower().replace(" ", "_"),
            "amount":                float(row["amount"]),
            "sender_id":             str(row["nameOrig"]),
            "sender_balance_before": float(row["oldbalanceOrg"]),
            "sender_balance_after":  float(row["newbalanceOrig"]),
            "receiver_id":           str(row["nameDest"]),
            "receiver_balance_before": float(row["oldbalanceDest"]),
            "receiver_balance_after":  float(row["newbalanceDest"]),
            "is_fraud":              bool(int(row["isFraud"])),
            "fraud_pattern":         None,
            "timestamp":             None,
            "source":                "paysim",
        }

        # Normalize type strings to match our encoding
        type_map = {
            "cash_in":   "cash_in",
            "cash_out":  "cash_out",
            "debit":     "debit",
            "payment":   "payment",
            "transfer":  "transfer",
        }
        tx_raw["type"] = type_map.get(tx_raw["type"], tx_raw["type"])

        # Skip unknown types
        if tx_raw["type"] not in TRANSACTION_TYPE_ENCODING:
            skipped += 1
            continue

        cleaned = preprocessor.process(tx_raw)
        if cleaned is None:
            skipped += 1
            continue

        features = engineer.extract(cleaned)
        X_rows.append(features)
        y_rows.append(1 if tx_raw["is_fraud"] else 0)

    if skipped:
        logger.warning(f"Skipped {skipped:,} rows during preprocessing")

    X = np.vstack(X_rows)
    y = np.array(y_rows, dtype=np.int32)

    logger.info(
        f"Feature matrix: {X.shape} | "
        f"Fraud: {y.sum():,} ({y.mean():.2%})"
    )
    return X, y


# ── Model Training ─────────────────────────────────────────────────────────────

def train_random_forest(X_train: np.ndarray, y_train: np.ndarray) -> RandomForestClassifier:
    logger.info("Training Random Forest ...")
    t0 = time.time()

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=12,
        min_samples_leaf=5,
        class_weight="balanced",   # handles class imbalance automatically
        n_jobs=-1,                 # use all CPU cores
        random_state=42,
    )
    model.fit(X_train, y_train)

    logger.info(f"Random Forest trained in {time.time() - t0:.1f}s")
    return model


def train_xgboost(X_train: np.ndarray, y_train: np.ndarray) -> XGBClassifier:
    logger.info("Training XGBoost ...")
    t0 = time.time()

    # scale_pos_weight handles class imbalance: ratio of negatives to positives
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    scale_pos_weight = neg / max(pos, 1)
    logger.info(f"XGBoost scale_pos_weight: {scale_pos_weight:.1f}")

    model = XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",       # area under precision-recall curve
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train)],
        verbose=False,
    )

    logger.info(f"XGBoost trained in {time.time() - t0:.1f}s")
    return model


def train_isolation_forest(X_train: np.ndarray) -> IsolationForest:
    logger.info("Training Isolation Forest (unsupervised) ...")
    t0 = time.time()

    model = IsolationForest(
        n_estimators=100,
        contamination=0.02,    # expected fraction of anomalies (~fraud rate)
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train)

    logger.info(f"Isolation Forest trained in {time.time() - t0:.1f}s")
    return model


# ── Save / Load ────────────────────────────────────────────────────────────────

def save_models(rf, xgb, iso):
    """Persist all three models to the models/ directory using joblib."""
    import joblib

    MODEL_DIR.mkdir(exist_ok=True)

    joblib.dump(rf,  MODEL_DIR / "random_forest.pkl")
    joblib.dump(xgb, MODEL_DIR / "xgboost.pkl")
    joblib.dump(iso, MODEL_DIR / "isolation_forest.pkl")

    logger.info(f"Models saved to {MODEL_DIR}/")


# ── Main ───────────────────────────────────────────────────────────────────────

def main(csv_path: str, max_rows: int | None):
    # Load and featurize
    X, y = load_paysim(csv_path, max_rows)

    # Train/test split — stratified to preserve fraud ratio in both splits
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        stratify=y,
        random_state=42,
    )
    logger.info(
        f"Train: {len(X_train):,} | Test: {len(X_test):,} | "
        f"Test fraud: {y_test.sum():,}"
    )

    # Train all three models
    rf  = train_random_forest(X_train, y_train)
    xgb = train_xgboost(X_train, y_train)
    iso = train_isolation_forest(X_train)

    # Evaluate supervised models on held-out test set
    logger.info("\n── Random Forest Evaluation ──")
    rf_metrics = evaluate_model(rf, X_test, y_test, model_name="RandomForest")
    print_report(rf_metrics)

    logger.info("\n── XGBoost Evaluation ──")
    xgb_metrics = evaluate_model(xgb, X_test, y_test, model_name="XGBoost")
    print_report(xgb_metrics)

    logger.info("\n── Isolation Forest Evaluation (as anomaly detector) ──")
    # IF returns -1 for anomaly, 1 for normal — convert to 0/1
    iso_preds = (iso.predict(X_test) == -1).astype(int)
    iso_metrics = evaluate_model(
        iso, X_test, y_test,
        model_name="IsolationForest",
        y_pred_override=iso_preds,
    )
    print_report(iso_metrics)

    # Save all models
    save_models(rf, xgb, iso)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train fraud detection models on PaySim")
    parser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to PaySim CSV file",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit rows for quick test",
    )
    args = parser.parse_args()
    main(args.csv, args.max_rows)
