"""
ml/evaluate.py
--------------
Evaluation metrics for fraud detection models.

Metrics used:
    Precision   — of all flagged transactions, how many were actually fraud
    Recall      — of all actual fraud, how many did we catch
    F1 Score    — harmonic mean of precision and recall
    ROC-AUC     — overall discrimination ability (threshold-independent)
    PR-AUC      — area under precision-recall curve (better for imbalanced data)

Usage:
    from ml.evaluate import evaluate_model, print_report

    metrics = evaluate_model(model, X_test, y_test, model_name="XGBoost")
    print_report(metrics)
"""

import logging

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

logger = logging.getLogger(__name__)


def evaluate_model(
    model,
    X_test: np.ndarray,
    y_test: np.ndarray,
    model_name: str = "Model",
    threshold: float = 0.5,
    y_pred_override: np.ndarray | None = None,
) -> dict:
    """
    Compute evaluation metrics for a trained model.

    Args:
        model:            Trained sklearn-compatible model.
        X_test:           Feature matrix.
        y_test:           True labels (0 = normal, 1 = fraud).
        model_name:       Label for logging.
        threshold:        Decision threshold for binary classification.
        y_pred_override:  If provided, skip model.predict() and use these labels directly.
                          Used for Isolation Forest which has a different prediction API.

    Returns:
        Dict of metric name → value.
    """
    # ── Predictions ────────────────────────────────────────────────────────────
    if y_pred_override is not None:
        y_pred = y_pred_override
        y_prob = None
    else:
        if hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X_test)[:, 1]
            y_pred = (y_prob >= threshold).astype(int)
        else:
            y_pred = model.predict(X_test)
            y_prob = None

    # ── Core metrics ───────────────────────────────────────────────────────────
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall    = recall_score(y_test, y_pred, zero_division=0)
    f1        = f1_score(y_test, y_pred, zero_division=0)
    cm        = confusion_matrix(y_test, y_pred)

    metrics = {
        "model":     model_name,
        "threshold": threshold,
        "precision": round(precision, 4),
        "recall":    round(recall, 4),
        "f1":        round(f1, 4),
        "confusion_matrix": cm.tolist(),
    }

    # ROC-AUC and PR-AUC require probability scores
    if y_prob is not None:
        try:
            roc_auc = roc_auc_score(y_test, y_prob)
            pr_auc  = average_precision_score(y_test, y_prob)
            metrics["roc_auc"] = round(roc_auc, 4)
            metrics["pr_auc"]  = round(pr_auc, 4)
        except ValueError as e:
            logger.warning(f"Could not compute AUC scores: {e}")

    # Full sklearn classification report as a string
    metrics["classification_report"] = classification_report(
        y_test, y_pred,
        target_names=["Normal", "Fraud"],
        zero_division=0,
    )

    return metrics


def print_report(metrics: dict):
    """Pretty-print an evaluation metrics dict."""
    print(f"\n{'─' * 50}")
    print(f"  Model      : {metrics['model']}")
    print(f"  Precision  : {metrics['precision']:.4f}")
    print(f"  Recall     : {metrics['recall']:.4f}")
    print(f"  F1 Score   : {metrics['f1']:.4f}")
    if "roc_auc" in metrics:
        print(f"  ROC-AUC    : {metrics['roc_auc']:.4f}")
    if "pr_auc" in metrics:
        print(f"  PR-AUC     : {metrics['pr_auc']:.4f}")
    print(f"\n  Confusion Matrix:")
    cm = metrics["confusion_matrix"]
    print(f"    TN={cm[0][0]:,}  FP={cm[0][1]:,}")
    print(f"    FN={cm[1][0]:,}  TP={cm[1][1]:,}")
    print(f"\n{metrics['classification_report']}")
    print(f"{'─' * 50}\n")
