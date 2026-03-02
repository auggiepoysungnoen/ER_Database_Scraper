"""
scoring/tiers.py
================
Tier definitions and classification logic for the Hickey Lab confidence
scoring system.

Tiers map a continuous Confidence Score (0–100) to a discrete label used
for filtering and display throughout the pipeline.

Usage
-----
    from scoring.tiers import TIERS, classify_tier

    tier = classify_tier(73.5)   # → "SILVER"
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Tier boundaries  (inclusive lower, inclusive upper)
# ---------------------------------------------------------------------------

TIERS: dict[str, tuple[int, int]] = {
    "GOLD":           (80, 100),
    "SILVER":         (60, 79),
    "BRONZE":         (40, 59),
    "LOW_CONFIDENCE": (0,  39),
}


def classify_tier(score: float) -> str:
    """Return the confidence tier label for a given score.

    Parameters
    ----------
    score : float
        Confidence Score value, expected in the range [0, 100].
        Values outside this range are clamped before classification.

    Returns
    -------
    str
        One of ``"GOLD"``, ``"SILVER"``, ``"BRONZE"``, or
        ``"LOW_CONFIDENCE"``.

    Examples
    --------
    >>> classify_tier(85.0)
    'GOLD'
    >>> classify_tier(63.2)
    'SILVER'
    >>> classify_tier(45.0)
    'BRONZE'
    >>> classify_tier(22.1)
    'LOW_CONFIDENCE'
    """
    clamped = max(0.0, min(100.0, float(score)))
    for tier, (low, high) in TIERS.items():
        if low <= clamped <= high:
            return tier
    # Fallback — should never be reached after clamping
    return "LOW_CONFIDENCE"
