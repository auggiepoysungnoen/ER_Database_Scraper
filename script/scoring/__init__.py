"""
scoring/__init__.py
===================
Scoring package for the Hickey Lab Endometrial Receptivity pipeline.

Exports the ``ConfidenceScorer`` class and the ``classify_tier`` helper so
that importers only need to reference the package root.

Usage
-----
    from scoring import ConfidenceScorer, classify_tier

    scorer = ConfidenceScorer()
    result = scorer.score(dataset_dict)
    tier   = classify_tier(result["final_CS"])
"""

from scoring.confidence import ConfidenceScorer
from scoring.tiers import classify_tier
from scoring.ai_extractor import extract_metadata, batch_enrich, enrich_record_live

__all__ = [
    "ConfidenceScorer",
    "classify_tier",
    "extract_metadata",
    "batch_enrich",
    "enrich_record_live",
]
