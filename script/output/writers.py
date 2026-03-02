"""
output/writers.py
=================
CSV / JSON writer functions for the Hickey Lab Endometrial Receptivity
pipeline.

All writers are idempotent: re-running with the same accessions will not
produce duplicate rows/entries.  The ``merge_registry`` function enforces
uniqueness keyed on ``accession``.

Outputs produced
----------------
- ``metadata_master.csv``   — one row per dataset, unified metadata
- ``confidence_scores.csv`` — scoring dimensions per dataset
- ``datasets_registry.json``— nested JSON registry keyed by accession

Usage
-----
    from output.writers import (
        write_metadata_master,
        write_confidence_scores,
        write_registry,
        load_existing_registry,
        merge_registry,
    )

    write_metadata_master(datasets, scores, "/path/to/output")
    write_confidence_scores(scores, "/path/to/output")
    write_registry(datasets, scores, "/path/to/output")
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_dir(output_dir: str) -> None:
    """Create output directory if it does not exist."""
    os.makedirs(output_dir, exist_ok=True)


def _join_list(value: Any, sep: str = ";") -> str:
    """Return a separator-joined string for list fields, or empty string."""
    if isinstance(value, list):
        return sep.join(str(v) for v in value if v is not None)
    if value is None:
        return ""
    return str(value)


def _score_by_accession(scores: list[dict]) -> dict[str, dict]:
    """Build accession → score-dict lookup from scores list."""
    return {s.get("accession", ""): s for s in scores if s.get("accession")}


# ---------------------------------------------------------------------------
# Public writers
# ---------------------------------------------------------------------------

def write_metadata_master(
    datasets: list[dict],
    scores: list[dict],
    output_dir: str,
) -> str:
    """Write the unified metadata table to ``metadata_master.csv``.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dicts from scrapers.
    scores : list[dict]
        List of score result dicts from ``ConfidenceScorer.score_all``.
    output_dir : str
        Directory path where the CSV will be written.

    Returns
    -------
    str
        Absolute path to the written file.

    Notes
    -----
    Columns written:
        accession, source_db, title, doi, modality, platform, n_patients,
        n_cells, lh_timepoints, sub_compartments, disease_groups,
        raw_data_available, confidence_score, confidence_tier, download_url,
        file_size_gb, date_scraped

    Missing fields are written as empty strings.  List fields
    (lh_timepoints, sub_compartments, disease_groups) are semicolon-joined.

    Examples
    --------
    >>> path = write_metadata_master(datasets, scores, "/output")
    >>> path.endswith("metadata_master.csv")
    True
    """
    _ensure_dir(output_dir)
    score_map = _score_by_accession(scores)

    rows: list[dict] = []
    for ds in datasets:
        acc   = ds.get("accession", "")
        sc    = score_map.get(acc, {})
        rows.append({
            "accession":          acc,
            "source_db":          ds.get("source_db", ""),
            "title":              ds.get("title", ""),
            "doi":                ds.get("doi", ""),
            "modality":           ds.get("modality", ""),
            "platform":           ds.get("platform", ""),
            "n_patients":         ds.get("n_patients", ""),
            "n_cells":            ds.get("n_cells", ""),
            "lh_timepoints":      _join_list(ds.get("lh_timepoints")),
            "sub_compartments":   _join_list(ds.get("sub_compartments")),
            "disease_groups":     _join_list(ds.get("disease_groups")),
            "raw_data_available": ds.get("raw_data_available", ""),
            "confidence_score":   sc.get("final_CS", ""),
            "confidence_tier":    sc.get("confidence_tier", ""),
            "download_url":       ds.get("download_url", ""),
            "file_size_gb":       ds.get("file_size_gb", ""),
            "date_scraped":       ds.get("date_scraped", ""),
        })

    df = pd.DataFrame(rows, columns=[
        "accession", "source_db", "title", "doi", "modality", "platform",
        "n_patients", "n_cells", "lh_timepoints", "sub_compartments",
        "disease_groups", "raw_data_available", "confidence_score",
        "confidence_tier", "download_url", "file_size_gb", "date_scraped",
    ])

    out_path = os.path.join(output_dir, "metadata_master.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return os.path.abspath(out_path)


def write_confidence_scores(
    scores: list[dict],
    output_dir: str,
) -> str:
    """Write per-dataset confidence scores to ``confidence_scores.csv``.

    Parameters
    ----------
    scores : list[dict]
        List of score result dicts from ``ConfidenceScorer.score_all``.
    output_dir : str
        Directory path where the CSV will be written.

    Returns
    -------
    str
        Absolute path to the written file.

    Notes
    -----
    Columns written:
        accession, DQS, TRS, SRS, MCS, DAS, penalties, modality_weight,
        final_CS, confidence_tier

    Examples
    --------
    >>> path = write_confidence_scores(scores, "/output")
    >>> path.endswith("confidence_scores.csv")
    True
    """
    _ensure_dir(output_dir)

    rows: list[dict] = []
    for sc in scores:
        rows.append({
            "accession":       sc.get("accession", ""),
            "DQS":             sc.get("DQS", ""),
            "TRS":             sc.get("TRS", ""),
            "SRS":             sc.get("SRS", ""),
            "MCS":             sc.get("MCS", ""),
            "DAS":             sc.get("DAS", ""),
            "penalties":       sc.get("penalties", ""),
            "modality_weight": sc.get("modality_weight", ""),
            "final_CS":        sc.get("final_CS", ""),
            "confidence_tier": sc.get("confidence_tier", ""),
        })

    df = pd.DataFrame(rows, columns=[
        "accession", "DQS", "TRS", "SRS", "MCS", "DAS",
        "penalties", "modality_weight", "final_CS", "confidence_tier",
    ])

    out_path = os.path.join(output_dir, "confidence_scores.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return os.path.abspath(out_path)


def write_registry(
    datasets: list[dict],
    scores: list[dict],
    output_dir: str,
) -> str:
    """Write the machine-readable JSON registry to ``datasets_registry.json``.

    The registry is a dict keyed by accession.  Each entry contains the full
    dataset metadata merged with its confidence score result.  List fields
    (lh_timepoints, sub_compartments, disease_groups) are preserved as lists
    (not joined) for downstream programmatic consumption.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dicts.
    scores : list[dict]
        List of score result dicts.
    output_dir : str
        Directory path where the JSON will be written.

    Returns
    -------
    str
        Absolute path to the written file.

    Notes
    -----
    This function loads any existing registry first (via
    ``load_existing_registry``) and merges new entries before writing, so
    the registry is always a superset of previous runs.

    Examples
    --------
    >>> path = write_registry(datasets, scores, "/output")
    >>> path.endswith("datasets_registry.json")
    True
    """
    _ensure_dir(output_dir)
    score_map = _score_by_accession(scores)

    # Build new entries
    new_datasets: list[dict] = []
    for ds in datasets:
        acc  = ds.get("accession", "")
        sc   = score_map.get(acc, {})
        entry = dict(ds)  # shallow copy of all scraper fields
        # Attach scoring results (exclude verbose score_breakdown for registry)
        entry["confidence_score"]  = sc.get("final_CS")
        entry["confidence_tier"]   = sc.get("confidence_tier")
        entry["DQS"]               = sc.get("DQS")
        entry["TRS"]               = sc.get("TRS")
        entry["SRS"]               = sc.get("SRS")
        entry["MCS"]               = sc.get("MCS")
        entry["DAS"]               = sc.get("DAS")
        entry["penalties"]         = sc.get("penalties")
        entry["modality_weight"]   = sc.get("modality_weight")
        entry["score_breakdown"]   = sc.get("score_breakdown")
        entry["registry_updated"]  = datetime.now(timezone.utc).isoformat()
        new_datasets.append(entry)

    # Merge with existing registry
    existing  = load_existing_registry(output_dir)
    registry  = merge_registry(existing, new_datasets)

    out_path = os.path.join(output_dir, "datasets_registry.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(registry, fh, indent=2, ensure_ascii=False, default=str)

    return os.path.abspath(out_path)


def load_existing_registry(output_dir: str) -> dict:
    """Load an existing ``datasets_registry.json`` if it exists.

    Parameters
    ----------
    output_dir : str
        Directory that may contain ``datasets_registry.json``.

    Returns
    -------
    dict
        Existing registry dict keyed by accession, or empty dict if the
        file does not exist or cannot be parsed.

    Examples
    --------
    >>> existing = load_existing_registry("/output")
    >>> isinstance(existing, dict)
    True
    """
    path = os.path.join(output_dir, "datasets_registry.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
        # If someone stored it as a list, convert to dict
        if isinstance(data, list):
            return {entry.get("accession", ""): entry for entry in data}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def merge_registry(existing: dict, new_datasets: list[dict]) -> dict:
    """Merge new dataset entries into an existing registry without duplicates.

    Existing entries are overwritten when the same accession appears in
    ``new_datasets``.  Accessions present only in ``existing`` are retained
    unchanged.

    Parameters
    ----------
    existing : dict
        Existing registry dict keyed by accession (may be empty).
    new_datasets : list[dict]
        New dataset entry dicts, each expected to have an ``accession`` key.

    Returns
    -------
    dict
        Merged registry dict keyed by accession.

    Examples
    --------
    >>> merged = merge_registry({"GSE1": {"title": "old"}},
    ...                          [{"accession": "GSE1", "title": "new"},
    ...                           {"accession": "GSE2", "title": "other"}])
    >>> set(merged.keys()) == {"GSE1", "GSE2"}
    True
    >>> merged["GSE1"]["title"]
    'new'
    """
    registry = dict(existing)  # copy
    for entry in new_datasets:
        acc = entry.get("accession", "")
        if acc:
            registry[acc] = entry
    return registry
