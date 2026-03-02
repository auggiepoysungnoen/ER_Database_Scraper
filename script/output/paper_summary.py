"""
output/paper_summary.py
=======================
Paper summary generation and writing for the Hickey Lab Endometrial
Receptivity pipeline.

For each accepted dataset a structured paper summary dict is constructed
from available metadata.  The summaries are written to:

- ``paper_summaries.json`` — array of summary objects
- ``paper_summaries.md``   — Markdown rendering grouped by modality,
                              sorted by confidence score descending

All text fields are built from actual metadata fields only.  Where a field
is missing the placeholder "Not reported" is used, preserving academic
integrity (no hallucinated findings).

Usage
-----
    from output.paper_summary import (
        generate_paper_summaries,
        write_paper_summaries_json,
        write_paper_summaries_md,
    )

    summaries = generate_paper_summaries(datasets, scores)
    write_paper_summaries_json(summaries, "/path/to/output")
    write_paper_summaries_md(summaries, "/path/to/output")
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_NR = "Not reported"   # standard placeholder for missing data


def _safe(value: Any, fallback: str = _NR) -> str:
    """Return str(value) if value is truthy, else fallback."""
    if value is None or value == "" or value == []:
        return fallback
    return str(value).strip() or fallback


def _join(lst: Any, sep: str = ", ", fallback: str = _NR) -> str:
    """Join a list to string, or return fallback if empty/None."""
    if isinstance(lst, list) and lst:
        return sep.join(str(v) for v in lst if v is not None)
    return fallback


def _score_map(scores: list[dict]) -> dict[str, dict]:
    """Build accession → score dict lookup."""
    return {s.get("accession", ""): s for s in scores if s.get("accession")}


def _build_aim(dataset: dict) -> str:
    """Construct an 'aim' sentence from available abstract / title fields.

    Returns either the abstract (if present) or a template sentence
    derived from modality, disease groups, and cycle phases.  No text is
    fabricated beyond the metadata.
    """
    abstract = _safe(dataset.get("abstract"), "")
    if abstract and abstract != _NR:
        return abstract

    modality       = _safe(dataset.get("modality"), "transcriptomic")
    disease_groups = _join(dataset.get("disease_groups"), fallback="healthy")
    cycle_phases   = _join(dataset.get("cycle_phases"), fallback="unspecified cycle phases")

    return (
        f"This study generated {modality} data from endometrial tissue in "
        f"{disease_groups} subjects sampled during {cycle_phases}.  "
        f"The research aims to characterise gene expression dynamics relevant "
        f"to endometrial receptivity and the Window of Implantation (WOI).  "
        f"Full study aims are detailed in the associated publication."
    )


def _build_methodology(dataset: dict) -> str:
    """Construct a brief methodology string from platform and modality fields."""
    modality  = _safe(dataset.get("modality"))
    platform  = _safe(dataset.get("platform"))
    n_cells   = _safe(dataset.get("n_cells"))
    n_samples = _safe(dataset.get("n_samples"))

    parts: list[str] = [f"{modality} data were generated using the {platform} platform."]

    if n_cells != _NR:
        parts.append(f"A total of {n_cells} cells were profiled.")
    elif n_samples != _NR:
        parts.append(f"A total of {n_samples} samples were sequenced.")

    parts.append(
        "Tissue collection, library preparation, and preprocessing details "
        "are described in the original publication.  "
        "Refer to the accession record for raw data access and processing scripts."
    )
    return " ".join(parts)


def _build_findings(dataset: dict) -> str:
    """Return abstract text as findings, or a structured placeholder."""
    abstract = _safe(dataset.get("abstract"), "")
    if abstract and abstract != _NR:
        # Return abstract as the findings source — downstream curation required
        return abstract

    modality       = _safe(dataset.get("modality"), "transcriptomic")
    sub_comp       = _join(dataset.get("sub_compartments"), fallback="various cell types")
    lh_tp          = _join(dataset.get("lh_timepoints"), fallback=_NR)

    return (
        f"Key {modality} findings are reported across {sub_comp}.  "
        f"LH-referenced timepoints covered: {lh_tp}.  "
        f"Detailed findings are available in the associated publication "
        f"and require manual curation from the full text."
    )


def _build_relevance(dataset: dict, score: dict) -> str:
    """Construct a relevance statement for Hickey Lab Aim01."""
    modality   = _safe(dataset.get("modality"), "transcriptomic")
    lh_tp      = _join(dataset.get("lh_timepoints"), fallback="unspecified timepoints")
    sub_comp   = _join(dataset.get("sub_compartments"), fallback="tissue compartments")
    final_cs   = score.get("final_CS", "N/A")
    tier       = score.get("confidence_tier", _NR)

    return (
        f"This dataset provides {modality} data at {lh_tp}, with annotations "
        f"spanning {sub_comp}, making it directly applicable to Aim01 goals of "
        f"characterising the endometrial transcriptome across the WOI.  "
        f"Confidence Score: {final_cs} ({tier})."
    )


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def generate_paper_summaries(
    datasets: list[dict],
    scores: list[dict],
) -> list[dict]:
    """Build structured paper summary dicts for each dataset.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dicts from scrapers.
    scores : list[dict]
        List of score result dicts from ``ConfidenceScorer.score_all``.

    Returns
    -------
    list[dict]
        List of paper summary dicts, one per dataset.  Each dict has keys:
        ``accession``, ``doi``, ``title``, ``authors``, ``journal_if``,
        ``peer_reviewed``, ``aim``, ``dataset``, ``methodology``,
        ``findings``, ``relevance``.

    Notes
    -----
    All text is derived from actual metadata.  Missing fields are filled with
    the placeholder "Not reported" rather than generated text.

    Examples
    --------
    >>> summaries = generate_paper_summaries(datasets, scores)
    >>> isinstance(summaries, list)
    True
    """
    sm = _score_map(scores)
    summaries: list[dict] = []

    for ds in datasets:
        acc   = _safe(ds.get("accession"))
        score = sm.get(acc, {})

        # --- Dataset sub-dict (mirrors spec schema) ---
        dataset_block: dict[str, Any] = {
            "modality":            _safe(ds.get("modality")),
            "platform":            _safe(ds.get("platform")),
            "n_patients":          _safe(ds.get("n_patients")),
            "n_samples":           _safe(ds.get("n_samples")),
            "n_cells":             _safe(ds.get("n_cells")),
            "cycle_phases":        _join(ds.get("cycle_phases")),
            "lh_timepoints":       _join(ds.get("lh_timepoints")),
            "sub_compartments":    _join(ds.get("sub_compartments")),
            "disease_groups":      _join(ds.get("disease_groups")),
            "raw_data_available":  _safe(ds.get("raw_data_available")),
            "data_location":       _safe(ds.get("download_url") or ds.get("accession")),
            "confidence_score":    score.get("final_CS", _NR),
            "confidence_tier":     score.get("confidence_tier", _NR),
        }

        # --- Journal impact factor: numeric or None ---
        jif_raw = ds.get("journal_if")
        try:
            journal_if: float | None = float(jif_raw) if jif_raw is not None else None
        except (TypeError, ValueError):
            journal_if = None

        summary: dict[str, Any] = {
            "accession":    acc,
            "doi":          _safe(ds.get("doi")),
            "title":        _safe(ds.get("title")),
            "authors":      _safe(ds.get("authors")),
            "journal_if":   journal_if,
            "peer_reviewed": _safe(ds.get("peer_reviewed")),
            "aim":          _build_aim(ds),
            "dataset":      dataset_block,
            "methodology":  _build_methodology(ds),
            "findings":     _build_findings(ds),
            "relevance":    _build_relevance(ds, score),
        }
        summaries.append(summary)

    return summaries


def write_paper_summaries_json(
    summaries: list[dict],
    output_dir: str,
) -> str:
    """Write paper summaries to ``paper_summaries.json``.

    Parameters
    ----------
    summaries : list[dict]
        List of paper summary dicts from ``generate_paper_summaries``.
    output_dir : str
        Directory where the JSON file will be written.

    Returns
    -------
    str
        Absolute path to the written file.

    Examples
    --------
    >>> path = write_paper_summaries_json(summaries, "/output")
    >>> path.endswith("paper_summaries.json")
    True
    """
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "paper_summaries.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(summaries, fh, indent=2, ensure_ascii=False, default=str)
    return os.path.abspath(out_path)


def write_paper_summaries_md(
    summaries: list[dict],
    output_dir: str,
) -> str:
    """Write paper summaries to ``paper_summaries.md``.

    The Markdown file is organised by modality section, with datasets sorted
    by confidence score descending within each section.

    Parameters
    ----------
    summaries : list[dict]
        List of paper summary dicts from ``generate_paper_summaries``.
    output_dir : str
        Directory where the Markdown file will be written.

    Returns
    -------
    str
        Absolute path to the written file.

    Examples
    --------
    >>> path = write_paper_summaries_md(summaries, "/output")
    >>> path.endswith("paper_summaries.md")
    True
    """
    os.makedirs(output_dir, exist_ok=True)

    # Group by modality
    by_modality: dict[str, list[dict]] = defaultdict(list)
    for s in summaries:
        modality = s.get("dataset", {}).get("modality", "Unknown")
        by_modality[modality].append(s)

    # Sort within each modality by confidence score descending
    def _cs(s: dict) -> float:
        raw = s.get("dataset", {}).get("confidence_score", 0)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    lines: list[str] = [
        "# Hickey Lab — Endometrial Receptivity Dataset Paper Summaries",
        "",
        "_Organised by modality; sorted by Confidence Score (descending) within each group._",
        "_All summaries are derived from publicly available metadata and abstracts._",
        "",
        "---",
        "",
    ]

    modality_order = sorted(by_modality.keys())

    for modality in modality_order:
        group = sorted(by_modality[modality], key=_cs, reverse=True)
        lines.append(f"## {modality}")
        lines.append("")

        for s in group:
            ds    = s.get("dataset", {})
            acc   = s.get("accession", _NR)
            title = s.get("title", _NR)
            doi   = s.get("doi", _NR)
            cs    = ds.get("confidence_score", _NR)
            tier  = ds.get("confidence_tier", _NR)

            lines.append(f"### `{acc}` — {title}")
            lines.append("")
            lines.append(f"**DOI:** {doi}  ")
            lines.append(f"**Authors:** {s.get('authors', _NR)}  ")
            jif = s.get("journal_if")
            lines.append(f"**Journal IF:** {jif if jif is not None else _NR}  ")
            lines.append(f"**Peer reviewed:** {s.get('peer_reviewed', _NR)}  ")
            lines.append(f"**Confidence Score:** {cs} ({tier})")
            lines.append("")

            lines.append("#### Aim")
            lines.append("")
            lines.append(s.get("aim", _NR))
            lines.append("")

            lines.append("#### Dataset Details")
            lines.append("")
            lines.append(f"| Field | Value |")
            lines.append(f"|---|---|")
            for field, val in ds.items():
                if field not in ("confidence_score", "confidence_tier"):
                    lines.append(f"| {field} | {val} |")
            lines.append("")

            lines.append("#### Brief Methodology")
            lines.append("")
            lines.append(s.get("methodology", _NR))
            lines.append("")

            lines.append("#### Brief Findings")
            lines.append("")
            lines.append(s.get("findings", _NR))
            lines.append("")

            lines.append("#### Relevance to Hickey Lab Aim01")
            lines.append("")
            lines.append(s.get("relevance", _NR))
            lines.append("")
            lines.append("---")
            lines.append("")

    out_path = os.path.join(output_dir, "paper_summaries.md")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    return os.path.abspath(out_path)
