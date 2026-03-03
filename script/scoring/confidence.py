"""
scoring/confidence.py
=====================
Five-dimension Confidence Scoring for the Hickey Lab Endometrial Receptivity
pipeline.

Each dataset dictionary is evaluated across five dimensions (DQS, TRS, SRS,
MCS, DAS), penalties are subtracted, and the result is multiplied by a
modality-specific weight, then clamped to [0, 100].

Scoring formula (from pipeline specification §4):
    raw_score = DQS + TRS + SRS + MCS + DAS  (max 100 before penalties)
    penalised  = raw_score + penalties         (penalties are negative)
    final_CS   = clamp(penalised * modality_weight, 0, 100)

Usage
-----
    from scoring.confidence import ConfidenceScorer

    scorer  = ConfidenceScorer()
    result  = scorer.score(dataset)
    results = scorer.score_all(dataset_list)

Expected dataset dict keys (all optional — missing values are handled
gracefully as None / empty):
    accession          : str
    abstract           : str
    modality           : str
    platform           : str
    n_cells            : int | None
    n_samples          : int | None
    n_patients         : int | None
    raw_data_available : bool | None
    controlled_access  : bool | None
    download_url       : str | None
    doi                : str | None
    lh_timepoints      : list[str]
    sub_compartments   : list[str]
    cycle_phases       : list[str]
    disease_groups     : list[str]
    supplemental_files : list[str]
    peer_reviewed      : str   ("Yes" | "No" | "Preprint")
    metadata           : dict  (may contain "age", "bmi", "parity")
"""

from __future__ import annotations

import re
from typing import Any

from scoring.tiers import classify_tier

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# WOI-relevant LH+ timepoints
_WOI_TIMEPOINTS: frozenset[str] = frozenset(
    {"LH+5", "LH+6", "LH+7", "LH+8", "LH+9"}
)

# Immune-compartment keywords (case-insensitive substring match)
_IMMUNE_KEYWORDS: tuple[str, ...] = (
    "unk", "uterine natural killer", "macrophage", "t cell", "t-cell",
    "nk cell", "nk-cell", "natural killer",
)

# Modality weight table
MODALITY_WEIGHTS: dict[str, float] = {
    "scRNA-seq":               1.00,
    "snRNA-seq":               1.00,
    "Spatial Transcriptomics": 1.00,
    "Spatial Proteomics":      0.95,
    "bulkRNA-seq":             0.80,   # may be downweighted further below
    "Microarray":              0.50,
    "Unknown":                 0.60,
}

# Spatial modalities that earn the +5 SRS spatial bonus
_SPATIAL_MODALITIES: frozenset[str] = frozenset(
    {"Spatial Transcriptomics", "Spatial Proteomics"}
)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _safe_list(value: Any) -> list:
    """Return value if it is a non-None list, else an empty list."""
    if isinstance(value, list):
        return value
    return []


def _safe_str(value: Any) -> str:
    """Return value as a stripped string, or empty string if None/missing."""
    if value is None:
        return ""
    return str(value).strip()


def _abstract_contains(abstract: str, *phrases: str) -> bool:
    """Case-insensitive check whether *any* phrase appears in abstract."""
    low = abstract.lower()
    return any(p.lower() in low for p in phrases)


def _has_immune_compartment(sub_compartments: list[str]) -> bool:
    """Return True if any immune-related compartment is in the list."""
    joined = " ".join(sub_compartments).lower()
    return any(kw in joined for kw in _IMMUNE_KEYWORDS)


def _has_woi_timepoints(lh_timepoints: list[str]) -> bool:
    """Return True if any WOI timepoint (LH+5 through LH+9) is present."""
    normalised = {tp.strip().upper().replace(" ", "") for tp in lh_timepoints}
    return bool(normalised & {t.upper() for t in _WOI_TIMEPOINTS})


def _is_longitudinal(abstract: str) -> bool:
    """Infer longitudinal design from abstract keywords."""
    return _abstract_contains(
        abstract, "longitudinal", "same patient", "paired", "repeated measures",
        "within-patient", "intra-patient",
    )


def _qc_metrics_reported(abstract: str) -> bool:
    """Infer QC reporting from abstract keywords."""
    return _abstract_contains(
        abstract, "mitochondrial", "doublet", "QC", "quality control",
        "ambient RNA", "scrublet", "DoubletFinder",
    )


def _sequencing_depth_adequate(abstract: str, modality: str) -> bool:
    """Infer adequate sequencing depth from abstract text."""
    # scRNA-seq: ">20,000 reads/cell" variants
    sc_patterns = [
        r">\s*20[,.]?000\s*reads[/ ]cell",
        r">\s*20k\s*reads",
        r"median.*reads.*cell.*\b[2-9]\d{4,}\b",
    ]
    # bulk: ">30M reads"
    bulk_patterns = [
        r">\s*30\s*[mM](illion)?",
        r">\s*3\s*[xX]\s*10\^?7",
        r"\b[3-9]\d\s*[mM](illion)?\s*reads",
    ]
    low = abstract.lower()
    if modality in ("scRNA-seq", "snRNA-seq", "Spatial Transcriptomics"):
        return any(re.search(p, low) for p in sc_patterns)
    if modality in ("bulkRNA-seq", "Microarray"):
        return any(re.search(p, low) for p in bulk_patterns)
    return False


def _is_cell_line_or_organoid(abstract: str) -> bool:
    """Return True if abstract indicates cell-line or organoid-only study."""
    return _abstract_contains(abstract, "cell line", "organoid", "iPSC", "in vitro model")


def _resolve_modality_weight(modality: str, n_samples: int | None, abstract: str) -> float:
    """Return the modality weight, applying bulk/cell-line downweights."""
    weight = MODALITY_WEIGHTS.get(modality, MODALITY_WEIGHTS["Unknown"])

    # Bulk < 20 samples: 0.65 instead of 0.80
    if modality == "bulkRNA-seq":
        if n_samples is not None and n_samples < 20:
            weight = 0.65

    # Cell lines / organoids: ×0.30 override
    if _is_cell_line_or_organoid(abstract):
        weight = 0.30

    return weight


# ---------------------------------------------------------------------------
# Main scorer class
# ---------------------------------------------------------------------------

class ConfidenceScorer:
    """Compute five-dimension confidence scores for endometrial datasets.

    Parameters
    ----------
    None — scorer is stateless; all configuration is embedded in the
    class-level constants above.

    Methods
    -------
    score(dataset)
        Score a single dataset dict.
    score_all(datasets)
        Batch-score a list of dataset dicts.

    Examples
    --------
    >>> scorer = ConfidenceScorer()
    >>> result = scorer.score({"accession": "GSE12345", "modality": "scRNA-seq",
    ...                        "raw_data_available": True, "n_cells": 8000,
    ...                        "lh_timepoints": ["LH+2", "LH+7"],
    ...                        "sub_compartments": ["epithelium", "stroma", "uNK"]})
    >>> result["confidence_tier"]
    'BRONZE'
    """

    # ------------------------------------------------------------------
    # Dimension scorers
    # ------------------------------------------------------------------

    def _score_dqs(
        self,
        dataset: dict,
        abstract: str,
        modality: str,
    ) -> tuple[float, dict]:
        """Dimension 1 — Data Quality Score (max 25).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.
        abstract : str
            Cleaned abstract string.
        modality : str
            Normalised modality string.

        Returns
        -------
        tuple[float, dict]
            (score, breakdown) where breakdown maps sub-criterion → points.
        """
        breakdown: dict[str, float] = {}
        score = 0.0

        # +10 raw data available
        if dataset.get("raw_data_available") is True:
            breakdown["raw_data_available"] = 10.0
            score += 10.0
        else:
            breakdown["raw_data_available"] = 0.0

        # +5 QC metrics reported
        if _qc_metrics_reported(abstract):
            breakdown["qc_metrics_reported"] = 5.0
            score += 5.0
        else:
            breakdown["qc_metrics_reported"] = 0.0

        # +5 cell/sample count threshold
        n_cells   = dataset.get("n_cells")
        n_samples = dataset.get("n_samples")
        count_ok  = False
        if modality in ("scRNA-seq", "snRNA-seq", "Spatial Transcriptomics",
                        "Spatial Proteomics"):
            count_ok = isinstance(n_cells, int) and n_cells >= 5000
        else:
            count_ok = isinstance(n_samples, int) and n_samples >= 10
        if count_ok:
            breakdown["count_threshold"] = 5.0
            score += 5.0
        else:
            breakdown["count_threshold"] = 0.0

        # +5 sequencing depth adequate
        if _sequencing_depth_adequate(abstract, modality):
            breakdown["sequencing_depth_adequate"] = 5.0
            score += 5.0
        else:
            breakdown["sequencing_depth_adequate"] = 0.0

        return score, breakdown

    def _score_trs(
        self,
        dataset: dict,
        abstract: str,
    ) -> tuple[float, dict]:
        """Dimension 2 — Temporal Resolution Score (max 25).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.
        abstract : str
            Cleaned abstract string.

        Returns
        -------
        tuple[float, dict]
            (score, breakdown).
        """
        breakdown: dict[str, float] = {}
        score = 0.0

        lh_timepoints: list[str] = _safe_list(dataset.get("lh_timepoints"))

        # +2 per distinct LH-referenced timepoint, capped at 10
        n_distinct = len(set(lh_timepoints))
        tp_pts = min(n_distinct * 2.0, 10.0)
        breakdown["lh_timepoints_count"] = tp_pts
        score += tp_pts

        # +10 WOI timepoints present
        if _has_woi_timepoints(lh_timepoints):
            breakdown["woi_timepoints_present"] = 10.0
            score += 10.0
        else:
            breakdown["woi_timepoints_present"] = 0.0

        # +5 longitudinal design
        if _is_longitudinal(abstract):
            breakdown["longitudinal_design"] = 5.0
            score += 5.0
        else:
            breakdown["longitudinal_design"] = 0.0

        return score, breakdown

    def _score_srs(
        self,
        dataset: dict,
        modality: str,
    ) -> tuple[float, dict]:
        """Dimension 3 — Sub-compartment Resolution Score (max 20).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.
        modality : str
            Normalised modality string.

        Returns
        -------
        tuple[float, dict]
            (score, breakdown).
        """
        breakdown: dict[str, float] = {}
        score = 0.0

        sub_compartments: list[str] = _safe_list(dataset.get("sub_compartments"))

        # +1 per distinct compartment, capped at 10
        n_comp = len(set(sub_compartments))
        comp_pts = min(float(n_comp), 10.0)
        breakdown["sub_compartment_count"] = comp_pts
        score += comp_pts

        # +5 immune compartment present
        if _has_immune_compartment(sub_compartments):
            breakdown["immune_compartment_present"] = 5.0
            score += 5.0
        else:
            breakdown["immune_compartment_present"] = 0.0

        # +5 spatial modality
        if modality in _SPATIAL_MODALITIES:
            breakdown["spatial_modality"] = 5.0
            score += 5.0
        else:
            breakdown["spatial_modality"] = 0.0

        return score, breakdown

    def _score_mcs(
        self,
        dataset: dict,
        abstract: str,
    ) -> tuple[float, dict]:
        """Dimension 4 — Metadata Completeness Score (max 15).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.
        abstract : str
            Cleaned abstract string.

        Returns
        -------
        tuple[float, dict]
            (score, breakdown).
        """
        breakdown: dict[str, float] = {}
        score = 0.0

        meta: dict = dataset.get("metadata") or {}

        # +3 age and BMI reported
        age_present = (
            "age" in meta
            or _abstract_contains(abstract, "age", "years old", "year-old")
        )
        bmi_present = (
            "bmi" in meta
            or _abstract_contains(abstract, "bmi", "body mass index")
        )
        if age_present and bmi_present:
            breakdown["age_and_bmi"] = 3.0
            score += 3.0
        else:
            breakdown["age_and_bmi"] = 0.0

        # +5 cycle phase or hormonal status annotated
        cycle_phases: list[str] = _safe_list(dataset.get("cycle_phases"))
        if cycle_phases:
            breakdown["cycle_phases_annotated"] = 5.0
            score += 5.0
        else:
            breakdown["cycle_phases_annotated"] = 0.0

        # +4 disease groups labelled
        disease_groups: list[str] = _safe_list(dataset.get("disease_groups"))
        has_disease = bool(disease_groups) and disease_groups != ["healthy"]
        if has_disease:
            breakdown["disease_groups_labelled"] = 4.0
            score += 4.0
        else:
            breakdown["disease_groups_labelled"] = 0.0

        # +3 parity/reproductive history
        if _abstract_contains(
            abstract,
            "parity", "parous", "nulliparous", "reproductive history",
            "gravida", "para", "live birth", "prior pregnancy",
        ):
            breakdown["parity_reported"] = 3.0
            score += 3.0
        else:
            breakdown["parity_reported"] = 0.0

        return score, breakdown

    def _score_das(
        self,
        dataset: dict,
    ) -> tuple[float, dict]:
        """Dimension 5 — Dataset Accessibility Score (max 15).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.

        Returns
        -------
        tuple[float, dict]
            (score, breakdown).
        """
        breakdown: dict[str, float] = {}
        score = 0.0

        controlled  = dataset.get("controlled_access")
        download_url = dataset.get("download_url")
        doi          = _safe_str(dataset.get("doi"))
        supp_files: list[str] = _safe_list(dataset.get("supplemental_files"))

        # +10 fully public with download URL
        if controlled is not True and download_url is not None:
            breakdown["publicly_downloadable"] = 10.0
            score += 10.0
        else:
            breakdown["publicly_downloadable"] = 0.0

        # +3 stable DOI present
        if doi:
            breakdown["stable_doi"] = 3.0
            score += 3.0
        else:
            breakdown["stable_doi"] = 0.0

        # +2 multiple file formats (>1 distinct extension in supplemental files)
        if supp_files:
            exts = {
                f.rsplit(".", 1)[-1].lower()
                for f in supp_files
                if "." in f
            }
            if len(exts) > 1:
                breakdown["multiple_file_formats"] = 2.0
                score += 2.0
            else:
                breakdown["multiple_file_formats"] = 0.0
        else:
            breakdown["multiple_file_formats"] = 0.0

        return score, breakdown

    # ------------------------------------------------------------------
    # Penalty calculator
    # ------------------------------------------------------------------

    def _compute_penalties(
        self,
        dataset: dict,
        abstract: str,
    ) -> tuple[float, dict]:
        """Compute all applicable penalties (returned as a negative sum).

        Parameters
        ----------
        dataset : dict
            Full dataset metadata dict.
        abstract : str
            Cleaned abstract string.

        Returns
        -------
        tuple[float, dict]
            (total_penalty, breakdown) where total_penalty <= 0.
        """
        breakdown: dict[str, float] = {}
        total = 0.0

        # -10  no raw data
        if dataset.get("raw_data_available") is False:
            breakdown["no_raw_data"] = -10.0
            total -= 10.0

        # -8  n_patients < 3
        n_patients = dataset.get("n_patients")
        if n_patients is not None and isinstance(n_patients, int) and n_patients < 3:
            breakdown["underpowered_n_patients"] = -8.0
            total -= 8.0

        # -6  no cycle phase or LH annotation
        lh_timepoints: list[str] = _safe_list(dataset.get("lh_timepoints"))
        cycle_phases:  list[str] = _safe_list(dataset.get("cycle_phases"))
        if not lh_timepoints and not cycle_phases:
            breakdown["no_temporal_annotation"] = -6.0
            total -= 6.0

        # -3  preprint
        if _safe_str(dataset.get("peer_reviewed")).lower() == "preprint":
            breakdown["preprint"] = -3.0
            total -= 3.0

        # -5  missing critical metadata (n_patients AND n_samples both None)
        n_samples = dataset.get("n_samples")
        if n_patients is None and n_samples is None:
            breakdown["missing_critical_metadata"] = -5.0
            total -= 5.0

        # -5  controlled access barrier
        if dataset.get("controlled_access") is True:
            breakdown["controlled_access"] = -5.0
            total -= 5.0

        return total, breakdown

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def score(self, dataset: dict) -> dict:
        """Score a single dataset and return the full scoring result.

        Parameters
        ----------
        dataset : dict
            Dataset metadata dictionary.  All keys are optional; missing
            values are treated as None / empty.

        Returns
        -------
        dict
            Keys:
            - ``accession``        : str
            - ``DQS``              : float  (0–25)
            - ``TRS``              : float  (0–25)
            - ``SRS``              : float  (0–20)
            - ``MCS``              : float  (0–15)
            - ``DAS``              : float  (0–15)
            - ``penalties``        : float  (<= 0)
            - ``modality_weight``  : float
            - ``final_CS``         : float  (0–100, clamped)
            - ``confidence_tier``  : str
            - ``score_breakdown``  : dict   (human-readable sub-point breakdown)

        Examples
        --------
        >>> scorer = ConfidenceScorer()
        >>> r = scorer.score({"accession": "GSE99999",
        ...                    "raw_data_available": True,
        ...                    "modality": "scRNA-seq",
        ...                    "n_cells": 12000,
        ...                    "lh_timepoints": ["LH+7"],
        ...                    "sub_compartments": ["epithelium", "stroma",
        ...                                         "uNK", "macrophage"]})
        >>> 0 <= r["final_CS"] <= 100
        True
        """
        accession = _safe_str(dataset.get("accession")) or "UNKNOWN"
        abstract  = _safe_str(dataset.get("abstract"))
        modality  = _safe_str(dataset.get("modality")) or "Unknown"
        n_samples = dataset.get("n_samples")

        # --- Dimension scores ---
        dqs, dqs_bd = self._score_dqs(dataset, abstract, modality)
        trs, trs_bd = self._score_trs(dataset, abstract)
        srs, srs_bd = self._score_srs(dataset, modality)
        mcs, mcs_bd = self._score_mcs(dataset, abstract)
        das, das_bd = self._score_das(dataset)

        # --- Penalties ---
        penalties, pen_bd = self._compute_penalties(dataset, abstract)

        # --- Modality weight ---
        modality_weight = _resolve_modality_weight(modality, n_samples, abstract)

        # --- Final score ---
        raw      = dqs + trs + srs + mcs + das
        adjusted = raw + penalties               # penalties are negative
        final_cs = max(0.0, min(100.0, adjusted * modality_weight))

        # --- Human-readable breakdown ---
        score_breakdown: dict[str, Any] = {
            "DQS": {"total": dqs, "max": 25, **dqs_bd},
            "TRS": {"total": trs, "max": 25, **trs_bd},
            "SRS": {"total": srs, "max": 20, **srs_bd},
            "MCS": {"total": mcs, "max": 15, **mcs_bd},
            "DAS": {"total": das, "max": 15, **das_bd},
            "penalties": pen_bd,
            "raw_score_before_penalties": raw,
            "raw_score_after_penalties": adjusted,
            "modality_weight": modality_weight,
        }

        return {
            "accession":       accession,
            "DQS":             round(dqs, 2),
            "TRS":             round(trs, 2),
            "SRS":             round(srs, 2),
            "MCS":             round(mcs, 2),
            "DAS":             round(das, 2),
            "penalties":       round(penalties, 2),
            "modality_weight": round(modality_weight, 4),
            "final_CS":        round(final_cs, 2),
            "confidence_tier": classify_tier(final_cs),
            "score_breakdown": score_breakdown,
        }

    def score_all(self, datasets: list[dict]) -> list[dict]:
        """Batch-score a list of dataset dicts.

        Parameters
        ----------
        datasets : list[dict]
            List of dataset metadata dicts.

        Returns
        -------
        list[dict]
            List of score result dicts in the same order as the input.

        Examples
        --------
        >>> scorer = ConfidenceScorer()
        >>> results = scorer.score_all([{"accession": "GSE1"}, {"accession": "GSE2"}])
        >>> len(results)
        2
        """
        return [self.score(ds) for ds in datasets]


# ---------------------------------------------------------------------------
# Weighted scoring for Search Engine (user-adjustable dimension weights)
# ---------------------------------------------------------------------------

def score_with_weights(record: dict, weights: dict[str, float]) -> float:
    """
    Compute a confidence score using user-specified dimension weights.

    Parameters
    ----------
    record : dict
        Dataset record, expected to have AI-extracted fields including:
        lh_timepoints, tissue_sites, disease_groups, has_protocol,
        has_qc_metrics, has_raw_data, n_patients, n_samples,
        relevance_score (0-100), journal_if_estimate (0-100),
        download_url, controlled_access, doi, modality, peer_reviewed.
    weights : dict[str, float]
        Keys: journal_if, lh_timepoints, tissue_site, relevance,
              data_completeness, accessibility.
        Values: raw weights (will be normalised to sum=1 internally).
        Missing keys default to 0.

    Returns
    -------
    float
        Weighted confidence score clamped to [0, 100].
    """
    # ── Normalise weights ──────────────────────────────────────────────────
    keys = ("journal_if", "lh_timepoints", "tissue_site",
            "relevance", "data_completeness", "accessibility")
    raw_w = {k: float(weights.get(k, 0)) for k in keys}
    total_w = sum(raw_w.values())
    if total_w <= 0:
        total_w = 1.0
    norm = {k: v / total_w for k, v in raw_w.items()}

    # ── Component scores (each normalised 0–100 before weighting) ─────────

    # 1. Journal IF reliability (0–100 from AI estimate)
    ji = float(record.get("journal_if_estimate") or 0)
    ji = max(0.0, min(100.0, ji))

    # 2. LH timepoint availability (0–100)
    lh_tps = record.get("lh_timepoints") or []
    if isinstance(lh_tps, list):
        n_tp = len(set(lh_tps))
        woi = {"LH+5", "LH+6", "LH+7", "LH+8", "LH+9"}
        has_woi = bool(set(t.strip().upper().replace(" ", "") for t in lh_tps) & woi)
        lh_score = min(n_tp * 8.0, 60.0) + (40.0 if has_woi else 0.0)
    else:
        lh_score = 0.0
    lh_score = max(0.0, min(100.0, lh_score))

    # 3. Tissue site specificity (0–100)
    tissue = record.get("tissue_sites") or []
    n_tissue = len(tissue) if isinstance(tissue, list) else 0
    tissue_score = min(float(n_tissue) * 20.0, 100.0)

    # 4. Relevance to search query (0–100, from Gemini)
    rel = float(record.get("relevance_score") or 0)
    rel = max(0.0, min(100.0, rel))

    # 5. Data completeness / protocol metadata (0–100)
    flags = {
        "has_raw_data":    record.get("has_raw_data") or record.get("raw_data_available"),
        "has_qc_metrics":  record.get("has_qc_metrics"),
        "has_protocol":    record.get("has_protocol") or record.get("cell_isolation"),
        "has_n_patients":  (record.get("n_patients") or 0) > 0,
        "has_n_samples":   (record.get("n_samples") or 0) > 0,
    }
    completeness = sum(25.0 for k, v in flags.items() if v) * (4.0 / 5.0)
    completeness = max(0.0, min(100.0, completeness))

    # 6. Data accessibility (0–100)
    accessible = (
        not record.get("controlled_access")
        and bool(record.get("download_url") or record.get("url"))
    )
    has_doi = bool(record.get("doi"))
    accessibility = (60.0 if accessible else 0.0) + (40.0 if has_doi else 0.0)

    # ── Weighted sum ──────────────────────────────────────────────────────
    score = (
        norm["journal_if"]       * ji
        + norm["lh_timepoints"]  * lh_score
        + norm["tissue_site"]    * tissue_score
        + norm["relevance"]      * rel
        + norm["data_completeness"] * completeness
        + norm["accessibility"]  * accessibility
    )

    # Preprint penalty
    if _safe_str(record.get("peer_reviewed")).lower() == "preprint":
        score *= 0.85

    return round(max(0.0, min(100.0, score)), 2)
