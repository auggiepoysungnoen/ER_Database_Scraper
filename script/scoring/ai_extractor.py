"""
ai_extractor.py
===============
Gemini-powered metadata extraction for genomics dataset records.

Reads the abstract (and title) of a dataset and returns structured metadata
fields that are otherwise missing from raw API responses:

    - lh_timepoints    : list of inferred LH+N staging labels
    - tissue_sites     : list of tissue / sub-compartment terms
    - disease_groups   : list of clinical phenotype labels
    - n_patients       : estimated human donor / patient count
    - n_samples        : estimated biological sample count
    - has_protocol     : True if methods describe a clear experimental protocol
    - has_qc_metrics   : True if quality-control metrics are mentioned
    - has_raw_data     : True if raw data (FASTQ / BAM) availability is indicated
    - cell_isolation   : True if single-cell isolation method is described
    - library_prep     : True if library prep kit / method is named

Usage
-----
    from scoring.ai_extractor import extract_metadata, batch_enrich

    # single record
    extra = extract_metadata("GSE111976", title, abstract, api_key)
    record.update(extra)

    # batch
    enriched = batch_enrich(records, api_key, max_workers=8)
"""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a biomedical data-curation assistant. Given the title and abstract of
a genomics dataset, extract the following fields and return ONLY valid JSON
(no markdown fences, no extra text).

Return this exact JSON schema — use null for missing values:
{
  "lh_timepoints": ["LH+0", "LH+2"],
  "tissue_sites": ["endometrium", "decidua"],
  "disease_groups": ["fertile control", "unexplained infertility"],
  "n_patients": 12,
  "n_samples": 48,
  "has_protocol": true,
  "has_qc_metrics": false,
  "has_raw_data": true,
  "cell_isolation": true,
  "library_prep": false
}

Rules:
- lh_timepoints: any LH+N, cycle day, secretory phase, proliferative phase
  labels you can infer from the text. Use standard "LH+N" format when possible.
  Use ["unknown"] if no temporal staging is mentioned.
- tissue_sites: anatomical compartments (endometrium, decidua, myometrium,
  cervix, fallopian tube, placenta, ovary, blood, PBMC, etc.)
- disease_groups: clinical phenotypes (fertile control, unexplained infertility,
  endometriosis, PCOS, recurrent implantation failure, adenomyosis, etc.)
  Use ["healthy control"] if only healthy participants.
- n_patients: integer count of individual donors; null if not stated.
- n_samples: integer count of biological samples; null if not stated.
- has_protocol: true if cell isolation / fixation / dissociation protocol is
  clearly described.
- has_qc_metrics: true if QC thresholds, doublet removal, or filtering stats
  are mentioned.
- has_raw_data: true if raw sequencing files (FASTQ, BAM) are mentioned as
  available or deposited.
- cell_isolation: true if single-cell isolation technique is named.
- library_prep: true if a library preparation kit or protocol name is mentioned.
"""

_USER_TEMPLATE = """\
Accession: {accession}
Title: {title}
Abstract: {abstract}
"""

# ---------------------------------------------------------------------------
# Gemini client helper
# ---------------------------------------------------------------------------

def _get_model(api_key: str) -> Any:
    """
    Return a ``google.generativeai.GenerativeModel`` for gemini-1.5-flash.

    Parameters
    ----------
    api_key : str
        Gemini API key.

    Returns
    -------
    GenerativeModel
    """
    import google.generativeai as genai  # type: ignore
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        system_instruction=_SYSTEM_PROMPT,
        generation_config={
            "temperature": 0.0,
            "max_output_tokens": 512,
            "response_mime_type": "application/json",
        },
    )


# ---------------------------------------------------------------------------
# Single-record extraction
# ---------------------------------------------------------------------------

def extract_metadata(
    accession: str,
    title: str,
    abstract: str,
    api_key: str,
    retries: int = 2,
    backoff: float = 2.0,
) -> dict:
    """
    Extract structured metadata from a dataset abstract using Gemini.

    Parameters
    ----------
    accession : str
        Dataset accession number (for logging).
    title : str
        Dataset title.
    abstract : str
        Dataset abstract / summary text.
    api_key : str
        Gemini API key.
    retries : int
        Number of retry attempts on transient API errors.
    backoff : float
        Seconds to wait between retries (doubles each attempt).

    Returns
    -------
    dict
        Extracted fields (keys defined in schema above).  On failure returns
        an empty dict so the caller can safely merge without breaking scoring.
    """
    if not abstract and not title:
        return {}

    prompt = _USER_TEMPLATE.format(
        accession=accession or "UNKNOWN",
        title=title or "",
        abstract=(abstract or "")[:4000],  # truncate very long abstracts
    )

    attempt = 0
    wait = backoff
    while attempt <= retries:
        try:
            model = _get_model(api_key)
            response = model.generate_content(prompt)
            raw = response.text.strip()

            # Strip markdown fences if Gemini returns them despite mime type
            raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
            raw = re.sub(r"```$", "", raw).strip()

            data = json.loads(raw)

            # Normalise types
            for bool_key in ("has_protocol", "has_qc_metrics", "has_raw_data",
                             "cell_isolation", "library_prep"):
                if bool_key in data and data[bool_key] is not None:
                    data[bool_key] = bool(data[bool_key])

            for list_key in ("lh_timepoints", "tissue_sites", "disease_groups"):
                if list_key in data and isinstance(data[list_key], str):
                    data[list_key] = [data[list_key]]
                elif list_key not in data or data[list_key] is None:
                    data[list_key] = []

            log.debug("[AI] %s extracted: timepoints=%s tissue=%s disease=%s",
                      accession,
                      data.get("lh_timepoints"),
                      data.get("tissue_sites"),
                      data.get("disease_groups"))
            return data

        except json.JSONDecodeError as exc:
            log.warning("[AI] JSON parse failed for %s: %s | raw=%r",
                        accession, exc, raw[:200] if "raw" in dir() else "")
            return {}
        except Exception as exc:
            err_str = str(exc)
            # Rate-limit: back off and retry
            if "429" in err_str or "quota" in err_str.lower() or "rate" in err_str.lower():
                if attempt < retries:
                    log.debug("[AI] Rate limit on %s — waiting %.0fs", accession, wait)
                    time.sleep(wait)
                    wait *= 2
                    attempt += 1
                    continue
            log.warning("[AI] extract_metadata failed for %s: %s", accession, exc)
            return {}

    return {}


# ---------------------------------------------------------------------------
# Batch enrichment
# ---------------------------------------------------------------------------

def batch_enrich(
    records: list[dict],
    api_key: str,
    max_workers: int = 6,
    skip_if_enriched: bool = True,
) -> list[dict]:
    """
    Run Gemini metadata extraction over a list of records in parallel.

    For each record with an abstract (or title), ``extract_metadata`` is called
    and the returned fields are merged into the record dict only when the field
    is currently missing or empty.

    Parameters
    ----------
    records : list[dict]
        Dataset records (mutated in-place).
    api_key : str
        Gemini API key.
    max_workers : int
        Thread-pool concurrency for API calls.
    skip_if_enriched : bool
        If True, skip records that already have ``ai_enriched=True``.

    Returns
    -------
    list[dict]
        Same list with AI-extracted fields merged in.
    """
    if not api_key:
        log.warning("[AI] No Gemini API key — batch enrichment skipped")
        return records

    to_process = []
    for rec in records:
        if skip_if_enriched and rec.get("ai_enriched"):
            continue
        abstract = rec.get("abstract") or rec.get("summary") or rec.get("description") or ""
        title = rec.get("title") or rec.get("Title") or ""
        if abstract or title:
            to_process.append(rec)

    if not to_process:
        log.info("[AI] No records to enrich")
        return records

    log.info("[AI] Enriching %d records with Gemini (workers=%d) …",
             len(to_process), max_workers)

    enriched_count = 0
    errors = 0

    def _worker(rec: dict) -> tuple[dict, dict]:
        acc = rec.get("accession") or rec.get("Accession") or ""
        title = rec.get("title") or rec.get("Title") or ""
        abstract = (
            rec.get("abstract") or rec.get("summary")
            or rec.get("description") or ""
        )
        extra = extract_metadata(acc, title, abstract, api_key)
        return rec, extra

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker, rec): rec for rec in to_process}
        for future in as_completed(futures):
            try:
                rec, extra = future.result()
                if extra:
                    # Merge: only fill in missing / empty fields
                    for k, v in extra.items():
                        existing = rec.get(k)
                        is_empty = (
                            existing is None
                            or existing == ""
                            or existing == []
                            or existing == ["unknown"]
                        )
                        if is_empty and v is not None and v != [] and v != "":
                            rec[k] = v
                    rec["ai_enriched"] = True
                    enriched_count += 1
            except Exception as exc:
                errors += 1
                log.warning("[AI] worker error: %s", exc)

    log.info("[AI] Enrichment complete: %d enriched, %d errors",
             enriched_count, errors)
    return records


# ---------------------------------------------------------------------------
# Streamlit helper (real-time single-record enrichment from the UI)
# ---------------------------------------------------------------------------

def enrich_record_live(record: dict, api_key: str) -> dict:
    """
    Enrich a single record on demand (e.g., when a user views Dataset Detail).

    Suitable for calling directly from a Streamlit page.  Returns the enriched
    record.  Does not mutate the original if enrichment fails.

    Parameters
    ----------
    record : dict
        A dataset record (from the registry).
    api_key : str
        Gemini API key.

    Returns
    -------
    dict
        Record with AI fields merged in (or original on failure).
    """
    if record.get("ai_enriched"):
        return record

    acc = record.get("accession") or ""
    title = record.get("title") or record.get("Title") or ""
    abstract = (
        record.get("abstract") or record.get("summary")
        or record.get("description") or ""
    )

    extra = extract_metadata(acc, title, abstract, api_key)
    if extra:
        merged = {**record}
        for k, v in extra.items():
            existing = merged.get(k)
            is_empty = (
                existing is None or existing == "" or existing == []
                or existing == ["unknown"]
            )
            if is_empty and v is not None and v != [] and v != "":
                merged[k] = v
        merged["ai_enriched"] = True
        return merged

    return record


# ---------------------------------------------------------------------------
# Search-query relevance scoring
# ---------------------------------------------------------------------------

_RELEVANCE_PROMPT = """\
You are a biomedical data-curation assistant. Given a dataset title and abstract,
score its relevance to a user's search query on a scale of 0-100, and also estimate
the journal impact factor reliability.

Return ONLY valid JSON (no markdown, no extra text):
{
  "relevance_score": 85,
  "journal_name": "Nature Communications",
  "journal_if_estimate": 88,
  "full_text_available": true,
  "machine_platform": "10x Genomics Chromium",
  "reasoning": "1-2 sentence justification"
}

Rules:
- relevance_score: 0 = completely irrelevant, 100 = perfect match to the query
- journal_if_estimate: 0-100 scale where 100 = Nature/Cell/Science/NEJM,
  80 = Nature sub-journals / high-impact specialty journals,
  60 = solid mid-tier journals (PLOS Biology, Nucleic Acids Research),
  40 = broad access journals (PLOS ONE, Scientific Reports),
  20 = low-tier or predatory journals, 0 = preprint / no journal
- full_text_available: true if paper likely has full text in PubMed Central or similar
- machine_platform: sequencing/profiling platform inferred from abstract (null if unknown)
- reasoning: brief explanation of the relevance score
"""

def score_relevance(
    abstract: str,
    title: str,
    search_query: str,
    api_key: str,
    journal_name: str = "",
) -> dict:
    """
    Score a dataset's relevance to a search query using Gemini.

    Parameters
    ----------
    abstract : str
        Dataset abstract.
    title : str
        Dataset title.
    search_query : str
        The user's natural-language search query.
    api_key : str
        Gemini API key.
    journal_name : str, optional
        Journal name if known.

    Returns
    -------
    dict
        Keys: relevance_score (0-100), journal_name, journal_if_estimate (0-100),
        full_text_available (bool), machine_platform (str), reasoning (str).
        Returns zeros on failure.
    """
    _DEFAULT = {
        "relevance_score": 0,
        "journal_name": journal_name or "",
        "journal_if_estimate": 0,
        "full_text_available": False,
        "machine_platform": None,
        "reasoning": "",
    }

    if not api_key or not (abstract or title):
        return _DEFAULT

    prompt = (
        f"Search query: {search_query}\n\n"
        f"Dataset title: {title or 'Unknown'}\n"
        f"Journal: {journal_name or 'Unknown'}\n"
        f"Abstract: {(abstract or '')[:3000]}"
    )

    try:
        import google.generativeai as genai  # type: ignore
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=_RELEVANCE_PROMPT,
            generation_config={
                "temperature": 0.0,
                "max_output_tokens": 256,
                "response_mime_type": "application/json",
            },
        )
        response = model.generate_content(prompt)
        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
        raw = re.sub(r"```$", "", raw).strip()
        data = json.loads(raw)
        result = {**_DEFAULT, **data}
        result["relevance_score"] = max(0, min(100, int(result.get("relevance_score") or 0)))
        result["journal_if_estimate"] = max(0, min(100, int(result.get("journal_if_estimate") or 0)))
        return result
    except Exception as exc:
        log.debug("score_relevance failed for query=%r: %s", search_query[:50], exc)
        return _DEFAULT


def extract_metadata_with_relevance(
    accession: str,
    title: str,
    abstract: str,
    search_query: str,
    api_key: str,
    journal_name: str = "",
) -> dict:
    """
    Single Gemini call that extracts both structured metadata AND relevance scoring.

    Combines extract_metadata() and score_relevance() into one API call
    for efficiency when processing search results.

    Parameters
    ----------
    accession : str
        Dataset accession.
    title : str
        Dataset title.
    abstract : str
        Dataset abstract.
    search_query : str
        The user's search query (used for relevance scoring).
    api_key : str
        Gemini API key.
    journal_name : str
        Journal name if known.

    Returns
    -------
    dict
        Combined metadata + relevance fields. Merges extract_metadata() and
        score_relevance() results.
    """
    meta = extract_metadata(accession, title, abstract, api_key)
    rel = score_relevance(abstract, title, search_query, api_key, journal_name)
    return {**meta, **rel}
