"""
base.py
-------
Abstract base class for all Hickey Lab endometrial receptivity database scrapers.

Provides shared infrastructure: HTTP session with retry logic, rate limiting,
disk-based JSON caching, structured logging, and NLP helper functions used
by every concrete scraper.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Keyword vocabularies
# ---------------------------------------------------------------------------
_MODALITY_PATTERNS: list[tuple[str, str]] = [
    # Spatial Transcriptomics (check before scRNA to avoid false positives)
    (
        r"visium|merfish|seqfish|xenium|slide-?seq|stereo-?seq|"
        r"spatial transcriptom|in ?situ sequenc|spatial rna",
        "Spatial Transcriptomics",
    ),
    # Spatial Proteomics
    (
        r"codex|imaging mass cytom|imc|cycif|geomx|mibi|"
        r"spatial proteom|multiplexed imaging|imaging cytom",
        "Spatial Proteomics",
    ),
    # scRNA-seq / single-cell
    (
        r"10x genomics|10x chromium|scrna-?seq|single.?cell rna|"
        r"single.?cell sequenc|droplet.?based|smart-?seq|cel-?seq|"
        r"indrops|snrna-?seq|single.?nucleus",
        "scRNA-seq",
    ),
    # Bulk RNA-seq / microarray
    (
        r"bulk rna-?seq|rna-?seq|microarray|transcriptom|"
        r"gene expression profil|mrna sequenc|total rna",
        "bulkRNA-seq",
    ),
]

_LH_TIMEPOINT_DIRECT = re.compile(
    r"LH\s*[+\-]?\s*\d+",
    re.IGNORECASE,
)

_CYCLE_PHASE_MAP: dict[str, str] = {
    r"proliferative\s+phase|proliferative\s+endometrium": "proliferative",
    r"early\s+secretory": "early secretory",
    r"mid.?secretory|midsecretory": "mid-secretory",
    r"late\s+secretory": "late secretory",
    r"window\s+of\s+implantation|WOI": "window of implantation",
    r"menstrual\s+phase": "menstrual",
    r"pre.?ovulatory": "pre-ovulatory",
    r"post.?ovulatory": "post-ovulatory",
    r"luteal\s+phase": "luteal",
    r"follicular\s+phase": "follicular",
}

_SUB_COMPARTMENTS: list[str] = [
    "luminal epithelium",
    "glandular epithelium",
    "stroma",
    "stromal fibroblasts",
    "uNK",
    "uterine natural killer",
    "macrophage",
    "T cell",
    "endothelium",
    "decidual",
    "smooth muscle",
    "B cell",
    "dendritic cell",
    "mast cell",
    "pericyte",
]

# ---------------------------------------------------------------------------
# Helper functions (module-level, importable)
# ---------------------------------------------------------------------------


def _detect_modality(text: str) -> str:
    """
    Infer the assay modality from free-text description.

    Parameters
    ----------
    text : str
        Any free-text field (title, abstract, description, platform string).

    Returns
    -------
    str
        One of ``"scRNA-seq"``, ``"bulkRNA-seq"``,
        ``"Spatial Transcriptomics"``, ``"Spatial Proteomics"``,
        or ``"Unknown"``.
    """
    if not text:
        return "Unknown"
    lowered = text.lower()
    for pattern, label in _MODALITY_PATTERNS:
        if re.search(pattern, lowered, re.IGNORECASE):
            return label
    return "Unknown"


def _parse_lh_timepoints(text: str) -> list[str]:
    """
    Extract LH-relative timepoints and named cycle phases from text.

    Parameters
    ----------
    text : str
        Abstract, description, or any free-text field.

    Returns
    -------
    list[str]
        Deduplicated list of timepoint strings, e.g.
        ``["LH+5", "LH+7", "mid-secretory"]``.
    """
    if not text:
        return []
    found: list[str] = []

    # Numeric LH offsets — normalise spacing
    for m in _LH_TIMEPOINT_DIRECT.finditer(text):
        raw = m.group(0)
        normalised = re.sub(r"\s+", "", raw).upper()
        found.append(normalised)

    # Named cycle phases
    for pattern, label in _CYCLE_PHASE_MAP.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for item in found:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _parse_sub_compartments(text: str) -> list[str]:
    """
    Identify uterine cell / tissue sub-compartments mentioned in text.

    Parameters
    ----------
    text : str
        Abstract, description, or cell-type annotation list.

    Returns
    -------
    list[str]
        Matched compartment labels from the canonical vocabulary.
    """
    if not text:
        return []
    lowered = text.lower()
    matched: list[str] = []
    for compartment in _SUB_COMPARTMENTS:
        if compartment.lower() in lowered:
            matched.append(compartment)
    return matched


def _now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _cache_key(url: str, params: Optional[dict] = None) -> str:
    """
    Compute a deterministic cache filename from a URL and query params.

    Parameters
    ----------
    url : str
        Request URL.
    params : dict, optional
        Query parameters dict.

    Returns
    -------
    str
        Hex digest (SHA-256, first 16 chars) suitable as a filename stem.
    """
    payload = url + json.dumps(params or {}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------


class BaseScraper(ABC):
    """
    Abstract base class for all database scrapers.

    Provides HTTP session management, exponential-backoff retry, per-request
    rate limiting, and transparent disk caching of JSON responses.

    Parameters
    ----------
    api_key : str, optional
        API key forwarded as a query parameter or header where applicable.
    delay : float, optional
        Minimum seconds to sleep between consecutive outbound HTTP requests.
        Default ``0.34`` s (≈ 3 requests/second).
    cache_dir : str or Path, optional
        Directory for JSON response cache files.  Created automatically if it
        does not exist.  If *None*, caching is disabled.

    Attributes
    ----------
    session : requests.Session
        Shared HTTP session with ``User-Agent`` header set.
    delay : float
        Inter-request sleep interval in seconds.
    cache_dir : Path or None
        Resolved cache directory path, or *None* if caching is off.
    api_key : str or None
        API key for the target service.
    """

    SOURCE_DB: str = "Unknown"

    def __init__(
        self,
        api_key: Optional[str] = None,
        delay: float = 0.34,
        cache_dir: Optional[str | Path] = None,
    ) -> None:
        self.api_key = api_key
        self.delay = delay
        self._last_request_time: float = 0.0

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "HickeyLabEndometrialReceptivityScraper/1.0 "
                    "(Duke University; research use)"
                ),
                "Accept": "application/json",
            }
        )

        # Disk cache
        if cache_dir is not None:
            self.cache_dir: Optional[Path] = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_dir = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Sleep if necessary to honour ``self.delay`` between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.monotonic()

    def _load_cache(self, key: str) -> Optional[Any]:
        """
        Load a cached JSON response from disk.

        Parameters
        ----------
        key : str
            Cache key (file stem).

        Returns
        -------
        Any or None
            Parsed JSON object, or *None* on cache miss or read error.
        """
        if self.cache_dir is None:
            return None
        path = self.cache_dir / f"{key}.json"
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as fh:
                    return json.load(fh)
            except (json.JSONDecodeError, OSError) as exc:
                self._log(f"Cache read failed for {key}: {exc}")
        return None

    def _save_cache(self, key: str, data: Any) -> None:
        """
        Persist a JSON-serialisable object to the cache directory.

        Parameters
        ----------
        key : str
            Cache key (file stem).
        data : Any
            JSON-serialisable object to persist.
        """
        if self.cache_dir is None:
            return
        path = self.cache_dir / f"{key}.json"
        try:
            with path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
        except OSError as exc:
            self._log(f"Cache write failed for {key}: {exc}")

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _get(
        self,
        url: str,
        params: Optional[dict] = None,
        **kwargs: Any,
    ) -> Any:
        """
        HTTP GET with retry, rate limiting, and transparent disk caching.

        Attempts are made up to 3 times with exponential back-off (2 s, 4 s,
        8 s).  Successful JSON responses are stored in ``cache_dir`` and
        returned from cache on subsequent identical requests.

        Parameters
        ----------
        url : str
            Full URL to request.
        params : dict, optional
            Query string parameters.
        **kwargs
            Additional keyword arguments forwarded to ``requests.Session.get``.

        Returns
        -------
        Any
            Parsed JSON response body.

        Raises
        ------
        requests.HTTPError
            If the server returns a 4xx/5xx status after all retries.
        requests.RequestException
            On network-level failures after all retries.
        """
        key = _cache_key(url, params)
        cached = self._load_cache(key)
        if cached is not None:
            self._log(f"Cache hit: {url}")
            return cached

        self._rate_limit()
        self._log(f"GET {url} params={params}")
        response = self.session.get(url, params=params, timeout=30, **kwargs)
        response.raise_for_status()
        data = response.json()
        self._save_cache(key, data)
        return data

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _post(
        self,
        url: str,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
        **kwargs: Any,
    ) -> Any:
        """
        HTTP POST with retry and rate limiting.

        POST responses are **not** disk-cached (non-idempotent).

        Parameters
        ----------
        url : str
            Full URL to POST to.
        data : dict, optional
            Form-encoded body.
        json : dict, optional
            JSON body (mutually exclusive with *data*).
        **kwargs
            Additional keyword arguments forwarded to ``requests.Session.post``.

        Returns
        -------
        Any
            Parsed JSON response body.

        Raises
        ------
        requests.HTTPError
            If the server returns a 4xx/5xx status after all retries.
        requests.RequestException
            On network-level failures after all retries.
        """
        self._rate_limit()
        self._log(f"POST {url}")
        response = self.session.post(
            url, data=data, json=json, timeout=30, **kwargs
        )
        response.raise_for_status()
        return response.json()

    def _log(self, msg: str) -> None:
        """
        Emit a timestamped INFO log line.

        Parameters
        ----------
        msg : str
            Message to log.
        """
        logger.info("[%s] %s", self.__class__.__name__, msg)

    # ------------------------------------------------------------------
    # Shared NLP helpers (delegates to module-level functions)
    # ------------------------------------------------------------------

    @staticmethod
    def detect_modality(text: str) -> str:
        """Delegate to module-level :func:`_detect_modality`."""
        return _detect_modality(text)

    @staticmethod
    def parse_lh_timepoints(text: str) -> list[str]:
        """Delegate to module-level :func:`_parse_lh_timepoints`."""
        return _parse_lh_timepoints(text)

    @staticmethod
    def parse_sub_compartments(text: str) -> list[str]:
        """Delegate to module-level :func:`_parse_sub_compartments`."""
        return _parse_sub_compartments(text)

    @staticmethod
    def now_iso() -> str:
        """Return current UTC time as ISO-8601 string."""
        return _now_iso()

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def search(self, query: str, **kwargs: Any) -> list[dict]:
        """
        Search the target database and return a list of accession records.

        Parameters
        ----------
        query : str
            Free-text search query.
        **kwargs
            Scraper-specific keyword arguments (e.g. ``max_results``).

        Returns
        -------
        list[dict]
            Each element is a minimal dict containing at least ``"accession"``
            and ``"source_db"``.
        """

    @abstractmethod
    def fetch_metadata(self, accession: str) -> dict:
        """
        Fetch full metadata for a single dataset accession.

        Parameters
        ----------
        accession : str
            Dataset identifier (GSE*, E-MTAB-*, project UUID, etc.).

        Returns
        -------
        dict
            Standardised metadata dict conforming to the pipeline schema.
        """

    # ------------------------------------------------------------------
    # Standardised record factory
    # ------------------------------------------------------------------

    def _empty_record(self) -> dict:
        """
        Return a metadata dict pre-filled with schema defaults.

        Returns
        -------
        dict
            All keys from the pipeline schema with sensible defaults.
        """
        return {
            "accession": None,
            "source_db": self.SOURCE_DB,
            "title": None,
            "doi": None,
            "pubmed_id": None,
            "abstract": None,
            "authors": None,
            "journal": None,
            "journal_if": None,
            "peer_reviewed": "Unknown",
            "year": None,
            "modality": "Unknown",
            "platform": None,
            "n_patients": None,
            "n_samples": None,
            "n_cells": None,
            "organism": "Homo sapiens",
            "lh_timepoints": [],
            "cycle_phases": [],
            "sub_compartments": [],
            "disease_groups": [],
            "raw_data_available": None,
            "download_url": None,
            "file_size_gb": None,
            "controlled_access": False,
            "date_scraped": _now_iso(),
        }
