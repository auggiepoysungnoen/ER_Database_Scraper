"""
singlecellportal.py
-------------------
Scraper for the Broad Institute Single Cell Portal (SCP) using its REST API.

Retrieves single-cell studies relevant to endometrial receptivity and
normalises results into the pipeline's standardised metadata schema.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .base import BaseScraper, _detect_modality, _parse_lh_timepoints, _parse_sub_compartments

_DISEASE_KEYWORDS: dict[str, str] = {
    r"endometriosis": "endometriosis",
    r"recurrent\s+implantation\s+failure|RIF": "RIF",
    r"recurrent\s+pregnancy\s+loss|RPL": "RPL",
    r"infertil": "infertility",
    r"PCOS|polycystic": "PCOS",
    r"adenomyosis": "adenomyosis",
    r"leiomyoma|fibroid": "leiomyoma",
    r"endometrial\s+cancer|uterine\s+cancer|endometrial\s+carcinoma": "endometrial cancer",
    r"normal|healthy|control": "healthy",
}


def _detect_disease_groups(text: str) -> list[str]:
    found: list[str] = []
    for pattern, label in _DISEASE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)
    return found


class SingleCellPortalScraper(BaseScraper):
    """
    Scraper for the Broad Institute Single Cell Portal via its REST API.

    Parameters
    ----------
    api_key : str, optional
        Bearer token for the SCP API.  Public studies are accessible
        without authentication; private studies require a token.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    SCP API documentation:
    https://singlecell.broadinstitute.org/single_cell/api/swagger_docs/v1
    """

    SOURCE_DB = "SCP"
    BASE_URL = "https://singlecell.broadinstitute.org/single_cell/api/v1/"

    def __init__(
        self,
        api_key: Optional[str] = None,
        delay: float = 0.34,
        cache_dir=None,
    ) -> None:
        super().__init__(api_key=api_key, delay=delay, cache_dir=cache_dir)
        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_doi(self, study: dict) -> Optional[str]:
        """
        Extract a DOI from an SCP study record.

        Parameters
        ----------
        study : dict
            Raw study JSON from SCP API.

        Returns
        -------
        str or None
            DOI string, or *None* if not found.
        """
        # Check publication field variants
        for key in ("publication_doi", "doi", "publication_url"):
            val = study.get(key, "") or ""
            if val:
                return re.sub(r"https?://doi\.org/", "", str(val))

        # Check nested publication object
        pub = study.get("publication", {}) or {}
        if isinstance(pub, dict):
            for key in ("doi", "url"):
                val = pub.get(key, "") or ""
                if val and ("doi" in val.lower() or val.startswith("10.")):
                    return re.sub(r"https?://doi\.org/", "", str(val))
        return None

    def _extract_authors(self, study: dict) -> Optional[str]:
        """
        Build a short author string from an SCP study record.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str or None
            ``"LastName et al."`` style string, or *None*.
        """
        # Direct fields
        for key in ("authors", "attribution", "contact_name"):
            val = study.get(key, "") or ""
            if val:
                return str(val)

        # Nested publication
        pub = study.get("publication", {}) or {}
        if isinstance(pub, dict):
            authors_val = pub.get("authors", "") or ""
            if authors_val:
                return str(authors_val)
        return None

    def _extract_cell_count(self, study: dict) -> Optional[int]:
        """
        Extract cell count from an SCP study record.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        int or None
            Cell count, or *None*.
        """
        for key in ("cell_count", "num_cells", "total_cell_count"):
            val = study.get(key)
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass
        return None

    def _extract_species(self, study: dict) -> str:
        """
        Extract species / organism from an SCP study record.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str
            Organism name, defaulting to ``"Homo sapiens"``.
        """
        for key in ("species", "organism", "taxon_name"):
            val = study.get(key, "") or ""
            if val:
                if isinstance(val, list):
                    return val[0] if val else "Homo sapiens"
                return str(val)
        return "Homo sapiens"

    def _extract_data_links(self, study: dict) -> Optional[str]:
        """
        Extract a primary download URL from an SCP study record.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str or None
            Download URL, or *None*.
        """
        for key in ("study_url", "accession_url", "data_url"):
            val = study.get(key, "") or ""
            if val:
                return str(val)
        # Construct from accession
        acc = study.get("accession", "") or ""
        if acc:
            return f"https://singlecell.broadinstitute.org/single_cell/study/{acc}"
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        max_results: int = 200,
        **kwargs: Any,
    ) -> list[dict]:
        """
        Search Single Cell Portal for studies matching a query.

        Parameters
        ----------
        query : str
            Free-text search query, e.g. ``"endometrium receptivity"``.
        max_results : int, optional
            Maximum number of studies to return.  Default 200.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` and ``"source_db"``.
        """
        self._log(f"Searching SCP: query='{query}' max={max_results}")
        params: dict[str, Any] = {
            "type": "study",
            "terms": query,
            "limit": min(max_results, 100),
        }
        results: list[dict] = []
        page = 1

        while len(results) < max_results:
            params["page"] = page
            try:
                data = self._get(self.BASE_URL + "search", params=params)
            except Exception as exc:
                self._log(f"SCP search page {page} failed: {exc}")
                break

            studies = data.get("studies", []) or data.get("results", []) or []
            if not studies:
                break

            for study in studies:
                acc = study.get("accession", "") or study.get("study_accession", "") or ""
                if acc:
                    results.append({"accession": acc, "source_db": self.SOURCE_DB})

            total = data.get("total_studies", 0) or data.get("total", 0) or 0
            self._log(
                f"SCP page {page}: {len(studies)} studies "
                f"(total={total}, collected={len(results)})"
            )

            if len(results) >= total or len(studies) < params["limit"]:
                break
            page += 1

        return results[:max_results]

    def fetch_metadata(self, accession: str) -> dict:
        """
        Fetch and normalise full metadata for an SCP study.

        Parameters
        ----------
        accession : str
            SCP study accession, e.g. ``"SCP123"``.

        Returns
        -------
        dict
            Standardised metadata dict.
        """
        record = self._empty_record()
        record["accession"] = accession

        try:
            study = self._get(self.BASE_URL + f"studies/{accession}")
        except Exception as exc:
            self._log(f"fetch_metadata failed for {accession}: {exc}")
            return record

        # Core fields
        record["title"] = study.get("name") or study.get("title") or None
        record["abstract"] = study.get("description") or None
        record["n_cells"] = self._extract_cell_count(study)
        record["organism"] = self._extract_species(study)
        record["doi"] = self._extract_doi(study)
        record["authors"] = self._extract_authors(study)
        record["download_url"] = self._extract_data_links(study)

        if record["doi"]:
            record["peer_reviewed"] = "Yes"

        # Disease / condition
        for key in ("disease", "condition", "pathology"):
            val = study.get(key, "") or ""
            if val:
                if isinstance(val, list):
                    record["disease_groups"] = val
                else:
                    record["disease_groups"] = [str(val)]
                break

        # Modality
        combined_text = " ".join(
            filter(None, [record["title"], record["abstract"]])
        )
        record["modality"] = _detect_modality(combined_text)

        # Platform
        for key in ("technology", "library_protocol", "platform"):
            val = study.get(key, "") or ""
            if val:
                record["platform"] = str(val)
                break

        # Year
        for key in ("created_at", "updated_at", "publication_year"):
            val = study.get(key, "") or ""
            year_match = re.search(r"\b(20\d{2})\b", str(val))
            if year_match:
                record["year"] = int(year_match.group(1))
                break

        # NLP-derived fields
        full_text = " ".join(filter(None, [record["title"], record["abstract"]]))
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        record["sub_compartments"] = _parse_sub_compartments(full_text)
        if not record["disease_groups"]:
            record["disease_groups"] = _detect_disease_groups(full_text)

        # SCP is generally open access
        record["controlled_access"] = False
        record["raw_data_available"] = True

        return record
