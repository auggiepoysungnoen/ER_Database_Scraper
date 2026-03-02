"""
arrayexpress.py
---------------
Scraper for EMBL-EBI ArrayExpress / BioStudies using the BioStudies REST API.

Retrieves functional genomics studies relevant to endometrial receptivity
and normalises results into the pipeline's standardised metadata schema.
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
    r"healthy|normal\s+endometrium|control": "healthy",
}


def _detect_disease_groups(text: str) -> list[str]:
    """
    Identify disease / condition groups mentioned in free text.

    Parameters
    ----------
    text : str
        Any free-text field.

    Returns
    -------
    list[str]
        Matched disease group labels.
    """
    found: list[str] = []
    for pattern, label in _DISEASE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)
    return found


class ArrayExpressScraper(BaseScraper):
    """
    Scraper for ArrayExpress / BioStudies via the EMBL-EBI BioStudies REST API.

    Parameters
    ----------
    api_key : str, optional
        Not required by the BioStudies API; reserved for future use.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    BioStudies API documentation:
    https://www.ebi.ac.uk/biostudies/help#rest-api
    """

    SOURCE_DB = "ArrayExpress"
    BASE_URL = "https://www.ebi.ac.uk/biostudies/api/v1/"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_links(self, study: dict) -> list[dict]:
        """
        Recursively collect all link objects from a BioStudies study document.

        Parameters
        ----------
        study : dict
            Raw study JSON returned by the BioStudies API.

        Returns
        -------
        list[dict]
            Flat list of link dicts, each typically containing ``"url"`` and
            ``"type"`` keys.
        """
        links: list[dict] = []
        section = study.get("section", {}) or {}

        def _collect(node: Any) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    if k == "links" and isinstance(v, list):
                        links.extend(v)
                    else:
                        _collect(v)
            elif isinstance(node, list):
                for item in node:
                    _collect(item)

        _collect(section)
        return links

    def _extract_attributes(self, study: dict) -> dict[str, str]:
        """
        Flatten BioStudies study attributes into a key→value dict.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        dict[str, str]
            Attribute name → value mapping (case-preserved keys).
        """
        attrs: dict[str, str] = {}
        for attr in study.get("attributes", []) or []:
            name = attr.get("name", "")
            value = attr.get("value", "")
            if name and value:
                attrs[name] = value
        return attrs

    def _extract_authors(self, study: dict) -> Optional[str]:
        """
        Build a short author string from a BioStudies study document.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str or None
            ``"LastName et al."`` style string, or *None* if no authors found.
        """
        authors: list[str] = []
        section = study.get("section", {}) or {}
        subsections = section.get("subsections", []) or []
        for sub in subsections:
            if isinstance(sub, list):
                sub = sub[0] if sub else {}
            stype = (sub.get("type", "") or "").lower()
            if stype in ("author", "authors"):
                for attr in sub.get("attributes", []) or []:
                    if (attr.get("name", "") or "").lower() == "name":
                        authors.append(attr.get("value", ""))
        if not authors:
            return None
        if len(authors) == 1:
            return authors[0]
        return f"{authors[0]} et al."

    def _extract_doi(self, study: dict) -> Optional[str]:
        """
        Extract a DOI from the study's publication links or attributes.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str or None
            DOI string without URL prefix, or *None*.
        """
        # Check top-level links
        for link in self._extract_links(study):
            url = link.get("url", "") or ""
            if "doi.org" in url:
                return re.sub(r"https?://doi\.org/", "", url)
        # Check attributes
        attrs = self._extract_attributes(study)
        for key, val in attrs.items():
            if "doi" in key.lower():
                return re.sub(r"https?://doi\.org/", "", val)
        return None

    def _extract_organism(self, study: dict) -> str:
        """
        Extract organism name from study attributes.

        Parameters
        ----------
        study : dict
            Raw study JSON.

        Returns
        -------
        str
            Organism name, defaulting to ``"Homo sapiens"``.
        """
        attrs = self._extract_attributes(study)
        for key in ("Organism", "organism", "Species", "species"):
            if key in attrs:
                return attrs[key]
        # Search in section attributes
        section = study.get("section", {}) or {}
        for attr in section.get("attributes", []) or []:
            if (attr.get("name", "") or "").lower() in ("organism", "species"):
                return attr.get("value", "Homo sapiens")
        return "Homo sapiens"

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
        Search BioStudies for functional genomics studies.

        Parameters
        ----------
        query : str
            Free-text search query, e.g. ``"endometrium receptivity RNA-seq"``.
        max_results : int, optional
            Maximum number of studies to return.  Default 200.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` and ``"source_db"``.
        """
        self._log(f"Searching BioStudies: query='{query}' max={max_results}")
        params: dict[str, Any] = {
            "query": query,
            "pageSize": min(max_results, 100),
            "type": "study",
        }
        results: list[dict] = []
        page = 1

        while len(results) < max_results:
            params["page"] = page
            try:
                data = self._get(self.BASE_URL + "search", params=params)
            except Exception as exc:
                self._log(f"BioStudies search page {page} failed: {exc}")
                break

            hits = data.get("hits", []) or []
            if not hits:
                break

            for hit in hits:
                acc = hit.get("accession", "") or ""
                if acc:
                    results.append(
                        {"accession": acc, "source_db": self.SOURCE_DB}
                    )

            total = data.get("totalHits", 0) or 0
            self._log(
                f"Page {page}: {len(hits)} hits (total={total}, collected={len(results)})"
            )

            if len(results) >= total or len(hits) < params["pageSize"]:
                break
            page += 1

        return results[:max_results]

    def fetch_metadata(self, accession: str) -> dict:
        """
        Fetch and normalise full metadata for a BioStudies / ArrayExpress study.

        Parameters
        ----------
        accession : str
            BioStudies accession, e.g. ``"E-MTAB-10287"`` or ``"S-BSST123"``.

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

        attrs = self._extract_attributes(study)

        # Title
        record["title"] = (
            study.get("title")
            or attrs.get("Title")
            or attrs.get("title")
            or None
        )

        # Description / abstract
        description = (
            attrs.get("Description")
            or attrs.get("description")
            or attrs.get("Abstract")
            or None
        )
        record["abstract"] = description

        # Organism
        record["organism"] = self._extract_organism(study)

        # Year from release date
        release_date = (
            study.get("releaseDate")
            or attrs.get("Release Date")
            or attrs.get("releasedate")
            or ""
        )
        year_match = re.search(r"\b(20\d{2}|19\d{2})\b", str(release_date))
        if year_match:
            record["year"] = int(year_match.group(1))

        # Authors
        record["authors"] = self._extract_authors(study)

        # DOI
        record["doi"] = self._extract_doi(study)
        if record["doi"]:
            record["peer_reviewed"] = "Yes"

        # Study type / modality
        study_type = attrs.get("Study type", "") or attrs.get("studytype", "") or ""
        combined_text = " ".join(
            filter(None, [record["title"], description, study_type])
        )
        record["modality"] = _detect_modality(combined_text)

        # Download links
        links = self._extract_links(study)
        if links:
            # Prefer FTP or direct data links
            for link in links:
                url = link.get("url", "") or ""
                if url.startswith("ftp://") or "data" in url.lower():
                    record["download_url"] = url
                    break
            if not record["download_url"] and links:
                record["download_url"] = links[0].get("url") or None

        # Raw data available heuristic
        if links:
            raw_exts = re.compile(
                r"\.(h5|h5ad|mtx|bam|fastq|fastq\.gz|tar\.gz|cel)$",
                re.IGNORECASE,
            )
            record["raw_data_available"] = any(
                raw_exts.search(lnk.get("url", "") or "") for lnk in links
            )

        # NLP-derived fields
        full_text = " ".join(filter(None, [record["title"], record["abstract"]]))
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        record["sub_compartments"] = _parse_sub_compartments(full_text)
        record["disease_groups"] = _detect_disease_groups(full_text)

        # Controlled access: EGA accessions are controlled
        record["controlled_access"] = accession.upper().startswith("EGA")

        return record
