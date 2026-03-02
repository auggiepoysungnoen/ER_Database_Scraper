"""
zenodo.py
---------
Scraper for Zenodo using its public REST API.

Searches the Zenodo record index for genomics datasets relevant to
endometrial receptivity and normalises results into the pipeline's
standardised metadata schema.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .base import BaseScraper, _detect_modality, _parse_lh_timepoints, _parse_sub_compartments

# Endometrial / uterine relevance filter terms
_RELEVANCE_TERMS = re.compile(
    r"endometri|uterine|uterus|implantation|window of implantation|"
    r"receptivity|decidual|trophoblast|endometriosiss",
    re.IGNORECASE,
)

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

_RAW_DATA_EXTENSIONS = re.compile(
    r"\.(h5|h5ad|mtx|loom|rds|tar\.gz|bam|fastq|gz|csv|tsv)$",
    re.IGNORECASE,
)


def _detect_disease_groups(text: str) -> list[str]:
    found: list[str] = []
    for pattern, label in _DISEASE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)
    return found


class ZenodoScraper(BaseScraper):
    """
    Scraper for Zenodo dataset records via the Zenodo REST API.

    Parameters
    ----------
    api_key : str, optional
        Zenodo personal access token.  Increases rate limits and allows
        access to embargoed / restricted records owned by the token holder.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    Zenodo REST API documentation:
    https://developers.zenodo.org/
    """

    SOURCE_DB = "Zenodo"
    BASE_URL = "https://zenodo.org/api/"

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

    def _is_relevant(self, record: dict) -> bool:
        """
        Check whether a Zenodo record is relevant to endometrial research.

        Parameters
        ----------
        record : dict
            Raw Zenodo record dict.

        Returns
        -------
        bool
            ``True`` if any relevance term appears in title or description.
        """
        metadata = record.get("metadata", {}) or {}
        text = " ".join(
            filter(
                None,
                [
                    metadata.get("title", "") or "",
                    metadata.get("description", "") or "",
                    " ".join(
                        kw.get("tag", "") or ""
                        for kw in (metadata.get("keywords", []) or [])
                        if isinstance(kw, dict)
                    )
                    if isinstance(metadata.get("keywords", []), list)
                    else str(metadata.get("keywords", "") or ""),
                ],
            )
        )
        return bool(_RELEVANCE_TERMS.search(text))

    def _extract_doi(self, record: dict) -> Optional[str]:
        """
        Extract the DOI from a Zenodo record.

        Parameters
        ----------
        record : dict
            Raw Zenodo record dict.

        Returns
        -------
        str or None
            DOI string without URL prefix.
        """
        doi = record.get("doi", "") or record.get("conceptdoi", "") or ""
        if doi:
            return re.sub(r"https?://doi\.org/", "", doi)
        # Check related identifiers for publication DOI
        metadata = record.get("metadata", {}) or {}
        for rel in metadata.get("related_identifiers", []) or []:
            if rel.get("relation") == "isSupplementTo" and rel.get("scheme") == "doi":
                return rel.get("identifier") or None
        return None

    def _extract_pub_doi(self, record: dict) -> Optional[str]:
        """
        Find a related publication DOI in the record's related identifiers.

        Parameters
        ----------
        record : dict
            Raw Zenodo record dict.

        Returns
        -------
        str or None
            Related publication DOI, or *None*.
        """
        metadata = record.get("metadata", {}) or {}
        for rel in metadata.get("related_identifiers", []) or []:
            scheme = rel.get("scheme", "") or ""
            relation = rel.get("relation", "") or ""
            if scheme == "doi" and relation in (
                "isSupplementTo",
                "isCitedBy",
                "isReferencedBy",
            ):
                return rel.get("identifier") or None
        return None

    def _extract_authors(self, record: dict) -> Optional[str]:
        """
        Build a short author string from a Zenodo record.

        Parameters
        ----------
        record : dict
            Raw Zenodo record dict.

        Returns
        -------
        str or None
            ``"Last et al."`` style string.
        """
        metadata = record.get("metadata", {}) or {}
        creators = metadata.get("creators", []) or []
        if not creators:
            return None
        first = creators[0]
        name = first.get("name", "") or first.get("familyname", "") or ""
        suffix = " et al." if len(creators) > 1 else ""
        return f"{name}{suffix}" if name else None

    def _extract_files(self, record: dict) -> tuple[list[dict], Optional[float]]:
        """
        Extract file list and total size in GB from a Zenodo record.

        Parameters
        ----------
        record : dict
            Raw Zenodo record dict.

        Returns
        -------
        tuple[list[dict], float | None]
            ``(files, total_size_gb)`` — files is a list of
            ``{"name", "size", "url"}`` dicts; size is total GB or *None*.
        """
        files: list[dict] = []
        total_bytes = 0
        for f in record.get("files", []) or []:
            fname = f.get("filename", "") or f.get("key", "") or ""
            size = f.get("filesize") or f.get("size") or 0
            url = (
                f.get("links", {}).get("self", "")
                or f.get("links", {}).get("download", "")
                or ""
            )
            files.append({"name": fname, "size": size, "url": url})
            try:
                total_bytes += int(size)
            except (TypeError, ValueError):
                pass
        size_gb = round(total_bytes / 1e9, 3) if total_bytes > 0 else None
        return files, size_gb

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        max_results: int = 100,
        **kwargs: Any,
    ) -> list[dict]:
        """
        Search Zenodo for dataset records matching *query*.

        Results are post-filtered to keep only records whose title or
        description mention endometrial / uterine terminology.

        Parameters
        ----------
        query : str
            Zenodo search query string, e.g.
            ``"endometrium scRNA-seq single cell"``.
        max_results : int, optional
            Maximum number of relevant records to return.  Default 100.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` (record ID as string)
            and ``"source_db"``.
        """
        self._log(f"Searching Zenodo: query='{query}' max={max_results}")
        params: dict[str, Any] = {
            "q": query,
            "type": "dataset",
            "size": min(max_results * 3, 200),  # over-fetch before relevance filter
            "page": 1,
            "sort": "mostrecent",
        }
        results: list[dict] = []

        while len(results) < max_results:
            try:
                data = self._get(self.BASE_URL + "records", params=params)
            except Exception as exc:
                self._log(f"Zenodo search page {params['page']} failed: {exc}")
                break

            hits = data.get("hits", {}).get("hits", []) or []
            if not hits:
                break

            for record in hits:
                if self._is_relevant(record):
                    rid = str(record.get("id", "") or "")
                    if rid:
                        results.append(
                            {"accession": rid, "source_db": self.SOURCE_DB}
                        )

            total = data.get("hits", {}).get("total", 0) or 0
            fetched = params["page"] * params["size"]
            self._log(
                f"Zenodo page {params['page']}: {len(hits)} records, "
                f"{len(results)} relevant so far (total={total})"
            )

            if fetched >= total or len(hits) < params["size"]:
                break
            params["page"] += 1

        return results[:max_results]

    def fetch_metadata(self, record_id: str) -> dict:
        """
        Fetch and normalise full metadata for a Zenodo record.

        Parameters
        ----------
        record_id : str
            Zenodo record ID (numeric string).

        Returns
        -------
        dict
            Standardised metadata dict.
        """
        record_out = self._empty_record()
        record_out["accession"] = record_id

        try:
            record = self._get(self.BASE_URL + f"records/{record_id}")
        except Exception as exc:
            self._log(f"fetch_metadata failed for record {record_id}: {exc}")
            return record_out

        metadata = record.get("metadata", {}) or {}

        # Core fields
        record_out["title"] = metadata.get("title") or None
        record_out["abstract"] = metadata.get("description") or None
        record_out["doi"] = self._extract_doi(record)
        record_out["authors"] = self._extract_authors(record)

        # Publication year
        pub_date = metadata.get("publication_date", "") or ""
        year_match = re.search(r"\b(20\d{2}|19\d{2})\b", pub_date)
        if year_match:
            record_out["year"] = int(year_match.group(1))

        # Journal / publisher
        record_out["journal"] = metadata.get("journal", {}).get("title") if isinstance(
            metadata.get("journal"), dict
        ) else None

        if record_out["doi"]:
            record_out["peer_reviewed"] = "Yes"

        # Files
        files, size_gb = self._extract_files(record)
        record_out["file_size_gb"] = size_gb
        if files:
            record_out["download_url"] = files[0].get("url") or None
            record_out["raw_data_available"] = any(
                _RAW_DATA_EXTENSIONS.search(f.get("name", "") or "") for f in files
            )

        # Modality
        combined_text = " ".join(
            filter(None, [record_out["title"], record_out["abstract"]])
        )
        record_out["modality"] = _detect_modality(combined_text)

        # Access rights
        access_right = metadata.get("access_right", "open") or "open"
        record_out["controlled_access"] = access_right not in ("open", "embargoed")

        # NLP-derived fields
        full_text = " ".join(
            filter(None, [record_out["title"], record_out["abstract"]])
        )
        record_out["lh_timepoints"] = _parse_lh_timepoints(full_text)
        record_out["sub_compartments"] = _parse_sub_compartments(full_text)
        record_out["disease_groups"] = _detect_disease_groups(full_text)

        return record_out
