"""
figshare.py
-----------
Scraper for figshare using its public REST API.

Searches figshare for dataset articles relevant to endometrial receptivity
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
    r"normal|healthy|control": "healthy",
}

_RAW_DATA_EXTENSIONS = re.compile(
    r"\.(h5|h5ad|mtx|loom|rds|tar\.gz|bam|fastq|gz|csv|tsv|xlsx)$",
    re.IGNORECASE,
)


def _detect_disease_groups(text: str) -> list[str]:
    found: list[str] = []
    for pattern, label in _DISEASE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)
    return found


class FigshareScraper(BaseScraper):
    """
    Scraper for figshare dataset articles via the figshare REST API.

    Parameters
    ----------
    api_key : str, optional
        figshare personal token.  Required for private / institutional
        articles; public articles are accessible without authentication.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    figshare API documentation:
    https://docs.figshare.com/
    """

    SOURCE_DB = "figshare"
    BASE_URL = "https://api.figshare.com/v2/"

    def __init__(
        self,
        api_key: Optional[str] = None,
        delay: float = 0.34,
        cache_dir=None,
    ) -> None:
        super().__init__(api_key=api_key, delay=delay, cache_dir=cache_dir)
        if api_key:
            self.session.headers["Authorization"] = f"token {api_key}"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_doi(self, article: dict) -> Optional[str]:
        """
        Extract the DOI from a figshare article record.

        Parameters
        ----------
        article : dict
            Raw figshare article JSON.

        Returns
        -------
        str or None
            DOI string without URL prefix, or *None*.
        """
        doi = article.get("doi", "") or ""
        if doi:
            return re.sub(r"https?://doi\.org/", "", doi)
        resource_doi = article.get("resource_doi", "") or ""
        if resource_doi:
            return re.sub(r"https?://doi\.org/", "", resource_doi)
        return None

    def _extract_authors(self, article: dict) -> Optional[str]:
        """
        Build a short author string from a figshare article record.

        Parameters
        ----------
        article : dict
            Raw figshare article JSON.

        Returns
        -------
        str or None
            ``"Last et al."`` style string, or *None*.
        """
        authors = article.get("authors", []) or []
        if not authors:
            return None
        first = authors[0]
        name = (
            first.get("full_name", "")
            or first.get("last_name", "")
            or first.get("name", "")
            or ""
        )
        suffix = " et al." if len(authors) > 1 else ""
        return f"{name}{suffix}" if name else None

    def _extract_files(self, article: dict) -> tuple[list[dict], Optional[float]]:
        """
        Extract file list and total size in GB from a figshare article.

        Parameters
        ----------
        article : dict
            Raw figshare article JSON (detail endpoint).

        Returns
        -------
        tuple[list[dict], float | None]
            ``(files, total_size_gb)`` where each file dict has
            ``{"name", "size", "download_url"}``; size is total GB or *None*.
        """
        files: list[dict] = []
        total_bytes = 0
        for f in article.get("files", []) or []:
            fname = f.get("name", "") or ""
            size = f.get("size") or 0
            url = f.get("download_url", "") or ""
            files.append({"name": fname, "size": size, "download_url": url})
            try:
                total_bytes += int(size)
            except (TypeError, ValueError):
                pass
        size_gb = round(total_bytes / 1e9, 3) if total_bytes > 0 else None
        return files, size_gb

    def _extract_tags(self, article: dict) -> list[str]:
        """
        Extract tag strings from a figshare article.

        Parameters
        ----------
        article : dict
            Raw figshare article JSON.

        Returns
        -------
        list[str]
            Tag strings.
        """
        tags = article.get("tags", []) or []
        result: list[str] = []
        for tag in tags:
            if isinstance(tag, str):
                result.append(tag)
            elif isinstance(tag, dict):
                val = tag.get("name", "") or tag.get("tag", "") or ""
                if val:
                    result.append(val)
        return result

    def _extract_categories(self, article: dict) -> list[str]:
        """
        Extract category names from a figshare article.

        Parameters
        ----------
        article : dict
            Raw figshare article JSON.

        Returns
        -------
        list[str]
            Category name strings.
        """
        cats = article.get("categories", []) or []
        return [
            c.get("title", "") or c.get("name", "")
            for c in cats
            if isinstance(c, dict)
        ]

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
        Search figshare for dataset articles matching *query*.

        Uses the ``/articles/search`` POST endpoint with
        ``item_type=3`` (dataset).

        Parameters
        ----------
        query : str
            Free-text search query.
        max_results : int, optional
            Maximum number of articles to return.  Default 100.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` (article ID as string)
            and ``"source_db"``.
        """
        self._log(f"Searching figshare: query='{query}' max={max_results}")
        results: list[dict] = []
        page = 1
        page_size = min(max_results, 100)

        while len(results) < max_results:
            payload: dict[str, Any] = {
                "search_for": query,
                "item_type": 3,  # dataset
                "page_size": page_size,
                "page": page,
            }
            try:
                articles = self._post(
                    self.BASE_URL + "articles/search", json=payload
                )
            except Exception as exc:
                self._log(f"figshare search page {page} failed: {exc}")
                break

            if not isinstance(articles, list) or not articles:
                break

            for article in articles:
                aid = article.get("id", "") or article.get("article_id", "")
                if aid:
                    results.append(
                        {"accession": str(aid), "source_db": self.SOURCE_DB}
                    )

            self._log(
                f"figshare page {page}: {len(articles)} articles "
                f"(collected={len(results)})"
            )

            if len(articles) < page_size:
                break
            page += 1

        return results[:max_results]

    def fetch_metadata(self, article_id: str) -> dict:
        """
        Fetch and normalise full metadata for a figshare article.

        Parameters
        ----------
        article_id : str
            figshare article ID (numeric string).

        Returns
        -------
        dict
            Standardised metadata dict.
        """
        record = self._empty_record()
        record["accession"] = article_id

        try:
            article = self._get(self.BASE_URL + f"articles/{article_id}")
        except Exception as exc:
            self._log(f"fetch_metadata failed for article {article_id}: {exc}")
            return record

        # Core fields
        record["title"] = article.get("title") or None
        record["abstract"] = article.get("description") or None
        record["doi"] = self._extract_doi(article)
        record["authors"] = self._extract_authors(article)

        if record["doi"]:
            record["peer_reviewed"] = "Yes"

        # Year from published_date or created_date
        for key in ("published_date", "created_date", "modified_date"):
            date_str = article.get(key, "") or ""
            year_match = re.search(r"\b(20\d{2}|19\d{2})\b", str(date_str))
            if year_match:
                record["year"] = int(year_match.group(1))
                break

        # Files
        files, size_gb = self._extract_files(article)
        record["file_size_gb"] = size_gb
        if files:
            record["download_url"] = files[0].get("download_url") or None
            record["raw_data_available"] = any(
                _RAW_DATA_EXTENSIONS.search(f.get("name", "") or "") for f in files
            )

        # Tags → enrich combined text for modality detection
        tags = self._extract_tags(article)
        categories = self._extract_categories(article)
        tag_text = " ".join(tags + categories)

        combined_text = " ".join(
            filter(None, [record["title"], record["abstract"], tag_text])
        )
        record["modality"] = _detect_modality(combined_text)

        # figshare is open access by default
        record["controlled_access"] = False

        # NLP-derived fields
        full_text = " ".join(
            filter(None, [record["title"], record["abstract"], tag_text])
        )
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        record["sub_compartments"] = _parse_sub_compartments(full_text)
        record["disease_groups"] = _detect_disease_groups(full_text)

        return record
