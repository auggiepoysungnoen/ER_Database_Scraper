"""
hca.py
------
Scraper for the Human Cell Atlas Data Coordination Platform (HCA DCP)
using the Azul REST API.

Retrieves HCA projects filtered to uterine / endometrial organ and
normalises results into the pipeline's standardised metadata schema.
"""

from __future__ import annotations

import json as _json
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


class HCAScraper(BaseScraper):
    """
    Scraper for the Human Cell Atlas DCP via the Azul REST API.

    Parameters
    ----------
    api_key : str, optional
        Not required by the HCA Azul API; reserved for future use.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    HCA Azul API documentation:
    https://service.azul.data.humancellatlas.org/
    """

    SOURCE_DB = "HCA"
    BASE_URL = "https://service.azul.data.humancellatlas.org/"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_first(self, lst: Any, default: Any = None) -> Any:
        """
        Return the first element of *lst*, or *default* if empty / None.

        Parameters
        ----------
        lst : Any
            A list or None.
        default : Any, optional
            Value returned when *lst* is falsy.

        Returns
        -------
        Any
            First element or default.
        """
        if isinstance(lst, list) and lst:
            return lst[0]
        return default

    def _flatten_hits(self, data: dict) -> list[dict]:
        """
        Extract the ``hits`` list from an Azul search response.

        Parameters
        ----------
        data : dict
            Raw JSON response from the Azul ``/index/projects`` endpoint.

        Returns
        -------
        list[dict]
            List of project hit dicts.
        """
        return data.get("hits", []) or []

    def _project_id_from_hit(self, hit: dict) -> Optional[str]:
        """
        Extract the project UUID from an Azul search hit.

        Parameters
        ----------
        hit : dict
            Single hit from Azul search response.

        Returns
        -------
        str or None
            Project UUID string.
        """
        projects = hit.get("projects", []) or []
        if projects:
            return projects[0].get("projectId") or None
        return None

    def _parse_library_approach(self, hit: dict) -> Optional[str]:
        """
        Extract library construction approach from a project hit.

        Parameters
        ----------
        hit : dict
            Single hit or project detail dict.

        Returns
        -------
        str or None
            Comma-separated library construction approach string.
        """
        protocols = hit.get("protocols", []) or []
        approaches: list[str] = []
        for proto in protocols:
            lca = proto.get("libraryConstructionApproach", []) or []
            approaches.extend(lca)
        return ", ".join(approaches) if approaches else None

    def _parse_cell_count(self, hit: dict) -> Optional[int]:
        """
        Extract total estimated cell count from a project hit.

        Parameters
        ----------
        hit : dict
            Single hit or project detail dict.

        Returns
        -------
        int or None
            Total cell count.
        """
        cell_suspensions = hit.get("cellSuspensions", []) or []
        total = 0
        for cs in cell_suspensions:
            count = cs.get("totalCells") or cs.get("estimatedCellCount") or 0
            try:
                total += int(count)
            except (TypeError, ValueError):
                pass
        return total if total > 0 else None

    def _parse_publications(self, project_detail: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract publication DOI, title, and authors from a project detail dict.

        Parameters
        ----------
        project_detail : dict
            Project detail block (``projects[0]`` from Azul response).

        Returns
        -------
        tuple[str | None, str | None, str | None]
            ``(doi, pub_title, authors)`` — any may be *None*.
        """
        publications = project_detail.get("publications", []) or []
        if not publications:
            return None, None, None
        pub = publications[0]
        doi: Optional[str] = pub.get("doi") or pub.get("publicationDoi") or None
        pub_title: Optional[str] = pub.get("publicationTitle") or pub.get("title") or None
        authors_list = pub.get("authors", []) or []
        if authors_list:
            first = authors_list[0] if isinstance(authors_list[0], str) else str(authors_list[0])
            suffix = " et al." if len(authors_list) > 1 else ""
            authors = f"{first}{suffix}"
        else:
            authors = None
        return doi, pub_title, authors

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        organ: str = "uterus",
        max_results: int = 200,
        **kwargs: Any,
    ) -> list[dict]:
        """
        Search HCA DCP for projects filtered by organ.

        Parameters
        ----------
        organ : str, optional
            Organ filter value.  Default ``"uterus"``.  Additional terms
            (``"endometrium"``, ``"uterine cervix"``) are also searched in
            separate requests and deduplicated.
        max_results : int, optional
            Maximum number of project IDs to return.  Default 200.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` (project UUID) and
            ``"source_db"``.
        """
        organ_terms = list({organ, "uterus", "endometrium", "uterine cervix"})
        seen: set[str] = set()
        results: list[dict] = []

        for term in organ_terms:
            if len(results) >= max_results:
                break
            filters = _json.dumps({"organ": {"is": [term]}})
            params: dict[str, Any] = {
                "filters": filters,
                "size": min(max_results - len(results), 100),
            }
            self._log(f"HCA search organ='{term}'")
            try:
                data = self._get(self.BASE_URL + "index/projects", params=params)
            except Exception as exc:
                self._log(f"HCA search failed for organ='{term}': {exc}")
                continue

            for hit in self._flatten_hits(data):
                pid = self._project_id_from_hit(hit)
                if pid and pid not in seen:
                    seen.add(pid)
                    results.append({"accession": pid, "source_db": self.SOURCE_DB})

            # Pagination
            pagination = data.get("pagination", {}) or {}
            next_url = pagination.get("next") or None
            while next_url and len(results) < max_results:
                try:
                    page_data = self._get(next_url)
                except Exception as exc:
                    self._log(f"HCA pagination failed: {exc}")
                    break
                for hit in self._flatten_hits(page_data):
                    pid = self._project_id_from_hit(hit)
                    if pid and pid not in seen:
                        seen.add(pid)
                        results.append({"accession": pid, "source_db": self.SOURCE_DB})
                pagination = page_data.get("pagination", {}) or {}
                next_url = pagination.get("next") or None

        self._log(f"HCA search returned {len(results)} unique projects")
        return results[:max_results]

    def fetch_metadata(self, project_id: str) -> dict:
        """
        Fetch and normalise full metadata for an HCA project.

        Parameters
        ----------
        project_id : str
            HCA project UUID.

        Returns
        -------
        dict
            Standardised metadata dict.
        """
        record = self._empty_record()
        record["accession"] = project_id

        try:
            data = self._get(self.BASE_URL + f"index/projects/{project_id}")
        except Exception as exc:
            self._log(f"fetch_metadata failed for {project_id}: {exc}")
            return record

        hits = self._flatten_hits(data)
        if not hits:
            # Direct project endpoint may return a single project object
            hit = data
        else:
            hit = hits[0]

        projects = hit.get("projects", []) or []
        project_detail = projects[0] if projects else hit

        # Core fields
        record["title"] = (
            project_detail.get("projectTitle")
            or project_detail.get("projectShortname")
            or None
        )
        record["abstract"] = (
            project_detail.get("projectDescription")
            or project_detail.get("laboratory", [""])[0]
            if isinstance(project_detail.get("laboratory"), list)
            else project_detail.get("projectDescription")
            or None
        )

        # Publication
        doi, _pub_title, authors = self._parse_publications(project_detail)
        record["doi"] = doi
        record["authors"] = authors
        if doi:
            record["peer_reviewed"] = "Yes"

        # Cell count
        record["n_cells"] = self._parse_cell_count(hit)

        # Sample count
        samples = hit.get("samples", []) or []
        if samples:
            try:
                record["n_samples"] = sum(
                    int(s.get("totalCells") or 0) for s in samples
                ) or len(samples)
            except (TypeError, ValueError):
                record["n_samples"] = len(samples)

        # Library construction approach → modality + platform
        lib_approach = self._parse_library_approach(hit)
        record["platform"] = lib_approach or None
        combined_text = " ".join(
            filter(None, [record["title"], record["abstract"], lib_approach])
        )
        record["modality"] = _detect_modality(combined_text)

        # Organ parts
        specimens = hit.get("specimens", []) or []
        organ_parts: list[str] = []
        for sp in specimens:
            parts = sp.get("organPart", []) or []
            organ_parts.extend(parts)
        if organ_parts:
            record["sub_compartments"] = list(set(organ_parts))

        # Disease status
        donor_orgs = hit.get("donorOrganisms", []) or []
        disease_statuses: list[str] = []
        for donor in donor_orgs:
            diseases = donor.get("disease", []) or []
            disease_statuses.extend(diseases)
        if disease_statuses:
            record["disease_groups"] = list(set(disease_statuses))

        # Organism
        for donor in donor_orgs:
            species = donor.get("genusSpecies", []) or []
            if species:
                record["organism"] = species[0]
                break

        # Year from submission date
        proj_dates = project_detail.get("dates", {}) or {}
        submission_date = proj_dates.get("submissionDate", "") or ""
        year_match = re.search(r"\b(20\d{2})\b", str(submission_date))
        if year_match:
            record["year"] = int(year_match.group(1))

        # NLP-derived fields
        full_text = " ".join(filter(None, [record["title"], record["abstract"]]))
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        if not record["sub_compartments"]:
            record["sub_compartments"] = _parse_sub_compartments(full_text)
        if not record["disease_groups"]:
            record["disease_groups"] = _detect_disease_groups(full_text)

        # HCA is open access
        record["controlled_access"] = False
        record["raw_data_available"] = True

        return record
