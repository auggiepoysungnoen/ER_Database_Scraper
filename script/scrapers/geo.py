"""
geo.py
------
Scraper for NCBI Gene Expression Omnibus (GEO) using NCBI E-utilities API.

Retrieves GEO DataSets (GDS / GSE accessions) relevant to endometrial
receptivity and the Window of Implantation, normalising results into the
pipeline's standardised metadata schema.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .base import BaseScraper, _detect_modality, _parse_lh_timepoints, _parse_sub_compartments

# ---------------------------------------------------------------------------
# Platform → modality keyword map (used in _parse_gse_metadata)
# ---------------------------------------------------------------------------
_PLATFORM_MODALITY_HINTS: list[tuple[str, str]] = [
    (r"visium|spatial", "Spatial Transcriptomics"),
    (r"codex|cycif|imc|geomx|mibi", "Spatial Proteomics"),
    (r"10x|chromium|dropseq|drop-seq|smart-?seq|indrops|snrna", "scRNA-seq"),
    (r"microarray|agilent|affymetrix|illumina beadarray", "bulkRNA-seq"),
    (r"rna-?seq|illumina|nextseq|hiseq|novaseq", "bulkRNA-seq"),
]

_RAW_DATA_EXTENSIONS = re.compile(
    r"\.(h5|h5ad|mtx|loom|rds|tar\.gz|bam|fastq|gz|cel|idat)$",
    re.IGNORECASE,
)

_DISEASE_KEYWORDS: dict[str, str] = {
    r"endometriosis": "endometriosis",
    r"recurrent\s+implantation\s+failure|RIF": "RIF",
    r"recurrent\s+pregnancy\s+loss|RPL|recurrent\s+miscarriage": "RPL",
    r"infertil": "infertility",
    r"polycystic|PCOS": "PCOS",
    r"leiomyoma|fibroid": "leiomyoma",
    r"adenomyosis": "adenomyosis",
    r"endometrial\s+cancer|endometrial\s+carcinoma|uterine\s+cancer": "endometrial cancer",
    r"healthy|normal\s+endometrium|control": "healthy",
}


class GEOScraper(BaseScraper):
    """
    Scraper for NCBI GEO using the E-utilities REST API.

    Parameters
    ----------
    api_key : str, optional
        NCBI API key.  Increases rate limit from 3 req/s to 10 req/s.
    delay : float, optional
        Seconds between requests.  Defaults to ``0.34`` (≈ 3/s); set to
        ``0.1`` when an *api_key* is provided.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    NCBI E-utilities documentation:
    https://www.ncbi.nlm.nih.gov/books/NBK25499/
    """

    SOURCE_DB = "GEO"
    BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

    def __init__(
        self,
        api_key: Optional[str] = None,
        delay: Optional[float] = None,
        cache_dir=None,
    ) -> None:
        if delay is None:
            delay = 0.1 if api_key else 0.34
        super().__init__(api_key=api_key, delay=delay, cache_dir=cache_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ncbi_params(self, extra: Optional[dict] = None) -> dict:
        """
        Build base NCBI query-param dict, appending API key when available.

        Parameters
        ----------
        extra : dict, optional
            Additional key/value pairs to merge in.

        Returns
        -------
        dict
            Parameter dict ready for ``_get``.
        """
        params: dict[str, Any] = {"retmode": "json"}
        if self.api_key:
            params["api_key"] = self.api_key
        if extra:
            params.update(extra)
        return params

    def _parse_platform_modality(self, platform_str: str) -> str:
        """
        Map a GEO platform string to a modality label.

        Parameters
        ----------
        platform_str : str
            Raw platform / GPL description from GEO.

        Returns
        -------
        str
            Modality label.
        """
        for pattern, label in _PLATFORM_MODALITY_HINTS:
            if re.search(pattern, platform_str, re.IGNORECASE):
                return label
        return _detect_modality(platform_str)

    def _parse_gse_metadata(self, raw: dict) -> dict:
        """
        Extract structured fields from a raw GEO esummary result document.

        Parameters
        ----------
        raw : dict
            Single entry from the ``result`` block of an esummary response.

        Returns
        -------
        dict
            Partial metadata dict (merged by ``fetch_metadata``).
        """
        platform = raw.get("GPL", "") or raw.get("gpl", "") or ""
        platform_title = raw.get("platformtaxid", "") or ""

        # n_samples
        n_samples: Optional[int] = None
        try:
            n_samples = int(raw.get("n_samples", 0)) or None
        except (TypeError, ValueError):
            pass

        # Supplemental files → raw data flag
        suppfiles: list[str] = []
        for key in ("suppfile", "ftplink"):
            val = raw.get(key, "")
            if isinstance(val, str) and val:
                suppfiles.append(val)
            elif isinstance(val, list):
                suppfiles.extend(val)
        has_raw = any(
            _RAW_DATA_EXTENSIONS.search(f) for f in suppfiles if f
        )

        # FTP download URL
        ftp_link: Optional[str] = raw.get("ftplink") or None

        # PubMed IDs
        pubmed_ids: list[str] = []
        gse_refs = raw.get("pubmedids", []) or []
        if isinstance(gse_refs, str):
            gse_refs = [gse_refs]
        pubmed_ids = [str(p) for p in gse_refs if p]

        # Platform technology string
        platform_tech = platform or platform_title or None

        return {
            "n_samples": n_samples,
            "platform": platform_tech,
            "raw_data_available": has_raw if suppfiles else None,
            "download_url": ftp_link,
            "pubmed_ids": pubmed_ids,
            "supplemental_files": suppfiles,
        }

    @staticmethod
    def _detect_disease_groups(text: str) -> list[str]:
        """
        Identify disease / condition groups mentioned in free text.

        Parameters
        ----------
        text : str
            Abstract, title, or description.

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
        Search GEO DataSets using NCBI esearch and return GSE accessions.

        Parameters
        ----------
        query : str
            E-utilities search query string, e.g.
            ``"endometrium[MeSH] AND scRNA-seq"``.
        max_results : int, optional
            Maximum number of records to retrieve.  Default 200.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            List of dicts, each with ``"accession"`` and ``"source_db"``.
        """
        self._log(f"Searching GEO: query='{query}' max={max_results}")
        params = self._ncbi_params(
            {
                "db": "gds",
                "term": query,
                "retmax": max_results,
                "usehistory": "y",
            }
        )
        try:
            data = self._get(self.BASE_URL + "esearch.fcgi", params=params)
        except Exception as exc:
            self._log(f"esearch failed: {exc}")
            return []

        uid_list: list[str] = data.get("esearchresult", {}).get("idlist", [])
        self._log(f"esearch returned {len(uid_list)} UIDs")

        # Convert GDS UIDs to GSE accessions via esummary
        results: list[dict] = []
        for uid in uid_list:
            accession = self._uid_to_accession(uid)
            if accession:
                results.append(
                    {"accession": accession, "source_db": self.SOURCE_DB}
                )
        return results

    def _uid_to_accession(self, uid: str) -> Optional[str]:
        """
        Convert a GDS numeric UID to a GSE accession string.

        Parameters
        ----------
        uid : str
            Numeric GDS UID from esearch.

        Returns
        -------
        str or None
            GSE accession (e.g. ``"GSE12345"``), or *None* on failure.
        """
        params = self._ncbi_params({"db": "gds", "id": uid})
        try:
            data = self._get(self.BASE_URL + "esummary.fcgi", params=params)
            result_block = data.get("result", {})
            doc = result_block.get(uid, {})
            accession: str = doc.get("accession", "") or ""
            if accession.upper().startswith("GSE"):
                return accession.upper()
            # Sometimes the accession field holds the GSE; fall back to entrytype
            entry_type = doc.get("entrytype", "")
            if entry_type == "GSE":
                acc2 = doc.get("gse", "") or doc.get("accession", "")
                return acc2.upper() if acc2 else None
        except Exception as exc:
            self._log(f"UID→accession lookup failed for {uid}: {exc}")
        return None

    def fetch_metadata(self, accession: str) -> dict:
        """
        Fetch and normalise full metadata for a GEO series accession.

        Parameters
        ----------
        accession : str
            GSE accession, e.g. ``"GSE12345"``.

        Returns
        -------
        dict
            Standardised metadata dict.  Falls back to populated defaults
            on any API failure.
        """
        record = self._empty_record()
        record["accession"] = accession.upper()

        # esearch to get UID for this GSE
        params = self._ncbi_params(
            {"db": "gds", "term": f"{accession}[Accession]", "retmax": 1}
        )
        try:
            search_data = self._get(
                self.BASE_URL + "esearch.fcgi", params=params
            )
            uids = (
                search_data.get("esearchresult", {}).get("idlist", [])
            )
            if not uids:
                self._log(f"No UID found for {accession}")
                return record
            uid = uids[0]
        except Exception as exc:
            self._log(f"esearch for {accession} failed: {exc}")
            return record

        # esummary to get metadata
        sum_params = self._ncbi_params({"db": "gds", "id": uid})
        try:
            sum_data = self._get(
                self.BASE_URL + "esummary.fcgi", params=sum_params
            )
            result_block = sum_data.get("result", {})
            doc = result_block.get(uid, {})
        except Exception as exc:
            self._log(f"esummary for {accession} failed: {exc}")
            return record

        # Core fields
        record["title"] = doc.get("title") or None
        record["abstract"] = doc.get("summary") or None
        record["organism"] = doc.get("taxon") or "Homo sapiens"

        # Year from update/create date
        for date_key in ("pdat", "crdt", "entrezdate"):
            date_str = doc.get(date_key, "") or ""
            year_match = re.search(r"\b(20\d{2}|19\d{2})\b", date_str)
            if year_match:
                record["year"] = int(year_match.group(1))
                break

        # Parse platform / supplemental / sample data
        parsed = self._parse_gse_metadata(doc)
        record["n_samples"] = parsed["n_samples"]
        record["platform"] = parsed["platform"]
        record["raw_data_available"] = parsed["raw_data_available"]
        record["download_url"] = parsed["download_url"]

        # PubMed ID → fetch abstract
        pmids = parsed["pubmed_ids"]
        if pmids:
            record["pubmed_id"] = pmids[0]
            abstract_data = self.get_pubmed_abstract(pmids[0])
            if abstract_data:
                record["abstract"] = abstract_data.get("abstract") or record["abstract"]
                record["authors"] = abstract_data.get("authors") or None
                record["journal"] = abstract_data.get("journal") or None
                record["year"] = abstract_data.get("year") or record["year"]
                record["doi"] = abstract_data.get("doi") or None
                record["peer_reviewed"] = "Yes"

        # Modality detection
        combined_text = " ".join(
            filter(
                None,
                [
                    record["title"],
                    record["abstract"],
                    record["platform"],
                ],
            )
        )
        record["modality"] = _detect_modality(combined_text)

        # NLP-derived fields
        full_text = " ".join(filter(None, [record["title"], record["abstract"]]))
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        record["sub_compartments"] = _parse_sub_compartments(full_text)
        record["disease_groups"] = self._detect_disease_groups(full_text)

        return record

    def get_pubmed_abstract(self, pmid: str) -> Optional[dict]:
        """
        Fetch title, abstract, authors, journal, and year from PubMed.

        Parameters
        ----------
        pmid : str
            PubMed identifier.

        Returns
        -------
        dict or None
            Keys: ``abstract``, ``authors``, ``journal``, ``year``, ``doi``.
            Returns *None* on any failure.
        """
        params = self._ncbi_params(
            {"db": "pubmed", "id": pmid, "rettype": "abstract", "retmode": "xml"}
        )
        # For XML we need raw text, not JSON
        try:
            self._rate_limit()
            response = self.session.get(
                self.BASE_URL + "efetch.fcgi",
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            xml = response.text
        except Exception as exc:
            self._log(f"PubMed efetch for PMID {pmid} failed: {exc}")
            return None

        result: dict[str, Any] = {}

        # Abstract
        abstract_match = re.search(
            r"<AbstractText[^>]*>(.*?)</AbstractText>", xml, re.DOTALL
        )
        result["abstract"] = abstract_match.group(1).strip() if abstract_match else None

        # Authors: LastName + ForeName for first author, "et al." if >1
        author_matches = re.findall(
            r"<LastName>(.*?)</LastName>.*?<ForeName>(.*?)</ForeName>",
            xml,
            re.DOTALL,
        )
        if author_matches:
            last, first = author_matches[0]
            suffix = " et al." if len(author_matches) > 1 else ""
            result["authors"] = f"{last}, {first}{suffix}"
        else:
            result["authors"] = None

        # Journal
        journal_match = re.search(r"<Title>(.*?)</Title>", xml)
        result["journal"] = journal_match.group(1).strip() if journal_match else None

        # Year
        year_match = re.search(r"<PubDate>.*?<Year>(\d{4})</Year>", xml, re.DOTALL)
        result["year"] = int(year_match.group(1)) if year_match else None

        # DOI
        doi_match = re.search(
            r'<ArticleId IdType="doi">(.*?)</ArticleId>', xml
        )
        result["doi"] = doi_match.group(1).strip() if doi_match else None

        return result
