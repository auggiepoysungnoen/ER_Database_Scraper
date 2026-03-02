"""
cellxgene.py
------------
Scraper for the Chan Zuckerberg CELLxGENE Census using the
``cellxgene_census`` Python package.

Queries the SOMA-backed census for single-cell datasets filtered to
uterine / endometrial tissue, then normalises results into the pipeline's
standardised metadata schema.
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

_UTERINE_TISSUE_TERMS = [
    "uterus",
    "endometrium",
    "uterine",
    "endometrial",
    "myometrium",
    "cervix",
]


def _detect_disease_groups(text: str) -> list[str]:
    found: list[str] = []
    for pattern, label in _DISEASE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(label)
    return found


class CellxGeneScraper(BaseScraper):
    """
    Scraper for CZ CELLxGENE Census.

    Uses the ``cellxgene_census`` package to open the public Census SOMA
    store and query the ``obs`` (cell observation) table for uterine /
    endometrial datasets.

    Parameters
    ----------
    api_key : str, optional
        Not used by CELLxGENE Census; reserved for future use.
    delay : float, optional
        Seconds between requests.  Default ``0.34``.
    cache_dir : str or Path, optional
        Directory for caching raw JSON responses.

    Notes
    -----
    CELLxGENE Census documentation:
    https://chanzuckerberg.github.io/cellxgene-census/
    """

    SOURCE_DB = "CELLxGENE"
    CENSUS_VERSION = "stable"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _require_census():
        """
        Import and return the ``cellxgene_census`` module.

        Returns
        -------
        module
            The imported ``cellxgene_census`` module.

        Raises
        ------
        ImportError
            If the package is not installed, with installation instructions.
        """
        try:
            import cellxgene_census  # type: ignore[import]
            return cellxgene_census
        except ImportError as exc:
            raise ImportError(
                "The 'cellxgene_census' package is required for CellxGeneScraper.\n"
                "Install it with:\n"
                "    pip install cellxgene-census\n"
                "or:\n"
                "    conda install -c conda-forge cellxgene-census"
            ) from exc

    @staticmethod
    def _require_pandas():
        """Import and return pandas."""
        try:
            import pandas as pd  # type: ignore[import]
            return pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for CellxGeneScraper: pip install pandas"
            ) from exc

    def _build_tissue_filter(self) -> str:
        """
        Build a SOMA value filter string for uterine tissue terms.

        Returns
        -------
        str
            A ``|``-joined filter expression compatible with
            ``cellxgene_census`` ``value_filter`` parameter.
        """
        clauses = []
        for term in _UTERINE_TISSUE_TERMS:
            clauses.append(f'tissue_general == "{term}"')
            clauses.append(f'tissue == "{term}"')
        return " or ".join(clauses)

    def _get_dataset_catalog(self, census) -> dict[str, dict]:
        """
        Retrieve the Census dataset catalog as a dict keyed by dataset_id.

        Parameters
        ----------
        census : cellxgene_census.CensusSoma
            Open census context manager object.

        Returns
        -------
        dict[str, dict]
            Mapping of ``dataset_id`` → dataset metadata dict.
        """
        pd = self._require_pandas()
        try:
            datasets_df: Any = (
                census["census_info"]["datasets"]
                .read()
                .concat()
                .to_pandas()
            )
            return {
                row["dataset_id"]: row.to_dict()
                for _, row in datasets_df.iterrows()
            }
        except Exception as exc:
            self._log(f"Failed to read dataset catalog: {exc}")
            return {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        tissue_filter: str = "uterus",
        max_results: int = 200,
        **kwargs: Any,
    ) -> list[dict]:
        """
        Query CELLxGENE Census for uterine / endometrial datasets.

        Opens the public Census, queries ``obs`` for cells with uterine
        tissue annotations, and returns unique ``dataset_id`` values.

        Parameters
        ----------
        tissue_filter : str, optional
            Primary tissue term to filter on.  Default ``"uterus"``.
            The scraper also checks ``"endometrium"`` and related terms
            regardless of this parameter.
        max_results : int, optional
            Maximum number of dataset IDs to return.  Default 200.
        **kwargs
            Ignored (for interface compatibility).

        Returns
        -------
        list[dict]
            Each element contains ``"accession"`` (dataset_id) and
            ``"source_db"``.
        """
        cellxgene_census = self._require_census()
        self._log(
            f"Opening CELLxGENE Census (version={self.CENSUS_VERSION}) "
            f"tissue_filter='{tissue_filter}'"
        )

        results: list[dict] = []
        try:
            with cellxgene_census.open_soma(census_version=self.CENSUS_VERSION) as census:
                value_filter = self._build_tissue_filter()
                self._log(f"obs value_filter: {value_filter}")

                obs = (
                    census["census_data"]["homo_sapiens"]
                    .obs.read(
                        value_filter=value_filter,
                        column_names=["dataset_id", "tissue", "tissue_general"],
                    )
                    .concat()
                    .to_pandas()
                )

                unique_ids = obs["dataset_id"].dropna().unique().tolist()
                self._log(f"Found {len(unique_ids)} unique dataset_ids")

                for did in unique_ids[:max_results]:
                    results.append(
                        {"accession": did, "source_db": self.SOURCE_DB}
                    )
        except Exception as exc:
            self._log(f"Census search failed: {exc}")

        return results

    def fetch_metadata(self, dataset_id: str) -> dict:
        """
        Fetch metadata for a single CELLxGENE dataset.

        Opens the Census, queries the dataset catalog and ``obs`` table for
        the given ``dataset_id``, then normalises into the pipeline schema.

        Parameters
        ----------
        dataset_id : str
            CELLxGENE dataset UUID.

        Returns
        -------
        dict
            Standardised metadata dict.
        """
        cellxgene_census = self._require_census()
        pd = self._require_pandas()

        record = self._empty_record()
        record["accession"] = dataset_id

        try:
            with cellxgene_census.open_soma(census_version=self.CENSUS_VERSION) as census:
                # Dataset catalog
                catalog = self._get_dataset_catalog(census)
                ds_info = catalog.get(dataset_id, {})

                record["title"] = ds_info.get("dataset_title") or ds_info.get("title") or None
                record["doi"] = ds_info.get("dataset_doi") or None
                record["abstract"] = ds_info.get("dataset_description") or None

                # Cell-level obs slice for this dataset
                obs: Any = (
                    census["census_data"]["homo_sapiens"]
                    .obs.read(
                        value_filter=f'dataset_id == "{dataset_id}"',
                        column_names=[
                            "cell_type",
                            "assay",
                            "disease",
                            "organism_ontology_term_id",
                            "tissue",
                            "sex",
                        ],
                    )
                    .concat()
                    .to_pandas()
                )

                record["n_cells"] = len(obs)

                # Cell types
                if "cell_type" in obs.columns:
                    cell_types = obs["cell_type"].dropna().unique().tolist()
                    record["sub_compartments"] = cell_types[:50]  # cap for schema

                # Assay → modality + platform
                if "assay" in obs.columns:
                    assay_vals = obs["assay"].dropna().unique().tolist()
                    assay_str = ", ".join(assay_vals)
                    record["platform"] = assay_str or None
                    record["modality"] = _detect_modality(assay_str)

                # Disease groups
                if "disease" in obs.columns:
                    diseases = obs["disease"].dropna().unique().tolist()
                    record["disease_groups"] = diseases

                # Organism
                if "organism_ontology_term_id" in obs.columns:
                    org_vals = obs["organism_ontology_term_id"].dropna().unique().tolist()
                    if org_vals:
                        record["organism"] = org_vals[0]

        except Exception as exc:
            self._log(f"fetch_metadata failed for {dataset_id}: {exc}")
            return record

        # DOI present → assume peer-reviewed
        if record["doi"]:
            record["peer_reviewed"] = "Yes"

        # NLP-derived fields from title + abstract
        full_text = " ".join(filter(None, [record["title"], record["abstract"]]))
        record["lh_timepoints"] = _parse_lh_timepoints(full_text)
        if not record["sub_compartments"]:
            record["sub_compartments"] = _parse_sub_compartments(full_text)
        if not record["disease_groups"]:
            record["disease_groups"] = _detect_disease_groups(full_text)

        # CZ CELLxGENE is open access
        record["controlled_access"] = False
        record["raw_data_available"] = True

        return record
