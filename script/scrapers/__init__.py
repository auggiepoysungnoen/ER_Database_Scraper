"""
scrapers/__init__.py
--------------------
Public interface for the Hickey Lab endometrial receptivity database
scraper package.

All scraper classes and shared helper functions are re-exported here so
that pipeline code can use simple imports such as::

    from scrapers import GEOScraper, ArrayExpressScraper

Scraper classes
---------------
GEOScraper
    NCBI Gene Expression Omnibus via E-utilities.
ArrayExpressScraper
    EMBL-EBI ArrayExpress / BioStudies via BioStudies REST API.
CellxGeneScraper
    CZ CELLxGENE Census via the ``cellxgene_census`` package.
HCAScraper
    Human Cell Atlas DCP via the Azul REST API.
SingleCellPortalScraper
    Broad Institute Single Cell Portal via SCP REST API.
ZenodoScraper
    Zenodo dataset records via the Zenodo REST API.
FigshareScraper
    figshare dataset articles via the figshare REST API.

Helper functions
----------------
_detect_modality
    Infer assay modality from free text.
_parse_lh_timepoints
    Extract LH-relative timepoints and cycle phase labels from text.
_parse_sub_compartments
    Identify uterine sub-compartments mentioned in text.
"""

from .arrayexpress import ArrayExpressScraper
from .base import (
    BaseScraper,
    _detect_modality,
    _parse_lh_timepoints,
    _parse_sub_compartments,
)
from .cellxgene import CellxGeneScraper
from .figshare import FigshareScraper
from .geo import GEOScraper
from .hca import HCAScraper
from .singlecellportal import SingleCellPortalScraper
from .zenodo import ZenodoScraper

__all__ = [
    # Abstract base
    "BaseScraper",
    # Concrete scrapers
    "GEOScraper",
    "ArrayExpressScraper",
    "CellxGeneScraper",
    "HCAScraper",
    "SingleCellPortalScraper",
    "ZenodoScraper",
    "FigshareScraper",
    # Shared helpers
    "_detect_modality",
    "_parse_lh_timepoints",
    "_parse_sub_compartments",
]
