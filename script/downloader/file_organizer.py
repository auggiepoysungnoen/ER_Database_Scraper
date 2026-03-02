"""
downloader/file_organizer.py
============================
Post-download file organizer for the Hickey Lab Endometrial Receptivity pipeline.

After ``DownloadManager`` places raw files under ``{raw_dir}/{accession}/``,
``FileOrganizer`` moves and renames them into a canonical layout:

    {raw_dir}/{modality}/{accession}/
        {accession}_counts.h5          (or .h5ad / .mtx.gz)
        {accession}_metadata.csv
        {accession}_README.txt

A README.txt is generated for every accession containing provenance metadata
(accession, title, authors, DOI, modality, platform, n_cells,
confidence_tier, date_downloaded).
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File-type detection patterns
# ---------------------------------------------------------------------------
_COUNTS_PATTERNS: list[str] = [
    r"\.h5$", r"\.h5ad$", r"\.loom$",
    r"matrix\.mtx", r"\.mtx\.gz$",
    r"counts", r"expression", r"rawdata",
]
_METADATA_PATTERNS: list[str] = [
    r"barcode", r"cell_?meta", r"metadata", r"annotation",
    r"obs\.csv", r"cells\.csv", r"barcodes\.tsv",
]
_README_PATTERNS: list[str] = [
    r"readme", r"\.txt$",
]


class FileOrganizer:
    """
    Organise downloaded raw files into a canonical modality/accession layout.

    Parameters
    ----------
    raw_dir : str
        Root directory containing ``{accession}/`` subfolders produced by
        ``DownloadManager``.
    registry_path : str
        Path to ``datasets_registry.json`` (used for metadata lookup).

    Attributes
    ----------
    raw_dir : Path
        Resolved root raw directory.
    registry : dict[str, dict]
        Accession-keyed lookup built from the registry JSON.
    """

    def __init__(self, raw_dir: str, registry_path: str) -> None:
        self.raw_dir = Path(raw_dir)
        self._registry_path = Path(registry_path)
        self.registry: dict[str, dict] = self._load_registry()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def organize(self) -> None:
        """
        Iterate over every ``{accession}/`` subfolder and reorganise files.

        For each accession folder directly under ``self.raw_dir``:

        1. Look up the dataset metadata from the registry.
        2. Identify the counts, metadata, and supplemental files.
        3. Move the folder to ``{raw_dir}/{modality}/{accession}/``.
        4. Rename files to the canonical ``{accession}_*`` scheme.
        5. Write a ``{accession}_README.txt``.

        Accession folders that are already nested under a modality subdirectory
        are skipped to avoid double-processing.
        """
        if not self.raw_dir.exists():
            logger.warning("[FileOrganizer] raw_dir does not exist: %s", self.raw_dir)
            return

        # Only process direct children that are directories (not already nested)
        for folder in sorted(self.raw_dir.iterdir()):
            if not folder.is_dir():
                continue
            # Skip folders that look like modality groupings (contain sub-dirs)
            accession = folder.name
            if any(child.is_dir() for child in folder.iterdir()
                   if not child.name.startswith(".")):
                # Could be modality folder — skip
                logger.info(
                    "[FileOrganizer] Skipping apparent modality directory: %s", accession
                )
                continue

            dataset = self.registry.get(accession, {})
            modality = (dataset.get("modality") or "Unknown").replace(" ", "_")

            dest_base = self.raw_dir / modality / accession
            dest_base.mkdir(parents=True, exist_ok=True)

            files = list(folder.iterdir())
            renamed: list[str] = []

            for src in files:
                if src.is_dir():
                    continue
                file_type = self._detect_file_type(src.name)
                dest = self._canonical_dest(src, accession, file_type, dest_base)
                if dest and dest != src:
                    try:
                        shutil.move(str(src), str(dest))
                        renamed.append(dest.name)
                        logger.info(
                            "[FileOrganizer] %s → %s", src.name, dest.relative_to(self.raw_dir)
                        )
                    except OSError as exc:
                        logger.warning("[FileOrganizer] Move failed %s: %s", src, exc)
                else:
                    # Move unchanged filename to dest dir
                    dest_file = dest_base / src.name
                    if dest_file != src:
                        try:
                            shutil.move(str(src), str(dest_file))
                        except OSError as exc:
                            logger.warning("[FileOrganizer] Move failed %s: %s", src, exc)

            # Write README
            self.write_readme(accession, dataset, str(dest_base))

            # Remove original folder if now empty
            try:
                if folder.exists() and not any(folder.iterdir()):
                    folder.rmdir()
            except OSError:
                pass

            logger.info(
                "[FileOrganizer] Organised %s → %s/%s (%d files)",
                accession, modality, accession, len(renamed),
            )

    def write_readme(self, accession: str, dataset: dict, folder: str) -> None:
        """
        Generate a ``{accession}_README.txt`` inside *folder*.

        Parameters
        ----------
        accession : str
            Dataset accession identifier.
        dataset : dict
            Registry metadata dict for this accession.
        folder : str
            Destination directory path (string or Path-coercible).
        """
        dest = Path(folder) / f"{accession}_README.txt"

        authors = dataset.get("authors") or "N/A"
        if isinstance(authors, list):
            authors = "; ".join(str(a) for a in authors)

        lh_timepoints = dataset.get("lh_timepoints") or []
        if isinstance(lh_timepoints, list):
            lh_timepoints = ", ".join(lh_timepoints) or "N/A"

        sub_compartments = dataset.get("sub_compartments") or []
        if isinstance(sub_compartments, list):
            sub_compartments = ", ".join(sub_compartments) or "N/A"

        lines = [
            "=" * 60,
            f"Hickey Lab — Endometrial Receptivity Database",
            f"Dataset README",
            "=" * 60,
            "",
            f"Accession        : {accession}",
            f"Title            : {dataset.get('title') or 'N/A'}",
            f"Authors          : {authors}",
            f"DOI              : {dataset.get('doi') or 'N/A'}",
            f"PubMed ID        : {dataset.get('pubmed_id') or 'N/A'}",
            f"Journal          : {dataset.get('journal') or 'N/A'}",
            f"Year             : {dataset.get('year') or 'N/A'}",
            "",
            f"Modality         : {dataset.get('modality') or 'N/A'}",
            f"Platform         : {dataset.get('platform') or 'N/A'}",
            f"Organism         : {dataset.get('organism') or 'Homo sapiens'}",
            "",
            f"N Patients       : {dataset.get('n_patients') or 'N/A'}",
            f"N Samples        : {dataset.get('n_samples') or 'N/A'}",
            f"N Cells          : {dataset.get('n_cells') or 'N/A'}",
            "",
            f"LH Timepoints    : {lh_timepoints}",
            f"Sub-compartments : {sub_compartments}",
            "",
            f"Confidence Tier  : {dataset.get('confidence_tier') or 'N/A'}",
            f"Confidence Score : {dataset.get('final_CS') or dataset.get('confidence_score') or 'N/A'}",
            f"Source DB        : {dataset.get('source_db') or 'N/A'}",
            "",
            f"Download URL     : {dataset.get('download_url') or 'N/A'}",
            f"Controlled Access: {dataset.get('controlled_access', False)}",
            "",
            f"Date Downloaded  : {_now_iso()}",
            f"Date Scraped     : {dataset.get('date_scraped') or 'N/A'}",
            "",
            "=" * 60,
            "This file was auto-generated by the Hickey Lab pipeline.",
            "Duke University | Hickey Lab | Endometrial Receptivity Aim 01",
            "=" * 60,
        ]

        try:
            with dest.open("w", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
            logger.info("[FileOrganizer] README written: %s", dest)
        except OSError as exc:
            logger.warning("[FileOrganizer] README write failed for %s: %s", accession, exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _detect_file_type(self, filename: str) -> str:
        """
        Classify a filename into a semantic file type.

        Parameters
        ----------
        filename : str
            Bare filename (no directory component).

        Returns
        -------
        str
            One of ``"counts"``, ``"metadata"``, ``"readme"``, or
            ``"supplemental"``.
        """
        lower = filename.lower()
        for pat in _COUNTS_PATTERNS:
            if re.search(pat, lower):
                return "counts"
        for pat in _METADATA_PATTERNS:
            if re.search(pat, lower):
                return "metadata"
        for pat in _README_PATTERNS:
            if re.search(pat, lower):
                return "readme"
        return "supplemental"

    def _canonical_dest(
        self,
        src: Path,
        accession: str,
        file_type: str,
        dest_dir: Path,
    ) -> Optional[Path]:
        """
        Compute the canonical destination path for a file.

        The renaming rules are:

        * ``counts`` → ``{accession}_counts{suffix}`` preserving the original
          extension chain (e.g. ``.h5ad``, ``.mtx.gz``).
        * ``metadata`` → ``{accession}_metadata.csv``.
        * ``readme`` → ``{accession}_README.txt``.
        * ``supplemental`` → unchanged filename, moved to dest_dir.

        Parameters
        ----------
        src : Path
            Source file path.
        accession : str
            Dataset accession identifier.
        file_type : str
            File type string from :meth:`_detect_file_type`.
        dest_dir : Path
            Destination directory.

        Returns
        -------
        Path or None
            Destination path, or *None* to leave the file in place.
        """
        if file_type == "counts":
            suffix = _multi_suffix(src)
            return dest_dir / f"{accession}_counts{suffix}"
        if file_type == "metadata":
            return dest_dir / f"{accession}_metadata.csv"
        if file_type == "readme":
            return dest_dir / f"{accession}_README.txt"
        # supplemental: keep name, just move to dest_dir
        return dest_dir / src.name

    def _load_registry(self) -> dict[str, dict]:
        """
        Load and index the registry JSON by accession.

        Returns
        -------
        dict[str, dict]
            Mapping of accession string to dataset dict.  Empty dict if the
            registry does not exist or cannot be parsed.
        """
        if not self._registry_path.exists():
            logger.warning(
                "[FileOrganizer] Registry not found: %s", self._registry_path
            )
            return {}
        try:
            with self._registry_path.open("r", encoding="utf-8") as fh:
                records: list[dict] = json.load(fh)
            return {r.get("accession", ""): r for r in records if r.get("accession")}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("[FileOrganizer] Registry load failed: %s", exc)
            return {}


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _multi_suffix(path: Path) -> str:
    """
    Return the full suffix chain of a Path (e.g. ``.mtx.gz``).

    Parameters
    ----------
    path : Path
        File path.

    Returns
    -------
    str
        Concatenated suffixes string, e.g. ``".mtx.gz"`` or ``".h5ad"``.
    """
    return "".join(path.suffixes) if path.suffixes else path.suffix


def _now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
