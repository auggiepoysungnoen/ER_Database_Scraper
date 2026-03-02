"""
downloader/download_manager.py
==============================
Parallel dataset downloader for the Hickey Lab Endometrial Receptivity pipeline.

Downloads GOLD and SILVER tier datasets from ``datasets_registry.json``,
verifies MD5 checksums, tracks progress with tqdm, retries on failure, and
can generate a standalone bash manifest for manual or HPC execution.

Controlled-access datasets (dbGaP / EGA) are never downloaded automatically;
a warning is printed and the accession is written to
``skipped_controlled_access.txt`` in the output directory.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Tier ordering: lower index = higher quality
_TIER_RANK: dict[str, int] = {"GOLD": 0, "SILVER": 1, "BRONZE": 2, "LOW": 3}

# Chunk size for streaming downloads (bytes)
_CHUNK_SIZE: int = 1024 * 1024  # 1 MB


class DownloadManager:
    """
    Parallel dataset downloader with resume support, MD5 verification, and
    bash manifest generation.

    Parameters
    ----------
    registry_path : str
        Absolute or relative path to ``datasets_registry.json``.
    output_dir : str
        Root directory where raw dataset folders will be created.
        Structure: ``{output_dir}/{accession}/``.
    workers : int, optional
        Number of parallel download threads.  Default ``4``.
    min_tier : str, optional
        Minimum confidence tier to download.  ``"SILVER"`` downloads GOLD and
        SILVER; ``"BRONZE"`` additionally downloads BRONZE.  Default
        ``"SILVER"``.
    log_path : str or None, optional
        Path to a log file.  If *None*, only console logging is performed.

    Attributes
    ----------
    registry_path : Path
        Resolved path to the registry JSON.
    output_dir : Path
        Resolved root output directory.
    workers : int
        Thread pool size.
    min_tier : str
        Minimum tier string (upper-cased).
    log_path : Path or None
        Resolved log file path, or *None*.
    """

    def __init__(
        self,
        registry_path: str,
        output_dir: str,
        workers: int = 4,
        min_tier: str = "SILVER",
        log_path: Optional[str] = None,
    ) -> None:
        self.registry_path = Path(registry_path)
        self.output_dir = Path(output_dir)
        self.workers = workers
        self.min_tier = min_tier.upper()
        self.log_path: Optional[Path] = Path(log_path) if log_path else None

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Configure file handler if a log path was requested
        if self.log_path:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(self.log_path, encoding="utf-8")
            fh.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            logger.addHandler(fh)

        logger.setLevel(logging.INFO)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_queue(self) -> list[dict]:
        """
        Read the registry and return datasets eligible for download.

        Eligibility criteria:

        * Confidence tier is at or above ``self.min_tier`` (GOLD or SILVER by
          default).
        * ``controlled_access`` is not ``True``.
        * The accession folder does not already exist in ``output_dir``
          (skip already-downloaded datasets).

        Returns
        -------
        list[dict]
            Filtered list of dataset dicts from the registry.
        """
        if not self.registry_path.exists():
            self._log(f"Registry not found: {self.registry_path}")
            return []

        with self.registry_path.open("r", encoding="utf-8") as fh:
            registry: list[dict] = json.load(fh)

        min_rank = _TIER_RANK.get(self.min_tier, 1)
        queue: list[dict] = []
        skipped_controlled: list[str] = []

        for dataset in registry:
            accession: str = dataset.get("accession") or ""
            tier: str = (dataset.get("confidence_tier") or "LOW").upper()
            controlled: bool = bool(dataset.get("controlled_access", False))

            if controlled:
                url = dataset.get("download_url") or dataset.get("url") or "N/A"
                print(
                    f"⚠  {accession}: Controlled access (dbGaP/EGA). "
                    f"Submit access request at {url}"
                )
                self._log(f"SKIP controlled_access: {accession}")
                skipped_controlled.append(accession)
                continue

            tier_rank = _TIER_RANK.get(tier, 99)
            if tier_rank > min_rank:
                self._log(f"SKIP low-tier ({tier}): {accession}")
                continue

            # Skip if accession folder already exists (resume logic)
            dest_folder = self.output_dir / accession
            if dest_folder.exists():
                self._log(f"SKIP already-downloaded: {accession}")
                continue

            if not dataset.get("download_url"):
                self._log(f"SKIP no download_url: {accession}")
                continue

            queue.append(dataset)

        # Write skipped controlled-access list
        if skipped_controlled:
            skip_path = self.output_dir / "skipped_controlled_access.txt"
            with skip_path.open("w", encoding="utf-8") as fh:
                fh.write("\n".join(skipped_controlled) + "\n")
            self._log(
                f"Wrote {len(skipped_controlled)} controlled-access accessions "
                f"to {skip_path}"
            )

        self._log(
            f"Queue loaded: {len(queue)} datasets eligible for download "
            f"(min_tier={self.min_tier})"
        )
        return queue

    def download_all(self) -> None:
        """
        Download all queued datasets in parallel using a thread pool.

        Uses ``concurrent.futures.ThreadPoolExecutor`` with ``self.workers``
        threads.  A tqdm progress bar tracks completion at the dataset level.
        Results are logged; failures do not abort remaining downloads.
        """
        queue = self.load_queue()
        if not queue:
            self._log("Nothing to download.")
            print("Nothing to download.")
            return

        self._log(f"Starting parallel download: {len(queue)} datasets, {self.workers} workers")
        results: dict[str, bool] = {}

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            future_to_acc = {
                pool.submit(self.download_dataset, ds): ds.get("accession", "unknown")
                for ds in queue
            }
            with tqdm(total=len(queue), unit="dataset", desc="Downloading") as pbar:
                for future in as_completed(future_to_acc):
                    accession = future_to_acc[future]
                    try:
                        success = future.result()
                    except Exception as exc:
                        self._log(f"ERROR {accession}: {exc}")
                        success = False
                    results[accession] = success
                    status = "OK" if success else "FAIL"
                    pbar.set_postfix(last=f"{accession}:{status}")
                    pbar.update(1)

        n_ok = sum(1 for v in results.values() if v)
        n_fail = len(results) - n_ok
        self._log(f"Download complete: {n_ok} succeeded, {n_fail} failed")
        print(f"\nDownload complete: {n_ok} succeeded, {n_fail} failed")

        if n_fail:
            failed = [acc for acc, ok in results.items() if not ok]
            print("  Failed accessions:", ", ".join(failed))

    def download_dataset(self, dataset: dict) -> bool:
        """
        Download a single dataset to ``{output_dir}/{accession}/``.

        Uses streaming HTTP GET with 1 MB chunks, retries up to 3 times with
        exponential back-off via tenacity, verifies MD5 checksum when one is
        provided in the registry, and skips the download if the destination
        file already exists and has the expected size.

        Parameters
        ----------
        dataset : dict
            Registry entry containing at least ``"accession"`` and
            ``"download_url"``.

        Returns
        -------
        bool
            ``True`` on success (downloaded or already present), ``False`` on
            any unrecoverable error.
        """
        accession: str = dataset.get("accession") or "unknown"
        url: str = dataset.get("download_url") or ""
        expected_md5: Optional[str] = dataset.get("md5") or None
        expected_size: Optional[int] = (
            int(float(dataset["file_size_gb"]) * 1024 ** 3)
            if dataset.get("file_size_gb")
            else None
        )

        if not url:
            self._log(f"SKIP {accession}: no download_url")
            return False

        dest_dir = self.output_dir / accession
        dest_dir.mkdir(parents=True, exist_ok=True)

        filename = url.split("/")[-1].split("?")[0] or f"{accession}_data"
        dest_file = dest_dir / filename

        # Skip if already present with matching size
        if dest_file.exists():
            if expected_size is None or dest_file.stat().st_size == expected_size:
                self._log(f"SKIP {accession}: file already present ({dest_file.name})")
                return True

        try:
            success = self._download_with_retry(url, dest_file, accession)
        except Exception as exc:
            self._log(f"ERROR {accession}: download failed after retries — {exc}")
            return False

        if not success:
            return False

        # MD5 verification
        if expected_md5:
            actual_md5 = self._md5_file(dest_file)
            if actual_md5 != expected_md5.lower():
                self._log(
                    f"ERROR {accession}: MD5 mismatch — "
                    f"expected {expected_md5}, got {actual_md5}"
                )
                return False
            self._log(f"OK {accession}: MD5 verified")

        self._log(f"OK {accession}: download complete → {dest_file}")
        return True

    def generate_manifest(self, output_path: str) -> None:
        """
        Write a standalone bash download manifest.

        The script contains ``wget`` commands for every GOLD and SILVER dataset
        in the registry (including controlled-access, marked with comments),
        grouped by modality with section headers.  MD5 verification commands
        are included where checksums are available.

        Parameters
        ----------
        output_path : str
            Destination path for ``download_manifest.sh``.
        """
        if not self.registry_path.exists():
            self._log(f"Registry not found: {self.registry_path}")
            return

        with self.registry_path.open("r", encoding="utf-8") as fh:
            registry: list[dict] = json.load(fh)

        min_rank = _TIER_RANK.get(self.min_tier, 1)
        eligible = [
            ds for ds in registry
            if _TIER_RANK.get((ds.get("confidence_tier") or "LOW").upper(), 99) <= min_rank
        ]

        # Group by modality
        by_modality: dict[str, list[dict]] = {}
        for ds in eligible:
            modality = ds.get("modality") or "Unknown"
            by_modality.setdefault(modality, []).append(ds)

        lines: list[str] = [
            "#!/usr/bin/env bash",
            "# ============================================================",
            "# Hickey Lab — Endometrial Receptivity Database Download Manifest",
            f"# Generated: {_now_iso()}",
            f"# Registry:  {self.registry_path}",
            f"# Min tier:  {self.min_tier}",
            "# ============================================================",
            "",
            'set -euo pipefail',
            f'OUTPUT_DIR="{self.output_dir}"',
            "",
        ]

        for modality, datasets in sorted(by_modality.items()):
            lines += [
                "",
                "# " + "=" * 60,
                f"# Modality: {modality}  ({len(datasets)} datasets)",
                "# " + "=" * 60,
                "",
            ]
            for ds in datasets:
                accession = ds.get("accession") or "unknown"
                url = ds.get("download_url") or ""
                md5 = ds.get("md5") or ""
                tier = ds.get("confidence_tier") or "UNKNOWN"
                title = ds.get("title") or ""
                controlled = bool(ds.get("controlled_access", False))

                lines.append(f"# {accession} [{tier}] — {title[:80]}")

                if controlled:
                    lines.append(
                        f"# CONTROLLED ACCESS — manual request required"
                    )
                    lines.append(f"# Request access at: {url}")
                    lines.append("")
                    continue

                if not url:
                    lines.append("# WARNING: no download_url in registry")
                    lines.append("")
                    continue

                filename = url.split("/")[-1].split("?")[0] or f"{accession}_data"
                dest = '"${OUTPUT_DIR}"' + f'/{accession}/{filename}'

                lines.append(f'mkdir -p "${{OUTPUT_DIR}}/{accession}"')
                lines.append(
                    f'wget --continue --progress=bar:force:noscroll \\\n'
                    f'     -O {dest} \\\n'
                    f'     "{url}"'
                )

                if md5:
                    lines.append(
                        f'echo "{md5}  {dest}" | md5sum --check --status \\\n'
                        f'  && echo "MD5 OK: {accession}" \\\n'
                        f'  || echo "MD5 FAIL: {accession}" >&2'
                    )

                lines.append("")

        lines += [
            "",
            'echo "All downloads complete."',
        ]

        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="\n") as fh:
            fh.write("\n".join(lines) + "\n")

        # Make executable on Unix-like systems
        try:
            out_path.chmod(out_path.stat().st_mode | 0o755)
        except OSError:
            pass

        self._log(f"Manifest written: {out_path} ({len(eligible)} datasets)")
        print(f"Manifest written: {out_path}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        """
        Emit a timestamped INFO log line to the logger (and file if configured).

        Parameters
        ----------
        msg : str
            Message text.
        """
        logger.info("[DownloadManager] %s", msg)

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _download_with_retry(
        self, url: str, dest_file: Path, accession: str
    ) -> bool:
        """
        Stream-download *url* to *dest_file* with tenacity retry decoration.

        Parameters
        ----------
        url : str
            Source URL.
        dest_file : Path
            Destination file path.
        accession : str
            Accession string (used only for log messages).

        Returns
        -------
        bool
            ``True`` on success.

        Raises
        ------
        requests.RequestException
            Propagated after all retry attempts are exhausted.
        """
        self._log(f"GET {accession}: {url}")
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0)) or None
            with (
                dest_file.open("wb") as fh,
                tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=accession,
                    leave=False,
                ) as bar,
            ):
                for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
                    if chunk:
                        fh.write(chunk)
                        bar.update(len(chunk))
        return True

    @staticmethod
    def _md5_file(path: Path) -> str:
        """
        Compute the MD5 hex digest of a file.

        Parameters
        ----------
        path : Path
            File to hash.

        Returns
        -------
        str
            Lower-case hex MD5 digest.
        """
        hasher = hashlib.md5()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b""):
                hasher.update(chunk)
        return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
