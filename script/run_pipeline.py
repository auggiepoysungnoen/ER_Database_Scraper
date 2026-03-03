"""
run_pipeline.py
===============
CLI entry point for the Hickey Lab Endometrial Receptivity Database Pipeline.

Orchestrates all scraping, scoring, output writing, and optional download steps
in a single invocation.  Each scraper failure is isolated so a single broken
endpoint never aborts the entire run.

Usage
-----
    python run_pipeline.py [OPTIONS]

Options
-------
  --databases TEXT      Comma-separated databases to scrape [default: all]
                        Choices: geo,arrayexpress,cellxgene,hca,scp,zenodo,figshare
  --min-score FLOAT     Minimum confidence score to include [default: 40]
  --output-dir TEXT     Output directory [default: ../output]
  --cache-dir TEXT      Cache directory for API responses [default: ../output/.cache]
  --download            If set, auto-download GOLD+SILVER datasets after scoring
  --workers INT         Parallel download workers [default: 4]
  --ncbi-key TEXT       NCBI API key (or set NCBI_API_KEY env var)
  --gemini-key TEXT     Gemini API key for AI metadata extraction (or set GEMINI_API_KEY env var)
  --no-ai               Disable Gemini AI enrichment (faster, lower quality scores)
  --search-terms TEXT   Comma-separated tissue/topic terms to search [default: endometrial terms]
  --dry-run             Scrape and score but do not write output files
  --resume              Skip already-scraped accessions (load existing registry)
  --verbose             Verbose logging
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Search-term constants
# ---------------------------------------------------------------------------
TISSUE_TERMS: list[str] = [
    "endometrium",
    "endometrial",
    "uterus",
    "uterine",
    "decidua",
    "decidual",
]

MODALITY_QUERIES: dict[str, list[str]] = {
    "scRNA-seq": [
        "single cell RNA-seq",
        "scRNA-seq",
        "10x Genomics",
        "snRNA-seq",
    ],
    "bulkRNA-seq": [
        "bulk RNA-seq",
        "endometrial transcriptome",
        "RNA-seq uterus",
    ],
    "Spatial Transcriptomics": [
        "spatial transcriptomics endometrium",
        "Visium uterus",
        "MERFISH endometrium",
    ],
    "Spatial Proteomics": [
        "spatial proteomics endometrium",
        "CODEX uterus",
        "IMC endometrium",
    ],
}

EXCLUDE_TERMS: list[str] = [
    "cancer",
    "carcinoma",
    "tumor",
    "sarcoma",
    "cervical cancer",
    "ovarian cancer",
]

ALL_DATABASES: list[str] = [
    "geo",
    "arrayexpress",
    "cellxgene",
    "hca",
    "scp",
    "zenodo",
    "figshare",
]

# ---------------------------------------------------------------------------
# Scraper registry — lazy imports so missing optional deps do not crash startup
# ---------------------------------------------------------------------------
_SCRAPER_MODULE_MAP: dict[str, tuple[str, str]] = {
    "geo":          ("scrapers.geo",              "GEOScraper"),
    "arrayexpress": ("scrapers.arrayexpress",     "ArrayExpressScraper"),
    "cellxgene":    ("scrapers.cellxgene",        "CellxGeneScraper"),
    "hca":          ("scrapers.hca",              "HCAScraper"),
    "scp":          ("scrapers.singlecellportal", "SingleCellPortalScraper"),
    "zenodo":       ("scrapers.zenodo",           "ZenodoScraper"),
    "figshare":     ("scrapers.figshare",         "FigshareScraper"),
}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(output_dir: Path, verbose: bool) -> logging.Logger:
    """
    Configure dual-handler logging (console + file) with timestamps.

    Parameters
    ----------
    output_dir : Path
        Directory for ``scrape_log.txt``.
    verbose : bool
        If ``True``, set level to DEBUG; otherwise INFO.

    Returns
    -------
    logging.Logger
        Configured root logger.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(ch)

    # File handler
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "scrape_log.txt"
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(fh)

    return root


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Fully configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="run_pipeline.py",
        description=(
            "Hickey Lab Endometrial Receptivity Database Pipeline — "
            "scrape, score, and optionally download datasets."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--databases",
        default="all",
        help=(
            "Comma-separated list of databases to scrape. "
            "Choices: geo,arrayexpress,cellxgene,hca,scp,zenodo,figshare  [default: all]"
        ),
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.0,
        metavar="FLOAT",
        help="Minimum confidence score (0-100) to include in output [default: 0]",
    )
    parser.add_argument(
        "--output-dir",
        default="../output",
        metavar="TEXT",
        help="Root output directory [default: ../output]",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        metavar="TEXT",
        help="Cache directory for API responses [default: {output-dir}/.cache]",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Auto-download GOLD+SILVER datasets after scoring",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="INT",
        help="Parallel download workers [default: 4]",
    )
    parser.add_argument(
        "--ncbi-key",
        default=None,
        metavar="TEXT",
        help="NCBI API key (overrides NCBI_API_KEY env var)",
    )
    parser.add_argument(
        "--gemini-key",
        default=None,
        metavar="TEXT",
        help="Gemini API key for AI metadata enrichment (overrides GEMINI_API_KEY env var)",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Disable Gemini AI enrichment (faster runs, lower confidence score quality)",
    )
    parser.add_argument(
        "--search-terms",
        default=None,
        metavar="TEXT",
        help=(
            "Comma-separated tissue/topic terms overriding the built-in TISSUE_TERMS list. "
            "Example: 'placenta,trophoblast,decidua'"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and score but do not write output files",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip already-scraped accessions (load existing registry)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG-level) logging",
    )
    return parser


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

def load_existing_registry(registry_path: Path) -> dict[str, dict]:
    """
    Load an existing ``datasets_registry.json`` keyed by accession.

    Parameters
    ----------
    registry_path : Path
        Path to the registry JSON file.

    Returns
    -------
    dict[str, dict]
        Accession-keyed dict of existing records, or empty dict if not found.
    """
    if not registry_path.exists():
        return {}
    try:
        with registry_path.open("r", encoding="utf-8") as fh:
            records: list[dict] = json.load(fh)
        return {r["accession"]: r for r in records if r.get("accession")}
    except (json.JSONDecodeError, OSError) as exc:
        logging.warning("Could not load existing registry: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

def _load_scraper(db_key: str, api_key: Optional[str], cache_dir: Path):
    """
    Dynamically import and instantiate a scraper class.

    Parameters
    ----------
    db_key : str
        Database key from ``_SCRAPER_MODULE_MAP``.
    api_key : str or None
        API key to pass to the scraper.
    cache_dir : Path
        Cache directory for the scraper.

    Returns
    -------
    BaseScraper or None
        Instantiated scraper, or *None* if import fails.
    """
    if db_key not in _SCRAPER_MODULE_MAP:
        logging.warning("Unknown database key: %s", db_key)
        return None

    module_path, class_name = _SCRAPER_MODULE_MAP[db_key]
    try:
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls(api_key=api_key, cache_dir=str(cache_dir))
    except (ImportError, AttributeError) as exc:
        logging.warning("Could not load scraper %s.%s: %s", module_path, class_name, exc)
        return None


def _build_queries() -> list[str]:
    """
    Construct the full list of search queries from tissue and modality terms.

    Combines each tissue term with each modality query phrase, producing a
    deduplicated cross-product of search strings.

    Returns
    -------
    list[str]
        Deduplicated list of query strings.
    """
    queries: list[str] = []
    seen: set[str] = set()
    for modality_phrases in MODALITY_QUERIES.values():
        for phrase in modality_phrases:
            for tissue in TISSUE_TERMS:
                q = f"{phrase} {tissue}"
                if q not in seen:
                    seen.add(q)
                    queries.append(q)
    return queries


def _is_excluded(record: dict) -> bool:
    """
    Return ``True`` if a dataset record contains any exclusion term.

    Checks ``title`` and ``abstract`` fields.

    Parameters
    ----------
    record : dict
        Dataset metadata dict.

    Returns
    -------
    bool
        ``True`` if the record should be excluded.
    """
    text = " ".join(
        str(record.get(f) or "") for f in ("title", "abstract")
    ).lower()
    return any(term.lower() in text for term in EXCLUDE_TERMS)


def _scrape_database(
    db_key: str,
    scraper: Any,
    queries: list[str],
    existing: dict[str, dict],
    resume: bool,
    log: logging.Logger,
) -> list[dict]:
    """
    Run all queries against a single scraper and collect results.

    Parameters
    ----------
    db_key : str
        Database identifier (for logging).
    scraper : BaseScraper
        Instantiated scraper object.
    queries : list[str]
        Search queries to run.
    existing : dict[str, dict]
        Already-scraped accessions (used when ``resume=True``).
    resume : bool
        If ``True``, skip accessions already present in *existing*.
    log : logging.Logger
        Logger instance.

    Returns
    -------
    list[dict]
        Flat list of raw metadata dicts returned by this scraper.
    """
    results: list[dict] = []
    seen_in_run: set[str] = set()

    for query in queries:
        try:
            raw_list = scraper.search(query)
        except Exception as exc:
            log.warning("[%s] search(%r) failed: %s", db_key, query, exc)
            continue

        for item in raw_list:
            accession = item.get("accession") or ""
            if not accession:
                continue
            if accession in seen_in_run:
                continue
            if resume and accession in existing:
                log.debug("[%s] resume skip: %s", db_key, accession)
                continue

            # Fetch full metadata
            try:
                full = scraper.fetch_metadata(accession)
            except Exception as exc:
                log.warning("[%s] fetch_metadata(%s) failed: %s", db_key, accession, exc)
                full = item  # fall back to search stub

            seen_in_run.add(accession)
            results.append(full)

    log.info("[%s] collected %d records", db_key, len(results))
    return results


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(records: list[dict]) -> list[dict]:
    """
    Deduplicate records by accession, keeping the highest-score source.

    When the same accession appears from multiple databases, the record with
    the higher ``confidence_score`` (or ``final_CS``) is retained.

    Parameters
    ----------
    records : list[dict]
        Flat list of all scraped metadata dicts.

    Returns
    -------
    list[dict]
        Deduplicated list, one record per accession.
    """
    best: dict[str, dict] = {}
    for record in records:
        accession = record.get("accession") or ""
        if not accession:
            continue
        score = float(
            record.get("final_CS")
            or record.get("confidence_score")
            or 0
        )
        existing_score = float(
            best.get(accession, {}).get("final_CS")
            or best.get(accession, {}).get("confidence_score")
            or 0
        )
        if accession not in best or score > existing_score:
            best[accession] = record

    return list(best.values())


# ---------------------------------------------------------------------------
# Output writers (lazy imports)
# ---------------------------------------------------------------------------

def _write_outputs(
    records: list[dict],
    output_dir: Path,
    log: logging.Logger,
) -> None:
    """
    Write all pipeline output files.

    Attempts to import and call each output writer.  Individual writer failures
    are logged but do not abort the others.

    Files written:
    * ``metadata_master.csv``
    * ``confidence_scores.csv``
    * ``datasets_registry.json``
    * ``paper_summaries.json`` + ``paper_summaries.md``
    * ``pipeline_report.html``

    Parameters
    ----------
    records : list[dict]
        Scored and filtered dataset records.
    output_dir : Path
        Root output directory.
    log : logging.Logger
        Logger instance.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- datasets_registry.json (always write directly — no external dep) ---
    registry_path = output_dir / "datasets_registry.json"
    try:
        with registry_path.open("w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False, default=str)
        log.info("Wrote %s (%d records)", registry_path, len(records))
    except OSError as exc:
        log.error("Failed to write datasets_registry.json: %s", exc)

    # --- output.writers ---
    try:
        import importlib
        writers_mod = importlib.import_module("output.writers")
        writers_mod.write_metadata_master(records, records, str(output_dir))
        writers_mod.write_confidence_scores(records, str(output_dir))
        log.info("Wrote metadata_master.csv and confidence_scores.csv")
    except Exception as exc:
        log.warning("output.writers unavailable or failed: %s", exc)

    # --- output.paper_summary ---
    try:
        import importlib
        ps_mod = importlib.import_module("output.paper_summary")
        summaries = ps_mod.generate_paper_summaries(records, records)
        ps_mod.write_paper_summaries_json(summaries, str(output_dir))
        ps_mod.write_paper_summaries_md(summaries, str(output_dir))
        log.info("Wrote paper_summaries.json + .md")
    except Exception as exc:
        log.warning("output.paper_summary unavailable or failed: %s", exc)

    # --- output.report ---
    try:
        import importlib
        from datetime import datetime, timezone
        report_mod = importlib.import_module("output.report")
        report_mod.generate_pipeline_report(
            records, records, str(output_dir),
            datetime.now(timezone.utc).isoformat()
        )
        log.info("Wrote pipeline_report.html")
    except Exception as exc:
        log.warning("output.report unavailable or failed: %s", exc)


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def _print_summary(
    total_scraped: int,
    records: list[dict],
    min_score: float,
) -> None:
    """
    Print a formatted summary table to stdout.

    Parameters
    ----------
    total_scraped : int
        Total number of records scraped before filtering.
    records : list[dict]
        Accepted (filtered) records.
    min_score : float
        The min-score threshold that was applied.
    """
    tier_counts: dict[str, int] = {"GOLD": 0, "SILVER": 0, "BRONZE": 0, "LOW": 0}
    for r in records:
        tier = (r.get("confidence_tier") or "LOW").upper()
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    accepted = len(records)
    rejected = total_scraped - accepted

    sep = "=" * 52
    print(f"\n{sep}")
    print("  Hickey Lab Pipeline — Run Summary")
    print(sep)
    print(f"  Total scraped   : {total_scraped:>6}")
    print(f"  Accepted (>={min_score:.0f}) : {accepted:>6}")
    print(f"  Rejected        : {rejected:>6}")
    print(f"  ---")
    print(f"  GOLD            : {tier_counts['GOLD']:>6}")
    print(f"  SILVER          : {tier_counts['SILVER']:>6}")
    print(f"  BRONZE          : {tier_counts['BRONZE']:>6}")
    print(f"  LOW             : {tier_counts['LOW']:>6}")
    print(sep)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    """
    Main pipeline entry point.

    Parameters
    ----------
    argv : list[str] or None
        Argument vector (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: 0 on success, 1 on fatal error.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
    cache_dir = Path(args.cache_dir).resolve() if args.cache_dir else output_dir / ".cache"
    registry_path = output_dir / "datasets_registry.json"

    log = _setup_logging(output_dir, args.verbose)
    log.info("=" * 60)
    log.info("Hickey Lab Endometrial Receptivity Pipeline — START")
    log.info("  output_dir : %s", output_dir)
    log.info("  cache_dir  : %s", cache_dir)
    log.info("  min_score  : %s", args.min_score)
    log.info("  dry_run    : %s", args.dry_run)
    log.info("  resume     : %s", args.resume)
    log.info("  download   : %s", args.download)
    log.info("=" * 60)

    # --- Determine requested databases ---
    if args.databases.strip().lower() == "all":
        requested_dbs = ALL_DATABASES
    else:
        requested_dbs = [d.strip().lower() for d in args.databases.split(",") if d.strip()]
        unknown = [d for d in requested_dbs if d not in ALL_DATABASES]
        if unknown:
            log.warning("Unrecognised database(s): %s — ignoring", unknown)
            requested_dbs = [d for d in requested_dbs if d in ALL_DATABASES]

    # --- NCBI API key ---
    ncbi_key = args.ncbi_key or os.environ.get("NCBI_API_KEY")

    # --- Gemini API key ---
    gemini_key = args.gemini_key or os.environ.get("GEMINI_API_KEY", "")

    # --- Override search terms if provided via CLI ---
    if getattr(args, "search_terms", None):
        global TISSUE_TERMS
        TISSUE_TERMS = [t.strip() for t in args.search_terms.split(",") if t.strip()]
        log.info("Custom search terms: %s", TISSUE_TERMS)

    # --- Load existing registry (resume mode) ---
    existing: dict[str, dict] = {}
    if args.resume:
        existing = load_existing_registry(registry_path)
        log.info("Resume mode: %d existing accessions loaded", len(existing))

    # --- Build search queries ---
    queries = _build_queries()
    log.info("Built %d search queries across %d modalities", len(queries), len(MODALITY_QUERIES))

    # --- Scrape each database ---
    all_records: list[dict] = []
    scraper_errors: dict[str, str] = {}

    for db_key in requested_dbs:
        api_key = ncbi_key if db_key == "geo" else None
        scraper = _load_scraper(db_key, api_key, cache_dir)
        if scraper is None:
            scraper_errors[db_key] = "scraper not available"
            continue

        log.info("Scraping [%s] ...", db_key)
        try:
            db_records = _scrape_database(
                db_key, scraper, queries, existing, args.resume, log
            )
            all_records.extend(db_records)
        except Exception as exc:
            tb = traceback.format_exc()
            log.error("[%s] Fatal scraper error: %s\n%s", db_key, exc, tb)
            scraper_errors[db_key] = str(exc)

    log.info("Total raw records collected: %d", len(all_records))

    # --- Merge with existing (resume) ---
    if args.resume and existing:
        merged = list(existing.values()) + all_records
    else:
        merged = all_records

    # --- Deduplicate ---
    deduped = deduplicate(merged)
    log.info("After deduplication: %d unique accessions", len(deduped))

    # --- Apply exclusion filter ---
    pre_excl = len(deduped)
    deduped = [r for r in deduped if not _is_excluded(r)]
    log.info(
        "After exclusion filter: %d records (%d removed)",
        len(deduped), pre_excl - len(deduped),
    )

    total_scraped = len(deduped)

    # --- AI metadata enrichment (Gemini) ---
    if not getattr(args, "no_ai", False) and gemini_key:
        log.info("Starting Gemini AI enrichment for %d records …", len(deduped))
        try:
            import importlib
            ai_mod = importlib.import_module("scoring.ai_extractor")
            deduped = ai_mod.batch_enrich(deduped, gemini_key, max_workers=8)
            enriched_n = sum(1 for r in deduped if r.get("ai_enriched"))
            log.info("AI enrichment complete: %d/%d records enriched",
                     enriched_n, len(deduped))
        except Exception as exc:
            log.warning("AI enrichment unavailable or failed: %s — skipping", exc)
    elif not getattr(args, "no_ai", False):
        log.info("No Gemini API key — skipping AI enrichment (pass --gemini-key or set GEMINI_API_KEY)")

    # --- Score all datasets ---
    try:
        import importlib
        scoring_mod = importlib.import_module("scoring")
        ConfidenceScorer = getattr(scoring_mod, "ConfidenceScorer")
        classify_tier = getattr(scoring_mod, "classify_tier")
        scorer = ConfidenceScorer()

        for record in deduped:
            try:
                result = scorer.score(record)
                record.update(result)
                record["confidence_tier"] = classify_tier(result.get("final_CS", 0))
            except Exception as exc:
                log.warning("Scoring failed for %s: %s", record.get("accession"), exc)

        log.info("Scoring complete for %d records", len(deduped))
    except Exception as exc:
        log.warning("Scoring module unavailable or failed: %s — scores not applied", exc)

    # --- Filter by min_score ---
    accepted = [
        r for r in deduped
        if float(r.get("final_CS") or r.get("confidence_score") or 0) >= args.min_score
    ]
    log.info(
        "After min_score=%.1f filter: %d accepted, %d rejected",
        args.min_score, len(accepted), total_scraped - len(accepted),
    )

    # --- Generate paper summaries (in-memory, before writing) ---
    try:
        import importlib
        ps_mod = importlib.import_module("output.paper_summary")
        if hasattr(ps_mod, "enrich_records"):
            accepted = ps_mod.enrich_records(accepted)
            log.info("Paper summaries generated")
    except Exception as exc:
        log.debug("paper_summary.enrich_records not available: %s", exc)

    # --- Write outputs ---
    if not args.dry_run:
        _write_outputs(accepted, output_dir, log)
    else:
        log.info("Dry-run mode: skipping all file writes")

    # --- Optional download ---
    if args.download and not args.dry_run:
        if not registry_path.exists():
            log.warning("--download requested but registry not written; skipping")
        else:
            log.info("Starting DownloadManager ...")
            try:
                from downloader import DownloadManager
                manager = DownloadManager(
                    registry_path=str(registry_path),
                    output_dir=str(output_dir / "raw"),
                    workers=args.workers,
                    min_tier="SILVER",
                    log_path=str(output_dir / "download_log.txt"),
                )
                manager.download_all()
                manager.generate_manifest(str(output_dir / "download_manifest.sh"))
            except Exception as exc:
                log.error("DownloadManager failed: %s\n%s", exc, traceback.format_exc())

    # --- Print summary ---
    _print_summary(total_scraped, accepted, args.min_score)

    if scraper_errors:
        print("\nScraper errors:")
        for db, err in scraper_errors.items():
            print(f"  [{db}] {err}")

    log.info("Pipeline complete — %d datasets accepted", len(accepted))
    log.info("Output directory: %s", output_dir)

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
