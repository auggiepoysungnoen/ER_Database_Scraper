# Endometrial Receptivity Database Pipeline
### Hickey Lab · Duke University · Aim01

An automated pipeline that discovers, evaluates, downloads, and catalogues open-access genomics datasets relevant to **endometrial receptivity** and the **Window of Implantation (WOI)**.

---

## Overview

This pipeline scrapes four data modalities across nine databases:

| Modality | Examples |
|---|---|
| scRNA-seq / snRNA-seq | 10x Chromium, SMART-seq2, Drop-seq |
| Bulk RNA-seq | Illumina short-read, microarray |
| Spatial Transcriptomics | 10x Visium, MERFISH, Xenium |
| Spatial Proteomics | CODEX, IMC, CyCIF, GeoMx |

Each dataset is assigned a **Confidence Score (0–100)** and a tier (GOLD / SILVER / BRONZE / LOW_CONFIDENCE). Results are browsable in an interactive Streamlit web app with 8 Plotly dashboards.

---

## Repository Structure

```
Aim01_Database_Regeneration/
├── script/
│   ├── scrapers/          # One module per database
│   ├── scoring/           # Confidence score formula
│   ├── output/            # CSV / JSON / HTML writers
│   ├── downloader/        # Parallel download manager
│   ├── app/               # Streamlit web app
│   │   └── pages/         # One file per page
│   └── run_pipeline.py    # CLI entry point
├── output/                # Generated outputs (gitignored for raw data)
│   └── raw/               # Downloaded datasets (gitignored)
├── documentation/         # This folder
├── prompt/                # Original specification
├── requirements.txt
├── environment.yml
├── .streamlit/
│   └── secrets.toml       # Auth credentials (gitignored — template only)
└── PROGRESS.md            # Build progress log
```

---

## Quick Start

### 1. Clone and set up environment

```bash
git clone https://github.com/hickey-lab/endometrial-receptivity-db.git
cd endometrial-receptivity-db

# Option A — conda (recommended)
conda env create -f environment.yml
conda activate endometrial-db

# Option B — pip
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure API keys and credentials

```bash
cp .streamlit/secrets.toml .streamlit/secrets.toml.local
# Edit .streamlit/secrets.toml and fill in:
#   - NCBI API key (see documentation/API_KEYS.md)
#   - bcrypt-hashed app password
```

Generate a bcrypt password hash:
```bash
python -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"
```
Paste the output into `secrets.toml` under `[auth] password_hash`.

### 3. Run the scraping pipeline

```bash
# Full run — all databases, all modalities
python script/run_pipeline.py

# Targeted run — GEO + ArrayExpress only, auto-download GOLD datasets
python script/run_pipeline.py --databases geo,arrayexpress --download

# Dry run — scrape and score but do not write files
python script/run_pipeline.py --dry-run --verbose

# Resume a previous run (skip already-scraped accessions)
python script/run_pipeline.py --resume
```

Output files are written to `output/`:
- `metadata_master.csv`
- `confidence_scores.csv`
- `datasets_registry.json`
- `paper_summaries.json` + `paper_summaries.md`
- `pipeline_report.html`

### 4. Launch the web app

```bash
streamlit run script/app/main.py
```

Navigate to `http://localhost:8501`. Log in with the credentials from `secrets.toml`.

---

## Confidence Score Interpretation

| Tier | Score | Meaning |
|---|---|---|
| GOLD | 80–100 | High-quality, well-annotated, WOI coverage, raw data available |
| SILVER | 60–79 | Good quality, some gaps in metadata or temporal coverage |
| BRONZE | 40–59 | Usable but limited annotation or small sample size |
| LOW_CONFIDENCE | <40 | Excluded from primary analysis; manually review before use |

See `documentation/METHODS.md` for the full scoring formula.

---

## Adding New Search Terms or Databases

**New search terms:** Edit `TISSUE_TERMS` and `MODALITY_QUERIES` constants at the top of `script/run_pipeline.py`.

**New database:** Create `script/scrapers/newdb.py` inheriting from `BaseScraper`, implement `search()` and `fetch_metadata()` returning the standardized metadata dict, then add it to `script/scrapers/__init__.py` and the `--databases` argument parser in `run_pipeline.py`.

---

## Requesting Controlled-Access Datasets

Datasets from **dbGaP** (accession prefix `phs`) and **EGA** (prefix `EGAD`) are flagged in the registry but never auto-downloaded. To request access:

- **dbGaP:** https://dbgap.ncbi.nlm.nih.gov/aa/wga.cgi?page=login
- **EGA:** https://ega-archive.org/access/data-access

Once approved, download manually and place files in `output/raw/Controlled/{accession}/`.

---

## Citation

If you use this pipeline in your work, please cite:

> Hickey Lab, Duke University. *Endometrial Receptivity Database Pipeline (Aim01)*. 2026. https://github.com/hickey-lab/endometrial-receptivity-db

---

## Contact

Hickey Lab · Department of Biomedical Engineering · Duke University
PI: John Hickey
