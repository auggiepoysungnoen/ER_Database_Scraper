# Data Dictionary

## metadata_master.csv

| Column | Type | Description |
|---|---|---|
| accession | str | Database accession (GSE*, E-MTAB-*, dataset_id, etc.) |
| source_db | str | Source database: GEO, ArrayExpress, CELLxGENE, HCA, SCP, Zenodo, figshare |
| title | str | Full dataset/study title |
| doi | str \| None | DOI URL (https://doi.org/...) or None if unavailable |
| pubmed_id | str \| None | PubMed ID of associated publication |
| authors | str | Formatted as "Last et al., Year, Journal" |
| journal | str \| None | Journal name |
| peer_reviewed | str | "Yes", "No", or "Preprint" |
| year | int \| None | Publication year |
| modality | str | scRNA-seq / bulkRNA-seq / Spatial Transcriptomics / Spatial Proteomics / Unknown |
| platform | str \| None | Sequencing/imaging platform (e.g., "10x Chromium v3.1", "Visium") |
| n_patients | int \| None | Number of human donors/patients |
| n_samples | int \| None | Number of samples/libraries |
| n_cells | int \| None | Total cell count (scRNA-seq/ST); None for bulk |
| organism | str | Always "Homo sapiens" for included datasets |
| lh_timepoints | str | Semicolon-separated LH timepoints (e.g., "LH+5;LH+7;LH+9") |
| cycle_phases | str | Semicolon-separated cycle phases |
| sub_compartments | str | Semicolon-separated cell types / tissue regions |
| disease_groups | str | Semicolon-separated disease conditions (e.g., "healthy;endometriosis") |
| raw_data_available | bool | True if raw count matrix is publicly downloadable |
| controlled_access | bool | True if dbGaP/EGA approval required |
| download_url | str \| None | Direct download URL or FTP path |
| file_size_gb | float \| None | Estimated total file size in GB |
| confidence_score | float | Final confidence score [0–100] |
| confidence_tier | str | GOLD / SILVER / BRONZE / LOW_CONFIDENCE |
| date_scraped | str | ISO 8601 datetime of scrape (e.g., "2026-03-02T18:00:00Z") |

---

## confidence_scores.csv

| Column | Type | Description |
|---|---|---|
| accession | str | Dataset accession |
| DQS | float | Data Quality Score [0–25] |
| TRS | float | Temporal Resolution Score [0–25] |
| SRS | float | Sub-compartment Resolution Score [0–20] |
| MCS | float | Metadata Completeness Score [0–15] |
| DAS | float | Dataset Accessibility Score [0–15] |
| penalties | float | Total penalty points (negative value) |
| modality_weight | float | Modality downweight factor [0.30–1.00] |
| final_CS | float | (DQS+TRS+SRS+MCS+DAS+penalties) × modality_weight, clamped [0–100] |
| confidence_tier | str | GOLD / SILVER / BRONZE / LOW_CONFIDENCE |

---

## datasets_registry.json

Top-level structure:
```json
{
  "GSE123456": {
    "accession": "GSE123456",
    "source_db": "GEO",
    "title": "...",
    "doi": "https://doi.org/...",
    ...all metadata_master fields...,
    "lh_timepoints": ["LH+5", "LH+7"],      ← array (not semicolon-joined)
    "sub_compartments": ["luminal epithelium", "uNK"],
    "disease_groups": ["healthy", "RIF"],
    "score": {
      "DQS": 20.0, "TRS": 16.0, "SRS": 10.0, "MCS": 9.0, "DAS": 13.0,
      "penalties": 0.0, "modality_weight": 1.0, "final_CS": 68.0,
      "confidence_tier": "SILVER",
      "score_breakdown": {...}
    },
    "paper_summary": {
      "aim": "...", "methodology": "...", "findings": "...", "relevance": "..."
    }
  },
  ...
}
```

---

## paper_summaries.md (section header format)

```markdown
## [MODALITY] Datasets

### GSE123456 — SILVER (68.0)
**Title:** ...
**Authors:** Last et al., 2024, Nature Communications
**DOI:** https://doi.org/...

#### Aim
...

#### Dataset
| Field | Value |
...

#### Brief Methodology
...

#### Brief Findings
...

#### Relevance to Hickey Lab Aim01
...
```
