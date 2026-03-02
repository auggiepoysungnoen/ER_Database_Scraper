# Methods — Endometrial Receptivity Database Pipeline

## 1. Database Scraping

Each database is queried programmatically via its official API. Queries are
constructed by combining tissue terms with modality-specific terms using boolean
AND logic. Results are cached locally to avoid redundant API calls on re-runs.

### 1.1 NCBI GEO (E-utilities)

Endpoint: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`

Query construction:
```
({tissue_term}[Title/Abstract]) AND ({modality_term}[Title/Abstract])
AND "Homo sapiens"[Organism]
```

Rate limit: 10 requests/second with API key; 3/second without.
Metadata retrieved via `esummary` (db=gds). PubMed abstracts fetched via
`efetch` (db=pubmed) using linked PubMed IDs.

### 1.2 ArrayExpress / BioStudies (EMBL-EBI)

Endpoint: `https://www.ebi.ac.uk/biostudies/api/v1/search`

Filters: `type=study`, `organism=Homo sapiens`. Text search across title,
description, and sample annotations.

### 1.3 CELLxGENE Census (CZI)

Uses the `cellxgene-census` Python package to query the SOMA object store.
Tissue filter applied: `tissue_general` contains "uterus" or "endometrium".
Cell metadata (cell types, disease, assay) retrieved from the `obs` table.

### 1.4 Human Cell Atlas (DCP API)

Endpoint: `https://service.azul.data.humancellatlas.org/index/projects`
Filter: `organ = ["uterus"]`. Project-level metadata retrieved including
library construction approach and organ parts.

### 1.5 Single Cell Portal (Broad Institute)

Endpoint: `https://singlecell.broadinstitute.org/single_cell/api/v1/search`
Free-text search with tissue keyword filtering.

### 1.6 Zenodo / figshare

REST API search filtered to `type=dataset`. Results post-filtered by
keyword matching against title and description for endometrial/uterine terms.

---

## 2. Dataset Inclusion / Exclusion Criteria

**Include if:**
- Organism: Homo sapiens
- Tissue: endometrium, uterus, decidua, or myometrium (with endometrial context)
- At least one modality keyword matched

**Exclude if:**
- Title/abstract contains any exclusion term: cancer, carcinoma, tumor,
  sarcoma, cervical cancer, ovarian cancer
- Dataset is exclusively from cell lines or organoids (confidence weight ×0.30;
  not hard-excluded — flagged for review)
- Accession does not resolve in source database (broken link → flagged `DOI_BROKEN`)

---

## 3. Confidence Score Formula

Final CS = (DQS + TRS + SRS + MCS + DAS − Penalties) × Modality_Weight

Clamped to [0, 100].

### 3.1 Worked Example

Dataset: GSE12345 — scRNA-seq, 10x Chromium, N=8 patients, N=45,000 cells,
timepoints LH+5/LH+7/LH+9, cell types: LE, GE, stroma, uNK, macrophage,
raw counts available, no controlled access.

| Dimension | Calculation | Score |
|---|---|---|
| DQS | raw+10, QC+5, cells≥5k+5, depth inferred+0 | 20 |
| TRS | 3 LH timepoints×2=6, WOI present+10, not longitudinal+0 | 16 |
| SRS | 5 compartments×1=5, immune present+5, not spatial+0 | 10 |
| MCS | no age/BMI=0, cycle phase+5, disease group+4, parity=0 | 9 |
| DAS | public+10, DOI+3, single format+0 | 13 |
| **Subtotal** | | **68** |
| Penalties | none | 0 |
| Modality weight | scRNA-seq × 1.00 | × 1.00 |
| **Final CS** | | **68 → SILVER** |

### 3.2 Reproducibility

Given identical dataset metadata, the formula is deterministic. The scoring
module contains no random state. Scores are recalculated from scratch on each
pipeline run unless `--resume` is passed, in which case existing scores are
preserved for unchanged accessions.

---

## 4. Metadata Parsing

### LH Timepoint Extraction

Regular expressions applied to title, abstract, and sample descriptions:

```
Patterns: LH\s*[+-]?\s*\d+, LH\+\d+, LH-\d+
Phrase maps:
  "proliferative phase"   → "proliferative"
  "early secretory"       → "early secretory"
  "mid-secretory" / "WOI" / "window of implantation" → "mid-secretory (WOI)"
  "late secretory"        → "late secretory"
```

### Sub-compartment Detection

Case-insensitive substring matching against a curated vocabulary of 15 terms:
luminal epithelium, glandular epithelium, stroma, stromal fibroblasts, uNK,
uterine natural killer, macrophage, T cell, endothelium, decidual,
smooth muscle, B cell, dendritic cell, mast cell, pericyte.

### Modality Detection

Priority-ordered keyword matching:
1. Spatial Proteomics keywords (CODEX, IMC, CyCIF, GeoMx, MIBI)
2. Spatial Transcriptomics keywords (Visium, MERFISH, seqFISH, Xenium)
3. scRNA-seq keywords (10x Genomics, single cell, scRNA-seq, Drop-seq)
4. bulkRNA-seq keywords (RNA-seq, bulk, transcriptome, microarray)
5. Default: "Unknown"

---

## 5. Known Limitations

- LH timepoint extraction relies on text parsing; studies that report cycle
  days without LH referencing may be under-annotated.
- Sub-compartment detection cannot capture novel or non-standard cell type
  nomenclature outside the curated vocabulary.
- Journal impact factor is not currently auto-retrieved (field left as None).
- CELLxGENE requires `cellxgene-census` package which downloads a large index
  (~2 GB) on first run.
- GEO supplemental file lists may be incomplete for older deposits.
- Confidence scores reflect data availability and annotation quality, not
  biological relevance or experimental validity.
