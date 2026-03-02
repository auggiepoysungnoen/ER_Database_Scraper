"""
Documentation — Endometrial Receptivity Database
"""

from pathlib import Path
import sys
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Documentation | Endometrial Receptivity DB",
    layout="wide",
    page_icon="📚",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"

# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------
st.title("📚 Documentation")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Getting Started",
    "Confidence Score",
    "Data Dictionary",
    "Databases",
    "Search Terms",
    "Controlled Access",
])

# ===========================================================================
# Tab 1 — Getting Started
# ===========================================================================
with tab1:
    st.markdown(
        f"""
        ## Getting Started

        ### Prerequisites

        - Python ≥ 3.10
        - Git
        - conda or virtualenv (recommended)

        ---

        ### 1. Clone the repository

        ```bash
        git clone <repo-url>
        cd Aim01_Database_Regeneration
        ```

        ---

        ### 2. Install dependencies

        ```bash
        pip install -r requirements.txt
        ```

        Key packages include:
        - `streamlit` — web application framework
        - `pandas`, `numpy` — data manipulation
        - `plotly` — interactive visualisations
        - `bcrypt` — password hashing for authentication
        - `scikit-learn` — TF-IDF keyword extraction
        - `networkx` — keyword co-occurrence graph layout
        - `requests`, `biopython`, `GEOparse` — database scrapers

        ---

        ### 3. Configure authentication

        Create `.streamlit/secrets.toml` in the project root:

        ```toml
        [auth]
        username = "hickeylab"
        password_hash = "$2b$12$..."   # bcrypt hash — generate with script below
        ```

        Generate a bcrypt hash:

        ```python
        import bcrypt
        pw = b"your-password-here"
        print(bcrypt.hashpw(pw, bcrypt.gensalt()).decode())
        ```

        ---

        ### 4. Run the data pipeline

        ```bash
        python script/run_pipeline.py
        ```

        This will:
        1. Scrape all nine source databases
        2. Download and parse metadata
        3. Score each dataset (0–100)
        4. Write `output/metadata_master.csv`, `output/confidence_scores.csv`,
           and `output/datasets_registry.json`

        ---

        ### 5. Launch the web application

        ```bash
        streamlit run script/app/main.py
        ```

        The app will open at `http://localhost:8501`.

        ---

        ### Directory structure

        ```
        Aim01_Database_Regeneration/
        ├── script/
        │   ├── app/              ← Streamlit application (this code)
        │   │   ├── main.py
        │   │   ├── auth.py
        │   │   └── pages/
        │   ├── scrapers/         ← Per-database scrapers
        │   ├── scoring/          ← Confidence scoring engine
        │   └── run_pipeline.py   ← Pipeline entry point
        ├── output/
        │   ├── metadata_master.csv
        │   ├── confidence_scores.csv
        │   └── datasets_registry.json
        └── .streamlit/
            └── secrets.toml      ← Auth credentials (never commit)
        ```
        """
    )

# ===========================================================================
# Tab 2 — Confidence Score
# ===========================================================================
with tab2:
    st.markdown(
        """
        ## Confidence Score System

        Each dataset receives a **composite score from 0 to 100** derived from five
        independently weighted dimensions. The score determines the tier assignment.

        ### Tier Thresholds

        | Score Range | Tier | Meaning |
        |------------|------|---------|
        | ≥ 80 | **GOLD** | High-priority; use without major caveats |
        | 60 – 79 | **SILVER** | Good quality; minor limitations noted |
        | 40 – 59 | **BRONZE** | Usable with caution; key limitations present |
        | < 40 | **LOW_CONFIDENCE** | Significant quality issues; use with care |

        ---

        ### Scoring Dimensions

        | Dimension | Abbreviation | Max Points | Description |
        |-----------|-------------|-----------|-------------|
        | Data Quality Score | **DQS** | 25 | Raw data availability, sequencing depth, QC metrics |
        | Temporal Resolution Score | **TRS** | 25 | LH/P+x staging coverage across the cycle |
        | Sample Representation Score | **SRS** | 20 | Patient N, disease groups, demographic diversity |
        | Methodological Completeness Score | **MCS** | 20 | Protocols, antibodies, cell isolation, reproducibility |
        | Data Accessibility Score | **DAS** | 10 | Open access vs. controlled; download convenience |

        ---

        ### Formula

        ```
        Total Score = DQS + TRS + SRS + MCS + DAS − Σ(penalties)
        ```

        Penalties are applied for:
        - Missing metadata fields (per dimension)
        - No raw counts matrix (DQS −5)
        - Fewer than 3 LH timepoints (TRS −5)
        - N < 5 patients (SRS −5)
        - No protocol/methods section (MCS −5)
        - Controlled access only with no alternative (DAS −3)

        ---

        ### DQS sub-criteria (25 pts)

        | Sub-criterion | Points |
        |--------------|--------|
        | Raw counts matrix available | 8 |
        | QC metrics reported (% MT, doublets) | 5 |
        | Sequencing depth ≥ 50,000 reads/cell | 5 |
        | Platform documented | 4 |
        | Cell ranger / pipeline version stated | 3 |

        ### TRS sub-criteria (25 pts)

        | Sub-criterion | Points |
        |--------------|--------|
        | LH or P+x staging documented | 10 |
        | WOI window covered (LH+5–LH+9) | 8 |
        | ≥ 5 distinct timepoints | 5 |
        | Proliferative and secretory both present | 2 |

        ### SRS sub-criteria (20 pts)

        | Sub-criterion | Points |
        |--------------|--------|
        | N ≥ 20 patients | 8 |
        | Includes disease group (e.g. RIF, endometriosis) | 5 |
        | Age and BMI reported | 4 |
        | Ethnicity / reproductive history noted | 3 |

        ### MCS sub-criteria (20 pts)

        | Sub-criterion | Points |
        |--------------|--------|
        | Cell isolation protocol described | 7 |
        | Library preparation kit named | 5 |
        | Antibody panel (for spatial/CITE-seq) | 4 |
        | Bioinformatics pipeline documented | 4 |

        ### DAS sub-criteria (10 pts)

        | Sub-criterion | Points |
        |--------------|--------|
        | Fully open access | 6 |
        | Direct download URL available | 4 |
        """
    )

# ===========================================================================
# Tab 3 — Data Dictionary
# ===========================================================================
with tab3:
    st.markdown("## Data Dictionary — `metadata_master.csv`")

    import pandas as pd

    data_dict = [
        ("accession",          "str",   "Primary accession (GEO, ArrayExpress, EGA…)"),
        ("title",              "str",   "Study title as listed in source database"),
        ("authors",            "str",   "Author list (semicolon-separated)"),
        ("year",               "int",   "Publication year"),
        ("journal",            "str",   "Journal name"),
        ("doi",                "str",   "DOI of the associated publication"),
        ("source_db",          "str",   "Source database (GEO, AE, EGA, CELLxGENE…)"),
        ("modality",           "str",   "scRNA-seq | bulkRNA-seq | Spatial Transcriptomics | Spatial Proteomics"),
        ("platform",           "str",   "Sequencing platform (10x Chromium, Smart-seq2…)"),
        ("n_patients",         "int",   "Number of biological donors / subjects"),
        ("n_cells",            "int",   "Total cells (scRNA-seq / spatial) or N/A for bulk"),
        ("n_samples",          "int",   "Number of sequencing samples"),
        ("lh_timepoints",      "str",   "Comma-separated LH/P+x timepoints (e.g. LH+0, LH+5)"),
        ("sub_compartments",   "str",   "Comma-separated cell types or tissue compartments profiled"),
        ("disease_group",      "str",   "Disease/condition (e.g. Healthy, RIF, Endometriosis)"),
        ("age",                "float", "Mean donor age (years) where reported"),
        ("bmi",                "float", "Mean BMI where reported"),
        ("abstract",           "str",   "Full abstract text from publication"),
        ("confidence_score",   "float", "Composite confidence score 0–100"),
        ("confidence_tier",    "str",   "GOLD | SILVER | BRONZE | LOW_CONFIDENCE"),
        ("dqs",                "float", "Data Quality Score sub-component (0–25)"),
        ("trs",                "float", "Temporal Resolution Score sub-component (0–25)"),
        ("srs",                "float", "Sample Representation Score sub-component (0–20)"),
        ("mcs",                "float", "Methodological Completeness Score sub-component (0–20)"),
        ("das",                "float", "Data Accessibility Score sub-component (0–10)"),
        ("download_url",       "str",   "Direct download URL (if available)"),
        ("controlled_access",  "bool",  "True if dataset requires dbGaP/EGA application"),
        ("raw_data_available", "bool",  "True if raw counts matrix is available"),
        ("file_size_gb",       "float", "Estimated download size in GB"),
        ("md5",                "str",   "MD5 checksum of primary download file"),
    ]

    dd_df = pd.DataFrame(data_dict, columns=["Column", "Type", "Description"])
    st.dataframe(
        dd_df.style.set_properties(
            subset=["Column"], **{"font-family": "monospace", "font-size": "0.85rem"}
        ),
        use_container_width=True,
        hide_index=True,
    )

# ===========================================================================
# Tab 4 — Databases
# ===========================================================================
with tab4:
    st.markdown("## Source Databases")

    import pandas as pd

    db_table = [
        ("GEO",         "NCBI Gene Expression Omnibus",       "https://www.ncbi.nlm.nih.gov/geo/",            "Open",       "GEO query API (Entrez)"),
        ("ArrayExpress","EBI ArrayExpress / BioStudies",       "https://www.ebi.ac.uk/biostudies/arrayexpress","Open",       "BioStudies REST API"),
        ("ENCODE",      "Encyclopedia of DNA Elements",         "https://www.encodeproject.org/",              "Open",       "ENCODE REST API"),
        ("GTEx",        "Genotype-Tissue Expression Project",   "https://gtexportal.org/",                     "Open",       "GTEx REST API"),
        ("HCA",         "Human Cell Atlas",                    "https://www.humancellatlas.org/",              "Open",       "HCA Data Portal API"),
        ("dbGaP",       "Database of Genotypes & Phenotypes",  "https://www.ncbi.nlm.nih.gov/gap/",           "Controlled", "Entrez eSearch"),
        ("EGA",         "European Genome-phenome Archive",     "https://ega-archive.org/",                    "Controlled", "EGA REST API"),
        ("CELLxGENE",   "CZ CELLxGENE Discover",              "https://cellxgene.cziscience.com/",           "Open",       "CELLxGENE REST API"),
        ("Zenodo",      "Zenodo research data repository",     "https://zenodo.org/",                         "Open",       "Zenodo REST API"),
    ]

    db_df = pd.DataFrame(db_table, columns=["Short Name", "Full Name", "URL", "Access", "API Method"])

    def _style_access(val):
        if val == "Controlled":
            return "color: #E53935; font-weight: bold;"
        return "color: #2E7D32; font-weight: bold;"

    styled_db = db_df.style.applymap(_style_access, subset=["Access"])
    st.dataframe(styled_db, use_container_width=True, hide_index=True)

    st.markdown(
        """
        ---
        ### API Notes

        - **GEO**: Use `Entrez.esearch(db="gds", term=query)` followed by `Entrez.esummary` for metadata.
        - **ArrayExpress / BioStudies**: `GET https://www.ebi.ac.uk/biostudies/api/v1/search?query=...`
        - **CELLxGENE**: `GET https://api.cellxgene.cziscience.com/curation/v1/collections`
        - **dbGaP**: Metadata via Entrez; controlled data requires approved application + dbGaP key.
        - **EGA**: Metadata via `GET https://ega-archive.org/metadata/v2/datasets`; access requires DAC approval.
        """
    )

# ===========================================================================
# Tab 5 — Search Terms
# ===========================================================================
with tab5:
    st.markdown(
        """
        ## Search Terms Used in Database Queries

        ### Tissue / Anatomy Terms
        ```
        endometrium
        endometrial
        uterine lining
        uterine endometrium
        endometrial stroma
        endometrial epithelium
        endometrial glands
        decidua
        ```

        ### Modality Terms
        ```
        single-cell RNA-seq
        scRNA-seq
        single cell transcriptomics
        bulk RNA-seq
        RNA sequencing
        spatial transcriptomics
        10x Visium
        MERFISH
        seqFISH
        spatial proteomics
        CODEX
        CyCIF
        IMC (imaging mass cytometry)
        CITE-seq
        ```

        ### Temporal / Cycle Phase Terms
        ```
        menstrual cycle
        window of implantation
        WOI
        LH surge
        luteal phase
        proliferative phase
        secretory phase
        mid-secretory
        peri-implantation
        LH+0  LH+2  LH+5  LH+7  LH+9
        P+x (progesterone-referenced staging)
        ```

        ### Disease / Condition Terms
        ```
        recurrent implantation failure  (RIF)
        recurrent pregnancy loss        (RPL)
        endometriosis
        uterine fibroids
        adenomyosis
        polycystic ovary syndrome       (PCOS)
        unexplained infertility
        thin endometrium
        hydrosalpinx
        ```

        ### Exclusion Terms (applied to filter irrelevant results)
        ```
        endometrial cancer
        endometrial carcinoma
        uterine cancer
        cervical cancer
        ovarian cancer
        mouse
        murine
        rat
        in vitro
        organoid        (unless explicitly paired with endometrial receptivity)
        ```

        ### Boolean Query Template

        ```
        (endometrium OR endometrial OR "uterine lining") AND
        ("single-cell" OR "scRNA-seq" OR "bulk RNA" OR "spatial transcriptomics" OR "proteomics") AND
        ("menstrual cycle" OR "implantation" OR "luteal" OR "secretory")
        NOT ("cancer" OR "carcinoma" OR "mouse" OR "murine")
        ```
        """
    )

# ===========================================================================
# Tab 6 — Controlled Access
# ===========================================================================
with tab6:
    st.markdown(
        """
        ## Controlled Access Datasets

        Some datasets in this database require a formal data access application before
        download. These are marked with a **Controlled Access** badge in the Search and
        Download pages.

        ---

        ### dbGaP (Database of Genotypes and Phenotypes)

        **Authority**: NIH / NCBI

        1. Create an [eRA Commons account](https://public.era.nih.gov/commons/) if you
           do not already have one.
        2. Navigate to [dbGaP](https://www.ncbi.nlm.nih.gov/gap/) and search for the
           accession (e.g. `phs001234`).
        3. Click **Request Access** on the study page.
        4. Complete the **Data Access Request (DAR)** form:
           - Specify intended research use
           - Attach IRB approval or exemption letter
           - List all investigators who will access the data
        5. Submit to the relevant **Data Access Committee (DAC)**.
        6. Approval typically takes **4–8 weeks**.
        7. Once approved, download via:
           ```bash
           prefetch phs001234 --output-directory ./downloads/
           # or via the dbGaP download toolkit
           ```

        ---

        ### EGA (European Genome-phenome Archive)

        **Authority**: EMBL-EBI / CRG

        1. Register at [EGA](https://ega-archive.org/).
        2. Search for the EGAD or EGAN accession.
        3. Click **Request Access** — this contacts the **Data Access Committee (DAC)**
           for the specific study.
        4. Submit your data access agreement (DAA) signed by your institution.
        5. Once approved, download using the **EGA download client**:
           ```bash
           pip install pyega3
           pyega3 -cf credentials.json fetch EGAD00001000001
           ```

        ---

        ### Tips

        - Keep a copy of your approval letter — required for data transfer agreements.
        - Data should remain on institutional compute infrastructure.
        - Do **not** share controlled-access data with unapproved collaborators.
        - Cite the dataset and original study in any publication using these data.

        ---
        """
    )

st.info(
    "For bugs, missing datasets, or scoring questions, contact the Hickey Lab "
    "(Duke University, Department of Cell Biology). "
    "Submit issues via the lab's internal GitHub repository.",
    icon="ℹ️",
)
