"""
Documentation — Endometrial Receptivity Database
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
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
# Auth
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1100px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.25rem 0!important}
.stButton>button{border-radius:2px;font-weight:500}
[data-testid="stDataFrame"]{border:1px solid #e5e7eb;border-radius:2px}
/* Prose */
.prose{font-family:Arial,sans-serif;font-size:0.875rem;color:#374151;line-height:1.75}
.prose h2{font-size:1.15rem;font-weight:700;color:#012169;margin:1.5rem 0 0.5rem 0}
.prose h3{font-size:0.95rem;font-weight:700;color:#012169;margin:1.2rem 0 0.35rem 0}
.prose p{margin:0 0 0.75rem 0}
.prose code{background:#f3f4f6;padding:1px 5px;border-radius:2px;
            font-size:0.8rem;font-family:monospace;color:#374151}
.prose pre{background:#f8f9fa;border:1px solid #e5e7eb;border-radius:2px;
           padding:0.85rem 1rem;overflow-x:auto;margin:0.75rem 0}
.prose pre code{background:none;padding:0;font-size:0.8rem}
.prose table{width:100%;border-collapse:collapse;font-size:0.82rem;margin:0.75rem 0}
.prose th{background:#f8f9fa;font-weight:700;color:#012169;
          padding:0.5rem 0.75rem;border:1px solid #e5e7eb;text-align:left}
.prose td{padding:0.45rem 0.75rem;border:1px solid #e5e7eb;color:#374151}
.prose tr:nth-child(even){background:#fafafa}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown("""
<p style="font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
          color:#012169;letter-spacing:-0.02em;margin-bottom:0">
    Documentation
</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 1.25rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Getting Started",
    "Confidence Score",
    "Data Dictionary",
    "Source Databases",
    "Search Terms",
    "Controlled Access",
])

# ===========================================================================
# Tab 1 — Getting Started
# ===========================================================================
with tab1:
    st.markdown("""
<div class="prose">

<h2>Getting Started</h2>

<h3>Prerequisites</h3>
<p>Python ≥ 3.10 · Git · conda or virtualenv (recommended)</p>

<h3>1. Clone the repository</h3>
<pre><code>git clone &lt;repo-url&gt;
cd Aim01_Database_Regeneration</code></pre>

<h3>2. Install dependencies</h3>
<pre><code>pip install -r requirements.txt</code></pre>

<p>Key packages:</p>
<ul>
<li><code>streamlit</code> — web application framework</li>
<li><code>pandas</code>, <code>numpy</code> — data manipulation</li>
<li><code>plotly</code> — interactive visualisations</li>
<li><code>bcrypt</code> — password hashing for authentication</li>
<li><code>scikit-learn</code> — TF-IDF keyword extraction</li>
<li><code>networkx</code> — keyword co-occurrence graph layout</li>
<li><code>requests</code>, <code>GEOparse</code> — database scrapers</li>
</ul>

<h3>3. Configure authentication</h3>
<p>Create <code>.streamlit/secrets.toml</code> in the project root:</p>
<pre><code>[auth]
username = "hickeylab"
password_hash = "$2b$12$..."   # generate with bcrypt below

[ncbi]
api_key = "your-ncbi-api-key"</code></pre>

<p>Generate a bcrypt hash:</p>
<pre><code>import bcrypt
pw = b"your-password-here"
print(bcrypt.hashpw(pw, bcrypt.gensalt()).decode())</code></pre>

<h3>4. Run the data pipeline</h3>
<pre><code>python script/run_pipeline.py</code></pre>

<p>This will:</p>
<ol>
<li>Scrape all source databases</li>
<li>Download and parse metadata</li>
<li>Score each dataset (0–100)</li>
<li>Write <code>output/metadata_master.csv</code>, <code>confidence_scores.csv</code>, and <code>datasets_registry.json</code></li>
</ol>

<h3>5. Launch the application</h3>
<pre><code>streamlit run script/app/main.py</code></pre>
<p>Opens at <code>http://localhost:8501</code>.</p>

<h3>Directory structure</h3>
<pre><code>Aim01_Database_Regeneration/
├── script/
│   ├── app/              ← Streamlit application
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
    └── secrets.toml      ← Auth credentials (never commit)</code></pre>

</div>
""", unsafe_allow_html=True)

# ===========================================================================
# Tab 2 — Confidence Score
# ===========================================================================
with tab2:
    st.markdown("""
<div class="prose">

<h2>Confidence Score System</h2>

<p>Each dataset receives a <strong>composite score from 0 to 100</strong> derived from five
independently weighted dimensions. The score determines the tier assignment.</p>

<h3>Tier Thresholds</h3>

<table>
<tr><th>Score Range</th><th>Tier</th><th>Interpretation</th></tr>
<tr><td>≥ 80</td><td><strong>GOLD</strong></td><td>High-priority; use without major caveats</td></tr>
<tr><td>60 – 79</td><td><strong>SILVER</strong></td><td>Good quality; minor limitations noted</td></tr>
<tr><td>40 – 59</td><td><strong>BRONZE</strong></td><td>Usable with caution; key limitations present</td></tr>
<tr><td>&lt; 40</td><td><strong>LOW CONFIDENCE</strong></td><td>Significant quality issues; use with care</td></tr>
</table>

<h3>Scoring Dimensions</h3>

<table>
<tr><th>Dimension</th><th>Abbrev.</th><th>Max pts</th><th>Description</th></tr>
<tr><td>Data Quality Score</td><td><strong>DQS</strong></td><td>25</td>
    <td>Raw data availability, sequencing depth, QC metrics</td></tr>
<tr><td>Temporal Resolution Score</td><td><strong>TRS</strong></td><td>25</td>
    <td>LH/P+x staging coverage across the menstrual cycle</td></tr>
<tr><td>Sample Representation Score</td><td><strong>SRS</strong></td><td>20</td>
    <td>Patient N, disease groups, demographic diversity</td></tr>
<tr><td>Methodological Completeness Score</td><td><strong>MCS</strong></td><td>20</td>
    <td>Protocols, antibodies, cell isolation, reproducibility</td></tr>
<tr><td>Data Accessibility Score</td><td><strong>DAS</strong></td><td>10</td>
    <td>Open access vs. controlled; download convenience</td></tr>
</table>

<h3>Formula</h3>
<pre><code>Total Score = DQS + TRS + SRS + MCS + DAS − Σ(penalties)</code></pre>

<p>Penalties are applied for:</p>
<ul>
<li>Missing metadata fields (per dimension)</li>
<li>No raw counts matrix (DQS −5)</li>
<li>Fewer than 3 LH timepoints (TRS −5)</li>
<li>N &lt; 5 patients (SRS −5)</li>
<li>No protocol/methods section (MCS −5)</li>
<li>Controlled access only with no alternative (DAS −3)</li>
</ul>

<h3>DQS sub-criteria (25 pts)</h3>
<table>
<tr><th>Sub-criterion</th><th>Points</th></tr>
<tr><td>Raw counts matrix available</td><td>8</td></tr>
<tr><td>QC metrics reported (% MT, doublets)</td><td>5</td></tr>
<tr><td>Sequencing depth ≥ 50,000 reads/cell</td><td>5</td></tr>
<tr><td>Platform documented</td><td>4</td></tr>
<tr><td>Cell Ranger / pipeline version stated</td><td>3</td></tr>
</table>

<h3>TRS sub-criteria (25 pts)</h3>
<table>
<tr><th>Sub-criterion</th><th>Points</th></tr>
<tr><td>LH or P+x staging documented</td><td>10</td></tr>
<tr><td>WOI window covered (LH+5–LH+9)</td><td>8</td></tr>
<tr><td>≥ 5 distinct timepoints</td><td>5</td></tr>
<tr><td>Proliferative and secretory both present</td><td>2</td></tr>
</table>

<h3>SRS sub-criteria (20 pts)</h3>
<table>
<tr><th>Sub-criterion</th><th>Points</th></tr>
<tr><td>N ≥ 20 patients</td><td>8</td></tr>
<tr><td>Includes disease group (RIF, endometriosis…)</td><td>5</td></tr>
<tr><td>Age and BMI reported</td><td>4</td></tr>
<tr><td>Ethnicity / reproductive history noted</td><td>3</td></tr>
</table>

<h3>MCS sub-criteria (20 pts)</h3>
<table>
<tr><th>Sub-criterion</th><th>Points</th></tr>
<tr><td>Cell isolation protocol described</td><td>7</td></tr>
<tr><td>Library preparation kit named</td><td>5</td></tr>
<tr><td>Antibody panel (spatial / CITE-seq)</td><td>4</td></tr>
<tr><td>Bioinformatics pipeline documented</td><td>4</td></tr>
</table>

<h3>DAS sub-criteria (10 pts)</h3>
<table>
<tr><th>Sub-criterion</th><th>Points</th></tr>
<tr><td>Fully open access</td><td>6</td></tr>
<tr><td>Direct download URL available</td><td>4</td></tr>
</table>

</div>
""", unsafe_allow_html=True)

# ===========================================================================
# Tab 3 — Data Dictionary
# ===========================================================================
with tab3:
    st.markdown("""
<div class="prose">
<h2>Data Dictionary — <code>metadata_master.csv</code></h2>
</div>
""", unsafe_allow_html=True)

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
        ("sub_compartments",   "str",   "Comma-separated tissue collection sites or cell populations"),
        ("disease_group",      "str",   "Disease/condition (e.g. Healthy, RIF, Endometriosis); may be comma-separated"),
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
            subset=["Column"],
            **{"font-family": "monospace", "font-size": "0.82rem"},
        ),
        use_container_width=True,
        hide_index=True,
    )

# ===========================================================================
# Tab 4 — Source Databases
# ===========================================================================
with tab4:
    st.markdown("""
<div class="prose">
<h2>Source Databases</h2>
</div>
""", unsafe_allow_html=True)

    db_table = [
        ("GEO",          "NCBI Gene Expression Omnibus",         "Open",       "GEO query API (Entrez)"),
        ("ArrayExpress", "EBI ArrayExpress / BioStudies",         "Open",       "BioStudies REST API"),
        ("CELLxGENE",    "CZ CELLxGENE Discover",                "Open",       "CELLxGENE REST API"),
        ("HCA",          "Human Cell Atlas Data Portal",          "Open",       "HCA Data Portal API"),
        ("SCP",          "Single Cell Portal (Broad Institute)",  "Open",       "SCP REST API"),
        ("Zenodo",       "Zenodo research data repository",       "Open",       "Zenodo REST API"),
        ("figshare",     "figshare open research platform",       "Open",       "figshare REST API"),
        ("dbGaP",        "Database of Genotypes & Phenotypes",    "Controlled", "Entrez eSearch"),
        ("EGA",          "European Genome-phenome Archive",       "Controlled", "EGA REST API"),
    ]
    db_df = pd.DataFrame(db_table, columns=["Database", "Full Name", "Access", "API Method"])

    def _style_access(val):
        if val == "Controlled":
            return "color:#E53935;font-weight:700"
        return "color:#2E7D32;font-weight:700"

    st.dataframe(
        db_df.style.applymap(_style_access, subset=["Access"]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("""
<div class="prose">

<h3>API Notes</h3>
<ul>
<li><strong>GEO</strong>: <code>Entrez.esearch(db="gds", term=query)</code> + <code>Entrez.esummary</code></li>
<li><strong>ArrayExpress</strong>: <code>GET https://www.ebi.ac.uk/biostudies/api/v1/search?query=…</code></li>
<li><strong>CELLxGENE</strong>: <code>GET https://api.cellxgene.cziscience.com/curation/v1/collections</code></li>
<li><strong>HCA</strong>: HCA Data Portal REST API with optional token authentication</li>
<li><strong>SCP</strong>: Single Cell Portal API (Broad Institute)</li>
<li><strong>Zenodo</strong>: <code>GET https://zenodo.org/api/records?q=…</code></li>
<li><strong>figshare</strong>: <code>GET https://api.figshare.com/v2/articles?search_for=…</code></li>
<li><strong>dbGaP</strong>: Metadata via Entrez; controlled data requires approved application + dbGaP key.</li>
<li><strong>EGA</strong>: Metadata via <code>GET https://ega-archive.org/metadata/v2/datasets</code>; access requires DAC approval.</li>
</ul>

</div>
""", unsafe_allow_html=True)

# ===========================================================================
# Tab 5 — Search Terms
# ===========================================================================
with tab5:
    st.markdown("""
<div class="prose">

<h2>Search Terms Used in Database Queries</h2>

<h3>Tissue / Anatomy Terms</h3>
<pre><code>endometrium
endometrial
uterine lining
uterine endometrium
endometrial stroma
endometrial epithelium
endometrial glands
decidua</code></pre>

<h3>Modality Terms</h3>
<pre><code>single-cell RNA-seq
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
CITE-seq</code></pre>

<h3>Temporal / Cycle Phase Terms</h3>
<pre><code>menstrual cycle
window of implantation
WOI
LH surge
luteal phase
proliferative phase
secretory phase
mid-secretory
peri-implantation
LH+0  LH+2  LH+5  LH+7  LH+9
P+x (progesterone-referenced staging)</code></pre>

<h3>Disease / Condition Terms</h3>
<pre><code>recurrent implantation failure  (RIF)
recurrent pregnancy loss        (RPL)
endometriosis
uterine fibroids
adenomyosis
polycystic ovary syndrome       (PCOS)
unexplained infertility
thin endometrium
hydrosalpinx</code></pre>

<h3>Exclusion Terms</h3>
<pre><code>endometrial cancer
endometrial carcinoma
uterine cancer
cervical cancer
ovarian cancer
mouse
murine
rat
in vitro
organoid  (unless paired with endometrial receptivity)</code></pre>

<h3>Boolean Query Template</h3>
<pre><code>(endometrium OR endometrial OR "uterine lining") AND
("single-cell" OR "scRNA-seq" OR "bulk RNA" OR "spatial transcriptomics" OR "proteomics") AND
("menstrual cycle" OR "implantation" OR "luteal" OR "secretory")
NOT ("cancer" OR "carcinoma" OR "mouse" OR "murine")</code></pre>

</div>
""", unsafe_allow_html=True)

# ===========================================================================
# Tab 6 — Controlled Access
# ===========================================================================
with tab6:
    st.markdown("""
<div class="prose">

<h2>Controlled Access Datasets</h2>

<p>Some datasets require a formal data access application before download.
These are marked <strong>Controlled Access</strong> in the Search and Download pages.</p>

<h3>dbGaP (Database of Genotypes and Phenotypes)</h3>
<p><em>Authority: NIH / NCBI</em></p>
<ol>
<li>Create an <a href="https://public.era.nih.gov/commons/">eRA Commons account</a>.</li>
<li>Navigate to <a href="https://www.ncbi.nlm.nih.gov/gap/">dbGaP</a> and search for the accession (e.g. <code>phs001234</code>).</li>
<li>Click <strong>Request Access</strong> and complete the Data Access Request (DAR) form.</li>
<li>Attach IRB approval or exemption letter and list all investigators.</li>
<li>Submit to the relevant Data Access Committee (DAC).</li>
<li>Approval typically takes <strong>4–8 weeks</strong>.</li>
<li>Once approved, download via:<br>
<pre><code>prefetch phs001234 --output-directory ./downloads/</code></pre></li>
</ol>

<h3>EGA (European Genome-phenome Archive)</h3>
<p><em>Authority: EMBL-EBI / CRG</em></p>
<ol>
<li>Register at <a href="https://ega-archive.org/">EGA</a>.</li>
<li>Search for the EGAD or EGAN accession and click <strong>Request Access</strong>.</li>
<li>Submit your data access agreement (DAA) signed by your institution.</li>
<li>Once approved, download using the EGA download client:<br>
<pre><code>pip install pyega3
pyega3 -cf credentials.json fetch EGAD00001000001</code></pre></li>
</ol>

<h3>Best Practices</h3>
<ul>
<li>Keep a copy of your approval letter — required for data transfer agreements.</li>
<li>Data should remain on institutional compute infrastructure.</li>
<li>Do <strong>not</strong> share controlled-access data with unapproved collaborators.</li>
<li>Cite the dataset and original study in any publication using these data.</li>
</ul>

</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    '<div style="margin-top:2rem;height:1px;background:#e5e7eb"></div>',
    unsafe_allow_html=True,
)
st.info(
    "For bugs, missing datasets, or scoring questions, contact the Hickey Lab "
    "(Duke University, Department of Cell Biology). "
    "Submit issues via the lab's internal GitHub repository.",
    icon="ℹ️",
)
