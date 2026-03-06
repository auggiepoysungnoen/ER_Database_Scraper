"""
pages/02_Dataset_Detail.py
==========================
Detailed view of a single dataset record.

Accepts an accession via query parameter ``?acc=GSE12345`` or a selectbox.
Optionally enriches the record live with Gemini AI.
"""

import json
import sys
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_APP_DIR = Path(__file__).resolve().parent.parent
_SCRIPT_DIR = _APP_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parent
_OUTPUT_DIR = _REPO_ROOT / "output"

sys.path.insert(0, str(_APP_DIR))
sys.path.insert(0, str(_SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Dataset Detail - Hickey Lab",
    layout="wide",
    page_icon="\U0001f52c",
)

st.markdown(
    """
    <style>
    .block-container { max-width: 1140px; }
    h1, h2, h3 { font-family: Arial, sans-serif; color: #012169; }
    .detail-card {
        background: #fff;
        border-top: 4px solid #00539B;
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 6px rgba(0,0,0,0.07);
    }
    .footer-text {
        text-align: center; color: #888; font-size: 0.8rem;
        margin-top: 3rem; padding-top: 1rem;
        border-top: 1px solid #eee;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Navigation")
st.sidebar.page_link("main.py", label="Home")
st.sidebar.page_link("pages/00_Search_Engine.py", label="Search Engine")
st.sidebar.page_link("pages/01_Search.py", label="Browse Datasets")
st.sidebar.page_link("pages/02_Dataset_Detail.py", label="Dataset Detail")
st.sidebar.page_link("pages/03_Download.py", label="Downloads")
st.sidebar.page_link("pages/04_Statistics.py", label="Statistics")
st.sidebar.page_link("pages/05_Documentation.py", label="Documentation")

# ---------------------------------------------------------------------------
# Load registry
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def _load_registry() -> list:
    path = _OUTPUT_DIR / "datasets_registry.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return []


registry = _load_registry()

st.title("Dataset Detail")

if not registry:
    st.info("No datasets found. Run the pipeline or use the Search Engine first.")
    st.stop()

# ---------------------------------------------------------------------------
# Select dataset
# ---------------------------------------------------------------------------
accession_list = [r.get("accession", "unknown") for r in registry if r.get("accession")]

# Check query params
query_acc = st.query_params.get("acc", "")

if query_acc and query_acc in accession_list:
    default_idx = accession_list.index(query_acc)
else:
    default_idx = 0

selected_acc = st.selectbox("Select dataset", accession_list, index=default_idx)

# Find the record
record = None
for r in registry:
    if r.get("accession") == selected_acc:
        record = dict(r)
        break

if record is None:
    st.error("Dataset not found.")
    st.stop()

# ---------------------------------------------------------------------------
# AI enrichment
# ---------------------------------------------------------------------------
gemini_key = ""
try:
    gemini_key = st.secrets["gemini"]["api_key"]
except Exception:
    pass

if gemini_key and not record.get("ai_enriched"):
    if st.button("Enrich with Gemini AI"):
        with st.spinner("Running AI enrichment..."):
            try:
                from scoring.ai_extractor import enrich_record_live
                record = enrich_record_live(record, gemini_key)
                st.success("AI enrichment complete.")
            except Exception as exc:
                st.warning(f"AI enrichment failed: {exc}")

# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
st.markdown('<div class="detail-card">', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"**Accession:** {record.get('accession', 'N/A')}")
    st.markdown(f"**Source DB:** {record.get('source_db', 'N/A')}")
    st.markdown(f"**Modality:** {record.get('modality', 'Unknown')}")
    st.markdown(f"**Platform:** {record.get('platform', 'N/A')}")
    st.markdown(f"**Year:** {record.get('year', 'N/A')}")
    st.markdown(f"**Organism:** {record.get('organism', 'N/A')}")

with col2:
    tier = record.get("confidence_tier", "N/A")
    score = record.get("final_CS", record.get("confidence_score", "N/A"))
    st.markdown(f"**Confidence Tier:** {tier}")
    st.markdown(f"**Confidence Score:** {score}")
    st.markdown(f"**N Patients:** {record.get('n_patients', 'N/A')}")
    st.markdown(f"**N Samples:** {record.get('n_samples', 'N/A')}")
    st.markdown(f"**N Cells:** {record.get('n_cells', 'N/A')}")
    controlled = record.get("controlled_access", False)
    access_str = "Controlled" if controlled else "Open"
    st.markdown(f"**Access:** {access_str}")

st.markdown("</div>", unsafe_allow_html=True)

# Title and abstract
st.subheader("Title")
st.write(record.get("title", "No title available"))

st.subheader("Abstract")
abstract_text = record.get("abstract", "") or record.get("summary", "") or "No abstract available."
st.write(abstract_text)

# Lists
lh_tps = record.get("lh_timepoints", [])
if lh_tps:
    st.subheader("LH Timepoints")
    st.write(", ".join(str(t) for t in lh_tps))

sub_comp = record.get("sub_compartments", []) or record.get("tissue_sites", [])
if sub_comp:
    st.subheader("Sub-compartments / Tissue Sites")
    st.write(", ".join(str(s) for s in sub_comp))

disease = record.get("disease_groups", [])
if disease:
    st.subheader("Disease Groups")
    st.write(", ".join(str(d) for d in disease))

cycle = record.get("cycle_phases", [])
if cycle:
    st.subheader("Cycle Phases")
    st.write(", ".join(str(c) for c in cycle))

# DOI and links
doi = record.get("doi", "")
if doi:
    doi_url = doi if doi.startswith("http") else f"https://doi.org/{doi}"
    st.markdown(f"**DOI:** [{doi}]({doi_url})")

dl_url = record.get("download_url", "")
if dl_url:
    st.markdown(f"**Download URL:** [{dl_url}]({dl_url})")

# Full JSON
with st.expander("Raw JSON record"):
    st.json(record)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    '<div class="footer-text">'
    "Hickey Lab &middot; Department of Biomedical Engineering &middot; Duke University"
    " | Built by Koravit (Auggie) Poysungnoen &middot; "
    "Department of Biological Sciences &amp; Department of Financial Economics"
    "</div>",
    unsafe_allow_html=True,
)
