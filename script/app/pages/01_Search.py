"""
pages/01_Search.py
==================
Browse and filter saved pipeline datasets from datasets_registry.json
and metadata_master.csv.
"""

import json
import sys
from pathlib import Path

import pandas as pd
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
    page_title="Browse Datasets - Hickey Lab",
    layout="wide",
    page_icon="\U0001f52c",
)

st.markdown(
    """
    <style>
    .block-container { max-width: 1140px; }
    h1, h2, h3 { font-family: Arial, sans-serif; color: #012169; }
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

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def _load_data() -> pd.DataFrame:
    """Load registry JSON or metadata CSV into a DataFrame."""
    registry_path = _OUTPUT_DIR / "datasets_registry.json"
    csv_path = _OUTPUT_DIR / "metadata_master.csv"

    if registry_path.exists():
        with open(registry_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if data:
            return pd.DataFrame(data)

    if csv_path.exists():
        return pd.read_csv(csv_path)

    return pd.DataFrame()


df = _load_data()

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("Browse Datasets")

if df.empty:
    st.info("No dataset files found in the output directory. Run the pipeline first.")
    st.stop()

# Filters
col_search, col_mod, col_tier = st.columns([2, 1, 1])

with col_search:
    search_text = st.text_input("Search (accession, title, abstract)", "")

with col_mod:
    modalities = ["All"]
    if "modality" in df.columns:
        modalities += sorted(df["modality"].dropna().unique().tolist())
    selected_mod = st.selectbox("Modality", modalities)

with col_tier:
    tiers = ["All"]
    if "confidence_tier" in df.columns:
        tiers += sorted(df["confidence_tier"].dropna().unique().tolist())
    selected_tier = st.selectbox("Tier", tiers)

# Apply filters
filtered = df.copy()

if search_text:
    mask = pd.Series(False, index=filtered.index)
    for col in ["accession", "title", "abstract", "summary"]:
        if col in filtered.columns:
            mask = mask | filtered[col].astype(str).str.contains(search_text, case=False, na=False)
    filtered = filtered[mask]

if selected_mod != "All" and "modality" in filtered.columns:
    filtered = filtered[filtered["modality"] == selected_mod]

if selected_tier != "All" and "confidence_tier" in filtered.columns:
    filtered = filtered[filtered["confidence_tier"] == selected_tier]

st.caption(f"Showing {len(filtered)} of {len(df)} datasets")

# Pagination
PAGE_SIZE = 25
total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

start_idx = (page - 1) * PAGE_SIZE
end_idx = start_idx + PAGE_SIZE
page_df = filtered.iloc[start_idx:end_idx]

# Pick display columns
preferred_cols = [
    "accession", "title", "source_db", "modality", "confidence_tier",
    "final_CS", "confidence_score", "year", "platform", "n_cells",
    "n_samples", "n_patients",
]
display_cols = [c for c in preferred_cols if c in page_df.columns]
if not display_cols:
    display_cols = list(page_df.columns[:8])

st.dataframe(page_df[display_cols], use_container_width=True, hide_index=True)

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
