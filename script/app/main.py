"""
Hickey Lab Endometrial Receptivity Database — Streamlit entry point.
Run with: streamlit run script/app/main.py
"""

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from auth import check_password

# ---------------------------------------------------------------------------
# Page configuration (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Hickey Lab | Endometrial Receptivity DB",
    layout="wide",
    page_icon="🔬",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Paths — relative to this file's location
# ---------------------------------------------------------------------------
APP_DIR = Path(__file__).parent
REPO_ROOT = APP_DIR.parent.parent          # Aim01_Database_Regeneration/
OUTPUT_DIR = REPO_ROOT / "output"
REGISTRY_PATH = OUTPUT_DIR / "datasets_registry.json"
METADATA_PATH = OUTPUT_DIR / "metadata_master.csv"
CONFIDENCE_PATH = OUTPUT_DIR / "confidence_scores.csv"

# ---------------------------------------------------------------------------
# Duke brand constants
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"
DUKE_GREY = "#666666"

# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------
if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown(
    f"""
    <style>
    /* Sidebar branding */
    .sidebar-logo {{
        font-family: system-ui, sans-serif;
        font-size: 1.05rem;
        font-weight: 700;
        color: {DUKE_BLUE};
        letter-spacing: 0.03em;
        line-height: 1.3;
    }}
    .sidebar-sub {{
        font-family: system-ui, sans-serif;
        font-size: 0.78rem;
        color: {DUKE_GREY};
        margin-top: 0.15rem;
    }}
    .sidebar-divider {{
        border-top: 2px solid {DUKE_GOLD};
        margin: 0.6rem 0 1rem 0;
    }}
    /* Stat cards */
    .stat-card {{
        background: #F5F7FA;
        border-left: 4px solid {DUKE_BLUE};
        border-radius: 6px;
        padding: 0.9rem 1.1rem;
        font-family: system-ui, sans-serif;
    }}
    .stat-number {{
        font-size: 2rem;
        font-weight: 700;
        color: {DUKE_NAVY};
    }}
    .stat-label {{
        font-size: 0.82rem;
        color: {DUKE_GREY};
        margin-top: 0.1rem;
    }}
    /* Gold tier highlight */
    .tier-gold {{ color: {DUKE_GOLD}; font-weight: 700; }}
    .tier-silver {{ color: #9E9E9E; font-weight: 700; }}
    .tier-bronze {{ color: #CD7F32; font-weight: 700; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-logo">HICKEY LAB</div>
        <div class="sidebar-sub">Duke University</div>
        <div class="sidebar-divider"></div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("**Navigation**")
    st.page_link("main.py", label="Home", icon="🏠")
    st.page_link("pages/01_Search.py", label="Search Datasets", icon="🔍")
    st.page_link("pages/02_Dataset_Detail.py", label="Dataset Detail", icon="📄")
    st.page_link("pages/03_Download.py", label="Download Manager", icon="⬇️")
    st.page_link("pages/04_Statistics.py", label="Statistics Dashboard", icon="📊")
    st.page_link("pages/05_Documentation.py", label="Documentation", icon="📚")

    st.markdown("---")
    st.markdown(
        """
        <div style="font-family:system-ui,sans-serif; font-size:0.78rem; color:#666;">
        <strong>About</strong><br>
        This database catalogues single-cell, bulk, and spatial transcriptomic/proteomic
        studies of human endometrial receptivity. Datasets are scored on a 0–100
        confidence scale across five quality dimensions.
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_output_data():
    """Load metadata_master.csv and confidence_scores.csv from output/."""
    meta_df = None
    conf_df = None

    if METADATA_PATH.exists():
        meta_df = pd.read_csv(METADATA_PATH)
    if CONFIDENCE_PATH.exists():
        conf_df = pd.read_csv(CONFIDENCE_PATH)

    return meta_df, conf_df


@st.cache_data(ttl=3600)
def load_registry() -> dict:
    """Load datasets_registry.json."""
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


meta_df, conf_df = load_output_data()
registry = load_registry()

# ---------------------------------------------------------------------------
# Missing-data warning banner
# ---------------------------------------------------------------------------
output_missing = not METADATA_PATH.exists() or not CONFIDENCE_PATH.exists()
if output_missing:
    st.warning(
        "Output files not found. Run the pipeline first: `python script/run_pipeline.py`",
        icon="⚠️",
    )

# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
st.markdown(
    f"""
    <h1 style="font-family:system-ui,sans-serif; color:{DUKE_NAVY}; margin-bottom:0.2rem;">
        Endometrial Receptivity Database
    </h1>
    <p style="font-family:system-ui,sans-serif; color:{DUKE_GREY}; font-size:1.05rem; margin-top:0;">
        Hickey Lab &mdash; Duke University &nbsp;|&nbsp; Aim 01: Database Regeneration
    </p>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    f'<hr style="border-top:3px solid {DUKE_GOLD}; margin-bottom:1.5rem;">',
    unsafe_allow_html=True,
)

# Project description
st.markdown(
    """
    ### Project Overview

    This resource aggregates multi-omic datasets profiling the human endometrium across the
    menstrual cycle, with particular focus on the **Window of Implantation (WOI)** — the
    narrow peri-implantation window (LH+5 to LH+9) during which the endometrium becomes
    receptive to embryo implantation.

    Datasets are curated from nine public repositories (GEO, ArrayExpress, ENCODE, GTEx,
    HCA, dbGaP, EGA, CELLxGENE, Zenodo) and scored on a **0–100 confidence scale**
    across five quality dimensions:

    | Dimension | Abbrev | Weight |
    |-----------|--------|--------|
    | Data Quality Score | DQS | 25 pts |
    | Temporal Resolution Score | TRS | 25 pts |
    | Sample Representation Score | SRS | 20 pts |
    | Methodological Completeness Score | MCS | 20 pts |
    | Data Accessibility Score | DAS | 10 pts |

    Datasets scoring ≥80 are **GOLD**, 60–79 **SILVER**, 40–59 **BRONZE**, and <40
    **LOW_CONFIDENCE**.
    """,
    unsafe_allow_html=True,
)

st.divider()

# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------
st.markdown("### Dataset Summary")

# Compute counts
if registry:
    datasets = registry.get("datasets", registry) if isinstance(registry, dict) else []
    if isinstance(registry, dict) and "datasets" in registry:
        ds_list = registry["datasets"]
    elif isinstance(registry, dict):
        ds_list = list(registry.values())
    else:
        ds_list = registry

    total = len(ds_list)

    def _count_tier(tier: str) -> int:
        return sum(
            1 for d in ds_list
            if (d.get("confidence_tier") or d.get("tier") or "").upper() == tier
        )

    gold_n = _count_tier("GOLD")
    silver_n = _count_tier("SILVER")
    bronze_n = _count_tier("BRONZE")
    low_n = _count_tier("LOW_CONFIDENCE")
elif meta_df is not None:
    total = len(meta_df)
    tier_col = next((c for c in meta_df.columns if "tier" in c.lower()), None)
    if tier_col:
        gold_n = int((meta_df[tier_col].str.upper() == "GOLD").sum())
        silver_n = int((meta_df[tier_col].str.upper() == "SILVER").sum())
        bronze_n = int((meta_df[tier_col].str.upper() == "BRONZE").sum())
        low_n = int((meta_df[tier_col].str.upper() == "LOW_CONFIDENCE").sum())
    else:
        gold_n = silver_n = bronze_n = low_n = 0
else:
    total = gold_n = silver_n = bronze_n = low_n = 0

col1, col2, col3, col4, col5 = st.columns(5)

def _stat_card(col, number, label, border_color=DUKE_BLUE):
    col.markdown(
        f"""
        <div class="stat-card" style="border-left-color:{border_color};">
            <div class="stat-number">{number}</div>
            <div class="stat-label">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

_stat_card(col1, total, "Total Datasets")
_stat_card(col2, gold_n, "GOLD Datasets", DUKE_GOLD)
_stat_card(col3, silver_n, "SILVER Datasets", "#9E9E9E")
_stat_card(col4, bronze_n, "BRONZE Datasets", "#CD7F32")
_stat_card(col5, low_n, "Low Confidence", DUKE_GREY)

st.divider()

# ---------------------------------------------------------------------------
# Modality breakdown (if data available)
# ---------------------------------------------------------------------------
if meta_df is not None and not meta_df.empty:
    st.markdown("### Modality Breakdown")
    mod_col = next((c for c in meta_df.columns if "modality" in c.lower()), None)
    if mod_col:
        mod_counts = meta_df[mod_col].value_counts().reset_index()
        mod_counts.columns = ["Modality", "Count"]

        import plotly.express as px

        MODALITY_COLORS = {
            "scRNA-seq": DUKE_BLUE,
            "bulkRNA-seq": DUKE_GOLD,
            "Spatial Transcriptomics": DUKE_NAVY,
            "Spatial Proteomics": "#4A90D9",
        }
        fig = px.bar(
            mod_counts,
            x="Modality",
            y="Count",
            color="Modality",
            color_discrete_map=MODALITY_COLORS,
            template="simple_white",
        )
        fig.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20, l=20, r=20),
            font=dict(family="system-ui, sans-serif"),
        )
        st.plotly_chart(fig, use_container_width=True)
elif not output_missing:
    st.info("No metadata loaded. Check output directory.")

st.divider()
st.markdown(
    f"""
    <p style="font-family:system-ui,sans-serif; font-size:0.8rem; color:{DUKE_GREY}; text-align:center;">
    Hickey Lab &mdash; Duke University &nbsp;&bull;&nbsp; Endometrial Receptivity Aim 01
    </p>
    """,
    unsafe_allow_html=True,
)
