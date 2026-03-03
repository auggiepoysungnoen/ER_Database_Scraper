"""
Hickey Lab Endometrial Receptivity Database — Home
Run with: streamlit run script/app/main.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from auth import check_password

# ---------------------------------------------------------------------------
# Page config (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Open-Access Genomics Search | Hickey Lab",
    layout="wide",
    page_icon="🔬",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
APP_DIR        = Path(__file__).parent
REPO_ROOT      = APP_DIR.parent.parent
OUTPUT_DIR     = REPO_ROOT / "output"
REGISTRY_PATH  = OUTPUT_DIR / "datasets_registry.json"
METADATA_PATH  = OUTPUT_DIR / "metadata_master.csv"
CONFIDENCE_PATH= OUTPUT_DIR / "confidence_scores.csv"

# ---------------------------------------------------------------------------
# Brand
# ---------------------------------------------------------------------------
DUKE_BLUE  = "#00539B"
DUKE_NAVY  = "#012169"
DUKE_GOLD  = "#B5A369"
DUKE_GREY  = "#6b7280"

MODALITY_COLORS = {
    "scRNA-seq":               "#00539B",
    "bulkRNA-seq":             "#B5A369",
    "Spatial Transcriptomics": "#2E7D32",
    "Spatial Proteomics":      "#E65100",
}

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Global CSS — white, clean, academic
# ---------------------------------------------------------------------------
st.markdown("""
<style>
/* ── Layout ── */
.block-container{padding-top:2.5rem;padding-bottom:3rem;max-width:1140px}
/* ── Dividers ── */
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.5rem 0!important}
/* ── Buttons ── */
.stButton>button{border-radius:2px;font-weight:500;letter-spacing:0.01em}
/* ── Tables ── */
[data-testid="stDataFrame"]{border:1px solid #e5e7eb;border-radius:2px}
/* ── Cards ── */
.metric-card{
    background:#fff;border:1px solid #e5e7eb;
    border-top:3px solid #00539B;border-radius:2px;
    padding:1.2rem 1rem;font-family:Arial,sans-serif;
}
.metric-num{font-size:2.2rem;font-weight:700;color:#012169;line-height:1}
.metric-lbl{font-size:0.7rem;color:#6b7280;margin-top:0.4rem;
            letter-spacing:0.06em;text-transform:uppercase}
.tier-gold  {border-top-color:#B5A369!important}
.tier-silver{border-top-color:#9E9E9E!important}
.tier-bronze{border-top-color:#CD7F32!important}
.tier-low   {border-top-color:#d1d5db!important}
/* ── Section headings ── */
.section-label{
    font-family:Arial,sans-serif;font-size:0.65rem;font-weight:700;
    letter-spacing:0.1em;text-transform:uppercase;color:#6b7280;
    margin-bottom:0.9rem;padding-bottom:0.4rem;border-bottom:1px solid #e5e7eb;
}
/* ── Page title ── */
.page-title{
    font-family:Arial,sans-serif;font-size:2rem;font-weight:700;
    color:#012169;letter-spacing:-0.02em;margin-bottom:0.1rem;line-height:1.15;
}
.page-sub{
    font-family:Arial,sans-serif;font-size:0.9rem;
    color:#6b7280;margin:0 0 0 0;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="font-family:Arial,sans-serif;padding:0.5rem 0 1rem 0">
        <div style="font-size:0.6rem;font-weight:700;letter-spacing:0.14em;
                    text-transform:uppercase;color:#9ca3af;margin-bottom:0.35rem">
            HICKEY LAB · DUKE UNIVERSITY
        </div>
        <div style="font-size:1rem;font-weight:700;color:#012169;line-height:1.25">
            Genomics Dataset<br>Search Engine
        </div>
        <div style="margin-top:0.85rem;height:1px;background:#e5e7eb"></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size:0.6rem;font-weight:700;letter-spacing:0.12em;
                text-transform:uppercase;color:#9ca3af;margin-bottom:0.5rem">
        Navigation
    </div>
    """, unsafe_allow_html=True)

    st.page_link("main.py",                    label="Home",            icon="·")
    st.page_link("pages/00_Search_Engine.py",  label="Search Engine",   icon="·")
    st.page_link("pages/01_Search.py",         label="Search Datasets", icon="·")
    st.page_link("pages/02_Dataset_Detail.py", label="Dataset Detail",  icon="·")
    st.page_link("pages/03_Download.py",       label="Downloads",       icon="·")
    st.page_link("pages/04_Statistics.py",     label="Statistics",      icon="·")
    st.page_link("pages/05_Documentation.py",  label="Documentation",   icon="·")

    st.markdown('<div style="margin-top:1rem;height:1px;background:#e5e7eb"></div>',
                unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.72rem;color:#9ca3af;font-family:Arial,sans-serif;
                line-height:1.6;margin-top:0.75rem">
        Real-time search across GEO, ArrayExpress, CELLxGENE, HCA, Zenodo,
        and figshare. Gemini AI enriches results and scores them on a
        0–100 composite confidence scale.
    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_output_data():
    meta_df = None
    conf_df = None
    if METADATA_PATH.exists():
        meta_df = pd.read_csv(METADATA_PATH)
    if CONFIDENCE_PATH.exists():
        conf_df = pd.read_csv(CONFIDENCE_PATH)
    return meta_df, conf_df

@st.cache_data(ttl=3600)
def load_registry() -> dict:
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

meta_df, conf_df  = load_output_data()
registry          = load_registry()
output_missing    = not METADATA_PATH.exists() or not CONFIDENCE_PATH.exists()

# ---------------------------------------------------------------------------
# Compute counts
# ---------------------------------------------------------------------------
if registry:
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
    gold_n, silver_n, bronze_n, low_n = (
        _count_tier("GOLD"), _count_tier("SILVER"),
        _count_tier("BRONZE"), _count_tier("LOW_CONFIDENCE"),
    )
elif meta_df is not None:
    total    = len(meta_df)
    tier_col = next((c for c in meta_df.columns if "tier" in c.lower()), None)
    if tier_col:
        gold_n   = int((meta_df[tier_col].str.upper() == "GOLD").sum())
        silver_n = int((meta_df[tier_col].str.upper() == "SILVER").sum())
        bronze_n = int((meta_df[tier_col].str.upper() == "BRONZE").sum())
        low_n    = int((meta_df[tier_col].str.upper() == "LOW_CONFIDENCE").sum())
    else:
        gold_n = silver_n = bronze_n = low_n = 0
else:
    total = gold_n = silver_n = bronze_n = low_n = 0

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown("""
<p class="page-title">Open-Access Genomics Dataset Search</p>
<p class="page-sub">Hickey Lab &nbsp;·&nbsp; Duke University &nbsp;·&nbsp; Search, score, and export any open-access multi-omic dataset</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1rem 0 1.75rem 0"></div>',
    unsafe_allow_html=True,
)

if output_missing:
    st.warning(
        "Output files not found. Run the pipeline from the **⚙ Run Pipeline** page.",
        icon="⚠️",
    )

# ---------------------------------------------------------------------------
# Stat cards
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Dataset Summary</div>', unsafe_allow_html=True)

c1, c2, c3, c4, c5 = st.columns(5)

def _card(col, num, lbl, extra_cls=""):
    col.markdown(
        f"""<div class="metric-card {extra_cls}">
            <div class="metric-num">{num:,}</div>
            <div class="metric-lbl">{lbl}</div>
        </div>""",
        unsafe_allow_html=True,
    )

_card(c1, total,    "Total Datasets")
_card(c2, gold_n,   "Gold",            "tier-gold")
_card(c3, silver_n, "Silver",          "tier-silver")
_card(c4, bronze_n, "Bronze",          "tier-bronze")
_card(c5, low_n,    "Low Confidence",  "tier-low")

st.markdown('<div style="margin-bottom:2rem"></div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# About + scoring dimensions
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">About This Resource</div>', unsafe_allow_html=True)

col_txt, col_score = st.columns([3, 2], gap="large")

with col_txt:
    st.markdown("""
    <div style="font-family:Arial,sans-serif;font-size:0.875rem;color:#374151;line-height:1.75">
    <p>This is an open-access search engine for multi-omic genomics datasets spanning any tissue
    type, disease group, or experimental modality. Datasets are automatically catalogued from
    major public repositories and enriched with AI-extracted metadata to power precise filtering
    and reproducible downstream analyses.</p>
    <p>Datasets are indexed from GEO, ArrayExpress, CELLxGENE, HCA, Single Cell Portal, Zenodo,
    and figshare. Each dataset is independently scored on a
    <strong>0–100 composite confidence scale</strong> across five quality dimensions.
    Gemini AI extraction enriches records with timepoints, tissue sites, disease groups,
    and protocol flags directly from abstracts.</p>
    <p style="margin-bottom:0">Use the <strong>Search</strong> page to filter by modality, tissue collection
    site, disease group, or free-text query. Use the <strong>Download Manager</strong>
    to generate batch wget or Python scripts for GOLD and SILVER datasets.</p>
    </div>
    """, unsafe_allow_html=True)

with col_score:
    st.markdown("""
    <div style="font-family:Arial,sans-serif">
    <div style="font-size:0.6rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;
                color:#9ca3af;margin-bottom:0.85rem">
        Confidence Score Dimensions
    </div>
    """, unsafe_allow_html=True)

    dims = [
        ("DQS", "Data Quality Score",                  25),
        ("TRS", "Temporal Resolution Score",            25),
        ("SRS", "Sample Representation Score",          20),
        ("MCS", "Methodological Completeness Score",    20),
        ("DAS", "Data Accessibility Score",             10),
    ]
    for abbr, name, pts in dims:
        st.markdown(f"""
        <div style="margin-bottom:0.7rem">
            <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px">
                <span style="font-size:0.8rem;font-weight:700;color:#012169">{abbr}</span>
                <span style="font-size:0.7rem;color:#9ca3af">{pts} pts</span>
            </div>
            <div style="font-size:0.7rem;color:#6b7280;margin-bottom:4px">{name}</div>
            <div style="height:3px;background:#e5e7eb;border-radius:1px">
                <div style="height:3px;background:#00539B;border-radius:1px;width:{pts}%"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Modality distribution chart (Nature style)
# ---------------------------------------------------------------------------
st.markdown('<div style="margin-top:1.75rem"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-label">Modality Distribution</div>', unsafe_allow_html=True)

if meta_df is not None and not meta_df.empty:
    mod_col = next((c for c in meta_df.columns if "modality" in c.lower()), None)
    if mod_col:
        mod_counts = meta_df[mod_col].value_counts().reset_index()
        mod_counts.columns = ["Modality", "Count"]

        fig = px.bar(
            mod_counts, x="Modality", y="Count",
            color="Modality",
            color_discrete_map=MODALITY_COLORS,
        )
        fig.update_layout(
            template="simple_white",
            showlegend=False,
            font=dict(family="Arial, Helvetica, sans-serif", size=11, color="#1a1a2e"),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, linecolor="#333", linewidth=0.8,
                       ticks="outside", ticklen=4, tickwidth=0.8, tickcolor="#333"),
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0", gridwidth=0.5,
                       linecolor="#333", linewidth=0.8, ticks="outside", ticklen=4,
                       tickwidth=0.8, tickcolor="#333", title="Count"),
            margin=dict(l=55, r=20, t=15, b=45),
            height=250,
        )
        st.plotly_chart(fig, use_container_width=True)
elif not output_missing:
    st.info("No metadata loaded. Check the output directory.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    '<div style="margin-top:2.5rem;height:1px;background:#e5e7eb"></div>',
    unsafe_allow_html=True,
)
st.markdown("""
<div style="font-family:Arial,sans-serif;font-size:0.7rem;color:#9ca3af;
            text-align:center;padding:0.85rem 0;line-height:1.8">
    <strong style="color:#6b7280">Hickey Lab</strong> · Department of Biomedical Engineering · Duke University<br>
    Built by Koravit (Auggie) Poysungnoen · Department of Biological Sciences &amp; Department of Financial Economics
</div>
""", unsafe_allow_html=True)
