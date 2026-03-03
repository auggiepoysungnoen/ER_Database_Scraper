"""
Search & Filter — Endometrial Receptivity Database
Free-text search across title, accession, tissue, disease.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Search | Endometrial Receptivity DB",
    layout="wide",
    page_icon="🔍",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
APP_DIR         = Path(__file__).parent.parent
REPO_ROOT       = APP_DIR.parent.parent
OUTPUT_DIR      = REPO_ROOT / "output"
METADATA_PATH   = OUTPUT_DIR / "metadata_master.csv"
CONFIDENCE_PATH = OUTPUT_DIR / "confidence_scores.csv"
REGISTRY_PATH   = OUTPUT_DIR / "datasets_registry.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUKE_BLUE  = "#00539B"
DUKE_NAVY  = "#012169"
DUKE_GOLD  = "#B5A369"
DUKE_GREY  = "#6b7280"

MODALITY_OPTIONS = [
    "scRNA-seq",
    "bulkRNA-seq",
    "Spatial Transcriptomics",
    "Spatial Proteomics",
]
TIER_OPTIONS = ["GOLD", "SILVER", "BRONZE", "LOW_CONFIDENCE"]

TIER_BADGE = {
    "GOLD":           '<span style="background:#FEF3C7;color:#92710A;padding:1px 7px;border-radius:2px;font-size:0.72rem;font-weight:700;font-family:Arial,sans-serif">GOLD</span>',
    "SILVER":         '<span style="background:#F3F4F6;color:#6B7280;padding:1px 7px;border-radius:2px;font-size:0.72rem;font-weight:700;font-family:Arial,sans-serif">SILVER</span>',
    "BRONZE":         '<span style="background:#FEF0E6;color:#C05621;padding:1px 7px;border-radius:2px;font-size:0.72rem;font-weight:700;font-family:Arial,sans-serif">BRONZE</span>',
    "LOW_CONFIDENCE": '<span style="background:#F9FAFB;color:#9CA3AF;padding:1px 7px;border-radius:2px;font-size:0.72rem;font-weight:600;font-family:Arial,sans-serif">LOW</span>',
}

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.stApp,[data-testid="stAppViewContainer"]{background:#fff}
[data-testid="stHeader"]{background:#fff;border-bottom:1px solid #e5e7eb}
[data-testid="stSidebar"]{background:#f9fafb!important;border-right:1px solid #e5e7eb}
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1300px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.25rem 0!important}
.stButton>button{border-radius:2px;font-weight:500;font-family:Arial,sans-serif}
.stButton>button[kind="primary"]{background:#00539B;border:none}
[data-testid="stDataFrame"]{border:1px solid #e5e7eb;border-radius:2px}
.page-title{font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
            color:#012169;letter-spacing:-0.02em;margin-bottom:0}
.section-label{font-family:Arial,sans-serif;font-size:0.6rem;font-weight:700;
               letter-spacing:0.12em;text-transform:uppercase;color:#9ca3af;
               margin-bottom:0.7rem;padding-bottom:0.35rem;border-bottom:1px solid #e5e7eb}
.result-count{font-family:Arial,sans-serif;font-size:0.82rem;color:#6b7280}
.result-count strong{color:#012169}
/* Search bar */
[data-testid="stTextInput"]>div>div>input{
    border:1px solid #e5e7eb;border-radius:2px;
    font-family:Arial,sans-serif;font-size:0.9rem;
    padding:0.5rem 0.75rem;background:#fff;
}
[data-testid="stTextInput"]>div>div>input:focus{
    border-color:#00539B;box-shadow:0 0 0 2px rgba(0,83,155,0.08)
}
/* Sidebar filter labels */
.stSidebar label{font-family:Arial,sans-serif;font-size:0.82rem;color:#374151}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    meta_df = pd.DataFrame()
    conf_df = pd.DataFrame()
    if METADATA_PATH.exists():
        meta_df = pd.read_csv(METADATA_PATH)
    if CONFIDENCE_PATH.exists():
        conf_df = pd.read_csv(CONFIDENCE_PATH)
    if not meta_df.empty and not conf_df.empty:
        key_candidates = ["accession", "Accession", "dataset_id", "ID"]
        meta_key = next((c for c in key_candidates if c in meta_df.columns), None)
        conf_key = next((c for c in key_candidates if c in conf_df.columns), None)
        if meta_key and conf_key:
            meta_df = meta_df.merge(
                conf_df, left_on=meta_key, right_on=conf_key,
                how="left", suffixes=("", "_conf"),
            )
    return meta_df, conf_df

@st.cache_data(ttl=3600)
def load_registry() -> list:
    if not REGISTRY_PATH.exists():
        return []
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "datasets" in data:
        return data["datasets"]
    elif isinstance(data, dict):
        return list(data.values())
    return data

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
meta_df, conf_df = load_data()
registry_list    = load_registry()
files_missing    = meta_df.empty and not METADATA_PATH.exists()

# ── Header ──────────────────────────────────────────────────────────────────
st.markdown('<p class="page-title">Search Datasets</p>', unsafe_allow_html=True)
st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.9rem 0 1.25rem 0"></div>',
            unsafe_allow_html=True)

if files_missing:
    st.warning(
        "Output files not found. Run the pipeline from the **⚙ Run Pipeline** page.",
        icon="⚠️",
    )
    st.stop()

# Fall back to registry
if meta_df.empty and registry_list:
    meta_df = pd.DataFrame(registry_list)

if meta_df.empty:
    st.info("No dataset records found.")
    st.stop()

# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------
def _get(df: pd.DataFrame, *candidates) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
        cl = c.lower().replace(" ", "_")
        for col in df.columns:
            if col.lower().replace(" ", "_") == cl:
                return col
    return None

ACC_COL      = _get(meta_df, "accession", "Accession", "dataset_id", "ID")
TITLE_COL    = _get(meta_df, "title", "Title", "study_title")
MODALITY_COL = _get(meta_df, "modality", "Modality", "data_type")
PLATFORM_COL = _get(meta_df, "platform", "Platform", "sequencing_platform")
SCORE_COL    = _get(meta_df, "confidence_score", "score", "total_score", "Score")
TIER_COL     = _get(meta_df, "confidence_tier", "tier", "Tier", "Confidence_Tier")
PATIENTS_COL = _get(meta_df, "n_patients", "n_samples", "sample_count", "N_Patients")
CELLS_COL    = _get(meta_df, "n_cells", "cell_count", "N_Cells")
LH_COL       = _get(meta_df, "lh_timepoints", "timepoints", "cycle_phase", "LH_Timepoints")
SUBCOMP_COL  = _get(meta_df, "sub_compartments", "cell_types", "compartments", "Sub_Compartments")
DISEASE_COL  = _get(meta_df, "disease_group", "condition", "diagnosis", "Disease_Group")
YEAR_COL     = _get(meta_df, "year", "publication_year", "Year")
SOURCE_COL   = _get(meta_df, "source_db", "database", "source", "Source_DB")
RAW_COL      = _get(meta_df, "raw_data_available", "has_raw", "raw_available")
ABSTRACT_COL = _get(meta_df, "abstract", "Abstract", "summary")

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="font-family:Arial,sans-serif;padding:0.5rem 0 0.75rem 0">
        <div style="font-size:0.6rem;font-weight:700;letter-spacing:0.12em;
                    text-transform:uppercase;color:#9ca3af;margin-bottom:0.25rem">
            HICKEY LAB · DUKE UNIVERSITY
        </div>
        <div style="font-size:1rem;font-weight:700;color:#012169">
            Endometrial Receptivity DB
        </div>
        <div style="margin-top:0.75rem;height:1px;background:#e5e7eb"></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-label">Filters</div>', unsafe_allow_html=True)

    # Modality
    f_modality = st.multiselect(
        "Modality",
        options=MODALITY_OPTIONS,
        default=[],
        help="Leave empty to show all.",
    )

    # Confidence tier
    f_tier = st.multiselect(
        "Confidence Tier",
        options=TIER_OPTIONS,
        default=[],
        help="Leave empty to show all tiers.",
    )

    # LH timepoints
    lh_options = []
    if LH_COL:
        unique_lh: set = set()
        for val in meta_df[LH_COL].dropna().astype(str):
            for tp in val.split(","):
                tp = tp.strip()
                if tp and tp.lower() not in ("nan", "none", ""):
                    unique_lh.add(tp)
        lh_options = sorted(unique_lh)
    f_lh = st.multiselect("LH Timepoints", options=lh_options, default=[])

    # Tissue collection site (sub-compartments)
    subcomp_options = []
    if SUBCOMP_COL:
        unique_sc: set = set()
        for val in meta_df[SUBCOMP_COL].dropna().astype(str):
            for sc in val.split(","):
                sc = sc.strip()
                if sc and sc.lower() not in ("nan", "none", ""):
                    unique_sc.add(sc)
        subcomp_options = sorted(unique_sc)
    f_subcomp = st.multiselect(
        "Tissue Collection Site",
        options=subcomp_options,
        default=[],
        help="Sub-compartment = tissue/cell population from which data were collected.",
    )

    # Disease groups — split by comma or semicolon for individual options
    disease_options = []
    if DISEASE_COL:
        unique_diseases: set = set()
        for val in meta_df[DISEASE_COL].dropna().astype(str):
            for d in re.split(r"[;,]", val):
                d = d.strip()
                if d and d.lower() not in ("nan", "none", ""):
                    unique_diseases.add(d)
        disease_options = sorted(unique_diseases)
    f_disease = st.multiselect(
        "Disease / Condition",
        options=disease_options,
        default=[],
        help="Datasets may contain multiple disease groups per study.",
    )

    # Source database
    source_options = []
    if SOURCE_COL:
        source_options = sorted(meta_df[SOURCE_COL].dropna().unique().tolist())
    f_source = st.multiselect("Source Database", options=source_options, default=[])

    # Year range
    year_min, year_max = 2000, 2025
    if YEAR_COL:
        years = pd.to_numeric(meta_df[YEAR_COL], errors="coerce").dropna()
        if not years.empty:
            year_min = int(years.min())
            year_max = int(years.max())
    f_year = st.slider(
        "Publication Year",
        min_value=year_min,
        max_value=year_max,
        value=(year_min, year_max),
    )

    # Raw data only
    f_raw = st.checkbox("Raw data available only", value=False)

    st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.75rem 0"></div>',
                unsafe_allow_html=True)
    if st.button("Clear all filters", use_container_width=True):
        st.rerun()

# ---------------------------------------------------------------------------
# Free-text search bar (main panel)
# ---------------------------------------------------------------------------
search_cols_for_text = [c for c in [TITLE_COL, ACC_COL, DISEASE_COL,
                                     SUBCOMP_COL, ABSTRACT_COL] if c]

f_text = st.text_input(
    "search",
    placeholder="Search by title, accession, disease, tissue site, keyword…",
    label_visibility="collapsed",
)

# ---------------------------------------------------------------------------
# Filtering logic
# ---------------------------------------------------------------------------
mask = pd.Series([True] * len(meta_df), index=meta_df.index)

# Free-text
if f_text and f_text.strip():
    text_mask = pd.Series([False] * len(meta_df), index=meta_df.index)
    for col in search_cols_for_text:
        text_mask |= (
            meta_df[col].fillna("").astype(str)
            .str.contains(f_text.strip(), case=False, na=False, regex=False)
        )
    mask &= text_mask

if f_modality and MODALITY_COL:
    mask &= meta_df[MODALITY_COL].isin(f_modality)

if f_tier and TIER_COL:
    mask &= meta_df[TIER_COL].str.upper().isin([t.upper() for t in f_tier])

if f_lh and LH_COL:
    def _lh_match(val):
        val_str = str(val) if pd.notna(val) else ""
        tps = {t.strip() for t in val_str.split(",")}
        return bool(tps & set(f_lh))
    mask &= meta_df[LH_COL].apply(_lh_match)

if f_subcomp and SUBCOMP_COL:
    def _sc_match(val):
        val_str = str(val) if pd.notna(val) else ""
        scs = {s.strip() for s in val_str.split(",")}
        return bool(scs & set(f_subcomp))
    mask &= meta_df[SUBCOMP_COL].apply(_sc_match)

if f_disease and DISEASE_COL:
    def _disease_match(val):
        val_str = str(val) if pd.notna(val) else ""
        diseases = {d.strip() for d in re.split(r"[;,]", val_str)}
        return bool(diseases & set(f_disease))
    mask &= meta_df[DISEASE_COL].apply(_disease_match)

if f_source and SOURCE_COL:
    mask &= meta_df[SOURCE_COL].isin(f_source)

if YEAR_COL:
    years_num = pd.to_numeric(meta_df[YEAR_COL], errors="coerce")
    mask &= years_num.between(f_year[0], f_year[1], inclusive="both").fillna(False)

if f_raw and RAW_COL:
    raw_vals = meta_df[RAW_COL].astype(str).str.lower()
    mask &= raw_vals.isin(["true", "yes", "1"])

filtered_df = meta_df[mask].copy()

# ---------------------------------------------------------------------------
# Result count
# ---------------------------------------------------------------------------
st.markdown(
    f'<p class="result-count"><strong>{len(filtered_df):,}</strong> of '
    f'<strong>{len(meta_df):,}</strong> datasets</p>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Display columns
# ---------------------------------------------------------------------------
DISPLAY_COLS = [c for c in [
    ACC_COL, TITLE_COL, MODALITY_COL, PLATFORM_COL,
    SCORE_COL, TIER_COL, PATIENTS_COL, CELLS_COL,
    LH_COL, SUBCOMP_COL, DISEASE_COL, SOURCE_COL,
] if c is not None]

display_df = filtered_df[DISPLAY_COLS].copy() if DISPLAY_COLS else filtered_df.copy()

rename_map: dict[str, str] = {}
if ACC_COL:      rename_map[ACC_COL]      = "Accession"
if TITLE_COL:    rename_map[TITLE_COL]    = "Title"
if MODALITY_COL: rename_map[MODALITY_COL] = "Modality"
if PLATFORM_COL: rename_map[PLATFORM_COL] = "Platform"
if SCORE_COL:    rename_map[SCORE_COL]    = "Score"
if TIER_COL:     rename_map[TIER_COL]     = "Tier"
if PATIENTS_COL: rename_map[PATIENTS_COL] = "N Patients"
if CELLS_COL:    rename_map[CELLS_COL]    = "N Cells"
if LH_COL:       rename_map[LH_COL]       = "LH Timepoints"
if SUBCOMP_COL:  rename_map[SUBCOMP_COL]  = "Tissue Collection Site"
if DISEASE_COL:  rename_map[DISEASE_COL]  = "Disease / Condition"
if SOURCE_COL:   rename_map[SOURCE_COL]   = "Source DB"

display_df = display_df.rename(columns=rename_map)

# Tier color styling (subtle, table-compatible)
TIER_STYLE = {
    "GOLD":           "background-color:#FEF9EC;color:#92710A;font-weight:700",
    "SILVER":         "background-color:#F3F4F6;color:#6B7280;font-weight:700",
    "BRONZE":         "background-color:#FEF0E6;color:#C05621;font-weight:700",
    "LOW_CONFIDENCE": "background-color:#F9FAFB;color:#9CA3AF;font-weight:600",
}

def _style_tier(val):
    tier = str(val).upper() if pd.notna(val) else ""
    return TIER_STYLE.get(tier, "")

styled = display_df.style
if "Tier" in display_df.columns:
    styled = styled.applymap(_style_tier, subset=["Tier"])
if "Score" in display_df.columns:
    styled = styled.format({"Score": lambda x: f"{x:.1f}" if pd.notna(x) else "—"})
if "Accession" in display_df.columns:
    styled = styled.set_properties(
        subset=["Accession"], **{"font-family": "monospace", "font-size": "0.82rem"}
    )

st.dataframe(styled, use_container_width=True, hide_index=True, height=520)

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
st.markdown('<div style="margin-top:0.75rem"></div>', unsafe_allow_html=True)
col_exp, col_hint = st.columns([2, 5])
with col_exp:
    csv_bytes = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Export as CSV",
        data=csv_bytes,
        file_name="endometrial_receptivity_filtered.csv",
        mime="text/csv",
    )
with col_hint:
    st.markdown(
        '<p style="font-family:Arial,sans-serif;font-size:0.75rem;color:#9ca3af;'
        'padding-top:0.45rem">Click a row to view its accession, then open '
        '<strong>Dataset Detail</strong> for full metadata and score breakdown.</p>',
        unsafe_allow_html=True,
    )
