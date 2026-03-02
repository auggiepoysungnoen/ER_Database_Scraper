"""
Search & Filter — Endometrial Receptivity Database
"""

import json
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
# Auth guard
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
APP_DIR = Path(__file__).parent.parent
REPO_ROOT = APP_DIR.parent.parent
OUTPUT_DIR = REPO_ROOT / "output"
METADATA_PATH = OUTPUT_DIR / "metadata_master.csv"
CONFIDENCE_PATH = OUTPUT_DIR / "confidence_scores.csv"
REGISTRY_PATH = OUTPUT_DIR / "datasets_registry.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"
DUKE_GREY = "#666666"

MODALITY_OPTIONS = [
    "scRNA-seq",
    "bulkRNA-seq",
    "Spatial Transcriptomics",
    "Spatial Proteomics",
]
TIER_OPTIONS = ["GOLD", "SILVER", "BRONZE", "LOW_CONFIDENCE"]

TIER_COLORS = {
    "GOLD": "background-color: #FFF8E1; color: #B5A369; font-weight: bold;",
    "SILVER": "background-color: #F5F5F5; color: #757575; font-weight: bold;",
    "BRONZE": "background-color: #FBE9E7; color: #CD7F32; font-weight: bold;",
    "LOW_CONFIDENCE": "background-color: #FAFAFA; color: #9E9E9E;",
}

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

    # Merge confidence scores into metadata if both available
    if not meta_df.empty and not conf_df.empty:
        key_candidates = ["accession", "Accession", "dataset_id", "ID"]
        meta_key = next((c for c in key_candidates if c in meta_df.columns), None)
        conf_key = next((c for c in key_candidates if c in conf_df.columns), None)
        if meta_key and conf_key:
            meta_df = meta_df.merge(
                conf_df, left_on=meta_key, right_on=conf_key, how="left", suffixes=("", "_conf")
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
registry_list = load_registry()

files_missing = meta_df.empty and not METADATA_PATH.exists()

st.title("🔍 Search Datasets")

if files_missing:
    st.warning(
        "Output files not found. Run the pipeline first: `python script/run_pipeline.py`",
        icon="⚠️",
    )
    st.stop()

# Fall back to registry if metadata CSV is missing
if meta_df.empty and registry_list:
    meta_df = pd.DataFrame(registry_list)

if meta_df.empty:
    st.info("No dataset records found.")
    st.stop()

# ---------------------------------------------------------------------------
# Normalise column names (flexible — works with varied pipeline outputs)
# ---------------------------------------------------------------------------
col_map = {}
for col in meta_df.columns:
    cl = col.lower().replace(" ", "_")
    col_map[cl] = col

def _get(df: pd.DataFrame, *candidates) -> str | None:
    """Return first matching column name (case-insensitive)."""
    for c in candidates:
        if c in df.columns:
            return c
        cl = c.lower().replace(" ", "_")
        for col in df.columns:
            if col.lower().replace(" ", "_") == cl:
                return col
    return None


ACC_COL = _get(meta_df, "accession", "Accession", "dataset_id", "ID")
TITLE_COL = _get(meta_df, "title", "Title", "study_title")
MODALITY_COL = _get(meta_df, "modality", "Modality", "data_type")
PLATFORM_COL = _get(meta_df, "platform", "Platform", "sequencing_platform")
SCORE_COL = _get(meta_df, "confidence_score", "score", "total_score", "Score")
TIER_COL = _get(meta_df, "confidence_tier", "tier", "Tier", "Confidence_Tier")
PATIENTS_COL = _get(meta_df, "n_patients", "n_samples", "sample_count", "N_Patients")
CELLS_COL = _get(meta_df, "n_cells", "cell_count", "N_Cells")
LH_COL = _get(meta_df, "lh_timepoints", "timepoints", "cycle_phase", "LH_Timepoints")
SUBCOMP_COL = _get(meta_df, "sub_compartments", "cell_types", "compartments", "Sub_Compartments")
DISEASE_COL = _get(meta_df, "disease_group", "condition", "diagnosis", "Disease_Group")
YEAR_COL = _get(meta_df, "year", "publication_year", "Year")
SOURCE_COL = _get(meta_df, "source_db", "database", "source", "Source_DB")
RAW_COL = _get(meta_df, "raw_data_available", "has_raw", "raw_available")

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        f'<p style="color:{DUKE_BLUE};font-weight:700;font-size:1rem;">Filters</p>',
        unsafe_allow_html=True,
    )

    # Modality
    f_modality = st.multiselect(
        "Modality",
        options=MODALITY_OPTIONS,
        default=[],
        help="Leave empty to show all modalities.",
    )

    # Confidence tier
    f_tier = st.multiselect(
        "Confidence Tier",
        options=TIER_OPTIONS,
        default=[],
        help="Leave empty to show all tiers.",
    )

    # LH timepoints — dynamic
    lh_options = []
    if LH_COL:
        raw_lh = meta_df[LH_COL].dropna().astype(str)
        unique_lh = set()
        for val in raw_lh:
            for tp in val.split(","):
                tp = tp.strip()
                if tp:
                    unique_lh.add(tp)
        lh_options = sorted(unique_lh)
    f_lh = st.multiselect("LH Timepoints", options=lh_options, default=[])

    # Sub-compartments — dynamic
    subcomp_options = []
    if SUBCOMP_COL:
        raw_sc = meta_df[SUBCOMP_COL].dropna().astype(str)
        unique_sc = set()
        for val in raw_sc:
            for sc in val.split(","):
                sc = sc.strip()
                if sc:
                    unique_sc.add(sc)
        subcomp_options = sorted(unique_sc)
    f_subcomp = st.multiselect("Sub-Compartments", options=subcomp_options, default=[])

    # Disease groups — dynamic
    disease_options = []
    if DISEASE_COL:
        disease_options = sorted(meta_df[DISEASE_COL].dropna().unique().tolist())
    f_disease = st.multiselect("Disease Groups", options=disease_options, default=[])

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

    st.divider()
    if st.button("Clear all filters", use_container_width=True):
        st.rerun()

# ---------------------------------------------------------------------------
# Filtering logic (AND across dimensions; empty = no filter)
# ---------------------------------------------------------------------------
mask = pd.Series([True] * len(meta_df), index=meta_df.index)

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
    mask &= meta_df[DISEASE_COL].isin(f_disease)

if YEAR_COL:
    years_num = pd.to_numeric(meta_df[YEAR_COL], errors="coerce")
    mask &= years_num.between(f_year[0], f_year[1], inclusive="both").fillna(False)

if f_raw and RAW_COL:
    raw_vals = meta_df[RAW_COL].astype(str).str.lower()
    mask &= raw_vals.isin(["true", "yes", "1"])

filtered_df = meta_df[mask].copy()

# ---------------------------------------------------------------------------
# Display columns selection
# ---------------------------------------------------------------------------
DISPLAY_COLS = [c for c in [
    ACC_COL, TITLE_COL, MODALITY_COL, PLATFORM_COL,
    SCORE_COL, TIER_COL, PATIENTS_COL, CELLS_COL,
    LH_COL, SOURCE_COL,
] if c is not None]

display_df = filtered_df[DISPLAY_COLS].copy() if DISPLAY_COLS else filtered_df.copy()

# Rename for display
rename = {}
if ACC_COL:       rename[ACC_COL]       = "Accession"
if TITLE_COL:     rename[TITLE_COL]     = "Title"
if MODALITY_COL:  rename[MODALITY_COL]  = "Modality"
if PLATFORM_COL:  rename[PLATFORM_COL]  = "Platform"
if SCORE_COL:     rename[SCORE_COL]     = "Confidence Score"
if TIER_COL:      rename[TIER_COL]      = "Tier"
if PATIENTS_COL:  rename[PATIENTS_COL]  = "N Patients"
if CELLS_COL:     rename[CELLS_COL]     = "N Cells"
if LH_COL:        rename[LH_COL]        = "LH Timepoints"
if SOURCE_COL:    rename[SOURCE_COL]    = "Source DB"
display_df = display_df.rename(columns=rename)

# ---------------------------------------------------------------------------
# Tier color styling
# ---------------------------------------------------------------------------
def _style_tier(val):
    tier = str(val).upper() if pd.notna(val) else ""
    return TIER_COLORS.get(tier, "")

# ---------------------------------------------------------------------------
# Main panel
# ---------------------------------------------------------------------------
st.markdown(
    f"**Showing {len(filtered_df):,} of {len(meta_df):,} datasets**",
    unsafe_allow_html=False,
)

# Apply styling
styled = display_df.style
if "Tier" in display_df.columns:
    styled = styled.applymap(_style_tier, subset=["Tier"])
if "Confidence Score" in display_df.columns:
    styled = styled.format({"Confidence Score": lambda x: f"{x:.1f}" if pd.notna(x) else "—"})
if "Accession" in display_df.columns:
    styled = styled.set_properties(subset=["Accession"], **{"font-family": "monospace"})

st.dataframe(styled, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Export button
# ---------------------------------------------------------------------------
csv_bytes = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Export filtered list as CSV",
    data=csv_bytes,
    file_name="endometrial_receptivity_filtered.csv",
    mime="text/csv",
    use_container_width=False,
)
