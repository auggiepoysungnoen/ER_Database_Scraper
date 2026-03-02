"""
Dataset Detail — Endometrial Receptivity Database
"""

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Dataset Detail | Endometrial Receptivity DB",
    layout="wide",
    page_icon="📄",
    initial_sidebar_state="collapsed",
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
REGISTRY_PATH = OUTPUT_DIR / "datasets_registry.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"
DUKE_GREY = "#666666"

SCORE_DIMENSIONS = [
    ("DQS", "Data Quality Score", 25),
    ("TRS", "Temporal Resolution Score", 25),
    ("SRS", "Sample Representation Score", 20),
    ("MCS", "Methodological Completeness Score", 20),
    ("DAS", "Data Accessibility Score", 10),
]

TIER_BADGE = {
    "GOLD":           ('<span style="background:#FFF8E1;color:#B5A369;padding:3px 10px;'
                       'border-radius:12px;font-weight:700;font-size:0.9rem;">GOLD</span>'),
    "SILVER":         ('<span style="background:#F5F5F5;color:#757575;padding:3px 10px;'
                       'border-radius:12px;font-weight:700;font-size:0.9rem;">SILVER</span>'),
    "BRONZE":         ('<span style="background:#FBE9E7;color:#CD7F32;padding:3px 10px;'
                       'border-radius:12px;font-weight:700;font-size:0.9rem;">BRONZE</span>'),
    "LOW_CONFIDENCE": ('<span style="background:#FAFAFA;color:#9E9E9E;padding:3px 10px;'
                       'border-radius:12px;font-weight:700;font-size:0.9rem;">LOW CONFIDENCE</span>'),
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_registry() -> dict:
    if not REGISTRY_PATH.exists():
        return {}
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # Normalise to {accession: record} dict
    if isinstance(raw, dict) and "datasets" in raw:
        items = raw["datasets"]
    elif isinstance(raw, list):
        items = raw
    else:
        items = list(raw.values())

    registry = {}
    for item in items:
        key = (
            item.get("accession")
            or item.get("Accession")
            or item.get("dataset_id")
            or item.get("ID")
            or ""
        )
        if key:
            registry[key.upper()] = item
    return registry


registry = load_registry()

# ---------------------------------------------------------------------------
# Resolve accession from query params or user input
# ---------------------------------------------------------------------------
params = st.query_params
accession_from_url = params.get("accession", "")

st.title("📄 Dataset Detail")

col_input, col_btn = st.columns([3, 1])
with col_input:
    accession_input = st.text_input(
        "Accession Number",
        value=accession_from_url,
        placeholder="e.g. GSE111976",
        label_visibility="collapsed",
    )
with col_btn:
    search_clicked = st.button("Load Dataset", type="primary", use_container_width=True)

accession = accession_input.strip().upper()

# Back-to-search link
st.markdown(
    '[← Back to Search](01_Search)',
    unsafe_allow_html=False,
)
st.divider()

# ---------------------------------------------------------------------------
# Lookup record
# ---------------------------------------------------------------------------
if not accession:
    st.info("Enter an accession number above to view dataset details.")
    st.stop()

if not registry:
    st.warning(
        "Registry not found. Run the pipeline first: `python script/run_pipeline.py`",
        icon="⚠️",
    )
    st.stop()

record = registry.get(accession)
if record is None:
    st.error(f"Accession **{accession}** not found in the registry.")
    st.stop()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
title = record.get("title") or record.get("Title") or record.get("study_title") or accession
tier = (record.get("confidence_tier") or record.get("tier") or "").upper()
badge_html = TIER_BADGE.get(tier, "")

st.markdown(
    f"""
    <h2 style="font-family:system-ui,sans-serif;color:{DUKE_NAVY};margin-bottom:0.2rem;">
        <code style="font-size:0.8em;color:{DUKE_GREY};">{accession}</code>&nbsp;&nbsp;{title}
    </h2>
    <div style="margin-bottom:1rem;">{badge_html}</div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Two-column layout: metadata | confidence breakdown
# ---------------------------------------------------------------------------
left_col, right_col = st.columns([1, 1], gap="large")

# ---------- Left: metadata table ----------
with left_col:
    st.markdown(f"#### Metadata")

    SKIP_KEYS = {
        "abstract", "summary", "aims", "methodology", "findings",
        "relevance", "paper_summary", "dqs_breakdown", "trs_breakdown",
        "srs_breakdown", "mcs_breakdown", "das_breakdown",
        "dqs_penalties", "trs_penalties", "srs_penalties",
        "mcs_penalties", "das_penalties",
    }

    meta_rows = []
    for k, v in record.items():
        if k.lower() in SKIP_KEYS:
            continue
        if v is None or v == "" or v == []:
            continue
        if isinstance(v, list):
            v = ", ".join(str(i) for i in v)
        elif isinstance(v, dict):
            continue
        meta_rows.append({"Field": k.replace("_", " ").title(), "Value": str(v)})

    if meta_rows:
        meta_table = pd.DataFrame(meta_rows)
        st.dataframe(
            meta_table.style.set_properties(
                subset=["Field"], **{"font-weight": "600", "color": DUKE_NAVY}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No metadata fields available.")

# ---------- Right: confidence score breakdown ----------
with right_col:
    st.markdown("#### Confidence Score Breakdown")

    total_score = record.get("confidence_score") or record.get("score") or record.get("total_score")

    # Build bar data from record
    bar_labels = []
    bar_values = []
    bar_colors = []
    penalty_labels = []
    penalty_values = []

    for abbr, full_name, max_pts in SCORE_DIMENSIONS:
        score_key_candidates = [
            abbr.lower(), abbr, f"{abbr.lower()}_score", f"{abbr}_score",
            abbr.lower() + "_raw",
        ]
        score_val = None
        for k in score_key_candidates:
            if k in record:
                score_val = record[k]
                break

        penalty_key_candidates = [
            f"{abbr.lower()}_penalty", f"{abbr}_penalty",
            f"{abbr.lower()}_penalties",
        ]
        penalty_val = None
        for k in penalty_key_candidates:
            if k in record:
                penalty_val = record[k]
                break

        bar_labels.append(f"{abbr}<br><sup>{full_name}</sup>")
        bar_values.append(float(score_val) if score_val is not None else 0.0)
        bar_colors.append(DUKE_BLUE)

        if penalty_val and float(penalty_val) != 0:
            penalty_labels.append(abbr)
            penalty_values.append(abs(float(penalty_val)))

    if any(v > 0 for v in bar_values):
        fig = go.Figure()

        # Positive score bars
        fig.add_trace(go.Bar(
            y=bar_labels,
            x=bar_values,
            orientation="h",
            name="Score",
            marker_color=DUKE_BLUE,
            text=[f"{v:.1f}" for v in bar_values],
            textposition="auto",
        ))

        # Penalty bars (negative, shown in red)
        if penalty_values:
            pen_y = []
            pen_x = []
            for abbr, _ in zip(penalty_labels, penalty_values):
                idx = next(
                    (i for i, lbl in enumerate(bar_labels) if lbl.startswith(abbr)), None
                )
                if idx is not None:
                    pen_y.append(bar_labels[idx])
                    pen_x.append(-penalty_values[penalty_labels.index(abbr)])

            fig.add_trace(go.Bar(
                y=pen_y,
                x=pen_x,
                orientation="h",
                name="Penalty",
                marker_color="#E53935",
                text=[f"{v:.1f}" for v in penalty_values],
                textposition="outside",
            ))

        fig.update_layout(
            barmode="relative",
            xaxis_title="Points",
            yaxis_title="",
            template="simple_white",
            height=320,
            margin=dict(l=10, r=20, t=30, b=20),
            font=dict(family="system-ui, sans-serif"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            title=dict(
                text=f"Total: {total_score:.1f} / 100" if total_score else "Score breakdown",
                font=dict(size=14, color=DUKE_NAVY),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        if total_score is not None:
            st.metric("Total Confidence Score", f"{float(total_score):.1f} / 100")
        else:
            st.info("Confidence score breakdown not available for this dataset.")

# ---------------------------------------------------------------------------
# Paper summary (collapsible)
# ---------------------------------------------------------------------------
st.divider()

summary_fields = {
    "Aim": record.get("aim") or record.get("aims") or record.get("Aim"),
    "Dataset": record.get("dataset") or record.get("Dataset"),
    "Methodology": record.get("methodology") or record.get("methods") or record.get("Methodology"),
    "Findings": record.get("findings") or record.get("results") or record.get("Findings"),
    "Relevance": record.get("relevance") or record.get("Relevance"),
    "Abstract": record.get("abstract") or record.get("Abstract") or record.get("summary"),
}

has_summary = any(v for v in summary_fields.values())

with st.expander("Paper Summary", expanded=has_summary):
    if has_summary:
        for field_name, content in summary_fields.items():
            if content:
                st.markdown(f"**{field_name}**")
                st.markdown(str(content))
                st.markdown("")
    else:
        st.info("No paper summary available for this dataset.")

# ---------------------------------------------------------------------------
# Download section
# ---------------------------------------------------------------------------
st.divider()
st.markdown("#### Download / Access")

download_url = record.get("download_url") or record.get("url") or record.get("ftp_url")
controlled = str(record.get("controlled_access") or record.get("access_type") or "").lower() in (
    "true", "yes", "controlled", "restricted", "1"
)

if controlled:
    st.warning(
        "This dataset requires controlled access. "
        "Request access via [dbGaP](https://www.ncbi.nlm.nih.gov/gap) or "
        "[EGA](https://ega-archive.org/) using the accession number above. "
        "See the Documentation page for step-by-step instructions.",
        icon="🔒",
    )
elif download_url:
    st.link_button("Download / View Dataset", url=download_url, type="primary")
else:
    db_url_map = {
        "GSE": f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}",
        "E-": f"https://www.ebi.ac.uk/biostudies/arrayexpress/studies/{accession}",
    }
    auto_url = next(
        (url for prefix, url in db_url_map.items() if accession.startswith(prefix)), None
    )
    if auto_url:
        st.link_button("View on Source Database", url=auto_url)
    else:
        st.info("No direct download link available. Search for this accession in the source database.")
