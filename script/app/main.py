"""
main.py
=======
Home page for the Hickey Lab Genomics Search Streamlit application.

Displays summary statistics from the pipeline output and provides
navigation to all sub-pages.
"""

import json
import sys
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_APP_DIR = Path(__file__).resolve().parent
_SCRIPT_DIR = _APP_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parent
_OUTPUT_DIR = _REPO_ROOT / "output"

sys.path.insert(0, str(_APP_DIR))
sys.path.insert(0, str(_SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Hickey Lab Genomics Search",
    layout="wide",
    page_icon="\U0001f52c",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .block-container { max-width: 1140px; }
    h1, h2, h3 { font-family: Arial, sans-serif; color: #012169; }
    .stat-card {
        background: #ffffff;
        border-top: 4px solid #00539B;
        border-radius: 8px;
        padding: 1.2rem 1rem;
        box-shadow: 0 1px 6px rgba(0,0,0,0.07);
        text-align: center;
    }
    .stat-card .label { color: #555; font-size: 0.85rem; margin-bottom: 0.2rem; }
    .stat-card .value { color: #012169; font-size: 2rem; font-weight: 700; }
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
# Sidebar navigation
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
# Load data
# ---------------------------------------------------------------------------


def _load_registry() -> list:
    """Load datasets_registry.json from the output directory."""
    path = _OUTPUT_DIR / "datasets_registry.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return []


def _load_scores() -> list:
    """Load confidence_scores.csv as a list of dicts."""
    path = _OUTPUT_DIR / "confidence_scores.csv"
    if path.exists():
        import csv
        with open(path, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            return list(reader)
    return []


registry = _load_registry()
scores = _load_scores()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Hickey Lab Genomics Search")
st.markdown(
    "Automated discovery, scoring, and download of open-access genomics "
    "datasets for endometrial receptivity and Window of Implantation research."
)

# ---------------------------------------------------------------------------
# Stats cards
# ---------------------------------------------------------------------------
total = len(registry)

tier_counts = {"GOLD": 0, "SILVER": 0, "BRONZE": 0, "LOW": 0}
modality_counts: dict[str, int] = {}

for rec in registry:
    tier = str(rec.get("confidence_tier", "")).upper()
    if tier in tier_counts:
        tier_counts[tier] += 1
    elif tier == "LOW_CONFIDENCE":
        tier_counts["LOW"] += 1

    mod = rec.get("modality", "Unknown") or "Unknown"
    modality_counts[mod] = modality_counts.get(mod, 0) + 1

# If registry has no tiers, try scores CSV
if total == 0 and scores:
    total = len(scores)
    for row in scores:
        tier = str(row.get("confidence_tier", "")).upper()
        if tier in tier_counts:
            tier_counts[tier] += 1
        elif tier == "LOW_CONFIDENCE":
            tier_counts["LOW"] += 1

cols = st.columns(5)
labels = ["Total Datasets", "GOLD", "SILVER", "BRONZE", "LOW"]
values = [total, tier_counts["GOLD"], tier_counts["SILVER"], tier_counts["BRONZE"], tier_counts["LOW"]]
colors = ["#012169", "#FFD700", "#C0C0C0", "#CD7F32", "#999999"]

for col, label, value, color in zip(cols, labels, values, colors):
    col.markdown(
        f'<div class="stat-card">'
        f'<div class="label">{label}</div>'
        f'<div class="value" style="color:{color}">{value}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Modality distribution chart
# ---------------------------------------------------------------------------
if modality_counts:
    st.subheader("Modality Distribution")
    import plotly.express as px

    mod_df_data = [{"Modality": k, "Count": v} for k, v in sorted(modality_counts.items(), key=lambda x: -x[1])]
    fig = px.bar(
        mod_df_data,
        x="Modality",
        y="Count",
        color="Modality",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        xaxis_title="",
        yaxis_title="Number of Datasets",
        font=dict(family="Arial, sans-serif"),
    )
    st.plotly_chart(fig, use_container_width=True)
elif total == 0:
    st.info("No pipeline output data found yet. Run the pipeline or use the Search Engine to discover datasets.")

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
