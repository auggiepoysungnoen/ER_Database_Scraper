"""
Dataset Detail — Endometrial Receptivity Database
"""

from __future__ import annotations

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
APP_DIR       = Path(__file__).parent.parent
REPO_ROOT     = APP_DIR.parent.parent
OUTPUT_DIR    = REPO_ROOT / "output"
REGISTRY_PATH = OUTPUT_DIR / "datasets_registry.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"
DUKE_GREY = "#6b7280"

SCORE_DIMENSIONS = [
    ("DQS", "Data Quality Score",                25),
    ("TRS", "Temporal Resolution Score",          25),
    ("SRS", "Sample Representation Score",        20),
    ("MCS", "Methodological Completeness Score",  20),
    ("DAS", "Data Accessibility Score",           10),
]

TIER_BADGE = {
    "GOLD": (
        '<span style="background:#FEF3C7;color:#92710A;padding:2px 10px;'
        'border-radius:2px;font-size:0.75rem;font-weight:700;'
        'font-family:Arial,sans-serif;letter-spacing:0.04em">GOLD</span>'
    ),
    "SILVER": (
        '<span style="background:#F3F4F6;color:#6B7280;padding:2px 10px;'
        'border-radius:2px;font-size:0.75rem;font-weight:700;'
        'font-family:Arial,sans-serif;letter-spacing:0.04em">SILVER</span>'
    ),
    "BRONZE": (
        '<span style="background:#FEF0E6;color:#C05621;padding:2px 10px;'
        'border-radius:2px;font-size:0.75rem;font-weight:700;'
        'font-family:Arial,sans-serif;letter-spacing:0.04em">BRONZE</span>'
    ),
    "LOW_CONFIDENCE": (
        '<span style="background:#F9FAFB;color:#9CA3AF;padding:2px 10px;'
        'border-radius:2px;font-size:0.75rem;font-weight:600;'
        'font-family:Arial,sans-serif;letter-spacing:0.04em">LOW CONFIDENCE</span>'
    ),
}

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1200px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.25rem 0!important}
.stButton>button{border-radius:2px;font-weight:500}
[data-testid="stDataFrame"]{border:1px solid #e5e7eb;border-radius:2px}
details{border:1px solid #e5e7eb!important;border-radius:2px!important;padding:0.1rem}
details summary{font-weight:600;font-size:0.88rem;color:#012169;padding:0.5rem 0}
.section-label{font-family:Arial,sans-serif;font-size:0.6rem;font-weight:700;
               letter-spacing:0.12em;text-transform:uppercase;color:#9ca3af;
               margin-bottom:0.75rem;padding-bottom:0.35rem;
               border-bottom:1px solid #e5e7eb}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_registry() -> dict:
    if not REGISTRY_PATH.exists():
        return {}
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "datasets" in raw:
        items = raw["datasets"]
    elif isinstance(raw, list):
        items = raw
    else:
        items = list(raw.values())
    registry: dict = {}
    for item in items:
        key = (
            item.get("accession") or item.get("Accession")
            or item.get("dataset_id") or item.get("ID") or ""
        )
        if key:
            registry[key.upper()] = item
    return registry

registry = load_registry()

# ---------------------------------------------------------------------------
# Header + accession input
# ---------------------------------------------------------------------------
params              = st.query_params
accession_from_url  = params.get("accession", "")

st.markdown("""
<p style="font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
          color:#012169;letter-spacing:-0.02em;margin-bottom:0">
    Dataset Detail
</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 1.25rem 0"></div>',
    unsafe_allow_html=True,
)

col_input, col_btn = st.columns([4, 1])
with col_input:
    accession_input = st.text_input(
        "Accession",
        value=accession_from_url,
        placeholder="e.g. GSE111976",
        label_visibility="collapsed",
    )
with col_btn:
    st.button("Load", type="primary", use_container_width=True)

accession = accession_input.strip().upper()

st.markdown(
    '<a href="01_Search" style="font-family:Arial,sans-serif;font-size:0.8rem;'
    'color:#6b7280;text-decoration:none">← Back to Search</a>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 1rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------
if not accession:
    st.info("Enter an accession number above to view dataset details.")
    st.stop()

if not registry:
    st.warning(
        "Registry not found. Run the pipeline from the **⚙ Run Pipeline** page.",
        icon="⚠️",
    )
    st.stop()

record = registry.get(accession)
if record is None:
    st.error(f"Accession **{accession}** not found in the registry.")
    st.stop()

# ---------------------------------------------------------------------------
# Live Gemini enrichment (if record not yet AI-enriched)
# ---------------------------------------------------------------------------
ai_badge_html = ""
if not record.get("ai_enriched"):
    try:
        gemini_key = st.secrets.get("gemini", {}).get("api_key", "")
        if gemini_key:
            import sys as _sys
            _sys.path.insert(0, str(APP_DIR.parent))
            from scoring.ai_extractor import enrich_record_live
            with st.spinner("Enriching with Gemini AI…"):
                record = enrich_record_live(record, gemini_key)
            if record.get("ai_enriched"):
                ai_badge_html = (
                    '<span style="background:#EFF6FF;color:#1D4ED8;padding:2px 8px;'
                    'border-radius:2px;font-size:0.72rem;font-weight:600;'
                    'font-family:Arial,sans-serif;letter-spacing:0.03em">AI ENRICHED</span>'
                )
    except Exception:
        pass  # silently skip if Gemini unavailable
elif record.get("ai_enriched"):
    ai_badge_html = (
        '<span style="background:#EFF6FF;color:#1D4ED8;padding:2px 8px;'
        'border-radius:2px;font-size:0.72rem;font-weight:600;'
        'font-family:Arial,sans-serif;letter-spacing:0.03em">AI ENRICHED</span>'
    )

# ---------------------------------------------------------------------------
# Dataset header
# ---------------------------------------------------------------------------
title = (
    record.get("title") or record.get("Title")
    or record.get("study_title") or accession
)
tier      = (record.get("confidence_tier") or record.get("tier") or "").upper()
badge_html = TIER_BADGE.get(tier, "")

st.markdown(
    f"""
    <div style="margin-bottom:0.25rem">
        <span style="font-family:monospace;font-size:0.8rem;color:#6b7280;
                     background:#f3f4f6;padding:2px 6px;border-radius:2px">{accession}</span>
        &nbsp; {badge_html}
        {("&nbsp; " + ai_badge_html) if ai_badge_html else ""}
    </div>
    <h2 style="font-family:Arial,sans-serif;font-size:1.25rem;font-weight:700;
               color:#012169;margin:0.4rem 0 1rem 0;line-height:1.3">{title}</h2>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Two-column layout: metadata | confidence score
# ---------------------------------------------------------------------------
left_col, right_col = st.columns([1, 1], gap="large")

# ── Left: metadata table ──────────────────────────────────────────────────
with left_col:
    st.markdown('<div class="section-label">Metadata</div>', unsafe_allow_html=True)

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
        meta_rows.append({
            "Field": k.replace("_", " ").title(),
            "Value": str(v),
        })

    if meta_rows:
        meta_table = pd.DataFrame(meta_rows)
        st.dataframe(
            meta_table.style.set_properties(
                subset=["Field"],
                **{"font-weight": "600", "color": DUKE_NAVY,
                   "font-family": "Arial, sans-serif", "font-size": "0.82rem"},
            ).set_properties(
                subset=["Value"],
                **{"font-family": "Arial, sans-serif", "font-size": "0.82rem"},
            ),
            use_container_width=True,
            hide_index=True,
            height=420,
        )
    else:
        st.info("No metadata fields available.")

# ── Right: confidence score breakdown ────────────────────────────────────
with right_col:
    st.markdown('<div class="section-label">Confidence Score Breakdown</div>',
                unsafe_allow_html=True)

    total_score = (
        record.get("confidence_score")
        or record.get("score")
        or record.get("total_score")
    )

    bar_labels, bar_values, bar_colors = [], [], []
    penalty_labels, penalty_values     = [], []

    for abbr, full_name, max_pts in SCORE_DIMENSIONS:
        score_val = None
        for k in [abbr.lower(), abbr, f"{abbr.lower()}_score"]:
            if k in record:
                score_val = record[k]
                break

        penalty_val = None
        for k in [f"{abbr.lower()}_penalty", f"{abbr}_penalty",
                  f"{abbr.lower()}_penalties"]:
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

        fig.add_trace(go.Bar(
            y=bar_labels, x=bar_values, orientation="h",
            name="Score", marker_color=DUKE_BLUE,
            text=[f"{v:.1f}" for v in bar_values], textposition="auto",
            textfont=dict(size=10, family="Arial, Helvetica, sans-serif"),
        ))

        if penalty_values:
            pen_y, pen_x = [], []
            for abbr_p, _ in zip(penalty_labels, penalty_values):
                idx = next(
                    (i for i, lbl in enumerate(bar_labels)
                     if lbl.startswith(abbr_p)), None,
                )
                if idx is not None:
                    pen_y.append(bar_labels[idx])
                    pen_x.append(-penalty_values[penalty_labels.index(abbr_p)])
            fig.add_trace(go.Bar(
                y=pen_y, x=pen_x, orientation="h",
                name="Penalty", marker_color="#EF4444",
                text=[f"{v:.1f}" for v in penalty_values],
                textposition="outside",
                textfont=dict(size=10, family="Arial, Helvetica, sans-serif"),
            ))

        title_text = (
            f"Total: {float(total_score):.1f} / 100"
            if total_score else "Score breakdown"
        )

        fig.update_layout(
            barmode="relative",
            xaxis=dict(
                title="Points",
                showgrid=True, gridcolor="#f0f0f0", gridwidth=0.5,
                linecolor="#444", linewidth=0.8,
                ticks="outside", ticklen=3,
                title_font=dict(size=11),
            ),
            yaxis=dict(linecolor="#444", linewidth=0.8),
            template="simple_white",
            plot_bgcolor="white", paper_bgcolor="white",
            height=340,
            margin=dict(l=10, r=20, t=40, b=30),
            font=dict(family="Arial, Helvetica, sans-serif", size=10, color="#1a1a2e"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        font=dict(size=10)),
            title=dict(
                text=title_text,
                font=dict(size=12, color=DUKE_NAVY,
                          family="Arial, Helvetica, sans-serif"),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        if total_score is not None:
            st.metric("Total Confidence Score", f"{float(total_score):.1f} / 100")
        else:
            st.info("Score breakdown not available for this dataset.")

# ---------------------------------------------------------------------------
# Paper summary
# ---------------------------------------------------------------------------
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 1rem 0"></div>',
    unsafe_allow_html=True,
)

summary_fields = {
    "Aim":         record.get("aim") or record.get("aims") or record.get("Aim"),
    "Dataset":     record.get("dataset") or record.get("Dataset"),
    "Methodology": record.get("methodology") or record.get("methods"),
    "Findings":    record.get("findings") or record.get("results"),
    "Relevance":   record.get("relevance") or record.get("Relevance"),
    "Abstract":    record.get("abstract") or record.get("Abstract")
                   or record.get("summary"),
}
has_summary = any(v for v in summary_fields.values())

with st.expander("Paper Summary", expanded=has_summary):
    if has_summary:
        for field_name, content in summary_fields.items():
            if content:
                st.markdown(
                    f'<div style="font-family:Arial,sans-serif;font-size:0.8rem;'
                    f'font-weight:700;color:#012169;margin:0.75rem 0 0.2rem 0">'
                    f'{field_name}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div style="font-family:Arial,sans-serif;font-size:0.85rem;'
                    f'color:#374151;line-height:1.65">{str(content)}</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.info("No paper summary available for this dataset.")

# ---------------------------------------------------------------------------
# Download / Access
# ---------------------------------------------------------------------------
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1rem 0 0.75rem 0"></div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="section-label">Download / Access</div>',
    unsafe_allow_html=True,
)

download_url = (
    record.get("download_url") or record.get("url") or record.get("ftp_url")
)
controlled = str(
    record.get("controlled_access") or record.get("access_type") or ""
).lower() in ("true", "yes", "controlled", "restricted", "1")

if controlled:
    st.warning(
        "This dataset requires controlled access. "
        "Request access via [dbGaP](https://www.ncbi.nlm.nih.gov/gap) or "
        "[EGA](https://ega-archive.org/) using the accession number above. "
        "See the **Documentation** page for step-by-step instructions.",
        icon="🔒",
    )
elif download_url:
    st.link_button("Download / View Dataset", url=download_url, type="primary")
else:
    db_url_map = {
        "GSE": f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}",
        "E-":  f"https://www.ebi.ac.uk/biostudies/arrayexpress/studies/{accession}",
    }
    auto_url = next(
        (url for prefix, url in db_url_map.items()
         if accession.startswith(prefix)), None,
    )
    if auto_url:
        st.link_button("View on Source Database", url=auto_url)
    else:
        st.info(
            "No direct download link available. "
            "Search for this accession in the source database."
        )
