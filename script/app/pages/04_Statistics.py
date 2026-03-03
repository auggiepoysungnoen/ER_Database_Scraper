"""
Statistics — Endometrial Receptivity Database
Nature publication-style interactive figures.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Statistics | Endometrial Receptivity DB",
    layout="wide",
    page_icon="📊",
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
APP_DIR         = Path(__file__).parent.parent
REPO_ROOT       = APP_DIR.parent.parent
OUTPUT_DIR      = REPO_ROOT / "output"
METADATA_PATH   = OUTPUT_DIR / "metadata_master.csv"
CONFIDENCE_PATH = OUTPUT_DIR / "confidence_scores.csv"

# ---------------------------------------------------------------------------
# Design constants — Nature publication palette
# ---------------------------------------------------------------------------
DUKE_BLUE  = "#00539B"
DUKE_NAVY  = "#012169"
DUKE_GOLD  = "#B5A369"
DUKE_GREY  = "#6b7280"

# Nature-inspired modality palette
MODALITY_COLORS = {
    "scRNA-seq":               "#00539B",
    "bulkRNA-seq":             "#B5A369",
    "Spatial Transcriptomics": "#2E7D32",
    "Spatial Proteomics":      "#E65100",
}
TIER_COLORS = {
    "GOLD":           "#B5A369",
    "SILVER":         "#9E9E9E",
    "BRONZE":         "#CD7F32",
    "LOW_CONFIDENCE": "#D1D5DB",
}

# Nature plot base — minimal axes, clean white background
def _nl(h=380, **kw):
    """Return a Nature-style Plotly layout dict."""
    base = dict(
        template="simple_white",
        font=dict(family="Arial, Helvetica, sans-serif", size=11, color="#1a1a2e"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            showgrid=False, linecolor="#444", linewidth=0.8,
            ticks="outside", ticklen=4, tickwidth=0.8, tickcolor="#444",
            title_font=dict(size=11),
        ),
        yaxis=dict(
            showgrid=True, gridcolor="#f0f0f0", gridwidth=0.5,
            linecolor="#444", linewidth=0.8,
            ticks="outside", ticklen=4, tickwidth=0.8, tickcolor="#444",
            title_font=dict(size=11),
        ),
        margin=dict(l=60, r=20, t=40, b=60),
        height=h,
    )
    base.update(kw)
    return base

# Section heading helper
def _heading(text: str):
    st.markdown(
        f'<div style="font-family:Arial,sans-serif;font-size:0.85rem;font-weight:700;'
        f'color:#012169;margin:1.75rem 0 0.2rem 0">{text}</div>',
        unsafe_allow_html=True,
    )

def _caption(text: str):
    st.markdown(
        f'<div style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af;'
        f'margin-bottom:0.5rem">{text}</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1200px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1rem 0!important}
.page-title{font-size:1.65rem;font-weight:700;color:#012169;
            letter-spacing:-0.02em;margin-bottom:0}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    meta = pd.DataFrame()
    conf = pd.DataFrame()
    if METADATA_PATH.exists():
        meta = pd.read_csv(METADATA_PATH)
    if CONFIDENCE_PATH.exists():
        conf = pd.read_csv(CONFIDENCE_PATH)
    return meta, conf

meta_df, conf_df = load_data()

# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------
st.markdown('<p class="page-title">Statistics</p>', unsafe_allow_html=True)
st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 0.25rem 0"></div>',
            unsafe_allow_html=True)

if meta_df.empty and not METADATA_PATH.exists():
    st.warning(
        "Output files not found. Run the pipeline from the **⚙ Run Pipeline** page.",
        icon="⚠️",
    )
    st.stop()

if meta_df.empty:
    st.info("Metadata file is empty.")
    st.stop()

# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------
def _col(df, *candidates):
    for c in candidates:
        if c in df.columns:
            return c
        for col in df.columns:
            if col.lower().replace(" ", "_") == c.lower().replace(" ", "_"):
                return col
    return None

MODALITY_COL = _col(meta_df, "modality", "Modality", "data_type")
LH_COL       = _col(meta_df, "lh_timepoints", "timepoints", "cycle_phase", "LH_Timepoints")
SUBCOMP_COL  = _col(meta_df, "sub_compartments", "cell_types", "Sub_Compartments")
DISEASE_COL  = _col(meta_df, "disease_group", "condition", "Disease_Group")
PATIENTS_COL = _col(meta_df, "n_patients", "n_samples", "N_Patients")
CELLS_COL    = _col(meta_df, "n_cells", "cell_count", "N_Cells")
YEAR_COL     = _col(meta_df, "year", "publication_year", "Year")
TIER_COL     = _col(meta_df, "confidence_tier", "tier", "Tier")
SCORE_COL    = _col(meta_df, "confidence_score", "score", "total_score", "Score")
ACC_COL      = _col(meta_df, "accession", "Accession", "dataset_id")
TITLE_COL    = _col(meta_df, "title", "Title", "study_title")
AUTHORS_COL  = _col(meta_df, "authors", "Authors", "author")
ABSTRACT_COL = _col(meta_df, "abstract", "Abstract", "summary")
AGE_COL      = _col(meta_df, "age", "mean_age", "Age")
BMI_COL      = _col(meta_df, "bmi", "mean_bmi", "BMI")

# Merge confidence scores if separate
if not conf_df.empty and SCORE_COL is None:
    conf_acc = _col(conf_df, "accession", "Accession", "dataset_id")
    meta_acc = ACC_COL
    if conf_acc and meta_acc:
        meta_df = meta_df.merge(
            conf_df, left_on=meta_acc, right_on=conf_acc,
            how="left", suffixes=("", "_c"),
        )
        SCORE_COL = _col(meta_df, "confidence_score", "score", "total_score")
        TIER_COL  = _col(meta_df, "confidence_tier", "tier", "Tier")

# ---------------------------------------------------------------------------
# FIGURE 1 — Dataset count by LH timepoint (stacked bar, full width)
# ---------------------------------------------------------------------------
_heading("Dataset Coverage by LH Timepoint")
_caption("Window of Implantation (WOI) highlighted in gold. Bars coloured by sequencing modality.")

if LH_COL and MODALITY_COL:
    lh_rows = []
    for _, row in meta_df.iterrows():
        lh_raw   = str(row[LH_COL]) if pd.notna(row[LH_COL]) else ""
        modality = row[MODALITY_COL] if pd.notna(row.get(MODALITY_COL)) else "Unknown"
        for tp in lh_raw.split(","):
            tp = tp.strip()
            if tp and tp.lower() not in ("nan", "none", ""):
                lh_rows.append({"LH Timepoint": tp, "Modality": modality})

    if lh_rows:
        lh_df = pd.DataFrame(lh_rows)
        lh_counts = (
            lh_df.groupby(["LH Timepoint", "Modality"])
            .size().reset_index(name="Count")
        )

        import re as _re
        def _lh_key(tp: str) -> float:
            m = _re.search(r"([+-]?\d+)", tp)
            return float(m.group(1)) if m else 999.0

        tp_order = sorted(lh_counts["LH Timepoint"].unique(), key=_lh_key)

        fig1 = px.bar(
            lh_counts,
            x="LH Timepoint", y="Count",
            color="Modality",
            color_discrete_map=MODALITY_COLORS,
            category_orders={"LH Timepoint": tp_order},
            barmode="stack",
        )

        # WOI shaded region
        woi_start = tp_order.index("LH+5") if "LH+5" in tp_order else None
        woi_end   = tp_order.index("LH+9") if "LH+9" in tp_order else None
        if woi_start is not None and woi_end is not None:
            fig1.add_vrect(
                x0=woi_start - 0.5, x1=woi_end + 0.5,
                fillcolor=DUKE_GOLD, opacity=0.12, line_width=0,
                annotation_text="Window of Implantation",
                annotation_position="top left",
                annotation_font=dict(color=DUKE_NAVY, size=10,
                                     family="Arial, Helvetica, sans-serif"),
            )

        fig1.update_layout(**_nl(h=320, margin=dict(l=55, r=20, t=40, b=55)))
        fig1.update_layout(
            yaxis_title="Dataset count",
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        font=dict(size=10)),
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No LH timepoint data available.")
else:
    st.info("LH timepoint or modality column not found in metadata.")

st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 0"></div>',
            unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# FIGURE 2 — Sub-compartment coverage heatmap (full width)
# ---------------------------------------------------------------------------
_heading("Tissue Collection Site Coverage")
_caption(
    "Binary presence/absence matrix for the top 30 highest-scoring datasets. "
    "Blue = present, white = absent."
)

if SUBCOMP_COL and ACC_COL:
    plot_df = meta_df.copy()
    if SCORE_COL:
        plot_df[SCORE_COL] = pd.to_numeric(plot_df[SCORE_COL], errors="coerce")
        plot_df = plot_df.nlargest(30, SCORE_COL)
    else:
        plot_df = plot_df.head(30)

    all_sites: set = set()
    for val in plot_df[SUBCOMP_COL].dropna().astype(str):
        for ct in val.split(","):
            ct = ct.strip()
            if ct and ct.lower() not in ("nan", "none", ""):
                all_sites.add(ct)

    site_list  = sorted(all_sites)
    accessions = plot_df[ACC_COL].fillna("").astype(str).tolist()

    z_matrix = []
    for _, row in plot_df.iterrows():
        raw     = str(row[SUBCOMP_COL]) if pd.notna(row[SUBCOMP_COL]) else ""
        present = {c.strip() for c in raw.split(",")}
        z_matrix.append([1 if s in present else 0 for s in site_list])

    if z_matrix and site_list:
        fig2 = go.Figure(go.Heatmap(
            z=z_matrix, x=site_list, y=accessions,
            colorscale=[[0, "#ffffff"], [1, DUKE_BLUE]],
            showscale=False, xgap=1.5, ygap=1.5,
        ))
        fig2.update_layout(
            xaxis=dict(tickangle=-45, tickfont=dict(size=9, family="Arial"),
                       linecolor="#444", linewidth=0.8),
            yaxis=dict(tickfont=dict(family="monospace", size=9),
                       linecolor="#444", linewidth=0.8),
            height=max(280, 18 * len(accessions)),
            margin=dict(l=10, r=10, t=20, b=70),
            font=dict(family="Arial, Helvetica, sans-serif", size=10),
            template="simple_white",
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Insufficient sub-compartment data.")
else:
    st.info("Sub-compartment or accession column not found.")

st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 0"></div>',
            unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# FIGURES 3 & 4 — Disease donut | Sample size violin  (50 / 50)
# ---------------------------------------------------------------------------
col3, col4 = st.columns(2, gap="large")

with col3:
    _heading("Disease Group Distribution")
    _caption("Proportion of datasets per disease / condition category.")

    if DISEASE_COL:
        import re as _re2
        # Explode multi-disease entries
        disease_rows = []
        for val in meta_df[DISEASE_COL].dropna().astype(str):
            for d in _re2.split(r"[;,]", val):
                d = d.strip()
                if d and d.lower() not in ("nan", "none", ""):
                    disease_rows.append(d)

        if disease_rows:
            from collections import Counter
            disease_counts = pd.DataFrame(
                Counter(disease_rows).most_common(),
                columns=["Disease Group", "Count"],
            )

            # Duke-aligned academic palette
            donut_colors = [
                "#00539B", "#B5A369", "#012169", "#2E7D32",
                "#E65100", "#6A1B9A", "#00838F", "#AD1457",
                "#37474F", "#558B2F",
            ]

            fig3 = go.Figure(go.Pie(
                labels=disease_counts["Disease Group"],
                values=disease_counts["Count"],
                hole=0.45,
                marker=dict(
                    colors=donut_colors[:len(disease_counts)],
                    line=dict(color="white", width=1.5),
                ),
                textinfo="label+percent",
                textfont=dict(size=10, family="Arial, Helvetica, sans-serif"),
                hovertemplate="%{label}<br>n = %{value}<extra></extra>",
            ))
            fig3.update_layout(
                showlegend=False,
                margin=dict(t=10, b=10, l=10, r=10),
                font=dict(family="Arial, Helvetica, sans-serif", size=10),
                height=360,
                plot_bgcolor="white", paper_bgcolor="white",
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No disease group data available.")
    else:
        st.info("Disease group column not found.")

with col4:
    _heading("Sample Size Distribution by Modality")
    _caption("Each point is one dataset. Box shows IQR; line shows median.")

    if PATIENTS_COL and MODALITY_COL:
        viol_df = meta_df[[PATIENTS_COL, MODALITY_COL]].copy()
        if TIER_COL:
            viol_df["Tier"] = meta_df[TIER_COL].fillna("Unknown").str.upper()
        viol_df[PATIENTS_COL] = pd.to_numeric(viol_df[PATIENTS_COL], errors="coerce")
        viol_df = viol_df.dropna(subset=[PATIENTS_COL])

        fig4 = go.Figure()
        for mod in viol_df[MODALITY_COL].unique():
            sub   = viol_df[viol_df[MODALITY_COL] == mod]
            color = MODALITY_COLORS.get(str(mod), DUKE_GREY)
            fig4.add_trace(go.Violin(
                x=sub[MODALITY_COL], y=sub[PATIENTS_COL],
                name=mod, fillcolor=color, line_color=color,
                opacity=0.55, points="all", pointpos=0, jitter=0.25,
                marker=dict(
                    color=(sub["Tier"].map(TIER_COLORS).fillna(DUKE_GREY).tolist()
                           if TIER_COL in viol_df.columns else color),
                    size=5, opacity=0.7,
                    line=dict(width=0.5, color="white"),
                ),
                box_visible=True, meanline_visible=True, showlegend=False,
            ))
        layout4 = _nl(h=360)
        layout4["yaxis"]["title"] = "N patients / samples"
        layout4["xaxis"]["title"] = ""
        layout4["margin"] = dict(l=60, r=15, t=20, b=55)
        fig4.update_layout(**layout4)
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("Patient count or modality column not found.")

st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 0"></div>',
            unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# FIGURES 5 & 6 — Demographics | Keyword network  (50 / 50)
# ---------------------------------------------------------------------------
col5, col6 = st.columns(2, gap="large")

with col5:
    _heading("Demographic Summary")
    _caption("Age and BMI by disease group (where reported in metadata).")

    has_demo = AGE_COL or BMI_COL
    if has_demo and DISEASE_COL:
        demo_cols = [c for c in [AGE_COL, BMI_COL, DISEASE_COL] if c]
        demo_df   = meta_df[demo_cols].copy()
        if AGE_COL:
            demo_df[AGE_COL] = pd.to_numeric(demo_df[AGE_COL], errors="coerce")
        if BMI_COL:
            demo_df[BMI_COL] = pd.to_numeric(demo_df[BMI_COL], errors="coerce")
        demo_df = demo_df.dropna(how="all", subset=[c for c in [AGE_COL, BMI_COL] if c])

        fig5 = go.Figure()
        for dg in demo_df[DISEASE_COL].unique():
            sub = demo_df[demo_df[DISEASE_COL] == dg]
            if AGE_COL and sub[AGE_COL].notna().any():
                fig5.add_trace(go.Box(
                    y=sub[AGE_COL].dropna(), name=f"{dg} — Age",
                    marker_color=DUKE_BLUE, boxmean=True,
                    line=dict(width=1),
                ))
            if BMI_COL and sub[BMI_COL].notna().any():
                fig5.add_trace(go.Box(
                    y=sub[BMI_COL].dropna(), name=f"{dg} — BMI",
                    marker_color=DUKE_GOLD, boxmean=True,
                    line=dict(width=1),
                ))
        layout5 = _nl(h=360)
        layout5["showlegend"] = True
        layout5["legend"]     = dict(font=dict(size=9), orientation="v")
        layout5["margin"]     = dict(l=60, r=15, t=20, b=55)
        fig5.update_layout(**layout5)
        st.plotly_chart(fig5, use_container_width=True)
    elif has_demo:
        st.info("Disease group column needed for stratified demographic plot.")
    else:
        st.info("Age / BMI columns not found in metadata.")

with col6:
    _heading("Abstract Keyword Network")
    _caption("TF-IDF co-occurrence of top 25 terms across all abstracts. Node size ∝ term frequency.")

    if ABSTRACT_COL and meta_df[ABSTRACT_COL].notna().sum() >= 3:
        try:
            import networkx as nx
            from sklearn.cluster import KMeans
            from sklearn.feature_extraction.text import TfidfVectorizer

            abstracts = meta_df[ABSTRACT_COL].dropna().astype(str).tolist()
            vectorizer = TfidfVectorizer(
                max_features=25, stop_words="english", ngram_range=(1, 2),
            )
            tfidf_matrix = vectorizer.fit_transform(abstracts)
            terms        = vectorizer.get_feature_names_out()
            term_freq    = np.asarray(tfidf_matrix.sum(axis=0)).flatten()

            dense = tfidf_matrix.toarray()
            cooc  = dense.T @ dense
            np.fill_diagonal(cooc, 0)
            cooc  = cooc / (cooc.max() + 1e-9)

            n_clusters = min(4, len(terms))
            kmeans     = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters   = kmeans.fit_predict(dense.T @ dense)

            G = nx.Graph()
            for t in terms:
                G.add_node(t)
            for i in range(len(terms)):
                for j in range(i + 1, len(terms)):
                    if cooc[i, j] > 0.1:
                        G.add_edge(terms[i], terms[j], weight=float(cooc[i, j]))

            pos = nx.spring_layout(G, seed=42, k=0.8)

            cluster_colors = [DUKE_BLUE, DUKE_GOLD, DUKE_NAVY, "#2E7D32"]
            node_x    = [pos[t][0] for t in terms]
            node_y    = [pos[t][1] for t in terms]
            node_sz   = [max(8, freq * 38) for freq in term_freq]
            node_clrs = [cluster_colors[clusters[i] % len(cluster_colors)]
                         for i in range(len(terms))]

            edge_traces = []
            for (u, v, data) in G.edges(data=True):
                x0, y0 = pos[u]; x1, y1 = pos[v]
                w = data.get("weight", 0.1)
                edge_traces.append(go.Scatter(
                    x=[x0, x1, None], y=[y0, y1, None],
                    mode="lines",
                    line=dict(width=w * 3, color="#d1d5db"),
                    hoverinfo="none", showlegend=False,
                ))

            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode="markers+text",
                text=list(terms),
                textposition="top center",
                textfont=dict(size=8, family="Arial, Helvetica, sans-serif",
                              color="#374151"),
                marker=dict(
                    size=node_sz, color=node_clrs,
                    line=dict(width=0.8, color="white"),
                ),
                hovertemplate="%{text}<extra></extra>",
                showlegend=False,
            )

            fig6 = go.Figure(data=edge_traces + [node_trace])
            fig6.update_layout(
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                           showline=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                           showline=False),
                height=360,
                margin=dict(l=10, r=10, t=20, b=10),
                font=dict(family="Arial, Helvetica, sans-serif"),
                plot_bgcolor="white", paper_bgcolor="white",
            )
            st.plotly_chart(fig6, use_container_width=True)

        except ImportError as e:
            st.warning(f"Keyword network requires additional packages: {e}")
        except Exception as e:
            st.warning(f"Could not render keyword network: {e}")
    else:
        st.info("Abstract column not found or too few records for keyword network.")

st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 0"></div>',
            unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# FIGURE 7 — Confidence score histogram  (half width)
# ---------------------------------------------------------------------------
col7, col_blank = st.columns(2, gap="large")

with col7:
    _heading("Confidence Score Distribution")
    _caption("Tier thresholds: BRONZE ≥ 40, SILVER ≥ 60, GOLD ≥ 80.")

    if SCORE_COL:
        score_df = meta_df[[SCORE_COL]].copy()
        if TIER_COL:
            score_df["Tier"] = meta_df[TIER_COL].fillna("LOW_CONFIDENCE").str.upper()
        score_df[SCORE_COL] = pd.to_numeric(score_df[SCORE_COL], errors="coerce")
        score_df = score_df.dropna(subset=[SCORE_COL])

        fig7 = px.histogram(
            score_df, x=SCORE_COL,
            color="Tier" if TIER_COL else None,
            color_discrete_map=TIER_COLORS,
            nbins=20,
            labels={SCORE_COL: "Confidence score"},
        )
        for val, label, clr in [
            (40, "BRONZE", TIER_COLORS["BRONZE"]),
            (60, "SILVER", TIER_COLORS["SILVER"]),
            (80, "GOLD",   TIER_COLORS["GOLD"]),
        ]:
            fig7.add_vline(
                x=val, line_dash="dash", line_color=clr, line_width=1.2,
                annotation_text=label,
                annotation_position="top",
                annotation_font=dict(color=clr, size=9,
                                     family="Arial, Helvetica, sans-serif"),
            )

        layout7 = _nl(h=340)
        layout7["yaxis"]["title"]  = "Count"
        layout7["xaxis"]["title"]  = "Confidence score"
        layout7["legend"]          = dict(title="Tier", font=dict(size=10))
        layout7["margin"]          = dict(l=60, r=15, t=30, b=55)
        fig7.update_layout(**layout7)
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("Confidence score column not found.")

st.markdown('<div style="height:1px;background:#e5e7eb;margin:0.5rem 0 0"></div>',
            unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# FIGURE 8 — Publication timeline (full width)
# ---------------------------------------------------------------------------
_heading("Publication Timeline")
_caption(
    "Each point represents one dataset. Marker area ∝ dataset size "
    "(cells or patients). Dashed lines indicate tier thresholds."
)

if YEAR_COL and SCORE_COL:
    time_df = meta_df.copy()
    time_df[YEAR_COL]  = pd.to_numeric(time_df[YEAR_COL], errors="coerce")
    time_df[SCORE_COL] = pd.to_numeric(time_df[SCORE_COL], errors="coerce")
    time_df = time_df.dropna(subset=[YEAR_COL, SCORE_COL])

    size_col = CELLS_COL or PATIENTS_COL
    if size_col:
        time_df[size_col] = pd.to_numeric(time_df[size_col], errors="coerce").fillna(100)
        sz_min = time_df[size_col].min()
        sz_max = time_df[size_col].max()
        time_df["_msz"] = (
            6 + 22 * (time_df[size_col] - sz_min) / (sz_max - sz_min + 1e-9)
        )
    else:
        time_df["_msz"] = 10.0

    cdata_cols = [c for c in [TITLE_COL, ACC_COL, AUTHORS_COL] if c and c in time_df.columns]
    hover_tmpl = ""
    if TITLE_COL:   hover_tmpl += "<b>%{customdata[0]}</b><br>"
    if ACC_COL:     hover_tmpl += "Accession: %{customdata[1]}<br>"
    hover_tmpl += "Year: %{x} &nbsp; Score: %{y:.1f}<extra></extra>"

    traces = []
    groups = time_df.groupby(MODALITY_COL) if MODALITY_COL else [(None, time_df)]
    for mod, grp in groups:
        color = MODALITY_COLORS.get(str(mod), DUKE_GREY) if mod else DUKE_BLUE
        cd    = grp[cdata_cols].values if cdata_cols else None
        traces.append(go.Scatter(
            x=grp[YEAR_COL], y=grp[SCORE_COL],
            mode="markers",
            name=str(mod) if mod else "All",
            marker=dict(
                size=grp["_msz"].tolist(), color=color,
                opacity=0.72, line=dict(width=0.5, color="white"),
            ),
            customdata=cd,
            hovertemplate=hover_tmpl,
        ))

    fig8 = go.Figure(data=traces)
    for val, label, clr in [
        (40, "BRONZE", TIER_COLORS["BRONZE"]),
        (60, "SILVER", TIER_COLORS["SILVER"]),
        (80, "GOLD",   TIER_COLORS["GOLD"]),
    ]:
        fig8.add_hline(
            y=val, line_dash="dot", line_color=clr, line_width=1,
            annotation_text=label, annotation_position="right",
            annotation_font=dict(size=9, color=clr,
                                 family="Arial, Helvetica, sans-serif"),
        )

    layout8 = _nl(h=400)
    layout8["xaxis"]["title"] = "Publication year"
    layout8["yaxis"]["title"] = "Confidence score"
    layout8["legend"]         = dict(
        title=dict(text="Modality", font=dict(size=10)),
        font=dict(size=10),
    )
    layout8["margin"] = dict(l=60, r=80, t=30, b=60)
    fig8.update_layout(**layout8)
    st.plotly_chart(fig8, use_container_width=True)
else:
    st.info("Year or confidence score column not found.")

# Footer
st.markdown(
    '<div style="margin-top:2rem;height:1px;background:#e5e7eb"></div>',
    unsafe_allow_html=True,
)
st.markdown("""
<div style="font-family:Arial,sans-serif;font-size:0.7rem;color:#9ca3af;
            text-align:center;padding:0.75rem 0">
    Hickey Lab · Duke University · Endometrial Receptivity Aim 01
</div>
""", unsafe_allow_html=True)
