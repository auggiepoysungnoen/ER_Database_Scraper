"""
Statistics Dashboard — Endometrial Receptivity Database
8 interactive Plotly charts with Duke branding.
"""

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

# ---------------------------------------------------------------------------
# Duke palette
# ---------------------------------------------------------------------------
DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GREY = "#666666"
DUKE_GOLD = "#B5A369"

MODALITY_COLORS = {
    "scRNA-seq": "#00539B",
    "bulkRNA-seq": "#B5A369",
    "Spatial Transcriptomics": "#012169",
    "Spatial Proteomics": "#4A90D9",
}
TIER_COLORS = {
    "GOLD": "#B5A369",
    "SILVER": "#9E9E9E",
    "BRONZE": "#CD7F32",
    "LOW_CONFIDENCE": "#CCCCCC",
}

PLOTLY_TEMPLATE = "simple_white"
FONT = dict(family="system-ui, sans-serif")

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
# Missing data guard
# ---------------------------------------------------------------------------
st.title("📊 Statistics Dashboard")

if meta_df.empty and not METADATA_PATH.exists():
    st.warning(
        "Output files not found. Run the pipeline first: `python script/run_pipeline.py`",
        icon="⚠️",
    )
    st.stop()

if meta_df.empty:
    st.info("Metadata file is empty.")
    st.stop()

# ---------------------------------------------------------------------------
# Column resolution helpers
# ---------------------------------------------------------------------------
def _col(df, *candidates):
    for c in candidates:
        if c in df.columns:
            return c
        for col in df.columns:
            if col.lower().replace(" ", "_") == c.lower().replace(" ", "_"):
                return col
    return None


MODALITY_COL  = _col(meta_df, "modality", "Modality", "data_type")
LH_COL        = _col(meta_df, "lh_timepoints", "timepoints", "cycle_phase", "LH_Timepoints")
SUBCOMP_COL   = _col(meta_df, "sub_compartments", "cell_types", "Sub_Compartments")
DISEASE_COL   = _col(meta_df, "disease_group", "condition", "Disease_Group")
PATIENTS_COL  = _col(meta_df, "n_patients", "n_samples", "N_Patients")
CELLS_COL     = _col(meta_df, "n_cells", "cell_count", "N_Cells")
YEAR_COL      = _col(meta_df, "year", "publication_year", "Year")
TIER_COL      = _col(meta_df, "confidence_tier", "tier", "Tier")
SCORE_COL     = _col(meta_df, "confidence_score", "score", "total_score", "Score")
ACC_COL       = _col(meta_df, "accession", "Accession", "dataset_id")
TITLE_COL     = _col(meta_df, "title", "Title", "study_title")
AUTHORS_COL   = _col(meta_df, "authors", "Authors", "author")
ABSTRACT_COL  = _col(meta_df, "abstract", "Abstract", "summary")
AGE_COL       = _col(meta_df, "age", "mean_age", "Age")
BMI_COL       = _col(meta_df, "bmi", "mean_bmi", "BMI")

# Merge conf_df into meta_df if available
if not conf_df.empty and SCORE_COL is None:
    conf_acc = _col(conf_df, "accession", "Accession", "dataset_id")
    meta_acc = ACC_COL
    if conf_acc and meta_acc:
        meta_df = meta_df.merge(conf_df, left_on=meta_acc, right_on=conf_acc, how="left", suffixes=("", "_c"))
        SCORE_COL = _col(meta_df, "confidence_score", "score", "total_score")
        TIER_COL  = _col(meta_df, "confidence_tier", "tier", "Tier")


# ---------------------------------------------------------------------------
# Helper: get modality color list for a series
# ---------------------------------------------------------------------------
def _mod_colors(series: pd.Series) -> list:
    return [MODALITY_COLORS.get(m, DUKE_GREY) for m in series]


def _tier_colors(series: pd.Series) -> list:
    return [TIER_COLORS.get(str(t).upper(), DUKE_GREY) for t in series]


# ===========================================================================
# PLOT 1 — Dataset Count by LH Timepoint (full width)
# ===========================================================================
st.markdown("### Dataset Count by LH Timepoint")

if LH_COL and MODALITY_COL:
    # Explode multi-value LH timepoints
    lh_rows = []
    for _, row in meta_df.iterrows():
        lh_raw = str(row[LH_COL]) if pd.notna(row[LH_COL]) else ""
        modality = row[MODALITY_COL] if MODALITY_COL and pd.notna(row.get(MODALITY_COL)) else "Unknown"
        for tp in lh_raw.split(","):
            tp = tp.strip()
            if tp:
                lh_rows.append({"LH_Timepoint": tp, "Modality": modality})

    if lh_rows:
        lh_df = pd.DataFrame(lh_rows)
        lh_counts = lh_df.groupby(["LH_Timepoint", "Modality"]).size().reset_index(name="Count")

        # Chronological sort for LH timepoints
        def _lh_sort_key(tp: str) -> float:
            import re
            m = re.search(r"([+-]?\d+)", tp)
            return float(m.group(1)) if m else 999

        tp_order = sorted(lh_counts["LH_Timepoint"].unique(), key=_lh_sort_key)

        fig1 = px.bar(
            lh_counts,
            x="LH_Timepoint",
            y="Count",
            color="Modality",
            color_discrete_map=MODALITY_COLORS,
            category_orders={"LH_Timepoint": tp_order},
            template=PLOTLY_TEMPLATE,
            barmode="stack",
        )

        # WOI shaded region
        woi_start = tp_order.index("LH+5") if "LH+5" in tp_order else None
        woi_end = tp_order.index("LH+9") if "LH+9" in tp_order else None
        if woi_start is not None and woi_end is not None:
            fig1.add_vrect(
                x0=woi_start - 0.5,
                x1=woi_end + 0.5,
                fillcolor=DUKE_GOLD,
                opacity=0.12,
                line_width=0,
                annotation_text="Window of Implantation",
                annotation_position="top left",
                annotation_font=dict(color=DUKE_NAVY, size=11),
            )

        fig1.update_layout(font=FONT, margin=dict(t=30, b=30))
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No LH timepoint data to plot.")
else:
    st.info("LH timepoint or modality column not found in metadata.")

st.divider()

# ===========================================================================
# PLOT 2 — Sub-compartment Coverage Heatmap (full width)
# ===========================================================================
st.markdown("### Sub-compartment Coverage Heatmap")
st.caption("Top 30 datasets by confidence score. Binary: blue = present, white = absent.")

if SUBCOMP_COL and ACC_COL:
    # Top 30 by score
    plot_df = meta_df.copy()
    if SCORE_COL:
        plot_df[SCORE_COL] = pd.to_numeric(plot_df[SCORE_COL], errors="coerce")
        plot_df = plot_df.nlargest(30, SCORE_COL)
    else:
        plot_df = plot_df.head(30)

    # Build binary matrix
    all_celltypes: set = set()
    for val in plot_df[SUBCOMP_COL].dropna().astype(str):
        for ct in val.split(","):
            ct = ct.strip()
            if ct:
                all_celltypes.add(ct)

    cell_list = sorted(all_celltypes)
    accessions = plot_df[ACC_COL].fillna("").astype(str).tolist()

    z_matrix = []
    for _, row in plot_df.iterrows():
        raw = str(row[SUBCOMP_COL]) if pd.notna(row[SUBCOMP_COL]) else ""
        present = {c.strip() for c in raw.split(",")}
        z_matrix.append([1 if ct in present else 0 for ct in cell_list])

    if z_matrix and cell_list:
        fig2 = go.Figure(go.Heatmap(
            z=z_matrix,
            x=cell_list,
            y=accessions,
            colorscale=[[0, "white"], [1, DUKE_BLUE]],
            showscale=False,
            xgap=1,
            ygap=1,
        ))
        fig2.update_layout(
            xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
            yaxis=dict(tickfont=dict(family="monospace", size=10)),
            height=max(300, 20 * len(accessions)),
            margin=dict(l=10, r=10, t=20, b=60),
            font=FONT,
            template=PLOTLY_TEMPLATE,
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Insufficient sub-compartment data to build heatmap.")
else:
    st.info("Sub-compartment or accession column not found in metadata.")

st.divider()

# ===========================================================================
# PLOTS 3 & 4 — Disease donut | Sample size violin  (50/50 columns)
# ===========================================================================
col3, col4 = st.columns(2)

# ---------- Plot 3: Disease Group Donut ----------
with col3:
    st.markdown("### Disease Group Distribution")
    if DISEASE_COL:
        disease_counts = meta_df[DISEASE_COL].value_counts().reset_index()
        disease_counts.columns = ["Disease Group", "Count"]

        donut_colors = [
            DUKE_BLUE, DUKE_GOLD, DUKE_NAVY, "#4A90D9", "#7FB3D3",
            "#D4A843", "#3D6A8A", "#8AAFC1", "#C19A2E",
        ]

        fig3 = go.Figure(go.Pie(
            labels=disease_counts["Disease Group"],
            values=disease_counts["Count"],
            hole=0.45,
            marker=dict(colors=donut_colors[:len(disease_counts)]),
            textinfo="label+percent",
            hovertemplate="%{label}<br>Count: %{value}<extra></extra>",
        ))
        fig3.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20, l=10, r=10),
            font=FONT,
            height=380,
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Disease group column not found in metadata.")

# ---------- Plot 4: Sample Size Violin ----------
with col4:
    st.markdown("### Sample Size Distribution")
    if PATIENTS_COL and MODALITY_COL:
        viol_df = meta_df[[PATIENTS_COL, MODALITY_COL]].copy()
        if TIER_COL:
            viol_df["Tier"] = meta_df[TIER_COL].fillna("Unknown").str.upper()
        viol_df[PATIENTS_COL] = pd.to_numeric(viol_df[PATIENTS_COL], errors="coerce")
        viol_df = viol_df.dropna(subset=[PATIENTS_COL])

        modalities = viol_df[MODALITY_COL].unique()
        fig4 = go.Figure()

        for mod in modalities:
            sub = viol_df[viol_df[MODALITY_COL] == mod]
            color = MODALITY_COLORS.get(mod, DUKE_GREY)

            fig4.add_trace(go.Violin(
                x=sub[MODALITY_COL],
                y=sub[PATIENTS_COL],
                name=mod,
                fillcolor=color,
                line_color=color,
                opacity=0.6,
                points="all",
                pointpos=0,
                jitter=0.3,
                marker=dict(
                    color=sub["Tier"].map(TIER_COLORS).fillna(DUKE_GREY).tolist()
                    if TIER_COL else color,
                    size=6,
                    opacity=0.8,
                ),
                box_visible=True,
                meanline_visible=True,
                showlegend=False,
            ))

        fig4.update_layout(
            yaxis_title="N Patients / Samples",
            xaxis_title="",
            template=PLOTLY_TEMPLATE,
            font=FONT,
            height=380,
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("Patient count or modality column not found in metadata.")

st.divider()

# ===========================================================================
# PLOTS 5 & 6 — Demographics | Keyword Network  (50/50)
# ===========================================================================
col5, col6 = st.columns(2)

# ---------- Plot 5: Demographics ----------
with col5:
    st.markdown("### Demographic Summary")
    st.caption("Age/BMI only shown where reported in metadata.")

    has_demo = AGE_COL or BMI_COL
    if has_demo and DISEASE_COL:
        demo_cols = [c for c in [AGE_COL, BMI_COL, DISEASE_COL] if c]
        demo_df = meta_df[demo_cols].copy()
        if AGE_COL:
            demo_df[AGE_COL] = pd.to_numeric(demo_df[AGE_COL], errors="coerce")
        if BMI_COL:
            demo_df[BMI_COL] = pd.to_numeric(demo_df[BMI_COL], errors="coerce")
        demo_df = demo_df.dropna(how="all", subset=[c for c in [AGE_COL, BMI_COL] if c])

        fig5 = go.Figure()
        diseases = demo_df[DISEASE_COL].unique()

        for dg in diseases:
            sub = demo_df[demo_df[DISEASE_COL] == dg]
            if AGE_COL and sub[AGE_COL].notna().any():
                fig5.add_trace(go.Box(
                    y=sub[AGE_COL].dropna(),
                    name=f"{dg} — Age",
                    marker_color=DUKE_BLUE,
                    boxmean=True,
                ))
            if BMI_COL and sub[BMI_COL].notna().any():
                fig5.add_trace(go.Box(
                    y=sub[BMI_COL].dropna(),
                    name=f"{dg} — BMI",
                    marker_color=DUKE_GOLD,
                    boxmean=True,
                ))

        fig5.update_layout(
            template=PLOTLY_TEMPLATE,
            font=FONT,
            height=380,
            margin=dict(t=20, b=20),
            showlegend=True,
            legend=dict(font=dict(size=9)),
        )
        st.plotly_chart(fig5, use_container_width=True)
    elif has_demo:
        st.info("Disease group column needed for faceted demographic plot.")
    else:
        st.info("Age/BMI columns not found in metadata.")

# ---------- Plot 6: Keyword Network ----------
with col6:
    st.markdown("### Abstract Keyword Network")

    if ABSTRACT_COL and meta_df[ABSTRACT_COL].notna().sum() >= 3:
        try:
            import networkx as nx
            from sklearn.cluster import KMeans
            from sklearn.feature_extraction.text import TfidfVectorizer

            abstracts = meta_df[ABSTRACT_COL].dropna().astype(str).tolist()

            # TF-IDF
            vectorizer = TfidfVectorizer(
                max_features=25,
                stop_words="english",
                ngram_range=(1, 2),
            )
            tfidf_matrix = vectorizer.fit_transform(abstracts)
            terms = vectorizer.get_feature_names_out()
            term_freq = np.asarray(tfidf_matrix.sum(axis=0)).flatten()

            # Co-occurrence matrix
            dense = tfidf_matrix.toarray()
            cooc = dense.T @ dense
            np.fill_diagonal(cooc, 0)
            cooc = cooc / (cooc.max() + 1e-9)

            # Cluster
            n_clusters = min(4, len(terms))
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(dense.T @ dense)

            # Graph layout
            G = nx.Graph()
            for i, t in enumerate(terms):
                G.add_node(t)
            threshold = 0.1
            for i in range(len(terms)):
                for j in range(i + 1, len(terms)):
                    if cooc[i, j] > threshold:
                        G.add_edge(terms[i], terms[j], weight=float(cooc[i, j]))

            pos = nx.spring_layout(G, seed=42, k=0.8)

            cluster_colors = [DUKE_BLUE, DUKE_GOLD, DUKE_NAVY, "#4A90D9"]

            node_x = [pos[t][0] for t in terms]
            node_y = [pos[t][1] for t in terms]
            node_sizes = [max(10, freq * 40) for freq in term_freq]
            node_colors = [cluster_colors[clusters[i] % len(cluster_colors)] for i in range(len(terms))]

            # Edges
            edge_traces = []
            for (u, v, data) in G.edges(data=True):
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                w = data.get("weight", 0.1)
                edge_traces.append(go.Scatter(
                    x=[x0, x1, None],
                    y=[y0, y1, None],
                    mode="lines",
                    line=dict(width=w * 4, color=DUKE_GREY),
                    hoverinfo="none",
                    showlegend=False,
                ))

            node_trace = go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                text=list(terms),
                textposition="top center",
                textfont=dict(size=9, family="system-ui, sans-serif"),
                marker=dict(
                    size=node_sizes,
                    color=node_colors,
                    line=dict(width=1, color="white"),
                ),
                hovertemplate="%{text}<br>TF-IDF: %{marker.size:.1f}<extra></extra>",
                showlegend=False,
            )

            fig6 = go.Figure(data=edge_traces + [node_trace])
            fig6.update_layout(
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=380,
                margin=dict(l=10, r=10, t=20, b=10),
                font=FONT,
                template=PLOTLY_TEMPLATE,
            )
            st.plotly_chart(fig6, use_container_width=True)

        except ImportError as e:
            st.warning(f"Keyword network requires additional packages: {e}")
        except Exception as e:
            st.warning(f"Could not render keyword network: {e}")
    else:
        st.info("Abstract column not found or insufficient records for keyword network.")

st.divider()

# ===========================================================================
# PLOTS 7 & 8 — Score histogram | Publication timeline
# ===========================================================================
col7, _ = st.columns(2)

# ---------- Plot 7: Confidence Score Histogram ----------
with col7:
    st.markdown("### Confidence Score Distribution")
    if SCORE_COL:
        score_df = meta_df[[SCORE_COL]].copy()
        if TIER_COL:
            score_df["Tier"] = meta_df[TIER_COL].fillna("LOW_CONFIDENCE").str.upper()
        score_df[SCORE_COL] = pd.to_numeric(score_df[SCORE_COL], errors="coerce")
        score_df = score_df.dropna(subset=[SCORE_COL])

        fig7 = px.histogram(
            score_df,
            x=SCORE_COL,
            color="Tier" if TIER_COL else None,
            color_discrete_map=TIER_COLORS,
            nbins=20,
            template=PLOTLY_TEMPLATE,
            labels={SCORE_COL: "Confidence Score"},
        )

        # Threshold lines
        thresholds = [(40, "BRONZE", TIER_COLORS["BRONZE"]),
                      (60, "SILVER", TIER_COLORS["SILVER"]),
                      (80, "GOLD",   TIER_COLORS["GOLD"])]
        for val, label, color in thresholds:
            fig7.add_vline(
                x=val,
                line_dash="dash",
                line_color=color,
                line_width=2,
                annotation_text=label,
                annotation_position="top",
                annotation_font=dict(color=color, size=11),
            )

        fig7.update_layout(
            font=FONT,
            height=350,
            margin=dict(t=30, b=20),
            legend=dict(title="Tier"),
        )
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("Confidence score column not found in metadata.")

st.divider()

# ---------- Plot 8: Publication Timeline (full width) ----------
st.markdown("### Publication Timeline")

if YEAR_COL and SCORE_COL:
    time_df = meta_df.copy()
    time_df[YEAR_COL] = pd.to_numeric(time_df[YEAR_COL], errors="coerce")
    time_df[SCORE_COL] = pd.to_numeric(time_df[SCORE_COL], errors="coerce")
    time_df = time_df.dropna(subset=[YEAR_COL, SCORE_COL])

    size_col = CELLS_COL or PATIENTS_COL
    if size_col:
        time_df[size_col] = pd.to_numeric(time_df[size_col], errors="coerce").fillna(100)
        # Normalise size for marker size: 6 – 30
        sz_min = time_df[size_col].min()
        sz_max = time_df[size_col].max()
        if sz_max > sz_min:
            time_df["_marker_size"] = 6 + 24 * (time_df[size_col] - sz_min) / (sz_max - sz_min)
        else:
            time_df["_marker_size"] = 12.0
    else:
        time_df["_marker_size"] = 12.0

    hover_parts = []
    if TITLE_COL:
        hover_parts.append(f"<b>%{{customdata[0]}}</b><br>")
    if ACC_COL:
        hover_parts.append("Accession: %{customdata[1]}<br>")
    if AUTHORS_COL:
        hover_parts.append("Authors: %{customdata[2]}<br>")
    hover_parts.append("Year: %{x}<br>Score: %{y:.1f}<extra></extra>")
    hover_template = "".join(hover_parts)

    custom_data_cols = [
        TITLE_COL or ACC_COL or SCORE_COL,
        ACC_COL or SCORE_COL,
        AUTHORS_COL or SCORE_COL,
    ]
    custom_data = time_df[[c for c in custom_data_cols if c and c in time_df.columns]].values

    traces = []
    modality_groups = time_df.groupby(MODALITY_COL) if MODALITY_COL else [(None, time_df)]
    for mod, grp in modality_groups:
        color = MODALITY_COLORS.get(str(mod), DUKE_GREY) if mod else DUKE_BLUE
        cd = grp[[c for c in custom_data_cols if c and c in grp.columns]].values
        traces.append(go.Scatter(
            x=grp[YEAR_COL],
            y=grp[SCORE_COL],
            mode="markers",
            name=str(mod) if mod else "All",
            marker=dict(
                size=grp["_marker_size"].tolist(),
                color=color,
                opacity=0.75,
                line=dict(width=1, color="white"),
            ),
            customdata=cd,
            hovertemplate=hover_template,
        ))

    fig8 = go.Figure(data=traces)
    # Threshold lines
    for val, label, color in [(40, "BRONZE", TIER_COLORS["BRONZE"]),
                               (60, "SILVER", TIER_COLORS["SILVER"]),
                               (80, "GOLD",   TIER_COLORS["GOLD"])]:
        fig8.add_hline(
            y=val,
            line_dash="dot",
            line_color=color,
            line_width=1,
            annotation_text=label,
            annotation_position="right",
        )

    fig8.update_layout(
        xaxis_title="Publication Year",
        yaxis_title="Confidence Score",
        template=PLOTLY_TEMPLATE,
        font=FONT,
        height=420,
        margin=dict(t=20, b=30),
        legend=dict(title="Modality"),
    )
    st.plotly_chart(fig8, use_container_width=True)
else:
    st.info("Year or confidence score column not found in metadata.")
