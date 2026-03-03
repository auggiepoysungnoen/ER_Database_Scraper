"""
Search Engine — Hickey Lab Open-Access Genomics Search
Real-time search across open-source genomics repositories with Gemini AI enrichment,
weighted confidence scoring, and approve/reject curation workflow.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Search Engine | Hickey Lab",
    layout="wide",
    page_icon="🔍",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
APP_DIR    = Path(__file__).parent.parent
REPO_ROOT  = APP_DIR.parent.parent
SCRIPT_DIR = REPO_ROOT / "script"
OUTPUT_DIR = REPO_ROOT / "output"

# Add script dir for scraper + scoring imports
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DB_OPTIONS = ["geo", "arrayexpress", "cellxgene", "hca", "scp", "zenodo", "figshare"]

TIER_COLORS = {
    "GOLD":           ("#FEF3C7", "#92710A"),
    "SILVER":         ("#F3F4F6", "#6B7280"),
    "BRONZE":         ("#FEF0E6", "#C05621"),
    "LOW_CONFIDENCE": ("#F9FAFB", "#9CA3AF"),
}

SCRAPER_MAP: dict[str, tuple[str, str]] = {
    "geo":          ("scrapers.geo",              "GEOScraper"),
    "arrayexpress": ("scrapers.arrayexpress",     "ArrayExpressScraper"),
    "cellxgene":    ("scrapers.cellxgene",        "CellxGeneScraper"),
    "hca":          ("scrapers.hca",              "HCAScraper"),
    "scp":          ("scrapers.singlecellportal", "SingleCellPortalScraper"),
    "zenodo":       ("scrapers.zenodo",           "ZenodoScraper"),
    "figshare":     ("scrapers.figshare",         "FigshareScraper"),
}

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:1.5rem;padding-bottom:3rem;max-width:1320px}
.section-label{
    font-family:Arial,sans-serif;font-size:0.6rem;font-weight:700;
    letter-spacing:0.12em;text-transform:uppercase;color:#9ca3af;
    margin-bottom:0.7rem;padding-bottom:0.35rem;border-bottom:1px solid #e5e7eb
}
.result-card{
    background:#fff;border:1px solid #e5e7eb;border-left:3px solid #e5e7eb;
    border-radius:2px;padding:0.9rem 1rem;margin-bottom:0.55rem;
    font-family:Arial,sans-serif;transition:border-color 0.15s
}
.result-card.approved{border-left-color:#16A34A;background:#F0FDF4}
.result-card.rejected{border-left-color:#DC2626;background:#FEF2F2;opacity:0.65}
.score-pill{
    display:inline-block;padding:1px 8px;border-radius:2px;
    font-size:0.7rem;font-weight:700;letter-spacing:0.04em;
    font-family:Arial,sans-serif
}
.meta-chip{
    display:inline-block;background:#f3f4f6;color:#374151;
    padding:1px 7px;border-radius:2px;font-size:0.72rem;
    font-family:Arial,sans-serif;margin-right:4px
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
_DEFAULTS: dict = {
    "se_results":     [],
    "se_approved":    set(),
    "se_rejected":    set(),
    "se_done":        False,
    "se_query":       "",
    "se_weights":     {},
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
try:
    GEMINI_KEY = st.secrets.get("gemini", {}).get("api_key", "")
except Exception:
    GEMINI_KEY = ""

try:
    NCBI_KEY = st.secrets.get("ncbi", {}).get("api_key", "")
except Exception:
    NCBI_KEY = ""

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _tier_pill(tier: str) -> str:
    bg, fg = TIER_COLORS.get(tier.upper(), ("#F9FAFB", "#9CA3AF"))
    label = tier.replace("_", " ").upper()
    return (f'<span class="score-pill" style="background:{bg};color:{fg}">'
            f'{label}</span>')


def _score_pill(score: float) -> str:
    if score >= 80:
        bg, fg = "#FEF3C7", "#92710A"
    elif score >= 60:
        bg, fg = "#F3F4F6", "#6B7280"
    elif score >= 40:
        bg, fg = "#FEF0E6", "#C05621"
    else:
        bg, fg = "#F9FAFB", "#9CA3AF"
    return (f'<span class="score-pill" style="background:{bg};color:{fg}">'
            f'{score:.0f} / 100</span>')


def _db_url(accession: str, db: str) -> str:
    acc = accession.upper()
    if acc.startswith(("GSE", "GDS", "GSM")):
        return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"
    if acc.startswith("E-"):
        return f"https://www.ebi.ac.uk/biostudies/arrayexpress/studies/{accession}"
    if db == "zenodo":
        zid = accession.replace("zenodo.", "").replace("ZENODO.", "")
        return f"https://zenodo.org/record/{zid}"
    if db == "figshare":
        return f"https://figshare.com/search?q={accession}"
    return ""


def _approve(acc: str):
    st.session_state.se_approved.add(acc)
    st.session_state.se_rejected.discard(acc)


def _reject(acc: str):
    st.session_state.se_rejected.add(acc)
    st.session_state.se_approved.discard(acc)


def _undo(acc: str):
    st.session_state.se_approved.discard(acc)
    st.session_state.se_rejected.discard(acc)


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown("""
<p style="font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
          color:#012169;letter-spacing:-0.02em;margin-bottom:0.1rem">
    Genomics Dataset Search Engine
</p>
<p style="font-family:Arial,sans-serif;font-size:0.875rem;color:#6b7280;margin:0">
    Real-time search across open-access repositories · Gemini AI metadata extraction ·
    Customisable confidence scoring · Approve &amp; download curated datasets
</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.85rem 0 1.5rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Configuration panel
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Search Configuration</div>',
            unsafe_allow_html=True)

col_query, col_weights, col_opts = st.columns([2.4, 2.4, 1.2], gap="large")

with col_query:
    selected_dbs = st.multiselect(
        "Repositories to search",
        options=DB_OPTIONS,
        default=["geo", "arrayexpress", "zenodo", "figshare"],
        help="Select which open-access repositories to query.",
    )
    search_query = st.text_area(
        "Search Query",
        placeholder=(
            "Describe what you're looking for in natural language:\n\n"
            "e.g. 'Endometrial single-cell RNA-seq LH+7 window of implantation'\n"
            "     'Placental trophoblast spatial transcriptomics'\n"
            "     'COVID-19 PBMC bulk RNA-seq longitudinal cohort'"
        ),
        height=140,
        help="Gemini AI scores each result's relevance to this exact query.",
    )
    full_text_only = st.checkbox(
        "Require full paper text available for AI analysis",
        value=True,
        help="Only include datasets where Gemini confirms the paper has accessible full text.",
    )

with col_weights:
    st.markdown(
        '<div style="font-size:0.72rem;color:#6b7280;font-family:Arial,sans-serif;'
        'margin-bottom:0.6rem;font-weight:600">Confidence Score Dimension Weights</div>',
        unsafe_allow_html=True,
    )
    w_journal  = st.slider("Journal Impact Factor / Reliability",    0, 100, 20, key="w_j")
    w_lh       = st.slider("LH Timepoint Data Availability",         0, 100, 25, key="w_l")
    w_tissue   = st.slider("Tissue Collection Site Specificity",     0, 100, 15, key="w_t")
    w_rel      = st.slider("Relatability to Search Topic (Gemini)",  0, 100, 20, key="w_r")
    w_comp     = st.slider("Data Completeness / Protocol Metadata",  0, 100, 10, key="w_c")
    w_access   = st.slider("Data Accessibility",                     0, 100, 10, key="w_a")

    total_w = w_journal + w_lh + w_tissue + w_rel + w_comp + w_access
    w_color = "#16A34A" if total_w > 0 else "#D97706"
    st.markdown(
        f'<div style="font-family:Arial,sans-serif;font-size:0.72rem;margin-top:0.35rem">'
        f'Total: <span style="color:{w_color};font-weight:700">{total_w}</span>'
        f'<span style="color:#9ca3af"> (normalised to 100 internally)</span></div>',
        unsafe_allow_html=True,
    )

with col_opts:
    min_conf = st.slider(
        "Min Confidence Score",
        min_value=0, max_value=80, value=25, step=5,
        help="Hide results below this threshold.",
    )
    max_per_db = st.number_input(
        "Max results per database",
        min_value=5, max_value=300, value=40, step=5,
    )
    if GEMINI_KEY:
        st.markdown(
            '<div style="font-size:0.7rem;background:#EFF6FF;color:#1D4ED8;'
            'padding:6px 8px;border-radius:2px;margin-top:0.5rem">'
            '✓ Gemini AI connected</div>',
            unsafe_allow_html=True,
        )
    else:
        st.warning("Gemini key not configured — AI enrichment disabled", icon="⚠️")

weights = {
    "journal_if":        w_journal,
    "lh_timepoints":     w_lh,
    "tissue_site":       w_tissue,
    "relevance":         w_rel,
    "data_completeness": w_comp,
    "accessibility":     w_access,
}

st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1rem 0 1rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Search / Clear buttons
# ---------------------------------------------------------------------------
btn_col, clear_col, _ = st.columns([2, 1, 4])
with btn_col:
    search_clicked = st.button(
        "Search",
        type="primary",
        use_container_width=True,
        disabled=(not selected_dbs or not search_query.strip()),
    )
with clear_col:
    if st.button("Clear", use_container_width=True):
        st.session_state.se_results  = []
        st.session_state.se_approved = set()
        st.session_state.se_rejected = set()
        st.session_state.se_done     = False
        st.rerun()

# ---------------------------------------------------------------------------
# Execute search
# ---------------------------------------------------------------------------
if search_clicked and search_query.strip() and selected_dbs:
    st.session_state.se_results  = []
    st.session_state.se_approved = set()
    st.session_state.se_rejected = set()
    st.session_state.se_done     = False
    st.session_state.se_query    = search_query.strip()
    st.session_state.se_weights  = weights

    cache_dir = OUTPUT_DIR / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    prog_bar    = st.progress(0.0, text="Starting search…")
    status_slot = st.empty()
    n_dbs       = len(selected_dbs)

    for db_idx, db_key in enumerate(selected_dbs):
        status_slot.markdown(
            f'<div style="font-family:Arial,sans-serif;font-size:0.8rem;color:#6b7280">'
            f'Searching <strong>{db_key.upper()}</strong> '
            f'({db_idx + 1} / {n_dbs})…</div>',
            unsafe_allow_html=True,
        )

        if db_key not in SCRAPER_MAP:
            continue

        try:
            import importlib
            mod_path, cls_name = SCRAPER_MAP[db_key]
            mod   = importlib.import_module(mod_path)
            cls   = getattr(mod, cls_name)
            api_k = NCBI_KEY if db_key == "geo" else None
            scraper = cls(api_key=api_k, cache_dir=str(cache_dir))

            hits = scraper.search(search_query.strip())
            hits = hits[:int(max_per_db)]

        except Exception as exc:
            status_slot.warning(f"[{db_key.upper()}] scraper error: {exc}")
            prog_bar.progress((db_idx + 1) / n_dbs)
            continue

        seen_in_session = {r.get("accession") for r in st.session_state.se_results}

        for i, hit in enumerate(hits):
            acc = hit.get("accession") or hit.get("dataset_id") or ""
            if not acc or acc in seen_in_session:
                continue

            status_slot.markdown(
                f'<div style="font-family:Arial,sans-serif;font-size:0.78rem;color:#9ca3af">'
                f'[{db_key.upper()}] {acc} ({i + 1}/{len(hits)})'
                f' — {len(st.session_state.se_results)} results so far</div>',
                unsafe_allow_html=True,
            )

            # Fetch full metadata
            try:
                record = scraper.fetch_metadata(acc)
            except Exception:
                record = hit

            record["source_db"] = db_key
            seen_in_session.add(acc)

            # Gemini AI enrichment + relevance scoring
            if GEMINI_KEY:
                try:
                    from scoring.ai_extractor import extract_metadata_with_relevance
                    abstract  = record.get("abstract") or record.get("summary") or ""
                    title_str = record.get("title") or ""
                    journal   = (record.get("journal") or record.get("publication")
                                 or record.get("journal_name") or "")
                    ai_data = extract_metadata_with_relevance(
                        acc, title_str, abstract,
                        search_query.strip(), GEMINI_KEY, journal,
                    )
                    for k, v in ai_data.items():
                        existing = record.get(k)
                        if (v is not None and v != "" and v != []
                                and (existing is None or existing == ""
                                     or existing == [])):
                            record[k] = v
                    # Always take relevance fields (overwrite with fresh Gemini score)
                    for rk in ("relevance_score", "journal_if_estimate",
                               "full_text_available", "machine_platform",
                               "journal_name", "reasoning"):
                        if rk in ai_data:
                            record[rk] = ai_data[rk]
                    record["ai_enriched"] = True
                except Exception:
                    pass

            # Full-text filter
            if full_text_only and not record.get("full_text_available", True):
                continue

            # Weighted confidence score
            try:
                from scoring.confidence import score_with_weights
                from scoring.tiers import classify_tier
                cs = score_with_weights(record, weights)
                record["final_CS"]         = cs
                record["confidence_score"] = cs
                record["confidence_tier"]  = classify_tier(cs)
            except Exception:
                record["final_CS"]        = 0.0
                record["confidence_tier"] = "LOW_CONFIDENCE"

            # Min confidence filter
            if record.get("final_CS", 0) < min_conf:
                continue

            st.session_state.se_results.append(record)

        prog_bar.progress(
            (db_idx + 1) / n_dbs,
            text=f"Searched {db_idx + 1}/{n_dbs} databases · {len(st.session_state.se_results)} results",
        )

    prog_bar.progress(1.0,
                      text=f"Complete · {len(st.session_state.se_results)} datasets found")
    status_slot.empty()
    st.session_state.se_done = True

    # Persist results to output (for Browse/Download pages)
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / "search_results.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(st.session_state.se_results, fh,
                      indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------
results: list[dict] = st.session_state.se_results

if results:
    n_app = len(st.session_state.se_approved)
    n_rej = len(st.session_state.se_rejected)
    n_pen = len(results) - n_app - n_rej

    st.markdown(
        f'<div class="section-label">Results — {len(results)} datasets found</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af;'
        f'margin-bottom:1rem">'
        f'<span style="color:#16A34A;font-weight:700">{n_app} approved</span>'
        f' &nbsp;·&nbsp; '
        f'<span style="color:#DC2626;font-weight:700">{n_rej} rejected</span>'
        f' &nbsp;·&nbsp; {n_pen} pending</div>',
        unsafe_allow_html=True,
    )

    # Sort by confidence score
    sorted_results = sorted(
        results,
        key=lambda x: float(x.get("final_CS", 0)),
        reverse=True,
    )

    for rec in sorted_results:
        acc       = rec.get("accession") or rec.get("dataset_id") or "Unknown"
        title     = rec.get("title") or rec.get("Title") or acc
        score     = float(rec.get("final_CS", 0))
        tier      = rec.get("confidence_tier", "LOW_CONFIDENCE")
        modality  = rec.get("modality") or "—"
        source_db = rec.get("source_db", "")
        platform  = rec.get("machine_platform") or rec.get("platform") or "—"
        tissues   = rec.get("tissue_sites") or rec.get("sub_compartments") or []
        tissue_str = ", ".join(str(t) for t in tissues[:3]) or "—"
        ai_summary = rec.get("reasoning") or ""
        abstract  = rec.get("abstract") or rec.get("summary") or ""
        rel_score = rec.get("relevance_score") or 0
        journal   = rec.get("journal_name") or rec.get("journal") or ""
        lh_tps    = rec.get("lh_timepoints") or []
        disease_g = rec.get("disease_groups") or []
        ai_enriched = rec.get("ai_enriched", False)

        is_approved = acc in st.session_state.se_approved
        is_rejected = acc in st.session_state.se_rejected
        card_mod = "approved" if is_approved else ("rejected" if is_rejected else "")

        db_url = _db_url(acc, source_db)
        acc_link = (
            f'<a href="{db_url}" target="_blank" style="font-family:monospace;'
            f'font-size:0.78rem;color:#00539B;background:#EFF6FF;'
            f'padding:2px 6px;border-radius:2px;text-decoration:none">{acc}</a>'
            if db_url else
            f'<span style="font-family:monospace;font-size:0.78rem;color:#6b7280;'
            f'background:#f3f4f6;padding:2px 6px;border-radius:2px">{acc}</span>'
        )
        ai_tag = (
            '<span style="background:#EFF6FF;color:#1D4ED8;padding:1px 6px;'
            'border-radius:2px;font-size:0.65rem;font-weight:700;'
            'font-family:Arial;letter-spacing:0.03em">AI</span>'
            if ai_enriched else ""
        )

        # Build metadata chips
        chips = ""
        if source_db:
            chips += f'<span class="meta-chip">{source_db.upper()}</span>'
        if modality and modality != "—":
            chips += f'<span class="meta-chip">{modality}</span>'
        if tissue_str and tissue_str != "—":
            chips += f'<span class="meta-chip">{tissue_str}</span>'
        if platform and platform != "—":
            chips += f'<span class="meta-chip">{platform}</span>'
        if lh_tps:
            tp_str = ", ".join(str(t) for t in lh_tps[:4])
            chips += f'<span class="meta-chip">⏱ {tp_str}</span>'
        if disease_g:
            dg_str = ", ".join(str(d) for d in disease_g[:2])
            chips += f'<span class="meta-chip">🩺 {dg_str}</span>'

        # AI summary or abstract fallback
        body_html = ""
        if ai_summary:
            body_html = (
                f'<div style="font-size:0.79rem;color:#374151;line-height:1.6;'
                f'font-style:italic;margin:0.45rem 0 0.35rem 0">{ai_summary}</div>'
            )
        elif abstract:
            preview = abstract[:320] + ("…" if len(abstract) > 320 else "")
            body_html = (
                f'<div style="font-size:0.79rem;color:#6b7280;line-height:1.6;'
                f'margin:0.45rem 0 0.35rem 0">{preview}</div>'
            )

        journal_html = (
            f'<div style="font-size:0.68rem;color:#9ca3af;margin-top:0.2rem">'
            f'Published in: {journal}</div>'
            if journal else ""
        )

        st.markdown(
            f'<div class="result-card {card_mod}">'
            f'<div style="display:flex;justify-content:space-between;'
            f'align-items:center;margin-bottom:0.35rem">'
            f'<div style="display:flex;align-items:center;gap:6px">'
            f'{acc_link} {_score_pill(score)} {_tier_pill(tier)} {ai_tag}</div>'
            f'<div style="font-size:0.7rem;color:#9ca3af">'
            f'Relevance {rel_score}/100</div>'
            f'</div>'
            f'<div style="font-size:0.88rem;font-weight:700;color:#012169;'
            f'line-height:1.35;margin-bottom:0.3rem">'
            f'{title[:140]}{"…" if len(title) > 140 else ""}</div>'
            f'{chips and ("<div style=\\"margin:0.3rem 0\\">" + chips + "</div>") or ""}'
            f'{body_html}'
            f'{journal_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Action buttons
        b1, b2, b3, _ = st.columns([1, 1, 1, 8])
        if is_approved:
            b1.button("✓ Approved", key=f"a_{acc}", disabled=True,
                      use_container_width=True)
            b2.button("↩ Undo", key=f"u_{acc}", on_click=_undo, args=(acc,),
                      use_container_width=True)
        elif is_rejected:
            b1.button("↩ Undo", key=f"u_{acc}", on_click=_undo, args=(acc,),
                      use_container_width=True)
            b2.button("✗ Rejected", key=f"r_{acc}", disabled=True,
                      use_container_width=True)
        else:
            b1.button("✓ Approve", key=f"a_{acc}", type="primary",
                      on_click=_approve, args=(acc,), use_container_width=True)
            b2.button("✗ Reject", key=f"r_{acc}",
                      on_click=_reject, args=(acc,), use_container_width=True)

elif st.session_state.se_done:
    st.info(
        "No results matched your criteria. Try a broader query, "
        "add more databases, or lower the minimum confidence score.",
    )

# ---------------------------------------------------------------------------
# Approved datasets panel
# ---------------------------------------------------------------------------
if st.session_state.se_approved:
    import pandas as pd

    approved_records = [
        r for r in results
        if (r.get("accession") or r.get("dataset_id")) in st.session_state.se_approved
    ]

    st.markdown(
        '<div style="height:1px;background:#e5e7eb;margin:1.5rem 0 1rem 0"></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="section-label">Approved Datasets '
        f'({len(approved_records)})</div>',
        unsafe_allow_html=True,
    )

    rows = []
    for r in approved_records:
        acc = r.get("accession") or r.get("dataset_id") or ""
        db_url = _db_url(acc, r.get("source_db", ""))
        rows.append({
            "Accession": acc,
            "Title":     (r.get("title") or "")[:70],
            "Modality":  r.get("modality") or "—",
            "Tissue":    ", ".join((r.get("tissue_sites") or [])[:2]) or "—",
            "LH":        ", ".join((r.get("lh_timepoints") or [])[:3]) or "—",
            "Score":     round(float(r.get("final_CS", 0)), 1),
            "Tier":      r.get("confidence_tier", "—"),
            "Source":    r.get("source_db", "").upper(),
            "URL":       r.get("download_url") or r.get("url") or db_url or "",
        })
    df_ap = pd.DataFrame(rows)
    st.dataframe(
        df_ap,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.NumberColumn("Score", format="%.1f"),
            "URL":   st.column_config.LinkColumn("URL", width="medium"),
        },
    )

    # Save to JSON
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_DIR / "approved_datasets.json", "w", encoding="utf-8") as fh:
            json.dump(approved_records, fh, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass

    # Download script generation
    st.markdown(
        '<div style="font-size:0.6rem;font-weight:700;letter-spacing:0.1em;'
        'text-transform:uppercase;color:#9ca3af;margin:0.9rem 0 0.5rem 0">'
        'Generate Download Scripts</div>',
        unsafe_allow_html=True,
    )

    def _build_sh(recs: list[dict]) -> str:
        lines = [
            "#!/usr/bin/env bash",
            "# Hickey Lab Genomics Search Engine — Approved Datasets",
            "# Hickey Lab · Duke University",
            "",
            'OUTPUT_DIR="${1:-./downloads}"',
            'mkdir -p "$OUTPUT_DIR"',
            "",
        ]
        for r in recs:
            acc = r.get("accession") or r.get("dataset_id") or "UNKNOWN"
            url = r.get("download_url") or r.get("url") or ""
            title = (r.get("title") or "")[:60]
            lines.append(f"# {acc}: {title}")
            if r.get("controlled_access"):
                lines.append(
                    f"# CONTROLLED ACCESS: {acc} — apply via dbGaP/EGA"
                )
            elif url:
                lines.append(f'wget -c -P "$OUTPUT_DIR" "{url}"  # {acc}')
            else:
                db_url_fb = _db_url(acc, r.get("source_db", ""))
                lines.append(
                    f"# {acc}: no direct URL"
                    + (f" — see {db_url_fb}" if db_url_fb else "")
                )
            lines.append("")
        lines.append("echo 'Done.'")
        return "\n".join(lines)

    def _build_py(recs: list[dict]) -> str:
        lines = [
            '"""Hickey Lab Genomics Search Engine — Approved Datasets Download"""',
            "",
            "import os, subprocess, sys",
            "",
            'OUTPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "./downloads"',
            "os.makedirs(OUTPUT_DIR, exist_ok=True)",
            "",
            "datasets = [",
        ]
        for r in recs:
            acc = r.get("accession") or r.get("dataset_id") or ""
            url = r.get("download_url") or r.get("url") or ""
            ctrl = bool(r.get("controlled_access"))
            lines.append(
                f'    {{"accession": {acc!r}, "url": {url!r},'
                f' "controlled": {ctrl!r}}},'
            )
        lines += [
            "]",
            "",
            "for ds in datasets:",
            '    if ds["controlled"]:',
            "        print(f\"SKIP {ds['accession']}: controlled access\")",
            "        continue",
            '    if not ds["url"]:',
            "        print(f\"SKIP {ds['accession']}: no URL\")",
            "        continue",
            '    fname = os.path.basename(ds["url"]) or ds["accession"]',
            '    dest  = os.path.join(OUTPUT_DIR, fname)',
            "    print(f\"Downloading {ds['accession']} → {dest}\")",
            '    subprocess.run(["wget", "-c", "-O", dest, ds["url"]], check=True)',
            "",
            "print('Done.')",
        ]
        return "\n".join(lines)

    sh_content = _build_sh(approved_records)
    py_content = _build_py(approved_records)

    dc1, dc2 = st.columns(2)
    with dc1:
        st.download_button(
            "Download Script (.sh)",
            data=sh_content.encode(),
            file_name="approved_download.sh",
            mime="text/x-sh",
            use_container_width=True,
            type="primary",
        )
        st.caption("Bash — `bash approved_download.sh [output_dir]`")
    with dc2:
        st.download_button(
            "Download Script (.py)",
            data=py_content.encode(),
            file_name="approved_download.py",
            mime="text/x-python",
            use_container_width=True,
        )
        st.caption("Python — `python approved_download.py [output_dir]`")

    with st.expander("Preview shell script"):
        st.code(sh_content, language="bash")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    '<div style="margin-top:3rem;height:1px;background:#e5e7eb"></div>',
    unsafe_allow_html=True,
)
st.markdown("""
<div style="font-family:Arial,sans-serif;font-size:0.7rem;color:#9ca3af;
            text-align:center;padding:0.85rem 0;line-height:1.85">
    <strong style="color:#6b7280">Hickey Lab</strong>
    &nbsp;·&nbsp; Department of Biomedical Engineering &nbsp;·&nbsp; Duke University<br>
    Built by Koravit (Auggie) Poysungnoen
    &nbsp;·&nbsp; Department of Biological Sciences
    &amp; Department of Financial Economics
</div>
""", unsafe_allow_html=True)
