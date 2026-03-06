"""
pages/00_Search_Engine.py
=========================
Real-time search engine across open genomics databases.

Imports scrapers dynamically, scores results with Gemini AI, and lets
users approve/reject datasets for downstream download.
"""

import importlib
import json
import sys
import time
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_APP_DIR = Path(__file__).resolve().parent.parent
_SCRIPT_DIR = _APP_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parent
_OUTPUT_DIR = _REPO_ROOT / "output"

sys.path.insert(0, str(_APP_DIR))
sys.path.insert(0, str(_SCRIPT_DIR))

from auth import check_password  # noqa: E402

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Search Engine - Hickey Lab",
    layout="wide",
    page_icon="\U0001f52c",
)

st.markdown(
    """
    <style>
    .block-container { max-width: 1140px; }
    h1, h2, h3 { font-family: Arial, sans-serif; color: #012169; }
    .result-card {
        background: #fff;
        border-top: 3px solid #00539B;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .pill {
        display: inline-block; padding: 2px 10px; border-radius: 12px;
        font-size: 0.78rem; font-weight: 600; margin-right: 6px;
    }
    .pill-gold { background: #FFF8DC; color: #B8860B; }
    .pill-silver { background: #F0F0F0; color: #555; }
    .pill-bronze { background: #FFF0E0; color: #8B4513; }
    .pill-low { background: #F5F5F5; color: #999; }
    .pill-score { background: #E8F0FE; color: #00539B; }
    .pill-ai { background: #E8F5E9; color: #2E7D32; }
    .chip {
        display: inline-block; background: #f0f2f6; color: #333;
        padding: 2px 8px; border-radius: 6px; font-size: 0.75rem;
        margin: 2px 2px;
    }
    .footer-text {
        text-align: center; color: #888; font-size: 0.8rem;
        margin-top: 3rem; padding-top: 1rem;
        border-top: 1px solid #eee;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar
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
# Session state defaults
# ---------------------------------------------------------------------------
_DEFAULTS = {
    "se_results": [],
    "se_approved": set(),
    "se_rejected": set(),
    "se_done": False,
    "se_query": "",
}
for key, default in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ---------------------------------------------------------------------------
# Database <-> scraper module mapping
# ---------------------------------------------------------------------------
_DB_MAP = {
    "geo": ("scrapers.geo", "GEOScraper"),
    "arrayexpress": ("scrapers.arrayexpress", "ArrayExpressScraper"),
    "cellxgene": ("scrapers.cellxgene", "CellxGeneScraper"),
    "hca": ("scrapers.hca", "HCAScraper"),
    "scp": ("scrapers.singlecellportal", "SingleCellPortalScraper"),
    "zenodo": ("scrapers.zenodo", "ZenodoScraper"),
    "figshare": ("scrapers.figshare", "FigshareScraper"),
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Search Engine")
st.markdown("Search open genomics databases in real time, score with AI, and approve datasets.")

# ---------------------------------------------------------------------------
# Config panel
# ---------------------------------------------------------------------------
st.subheader("Configuration")
col_left, col_mid, col_right = st.columns([1, 1, 1])

with col_left:
    selected_dbs = st.multiselect(
        "Databases",
        options=list(_DB_MAP.keys()),
        default=["geo"],
        help="Select which databases to search.",
    )
    search_query = st.text_area(
        "Search query (natural language)",
        value=st.session_state["se_query"],
        height=100,
        placeholder="e.g. single-cell RNA-seq endometrium window of implantation",
    )
    require_full_text = st.checkbox("Require full paper text", value=False)

with col_mid:
    st.markdown("**Dimension Weights** (normalised internally)")
    w_journal = st.slider("Journal IF", 0, 100, 50, key="w_journal")
    w_lh = st.slider("LH Timepoints", 0, 100, 50, key="w_lh")
    w_tissue = st.slider("Tissue Site", 0, 100, 50, key="w_tissue")
    w_relevance = st.slider("Relevance", 0, 100, 70, key="w_relevance")
    w_completeness = st.slider("Data Completeness", 0, 100, 50, key="w_completeness")
    w_access = st.slider("Accessibility", 0, 100, 50, key="w_access")
    total_weight = w_journal + w_lh + w_tissue + w_relevance + w_completeness + w_access
    st.caption(f"Total weight: {total_weight} (normalised to 1.0)")

with col_right:
    min_confidence = st.slider("Min confidence score", 0, 80, 25)
    max_results = st.slider("Max results per database", 5, 300, 40)

# ---------------------------------------------------------------------------
# Search execution
# ---------------------------------------------------------------------------
run_search = st.button("Run Search", type="primary", use_container_width=True)

if run_search and search_query.strip():
    st.session_state["se_query"] = search_query.strip()
    st.session_state["se_results"] = []
    st.session_state["se_approved"] = set()
    st.session_state["se_rejected"] = set()
    st.session_state["se_done"] = False

    weights = {
        "journal_if": w_journal,
        "lh_timepoints": w_lh,
        "tissue_site": w_tissue,
        "relevance": w_relevance,
        "data_completeness": w_completeness,
        "accessibility": w_access,
    }

    gemini_key = ""
    try:
        gemini_key = st.secrets["gemini"]["api_key"]
    except Exception:
        pass

    ncbi_key = ""
    try:
        ncbi_key = st.secrets["ncbi"]["api_key"]
    except Exception:
        pass

    all_results = []
    progress = st.progress(0, text="Starting search...")
    total_dbs = len(selected_dbs)

    for db_idx, db_name in enumerate(selected_dbs):
        frac = db_idx / max(total_dbs, 1)
        progress_text = f"Searching {db_name}... ({db_idx + 1}/{total_dbs})"
        progress.progress(frac, text=progress_text)

        mod_path, cls_name = _DB_MAP[db_name]

        try:
            mod = importlib.import_module(mod_path)
            scraper_cls = getattr(mod, cls_name)

            # Construct scraper with appropriate API key
            if db_name == "geo" and ncbi_key:
                scraper = scraper_cls(api_key=ncbi_key)
            else:
                scraper = scraper_cls()

            hits = scraper.search(search_query.strip(), max_results=max_results)
        except Exception as exc:
            st.warning(f"Error searching {db_name}: {exc}")
            continue

        for hit_idx, hit in enumerate(hits):
            sub_frac = frac + (hit_idx / max(len(hits), 1)) / max(total_dbs, 1)
            progress.progress(min(sub_frac, 0.99), text=f"Processing {db_name} result {hit_idx + 1}/{len(hits)}...")

            accession = hit.get("accession", "")
            if not accession:
                continue

            # Fetch full metadata
            try:
                meta = scraper.fetch_metadata(accession)
            except Exception:
                meta = hit

            # AI enrichment
            title = meta.get("title", "") or ""
            abstract = meta.get("abstract", "") or meta.get("summary", "") or ""

            if gemini_key and (title or abstract):
                try:
                    from scoring.ai_extractor import extract_metadata_with_relevance
                    ai_data = extract_metadata_with_relevance(
                        accession, title, abstract,
                        search_query.strip(), gemini_key,
                        journal_name=meta.get("journal", ""),
                    )
                    meta.update({k: v for k, v in ai_data.items() if v is not None and v != [] and v != ""})
                    meta["ai_enriched"] = True
                except Exception:
                    pass

            # Filter: full text
            if require_full_text and not meta.get("full_text_available", False):
                continue

            # Score with weights
            try:
                from scoring.confidence import score_with_weights
                conf_score = score_with_weights(meta, weights)
            except Exception:
                conf_score = 0.0

            if conf_score < min_confidence:
                continue

            meta["confidence_score"] = conf_score

            # Assign tier
            try:
                from scoring.tiers import classify_tier
                meta["confidence_tier"] = classify_tier(conf_score)
            except Exception:
                if conf_score >= 80:
                    meta["confidence_tier"] = "GOLD"
                elif conf_score >= 60:
                    meta["confidence_tier"] = "SILVER"
                elif conf_score >= 40:
                    meta["confidence_tier"] = "BRONZE"
                else:
                    meta["confidence_tier"] = "LOW_CONFIDENCE"

            meta["source_db"] = meta.get("source_db", db_name)
            all_results.append(meta)

            # Rate limit between items
            time.sleep(0.05)

    progress.progress(1.0, text="Search complete!")
    all_results.sort(key=lambda r: r.get("confidence_score", 0), reverse=True)
    st.session_state["se_results"] = all_results
    st.session_state["se_done"] = True
    st.rerun()

elif run_search and not search_query.strip():
    st.warning("Please enter a search query.")

# ---------------------------------------------------------------------------
# Callback factories for approve / reject / undo
# ---------------------------------------------------------------------------


def _approve(acc: str):
    """Callback: move accession to approved set."""
    st.session_state["se_approved"].add(acc)
    st.session_state["se_rejected"].discard(acc)


def _reject(acc: str):
    """Callback: move accession to rejected set."""
    st.session_state["se_rejected"].add(acc)
    st.session_state["se_approved"].discard(acc)


def _undo(acc: str):
    """Callback: remove accession from both sets."""
    st.session_state["se_approved"].discard(acc)
    st.session_state["se_rejected"].discard(acc)


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------
results = st.session_state["se_results"]
if results:
    st.subheader(f"Results ({len(results)})")

    for rec in results:
        acc = rec.get("accession", "unknown")
        score = rec.get("confidence_score", 0)
        tier = rec.get("confidence_tier", "LOW_CONFIDENCE")
        title = rec.get("title", "Untitled") or "Untitled"
        abstract_text = rec.get("abstract", "") or rec.get("summary", "") or ""
        db = rec.get("source_db", "")
        modality = rec.get("modality", "Unknown") or "Unknown"
        tissue_list = rec.get("tissue_sites", []) or rec.get("sub_compartments", []) or []
        platform = rec.get("platform", "") or rec.get("machine_platform", "") or ""
        lh_tps = rec.get("lh_timepoints", []) or []
        reasoning = rec.get("reasoning", "") or ""
        journal = rec.get("journal", "") or rec.get("journal_name", "") or ""
        ai_flag = rec.get("ai_enriched", False)

        # Determine status
        is_approved = acc in st.session_state["se_approved"]
        is_rejected = acc in st.session_state["se_rejected"]

        # Tier pill class
        tier_lower = tier.lower().replace("_confidence", "")
        pill_cls = f"pill-{tier_lower}" if tier_lower in ("gold", "silver", "bronze", "low") else "pill-low"

        # Build chips HTML
        chips_parts = []
        if db:
            chips_parts.append(f'<span class="chip">{db}</span>')
        if modality:
            chips_parts.append(f'<span class="chip">{modality}</span>')
        if isinstance(tissue_list, list):
            for t in tissue_list[:3]:
                chips_parts.append(f'<span class="chip">{t}</span>')
        if platform:
            chips_parts.append(f'<span class="chip">{platform}</span>')
        if isinstance(lh_tps, list):
            for tp in lh_tps[:4]:
                chips_parts.append(f'<span class="chip">{tp}</span>')
        chips_html = " ".join(chips_parts)

        # Preview text
        preview = reasoning if reasoning else abstract_text
        if len(preview) > 300:
            preview = preview[:297] + "..."

        # AI badge
        ai_badge = '<span class="pill pill-ai">AI</span>' if ai_flag else ""

        # Journal line
        journal_html = f"<div style='color:#666; font-size:0.8rem; margin-top:4px;'>{journal}</div>" if journal else ""

        # Card border color
        border_color = "#00539B"
        if is_approved:
            border_color = "#2E7D32"
        elif is_rejected:
            border_color = "#c0c0c0"

        card_opacity = "0.5" if is_rejected else "1.0"

        st.markdown(
            f'<div class="result-card" style="border-top-color:{border_color}; opacity:{card_opacity};">'
            f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:6px;">'
            f'<strong>{acc}</strong>'
            f'<span class="pill pill-score">{score:.1f}</span>'
            f'<span class="pill {pill_cls}">{tier}</span>'
            f'{ai_badge}'
            f'</div>'
            f'<div style="font-size:0.95rem; font-weight:500; margin-bottom:4px;">{title}</div>'
            f'<div style="margin-bottom:6px;">{chips_html}</div>'
            f'<div style="color:#444; font-size:0.85rem;">{preview}</div>'
            f'{journal_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Action buttons
        btn_cols = st.columns([1, 1, 1, 5])
        with btn_cols[0]:
            if not is_approved:
                st.button("Approve", key=f"appr_{acc}", on_click=_approve, args=(acc,))
            else:
                st.success("Approved")
        with btn_cols[1]:
            if not is_rejected:
                st.button("Reject", key=f"rej_{acc}", on_click=_reject, args=(acc,))
            else:
                st.warning("Rejected")
        with btn_cols[2]:
            if is_approved or is_rejected:
                st.button("Undo", key=f"undo_{acc}", on_click=_undo, args=(acc,))

# ---------------------------------------------------------------------------
# Approved panel
# ---------------------------------------------------------------------------
approved_accs = st.session_state.get("se_approved", set())
if approved_accs and results:
    st.divider()
    st.subheader("Approved Datasets")

    approved_records = [r for r in results if r.get("accession") in approved_accs]

    if approved_records:
        import pandas as pd

        display_cols = ["accession", "title", "source_db", "modality", "confidence_score", "confidence_tier"]
        rows = []
        for r in approved_records:
            rows.append({c: r.get(c, "") for c in display_cols})
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download scripts
        col_dl1, col_dl2, col_dl3 = st.columns(3)

        # Shell script
        sh_lines = ["#!/usr/bin/env bash", "# Auto-generated download script", "set -euo pipefail", ""]
        for r in approved_records:
            url = r.get("download_url", "") or r.get("url", "")
            acc = r.get("accession", "unknown")
            if url:
                sh_lines.append(f"# {acc}")
                sh_lines.append(f'wget -c "{url}" -P downloads/{acc}/')
                sh_lines.append("")
        sh_content = "\n".join(sh_lines)
        col_dl1.download_button(
            "Download .sh script",
            data=sh_content,
            file_name="download_approved.sh",
            mime="text/x-shellscript",
        )

        # Python script
        py_lines = [
            '"""Auto-generated Python download script."""',
            "import os, urllib.request",
            "",
            "DATASETS = [",
        ]
        for r in approved_records:
            url = r.get("download_url", "") or r.get("url", "")
            acc = r.get("accession", "unknown")
            py_lines.append(f'    {{"accession": "{acc}", "url": "{url}"}},')
        py_lines.append("]")
        py_lines.append("")
        py_lines.append("for ds in DATASETS:")
        py_lines.append('    if not ds["url"]:')
        py_lines.append("        continue")
        py_lines.append('    out_dir = os.path.join("downloads", ds["accession"])')
        py_lines.append("    os.makedirs(out_dir, exist_ok=True)")
        py_lines.append('    fname = ds["url"].split("/")[-1] or "data"')
        py_lines.append("    dest = os.path.join(out_dir, fname)")
        py_lines.append('    print(f"Downloading {ds[\'accession\']} -> {dest}")')
        py_lines.append('    urllib.request.urlretrieve(ds["url"], dest)')
        py_lines.append('print("Done.")')
        py_content = "\n".join(py_lines)
        col_dl2.download_button(
            "Download .py script",
            data=py_content,
            file_name="download_approved.py",
            mime="text/x-python",
        )

        # Save to output
        def _save_approved():
            save_path = _OUTPUT_DIR / "approved_datasets.json"
            _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            # Convert sets to lists for JSON serialization
            serializable = []
            for r in approved_records:
                clean = {}
                for k, v in r.items():
                    if isinstance(v, set):
                        clean[k] = list(v)
                    else:
                        clean[k] = v
                serializable.append(clean)
            with open(save_path, "w", encoding="utf-8") as fh:
                json.dump(serializable, fh, indent=2, ensure_ascii=False, default=str)
            st.success(f"Saved {len(serializable)} datasets to {save_path}")

        col_dl3.button("Save to output/approved_datasets.json", on_click=_save_approved)

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
