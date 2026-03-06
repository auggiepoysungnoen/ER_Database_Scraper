"""
pages/03_Download.py
====================
Download manager page. Shows approved datasets and generates download scripts.
Also displays the pipeline download_manifest.sh if present.
"""

import json
import sys
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
    page_title="Downloads - Hickey Lab",
    layout="wide",
    page_icon="\U0001f52c",
)

st.markdown(
    """
    <style>
    .block-container { max-width: 1140px; }
    h1, h2, h3 { font-family: Arial, sans-serif; color: #012169; }
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
# Main content
# ---------------------------------------------------------------------------
st.title("Downloads")

# Section 1: Approved datasets
st.subheader("Approved Datasets")

approved_path = _OUTPUT_DIR / "approved_datasets.json"
if approved_path.exists():
    with open(approved_path, "r", encoding="utf-8") as fh:
        approved = json.load(fh)

    if approved:
        import pandas as pd

        display_cols = ["accession", "title", "source_db", "modality", "confidence_score", "confidence_tier"]
        rows = []
        for r in approved:
            rows.append({c: r.get(c, "") for c in display_cols})
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Generate wget script
        sh_lines = ["#!/usr/bin/env bash", "# Download script for approved datasets", "set -euo pipefail", ""]
        for r in approved:
            url = r.get("download_url", "") or r.get("url", "")
            acc = r.get("accession", "unknown")
            if url:
                sh_lines.append(f"# {acc}")
                sh_lines.append(f'wget -c "{url}" -P downloads/{acc}/')
                sh_lines.append("")
        sh_content = "\n".join(sh_lines)
        st.download_button(
            "Download wget script (.sh)",
            data=sh_content,
            file_name="download_approved.sh",
            mime="text/x-shellscript",
        )

        # Generate Python script
        py_lines = [
            '"""Download approved datasets."""',
            "import os, urllib.request",
            "",
            "DATASETS = [",
        ]
        for r in approved:
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
        st.download_button(
            "Download Python script (.py)",
            data=py_content,
            file_name="download_approved.py",
            mime="text/x-python",
        )
    else:
        st.info("approved_datasets.json exists but is empty.")
else:
    st.info(
        "No approved datasets found. Use the Search Engine to approve datasets, "
        "or they will appear here after saving."
    )

# Section 2: Pipeline download manifest
st.divider()
st.subheader("Pipeline Download Manifest")

manifest_path = _OUTPUT_DIR / "download_manifest.sh"
if manifest_path.exists():
    manifest_content = manifest_path.read_text(encoding="utf-8")
    st.code(manifest_content, language="bash")
    st.download_button(
        "Download manifest (.sh)",
        data=manifest_content,
        file_name="download_manifest.sh",
        mime="text/x-shellscript",
    )
else:
    st.info("No download_manifest.sh found in the output directory.")

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
