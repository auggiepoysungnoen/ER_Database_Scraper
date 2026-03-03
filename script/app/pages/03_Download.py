"""
Download Manager — Endometrial Receptivity Database
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Downloads | Endometrial Receptivity DB",
    layout="wide",
    page_icon="⬇️",
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
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1200px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.25rem 0!important}
.stButton>button{border-radius:2px;font-weight:500}
[data-testid="stDataFrame"]{border:1px solid #e5e7eb;border-radius:2px}
details{border:1px solid #e5e7eb!important;border-radius:2px!important}
details summary{font-weight:600;font-size:0.88rem;color:#012169}
.section-label{font-family:Arial,sans-serif;font-size:0.6rem;font-weight:700;
               letter-spacing:0.12em;text-transform:uppercase;color:#9ca3af;
               margin-bottom:0.7rem;padding-bottom:0.35rem;border-bottom:1px solid #e5e7eb}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_downloadable_datasets() -> pd.DataFrame:
    if not REGISTRY_PATH.exists():
        return pd.DataFrame()
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "datasets" in raw:
        items = raw["datasets"]
    elif isinstance(raw, list):
        items = raw
    else:
        items = list(raw.values())
    rows = []
    for item in items:
        tier = (item.get("confidence_tier") or item.get("tier") or "").upper()
        if tier not in ("GOLD", "SILVER"):
            continue
        acc = (
            item.get("accession") or item.get("Accession")
            or item.get("dataset_id") or item.get("ID") or ""
        )
        rows.append({
            "accession":        acc,
            "title":            item.get("title") or item.get("Title") or acc,
            "modality":         item.get("modality") or item.get("Modality") or "",
            "tier":             tier,
            "file_size_gb":     item.get("file_size_gb") or item.get("size_gb"),
            "download_url":     item.get("download_url") or item.get("url") or item.get("ftp_url") or "",
            "controlled_access": str(
                item.get("controlled_access") or item.get("access_type") or ""
            ).lower() in ("true", "yes", "controlled", "restricted", "1"),
            "md5":     item.get("md5") or item.get("checksum") or "",
            "platform": item.get("platform") or item.get("Platform") or "",
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fmt_size(gb) -> str:
    if gb is None or (isinstance(gb, float) and pd.isna(gb)):
        return "—"
    try:
        gb = float(gb)
        return f"{gb:.1f} GB" if gb >= 1 else f"{gb*1024:.0f} MB"
    except (ValueError, TypeError):
        return str(gb)

def _parse_gb(size_str: str) -> float:
    if not size_str or size_str in ("—", "Unknown"):
        return 0.0
    try:
        if "GB" in size_str:
            return float(size_str.replace("GB", "").strip())
        if "MB" in size_str:
            return float(size_str.replace("MB", "").strip()) / 1024
    except ValueError:
        pass
    return 0.0

def _shell_script(selected: pd.DataFrame) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "# Endometrial Receptivity Database — Download Script",
        "# Hickey Lab | Duke University",
        "",
        'OUTPUT_DIR="${1:-./downloads}"',
        'mkdir -p "$OUTPUT_DIR"',
        "",
    ]
    for modality, grp in selected.groupby("modality"):
        lines.append(f"# ── {modality} ────────────────────────────────────────────")
        for _, row in grp.iterrows():
            acc, url, md5 = row["accession"], row["download_url"], row.get("md5", "")
            if row.get("controlled_access"):
                lines.append(f"# CONTROLLED ACCESS: {acc} — request via dbGaP/EGA")
            else:
                if md5:
                    lines.append(f"# MD5: {md5}")
                if url:
                    lines.append(f'wget -c -P "$OUTPUT_DIR" "{url}"  # {acc}')
                else:
                    lines.append(f"# {acc}: no direct download URL")
        lines.append("")
    lines.append("echo 'Download complete.'")
    return "\n".join(lines)

def _python_script(selected: pd.DataFrame) -> str:
    lines = [
        '"""Endometrial Receptivity Database — Python Download Script',
        "Hickey Lab | Duke University",
        '"""',
        "",
        "import os, subprocess, sys",
        "",
        'OUTPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "./downloads"',
        "os.makedirs(OUTPUT_DIR, exist_ok=True)",
        "",
        "datasets = [",
    ]
    for _, row in selected.iterrows():
        lines.append(
            f'    {{"accession": {row["accession"]!r}, "url": {row["download_url"]!r}, '
            f'"controlled": {row.get("controlled_access", False)!r}, '
            f'"md5": {row.get("md5", "")!r}}},'
        )
    lines += [
        "]",
        "",
        "for ds in datasets:",
        '    if ds["controlled"]:',
        '        print(f"SKIP {ds[\'accession\']}: controlled access")',
        "        continue",
        '    if not ds["url"]:',
        '        print(f"SKIP {ds[\'accession\']}: no URL")',
        "        continue",
        '    dest = os.path.join(OUTPUT_DIR, os.path.basename(ds["url"]))',
        '    print(f"Downloading {ds[\'accession\']} → {dest}")',
        '    subprocess.run(["wget", "-c", "-O", dest, ds["url"]], check=True)',
        "",
        "print('Done.')",
    ]
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------
st.markdown("""
<p style="font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
          color:#012169;letter-spacing:-0.02em;margin-bottom:0">
    Download Manager
</p>
<p style="font-family:Arial,sans-serif;font-size:0.875rem;color:#6b7280;margin-top:0.25rem">
    Select GOLD and SILVER datasets to generate wget download scripts.
    Controlled-access datasets require a separate dbGaP or EGA application.
</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 1.5rem 0"></div>',
    unsafe_allow_html=True,
)

df = load_downloadable_datasets()

if not REGISTRY_PATH.exists():
    st.warning(
        "Registry not found. Run the pipeline from the **⚙ Run Pipeline** page.",
        icon="⚠️",
    )
    st.stop()

if df.empty:
    st.info("No GOLD or SILVER datasets found in the registry.")
    st.stop()

# ---------------------------------------------------------------------------
# Dataset table
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Available Datasets</div>', unsafe_allow_html=True)

df_display = df.copy()
df_display.insert(0, "Select", False)
df_display["file_size_gb"] = df_display["file_size_gb"].apply(_fmt_size)

df_display = df_display.rename(columns={
    "accession":        "Accession",
    "title":            "Title",
    "modality":         "Modality",
    "tier":             "Tier",
    "file_size_gb":     "File Size",
    "download_url":     "URL",
    "controlled_access":"Controlled Access",
    "platform":         "Platform",
})

col_config = {
    "Select":           st.column_config.CheckboxColumn("Select", default=False, width="small"),
    "Accession":        st.column_config.TextColumn("Accession", width="small"),
    "Title":            st.column_config.TextColumn("Title", width="large"),
    "Modality":         st.column_config.TextColumn("Modality", width="medium"),
    "Tier":             st.column_config.TextColumn("Tier", width="small"),
    "File Size":        st.column_config.TextColumn("File Size", width="small"),
    "URL":              st.column_config.LinkColumn("URL", width="medium"),
    "Controlled Access":st.column_config.CheckboxColumn("Controlled", disabled=True, width="small"),
    "Platform":         st.column_config.TextColumn("Platform", width="medium"),
}

edited = st.data_editor(
    df_display,
    column_config=col_config,
    use_container_width=True,
    hide_index=True,
    disabled=[c for c in df_display.columns if c != "Select"],
    key="download_editor",
)

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
selected_mask   = edited["Select"].fillna(False)
selected_mask  &= ~edited["Controlled Access"].fillna(False)
n_selected      = int(selected_mask.sum())
n_controlled    = int(
    (edited["Select"].fillna(False) & edited["Controlled Access"].fillna(False)).sum()
)
selected_df     = df[selected_mask.values].copy()
total_gb        = sum(_parse_gb(s) for s in edited[selected_mask]["File Size"])

st.markdown('<div style="margin-top:0.75rem"></div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
c1.metric("Selected Datasets", n_selected)
c2.metric("Estimated Total Size", f"{total_gb:.1f} GB" if total_gb > 0 else "—")
if n_controlled > 0:
    c3.warning(
        f"{n_controlled} controlled-access dataset(s) excluded from scripts.",
        icon="🔒",
    )

if df["controlled_access"].any():
    st.info(
        "Rows marked **Controlled Access** require a manual application to "
        "[dbGaP](https://www.ncbi.nlm.nih.gov/gap) or "
        "[EGA](https://ega-archive.org/). They are excluded from generated scripts.",
        icon="🔒",
    )

st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Script generation
# ---------------------------------------------------------------------------
if n_selected == 0:
    st.markdown(
        '<p style="font-family:Arial,sans-serif;font-size:0.85rem;color:#9ca3af">'
        'Select datasets above to generate download scripts.</p>',
        unsafe_allow_html=True,
    )
else:
    st.markdown('<div class="section-label">Generate Scripts</div>', unsafe_allow_html=True)
    btn1, btn2 = st.columns(2)

    with btn1:
        sh_content = _shell_script(selected_df)
        st.download_button(
            label="Download Script (.sh)",
            data=sh_content.encode("utf-8"),
            file_name="download_manifest.sh",
            mime="text/x-sh",
            use_container_width=True,
            type="primary",
        )
        st.markdown(
            '<p style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af;'
            'margin-top:0.25rem">Bash wget script — '
            'run with <code>bash download_manifest.sh [output_dir]</code></p>',
            unsafe_allow_html=True,
        )

    with btn2:
        py_content = _python_script(selected_df)
        st.download_button(
            label="Download Script (.py)",
            data=py_content.encode("utf-8"),
            file_name="download_manifest.py",
            mime="text/x-python",
            use_container_width=True,
        )
        st.markdown(
            '<p style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af;'
            'margin-top:0.25rem">Python wget wrapper — '
            'run with <code>python download_manifest.py [output_dir]</code></p>',
            unsafe_allow_html=True,
        )

    with st.expander("Preview shell script"):
        st.code(_shell_script(selected_df), language="bash")
