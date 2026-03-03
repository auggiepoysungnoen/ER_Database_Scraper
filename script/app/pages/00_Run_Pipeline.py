"""
Run Pipeline — trigger the scraper and refresh data from within the app.
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Run Pipeline | Endometrial Receptivity DB",
    layout="wide",
    page_icon="⚙️",
    initial_sidebar_state="expanded",
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
APP_DIR   = Path(__file__).parent.parent
REPO_ROOT = APP_DIR.parent.parent
SCRIPT    = REPO_ROOT / "script" / "run_pipeline.py"
OUTPUT    = REPO_ROOT / "output"
LOG_FILE  = OUTPUT / "scrape_log.txt"

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
.block-container{padding-top:2rem;padding-bottom:3rem;max-width:1100px}
hr{border:none!important;border-top:1px solid #e5e7eb!important;margin:1.25rem 0!important}
.stButton>button{border-radius:2px;font-weight:500}
.section-label{font-family:Arial,sans-serif;font-size:0.6rem;font-weight:700;
               letter-spacing:0.12em;text-transform:uppercase;color:#9ca3af;
               margin-bottom:0.7rem;padding-bottom:0.35rem;border-bottom:1px solid #e5e7eb}
.status-card{background:#fff;border:1px solid #e5e7eb;border-top:3px solid #00539B;
             border-radius:2px;padding:1.1rem 1rem;font-family:Arial,sans-serif}
.status-num{font-size:1.9rem;font-weight:700;color:#012169;line-height:1}
.status-lbl{font-size:0.68rem;color:#9ca3af;margin-top:0.35rem;
            letter-spacing:0.06em;text-transform:uppercase}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown("""
<p style="font-family:Arial,sans-serif;font-size:1.65rem;font-weight:700;
          color:#012169;letter-spacing:-0.02em;margin-bottom:0">
    Run Pipeline
</p>
<p style="font-family:Arial,sans-serif;font-size:0.875rem;color:#6b7280;margin-top:0.25rem">
    Scrape public repositories, score datasets, and refresh the Search and Statistics pages.
</p>
""", unsafe_allow_html=True)
st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:0.75rem 0 1.5rem 0"></div>',
    unsafe_allow_html=True,
)

st.info(
    "**Streamlit Cloud:** Results persist for this session. "
    "To make them permanent, run the pipeline locally and push `output/` to GitHub.",
    icon="ℹ️",
)

# ---------------------------------------------------------------------------
# Current data status
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Current Data Status</div>', unsafe_allow_html=True)

meta_path = OUTPUT / "metadata_master.csv"
conf_path = OUTPUT / "confidence_scores.csv"

c1, c2, c3, c4 = st.columns(4)

if meta_path.exists():
    import pandas as pd
    df = pd.read_csv(meta_path)
    tier_col = next((c for c in df.columns if "tier" in c.lower()), None)

    c1.markdown(
        f'<div class="status-card"><div class="status-num">{len(df):,}</div>'
        f'<div class="status-lbl">Total Datasets</div></div>',
        unsafe_allow_html=True,
    )
    if tier_col:
        gold_n   = int((df[tier_col].str.upper() == "GOLD").sum())
        silver_n = int((df[tier_col].str.upper() == "SILVER").sum())
        bronze_n = int((df[tier_col].str.upper() == "BRONZE").sum())
        c2.markdown(
            f'<div class="status-card" style="border-top-color:#B5A369">'
            f'<div class="status-num">{gold_n:,}</div>'
            f'<div class="status-lbl">Gold</div></div>',
            unsafe_allow_html=True,
        )
        c3.markdown(
            f'<div class="status-card" style="border-top-color:#9E9E9E">'
            f'<div class="status-num">{silver_n:,}</div>'
            f'<div class="status-lbl">Silver</div></div>',
            unsafe_allow_html=True,
        )
        c4.markdown(
            f'<div class="status-card" style="border-top-color:#CD7F32">'
            f'<div class="status-num">{bronze_n:,}</div>'
            f'<div class="status-lbl">Bronze</div></div>',
            unsafe_allow_html=True,
        )
    last_mod = datetime.fromtimestamp(meta_path.stat().st_mtime)
    st.markdown(
        f'<p style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af;'
        f'margin-top:0.5rem">Last updated: {last_mod.strftime("%Y-%m-%d %H:%M")}</p>',
        unsafe_allow_html=True,
    )
else:
    for col in (c1, c2, c3, c4):
        col.markdown(
            '<div class="status-card"><div class="status-num">—</div>'
            '<div class="status-lbl">No data</div></div>',
            unsafe_allow_html=True,
        )
    st.warning("No output files found. Run the pipeline below.", icon="⚠️")

st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1.25rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Pipeline Configuration</div>', unsafe_allow_html=True)

cfg1, cfg2 = st.columns(2)

with cfg1:
    databases = st.multiselect(
        "Databases to scrape",
        options=["geo", "arrayexpress", "cellxgene", "hca", "scp", "zenodo", "figshare"],
        default=["geo", "arrayexpress", "zenodo", "figshare"],
        help="GEO + ArrayExpress cover the majority of datasets.",
    )
    min_score = st.slider(
        "Minimum confidence score",
        min_value=0, max_value=80, value=0, step=5,
        help="Datasets below this threshold are excluded from output.",
    )

with cfg2:
    resume = st.checkbox(
        "Resume (skip already-scraped accessions)",
        value=True,
        help="Faster re-runs — only fetches new datasets.",
    )
    verbose = st.checkbox("Verbose logging", value=True)

st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1.25rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Run button
# ---------------------------------------------------------------------------
if st.button("Run Pipeline", type="primary"):
    if not databases:
        st.error("Select at least one database.")
        st.stop()

    OUTPUT.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, str(SCRIPT),
        "--databases", ",".join(databases),
        "--min-score", str(min_score),
        "--output-dir", str(OUTPUT),
    ]
    if resume:
        cmd.append("--resume")
    if verbose:
        cmd.append("--verbose")

    st.markdown(
        f'<div style="font-family:monospace;font-size:0.75rem;color:#6b7280;'
        f'background:#f8f9fa;border:1px solid #e5e7eb;border-radius:2px;'
        f'padding:0.5rem 0.75rem;margin-bottom:0.75rem">{" ".join(cmd)}</div>',
        unsafe_allow_html=True,
    )

    log_box    = st.empty()
    prog_bar   = st.progress(0, text="Starting…")
    log_lines: list[str] = []
    start_time = time.time()

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(REPO_ROOT),
        )

        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip()
            if not line:
                continue
            log_lines.append(line)
            log_box.code("\n".join(log_lines[-50:]), language="bash")
            scraped = sum(
                1 for ln in log_lines
                if "scraped" in ln.lower() or "found" in ln.lower()
            )
            prog_bar.progress(
                min(scraped / max(len(databases) * 20, 1), 0.95),
                text=f"Scraping… ({scraped} records found)",
            )

        proc.wait()
        elapsed = time.time() - start_time

        if proc.returncode == 0:
            prog_bar.progress(1.0, text="Complete")
            st.success(
                f"Pipeline finished in {elapsed:.0f} s. "
                "Refresh **Search** and **Statistics** to see updated results.",
                icon="✅",
            )
            st.cache_data.clear()
        else:
            st.error(
                f"Pipeline exited with code {proc.returncode}. Review the log above.",
                icon="❌",
            )

    except FileNotFoundError:
        st.error(
            f"Script not found: `{SCRIPT}`. "
            "Ensure the app is run from the repository root.",
            icon="❌",
        )
    except Exception as exc:
        st.error(f"Unexpected error: {exc}", icon="❌")

st.markdown(
    '<div style="height:1px;background:#e5e7eb;margin:1.25rem 0"></div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Log viewer
# ---------------------------------------------------------------------------
st.markdown('<div class="section-label">Scrape Log</div>', unsafe_allow_html=True)

if LOG_FILE.exists():
    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
        log_content = f.read()
    lines = log_content.strip().splitlines()
    st.markdown(
        f'<p style="font-family:Arial,sans-serif;font-size:0.72rem;color:#9ca3af">'
        f'{len(lines):,} lines — showing last 100</p>',
        unsafe_allow_html=True,
    )
    st.code("\n".join(lines[-100:]), language="bash")
else:
    st.markdown(
        '<p style="font-family:Arial,sans-serif;font-size:0.85rem;color:#9ca3af">'
        'No log file yet. Run the pipeline above.</p>',
        unsafe_allow_html=True,
    )
