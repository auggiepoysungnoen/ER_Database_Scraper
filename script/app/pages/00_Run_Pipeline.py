"""
Run Pipeline — trigger the scraper and refresh data from within the app.

On Streamlit Cloud: pipeline runs in-process and saves outputs to the
repo's output/ directory. Files persist for the session; to make them
permanent, commit and push from your local machine.

On local deployment: same behaviour, but files persist to disk.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

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
# Auth guard
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password  # noqa: E402

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

DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.title("⚙️ Run Pipeline")
st.markdown(
    "Scrape open-access databases for endometrial receptivity datasets, "
    "score them, and refresh the Search and Statistics pages."
)

st.info(
    "**Streamlit Cloud note:** Results are saved for this session. "
    "To make them permanent, run the pipeline locally and push `output/` files to GitHub.",
    icon="ℹ️",
)

st.divider()

# ---------------------------------------------------------------------------
# Current data status
# ---------------------------------------------------------------------------
st.subheader("Current Data Status")

meta_path = OUTPUT / "metadata_master.csv"
conf_path = OUTPUT / "confidence_scores.csv"

col1, col2, col3 = st.columns(3)

if meta_path.exists():
    import pandas as pd
    df = pd.read_csv(meta_path)
    col1.metric("Total Datasets", f"{len(df):,}")
    if "confidence_tier" in df.columns:
        gold = (df["confidence_tier"] == "GOLD").sum()
        silver = (df["confidence_tier"] == "SILVER").sum()
        col2.metric("GOLD", gold)
        col3.metric("SILVER", silver)
    last_mod = datetime.fromtimestamp(meta_path.stat().st_mtime)
    st.caption(f"Last updated: {last_mod.strftime('%Y-%m-%d %H:%M')}")
else:
    col1.metric("Total Datasets", "—")
    col2.metric("GOLD", "—")
    col3.metric("SILVER", "—")
    st.warning("No data yet — run the pipeline below.", icon="⚠️")

st.divider()

# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------
st.subheader("Pipeline Configuration")

c1, c2 = st.columns(2)

with c1:
    databases = st.multiselect(
        "Databases to scrape",
        options=["geo", "arrayexpress", "cellxgene", "hca", "scp", "zenodo", "figshare"],
        default=["geo", "arrayexpress", "zenodo", "figshare"],
        help="Select which databases to query. GEO + ArrayExpress cover most datasets.",
    )
    min_score = st.slider(
        "Minimum confidence score",
        min_value=0, max_value=80, value=40, step=5,
        help="Datasets below this score are excluded from output.",
    )

with c2:
    resume = st.checkbox(
        "Resume (skip already-scraped accessions)",
        value=True,
        help="Faster re-runs — only fetches new datasets.",
    )
    verbose = st.checkbox("Verbose logging", value=True)

st.divider()

# ---------------------------------------------------------------------------
# Run button
# ---------------------------------------------------------------------------
if st.button("▶ Run Pipeline Now", type="primary", use_container_width=False):
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

    st.info(f"Running: `{' '.join(cmd)}`", icon="🚀")

    log_box = st.empty()
    progress_bar = st.progress(0, text="Starting scrape…")

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

        # Stream output line by line
        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip()
            if not line:
                continue
            log_lines.append(line)
            # Keep last 50 lines in display
            display = "\n".join(log_lines[-50:])
            log_box.code(display, language="bash")

            # Progress heuristic: count "Scraped" lines
            scraped = sum(1 for l in log_lines if "scraped" in l.lower() or "found" in l.lower())
            progress_bar.progress(min(scraped / max(len(databases) * 20, 1), 0.95),
                                  text=f"Scraping… ({scraped} records found so far)")

        proc.wait()
        elapsed = time.time() - start_time

        if proc.returncode == 0:
            progress_bar.progress(1.0, text="Complete!")
            st.success(
                f"Pipeline finished in {elapsed:.0f}s. "
                "Refresh the **Search** and **Statistics** pages to see results.",
                icon="✅",
            )
            # Clear Streamlit data cache so pages reload fresh data
            st.cache_data.clear()
        else:
            st.error(f"Pipeline exited with code {proc.returncode}. Check log above.", icon="❌")

    except FileNotFoundError:
        st.error(
            f"Could not find `{SCRIPT}`. "
            "Make sure you are running the app from the repo root.",
            icon="❌",
        )
    except Exception as exc:
        st.error(f"Unexpected error: {exc}", icon="❌")

st.divider()

# ---------------------------------------------------------------------------
# Log viewer
# ---------------------------------------------------------------------------
st.subheader("Scrape Log")

if LOG_FILE.exists():
    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
        log_content = f.read()
    lines = log_content.strip().splitlines()
    st.caption(f"{len(lines)} log lines — showing last 100")
    st.code("\n".join(lines[-100:]), language="bash")
else:
    st.caption("No log file yet. Run the pipeline above.")
