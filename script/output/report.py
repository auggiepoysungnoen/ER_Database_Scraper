"""
output/report.py
================
Standalone HTML pipeline report generator for the Hickey Lab Endometrial
Receptivity pipeline.

The report is fully self-contained: layout CSS is embedded inline; Plotly JS
is loaded from the official CDN (cdn.plot.ly) for interactive charts.

Color scheme: Duke blue (#00539B), Duke navy (#012169), accent gold (#B5A369),
Duke grey (#666666).

Output produced
---------------
- ``pipeline_report.html`` — standalone HTML summary report

Usage
-----
    from output.report import generate_pipeline_report

    generate_pipeline_report(
        datasets=datasets,
        scores=scores,
        output_dir="/path/to/output",
        run_timestamp="2026-03-02T14:30:00Z",
    )
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from typing import Any

from jinja2 import Template

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PIPELINE_VERSION = "2.0.0"

_DUKE_BLUE   = "#00539B"
_DUKE_NAVY   = "#012169"
_DUKE_GOLD   = "#B5A369"
_DUKE_GREY   = "#666666"
_WHITE       = "#FFFFFF"

_TIER_COLORS = {
    "GOLD":           "#B5A369",
    "SILVER":         "#8A9BB0",
    "BRONZE":         "#C87941",
    "LOW_CONFIDENCE": "#AAAAAA",
}

# ---------------------------------------------------------------------------
# Jinja2 HTML template (inline — no external file required)
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Hickey Lab — Endometrial Receptivity Pipeline Report</title>
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
  <style>
    /* ---------- Reset & Base ---------- */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: Georgia, 'Times New Roman', serif;
      background: {{ white }};
      color: #222;
      line-height: 1.6;
    }
    a { color: {{ duke_blue }}; text-decoration: none; }
    a:hover { text-decoration: underline; }

    /* ---------- Header ---------- */
    header {
      background: {{ duke_navy }};
      color: {{ white }};
      padding: 2rem 3rem;
      border-bottom: 4px solid {{ duke_gold }};
    }
    header h1 {
      font-size: 1.8rem;
      font-weight: normal;
      letter-spacing: 0.02em;
    }
    header .subtitle {
      font-size: 0.95rem;
      color: #ccd6e8;
      margin-top: 0.3rem;
    }
    header .meta {
      font-size: 0.85rem;
      color: {{ duke_gold }};
      margin-top: 0.6rem;
      font-family: 'Courier New', monospace;
    }

    /* ---------- Layout ---------- */
    main {
      max-width: 1100px;
      margin: 2rem auto;
      padding: 0 2rem 4rem;
    }
    section {
      margin-bottom: 3rem;
    }
    h2 {
      color: {{ duke_blue }};
      font-size: 1.3rem;
      font-weight: normal;
      border-bottom: 2px solid {{ duke_blue }};
      padding-bottom: 0.4rem;
      margin-bottom: 1.2rem;
    }
    h3 {
      color: {{ duke_grey }};
      font-size: 1.05rem;
      font-weight: normal;
      margin-bottom: 0.8rem;
    }

    /* ---------- Summary Cards ---------- */
    .card-row {
      display: flex;
      gap: 1.2rem;
      flex-wrap: wrap;
      margin-bottom: 1.5rem;
    }
    .card {
      flex: 1 1 140px;
      background: #f4f7fb;
      border-left: 4px solid {{ duke_blue }};
      border-radius: 4px;
      padding: 1rem 1.2rem;
      min-width: 120px;
    }
    .card .value {
      font-size: 2rem;
      color: {{ duke_blue }};
      font-family: 'Courier New', monospace;
      line-height: 1;
    }
    .card .label {
      font-size: 0.78rem;
      color: {{ duke_grey }};
      margin-top: 0.35rem;
      text-transform: uppercase;
      letter-spacing: 0.07em;
    }
    .card.gold   { border-left-color: {{ tier_gold }}; }
    .card.silver { border-left-color: {{ tier_silver }}; }
    .card.bronze { border-left-color: {{ tier_bronze }}; }
    .card.low    { border-left-color: {{ tier_low }}; }
    .card.gold   .value { color: {{ tier_gold }}; }
    .card.silver .value { color: {{ tier_silver }}; }
    .card.bronze .value { color: {{ tier_bronze }}; }
    .card.low    .value { color: {{ tier_low }}; }

    /* ---------- Tables ---------- */
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.88rem;
    }
    th {
      background: {{ duke_blue }};
      color: {{ white }};
      padding: 0.55rem 0.8rem;
      text-align: left;
      font-weight: normal;
      letter-spacing: 0.04em;
    }
    td {
      padding: 0.45rem 0.8rem;
      border-bottom: 1px solid #e0e6ef;
    }
    tr:nth-child(even) td { background: #f4f7fb; }
    .mono { font-family: 'Courier New', monospace; font-size: 0.82rem; }
    .tier-badge {
      display: inline-block;
      padding: 0.1rem 0.5rem;
      border-radius: 3px;
      font-size: 0.75rem;
      font-family: 'Courier New', monospace;
    }
    .tier-GOLD           { background: {{ tier_gold }};   color: #fff; }
    .tier-SILVER         { background: {{ tier_silver }}; color: #fff; }
    .tier-BRONZE         { background: {{ tier_bronze }}; color: #fff; }
    .tier-LOW_CONFIDENCE { background: {{ tier_low }};    color: #fff; }

    /* ---------- Charts ---------- */
    .chart-container {
      width: 100%;
      min-height: 380px;
      margin-bottom: 1.5rem;
    }

    /* ---------- Footer ---------- */
    footer {
      background: {{ duke_navy }};
      color: #8ca0be;
      text-align: center;
      padding: 1.2rem;
      font-size: 0.8rem;
    }
    footer span { color: {{ duke_gold }}; }
  </style>
</head>
<body>

<!-- ===== HEADER ===== -->
<header>
  <h1>Hickey Lab &mdash; Endometrial Receptivity Dataset Registry</h1>
  <p class="subtitle">Aim01: Automated Database Regeneration Pipeline &mdash; Run Report</p>
  <p class="meta">
    Run timestamp: {{ run_timestamp }} &nbsp;|&nbsp;
    Pipeline version: {{ pipeline_version }}
  </p>
</header>

<main>

  <!-- ===== 1. SUMMARY ===== -->
  <section id="summary">
    <h2>1. Run Summary</h2>
    <div class="card-row">
      <div class="card">
        <div class="value">{{ total_found }}</div>
        <div class="label">Datasets found</div>
      </div>
      <div class="card">
        <div class="value">{{ total_accepted }}</div>
        <div class="label">Accepted (CS &ge; 40)</div>
      </div>
      <div class="card">
        <div class="value">{{ total_rejected }}</div>
        <div class="label">Rejected (CS &lt; 40)</div>
      </div>
      <div class="card gold">
        <div class="value">{{ count_gold }}</div>
        <div class="label">GOLD (&ge; 80)</div>
      </div>
      <div class="card silver">
        <div class="value">{{ count_silver }}</div>
        <div class="label">SILVER (60–79)</div>
      </div>
      <div class="card bronze">
        <div class="value">{{ count_bronze }}</div>
        <div class="label">BRONZE (40–59)</div>
      </div>
      <div class="card low">
        <div class="value">{{ count_low }}</div>
        <div class="label">LOW (&lt; 40)</div>
      </div>
    </div>
  </section>

  <!-- ===== 2. MODALITY BREAKDOWN ===== -->
  <section id="modality">
    <h2>2. Modality Breakdown</h2>
    <div id="chart-modality" class="chart-container"></div>
  </section>

  <!-- ===== 3. CONFIDENCE SCORE HISTOGRAM ===== -->
  <section id="histogram">
    <h2>3. Confidence Score Distribution</h2>
    <div id="chart-histogram" class="chart-container"></div>
  </section>

  <!-- ===== 4. LH TIMEPOINT COVERAGE ===== -->
  <section id="timepoints">
    <h2>4. LH Timepoint Coverage</h2>
    <table>
      <thead>
        <tr>
          <th>LH Timepoint</th>
          <th>Dataset Count</th>
          <th>Coverage</th>
        </tr>
      </thead>
      <tbody>
        {% for row in timepoint_rows %}
        <tr>
          <td class="mono">{{ row.timepoint }}</td>
          <td>{{ row.count }}</td>
          <td>{{ row.pct }}%</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </section>

</main>

<!-- ===== FOOTER ===== -->
<footer>
  Generated on <span>{{ run_timestamp }}</span> &mdash;
  Hickey Lab, Duke University &mdash;
  Aim01 Database Regeneration Pipeline v{{ pipeline_version }}
</footer>

<!-- ===== PLOTLY CHARTS ===== -->
<script>
  // --- Modality bar chart ---
  var modalityData = {{ modality_chart_json }};
  Plotly.newPlot('chart-modality', modalityData.data, modalityData.layout,
                 {responsive: true, displayModeBar: false});

  // --- Confidence score histogram ---
  var histData = {{ histogram_chart_json }};
  Plotly.newPlot('chart-histogram', histData.data, histData.layout,
                 {responsive: true, displayModeBar: false});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Chart builders  (return plain dicts — serialised to JSON in template)
# ---------------------------------------------------------------------------

def _build_modality_chart(datasets: list[dict], scores: list[dict]) -> dict:
    """Build a Plotly bar chart spec for modality breakdown."""
    score_map = {s.get("accession", ""): s for s in scores}

    # Count datasets per modality
    modality_counts: Counter = Counter()
    for ds in datasets:
        modality_counts[ds.get("modality") or "Unknown"] += 1

    modalities = sorted(modality_counts.keys())
    counts     = [modality_counts[m] for m in modalities]

    return {
        "data": [{
            "type":        "bar",
            "x":           modalities,
            "y":           counts,
            "marker":      {"color": _DUKE_BLUE},
            "hovertemplate": "%{x}: %{y} datasets<extra></extra>",
        }],
        "layout": {
            "title":  {"text": "Datasets by Modality", "font": {"color": _DUKE_NAVY}},
            "xaxis":  {"title": "Modality", "tickfont": {"size": 11}},
            "yaxis":  {"title": "Number of Datasets"},
            "paper_bgcolor": _WHITE,
            "plot_bgcolor":  "#f4f7fb",
            "margin": {"l": 50, "r": 20, "t": 50, "b": 80},
            "font":   {"family": "Georgia, serif", "color": "#222"},
        },
    }


def _build_histogram_chart(scores: list[dict]) -> dict:
    """Build a Plotly histogram spec for confidence score distribution."""
    tier_order = ["GOLD", "SILVER", "BRONZE", "LOW_CONFIDENCE"]

    # Separate scores by tier
    tier_scores: dict[str, list[float]] = defaultdict(list)
    for sc in scores:
        tier = sc.get("confidence_tier", "LOW_CONFIDENCE")
        cs   = sc.get("final_CS")
        if cs is not None:
            tier_scores[tier].append(float(cs))

    traces = []
    for tier in tier_order:
        vals = tier_scores.get(tier, [])
        if not vals:
            continue
        traces.append({
            "type":      "histogram",
            "x":         vals,
            "name":      tier,
            "nbinsx":    20,
            "marker":    {"color": _TIER_COLORS.get(tier, _DUKE_GREY), "opacity": 0.85},
            "hovertemplate": f"{tier}: %{{x:.1f}} — %{{y}} datasets<extra></extra>",
        })

    # Vertical threshold lines via shapes
    shapes = []
    for threshold, label in [(40, "BRONZE threshold"), (60, "SILVER threshold"),
                              (80, "GOLD threshold")]:
        shapes.append({
            "type": "line",
            "x0": threshold, "x1": threshold,
            "y0": 0, "y1": 1,
            "yref": "paper",
            "line": {"color": _DUKE_NAVY, "width": 1.5, "dash": "dash"},
        })

    return {
        "data": traces,
        "layout": {
            "title":    {"text": "Confidence Score Distribution", "font": {"color": _DUKE_NAVY}},
            "xaxis":    {"title": "Confidence Score (CS)", "range": [0, 105]},
            "yaxis":    {"title": "Number of Datasets"},
            "barmode":  "stack",
            "shapes":   shapes,
            "legend":   {"title": {"text": "Tier"}, "orientation": "h",
                         "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            "paper_bgcolor": _WHITE,
            "plot_bgcolor":  "#f4f7fb",
            "margin": {"l": 50, "r": 20, "t": 60, "b": 60},
            "font":   {"family": "Georgia, serif", "color": "#222"},
        },
    }


def _build_timepoint_rows(datasets: list[dict], total: int) -> list[dict]:
    """Count how many datasets include each LH timepoint."""
    tp_counter: Counter = Counter()
    for ds in datasets:
        for tp in (ds.get("lh_timepoints") or []):
            tp_counter[str(tp).strip()] += 1

    if not tp_counter:
        return [{"timepoint": "No LH timepoints recorded", "count": 0, "pct": "0.0"}]

    rows = []
    for tp, count in sorted(tp_counter.items()):
        pct = (count / total * 100) if total else 0.0
        rows.append({"timepoint": tp, "count": count, "pct": f"{pct:.1f}"})
    return rows


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def generate_pipeline_report(
    datasets: list[dict],
    scores: list[dict],
    output_dir: str,
    run_timestamp: str,
) -> str:
    """Generate a standalone HTML pipeline report.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dicts from scrapers.
    scores : list[dict]
        List of score result dicts from ``ConfidenceScorer.score_all``.
    output_dir : str
        Directory where ``pipeline_report.html`` will be written.
    run_timestamp : str
        ISO 8601 timestamp string for the run (e.g. "2026-03-02T14:30:00Z").

    Returns
    -------
    str
        Absolute path to the written HTML file.

    Notes
    -----
    The report is fully self-contained: all CSS is inline; Plotly JS is
    loaded from ``https://cdn.plot.ly/plotly-2.32.0.min.js``.  An internet
    connection is required only to render the interactive charts.

    Examples
    --------
    >>> path = generate_pipeline_report(datasets, scores, "/output",
    ...                                  "2026-03-02T14:30:00Z")
    >>> path.endswith("pipeline_report.html")
    True
    """
    os.makedirs(output_dir, exist_ok=True)

    total_found    = len(datasets)
    tier_counts    = Counter(sc.get("confidence_tier", "LOW_CONFIDENCE") for sc in scores)
    count_gold     = tier_counts.get("GOLD", 0)
    count_silver   = tier_counts.get("SILVER", 0)
    count_bronze   = tier_counts.get("BRONZE", 0)
    count_low      = tier_counts.get("LOW_CONFIDENCE", 0)
    total_accepted = count_gold + count_silver + count_bronze
    total_rejected = count_low

    # Build chart JSON
    modality_chart  = _build_modality_chart(datasets, scores)
    histogram_chart = _build_histogram_chart(scores)
    timepoint_rows  = _build_timepoint_rows(datasets, total_found)

    # Render template
    template = Template(_HTML_TEMPLATE)
    html = template.render(
        run_timestamp    = run_timestamp,
        pipeline_version = PIPELINE_VERSION,
        total_found      = total_found,
        total_accepted   = total_accepted,
        total_rejected   = total_rejected,
        count_gold       = count_gold,
        count_silver     = count_silver,
        count_bronze     = count_bronze,
        count_low        = count_low,
        timepoint_rows   = timepoint_rows,
        modality_chart_json  = json.dumps(modality_chart),
        histogram_chart_json = json.dumps(histogram_chart),
        # Colors
        white        = _WHITE,
        duke_blue    = _DUKE_BLUE,
        duke_navy    = _DUKE_NAVY,
        duke_gold    = _DUKE_GOLD,
        duke_grey    = _DUKE_GREY,
        tier_gold    = _TIER_COLORS["GOLD"],
        tier_silver  = _TIER_COLORS["SILVER"],
        tier_bronze  = _TIER_COLORS["BRONZE"],
        tier_low     = _TIER_COLORS["LOW_CONFIDENCE"],
    )

    out_path = os.path.join(output_dir, "pipeline_report.html")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    return os.path.abspath(out_path)
