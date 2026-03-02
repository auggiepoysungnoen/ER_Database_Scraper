"""
output/__init__.py
==================
Output writers package for the Hickey Lab Endometrial Receptivity pipeline.

Exports all public writer functions so that callers can import from the
package root.

Usage
-----
    from output import (
        write_metadata_master,
        write_confidence_scores,
        write_registry,
        load_existing_registry,
        merge_registry,
        generate_paper_summaries,
        write_paper_summaries_json,
        write_paper_summaries_md,
        generate_pipeline_report,
    )
"""

from output.writers import (
    write_metadata_master,
    write_confidence_scores,
    write_registry,
    load_existing_registry,
    merge_registry,
)
from output.paper_summary import (
    generate_paper_summaries,
    write_paper_summaries_json,
    write_paper_summaries_md,
)
from output.report import generate_pipeline_report

__all__ = [
    "write_metadata_master",
    "write_confidence_scores",
    "write_registry",
    "load_existing_registry",
    "merge_registry",
    "generate_paper_summaries",
    "write_paper_summaries_json",
    "write_paper_summaries_md",
    "generate_pipeline_report",
]
