# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Post-run analysis: statistics, plots, report generation.

This package reads only from serialized output artifacts in `results/<run_id>/`.
It never imports from `graph_bench` to preserve the clean-architecture
dependency direction (analysis sits outside the harness, not inside it).
"""
