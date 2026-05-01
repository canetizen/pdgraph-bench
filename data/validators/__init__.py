# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Post-load validators.

After a bulk load completes, a validator runs a small set of known-answer
queries against the loaded SUT. A validator failure aborts the benchmark run
before warmup, so that measurements are never taken on a partially-loaded
graph. Reference answers are derived from the dataset itself
(`data/generated/<dataset>/parameters.json` plus on-the-fly computed counts).
"""

from data.validators.runner import ValidationResult, run_validation

__all__ = ["ValidationResult", "run_validation"]
