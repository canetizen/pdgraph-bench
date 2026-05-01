# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_results_dir(tmp_path: Path) -> Path:
    """A fresh results directory per test."""
    d = tmp_path / "run"
    d.mkdir()
    return d
