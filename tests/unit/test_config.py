# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Config validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from graph_bench.utils.config import ScaleOutConfig, ScenarioConfig


def _minimal() -> dict:
    return {
        "id": "s1",
        "name": "Transactional-only",
        "workload": "synthetic",
        "dataset": "synthetic_sf1",
        "system": "mock",
    }


def test_minimal_config_produces_spec() -> None:
    cfg = ScenarioConfig.model_validate(_minimal())
    spec = cfg.to_spec()
    assert spec.id == "s1"
    assert spec.worker_count == 16
    assert spec.scale_out is None


def test_negative_worker_count_rejected() -> None:
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate({**_minimal(), "worker_count": 0})


def test_scale_out_config_projected() -> None:
    cfg = ScenarioConfig.model_validate(
        {
            **_minimal(),
            "id": "s5",
            "scale_out": {"at_offset_seconds": 300, "target_node": "node5", "role": "storaged"},
        }
    )
    spec = cfg.to_spec()
    assert spec.scale_out is not None
    assert spec.scale_out.target_node == "node5"
    assert int(spec.scale_out.at_offset.total_seconds()) == 300


def test_unknown_field_rejected() -> None:
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate({**_minimal(), "nonsense": 1})


def test_scale_out_standalone_validation() -> None:
    cfg = ScaleOutConfig(at_offset_seconds=60, target_node="node5", role="alpha")
    assert cfg.target_node == "node5"
