# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

.PHONY: help install dev-install lint format typecheck test test-unit test-integration docs clean

PYTHON ?= uv run

help:
	@echo "graph-bench — common tasks"
	@echo ""
	@echo "  install         install runtime dependencies via uv"
	@echo "  dev-install     install with dev + drivers + deploy + analysis extras"
	@echo "  lint            run ruff check"
	@echo "  format          run black + ruff --fix"
	@echo "  typecheck       run mypy"
	@echo "  test            run all tests"
	@echo "  test-unit       run unit tests only"
	@echo "  test-integration  run integration tests only"
	@echo "  docs            compile the LaTeX progress report"
	@echo "  clean           remove caches and build artifacts"

install:
	uv sync

dev-install:
	uv sync --extra dev --extra drivers --extra deploy --extra analysis

lint:
	$(PYTHON) ruff check .

format:
	$(PYTHON) black src tests analysis
	$(PYTHON) ruff check --fix .

typecheck:
	$(PYTHON) mypy src

test:
	$(PYTHON) pytest

test-unit:
	$(PYTHON) pytest -m "not integration" tests/unit

test-integration:
	$(PYTHON) pytest -m integration tests/integration

docs:
	cd docs && ./build.sh

clean:
	rm -rf dist build .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
