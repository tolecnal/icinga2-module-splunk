# Developer shortcuts. Run `make dev` once, then `make check` before pushing.
VENV ?= .venv
PY   := $(VENV)/bin/python

.PHONY: dev lint format typecheck test check deb clean

$(PY):
	python3 -m venv $(VENV)

dev: $(PY)  ## Create the virtualenv, install dev dependencies and git hooks
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e '.[dev]'
	$(VENV)/bin/pre-commit install

lint:  ## Lint (ruff) and check formatting
	$(VENV)/bin/ruff check src tests
	$(VENV)/bin/ruff format --check src tests

format:  ## Auto-fix lint issues and reformat
	$(VENV)/bin/ruff check --fix src tests
	$(VENV)/bin/ruff format src tests

typecheck:  ## Static type checking (mypy --strict)
	$(VENV)/bin/mypy

test:  ## Run the test suite with coverage
	$(VENV)/bin/pytest --cov --cov-report=term-missing

check: lint typecheck test  ## Everything CI runs on Python code
	$(VENV)/bin/pre-commit run --all-files

deb:  ## Build and install-test .debs in Docker (IMAGES="debian:12 ubuntu:24.04" to limit)
	packaging/docker-build.sh $(IMAGES)

clean:
	rm -rf build dist *.egg-info src/*.egg-info .coverage coverage.xml htmlcov \
		.pytest_cache .mypy_cache .ruff_cache
