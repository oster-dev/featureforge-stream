PYTHON := .venv/bin/python

.PHONY: check lint format-check format test

check: lint format-check test

lint:
	$(PYTHON) -m ruff check .

format-check:
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .

test:
	$(PYTHON) -m pytest -q