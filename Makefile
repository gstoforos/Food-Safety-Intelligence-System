# AFTS FSIS — test runner Makefile
# ==================================
# Targets:
#   make test         — full test suite with verbose output
#   make test-fast    — skip slow filesystem tests (run only logic tests)
#   make test-watch   — re-run on file change (requires pytest-watch)
#   make install-dev  — pip-install test dependencies
#
# Run from the repo root: make test

.PHONY: test test-fast test-watch install-dev coverage clean

install-dev:
	python -m pip install -r requirements-dev.txt

test:
	pytest -v

test-fast:
	pytest -v -m "not slow"

test-watch:
	@which ptw >/dev/null || (echo "Install pytest-watch first: pip install pytest-watch" && exit 1)
	ptw -- -v

coverage:
	@which coverage >/dev/null || (echo "Install coverage first: pip install coverage" && exit 1)
	coverage run --source=pipeline -m pytest -v
	coverage report -m
	coverage html
	@echo "Open htmlcov/index.html in a browser to inspect line-by-line."

clean:
	rm -rf .pytest_cache htmlcov .coverage
	find . -name __pycache__ -type d -exec rm -rf {} +
