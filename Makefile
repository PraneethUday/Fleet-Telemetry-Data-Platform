# ---------------------------------------------------------------------------
# Fleet Telemetry Data Platform — local developer entrypoints.
#
# Every target here is the *same* command the CI job or the container runs, so
# "works on my machine" and "works in Azure" cannot drift apart quietly. All
# Python goes through ./.venv/bin/python explicitly rather than relying on an
# activated shell, because a forgotten `source .venv/bin/activate` silently
# falls back to the system interpreter (3.14 on this machine, which has none
# of the dependencies installed).
#
# Run `make` with no target for the list.
# ---------------------------------------------------------------------------

# Bootstrap interpreter used ONCE to create the venv. Override if python3.12
# is not on PATH:  make install PYTHON_BOOTSTRAP=/opt/homebrew/bin/python3.12
PYTHON_BOOTSTRAP ?= python3.12

VENV := .venv
PY   := ./$(VENV)/bin/python
PIP  := ./$(VENV)/bin/pip

# Import path of the FastAPI instance. Overridable so a rename of the backend
# package is a flag, not an edit:  make backend BACKEND_APP=backend.main:app
BACKEND_APP  ?= backend.app.main:app
BACKEND_PORT ?= 8000

# Days of history `make generate` simulates.
GEN_DAYS ?= 3

# Local image tags. Azure builds are tagged in docs/DEPLOYMENT.md instead.
ETL_IMAGE ?= fleet-etl:local
API_IMAGE ?= fleet-api:local

.DEFAULT_GOAL := help
.PHONY: help install generate silver gold etl queries backend frontend test clean docker-etl docker-backend

help: ## Print the available targets.
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- environment -----------------------------------------------------------

install: ## Create .venv with python3.12 and install the pipeline + backend deps.
	$(PYTHON_BOOTSTRAP) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
# The backend shares this one venv locally (it gets its own image in prod), so
# install its pins too when they exist — a clean checkout should be one command.
	@test -f backend/requirements.txt \
		&& $(PIP) install -r backend/requirements.txt \
		|| echo "note: backend/requirements.txt not present yet — skipped"
	@test -f .env || (cp .env.example .env && echo "note: created .env from .env.example")

# --- pipeline --------------------------------------------------------------

generate: ## Regenerate the dirty bronze feed from scratch (GEN_DAYS, default 3).
	$(PY) -m pipeline.generator.run_generator --days $(GEN_DAYS) --reset

silver: ## Clean, type and deduplicate bronze into the silver layer.
	$(PY) -m pipeline.flows.silver_flow

gold: ## Aggregate silver into the gold marts the API serves.
	$(PY) -m pipeline.flows.gold_flow

etl: ## Run silver then gold — byte-for-byte what the Container Apps Job runs.
	$(PY) -m pipeline.flows.etl_flow

queries: ## Run the DuckDB example queries against the Parquet lake.
	$(PY) -m pipeline.warehouse.run_queries

# --- services --------------------------------------------------------------

backend: ## Serve the FastAPI read API on :8000 with autoreload.
	$(PY) -m uvicorn $(BACKEND_APP) --reload --host 127.0.0.1 --port $(BACKEND_PORT)

frontend: ## Serve the Vite dashboard (expects `npm install` in frontend/ first).
	cd frontend && npm run dev

# --- quality ---------------------------------------------------------------

test: ## Run the test suite.
	$(PY) -m pytest

# --- housekeeping ----------------------------------------------------------

clean: ## Delete the local lakehouse and all bytecode caches.
# data/ is disposable by design: `make generate` rebuilds it deterministically
# from GENERATOR_SEED, which is why it is gitignored rather than committed.
	rm -rf data
	mkdir -p data
	find . -path ./$(VENV) -prune -o -name '__pycache__' -type d -print0 \
		| xargs -0 rm -rf
	find . -path ./$(VENV) -prune -o -name '*.pyc' -type f -print0 | xargs -0 rm -f

# --- containers ------------------------------------------------------------
# --platform linux/amd64 is not optional on an Apple Silicon Mac: Container
# Apps runs amd64 only, and a locally built arm64 image fails at runtime with
# "exec format error" rather than at build time. Same trap, same fix, as the
# `az acr build --platform` note in docs/DEPLOYMENT.md.

docker-etl: ## Build the ETL job image for linux/amd64.
	docker build --platform linux/amd64 -f Dockerfile.etl -t $(ETL_IMAGE) .

docker-backend: ## Build the FastAPI image for linux/amd64.
	docker build --platform linux/amd64 -f Dockerfile.backend -t $(API_IMAGE) .
