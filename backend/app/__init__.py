"""FastAPI read API over the gold layer.

Kept as a separate top-level package from `pipeline` because it ships as its
own container image: the API needs DuckDB and FastAPI, not Prefect. See
Dockerfile.backend and backend/requirements.txt.
"""
