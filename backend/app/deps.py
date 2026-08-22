"""DuckDB connection lifecycle, and the one place rows become JSON-safe.

CONNECTION LIFECYCLE — one connection for the process, one cursor per request.

`pipeline.warehouse.session.get_warehouse()` opens an in-memory DuckDB and
registers five views over the Parquet already sitting in the lake. Registering
them costs a directory glob per view, so the shape to avoid is obvious: opening
a warehouse inside a request handler would re-glob the whole lake on every call
and turn a millisecond query into a filesystem walk (a listing round trip, once
`LAKE_BACKEND=azure`).

So the connection is opened exactly once, in the app's async lifespan handler,
and lives for the life of the process. That leaves the thread-safety question,
because FastAPI runs `def` handlers in a threadpool and several requests really
do land at once. Two workable answers:

  a) share the single connection and serialise every query behind a lock —
     correct, but it makes the API a queue of one, which is precisely what
     `get_warehouse`'s own docstring warns against;
  b) call `connection.cursor()` per request.

This module does (b). `cursor()` returns a *new connection to the same database
instance*: it shares the catalog — so it sees the views the root connection
registered, with no re-globbing and no second copy of anything — while getting
its own transaction and execution context, which is what makes concurrent use
safe. Queries then genuinely run in parallel. Verified rather than assumed:
twelve threads issuing cursor queries against these views concurrently return
correct results with no errors.

One sharp edge that verification also turned up: a cursor does NOT inherit the
root connection's session settings. `get_warehouse` runs `SET TimeZone='UTC'`,
but a fresh cursor falls back to the host's zone, so on a laptop in IST the same
TIMESTAMPTZ renders as `+05:30` while the root connection renders it as `Z`.
The instant is identical, and `_jsonable` normalises to UTC regardless — but the
cursor is pinned to UTC anyway, so that a query logged from here and the same
query run through `make queries` cannot print different-looking times.

DEGRADED START. The API must come up even when the ETL has not run, because
`/api/health` is the container's liveness probe: a server that refuses to start
because a Parquet file is missing looks exactly like a server that crashed, and
the platform restarts it forever instead of showing anyone the real problem. So
a warehouse that cannot open is recorded, not raised, and the data routes answer
503 with the command that fixes it.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

import duckdb
from fastapi import HTTPException, Request

from pipeline.config import Settings, get_settings
from pipeline.schemas import GOLD_TABLES
from pipeline.storage import get_storage
from pipeline.warehouse.session import get_warehouse

log = logging.getLogger(__name__)

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# JSON coercion
# ---------------------------------------------------------------------------
def _jsonable(value: Any) -> Any:
    """Coerce one DuckDB value into something `json.dumps` can actually emit.

    This exists in one function, applied to every value of every row, rather
    than as per-field handling in the routes — the failure it prevents is
    exactly the kind that hides in the one column nobody thought about.

    What it is defending against:

    * `float('nan')` and `float('inf')`. Python's json module writes these as
      the bare tokens `NaN` and `Infinity`, which are NOT valid JSON: the
      browser's `JSON.parse` throws on them, so a single unlucky division in
      the gold layer would blank the whole dashboard rather than one cell. Both
      become null. (A SQL NULL is already `None` here — see below — so this
      catches only a NaN genuinely stored in the Parquet.)
    * DATE vs TIMESTAMP. `day` is a calendar date and must serialise as
      "YYYY-MM-DD"; rendering it as a midnight instant would let a client west
      of UTC display it as the previous day. Because `datetime` subclasses
      `date`, the datetime branch has to come first.
    * Timestamp offsets. Everything the lake stores is UTC, but DuckDB renders
      TIMESTAMPTZ in the session zone, so the same value can arrive as
      `+05:30`. It is converted to real UTC and suffixed `Z`, which is what the
      contract promises and what `new Date()` parses unambiguously.

    Note what is deliberately absent: any pandas handling. Rows are read with
    `fetchall()`, not `.df()`, because a DataFrame collapses SQL NULL and IEEE
    NaN into the same `float('nan')` — it manufactures the invalid-JSON problem
    on the way in. DuckDB's native conversion keeps NULL as `None`, so the only
    NaN left to catch is a real one.
    """
    if value is None:
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        # Naive timestamps in this lake are UTC by construction; stamping the
        # zone is more honest than emitting an offset-less string a client is
        # free to interpret as local time.
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        # No gold column is DECIMAL today, but a future SUM over one would
        # serialise as a repr string without this.
        return float(value)
    return value


def _rows(cursor: duckdb.DuckDBPyConnection) -> list[Row]:
    """Every row of an executed cursor, as JSON-safe dicts keyed by column name."""
    columns = [descriptor[0] for descriptor in cursor.description or ()]
    return [
        {name: _jsonable(value) for name, value in zip(columns, record)}
        for record in cursor.fetchall()
    ]


# ---------------------------------------------------------------------------
# Warehouse handle
# ---------------------------------------------------------------------------
@dataclass
class Warehouse:
    """The process-wide DuckDB handle, plus whatever went wrong opening it."""

    settings: Settings
    connection: duckdb.DuckDBPyConnection | None = None
    open_error: str | None = None
    _gold_keys: dict[str, str] = field(default_factory=dict)

    @classmethod
    def open(cls, settings: Settings | None = None) -> "Warehouse":
        """Open the warehouse, recording rather than raising a failure.

        `require_gold=False` lets the views register over whatever exists, so
        the process starts against a half-built lake. It does not cover every
        case: `register_views` still hard-requires the bronze and silver layers,
        so a deployment that ships only the gold marts to the API cannot open a
        warehouse at all. That is why the failure is captured instead of
        propagated — the liveness probe stays useful either way.
        """
        settings = settings or get_settings()
        warehouse = cls(
            settings=settings,
            _gold_keys={
                name: f"{settings.gold_prefix}/{name}.parquet" for name in GOLD_TABLES
            },
        )
        try:
            warehouse.connection = get_warehouse(settings, require_gold=False)
        except Exception as exc:  # noqa: BLE001 — any failure must still leave a live probe
            warehouse.open_error = str(exc)
            log.warning("Warehouse unavailable at startup: %s", exc)
        return warehouse

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def gold_tables(self) -> dict[str, bool]:
        """Which gold tables are present in the lake, table name -> bool.

        Answered from storage rather than by querying the views, on purpose:
        this is the health check, and it has to keep working in exactly the case
        where the warehouse failed to open. It is also the honest question — the
        frontend wants to know whether the ETL has produced its marts, which is
        a fact about the lake, not about this process's DuckDB catalog.
        """
        storage = get_storage(self.settings)
        present: dict[str, bool] = {}
        for name, key in self._gold_keys.items():
            try:
                present[name] = storage.exists(key)
            except Exception as exc:  # noqa: BLE001 — an unreachable lake is "absent", not a 500
                log.warning("Could not probe gold table %s: %s", name, exc)
                present[name] = False
        return present

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[Row]:
        """Run one parameterised query on a per-request cursor.

        Every caller passes values through `params`. Not one route formats a
        value into the SQL string, even where the value is a machine id matched
        against an enum-like column — the habit is the point, and the moment a
        filter grows to accept something less constrained the escaping bug is
        already written.
        """
        if self.connection is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "The gold layer is not queryable: "
                    f"{self.open_error or 'the warehouse is not open'}"
                ),
            )

        cursor = self.connection.cursor()
        try:
            # Cursors do not inherit the root connection's session settings; see
            # the module docstring.
            cursor.execute("SET TimeZone='UTC'")
            cursor.execute(sql, list(params))
            return _rows(cursor)
        except duckdb.CatalogException as exc:
            # The view was skipped at startup because its Parquet did not exist.
            # A 503 naming the flow that builds it beats DuckDB's "Table with
            # name X does not exist" reaching the browser as a 500.
            raise HTTPException(
                status_code=503,
                detail=(
                    "A gold table has not been built yet — run the ETL "
                    "(python -m pipeline.flows.etl_flow). Detail: " + str(exc)
                ),
            ) from exc
        finally:
            cursor.close()

    def one(self, sql: str, params: Sequence[Any] = ()) -> Row | None:
        """First row of a query, or None. For the single-row lookups."""
        rows = self.query(sql, params)
        return rows[0] if rows else None


def warehouse(request: Request) -> Warehouse:
    """FastAPI dependency: hand routes the warehouse the lifespan opened.

    It hangs off `app.state` rather than a module global so that a test can
    build an app against a fixture lake without a leftover connection from a
    previous test leaking into it.
    """
    return request.app.state.warehouse
