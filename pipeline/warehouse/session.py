"""DuckDB session: a stateless OLAP engine pointed at the lake.

The lake is the storage layer. DuckDB is *only* the query engine, and it holds
no copy of anything: every view registered here is a `read_parquet()` over the
files where the pipeline already wrote them. There is no `COPY`, no `CREATE
TABLE AS`, no `INSERT` anywhere in this package, and `get_warehouse()` opens an
in-memory database so there is not even a file it could write to.

Why that constraint is worth defending rather than merely following:

* **A materialised copy is a second source of truth.** The moment gold is
  rebuilt, a loaded table is stale and every answer it gives is confidently
  wrong. Reading the Parquet in place means the warehouse is always exactly as
  fresh as the last flow run — there is no refresh step that can be forgotten.
* **The engine becomes disposable.** Nothing is lost when the process exits, so
  the API, a notebook and a Container Apps Job can each open their own
  connection against the same lake with no coordination, no locking and no
  migration. Scaling out is starting more processes.
* **Loading would throw away the file layout.** Predicate pushdown and Hive
  partition pruning are properties of the *files*; copying rows into DuckDB's
  own storage discards `dt=` and replaces a free file-skip with a full scan.

Pointing this at Azure Blob instead of a local disk is one environment variable
(`LAKE_BACKEND=azure`). The SQL, the view names and every query in
`queries.sql` are byte-for-byte identical either way, because the key space in
the lake is identical — see `pipeline/storage.py`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb

from pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)


class MissingLayerError(RuntimeError):
    """A layer the warehouse wants to query has not been built yet.

    Raised in place of DuckDB's raw `IOException`, which reports a glob that did
    not match and leaves the reader to work out which flow produces it.
    """


@dataclass(frozen=True)
class ViewSource:
    """One registered view and the object pattern it scans."""

    view: str
    layer: str
    # Pattern in *key space* — relative to the lake root, identical on local
    # disk and in Blob Storage. Prefixes are substituted from Settings so a
    # deployment that renames `raw/` to `bronze/` needs no code change.
    pattern: str
    hive: bool
    question: str


# Silver's pattern is `dt=*/part-*.parquet` rather than `**/*.parquet` for a
# reason that would otherwise be a silent data bug: the silver flow writes
# rejected rows to `silver/_quarantine/dt=<date>/`, under the same prefix but
# with a completely different schema. A looser glob would union quarantined
# rows into the clean table and nothing would complain — DuckDB would simply
# return a wider table with a lot of new NULLs. The shape of this glob is the
# guard.
_SOURCES: tuple[ViewSource, ...] = (
    ViewSource(
        view="bronze_telemetry",
        layer="bronze",
        pattern="{bronze}/dt=*/*.parquet",
        hive=True,
        question="raw device readings exactly as they landed, still dirty",
    ),
    ViewSource(
        view="silver_telemetry",
        layer="silver",
        pattern="{silver}/dt=*/part-*.parquet",
        hive=True,
        question="cleaned, deduplicated, conformed readings with a quality audit trail",
    ),
    ViewSource(
        view="daily_equipment_summary",
        layer="gold",
        pattern="{gold}/daily_equipment_summary.parquet",
        hive=False,
        question="one row per machine per day",
    ),
    ViewSource(
        view="fleet_health_flags",
        layer="gold",
        pattern="{gold}/fleet_health_flags.parquet",
        hive=False,
        question="one row per machine currently worth a planner's attention",
    ),
    ViewSource(
        view="fleet_summary_by_type",
        layer="gold",
        pattern="{gold}/fleet_summary_by_type.parquet",
        hive=False,
        question="one row per machine class",
    ),
)

# What to tell the user to run when a layer is missing. An error that names the
# fix is the difference between a five-second and a five-minute recovery.
_BUILD_COMMAND = {
    "bronze": "python -m pipeline.generator.run_generator --days 3 --reset",
    "silver": "python -m pipeline.flows.silver_flow",
    "gold": "python -m pipeline.flows.gold_flow",
}


def _key_pattern(source: ViewSource, settings: Settings) -> str:
    return source.pattern.format(
        bronze=settings.bronze_prefix,
        silver=settings.silver_prefix,
        gold=settings.gold_prefix,
    )


def _location(source: ViewSource, settings: Settings) -> str:
    """Absolute scan target for a view, in whichever dialect the backend needs.

    Local  -> /abs/path/to/data/silver/dt=*/part-*.parquet
    Azure  -> az://<container>/silver/dt=*/part-*.parquet

    `as_posix()` even on Windows: DuckDB's globber treats backslashes as literal
    characters, so a Windows-style path silently matches nothing.
    """
    key = _key_pattern(source, settings)
    if settings.lake_backend == "azure":
        return f"az://{settings.azure_container}/{key}"
    return (settings.lake_local_root / key).as_posix()


def _sql_literal(value: str) -> str:
    """Single-quoted SQL string. DDL takes no bind parameters, so escape here."""
    return "'" + value.replace("'", "''") + "'"


def view_locations(settings: Settings | None = None) -> dict[str, str]:
    """View name -> the location it scans. Useful for logs and error messages."""
    settings = settings or get_settings()
    return {source.view: _location(source, settings) for source in _SOURCES}


def _local_layer_present(source: ViewSource, settings: Settings) -> bool:
    """Does at least one object match this pattern on local disk?

    Checked before `CREATE VIEW` rather than after, because DuckDB binds a view
    at creation time and the failure it raises ("No files found that match the
    pattern") does not say which pipeline stage was supposed to produce them.
    """
    key = _key_pattern(source, settings)
    root = settings.lake_local_root
    if "*" not in key:
        return (root / key).is_file()
    return next(root.glob(key), None) is not None


def _missing_layer_error(source: ViewSource, settings: Settings) -> MissingLayerError:
    command = _BUILD_COMMAND.get(source.layer, "the flow that builds it")
    return MissingLayerError(
        f"The {source.layer} layer is empty: nothing matches "
        f"{_location(source, settings)!r}, so the view {source.view!r} cannot be "
        f"registered.\nBuild it first:  {command}"
    )


def _configure_azure(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    """Teach this connection to read `az://` URLs.

    The extension load is guarded so a local run never touches the network:
    `LOAD azure` succeeds outright if the extension is already present (the ETL
    image bakes it in at build time), and only a failure falls through to
    `INSTALL`, which is the call that would reach for DuckDB's extension CDN.

    The credential goes into a DuckDB *secret* rather than the global
    `azure_storage_connection_string` setting: a secret is scoped to this
    connection and is redacted in `duckdb_settings()` output, so it cannot leak
    into a query plan someone pastes into a ticket.
    """
    try:
        con.execute("LOAD azure")
    except duckdb.Error:
        log.info("azure extension not present; installing from the DuckDB repository")
        con.execute("INSTALL azure")
        con.execute("LOAD azure")

    con.execute(
        "CREATE OR REPLACE SECRET fleet_lake "
        f"(TYPE azure, CONNECTION_STRING {_sql_literal(settings.azure_connection_string)})"
    )


def register_views(
    con: duckdb.DuckDBPyConnection,
    settings: Settings | None = None,
    *,
    require_gold: bool = True,
) -> list[str]:
    """Register every lake view on an existing connection. Returns the names.

    Args:
        require_gold: when False, a missing gold layer is logged and skipped
            instead of raised. The gold flow itself has no business failing to
            start just because it has not run yet; a dashboard querying gold
            very much does.
    """
    settings = settings or get_settings()
    registered: list[str] = []

    for source in _SOURCES:
        location = _location(source, settings)

        if settings.lake_backend == "local" and not _local_layer_present(source, settings):
            if source.layer == "gold" and not require_gold:
                log.warning("Skipping view %s: %s not built yet", source.view, source.layer)
                continue
            raise _missing_layer_error(source, settings)

        hive = ", hive_partitioning=true" if source.hive else ""
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {source.view} AS "
                f"SELECT * FROM read_parquet({_sql_literal(location)}{hive})"
            )
        except duckdb.IOException as exc:
            # The remote backends cannot be pre-checked as cheaply as the local
            # one (a listing is a network round trip), so the same actionable
            # error is reconstructed from the failure instead.
            if source.layer == "gold" and not require_gold:
                log.warning("Skipping view %s: %s not readable (%s)", source.view, source.layer, exc)
                continue
            raise _missing_layer_error(source, settings) from exc

        registered.append(source.view)

    log.info("Registered %d view(s) over %s", len(registered), settings.lake_backend)
    return registered


def get_warehouse(
    settings: Settings | None = None,
    *,
    require_gold: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the lake registered as views.

    In-memory on purpose — see the module docstring. The database is `:memory:`
    so the engine physically cannot persist a copy of the lake; the only state
    this connection holds is five view definitions, each of which is a string.

    The caller owns the connection and should close it (or use it as a context
    manager). It is not cached: DuckDB connections are not thread-safe, and a
    module-level singleton would turn a FastAPI worker into a queue of one.
    """
    settings = settings or get_settings()

    con = duckdb.connect(database=":memory:")

    # Every timestamp in the lake is UTC. Without this, DuckDB renders
    # TIMESTAMPTZ in the *host's* zone, so the same query prints different
    # times on a laptop in IST and in a container in UTC — and screenshots of
    # query output stop being comparable. Pinning it makes results reproducible.
    con.execute("SET TimeZone='UTC'")

    # Both optional, both read straight from the environment rather than from
    # Settings, because they are properties of the machine and not of the lake.
    # The memory limit matters in Container Apps: DuckDB otherwise sizes its
    # buffer pool from total system RAM, overshoots the container's cgroup cap
    # and gets OOM-killed rather than spilling.
    memory_limit = os.getenv("WAREHOUSE_MEMORY_LIMIT", "").strip()
    if memory_limit:
        con.execute(f"SET memory_limit={_sql_literal(memory_limit)}")
    threads = os.getenv("WAREHOUSE_THREADS", "").strip()
    if threads:
        con.execute(f"SET threads={int(threads)}")

    if settings.lake_backend == "azure":
        _configure_azure(con, settings)

    try:
        register_views(con, settings, require_gold=require_gold)
    except Exception:
        con.close()
        raise
    return con


def materialised_objects(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Base tables in DuckDB's own storage. Should always be empty.

    This is the assertion behind the claim the module docstring makes. A view is
    a stored query; a table is a stored copy. If this ever returns a name,
    someone has quietly turned the query engine into a second warehouse and the
    freshness guarantee is gone — so `run_queries.py` checks it on every run
    rather than trusting the code to stay honest by inspection.
    """
    rows = con.execute(
        "SELECT table_name FROM duckdb_tables() ORDER BY table_name"
    ).fetchall()
    return [row[0] for row in rows]
