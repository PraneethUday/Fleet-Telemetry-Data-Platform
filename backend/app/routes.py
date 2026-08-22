"""The HTTP endpoints: SQL in, JSON out.

This layer is deliberately thin, and the boundary is worth stating because it is
easy to erode. Every number the dashboard shows — the health scores, the
temperature trends, the fuel burn per hour, the per-type rollups — was computed
once by the gold flow and written to Parquet. Nothing here recomputes any of it.

Where the line is: a route may SELECT, FILTER, JOIN, ORDER and LIMIT, because
those are questions about *which* precomputed rows a client wants. A route may
not average, score, or derive. The test is whether the answer would change if
the same question were asked from a BI tool over the same Parquet — if yes, the
logic has leaked out of the warehouse into an application server, where it is
invisible to every other consumer, untested by the pipeline's tests, and
recomputed on every request instead of once per ETL run. The one aggregate in
this file (the fleet-wide totals in `/api/fleet-summary`) is a SUM over the four
rows of `fleet_summary_by_type`, expressed in SQL rather than Python for exactly
that reason: it stays a warehouse question.

The practical tell that this line has been crossed: an `import pandas` in this
file. There isn't one.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from backend.app.deps import Warehouse, warehouse as warehouse_dep
from backend.app.models import (
    EquipmentHistory,
    EquipmentListResponse,
    FleetSummary,
    HealthFlagsResponse,
    HealthResponse,
)

router = APIRouter(prefix="/api", tags=["fleet"])


# ---------------------------------------------------------------------------
# SQL
#
# Held as module constants so each statement is readable as one thing, and so
# DuckDB can reuse its prepared plan across requests instead of re-parsing a
# string that was rebuilt per call.
#
# Every optional filter uses the `(? IS NULL OR column = ?)` guard rather than
# appending a WHERE clause when a parameter happens to be set. That keeps the
# statement a single constant with a fixed parameter count — there is no code
# path where a value could reach the SQL by any route other than a bind
# parameter, which is a stronger guarantee than "we remembered to escape it".
# ---------------------------------------------------------------------------

FLEET_TOTALS_SQL = """
SELECT max(generated_at_utc)             AS generated_at_utc,
       coalesce(sum(machine_count), 0)   AS fleet_size,
       coalesce(sum(machines_flagged), 0) AS machines_flagged,
       coalesce(sum(critical_count), 0)  AS critical_count,
       coalesce(sum(warning_count), 0)   AS warning_count,
       coalesce(max(days_covered), 0)    AS days_covered
FROM fleet_summary_by_type
"""

# The two bookkeeping columns are dropped rather than SELECT *'d: they describe
# the table, not a machine class, and are lifted to the enclosing object.
FLEET_BY_TYPE_SQL = """
SELECT equipment_type,
       machine_count,
       active_machines_24h,
       avg_coolant_temp_c,
       max_coolant_temp_c,
       avg_oil_pressure_psi,
       total_operating_hours,
       avg_fuel_burn_pct_per_hour,
       total_fault_codes,
       machines_flagged,
       pct_flagged,
       critical_count,
       warning_count
FROM fleet_summary_by_type
ORDER BY machine_count DESC, equipment_type ASC
"""

# ORDER BY before LIMIT is load-bearing, not cosmetic: `limit` has to truncate
# the machines a planner cares least about. Sorting critical first and then by
# ascending health score means the rows that fall off the end are always the
# least urgent ones.
HEALTH_FLAGS_SQL = """
SELECT *
FROM fleet_health_flags
WHERE (? IS NULL OR equipment_type = ?)
  AND (? IS NULL OR severity = ?)
ORDER BY (severity = 'critical') DESC, health_score ASC, equipment_id ASC
LIMIT ?
"""

# The roster is every machine gold knows about, LEFT JOINed to its flag. A flag
# row exists only for an unhealthy machine, so the join miss *is* the "healthy"
# answer — which is why severity and health_score are null rather than invented
# defaults. `arg_max(equipment_type, day)` takes the class as of the machine's
# most recent day instead of assuming it never changes.
EQUIPMENT_SQL = """
WITH machines AS (
    SELECT equipment_id,
           arg_max(equipment_type, day) AS equipment_type,
           max(day)                     AS last_day
    FROM daily_equipment_summary
    GROUP BY equipment_id
)
SELECT m.equipment_id,
       m.equipment_type,
       m.last_day,
       f.severity,
       f.health_score
FROM machines m
LEFT JOIN fleet_health_flags f ON f.equipment_id = m.equipment_id
WHERE (? IS NULL OR m.equipment_type = ?)
  AND (? = FALSE OR f.equipment_id IS NOT NULL)
ORDER BY f.health_score ASC NULLS LAST, m.equipment_id ASC
LIMIT ?
"""

# Aggregating with no GROUP BY always yields exactly one row, so an unknown
# machine comes back as (NULL, NULL) rather than as an empty result. That makes
# "does this machine exist in gold at all" a single cheap question, separable
# from "does it have rows inside the requested window" — the two have different
# answers (404 versus an empty series) and conflating them would 404 a real
# machine that has simply been quiet.
EQUIPMENT_IDENTITY_SQL = """
SELECT arg_max(equipment_type, day) AS equipment_type,
       max(day)                     AS last_day
FROM daily_equipment_summary
WHERE equipment_id = ?
"""

# The window is anchored to the newest day in the *table*, not to the newest day
# for this machine. "The last 30 days" has to mean the same span for every
# machine on the page, otherwise a unit that stopped reporting a month ago would
# silently render a full-looking chart of stale readings next to a live one.
EQUIPMENT_HISTORY_SQL = """
SELECT *
FROM daily_equipment_summary
WHERE equipment_id = ?
  AND day > (SELECT max(day) FROM daily_equipment_summary) - CAST(? AS INTEGER)
ORDER BY day ASC
"""

EQUIPMENT_FLAG_SQL = """
SELECT * FROM fleet_health_flags WHERE equipment_id = ?
"""


def _canonical_type(equipment_type: str | None) -> str | None:
    """Normalise an equipment_type filter to the form silver conformed it to.

    Silver collapses 24 raw spellings to 4 lowercase snake_case values, so the
    gold column only ever holds those. Matching the filter to that form means
    `?equipment_type=Excavator` from a hand-typed URL finds rows instead of
    quietly returning an empty list, which reads like "no excavators are
    flagged" — a wrong answer, not an error.
    """
    if equipment_type is None:
        return None
    cleaned = equipment_type.strip().lower()
    return cleaned or None


@router.get("/health", response_model=HealthResponse)
def health(warehouse: Warehouse = Depends(warehouse_dep)) -> HealthResponse:
    """Liveness probe. Answers 200 even when the lake is empty.

    Reporting missing tables instead of failing is the whole design: on Azure
    Container Apps a non-200 here means the revision is unhealthy and gets
    restarted, so an API that 503'd because the ETL had not run yet would be
    killed in a loop while the actual fix ("run the ETL") was never shown to
    anyone. The frontend reads `gold_tables` and renders that instruction.
    """
    tables = warehouse.gold_tables()
    ready = warehouse.connection is not None and all(tables.values())
    return HealthResponse(
        status="ok" if ready else "degraded",
        lake_backend=warehouse.settings.lake_backend,
        gold_tables=tables,
    )


@router.get("/fleet-summary", response_model=FleetSummary)
def fleet_summary(warehouse: Warehouse = Depends(warehouse_dep)) -> FleetSummary:
    """The dashboard's header: fleet-wide counters plus the per-class breakdown."""
    totals = warehouse.one(FLEET_TOTALS_SQL) or {}
    by_type = warehouse.query(FLEET_BY_TYPE_SQL)

    return FleetSummary(
        generated_at_utc=totals.get("generated_at_utc"),
        fleet_size=totals.get("fleet_size") or 0,
        machines_flagged=totals.get("machines_flagged") or 0,
        critical_count=totals.get("critical_count") or 0,
        warning_count=totals.get("warning_count") or 0,
        days_covered=totals.get("days_covered") or 0,
        by_type=by_type,
    )


@router.get("/health-flags", response_model=HealthFlagsResponse)
def health_flags(
    warehouse: Warehouse = Depends(warehouse_dep),
    equipment_type: str | None = Query(None, description="Canonical class, e.g. excavator"),
    # A Literal rather than a free string: a typo'd severity should be a 422
    # naming the two legal values, not a 200 with an empty list that reads as
    # "nothing is critical".
    severity: Literal["warning", "critical"] | None = Query(None),
    limit: int = Query(200, ge=1, le=5000),
) -> HealthFlagsResponse:
    """Machines currently worth a maintenance planner's attention, worst first."""
    machine_class = _canonical_type(equipment_type)
    items = warehouse.query(
        HEALTH_FLAGS_SQL,
        [machine_class, machine_class, severity, severity, limit],
    )
    return HealthFlagsResponse(count=len(items), items=items)


@router.get("/equipment", response_model=EquipmentListResponse)
def equipment(
    warehouse: Warehouse = Depends(warehouse_dep),
    equipment_type: str | None = Query(None),
    flagged_only: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
) -> EquipmentListResponse:
    """The fleet roster — one row per machine, with its flag if it has one."""
    machine_class = _canonical_type(equipment_type)
    items = warehouse.query(
        EQUIPMENT_SQL,
        [machine_class, machine_class, flagged_only, limit],
    )
    return EquipmentListResponse(count=len(items), items=items)


@router.get("/equipment/{equipment_id}/history", response_model=EquipmentHistory)
def equipment_history(
    equipment_id: str = Path(..., min_length=1, max_length=64),
    warehouse: Warehouse = Depends(warehouse_dep),
    days: int = Query(30, ge=1, le=3650),
) -> EquipmentHistory:
    """One machine's daily series plus its current flag, in a single request.

    Bundling the flag in avoids the detail view doing a second round trip to
    /api/health-flags just to find out whether the machine it is already
    displaying is flagged.
    """
    identity = warehouse.one(EQUIPMENT_IDENTITY_SQL, [equipment_id]) or {}
    if identity.get("last_day") is None:
        # Distinguishable from "known machine, no readings in the window", which
        # is a 200 with an empty `days`.
        raise HTTPException(status_code=404, detail=f"unknown equipment_id: {equipment_id}")

    return EquipmentHistory(
        equipment_id=equipment_id,
        equipment_type=identity["equipment_type"],
        days=warehouse.query(EQUIPMENT_HISTORY_SQL, [equipment_id, days]),
        flag=warehouse.one(EQUIPMENT_FLAG_SQL, [equipment_id]),
    )
