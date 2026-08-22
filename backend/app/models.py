"""Pydantic response models — the server's half of the HTTP contract.

Why declare these at all when every route could return a bare dict: the React
dashboard is written against a hand-maintained TypeScript mirror of this
contract (frontend/src/api/types.ts) and the two are developed in parallel. A
declared response model is what makes the contract checkable — it shows up in
/docs, and a gold column that silently changes type fails here at the boundary
rather than as `undefined` in a table cell in someone's browser.

Two deliberate looseness decisions:

1. `HealthFlag` and `DailySummary` set `extra="allow"`. The contract promises
   "ALL columns" of the corresponding gold row, so a column added to
   pipeline/schemas.py must reach the client without an edit here. Listing the
   known columns still documents them; allowing extras stops this file from
   becoming a silent filter that drops new gold work on the floor.

2. `severity` is `str`, not `Literal["warning", "critical"]`. A serving layer
   should not return 500 because the gold layer invented a third severity — a
   read API's job is to hand over what the warehouse computed, and validating
   gold's own content belongs in the gold flow. The frontend narrows the union
   on its side, where a surprise value degrades a badge instead of an endpoint.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    """GET /api/health — the container liveness probe.

    `gold_tables` maps each expected gold table to whether it is present in the
    lake. It is reported rather than asserted: an API that 500s because the ETL
    has not run yet looks identical to an API that has crashed, and the platform
    would restart it forever. This way the probe stays green and the dashboard
    can tell the operator to run `make etl`.
    """

    status: str
    lake_backend: str
    gold_tables: dict[str, bool]


class FleetTypeSummary(BaseModel):
    """One row of gold `fleet_summary_by_type`, minus its bookkeeping columns.

    `generated_at_utc` and `days_covered` are per-table facts, not per-type
    ones, so they are lifted to the enclosing FleetSummary instead of repeated
    on every element.
    """

    equipment_type: str
    machine_count: int
    active_machines_24h: int
    avg_coolant_temp_c: float | None
    max_coolant_temp_c: float | None
    avg_oil_pressure_psi: float | None
    total_operating_hours: float | None
    avg_fuel_burn_pct_per_hour: float | None
    total_fault_codes: int
    machines_flagged: int
    pct_flagged: float
    critical_count: int
    warning_count: int


class FleetSummary(BaseModel):
    """GET /api/fleet-summary — the dashboard's header cards plus the breakdown."""

    generated_at_utc: str | None
    fleet_size: int
    machines_flagged: int
    critical_count: int
    warning_count: int
    days_covered: int
    by_type: list[FleetTypeSummary]


class HealthFlag(BaseModel):
    """One row of gold `fleet_health_flags` — a machine worth a planner's time."""

    model_config = ConfigDict(extra="allow")

    equipment_id: str
    equipment_type: str
    severity: str
    health_score: float
    # Comma-separated rule names ("temp_trend,low_oil_pressure"). Kept as the
    # raw string the gold layer wrote instead of a pre-split list: the UI wants
    # chips, a CSV export wants the string, and splitting is not the API's call.
    flag_reasons: str
    temp_trend_c_per_hour: float | None
    recent_avg_temp_c: float | None
    recent_max_temp_c: float | None
    recent_min_oil_psi: float | None
    recent_fault_count: int
    distinct_fault_codes: int
    top_fault_code: str | None
    readings_in_window: int
    hours_since_last_reading: float | None
    last_seen_utc: str | None
    last_gps_lat: float | None
    last_gps_lon: float | None
    window_hours: int
    generated_at_utc: str | None


class HealthFlagsResponse(BaseModel):
    """GET /api/health-flags.

    `count` is the number of items in this response, not the size of the
    unfiltered match — the client already knows how many rows it received, and a
    second COUNT(*) round trip to tell it the same thing twice would be waste.
    """

    count: int
    items: list[HealthFlag]


class EquipmentListItem(BaseModel):
    """One machine in the fleet roster.

    `severity` and `health_score` are null for a healthy machine: gold's
    `fleet_health_flags` holds only flagged machines, so this is a LEFT JOIN
    miss, not a missing value.
    """

    equipment_id: str
    equipment_type: str
    last_day: str
    severity: str | None
    health_score: float | None


class EquipmentListResponse(BaseModel):
    """GET /api/equipment."""

    count: int
    items: list[EquipmentListItem]


class DailySummary(BaseModel):
    """One row of gold `daily_equipment_summary` — a machine on one day."""

    model_config = ConfigDict(extra="allow")

    equipment_id: str
    equipment_type: str
    # "YYYY-MM-DD". A calendar date, not an instant: the aggregation grain is a
    # day, and serialising it as a midnight timestamp would invite a client in
    # another timezone to render it as the day before.
    day: str
    readings_count: int
    under_load_readings: int
    avg_coolant_temp_c: float | None
    max_coolant_temp_c: float | None
    p95_coolant_temp_c: float | None
    avg_oil_pressure_psi: float | None
    min_oil_pressure_psi: float | None
    engine_hours_start: float | None
    engine_hours_end: float | None
    operating_hours: float | None
    fuel_consumed_pct: float | None
    fuel_burn_pct_per_hour: float | None
    refuel_events: int
    fault_code_count: int
    distinct_fault_codes: int
    last_gps_lat: float | None
    last_gps_lon: float | None


class EquipmentHistory(BaseModel):
    """GET /api/equipment/{equipment_id}/history.

    `days` is ascending so the client can plot it without sorting, and `flag`
    carries the machine's current health row (or null) so the detail view is one
    request rather than a fan-out to /api/health-flags per machine.
    """

    equipment_id: str
    equipment_type: str
    days: list[DailySummary]
    flag: HealthFlag | None
