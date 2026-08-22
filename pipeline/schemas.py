"""Explicit Parquet schemas for each lakehouse layer.

Declaring the schema (rather than letting pandas infer it per batch) matters in
a lakehouse: every file written under a prefix must be readable as one table by
DuckDB/Spark. Inferred types drift — a batch where `fault_code` happens to be
all-null infers as `null` type and poisons the scan. Pinning the schema makes
each layer a real contract.
"""

from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# BRONZE (raw landing zone)
#
# Two deliberate choices here, both central to the bronze layer's job:
#
# 1. `timestamp` is a STRING, not a Parquet timestamp. Devices in the field send
#    mixed formats and offsets ("2026-08-22T06:05:00Z", "2026-08-22 16:05:00",
#    "2026-08-22T16:05:00+10:00"). Parsing at ingest would silently destroy the
#    evidence of that inconsistency. Bronze keeps what the device actually sent;
#    silver is where it becomes a real UTC timestamp.
#
# 2. Every numeric column is nullable float64 and is NOT range-checked. A fuel
#    reading of 127.4% is physically impossible and is written anyway — that is
#    the point of a raw zone. If we cleaned here we could never reprocess with a
#    corrected rule, or audit what the sensor originally claimed.
#
# The `_`-prefixed columns are ingestion lineage, not telemetry: which batch a
# row arrived in and when we received it. Silver dedup keys off them.
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = pa.schema(
    [
        # Nullable, deliberately. A landing zone that cannot accept a record
        # missing its device id is a landing zone that CRASHES on the exact
        # malformed input it exists to capture — and a raw writer that rejects
        # rows is no longer a raw writer. Silver is where a row with no
        # equipment_id gets quarantined with a reason; bronze just stores it.
        pa.field("equipment_id", pa.string(), nullable=True),
        pa.field("equipment_type", pa.string(), nullable=True),
        pa.field("timestamp", pa.string(), nullable=True),
        pa.field("engine_hours", pa.float64(), nullable=True),
        pa.field("fuel_level_pct", pa.float64(), nullable=True),
        pa.field("coolant_temp_c", pa.float64(), nullable=True),
        pa.field("oil_pressure_psi", pa.float64(), nullable=True),
        pa.field("gps_lat", pa.float64(), nullable=True),
        pa.field("gps_lon", pa.float64(), nullable=True),
        pa.field("fault_code", pa.string(), nullable=True),
        pa.field("_ingest_batch_id", pa.string(), nullable=False),
        pa.field("_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

BRONZE_TELEMETRY_COLUMNS = [
    "equipment_id",
    "equipment_type",
    "timestamp",
    "engine_hours",
    "fuel_level_pct",
    "coolant_temp_c",
    "oil_pressure_psi",
    "gps_lat",
    "gps_lon",
    "fault_code",
]

# Canonical equipment types. The generator emits messy variants of these
# ("EXCAVATOR", " Excavator", "generator-engine"); silver maps them back.
EQUIPMENT_TYPES = ("excavator", "dozer", "loader", "generator_engine")


# ---------------------------------------------------------------------------
# SILVER (cleaned, conformed layer)
#
# What changes versus bronze, and why each change belongs *here* and not upstream:
#
#   timestamp (string) -> event_time (timestamp, UTC)
#       All four raw encodings are parsed to one instant. Bronze could not do
#       this without destroying the evidence of the inconsistency.
#   equipment_type -> canonical lowercase snake_case
#       "EXCAVATOR", " excavator", "wheel loader" all collapse to one value, so
#       a GROUP BY in the gold layer returns 4 rows instead of 24.
#   out-of-range sensor values -> NULL + a quality flag
#       The *row* survives; only the bad field is nulled. Discarding a whole
#       reading because one sensor glitched would throw away five good sensors.
#   duplicates -> one row per (equipment_id, event_time)
#       Resolved by keeping the earliest ingest, so replays are idempotent.
#
# `quality_flags` is a comma-separated audit trail of every rule that fired on
# the row. It is what makes the cleaning defensible rather than magical: you can
# always ask silver "what did you change, and why".
# ---------------------------------------------------------------------------
SILVER_SCHEMA = pa.schema(
    [
        pa.field("equipment_id", pa.string(), nullable=False),
        pa.field("equipment_type", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_date", pa.date32(), nullable=False),
        pa.field("engine_hours", pa.float64(), nullable=True),
        pa.field("fuel_level_pct", pa.float64(), nullable=True),
        pa.field("coolant_temp_c", pa.float64(), nullable=True),
        pa.field("oil_pressure_psi", pa.float64(), nullable=True),
        pa.field("gps_lat", pa.float64(), nullable=True),
        pa.field("gps_lon", pa.float64(), nullable=True),
        pa.field("fault_code", pa.string(), nullable=True),
        pa.field("has_fault", pa.bool_(), nullable=False),
        # True when the engine was under load. Parked machines legitimately read
        # 0 psi and ambient temperature; averaging those in would drag every
        # fleet statistic toward nonsense, so downstream aggregations filter on
        # this rather than re-deriving the rule.
        pa.field("is_under_load", pa.bool_(), nullable=False),
        pa.field("quality_flags", pa.string(), nullable=True),
        pa.field("_ingest_batch_id", pa.string(), nullable=True),
        pa.field("_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("_silver_processed_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Rows that cannot be repaired at all (no equipment_id, or a timestamp nothing
# can parse) go here instead of silver. Quarantining rather than dropping keeps
# the pipeline auditable — "we processed 429,290 rows and rejected 118" is a
# statement you can defend; a silent row-count drop is not.
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("equipment_id", pa.string(), nullable=True),
        pa.field("equipment_type", pa.string(), nullable=True),
        pa.field("timestamp", pa.string(), nullable=True),
        pa.field("reject_reason", pa.string(), nullable=False),
        pa.field("_ingest_batch_id", pa.string(), nullable=True),
        pa.field("_quarantined_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Engine is considered under load above this oil pressure. Below it the machine
# is parked or in the workshop.
UNDER_LOAD_OIL_PSI = 5.0

# Physical validity ranges. A value outside its range is nulled and flagged.
VALID_RANGES: dict[str, tuple[float, float]] = {
    "engine_hours": (0.0, 200_000.0),
    "fuel_level_pct": (0.0, 100.0),
    "coolant_temp_c": (-50.0, 200.0),
    "oil_pressure_psi": (0.0, 200.0),
    "gps_lat": (-90.0, 90.0),
    "gps_lon": (-180.0, 180.0),
}


# ---------------------------------------------------------------------------
# GOLD (analytics-ready layer)
#
# Deliberately NOT date-partitioned. These are small, already-aggregated tables
# describing current state; a dashboard reads all of each one every time. Hive
# partitioning exists to let a query skip files, and there is nothing here worth
# skipping — partitioning them would add path complexity and buy nothing.
# ---------------------------------------------------------------------------

# One row per machine per day. The grain a fleet manager actually reports on.
GOLD_DAILY_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("equipment_id", pa.string(), nullable=False),
        pa.field("equipment_type", pa.string(), nullable=False),
        pa.field("day", pa.date32(), nullable=False),
        pa.field("readings_count", pa.int64(), nullable=False),
        pa.field("under_load_readings", pa.int64(), nullable=False),
        pa.field("avg_coolant_temp_c", pa.float64(), nullable=True),
        pa.field("max_coolant_temp_c", pa.float64(), nullable=True),
        pa.field("p95_coolant_temp_c", pa.float64(), nullable=True),
        pa.field("avg_oil_pressure_psi", pa.float64(), nullable=True),
        pa.field("min_oil_pressure_psi", pa.float64(), nullable=True),
        pa.field("engine_hours_start", pa.float64(), nullable=True),
        pa.field("engine_hours_end", pa.float64(), nullable=True),
        pa.field("operating_hours", pa.float64(), nullable=True),
        # Sum of positive tank-level drops only. Naively doing max-min would
        # report a NEGATIVE consumption on any day the machine was refuelled.
        pa.field("fuel_consumed_pct", pa.float64(), nullable=True),
        pa.field("fuel_burn_pct_per_hour", pa.float64(), nullable=True),
        pa.field("refuel_events", pa.int64(), nullable=False),
        pa.field("fault_code_count", pa.int64(), nullable=False),
        pa.field("distinct_fault_codes", pa.int64(), nullable=False),
        pa.field("last_gps_lat", pa.float64(), nullable=True),
        pa.field("last_gps_lon", pa.float64(), nullable=True),
    ]
)

# Machines currently worth a maintenance planner's attention. One row per
# flagged machine — healthy machines are absent, not present with a false flag.
GOLD_HEALTH_FLAGS_SCHEMA = pa.schema(
    [
        pa.field("equipment_id", pa.string(), nullable=False),
        pa.field("equipment_type", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),  # "warning" | "critical"
        pa.field("health_score", pa.float64(), nullable=False),  # 100 = healthy
        pa.field("flag_reasons", pa.string(), nullable=False),  # comma-separated
        pa.field("temp_trend_c_per_hour", pa.float64(), nullable=True),
        pa.field("recent_avg_temp_c", pa.float64(), nullable=True),
        pa.field("recent_max_temp_c", pa.float64(), nullable=True),
        pa.field("recent_min_oil_psi", pa.float64(), nullable=True),
        pa.field("recent_fault_count", pa.int64(), nullable=False),
        pa.field("distinct_fault_codes", pa.int64(), nullable=False),
        pa.field("top_fault_code", pa.string(), nullable=True),
        pa.field("readings_in_window", pa.int64(), nullable=False),
        pa.field("hours_since_last_reading", pa.float64(), nullable=True),
        pa.field("last_seen_utc", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("last_gps_lat", pa.float64(), nullable=True),
        pa.field("last_gps_lon", pa.float64(), nullable=True),
        pa.field("window_hours", pa.int64(), nullable=False),
        pa.field("generated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Fleet rollup by machine class — the dashboard's summary cards.
GOLD_FLEET_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("equipment_type", pa.string(), nullable=False),
        pa.field("machine_count", pa.int64(), nullable=False),
        pa.field("active_machines_24h", pa.int64(), nullable=False),
        pa.field("avg_coolant_temp_c", pa.float64(), nullable=True),
        pa.field("max_coolant_temp_c", pa.float64(), nullable=True),
        pa.field("avg_oil_pressure_psi", pa.float64(), nullable=True),
        pa.field("total_operating_hours", pa.float64(), nullable=True),
        pa.field("avg_fuel_burn_pct_per_hour", pa.float64(), nullable=True),
        pa.field("total_fault_codes", pa.int64(), nullable=False),
        pa.field("machines_flagged", pa.int64(), nullable=False),
        pa.field("pct_flagged", pa.float64(), nullable=False),
        pa.field("critical_count", pa.int64(), nullable=False),
        pa.field("warning_count", pa.int64(), nullable=False),
        pa.field("days_covered", pa.int64(), nullable=False),
        pa.field("generated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Gold object keys, relative to GOLD_PREFIX. Flat, single-file-per-table.
GOLD_TABLES = {
    "daily_equipment_summary": GOLD_DAILY_SUMMARY_SCHEMA,
    "fleet_health_flags": GOLD_HEALTH_FLAGS_SCHEMA,
    "fleet_summary_by_type": GOLD_FLEET_SUMMARY_SCHEMA,
}
