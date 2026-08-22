"""End-to-end contract tests across bronze -> silver -> gold.

The unit tests elsewhere pin each layer in isolation. These pin the *seams*,
which is where layered pipelines actually break: a silver change that quietly
alters a column type, a gold aggregation that assumes a filter silver stopped
applying, a rerun that doubles every row.

Everything runs against a throwaway lake in tmp_path, generated from a fixed
seed, so the assertions are about pipeline behaviour and never about whatever
happens to be sitting in ./data.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pytest

from pipeline.generator.simulator import FleetSimulator, GeneratorConfig
from pipeline.schemas import (
    EQUIPMENT_TYPES,
    GOLD_TABLES,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMA,
    VALID_RANGES,
)
from pipeline.storage import get_storage

pytestmark = pytest.mark.integration

WINDOW_START = datetime(2026, 8, 18, tzinfo=timezone.utc)
WINDOW_HOURS = 60


@pytest.fixture
def seeded_lake(lake_env):
    """A small but complete bronze layer: 120 machines over 2.5 days."""
    storage = get_storage(lake_env)
    cfg = GeneratorConfig(fleet_size=120, seed=5, interval_seconds=300, batch_minutes=180)
    sim = FleetSimulator(cfg)
    end = WINDOW_START + timedelta(hours=WINDOW_HOURS)

    from pipeline.schemas import BRONZE_SCHEMA

    rows = 0
    for batch in sim.simulate(WINDOW_START, end):
        key = f"{lake_env.bronze_prefix}/dt={batch.partition_date}/{batch.filename}"
        storage.write_parquet(batch.frame, key, schema=BRONZE_SCHEMA)
        rows += len(batch.frame)
    return {"settings": lake_env, "storage": storage, "sim": sim, "bronze_rows": rows}


@pytest.fixture
def silver_built(seeded_lake):
    from pipeline.flows.silver_flow import silver_flow

    result = silver_flow(full_refresh=True)
    return {**seeded_lake, "silver_result": result}


@pytest.fixture
def gold_built(silver_built):
    from pipeline.flows.gold_flow import gold_flow

    result = gold_flow()
    return {**silver_built, "gold_result": result}


def read_silver(ctx) -> pd.DataFrame:
    storage, settings = ctx["storage"], ctx["settings"]
    keys = storage.list_keys(settings.silver_prefix)
    keys = [k for k in keys if "_quarantine" not in k]
    assert keys, "silver flow produced no output"
    return storage.read_parquet_many(keys)


def read_gold(ctx, table: str) -> pd.DataFrame:
    return ctx["storage"].read_parquet(f"{ctx['settings'].gold_prefix}/{table}.parquet")


# ---------------------------------------------------------------------------
# Bronze -> silver
# ---------------------------------------------------------------------------
def test_silver_conforms_to_its_declared_schema(silver_built):
    df = read_silver(silver_built)
    pa.Table.from_pandas(df, schema=SILVER_SCHEMA, preserve_index=False)


def test_silver_collapses_every_type_spelling_to_four(silver_built):
    types = set(read_silver(silver_built)["equipment_type"].unique())
    assert types <= set(EQUIPMENT_TYPES)
    assert len(types) == 4, f"expected 4 canonical types, got {sorted(types)}"


def test_silver_has_no_duplicate_business_keys(silver_built):
    """Bronze deliberately contains cross-batch resends that survive a naive
    drop_duplicates(). If any reach silver, every gold average is weighted wrong."""
    df = read_silver(silver_built)
    dupes = df.duplicated(subset=["equipment_id", "event_time"]).sum()
    assert dupes == 0, f"{dupes} duplicate (equipment_id, event_time) rows in silver"


def test_silver_enforces_every_declared_range(silver_built):
    df = read_silver(silver_built)
    for column, (low, high) in VALID_RANGES.items():
        values = df[column].dropna()
        assert values.between(low, high).all(), f"{column} escaped its valid range"


def test_silver_event_time_is_a_real_utc_timestamp(silver_built):
    df = read_silver(silver_built)
    assert isinstance(df["event_time"].dtype, pd.DatetimeTZDtype)
    assert str(df["event_time"].dt.tz) == "UTC"
    assert df["event_time"].notna().all()


def test_silver_recovers_the_original_instants_not_shifted_ones(silver_built):
    """The timezone trap, asserted directly.

    Bronze encodes the same 5-minute grid four different ways, including naive
    strings alongside "+10:00" ones. Parsing them in a single pandas call makes
    the naive values inherit the array's inferred offset and land hours away.
    Every reading is emitted on a 5-minute boundary inside a known window, so a
    misparse shows up as an off-grid or out-of-window instant.
    """
    df = read_silver(silver_built)
    end = WINDOW_START + timedelta(hours=WINDOW_HOURS)
    assert df["event_time"].min() >= WINDOW_START
    assert df["event_time"].max() < end
    off_grid = (df["event_time"].dt.minute % 5 != 0) | (df["event_time"].dt.second != 0)
    assert not off_grid.any(), f"{off_grid.sum()} readings landed off the 5-minute grid"


def test_silver_event_date_agrees_with_event_time(silver_built):
    df = read_silver(silver_built)
    derived = pd.to_datetime(df["event_time"]).dt.tz_convert("UTC").dt.date
    assert (pd.to_datetime(df["event_date"]).dt.date == derived).all()


def test_silver_row_count_is_explained_not_merely_smaller(silver_built):
    """A shrinking row count is only acceptable if we can say where rows went."""
    df = read_silver(silver_built)
    bronze = silver_built["bronze_rows"]
    assert 0.90 * bronze < len(df) <= bronze
    result = silver_built["silver_result"]
    assert isinstance(result, dict) and result, "silver_flow must return a summary dict"


def test_silver_keeps_rows_with_one_bad_sensor(silver_built):
    """Nulling a field is the right response to a sensor glitch; dropping the
    row throws away five good sensors that were on it."""
    df = read_silver(silver_built)
    flagged = df[df["quality_flags"].notna() & (df["quality_flags"] != "")]
    assert len(flagged) > 0, "no quality flags recorded — cleaning did nothing"
    # A flagged row still carries at least one usable measurement.
    sensors = ["engine_hours", "fuel_level_pct", "coolant_temp_c", "oil_pressure_psi"]
    assert flagged[sensors].notna().any(axis=1).mean() > 0.9


def test_silver_is_idempotent(silver_built):
    """The hourly Container Apps Job will reprocess overlapping windows. If a
    rerun appended instead of replacing, the lake would grow without bound and
    every average would drift."""
    before = read_silver(silver_built)
    from pipeline.flows.silver_flow import silver_flow

    silver_flow(full_refresh=True)
    after = read_silver(silver_built)
    assert len(before) == len(after)

    # `_silver_processed_at_utc` records *when this run happened* and is expected
    # to move. Idempotency is a claim about the data, not about the lineage
    # stamp — comparing it would make the assertion unfalsifiable in the wrong
    # direction, failing on a correct rerun.
    key = ["equipment_id", "event_time"]
    lineage = ["_silver_processed_at_utc"]
    pd.testing.assert_frame_equal(
        before.sort_values(key).drop(columns=lineage).reset_index(drop=True),
        after.sort_values(key).drop(columns=lineage).reset_index(drop=True),
        check_like=True,
    )


# ---------------------------------------------------------------------------
# Silver -> gold
# ---------------------------------------------------------------------------
def test_every_gold_table_exists_and_matches_its_schema(gold_built):
    for table, schema in GOLD_TABLES.items():
        df = read_gold(gold_built, table)
        assert len(df) > 0, f"gold table {table} is empty"
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def test_daily_summary_grain_is_one_row_per_machine_per_day(gold_built):
    df = read_gold(gold_built, "daily_equipment_summary")
    assert not df.duplicated(subset=["equipment_id", "day"]).any()


def test_fuel_consumption_survives_refuelling(gold_built):
    """max-minus-min goes negative on any day a machine was refuelled. This is
    the trap the daily summary has to step around, so assert it did."""
    df = read_gold(gold_built, "daily_equipment_summary")
    consumed = df["fuel_consumed_pct"].dropna()
    assert (consumed >= 0).all(), "negative fuel consumption — refuels not handled"
    refuelled = df[df["refuel_events"] > 0]
    assert len(refuelled) > 0, "no refuels in the window; the trap is untested"
    assert (refuelled["fuel_consumed_pct"].fillna(0) > 0).mean() > 0.9


def test_operating_hours_are_never_negative_and_never_exceed_the_day(gold_built):
    df = read_gold(gold_built, "daily_equipment_summary")
    hours = df["operating_hours"].dropna()
    assert (hours >= 0).all()
    assert (hours <= 24.01).all()


def test_health_flags_cover_a_plausible_share_of_the_fleet(gold_built):
    """Flagging nothing means the rules are dead; flagging everything means they
    are noise. Either way the dashboard is useless."""
    flags = read_gold(gold_built, "fleet_health_flags")
    silver = read_silver(gold_built)
    fleet = silver["equipment_id"].nunique()
    share = len(flags) / fleet
    assert 0.01 < share < 0.40, f"{share:.1%} of the fleet flagged"


def test_health_flags_are_one_row_per_machine_with_real_reasons(gold_built):
    flags = read_gold(gold_built, "fleet_health_flags")
    assert not flags["equipment_id"].duplicated().any()
    assert flags["severity"].isin(["warning", "critical"]).all()
    assert flags["flag_reasons"].str.len().gt(0).all()
    assert flags["health_score"].between(0, 100).all()


def test_flagged_machines_are_measurably_sicker_than_the_rest(gold_built):
    """The point of the gold layer. If flagged machines are statistically
    indistinguishable from the fleet, the rules are picking noise."""
    flags = read_gold(gold_built, "fleet_health_flags")
    daily = read_gold(gold_built, "daily_equipment_summary")
    flagged = set(flags["equipment_id"])
    by_machine = daily.groupby("equipment_id").agg(
        peak=("max_coolant_temp_c", "max"), faults=("fault_code_count", "sum")
    )
    sick = by_machine[by_machine.index.isin(flagged)]
    well = by_machine[~by_machine.index.isin(flagged)]
    assert len(sick) > 0 and len(well) > 0
    assert sick["faults"].mean() > well["faults"].mean()


def test_flags_agree_with_the_planted_failures(gold_built):
    """Cross-check against ground truth the generator recorded. Rule-based
    detection will not be perfect, and should not be — but it must beat chance
    by a wide margin or the thresholds are wrong."""
    flags = read_gold(gold_built, "fleet_health_flags")
    roster = gold_built["sim"].roster().set_index("equipment_id")
    planted = set(roster.index[roster["planned_failure_mode"].notna()])
    flagged = set(flags["equipment_id"])
    fleet = set(roster.index)

    caught = len(planted & flagged)
    base_rate = len(planted) / len(fleet)
    precision = caught / max(len(flagged), 1)
    assert precision > 2 * base_rate, (
        f"precision {precision:.1%} barely beats the {base_rate:.1%} base rate"
    )


def test_fleet_summary_has_one_row_per_type_and_ties_out(gold_built):
    summary = read_gold(gold_built, "fleet_summary_by_type")
    flags = read_gold(gold_built, "fleet_health_flags")
    silver = read_silver(gold_built)

    assert set(summary["equipment_type"]) == set(silver["equipment_type"].unique())
    assert not summary["equipment_type"].duplicated().any()
    assert summary["machine_count"].sum() == silver["equipment_id"].nunique()
    assert summary["machines_flagged"].sum() == len(flags)
    assert (summary["critical_count"] + summary["warning_count"] == summary["machines_flagged"]).all()
    assert summary["generated_at_utc"].nunique() == 1


def test_gold_is_a_full_rebuild_not_an_append(gold_built):
    from pipeline.flows.gold_flow import gold_flow

    before = {t: len(read_gold(gold_built, t)) for t in GOLD_TABLES}
    gold_flow()
    after = {t: len(read_gold(gold_built, t)) for t in GOLD_TABLES}
    assert before == after


def test_etl_flow_runs_both_stages_end_to_end(seeded_lake):
    from pipeline.flows.etl_flow import etl_flow

    result = etl_flow()
    assert isinstance(result, dict)
    storage, settings = seeded_lake["storage"], seeded_lake["settings"]
    for table in GOLD_TABLES:
        assert storage.exists(f"{settings.gold_prefix}/{table}.parquet")


# ---------------------------------------------------------------------------
# Quarantine
#
# The generator's dirt is all *repairable* — a bad range, a duplicate, an odd
# spelling — so a normal run quarantines nothing and this path never executes.
# That is precisely why it needs a test: an untested reject path is one that
# discovers its own bugs in production, on the day something upstream breaks.
# ---------------------------------------------------------------------------
@pytest.fixture
def lake_with_unrepairable_rows(seeded_lake):
    """Append a bronze batch containing rows nothing can rescue."""
    from pipeline.schemas import BRONZE_SCHEMA

    storage, settings = seeded_lake["storage"], seeded_lake["settings"]
    day = WINDOW_START.strftime("%Y-%m-%d")
    good = storage.read_parquet(storage.list_keys(settings.bronze_prefix)[0]).head(5)

    broken = good.copy()
    broken["equipment_id"] = [None, "", "EXC-0001", "EXC-0002", "EXC-0003"]
    broken["timestamp"] = [
        "2026-08-18T00:00:00Z",
        "2026-08-18T00:00:00Z",
        "not-a-timestamp",
        "",
        "2026-08-18T25:99:99Z",  # syntactically shaped, semantically impossible
    ]
    broken["_ingest_batch_id"] = "deadbeefdeadbeefdeadbeefdeadbeef"
    key = f"{settings.bronze_prefix}/dt={day}/batch_9999_deadbeef.parquet"
    storage.write_parquet(broken, key, schema=BRONZE_SCHEMA)
    return {**seeded_lake, "broken_rows": len(broken), "day": day}


def test_unrepairable_rows_are_quarantined_not_silently_dropped(
    lake_with_unrepairable_rows,
):
    from pipeline.flows.silver_flow import silver_flow

    ctx = lake_with_unrepairable_rows
    silver_flow(full_refresh=True)

    storage, settings = ctx["storage"], ctx["settings"]
    keys = [k for k in storage.list_keys(settings.silver_prefix) if "_quarantine" in k]
    assert keys, "unrepairable rows vanished instead of being quarantined"

    rejects = storage.read_parquet_many(keys)
    pa.Table.from_pandas(rejects, schema=QUARANTINE_SCHEMA, preserve_index=False)
    assert len(rejects) == ctx["broken_rows"]

    reasons = set(rejects["reject_reason"])
    assert "missing_equipment_id" in reasons
    assert "unparseable_timestamp" in reasons
    # Every reject carries a stated reason — a bare rejected row is unauditable.
    assert rejects["reject_reason"].notna().all()
    assert (rejects["reject_reason"].str.len() > 0).all()


def test_quarantined_rows_never_reach_silver(lake_with_unrepairable_rows):
    from pipeline.flows.silver_flow import silver_flow

    ctx = lake_with_unrepairable_rows
    silver_flow(full_refresh=True)
    silver = read_silver(ctx)

    assert silver["equipment_id"].notna().all()
    assert (silver["equipment_id"].str.len() > 0).all()
    assert silver["event_time"].notna().all()
    # The three broken rows that DID carry a usable equipment_id must not appear
    # with a bogus event_time smuggled in.
    end = WINDOW_START + timedelta(hours=WINDOW_HOURS)
    assert silver["event_time"].between(WINDOW_START, end).all()
