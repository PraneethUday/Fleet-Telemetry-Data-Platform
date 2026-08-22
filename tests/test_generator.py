"""Tests for the bronze telemetry generator.

Two things are being pinned here, and they pull in opposite directions:

  * the data must be *realistic* — continuous hour meters, correlated sensors,
    fault codes that cluster before a failure — otherwise the gold layer has
    nothing genuine to detect and the analytics are theatre;
  * the data must be *dirty* — duplicates, bad ranges, four timestamp encodings —
    otherwise the silver layer is a no-op and the layered architecture is
    decoration.

If either property regresses, the downstream layers stop proving anything, so
both get asserted directly.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from pipeline.generator.profiles import (
    EQUIPMENT_PROFILES,
    FAULT_CODES_BY_SYMPTOM,
    TYPE_VARIANTS,
)
from pipeline.generator.simulator import MODES, Batch, FleetSimulator, GeneratorConfig
from pipeline.schemas import BRONZE_SCHEMA, BRONZE_TELEMETRY_COLUMNS, EQUIPMENT_TYPES


def parse_any(series: pd.Series) -> pd.Series:
    """Parse all four raw encodings the generator emits, to UTC.

    Deliberately reimplemented here rather than imported from the silver layer:
    these tests assert what the *generator* produces, and must not pass merely
    because the cleaner and the generator share a bug.

    The three passes are not fussiness. Handing pandas a mixed array of
    offset-bearing and naive strings at once makes it infer one timezone for the
    whole array and apply it to the naive members, so "2026-08-20 00:05:00"
    silently becomes 2026-08-19T14:05Z next to a "+10:00" neighbour. Naive
    strings have to be parsed in isolation, where "no offset" means UTC.
    """
    s = series.astype(str)
    epoch = s.str.fullmatch(r"\d{10,13}")
    aware = s.str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", regex=True) & ~epoch
    naive = ~epoch & ~aware

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")
    if epoch.any():
        out[epoch] = pd.to_datetime(s[epoch].astype("int64"), unit="ms", utc=True)
    if aware.any():
        out[aware] = pd.to_datetime(s[aware], format="ISO8601", utc=True)
    if naive.any():
        out[naive] = pd.to_datetime(s[naive], format="ISO8601").dt.tz_localize("UTC")
    return out


def all_rows(batches: list[Batch]) -> pd.DataFrame:
    return pd.concat([b.frame for b in batches], ignore_index=True)


# ---------------------------------------------------------------------------
# Fleet composition
# ---------------------------------------------------------------------------
def test_fleet_has_requested_size_and_all_four_types(small_config):
    sim = FleetSimulator(small_config)
    assert sim.n == small_config.fleet_size
    assert set(sim.equipment_type) == set(EQUIPMENT_TYPES)


def test_default_fleet_is_exactly_500_machines():
    sim = FleetSimulator(GeneratorConfig())
    assert sim.n == 500
    assert len(set(sim.equipment_id)) == 500  # ids are unique


def test_type_mix_follows_the_declared_shares():
    sim = FleetSimulator(GeneratorConfig(fleet_size=500))
    counts = pd.Series(sim.equipment_type).value_counts()
    for profile in EQUIPMENT_PROFILES:
        expected = profile.fleet_share * 500
        assert abs(counts[profile.equipment_type] - expected) <= 2


def test_generator_engines_are_stationary(small_config, window):
    """A genset sits on a pad. If its GPS wanders, the site model is broken."""
    start, end = window
    sim = FleetSimulator(small_config)
    df = all_rows(list(sim.simulate(start, end)))
    gens = df[df["equipment_type"] == "generator_engine"].dropna(subset=["gps_lat"])
    gens = gens[gens["gps_lat"] != 0.0]  # exclude injected null-island rows
    spread = gens.groupby("equipment_id")["gps_lat"].agg(lambda s: s.max() - s.min())
    assert (spread < 1e-9).all()


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
def test_same_seed_reproduces_byte_identical_batches(small_config, window):
    """Reproducibility is what makes every downstream assertion in this repo
    meaningful — a test can only pin gold's output if bronze is deterministic."""
    start, end = window
    a = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    b = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    pd.testing.assert_frame_equal(a, b)


def test_different_seed_produces_different_data(small_config, window):
    start, end = window
    other = GeneratorConfig(**{**small_config.__dict__, "seed": small_config.seed + 1})
    a = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    b = all_rows(list(FleetSimulator(other).simulate(start, end)))
    assert not a["coolant_temp_c"].equals(b["coolant_temp_c"])


# ---------------------------------------------------------------------------
# Bronze contract
# ---------------------------------------------------------------------------
def test_frames_conform_to_the_declared_bronze_schema(batches):
    """Every batch must be writable under BRONZE_SCHEMA, or a later batch with a
    different inferred type would poison the whole prefix for DuckDB."""
    for batch in batches:
        pa.Table.from_pandas(batch.frame, schema=BRONZE_SCHEMA, preserve_index=False)


def test_columns_are_exactly_the_contract(batches):
    expected = BRONZE_TELEMETRY_COLUMNS + ["_ingest_batch_id", "_ingested_at_utc"]
    assert list(batches[0].frame.columns) == expected


def test_equipment_id_is_never_null(batches):
    assert all_rows(batches)["equipment_id"].notna().all()


def test_batch_filename_encodes_the_window_start(batches):
    for batch in batches:
        assert batch.filename.startswith(f"batch_{batch.batch_start.strftime('%H%M')}_")
        assert batch.filename.endswith(".parquet")


def test_partition_date_matches_the_readings_own_event_date(batches):
    """Rows are partitioned by when the reading happened, not when it was written.

    Getting this backwards is the classic partitioning bug: a batch that arrives
    late lands in today's folder and quietly disappears from yesterday's query.
    """
    for batch in batches:
        dates = parse_any(batch.frame["timestamp"]).dt.strftime("%Y-%m-%d")
        assert set(dates) == {batch.partition_date}


def test_readings_land_inside_the_requested_window(small_config, window):
    start, end = window
    df = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    ts = parse_any(df["timestamp"])
    assert ts.min() >= start
    assert ts.max() < end


def test_naive_and_offset_timestamps_are_the_same_instant(small_config, window):
    """The +08:00 rows must be genuinely offset, not the UTC string relabelled.

    If the generator emitted local wall-clock digits with a UTC marker, silver
    would 'normalise' them into an eight-hour error and nothing would notice.
    """
    start, end = window
    df = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    offset_rows = df[df["timestamp"].str.contains(r"[+-]\d{2}:\d{2}$", regex=True)]
    assert len(offset_rows) > 0
    parsed = parse_any(offset_rows["timestamp"])
    # Every reading is emitted on a 5-minute grid; a mis-signed offset would
    # still land on the grid, so also check the instants stay inside the window.
    assert (parsed.dt.minute % 5 == 0).all()
    assert parsed.min() >= start and parsed.max() < end


def test_rejects_naive_datetimes(small_config):
    sim = FleetSimulator(small_config)
    with pytest.raises(ValueError, match="timezone-aware"):
        list(sim.simulate(datetime(2026, 8, 20), datetime(2026, 8, 20, 1)))


# ---------------------------------------------------------------------------
# Physical plausibility — what makes the analytics layer worth building
# ---------------------------------------------------------------------------
def test_engine_hours_never_run_backwards(small_config, window):
    """The hour meter is monotonic on real equipment; a decrease would let the
    gold layer compute negative operating hours."""
    start, end = window
    df = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    df = df.dropna(subset=["engine_hours"]).copy()
    df["ts"] = parse_any(df["timestamp"])
    for _, machine in df.sort_values("ts").groupby("equipment_id"):
        assert machine["engine_hours"].is_monotonic_increasing


def test_engine_hours_accrue_no_faster_than_wall_clock(small_config, window):
    start, end = window
    df = all_rows(list(FleetSimulator(small_config).simulate(start, end)))
    span_h = (end - start).total_seconds() / 3600
    grown = df.groupby("equipment_id")["engine_hours"].agg(lambda s: s.max() - s.min())
    assert (grown <= span_h + 1e-6).all()


def test_oil_pressure_is_bimodal_engine_on_or_engine_off(batches):
    """Oil pressure comes from the engine turning, so the distribution has two
    modes — exactly zero when parked, 20-60 psi under load — with a genuine gap
    between them. That gap is what makes silver's UNDER_LOAD_OIL_PSI threshold a
    real discriminator rather than an arbitrary cut through a continuum."""
    oil = all_rows(batches)["oil_pressure_psi"].dropna()
    oil = oil[oil >= 0]  # exclude injected negatives
    assert (oil == 0.0).mean() > 0.05, "no parked machines in the feed"
    assert (oil > 20.0).mean() > 0.30, "no working machines in the feed"
    assert ((oil > 0.0) & (oil < 10.0)).mean() < 0.02, "the on/off gap is smeared"


def test_hot_engines_report_real_oil_pressure(batches):
    """The two sensors must agree about whether the engine is running. If they
    disagreed, every downstream 'under load' statistic would be noise."""
    df = all_rows(batches)
    hot = df[(df["coolant_temp_c"] > 80) & (df["coolant_temp_c"] < 140)]
    hot = hot.dropna(subset=["oil_pressure_psi"])
    assert len(hot) > 100
    # A handful of rows are engines that stopped within the last tick and have
    # not cooled yet, so this is a strong majority rather than an absolute.
    assert (hot["oil_pressure_psi"] > 10.0).mean() > 0.95


def test_sensor_values_are_mostly_physically_plausible(batches):
    """Dirt is injected on purpose, but only at a few tenths of a percent."""
    df = all_rows(batches)
    fuel = df["fuel_level_pct"].dropna()
    temp = df["coolant_temp_c"].dropna()
    assert ((fuel >= 0) & (fuel <= 100)).mean() > 0.99
    assert ((temp > -50) & (temp < 200)).mean() > 0.99


# ---------------------------------------------------------------------------
# Injected data-quality issues — silver's reason to exist
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def dirty_frame():
    cfg = GeneratorConfig(fleet_size=120, seed=11, interval_seconds=300, batch_minutes=60)
    start = datetime(2026, 8, 20, tzinfo=timezone.utc)
    return all_rows(list(FleetSimulator(cfg).simulate(start, start + timedelta(hours=24))))


def test_all_four_timestamp_encodings_appear(dirty_frame):
    ts = dirty_frame["timestamp"].astype(str)
    assert ts.str.endswith("Z").any()
    assert ts.str.contains(r"[+-]\d{2}:\d{2}$", regex=True).any()
    assert ts.str.contains(r"^\d{4}-\d{2}-\d{2} ", regex=True).any()
    assert ts.str.fullmatch(r"\d{10,13}").any()


def test_equipment_type_arrives_in_non_canonical_spellings(dirty_frame):
    seen = set(dirty_frame["equipment_type"].unique())
    assert seen - set(EQUIPMENT_TYPES), "no dirty type variants were injected"
    for canonical, variants in TYPE_VARIANTS.items():
        assert seen & set(variants), f"no variants seen for {canonical}"


def test_business_key_duplicates_exist_including_cross_batch_resends(dirty_frame):
    """The cross-batch case is the one that matters: the same reading re-sent
    under a new _ingest_batch_id survives drop_duplicates() on all columns, so
    silver has to dedup on (equipment_id, timestamp) instead."""
    grouped = dirty_frame.groupby(["equipment_id", "timestamp"])
    dupes = grouped.size()
    assert (dupes > 1).sum() > 0
    batches_per_key = grouped["_ingest_batch_id"].nunique()
    assert (batches_per_key > 1).sum() > 0


def test_nulls_and_impossible_values_are_present(dirty_frame):
    assert dirty_frame["engine_hours"].isna().any()
    assert dirty_frame["coolant_temp_c"].isna().any()
    fuel = dirty_frame["fuel_level_pct"]
    assert ((fuel < 0) | (fuel > 100)).any()
    temp = dirty_frame["coolant_temp_c"]
    assert ((temp < -50) | (temp > 200)).any()
    null_island = (dirty_frame["gps_lat"] == 0.0) & (dirty_frame["gps_lon"] == 0.0)
    assert null_island.any()


def test_dirt_stays_a_minority_of_the_feed(dirty_frame):
    """Bronze is dirty, not garbage — a feed that is 30% broken would be a
    different problem (a broken gateway) than the one this models."""
    canonical = dirty_frame["equipment_type"].isin(EQUIPMENT_TYPES).mean()
    assert 0.90 < canonical < 0.99


def test_fault_codes_are_rare_overall(dirty_frame):
    rate = dirty_frame["fault_code"].notna().mean()
    assert 0.0 < rate < 0.05, f"fault rate {rate:.3%} is not 'mostly null'"


# ---------------------------------------------------------------------------
# The failure signal the gold layer is supposed to find
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def degrading_run():
    cfg = GeneratorConfig(fleet_size=200, seed=3, interval_seconds=300, batch_minutes=180)
    start = datetime(2026, 8, 18, tzinfo=timezone.utc)
    sim = FleetSimulator(cfg)
    df = all_rows(list(sim.simulate(start, start + timedelta(days=3))))
    return sim, df


def test_a_realistic_minority_of_the_fleet_is_degrading(degrading_run):
    sim, _ = degrading_run
    roster = sim.roster()
    share = roster["planned_failure_mode"].notna().mean()
    assert 0.02 < share < 0.20, f"{share:.1%} degrading is not a realistic fleet"


def test_degrading_machines_report_far_more_faults(degrading_run):
    """Without this the gold rules would be detecting noise."""
    sim, df = degrading_run
    roster = sim.roster().set_index("equipment_id")
    faults = df.groupby("equipment_id")["fault_code"].count()
    # `planned_failure_mode` records the original trajectory. Using the live
    # `degrades` flag instead would file every machine that already failed and
    # was repaired under "healthy", dragging the healthy baseline up with the
    # very fault storms this test is trying to detect.
    planned = roster["planned_failure_mode"].notna().reindex(faults.index).fillna(False)
    sick, well = faults[planned], faults[~planned]
    assert len(sick) > 3 and len(well) > 50
    assert sick.mean() > 5 * max(well.mean(), 0.1), (
        f"sick={sick.mean():.1f} well={well.mean():.1f} faults/machine"
    )


def test_fault_codes_match_the_machines_actual_failure_mode(degrading_run):
    """A machine that is overheating must not report a fuel-delivery code.

    This correlation is what lets the rule-based gold layer agree with itself:
    the sensor trend and the DTC point at the same problem.
    """
    sim, df = degrading_run
    modes = dict(zip(sim.equipment_id, [MODES[i] for i in sim.mode_idx]))
    degrading = set(sim.roster().query("planned_failure_mode.notna()")["equipment_id"])

    matched = mismatched = 0
    for eq_id, group in df[df["fault_code"].notna()].groupby("equipment_id"):
        if eq_id not in degrading:
            continue
        own_pool = set(FAULT_CODES_BY_SYMPTOM[modes[eq_id]])
        matched += group["fault_code"].isin(own_pool).sum()
        mismatched += (~group["fault_code"].isin(own_pool)).sum()
    assert matched > 0
    assert matched / (matched + mismatched) > 0.75


def test_overheating_machines_actually_run_hot(degrading_run):
    sim, df = degrading_run
    overheating = {
        eq
        for eq, mi, deg in zip(sim.equipment_id, sim.mode_idx, sim.degrades)
        if deg and MODES[mi] == "overheat"
    }
    if not overheating:
        pytest.skip("no overheat-mode machines in this seed")
    peaks = df[df["coolant_temp_c"] < 200].groupby("equipment_id")["coolant_temp_c"].max()
    assert peaks[peaks.index.isin(overheating)].max() > 105


def test_some_machines_are_still_mid_failure_when_the_data_ends(degrading_run):
    """These are the machines `fleet_health_flags` must surface: trending toward
    a failure that has not happened yet. If every planned failure completed
    inside the window, the gold layer would only ever report history."""
    sim, _ = degrading_run
    end_epoch = datetime(2026, 8, 21, tzinfo=timezone.utc).timestamp()
    severity = sim._severity(end_epoch)
    assert ((severity > 0.15) & (severity < 1.0)).sum() > 0


# ---------------------------------------------------------------------------
# Checkpointing — what lets an hourly container job resume
# ---------------------------------------------------------------------------
def test_state_roundtrip_preserves_the_fleets_physical_state(small_config, window):
    start, end = window
    first = FleetSimulator(small_config)
    list(first.simulate(start, end))
    state = first.to_state(end)

    resumed = FleetSimulator(small_config)
    resumed.load_state(state)
    np.testing.assert_allclose(resumed.engine_hours, first.engine_hours)
    np.testing.assert_allclose(resumed.fuel_pct, first.fuel_pct)
    np.testing.assert_allclose(resumed.coolant, first.coolant)


def test_resumed_run_continues_the_hour_meter(small_config, window):
    """Two consecutive one-hour jobs must look like one two-hour job's tail, not
    like the fleet resetting to factory-fresh every time the container starts."""
    start, end = window
    first = FleetSimulator(small_config)
    list(first.simulate(start, end))

    resumed = FleetSimulator(small_config)
    resumed.load_state(first.to_state(end))
    df = all_rows(list(resumed.simulate(end, end + timedelta(hours=1))))
    lowest = df.dropna(subset=["engine_hours"]).groupby("equipment_id")["engine_hours"].min()
    baseline = pd.Series(first.engine_hours, index=first.equipment_id)
    # emitted engine_hours are rounded to 2 dp, so allow half a rounding step
    assert (lowest >= baseline.reindex(lowest.index) - 0.005).all()


def test_state_from_a_different_fleet_is_rejected(small_config, window):
    """Silently applying a mismatched checkpoint would corrupt the hour meters."""
    start, end = window
    sim = FleetSimulator(small_config)
    state = sim.to_state(end)
    other = FleetSimulator(GeneratorConfig(fleet_size=small_config.fleet_size + 8, seed=7))
    with pytest.raises(ValueError, match="does not match"):
        other.load_state(state)
