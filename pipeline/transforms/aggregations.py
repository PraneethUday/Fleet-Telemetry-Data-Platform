"""Silver -> gold aggregation rules, as pure DataFrame functions.

Why this module exists separately from `flows/gold_flow.py`
----------------------------------------------------------
Same split as `transforms.cleaning`: the judgement lives here, the plumbing
lives in the flow. Every function below is `DataFrame in -> DataFrame out` with
no storage, no Prefect and no clock of its own, so a rule can be argued about
and regression-tested on a ten-row fixture instead of on 430,000 rows and an
Azure account. That matters more in gold than in silver, because gold is where
the numbers a maintenance planner acts on are actually decided.

The three things this module gets right that a naive GROUP BY gets wrong
-----------------------------------------------------------------------
**Parked machines are excluded from sensor statistics.** A machine sitting idle
reports 0 psi and ambient temperature. Those are true readings of a stationary
engine and complete nonsense as "how hot does this machine run" — averaging them
in drags every temperature down toward ambient and every oil pressure toward
zero, which makes a healthy fleet look like it is failing and a genuinely sick
machine look average. Silver already materialised `is_under_load`; every
temperature and oil-pressure statistic here filters on it.

**Fuel consumption is the sum of the drops, not max minus min.** Any day a
machine is refuelled, `max - min` is not merely inaccurate, it has the wrong
sign. Diffing the sorted series and summing only the negative steps is the only
version that survives contact with a fuel truck.

**Health flags are anchored to the data, not to the wall clock.** The lake holds
simulated history; `datetime.now()` would put every reading hours or days in the
past and flag the entire fleet as stale. The trailing window ends at the newest
`event_time` actually present.

Rule thresholds are module-level constants, not literals buried in an
expression, because they are the part of this file a maintenance planner will
want to argue with. Every one of them is a judgement call and is justified on
the line above it.
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.schemas import (
    GOLD_DAILY_SUMMARY_SCHEMA,
    GOLD_FLEET_SUMMARY_SCHEMA,
    GOLD_HEALTH_FLAGS_SCHEMA,
)

# ---------------------------------------------------------------------------
# Tunables shared by both gold tables
# ---------------------------------------------------------------------------

# Percentile used wherever "how hot does it get" must ignore single-sample
# spikes. 95th, linear interpolation between the two bracketing order
# statistics (numpy's default `method="linear"`, pandas' `interpolation="linear"`).
# Naming the interpolation matters: DuckDB's quantile_cont agrees with it and
# quantile_disc does not, so a dashboard re-deriving this number gets the same
# answer only if both sides use continuous interpolation.
TEMP_PERCENTILE = 0.95

# 5th percentile, not min, for oil pressure — see OIL_* constants below.
OIL_PRESSURE_PERCENTILE = 0.05

# A positive tank-level step larger than this is a fuel truck, not sensor noise.
# Float senders slosh by a couple of points on a rough haul road; nothing short
# of a refuel moves a tank up 20 points between two readings five minutes apart.
REFUEL_DELTA_PCT = 20.0

# Below this many operating hours a burn *rate* is meaningless: a machine that
# ran for 30 seconds and used 0.4% of its tank did not burn 48 %/hour. Report
# null rather than a number that is arithmetically correct and physically absurd.
MIN_OPERATING_HOURS_FOR_RATE = 0.05


# ---------------------------------------------------------------------------
# Health-flag thresholds
#
# Every threshold below is a maintenance decision expressed as a number, so each
# one carries the reason it is that number. They are module-level so a planner
# can be shown the table and argue with it, and so retuning is a one-line diff
# with a visible blame trail rather than an edit inside a boolean expression.
# ---------------------------------------------------------------------------

# Coolant creeping up ~0.35 C/hour is ~8 C/day: a radiator fouling or a water
# pump on the way out, still well inside the normal operating band today and
# out of it within the week. That is the lead time a planner can actually use.
TEMP_TREND_C_PER_HOUR = 0.35

# Diesel engines run 85-95 C. 105 C is above every manufacturer's normal band
# but below the point where the ECU derates.
TEMP_HIGH_C = 105.0

# 115 C is where coolant boils in a pressurised system and head-gasket damage
# starts. This is a "stop the machine" number, not a "book it in" number.
TEMP_CRITICAL_C = 115.0

# Nominal hot-idle oil pressure across this fleet is 40-50 psi. 30 is low enough
# to mean bearing wear or a tired pump rather than a hot day.
OIL_PRESSURE_LOW_PSI = 30.0

# Below 20 psi at operating temperature an engine is running on the edge of
# hydrodynamic lubrication. Minutes of this cost a crankshaft.
OIL_PRESSURE_CRITICAL_PSI = 20.0

# Three DTCs in a day is a pattern, not a glitch: one loose connector reports
# once, a failing sensor reports repeatedly.
FAULT_REPEAT_COUNT = 3

# Twenty-five or more in a day is an ECU shouting — roughly one DTC per operating
# hour, usually one root cause cascading into every subsystem that depends on it.
#
# Measured rather than assumed. Across this fleet the 24-hour fault distribution
# is starkly bimodal: 415 of 500 machines report zero, 63 report one or two, and
# the p90 is 1. Only 2 machines land in the 3-7 "pattern" band, then 5 in 8-24
# and 15 at 25 or more. An earlier threshold of 8 put 20 of 23 flagged machines
# into "critical", which is not triage — a machine with 8 faults and one with 123
# arrived on the planner's screen wearing the same colour. Moving the line to 25
# splits the same list 15 critical / 8 warning, and the warning band is now
# machines you book in this week rather than machines you walk out to now.
FAULT_STORM_COUNT = 25

# The same code three times is the machine naming its own fault, which is far
# more actionable than three unrelated codes.
RECURRING_FAULT_CODE_COUNT = 3

# Telemetry cadence is one reading per five minutes. Three hours of silence is
# ~36 missed readings: a machine in a pit with no signal, a dead gateway, or a
# machine that stopped so hard it took the telematics box with it. All three are
# worth a phone call, and none of them are visible in the sensor rules — a
# machine that stops reporting has no bad readings at all.
STALE_TELEMETRY_HOURS = 3.0

# Minimum readings before a trend or a percentile is trusted. At a five-minute
# cadence this is one hour of work.
MIN_UNDER_LOAD_READINGS = 12

# Readings to discard at the start of every duty cycle, because the engine is
# still warming up and its coolant temperature has not reached steady state.
#
# `is_under_load` flips true the instant oil pressure rises, but coolant lags it
# badly: the block has a thermal time constant near 15 minutes, so a cold engine
# needs roughly three of those — 45 minutes, nine readings — to settle within a
# few degrees of its operating band. Every reading before that is a machine
# climbing from ambient, not a machine running hot or cold.
#
# Measured on this dataset rather than assumed. Include the warm-up rows and the
# mean "under load" temperature is 74 C against a nominal band of 85-95 C, and a
# least-squares line fitted through a machine that cold-started inside the window
# reports 2-4 C/hour of warm-up as if it were degradation. That single effect was
# responsible for 56 of 102 flagged machines, at 3.6% precision against the
# generator's planted failures — worse than the 8.6% base rate, i.e. actively
# misleading. Discarding the first nine readings of each cycle lifts the same
# rule to 100% precision. See docs/WAREHOUSE.md.
WARMUP_READINGS = 9


# Rule names. Ordered, because `flag_reasons` is a string that ends up in
# equality assertions and GROUP BYs — "temp_high,repeated_faults" must never
# come back reversed because a dict iterated differently.
RULE_TEMP_TRENDING_UP = "temp_trending_up"
RULE_TEMP_HIGH = "temp_high"
RULE_TEMP_CRITICAL = "temp_critical"
RULE_OIL_PRESSURE_LOW = "oil_pressure_low"
RULE_OIL_PRESSURE_CRITICAL = "oil_pressure_critical"
RULE_REPEATED_FAULTS = "repeated_faults"
RULE_FAULT_STORM = "fault_storm"
RULE_RECURRING_FAULT_CODE = "recurring_fault_code"
RULE_STALE_TELEMETRY = "stale_telemetry"

RULE_ORDER: tuple[str, ...] = (
    RULE_TEMP_TRENDING_UP,
    RULE_TEMP_HIGH,
    RULE_TEMP_CRITICAL,
    RULE_OIL_PRESSURE_LOW,
    RULE_OIL_PRESSURE_CRITICAL,
    RULE_REPEATED_FAULTS,
    RULE_FAULT_STORM,
    RULE_RECURRING_FAULT_CODE,
    RULE_STALE_TELEMETRY,
)

# Rules that mean "take it out of service now" rather than "book it in".
CRITICAL_RULES: frozenset[str] = frozenset(
    {RULE_TEMP_CRITICAL, RULE_OIL_PRESSURE_CRITICAL, RULE_FAULT_STORM}
)

# health_score = 100 - the penalties of every rule that fired, clipped to [0,100].
#
# The scale is deliberately blunt: a single critical rule (40) plus its warning
# precursor (e.g. temp_critical always fires alongside temp_high) lands around
# 40-45, and two independent critical faults floor the machine near zero. The
# score exists to *rank* the flagged list for a planner working top-down, not to
# be a calibrated probability — nothing here is fitted to failure data, so a
# score of 62 does not mean 38% chance of failure and must not be presented as
# though it did. Penalties live in this dict so they can be retuned without
# touching the rule logic, and so the whole scoring model is visible at once.
HEALTH_PENALTIES: dict[str, float] = {
    RULE_TEMP_TRENDING_UP: 15.0,
    RULE_TEMP_HIGH: 20.0,
    RULE_TEMP_CRITICAL: 40.0,
    RULE_OIL_PRESSURE_LOW: 20.0,
    RULE_OIL_PRESSURE_CRITICAL: 40.0,
    RULE_REPEATED_FAULTS: 10.0,
    RULE_FAULT_STORM: 30.0,
    RULE_RECURRING_FAULT_CODE: 15.0,
    RULE_STALE_TELEMETRY: 10.0,
}

SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"

DEFAULT_WINDOW_HOURS = 24

# "Active" in the fleet rollup is always the last 24 hours of data, regardless of
# the health window, because it answers a different question ("what is working
# today") and a planner comparing two runs needs it to mean the same thing even
# if the flag window was widened.
ACTIVE_WINDOW_HOURS = 24


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------
def _empty(schema) -> pd.DataFrame:
    """A correctly-typed zero-row frame, so an empty run still writes a table."""
    return schema.empty_table().to_pandas()


def conform(df: pd.DataFrame, schema) -> pd.DataFrame:
    """Project to exactly the schema's columns, in order.

    The gold tables are a published contract read by DuckDB and the API. Ordering
    and column-set enforcement happen here rather than being left to
    `pa.Table.from_pandas`, so a mistake surfaces as a clear KeyError in the
    transform instead of an opaque Arrow cast error at write time.
    """
    missing = [name for name in schema.names if name not in df.columns]
    if missing:
        raise KeyError(f"aggregation is missing gold column(s): {missing}")
    return df.loc[:, list(schema.names)].reset_index(drop=True)


def _least_squares_slope(
    frame: pd.DataFrame, group: str, x: str, y: str, min_points: int
) -> pd.Series:
    """Per-group OLS slope dy/dx, computed from group sums.

    Closed form rather than a groupby-apply over `numpy.polyfit`: at 500 groups
    the difference is ~40x, and more importantly the sums version is a single
    vectorised pass that behaves identically on 500 machines or 50,000.

        slope = (n*Sxy - Sx*Sy) / (n*Sxx - Sx^2)

    `x` must be hours *relative to the window start*, not epoch hours. Epoch
    hours are ~4.9e5, so Sxx is ~1e14 and the numerator becomes a difference of
    two nearly-equal large numbers — float64 loses most of its significant digits
    to cancellation and the slope comes back visibly wrong. Recentring costs one
    subtraction and removes the problem entirely.

    Groups with fewer than `min_points` points, or with all readings at the same
    instant (zero x-variance, a vertical fit), get NaN — an unknown trend, which
    is honestly different from a flat one.
    """
    valid = frame[[group, x, y]].dropna()
    if valid.empty:
        return pd.Series(dtype="float64")

    grouped = valid.groupby(group, sort=True)
    n = grouped.size()
    sx = grouped[x].sum()
    sy = grouped[y].sum()
    sxy = (valid[x] * valid[y]).groupby(valid[group]).sum()
    sxx = (valid[x] ** 2).groupby(valid[group]).sum()

    denominator = n * sxx - sx**2
    slope = (n * sxy - sx * sy) / denominator.replace(0.0, np.nan)
    return slope.where(n >= min_points)


def thermally_settled(df: pd.DataFrame) -> pd.Series:
    """Boolean mask: engine under load AND coolant at steady state.

    Temperature statistics are computed over these rows rather than over every
    under-load row. Oil pressure deliberately is not — oil pressure comes up with
    the first turn of the pump and has no warm-up transient to exclude, so
    filtering it here would only throw away good samples.

    Vectorised rather than a per-machine loop: a duty cycle boundary is any row
    where the engine is off, or where the machine changes, so a cumulative sum
    over those boundaries labels every contiguous running stretch, and a grouped
    cumulative sum ranks the readings inside it.
    """
    ordered = df.sort_values(["equipment_id", "event_time"], kind="stable")
    running = ordered["is_under_load"].to_numpy(dtype=bool)
    machine = ordered["equipment_id"].to_numpy()

    boundary = ~running
    if len(boundary) > 1:
        boundary[1:] |= machine[1:] != machine[:-1]
    cycle = np.cumsum(boundary)

    # 1-based rank of this reading among the running readings of its cycle.
    rank = pd.Series(running.astype(np.int64)).groupby(cycle).cumsum().to_numpy()
    settled = pd.Series(running & (rank > WARMUP_READINGS), index=ordered.index)
    return settled.reindex(df.index).fillna(False)


def _row_counts(frame: pd.DataFrame, group: str, index: pd.Index) -> pd.Series:
    """Rows per group, reindexed onto `index`. A group with no rows counts 0, not
    NaN — "this machine reported nothing in the window" is a number, not a gap.
    """
    return frame.groupby(group, sort=True).size().reindex(index).fillna(0).astype("int64")


def _top_value_by_frequency(
    frame: pd.DataFrame, group: str, value: str
) -> tuple[pd.Series, pd.Series]:
    """Most frequent `value` per group, and its count. Ties break on the lowest
    value so two identical runs cannot disagree about which code to show.
    """
    if frame.empty:
        return pd.Series(dtype="object"), pd.Series(dtype="int64")
    counts = frame.groupby([group, value], sort=False).size().rename("n").reset_index()
    counts = counts.sort_values([group, "n", value], ascending=[True, False, True])
    top = counts.groupby(group, sort=True).first()
    return top[value], top["n"]


# ---------------------------------------------------------------------------
# Table 1: daily_equipment_summary   (grain: equipment_id x day)
# ---------------------------------------------------------------------------
def daily_equipment_summary(silver: pd.DataFrame) -> pd.DataFrame:
    """One row per machine per UTC day, from cleaned silver readings."""
    if silver is None or len(silver) == 0:
        return _empty(GOLD_DAILY_SUMMARY_SCHEMA)

    keys = ["equipment_id", "event_date"]
    # Sorted once, up front: the fuel diff and the last-known GPS fix both depend
    # on chronological order within a machine-day, and doing it here means
    # neither has to re-sort (or, worse, silently assume the caller did).
    df = silver.sort_values(["equipment_id", "event_date", "event_time"], kind="stable")

    grouped = df.groupby(keys, sort=True)
    summary = grouped.agg(
        equipment_type=("equipment_type", "first"),
        readings_count=("equipment_id", "size"),
        under_load_readings=("is_under_load", "sum"),
        # A monotonic hour meter: the day's first and last readings bracket the
        # work done. min/max rather than first/last so a single out-of-order or
        # rolled-back reading cannot invert the pair.
        engine_hours_start=("engine_hours", "min"),
        engine_hours_end=("engine_hours", "max"),
        fault_code_count=("has_fault", "sum"),
    )
    summary["distinct_fault_codes"] = grouped["fault_code"].nunique()

    # --- sensor statistics: UNDER LOAD ONLY ---------------------------------
    # The single most important correctness detail in this table. A parked
    # machine reads 0 psi and ambient air temperature. Include those rows and
    # every machine's average oil pressure collapses toward zero and its average
    # coolant temperature toward the desert overnight low — so the fleet appears
    # to be failing, the genuinely low-pressure machines stop standing out, and
    # the whole table becomes a measure of how much each machine was parked
    # rather than how it ran.
    under_load = df[df["is_under_load"]]
    # Temperature additionally excludes warm-up (see WARMUP_READINGS): an engine
    # climbing from ambient is not information about how hot it runs, and a
    # machine doing many short jobs would otherwise report a low average purely
    # because it spent its day warming up.
    settled = df[thermally_settled(df)]
    if len(settled):
        temps = settled.groupby(keys, sort=True)["coolant_temp_c"]
        summary["avg_coolant_temp_c"] = temps.mean()
        summary["max_coolant_temp_c"] = temps.max()
        summary["p95_coolant_temp_c"] = temps.quantile(
            TEMP_PERCENTILE, interpolation="linear"
        )
    else:
        for column in ("avg_coolant_temp_c", "max_coolant_temp_c", "p95_coolant_temp_c"):
            summary[column] = np.nan
    if len(under_load):
        oil = under_load.groupby(keys, sort=True)["oil_pressure_psi"]
        summary["avg_oil_pressure_psi"] = oil.mean()
        summary["min_oil_pressure_psi"] = oil.min()
    else:
        summary["avg_oil_pressure_psi"] = np.nan
        summary["min_oil_pressure_psi"] = np.nan

    # --- engine and fuel ----------------------------------------------------
    # Clipped at zero so one bad hour-meter reading (a gateway reboot reporting
    # 0.0, a CAN glitch) produces "no work recorded" rather than negative hours
    # that would then propagate into a negative fleet total.
    summary["operating_hours"] = (
        summary["engine_hours_end"] - summary["engine_hours_start"]
    ).clip(lower=0.0)

    fuel = _fuel_movement(df, keys)
    summary = summary.join(fuel)
    summary["refuel_events"] = summary["refuel_events"].fillna(0).astype("int64")

    # Null, not inf, when the machine barely moved: dividing a real consumption
    # by ~0 hours yields a burn rate of thousands of percent per hour, which
    # then poisons any fleet average that touches it.
    hours = summary["operating_hours"]
    summary["fuel_burn_pct_per_hour"] = (summary["fuel_consumed_pct"] / hours).where(
        hours >= MIN_OPERATING_HOURS_FOR_RATE
    )

    # --- last known position ------------------------------------------------
    # Filtered on both axes together because a fix is a pair, not two numbers:
    # silver may have nulled one axis as out-of-range, and "last known latitude"
    # paired with an hour-old longitude is a position the machine never occupied.
    fixes = df[df["gps_lat"].notna() & df["gps_lon"].notna()]
    if len(fixes):
        last_fix = fixes.groupby(keys, sort=True)[["gps_lat", "gps_lon"]].last()
        summary = summary.join(
            last_fix.rename(
                columns={"gps_lat": "last_gps_lat", "gps_lon": "last_gps_lon"}
            )
        )
    if "last_gps_lat" not in summary.columns:
        summary["last_gps_lat"] = np.nan
        summary["last_gps_lon"] = np.nan

    summary = summary.reset_index().rename(columns={"event_date": "day"})
    for column in (
        "readings_count",
        "under_load_readings",
        "fault_code_count",
        "distinct_fault_codes",
    ):
        summary[column] = summary[column].astype("int64")

    summary = summary.sort_values(["day", "equipment_id"], kind="stable")
    return conform(summary, GOLD_DAILY_SUMMARY_SCHEMA)


def _fuel_movement(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Fuel consumed and refuel events per group, from the sorted level series.

    The trap this exists to avoid: `max(fuel) - min(fuel)` is the obvious
    formula and it is wrong. On any day a machine is refuelled the tank ends
    higher than it started, so the naive answer is negative — a machine that
    burned 60% of a tank and then took 80% from the fuel truck reports "-20%
    consumed", and the fleet total quietly gains fuel out of nowhere.

    Diffing the chronological series and summing only the negative steps
    measures what was actually burned, and is unchanged by how many times the
    truck came. The positive steps are counted separately, because "how often
    does this machine need refuelling" is a real logistics question and the
    number is free once the diff exists.

    Rows with a null level are dropped before diffing rather than treated as
    zero: the delta then spans the gap, which is the honest reading of "we did
    not observe the tank for twenty minutes, and over that span it fell 3%".
    """
    known = df.loc[df["fuel_level_pct"].notna(), keys + ["fuel_level_pct"]].copy()
    if known.empty:
        empty_index = df.groupby(keys, sort=True).size().index
        return pd.DataFrame(
            {"fuel_consumed_pct": np.nan, "refuel_events": 0}, index=empty_index
        )

    delta = known.groupby(keys, sort=True)["fuel_level_pct"].diff()
    known["drop_pct"] = (-delta).clip(lower=0.0)
    known["is_refuel"] = (delta > REFUEL_DELTA_PCT).fillna(False)

    grouped = known.groupby(keys, sort=True)
    return pd.DataFrame(
        {
            # min_count=1 keeps the distinction between "burned nothing" (0.0,
            # from at least one observed step) and "we never saw two readings,
            # so consumption is unknown" (NaN). Reporting the second as 0.0
            # would understate the fleet's fuel use with no way to notice.
            "fuel_consumed_pct": grouped["drop_pct"].sum(min_count=1),
            "refuel_events": grouped["is_refuel"].sum().astype("int64"),
        }
    )


# ---------------------------------------------------------------------------
# Table 2: fleet_health_flags   (grain: one row per FLAGGED machine)
# ---------------------------------------------------------------------------
def fleet_health_flags(
    silver: pd.DataFrame,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    generated_at: datetime | None = None,
) -> pd.DataFrame:
    """Rule-based health flags over a trailing window of readings.

    Rule-based and not a model, on purpose. A maintenance planner has to be able
    to act on this at 6am, and "coolant is trending up 0.6 C/hour and it has
    thrown P0128 four times" is a sentence they can take to a fitter. "The model
    scored it 0.83" is not, and nobody can tell whether it is wrong.

    The window ends at the newest `event_time` in the data, never at
    `datetime.now()`. The lake holds simulated history, so a wall-clock anchor
    would put every reading outside the window, produce no sensor statistics at
    all, and flag all 500 machines as stale — a spectacular, entirely artificial
    alert storm. Anchoring to the data also makes reruns reproducible, which a
    wall-clock version can never be.
    """
    generated_at = generated_at or datetime.now(timezone.utc)
    if silver is None or len(silver) == 0:
        return _empty(GOLD_HEALTH_FLAGS_SCHEMA)

    df = silver.sort_values(["equipment_id", "event_time"], kind="stable")
    anchor = df["event_time"].max()
    window_start = anchor - pd.Timedelta(hours=window_hours)

    # --- the machine universe ----------------------------------------------
    # Every machine in the frame the caller handed us, NOT only the machines
    # that reported inside the window. That is what makes `stale_telemetry`
    # possible: a machine that went silent six hours ago has no rows in the
    # window and therefore no bad readings, and a window-only universe would
    # score it as perfectly healthy by virtue of having said nothing. The
    # caller bounds the frame with `lookback_days` so a machine sold last year
    # does not reappear as a stale alert forever.
    per_machine = df.groupby("equipment_id", sort=True).agg(
        equipment_type=("equipment_type", "last"),
        last_seen_utc=("event_time", "max"),
    )
    machines = per_machine.index

    window = df[df["event_time"] >= window_start]
    flags = pd.DataFrame(index=machines)
    flags["equipment_type"] = per_machine["equipment_type"]
    flags["last_seen_utc"] = per_machine["last_seen_utc"]
    flags["readings_in_window"] = _row_counts(window, "equipment_id", machines)
    flags["hours_since_last_reading"] = (
        (anchor - flags["last_seen_utc"]).dt.total_seconds() / 3600.0
    )

    # --- sensor statistics, under load only (see module docstring) ----------
    under_load = window[window["is_under_load"]]
    ul_count = _row_counts(under_load, "equipment_id", machines)

    # Temperature drops the warm-up rows on top of that (see WARMUP_READINGS).
    settled = window[thermally_settled(window)]
    settled_count = _row_counts(settled, "equipment_id", machines)

    if len(settled):
        st_grouped = settled.groupby("equipment_id", sort=True)
        flags["recent_avg_temp_c"] = st_grouped["coolant_temp_c"].mean().reindex(machines)
        flags["recent_max_temp_c"] = st_grouped["coolant_temp_c"].max().reindex(machines)
        # Reported as the peak because that is what a planner wants to see, but
        # the RULES below fire on the 95th percentile instead. A single 120 C
        # sample is a CAN glitch or a momentary load spike; twelve of them is an
        # engine running hot. Keying the rule off max() made 26 machines look
        # critical on one reading each, at 7.7% precision.
        p95_temp = (
            st_grouped["coolant_temp_c"]
            .quantile(TEMP_PERCENTILE, interpolation="linear")
            .reindex(machines)
        )
    else:
        flags["recent_avg_temp_c"] = np.nan
        flags["recent_max_temp_c"] = np.nan
        p95_temp = pd.Series(np.nan, index=machines)

    if len(under_load):
        ul_grouped = under_load.groupby("equipment_id", sort=True)
        # A percentile, not min(). One glitched sample of 4 psi on a CAN bus
        # read is common and means nothing; min() would let that single sample
        # flag a perfectly healthy machine, and the alert list a planner stops
        # trusting is worse than no alert list. The 5th percentile still catches
        # a machine that genuinely spends part of its day starved of pressure —
        # over 24h that is ~14 readings, not one.
        flags["recent_min_oil_psi"] = (
            ul_grouped["oil_pressure_psi"]
            .quantile(OIL_PRESSURE_PERCENTILE, interpolation="linear")
            .reindex(machines)
        )
    else:
        flags["recent_min_oil_psi"] = np.nan

    # Hours since the window opened, not epoch hours — see _least_squares_slope.
    # Fitted over settled rows: a line through a warm-up ramp reports the ramp.
    trend_input = settled.assign(
        _hours=(settled["event_time"] - window_start).dt.total_seconds() / 3600.0
    )
    flags["temp_trend_c_per_hour"] = (
        _least_squares_slope(
            trend_input,
            group="equipment_id",
            x="_hours",
            y="coolant_temp_c",
            min_points=MIN_UNDER_LOAD_READINGS,
        )
        .reindex(machines)
        .where(settled_count >= MIN_UNDER_LOAD_READINGS)
    )

    # --- faults -------------------------------------------------------------
    faults = window[window["has_fault"] & window["fault_code"].notna()]
    flags["recent_fault_count"] = _row_counts(faults, "equipment_id", machines)
    flags["distinct_fault_codes"] = (
        faults.groupby("equipment_id", sort=True)["fault_code"]
        .nunique()
        .reindex(machines)
        .fillna(0)
        .astype("int64")
    )
    top_code, top_code_count = _top_value_by_frequency(faults, "equipment_id", "fault_code")
    flags["top_fault_code"] = top_code.reindex(machines)
    top_count = top_code_count.reindex(machines).fillna(0)

    # --- last known position, over the window -------------------------------
    fixes = window[window["gps_lat"].notna() & window["gps_lon"].notna()]
    if len(fixes):
        last_fix = fixes.groupby("equipment_id", sort=True)[["gps_lat", "gps_lon"]].last()
        flags["last_gps_lat"] = last_fix["gps_lat"].reindex(machines)
        flags["last_gps_lon"] = last_fix["gps_lon"].reindex(machines)
    else:
        flags["last_gps_lat"] = np.nan
        flags["last_gps_lon"] = np.nan

    # --- rules --------------------------------------------------------------
    # Each rule is one boolean column. Comparisons against NaN are False in
    # numpy, which is the behaviour wanted throughout: an unknown statistic must
    # never fire a rule. The two gated rules say so explicitly anyway, because
    # relying on NaN semantics for a safety decision is the kind of cleverness
    # that breaks when someone fills a null.
    enough_under_load = ul_count >= MIN_UNDER_LOAD_READINGS
    # Temperature rules need an hour of *settled* running, which is a stricter
    # bar than an hour of running: a machine doing six ten-minute jobs is under
    # load all hour and never once reaches operating temperature.
    enough_settled = settled_count >= MIN_UNDER_LOAD_READINGS
    fired = pd.DataFrame(index=machines)
    fired[RULE_TEMP_TRENDING_UP] = (
        flags["temp_trend_c_per_hour"].fillna(-np.inf) >= TEMP_TREND_C_PER_HOUR
    ) & enough_settled
    fired[RULE_TEMP_HIGH] = (p95_temp.fillna(-np.inf) >= TEMP_HIGH_C) & enough_settled
    fired[RULE_TEMP_CRITICAL] = (
        p95_temp.fillna(-np.inf) >= TEMP_CRITICAL_C
    ) & enough_settled
    fired[RULE_OIL_PRESSURE_LOW] = (
        flags["recent_min_oil_psi"].fillna(np.inf) <= OIL_PRESSURE_LOW_PSI
    ) & enough_under_load
    fired[RULE_OIL_PRESSURE_CRITICAL] = (
        flags["recent_min_oil_psi"].fillna(np.inf) <= OIL_PRESSURE_CRITICAL_PSI
    ) & enough_under_load
    fired[RULE_REPEATED_FAULTS] = flags["recent_fault_count"] >= FAULT_REPEAT_COUNT
    fired[RULE_FAULT_STORM] = flags["recent_fault_count"] >= FAULT_STORM_COUNT
    fired[RULE_RECURRING_FAULT_CODE] = top_count >= RECURRING_FAULT_CODE_COUNT
    fired[RULE_STALE_TELEMETRY] = (
        flags["hours_since_last_reading"].fillna(np.inf) >= STALE_TELEMETRY_HOURS
    )

    flags["flag_reasons"] = _join_reasons(fired)
    flags["severity"] = np.where(
        fired[list(CRITICAL_RULES)].any(axis=1), SEVERITY_CRITICAL, SEVERITY_WARNING
    )
    penalties = sum(
        fired[rule].astype(float) * HEALTH_PENALTIES[rule] for rule in RULE_ORDER
    )
    flags["health_score"] = (100.0 - penalties).clip(lower=0.0, upper=100.0)

    # Only flagged machines get a row. A table of 500 rows where 460 say
    # "nothing wrong" is a table nobody opens; a table of 40 is a work list.
    flags = flags[fired.any(axis=1)].copy()
    flags["window_hours"] = np.int64(window_hours)
    flags["generated_at_utc"] = pd.Timestamp(generated_at).tz_convert("UTC")

    flags = flags.reset_index().rename(columns={"index": "equipment_id"})
    # Worst first: this table is read top-down by someone with a finite morning.
    flags["_severity_rank"] = (flags["severity"] == SEVERITY_CRITICAL).astype(int)
    flags = flags.sort_values(
        ["_severity_rank", "health_score", "equipment_id"],
        ascending=[False, True, True],
        kind="stable",
    )
    return conform(flags, GOLD_HEALTH_FLAGS_SCHEMA)


def _join_reasons(fired: pd.DataFrame) -> pd.Series:
    """Comma-join the rules that fired, always in RULE_ORDER."""
    joined = np.full(len(fired), "", dtype=object)
    for rule in RULE_ORDER:
        mask = fired[rule].to_numpy(dtype=bool)
        joined = np.where(
            mask,
            np.where(joined == "", rule, np.char.add(joined.astype(str), "," + rule)),
            joined,
        )
    return pd.Series(joined, index=fired.index)


# ---------------------------------------------------------------------------
# Table 3: fleet_summary_by_type   (grain: one row per equipment_type)
# ---------------------------------------------------------------------------
def fleet_summary_by_type(
    silver: pd.DataFrame,
    daily: pd.DataFrame,
    flags: pd.DataFrame,
    generated_at: datetime | None = None,
) -> pd.DataFrame:
    """Roll the per-machine tables up to machine class, for the dashboard cards.

    Built from the two gold tables rather than from silver a second time, so the
    summary cards and the detail tables can never disagree — if a number is
    wrong here it is wrong there too, which is a much easier bug to find than two
    slightly different pipelines over the same rows.
    """
    generated_at = generated_at or datetime.now(timezone.utc)
    if silver is None or len(silver) == 0:
        return _empty(GOLD_FLEET_SUMMARY_SCHEMA)

    types = sorted(silver["equipment_type"].dropna().unique())
    out = pd.DataFrame(index=pd.Index(types, name="equipment_type"))

    out["machine_count"] = (
        silver.groupby("equipment_type", sort=True)["equipment_id"]
        .nunique()
        .reindex(types)
        .fillna(0)
    )

    # Active = did real work recently, not merely "sent a packet". A machine
    # idling in the yard reports every five minutes and is not available capacity.
    anchor = silver["event_time"].max()
    recent = silver[
        (silver["event_time"] >= anchor - pd.Timedelta(hours=ACTIVE_WINDOW_HOURS))
        & silver["is_under_load"]
    ]
    out["active_machines_24h"] = (
        recent.groupby("equipment_type", sort=True)["equipment_id"]
        .nunique()
        .reindex(types)
        .fillna(0)
    )

    if len(daily):
        by_type = daily.groupby("equipment_type", sort=True)
        # Weighted by the number of readings each machine-day is based on, so
        # this equals the pooled average over all under-load readings. An
        # unweighted mean-of-daily-means would give a machine that ran for two
        # hours the same influence as one that ran for twenty — the classic
        # Simpson's-paradox mistake in a rollup, and it silently shifts the
        # fleet number by degrees.
        out["avg_coolant_temp_c"] = _weighted_mean(
            daily, "avg_coolant_temp_c", "under_load_readings"
        ).reindex(types)
        out["max_coolant_temp_c"] = by_type["max_coolant_temp_c"].max().reindex(types)
        out["avg_oil_pressure_psi"] = _weighted_mean(
            daily, "avg_oil_pressure_psi", "under_load_readings"
        ).reindex(types)
        out["total_operating_hours"] = by_type["operating_hours"].sum(min_count=1).reindex(types)
        # Weighted by operating hours, which makes this exactly
        # total fuel burned / total hours run rather than an average of rates.
        out["avg_fuel_burn_pct_per_hour"] = _weighted_mean(
            daily, "fuel_burn_pct_per_hour", "operating_hours"
        ).reindex(types)
        out["total_fault_codes"] = by_type["fault_code_count"].sum().reindex(types).fillna(0)
        out["days_covered"] = by_type["day"].nunique().reindex(types).fillna(0)
    else:
        for column in (
            "avg_coolant_temp_c",
            "max_coolant_temp_c",
            "avg_oil_pressure_psi",
            "total_operating_hours",
            "avg_fuel_burn_pct_per_hour",
        ):
            out[column] = np.nan
        out["total_fault_codes"] = 0
        out["days_covered"] = 0

    # Left-joined and filled with zero, never inner-joined: a machine class with
    # nothing wrong must still appear, showing 0 flagged. Dropping it would make
    # a healthy fleet look like a missing one on the dashboard.
    if len(flags):
        flag_groups = flags.groupby("equipment_type", sort=True)
        out["machines_flagged"] = (
            flag_groups["equipment_id"].nunique().reindex(types).fillna(0)
        )
        out["critical_count"] = (
            flags[flags["severity"] == SEVERITY_CRITICAL]
            .groupby("equipment_type", sort=True)["equipment_id"]
            .nunique()
            .reindex(types)
            .fillna(0)
        )
        out["warning_count"] = (
            flags[flags["severity"] == SEVERITY_WARNING]
            .groupby("equipment_type", sort=True)["equipment_id"]
            .nunique()
            .reindex(types)
            .fillna(0)
        )
    else:
        out["machines_flagged"] = 0
        out["critical_count"] = 0
        out["warning_count"] = 0

    out["pct_flagged"] = np.where(
        out["machine_count"] > 0,
        (100.0 * out["machines_flagged"] / out["machine_count"]).round(2),
        0.0,
    )
    out["generated_at_utc"] = pd.Timestamp(generated_at).tz_convert("UTC")

    out = out.reset_index()
    for column in (
        "machine_count",
        "active_machines_24h",
        "total_fault_codes",
        "machines_flagged",
        "critical_count",
        "warning_count",
        "days_covered",
    ):
        out[column] = out[column].astype("int64")

    return conform(out, GOLD_FLEET_SUMMARY_SCHEMA)


def _weighted_mean(daily: pd.DataFrame, value: str, weight: str) -> pd.Series:
    """Weighted mean of `value` by `weight`, per equipment_type.

    Rows where either side is null are dropped rather than treated as zero: a
    machine-day with no under-load readings has no opinion about the fleet's
    average temperature and must not vote with weight zero *or* pull the mean.
    """
    usable = daily[["equipment_type", value, weight]].dropna()
    usable = usable[usable[weight] > 0]
    if usable.empty:
        return pd.Series(dtype="float64")
    products = (usable[value] * usable[weight]).groupby(usable["equipment_type"]).sum()
    weights = usable[weight].groupby(usable["equipment_type"]).sum()
    return products / weights
