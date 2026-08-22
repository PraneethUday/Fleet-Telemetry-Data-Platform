"""Prefect flow: silver -> gold.

    python -m pipeline.flows.gold_flow                       # defaults
    python -m pipeline.flows.gold_flow --window-hours 48 -v  # wider health window
    python -m pipeline.flows.gold_flow --lookback-days 7     # only recent silver

Produces exactly three objects::

    gold/daily_equipment_summary.parquet    one row per machine per day
    gold/fleet_health_flags.parquet         one row per machine needing attention
    gold/fleet_summary_by_type.parquet      one row per machine class

Three decisions worth defending in this module
----------------------------------------------
**Gold is not partitioned.** Bronze and silver are, for a reason that does not
apply here: they are large, they are appended to daily, and a query for one day
must not open a year of files. These three tables are small (thousands of rows),
already aggregated, and read *whole* every single time — a dashboard loads all
of `fleet_summary_by_type`, not one day of it. Hive partitioning exists to let a
reader skip files, and there is nothing here worth skipping. Partitioning them
would add directory complexity, a merge step for the API, and buy exactly
nothing.

**Gold is a full rebuild, never an append.** Every run recomputes all three
tables from silver and overwrites them. The trade-off is real and taken
deliberately: an incremental gold would be cheaper on a fleet ten times this
size, but these are *current-state* tables — a machine drops off the health-flag
list when it gets fixed, and yesterday's flag must not linger. Incremental
updates would need delete-and-replace logic per key anyway, which is a rebuild
with more moving parts and more ways to leave a stale row behind. Rebuilding is
the simplest thing that is correct, and at this size it costs about a second.

**Everything is anchored to the data, not to the wall clock.** The lookback
window and the health window both end at the newest reading actually present.
The lake holds simulated history; `datetime.now()` would silently select zero
partitions and flag all 500 machines as stale telemetry. It also makes the run
reproducible: the same silver always produces the same gold.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger

from pipeline.config import Settings, get_settings
from pipeline.flows.silver_flow import QUARANTINE_DIRNAME, partition_date_of
from pipeline.schemas import GOLD_TABLES
from pipeline.storage import LakeStorage, get_storage
from pipeline.transforms.aggregations import (
    DEFAULT_WINDOW_HOURS,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    daily_equipment_summary,
    fleet_health_flags,
    fleet_summary_by_type,
)

# How much silver history the daily table covers by default. Wide enough for a
# month-on-month comparison, narrow enough that a three-year-old lake does not
# get read into memory every hour. The health window is a separate, much shorter
# parameter — the two answer different questions.
DEFAULT_LOOKBACK_DAYS = 30


def gold_table_key(settings: Settings, table: str) -> str:
    """Flat object key for a gold table. No ``dt=`` — see the module docstring."""
    if table not in GOLD_TABLES:
        raise KeyError(f"unknown gold table {table!r}; expected one of {sorted(GOLD_TABLES)}")
    return f"{settings.gold_prefix}/{table}.parquet"


# ---------------------------------------------------------------------------
# Tasks
#
# `cache_policy=NO_CACHE` wherever a LakeStorage is an argument: Prefect's
# default policy hashes inputs to build a cache key and a live blob client is
# neither hashable nor meaningfully cacheable. `retries` only on the tasks that
# touch the network — a blob GET fails transiently, a pandas aggregation does
# not, and retrying pure computation only hides a bug behind a delay.
# ---------------------------------------------------------------------------
@task(name="discover-silver-partitions", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def discover_silver_keys(
    storage: LakeStorage, settings: Settings, lookback_days: int
) -> list[str]:
    """Silver data objects within the lookback, newest partition first in date terms.

    Two filters that matter more than they look:

    * ``_quarantine`` is skipped. It lives under the silver prefix but carries a
      completely different schema, and a plain prefix scan would union rejected
      rows into the clean table — silently, since `read_parquet_many` would
      simply produce a frame with a lot of new NaN columns.
    * the lookback is measured from the newest *partition date present*, not
      from today. Same reason as everywhere else in this flow: the data is
      history, and a wall-clock cutoff would select nothing at all.
    """
    logger = get_run_logger()
    dated: dict[str, list[str]] = {}
    for key in storage.list_keys(settings.silver_prefix):
        if f"/{QUARANTINE_DIRNAME}/" in key:
            continue
        date = partition_date_of(key)
        if date is None:
            continue
        dated.setdefault(date, []).append(key)

    if not dated:
        logger.warning("No silver partitions found under %s", settings.silver_prefix)
        return []

    newest = max(dated)
    cutoff = (datetime.fromisoformat(newest) - timedelta(days=lookback_days - 1)).date()
    selected = sorted(
        key
        for date, keys in dated.items()
        if datetime.fromisoformat(date).date() >= cutoff
        for key in keys
    )
    logger.info(
        "Selected %d silver object(s) from %d partition(s) in [%s .. %s]",
        len(selected),
        sum(1 for d in dated if datetime.fromisoformat(d).date() >= cutoff),
        cutoff.isoformat(),
        newest,
    )
    return selected


@task(name="read-silver", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def read_silver(storage: LakeStorage, keys: list[str]) -> pd.DataFrame:
    """Read the selected silver partitions into one frame.

    The whole lookback is held in memory at once, unlike the silver flow which
    is strictly one partition at a time. That is a considered difference, not an
    oversight: gold aggregates *across* days (a trailing 24-hour window does not
    respect midnight, and neither does a machine's fuel curve), so a per-partition
    loop would have to carry state across iterations and would still be wrong at
    the boundary. A month of this fleet is ~3.2M rows and ~400 MB — comfortable.
    `lookback_days` is the dial that keeps it that way.
    """
    logger = get_run_logger()
    frame = storage.read_parquet_many(keys)
    logger.info("Read %d silver row(s) from %d object(s)", len(frame), len(keys))
    return frame


@task(name="build-daily-summary", cache_policy=NO_CACHE)
def build_daily_summary(silver: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    daily = daily_equipment_summary(silver)
    logger.info(
        "daily_equipment_summary: %d row(s), %d machine(s), %d day(s)",
        len(daily),
        daily["equipment_id"].nunique(),
        daily["day"].nunique(),
    )
    return daily


@task(name="build-health-flags", cache_policy=NO_CACHE)
def build_health_flags(
    silver: pd.DataFrame, window_hours: int, generated_at: datetime
) -> pd.DataFrame:
    logger = get_run_logger()
    flags = fleet_health_flags(silver, window_hours=window_hours, generated_at=generated_at)
    fleet_size = silver["equipment_id"].nunique() if len(silver) else 0
    rate = (100.0 * len(flags) / fleet_size) if fleet_size else 0.0
    logger.info(
        "fleet_health_flags: %d machine(s) flagged of %d (%.1f%%) over a %dh window",
        len(flags),
        fleet_size,
        rate,
        window_hours,
    )
    return flags


@task(name="build-fleet-summary", cache_policy=NO_CACHE)
def build_fleet_summary(
    silver: pd.DataFrame,
    daily: pd.DataFrame,
    flags: pd.DataFrame,
    generated_at: datetime,
) -> pd.DataFrame:
    logger = get_run_logger()
    summary = fleet_summary_by_type(silver, daily, flags, generated_at=generated_at)
    logger.info("fleet_summary_by_type: %d equipment type(s)", len(summary))
    return summary


@task(name="write-gold-table", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def write_gold_table(
    storage: LakeStorage, settings: Settings, table: str, frame: pd.DataFrame
) -> str:
    """Overwrite one gold table with this run's version.

    A single fixed key per table, so the write *is* the replacement — no
    delete-then-write gap during which a dashboard could read a missing table,
    and no chance of a stale ``part-0001`` from an older run being unioned in
    forever. Deliberately not `delete_prefix(gold/)`: other tables may come to
    live under this prefix, and a rebuild of these three has no business
    removing them.
    """
    logger = get_run_logger()
    key = gold_table_key(settings, table)
    storage.write_parquet(frame, key, schema=GOLD_TABLES[table])
    logger.info("Wrote %s (%d row(s)) -> %s", table, len(frame), storage.uri(key))
    return key


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------
@flow(name="silver-to-gold")
def gold_flow(
    window_hours: int = DEFAULT_WINDOW_HOURS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    verbose: bool = False,
) -> dict[str, Any]:
    """Rebuild all three gold tables from the silver layer.

    Args:
        window_hours: trailing window for the health flags, ending at the newest
            reading in silver. Wider means more history per trend fit and staler
            alerts; 24h is one full duty cycle including a night shift.
        lookback_days: how much silver history to aggregate for the daily table.
        verbose: log the per-rule flag breakdown.
    """
    logger = get_run_logger()
    started = datetime.now(timezone.utc)

    settings = get_settings()
    storage = get_storage(settings)
    logger.info(
        "Gold flow starting: storage=%s window_hours=%d lookback_days=%d",
        storage,
        window_hours,
        lookback_days,
    )

    keys = discover_silver_keys(storage, settings, lookback_days)
    silver = read_silver(storage, keys)

    # One timestamp for the whole run, taken once and threaded through both
    # tables. If each table called `now()` itself, `generated_at_utc` would
    # differ by milliseconds between them and "the flags and the summary from
    # the same run" would stop being a thing you can express as a join.
    generated_at = datetime.now(timezone.utc)

    daily = build_daily_summary(silver)
    flags = build_health_flags(silver, window_hours, generated_at)
    fleet = build_fleet_summary(silver, daily, flags, generated_at)

    tables = {
        "daily_equipment_summary": daily,
        "fleet_health_flags": flags,
        "fleet_summary_by_type": fleet,
    }
    written = {
        name: write_gold_table(storage, settings, name, frame)
        for name, frame in tables.items()
    }

    fleet_size = int(silver["equipment_id"].nunique()) if len(silver) else 0
    severity = flags["severity"].value_counts().to_dict() if len(flags) else {}
    reason_counts: dict[str, int] = {}
    for reasons in flags["flag_reasons"] if len(flags) else []:
        for reason in reasons.split(","):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    summary: dict[str, Any] = {
        "storage": str(storage),
        "window_hours": window_hours,
        "lookback_days": lookback_days,
        "generated_at_utc": generated_at.isoformat(),
        "silver_files": len(keys),
        "silver_rows": int(len(silver)),
        "fleet_size": fleet_size,
        # The instant every window is measured back from — the newest reading in
        # silver, not the clock. Reported so a reader of the logs can see which
        # it was without re-deriving it.
        "data_anchor_utc": (
            silver["event_time"].max().isoformat() if len(silver) else None
        ),
        "rows": {name: int(len(frame)) for name, frame in tables.items()},
        "keys": written,
        "machines_flagged": int(len(flags)),
        "flag_rate_pct": round(100.0 * len(flags) / fleet_size, 2) if fleet_size else 0.0,
        "severity": {
            SEVERITY_CRITICAL: int(severity.get(SEVERITY_CRITICAL, 0)),
            SEVERITY_WARNING: int(severity.get(SEVERITY_WARNING, 0)),
        },
        "flag_reasons": dict(sorted(reason_counts.items(), key=lambda kv: -kv[1])),
    }
    summary["duration_seconds"] = round(
        (datetime.now(timezone.utc) - started).total_seconds(), 2
    )

    logger.info(
        "Gold flow finished: %d silver row(s) -> daily=%d flags=%d types=%d "
        "(%d critical, %d warning; %.1f%% of %d machines) in %.1fs",
        summary["silver_rows"],
        summary["rows"]["daily_equipment_summary"],
        summary["rows"]["fleet_health_flags"],
        summary["rows"]["fleet_summary_by_type"],
        summary["severity"][SEVERITY_CRITICAL],
        summary["severity"][SEVERITY_WARNING],
        summary["flag_rate_pct"],
        fleet_size,
        summary["duration_seconds"],
    )
    if verbose and reason_counts:
        logger.info("Flag reasons: %s", summary["flag_reasons"])
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gold_flow",
        description="Aggregate the silver zone into the gold (analytics) zone.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=DEFAULT_WINDOW_HOURS,
        help="trailing health window, ending at the newest reading "
        f"(default: {DEFAULT_WINDOW_HOURS})",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"how much silver history to aggregate (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="log the per-rule flag breakdown"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    summary = gold_flow(
        window_hours=args.window_hours,
        lookback_days=args.lookback_days,
        verbose=args.verbose,
    )
    # JSON on stdout so the container job's logs are machine-readable: an alert
    # rule can assert on flag_rate_pct without scraping prose.
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
