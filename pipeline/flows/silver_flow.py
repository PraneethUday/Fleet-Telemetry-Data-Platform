"""Prefect flow: bronze -> silver.

    python -m pipeline.flows.silver_flow                      # incremental
    python -m pipeline.flows.silver_flow --full-refresh -v    # rebuild everything
    python -m pipeline.flows.silver_flow --dates 2026-08-22   # one partition

Produces::

    silver/dt=2026-08-22/part-0000.parquet     cleaned readings (SILVER_SCHEMA)
    silver/dt=2026-08-22/_manifest.json        what was processed, and from what
    silver/_quarantine/dt=2026-08-22/part-0000.parquet   rejects (QUARANTINE_SCHEMA)

Three decisions worth defending in this module
----------------------------------------------
**One partition at a time.** The flow never holds more than a single day of
readings in memory, so its footprint is set by the *busiest day* rather than by
how much history the lake contains. Three years of bronze costs exactly the same
RAM as three days. That property — not query speed — is the main reason the lake
is partitioned by date at all, and it is what lets this run in a 0.5 GB Container
Apps job.

**Rewrite the partition, never append.** Reprocessing a date deletes that date's
silver objects and writes them again. If it appended, a rerun after a failed
downstream step would double the day's rows, and nobody would notice until a
fuel-consumption number looked odd a week later. Idempotence is a property you
build in deliberately or you do not have.

**Freshness is decided by the bronze key set, not by mtimes.** A partition is
skipped when the exact set of bronze files it was built from is still the set of
bronze files present. Bronze objects are immutable-by-name (every batch lands
under a fresh uuid-suffixed filename), so the key set is a sound content
signature — and unlike a modification time, it means the same thing on local
disk as in Blob Storage, where restoring or re-uploading a container rewrites
every timestamp without changing a byte. The manifest recording it is written
*after* the data, so a crash mid-write leaves no manifest and the next run
rebuilds the partition instead of trusting a half-written one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger

from pipeline.config import Settings, get_settings
from pipeline.schemas import QUARANTINE_SCHEMA, SILVER_SCHEMA
from pipeline.storage import LakeStorage, get_storage
from pipeline.transforms.cleaning import CleaningResult, clean_bronze_partition

# Hive partition marker in a key: "raw/dt=2026-08-22/batch_0600_a1b2c3d4.parquet".
_PARTITION_RE = re.compile(r"(?:^|/)dt=(\d{4}-\d{2}-\d{2})(?:/|$)")

PART_FILENAME = "part-0000.parquet"
MANIFEST_FILENAME = "_manifest.json"
QUARANTINE_DIRNAME = "_quarantine"
MANIFEST_VERSION = 1


# ---------------------------------------------------------------------------
# Key layout. Centralised so the gold flow and the API can import these instead
# of re-deriving path strings that must agree exactly.
# ---------------------------------------------------------------------------
def silver_partition_prefix(settings: Settings, date: str) -> str:
    return f"{settings.silver_prefix}/dt={date}"


def silver_partition_key(settings: Settings, date: str) -> str:
    return f"{silver_partition_prefix(settings, date)}/{PART_FILENAME}"


def manifest_key(settings: Settings, date: str) -> str:
    return f"{silver_partition_prefix(settings, date)}/{MANIFEST_FILENAME}"


def quarantine_partition_prefix(settings: Settings, date: str) -> str:
    # Under the silver prefix but one level deeper, so the ordinary
    # "silver/dt=*/*.parquet" scan cannot accidentally union rejected rows —
    # which carry a different schema — into the clean table.
    return f"{settings.silver_prefix}/{QUARANTINE_DIRNAME}/dt={date}"


def quarantine_partition_key(settings: Settings, date: str) -> str:
    return f"{quarantine_partition_prefix(settings, date)}/{PART_FILENAME}"


def partition_date_of(key: str) -> str | None:
    match = _PARTITION_RE.search(key)
    return match.group(1) if match else None


def _fingerprint(keys: list[str]) -> str:
    """Content signature of a partition's inputs: its sorted key set."""
    return hashlib.sha256("\n".join(sorted(keys)).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Tasks
#
# `cache_policy=NO_CACHE` on the tasks that take a LakeStorage: Prefect's default
# policy hashes task inputs to build a cache key, and a live blob client is
# neither hashable nor meaningfully cacheable. Being explicit beats a warning.
# `retries` sit on exactly the tasks that touch the network — a blob GET can fail
# transiently, a pandas transform cannot, and retrying pure computation just
# hides bugs behind a delay.
# ---------------------------------------------------------------------------
@task(name="discover-bronze-partitions", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def discover_bronze_partitions(storage: LakeStorage, settings: Settings) -> dict[str, list[str]]:
    """Group every bronze object under its ``dt=`` partition date."""
    logger = get_run_logger()
    partitions: dict[str, list[str]] = {}
    skipped = 0
    for key in storage.list_keys(settings.bronze_prefix):
        date = partition_date_of(key)
        if date is None:
            skipped += 1  # not Hive-partitioned; not ours to interpret
            continue
        partitions.setdefault(date, []).append(key)

    for date in partitions:
        partitions[date].sort()  # deterministic read order -> deterministic dedup
    if skipped:
        logger.warning("Ignored %d bronze object(s) outside a dt= partition", skipped)
    logger.info(
        "Discovered %d bronze partition(s), %d file(s) total",
        len(partitions),
        sum(len(v) for v in partitions.values()),
    )
    return dict(sorted(partitions.items()))


@task(name="check-partition-freshness", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def partition_is_current(
    storage: LakeStorage, settings: Settings, date: str, bronze_keys: list[str]
) -> bool:
    """True when silver for ``date`` was built from exactly these bronze files."""
    key = manifest_key(settings, date)
    if not storage.exists(key) or not storage.exists(silver_partition_key(settings, date)):
        return False
    try:
        manifest = json.loads(storage.read_bytes(key).decode())
    except (ValueError, UnicodeDecodeError):
        # A manifest we cannot read is not evidence of anything. Rebuild.
        return False
    return manifest.get("bronze_fingerprint") == _fingerprint(bronze_keys)


@task(name="read-bronze-partition", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def read_bronze_partition(storage: LakeStorage, keys: list[str]) -> pd.DataFrame:
    logger = get_run_logger()
    frame = storage.read_parquet_many(keys)
    logger.info("Read %d row(s) from %d bronze file(s)", len(frame), len(keys))
    return frame


@task(name="clean-partition", cache_policy=NO_CACHE)
def clean_partition(frame: pd.DataFrame, processed_at: datetime) -> CleaningResult:
    """Thin wrapper: all the judgement lives in `transforms.cleaning`."""
    return clean_bronze_partition(frame, processed_at=processed_at)


@task(name="write-silver-partition", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def write_silver_partition(
    storage: LakeStorage,
    settings: Settings,
    date: str,
    result: CleaningResult,
    bronze_keys: list[str],
) -> dict[str, Any]:
    """Replace the date's silver partition and record what it was built from."""
    logger = get_run_logger()

    # Full replacement, not overwrite-one-file: an older run could have left
    # extra parts behind, and a stale part-0001 would be read as real data
    # forever. Deleting the prefix first makes the partition exactly what this
    # run produced.
    removed = storage.delete_prefix(silver_partition_prefix(settings, date))
    removed += storage.delete_prefix(quarantine_partition_prefix(settings, date))
    if removed:
        logger.debug("Cleared %d existing object(s) for dt=%s", removed, date)

    silver_key = silver_partition_key(settings, date)
    storage.write_parquet(result.silver, silver_key, schema=SILVER_SCHEMA)

    quarantine_key = None
    if result.rows_quarantined:
        quarantine_key = quarantine_partition_key(settings, date)
        storage.write_parquet(result.quarantine, quarantine_key, schema=QUARANTINE_SCHEMA)
        logger.warning(
            "dt=%s quarantined %d row(s): %s",
            date,
            result.rows_quarantined,
            result.quarantine_by_reason,
        )
    # No rejects means no file. An empty Parquet object would still have to be
    # opened by every reader of the quarantine table, forever, to learn nothing.

    manifest = {
        "version": MANIFEST_VERSION,
        "partition_date": date,
        "silver_key": silver_key,
        "quarantine_key": quarantine_key,
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_quarantined": result.rows_quarantined,
        "duplicates_removed": result.dedup.total_removed,
        "bronze_file_count": len(bronze_keys),
        "bronze_fingerprint": _fingerprint(bronze_keys),
        "bronze_keys": sorted(bronze_keys),
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    # Written last, deliberately: the manifest is the "this partition is
    # complete" marker the next run trusts, so it must not exist until the data
    # it describes does.
    storage.write_bytes(manifest_key(settings, date), json.dumps(manifest, indent=2).encode())
    return manifest


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------
@flow(name="bronze-to-silver")
def silver_flow(
    dates: list[str] | None = None,
    full_refresh: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
    """Clean every bronze partition that needs it and land it in silver.

    Args:
        dates: only these ``YYYY-MM-DD`` partitions. Default: all discovered.
        full_refresh: reprocess even partitions whose inputs have not changed.
        verbose: log the per-flag quality breakdown for every partition.
    """
    logger = get_run_logger()
    started = datetime.now(timezone.utc)

    settings = get_settings()
    storage = get_storage(settings)
    logger.info("Silver flow starting: storage=%s full_refresh=%s", storage, full_refresh)

    partitions = discover_bronze_partitions(storage, settings)
    if dates:
        requested = set(dates)
        missing = sorted(requested - partitions.keys())
        if missing:
            logger.warning("No bronze data for requested date(s): %s", ", ".join(missing))
        partitions = {d: k for d, k in partitions.items() if d in requested}

    summary: dict[str, Any] = {
        "storage": str(storage),
        "full_refresh": full_refresh,
        "dates_processed": [],
        "dates_skipped": [],
        "bronze_files": 0,
        "rows_in": 0,
        "rows_out": 0,
        "duplicates_removed": 0,
        "duplicates_exact_resend": 0,
        "duplicates_cross_batch_resend": 0,
        "rows_quarantined": 0,
        "quarantine_by_reason": {},
        "values_nulled": {},
        "quality_flag_row_counts": {},
        "partitions": {},
    }

    for date, bronze_keys in partitions.items():
        if not full_refresh and partition_is_current(storage, settings, date, bronze_keys):
            logger.info("dt=%s up to date (%d bronze file(s)) — skipping", date, len(bronze_keys))
            summary["dates_skipped"].append(date)
            continue

        frame = read_bronze_partition(storage, bronze_keys)
        result = clean_partition(frame, started)

        if not result.reconciles():
            # The row-count identity is the flow's own audit: every row that
            # entered is accounted for as kept, rejected or deduplicated. If it
            # ever fails, rows are vanishing somewhere in the transform and the
            # partition must not be published.
            raise ValueError(
                f"dt={date} row counts do not reconcile: in={result.rows_in} "
                f"out={result.rows_out} quarantined={result.rows_quarantined} "
                f"deduped={result.dedup.total_removed}"
            )

        write_silver_partition(storage, settings, date, result, bronze_keys)

        logger.info(
            "dt=%s  in=%d  out=%d  dedup=%d (exact=%d, cross-batch=%d)  quarantined=%d",
            date,
            result.rows_in,
            result.rows_out,
            result.dedup.total_removed,
            result.dedup.exact_resends,
            result.dedup.cross_batch_resends,
            result.rows_quarantined,
        )
        detail = "dt=%s quality flags: %s | values nulled: %s"
        if verbose:
            logger.info(detail, date, result.flag_row_counts, result.values_nulled)
        else:
            logger.debug(detail, date, result.flag_row_counts, result.values_nulled)

        summary["dates_processed"].append(date)
        summary["bronze_files"] += len(bronze_keys)
        summary["rows_in"] += result.rows_in
        summary["rows_out"] += result.rows_out
        summary["duplicates_removed"] += result.dedup.total_removed
        summary["duplicates_exact_resend"] += result.dedup.exact_resends
        summary["duplicates_cross_batch_resend"] += result.dedup.cross_batch_resends
        summary["rows_quarantined"] += result.rows_quarantined
        _accumulate(summary["quarantine_by_reason"], result.quarantine_by_reason)
        _accumulate(summary["values_nulled"], result.values_nulled)
        _accumulate(summary["quality_flag_row_counts"], result.flag_row_counts)
        summary["partitions"][date] = {
            "rows_in": result.rows_in,
            "rows_out": result.rows_out,
            "duplicates_removed": result.dedup.total_removed,
            "rows_quarantined": result.rows_quarantined,
        }

    summary["duration_seconds"] = round(
        (datetime.now(timezone.utc) - started).total_seconds(), 2
    )
    logger.info(
        "Silver flow finished: %d partition(s) processed, %d skipped, %d row(s) in -> %d out "
        "(%d duplicate, %d quarantined) in %.1fs",
        len(summary["dates_processed"]),
        len(summary["dates_skipped"]),
        summary["rows_in"],
        summary["rows_out"],
        summary["duplicates_removed"],
        summary["rows_quarantined"],
        summary["duration_seconds"],
    )
    return summary


def _accumulate(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + int(value)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="silver_flow",
        description="Clean the bronze zone into the silver (conformed) zone.",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="only these partitions (default: every bronze partition)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="reprocess partitions even when their bronze inputs are unchanged",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="log per-partition quality detail"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    summary = silver_flow(
        dates=args.dates, full_refresh=args.full_refresh, verbose=args.verbose
    )
    # Printed as JSON so the container job's logs are machine-readable: a
    # scheduler or an alert rule can assert on rows_out without scraping prose.
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
