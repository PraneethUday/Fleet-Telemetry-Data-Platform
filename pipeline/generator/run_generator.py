"""CLI: generate simulated telemetry and land it in the bronze zone.

    # 3 days of history for a 500-machine fleet, into ./data/raw
    python -m pipeline.generator.run_generator --days 3 --reset

    # one hour, appended to whatever is already there (what the hourly job runs)
    python -m pipeline.generator.run_generator --hours 1

Layout produced::

    raw/dt=2026-08-22/batch_0000_3f9a1c2d.parquet
    raw/dt=2026-08-22/batch_0100_8c40be71.parquet
    ...

Partitioning by ``dt=`` is Hive-style partitioning. It is not cosmetic: DuckDB,
Spark and Synapse all read the date out of the *path* and skip whole files when
a query filters on it, so "yesterday's readings" never opens last month's data.
The partition key is the reading's own UTC event date, not the time the file was
written, so a late-arriving batch still lands in the day it belongs to.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from pipeline.config import get_settings
from pipeline.generator.simulator import FleetSimulator, GeneratorConfig
from pipeline.schemas import BRONZE_SCHEMA
from pipeline.storage import get_storage

log = logging.getLogger("generator")

STATE_KEY = "_state/generator_state.json"


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(
        timezone.utc
    )


def _floor_to(dt: datetime, seconds: int) -> datetime:
    epoch = int(dt.timestamp()) // seconds * seconds
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_generator",
        description="Generate fleet telemetry into the bronze (raw) zone.",
    )
    window = p.add_argument_group("simulation window")
    window.add_argument("--start", type=str, help="ISO start, UTC (default: derived)")
    window.add_argument("--end", type=str, help="ISO end, UTC (default: now)")
    window.add_argument("--days", type=float, help="backfill this many days")
    window.add_argument("--hours", type=float, help="backfill this many hours")

    fleet = p.add_argument_group("fleet")
    fleet.add_argument("--fleet-size", type=int, help="default from FLEET_SIZE env")
    fleet.add_argument("--seed", type=int, help="default from GENERATOR_SEED env")
    fleet.add_argument("--interval-seconds", type=int, default=300)
    fleet.add_argument("--batch-minutes", type=int, default=60)

    out = p.add_argument_group("output")
    out.add_argument(
        "--reset", action="store_true", help="delete the existing raw/ prefix first"
    )
    out.add_argument(
        "--no-resume",
        action="store_true",
        help="ignore any saved generator state and start the fleet fresh",
    )
    out.add_argument(
        "--dry-run", action="store_true", help="simulate but write nothing"
    )
    out.add_argument("-v", "--verbose", action="store_true")
    return p


def resolve_window(args) -> tuple[datetime, datetime]:
    """Work out [start, end) from whichever combination of flags was given."""
    now = _floor_to(datetime.now(timezone.utc), args.interval_seconds)
    end = _parse_dt(args.end) if args.end else now
    if args.start:
        start = _parse_dt(args.start)
    elif args.days:
        start = end - timedelta(days=args.days)
    elif args.hours:
        start = end - timedelta(hours=args.hours)
    else:
        start = end - timedelta(days=1)
    start = _floor_to(start, args.interval_seconds)
    end = _floor_to(end, args.interval_seconds)
    if start >= end:
        raise SystemExit(f"empty window: start={start.isoformat()} end={end.isoformat()}")
    return start, end


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    settings = get_settings()
    storage = get_storage(settings)
    cfg = GeneratorConfig(
        fleet_size=args.fleet_size or settings.fleet_size,
        seed=args.seed if args.seed is not None else settings.generator_seed,
        interval_seconds=args.interval_seconds,
        batch_minutes=args.batch_minutes,
    )
    start, end = resolve_window(args)

    log.info("lake backend   : %s", storage)
    log.info("bronze prefix  : %s", storage.uri(settings.bronze_prefix))
    log.info(
        "window         : %s -> %s  (%.1f h)",
        start.isoformat(),
        end.isoformat(),
        (end - start).total_seconds() / 3600,
    )
    log.info(
        "fleet          : %d machines, one reading / %ds, %d-min batches",
        cfg.fleet_size,
        cfg.interval_seconds,
        cfg.batch_minutes,
    )

    if args.reset and not args.dry_run:
        removed = storage.delete_prefix(settings.bronze_prefix)
        log.info("reset          : removed %d existing objects from raw/", removed)

    sim = FleetSimulator(cfg)

    # Resume the fleet's physical state from the last run so the hour meter and
    # fuel level stay continuous across separate container executions.
    if not args.no_resume and not args.reset and storage.exists(STATE_KEY):
        try:
            sim.load_state(json.loads(storage.read_bytes(STATE_KEY)))
            log.info("state          : resumed from %s", storage.uri(STATE_KEY))
        except (ValueError, KeyError) as exc:
            log.warning("state          : ignoring saved state (%s)", exc)

    files = 0
    rows = 0
    faults = 0
    last_ts = start
    for batch in sim.simulate(start, end):
        key = f"{settings.bronze_prefix}/dt={batch.partition_date}/{batch.filename}"
        rows += len(batch.frame)
        faults += int(batch.frame["fault_code"].notna().sum())
        last_ts = batch.batch_end
        if args.dry_run:
            log.debug("[dry-run] would write %6d rows -> %s", len(batch.frame), key)
        else:
            storage.write_parquet(batch.frame, key, schema=BRONZE_SCHEMA)
            log.debug("wrote %6d rows -> %s", len(batch.frame), key)
        files += 1
        if files % 24 == 0:
            log.info("  ... %d files, %d rows so far", files, rows)

    if not args.dry_run:
        state = json.dumps(sim.to_state(last_ts)).encode()
        storage.write_bytes(STATE_KEY, state)

    log.info("-" * 66)
    log.info("bronze written : %d files, %d rows", files, rows)
    log.info(
        "fault codes    : %d rows (%.2f%% — raw feed is mostly null by design)",
        faults,
        100.0 * faults / max(rows, 1),
    )
    if args.dry_run:
        log.info("DRY RUN — nothing was written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
