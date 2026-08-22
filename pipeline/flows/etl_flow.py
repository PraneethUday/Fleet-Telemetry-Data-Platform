"""Prefect flow: the whole pipeline, bronze -> silver -> gold.

    python -m pipeline.flows.etl_flow            # the container entrypoint
    python -m pipeline.flows.etl_flow --full-refresh -v

This is the image's entrypoint, and it is deliberately thin. All the judgement
lives in `silver_flow` and `gold_flow`; this module exists so the container has
one command to run and so the two stages are ordered by a single flow run rather
than by a shell script and a hope. Running them as subflows (not as separate
`docker run`s) means one Prefect run id covers both, so a gold table can always
be traced back to the silver run that produced it.

Ordering is the whole point: gold reads what silver just wrote, so a failure in
silver must stop the run rather than quietly rebuilding gold from yesterday's
data — which would look exactly like success. Prefect gives that for free, since
an exception in the subflow propagates here.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from prefect import flow
from prefect.logging import get_run_logger

from pipeline.flows.gold_flow import DEFAULT_LOOKBACK_DAYS, gold_flow
from pipeline.flows.silver_flow import silver_flow
from pipeline.transforms.aggregations import DEFAULT_WINDOW_HOURS


@flow(name="fleet-etl")
def etl_flow(
    dates: list[str] | None = None,
    full_refresh: bool = False,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    verbose: bool = False,
) -> dict[str, Any]:
    """Run bronze -> silver, then silver -> gold, and return both summaries."""
    logger = get_run_logger()
    started = datetime.now(timezone.utc)

    silver = silver_flow(dates=dates, full_refresh=full_refresh, verbose=verbose)
    # Unconditional, even when every silver partition was skipped as current:
    # gold is a full rebuild of *current state*, and its health window slides
    # with the clock even when no new readings arrived.
    gold = gold_flow(
        window_hours=window_hours, lookback_days=lookback_days, verbose=verbose
    )

    summary = {
        "silver": silver,
        "gold": gold,
        "duration_seconds": round(
            (datetime.now(timezone.utc) - started).total_seconds(), 2
        ),
    }
    logger.info(
        "ETL finished: silver %d row(s) out across %d partition(s); gold %d flagged "
        "of %d machine(s) in %.1fs total",
        silver["rows_out"],
        len(silver["dates_processed"]),
        gold["machines_flagged"],
        gold["fleet_size"],
        summary["duration_seconds"],
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl_flow",
        description="Run the full bronze -> silver -> gold pipeline.",
    )
    parser.add_argument(
        "--dates", nargs="+", metavar="YYYY-MM-DD", help="only these silver partitions"
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="reprocess silver even when unchanged"
    )
    parser.add_argument("--window-hours", type=int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    summary = etl_flow(
        dates=args.dates,
        full_refresh=args.full_refresh,
        window_hours=args.window_hours,
        lookback_days=args.lookback_days,
        verbose=args.verbose,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
