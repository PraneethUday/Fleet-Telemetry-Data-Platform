"""CLI: run the warehouse query catalogue against the lake.

    python -m pipeline.warehouse.run_queries                      # all queries
    python -m pipeline.warehouse.run_queries --list               # names only
    python -m pipeline.warehouse.run_queries --query fault_code_pareto
    python -m pipeline.warehouse.run_queries --query critical_machines --limit 5
    python -m pipeline.warehouse.run_queries --query partition_pruning_demo --analyze

This is two things at once, and the second one is the reason it exists.

It is a **demo**: it prints each business question above its answer, so the
catalogue reads as an explanation of what the warehouse layer is for rather
than as a pile of SQL.

It is also a **smoke test**. It exits non-zero if any query fails to compile or
run, which makes `python -m pipeline.warehouse.run_queries` a complete
end-to-end assertion that bronze, silver and gold are all present, mutually
consistent and readable by an external engine — one command, no test fixtures,
run against the real lake. With `--require-rows` it additionally fails on an
empty result, which is the stricter contract CI wants: a query that returns
nothing has usually stopped matching the data rather than found nothing.

`--explain` prints the query plan; `--analyze` runs the query and prints the
*profiled* plan. Both exist because they answer different questions, and the
one that matters for partitioning is `--analyze`: `Total Files Read` only
appears once DuckDB has actually opened the files, so it is the only form that
proves pruning rather than predicting it.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import duckdb

from pipeline.config import get_settings
from pipeline.warehouse.session import (
    MissingLayerError,
    get_warehouse,
    materialised_objects,
    view_locations,
)

log = logging.getLogger("warehouse")

QUERIES_PATH = Path(__file__).with_name("queries.sql")

_NAME_RE = re.compile(r"^--\s*name:\s*([A-Za-z0-9_]+)\s*$")
_META_RE = re.compile(r"^--\s*(layer|question):\s*(.+?)\s*$")

# Presentation caps. `--limit` is a real SQL LIMIT and changes the answer;
# these only change how much of it is printed, so a wide gold table never
# reflows the terminal into unreadable soup.
MAX_ROWS_SHOWN = 50
MAX_CELL_WIDTH = 44


@dataclass
class Query:
    """One named query from `queries.sql`."""

    name: str
    layer: str = ""
    question: str = ""
    body: list[str] = field(default_factory=list)

    @property
    def sql(self) -> str:
        """The statement alone: no surrounding prose, no trailing `;`.

        Comment lines are trimmed from *both* ends, not just the front. The
        trailing ones belong to the next query's banner — the parser cannot
        know a block has ended until it sees the next `-- name:`, so a few of
        its header lines always land here. Leaving them attached puts the
        statement's `;` in the middle of the text, which `--limit` then wraps
        into `SELECT * FROM ( ... ; -- ... ) LIMIT n` and DuckDB rejects.
        """
        def is_prose(line: str) -> bool:
            return not line.strip() or line.lstrip().startswith("--")

        lines = list(self.body)
        while lines and is_prose(lines[0]):
            lines.pop(0)
        while lines and is_prose(lines[-1]):
            lines.pop()
        return "\n".join(lines).strip().rstrip(";")


def load_queries(path: Path | None = None) -> dict[str, Query]:
    """Parse `queries.sql` into named statements, in file order.

    The file is deliberately parsed rather than kept as a Python dict of
    strings: SQL in a `.sql` file gets syntax highlighting, diffs cleanly and
    can be pasted straight into a DuckDB shell. Embedding it in Python would
    trade all of that for nothing.
    """
    path = path or QUERIES_PATH
    text = path.read_text(encoding="utf-8")

    queries: dict[str, Query] = {}
    current: Query | None = None

    for line in text.splitlines():
        name_match = _NAME_RE.match(line)
        if name_match:
            current = Query(name=name_match.group(1))
            if current.name in queries:
                raise ValueError(f"duplicate query name {current.name!r} in {path}")
            queries[current.name] = current
            continue
        if current is None:
            continue  # file-level preamble, before the first `-- name:`

        meta_match = _META_RE.match(line)
        if meta_match and not current.sql:
            key, value = meta_match.groups()
            setattr(current, key, value)
            continue
        current.body.append(line)

    if not queries:
        raise ValueError(f"no `-- name:` headers found in {path}")
    return queries


# ---------------------------------------------------------------------------
# Rendering
#
# Hand-rolled rather than pulled from a table library: the output is part of
# what this CLI is for, the dependency list is a deployment surface, and the
# whole thing is forty lines.
# ---------------------------------------------------------------------------
def _format_value(value: Any) -> tuple[str, bool]:
    """Render one cell. Returns (text, is_numeric) — numerics right-align."""
    if value is None:
        return "NULL", False
    if isinstance(value, bool):
        return str(value).lower(), False
    if isinstance(value, (int, Decimal)):
        return f"{value:,}", True
    if isinstance(value, float):
        # Two decimals reads as a measurement; the queries already round to the
        # precision each number deserves, so this is only a display floor.
        return (f"{value:,.2f}" if abs(value) >= 0.01 or value == 0 else f"{value:.3g}"), True
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S") + ("Z" if value.tzinfo else ""), False
    if isinstance(value, date):
        return value.isoformat(), False
    return str(value), False


def _truncate(text: str, width: int) -> str:
    return text if len(text) <= width else text[: width - 1] + "…"


def render_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not rows:
        return "(no rows)"

    shown = rows[:MAX_ROWS_SHOWN]
    cells: list[list[str]] = []
    numeric = [True] * len(columns)
    for row in shown:
        rendered = []
        for index, value in enumerate(row):
            text, is_numeric = _format_value(value)
            numeric[index] = numeric[index] and (is_numeric or value is None)
            rendered.append(_truncate(text, MAX_CELL_WIDTH))
        cells.append(rendered)

    widths = [
        min(MAX_CELL_WIDTH, max(len(str(col)), *(len(row[i]) for row in cells)))
        for i, col in enumerate(columns)
    ]

    def line(parts: Sequence[str], align_numeric: bool) -> str:
        return "  ".join(
            part.rjust(widths[i]) if (align_numeric and numeric[i]) else part.ljust(widths[i])
            for i, part in enumerate(parts)
        ).rstrip()

    out = [
        line([_truncate(str(c), MAX_CELL_WIDTH) for c in columns], True),
        "  ".join("-" * width for width in widths),
    ]
    out.extend(line(row, True) for row in cells)
    if len(rows) > len(shown):
        out.append(f"... {len(rows) - len(shown):,} more row(s) not shown")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def _statement(query: Query, limit: int | None, mode: str) -> str:
    sql = query.sql
    if limit is not None:
        # Wrapping rather than appending: several catalogue queries already end
        # in their own LIMIT or ORDER BY, and a second LIMIT glued on the end is
        # either a syntax error or a silent change of meaning.
        sql = f"SELECT * FROM (\n{sql}\n) LIMIT {int(limit)}"
    if mode == "explain":
        return f"EXPLAIN {sql}"
    if mode == "analyze":
        return f"EXPLAIN ANALYZE {sql}"
    return sql


def run_one(
    con: duckdb.DuckDBPyConnection,
    query: Query,
    limit: int | None = None,
    mode: str = "run",
) -> tuple[int, float]:
    """Execute one query and print it. Returns (row_count, seconds)."""
    # `execute` rather than `sql`: `sql()` returns a relation, and a relation is
    # None for anything DuckDB has to expand into more than one statement —
    # which includes `EXPLAIN` over a `PIVOT`, because PIVOT first runs a query
    # of its own to discover the column values. `execute` handles both.
    started = time.perf_counter()
    cursor = con.execute(_statement(query, limit, mode))
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - started

    if mode in ("explain", "analyze"):
        # The plan comes back as a single (plan_type, plan_text) row of box art.
        print("\n".join(str(row[-1]) for row in rows))
        return len(rows), elapsed

    print(render_table([d[0] for d in cursor.description], rows))
    return len(rows), elapsed


def _header(index: int, total: int, query: Query) -> str:
    rule = "─" * 78
    lines = [rule, f"[{index}/{total}] {query.name}"]
    if query.layer:
        lines.append(f"   layer: {query.layer}")
    if query.question:
        lines.append(f"   Q: {query.question}")
    lines.append(rule)
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_queries",
        description="Run the DuckDB warehouse query catalogue against the lake.",
    )
    parser.add_argument("--query", "-q", help="run a single query by name")
    parser.add_argument("--limit", "-n", type=int, help="cap each result to N rows")
    parser.add_argument("--list", action="store_true", help="list query names and exit")
    plans = parser.add_mutually_exclusive_group()
    plans.add_argument("--explain", action="store_true", help="print the query plan instead of results")
    plans.add_argument(
        "--analyze",
        action="store_true",
        help="run the query and print the profiled plan (shows files actually read)",
    )
    parser.add_argument(
        "--require-rows",
        action="store_true",
        help="exit non-zero if any query returns no rows (stricter CI smoke test)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)-7s %(name)s | %(message)s",
    )

    queries = load_queries()

    if args.list:
        for query in queries.values():
            print(f"{query.name:<24} {query.layer:<40} {query.question}")
        return 0

    if args.query:
        if args.query not in queries:
            print(
                f"unknown query {args.query!r}. Available: {', '.join(queries)}",
                file=sys.stderr,
            )
            return 2
        selected = {args.query: queries[args.query]}
    else:
        selected = queries

    settings = get_settings()
    try:
        con = get_warehouse(settings)
    except MissingLayerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    mode = "explain" if args.explain else "analyze" if args.analyze else "run"
    failures: list[str] = []
    empties: list[str] = []

    print(f"lake backend: {settings.lake_backend}")
    for view, location in view_locations(settings).items():
        print(f"  {view:<24} {location}")
    print()

    try:
        total = len(selected)
        for index, query in enumerate(selected.values(), start=1):
            print(_header(index, total, query))
            try:
                rows, elapsed = run_one(con, query, args.limit, mode)
            except duckdb.Error as exc:
                # Caught per query so one broken statement reports itself and
                # the rest still run: a smoke test that stops at the first
                # failure hides how much else is broken.
                failures.append(query.name)
                print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
            else:
                if mode == "run":
                    print(f"\n{rows:,} row(s) in {elapsed * 1000:.0f} ms\n")
                    if rows == 0:
                        empties.append(query.name)
                else:
                    print(f"\nplan produced in {elapsed * 1000:.0f} ms\n")

        # The claim this whole layer rests on, re-checked every run rather than
        # asserted in a docstring: DuckDB is holding views, not data.
        materialised = materialised_objects(con)
        print("─" * 78)
        if materialised:
            print(f"WARNING: base tables exist in DuckDB storage: {materialised}")
            failures.append("materialisation-check")
        else:
            print("storage check: 0 base tables in DuckDB — every view scans Parquet in place")
    finally:
        con.close()

    ran = len(selected) - len(failures)
    print(f"ran {ran}/{len(selected)} query(s) against the {settings.lake_backend} lake")
    if empties:
        print(f"WARNING: returned no rows: {', '.join(empties)}")
    if failures:
        print(f"FAILED: {', '.join(failures)}", file=sys.stderr)
        return 1
    if empties and args.require_rows:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
