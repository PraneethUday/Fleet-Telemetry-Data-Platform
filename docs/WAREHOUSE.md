# The warehouse layer

DuckDB, pointed at the Parquet in the lake, holding nothing of its own.

```
pipeline/warehouse/session.py      connection factory + view registration
pipeline/warehouse/queries.sql     the query catalogue (8 documented queries)
pipeline/warehouse/run_queries.py  CLI: runs the catalogue, doubles as a smoke test
```

```bash
python -m pipeline.warehouse.run_queries                       # all 8 queries
python -m pipeline.warehouse.run_queries --list                # names and questions
python -m pipeline.warehouse.run_queries -q fault_code_pareto -n 10
python -m pipeline.warehouse.run_queries -q partition_pruning_demo --analyze
```

The CLI exits non-zero if any query fails, which makes that first command a
complete end-to-end assertion that bronze, silver and gold are present, mutually
consistent, and readable by an engine that is not the one that wrote them. Add
`--require-rows` to also fail on an empty result — the stricter contract for CI.

---

## 1. What this layer is

Five views, registered on an in-memory DuckDB connection:

| View | Reads | Grain |
| --- | --- | --- |
| `bronze_telemetry` | `raw/dt=*/*.parquet` | one raw device reading, still dirty |
| `silver_telemetry` | `silver/dt=*/part-*.parquet` | one cleaned reading, deduplicated |
| `daily_equipment_summary` | `gold/daily_equipment_summary.parquet` | machine × day |
| `fleet_health_flags` | `gold/fleet_health_flags.parquet` | one flagged machine |
| `fleet_summary_by_type` | `gold/fleet_summary_by_type.parquet` | machine class |

Every one of them is a `read_parquet()` over files the pipeline already wrote.
There is no `COPY`, no `CREATE TABLE AS`, no `INSERT` anywhere in the package,
and the connection is opened as `:memory:` so there is no file it could write to
even by accident.

That is a claim worth verifying rather than asserting, so `run_queries.py`
checks it on every run and prints the result:

```
storage check: 0 base tables in DuckDB — every view scans Parquet in place
ran 8/8 query(s) against the local lake
```

A view is a stored *query*. A table is a stored *copy*. The distinction is the
whole design.

---

## 2. Why DuckDB and not Postgres

Not "DuckDB is faster" — they are built for different shapes of work, and this
layer is squarely one of them.

**Postgres is a row store built for point lookups and writes.** A row lives
contiguously on an 8 KB page. `SELECT * FROM readings WHERE id = 42` touches one
page through a B-tree: perfect. `SELECT avg(coolant_temp_c) FROM readings` has to
walk every page of every row to reach one 8-byte column, dragging eleven columns
it does not want through memory to get there. It also pays for machinery this
layer never uses: MVCC row versions, a WAL, per-row visibility checks, locks.
All of that exists to make concurrent *writes* safe, and the warehouse never
writes.

**DuckDB is a column store built for scan-and-aggregate.** Reading one column
reads one contiguous run of values. It executes in vectorised batches over
columns rather than row-at-a-time, so the aggregate loop stays in cache and the
compiler can use SIMD. Every query in `queries.sql` is the same shape — read many
rows, touch few columns, group, aggregate — which is the workload column stores
were invented for. The measurement in §4 shows the projection effect directly:
the same 426,760 rows cost 1.3 ms for one column and 8.1 ms for ten.

**Separating analytics from any transactional store is the point, not a
side effect.** The lake is append-only files; the warehouse is a read-only engine
over them. A heavy fleet-wide aggregation cannot lock a table, exhaust a
connection pool, or slow down an ingest, because it is not running in the same
system as any of those things. This is the same argument behind Snowflake and
BigQuery — separate storage from compute — applied at a scale where the compute
happens to be a 20 MB library instead of a cluster.

**Operationally it is a Python import.** No server to provision, patch, back up
or pay for; no schema migration to keep in step with the Parquet; nothing to
restore after the Container Apps Job scales to zero. For a platform whose whole
cost story (`docs/COST.md`) is "storage plus a few minutes of compute a day",
adding a managed database would be the single largest line item and would buy
nothing this workload needs.

The honest boundary: the moment this platform needs concurrent writers, row-level
updates, per-user permissions or single-row lookups at API latency, Postgres is
the right tool and DuckDB is not. None of those are warehouse-layer problems.

---

## 3. Why query Parquet in place instead of loading it

DuckDB would happily `CREATE TABLE silver AS SELECT * FROM read_parquet(...)`.
It is deliberately never done, for three reasons in increasing order of
importance.

**A loaded copy is a second source of truth.** The instant the gold flow reruns,
that table is stale, and it answers with the same confidence as before. Reading
the files means the warehouse is exactly as fresh as the last flow run, with no
refresh step anyone can forget and no window where the dashboard and the lake
disagree.

**The engine stops being disposable.** With nothing to persist, the API, a
notebook and a scheduled job each open their own connection against the same
lake — no locking, no coordination, no migration. Scaling out is starting more
processes. With a loaded copy, every one of them needs its own load, and they
drift.

**Loading throws away the file layout, which is where the performance is.**
Partition pruning and predicate pushdown are properties of *files*: the `dt=` in
the path, the row-group statistics in the footer. Copying rows into DuckDB's own
storage discards the directory structure and converts a free file-skip into a
full scan. Loading to go faster would make this query layer slower.

The cost of reading in place is that Parquet must be decompressed on every
query. At this fleet's size (~430k rows, 77 bronze files, zstd) that is
single-digit milliseconds — see §4 — and it is bounded by partitioning rather
than by total lake size, which is the property that actually matters as history
accumulates.

---

## 4. What the file layout buys, measured

All figures below are from this repo's data: 500 machines, 4 days, 429,290
bronze rows in 77 files, 426,760 silver rows in 4 files. Reproduce with
`python -m pipeline.warehouse.run_queries -q partition_pruning_demo --analyze`.

### 4.1 Partition pruning — the evidence

Bronze is written as `raw/dt=<date>/batch_*.parquet`. `dt` is not stored inside
any file; it exists only in the directory name, and `hive_partitioning=true`
tells DuckDB to read it from there and type it as a `DATE`. A predicate on `dt`
then becomes a *file filter*, applied before a single byte of Parquet is read.

With a literal predicate, pruning happens when the plan is built and `EXPLAIN`
alone shows it:

```sql
SELECT dt, count(*), avg(coolant_temp_c)
FROM bronze_telemetry
WHERE dt = DATE '2026-08-22'
GROUP BY dt;
```

```
┌───────────────────────────┐
│        READ_PARQUET       │
│    ────────────────────   │
│         Function:         │
│        READ_PARQUET       │
│                           │
│        Projections:       │
│       coolant_temp_c      │
│             dt            │
│                           │
│       File Filters:       │
│ (dt = '2026-08-22'::DATE) │
│                           │
│   Scanning Files: 10/77   │
│                           │
│        ~26,660 rows       │
└───────────────────────────┘
```

`Scanning Files: 10/77`. Sixty-seven files are never opened. `EXPLAIN ANALYZE`
confirms it after execution — `Total Files Read: 10` — and the same query with
the `WHERE` clause removed reports `Total Files Read: 77`.

Silver, one file per partition, behaves identically:

```
│       File Filters:       │
│ (dt >= '2026-08-21'::DATE)│
│                           │
│    Scanning Files: 2/4    │
```

Timings, best of nine on a warm page cache:

| Query | Files opened | Time |
| --- | --- | --- |
| `WHERE dt = DATE '2026-08-22'` | 10 of 77 | 1.0 ms |
| no predicate | 77 of 77 | 2.8 ms |

**Read that honestly.** 1.8 ms saved is nothing. What the table shows is that
cost tracks *files opened* rather than lake size, and that is the property that
matters: at three years of history the same query still opens ten files, while
the unpruned version opens twenty-seven thousand. The mechanism is proven here;
the payoff arrives with volume.

### 4.2 The catalogue query prunes at runtime, not at plan time

`partition_pruning_demo` anchors to the data instead of hardcoding a date:

```sql
WHERE dt >= (SELECT max(dt) FROM bronze_telemetry) - 1
```

DuckDB cannot fold that at plan time, so `EXPLAIN` shows no `File Filters` line.
It prunes anyway, one step later — `EXPLAIN ANALYZE` shows the scan node with a
**dynamic** filter:

```
│         TABLE_SCAN        │
│    ────────────────────   │
│         Function:         │
│        READ_PARQUET       │
│                           │
│      Dynamic Filters:     │
│   dt>='2026-08-21'::DATE  │
│                           │
│    Total Files Read: 36   │
```

36 files instead of 77, decided at runtime once the subquery produced its value.

The part that is easy to oversell, so stated plainly: **the anchor subquery
itself reports `Total Files Read: 77`.** It projects only `dt`, which comes from
the path rather than from column data, but DuckDB still opens all 77 footers to
know how many rows each file contributes. So the anchored form does one cheap
metadata pass over the whole glob and one pruned pass over the data. A
production job that already knows which date it wants should pass a literal and
get the plan-time pruning in §4.1 instead. The catalogue takes the metadata pass
deliberately, because a hardcoded date stops matching the moment the lake is
regenerated and the query would silently return nothing.

### 4.3 Projection and predicate pushdown

Two more file-level optimisations, both free consequences of Parquet being
columnar with per-row-group statistics.

**Projection pushdown** — only the columns a query names leave the disk. Same
files, same rows, different column count:

| Query over `silver_telemetry` (426,760 rows) | Time |
| --- | --- |
| `avg(coolant_temp_c)` — 1 column | 1.3 ms |
| ten aggregates over 10 columns | 8.1 ms |

Roughly linear in columns read, flat in columns ignored. A row store cannot do
this at all: the whole row comes off the page regardless.

**Predicate pushdown** — a filter on an ordinary (non-partition) column is
handed to the scan rather than applied above it, so DuckDB compares it against
each row group's min/max in the Parquet footer and skips groups that cannot
contain a match:

```sql
SELECT count(*) FROM silver_telemetry WHERE coolant_temp_c > 110;
```
```
│          Filters:         │
│     coolant_temp_c>110.0  │
```

The filter appears *inside* the scan node. 442 rows survive out of 426,760, and
the rows that never matched were never materialised.

### 4.4 Why gold is not partitioned

Deliberate, and the same reasoning read backwards. Partitioning exists to let a
reader skip files. The gold tables are small, already aggregated, and read whole
every time — a dashboard loads all of `fleet_summary_by_type`, not one day of it.
There is nothing worth skipping, so partitioning them would add path complexity
and buy zero. `pipeline/flows/gold_flow.py` makes the same argument from the
write side.

---

## 5. Pointing the same code at Azure Blob

One environment variable:

```bash
LAKE_BACKEND=azure
AZURE_STORAGE_CONNECTION_STRING=...   # a Container Apps secret, never a plain env var
AZURE_BLOB_CONTAINER=fleet-lake
```

Nothing else changes — not the view names, not a single character of
`queries.sql`. `session.py` swaps the scan target from a filesystem path to an
`az://` URL:

```
local:  /…/data/silver/dt=*/part-*.parquet
azure:  az://fleet-lake/silver/dt=*/part-*.parquet
```

This works because `pipeline/storage.py` addresses everything by *key* — a
relative POSIX path inside the lake — and writes the identical key space to both
backends. The layout on disk is byte-for-byte the layout in Blob Storage, so
developing locally is a rehearsal of the deployment rather than an approximation
of it.

Three details in the Azure path worth knowing:

- **The extension load is guarded.** `LOAD azure` is tried first and only a
  failure falls through to `INSTALL azure`, which is the call that would reach
  DuckDB's extension CDN. A local run therefore never touches the network, and
  the ETL image bakes the extension in at build time so neither does the job.
- **The credential goes in a DuckDB secret**, not the global
  `azure_storage_connection_string` setting. A secret is scoped to the
  connection and is redacted from `duckdb_settings()`, so it cannot leak into a
  query plan pasted into a ticket.
- **Pruning still works, and matters more.** Over HTTP the unit that gets
  skipped is a request, not a page-cache hit. Not opening 67 files means 67
  round trips and their egress that never happen — the same mechanism, with the
  savings multiplied by network latency.

**Untested.** The Azure branch has been written carefully against the DuckDB
azure extension's documented interface, but there were no Azure credentials
available while building this layer, so it has never actually run. Local is
verified end to end; `LAKE_BACKEND=azure` should be treated as unproven until it
is exercised against a real storage account.

---

## 6. The query catalogue

`queries.sql` is both the input to the CLI and the documentation of what this
layer is for. Each query carries a `-- name:` header the CLI splits on, plus the
business question and the layer it reads.

| Query | Layer | Question |
| --- | --- | --- |
| `top_fault_equipment` | gold | Which ten machines threw the most fault codes in the last 7 days? |
| `avg_fuel_by_type` | gold | Fuel burned per operating hour, by machine class? |
| `critical_machines` | gold | Which machines are critical right now, worst first? |
| `temp_trend_leaders` | gold | The 15 steepest rising coolant-temperature trends? |
| `fleet_utilisation` | gold | Operating hours per type per day? |
| `fault_code_pareto` | silver | Which fault codes account for most of the fault volume? |
| `data_quality_report` | silver + bronze | What did cleaning change, and does bronze reconcile to silver? |
| `partition_pruning_demo` | bronze | (the plan is the point — see §4) |

Two of them are worth calling out.

**`fault_code_pareto` reads silver on purpose.** Gold aggregates faults per
machine, not per *code*, so the code-level breakdown exists nowhere but the
cleaned readings. That is the case for keeping every layer queryable rather than
shipping only the rollups: the warehouse can answer a question no pre-built
table anticipated, without waiting for a new pipeline.

**`data_quality_report` audits the pipeline itself.** It reconciles bronze row
count against silver, attributes the difference, breaks down every repair flag,
and asserts that zero duplicate business keys survive deduplication — against
the real files, not a fixture:

```
section         metric                                        row_count  basis   pct_of_basis
--------------  --------------------------------------------  ---------  ------  ------------
reconciliation  bronze rows landed                              429,290  bronze        100.00
reconciliation  silver rows retained                            426,760  bronze         99.41
reconciliation  silver rows carrying >= 1 repair flag            22,415  silver          5.25
reconciliation  rows removed (quarantined or deduplicated)        2,530  bronze          0.59
reconciliation  duplicate business keys left in silver (mus…          0  silver          0.00
quality_flag    assumed_utc                                      21,218  silver          4.97
quality_flag    gps_null_island                                     342  silver          0.08
quality_flag    oil_pressure_psi_out_of_range                       319  silver          0.07
quality_flag    fuel_level_pct_out_of_range                         306  silver          0.07
quality_flag    coolant_temp_c_out_of_range                         299  silver          0.07
```

A row count that drops without an explanation is the most common way a data
platform lies to its users. This query is the explanation, and it is one command
away at any time.

---

## 7. Operational notes

**Every window is anchored to the data, never to `current_date`.** The lake holds
generated history; a wall-clock filter would return zero rows the day after a
backfill and every query would become a silent false negative. The flows follow
the same rule.

**Missing layers fail with a fixable message,** not a raw DuckDB IO exception:

```
ERROR: The gold layer is empty: nothing matches
'/…/data/gold/daily_equipment_summary.parquet', so the view
'daily_equipment_summary' cannot be registered.
Build it first:  python -m pipeline.flows.gold_flow
```

Callers that legitimately run before gold exists can pass
`get_warehouse(require_gold=False)` to skip those views instead.

**The silver glob is `dt=*/part-*.parquet`, not `**/*.parquet`,** and the shape
is load-bearing. The silver flow writes rejected rows to
`silver/_quarantine/dt=<date>/` — same prefix, completely different schema. A
looser glob would union quarantined rows into the clean table and nothing would
complain; DuckDB would just return a wider table full of new NULLs.

**Connections are not cached.** DuckDB connections are not thread-safe, so a
module-level singleton would turn a FastAPI worker into a queue of one. Callers
open one and close it.

**`WAREHOUSE_MEMORY_LIMIT` and `WAREHOUSE_THREADS`** are optional env vars read
straight from the environment, because they describe the machine rather than the
lake. The memory limit matters in Container Apps: DuckDB otherwise sizes its
buffer pool from total system RAM, overshoots the container's cgroup cap and
gets OOM-killed instead of spilling to disk.

**Timestamps are pinned to UTC** on every connection. Without it DuckDB renders
`TIMESTAMPTZ` in the host's local zone, and the same query prints different times
on a laptop in IST and in a container in UTC.
