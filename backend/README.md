# Fleet Telemetry API

Read-only FastAPI service over the **gold** layer of the lakehouse. It answers
the five questions the React dashboard asks and nothing else: no writes, no
auth, no database of its own.

The service is a thin shell around DuckDB. Every number it returns — health
scores, temperature trends, fuel burn, per-class rollups — was computed once by
the Prefect gold flow and written to Parquet. The API selects, filters, joins,
orders and limits; it never averages, scores or derives. If a change would make
this server compute a new statistic, that logic belongs in `pipeline/transforms/`
and `pipeline/flows/gold_flow.py`, where the warehouse, a notebook and a BI tool
all get it too — instead of only this process, recomputing it on every request.

## Run it

The pipeline must have produced the gold marts first:

```bash
make etl                 # or: ./.venv/bin/python -m pipeline.flows.etl_flow
```

Then, from the repo root:

```bash
make backend             # uvicorn with autoreload on :8000
```

or directly:

```bash
./.venv/bin/python -m uvicorn backend.app.main:app --reload --port 8000
```

Interactive docs at <http://localhost:8000/docs>; the generated OpenAPI schema
is the machine-readable form of the contract below.

The API starts even when gold is missing — see *Degraded start*.

### In a container

```bash
make docker-backend      # builds Dockerfile.backend
docker run --rm -p 8000:8000 -v "$PWD/data:/app/data" fleet-api:local
```

The image installs only `backend/requirements.txt`, so it has no Prefect in it.

## Endpoints

All responses are JSON. Every `NaN`/`NaT` serialises as `null`, every timestamp
as ISO-8601 UTC (`2026-08-22T10:30:16Z`), every date as `YYYY-MM-DD`.

| Method | Path | Query params | Returns |
| --- | --- | --- | --- |
| GET | `/api/health` | — | `{status, lake_backend, gold_tables}` — liveness probe |
| GET | `/api/fleet-summary` | — | fleet-wide counters + `by_type[]` breakdown |
| GET | `/api/health-flags` | `equipment_type`, `severity` (`warning\|critical`), `limit` (200) | `{count, items[]}` — every column of `fleet_health_flags`, worst first |
| GET | `/api/equipment` | `equipment_type`, `flagged_only` (false), `limit` (500) | `{count, items[]}` — the roster, with `severity`/`health_score` null when healthy |
| GET | `/api/equipment/{equipment_id}/history` | `days` (30) | `{equipment_id, equipment_type, days[], flag}`; **404** if the machine has no gold rows |

Notes on the contract's less obvious corners:

- `count` is the number of items in *this* response, not the size of the
  unfiltered match. The client already knows how many rows it got; a second
  `COUNT(*)` round trip to repeat it would be waste.
- `limit` truncates the **least urgent** rows. `/api/health-flags` orders
  critical-first then by ascending health score, so what falls off the end is
  always what a planner cares least about.
- `severity` and `health_score` being `null` in `/api/equipment` is a LEFT JOIN
  miss, not a missing value: gold's `fleet_health_flags` contains only flagged
  machines, so "no row" *is* the healthy answer.
- `days=30` means the last 30 calendar days of the **lake**, not the last 30 days
  this machine reported. A unit that went quiet a month ago returns a short
  series rather than a full-looking chart of stale readings.
- An unknown machine is a 404; a known machine with nothing inside the window is
  a 200 with an empty `days`. Those are different facts.
- `equipment_type` is matched case-insensitively against silver's canonical
  lowercase form, so a hand-typed `?equipment_type=Excavator` finds rows instead
  of returning an empty list that reads like "no excavators are flagged".

## Configuration

Everything comes from the environment; there are no paths or credentials in the
code. Lake settings are shared with the pipeline (see `.env.example` at the repo
root) — the API only adds these:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:3000` | Comma-separated browser origin allow-list. Empty entries are ignored, so a trailing comma is harmless. |
| `PORT` | `8000` | Listen port. Azure Container Apps injects this and routes ingress to it, which is why nothing hardcodes 8000. |
| `LOG_LEVEL` | `INFO` | Root log level. |
| `WAREHOUSE_MEMORY_LIMIT` | unset | Passed through to DuckDB by `pipeline.warehouse.session`. Worth setting in a container: DuckDB otherwise sizes its buffer pool from host RAM, overshoots the cgroup cap and gets OOM-killed instead of spilling. |

## Design decisions

### One connection, one cursor per request

`get_warehouse()` registers five views, each a `read_parquet()` glob over the
lake. That glob is the expensive part, so opening a warehouse per request is out:
it would re-walk the lake on every call, and once `LAKE_BACKEND=azure` that walk
is a network listing.

So the connection is opened once in the app's async lifespan handler and closed
on shutdown. Concurrency is then handled with `connection.cursor()` per request
rather than a lock around a shared connection. A cursor is a new connection to
the *same* database instance: it sees the views the root connection registered —
no re-globbing, no second copy — while getting its own execution context, which
is what makes parallel use safe. The alternative, serialising every query behind
a mutex, would make the API a queue of one.

Measured: 200 requests across 32 threads, all 200s, 4.6 MB of JSON, 0.21 s.

One trap this uncovered: **a cursor does not inherit the root connection's
session settings.** `get_warehouse` runs `SET TimeZone='UTC'`, but a fresh cursor
falls back to the host zone, so on a laptop in IST the same `TIMESTAMPTZ` renders
`+05:30`. The instant is unchanged and the serialiser normalises to UTC anyway,
but each cursor re-pins the zone so that a query run from here and the same query
run through `make queries` cannot print different-looking times.

### NaN is not JSON

`json.dumps` writes `float('nan')` as the bare token `NaN` and `float('inf')` as
`Infinity`. Neither is valid JSON, and `JSON.parse` throws on both — so one
unlucky division in the gold layer would blank the entire dashboard rather than
one cell.

Two things prevent it, both central rather than per-field:

1. Rows are read with `fetchall()`, not `.df()`. A DataFrame collapses SQL NULL
   and IEEE NaN into the same `float('nan')`, manufacturing the problem on the
   way in; DuckDB's native conversion keeps NULL as `None`.
2. `deps._jsonable()` runs over every value of every row and maps any non-finite
   float to `null`, dates to `YYYY-MM-DD` and timestamps to UTC ISO-8601 with a
   `Z`. It is one function so that the column nobody thought about is covered.

### Degraded start

`/api/health` returns 200 even when the lake is empty, reporting which gold
tables exist. This is deliberate: on Container Apps a non-200 marks the revision
unhealthy and restarts it, so an API that refused to start because the ETL had
not run yet would be killed in a loop while the real fix ("run the ETL") reached
nobody. `status` is `"ok"` when the warehouse is open and all three marts exist,
`"degraded"` otherwise, and the data routes answer 503 with the command to run.

### Known limitation: the API cannot run on a gold-only lake

`pipeline.warehouse.session.register_views` hard-requires the **bronze and
silver** layers — `require_gold=False` relaxes only the gold views. An API
deployment given just the three gold Parquet files therefore fails to open a
warehouse at all: `/api/health` correctly reports all three tables present, and
every data route still returns 503. That matters for the Azure shape where the
ETL Job writes the whole lake but the API container only needs the marts.

The fix belongs in the warehouse module (a layer selector, so a caller can ask
for gold views alone), not here. Until then, the API container needs read access
to the full lake prefix, not just `gold/`.
