# What this costs to run

**Headline: about $5.35/month, and $5.08 of that is the container registry.**

Everything else — the hourly ETL, the API, the lake, the logs — fits inside
Azure's free grants with room to spare. Delete the registry after deploying (or
push to a public Docker Hub repo instead) and the whole platform runs for
roughly **$0.25/month**.

| Line item | Monthly |
| --- | ---: |
| Azure Container Registry, Basic SKU | **$5.08** |
| Blob Storage — bytes stored | $0.01 |
| Blob Storage — transactions | ~$0.24 |
| Container Apps — ETL job compute | $0.00 *(25% of free grant)* |
| Container Apps — API compute | $0.00 *(inside the same grant)* |
| Log Analytics ingestion | $0.00 *(<1% of the 5 GB free tier)* |
| Outbound data transfer | $0.00 *(first 100 GB/month free)* |
| Vercel (frontend) | $0.00 *(Hobby tier)* |
| **Total** | **≈ $5.35** |
| **Total without ACR** | **≈ $0.25** |

Prices are US-region pay-as-you-go list prices. They vary by region — some
regions price Hot blob storage at $0.021/GB rather than $0.018 — and Azure
changes them. Check the [pricing calculator](https://azure.microsoft.com/pricing/calculator/)
before trusting these to the cent. The *shape* of the answer (one $5 line item,
everything else inside free grants) is stable; the decimals are not.

---

## How much data this actually produces

Measured, not estimated, from a real `make generate` run:

```
data/raw/  →  77 Parquet files
              429,290 rows
              12,182,547 bytes  =  11.62 MB
              500 machines × 3 simulated days × one reading / 5 min
```

That works out to:

| | |
| --- | --- |
| Bytes per row | **28.4** (Parquet + zstd) |
| Rows per day | 143,097 |
| Rows per hourly batch | 5,962 |
| **Bronze per day** | **3.87 MB** |
| **Bronze per month** | **~118 MB** |

28 bytes for a row carrying eleven sensor fields plus GPS is the columnar
format earning its keep. The same data as JSON is roughly 300 bytes a row —
about 10x — which would turn 118 MB/month into 1.2 GB/month. Still cheap, but
it is the difference between "never think about storage again" and "check the
bill occasionally".

Silver is the same order of magnitude: deduplication removes only 0.6% of rows
(2,528 of 429,290), so call it ~100 MB/month. Gold is aggregated to one row per
machine per day across three marts — a few hundred KB, forever.

**Total steady state: under 250 MB after the first month, and roughly 2.6 GB if
you let a full year accumulate without ever deleting anything.** At $0.018/GB
that year of history costs **five cents a month**. Storage is not the problem
here and never will be.

Azure also gives new accounts a 12-month free grant of 5 GB Hot LRS blob storage
plus 20,000 read and 20,000 write operations a month. If your subscription still
has that, the storage line goes to zero and the transaction line drops to near
zero too.

### Where the blob money actually goes

Not the bytes — the *transactions*. Hot LRS charges roughly $0.055 per 10,000
write operations and $0.0044 per 10,000 reads. Each ETL execution lists a
prefix, reads a partition's worth of Parquet, and writes silver and gold back.

Budgeting a pessimistic 200 operations per execution:

```
720 executions/month × 200 ops = 144,000 operations
  at a realistic 80/20 read/write mix ................ $0.21
  if every single one were a write (worst case) ...... $0.79
```

Call it **$0.25/month**, with a ceiling under a dollar even if the flows are far
chattier than expected. This is why the runbook picks the **Hot** tier: Cool and
Cold trade cheaper bytes for *more expensive transactions*, and this workload is
almost entirely transactions.

---

## Container Apps: how an hourly job stays free

Azure Container Apps includes a monthly free grant per subscription on the
Consumption plan:

- **180,000 vCPU-seconds**
- **360,000 GiB-seconds**
- 2,000,000 requests

The ETL job runs at `--cpu 0.5 --memory 1.0Gi`, hourly, 720 times a month. What
that consumes depends only on how long an execution takes:

| Execution length | vCPU-seconds | of grant | GiB-seconds | of grant |
| --- | ---: | ---: | ---: | ---: |
| 60 s | 21,600 | 12% | 43,200 | 12% |
| **90 s** (planning figure) | **32,400** | **18%** | **64,800** | **18%**  |
| 120 s | 43,200 | 24% | 86,400 | 24% |

Where 90 seconds comes from: running the built image locally, a Prefect flow
that reads all 77 bronze files and materialises 429,290 rows takes **12 seconds
wall-clock end to end**, including roughly 7 seconds for Prefect to stand up its
ephemeral server — and that measurement was taken under `linux/amd64` emulation
on an arm64 Mac, so native amd64 is faster still. Azure adds container
scheduling, an image pull on a cold node, and reading 12 MB over the network
instead of off local disk. 90 seconds is a conservative planning number with a
large margin; the honest expectation is 30-60.

The API adds a small amount on top. With `--min-replicas 0` there is no replica
between requests, so it bills only while a visitor is actually looking at the
dashboard. Budgeting 40 sessions a month, each keeping a replica alive for ~10
minutes (browsing plus the 300-second scale-down cooldown):

```
24,000 replica-seconds → 12,000 vCPU-s (7%) + 24,000 GiB-s (7%)
```

**Combined: 44,400 of 180,000 vCPU-seconds — 25% of the grant. Same 25% on
memory.** Three quarters of the allowance is untouched.

### How much headroom is that, concretely?

Cadence you could move to and still stay inside the free grant:

| Schedule | Executions/month | Grant used |
| --- | ---: | ---: |
| Hourly (`0 * * * *`) | 720 | 25% |
| Every 30 min | 1,440 | 43% |
| Every 20 min | 2,160 | 61% |
| Every 15 min | 2,880 | 79% |

You could run the pipeline **four times as often as it currently does** and
still not pay for compute. That is the margin that makes the Job-versus-App
decision worth making properly.

---

## The one line item that costs money: ACR Basic

**$0.167/day → $5.08/month.** Flat. There is no free tier for Azure Container
Registry, and Basic is the cheapest SKU. You pay the same whether you push one
image or a hundred; the price buys 10 GB of included storage, which is about
twenty times what this project needs:

```
fleet-etl:v1   ~763 MB uncompressed   (~300 MB stored, compressed)
fleet-api:v1   ~564 MB uncompressed   (~220 MB stored, compressed)
```

*(Measured from the built images. The API image is ~200 MB smaller because it
carries no Prefect and none of its forty-odd transitive dependencies — which is
the reason the two Dockerfiles exist separately rather than as one image with
two entrypoints. On a scale-to-zero app that difference is image-pull time a
real user waits through.)*

### Two ways not to pay it

**Delete the registry after deploying.** Container Apps pulls the image once and
the node caches it; deleting the registry does not stop the running job or app.

```bash
az acr delete --name "$ACR" --resource-group "$RG" --yes
```

You lose the ability to deploy a *new* version until you create a registry
again — but `az acr create` + two `az acr build`s takes about five minutes, and
a portfolio project does not redeploy daily. This drops the total from $5.35 to
$0.25.

**Or push to Docker Hub instead.** A public repository is free and unlimited for
public images, and Container Apps pulls from it with no registry credentials at
all (drop `--registry-server` / `--registry-username` / `--registry-password`
and use `docker.io/<user>/fleet-etl:v1` as the image). The catch: the images are
public and Docker Hub rate-limits anonymous pulls. Neither matters for images
that contain no secrets and are pulled a handful of times a month. It does mean
building locally with `docker build --platform linux/amd64` — see the Apple
Silicon trap in `docs/DEPLOYMENT.md`.

GitHub Container Registry (`ghcr.io`) is a third option, also free for public
images, and is a natural fit if the repo already lives on GitHub.

---

## The three things that would blow this budget

Ranked by how much money they cost and how easy they are to do by accident.

### 1. Setting the API's `--min-replicas` to 1 — $10 to $34/month

This is the big one, and it is a single flag.

A replica that exists for a whole month exists for 2,628,000 seconds. At 0.5
vCPU that is 1,314,000 vCPU-seconds against a 180,000-second grant — **seven
times the entire monthly allowance**, before the ETL job has run once.

```
idle-rate billing  ($0.000003/vCPU-s):  $3.40 cpu + $6.80 mem = $10.21/mo
active-rate billing ($0.000024/vCPU-s): $27.22 cpu + $6.80 mem = $34.02/mo
```

Which rate applies depends on whether Azure classifies the replica as idle or
active, and anything that touches it regularly — a health probe, a monitoring
check, a crawler — pushes it toward active. So the honest answer is a range:
**$10-34/month, i.e. two to seven times the cost of everything else in this
project combined.**

And the damage is not confined to the API. Blowing through the shared free grant
means the ETL job's 44,400 vCPU-seconds, which were free, start billing too.

The temptation is real: `--min-replicas 0` means the first request after an idle
period takes 2-15 seconds. The fix is a loading state in the dashboard, not a
flag. And specifically **not** a cron job that pings the API every five minutes
to keep it warm — that is `--min-replicas 1` with extra steps and an identical
bill.

### 2. Making the cron more frequent than the work justifies — up to $53/month

`--cron-expression` is five fields of plain text with no guardrail. Changing
`"0 * * * *"` to `"* * * * *"` is one character and sixty times the executions:

```
43,200 executions × 90 s × 0.5 vCPU = 1,956,000 vCPU-s  (1,087% of grant)
                                    → $42.34 cpu + $10.58 mem = $52.92/mo
```

Worse, it is silent. Nothing warns you; the job just runs, the executions list
fills up, and the bill arrives four weeks later. The pipeline processes telemetry
that arrives every five minutes into daily aggregates — there is no analytical
question this project answers that gets a better answer from a minutely rebuild.

Related and sneakier: **dropping `--parallelism 1`**. If an execution takes
longer than the gap between schedule ticks, runs start overlapping, and the
overlap compounds — you get N concurrent replicas fighting over the same
partition and billing N times over.

### 3. A hung execution with a generous (or missing) `--replica-timeout` — $14 to $228/month

The failure mode nobody plans for. A flow that blocks on a blob call that never
returns does not crash and does not log; it just sits there, consuming its full
CPU and memory allocation, until something stops it. `--replica-timeout` is that
something.

| Scenario | vCPU-s/month | of grant | Cost |
| --- | ---: | ---: | ---: |
| Healthy: 90 s per run | 44,400 | 25% | $0.00 |
| Hangs, killed at `--replica-timeout 1800` | 660,000 | 367% | **$14.40** |
| Hangs, no timeout, noticed after 6 h | 7,788,000 | 4,327% | **$228.24** |

A 30-minute timeout on a job whose realistic worst case is two minutes looks
generous, and it is — deliberately. It is not a performance target, it is a
**blast-radius cap**: it converts an unbounded runaway into a bounded $14
mistake that also shows up as a `Failed` execution you can actually see. Setting
it to 30 seconds to "save money" instead turns every slow-but-fine run into a
false alarm.

Set it, keep `--replica-retry-limit` low (a retry multiplies a runaway, it does
not fix one), and put a budget alert on the subscription so a runaway announces
itself in days rather than at the end of the month. The Portal path is
**Cost Management + Billing → Budgets → Add**; scope it to the resource group,
set the amount to $10, and add an alert at 50%. (The CLI equivalent lives under
`az consumption budget`, but its required arguments have changed across CLI
versions — the Portal form is the stable way to do this once.)

### Honourable mentions

Things that cost real money but are harder to do by accident:

- **`--sku Premium` on the registry** — $1.667/day, **$50/month**, ten times
  Basic, for geo-replication and private endpoints this project has no use for.
- **`Standard_GRS` instead of `Standard_LRS`** on the storage account — roughly
  double the per-GB price for cross-region durability on a dataset that
  `make generate` rebuilds deterministically in ninety seconds.
- **Verbose logging.** Log Analytics is free to 5 GB/month, then ~$2.76/GB. This
  project emits single-digit MB. Turning on debug logging in a flow that
  processes 143,000 rows a day, one log line per row, is how a free workspace
  becomes a $30 one.
- **Oversizing the job.** `--cpu 2 --memory 4.0Gi` is 4x the burn per second for
  identical work on 12 MB of Parquet. On its own at hourly cadence it still just
  fits (79% of grant vs 25%) and bills $0 — but it eats the entire margin, so
  the *next* mistake, which would have been absorbed, becomes a bill instead.
- **Forgetting `az group delete`.** The $5.08 registry charge does not stop
  because you stopped looking at the project. If you are done, tear it down;
  step 11 of `docs/DEPLOYMENT.md` is one command.

---

## Why this is cheap: the architecture is the cost control

Almost none of the above is a billing trick. The bill is low because of design
decisions made for other reasons that happen to also be the cheap ones:

- **A Container Apps Job, not a Container App.** A job that runs 90 seconds an
  hour bills for 64,800 seconds a month. The same work wrapped in a service
  bills for 2,628,000. Same schedule, same computation, **40x** the cost. This is
  the single biggest decision in the deployment and it is the first thing
  `docs/DEPLOYMENT.md` argues for.
- **Scale to zero on the API.** A read-only dashboard nobody is looking at
  should cost nothing, and with `--min-replicas 0` it does.
- **Parquet with zstd.** 28 bytes a row instead of ~300 as JSON. It was chosen
  for query speed and columnar pruning; a 10x storage reduction came free.
- **Static frontend on a CDN.** No compute at all, and Vercel's free tier
  covers it.
- **The lake is reproducible.** Because bronze regenerates deterministically
  from `GENERATOR_SEED` and silver and gold are pure functions of bronze, there
  is nothing here worth paying for geo-redundancy, backups, or snapshots to
  protect. That is what lets `Standard_LRS` be the right answer rather than the
  cheap answer.
