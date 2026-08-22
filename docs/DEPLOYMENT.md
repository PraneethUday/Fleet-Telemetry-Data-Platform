# Deploying to Azure

A numbered runbook. Follow it top to bottom in one shell session — every step
reuses the variables defined in step 2.

**How to read the cost markers.** Every command that creates a resource capable
of generating a bill is tagged inline:

| Marker | Meaning |
| --- | --- |
| `[FREE]` | Creates no billable resource, or is a read-only query. |
| `[BILLABLE — pennies]` | Real but rounding-error money at this project's scale. |
| `[BILLABLE — ~$5/mo]` | The one line item that costs actual money. |

Prices quoted here are US-region pay-as-you-go list prices and vary by region.
`docs/COST.md` has the full arithmetic and the headline monthly figure.

---

## 1. Prerequisites

You need the Azure CLI and an Azure subscription. Nothing else — in particular
you do **not** need Docker running locally, because step 5 builds the images
server-side.

```bash
# Sign in. Use --use-device-code if the browser handoff fails (common over SSH).
az login

# Pin the subscription explicitly. If you have more than one, the CLI's default
# is whichever came back first, and deploying into the wrong one is easy.
az account set --subscription "<YOUR SUBSCRIPTION NAME OR ID>"
az account show --output table
```
`[FREE]`

The Container Apps commands live in an extension that is not installed by
default:

```bash
az extension add --name containerapp --upgrade
```
`[FREE]`

Azure subscriptions do not have every resource provider enabled up front. If
you skip this, `az containerapp env create` fails several minutes in with
`MissingSubscriptionRegistration`, which is an unpleasant way to learn about it.
`--wait` blocks until registration finishes (typically under two minutes).

```bash
az provider register --namespace Microsoft.App               --wait
az provider register --namespace Microsoft.OperationalInsights --wait
az provider register --namespace Microsoft.ContainerRegistry --wait
az provider register --namespace Microsoft.Storage           --wait
```
`[FREE]` — registering a provider does not create anything.

---

## 2. Variables

Set these once. Every later command references them, so the rest of this
document is genuinely copy-pasteable rather than "copy-paste and then edit".

```bash
# A random suffix, evaluated ONCE, because storage account and registry names
# are globally unique across all of Azure — "stfleet" was taken years ago.
SUFFIX=$(openssl rand -hex 3)

RG="rg-fleet-telemetry"
LOCATION="eastus"

# Storage account: 3-24 chars, lowercase letters and digits ONLY. No hyphens.
STORAGE="stfleet${SUFFIX}"
CONTAINER="fleet-lake"

# Container registry: 5-50 chars, alphanumeric ONLY. No hyphens either.
ACR="acrfleet${SUFFIX}"

CAE="cae-fleet-telemetry"     # Container Apps environment
JOB="fleet-etl-job"           # the scheduled ETL
APP="fleet-api"               # the FastAPI backend

# Print and save these. If your shell dies you cannot re-derive $SUFFIX.
cat <<EOF
RG=$RG  LOCATION=$LOCATION
STORAGE=$STORAGE  CONTAINER=$CONTAINER
ACR=$ACR  CAE=$CAE  JOB=$JOB  APP=$APP
EOF
```
`[FREE]`

Pick a `LOCATION` near you that supports Container Apps. `az containerapp
list-usages --location eastus` failing with an unsupported-location error is
the fastest way to find out you picked a region that does not offer it.

---

## 3. Resource group and the lake's storage account

The resource group is just a label. It creates nothing and costs nothing — but
it is also the *delete handle* for the entire project (step 11), so put
everything in it.

```bash
az group create --name "$RG" --location "$LOCATION" --output table
```
`[FREE]`

Now the storage account that holds the bronze/silver/gold lake:

```bash
az storage account create \
  --name "$STORAGE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --kind StorageV2 \
  --sku Standard_LRS \
  --access-tier Hot \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --output table
```
`[BILLABLE — pennies]` This pipeline produces roughly **3.9 MB of Parquet per
simulated day**, so about 120 MB/month of bronze. At Hot LRS list price that is
well under one cent per month of storage; the transaction charges dominate and
still land around $0.25/month. See `docs/COST.md`.

**Why `Standard_LRS` and not ZRS/GRS.** LRS keeps three replicas inside one
datacentre. GRS adds a second region and roughly doubles the bill for a dataset
that is *fully reproducible*: `make generate` rebuilds the entire bronze layer
deterministically from `GENERATOR_SEED`, and silver and gold are pure functions
of bronze. Paying for cross-region durability to protect data you can recreate
in ninety seconds is buying insurance on a photocopy.

**Why the Hot tier.** Cool and Cold tiers trade a lower per-GB storage price for
a *higher* per-transaction price and an early-deletion penalty. This workload is
the exact inverse of their target: a few hundred MB (so the per-GB saving is
worth fractions of a cent) read and rewritten every single hour (so the
transaction premium is charged 720 times a month). Hot is cheaper here, not
just simpler.

**Why `--allow-blob-public-access false`.** The lake is reached through the
connection string only. Nothing in this project needs anonymous blob reads, and
leaving public access on is how storage accounts end up indexed.

Grab the connection string and create the container. The connection string is a
credential — it goes into a shell variable and then into a Container Apps
secret, and never into a file that git can see.

```bash
STORAGE_CONN=$(az storage account show-connection-string \
  --name "$STORAGE" \
  --resource-group "$RG" \
  --query connectionString \
  --output tsv)

az storage container create \
  --name "$CONTAINER" \
  --account-name "$STORAGE" \
  --connection-string "$STORAGE_CONN" \
  --output table
```
`[FREE]` — a container is a namespace; you pay for the bytes in it, not for it.

---

## 4. Container registry and the two images

```bash
az acr create \
  --name "$ACR" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Basic \
  --admin-enabled true \
  --output table

ACR_LOGIN=$(az acr show --name "$ACR" --query loginServer --output tsv)
```
`[BILLABLE — ~$5/mo]` **This is the only resource in the project that costs
real money.** ACR Basic is a flat $0.167/day (~$5.08/month) whether you push one
image or a hundred; there is no free tier. It includes 10 GB of storage, which
is roughly twenty times what these two images need. `docs/COST.md` covers the
two ways to avoid it (Docker Hub public repo, or delete the registry after
deployment — Container Apps caches the pulled image and keeps running).

`--admin-enabled true` turns on a username/password pair so Container Apps can
pull. It is the simplest thing that works, and it is what steps 6 and 8 use. In
a real production setup you would instead grant the app a managed identity the
`AcrPull` role and pass `--registry-identity system`, which avoids a shared
credential entirely.

Now build both images **in Azure**, from this repo's working directory:

```bash
cd /path/to/Fleet-Telemetry-Data-Platform   # the directory containing Dockerfile.etl

az acr build \
  --registry "$ACR" \
  --image fleet-etl:v1 \
  --file Dockerfile.etl \
  --platform linux/amd64 \
  .

az acr build \
  --registry "$ACR" \
  --image fleet-api:v1 \
  --file Dockerfile.backend \
  --platform linux/amd64 \
  .
```
`[BILLABLE — pennies]` ACR Tasks builds are billed per CPU-second and the first
100 minutes per month are included in the Basic SKU. Two builds of this size use
a handful of minutes.

### The Apple Silicon trap — read this one

`az acr build` uploads the build context and builds it on an Azure-hosted
agent. Two things follow, and the second one bites people:

1. **You do not need a local Docker daemon.** The build runs server-side. This
   is why step 1 does not ask you to install Docker.

2. **On an M-series Mac, `docker build && docker push` produces an image that
   Container Apps cannot run.** A local build defaults to your host
   architecture, `linux/arm64`. Azure Container Apps runs `linux/amd64` only.
   The push succeeds, the deployment succeeds, the revision reports healthy —
   and then every execution dies instantly with `exec format error` or, worse,
   a bare non-zero exit with nothing useful in the logs. Nothing in the
   deployment path warns you, because as far as Azure is concerned you asked
   for exactly what you got.

   `az acr build --platform linux/amd64` sidesteps this by building on an amd64
   agent in the first place. If you *do* want to build locally, the equivalent
   is `docker build --platform linux/amd64` (which is what `make docker-etl`
   does) — it works, but it runs the whole install under emulation and is
   several times slower.

Confirm both images landed:

```bash
az acr repository list --name "$ACR" --output table
az acr repository show-tags --name "$ACR" --repository fleet-etl --output table
az acr repository show-tags --name "$ACR" --repository fleet-api --output table
```
`[FREE]`

---

## 5. Container Apps environment

The environment is the shared network and logging boundary that the job and the
app both live in. Creating it takes two to three minutes.

```bash
az containerapp env create \
  --name "$CAE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --enable-workload-profiles true \
  --logs-destination log-analytics \
  --output table
```
`[FREE to create]` The environment itself has no standing charge — you pay for
replica time inside it, which is what steps 6 and 8 control.

`--enable-workload-profiles true` gives the environment a **Consumption**
profile, which is the serverless, scale-to-zero, billed-per-second one. It is
the default profile, so steps 6 and 8 do not need to name it. The alternative
(Dedicated profiles) reserves capacity and bills continuously — exactly what
this project is built to avoid.

`--logs-destination log-analytics` auto-creates a Log Analytics workspace in the
resource group. That workspace is what makes `az containerapp job logs show`
able to retrieve logs from an execution that has already finished — which is
the only way to debug a job, since by definition it is not running when you go
looking. Log Analytics includes 5 GB/month of free ingestion; this project
produces single-digit MB. Passing `--logs-destination none` removes even that
exposure, at the cost of losing historical logs entirely. Keep the workspace.

---

## 6. The ETL as a Container Apps **Job**

```bash
ACR_USER=$(az acr credential show --name "$ACR" --query username --output tsv)
ACR_PASS=$(az acr credential show --name "$ACR" --query "passwords[0].value" --output tsv)

az containerapp job create \
  --name "$JOB" \
  --resource-group "$RG" \
  --environment "$CAE" \
  --image "${ACR_LOGIN}/fleet-etl:v1" \
  --trigger-type Schedule \
  --cron-expression "0 * * * *" \
  --replica-timeout 1800 \
  --replica-retry-limit 1 \
  --replica-completion-count 1 \
  --parallelism 1 \
  --cpu 0.5 \
  --memory 1.0Gi \
  --registry-server "$ACR_LOGIN" \
  --registry-username "$ACR_USER" \
  --registry-password "$ACR_PASS" \
  --secrets "storage-conn=$STORAGE_CONN" \
  --env-vars \
      "LAKE_BACKEND=azure" \
      "AZURE_STORAGE_CONNECTION_STRING=secretref:storage-conn" \
      "AZURE_BLOB_CONTAINER=$CONTAINER" \
      "BRONZE_PREFIX=raw" \
      "SILVER_PREFIX=silver" \
      "GOLD_PREFIX=gold" \
      "FLEET_SIZE=500" \
  --output table
```
`[BILLABLE — $0 in practice]` A Job bills only for execution seconds. At 0.5
vCPU and ~90 seconds an execution, 720 executions a month consume roughly 18% of
the Container Apps monthly free grant. See `docs/COST.md` for the arithmetic.

### Why a Job and not a Container App — the decision that makes this free

This is the single most consequential choice in the deployment.

An **Azure Container App** is a long-running service. Even with no traffic, if
`--min-replicas` is 1 the replica exists for all 2.6 million seconds in a month
and you are billed for every one of them. At 0.5 vCPU / 1 GiB that is on the
order of **$10-34/month** depending on whether the platform bills those seconds
at the idle rate or the active rate — and, just as importantly, it consumes the
entire monthly free vCPU-second grant several times over, so everything else in
the subscription starts billing too.

An **Azure Container Apps Job** has no steady state. The platform holds a cron
schedule; at each tick it starts a replica, runs the container to completion,
records the exit code, and destroys the replica. Between ticks there is nothing
running and nothing to bill. An hourly job that works for ninety seconds is
billed for 90 × 720 = 64,800 seconds a month instead of 2,628,000 — a 40x
reduction for identical work.

The workload has to actually *terminate* for this to hold, which is why
`Dockerfile.etl` runs `python -m pipeline.flows.etl_flow` and that flow returns
rather than looping. A batch pipeline is the natural shape for this; wrapping it
in a web server so it could be "a service" would cost forty times more to do
exactly the same thing on exactly the same schedule.

### The other flags

- `--cron-expression "0 * * * *"` — top of every hour, **in UTC**. Container
  Apps cron is always UTC; there is no timezone field. Five fields, not six —
  there is no seconds column.
- `--replica-timeout 1800` — hard kill after 30 minutes. This is a circuit
  breaker, not a target. A run that hangs on a network call would otherwise
  bill until someone noticed; with a timeout the worst case is bounded and the
  execution is marked Failed so you can see it.
- `--replica-retry-limit 1` — one retry. The flows are idempotent (they rewrite
  a partition rather than appending to it), so a retry after a transient blob
  timeout is safe. Retrying more than once mostly just multiplies the cost of a
  genuine bug.
- `--parallelism 1` and `--replica-completion-count 1` — one replica, and it has
  to succeed for the execution to count as successful. Without `--parallelism 1`
  a backlog of schedule ticks can start overlapping runs that fight over the
  same partition.
- `--cpu 0.5 --memory 1.0Gi` — Container Apps only accepts certain cpu/memory
  pairings, and 0.5 vCPU pairs with 1.0 GiB. The whole three-day bronze set is
  12 MB of Parquet; it fits in a fraction of this. Anything larger burns free
  grant for no benefit.

### Optional: a second job to keep bronze flowing

The ETL transforms whatever bronze it finds. In a real deployment something has
to *produce* bronze. The same image already contains the generator, so a second
job is a command override rather than a second build:

```bash
az containerapp job create \
  --name "fleet-generator-job" \
  --resource-group "$RG" \
  --environment "$CAE" \
  --image "${ACR_LOGIN}/fleet-etl:v1" \
  --trigger-type Schedule \
  --cron-expression "50 * * * *" \
  --replica-timeout 600 \
  --replica-retry-limit 1 \
  --cpu 0.5 --memory 1.0Gi \
  --registry-server "$ACR_LOGIN" \
  --registry-username "$ACR_USER" \
  --registry-password "$ACR_PASS" \
  --secrets "storage-conn=$STORAGE_CONN" \
  --env-vars \
      "LAKE_BACKEND=azure" \
      "AZURE_STORAGE_CONNECTION_STRING=secretref:storage-conn" \
      "AZURE_BLOB_CONTAINER=$CONTAINER" \
  --command "python" \
  --args "-m" "pipeline.generator.run_generator" "--hours" "1" \
  --output table
```
`[BILLABLE — $0 in practice]` It runs for a couple of seconds. Note the cron is
`50 * * * *` — ten minutes *before* the ETL, so each hour's readings exist
before the transform goes looking for them.

---

## 7. Secrets: `secretref:`, never plaintext

Step 6 already does this correctly. It is worth spelling out because getting it
wrong is invisible.

Two flags do the work:

```bash
  --secrets "storage-conn=$STORAGE_CONN" \
  --env-vars "AZURE_STORAGE_CONNECTION_STRING=secretref:storage-conn"
```

`--secrets` registers a named secret on the job. `--env-vars` then *references*
it with the literal prefix `secretref:` followed by the secret's name. At
runtime Container Apps resolves the reference and the container sees an ordinary
environment variable — `pipeline/config.py` reads
`AZURE_STORAGE_CONNECTION_STRING` and neither knows nor cares where it came
from.

Secret names must be lowercase alphanumerics and hyphens (`storage-conn` is
fine, `STORAGE_CONN` is rejected).

**Why not just `--env-vars "AZURE_STORAGE_CONNECTION_STRING=$STORAGE_CONN"`?**
It would work. It would also be wrong, for reasons that only surface later:

- A plaintext env var is stored in the resource's ARM definition and is echoed
  back in full by `az containerapp job show`, by the Portal's overview blade,
  by `az resource list`, and by anything with Reader on the resource group.
  Reader is a role people hand out casually. A storage connection string is a
  full-access credential — read, write, and delete on every container in the
  account.
- It leaks into support bundles, ARM exports, and any `--output json` a
  colleague pastes into a chat window while asking for help.
- Rotating it means editing the deployment. With a secret, `az containerapp job
  secret set` updates the value and the next execution picks it up.

`secretref:` costs one extra flag and removes an entire category of accident.
Verify that only the names are visible:

```bash
az containerapp job secret list --name "$JOB" --resource-group "$RG" --output table
```
`[FREE]` — this prints secret *names*. Values require an explicit
`--show-values` on the equivalent app command, which is the point.

---

## 8. The FastAPI backend as a Container App

```bash
az containerapp create \
  --name "$APP" \
  --resource-group "$RG" \
  --environment "$CAE" \
  --image "${ACR_LOGIN}/fleet-api:v1" \
  --ingress external \
  --target-port 8000 \
  --min-replicas 0 \
  --max-replicas 1 \
  --cpu 0.5 \
  --memory 1.0Gi \
  --registry-server "$ACR_LOGIN" \
  --registry-username "$ACR_USER" \
  --registry-password "$ACR_PASS" \
  --secrets "storage-conn=$STORAGE_CONN" \
  --env-vars \
      "PORT=8000" \
      "LAKE_BACKEND=azure" \
      "AZURE_STORAGE_CONNECTION_STRING=secretref:storage-conn" \
      "AZURE_BLOB_CONTAINER=$CONTAINER" \
      "GOLD_PREFIX=gold" \
      "CORS_ORIGINS=http://localhost:5173" \
  --output table

API_FQDN=$(az containerapp show \
  --name "$APP" \
  --resource-group "$RG" \
  --query properties.configuration.ingress.fqdn \
  --output tsv)

echo "https://${API_FQDN}"
```
`[BILLABLE — $0 while idle]` With `--min-replicas 0` there is no replica between
requests and therefore nothing to bill. Ingress and the TLS certificate are
included.

**`PORT` and `--target-port` must agree.** `Dockerfile.backend` binds
`${PORT:-8000}` rather than a literal, so the container follows whatever the
environment says. Setting `PORT=8000` explicitly alongside `--target-port 8000`
makes the contract visible in one place instead of relying on the default. If
you change one, change both — a mismatch presents as ingress returning 502 with
a perfectly healthy container behind it.

### Scale-to-zero: the honest trade-off

`--min-replicas 0` is what makes the API free, and it is not free of
consequences. After an idle period there is no replica. The first request has to
wait for Azure to schedule a replica, pull the image if it is not cached on the
node, start the Python interpreter, import FastAPI and the pipeline package, and
run application startup. **In practice that first request takes somewhere
between two and fifteen seconds**, and if a client has a short timeout it will
simply fail. Subsequent requests are normal — single-digit milliseconds — until
the app scales back to zero after its cooldown (300 seconds by default).

For a portfolio dashboard this is the right call: the alternative costs
$10-34/month to make one request per visit faster. But do not pretend it away:

- Have the dashboard show a loading state that tolerates a slow first call
  rather than a spinner that gives up at three seconds.
- If a cold start is genuinely unacceptable, `--min-replicas 1` is the fix and
  you should read `docs/COST.md` first, because it is the single biggest way to
  blow this budget.
- Do not "solve" it with a cron job that pings the API every five minutes. That
  keeps a replica alive continuously, which is `--min-replicas 1` with extra
  steps and the same bill.

`--max-replicas 1` caps the blast radius. One replica saturates long before
this dataset does, and an accidental scale-out (a crawler, a load test) would
otherwise multiply the burn against a shared free grant.

---

## 9. Frontend

The dashboard is a Vite build — static HTML, JS and CSS with no server of its
own. Two reasonable homes.

### Option A — Vercel (recommended)

Free for this, and it is how the rest of this repo's author's projects deploy,
so there is one fewer thing to learn.

1. Import the repository at <https://vercel.com/new>.
2. **Root Directory:** `frontend`
3. **Framework Preset:** Vite. Build command `npm run build`, output `dist`.
4. Under **Environment Variables**, add:

   ```
   VITE_API_BASE_URL = https://<paste the $API_FQDN from step 8>
   ```

   Include the `https://` scheme and no trailing slash.

**Vite inlines `VITE_*` variables at build time.** They are not read at runtime.
Changing `VITE_API_BASE_URL` in the Vercel dashboard does nothing until you
redeploy — the old URL is already compiled into the JavaScript bundle. Always
change the variable *and* trigger a rebuild.

Then point the backend's CORS at the new domain. The browser enforces this, so
without it the dashboard loads, fires its first request, and shows a network
error with a CORS message in the console while the API itself is perfectly fine:

```bash
az containerapp update \
  --name "$APP" \
  --resource-group "$RG" \
  --set-env-vars "CORS_ORIGINS=https://<your-project>.vercel.app,http://localhost:5173" \
  --output table
```
`[FREE]` — an env-var update creates a new revision; it does not add a resource.

Keep `http://localhost:5173` in the list so local development against the
deployed API keeps working.

**Preview deployments will trip you up.** Vercel gives every branch and every
commit its own URL like `fleet-dashboard-git-feat-x-you.vercel.app`. Those are
*different origins* and none of them are in `CORS_ORIGINS`, so previews break
while production works. Either add the stable
`https://<project>-<scope>.vercel.app` alias, or have the backend accept a
regex for `*.vercel.app` in non-production, or accept that only production
talks to the API.

### Option B — a second Container App

Keeps everything in one cloud and one resource group. Costs a little (a second
scale-to-zero app draws from the same free grant) and needs an image: a
multi-stage Dockerfile that runs `npm run build` and copies `dist/` into an
`nginx:alpine`, with `--target-port 80`, `--min-replicas 0 --max-replicas 1`,
and `--build-arg VITE_API_BASE_URL=...` baked in at build time for the same
inlining reason as above.

This project does not ship that Dockerfile, because paying compute to serve
static files that a CDN serves for free is the wrong trade. Option A is the
recommendation.

---

## 10. Verify

### Trigger the ETL manually — do not wait for the cron

The first run is where mistakes surface (wrong architecture, bad secret,
container missing). Waiting up to an hour to find out is a waste of an hour.

```bash
az containerapp job start --name "$JOB" --resource-group "$RG" --output table
```
`[BILLABLE — seconds]` One execution's worth of compute.

### Watch the executions

```bash
az containerapp job execution list \
  --name "$JOB" \
  --resource-group "$RG" \
  --output table
```
`[FREE]`

You are looking at the `Status` column: `Running`, then `Succeeded` or `Failed`.
`Succeeded` means the container exited 0, which is exactly what
`pipeline.flows.etl_flow` does on a clean run.

Capture the most recent execution's name for the log query:

```bash
EXEC=$(az containerapp job execution list \
  --name "$JOB" \
  --resource-group "$RG" \
  --query "sort_by([], &properties.startTime)[-1].name" \
  --output tsv)
echo "$EXEC"
```

### Read the job's logs

```bash
az containerapp job logs show \
  --name "$JOB" \
  --resource-group "$RG" \
  --container "$JOB" \
  --execution "$EXEC" \
  --tail 200
```
`[FREE]`

`--container` is required and defaults to the job's own name, because a job can
define several containers in one replica. This query goes to the Log Analytics
workspace from step 5 — which is why step 5 keeps it. Logs take a minute or two
to be ingested after an execution ends, so an empty result immediately after a
run usually means "wait", not "nothing was logged".

Add `--follow` to stream a currently-running execution instead.

### Confirm data actually landed in the lake

The job reporting `Succeeded` only proves the process exited 0. Check the blobs:

```bash
az storage blob list \
  --container-name "$CONTAINER" \
  --account-name "$STORAGE" \
  --connection-string "$STORAGE_CONN" \
  --prefix "gold/" \
  --query "[].{name:name, sizeKB:properties.contentLength}" \
  --output table
```
`[FREE]` — listing is a transaction, priced in millionths of a dollar.

### Check the backend

```bash
curl -sS -w '\nHTTP %{http_code} in %{time_total}s\n' "https://${API_FQDN}/health"
```

Run it twice. The first call pays the cold start from step 8; the second should
return in milliseconds. That difference *is* the scale-to-zero trade-off, and
seeing it directly is more useful than reading about it.

Stream the app's logs:

```bash
az containerapp logs show \
  --name "$APP" \
  --resource-group "$RG" \
  --container "$APP" \
  --tail 100 \
  --follow
```
`[FREE]`

### Shipping a new version

```bash
az acr build --registry "$ACR" --image fleet-etl:v2 --file Dockerfile.etl     --platform linux/amd64 .
az acr build --registry "$ACR" --image fleet-api:v2 --file Dockerfile.backend --platform linux/amd64 .

az containerapp job update --name "$JOB" --resource-group "$RG" --image "${ACR_LOGIN}/fleet-etl:v2"
az containerapp update     --name "$APP" --resource-group "$RG" --image "${ACR_LOGIN}/fleet-api:v2"
```

Use a new tag every time. Re-pushing `:v1` and redeploying often serves the
cached old layer, and you spend an afternoon debugging a fix that did deploy
correctly three times.

---

## 11. Teardown

```bash
az group delete --name "$RG" --yes --no-wait
```

**This is the command that stops all billing.** Deleting the resource group
deletes the storage account, the registry, the Container Apps environment, the
job, the app, and the Log Analytics workspace — everything created by this
runbook lives in `$RG` for exactly this reason. Nothing continues to charge
afterwards.

It is irreversible. The lake goes with it. That is acceptable here because
`make generate` rebuilds bronze deterministically and the rest is derived, but
check that assumption before running it against anything real.

`--no-wait` returns immediately; deletion continues server-side and takes a few
minutes. Confirm it finished:

```bash
az group exists --name "$RG"     # false when done
```

### Keeping the project but killing the $5

If you want the deployment to keep running and only want the registry charge to
stop, delete just the ACR. Container Apps has already pulled and cached the
images, so the running job and app are unaffected — you simply cannot deploy a
new version until you create a registry again.

```bash
az acr delete --name "$ACR" --resource-group "$RG" --yes
```

That drops the monthly bill from roughly $5.35 to roughly $0.25. See
`docs/COST.md`.
