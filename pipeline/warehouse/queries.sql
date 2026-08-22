-- ===========================================================================
-- Fleet Telemetry Data Platform — warehouse query catalogue
--
-- Every query below runs against views registered by
-- `pipeline/warehouse/session.py`, each of which is a `read_parquet()` over
-- the lake. Nothing here is loaded into DuckDB: the FROM clauses name views,
-- and a view is a stored query, not a stored copy.
--
-- FILE FORMAT (this file is both documentation and the CLI's input)
--   `-- name: <slug>`      starts a query; everything until the next `name:`
--                          header belongs to it.
--   `-- layer: <...>`      which lakehouse layer the query reads, so a reader
--                          can see at a glance whether an answer costs a
--                          420k-row scan or a 2k-row one.
--   `-- question: <...>`   the business question, printed above the results by
--                          `run_queries.py`.
--   Everything else is ordinary SQL comment and is passed to DuckDB untouched.
--
-- ONE CONVENTION WORTH STATING UP FRONT: every window ("the last 7 days") is
-- anchored to the newest reading actually present in the data, never to
-- `current_date`. The lake holds simulated history that is regenerated on
-- demand; a wall-clock filter would quietly return zero rows the day after a
-- backfill and every query in this catalogue would become a false negative.
-- The same rule is followed by `pipeline/flows/gold_flow.py`.
-- ===========================================================================


-- ===========================================================================
-- name: top_fault_equipment
-- layer: gold (daily_equipment_summary)
-- question: Which ten machines threw the most fault codes over the last 7 days?
--
-- The maintenance planner's morning list. It reads gold rather than silver on
-- purpose: the per-machine-per-day rollup already exists, so this answer costs
-- a 2,000-row scan instead of re-aggregating ~430,000 raw readings. That is the
-- entire argument for having a gold layer, expressed as one query.
--
-- `faults_per_op_hour` is the column that changes decisions. A machine that ran
-- 50 hours and faulted 100 times is in worse shape than one that faulted 90
-- times in 14 hours — but the raw count ranks them the other way round, so the
-- rate is reported next to it rather than instead of it.
-- ===========================================================================
WITH anchor AS (
    SELECT max(day) AS latest_day FROM daily_equipment_summary
),
recent AS (
    -- `latest_day - 6` is 7 calendar days inclusive of the anchor day.
    SELECT d.*
    FROM daily_equipment_summary d, anchor a
    WHERE d.day >= a.latest_day - 6
)
SELECT
    equipment_id,
    any_value(equipment_type)                                       AS equipment_type,
    sum(fault_code_count)                                           AS faults,
    max(distinct_fault_codes)                                       AS worst_day_codes,
    round(sum(operating_hours), 1)                                  AS operating_hours,
    round(sum(fault_code_count) / nullif(sum(operating_hours), 0), 2) AS faults_per_op_hour,
    count(*)                                                        AS days_reporting
FROM recent
GROUP BY equipment_id
HAVING sum(fault_code_count) > 0
ORDER BY faults DESC, faults_per_op_hour DESC
LIMIT 10;


-- ===========================================================================
-- name: avg_fuel_by_type
-- layer: gold (daily_equipment_summary)
-- question: How much fuel does each class of machine burn per operating hour?
--
-- The number a fleet budget is built from, and a worked example of why the
-- aggregation order matters. `pct_per_op_hour` divides total tank percentage
-- burned by total hours run — an hours-weighted rate. `unweighted_daily_avg`
-- is the same data averaged the naive way, one ratio per machine-day averaged
-- flat, which lets a machine that idled for twelve minutes and burned 1% count
-- as heavily as one that worked a full shift. The two columns are printed side
-- by side deliberately: the gap between them is the bias.
--
-- `WHERE operating_hours > 0` excludes machine-days that never ran. Including
-- them would put zeros in the denominator, and coalescing those to something
-- harmless would understate the fleet's real burn rate.
-- ===========================================================================
WITH per_type AS (
    SELECT
        equipment_type,
        count(DISTINCT equipment_id) AS machines,
        count(*)                     AS machine_days,
        sum(operating_hours)         AS operating_hours,
        sum(fuel_consumed_pct)       AS tank_pct_burned,
        avg(fuel_burn_pct_per_hour)  AS unweighted_daily_avg,
        sum(refuel_events)           AS refuel_events
    FROM daily_equipment_summary
    WHERE operating_hours > 0
    GROUP BY equipment_type
)
SELECT
    equipment_type,
    machines,
    machine_days,
    round(operating_hours, 1)                          AS operating_hours,
    round(tank_pct_burned, 1)                          AS tank_pct_burned,
    round(tank_pct_burned / operating_hours, 3)        AS pct_per_op_hour,
    round(unweighted_daily_avg, 3)                     AS unweighted_daily_avg,
    refuel_events
FROM per_type
ORDER BY pct_per_op_hour DESC;


-- ===========================================================================
-- name: critical_machines
-- layer: gold (fleet_health_flags)
-- question: Which machines are flagged critical right now, worst health score first?
--
-- The alert list, straight off the gold table the health scoring produced.
-- `health_score` runs 100 (healthy) down to 0, so ASC is "worst first"; ties
-- break on fault volume because two machines at 25.0 are not equally urgent.
--
-- Healthy machines are absent from this table rather than present with a false
-- flag, which is why there is no `WHERE health_score < threshold` here — the
-- gold layer already made that call, and re-deriving it in SQL would let the
-- dashboard and the alerting disagree about what "critical" means.
-- ===========================================================================
SELECT
    equipment_id,
    equipment_type,
    round(health_score, 1)              AS health_score,
    flag_reasons,
    round(recent_max_temp_c, 1)         AS max_temp_c,
    round(recent_min_oil_psi, 1)        AS min_oil_psi,
    recent_fault_count,
    top_fault_code,
    round(hours_since_last_reading, 2)  AS hrs_since_seen
FROM fleet_health_flags
WHERE severity = 'critical'
ORDER BY health_score ASC, recent_fault_count DESC;


-- ===========================================================================
-- name: temp_trend_leaders
-- layer: gold (fleet_health_flags)
-- question: Which 15 machines have the steepest rising coolant-temperature trend?
--
-- Degradation shows up as a slope before it shows up as an alarm: a machine
-- climbing 1.3 C every hour is heading somewhere bad even while every single
-- reading is still inside spec. `temp_trend_c_per_hour` is a least-squares fit
-- over the health window, computed once in the gold layer.
--
-- `QUALIFY` earns its place here. The alternative is wrapping the whole SELECT
-- in a subquery just to filter on `rank()`, because a window function cannot
-- appear in `WHERE` — it is evaluated after it. QUALIFY is to window functions
-- what HAVING is to aggregates, and using `rank()` rather than `LIMIT 15` means
-- a tie at the boundary returns both machines instead of an arbitrary one.
-- ===========================================================================
SELECT
    equipment_id,
    equipment_type,
    severity,
    round(temp_trend_c_per_hour, 3)                    AS trend_c_per_hour,
    round(recent_avg_temp_c, 1)                        AS avg_temp_c,
    round(recent_max_temp_c, 1)                        AS max_temp_c,
    -- Where the trend lands after one more window if nobody intervenes.
    round(recent_avg_temp_c + temp_trend_c_per_hour * window_hours, 1) AS projected_temp_c,
    readings_in_window
FROM fleet_health_flags
WHERE temp_trend_c_per_hour IS NOT NULL
QUALIFY rank() OVER (ORDER BY temp_trend_c_per_hour DESC) <= 15
ORDER BY trend_c_per_hour DESC;


-- ===========================================================================
-- name: fleet_utilisation
-- layer: gold (daily_equipment_summary)
-- question: How many operating hours did each equipment type run on each day?
--
-- The utilisation board: one row per day, one column per machine class, plus
-- the fleet total and the day-on-day swing.
--
-- Built with DuckDB's `PIVOT`, not a hand-written `sum(...) FILTER (WHERE
-- equipment_type = 'excavator')` per class. Hand-written conditional
-- aggregation would hardcode the four equipment types into the SQL, and a fifth
-- class arriving in the fleet would silently vanish from the report rather than
-- appear as a new column. `PIVOT` reads the column list out of the data.
--
-- `b.* EXCLUDE (day)` then splices those dynamic columns in without naming
-- them, and the `LAG` window supplies the comparison a flat GROUP BY cannot:
-- yesterday's total on today's row.
-- ===========================================================================
WITH fleet AS (
    SELECT
        day,
        sum(operating_hours)         AS fleet_hours,
        count(DISTINCT equipment_id) AS machines_reporting,
        sum(readings_count)          AS readings
    FROM daily_equipment_summary
    GROUP BY day
),
by_type AS (
    PIVOT daily_equipment_summary
    ON equipment_type
    USING round(sum(operating_hours), 1)
    GROUP BY day
)
SELECT
    f.day,
    b.* EXCLUDE (day),
    round(f.fleet_hours, 1)                              AS fleet_hours,
    f.machines_reporting,
    round(f.fleet_hours / nullif(f.machines_reporting, 0), 2) AS hrs_per_machine,
    round(
        100.0 * (f.fleet_hours - lag(f.fleet_hours) OVER (ORDER BY f.day))
        / nullif(lag(f.fleet_hours) OVER (ORDER BY f.day), 0), 1
    )                                                    AS pct_change_vs_prev_day
FROM fleet f
JOIN by_type b USING (day)
ORDER BY f.day;


-- ===========================================================================
-- name: fault_code_pareto
-- layer: silver (silver_telemetry)
-- question: Which fault codes account for most of the fleet's faults, cumulatively?
--
-- The classic maintenance Pareto: fix the codes above the 80% line and you have
-- addressed four fifths of the fleet's fault volume. This reads silver rather
-- than gold because gold aggregates faults per machine, not per *code* — the
-- code-level breakdown only exists in the cleaned readings, so the warehouse
-- earns its keep by answering a question no pre-built table anticipated. That
-- is exactly the case for keeping raw and cleaned layers queryable rather than
-- shipping only the rollups.
--
-- Two windows over the same partition (the whole result set), which is why
-- neither needs a PARTITION BY:
--   sum(...) OVER ()                       the grand total, on every row
--   sum(...) OVER (ORDER BY ... ROWS ...)  the running total in rank order
-- Their ratio is the cumulative percentage. Doing this with a self-join would
-- be O(n^2) and considerably harder to read.
--
-- `machines_affected` is the sanity check on the ranking: 200 occurrences
-- spread over 150 machines is a fleet-wide design issue, and 200 occurrences on
-- two machines is two broken machines. Same rank, completely different action.
-- ===========================================================================
WITH counts AS (
    SELECT
        fault_code,
        count(*)                     AS occurrences,
        count(DISTINCT equipment_id) AS machines_affected,
        min(event_time)              AS first_seen,
        max(event_time)              AS last_seen
    FROM silver_telemetry
    WHERE fault_code IS NOT NULL
    GROUP BY fault_code
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY occurrences DESC, fault_code) AS pareto_rank,
        fault_code,
        occurrences,
        machines_affected,
        last_seen,
        round(100.0 * occurrences / sum(occurrences) OVER (), 2) AS pct_of_faults,
        round(
            100.0 * sum(occurrences) OVER (
                ORDER BY occurrences DESC, fault_code
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / sum(occurrences) OVER (), 2
        ) AS cumulative_pct
    FROM counts
)
SELECT
    pareto_rank,
    fault_code,
    occurrences,
    machines_affected,
    pct_of_faults,
    cumulative_pct,
    -- A code belongs to the vital few if the running total was still under 80%
    -- *before* it was added, so the code that crosses the line is included —
    -- that is the standard reading of the rule, and `cumulative_pct <= 80`
    -- alone would drop it.
    CASE WHEN cumulative_pct - pct_of_faults < 80 THEN 'vital few' ELSE 'trivial many' END
        AS pareto_class,
    last_seen
FROM ranked
ORDER BY pareto_rank;


-- ===========================================================================
-- name: data_quality_report
-- layer: silver + bronze (silver_telemetry, bronze_telemetry)
-- question: What did the cleaning step change, and does bronze still reconcile to silver?
--
-- The warehouse auditing the pipeline that fills it. Two sections in one
-- result set:
--
--   reconciliation  — bronze rows landed vs silver rows retained, and the
--                     difference accounted for. A row count that drops without
--                     an explanation is the single most common way a data
--                     platform lies to its users.
--   quality_flag    — how many rows carry each repair flag. `quality_flags` is
--                     a comma-separated audit trail, so a row repaired twice
--                     appears under both flags; `unnest(string_split(...))`
--                     explodes it to one row per flag rather than counting the
--                     literal string "assumed_utc,gps_null_island" as its own
--                     category.
--
-- "duplicate business keys left in silver" must be 0. Silver's contract is one
-- row per (equipment_id, event_time), and bronze deliberately contains
-- cross-batch resends — the same reading sent again under a different
-- `_ingest_batch_id`, which a naive `DISTINCT *` would not catch. This line is
-- the assertion that the dedup actually held, run against the real files rather
-- than against a fixture.
-- ===========================================================================
WITH bronze AS (
    SELECT count(*) AS row_count FROM bronze_telemetry
),
silver AS (
    SELECT
        count(*)                                          AS row_count,
        count(*) FILTER (WHERE quality_flags IS NOT NULL) AS repaired_rows,
        count(DISTINCT (equipment_id, event_time))        AS business_keys
    FROM silver_telemetry
),
flag_rows AS (
    SELECT trim(f.flag) AS flag, count(*) AS row_count
    FROM silver_telemetry s,
         unnest(string_split(s.quality_flags, ',')) AS f(flag)
    WHERE s.quality_flags IS NOT NULL
    GROUP BY 1
),
report AS (
    SELECT 0 AS section_order, 'reconciliation' AS section,
           'bronze rows landed' AS metric,
           (SELECT row_count FROM bronze) AS row_count, 'bronze' AS basis
    UNION ALL
    SELECT 0, 'reconciliation', 'silver rows retained',
           (SELECT row_count FROM silver), 'bronze'
    UNION ALL
    SELECT 0, 'reconciliation', 'rows removed (quarantined or deduplicated)',
           (SELECT row_count FROM bronze) - (SELECT row_count FROM silver), 'bronze'
    UNION ALL
    SELECT 0, 'reconciliation', 'silver rows carrying >= 1 repair flag',
           (SELECT repaired_rows FROM silver), 'silver'
    UNION ALL
    SELECT 0, 'reconciliation', 'duplicate business keys left in silver (must be 0)',
           (SELECT row_count - business_keys FROM silver), 'silver'
    UNION ALL
    SELECT 1, 'quality_flag', flag, row_count, 'silver' FROM flag_rows
)
SELECT
    section,
    metric,
    row_count,
    basis,
    round(
        100.0 * row_count / CASE basis
            WHEN 'bronze' THEN (SELECT row_count FROM bronze)
            ELSE (SELECT row_count FROM silver)
        END, 3
    ) AS pct_of_basis
FROM report
ORDER BY section_order, row_count DESC;


-- ===========================================================================
-- name: partition_pruning_demo
-- layer: bronze (bronze_telemetry)
-- question: How many readings landed in the two most recent daily partitions?
--
-- The point of this query is not its result, it is its plan. Bronze is written
-- as `raw/dt=<date>/batch_*.parquet`, and `dt` is not stored inside any file —
-- DuckDB reads the date out of the *directory name* (`hive_partitioning=true`)
-- and treats a predicate on it as a file filter. Files whose path cannot
-- satisfy the predicate are never opened at all: no HTTP range request on
-- Azure, no page cache churn locally. That is the concrete payoff of
-- partitioning, and it costs nothing at write time.
--
-- Run `python -m pipeline.warehouse.run_queries --query partition_pruning_demo
-- --analyze` to see it: the scan node reports `Total Files Read: 10` against a
-- glob of 77 files. `docs/WAREHOUSE.md` has the measured plans, including the
-- difference between a literal predicate (pruned when the plan is built) and
-- the anchored subquery used here (pruned at runtime by a dynamic filter).
--
-- The anchor is a subquery instead of a hardcoded date because a hardcoded date
-- stops matching the moment the lake is regenerated. That choice costs a cheap
-- second pass over the partition column, which is read from the paths and not
-- from the Parquet payload.
-- ===========================================================================
SELECT
    dt AS partition_date,
    count(*)                                          AS readings,
    count(DISTINCT equipment_id)                      AS machines,
    count(*) FILTER (WHERE fault_code IS NOT NULL)    AS fault_readings,
    count(DISTINCT _ingest_batch_id)                  AS ingest_batches,
    min(_ingested_at_utc)                             AS first_ingest_utc,
    max(_ingested_at_utc)                             AS last_ingest_utc
FROM bronze_telemetry
WHERE dt >= (SELECT max(dt) FROM bronze_telemetry) - 1
GROUP BY dt
ORDER BY dt;
