/**
 * Drill-down for one machine: its daily gold rows plus the flag that put it in
 * the queue.
 *
 * Implemented as an overlay drawer rather than a route so the queue stays on
 * screen behind it — triage is a scan-and-dip loop ("why is this one critical?
 * next"), and a full page navigation loses the reader's place in a 40-row list
 * every time. The cost is no deep-linkable URL per machine, which is the right
 * trade for a single-screen console.
 *
 * The daily table shows every column of gold daily_equipment_summary. This is
 * an engineering console, not a summary email: the person asking why a machine
 * is flagged wants engine_hours_start and refuel_events, not a curated subset.
 */

import { useEffect } from "react";

import { fetchEquipmentHistory } from "../api/client";
import type { DailySummary } from "../api/types";
import { useAsync } from "../hooks/useAsync";
import { fmtInt, fmtNumber, fmtSigned, fmtUtc, humanise, MISSING, splitReasons } from "../lib/format";
import { SeverityBadge } from "./SeverityBadge";
import { Sparkline } from "./Sparkline";
import { ErrorPanel, LoadingPanel } from "./StatePanels";

const HISTORY_DAYS = 30;

/**
 * Column order mirrors GOLD_DAILY_SUMMARY_SCHEMA so the table reads like the
 * table it came from. `digits` is per-column because rounding operating hours
 * to two decimals implies a precision the sensor never had.
 */
const DAY_COLUMNS: Array<{
  key: keyof DailySummary;
  label: string;
  digits?: number;
}> = [
  { key: "day", label: "Day" },
  { key: "readings_count", label: "Readings" },
  { key: "under_load_readings", label: "Under load" },
  { key: "avg_coolant_temp_c", label: "Avg °C", digits: 1 },
  { key: "max_coolant_temp_c", label: "Max °C", digits: 1 },
  { key: "p95_coolant_temp_c", label: "P95 °C", digits: 1 },
  { key: "avg_oil_pressure_psi", label: "Avg psi", digits: 1 },
  { key: "min_oil_pressure_psi", label: "Min psi", digits: 1 },
  { key: "engine_hours_start", label: "Hrs start", digits: 1 },
  { key: "engine_hours_end", label: "Hrs end", digits: 1 },
  { key: "operating_hours", label: "Operating hrs", digits: 2 },
  { key: "fuel_consumed_pct", label: "Fuel used %", digits: 1 },
  { key: "fuel_burn_pct_per_hour", label: "Fuel %/hr", digits: 2 },
  { key: "refuel_events", label: "Refuels" },
  { key: "fault_code_count", label: "Faults" },
  { key: "distinct_fault_codes", label: "Distinct faults" },
  { key: "last_gps_lat", label: "Lat", digits: 4 },
  { key: "last_gps_lon", label: "Lon", digits: 4 },
];

function renderCell(row: DailySummary, key: keyof DailySummary, digits?: number) {
  const value = row[key];
  if (value === null || value === undefined) return MISSING;
  if (typeof value === "number") {
    return digits === undefined ? fmtInt(value) : fmtNumber(value, digits);
  }
  return String(value);
}

export function EquipmentDetail({
  equipmentId,
  onClose,
}: {
  equipmentId: string;
  onClose: () => void;
}) {
  const history = useAsync(
    () => fetchEquipmentHistory(equipmentId, HISTORY_DAYS),
    [equipmentId],
  );

  // Escape closes the drawer: the mouse is already on the table when the reader
  // decides they are done with this machine.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const data = history.data;
  const flag = data?.flag ?? null;
  const days = data?.days ?? [];

  return (
    <div className="drawer-overlay" onClick={onClose} role="presentation">
      <aside
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-label={`History for ${equipmentId}`}
        onClick={(event) => {
          // Clicks inside the panel must not bubble to the overlay's close handler.
          event.stopPropagation();
        }}
      >
        <header className="drawer-head">
          <div>
            <h2 className="mono">{equipmentId}</h2>
            <p className="muted">
              {data ? humanise(data.equipment_type) : "loading…"}
              {flag ? " · flagged" : data ? " · not currently flagged" : ""}
            </p>
          </div>
          <div className="drawer-head-right">
            {flag && <SeverityBadge severity={flag.severity} />}
            <button type="button" className="btn" onClick={onClose} aria-label="Close detail">
              Close
            </button>
          </div>
        </header>

        {history.error ? (
          <ErrorPanel error={history.error} onRetry={history.reload} />
        ) : history.initial ? (
          <LoadingPanel label={`Loading ${HISTORY_DAYS}-day history…`} />
        ) : (
          <div className={history.loading ? "drawer-body is-stale" : "drawer-body"}>
            {flag && (
              <section className="detail-block">
                <h3>Why it is flagged</h3>
                <div className="chips">
                  {splitReasons(flag.flag_reasons).map((reason) => (
                    <span className="chip chip-strong" key={reason}>{humanise(reason)}</span>
                  ))}
                </div>
                <dl className="kv">
                  <div><dt>Health score</dt><dd>{fmtNumber(flag.health_score, 0)}</dd></div>
                  <div><dt>Temp trend</dt><dd>{fmtSigned(flag.temp_trend_c_per_hour)} °C/h</dd></div>
                  <div><dt>Recent avg / max</dt><dd>{fmtNumber(flag.recent_avg_temp_c)} / {fmtNumber(flag.recent_max_temp_c)} °C</dd></div>
                  <div><dt>Min oil pressure</dt><dd>{fmtNumber(flag.recent_min_oil_psi)} psi</dd></div>
                  <div><dt>Faults in window</dt><dd>{fmtInt(flag.recent_fault_count)} ({fmtInt(flag.distinct_fault_codes)} distinct{flag.top_fault_code ? `, top ${flag.top_fault_code}` : ""})</dd></div>
                  <div><dt>Readings in window</dt><dd>{fmtInt(flag.readings_in_window)} over {fmtInt(flag.window_hours)}h</dd></div>
                  <div><dt>Last seen</dt><dd>{fmtUtc(flag.last_seen_utc)}</dd></div>
                  <div><dt>Silent for</dt><dd>{fmtNumber(flag.hours_since_last_reading)} h</dd></div>
                </dl>
              </section>
            )}

            <section className="detail-block">
              <h3>Max coolant temperature by day</h3>
              {days.length === 0 ? (
                <p className="muted">No daily rows in the last {HISTORY_DAYS} days.</p>
              ) : (
                <Sparkline
                  values={days.map((day) => day.max_coolant_temp_c)}
                  labels={days.map((day) => day.day)}
                  unit=" °C"
                />
              )}
            </section>

            <section className="detail-block">
              <h3>Daily summary ({days.length} {days.length === 1 ? "day" : "days"})</h3>
              <div className="table-wrap">
                <table className="data-table compact">
                  <thead>
                    <tr>
                      {DAY_COLUMNS.map((column) => (
                        <th key={String(column.key)} scope="col" className={column.key === "day" ? "" : "num"}>
                          {column.label}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {days.map((row) => (
                      <tr key={row.day}>
                        {DAY_COLUMNS.map((column) => (
                          <td
                            key={String(column.key)}
                            className={column.key === "day" ? "mono nowrap" : "num"}
                          >
                            {renderCell(row, column.key, column.digits)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        )}
      </aside>
    </div>
  );
}
