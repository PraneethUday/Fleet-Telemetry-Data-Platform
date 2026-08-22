/**
 * The maintenance work queue: every machine gold flagged, worst first.
 *
 * Ordering is the whole value of this table. It is sorted critical-before-
 * warning and then by ascending health score, so the machine most likely to
 * strand a crew is the first row on the screen and nobody has to sort a column
 * to find it. Ties fall back to equipment_id purely so the order is stable
 * across refetches — a queue that reshuffles under the cursor is unusable.
 *
 * Sorting happens client-side because the API returns at most a few hundred
 * flagged machines out of a 500-machine fleet: that is one small array, and
 * keeping the rule here means it cannot disagree with what the user sees.
 */

import type { HealthFlag } from "../api/types";
import { fmtInt, fmtNumber, fmtSigned, fmtUtc, humanise, splitReasons } from "../lib/format";
import { SeverityBadge } from "./SeverityBadge";

const SEVERITY_RANK: Record<string, number> = { critical: 0, warning: 1 };

export function sortFlags(flags: HealthFlag[]): HealthFlag[] {
  return [...flags].sort((a, b) => {
    const bySeverity =
      (SEVERITY_RANK[a.severity] ?? 99) - (SEVERITY_RANK[b.severity] ?? 99);
    if (bySeverity !== 0) return bySeverity;
    if (a.health_score !== b.health_score) return a.health_score - b.health_score;
    return a.equipment_id.localeCompare(b.equipment_id);
  });
}

interface FlaggedTableProps {
  flags: HealthFlag[];
  selectedId: string | null;
  onSelect: (equipmentId: string) => void;
}

export function FlaggedTable({ flags, selectedId, onSelect }: FlaggedTableProps) {
  const rows = sortFlags(flags);

  return (
    <div className="table-wrap">
      <table className="data-table">
        <caption className="sr-only">
          Machines flagged for maintenance, most severe first. Select a row for daily history.
        </caption>
        <thead>
          <tr>
            <th scope="col">Equipment</th>
            <th scope="col">Type</th>
            <th scope="col">Severity</th>
            <th scope="col" className="num">Health</th>
            <th scope="col">Reasons</th>
            <th scope="col" className="num">Temp trend °C/h</th>
            <th scope="col" className="num">Max temp °C</th>
            <th scope="col" className="num">Min oil psi</th>
            <th scope="col" className="num">Faults</th>
            <th scope="col">Last seen</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((flag) => {
            const isSelected = flag.equipment_id === selectedId;
            // A rising coolant trend is the signal that separates "hot because
            // it worked hard today" from "hot and getting hotter".
            const rising = (flag.temp_trend_c_per_hour ?? 0) > 0;
            return (
              <tr
                key={flag.equipment_id}
                className={isSelected ? "is-selected clickable" : "clickable"}
                onClick={() => onSelect(flag.equipment_id)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onSelect(flag.equipment_id);
                  }
                }}
                tabIndex={0}
                role="button"
                aria-label={`Open history for ${flag.equipment_id}`}
              >
                <th scope="row" className="mono">{flag.equipment_id}</th>
                <td>{humanise(flag.equipment_type)}</td>
                <td><SeverityBadge severity={flag.severity} /></td>
                <td className="num">
                  <div className="bar-cell">
                    <span>{fmtNumber(flag.health_score, 0)}</span>
                    <span className="bar" aria-hidden="true">
                      <span
                        className={`bar-fill bar-${flag.severity}`}
                        style={{ width: `${Math.max(0, Math.min(flag.health_score, 100))}%` }}
                      />
                    </span>
                  </div>
                </td>
                <td>
                  <span className="chips">
                    {splitReasons(flag.flag_reasons).map((reason) => (
                      <span className="chip" key={reason}>{humanise(reason)}</span>
                    ))}
                  </span>
                </td>
                <td className={rising ? "num critical-text" : "num"}>
                  {rising ? "▲ " : ""}
                  {fmtSigned(flag.temp_trend_c_per_hour)}
                </td>
                <td className="num">{fmtNumber(flag.recent_max_temp_c)}</td>
                <td className="num">{fmtNumber(flag.recent_min_oil_psi)}</td>
                <td className="num">
                  {fmtInt(flag.recent_fault_count)}
                  {flag.top_fault_code ? (
                    <span className="muted"> · {flag.top_fault_code}</span>
                  ) : null}
                </td>
                <td className="mono nowrap">{fmtUtc(flag.last_seen_utc)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
