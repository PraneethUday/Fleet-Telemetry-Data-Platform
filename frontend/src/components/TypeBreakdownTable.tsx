/**
 * Per-machine-class rollup, straight from gold fleet_summary_by_type.
 *
 * Rows are clickable and set the equipment-type filter. A breakdown table whose
 * rows are inert forces the reader to look at "excavators: 18 flagged", then
 * travel to a dropdown to see which 18 — the row already names the thing they
 * want to drill into, so it should be the control.
 */

import type { FleetTypeSummary } from "../api/types";
import { fmtInt, fmtNumber, fmtPercent, humanise } from "../lib/format";

interface TypeBreakdownTableProps {
  rows: FleetTypeSummary[];
  selectedType: string;
  onSelectType: (equipmentType: string) => void;
}

export function TypeBreakdownTable({
  rows,
  selectedType,
  onSelectType,
}: TypeBreakdownTableProps) {
  return (
    <div className="table-wrap">
      <table className="data-table">
        <caption className="sr-only">Fleet metrics broken down by equipment type</caption>
        <thead>
          <tr>
            <th scope="col">Type</th>
            <th scope="col" className="num">Machines</th>
            <th scope="col" className="num">Active 24h</th>
            <th scope="col" className="num">Avg coolant °C</th>
            <th scope="col" className="num">Max coolant °C</th>
            <th scope="col" className="num">Avg oil psi</th>
            <th scope="col" className="num">Operating hrs</th>
            <th scope="col" className="num">Fuel %/hr</th>
            <th scope="col" className="num">Faults</th>
            <th scope="col" className="num">Flagged</th>
            <th scope="col" className="num">Crit</th>
            <th scope="col" className="num">Warn</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const isSelected = row.equipment_type === selectedType;
            return (
              <tr
                key={row.equipment_type}
                className={isSelected ? "is-selected clickable" : "clickable"}
                onClick={() => onSelectType(isSelected ? "all" : row.equipment_type)}
                aria-selected={isSelected}
              >
                <th scope="row">{humanise(row.equipment_type)}</th>
                <td className="num">{fmtInt(row.machine_count)}</td>
                <td className="num">{fmtInt(row.active_machines_24h)}</td>
                <td className="num">{fmtNumber(row.avg_coolant_temp_c)}</td>
                <td className="num">{fmtNumber(row.max_coolant_temp_c)}</td>
                <td className="num">{fmtNumber(row.avg_oil_pressure_psi)}</td>
                <td className="num">{fmtNumber(row.total_operating_hours, 0)}</td>
                <td className="num">{fmtNumber(row.avg_fuel_burn_pct_per_hour, 2)}</td>
                <td className="num">{fmtInt(row.total_fault_codes)}</td>
                <td className="num">
                  <div className="bar-cell">
                    <span>{fmtInt(row.machines_flagged)}</span>
                    <span className="bar" aria-hidden="true">
                      <span
                        className="bar-fill"
                        style={{ width: `${Math.min(row.pct_flagged, 100)}%` }}
                      />
                    </span>
                    <span className="muted">{fmtPercent(row.pct_flagged, 1)}</span>
                  </div>
                </td>
                <td className="num critical-text">{fmtInt(row.critical_count)}</td>
                <td className="num warning-text">{fmtInt(row.warning_count)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
