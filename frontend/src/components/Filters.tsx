/**
 * The two filters that scope the whole console.
 *
 * The equipment-type options are derived from the fleet-summary response, never
 * from a constant in this file. The canonical type list already lives in
 * pipeline/schemas.py; duplicating it in the frontend would mean a new machine
 * class silently missing from the dropdown until someone remembered to edit two
 * repositories. If the pipeline knows about it, the filter offers it.
 */

import type { FleetTypeSummary, Severity } from "../api/types";
import { fmtInt, humanise } from "../lib/format";

export type SeverityFilter = Severity | "all";

interface FiltersProps {
  byType: FleetTypeSummary[];
  equipmentType: string;
  onEquipmentTypeChange: (value: string) => void;
  severity: SeverityFilter;
  onSeverityChange: (value: SeverityFilter) => void;
}

const SEVERITY_OPTIONS: Array<{ value: SeverityFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "critical", label: "Critical" },
  { value: "warning", label: "Warning" },
];

export function Filters({
  byType,
  equipmentType,
  onEquipmentTypeChange,
  severity,
  onSeverityChange,
}: FiltersProps) {
  const totalMachines = byType.reduce((total, row) => total + row.machine_count, 0);

  return (
    <div className="filters">
      <label className="field">
        <span className="field-label">Equipment type</span>
        <select
          value={equipmentType}
          onChange={(event) => onEquipmentTypeChange(event.target.value)}
        >
          <option value="all">All types ({fmtInt(totalMachines)})</option>
          {byType.map((row) => (
            <option key={row.equipment_type} value={row.equipment_type}>
              {humanise(row.equipment_type)} ({fmtInt(row.machine_count)})
            </option>
          ))}
        </select>
      </label>

      <div className="field">
        <span className="field-label">Severity</span>
        <div className="segmented" role="group" aria-label="Filter by severity">
          {SEVERITY_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              className={severity === option.value ? "seg is-active" : "seg"}
              aria-pressed={severity === option.value}
              onClick={() => onSeverityChange(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
