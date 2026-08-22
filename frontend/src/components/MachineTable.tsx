/**
 * The full roster from /api/equipment — every machine with gold rows, flagged
 * or not.
 *
 * The flagged queue answers "what needs attention"; this answers "what is the
 * status of EXC-0137", which is the other question a dispatcher gets asked and
 * cannot answer from a table that only lists problems. Unflagged machines carry
 * a null severity and health score by contract, which renders as an em dash
 * rather than a zero — a machine with no score is not a machine scoring zero.
 */

import type { EquipmentListItem } from "../api/types";
import { fmtNumber, humanise, MISSING } from "../lib/format";
import { SeverityBadge } from "./SeverityBadge";

interface MachineTableProps {
  items: EquipmentListItem[];
  selectedId: string | null;
  onSelect: (equipmentId: string) => void;
}

export function MachineTable({ items, selectedId, onSelect }: MachineTableProps) {
  return (
    <div className="table-wrap">
      <table className="data-table">
        <caption className="sr-only">All machines with gold-layer rows</caption>
        <thead>
          <tr>
            <th scope="col">Equipment</th>
            <th scope="col">Type</th>
            <th scope="col">Status</th>
            <th scope="col" className="num">Health</th>
            <th scope="col">Last day</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => {
            const isSelected = item.equipment_id === selectedId;
            return (
              <tr
                key={item.equipment_id}
                className={isSelected ? "is-selected clickable" : "clickable"}
                onClick={() => onSelect(item.equipment_id)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onSelect(item.equipment_id);
                  }
                }}
                tabIndex={0}
                role="button"
                aria-label={`Open history for ${item.equipment_id}`}
              >
                <th scope="row" className="mono">{item.equipment_id}</th>
                <td>{humanise(item.equipment_type)}</td>
                <td>
                  {item.severity ? (
                    <SeverityBadge severity={item.severity} />
                  ) : (
                    <span className="badge badge-ok">ok</span>
                  )}
                </td>
                <td className="num">
                  {item.health_score === null ? MISSING : fmtNumber(item.health_score, 0)}
                </td>
                <td className="mono">{item.last_day}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
