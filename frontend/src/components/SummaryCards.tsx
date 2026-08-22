/**
 * The six numbers a fleet manager wants before reading anything else.
 *
 * The cards respect the equipment-type filter. When a type is selected they are
 * recomputed from that type's row rather than left showing fleet totals, because
 * a header claiming "500 machines / 41 flagged" above a table filtered to
 * excavators is actively misleading. With no filter the fleet-wide figures come
 * straight from the API's top-level fields, which are authoritative.
 */

import type { FleetSummary, FleetTypeSummary } from "../api/types";
import { fmtInt, fmtNumber, fmtPercent, humanise, MISSING } from "../lib/format";

interface SummaryCardsProps {
  summary: FleetSummary;
  /** Canonical equipment_type, or "all". */
  equipmentType: string;
}

function sum(rows: FleetTypeSummary[], pick: (row: FleetTypeSummary) => number): number {
  return rows.reduce((total, row) => total + pick(row), 0);
}

/**
 * Weighted by machine_count, not a plain mean of the per-type averages.
 * An unweighted mean would give a 12-machine generator fleet the same say as a
 * 180-machine excavator fleet. This is still an approximation — the exact
 * figure needs per-type reading counts, which the gold table does not expose —
 * so it is labelled "fleet avg" rather than presented as a precise statistic.
 */
function weightedAvgTemp(rows: FleetTypeSummary[]): number | null {
  const usable = rows.filter((row) => row.avg_coolant_temp_c !== null && row.machine_count > 0);
  if (usable.length === 0) return null;
  const weight = sum(usable, (row) => row.machine_count);
  if (weight === 0) return null;
  return sum(usable, (row) => (row.avg_coolant_temp_c as number) * row.machine_count) / weight;
}

function maxTemp(rows: FleetTypeSummary[]): number | null {
  const values = rows
    .map((row) => row.max_coolant_temp_c)
    .filter((value): value is number => value !== null && Number.isFinite(value));
  return values.length ? Math.max(...values) : null;
}

export function SummaryCards({ summary, equipmentType }: SummaryCardsProps) {
  const scoped = equipmentType !== "all";
  const rows = scoped
    ? summary.by_type.filter((row) => row.equipment_type === equipmentType)
    : summary.by_type;

  const machines = scoped ? sum(rows, (row) => row.machine_count) : summary.fleet_size;
  const flagged = scoped ? sum(rows, (row) => row.machines_flagged) : summary.machines_flagged;
  const critical = scoped ? sum(rows, (row) => row.critical_count) : summary.critical_count;
  const warning = scoped ? sum(rows, (row) => row.warning_count) : summary.warning_count;
  const active = sum(rows, (row) => row.active_machines_24h);
  const avgTemp = weightedAvgTemp(rows);
  const peakTemp = maxTemp(rows);

  const flaggedPct = machines > 0 ? (flagged / machines) * 100 : null;
  const scopeLabel = scoped ? humanise(equipmentType) : "whole fleet";

  return (
    <section className="cards" aria-label={`Fleet summary for ${scopeLabel}`}>
      <Card
        label="Machines"
        value={fmtInt(machines)}
        sub={`${fmtInt(active)} active in last 24h`}
      />
      <Card
        label="Flagged for maintenance"
        value={fmtInt(flagged)}
        sub={flaggedPct === null ? MISSING : `${fmtPercent(flaggedPct)} of ${scopeLabel}`}
        tone={flagged > 0 ? "warn" : "ok"}
      />
      <Card
        label="Critical"
        value={fmtInt(critical)}
        sub="act now"
        tone={critical > 0 ? "critical" : "ok"}
      />
      <Card
        label="Warning"
        value={fmtInt(warning)}
        sub="schedule inspection"
        tone={warning > 0 ? "warn" : "ok"}
      />
      <Card label="Fleet avg coolant" value={`${fmtNumber(avgTemp)} °C`} sub="machine-weighted" />
      <Card label="Peak coolant" value={`${fmtNumber(peakTemp)} °C`} sub="highest daily max" />
    </section>
  );
}

function Card({
  label,
  value,
  sub,
  tone = "neutral",
}: {
  label: string;
  value: string;
  sub: string;
  tone?: "neutral" | "ok" | "warn" | "critical";
}) {
  return (
    <article className={`card card-${tone}`}>
      <h3>{label}</h3>
      <p className="card-value">{value}</p>
      <p className="card-sub">{sub}</p>
    </article>
  );
}
