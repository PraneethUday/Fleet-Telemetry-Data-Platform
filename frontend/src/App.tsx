/**
 * The console shell: owns the filter state and wires it to three requests.
 *
 * Filtering is done server-side by passing equipment_type and severity to the
 * API rather than fetching everything once and filtering in the browser. That
 * looks like extra round trips for a 500-machine fleet, and today it is — but
 * the filter parameters exist in the contract precisely so the client does not
 * become the thing that has to change when the fleet is 50,000 machines. The
 * one exception is the by_type breakdown, which is already fully materialised
 * in the fleet-summary response and needs no second call to narrow.
 *
 * Fleet summary is fetched once and is NOT refetched when filters change: it
 * describes the whole fleet, and refetching it would throw away the totals the
 * summary cards need in order to say "18 of 500".
 */

import { useCallback, useMemo, useState } from "react";

import { fetchEquipment, fetchFleetSummary, fetchHealth, fetchHealthFlags } from "./api/client";
import type { SeverityFilter } from "./components/Filters";
import { EquipmentDetail } from "./components/EquipmentDetail";
import { Filters } from "./components/Filters";
import { FlaggedTable } from "./components/FlaggedTable";
import { MachineTable } from "./components/MachineTable";
import { SummaryCards } from "./components/SummaryCards";
import { TypeBreakdownTable } from "./components/TypeBreakdownTable";
import { EmptyPanel, ErrorPanel, LoadingPanel } from "./components/StatePanels";
import { useAsync } from "./hooks/useAsync";
import { fmtInt, fmtUtc, humanise } from "./lib/format";

const ALL_TYPES = "all";

// Above the whole fleet size, so "flagged" is never silently truncated. The
// contract's default of 200 would quietly hide machines on a bad day.
const FLAG_LIMIT = 1000;
const ROSTER_LIMIT = 1000;

type Tab = "flagged" | "roster";

export default function App() {
  const [equipmentType, setEquipmentType] = useState<string>(ALL_TYPES);
  const [severity, setSeverity] = useState<SeverityFilter>("all");
  const [tab, setTab] = useState<Tab>("flagged");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const typeParam = equipmentType === ALL_TYPES ? undefined : equipmentType;
  const severityParam = severity === "all" ? undefined : severity;

  const health = useAsync(() => fetchHealth(), []);
  const summary = useAsync(() => fetchFleetSummary(), []);

  const flags = useAsync(
    () =>
      fetchHealthFlags({
        equipmentType: typeParam,
        severity: severityParam,
        limit: FLAG_LIMIT,
      }),
    [typeParam, severityParam],
  );

  const roster = useAsync(
    () =>
      // Only fetched when the roster tab is open: the flagged queue is the
      // default view and most sessions never need the full 500-row list.
      tab === "roster"
        ? fetchEquipment({ equipmentType: typeParam, limit: ROSTER_LIMIT })
        : Promise.resolve(null),
    [typeParam, tab],
  );

  const byType = summary.data?.by_type ?? [];
  const visibleTypes = useMemo(
    () =>
      equipmentType === ALL_TYPES
        ? byType
        : byType.filter((row) => row.equipment_type === equipmentType),
    [byType, equipmentType],
  );

  const closeDetail = useCallback(() => setSelectedId(null), []);

  const reloadAll = useCallback(() => {
    health.reload();
    summary.reload();
    flags.reload();
    roster.reload();
  }, [health.reload, summary.reload, flags.reload, roster.reload]);

  const missingGoldTables = Object.entries(health.data?.gold_tables ?? {})
    .filter(([, present]) => !present)
    .map(([name]) => name);

  const scopeLabel = equipmentType === ALL_TYPES ? "all types" : humanise(equipmentType);

  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-title">
          <h1>Fleet Telemetry</h1>
          <span className="topbar-sub">operations console</span>
        </div>
        <div className="topbar-meta">
          {health.data && (
            <span className="pill" title="Lakehouse backend serving the gold tables">
              lake: <b>{health.data.lake_backend}</b>
            </span>
          )}
          {summary.data && (
            <>
              <span className="pill">
                {fmtInt(summary.data.days_covered)} days covered
              </span>
              <span className="pill" title="When the gold layer was last built">
                gold built {fmtUtc(summary.data.generated_at_utc)}
              </span>
            </>
          )}
          <button type="button" className="btn" onClick={reloadAll}>
            Refresh
          </button>
        </div>
      </header>

      {missingGoldTables.length > 0 && (
        <div className="notice" role="status">
          Gold tables missing: <b>{missingGoldTables.join(", ")}</b>. Run the pipeline to
          build them; panels below will be empty until you do.
        </div>
      )}

      <main>
        {/* Everything else depends on the fleet summary — it supplies the filter
            options — so its failure is the one that takes over the screen. */}
        {summary.error ? (
          <ErrorPanel error={summary.error} onRetry={reloadAll} />
        ) : summary.initial || !summary.data ? (
          <LoadingPanel label="Loading fleet summary…" />
        ) : (
          <>
            <Filters
              byType={byType}
              equipmentType={equipmentType}
              onEquipmentTypeChange={setEquipmentType}
              severity={severity}
              onSeverityChange={setSeverity}
            />

            <SummaryCards summary={summary.data} equipmentType={equipmentType} />

            <section className="panel">
              <div className="panel-head">
                <h2>Breakdown by equipment type</h2>
                <p className="muted">
                  Select a row to scope the console to that machine class.
                </p>
              </div>
              {visibleTypes.length === 0 ? (
                <EmptyPanel label="No equipment types in the gold summary." />
              ) : (
                <TypeBreakdownTable
                  rows={visibleTypes}
                  selectedType={equipmentType}
                  onSelectType={setEquipmentType}
                />
              )}
            </section>

            <section className="panel">
              <div className="panel-head">
                <div className="tabs" role="tablist" aria-label="Machine list">
                  <button
                    type="button"
                    role="tab"
                    aria-selected={tab === "flagged"}
                    className={tab === "flagged" ? "tab is-active" : "tab"}
                    onClick={() => setTab("flagged")}
                  >
                    Flagged for maintenance
                    {flags.data ? ` (${fmtInt(flags.data.count)})` : ""}
                  </button>
                  <button
                    type="button"
                    role="tab"
                    aria-selected={tab === "roster"}
                    className={tab === "roster" ? "tab is-active" : "tab"}
                    onClick={() => setTab("roster")}
                  >
                    All machines
                    {roster.data ? ` (${fmtInt(roster.data.count)})` : ""}
                  </button>
                </div>
                <p className="muted">
                  Scope: {scopeLabel}
                  {/* The roster endpoint has no severity parameter, so saying
                      "critical only" above an unfiltered list would be a lie. */}
                  {tab === "flagged" && severity !== "all" ? ` · ${severity} only` : ""} ·
                  select a row for its daily history
                </p>
              </div>

              {tab === "flagged" ? (
                flags.error ? (
                  <ErrorPanel error={flags.error} onRetry={flags.reload} />
                ) : flags.initial || !flags.data ? (
                  <LoadingPanel label="Loading health flags…" />
                ) : flags.data.items.length === 0 ? (
                  <EmptyPanel label={`No machines flagged for ${scopeLabel}. Fleet is healthy.`} />
                ) : (
                  <div className={flags.loading ? "is-stale" : undefined}>
                    <FlaggedTable
                      flags={flags.data.items}
                      selectedId={selectedId}
                      onSelect={setSelectedId}
                    />
                  </div>
                )
              ) : roster.error ? (
                <ErrorPanel error={roster.error} onRetry={roster.reload} />
              ) : roster.initial || !roster.data ? (
                <LoadingPanel label="Loading machine roster…" />
              ) : roster.data.items.length === 0 ? (
                <EmptyPanel label={`No machines found for ${scopeLabel}.`} />
              ) : (
                <div className={roster.loading ? "is-stale" : undefined}>
                  <MachineTable
                    items={roster.data.items}
                    selectedId={selectedId}
                    onSelect={setSelectedId}
                  />
                </div>
              )}
            </section>
          </>
        )}
      </main>

      {selectedId && <EquipmentDetail equipmentId={selectedId} onClose={closeDetail} />}
    </div>
  );
}
