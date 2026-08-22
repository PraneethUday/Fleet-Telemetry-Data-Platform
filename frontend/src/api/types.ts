/**
 * TypeScript mirror of the HTTP contract the FastAPI backend serves.
 *
 * These interfaces are hand-written from the agreed contract rather than
 * generated, because the frontend is built in parallel with the backend and
 * must compile before any server exists to introspect. They are the frontend's
 * half of the contract: if the API ever drifts, the mismatch shows up here as a
 * type error against the rendering code instead of as `undefined` in a table
 * cell at runtime.
 *
 * Nullability follows the gold Parquet schemas in pipeline/schemas.py. Every
 * column declared `nullable=True` there is `| null` here, because the backend
 * serialises NaN/NaT as JSON null.
 */

export type Severity = "critical" | "warning";

/** GET /api/health */
export interface HealthResponse {
  status: string;
  lake_backend: string;
  gold_tables: Record<string, boolean>;
}

/** One element of GET /api/fleet-summary -> by_type (gold fleet_summary_by_type). */
export interface FleetTypeSummary {
  equipment_type: string;
  machine_count: number;
  active_machines_24h: number;
  avg_coolant_temp_c: number | null;
  max_coolant_temp_c: number | null;
  avg_oil_pressure_psi: number | null;
  total_operating_hours: number | null;
  avg_fuel_burn_pct_per_hour: number | null;
  total_fault_codes: number;
  machines_flagged: number;
  pct_flagged: number;
  critical_count: number;
  warning_count: number;
}

/** GET /api/fleet-summary */
export interface FleetSummary {
  generated_at_utc: string;
  fleet_size: number;
  machines_flagged: number;
  critical_count: number;
  warning_count: number;
  days_covered: number;
  by_type: FleetTypeSummary[];
}

/** One row of gold fleet_health_flags, as returned by /api/health-flags. */
export interface HealthFlag {
  equipment_id: string;
  equipment_type: string;
  severity: Severity;
  health_score: number;
  /** Comma-separated rule names; the UI splits it into chips. */
  flag_reasons: string;
  temp_trend_c_per_hour: number | null;
  recent_avg_temp_c: number | null;
  recent_max_temp_c: number | null;
  recent_min_oil_psi: number | null;
  recent_fault_count: number;
  distinct_fault_codes: number;
  top_fault_code: string | null;
  readings_in_window: number;
  hours_since_last_reading: number | null;
  last_seen_utc: string | null;
  last_gps_lat: number | null;
  last_gps_lon: number | null;
  window_hours: number;
  generated_at_utc: string;
}

/** GET /api/health-flags */
export interface HealthFlagsResponse {
  count: number;
  items: HealthFlag[];
}

/** One element of GET /api/equipment -> items. */
export interface EquipmentListItem {
  equipment_id: string;
  equipment_type: string;
  last_day: string;
  /** null for machines that are not currently flagged. */
  severity: Severity | null;
  health_score: number | null;
}

/** GET /api/equipment */
export interface EquipmentListResponse {
  count: number;
  items: EquipmentListItem[];
}

/** One row of gold daily_equipment_summary. `day` arrives as "YYYY-MM-DD". */
export interface DailySummary {
  equipment_id: string;
  equipment_type: string;
  day: string;
  readings_count: number;
  under_load_readings: number;
  avg_coolant_temp_c: number | null;
  max_coolant_temp_c: number | null;
  p95_coolant_temp_c: number | null;
  avg_oil_pressure_psi: number | null;
  min_oil_pressure_psi: number | null;
  engine_hours_start: number | null;
  engine_hours_end: number | null;
  operating_hours: number | null;
  fuel_consumed_pct: number | null;
  fuel_burn_pct_per_hour: number | null;
  refuel_events: number;
  fault_code_count: number;
  distinct_fault_codes: number;
  last_gps_lat: number | null;
  last_gps_lon: number | null;
}

/** GET /api/equipment/{id}/history */
export interface EquipmentHistory {
  equipment_id: string;
  equipment_type: string;
  /** Ascending by day. */
  days: DailySummary[];
  flag: HealthFlag | null;
}
