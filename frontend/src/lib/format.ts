/**
 * Display formatting for an operations console.
 *
 * The rule throughout: a missing value renders as an em dash, never as "null",
 * "NaN" or "0". Zero is a real reading (a parked machine really does report
 * 0 psi) and must stay distinguishable from "the sensor said nothing".
 */

export const MISSING = "—";

export function fmtNumber(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return value.toFixed(digits);
}

export function fmtInt(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return Math.round(value).toLocaleString("en-US");
}

export function fmtPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return `${value.toFixed(digits)}%`;
}

/**
 * Renders an instant in UTC, always.
 *
 * A fleet spans time zones and the pipeline stores everything in UTC, so
 * localising to the viewer's browser would mean two dispatchers looking at the
 * same screen read different times for the same event. The trailing Z is kept
 * so the displayed value is unambiguous rather than merely consistent.
 */
export function fmtUtc(iso: string | null | undefined): string {
  if (!iso) return MISSING;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) return iso; // show what we got, don't lie
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${parsed.getUTCFullYear()}-${pad(parsed.getUTCMonth() + 1)}-${pad(parsed.getUTCDate())} ` +
    `${pad(parsed.getUTCHours())}:${pad(parsed.getUTCMinutes())}Z`
  );
}

/** Signed, so a rising coolant trend reads as "+1.8" not "1.8". */
export function fmtSigned(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

/** "coolant_rising, low_oil_pressure" -> ["coolant rising", "low oil pressure"] */
export function splitReasons(reasons: string | null | undefined): string[] {
  if (!reasons) return [];
  return reasons
    .split(",")
    .map((reason) => reason.trim())
    .filter((reason) => reason.length > 0);
}

/** Turns a snake_case machine class or rule name into something readable. */
export function humanise(value: string): string {
  return value.replace(/_/g, " ");
}
