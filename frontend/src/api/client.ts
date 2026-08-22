/**
 * The single place that speaks HTTP.
 *
 * Components never call fetch directly. Two reasons: the base URL and query
 * string encoding stay in one testable place, and — more importantly — every
 * failure mode is normalised into one `ApiError` shape. `fetch` is hostile
 * about this by default: it rejects with a bare TypeError when the host is
 * down, and it *resolves* on a 500, so naive callers show "success" for an
 * error page. Distinguishing "backend not running" from "backend returned 503"
 * is what lets the UI print a fix-it command instead of a generic red box.
 */

import { API_BASE_URL } from "../config";
import type {
  EquipmentHistory,
  EquipmentListResponse,
  FleetSummary,
  HealthFlagsResponse,
  HealthResponse,
  Severity,
} from "./types";

/**
 * "network" means we never reached the server (down, wrong port, CORS refusal);
 * "http" means the server answered and said no; "parse" means it answered with
 * something that is not JSON. Only "network" is fixed by starting the backend,
 * so the UI branches on this to decide whether to print the start command.
 */
export type ApiErrorKind = "network" | "http" | "parse";

export class ApiError extends Error {
  readonly kind: ApiErrorKind;
  readonly status: number | null;

  constructor(kind: ApiErrorKind, message: string, status: number | null = null) {
    super(message);
    this.name = "ApiError";
    this.kind = kind;
    this.status = status;
  }
}

export function isApiError(value: unknown): value is ApiError {
  return value instanceof ApiError;
}

type QueryValue = string | number | boolean | undefined | null;

function buildUrl(path: string, query?: Record<string, QueryValue>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(query ?? {})) {
    // Undefined means "omit this filter" — sending `equipment_type=undefined`
    // would have the backend filter for a machine class literally named that.
    if (value === undefined || value === null || value === "") continue;
    search.set(key, String(value));
  }
  const qs = search.toString();
  return `${API_BASE_URL}${path}${qs ? `?${qs}` : ""}`;
}

async function request<T>(path: string, query?: Record<string, QueryValue>): Promise<T> {
  const url = buildUrl(path, query);

  let response: Response;
  try {
    response = await fetch(url, { headers: { Accept: "application/json" } });
  } catch {
    // A browser reports a refused connection and a blocked CORS preflight
    // identically, so name both possibilities rather than guessing wrong.
    throw new ApiError(
      "network",
      `Could not reach the API at ${API_BASE_URL}. The backend is not running, ` +
        `is on a different port, or has not allowed this origin via CORS_ORIGINS.`,
    );
  }

  if (!response.ok) {
    // FastAPI puts the human-readable reason in {"detail": ...}; fall back to
    // the status line when the body is empty or is an HTML error page.
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body: unknown = await response.json();
      if (body && typeof body === "object" && "detail" in body) {
        detail = String((body as { detail: unknown }).detail);
      }
    } catch {
      /* non-JSON error body — the status line is the best we have */
    }
    throw new ApiError("http", detail, response.status);
  }

  try {
    return (await response.json()) as T;
  } catch {
    // The contract requires NaN to be serialised as null precisely because
    // bare NaN is invalid JSON and lands here.
    throw new ApiError("parse", `The API returned a body that is not valid JSON (${path}).`);
  }
}

export function fetchHealth(): Promise<HealthResponse> {
  return request<HealthResponse>("/api/health");
}

export function fetchFleetSummary(): Promise<FleetSummary> {
  return request<FleetSummary>("/api/fleet-summary");
}

export function fetchHealthFlags(params: {
  equipmentType?: string;
  severity?: Severity;
  limit?: number;
}): Promise<HealthFlagsResponse> {
  return request<HealthFlagsResponse>("/api/health-flags", {
    equipment_type: params.equipmentType,
    severity: params.severity,
    limit: params.limit,
  });
}

export function fetchEquipment(params: {
  equipmentType?: string;
  flaggedOnly?: boolean;
  limit?: number;
}): Promise<EquipmentListResponse> {
  return request<EquipmentListResponse>("/api/equipment", {
    equipment_type: params.equipmentType,
    flagged_only: params.flaggedOnly,
    limit: params.limit,
  });
}

export function fetchEquipmentHistory(
  equipmentId: string,
  days = 30,
): Promise<EquipmentHistory> {
  // encodeURIComponent because equipment ids come from data, not from a
  // whitelist — a stray "/" would otherwise silently change the route.
  return request<EquipmentHistory>(
    `/api/equipment/${encodeURIComponent(equipmentId)}/history`,
    { days },
  );
}
