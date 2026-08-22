/**
 * Deployment-dependent settings, resolved once at module load.
 *
 * The dashboard runs against a backend on localhost during development and
 * against a Container App URL in Azure, so the base URL cannot be a literal in
 * the fetch calls. Everything host-specific is funnelled through this module,
 * which keeps the "no hardcoded endpoints" rule enforceable by reading one file.
 */

// A trailing slash would join into "http://host//api/health". Servers usually
// tolerate that; some proxies do not, and it makes the network tab confusing.
const rawBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export const API_BASE_URL: string = rawBaseUrl.replace(/\/+$/, "");

// Displayed verbatim when the API cannot be reached. A dashboard that says
// "failed to fetch" leaves the reader guessing; one that prints the command
// that fixes it is the difference between a dead end and a two-second recovery.
export const BACKEND_START_COMMAND: string =
  import.meta.env.VITE_BACKEND_START_COMMAND ??
  "make backend   # or: ./.venv/bin/python -m uvicorn backend.app.main:app --port 8000";
