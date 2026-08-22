/**
 * The screens nobody designs and everybody sees.
 *
 * A dashboard whose backend is down is the single most common state during
 * development, and "Failed to fetch" in a console is a dead end. The error
 * panel therefore names the URL it tried, distinguishes "nothing answered"
 * from "the server said no", and prints the exact command that starts the
 * backend so recovery is a copy-paste rather than a hunt through the README.
 */

import { ApiError, isApiError } from "../api/client";
import { API_BASE_URL, BACKEND_START_COMMAND } from "../config";

export function LoadingPanel({ label }: { label: string }) {
  return (
    <div className="state-panel" role="status" aria-live="polite">
      <span className="spinner" aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}

export function EmptyPanel({ label }: { label: string }) {
  return <div className="state-panel state-panel-empty">{label}</div>;
}

function describe(error: unknown): { title: string; message: string; unreachable: boolean } {
  if (isApiError(error)) {
    const apiError: ApiError = error;
    if (apiError.kind === "network") {
      return { title: "API unreachable", message: apiError.message, unreachable: true };
    }
    return {
      title: apiError.status ? `API error ${apiError.status}` : "API error",
      message: apiError.message,
      unreachable: false,
    };
  }
  return {
    title: "Unexpected error",
    message: error instanceof Error ? error.message : String(error),
    unreachable: false,
  };
}

export function ErrorPanel({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const { title, message, unreachable } = describe(error);

  return (
    <div className="state-panel state-panel-error" role="alert">
      <div className="error-head">
        <strong>{title}</strong>
        {onRetry && (
          <button type="button" className="btn" onClick={onRetry}>
            Retry
          </button>
        )}
      </div>
      <p>{message}</p>

      {unreachable && (
        <>
          <p className="muted">
            The dashboard is pointed at <code>{API_BASE_URL}</code>. Start the API from the
            repository root:
          </p>
          <pre className="command">{BACKEND_START_COMMAND}</pre>
          <p className="muted">
            If the backend listens elsewhere, set <code>VITE_API_BASE_URL</code> in{" "}
            <code>frontend/.env.local</code> and restart the dev server. If it is running,
            check that this origin is listed in the backend&apos;s <code>CORS_ORIGINS</code>.
          </p>
        </>
      )}
    </div>
  );
}
