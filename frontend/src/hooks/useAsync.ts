/**
 * One async-data hook, used by every panel that talks to the API.
 *
 * Written by hand rather than pulled from a data-fetching library because the
 * dependency budget for this dashboard is react + react-dom, and because the
 * two behaviours that actually matter here are both a few lines:
 *
 *  - a sequence guard, so a slow response for filters the user has already
 *    moved off cannot overwrite the results of a later, faster request;
 *  - keeping the previous data visible while refetching, so changing a filter
 *    dims the table instead of collapsing the layout to a spinner and back.
 */

import type { DependencyList } from "react";
import { useCallback, useEffect, useRef, useState } from "react";

export interface AsyncState<T> {
  data: T | null;
  error: unknown;
  loading: boolean;
  /** True on the very first load, when there is no stale data to show yet. */
  initial: boolean;
}

export interface AsyncResult<T> extends AsyncState<T> {
  reload: () => void;
}

export function useAsync<T>(load: () => Promise<T>, deps: DependencyList): AsyncResult<T> {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    error: null,
    loading: true,
    initial: true,
  });

  // Bumped by reload() to re-run the effect without changing the caller's deps.
  const [reloadCount, setReloadCount] = useState(0);

  // Monotonic request id. Only the newest in-flight request may write state.
  const latestRequest = useRef(0);

  // `load` is intentionally not a dependency: callers pass an inline closure,
  // which is a new function identity on every render, so depending on it would
  // refetch forever. The caller's `deps` array is the real trigger.
  useEffect(() => {
    const requestId = ++latestRequest.current;

    setState((previous) => ({
      data: previous.data,
      error: null,
      loading: true,
      initial: previous.data === null,
    }));

    load().then(
      (data) => {
        if (requestId !== latestRequest.current) return;
        setState({ data, error: null, loading: false, initial: false });
      },
      (error: unknown) => {
        if (requestId !== latestRequest.current) return;
        // Drop stale data on error: showing yesterday's numbers next to an
        // error banner invites someone to act on figures that are no longer
        // known to be true.
        setState({ data: null, error, loading: false, initial: false });
      },
    );
  }, [...deps, reloadCount]);

  const reload = useCallback(() => setReloadCount((n) => n + 1), []);

  return { ...state, reload };
}
