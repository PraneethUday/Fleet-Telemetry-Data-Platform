# Fleet Telemetry — dashboard

Vite + React 18 + TypeScript. Reads the gold layer through the FastAPI backend;
it never touches the lakehouse itself, so the dashboard has no idea whether the
Parquet behind it lives on a laptop or in Azure Blob Storage.

## Run

```bash
cd frontend
npm install
npm run dev          # http://localhost:5173
```

The backend must be running separately (default `http://localhost:8000`).

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `VITE_API_BASE_URL` | `http://localhost:8000` | Base URL of the FastAPI backend |
| `VITE_BACKEND_START_COMMAND` | `./.venv/bin/uvicorn backend.main:app --reload --port 8000` | Printed in the "API unreachable" panel |

Copy `.env.example` to `.env.local` to override. Vite inlines `VITE_*` at build
time, so a bundle built for localhost must be rebuilt to point at Azure — that
is the trade for shipping as static files with no server of its own.

## Scripts

- `npm run dev` — dev server with HMR
- `npm run build` — typecheck (`tsc --noEmit`) then production build into `dist/`
- `npm run typecheck` — types only
- `npm run preview` — serve the built `dist/`

## Notes

Dependencies are `react` and `react-dom` plus the Vite/TypeScript toolchain, and
nothing else. The trend chart in the machine detail drawer is hand-rolled inline
SVG (`src/components/Sparkline.tsx`) rather than a charting library: at one
point per machine per day it is a `<polyline>`, and a chart library would have
been the largest thing in the bundle.
