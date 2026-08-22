/**
 * Hand-rolled inline SVG trend line.
 *
 * Deliberately not a charting library. Recharts or Chart.js would add hundreds
 * of kilobytes and a render-loop dependency to draw what is, at this data
 * volume (one point per day per machine), a `<polyline>` with a min/max scale.
 * The dependency budget for this dashboard is react + react-dom.
 *
 * The SVG stretches to its container (preserveAspectRatio="none") because the
 * default "meet" would letterbox a wide panel and leave the trend line floating
 * in the middle of it; strokes opt out of that non-uniform scale so horizontal
 * runs do not render thinner than vertical ones.
 *
 * Gaps matter here: a day with no readings is a real operational signal — the
 * machine was off the network — so nulls break the line rather than being
 * interpolated over, which would invent a reading that never happened.
 */

import { fmtNumber, MISSING } from "../lib/format";

interface SparklineProps {
  values: Array<number | null>;
  /** One label per value, used for the per-point hover title (e.g. the day). */
  labels: string[];
  unit?: string;
  width?: number;
  height?: number;
}

interface Point {
  index: number;
  value: number;
  x: number;
  y: number;
}

export function Sparkline({
  values,
  labels,
  unit = "",
  width = 900,
  height = 96,
}: SparklineProps) {
  const padX = 6;
  const padY = 10;

  const finite = values
    .map((value, index) => ({ index, value }))
    .filter((entry): entry is { index: number; value: number } =>
      entry.value !== null && Number.isFinite(entry.value),
    );

  if (finite.length === 0) {
    return <p className="sparkline-empty">No readings to plot.</p>;
  }

  const min = Math.min(...finite.map((entry) => entry.value));
  const max = Math.max(...finite.map((entry) => entry.value));
  // A flat series has zero range; without padding it would divide by zero and
  // then draw along the very top edge. Centre it instead.
  const span = max - min || Math.max(Math.abs(max) * 0.1, 1);
  const floor = max === min ? min - span / 2 : min;

  const lastIndex = Math.max(values.length - 1, 1);
  const scaleX = (index: number) =>
    padX + (index / lastIndex) * (width - padX * 2);
  const scaleY = (value: number) =>
    height - padY - ((value - floor) / span) * (height - padY * 2);

  const points: Point[] = finite.map((entry) => ({
    ...entry,
    x: scaleX(entry.index),
    y: scaleY(entry.value),
  }));

  // Split into runs of consecutive days so missing days leave a visible gap.
  const segments: Point[][] = [];
  for (const point of points) {
    const current = segments[segments.length - 1];
    if (current && point.index === current[current.length - 1].index + 1) {
      current.push(point);
    } else {
      segments.push([point]);
    }
  }

  const last = points[points.length - 1];
  const lastValue = values[values.length - 1];

  return (
    <figure className="sparkline">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        role="img"
        aria-label={`Trend of ${labels.length} points, minimum ${fmtNumber(min)}${unit}, maximum ${fmtNumber(max)}${unit}`}
        preserveAspectRatio="none"
      >
        <line
          className="sparkline-grid"
          vectorEffect="non-scaling-stroke"
          x1={0}
          x2={width}
          y1={scaleY(max)}
          y2={scaleY(max)}
        />
        <line
          className="sparkline-grid"
          vectorEffect="non-scaling-stroke"
          x1={0}
          x2={width}
          y1={scaleY(floor)}
          y2={scaleY(floor)}
        />

        {segments.map((segment) => {
          const path = segment.map((p) => `${p.x},${p.y}`).join(" ");
          return (
            <g key={`seg-${segment[0].index}`}>
              {segment.length > 1 && (
                <polygon
                  className="sparkline-area"
                  points={`${segment[0].x},${height - padY} ${path} ${segment[segment.length - 1].x},${height - padY}`}
                />
              )}
              <polyline
                className="sparkline-line"
                vectorEffect="non-scaling-stroke"
                points={path}
              />
            </g>
          );
        })}

        {points.map((point) => (
          <circle
            key={point.index}
            className={point === last ? "sparkline-dot is-last" : "sparkline-dot"}
            cx={point.x}
            cy={point.y}
            r={point === last ? 4 : 2.5}
          >
            <title>{`${labels[point.index] ?? point.index}: ${fmtNumber(point.value)}${unit}`}</title>
          </circle>
        ))}
      </svg>

      <figcaption className="sparkline-caption">
        <span>
          min <b>{fmtNumber(min)}</b>
          {unit}
        </span>
        <span>
          max <b>{fmtNumber(max)}</b>
          {unit}
        </span>
        <span>
          last <b>{lastValue === null ? MISSING : fmtNumber(lastValue)}</b>
          {lastValue === null ? "" : unit}
        </span>
        <span className="sparkline-range">
          {labels[0]} → {labels[labels.length - 1]}
        </span>
      </figcaption>
    </figure>
  );
}
