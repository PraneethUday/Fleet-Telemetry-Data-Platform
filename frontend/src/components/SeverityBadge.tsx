/**
 * Severity is the one thing on this screen that gets read at a glance from
 * across a room, so it is a filled block with a text label rather than a bare
 * coloured dot: colour alone fails for the ~8% of men with red/green colour
 * blindness, and a dispatcher should never have to hover to learn that a
 * machine is critical.
 */

import type { Severity } from "../api/types";
import { MISSING } from "../lib/format";

export function SeverityBadge({ severity }: { severity: Severity | null }) {
  if (!severity) return <span className="muted">{MISSING}</span>;
  return <span className={`badge badge-${severity}`}>{severity}</span>;
}
