"""Physics-flavoured fleet telemetry simulator.

Design notes
------------
**Why not faker.** Faker produces independent random values. Telemetry is the
opposite: a machine's coolant temperature at 06:05 is mostly determined by what
it was at 06:00. Without that continuity the silver layer has nothing to
deduplicate meaningfully and the gold layer has no trend to detect, so the whole
point of the lakehouse collapses. This module carries per-machine state forward
tick by tick instead.

**Why vectorised.** State is held as parallel numpy arrays over the fleet rather
than a list of objects. One tick is a handful of array operations regardless of
fleet size, so 500 machines x 288 ticks/day stays in the low seconds. It is also
how you would actually write this at scale.

**The degradation model.** Roughly 9% of machines are put on a failure
trajectory: a mode is chosen (overheating, oil pressure loss, fuel delivery,
aftertreatment, electrical), a failure time is scheduled, and a severity ramp
runs for 18-40 hours before it. Severity does three things at once — it pushes
the relevant sensor away from nominal, it raises the fault-code rate
super-linearly so codes *cluster* before the event, and it eventually triggers
an unplanned-downtime window. Machines whose ramp is still climbing when the
simulation window ends are exactly the population the gold-layer
`fleet_health_flags` rules are meant to surface.
"""

from __future__ import annotations

import hashlib
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

import numpy as np
import pandas as pd

from pipeline.generator.profiles import (
    EQUIPMENT_PROFILES,
    FAULT_CODES_BY_SYMPTOM,
    NUISANCE_SYMPTOMS,
    SITES,
    TYPE_VARIANTS,
)
from pipeline.schemas import BRONZE_TELEMETRY_COLUMNS

# Failure modes and how often each is the cause. Overheating and oil-pressure
# loss dominate because they are the two that show clearly in the sensor trend,
# which keeps the rule-based gold layer honest.
MODES = ("overheat", "low_oil_pressure", "fuel", "aftertreatment", "electrical")
MODE_WEIGHTS = np.array([0.34, 0.25, 0.12, 0.17, 0.12])

DEG_PER_KM = 1.0 / 111.0  # crude but fine at these latitudes
THERMAL_TAU_S = 900.0  # coolant approaches its target with ~15 min inertia


@dataclass(frozen=True)
class GeneratorConfig:
    """Knobs for one generator run."""

    fleet_size: int = 500
    seed: int = 42
    interval_seconds: int = 300  # one telemetry reading per machine per 5 min
    batch_minutes: int = 60  # simulated time covered by one Parquet file

    # --- data-quality injection rates (bronze must NOT be clean) -----------
    dropout_rate: float = 0.012  # gateway offline, reading never arrives
    duplicate_rate: float = 0.004  # at-least-once delivery, exact resend
    late_retransmit_rate: float = 0.002  # resent in a LATER batch, new batch id
    null_rate: float = 0.010  # a sensor field arrives empty
    out_of_range_rate: float = 0.003  # physically impossible sensor value
    type_variant_rate: float = 0.045  # vendor-specific equipment_type spelling
    degrading_fraction: float = 0.09  # share of fleet on a failure trajectory

    def interval_hours(self) -> float:
        return self.interval_seconds / 3600.0

    def ticks_per_batch(self) -> int:
        return max(1, int(self.batch_minutes * 60 // self.interval_seconds))


@dataclass
class Batch:
    """One Parquet file's worth of readings, already tagged for its partition."""

    partition_date: str  # "2026-08-22" -> raw/dt=2026-08-22/
    batch_start: datetime
    batch_end: datetime
    batch_id: str
    frame: pd.DataFrame

    @property
    def filename(self) -> str:
        return f"batch_{self.batch_start.strftime('%H%M')}_{self.batch_id[-8:]}.parquet"


def _stable_seed(*parts: str) -> int:
    """Deterministic 32-bit seed from strings; reproducible across processes."""
    digest = hashlib.sha256("|".join(parts).encode()).digest()
    return int.from_bytes(digest[:4], "big")


class FleetSimulator:
    """Simulates a fleet forward in time, yielding one Batch per file."""

    def __init__(self, config: GeneratorConfig | None = None):
        self.cfg = config or GeneratorConfig()
        self.rng = np.random.default_rng(self.cfg.seed)
        self._build_roster()
        self._planned = False
        # Rows held back to be re-sent in the following batch, simulating a
        # gateway that buffered during a dropout and flushed late.
        self._pending_retransmits: pd.DataFrame | None = None

    # -----------------------------------------------------------------
    # Roster
    # -----------------------------------------------------------------
    def _build_roster(self) -> None:
        n = self.cfg.fleet_size
        rng = self.rng

        counts = [max(1, int(round(p.fleet_share * n))) for p in EQUIPMENT_PROFILES]
        counts[0] += n - sum(counts)  # absorb rounding drift into the largest class

        ids: list[str] = []
        type_idx: list[int] = []
        for ti, (prof, count) in enumerate(zip(EQUIPMENT_PROFILES, counts)):
            for k in range(1, count + 1):
                ids.append(f"{prof.id_prefix}-{k:04d}")
                type_idx.append(ti)

        self.equipment_id = np.array(ids, dtype=object)
        self.type_idx = np.array(type_idx, dtype=np.int64)
        self.n = len(ids)

        prof_of = [EQUIPMENT_PROFILES[i] for i in type_idx]
        self.equipment_type = np.array([p.equipment_type for p in prof_of], dtype=object)
        self.nominal_temp = np.array([p.nominal_temp_c for p in prof_of])
        self.temp_noise = np.array([p.temp_noise_c for p in prof_of])
        self.nominal_oil = np.array([p.nominal_oil_psi for p in prof_of])
        self.oil_noise = np.array([p.oil_noise_psi for p in prof_of])
        self.burn_rate = np.array([p.fuel_burn_pct_per_hour for p in prof_of])
        self.mobile = np.array([p.mobile for p in prof_of])
        self.wander_deg = np.array([p.wander_km * DEG_PER_KM for p in prof_of])
        self.continuous = np.array([p.duty == "continuous" for p in prof_of])

        site_idx = rng.integers(0, len(SITES), size=self.n)
        self.site_idx = site_idx
        self.site_lat = np.array([SITES[i].lat for i in site_idx])
        self.site_lon = np.array([SITES[i].lon for i in site_idx])
        self.ambient = np.array([SITES[i].ambient_c for i in site_idx])
        self.ops_24h = np.array([SITES[i].ops == "24h" for i in site_idx])
        self.utc_offset = np.array([SITES[i].utc_offset_hours for i in site_idx])

        # --- initial dynamic state (a used fleet, not 500 brand-new machines)
        self.engine_hours = rng.uniform(320.0, 14500.0, self.n).round(1)
        self.fuel_pct = rng.uniform(22.0, 97.0, self.n)
        self.coolant = self.ambient + rng.normal(2.0, 1.5, self.n)
        self.oil_psi = np.zeros(self.n)
        self.lat = self.site_lat + rng.normal(0, 0.004, self.n)
        self.lon = self.site_lon + rng.normal(0, 0.004, self.n)
        self.running = rng.random(self.n) < 0.55
        self.refuel_at = rng.uniform(7.0, 16.0, self.n)

        # --- failure plan placeholders, filled by plan_failures()
        self.degrades = np.zeros(self.n, dtype=bool)
        self.mode_idx = np.zeros(self.n, dtype=np.int64)
        # `degrades` is cleared when a machine comes back from the workshop, so
        # it answers "is this machine failing right now". The planned_* copies
        # answer "was this machine ever put on a failure trajectory", which is
        # what tests and documentation need — otherwise a machine that failed
        # and was repaired reads as healthy-all-along and quietly poisons any
        # comparison between the sick and healthy populations.
        self.planned_degrades = np.zeros(self.n, dtype=bool)
        self.planned_mode_idx = np.zeros(self.n, dtype=np.int64)
        self.fail_epoch = np.full(self.n, np.inf)
        self.ramp_seconds = np.zeros(self.n)
        self.repair_seconds = np.zeros(self.n)
        self.down_until = np.full(self.n, -np.inf)

    def plan_failures(self, start: datetime, end: datetime) -> None:
        """Schedule which machines degrade, how, and when they let go.

        Failure times are drawn across ``[start + 25% of window, start + 160% of
        window]``. Deliberately overshooting the end of the window leaves a
        cohort of machines still mid-ramp when the data stops — those are the
        ones that should appear in `fleet_health_flags` as "needs attention now"
        rather than "already broke".
        """
        if self._planned:
            return
        rng = self.rng
        span = max((end - start).total_seconds(), 3600.0)
        t0 = start.timestamp()

        self.degrades = rng.random(self.n) < self.cfg.degrading_fraction
        self.mode_idx = rng.choice(len(MODES), size=self.n, p=MODE_WEIGHTS)
        offsets = rng.uniform(0.25 * span, 1.60 * span, self.n)
        self.fail_epoch = np.where(self.degrades, t0 + offsets, np.inf)
        self.ramp_seconds = rng.uniform(18 * 3600, 40 * 3600, self.n)
        self.repair_seconds = rng.uniform(6 * 3600, 30 * 3600, self.n)
        self.planned_degrades = self.degrades.copy()
        self.planned_mode_idx = self.mode_idx.copy()
        self._planned = True

    def roster(self) -> pd.DataFrame:
        """Static fleet registry — handy for tests and for the README."""
        return pd.DataFrame(
            {
                "equipment_id": self.equipment_id,
                "equipment_type": self.equipment_type,
                "site_id": [SITES[i].site_id for i in self.site_idx],
                "planned_failure_mode": np.where(
                    self.planned_degrades,
                    [MODES[i] for i in self.planned_mode_idx],
                    None,
                ),
                "currently_degrading": self.degrades,
            }
        )

    # -----------------------------------------------------------------
    # One tick
    # -----------------------------------------------------------------
    def _severity(self, t: float) -> np.ndarray:
        """0.0 healthy -> 1.0 at the point of failure, then held at 1 while down."""
        ramp_start = self.fail_epoch - self.ramp_seconds
        with np.errstate(invalid="ignore"):
            sev = (t - ramp_start) / np.maximum(self.ramp_seconds, 1.0)
        sev = np.clip(np.nan_to_num(sev, nan=0.0, posinf=0.0, neginf=0.0), 0.0, 1.0)
        return np.where(self.degrades, sev, 0.0)

    def _activity_probability(self, t: float) -> np.ndarray:
        """How likely each machine is to be under load right now.

        Mining sites run around the clock with a dip at each 12-hour shift
        change; civil sites run daylight hours on weekdays; gensets never stop.
        """
        local_hour = ((t + self.utc_offset * 3600) / 3600.0) % 24.0
        local_dow = int(
            ((t / 86400.0) + 4) % 7
        )  # 1970-01-01 was a Thursday; 5,6 = weekend

        p = np.empty(self.n)

        # 24-hour mining operations
        shift_change = (np.abs(local_hour - 6.0) < 0.6) | (
            np.abs(local_hour - 18.0) < 0.6
        )
        p_mine = np.where(shift_change, 0.35, 0.90)

        # Daylight civil construction
        in_day = (local_hour >= 7.0) & (local_hour < 17.0)
        p_day = np.where(in_day, 0.85, 0.03)
        if local_dow >= 5:
            p_day = p_day * 0.15

        p = np.where(self.ops_24h, p_mine, p_day)
        return np.where(self.continuous, 0.98, p)

    def _step(self, t: float) -> dict[str, np.ndarray]:
        """Advance every machine one interval and return this tick's readings."""
        rng = self.rng
        cfg = self.cfg
        dt_h = cfg.interval_hours()
        sev = self._severity(t)
        mode = self.mode_idx

        # --- failure / repair state machine -------------------------------
        just_failed = (t >= self.fail_epoch) & (self.down_until == -np.inf)
        self.down_until = np.where(
            just_failed, t + self.repair_seconds, self.down_until
        )
        repaired = (self.down_until != -np.inf) & (t >= self.down_until)
        if repaired.any():
            # Back from the workshop: fault cleared, machine healthy again.
            self.down_until = np.where(repaired, -np.inf, self.down_until)
            self.fail_epoch = np.where(repaired, np.inf, self.fail_epoch)
            self.degrades = self.degrades & ~repaired
            sev = np.where(repaired, 0.0, sev)
        is_down = self.down_until != -np.inf

        # --- run / stop with persistence ----------------------------------
        p_active = self._activity_probability(t)
        p_stay = 0.55 + 0.44 * p_active
        p_start = 0.55 * p_active
        draw = rng.random(self.n)
        running = np.where(self.running, draw < p_stay, draw < p_start)
        running = running & ~is_down  # a machine in the workshop is not working
        self.running = running

        # --- engine hour meter (monotonic, never decreases) ---------------
        self.engine_hours = self.engine_hours + running * dt_h

        # --- fuel ----------------------------------------------------------
        fuel_penalty = np.where(mode == MODES.index("fuel"), 1.0 + 0.45 * sev, 1.0)
        self.fuel_pct = self.fuel_pct - running * self.burn_rate * dt_h * fuel_penalty
        refuel = self.fuel_pct <= self.refuel_at
        if refuel.any():
            self.fuel_pct = np.where(
                refuel, rng.uniform(92.0, 100.0, self.n), self.fuel_pct
            )
            self.refuel_at = np.where(
                refuel, rng.uniform(7.0, 16.0, self.n), self.refuel_at
            )
        self.fuel_pct = np.clip(self.fuel_pct, 0.0, 100.0)

        # --- coolant temperature with thermal inertia ----------------------
        overheat_offset = np.where(mode == MODES.index("overheat"), 30.0 * sev, 0.0)
        # A struggling aftertreatment system also runs hot, just less so.
        overheat_offset += np.where(
            mode == MODES.index("aftertreatment"), 9.0 * sev, 0.0
        )
        target = np.where(
            running, self.nominal_temp + overheat_offset, self.ambient + 2.0
        )
        alpha = 1.0 - math.exp(-cfg.interval_seconds / THERMAL_TAU_S)
        self.coolant = self.coolant + (target - self.coolant) * alpha
        self.coolant += rng.normal(0.0, self.temp_noise * 0.45, self.n)

        # Transient spikes: rare on healthy machines, common on failing ones.
        # This is what stops "one hot reading" from being a usable rule and
        # forces the gold layer to look at a trend instead.
        spike_p = 0.0004 + 0.020 * np.where(mode == MODES.index("overheat"), sev, 0.0)
        spikes = rng.random(self.n) < spike_p
        if spikes.any():
            self.coolant = self.coolant + spikes * rng.uniform(12.0, 34.0, self.n)
        self.coolant = np.clip(self.coolant, self.ambient - 6.0, 142.0)

        # --- oil pressure ---------------------------------------------------
        oil_drop = np.where(mode == MODES.index("low_oil_pressure"), 26.0 * sev, 0.0)
        # Hot oil thins out and loses pressure — a second-order coupling that
        # makes overheating machines look bad on two sensors, as they should.
        oil_drop += np.clip((self.coolant - 105.0) / 2.0, 0.0, 12.0)
        oil = self.nominal_oil - oil_drop + rng.normal(0.0, self.oil_noise, self.n)
        self.oil_psi = np.where(running, np.maximum(oil, 0.0), 0.0)

        # --- GPS random walk, bounded to the machine's work area -----------
        moving = running & self.mobile
        self.lat = self.lat + moving * rng.normal(0.0, 0.00035, self.n)
        self.lon = self.lon + moving * rng.normal(0.0, 0.00035, self.n)
        d_lat = self.lat - self.site_lat
        d_lon = self.lon - self.site_lon
        dist = np.sqrt(d_lat**2 + d_lon**2)
        limit = np.maximum(self.wander_deg, 1e-9)
        too_far = dist > limit
        if too_far.any():
            scale = np.where(too_far, limit / np.maximum(dist, 1e-12), 1.0)
            self.lat = self.site_lat + d_lat * scale
            self.lon = self.site_lon + d_lon * scale

        # --- fault codes ----------------------------------------------------
        fault_code = self._draw_fault_codes(sev, is_down, running)

        return {
            "equipment_id": self.equipment_id,
            "equipment_type": self.equipment_type,
            "engine_hours": np.round(self.engine_hours, 2),
            "fuel_level_pct": np.round(self.fuel_pct, 2),
            "coolant_temp_c": np.round(self.coolant, 2),
            "oil_pressure_psi": np.round(self.oil_psi, 2),
            "gps_lat": np.round(self.lat, 6),
            "gps_lon": np.round(self.lon, 6),
            "fault_code": fault_code,
            "_utc_offset": self.utc_offset,
        }

    def _draw_fault_codes(
        self, sev: np.ndarray, is_down: np.ndarray, running: np.ndarray
    ) -> np.ndarray:
        """Emit DTCs. Mostly null, clustered before failure, correlated with mode."""
        rng = self.rng

        # sev ** 2.4 makes codes rare early in the ramp and dense at the end —
        # the "clustering before a failure event" the analytics layer detects.
        p = 0.0006 + 0.004 * (sev > 0) + 0.30 * np.power(sev, 2.4)
        p = np.where(is_down, 0.45, p)  # a downed machine holds an active fault
        p = np.where(running | is_down, p, p * 0.15)  # parked machines say little

        fires = rng.random(self.n) < p
        codes = np.full(self.n, None, dtype=object)
        if not fires.any():
            return codes

        # Failing machines report codes for their own failure mode; everyone
        # else reports background nuisance faults.
        symptomatic = fires & ((sev > 0) | is_down)
        for mi, mode_name in enumerate(MODES):
            mask = symptomatic & (self.mode_idx == mi)
            if not mask.any():
                continue
            pool = FAULT_CODES_BY_SYMPTOM[mode_name]
            picks = rng.integers(0, len(pool), size=int(mask.sum()))
            codes[mask] = [pool[i] for i in picks]

        nuisance = fires & ~symptomatic
        if nuisance.any():
            pool = [c for s in NUISANCE_SYMPTOMS for c in FAULT_CODES_BY_SYMPTOM[s]]
            picks = rng.integers(0, len(pool), size=int(nuisance.sum()))
            codes[nuisance] = [pool[i] for i in picks]

        return codes

    # -----------------------------------------------------------------
    # Batching
    # -----------------------------------------------------------------
    def simulate(self, start: datetime, end: datetime) -> Iterator[Batch]:
        """Yield one Batch per ``batch_minutes`` of simulated time."""
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware (UTC)")
        self.plan_failures(start, end)

        step = timedelta(seconds=self.cfg.interval_seconds)
        batch_span = timedelta(minutes=self.cfg.batch_minutes)

        batch_start = start
        while batch_start < end:
            batch_end = min(batch_start + batch_span, end)
            tick_arrays: list[dict[str, np.ndarray]] = []
            tick_times: list[datetime] = []

            t = batch_start
            while t < batch_end:
                tick_arrays.append(self._step(t.timestamp()))
                tick_times.append(t)
                t += step

            if tick_arrays:
                frame = self._assemble(tick_arrays, tick_times, batch_end)
                for part_date, part in frame.groupby("_partition_date", sort=True):
                    yield Batch(
                        partition_date=str(part_date),
                        batch_start=batch_start,
                        batch_end=batch_end,
                        batch_id=part["_ingest_batch_id"].iloc[0],
                        frame=part.drop(columns=["_partition_date"]).reset_index(
                            drop=True
                        ),
                    )
            batch_start = batch_end

    def _assemble(
        self,
        tick_arrays: list[dict[str, np.ndarray]],
        tick_times: list[datetime],
        batch_end: datetime,
    ) -> pd.DataFrame:
        """Stack ticks into one frame, then deliberately dirty it."""
        rng = self.rng
        cfg = self.cfg

        cols = {
            k: np.concatenate([ta[k] for ta in tick_arrays])
            for k in tick_arrays[0]
            if k != "_utc_offset"
        }
        offsets = np.concatenate([ta["_utc_offset"] for ta in tick_arrays])
        instants = np.concatenate(
            [np.full(self.n, tt.timestamp()) for tt in tick_times]
        )

        df = pd.DataFrame(cols)
        df["timestamp"] = self._format_timestamps(instants, offsets)
        df["_epoch"] = instants

        batch_id = uuid.UUID(bytes=bytes(rng.bytes(16))).hex
        ingested_at = batch_end + timedelta(seconds=float(rng.uniform(5, 90)))
        df["_ingest_batch_id"] = batch_id
        df["_ingested_at_utc"] = pd.Timestamp(ingested_at).tz_convert("UTC")

        df = self._inject_quality_issues(df, batch_id, ingested_at)

        df["_partition_date"] = pd.to_datetime(
            df["_epoch"], unit="s", utc=True
        ).dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["_epoch"])

        ordered = BRONZE_TELEMETRY_COLUMNS + [
            "_ingest_batch_id",
            "_ingested_at_utc",
            "_partition_date",
        ]
        return df[ordered]

    def _format_timestamps(
        self, instants: np.ndarray, offsets: np.ndarray
    ) -> np.ndarray:
        """Render each reading's time the way its gateway would have sent it.

        Four shapes appear in the raw feed. All four are recoverable, but only
        if something parses them — which is silver's job, not bronze's.
        """
        rng = self.rng
        n = len(instants)
        variant = rng.choice([0, 1, 2, 3], size=n, p=[0.80, 0.12, 0.05, 0.03])
        out = np.empty(n, dtype=object)

        # Cache the formatted strings per distinct instant: within a tick every
        # machine shares the same moment, so this is ~4 formats per tick, not n.
        cache: dict[tuple[float, float, int], str] = {}
        for i in range(n):
            key = (instants[i], offsets[i] if variant[i] == 1 else 0.0, variant[i])
            hit = cache.get(key)
            if hit is None:
                dt_utc = datetime.fromtimestamp(instants[i], tz=timezone.utc)
                v = variant[i]
                if v == 0:  # canonical ISO-8601 UTC
                    hit = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                elif v == 1:  # site-local time carrying its UTC offset
                    off = float(offsets[i])
                    local = dt_utc + timedelta(hours=off)
                    sign = "+" if off >= 0 else "-"
                    hh, mm = divmod(int(round(abs(off) * 60)), 60)
                    hit = local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hh:02d}:{mm:02d}"
                elif v == 2:  # gateway dropped the timezone marker entirely
                    hit = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                else:  # epoch milliseconds as a string
                    hit = str(int(instants[i] * 1000))
                cache[key] = hit
            out[i] = hit
        return out

    def _inject_quality_issues(
        self, df: pd.DataFrame, batch_id: str, ingested_at: datetime
    ) -> pd.DataFrame:
        """Make the raw zone look like a real raw zone.

        Every issue here is one the silver layer is explicitly built to handle.
        None of it is cosmetic: if bronze were clean, the transform step would be
        a no-op and the layered architecture would be decoration.
        """
        rng = self.rng
        cfg = self.cfg
        n = len(df)

        # 1. Gateway dropouts — readings that simply never arrive.
        keep = rng.random(n) >= cfg.dropout_rate
        df = df[keep].reset_index(drop=True)
        n = len(df)

        # 2. Vendor-specific equipment_type spellings.
        variant_mask = rng.random(n) < cfg.type_variant_rate
        if variant_mask.any():
            types = df["equipment_type"].to_numpy(copy=True)
            for canonical, variants in TYPE_VARIANTS.items():
                m = variant_mask & (types == canonical)
                if m.any():
                    picks = rng.integers(0, len(variants), size=int(m.sum()))
                    types[m] = [variants[i] for i in picks]
            df["equipment_type"] = types

        # 3. Empty sensor fields.
        sensor_cols = [
            "engine_hours",
            "fuel_level_pct",
            "coolant_temp_c",
            "oil_pressure_psi",
            "gps_lat",
            "gps_lon",
        ]
        null_mask = rng.random(n) < cfg.null_rate
        if null_mask.any():
            idx = np.flatnonzero(null_mask)
            which = rng.integers(0, len(sensor_cols), size=len(idx))
            for col_i, col in enumerate(sensor_cols):
                rows = idx[which == col_i]
                if len(rows):
                    df.loc[rows, col] = np.nan

        # 4. Physically impossible values from a failing sensor or a bad CAN read.
        oor_mask = rng.random(n) < cfg.out_of_range_rate
        if oor_mask.any():
            idx = np.flatnonzero(oor_mask)
            which = rng.integers(0, 4, size=len(idx))
            bad_fuel = idx[which == 0]
            bad_temp = idx[which == 1]
            bad_oil = idx[which == 2]
            bad_gps = idx[which == 3]
            if len(bad_fuel):
                df.loc[bad_fuel, "fuel_level_pct"] = rng.choice(
                    [-5.0, -1.2, 128.4, 255.0, 999.9], size=len(bad_fuel)
                )
            if len(bad_temp):
                df.loc[bad_temp, "coolant_temp_c"] = rng.choice(
                    [-273.0, -60.0, 850.0, 1023.0], size=len(bad_temp)
                )
            if len(bad_oil):
                df.loc[bad_oil, "oil_pressure_psi"] = rng.choice(
                    [-14.0, -1.0, 640.0], size=len(bad_oil)
                )
            if len(bad_gps):
                df.loc[bad_gps, "gps_lat"] = 0.0
                df.loc[bad_gps, "gps_lon"] = 0.0  # null island

        # 5. At-least-once delivery — the same reading arrives twice.
        dup_mask = rng.random(n) < cfg.duplicate_rate
        pieces = [df]
        if dup_mask.any():
            pieces.append(df[dup_mask].copy())

        # 6. Buffered rows flushed into the NEXT batch under a new batch id.
        #    Exact-duplicate detection misses these; silver has to dedup on the
        #    business key (equipment_id, timestamp) instead.
        if self._pending_retransmits is not None and len(self._pending_retransmits):
            late = self._pending_retransmits.copy()
            late["_ingest_batch_id"] = batch_id
            late["_ingested_at_utc"] = pd.Timestamp(ingested_at).tz_convert("UTC")
            pieces.append(late)
        late_mask = rng.random(n) < cfg.late_retransmit_rate
        self._pending_retransmits = df[late_mask].copy() if late_mask.any() else None

        out = pd.concat(pieces, ignore_index=True) if len(pieces) > 1 else df
        # Shuffle so duplicates are not conveniently adjacent in the file.
        return out.sample(frac=1.0, random_state=int(rng.integers(0, 2**31))).reset_index(
            drop=True
        )

    # -----------------------------------------------------------------
    # Checkpointing (so an hourly container job resumes where it left off)
    # -----------------------------------------------------------------
    def to_state(self, last_timestamp: datetime) -> dict:
        return {
            "version": 1,
            "seed": self.cfg.seed,
            "fleet_size": self.cfg.fleet_size,
            "last_timestamp": last_timestamp.isoformat(),
            "equipment_id": self.equipment_id.tolist(),
            "engine_hours": self.engine_hours.tolist(),
            "fuel_pct": self.fuel_pct.tolist(),
            "coolant": self.coolant.tolist(),
            "lat": self.lat.tolist(),
            "lon": self.lon.tolist(),
            "running": self.running.tolist(),
            "refuel_at": self.refuel_at.tolist(),
            "degrades": self.degrades.tolist(),
            "mode_idx": self.mode_idx.tolist(),
            "planned_degrades": self.planned_degrades.tolist(),
            "planned_mode_idx": self.planned_mode_idx.tolist(),
            "fail_epoch": [None if math.isinf(v) else v for v in self.fail_epoch],
            "ramp_seconds": self.ramp_seconds.tolist(),
            "repair_seconds": self.repair_seconds.tolist(),
            "down_until": [None if math.isinf(v) else v for v in self.down_until],
        }

    def load_state(self, state: dict) -> None:
        """Restore dynamic state so a later run continues the same machines.

        Guards on fleet identity: a state file written for a different fleet is
        ignored rather than silently misapplied.
        """
        if state.get("equipment_id") != self.equipment_id.tolist():
            raise ValueError("state file does not match the current fleet roster")
        self.engine_hours = np.array(state["engine_hours"], dtype=float)
        self.fuel_pct = np.array(state["fuel_pct"], dtype=float)
        self.coolant = np.array(state["coolant"], dtype=float)
        self.lat = np.array(state["lat"], dtype=float)
        self.lon = np.array(state["lon"], dtype=float)
        self.running = np.array(state["running"], dtype=bool)
        self.refuel_at = np.array(state["refuel_at"], dtype=float)
        self.degrades = np.array(state["degrades"], dtype=bool)
        self.mode_idx = np.array(state["mode_idx"], dtype=np.int64)
        self.planned_degrades = np.array(
            state.get("planned_degrades", state["degrades"]), dtype=bool
        )
        self.planned_mode_idx = np.array(
            state.get("planned_mode_idx", state["mode_idx"]), dtype=np.int64
        )
        self.fail_epoch = np.array(
            [np.inf if v is None else v for v in state["fail_epoch"]], dtype=float
        )
        self.ramp_seconds = np.array(state["ramp_seconds"], dtype=float)
        self.repair_seconds = np.array(state["repair_seconds"], dtype=float)
        self.down_until = np.array(
            [-np.inf if v is None else v for v in state["down_until"]], dtype=float
        )
        self._planned = True
