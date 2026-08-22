"""Domain constants for the simulated fleet.

Kept separate from the simulation engine so the physics in `simulator.py` reads
as physics, and the fleet composition can be retuned without touching it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EquipmentProfile:
    """Nominal operating envelope for one class of machine."""

    equipment_type: str
    id_prefix: str
    fleet_share: float  # fraction of total fleet size
    nominal_temp_c: float  # steady-state coolant temp under load
    temp_noise_c: float
    nominal_oil_psi: float
    oil_noise_psi: float
    fuel_burn_pct_per_hour: float  # % of tank consumed per operating hour
    tank_hours: float  # rough hours of runtime on a full tank (for context)
    mobile: bool  # generator sets sit on a pad and never move
    wander_km: float  # radius the machine works within, around its site
    duty: str  # "shift" (follows site hours) or "continuous"


# 500 machines by default: 180 excavators, 120 dozers, 140 loaders, 60 gensets.
# Shares are intentionally uneven — a real earthmoving fleet is not uniform, and
# an uneven distribution makes the `fleet_summary_by_type` gold table show
# something other than four identical rows.
EQUIPMENT_PROFILES: tuple[EquipmentProfile, ...] = (
    EquipmentProfile(
        equipment_type="excavator",
        id_prefix="EXC",
        fleet_share=0.36,
        nominal_temp_c=92.0,
        temp_noise_c=2.4,
        nominal_oil_psi=52.0,
        oil_noise_psi=2.8,
        fuel_burn_pct_per_hour=5.6,
        tank_hours=17.8,
        mobile=True,
        wander_km=1.2,
        duty="shift",
    ),
    EquipmentProfile(
        equipment_type="dozer",
        id_prefix="DZR",
        fleet_share=0.24,
        nominal_temp_c=90.0,
        temp_noise_c=2.8,
        nominal_oil_psi=48.0,
        oil_noise_psi=3.1,
        fuel_burn_pct_per_hour=7.4,
        tank_hours=13.5,
        mobile=True,
        wander_km=2.0,
        duty="shift",
    ),
    EquipmentProfile(
        equipment_type="loader",
        id_prefix="LDR",
        fleet_share=0.28,
        nominal_temp_c=88.0,
        temp_noise_c=2.2,
        nominal_oil_psi=50.0,
        oil_noise_psi=2.6,
        fuel_burn_pct_per_hour=6.1,
        tank_hours=16.4,
        mobile=True,
        wander_km=1.8,
        duty="shift",
    ),
    EquipmentProfile(
        equipment_type="generator_engine",
        id_prefix="GEN",
        fleet_share=0.12,
        nominal_temp_c=85.0,
        temp_noise_c=1.6,
        nominal_oil_psi=45.0,
        oil_noise_psi=2.0,
        fuel_burn_pct_per_hour=3.2,
        tank_hours=31.2,
        mobile=False,
        wander_km=0.0,
        duty="continuous",  # site power — runs around the clock
    ),
)

PROFILES_BY_TYPE = {p.equipment_type: p for p in EQUIPMENT_PROFILES}


@dataclass(frozen=True)
class Site:
    """A job site. Machines are assigned to one and work within its hours."""

    site_id: str
    lat: float
    lon: float
    ops: str  # "24h" for mining, "day" for civil construction
    ambient_c: float  # mean ambient temperature, drives cold-engine readings
    utc_offset_hours: float  # local time of the site, used to emit dirty timestamps


# Six sites across two operating patterns. `ops` drives the duty cycle, which is
# what makes engine_hours and fuel_level_pct move in believable daily rhythms
# instead of a flat line.
SITES: tuple[Site, ...] = (
    Site("PIL-01", -22.5891, 117.7834, "24h", 31.0, 8.0),  # Pilbara iron ore
    Site("PIL-02", -23.3583, 119.7317, "24h", 32.5, 8.0),  # Newman expansion
    Site("QLD-01", -22.3400, 148.1200, "24h", 27.0, 10.0),  # Bowen Basin coal
    Site("NSW-01", -32.5600, 150.9500, "day", 21.0, 10.0),  # Hunter Valley
    Site("SA-01", -30.4400, 136.8800, "24h", 25.0, 9.5),  # Olympic Dam haul road
    Site("VIC-01", -37.8100, 144.9500, "day", 16.0, 10.0),  # Melbourne metro tunnel
)

# ---------------------------------------------------------------------------
# Fault codes
#
# Heavy equipment uses SAE J1939 diagnostics: an SPN (Suspect Parameter Number,
# "which parameter") plus an FMI (Failure Mode Identifier, "how it failed").
# A handful of OBD-II style P-codes are mixed in because mixed-vendor fleets do
# report both, and the inconsistency is realistic.
#
# Codes are grouped by the symptom that produces them, so a machine whose
# coolant temperature is climbing throws overheat codes rather than random ones.
# That correlation is what makes the gold-layer rules meaningful: fault codes and
# sensor trends agree, so a rule keying off either one finds the same machines.
# ---------------------------------------------------------------------------
FAULT_CODES_BY_SYMPTOM: dict[str, tuple[str, ...]] = {
    "overheat": (
        "SPN-110-FMI-0",  # engine coolant temp — above normal, most severe
        "SPN-110-FMI-16",  # engine coolant temp — above normal, moderate
        "SPN-1636-FMI-16",  # intake manifold temp high
        "P0217",  # engine overtemp condition
    ),
    "low_oil_pressure": (
        "SPN-100-FMI-1",  # engine oil pressure — below normal, most severe
        "SPN-100-FMI-18",  # engine oil pressure — below normal, moderate
        "P0524",  # engine oil pressure too low
    ),
    "fuel": (
        "SPN-96-FMI-1",  # fuel level low
        "SPN-94-FMI-18",  # fuel delivery pressure low
        "P0087",  # fuel rail pressure too low
    ),
    "aftertreatment": (
        "SPN-3251-FMI-2",  # DPF differential pressure erratic
        "SPN-3936-FMI-16",  # aftertreatment system cleaning required
        "SPN-3226-FMI-4",  # NOx sensor circuit low
    ),
    "electrical": (
        "SPN-629-FMI-12",  # ECM bad intelligent device
        "SPN-636-FMI-2",  # engine position sensor erratic
        "SPN-168-FMI-18",  # battery voltage low
    ),
}

# Codes that fire at a low background rate on healthy machines — nuisance faults
# a fleet manager sees every day. Without these, "has any fault code" would be a
# perfect failure predictor and the gold rules would be trivially accurate.
NUISANCE_SYMPTOMS = ("electrical", "aftertreatment", "fuel")

# ---------------------------------------------------------------------------
# Dirt injection
#
# Telematics gateways from different vendors label the same machine class
# differently, and nobody normalises it before it hits the lake. Bronze stores
# whatever arrived; silver maps these variants back to the canonical type.
# ---------------------------------------------------------------------------
TYPE_VARIANTS: dict[str, tuple[str, ...]] = {
    "excavator": ("Excavator", "EXCAVATOR", " excavator", "excavator ", "Excavator "),
    "dozer": ("Dozer", "DOZER", "bulldozer", " dozer", "Dozer "),
    "loader": ("Loader", "LOADER", "wheel loader", " loader", "Loader "),
    "generator_engine": (
        "Generator Engine",
        "GENERATOR_ENGINE",
        "generator-engine",
        "genset",
        "Generator engine ",
    ),
}
