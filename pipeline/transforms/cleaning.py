"""Bronze -> silver cleaning rules, as pure DataFrame functions.

Why this module has no imports from `storage`, `prefect` or `os`
----------------------------------------------------------------
Cleaning logic is the part of a pipeline that actually needs testing: it encodes
judgement calls about what a reading *means*, and those calls are argued about,
changed, and regression-tested for years. Orchestration and I/O are plumbing.
Keeping them apart means every rule below can be exercised on a five-row
DataFrame in milliseconds, with no Azure account, no Prefect server and no
filesystem — and `silver_flow.py` shrinks to "read bytes, call this, write
bytes".

The one principle behind every rule here
----------------------------------------
**A bad field costs you the field, not the row.** A telemetry reading carries
six independent sensors. If the fuel float sticks and reports 128%, the coolant
temperature on that same row is still a perfectly good observation of an engine.
Dropping the row to "clean" the data would silently delete five true facts to
suppress one false one, and would bias every downstream average toward whichever
machines happen to have healthy fuel senders. So out-of-range values are nulled
and *flagged*, and the row survives.

Only two things justify losing a whole row, and both are recorded in the
quarantine table rather than dropped: no equipment_id (nothing to join it to)
and no parseable timestamp (nothing to order it by). Those are the row's
identity — without them there is no reading, just numbers.

Every repair is recorded in `quality_flags`, which is what separates a defensible
transform from a magic one: silver can always answer "what did you change on this
row, and which rule changed it".
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.schemas import (
    EQUIPMENT_TYPES,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMA,
    UNDER_LOAD_OIL_PSI,
    VALID_RANGES,
)

# ---------------------------------------------------------------------------
# Vocabulary
#
# Flags and reject reasons are module constants rather than string literals
# sprinkled through the code because they are a published contract: the gold
# layer and the API filter on these exact strings.
# ---------------------------------------------------------------------------
FLAG_ASSUMED_UTC = "assumed_utc"
FLAG_GPS_NULL_ISLAND = "gps_null_island"

REJECT_MISSING_EQUIPMENT_ID = "missing_equipment_id"
REJECT_UNPARSEABLE_TIMESTAMP = "unparseable_timestamp"
REJECT_UNKNOWN_EQUIPMENT_TYPE = "unknown_equipment_type"


def out_of_range_flag(column: str) -> str:
    return f"{column}_out_of_range"


# Canonical flag order. `quality_flags` is a string that ends up in equality
# comparisons, GROUP BYs and test fixtures, so "assumed_utc,gps_null_island" must
# never come back as "gps_null_island,assumed_utc" just because the rules ran in
# a different order.
FLAG_ORDER: tuple[str, ...] = (
    FLAG_ASSUMED_UTC,
    *(out_of_range_flag(column) for column in VALID_RANGES),
    FLAG_GPS_NULL_ISLAND,
)

# Working columns. Prefixed and stripped before the frame is written, so they
# can never leak into the silver contract.
_FLAG_PREFIX = "_flag_"
_RAW_TYPE_COLUMN = "_equipment_type_raw"


# ---------------------------------------------------------------------------
# 0. Text conformance
# ---------------------------------------------------------------------------
def normalise_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Trim the identifier and turn "empty" fault codes into real NULLs.

    Two small conformance rules with outsized downstream effects:

    * ``equipment_id`` is the join key for every gold table. If one gateway pads
      it to a fixed width, " EXC-0001 " and "EXC-0001" are two machines to a
      GROUP BY and two keys to the deduplicator — the same machine counted
      twice, forever.
    * a fault code of "" is a gateway sending an empty field rather than
      omitting it. Left as-is, the perfectly reasonable
      ``WHERE fault_code IS NOT NULL`` counts it as a diagnostic trouble code.
      Absence of a value should look like absence of a value.
    """
    out = df.copy()
    out["equipment_id"] = out["equipment_id"].astype("string").str.strip()
    fault = out["fault_code"].astype("string").str.strip()
    out["fault_code"] = fault.mask(fault == "")
    return out


# ---------------------------------------------------------------------------
# 1. Timestamps
# ---------------------------------------------------------------------------
# Four encodings reach the raw zone from four vendors' gateways:
#
#   "2026-08-19T09:55:00Z"        ISO-8601, already UTC
#   "2026-08-19T17:55:00+08:00"   site-local, carrying a real offset
#   "2026-08-19 09:55:00"         tz marker dropped somewhere in transit
#   "1787133300000"               epoch milliseconds, as a string
#
# They are parsed in four separate passes, one per encoding, and NOT in a single
# pd.to_datetime() call over the whole column. That is not stylistic. pandas
# infers one offset for a mixed column: hand it an offset-bearing string and a
# naive string together and the naive one silently inherits +08:00, moving the
# reading eight hours. Verified on pandas 2.3.3:
#
#   to_datetime(["...T09:55:00Z", "...T17:55:00+08:00", "... 09:55:00"], utc=True)
#     -> 09:55Z, 09:55Z, 01:55Z     <- the third value is eight hours wrong
#
# An eight-hour shift is invisible in aggregate (the daily average barely moves)
# and catastrophic in detail (readings land in the wrong shift, the wrong day,
# the wrong partition). Masking by encoding first makes each pass unambiguous.
# ---------------------------------------------------------------------------
_EPOCH_DIGITS_RE = re.compile(r"^-?\d{9,}$")
_TZ_SUFFIX_RE = re.compile(r"(?:Z|z|[+-]\d{2}:?\d{2})$")

# Epoch values are documented as milliseconds, but digit count is the honest
# discriminator: 13 digits is milliseconds, 10 is seconds. Guessing wrong puts
# the reading in 1970 or in the year 55000, so it is worth two lines to check.
_EPOCH_MS_MIN_DIGITS = 12


def parse_event_times(raw: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse a raw ``timestamp`` column to UTC instants.

    Returns ``(event_time, assumed_utc)`` where ``event_time`` is
    ``datetime64[us, UTC]`` (NaT where nothing parsed) and ``assumed_utc`` marks
    the rows whose timezone we supplied rather than read.
    """
    text = raw.astype("string").str.strip()
    blank = text.isna() | (text == "")

    event_time = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns, UTC]")
    assumed_utc = pd.Series(False, index=raw.index)

    is_epoch = ~blank & text.str.fullmatch(_EPOCH_DIGITS_RE, na=False)
    is_offset = ~blank & ~is_epoch & text.str.contains(_TZ_SUFFIX_RE, na=False)
    is_naive = ~blank & ~is_epoch & ~is_offset

    if is_epoch.any():
        numeric = pd.to_numeric(text.where(is_epoch), errors="coerce")
        digit_count = text.where(is_epoch).str.lstrip("-").str.len()
        as_ms = is_epoch & (digit_count >= _EPOCH_MS_MIN_DIGITS).fillna(False)
        as_seconds = is_epoch & ~as_ms
        event_time.loc[as_ms] = pd.to_datetime(
            numeric[as_ms], unit="ms", utc=True, errors="coerce"
        )
        event_time.loc[as_seconds] = pd.to_datetime(
            numeric[as_seconds], unit="s", utc=True, errors="coerce"
        )

    if is_offset.any():
        # Every value here carries its own offset, so utc=True *converts* rather
        # than assumes: 17:55+08:00 becomes 09:55Z, the same instant the Z-format
        # gateway would have sent.
        event_time.loc[is_offset] = pd.to_datetime(
            text[is_offset], format="ISO8601", utc=True, errors="coerce"
        )

    if is_naive.any():
        # No offset in the payload. UTC is the only defensible assumption (the
        # fleet spans four Australian offsets, so "site local" is not a single
        # thing), but an assumption made silently is a bug waiting to be
        # rediscovered in six months — so it is flagged on the row.
        parsed = pd.to_datetime(text[is_naive], format="ISO8601", errors="coerce")
        event_time.loc[is_naive] = parsed.dt.tz_localize("UTC")
        assumed_utc.loc[is_naive] = parsed.notna()

    return event_time.astype("datetime64[us, UTC]"), assumed_utc


def normalise_timestamps(df: pd.DataFrame, source_column: str = "timestamp") -> pd.DataFrame:
    """Add ``event_time`` (UTC) and ``event_date`` (UTC calendar date)."""
    out = _ensure_flag_columns(df.copy())
    event_time, assumed_utc = parse_event_times(out[source_column])
    out["event_time"] = event_time
    # event_date is derived from the UTC instant, never from the source string:
    # "2026-08-19T17:55:00+08:00" is a 2026-08-19 reading in Perth and still
    # 2026-08-19 in UTC, but a 23:30+10:00 reading is the *previous* UTC day.
    # Partitioning on the local-looking date would scatter one day's data across
    # two partitions and make "yesterday" mean two different things.
    out["event_date"] = event_time.dt.date
    out[_FLAG_PREFIX + FLAG_ASSUMED_UTC] = assumed_utc.to_numpy()
    return out


# ---------------------------------------------------------------------------
# 2. Equipment type
# ---------------------------------------------------------------------------
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def slugify_equipment_type(value: object) -> str:
    """Lowercase, trim, and collapse any run of separators to one underscore.

    This is the part that generalises. Most vendor "variants" are not synonyms at
    all, just presentation noise — " Excavator ", "EXCAVATOR", "generator-engine"
    and "Generator Engine" are one token wearing four hats. Normalising shape
    first means the synonym table only has to carry genuine vocabulary
    differences ("genset", "bulldozer"), and a spelling nobody has seen yet
    ("  GENERATOR   ENGINE") still lands on its feet.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return _NON_ALNUM_RE.sub("_", str(value).strip().lower()).strip("_")


def _load_generator_variants() -> dict[str, tuple[str, ...]]:
    """Reuse the generator's variant table so the two cannot drift apart.

    Imported defensively: the deployed transform image has no reason to ship the
    simulator, and silver must not stop working because it is absent. The
    fallbacks below cover the same ground.
    """
    try:
        from pipeline.generator.profiles import TYPE_VARIANTS
    except ImportError:  # pragma: no cover - only hit in a generator-less image
        return {}
    return dict(TYPE_VARIANTS)


# Vocabulary the shape-normaliser cannot reach, because these are different
# words rather than different formatting. Kept small and deliberate: guessing
# aggressively here is how a backhoe silently becomes an excavator.
_EXTRA_TYPE_SYNONYMS: dict[str, str] = {
    "bull_dozer": "dozer",
    "crawler_dozer": "dozer",
    "track_dozer": "dozer",
    "front_end_loader": "loader",
    "wheeled_loader": "loader",
    "digger": "excavator",  # site slang, and it does turn up in vendor feeds
    "gen_set": "generator_engine",
    "generator_set": "generator_engine",
    "generator": "generator_engine",
}


def _build_type_synonyms() -> dict[str, str]:
    synonyms = {slugify_equipment_type(t): t for t in EQUIPMENT_TYPES}
    for canonical, variants in _load_generator_variants().items():
        if canonical not in EQUIPMENT_TYPES:
            continue
        for variant in variants:
            synonyms[slugify_equipment_type(variant)] = canonical
    synonyms.update(_EXTRA_TYPE_SYNONYMS)
    return synonyms


TYPE_SYNONYMS: dict[str, str] = _build_type_synonyms()


def canonical_equipment_type(value: object) -> str | None:
    """Map any vendor spelling to one of ``EQUIPMENT_TYPES``, or None.

    Four passes, cheapest and most certain first: exact slug, known synonym,
    plural, then a last-resort substring match that is only trusted when exactly
    one canonical type matches. "excavator_loader" (a backhoe — genuinely both)
    matches two, so it stays unknown and goes to quarantine instead of being
    guessed into the wrong fleet statistic.
    """
    slug = slugify_equipment_type(value)
    if not slug:
        return None
    if slug in TYPE_SYNONYMS:
        return TYPE_SYNONYMS[slug]
    if slug.endswith("s") and slug[:-1] in TYPE_SYNONYMS:
        return TYPE_SYNONYMS[slug[:-1]]
    matches = {t for t in EQUIPMENT_TYPES if t in slug}
    if len(matches) == 1:
        return matches.pop()
    return None


def normalise_equipment_types(df: pd.DataFrame) -> pd.DataFrame:
    """Canonicalise ``equipment_type``; unmappable values become NaN."""
    out = _ensure_flag_columns(df.copy())
    out[_RAW_TYPE_COLUMN] = out["equipment_type"]
    # Only a couple of dozen distinct spellings exist in a partition of 100k
    # rows, so resolve each spelling once and map, rather than calling the
    # resolver per row.
    lookup = {value: canonical_equipment_type(value) for value in out["equipment_type"].unique()}
    out["equipment_type"] = out["equipment_type"].map(lookup)
    return out


# ---------------------------------------------------------------------------
# 3. Range validation
# ---------------------------------------------------------------------------
def apply_range_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Null physically impossible sensor values and flag each one.

    Note what this deliberately does NOT do: drop the row. A fuel sender
    reporting 999.9% is one broken sensor on a machine whose other five sensors
    are fine, and the fleet's coolant statistics have no business changing
    because of it. See the module docstring — this is the decision the silver
    layer exists to make.
    """
    out = _ensure_flag_columns(df.copy())

    for column, (low, high) in VALID_RANGES.items():
        values = pd.to_numeric(out[column], errors="coerce")
        bad = values.notna() & ((values < low) | (values > high))
        out[column] = values.mask(bad)
        out[_FLAG_PREFIX + out_of_range_flag(column)] = bad.to_numpy()

    # "Null island": exactly (0.0, 0.0) is a point in the Gulf of Guinea. It is
    # not a coordinate a machine in the Pilbara can occupy, it is what a GPS
    # module emits when it has no fix and the gateway serialises the empty
    # struct as zeroes. Both axes are nulled together because the pair is one
    # observation — keeping lat=0 alone would be worse than keeping neither.
    lat = out["gps_lat"]
    lon = out["gps_lon"]
    null_island = (lat == 0.0) & (lon == 0.0)
    out.loc[null_island, ["gps_lat", "gps_lon"]] = np.nan
    out[_FLAG_PREFIX + FLAG_GPS_NULL_ISLAND] = null_island.to_numpy()

    return out


# ---------------------------------------------------------------------------
# 4. Deduplication
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DedupStats:
    """How many rows each duplication mechanism cost, kept separate on purpose.

    They are different failure modes: exact resends are the delivery layer doing
    its job (at-least-once), cross-batch resends are a gateway that buffered
    during an outage and flushed later. A jump in the second number is an
    operational signal; a jump in the first is noise.
    """

    rows_in: int = 0
    rows_out: int = 0
    exact_resends: int = 0
    cross_batch_resends: int = 0

    @property
    def total_removed(self) -> int:
        return self.exact_resends + self.cross_batch_resends


def deduplicate_readings(df: pd.DataFrame) -> tuple[pd.DataFrame, DedupStats]:
    """Collapse to one row per (equipment_id, event_time), earliest ingest wins.

    Why the business key and not ``drop_duplicates()`` over every column: a
    gateway that buffers during an outage re-sends the identical reading in a
    LATER batch, so the resend carries a different ``_ingest_batch_id`` and a
    later ``_ingested_at_utc``. Across all columns the two rows are *not* equal,
    and a naive drop_duplicates() keeps both — this dataset hides 821 such rows
    behind 2,530 total duplicates, so the naive version silently misses a third
    of them. The only thing that identifies "the same physical observation" is
    the machine plus the instant it observed.

    First-write-wins rather than last: the earliest arrival is the original
    transmission, and later copies are replays of it. Choosing "latest" would
    make the output depend on how often the pipeline happened to run.
    """
    rows_in = len(df)
    if rows_in == 0:
        return df, DedupStats()

    key = ["equipment_id", "event_time"]
    # Stable sort + a tie-break on batch id: two copies inside one batch share an
    # ingest timestamp to the microsecond, so ordering by time alone would leave
    # the winner to pandas' internal ordering and make reruns non-reproducible.
    ordered = df.sort_values(
        ["_ingested_at_utc", "_ingest_batch_id"], kind="stable", na_position="last"
    )
    duplicated = ordered.duplicated(subset=key, keep="first")

    removed = ordered[duplicated]
    kept = ordered[~duplicated]

    cross_batch = 0
    if len(removed):
        winner_batch = kept.set_index(key)["_ingest_batch_id"]
        loser_index = pd.MultiIndex.from_frame(removed[key])
        winners = winner_batch.reindex(loser_index).to_numpy()
        cross_batch = int((winners != removed["_ingest_batch_id"].to_numpy()).sum())

    stats = DedupStats(
        rows_in=rows_in,
        rows_out=len(kept),
        exact_resends=len(removed) - cross_batch,
        cross_batch_resends=cross_batch,
    )
    return kept, stats


# ---------------------------------------------------------------------------
# 5. Derived columns
# ---------------------------------------------------------------------------
def add_derived_columns(df: pd.DataFrame, processed_at: datetime) -> pd.DataFrame:
    """Add has_fault, is_under_load and the processing timestamp."""
    out = _ensure_flag_columns(df.copy())

    # `normalise_text_fields` has already collapsed ""/whitespace to NULL, so
    # "has a code" is now simply "is not null" — but the emptiness test stays
    # here too, because these functions are meant to compose in any order.
    fault = out["fault_code"].astype("string").str.strip()
    out["has_fault"] = (fault.notna() & (fault != "")).to_numpy(dtype=bool)

    # Parked machines read 0 psi and ambient temperature. Materialising the
    # under-load test here means every downstream aggregate filters on the same
    # definition instead of each query re-deriving its own threshold.
    oil = pd.to_numeric(out["oil_pressure_psi"], errors="coerce")
    out["is_under_load"] = (oil > UNDER_LOAD_OIL_PSI).fillna(False).to_numpy(dtype=bool)

    out["_silver_processed_at_utc"] = pd.Timestamp(processed_at).tz_convert("UTC")
    return out


def collapse_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Fold the boolean working flags into one comma-separated audit string."""
    out = df.copy()
    joined = np.full(len(out), "", dtype=object)
    for flag in FLAG_ORDER:
        column = _FLAG_PREFIX + flag
        if column not in out.columns:
            continue
        mask = out[column].fillna(False).to_numpy(dtype=bool)
        joined = np.where(
            mask, np.where(joined == "", flag, np.char.add(joined.astype(str), "," + flag)), joined
        )
    # None, not "": a clean row has nothing to say, and an empty string would
    # force every consumer to test for two flavours of "no flags".
    out["quality_flags"] = pd.Series(joined, index=out.index).replace("", None)
    return out.drop(columns=[c for c in out.columns if c.startswith(_FLAG_PREFIX)])


# ---------------------------------------------------------------------------
# 6. Quarantine
# ---------------------------------------------------------------------------
def split_quarantine(
    df: pd.DataFrame, quarantined_at: datetime
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate unrepairable rows. Returns ``(keep, quarantined)``.

    Quarantining beats dropping for one reason: "429,290 in, 429,288 out, 2
    rejected and here they are" is an auditable statement, and a row count that
    quietly shrinks is not. The quarantine table is also the first place to look
    when a new vendor is onboarded badly — a sudden pile of
    ``unknown_equipment_type`` is that vendor announcing itself.
    """
    reason = pd.Series(pd.NA, index=df.index, dtype="string")

    equipment_id = df["equipment_id"].astype("string").str.strip()
    missing_id = equipment_id.isna() | (equipment_id == "")
    reason = reason.mask(reason.isna() & missing_id, REJECT_MISSING_EQUIPMENT_ID)

    bad_time = df["event_time"].isna()
    reason = reason.mask(reason.isna() & bad_time, REJECT_UNPARSEABLE_TIMESTAMP)

    unknown_type = df["equipment_type"].isna()
    reason = reason.mask(reason.isna() & unknown_type, REJECT_UNKNOWN_EQUIPMENT_TYPE)

    rejected = reason.notna()
    quarantined = pd.DataFrame(
        {
            "equipment_id": df.loc[rejected, "equipment_id"],
            # The *raw* spelling, not the normalised one: the point of the record
            # is to show what arrived so a human can extend the synonym table.
            "equipment_type": df.loc[rejected, _RAW_TYPE_COLUMN]
            if _RAW_TYPE_COLUMN in df.columns
            else df.loc[rejected, "equipment_type"],
            "timestamp": df.loc[rejected, "timestamp"],
            "reject_reason": reason[rejected],
            "_ingest_batch_id": df.loc[rejected, "_ingest_batch_id"],
            "_quarantined_at_utc": pd.Timestamp(quarantined_at).tz_convert("UTC"),
        }
    ).reset_index(drop=True)

    return df[~rejected], quarantined


# ---------------------------------------------------------------------------
# Orchestration of the pure steps
# ---------------------------------------------------------------------------
@dataclass
class CleaningResult:
    """Everything one bronze partition turns into, plus the arithmetic to prove it."""

    silver: pd.DataFrame
    quarantine: pd.DataFrame
    rows_in: int = 0
    dedup: DedupStats = field(default_factory=DedupStats)
    quarantine_by_reason: dict[str, int] = field(default_factory=dict)
    values_nulled: dict[str, int] = field(default_factory=dict)
    flag_row_counts: dict[str, int] = field(default_factory=dict)

    @property
    def rows_out(self) -> int:
        return len(self.silver)

    @property
    def rows_quarantined(self) -> int:
        return len(self.quarantine)

    def reconciles(self) -> bool:
        """rows_in must equal rows_out + rejected + deduped. No silent losses."""
        return self.rows_in == self.rows_out + self.rows_quarantined + self.dedup.total_removed


def empty_silver_frame() -> pd.DataFrame:
    return SILVER_SCHEMA.empty_table().to_pandas()


def empty_quarantine_frame() -> pd.DataFrame:
    return QUARANTINE_SCHEMA.empty_table().to_pandas()


def clean_bronze_partition(
    df: pd.DataFrame, processed_at: datetime | None = None
) -> CleaningResult:
    """Run the full bronze -> silver transform over one partition of raw rows.

    Step order is load-bearing:
      1. timestamps and 2. types first, because they decide row identity;
      3. quarantine next, so rows without an identity never reach the key-based
         logic that follows;
      4. ranges before dedup, so both copies of a duplicate were cleaned
         identically and it cannot matter which copy survives;
      5. dedup, then 6. derived columns — computed once, on the survivors.
    """
    processed_at = processed_at or datetime.now(timezone.utc)

    if df is None or len(df) == 0:
        return CleaningResult(silver=empty_silver_frame(), quarantine=empty_quarantine_frame())

    rows_in = len(df)
    working = normalise_text_fields(df)
    working = normalise_timestamps(working)
    working = normalise_equipment_types(working)

    working, quarantined = split_quarantine(working, processed_at)
    quarantine_by_reason = (
        quarantined["reject_reason"].value_counts().to_dict() if len(quarantined) else {}
    )

    working = apply_range_rules(working)
    # Counted before dedup so the numbers describe what the *sensors* did, and
    # again after, so `values_nulled` describes what silver actually contains.
    working, dedup = deduplicate_readings(working)

    flag_row_counts = {
        flag: int(working[_FLAG_PREFIX + flag].sum())
        for flag in FLAG_ORDER
        if _FLAG_PREFIX + flag in working.columns and working[_FLAG_PREFIX + flag].any()
    }
    values_nulled = {
        column: int(working[_FLAG_PREFIX + out_of_range_flag(column)].sum())
        for column in VALID_RANGES
        if _FLAG_PREFIX + out_of_range_flag(column) in working.columns
    }
    if _FLAG_PREFIX + FLAG_GPS_NULL_ISLAND in working.columns:
        # Null island nulls two columns from one flag; count it that way.
        island = int(working[_FLAG_PREFIX + FLAG_GPS_NULL_ISLAND].sum())
        values_nulled["gps_lat"] = values_nulled.get("gps_lat", 0) + island
        values_nulled["gps_lon"] = values_nulled.get("gps_lon", 0) + island

    working = add_derived_columns(working, processed_at)
    working = collapse_quality_flags(working)

    # Sorted on the natural read order of the layer (time, then machine). It also
    # makes the file byte-stable across reruns: the row order no longer depends
    # on which order the bronze files happened to be listed in.
    silver = (
        working.sort_values(["event_time", "equipment_id"], kind="stable")
        .loc[:, list(SILVER_SCHEMA.names)]
        .reset_index(drop=True)
    )

    return CleaningResult(
        silver=silver,
        quarantine=quarantined if len(quarantined) else empty_quarantine_frame(),
        rows_in=rows_in,
        dedup=dedup,
        quarantine_by_reason={str(k): int(v) for k, v in quarantine_by_reason.items()},
        values_nulled={k: v for k, v in values_nulled.items() if v},
        flag_row_counts=flag_row_counts,
    )


def _ensure_flag_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee every flag column exists so the steps compose in any order."""
    for flag in FLAG_ORDER:
        column = _FLAG_PREFIX + flag
        if column not in df.columns:
            df[column] = False
    return df
