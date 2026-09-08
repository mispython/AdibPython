from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import pandas as pd
import saspy
from PBBLNFMT import put, informat, apply_format, available_formats
import duckdb  # noqa: F401
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401
import re
import sys
from collections import Counter


# =========================
# Paths (adjust to your env)
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Input SAS datasets (all in sas7bdat format) ----
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")

LOANI_LNNOTE  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOANI_LNCOMM  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")

CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")

# COLL / DESC (EBCDIC encoded text files)
COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")

MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/BOPESS.txt")

NPGS_SMEZ     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/smez.sas7bdat")

CHUNK_SIZE = 100000

# =========================
# KNOWN record lengths (fill these in from the copybook / JCL LRECL if you have them)
# Leave as None to force auto-detection with validation instead of guessing.
# =========================
KNOWN_COLL_RECORD_LENGTH = None  # 158 was carried over from a run that only ever
                                   # sampled the first 70,000 records (old hard cap) --
                                   # not actually verified against the full file. Force
                                   # detection until this is confirmed from the copybook.
KNOWN_DESC_RECORD_LENGTH = None  # do NOT guess this from file_size // expected_records


# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin + timedelta(days=int(days))


def date_to_sas_days(d: date) -> int:
    origin = date(1960, 1, 1)
    return (d - origin).days


def read_sas7bdat_filtered(filepath: Path, entity_filter: str = None,
                           chunk_size: int = CHUNK_SIZE,
                           column_filter: dict = None) -> pl.DataFrame:
    chunks = []
    offset = 0

    while True:
        try:
            df, _ = pyreadstat.read_sas7bdat(
                str(filepath),
                row_offset=offset,
                row_limit=chunk_size
            )

            if df.empty:
                break

            df.columns = [col.lower() for col in df.columns]

            if entity_filter and 'entity_cd' in df.columns:
                if entity_filter == 'PIBB':
                    df = df[df['entity_cd'] == 'PIBB']
                elif entity_filter == 'NON_PIBB':
                    df = df[df['entity_cd'] != 'PIBB']

            if column_filter:
                for col_name, col_value in column_filter.items():
                    if col_name in df.columns:
                        df = df[df[col_name] == col_value]

            if not df.empty:
                chunks.append(pl.from_pandas(df))

            offset += chunk_size

            if len(df) < chunk_size:
                break

        except Exception as e:
            print(f"Error reading chunk at offset {offset}: {e}")
            break

    if not chunks:
        return pl.DataFrame()

    return pl.concat(chunks, how="vertical", rechunk=True)


def read_sas7bdat(filepath: Path) -> pl.DataFrame:
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    df.columns = [col.lower() for col in df.columns]
    return pl.from_pandas(df)


def read_fixed_width_file(filepath: Path, col_specs: list, encoding: str = 'cp037') -> pl.DataFrame:
    with open(filepath, 'rb') as f:
        raw_data = f.read()

    if encoding == 'cp037':
        try:
            decoded_data = raw_data.decode('cp037')
        except Exception:
            try:
                decoded_data = raw_data.decode('cp500')
            except Exception:
                decoded_data = raw_data.decode('latin-1')
    else:
        decoded_data = raw_data.decode(encoding)

    rows = []
    lines = decoded_data.split('\n')

    for line in lines:
        if line.strip():
            row = {}
            for col_name, start, end, col_type in col_specs:
                value = line[start-1:end].strip() if len(line) >= end else ""

                if col_type == 'numeric':
                    try:
                        row[col_name.lower()] = float(value) if value else None
                    except Exception:
                        row[col_name.lower()] = None
                elif col_type == 'pd':
                    row[col_name.lower()] = value
                else:
                    row[col_name.lower()] = value
            rows.append(row)

    return pl.DataFrame(rows)


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
    if x is None:
        return None
    try:
        xi = int(x)
        if xi <= 0:
            return None
        s = f"{xi:011d}"[:8]
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


def month_end_of(d: date) -> date:
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


def format_date_ddmmyyyy(d: date | None) -> str:
    if d is None:
        return "          "
    return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"


# =====================================================================
# FIX #1: Record-length detection with validation instead of a blind guess
# =====================================================================
def divisors_near(file_size: int, approx_length: int, tolerance: float = 0.6) -> list[int]:
    """
    Return all exact divisors of file_size that fall within +/- tolerance
    of approx_length, ordered by closeness to approx_length.
    Only exact divisors are structurally possible record lengths.
    """
    lo = int(approx_length * (1 - tolerance))
    hi = int(approx_length * (1 + tolerance))
    lo = max(lo, 1)
    candidates = []
    for L in range(lo, hi + 1):
        if file_size % L == 0:
            candidates.append(L)
    candidates.sort(key=lambda L: abs(L - approx_length))
    return candidates


def score_field_quality(values: list[str]) -> float:
    """
    Score how 'clean' a decoded EBCDIC character field looks, on a 0-1 scale.
    Real SAS character fields (codes like CINSTCL, NATGUAR) are almost always
    blank, digits, or upper-case letters. Garbage from misaligned offsets tends
    to contain control characters, punctuation soup, or high-bit junk.
    A correct record length should push this score close to 1.0 across the file;
    a wrong one will show up as a low, noisy score.
    """
    if not values:
        return 0.0
    good = 0
    total = 0
    for v in values:
        if v is None:
            continue
        for ch in v:
            total += 1
            if ch == ' ' or ch.isdigit() or ch.isalpha():
                good += 1
    if total == 0:
        return 0.0
    return good / total


def score_pd_quality(raw_chunks: list[bytes]) -> float:
    """
    Score how 'clean' a packed-decimal field looks, on a 0-1 scale.
    A packed decimal field is only structurally valid if:
      - every nibble except the last is 0-9 (BCD digits)
      - the final (sign) nibble is one of the valid packed-decimal sign
        codes: C/F (positive), D/B (negative), A/E (also positive, less common)
    Misaligned offsets produce packed fields whose bytes are actually parts of
    character data, timestamps, or other numerics -- these fail the nibble
    check at a high rate. A correct record length should score close to 1.0.
    """
    if not raw_chunks:
        return 0.0
    valid_sign_nibbles = set('CFDBAE')
    good = 0
    total = 0
    for chunk in raw_chunks:
        total += 1
        try:
            hex_str = chunk.hex()
            digits = hex_str[:-1]
            sign_nibble = hex_str[-1].upper()
            if digits and all(c in '0123456789' for c in digits) and sign_nibble in valid_sign_nibbles:
                good += 1
        except Exception:
            pass
    if total == 0:
        return 0.0
    return good / total


def detect_record_length(filepath: Path, approx_length: int,
                          probe_field_specs: list,
                          sample_records: int = 500,
                          tolerance: float = 0.6) -> int:
    """
    Try every exact divisor of the file size near approx_length, decode a
    sample of records for each candidate using probe_field_specs (character
    fields expected to be codes/blank), and pick the candidate with the
    highest field-quality score. Raises if no candidate looks clean.
    """
    file_size = filepath.stat().st_size
    candidates = divisors_near(file_size, approx_length, tolerance)

    if not candidates:
        raise ValueError(
            f"No exact divisor of file size {file_size} found near {approx_length} "
            f"(+/-{int(tolerance*100)}%). The approximate record count used to seed "
            f"this guess is probably wrong -- get the true LRECL from the copybook/JCL."
        )

    with open(filepath, 'rb') as f:
        raw = f.read()

    best_length = None
    best_score = -1.0
    report = []

    for L in candidates:
        n = min(sample_records, file_size // L)
        char_values = []
        pd_chunks = []
        for i in range(n):
            record = raw[i*L:(i+1)*L]
            for col_name, start, end, col_type in probe_field_specs:
                chunk = record[start-1:end]
                if col_type == 'character':
                    try:
                        decoded = chunk.decode('cp037').strip()
                    except Exception:
                        decoded = ""
                    char_values.append(decoded)
                elif col_type == 'pd':
                    pd_chunks.append(chunk)

        # Combine scores across whichever field types are present in this spec.
        scores = []
        if char_values:
            scores.append(score_field_quality(char_values))
        if pd_chunks:
            scores.append(score_pd_quality(pd_chunks))
        score = sum(scores) / len(scores) if scores else 0.0

        report.append((L, score))
        if score > best_score:
            best_score = score
            best_length = L

    report.sort(key=lambda r: -r[1])
    print("  Record length candidates (length, quality score):")
    for L, s in report[:8]:
        print(f"    {L:>8}  ->  {s:.3f}")

    if best_score < 0.85:
        raise ValueError(
            f"Best candidate record length {best_length} only scores {best_score:.3f} "
            f"on field-quality validation (want >= 0.85). None of the candidates near "
            f"{approx_length} decode cleanly -- confirm the true LRECL from the source "
            f"copybook/JCL rather than trusting an estimated record count."
        )

    print(f"  Selected record length: {best_length} (score {best_score:.3f})")
    return best_length


def decode_packed_decimal(raw_bytes: bytes) -> float | None:
    try:
        hex_str = raw_bytes.hex()
        digits = hex_str[:-1]
        sign_nibble = hex_str[-1].upper()
        if digits and all(c in '0123456789ABCDEF' for c in digits):
            value = int(digits, 16)
            if sign_nibble in ('D', 'B'):
                value = -value
            return float(value)
    except Exception:
        pass
    return None


def read_ebcdic_fixed_records(filepath: Path, record_length: int, col_specs: list,
                               max_records: int | None = None) -> tuple[pl.DataFrame, dict]:
    """
    Read EBCDIC file with fixed-length records.
    Returns (dataframe, decode_stats) -- decode_stats tracks per-field failure
    counts so misalignment shows up as a visible number instead of silently
    becoming None everywhere.
    """
    rows = []
    fail_counts = Counter()
    total_counts = Counter()
    records_read = 0

    file_size = filepath.stat().st_size
    if file_size % record_length != 0:
        print(f"  WARNING: file size {file_size} is not an exact multiple of "
              f"record length {record_length} (remainder {file_size % record_length}). "
              f"This record length is almost certainly wrong.")

    expected_records = file_size // record_length

    with open(filepath, 'rb') as f:
        while True:
            record = f.read(record_length)
            if not record or len(record) < record_length:
                break

            row = {}
            for col_name, start, end, col_type in col_specs:
                start_idx = start - 1
                end_idx = end
                total_counts[col_name] += 1

                if col_type == 'pd':
                    raw_bytes = record[start_idx:end_idx]
                    val = decode_packed_decimal(raw_bytes)
                    if val is None:
                        fail_counts[col_name] += 1
                    row[col_name.lower()] = val

                elif col_type == 'numeric':
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        decoded_clean = ''.join(c for c in decoded if c.isdigit() or c in '.-')
                        row[col_name.lower()] = float(decoded_clean) if decoded_clean else None
                        if not decoded_clean:
                            fail_counts[col_name] += 1
                    except Exception:
                        row[col_name.lower()] = None
                        fail_counts[col_name] += 1

                else:  # character
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        row[col_name.lower()] = decoded
                    except Exception:
                        row[col_name.lower()] = ""
                        fail_counts[col_name] += 1

            rows.append(row)
            records_read += 1

            if max_records is not None and records_read >= max_records:
                break
            if records_read > expected_records + 10:
                break

    stats = {
        "records_read": records_read,
        "expected_records": expected_records,
        "fail_counts": dict(fail_counts),
        "total_counts": dict(total_counts),
    }
    return pl.DataFrame(rows), stats


def print_decode_report(name: str, stats: dict) -> None:
    print(f"  {name}: read {stats['records_read']} records "
          f"(expected ~{stats['expected_records']})")
    for col, total in stats["total_counts"].items():
        fails = stats["fail_counts"].get(col, 0)
        rate = fails / total if total else 0.0
        flag = "  <-- HIGH FAILURE RATE" if rate > 0.10 else ""
        print(f"    {col:>10}: {fails}/{total} decode failures ({rate:.1%}){flag}")


def validate_code_field(df: pl.DataFrame, col: str, expected_values: list[str] | None = None,
                         max_unique_to_show: int = 15) -> bool:
    """
    Sanity-check a decoded code field before trusting it in a filter/join.
    Prints the top values so a human can eyeball whether they look like real
    codes (short, alnum, blank) vs. garbage (control chars, high-bit noise).
    Returns True if the field looks plausible, False otherwise.
    """
    if col not in df.columns or df.height == 0:
        print(f"  Validation: column '{col}' missing or empty -- cannot validate.")
        return False

    vc = (
        df.select(pl.col(col))
        .to_series()
        .value_counts()
        .sort("count", descending=True)
        .head(max_unique_to_show)
    )
    print(f"  Top values for '{col}':")
    print(vc)

    values = df[col].drop_nulls().to_list()
    score = score_field_quality(values)
    print(f"  '{col}' field-quality score: {score:.3f}")

    if expected_values:
        hit_rate = sum(1 for v in values if v in expected_values) / max(len(values), 1)
        print(f"  '{col}' match rate against expected values {expected_values}: {hit_rate:.3%}")

    return score >= 0.85


# =========================
# Calculate REPTDATE
# =========================
REPTDATE = date.today() - timedelta(days=6)
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
REPTYEAR = f"{REPTDATE.year:04d}"
SDATE_INT = date_to_sas_days(REPTDATE)
SDATE     = f"{SDATE_INT:05d}"
NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

print(f"Report Date: {REPTDATE}")
print(f"Normalization Date: {NORMDT}")


# =========================
# Build LOAN0 / LOAN1
# =========================
print("Reading LOAN/LNNOTE datasets in chunks...")
print("Reading Islamic LNNOTE (ENTITY_CD = 'PIBB')...")
loani_ln = read_sas7bdat_filtered(LOANI_LNNOTE, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNNOTE rows: {loani_ln.height}")

print("Reading Conventional LNNOTE (ENTITY_CD != 'PIBB')...")
loan_ln = read_sas7bdat_filtered(LOAN_LNNOTE, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNNOTE rows: {loan_ln.height}")

print("Combining LNNOTE datasets...")
loan_base = (
    pl.concat([loani_ln, loan_ln], how="vertical", rechunk=True)
    .with_columns([
        pl.col("loantype").alias("product"),
        pl.col("census").alias("censust"),
        pl.lit("    ").alias("sch")
    ])
)

loan_base = loan_base.with_columns([
    pl.when(pl.col("loantype") == 163).then(pl.lit("P94"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.01)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 574) & (pl.col("census") == 574.02)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.00)).then(pl.lit("P101"))
     .otherwise(pl.col("sch"))
     .alias("sch")
])

loan_base = loan_base.filter(pl.col("sch") != "    ")

loan1 = loan_base.filter(pl.col("commno") > 0)
loan0 = loan_base.filter(~(pl.col("commno") > 0))

print(f"  LOAN0 rows: {loan0.height}")
print(f"  LOAN1 rows: {loan1.height}")

# =========================
# COMM
# =========================
print("Reading COMM datasets in chunks...")
print("Reading Islamic LNCOMM (ENTITY_CD = 'PIBB')...")
loani_comm = read_sas7bdat_filtered(LOANI_LNCOMM, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNCOMM rows: {loani_comm.height}")

print("Reading Conventional LNCOMM (ENTITY_CD != 'PIBB')...")
loan_comm = read_sas7bdat_filtered(LOAN_LNCOMM, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNCOMM rows: {loan_comm.height}")

has_intamt = 'intamt' in loani_comm.columns or 'intamt' in loan_comm.columns

if has_intamt:
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
            pl.when(pl.col("intamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("intamt")).alias("intamt"),
        ])
        .with_columns([
            (pl.col("corgamt") - pl.col("intamt")).alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )
else:
    print("Warning: INTAMT column not found. Using CORGAMT as NETPROC.")
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
        ])
        .with_columns([
            pl.col("corgamt").alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )

if loan1.height > 0:
    loan1 = loan1.join(comm, on=["acctno", "commno"], how="inner")
else:
    loan1 = loan1.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

if "netproc" not in loan0.columns:
    loan0 = loan0.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)
print(f"Total LOAN rows after merge: {loan.height}")

# =========================
# Derive ISSUED, NODAYS, ARREARS, NPLDATE
# =========================
print("Calculating ISSUED, NODAYS, ARREARS, NPLDATE...")

loan = loan.with_columns([
    pl.lit(None, dtype=pl.Date).alias("issued"),
    pl.lit(0).alias("nodays"),
    pl.lit(0).alias("arrears")
])

loan = loan.with_columns([
    pl.when(pl.col("issuedt").is_not_null() & (pl.col("issuedt") > 0))
      .then(pl.col("issuedt").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("issued")
])

loan = loan.with_columns([
    pl.when((pl.col("bldate") > 0) & (pl.lit(SDATE_INT) > pl.col("bldate")))
      .then(pl.lit(SDATE_INT) - pl.col("bldate"))
      .otherwise(pl.lit(0))
      .alias("nodays")
])

print("Applying NDAYS format...")

loan = loan.with_columns([
    pl.col("nodays").map_elements(
        lambda x: informat(int(x) if x is not None else 0, "NDAYS", default=0),
        return_dtype=pl.Int64
    ).alias("arrears")
])

loan = loan.with_columns([
    pl.when(pl.col("arrears") == 24)
      .then((pl.col("nodays").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("arrears"))
      .alias("arrears")
])


def calculate_npldate(bldate_val, nodays_val):
    if nodays_val is None or nodays_val <= 89:
        return None
    adjusted_date = sas_days_to_date(int(bldate_val) + 90)
    npl_mm = adjusted_date.month
    npl_yy = adjusted_date.year
    npl_dd = month_end_of(adjusted_date).day
    return date(npl_yy, npl_mm, npl_dd)


loan = loan.with_columns([
    pl.struct(["bldate", "nodays"])
      .map_elements(lambda row: calculate_npldate(row["bldate"], row["nodays"]),
                    return_dtype=pl.Date)
      .alias("npldate")
])

loan = loan.unique(subset=["acctno", "noteno"], keep="first")
print(f"LOAN rows after deduplication: {loan.height}")

# =========================
# CISLN
# =========================
print("Processing CISLN in chunks...")
cisln = read_sas7bdat_filtered(
    CISLN_LOAN,
    column_filter={'seccust': '901'},
    chunk_size=CHUNK_SIZE
)

cisln = (
    cisln
      .select(["acctno", "newic", "custname"])
      .unique(subset=["acctno"], keep="first")
)
print(f"  CISLN rows after filter: {cisln.height}")

loan = loan.join(cisln, on="acctno", how="left")

# =====================================================================
# COLL / DESC file processing -- with record-length detection + validation
# =====================================================================
print("Processing COLL and DESC files...")

coll_specs = [
    ("ccollno", 4, 9, "pd"),
    ("acctno", 146, 151, "pd"),
    ("noteno", 153, 158, "pd")
]

desc_specs = [
    ("ccollno", 1, 11, "numeric"),
    ("cinstcl", 51, 52, "character"),
    ("natguar", 55, 56, "character"),
    ("census", 211, 220, "numeric"),
    ("tranche", 291, 298, "character")
]

# ---- Resolve COLL record length ----
if KNOWN_COLL_RECORD_LENGTH is not None:
    COLL_RECORD_LENGTH = KNOWN_COLL_RECORD_LENGTH
    coll_file_size = COLL_FILE.stat().st_size
    if coll_file_size % COLL_RECORD_LENGTH != 0:
        print(f"  WARNING: known COLL_RECORD_LENGTH={COLL_RECORD_LENGTH} does not evenly "
              f"divide file size {coll_file_size} -- this value may be stale.")
    else:
        print(f"  Using known COLL record length: {COLL_RECORD_LENGTH}")
else:
    print("Detecting COLL record length...")
    COLL_RECORD_LENGTH = detect_record_length(
        COLL_FILE, approx_length=158, probe_field_specs=coll_specs
    )

# ---- Resolve DESC record length (this was the actual bug: never guess this) ----
if KNOWN_DESC_RECORD_LENGTH is not None:
    DESC_RECORD_LENGTH = KNOWN_DESC_RECORD_LENGTH
    desc_file_size = DESC_FILE.stat().st_size
    if desc_file_size % DESC_RECORD_LENGTH != 0:
        print(f"  WARNING: known DESC_RECORD_LENGTH={DESC_RECORD_LENGTH} does not evenly "
              f"divide file size {desc_file_size} -- this value may be stale.")
    else:
        print(f"  Using known DESC record length: {DESC_RECORD_LENGTH}")
else:
    print("Detecting DESC record length (previous code guessed this from an assumed "
          "row count -- that produced a non-integer remainder and misaligned every field)...")
    # 84686 was the old (wrong) guess; give the detector a wide net around it,
    # and also try the field layout's own minimum span (>=298 bytes) as a floor.
    approx = max(84686, 298)
    DESC_RECORD_LENGTH = detect_record_length(
        DESC_FILE, approx_length=approx, probe_field_specs=desc_specs, tolerance=0.9
    )

print(f"COLL record length: {COLL_RECORD_LENGTH}")
print(f"DESC record length: {DESC_RECORD_LENGTH}")

try:
    print("\nReading COLL file...")
    coll, coll_stats = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
    print_decode_report("COLL", coll_stats)

    # ---- Validation gate: don't trust COLL's key fields until decode failure rate is low ----
    max_fail_rate = 0.0
    for col, fails in coll_stats["fail_counts"].items():
        total = coll_stats["total_counts"].get(col, 1)
        max_fail_rate = max(max_fail_rate, fails / total if total else 0.0)

    if max_fail_rate > 0.10:
        raise ValueError(
            f"COLL packed-decimal fields (CCOLLNO / ACCTNO / NOTENO) failed to decode "
            f"cleanly (worst field failure rate {max_fail_rate:.1%}, want <= 10%). This "
            f"means COLL_RECORD_LENGTH={COLL_RECORD_LENGTH} is likely still wrong, or the "
            f"byte offsets in coll_specs don't match the true copybook layout. Refusing "
            f"to proceed to the CCOLLNO join, since that would silently return near-empty "
            f"or wrong results. Confirm the COLL layout against the source copybook."
        )

    print("Reading DESC file...")
    desc, desc_stats = read_ebcdic_fixed_records(DESC_FILE, DESC_RECORD_LENGTH, desc_specs)
    print_decode_report("DESC", desc_stats)

    print("\n=== COLL Data Sample (first 3 rows) ===")
    print(coll.head(3))

    print("\n=== DESC Data Sample (first 3 rows) ===")
    print(desc.head(3))

    # ---- Validation gate: don't trust cinstcl/natguar until they look clean ----
    print("\nValidating decoded code fields before filtering...")
    cinstcl_ok = validate_code_field(desc, "cinstcl")
    natguar_ok = validate_code_field(desc, "natguar")

    if not (cinstcl_ok and natguar_ok):
        raise ValueError(
            "DESC code fields (CINSTCL / NATGUAR) do not look valid after decoding "
            "-- values contain non-alnum/control characters, which means the record "
            "length or field offsets are still wrong. Refusing to proceed to the "
            "CINSTCL='18' AND NATGUAR='06' filter, since that would silently return "
            "zero (or wrong) rows. Confirm the DESC layout (LRECL and column offsets) "
            "against the source copybook and re-run."
        )

    has_18 = desc.filter(pl.col('cinstcl') == '18').height
    has_06 = desc.filter(pl.col('natguar') == '06').height
    print(f"Rows with CINSTCL='18': {has_18}")
    print(f"Rows with NATGUAR='06': {has_06}")

    coll = coll.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))

    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Float64).alias("acctno"),
        pl.col("noteno").cast(pl.Float64).alias("noteno")
    ])

    coll = coll.sort(by="ccollno")
    desc = desc.sort(by="ccollno")

    coll_joined = coll.join(desc, on="ccollno", how="inner")
    print(f"\nCOLL rows after join: {coll_joined.height}")

    # Sanity check on join cardinality: ccollno should behave close to 1:1.
    # A large blow-up here is a strong signal of upstream key corruption.
    if coll.height > 0:
        ratio = coll_joined.height / coll.height
        if ratio > 5:
            print(f"  WARNING: join produced {ratio:.1f}x more rows than the COLL side "
                  f"({coll_joined.height} vs {coll.height}). This suggests CCOLLNO values "
                  f"are colliding due to decode corruption rather than a genuine 1:many "
                  f"relationship. Inspect ccollno distributions before trusting this join.")

    coll_filtered = coll_joined.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))
    print(f"COLL rows after filter: {coll_filtered.height}")

    coll = coll_filtered

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    print("\nAborting run: COLL/DESC could not be read and validated correctly. "
          "Fix the record length / field offsets (see messages above) before rerunning; "
          "continuing with an empty placeholder would silently produce an empty final output.")
    sys.exit(1)

print(f"\nFinal COLL rows: {coll.height}")

# =========================
# NPGS merge
# =========================
if loan.height > 0 and coll.height > 0:
    if loan.schema["acctno"] != pl.Float64:
        loan = loan.with_columns(pl.col("acctno").cast(pl.Float64).alias("acctno"))
    if loan.schema["noteno"] != pl.Float64:
        loan = loan.with_columns(pl.col("noteno").cast(pl.Float64).alias("noteno"))

npgs = loan.join(coll, on=["acctno", "noteno"], how="inner")
print(f"NPGS rows after COLL merge: {npgs.height}")

npgs = npgs.sort(by="pendbrh")

# =========================
# MICR
# =========================
print("Processing MICR file...")

micr_specs = [
    ("pendbrh", 1, 3, "numeric"),
    ("micrcd", 40, 44, "character")
]

try:
    micr = read_fixed_width_file(MICR_FILE, micr_specs, encoding='ascii')
    micr = micr.sort(by="pendbrh")
except Exception as e:
    print(f"Warning: Error reading MICR file: {e}")
    micr = pl.DataFrame(schema={"pendbrh": pl.Float64, "micrcd": pl.Utf8})

if npgs.height > 0 and micr.height > 0:
    if npgs.schema["pendbrh"] != micr.schema["pendbrh"]:
        npgs = npgs.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))
        micr = micr.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))

npgs = npgs.join(micr, on="pendbrh", how="left")

# =========================
# CVAR02 mapping from SCH
# =========================
print("Creating CVAR fields...")

npgs = npgs.with_columns([
    pl.lit("   ").alias("cvar02")
])

npgs = npgs.with_columns([
    pl.when(pl.col("sch") == "P93").then(pl.lit("93"))
     .when(pl.col("sch") == "P94").then(pl.lit("94"))
     .when(pl.col("sch") == "P101").then(pl.lit("101"))
     .otherwise(pl.col("cvar02"))
     .alias("cvar02")
])

npgs = npgs.filter(pl.col("cvar02") != "   ")

# =========================
# Final CVAR fields
# =========================
npgs = npgs.with_columns([
    pl.col("census").alias("cvar01"),
    pl.col("newic").alias("cvar03"),
    pl.col("custname").alias("cvar04"),
    pl.col("issued").alias("cvar05"),
    pl.col("acctno").alias("cvar06"),
    pl.lit("FL").alias("cvar07"),
    pl.col("netproc").alias("cvar08"),
    pl.col("balance").alias("cvar09"),
    pl.lit(0.00).alias("cvar10"),
    pl.col("arrears").alias("cvar11"),
    pl.lit("   ").alias("cvar12"),
    pl.lit("          ").alias("cvar13"),
    pl.lit("0233").alias("cvar14"),
    pl.col("micrcd").alias("cvar15"),
    pl.col("pendbrh").alias("branch"),
    pl.lit("TL").alias("cvar16"),
    pl.col("curbal").alias("cvar17"),
])

if "name" in npgs.columns:
    npgs = npgs.with_columns([
        pl.when(pl.col("cvar04") == "  ")
          .then(pl.col("name"))
          .otherwise(pl.col("cvar04"))
          .alias("cvar04")
    ])

npgs = npgs.with_columns([
    pl.when(pl.col("npldate").is_not_null())
      .then(pl.col("npldate").map_elements(format_date_ddmmyyyy, return_dtype=pl.Utf8))
      .otherwise(pl.lit("          "))
      .alias("cvar13")
])

npgs = npgs.with_columns([
    pl.lit(NORMDT).alias("normdt")
])

npgs = npgs.with_columns([
    pl.when((pl.col("arrears") >= 3) & pl.col("npldate").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("cvar12"))
      .alias("cvar12")
])

npgs = npgs.sort(by=["cvar06", "cvar01"])

if NPGS_SMEZ.exists():
    npla = read_sas7bdat(NPGS_SMEZ).sort(by=["cvar06", "cvar01"])
    npgs = npgs.join(npla, on=["cvar06", "cvar01"], how="left", suffix="_npla")
else:
    npgs = npgs.with_columns([
        pl.lit(None).alias("status"),
        pl.lit("          ").alias("ndate")
    ])


def adjust_cvar13(row):
    cvar12 = row.get("cvar12", "   ")
    status = row.get("status", "   ")
    ndate = row.get("ndate", "          ")
    cvar13 = row.get("cvar13", "          ")
    normdt = row.get("normdt", "          ")

    if cvar12 == "NPL":
        if status == "NPL":
            return ndate
        return cvar13
    else:
        if status == "NPL":
            return normdt
        if status == "   " and ndate != "          ":
            return ndate
        return cvar13


npgs = npgs.with_columns([
    pl.struct(["cvar12", "status", "ndate", "cvar13", "normdt"])
      .map_elements(adjust_cvar13, return_dtype=pl.Utf8)
      .alias("cvar13")
])

npgs = npgs.sort(by="cvar01")

for c in ["costctr", "balance", "curbal", "accrual", "tranche",
          "censust", "product", "natguar", "cinstcl"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

keep_cols = [
    "cvar01", "cvar02", "cvar03", "cvar04", "cvar05", "cvar06", "cvar07",
    "cvar08", "cvar09", "cvar10", "cvar11", "cvar12", "cvar13", "cvar14",
    "costctr", "balance", "curbal", "accrual", "tranche",
    "branch", "cvar15", "censust", "product", "natguar", "cinstcl", "sch",
    "cvar16", "cvar17"
]

out = npgs.select(keep_cols)
out = out.rename({col: col.upper() for col in out.columns})

# =========================
# Output via SASPy
# =========================
print(f"Writing NPGS.LNSMEZ{REPTMON}...")

out_pandas = out.to_pandas()

sas = saspy.SASsession(results='TEXT')

sas.submit(f"""
    libname npgs "{BASE_OUTPUT}";
    options nofmterr;
""")

sas_df = sas.df2sd(out_pandas, table='work.temp_out')

sas.submit(f"""
    data npgs.lnsmez{REPTMON};
        set work.temp_out;
        format CVAR01 CVAR06 10.
               CVAR03 $15.
               CVAR04 $50.
               CVAR14 $4.
               CVAR13 $10.
               CVAR08 CVAR09 CVAR10 CVAR17 10.2
               CVAR11 5.
               CVAR02 $3.
               CVAR12 $3.
               CVAR15 $5.
               CVAR16 $2.
               CVAR07 $2.;
    run;

    proc datasets lib=npgs nolist;
        modify lnsmez{REPTMON};
        label
            CVAR01='Census'
            CVAR02='Schedule Code'
            CVAR03='New IC'
            CVAR04='Customer Name'
            CVAR05='Issue Date'
            CVAR06='Account Number'
            CVAR07='Flag'
            CVAR08='Net Proceeds'
            CVAR09='Balance'
            CVAR10='Zero Balance'
            CVAR11='Arrears'
            CVAR12='NPL Status'
            CVAR13='NPL Date'
            CVAR14='Constant Value'
            CVAR15='MICR Code'
            CVAR16='Type'
            CVAR17='Current Balance';
    run;
""")

print(f"Successfully wrote NPGS.LNSMEZ{REPTMON} to {BASE_OUTPUT}")

sas.endsas()
