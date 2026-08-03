"""
EIBWCRMA pipeline -- corrected Python port of the SAS program.

============================================================================
KNOWN OPEN ITEMS -- confirm these before running in production:
============================================================================
1. CRMA_TXT path/filename below is a guess (same BASE, following the
   conv/islamic/cisca/cisdp folder pattern). Update to the real path.
2. FORATE_SRC: the $FORATE SAS format (currency code -> FX rate) lives in
   the MISCA format library and isn't available here. Until you supply a
   real CURCODE->FORATE mapping (see load_forate_lookup), non-MYR accounts
   will get FORATE = null and MTDAVBAL/CURBAL will NOT be converted. The
   script prints a warning listing which CURCODE values were affected so
   you can see the blast radius.
3. REPTDATE: SAS reads this from a DEPO.REPTDATE dataset (an actual
   business-date table), not simply "yesterday". This script still uses
   datetime.now() - 1 day as the earlier version did. If DEPO.REPTDATE
   reflects a business calendar (skips weekends/holidays), this will
   diverge on Mondays / after holidays. Replace REPTDATE below with a real
   read from that source if available.
4. Encoding of CRMA_TXT assumed latin-1 (permissive). Confirm if NRICNO /
   AANO ever contain non-ASCII characters.
============================================================================

FIXES applied vs. the earlier draft:
- CRMA is now read as the actual raw fixed-width base file (previously the
  script incorrectly built EXTCRMA from DEPOSIT itself, silently dropping
  any CRMA record with no deposit match and hardcoding MATCHIND='M').
- SAVING/CURRENT/FD column selection uses the real, fixed KEEP list (no
  more set-intersection across DEPO/IDEPO pairs -- that's what silently
  dropped OPENDT/CLOSEDT/MTDAVBAL/etc. before).
- CURCODE-based currency conversion (FORBAL/FORATE/CURBAL/MTDAVBAL) is
  implemented, matching the SAS DATA step logic exactly (SAVING converts
  CURBAL and preserves FORBAL; CURRENT/FD only convert MTDAVBAL; GIA/XAU
  balances are excluded from the MTDAVBAL conversion).
- CIS merges (SAVING/CURRENT/FD + CISDP/CISCA) changed from inner join to
  LEFT join. SAS's "MERGE X(IN=A) CIS; IF A;" keeps ALL of X regardless of
  a CIS match -- the earlier inner join was silently dropping every
  deposit account whose customer didn't have a matching SECCUST='901' CIS
  record. This was a real data-loss bug.
- LN_NOTE (20GB/6M rows) read via chunked, column-pruned pyreadstat instead
  of loading the whole file into memory.
- EXTMIS output written as a true fixed-width file matching the SAS PUT
  statement's exact column positions/formats, not pipe-delimited CSV
  (the earlier delimited version would have broken any downstream fixed-
  width consumer of this file).
"""

from __future__ import annotations
from pathlib import Path
from datetime import date, datetime, timedelta
import gc
import polars as pl
import pyarrow.parquet as pq
import pyreadstat
import pandas as pd

BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWCRMA")

# ---- input paths ----
DEPO_SAV   = BASE / "conv" / "saving.sas7bdat"
DEPO_CUR   = BASE / "conv" / "current.sas7bdat"
DEPO_FD    = BASE / "conv" / "fd.sas7bdat"

IDEPO_SAV  = BASE / "islamic" / "saving.sas7bdat"
IDEPO_CUR  = BASE / "islamic" / "current.sas7bdat"
IDEPO_FD   = BASE / "islamic" / "fd.sas7bdat"

CISCA_DEP  = BASE / "cisca" / "deposit.sas7bdat"
CISDP_DEP  = BASE / "cisdp" / "deposit.sas7bdat"

LN_NOTE    = BASE / "conv" / "lnnote.sas7bdat"
ILN_NOTE   = BASE / "islamic" / "lnnote.sas7bdaz                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  t"

# TODO: confirm real filename/subfolder -- guessed to follow the same
# BASE/<source>/<file> pattern as everything else.
CRMA_TXT     = BASE / "crma.txt"

# TODO: confirm real filename/subfolder. This is a .sas7bdat -- either a
# CNTLOUT-style format-catalog dump (FMTNAME/START/LABEL/...) or  a
# pre-filtered CURCODE/FORATE table; load_forate_lookup() handles either.
FORATE_SRC   = BASE / "forate.sas7bdat"

OUT_BEP    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWCRMA")
OUT_BEP.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Generic helpers
# ============================================================================

def read_sas7bdat(path: Path, usecols: list[str] | None = None) -> pl.DataFrame:
    """Read a SAS7BDAT file into Polars, optionally restricting to usecols."""
    df, _meta = pyreadstat.read_sas7bdat(str(path), usecols=usecols)
    return pl.from_pandas(df)


def read_sas_chunked(
    path: Path,
    usecols: list[str] | None = None,
    chunksize: int = 500_000,
    filter_expr: pl.Expr | None = None,
) -> pl.DataFrame:
    """Stream a large .sas7bdat in row-chunks, selecting only usecols and
    applying filter_expr per chunk so memory stays bounded. Use this for
    LN_NOTE (20GB / 6M rows) instead of read_sas7bdat."""
    chunks: list[pl.DataFrame] = []
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat, str(path), chunksize=chunksize, usecols=usecols,
    )
    for df_chunk, _meta in reader:
        pl_chunk = pl.from_pandas(df_chunk)
        if filter_expr is not None:
            pl_chunk = pl_chunk.filter(filter_expr)
        if pl_chunk.height:
            chunks.append(pl_chunk)
        del df_chunk, pl_chunk
    gc.collect()
    return pl.concat(chunks, how="vertical", rechunk=True) if chunks else pl.DataFrame(schema=usecols)


def write_sas7bdat(df: pl.DataFrame, path: Path):
    import saspy
    sas = saspy.SASsession()
    pdf = df.to_pandas()
    sas.df2sd(pdf, table="temp_table")
    sas.submit(f"""
        PROC EXPORT DATA=temp_table
            OUTFILE="{path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    sas.endsas()


def write_parquet(df: pl.DataFrame, path: Path):
    pq.write_table(df.to_arrow(), path)


def z11_first8_to_mmddyyyy_date(n) -> date | None:
    """Matches: INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.)"""
    if n is None:
        return None
    try:
        s = str(int(n)).zfill(11)[:8]  # MMDDYYYY
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


# ============================================================================
# CRMA raw fixed-width base file
# ============================================================================

def read_crma_raw(path: Path, encoding: str = "latin-1") -> pl.DataFrame:
    """Matches SAS:
        INFILE CRMA;
        INPUT  @001 NRICNO  $20.
               @021 CNTIC     5.
               @026 ACCTNO   10.
               @046 CNTAC     5.
               @051 AANO    $13.
               ;
    Columns 36-45 are an unused gap in the source layout. AANO is
    intentionally truncated to 13 chars -- some rows show a longer
    ';'-joined string past column 63 in the raw text, but that's outside
    the field and is never read; repeated rows with incrementing CNTAC
    represent additional loan refs instead.
    """
    colspecs = [(0, 20), (20, 25), (25, 35), (45, 50), (50, 63)]
    names = ["NRICNO", "CNTIC", "ACCTNO", "CNTAC", "AANO"]
    pdf = pd.read_fwf(str(path), colspecs=colspecs, names=names, dtype=str, encoding=encoding)

    pdf["NRICNO"] = pdf["NRICNO"].str.strip()
    pdf["AANO"] = pdf["AANO"].str.strip()
    pdf["CNTIC"] = pdf["CNTIC"].str.strip().astype("Int64")
    pdf["ACCTNO"] = pdf["ACCTNO"].str.strip().astype("Int64")
    pdf["CNTAC"] = pdf["CNTAC"].str.strip().astype("Int64")

    return pl.from_pandas(pdf)


# ============================================================================
# FORATE (FX rate) lookup -- placeholder until MISCA source is located
# ============================================================================

def sas_days_to_date(v) -> date | None:
    """SAS dates are stored as days-since-1960-01-01. Use this only if
    pyreadstat returns REPTDATE as a raw number rather than already
    converting it to a python date (it converts automatically when the
    source column carries a SAS date format -- this is a fallback)."""
    if v is None:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(v))
    except Exception:
        return None


def load_forate_lookup(path: Path, as_of: date) -> pl.DataFrame:
    """FORATE_SRC is a .sas7bdat. Handles three possible shapes:
      (a) pre-filtered (CURCODE, FORATE) static table
      (b) CNTLOUT format-catalog dump (FMTNAME/START/LABEL/...)
      (c) daily FX rate table (CURCODE, SPOTRATE, REPTDATE) -- picks the
          most recent SPOTRATE on or before `as_of` per currency, since a
          rate may not be published every single day (weekends/holidays)."""
    empty = pl.DataFrame({"CURCODE": [], "FORATE": []}, schema={"CURCODE": pl.Utf8, "FORATE": pl.Float64})

    if not path.exists():
        print(f"[WARN] FORATE source not found at {path} -- "
              f"non-MYR currency conversion will be skipped (FORATE will be null).")
        return empty

    df = read_sas7bdat(path)

    if {"CURCODE", "FORATE"}.issubset(df.columns):
        return df.select([
            pl.col("CURCODE").str.strip_chars(),
            pl.col("FORATE").cast(pl.Float64, strict=False),
        ]).unique(subset=["CURCODE"], keep="first")

    if {"CURCODE", "SPOTRATE", "REPTDATE"}.issubset(df.columns):
        if df.schema["REPTDATE"] not in (pl.Date, pl.Datetime):
            df = df.with_columns(
                pl.col("REPTDATE").map_elements(sas_days_to_date, return_dtype=pl.Date)
            )
        candidates = df.filter(pl.col("REPTDATE") <= as_of)
        if candidates.height == 0:
            print(f"[WARN] {path}: no FX rate rows on or before {as_of} -- "
                  f"conversion will be skipped.")
            return empty
        latest_dates = candidates.group_by("CURCODE").agg(pl.col("REPTDATE").max())
        out = (
            latest_dates.join(df, on=["CURCODE", "REPTDATE"], how="left")
            .select([
                pl.col("CURCODE").str.strip_chars(),
                pl.col("SPOTRATE").cast(pl.Float64, strict=False).alias("FORATE"),
            ])
            .unique(subset=["CURCODE"], keep="first")
        )
        stale = (
            latest_dates.filter(pl.col("REPTDATE") < as_of)
            .select(["CURCODE", "REPTDATE"]).to_dicts()
        )
        if stale:
            print(f"[WARN] {path}: using a rate older than {as_of} for some currencies "
                  f"(most recent available on/before that date): {stale}")
        return out

    if {"FMTNAME", "START", "LABEL"}.issubset(df.columns):
        out = (
            df.filter(pl.col("FMTNAME").str.to_uppercase() == "FORATE")
              .select([
                  pl.col("START").str.strip_chars().alias("CURCODE"),
                  pl.col("LABEL").cast(pl.Float64, strict=False).alias("FORATE"),
              ])
              .unique(subset=["CURCODE"], keep="first")
        )
        if out.height == 0:
            print(f"[WARN] {path} read OK but no FMTNAME='FORATE' rows found. "
                  f"Available FMTNAME values: {df.select('FMTNAME').unique().to_series().to_list()[:20]}")
        return out

    print(f"[WARN] FORATE source at {path} has unrecognized columns {df.columns} -- "
          f"skipping conversion.")
    return empty


# ============================================================================
# REPTDATE
# ============================================================================
# TODO: SAS reads this from DEPO.REPTDATE (a business-date table), not
# simply "yesterday" -- replace if that source is available to you.
NOW = datetime.now()
YESTERDAY = NOW - timedelta(days=1)
REPTDATE = YESTERDAY.date()
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON = f"{REPTDATE.month:02d}"
RDATE = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year % 100:02d}"
NOWK = "1" if REPTDATE.day == 8 else "2" if REPTDATE.day == 15 else "3" if REPTDATE.day == 22 else "4"

# ============================================================================
# Load FORATE_LOOKUP (must be loaded before the functions that use it)
# ============================================================================
FORATE_LOOKUP = load_forate_lookup(FORATE_SRC, REPTDATE)

# ============================================================================
# Deposit table building functions
# ============================================================================

# Source columns from the SAS7BDAT files with their expected types
SOURCE_COLS = {
    "BRANCH": pl.Utf8,
    "ACCTNO": pl.Int64,
    "MTDAVBAL": pl.Float64,
    "PRODUCT": pl.Utf8,
    "OPENDT": pl.Float64,  # numeric in SAS, will be converted to date later
    "OPENIND": pl.Utf8,
    "CLOSEDT": pl.Float64,  # numeric in SAS, will be converted to date later
    "CURBAL": pl.Float64,
    "AVGAMT": pl.Float64,
    "INACTIVE": pl.Utf8,
    "CURCODE": pl.Utf8,
}

# Final KEEP list per SAS (FORBAL/FORATE added after conversion logic runs)
FINAL_DEPOSIT_COLS = ["BRANCH", "ACCTNO", "MTDAVBAL", "PRODUCT", "OPENDT", "OPENIND",
                      "CLOSEDT", "CURBAL", "AVGAMT", "INACTIVE", "FORBAL", "FORATE"]


def align_to_schema(df: pl.DataFrame, col_specs: dict[str, pl.DataType]) -> pl.DataFrame:
    """Select columns, adding any genuinely-missing ones as typed nulls
    with the correct dtype instead of silently dropping columns."""
    exprs = []
    for col, dtype in col_specs.items():
        if col in df.columns:
            exprs.append(pl.col(col).cast(dtype, strict=False))
        else:
            exprs.append(pl.lit(None).cast(dtype).alias(col))
    return df.select(exprs)


def apply_currency_conversion(df: pl.DataFrame, table_type: str) -> pl.DataFrame:
    """Mirrors SAS:
      SAVING:
        IF CURCODE NE 'MYR':
           FORATE = lookup(CURCODE); FORBAL = CURBAL; CURBAL = CURBAL*FORATE
           IF CURCODE NE 'XAU': MTDAVBAL = MTDAVBAL*FORATE
      CURRENT / FD:
        IF CURCODE NE 'MYR':
           FORATE = lookup(CURCODE); MTDAVBAL = MTDAVBAL*FORATE
           (CURBAL, FORBAL untouched)
    """
    df = df.join(FORATE_LOOKUP, on="CURCODE", how="left")
    is_foreign = pl.col("CURCODE") != "MYR"

    if table_type == "saving":
        df = df.with_columns([
            pl.when(is_foreign).then(pl.col("CURBAL")).otherwise(pl.lit(None).cast(pl.Float64)).alias("FORBAL"),
            pl.when(is_foreign).then(pl.col("CURBAL") * pl.col("FORATE")).otherwise(pl.col("CURBAL")).alias("CURBAL"),
            pl.when(is_foreign & (pl.col("CURCODE") != "XAU"))
              .then(pl.col("MTDAVBAL") * pl.col("FORATE"))
              .otherwise(pl.col("MTDAVBAL")).alias("MTDAVBAL"),
        ])
    else:  # current / fd
        df = df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("FORBAL"),
            pl.when(is_foreign).then(pl.col("MTDAVBAL") * pl.col("FORATE"))
              .otherwise(pl.col("MTDAVBAL")).alias("MTDAVBAL"),
        ])

    # FORATE stays null for MYR rows (matches SAS leaving it uninitialized/missing)
    df = df.with_columns(
        pl.when(is_foreign).then(pl.col("FORATE")).otherwise(pl.lit(None).cast(pl.Float64)).alias("FORATE")
    )

    # Surface which currencies had no rate available, if FORATE_LOOKUP is empty/incomplete
    unmatched = (
        df.filter(is_foreign & pl.col("FORATE").is_null())
          .select("CURCODE").unique().to_series().to_list()
    )
    if unmatched:
        print(f"[WARN] {table_type}: no FORATE for currencies {unmatched} -- "
              f"MTDAVBAL/CURBAL left unconverted for these rows.")

    return df.select(FINAL_DEPOSIT_COLS)


def build_deposit_table(depo_path: Path, idepo_path: Path, table_type: str) -> pl.DataFrame:
    # Read only the columns we need (use the keys of SOURCE_COLS)
    usecols = list(SOURCE_COLS.keys())
    depo = read_sas7bdat(depo_path, usecols=usecols)
    idepo = read_sas7bdat(idepo_path, usecols=usecols)
    
    # Align both to the same schema with correct types
    depo = align_to_schema(depo, SOURCE_COLS)
    idepo = align_to_schema(idepo, SOURCE_COLS)
    
    combined = pl.concat([depo, idepo], how="vertical", rechunk=True)
    combined = apply_currency_conversion(combined, table_type)
    return combined.unique(subset=["ACCTNO"], keep="first")


# ============================================================================
# Build SAVING / CURRENT / FD
# ============================================================================

SAVING = build_deposit_table(DEPO_SAV, IDEPO_SAV, "saving")
CURRENT = build_deposit_table(DEPO_CUR, IDEPO_CUR, "current")
FD = build_deposit_table(DEPO_FD, IDEPO_FD, "fd")


# ============================================================================
# CIS filters (SECCUST='901')
# ============================================================================

CISCA = (read_sas7bdat(CISCA_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO", "CUSTNAM1", "NEWIC", "OLDIC"])
           .with_columns(pl.col("ACCTNO").cast(pl.Int64, strict=False))
           .unique(subset=["ACCTNO"], keep="first"))
CISDP = (read_sas7bdat(CISDP_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO", "CUSTNAM1", "NEWIC", "OLDIC"])
           .with_columns(pl.col("ACCTNO").cast(pl.Int64, strict=False))
           .unique(subset=["ACCTNO"], keep="first"))

# FIX: SAS "MERGE X(IN=A) CIS; IF A;" keeps ALL of X regardless of a CIS
# match -- this is a LEFT join, not inner. The earlier draft used an inner
# join here, which silently dropped every deposit account whose customer
# had no matching SECCUST='901' CIS record.
SAVING = SAVING.join(CISDP, on="ACCTNO", how="left")
CURRENT = CURRENT.join(CISCA, on="ACCTNO", how="left")
FD = FD.join(CISDP, on="ACCTNO", how="left")


# ============================================================================
# DEPOSIT
# ============================================================================

DEPOSIT = pl.concat([SAVING, CURRENT, FD], how="vertical", rechunk=True)

bad_ic = {"", "00000000000", "0", "-"}
DEPOSIT = DEPOSIT.with_columns([
    pl.when(~pl.col("NEWIC").is_in(bad_ic)).then(pl.col("NEWIC"))
      .when(~pl.col("OLDIC").is_in(bad_ic)).then(pl.col("OLDIC"))
      .otherwise(pl.lit("")).alias("NRICCIS")
])

DEPOSIT = DEPOSIT.with_columns([
    pl.col("OPENDT").map_elements(z11_first8_to_mmddyyyy_date, return_dtype=pl.Date).alias("OPENDT"),
    pl.col("CLOSEDT").map_elements(z11_first8_to_mmddyyyy_date, return_dtype=pl.Date).alias("CLOSEDT"),
]).unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ============================================================================
# EXTCRMA base (from raw CRMA file) + DEPOSIT match
# ============================================================================

EXTCRMA_BASE = read_crma_raw(CRMA_TXT).sort("ACCTNO")

# Ensure ACCTNO types match for join (CRMA already has Int64 from read_crma_raw)
EXTCRMA = EXTCRMA_BASE.join(DEPOSIT, on="ACCTNO", how="left")
EXTCRMA = EXTCRMA.with_columns([
    pl.when(pl.col("PRODUCT").is_not_null()).then(pl.lit("M")).otherwise(pl.lit("F")).alias("MATCHIND"),
])
EXTCRMA = EXTCRMA.with_columns([
    pl.when((pl.col("INACTIVE").is_null() | (pl.col("INACTIVE") == "")) & (pl.col("MATCHIND") == "M"))
      .then(pl.lit("A")).otherwise(pl.col("INACTIVE")).alias("INACTIVE"),
])
EXTCRMA = EXTCRMA.sort("AANO")


# ============================================================================
# LOAN match (chunked, column-pruned read of the large lnnote files)
# ============================================================================

LN_NOTE_COLS = ["VINNO", "ESCRACCT"]

def load_loan(ln_note_path: Path, iln_note_path: Path, chunksize: int = 500_000) -> pl.DataFrame:
    conv_loan = read_sas_chunked(ln_note_path, usecols=LN_NOTE_COLS, chunksize=chunksize,
                                  filter_expr=pl.col("VINNO") != "")
    islamic_loan = read_sas_chunked(iln_note_path, usecols=LN_NOTE_COLS, chunksize=chunksize,
                                     filter_expr=pl.col("VINNO") != "")
    return (
        pl.concat([conv_loan, islamic_loan], how="vertical", rechunk=True)
        .rename({"VINNO": "AANO"})
        .unique(subset=["AANO"], keep="first")
    )

LOAN = load_loan(LN_NOTE, ILN_NOTE)

# SAS: MERGE EXTCRMA(IN=A) LOAN; IF A;  -> left join, keep all EXTCRMA rows
EXTCRMA = EXTCRMA.join(LOAN, on="AANO", how="left")


# ============================================================================
# Derived fields: date parts, scaling, PRODTYPE
# ============================================================================

def prodtype_from_acctno(v: int | None) -> str | None:
    if v is None:
        return None
    if (4000000000 <= v <= 4999999999) or (5000000000 <= v <= 5999999999) or \
       (6000000000 <= v <= 6589999999) or (6600000000 <= v <= 6999999999):
        return "SA"
    if (3000000000 <= v <= 3589999999) or (3600000000 <= v <= 3999999999):
        return "CA"
    if (1000000000 <= v <= 1589999999) or (1600000000 <= v <= 1999999999) or \
       (7000000000 <= v <= 7999999999):
        return "FD"
    if (1590000000 <= v <= 1599999999) or (1689999999 <= v <= 1699999999) or \
       (1789999999 <= v <= 1799999999):
        return "FCYFD"
    if (3590000000 <= v <= 3599999999) or (3790000000 <= v <= 3799999999):
        return "FCYCA"
    if 6590000000 <= v <= 6599999999:
        return "GIA"
    return None

EXTCRMA = EXTCRMA.with_columns([
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.day()).otherwise(pl.lit(0)).alias("OPENDD"),
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.month()).otherwise(pl.lit(0)).alias("OPENMM"),
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.year()).otherwise(pl.lit(0)).alias("OPENYY"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.day()).otherwise(pl.lit(0)).alias("CLOSEDD"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.month()).otherwise(pl.lit(0)).alias("CLOSEMM"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.year()).otherwise(pl.lit(0)).alias("CLOSEYY"),
    (pl.col("MTDAVBAL").round(2) * 100).alias("MTDAVBAL"),
    (pl.col("AVGAMT").round(2) * 100).alias("AVGAMT"),
    (pl.col("CURBAL").round(2) * 100).alias("CURBAL"),
]).with_columns([
    pl.col("ACCTNO").map_elements(prodtype_from_acctno, return_dtype=pl.Utf8).alias("PRODTYPE")
]).sort("ACCTNO")


# ============================================================================
# Write EXTCRMA outputs
# ============================================================================

base_name = f"EXTCRMA{REPTMON}{NOWK}"
EXTCRMA.write_csv(OUT_BEP / f"{base_name}.txt", separator="|")   # ad hoc/audit copy
write_sas7bdat(EXTCRMA, OUT_BEP / f"{base_name}.sas7bdat")
write_parquet(EXTCRMA, OUT_BEP / f"{base_name}.parquet")


# ============================================================================
# EXTMIS -- fixed-width output matching the SAS PUT statement exactly
# ============================================================================

EXTMIS = EXTCRMA.select([
    "NRICNO", "CNTIC", "ACCTNO", "CNTAC", "AANO", "MATCHIND",
    "MTDAVBAL", "PRODUCT", "PRODTYPE", "OPENYY", "OPENMM", "OPENDD",
    "OPENIND", "INACTIVE", "CLOSEYY", "CLOSEMM", "CLOSEDD",
    "CURBAL", "AVGAMT", "BRANCH", "ESCRACCT", "CUSTNAM1", "NRICCIS",
])

# (start_col, width, kind) -- kind: 's'=left-justified space-pad,
# 'n'=right-justified space-pad (plain numeric), 'z'=zero-padded numeric
_EXTMIS_SPEC = [
    ("NRICNO", 1, 20, "s"), ("CNTIC", 21, 5, "n"), ("ACCTNO", 26, 20, "n"),
    ("CNTAC", 46, 5, "n"), ("AANO", 51, 13, "s"), ("MATCHIND", 65, 1, "s"),
    ("MTDAVBAL", 67, 16, "z"), ("PRODUCT", 84, 3, "z"), ("PRODTYPE", 88, 5, "s"),
    ("OPENYY", 94, 4, "z"), ("OPENMM", 98, 2, "z"), ("OPENDD", 100, 2, "z"),
    ("OPENIND", 103, 1, "s"), ("INACTIVE", 105, 1, "s"), ("CLOSEYY", 107, 4, "z"),
    ("CLOSEMM", 111, 2, "z"), ("CLOSEDD", 113, 2, "z"), ("CURBAL", 116, 16, "z"),
    ("AVGAMT", 133, 16, "z"), ("BRANCH", 150, 3, "z"), ("ESCRACCT", 154, 20, "n"),
    ("CUSTNAM1", 175, 40, "s"), ("NRICCIS", 216, 20, "s"),
]
_EXTMIS_LEN = max(c[1] + c[2] - 1 for c in _EXTMIS_SPEC)


def _fmt_s(value, width):
    s = "" if value is None else str(value)
    return s[:width].ljust(width)

def _fmt_n(value, width):
    ival = 0 if value is None else int(value)
    s = str(ival)
    return s[-width:] if len(s) > width else s.rjust(width)

def _fmt_z(value, width):
    ival = 0 if value is None else int(value)
    sign = "-" if ival < 0 else ""
    digits = str(abs(ival)).zfill(width - len(sign))
    s = sign + digits
    return s[-width:] if len(s) > width else s


def write_extmis_fixed_width(df: pl.DataFrame, path: Path):
    fmt_fn = {"s": _fmt_s, "n": _fmt_n, "z": _fmt_z}
    with open(path, "w") as f:
        for row in df.to_dicts():
            line = [" "] * _EXTMIS_LEN
            for name, start, width, kind in _EXTMIS_SPEC:
                s = fmt_fn[kind](row.get(name), width)
                idx = start - 1
                line[idx: idx + width] = list(s)
            f.write("".join(line) + "\n")


base_name_mis = f"EXTMIS{REPTMON}{NOWK}"
write_extmis_fixed_width(EXTMIS, OUT_BEP / f"{base_name_mis}.txt")
write_sas7bdat(EXTMIS, OUT_BEP / f"{base_name_mis}.sas7bdat")
write_parquet(EXTMIS, OUT_BEP / f"{base_name_mis}.parquet")
