"""
EIBWCRMA pipeline -- corrected Python port of the SAS program.

============================================================================
KNOWN OPEN ITEMS -- confirm these before running in production:
============================================================================
1. CRMA_TXT path/filename below is a guess (same BASE, following the
   conv/islamic/cisca/cisdp folder pattern). Update to the real path.
2. REPTDATE: SAS reads this from a DEPO.REPTDATE dataset (an actual
   business-date table), not simply "yesterday". This script still uses
   datetime.now() - 1 day. If DEPO.REPTDATE reflects a business calendar
   (skips weekends/holidays), this will diverge on Mondays / after
   holidays. Replace REPTDATE below with a real read from that source if
   available.
3. Encoding of CRMA_TXT assumed latin-1 (permissive). Confirm if NRICNO /
   AANO ever contain non-ASCII characters.
4. SOURCE_COLS dtypes for BRANCH/PRODUCT are best-guess (currently Utf8).
   The EXTMIS output formats them with Z-format (zero-padded numeric),
   which strongly suggests they're numeric in the real source schema.
   Check the [WARN] output from write_extmis_fixed_width on your next run
   -- if BRANCH/PRODUCT show up in the bad-field counts, switch their
   dtype here to pl.Int64/pl.Float64.
============================================================================
"""

from __future__ import annotations
from pathlib import Path
from datetime import date, datetime, timedelta
import gc
import multiprocessing
import time
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
ILN_NOTE   = BASE / "islamic" / "lnnote.sas7bdat"

# TODO: confirm real filename/subfolder
CRMA_TXT     = BASE / "crma.txt"

# TODO: confirm real filename/subfolder -- this is a .sas7bdat, either a
# CNTLOUT-style format-catalog dump, a pre-filtered CURCODE/FORATE table,
# or (as confirmed) a daily (CURCODE, SPOTRATE, REPTDATE) rate table.
FORATE_SRC   = BASE / "forate.sas7bdat"

OUT_BEP    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWCRMA")
OUT_BEP.mkdir(parents=True, exist_ok=True)

# Lightweight profiling: prints elapsed time since the last _tick() call, so
# you can see exactly which stage is actually slow instead of guessing.
_LAST_T = time.perf_counter()
def _tick(label: str):
    global _LAST_T
    now = time.perf_counter()
    print(f"[TIME] {label}: {now - _LAST_T:.1f}s")
    _LAST_T = now


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
    applying filter_expr per chunk so memory stays bounded. Kept as a
    fallback for genuinely wide/huge reads; LN_NOTE now uses the faster
    parallel reader below since it only needs 2 narrow columns."""
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


def read_sas_parallel(
    path: Path,
    usecols: list[str] | None = None,
    num_processes: int | None = None,
) -> pl.DataFrame:
    """Read a .sas7bdat across multiple CPU cores in parallel. Safe to use
    here (rather than the chunked reader) because usecols narrows LN_NOTE
    down to 2 small string columns -- the full 6M-row result for just
    those columns is small enough to hold in memory outright, so we can
    trade the chunk-by-chunk memory discipline for wall-clock speed."""
    df, _meta = pyreadstat.read_file_multiprocessing(
        pyreadstat.read_sas7bdat,
        str(path),
        usecols=usecols,
        num_processes=num_processes or multiprocessing.cpu_count(),
    )
    return pl.from_pandas(df)


def write_sas7bdat_batch(items: list[tuple[pl.DataFrame, Path]]):
    """Write multiple DataFrames to .sas7bdat files using ONE shared SAS
    session (avoids paying SAS subprocess startup cost per file).

    FIX: the previous version used PROC EXPORT ... DBMS=SAS7BDAT, which is
    invalid -- PROC EXPORT is for foreign formats (CSV/XLSX/etc). A
    .sas7bdat IS the native SAS dataset format, so you write it by
    assigning a LIBNAME to the target directory and writing the dataset
    directly into that libref (df2sd supports a `libref` argument for
    exactly this, skipping the extra WORK-copy step too).
    """
    import saspy
    sas = saspy.SASsession()
    try:
        for df, path in items:
            t0 = time.perf_counter()
            outdir = str(path.parent)
            dsname = path.stem  # SAS dataset name, must be <=32 chars, start with a letter

            lib_result = sas.submit(f'libname outlib "{outdir}";')
            if "ERROR" in lib_result.get("LOG", ""):
                print(lib_result["LOG"])
                raise RuntimeError(f"Failed to assign libname for {outdir}")

            pdf = df.to_pandas()
            sas.df2sd(pdf, table=dsname, libref="outlib")

            check = sas.submit(
                f'proc sql noprint; select count(*) into :n from outlib.{dsname}; quit; '
                f'%put NROWS=&n;'
            )
            if "ERROR" in check.get("LOG", ""):
                print(check["LOG"])
                raise RuntimeError(f"Verification failed writing {path}")

            sas.submit("libname outlib clear;")
            print(f"[TIME] wrote {path.name} ({df.height} rows) in {time.perf_counter() - t0:.1f}s")
    finally:
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
# FORATE (FX rate) lookup
# ============================================================================

def sas_days_to_date(v) -> date | None:
    """SAS dates are stored as days-since-1960-01-01. Fallback only -- used
    if pyreadstat returns REPTDATE as a raw number rather than already
    converting it (it converts automatically when the source column
    carries a SAS date format)."""
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
      (c) daily FX rate table (CURCODE, SPOTRATE, REPTDATE) -- confirmed
          shape in production. Picks the most recent SPOTRATE on or before
          `as_of` per currency, since a rate may not be published every
          single day (weekends/holidays)."""
    empty = pl.DataFrame({"CURCODE": [], "FORATE": []}, schema={"CURCODE": pl.Utf8, "FORATE": pl.Float64})

    if not path.exists():
        print(f"[WARN] FORATE source not found at {path} -- "
              f"non-MYR currency conversion will be skipped (FORATE will be null).")
        return empty

    df = read_sas7bdat(path)

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

    if {"CURCODE", "FORATE"}.issubset(df.columns):
        return df.select([
            pl.col("CURCODE").str.strip_chars(),
            pl.col("FORATE").cast(pl.Float64, strict=False),
        ]).unique(subset=["CURCODE"], keep="first")

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
NOW = datetime.now()
YESTERDAY = NOW - timedelta(days=1)
REPTDATE = YESTERDAY.date()
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON = f"{REPTDATE.month:02d}"
RDATE = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year % 100:02d}"
NOWK = "1" if REPTDATE.day == 8 else "2" if REPTDATE.day == 15 else "3" if REPTDATE.day == 22 else "4"

FORATE_LOOKUP = load_forate_lookup(FORATE_SRC, REPTDATE)


# ============================================================================
# Deposit table building
# ============================================================================

# Source columns from the SAS7BDAT files with their expected types.
# BRANCH/PRODUCT dtypes are a best guess -- see KNOWN OPEN ITEMS #4 above.
SOURCE_COLS = {
    "BRANCH": pl.Utf8,
    "ACCTNO": pl.Int64,
    "MTDAVBAL": pl.Float64,
    "PRODUCT": pl.Utf8,
    "OPENDT": pl.Float64,
    "OPENIND": pl.Utf8,
    "CLOSEDT": pl.Float64,
    "CURBAL": pl.Float64,
    "AVGAMT": pl.Float64,
    "INACTIVE": pl.Utf8,
    "CURCODE": pl.Utf8,
}

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


def apply_currency_conversion(df: pl.DataFrame, table_type: str, forate_lookup: pl.DataFrame) -> pl.DataFrame:
    """Mirrors SAS:
      SAVING:
        IF CURCODE NE 'MYR':
           FORATE = lookup(CURCODE); FORBAL = CURBAL; CURBAL = CURBAL*FORATE
           IF CURCODE NE 'XAU': MTDAVBAL = MTDAVBAL*FORATE
      CURRENT / FD:
        IF CURCODE NE 'MYR':
           FORATE = lookup(CURCODE); MTDAVBAL = MTDAVBAL*FORATE
           (CURBAL, FORBAL untouched)

    forate_lookup is passed explicitly (rather than read as a module
    global) so this function has no forward-reference to a name defined
    later in the module -- avoids the "possibly unbound" warning your
    editor was flagging on the old FORATE_LOOKUP global reference.
    """
    df = df.join(forate_lookup, on="CURCODE", how="left")
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

    df = df.with_columns(
        pl.when(is_foreign).then(pl.col("FORATE")).otherwise(pl.lit(None).cast(pl.Float64)).alias("FORATE")
    )

    unmatched = (
        df.filter(is_foreign & pl.col("FORATE").is_null())
          .select("CURCODE").unique().to_series().to_list()
    )
    if unmatched:
        print(f"[WARN] {table_type}: no FORATE for currencies {unmatched} -- "
              f"MTDAVBAL/CURBAL left unconverted for these rows.")

    return df.select(FINAL_DEPOSIT_COLS)


def build_deposit_table(depo_path: Path, idepo_path: Path, table_type: str, forate_lookup: pl.DataFrame) -> pl.DataFrame:
    usecols = list(SOURCE_COLS.keys())
    depo = align_to_schema(read_sas7bdat(depo_path, usecols=usecols), SOURCE_COLS)
    idepo = align_to_schema(read_sas7bdat(idepo_path, usecols=usecols), SOURCE_COLS)
    combined = pl.concat([depo, idepo], how="vertical", rechunk=True)
    combined = apply_currency_conversion(combined, table_type, forate_lookup)
    return combined.unique(subset=["ACCTNO"], keep="first")


SAVING = build_deposit_table(DEPO_SAV, IDEPO_SAV, "saving", FORATE_LOOKUP)
CURRENT = build_deposit_table(DEPO_CUR, IDEPO_CUR, "current", FORATE_LOOKUP)
FD = build_deposit_table(DEPO_FD, IDEPO_FD, "fd", FORATE_LOOKUP)
_tick("build SAVING/CURRENT/FD")


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

# LEFT join: SAS "MERGE X(IN=A) CIS; IF A;" keeps ALL of X regardless of a
# CIS match.
SAVING = SAVING.join(CISDP, on="ACCTNO", how="left")
CURRENT = CURRENT.join(CISCA, on="ACCTNO", how="left")
FD = FD.join(CISDP, on="ACCTNO", how="left")
_tick("CIS join")


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
_tick("build DEPOSIT")


# ============================================================================
# EXTCRMA base (from raw CRMA file) + DEPOSIT match
# ============================================================================

EXTCRMA_BASE = read_crma_raw(CRMA_TXT).sort("ACCTNO")
_tick("read CRMA raw file")

EXTCRMA = EXTCRMA_BASE.join(DEPOSIT, on="ACCTNO", how="left")
EXTCRMA = EXTCRMA.with_columns([
    pl.when(pl.col("PRODUCT").is_not_null()).then(pl.lit("M")).otherwise(pl.lit("F")).alias("MATCHIND"),
])
EXTCRMA = EXTCRMA.with_columns([
    pl.when((pl.col("INACTIVE").is_null() | (pl.col("INACTIVE") == "")) & (pl.col("MATCHIND") == "M"))
      .then(pl.lit("A")).otherwise(pl.col("INACTIVE")).alias("INACTIVE"),
])
EXTCRMA = EXTCRMA.sort("AANO")
_tick("build EXTCRMA base (join+MATCHIND+INACTIVE)")


# ============================================================================
# LOAN match -- FAST parallel read of the large lnnote files
# ============================================================================

LN_NOTE_COLS = ["VINNO", "ESCRACCT"]

def load_loan(ln_note_path: Path, iln_note_path: Path, num_processes: int | None = None) -> pl.DataFrame:
    """Reads both lnnote files with pyreadstat's multiprocessing reader,
    splitting the row range across CPU cores. This replaces the earlier
    sequential chunked read -- safe to do because usecols already narrows
    each file down to 2 small string columns, so holding the full 6M-row
    result in memory isn't a concern; the remaining cost is pure read/parse
    time, which parallelizes well across cores.

    If you're on a shared/constrained box, pass num_processes explicitly
    (e.g. multiprocessing.cpu_count() // 2) to avoid starving other jobs.
    """
    n = num_processes or multiprocessing.cpu_count()
    conv_loan = read_sas_parallel(ln_note_path, usecols=LN_NOTE_COLS, num_processes=n)
    islamic_loan = read_sas_parallel(iln_note_path, usecols=LN_NOTE_COLS, num_processes=n)

    conv_loan = conv_loan.filter(pl.col("VINNO") != "")
    islamic_loan = islamic_loan.filter(pl.col("VINNO") != "")

    return (
        pl.concat([conv_loan, islamic_loan], how="vertical", rechunk=True)
        .rename({"VINNO": "AANO"})
        .unique(subset=["AANO"], keep="first")
    )

LOAN = load_loan(LN_NOTE, ILN_NOTE)
_tick("load LOAN (lnnote parallel read)")

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
_tick("derive date parts / scaling / PRODTYPE")


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


def _to_number(value):
    """Best-effort numeric coercion. Returns 0 for None, passes through
    int/float, and handles numeric strings including a stray trailing
    '.0' (e.g. from a column cast to Utf8 upstream that's really numeric).
    Returns None if the value genuinely can't be interpreted as a number."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return 0 if value != value else value  # NaN check
    s = str(value).strip()
    if s == "":
        return 0
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            return None


def _fmt_s(value, width):
    s = "" if value is None else str(value)
    return s[:width].ljust(width)

def _fmt_n(value, width):
    num = _to_number(value)
    if num is None:
        num = 0
    s = str(num)
    return s[-width:] if len(s) > width else s.rjust(width)

def _fmt_z(value, width):
    num = _to_number(value)
    if num is None:
        num = 0
    sign = "-" if num < 0 else ""
    digits = str(abs(num)).zfill(width - len(sign))
    s = sign + digits
    return s[-width:] if len(s) > width else s


def write_extmis_fixed_width(df: pl.DataFrame, path: Path):
    fmt_fn = {"s": _fmt_s, "n": _fmt_n, "z": _fmt_z}
    bad_rows = 0
    bad_fields: dict[str, int] = {}
    with open(path, "w") as f:
        for row in df.to_dicts():
            line = [" "] * _EXTMIS_LEN
            row_had_issue = False
            for name, start, width, kind in _EXTMIS_SPEC:
                raw = row.get(name)
                s = fmt_fn[kind](raw, width)
                if kind in ("n", "z") and _to_number(raw) is None:
                    bad_fields[name] = bad_fields.get(name, 0) + 1
                    row_had_issue = True
                idx = start - 1
                line[idx: idx + width] = list(s)
            if row_had_issue:
                bad_rows += 1
                if bad_rows <= 20:
                    print(f"[WARN] EXTMIS row ACCTNO={row.get('ACCTNO')} had "
                          f"non-numeric value(s) coerced to 0 -- check source data.")
            f.write("".join(line) + "\n")
    if bad_rows:
        print(f"[WARN] {bad_rows} total EXTMIS rows had at least one non-numeric "
              f"field coerced to 0. Per-field counts: {bad_fields}. "
              f"If BRANCH/PRODUCT show up here, their SOURCE_COLS dtype "
              f"guess (currently pl.Utf8) is likely wrong.")


# ============================================================================
# Write all outputs. Parquet/text are fast native-Polars writes and happen
# immediately; both .sas7bdat exports are batched into ONE shared SAS
# session (see write_sas7bdat_batch) to avoid paying subprocess startup
# cost twice, and _tick() timing shows exactly how long each part took.
# ============================================================================

base_name = f"EXTCRMA{REPTMON}{NOWK}"
base_name_mis = f"EXTMIS{REPTMON}{NOWK}"

EXTCRMA.write_csv(OUT_BEP / f"{base_name}.txt", separator="|")
write_parquet(EXTCRMA, OUT_BEP / f"{base_name}.parquet")
_tick("write EXTCRMA txt+parquet")

write_extmis_fixed_width(EXTMIS, OUT_BEP / f"{base_name_mis}.txt")
write_parquet(EXTMIS, OUT_BEP / f"{base_name_mis}.parquet")
_tick("write EXTMIS txt+parquet")

write_sas7bdat_batch([
    (EXTCRMA, OUT_BEP / f"{base_name}.sas7bdat"),
    (EXTMIS, OUT_BEP / f"{base_name_mis}.sas7bdat"),
])
_tick("write EXTCRMA+EXTMIS sas7bdat (shared SAS session)")
