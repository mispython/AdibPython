from __future__ import annotations

import re
import logging
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================
# CONFIGURATION
# =========================
INPUT_DIR       = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
BASE_OUTPUT     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")
USE_DUCKDB_COPY = False

SAS_EPOCH = date(1960, 1, 1)

# =========================
# UTILITIES
# =========================
def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if USE_DUCKDB_COPY:
        con = duckdb.connect()
        con.register("DF", df.to_arrow())
        con.execute(f"COPY DF TO '{path.as_posix()}' (FORMAT PARQUET)")
        con.close()
    else:
        pq.write_table(df.to_arrow(), path)

def write_csv(df: pl.DataFrame, path: Path):
    """Write DataFrame to CSV with header"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)

def to_sas_date(d: date) -> int:
    """Convert Python date to SAS date serial (days since 1960-01-01)
    e.g. 22/04/2026 -> 24218
    """
    return (d - SAS_EPOCH).days

def from_sas_date(n: int) -> date:
    """Convert SAS date serial back to Python date"""
    return SAS_EPOCH + timedelta(days=int(n))

def yyyymmdd_to_date(s: str) -> date:
    """Convert YYYYMMDD string to date"""
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_prev_month(d: date) -> date:
    """End of previous month — mirrors SAS INTNX('MONTH', d, -1, 'E')"""
    if d.month == 1:
        return date(d.year - 1, 12, 31)
    return date(d.year, d.month, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    """MMYY string — mirrors SAS MMYYN4. format"""
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[int]:
    """Create SAS date serial from month/day/year — mirrors SAS MDY().
    Returns SAS serial integer, or None if invalid.
    """
    if None in (month, day, year):
        return None
    try:
        return to_sas_date(date(year, month, day))
    except ValueError:
        return None

def read_first_line(path: Path) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.readline().strip()

def clean_numeric_id(val: str) -> str:
    """Convert a numeric ID that may appear as float scientific notation
    e.g. '8.123456789E+09' -> '8123456789'
         '8123456789.0'    -> '8123456789'
         '8123456789'      -> '8123456789'  (unchanged)
    Strips any trailing .0 or scientific notation, returns clean integer string.
    """
    if not val or val.strip() == '':
        return ''
    val = val.strip()
    try:
        # Parse as float first to handle scientific notation, then to int
        return str(int(float(val)))
    except (ValueError, OverflowError):
        return val


# =========================
# AUTO-DETECT INPUT FILES
# =========================
def find_rpvbdata(input_dir: Path, run_date: Optional[date] = None) -> Path:
    """Auto-detect RPVBDATA.txt — fixed filename, validated each run."""
    primary = input_dir / "RPVBDATA.txt"
    if primary.exists():
        logger.info(f"  ✓ RPVBDATA      : {primary.name}")
        return primary

    # Fallback: any RPVBDATA*.txt
    candidates = sorted(input_dir.glob("RPVBDATA*.txt"))
    if candidates:
        chosen = candidates[-1]
        logger.warning(f"  ⚠ RPVBDATA.txt not found — using: {chosen.name}")
        return chosen

    raise FileNotFoundError(f"No RPVBDATA*.txt found in {input_dir}")

def find_srsdata(input_dir: Path, run_date: Optional[date] = None) -> Path:
    """Auto-detect SRSDATA.txt — fixed filename, with dated variant fallback."""
    if run_date is None:
        run_date = date.today()

    reptdate = end_of_prev_month(run_date)
    mmyy     = mmyy_format(reptdate)

    # Tier 1: fixed name
    primary = input_dir / "SRSDATA.txt"
    if primary.exists():
        logger.info(f"  ✓ SRSDATA       : {primary.name}")
        return primary

    # Tier 2: dated variant e.g. SRSDATA_0426.txt
    dated = input_dir / f"SRSDATA_{mmyy}.txt"
    if dated.exists():
        logger.info(f"  ✓ SRSDATA (dated): {dated.name}")
        return dated

    # Tier 3: any SRSDATA*.txt, pick most recent
    candidates = sorted(input_dir.glob("SRSDATA*.txt"))
    if candidates:
        chosen = candidates[-1]
        logger.warning(f"  ⚠ SRSDATA.txt not found — using: {chosen.name}")
        return chosen

    raise FileNotFoundError(f"No SRSDATA*.txt found in {input_dir}")


# =========================
# FIELD SPECS
# SAS @position maps to Python [position-1 : position-1+length]
# =========================
FIELDS = [
    (0,   1,   'RECID',    str),
    (2,   12,  'MNIACTNO', str),   # kept as str — will be cleaned to integer string
    (13,  23,  'LOANNOTE', str),
    (24,  74,  'NAME',     'u'),
    (75,  76,  'ACCTSTA',  'u'),
    (77,  82,  'PRODTYPE', str),
    (83,  84,  'PRSTCOND', 'u'),
    (85,  86,  'REGCARD',  'u'),
    (87,  88,  'IGNTKEY',  'u'),
    (89,  99,  'REPODIST', str),
    (100, 101, 'ACCTWOFF', 'u'),
    (102, 106, 'YY1',      int),
    (106, 108, 'MM1',      int),
    (108, 110, 'DD1',      int),
    (111, 112, 'MODEREPO', 'u'),
    (113, 117, 'YY2',      int),
    (117, 119, 'MM2',      int),
    (119, 121, 'DD2',      int),
    (122, 132, 'REPOPAID', str),
    (133, 139, 'REPOSTAT', 'u'),
    (140, 150, 'TKEPRICE', str),
    (151, 161, 'MRKTVAL',  str),
    (162, 172, 'RSVPRICE', str),
    (173, 183, 'FTHSCHLD', str),
    (184, 188, 'YY3',      int),
    (188, 190, 'MM3',      int),
    (190, 192, 'DD3',      int),
    (193, 194, 'MODEDISP', 'u'),
    (195, 205, 'APPVDISP', str),
    (206, 210, 'YY4',      int),
    (210, 212, 'MM4',      int),
    (212, 214, 'DD4',      int),
    (215, 219, 'YY5',      int),
    (219, 221, 'MM5',      int),
    (221, 223, 'DD5',      int),
    (224, 228, 'YY6',      int),
    (228, 230, 'MM6',      int),
    (230, 232, 'DD6',      int),
    (233, 243, 'HOPRICE',  str),
    (244, 249, 'NOAUCT',   str),
    (250, 270, 'PRIOUT',   str),
]

# Date triplets: (year_col, month_col, day_col, output_col)
DATE_COLS = [
    ('YY1', 'MM1', 'DD1', 'DATEWOFF'),
    ('YY2', 'MM2', 'DD2', 'DATEREPO'),
    ('YY3', 'MM3', 'DD3', 'DATE5TH'),
    ('YY4', 'MM4', 'DD4', 'DATEAPRV'),
    ('YY5', 'MM5', 'DD5', 'DATESTLD'),
    ('YY6', 'MM6', 'DD6', 'DATEHO'),
]

DATE_COMPONENT_COLS = [
    'YY1','MM1','DD1',
    'YY2','MM2','DD2',
    'YY3','MM3','DD3',
    'YY4','MM4','DD4',
    'YY5','MM5','DD5',
    'YY6','MM6','DD6',
]

# Numeric ID columns that must be stored as clean integer strings
# (no scientific notation, no decimal points)
NUMERIC_ID_COLS = ['MNIACTNO', 'LOANNOTE', 'REPODIST', 'REPOPAID', 'APPVDISP']


# =========================
# DATA READING
# =========================
def read_rpvdata(path: Path) -> pl.DataFrame:
    """Read RPVBDATA.txt with fixed-width parsing.
    Skips first line (header). Mirrors SAS FIRSTOBS=2 + INPUT statement.

    MNIACTNO and other numeric ID fields are stored as clean integer
    strings — no scientific notation, no decimal points.
    All date columns stored as SAS serial integers (days since 1960-01-01).
    """
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]   # skip header — mirrors SAS FIRSTOBS=2

    data = []
    for line in lines:
        line = line.rstrip('\n')
        if not line.strip():
            continue

        rec = {}
        for start, end, field, dtype in FIELDS:
            raw = line[start:end].strip() if len(line) >= end else ''
            if dtype == 'u':
                rec[field] = raw.upper()
            elif dtype == int:
                rec[field] = int(raw) if raw.isdigit() else None
            else:
                rec[field] = raw
        data.append(rec)

    df = pl.DataFrame(data)

    # --- Fix MNIACTNO and other numeric ID columns ---
    # These are read as strings from the fixed-width file, but if they
    # contain digits only they can appear as scientific notation downstream
    # when written to CSV/parquet via some tools. Explicitly cast to Int64
    # then back to String to guarantee clean integer representation.
    for col in NUMERIC_ID_COLS:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                  .map_elements(
                      lambda v: clean_numeric_id(v) if v else '',
                      return_dtype=pl.String
                  )
                  .alias(col)
            )

    # Cast MNIACTNO to Int64 for proper numeric storage and deduplication
    # (avoids any float representation in parquet/csv)
    if 'MNIACTNO' in df.columns:
        df = df.with_columns(
            pl.col('MNIACTNO')
              .cast(pl.Int64, strict=False)
              .alias('MNIACTNO')
        )

    # Build date columns as SAS serial integers — mirrors SAS MDY(MM,DD,YY)
    # Default-arg capture avoids Python lambda closure bug
    for yy, mm, dd, dcol in DATE_COLS:
        df = df.with_columns(
            pl.struct([yy, mm, dd]).map_elements(
                lambda x, m=mm, d=dd, y=yy: mdy(x[m], x[d], x[y]),
                return_dtype=pl.Int32
            ).alias(dcol)
        )

    # Drop raw date component columns — mirrors SAS DROP= option
    return df.drop([c for c in df.columns if c in DATE_COMPONENT_COLS])


# =========================
# MAIN PROCESSING
# =========================
def main():
    logger.info("=" * 60)
    logger.info("Starting EIBMREPO Processing")
    logger.info("=" * 60)

    run_date = date.today()

    # -------------------------------------------------------
    # STEP 1: Auto-detect input files
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: Auto-detecting input files")
    logger.info("=" * 60)

    rpvbdata_path = find_rpvbdata(INPUT_DIR, run_date)
    srsdata_path  = find_srsdata(INPUT_DIR, run_date)

    logger.info(f"  RPVBDATA path   : {rpvbdata_path}")
    logger.info(f"  SRSDATA path    : {srsdata_path}")

    # -------------------------------------------------------
    # STEP 2: Read TBDATE from RPVBDATA — derive REPTDT / PREVDT
    # Mirrors SAS DATA REPTDATE step
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Processing RPVBDATA dates")
    logger.info("=" * 60)

    tbdate_rpvb  = "N/A"
    reptdate_sas = 0
    prevdate_sas = 0
    try:
        line        = read_first_line(rpvbdata_path)
        tbdate_rpvb = line[2:10]   # @03 TBDATE $8. → 0-indexed [2:10]

        if not (tbdate_rpvb.isdigit() and len(tbdate_rpvb) == 8):
            raise ValueError(f"Invalid TBDATE: '{tbdate_rpvb}'")

        tb_date      = yyyymmdd_to_date(tbdate_rpvb)
        reptdate     = end_of_prev_month(tb_date)       # INTNX('MONTH',...,-1,'E')
        prevdate     = end_of_prev_month(reptdate)       # INTNX('MONTH',...,-1,'E')
        reptdt       = mmyy_format(reptdate)             # PUT(..., MMYYN4.)
        prevdt       = mmyy_format(prevdate)
        reptdate_sas = to_sas_date(reptdate)
        prevdate_sas = to_sas_date(prevdate)

        logger.info(f"  TBDATE          : {tbdate_rpvb}")
        logger.info(f"  REPTDATE        : {reptdate}  (SAS serial: {reptdate_sas})")
        logger.info(f"  PREVDATE        : {prevdate}  (SAS serial: {prevdate_sas})")
        logger.info(f"  REPTDT (MMYY)   : {reptdt}")
        logger.info(f"  PREVDT (MMYY)   : {prevdt}")

    except Exception as e:
        logger.error(f"  ✗ Error: {e}")
        reptdate     = end_of_prev_month(run_date)
        prevdate     = end_of_prev_month(reptdate)
        reptdt       = mmyy_format(reptdate)
        prevdt       = mmyy_format(prevdate)
        reptdate_sas = to_sas_date(reptdate)
        prevdate_sas = to_sas_date(prevdate)
        logger.warning(f"  Fallback: REPTDT={reptdt} (SAS:{reptdate_sas}), PREVDT={prevdt} (SAS:{prevdate_sas})")

    # -------------------------------------------------------
    # STEP 3: Read TBDATE from SRSDATA — derive SRSTDT
    # Mirrors SAS DATA _NULL_ (direct INPUT, no month shift)
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Processing SRSDATA dates")
    logger.info("=" * 60)

    tbdate_srs = "N/A"
    srstdt     = reptdt   # default fallback
    try:
        line       = read_first_line(srsdata_path)
        tbdate_srs = line[0:8]   # @01 TBDATE $8.

        if tbdate_srs.isdigit() and len(tbdate_srs) == 8:
            srs_date     = yyyymmdd_to_date(tbdate_srs)
            srstdt       = mmyy_format(srs_date)         # direct PUT, no INTNX
            srs_date_sas = to_sas_date(srs_date)
            logger.info(f"  TBDATE          : {tbdate_srs}")
            logger.info(f"  SRS date        : {srs_date}  (SAS serial: {srs_date_sas})")
            logger.info(f"  SRSTDT (MMYY)   : {srstdt}")
        else:
            match = re.search(r'(\d{8})', line)
            if match:
                tbdate_srs   = match.group(1)
                srs_date     = yyyymmdd_to_date(tbdate_srs)
                srstdt       = mmyy_format(srs_date)
                srs_date_sas = to_sas_date(srs_date)
                logger.info(f"  Extracted       : {tbdate_srs}")
                logger.info(f"  SRS date        : {srs_date}  (SAS serial: {srs_date_sas})")
                logger.info(f"  SRSTDT (MMYY)   : {srstdt}")
            else:
                logger.warning(f"  ⚠ Could not parse SRSDATA date — using REPTDT fallback: {srstdt}")

    except Exception as e:
        logger.error(f"  ✗ Error: {e}")
        logger.warning(f"  Using REPTDT as fallback for SRSTDT: {srstdt}")

    # -------------------------------------------------------
    # STEP 4: Date guard — mirrors SAS %MACRO PROCESS / ABORT 77
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Date validation guard")
    logger.info("=" * 60)

    logger.info(f"  REPTDT={reptdt}  vs  SRSTDT={srstdt}")
    if reptdt != srstdt:
        error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})"
        logger.error(f"  ✗ {error_msg}")
        raise RuntimeError(error_msg)
    logger.info(f"  ✓ Dates match — proceeding")

    # -------------------------------------------------------
    # STEP 5: Read RPVBDATA — mirrors SAS DATA RPVB1
    # MNIACTNO stored as Int64 (no scientific notation)
    # Date columns stored as SAS serial integers
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Reading RPVBDATA (fixed-width parse)")
    logger.info("=" * 60)

    rpvb1 = read_rpvdata(rpvbdata_path)
    logger.info(f"  ✓ RPVB1         : {len(rpvb1):,} records, {len(rpvb1.columns)} columns")

    # Verify MNIACTNO looks correct — sample check
    if 'MNIACTNO' in rpvb1.columns and len(rpvb1) > 0:
        sample = rpvb1['MNIACTNO'].drop_nulls().head(3).to_list()
        logger.info(f"  MNIACTNO sample : {sample}  (dtype: {rpvb1['MNIACTNO'].dtype})")

    date_cols_present = [
        c for c in ['DATEWOFF','DATEREPO','DATE5TH','DATEAPRV','DATESTLD','DATEHO']
        if c in rpvb1.columns
    ]
    logger.info(f"  Date cols (SAS serial): {date_cols_present}")

    # -------------------------------------------------------
    # STEP 6: Filter — mirrors SAS DATA RPVB2 / RPVB3
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: Filtering (RPVB2 and RPVB3)")
    logger.info("=" * 60)

    if len(rpvb1) > 0 and 'ACCTSTA' in rpvb1.columns:
        # SAS: IF ACCTSTA IN ('D','S','R')
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        logger.info(f"  ✓ RPVB2         : {len(rpvb2):,} records  (ACCTSTA in D, S, R)")

        # SAS: IF DATESTLD NE '' — for SAS serial: not null
        if 'DATESTLD' in rpvb2.columns:
            rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null())
            logger.info(f"  ✓ RPVB3         : {len(rpvb3):,} records  (DATESTLD not null)")
        else:
            logger.warning("  ⚠ DATESTLD column missing — RPVB3 will be empty")
            rpvb3 = rpvb2.filter(pl.lit(False))
    else:
        logger.warning("  ⚠ No data or ACCTSTA column missing")
        rpvb2 = rpvb3 = rpvb1

    # -------------------------------------------------------
    # STEP 7: Build REPO.REPS&REPTDT = RPVB3 + REPO.REPS&PREVDT
    # Mirrors SAS DATA REPO.REPS&REPTDT; SET RPVB3 REPO.REPS&PREVDT
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 7: Creating REPO.REPS output")
    logger.info("=" * 60)

    repo_prev_path = BASE_OUTPUT / "REPO" / f"REPS_{prevdt}.parquet"
    repo_curr_path = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.parquet"
    repo_curr_csv  = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.csv"

    logger.info(f"  Previous parquet: {repo_prev_path}")
    logger.info(f"  Output parquet  : {repo_curr_path}")
    logger.info(f"  Output csv      : {repo_curr_csv}")

    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        logger.info(f"  ✓ Loaded previous REPO: {len(repo_prev):,} records")

        # Ensure MNIACTNO type consistency before concat
        if 'MNIACTNO' in repo_prev.columns:
            repo_prev = repo_prev.with_columns(
                pl.col('MNIACTNO').cast(pl.Int64, strict=False).alias('MNIACTNO')
            )

        # Align schemas before concat
        if len(rpvb3) > 0 and len(repo_prev) > 0:
            all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
            for col in all_cols:
                if col not in rpvb3.columns:
                    rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
                if col not in repo_prev.columns:
                    repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
            rpvb3     = rpvb3.select(all_cols)
            repo_prev = repo_prev.select(all_cols)

    except Exception as e:
        logger.info(f"  No previous REPO file ({e}) — using RPVB3 only")
        repo_prev = pl.DataFrame()

    repo_reps = (
        rpvb3 if len(repo_prev) == 0
        else pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
    )

    write_parquet(repo_reps, repo_curr_path)
    write_csv(repo_reps, repo_curr_csv)
    logger.info(f"  ✓ REPO saved    : {len(repo_reps):,} records  →  parquet + csv")

    # -------------------------------------------------------
    # STEP 8: Build REPOWH.REPS&REPTDT
    # Mirrors SAS PROC SORT NODUPKEY BY MNIACTNO
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 8: Creating REPOWH.REPS output (NODUPKEY)")
    logger.info("=" * 60)

    repowh_path = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.parquet"
    repowh_csv  = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.csv"

    logger.info(f"  Output parquet  : {repowh_path}")
    logger.info(f"  Output csv      : {repowh_csv}")

    if len(repo_reps) > 0 and 'MNIACTNO' in repo_reps.columns:
        repowh_reps        = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
        duplicates_removed = len(repo_reps) - len(repowh_reps)
        logger.info(f"  ✓ Duplicates removed : {duplicates_removed:,}")
    else:
        repowh_reps = repo_reps
        logger.info("  No MNIACTNO column or empty data — skipping deduplication")

    write_parquet(repowh_reps, repowh_path)
    write_csv(repowh_reps, repowh_csv)
    logger.info(f"  ✓ REPOWH saved  : {len(repowh_reps):,} records  →  parquet + csv")

    # -------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  RPVBDATA file   : {rpvbdata_path.name}")
    logger.info(f"  SRSDATA file    : {srsdata_path.name}")
    logger.info(f"  TBDATE RPVBDATA : {tbdate_rpvb}")
    logger.info(f"  TBDATE SRSDATA  : {tbdate_srs}")
    logger.info(f"  REPTDATE        : {reptdate}  (SAS: {reptdate_sas})")
    logger.info(f"  PREVDATE        : {prevdate}  (SAS: {prevdate_sas})")
    logger.info(f"  REPTDT (MMYY)   : {reptdt}")
    logger.info(f"  PREVDT (MMYY)   : {prevdt}")
    logger.info(f"  SRSTDT (MMYY)   : {srstdt}")
    logger.info(f"  RPVB1           : {len(rpvb1):,} records")
    logger.info(f"  RPVB2           : {len(rpvb2):,} records")
    logger.info(f"  RPVB3           : {len(rpvb3):,} records")
    logger.info(f"  REPO_REPS       : {len(repo_reps):,} records")
    logger.info(f"  REPOWH_REPS     : {len(repowh_reps):,} records")
    logger.info(f"\n  Output files:")
    logger.info(f"    {repo_curr_path}")
    logger.info(f"    {repo_curr_csv}")
    logger.info(f"    {repowh_path}")
    logger.info(f"    {repowh_csv}")
    logger.info("=" * 60)
    logger.info("✓ Processing completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
