from __future__ import annotations
 
import re
import logging
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta
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
INPUT_DIR     = Path("/dwh/ln_ln/")
BASE_OUT_PATH = Path("/host/mis/output/report")

SAS_EPOCH = date(1960, 1, 1)

# Expected output columns — must match SAS dataset variable order
OUTPUT_COLUMNS = [
    "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
    "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
    "OTHERFEE", "CTPRINT", "CTPRNNAC"
]

# =========================
# UTILITIES
# =========================
def to_sas_date(d: date) -> int:
    """Convert Python date to SAS date serial (days since 1960-01-01)
    e.g. 22/04/2026 -> 24218
    """
    return (d - SAS_EPOCH).days

def from_sas_date(n: int) -> date:
    """Convert SAS date serial back to Python date"""
    return SAS_EPOCH + timedelta(days=int(n))

def end_of_prev_month(d: date) -> date:
    """End of previous month — mirrors SAS INTNX('MONTH', d, -1, 'E')"""
    if d.month == 1:
        return date(d.year - 1, 12, 31)
    return date(d.year, d.month, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    """MMYY string — mirrors SAS MMYYN4. format"""
    return f"{d.month:02d}{d.year % 100:02d}"

# =========================
# AUTO-DETECT INPUT FILE
# =========================
def find_input_file(input_dir: Path, run_date: Optional[date] = None) -> Path:
    """Auto-detect the r6999mmyy.sas7bdat input file for the current run month.

    File naming pattern: r6999MMYY.sas7bdat
      - MM  = 2-digit month
      - YY  = 2-digit year
    e.g. r69990426.sas7bdat for April 2026
         r69990526.sas7bdat for May 2026

    Strategy:
      1. Use run_date if provided, else use today.
      2. The reporting month is end_of_prev_month(run_date), so MMYY is
         derived from that — matches REPTDT logic used everywhere.
      3. If exact file not found, scan directory for any r6999*.sas7bdat
         and pick the most recent one, with a warning.
    """
    if run_date is None:
        run_date = date.today()

    reptdate = end_of_prev_month(run_date)
    mmyy     = mmyy_format(reptdate)                          # e.g. '0426'
    expected = input_dir / f"r6999{mmyy}.sas7bdat"

    logger.info(f"  Run date        : {run_date}")
    logger.info(f"  REPTDATE        : {reptdate}  (MMYY: {mmyy})")
    logger.info(f"  Expected file   : {expected.name}")

    # --- exact match ---
    if expected.exists():
        logger.info(f"  ✓ Found exact match: {expected.name}")
        return expected

    # --- case-insensitive fallback (some systems differ in case) ---
    for f in input_dir.glob("r6999*.sas7bdat"):
        if f.name.lower() == expected.name.lower():
            logger.info(f"  ✓ Found case-insensitive match: {f.name}")
            return f

    # --- scan for any r6999*.sas7bdat and pick most recent by filename date ---
    logger.warning(f"  ⚠ Exact file '{expected.name}' not found — scanning directory...")
    candidates = sorted(input_dir.glob("r6999*.sas7bdat"))

    if not candidates:
        raise FileNotFoundError(
            f"No r6999*.sas7bdat files found in {input_dir}. "
            f"Expected: {expected.name}"
        )

    # Parse MMYY from each candidate filename and pick the latest
    def parse_mmyy(path: Path) -> date:
        m = re.search(r'r6999(\d{2})(\d{2})\.sas7bdat', path.name, re.IGNORECASE)
        if m:
            mm, yy = int(m.group(1)), int(m.group(2))
            # YY -> full year: assume 2000s
            yyyy = 2000 + yy
            try:
                return date(yyyy, mm, 1)
            except ValueError:
                pass
        return date(1960, 1, 1)   # fallback sort key

    candidates_sorted = sorted(candidates, key=parse_mmyy, reverse=True)
    chosen = candidates_sorted[0]
    logger.warning(f"  ⚠ Using most recent available file: {chosen.name}")
    logger.warning(f"    (Expected {expected.name} for REPTDATE {reptdate})")
    return chosen

# =========================
# READ SAS7BDAT
# =========================
def read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file using pandas.read_sas and convert to Polars.

    Handles:
    - Numeric columns stored as float (SAS default)
    - Byte-string column names / values decoded to str
    - Missing values filled with appropriate defaults
    """
    logger.info(f"  Reading: {path}")

    pdf = pd.read_sas(str(path), format='sas7bdat', encoding='utf-8')
    logger.info(f"  Raw shape       : {pdf.shape[0]:,} rows × {pdf.shape[1]} columns")

    # Decode byte-string column names
    pdf.columns = [
        c.decode('utf-8') if isinstance(c, bytes) else str(c)
        for c in pdf.columns
    ]

    # Decode byte-string cell values in object columns
    for col in pdf.select_dtypes(include='object').columns:
        pdf[col] = pdf[col].apply(
            lambda v: v.decode('utf-8').strip() if isinstance(v, bytes)
                      else (str(v).strip() if pd.notna(v) else '')
        )

    df = pl.from_pandas(pdf)
    logger.info(f"  Polars shape    : {df.shape[0]:,} rows × {df.shape[1]} columns")
    logger.info(f"  Columns found   : {df.columns}")
    return df


# =========================
# COLUMN ALIGNMENT
# =========================
def align_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure all OUTPUT_COLUMNS exist; add nulls for missing ones."""
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            logger.warning(f"  Column '{col}' not in SAS dataset — filling with null")
            df = df.with_columns(pl.lit(None).alias(col))

    extra = [c for c in df.columns if c not in OUTPUT_COLUMNS]
    if extra:
        logger.info(f"  Extra columns (kept): {extra}")

    return df.select(OUTPUT_COLUMNS + extra)


# =========================
# FORMAT NUMERIC COLUMNS
# =========================
def format_numeric_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Cast numeric columns to correct types, fill nulls with 0."""
    int_cols    = ["COSTCT", "NOTETYPE", "CTPRINT", "CTPRNNAC"]
    float_cols  = ["PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
                   "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR",
                   "NONDDA", "LATEFEES", "OTHERFEE"]

    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
                           .fill_null(0)
                           .cast(pl.Int64)
                           .alias(col)
            )

    for col in float_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
                           .fill_null(0.0)
                           .alias(col)
            )

    return df


# =========================
# SAVE OUTPUT
# =========================
def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path)
    logger.info(f"  Parquet written : {path}")

def write_csv(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)
    logger.info(f"  CSV written     : {path}")


# =========================
# MAIN
# =========================
def main():
    logger.info("=" * 60)
    logger.info("Starting LNR6999 Processing")
    logger.info("=" * 60)

    # -------------------------------------------------------
    # STEP 1: Derive run dates
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: Deriving run dates")
    logger.info("=" * 60)

    run_date      = date.today()
    reptdate      = end_of_prev_month(run_date)
    reptdt        = mmyy_format(reptdate)
    reptmon       = f"{reptdate.month:02d}"
    reptyear      = f"{reptdate.year % 100:02d}"
    run_date_sas  = to_sas_date(run_date)
    reptdate_sas  = to_sas_date(reptdate)

    logger.info(f"  Today           : {run_date}  (SAS serial: {run_date_sas})")
    logger.info(f"  REPTDATE        : {reptdate}  (SAS serial: {reptdate_sas})")
    logger.info(f"  REPTDT (MMYY)   : {reptdt}")
    logger.info(f"  REPTMON         : {reptmon}")
    logger.info(f"  REPTYEAR        : {reptyear}")

    # -------------------------------------------------------
    # STEP 2: Auto-detect input file for this month
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Auto-detecting input file")
    logger.info("=" * 60)

    input_path = find_input_file(INPUT_DIR, run_date)
    logger.info(f"  ✓ Input file    : {input_path.name}")

    # -------------------------------------------------------
    # STEP 3: Read SAS dataset
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Reading SAS7BDAT file")
    logger.info("=" * 60)

    df = read_sas7bdat(input_path)

    # -------------------------------------------------------
    # STEP 4: Align and format columns
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Aligning and formatting columns")
    logger.info("=" * 60)

    df = align_columns(df)
    df = format_numeric_columns(df)

    logger.info(f"  Final shape     : {df.shape[0]:,} rows × {df.shape[1]} columns")
    logger.info(f"  Unique COSTCT   : {df['COSTCT'].n_unique() if 'COSTCT' in df.columns else 'N/A'}")
    logger.info(f"  Unique NOTETYPE : {df['NOTETYPE'].n_unique() if 'NOTETYPE' in df.columns else 'N/A'}")

    # -------------------------------------------------------
    # STEP 5: Save outputs — parquet + csv
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Saving output files")
    logger.info("=" * 60)

    out_dir      = BASE_OUT_PATH / "LNR6999"
    out_name     = f"R6999{reptmon}{reptyear}"
    parquet_path = out_dir / f"{out_name}.parquet"
    csv_path     = out_dir / f"{out_name}.csv"

    write_parquet(df, parquet_path)
    write_csv(df, csv_path)

    # -------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Input file      : {input_path.name}")
    logger.info(f"  Run date        : {run_date}  (SAS: {run_date_sas})")
    logger.info(f"  REPTDATE        : {reptdate}  (SAS: {reptdate_sas})")
    logger.info(f"  REPTDT          : {reptdt}")
    logger.info(f"  Total records   : {len(df):,}")
    logger.info(f"  Output parquet  : {parquet_path}")
    logger.info(f"  Output csv      : {csv_path}")
    logger.info("=" * 60)
    logger.info("✓ Processing completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
