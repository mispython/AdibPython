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
INPUT_SAS_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/r69990426.sas7bdat")
BASE_OUT_PATH  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

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

def extract_batch_date(filename: str) -> date:
    """Extract batch date from filename (e.g. r69990426.sas7bdat -> 2026-04-26 if YYYYMMDD,
    or tries MMDDYYYY / other patterns as fallback).
    Returns today if no date found.
    """
    stem = Path(filename).stem   # e.g. 'r69990426'

    # Try to find 8 consecutive digits anywhere in the filename
    match = re.search(r'(\d{8})', stem)
    if match:
        s = match.group(1)
        # Try YYYYMMDD
        try:
            return datetime.strptime(s, '%Y%m%d').date()
        except ValueError:
            pass
        # Try MMDDYYYY
        try:
            return datetime.strptime(s, '%m%d%Y').date()
        except ValueError:
            pass
        # Try DDMMYYYY
        try:
            return datetime.strptime(s, '%d%m%Y').date()
        except ValueError:
            pass

    # Try 6 digits: MMDDYY or YYMMDD
    match6 = re.search(r'(\d{6})', stem)
    if match6:
        s = match6.group(1)
        for fmt in ('%m%d%y', '%y%m%d', '%d%m%y'):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass

    logger.warning(f"Could not extract date from filename '{filename}' — using today")
    return date.today()

def end_of_prev_month(d: date) -> date:
    """End of previous month — mirrors SAS INTNX('MONTH', d, -1, 'E')"""
    if d.month == 1:
        return date(d.year - 1, 12, 31)
    return date(d.year, d.month, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    """MMYY string — mirrors SAS MMYYN4. format"""
    return f"{d.month:02d}{d.year % 100:02d}"

# =========================
# READ SAS7BDAT
# =========================
def read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file using pandas.read_sas and convert to Polars.

    Handles:
    - Numeric columns stored as float (SAS default)
    - SAS date serials (float days since 1960-01-01) left as integers
    - Byte-string column names/values decoded to str
    - Missing values filled with appropriate defaults
    """
    logger.info(f"Reading SAS dataset: {path}")

    # pandas.read_sas is the most reliable reader without pyreadstat
    pdf = pd.read_sas(str(path), format='sas7bdat', encoding='utf-8')

    logger.info(f"  Raw shape      : {pdf.shape[0]:,} rows × {pdf.shape[1]} columns")
    logger.info(f"  Raw columns    : {list(pdf.columns)}")

    # Decode byte-string column names (pandas sometimes returns bytes)
    pdf.columns = [
        c.decode('utf-8') if isinstance(c, bytes) else str(c)
        for c in pdf.columns
    ]

    # Decode byte-string cell values
    for col in pdf.select_dtypes(include='object').columns:
        pdf[col] = pdf[col].apply(
            lambda v: v.decode('utf-8').strip() if isinstance(v, bytes) else (str(v).strip() if pd.notna(v) else '')
        )

    # Convert to Polars
    df = pl.from_pandas(pdf)
    logger.info(f"  Polars shape   : {df.shape[0]:,} rows × {df.shape[1]} columns")

    return df


# =========================
# COLUMN ALIGNMENT
# =========================
def align_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure all OUTPUT_COLUMNS exist; add nulls for any missing ones.
    Reorders to OUTPUT_COLUMNS order.
    """
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            logger.warning(f"  Column '{col}' not found in SAS dataset — filling with null")
            df = df.with_columns(pl.lit(None).alias(col))

    extra = [c for c in df.columns if c not in OUTPUT_COLUMNS]
    if extra:
        logger.info(f"  Extra columns (kept): {extra}")

    # Reorder: OUTPUT_COLUMNS first, then any extras
    return df.select(OUTPUT_COLUMNS + extra)


# =========================
# FORMAT NUMERIC COLUMNS
# =========================
def format_numeric_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Cast numeric columns to appropriate types.

    - COSTCT, NOTETYPE, CTPRINT, CTPRNNAC  → Int64 (integer)
    - PRINBAL, PRNNACCR, UNERNINT, ...      → Float64 (2 decimal)
    - INTACCR, INTNACCR                     → Float64 (7 decimal)

    Nulls are filled with 0.
    """
    int_cols   = ["COSTCT", "NOTETYPE", "CTPRINT", "CTPRNNAC"]
    float2_cols = ["PRINBAL", "PRNNACCR", "UNERNINT", "UNERNNON",
                   "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES", "OTHERFEE"]
    float7_cols = ["INTACCR", "INTNACCR"]

    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
                           .fill_null(0)
                           .cast(pl.Int64)
                           .alias(col)
            )

    for col in float2_cols + float7_cols:
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
    logger.info(f"Input : {INPUT_SAS_PATH}")
    logger.info("=" * 60)

    # -------------------------------------------------------
    # STEP 1: Validate input file
    # -------------------------------------------------------
    if not INPUT_SAS_PATH.exists():
        logger.error(f"Input file not found: {INPUT_SAS_PATH}")
        raise FileNotFoundError(f"Input file not found: {INPUT_SAS_PATH}")

    # -------------------------------------------------------
    # STEP 2: Derive batch dates from filename
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Extracting batch date from filename")
    logger.info("=" * 60)

    batch_date    = extract_batch_date(INPUT_SAS_PATH.name)
    reptdate      = end_of_prev_month(batch_date)    # INTNX('MONTH',...,-1,'E')
    reptdt        = mmyy_format(reptdate)
    reptdate_sas  = to_sas_date(reptdate)
    batch_date_sas = to_sas_date(batch_date)

    reptmon  = f"{reptdate.month:02d}"
    reptyear = f"{reptdate.year % 100:02d}"

    logger.info(f"  Filename        : {INPUT_SAS_PATH.name}")
    logger.info(f"  Batch date      : {batch_date}  (SAS serial: {batch_date_sas})")
    logger.info(f"  REPTDATE        : {reptdate}  (SAS serial: {reptdate_sas})")
    logger.info(f"  REPTDT (MMYY)   : {reptdt}")
    logger.info(f"  REPTMON         : {reptmon}")
    logger.info(f"  REPTYEAR        : {reptyear}")

    # -------------------------------------------------------
    # STEP 3: Read SAS dataset
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Reading SAS7BDAT file")
    logger.info("=" * 60)

    df = read_sas7bdat(INPUT_SAS_PATH)

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
    # STEP 5: Save outputs (parquet + csv)
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
    logger.info(f"  Input file      : {INPUT_SAS_PATH.name}")
    logger.info(f"  Batch date      : {batch_date}  (SAS: {batch_date_sas})")
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
