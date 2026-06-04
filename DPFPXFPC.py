from __future__ import annotations

import re
import logging
import pandas as pd
import pyreadstat
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
INPUT_DIR     = Path("/dwh/ln_ln/")
BASE_OUT_PATH = Path("/host/mis/output/report")

SAS_EPOCH = date(1960, 1, 1)

# Expected output columns — must match SAS dataset variable order
OUTPUT_COLUMNS = [
    "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
    "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
    "OTHERFEE", "CTPRINT", "CTPRNNAC"
]

# Variable labels for SAS output (metadata)
VAR_LABELS = {
    "COSTCT": "Cost Center",
    "NOTETYPE": "Note Type",
    "PRINBAL": "Principal Balance",
    "PRNNACCR": "Principal Not Accrued",
    "INTACCR": "Interest Accrued",
    "INTNACCR": "Interest Not Accrued",
    "UNERNINT": "Unearned Interest",
    "UNERNNON": "Unearned Non-Interest",
    "RSRVFIN": "Reserve Financial",
    "RSRVDLR": "Reserve Dollar",
    "NONDDA": "Non DDA",
    "LATEFEES": "Late Fees",
    "OTHERFEE": "Other Fees",
    "CTPRINT": "Count Principal",
    "CTPRNNAC": "Count Principal Not Accrued",
}

# =========================
# UTILITIES
# =========================
def to_sas_date(d: date) -> int:
    """Convert Python date to SAS date serial (days since 1960-01-01)"""
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
    """Auto-detect the r6999mmyy.sas7bdat input file for the current run month."""
    if run_date is None:
        run_date = date.today()

    reptdate = end_of_prev_month(run_date)
    mmyy     = mmyy_format(reptdate)
    expected = input_dir / f"r6999{mmyy}.sas7bdat"

    logger.info(f"  Run date        : {run_date}")
    logger.info(f"  REPTDATE        : {reptdate}  (MMYY: {mmyy})")
    logger.info(f"  Expected file   : {expected.name}")

    # --- exact match ---
    if expected.exists():
        logger.info(f"  ✓ Found exact match: {expected.name}")
        return expected

    # --- case-insensitive fallback ---
    for f in input_dir.glob("r6999*.sas7bdat"):
        if f.name.lower() == expected.name.lower():
            logger.info(f"  ✓ Found case-insensitive match: {f.name}")
            return f

    # --- scan for any r6999*.sas7bdat and pick most recent ---
    logger.warning(f"  ⚠ Exact file '{expected.name}' not found — scanning directory...")
    candidates = sorted(input_dir.glob("r6999*.sas7bdat"))

    if not candidates:
        raise FileNotFoundError(
            f"No r6999*.sas7bdat files found in {input_dir}. "
            f"Expected: {expected.name}"
        )

    def parse_mmyy(path: Path) -> date:
        m = re.search(r'r6999(\d{2})(\d{2})\.sas7bdat', path.name, re.IGNORECASE)
        if m:
            mm, yy = int(m.group(1)), int(m.group(2))
            yyyy = 2000 + yy
            try:
                return date(yyyy, mm, 1)
            except ValueError:
                pass
        return date(1960, 1, 1)

    candidates_sorted = sorted(candidates, key=parse_mmyy, reverse=True)
    chosen = candidates_sorted[0]
    logger.warning(f"  ⚠ Using most recent available file: {chosen.name}")
    logger.warning(f"    (Expected {expected.name} for REPTDATE {reptdate})")
    return chosen

# =========================
# READ SAS7BDAT
# =========================
def read_sas7bdat(path: Path) -> pd.DataFrame:
    """Read a .sas7bdat file using pyreadstat."""
    logger.info(f"  Reading: {path}")
    
    df, meta = pyreadstat.read_sas7bdat(str(path))
    
    logger.info(f"  Raw shape       : {df.shape[0]:,} rows × {df.shape[1]} columns")
    logger.info(f"  Columns found   : {list(df.columns)}")
    
    # Clean up column names (remove any trailing spaces)
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    
    return df, meta

# =========================
# COLUMN ALIGNMENT
# =========================
def align_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all OUTPUT_COLUMNS exist; add nulls for missing ones."""
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            logger.warning(f"  Column '{col}' not in SAS dataset — filling with null")
            df[col] = None

    extra = [c for c in df.columns if c not in OUTPUT_COLUMNS]
    if extra:
        logger.info(f"  Extra columns (kept): {extra}")

    return df[OUTPUT_COLUMNS + extra]

# =========================
# FORMAT NUMERIC COLUMNS
# =========================
def format_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cast numeric columns to correct types, fill nulls with 0."""
    int_cols    = ["COSTCT", "NOTETYPE", "CTPRINT", "CTPRNNAC"]
    float_cols  = ["PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
                   "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR",
                   "NONDDA", "LATEFEES", "OTHERFEE"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')

    return df

# =========================
# SAVE OUTPUT (XPORT FORMAT)
# =========================
def write_xport(df: pd.DataFrame, path: Path, table_name: str = 'R6999'):
    """Write DataFrame to SAS XPORT transport format.
    
    XPORT is the official SAS transport format that can be read by any SAS version.
    SAS code to read: 
        LIBNAME inlib XPORT "/path/to/file.xpt";
        DATA want; SET inlib.R6999; RUN;
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Ensure table name follows SAS naming rules (8 chars max, alphanumeric, underscore)
    sas_table_name = table_name[:8].upper()
    
    # Prepare variable labels (only for columns that exist)
    variable_labels = {}
    for col in df.columns:
        if col in VAR_LABELS:
            variable_labels[col] = VAR_LABELS[col]
    
    try:
        # Write to XPORT format (universal SAS transport format)
        pyreadstat.write_xport(
            df, 
            str(path),
            table_name=sas_table_name,
            column_labels=variable_labels,
            compress=False  # XPORT format doesn't support compression
        )
        logger.info(f"  SAS XPORT written : {path}")
        logger.info(f"  Table name         : {sas_table_name}")
        logger.info(f"  Variable labels    : {len(variable_labels)} columns have labels")
        
        # Also create a README file with SAS import instructions
        readme_path = path.parent / f"{path.stem}_README.txt"
        with open(readme_path, 'w') as f:
            f.write(f"SAS XPORT File: {path.name}\n")
            f.write(f"Created: {date.today()}\n\n")
            f.write("To read this file in SAS, use:\n")
            f.write("    LIBNAME inlib XPORT \"" + str(path.absolute()) + "\";\n")
            f.write("    DATA output; SET inlib." + sas_table_name + "; RUN;\n\n")
            f.write("Or with PROC IMPORT:\n")
            f.write(f"    PROC IMPORT DATAFILE=\"{path}\" OUT=work.R6999 DBMS=XPORT REPLACE;\n")
            f.write("    RUN;\n")
        logger.info(f"  README created    : {readme_path}")
        
    except Exception as e:
        logger.error(f"  Error writing XPORT: {e}")
        logger.warning(f"  Falling back to CSV output at {path.with_suffix('.csv')}")
        df.to_csv(path.with_suffix('.csv'), index=False)
        raise

# =========================
# MAIN
# =========================
def main():
    logger.info("=" * 60)
    logger.info("Starting LNR6999 Processing (SAS XPORT Output)")
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

    df, input_meta = read_sas7bdat(input_path)

    # -------------------------------------------------------
    # STEP 4: Align and format columns
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Aligning and formatting columns")
    logger.info("=" * 60)

    df = align_columns(df)
    df = format_numeric_columns(df)

    logger.info(f"  Final shape     : {df.shape[0]:,} rows × {df.shape[1]} columns")
    logger.info(f"  Unique COSTCT   : {df['COSTCT'].nunique() if 'COSTCT' in df.columns else 'N/A'}")
    logger.info(f"  Unique NOTETYPE : {df['NOTETYPE'].nunique() if 'NOTETYPE' in df.columns else 'N/A'}")

    # -------------------------------------------------------
    # STEP 5: Save output as SAS XPORT format
    # -------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Saving output as SAS XPORT format")
    logger.info("=" * 60)

    out_dir      = BASE_OUT_PATH / "LNR6999"
    out_name     = f"R6999{reptmon}{reptyear}"
    xport_path   = out_dir / f"{out_name}.xpt"
    csv_path     = out_dir / f"{out_name}.csv"  # Also save CSV for verification
    
    write_xport(df, xport_path, table_name=f"R6999{reptyear}{reptmon}")

    # Also save CSV for verification and backup
    df.to_csv(csv_path, index=False)
    logger.info(f"  CSV written (verification): {csv_path}")

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
    logger.info(f"  Output XPORT    : {xport_path}")
    logger.info(f"  Output CSV      : {csv_path}")
    logger.info("=" * 60)
    logger.info("✓ Processing completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
