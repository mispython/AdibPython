#!/usr/bin/env python3
"""
ELDS BNM Summary Processing
Processes Bank Negara Malaysia summary files with cumulative data loading
"""

import polars as pl
import pyreadstat
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re

# =============================================================================
# CONFIGURATION PATHS
# =============================================================================
BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/input")
ELDS_DATA_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE VARIABLES
# =============================================================================
# REPTDATE = datetime.today() - timedelta(days=1)
REPTDATE = datetime.strptime("2025-10-15 00:00:00", '%Y-%m-%d %H:%M:%S')
PREVDATE = REPTDATE - timedelta(days=1)
FILE_DT = REPTDATE.strftime('%Y%m%d')

# SAS-style numeric RDATE (days since 1960-01-01)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

print("="*70)
print(f"ELDS BNM Summary Processing - {REPTDATE.strftime('%Y-%m-%d')}")
print("="*70)

# =============================================================================
# FILE PATHS
# =============================================================================
BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"

OUTPUT_DATA_PATH = Path(f"{ELDS_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

# EHP SAS7BDAT sources
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
EHP2_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_concat(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """Align all columns by casting to string before concatenation."""
    all_cols = sorted(set(df1.columns) | set(df2.columns))
    df1 = df1.select([
        pl.col(c).cast(pl.Utf8, strict=False).alias(c) if c in df1.columns 
        else pl.lit(None).alias(c) for c in all_cols
    ])
    df2 = df2.select([
        pl.col(c).cast(pl.Utf8, strict=False).alias(c) if c in df2.columns 
        else pl.lit(None).alias(c) for c in all_cols
    ])
    return pl.concat([df1, df2], how="vertical_relaxed")


def apply_sas_eof_check(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """
    SAS-style EOF control:
      - Footer row (FT00001724) contains expected record count
      - Compare expected vs actual data row count
      - Abort if mismatch (exit code 77)
    """
    if "MAANO" not in df.columns:
        print(f"  ⚠ MAANO column missing in {source_name}, skipping count validation.")
        return df.with_columns([
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        ])

    # Extract the last row (footer)
    control_row = df[-1, :]
    ctrl_val = str(control_row["MAANO"][0]).strip()

    # Detect control footer pattern (FT00001724)
    match = re.match(r"FT0*(\d+)", ctrl_val)
    if not match:
        print(f"  ℹ No control count found in {source_name}, proceeding without checks.")
        return df.with_columns([
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        ])

    control_count = int(match.group(1))
    print(f"  📊 {source_name}: Control count = {control_count}")

    # Exclude header (if any) and footer
    df_data = df.slice(0, df.height - 1)
    actual_count = df_data.height - 1

    print(f"  📊 {source_name}: Actual data rows = {actual_count}")

    # Compare control vs data row count
    if control_count != actual_count:
        print(f"  ✗ ABORT 77: Row count mismatch in {source_name}")
        print(f"     Expected: {control_count}, Got: {actual_count}")
        raise SystemExit(77)
    else:
        print(f"  ✓ Record count matches control ({actual_count})")

    # Add SAS-like columns
    df_data = df_data.with_columns([
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    ])
    return df_data

# =============================================================================
# PROCESS SUMM1 (Loan Applications)
# =============================================================================

print("\n" + "="*70)
print("PROCESSING SUMM1 (Loan Applications)")
print("="*70)

# Read CSV
print(f"\nReading {BNMSUMM1.name}...")
if not BNMSUMM1.exists():
    print(f"✗ ABORT: File not found: {BNMSUMM1}")
    sys.exit(1)

SUMM1 = pl.read_csv(
    BNMSUMM1,
    separator=",",
    has_header=False,
    ignore_errors=True,
    truncate_ragged_lines=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "AANO", "APPKEY", 
        "PRIORITY_SECTOR", "DSRISS3", "FIN_CONCEPT", "LN_UTILISE_LOCAT_CD", 
        "SPECIALFUND", "ASSET_PURCH_AMT", "PURPOSE_LOAN", "STRUPCO_3YR", 
        "FACICODE", "AMTAPPLY", "AMOUNT", "APPTYPE", "REJREASON", "DATEXT", 
        "ACCTNO", "EIR", "EREQNO", "REFIN_FLG", "STATUS", "CCPT_TAG", "CIR", 
        "PRICING_TYPE", "LU_ADD1", "LU_ADD2", "LU_ADD3", "LU_ADD4", 
        "LU_TOWN_CITY", "LU_POSTCODE", "LU_STATE_CD", "LU_COUNTRY_CD", 
        "PROP_STATUS", "DTCOMPLETE", "LU_SOURCE"
    ]
)
print(f"✓ CSV loaded: {SUMM1.height:,} rows")

SUMM1 = apply_sas_eof_check(SUMM1, "bnmsummary1_csv")

# Read EHP SAS7BDAT
print(f"\nReading {Path(EHP_SRC).name}...")
if not Path(EHP_SRC).exists():
    print(f"✗ ABORT: File not found: {EHP_SRC}")
    sys.exit(1)

SUMM1_EHP_df, _ = pyreadstat.read_sas7bdat(EHP_SRC)
print(f"✓ SAS7BDAT loaded: {len(SUMM1_EHP_df):,} rows")

SUMM1_EHP = apply_sas_eof_check(pl.from_pandas(SUMM1_EHP_df), "bnmsummary1_ehp")

# Combine CSV + EHP
print(f"\nCombining CSV + EHP sources...")
SUMM1 = safe_concat(SUMM1, SUMM1_EHP)
print(f"✓ Combined: {SUMM1.height:,} rows")

# Append previous day (REQUIRED - cumulative dataset)
print(f"\nAppending previous day data...")
prev_parquet = Path(f"{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM1.parquet")

if not prev_parquet.exists():
    print(f"✗ ABORT: Previous day file not found")
    print(f"  Required: {prev_parquet}")
    print(f"  Date: {PREVDATE.strftime('%Y-%m-%d')}")
    print(f"  Cumulative dataset requires previous day's data")
    sys.exit(1)

try:
    query = f"SELECT * FROM read_parquet('{prev_parquet}')"
    PREV_SUMM1 = pl.from_pandas(duckdb.query(query).to_df())
    print(f"✓ Previous day: {PREV_SUMM1.height:,} rows")
    
    ELDS_SUMM1 = safe_concat(PREV_SUMM1, SUMM1)
    print(f"✓ Total cumulative: {ELDS_SUMM1.height:,} rows")
except Exception as e:
    print(f"✗ ABORT: Error reading previous day file: {e}")
    sys.exit(1)

# Save to Parquet
print(f"\nSaving SUMM1.parquet...")
duckdb.sql(f"""
    COPY (SELECT * FROM ELDS_SUMM1) 
    TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)
""")
print(f"✓ Saved: {OUTPUT_DATA_PATH}/SUMM1.parquet ({ELDS_SUMM1.height:,} rows)")

# =============================================================================
# PROCESS SUMM2 (Borrower Details)
# =============================================================================

print("\n" + "="*70)
print("PROCESSING SUMM2 (Borrower Details)")
print("="*70)

# Read CSV
print(f"\nReading {BNMSUMM2.name}...")
if not BNMSUMM2.exists():
    print(f"✗ ABORT: File not found: {BNMSUMM2}")
    sys.exit(1)

SUMM2 = pl.read_csv(
    BNMSUMM2,
    separator=",",
    has_header=False,
    ignore_errors=True,
    truncate_ragged_lines=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "IDNO", "ENTKEY", 
        "APPLNAME", "COUNTRY", "DBIRTH", "ENTITY_TYPE", "CORP_STATUS_CD", 
        "INDUSTRIAL_STATUS", "RESIDENCY_STATUS_CD", "ANNSUBTSALARY", "GENDER", 
        "OCCUPATION", "EMPNAME", "EMPLOY_SECTOR_CD", "EMPLOY_TYPE_CD", 
        "POSTCODE", "STATE_CD", "COUNTRY_CD", "ROLE", "ICPP", "MARRIED", 
        "CUSTOMER_CODE", "CISNUMBER", "NO_OF_EMPLOYEE", "ANNUAL_TURNOVER", 
        "SMESIZE", "RACE", "INDUSTRIAL_SECTOR_CD", "OCCUPAT_MASCO_CD", 
        "EREQNO", "DSRISS3", "DTCOMPLETE"
    ]
)
print(f"✓ CSV loaded: {SUMM2.height:,} rows")

SUMM2 = apply_sas_eof_check(SUMM2, "bnmsummary2_csv")

# Read EHP2 SAS7BDAT
print(f"\nReading {Path(EHP2_SRC).name}...")
if not Path(EHP2_SRC).exists():
    print(f"✗ ABORT: File not found: {EHP2_SRC}")
    sys.exit(1)

SUMM2_EHP_df, _ = pyreadstat.read_sas7bdat(EHP2_SRC)
print(f"✓ SAS7BDAT loaded: {len(SUMM2_EHP_df):,} rows")

SUMM2_EHP = apply_sas_eof_check(pl.from_pandas(SUMM2_EHP_df), "bnmsummary2_ehp")

# Combine CSV + EHP
print(f"\nCombining CSV + EHP sources...")
SUMM2 = safe_concat(SUMM2, SUMM2_EHP)
print(f"✓ Combined: {SUMM2.height:,} rows")

# Append previous day (REQUIRED - cumulative dataset)
print(f"\nAppending previous day data...")
prev_parquet2 = Path(f"{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM2.parquet")

if not prev_parquet2.exists():
    print(f"✗ ABORT: Previous day file not found")
    print(f"  Required: {prev_parquet2}")
    print(f"  Date: {PREVDATE.strftime('%Y-%m-%d')}")
    print(f"  Cumulative dataset requires previous day's data")
    sys.exit(1)

try:
    query = f"SELECT * FROM read_parquet('{prev_parquet2}')"
    PREV_SUMM2 = pl.from_pandas(duckdb.query(query).to_df())
    print(f"✓ Previous day: {PREV_SUMM2.height:,} rows")
    
    ELDS_SUMM2 = safe_concat(PREV_SUMM2, SUMM2)
    print(f"✓ Total cumulative: {ELDS_SUMM2.height:,} rows")
except Exception as e:
    print(f"✗ ABORT: Error reading previous day file: {e}")
    sys.exit(1)

# Save to Parquet
print(f"\nSaving SUMM2.parquet...")
duckdb.sql(f"""
    COPY (SELECT * FROM ELDS_SUMM2) 
    TO '{OUTPUT_DATA_PATH}/SUMM2.parquet' (FORMAT PARQUET)
""")
print(f"✓ Saved: {OUTPUT_DATA_PATH}/SUMM2.parquet ({ELDS_SUMM2.height:,} rows)")

# =============================================================================
# COMPLETION SUMMARY
# =============================================================================

print("\n" + "="*70)
print("PROCESSING COMPLETE")
print("="*70)
print(f"Date: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Output Directory: {OUTPUT_DATA_PATH}")
print(f"\nResults:")
print(f"  SUMM1.parquet: {ELDS_SUMM1.height:,} rows")
print(f"  SUMM2.parquet: {ELDS_SUMM2.height:,} rows")
print("="*70)
print("✓ All files processed successfully")






======================================================================
ELDS BNM Summary Processing - 2025-10-15
======================================================================

======================================================================
PROCESSING SUMM1 (Loan Applications)
======================================================================

Reading bnmsummary1_20251015.csv...
✓ CSV loaded: 1,726 rows
  📊 bnmsummary1_csv: Control count = 1724
  📊 bnmsummary1_csv: Actual data rows = 1724
  ✓ Record count matches control (1724)

Reading intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat...
✓ SAS7BDAT loaded: 4,550 rows
  ℹ No control count found in bnmsummary1_ehp, proceeding without checks.

Combining CSV + EHP sources...
✓ Combined: 6,275 rows

Appending previous day data...
✗ ABORT: Previous day file not found
  Required: /sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output/year=2025/month=10/day=14/SUMM1.parquet
  Date: 2025-10-14
  Cumulative dataset requires previous day's data
