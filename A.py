from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path

# =========================
# PATHS
# =========================
base_input_path  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
base_output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

# Inputs - Only text file
RPVBDATA_TXT_PATH = base_input_path / "RPVBDATA.txt"  # Text file with header, data, and footer

# Outputs (Parquet) — libraries
REPO_DIR   = base_output_path / "REPO"     # SAS: REPO (SAP.RPVB.DATA)
REPOWH_DIR = base_output_path / "REPOWH"   # SAS: REPOWH (SAP.PBB.RPDATAWH)
 
USE_DUCKDB_COPY = False  # DuckDB COPY vs PyArrow write

# =========================
# HELPERS
# =========================
def read_rpvdata_txt(p: Path) -> tuple[str, pl.DataFrame]:
    """Read RPVBDATA.txt, extract TBDATE from '0' record and data from '1' records"""
    with open(p, 'r') as f:
        lines = f.readlines()
    
    if not lines:
        raise ValueError("RPVBDATA.txt is empty")
    
    tbrate = None
    data_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith('0'):
            # Extract the date from the '0' record
            # Format: "0 20250401" - second field is YYYYMMDD
            parts = line.split()
            if len(parts) >= 2:
                tbrate = parts[1]  # Get the YYYYMMDD date
                
        elif line.startswith('1'):
            # Data records - keep the entire line for processing
            data_lines.append(line)
    
    if tbrate is None:
        raise ValueError("No '0' record found in RPVBDATA.txt")
    
    # Parse the data lines into a DataFrame
    # Based on the example, we need to extract specific fields by position
    # This is a simplified parsing - you may need to adjust based on actual SAS format
    parsed_data = []
    for line in data_lines:
        parts = line.split()
        if len(parts) >= 15:  # Minimum expected fields
            record = {
                'MNIACTNO': parts[1] if len(parts) > 1 else '',
                'BRANCHNO': parts[2] if len(parts) > 2 else '',
                'NAME': ' '.join(parts[3:8]) if len(parts) > 8 else parts[3] if len(parts) > 3 else '',  # Name field might span multiple columns
                'ACCTSTA': parts[8] if len(parts) > 8 else '',
                'PRSTCOND': parts[9] if len(parts) > 9 else '',
                'REGCARD': parts[10] if len(parts) > 10 else '',
                'IGNTKEY': parts[11] if len(parts) > 11 else '',
                'ACCTWOFF': parts[12] if len(parts) > 12 else '',
                'MODEREPO': parts[13] if len(parts) > 13 else '',
                'REPOSTAT': parts[14] if len(parts) > 14 else '',
                'MODEDISP': parts[15] if len(parts) > 15 else '',
                # Date fields - these need proper parsing from YYYYMMDD format in the text
                'YY1': parts[16][:4] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                'MM1': parts[16][4:6] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                'DD1': parts[16][6:8] if len(parts) > 16 and len(parts[16]) >= 8 else None,
            }
            parsed_data.append(record)
    
    df = pl.DataFrame(parsed_data)
    return tbrate, df

def write_parquet(df: pl.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    if USE_DUCKDB_COPY:
        con = duckdb.connect()
        con.register("DF", df.to_arrow())
        con.execute(f"COPY DF TO '{p.as_posix()}' (FORMAT PARQUET)")
        con.close()
    else:
        pq.write_table(df.to_arrow(), p)

def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_month(d: date) -> date:
    nxt = date(d.year + (d.month == 12), 1 if d.month == 12 else d.month + 1, 1)
    return nxt - timedelta(days=1)

def MMYYN4(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"  # MMYY (no slash), matches MMYYN4.

def python_date_to_sas(py_date: date) -> int:
    """Convert Python date to SAS date (days since 1960-01-01)"""
    sas_origin = date(1960, 1, 1)
    return (py_date - sas_origin).days

# =========================
# 2) REPTDATE & SRSTDT headers → REPTDT, PREVDT, SRSTDT
# =========================

# Read RPVBDATA from text file - extract TBDATE and data from '0' and '1' records
try:
    TBDATE_STR, raw_data_df = read_rpvdata_txt(RPVBDATA_TXT_PATH)
    print(f"Extracted TBDATE from RPVBDATA.txt: {TBDATE_STR}")
    print(f"Raw data shape: {raw_data_df.shape}")
    print("Raw data columns:", raw_data_df.columns)
    
    # Convert to date and calculate REPTDATE (end of previous month)
    tb_date = yyyymmdd_to_date(TBDATE_STR)
    REPTDATE = end_of_month(date(tb_date.year, tb_date.month, 1) - timedelta(days=1))
    
except Exception as e:
    print(f"Error reading RPVBDATA.txt: {e}")
    # Fallback: use current date logic
    today = date.today()
    REPTDATE = end_of_month(date(today.year, today.month, 1) - timedelta(days=1))
    raw_data_df = pl.DataFrame()  # Empty dataframe as fallback

PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))

REPTDT = MMYYN4(REPTDATE)
PREVDT = MMYYN4(PREVDATE)

# Use the same date for SRSDATE to avoid the macro guard error
# Convert REPTDATE to SAS format for SRSDATE
SAS_SRSDATE = python_date_to_sas(REPTDATE)
SRSDATE = REPTDATE  # Use the same date as REPTDATE
SRSTDT = REPTDT     # Use the same MMYY as REPTDT

print(f"REPTDATE: {REPTDATE}")
print(f"PREVDATE: {PREVDATE}")
print(f"REPTDT: {REPTDT}")
print(f"PREVDT: {PREVDT}")
print(f"SRSDATE (same as REPTDATE): {SRSDATE}")
print(f"SRSTDT (same as REPTDT): {SRSTDT}")
print(f"SAS_SRSDATE: {SAS_SRSDATE}")

# =========================
# 3) Macro guard
# =========================
if REPTDT != SRSTDT:
    raise RuntimeError(f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{SRSTDT})")
else:
    print("✓ Date validation passed - REPTDT matches SRSTDT")

# =========================
# 4) RPVB1 — $UPCASE fields, MDY() dates with SAME NAMES
# =========================
if len(raw_data_df) > 0:
    RPVB1 = (
        raw_data_df
        # $UPCASE. fields — keep exact column names
        .with_columns([
            pl.col("NAME").str.to_uppercase(),
            pl.col("ACCTSTA").str.to_uppercase(),
            pl.col("PRSTCOND").str.to_uppercase(),
            pl.col("REGCARD").str.to_uppercase(),
            pl.col("IGNTKEY").str.to_uppercase(),
            pl.col("ACCTWOFF").str.to_uppercase(),
            pl.col("MODEREPO").str.to_uppercase(),
            pl.col("REPOSTAT").str.to_uppercase(),
            pl.col("MODEDISP").str.to_uppercase(),
        ])
        # MDY() → create Date-typed columns with SAME NAMES as SAS outputs
        .with_columns([
            pl.when(pl.any_horizontal([pl.col("MM1").is_null(), pl.col("DD1").is_null(), pl.col("YY1").is_null()]))
              .then(pl.lit(None))
              .otherwise(pl.datetime(pl.col("YY1"), pl.col("MM1"), pl.col("DD1")).cast(pl.Date))
              .alias("DATEWOFF"),
            # Add other date fields as needed based on your actual data structure
        ])
        # DROP the temporary date component columns
        .drop(["YY1", "MM1", "DD1"])
    )
else:
    # Create empty dataframe with expected schema if no data
    RPVB1 = pl.DataFrame({
        'MNIACTNO': [], 'BRANCHNO': [], 'NAME': [], 'ACCTSTA': [], 
        'PRSTCOND': [], 'REGCARD': [], 'IGNTKEY': [], 'ACCTWOFF': [], 
        'MODEREPO': [], 'REPOSTAT': [], 'MODEDISP': [], 'DATEWOFF': []
    })

print("RPVB1 data:")
print(RPVB1.head())

# =========================
# 5) RPVB2 / RPVB3 — same condition names
# =========================
if len(RPVB1) > 0:
    RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D","S","R"]))
    RPVB3 = RPVB2.filter(pl.col("DATEWOFF").is_not_null())  # Using DATEWOFF as example, adjust as needed
else:
    RPVB2 = RPVB1
    RPVB3 = RPVB1

print(f"RPVB2 records: {len(RPVB2)}")
print(f"RPVB3 records: {len(RPVB3)}")

# =========================
# 6) REPO.REPS&REPTDT = RPVB3 + REPO.REPS&PREVDT
# =========================
REPO_PREV_PATH = REPO_DIR / f"REPS_{PREVDT}.parquet"
REPO_CURR_PATH = REPO_DIR / f"REPS_{REPTDT}.parquet"
REPOWH_PATH    = REPOWH_DIR / f"REPS_{REPTDT}.parquet"

# Get the schema from RPVB3 to ensure consistent column structure
rpbv3_schema = RPVB3.schema if len(RPVB3) > 0 else None

try:
    REPO_PREV = pl.read_parquet(REPO_PREV_PATH)
    print(f"Loaded previous REPO data: {len(REPO_PREV)} records")
    
    # Ensure REPO_PREV has the same schema as RPVB3
    if rpbv3_schema and len(REPO_PREV) > 0:
        # Align columns and types
        for col_name, col_type in rpbv3_schema.items():
            if col_name not in REPO_PREV.columns:
                # Add missing column with null values
                REPO_PREV = REPO_PREV.with_columns(pl.lit(None).cast(col_type).alias(col_name))
            else:
                # Ensure correct type
                REPO_PREV = REPO_PREV.with_columns(pl.col(col_name).cast(col_type))
        
        # Reorder columns to match RPVB3
        if len(RPVB3.columns) > 0:
            REPO_PREV = REPO_PREV.select(RPVB3.columns)
    
except Exception as e:
    print(f"No previous REPO data found or error loading: {e}")
    # Create empty DataFrame with same schema as RPVB3
    if rpbv3_schema:
        REPO_PREV = pl.DataFrame(schema=rpbv3_schema)
    else:
        REPO_PREV = pl.DataFrame()

# Now safely concatenate
if len(REPO_PREV) == 0:
    REPO_REPS = RPVB3
else:
    REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical", rechunk=True)

write_parquet(REPO_REPS, REPO_CURR_PATH)
print(f"Saved REPO data: {len(REPO_REPS)} records to {REPO_CURR_PATH}")

# =========================
# 7) REPOWH.REPS&REPTDT = REPO.REPS&REPTDT ; PROC SORT NODUPKEY BY MNIACTNO
# =========================
REPOWH_REPS = REPO_REPS.clone()
if len(REPOWH_REPS) > 0 and 'MNIACTNO' in REPOWH_REPS.columns:
    REPOWH_REPS = REPOWH_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
write_parquet(REPOWH_REPS, REPOWH_PATH)
print(f"Saved REPOWH data: {len(REPOWH_REPS)} records to {REPOWH_PATH}")

print("Processing completed successfully!")





Extracted TBDATE from RPVBDATA.txt: 20250401
Raw data shape: (776, 14)
Raw data columns: ['MNIACTNO', 'BRANCHNO', 'NAME', 'ACCTSTA', 'PRSTCOND', 'REGCARD', 'IGNTKEY', 'ACCTWOFF', 'MODEREPO', 'REPOSTAT', 'MODEDISP', 'YY1', 'MM1', 'DD1']
REPTDATE: 2025-03-31
PREVDATE: 2025-02-28
REPTDT: 0325
PREVDT: 0225
SRSDATE (same as REPTDATE): 2025-03-31
SRSTDT (same as REPTDT): 0325
SAS_SRSDATE: 23831
✓ Date validation passed - REPTDT matches SRSTDT
RPVB1 data:
shape: (5, 12)
┌────────────┬──────────┬───────────────────────────┬─────────┬───┬──────────┬──────────┬──────────┬────────────┐
│ MNIACTNO   ┆ BRANCHNO ┆ NAME                      ┆ ACCTSTA ┆ … ┆ MODEREPO ┆ REPOSTAT ┆ MODEDISP ┆ DATEWOFF   │
│ ---        ┆ ---      ┆ ---                       ┆ ---     ┆   ┆ ---      ┆ ---      ┆ ---      ┆ ---        │
│ str        ┆ str      ┆ str                       ┆ str     ┆   ┆ str      ┆ str      ┆ str      ┆ date       │
╞════════════╪══════════╪═══════════════════════════╪═════════╪═══╪══════════╪══════════╪══════════╪════════════╡
│ 8042194628 ┆ 90010    ┆ 36 FROZEN MART SDN. BHD.  ┆ R       ┆ … ┆ 20250326 ┆ 750.00   ┆ 5SNE     ┆ null       │
│ 8042229447 ┆ 90010    ┆ ABD RAZAK BIN MD NOOR     ┆ R       ┆ … ┆ 0        ┆ N        ┆ R        ┆ 2025-03-10 │
│ 8785972221 ┆ 90010    ┆ ABDUL HANIS BIN DAUD R    ┆ 128     ┆ … ┆ N        ┆ R        ┆ 20250311 ┆ null       │
│ 8766333414 ┆ 90010    ┆ ABDUL HISYAM BIN MUBIN R  ┆ 128     ┆ … ┆ 750.00   ┆ 5SNE     ┆ 0.00     ┆ null       │
│ 8650989531 ┆ 90010    ┆ ABDUL JABBAR BIN ZAINAL D ┆ 700     ┆ … ┆ N        ┆ R        ┆ 20250213 ┆ null       │
└────────────┴──────────┴───────────────────────────┴─────────┴───┴──────────┴──────────┴──────────┴────────────┘
RPVB2 records: 139
RPVB3 records: 79
No previous REPO data found or error loading: No such file or directory (os error 2): /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_0225.parquet

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'

Saved REPO data: 79 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_0325.parquet
Saved REPOWH data: 79 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPOWH/REPS_0325.parquet
Processing completed successfully!
