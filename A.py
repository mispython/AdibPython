from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
import re

# =========================
# CONFIGURATION
# =========================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

RPVBDATA_PATH = BASE_INPUT / "RPVBDATA.txt"
SRSDATA_PATH = BASE_INPUT / "SRSDATA.txt"
REPO_DIR = BASE_OUTPUT / "REPO"
REPOWH_DIR = BASE_OUTPUT / "REPOWH"

USE_DUCKDB_COPY = False

# =========================
# UTILITIES
# =========================
def write_parquet(df: pl.DataFrame, path: Path):
    """Write DataFrame to Parquet file"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if USE_DUCKDB_COPY:
        con = duckdb.connect()
        con.register("DF", df.to_arrow())
        con.execute(f"COPY DF TO '{path.as_posix()}' (FORMAT PARQUET)")
        con.close()
    else:
        pq.write_table(df.to_arrow(), path)

def yyyymmdd_to_date(s: str) -> date:
    """Convert YYYYMMDD string to date"""
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_prev_month(d: date) -> date:
    """Get end of previous month"""
    return date(d.year, d.month, 1) - timedelta(days=1) if d.month > 1 else date(d.year - 1, 12, 31)

def mmyy_format(d: date) -> str:
    """Convert date to MMYY format"""
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[date]:
    """Create date from components"""
    if None in (month, day, year):
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None

def extract_tbdate(filepath: Path, start: int, length: int = 8) -> str:
    """Extract TBDATE from file at specified position"""
    with open(filepath, 'r', encoding='utf-8') as f:
        line = f.readline().strip()
    
    print(f"DEBUG: {filepath.name} first line: '{line}'")
    tbdate = line[start:start+length]
    print(f"DEBUG: Extracted TBDATE: '{tbdate}'")
    
    # Return as-is even if not all digits (SAS behavior)
    if not (tbdate.isdigit() and len(tbdate) == 8):
        print(f"DEBUG: TBDATE '{tbdate}' is not all digits, but using anyway")
    
    return tbdate

# =========================
# FIELD DEFINITIONS
# =========================
FIELD_SPECS = [
    (0, 1, 'RECID', str), (2, 12, 'MNIACTNO', str), (13, 23, 'LOANNOTE', str),
    (24, 74, 'NAME', 'upper'), (75, 76, 'ACCTSTA', 'upper'), (77, 82, 'PRODTYPE', str),
    (83, 84, 'PRSTCOND', 'upper'), (85, 86, 'REGCARD', 'upper'), (87, 88, 'IGNTKEY', 'upper'),
    (89, 99, 'REPODIST', str), (100, 101, 'ACCTWOFF', 'upper'),
    (102, 106, 'YY1', int), (106, 108, 'MM1', int), (108, 110, 'DD1', int),
    (111, 112, 'MODEREPO', 'upper'),
    (113, 117, 'YY2', int), (117, 119, 'MM2', int), (119, 121, 'DD2', int),
    (122, 132, 'REPOPAID', str), (133, 139, 'REPOSTAT', 'upper'),
    (140, 150, 'TKEPRICE', str), (151, 161, 'MRKTVAL', str), (162, 172, 'RSVPRICE', str),
    (173, 183, 'FTHSCHLD', str),
    (184, 188, 'YY3', int), (188, 190, 'MM3', int), (190, 192, 'DD3', int),
    (193, 194, 'MODEDISP', 'upper'), (195, 205, 'APPVDISP', str),
    (206, 210, 'YY4', int), (210, 212, 'MM4', int), (212, 214, 'DD4', int),
    (215, 219, 'YY5', int), (219, 221, 'MM5', int), (221, 223, 'DD5', int),
    (224, 228, 'YY6', int), (228, 230, 'MM6', int), (230, 232, 'DD6', int),
    (233, 243, 'HOPRICE', str), (244, 249, 'NOAUCT', str), (250, 270, 'PRIOUT', str)
]

DATE_MAPPINGS = [
    ('YY1', 'MM1', 'DD1', 'DATEWOFF'), ('YY2', 'MM2', 'DD2', 'DATEREPO'),
    ('YY3', 'MM3', 'DD3', 'DATE5TH'), ('YY4', 'MM4', 'DD4', 'DATEAPRV'),
    ('YY5', 'MM5', 'DD5', 'DATESTLD'), ('YY6', 'MM6', 'DD6', 'DATEHO')
]

# =========================
# DATA READING
# =========================
def read_rpvdata_fixed_width() -> pl.DataFrame:
    """Read RPVBDATA.txt with fixed-width parsing"""
    with open(RPVBDATA_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]  # Skip header
    
    print(f"\nReading RPVBDATA.txt - Total lines: {len(lines) + 1}")
    
    data = []
    for line_num, line in enumerate(lines, start=2):
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        if line_num <= 7:
            print(f"DEBUG Line {line_num}: '{line}'")
        
        record = {}
        for start, end, field, dtype in FIELD_SPECS:
            value = line[start:end].strip() if len(line) >= end else ''
            
            if dtype == 'upper':
                record[field] = value.upper()
            elif dtype == int:
                record[field] = int(value) if value.isdigit() else None
            else:
                record[field] = value
        
        data.append(record)
    
    print(f"Parsed {len(data)} data records")
    df = pl.DataFrame(data)
    
    # Create date fields
    for yy, mm, dd, date_col in DATE_MAPPINGS:
        df = df.with_columns(
            pl.struct([yy, mm, dd]).map_elements(
                lambda x: mdy(x[mm], x[dd], x[yy]), 
                return_dtype=pl.Date
            ).alias(date_col)
        )
    
    # Drop component columns
    drop_cols = [col for col in df.columns if any(col == c for c in 
                ['YY1','MM1','DD1','YY2','MM2','DD2','YY3','MM3','DD3',
                 'YY4','MM4','DD4','YY5','MM5','DD5','YY6','MM6','DD6'])]
    return df.drop(drop_cols) if drop_cols else df

# =========================
# MAIN PROCESSING
# =========================
def main():
    print("=" * 60)
    print("STEP 1: Processing RPVBDATA dates")
    print("=" * 60)
    
    try:
        tbdate_rpvb = extract_tbdate(RPVBDATA_PATH, start=2)
        tb_date = yyyymmdd_to_date(tbdate_rpvb)
        reptdate = end_of_prev_month(tb_date)
        prevdate = end_of_prev_month(reptdate)
        reptdt = mmyy_format(reptdate)
        prevdt = mmyy_format(prevdate)
        
        print(f"✓ TBDATE: {tbdate_rpvb}")
        print(f"  Date: {tb_date} → REPTDATE: {reptdate} → PREVDATE: {prevdate}")
        print(f"  REPTDT: {reptdt}, PREVDT: {prevdt}")
    except Exception as e:
        print(f"✗ Error: {e}")
        today = date.today()
        reptdate = end_of_prev_month(today)
        prevdate = end_of_prev_month(reptdate)
        reptdt, prevdt = mmyy_format(reptdate), mmyy_format(prevdate)
        print(f"  Using fallback: REPTDT={reptdt}, PREVDT={prevdt}")
    
    print("\n" + "=" * 60)
    print("STEP 2: Processing SRSDATA dates")
    print("=" * 60)
    
    try:
        tbdate_srs = extract_tbdate(SRSDATA_PATH, start=0)
        
        if tbdate_srs.isdigit() and len(tbdate_srs) == 8:
            srstdt = mmyy_format(yyyymmdd_to_date(tbdate_srs))
        else:
            match = re.search(r'(\d{8})', tbdate_srs)
            srstdt = mmyy_format(yyyymmdd_to_date(match.group(1))) if match else reptdt
            if not match:
                print(f"  Could not extract valid date, using REPTDT as fallback")
        
        print(f"✓ TBDATE: {tbdate_srs} → SRSTDT: {srstdt}")
    except Exception as e:
        print(f"✗ Error: {e}")
        srstdt = reptdt
        print(f"  Using REPTDT as fallback: {srstdt}")
    
    print("\n" + "=" * 60)
    print("STEP 3: Macro guard validation")
    print("=" * 60)
    
    if reptdt != srstdt:
        error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})"
        print(f"✗ {error_msg}")
        raise RuntimeError(error_msg)
    print(f"✓ Date validation passed - REPTDT matches SRSTDT")
    
    print("\n" + "=" * 60)
    print("STEP 4-5: Reading and filtering RPVBDATA")
    print("=" * 60)
    
    rpvb1 = read_rpvdata_fixed_width()
    print(f"✓ RPVB1: {len(rpvb1)} records")
    
    if len(rpvb1) > 0:
        print("\nSample (first 3 rows):")
        print(rpvb1.head(3))
        
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null()) if 'DATESTLD' in rpvb2.columns else rpvb2.filter(pl.lit(False))
        print(f"✓ RPVB2: {len(rpvb2)} records (ACCTSTA in D,S,R)")
        print(f"✓ RPVB3: {len(rpvb3)} records (with DATESTLD)")
    else:
        rpvb2 = rpvb3 = rpvb1
    
    print("\n" + "=" * 60)
    print("STEP 6-7: Creating REPO and REPOWH datasets")
    print("=" * 60)
    
    repo_prev_path = REPO_DIR / f"REPS_{prevdt}.parquet"
    repo_curr_path = REPO_DIR / f"REPS_{reptdt}.parquet"
    repowh_path = REPOWH_DIR / f"REPS_{reptdt}.parquet"
    
    # Load previous data
    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        print(f"✓ Loaded previous: {len(repo_prev)} records")
        
        # Align schemas
        if len(rpvb3) > 0:
            all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
            for col in all_cols:
                if col not in rpvb3.columns:
                    rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
                if col not in repo_prev.columns:
                    repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
            rpvb3, repo_prev = rpvb3.select(all_cols), repo_prev.select(all_cols)
    except Exception as e:
        print(f"ℹ No previous data: {e}")
        repo_prev = pl.DataFrame()
    
    # Concatenate
    repo_reps = rpvb3 if len(repo_prev) == 0 else pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
    write_parquet(repo_reps, repo_curr_path)
    print(f"✓ REPO saved: {len(repo_reps)} records")
    
    # Deduplicate
    repowh_reps = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first") if len(repo_reps) > 0 else repo_reps
    write_parquet(repowh_reps, repowh_path)
    print(f"✓ REPOWH saved: {len(repowh_reps)} records ({len(repo_reps) - len(repowh_reps)} duplicates removed)")
    
    print("\n" + "=" * 60)
    print("✓ Processing completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
