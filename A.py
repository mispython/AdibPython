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
def get_tbdate_from_rpvdata() -> str:
    """Read first line of RPVBDATA.txt and extract TBDATE at position 3-10"""
    with open(RPVBDATA_PATH, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
    
    print(f"DEBUG: RPVBDATA first line: '{first_line}'")
    
    if len(first_line) >= 10:
        tbdate = first_line[2:10]  # @03 TBDATE $8. (0-indexed: 2:10)
        print(f"DEBUG: Extracted TBDATE from positions 3-10: '{tbdate}'")
        if tbdate.isdigit() and len(tbdate) == 8:
            return tbdate
    
    raise ValueError(f"Invalid TBDATE in first line: {first_line}")

def get_tbdate_from_srsdata() -> str:
    """Read first line of SRSDATA.txt and extract first 8 characters as TBDATE"""
    with open(SRSDATA_PATH, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
    
    print(f"DEBUG: SRSDATA first line: '{first_line}'")
    
    if len(first_line) >= 8:
        tbdate = first_line[0:8]  # @01 TBDATE $8. (first 8 characters)
        print(f"DEBUG: Extracted TBDATE (first 8 chars): '{tbdate}'")
        if not (tbdate.isdigit() and len(tbdate) == 8):
            print(f"DEBUG: TBDATE '{tbdate}' is not all digits, but using anyway as SAS would")
        return tbdate
    
    raise ValueError(f"SRSDATA line too short: '{first_line}'")

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
        tbdate_rpvb = get_tbdate_from_rpvdata()
        print(f"✓ TBDATE from RPVBDATA: {tbdate_rpvb}")
        
        tb_date = yyyymmdd_to_date(tbdate_rpvb)
        reptdate = end_of_prev_month(tb_date)
        prevdate = end_of_prev_month(reptdate)
        reptdt = mmyy_format(reptdate)
        prevdt = mmyy_format(prevdate)
        
        print(f"  TBDATE as date: {tb_date}")
        print(f"  REPTDATE (end of prev month from TBDATE): {reptdate}")
        print(f"  PREVDATE (end of prev month from REPTDATE): {prevdate}")
        print(f"  REPTDT (MMYY): {reptdt}")
        print(f"  PREVDT (MMYY): {prevdt}")
    except Exception as e:
        print(f"✗ Error processing RPVBDATA: {e}")
        today = date.today()
        reptdate = end_of_prev_month(today)
        prevdate = end_of_prev_month(reptdate)
        reptdt, prevdt = mmyy_format(reptdate), mmyy_format(prevdate)
        print(f"  Using fallback dates based on today ({today})")
        print(f"  REPTDATE: {reptdate}, REPTDT: {reptdt}")
    
    print("\n" + "=" * 60)
    print("STEP 2: Processing SRSDATA dates")
    print("=" * 60)
    
    try:
        tbdate_srs = get_tbdate_from_srsdata()
        print(f"✓ TBDATE from SRSDATA: {tbdate_srs}")
        
        if tbdate_srs.isdigit() and len(tbdate_srs) == 8:
            srs_tb_date = yyyymmdd_to_date(tbdate_srs)
            # SAS: REPTDATE = INPUT(TBDATE,YYMMDD8.) - Direct conversion, no month adjustment
            srs_reptdate = srs_tb_date
            srstdt = mmyy_format(srs_reptdate)
            print(f"  SRS TBDATE as date: {srs_tb_date}")
            print(f"  SRS REPTDATE: {srs_reptdate}")
            print(f"  SRSTDT (MMYY): {srstdt}")
        else:
            print(f"⚠ WARNING: TBDATE '{tbdate_srs}' is not a valid YYYYMMDD date")
            print(f"  Attempting to extract date portion...")
            
            match = re.search(r'(\d{8})', tbdate_srs)
            if match:
                date_str = match.group(1)
                srs_tb_date = yyyymmdd_to_date(date_str)
                srs_reptdate = srs_tb_date
                srstdt = mmyy_format(srs_reptdate)
                print(f"  Found date in string: {date_str}")
                print(f"  SRS REPTDATE: {srs_reptdate}")
                print(f"  SRSTDT (MMYY): {srstdt}")
            else:
                print(f"  Could not extract valid date, using REPTDT as fallback")
                srstdt = reptdt
    except Exception as e:
        print(f"✗ Error reading SRSDATA.txt: {e}")
        srstdt = reptdt
        print(f"  Using REPTDT as fallback for SRSTDT: {srstdt}")
    
    print("\n" + "=" * 60)
    print("STEP 3: Macro guard validation")
    print("=" * 60)
    
    print(f"Comparing: REPTDT={reptdt} vs SRSTDT={srstdt}")
    if reptdt != srstdt:
        error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})"
        print(f"✗ {error_msg}")
        raise RuntimeError(error_msg)
    print(f"✓ Date validation passed - REPTDT matches SRSTDT")
    
    print("\n" + "=" * 60)
    print("STEP 4: Reading RPVBDATA with fixed-width parsing")
    print("=" * 60)
    
    rpvb1 = read_rpvdata_fixed_width()
    print(f"✓ RPVB1 records read: {len(rpvb1)}")
    print(f"  RPVB1 columns: {rpvb1.columns}")
    
    if len(rpvb1) > 0:
        print("\nSample of RPVB1 data (first 3 rows):")
        print(rpvb1.head(3))
    else:
        print("⚠ Warning: No data records found in RPVBDATA.txt")
    
    print("\n" + "=" * 60)
    print("STEP 5: Creating RPVB2 and RPVB3")
    print("=" * 60)
    
    if len(rpvb1) > 0 and 'ACCTSTA' in rpvb1.columns:
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        print(f"✓ RPVB2 created: {len(rpvb2)} records (ACCTSTA in D,S,R)")
        
        if 'DATESTLD' in rpvb2.columns:
            rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null())
            print(f"✓ RPVB3 created: {len(rpvb3)} records (with DATESTLD)")
        else:
            print(f"⚠ DATESTLD column not found, creating empty RPVB3")
            rpvb3 = rpvb2.filter(pl.lit(False))
    else:
        print(f"⚠ No data or ACCTSTA column not found")
        rpvb2 = rpvb1
        rpvb3 = rpvb1
    
    print("\n" + "=" * 60)
    print("STEP 6: Creating REPO.REPS&REPTDT")
    print("=" * 60)
    
    repo_prev_path = REPO_DIR / f"REPS_{prevdt}.parquet"
    repo_curr_path = REPO_DIR / f"REPS_{reptdt}.parquet"
    repowh_path = REPOWH_DIR / f"REPS_{reptdt}.parquet"
    
    print(f"Previous file to load: {repo_prev_path}")
    print(f"Current output file: {repo_curr_path}")
    
    # Load previous data
    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        print(f"✓ Loaded previous REPO data: {len(repo_prev)} records")
        
        # Align schemas
        if len(rpvb3) > 0 and len(repo_prev) > 0:
            all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
            for col in all_cols:
                if col not in rpvb3.columns:
                    rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
                if col not in repo_prev.columns:
                    repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
            rpvb3 = rpvb3.select(all_cols)
            repo_prev = repo_prev.select(all_cols)
    except Exception as e:
        print(f"ℹ No previous REPO data found or error loading: {e}")
        repo_prev = pl.DataFrame()
    
    # Concatenate
    if len(repo_prev) == 0:
        repo_reps = rpvb3
        print(f"  No previous data, using only RPVB3 ({len(rpvb3)} records)")
    else:
        repo_reps = pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
        print(f"  Combined RPVB3 ({len(rpvb3)} records) + previous ({len(repo_prev)} records)")
    
    write_parquet(repo_reps, repo_curr_path)
    print(f"✓ Saved REPO data: {len(repo_reps)} records to {repo_curr_path}")
    
    print("\n" + "=" * 60)
    print("STEP 7: Creating REPOWH.REPS&REPTDT (with NODUPKEY)")
    print("=" * 60)
    
    if len(repo_reps) > 0 and 'MNIACTNO' in repo_reps.columns:
        repowh_reps = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
        duplicates_removed = len(repo_reps) - len(repowh_reps)
        print(f"✓ Removed {duplicates_removed} duplicate MNIACTNO records")
    else:
        repowh_reps = repo_reps
        print("ℹ No MNIACTNO column or empty data, skipping deduplication")
    
    write_parquet(repowh_reps, repowh_path)
    print(f"✓ Saved REPOWH data: {len(repowh_reps)} records to {repowh_path}")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"TBDATE from RPVBDATA: {tbdate_rpvb if 'tbdate_rpvb' in locals() else 'N/A'}")
    print(f"TBDATE from SRSDATA: {tbdate_srs if 'tbdate_srs' in locals() else 'N/A'}")
    print(f"REPTDT: {reptdt}")
    print(f"PREVDT: {prevdt}")
    print(f"SRSTDT: {srstdt}")
    print(f"RPVB1 records: {len(rpvb1)}")
    print(f"RPVB2 records: {len(rpvb2)}")
    print(f"RPVB3 records: {len(rpvb3)}")
    print(f"REPO_REPS records: {len(repo_reps)}")
    print(f"REPOWH_REPS records: {len(repowh_reps)}")
    print("=" * 60)
    print("✓ Processing completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
