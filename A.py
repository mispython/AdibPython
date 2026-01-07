from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path

# =========================
# PATHS
# =========================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

RPVBDATA_PATH = BASE_INPUT / "RPVBDATA.txt"
SRSDATA_PATH = BASE_INPUT / "SRSDATA.txt"
REPO_DIR = BASE_OUTPUT / "REPO"
REPOWH_DIR = BASE_OUTPUT / "REPOWH"

# =========================
# UTILITIES
# =========================
def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path)

def yyyymmdd_to_date(s: str) -> date:
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except:
        return None

def end_of_month(d: date) -> date:
    """Get last day of month for given date"""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"

# =========================
# DATE EXTRACTION
# =========================
def extract_rpvb_date():
    """Extract date from RPVBDATA.txt"""
    with open(RPVBDATA_PATH, 'r') as f:
        line = f.readline().strip()
    return line.split()[1] if line.startswith('0') else ""

def extract_srs_date():
    """Extract date from SRSDATA.txt"""
    with open(SRSDATA_PATH, 'r') as f:
        line = f.readline().strip()
    return line[:8] if line else ""

# =========================
# DATA PARSING
# =========================
def parse_rpvdata():
    """Parse using your original working logic"""
    records = []
    
    with open(RPVBDATA_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.rstrip('\n')
        if not line or line.startswith('0'):
            continue
            
        if line.startswith('1'):
            parts = line.split()
            if len(parts) >= 15:  # Minimum for basic parsing
                # Your original parsing that gave 1207 records
                record = {
                    'RECID': parts[0],
                    'MNIACTNO': parts[1],
                    'BRANCHNO': parts[2],
                    'NAME': ' '.join(parts[3:8]),
                    'ACCTSTA': parts[8] if len(parts) > 8 else '',
                    'PRSTCOND': parts[9] if len(parts) > 9 else '',
                    'REGCARD': parts[10] if len(parts) > 10 else '',
                    'IGNTKEY': parts[11] if len(parts) > 11 else '',
                    'ACCTWOFF': parts[12] if len(parts) > 12 else '',
                    'MODEREPO': parts[13] if len(parts) > 13 else '',
                    'REPOSTAT': parts[14] if len(parts) > 14 else '',
                    'MODEDISP': parts[15] if len(parts) > 15 else '',
                    # Add other fields as needed from your original working code
                }
                records.append(record)
    
    df = pl.DataFrame(records)
    
    # Apply $UPCASE
    uppercase_cols = ['NAME', 'ACCTSTA', 'PRSTCOND', 'REGCARD', 'IGNTKEY', 
                     'ACCTWOFF', 'MODEREPO', 'REPOSTAT', 'MODEDISP']
    for col in uppercase_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_uppercase())
    
    return df

# =========================
# MAIN PROCESSING
# =========================
def main():
    # 1. Extract dates
    rpvb_date_str = extract_rpvb_date()
    srs_date_str = extract_srs_date()
    
    if not rpvb_date_str or not srs_date_str:
        raise ValueError("Could not extract dates from input files")
    
    # 2. Calculate dates
    tb_date = yyyymmdd_to_date(rpvb_date_str)
    
    # REPTDATE = end of previous month from TBDATE
    first_of_curr_month = date(tb_date.year, tb_date.month, 1)
    REPTDATE = end_of_month(first_of_curr_month - timedelta(days=1))
    PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))
    
    REPTDT = mmyy_format(REPTDATE)
    PREVDT = mmyy_format(PREVDATE)
    SRSTDT = mmyy_format(yyyymmdd_to_date(srs_date_str))
    
    # 3. Validate dates
    if REPTDT != SRSTDT:
        raise RuntimeError(f"Date mismatch: REPTDT={REPTDT}, SRSTDT={SRSTDT}")
    
    print(f"Dates: REPTDT={REPTDT}, PREVDT={PREVDT}, SRSTDT={SRSTDT}")
    
    # 4. Process data
    rpvb1 = parse_rpvdata()
    print(f"RPVB1: {len(rpvb1)} records")
    
    if len(rpvb1) > 0:
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null()) if 'DATESTLD' in rpvb2.columns else rpvb2
    else:
        rpvb2 = rpvb3 = rpvb1
    
    print(f"RPVB2: {len(rpvb2)} records, RPVB3: {len(rpvb3)} records")
    
    # 5. Load previous data
    repo_prev_path = REPO_DIR / f"REPS_{PREVDT}.parquet"
    repo_curr_path = REPO_DIR / f"REPS_{REPTDT}.parquet"
    repowh_path = REPOWH_DIR / f"REPS_{REPTDT}.parquet"
    
    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        print(f"Loaded previous data: {len(repo_prev)} records")
    except:
        repo_prev = pl.DataFrame()
        print("No previous data found")
    
    # 6. Combine and save
    if len(repo_prev) > 0 and len(rpvb3) > 0:
        repo_reps = pl.concat([rpvb3, repo_prev], how="vertical")
    else:
        repo_reps = rpvb3 if len(rpvb3) > 0 else repo_prev
    
    write_parquet(repo_reps, repo_curr_path)
    print(f"Saved REPO: {len(repo_reps)} records")
    
    # 7. Deduplicate
    if 'MNIACTNO' in repo_reps.columns and len(repo_reps) > 0:
        repowh_reps = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"])
    else:
        repowh_reps = repo_reps
    
    write_parquet(repowh_reps, repowh_path)
    print(f"Saved REPOWH: {len(repowh_reps)} records")

if __name__ == "__main__":
    main()
