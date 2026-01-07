from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path

# =========================
# PATHS
# =========================
base_input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
base_output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")
RPVBDATA_TXT_PATH = base_input_path / "RPVBDATA.txt"
SRSDATA_TXT_PATH = base_input_path / "SRSDATA.txt"
REPO_DIR = base_output_path / "REPO"
REPOWH_DIR = base_output_path / "REPOWH"

# =========================
# UTILITIES
# =========================
def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_month(d: date) -> date:
    nxt = date(d.year + (d.month == 12), 1 if d.month == 12 else d.month + 1, 1)
    return nxt - timedelta(days=1)

def MMYYN4(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"

def write_parquet(df: pl.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), p)

# =========================
# DATE PROCESSING
# =========================
def extract_dates():
    with open(RPVBDATA_TXT_PATH, 'r') as f:
        rpvb_first = f.readline().strip()
        rpvb_date = rpvb_first.split()[1] if rpvb_first.startswith('0') else ""
    
    with open(SRSDATA_TXT_PATH, 'r') as f:
        srs_first = f.readline().strip()
        srs_date = srs_first[:8] if srs_first else ""
    
    return rpvb_date, srs_date

def calculate_report_dates(rpvb_date_str: str, srs_date_str: str):
    rpvb_date = yyyymmdd_to_date(rpvb_date_str)
    srs_date = yyyymmdd_to_date(srs_date_str)
    
    REPTDATE = end_of_month(date(rpvb_date.year, rpvb_date.month, 1) - timedelta(days=1))
    PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))
    
    REPTDT = MMYYN4(REPTDATE)
    PREVDT = MMYYN4(PREVDATE)
    SRSTDT = MMYYN4(srs_date)
    
    return REPTDATE, PREVDATE, REPTDT, PREVDT, SRSTDT

# =========================
# DATA PROCESSING - SIMPLIFIED
# =========================
def parse_rpvdata():
    """Simplified parsing based on your original working code"""
    records = []
    
    with open(RPVBDATA_TXT_PATH, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('0'):
                continue
                
            if line.startswith('1'):
                parts = line.split()
                if len(parts) >= 32:  # Need at least 33 parts for all fields
                    record = {
                        'RECID': parts[0],
                        'MNIACTNO': parts[1],
                        'BRANCHNO': parts[2],
                        'NAME': ' '.join(parts[3:8]),
                        'ACCTSTA': parts[8],
                        'PRODTYPE': parts[9] if len(parts) > 9 else '',
                        'PRSTCOND': parts[10] if len(parts) > 10 else '',
                        'REGCARD': parts[11] if len(parts) > 11 else '',
                        'IGNTKEY': parts[12] if len(parts) > 12 else '',
                        'REPODIST': parts[13] if len(parts) > 13 else '',
                        'ACCTWOFF': parts[14] if len(parts) > 14 else '',
                        'MODEREPO': parts[15] if len(parts) > 15 else '',
                        'REPOPAID': parts[16] if len(parts) > 16 else '',
                        'REPOSTAT': parts[17] if len(parts) > 17 else '',
                        'TKEPRICE': parts[18] if len(parts) > 18 else '',
                        'MRKTVAL': parts[19] if len(parts) > 19 else '',
                        'RSVPRICE': parts[20] if len(parts) > 20 else '',
                        'FTHSCHLD': parts[21] if len(parts) > 21 else '',
                        'MODEDISP': parts[22] if len(parts) > 22 else '',
                        'APPVDISP': parts[23] if len(parts) > 23 else '',
                        'HOPRICE': parts[24] if len(parts) > 24 else '',
                        'NOAUCT': parts[25] if len(parts) > 25 else '',
                        'PRIOUT': parts[26] if len(parts) > 26 else '',
                        'DATEWOFF': parts[27] if len(parts) > 27 else '',
                        'DATEREPO': parts[28] if len(parts) > 28 else '',
                        'DATE5TH': parts[29] if len(parts) > 29 else '',
                        'DATEAPRV': parts[30] if len(parts) > 30 else '',
                        'DATESTLD': parts[31] if len(parts) > 31 else '',
                        'DATEHO': parts[32] if len(parts) > 32 else '',
                    }
                    records.append(record)
    
    df = pl.DataFrame(records)
    
    # Parse date fields - handle empty strings gracefully
    date_fields = ['DATEWOFF', 'DATEREPO', 'DATE5TH', 'DATEAPRV', 'DATESTLD', 'DATEHO']
    for field in date_fields:
        if field in df.columns:
            # First create a date column, then rename
            df = df.with_columns(
                pl.when(pl.col(field).str.len_chars() == 8)
                .then(pl.col(field))
                .otherwise(None)
                .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                .alias(f"{field}_DATE")
            )
    
    return df

def process_rpvb_data(df: pl.DataFrame):
    """Apply $UPCASE to relevant columns"""
    if len(df) == 0:
        return df
    
    uppercase_cols = ['NAME', 'ACCTSTA', 'PRSTCOND', 'REGCARD', 'IGNTKEY', 
                     'ACCTWOFF', 'MODEREPO', 'REPOSTAT', 'MODEDISP']
    
    expressions = []
    for col in uppercase_cols:
        if col in df.columns:
            expressions.append(pl.col(col).str.to_uppercase())
    
    if expressions:
        df = df.with_columns(expressions)
    
    return df

# =========================
# MAIN
# =========================
def main():
    # 1. Extract and validate dates
    rpvb_date_str, srs_date_str = extract_dates()
    REPTDATE, PREVDATE, REPTDT, PREVDT, SRSTDT = calculate_report_dates(rpvb_date_str, srs_date_str)
    
    if REPTDT != SRSTDT:
        raise RuntimeError(f"Date validation failed: REPTDT={REPTDT}, SRSTDT={SRSTDT}")
    
    print(f"Processing dates: REPTDT={REPTDT}, PREVDT={PREVDT}, SRSTDT={SRSTDT}")
    
    # 2. Parse and process data
    raw_df = parse_rpvdata()
    print(f"Raw data parsed: {len(raw_df)} records")
    
    RPVB1 = process_rpvb_data(raw_df)
    print(f"RPVB1 (after UPCASE): {len(RPVB1)} records")
    
    # 3. Apply filters
    if len(RPVB1) > 0:
        RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        RPVB3 = RPVB2.filter(pl.col("DATESTLD_DATE").is_not_null())
    else:
        RPVB2 = RPVB1
        RPVB3 = RPVB1
    
    print(f"RPVB2 (ACCTSTA in D,S,R): {len(RPVB2)} records")
    print(f"RPVB3 (with DATESTLD): {len(RPVB3)} records")
    
    # 4. Load previous month's data
    REPO_PREV_PATH = REPO_DIR / f"REPS_{PREVDT}.parquet"
    REPO_CURR_PATH = REPO_DIR / f"REPS_{REPTDT}.parquet"
    REPOWH_PATH = REPOWH_DIR / f"REPS_{REPTDT}.parquet"
    
    try:
        REPO_PREV = pl.read_parquet(REPO_PREV_PATH)
        print(f"Loaded previous REPO data: {len(REPO_PREV)} records")
        
        # Ensure schema compatibility
        for col in RPVB3.columns:
            if col not in REPO_PREV.columns:
                col_type = RPVB3.schema[col] if col in RPVB3.schema else pl.String()
                REPO_PREV = REPO_PREV.with_columns(pl.lit(None).cast(col_type).alias(col))
        
        if len(RPVB3.columns) > 0:
            REPO_PREV = REPO_PREV.select(RPVB3.columns)
            
    except Exception as e:
        print(f"No previous REPO data found: {e}")
        REPO_PREV = pl.DataFrame(schema=RPVB3.schema) if RPVB3.schema else pl.DataFrame()
    
    # 5. Combine current and previous data
    if len(REPO_PREV) == 0:
        REPO_REPS = RPVB3
    else:
        REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical")
    
    print(f"REPO combined data: {len(REPO_REPS)} records")
    
    # 6. Write outputs
    write_parquet(REPO_REPS, REPO_CURR_PATH)
    print(f"Saved REPO data to: {REPO_CURR_PATH}")
    
    # 7. Create deduplicated version
    if 'MNIACTNO' in REPO_REPS.columns and len(REPO_REPS) > 0:
        REPOWH_REPS = REPO_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"])
    else:
        REPOWH_REPS = REPO_REPS
    
    write_parquet(REPOWH_REPS, REPOWH_PATH)
    print(f"Saved REPOWH data to: {REPOWH_PATH}")
    print(f"REPOWH deduplicated: {len(REPOWH_REPS)} records")

if __name__ == "__main__":
    main()
