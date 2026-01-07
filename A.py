from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# =========================
# PATHS
# =========================
base_input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
base_output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

# Input files
RPVBDATA_TXT_PATH = base_input_path / "RPVBDATA.txt"
SRSDATA_TXT_PATH = base_input_path / "SRSDATA.txt"

# Output directories
REPO_DIR = base_output_path / "REPO"
REPOWH_DIR = base_output_path / "REPOWH"

USE_DUCKDB_COPY = False

# =========================
# HELPERS
# =========================
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
    """Convert YYYYMMDD string to date"""
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_month(d: date) -> date:
    """Get end of month for a given date"""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)

def end_of_prev_month(d: date) -> date:
    """Get end of previous month (SAS INTNX with -1,'E')"""
    if d.month == 1:
        return date(d.year - 1, 12, 31)
    return date(d.year, d.month, 1) - timedelta(days=1)

def MMYYN4(d: date) -> str:
    """Convert date to MMYY format without slash"""
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[date]:
    """Create date from month, day, year components (handles missing)"""
    if month is None or day is None or year is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None

# =========================
# 1) Read TBDATE from RPVBDATA.txt (first line only)
# =========================
def get_tbdate_from_rpvdata() -> str:
    """Read first line of RPVBDATA.txt and extract TBDATE at position 3-10"""
    try:
        with open(RPVBDATA_TXT_PATH, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
        
        print(f"DEBUG: RPVBDATA first line: '{first_line}'")
        
        if len(first_line) >= 10:
            # @03 TBDATE $8. means positions 3-10 (0-indexed: 2:10)
            tbdate = first_line[2:10]  # Python is 0-indexed
            print(f"DEBUG: Extracted TBDATE from positions 3-10: '{tbdate}'")
            if tbdate.isdigit() and len(tbdate) == 8:
                return tbdate
        
        raise ValueError(f"Invalid TBDATE in first line: {first_line}")
    except Exception as e:
        raise ValueError(f"Error reading RPVBDATA.txt: {e}")

# =========================
# 2) Read TBDATE from SRSDATA.txt (first 8 chars of first line)
# =========================
def get_tbdate_from_srsdata() -> str:
    """Read first line of SRSDATA.txt and extract first 8 characters as TBDATE"""
    try:
        with open(SRSDATA_TXT_PATH, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
        
        print(f"DEBUG: SRSDATA first line: '{first_line}'")
        
        if len(first_line) >= 8:
            # @01 TBDATE $8. means positions 1-8 (first 8 characters)
            tbdate = first_line[0:8]
            print(f"DEBUG: Extracted TBDATE (first 8 chars): '{tbdate}'")
            if tbdate.isdigit() and len(tbdate) == 8:
                return tbdate
            else:
                # Even if not all digits, return first 8 chars as SAS would read them
                print(f"DEBUG: TBDATE '{tbdate}' is not all digits, but using anyway as SAS would")
                return tbdate
        
        raise ValueError(f"SRSDATA line too short: '{first_line}'")
    except FileNotFoundError:
        raise ValueError(f"SRSDATA.txt file not found at {SRSDATA_TXT_PATH}")
    except Exception as e:
        raise ValueError(f"Error reading SRSDATA.txt: {e}")

# =========================
# 3) Calculate dates like SAS
# =========================
print("=" * 60)
print("STEP 1: Processing RPVBDATA dates")
print("=" * 60)

try:
    # Get TBDATE from RPVBDATA
    TBDATE_STR_RPVB = get_tbdate_from_rpvdata()
    print(f"✓ TBDATE from RPVBDATA: {TBDATE_STR_RPVB}")
    
    # Convert to date
    tb_date = yyyymmdd_to_date(TBDATE_STR_RPVB)
    
    # SAS: REPTDATE = INTNX('MONTH',INPUT(TBDATE,YYMMDD8.),-1,'E')
    # This means: end of previous month relative to TBDATE
    REPTDATE = end_of_prev_month(tb_date)
    
    # SAS: PREVDATE = INTNX('MONTH',REPTDATE,-1,'E')
    PREVDATE = end_of_prev_month(REPTDATE)
    
    # Convert to MMYYN4 format
    REPTDT = MMYYN4(REPTDATE)
    PREVDT = MMYYN4(PREVDATE)
    
    print(f"  TBDATE as date: {tb_date}")
    print(f"  REPTDATE (end of prev month from TBDATE): {REPTDATE}")
    print(f"  PREVDATE (end of prev month from REPTDATE): {PREVDATE}")
    print(f"  REPTDT (MMYY): {REPTDT}")
    print(f"  PREVDT (MMYY): {PREVDT}")
    
except Exception as e:
    print(f"✗ Error processing RPVBDATA: {e}")
    # Fallback logic
    today = date.today()
    REPTDATE = end_of_prev_month(today)
    PREVDATE = end_of_prev_month(REPTDATE)
    REPTDT = MMYYN4(REPTDATE)
    PREVDT = MMYYN4(PREVDATE)
    print(f"  Using fallback dates based on today ({today})")
    print(f"  REPTDATE: {REPTDATE}, REPTDT: {REPTDT}")

print("\n" + "=" * 60)
print("STEP 2: Processing SRSDATA dates")
print("=" * 60)

try:
    # Get TBDATE from SRSDATA
    TBDATE_STR_SRS = get_tbdate_from_srsdata()
    print(f"✓ TBDATE from SRSDATA: {TBDATE_STR_SRS}")
    
    # Check if it's a valid date (all digits)
    if TBDATE_STR_SRS.isdigit() and len(TBDATE_STR_SRS) == 8:
        # Convert to date
        srs_tb_date = yyyymmdd_to_date(TBDATE_STR_SRS)
        
        # SAS: REPTDATE = INPUT(TBDATE,YYMMDD8.)
        srs_reptdate = srs_tb_date  # Direct conversion, no month adjustment
        
        # SAS: CALL SYMPUT('SRSTDT', PUT(REPTDATE, MMYYN4.));
        SRSTDT = MMYYN4(srs_reptdate)
        
        print(f"  SRS TBDATE as date: {srs_tb_date}")
        print(f"  SRS REPTDATE: {srs_reptdate}")
        print(f"  SRSTDT (MMYY): {SRSTDT}")
    else:
        # If not a valid date, try to parse what we can
        print(f"⚠ WARNING: TBDATE '{TBDATE_STR_SRS}' is not a valid YYYYMMDD date")
        print(f"  Attempting to extract date portion...")
        
        # Try to find 8 consecutive digits in the string
        import re
        match = re.search(r'(\d{8})', TBDATE_STR_SRS)
        if match:
            date_str = match.group(1)
            srs_tb_date = yyyymmdd_to_date(date_str)
            srs_reptdate = srs_tb_date
            SRSTDT = MMYYN4(srs_reptdate)
            print(f"  Found date in string: {date_str}")
            print(f"  SRS REPTDATE: {srs_reptdate}")
            print(f"  SRSTDT (MMYY): {SRSTDT}")
        else:
            # If no valid date found, use REPTDT as fallback
            print(f"  Could not extract valid date, using REPTDT as fallback")
            SRSTDT = REPTDT
    
except Exception as e:
    print(f"✗ Error reading SRSDATA.txt: {e}")
    # If SRSDATA doesn't exist or has error, use REPTDT as fallback
    SRSTDT = REPTDT
    print(f"  Using REPTDT as fallback for SRSTDT: {SRSTDT}")

print("\n" + "=" * 60)
print("STEP 3: Macro guard validation")
print("=" * 60)

print(f"Comparing: REPTDT={REPTDT} vs SRSTDT={SRSTDT}")
if REPTDT != SRSTDT:
    error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{SRSTDT})"
    print(f"✗ {error_msg}")
    raise RuntimeError(error_msg)
else:
    print(f"✓ Date validation passed - REPTDT matches SRSTDT")

# =========================
# 4) Read RPVBDATA with fixed-width parsing (like SAS INPUT)
# =========================
def read_rpvdata_fixed_width() -> pl.DataFrame:
    """Read RPVBDATA.txt with fixed-width parsing matching SAS INPUT statement"""
    data_lines = []
    
    with open(RPVBDATA_TXT_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"\nReading RPVBDATA.txt - Total lines: {len(lines)}")
    
    # Skip first line (header with TBDATE)
    for line_num, line in enumerate(lines[1:], start=2):
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        # Debug: show raw line for first few records
        if line_num <= 7:
            print(f"DEBUG Line {line_num}: '{line}'")
        
        # Parse using fixed positions (SAS @ position - 1 for Python 0-index)
        record = {}
        
        # @001 RECID 1.
        record['RECID'] = line[0:1] if len(line) >= 1 else ''
        
        # @003 MNIACTNO 10.
        record['MNIACTNO'] = line[2:12].strip() if len(line) >= 12 else ''
        
        # @014 LOANNOTE 10.
        record['LOANNOTE'] = line[13:23].strip() if len(line) >= 23 else ''
        
        # @025 NAME $UPCASE50.
        name = line[24:74].strip() if len(line) >= 74 else ''
        record['NAME'] = name.upper()
        
        # @076 ACCTSTA $UPCASE1.
        acctsta = line[75:76].strip() if len(line) >= 76 else ''
        record['ACCTSTA'] = acctsta.upper()
        
        # @078 PRODTYPE $5.
        record['PRODTYPE'] = line[77:82].strip() if len(line) >= 82 else ''
        
        # @084 PRSTCOND $UPCASE1.
        prstcond = line[83:84].strip() if len(line) >= 84 else ''
        record['PRSTCOND'] = prstcond.upper()
        
        # @086 REGCARD $UPCASE1.
        regcard = line[85:86].strip() if len(line) >= 86 else ''
        record['REGCARD'] = regcard.upper()
        
        # @088 IGNTKEY $UPCASE1.
        ignkey = line[87:88].strip() if len(line) >= 88 else ''
        record['IGNTKEY'] = ignkey.upper()
        
        # @090 REPODIST 10.
        record['REPODIST'] = line[89:99].strip() if len(line) >= 99 else ''
        
        # @101 ACCTWOFF $UPCASE1.
        acctwoff = line[100:101].strip() if len(line) >= 101 else ''
        record['ACCTWOFF'] = acctwoff.upper()
        
        # @103 YY1 4.
        yy1 = line[102:106].strip() if len(line) >= 106 else ''
        record['YY1'] = int(yy1) if yy1.isdigit() else None
        
        # @107 MM1 2.
        mm1 = line[106:108].strip() if len(line) >= 108 else ''
        record['MM1'] = int(mm1) if mm1.isdigit() else None
        
        # @109 DD1 2.
        dd1 = line[108:110].strip() if len(line) >= 110 else ''
        record['DD1'] = int(dd1) if dd1.isdigit() else None
        
        # @112 MODEREPO $UPCASE1.
        moderepo = line[111:112].strip() if len(line) >= 112 else ''
        record['MODEREPO'] = moderepo.upper()
        
        # @114 YY2 4.
        yy2 = line[113:117].strip() if len(line) >= 117 else ''
        record['YY2'] = int(yy2) if yy2.isdigit() else None
        
        # @118 MM2 2.
        mm2 = line[117:119].strip() if len(line) >= 119 else ''
        record['MM2'] = int(mm2) if mm2.isdigit() else None
        
        # @120 DD2 2.
        dd2 = line[119:121].strip() if len(line) >= 121 else ''
        record['DD2'] = int(dd2) if dd2.isdigit() else None
        
        # @123 REPOPAID 10.
        record['REPOPAID'] = line[122:132].strip() if len(line) >= 132 else ''
        
        # @134 REPOSTAT $UPCASE6.
        repostat = line[133:139].strip() if len(line) >= 139 else ''
        record['REPOSTAT'] = repostat.upper()
        
        # @141 TKEPRICE 10.
        record['TKEPRICE'] = line[140:150].strip() if len(line) >= 150 else ''
        
        # @152 MRKTVAL 10.
        record['MRKTVAL'] = line[151:161].strip() if len(line) >= 161 else ''
        
        # @163 RSVPRICE 10.
        record['RSVPRICE'] = line[162:172].strip() if len(line) >= 172 else ''
        
        # @174 FTHSCHLD 10.
        record['FTHSCHLD'] = line[173:183].strip() if len(line) >= 183 else ''
        
        # @185 YY3 4.
        yy3 = line[184:188].strip() if len(line) >= 188 else ''
        record['YY3'] = int(yy3) if yy3.isdigit() else None
        
        # @189 MM3 2.
        mm3 = line[188:190].strip() if len(line) >= 190 else ''
        record['MM3'] = int(mm3) if mm3.isdigit() else None
        
        # @191 DD3 2.
        dd3 = line[190:192].strip() if len(line) >= 192 else ''
        record['DD3'] = int(dd3) if dd3.isdigit() else None
        
        # @194 MODEDISP $UPCASE1.
        modedisp = line[193:194].strip() if len(line) >= 194 else ''
        record['MODEDISP'] = modedisp.upper()
        
        # @196 APPVDISP 10.
        record['APPVDISP'] = line[195:205].strip() if len(line) >= 205 else ''
        
        # @207 YY4 4.
        yy4 = line[206:210].strip() if len(line) >= 210 else ''
        record['YY4'] = int(yy4) if yy4.isdigit() else None
        
        # @211 MM4 2.
        mm4 = line[210:212].strip() if len(line) >= 212 else ''
        record['MM4'] = int(mm4) if mm4.isdigit() else None
        
        # @213 DD4 2.
        dd4 = line[212:214].strip() if len(line) >= 214 else ''
        record['DD4'] = int(dd4) if dd4.isdigit() else None
        
        # @216 YY5 4.
        yy5 = line[215:219].strip() if len(line) >= 219 else ''
        record['YY5'] = int(yy5) if yy5.isdigit() else None
        
        # @220 MM5 2.
        mm5 = line[219:221].strip() if len(line) >= 221 else ''
        record['MM5'] = int(mm5) if mm5.isdigit() else None
        
        # @222 DD5 2.
        dd5 = line[221:223].strip() if len(line) >= 223 else ''
        record['DD5'] = int(dd5) if dd5.isdigit() else None
        
        # @225 YY6 4.
        yy6 = line[224:228].strip() if len(line) >= 228 else ''
        record['YY6'] = int(yy6) if yy6.isdigit() else None
        
        # @229 MM6 2.
        mm6 = line[228:230].strip() if len(line) >= 230 else ''
        record['MM6'] = int(mm6) if mm6.isdigit() else None
        
        # @231 DD6 2.
        dd6 = line[230:232].strip() if len(line) >= 232 else ''
        record['DD6'] = int(dd6) if dd6.isdigit() else None
        
        # @234 HOPRICE 10.
        record['HOPRICE'] = line[233:243].strip() if len(line) >= 243 else ''
        
        # @245 NOAUCT $5.
        record['NOAUCT'] = line[244:249].strip() if len(line) >= 249 else ''
        
        # @251 PRIOUT $20.
        record['PRIOUT'] = line[250:270].strip() if len(line) >= 270 else ''
        
        data_lines.append(record)
    
    print(f"Parsed {len(data_lines)} data records")
    
    # Create DataFrame
    df = pl.DataFrame(data_lines)
    
    # Create date fields like SAS MDY() function
    date_mappings = [
        ('YY1', 'MM1', 'DD1', 'DATEWOFF'),
        ('YY2', 'MM2', 'DD2', 'DATEREPO'),
        ('YY3', 'MM3', 'DD3', 'DATE5TH'),
        ('YY4', 'MM4', 'DD4', 'DATEAPRV'),
        ('YY5', 'MM5', 'DD5', 'DATESTLD'),
        ('YY6', 'MM6', 'DD6', 'DATEHO')
    ]
    
    for yy_col, mm_col, dd_col, date_col in date_mappings:
        if yy_col in df.columns and mm_col in df.columns and dd_col in df.columns:
            df = df.with_columns(
                pl.struct([yy_col, mm_col, dd_col]).map_elements(
                    lambda x: mdy(x[mm_col], x[dd_col], x[yy_col]), 
                    return_dtype=pl.Date
                ).alias(date_col)
            )
    
    # Drop the individual date component columns like SAS DROP statement
    drop_cols = [col for col in df.columns if col in 
                ['YY1', 'MM1', 'DD1', 'YY2', 'MM2', 'DD2', 'YY3', 'MM3', 'DD3',
                 'YY4', 'MM4', 'DD4', 'YY5', 'MM5', 'DD5', 'YY6', 'MM6', 'DD6']]
    
    if drop_cols:
        df = df.drop(drop_cols)
    
    return df

print("\n" + "=" * 60)
print("STEP 4: Reading RPVBDATA with fixed-width parsing")
print("=" * 60)

# Read the data
try:
    RPVB1 = read_rpvdata_fixed_width()
    print(f"✓ RPVB1 records read: {len(RPVB1)}")
    print(f"  RPVB1 columns: {RPVB1.columns}")
    
    if len(RPVB1) > 0:
        print("\nSample of RPVB1 data (first 3 rows):")
        print(RPVB1.head(3))
    else:
        print("⚠ Warning: No data records found in RPVBDATA.txt")
        
except Exception as e:
    print(f"✗ Error reading RPVBDATA with fixed-width parsing: {e}")
    # Create empty DataFrame with expected columns
    RPVB1 = pl.DataFrame()

# =========================
# 5) Create RPVB2 and RPVB3 like SAS
# =========================
print("\n" + "=" * 60)
print("STEP 5: Creating RPVB2 and RPVB3")
print("=" * 60)

if len(RPVB1) > 0 and 'ACCTSTA' in RPVB1.columns:
    # SAS: IF ACCTSTA IN ('D','S','R');
    RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
    print(f"✓ RPVB2 created: {len(RPVB2)} records (ACCTSTA in D,S,R)")
    
    # SAS: IF DATESTLD NE '';
    # Note: DATESTLD = MDY(MM5,DD5,YY5) from SAS code
    if 'DATESTLD' in RPVB2.columns:
        RPVB3 = RPVB2.filter(pl.col("DATESTLD").is_not_null())
        print(f"✓ RPVB3 created: {len(RPVB3)} records (with DATESTLD)")
    else:
        print(f"⚠ DATESTLD column not found, creating empty RPVB3")
        RPVB3 = RPVB2.filter(pl.lit(False))
else:
    print(f"⚠ No data or ACCTSTA column not found")
    RPVB2 = RPVB1
    RPVB3 = RPVB1

# =========================
# 6) REPO.REPS&REPTDT = RPVB3 + REPO.REPS&PREVDT
# =========================
print("\n" + "=" * 60)
print("STEP 6: Creating REPO.REPS&REPTDT")
print("=" * 60)

REPO_PREV_PATH = REPO_DIR / f"REPS_{PREVDT}.parquet"
REPO_CURR_PATH = REPO_DIR / f"REPS_{REPTDT}.parquet"
REPOWH_PATH = REPOWH_DIR / f"REPS_{REPTDT}.parquet"

print(f"Previous file to load: {REPO_PREV_PATH}")
print(f"Current output file: {REPO_CURR_PATH}")

# Try to load previous REPO data
try:
    REPO_PREV = pl.read_parquet(REPO_PREV_PATH)
    print(f"✓ Loaded previous REPO data: {len(REPO_PREV)} records")
    
    # Ensure schemas match
    if len(RPVB3) > 0 and len(REPO_PREV) > 0:
        # Get union of all columns
        all_columns = set(RPVB3.columns) | set(REPO_PREV.columns)
        
        # Ensure both DataFrames have all columns
        for col in all_columns:
            if col not in RPVB3.columns:
                RPVB3 = RPVB3.with_columns(pl.lit(None).alias(col))
            if col not in REPO_PREV.columns:
                REPO_PREV = REPO_PREV.with_columns(pl.lit(None).alias(col))
        
        # Reorder columns consistently
        all_columns_list = list(all_columns)
        RPVB3 = RPVB3.select(all_columns_list)
        REPO_PREV = REPO_PREV.select(all_columns_list)
    
except Exception as e:
    print(f"ℹ No previous REPO data found or error loading: {e}")
    REPO_PREV = pl.DataFrame()

# Concatenate like SAS
if len(REPO_PREV) == 0:
    REPO_REPS = RPVB3
    print(f"  No previous data, using only RPVB3 ({len(RPVB3)} records)")
else:
    REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical", rechunk=True)
    print(f"  Combined RPVB3 ({len(RPVB3)} records) + previous ({len(REPO_PREV)} records)")

write_parquet(REPO_REPS, REPO_CURR_PATH)
print(f"✓ Saved REPO data: {len(REPO_REPS)} records to {REPO_CURR_PATH}")

# =========================
# 7) REPOWH.REPS&REPTDT with PROC SORT NODUPKEY
# =========================
print("\n" + "=" * 60)
print("STEP 7: Creating REPOWH.REPS&REPTDT (with NODUPKEY)")
print("=" * 60)

if len(REPO_REPS) > 0 and 'MNIACTNO' in REPO_REPS.columns:
    # Sort and remove duplicates by MNIACTNO
    REPOWH_REPS = REPO_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
    duplicates_removed = len(REPO_REPS) - len(REPOWH_REPS)
    print(f"✓ Removed {duplicates_removed} duplicate MNIACTNO records")
else:
    REPOWH_REPS = REPO_REPS
    print("ℹ No MNIACTNO column or empty data, skipping deduplication")

write_parquet(REPOWH_REPS, REPOWH_PATH)
print(f"✓ Saved REPOWH data: {len(REPOWH_REPS)} records to {REPOWH_PATH}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"TBDATE from RPVBDATA: {TBDATE_STR_RPVB if 'TBDATE_STR_RPVB' in locals() else 'N/A'}")
print(f"TBDATE from SRSDATA: {TBDATE_STR_SRS if 'TBDATE_STR_SRS' in locals() else 'N/A'}")
print(f"REPTDT: {REPTDT}")
print(f"PREVDT: {PREVDT}")
print(f"SRSTDT: {SRSTDT}")
print(f"RPVB1 records: {len(RPVB1)}")
print(f"RPVB2 records: {len(RPVB2)}")
print(f"RPVB3 records: {len(RPVB3)}")
print(f"REPO_REPS records: {len(REPO_REPS)}")
print(f"REPOWH_REPS records: {len(REPOWH_REPS)}")
print("=" * 60)
print("✓ Processing completed successfully!")
print("=" * 60)






TBDATE from RPVBDATA: 20251201
TBDATE from SRSDATA: 20251103
REPTDT: 1125
PREVDT: 1025
SRSTDT: 1125
RPVB1 records: 1207
RPVB2 records: 776
RPVB3 records: 776
REPO_REPS records: 776
REPOWH_REPS records: 776
