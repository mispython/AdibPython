from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path
import logging
from typing import Optional

# =========================
# LOGGING CONFIGURATION
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
def write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Write DataFrame to Parquet file with directory creation"""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path)
    logger.info(f"Wrote {len(df)} records to {path}")

def yyyymmdd_to_date(s: str) -> Optional[date]:
    """Convert YYYYMMDD string to date object"""
    try:
        if s and len(s) >= 8 and s.isdigit():
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return None
    except (ValueError, TypeError):
        return None

def end_of_month(d: date) -> date:
    """Get last day of the month for given date"""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)

def mdy(month: int, day: int, year: int) -> Optional[date]:
    """SAS MDY function equivalent"""
    try:
        if month and day and year:
            return date(year, month, day)
        return None
    except (ValueError, TypeError):
        return None

def mmyy_format(d: date) -> str:
    """Format date as MMYY (SAS MMYYN4. format)"""
    return f"{d.month:02d}{d.year % 100:02d}"

def safe_int(value: str) -> Optional[int]:
    """Safely convert string to int, handling blanks and invalid values"""
    if value is None:
        return None
    value = str(value).strip()
    if not value or value == '':
        return None
    try:
        return int(value)
    except ValueError:
        return None

# =========================
# DATE EXTRACTION (SAS equivalent)
# =========================
def extract_rpvb_date() -> str:
    """
    Extract date from RPVBDATA.txt header (record type '0')
    SAS: INPUT @03 TBDATE $8. (position 3, length 8)
    """
    with open(RPVBDATA_PATH, 'r') as f:
        first_line = f.readline().strip()
    # SAS @03 means position 3 (1-indexed) = index 2 (0-indexed)
    return first_line[2:10] if len(first_line) >= 10 else ""

def extract_srs_date() -> str:
    """
    Extract date from SRSDATA.txt first line
    SAS: INPUT @01 TBDATE $8. (position 1, length 8)
    """
    with open(SRSDATA_PATH, 'r') as f:
        first_line = f.readline().strip()
    # SAS @01 means position 1 (1-indexed) = index 0 (0-indexed)
    return first_line[:8] if len(first_line) >= 8 else ""

# =========================
# DATA PARSING - EXACT SAS EQUIVALENT
# =========================
def parse_rpvdata() -> pl.DataFrame:
    """
    Parse RPVBDATA.txt using exact SAS fixed-width positions
    Based on the SAS INPUT statements
    """
    records = []
    
    with open(RPVBDATA_PATH, 'r') as f:
        lines = f.readlines()
    
    # Skip header (FIRSTOBS=2 in SAS)
    for line_num, line in enumerate(lines[1:], start=2):
        line = line.rstrip('\n')
        if not line.strip():
            continue
            
        # Ensure line is long enough
        if len(line) < 251:  # Minimum length based on SAS positions
            continue
        
        try:
            # Extract fields based on SAS positions (1-indexed -> 0-indexed)
            record = {
                # @001 RECID 1. -> position 1, length 1
                'RECID': line[0:1].strip() if len(line) > 0 else '',
                # @003 MNIACTNO 10. -> position 3, length 10
                'MNIACTNO': line[2:12].strip() if len(line) >= 12 else '',
                # @014 LOANNOTE 10. -> position 14, length 10
                'LOANNOTE': line[13:23].strip() if len(line) >= 23 else '',
                # @025 NAME $UPCASE50. -> position 25, length 50
                'NAME': line[24:74].strip().upper() if len(line) >= 74 else '',
                # @076 ACCTSTA $UPCASE1. -> position 76, length 1
                'ACCTSTA': line[75:76].strip().upper() if len(line) >= 76 else '',
                # @078 PRODTYPE $5. -> position 78, length 5
                'PRODTYPE': line[77:82].strip() if len(line) >= 82 else '',
                # @084 PRSTCOND $UPCASE1. -> position 84, length 1
                'PRSTCOND': line[83:84].strip().upper() if len(line) >= 84 else '',
                # @086 REGCARD $UPCASE1. -> position 86, length 1
                'REGCARD': line[85:86].strip().upper() if len(line) >= 86 else '',
                # @088 IGNTKEY $UPCASE1. -> position 88, length 1
                'IGNTKEY': line[87:88].strip().upper() if len(line) >= 88 else '',
                # @090 REPODIST 10. -> position 90, length 10
                'REPODIST': safe_int(line[89:99].strip() if len(line) >= 99 else ''),
                # @101 ACCTWOFF $UPCASE1. -> position 101, length 1
                'ACCTWOFF': line[100:101].strip().upper() if len(line) >= 101 else '',
                # Date components for DATEWOFF
                'YY1': safe_int(line[102:106].strip() if len(line) >= 106 else ''),
                'MM1': safe_int(line[106:108].strip() if len(line) >= 108 else ''),
                'DD1': safe_int(line[108:110].strip() if len(line) >= 110 else ''),
                # @112 MODEREPO $UPCASE1. -> position 112, length 1
                'MODEREPO': line[111:112].strip().upper() if len(line) >= 112 else '',
                # Date components for DATEREPO
                'YY2': safe_int(line[113:117].strip() if len(line) >= 117 else ''),
                'MM2': safe_int(line[117:119].strip() if len(line) >= 119 else ''),
                'DD2': safe_int(line[119:121].strip() if len(line) >= 121 else ''),
                # @123 REPOPAID 10. -> position 123, length 10
                'REPOPAID': safe_int(line[122:132].strip() if len(line) >= 132 else ''),
                # @134 REPOSTAT $UPCASE6. -> position 134, length 6
                'REPOSTAT': line[133:139].strip().upper() if len(line) >= 139 else '',
                # @141 TKEPRICE 10. -> position 141, length 10
                'TKEPRICE': safe_int(line[140:150].strip() if len(line) >= 150 else ''),
                # @152 MRKTVAL 10. -> position 152, length 10
                'MRKTVAL': safe_int(line[151:161].strip() if len(line) >= 161 else ''),
                # @163 RSVPRICE 10. -> position 163, length 10
                'RSVPRICE': safe_int(line[162:172].strip() if len(line) >= 172 else ''),
                # @174 FTHSCHLD 10. -> position 174, length 10
                'FTHSCHLD': safe_int(line[173:183].strip() if len(line) >= 183 else ''),
                # Date components for DATE5TH
                'YY3': safe_int(line[184:188].strip() if len(line) >= 188 else ''),
                'MM3': safe_int(line[188:190].strip() if len(line) >= 190 else ''),
                'DD3': safe_int(line[190:192].strip() if len(line) >= 192 else ''),
                # @194 MODEDISP $UPCASE1. -> position 194, length 1
                'MODEDISP': line[193:194].strip().upper() if len(line) >= 194 else '',
                # @196 APPVDISP 10. -> position 196, length 10
                'APPVDISP': safe_int(line[195:205].strip() if len(line) >= 205 else ''),
                # Date components for DATEAPRV
                'YY4': safe_int(line[206:210].strip() if len(line) >= 210 else ''),
                'MM4': safe_int(line[210:212].strip() if len(line) >= 212 else ''),
                'DD4': safe_int(line[212:214].strip() if len(line) >= 214 else ''),
                # Date components for DATESTLD
                'YY5': safe_int(line[215:219].strip() if len(line) >= 219 else ''),
                'MM5': safe_int(line[219:221].strip() if len(line) >= 221 else ''),
                'DD5': safe_int(line[221:223].strip() if len(line) >= 223 else ''),
                # Date components for DATEHO
                'YY6': safe_int(line[224:228].strip() if len(line) >= 228 else ''),
                'MM6': safe_int(line[228:230].strip() if len(line) >= 230 else ''),
                'DD6': safe_int(line[230:232].strip() if len(line) >= 232 else ''),
                # @234 HOPRICE 10. -> position 234, length 10
                'HOPRICE': safe_int(line[233:243].strip() if len(line) >= 243 else ''),
                # @245 NOAUCT $5. -> position 245, length 5
                'NOAUCT': line[244:249].strip() if len(line) >= 249 else '',
                # @251 PRIOUT $20. -> position 251, length 20
                'PRIOUT': line[250:270].strip() if len(line) >= 270 else '',
            }
            
            # Create date fields using MDY function equivalent
            record['DATEWOFF'] = mdy(record['MM1'], record['DD1'], record['YY1'])
            record['DATEREPO'] = mdy(record['MM2'], record['DD2'], record['YY2'])
            record['DATE5TH'] = mdy(record['MM3'], record['DD3'], record['YY3'])
            record['DATEAPRV'] = mdy(record['MM4'], record['DD4'], record['YY4'])
            record['DATESTLD'] = mdy(record['MM5'], record['DD5'], record['YY5'])
            record['DATEHO'] = mdy(record['MM6'], record['DD6'], record['YY6'])
            
            records.append(record)
            
        except Exception as e:
            logger.debug(f"Error parsing line {line_num}: {e}")
            continue
    
    logger.info(f"Parsed {len(records)} records from {RPVBDATA_PATH}")
    return pl.DataFrame(records)

# =========================
# MAIN PROCESSING
# =========================
def main():
    """Main processing pipeline - exact SAS logic equivalent"""
    try:
        logger.info("="*60)
        logger.info("Starting REPO Processing Pipeline")
        logger.info("="*60)
        
        # ==========================================
        # STEP 1: Extract dates (SAS DATA REPTDATE)
        # ==========================================
        logger.info("STEP 1: Extracting dates from input files")
        
        # SAS: INPUT @03 TBDATE $8.
        rpvb_date_str = extract_rpvb_date()
        logger.info(f"RPVBDATA TBDATE: {rpvb_date_str}")
        
        # SAS: REPTDATE = INTNX('MONTH',INPUT(TBDATE,YYMMDD8.),-1,'E')
        tb_date = yyyymmdd_to_date(rpvb_date_str)
        if tb_date is None:
            raise ValueError(f"Invalid RPVB date: {rpvb_date_str}")
        
        # End of previous month
        first_of_month = date(tb_date.year, tb_date.month, 1)
        REPTDATE = end_of_month(first_of_month - timedelta(days=1))
        
        # SAS: PREVDATE = INTNX('MONTH',REPTDATE,-1,'E')
        PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))
        
        # SAS: CALL SYMPUT('REPTDT', PUT(REPTDATE, MMYYN4.))
        REPTDT = mmyy_format(REPTDATE)
        PREVDT = mmyy_format(PREVDATE)
        
        logger.info(f"REPTDATE: {REPTDATE} ({REPTDT})")
        logger.info(f"PREVDATE: {PREVDATE} ({PREVDT})")
        
        # ==========================================
        # STEP 2: Extract SRS date (DATA _NULL_)
        # ==========================================
        logger.info("STEP 2: Processing SRSDATA dates")
        
        # SAS: INPUT @01 TBDATE $8.
        srs_date_str = extract_srs_date()
        logger.info(f"SRSDATA TBDATE: {srs_date_str}")
        
        # SAS: REPTDATE = INPUT(TBDATE,YYMMDD8.)
        srs_date = yyyymmdd_to_date(srs_date_str)
        if srs_date is None:
            raise ValueError(f"Invalid SRS date: {srs_date_str}")
        
        # SAS: CALL SYMPUT('SRSTDT', PUT(REPTDATE, MMYYN4.))
        SRSTDT = mmyy_format(srs_date)
        logger.info(f"SRSTDT: {SRSTDT}")
        
        # ==========================================
        # STEP 3: Macro guard validation
        # ==========================================
        logger.info("STEP 3: Validating dates")
        if REPTDT != SRSTDT:
            error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{SRSTDT})"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        logger.info("✓ Date validation passed")
        
        # ==========================================
        # STEP 4: Parse RPVB data (DATA RPVB1)
        # ==========================================
        logger.info("STEP 4: Parsing RPVB data")
        RPVB1 = parse_rpvdata()
        logger.info(f"RPVB1: {len(RPVB1)} records")
        
        # ==========================================
        # STEP 5: Apply filters (DATA RPVB2, RPVB3)
        # ==========================================
        logger.info("STEP 5: Applying filters")
        
        # SAS: IF ACCTSTA IN ('D','S','R')
        if len(RPVB1) > 0:
            RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
            logger.info(f"RPVB2 (ACCTSTA in D,S,R): {len(RPVB2)} records")
        else:
            RPVB2 = RPVB1
        
        # SAS: IF DATESTLD NE ''
        if len(RPVB2) > 0:
            RPVB3 = RPVB2.filter(pl.col("DATESTLD").is_not_null())
            logger.info(f"RPVB3 (with DATESTLD): {len(RPVB3)} records")
        else:
            RPVB3 = RPVB2
        
        # ==========================================
        # STEP 6: Create REPO dataset
        # ==========================================
        logger.info("STEP 6: Creating REPO dataset")
        
        repo_prev_path = REPO_DIR / f"REPS_{PREVDT}.parquet"
        repo_curr_path = REPO_DIR / f"REPS_{REPTDT}.parquet"
        
        # SAS: DATA REPO.REPS&REPTDT; SET RPVB3 REPO.REPS&PREVDT;
        try:
            REPO_PREV = pl.read_parquet(repo_prev_path)
            logger.info(f"Loaded previous REPO data: {len(REPO_PREV)} records")
        except Exception as e:
            logger.info(f"No previous REPO data found: {e}")
            REPO_PREV = pl.DataFrame()
        
        # Combine current and previous
        if len(RPVB3) == 0 and len(REPO_PREV) == 0:
            REPO_REPS = pl.DataFrame()
        elif len(RPVB3) == 0:
            REPO_REPS = REPO_PREV
        elif len(REPO_PREV) == 0:
            REPO_REPS = RPVB3
        else:
            REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical", rechunk=True)
        
        logger.info(f"REPO combined data: {len(REPO_REPS)} records")
        write_parquet(REPO_REPS, repo_curr_path)
        
        # ==========================================
        # STEP 7: Create REPOWH dataset (deduplicated)
        # ==========================================
        logger.info("STEP 7: Creating REPOWH dataset")
        
        repowh_path = REPOWH_DIR / f"REPS_{REPTDT}.parquet"
        
        # SAS: PROC SORT NODUPKEY DATA=REPOWH.REPS&REPTDT; BY MNIACTNO;
        if len(REPO_REPS) > 0 and 'MNIACTNO' in REPO_REPS.columns:
            REPOWH_REPS = REPO_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
            duplicates_removed = len(REPO_REPS) - len(REPOWH_REPS)
            logger.info(f"Removed {duplicates_removed} duplicate records")
        else:
            REPOWH_REPS = REPO_REPS
        
        write_parquet(REPOWH_REPS, repowh_path)
        
        # ==========================================
        # SUMMARY
        # ==========================================
        logger.info("="*60)
        logger.info("PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"RPVB1: {len(RPVB1)} records")
        logger.info(f"RPVB2: {len(RPVB2)} records")
        logger.info(f"RPVB3: {len(RPVB3)} records")
        logger.info(f"REPO: {len(REPO_REPS)} records")
        logger.info(f"REPOWH: {len(REPOWH_REPS)} records")
        logger.info("="*60)
        
        return {
            'RPVB1': RPVB1,
            'RPVB2': RPVB2,
            'RPVB3': RPVB3,
            'REPO': REPO_REPS,
            'REPOWH': REPOWH_REPS
        }
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    results = main()
