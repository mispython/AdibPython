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
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(df.to_arrow(), path)
        logger.info(f"Successfully wrote {len(df)} records to {path}")
    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")
        raise

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

def mmyy_format(d: date) -> str:
    """Format date as MMYY (e.g., March 2025 -> 0325)"""
    return f"{d.month:02d}{d.year % 100:02d}"

def safe_int(value: str) -> Optional[int]:
    """Safely convert string to int, handling invalid values"""
    try:
        if value and value.strip() and value.strip().replace('.', '').isdigit():
            return int(float(value.strip()))
        return None
    except (ValueError, TypeError):
        return None

# =========================
# DATE EXTRACTION
# =========================
def extract_rpvb_date() -> str:
    """Extract date from RPVBDATA.txt header (record type '0')"""
    with open(RPVBDATA_PATH, 'r') as f:
        first_line = f.readline().strip()
    if not first_line.startswith('0'):
        raise ValueError("RPVBDATA.txt: Expected header record '0' not found")
    return first_line.split()[1]  # Format: "0 20251201"

def extract_srs_date() -> str:
    """Extract date from SRSDATA.txt first line"""
    with open(SRSDATA_PATH, 'r') as f:
        first_line = f.readline().strip()
    if len(first_line) < 8:
        raise ValueError("SRSDATA.txt: First line too short for date extraction")
    return first_line[:8]  # First 8 characters should be YYYYMMDD

# =========================
# DATA PARSING - FIXED WIDTH BASED ON ACTUAL FILE STRUCTURE
# =========================
def parse_rpvdata() -> pl.DataFrame:
    """
    Parse RPVBDATA.txt using fixed-width parsing based on actual file structure
    Based on the debug output showing record structure
    """
    records = []
    
    with open(RPVBDATA_PATH, 'r') as f:
        lines = f.readlines()
    
    logger.info(f"Total lines in file: {len(lines)}")
    
    for line_num, line in enumerate(lines, start=1):
        line = line.rstrip('\n')
        if not line.strip():
            continue
            
        if line.startswith('0'):
            continue  # Skip header
            
        if line.startswith('1'):
            # Based on debug output: 
            # '1 8042194628 90010      36 FROZEN MART SDN. BHD.                           R   700                0 N          R 20250326     750.00 5SNE         0.00       0.00       0.00   54000.00 20250329                       20250327                0.00'
            # 
            # This is space-delimited but with varying spaces
            # Let's use split() but be more careful with the data
            
            parts = line.split()
            
            # Skip if too few parts
            if len(parts) < 12:
                continue
            
            try:
                # Extract the date from position 16 (if available) or 27
                # Based on the debug output, the date is at position 27 in the split
                date_str = ""
                if len(parts) > 27:
                    date_str = parts[27]  # DATEWOFF
                
                # Extract the date components more carefully
                # The date in the debug output is at position 27: "20250326"
                date_components = date_str if date_str and len(date_str) >= 8 else ""
                
                record = {
                    'MNIACTNO': parts[1] if len(parts) > 1 else '',
                    'BRANCHNO': parts[2] if len(parts) > 2 else '',
                    'NAME': ' '.join(parts[3:8]) if len(parts) > 8 else '',
                    'ACCTSTA': parts[8] if len(parts) > 8 else '',
                    'PRSTCOND': parts[9] if len(parts) > 9 else '',
                    'REGCARD': parts[10] if len(parts) > 10 else '',
                    'IGNTKEY': parts[11] if len(parts) > 11 else '',
                    'ACCTWOFF': parts[12] if len(parts) > 12 else '',
                    'MODEREPO': parts[13] if len(parts) > 13 else '',
                    'REPOSTAT': parts[14] if len(parts) > 14 else '',
                    'MODEDISP': parts[15] if len(parts) > 15 else '',
                    # DATEWOFF is at position 27 in the split
                    'DATEWOFF': date_components,
                    # Extract YY, MM, DD from DATEWOFF
                    'YY1': date_components[:4] if len(date_components) >= 8 else None,
                    'MM1': date_components[4:6] if len(date_components) >= 8 else None,
                    'DD1': date_components[6:8] if len(date_components) >= 8 else None,
                }
                records.append(record)
            except Exception as e:
                logger.debug(f"Error parsing line {line_num}: {e}")
                continue
    
    logger.info(f"Parsed {len(records)} records from {RPVBDATA_PATH}")
    return pl.DataFrame(records)

def process_rpvb_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply $UPCASE transformations and create date fields
    """
    if len(df) == 0:
        logger.warning("Empty DataFrame passed to process_rpvb_data")
        return df
    
    # Apply uppercase to string columns
    uppercase_cols = [
        'NAME', 'ACCTSTA', 'PRSTCOND', 'REGCARD', 
        'IGNTKEY', 'ACCTWOFF', 'MODEREPO', 'REPOSTAT', 'MODEDISP'
    ]
    
    for col in uppercase_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_uppercase())
    
    # Create date field from components - using safe conversion
    if all(col in df.columns for col in ['YY1', 'MM1', 'DD1']):
        # First convert to proper integers
        df = df.with_columns([
            pl.col('YY1').map_elements(lambda x: safe_int(x), return_dtype=pl.Int32),
            pl.col('MM1').map_elements(lambda x: safe_int(x), return_dtype=pl.Int32),
            pl.col('DD1').map_elements(lambda x: safe_int(x), return_dtype=pl.Int32),
        ])
        
        # Then create date
        df = df.with_columns([
            pl.when(
                pl.any_horizontal([
                    pl.col("MM1").is_null(), 
                    pl.col("DD1").is_null(), 
                    pl.col("YY1").is_null()
                ])
            )
            .then(pl.lit(None))
            .otherwise(
                pl.datetime(
                    pl.col("YY1"),
                    pl.col("MM1"), 
                    pl.col("DD1")
                ).cast(pl.Date)
            )
            .alias("DATEWOFF_DATE")
        ]).drop(["YY1", "MM1", "DD1"])
    
    return df

# =========================
# PREVIOUS DATA LOADING
# =========================
def load_previous_data(prevdt: str, expected_schema: dict) -> pl.DataFrame:
    """Load previous month's data with schema alignment"""
    path = REPO_DIR / f"REPS_{prevdt}.parquet"
    
    if not path.exists():
        logger.info(f"No previous data found at {path}")
        return pl.DataFrame(schema=expected_schema) if expected_schema else pl.DataFrame()
    
    try:
        df = pl.read_parquet(path)
        logger.info(f"Loaded {len(df)} records from {path}")
        
        # Align schema to match expected
        if expected_schema:
            for col_name, col_type in expected_schema.items():
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    df = df.with_columns(pl.col(col_name).cast(col_type))
            
            # Reorder columns to match expected
            df = df.select(list(expected_schema.keys()))
        
        return df
    except Exception as e:
        logger.warning(f"Error loading previous data: {e}")
        return pl.DataFrame(schema=expected_schema) if expected_schema else pl.DataFrame()

# =========================
# MAIN PROCESSING
# =========================
def main():
    """Main processing pipeline"""
    try:
        logger.info("="*60)
        logger.info("Starting REPO Processing Pipeline")
        logger.info("="*60)
        
        # ==========================================
        # STEP 1: Extract dates
        # ==========================================
        logger.info("STEP 1: Extracting dates from input files")
        rpvb_date_str = extract_rpvb_date()
        srs_date_str = extract_srs_date()
        
        logger.info(f"RPVBDATA date: {rpvb_date_str}")
        logger.info(f"SRSDATA date: {srs_date_str}")
        
        # ==========================================
        # STEP 2: Calculate report dates
        # ==========================================
        logger.info("STEP 2: Calculating report dates")
        tb_date = yyyymmdd_to_date(rpvb_date_str)
        if tb_date is None:
            raise ValueError(f"Invalid RPVB date: {rpvb_date_str}")
        
        # REPTDATE = end of previous month from TBDATE
        first_of_month = date(tb_date.year, tb_date.month, 1)
        REPTDATE = end_of_month(first_of_month - timedelta(days=1))
        PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))
        
        REPTDT = mmyy_format(REPTDATE)
        PREVDT = mmyy_format(PREVDATE)
        
        # SRSDATE (direct from SRSDATA file)
        srs_date = yyyymmdd_to_date(srs_date_str)
        if srs_date is None:
            raise ValueError(f"Invalid SRS date: {srs_date_str}")
        SRSTDT = mmyy_format(srs_date)
        
        logger.info(f"REPTDATE: {REPTDATE} ({REPTDT})")
        logger.info(f"PREVDATE: {PREVDATE} ({PREVDT})")
        logger.info(f"SRSDATE: {srs_date} ({SRSTDT})")
        
        # ==========================================
        # STEP 3: Validate dates (macro guard)
        # ==========================================
        logger.info("STEP 3: Validating dates")
        if REPTDT != SRSTDT:
            error_msg = f"Date mismatch: REPTDT={REPTDT} vs SRSTDT={SRSTDT}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        logger.info("✓ Date validation passed")
        
        # ==========================================
        # STEP 4: Parse and process RPVB data
        # ==========================================
        logger.info("STEP 4: Parsing RPVB data")
        raw_data = parse_rpvdata()
        logger.info(f"Raw data: {len(raw_data)} records")
        
        logger.info("STEP 5: Processing RPVB data (UPCASE + dates)")
        RPVB1 = process_rpvb_data(raw_data)
        logger.info(f"RPVB1: {len(RPVB1)} records")
        
        # ==========================================
        # STEP 6: Apply filters
        # ==========================================
        logger.info("STEP 6: Applying filters")
        
        # RPVB2: ACCTSTA in ('D', 'S', 'R')
        if len(RPVB1) > 0:
            RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
            logger.info(f"RPVB2 (ACCTSTA in D,S,R): {len(RPVB2)} records")
        else:
            RPVB2 = RPVB1
        
        # RPVB3: DATEWOFF_DATE is not null
        if len(RPVB2) > 0:
            RPVB3 = RPVB2.filter(pl.col("DATEWOFF_DATE").is_not_null())
            logger.info(f"RPVB3 (with DATEWOFF): {len(RPVB3)} records")
        else:
            RPVB3 = RPVB2
        
        # ==========================================
        # STEP 7: Create REPO dataset
        # ==========================================
        logger.info("STEP 7: Creating REPO dataset")
        logger.info(f"Previous month: {PREVDT}")
        logger.info(f"Current month: {REPTDT}")
        
        # Load previous data
        prev_schema = RPVB3.schema if len(RPVB3) > 0 else None
        REPO_PREV = load_previous_data(PREVDT, prev_schema)
        logger.info(f"Previous data: {len(REPO_PREV)} records")
        
        # Combine current and previous data
        if len(RPVB3) == 0 and len(REPO_PREV) == 0:
            logger.warning("No data to process")
            REPO_REPS = pl.DataFrame()
        elif len(RPVB3) == 0:
            REPO_REPS = REPO_PREV
        elif len(REPO_PREV) == 0:
            REPO_REPS = RPVB3
        else:
            REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical", rechunk=True)
        
        logger.info(f"Combined REPO data: {len(REPO_REPS)} records")
        
        # Save REPO
        repo_path = REPO_DIR / f"REPS_{REPTDT}.parquet"
        write_parquet(REPO_REPS, repo_path)
        
        # ==========================================
        # STEP 8: Create REPOWH dataset (deduplicated)
        # ==========================================
        logger.info("STEP 8: Creating REPOWH dataset (deduplicated)")
        
        if len(REPO_REPS) > 0 and 'MNIACTNO' in REPO_REPS.columns:
            original_count = len(REPO_REPS)
            REPOWH_REPS = REPO_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
            duplicates_removed = original_count - len(REPOWH_REPS)
            logger.info(f"Removed {duplicates_removed} duplicate records")
        else:
            REPOWH_REPS = REPO_REPS
        
        # Save REPOWH
        repowh_path = REPOWH_DIR / f"REPS_{REPTDT}.parquet"
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
