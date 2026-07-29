import sys
import logging
import pandas as pd
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(__file__).parent.parent  # Goes from XMIS to MIS
INPUT_DIR = BASE_DIR / "XMIS" / "input" / "prod" / "EIBWHP04"
OUTPUT_DIR = BASE_DIR / "XMIS" / "output" / "EIBWHP04"

# Calculate REPTDATE based on current date
CURRENT_DATE = datetime.now()
# Find the most recent reporting date (8th, 15th, 22nd, or last day of month)
if CURRENT_DATE.day <= 8:
    REPTDATE = CURRENT_DATE.replace(day=8)
elif CURRENT_DATE.day <= 15:
    REPTDATE = CURRENT_DATE.replace(day=15)
elif CURRENT_DATE.day <= 22:
    REPTDATE = CURRENT_DATE.replace(day=22)
else:
    # Last day of current month
    if CURRENT_DATE.month == 12:
        REPTDATE = CURRENT_DATE.replace(year=CURRENT_DATE.year, month=12, day=31)
    else:
        next_month = CURRENT_DATE.replace(month=CURRENT_DATE.month + 1, day=1)
        REPTDATE = next_month - timedelta(days=1)

# Determine week number based on REPTDATE day
if REPTDATE.day == 8:
    SDD = 1
    WK = '1'
    WK1 = '4'
elif REPTDATE.day == 15:
    SDD = 9
    WK = '2'
    WK1 = '1'
elif REPTDATE.day == 22:
    SDD = 16
    WK = '3'
    WK1 = '2'
else:  # Last day of month
    SDD = 23
    WK = '4'
    WK1 = '3'

MM = REPTDATE.month
# Calculate MM1 (previous month for WK1)
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

# Calculate SDATE
SDATE = datetime(REPTDATE.year, MM, SDD)

# Format strings for file naming
REPTMON = f"{MM:02d}"  # 2-digit month
REPTMON1 = f"{MM1:02d}"  # 2-digit previous month
REPTYEAR = f"{REPTDATE.year}"  # 4-digit year
REPTDAY = f"{REPTDATE.day:02d}"  # 2-digit day
RDATE = REPTDATE.strftime("%d%m%Y")  # DDMMYYYY format
SDATE_STR = SDATE.strftime("%d%m%Y")

# Input files with correct SAS naming convention
INPUT_DATASETS = {
    "LOAN_CURRENT": INPUT_DIR / f"loan{REPTMON}{WK}.sas7bdat",
    "LOAN_PREVIOUS": INPUT_DIR / f"loan{REPTMON1}{WK1}.sas7bdat",
    "ULOAN": INPUT_DIR / f"uloan{REPTMON}{WK}.sas7bdat"
}

OUTPUT_DATASET = OUTPUT_DIR / f"EIBWHP04_{REPTDATE.strftime('%Y%m%d')}.txt"
LOG_FILE = OUTPUT_DIR / f"{JOB_NAME}_{REPTDATE.strftime('%Y%m%d')}.log"

# =====================================================
# LOGGING SETUP
# =====================================================

try:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"DEBUG: Created/verified output directory: {OUTPUT_DIR}")
except Exception as e:
    print(f"ERROR: Failed to create output directory: {e}")
    sys.exit(8)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =====================================================
# SECTOR CODE FORMATS (from PBBLNFMT)
# =====================================================

# These would normally come from PBBLNFMT.py format definitions
# Example format definitions - adjust based on actual PBBLNFMT formats
SECTA_FORMAT = {
    # Add actual sector code mappings here
    # Example: '01': '1000', '02': '2000', etc.
}

SECTB_FORMAT = {
    # Add actual sector code mappings here
}

def format_secta(sectorcd):
    """Apply $SECTA format"""
    # This should match the SAS format $SECTA
    # Placeholder - replace with actual format logic from PBBLNFMT
    return str(sectorcd) if sectorcd else ' '

def format_sectb(sectorcd):
    """Apply $SECTB format"""
    # This should match the SAS format $SECTB
    # Placeholder - replace with actual format logic from PBBLNFMT
    return str(sectorcd) if sectorcd else ' '

# =====================================================
# DEBUG INFORMATION
# =====================================================

def print_debug_info():
    """Print debug information about the environment and configuration"""
    logger.debug("=" * 60)
    logger.debug("DEBUG INFORMATION:")
    logger.debug(f"Python version: {sys.version}")
    logger.debug(f"Current working directory: {Path.cwd()}")
    logger.debug(f"Script location: {Path(__file__).absolute()}")
    logger.debug(f"BASE_DIR: {BASE_DIR.absolute()}")
    logger.debug(f"INPUT_DIR: {INPUT_DIR.absolute()}")
    logger.debug(f"OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.debug(f"REPTDATE: {REPTDATE}")
    logger.debug(f"REPTMON: {REPTMON}, WK: {WK}")
    logger.debug(f"REPTMON1: {REPTMON1}, WK1: {WK1}")
    logger.debug(f"SDATE: {SDATE}")
    logger.debug(f"RDATE: {RDATE}")
    logger.debug(f"SDATE_STR: {SDATE_STR}")
    logger.debug(f"LOG_FILE: {LOG_FILE}")
    logger.debug(f"OUTPUT_DATASET: {OUTPUT_DATASET}")
    
    logger.debug(f"INPUT_DIR exists: {INPUT_DIR.exists()}")
    if INPUT_DIR.exists():
        files = list(INPUT_DIR.glob("*"))
        logger.debug(f"Files in INPUT_DIR ({len(files)}):")
        for f in files[:20]:  # Show first 20 files
            logger.debug(f"  {f.name} ({f.stat().st_size} bytes)")
    
    logger.debug("Input datasets:")
    for name, path in INPUT_DATASETS.items():
        logger.debug(f"  {name}: {path}")
        logger.debug(f"    Absolute: {path.absolute()}")
        logger.debug(f"    Exists: {path.exists()}")
        if path.exists():
            logger.debug(f"    Size: {path.stat().st_size} bytes")
    logger.debug("=" * 60)

# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(path: Path):
    """Delete dataset if exists"""
    logger.debug(f"DISP DELETE - Attempting to delete: {path}")
    if path.exists():
        try:
            path.unlink()
            logger.info(f"Deleted dataset: {path}")
        except Exception as e:
            logger.error(f"Error deleting {path}: {e}")
            raise

def disp_shr(path: Path):
    """Validate SHR dataset exists"""
    logger.debug(f"DISP SHR - Validating: {path}")
    if not path.parent.exists():
        error_msg = f"Parent directory does not exist: {path.parent}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    if not path.exists():
        files_in_dir = list(path.parent.glob("*"))
        logger.debug(f"Files in directory ({len(files_in_dir)}):")
        for f in files_in_dir[:10]:
            logger.debug(f"  {f.name}")
        error_msg = f"DISP=SHR failed - File not found: {path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    logger.info(f"Validated SHR dataset: {path}")

def disp_new(path: Path):
    """Validate NEW dataset doesn't exist"""
    logger.debug(f"DISP NEW - Validating: {path}")
    if path.exists():
        error_msg = f"DISP=NEW failed - File already exists: {path}"
        logger.error(error_msg)
        raise FileExistsError(error_msg)
    
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Validated NEW dataset: {path}")

# =====================================================
# SAS7BDAT READER
# =====================================================

def read_sas7bdat(path: Path):
    """Read SAS7BDAT file and return pandas DataFrame"""
    logger.debug(f"READ SAS7BDAT - Starting to read: {path}")
    
    try:
        df = pd.read_sas(path, format='sas7bdat', encoding='utf-8')
        logger.debug(f"Successfully read {len(df)} rows, columns: {df.columns.tolist()}")
        return df
    except UnicodeDecodeError:
        logger.debug("Attempting with latin1 encoding...")
        df = pd.read_sas(path, format='sas7bdat', encoding='latin1')
        logger.debug(f"Successfully read with latin1 encoding")
        return df
    except Exception as e:
        logger.error(f"Error reading SAS file {path}: {e}")
        raise

# =====================================================
# TEXT FILE WRITER (semicolon-separated, matching SAS PUT)
# =====================================================

def write_text_file(path: Path, records):
    """Write records to text file in SAS PUT format (semicolon-separated, no spaces)"""
    logger.debug(f"WRITE TEXT FILE - Starting to write: {path}")
    
    try:
        with open(path, "w", encoding="utf-8") as f:
            for i, record in enumerate(records):
                # Format: BNMCODE;AMOUNTD;AMOUNTI;NOACCTD;NOACCTI
                # No spaces after semicolons (matching SAS +(-1) format)
                line = f"{record['BNMCODE']};{record['AMOUNTD']};{record['AMOUNTI']};{record['NOACCTD']};{record['NOACCTI']}"
                f.write(line + "\n")
                
                if i < 5:  # Debug first 5 records
                    logger.debug(f"  Record {i}: {line}")
        
        logger.info(f"Text file created: {path} ({len(records)} records)")
    except Exception as e:
        logger.error(f"Error writing text file {path}: {e}")
        raise

# =====================================================
# BUSINESS LOGIC (SAS translation)
# =====================================================

def execute_business_logic():
    """
    Execute EIBWHP04 business logic - translated from SAS code.
    Process loan data and generate BNM reporting records.
    """
    logger.debug("EXECUTE BUSINESS LOGIC - Starting")
    
    # Read input datasets
    loan_current = read_sas7bdat(INPUT_DATASETS["LOAN_CURRENT"])
    loan_previous = read_sas7bdat(INPUT_DATASETS["LOAN_PREVIOUS"])
    uloan = read_sas7bdat(INPUT_DATASETS["ULOAN"])
    
    # Filter for products 131, 132, 720, 725
    valid_products = [131, 132, 720, 725]
    
    # Prepare ALW1 (previous period)
    alw1 = loan_previous[loan_previous['PRODUCT'].isin(valid_products)].copy()
    alw1_cols = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 
                 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE']
    alw1 = alw1[alw1_cols].rename(columns={'BALANCE': 'LASTBAL', 'NOTETERM': 'LASTNOTE'})
    
    # Prepare ALW (current period)
    alw = loan_current[loan_current['PRODUCT'].isin(valid_products)].copy()
    alw_cols = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 
                'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 
                'CUSTCD', 'AMTIND', 'ISSDTE']
    alw = alw[alw_cols]
    
    # Merge ALW1 and ALW (similar to SAS MERGE BY ACCTNO NOTENO SECTORCD)
    merged = pd.merge(alw, alw1[['ACCTNO', 'NOTENO', 'SECTORCD', 'LASTBAL', 'LASTNOTE']], 
                      on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='outer', indicator=True)
    
    # Calculate DISBURSE and REPAID
    merged['NOACCT'] = 1
    merged['DISBURSE'] = 0.0
    merged['REPAID'] = 0.0
    
    # Both periods exist
    both_mask = merged['_merge'] == 'both'
    merged.loc[both_mask & (merged['LASTBAL'] > merged['BALANCE']), 'REPAID'] = \
        merged['LASTBAL'] - merged['BALANCE']
    merged.loc[both_mask & (merged['LASTBAL'] <= merged['BALANCE']), 'DISBURSE'] = \
        merged['BALANCE'] - merged['LASTBAL']
    
    # Only in previous (loan paid off)
    left_mask = merged['_merge'] == 'left_only'
    merged.loc[left_mask, 'REPAID'] = merged.loc[left_mask, 'LASTBAL']
    
    # Only in current (new loan)
    right_mask = merged['_merge'] == 'right_only'
    merged.loc[right_mask, 'DISBURSE'] = merged.loc[right_mask, 'BALANCE']
    
    # Apply sector code formats and create duplicate records
    records_alw = []
    for _, row in merged.iterrows():
        # Apply SECTA format
        sectcd_a = format_secta(row['SECTORCD'])
        if sectcd_a and sectcd_a != ' ':
            record_a = {
                'SECTCD': sectcd_a,
                'DISBURSE': row['DISBURSE'],
                'REPAID': row['REPAID'],
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'CUSTCD': row.get('CUSTCD', ''),
                'NOACCT': row['NOACCT']
            }
            records_alw.append(record_a)
        
        # Apply SECTB format
        sectcd_b = format_sectb(row['SECTORCD'])
        if sectcd_b and sectcd_b != ' ':
            record_b = {
                'SECTCD': sectcd_b,
                'DISBURSE': row['DISBURSE'],
                'REPAID': row['REPAID'],
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'CUSTCD': row.get('CUSTCD', ''),
                'NOACCT': row['NOACCT']
            }
            records_alw.append(record_b)
    
    # Process ULOAN data
    records_ualw = []
    for _, row in uloan.iterrows():
        sectcd_a = format_secta(row['SECTORCD'])
        if sectcd_a and sectcd_a != ' ':
            records_ualw.append({
                'SECTCD': sectcd_a,
                'DISBURSE': row.get('DISBURSE', 0),
                'REPAID': row.get('REPAID', 0),
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'CUSTCD': row.get('CUSTCD', '')
            })
        
        sectcd_b = format_sectb(row['SECTORCD'])
        if sectcd_b and sectcd_b != ' ':
            records_ualw.append({
                'SECTCD': sectcd_b,
                'DISBURSE': row.get('DISBURSE', 0),
                'REPAID': row.get('REPAID', 0),
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'CUSTCD': row.get('CUSTCD', '')
            })
    
    # Combine ALW and UALW records
    all_records = records_alw + records_ualw
    df_all = pd.DataFrame(all_records)
    
    # Summary by SECTCD and AMTIND (PROC SUMMARY equivalent)
    summary = df_all.groupby(['SECTCD', 'AMTIND']).agg({
        'DISBURSE': 'sum',
        'REPAID': 'sum',
        'APPRLIM2': 'sum'
    }).reset_index()
    
    if 'NOACCT' in df_all.columns:
        noacct_summary = df_all.groupby(['SECTCD', 'AMTIND'])['NOACCT'].sum().reset_index()
        summary = pd.merge(summary, noacct_summary, on=['SECTCD', 'AMTIND'])
    else:
        summary['NOACCT'] = 0
    
    # Create BNM records (DATA ALWLOAN step)
    bnm_records = []
    for _, row in summary.iterrows():
        sectcd = row['SECTCD']
        
        # 673400000 + SECTCD + Y - DISBURSE
        bnm_records.append({
            'BNMCODE': f"673400000{sectcd}Y",
            'AMTIND': row['AMTIND'],
            'AMOUNT': row['DISBURSE'],
            'NOACCT': row.get('NOACCT', 0)
        })
        
        # 773400000 + SECTCD + Y - REPAID
        noacct_repay = row.get('NOACCT', 0) if row['REPAID'] != 0 else 0
        bnm_records.append({
            'BNMCODE': f"773400000{sectcd}Y",
            'AMTIND': row['AMTIND'],
            'AMOUNT': row['REPAID'],
            'NOACCT': noacct_repay
        })
        
        # 871500000/871510000 + SECTCD + Y - APPRLIM2
        if sectcd == '0000':
            bnmcode = f"871500000{sectcd}Y"
        else:
            bnmcode = f"871510000{sectcd}Y"
        
        bnm_records.append({
            'BNMCODE': bnmcode,
            'AMTIND': row['AMTIND'],
            'AMOUNT': row['APPRLIM2'],
            'NOACCT': 0
        })
    
    # Convert to DataFrame and summarize by BNMCODE and AMTIND
    df_bnm = pd.DataFrame(bnm_records)
    final_summary = df_bnm.groupby(['BNMCODE', 'AMTIND']).agg({
        'AMOUNT': 'sum',
        'NOACCT': 'sum'
    }).reset_index()
    
    # Format final output (DATA _NULL_ step)
    output_records = []
    # Process by BNMCODE groups
    for bnmcode, group in final_summary.groupby('BNMCODE'):
        amountd = 0
        amounti = 0
        noacctd = 0
        noaccti = 0
        
        for _, row in group.iterrows():
            amount_k = round(row['AMOUNT'] / 1000)
            noacct = round(row['NOACCT'])
            
            if row['AMTIND'] == 'D':
                amountd += amount_k
                noacctd += noacct
            elif row['AMTIND'] == 'I':
                amounti += amount_k
                noaccti += noacct
        
        # Combine D and I
        amountd_total = amountd + amounti
        noacctd_total = noacctd + noaccti
        
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': amountd_total,
            'AMOUNTI': amounti,
            'NOACCTD': noacctd_total,
            'NOACCTI': noaccti
        })
    
    logger.info(f"Generated {len(output_records)} BNM output records")
    return output_records

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    """Main job execution"""
    logger.info(f"========== START JOB {JOB_NAME} ==========")
    
    try:
        print_debug_info()
        
        logger.info(f"REPTDATE: {REPTDATE.strftime('%Y-%m-%d')}")
        logger.info(f"REPTMON: {REPTMON}, WK: {WK}")
        logger.info(f"REPTMON1: {REPTMON1}, WK1: {WK1}")
        logger.info(f"SDATE: {SDATE.strftime('%Y-%m-%d')}")
        
        # DELETE STEP
        disp_delete(OUTPUT_DATASET)
        
        # SHR VALIDATION
        for name, path in INPUT_DATASETS.items():
            disp_shr(path)
        
        # NEW VALIDATION
        disp_new(OUTPUT_DATASET)
        
        # EXECUTE LOGIC
        records = execute_business_logic()
        
        # WRITE OUTPUT
        write_text_file(OUTPUT_DATASET, records)
        
        logger.info(f"========== END JOB {JOB_NAME} SUCCESSFULLY ==========")
        return 0
        
    except Exception as e:
        logger.error(f"JOB FAILED: {type(e).__name__}: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        exit_code = run_job()
        sys.exit(exit_code)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        sys.exit(8)
