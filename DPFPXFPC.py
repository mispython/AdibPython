import sys
import logging
import pandas as pd
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import gc
import psutil
import os

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "XMIS" / "input" / "prod" / "EIBWHP04"
OUTPUT_DIR = BASE_DIR / "XMIS" / "output" / "EIBWHP04"

# Memory management settings
CHUNK_SIZE = 10000  # Reduced to 10,000 rows for safety
MAX_MEMORY_PERCENT = 60  # Lower threshold

# Calculate REPTDATE
CURRENT_DATE = datetime.now()
if CURRENT_DATE.day <= 8:
    REPTDATE = CURRENT_DATE.replace(day=8)
elif CURRENT_DATE.day <= 15:
    REPTDATE = CURRENT_DATE.replace(day=15)
elif CURRENT_DATE.day <= 22:
    REPTDATE = CURRENT_DATE.replace(day=22)
else:
    if CURRENT_DATE.month == 12:
        REPTDATE = CURRENT_DATE.replace(year=CURRENT_DATE.year, month=12, day=31)
    else:
        next_month = CURRENT_DATE.replace(month=CURRENT_DATE.month + 1, day=1)
        REPTDATE = next_month - timedelta(days=1)

# Determine week number
if REPTDATE.day == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif REPTDATE.day == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif REPTDATE.day == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1 = 23, '4', '3'

MM = REPTDATE.month
MM1 = MM - 1 if WK == '1' else MM
if MM1 == 0:
    MM1 = 12

SDATE = datetime(REPTDATE.year, MM, SDD)
REPTMON = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"

# Input files
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
except Exception as e:
    print(f"ERROR: Failed to create output directory: {e}")
    sys.exit(8)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =====================================================
# MEMORY MANAGEMENT
# =====================================================

def check_memory_usage():
    """Check current memory usage and force GC if needed"""
    try:
        process = psutil.Process(os.getpid())
        memory_percent = process.memory_percent()
        if memory_percent > MAX_MEMORY_PERCENT:
            logger.warning(f"High memory usage ({memory_percent:.1f}%), forcing GC")
            gc.collect()
            memory_percent = process.memory_percent()
            logger.info(f"Memory after GC: {memory_percent:.1f}%")
        return memory_percent
    except:
        return 0

def log_memory_usage(stage=""):
    """Log current memory usage"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        memory_percent = process.memory_percent()
        logger.info(f"MEMORY [{stage}]: {memory_mb:.1f} MB ({memory_percent:.1f}%)")
    except:
        logger.info(f"MEMORY [{stage}]: Unable to get memory info")

# =====================================================
# SECTOR CODE FORMATS
# =====================================================

# Define format mappings - populate from PBBLNFMT.py if available
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from PBBLNFMT import SECTA_FORMAT, SECTB_FORMAT
    logger.info("Loaded sector formats from PBBLNFMT.py")
except ImportError:
    logger.warning("PBBLNFMT.py not found, using default sector formats")
    SECTA_FORMAT = {}
    SECTB_FORMAT = {}

def apply_sector_formats(sectorcd):
    """Apply both SECTA and SECTB formats and return valid codes"""
    results = []
    
    # Convert sectorcd to string for lookup
    sectorcd_str = str(sectorcd).strip()
    
    # Apply SECTA format
    if SECTA_FORMAT:
        secta = SECTA_FORMAT.get(sectorcd_str, sectorcd_str)
    else:
        secta = sectorcd_str
    
    if secta and secta != ' ' and secta != 'nan' and secta != '':
        results.append(secta)
    
    # Apply SECTB format  
    if SECTB_FORMAT:
        sectb = SECTB_FORMAT.get(sectorcd_str, sectorcd_str)
    else:
        sectb = sectorcd_str
    
    if sectb and sectb != ' ' and sectb != 'nan' and sectb != '' and sectb != secta:
        results.append(sectb)
    
    # If no formats applied, use original
    if not results and sectorcd_str:
        results.append(sectorcd_str)
    
    return results

# =====================================================
# SAS7BDAT READER (Fixed - no columns parameter)
# =====================================================

def read_sas7bdat_chunked(path: Path, chunk_size=CHUNK_SIZE):
    """
    Read SAS7BDAT file in chunks.
    pd.read_sas doesn't support 'columns' parameter, so we read all columns
    but only keep what we need after reading.
    """
    logger.info(f"Reading SAS7BDAT in chunks: {path.name} (chunk size: {chunk_size:,})")
    
    try:
        reader = pd.read_sas(
            path, 
            format='sas7bdat', 
            encoding='utf-8',
            chunksize=chunk_size
        )
        return reader
    except UnicodeDecodeError:
        logger.info("UTF-8 failed, trying latin1 encoding...")
        reader = pd.read_sas(
            path, 
            format='sas7bdat', 
            encoding='latin1',
            chunksize=chunk_size
        )
        return reader
    except Exception as e:
        logger.error(f"Error reading SAS file {path}: {e}")
        raise

def read_sas7bdat_full(path: Path, keep_columns=None):
    """
    Read entire SAS7BDAT file.
    If keep_columns is specified, only those columns are retained after reading.
    """
    logger.info(f"Reading SAS7BDAT: {path.name}")
    
    try:
        df = pd.read_sas(path, format='sas7bdat', encoding='utf-8')
    except UnicodeDecodeError:
        logger.info("UTF-8 failed, trying latin1 encoding...")
        df = pd.read_sas(path, format='sas7bdat', encoding='latin1')
    
    logger.info(f"Read {len(df):,} rows, {len(df.columns)} columns")
    
    # Select only needed columns to save memory
    if keep_columns:
        # Only keep columns that actually exist in the dataframe
        available_columns = [col for col in keep_columns if col in df.columns]
        missing_columns = [col for col in keep_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
        df = df[available_columns].copy()
        logger.info(f"Kept {len(available_columns)} columns: {available_columns}")
    
    return df

# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(path: Path):
    if path.exists():
        path.unlink()
        logger.info(f"Deleted: {path}")

def disp_shr(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"DISP=SHR failed: {path}")
    logger.info(f"Validated SHR: {path}")

def disp_new(path: Path):
    if path.exists():
        raise FileExistsError(f"DISP=NEW failed: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Validated NEW: {path}")

# =====================================================
# OPTIMIZED BUSINESS LOGIC
# =====================================================

def process_loan_data_optimized():
    """
    Main processing function with memory optimization.
    """
    log_memory_usage("START")
    
    valid_products = {131, 132, 720, 725}
    
    # Define required columns
    current_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE', 
                       'APPRLIM2', 'AMTIND', 'CUSTCD']
    previous_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
    uloan_columns = ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND']
    
    # Step 1: Read previous period data
    logger.info("="*40)
    logger.info("STEP 1: Reading previous period loan data...")
    previous_df = read_sas7bdat_full(
        INPUT_DATASETS["LOAN_PREVIOUS"], 
        keep_columns=previous_columns
    )
    
    # Filter for valid products
    previous_df = previous_df[previous_df['PRODUCT'].isin(valid_products)].copy()
    logger.info(f"Previous period loans after filter: {len(previous_df):,} rows")
    
    # Create lookup set for faster matching
    previous_keys = set()
    previous_balances = {}
    
    for _, row in previous_df.iterrows():
        key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
        previous_keys.add(key)
        previous_balances[key] = float(row['BALANCE'])
    
    # Free memory
    del previous_df
    gc.collect()
    log_memory_usage("AFTER_PREVIOUS_LOAD")
    
    # Step 2: Process current period in chunks
    logger.info("="*40)
    logger.info("STEP 2: Processing current period loan data...")
    
    # Track matched previous loans
    previous_matched = set()
    all_records = []
    chunk_count = 0
    
    current_reader = read_sas7bdat_chunked(INPUT_DATASETS["LOAN_CURRENT"])
    
    for chunk in current_reader:
        chunk_count += 1
        logger.info(f"Processing chunk {chunk_count} ({len(chunk):,} rows)...")
        
        # Select only needed columns
        available_cols = [col for col in current_columns if col in chunk.columns]
        chunk = chunk[available_cols].copy()
        
        # Filter for valid products
        if 'PRODUCT' in chunk.columns:
            chunk = chunk[chunk['PRODUCT'].isin(valid_products)]
        
        if len(chunk) == 0:
            logger.info(f"  Chunk {chunk_count}: No valid records after filter")
            del chunk
            continue
        
        # Process each row in chunk
        for _, row in chunk.iterrows():
            key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
            previous_matched.add(key)
            
            current_balance = float(row.get('BALANCE', 0))
            previous_balance = previous_balances.get(key, None)
            apprlim2 = float(row.get('APPRLIM2', 0))
            amtind = str(row.get('AMTIND', 'D'))
            
            # Calculate DISBURSE and REPAID
            if previous_balance is not None:
                # Exists in both periods
                if previous_balance > current_balance:
                    disburse = 0.0
                    repaid = previous_balance - current_balance
                else:
                    disburse = current_balance - previous_balance
                    repaid = 0.0
            else:
                # New loan
                disburse = current_balance
                repaid = 0.0
            
            # Generate records with sector codes
            sector_codes = apply_sector_formats(row.get('SECTORCD', ''))
            for sectcd in sector_codes:
                all_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': disburse,
                    'REPAID': repaid,
                    'APPRLIM2': apprlim2,
                    'AMTIND': amtind,
                    'NOACCT': 1
                })
        
        # Memory management
        del chunk
        if chunk_count % 10 == 0:
            logger.info(f"  Processed {chunk_count} chunks, {len(all_records):,} records so far")
            check_memory_usage()
            log_memory_usage(f"CHUNK_{chunk_count}")
    
    logger.info(f"Total chunks processed: {chunk_count}")
    log_memory_usage("AFTER_CURRENT_PROCESSING")
    
    # Step 3: Process paid-off loans (only in previous period)
    logger.info("="*40)
    logger.info("STEP 3: Processing paid-off loans...")
    
    paid_off_count = 0
    for key in previous_keys:
        if key not in previous_matched:
            balance = previous_balances[key]
            sectorcd = key[2]  # SECTORCD is third element of key
            
            sector_codes = apply_sector_formats(sectorcd)
            for sectcd in sector_codes:
                all_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': 0.0,
                    'REPAID': balance,
                    'APPRLIM2': 0.0,
                    'AMTIND': 'D',
                    'NOACCT': 0
                })
                paid_off_count += 1
    
    logger.info(f"Added {paid_off_count:,} paid-off records")
    
    # Free memory
    del previous_keys, previous_balances, previous_matched
    gc.collect()
    log_memory_usage("AFTER_PAID_OFF")
    
    # Step 4: Process ULOAN data
    logger.info("="*40)
    logger.info("STEP 4: Processing ULOAN data...")
    
    uloan_df = read_sas7bdat_full(
        INPUT_DATASETS["ULOAN"],
        keep_columns=uloan_columns
    )
    
    uloan_count = 0
    for _, row in uloan_df.iterrows():
        sector_codes = apply_sector_formats(row.get('SECTORCD', ''))
        for sectcd in sector_codes:
            all_records.append({
                'SECTCD': sectcd,
                'DISBURSE': float(row.get('DISBURSE', 0)),
                'REPAID': float(row.get('REPAID', 0)),
                'APPRLIM2': float(row.get('APPRLIM2', 0)),
                'AMTIND': str(row.get('AMTIND', 'D')),
                'NOACCT': 0
            })
            uloan_count += 1
    
    logger.info(f"Added {uloan_count:,} ULOAN records")
    del uloan_df
    gc.collect()
    log_memory_usage("AFTER_ULOAN")
    
    # Step 5: Aggregate data
    logger.info("="*40)
    logger.info("STEP 5: Aggregating data...")
    logger.info(f"Total records to aggregate: {len(all_records):,}")
    
    # Use dictionary for aggregation (more memory efficient)
    summary_dict = {}
    
    for record in all_records:
        key = (record['SECTCD'], record['AMTIND'])
        if key not in summary_dict:
            summary_dict[key] = {
                'DISBURSE': 0.0,
                'REPAID': 0.0,
                'APPRLIM2': 0.0,
                'NOACCT': 0
            }
        summary_dict[key]['DISBURSE'] += record['DISBURSE']
        summary_dict[key]['REPAID'] += record['REPAID']
        summary_dict[key]['APPRLIM2'] += record['APPRLIM2']
        summary_dict[key]['NOACCT'] += record['NOACCT']
    
    # Free records list
    del all_records
    gc.collect()
    log_memory_usage("AFTER_AGGREGATION")
    
    # Step 6: Generate BNM records
    logger.info("="*40)
    logger.info("STEP 6: Generating BNM records...")
    
    bnm_dict = {}
    
    for (sectcd, amtind), values in summary_dict.items():
        disburse = values['DISBURSE']
        repaid = values['REPAID']
        apprlim2 = values['APPRLIM2']
        noacct = values['NOACCT']
        
        # 673400000 - DISBURSE
        code1 = f"673400000{sectcd}Y"
        key1 = (code1, amtind)
        if key1 not in bnm_dict:
            bnm_dict[key1] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key1]['AMOUNT'] += disburse
        bnm_dict[key1]['NOACCT'] += noacct
        
        # 773400000 - REPAID
        code2 = f"773400000{sectcd}Y"
        key2 = (code2, amtind)
        if key2 not in bnm_dict:
            bnm_dict[key2] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key2]['AMOUNT'] += repaid
        bnm_dict[key2]['NOACCT'] += (noacct if repaid != 0 else 0)
        
        # 871500000 or 871510000 - APPRLIM2
        if sectcd == '0000':
            code3 = f"871500000{sectcd}Y"
        else:
            code3 = f"871510000{sectcd}Y"
        key3 = (code3, amtind)
        if key3 not in bnm_dict:
            bnm_dict[key3] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key3]['AMOUNT'] += apprlim2
    
    # Aggregate by BNMCODE (combining D and I amounts)
    final_dict = {}
    for (bnmcode, amtind), values in bnm_dict.items():
        if bnmcode not in final_dict:
            final_dict[bnmcode] = {
                'AMOUNTD': 0, 'AMOUNTI': 0,
                'NOACCTD': 0, 'NOACCTI': 0
            }
        
        # Convert to thousands (divide by 1000 and round)
        amount_k = int(round(values['AMOUNT'] / 1000))
        noacct_k = int(round(values['NOACCT']))
        
        if amtind == 'D':
            final_dict[bnmcode]['AMOUNTD'] += amount_k
            final_dict[bnmcode]['NOACCTD'] += noacct_k
        else:  # 'I' (Islamic)
            final_dict[bnmcode]['AMOUNTI'] += amount_k
            final_dict[bnmcode]['NOACCTI'] += noacct_k
    
    # Convert to output format (matching SAS logic)
    output_records = []
    for bnmcode, values in final_dict.items():
        # SAS combines D+I for AMOUNTD and NOACCTD
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': values['AMOUNTD'] + values['AMOUNTI'],  # Combined total
            'AMOUNTI': values['AMOUNTI'],                      # Islamic only
            'NOACCTD': values['NOACCTD'] + values['NOACCTI'],  # Combined total
            'NOACCTI': values['NOACCTI']                       # Islamic only
        })
    
    logger.info(f"Generated {len(output_records):,} BNM output records")
    
    del summary_dict, bnm_dict, final_dict
    gc.collect()
    log_memory_usage("END")
    
    return output_records

# =====================================================
# FILE WRITING
# =====================================================

def write_output_file(path: Path, records):
    """Write output records to file with buffering"""
    logger.info(f"Writing {len(records):,} records to output file...")
    
    try:
        with open(path, "w", encoding="utf-8", buffering=65536) as f:  # 64KB buffer
            # Write in batches
            batch_size = 5000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                lines = []
                for record in batch:
                    # Format: BNMCODE;AMOUNTD;AMOUNTI;NOACCTD;NOACCTI
                    line = f"{record['BNMCODE']};{record['AMOUNTD']};{record['AMOUNTI']};{record['NOACCTD']};{record['NOACCTI']}\n"
                    lines.append(line)
                f.writelines(lines)
                
                if (i + batch_size) % 50000 == 0:
                    logger.info(f"  Written {i + batch_size:,} records...")
        
        file_size = path.stat().st_size / 1024 / 1024
        logger.info(f"Output file created: {path} ({file_size:.1f} MB)")
        
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        raise

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    """Main job execution"""
    logger.info(f"{'='*60}")
    logger.info(f"START JOB: {JOB_NAME}")
    logger.info(f"{'='*60}")
    
    try:
        log_memory_usage("JOB_START")
        
        logger.info(f"REPTDATE: {REPTDATE.strftime('%Y-%m-%d')}")
        logger.info(f"Period: Month={REPTMON}/Week={WK} vs Month={REPTMON1}/Week={WK1}")
        
        # Validate inputs
        logger.info("Validating input files...")
        for name, path in INPUT_DATASETS.items():
            if not path.exists():
                logger.error(f"Input file not found: {path}")
                if path.parent.exists():
                    similar = list(path.parent.glob("*.sas7bdat"))
                    logger.info(f"Available files in {path.parent}:")
                    for f in similar[:20]:
                        logger.info(f"  - {f.name}")
                return 8
            else:
                file_size_mb = path.stat().st_size / 1024 / 1024
                logger.info(f"  {name}: {path.name} ({file_size_mb:.1f} MB)")
        
        # Delete old output if exists
        disp_delete(OUTPUT_DATASET)
        
        # Process data
        logger.info("Starting data processing...")
        records = process_loan_data_optimized()
        
        if not records:
            logger.warning("No records generated!")
            return 4
        
        # Write output
        disp_new(OUTPUT_DATASET)
        write_output_file(OUTPUT_DATASET, records)
        
        logger.info(f"{'='*60}")
        logger.info(f"JOB COMPLETED SUCCESSFULLY")
        logger.info(f"Total records written: {len(records):,}")
        logger.info(f"{'='*60}")
        
        return 0
        
    except MemoryError as e:
        logger.error(f"OUT OF MEMORY: {e}")
        log_memory_usage("MEMORY_ERROR")
        return 8
        
    except Exception as e:
        logger.error(f"JOB FAILED: {type(e).__name__}: {e}")
        logger.debug(traceback.format_exc())
        return 8

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        # Lower process priority to avoid overwhelming server
        try:
            os.nice(10)
        except:
            pass
        
        exit_code = run_job()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("Job interrupted by user")
        sys.exit(8)
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        sys.exit(8)
