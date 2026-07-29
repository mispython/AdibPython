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
CHUNK_SIZE = 50000  # Process 50,000 rows at a time
MAX_MEMORY_PERCENT = 75  # Maximum memory usage before forcing garbage collection

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
    level=logging.INFO,  # Changed to INFO to reduce log overhead
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
    memory_percent = psutil.Process(os.getpid()).memory_percent()
    if memory_percent > MAX_MEMORY_PERCENT:
        logger.warning(f"High memory usage ({memory_percent:.1f}%), forcing garbage collection")
        gc.collect()
        memory_percent = psutil.Process(os.getpid()).memory_percent()
        logger.info(f"Memory after GC: {memory_percent:.1f}%")
    return memory_percent

def log_memory_usage(stage=""):
    """Log current memory usage"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    memory_percent = process.memory_percent()
    logger.info(f"MEMORY [{stage}]: {memory_mb:.1f} MB ({memory_percent:.1f}%)")

# =====================================================
# SECTOR CODE FORMATS (Optimized with dictionaries)
# =====================================================

# Define format mappings as class attributes for faster lookup
# These should be populated from PBBLNFMT.py
SECTA_FORMAT = {}  # Populate with actual mappings
SECTB_FORMAT = {}  # Populate with actual mappings

def apply_sector_formats(sectorcd):
    """Apply both SECTA and SECTB formats and return valid codes"""
    results = []
    
    # Apply SECTA format
    secta = SECTA_FORMAT.get(sectorcd, str(sectorcd)) if SECTA_FORMAT else str(sectorcd)
    if secta and secta != ' ' and secta != 'nan':
        results.append(secta)
    
    # Apply SECTB format
    sectb = SECTB_FORMAT.get(sectorcd, str(sectorcd)) if SECTB_FORMAT else str(sectorcd)
    if sectb and sectb != ' ' and sectb != 'nan' and sectb != secta:  # Avoid duplicates
        results.append(sectb)
    
    return results

# =====================================================
# CHUNKED SAS7BDAT READER
# =====================================================

def read_sas7bdat_chunked(path: Path, columns=None, chunk_size=CHUNK_SIZE):
    """
    Read SAS7BDAT file in chunks to manage memory.
    Returns a generator yielding DataFrames.
    """
    logger.info(f"Reading SAS7BDAT in chunks: {path.name}")
    logger.info(f"Chunk size: {chunk_size:,} rows")
    
    try:
        # First, read just the header to get column info
        # Use iterator to read in chunks
        reader = pd.read_sas(path, format='sas7bdat', encoding='utf-8', 
                            chunksize=chunk_size, iterator=True)
        return reader
    except UnicodeDecodeError:
        logger.info("Trying latin1 encoding...")
        reader = pd.read_sas(path, format='sas7bdat', encoding='latin1',
                            chunksize=chunk_size, iterator=True)
        return reader
    except Exception as e:
        logger.error(f"Error reading SAS file {path}: {e}")
        raise

def read_sas7bdat_filtered(path: Path, columns=None, where_clause=None):
    """
    Read SAS7BDAT with column selection to reduce memory.
    Only read required columns.
    """
    logger.info(f"Reading SAS7BDAT filtered: {path.name}")
    
    try:
        if columns:
            df = pd.read_sas(path, format='sas7bdat', encoding='utf-8', columns=columns)
        else:
            df = pd.read_sas(path, format='sas7bdat', encoding='utf-8')
        
        logger.info(f"Read {len(df):,} rows with {len(df.columns)} columns")
        return df
    except UnicodeDecodeError:
        logger.info("Trying latin1 encoding...")
        if columns:
            df = pd.read_sas(path, format='sas7bdat', encoding='latin1', columns=columns)
        else:
            df = pd.read_sas(path, format='sas7bdat', encoding='latin1')
        logger.info(f"Read {len(df):,} rows with latin1 encoding")
        return df
    except Exception as e:
        logger.error(f"Error reading SAS file {path}: {e}")
        raise

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
# OPTIMIZED BUSINESS LOGIC WITH CHUNKING
# =====================================================

def process_loan_chunk(current_chunk, previous_df, valid_products):
    """
    Process a chunk of current loan data against previous period.
    Returns processed records to avoid keeping everything in memory.
    """
    # Filter for valid products
    chunk_filtered = current_chunk[current_chunk['PRODUCT'].isin(valid_products)].copy()
    
    if len(chunk_filtered) == 0:
        return []
    
    # Merge with previous period data
    merged = pd.merge(
        chunk_filtered,
        previous_df[['ACCTNO', 'NOTENO', 'SECTORCD', 'BALANCE']],
        on=['ACCTNO', 'NOTENO', 'SECTORCD'],
        how='left',
        suffixes=('', '_PREV')
    )
    
    # Calculate DISBURSE and REPAID using vectorized operations
    merged['DISBURSE'] = 0.0
    merged['REPAID'] = 0.0
    
    # Both periods exist: compare balances
    both_mask = merged['BALANCE_PREV'].notna()
    merged.loc[both_mask & (merged['BALANCE_PREV'] > merged['BALANCE']), 'REPAID'] = \
        merged['BALANCE_PREV'] - merged['BALANCE']
    merged.loc[both_mask & (merged['BALANCE_PREV'] <= merged['BALANCE']), 'DISBURSE'] = \
        merged['BALANCE'] - merged['BALANCE_PREV']
    
    # Only in current (new loans)
    new_mask = merged['BALANCE_PREV'].isna()
    merged.loc[new_mask, 'DISBURSE'] = merged.loc[new_mask, 'BALANCE']
    
    # Generate records with sector codes
    records = []
    for _, row in merged.iterrows():
        sector_codes = apply_sector_formats(row['SECTORCD'])
        for sectcd in sector_codes:
            records.append({
                'SECTCD': sectcd,
                'DISBURSE': row['DISBURSE'],
                'REPAID': row['REPAID'],
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'NOACCT': 1
            })
    
    # Find loans only in previous period (paid off)
    # This requires checking which previous loans are NOT in current chunk
    # We'll handle this separately after processing all chunks
    
    return records

def process_loan_data_optimized():
    """
    Main processing function with memory optimization.
    """
    log_memory_usage("START")
    
    valid_products = {131, 132, 720, 725}
    
    # Define required columns to minimize memory
    current_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE', 
                       'APPRLIM2', 'AMTIND', 'CUSTCD']
    previous_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
    uloan_columns = ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND']
    
    # Read previous period (usually smaller, can fit in memory)
    logger.info("Reading previous period loan data...")
    previous_df = read_sas7bdat_filtered(
        INPUT_DATASETS["LOAN_PREVIOUS"], 
        columns=previous_columns
    )
    previous_df = previous_df[previous_df['PRODUCT'].isin(valid_products)]
    logger.info(f"Previous period loans: {len(previous_df):,} rows")
    
    # Create index for faster merging
    previous_df.set_index(['ACCTNO', 'NOTENO', 'SECTORCD'], inplace=True)
    
    # Track which previous loans are matched
    previous_matched = set()
    
    # Process current period in chunks
    logger.info("Processing current period loan data in chunks...")
    all_records = []
    chunk_count = 0
    
    current_reader = read_sas7bdat_chunked(
        INPUT_DATASETS["LOAN_CURRENT"],
        chunk_size=CHUNK_SIZE
    )
    
    for chunk in current_reader:
        chunk_count += 1
        logger.info(f"Processing chunk {chunk_count}...")
        
        # Select only needed columns
        chunk = chunk[current_columns]
        
        # Track matched previous loans
        chunk_keys = set(zip(chunk['ACCTNO'], chunk['NOTENO'], chunk['SECTORCD']))
        previous_matched.update(chunk_keys)
        
        # Process chunk
        records = process_loan_chunk(chunk, previous_df.reset_index(), valid_products)
        all_records.extend(records)
        
        # Memory management
        del chunk, records
        if chunk_count % 5 == 0:  # Every 5 chunks
            check_memory_usage()
            log_memory_usage(f"CHUNK_{chunk_count}")
    
    # Process loans only in previous period (paid off)
    logger.info("Processing paid-off loans from previous period...")
    all_previous_keys = set(zip(previous_df.index.get_level_values(0),
                                previous_df.index.get_level_values(1),
                                previous_df.index.get_level_values(2)))
    paid_off_keys = all_previous_keys - previous_matched
    
    if paid_off_keys:
        paid_off_records = []
        for acctno, noteno, sectorcd in paid_off_keys:
            balance = previous_df.loc[(acctno, noteno, sectorcd), 'BALANCE']
            sector_codes = apply_sector_formats(sectorcd)
            for sectcd in sector_codes:
                paid_off_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': 0.0,
                    'REPAID': float(balance),
                    'APPRLIM2': 0.0,
                    'AMTIND': 'D',
                    'NOACCT': 0
                })
        all_records.extend(paid_off_records)
        logger.info(f"Added {len(paid_off_records)} paid-off records")
    
    # Free memory from previous_df
    del previous_df, previous_matched, paid_off_keys
    gc.collect()
    
    # Process ULOAN data (typically smaller)
    logger.info("Processing ULOAN data...")
    uloan_df = read_sas7bdat_filtered(
        INPUT_DATASETS["ULOAN"],
        columns=uloan_columns
    )
    
    for _, row in uloan_df.iterrows():
        sector_codes = apply_sector_formats(row['SECTORCD'])
        for sectcd in sector_codes:
            all_records.append({
                'SECTCD': sectcd,
                'DISBURSE': row.get('DISBURSE', 0),
                'REPAID': row.get('REPAID', 0),
                'APPRLIM2': row.get('APPRLIM2', 0),
                'AMTIND': row.get('AMTIND', 'D'),
                'NOACCT': 0
            })
    
    del uloan_df
    gc.collect()
    
    log_memory_usage("AFTER_DATA_LOAD")
    
    # Convert to DataFrame for aggregation
    logger.info(f"Converting {len(all_records):,} records to DataFrame...")
    df_all = pd.DataFrame(all_records)
    del all_records
    gc.collect()
    
    # Aggregate by SECTCD and AMTIND
    logger.info("Aggregating data...")
    summary = df_all.groupby(['SECTCD', 'AMTIND']).agg({
        'DISBURSE': 'sum',
        'REPAID': 'sum',
        'APPRLIM2': 'sum',
        'NOACCT': 'sum'
    }).reset_index()
    
    del df_all
    gc.collect()
    
    log_memory_usage("AFTER_AGGREGATION")
    
    # Generate BNM records
    logger.info("Generating BNM records...")
    output_records = generate_bnm_records(summary)
    
    del summary
    gc.collect()
    
    log_memory_usage("END")
    
    return output_records

def generate_bnm_records(summary_df):
    """
    Generate BNM output records from summary data.
    Uses dictionary accumulation instead of DataFrame for memory efficiency.
    """
    # Use dictionary for accumulation (more memory efficient than creating many DataFrames)
    bnm_dict = {}
    
    for _, row in summary_df.iterrows():
        sectcd = row['SECTCD']
        amtind = row['AMTIND']
        noacct = row['NOACCT']
        
        # 673400000 - DISBURSE
        code1 = f"673400000{sectcd}Y"
        key1 = (code1, amtind)
        bnm_dict[key1] = bnm_dict.get(key1, {'AMOUNT': 0, 'NOACCT': 0})
        bnm_dict[key1]['AMOUNT'] += row['DISBURSE']
        bnm_dict[key1]['NOACCT'] += noacct
        
        # 773400000 - REPAID
        code2 = f"773400000{sectcd}Y"
        key2 = (code2, amtind)
        bnm_dict[key2] = bnm_dict.get(key2, {'AMOUNT': 0, 'NOACCT': 0})
        bnm_dict[key2]['AMOUNT'] += row['REPAID']
        bnm_dict[key2]['NOACCT'] += (noacct if row['REPAID'] != 0 else 0)
        
        # 871500000 or 871510000 - APPRLIM2
        code3 = f"871500000{sectcd}Y" if sectcd == '0000' else f"871510000{sectcd}Y"
        key3 = (code3, amtind)
        bnm_dict[key3] = bnm_dict.get(key3, {'AMOUNT': 0, 'NOACCT': 0})
        bnm_dict[key3]['AMOUNT'] += row['APPRLIM2']
    
    # Aggregate by BNMCODE (combining D and I)
    final_dict = {}
    for (bnmcode, amtind), values in bnm_dict.items():
        if bnmcode not in final_dict:
            final_dict[bnmcode] = {'AMOUNTD': 0, 'AMOUNTI': 0, 'NOACCTD': 0, 'NOACCTI': 0}
        
        amount_k = round(values['AMOUNT'] / 1000)
        noacct_k = round(values['NOACCT'])
        
        if amtind == 'D':
            final_dict[bnmcode]['AMOUNTD'] += amount_k
            final_dict[bnmcode]['NOACCTD'] += noacct_k
        else:  # 'I'
            final_dict[bnmcode]['AMOUNTI'] += amount_k
            final_dict[bnmcode]['NOACCTI'] += noacct_k
    
    # Convert to output format
    output_records = []
    for bnmcode, values in final_dict.items():
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': values['AMOUNTD'] + values['AMOUNTI'],  # Combined
            'AMOUNTI': values['AMOUNTI'],
            'NOACCTD': values['NOACCTD'] + values['NOACCTI'],  # Combined
            'NOACCTI': values['NOACCTI']
        })
    
    return output_records

# =====================================================
# FILE WRITING (Buffered)
# =====================================================

def write_output_file(path: Path, records):
    """
    Write output records to file with buffering.
    """
    logger.info(f"Writing {len(records):,} records to output file...")
    
    buffer_size = 8192  # 8KB buffer
    
    try:
        with open(path, "w", encoding="utf-8", buffering=buffer_size) as f:
            # Write in batches to reduce I/O overhead
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                lines = []
                for record in batch:
                    line = f"{record['BNMCODE']};{record['AMOUNTD']};{record['AMOUNTI']};{record['NOACCTD']};{record['NOACCTI']}\n"
                    lines.append(line)
                f.writelines(lines)
                
                if i % 10000 == 0:  # Progress update every 10K records
                    logger.info(f"  Written {i:,} records...")
        
        file_size = path.stat().st_size / 1024 / 1024
        logger.info(f"Output file created: {path} ({file_size:.1f} MB)")
        
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        raise

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    """Main job execution with memory management"""
    logger.info(f"{'='*60}")
    logger.info(f"START JOB: {JOB_NAME}")
    logger.info(f"{'='*60}")
    
    try:
        log_memory_usage("JOB_START")
        
        logger.info(f"REPTDATE: {REPTDATE.strftime('%Y-%m-%d')}")
        logger.info(f"Period: {REPTMON}/WK{WK} vs {REPTMON1}/WK{WK1}")
        
        # Validate inputs
        logger.info("Validating input files...")
        for name, path in INPUT_DATASETS.items():
            if not path.exists():
                logger.error(f"Input file not found: {path}")
                # Try to find similar files
                if path.parent.exists():
                    similar = list(path.parent.glob(f"*.sas7bdat"))
                    logger.info(f"Available files: {[f.name for f in similar[:10]]}")
                return 8
        
        # Delete old output
        disp_delete(OUTPUT_DATASET)
        
        # Process data
        logger.info("Starting data processing...")
        records = process_loan_data_optimized()
        
        # Write output
        disp_new(OUTPUT_DATASET)
        write_output_file(OUTPUT_DATASET, records)
        
        logger.info(f"{'='*60}")
        logger.info(f"JOB COMPLETED SUCCESSFULLY: {len(records):,} records written")
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
        # Set process priority if needed (Unix)
        try:
            os.nice(10)  # Lower priority to not overwhelm server
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
