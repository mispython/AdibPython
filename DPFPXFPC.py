import sys
import logging
import pandas as pd
import pyreadstat
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
CHUNK_SIZE = 10000
MAX_MEMORY_PERCENT = 60

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

def log_memory_usage(stage=""):
    """Log current memory usage"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        memory_percent = process.memory_percent()
        logger.info(f"MEMORY [{stage}]: {memory_mb:.1f} MB ({memory_percent:.1f}%)")
    except:
        pass

def check_memory():
    """Check memory and force GC if needed"""
    try:
        process = psutil.Process(os.getpid())
        if process.memory_percent() > MAX_MEMORY_PERCENT:
            logger.warning(f"High memory ({process.memory_percent():.1f}%), forcing GC")
            gc.collect()
    except:
        pass

# =====================================================
# SECTOR CODE FORMATS
# =====================================================

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
    sectorcd_str = str(sectorcd).strip()
    
    if SECTA_FORMAT:
        secta = SECTA_FORMAT.get(sectorcd_str, sectorcd_str)
    else:
        secta = sectorcd_str
    
    if secta and secta != ' ' and secta != 'nan' and secta != '':
        results.append(secta)
    
    if SECTB_FORMAT:
        sectb = SECTB_FORMAT.get(sectorcd_str, sectorcd_str)
    else:
        sectb = sectorcd_str
    
    if sectb and sectb != ' ' and sectb != 'nan' and sectb != '' and sectb != secta:
        results.append(sectb)
    
    if not results and sectorcd_str:
        results.append(sectorcd_str)
    
    return results

# =====================================================
# PYREADSTAT READER - SIMPLIFIED
# =====================================================

def read_sas_chunked(file_path, columns, chunk_size=CHUNK_SIZE):
    """
    Read SAS file in chunks using pyreadstat.
    Returns a generator yielding DataFrames.
    """
    file_str = str(file_path)
    file_size_mb = file_path.stat().st_size / 1024 / 1024
    logger.info(f"Reading in chunks: {file_path.name} ({file_size_mb:.1f} MB)")
    logger.info(f"  Columns: {columns}")
    logger.info(f"  Chunk size: {chunk_size:,} rows")
    
    try:
        # Use pyreadstat's native chunked reading
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            file_str,
            usecols=columns,
            encoding='latin1',
            chunksize=chunk_size,
            dates_as_pandas_datetime=True
        )
        
        chunk_num = 0
        for df, meta in reader:
            chunk_num += 1
            logger.info(f"  Chunk {chunk_num}: {len(df):,} rows, {len(df.columns)} columns")
            
            yield df
            
            # Memory management every 10 chunks
            if chunk_num % 10 == 0:
                log_memory_usage(f"CHUNK_{chunk_num}")
                check_memory()
        
        logger.info(f"  Total chunks read: {chunk_num}")
        
    except pyreadstat.ReadstatError as e:
        logger.error(f"pyreadstat error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading chunks: {e}")
        logger.debug(traceback.format_exc())
        raise

def read_sas_full(file_path, columns):
    """
    Read entire SAS file into DataFrame using pyreadstat.
    """
    file_str = str(file_path)
    file_size_mb = file_path.stat().st_size / 1024 / 1024
    logger.info(f"Reading full file: {file_path.name} ({file_size_mb:.1f} MB)")
    logger.info(f"  Columns: {columns}")
    
    try:
        df, meta = pyreadstat.read_sas7bdat(
            file_str,
            usecols=columns,
            encoding='latin1',
            dates_as_pandas_datetime=True
        )
        
        logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")
        
        # Log column info safely
        try:
            if hasattr(meta, 'column_names'):
                logger.debug(f"  Column names: {meta.column_names[:10]}")
            if hasattr(meta, 'variable_types'):
                logger.debug(f"  Variable types available")
        except:
            pass
        
        return df
        
    except pyreadstat.ReadstatError as e:
        logger.error(f"pyreadstat error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        logger.debug(traceback.format_exc())
        raise

# =====================================================
# OPTIMIZED BUSINESS LOGIC
# =====================================================

def process_loan_data_optimized():
    """Main processing function."""
    log_memory_usage("START")
    
    valid_products = {131, 132, 720, 725}
    
    # Define required columns
    previous_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
    current_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE', 
                       'APPRLIM2', 'AMTIND', 'CUSTCD']
    uloan_columns = ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND']
    
    # ============================================
    # STEP 1: Read previous period data (full read - small file)
    # ============================================
    logger.info("="*60)
    logger.info("STEP 1: Reading previous period loan data...")
    
    previous_df = read_sas_full(
        INPUT_DATASETS["LOAN_PREVIOUS"],
        columns=previous_columns
    )
    
    # Filter for valid products
    previous_df = previous_df[previous_df['PRODUCT'].isin(valid_products)].copy()
    logger.info(f"  Previous period loans after filter: {len(previous_df):,} rows")
    
    # Create lookup dictionaries
    previous_balances = {}
    for _, row in previous_df.iterrows():
        key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
        previous_balances[key] = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
    
    previous_keys = set(previous_balances.keys())
    
    del previous_df
    gc.collect()
    log_memory_usage("AFTER_PREVIOUS_LOAD")
    
    # ============================================
    # STEP 2: Process current period in chunks (large 3GB file)
    # ============================================
    logger.info("="*60)
    logger.info("STEP 2: Processing current period loan data in chunks...")
    
    previous_matched = set()
    all_records = []
    chunk_count = 0
    total_rows = 0
    total_filtered = 0
    
    for chunk in read_sas_chunked(
        INPUT_DATASETS["LOAN_CURRENT"],
        columns=current_columns,
        chunk_size=CHUNK_SIZE
    ):
        chunk_count += 1
        total_rows += len(chunk)
        
        # Filter for valid products
        if 'PRODUCT' in chunk.columns:
            chunk = chunk[chunk['PRODUCT'].isin(valid_products)]
        
        total_filtered += len(chunk)
        
        if len(chunk) == 0:
            continue
        
        # Process chunk
        chunk_records = []
        
        for _, row in chunk.iterrows():
            try:
                acctno = str(row['ACCTNO'])
                noteno = str(row['NOTENO'])
                sectorcd = str(row['SECTORCD'])
                key = (acctno, noteno, sectorcd)
                previous_matched.add(key)
                
                # Safe value extraction
                current_balance = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
                previous_balance = previous_balances.get(key)
                apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
                amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
                
                # Calculate DISBURSE and REPAID
                if previous_balance is not None:
                    if previous_balance > current_balance:
                        disburse = 0.0
                        repaid = previous_balance - current_balance
                    else:
                        disburse = current_balance - previous_balance
                        repaid = 0.0
                else:
                    disburse = current_balance
                    repaid = 0.0
                
                # Skip if all zeros
                if disburse == 0.0 and repaid == 0.0 and apprlim2 == 0.0:
                    continue
                
                # Generate records with sector codes
                sector_codes = apply_sector_formats(sectorcd)
                for sectcd in sector_codes:
                    chunk_records.append({
                        'SECTCD': sectcd,
                        'DISBURSE': disburse,
                        'REPAID': repaid,
                        'APPRLIM2': apprlim2,
                        'AMTIND': amtind,
                        'NOACCT': 1
                    })
                    
            except Exception as e:
                logger.debug(f"Error processing row: {e}")
                continue
        
        all_records.extend(chunk_records)
        del chunk_records
        
        # Progress update every 20 chunks
        if chunk_count % 20 == 0:
            logger.info(f"  Progress: {chunk_count} chunks, {total_rows:,} rows, "
                       f"{total_filtered:,} filtered, {len(all_records):,} records")
            log_memory_usage(f"CHUNK_{chunk_count}")
            check_memory()
    
    logger.info(f"  Completed: {chunk_count} chunks, {total_rows:,} total rows, "
                f"{total_filtered:,} filtered, {len(all_records):,} records")
    log_memory_usage("AFTER_CURRENT")
    
    # ============================================
    # STEP 3: Process paid-off loans
    # ============================================
    logger.info("="*60)
    logger.info("STEP 3: Processing paid-off loans...")
    
    paid_off_keys = previous_keys - previous_matched
    paid_off_count = 0
    
    for key in paid_off_keys:
        balance = previous_balances[key]
        sectorcd = key[2]
        
        if balance == 0.0:
            continue
        
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
    
    logger.info(f"  Added {paid_off_count:,} paid-off records")
    
    del previous_keys, previous_balances, previous_matched, paid_off_keys
    gc.collect()
    log_memory_usage("AFTER_PAID_OFF")
    
    # ============================================
    # STEP 4: Process ULOAN data (full read - small file)
    # ============================================
    logger.info("="*60)
    logger.info("STEP 4: Processing ULOAN data...")
    
    uloan_df = read_sas_full(
        INPUT_DATASETS["ULOAN"],
        columns=uloan_columns
    )
    
    uloan_count = 0
    for _, row in uloan_df.iterrows():
        try:
            sectorcd = str(row['SECTORCD']) if pd.notna(row['SECTORCD']) else ''
            disburse = float(row['DISBURSE']) if pd.notna(row.get('DISBURSE')) else 0.0
            repaid = float(row['REPAID']) if pd.notna(row.get('REPAID')) else 0.0
            apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
            amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
            
            sector_codes = apply_sector_formats(sectorcd)
            for sectcd in sector_codes:
                all_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': disburse,
                    'REPAID': repaid,
                    'APPRLIM2': apprlim2,
                    'AMTIND': amtind,
                    'NOACCT': 0
                })
                uloan_count += 1
        except Exception as e:
            logger.debug(f"Error processing ULOAN row: {e}")
            continue
    
    logger.info(f"  Added {uloan_count:,} ULOAN records")
    del uloan_df
    gc.collect()
    log_memory_usage("AFTER_ULOAN")
    
    # ============================================
    # STEP 5: Aggregate data
    # ============================================
    logger.info("="*60)
    logger.info(f"STEP 5: Aggregating {len(all_records):,} records...")
    
    summary_dict = {}
    for record in all_records:
        key = (record['SECTCD'], record['AMTIND'])
        if key not in summary_dict:
            summary_dict[key] = {'DISBURSE': 0.0, 'REPAID': 0.0, 'APPRLIM2': 0.0, 'NOACCT': 0}
        summary_dict[key]['DISBURSE'] += record['DISBURSE']
        summary_dict[key]['REPAID'] += record['REPAID']
        summary_dict[key]['APPRLIM2'] += record['APPRLIM2']
        summary_dict[key]['NOACCT'] += record['NOACCT']
    
    logger.info(f"  Aggregated into {len(summary_dict):,} groups")
    
    del all_records
    gc.collect()
    log_memory_usage("AFTER_AGGREGATION")
    
    # ============================================
    # STEP 6: Generate BNM records
    # ============================================
    logger.info("="*60)
    logger.info("STEP 6: Generating BNM records...")
    
    bnm_dict = {}
    for (sectcd, amtind), values in summary_dict.items():
        disburse = values['DISBURSE']
        repaid = values['REPAID']
        apprlim2 = values['APPRLIM2']
        noacct = values['NOACCT']
        
        # 673400000 - DISBURSE
        key1 = (f"673400000{sectcd}Y", amtind)
        if key1 not in bnm_dict:
            bnm_dict[key1] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key1]['AMOUNT'] += disburse
        bnm_dict[key1]['NOACCT'] += noacct
        
        # 773400000 - REPAID
        key2 = (f"773400000{sectcd}Y", amtind)
        if key2 not in bnm_dict:
            bnm_dict[key2] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key2]['AMOUNT'] += repaid
        bnm_dict[key2]['NOACCT'] += (noacct if repaid != 0 else 0)
        
        # 8715X0000 - APPRLIM2
        prefix = "871500000" if sectcd == '0000' else "871510000"
        key3 = (f"{prefix}{sectcd}Y", amtind)
        if key3 not in bnm_dict:
            bnm_dict[key3] = {'AMOUNT': 0.0, 'NOACCT': 0}
        bnm_dict[key3]['AMOUNT'] += apprlim2
    
    # Combine D and I
    final_dict = {}
    for (bnmcode, amtind), values in bnm_dict.items():
        if bnmcode not in final_dict:
            final_dict[bnmcode] = {'AMOUNTD': 0, 'AMOUNTI': 0, 'NOACCTD': 0, 'NOACCTI': 0}
        
        amount_k = int(round(values['AMOUNT'] / 1000))
        noacct_k = int(round(values['NOACCT']))
        
        if amtind == 'D':
            final_dict[bnmcode]['AMOUNTD'] += amount_k
            final_dict[bnmcode]['NOACCTD'] += noacct_k
        else:  # 'I' for Islamic
            final_dict[bnmcode]['AMOUNTI'] += amount_k
            final_dict[bnmcode]['NOACCTI'] += noacct_k
    
    # Format output records
    output_records = []
    for bnmcode, values in final_dict.items():
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': values['AMOUNTD'] + values['AMOUNTI'],  # Combined D+I
            'AMOUNTI': values['AMOUNTI'],
            'NOACCTD': values['NOACCTD'] + values['NOACCTI'],  # Combined D+I
            'NOACCTI': values['NOACCTI']
        })
    
    logger.info(f"  Generated {len(output_records):,} BNM output records")
    
    del summary_dict, bnm_dict, final_dict
    gc.collect()
    log_memory_usage("END")
    
    return output_records

# =====================================================
# FILE WRITING
# =====================================================

def write_output_file(path, records):
    """Write output records in SAS PUT format"""
    logger.info(f"Writing {len(records):,} records to output...")
    
    with open(path, 'w', encoding='utf-8', buffering=131072) as f:
        batch_size = 10000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            lines = [
                f"{r['BNMCODE']};{r['AMOUNTD']};{r['AMOUNTI']};{r['NOACCTD']};{r['NOACCTI']}\n"
                for r in batch
            ]
            f.writelines(lines)
            
            if (i + batch_size) % 100000 == 0:
                logger.info(f"  Written {i + batch_size:,} records...")
    
    file_size = path.stat().st_size / 1024 / 1024
    logger.info(f"  Output file: {path.name} ({file_size:.1f} MB)")

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    """Main job execution"""
    logger.info("="*60)
    logger.info(f"START JOB: {JOB_NAME}")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
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
                    logger.info(f"Available files: {[f.name for f in similar[:20]]}")
                return 8
            else:
                size_mb = path.stat().st_size / 1024 / 1024
                logger.info(f"  {name}: {path.name} ({size_mb:.1f} MB)")
        
        # Delete old output
        if OUTPUT_DATASET.exists():
            OUTPUT_DATASET.unlink()
            logger.info("Deleted old output file")
        
        # Process data
        start_time = datetime.now()
        records = process_loan_data_optimized()
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Processing completed in {elapsed:.1f} seconds")
        
        if not records:
            logger.warning("No records generated!")
            return 4
        
        # Write output
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        write_output_file(OUTPUT_DATASET, records)
        
        logger.info("="*60)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info(f"Total records: {len(records):,}")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info("="*60)
        
        return 0
        
    except MemoryError as e:
        logger.error(f"OUT OF MEMORY: {e}")
        log_memory_usage("MEMORY_ERROR")
        return 8
        
    except pyreadstat.ReadstatError as e:
        logger.error(f"PYREADSTAT ERROR: {e}")
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
        os.nice(10)
    except:
        pass
    
    try:
        exit_code = run_job()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("Job interrupted by user")
        sys.exit(8)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        sys.exit(8)
