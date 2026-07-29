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
# SECTOR CODE FORMATS (from PBBLNFMT)
# =====================================================

try:
    sys.path.insert(0, str(Path(__file__).parent))
    from PBBLNFMT import SECTA_FORMAT, SECTB_FORMAT
    logger.info("Loaded sector formats from PBBLNFMT.py")
except ImportError:
    logger.warning("PBBLNFMT.py not found, using default sector formats")
    # These format mappings need to match your SAS formats
    SECTA_FORMAT = {
        # Example: '1000': '1000', etc.
    }
    SECTB_FORMAT = {
        # Example: '1000': '1000', etc.
    }

def apply_sector_formats(sectorcd):
    """
    Apply SECTA and SECTB formats.
    Returns list of formatted sector codes (SAS logic: output if not blank).
    """
    results = []
    sectorcd_str = str(sectorcd).strip()
    
    if not sectorcd_str or sectorcd_str == 'nan':
        return results
    
    # Apply SECTA format
    if SECTA_FORMAT:
        secta = SECTA_FORMAT.get(sectorcd_str, '')
    else:
        secta = sectorcd_str
    
    if secta and secta.strip() and secta != 'nan':
        results.append(secta)
    
    # Apply SECTB format  
    if SECTB_FORMAT:
        sectb = SECTB_FORMAT.get(sectorcd_str, '')
    else:
        sectb = sectorcd_str
    
    if sectb and sectb.strip() and sectb != 'nan' and sectb != secta:
        results.append(sectb)
    
    # If no format applied, use original
    if not results:
        results.append(sectorcd_str)
    
    return results

# =====================================================
# PYREADSTAT READER
# =====================================================

def read_sas_chunked(file_path, columns, chunk_size=CHUNK_SIZE):
    """Read SAS file in chunks using pyreadstat."""
    file_str = str(file_path)
    file_size_mb = file_path.stat().st_size / 1024 / 1024
    logger.info(f"Reading in chunks: {file_path.name} ({file_size_mb:.1f} MB)")
    
    try:
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
            if chunk_num % 20 == 0:
                logger.info(f"  Chunk {chunk_num}: {len(df):,} rows")
            yield df
        
        logger.info(f"  Total chunks: {chunk_num}")
        
    except Exception as e:
        logger.error(f"Error reading chunks: {e}")
        raise

def read_sas_full(file_path, columns):
    """Read entire SAS file into DataFrame."""
    file_str = str(file_path)
    file_size_mb = file_path.stat().st_size / 1024 / 1024
    logger.info(f"Reading full file: {file_path.name} ({file_size_mb:.1f} MB)")
    
    try:
        df, meta = pyreadstat.read_sas7bdat(
            file_str,
            usecols=columns,
            encoding='latin1',
            dates_as_pandas_datetime=True
        )
        logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise

# =====================================================
# BUSINESS LOGIC - MATCHING SAS CODE
# =====================================================

def process_loan_data():
    """Process loan data matching SAS logic exactly."""
    log_memory_usage("START")
    
    valid_products = {131, 132, 720, 725}
    
    previous_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
    current_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE', 
                       'APPRLIM2', 'AMTIND', 'CUSTCD']
    uloan_columns = ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND']
    
    # ============================================
    # STEP 1: Read previous period
    # ============================================
    logger.info("="*60)
    logger.info("STEP 1: Reading previous period loan data...")
    
    previous_df = read_sas_full(
        INPUT_DATASETS["LOAN_PREVIOUS"],
        columns=previous_columns
    )
    
    # Filter for valid products (WHERE PRODUCT IN (131,132,720,725))
    previous_df = previous_df[previous_df['PRODUCT'].isin(valid_products)].copy()
    logger.info(f"  Previous period loans: {len(previous_df):,} rows")
    
    # Create lookup: key -> balance (simulating ALW1 dataset)
    previous_balances = {}
    for _, row in previous_df.iterrows():
        key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
        previous_balances[key] = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
    
    previous_keys = set(previous_balances.keys())
    del previous_df
    gc.collect()
    log_memory_usage("AFTER_PREVIOUS")
    
    # ============================================
    # STEP 2: Process current period (ALW)
    # ============================================
    logger.info("="*60)
    logger.info("STEP 2: Processing current period in chunks...")
    
    previous_matched = set()
    alw_records = []  # Matches DATA ALW output
    
    chunk_count = 0
    for chunk in read_sas_chunked(
        INPUT_DATASETS["LOAN_CURRENT"],
        columns=current_columns,
        chunk_size=CHUNK_SIZE
    ):
        chunk_count += 1
        
        # Filter for valid products
        chunk = chunk[chunk['PRODUCT'].isin(valid_products)]
        if len(chunk) == 0:
            continue
        
        for _, row in chunk.iterrows():
            try:
                key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
                previous_matched.add(key)
                
                # Get balances
                current_balance = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
                previous_balance = previous_balances.get(key)
                apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
                amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
                
                # SAS LOGIC:
                # IF A & B THEN DO;
                #   IF LASTBAL > BALANCE THEN REPAID = LASTBAL - BALANCE;
                #   ELSE DISBURSE = BALANCE - LASTBAL;
                # END;
                # IF ^B THEN REPAID = LASTBAL;  (only in previous - handled separately)
                # IF ^A THEN DISBURSE = BALANCE; (only in current)
                
                if previous_balance is not None:
                    # Both periods exist (A & B)
                    if previous_balance > current_balance:
                        disburse = 0.0
                        repaid = previous_balance - current_balance
                    else:
                        disburse = current_balance - previous_balance
                        repaid = 0.0
                else:
                    # Only in current (^A)
                    disburse = current_balance
                    repaid = 0.0
                
                # NOACCT = 1 (from SAS code)
                noacct = 1
                
                # Apply sector formats - creates multiple records if both SECTA and SECTB apply
                sector_codes = apply_sector_formats(str(row['SECTORCD']))
                
                for sectcd in sector_codes:
                    if sectcd and sectcd.strip() and sectcd != 'nan':
                        alw_records.append({
                            'SECTCD': sectcd,
                            'DISBURSE': disburse,
                            'REPAID': repaid,
                            'APPRLIM2': apprlim2,
                            'AMTIND': amtind,
                            'NOACCT': noacct
                        })
                        
            except Exception as e:
                logger.debug(f"Error processing row: {e}")
                continue
        
        if chunk_count % 20 == 0:
            logger.info(f"  Chunks: {chunk_count}, Records: {len(alw_records):,}")
            check_memory()
    
    logger.info(f"  Total records from ALW: {len(alw_records):,}")
    
    # ============================================
    # STEP 3: Paid-off loans (IF ^B THEN REPAID = LASTBAL)
    # ============================================
    logger.info("="*60)
    logger.info("STEP 3: Processing paid-off loans...")
    
    paid_off_keys = previous_keys - previous_matched
    for key in paid_off_keys:
        balance = previous_balances[key]
        if balance == 0.0:
            continue
        
        sectorcd = key[2]
        sector_codes = apply_sector_formats(sectorcd)
        
        for sectcd in sector_codes:
            if sectcd and sectcd.strip() and sectcd != 'nan':
                alw_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': 0.0,
                    'REPAID': balance,
                    'APPRLIM2': 0.0,
                    'AMTIND': 'D',
                    'NOACCT': 0  # No NOACCT for repaid loans
                })
    
    logger.info(f"  Added {len(paid_off_keys):,} paid-off loans")
    
    del previous_keys, previous_balances, previous_matched
    gc.collect()
    
    # ============================================
    # STEP 4: Process ULOAN data
    # ============================================
    logger.info("="*60)
    logger.info("STEP 4: Processing ULOAN data...")
    
    uloan_df = read_sas_full(
        INPUT_DATASETS["ULOAN"],
        columns=uloan_columns
    )
    
    for _, row in uloan_df.iterrows():
        try:
            sectorcd = str(row['SECTORCD']) if pd.notna(row['SECTORCD']) else ''
            disburse = float(row['DISBURSE']) if pd.notna(row.get('DISBURSE')) else 0.0
            repaid = float(row['REPAID']) if pd.notna(row.get('REPAID')) else 0.0
            apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
            amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
            
            sector_codes = apply_sector_formats(sectorcd)
            
            for sectcd in sector_codes:
                if sectcd and sectcd.strip() and sectcd != 'nan':
                    alw_records.append({
                        'SECTCD': sectcd,
                        'DISBURSE': disburse,
                        'REPAID': repaid,
                        'APPRLIM2': apprlim2,
                        'AMTIND': amtind,
                        'NOACCT': 0  # ULOAN has no NOACCT
                    })
        except Exception as e:
            logger.debug(f"Error processing ULOAN: {e}")
            continue
    
    logger.info(f"  Total ULOAN records: {len(uloan_df):,}")
    del uloan_df
    gc.collect()
    
    # ============================================
    # STEP 5: PROC SUMMARY - Aggregate by SECTCD and AMTIND
    # ============================================
    logger.info("="*60)
    logger.info(f"STEP 5: Aggregating {len(alw_records):,} records...")
    
    # SAS: PROC SUMMARY DATA=ALW NWAY; CLASS SECTCD AMTIND; VAR DISBURSE REPAID APPRLIM2 NOACCT;
    summary = {}
    for rec in alw_records:
        key = (rec['SECTCD'], rec['AMTIND'])
        if key not in summary:
            summary[key] = {'DISBURSE': 0.0, 'REPAID': 0.0, 'APPRLIM2': 0.0, 'NOACCT': 0}
        summary[key]['DISBURSE'] += rec['DISBURSE']
        summary[key]['REPAID'] += rec['REPAID']
        summary[key]['APPRLIM2'] += rec['APPRLIM2']
        summary[key]['NOACCT'] += rec['NOACCT']
    
    logger.info(f"  Aggregated into {len(summary):,} groups")
    del alw_records
    gc.collect()
    
    # ============================================
    # STEP 6: DATA ALWLOAN - Generate BNM codes
    # ============================================
    logger.info("="*60)
    logger.info("STEP 6: Generating BNM records...")
    
    # SAS: DATA ALWLOAN; SET ALWLOAN;
    # BNMCODE='673400000'||SECTCD||'Y'; AMOUNT=DISBURSE; OUTPUT;
    # BNMCODE='773400000'||SECTCD||'Y'; AMOUNT=REPAID; IF REPAID=0 THEN NOACCT=0; OUTPUT;
    # IF SECTCD='0000' THEN BNMCODE='871500000'||SECTCD||'Y';
    #                 ELSE BNMCODE='871510000'||SECTCD||'Y';
    # AMOUNT=APPRLIM2; OUTPUT;
    
    bnm_records = []
    for (sectcd, amtind), vals in summary.items():
        # 673400000 - DISBURSE
        bnm_records.append({
            'BNMCODE': f"673400000{sectcd}Y",
            'AMTIND': amtind,
            'AMOUNT': vals['DISBURSE'],
            'NOACCT': vals['NOACCT']
        })
        
        # 773400000 - REPAID (SAS: IF REPAID=0 THEN NOACCT=0)
        repaid_noacct = vals['NOACCT'] if vals['REPAID'] != 0 else 0
        bnm_records.append({
            'BNMCODE': f"773400000{sectcd}Y",
            'AMTIND': amtind,
            'AMOUNT': vals['REPAID'],
            'NOACCT': repaid_noacct
        })
        
        # 871500000 or 871510000 - APPRLIM2
        if sectcd == '0000':
            code = f"871500000{sectcd}Y"
        else:
            code = f"871510000{sectcd}Y"
        bnm_records.append({
            'BNMCODE': code,
            'AMTIND': amtind,
            'AMOUNT': vals['APPRLIM2'],
            'NOACCT': 0
        })
    
    # ============================================
    # STEP 7: PROC SUMMARY by BNMCODE and AMTIND
    # ============================================
    logger.info("STEP 7: Final aggregation by BNMCODE...")
    
    # SAS: PROC SUMMARY DATA=ALWLOAN NWAY; CLASS BNMCODE AMTIND; VAR AMOUNT NOACCT;
    final_summary = {}
    for rec in bnm_records:
        key = (rec['BNMCODE'], rec['AMTIND'])
        if key not in final_summary:
            final_summary[key] = {'AMOUNT': 0.0, 'NOACCT': 0}
        final_summary[key]['AMOUNT'] += rec['AMOUNT']
        final_summary[key]['NOACCT'] += rec['NOACCT']
    
    del summary, bnm_records
    gc.collect()
    
    # ============================================
    # STEP 8: DATA _NULL_ - Final output format
    # ============================================
    logger.info("STEP 8: Formatting final output...")
    
    # SAS LOGIC:
    # RETAIN AMOUNTD AMOUNTI NOACCTD NOACCTI 0;
    # IF AMTIND='D' THEN DO;
    #   AMOUNTD+ROUND(AMOUNT/1000);
    #   NOACCTD+ROUND(NOACCT);
    # END;
    # ELSE IF AMTIND='I' THEN DO;
    #   AMOUNTI+ROUND(AMOUNT/1000);
    #   NOACCTI+ROUND(NOACCT);
    # END;
    # IF LAST.BNMCODE THEN DO;
    #   AMOUNTD=AMOUNTD+AMOUNTI;
    #   NOACCTD=NOACCTD+NOACCTI;
    #   PUT @1 BNMCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI +(-1) ';'
    #          NOACCTD +(-1) ';' NOACCTI +(-1);
    
    # Group by BNMCODE
    by_bnmcode = {}
    for (bnmcode, amtind), vals in final_summary.items():
        if bnmcode not in by_bnmcode:
            by_bnmcode[bnmcode] = {}
        by_bnmcode[bnmcode][amtind] = vals
    
    output_records = []
    for bnmcode, amtind_data in sorted(by_bnmcode.items()):
        # Initialize retain variables
        amountd = 0
        amounti = 0
        noacctd = 0
        noaccti = 0
        
        # Process D (conventional)
        if 'D' in amtind_data:
            amountd = int(round(amtind_data['D']['AMOUNT'] / 1000))
            noacctd = int(round(amtind_data['D']['NOACCT']))
        
        # Process I (Islamic)
        if 'I' in amtind_data:
            amounti = int(round(amtind_data['I']['AMOUNT'] / 1000))
            noaccti = int(round(amtind_data['I']['NOACCT']))
        
        # SAS: AMOUNTD = AMOUNTD + AMOUNTI; NOACCTD = NOACCTD + NOACCTI;
        amountd_total = amountd + amounti
        noacctd_total = noacctd + noaccti
        
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': amountd_total,
            'AMOUNTI': amounti,
            'NOACCTD': noacctd_total,
            'NOACCTI': noaccti
        })
    
    logger.info(f"  Final output records: {len(output_records):,}")
    log_memory_usage("END")
    
    return output_records

# =====================================================
# FILE WRITING - Matching SAS PUT format
# =====================================================

def write_output_file(path, records):
    """
    Write output in SAS PUT format.
    SAS: PUT @1 BNMCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI +(-1) ';'
                NOACCTD +(-1) ';' NOACCTI +(-1);
    The +(-1) removes the space after each variable.
    """
    logger.info(f"Writing {len(records):,} records...")
    
    with open(path, 'w', encoding='utf-8') as f:
        for record in records:
            # No spaces after semicolons (matching SAS +(-1) format)
            line = f"{record['BNMCODE']};{record['AMOUNTD']};{record['AMOUNTI']};{record['NOACCTD']};{record['NOACCTI']}"
            f.write(line + "\n")
    
    file_size = path.stat().st_size / 1024
    logger.info(f"  Output: {path.name} ({file_size:.1f} KB)")

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
        
        # Process
        start_time = datetime.now()
        records = process_loan_data()
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Processing completed in {elapsed:.1f} seconds")
        
        if not records:
            logger.warning("No records generated!")
            return 4
        
        # Write output
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        write_output_file(OUTPUT_DATASET, records)
        
        # Print sample for verification
        logger.info("Sample output records:")
        for rec in records[:10]:
            logger.info(f"  {rec['BNMCODE']};{rec['AMOUNTD']};{rec['AMOUNTI']};{rec['NOACCTD']};{rec['NOACCTI']}")
        
        logger.info("="*60)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info(f"Total records: {len(records):,}")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"JOB FAILED: {type(e).__name__}: {e}")
        logger.debug(traceback.format_exc())
        return 8

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
