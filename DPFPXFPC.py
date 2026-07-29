import sys
import pandas as pd
import pyreadstat
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import gc
import os

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "XMIS" / "input" / "prod" / "EIBWHP04"
OUTPUT_DIR = BASE_DIR / "XMIS" / "output" / "EIBWHP04"

CHUNK_SIZE = 50000

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

REPTMON = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"

INPUT_DATASETS = {
    "LOAN_CURRENT": INPUT_DIR / f"loan{REPTMON}{WK}.sas7bdat",
    "LOAN_PREVIOUS": INPUT_DIR / f"loan{REPTMON1}{WK1}.sas7bdat",
    "ULOAN": INPUT_DIR / f"uloan{REPTMON}{WK}.sas7bdat"
}

OUTPUT_DATASET = OUTPUT_DIR / f"EIBWHP04_{REPTDATE.strftime('%Y%m%d')}.txt"

# =====================================================
# IMPORT PBBLNFMT FORMAT FUNCTIONS
# =====================================================

try:
    sys.path.insert(0, str(Path(__file__).parent))
    from PBBLNFMT import format_secta, format_sectb
    print(f"Loaded format_secta and format_sectb from PBBLNFMT.py")
except ImportError as e:
    print(f"WARNING: Cannot import from PBBLNFMT.py: {e}")
    # Fallback: pass-through
    def format_secta(code):
        c = str(code).strip()
        return c if c and c != 'nan' else ' '
    def format_sectb(code):
        return ' '

def apply_sector_formats(sectorcd):
    """
    Apply SECTA and SECTB formats.
    SAS: SECTCD = PUT(SECTORCD,$SECTA.); IF SECTCD ^= ' ' THEN OUTPUT;
         SECTCD = PUT(SECTORCD,$SECTB.); IF SECTCD ^= ' ' THEN OUTPUT;
    """
    results = []
    sectorcd_str = str(sectorcd).strip()
    
    if not sectorcd_str or sectorcd_str == 'nan':
        return results
    
    # Apply SECTA format (calls PBBLNFMT.format_secta)
    secta = format_secta(sectorcd_str)
    if secta and secta.strip() and secta != 'nan':
        results.append(secta.strip())
    
    # Apply SECTB format (calls PBBLNFMT.format_sectb)
    sectb = format_sectb(sectorcd_str)
    if sectb and sectb.strip() and sectb != 'nan' and sectb.strip() != secta.strip():
        results.append(sectb.strip())
    
    return results

# =====================================================
# PYREADSTAT READER
# =====================================================

def read_sas_chunked(file_path, columns, chunk_size=CHUNK_SIZE):
    file_str = str(file_path)
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        file_str,
        usecols=columns,
        encoding='latin1',
        chunksize=chunk_size,
        dates_as_pandas_datetime=True
    )
    for df, meta in reader:
        yield df

def read_sas_full(file_path, columns):
    file_str = str(file_path)
    df, meta = pyreadstat.read_sas7bdat(
        file_str,
        usecols=columns,
        encoding='latin1',
        dates_as_pandas_datetime=True
    )
    return df

# =====================================================
# BUSINESS LOGIC
# =====================================================

def process_loan_data():
    valid_products = {131, 132, 720, 725}
    
    previous_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
    current_columns = ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE', 
                       'APPRLIM2', 'AMTIND', 'CUSTCD']
    uloan_columns = ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND']
    
    # STEP 1: Read previous period
    print("Reading previous period...")
    previous_df = read_sas_full(INPUT_DATASETS["LOAN_PREVIOUS"], columns=previous_columns)
    previous_df = previous_df[previous_df['PRODUCT'].isin(valid_products)].copy()
    print(f"  Previous period loans: {len(previous_df):,}")
    
    # Show sample sector code transformations for debugging
    sample_sectors = previous_df['SECTORCD'].dropna().unique()[:10]
    print(f"  Sample SECTORCD -> SECTA/SECTB:")
    for sc in sample_sectors:
        formatted = apply_sector_formats(sc)
        print(f"    {sc} -> {formatted}")
    
    previous_balances = {}
    for _, row in previous_df.iterrows():
        key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
        previous_balances[key] = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
    
    previous_keys = set(previous_balances.keys())
    del previous_df
    gc.collect()
    
    # STEP 2: Process current period
    print("Processing current period...")
    previous_matched = set()
    alw_records = []
    chunk_count = 0
    
    for chunk in read_sas_chunked(INPUT_DATASETS["LOAN_CURRENT"], columns=current_columns):
        chunk_count += 1
        chunk = chunk[chunk['PRODUCT'].isin(valid_products)]
        if len(chunk) == 0:
            continue
        
        for _, row in chunk.iterrows():
            try:
                key = (str(row['ACCTNO']), str(row['NOTENO']), str(row['SECTORCD']))
                previous_matched.add(key)
                
                current_balance = float(row['BALANCE']) if pd.notna(row['BALANCE']) else 0.0
                previous_balance = previous_balances.get(key)
                apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
                amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
                
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
                
                sector_codes = apply_sector_formats(str(row['SECTORCD']))
                
                for sectcd in sector_codes:
                    if sectcd and sectcd.strip():
                        alw_records.append({
                            'SECTCD': sectcd,
                            'DISBURSE': disburse,
                            'REPAID': repaid,
                            'APPRLIM2': apprlim2,
                            'AMTIND': amtind,
                            'NOACCT': 1
                        })
            except:
                continue
        
        if chunk_count % 10 == 0:
            print(f"  Chunks: {chunk_count}, Records: {len(alw_records):,}")
    
    print(f"  Total current records: {len(alw_records):,}")
    
    # STEP 3: Paid-off loans
    print("Processing paid-off loans...")
    paid_off_keys = previous_keys - previous_matched
    for key in paid_off_keys:
        balance = previous_balances[key]
        if balance == 0.0:
            continue
        sectorcd = key[2]
        sector_codes = apply_sector_formats(sectorcd)
        for sectcd in sector_codes:
            if sectcd and sectcd.strip():
                alw_records.append({
                    'SECTCD': sectcd,
                    'DISBURSE': 0.0,
                    'REPAID': balance,
                    'APPRLIM2': 0.0,
                    'AMTIND': 'D',
                    'NOACCT': 0
                })
    
    print(f"  Paid-off records added: {len(paid_off_keys):,}")
    del previous_keys, previous_balances, previous_matched
    gc.collect()
    
    # STEP 4: Process ULOAN
    print("Processing ULOAN...")
    uloan_df = read_sas_full(INPUT_DATASETS["ULOAN"], columns=uloan_columns)
    
    for _, row in uloan_df.iterrows():
        try:
            sectorcd = str(row['SECTORCD']) if pd.notna(row['SECTORCD']) else ''
            disburse = float(row['DISBURSE']) if pd.notna(row.get('DISBURSE')) else 0.0
            repaid = float(row['REPAID']) if pd.notna(row.get('REPAID')) else 0.0
            apprlim2 = float(row['APPRLIM2']) if pd.notna(row.get('APPRLIM2')) else 0.0
            amtind = str(row['AMTIND']).strip() if pd.notna(row.get('AMTIND')) else 'D'
            
            sector_codes = apply_sector_formats(sectorcd)
            for sectcd in sector_codes:
                if sectcd and sectcd.strip():
                    alw_records.append({
                        'SECTCD': sectcd,
                        'DISBURSE': disburse,
                        'REPAID': repaid,
                        'APPRLIM2': apprlim2,
                        'AMTIND': amtind,
                        'NOACCT': 0
                    })
        except:
            continue
    
    print(f"  ULOAN records added: {len(uloan_df):,}")
    del uloan_df
    gc.collect()
    
    # Show distinct SECTCD values being used
    distinct_sectcd = sorted(set(r['SECTCD'] for r in alw_records))
    print(f"  Distinct SECTCD values: {len(distinct_sectcd)}")
    print(f"  Samples: {distinct_sectcd[:20]}")
    
    # STEP 5: Aggregate by SECTCD and AMTIND
    print("Aggregating...")
    summary = {}
    for rec in alw_records:
        key = (rec['SECTCD'], rec['AMTIND'])
        if key not in summary:
            summary[key] = {'DISBURSE': 0.0, 'REPAID': 0.0, 'APPRLIM2': 0.0, 'NOACCT': 0}
        summary[key]['DISBURSE'] += rec['DISBURSE']
        summary[key]['REPAID'] += rec['REPAID']
        summary[key]['APPRLIM2'] += rec['APPRLIM2']
        summary[key]['NOACCT'] += rec['NOACCT']
    
    del alw_records
    gc.collect()
    
    # STEP 6: Generate BNM records
    print("Generating BNM records...")
    bnm_records = []
    for (sectcd, amtind), vals in summary.items():
        # 673400000 - DISBURSE
        bnm_records.append({
            'BNMCODE': f"673400000{sectcd}Y",
            'AMTIND': amtind,
            'AMOUNT': vals['DISBURSE'],
            'NOACCT': vals['NOACCT']
        })
        
        # 773400000 - REPAID (IF REPAID=0 THEN NOACCT=0)
        repaid_noacct = vals['NOACCT'] if vals['REPAID'] != 0 else 0
        bnm_records.append({
            'BNMCODE': f"773400000{sectcd}Y",
            'AMTIND': amtind,
            'AMOUNT': vals['REPAID'],
            'NOACCT': repaid_noacct
        })
        
        # 8715X0000 - APPRLIM2
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
    
    # STEP 7: Final aggregation
    final_summary = {}
    for rec in bnm_records:
        key = (rec['BNMCODE'], rec['AMTIND'])
        if key not in final_summary:
            final_summary[key] = {'AMOUNT': 0.0, 'NOACCT': 0}
        final_summary[key]['AMOUNT'] += rec['AMOUNT']
        final_summary[key]['NOACCT'] += rec['NOACCT']
    
    del summary, bnm_records
    gc.collect()
    
    # STEP 8: Final output format (matching SAS DATA _NULL_)
    by_bnmcode = {}
    for (bnmcode, amtind), vals in final_summary.items():
        if bnmcode not in by_bnmcode:
            by_bnmcode[bnmcode] = {}
        by_bnmcode[bnmcode][amtind] = vals
    
    output_records = []
    for bnmcode in sorted(by_bnmcode.keys()):
        amtind_data = by_bnmcode[bnmcode]
        amountd = 0
        amounti = 0
        noacctd = 0
        noaccti = 0
        
        if 'D' in amtind_data:
            amountd = int(round(amtind_data['D']['AMOUNT'] / 1000))
            noacctd = int(round(amtind_data['D']['NOACCT']))
        
        if 'I' in amtind_data:
            amounti = int(round(amtind_data['I']['AMOUNT'] / 1000))
            noaccti = int(round(amtind_data['I']['NOACCT']))
        
        # SAS: AMOUNTD = AMOUNTD + AMOUNTI; NOACCTD = NOACCTD + NOACCTI;
        output_records.append({
            'BNMCODE': bnmcode,
            'AMOUNTD': amountd + amounti,
            'AMOUNTI': amounti,
            'NOACCTD': noacctd + noaccti,
            'NOACCTI': noaccti
        })
    
    print(f"  Final records: {len(output_records):,}")
    
    # Print sample for verification
    print("\nSample output (first 10):")
    for rec in output_records[:10]:
        print(f"  {rec['BNMCODE']};{rec['AMOUNTD']};{rec['AMOUNTI']};{rec['NOACCTD']};{rec['NOACCTI']}")
    
    return output_records

# =====================================================
# FILE WRITING
# =====================================================

def write_output_file(path, records):
    with open(path, 'w', encoding='utf-8') as f:
        for record in records:
            line = f"{record['BNMCODE']};{record['AMOUNTD']};{record['AMOUNTI']};{record['NOACCTD']};{record['NOACCTI']}"
            f.write(line + "\n")

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    try:
        for name, path in INPUT_DATASETS.items():
            if not path.exists():
                print(f"ERROR: Input file not found: {path}")
                return 8
        
        if OUTPUT_DATASET.exists():
            OUTPUT_DATASET.unlink()
        
        records = process_loan_data()
        
        if not records:
            print("WARNING: No records generated!")
            return 4
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        write_output_file(OUTPUT_DATASET, records)
        print(f"\nOutput written to: {OUTPUT_DATASET}")
        
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        return 8

if __name__ == "__main__":
    try:
        os.nice(10)
    except:
        pass
    
    exit_code = run_job()
    sys.exit(exit_code)
