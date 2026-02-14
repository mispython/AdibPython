"""
SAS to Python Translation - EIWELNEX
Processes loan application data from multiple source files
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import re

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

# Create directories
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Libraries (SAS libnames)
ELDSRV_DIR = OUTPUT_DIR / "eldsrv"
ELDSRVO_DIR = INPUT_DIR / "eldsrvo"
ELDSFUL_DIR = INPUT_DIR / "eldsful"

for dir_path in [ELDSRV_DIR, ELDSRVO_DIR, ELDSFUL_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Input files (8 source files)
INPUT_FILES = {
    f'ELDSRV{i}': INPUT_DIR / f"ELDSRV{i}.parquet" for i in range(1, 9)
}

# =============================================================================
# DATE HANDLING AND MACRO VARIABLES
# =============================================================================

def sas_date_to_datetime(sas_date):
    """Convert SAS numeric date to datetime"""
    if sas_date is None or sas_date <= 0:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def datetime_to_sas_date(dt):
    """Convert datetime to SAS numeric date"""
    if dt is None:
        return 0
    reference = datetime(1960, 1, 1)
    return (dt - reference).days

def parse_ddmmyyyy(date_str):
    """Parse DDMMYYYY string to date"""
    if not date_str or len(date_str) != 8:
        return None
    try:
        day = int(date_str[0:2])
        month = int(date_str[2:4])
        year = int(date_str[4:8])
        return datetime(year, month, day).date()
    except:
        return None

def setup_date_variables():
    """Calculate report date and week based on current day"""
    today = datetime.now().date()
    day = today.day
    month = today.month
    year = today.year
    
    # Determine report date and week based on day ranges
    if 8 <= day <= 14:
        reptdate = datetime(year, month, 8).date()
        wk = '1'
    elif 15 <= day <= 21:
        reptdate = datetime(year, month, 15).date()
        wk = '2'
    elif 22 <= day <= 27:
        reptdate = datetime(year, month, 22).date()
        wk = '3'
    else:
        # First day of month minus 1 day
        first_of_month = datetime(year, month, 1).date()
        reptdate = first_of_month - timedelta(days=1)
        wk = '4'
    
    reptdate1 = today - timedelta(days=1)
    
    return {
        'REPTDATE': reptdate,
        'REPTDATE_SAS': datetime_to_sas_date(datetime.combine(reptdate, datetime.min.time())),
        'WK': wk,
        'NOWK': wk,
        'RDATE': reptdate.strftime('%d%m%Y'),
        'REPTMON': reptdate.strftime('%m'),
        'REPTDAY1': reptdate1.strftime('%d'),
        'REPTMON1': reptdate1.strftime('%m'),
        'REPTYR1': reptdate1.strftime('%y'),
        'REPTYEAR': reptdate.strftime('%y')
    }

def extract_file_date(file_path):
    """Extract date from position 54-60 in file"""
    if not file_path.exists():
        return None
    
    try:
        # Read first line only
        with open(file_path, 'rb') as f:
            first_line = f.readline().decode('utf-8', errors='ignore')
        
        # Extract date from positions 54-60 (1-indexed in SAS, 0-indexed in Python)
        if len(first_line) >= 60:
            dd = int(first_line[53:55].strip() or '0')
            mm = int(first_line[56:58].strip() or '0')
            yy = int(first_line[59:63].strip() or '0')
            
            if dd > 0 and mm > 0 and yy > 0:
                return datetime(yy, mm, dd).date()
    except:
        pass
    
    return None

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def check_all_files_dated(macro_vars):
    """Check if all 8 input files have the correct date"""
    rdate = macro_vars['RDATE']
    file_dates = {}
    
    for i in range(1, 9):
        file_path = INPUT_FILES[f'ELDSRV{i}']
        file_date = extract_file_date(file_path)
        
        if file_date:
            file_date_str = file_date.strftime('%d%m%Y')
            file_dates[f'ELDSDT{i}'] = file_date_str
            print(f"File ELDSRV{i} date: {file_date_str}")
        else:
            file_dates[f'ELDSDT{i}'] = None
            print(f"File ELDSRV{i}: Could not extract date")
    
    # Check if all files have the correct date
    all_correct = all(
        file_dates[f'ELDSDT{i}'] == rdate for i in range(1, 9)
    )
    
    return all_correct, file_dates

def process_elna1(macro_vars):
    """Process ELNA1 - Main application data"""
    file_path = INPUT_FILES['ELDSRV1']
    
    if not file_path.exists():
        print("Warning: ELDSRV1 not found")
        return None
    
    # Read with Polars, skip header row
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        separator='|',  # Assuming fixed-width converted to CSV/Parquet
        null_values=['', ' ']
    )
    
    # Rename columns based on SAS input positions
    # This mapping would need to be completed based on actual parquet schema
    # For brevity, showing key fields only
    df = df.rename({
        "column_1": "AANO",
        "column_17": "NAME",
        "column_80": "NEWIC",
        "column_95": "BRANCH",
        "column_102": "CUSTCODE",
        "column_109": "SECTOR",
        "column_116": "STATE",
        "column_122": "PRODUCTX",
        "column_128": "PRICING",
        "column_136": "DD",
        "column_139": "MM",
        "column_142": "YY",
        "column_149": "AMOUNT",
        "column_983": "STATUS"
    })
    
    # Filter APPROVED status
    df = df.filter(pl.col('STATUS').str.to_uppercase().is_in(['APPROVED']))
    
    # Create date fields
    df = df.with_columns([
        pl.date(pl.col('YY'), pl.col('MM'), pl.col('DD')).alias('AADATE'),
        pl.date(pl.col('YY1'), pl.col('MM1'), pl.col('DD1')).alias('LODATE'),
        pl.date(pl.col('YY2'), pl.col('MM2'), pl.col('DD2')).alias('APVDTE1'),
        pl.date(pl.col('YY3'), pl.col('MM3'), pl.col('DD3')).alias('APVDTE2'),
        pl.date(pl.col('YY4'), pl.col('MM4'), pl.col('DD4')).alias('ICDATE'),
        pl.date(pl.col('YY5'), pl.col('MM5'), pl.col('DD5')).alias('LOBEFUDT'),
        pl.date(pl.col('YY6'), pl.col('MM6'), pl.col('DD6')).alias('LOBEAPDT'),
        pl.date(pl.col('YY7'), pl.col('MM7'), pl.col('DD7')).alias('LOBELODT')
    ])
    
    return df

def process_elna2(macro_vars):
    """Process ELNA2 - Additional application details"""
    file_path = INPUT_FILES['ELDSRV2']
    
    if not file_path.exists():
        print("Warning: ELDSRV2 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    # Rename columns
    df = df.rename({
        "column_1": "AANO",
        "column_17": "REFTYPE",
        "column_90": "NMREF1",
        "column_153": "NMREF2",
        "column_216": "CPARTY",
        "column_222": "CPSTAFF",
        "column_993": "STATUS"
    })
    
    # Filter APPROVED status
    df = df.filter(pl.col('STATUS').str.to_uppercase().is_in(['APPROVED']))
    
    # Handle special character translation
    if 'INCEPTION_RISK_RATING_GRADE_SME' in df.columns:
        df = df.with_columns([
            pl.col('INCEPTION_RISK_RATING_GRADE_SME').str.replace_all('��', '[]')
        ])
    
    return df

def process_elna3(macro_vars):
    """Process ELNA3 - Facility details"""
    file_path = INPUT_FILES['ELDSRV3']
    
    if not file_path.exists():
        print("Warning: ELDSRV3 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    # Rename columns
    df = df.rename({
        "column_1": "AANO",
        "column_17": "MAANO",
        "column_33": "APVBY",
        "column_38": "MRTAIND",
        "column_786": "STATUS",
        "column_971": "FACCODE"
    })
    
    # Filter APPROVED status
    df = df.filter(pl.col('STATUS').str.to_uppercase().is_in(['APPROVED']))
    
    # Apply left strip to character columns
    str_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
    for col in str_cols:
        df = df.with_columns([
            pl.col(col).str.strip_chars_start().alias(col)
        ])
    
    # Determine PRODUCT from FACCODE
    df = df.with_columns([
        pl.when(
            pl.col('FACCODE').str.slice(2, 1) == '5'
        ).then(
            pl.col('FACCODE').str.slice(5, 5).cast(pl.Int64)
        ).otherwise(
            pl.col('FACCODE').str.slice(7, 3).cast(pl.Int64)
        ).alias('PRODUCT')
    ])
    
    # Handle DNBFISME
    if 'DNBFISME' in df.columns:
        df = df.with_columns([
            pl.col('DNBFISME').fill_null('0').alias('DNBFISME')
        ])
    
    return df

def process_elna4(macro_vars):
    """Process ELNA4 - Applicant personal details"""
    file_path = INPUT_FILES['ELDSRV4']
    
    if not file_path.exists():
        print("Warning: ELDSRV4 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    # Rename columns
    df = df.rename({
        "column_1": "MAANO",
        "column_17": "APPLNAME",
        "column_70": "APVBY",
        "column_289": "DOBB",
        "column_292": "DOMM",
        "column_295": "DOYY"
    })
    
    # Create birth date
    df = df.with_columns([
        pl.date(pl.col('DOYY'), pl.col('DOMM'), pl.col('DOBB')).alias('DBIRTH')
    ])
    
    # Handle special character translation for ASCORE_GRADE
    if 'ASCORE_GRADE' in df.columns:
        df = df.with_columns([
            pl.col('ASCORE_GRADE').str.replace_all('��', '[]').alias('ASCORE_GRADE')
        ])
    
    return df

def process_elna5(macro_vars):
    """Process ELNA5 - CRR data"""
    file_path = INPUT_FILES['ELDSRV5']
    
    if not file_path.exists():
        print("Warning: ELDSRV5 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    df = df.rename({
        "column_1": "MAANO",
        "column_17": "DEBTSR",
        "column_60": "NETWRTH5"
    })
    
    return df

def process_elna6(macro_vars):
    """Process ELNA6 - Financial computation"""
    file_path = INPUT_FILES['ELDSRV6']
    
    if not file_path.exists():
        print("Warning: ELDSRV6 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    df = df.rename({
        "column_1": "MAANO",
        "column_17": "FINDD",
        "column_20": "FINMM",
        "column_23": "FINYY",
        "column_30": "AUMDIND"
    })
    
    # Create financial year end date
    df = df.with_columns([
        pl.date(pl.col('FINYY'), pl.col('FINMM'), pl.col('FINDD')).alias('FYREND')
    ])
    
    return df

def process_elna7(macro_vars):
    """Process ELNA7 - Security details"""
    file_path = INPUT_FILES['ELDSRV7']
    
    if not file_path.exists():
        print("Warning: ELDSRV7 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    df = df.rename({
        "column_1": "MAANO",
        "column_17": "AANOSEC7",
        "column_33": "INDNEWE"
    })
    
    # Create date fields
    df = df.with_columns([
        pl.when(
            pl.col('SYY').is_not_null()
        ).then(
            pl.date(pl.col('SYY'), pl.col('SMM'), pl.col('SDD'))
        ).otherwise(None).alias('SPADT'),
        
        pl.when(
            pl.col('VYY').is_not_null()
        ).then(
            pl.date(pl.col('VYY'), pl.col('VMM'), pl.col('VDD'))
        ).otherwise(None).alias('VALUEDT'),
        
        pl.when(
            pl.col('AYY').is_not_null()
        ).then(
            pl.date(pl.col('AYY'), pl.col('AMM'), pl.col('ADD'))
        ).otherwise(None).alias('ADLAPRDT'),
        
        pl.when(
            pl.col('PCTCOMP') == 100
        ).then(pl.lit('Y')).otherwise(pl.lit('N')).alias('COMPSTAT')
    ])
    
    return df

def process_elna8(macro_vars):
    """Process ELNA8 - Main form & sub form"""
    file_path = INPUT_FILES['ELDSRV8']
    
    if not file_path.exists():
        print("Warning: ELDSRV8 not found")
        return None
    
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        null_values=['', ' ']
    )
    
    df = df.rename({
        "column_1": "MAANO",
        "column_17": "CISNO",
        "column_35": "ACCTNO1",
        "column_53": "ACCTNO2"
    })
    
    # Create date fields
    df = df.with_columns([
        pl.when(
            pl.col('XYY').is_not_null()
        ).then(
            pl.date(pl.col('XYY'), pl.col('XMM'), pl.col('XDD'))
        ).otherwise(None).alias('APPLDATE'),
        
        pl.when(
            pl.col('INCYY').is_not_null()
        ).then(
            pl.date(pl.col('INCYY'), pl.col('INCMM'), pl.col('INCDD'))
        ).otherwise(None).alias('INCRDT'),
        
        pl.when(
            pl.col('CYY').is_not_null()
        ).then(
            pl.date(pl.col('CYY'), pl.col('CMM'), pl.col('CDD'))
        ).otherwise(None).alias('COMPLIDT')
    ])
    
    return df

def update_with_summ1(elna2_df, macro_vars):
    """Update ELNA2 with SUMM1 data"""
    summ1_file = ELDSFUL_DIR / f"SUMM1{macro_vars['REPTYR1']}{macro_vars['REPTMON1']}{macro_vars['REPTDAY1']}.parquet"
    
    if not summ1_file.exists():
        print("Warning: SUMM1 file not found")
        return elna2_df
    
    summ1_df = pl.read_parquet(summ1_file)
    
    # Filter STAGE = 'A' and keep required columns
    summ1_df = summ1_df.filter(
        pl.col('STAGE') == 'A'
    ).select([
        'AANO', 'STRUPCO_3YR', 'FIN_CONCEPT', 'LN_UTILISE_LOCAT_CD',
        'ASSET_PURCH_AMT', 'AMTAPPLY'
    ]).unique(subset=['AANO'])
    
    # Merge with ELNA2
    result = elna2_df.join(summ1_df, on='AANO', how='left')
    
    return result

def update_with_summ2(elna4_df, macro_vars):
    """Update ELNA4 with SUMM2 data"""
    summ2_file = ELDSFUL_DIR / f"SUMM2{macro_vars['REPTYR1']}{macro_vars['REPTMON1']}{macro_vars['REPTDAY1']}.parquet"
    
    if not summ2_file.exists():
        print("Warning: SUMM2 file not found")
        return elna4_df
    
    summ2_df = pl.read_parquet(summ2_file)
    
    # Filter STAGE = 'A' and keep required columns
    summ2_df = summ2_df.filter(
        pl.col('STAGE') == 'A'
    ).select([
        'MAANO', 'ICPP', 'EMPLOY_SECTOR_CD', 'EMPLOY_TYPE_CD',
        'OCCUPAT_MASCO_CD', 'CORP_STATUS_CD', 'RESIDENCY_STATUS_CD',
        'ANNSUBTSALARY', 'EMPNAME', 'POSTCODE', 'STATE_CD', 'COUNTRY_CD',
        'INDUSTRIAL_SECTOR_CD', 'DSRISS3'
    ]).unique(subset=['MAANO', 'ICPP'])
    
    # Merge with ELNA4
    result = elna4_df.join(summ2_df, on=['MAANO', 'ICPP'], how='left')
    
    return result

def combine_with_archive(df, archive_name):
    """Combine current data with archive data"""
    archive_path = ELDSRVO_DIR / f"{archive_name}.parquet"
    
    if archive_path.exists():
        archive_df = pl.read_parquet(archive_path)
        combined = pl.concat([df, archive_df])
        return combined
    
    return df

def create_final_dataset(macro_vars):
    """Create final ELBNMAX dataset by merging all components"""
    
    # Process all ELNA datasets
    print("Processing ELNA1...")
    elna1_df = process_elna1(macro_vars)
    if elna1_df is not None:
        elna1_df = combine_with_archive(elna1_df, 'ELNA1')
        elna1_df = elna1_df.unique(subset=['AANO']).sort('AANO')
    
    print("Processing ELNA2...")
    elna2_df = process_elna2(macro_vars)
    if elna2_df is not None:
        elna2_df = update_with_summ1(elna2_df, macro_vars)
        elna2_df = combine_with_archive(elna2_df, 'ELNA2')
        elna2_df = elna2_df.unique(subset=['AANO']).sort('AANO')
    
    print("Processing ELNA3...")
    elna3_df = process_elna3(macro_vars)
    if elna3_df is not None:
        elna3_df = combine_with_archive(elna3_df, 'ELNA3')
        elna3_df = elna3_df.unique(subset=['AANO']).sort('AANO')
    
    # Create ELNA13 by merging ELNA1, ELNA2, ELNA3
    if elna1_df is not None and elna2_df is not None and elna3_df is not None:
        elna13_df = elna1_df.join(elna2_df, on='AANO', how='left')
        elna13_df = elna13_df.join(elna3_df, on='AANO', how='left')
        
        # Handle PRODUCT from FACCODE if empty
        if 'PRODUCT' in elna13_df.columns and 'PRODUCTX' in elna13_df.columns:
            elna13_df = elna13_df.with_columns([
                pl.when(pl.col('FACCODE').is_null())
                .then(pl.col('PRODUCTX'))
                .otherwise(pl.col('PRODUCT'))
                .alias('PRODUCT')
            ])
        
        elna13_df = elna13_df.sort('MAANO')
    else:
        elna13_df = None
    
    # Process other ELNA datasets
    print("Processing ELNA4...")
    elna4_df = process_elna4(macro_vars)
    if elna4_df is not None:
        elna4_df = update_with_summ2(elna4_df, macro_vars)
        elna4_df = combine_with_archive(elna4_df, 'ELNA4')
        elna4_df = elna4_df.unique(subset=['MAANO']).sort('MAANO')
    
    print("Processing ELNA5...")
    elna5_df = process_elna5(macro_vars)
    if elna5_df is not None:
        elna5_df = combine_with_archive(elna5_df, 'ELNA5')
        elna5_df = elna5_df.unique(subset=['MAANO']).sort('MAANO')
    
    print("Processing ELNA6...")
    elna6_df = process_elna6(macro_vars)
    if elna6_df is not None:
        elna6_df = combine_with_archive(elna6_df, 'ELNA6')
        elna6_df = elna6_df.unique(subset=['MAANO']).sort('MAANO')
    
    print("Processing ELNA7...")
    elna7_df = process_elna7(macro_vars)
    if elna7_df is not None:
        elna7_df = combine_with_archive(elna7_df, 'ELNA7')
        elna7_df = elna7_df.unique(subset=['MAANO']).sort('MAANO')
    
    print("Processing ELNA8...")
    elna8_df = process_elna8(macro_vars)
    if elna8_df is not None:
        elna8_df = combine_with_archive(elna8_df, 'ELNA8')
        elna8_df = elna8_df.unique(subset=['MAANO']).sort('MAANO')
    
    # Final merge by MAANO
    if elna13_df is not None:
        final_df = elna13_df
        
        for df, name in [(elna4_df, 'ELNA4'), (elna5_df, 'ELNA5'), 
                         (elna6_df, 'ELNA6'), (elna7_df, 'ELNA7'), 
                         (elna8_df, 'ELNA8')]:
            if df is not None:
                final_df = final_df.join(df, on='MAANO', how='left')
        
        # Save final dataset
        output_path = ELDSRV_DIR / "ELBNMAX.parquet"
        final_df.write_parquet(output_path, compression='snappy')
        print(f"Final ELBNMAX created with {len(final_df)} rows")
        
        return final_df
    
    return None

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    print(f"Starting EIWELNEX translation at {datetime.now()}")
    
    # Setup date variables
    macro_vars = setup_date_variables()
    print(f"Report Date: {macro_vars['RDATE']}, Week: {macro_vars['WK']}")
    
    # Check if all input files have the correct date
    print("\nChecking input file dates...")
    all_correct, file_dates = check_all_files_dated(macro_vars)
    
    if all_correct:
        print("\nAll files have correct date. Processing data...")
        
        try:
            # Create final dataset
            final_df = create_final_dataset(macro_vars)
            
            if final_df is not None:
                print(f"\nProcessing completed successfully at {datetime.now()}")
                print(f"Output saved to: {ELDSRV_DIR / 'ELBNMAX.parquet'}")
            else:
                print("Failed to create final dataset")
                
        except Exception as e:
            print(f"\nError during processing: {str(e)}")
            import traceback
            traceback.print_exc()
            
    else:
        print("\n" + "="*60)
        print("FILE DATE MISMATCH DETECTED!")
        print("="*60)
        
        for i in range(1, 9):
            expected = macro_vars['RDATE']
            actual = file_dates.get(f'ELDSDT{i}', 'Unknown')
            status = "✓" if actual == expected else "✗"
            print(f"File ELDSRV{i}: Expected {expected}, Got {actual} {status}")
        
        print(f"\nThe SAP.PBB.ELDS.NEWBNM1.TEXT(0) not dated {macro_vars['RDATE']}")
        print("THE JOB IS NOT DONE !!")
        
        # Simulate ABORT 77
        raise SystemExit(77)

if __name__ == "__main__":
    main()
