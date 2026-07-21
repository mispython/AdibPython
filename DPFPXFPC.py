"""
EIIFTXT1 - Bad Debt Write-Off List Generation
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import sys
import os
import time

# Import format definition programs (%INC PGM equivalent)
sys.path.insert(0, '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')
from PBBLNFMT import (
    HP_ALL, HP_ACTIVE, AITAB, MORE_PLAN, MORE_ISLAM,
    HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL,
    FCY_PRODUCTS,
    format_mthpass, format_ndays, format_lnprod, format_lndenom,
    format_odprod, format_oddenom, format_collcd, format_riskcd,
    format_delqdes, format_statecd
)

# TESTING MODE - Limit to 100000 rows
TEST_MODE = True
TEST_LIMIT = 100000

# Since get_branch_name is not in PBBLNFMT, define it here
def get_branch_name(branch_code):
    """Get branch abbreviation from branch code."""
    branch_map = {
        1: 'HQ', 2: 'KL', 3: 'PJ', 4: 'JB', 5: 'PG',
        6: 'IP', 7: 'KK', 8: 'KU', 9: 'MK', 10: 'SB',
    }
    return branch_map.get(branch_code, 'BR')

# Use library paths from PBBELF
try:
    from PBBELF import LIBRARY_PATHS, format_ddmmyy10, format_mmddyy10
except ImportError:
    LIBRARY_PATHS = {
        'LOAN': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/',
        'NPL6': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/',
    }
    def format_ddmmyy10(date_obj):
        return date_obj.strftime('%d/%m/%Y') if date_obj else ''
    def format_mmddyy10(date_obj):
        return date_obj.strftime('%m/%d/%Y') if date_obj else ''

LOAN_DIR = LIBRARY_PATHS.get('LOAN', '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/')
NPL_DIR = LIBRARY_PATHS.get('NPL6', '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/')
SASLN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/'
CISNAME_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/'
CCRIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/'
BKCTRL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/'

OUTPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt'
OUTPUT_FILE1 = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt'

HPD = HP_ACTIVE

# Additional formats
OCCUPFMT = {
    '001': 'PROFESSIONAL', '002': 'BUSINESSMAN', '003': 'SELF EMPLOYED',
    '004': 'EMPLOYEE - PRIVATE', '005': 'EMPLOYEE - GOVERNMENT',
    '006': 'RETIRED', '999': 'OTHERS'
}

BGCFMT = {
    'B': 'BUSINESS', 'G': 'GOVERNMENT', 'C': 'CORPORATE',
    'I': 'INDIVIDUAL', '  ': 'NOT SPECIFIED'
}

def read_sas7bdat_fast(filepath, columns=None, row_limit=None):
    """
    Read SAS dataset quickly with limited rows using pyreadstat's row_limit parameter.
    This is much faster than reading the entire file.
    """
    try:
        if not os.path.exists(filepath):
            print(f"Warning: File not found: {filepath}")
            return None
        
        start_time = time.time()
        print(f"  Reading: {os.path.basename(filepath)}", end="", flush=True)
        
        # Use row_limit parameter to only read needed rows
        if row_limit and row_limit > 0:
            df, meta = pyreadstat.read_sas7bdat(
                filepath, 
                row_limit=row_limit,
                usecols=columns if columns else None
            )
        else:
            df, meta = pyreadstat.read_sas7bdat(
                filepath,
                usecols=columns if columns else None
            )
        
        elapsed = time.time() - start_time
        print(f" - {len(df)} rows, {elapsed:.1f}s")
        
        if len(df) == 0:
            return None
            
        return pl.from_pandas(df)
    except Exception as e:
        print(f"\nError reading {filepath}: {e}")
        return None

def write_sas7bdat(df, filepath):
    """Write DataFrame to SAS dataset using pyreadstat"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        pyreadstat.write_sas7bdat(df.to_pandas(), filepath)
        print(f"  Wrote: {os.path.basename(filepath)}")
    except Exception as e:
        print(f"Error writing {filepath}: {e}")

def get_delq_desc(delqcd):
    return format_delqdes(delqcd) if delqcd else 'NO LEGAL ACTION TAKEN'

def get_occup_desc(occupat):
    return OCCUPFMT.get(occupat if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    return BGCFMT.get(bgc if bgc else '  ', 'NOT SPECIFIED')

def standardize_schema(df, schema_dict):
    """Standardize column types to match expected schema"""
    if df is None or df.height == 0:
        return df
    
    for col, dtype in schema_dict.items():
        if col in df.columns:
            try:
                if dtype == pl.Int64:
                    df = df.with_columns(pl.col(col).cast(pl.Float64).cast(pl.Int64))
                elif dtype == pl.Float64:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
                elif dtype == pl.Utf8:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))
            except Exception as e:
                print(f"  Warning: Could not cast column {col} to {dtype}: {e}")
    
    return df

# Set report date to yesterday
reptdate = datetime.now().date() - timedelta(days=1)
print(f"\n{'='*60}")
print(f"Bad Debt Write-Off List Generation")
print(f"{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

day = reptdate.day
if day == 8:
    wk, wk1 = '1', '4'
elif day == 15:
    wk, wk1 = '2', '1'
elif day == 22:
    wk, wk1 = '3', '2'
else:
    wk, wk1 = '4', '3'

mm = reptdate.month
mm1 = mm - 1 if mm > 1 else 12

nowk, nowks, nowk1 = wk, '4', wk1
reptmon, reptmon1 = f'{mm:02d}', f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate = reptdate.strftime('%d/%m/%y')

print(f"Week: {nowk}, Previous Month: {reptmon1}")
if TEST_MODE:
    print(f"*** TEST MODE: Limiting to {TEST_LIMIT} rows ***")
print(f"{'='*60}\n")

# ============================================================================
# STEP 1: DEBUG - Check BORSTAT values
# ============================================================================
print("STEP 1: DEBUG - Checking BORSTAT values...")

# Read a small sample to check BORSTAT values
df_sample = read_sas7bdat_fast(
    f'{LOAN_DIR}lnnote.sas7bdat',
    columns=['BORSTAT', 'LOANTYPE'],
    row_limit=1000
)

if df_sample is not None:
    print(f"\n  BORSTAT column info:")
    print(f"  Data type: {df_sample['BORSTAT'].dtype}")
    
    # Get unique values and their counts - use pl.len() instead of pl.count()
    unique_vals = df_sample.group_by('BORSTAT').agg(pl.len())
    print(f"  Unique values:")
    for row in unique_vals.iter_rows(named=True):
        print(f"    '{row['BORSTAT']}' : {row['len']} rows")
    
    # Check if there are any rows with BORSTAT that might be 'A'
    if 'A' in df_sample['BORSTAT'].to_list():
        print("  Found 'A' values in sample")
    else:
        print("  No 'A' values found in sample")
        print("  Trying with string strip...")
        
        # Try with strip_chars (Polars equivalent of strip)
        df_sample = df_sample.with_columns([
            pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars().alias('BORSTAT_STRIPPED')
        ])
        unique_vals_stripped = df_sample.group_by('BORSTAT_STRIPPED').agg(pl.len())
        print(f"  Unique values after stripping:")
        for row in unique_vals_stripped.iter_rows(named=True):
            print(f"    '{row['BORSTAT_STRIPPED']}' : {row['len']} rows")
        
        # Check for 'A' after stripping
        if 'A' in df_sample['BORSTAT_STRIPPED'].to_list():
            print("  Found 'A' values after stripping!")
        else:
            print("  No 'A' values found after stripping")
            
        # Also check for numeric codes - maybe BORSTAT is stored as numeric
        print("\n  Checking if BORSTAT might be numeric...")
        try:
            # Try to convert to numeric
            df_sample = df_sample.with_columns([
                pl.col('BORSTAT').cast(pl.Float64, strict=False).alias('BORSTAT_NUM')
            ])
            unique_nums = df_sample.group_by('BORSTAT_NUM').agg(pl.len())
            print(f"  Numeric values:")
            for row in unique_nums.iter_rows(named=True):
                print(f"    {row['BORSTAT_NUM']} : {row['len']} rows")
        except:
            print("  Could not convert BORSTAT to numeric")
else:
    print("  Could not read sample data")
    sys.exit(1)

# ============================================================================
# STEP 2: Read NPLA data - Active accounts with borrower status 'A'
# ============================================================================
print("\nSTEP 2: Reading NPLA data...")
start_total = time.time()

# Read only needed columns from LNNOTE
loan_columns = ['BORSTAT', 'LOANTYPE', 'NAME', 'ACCTNO', 'NOTENO', 
                'FEEDUE', 'FEEDUEMS', 'FEEAMT16', 'MARKETVL', 'NTBRCH']
df_npla_raw = read_sas7bdat_fast(
    f'{LOAN_DIR}lnnote.sas7bdat',
    columns=loan_columns,
    row_limit=TEST_LIMIT if TEST_MODE else None
)

if df_npla_raw is not None:
    # Standardize column types
    df_npla_raw = standardize_schema(df_npla_raw, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'FEEDUE': pl.Float64,
        'FEEDUEMS': pl.Float64,
        'FEEAMT16': pl.Float64,
        'MARKETVL': pl.Float64,
        'NTBRCH': pl.Float64
    })
    
    # Debug: Check BORSTAT values in full dataset
    print(f"\n  Checking BORSTAT values in full dataset...")
    borstat_counts = df_npla_raw.group_by('BORSTAT').agg(pl.len())
    print("  BORSTAT distribution:")
    for row in borstat_counts.iter_rows(named=True):
        print(f"    '{row['BORSTAT']}' : {row['len']} rows")
    
    # Try different ways to filter for active accounts
    # 1. Exact match
    df_active_exact = df_npla_raw.filter(pl.col('BORSTAT') == 'A')
    print(f"\n  Exact match 'A': {df_active_exact.height} rows")
    
    # 2. Strip whitespace
    df_npla_raw = df_npla_raw.with_columns([
        pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars().alias('BORSTAT_CLEAN')
    ])
    df_active_clean = df_npla_raw.filter(pl.col('BORSTAT_CLEAN') == 'A')
    print(f"  After strip 'A': {df_active_clean.height} rows")
    
    # 3. Check for numeric codes - in SAS, 'A' might be stored as 1 or some other number
    print(f"\n  Checking for possible numeric codes...")
    # Check if BORSTAT can be converted to numeric
    try:
        df_npla_raw = df_npla_raw.with_columns([
            pl.col('BORSTAT').cast(pl.Float64, strict=False).alias('BORSTAT_NUM')
        ])
        # Look for active status codes (common: 1, 2, 3, etc.)
        for code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            df_active_num = df_npla_raw.filter(pl.col('BORSTAT_NUM') == code)
            if df_active_num.height > 0:
                print(f"    Numeric code {code}: {df_active_num.height} rows")
    except:
        print("    Could not convert BORSTAT to numeric")
    
    # Use the most successful method
    if df_active_clean.height > 0:
        print("\n  Using stripped 'A' filtering...")
        df_npla = df_active_clean.with_columns([
            pl.lit(0.0).alias('IIS'),
            (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('OI'),
            (pl.lit(0.0) + (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0))).alias('TOTIIS'),
            (pl.col('FEEDUEMS').fill_null(0) + pl.col('FEEAMT16').fill_null(0)).alias('SP')
        ])
    elif df_active_exact.height > 0:
        print("\n  Using exact 'A' filtering...")
        df_npla = df_active_exact.with_columns([
            pl.lit(0.0).alias('IIS'),
            (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('OI'),
            (pl.lit(0.0) + (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0))).alias('TOTIIS'),
            (pl.col('FEEDUEMS').fill_null(0) + pl.col('FEEAMT16').fill_null(0)).alias('SP')
        ])
    else:
        print("\n  WARNING: Could not find 'A' values with any method!")
        print("  Available BORSTAT values:", df_npla_raw['BORSTAT'].unique().to_list())
        print("  Available BORSTAT_CLEAN values:", df_npla_raw['BORSTAT_CLEAN'].unique().to_list())
        
        # If no 'A' found, check if there are any non-empty values
        non_empty = df_npla_raw.filter(pl.col('BORSTAT_CLEAN') != '')
        print(f"\n  Non-empty BORSTAT rows: {non_empty.height}")
        if non_empty.height > 0:
            print("  BORSTAT values in non-empty rows:", non_empty['BORSTAT'].unique().to_list())
        
        print("\n  Since BORSTAT='A' not found, exiting...")
        sys.exit(1)

    # Apply branch codes
    branch_list = []
    for ntbrch in df_npla['NTBRCH'].to_list():
        if ntbrch is not None:
            try:
                branch_abbr = get_branch_name(int(ntbrch) if isinstance(ntbrch, float) else ntbrch)
                branch_list.append(f"{branch_abbr} {int(ntbrch):03d}")
            except:
                branch_list.append("BR 000")
        else:
            branch_list.append("BR 000")

    df_npla = df_npla.with_columns([
        pl.Series('BRANCH', branch_list)
    ]).select(['NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'])
    
    # Standardize final NPLA schema
    df_npla = standardize_schema(df_npla, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'IIS': pl.Float64,
        'OI': pl.Float64,
        'TOTIIS': pl.Float64,
        'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
else:
    raise Exception("Failed to read LOAN.LNNOTE")

print(f"\n  NPLA rows: {df_npla.height}\n")

# Check if NPLA has data
if df_npla.height == 0:
    print("WARNING: No active accounts found (BORSTAT='A'). Exiting.")
    sys.exit(0)

# ============================================================================
# STEP 3: Read IIS and SP data
# ============================================================================
print("STEP 3: Reading IIS and SP data...")
df_iis = read_sas7bdat_fast(
    f'{NPL_DIR}iis.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)
if df_iis is not None:
    df_iis = standardize_schema(df_iis, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'IIS': pl.Float64,
        'OI': pl.Float64,
        'TOTIIS': pl.Float64,
        'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    df_iis = df_iis.unique(subset=['ACCTNO', 'NOTENO'])

df_sp = read_sas7bdat_fast(
    f'{NPL_DIR}sp2.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)
if df_sp is not None:
    df_sp = standardize_schema(df_sp, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'IIS': pl.Float64,
        'OI': pl.Float64,
        'TOTIIS': pl.Float64,
        'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    df_sp = df_sp.unique(subset=['ACCTNO', 'NOTENO'])

print(f"  IIS rows: {df_iis.height if df_iis is not None else 0}")
print(f"  SP rows: {df_sp.height if df_sp is not None else 0}")

# Merge IIS and SP
if df_iis is not None and df_sp is not None and df_iis.height > 0 and df_sp.height > 0:
    # Use 'full' instead of 'outer' (deprecated)
    df_npl_data = df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='full').select([
        'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'
    ])
else:
    df_npl_data = pl.DataFrame()

print(f"\n")

# ============================================================================
# STEP 4: Combine NPL data
# ============================================================================
print("STEP 4: Combining NPL data...")

# Ensure both dataframes have consistent schemas before concatenation
if df_npl_data.height > 0:
    # Standardize NPL data schema
    df_npl_data = standardize_schema(df_npl_data, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'IIS': pl.Float64,
        'OI': pl.Float64,
        'TOTIIS': pl.Float64,
        'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    
    # Concatenate
    try:
        df_npl = pl.concat([df_npla, df_npl_data])
    except Exception as e:
        print(f"  Error in concat: {e}")
        print(f"  df_npla schema: {df_npla.schema}")
        print(f"  df_npl_data schema: {df_npl_data.schema}")
        # If concat fails, use just the NPLA data
        df_npl = df_npla.clone()
else:
    df_npl = df_npla.clone()

# Continue with processing
if df_npl.height > 0:
    df_npl = df_npl.with_columns([
        pl.col('MARKETVL').fill_null(0).round(2),
        pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
        pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
    ]).unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_npl = df_npla.clone()

print(f"  NPL combined rows: {df_npl.height}\n")

# ============================================================================
# STEP 5: Read CCRIS credit submission data
# ============================================================================
print("STEP 5: Reading CCRIS data...")
ccris_file = f'{CCRIS_DIR}icredmsubac{reptmon}{reptyear}.sas7bdat'
df_credsub = read_sas7bdat_fast(
    ccris_file,
    columns=['ACCTNUM', 'NOTENO', 'DAYSARR', 'FACILITY'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)
if df_credsub is not None:
    df_credsub = standardize_schema(df_credsub, {
        'ACCTNUM': pl.Float64,
        'NOTENO': pl.Float64,
        'DAYSARR': pl.Float64
    })
    df_credsub = df_credsub.filter(
        pl.col('FACILITY').is_in(['34331', '34332'])
    ).rename({
        'ACCTNUM': 'ACCTNO',
        'DAYSARR': 'DAYS'
    }).sort(['ACCTNO', 'NOTENO', 'DAYS'], descending=[False, False, True]).unique(
        subset=['ACCTNO', 'NOTENO']
    ).select(['ACCTNO', 'NOTENO', 'DAYS', 'FACILITY'])
else:
    df_credsub = pl.DataFrame()

print(f"  CCRIS rows: {df_credsub.height}\n")

# ============================================================================
# STEP 6: Read loan data for HPD types
# ============================================================================
print("STEP 6: Reading HPD loan data...")
df_loan_raw = read_sas7bdat_fast(
    f'{LOAN_DIR}lnnote.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'LOANTYPE'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)
if df_loan_raw is not None:
    df_loan_raw = standardize_schema(df_loan_raw, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'LOANTYPE': pl.Float64
    })
    df_loan_raw = df_loan_raw.filter(
        pl.col('LOANTYPE').is_in(HPD)
    ).unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_loan_raw = pl.DataFrame()

print(f"  HPD loan rows: {df_loan_raw.height}\n")

# ============================================================================
# STEP 7: Merge data and create loan dataset
# ============================================================================
print("STEP 7: Merging data...")
if df_npl.height > 0:
    df_loan = df_npl.join(df_credsub, on=['ACCTNO', 'NOTENO'], how='left').join(
        df_loan_raw, on=['ACCTNO', 'NOTENO'], how='left', suffix='_loan'
    ).filter(pl.col('ACCTNO').is_not_null())
else:
    df_loan = pl.DataFrame()

print(f"  Merged loan rows: {df_loan.height}\n")

# ============================================================================
# STEP 8: Calculate derived fields (using polars for speed)
# ============================================================================
print("STEP 8: Calculating derived fields...")
start_time = time.time()

if df_loan.height > 0:
    # Read additional columns needed for calculations
    df_extra = read_sas7bdat_fast(
        f'{LOAN_DIR}lnnote.sas7bdat',
        columns=['ACCTNO', 'NOTENO', 'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2',
                 'FEEAMTA', 'FEEAMT5', 'ECSRRSRV', 'MATUREDT', 'LASTTRAN', 'DAYS',
                 'SCORE2', 'CONTRTYPE', 'NETPROC', 'APPVALUE', 'BIRTHDT', 'ORGBAL',
                 'CURBAL', 'PAYAMT', 'NACOSPADT', 'BALANCE', 'GUAREND', 'ISSXDTE',
                 'COLLDESC', 'COLLYEAR', 'DELQCD', 'CP', 'MODELDES', 'AKPK_STATUS',
                 'BORSTAT', 'LOANTYPE', 'PAIDIND', 'POSTNTRN', 'INTAMT', 'INTEARN4',
                 'CUSTCODE', 'LSTTRNCD', 'MARKETVL'],
        row_limit=TEST_LIMIT if TEST_MODE else None
    )
    
    if df_extra is not None:
        df_extra = standardize_schema(df_extra, {
            'ACCTNO': pl.Float64,
            'NOTENO': pl.Float64,
            'FEETOTAL': pl.Float64,
            'NFEEAMT5': pl.Float64,
            'FEEAMT3': pl.Float64,
            'FEETOT2': pl.Float64,
            'FEEAMTA': pl.Float64,
            'FEEAMT5': pl.Float64,
            'ECSRRSRV': pl.Float64,
            'NETPROC': pl.Float64,
            'APPVALUE': pl.Float64,
            'ORGBAL': pl.Float64,
            'CURBAL': pl.Float64,
            'PAYAMT': pl.Float64,
            'BALANCE': pl.Float64,
            'INTAMT': pl.Float64,
            'INTEARN4': pl.Float64,
            'CUSTCODE': pl.Float64,
            'LSTTRNCD': pl.Float64,
            'MARKETVL': pl.Float64
        })
        df_loan = df_loan.join(df_extra, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Use polars expressions for faster calculations
    df_loan = df_loan.with_columns([
        (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0)).alias('POSTAMT'),
        (pl.col('FEEAMT3').fill_null(0) - (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0))).alias('OTHERAMT'),
        (pl.col('FEETOT2').fill_null(0) - pl.col('FEEAMTA').fill_null(0) + pl.col('FEEAMT5').fill_null(0)).alias('OIFEEAMT'),
        pl.when(pl.col('ECSRRSRV').fill_null(0) <= 0).then(0.0).otherwise(pl.col('ECSRRSRV')).alias('ECSRRSRV'),
        pl.when(pl.col('ECSRRSRV').fill_null(0) > 0).then('Y').otherwise('N').alias('ECSRIND'),
        ((pl.col('ORGBAL').fill_null(0) - pl.col('CURBAL').fill_null(0)) / pl.when(pl.col('PAYAMT').fill_null(0) > 0).then(pl.col('PAYAMT')).otherwise(1)).cast(pl.Int64).alias('BILPAID'),
        pl.when(pl.col('NACOSPADT').fill_null(0) > 0).then('Y').otherwise('N').alias('PAY75PCT'),
    ])

    # Apply MTHPDUE using format_mthpass
    df_loan = df_loan.with_columns([
        pl.col('DAYS').map_elements(lambda x: format_mthpass(x) if x and x > 0 else 0, return_dtype=pl.Int64).alias('MTHPDUE')
    ])
    
    # CRRGRADE
    df_loan = df_loan.with_columns([
        (pl.col('SCORE2').fill_null('').cast(pl.Utf8) + pl.col('CONTRTYPE').fill_null('').cast(pl.Utf8)).alias('CRRGRADE')
    ])
    
    # MARGINFI
    df_loan = df_loan.with_columns([
        pl.when(pl.col('APPVALUE').fill_null(0) > 0)
        .then((pl.col('NETPROC').fill_null(0) / pl.col('APPVALUE')).round(2))
        .otherwise(0)
        .alias('MARGINFI')
    ])

elapsed = time.time() - start_time
print(f"  Calculations completed in {elapsed:.1f}s")
print(f"  Loan records: {df_loan.height}\n")

# ============================================================================
# STEP 9: Read customer names
# ============================================================================
print("STEP 9: Reading customer names...")
df_cname = read_sas7bdat_fast(
    f'{CISNAME_DIR}loan.sas7bdat',
    columns=['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC', 'SECCUST'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)
if df_cname is not None:
    df_cname = standardize_schema(df_cname, {'ACCTNO': pl.Float64})
    df_cname = df_cname.filter(
        pl.col('SECCUST') == '901'
    ).select(['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC']).unique(subset=['ACCTNO'])
else:
    df_cname = pl.DataFrame()

print(f"  Customer names: {df_cname.height}\n")

# ============================================================================
# STEP 10: Read guarantor information
# ============================================================================
print("STEP 10: Reading guarantor information...")
guarantor_data = {}
df_liab = read_sas7bdat_fast(
    f'{LOAN_DIR}lnliab07226.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'LIABACCT', 'LIABNAME'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)

if df_liab is not None and df_cname.height > 0:
    df_liab = standardize_schema(df_liab, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'LIABACCT': pl.Float64
    })
    df_liab = df_liab.sort('LIABACCT')
    
    df_liab = df_liab.join(
        df_cname.rename({'ACCTNO': 'LIABACCT', 'CUSTNAM1': 'GNAME'}),
        on='LIABACCT',
        how='left'
    ).with_columns([
        pl.when(pl.col('GNAME').is_null() | (pl.col('GNAME') == ''))
        .then(pl.col('LIABNAME'))
        .otherwise(pl.col('GNAME'))
        .alias('GNAME')
    ]).sort(['ACCTNO', 'NOTENO'])
    
    # Transpose guarantor names - limit to first 2 guarantors
    for (acctno, noteno), group in df_liab.group_by(['ACCTNO', 'NOTENO']):
        gnames = group['GNAME'].to_list()
        guarantor_data[(acctno, noteno)] = {
            'GUARNAM1': gnames[0] if len(gnames) > 0 else '',
            'GUARNAM2': gnames[1] if len(gnames) > 1 else ''
        }

print(f"  Guarantor entries: {len(guarantor_data)}\n")

# ============================================================================
# STEP 11: Get previous balance from SASLN
# ============================================================================
print("STEP 11: Reading previous balance...")
sasln_file = f'{SASLN_DIR}loan{reptmon1}{nowks}.sas7bdat'
df_sasln = read_sas7bdat_fast(
    sasln_file,
    columns=['ACCTNO', 'NOTENO', 'CURBAL'],
    row_limit=TEST_LIMIT if TEST_MODE else None
)

if df_sasln is not None:
    df_sasln = standardize_schema(df_sasln, {
        'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64,
        'CURBAL': pl.Float64
    })
    df_sasln = df_sasln.rename({'CURBAL': 'PREVBAL'}).sort(['ACCTNO', 'NOTENO'])
    df_sasln = df_sasln.join(df_npl.select(['ACCTNO', 'NOTENO']), on=['ACCTNO', 'NOTENO'], how='inner')
else:
    df_sasln = pl.DataFrame()

print(f"  SASLN rows: {df_sasln.height}\n")

# ============================================================================
# STEP 12: Final merge and filtering
# ============================================================================
print("STEP 12: Final merge and filtering...")
if df_sasln.height > 0 and df_loan.height > 0:
    # Ensure consistent schema before joining
    df_sasln = standardize_schema(df_sasln, {'ACCTNO': pl.Float64, 'NOTENO': pl.Float64})
    df_loan = standardize_schema(df_loan, {'ACCTNO': pl.Float64, 'NOTENO': pl.Float64})
    
    df_woff = df_sasln.join(df_loan, on=['ACCTNO', 'NOTENO'], how='full')
    if 'BRANCH' not in df_woff.columns:
        df_woff = df_woff.join(df_npl.select(['ACCTNO', 'BRANCH', 'MARKETVL']), on='ACCTNO', how='left')
    
    df_woff = df_woff.with_columns([
        (pl.col('CURBAL').fill_null(0) - pl.col('PREVBAL').fill_null(0)).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit('I').alias('RIND')
    ])
else:
    df_woff = pl.DataFrame()

print(f"  WOFF before filter: {df_woff.height}")

# Apply write-off criteria
if df_woff.height > 0:
    # Ensure required columns exist
    for col in ['BORSTAT', 'DAYS', 'LOANTYPE', 'PAIDIND', 'TOTAL']:
        if col not in df_woff.columns:
            df_woff = df_woff.with_columns(pl.lit(None).alias(col))
    
    df_woff = df_woff.with_columns([
        pl.col('BORSTAT').cast(pl.Utf8),
        pl.col('DAYS').cast(pl.Float64),
        pl.col('LOANTYPE').cast(pl.Float64),
        pl.col('PAIDIND').cast(pl.Utf8),
        pl.col('TOTAL').cast(pl.Float64)
    ])
    
    df_woff = df_woff.filter(
        (
            ((pl.col('BORSTAT').is_in(['F', 'I'])) & (pl.col('DAYS') >= 334)) |
            (pl.col('DAYS') >= 334) |
            (
                (pl.col('BORSTAT') == 'A') &
                ~pl.col('LOANTYPE').is_in([983, 993, 678, 679, 698, 699]) &
                (pl.col('PAIDIND') != 'P')
            )
        ) &
        (pl.col('TOTAL') != 0)
    ).with_columns([
        pl.lit('Y').alias('CONFIRM')
    ]).sort('ACCTNO')
    
    # Add customer names
    if 'NAME' not in df_woff.columns and df_cname.height > 0:
        df_woff = df_woff.join(
            df_cname.rename({'CUSTNAM1': 'NAME'}),
            on='ACCTNO',
            how='left'
        )

print(f"  WOFF after filter: {df_woff.height}\n")

# ============================================================================
# STEP 13: Save outputs
# ============================================================================
if df_woff.height > 0:
    # Save to NPL.LIST
    write_sas7bdat(df_woff, f'{NPL_DIR}LIST.sas7bdat')
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Accounts identified for write-off: {len(df_woff)}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
    
    # Write fixed-width output file
    print(f"\nWriting output files...")
    with open(OUTPUT_FILE1, 'w') as f:
        for idx, row in enumerate(df_woff.iter_rows(named=True)):
            if idx % 100 == 0 and idx > 0:
                print(f"  Writing record {idx}/{len(df_woff)}", end="\r", flush=True)
            
            branch = (row.get('BRANCH', '') or '')[:7]
            name = (row.get('NAME', '') or '')[:40]
            acctno = row.get('ACCTNO', 0) or 0
            noteno = row.get('NOTENO', 0) or 0
            borstat = (row.get('BORSTAT', '') or '')[:1]
            iis = row.get('IIS', 0) or 0
            oi = row.get('OI', 0) or 0
            totiis = row.get('TOTIIS', 0) or 0
            sp = row.get('SP', 0) or 0
            curbal = row.get('CURBAL', 0) or 0
            prevbal = row.get('PREVBAL', 0) or 0
            payment = row.get('PAYMENT', 0) or 0
            ecsrrsrv = row.get('ECSRRSRV', 0) or 0
            postamt = row.get('POSTAMT', 0) or 0
            otheramt = row.get('OTHERAMT', 0) or 0
            matdate = (row.get('MATDATE', '') or '')[:10]
            loantype = row.get('LOANTYPE', 0) or 0
            intamt = row.get('INTAMT', 0) or 0
            postntrn = (row.get('POSTNTRN', '') or '')[:1]
            marketvl = row.get('MARKETVL', 0) or 0
            intearn4 = row.get('INTEARN4', 0) or 0
            days = row.get('DAYS', 0) or 0
            custcode = row.get('CUSTCODE', 0) or 0
            rind = (row.get('RIND', '') or '')[:1]
            oifeeamt = row.get('OIFEEAMT', 0) or 0
            lasttra1 = (row.get('LASTTRA1', '') or '')[:10]
            lsttrncd = row.get('LSTTRNCD', 0) or 0
            mthpdue = row.get('MTHPDUE', 0) or 0
            balance = row.get('BALANCE', 0) or 0
            guarend = (row.get('GUAREND', '') or '')[:20]
            guarnam1 = (row.get('GUARNAM1', '') or '')[:40]
            guarnam2 = (row.get('GUARNAM2', '') or '')[:40]
            
            issxdte = row.get('ISSXDTE', '')
            if issxdte:
                try:
                    issxdte_str = format_mmddyy10(issxdte)[:10]
                except:
                    issxdte_str = ' ' * 10
            else:
                issxdte_str = ' ' * 10
            
            netproc = row.get('NETPROC', 0) or 0
            colldesc = (row.get('COLLDESC', '') or '')[:70]
            collyear = row.get('COLLYEAR', 0) or 0
            bilpaid = row.get('BILPAID', 0) or 0
            crrgrade = (row.get('CRRGRADE', '') or '')[:5]
            marginfi = row.get('MARGINFI', 0) or 0
            noteterm = row.get('NOTETERM', 0) or 0
            payamt = row.get('PAYAMT', 0) or 0
            
            dobmni = row.get('DOBMNI', '')
            if dobmni:
                try:
                    dobmni_str = format_mmddyy10(dobmni)[:10]
                except:
                    dobmni_str = ' ' * 10
            else:
                dobmni_str = ' ' * 10
            
            ecsrind = (row.get('ECSRIND', '') or '')[:1]
            delqcd = (row.get('DELQCD', '') or '')[:2]
            occupat = (row.get('OCCUPAT', '') or '')[:3]
            bgc = (row.get('BGC', '') or '')[:2]
            pay75pct = (row.get('PAY75PCT', '') or '')[:1]
            nacodate = (row.get('NACODATE', '') or '')[:10]
            cp = (row.get('CP', '') or '')[:1]
            modeldes = (row.get('MODELDES', '') or '')[:6]
            akpk_status = (row.get('AKPK_STATUS', '') or '')[:9]
            
            f.write(f"{branch:<7}{name:<40}{acctno:>10.0f}{noteno:>5.0f}{borstat:1}")
            f.write(f"{iis:>16.2f}{oi:>16.2f}{totiis:>16.2f}{sp:>16.2f}")
            f.write(f"{curbal:>16.2f}{prevbal:>16.2f}{payment:>16.2f}")
            f.write(f"{ecsrrsrv:>16.2f}{postamt:>16.2f}{otheramt:>16.2f}")
            f.write(f"{matdate:10}{loantype:>3.0f}{intamt:>16.2f}{postntrn:1}")
            f.write(f"{marketvl:>16.2f}{intearn4:>16.2f}{days:>6.0f}{custcode:>3.0f}{rind:1}")
            f.write(f"{oifeeamt:>16.2f}{lasttra1:10}{lsttrncd:>3.0f}{mthpdue:>3.0f}")
            f.write(f"{balance:>16.2f}{guarend:20}{guarnam1:40}{guarnam2:40}")
            f.write(f"{issxdte_str:10}{netproc:>16.2f}{colldesc:70}{collyear:>4.0f}")
            f.write(f"{bilpaid:>3.0f}{crrgrade:5}{marginfi:>16.2f}{noteterm:>3.0f}")
            f.write(f"{payamt:>16.2f}{dobmni_str:10}{ecsrind:1}{delqcd:2}")
            f.write(f"{occupat:3}{bgc:2}{pay75pct:1}{nacodate:10}{cp:1}")
            f.write(f"{modeldes:6}{akpk_status:9}\n")
    
    print(f"\n  {OUTPUT_FILE1} written with {df_woff.height} rows")
    
    # Create final formatted output
    print("\nCreating final formatted output...")
    text_records = []
    with open(OUTPUT_FILE1, 'r') as f:
        for idx, line in enumerate(f):
            if idx % 100 == 0 and idx > 0:
                print(f"  Processing record {idx}/{df_woff.height}", end="\r", flush=True)
            if len(line) >= 372:
                record = {
                    'BRANCH': line[0:7].strip(),
                    'NAME': line[8:48].strip(),
                    'ACCTNO': float(line[49:59]) if line[49:59].strip() else 0,
                    'NOTENO': float(line[60:65]) if line[60:65].strip() else 0,
                    'BORSTAT': line[66:67] if len(line) > 66 else '',
                    'IIS': float(line[68:84]) if len(line) > 84 and line[68:84].strip() else 0,
                    'OI': float(line[84:100]) if len(line) > 100 and line[84:100].strip() else 0,
                    'TOTIIS': float(line[100:116]) if len(line) > 116 and line[100:116].strip() else 0,
                    'BALANCE': float(line[356:372]) if len(line) > 372 and line[356:372].strip() else 0
                }
                record['SP'] = record['BALANCE'] - record['TOTIIS']
                record['TOTAL'] = record['TOTIIS'] + record['SP']
                record['_LINE'] = line
                text_records.append(record)
    
    if text_records:
        df_text = pl.DataFrame(text_records)
        with open(OUTPUT_FILE, 'w') as f:
            for idx, row in enumerate(df_text.iter_rows(named=True)):
                if idx % 100 == 0 and idx > 0:
                    print(f"  Writing final record {idx}/{len(text_records)}", end="\r", flush=True)
                line = row['_LINE']
                delqcd = line[676:678] if len(line) > 678 else '  '
                occupat = line[712:715] if len(line) > 715 else '999'
                bgc = line[742:744] if len(line) > 744 else '  '
                delqdes = get_delq_desc(delqcd)
                occupdes = get_occup_desc(occupat)
                bgcdes = get_bgc_desc(bgc)
                biztype, cap, latechg = 'I', 0.0, row['OI']
                sp_calc, total_calc = row['SP'], row['TOTAL']
                
                if len(line) >= 116:
                    f.write(line[:116])
                    f.write(f"{sp_calc:>16.2f}")
                    f.write(f"{total_calc:>16.2f}")
                    if len(line) > 373:
                        f.write(line[148:373])
                        f.write(f"{cap:>16.2f}")
                        f.write(f"{latechg:>16.2f}")
                        f.write(line[407:679] if len(line) > 679 else line[407:])
                        f.write(f"{delqdes:30}")
                        f.write(f"{biztype:1}")
                        f.write(line[712:715] if len(line) > 715 else '   ')
                        f.write(f"{occupdes:25}")
                        f.write(line[742:744] if len(line) > 744 else '  ')
                        f.write(f"{bgcdes:20}")
                        f.write(line[766:] if len(line) > 766 else '')
                        f.write('\n')
        
        write_sas7bdat(df_text, f'{NPL_DIR}WOFFTXT.sas7bdat')
        print(f"\n  {OUTPUT_FILE} written with {len(text_records)} rows")
        print(f"  {NPL_DIR}WOFFTXT.sas7bdat written")

else:
    print("\nNo accounts identified for write-off")

# ============================================================================
# Summary
# ============================================================================
total_elapsed = time.time() - start_total
print(f"\n{'='*60}")
print(f"COMPLETED IN {total_elapsed:.1f} SECONDS")
print(f"{'='*60}")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
if df_woff.height > 0:
    print(f"  {NPL_DIR}LIST.sas7bdat (Data file)")
    print(f"  {NPL_DIR}WOFFTXT.sas7bdat (Final dataset)")
