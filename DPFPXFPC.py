"""
EIIFTXT2 - Bad Debt Write-Off List (Filtered by NPL.LIST)

Key difference from EIIFTXT1: 
- Filters accounts by existing NPL.LIST file (only accounts previously identified for write-off)
- No LOANTYPE exclusion in NPLA WHERE clause
- Uses SAS7BDAT input files via pyreadstat
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

OUTPUT_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'wofftext.txt')
OUTPUT_FILE1 = os.path.join(OUTPUT_DIR, 'wofftex1.txt')

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

def read_sas7bdat(filepath, columns=None):
    """Read SAS dataset using pyreadstat."""
    try:
        if not os.path.exists(filepath):
            return None
        
        df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def write_sas7bdat(df, filepath):
    """Write DataFrame to SAS dataset using pyreadstat."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # pyreadstat uses write_sas7bdat
        pyreadstat.write_sas7bdat(df.to_pandas(), filepath)
        print(f"  Wrote: {os.path.basename(filepath)}")
    except AttributeError:
        # If write_sas7bdat doesn't exist, try using pandas to write
        try:
            import pandas as pd
            df.to_pandas().to_sas(filepath, format='sas7bdat')
            print(f"  Wrote (pandas): {os.path.basename(filepath)}")
        except Exception as e:
            print(f"Error writing {filepath}: {e}")
    except Exception as e:
        print(f"Error writing {filepath}: {e}")

def get_delq_desc(delqcd):
    return format_delqdes(delqcd) if delqcd else 'NO LEGAL ACTION TAKEN'

def get_occup_desc(occupat):
    return OCCUPFMT.get(occupat if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    return BGCFMT.get(bgc if bgc else '  ', 'NOT SPECIFIED')

def standardize_schema(df, schema_dict):
    """Standardize column types to match expected schema."""
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
            except Exception:
                pass
    return df

def create_empty_dataframe_with_schema(schema):
    """Create an empty DataFrame with the specified schema."""
    return pl.DataFrame(schema=schema)

def calc_mthpdue(days):
    """Calculate months past due from days."""
    if days is None or days <= 0:
        return 0
    result = format_mthpass(days)
    return int(result) if result else 0

# Set report date to yesterday
reptdate = datetime.now().date() - timedelta(days=1)
print(f"\n{'='*60}")
print(f"Bad Debt Write-Off List (Filtered by NPL.LIST)")
print(f"{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Output Directory: {OUTPUT_DIR}")
print(f"Output File 1: {OUTPUT_FILE1}")
print(f"Output File 2: {OUTPUT_FILE}")

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
print(f"{'='*60}\n")

start_total = time.time()

# ============================================================================
# STEP 1: Create NPLA - Active accounts with borrower status 'A'
# KEY DIFFERENCE: No LOANTYPE exclusion in WHERE clause
# ============================================================================
print("STEP 1: Reading NPLA data (Active accounts with BORSTAT='A')...")

loan_columns = ['BORSTAT', 'LOANTYPE', 'NAME', 'ACCTNO', 'NOTENO', 
                'FEEDUE', 'FEEDUEMS', 'FEEAMT16', 'MARKETVL', 'NTBRCH']
df_npla_raw = read_sas7bdat(f'{LOAN_DIR}lnnote.sas7bdat', columns=loan_columns)

if df_npla_raw is None:
    raise Exception("Failed to read LOAN.LNNOTE")

df_npla_raw = standardize_schema(df_npla_raw, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'FEEDUE': pl.Float64,
    'FEEDUEMS': pl.Float64, 'FEEAMT16': pl.Float64, 'MARKETVL': pl.Float64,
    'NTBRCH': pl.Float64
})

# KEY DIFFERENCE: No LOANTYPE exclusion
df_npla = df_npla_raw.filter(
    pl.col('BORSTAT') == 'A'
).with_columns([
    pl.lit(0.0).alias('IIS'),
    (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('OI'),
    (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('TOTIIS'),
    (pl.col('FEEDUEMS').fill_null(0) + pl.col('FEEAMT16').fill_null(0)).alias('SP')
])

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

df_npla = standardize_schema(df_npla, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
    'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64,
    'MARKETVL': pl.Float64
})

print(f"  NPLA rows: {df_npla.height}\n")

# ============================================================================
# STEP 2: Read IIS and SP data
# ============================================================================
print("STEP 2: Reading IIS and SP data...")

df_iis = read_sas7bdat(f'{NPL_DIR}iis.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'])
if df_iis is not None:
    df_iis = standardize_schema(df_iis, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    df_iis = df_iis.unique(subset=['ACCTNO', 'NOTENO'])

df_sp = read_sas7bdat(f'{NPL_DIR}sp2.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'])
if df_sp is not None:
    df_sp = standardize_schema(df_sp, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    df_sp = df_sp.unique(subset=['ACCTNO', 'NOTENO'])

print(f"  IIS rows: {df_iis.height if df_iis is not None else 0}")
print(f"  SP rows: {df_sp.height if df_sp is not None else 0}")

# Merge IIS and SP
if df_iis is not None and df_sp is not None and df_iis.height > 0 and df_sp.height > 0:
    df_npl_data = df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='full').select([
        'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'
    ])
else:
    df_npl_data = create_empty_dataframe_with_schema({
        'NAME': pl.Utf8, 'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'IIS': pl.Float64, 'OI': pl.Float64, 'TOTIIS': pl.Float64,
        'SP': pl.Float64, 'MARKETVL': pl.Float64, 'BRANCH': pl.Utf8
    })

print()

# ============================================================================
# STEP 3: Combine NPL data
# ============================================================================
print("STEP 3: Combining NPL data...")

if df_npl_data.height > 0:
    df_npl_data = standardize_schema(df_npl_data, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64,
        'MARKETVL': pl.Float64
    })
    try:
        df_npl = pl.concat([df_npla, df_npl_data])
    except Exception:
        df_npl = df_npla.clone()
else:
    df_npl = df_npla.clone()

if df_npl.height > 0:
    df_npl = df_npl.with_columns([
        pl.col('MARKETVL').fill_null(0).round(2),
        pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
        pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
    ]).unique(subset=['ACCTNO', 'NOTENO'])

print(f"  NPL combined rows: {df_npl.height}\n")

# ============================================================================
# STEP 4: Read CCRIS credit submission data
# ============================================================================
print("STEP 4: Reading CCRIS data...")

ccris_file = f'{CCRIS_DIR}icredmsubac{reptmon}{reptyear}.sas7bdat'
print(f"  Looking for: {ccris_file}")

# Try different naming patterns for CCRIS file
ccris_file_patterns = [
    f'{CCRIS_DIR}icredmsubac{reptmon}{reptyear}.sas7bdat',
    f'{CCRIS_DIR}ICREDMSUBAC{reptmon}{reptyear}.sas7bdat',
    f'{CCRIS_DIR}icredmsubac{reptmon}0{reptyear}.sas7bdat',
    f'{CCRIS_DIR}ICREDMSUBAC{reptmon}0{reptyear}.sas7bdat',
]

df_credsub = None
for pattern in ccris_file_patterns:
    df_credsub = read_sas7bdat(pattern, columns=None)
    if df_credsub is not None:
        print(f"  Found CCRIS file: {os.path.basename(pattern)}")
        break

if df_credsub is not None and df_credsub.height > 0:
    # Find column names (case-insensitive)
    acct_col = None
    for col in df_credsub.columns:
        if col.lower() in ['acctnum', 'acctno']:
            acct_col = col
            break
    
    days_col = None
    for col in df_credsub.columns:
        if col.lower() in ['daysarr', 'days']:
            days_col = col
            break
    
    facility_col = None
    for col in df_credsub.columns:
        if col.lower() in ['facility']:
            facility_col = col
            break
    
    if acct_col:
        rename_map = {}
        if acct_col and acct_col != 'ACCTNO':
            rename_map[acct_col] = 'ACCTNO'
        if days_col and days_col != 'DAYS':
            rename_map[days_col] = 'DAYS'
        if facility_col and facility_col != 'FACILITY':
            rename_map[facility_col] = 'FACILITY'
        
        if rename_map:
            df_credsub = df_credsub.rename(rename_map)
        
        if 'FACILITY' in df_credsub.columns:
            df_credsub = df_credsub.filter(
                pl.col('FACILITY').is_in(['34331', '34332'])
            )
        
        select_cols = ['ACCTNO', 'NOTENO', 'DAYS', 'FACILITY']
        available_cols = [col for col in select_cols if col in df_credsub.columns]
        if available_cols:
            df_credsub = df_credsub.select(available_cols)
        
        if 'ACCTNO' in df_credsub.columns and 'NOTENO' in df_credsub.columns:
            df_credsub = df_credsub.sort(
                ['ACCTNO', 'NOTENO', 'DAYS'],
                descending=[False, False, True]
            ).unique(subset=['ACCTNO', 'NOTENO'])
        
        df_credsub = standardize_schema(df_credsub, {
            'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'DAYS': pl.Float64
        })
    else:
        df_credsub = create_empty_dataframe_with_schema({
            'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
            'DAYS': pl.Float64, 'FACILITY': pl.Utf8
        })
else:
    df_credsub = create_empty_dataframe_with_schema({
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'DAYS': pl.Float64, 'FACILITY': pl.Utf8
    })

print(f"  CCRIS rows: {df_credsub.height}\n")

# ============================================================================
# STEP 5: Read loan data for HPD types
# ============================================================================
print("STEP 5: Reading HPD loan data...")

df_loan_raw = read_sas7bdat(f'{LOAN_DIR}lnnote.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'LOANTYPE'])
if df_loan_raw is not None:
    df_loan_raw = standardize_schema(df_loan_raw, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'LOANTYPE': pl.Float64
    })
    df_loan_raw = df_loan_raw.filter(
        pl.col('LOANTYPE').is_in(HP_ACTIVE)
    ).unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_loan_raw = create_empty_dataframe_with_schema({
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'LOANTYPE': pl.Float64
    })

print(f"  HPD loan rows: {df_loan_raw.height}\n")

# ============================================================================
# STEP 6: Merge data and create loan dataset
# ============================================================================
print("STEP 6: Merging data...")

if df_npl.height > 0:
    df_loan = df_npl.clone()
    
    if df_credsub.height > 0:
        df_loan = df_loan.join(df_credsub, on=['ACCTNO', 'NOTENO'], how='left')
    else:
        df_loan = df_loan.with_columns([
            pl.lit(None).cast(pl.Float64).alias('DAYS'),
            pl.lit(None).cast(pl.Utf8).alias('FACILITY')
        ])
    
    if df_loan_raw.height > 0:
        df_loan = df_loan.join(df_loan_raw, on=['ACCTNO', 'NOTENO'], how='left', suffix='_loan')
    else:
        df_loan = df_loan.with_columns([
            pl.lit(None).cast(pl.Float64).alias('LOANTYPE')
        ])
    
    df_loan = df_loan.filter(pl.col('ACCTNO').is_not_null())
else:
    df_loan = df_npl.clone()

print(f"  Merged loan rows: {df_loan.height}\n")

# ============================================================================
# STEP 7: Calculate derived fields
# ============================================================================
print("STEP 7: Calculating derived fields...")
start_time = time.time()

if df_loan.height > 0:
    # Read additional columns
    extra_columns = ['ACCTNO', 'NOTENO', 'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2',
                     'FEEAMTA', 'FEEAMT5', 'ECSRRSRV', 'MATUREDT', 'LASTTRAN',
                     'SCORE2', 'CONTRTYPE', 'NETPROC', 'APPVALUE', 'BIRTHDT', 'ORGBAL',
                     'CURBAL', 'PAYAMT', 'NACOSPADT', 'BALANCE', 'GUAREND', 'ISSXDTE',
                     'COLLDESC', 'COLLYEAR', 'DELQCD', 'CP', 'MODELDES', 'AKPK_STATUS',
                     'BORSTAT', 'PAIDIND', 'POSTNTRN', 'INTAMT', 'INTEARN4',
                     'CUSTCODE', 'LSTTRNCD']
    
    df_extra = read_sas7bdat(f'{LOAN_DIR}lnnote.sas7bdat', columns=extra_columns)
    
    if df_extra is not None:
        numeric_cols = ['ACCTNO', 'NOTENO', 'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2',
                        'FEEAMTA', 'FEEAMT5', 'ECSRRSRV', 'NETPROC', 'APPVALUE',
                        'ORGBAL', 'CURBAL', 'PAYAMT', 'BALANCE', 'INTAMT', 'INTEARN4',
                        'CUSTCODE', 'LSTTRNCD']
        for col in numeric_cols:
            if col in df_extra.columns:
                df_extra = df_extra.with_columns(pl.col(col).cast(pl.Float64))
        
        df_loan = df_loan.join(df_extra, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Add missing columns if needed
    for col in ['FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2', 'FEEAMTA', 'FEEAMT5',
                'ECSRRSRV', 'ORGBAL', 'CURBAL', 'PAYAMT', 'NACOSPADT', 'SCORE2',
                'CONTRTYPE', 'NETPROC', 'APPVALUE']:
        if col not in df_loan.columns:
            df_loan = df_loan.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    
    # Calculate derived fields
    try:
        df_loan = df_loan.with_columns([
            (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0)).alias('POSTAMT')
        ])
        df_loan = df_loan.with_columns([
            (pl.col('FEEAMT3').fill_null(0) - pl.col('POSTAMT')).alias('OTHERAMT')
        ])
        df_loan = df_loan.with_columns([
            (pl.col('FEETOT2').fill_null(0) - pl.col('FEEAMTA').fill_null(0) + pl.col('FEEAMT5').fill_null(0)).alias('OIFEEAMT')
        ])
    except Exception:
        df_loan = df_loan.with_columns([
            pl.lit(0.0).alias('POSTAMT'),
            pl.lit(0.0).alias('OTHERAMT'),
            pl.lit(0.0).alias('OIFEEAMT')
        ])
    
    # ECSRRSRV
    try:
        df_loan = df_loan.with_columns([
            pl.col('ECSRRSRV').map_elements(
                lambda x: 0.0 if x is None or x <= 0 else float(x),
                return_dtype=pl.Float64
            ).alias('ECSRRSRV')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit(0.0).alias('ECSRRSRV')])
    
    # ECSRIND
    try:
        df_loan = df_loan.with_columns([
            pl.col('ECSRRSRV').map_elements(
                lambda x: 'Y' if x is not None and x > 0 else 'N',
                return_dtype=pl.Utf8
            ).alias('ECSRIND')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit('N').alias('ECSRIND')])
    
    # BILPAID
    try:
        def calc_bilpaid(orgbal, curbal, payamt):
            if payamt and payamt > 0:
                return int((orgbal - curbal) / payamt)
            return 0
        
        df_loan = df_loan.with_columns([
            pl.struct(['ORGBAL', 'CURBAL', 'PAYAMT'])
            .map_elements(lambda x: calc_bilpaid(x['ORGBAL'] or 0, x['CURBAL'] or 0, x['PAYAMT'] or 0),
                         return_dtype=pl.Int64)
            .alias('BILPAID')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit(0).alias('BILPAID')])
    
    # PAY75PCT
    try:
        df_loan = df_loan.with_columns([
            pl.col('NACOSPADT').map_elements(
                lambda x: 'Y' if x and x > 0 else 'N',
                return_dtype=pl.Utf8
            ).alias('PAY75PCT')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit('N').alias('PAY75PCT')])
    
    # MTHPDUE
    if 'DAYS' in df_loan.columns:
        try:
            df_loan = df_loan.with_columns([
                pl.col('DAYS').map_elements(calc_mthpdue, return_dtype=pl.Int64).alias('MTHPDUE')
            ])
        except Exception:
            df_loan = df_loan.with_columns([pl.lit(0).alias('MTHPDUE')])
    else:
        df_loan = df_loan.with_columns([pl.lit(0).alias('MTHPDUE')])
    
    # CRRGRADE
    try:
        df_loan = df_loan.with_columns([
            (pl.col('SCORE2').fill_null('').cast(pl.Utf8) + 
             pl.col('CONTRTYPE').fill_null('').cast(pl.Utf8)).alias('CRRGRADE')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit('').alias('CRRGRADE')])
    
    # MARGINFI
    try:
        df_loan = df_loan.with_columns([
            pl.when(pl.col('APPVALUE').fill_null(0) > 0)
            .then((pl.col('NETPROC').fill_null(0) / pl.col('APPVALUE')).round(2))
            .otherwise(0)
            .alias('MARGINFI')
        ])
    except Exception:
        df_loan = df_loan.with_columns([pl.lit(0.0).alias('MARGINFI')])
    
    # Placeholder columns
    for col in ['MATDATE', 'LASTTRA1', 'NACODATE']:
        if col not in df_loan.columns:
            df_loan = df_loan.with_columns([pl.lit('').alias(col)])
    if 'DOBMNI' not in df_loan.columns:
        df_loan = df_loan.with_columns([pl.lit(None).cast(pl.Utf8).alias('DOBMNI')])

elapsed = time.time() - start_time
print(f"  Calculations completed in {elapsed:.1f}s")
print(f"  Loan records: {df_loan.height}\n")

# ============================================================================
# STEP 8: Read customer names
# ============================================================================
print("STEP 8: Reading customer names...")

df_cname = read_sas7bdat(f'{CISNAME_DIR}loan.sas7bdat',
    columns=['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC', 'SECCUST'])

if df_cname is not None:
    df_cname = standardize_schema(df_cname, {'ACCTNO': pl.Float64})
    df_cname = df_cname.filter(
        pl.col('SECCUST') == '901'
    ).select(['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC']).unique(subset=['ACCTNO'])
else:
    df_cname = create_empty_dataframe_with_schema({
        'ACCTNO': pl.Float64, 'CUSTNAM1': pl.Utf8, 'OCCUPAT': pl.Utf8, 'BGC': pl.Utf8
    })

print(f"  Customer names: {df_cname.height}\n")

# ============================================================================
# STEP 9: Read guarantor information
# ============================================================================
print("STEP 9: Reading guarantor information...")

guarantor_data = {}
df_liab = read_sas7bdat(f'{LOAN_DIR}lnliab07226.sas7bdat',
    columns=['ACCTNO', 'NOTENO', 'LIABACCT', 'LIABNAME'])

if df_liab is not None and df_cname.height > 0:
    df_liab = standardize_schema(df_liab, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'LIABACCT': pl.Float64
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
    
    for (acctno, noteno), group in df_liab.group_by(['ACCTNO', 'NOTENO']):
        gnames = group['GNAME'].to_list()
        guarantor_data[(acctno, noteno)] = {
            'GUARNAM1': gnames[0] if len(gnames) > 0 else '',
            'GUARNAM2': gnames[1] if len(gnames) > 1 else ''
        }

print(f"  Guarantor entries: {len(guarantor_data)}\n")

# Add guarantor names to loan data
if df_loan.height > 0:
    guarnam1_list = []
    guarnam2_list = []
    for row in df_loan.iter_rows(named=True):
        key = (row['ACCTNO'], row['NOTENO'])
        gdata = guarantor_data.get(key, {'GUARNAM1': '', 'GUARNAM2': ''})
        guarnam1_list.append(gdata['GUARNAM1'])
        guarnam2_list.append(gdata['GUARNAM2'])
    
    df_loan = df_loan.with_columns([
        pl.Series('GUARNAM1', guarnam1_list),
        pl.Series('GUARNAM2', guarnam2_list)
    ])

# ============================================================================
# STEP 10: Get previous balance from SASLN
# ============================================================================
print("STEP 10: Reading previous balance...")

sasln_file = f'{SASLN_DIR}loan{reptmon1}{nowks}.sas7bdat'
df_sasln = read_sas7bdat(sasln_file, columns=['ACCTNO', 'NOTENO', 'CURBAL'])

if df_sasln is not None:
    df_sasln = standardize_schema(df_sasln, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'CURBAL': pl.Float64
    })
    df_sasln = df_sasln.rename({'CURBAL': 'PREVBAL'}).sort(['ACCTNO', 'NOTENO'])
    df_sasln = df_sasln.join(df_npl.select(['ACCTNO', 'NOTENO']), on=['ACCTNO', 'NOTENO'], how='inner')
else:
    df_sasln = create_empty_dataframe_with_schema({
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'PREVBAL': pl.Float64
    })

print(f"  SASLN rows: {df_sasln.height}\n")

# ============================================================================
# STEP 11: Final merge
# ============================================================================
print("STEP 11: Final merge...")

if df_sasln.height > 0 and df_loan.height > 0:
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
    df_woff = df_npl.clone()
    df_woff = df_woff.with_columns([
        pl.lit(0.0).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit('I').alias('RIND')
    ])

print(f"  WOFF before filtering by NPL.LIST: {df_woff.height}")

# ============================================================================
# STEP 12: KEY DIFFERENCE - Filter by NPL.LIST
# Only include accounts that are in the NPL.LIST file
# ============================================================================
print("STEP 12: Filtering by NPL.LIST...")

# Try different possible names for the LIST file
list_patterns = [
    f'{NPL_DIR}LIST.sas7bdat',
    f'{NPL_DIR}list.sas7bdat',
    f'{OUTPUT_DIR}LIST.sas7bdat',
    f'{OUTPUT_DIR}list.sas7bdat',
]

df_list = None
for pattern in list_patterns:
    df_list = read_sas7bdat(pattern, columns=['ACCTNO'])
    if df_list is not None:
        print(f"  Found NPL.LIST file: {os.path.basename(pattern)}")
        break

if df_list is not None and df_list.height > 0:
    df_list = df_list.unique(subset=['ACCTNO']).sort('ACCTNO')
    print(f"  NPL.LIST accounts: {df_list.height}")
    
    # Join to filter only accounts in LIST
    df_woff = df_woff.join(df_list, on='ACCTNO', how='inner')
    print(f"  WOFF after filtering by NPL.LIST: {df_woff.height}")
else:
    print("  WARNING: NPL.LIST file not found - no filtering applied")
    print("  WOFF after filtering by NPL.LIST: {df_woff.height} (no filter applied)")

print()

# ============================================================================
# STEP 13: Add customer names
# ============================================================================
if 'NAME' not in df_woff.columns and df_cname.height > 0:
    df_woff = df_woff.join(
        df_cname.rename({'CUSTNAM1': 'NAME'}),
        on='ACCTNO',
        how='left'
    )

# ============================================================================
# STEP 14: Save outputs
# ============================================================================
print("STEP 13: Saving output files...")

if df_woff.height > 0:
    # Save to SAS dataset (try multiple methods)
    try:
        # Try pyreadstat first
        pyreadstat.write_sas7bdat(df_woff.to_pandas(), f'{NPL_DIR}WOFFTXT.sas7bdat')
        print(f"  Wrote: WOFFTXT.sas7bdat (pyreadstat)")
    except AttributeError:
        # Try pandas to_sas
        try:
            import pandas as pd
            df_woff.to_pandas().to_sas(f'{NPL_DIR}WOFFTXT.sas7bdat', format='sas7bdat')
            print(f"  Wrote: WOFFTXT.sas7bdat (pandas)")
        except Exception as e:
            print(f"  Could not write SAS dataset: {e}")
            # Save as parquet as fallback
            df_woff.write_parquet(f'{NPL_DIR}WOFFTXT.parquet')
            print(f"  Saved as parquet: WOFFTXT.parquet")
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Accounts in filtered write-off list: {len(df_woff)}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
    
    # Write fixed-width output file
    print(f"\nWriting output files to: {OUTPUT_DIR}")
    
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
    print(f"  File size: {os.path.getsize(OUTPUT_FILE1):,} bytes")
    
    # Create final formatted output
    print("\nCreating final formatted output...")
    text_records = []
    with open(OUTPUT_FILE1, 'r') as f:
        for line in f:
            if len(line) >= 372:
                try:
                    # Safely parse the fixed-width fields
                    record = {
                        'BRANCH': line[0:7].strip(),
                        'NAME': line[8:48].strip(),
                        'ACCTNO': float(line[49:59].strip()) if line[49:59].strip() else 0,
                        'NOTENO': float(line[60:65].strip()) if line[60:65].strip() else 0,
                        'BORSTAT': line[66:67] if len(line) > 66 else '',
                        'IIS': float(line[68:84].strip()) if len(line) > 84 and line[68:84].strip() else 0,
                        'OI': float(line[84:100].strip()) if len(line) > 100 and line[84:100].strip() else 0,
                        'TOTIIS': float(line[100:116].strip()) if len(line) > 116 and line[100:116].strip() else 0,
                        'BALANCE': float(line[356:372].strip()) if len(line) > 372 and line[356:372].strip() else 0
                    }
                    record['SP'] = record['BALANCE'] - record['TOTIIS']
                    record['TOTAL'] = record['TOTIIS'] + record['SP']
                    record['_LINE'] = line
                    text_records.append(record)
                except ValueError as e:
                    # Skip invalid lines
                    continue
    
    if text_records:
        df_text = pl.DataFrame(text_records)
        with open(OUTPUT_FILE, 'w') as f:
            for row in df_text.iter_rows(named=True):
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
        
        try:
            pyreadstat.write_sas7bdat(df_text.to_pandas(), f'{NPL_DIR}WOFFTXT_FORMATTED.sas7bdat')
            print(f"\n  {OUTPUT_FILE} written with {len(text_records)} rows")
            print(f"  File size: {os.path.getsize(OUTPUT_FILE):,} bytes")
            print(f"  {NPL_DIR}WOFFTXT_FORMATTED.sas7bdat written")
        except:
            df_text.write_parquet(f'{NPL_DIR}WOFFTXT_FORMATTED.parquet')
            print(f"\n  {OUTPUT_FILE} written with {len(text_records)} rows")
            print(f"  File size: {os.path.getsize(OUTPUT_FILE):,} bytes")
            print(f"  {NPL_DIR}WOFFTXT_FORMATTED.parquet written (parquet fallback)")

else:
    print("\nNo accounts found in NPL.LIST filtering")
    print("Creating empty output files...")
    
    with open(OUTPUT_FILE1, 'w') as f:
        f.write("")
    print(f"  Created empty file: {OUTPUT_FILE1}")
    
    with open(OUTPUT_FILE, 'w') as f:
        f.write("")
    print(f"  Created empty file: {OUTPUT_FILE}")

# ============================================================================
# Summary
# ============================================================================
total_elapsed = time.time() - start_total
print(f"\n{'='*60}")
print(f"COMPLETED IN {total_elapsed:.1f} SECONDS")
print(f"{'='*60}")
print(f"\nKey Difference from EIIFTXT1:")
print(f"  - Filtered by existing NPL.LIST file")
print(f"  - No LOANTYPE exclusion in NPLA WHERE clause")
print(f"  - Only accounts previously identified for write-off")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
if df_woff.height > 0:
    print(f"  {NPL_DIR}WOFFTXT.sas7bdat or .parquet (Final dataset)")
print(f"\nTo view output files:")
print(f"  ls -la {OUTPUT_DIR}")
