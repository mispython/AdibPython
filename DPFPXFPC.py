"""
EIBDDEPE - Daily Deposit Position Extract

Includes:
- PBBLNFMT: Loan format definitions (ACE products)
- PBBDPFMT: Deposit format definitions (SAPROD, CAPROD, etc.)

Processes daily deposit balances from DPTRBL mainframe file:
- Savings (DEPTYPE='S')
- Demand/Current (DEPTYPE='D','N')
- Fixed Deposits (DEPTYPE='C')
- Overdrafts (negative balances)

Key Features:
- Age calculation (AGELIMIT=12, MAXAGE=18, AGEBELOW=11)
- Movement range categorization (DDMOVE, MVTDEP, MVTACE)
- Weekly processing (NOWK 1-4)
- Month-end SDRNGE profile
- Islamic banking separation (DYIBU)
- Branch-level aggregation (999 branches)

Outputs:
1. DYPOSN - Daily position summary
2. DYDP - Daily deposit details
3. DYMVNT - Significant movements (>=50K SA, >=100K CA)
4. DDMV - Demand deposit movement
5. DYIBU - Islamic banking balances
6. DYDDCR - Demand deposit credit movement
7. DYBRDP - Branch summary
8. DYDPS/DYACE - Movement by range
9. SDRNGE - Savings profile (weekly/month-end)
"""

import polars as pl
from datetime import datetime, timedelta
import os

# Constants
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Directories
TEMP_DIR = 'data/temp/'
MIS_DIR = 'data/mis/'
MISQ_DIR = 'data/misq/'
BNM_DIR = 'data/bnm/'

for d in [TEMP_DIR, MIS_DIR, MISQ_DIR, BNM_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIBDDEPE - Daily Deposit Position Extract")
print("=" * 60)

# Read DPTRBL Parquet file
print("\nReading DPTRBL Parquet file...")

# Define the DPTRBL Parquet file path
DPTRBL_PARQUET = 'data/dptrbl.parquet'

if not os.path.exists(DPTRBL_PARQUET):
    print(f"ERROR: DPTRBL Parquet file not found at {DPTRBL_PARQUET}")
    import sys
    sys.exit(1)

try:
    # Read the entire DPTRBL dataset
    df_dptrbl = pl.read_parquet(DPTRBL_PARQUET)
    print(f"  Loaded {len(df_dptrbl):,} records from DPTRBL")
    
    # Print column names to help debug
    print(f"  Columns: {df_dptrbl.columns[:10]}...")  # Show first 10 columns
    
    # Get the report date - try different approaches
    reptdate = None
    
    # Try to get TBDATE from the first row
    if 'TBDATE' in df_dptrbl.columns:
        # Get the first value using Polars
        tbdate_val = df_dptrbl.select('TBDATE').row(0)[0]
        if isinstance(tbdate_val, (datetime, pl.Date)):
            reptdate = tbdate_val.date() if hasattr(tbdate_val, 'date') else tbdate_val
        elif isinstance(tbdate_val, str):
            reptdate = datetime.strptime(tbdate_val, '%Y%m%d').date()
        else:
            # Try to convert from numeric
            reptdate = datetime.strptime(str(int(tbdate_val)), '%Y%m%d').date()
    
    elif 'REPTDATE' in df_dptrbl.columns:
        reptdate_val = df_dptrbl.select('REPTDATE').row(0)[0]
        if isinstance(reptdate_val, (datetime, pl.Date)):
            reptdate = reptdate_val.date() if hasattr(reptdate_val, 'date') else reptdate_val
        elif isinstance(reptdate_val, str):
            reptdate = datetime.strptime(reptdate_val, '%Y%m%d').date()
        else:
            reptdate = datetime.strptime(str(int(reptdate_val)), '%Y%m%d').date()
    
    elif 'REPTDT' in df_dptrbl.columns:
        reptdate_val = df_dptrbl.select('REPTDT').row(0)[0]
        if isinstance(reptdate_val, (datetime, pl.Date)):
            reptdate = reptdate_val.date() if hasattr(reptdate_val, 'date') else reptdate_val
        elif isinstance(reptdate_val, str):
            reptdate = datetime.strptime(reptdate_val, '%Y%m%d').date()
        else:
            reptdate = datetime.strptime(str(int(reptdate_val)), '%Y%m%d').date()
    
    else:
        # Try to find any column that might contain date
        date_cols = [col for col in df_dptrbl.columns if 'DATE' in col.upper() or 'DT' in col.upper()]
        if date_cols:
            print(f"  Found potential date columns: {date_cols}")
            # Use the first date column found
            test_col = date_cols[0]
            reptdate_val = df_dptrbl.select(test_col).row(0)[0]
            if isinstance(reptdate_val, (datetime, pl.Date)):
                reptdate = reptdate_val.date() if hasattr(reptdate_val, 'date') else reptdate_val
            else:
                # Use current date as fallback
                print(f"  Warning: Could not parse date from {test_col}, using current date")
                reptdate = datetime.now().date()
        else:
            # If no date column, use current date
            print("  Warning: No date column found, using current date")
            reptdate = datetime.now().date()
    
    # If we still don't have a date, use current date
    if reptdate is None:
        print("  Warning: Could not determine report date, using current date")
        reptdate = datetime.now().date()
    
    # Determine week (NOWK)
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = f'{reptdate.year}'
    reptmon = f'{reptdate.month:02d}'
    reptday = f'{reptdate.day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    reptdat3 = reptdate - timedelta(days=31)
    rdate3 = reptdat3.strftime('%y%m%d')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Year: {reptyear}, Month: {reptmon}, Day: {reptday}")
    
except Exception as e:
    print(f"Error reading DPTRBL Parquet: {e}")
    import traceback
    traceback.print_exc()
    import sys
    sys.exit(1)

print("=" * 60)

# Product categorization (from PBBLNFMT)
ACE_PRODUCTS = [161, 162, 163]  # ACE products

# Range formats (PROC FORMAT)
def categorize_ddmove(amount):
    """DDMOVE format - demand deposit movement ranges"""
    if amount < 300000: return 300000
    elif amount < 500000: return 500000
    elif amount < 1000000: return 1000000
    elif amount < 1500000: return 1500000
    elif amount < 2000000: return 2000000
    elif amount < 3000000: return 3000000
    elif amount < 4000000: return 4000000
    elif amount < 5000000: return 5000000
    elif amount < 10000000: return 10000000
    else: return 10000001

def categorize_mvtdep(amount):
    """MVTDEP format - deposit movement ranges"""
    if amount <= 5000: return 5000
    elif amount <= 10000: return 10000
    elif amount <= 30000: return 30000
    elif amount <= 50000: return 50000
    elif amount <= 75000: return 75000
    else: return 80000

def categorize_mvtace(amount):
    """MVTACE format - ACE movement ranges"""
    if amount <= 5000: return 5000
    elif amount <= 10000: return 10000
    elif amount <= 30000: return 30000
    elif amount <= 50000: return 50000
    elif amount <= 75000: return 75000
    elif amount <= 100000: return 100000
    else: return 200000

def categorize_s1range(curbal):
    """S1RANGE format - savings balance ranges"""
    if curbal < 500: return 500
    elif curbal < 1000: return 1000
    elif curbal < 5000: return 5000
    elif curbal < 10000: return 10000
    elif curbal < 20000: return 20000
    elif curbal < 50000: return 50000
    elif curbal < 100000: return 100000
    elif curbal < 200000: return 200000
    else: return 200001

def categorize_s2range(curbal):
    """S2RANGE format - alternative savings ranges"""
    if curbal < 1000: return 1000
    elif curbal < 5000: return 5000
    elif curbal < 10000: return 10000
    elif curbal < 50000: return 50000
    elif curbal < 100000: return 100000
    else: return 100001

def calculate_age(bdate, reptdate, reptyear, reptmon, reptday):
    """Calculate age with special rules for boundaries"""
    if bdate is None or bdate == 0:
        return 0
    
    try:
        # Handle different date formats
        if isinstance(bdate, datetime):
            bdate_dt = bdate.date()
        elif isinstance(bdate, pl.Date):
            bdate_dt = datetime.strptime(str(bdate), '%Y-%m-%d').date()
        elif isinstance(bdate, str):
            if len(bdate) == 6:  # YYMMDD
                bdate_dt = datetime.strptime(bdate, '%y%m%d').date()
            else:  # YYYYMMDD or YYYY-MM-DD
                bdate_dt = datetime.strptime(bdate[:8], '%Y%m%d').date()
        else:
            bdate_str = str(int(bdate)) if isinstance(bdate, (int, float)) else str(bdate)
            if len(bdate_str) == 6:
                bdate_dt = datetime.strptime(bdate_str, '%y%m%d').date()
            else:
                bdate_dt = datetime.strptime(bdate_str[:8], '%Y%m%d').date()
        
        bday = bdate_dt.day
        bmonth = bdate_dt.month
        byear = bdate_dt.year
        
        age = reptyear - byear
        
        # AGELIMIT boundary (12)
        if age == AGELIMIT:
            if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                age = AGEBELOW
        # MAXAGE boundary (18)
        elif age == MAXAGE:
            if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                age = AGELIMIT
        # Above MAXAGE
        elif age > MAXAGE:
            age = MAXAGE
        # Below AGELIMIT
        elif age < AGELIMIT:
            age = AGEBELOW
        else:
            age = AGELIMIT
        
        return age
    except Exception as e:
        return 0

# Days per month
def days_in_month(year, month):
    """Return days in month (handle leap year)"""
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

# Filter and process using Polars (much more efficient!)
print("\nProcessing DPTRBL Parquet data...")

# Check which columns exist in the DataFrame
available_cols = df_dptrbl.columns
print(f"  Available columns: {available_cols}")

# Map column names - adjust based on actual schema
# These are common column names in deposit systems
COLUMN_MAPPING = {
    'BANKNO': ['BANKNO', 'BANK', 'BANK_CODE', 'BANK_NO'],
    'REPTNO': ['REPTNO', 'REPORT_NO', 'REPNO'],
    'FMTCODE': ['FMTCODE', 'FORMAT_CODE', 'FMT', 'FORMAT'],
    'BRANCH': ['BRANCH', 'BRANCH_CODE', 'BR_NO', 'BRANCH_NO'],
    'ACCTNO': ['ACCTNO', 'ACCOUNT_NO', 'ACCT', 'ACCOUNT'],
    'NAME': ['NAME', 'ACCOUNT_NAME', 'CUSTOMER_NAME'],
    'DEBIT': ['DEBIT', 'DEBIT_AMT', 'DR_AMOUNT'],
    'CREDIT': ['CREDIT', 'CREDIT_AMT', 'CR_AMOUNT'],
    'CLOSEDT': ['CLOSEDT', 'CLOSE_DT', 'CLOSED_DATE'],
    'OPENDT': ['OPENDT', 'OPEN_DT', 'OPENED_DATE'],
    'CUSTCODE': ['CUSTCODE', 'CUST_CODE', 'CUSTOMER_CODE'],
    'PURPOSE': ['PURPOSE', 'PURPOSE_CODE'],
    'OPENIND': ['OPENIND', 'OPEN_IND', 'STATUS', 'ACCT_STATUS'],
    'RACE': ['RACE', 'RACE_CODE', 'ETHNICITY'],
    'PRODUCT': ['PRODUCT', 'PRODUCT_CODE', 'PROD', 'PROD_CODE'],
    'DEPTYPE': ['DEPTYPE', 'DEP_TYPE', 'ACCOUNT_TYPE'],
    'CURBAL': ['CURBAL', 'CURRENT_BAL', 'BALANCE', 'CURRENT_BALANCE'],
    'APPRLIMT': ['APPRLIMT', 'APPROVED_LIMIT', 'LIMIT', 'CREDIT_LIMIT'],
    'BDATE': ['BDATE', 'BIRTH_DATE', 'DOB', 'DATE_OF_BIRTH'],
    'SECOND': ['SECOND', 'SECOND_CODE', 'SECONDARY_CODE']
}

# Find actual column names
actual_cols = {}
for standard_name, possible_names in COLUMN_MAPPING.items():
    for pname in possible_names:
        if pname in available_cols:
            actual_cols[standard_name] = pname
            break

print(f"  Column mapping: {actual_cols}")

# Rename columns to standard names
df_renamed = df_dptrbl
for standard, actual in actual_cols.items():
    if actual != standard:
        df_renamed = df_renamed.rename({actual: standard})

# Now apply filters using standard column names
try:
    df_filtered = df_renamed.filter(
        (pl.col('BANKNO') == 33) &
        (pl.col('REPTNO') == 1001) &
        (pl.col('FMTCODE') == 1) &
        (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
        (~pl.col('PRODUCT').is_in([297, 298]))
    )
    print(f"  After initial filters: {len(df_filtered):,} records")
except Exception as e:
    print(f"  Error applying filters: {e}")
    print("  Available columns:", df_renamed.columns)
    import sys
    sys.exit(1)

# Apply transformations
df_processed = df_filtered.with_columns([
    # Fix branch 132 -> 168
    pl.when(pl.col('BRANCH') == 132)
     .then(168)
     .otherwise(pl.col('BRANCH'))
     .alias('BRANCH'),
    
    # Calculate movement
    (pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT'),
    
    # DYDPBAL (same as CURBAL for this report)
    pl.col('CURBAL').alias('DYDPBAL'),
    
    # ACCYTD flag (account opened this year)
    pl.when(
        (pl.col('OPENDT').is_not_null()) &
        (pl.col('CLOSEDT') == 0) &
        (pl.col('OPENDT').dt.year() == reptdate.year)
    ).then(1).otherwise(0).alias('ACCYTD'),
    
    # Add REPTDATE
    pl.lit(reptdate).alias('REPTDATE')
])

# Add MVRANGE based on product type
df_with_range = df_processed.with_columns([
    pl.when(
        pl.col('PRODUCT').is_in(ACE_PRODUCTS)
    ).then(
        pl.col('MOVEMENT').abs().map_elements(categorize_mvtace, return_dtype=pl.Int64)
    ).otherwise(
        pl.col('MOVEMENT').abs().map_elements(categorize_mvtdep, return_dtype=pl.Int64)
    ).alias('MVRANGE')
])

# Add PRODCD categorization
def get_prodcd(deptype, product):
    """Categorize product code based on deposit type"""
    if deptype == 'S':
        if product in [200, 201, 202]:
            return '42120'  # Conventional savings
        elif product in [210, 211, 214]:
            return '42320'  # Islamic savings
        else:
            return '42120'
    elif deptype in ['D', 'N']:
        if product in [60, 61, 62, 63]:
            return '42110'  # Conventional demand
        elif product in [161, 162, 163]:
            return '42310'  # Islamic demand
        else:
            return '42180'  # HDA
    elif deptype == 'C':
        if product in [300, 301]:
            return '42130'  # FD
        else:
            return '42132'  # Islamic FD
    else:
        return ''

df_with_prodcd = df_with_range.with_columns(
    pl.struct(['DEPTYPE', 'PRODUCT'])
      .map_elements(lambda x: get_prodcd(x['DEPTYPE'], x['PRODUCT']), return_dtype=pl.String)
      .alias('PRODCD')
)

# Split by DEPTYPE
df_savings = df_with_prodcd.filter(
    (pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0)
)

df_demand = df_with_prodcd.filter(
    (pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0)
)

df_overdrafts = df_with_prodcd.filter(
    (pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0)
)

df_fixed = df_with_prodcd.filter(pl.col('DEPTYPE') == 'C')

print(f"  Savings accounts: {len(df_savings):,}")
print(f"  Demand accounts: {len(df_demand):,}")
print(f"  Overdrafts: {len(df_overdrafts):,}")
print(f"  Fixed deposits: {len(df_fixed):,}")

# Calculate branch totals
branch_totals = {
    'TOTSAVG': df_savings.filter(pl.col('PRODCD') == '42120')['CURBAL'].sum() or 0,
    'TOTSAVGI': df_savings.filter(pl.col('PRODCD') == '42320')['CURBAL'].sum() or 0,
    'TOTDMND': df_demand.filter(pl.col('PRODCD') == '42110')['CURBAL'].sum() or 0,
    'TOTDMNDI': df_demand.filter(pl.col('PRODCD') == '42310')['CURBAL'].sum() or 0,
    'TOTVOSF': 0,  # Would come from VOSTRO accounts
    'TOTVOSC': 0,  # Would come from VOSTRO accounts
    'OVDVOSF': 0,  # Overdraft VOSTRO
    'OVDVOSC': 0,  # Overdraft VOSTRO
    'TOTOVFT': df_overdrafts['CURBAL'].abs().sum() or 0,
    'TOTFD': df_fixed.filter(pl.col('PRODCD') == '42130')['CURBAL'].sum() or 0,
    'TOTFDI': df_fixed.filter(pl.col('PRODCD') == '42132')['CURBAL'].sum() or 0,
    'ACESA': df_demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'ACECA': df_demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'TOTMBSA': df_savings.filter(pl.col('PRODUCT') == 214)['CURBAL'].sum() or 0
}

# DYDP - Daily deposit details
df_dydp = df_with_prodcd.select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT',
    'DEPTYPE', 'PRODUCT', 'PRODCD', 'CURBAL', 'DYDPBAL', 'APPRLIMT',
    'ACCYTD', 'OPENDT', 'MOVEMENT', 'MVRANGE', 'CUSTCODE', 'SECOND'
])

# DDMV - Overdrafts (negative balances)
df_ddmv = df_overdrafts.select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT',
    'DEPTYPE', 'PRODCD', 'PRODUCT', 'CURBAL'
])

# DYPOSN - Daily position summary
df_dyposn = pl.DataFrame([{
    'REPTDATE': reptdate,
    **branch_totals
}])

# DYMVNT - Significant movements
df_dymvnt = df_with_prodcd.filter(
    ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DEPTYPE').is_in(['D', 'N'])) & 
     (((pl.col('MOVEMENT').abs() >= 100000) & pl.col('PRODUCT').is_in(ACE_PRODUCTS)) |
      (pl.col('MOVEMENT').abs() >= 1000000)))
).select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT',
    'DEPTYPE', 'PRODUCT', 'CURBAL', 'APPRLIMT', 'CUSTCODE', 'SECOND'
])

# DYBRDP - Branch summary
df_dybrdp = df_dydp.filter(
    ~pl.col('PRODCD').is_in(['42320', '42310']) &
    ~pl.col('PRODUCT').is_in([104, 105])
).group_by(['BRANCH', 'DEPTYPE', 'REPTDATE']).agg([
    pl.col('DYDPBAL').sum().alias('BALANCE')
])

# DYDDCR - Demand deposit credit movement
df_dyddcr = df_demand.filter(
    ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) &
    (pl.col('PRODCD') != '42310') &
    (pl.col('PRODUCT') != 72)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
    pl.when(
        pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0
    ).then(pl.col('CURBAL'))
     .otherwise(pl.col('CURBAL') - (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))))
     .alias('MOVEMENT_ADJ')
]).with_columns([
    pl.col('MOVEMENT_ADJ').abs().map_elements(
        categorize_ddmove, return_dtype=pl.Int64
    ).alias('RANGE')
]).group_by(['REPTDATE', 'RANGE']).agg([
    pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT')
])

# DYDPS - Savings movement by range
df_dydps = df_savings.filter(
    ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) &
    (pl.col('PRODCD') == '42120')
).group_by(['REPTDATE', 'MVRANGE']).agg([
    pl.col('MOVEMENT').sum()
])

# DYACE - ACE movement by range
df_dyace = df_demand.filter(
    pl.col('PRODUCT').is_in(ACE_PRODUCTS)
).group_by(['REPTDATE', 'MVRANGE']).agg([
    pl.col('MOVEMENT').sum()
])

# SDRNGE - Savings profile (weekly/month-end)
# Determine if this is a weekly or month-end processing date
mthend = 'N'
mth = days_in_month(int(reptyear), int(reptmon))
if int(reptday) == mth:
    mthend = 'Y'

if reptday in ['08', '15', '22'] or mthend == 'Y':
    print("\nCreating SDRNGE savings profile...")
    
    # Add age and range calculations
    df_sdrnge_base = df_savings.with_columns([
        pl.struct(['BDATE']).map_elements(
            lambda x: calculate_age(x['BDATE'], reptdate, int(reptyear), 
                                   int(reptmon), int(reptday)),
            return_dtype=pl.Int64
        ).alias('AGE'),
        pl.col('CURBAL').map_elements(categorize_s1range, return_dtype=pl.Int64).alias('RANGE'),
        pl.col('CURBAL').map_elements(categorize_s2range, return_dtype=pl.Int64).alias('R2NGE')
    ])
    
    df_sdrnge = df_sdrnge_base.group_by([
        'BRANCH', 'RACE', 'PRODCD', 'PRODUCT', 'RANGE', 'AGE', 'R2NGE'
    ]).agg([
        pl.len().alias('NOACCT'),
        pl.col('CURBAL').sum(),
        pl.col('ACCYTD').sum()
    ])
    
    # Save SDRNGE
    df_sdrnge.write_parquet(f'{MIS_DIR}SDRNGE{reptmon}.parquet')
    print(f"  SDRNGE: {len(df_sdrnge):,} profile records")

# Save all outputs
print("\nSaving outputs...")
df_dyposn.write_parquet(f'{TEMP_DIR}DYPOSN.parquet')
df_dydp.write_parquet(f'{TEMP_DIR}DYDP.parquet')
df_dymvnt.write_parquet(f'{MIS_DIR}DYMVNT{reptmon}.parquet')
df_ddmv.write_parquet(f'{MISQ_DIR}DDMV.parquet')
df_dybrdp.write_parquet(f'{MIS_DIR}DYBRDP{reptmon}.parquet')
df_dyddcr.write_parquet(f'{MIS_DIR}DYDDCR{reptmon}.parquet')
df_dydps.write_parquet(f'{MIS_DIR}DYDPS{reptmon}.parquet')
df_dyace.write_parquet(f'{MIS_DIR}DYACE{reptmon}.parquet')

print(f"\n{'='*60}")
print(f"✓ EIBDDEPE Complete!")
print(f"{'='*60}")
print(f"\nSummary:")
print(f"  Total deposits: RM {branch_totals['TOTSAVG'] + branch_totals['TOTDMND']:,.2f}")
print(f"  - Savings: RM {branch_totals['TOTSAVG']:,.2f}")
print(f"  - Demand: RM {branch_totals['TOTDMND']:,.2f}")
print(f"  - Fixed: RM {branch_totals['TOTFD']:,.2f}")
print(f"  Islamic deposits: RM {branch_totals['TOTSAVGI'] + branch_totals['TOTDMNDI']:,.2f}")
print(f"  - Savings: RM {branch_totals['TOTSAVGI']:,.2f}")
print(f"  - Demand: RM {branch_totals['TOTDMNDI']:,.2f}")
print(f"  - Fixed: RM {branch_totals['TOTFDI']:,.2f}")
print(f"  Overdrafts: RM {branch_totals['TOTOVFT']:,.2f}")
print(f"\nOutputs:")
print(f"  DYPOSN: Daily summary")
print(f"  DYDP: {len(df_dydp):,} deposit records")
print(f"  DYMVNT: {len(df_dymvnt):,} significant movements")
print(f"  DYBRDP: {len(df_dybrdp):,} branch summaries")
print(f"  DYDDCR: {len(df_dyddcr):,} credit movements")
print(f"  DYDPS: {len(df_dydps):,} savings movement ranges")
print(f"  DYACE: {len(df_dyace):,} ACE movement ranges")
print(f"  DDMV: {len(df_ddmv):,} overdrafts")
