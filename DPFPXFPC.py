"""
EIBDDEPE - Daily Deposit Position Extract
"""

import polars as pl
from datetime import datetime
import os
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input file
INPUT_FILE = 'data/dptrbl.parquet'

# Output directories
OUTPUT_DIRS = {
    'temp': 'data/temp/',      # Temporary/work files
    'mis': 'data/mis/',        # MIS reports
    'misq': 'data/misq/',      # MIS query files
    'bnm': 'data/bnm/'         # BNM regulatory files
}

# File naming conventions
FILE_NAMES = {
    'dyposn': 'DYPOSN.parquet',
    'dydp': 'DYDP.parquet',
    'dymvnt': 'DYMVNT{month:02d}.parquet',
    'dybrdp': 'DYBRDP{month:02d}.parquet',
    'dyddcr': 'DYDDCR{month:02d}.parquet',
    'dydps': 'DYDPS{month:02d}.parquet',
    'dyace': 'DYACE{month:02d}.parquet',
    'sdrnge': 'SDRNGE{month:02d}.parquet',
    'ddmv': 'DDMV.parquet'
}

# Report parameters
BANKNO = 33
REPTNO = 1001
FMTCODE = 1

# Product constants
ACE_PRODUCTS = [161, 162, 163]
EXCLUDED_PRODUCTS = [297, 298]
EXCLUDED_OPENIND = ['B', 'C', 'P']

# Age calculation constants
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# ============================================================================
# SETUP
# ============================================================================

# Create directories
for dir_path in OUTPUT_DIRS.values():
    os.makedirs(dir_path, exist_ok=True)

print("=" * 70)
print("EIBDDEPE - Daily Deposit Position Extract")
print("=" * 70)
print(f"Input file: {INPUT_FILE}")
print(f"Output directories:")
for name, path in OUTPUT_DIRS.items():
    print(f"  {name:>6}: {path}")

# ============================================================================
# DATE PARSING
# ============================================================================

def parse_date(val):
    """Parse date from various formats (packed decimal, numeric, string)"""
    if not val:
        return None
    s = re.sub(r'[^0-9]', '', str(int(val)) if isinstance(val, (int, float)) else str(val))
    if len(s) >= 8:
        try: return datetime.strptime(s[:8], '%Y%m%d').date()
        except: pass
    if len(s) >= 6:
        try:
            y = int(s[:2]) + (1900 if int(s[:2]) >= 70 else 2000)
            return datetime.strptime(f"{y}{s[2:6]}", '%Y%m%d').date()
        except: pass
    return None

# ============================================================================
# RANGE FUNCTIONS
# ============================================================================

def range_ddmove(a):
    """DDMOVE format - demand deposit movement ranges"""
    return next((r for r in [300000,500000,1000000,1500000,2000000,3000000,
                             4000000,5000000,10000000] if a < r), 10000001)

def range_mvtdep(a):
    """MVTDEP format - deposit movement ranges"""
    return next((r for r in [5000,10000,30000,50000,75000] if a <= r), 80000)

def range_mvtace(a):
    """MVTACE format - ACE movement ranges"""
    return next((r for r in [5000,10000,30000,50000,75000,100000] if a <= r), 200000)

def range_s1(a):
    """S1RANGE format - savings balance ranges"""
    return next((r for r in [500,1000,5000,10000,20000,50000,100000,200000] if a < r), 200001)

def range_s2(a):
    """S2RANGE format - alternative savings ranges"""
    return next((r for r in [1000,5000,10000,50000,100000] if a < r), 100001)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_ytd(opendt_str, closedt, year):
    """Check if account was opened this year"""
    try:
        s = re.sub(r'[^0-9]', '', str(opendt_str).split('.')[0] if '.' in str(opendt_str) else str(opendt_str))
        if len(s) >= 8 and datetime.strptime(s[:8], '%Y%m%d').date().year == year:
            return 1
    except: pass
    return 0

def calc_age(bdate, year, mon, day):
    """Calculate age with special rules for boundaries"""
    if not bdate:
        return 0
    try:
        s = re.sub(r'[^0-9]', '', str(bdate).split('.')[0] if '.' in str(bdate) else str(bdate))
        if len(s) >= 8:
            bd = datetime.strptime(s[:8], '%Y%m%d').date()
        else:
            y = int(s[:2]) + (1900 if int(s[:2]) >= 70 else 2000)
            bd = datetime.strptime(f"{y}{s[2:6]}", '%Y%m%d').date()
        age = year - bd.year
        if age == AGELIMIT and ((bd.month == mon and bd.day > day) or bd.month > mon):
            return AGEBELOW
        if age == MAXAGE and ((bd.month == mon and bd.day > day) or bd.month > mon):
            return AGELIMIT
        return max(AGELIMIT, min(MAXAGE, age)) if age < AGELIMIT or age > MAXAGE else age
    except: return 0

def get_prodcd(deptype, prod):
    """Categorize product code based on deposit type"""
    if deptype == 'S':
        return '42320' if prod in [210,211,214] else '42120'
    if deptype in ['D','N']:
        return '42310' if prod in [161,162,163] else ('42110' if prod in [60,61,62,63] else '42180')
    if deptype == 'C':
        return '42130' if prod in [300,301] else '42132'
    return ''

# ============================================================================
# COLUMN MAPPING
# ============================================================================

COLUMN_MAPPING = {
    'BANKNO': ['BANKNO','BANK'],
    'REPTNO': ['REPTNO'],
    'FMTCODE': ['FMTCODE'],
    'BRANCH': ['BRANCH'],
    'ACCTNO': ['ACCTNO'],
    'NAME': ['NAME'],
    'DEBIT': ['DEBIT'],
    'CREDIT': ['CREDIT'],
    'CLOSEDT': ['CLOSEDT'],
    'OPENDT': ['OPENDT'],
    'CUSTCODE': ['CUSTCODE'],
    'PURPOSE': ['PURPOSE'],
    'OPENIND': ['OPENIND'],
    'RACE': ['RACE'],
    'PRODUCT': ['PRODUCT'],
    'DEPTYPE': ['DEPTYPE'],
    'CURBAL': ['CURBAL'],
    'APPRLIMT': ['APPRLIMT'],
    'BDATE': ['BDATE'],
    'SECOND': ['SECOND']
}

# ============================================================================
# READ DATA
# ============================================================================

print("\n" + "-" * 70)
print("STEP 1: Reading DPTRBL Parquet")
print("-" * 70)

df = pl.read_parquet(INPUT_FILE)
print(f"  Loaded {len(df):,} records")

# ============================================================================
# GET REPORT DATE
# ============================================================================

print("\n" + "-" * 70)
print("STEP 2: Determining Report Date")
print("-" * 70)

reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
    if col in df.columns:
        reptdate = parse_date(df.select(col).row(0)[0])
        if reptdate:
            print(f"  Date from {col}: {reptdate.strftime('%d/%m/%Y')}")
            break

if not reptdate:
    reptdate = datetime.now().date()
    print(f"  Using current date: {reptdate.strftime('%d/%m/%Y')}")

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'
print(f"  Year: {reptyear}, Month: {reptmon:02d}, Day: {reptday:02d}, Week: {nowk}")

# ============================================================================
# MAP COLUMNS
# ============================================================================

print("\n" + "-" * 70)
print("STEP 3: Mapping Columns")
print("-" * 70)

rename = {}
for std, alts in COLUMN_MAPPING.items():
    for alt in alts:
        if alt in df.columns:
            rename[alt] = std
            break

if rename:
    df = df.rename(rename)
    print(f"  Renamed {len(rename)} columns")

# ============================================================================
# FILTER DATA
# ============================================================================

print("\n" + "-" * 70)
print("STEP 4: Filtering Data")
print("-" * 70)

df = df.filter(
    (pl.col('BANKNO') == BANKNO) & 
    (pl.col('REPTNO') == REPTNO) & 
    (pl.col('FMTCODE') == FMTCODE) &
    (~pl.col('OPENIND').is_in(EXCLUDED_OPENIND)) & 
    (~pl.col('PRODUCT').is_in(EXCLUDED_PRODUCTS))
)
print(f"  Filtered records: {len(df):,}")

# ============================================================================
# PREPARE DATA
# ============================================================================

print("\n" + "-" * 70)
print("STEP 5: Preparing Data")
print("-" * 70)

# Convert date columns to strings
df = df.with_columns([
    pl.col('OPENDT').cast(pl.String).alias('OPENDT_STR'),
    pl.col('CLOSEDT').cast(pl.String).alias('CLOSEDT_STR')
])

# Apply transformations
df = df.with_columns([
    pl.when(pl.col('BRANCH') == 132).then(168).otherwise(pl.col('BRANCH')).alias('BRANCH'),
    (pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT'),
    pl.col('CURBAL').alias('DYDPBAL'),
    pl.lit(reptdate).alias('REPTDATE')
])

# Add ACCYTD
df = df.with_columns(
    pl.struct(['OPENDT_STR','CLOSEDT']).map_elements(
        lambda x: is_ytd(x['OPENDT_STR'], x['CLOSEDT'], reptyear), pl.Int64
    ).alias('ACCYTD')
)

# Add MVRANGE and PRODCD
df = df.with_columns([
    pl.when(pl.col('PRODUCT').is_in(ACE_PRODUCTS))
      .then(pl.col('MOVEMENT').abs().map_elements(range_mvtace, pl.Int64))
      .otherwise(pl.col('MOVEMENT').abs().map_elements(range_mvtdep, pl.Int64))
      .alias('MVRANGE'),
    pl.struct(['DEPTYPE','PRODUCT']).map_elements(
        lambda x: get_prodcd(x['DEPTYPE'], x['PRODUCT']), pl.String
    ).alias('PRODCD')
])

print(f"  Data prepared successfully")

# ============================================================================
# SPLIT BY TYPE
# ============================================================================

print("\n" + "-" * 70)
print("STEP 6: Splitting by Deposit Type")
print("-" * 70)

savings = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0))
demand = df.filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('CURBAL') >= 0))
overdrafts = df.filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('CURBAL') < 0))
fixed = df.filter(pl.col('DEPTYPE') == 'C')

print(f"  Savings accounts: {len(savings):,}")
print(f"  Demand accounts: {len(demand):,}")
print(f"  Overdrafts: {len(overdrafts):,}")
print(f"  Fixed deposits: {len(fixed):,}")

# ============================================================================
# CALCULATE TOTALS
# ============================================================================

print("\n" + "-" * 70)
print("STEP 7: Calculating Totals")
print("-" * 70)

totals = {
    'TOTSAVG': savings.filter(pl.col('PRODCD') == '42120')['CURBAL'].sum() or 0,
    'TOTSAVGI': savings.filter(pl.col('PRODCD') == '42320')['CURBAL'].sum() or 0,
    'TOTDMND': demand.filter(pl.col('PRODCD') == '42110')['CURBAL'].sum() or 0,
    'TOTDMNDI': demand.filter(pl.col('PRODCD') == '42310')['CURBAL'].sum() or 0,
    'TOTOVFT': overdrafts['CURBAL'].abs().sum() or 0,
    'TOTFD': fixed.filter(pl.col('PRODCD') == '42130')['CURBAL'].sum() or 0,
    'TOTFDI': fixed.filter(pl.col('PRODCD') == '42132')['CURBAL'].sum() or 0,
    'ACESA': demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'ACECA': demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'TOTMBSA': savings.filter(pl.col('PRODUCT') == 214)['CURBAL'].sum() or 0
}

print(f"  Total deposits: RM {totals['TOTSAVG'] + totals['TOTDMND']:,.2f}")

# ============================================================================
# CREATE OUTPUT DATAFRAMES
# ============================================================================

print("\n" + "-" * 70)
print("STEP 8: Creating Output DataFrames")
print("-" * 70)

# DYPOSN - Daily position summary
dyposn = pl.DataFrame([{'REPTDATE': reptdate, **totals}])

# DYDP - Daily deposit details
dydp = df.select([
    'REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
    'PRODUCT','PRODCD','CURBAL','DYDPBAL','APPRLIMT','ACCYTD',
    'OPENDT','MOVEMENT','MVRANGE','CUSTCODE','SECOND'
])

# DDMV - Overdrafts
ddmv = overdrafts.select([
    'REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT',
    'DEPTYPE','PRODCD','PRODUCT','CURBAL'
])

# DYMVNT - Significant movements
dymvnt = df.filter(
    ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DEPTYPE').is_in(['D','N'])) & 
     (((pl.col('MOVEMENT').abs() >= 100000) & pl.col('PRODUCT').is_in(ACE_PRODUCTS)) |
      (pl.col('MOVEMENT').abs() >= 1000000)))
).select([
    'REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT',
    'DEPTYPE','PRODUCT','CURBAL','APPRLIMT','CUSTCODE','SECOND'
])

# DYBRDP - Branch summary
dybrdp = dydp.filter(
    ~pl.col('PRODCD').is_in(['42320','42310']) & 
    ~pl.col('PRODUCT').is_in([104,105])
).group_by(['BRANCH','DEPTYPE','REPTDATE']).agg(
    pl.col('DYDPBAL').sum().alias('BALANCE')
)

# DYDDCR - Demand deposit credit movement
dyddcr = demand.filter(
    ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) & 
    (pl.col('PRODCD') != '42310') & 
    (pl.col('PRODUCT') != 72)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
    pl.when(pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0)
      .then(pl.col('CURBAL'))
      .otherwise(pl.col('CREDIT') - pl.col('DEBIT'))
      .alias('MOVEMENT_ADJ')
]).with_columns(
    pl.col('MOVEMENT_ADJ').abs().map_elements(range_ddmove, pl.Int64).alias('RANGE')
).group_by(['REPTDATE','RANGE']).agg(
    pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT')
)

# DYDPS - Savings movement by range
dydps = savings.filter(
    ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) & 
    (pl.col('PRODCD') == '42120')
).group_by(['REPTDATE','MVRANGE']).agg(
    pl.col('MOVEMENT').sum()
)

# DYACE - ACE movement by range
dyace = demand.filter(
    pl.col('PRODUCT').is_in(ACE_PRODUCTS)
).group_by(['REPTDATE','MVRANGE']).agg(
    pl.col('MOVEMENT').sum()
)

print(f"  DYDP: {len(dydp):,} records")
print(f"  DYMVNT: {len(dymvnt):,} significant movements")

# ============================================================================
# SDRNGE - Savings Profile (Weekly/Month-End)
# ============================================================================

mth = 29 if (reptyear % 4 == 0 and reptmon == 2) else (
    28 if reptmon == 2 else (30 if reptmon in [4,6,9,11] else 31)
)

if reptday in [8,15,22] or reptday == mth:
    print("\n" + "-" * 70)
    print("STEP 9: Creating SDRNGE Savings Profile (Weekly/Month-End)")
    print("-" * 70)
    
    sdrnge = savings.with_columns([
        pl.struct(['BDATE']).map_elements(
            lambda x: calc_age(x['BDATE'], reptyear, reptmon, reptday), pl.Int64
        ).alias('AGE'),
        pl.col('CURBAL').map_elements(range_s1, pl.Int64).alias('RANGE'),
        pl.col('CURBAL').map_elements(range_s2, pl.Int64).alias('R2NGE')
    ]).group_by(['BRANCH','RACE','PRODCD','PRODUCT','RANGE','AGE','R2NGE']).agg([
        pl.len().alias('NOACCT'), 
        pl.col('CURBAL').sum(), 
        pl.col('ACCYTD').sum()
    ])
    print(f"  SDRNGE: {len(sdrnge):,} records")

# ============================================================================
# SAVE OUTPUTS
# ============================================================================

print("\n" + "-" * 70)
print("STEP 10: Saving Outputs")
print("-" * 70)

# Get output paths
temp_dir = OUTPUT_DIRS['temp']
mis_dir = OUTPUT_DIRS['mis']
misq_dir = OUTPUT_DIRS['misq']

# Save files
dyposn.write_parquet(f"{temp_dir}{FILE_NAMES['dyposn']}")
print(f"  ✓ {FILE_NAMES['dyposn']} -> {temp_dir}")

dydp.write_parquet(f"{temp_dir}{FILE_NAMES['dydp']}")
print(f"  ✓ {FILE_NAMES['dydp']} -> {temp_dir}")

dymvnt.write_parquet(f"{mis_dir}{FILE_NAMES['dymvnt'].format(month=reptmon)}")
print(f"  ✓ {FILE_NAMES['dymvnt'].format(month=reptmon)} -> {mis_dir}")

ddmv.write_parquet(f"{misq_dir}{FILE_NAMES['ddmv']}")
print(f"  ✓ {FILE_NAMES['ddmv']} -> {misq_dir}")

dybrdp.write_parquet(f"{mis_dir}{FILE_NAMES['dybrdp'].format(month=reptmon)}")
print(f"  ✓ {FILE_NAMES['dybrdp'].format(month=reptmon)} -> {mis_dir}")

dyddcr.write_parquet(f"{mis_dir}{FILE_NAMES['dyddcr'].format(month=reptmon)}")
print(f"  ✓ {FILE_NAMES['dyddcr'].format(month=reptmon)} -> {mis_dir}")

dydps.write_parquet(f"{mis_dir}{FILE_NAMES['dydps'].format(month=reptmon)}")
print(f"  ✓ {FILE_NAMES['dydps'].format(month=reptmon)} -> {mis_dir}")

dyace.write_parquet(f"{mis_dir}{FILE_NAMES['dyace'].format(month=reptmon)}")
print(f"  ✓ {FILE_NAMES['dyace'].format(month=reptmon)} -> {mis_dir}")

if reptday in [8,15,22] or reptday == mth:
    sdrnge.write_parquet(f"{mis_dir}{FILE_NAMES['sdrnge'].format(month=reptmon)}")
    print(f"  ✓ {FILE_NAMES['sdrnge'].format(month=reptmon)} -> {mis_dir}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✓ EIBDDEPE Complete!")
print("=" * 70)

print("\n" + "-" * 70)
print("SUMMARY")
print("-" * 70)

print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")

print(f"\nDeposit Totals:")
print(f"  Total Deposits:     RM {totals['TOTSAVG'] + totals['TOTDMND']:>14,.2f}")
print(f"  - Savings:          RM {totals['TOTSAVG']:>14,.2f}")
print(f"  - Demand:           RM {totals['TOTDMND']:>14,.2f}")
print(f"  - Fixed:            RM {totals['TOTFD']:>14,.2f}")

print(f"\nIslamic Deposits:")
print(f"  Total Islamic:      RM {totals['TOTSAVGI'] + totals['TOTDMNDI']:>14,.2f}")
print(f"  - Savings:          RM {totals['TOTSAVGI']:>14,.2f}")
print(f"  - Demand:           RM {totals['TOTDMNDI']:>14,.2f}")
print(f"  - Fixed:            RM {totals['TOTFDI']:>14,.2f}")

print(f"\nOverdrafts:          RM {totals['TOTOVFT']:>14,.2f}")

print(f"\nOutput Files:")
print(f"  {FILE_NAMES['dyposn']:>25} -> {temp_dir}")
print(f"  {FILE_NAMES['dydp']:>25} -> {temp_dir}")
print(f"  {FILE_NAMES['dymvnt'].format(month=reptmon):>25} -> {mis_dir}")
print(f"  {FILE_NAMES['dybrdp'].format(month=reptmon):>25} -> {mis_dir}")
print(f"  {FILE_NAMES['dyddcr'].format(month=reptmon):>25} -> {mis_dir}")
print(f"  {FILE_NAMES['dydps'].format(month=reptmon):>25} -> {mis_dir}")
print(f"  {FILE_NAMES['dyace'].format(month=reptmon):>25} -> {mis_dir}")
print(f"  {FILE_NAMES['ddmv']:>25} -> {misq_dir}")

if reptday in [8,15,22] or reptday == mth:
    print(f"  {FILE_NAMES['sdrnge'].format(month=reptmon):>25} -> {mis_dir}")

print(f"\nRecord Counts:")
print(f"  DYDP:  {len(dydp):>10,} deposit records")
print(f"  DYMVNT:{len(dymvnt):>10,} significant movements")
print(f"  DYBRDP:{len(dybrdp):>10,} branch summaries")
print(f"  DYDDCR:{len(dyddcr):>10,} credit movements")
print(f"  DYDPS: {len(dydps):>10,} savings ranges")
print(f"  DYACE: {len(dyace):>10,} ACE ranges")
print(f"  DDMV:  {len(ddmv):>10,} overdrafts")

print("\n" + "=" * 70)
