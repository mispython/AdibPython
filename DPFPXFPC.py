"""
EIBDDEPE - Daily Deposit Position Extract
"""

import polars as pl
from datetime import datetime
import os
import re

# Constants
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11
ACE_PRODUCTS = [161, 162, 163]

# Directories
for d in ['data/temp/', 'data/mis/', 'data/misq/', 'data/bnm/']:
    os.makedirs(d, exist_ok=True)

print("EIBDDEPE - Daily Deposit Position Extract")
print("=" * 60)

# Date parser
def parse_date(val):
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

# Read DPTRBL
print("\nReading DPTRBL Parquet...")
df = pl.read_parquet('data/dptrbl.parquet')
print(f"  Loaded {len(df):,} records")

# Get report date
reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
    if col in df.columns:
        reptdate = parse_date(df.select(col).row(0)[0])
        if reptdate:
            print(f"  Date from {col}: {reptdate}")
            break

if not reptdate:
    reptdate = datetime.now().date()
    print(f"  Using current date: {reptdate}")

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}, Week: {nowk}")
print("=" * 60)

# Range functions
def range_ddmove(a):
    return next((r for r in [300000,500000,1000000,1500000,2000000,3000000,4000000,5000000,10000000] if a < r), 10000001)

def range_mvtdep(a):
    return next((r for r in [5000,10000,30000,50000,75000] if a <= r), 80000)

def range_mvtace(a):
    return next((r for r in [5000,10000,30000,50000,75000,100000] if a <= r), 200000)

def range_s1(a):
    return next((r for r in [500,1000,5000,10000,20000,50000,100000,200000] if a < r), 200001)

def range_s2(a):
    return next((r for r in [1000,5000,10000,50000,100000] if a < r), 100001)

# Column mapping
col_map = {
    'BANKNO': ['BANKNO','BANK'], 'REPTNO': ['REPTNO'], 'FMTCODE': ['FMTCODE'],
    'BRANCH': ['BRANCH'], 'ACCTNO': ['ACCTNO'], 'NAME': ['NAME'],
    'DEBIT': ['DEBIT'], 'CREDIT': ['CREDIT'], 'CLOSEDT': ['CLOSEDT'],
    'OPENDT': ['OPENDT'], 'CUSTCODE': ['CUSTCODE'], 'PURPOSE': ['PURPOSE'],
    'OPENIND': ['OPENIND'], 'RACE': ['RACE'], 'PRODUCT': ['PRODUCT'],
    'DEPTYPE': ['DEPTYPE'], 'CURBAL': ['CURBAL'], 'APPRLIMT': ['APPRLIMT'],
    'BDATE': ['BDATE'], 'SECOND': ['SECOND']
}

# Map actual columns
rename = {}
for std, alts in col_map.items():
    for alt in alts:
        if alt in df.columns:
            rename[alt] = std
            break
df = df.rename(rename) if rename else df

# Filter
df = df.filter(
    (pl.col('BANKNO') == 33) & (pl.col('REPTNO') == 1001) & (pl.col('FMTCODE') == 1) &
    (~pl.col('OPENIND').is_in(['B','C','P'])) & (~pl.col('PRODUCT').is_in([297,298]))
)
print(f"  Filtered: {len(df):,} records")

# Process dates - convert to string for handling
df = df.with_columns([
    pl.col('OPENDT').cast(pl.String).alias('OPENDT_STR'),
    pl.col('CLOSEDT').cast(pl.String).alias('CLOSEDT_STR')
])

# Helper functions for Polars
def is_ytd(opendt_str, closedt, year):
    try:
        s = re.sub(r'[^0-9]', '', str(opendt_str).split('.')[0] if '.' in str(opendt_str) else str(opendt_str))
        if len(s) >= 8 and datetime.strptime(s[:8], '%Y%m%d').date().year == year:
            return 1
    except: pass
    return 0

def calc_age(bdate, year, mon, day):
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
    if deptype == 'S':
        return '42320' if prod in [210,211,214] else '42120'
    if deptype in ['D','N']:
        return '42310' if prod in [161,162,163] else ('42110' if prod in [60,61,62,63] else '42180')
    if deptype == 'C':
        return '42130' if prod in [300,301] else '42132'
    return ''

# Apply transformations step by step
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

# Split by type
savings = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0))
demand = df.filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('CURBAL') >= 0))
overdrafts = df.filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('CURBAL') < 0))
fixed = df.filter(pl.col('DEPTYPE') == 'C')

print(f"  Savings: {len(savings):,}, Demand: {len(demand):,}, Overdrafts: {len(overdrafts):,}, Fixed: {len(fixed):,}")

# Totals
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

# Output DataFrames
dyposn = pl.DataFrame([{'REPTDATE': reptdate, **totals}])
dydp = df.select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
                  'PRODUCT','PRODCD','CURBAL','DYDPBAL','APPRLIMT','ACCYTD',
                  'OPENDT','MOVEMENT','MVRANGE','CUSTCODE','SECOND'])
ddmv = overdrafts.select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT',
                          'DEPTYPE','PRODCD','PRODUCT','CURBAL'])

# Significant movements
dymvnt = df.filter(
    ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DEPTYPE').is_in(['D','N'])) & 
     (((pl.col('MOVEMENT').abs() >= 100000) & pl.col('PRODUCT').is_in(ACE_PRODUCTS)) |
      (pl.col('MOVEMENT').abs() >= 1000000)))
).select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT',
          'DEPTYPE','PRODUCT','CURBAL','APPRLIMT','CUSTCODE','SECOND'])

# Branch summary
dybrdp = dydp.filter(~pl.col('PRODCD').is_in(['42320','42310']) & 
                      ~pl.col('PRODUCT').is_in([104,105])
).group_by(['BRANCH','DEPTYPE','REPTDATE']).agg(pl.col('DYDPBAL').sum().alias('BALANCE'))

# DDCR
dyddcr = demand.filter(~pl.col('PRODUCT').is_in(ACE_PRODUCTS) & 
                        (pl.col('PRODCD') != '42310') & (pl.col('PRODUCT') != 72)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
    pl.when(pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0)
      .then(pl.col('CURBAL'))
      .otherwise(pl.col('CREDIT') - pl.col('DEBIT'))
      .alias('MOVEMENT_ADJ')
]).with_columns(
    pl.col('MOVEMENT_ADJ').abs().map_elements(range_ddmove, pl.Int64).alias('RANGE')
).group_by(['REPTDATE','RANGE']).agg(pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT'))

# DYDPS & DYACE
dydps = savings.filter(~pl.col('PRODUCT').is_in(ACE_PRODUCTS) & 
                        (pl.col('PRODCD') == '42120')
).group_by(['REPTDATE','MVRANGE']).agg(pl.col('MOVEMENT').sum())

dyace = demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS)
).group_by(['REPTDATE','MVRANGE']).agg(pl.col('MOVEMENT').sum())

# SDRNGE (weekly/month-end)
mth = 29 if (reptyear % 4 == 0 and reptmon == 2) else (28 if reptmon == 2 else (30 if reptmon in [4,6,9,11] else 31))
if reptday in [8,15,22] or reptday == mth:
    print("\nCreating SDRNGE...")
    sdrnge = savings.with_columns([
        pl.struct(['BDATE']).map_elements(
            lambda x: calc_age(x['BDATE'], reptyear, reptmon, reptday), pl.Int64
        ).alias('AGE'),
        pl.col('CURBAL').map_elements(range_s1, pl.Int64).alias('RANGE'),
        pl.col('CURBAL').map_elements(range_s2, pl.Int64).alias('R2NGE')
    ]).group_by(['BRANCH','RACE','PRODCD','PRODUCT','RANGE','AGE','R2NGE']).agg([
        pl.len().alias('NOACCT'), pl.col('CURBAL').sum(), pl.col('ACCYTD').sum()
    ])
    sdrnge.write_parquet(f'data/mis/SDRNGE{reptmon:02d}.parquet')
    print(f"  SDRNGE: {len(sdrnge):,} records")

# Save outputs
print("\nSaving outputs...")
dyposn.write_parquet('data/temp/DYPOSN.parquet')
dydp.write_parquet('data/temp/DYDP.parquet')
dymvnt.write_parquet(f'data/mis/DYMVNT{reptmon:02d}.parquet')
ddmv.write_parquet('data/misq/DDMV.parquet')
dybrdp.write_parquet(f'data/mis/DYBRDP{reptmon:02d}.parquet')
dyddcr.write_parquet(f'data/mis/DYDDCR{reptmon:02d}.parquet')
dydps.write_parquet(f'data/mis/DYDPS{reptmon:02d}.parquet')
dyace.write_parquet(f'data/mis/DYACE{reptmon:02d}.parquet')

# Summary
print(f"\n{'='*60}")
print(f"✓ EIBDDEPE Complete!")
print(f"{'='*60}")
print(f"\nSummary:")
print(f"  Total deposits: RM {totals['TOTSAVG'] + totals['TOTDMND']:,.2f}")
print(f"  - Savings: RM {totals['TOTSAVG']:,.2f}")
print(f"  - Demand: RM {totals['TOTDMND']:,.2f}")
print(f"  - Fixed: RM {totals['TOTFD']:,.2f}")
print(f"  Islamic deposits: RM {totals['TOTSAVGI'] + totals['TOTDMNDI']:,.2f}")
print(f"  Overdrafts: RM {totals['TOTOVFT']:,.2f}")
print(f"\nOutputs:")
print(f"  DYPOSN: Daily summary")
print(f"  DYDP: {len(dydp):,} records")
print(f"  DYMVNT: {len(dymvnt):,} significant movements")
print(f"  DYBRDP: {len(dybrdp):,} branch summaries")
print(f"  DYDDCR: {len(dyddcr):,} credit movements")
print(f"  DYDPS: {len(dydps):,} savings ranges")
print(f"  DYACE: {len(dyace):,} ACE ranges")
print(f"  DDMV: {len(ddmv):,} overdrafts")
