"""
EIIDDEPE - Islamic Deposit Position Extract
"""

import polars as pl
from datetime import datetime
import os
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/DPTRBLGS.parquet'
BASE_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/DPTRBLGS/TEMP'
DIRS = {
    'temp': BASE_DIR,
    'mis': f'{BASE_DIR}/MIS_PIBB',
    'misq': f'{BASE_DIR}/MIS/MISQ',
    'bnm': f'{BASE_DIR}/MIS/MISQ/BNM'
}

ACE_PRODUCTS = [161, 162, 163, 150, 151, 152, 181]
EXCL_PROD, EXCL_OPEN = [297, 298], ['B', 'C', 'P']
ISLAMIC_COSTCTR_MIN, ISLAMIC_COSTCTR_MAX = 3000, 3999
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

for d in DIRS.values():
    os.makedirs(d, exist_ok=True)

print("=" * 70)
print("EIIDDEPE - Islamic Deposit Position Extract")
print("=" * 70)

# ============================================================================
# HELPERS
# ============================================================================

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
        boundary = (bd.month == mon and bd.day > day) or bd.month > mon
        if age == AGELIMIT and boundary: return AGEBELOW
        if age == MAXAGE and boundary: return AGELIMIT
        return max(AGELIMIT, min(MAXAGE, age)) if age < AGELIMIT or age > MAXAGE else age
    except: return 0

def get_prodcd(deptype, prod):
    if deptype == 'S':
        return '42320' if prod in [210,211,214] else '42120'
    if deptype in ['D','N']:
        if prod in [150,151,152,181]: return '42110'
        if prod in [61,161]: return '42310'
        if prod in [63,163]: return '42180'
        return '42180'
    if deptype == 'C':
        return '42132' if prod in [317,318] else '42130'
    return ''

# ============================================================================
# READ & FILTER
# ============================================================================

print("\nReading DPTRBL Parquet...")
df = pl.read_parquet(INPUT_FILE)
print(f"  Loaded {len(df):,} records")

# Get date
reptdate = None
for col in ['TBDATE','REPTDATE','REPTDT']:
    if col in df.columns:
        reptdate = parse_date(df.select(col).row(0)[0])
        if reptdate:
            print(f"  Date: {reptdate.strftime('%d/%m/%Y')}")
            break
if not reptdate:
    reptdate = datetime.now().date()
    print(f"  Using: {reptdate}")

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'

# Column mapping
col_map = {'BANKNO':['BANKNO','BANK'], 'REPTNO':['REPTNO'], 'FMTCODE':['FMTCODE'],
    'BRANCH':['BRANCH'], 'ACCTNO':['ACCTNO'], 'NAME':['NAME'], 'DEBIT':['DEBIT'],
    'CREDIT':['CREDIT'], 'CLOSEDT':['CLOSEDT'], 'OPENDT':['OPENDT'],
    'CUSTCODE':['CUSTCODE'], 'PURPOSE':['PURPOSE'], 'OPENIND':['OPENIND'],
    'RACE':['RACE'], 'PRODUCT':['PRODUCT'], 'DEPTYPE':['DEPTYPE'],
    'CURBAL':['CURBAL'], 'APPRLIMT':['APPRLIMT'], 'BDATE':['BDATE'],
    'SECOND':['SECOND'], 'COSTCTR':['COSTCTR','COST_CENTER']}

rename = {}
for std, alts in col_map.items():
    for alt in alts:
        if alt in df.columns:
            rename[alt] = std
            break
if rename:
    df = df.rename(rename)

# Islamic filter
df = df.filter(
    (pl.col('BANKNO') == 33) & (pl.col('REPTNO') == 1001) & (pl.col('FMTCODE') == 1) &
    (pl.col('COSTCTR').is_between(ISLAMIC_COSTCTR_MIN, ISLAMIC_COSTCTR_MAX)) &
    (~pl.col('OPENIND').is_in(EXCL_OPEN)) & (~pl.col('PRODUCT').is_in(EXCL_PROD))
)
print(f"  Islamic only: {len(df):,} records")

# ============================================================================
# PROCESS
# ============================================================================

df = df.with_columns([
    pl.col('OPENDT').cast(pl.String).alias('OPENDT_STR'),
    pl.col('CLOSEDT').cast(pl.String).alias('CLOSEDT_STR'),
    pl.when(pl.col('BRANCH') == 132).then(168).otherwise(pl.col('BRANCH')).alias('BRANCH'),
    (pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT'),
    pl.col('CURBAL').alias('DYDPBAL'),
    pl.lit(reptdate).alias('REPTDATE')
])

df = df.with_columns(
    pl.struct(['OPENDT_STR','CLOSEDT']).map_elements(
        lambda x: is_ytd(x['OPENDT_STR'], x['CLOSEDT'], reptyear), pl.Int64
    ).alias('ACCYTD')
)

df = df.with_columns(
    pl.struct(['DEPTYPE','PRODUCT']).map_elements(
        lambda x: get_prodcd(x['DEPTYPE'], x['PRODUCT']), pl.String
    ).alias('PRODCD')
)

# DPTYPE with ACE split
def get_dptype(prodcd, curbal, prod):
    if prodcd == '42110' and prod in [150,151,152,181] and curbal > 5000:
        return 'SPLIT'
    return 'S' if prodcd in ['42120','42320'] else 'D'

df = df.with_columns(
    pl.struct(['PRODCD','CURBAL','PRODUCT']).map_elements(
        lambda x: get_dptype(x['PRODCD'], x['CURBAL'], x['PRODUCT']), pl.String
    ).alias('DPTYPE')
)

df = df.with_columns([
    pl.when(pl.col('PRODUCT').is_in(ACE_PRODUCTS))
      .then(pl.col('MOVEMENT').abs().map_elements(range_mvtace, pl.Int64))
      .otherwise(pl.col('MOVEMENT').abs().map_elements(range_mvtdep, pl.Int64))
      .alias('MVRANGE')
])

# ACE Split
split = df.filter(pl.col('DPTYPE') == 'SPLIT')
normal = df.filter(pl.col('DPTYPE') != 'SPLIT')

if len(split) > 0:
    split_sa = split.with_columns([
        (pl.col('CURBAL') - 5000).alias('DYDPBAL'),
        pl.lit('S').alias('DPTYPE')
    ])
    split_ca = split.with_columns([
        pl.lit(5000).alias('DYDPBAL'),
        pl.lit('D').alias('DPTYPE')
    ])
    df = pl.concat([normal, split_sa, split_ca])
else:
    df = normal

# Split types
savings = df.filter((pl.col('DPTYPE') == 'S') & (pl.col('CURBAL') >= 0))
demand = df.filter((pl.col('DPTYPE') == 'D') & (pl.col('CURBAL') >= 0))
overdrafts = df.filter((pl.col('DPTYPE') == 'D') & (pl.col('CURBAL') < 0))
fixed = df.filter(pl.col('DEPTYPE') == 'C')

print(f"  Savings: {len(savings):,}, Demand: {len(demand):,}")
print(f"  Overdrafts: {len(overdrafts):,}, Fixed: {len(fixed):,}")

# ============================================================================
# TOTALS
# ============================================================================

totals = {
    'TOTSAVG': savings.filter(pl.col('PRODCD') == '42120')['CURBAL'].sum() or 0,
    'TOTSAVGI': savings.filter(pl.col('PRODCD') == '42320')['CURBAL'].sum() or 0,
    'TOTDMND': demand.filter(pl.col('PRODCD') == '42110')['CURBAL'].sum() or 0,
    'TOTDMNDI': demand.filter(pl.col('PRODCD') == '42310')['CURBAL'].sum() or 0,
    'TOTVOSF': 0, 'TOTVOSC': 0, 'OVDVOSF': 0, 'OVDVOSC': 0,
    'TOTOVFT': overdrafts['CURBAL'].abs().sum() or 0,
    'TOTFD': fixed.filter(pl.col('PRODCD') == '42130')['CURBAL'].sum() or 0,
    'TOTFDI': fixed.filter(pl.col('PRODCD') == '42132')['CURBAL'].sum() or 0,
    'ACESA': savings.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'ACECA': demand.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum() or 0,
    'TOTMBSA': savings.filter(pl.col('PRODUCT') == 214)['CURBAL'].sum() or 0
}

# ============================================================================
# OUTPUTS
# ============================================================================

dyposn = pl.DataFrame([{'REPTDATE': reptdate, **totals}])
dydp = df.select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
                  'PRODUCT','PRODCD','CURBAL','DYDPBAL','APPRLIMT','ACCYTD',
                  'OPENDT','MOVEMENT','MVRANGE','CUSTCODE','SECOND','DPTYPE'])
ddmv = overdrafts.select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT',
                          'DEPTYPE','PRODCD','PRODUCT','CURBAL','DPTYPE'])

# DYIBU - Islamic branch summary
dyibu = df.filter(pl.col('PRODCD').is_in(['42320','42310','42180']) | (pl.col('PRODUCT') == 214)
).group_by(['BRANCH','REPTDATE']).agg([
    pl.col('CURBAL').filter(pl.col('PRODCD') == '42320').sum().alias('SAI'),
    pl.len().filter(pl.col('PRODCD') == '42320').alias('SNO'),
    pl.col('CURBAL').filter(pl.col('PRODCD').is_in(['42310','42180'])).sum().alias('CAI'),
    pl.len().filter(pl.col('PRODCD').is_in(['42310','42180'])).alias('CNO'),
    pl.col('CURBAL').filter(pl.col('PRODUCT') == 214).sum().alias('MBS'),
    pl.len().filter(pl.col('PRODUCT') == 214).alias('MBSNO'),
])

# DYMVNT
dymvnt = df.filter(
    ((pl.col('DPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DPTYPE') == 'D') & (((pl.col('MOVEMENT').abs() >= 100000) &
      pl.col('PRODUCT').is_in(ACE_PRODUCTS)) | (pl.col('MOVEMENT').abs() >= 1000000)))
).select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
          'PRODUCT','CURBAL','APPRLIMT','CUSTCODE','SECOND'])

# DYBRDP
dybrdp = dydp.filter(~pl.col('PRODCD').is_in(['42320','42310']) & 
                      ~pl.col('PRODUCT').is_in([104,105])
).group_by(['BRANCH','DPTYPE','REPTDATE']).agg(pl.col('DYDPBAL').sum().alias('BALANCE'))

# DYDDCR
dyddcr = demand.filter(~pl.col('PRODUCT').is_in(ACE_PRODUCTS) & 
                        (pl.col('PRODCD') != '42310') & (pl.col('PRODUCT') != 72)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
    pl.when(pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0)
      .then(pl.col('CURBAL')).otherwise(pl.col('CREDIT') - pl.col('DEBIT'))
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

# SDRNGE
mth = 29 if (reptyear % 4 == 0 and reptmon == 2) else (28 if reptmon == 2 else (30 if reptmon in [4,6,9,11] else 31))
if reptday in [8,15,22] or reptday == mth:
    sdrnge = savings.with_columns([
        pl.struct(['BDATE']).map_elements(lambda x: calc_age(x['BDATE'], reptyear, reptmon, reptday), pl.Int64).alias('AGE'),
        pl.col('CURBAL').map_elements(range_s1, pl.Int64).alias('RANGE'),
        pl.col('CURBAL').map_elements(range_s2, pl.Int64).alias('R2NGE')
    ]).group_by(['BRANCH','RACE','PRODCD','PRODUCT','RANGE','AGE','R2NGE']).agg([
        pl.len().alias('NOACCT'), pl.col('CURBAL').sum(), pl.col('ACCYTD').sum()
    ])

# ============================================================================
# SAVE
# ============================================================================

print("\nSaving outputs...")
def save(df, name):
    if len(df) == 0:
        print(f"  ⚠ {name}: No data")
        return
    path = f"{DIRS['mis'] if 'SDRNGE' not in name else DIRS['mis']}/{name.format(month=reptmon)}"
    df.write_parquet(path)
    print(f"  ✓ {name.format(month=reptmon)}")

save(dyposn, 'DYPOSN{month:02d}.parquet')
save(dydp, 'DYDP.parquet')
save(dymvnt, 'DYMVNT{month:02d}.parquet')
save(ddmv, 'DDMV.parquet')
save(dybrdp, 'DYBRDP{month:02d}.parquet')
save(dyddcr, 'DYDDCR{month:02d}.parquet')
save(dydps, 'DYDPS{month:02d}.parquet')
save(dyace, 'DYACE{month:02d}.parquet')
save(dyibu, 'DYIBU{month:02d}.parquet')
if reptday in [8,15,22] or reptday == mth:
    save(sdrnge, 'SDRNGE{month:02d}.parquet')

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✓ EIIDDEPE Complete!")
print("=" * 70)
print(f"\nIslamic Report: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"\nKey: ACE Split (CURBAL > 5000) -> SA + CA")
print(f"\nDeposits: RM {totals['TOTSAVG'] + totals['TOTDMND']:>15,.2f}")
print(f"  Savings: RM {totals['TOTSAVG']:>15,.2f}")
print(f"  Demand:  RM {totals['TOTDMND']:>15,.2f}")
print(f"  Fixed:   RM {totals['TOTFD']:>15,.2f}")
print(f"\nIslamic: RM {totals['TOTSAVGI'] + totals['TOTDMNDI']:>15,.2f}")
print(f"Overdrafts: RM {totals['TOTOVFT']:>15,.2f}")
print(f"\nRecords: DYDP:{len(dydp):>8,} DYMVNT:{len(dymvnt):>8,} DYIBU:{len(dyibu):>8,}")
print("=" * 70)
