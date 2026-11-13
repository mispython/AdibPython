"""
EIBDDEPE - Daily Deposit Position Extract
Faithful port of original SAS EIBDDEPE program.

SAS-faithful behaviors:
- REPTDAT3 = REPTDATE - 31 days (used ONLY for MISQ.DDMV dedupe)
- DDMV: delete REPTDATE = REPTDAT3, then PROC APPEND (rolling ~1 month)
- Day 01 monthly files: fresh write
- Other days: delete-by-REPTDATE then append (SAS %MISAPPD macro)
- SDRNGE: overwrite each run (SAS %WISEPBB)
"""

import polars as pl
from datetime import datetime, timedelta
import os
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'
BASE_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE'
DIRS = {
    'temp': BASE_DIR,
    'mis': f'{BASE_DIR}/MIS',
    'misq': f'{BASE_DIR}/MIS/MISQ',
    'bnm': f'{BASE_DIR}/MIS/MISQ/BNM'
}

ACE_PRODUCTS = [161, 162, 163, 150, 151, 152, 181]
EXCL_PROD, EXCL_OPEN = [297, 298], ['B', 'C', 'P']
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

for d in DIRS.values():
    os.makedirs(d, exist_ok=True)

print("=" * 70)
print("EIBDDEPE - Daily Deposit Position Extract (SAS-faithful port)")
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
        return '42320' if prod in [210, 211, 214] else '42120'
    if deptype in ['D', 'N']:
        if prod in [150, 151, 152, 181]: return '42110'
        if prod in [61, 161]: return '42310'
        if prod in [63, 163]: return '42180'
        return '42180'
    if deptype == 'C':
        return '42132' if prod in [317, 318] else '42130'
    return ''

def days_in_month(year, month):
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

# ============================================================================
# READ & FILTER
# ============================================================================

print("\nReading DPTRBL Parquet...")
df = pl.read_parquet(INPUT_FILE)
print(f"  Loaded {len(df):,} records")

reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
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

# SAS: REPTDAT3 = REPTDATE - 31  (explicitly 31 days for DDMV rolling dedupe)
reptdat3 = reptdate - timedelta(days=31)
rdate   = reptdate.strftime('%d%m%y')
rdate3  = reptdat3.strftime('%d%m%y')

print(f"  Week: {nowk}, Month: {reptmon:02d}, Day: {reptday:02d}")
print(f"  DDMV dedupe target (REPTDAT3 = REPTDATE - 31): {rdate3}")

# Column mapping
col_map = {'BANKNO':['BANKNO','BANK'], 'REPTNO':['REPTNO'], 'FMTCODE':['FMTCODE'],
    'BRANCH':['BRANCH'], 'ACCTNO':['ACCTNO'], 'NAME':['NAME'], 'DEBIT':['DEBIT'],
    'CREDIT':['CREDIT'], 'CLOSEDT':['CLOSEDT'], 'OPENDT':['OPENDT'],
    'CUSTCODE':['CUSTCODE'], 'PURPOSE':['PURPOSE'], 'OPENIND':['OPENIND'],
    'RACE':['RACE'], 'PRODUCT':['PRODUCT'], 'DEPTYPE':['DEPTYPE'],
    'CURBAL':['CURBAL'], 'APPRLIMT':['APPRLIMT'], 'BDATE':['BDATE'],
    'SECOND':['SECOND']}

rename = {}
for std, alts in col_map.items():
    for alt in alts:
        if alt in df.columns:
            rename[alt] = std
            break
if rename:
    df = df.rename(rename)

df = df.filter(
    (pl.col('BANKNO') == 33) & (pl.col('REPTNO') == 1001) & (pl.col('FMTCODE') == 1)
)
print(f"  Filtered (bank/rept/fmt): {len(df):,}")

df = df.filter(
    (~pl.col('OPENIND').is_in(EXCL_OPEN)) & (~pl.col('PRODUCT').is_in(EXCL_PROD))
)
print(f"  After OPENIND/PRODUCT filter: {len(df):,}")

# ============================================================================
# ENRICH
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
    pl.struct(['OPENDT_STR', 'CLOSEDT']).map_elements(
        lambda x: is_ytd(x['OPENDT_STR'], x['CLOSEDT'], reptyear), pl.Int64
    ).alias('ACCYTD')
)

df = df.with_columns(
    pl.struct(['DEPTYPE', 'PRODUCT']).map_elements(
        lambda x: get_prodcd(x['DEPTYPE'], x['PRODUCT']), pl.String
    ).alias('PRODCD')
)

df = df.with_columns(
    pl.when(pl.col('DEPTYPE') == 'S').then(pl.lit('S'))
     .otherwise(pl.lit('D'))
     .alias('DPTYPE')
)

df = df.with_columns([
    pl.when(pl.col('PRODUCT').is_in(ACE_PRODUCTS))
      .then(pl.col('MOVEMENT').abs().map_elements(range_mvtace, pl.Int64))
      .otherwise(pl.col('MOVEMENT').abs().map_elements(range_mvtdep, pl.Int64))
      .alias('MVRANGE')
])

print(f"  Enriched: {len(df):,} records")

# ============================================================================
# SPLIT INTO DYDP, DDMV, SAVG
# ============================================================================

dydp_s = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0) &
                   (pl.col('PRODCD').is_in(['42120', '42320'])))

dydp_d = df.filter((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0) &
                   (pl.col('PRODCD').is_in(['42110', '42180', '42310'])))

dydp = pl.concat([dydp_s, dydp_d]).select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT', 'DEPTYPE',
    'DPTYPE', 'PRODUCT', 'PRODCD', 'CURBAL', 'DYDPBAL', 'APPRLIMT',
    'ACCYTD', 'OPENDT', 'MOVEMENT', 'MVRANGE', 'CUSTCODE', 'SECOND'
])

ddmv = df.filter(
    (pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0) &
    (pl.col('PRODCD').is_in(['42110', '42310']))
).select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT', 'DEPTYPE',
    'PRODCD', 'PRODUCT', 'CURBAL'
])

# SAS: MTHEND='Y' if REPTDAY = days_in_month; weekly if day in (08,15,22)
mth = days_in_month(reptyear, reptmon)
is_weekly_or_monthend = (reptday in [8, 15, 22]) or (reptday == mth)

if is_weekly_or_monthend:
    savg = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0)).with_columns([
        pl.struct(['BDATE']).map_elements(
            lambda x: calc_age(x['BDATE'], reptyear, reptmon, reptday), pl.Int64
        ).alias('AGE'),
        pl.col('CURBAL').map_elements(range_s1, pl.Int64).alias('RANGE'),
        pl.col('CURBAL').map_elements(range_s2, pl.Int64).alias('R2NGE')
    ]).select([
        'ACCTNO', 'BRANCH', 'PRODUCT', 'DEPTYPE', 'NAME', 'OPENIND',
        'RANGE', 'RACE', 'BDATE', 'CURBAL', 'PURPOSE', 'PRODCD',
        'AGE', 'ACCYTD', 'OPENDT', 'R2NGE'
    ])
else:
    savg = pl.DataFrame([])

# ============================================================================
# BRANCH-LEVEL SUMS
# ============================================================================

def branch_sum(df, valcol='CURBAL', cnt=False):
    if len(df) == 0:
        return {}
    out = df.group_by('BRANCH').agg(
        [pl.len().alias('CNT')] if cnt else [pl.col(valcol).sum().alias('SUM')]
    )
    return dict(zip(out['BRANCH'].to_list(), out[out.columns[1]].to_list()))

s_pos = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0))
SA_branch    = branch_sum(s_pos.filter(pl.col('PRODCD') == '42120'))
SACAI_branch = branch_sum(s_pos.filter(pl.col('PRODCD') == '42320'))
SAI_branch   = SACAI_branch.copy()
SNO_branch   = branch_sum(s_pos.filter(pl.col('PRODCD') == '42320'), cnt=True)
MBS_branch   = branch_sum(s_pos.filter((pl.col('PRODCD') == '42320') & (pl.col('PRODUCT') == 214)))
MBSNO_branch = branch_sum(s_pos.filter((pl.col('PRODCD') == '42320') & (pl.col('PRODUCT') == 214)), cnt=True)

d_pos = df.filter((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0))
CA_branch   = branch_sum(d_pos.filter(pl.col('PRODCD') == '42110'))
HDA_branch  = branch_sum(d_pos.filter(pl.col('PRODCD') == '42180'))
CAI_branch  = branch_sum(d_pos.filter(pl.col('PRODCD') == '42310'))
CNO_branch  = branch_sum(d_pos.filter(pl.col('PRODCD') == '42310'), cnt=True)
CAIG_branch = branch_sum(d_pos.filter((pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61, 161]))))
CGNO_branch = branch_sum(d_pos.filter((pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61, 161]))), cnt=True)
CAI_branch  = {k: CAI_branch.get(k, 0) + v for k, v in
               branch_sum(d_pos.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163])))).items()}
CNO_branch  = {k: CNO_branch.get(k, 0) + v for k, v in
               branch_sum(d_pos.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163]))), cnt=True).items()}
CAIH_branch = branch_sum(d_pos.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163]))))
CHNO_branch = branch_sum(d_pos.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163]))), cnt=True)

# Overdraft contribution to CAI/CAIH (SAS: CURBAL negative added to CAI{BRANCH})
od = df.filter((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0))
for k, v in branch_sum(od.filter(pl.col('PRODCD') == '42310')).items():
    CAI_branch[k] = CAI_branch.get(k, 0) + v
for k, v in branch_sum(od.filter(pl.col('PRODCD') == '42310'), cnt=True).items():
    CNO_branch[k] = CNO_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61, 161])))).items():
    CAIG_branch[k] = CAIG_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61, 161]))), cnt=True).items():
    CGNO_branch[k] = CGNO_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163])))).items():
    CAI_branch[k] = CAI_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163]))), cnt=True).items():
    CNO_branch[k] = CNO_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163])))).items():
    CAIH_branch[k] = CAIH_branch.get(k, 0) + v
for k, v in branch_sum(od.filter((pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63, 163]))), cnt=True).items():
    CHNO_branch[k] = CHNO_branch.get(k, 0) + v

# ============================================================================
# TOTALS
# ============================================================================

def safe_sum(s): return float(s) if s is not None else 0.0

totals = {
    'TOTSAVG':  safe_sum(dydp_s.filter(pl.col('PRODCD') == '42120')['CURBAL'].sum()),
    'TOTSAVGI': safe_sum(dydp_s.filter(pl.col('PRODCD') == '42320')['CURBAL'].sum()),
    'TOTDMND':  safe_sum(dydp_d.filter(pl.col('PRODCD').is_in(['42110', '42180']))['CURBAL'].sum()),
    'TOTDMNDI': safe_sum(dydp_d.filter(pl.col('PRODCD') == '42310')['CURBAL'].sum()),
    'TOTVOSF':  safe_sum(dydp_d.filter(pl.col('PRODUCT') == 105)['CURBAL'].sum()),
    'TOTVOSC':  safe_sum(dydp_d.filter(pl.col('PRODUCT') == 104)['CURBAL'].sum()),
    'OVDVOSF':  safe_sum(od.filter(pl.col('PRODUCT') == 105)['CURBAL'].sum()),
    'OVDVOSC':  safe_sum(od.filter(pl.col('PRODUCT') == 104)['CURBAL'].sum()),
    'TOTOVFT':  safe_sum(od['CURBAL'].sum()),
    'TOTFD':    safe_sum(df.filter((pl.col('DEPTYPE') == 'C') & (pl.col('PRODCD') == '42130'))['CURBAL'].sum()),
    'TOTFDI':   safe_sum(df.filter((pl.col('DEPTYPE') == 'C') & (pl.col('PRODCD') == '42132'))['CURBAL'].sum()),
    'ACESA':    safe_sum(dydp_s.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum()),
    'ACECA':    safe_sum(dydp_d.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))['CURBAL'].sum()),
    'TOTMBSA':  safe_sum(dydp_s.filter((pl.col('PRODCD') == '42320') & (pl.col('PRODUCT') == 214))['CURBAL'].sum()),
}

# ============================================================================
# OUTPUT DATAFRAMES
# ============================================================================

dyposn = pl.DataFrame([{'REPTDATE': reptdate, **totals}])

all_branches = sorted(set().union(*[
    set(d.keys()) for d in [SAI_branch, CAI_branch, CAIG_branch, CAIH_branch,
                            SNO_branch, CNO_branch, CGNO_branch, CHNO_branch,
                            MBS_branch, MBSNO_branch] if d
]))

dyibu_rows = []
for br in all_branches:
    row = {
        'REPTDATE': reptdate, 'BRANCH': br,
        'SAI': SAI_branch.get(br, 0.0), 'CAI': CAI_branch.get(br, 0.0),
        'CAIG': CAIG_branch.get(br, 0.0), 'CAIH': CAIH_branch.get(br, 0.0),
        'MBS': MBS_branch.get(br, 0.0), 'SNO': SNO_branch.get(br, 0),
        'CNO': CNO_branch.get(br, 0), 'CGNO': CGNO_branch.get(br, 0),
        'CHNO': CHNO_branch.get(br, 0), 'MBSNO': MBSNO_branch.get(br, 0),
    }
    if any(v != 0 for k, v in row.items() if k not in ('REPTDATE', 'BRANCH')):
        dyibu_rows.append(row)

dyibu = pl.DataFrame(dyibu_rows) if dyibu_rows else pl.DataFrame(
    schema={'REPTDATE': pl.Date, 'BRANCH': pl.Int64, 'SAI': pl.Float64,
            'CAI': pl.Float64, 'CAIG': pl.Float64, 'CAIH': pl.Float64,
            'MBS': pl.Float64, 'SNO': pl.Int64, 'CNO': pl.Int64,
            'CGNO': pl.Int64, 'CHNO': pl.Int64, 'MBSNO': pl.Int64}
)

dymvnt = dydp.filter(
    ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('DPTYPE') == 'D') &
     (((pl.col('MOVEMENT').abs() >= 100000) & (pl.col('PRODUCT').is_in(ACE_PRODUCTS))) |
      (pl.col('MOVEMENT').abs() >= 1000000)))
).select([
    'REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT', 'DEPTYPE',
    'PRODUCT', 'CURBAL', 'APPRLIMT', 'CUSTCODE', 'SECOND'
])

dybrdp = dydp.filter(
    (~pl.col('PRODCD').is_in(['42320', '42310'])) &
    (~pl.col('PRODUCT').is_in([104, 105]))
).group_by(['BRANCH', 'DPTYPE', 'REPTDATE']).agg(
    pl.col('DYDPBAL').sum().alias('BALANCE')
).select(['BRANCH', 'DPTYPE', 'REPTDATE', 'BALANCE'])

dyddcr_base = dydp.filter(
    (pl.col('DPTYPE') == 'D') &
    (~pl.col('PRODUCT').is_in(ACE_PRODUCTS)) &
    (pl.col('PRODCD') != '42310') &
    (pl.col('PRODUCT') != 72) &
    (pl.col('CURBAL') >= 0)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
    pl.when(pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0)
      .then(pl.col('CURBAL'))
      .otherwise(pl.col('CURBAL') - (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))))
      .alias('MOVEMENT_ADJ')
])

if len(dyddcr_base) > 0:
    dyddcr = dyddcr_base.with_columns(
        pl.col('MOVEMENT_ADJ').abs().map_elements(range_ddmove, pl.Int64).alias('RANGE')
    ).group_by(['REPTDATE', 'RANGE']).agg(
        pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT')
    )
else:
    dyddcr = pl.DataFrame(schema={'REPTDATE': pl.Date, 'RANGE': pl.Int64, 'MOVEMENT': pl.Float64})

dydps = dydp.filter(
    (pl.col('DPTYPE') == 'S') &
    (~pl.col('PRODUCT').is_in(ACE_PRODUCTS)) &
    (pl.col('PRODCD') == '42120')
).group_by(['REPTDATE', 'MVRANGE']).agg(pl.col('MOVEMENT').sum())

dyace = dydp.filter(
    (pl.col('DPTYPE') == 'D') &
    (pl.col('PRODUCT').is_in(ACE_PRODUCTS))
).group_by(['REPTDATE', 'MVRANGE']).agg(pl.col('MOVEMENT').sum())

if is_weekly_or_monthend and len(savg) > 0:
    sdrnge = savg.group_by(['BRANCH', 'RACE', 'PRODCD', 'PRODUCT', 'RANGE', 'AGE', 'R2NGE']).agg([
        pl.len().alias('NOACCT'),
        pl.col('CURBAL').sum(),
        pl.col('ACCYTD').sum()
    ])
else:
    sdrnge = pl.DataFrame([])

# ============================================================================
# UPSERT / APPEND LOGIC (matches SAS exactly)
# ============================================================================

def upsert_monthly(df_new, name, dir_key):
    """SAS %MISAPPD:
       - REPTDAY = '01': delete file, write fresh
       - Else: read old, delete rows with REPTDATE = current, append new
    """
    if len(df_new) == 0:
        print(f"  ⚠ {name.format(month=reptmon)}: No data")
        return
    path = f"{DIRS[dir_key]}/{name.format(month=reptmon)}"
    if reptday == 1:
        # SAS: PROC DATASETS DELETE then PROC APPEND
        df_new.write_parquet(path)
        print(f"  ✓ {name.format(month=reptmon)} (new)")
    else:
        try:
            df_old = pl.read_parquet(path)
            # SAS: IF REPTDATE EQ &RDATE THEN DELETE
            if 'REPTDATE' in df_old.columns:
                df_old = df_old.filter(pl.col('REPTDATE') != reptdate)
            pl.concat([df_old, df_new], how='diagonal_relaxed').write_parquet(path)
            print(f"  ✓ {name.format(month=reptmon)} (appended)")
        except FileNotFoundError:
            df_new.write_parquet(path)
            print(f"  ✓ {name.format(month=reptmon)} (created)")

def append_ddmv_sas_faithful(df_new):
    """SAS exact MISQ.DDMV logic:
       DATA MISQ.DDMV; SET MISQ.DDMV; IF REPTDATE=&RDATE3 THEN DELETE;
       PROC APPEND DATA=DDMV BASE=MISQ.DDMV;
       RDATE3 = REPTDATE - 31 days.
    """
    if len(df_new) == 0:
        print("  ⚠ DDMV: No data to append")
        return
    path = f"{DIRS['misq']}/DDMV.parquet"
    try:
        df_old = pl.read_parquet(path)
        # SAS: IF REPTDATE = REPTDAT3 THEN DELETE  (REPTDAT3 = REPTDATE - 31)
        before = len(df_old)
        df_old = df_old.filter(pl.col('REPTDATE') != reptdat3)
        removed = before - len(df_old)
        df_combined = pl.concat([df_old, df_new], how='diagonal_relaxed')
        df_combined.write_parquet(path)
        print(f"  ✓ DDMV (removed {removed} rows dated {rdate3}, appended {len(df_new)}, total {len(df_combined):,})")
    except FileNotFoundError:
        df_new.write_parquet(path)
        print(f"  ✓ DDMV (created, {len(df_new):,})")

# ============================================================================
# SAVE
# ============================================================================

print("\nSaving outputs...")

# TEMP.DYDP
dydp.write_parquet(f"{DIRS['temp']}/DYDP.parquet")
print(f"  ✓ DYDP -> {DIRS['temp']}")

# TEMP.SAVG
if len(savg) > 0:
    savg.write_parquet(f"{DIRS['temp']}/SAVG.parquet")
    print(f"  ✓ SAVG -> {DIRS['temp']}")

# Monthly MIS files (SAS %MISAPPD)
upsert_monthly(dyposn, 'DYPOSN{month:02d}.parquet', 'mis')
upsert_monthly(dymvnt, 'DYMVNT{month:02d}.parquet', 'mis')
upsert_monthly(dyibu, 'DYIBU{month:02d}.parquet', 'mis')
upsert_monthly(dyddcr, 'DYDDCR{month:02d}.parquet', 'mis')
upsert_monthly(dybrdp, 'DYBRDP{month:02d}.parquet', 'mis')
upsert_monthly(dydps, 'DYDPS{month:02d}.parquet', 'mis')
upsert_monthly(dyace, 'DYACE{month:02d}.parquet', 'mis')

# SDRNGE (SAS %WISEPBB: overwrite via OUTPUT OUT=MIS.SDRNGE&REPTMON)
if is_weekly_or_monthend and len(sdrnge) > 0:
    sdrnge.write_parquet(f"{DIRS['mis']}/SDRNGE{reptmon:02d}.parquet")
    print(f"  ✓ SDRNGE{reptmon:02d} (overwrites, SAS %WISEPBB)")

# DDMV (SAS exact: delete REPTDATE = REPTDATE-31, then append)
append_ddmv_sas_faithful(ddmv)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✓ EIBDDEPE Complete!")
print("=" * 70)
print(f"\nReport Date:            {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"DDMV dedupe target:     {reptdat3.strftime('%d/%m/%Y')}  (REPTDATE - 31 days)")
print(f"\nDeposits: RM {totals['TOTSAVG'] + totals['TOTDMND']:>15,.2f}")
print(f"  Savings: RM {totals['TOTSAVG']:>15,.2f}")
print(f"  Demand:  RM {totals['TOTDMND']:>15,.2f}")
print(f"  Fixed:   RM {totals['TOTFD']:>15,.2f}")
print(f"\nIslamic: RM {totals['TOTSAVGI'] + totals['TOTDMNDI']:>15,.2f}")
print(f"  Savings: RM {totals['TOTSAVGI']:>15,.2f}")
print(f"  Demand:  RM {totals['TOTDMNDI']:>15,.2f}")
print(f"\nOverdrafts: RM {totals['TOTOVFT']:>15,.2f}")
print(f"\nRecords:")
print(f"  DYDP:   {len(dydp):>10,}")
print(f"  DYMVNT: {len(dymvnt):>10,}")
print(f"  DYIBU:  {len(dyibu):>10,}")
print(f"  DYBRDP: {len(dybrdp):>10,}")
print(f"  DYDDCR: {len(dyddcr):>10,}")
print(f"  DYDPS:  {len(dydps):>10,}")
print(f"  DYACE:  {len(dyace):>10,}")
print(f"  DDMV:   {len(ddmv):>10,}")
if is_weekly_or_monthend:
    print(f"  SDRNGE: {len(sdrnge):>10,}")
print("=" * 70)
