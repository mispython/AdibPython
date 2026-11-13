"""
EIBDDEPE - Daily Deposit Position Extract
Faithful port of original SAS EIBDDEPE program.

Performance notes:
- DuckDB reads Parquet with predicate pushdown.
- All transforms are vectorized (no map_elements on 8.8M rows).
- Format lookups (SAPROD, CAPROD, FDPRODD, ODPROD, S1RANGE, S2RANGE, $RACE)
  are compiled once into Polars lookup DataFrames / when-then chains.
- Branch arrays computed in a single group_by pass.
- DYDP (8.8M rows) is written via PROC IMPORT (CSV -> SAS) for speed.
  All smaller summary files continue via SASPy df2sd.
"""

import duckdb
import polars as pl
import saspy
from datetime import datetime, timedelta
import os
import re

import PBBDPFMT
import PBBLNFMT

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'

BASE_OUT  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE'
MIS_PATH  = f'{BASE_OUT}/MIS'
MISQ_PATH = f'{BASE_OUT}/MISQ'
TEMP_PATH = f'{BASE_OUT}/TEMP'

# Scratch directory for the DYDP CSV (fast local storage preferred)
SCRATCH_DIR = TEMP_PATH
DYDP_CSV = f'{SCRATCH_DIR}/DYDP_import.csv'

for p in (MIS_PATH, MISQ_PATH, TEMP_PATH):
    os.makedirs(p, exist_ok=True)

SAS_LIB_MIS  = 'MIS'
SAS_LIB_MISQ = 'MISQ'
SAS_LIB_TEMP = 'TEMP'

ACE_PRODUCTS = sorted(PBBDPFMT.ACE)
EXCL_PROD = [297, 298]
EXCL_OPEN = ['B', 'C', 'P']
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

# ============================================================================
# SAS SESSION
# ============================================================================

print("Connecting to SAS...")
sas = saspy.SASsession(cfgname='default')
print(f"SAS session: {sas}")

sas.submit(f"""
    libname {SAS_LIB_MIS}  "{MIS_PATH}"  disp=new;
    libname {SAS_LIB_MISQ} "{MISQ_PATH}" disp=new;
    libname {SAS_LIB_TEMP} "{TEMP_PATH}" disp=new;
""")

# ============================================================================
# HELPERS
# ============================================================================

def parse_date(val):
    if not val: return None
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

def days_in_month(year, month):
    if month == 2: return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]: return 30
    else: return 31

# ============================================================================
# COMPILE FORMAT LOOKUP TABLES (once)
# ============================================================================

def _rules_to_lookup(format_name, library):
    rules = library.FORMATS[format_name]['rules']
    keys, vals = [], []
    for lo, hi, exlo, exhi, label, _opts in rules:
        if isinstance(lo, int) and isinstance(hi, int) and lo == hi:
            keys.append(lo); vals.append(label)
    return pl.DataFrame({'PRODUCT': keys, format_name: vals})

def _build_odprod_lookup():
    rules = PBBLNFMT.FORMATS['ODPROD']['rules']
    keys, vals = [], []
    for lo, hi, exlo, exhi, label, _opts in rules:
        if isinstance(lo, int) and isinstance(hi, int) and lo == hi:
            keys.append(lo); vals.append(label)
    return pl.DataFrame({'PRODUCT': keys, 'ODPROD': vals})

SAPROD_DF  = _rules_to_lookup('SAPROD',  PBBDPFMT)
CAPROD_DF  = _rules_to_lookup('CAPROD',  PBBDPFMT)
FDPRODD_DF = _rules_to_lookup('FDPRODD', PBBDPFMT)
ODPROD_DF  = _build_odprod_lookup()

SAPROD_DEFAULT, CAPROD_DEFAULT = '42120', '42110'
FDPRODD_DEFAULT, ODPROD_DEFAULT = '42130', '34180'

def build_mvrange_expr(col='MOVEMENT'):
    absm = pl.col(col).abs()
    ace = (
        pl.when(absm <= 5000).then(5000)
         .when(absm <= 10000).then(10000)
         .when(absm <= 30000).then(30000)
         .when(absm <= 50000).then(50000)
         .when(absm <= 75000).then(75000)
         .when(absm <= 100000).then(100000)
         .otherwise(200000)
    )
    non_ace = (
        pl.when(absm <= 5000).then(5000)
         .when(absm <= 10000).then(10000)
         .when(absm <= 30000).then(30000)
         .when(absm <= 50000).then(50000)
         .when(absm <= 75000).then(75000)
         .otherwise(80000)
    )
    return pl.when(pl.col('PRODUCT').is_in(ACE_PRODUCTS)).then(ace).otherwise(non_ace)

def build_ddmove_expr(col='MOVEMENT_ADJ'):
    a = pl.col(col).abs()
    return (
        pl.when(a < 300000).then(300000)
         .when(a < 500000).then(500000)
         .when(a < 1000000).then(1000000)
         .when(a < 1500000).then(1500000)
         .when(a < 2000000).then(2000000)
         .when(a < 3000000).then(3000000)
         .when(a < 4000000).then(4000000)
         .when(a < 5000000).then(5000000)
         .when(a < 10000000).then(10000000)
         .otherwise(10000001)
    )

S1_BOUNDS = list(PBBDPFMT._S1RANGE_BOUNDS) + [PBBDPFMT._S1RANGE_HIGH]
S2_BOUNDS = list(PBBDPFMT._S2RANGE_BOUNDS) + [PBBDPFMT._S2RANGE_HIGH]

def build_range_expr(col, bounds):
    c = pl.col(col)
    expr = pl.lit(float(bounds[-1]))
    for b in reversed(bounds[:-1]):
        expr = pl.when(c < b).then(float(b)).otherwise(expr)
    return expr

# ============================================================================
# READ PARQUET VIA DUCKDB
# ============================================================================

print("\nReading DPTRBL Parquet via DuckDB...")
con = duckdb.connect()
df = con.execute(f"""
    SELECT * FROM read_parquet('{INPUT_FILE}')
    WHERE BANKNO = 33 AND REPTNO = 1001 AND FMTCODE = 1
      AND OPENIND NOT IN ('B','C','P')
      AND PRODUCT NOT IN (297, 298)
""").pl()
con.close()
print(f"  Loaded {len(df):,} filtered records")

reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
    if col in df.columns:
        reptdate = parse_date(df.select(col).row(0)[0])
        if reptdate: break
if not reptdate: reptdate = datetime.now().date()

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'
reptdat3 = reptdate - timedelta(days=31)
rdate3 = reptdat3.strftime('%d%m%y')

print(f"  Date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"  DDMV dedupe target (REPTDATE - 31): {rdate3}")

# ============================================================================
# VECTORIZED ENRICH
# ============================================================================

print("\nEnriching (vectorized)...")

df = df.with_columns([
    pl.when(pl.col('BRANCH') == 132).then(168).otherwise(pl.col('BRANCH')).alias('BRANCH'),
    (pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT'),
    pl.col('CURBAL').alias('DYDPBAL'),
    pl.lit(reptdate).alias('REPTDATE'),
])

df = df.with_columns(
    pl.col('OPENDT').cast(pl.String).str.replace_all(r'\.', '')
      .str.slice(0, 8).str.strptime(pl.Date, '%Y%m%d', strict=False).dt.year()
      .alias('OPEN_YEAR')
).with_columns(
    pl.when((pl.col('OPEN_YEAR') == reptyear) & (pl.col('CLOSEDT') == 0))
      .then(1).otherwise(0).alias('ACCYTD')
).drop('OPEN_YEAR')

df = (
    df
    .join(SAPROD_DF,  on='PRODUCT', how='left')
    .join(CAPROD_DF,  on='PRODUCT', how='left')
    .join(FDPRODD_DF, on='PRODUCT', how='left')
    .join(ODPROD_DF,  on='PRODUCT', how='left')
)

df = df.with_columns(
    pl.when(pl.col('DEPTYPE') == 'S')
        .then(pl.col('SAPROD').fill_null(SAPROD_DEFAULT))
      .when((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0))
        .then(pl.col('CAPROD').fill_null(CAPROD_DEFAULT))
      .when((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0))
        .then(pl.col('ODPROD').fill_null(ODPROD_DEFAULT))
      .when(pl.col('DEPTYPE') == 'C')
        .then(pl.col('FDPRODD').fill_null(FDPRODD_DEFAULT))
      .otherwise(pl.lit(''))
      .alias('PRODCD')
).drop(['SAPROD', 'CAPROD', 'FDPRODD', 'ODPROD'])

df = df.with_columns(
    pl.when(pl.col('DEPTYPE') == 'S').then(pl.lit('S')).otherwise(pl.lit('D')).alias('DPTYPE')
)

df = df.with_columns(build_mvrange_expr('MOVEMENT').alias('MVRANGE'))

print(f"  Enriched: {len(df):,} records")

# ============================================================================
# SPLIT INTO DYDP / DDMV / SAVG
# ============================================================================

dydp_s = df.filter(
    (pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0) &
    (pl.col('PRODCD').is_in(['42120', '42320']))
)
dydp_d = df.filter(
    (pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0) &
    (pl.col('PRODCD').is_in(['42110', '42180', '42310']))
)

dydp = pl.concat([dydp_s, dydp_d]).select([
    'REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
    'DPTYPE','PRODUCT','PRODCD','CURBAL','DYDPBAL','APPRLIMT',
    'ACCYTD','OPENDT','MOVEMENT','MVRANGE','CUSTCODE','SECOND'
])

ddmv = df.filter(
    (pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0) &
    (pl.col('PRODCD').is_in(['42110', '42310']))
).select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
          'PRODCD','PRODUCT','CURBAL'])

mth = days_in_month(reptyear, reptmon)
is_weekly_or_monthend = (reptday in [8, 15, 22]) or (reptday == mth)

if is_weekly_or_monthend:
    savg = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0))

    savg = savg.with_columns(
        pl.col('BDATE').cast(pl.String).str.replace_all(r'\.', '')
          .str.slice(0, 8).str.strptime(pl.Date, '%Y%m%d', strict=False).alias('BDATE_DT')
    ).with_columns(
        (reptyear - pl.col('BDATE_DT').dt.year()).alias('RAW_AGE')
    ).with_columns(
        pl.when(pl.col('RAW_AGE') == AGELIMIT).then(
            pl.when(
                ((pl.col('BDATE_DT').dt.month() == reptmon) &
                 (pl.col('BDATE_DT').dt.day() > reptday)) |
                (pl.col('BDATE_DT').dt.month() > reptmon)
            ).then(AGEBELOW).otherwise(AGELIMIT)
        ).when(pl.col('RAW_AGE') == MAXAGE).then(
            pl.when(
                ((pl.col('BDATE_DT').dt.month() == reptmon) &
                 (pl.col('BDATE_DT').dt.day() > reptday)) |
                (pl.col('BDATE_DT').dt.month() > reptmon)
            ).then(AGELIMIT).otherwise(MAXAGE)
        ).when(pl.col('RAW_AGE') > MAXAGE).then(MAXAGE)
         .when(pl.col('RAW_AGE') < AGELIMIT).then(AGEBELOW)
         .otherwise(AGELIMIT).alias('AGE')
    ).with_columns([
        build_range_expr('CURBAL', S1_BOUNDS).alias('RANGE'),
        build_range_expr('CURBAL', S2_BOUNDS).alias('R2NGE'),
        pl.when(pl.col('RACE').is_in(['0', '1', '2', '3']))
          .then(pl.col('RACE')).otherwise(pl.lit('0')).alias('RACE'),
    ]).select([
        'ACCTNO','BRANCH','PRODUCT','DEPTYPE','NAME','OPENIND',
        'RANGE','RACE','BDATE','CURBAL','PURPOSE','PRODCD',
        'AGE','ACCYTD','OPENDT','R2NGE'
    ])
else:
    savg = pl.DataFrame([])

# ============================================================================
# BRANCH SUMS + TOTALS — single group_by pass
# ============================================================================

print("\nAggregating branch totals (single pass)...")

od = df.filter((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') < 0))

branch_agg = df.group_by('BRANCH').agg([
    pl.col('CURBAL').filter((pl.col('DEPTYPE') == 'S') & (pl.col('PRODCD') == '42120')).sum().alias('SA'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE') == 'S') & (pl.col('PRODCD') == '42320')).sum().alias('SACAI'),
    pl.len().filter((pl.col('DEPTYPE') == 'S') & (pl.col('PRODCD') == '42320')).alias('SNO'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE') == 'S') & (pl.col('PRODCD') == '42320') & (pl.col('PRODUCT') == 214)).sum().alias('MBS'),
    pl.len().filter((pl.col('DEPTYPE') == 'S') & (pl.col('PRODCD') == '42320') & (pl.col('PRODUCT') == 214)).alias('MBSNO'),

    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42110')).sum().alias('CA'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42180')).sum().alias('HDA'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310')).sum().alias('CAI_pos'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310')).alias('CNO_pos'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61,161]))).sum().alias('CAIG_pos'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61,161]))).alias('CGNO_pos'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63,163]))).sum().alias('CAIH_pos'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63,163]))).alias('CHNO_pos'),

    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('CURBAL') < 0)).sum().alias('CAI_neg'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('CURBAL') < 0)).alias('CNO_neg'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61,161])) & (pl.col('CURBAL') < 0)).sum().alias('CAIG_neg'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42310') & (pl.col('PRODUCT').is_in([61,161])) & (pl.col('CURBAL') < 0)).alias('CGNO_neg'),
    pl.col('CURBAL').filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63,163])) & (pl.col('CURBAL') < 0)).sum().alias('CAIH_neg'),
    pl.len().filter((pl.col('DEPTYPE').is_in(['D','N'])) & (pl.col('PRODCD') == '42180') & (pl.col('PRODUCT').is_in([63,163])) & (pl.col('CURBAL') < 0)).alias('CHNO_neg'),
])

def _dict(colname):
    return dict(zip(branch_agg['BRANCH'].to_list(), branch_agg[colname].to_list()))

SA_branch   = _dict('SA')
SACAI_branch= _dict('SACAI')
SAI_branch  = dict(SACAI_branch)
SNO_branch  = _dict('SNO')
MBS_branch  = _dict('MBS')
MBSNO_branch= _dict('MBSNO')
CA_branch   = _dict('CA')
HDA_branch  = _dict('HDA')
CAIG_branch = {k: _dict('CAIG_pos').get(k,0) + _dict('CAIG_neg').get(k,0) for k in branch_agg['BRANCH'].to_list()}
CGNO_branch = {k: _dict('CGNO_pos').get(k,0) + _dict('CGNO_neg').get(k,0) for k in branch_agg['BRANCH'].to_list()}
CAIH_branch = {k: _dict('CAIH_pos').get(k,0) + _dict('CAIH_neg').get(k,0) for k in branch_agg['BRANCH'].to_list()}
CHNO_branch = {k: _dict('CHNO_pos').get(k,0) + _dict('CHNO_neg').get(k,0) for k in branch_agg['BRANCH'].to_list()}
CAI_branch  = {
    k: _dict('CAI_pos').get(k,0) + _dict('CAI_neg').get(k,0)
       + _dict('CAIH_pos').get(k,0) + _dict('CAIH_neg').get(k,0)
    for k in branch_agg['BRANCH'].to_list()
}
CNO_branch  = {
    k: _dict('CNO_pos').get(k,0) + _dict('CNO_neg').get(k,0)
       + _dict('CHNO_pos').get(k,0) + _dict('CHNO_neg').get(k,0)
    for k in branch_agg['BRANCH'].to_list()
}

def safe_sum(s): return float(s) if s is not None else 0.0

od_with_odprod = (
    od.select(['PRODUCT', 'CURBAL'])
      .join(ODPROD_DF, on='PRODUCT', how='left')
      .with_columns(pl.col('ODPROD').fill_null(ODPROD_DEFAULT))
)

totals = {
    'TOTSAVG':  safe_sum(dydp_s.filter(pl.col('PRODCD') == '42120')['CURBAL'].sum()),
    'TOTSAVGI': safe_sum(dydp_s.filter(pl.col('PRODCD') == '42320')['CURBAL'].sum()),
    'TOTDMND':  safe_sum(dydp_d.filter(pl.col('PRODCD').is_in(['42110', '42180']))['CURBAL'].sum()),
    'TOTDMNDI': safe_sum(dydp_d.filter(pl.col('PRODCD') == '42310')['CURBAL'].sum()),
    'TOTVOSF':  safe_sum(dydp_d.filter(pl.col('PRODUCT') == 105)['CURBAL'].sum()),
    'TOTVOSC':  safe_sum(dydp_d.filter(pl.col('PRODUCT') == 104)['CURBAL'].sum()),
    'OVDVOSF':  safe_sum(od.filter(pl.col('PRODUCT') == 105)['CURBAL'].sum()),
    'OVDVOSC':  safe_sum(od.filter(pl.col('PRODUCT') == 104)['CURBAL'].sum()),
    'TOTOVFT':  safe_sum(od_with_odprod.filter(pl.col('ODPROD').is_in(['34180','34240','34380']))['CURBAL'].abs().sum()),
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
                            MBS_branch, MBSNO_branch] if d]))
dyibu_rows = []
for br in all_branches:
    row = {'REPTDATE': reptdate, 'BRANCH': br,
           'SAI': SAI_branch.get(br, 0.0), 'CAI': CAI_branch.get(br, 0.0),
           'CAIG': CAIG_branch.get(br, 0.0), 'CAIH': CAIH_branch.get(br, 0.0),
           'MBS': MBS_branch.get(br, 0.0), 'SNO': SNO_branch.get(br, 0),
           'CNO': CNO_branch.get(br, 0), 'CGNO': CGNO_branch.get(br, 0),
           'CHNO': CHNO_branch.get(br, 0), 'MBSNO': MBSNO_branch.get(br, 0)}
    if any(v != 0 for k, v in row.items() if k not in ('REPTDATE', 'BRANCH')):
        dyibu_rows.append(row)
dyibu = pl.DataFrame(dyibu_rows) if dyibu_rows else pl.DataFrame(
    schema={'REPTDATE': pl.Date, 'BRANCH': pl.Int64, 'SAI': pl.Float64,
            'CAI': pl.Float64, 'CAIG': pl.Float64, 'CAIH': pl.Float64,
            'MBS': pl.Float64, 'SNO': pl.Int64, 'CNO': pl.Int64,
            'CGNO': pl.Int64, 'CHNO': pl.Int64, 'MBSNO': pl.Int64})

dymvnt = dydp.filter(
    ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
    ((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('DPTYPE') == 'D') &
     (((pl.col('MOVEMENT').abs() >= 100000) & (pl.col('PRODUCT').is_in(ACE_PRODUCTS))) |
      (pl.col('MOVEMENT').abs() >= 1000000)))
).select(['REPTDATE','BRANCH','ACCTNO','NAME','DEBIT','CREDIT','DEPTYPE',
          'PRODUCT','CURBAL','APPRLIMT','CUSTCODE','SECOND'])

dybrdp = dydp.filter(
    (~pl.col('PRODCD').is_in(['42320', '42310'])) & (~pl.col('PRODUCT').is_in([104, 105]))
).group_by(['BRANCH', 'DPTYPE', 'REPTDATE']).agg(
    pl.col('DYDPBAL').sum().alias('BALANCE')
).select(['BRANCH', 'DPTYPE', 'REPTDATE', 'BALANCE'])

dyddcr_base = dydp.filter(
    (pl.col('DPTYPE') == 'D') & (~pl.col('PRODUCT').is_in(ACE_PRODUCTS)) &
    (pl.col('PRODCD') != '42310') & (pl.col('PRODUCT') != 72) & (pl.col('CURBAL') >= 0)
).with_columns([
    (pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT'))).alias('PREBAL'),
]).with_columns(
    pl.when(pl.col('PREBAL') < 0).then(pl.col('CURBAL'))
      .otherwise(pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT_ADJ')
)
if len(dyddcr_base) > 0:
    dyddcr = dyddcr_base.with_columns(
        build_ddmove_expr('MOVEMENT_ADJ').alias('RANGE')
    ).group_by(['REPTDATE', 'RANGE']).agg(pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT'))
else:
    dyddcr = pl.DataFrame(schema={'REPTDATE': pl.Date, 'RANGE': pl.Int64, 'MOVEMENT': pl.Float64})

dydps = dydp.filter(
    (pl.col('DPTYPE') == 'S') & (~pl.col('PRODUCT').is_in(ACE_PRODUCTS)) &
    (pl.col('PRODCD') == '42120')
).group_by(['REPTDATE', 'MVRANGE']).agg(pl.col('MOVEMENT').sum())

dyace = dydp.filter(
    (pl.col('DPTYPE') == 'D') & (pl.col('PRODUCT').is_in(ACE_PRODUCTS))
).group_by(['REPTDATE', 'MVRANGE']).agg(pl.col('MOVEMENT').sum())

if is_weekly_or_monthend and len(savg) > 0:
    sdrnge = savg.group_by(['BRANCH', 'RACE', 'PRODCD', 'PRODUCT', 'RANGE', 'AGE', 'R2NGE']).agg([
        pl.len().alias('NOACCT'), pl.col('CURBAL').sum(), pl.col('ACCYTD').sum()
    ])
else:
    sdrnge = pl.DataFrame([])

# ============================================================================
# SAVE VIA SASPY
# ============================================================================

def save_sas(df_polars, libref, table):
    if len(df_polars) == 0:
        print(f"  ⚠ {libref}.{table}: No data"); return
    pdf = df_polars.to_pandas()
    sas.df2sd(pdf, table=table, libref=libref, replace=True)
    print(f"  ✓ {libref}.{table}  ({len(df_polars):,} rows)")

def upsert_sas(df_new, libref, table):
    if len(df_new) == 0:
        print(f"  ⚠ {libref}.{table}: No data"); return
    pdf_new = df_new.to_pandas()
    if reptday == 1:
        sas.df2sd(pdf_new, table=table, libref=libref, replace=True)
        print(f"  ✓ {libref}.{table} (new)"); return
    if not sas.exist(table, libref=libref):
        sas.df2sd(pdf_new, table=table, libref=libref)
        print(f"  ✓ {libref}.{table} (created)"); return
    dt_str = reptdate.strftime('%d%b%Y').upper()
    sas.submit(f"""
        data {libref}.{table};
          set {libref}.{table};
          if REPTDATE = '{dt_str}'d then delete;
        run;
    """)
    sas.df2sd(pdf_new, table=table, libref=libref, replace=False)
    print(f"  ✓ {libref}.{table} (appended)")

def append_ddmv_sas(df_new):
    if len(df_new) == 0:
        print("  ⚠ MISQ.DDMV: No data"); return
    pdf_new = df_new.to_pandas()
    if not sas.exist('DDMV', libref=SAS_LIB_MISQ):
        sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ)
        print(f"  ✓ MISQ.DDMV (created, {len(df_new):,})"); return
    dt_str = reptdat3.strftime('%d%b%Y').upper()
    sas.submit(f"""
        data {SAS_LIB_MISQ}.DDMV;
          set {SAS_LIB_MISQ}.DDMV;
          if REPTDATE = '{dt_str}'d then delete;
        run;
    """)
    sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ, replace=False)
    print(f"  ✓ MISQ.DDMV (appended, {len(df_new):,})")

# ============================================================================
# OPTION A: DYDP via PROC IMPORT (CSV -> SAS)
# ============================================================================

def save_dydp_via_proc_import(df_polars, csv_path):
    """DYDP (8.8M rows): write CSV, then PROC IMPORT inside SAS."""
    if len(df_polars) == 0:
        print("  ⚠ DYDP: No data"); return
    # Polars write_csv is fast (vectorized)
    df_polars.write_csv(csv_path)
    print(f"  ✓ CSV written: {csv_path} ({len(df_polars):,} rows)")

    # Force SAS to drop the existing TEMP.DYDP (matches SAS %MISAPPD day-01 logic;
    # for non-day-01 runs DYDP is a temp work file, so overwriting is fine)
    sas.submit(f"""
        proc datasets lib={SAS_LIB_TEMP} nolist;
          delete DYDP;
        run;

        proc import datafile="{csv_path}"
             out={SAS_LIB_TEMP}.DYDP
             dbms=csv
             replace;
          getnames=yes;
          guessingrows=max;
        run;
    """)
    print(f"  ✓ {SAS_LIB_TEMP}.DYDP created via PROC IMPORT")

    # Clean up the scratch CSV
    try:
        os.remove(csv_path)
        print(f"  ✓ Removed scratch CSV: {csv_path}")
    except OSError as e:
        print(f"  ⚠ Could not remove {csv_path}: {e}")

# ============================================================================
# SAVE ALL OUTPUTS
# ============================================================================

print("\nSaving SAS datasets...")

# DYDP: 8.8M rows -> PROC IMPORT path
save_dydp_via_proc_import(dydp, DYDP_CSV)

# SAVG: weekly/month-end only, via df2sd (small)
if is_weekly_or_monthend and len(savg) > 0:
    save_sas(savg, SAS_LIB_TEMP, 'SAVG')

# All monthly files via df2sd (small aggregates)
upsert_sas(dyposn, SAS_LIB_MIS, f'DYPOSN{reptmon:02d}')
upsert_sas(dymvnt, SAS_LIB_MIS, f'DYMVNT{reptmon:02d}')
upsert_sas(dyibu,  SAS_LIB_MIS, f'DYIBU{reptmon:02d}')
upsert_sas(dyddcr, SAS_LIB_MIS, f'DYDDCR{reptmon:02d}')
upsert_sas(dybrdp, SAS_LIB_MIS, f'DYBRDP{reptmon:02d}')
upsert_sas(dydps,  SAS_LIB_MIS, f'DYDPS{reptmon:02d}')
upsert_sas(dyace,  SAS_LIB_MIS, f'DYACE{reptmon:02d}')

if is_weekly_or_monthend and len(sdrnge) > 0:
    save_sas(sdrnge, SAS_LIB_MIS, f'SDRNGE{reptmon:02d}')

append_ddmv_sas(ddmv)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✓ EIBDDEPE Complete  (DuckDB input → SAS7BDAT output via SASPy)")
print("=" * 70)
print(f"Report Date : {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"DDMV dedupe : {reptdat3.strftime('%d/%m/%Y')} (REPTDATE - 31 days)")
print(f"Output dirs : MIS={MIS_PATH}  MISQ={MISQ_PATH}  TEMP={TEMP_PATH}")
print(f"Records     : DYDP={len(dydp):,}  DYMVNT={len(dymvnt):,}  DYIBU={len(dyibu):,}  "
      f"DYBRDP={len(dybrdp):,}  DYDDCR={len(dyddcr):,}  DYDPS={len(dydps):,}  "
      f"DYACE={len(dyace):,}  DDMV={len(ddmv):,}"
      + (f"  SDRNGE={len(sdrnge):,}" if is_weekly_or_monthend else ""))
print("=" * 70)

sas.endsas()
