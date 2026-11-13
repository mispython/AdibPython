"""
EIBDDEPE - Daily Deposit Position Extract
DuckDB reads Parquet  ->  SASPy writes .sas7bdat to explicit directories.
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

# Physical output directories (created on disk by Python)
BASE_OUT  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE'
MIS_PATH  = f'{BASE_OUT}/MIS'
MISQ_PATH = f'{BASE_OUT}/MISQ'
TEMP_PATH = f'{BASE_OUT}/TEMP'

for p in (MIS_PATH, MISQ_PATH, TEMP_PATH):
    os.makedirs(p, exist_ok=True)

# SAS librefs (must match the LIBNAME assignments below)
SAS_LIB_MIS  = 'MIS'
SAS_LIB_MISQ = 'MISQ'
SAS_LIB_TEMP = 'TEMP'

# Product / filter constants
ACE_PRODUCTS = sorted(PBBDPFMT.ACE)
EXCL_PROD = [297, 298]
EXCL_OPEN = ['B', 'C', 'P']
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

# ============================================================================
# SAS SESSION + LIBNAME ASSIGNMENTS (do this BEFORE any save)
# ============================================================================

print("Connecting to SAS...")
sas = saspy.SASsession(cfgname='default')   # adjust cfgname if needed
print(f"SAS session: {sas}")

# Point librefs at the Python output directories.
# 'disp=new' lets SAS create a new library folder if it is empty.
sas.submit(f"""
    libname {SAS_LIB_MIS}  "{MIS_PATH}"  disp=new;
    libname {SAS_LIB_MISQ} "{MISQ_PATH}" disp=new;
    libname {SAS_LIB_TEMP} "{TEMP_PATH}" disp=new;
""")
print(sas.saslog())     # confirms libname assignment

# Sanity-check that the librefs resolved
check = sas.submit("""
    proc sql;
      select libname, path
      from dictionary.libnames
      where libname in ('MIS','MISQ','TEMP');
    quit;
""")
print(check['LST'])

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

def range_ddmove(a):
    return next((r for r in [300000,500000,1000000,1500000,2000000,3000000,4000000,5000000,10000000] if a < r), 10000001)
def range_mvtdep(a):
    return next((r for r in [5000,10000,30000,50000,75000] if a <= r), 80000)
def range_mvtace(a):
    return next((r for r in [5000,10000,30000,50000,75000,100000] if a <= r), 200000)

def is_ytd(opendt_str, closedt, year):
    try:
        s = re.sub(r'[^0-9]', '', str(opendt_str).split('.')[0] if '.' in str(opendt_str) else str(opendt_str))
        if len(s) >= 8 and datetime.strptime(s[:8], '%Y%m%d').date().year == year: return 1
    except: pass
    return 0

def calc_age(bdate, year, mon, day):
    if not bdate: return 0
    try:
        s = re.sub(r'[^0-9]', '', str(bdate).split('.')[0] if '.' in str(bdate) else str(bdate))
        if len(s) >= 8: bd = datetime.strptime(s[:8], '%Y%m%d').date()
        else:
            y = int(s[:2]) + (1900 if int(s[:2]) >= 70 else 2000)
            bd = datetime.strptime(f"{y}{s[2:6]}", '%Y%m%d').date()
        age = year - bd.year
        boundary = (bd.month == mon and bd.day > day) or bd.month > mon
        if age == AGELIMIT and boundary: return AGEBELOW
        if age == MAXAGE and boundary: return AGELIMIT
        return max(AGELIMIT, min(MAXAGE, age)) if age < AGELIMIT or age > MAXAGE else age
    except: return 0

def get_prodcd(deptype, prod, curbal):
    try:
        if deptype == 'S': return PBBDPFMT.SAPROD(prod)
        if deptype in ('D', 'N'):
            return PBBDPFMT.CAPROD(prod) if curbal >= 0 else PBBLNFMT.ODPROD(prod)
        if deptype == 'C': return PBBDPFMT.FDPRODD(prod)
    except: pass
    return ''

def days_in_month(year, month):
    if month == 2: return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]: return 30
    else: return 31

# ============================================================================
# READ PARQUET VIA DUCKDB (push SAS WHERE filters down)
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
rdate  = reptdate.strftime('%d%m%y')
rdate3 = reptdat3.strftime('%d%m%y')

print(f"  Date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"  DDMV dedupe target (REPTDATE - 31): {rdate3}")

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
    pl.struct(['DEPTYPE', 'PRODUCT', 'CURBAL']).map_elements(
        lambda x: get_prodcd(x['DEPTYPE'], x['PRODUCT'], x['CURBAL']), pl.String
    ).alias('PRODCD')
)

df = df.with_columns(
    pl.when(pl.col('DEPTYPE') == 'S').then(pl.lit('S')).otherwise(pl.lit('D')).alias('DPTYPE')
)

df = df.with_columns([
    pl.when(pl.col('PRODUCT').is_in(ACE_PRODUCTS))
      .then(pl.col('MOVEMENT').abs().map_elements(range_mvtace, pl.Int64))
      .otherwise(pl.col('MOVEMENT').abs().map_elements(range_mvtdep, pl.Int64))
      .alias('MVRANGE')
])
print(f"  Enriched: {len(df):,} records")

# ============================================================================
# SPLIT INTO DYDP / DDMV / SAVG
# ============================================================================

dydp_s = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0) &
                   (pl.col('PRODCD').is_in(['42120', '42320'])))
dydp_d = df.filter((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('CURBAL') >= 0) &
                   (pl.col('PRODCD').is_in(['42110', '42180', '42310'])))

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
    savg = df.filter((pl.col('DEPTYPE') == 'S') & (pl.col('CURBAL') >= 0)).with_columns([
        pl.struct(['BDATE']).map_elements(
            lambda x: calc_age(x['BDATE'], reptyear, reptmon, reptday), pl.Int64
        ).alias('AGE'),
        pl.col('CURBAL').map_elements(lambda x: PBBDPFMT.S1RANGE(x), pl.Float64).alias('RANGE'),
        pl.col('CURBAL').map_elements(lambda x: PBBDPFMT.S2RANGE(x), pl.Float64).alias('R2NGE'),
        pl.col('RACE').map_elements(lambda x: PBBDPFMT.RACE(x), pl.String).alias('RACE_FMT')
    ]).select(['ACCTNO','BRANCH','PRODUCT','DEPTYPE','NAME','OPENIND',
               'RANGE','RACE_FMT','BDATE','CURBAL','PURPOSE','PRODCD',
               'AGE','ACCYTD','OPENDT','R2NGE']).rename({'RACE_FMT':'RACE'})
else:
    savg = pl.DataFrame([])

# ============================================================================
# BRANCH SUMS + TOTALS
# ============================================================================

def branch_sum(df, valcol='CURBAL', cnt=False):
    if len(df) == 0: return {}
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
    'TOTOVFT':  safe_sum(od.with_columns(
        pl.col('PRODUCT').map_elements(lambda p: PBBLNFMT.ODPROD(p), pl.String).alias('ODPROD')
    ).filter(pl.col('ODPROD').is_in(['34180','34240','34380']))['CURBAL'].abs().sum()),
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
    pl.when(pl.col('CURBAL') - (pl.col('CREDIT') - pl.col('DEBIT')) < 0)
      .then(pl.col('CURBAL')).otherwise(pl.col('CREDIT') - pl.col('DEBIT')).alias('MOVEMENT_ADJ')
])
if len(dyddcr_base) > 0:
    dyddcr = dyddcr_base.with_columns(
        pl.col('MOVEMENT_ADJ').abs().map_elements(range_ddmove, pl.Int64).alias('RANGE')
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
# SAVE VIA SASPY  (writes .sas7bdat to MIS_PATH / MISQ_PATH / TEMP_PATH)
# ============================================================================

def save_sas(df_polars, libref, table, overwrite=True):
    if len(df_polars) == 0:
        print(f"  ⚠ {libref}.{table}: No data")
        return
    pdf = df_polars.to_pandas()
    sas.df2sd(pdf, table=table, libref=libref, replace=overwrite)
    print(f"  ✓ {libref}.{table}  ({len(df_polars):,} rows)")

def upsert_sas(df_new, libref, table):
    """SAS %MISAPPD: day 01 -> fresh; else delete-by-REPTDATE then append."""
    if len(df_new) == 0:
        print(f"  ⚠ {libref}.{table}: No data")
        return
    pdf_new = df_new.to_pandas()
    if reptday == 1:
        sas.df2sd(pdf_new, table=table, libref=libref, replace=True)
        print(f"  ✓ {libref}.{table} (new)")
        return
    if not sas.exist(table, libref=libref):
        sas.df2sd(pdf_new, table=table, libref=libref)
        print(f"  ✓ {libref}.{table} (created)")
        return
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
    """SAS MISQ.DDMV: delete REPTDATE = REPTDATE-31, then append."""
    if len(df_new) == 0:
        print("  ⚠ MISQ.DDMV: No data"); return
    pdf_new = df_new.to_pandas()
    if not sas.exist('DDMV', libref=SAS_LIB_MISQ):
        sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ)
        print(f"  ✓ MISQ.DDMV (created, {len(df_new):,})")
        return
    dt_str = reptdat3.strftime('%d%b%Y').upper()
    sas.submit(f"""
        data {SAS_LIB_MISQ}.DDMV;
          set {SAS_LIB_MISQ}.DDMV;
          if REPTDATE = '{dt_str}'d then delete;
        run;
    """)
    sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ, replace=False)
    print(f"  ✓ MISQ.DDMV (appended, {len(df_new):,})")

print("\nSaving SAS datasets...")
save_sas(dydp, SAS_LIB_TEMP, 'DYDP')
if is_weekly_or_monthend and len(savg) > 0:
    save_sas(savg, SAS_LIB_TEMP, 'SAVG')

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
print(f"\nOutput directories:")
print(f"  MIS  -> {MIS_PATH}")
print(f"  MISQ -> {MISQ_PATH}")
print(f"  TEMP -> {TEMP_PATH}")
print(f"\nTotals:  TOTSAVG={totals['TOTSAVG']:,.2f}  TOTDMND={totals['TOTDMND']:,.2f}  "
      f"TOTFD={totals['TOTFD']:,.2f}  TOTOVFT={totals['TOTOVFT']:,.2f}")
print(f"Records: DYDP={len(dydp):,}  DYMVNT={len(dymvnt):,}  DYIBU={len(dyibu):,}  "
      f"DYBRDP={len(dybrdp):,}  DYDDCR={len(dyddcr):,}  DYDPS={len(dydps):,}  "
      f"DYACE={len(dyace):,}  DDMV={len(ddmv):,}"
      + (f"  SDRNGE={len(sdrnge):,}" if is_weekly_or_monthend else ""))
print("=" * 70)

sas.endsas()
