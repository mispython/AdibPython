"""
EIBDDEPE - Daily Deposit Position Extract
Uses DuckDB for input (Parquet) and SASPy for output (SAS datasets).
"""

import duckdb
import polars as pl
import saspy
from datetime import datetime, timedelta
import os
import re

# Import SAS format libraries
import PBBDPFMT
import PBBLNFMT

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'

# SAS library paths (must match sascfg_personal.py)
SAS_LIB_MIS  = 'MIS'   # SAP.PBB.MIS.D&REPTYEAR
SAS_LIB_MISQ = 'MISQ'  # SAP.PBB.MIS.OD.SASDATA
SAS_LIB_TEMP = 'TEMP'  # Work library (or a dedicated temp lib)

# Initialize SAS session
print("Connecting to SAS...")
sas = saspy.SASsession(cfgname='default')  # adjust cfgname if needed
print(f"SAS session established: {sas}")

# ACE products from PBBDPFMT
ACE_PRODUCTS = sorted(PBBDPFMT.ACE)
EXCL_PROD = [297, 298]
EXCL_OPEN = ['B', 'C', 'P']
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

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
            if curbal >= 0: return PBBDPFMT.CAPROD(prod)
            else: return PBBLNFMT.ODPROD(prod)
        if deptype == 'C': return PBBDPFMT.FDPRODD(prod)
    except: pass
    return ''

def days_in_month(year, month):
    if month == 2: return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]: return 30
    else: return 31

# ============================================================================
# 1. READ PARQUET VIA DUCKDB (predicate pushdown)
# ============================================================================

print("\nReading DPTRBL Parquet via DuckDB...")

# Push the SAS WHERE conditions into DuckDB so only matching rows are materialized
con = duckdb.connect()
df = con.execute(f"""
    SELECT * FROM read_parquet('{INPUT_FILE}')
    WHERE BANKNO = 33 AND REPTNO = 1001 AND FMTCODE = 1
      AND OPENIND NOT IN ('B','C','P')
      AND PRODUCT NOT IN (297, 298)
""").pl()
con.close()
print(f"  Loaded {len(df):,} filtered records")

# Get report date
reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
    if col in df.columns:
        reptdate = parse_date(df.select(col).row(0)[0])
        if reptdate: break
if not reptdate: reptdate = datetime.now().date()

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'
reptdat3 = reptdate - timedelta(days=31)
rdate = reptdate.strftime('%d%m%y')
rdate3 = reptdat3.strftime('%d%m%y')

print(f"  Date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"  DDMV dedupe target (REPTDATE - 31): {rdate3}")

# ============================================================================
# 2. ENRICH (Polars transforms)
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
# 3. SPLIT & AGGREGATE (same as before)
# ============================================================================

# (The full logic for dydp_s, dydp_d, ddmv, savg, branch sums, totals,
#  dyposn, dyibu, dymvnt, dybrdp, dyddcr, dydps, dyace, sdrnge is unchanged.
#  To keep this answer focused on the input/output change, it is assumed
#  that all the Polars transformations from the previous version are kept.)

# ... [ALL THE POLARS TRANSFORMATIONS FROM THE PREVIOUS VERSION GO HERE] ...

# ============================================================================
# 4. SAVE VIA SASPY (write .sas7bdat)
# ============================================================================

def save_sas(df_polars, libref, table, description=""):
    """Convert Polars -> Pandas -> SAS dataset via SASPy."""
    if len(df_polars) == 0:
        print(f"  ⚠ {libref}.{table}: No data")
        return
    # SASPy expects a pandas DataFrame
    pdf = df_polars.to_pandas()
    try:
        sas.df2sd(pdf, table=table, libref=libref)
        print(f"  ✓ {libref}.{table} ({len(df_polars):,} rows) {description}")
    except Exception as e:
        print(f"  ✗ {libref}.{table}: {e}")

def upsert_sas(df_new, libref, table, dedupe_date=None):
    """
    SAS %MISAPPD logic:
    - Day 01: delete & recreate
    - Else: delete rows with REPTDATE = dedupe_date, then append
    """
    if len(df_new) == 0:
        print(f"  ⚠ {libref}.{table}: No data")
        return
    pdf_new = df_new.to_pandas()

    if reptday == 1:
        # Drop and recreate
        sas.submit(f"proc datasets lib={libref} nolist; delete {table}; run;")
        sas.df2sd(pdf_new, table=table, libref=libref)
        print(f"  ✓ {libref}.{table} (new)")
    else:
        # Check if table exists, then delete matching date and append
        exists = sas.exist(table, libref=libref)
        if not exists:
            sas.df2sd(pdf_new, table=table, libref=libref)
            print(f"  ✓ {libref}.{table} (created)")
        else:
            # Delete rows with same REPTDATE
            dt_str = dedupe_date.strftime('%d%b%Y').upper()
            sas.submit(f"""
                data {libref}.{table};
                  set {libref}.{table};
                  if REPTDATE = '{dt_str}'d then delete;
                run;
            """)
            sas.df2sd(pdf_new, table=table, libref=libref)
            print(f"  ✓ {libref}.{table} (appended)")

def append_ddmv_sas(df_new):
    """SAS MISQ.DDMV: delete REPTDATE = REPTDATE-31, then append."""
    if len(df_new) == 0:
        print("  ⚠ DDMV: No data")
        return
    pdf_new = df_new.to_pandas()
    exists = sas.exist('DDMV', libref=SAS_LIB_MISQ)
    if not exists:
        sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ)
        print(f"  ✓ DDMV (created, {len(df_new):,})")
    else:
        dt_str = reptdat3.strftime('%d%b%Y').upper()
        sas.submit(f"""
            data {SAS_LIB_MISQ}.DDMV;
              set {SAS_LIB_MISQ}.DDMV;
              if REPTDATE = '{dt_str}'d then delete;
            run;
        """)
        sas.df2sd(pdf_new, table='DDMV', libref=SAS_LIB_MISQ)
        print(f"  ✓ DDMV (appended, {len(df_new):,})")

# --- Save all outputs ---
print("\nSaving SAS datasets...")

# TEMP.DYDP
save_sas(dydp, SAS_LIB_TEMP, 'DYDP', '(temp detail)')

# TEMP.SAVG (weekly/month-end only)
if is_weekly_or_monthend and len(savg) > 0:
    save_sas(savg, SAS_LIB_TEMP, 'SAVG', '(temp savings)')

# Monthly MIS files
upsert_sas(dyposn, SAS_LIB_MIS, f'DYPOSN{reptmon:02d}', reptdate)
upsert_sas(dymvnt, SAS_LIB_MIS, f'DYMVNT{reptmon:02d}', reptdate)
upsert_sas(dyibu,  SAS_LIB_MIS, f'DYIBU{reptmon:02d}',  reptdate)
upsert_sas(dyddcr, SAS_LIB_MIS, f'DYDDCR{reptmon:02d}', reptdate)
upsert_sas(dybrdp, SAS_LIB_MIS, f'DYBRDP{reptmon:02d}', reptdate)
upsert_sas(dydps,  SAS_LIB_MIS, f'DYDPS{reptmon:02d}',  reptdate)
upsert_sas(dyace,  SAS_LIB_MIS, f'DYACE{reptmon:02d}',  reptdate)

# SDRNGE (overwrite)
if is_weekly_or_monthend and len(sdrnge) > 0:
    save_sas(sdrnge, SAS_LIB_MIS, f'SDRNGE{reptmon:02d}', '(overwrite)')

# DDMV (rolling ~1 month)
append_ddmv_sas(ddmv)

# ============================================================================
# 5. CLEANUP
# ============================================================================

sas.endsas()
print("\n" + "=" * 70)
print("✓ EIBDDEPE Complete (DuckDB input → SAS7BDAT output)")
print("=" * 70)
