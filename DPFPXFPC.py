"""
EIIFTXT3 - ESMR 2009-16 (SEND FILE TO LNS) - 3RD EXTRACTION
Bad Debt Write-Off for PIBB (Islamic) HP accounts.
RIND = 'I'.

Key differences from EIIFTXT1/EIIFTXT2:
  - RIND = 'I' (Islamic)
  - ECSRIND always '' (SAS quirk: unquoted Y)
  - NPL keeps BRNO/BRABBR/BORSTAT/DAYS
  - MTHPDUE width 2, output LRECL=750
  - Final output has @133 TOTAL, @374 CAP, @391 LATECHG active
  - No DELQCD/OCCUPAT/BGC/DELQDES/OCCUPDES/BGCDES in this variant
  - PROC PRINT listing produced

SASPy writes SAS7BDAT via NPLOUT libref. RSS guard bounds the large read.

NOTE: Report date is now derived from (today - 1 day) instead of REPTDATE.sas7bdat.
"""

import gc
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, date
from pathlib import Path

import polars as pl
import pyreadstat

try:
    import psutil
    def _rss_mb():
        return psutil.Process(os.getpid()).memory_info().rss / 1024**2
except ImportError:
    def _rss_mb():
        return 0.0

try:
    import saspy
    _SASPY_OK = True
except ImportError:
    _SASPY_OK = False

sys.path.insert(0, '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')
from PBBLNFMT import (
    HP_ALL, HP_ACTIVE, AITAB, MORE_PLAN, MORE_ISLAM,
    HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL,
    FCY_PRODUCTS,
    format_mthpass, format_ndays, format_lnprod, format_lndenom,
    format_odprod, format_oddenom, format_collcd, format_riskcd,
    format_delqdes, format_statecd,
)

try:
    from PBBELF import LIBRARY_PATHS, format_ddmmyy10, format_mmddyy10
except ImportError:
    def format_ddmmyy10(d):
        return d.strftime('%d/%m/%Y') if d else ''
    def format_mmddyy10(d):
        return d.strftime('%m/%d/%Y') if d else ''

# ============================================================================
# CONFIG
# ============================================================================
HPD = set(HP_ALL)
RIND = 'I'
RSS_GUARD_MB = 24000
EXPECTED_LEN = 750

# --- Input paths (each from its own source, no shared ROOT) -----------------
# PATH_REPTDATE removed — report date now derived from (today - 1 day).
PATH_IIS      = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/iis.sas7bdat')
PATH_SP2      = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/sp2.sas7bdat')
PATH_LNNOTE   = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note.sas7bdat')
PATH_CNAME    = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/loan.sas7bdat')
PATH_LIAB     = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/stg_ln_liabfile.sas7bdat')
PATH_LIST     = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT2/list.sas7bdat')

# --- Output paths ----------------------------------------------------------
OUTPUT_DIR    = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3')
WOFFTEXT_PATH = OUTPUT_DIR / 'wofftext.txt'
WOFFTEX1_PATH = OUTPUT_DIR / 'wofftex1.txt'
PRINT_PATH    = OUTPUT_DIR / 'EIIFTXT3_PRINT.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Report date & week logic (derived from today - 1 day)
# ============================================================================
reptdate = date.today() - timedelta(days=1)

day = reptdate.day
if   day == 8:  wk, wk1 = '1', '4'
elif day == 15: wk, wk1 = '2', '1'
elif day == 22: wk, wk1 = '3', '2'
else:           wk, wk1 = '4', '3'

mm, mm1 = reptdate.month, (reptdate.month - 1 if reptdate.month > 1 else 12)
nowk, nowks, nowk1 = wk, '4', wk1
reptmon, reptmon1  = f'{mm:02d}', f'{mm1:02d}'
rdate    = reptdate.strftime('%d/%m/%y')

# Dynamic input path (date-dependent)
PATH_SASLN = Path(
    f'/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/'
    f'loan{reptmon1}{nowks}.sas7bdat'
)

print(f"\n{'='*60}\nESMR 2009-16 (SEND FILE TO LNS) - 3RD EXTRACTION\n{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}  "
      f"Week: {nowk}  Prev Month: {reptmon1}  RIND: {RIND}")
print(f"Output Dir : {OUTPUT_DIR}")
print(f"{'='*60}\n")

# ============================================================================
# Helpers
# ============================================================================
def read_full(path, columns=None):
    path = Path(path)
    if not path.exists():
        print(f"  WARNING: missing {path}")
        return None
    t0 = time.time()
    print(f"  Reading: {path.name}", end="", flush=True)
    try:
        df, _ = pyreadstat.read_sas7bdat(str(path), usecols=columns or None)
        print(f" - {len(df):,} rows, {time.time()-t0:.1f}s")
        return pl.from_pandas(df) if len(df) else None
    except Exception as e:
        print(f"\n  ERROR: {e}")
        return None

def read_lnnote_filtered(path, columns, hpd, max_mb=RSS_GUARD_MB):
    """
    Full read + immediate filter to LOANTYPE in &HPD.
    Linear time, bounded RSS.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    rss_start = _rss_mb()
    if rss_start > max_mb:
        raise MemoryError(f"RSS {rss_start:.0f}MB already over guard {max_mb}MB")

    t0 = time.time()
    print(f"  Reading FULL {path.name} ({len(columns)} cols)...", flush=True)
    df, _ = pyreadstat.read_sas7bdat(str(path), usecols=columns)
    print(f"  Loaded {len(df):,} rows in {time.time()-t0:.1f}s  "
          f"RSS {_rss_mb():.0f}MB", flush=True)

    if _rss_mb() > max_mb:
        del df; gc.collect()
        raise MemoryError(f"RSS over guard {max_mb}MB after load")

    p = pl.from_pandas(df)
    del df
    gc.collect()
    print(f"  Converted to Polars, RSS {_rss_mb():.0f}MB", flush=True)

    p = p.with_columns(pl.col('LOANTYPE').cast(pl.Float64))
    p = p.filter(pl.col('LOANTYPE').is_in(list(hpd)))
    p = p.unique(subset=['ACCTNO', 'NOTENO'])
    print(f"  Filtered to {p.height:,} rows, RSS {_rss_mb():.0f}MB", flush=True)
    return p

def standardize_schema(df, schema):
    if df is None or df.height == 0:
        return df
    exprs = [pl.col(c).cast(dt) for c, dt in schema.items() if c in df.columns]
    return df.with_columns(exprs) if exprs else df

def empty_df(schema):
    return pl.DataFrame(schema=schema)

def align(df, cols):
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    return df.select(cols)

def _z11_substr8(val):
    """PUT(val, Z11.) -> SUBSTR(1,8) -> 'MMDDYYYY'."""
    if val is None or val == 0:
        return None
    try:
        return f"{int(val):011d}"[:8]
    except (TypeError, ValueError):
        return None

def _parse_mmddyyyy(val):
    """INPUT(SUBSTR(PUT(val,Z11.),1,8), MMDDYY8.) -> date."""
    s = _z11_substr8(val)
    if s is None:
        return None
    try:
        return datetime.strptime(s, '%m%d%Y').date()
    except ValueError:
        return None

def _fmt_mmddyy10(val):
    """PUT(date, MMDDYY10.) -> 'MM/DD/YYYY'. Missing -> 10 spaces."""
    if val is None:
        return ' ' * 10
    if isinstance(val, (int, float)):
        val = _parse_mmddyyyy(val)
    if isinstance(val, str):
        return f"{val:<10}"[:10]
    if isinstance(val, date):
        return val.strftime('%m/%d/%Y')
    return ' ' * 10

def calc_mthpdue(days):
    """SAS: MTHPDUE = PUT(DAYS,MTHPASS.); IF MTHPDUE=24 THEN INT((DAYS/365)*12)."""
    if days is None or days <= 0:
        return 0
    s = format_mthpass(int(days))
    v = int(s) if s else 0
    if v == 24:
        v = int((days / 365.0) * 12)
    return v

def _safe_float(s, default=0.0):
    try:
        return float(s.strip()) if s and s.strip() else default
    except (ValueError, TypeError):
        m = re.match(r'^\s*([+-]?\d+(?:\.\d+)?)', s or '')
        return float(m.group(1)) if m else default

# ============================================================================
# SASPy session (lazy init)
# ============================================================================
_SAS_SESSION = None
def get_sas_session():
    global _SAS_SESSION
    if _SAS_SESSION is not None:
        return _SAS_SESSION
    if not _SASPY_OK:
        print("  SASPy not installed — SAS7BDAT writes will be skipped")
        return None
    try:
        _SAS_SESSION = saspy.SASsession(cfgname='default')
        print(f"  SAS session ready: {_SAS_SESSION}")
        _SAS_SESSION.saslib('NPLOUT', path=str(OUTPUT_DIR))
        print(f"  libref NPLOUT -> {OUTPUT_DIR}")
        return _SAS_SESSION
    except Exception as e:
        print(f"  SASPy init failed: {e}")
        return None

def write_sas7bdat_saspy(df, table_name, libref='NPLOUT'):
    sas = get_sas_session()
    if sas is None:
        print(f"  Skipped {libref}.{table_name} (no SAS session)")
        return False
    try:
        pdf = df.to_pandas()
        sas.df2sd(pdf, table=table_name, libref=libref)
        print(f"  {libref}.{table_name} written via SASPy")
        return True
    except Exception as e:
        print(f"  SASPy write failed for {libref}.{table_name}: {e}")
        return False

# Canonical NPL column order (EIIFTXT3: includes BRNO, BRABBR, BORSTAT, DAYS)
NPL_COLS = ['BRNO', 'BRABBR', 'NAME', 'ACCTNO', 'NOTENO',
            'IIS', 'OI', 'TOTIIS', 'BORSTAT', 'SP', 'MARKETVL', 'DAYS', 'BRANCH']

# ============================================================================
# STEP 1: IIS / SP2 -- SAS MERGE precedence (IIS wins on collision)
# ============================================================================
print("STEP 1: IIS / SP2...")
_iis_cols = ['ACCTNO','NOTENO','NAME','IIS','OI','TOTIIS','BORSTAT','SP','MARKETVL','DAYS','BRANCH']

df_iis = read_full(PATH_IIS, columns=_iis_cols)
df_sp  = read_full(PATH_SP2, columns=_iis_cols)

if df_iis is not None:
    df_iis = standardize_schema(df_iis, {'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,
        'MARKETVL':pl.Float64,'DAYS':pl.Float64}).unique(subset=['ACCTNO'])
if df_sp is not None:
    df_sp = standardize_schema(df_sp, {'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,
        'MARKETVL':pl.Float64,'DAYS':pl.Float64}).unique(subset=['ACCTNO'])

print(f"  IIS: {df_iis.height if df_iis is not None else 0:,}  "
      f"SP2: {df_sp.height if df_sp is not None else 0:,}")

if df_iis is not None and df_sp is not None and df_iis.height == df_sp.height:
    print(f"  WARNING: IIS and SP2 have identical row counts ({df_iis.height:,}). "
          f"Verify: {PATH_IIS.name} vs {PATH_SP2.name}")

if df_iis is not None and df_sp is not None and df_iis.height and df_sp.height:
    overlap = [c for c in df_iis.columns
               if c in df_sp.columns and c not in ('ACCTNO','NOTENO')]
    df_npl = (df_sp.join(df_iis, on=['ACCTNO','NOTENO'], how='full', suffix='_iis')
                  .with_columns([pl.coalesce([pl.col(f'{c}_iis'), pl.col(c)]).alias(c)
                                 for c in overlap]))
elif df_iis is not None and df_iis.height:
    df_npl = df_iis.clone()
elif df_sp is not None and df_sp.height:
    df_npl = df_sp.clone()
else:
    df_npl = empty_df({
        'BRNO':pl.Utf8,'BRABBR':pl.Utf8,'NAME':pl.Utf8,'ACCTNO':pl.Float64,
        'NOTENO':pl.Float64,'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,
        'BORSTAT':pl.Utf8,'SP':pl.Float64,'MARKETVL':pl.Float64,'DAYS':pl.Float64,
        'BRANCH':pl.Utf8})

del df_iis, df_sp
gc.collect()

# NPL: ROUND(MARKETVL,.01); BRNO = SUBSTR(BRANCH,4,4); BRABBR = SUBSTR(BRANCH,1,3)
df_npl = df_npl.with_columns([
    pl.col('MARKETVL').fill_null(0).round(2).alias('MARKETVL'),
    pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
    pl.col('BRANCH').str.slice(0, 3).alias('BRABBR'),
])
df_npl = df_npl.select([c for c in NPL_COLS if c in df_npl.columns])
print(f"  NPL: {df_npl.height:,} rows\n")

# ============================================================================
# STEP 2: LNNOTE (LOANTYPE IN &HPD)
# ============================================================================
print("STEP 2: LNNOTE (LOANTYPE IN &HPD)...")
lnnote_columns = [
    'ACCTNO','NOTENO','FEETOTAL','NFEEAMT5','FEEAMT3','FEEAMT4',
    'FEETOT2','FEEAMTA','FEEAMT5','CURBAL','MATUREDT','LOANTYPE',
    'INTAMT','POSTNTRN','ECSRRSRV','INTEARN4','DAYS','CUSTCODE',
    'LASTTRAN','LSTTRNCD','BALANCE','GUAREND','ISSXDTE','NETPROC',
    'SCORE2','CONTRTYPE','APPVALUE','NOTETERM','PAYAMT','BIRTHDT',
    'COLLYEAR','COLLDESC','ORGBAL',
]

df_lnnote = read_lnnote_filtered(PATH_LNNOTE, columns=lnnote_columns, hpd=HPD)
print(f"  LNNOTE: {df_lnnote.height:,} rows\n")

# ============================================================================
# STEP 3: MERGE NPL(IN=AA) LOAN -> LOAN (NPL drives; LOAN cols from LNNOTE)
# ============================================================================
print("STEP 3: MERGE NPL(IN=AA) LOAN...")

df_loan = df_npl.join(df_lnnote, on=['ACCTNO','NOTENO'], how='inner', suffix='_LN')
df_loan = standardize_schema(df_loan, {
    'FEETOTAL':pl.Float64,'NFEEAMT5':pl.Float64,'FEEAMT3':pl.Float64,
    'FEETOT2':pl.Float64,'FEEAMTA':pl.Float64,'FEEAMT5':pl.Float64,
    'ECSRRSRV':pl.Float64,'NETPROC':pl.Float64,'APPVALUE':pl.Float64,
    'ORGBAL':pl.Float64,'CURBAL':pl.Float64,'PAYAMT':pl.Float64,
})
print(f"  LOAN after merge: {df_loan.height:,}\n")

# ============================================================================
# STEP 4: Derived fields (SAS-faithful, incl. ECSRIND quirk)
# ============================================================================
print("STEP 4: Derived fields...")

# POSTAMT = SUM(FEETOTAL, NFEEAMT5)
df_loan = df_loan.with_columns(
    (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0)).alias('POSTAMT'))
# OTHERAMT = SUM(FEEAMT3, -POSTAMT)
df_loan = df_loan.with_columns(
    (pl.col('FEEAMT3').fill_null(0) - pl.col('POSTAMT')).alias('OTHERAMT'))
# OIFEEAMT = SUM(FEETOT2, -FEEAMTA, FEEAMT5)
df_loan = df_loan.with_columns(
    (pl.col('FEETOT2').fill_null(0) - pl.col('FEEAMTA').fill_null(0) + pl.col('FEEAMT5').fill_null(0)).alias('OIFEEAMT'))
# IF ECSRRSRV LE 0 THEN ECSRRSRV = 0
df_loan = df_loan.with_columns(
    pl.when(pl.col('ECSRRSRV').is_null() | (pl.col('ECSRRSRV') <= 0))
      .then(0.0).otherwise(pl.col('ECSRRSRV')).alias('ECSRRSRV'))

# MATDATE / LASTTRA1 / DOBMNI (Z11 -> MMDDYY)
df_loan = df_loan.with_columns([
    pl.col('MATUREDT').map_elements(_parse_mmddyyyy, return_dtype=pl.Date).alias('_MATD'),
    pl.col('LASTTRAN').map_elements(_parse_mmddyyyy, return_dtype=pl.Date).alias('_LASTD'),
    pl.col('BIRTHDT').map_elements(_parse_mmddyyyy, return_dtype=pl.Date).alias('_DOBD'),
])
df_loan = df_loan.with_columns([
    pl.col('_MATD').map_elements(_fmt_mmddyy10, return_dtype=pl.Utf8).alias('MATDATE'),
    pl.col('_LASTD').map_elements(_fmt_mmddyy10, return_dtype=pl.Utf8).alias('LASTTRA1'),
    pl.col('_DOBD').map_elements(_fmt_mmddyy10, return_dtype=pl.Utf8).alias('DOBMNI'),
]).drop(['_MATD','_LASTD','_DOBD'])

# MTHPDUE
if 'DAYS' in df_loan.columns:
    df_loan = df_loan.with_columns(
        pl.col('DAYS').map_elements(calc_mthpdue, return_dtype=pl.Int64).alias('MTHPDUE'))
else:
    df_loan = df_loan.with_columns(pl.lit(0).alias('MTHPDUE'))

# CRRGRADE = TRIM(SCORE2) || TRIM(CONTRTYPE)
def _trunc_str(v):
    if v is None: return ''
    if isinstance(v, float):
        return str(int(v)) if v == int(v) else str(v).rstrip('0').rstrip('.')
    return str(v).strip()

df_loan = df_loan.with_columns(
    (pl.col('SCORE2').map_elements(_trunc_str, return_dtype=pl.Utf8) +
     pl.col('CONTRTYPE').map_elements(_trunc_str, return_dtype=pl.Utf8)).alias('CRRGRADE'))

# MARGINFI = ROUND(NETPROC/APPVALUE, .01)
df_loan = df_loan.with_columns(
    pl.when(pl.col('APPVALUE').is_not_null() & (pl.col('APPVALUE') != 0))
      .then((pl.col('NETPROC') / pl.col('APPVALUE')).round(2))
      .otherwise(0.0).alias('MARGINFI'))

# BILPAID = INT((ORGBAL - CURBAL)/PAYAMT)
df_loan = df_loan.with_columns(
    pl.when(pl.col('PAYAMT').is_not_null() & (pl.col('PAYAMT') > 0))
      .then(((pl.col('ORGBAL').fill_null(0) - pl.col('CURBAL').fill_null(0)) / pl.col('PAYAMT')).floor())
      .otherwise(0).cast(pl.Int64).alias('BILPAID'))

# ECSRIND quirk: always '' (SAS: IF ECSRRSRV > 0 THEN ECSRIND = Y; -- Y unquoted,
# no ELSE -> ECSRIND stays missing, OPTIONS MISSING=' ' -> blank).
df_loan = df_loan.with_columns(pl.lit('').alias('ECSRIND'))

# Drop helpers
drop_helpers = ['FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
                'ORGBAL','NETPROC','APPVALUE','BIRTHDT','MATUREDT','LASTTRAN',
                'SCORE2','CONTRTYPE','PAYAMT']
df_loan = df_loan.drop([c for c in drop_helpers if c in df_loan.columns])

# LOAN KEEP list (EIIFTXT3 - matches SAS)
_loan_keep = [
    'ACCTNO','NOTENO','FEEAMT3','FEEAMT4','CURBAL','MATDATE','LOANTYPE',
    'INTAMT','POSTNTRN','ECSRRSRV','INTEARN4','POSTAMT','OIFEEAMT',
    'OTHERAMT','DAYS','CUSTCODE','LASTTRAN','LSTTRNCD','LASTTRA1',
    'MTHPDUE','BALANCE','GUAREND','ISSXDTE','NETPROC','CRRGRADE',
    'MARGINFI','NOTETERM','PAYAMT','DOBMNI','ECSRIND','COLLYEAR',
    'COLLDESC','BILPAID',
]
df_loan = df_loan.select([c for c in _loan_keep if c in df_loan.columns])
del df_lnnote
gc.collect()
print(f"  LOAN ready: {df_loan.height:,}\n")

# ============================================================================
# STEP 5: CNAME (SECCUST='901')
# ============================================================================
print("STEP 5: Customer names...")
df_cname = read_full(PATH_CNAME, columns=['ACCTNO','CUSTNAM1','SECCUST'])
if df_cname is not None:
    df_cname = (standardize_schema(df_cname, {'ACCTNO':pl.Float64})
                .filter(pl.col('SECCUST') == '901')
                .select(['ACCTNO','CUSTNAM1'])
                .unique(subset=['ACCTNO']))
else:
    df_cname = empty_df({'ACCTNO':pl.Float64,'CUSTNAM1':pl.Utf8})
print(f"  CNAME: {df_cname.height:,}\n")

# ============================================================================
# STEP 6: Guarantors (LIAB + CNAME -> GUARNAM1/GUARNAM2)
# ============================================================================
print("STEP 6: Guarantors...")
guarantor_data = {}
df_liab = read_full(PATH_LIAB, columns=['ACCTNO','NOTENO','LIABACCT','LIABNAME'])
if df_liab is not None and df_cname.height:
    df_liab = standardize_schema(df_liab, {
        'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'LIABACCT':pl.Float64,
    }).sort('LIABACCT')
    df_liab = (df_liab
               .join(df_cname.rename({'ACCTNO':'LIABACCT','CUSTNAM1':'GNAME'}),
                     on='LIABACCT', how='left')
               .with_columns(
                   pl.when(pl.col('GNAME').is_null() | (pl.col('GNAME') == ''))
                     .then(pl.col('LIABNAME')).otherwise(pl.col('GNAME')).alias('GNAME'))
               .sort(['ACCTNO','NOTENO']))
    for (a, n), grp in df_liab.group_by(['ACCTNO','NOTENO']):
        names = grp['GNAME'].to_list()
        guarantor_data[(a, n)] = {
            'GUARNAM1': names[0] if names else '',
            'GUARNAM2': names[1] if len(names) > 1 else '',
        }
del df_liab
gc.collect()
print(f"  Guarantor keys: {len(guarantor_data):,}\n")

if df_loan.height:
    g1, g2 = [], []
    for r in df_loan.iter_rows(named=True):
        gd = guarantor_data.get((r['ACCTNO'], r['NOTENO']),
                                {'GUARNAM1':'','GUARNAM2':''})
        g1.append(gd['GUARNAM1']); g2.append(gd['GUARNAM2'])
    df_loan = df_loan.with_columns([
        pl.Series('GUARNAM1', g1), pl.Series('GUARNAM2', g2)])
del guarantor_data
gc.collect()

# ============================================================================
# STEP 7: SASLN (previous balance)
# ============================================================================
print("STEP 7: Previous balance...")
df_sasln = read_full(PATH_SASLN, columns=['ACCTNO','NOTENO','CURBAL'])
if df_sasln is not None:
    df_sasln = (standardize_schema(df_sasln, {
                    'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'CURBAL':pl.Float64})
                .rename({'CURBAL':'PREVBAL'})
                .sort(['ACCTNO','NOTENO'])
                .join(df_npl.select(['ACCTNO','NOTENO']),
                      on=['ACCTNO','NOTENO'], how='inner'))
    # Attach guarantor names to SASLN
    g1s, g2s = [], []
    for r in df_sasln.iter_rows(named=True):
        gd = guarantor_data.get((r['ACCTNO'], r['NOTENO']),
                                {'GUARNAM1':'','GUARNAM2':''})
        g1s.append(gd['GUARNAM1']); g2s.append(gd['GUARNAM2'])
    df_sasln = df_sasln.with_columns([
        pl.Series('GUARNAM1', g1s), pl.Series('GUARNAM2', g2s)])
else:
    df_sasln = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
                         'PREVBAL':pl.Float64,'GUARNAM1':pl.Utf8,'GUARNAM2':pl.Utf8})
print(f"  SASLN: {df_sasln.height:,}\n")

# ============================================================================
# STEP 8: Final WOFF merge + NPL.LIST filter + CNAME attach
# ============================================================================
print("STEP 8: MERGE SASLN LOAN NPL -> WOFF...")
if df_sasln.height and df_loan.height:
    df_woff = df_sasln.join(df_loan, on=['ACCTNO','NOTENO'], how='inner')

    # Bring in NPL fields not in LOAN (TOTIIS, SP, IIS, OI)
    need_from_npl = [c for c in ['IIS','OI','TOTIIS','SP','BORSTAT','BRANCH','MARKETVL']
                     if c not in df_woff.columns]
    if need_from_npl:
        df_woff = df_woff.join(
            df_npl.select(['ACCTNO','NOTENO'] + need_from_npl),
            on=['ACCTNO','NOTENO'], how='left')

    df_woff = df_woff.with_columns([
        (pl.col('CURBAL').fill_null(0) - pl.col('PREVBAL').fill_null(0)).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit(RIND).alias('RIND'),
    ])
else:
    df_woff = df_npl.clone().with_columns([
        pl.lit(0.0).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit(RIND).alias('RIND'),
    ])
del df_sasln, df_loan
gc.collect()
print(f"  WOFF pre-LIST: {df_woff.height:,}")

# FILTER by NPL.LIST (INNER join on ACCTNO)
df_list = read_full(PATH_LIST, columns=['ACCTNO'])
if df_list is not None and df_list.height:
    df_list = df_list.unique(subset=['ACCTNO']).sort('ACCTNO')
    print(f"  NPL.LIST accounts: {df_list.height:,}")
    df_woff = df_woff.join(df_list, on='ACCTNO', how='inner')
    print(f"  WOFF after NPL.LIST filter: {df_woff.height:,}")
else:
    print("  WARNING: NPL.LIST not found — no filter applied")

# Attach NAME (SAS: MERGE WOFF(IN=A DROP=NAME) CNAME(RENAME=(CUSTNAM1=NAME)))
if 'NAME' in df_woff.columns:
    df_woff = df_woff.drop('NAME')
if df_cname.height:
    df_woff = df_woff.join(
        df_cname.rename({'CUSTNAM1':'NAME'}),
        on='ACCTNO', how='left')
print()

# ============================================================================
# STEP 9: Output — 750-char fixed-width + SASPy SAS7BDAT + PRINT listing
# ============================================================================
print("STEP 9: Writing outputs...")

def _s(v, n):
    s = '' if v is None else str(v)
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s[:n].ljust(n)

def _n(v, n, d=2):
    try: v = 0.0 if v is None else float(v)
    except Exception: v = 0.0
    s = f"{v:.{d}f}"
    return s.rjust(n) if len(s) <= n else s[:n]

def _fd(v):
    return _fmt_mmddyy10(v)

def _line_wofftex1(r):
    """Intermediate (SAS @133 TOTAL and @374 CAP skipped). LRECL=750."""
    buf = [' '] * EXPECTED_LEN

    def put(pos, s, n):
        s = ('' if s is None else str(s)).replace('\n', ' ').replace('\r', ' ')[:n].ljust(n)
        start = pos - 1
        buf[start:start+n] = list(s)

    def put_num(pos, v, n, d=2):
        try: v = 0.0 if v is None else float(v)
        except Exception: v = 0.0
        s = f"{v:.{d}f}".rjust(n)
        if len(s) > n: s = s[:n]
        put(pos, s, n)

    def put_int(pos, v, n):
        try: v = 0 if v is None else int(float(v))
        except Exception: v = 0
        put(pos, str(v).rjust(n), n)

    put(1,   r.get('BRANCH'), 7)
    put(9,   r.get('NAME'), 40)
    put_num(50, r.get('ACCTNO'), 10, 0)
    put_num(61, r.get('NOTENO'), 5, 0)
    put(67,  r.get('BORSTAT'), 1)
    put_num(69,  r.get('IIS'), 16)
    put_num(85,  r.get('OI'), 16)
    put_num(101, r.get('TOTIIS'), 16)
    put_num(117, r.get('SP'), 16)
    # @133 TOTAL skipped
    put_num(149, r.get('CURBAL'), 16)
    put_num(165, r.get('PREVBAL'), 16)
    put_num(181, r.get('PAYMENT'), 16)
    put_num(197, r.get('ECSRRSRV'), 16)
    put_num(213, r.get('POSTAMT'), 16)
    put_num(229, r.get('OTHERAMT'), 16)
    put(245, r.get('MATDATE'), 10)
    put_int(255, r.get('LOANTYPE'), 3)
    put_num(258, r.get('INTAMT'), 16)
    put(274, r.get('POSTNTRN'), 1)
    put_num(278, r.get('MARKETVL'), 16)
    put_num(294, r.get('INTEARN4'), 16)
    put_int(310, r.get('DAYS'), 6)
    put_int(317, r.get('CUSTCODE'), 3)
    put(321, r.get('RIND'), 1)
    put_num(322, r.get('OIFEEAMT'), 16)
    put(339, r.get('LASTTRA1'), 10)
    put_int(350, r.get('LSTTRNCD'), 3)
    put_int(354, r.get('MTHPDUE'), 2)          # EIIFTXT3: width 2
    put_num(357, r.get('BALANCE'), 16)
    # @374 CAP skipped
    put(408, r.get('GUAREND'), 20)
    put(429, r.get('GUARNAM1'), 40)
    put(470, r.get('GUARNAM2'), 40)
    iss = r.get('ISSXDTE')
    iss_s = _fd(iss) if not isinstance(iss, str) else _s(iss, 10)
    put(511, iss_s, 10)
    put_num(522, r.get('NETPROC'), 16)
    put(539, r.get('COLLDESC'), 70)
    put_int(610, r.get('COLLYEAR'), 4)
    put_int(615, r.get('BILPAID'), 3)
    put(619, r.get('CRRGRADE'), 5)
    put_num(625, r.get('MARGINFI'), 16)
    put_int(642, r.get('NOTETERM'), 3)
    put_num(646, r.get('PAYAMT'), 16)
    dob = r.get('DOBMNI')
    dob_s = _fd(dob) if not isinstance(dob, str) else _s(dob, 10)
    put(663, dob_s, 10)
    put(674, r.get('ECSRIND'), 1)

    return ''.join(buf)

def _line_wofftext(r):
    """Final (SAS @133 TOTAL, @374 CAP, @391 LATECHG active). LRECL=750."""
    cap     = 0.0
    latechg = float(r.get('OI') or 0.0)

    buf = [' '] * EXPECTED_LEN

    def put(pos, s, n):
        s = ('' if s is None else str(s)).replace('\n', ' ').replace('\r', ' ')[:n].ljust(n)
        start = pos - 1
        buf[start:start+n] = list(s)

    def put_num(pos, v, n, d=2):
        try: v = 0.0 if v is None else float(v)
        except Exception: v = 0.0
        s = f"{v:.{d}f}".rjust(n)
        if len(s) > n: s = s[:n]
        put(pos, s, n)

    def put_int(pos, v, n):
        try: v = 0 if v is None else int(float(v))
        except Exception: v = 0
        put(pos, str(v).rjust(n), n)

    put(1,   r.get('BRANCH'), 7)
    put(9,   r.get('NAME'), 40)
    put_num(50, r.get('ACCTNO'), 10, 0)
    put_num(61, r.get('NOTENO'), 5, 0)
    put(67,  r.get('BORSTAT'), 1)
    put_num(69,  r.get('IIS'), 16)
    put_num(85,  r.get('OI'), 16)
    put_num(101, r.get('TOTIIS'), 16)
    put_num(117, r.get('SP'), 16)
    put_num(133, r.get('TOTAL'), 16)           # @133 TOTAL  - active
    put_num(149, r.get('CURBAL'), 16)
    put_num(165, r.get('PREVBAL'), 16)
    put_num(181, r.get('PAYMENT'), 16)
    put_num(197, r.get('ECSRRSRV'), 16)
    put_num(213, r.get('POSTAMT'), 16)
    put_num(229, r.get('OTHERAMT'), 16)
    put(245, r.get('MATDATE'), 10)
    put_int(255, r.get('LOANTYPE'), 3)
    put_num(258, r.get('INTAMT'), 16)
    put(274, r.get('POSTNTRN'), 1)
    put_num(278, r.get('MARKETVL'), 16)
    put_num(294, r.get('INTEARN4'), 16)
    put_int(310, r.get('DAYS'), 6)
    put_int(317, r.get('CUSTCODE'), 3)
    put(321, r.get('RIND'), 1)
    put_num(322, r.get('OIFEEAMT'), 16)
    put(339, r.get('LASTTRA1'), 10)
    put_int(350, r.get('LSTTRNCD'), 3)
    put_int(354, r.get('MTHPDUE'), 2)
    put_num(357, r.get('BALANCE'), 16)
    put_num(374, cap, 16)                      # @374 CAP  - active
    put_num(391, latechg, 16)                  # @391 LATECHG - active
    put(408, r.get('GUAREND'), 20)
    put(429, r.get('GUARNAM1'), 40)
    put(470, r.get('GUARNAM2'), 40)
    iss = r.get('ISSXDTE')
    iss_s = _fd(iss) if not isinstance(iss, str) else _s(iss, 10)
    put(511, iss_s, 10)
    put_num(522, r.get('NETPROC'), 16)
    put(539, r.get('COLLDESC'), 70)
    put_int(610, r.get('COLLYEAR'), 4)
    put_int(615, r.get('BILPAID'), 3)
    put(619, r.get('CRRGRADE'), 5)
    put_num(625, r.get('MARGINFI'), 16)
    put_int(642, r.get('NOTETERM'), 3)
    put_num(646, r.get('PAYAMT'), 16)
    dob = r.get('DOBMNI')
    dob_s = _fd(dob) if not isinstance(dob, str) else _s(dob, 10)
    put(663, dob_s, 10)
    put(674, r.get('ECSRIND'), 1)

    return ''.join(buf)

# -------------------------------------------------------------------------
# 9a. WOFFTEX1 (intermediate) -> then TEXT -> then WOFFTEXT
# -------------------------------------------------------------------------
if df_woff.height:
    # WOFFTXT.sas7bdat (NPL1.WOFFTXT) — raw WOFF dataframe
    write_sas7bdat_saspy(df_woff, table_name='WOFFTXT')

    print(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    print(f"Accounts: {len(df_woff):,}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")

    # Build WOFFTEX1 (intermediate)
    with open(WOFFTEX1_PATH, 'w') as f:
        for r in df_woff.iter_rows(named=True):
            f.write(_line_wofftex1(r) + '\n')
    print(f"  {WOFFTEX1_PATH.name}: {WOFFTEX1_PATH.stat().st_size:,} bytes")

    with open(WOFFTEX1_PATH) as f:
        lengths = [len(line.rstrip('\n')) for line in f]
    if lengths:
        print(f"  wofftex1 line lengths: min={min(lengths)} max={max(lengths)} expected={EXPECTED_LEN}")
        if min(lengths) != EXPECTED_LEN:
            print(f"  WARNING: line length mismatch — offsets may misalign")

    # DATA TEXT: read back WOFFTEX1, recompute SP=TOTAL
    text_rows = []
    with open(WOFFTEX1_PATH) as f:
        for line in f:
            L = line.rstrip('\n').ljust(EXPECTED_LEN)
            totiis  = _safe_float(L[100:116])
            balance = _safe_float(L[356:372])
            text_rows.append({
                'BRANCH':   L[0:7],
                'NAME':     L[8:48],
                'ACCTNO':   _safe_float(L[49:59]),
                'NOTENO':   _safe_float(L[60:65]),
                'BORSTAT':  L[66:67],
                'IIS':      _safe_float(L[68:84]),
                'OI':       _safe_float(L[84:100]),
                'TOTIIS':   totiis,
                'SP':       _safe_float(L[116:132]),
                'CURBAL':   _safe_float(L[148:164]),
                'PREVBAL':  _safe_float(L[164:180]),
                'PAYMENT':  _safe_float(L[180:196]),
                'ECSRRSRV': _safe_float(L[196:212]),
                'POSTAMT':  _safe_float(L[212:228]),
                'OTHERAMT': _safe_float(L[228:244]),
                'MATDATE':  L[244:254],
                'LOANTYPE': _safe_float(L[254:257]),
                'INTAMT':   _safe_float(L[257:273]),
                'POSTNTRN': L[273:274],
                'MARKETVL': _safe_float(L[277:293]),
                'INTEARN4': _safe_float(L[293:309]),
                'DAYS':     _safe_float(L[309:315]),
                'CUSTCODE': _safe_float(L[316:319]),
                'RIND':     L[320:321],
                'OIFEEAMT': _safe_float(L[321:337]),
                'LASTTRA1': L[338:348],
                'LSTTRNCD': _safe_float(L[349:352]),
                'MTHPDUE':  _safe_float(L[353:355]),
                'BALANCE':  balance,
                'GUAREND':  L[407:427],
                'GUARNAM1': L[428:468],
                'GUARNAM2': L[469:509],
                'ISSXDTE':  L[510:520],
                'NETPROC':  _safe_float(L[521:537]),
                'COLLDESC': L[538:608],
                'COLLYEAR': _safe_float(L[609:613]),
                'BILPAID':  _safe_float(L[614:617]),
                'CRRGRADE': L[618:623],
                'MARGINFI': _safe_float(L[624:640]),
                'NOTETERM': _safe_float(L[641:644]),
                'PAYAMT':   _safe_float(L[645:661]),
                'DOBMNI':   L[662:672],
                'ECSRIND':  L[673:674],
                'SP':       balance - totiis,
                'TOTAL':    totiis + (balance - totiis),
            })
    print(f"  TEXT parsed: {len(text_rows)} records")

    # Build WOFFTEXT (final)
    with open(WOFFTEXT_PATH, 'w') as f:
        for r in text_rows:
            f.write(_line_wofftext(r) + '\n')
    print(f"  {WOFFTEXT_PATH.name}: {WOFFTEXT_PATH.stat().st_size:,} bytes")

    # WOFFTEXT_FORMATTED (i.e. NPL1.WOFFTXT final dataset) via SASPy
    if text_rows:
        df_text = pl.DataFrame(text_rows)
        write_sas7bdat_saspy(df_text, table_name='WOFFTEXT_FORMATTED')

    # PROC PRINT listing
    _PVAR = ["BRANCH","NAME","ACCTNO","NOTENO","BORSTAT",
             "IIS","OI","TOTIIS","SP","TOTAL","CURBAL",
             "PREVBAL","PAYMENT","MATDATE","LOANTYPE","INTAMT",
             "POSTNTRN","MARKETVL","INTEARN4","DAYS",
             "LASTTRA1","LSTTRNCD","MTHPDUE","BALANCE"]
    _SVAR = {"IIS","OI","TOTIIS","SP","TOTAL","CURBAL","PREVBAL","PAYMENT","BALANCE"}
    _NVAR = {"ACCTNO","NOTENO","IIS","OI","TOTIIS","SP","TOTAL",
             "CURBAL","PREVBAL","PAYMENT","LOANTYPE","INTAMT",
             "MARKETVL","INTEARN4","DAYS","LSTTRNCD","MTHPDUE","BALANCE"}
    _CW = 16

    with open(PRINT_PATH, 'w', encoding='utf-8') as fp:
        fp.write("LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE\n")
        fp.write(f"AS AT {rdate}\n")
        fp.write("-" * (_CW * len(_PVAR)) + "\n")
        fp.write("".join(f"{v:>{_CW}}" if v in _NVAR else f"{v:<{_CW}}"
                         for v in _PVAR) + "\n")
        fp.write("-" * (_CW * len(_PVAR)) + "\n")

        _sums = {v: 0.0 for v in _SVAR}
        for row in text_rows:
            parts = []
            for v in _PVAR:
                val = row.get(v)
                if v in _NVAR:
                    fv = float(val or 0.0)
                    if v in _SVAR:
                        _sums[v] += fv
                    parts.append(f"{fv:>{_CW}.2f}")
                else:
                    parts.append(f"{str(val or ''):<{_CW}}")
            fp.write("".join(parts) + "\n")

        fp.write("-" * (_CW * len(_PVAR)) + "\n")
        fp.write("".join(f"{_sums[v]:>{_CW}.2f}" if v in _SVAR else " " * _CW
                         for v in _PVAR) + "\n")
    print(f"  {PRINT_PATH.name}: {PRINT_PATH.stat().st_size:,} bytes")
else:
    print("\nNo accounts — creating empty files.")
    WOFFTEX1_PATH.write_text('')
    WOFFTEXT_PATH.write_text('')
    PRINT_PATH.write_text('')

total_elapsed = time.time() - start_total if 'start_total' in dir() else 0
print(f"\n{'='*60}\nCOMPLETED\n{'='*60}")
print(f"  {WOFFTEXT_PATH}")
print(f"  {WOFFTEX1_PATH}")
print(f"  {PRINT_PATH}")
if df_woff.height:
    print(f"  NPLOUT.WOFFTXT")
    print(f"  NPLOUT.WOFFTEXT_FORMATTED")
