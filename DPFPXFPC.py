"""
EIIFTXT2 - Bad Debt Write-Off List (Filtered by NPL.LIST)

Key differences from EIIFTXT1:
- Filters accounts by existing NPL.LIST (only accounts previously identified)
- No LOANTYPE exclusion in NPLA WHERE clause
- Same RSS guard, SASPy writes, 797-char fixed-width layout as EIIFTXT1

LNNOTE filter: BORSTAT = 'A' AND ENTITY_CD = 'PIBB'
"""

import gc
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
HPD = HP_ACTIVE
RSS_GUARD_MB = 24000
EXPECTED_LEN = 797
EXCLUDED_LOANTYPES = []           # EIIFTXT2: NO LOANTYPE exclusion
ENTITY_CD_FILTER = 'PIBB'         # LNNOTE: WHERE ENTITY_CD == 'PIBB'

# --- Input paths (each from its own source, no shared ROOT) -----------------
PATH_LN_NOTE = Path('/sas/ln/dwh/enrichment/enrh_ln_note.sas7bdat')
PATH_IIS     = Path('/exdwh/host/ln/npl_hp_sasdata/pibb/woff/iis.sas7bdat')
PATH_SP2     = Path('/exdwh/host/ln/npl_hp_sasdata/pibb/woff/sp2.sas7bdat')
PATH_CNAME   = Path('/dwh/rsd_cis/loan.sas7bdat')
PATH_LIAB    = Path('/sas/ln/dwh/staging/stg_ln_liabfile.sas7bdat')
PATH_LIST    = Path('/exdwh/host/ln/npl_hp_sasdata/pibb/woff/list.sas7bdat')

# --- Output paths ----------------------------------------------------------
OUTPUT_DIR   = Path('/host_pq/mis/output/report/wofftxt')
OUTPUT_FILE  = OUTPUT_DIR / 'wofftext.txt'
OUTPUT_FILE1 = OUTPUT_DIR / 'wofftex1.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Report date & week logic
# ============================================================================
reptdate = datetime.now().date() - timedelta(days=1)

day = reptdate.day
if   day == 8:  wk, wk1 = '1', '4'
elif day == 15: wk, wk1 = '2', '1'
elif day == 22: wk, wk1 = '3', '2'
else:           wk, wk1 = '4', '3'

mm, mm1 = reptdate.month, (reptdate.month - 1 if reptdate.month > 1 else 12)
nowk, nowks, nowk1 = wk, '4', wk1
reptmon, reptmon1  = f'{mm:02d}', f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate    = reptdate.strftime('%d/%m/%y')

# Dynamic input paths (date-dependent, fully qualified — no ROOT)
PATH_CCRIS = Path(
    f'/dwh/iccc_srb/'
    f'icredmsubac{reptmon}{reptyear}.sas7bdat'
)
PATH_SASLN = Path(
    f'/dwh/dwh_m/tempsource/ln/'
    f'loan{reptmon1}{nowks}.sas7bdat'
)

print(f"\n{'='*60}\nBad Debt Write-Off List (Filtered by NPL.LIST)\n{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}  "
      f"Week: {nowk}  Prev Month: {reptmon1}")
print(f"Output Dir : {OUTPUT_DIR}")
print(f"{'='*60}\n")

# ============================================================================
# Format maps
# ============================================================================
OCCUPFMT = {
    '001': 'PROFESSIONAL', '002': 'BUSINESSMAN', '003': 'SELF EMPLOYED',
    '004': 'EMPLOYEE - PRIVATE', '005': 'EMPLOYEE - GOVERNMENT',
    '006': 'RETIRED', '999': 'OTHERS',
}
BGCFMT = {
    'B': 'BUSINESS', 'G': 'GOVERNMENT', 'C': 'CORPORATE',
    'I': 'INDIVIDUAL', '  ': 'NOT SPECIFIED',
}
def get_delq_desc(d):  return format_delqdes(d) if d else 'NO LEGAL ACTION TAKEN'
def get_occup_desc(o): return OCCUPFMT.get(o if o else '999', 'OTHERS')
def get_bgc_desc(b):   return BGCFMT.get(b if b else '  ', 'NOT SPECIFIED')

def get_branch_name(code):
    m = {1:'HQ',2:'KL',3:'PJ',4:'JB',5:'PG',6:'IP',7:'KK',8:'KU',9:'MK',10:'SB'}
    return m.get(code, 'BR')

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

def read_lnnote_full_filtered(path, columns, excluded_loantypes,
                              entity_cd=ENTITY_CD_FILTER,
                              max_mb=RSS_GUARD_MB):
    """
    Single full read + immediate filter. Linear time, bounded RSS.
    EIIFTXT2: filter BORSTAT='A' and ENTITY_CD=<entity_cd> ('PIBB');
    NO LOANTYPE exclusion.
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

    p = p.with_columns([
        pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars(),
        pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars(),
    ]).filter(
        (pl.col('BORSTAT') == 'A') & (pl.col('ENTITY_CD') == entity_cd)
    )
    if excluded_loantypes:
        p = p.filter(~pl.col('LOANTYPE').is_in(excluded_loantypes))
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

def calc_mthpdue(days):
    """SAS: MTHPDUE = PUT(DAYS,MTHPASS.)*1; IF MTHPDUE=24 THEN INT((DAYS/365)*12);"""
    if days is None or days <= 0:
        return 0
    s = format_mthpass(days)
    v = int(s) if s else 0
    if v == 24:
        v = int((days / 365) * 12)
    return v

def decode_z11_mmddyy(v):
    """SAS: INPUT(SUBSTR(PUT(val,Z11.),1,8),MMDDYY8.) -> date."""
    if v is None or v == 0:
        return None
    try:
        head = str(int(v)).zfill(11)[:8]
        return datetime.strptime(head, '%m%d%y').date()
    except Exception:
        return None

def fmt_mmddyy10(d):
    return d.strftime('%m/%d/%Y') if isinstance(d, date) else ''

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

# Canonical NPL column order
NPL_COLS = ['NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH']

# ============================================================================
# STEP 1: Read NPLA (BORSTAT='A', ENTITY_CD='PIBB', no LOANTYPE exclusion)
# ============================================================================
print(f"STEP 1: Reading NPLA (BORSTAT='A', ENTITY_CD='{ENTITY_CD_FILTER}')...")
start_total = time.time()

loan_columns = [
    'ENTITY_CD',
    'BORSTAT', 'LOANTYPE', 'NAME', 'ACCTNO', 'NOTENO',
    'FEEDUE', 'FEEDUEMS', 'FEEAMT16', 'MARKETVL', 'NTBRCH',
    'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2', 'FEEAMTA', 'FEEAMT5',
    'ECSRRSRV', 'MATUREDT', 'LASTTRAN', 'SCORE2', 'CONTRTYPE', 'NETPROC',
    'APPVALUE', 'BIRTHDT', 'ORGBAL', 'CURBAL', 'PAYAMT', 'NACOSPADT',
    'BALANCE', 'GUAREND', 'ISSXDTE', 'COLLDESC', 'COLLYEAR', 'DELQCD',
    'CP', 'MODELDES', 'AKPK_STATUS', 'PAIDIND', 'POSTNTRN', 'INTAMT',
    'INTEARN4', 'CUSTCODE', 'LSTTRNCD',
]

df_npla_raw = read_lnnote_full_filtered(
    PATH_LN_NOTE, columns=loan_columns, excluded_loantypes=EXCLUDED_LOANTYPES,
    entity_cd=ENTITY_CD_FILTER)

if df_npla_raw.height == 0:
    print(f"WARNING: No active accounts (BORSTAT='A', "
          f"ENTITY_CD='{ENTITY_CD_FILTER}'). Exiting.")
    sys.exit(0)
print(f"  NPLA survivors: {df_npla_raw.height:,}")

df_npla_raw = standardize_schema(df_npla_raw, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
    'FEEDUE': pl.Float64, 'FEEDUEMS': pl.Float64, 'FEEAMT16': pl.Float64,
    'MARKETVL': pl.Float64, 'NTBRCH': pl.Float64, 'LOANTYPE': pl.Float64,
})

branch_vals = []
for n in df_npla_raw['NTBRCH'].to_list():
    if n is None:
        branch_vals.append('BR 000')
    else:
        try:
            c = int(n) if isinstance(n, float) else n
            branch_vals.append(f"{get_branch_name(c)} {int(c):03d}")
        except Exception:
            branch_vals.append('BR 000')

df_npla = (df_npla_raw
           .with_columns([
               pl.Series('BRANCH', branch_vals),
               pl.lit(0.0).alias('IIS'),
               (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('OI'),
               (pl.lit(0.0) + (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0))).alias('TOTIIS'),
               (pl.col('FEEDUEMS').fill_null(0) + pl.col('FEEAMT16').fill_null(0)).alias('SP'),
           ])
           .select(NPL_COLS))
print(f"  NPLA ready: {df_npla.height:,} rows\n")

# ============================================================================
# STEP 2: IIS / SP2 — SAS MERGE precedence (IIS wins on collision)
# ============================================================================
print("STEP 2: IIS / SP2...")
_iis_cols = ['ACCTNO','NOTENO','NAME','IIS','OI','TOTIIS','SP','MARKETVL','BRANCH']
df_iis = read_full(PATH_IIS, columns=_iis_cols)
df_sp  = read_full(PATH_SP2, columns=_iis_cols)

if df_iis is not None:
    df_iis = standardize_schema(df_iis, {'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,
        'MARKETVL':pl.Float64}).unique(subset=['ACCTNO','NOTENO'])
if df_sp is not None:
    df_sp = standardize_schema(df_sp, {'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,
        'MARKETVL':pl.Float64}).unique(subset=['ACCTNO','NOTENO'])

print(f"  IIS: {df_iis.height if df_iis is not None else 0:,}  "
      f"SP2: {df_sp.height if df_sp is not None else 0:,}")

if df_iis is not None and df_sp is not None and df_iis.height == df_sp.height:
    print(f"  WARNING: IIS and SP2 have identical row counts ({df_iis.height:,}). "
          f"Verify: {PATH_IIS.name} vs {PATH_SP2.name}")

if df_iis is not None and df_sp is not None and df_iis.height and df_sp.height:
    overlap = [c for c in df_iis.columns
               if c in df_sp.columns and c not in ('ACCTNO','NOTENO')]
    df_npl_data = (df_sp.join(df_iis, on=['ACCTNO','NOTENO'], how='full', suffix='_iis')
                        .with_columns([pl.coalesce([pl.col(f'{c}_iis'), pl.col(c)]).alias(c)
                                       for c in overlap])
                        .select(NPL_COLS))
elif df_iis is not None and df_iis.height:
    df_npl_data = df_iis.select(NPL_COLS)
elif df_sp is not None and df_sp.height:
    df_npl_data = df_sp.select(NPL_COLS)
else:
    df_npl_data = empty_df({
        'NAME':pl.Utf8,'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,
        'SP':pl.Float64,'MARKETVL':pl.Float64,'BRANCH':pl.Utf8})

del df_iis, df_sp
gc.collect()

# ============================================================================
# STEP 3: Combine NPLA + NPL — align schemas first
# ============================================================================
print("\nSTEP 3: Combine NPL...")

df_npla     = align(df_npla,     NPL_COLS)
df_npl_data = align(df_npl_data, NPL_COLS)

df_npla = standardize_schema(df_npla, {
    'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'IIS':pl.Float64,
    'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,'MARKETVL':pl.Float64,
})
df_npl_data = standardize_schema(df_npl_data, {
    'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'IIS':pl.Float64,
    'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,'MARKETVL':pl.Float64,
})

if df_npl_data.height > 0:
    df_npl = pl.concat([df_npla, df_npl_data], how='vertical')
else:
    df_npl = df_npla.clone()

df_npl = (df_npl
          .with_columns([
              pl.col('MARKETVL').fill_null(0).round(2),
              pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
              pl.col('BRANCH').str.slice(0, 3).alias('BRABBR'),
          ])
          .unique(subset=['ACCTNO','NOTENO'], keep='first'))
del df_npl_data
gc.collect()
print(f"  NPL combined: {df_npl.height:,}\n")

# ============================================================================
# STEP 4: CCRIS
# ============================================================================
print("STEP 4: CCRIS...")
ccris_paths = [
    PATH_CCRIS,
    PATH_CCRIS.with_name(f'ICREDMSUBAC{reptmon}{reptyear}.sas7bdat'),
    PATH_CCRIS.with_name(f'icredmsubac{reptmon}0{reptyear}.sas7bdat'),
    PATH_CCRIS.with_name(f'ICREDMSUBAC{reptmon}0{reptyear}.sas7bdat'),
]
df_credsub = None
for p in ccris_paths:
    df_credsub = read_full(p)
    if df_credsub is not None:
        print(f"  Found: {p.name}")
        break

if df_credsub is not None and df_credsub.height:
    acct_col = next((c for c in df_credsub.columns
                     if c.lower() in ('acctnum','acctno','acct_num','account','accountno')), None)
    days_col = next((c for c in df_credsub.columns
                     if c.lower() in ('daysarr','days','days_arr','daysarrears')), None)
    fac_col  = next((c for c in df_credsub.columns
                     if c.lower() in ('facility','facility_cd','facilitycd','fac')), None)
    print(f"  cols -> ACCT:{acct_col} DAYS:{days_col} FAC:{fac_col}")

    if acct_col is None:
        df_credsub = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
                               'DAYS':pl.Float64,'FACILITY':pl.Utf8})
    else:
        ren = {}
        if acct_col != 'ACCTNO': ren[acct_col] = 'ACCTNO'
        if days_col and days_col != 'DAYS': ren[days_col] = 'DAYS'
        if fac_col and fac_col != 'FACILITY': ren[fac_col] = 'FACILITY'
        if ren: df_credsub = df_credsub.rename(ren)

        if 'FACILITY' in df_credsub.columns:
            df_credsub = df_credsub.filter(pl.col('FACILITY').is_in(['34331','34332']))

        keep = [c for c in ['ACCTNO','NOTENO','DAYS','FACILITY'] if c in df_credsub.columns]
        df_credsub = df_credsub.select(keep)
        if {'ACCTNO','NOTENO'} <= set(df_credsub.columns):
            df_credsub = (df_credsub
                          .sort(['ACCTNO','NOTENO','DAYS'], descending=[False,False,True])
                          .unique(subset=['ACCTNO','NOTENO'], keep='first'))
        df_credsub = standardize_schema(df_credsub, {
            'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'DAYS':pl.Float64})
else:
    df_credsub = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
                           'DAYS':pl.Float64,'FACILITY':pl.Utf8})
print(f"  CCRIS: {df_credsub.height:,}\n")

# ============================================================================
# STEP 5: LOAN (HPD types) — LOAN wins on collision
# ============================================================================
print("STEP 5: Filtering HPD loan types...")
if 'LOANTYPE' in df_npla_raw.columns:
    df_loan_src = (df_npla_raw
                   .select(['ACCTNO','NOTENO','LOANTYPE'])
                   .unique(subset=['ACCTNO','NOTENO'])
                   .filter(pl.col('LOANTYPE').is_in(HPD)))
else:
    df_loan_src = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'LOANTYPE':pl.Float64})
print(f"  HPD rows: {df_loan_src.height:,}\n")

# ============================================================================
# STEP 6: MERGE NPL(IN=AA) CREDSUB LOAN; IF AA -> keep NPL, LOAN wins
# ============================================================================
print("STEP 6: MERGE NPL / CREDSUB / LOAN...")

df_loan = df_npl.clone()

# CREDSUB
if df_credsub.height > 0:
    df_loan = df_loan.join(df_credsub, on=['ACCTNO','NOTENO'], how='left')
else:
    df_loan = df_loan.with_columns([
        pl.lit(None).cast(pl.Float64).alias('DAYS'),
        pl.lit(None).cast(pl.Utf8).alias('FACILITY'),
    ])
del df_credsub
gc.collect()

# LOAN — LOAN wins on overlap
if df_loan_src.height > 0:
    ov = [c for c in df_loan_src.columns
          if c in df_loan.columns and c not in ('ACCTNO','NOTENO')]
    df_loan = (df_loan.join(df_loan_src, on=['ACCTNO','NOTENO'], how='left', suffix='_loan')
                      .with_columns([pl.coalesce([pl.col(f'{c}_loan'), pl.col(c)]).alias(c)
                                     for c in ov]))
else:
    df_loan = df_loan.with_columns(pl.lit(None).cast(pl.Float64).alias('LOANTYPE'))
del df_loan_src
gc.collect()

# ============================================================================
# STEP 7: Extras (derived fields)
# ============================================================================
print("STEP 7: Derived fields...")
extra_cols = ['ACCTNO','NOTENO','FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2',
              'FEEAMTA','FEEAMT5','ECSRRSRV','MATUREDT','LASTTRAN','SCORE2',
              'CONTRTYPE','NETPROC','APPVALUE','BIRTHDT','ORGBAL','CURBAL',
              'PAYAMT','NACOSPADT','BALANCE','GUAREND','ISSXDTE','COLLDESC',
              'COLLYEAR','DELQCD','CP','MODELDES','AKPK_STATUS','BORSTAT',
              'PAIDIND','POSTNTRN','INTAMT','INTEARN4','CUSTCODE','LSTTRNCD']

avail = [c for c in extra_cols if c in df_npla_raw.columns]
df_extra = df_npla_raw.select(avail).unique(subset=['ACCTNO','NOTENO'])
for c in ('FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
          'ECSRRSRV','NETPROC','APPVALUE','ORGBAL','CURBAL','PAYAMT',
          'BALANCE','INTAMT','INTEARN4','CUSTCODE','LSTTRNCD'):
    if c in df_extra.columns:
        df_extra = df_extra.with_columns(pl.col(c).cast(pl.Float64))

ov = [c for c in df_extra.columns
      if c in df_loan.columns and c not in ('ACCTNO','NOTENO')]
df_loan = (df_loan.join(df_extra, on=['ACCTNO','NOTENO'], how='left', suffix='_x')
                  .with_columns([pl.coalesce([pl.col(f'{c}_x'), pl.col(c)]).alias(c)
                                 for c in ov]))
del df_extra, df_npla_raw
gc.collect()

for c in ('FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
          'ECSRRSRV','ORGBAL','CURBAL','PAYAMT','NACOSPADT','SCORE2',
          'CONTRTYPE','NETPROC','APPVALUE'):
    if c not in df_loan.columns:
        df_loan = df_loan.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

# Derived numeric
df_loan = df_loan.with_columns(
    (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0)).alias('POSTAMT'))
df_loan = df_loan.with_columns(
    (pl.col('FEEAMT3').fill_null(0) - pl.col('POSTAMT')).alias('OTHERAMT'))
df_loan = df_loan.with_columns(
    (pl.col('FEETOT2').fill_null(0) - pl.col('FEEAMTA').fill_null(0) + pl.col('FEEAMT5').fill_null(0)).alias('OIFEEAMT'))

# ECSRRSRV / ECSRIND
df_loan = df_loan.with_columns(
    pl.when(pl.col('ECSRRSRV').is_null() | (pl.col('ECSRRSRV') <= 0))
      .then(0.0).otherwise(pl.col('ECSRRSRV')).alias('ECSRRSRV'))
df_loan = df_loan.with_columns(
    pl.when(pl.col('ECSRRSRV') > 0).then(pl.lit('Y')).otherwise(pl.lit('N')).alias('ECSRIND'))

# MATUREDT / LASTTRAN / BIRTHDT -> date strings
df_loan = df_loan.with_columns([
    pl.col('MATUREDT').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('_MATD'),
    pl.col('LASTTRAN').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('_LASTD'),
    pl.col('BIRTHDT').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('_DOBD'),
])
df_loan = df_loan.with_columns([
    pl.col('_MATD').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('MATDATE'),
    pl.col('_LASTD').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('LASTTRA1'),
    pl.col('_DOBD').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('DOBMNI'),
]).drop(['_MATD','_LASTD','_DOBD'])

# MTHPDUE
if 'DAYS' in df_loan.columns:
    df_loan = df_loan.with_columns(
        pl.col('DAYS').map_elements(calc_mthpdue, return_dtype=pl.Int64).alias('MTHPDUE'))
else:
    df_loan = df_loan.with_columns(pl.lit(0).alias('MTHPDUE'))

# CRRGRADE
def _trunc_str(v):
    if v is None: return ''
    if isinstance(v, float):
        return str(int(v)) if v == int(v) else str(v).rstrip('0').rstrip('.')
    return str(v).strip()

df_loan = df_loan.with_columns(
    (pl.col('SCORE2').map_elements(_trunc_str, return_dtype=pl.Utf8) +
     pl.col('CONTRTYPE').map_elements(_trunc_str, return_dtype=pl.Utf8)).alias('CRRGRADE'))

# MARGINFI
df_loan = df_loan.with_columns(
    pl.when(pl.col('APPVALUE').is_not_null() & (pl.col('APPVALUE') != 0))
      .then((pl.col('NETPROC') / pl.col('APPVALUE')).round(2))
      .otherwise(0.0).alias('MARGINFI'))

# BILPAID (SAS INT -> floor)
df_loan = df_loan.with_columns(
    pl.when(pl.col('PAYAMT').is_not_null() & (pl.col('PAYAMT') > 0))
      .then(((pl.col('ORGBAL').fill_null(0) - pl.col('CURBAL').fill_null(0)) / pl.col('PAYAMT')).floor())
      .otherwise(0).cast(pl.Int64).alias('BILPAID'))

# PAY75PCT / NACODATE
df_loan = df_loan.with_columns(
    pl.when(pl.col('NACOSPADT').is_not_null() & (pl.col('NACOSPADT') > 0))
      .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('PAY75PCT'))
df_loan = df_loan.with_columns(
    pl.when(pl.col('NACOSPADT').is_not_null() & (pl.col('NACOSPADT') > 0))
      .then(pl.col('NACOSPADT').map_elements(
          lambda v: fmt_mmddyy10(decode_z11_mmddyy(v)), return_dtype=pl.Utf8))
      .otherwise(pl.lit('')).alias('NACODATE'))

# Drop helpers
drop_helpers = ['FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
                'ECSRRSRV','MATUREDT','LASTTRAN','SCORE2','CONTRTYPE','NETPROC',
                'APPVALUE','BIRTHDT','ORGBAL','PAYAMT','NACOSPADT']
df_loan = df_loan.drop([c for c in drop_helpers if c in df_loan.columns])
gc.collect()
print(f"  LOAN ready: {df_loan.height:,}\n")

# ============================================================================
# STEP 8: CNAME
# ============================================================================
print("STEP 8: Customer names...")
df_cname = read_full(PATH_CNAME,
                     columns=['ACCTNO','CUSTNAM1','OCCUPAT','BGC','SECCUST'])
if df_cname is not None:
    df_cname = (standardize_schema(df_cname, {'ACCTNO':pl.Float64})
                .filter(pl.col('SECCUST') == '901')
                .select(['ACCTNO','CUSTNAM1','OCCUPAT','BGC'])
                .unique(subset=['ACCTNO']))
else:
    df_cname = empty_df({'ACCTNO':pl.Float64,'CUSTNAM1':pl.Utf8,
                         'OCCUPAT':pl.Utf8,'BGC':pl.Utf8})
print(f"  CNAME: {df_cname.height:,}\n")

# ============================================================================
# STEP 9: Guarantors
# ============================================================================
print("STEP 9: Guarantors...")
guarantor_data = {}
df_liab = read_full(PATH_LIAB,
                    columns=['ACCTNO','NOTENO','LIABACCT','LIABNAME'])
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
# STEP 10: Previous balance
# ============================================================================
print("STEP 10: Previous balance...")
df_sasln = read_full(PATH_SASLN, columns=['ACCTNO','NOTENO','CURBAL'])
if df_sasln is not None:
    df_sasln = (standardize_schema(df_sasln, {
                    'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'CURBAL':pl.Float64})
                .rename({'CURBAL':'PREVBAL'})
                .sort(['ACCTNO','NOTENO'])
                .join(df_npl.select(['ACCTNO','NOTENO']),
                      on=['ACCTNO','NOTENO'], how='inner'))
else:
    df_sasln = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'PREVBAL':pl.Float64})
print(f"  SASLN: {df_sasln.height:,}\n")

# ============================================================================
# STEP 11: Final merge + SAS filter (WOFF construction)
# ============================================================================
print("STEP 11: Final merge...")
if df_sasln.height and df_loan.height:
    df_woff = df_sasln.join(df_loan, on=['ACCTNO','NOTENO'], how='inner')
    if 'BRANCH' not in df_woff.columns:
        df_woff = df_woff.join(df_npl.select(['ACCTNO','BRANCH','MARKETVL']),
                               on='ACCTNO', how='left')
    df_woff = df_woff.with_columns([
        (pl.col('CURBAL').fill_null(0) - pl.col('PREVBAL').fill_null(0)).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit('I').alias('RIND'),
    ])
else:
    df_woff = df_npl.clone().with_columns([
        pl.lit(0.0).alias('PAYMENT'),
        (pl.col('TOTIIS').fill_null(0) + pl.col('SP').fill_null(0)).alias('TOTAL'),
        pl.lit('I').alias('RIND'),
    ])
del df_sasln, df_loan
gc.collect()
print(f"  WOFF pre-filter: {df_woff.height:,}")

# Ensure required columns exist
for c in ('BORSTAT','DAYS','LOANTYPE','PAIDIND','TOTAL'):
    if c not in df_woff.columns:
        df_woff = df_woff.with_columns(pl.lit(None).alias(c))
df_woff = (df_woff
           .with_columns([pl.col(c).cast(pl.Utf8) for c in ('BORSTAT','PAIDIND')] +
                         [pl.col(c).cast(pl.Float64) for c in ('DAYS','LOANTYPE','TOTAL')]))

# SAS: (BORSTAT IN ('F','I') & DAYS GE 334) OR DAYS GE 334 OR
#      (BORSTAT='A' & LOANTYPE NOT IN excluded & PAIDIND NE 'P'); TOTAL NE 0
cond_borstat_fi = (pl.col('BORSTAT').is_in(['F','I'])) & (pl.col('DAYS') >= 334)
cond_days       = pl.col('DAYS') >= 334
cond_active     = ((pl.col('BORSTAT') == 'A') &
                   (~pl.col('LOANTYPE').is_in(EXCLUDED_LOANTYPES)) &
                   (pl.col('PAIDIND').is_null() | (pl.col('PAIDIND') != 'P')))

df_woff = (df_woff
           .filter((cond_borstat_fi | cond_days | cond_active) & (pl.col('TOTAL') != 0))
           .with_columns(pl.lit('Y').alias('CONFIRM'))
           .sort('ACCTNO'))

# ============================================================================
# STEP 12: KEY DIFFERENCE — filter by NPL.LIST
# ============================================================================
print("\nSTEP 12: Filtering by NPL.LIST...")
list_candidates = [
    PATH_LIST,
    PATH_LIST.with_name('list.sas7bdat'),
    OUTPUT_DIR / 'LIST.sas7bdat',
    OUTPUT_DIR / 'list.sas7bdat',
]
df_list = None
for p in list_candidates:
    df_list = read_full(p, columns=['ACCTNO'])
    if df_list is not None:
        print(f"  Found LIST: {p.name}")
        break

if df_list is not None and df_list.height:
    df_list = df_list.unique(subset=['ACCTNO']).sort('ACCTNO')
    print(f"  NPL.LIST accounts: {df_list.height:,}")
    df_woff = df_woff.join(df_list, on='ACCTNO', how='inner')
    print(f"  WOFF after NPL.LIST filter: {df_woff.height:,}")
else:
    print("  WARNING: NPL.LIST not found — no filter applied")

# CNAME attach (SAS: MERGE WOFF(IN=A DROP=NAME) CNAME)
if 'NAME' not in df_woff.columns and df_cname.height > 0:
    df_woff = df_woff.join(
        df_cname.rename({'CUSTNAM1':'NAME'}),
        on='ACCTNO',
        how='left'
    )
print()

# ============================================================================
# STEP 13: Output — 797-char fixed-width + SASPy SAS7BDAT
# ============================================================================
print("STEP 13: Writing outputs...")

def _s(v, n):
    s = '' if v is None else str(v)
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s[:n].ljust(n)

def _n(v, n, d=2):
    try: v = 0.0 if v is None else float(v)
    except Exception: v = 0.0
    s = f"{v:.{d}f}"
    return s.rjust(n) if len(s) <= n else s[:n]

def _line_wofftex1(r):
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
    put_int(354, r.get('MTHPDUE'), 3)
    put_num(357, r.get('BALANCE'), 16)
    put(408, r.get('GUAREND'), 20)
    put(429, r.get('GUARNAM1'), 40)
    put(470, r.get('GUARNAM2'), 40)
    iss = r.get('ISSXDTE')
    iss_s = fmt_mmddyy10(iss) if isinstance(iss, date) else (iss or '')
    put(511, iss_s, 10)
    put_num(522, r.get('NETPROC'), 16)
    put(539, r.get('COLLDESC'), 70)
    put_int(610, r.get('COLLYEAR'), 4)
    put_int(615, r.get('BILPAID'), 3)
    put(619, r.get('CRRGRADE'), 5)
    put_num(625, r.get('MARGINFI'), 16)
    put_int(642, r.get('NOTETERM'), 3)
    put_num(646, r.get('PAYAMT'), 16)
    put(663, r.get('DOBMNI'), 10)
    put(674, r.get('ECSRIND'), 1)
    put(677, r.get('DELQCD'), 2)
    put(713, r.get('OCCUPAT'), 3)
    put(743, r.get('BGC'), 2)
    put(767, r.get('PAY75PCT'), 1)
    put(769, r.get('NACODATE'), 10)
    put(780, r.get('CP'), 1)
    put(782, r.get('MODELDES'), 6)
    put(789, r.get('AKPK_STATUS'), 9)

    return ''.join(buf)

def write_wofftex1(df, path):
    with open(path, 'w') as f:
        for r in df.iter_rows(named=True):
            f.write(_line_wofftex1(r) + '\n')

def write_wofftext_streaming(src, dst):
    with open(src) as fin, open(dst, 'w') as fout:
        for line in fin:
            L = line.rstrip('\n').ljust(EXPECTED_LEN)
            totiis  = _safe_float(L[100:116])
            balance = _safe_float(L[356:372])
            oi      = _safe_float(L[84:100])
            sp   = balance - totiis
            tot  = totiis + sp
            delqcd, occupat, bgc = L[676:678], L[712:715], L[742:744]
            out = (L[:116] + _n(sp,16) + _n(tot,16) + L[148:373] +
                   _n(0.0,16) + _n(oi,16) + L[407:679] +
                   _s(get_delq_desc(delqcd),30) + _s('I',1) +
                   _s(occupat,3) + _s(get_occup_desc(occupat),25) +
                   _s(bgc,2) + _s(get_bgc_desc(bgc),20) + L[766:])
            fout.write(out + '\n')

if df_woff.height:
    # WOFFTXT (un-formatted) via SASPy
    write_sas7bdat_saspy(df_woff, table_name='WOFFTXT')

    print(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    print(f"Accounts in filtered write-off list: {len(df_woff):,}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")

    write_wofftex1(df_woff, OUTPUT_FILE1)
    print(f"  {OUTPUT_FILE1.name}: {OUTPUT_FILE1.stat().st_size:,} bytes")

    with open(OUTPUT_FILE1) as f:
        lengths = [len(line.rstrip('\n')) for line in f]
    if lengths:
        print(f"  wofftex1 line lengths: min={min(lengths)} max={max(lengths)} expected={EXPECTED_LEN}")
        if min(lengths) != EXPECTED_LEN:
            print(f"  WARNING: line length mismatch — offsets may misalign")

    write_wofftext_streaming(OUTPUT_FILE1, OUTPUT_FILE)
    print(f"  {OUTPUT_FILE.name}: {OUTPUT_FILE.stat().st_size:,} bytes")

    # WOFFTXT_FORMATTED via SASPy
    recs = []
    with open(OUTPUT_FILE1) as f:
        for line in f:
            L = line.rstrip('\n')
            if len(L) < 372:
                continue
            tot = _safe_float(L[100:116])
            bal = _safe_float(L[356:372])
            recs.append({
                'BRANCH':  L[0:7].strip(),
                'NAME':    L[8:48].strip(),
                'ACCTNO':  _safe_float(L[49:59]),
                'NOTENO':  L[60:65].strip(),
                'BORSTAT': L[66:67],
                'IIS':     _safe_float(L[68:84]),
                'OI':      _safe_float(L[84:100]),
                'TOTIIS':  tot,
                'BALANCE': bal,
                'SP':      bal - tot,
                'TOTAL':   tot + (bal - tot),
            })
    if recs:
        df_t = pl.DataFrame(recs)
        write_sas7bdat_saspy(df_t, table_name='WOFFTXT_FORMATTED')
else:
    print("\nNo accounts after NPL.LIST filter — creating empty files.")
    OUTPUT_FILE1.write_text('')
    OUTPUT_FILE.write_text('')

total_elapsed = time.time() - start_total
print(f"\n{'='*60}\nCOMPLETED IN {total_elapsed:.1f}s\n{'='*60}")
print(f"  {OUTPUT_FILE}\n  {OUTPUT_FILE1}")
if df_woff.height:
    print(f"  NPLOUT.WOFFTXT\n  NPLOUT.WOFFTXT_FORMATTED")
