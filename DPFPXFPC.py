"""
EIIFTXT1 - Bad Debt Write-Off List Generation
SAS-faithful port. ENTITY_CD='PIBB' scoped on enrh_ln_note.
Chunked read, streaming writers, bounded memory.
"""

import gc
import os
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
EXCLUDED_LOANTYPES = [983, 993, 678, 679, 698, 699]
CHUNK_ROWS = 250_000
RSS_GUARD_MB = 6000

ROOT = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')

PATH_LN_NOTE = ROOT / 'input/prod/a_lnnote/enrh_ln_note.sas7bdat'
PATH_IIS     = ROOT / 'input/prod/EIIFTXT1/iis.sas7bdat'
PATH_SP2     = ROOT / 'input/prod/EIIFTXT1/sp2.sas7bdat'
PATH_CNAME   = ROOT / 'input/prod/EIIFTXT1/loan.sas7bdat'
PATH_LIAB    = ROOT / 'input/prod/EIIFTXT1/lnliab07226.sas7bdat'

OUTPUT_DIR   = ROOT / 'output/EIIFTXT1'
OUTPUT_FILE  = OUTPUT_DIR / 'wofftext.txt'
OUTPUT_FILE1 = OUTPUT_DIR / 'wofftex1.txt'
OUT_LIST     = OUTPUT_DIR / 'LIST.sas7bdat'
OUT_WOFFTXT  = OUTPUT_DIR / 'WOFFTXT.sas7bdat'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Report date & week logic (system clock - 1 day; per instruction)
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

PATH_CCRIS = ROOT / f'input/prod/EIIFTXT1/icredmsubac{reptmon}{reptyear}.sas7bdat'
PATH_SASLN = ROOT / f'input/prod/EIIFTXT1/loan{reptmon1}{nowks}.sas7bdat'

print(f"\n{'='*60}\nBad Debt Write-Off List Generation\n{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}  "
      f"Week: {nowk}  Prev Month: {reptmon1}")
print(f"{'='*60}\n")

# ============================================================================
# Format maps (replace with BKCTRL.CISFMT loader if available)
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

def read_lnnote_chunked(path, columns, excluded_loantypes,
                        chunk=CHUNK_ROWS, max_mb=RSS_GUARD_MB):
    """
    Stream enrh_ln_note; keep only ENTITY_CD='PIBB' & BORSTAT='A'
    & LOANTYPE not in excluded. Bounded memory.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    parts, offset, kept = [], 0, 0
    t0 = time.time()
    while True:
        rss = _rss_mb()
        if rss > max_mb:
            raise MemoryError(f"RSS {rss:.0f}MB > guard {max_mb}MB")
        c, _ = pyreadstat.read_sas7bdat(
            str(path), usecols=columns, row_offset=offset, row_limit=chunk)
        if len(c) == 0:
            break
        p = pl.from_pandas(c).with_columns([
            pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars(),
            pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars(),
        ]).filter(
            (pl.col('ENTITY_CD') == 'PIBB') &
            (pl.col('BORSTAT') == 'A') &
            (~pl.col('LOANTYPE').is_in(excluded_loantypes))
        )
        if p.height:
            parts.append(p)
            kept += p.height
        del c
        offset += chunk
        gc.collect()
        print(f"  offset {offset:>10,}  kept {kept:>6,}  "
              f"RSS {_rss_mb():.0f}MB  {time.time()-t0:.0f}s", end='\r', flush=True)
    print()
    return pl.concat(parts, how='vertical_relaxed') if parts else pl.DataFrame()

def standardize_schema(df, schema):
    if df is None or df.height == 0:
        return df
    exprs = []
    for col, dt in schema.items():
        if col in df.columns:
            exprs.append(pl.col(col).cast(dt))
    return df.with_columns(exprs) if exprs else df

def empty_df(schema):
    return pl.DataFrame(schema=schema)

def calc_mthpdue(days):
    if days is None or days <= 0:
        return 0
    s = format_mthpass(days)
    v = int(s) if s else 0
    if v == 24:
        v = int((days / 365) * 12)
    return v

def decode_z11_mmddyy(v):
    if v is None or v == 0:
        return None
    try:
        head = str(int(v)).zfill(11)[:8]
        return datetime.strptime(head, '%m%d%y').date()
    except Exception:
        return None

def fmt_mmddyy10(d):
    return d.strftime('%m/%d/%Y') if isinstance(d, date) else ''

# ============================================================================
# STEP 1+2: Read NPLA (PIBB active) in chunks — single pass
# ============================================================================
print("STEP 1+2: Streaming enrh_ln_note (ENTITY_CD='PIBB', BORSTAT='A')...")
start_total = time.time()

loan_columns = [
    'ENTITY_CD', 'BORSTAT', 'LOANTYPE', 'NAME', 'ACCTNO', 'NOTENO',
    'FEEDUE', 'FEEDUEMS', 'FEEAMT16', 'MARKETVL', 'NTBRCH',
    'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2', 'FEEAMTA', 'FEEAMT5',
    'ECSRRSRV', 'MATUREDT', 'LASTTRAN', 'SCORE2', 'CONTRTYPE', 'NETPROC',
    'APPVALUE', 'BIRTHDT', 'ORGBAL', 'CURBAL', 'PAYAMT', 'NACOSPADT',
    'BALANCE', 'GUAREND', 'ISSXDTE', 'COLLDESC', 'COLLYEAR', 'DELQCD',
    'CP', 'MODELDES', 'AKPK_STATUS', 'PAIDIND', 'POSTNTRN', 'INTAMT',
    'INTEARN4', 'CUSTCODE', 'LSTTRNCD',
]

df_npla_raw = read_lnnote_chunked(
    PATH_LN_NOTE, columns=loan_columns, excluded_loantypes=EXCLUDED_LOANTYPES)

if df_npla_raw.height == 0:
    print("WARNING: No PIBB active accounts found. Exiting.")
    sys.exit(0)
print(f"  NPLA survivors: {df_npla_raw.height:,}")

df_npla_raw = standardize_schema(df_npla_raw, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
    'FEEDUE': pl.Float64, 'FEEDUEMS': pl.Float64, 'FEEAMT16': pl.Float64,
    'MARKETVL': pl.Float64, 'NTBRCH': pl.Float64, 'LOANTYPE': pl.Float64,
})

# IIS / OI / TOTIIS / SP + BRANCH
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
           .select(['NAME','ACCTNO','NOTENO','IIS','OI','TOTIIS','SP','MARKETVL','BRANCH']))
print(f"  NPLA ready: {df_npla.height:,} rows\n")

# ============================================================================
# STEP 3: IIS / SP2 — SAS MERGE precedence (IIS wins on collision)
# ============================================================================
print("STEP 3: IIS / SP2...")
_iis_cols = ['ACCTNO','NOTENO','NAME','IIS','OI','TOTIIS','SP','MARKETVL','BRANCH']
df_iis = read_full(PATH_IIS, columns=_iis_cols)
df_sp  = read_full(PATH_SP2, columns=_iis_cols)
for d in (df_iis, df_sp):
    if d is not None:
        pass  # type-normalized below
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

if df_iis is not None and df_sp is not None and df_iis.height and df_sp.height:
    overlap = [c for c in df_iis.columns
               if c in df_sp.columns and c not in ('ACCTNO','NOTENO')]
    df_npl_data = (df_sp.join(df_iis, on=['ACCTNO','NOTENO'], how='full', suffix='_iis')
                        .with_columns([pl.coalesce([pl.col(f'{c}_iis'), pl.col(c)]).alias(c)
                                       for c in overlap])
                        .select(_iis_cols))
else:
    df_npl_data = empty_df({
        'NAME':pl.Utf8,'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
        'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,
        'SP':pl.Float64,'MARKETVL':pl.Float64,'BRANCH':pl.Utf8})
del df_iis, df_sp
gc.collect()

# ============================================================================
# STEP 4: Combine NPLA + NPL (NPLA wins on duplicate keys, per SAS NODUPKEY)
# ============================================================================
print("\nSTEP 4: Combine NPL...")
if df_npl_data.height > 0:
    df_npl = pl.concat([df_npla, df_npl_data], how='vertical_relaxed')
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
# STEP 5: CCRIS (largest DAYS wins per ACCTNO/NOTENO; FACILITY in 34331/34332)
# ============================================================================
print("STEP 5: CCRIS...")
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
# STEP 6+7+8: Merge, derive, filter-ready (LOAN wins on collision)
# ============================================================================
print("STEP 6+7: MERGE NPL/CREDSUB/LOAN + derived fields...")

# LOAN source: HPD subset from cached df_npla_raw
if 'LOANTYPE' in df_npla_raw.columns:
    df_loan_src = (df_npla_raw
                   .select(['ACCTNO','NOTENO','LOANTYPE'])
                   .unique(subset=['ACCTNO','NOTENO'])
                   .filter(pl.col('LOANTYPE').is_in(HPD)))
else:
    df_loan_src = empty_df({'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'LOANTYPE':pl.Float64})

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

# Step 8 extras: only keep needed columns from df_npla_raw
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

# Ensure columns exist
for c in ('FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
          'ECSRRSRV','ORGBAL','CURBAL','PAYAMT','NACOSPADT','SCORE2',
          'CONTRTYPE','NETPROC','APPVALUE'):
    if c not in df_loan.columns:
        df_loan = df_loan.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

# POSTAMT / OTHERAMT / OIFEEAMT
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

# MATUREDT / LASTTRAN / BIRTHDT -> decode + MMDDYY10 strings
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

# Drop helpers no longer needed
drop_helpers = ['FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
                'ECSRRSRV','MATUREDT','LASTTRAN','SCORE2','CONTRTYPE','NETPROC',
                'APPVALUE','BIRTHDT','ORGBAL','PAYAMT','NACOSPADT']
df_loan = df_loan.drop([c for c in drop_helpers if c in df_loan.columns])
gc.collect()
print(f"  LOAN ready: {df_loan.height:,}\n")

# ============================================================================
# STEP 9: CNAME (SECCUST='901')
# ============================================================================
print("STEP 9: Customer names...")
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
# STEP 10: Guarantors (transpose; first 2)
# ============================================================================
print("STEP 10: Guarantors...")
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
# STEP 11: Previous balance
# ============================================================================
print("STEP 11: Previous balance...")
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
# STEP 12: Final merge + SAS filter
# ============================================================================
print("STEP 12: Final merge + filter...")
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

for c in ('BORSTAT','DAYS','LOANTYPE','PAIDIND','TOTAL'):
    if c not in df_woff.columns:
        df_woff = df_woff.with_columns(pl.lit(None).alias(c))
df_woff = (df_woff
           .with_columns([pl.col(c).cast(pl.Utf8) for c in ('BORSTAT','PAIDIND')] +
                         [pl.col(c).cast(pl.Float64) for c in ('DAYS','LOANTYPE','TOTAL')]))

cond_borstat_fi = (pl.col('BORSTAT').is_in(['F','I'])) & (pl.col('DAYS') >= 334)
cond_days       = pl.col('DAYS') >= 334
cond_active     = ((pl.col('BORSTAT') == 'A') &
                   (~pl.col('LOANTYPE').is_in(EXCLUDED_LOANTYPES)) &
                   (pl.col('PAIDIND').is_null() | (pl.col('PAIDIND') != 'P')))

df_woff = (df_woff
           .filter((cond_borstat_fi | cond_days | cond_active) & (pl.col('TOTAL') != 0))
           .with_columns(pl.lit('Y').alias('CONFIRM'))
           .sort('ACCTNO'))
print(f"  WOFF after filter: {df_woff.height:,}\n")

# ============================================================================
# STEP 13: Output — LIST.sas7bdat + streaming text writers
# ============================================================================
print("STEP 13: Writing outputs...")

def _s(v, n):
    return ('' if v is None else str(v))[:n].ljust(n)

def _n(v, n, d=2):
    try: v = 0.0 if v is None else float(v)
    except Exception: v = 0.0
    s = f"{v:.{d}f}"
    return s.rjust(n) if len(s) <= n else s[:n]

def _i(v, n):
    try: v = 0 if v is None else int(float(v))
    except Exception: v = 0
    return str(v).rjust(n)

def _line_wofftex1(r):
    iss = r.get('ISSXDTE')
    iss_s = fmt_mmddyy10(iss) if isinstance(iss, date) else _s(iss, 10)
    return (
        _s(r.get('BRANCH'),7) + _s(r.get('NAME'),40) +
        _n(r.get('ACCTNO'),10,0) + _n(r.get('NOTENO'),5,0) +
        _s(r.get('BORSTAT'),1) +
        _n(r.get('IIS'),16) + _n(r.get('OI'),16) +
        _n(r.get('TOTIIS'),16) + _n(r.get('SP'),16) + ' '*16 +
        _n(r.get('CURBAL'),16) + _n(r.get('PREVBAL'),16) +
        _n(r.get('PAYMENT'),16) + _n(r.get('ECSRRSRV'),16) +
        _n(r.get('POSTAMT'),16) + _n(r.get('OTHERAMT'),16) +
        _s(r.get('MATDATE'),10) + _n(r.get('LOANTYPE'),3,0) +
        _n(r.get('INTAMT'),16) + _s(r.get('POSTNTRN'),1) + ' '*4 +
        _n(r.get('MARKETVL'),16) + _n(r.get('INTEARN4'),16) +
        _n(r.get('DAYS'),6,0) + _n(r.get('CUSTCODE'),3,0) +
        _s(r.get('RIND'),1) + _n(r.get('OIFEEAMT'),16) +
        _s(r.get('LASTTRA1'),10) + _n(r.get('LSTTRNCD'),3,0) +
        _n(r.get('MTHPDUE'),3,0) + _n(r.get('BALANCE'),16) + ' '*17 +
        _s(r.get('GUAREND'),20) + _s(r.get('GUARNAM1'),40) +
        _s(r.get('GUARNAM2'),40) + _s(iss_s,10) +
        _n(r.get('NETPROC'),16) + _s(r.get('COLLDESC'),70) +
        _n(r.get('COLLYEAR'),4,0) + _n(r.get('BILPAID'),3,0) +
        _s(r.get('CRRGRADE'),5) + _n(r.get('MARGINFI'),16) +
        _n(r.get('NOTETERM'),3,0) + _n(r.get('PAYAMT'),16) +
        _s(r.get('DOBMNI'),10) + _s(r.get('ECSRIND'),1) +
        _s(r.get('DELQCD'),2) + ' '*36 +
        _s(r.get('OCCUPAT'),3) + ' '*28 + _s(r.get('BGC'),2) + ' '*23 +
        _s(r.get('PAY75PCT'),1) + _s(r.get('NACODATE'),10) +
        _s(r.get('CP'),1) + _s(r.get('MODELDES'),6) + _s(r.get('AKPK_STATUS'),9)
    )

def write_wofftex1(df, path):
    with open(path, 'w') as f:
        for r in df.iter_rows(named=True):
            f.write(_line_wofftex1(r) + '\n')

def write_wofftext_streaming(src, dst):
    with open(src) as fin, open(dst, 'w') as fout:
        for line in fin:
            L = line.rstrip('\n').ljust(789)
            totiis  = float(L[100:116]) if L[100:116].strip() else 0.0
            balance = float(L[356:372]) if L[356:372].strip() else 0.0
            oi      = float(L[84:100]) if L[84:100].strip() else 0.0
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
    try:
        pyreadstat.write_sas7bdat(df_woff.to_pandas(), str(OUT_LIST))
        print(f"  {OUT_LIST.name} written")
    except Exception as e:
        print(f"  LIST write skipped: {e}")

    print(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    print(f"Accounts: {len(df_woff):,}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")

    write_wofftex1(df_woff, OUTPUT_FILE1)
    print(f"  {OUTPUT_FILE1.name}: {OUTPUT_FILE1.stat().st_size:,} bytes")

    write_wofftext_streaming(OUTPUT_FILE1, OUTPUT_FILE)
    print(f"  {OUTPUT_FILE.name}: {OUTPUT_FILE.stat().st_size:,} bytes")

    # Build WOFFTXT.sas7bdat in a streamed way
    recs = []
    with open(OUTPUT_FILE1) as f:
        for line in f:
            L = line.rstrip('\n')
            if len(L) < 372:
                continue
            tot  = float(L[100:116]) if L[100:116].strip() else 0.0
            bal  = float(L[356:372]) if L[356:372].strip() else 0.0
            recs.append({
                'BRANCH': L[0:7].strip(),
                'NAME':   L[8:48].strip(),
                'ACCTNO': float(L[49:59]) if L[49:59].strip() else 0.0,
                'NOTENO': float(L[60:65]) if L[60:65].strip() else 0.0,
                'BORSTAT': L[66:67],
                'IIS':    float(L[68:84]) if L[68:84].strip() else 0.0,
                'OI':     float(L[84:100]) if L[84:100].strip() else 0.0,
                'TOTIIS': tot, 'BALANCE': bal,
                'SP': bal - tot, 'TOTAL': tot + (bal - tot),
            })
    if recs:
        df_t = pl.DataFrame(recs)
        try:
            pyreadstat.write_sas7bdat(df_t.to_pandas(), str(OUT_WOFFTXT))
            print(f"  {OUT_WOFFTXT.name} written")
        except Exception as e:
            print(f"  WOFFTXT write skipped: {e}")
else:
    print("No accounts for write-off — creating empty files.")
    OUTPUT_FILE1.write_text('')
    OUTPUT_FILE.write_text('')

total_elapsed = time.time() - start_total
print(f"\n{'='*60}\nCOMPLETED IN {total_elapsed:.1f}s\n{'='*60}")
print(f"  {OUTPUT_FILE}\n  {OUTPUT_FILE1}")
if df_woff.height:
    print(f"  {OUT_LIST}\n  {OUT_WOFFTXT}")
