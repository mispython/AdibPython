"""
EIIFTXT1 - Bad Debt Write-Off List Generation
SAS-faithful port, with ENTITY_CD='PIBB' filter on ln_note.
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta, date
from pathlib import Path
import sys
import os
import time

# Format definitions (%INC PGM equivalent)
from PBBLNFMT import (
    HP_ALL, HP_ACTIVE, AITAB, MORE_PLAN, MORE_ISLAM,
    HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL,
    FCY_PRODUCTS,
    format_mthpass, format_ndays, format_lnprod, format_lndenom,
    format_odprod, format_oddenom, format_collcd, format_riskcd,
    format_delqdes, format_statecd
)

try:
    from PBBELF import LIBRARY_PATHS, format_ddmmyy10, format_mmddyy10
except ImportError:
    def format_ddmmyy10(d):
        return d.strftime('%d/%m/%Y') if d else ''
    def format_mmddyy10(d):
        return d.strftime('%m/%d/%Y') if d else ''

# ============================================================================
# TESTING MODE
# ============================================================================
TEST_MODE = True
TEST_LIMIT = 500000

HPD = HP_ACTIVE
EXCLUDED_LOANTYPES = [983, 993, 678, 679, 698, 699]

# ============================================================================
# PATH CONFIG — each input is its own Path (no BASE_INPUT)
# ============================================================================
ROOT = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')

# Each input points to its own source (adjust per actual upstream)
PATH_LN_NOTE  = ROOT / 'input/prod/EIIFTXT1/enrh_ln_note.sas7bdat'   # LOAN.REPTDATE / LNNOTE / LIAB
PATH_IIS      = ROOT / 'input/prod/EIIFTXT1/iis.sas7bdat'            # NPL.IIS
PATH_SP2      = ROOT / 'input/prod/EIIFTXT1/sp2.sas7bdat'            # NPL.SP2
PATH_CNAME    = ROOT / 'input/prod/EIIFTXT1/loan.sas7bdat'           # CISNAME.LOAN
PATH_LIAB     = ROOT / 'input/prod/EIIFTXT1/lnliab07226.sas7bdat'    # LOAN.LIAB
PATH_BKCTRL   = ROOT / 'input/prod/EIIFTXT1/cisfmt.sas7bdat'         # BKCTRL.CISFMT (optional)

OUTPUT_DIR    = ROOT / 'output/EIIFTXT1'
OUTPUT_FILE   = OUTPUT_DIR / 'wofftext.txt'
OUTPUT_FILE1  = OUTPUT_DIR / 'wofftex1.txt'
OUT_LIST      = ROOT / 'output/EIIFTXT1/LIST.sas7bdat'               # NPL.LIST
OUT_WOFFTXT   = ROOT / 'output/EIIFTXT1/WOFFTXT.sas7bdat'            # NPL.WOFFTXT

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Report date & week logic (system clock minus 1 day — per instruction)
# ============================================================================
reptdate = datetime.now().date() - timedelta(days=1)
print(f"\n{'='*60}\nBad Debt Write-Off List Generation\n{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

day = reptdate.day
if   day == 8:  wk, wk1 = '1', '4'
elif day == 15: wk, wk1 = '2', '1'
elif day == 22: wk, wk1 = '3', '2'
else:           wk, wk1 = '4', '3'

mm  = reptdate.month
mm1 = mm - 1 if mm > 1 else 12

nowk, nowks, nowk1 = wk, '4', wk1
reptmon,  reptmon1 = f'{mm:02d}',  f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate    = reptdate.strftime('%d/%m/%y')

# Dynamic paths (month/week dependent)
PATH_CCRIS = ROOT / f'input/prod/EIIFTXT1/icredmsubac{reptmon}{reptyear}.sas7bdat'
PATH_SASLN = ROOT / f'input/prod/EIIFTXT1/loan{reptmon1}{nowks}.sas7bdat'

print(f"Week: {nowk}, Previous Month: {reptmon1}")
if TEST_MODE:
    print(f"*** TEST MODE: Limiting to {TEST_LIMIT} rows ***")
print(f"{'='*60}\n")

# ============================================================================
# Format maps (OCCUPFMT / BGCFMT are normally loaded from BKCTRL.CISFMT)
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

# ============================================================================
# Helpers
# ============================================================================
def get_branch_name(branch_code):
    branch_map = {
        1: 'HQ', 2: 'KL', 3: 'PJ', 4: 'JB', 5: 'PG',
        6: 'IP', 7: 'KK', 8: 'KU', 9: 'MK', 10: 'SB',
    }
    return branch_map.get(branch_code, 'BR')

def read_sas7bdat_fast(filepath, columns=None, row_limit=None):
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"Warning: File not found: {filepath}")
        return None
    start_time = time.time()
    print(f"  Reading: {filepath.name}", end="", flush=True)
    try:
        kw = {'usecols': columns if columns else None}
        if row_limit and row_limit > 0:
            kw['row_limit'] = row_limit
        df, meta = pyreadstat.read_sas7bdat(str(filepath), **kw)
        print(f" - {len(df)} rows, {time.time()-start_time:.1f}s")
        if len(df) == 0:
            return None
        return pl.from_pandas(df)
    except Exception as e:
        print(f"\nError reading {filepath}: {e}")
        return None

def write_sas7bdat(df, filepath):
    try:
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        pyreadstat.write_sas7bdat(df.to_pandas(), str(filepath))
        print(f"  Wrote: {filepath.name}")
    except Exception as e:
        print(f"Error writing {filepath}: {e}")

def standardize_schema(df, schema_dict):
    if df is None or df.height == 0:
        return df
    for col, dtype in schema_dict.items():
        if col in df.columns:
            try:
                if dtype == pl.Int64:
                    df = df.with_columns(pl.col(col).cast(pl.Float64).cast(pl.Int64))
                elif dtype == pl.Float64:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
                elif dtype == pl.Utf8:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))
            except Exception as e:
                print(f"  Warning: cast {col} -> {dtype} failed: {e}")
    return df

def empty_df(schema):
    return pl.DataFrame(schema=schema)

def calc_mthpdue(days):
    """SAS: MTHPDUE = PUT(DAYS,MTHPASS.)*1; IF MTHPDUE=24 THEN MTHPDUE=INT((DAYS/365)*12);"""
    if days is None or days <= 0:
        return 0
    s = format_mthpass(days)
    val = int(s) if s else 0
    if val == 24:                                  # SAS MTHPDUE=24 recalc (#14)
        val = int((days / 365) * 12)
    return val

def decode_z11_mmddyy(value):
    """SAS: INPUT(SUBSTR(PUT(val,Z11.),1,8),MMDDYY8.) -> date -> MMDDYY10."""
    if value is None or value == 0:
        return None
    try:
        s = str(int(value)).zfill(11)
        head = s[:8]                # MMDDYY8 as digits
        return datetime.strptime(head, '%m%d%y').date()
    except Exception:
        return None

def fmt_mmddyy10(d):
    return d.strftime('%m/%d/%Y') if isinstance(d, date) else ''

# ============================================================================
# STEP 1: DEBUG — BORSTAT distribution for PIBB
# ============================================================================
print("STEP 1: DEBUG — BORSTAT values (ENTITY_CD='PIBB')...")
df_sample = read_sas7bdat_fast(PATH_LN_NOTE,
                               columns=['ENTITY_CD', 'BORSTAT', 'LOANTYPE'],
                               row_limit=500000)
if df_sample is not None:
    df_sample = (df_sample
                 .with_columns(pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CD'))
                 .filter(pl.col('ENTITY_CD') == 'PIBB'))
    print(f"  PIBB sample rows: {df_sample.height}")
    for r in df_sample.group_by('BORSTAT').agg(pl.len()).iter_rows(named=True):
        print(f"    '{r['BORSTAT']}' : {r['len']}")
else:
    print("  Could not read sample data"); sys.exit(1)

# ============================================================================
# STEP 2: Read NPLA (ENTITY_CD='PIBB', LOANTYPE NOT IN excluded, BORSTAT='A')
# ============================================================================
print("\nSTEP 2: Reading NPLA (ENTITY_CD='PIBB', BORSTAT='A')...")
start_total = time.time()

loan_columns = [
    'ENTITY_CD', 'BORSTAT', 'LOANTYPE', 'NAME', 'ACCTNO', 'NOTENO',
    'FEEDUE', 'FEEDUEMS', 'FEEAMT16', 'MARKETVL', 'NTBRCH',
    # Step-8 extras (single read)
    'FEETOTAL', 'NFEEAMT5', 'FEEAMT3', 'FEETOT2', 'FEEAMTA', 'FEEAMT5',
    'ECSRRSRV', 'MATUREDT', 'LASTTRAN', 'SCORE2', 'CONTRTYPE', 'NETPROC',
    'APPVALUE', 'BIRTHDT', 'ORGBAL', 'CURBAL', 'PAYAMT', 'NACOSPADT',
    'BALANCE', 'GUAREND', 'ISSXDTE', 'COLLDESC', 'COLLYEAR', 'DELQCD',
    'CP', 'MODELDES', 'AKPK_STATUS', 'PAIDIND', 'POSTNTRN', 'INTAMT',
    'INTEARN4', 'CUSTCODE', 'LSTTRNCD',
]

df_npla_raw = read_sas7bdat_fast(PATH_LN_NOTE, columns=loan_columns,
                                 row_limit=TEST_LIMIT if TEST_MODE else None)
if df_npla_raw is None:
    raise Exception(f"Failed to read {PATH_LN_NOTE}")

# --- ENTITY_CD = 'PIBB' filter ---
df_npla_raw = df_npla_raw.with_columns(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CD')
)
before = df_npla_raw.height
df_npla_raw = df_npla_raw.filter(pl.col('ENTITY_CD') == 'PIBB')
print(f"  ENTITY_CD='PIBB' filter: {before} -> {df_npla_raw.height} rows")
if df_npla_raw.height == 0:
    print("WARNING: No PIBB rows. Exiting."); sys.exit(0)

df_npla_raw = standardize_schema(df_npla_raw, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
    'FEEDUE': pl.Float64, 'FEEDUEMS': pl.Float64, 'FEEAMT16': pl.Float64,
    'MARKETVL': pl.Float64, 'NTBRCH': pl.Float64, 'LOANTYPE': pl.Float64,
})

# SAS filter: WHERE BORSTAT='A' & LOANTYPE NOT IN (983,993,678,679,698,699)
df_active = df_npla_raw.filter(
    (pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars() == 'A') &
    (~pl.col('LOANTYPE').is_in(EXCLUDED_LOANTYPES))
)
print(f"  Active (PIBB, A, LOANTYPE not excluded): {df_active.height}")
if df_active.height == 0:
    print("WARNING: No active accounts after filter. Exiting."); sys.exit(1)

df_npla = df_active.with_columns([
    pl.lit(0.0).alias('IIS'),
    (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0)).alias('OI'),
    (pl.lit(0.0) + (pl.col('FEEDUE').fill_null(0) - pl.col('FEEDUEMS').fill_null(0))).alias('TOTIIS'),
    (pl.col('FEEDUEMS').fill_null(0) + pl.col('FEEAMT16').fill_null(0)).alias('SP'),
])

branch_vals = []
for ntbrch in df_npla['NTBRCH'].to_list():
    if ntbrch is None:
        branch_vals.append('BR 000')
    else:
        try:
            code = int(ntbrch) if isinstance(ntbrch, float) else ntbrch
            branch_vals.append(f"{get_branch_name(code)} {int(code):03d}")
        except Exception:
            branch_vals.append('BR 000')

df_npla = (df_npla
           .with_columns(pl.Series('BRANCH', branch_vals))
           .select(['NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH']))
df_npla = standardize_schema(df_npla, {
    'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
    'IIS': pl.Float64, 'OI': pl.Float64, 'TOTIIS': pl.Float64,
    'SP': pl.Float64, 'MARKETVL': pl.Float64,
})
print(f"  NPLA rows: {df_npla.height}\n")
if df_npla.height == 0:
    print("WARNING: NPLA empty. Exiting."); sys.exit(0)

# ============================================================================
# STEP 3: Read NPL.IIS / NPL.SP2 — SAS MERGE precedence (IIS wins on collision)
# ============================================================================
print("STEP 3: Reading IIS and SP2...")
df_iis = read_sas7bdat_fast(PATH_IIS,
                            columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'],
                            row_limit=TEST_LIMIT if TEST_MODE else None)
if df_iis is not None:
    df_iis = standardize_schema(df_iis, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64, 'MARKETVL': pl.Float64,
    }).unique(subset=['ACCTNO', 'NOTENO'])

df_sp = read_sas7bdat_fast(PATH_SP2,
                           columns=['ACCTNO', 'NOTENO', 'NAME', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'],
                           row_limit=TEST_LIMIT if TEST_MODE else None)
if df_sp is not None:
    df_sp = standardize_schema(df_sp, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64, 'MARKETVL': pl.Float64,
    }).unique(subset=['ACCTNO', 'NOTENO'])

print(f"  IIS rows: {df_iis.height if df_iis is not None else 0}")
print(f"  SP  rows: {df_sp.height if df_sp is not None else 0}")

# SAS: MERGE SP IIS -> last dataset (IIS) wins on overlapping vars
if df_iis is not None and df_sp is not None and df_iis.height and df_sp.height:
    overlap = [c for c in df_iis.columns
               if c in df_sp.columns and c not in ('ACCTNO', 'NOTENO')]
    df_npl_data = (
        df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='full', suffix='_iis')
             .with_columns([pl.coalesce([pl.col(f'{c}_iis'), pl.col(c)]).alias(c)
                            for c in overlap])
             .select(['NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'])
    )
else:
    df_npl_data = empty_df({
        'NAME': pl.Utf8, 'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'IIS': pl.Float64, 'OI': pl.Float64, 'TOTIIS': pl.Float64,
        'SP': pl.Float64, 'MARKETVL': pl.Float64, 'BRANCH': pl.Utf8,
    })

# ============================================================================
# STEP 4: Combine NPLA + NPL (SAS: SET NPLA NPL; sort NODUPKEY -> NPLA wins)
# ============================================================================
print("\nSTEP 4: Combining NPL...")
if df_npl_data.height > 0:
    df_npl_data = standardize_schema(df_npl_data, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'IIS': pl.Float64, 'OI': pl.Float64, 'TOTIIS': pl.Float64,
        'SP': pl.Float64, 'MARKETVL': pl.Float64,
    })
    try:
        df_npl = pl.concat([df_npla, df_npl_data], how='vertical_relaxed')
    except Exception:
        df_npl = df_npla.clone()
else:
    df_npl = df_npla.clone()

df_npl = (df_npl
          .with_columns([
              pl.col('MARKETVL').fill_null(0).round(2),
              pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
              pl.col('BRANCH').str.slice(0, 3).alias('BRABBR'),
          ])
          .unique(subset=['ACCTNO', 'NOTENO'], keep='first'))
print(f"  NPL combined rows: {df_npl.height}\n")

# ============================================================================
# STEP 5: CCRIS
# ============================================================================
print("STEP 5: CCRIS...")
print(f"  Path: {PATH_CCRIS}")
ccris_candidates = [
    PATH_CCRIS,
    PATH_CCRIS.with_name(f'ICREDMSUBAC{reptmon}{reptyear}.sas7bdat'),
    PATH_CCRIS.with_name(f'icredmsubac{reptmon}0{reptyear}.sas7bdat'),
    PATH_CCRIS.with_name(f'ICREDMSUBAC{reptmon}0{reptyear}.sas7bdat'),
]
df_credsub = None
for p in ccris_candidates:
    df_credsub = read_sas7bdat_fast(p, columns=None,
                                    row_limit=TEST_LIMIT if TEST_MODE else None)
    if df_credsub is not None:
        print(f"  Found CCRIS: {p.name}")
        break

if df_credsub is not None and df_credsub.height > 0:
    acct_col = next((c for c in df_credsub.columns
                     if c.lower() in ('acctnum','acctno','acct_num','account','accountno')), None)
    days_col = next((c for c in df_credsub.columns
                     if c.lower() in ('daysarr','days','days_arr','daysarrears')), None)
    fac_col  = next((c for c in df_credsub.columns
                     if c.lower() in ('facility','facility_cd','facilitycd','fac')), None)
    print(f"  cols -> ACCT:{acct_col} DAYS:{days_col} FAC:{fac_col}")

    if acct_col is None:
        df_credsub = empty_df({'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
                               'DAYS': pl.Float64, 'FACILITY': pl.Utf8})
    else:
        ren = {}
        if acct_col and acct_col != 'ACCTNO': ren[acct_col] = 'ACCTNO'
        if days_col and days_col != 'DAYS':   ren[days_col] = 'DAYS'
        if fac_col  and fac_col  != 'FACILITY': ren[fac_col] = 'FACILITY'
        if ren: df_credsub = df_credsub.rename(ren)

        if 'FACILITY' in df_credsub.columns:
            df_credsub = df_credsub.filter(pl.col('FACILITY').is_in(['34331', '34332']))

        keep = [c for c in ['ACCTNO','NOTENO','DAYS','FACILITY'] if c in df_credsub.columns]
        df_credsub = df_credsub.select(keep)

        if {'ACCTNO','NOTENO'} <= set(df_credsub.columns):
            # SAS: SORT BY ACCTNO NOTENO DESCENDING DAYS; NODUPKEY -> keep max DAYS
            df_credsub = (df_credsub
                          .sort(['ACCTNO','NOTENO','DAYS'], descending=[False,False,True])
                          .unique(subset=['ACCTNO','NOTENO'], keep='first'))

        df_credsub = standardize_schema(df_credsub, {
            'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'DAYS': pl.Float64,
        })
else:
    print("  CCRIS missing — empty placeholder.")
    df_credsub = empty_df({'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
                           'DAYS': pl.Float64, 'FACILITY': pl.Utf8})
print(f"  CCRIS rows: {df_credsub.height}\n")

# ============================================================================
# STEP 6: LOAN (HPD types) — SAS MERGE precedence: LOAN wins on collision
# ============================================================================
print("STEP 6: Filtering HPD loan types...")
if df_npla_raw is not None and 'LOANTYPE' in df_npla_raw.columns:
    df_loan_src = (df_npla_raw
                   .select(['ACCTNO', 'NOTENO', 'LOANTYPE'])
                   .unique(subset=['ACCTNO', 'NOTENO'])
                   .filter(pl.col('LOANTYPE').is_in(HPD)))
else:
    df_loan_src = empty_df({'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'LOANTYPE': pl.Float64})
print(f"  HPD rows: {df_loan_src.height}\n")

# ============================================================================
# STEP 7: MERGE NPL(IN=AA) CREDSUB LOAN; IF AA -> keep NPL, LOAN wins on collision
# ============================================================================
print("STEP 7: MERGE NPL / CREDSUB / LOAN...")
if df_npl.height > 0:
    df_loan = df_npl.clone()

    # CREDSUB
    if df_credsub.height > 0:
        df_loan = df_loan.join(df_credsub, on=['ACCTNO','NOTENO'], how='left')
    else:
        df_loan = df_loan.with_columns([
            pl.lit(None).cast(pl.Float64).alias('DAYS'),
            pl.lit(None).cast(pl.Utf8).alias('FACILITY'),
        ])

    # LOAN — merge in with LOAN-wins precedence
    if df_loan_src.height > 0:
        overlap_loan = [c for c in df_loan_src.columns
                        if c in df_loan.columns and c not in ('ACCTNO','NOTENO')]
        df_loan = (df_loan.join(df_loan_src, on=['ACCTNO','NOTENO'],
                                how='left', suffix='_loan')
                          .with_columns([pl.coalesce([pl.col(f'{c}_loan'), pl.col(c)]).alias(c)
                                         for c in overlap_loan]))
    else:
        df_loan = df_loan.with_columns([pl.lit(None).cast(pl.Float64).alias('LOANTYPE')])

    df_loan = df_loan.filter(pl.col('ACCTNO').is_not_null())
else:
    df_loan = df_npl.clone()
print(f"  LOAN rows: {df_loan.height}\n")

# ============================================================================
# STEP 8: Derived fields (SAS-faithful)
# ============================================================================
print("STEP 8: Derived fields...")
start_time = time.time()

if df_loan.height > 0:
    extra_cols = ['ACCTNO','NOTENO','FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2',
                  'FEEAMTA','FEEAMT5','ECSRRSRV','MATUREDT','LASTTRAN',
                  'SCORE2','CONTRTYPE','NETPROC','APPVALUE','BIRTHDT','ORGBAL',
                  'CURBAL','PAYAMT','NACOSPADT','BALANCE','GUAREND','ISSXDTE',
                  'COLLDESC','COLLYEAR','DELQCD','CP','MODELDES','AKPK_STATUS',
                  'BORSTAT','PAIDIND','POSTNTRN','INTAMT','INTEARN4',
                  'CUSTCODE','LSTTRNCD']

    if df_npla_raw is not None:
        avail = [c for c in extra_cols if c in df_npla_raw.columns]
        df_extra = df_npla_raw.select(avail).unique(subset=['ACCTNO','NOTENO'])
        for c in ['FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
                  'ECSRRSRV','NETPROC','APPVALUE','ORGBAL','CURBAL','PAYAMT',
                  'BALANCE','INTAMT','INTEARN4','CUSTCODE','LSTTRNCD']:
            if c in df_extra.columns:
                df_extra = df_extra.with_columns(pl.col(c).cast(pl.Float64))

        overlap_extra = [c for c in df_extra.columns
                         if c in df_loan.columns and c not in ('ACCTNO','NOTENO')]
        df_loan = (df_loan.join(df_extra, on=['ACCTNO','NOTENO'],
                                how='left', suffix='_x')
                          .with_columns([pl.coalesce([pl.col(f'{c}_x'), pl.col(c)]).alias(c)
                                         for c in overlap_extra]))

    # Ensure columns exist
    for c in ['FEETOTAL','NFEEAMT5','FEEAMT3','FEETOT2','FEEAMTA','FEEAMT5',
              'ECSRRSRV','ORGBAL','CURBAL','PAYAMT','NACOSPADT','SCORE2',
              'CONTRTYPE','NETPROC','APPVALUE']:
        if c not in df_loan.columns:
            df_loan = df_loan.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    # POSTAMT = SUM(FEETOTAL,NFEEAMT5)
    df_loan = df_loan.with_columns(
        (pl.col('FEETOTAL').fill_null(0) + pl.col('NFEEAMT5').fill_null(0)).alias('POSTAMT'))
    # OTHERAMT = SUM(FEEAMT3, -POSTAMT)
    df_loan = df_loan.with_columns(
        (pl.col('FEEAMT3').fill_null(0) - pl.col('POSTAMT')).alias('OTHERAMT'))
    # OIFEEAMT = SUM(FEETOT2, -FEEAMTA, FEEAMT5)
    df_loan = df_loan.with_columns(
        (pl.col('FEETOT2').fill_null(0) - pl.col('FEEAMTA').fill_null(0) + pl.col('FEEAMT5').fill_null(0)).alias('OIFEEAMT'))

    # ECSRRSRV = max(ECSRRSRV, 0)
    df_loan = df_loan.with_columns(
        pl.when(pl.col('ECSRRSRV').is_null() | (pl.col('ECSRRSRV') <= 0))
          .then(0.0).otherwise(pl.col('ECSRRSRV')).alias('ECSRRSRV'))

    # ECSRIND
    df_loan = df_loan.with_columns(
        pl.when(pl.col('ECSRRSRV') > 0).then(pl.lit('Y')).otherwise(pl.lit('N')).alias('ECSRIND'))

    # MATUREDT decode -> MATDATE / MATUREDT (date)
    df_loan = df_loan.with_columns([
        pl.col('MATUREDT').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('MATDATE_D'),
        pl.col('LASTTRAN').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('LASTTRA1_D'),
        pl.col('BIRTHDT').map_elements(decode_z11_mmddyy, return_dtype=pl.Date).alias('DOBMNI_D'),
    ])
    df_loan = df_loan.with_columns([
        pl.col('MATDATE_D').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('MATDATE'),
        pl.col('LASTTRA1_D').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('LASTTRA1'),
        pl.col('DOBMNI_D').map_elements(fmt_mmddyy10, return_dtype=pl.Utf8).alias('DOBMNI'),
    ])

    # MTHPDUE
    if 'DAYS' in df_loan.columns:
        df_loan = df_loan.with_columns(
            pl.col('DAYS').map_elements(calc_mthpdue, return_dtype=pl.Int64).alias('MTHPDUE'))
    else:
        df_loan = df_loan.with_columns(pl.lit(0).alias('MTHPDUE'))

    # CRRGRADE = TRIM(SCORE2)||TRIM(CONTRTYPE) — SAS treats as char; cast numerics to int string
    def _trunc_str(v):
        if v is None:
            return ''
        if isinstance(v, float):
            return str(int(v)) if v == int(v) else str(v).rstrip('0').rstrip('.')
        return str(v).strip()

    df_loan = df_loan.with_columns(
        (pl.col('SCORE2').map_elements(_trunc_str, return_dtype=pl.Utf8) +
         pl.col('CONTRTYPE').map_elements(_trunc_str, return_dtype=pl.Utf8)).alias('CRRGRADE'))

    # MARGINFI = ROUND(NETPROC/APPVALUE,.01)
    df_loan = df_loan.with_columns(
        pl.when(pl.col('APPVALUE').is_not_null() & (pl.col('APPVALUE') != 0))
          .then((pl.col('NETPROC') / pl.col('APPVALUE')).round(2))
          .otherwise(0.0).alias('MARGINFI'))

    # BILPAID = INT((ORGBAL-CURBAL)/PAYAMT)  — SAS INT truncates toward -inf
    df_loan = df_loan.with_columns(
        pl.when(pl.col('PAYAMT').is_not_null() & (pl.col('PAYAMT') > 0))
          .then(((pl.col('ORGBAL').fill_null(0) - pl.col('CURBAL').fill_null(0)) / pl.col('PAYAMT')).floor())
          .otherwise(0).cast(pl.Int64).alias('BILPAID'))

    # PAY75PCT / NACODATE
    df_loan = df_loan.with_columns([
        pl.when(pl.col('NACOSPADT').is_not_null() & (pl.col('NACOSPADT') > 0))
          .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('PAY75PCT'),
    ])
    df_loan = df_loan.with_columns(
        pl.when(pl.col('NACOSPADT').is_not_null() & (pl.col('NACOSPADT') > 0))
          .then(pl.col('NACOSPADT').map_elements(
              lambda v: fmt_mmddyy10(decode_z11_mmddyy(v)), return_dtype=pl.Utf8))
          .otherwise(pl.lit('')).alias('NACODATE'))

    # Drop helper columns
    df_loan = df_loan.drop([c for c in ('MATDATE_D','LASTTRA1_D','DOBMNI_D') if c in df_loan.columns])

elapsed = time.time() - start_time
print(f"  Derived fields in {elapsed:.1f}s; rows: {df_loan.height}\n")

# ============================================================================
# STEP 9: Customer names
# ============================================================================
print("STEP 9: Customer names...")
df_cname = read_sas7bdat_fast(PATH_CNAME,
                              columns=['ACCTNO','CUSTNAM1','OCCUPAT','BGC','SECCUST'],
                              row_limit=TEST_LIMIT if TEST_MODE else None)
if df_cname is not None:
    df_cname = (standardize_schema(df_cname, {'ACCTNO': pl.Float64})
                .filter(pl.col('SECCUST') == '901')
                .select(['ACCTNO','CUSTNAM1','OCCUPAT','BGC'])
                .unique(subset=['ACCTNO']))
else:
    df_cname = empty_df({'ACCTNO': pl.Float64, 'CUSTNAM1': pl.Utf8,
                         'OCCUPAT': pl.Utf8, 'BGC': pl.Utf8})
print(f"  CNAME rows: {df_cname.height}\n")

# ============================================================================
# STEP 10: Guarantors (transpose, first two)
# ============================================================================
print("STEP 10: Guarantors...")
guarantor_data = {}
df_liab = read_sas7bdat_fast(PATH_LIAB,
                             columns=['ACCTNO','NOTENO','LIABACCT','LIABNAME'],
                             row_limit=TEST_LIMIT if TEST_MODE else None)
if df_liab is not None and df_cname.height > 0:
    df_liab = standardize_schema(df_liab, {
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'LIABACCT': pl.Float64,
    }).sort('LIABACCT')

    df_liab = (df_liab
               .join(df_cname.rename({'ACCTNO':'LIABACCT','CUSTNAM1':'GNAME'}),
                     on='LIABACCT', how='left')
               .with_columns(
                   pl.when(pl.col('GNAME').is_null() | (pl.col('GNAME') == ''))
                     .then(pl.col('LIABNAME'))
                     .otherwise(pl.col('GNAME')).alias('GNAME'))
               .sort(['ACCTNO','NOTENO']))

    for (acctno, noteno), grp in df_liab.group_by(['ACCTNO','NOTENO']):
        names = grp['GNAME'].to_list()
        guarantor_data[(acctno, noteno)] = {
            'GUARNAM1': names[0] if len(names) > 0 else '',
            'GUARNAM2': names[1] if len(names) > 1 else '',
        }
print(f"  Guarantor keys: {len(guarantor_data)}\n")

if df_loan.height > 0:
    g1, g2 = [], []
    for r in df_loan.iter_rows(named=True):
        gd = guarantor_data.get((r['ACCTNO'], r['NOTENO']),
                                {'GUARNAM1':'', 'GUARNAM2':''})
        g1.append(gd['GUARNAM1']); g2.append(gd['GUARNAM2'])
    df_loan = df_loan.with_columns([
        pl.Series('GUARNAM1', g1), pl.Series('GUARNAM2', g2),
    ])

# ============================================================================
# STEP 11: Previous balance
# ============================================================================
print("STEP 11: Previous balance...")
df_sasln = read_sas7bdat_fast(PATH_SASLN,
                              columns=['ACCTNO','NOTENO','CURBAL'],
                              row_limit=TEST_LIMIT if TEST_MODE else None)
if df_sasln is not None:
    df_sasln = (standardize_schema(df_sasln, {'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'CURBAL': pl.Float64})
                .rename({'CURBAL':'PREVBAL'})
                .sort(['ACCTNO','NOTENO'])
                .join(df_npl.select(['ACCTNO','NOTENO']), on=['ACCTNO','NOTENO'], how='inner'))
else:
    df_sasln = empty_df({'ACCTNO': pl.Float64, 'NOTENO': pl.Float64, 'PREVBAL': pl.Float64})
print(f"  SASLN rows: {df_sasln.height}\n")

# ============================================================================
# STEP 12: Final MERGE + filter
# ============================================================================
print("STEP 12: MERGE / filter...")
if df_sasln.height > 0 and df_loan.height > 0:
    df_sasln = standardize_schema(df_sasln, {'ACCTNO': pl.Float64, 'NOTENO': pl.Float64})
    df_loan  = standardize_schema(df_loan,  {'ACCTNO': pl.Float64, 'NOTENO': pl.Float64})
    df_woff  = df_sasln.join(df_loan, on=['ACCTNO','NOTENO'], how='full')

    if 'BRANCH' not in df_woff.columns:
        df_woff = df_woff.join(df_npl.select(['ACCTNO','BRANCH','MARKETVL']), on='ACCTNO', how='left')

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

print(f"  WOFF pre-filter: {df_woff.height}")

# Ensure required columns exist
for c in ['BORSTAT','DAYS','LOANTYPE','PAIDIND','TOTAL']:
    if c not in df_woff.columns:
        df_woff = df_woff.with_columns(pl.lit(None).alias(c))

df_woff = (df_woff
           .with_columns([pl.col(c).cast(pl.Utf8) for c in ('BORSTAT','PAIDIND')] +
                         [pl.col(c).cast(pl.Float64) for c in ('DAYS','LOANTYPE','TOTAL')]))

# SAS IF semantics:
#   (BORSTAT IN ('F','I') AND DAYS GE 334)
# OR DAYS GE 334
# OR (BORSTAT='A' & LOANTYPE NOT IN excluded & PAIDIND NE 'P')
# IF TOTAL NE 0
# Note: SAS null NE 'P' -> TRUE; emulate by treating null PAIDIND as != 'P'
cond_borstat_fi = (pl.col('BORSTAT').is_in(['F','I'])) & (pl.col('DAYS') >= 334)
cond_days       = pl.col('DAYS') >= 334
cond_active     = ((pl.col('BORSTAT') == 'A') &
                   (~pl.col('LOANTYPE').is_in(EXCLUDED_LOANTYPES)) &
                   (pl.col('PAIDIND').is_null() | (pl.col('PAIDIND') != 'P')))

df_woff = (df_woff
           .filter((cond_borstat_fi | cond_days | cond_active) & (pl.col('TOTAL') != 0))
           .with_columns(pl.lit('Y').alias('CONFIRM'))
           .sort('ACCTNO'))

if 'NAME' not in df_woff.columns and df_cname.height > 0:
    df_woff = df_woff.join(df_cname.rename({'CUSTNAM1':'NAME'}), on='ACCTNO', how='left')

print(f"  WOFF after filter: {df_woff.height}\n")

# ============================================================================
# STEP 13: Write outputs — byte-layout faithful to SAS @n specs
# ============================================================================
print("STEP 13: Writing outputs...")

def _s(v, n):
    """Truncate/pad a string to exactly n chars (left-justified)."""
    v = '' if v is None else str(v)
    return v[:n].ljust(n)

def _n(v, n, d=2):
    """Numeric right-justified with n total width, d decimals."""
    try:
        v = 0.0 if v is None else float(v)
    except Exception:
        v = 0.0
    s = f"{v:.{d}f}"
    return s.rjust(n) if len(s) <= n else s[:n]

def _i(v, n):
    try:
        v = 0 if v is None else int(float(v))
    except Exception:
        v = 0
    return str(v).rjust(n)

def write_wofftex1(df, path):
    with open(path, 'w') as f:
        for r in df.iter_rows(named=True):
            line = (
                _s(r.get('BRANCH'), 7) +              # @1
                _s(r.get('NAME'), 40) +               # @9
                _n(r.get('ACCTNO'), 10, 0) +          # @50
                _n(r.get('NOTENO'), 5, 0) +           # @61
                _s(r.get('BORSTAT'), 1) +             # @67
                _n(r.get('IIS'), 16) +                # @69
                _n(r.get('OI'), 16) +                 # @85
                _n(r.get('TOTIIS'), 16) +             # @101
                _n(r.get('SP'), 16) +                 # @117
                # @133 TOTAL skipped per SAS comment
                ' ' * 16 +                            # @133..148 placeholder
                _n(r.get('CURBAL'), 16) +             # @149
                _n(r.get('PREVBAL'), 16) +            # @165
                _n(r.get('PAYMENT'), 16) +            # @181
                _n(r.get('ECSRRSRV'), 16) +           # @197
                _n(r.get('POSTAMT'), 16) +            # @213
                _n(r.get('OTHERAMT'), 16) +           # @229
                _s(r.get('MATDATE'), 10) +            # @245
                _n(r.get('LOANTYPE'), 3, 0) +         # @255
                _n(r.get('INTAMT'), 16) +             # @258
                _s(r.get('POSTNTRN'), 1) +            # @274
                ' ' * 4 +                             # @275..278 gap
                _n(r.get('MARKETVL'), 16) +           # @278
                _n(r.get('INTEARN4'), 16) +           # @294
                _n(r.get('DAYS'), 6, 0) +             # @310
                _n(r.get('CUSTCODE'), 3, 0) +         # @317
                _s(r.get('RIND'), 1) +                # @321
                _n(r.get('OIFEEAMT'), 16) +           # @322
                _s(r.get('LASTTRA1'), 10) +           # @339
                _n(r.get('LSTTRNCD'), 3, 0) +         # @350
                _n(r.get('MTHPDUE'), 3, 0) +          # @354
                _n(r.get('BALANCE'), 16) +            # @357
                ' ' * 17 +                            # @374..391 CAP slot
                _s(r.get('GUAREND'), 20) +            # @408
                _s(r.get('GUARNAM1'), 40) +           # @429
                _s(r.get('GUARNAM2'), 40) +           # @470
                _s(fmt_mmddyy10(r.get('ISSXDTE')) if isinstance(r.get('ISSXDTE'), date) else r.get('ISSXDTE'), 10) +  # @511
                _n(r.get('NETPROC'), 16) +            # @522
                _s(r.get('COLLDESC'), 70) +           # @539
                _n(r.get('COLLYEAR'), 4, 0) +         # @610
                _n(r.get('BILPAID'), 3, 0) +          # @615
                _s(r.get('CRRGRADE'), 5) +            # @619
                _n(r.get('MARGINFI'), 16) +           # @625
                _n(r.get('NOTETERM'), 3, 0) +         # @642
                _n(r.get('PAYAMT'), 16) +             # @646
                _s(r.get('DOBMNI'), 10) +             # @663
                _s(r.get('ECSRIND'), 1) +             # @674
                _s(r.get('DELQCD'), 2) +              # @677
                ' ' * 36 +                            # @679..715 gap
                _s(r.get('OCCUPAT'), 3) +             # @713  (overwrites gap tail)
                ' ' * 28 +                            # @716..744 gap
                _s(r.get('BGC'), 2) +                 # @743
                ' ' * 23 +                            # @745..768 gap
                _s(r.get('PAY75PCT'), 1) +            # @767
                _s(r.get('NACODATE'), 10) +           # @769
                _s(r.get('CP'), 1) +                  # @780
                _s(r.get('MODELDES'), 6) +            # @782
                _s(r.get('AKPK_STATUS'), 9)           # @789
            )
            f.write(line + '\n')

def write_wofftext_from_wofftex1(src_path, dst_path, df_woff):
    """Second pass: rebuild final line with SP/TOTAL/CAP/LATECHG/DELQDES/OCCUPDES/BGCDES inserted."""
    with open(src_path, 'r') as fin, open(dst_path, 'w') as fout:
        for line in fin:
            line = line.rstrip('\n').ljust(789)
            totiis = float(line[100:116]) if line[100:116].strip() else 0.0
            balance = float(line[356:372]) if line[356:372].strip() else 0.0
            sp_calc = balance - totiis
            total_calc = totiis + sp_calc
            oi = float(line[84:100]) if line[84:100].strip() else 0.0

            delqcd  = line[676:678]
            occupat = line[712:715]
            bgc     = line[742:744]

            out = (
                line[:116] +
                _n(sp_calc, 16) +
                _n(total_calc, 16) +
                line[148:373] +
                _n(0.0, 16) +                       # CAP
                _n(oi, 16) +                        # LATECHG
                line[407:679] +
                _s(get_delq_desc(delqcd), 30) +
                _s('I', 1) +                        # BIZTYPE
                _s(occupat, 3) +
                _s(get_occup_desc(occupat), 25) +
                _s(bgc, 2) +
                _s(get_bgc_desc(bgc), 20) +
                line[766:]
            )
            fout.write(out + '\n')

if df_woff.height > 0:
    write_sas7bdat(df_woff, OUT_LIST)
    print(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    print(f"Accounts for write-off: {len(df_woff)}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")

    write_wofftex1(df_woff, OUTPUT_FILE1)
    print(f"  {OUTPUT_FILE1} written ({OUTPUT_FILE1.stat().st_size:,} bytes)")

    write_wofftext_from_wofftex1(OUTPUT_FILE1, OUTPUT_FILE, df_woff)
    print(f"  {OUTPUT_FILE} written ({OUTPUT_FILE.stat().st_size:,} bytes)")

    # Build WOFFTXT dataset from the final text file's TAB-delimited equivalent
    # (for parity, mirror the SAS TEXT -> WOFFTXT dataset)
    text_records = []
    with open(OUTPUT_FILE1) as f:
        for line in f:
            line = line.rstrip('\n')
            if len(line) < 372:
                continue
            rec = {
                'BRANCH': line[0:7].strip(),
                'NAME':   line[8:48].strip(),
                'ACCTNO': float(line[49:59]) if line[49:59].strip() else 0.0,
                'NOTENO': float(line[60:65]) if line[60:65].strip() else 0.0,
                'BORSTAT': line[66:67],
                'IIS':    float(line[68:84]) if line[68:84].strip() else 0.0,
                'OI':     float(line[84:100]) if line[84:100].strip() else 0.0,
                'TOTIIS': float(line[100:116]) if line[100:116].strip() else 0.0,
                'BALANCE': float(line[356:372]) if line[356:372].strip() else 0.0,
            }
            rec['SP'] = rec['BALANCE'] - rec['TOTIIS']
            rec['TOTAL'] = rec['TOTIIS'] + rec['SP']
            text_records.append(rec)
    if text_records:
        df_text = pl.DataFrame(text_records)
        write_sas7bdat(df_text, OUT_WOFFTXT)
        print(f"  {OUT_WOFFTXT} written")
else:
    print("\nNo accounts for write-off — creating empty output files.")
    OUTPUT_FILE1.write_text('')
    OUTPUT_FILE.write_text('')
    print(f"  Empty: {OUTPUT_FILE1}")
    print(f"  Empty: {OUTPUT_FILE}")

total_elapsed = time.time() - start_total
print(f"\n{'='*60}\nCOMPLETED IN {total_elapsed:.1f}s\n{'='*60}")
print(f"Output files:\n  {OUTPUT_FILE}\n  {OUTPUT_FILE1}")
if df_woff.height > 0:
    print(f"  {OUT_LIST}\n  {OUT_WOFFTXT}")
