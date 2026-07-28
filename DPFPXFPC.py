"""
EIFFTXT1 - Bad Debt Write-Off List (Conventional Banking) - OPTIMIZED VERSION
Includes: PBBLNFMT, PBBELF format definitions

Key Differences from EIIFTXT1:
- RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)
- BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)
- Uses CREDMSUBAC (not ICREDMSUBAC - no 'I' prefix)

=== FIX NOTES (this version) ===
Root cause of the crash: pyreadstat.read_sas7bdat(..., usecols=[...]) matches
column names CASE-SENSITIVELY against the physical .sas7bdat file. The SAS
file's real column names (e.g. BORSTAT, ACCTNO) did not match the lowercase
names requested (borstat, acctno). pyreadstat did not raise an error for
that mismatch - it just silently returned a frame with 0 usable columns/rows,
which is why the log showed "Successfully read 0 records" with no exception.
Then df_lnnote.query("borstat == 'A' ...") failed with
UndefinedVariableError: name 'borstat' is not defined, because the column
genuinely was not in the dataframe.

Fixes applied:
1. New helper `read_sas_columns()` reads file metadata first, matches
   requested column names case-insensitively against the real file schema,
   reads using the FILE'S actual casing, then normalizes the result back to
   lowercase. Raises a clear, actionable error (listing available columns)
   if a requested column truly does not exist - instead of silently
   returning an empty/partial frame.
2. Added an explicit guard right after reading LNNOTE: if the frame is
   empty or missing required columns, we stop with a clear message instead
   of proceeding into `.query()`/downstream logic that would fail deep in
   pandas internals.
3. Replaced all `.query("string expression")` calls with boolean-mask
   indexing. This avoids pandas' string-eval engine entirely (which is what
   turned the missing column into a confusing multi-frame stack trace) and
   fails instantly and legibly if a column is missing.
4. Applied the same safe reader + column normalization to every other
   sas7bdat read in the script (iis, sp2, credmsubac, HPD lnnote re-read,
   loan/cname, liab, previous-month loan) for consistency, since they were
   all vulnerable to the identical case-mismatch failure mode.
5. Added small defensive checks (e.g. required-column validation) after
   each read so any future schema drift fails fast with a useful message.
"""

import pandas as pd
import pyreadstat
from datetime import datetime, timedelta
import sys
import os
import gc
import numpy as np

# Input directory paths (all lowercase)
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
NPL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
SASLN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
CISNAME_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
CCRIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
BKCTRL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'

OUTPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftext.txt'
OUTPUT_FILE1 = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftex1.txt'

# HPD loan types (from PBBLNFMT)
HPD = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
       201, 202, 203, 204, 205, 206, 207, 208, 209, 210,
       301, 302, 303, 304, 305, 306, 307, 308, 309, 310]


# =====================================================================
# ROBUST SAS READER (FIX)
# =====================================================================
def read_sas_columns(path, wanted_cols, required=None, disable_datetime_conversion=False,
                      encoding=None, allow_missing_file=False):
    """
    Safely read specific columns from a .sas7bdat file.

    - Matches `wanted_cols` against the file's ACTUAL column names
      case-insensitively (fixes the silent-empty-frame bug).
    - Returns a DataFrame with all column names lowercased.
    - Raises a clear RuntimeError (listing available columns) if any column
      in `required` (default: all of wanted_cols) truly isn't in the file.
    - If allow_missing_file=True and the file doesn't exist, returns an
      empty DataFrame with the requested (lowercased) columns instead of
      raising, so downstream merges still work.
    """
    if required is None:
        required = wanted_cols

    if not os.path.exists(path):
        if allow_missing_file:
            print(f"  Warning: file not found, using empty frame: {path}")
            return pd.DataFrame(columns=[c.lower() for c in wanted_cols])
        raise FileNotFoundError(f"Required SAS file not found: {path}")

    # Peek at metadata to discover the file's real column names/casing.
    _, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
    actual_cols = meta.column_names
    actual_lookup = {c.lower(): c for c in actual_cols}

    resolved_cols = []
    missing = []
    for wc in wanted_cols:
        real_name = actual_lookup.get(wc.lower())
        if real_name is None:
            missing.append(wc)
        else:
            resolved_cols.append(real_name)

    still_missing_required = [c for c in required if c not in wanted_cols or c in missing]
    hard_missing = [c for c in required if c.lower() not in actual_lookup]
    if hard_missing:
        raise RuntimeError(
            f"Column(s) {hard_missing} not found in {path}.\n"
            f"Available columns in file: {sorted(actual_cols)}"
        )

    read_kwargs = {}
    if disable_datetime_conversion:
        read_kwargs['disable_datetime_conversion'] = True
    if encoding:
        read_kwargs['encoding'] = encoding

    df, _ = pyreadstat.read_sas7bdat(path, usecols=resolved_cols, **read_kwargs)
    # Normalize every column name to lowercase so the rest of the script
    # (written entirely in lowercase) works regardless of file casing.
    df.columns = [c.lower() for c in df.columns]

    return df


def ensure_columns(df, cols, context=""):
    """Fail fast and clearly if expected columns are missing after a merge."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"Missing expected column(s) {missing} {('in ' + context) if context else ''}. "
            f"Columns present: {list(df.columns)}"
        )


# ===== FUNCTION DEFINITIONS =====
def get_branch_name(branch_code):
    """Get branch abbreviation - simplified version"""
    branch_map = {
        1: 'KL', 2: 'PJ', 3: 'JB', 4: 'PG', 5: 'IP',
    }
    return branch_map.get(branch_code, 'UNK')

def ndays_format(days):
    """Convert days to months past due"""
    if days <= 0:
        return 0
    elif days <= 30:
        return 1
    elif days <= 60:
        return 2
    elif days <= 90:
        return 3
    elif days <= 120:
        return 4
    elif days <= 150:
        return 5
    elif days <= 180:
        return 6
    elif days <= 210:
        return 7
    elif days <= 240:
        return 8
    elif days <= 270:
        return 9
    elif days <= 300:
        return 10
    elif days <= 330:
        return 11
    elif days <= 365:
        return 12
    else:
        return int(days / 30)

def format_ddmmyy10(date_obj):
    """Format date as DD/MM/YYYY"""
    if pd.isna(date_obj) or date_obj is None:
        return ''
    return date_obj.strftime('%d/%m/%Y')

def format_mmddyy10(date_obj):
    """Format date as MM/DD/YYYY"""
    if pd.isna(date_obj) or date_obj is None:
        return ''
    return date_obj.strftime('%m/%d/%Y')

def mthpass_format(days):
    """Convert days to months past due - same as NDAYS"""
    return ndays_format(days)

def safe_format_date(date_val, fmt_func):
    """Safely format date values"""
    if pd.isna(date_val) or date_val is None or date_val == 0:
        return ''
    try:
        date_str = str(int(date_val)).zfill(8)
        if len(date_str) >= 8:
            dt = datetime.strptime(date_str[:8], '%m%d%Y')
            return fmt_func(dt)
    except:
        pass
    return ''

def safe_get_date(date_val):
    """Safely get date object"""
    if pd.isna(date_val) or date_val is None or date_val == 0:
        return None
    try:
        date_str = str(int(date_val)).zfill(8)
        if len(date_str) >= 8:
            return datetime.strptime(date_str[:8], '%m%d%Y').date()
    except:
        pass
    return None

def create_fixed_width_line(row):
    """Create fixed-width line from row data"""
    branch = str(row.get('branch', '') or '')[:7]
    name = str(row.get('name', '') or '')[:40]
    acctno = row.get('acctno', 0) or 0
    noteno = row.get('noteno', 0) or 0
    borstat = str(row.get('borstat', '') or '')[:1]
    iis = row.get('iis', 0) or 0
    oi = row.get('oi', 0) or 0
    totiis = row.get('totiis', 0) or 0
    sp = row.get('sp', 0) or 0
    curbal = row.get('curbal', 0) or 0
    prevbal = row.get('prevbal', 0) or 0
    payment = row.get('payment', 0) or 0
    ecsrrsrv = row.get('ecsrrsrv', 0) or 0
    postamt = row.get('postamt', 0) or 0
    otheramt = row.get('otheramt', 0) or 0
    matdate = str(row.get('matdate', '') or '')[:10]
    loantype = row.get('loantype', 0) or 0
    intamt = row.get('intamt', 0) or 0
    postntrn = str(row.get('postntrn', '') or '')[:1]
    marketvl = row.get('marketvl', 0) or 0
    intearn4 = row.get('intearn4', 0) or 0
    days = row.get('days', 0) or 0
    custcode = row.get('custcode', 0) or 0
    rind = str(row.get('rind', '') or '')[:1]
    oifeeamt = row.get('oifeeamt', 0) or 0
    lasttra1 = str(row.get('lasttra1', '') or '')[:10]
    lsttrncd = row.get('lsttrncd', 0) or 0
    mthpdue = row.get('mthpdue', 0) or 0
    balance = row.get('balance', 0) or 0
    guarend = str(row.get('guarend', '') or '')[:20]
    guarnam1 = str(row.get('guarnam1', '') or '')[:40]
    guarnam2 = str(row.get('guarnam2', '') or '')[:40]

    issxdte = row.get('issxdte', '')
    if pd.notna(issxdte) and issxdte:
        try:
            issxdte_str = format_mmddyy10(issxdte)[:10]
        except:
            issxdte_str = ' ' * 10
    else:
        issxdte_str = ' ' * 10

    netproc = row.get('netproc', 0) or 0
    colldesc = str(row.get('colldesc', '') or '')[:70]
    collyear = row.get('collyear', 0) or 0
    bilpaid = row.get('bilpaid', 0) or 0
    crrgrade = str(row.get('crrgrade', '') or '')[:5]
    marginfi = row.get('marginfi', 0) or 0
    noteterm = row.get('noteterm', 0) or 0
    payamt = row.get('payamt', 0) or 0

    dobmni = row.get('dobmni', '')
    if pd.notna(dobmni) and dobmni:
        try:
            dobmni_str = format_mmddyy10(dobmni)[:10]
        except:
            dobmni_str = ' ' * 10
    else:
        dobmni_str = ' ' * 10

    ecsrind = str(row.get('ecsrind', '') or '')[:1]
    delqcd = str(row.get('delqcd', '') or '')[:2]
    occupat = str(row.get('occupat', '') or '')[:3]
    bgc = str(row.get('bgc', '') or '')[:2]
    pay75pct = str(row.get('pay75pct', '') or '')[:1]
    nacodate = str(row.get('nacodate', '') or '')[:10]
    cp = str(row.get('cp', '') or '')[:1]
    modeldes = str(row.get('modeldes', '') or '')[:6]
    akpk_status = str(row.get('akpk_status', '') or '')[:9]

    line = f"{branch:<7} {name:<40}{acctno:>10.0f}{noteno:>5.0f}{borstat:1}"
    line += f"{iis:>16.2f}{oi:>16.2f}{totiis:>16.2f}{sp:>16.2f}"
    line += f"{curbal:>16.2f}{prevbal:>16.2f}{payment:>16.2f}"
    line += f"{ecsrrsrv:>16.2f}{postamt:>16.2f}{otheramt:>16.2f}"
    line += f"{matdate:<10}{int(loantype):>3d}{intamt:>16.2f}{postntrn:1}"
    line += f"{marketvl:>16.2f}{intearn4:>16.2f}{int(days):>6d}{int(custcode):>3d}{rind:1}"
    line += f"{oifeeamt:>16.2f}{lasttra1:<10}{int(lsttrncd):>3d}{int(mthpdue):>3d}"
    line += f"{balance:>16.2f}{guarend:<20}{guarnam1:<40}{guarnam2:<40}"
    line += f"{issxdte_str:<10}{netproc:>16.2f}{colldesc:<70}{int(collyear):>4d}"
    line += f"{int(bilpaid):>3d}{crrgrade:<5}{marginfi:>16.2f}{int(noteterm):>3d}"
    line += f"{payamt:>16.2f}{dobmni_str:<10}{ecsrind:1}{delqcd:<2}"
    line += f"{occupat:<3}{bgc:<2}{pay75pct:1}{nacodate:<10}{cp:1}"
    line += f"{modeldes:<6}{akpk_status:<9}\n"

    return line

# Additional formats
DELQDES = {
    '01': 'RESIDENTIAL PROPERTY',
    '02': 'NON-RESIDENTIAL PROPERTY',
    '03': 'MOTOR VEHICLE',
    '04': 'OTHERS',
    '  ': 'NOT SPECIFIED'
}

OCCUPFMT = {
    '001': 'PROFESSIONAL',
    '002': 'BUSINESSMAN',
    '003': 'SELF EMPLOYED',
    '004': 'EMPLOYEE - PRIVATE',
    '005': 'EMPLOYEE - GOVERNMENT',
    '006': 'RETIRED',
    '999': 'OTHERS'
}

BGCFMT = {
    'B': 'BUSINESS',
    'G': 'GOVERNMENT',
    'C': 'CORPORATE',
    'I': 'INDIVIDUAL',
    '  ': 'NOT SPECIFIED'
}

def get_delq_desc(delqcd):
    return DELQDES.get(str(delqcd).strip() if delqcd else '  ', 'UNKNOWN')

def get_occup_desc(occupat):
    return OCCUPFMT.get(str(occupat).strip() if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    return BGCFMT.get(str(bgc).strip() if bgc else '  ', 'NOT SPECIFIED')

# ===== MAIN PROCESSING =====
# Calculate report date (yesterday)
reptdate = datetime.now() - timedelta(days=1)

day = reptdate.day
if day <= 7:
    wk = '4'
    wk1 = '3'
elif day <= 14:
    wk = '1'
    wk1 = '4'
elif day <= 21:
    wk = '2'
    wk1 = '1'
elif day <= 28:
    wk = '3'
    wk1 = '2'
else:
    wk = '4'
    wk1 = '3'

mm = reptdate.month
mm1 = mm - 1 if mm > 1 else 12

nowk = wk
nowks = '4'
nowk1 = wk1
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate = reptdate.strftime('%d/%m/%y')

print(f"Processing Bad Debt Write-Off List (Conventional Banking)")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Previous Month: {reptmon1}")

# ===== Columns needed from LNNOTE =====
LNNOTE_COLS_NEEDED = [
    'borstat', 'loantype', 'feedue', 'feeduems', 'feeamt16',
    'name', 'acctno', 'noteno', 'marketvl', 'ntbrch',
    'feetotal', 'nfeeamt5', 'feeamt3', 'feetot2', 'feeamta',
    'feeamt5', 'ecsrrsrv', 'maturedt', 'lasttran',
    'score2', 'contrtype', 'netproc', 'appvalue', 'birthdt',
    'orgbal', 'curbal', 'payamt', 'nacospadt', 'intamt',
    'postntrn', 'intearn4', 'custcode', 'lsttrncd', 'balance',
    'guarend', 'issxdte', 'colldesc', 'collyear', 'noteterm',
    'delqcd', 'cp', 'modeldes', 'akpk_status', 'paidind'
]
# NOTE (FIX): 'days' removed above - it does NOT exist in LNNOTE (confirmed
# against the file's real schema: only DAYARR_MO / DAYARR_MORA exist).
# 'days' is supplied later from CREDMSUBAC ('daysarr' renamed to 'days' in
# Step 3) and merged in at Step 4/9 - it was never meant to come from LNNOTE.

# ===== PERF FIX: read LNNOTE ONCE, not twice =====
# The original script read the (large) lnnote.sas7bdat file a second time in
# "Step 4" just to pull HPD-loantype rows using an overlapping column set.
# That doubles the slowest part of the whole job (disk I/O + SAS decoding)
# for no reason - everything Step 4 needed was already in the Step 1 read.
# We now read once and derive both df_npla and df_loan_raw from one frame.
print("Reading LNNOTE (optimized, single pass)...")
try:
    df_lnnote = read_sas_columns(
        f'{LOAN_DIR}lnnote.sas7bdat',
        LNNOTE_COLS_NEEDED,
        disable_datetime_conversion=True,
        encoding='latin1',
    )
    print(f"Successfully read {len(df_lnnote)} records from LNNOTE")
except Exception as e:
    print(f"pyreadstat-based safe reader failed: {e}")
    print("Trying pandas SAS reader as a last resort...")
    try:
        df_lnnote = pd.read_sas(
            f'{LOAN_DIR}lnnote.sas7bdat',
            format='sas7bdat',
            encoding='latin1'
        )
        df_lnnote.columns = [c.lower() for c in df_lnnote.columns]
        existing_cols = [col for col in LNNOTE_COLS_NEEDED if col in df_lnnote.columns]
        df_lnnote = df_lnnote[existing_cols]
        print(f"Successfully read {len(df_lnnote)} records using pandas")
    except Exception as e2:
        print(f"All readers failed. Error: {e2}")
        sys.exit(1)

# FIX: guard against an empty/malformed read before we ever touch it.
ensure_columns(df_lnnote, ['borstat', 'loantype', 'acctno', 'noteno'], context="LNNOTE")
if len(df_lnnote) == 0:
    print("ERROR: LNNOTE read returned 0 rows. Check the source file/date and "
          "column names before proceeding.")
    sys.exit(1)

# Step 1: Create NPLA - Active accounts with BORSTAT='A'
print("Step 1: Creating NPLA...")
# FIX: boolean-mask filter instead of df.query(...) so a missing/renamed
# column raises a clear KeyError immediately rather than an opaque
# UndefinedVariableError three layers deep in pandas' eval engine.
mask_npla = (
    (df_lnnote['borstat'] == 'A') &
    (~df_lnnote['loantype'].isin([983, 993, 678, 679, 698, 699]))
)
df_npla = df_lnnote[mask_npla].copy()

df_npla['iis'] = 0
df_npla['oi'] = df_npla['feedue'] - df_npla['feeduems']
df_npla['totiis'] = df_npla['oi']
df_npla['sp'] = df_npla['feeduems'] + df_npla['feeamt16']

# PERF FIX: still one .apply() (branch codes have no vectorized string-pad
# equivalent worth the complexity), but this now only runs once total
# instead of running on the whole file twice as when LNNOTE was reloaded.
df_npla['branch'] = df_npla['ntbrch'].apply(
    lambda x: f"{get_branch_name(x)} {x:03d}"
)

df_npla = df_npla[['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']]

# PERF FIX (replaces old Step 4's second disk read of LNNOTE): derive HPD
# loan rows from the SAME in-memory df_lnnote instead of re-reading the
# entire file from disk a second time.
print("Deriving HPD loan rows from the already-loaded LNNOTE frame...")
df_loan_raw = df_lnnote[df_lnnote['loantype'].isin(HPD)].copy()
df_loan_raw = df_loan_raw.drop_duplicates(subset=['acctno', 'noteno'])
print(f"HPD loan records: {len(df_loan_raw)}")

del df_lnnote
gc.collect()

# Step 2: Get IIS and SP data
print("Step 2: Reading IIS and SP data...")
try:
    df_iis = read_sas_columns(
        f'{NPL_DIR}iis.sas7bdat',
        ['acctno', 'noteno', 'iis', 'oi', 'totiis', 'name', 'sp', 'marketvl', 'branch'],
        allow_missing_file=True,
    )
except Exception as e:
    print(f"  Warning: could not read iis.sas7bdat cleanly ({e}); using empty frame.")
    df_iis = pd.DataFrame(columns=['acctno', 'noteno', 'iis', 'oi', 'totiis', 'name', 'sp', 'marketvl', 'branch'])

try:
    df_sp = read_sas_columns(
        f'{NPL_DIR}sp2.sas7bdat',
        ['acctno', 'noteno', 'sp', 'name', 'marketvl', 'branch'],
        allow_missing_file=True,
    )
except Exception as e:
    print(f"  Warning: could not read sp2.sas7bdat cleanly ({e}); using empty frame.")
    df_sp = pd.DataFrame(columns=['acctno', 'noteno', 'sp', 'name', 'marketvl', 'branch'])

df_iis = df_iis.drop_duplicates(subset=['acctno', 'noteno'])
df_sp = df_sp.drop_duplicates(subset=['acctno', 'noteno'])

df_npl_data = df_sp.merge(df_iis, on=['acctno', 'noteno'], how='outer')
cols_needed = ['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']
existing_cols = [col for col in cols_needed if col in df_npl_data.columns]
df_npl_data = df_npl_data[existing_cols]

df_npl = pd.concat([df_npla, df_npl_data], ignore_index=True)
df_npl['marketvl'] = df_npl['marketvl'].round(2)
df_npl['brno'] = df_npl['branch'].str[3:7]
df_npl['brabr'] = df_npl['branch'].str[0:3]
df_npl = df_npl.drop_duplicates(subset=['acctno', 'noteno'])

print(f"NPL records: {len(df_npl)}")

# Step 3: KEY DIFFERENCE - Get CCRIS credit submission data
print("Step 3: Reading CREDMSUBAC...")
credmsubac_file = f'{CCRIS_DIR}credmsubac{reptmon}{reptyear}.sas7bdat'
if os.path.exists(credmsubac_file):
    try:
        df_credsub = read_sas_columns(
            credmsubac_file,
            ['facility', 'acctnum', 'daysarr', 'noteno'],
        )
        df_credsub = df_credsub[
            df_credsub['facility'].astype(str).isin(['34331', '34332'])
        ].rename(columns={'acctnum': 'acctno', 'daysarr': 'days'})

        df_credsub = df_credsub.sort_values(
            ['acctno', 'noteno', 'days'],
            ascending=[True, True, False]
        )
        df_credsub = df_credsub.drop_duplicates(subset=['acctno', 'noteno'])
        df_credsub = df_credsub[['acctno', 'noteno', 'days', 'facility']]
        print(f"CREDMSUBAC records: {len(df_credsub)}")
    except Exception as e:
        print(f"Error reading CREDMSUBAC: {e}")
        df_credsub = pd.DataFrame(columns=['acctno', 'noteno', 'days', 'facility'])
else:
    print(f"Warning: {credmsubac_file} not found.")
    df_credsub = pd.DataFrame(columns=['acctno', 'noteno', 'days', 'facility'])

# Step 4: (PERF FIX) df_loan_raw already derived above from the single
# LNNOTE read - no second disk read needed here anymore.

# Merge NPL, CREDSUB, and LOAN
print("Merging NPL, CREDSUB, and LOAN data...")
df_loan = df_npl.merge(df_credsub, on=['acctno', 'noteno'], how='left')
df_loan = df_loan.merge(df_loan_raw, on=['acctno', 'noteno'], how='left', suffixes=('', '_loan'))
df_loan = df_loan[df_loan['acctno'].notna()]

print(f"Merged loan records: {len(df_loan)}")

# Step 5: Calculate derived fields (vectorized)
print("Step 5: Calculating derived fields (vectorized)...")
for col in ['feetotal', 'nfeeamt5', 'feeamt3', 'feetot2', 'feeamta', 'feeamt5']:
    if col not in df_loan.columns:
        df_loan[col] = 0

df_loan['postamt'] = df_loan['feetotal'].fillna(0) + df_loan['nfeeamt5'].fillna(0)
df_loan['otheramt'] = df_loan['feeamt3'].fillna(0) - df_loan['postamt']
df_loan['oifeeamt'] = df_loan['feetot2'].fillna(0) - df_loan['feeamta'].fillna(0) + df_loan['feeamt5'].fillna(0)
df_loan['ecsrrsrv'] = df_loan.get('ecsrrsrv', 0)
df_loan['ecsrrsrv'] = df_loan['ecsrrsrv'].apply(lambda x: 0 if pd.isna(x) or x <= 0 else x)

# Date formatting
df_loan['matdate'] = df_loan['maturedt'].apply(lambda x: safe_format_date(x, format_mmddyy10)) if 'maturedt' in df_loan.columns else ''
df_loan['lasttra1'] = df_loan['lasttran'].apply(lambda x: safe_format_date(x, format_mmddyy10)) if 'lasttran' in df_loan.columns else ''

# Months past due
df_loan['days'] = df_loan['days'].fillna(0).astype(int) if 'days' in df_loan.columns else 0
df_loan['mthpdue'] = df_loan['days'].apply(mthpass_format)
mask = df_loan['mthpdue'] == 24
df_loan.loc[mask, 'mthpdue'] = (df_loan.loc[mask, 'days'] / 365 * 12).astype(int)

# Credit grade
df_loan['score2'] = df_loan.get('score2', '').fillna('').astype(str) if 'score2' in df_loan.columns else ''
df_loan['contrtype'] = df_loan.get('contrtype', '').fillna('').astype(str) if 'contrtype' in df_loan.columns else ''
df_loan['crrgrade'] = (df_loan['score2'].astype(str) + df_loan['contrtype'].astype(str)).str.strip()

# Margin of financing
df_loan['netproc'] = df_loan['netproc'].fillna(0) if 'netproc' in df_loan.columns else 0
df_loan['appvalue'] = df_loan['appvalue'].fillna(0) if 'appvalue' in df_loan.columns else 0
df_loan['marginfi'] = np.where(
    df_loan['appvalue'] > 0,
    (df_loan['netproc'] / df_loan['appvalue']).round(2),
    0
)

# Date of birth
df_loan['dobmni'] = df_loan['birthdt'].apply(safe_get_date) if 'birthdt' in df_loan.columns else None

# ECSR indicator
df_loan['ecsrind'] = np.where(df_loan['ecsrrsrv'] > 0, 'Y', 'N')

# Bills paid
df_loan['orgbal'] = df_loan.get('orgbal', 0)
df_loan['orgbal'] = df_loan['orgbal'].fillna(0) if 'orgbal' in df_loan.columns else 0
df_loan['curbal'] = df_loan['curbal'].fillna(0) if 'curbal' in df_loan.columns else 0
df_loan['payamt'] = df_loan['payamt'].fillna(0) if 'payamt' in df_loan.columns else 0
df_loan['bilpaid'] = np.where(
    df_loan['payamt'] > 0,
    ((df_loan['orgbal'] - df_loan['curbal']) / df_loan['payamt']).astype(int),
    0
)

# NACO special attention
df_loan['nacospadt'] = df_loan.get('nacospadt', 0)
df_loan['pay75pct'] = np.where(df_loan['nacospadt'].fillna(0) > 0, 'Y', 'N')
df_loan['nacodate'] = df_loan['nacospadt'].apply(lambda x: safe_format_date(x, format_mmddyy10))

print("Derived fields calculated")

# Step 6: Get customer names
print("Step 6: Reading customer names...")
try:
    df_cname = read_sas_columns(
        f'{CISNAME_DIR}loan.sas7bdat',
        ['acctno', 'custnam1', 'occupat', 'bgc', 'seccust'],
    )
    df_cname = df_cname[df_cname['seccust'].astype(str) == '901']
    df_cname = df_cname[['acctno', 'custnam1', 'occupat', 'bgc']].drop_duplicates(subset=['acctno'])
    print(f"Customer records: {len(df_cname)}")
except Exception as e:
    print(f"Error reading customer names: {e}")
    df_cname = pd.DataFrame(columns=['acctno', 'custnam1', 'occupat', 'bgc'])

# Step 7: Get guarantors
print("Step 7: Reading liability data...")
try:
    df_liab = read_sas_columns(
        f'{LOAN_DIR}liab.sas7bdat',
        ['acctno', 'noteno', 'liabacct', 'liabname'],
    )
    df_liab = df_liab.sort_values('liabacct')

    df_liab = df_liab.merge(
        df_cname.rename(columns={'acctno': 'liabacct', 'custnam1': 'gname'}),
        on='liabacct',
        how='left'
    )

    df_liab['gname'] = df_liab['gname'].fillna(df_liab['liabname'])
    df_liab = df_liab.sort_values(['acctno', 'noteno'])

    guarantor_data = {}
    for (acctno, noteno), group in df_liab.groupby(['acctno', 'noteno']):
        gnames = group['gname'].tolist()
        guarantor_data[(acctno, noteno)] = {
            'guarnam1': gnames[0] if len(gnames) > 0 else '',
            'guarnam2': gnames[1] if len(gnames) > 1 else ''
        }
    print(f"Guarantor records processed: {len(guarantor_data)}")
except Exception as e:
    print(f"Error reading liability data: {e}")
    guarantor_data = {}
    df_liab = pd.DataFrame()

# Step 8: Get previous month balance
print("Step 8: Reading previous month balance...")
sasln_file = f'{SASLN_DIR}loan{reptmon1}{nowks}.sas7bdat'
try:
    df_sasln = read_sas_columns(
        sasln_file,
        ['acctno', 'noteno', 'curbal'],
        allow_missing_file=True,
    )
    df_sasln = df_sasln.rename(columns={'curbal': 'prevbal'})
    df_sasln = df_sasln.sort_values(['acctno', 'noteno'])
    print(f"Previous balance records: {len(df_sasln)}")
except Exception as e:
    print(f"Error reading {sasln_file}: {e}")
    df_sasln = pd.DataFrame(columns=['acctno', 'noteno', 'prevbal'])

# Merge with NPL to get only relevant accounts
df_sasln = df_sasln.merge(df_npl[['acctno', 'noteno']], on=['acctno', 'noteno'], how='inner')

# PERF FIX: the original did two row-by-row .apply(axis=1) dict lookups
# per row (effectively a per-row Python-level join). For any sizeable
# account base this is dramatically slower than pandas' native merge.
# Build a small DataFrame from the guarantor_data dict once, then merge -
# same result, vectorized.
if guarantor_data:
    df_guar = pd.DataFrame(
        [
            {'acctno': acctno, 'noteno': noteno, **names}
            for (acctno, noteno), names in guarantor_data.items()
        ]
    )
else:
    df_guar = pd.DataFrame(columns=['acctno', 'noteno', 'guarnam1', 'guarnam2'])

df_sasln = df_sasln.merge(df_guar, on=['acctno', 'noteno'], how='left')
df_sasln['guarnam1'] = df_sasln['guarnam1'].fillna('')
df_sasln['guarnam2'] = df_sasln['guarnam2'].fillna('')

# Step 9: Merge all data
print("Step 9: Merging all data...")
df_woff = df_sasln.merge(df_loan, on=['acctno', 'noteno'], how='outer')
df_woff = df_woff.merge(df_npl, on='acctno', how='outer', suffixes=('', '_npl'))

df_woff['payment'] = df_woff['curbal'].fillna(0) - df_woff['prevbal'].fillna(0)
df_woff['total'] = df_woff['totiis'].fillna(0) + df_woff['sp'].fillna(0)
df_woff['rind'] = 'D'

gc.collect()

# Step 10: Filter for write-off candidates
print("Step 10: Filtering write-off candidates...")
# FIX: ensure required columns exist before boolean filtering (clear error
# instead of a silent all-NaN mask if a merge didn't produce a column).
ensure_columns(df_woff, ['borstat', 'days', 'loantype', 'paidind', 'total'], context="df_woff filter step")

df_woff = df_woff[
    (
        ((df_woff['borstat'].isin(['F', 'I'])) & (df_woff['days'] >= 334)) |
        (df_woff['days'] >= 334) |
        (
            (df_woff['borstat'] == 'A') &
            (~df_woff['loantype'].isin([983, 993, 678, 679, 698, 699])) &
            (df_woff['paidind'] != 'P')
        )
    ) &
    (df_woff['total'] != 0)
]

df_woff['confirm'] = 'Y'
df_woff = df_woff.sort_values('acctno')

df_woff = df_woff.merge(
    df_cname.rename(columns={'custnam1': 'name'}),
    on='acctno',
    how='left',
    suffixes=('', '_cname')
)

print(f"Write-off candidates: {len(df_woff)}")

# Save to parquet
os.makedirs(os.path.dirname(f'{NPL_DIR}list.parquet'), exist_ok=True)
df_woff.to_parquet(f'{NPL_DIR}list.parquet', index=False)

print(f"\nBad Debt Write-Off List (Conventional) Generation Complete")

# Step 11: Write fixed-width output file
print("Step 11: Writing fixed-width output file...")
os.makedirs(os.path.dirname(OUTPUT_FILE1), exist_ok=True)

# PERF FIX: df.iterrows() reconstructs a pandas Series (with dtype
# alignment/boxing overhead) for every single row, which is one of the
# classic pandas performance traps on large frames. create_fixed_width_line()
# only ever does dict-style `.get(...)` lookups, so a plain list-of-dicts
# via to_dict('records') is a drop-in replacement that's substantially
# faster (no Series construction per row) while keeping identical output.
with open(OUTPUT_FILE1, 'w', buffering=8192*1024) as f:
    lines = []
    for row in df_woff.to_dict('records'):
        lines.append(create_fixed_width_line(row))

        if len(lines) >= 1000:
            f.writelines(lines)
            lines = []

    if lines:
        f.writelines(lines)

# Step 12-14: Read, recalculate, write final output
print("Step 12-14: Writing final formatted output...")
with open(OUTPUT_FILE1, 'r', buffering=8192*1024) as f_in, \
     open(OUTPUT_FILE, 'w', buffering=8192*1024) as f_out:

    for line in f_in:
        totiis = float(line[100:116]) if line[100:116].strip() else 0
        balance = float(line[356:372]) if line[356:372].strip() else 0
        oi = float(line[84:100]) if line[84:100].strip() else 0

        sp_calc = balance - totiis
        total_calc = totiis + sp_calc

        delqcd = line[676:678]
        occupat = line[712:715]
        bgc = line[742:744]

        delqdes = get_delq_desc(delqcd)
        occupdes = get_occup_desc(occupat)
        bgcdes = get_bgc_desc(bgc)

        biztype = 'C'
        cap = 0.0
        latechg = oi

        f_out.write(line[:116])
        f_out.write(f"{sp_calc:>16.2f}")
        f_out.write(f"{total_calc:>16.2f}")
        f_out.write(line[148:373])
        f_out.write(f"{cap:>16.2f}")
        f_out.write(f"{latechg:>16.2f}")
        f_out.write(line[407:679])
        f_out.write(f"{delqdes:<30}")
        f_out.write(f"{biztype:1}")
        f_out.write(line[712:715])
        f_out.write(f"{occupdes:<25}")
        f_out.write(line[742:744])
        f_out.write(f"{bgcdes:<20}")
        f_out.write(line[766:])

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
print(f"  {NPL_DIR}list.parquet (Data file)")
print(f"\nAccounts identified for write-off: {len(df_woff)}")
if len(df_woff) > 0:
    print(f"Total exposure: RM {df_woff['total'].sum():,.2f}")
print(f"\nKey Differences from EIIFTXT1 (Islamic):")
print(f"  - RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)")
print(f"  - BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)")
print(f"  - Uses CREDMSUBAC vs ICREDMSUBAC (CCRIS)")
