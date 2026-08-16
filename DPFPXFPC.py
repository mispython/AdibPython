"""
EIBDLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Conventional Banking
Consolidates deposits & treasury positions for BNM LCR reporting.
Includes DCI (Dual Currency Investments) and full treasury processing.
Outputs: LCR reports with customer categorization (08/19/29/39/49/59)

=====================================================================
FIXES APPLIED (see chat explanation):
1. All SAS/parquet dataframes are normalized to LOWERCASE column names
   immediately after reading. pyreadstat/parquet preserve the original
   SAS variable names (GWCCY, UTMAT, AMOUNT, ACCTCODE, ...) in
   UPPERCASE, but the processing code looks up lowercase keys/does
   case-sensitive 'x in df.columns' checks. That silently defaulted
   almost every field to '' / 0 / None instead of erroring -> zero
   item assignments, zero amounts, and rows getting dropped because
   date fields were always None.
2. read_walk_file() rewritten to match the ACTUAL fixed-width layout
   used in the SAS code (%INC ... DATA LCR.GL...): SET_ID at col 2
   (19 chars), AMOUNT at col 42 (COMMA20.2), SIGN at col 62 (1 char).
   The previous version assumed an acctno/custno layout that doesn't
   exist in this file, which is why it crashed with
   "invalid literal for int() with base 10: '1F144611FXS'".
3. process_cis_equity() no longer crashes (fixed by #1) and now casts
   PRISEC defensively in case it's read as float.
4. Added full (non-truncated) column dumps for K1TBL/K3TBL/DCI so you
   can immediately confirm real field names if anything is still off
   (e.g. verify INVCURR actually exists in the DCI file - only the
   first 10 of 33 columns were visible in your log).
5. Added an explicit "missing expected columns" warning in the
   critical parsers so future column-name mismatches show up loudly
   instead of silently zeroing out.
=====================================================================
"""

import polars as pl
import pyreadstat
from datetime import datetime, date, timedelta
import os
from pathlib import Path
import calendar
import glob

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'lcr': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/lcr/',
    'lcrm': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/lcrm/',
    'forate': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/',
    'cisdp': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/',
    'cisca': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisca/',
    'cis': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/',
    'dciwh': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/dciwh/',
    'equa': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/equa/',
    'bnmk': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/',
    'list': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/list/',
    'output': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDLCRM/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

inst = 'PBB'  # Institution code

# Customer category mappings (LCR)
cust_map = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Other retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],  # Financial institutions
    '59': [4,5,6,13,20] + list(range(30,41)) + [17]  # Corporate
}

# Special customers
special_cust = {
    '39': ['kwsp', 'kwap', 'kwan', 'lemtab'],
    '49': ['aim', 'pbl', 'pbleur', 'pblnid', 'pblusd', 'pivmyr', 'ipbb']
}

# Hardcoded FX rates (replaces FOFMT)
FX_RATES = {
    'MYR': 1.0,
    'USD': 4.0,
    'SGD': 3.0,
    'HKD': 0.5,
    'AUD': 3.0,
    'JPY': 0.03,
    'XAU': 200.0,
    'GBP': 5.0,
    'EUR': 4.5,
    'CNY': 0.6
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Get report date as yesterday's date"""
    reptdate = date.today() - timedelta(days=16)

    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'

    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if reptdate.year % 4 == 0:
        days_in_month[1] = 29

    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'rdate': reptdate.strftime('%d%m%y'),
        'rptdt': reptdate.strftime('%y%m%d'),
        'year': reptdate.year,
        'month': reptdate.month,
        'day_of_month': day,
        'days_in_month': days_in_month,
        'days_in_cur_month': days_in_month[reptdate.month - 1]
    }


SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_pydate(val):
    """
    FIX: pyreadstat is returning several date columns (K1TBL's GWMDT/GWSDT,
    K3TBL's MATDT, DCI's MATDT/STARTDT) as raw floats (SAS date serials -
    days since 1960-01-01) rather than converting them to python date
    objects. Comparing/subtracting those floats against a python date
    throws TypeError ("'>' not supported between instances of 'float'
    and 'datetime.date'" / similar). This converts defensively regardless
    of whether pyreadstat already converted it or not.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, (int, float)):
        try:
            return SAS_EPOCH + timedelta(days=int(val))
        except (ValueError, OverflowError):
            return None
    return None


def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    FIX #1: SAS variable names come back UPPERCASE from pyreadstat (and the
    parquet extract preserves whatever case it was written with, which is
    also uppercase here). All downstream code was written assuming
    lowercase column names, so without this normalization every
    'col_name' in df.columns / row.get('col_name') check silently failed
    and returned default values (0, '', None) instead of the real data.
    """
    if df is None:
        return df
    rename_map = {c: c.lower() for c in df.columns if c != c.lower()}
    if rename_map:
        df = df.rename(rename_map)
    return df


def warn_missing_columns(df, expected, context):
    """Loudly flag expected fields that are missing after normalization,
    instead of letting them silently default to 0/''/None."""
    if df is None:
        return
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"    !! WARNING [{context}]: expected columns not found after "
              f"normalization: {missing}. These will default to 0/''/None "
              f"and likely cause dropped/zeroed records. Check real column "
              f"names below.")


def read_sas_file(filepath, columns=None):
    """Read SAS dataset using pyreadstat and return polars DataFrame
    with columns normalized to lowercase (FIX #1)."""
    try:
        if columns:
            # columns filter must match the file's real (uppercase) names
            usecols = [c.upper() for c in columns]
            df, meta = pyreadstat.read_sas7bdat(filepath, usecols=usecols)
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"    Successfully read: {os.path.basename(filepath)} ({len(df)} rows, {len(df.columns)} columns)")
        pdf = pl.from_pandas(df)
        pdf = _normalize_columns(pdf)
        return pdf
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
        return None


def read_parquet_file(filepath):
    """Read parquet file and return polars DataFrame with columns
    normalized to lowercase (FIX #1)."""
    try:
        df = pl.read_parquet(filepath)
        print(f"    Successfully read: {os.path.basename(filepath)} ({len(df)} rows, {len(df.columns)} columns)")
        df = _normalize_columns(df)
        return df
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
        return None


def read_walk_file(filepath):
    """
    FIX #2: Read WALK.TXT fixed-width file.

    The original SAS layout (see EIBDLCRM.sas):
        INFILE WALK;
        INPUT @002 SET_ID  $19.
              @042 AMOUNT  COMMA20.2
              @062 SIGN    $1.
        IF SIGN = '' THEN AMOUNT = -1*AMOUNT;
        ITEM = PUT(SET_ID,$LCRCDGL.);

    SAS column positions are 1-indexed; @002 means "start at column 2"
    which is index 1 in a 0-indexed python string.
    The old implementation assumed a completely different acctno/custno
    layout that doesn't match this file, which is why it blew up trying
    to int() a value like '1F144611FXS' (that's actually a SET_ID).

    NOTE: ITEM = PUT(SET_ID,$LCRCDGL.) applies a SAS format (a
    lookup table named LCRCDGL) that maps SET_ID -> report ITEM code.
    That format table isn't available here, so `item` is left as None;
    if you have the LCRCDGL format's mapping (fmtname/start/label from
    PROC FORMAT), plug it into ITEM_LOOKUP below and this will produce
    real ITEM values.
    """
    records = []
    ITEM_LOOKUP = {}  # TODO: populate from $LCRCDGL format if available

    try:
        with open(filepath, 'r', errors='replace') as f:
            for line in f:
                if len(line) < 62:
                    continue
                set_id = line[1:20].strip()          # @002, $19.
                amount_raw = line[41:61].strip()      # @042, COMMA20.2
                sign = line[61:62].strip()            # @062, $1.

                if not set_id:
                    continue

                try:
                    amount = float(amount_raw.replace(',', '')) if amount_raw else 0.0
                except ValueError:
                    amount = 0.0

                if sign == '':
                    amount = -1 * amount

                item = ITEM_LOOKUP.get(set_id, '')

                records.append({
                    'set_id': set_id,
                    'amount': amount,
                    'sign': sign,
                    'item': item
                })
        print(f"    Read {len(records)} records from {os.path.basename(filepath)}")
        if not ITEM_LOOKUP:
            print("    NOTE: LCRCDGL format lookup not populated - 'item' will be blank "
                  "for all WALK records until ITEM_LOOKUP is filled in.")
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
    return records


def read_templ_file(filepath):
    """Read TEMPL.TXT file (fixed width format)"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 14:
                    records.append({
                        'tag': line[0:2].strip(),
                        'desc': line[2:14].strip()
                    })
        print(f"    Read {len(records)} records from {os.path.basename(filepath)}")
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
    return records


def get_customer_category(code, mapping, special=None, is_custno=False):
    """Get customer category from code"""
    if is_custno and special and code in special:
        return next((cat for cat, vals in special.items() if code in vals), '29')

    for cat, codes in mapping.items():
        if code in codes:
            return cat
    return '29'


def calculate_remaining_months(matdt, reptdate, days_in_month):
    """Calculate REMMTH and REM30D (equivalent to %REMMTH macro)"""
    if matdt <= reptdate:
        return 0.1, 0

    rp_year = reptdate.year
    rp_month = reptdate.month
    rp_day = reptdate.day

    md_year = matdt.year
    md_month = matdt.month
    md_day = matdt.day

    days_in_target_month = days_in_month[md_month - 1]
    if md_day > days_in_target_month:
        md_day = days_in_target_month

    rem_years = md_year - rp_year
    rem_months = md_month - rp_month
    rem_days = md_day - rp_day

    remmth = rem_years * 12 + rem_months + rem_days / days_in_month[rp_month - 1]
    rem30d = (matdt - reptdate).days / 30

    return remmth, rem30d


def format_mth_bucket(months):
    """Format months into bucket (01-10)"""
    if months <= 1: return '01'
    if months <= 3: return '02'
    if months <= 6: return '03'
    if months <= 9: return '04'
    if months <= 12: return '05'
    if months <= 24: return '06'
    if months <= 36: return '07'
    if months <= 60: return '08'
    if months <= 120: return '09'
    return '10'


def format_day_bucket(days):
    """Format days into bucket (01=<=30, 02=>30)"""
    return '01' if days <= 1 else '02'


def debug_directory(path, pattern=None, max_files=50):
    """Debug function to list files in a directory"""
    print(f"  Debug: Directory: {path}")

    if not os.path.exists(path):
        print(f"    Directory does not exist!")
        return []

    try:
        all_files = os.listdir(path)
        print(f"    Total files: {len(all_files)}")

        if pattern:
            filtered = [f for f in all_files if pattern.lower() in f.lower()]
            print(f"    Files matching '{pattern}': {len(filtered)}")
            for f in filtered[:max_files]:
                print(f"      - {f}")
            return filtered
        else:
            print(f"    First {min(max_files, len(all_files))} files:")
            for f in sorted(all_files)[:max_files]:
                print(f"      - {f}")
            return all_files
    except Exception as e:
        print(f"    Error listing directory: {e}")
        return []

# =============================================================================
# KALMLIQ LOGIC - Process K1TBL and K3TBL from BNMK source
# =============================================================================
def find_k1tbl_file(rep_date):
    """Find K1TBL file with debugging"""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']

    print(f"  Looking for K1TBL file...")
    print(f"    Base path: {base_path}")
    print(f"    Month: {month}, Week: {week}")

    if not os.path.exists(base_path):
        print(f"    ERROR: Directory does not exist: {base_path}")
        return None

    possible_names = [
        f"k1tbl{month}{week}.sas7bdat",
        f"K1TBL{month}{week}.sas7bdat",
        f"k1tbl{month}0{week}.sas7bdat",
        f"K1TBL{month}0{week}.sas7bdat",
        f"k1tbl{month}.sas7bdat",
        f"K1TBL{month}.sas7bdat",
    ]

    print(f"    Looking for exact matches:")
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        exists = os.path.exists(full_path)
        print(f"      {name}: {'✓ Found' if exists else '✗ Not found'}")
        if exists:
            return full_path

    print(f"    Searching with wildcards:")
    wildcards = [
        f"*k1tbl*{month}*.sas7bdat",
        f"*K1TBL*{month}*.sas7bdat",
        f"*k1tbl*.sas7bdat",
        f"*K1TBL*.sas7bdat",
    ]

    for wildcard in wildcards:
        pattern = os.path.join(base_path, wildcard)
        matches = glob.glob(pattern)
        if matches:
            print(f"      {wildcard}: Found {len(matches)} file(s)")
            for m in matches[:5]:
                print(f"        - {os.path.basename(m)}")
            return matches[0]

    print(f"    No K1TBL files found. Listing directory contents:")
    debug_directory(base_path, pattern="k1")

    return None


def find_k3tbl_file(rep_date):
    """Find K3TBL file with debugging"""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']

    print(f"  Looking for K3TBL file...")
    print(f"    Base path: {base_path}")
    print(f"    Month: {month}, Week: {week}")

    if not os.path.exists(base_path):
        print(f"    ERROR: Directory does not exist: {base_path}")
        return None

    possible_names = [
        f"k3tbl{month}{week}.sas7bdat",
        f"K3TBL{month}{week}.sas7bdat",
        f"k3tbl{month}0{week}.sas7bdat",
        f"K3TBL{month}0{week}.sas7bdat",
        f"k3tbl{month}.sas7bdat",
        f"K3TBL{month}.sas7bdat",
    ]

    print(f"    Looking for exact matches:")
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        exists = os.path.exists(full_path)
        print(f"      {name}: {'✓ Found' if exists else '✗ Not found'}")
        if exists:
            return full_path

    print(f"    Searching with wildcards:")
    wildcards = [
        f"*k3tbl*{month}*.sas7bdat",
        f"*K3TBL*{month}*.sas7bdat",
        f"*k3tbl*.sas7bdat",
        f"*K3TBL*.sas7bdat",
    ]

    for wildcard in wildcards:
        pattern = os.path.join(base_path, wildcard)
        matches = glob.glob(pattern)
        if matches:
            print(f"      {wildcard}: Found {len(matches)} file(s)")
            for m in matches[:5]:
                print(f"        - {os.path.basename(m)}")
            return matches[0]

    print(f"    No K3TBL files found. Listing directory contents:")
    debug_directory(base_path, pattern="k3")

    return None


def process_k1tbl(rep_date):
    """Process K1TBL from BNMK.K1TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k1_filepath = find_k1tbl_file(rep_date)

        if k1_filepath is None:
            print(f"  No K1TBL file found")
            return records

        print(f"  Using K1TBL file: {k1_filepath}")
        df = read_sas_file(k1_filepath)  # columns now normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K1TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['gwmvt', 'gwccy', 'gwocy', 'gwmpts', 'gwctp', 'gwdlp', 'gwmdt',
             'gwsdt', 'gwbalc', 'gwshn', 'gwc2r', 'gwdlr'],
            'K1TBL'
        )

        # FIX: 'gwmpts' and 'gwhsn' do not exist in this table's real
        # columns. 'gwhsn' is confidently 'gwshn' (confirmed - the SAS
        # ALLEQU step references GWSHN for CUSTNAME fallback). 'gwmpts'
        # has no confirmed equivalent; 'gwmvts' is the closest name match
        # (GWMVT / GWMVTS pairing) and is used as a best-effort fallback.
        # *** VERIFY THIS AGAINST THE REAL KALMLIQ SAS SOURCE IF YOU HAVE
        # ACCESS TO IT *** - we've only ever seen the wrapper that calls
        # %INC PGM(KALMLIQ), never the include itself, so this mapping
        # is inferred, not confirmed.
        gwmpts_col = 'gwmpts' if 'gwmpts' in df.columns else ('gwmvts' if 'gwmvts' in df.columns else None)
        gwhsn_col = 'gwhsn' if 'gwhsn' in df.columns else ('gwshn' if 'gwshn' in df.columns else None)
        if gwmpts_col == 'gwmvts':
            print("    NOTE: 'gwmpts' not found - falling back to 'gwmvts' as a best-effort "
                  "guess. Please verify this against the real KALMLIQ source if possible.")
        if gwhsn_col == 'gwshn':
            print("    NOTE: 'gwhsn' not found - using 'gwshn' instead (confirmed field).")

        gwmvt_col = 'gwmvt' if 'gwmvt' in df.columns else None
        if gwmvt_col is None:
            print(f"    Column 'gwmvt' not found! Available columns: {df.columns}")
            return records

        unique_gwmvt = df[gwmvt_col].unique().to_list()
        print(f"    Unique values in GWMVT: {unique_gwmvt}")

        gwmvt_values = df[gwmvt_col].to_list()
        p_count = sum(1 for v in gwmvt_values if str(v).upper() == 'P')
        print(f"    Rows with GWMVT = 'P': {p_count}")

        if p_count == 0:
            print(f"    No rows with GWMVT = 'P'. Sample values: {gwmvt_values[:10]}")
            return records

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in ['gwmvt', 'gwccy', 'gwocy', 'gwmpts', 'gwctp', 'gwdlp', 'gwmdt', 'gwbalc']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        filtered_out = 0
        gwmvt_p = 0
        excluded_currency = 0
        item_assigned = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            gwmvt = str(row.get(gwmvt_col, '')).upper()
            gwccy = str(row.get('gwccy', '')).upper() if 'gwccy' in df.columns else ''
            gwocy = str(row.get('gwocy', '')).upper() if 'gwocy' in df.columns else ''
            gwmpts = str(row.get(gwmpts_col, '') or '').upper() if gwmpts_col else ''
            gwctp = str(row.get('gwctp', '') or '').upper() if 'gwctp' in df.columns else ''
            gwdlp = str(row.get('gwdlp', '') or '').upper() if 'gwdlp' in df.columns else ''

            if gwmvt != 'P':
                filtered_out += 1
                continue

            gwmvt_p += 1

            if gwocy in ['XAU', 'XAT'] or gwccy in ['XAU', 'XAT']:
                excluded_currency += 1
                continue

            # FIX: SAS date serials (floats) -> python date
            matdt = sas_date_to_pydate(row.get('gwmdt')) if 'gwmdt' in df.columns else None
            issdt = sas_date_to_pydate(row.get('gwsdt')) if 'gwsdt' in df.columns else None
            # FIX: null-safe - real data has None in some rows (e.g. the blank header row)
            amount = (row.get('gwbalc', 0) or 0) if 'gwbalc' in df.columns else 0
            gwhsn = (row.get(gwhsn_col, '') or '') if gwhsn_col else ''
            gwc2r = (row.get('gwc2r', 0) or 0) if 'gwc2r' in df.columns else 0
            gwdlr = (row.get('gwdlr', '') or '') if 'gwdlr' in df.columns else ''

            if gwccy == 'MYR':
                part = '95'
                amtusd = 0
                amtsgd = 0

                if gwmpts == 'M':
                    if gwdlp in ['BCD', 'BCI', 'BCS', 'BCQ', 'BCT', 'BCW', 'BQD']:
                        item = '830'
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })

                    if gwctp and gwctp[0] == 'B':
                        if gwdlp in ['LO', 'LC', 'LF', 'LS', 'LOI', 'LSI', 'LSC', 'LSW',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOC', 'LOW']:
                            item = '610'
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BO', 'BF', 'BOI', 'BFI', 'BSC', 'BSW', 'BOC', 'BOW']:
                            item = '810'
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })

                    if len(gwdlp) >= 2 and gwdlp[1:3] in ['MI', 'MT']:
                        item = '820'
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                    if len(gwdlp) >= 2 and gwdlp[1:3] in ['XI', 'XT']:
                        item = '620'
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
            else:
                part = '96'
                amtusd = amount if gwccy == 'USD' else 0
                amtsgd = amount if gwccy == 'SGD' else 0

                if gwmpts == 'M':
                    if gwctp and gwctp[0] == 'B' and gwctp != 'BW':
                        if gwdlp in ['LO', 'LC', 'LS', 'LF', 'LOI', 'LSI', 'LSC', 'LOC',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOW', 'LSW']:
                            item = '610'
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BC', 'BF', 'BO', 'BSC', 'BOW', 'BSW']:
                            if gwhsn[:6] != 'FCY-FD':
                                item = '810'
                                item_assigned += 1
                                records.append({
                                    'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                    'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                    'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                                })
                        elif gwdlp == 'BOC':
                            item = '810'
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': item, 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })

        print(f"  K1TBL processing stats:")
        print(f"    Total rows: {total_rows}")
        print(f"    Filtered out (GWMVT != 'P'): {filtered_out}")
        print(f"    Passed GWMVT = 'P': {gwmvt_p}")
        print(f"    Excluded (XAU/XAT currency): {excluded_currency}")
        print(f"    Records with item assigned: {item_assigned}")

    except Exception as e:
        print(f"  K1TBL warning: {e}")
        import traceback
        traceback.print_exc()

    return records


def process_k3tbl(rep_date):
    """Process K3TBL from BNMK.K3TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k3_filepath = find_k3tbl_file(rep_date)

        if k3_filepath is None:
            print(f"  No K3TBL file found")
            return records

        print(f"  Using K3TBL file: {k3_filepath}")
        df = read_sas_file(k3_filepath)  # columns now normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K3TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['utref', 'utsty', 'utdlp', 'utcus', 'utclc', 'utctp', 'matdt',
             'utamoc', 'utdpf', 'utccy', 'utdlr', 'utaict', 'utpcp', 'utdpey',
             'utdpe', 'utaicy', 'utait', 'utmm1'],
            'K3TBL'
        )

        # FIX: the maturity date column is just 'matdt', not 'utmat'.
        # 'utmm1' genuinely does not exist in this table - there's no
        # confirmed substitute among the real columns, so it stays blank
        # (only affects the ISB/IDS/IBZ/ICN branch's item='636' vs '635'
        # split under the 'IINV'/'IDRI'/'IDLG' utref case).
        matdt_col = 'matdt' if 'matdt' in df.columns else None
        if matdt_col is None:
            print("    !! WARNING [K3TBL]: no maturity date column found - "
                  "all K3TBL records will be dropped in build_ktblall().")

        utref_col = 'utref' if 'utref' in df.columns else None
        if utref_col:
            unique_utref = df[utref_col].unique().to_list()
            print(f"    Unique values in UTREF: {unique_utref[:20]}")
        else:
            print(f"    Column 'utref' not found!")

        utsty_col = 'utsty' if 'utsty' in df.columns else None
        if utsty_col:
            unique_utsty = df[utsty_col].unique().to_list()
            print(f"    Unique values in UTSTY: {unique_utsty[:20]}")

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in ['utref', 'utsty', 'utdlp', 'utcus', 'utctp', 'utmat', 'utamoc', 'utdpf']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        utref_match = 0
        item_assigned = 0
        matdt_missing = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            # FIX: null-safe - some rows (e.g. the leading blank/header
            # row) have None for these numeric fields, and None-None
            # raises TypeError.
            utamoc = (row.get('utamoc', 0) or 0) if 'utamoc' in df.columns else 0
            utdpf = (row.get('utdpf', 0) or 0) if 'utdpf' in df.columns else 0
            amount = utamoc - utdpf

            utsty = str(row.get(utsty_col, '') or '').upper() if utsty_col else ''
            if utsty == 'IDC':
                amount = utamoc + utdpf

            utccy = str(row.get('utccy', 'MYR') or 'MYR').upper() if 'utccy' in df.columns else 'MYR'
            utcus = row.get('utcus', '') if 'utcus' in df.columns else ''
            utctp = row.get('utctp', 0) if 'utctp' in df.columns else 0
            utdlr = (row.get('utdlr', '') or '') if 'utdlr' in df.columns else ''
            utdlp = str(row.get('utdlp', '') or '').upper() if 'utdlp' in df.columns else ''
            utref = str(row.get(utref_col, '') or '').upper() if utref_col else ''
            utaict = (row.get('utaict', 0) or 0) if 'utaict' in df.columns else 0
            utpcp = (row.get('utpcp', 0) or 0) if 'utpcp' in df.columns else 0
            utdpey = (row.get('utdpey', 0) or 0) if 'utdpey' in df.columns else 0
            utdpe = (row.get('utdpe', 0) or 0) if 'utdpe' in df.columns else 0
            utaicy = (row.get('utaicy', 0) or 0) if 'utaicy' in df.columns else 0
            utait = (row.get('utait', 0) or 0) if 'utait' in df.columns else 0
            utmm1 = str(row.get('utmm1', '') or '').upper() if 'utmm1' in df.columns else ''
            # FIX: SAS date serial -> python date
            matdt = sas_date_to_pydate(row.get(matdt_col)) if matdt_col else None
            if matdt is None:
                matdt_missing += 1

            part = '95'
            amtusd = amount if utccy == 'USD' else 0
            amtsgd = amount if utccy == 'SGD' else 0

            # Process based on UTREF
            if utref in ['INV', 'DRI', 'DLG', 'AFSLIQ', 'AFSBOND', 'IAFSLIQ', 'AFS', 'IAFS']:
                utref_match += 1
                if utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGS', 'MTB', 'BNB', 'BNN',
                            'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM',
                            'CMB', 'MGI', 'SMC']:
                    item = '631'
                    if inst == 'PBB':
                        amount = amount + utaict
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['SDC', 'LDC', 'SLD', 'SSD', 'SFD', 'SZD']:
                    item = '632'
                    if utsty == 'SDC' and inst == 'PBB':
                        amount = (utamoc * (utpcp / 100)) + utdpey + utdpe
                    elif utsty in ['SLD', 'SSD'] and inst == 'PBB':
                        amount = (utamoc * (utpcp / 100)) + utaicy + utait
                    elif inst == 'PBB':
                        amount = amount + utaict
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty == 'SBA' and utdlp not in ['MOS', 'MSS']:
                    item = '633'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['ISB', 'DHB', 'KHA', 'PNB']:
                    item = '636'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['IDS', 'DMB', 'DBD', 'GRL', 'MTL', 'RUL']:
                    item = '635' if utsty != 'DBD' else '634'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty == 'PBA' and utdlp in ['MOS', 'MSS']:
                    item = '850'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })

            elif utref in ['PFD', 'PLD', 'PSD', 'PZD', 'PDC']:
                utref_match += 1
                if utsty in ['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']:
                    item = '840'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })

            elif utref in ['IINV', 'IDRI', 'IDLG']:
                utref_match += 1
                if utsty == 'SBA' and utdlp == 'IOP':
                    item = '633'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['SDC', 'LDC']:
                    item = '632'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGI',
                               'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM',
                               'MGS', 'MTB', 'BNB', 'BNN', 'CMB', 'SMC']:
                    item = '631'
                    if inst == 'PBB':
                        amount = amount + utaict
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['ISB', 'IDS', 'IBZ', 'ICN']:
                    item = '636' if utmm1 == 'GGB' else '635'
                    amount = amount + utaict
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty in ['DHB', 'KHA']:
                    item = '636'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })
                elif utsty == 'DBD':
                    item = '634'
                    item_assigned += 1
                    records.append({
                        'part': part, 'item': item, 'matdt': matdt,
                        'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                        'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                        'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                    })

            elif utsty == 'SIP':
                utref_match += 1
                item = '610'
                item_assigned += 1
                records.append({
                    'part': part, 'item': item, 'matdt': matdt,
                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                    'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                    'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                })

        print(f"  K3TBL processing stats:")
        print(f"    Total rows: {total_rows}")
        print(f"    Rows matching UTREF patterns: {utref_match}")
        print(f"    Records with item assigned: {item_assigned}")
        print(f"    Records with matdt missing/None (will be dropped by build_ktblall): {matdt_missing}")

    except Exception as e:
        print(f"  K3TBL warning: {e}")
        import traceback
        traceback.print_exc()

    return records


def build_ktblall(k1_records, k3_records, rep_date):
    """Build KTBLALL from K1 and K3 records"""
    all_records = []

    for r in k1_records:
        if r.get('item') and r.get('matdt'):
            matdt = r['matdt']
            issdt = r.get('issdt', matdt)

            if (matdt - rep_date['date']).days < 8:
                remmth = 0.1
            else:
                remmth, rem30d = calculate_remaining_months(
                    matdt, rep_date['date'], rep_date['days_in_month']
                )

            if issdt and (matdt - issdt).days < 8:
                ori30d = 0.1
            else:
                ori30d = (matdt - issdt).days / 30 if issdt else 0

            part = r['part']
            item = r['item']
            bnmcode = f"{part}{item}00{format_mth_bucket(remmth)}0000Y"

            all_records.append({
                'src': r['src'], 'bnmcode': bnmcode, 'part': part, 'item': item,
                'cur': r.get('gwccy', 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get('gwc2r', 0), 'custno': None,
                'dealtype': r.get('gwdlp', ''), 'dealref': r.get('gwdlr', ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': ori30d, 'matdt': matdt
            })

            if part == '95':
                new_part = '93'
            elif part == '96':
                new_part = '94'
            else:
                continue

            bnmcode2 = f"{new_part}{item}00{format_mth_bucket(remmth)}0000Y"
            all_records.append({
                'src': r['src'] + '_part1', 'bnmcode': bnmcode2, 'part': new_part, 'item': item,
                'cur': r.get('gwccy', 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get('gwc2r', 0), 'custno': None,
                'dealtype': r.get('gwdlp', ''), 'dealref': r.get('gwdlr', ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': ori30d, 'matdt': matdt
            })

    for r in k3_records:
        if r.get('item') and r.get('matdt'):
            matdt = r['matdt']

            if matdt and (matdt - rep_date['date']).days < 8:
                remmth = 0.1
            elif matdt:
                remmth, rem30d = calculate_remaining_months(
                    matdt, rep_date['date'], rep_date['days_in_month']
                )
            else:
                remmth = 0.1
                rem30d = 0

            part = r['part']
            item = r['item']
            bnmcode = f"{part}{item}00{format_mth_bucket(remmth)}0000Y"

            all_records.append({
                'src': r['src'], 'bnmcode': bnmcode, 'part': part, 'item': item,
                'cur': r.get('utccy', 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get('utctp', 0), 'custno': r.get('utcus', None),
                'dealtype': r.get('utdlp', ''), 'dealref': r.get('utdlr', ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': 0, 'matdt': matdt
            })

            if part == '95':
                new_part = '93'
            elif part == '96':
                new_part = '94'
            else:
                continue

            bnmcode2 = f"{new_part}{item}00{format_mth_bucket(remmth)}0000Y"
            all_records.append({
                'src': r['src'] + '_part1', 'bnmcode': bnmcode2, 'part': new_part, 'item': item,
                'cur': r.get('utccy', 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get('utctp', 0), 'custno': r.get('utcus', None),
                'dealtype': r.get('utdlp', ''), 'dealref': r.get('utdlr', ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': 0, 'matdt': matdt
            })

    return all_records

# =============================================================================
# DCI PROCESSING
# =============================================================================
def process_dci(rep_date):
    """Process DCI (Dual Currency Investments)"""
    records = []

    try:
        dci_pattern = f"{PATHS['dciwh']}dcid*.sas7bdat"
        dci_files = glob.glob(dci_pattern)
        if not dci_files:
            print(f"  No DCI files found")
            return records

        dci_file = max(dci_files)
        print(f"  Using DCI file: {os.path.basename(dci_file)}")
        df = read_sas_file(dci_file)  # columns now normalized to lowercase

        if df is None:
            return records

        print(f"    Columns ({len(df.columns)}): {df.columns}")

        # FIX: check for real matdt/startdt/invamt/invcurr field names.
        # Your file's first 10 columns were: TICKETNO, CUSTNAME, NEWIC,
        # SALESID, CUSTCODE, INVCURRAC, ALTCURRAC, ACCINT, ROLLOVER,
        # CONVERTIND - note 'INVCURRAC' rather than 'INVCURR'. If the
        # full column dump above doesn't contain 'invcurr'/'matdt'/
        # 'startdt'/'invamt', update the aliases below to match the
        # real names before relying on this function.
        col_aliases = {
            'matdt': ['matdt'],
            'startdt': ['startdt'],
            'invamt': ['invamt'],
            'invcurr': ['invcurr', 'invcurrac'],
            'custcode': ['custcode'],
            'product': ['product'],
            'ticketno': ['ticketno'],
        }

        def resolve(name):
            for cand in col_aliases.get(name, [name]):
                if cand in df.columns:
                    return cand
            return None

        matdt_col = resolve('matdt')
        startdt_col = resolve('startdt')
        invamt_col = resolve('invamt')
        invcurr_col = resolve('invcurr')

        missing = [n for n, c in [('matdt', matdt_col), ('startdt', startdt_col),
                                   ('invamt', invamt_col), ('invcurr', invcurr_col)] if c is None]
        if missing:
            print(f"    !! WARNING [DCI]: could not resolve columns for {missing}. "
                  f"All DCI records will be skipped until these are mapped correctly. "
                  f"Real columns available: {df.columns}")
            return records

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in [matdt_col, startdt_col, invamt_col, invcurr_col, 'custcode', 'product', 'ticketno']:
                if key and key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        for row in df.iter_rows(named=True):
            # FIX: these come back as raw SAS date-serial floats
            # (e.g. 24321.0), not python dates - convert before comparing.
            matdt = sas_date_to_pydate(row.get(matdt_col))
            startdt = sas_date_to_pydate(row.get(startdt_col))

            if matdt and startdt and matdt > rep_date['date'] and startdt <= rep_date['date']:
                if (matdt - rep_date['date']).days < 8:
                    remmth = 0.1
                    rem30d = 0
                else:
                    remmth, rem30d = calculate_remaining_months(
                        matdt, rep_date['date'], rep_date['days_in_month']
                    )

                invamt = row.get(invamt_col, 0)
                invccy = str(row.get(invcurr_col, 'MYR')).upper()
                spotrt = FX_RATES.get(invccy, 1.0)

                if invccy == 'JPY':
                    invamt = round(invamt)
                else:
                    invamt = round(invamt, 2)

                amount = invamt * spotrt
                remth_bucket = format_mth_bucket(remmth)

                if invccy == 'MYR':
                    bnmcode = f"9532900{remth_bucket}0000Y"
                else:
                    bnmcode = f"9632900{remth_bucket}0000Y"

                records.append({
                    'src': 'dci', 'bnmcode': bnmcode, 'cur': invccy, 'amt': amount,
                    'custfiss': f"{row.get('custcode', 0):02d}" if 'custcode' in df.columns else '00',
                    'dealtype': row.get('product', '') if 'product' in df.columns else '',
                    'dealref': row.get('ticketno', '') if 'ticketno' in df.columns else '',
                    'remmth': remmth, 'rem30d': rem30d, 'ori30d': 0
                })
    except Exception as e:
        print(f"  DCI warning: {e}")
        import traceback
        traceback.print_exc()

    return records

# =============================================================================
# UTSAS PROCESSING
# =============================================================================
def process_utsas(rep_date):
    """Process UTSAS from EQUA tables"""
    records = []
    utvar = ['dealref', 'dealtype', 'custfiss', 'custno', 'custname', 'custeqno', 'custid']

    try:
        print(f"  Looking for UTSAS files in: {PATHS['equa']}")
        print(f"    RPTDT format: {rep_date['rptdt']} ({rep_date['date'].strftime('%y%m%d')})")

        if not os.path.exists(PATHS['equa']):
            print(f"    ERROR: Directory does not exist: {PATHS['equa']}")
            return records

        all_files = sorted(os.listdir(PATHS['equa']))
        print(f"    Total files in directory: {len(all_files)}")

        print(f"    First 20 files:")
        for f in all_files[:20]:
            print(f"      - {f}")

        for prefix in ['utms', 'utfx', 'utrp']:
            print(f"\n    Looking for {prefix} files...")

            patterns = [
                f"{prefix}{rep_date['rptdt']}.sas7bdat",
                f"{prefix}{rep_date['mon']}{rep_date['day']}.sas7bdat",
                f"{prefix}*.sas7bdat",
                f"{prefix}*",
            ]

            found = False
            for pattern in patterns:
                full_pattern = os.path.join(PATHS['equa'], pattern)
                matches = glob.glob(full_pattern)
                if matches:
                    print(f"      Pattern '{pattern}' matched {len(matches)} file(s):")
                    for m in matches[:5]:
                        print(f"        - {os.path.basename(m)}")

                    filepath = matches[0]
                    print(f"      Using: {os.path.basename(filepath)}")

                    df = read_sas_file(filepath)  # columns now normalized to lowercase
                    if df is not None:
                        print(f"      Columns in {os.path.basename(filepath)} ({len(df.columns)}): {df.columns}")

                        keep_cols = [c for c in utvar if c in df.columns]
                        missing_cols = [c for c in utvar if c not in df.columns]
                        if missing_cols:
                            print(f"      NOTE: {prefix} is missing expected columns {missing_cols} "
                                  f"(will just be skipped for this file)")
                        if keep_cols:
                            df = df.select(keep_cols)
                            if 'custeqno' in df.columns:
                                df = df.rename({'custeqno': 'acctno'})
                            records.extend(df.rows(named=True))
                            print(f"      Added {len(df)} records from {os.path.basename(filepath)}")
                        else:
                            print(f"      !! No matching columns found in {os.path.basename(filepath)}, "
                                  f"0 records added from this file")
                    found = True
                    break

            if not found:
                print(f"      No {prefix} files found")

        print(f"\n    Total UTSAS records: {len(records):,}")

    except Exception as e:
        print(f"  UTSAS warning: {e}")
        import traceback
        traceback.print_exc()

    return records

# =============================================================================
# CIS EQUITY PROCESSING
# =============================================================================
def process_cis_equity():
    """Process CIS equity data from parquet file"""
    records = []

    try:
        cis_pattern = f"{PATHS['cis']}CIS_CUST_DAILY*.parquet"
        cis_files = glob.glob(cis_pattern)
        if not cis_files:
            print(f"  No CIS parquet files found")
            return records

        cis_file = max(cis_files)
        print(f"  Using CIS file: {os.path.basename(cis_file)}")
        df = read_parquet_file(cis_file)  # columns now normalized to lowercase

        if df is None:
            return records

        warn_missing_columns(df, ['acctcode', 'prisec', 'aliaskey',
                                   'custno', 'alias', 'custname', 'acctno'], 'CIS_CUST_DAILY')
        # NOTE: 'newic' genuinely does not exist in this CIS_CUST_DAILY
        # extract (confirmed against the full column list) - handled as
        # always-blank below rather than treated as an error.
        has_newic = 'newic' in df.columns

        if 'acctcode' not in df.columns or 'prisec' not in df.columns:
            print(f"    !! Cannot filter CIS equity - missing acctcode/prisec. "
                  f"Available columns: {df.columns}")
            return records

        # FIX: prisec is numeric (e.g. 901.0). Casting a float straight to
        # Utf8 gives "901.0", which never equals the string "901" - that
        # silently zeroed out the filter. Cast through Float64 -> Int64
        # instead so 901.0, 901, and "901" all normalize the same way.
        df = df.with_columns([
            pl.col('acctcode').cast(pl.Utf8).str.strip_chars(),
            pl.col('prisec').cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
        ])
        df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))

        print(f"    CIS equity rows after filter: {len(df)}")

        for row in df.iter_rows(named=True):
            newic = row.get('newic', '') if has_newic else ''
            if not newic or (len(str(newic)) >= 5 and str(newic)[:5] == '99999'):
                icno = f"{row.get('aliaskey', '')}{row.get('custno', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('aliaskey', '')}{row.get('alias', '')}".replace(' ', '')

            records.append({
                'acctno': row.get('acctno'),
                'custno': row.get('custno'),
                'cisno': row.get('custno'),
                'cisname': row.get('custname'),
                'icno': icno
            })
    except Exception as e:
        print(f"  CIS equity warning: {e}")
        import traceback
        traceback.print_exc()

    return records

# =============================================================================
# CORE BANKING PROCESSING
# =============================================================================
def process_core_banking(rep_date):
    """Process core banking data: FD, SA, CA, FCYCA"""
    records = []

    # FIX: 'fd*.sas7bdat' was matching BOTH fd30.sas7bdat (real per-account
    # FD data) AND fdhold.sas7bdat (a separate, already-aggregated pledge
    # summary with a completely different schema - bnmcode/curcode/amount/
    # item/fdpledge*/fxpledge* - that corresponds to the original SAS's
    # LCR.FDHOLD, used later for the "FD PLEDGED" report columns, not core
    # banking at all). It was silently getting ingested here as 57 junk
    # "banking" records with no acctno/custcd. Excluded explicitly below.
    EXCLUDE_SUBSTRINGS = {'fd': ['hold']}

    try:
        for tbl in ['fd', 'sa', 'ca', 'fcyca']:
            file_pattern = f"{PATHS['lcr']}{tbl}*.sas7bdat"
            files = glob.glob(file_pattern)
            excludes = EXCLUDE_SUBSTRINGS.get(tbl, [])
            files = [f for f in files if not any(x in os.path.basename(f).lower() for x in excludes)]

            for filepath in files:
                df = read_sas_file(filepath)  # columns now normalized to lowercase
                if df is None:
                    continue

                print(f"    [{tbl}] Columns ({len(df.columns)}): {df.columns}")
                warn_missing_columns(
                    df,
                    ['bnmcode', 'amount', 'curcode', 'custcd', 'acctno',
                     'custno', 'rem30d', 'remmth'],
                    f'core_banking:{tbl}'
                )

                for row in df.iter_rows(named=True):
                    # FIX: 'custcdx' was only a thing inside the original
                    # SAS's stacked SET statement (RENAME=(CUSTCD=CUSTCDX)
                    # to disambiguate FD's custcd from the other tables'
                    # during one combined SET). Since each file is read
                    # separately here, fd30.sas7bdat's field is just
                    # 'custcd' - fall back to custcdx only if it's really
                    # there (e.g. if someone pre-renamed the file).
                    custcd = row.get('custcd')
                    if custcd is None:
                        custcd = row.get('custcdx', 0)

                    cust = get_customer_category(custcd, cust_map)

                    rem30d = row.get('rem30d', row.get('remmth', 1))
                    remmth = row.get('remmth', 1)

                    if rem30d is None:
                        rem30d = remmth

                    bic = row['bnmcode'][:5] if row.get('bnmcode') else '95311'

                    records.append({
                        'src': f'banking_{tbl}',
                        'bic': bic,
                        'bnmcode': f"{bic}{cust}020000Y",
                        'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                        'cur': row.get('curcode', 'MYR'),
                        'amt': row.get('amount', 0),
                        'acctno': row.get('acctno', 0),
                        'custno': row.get('custno', 0),
                        'custcd': custcd,
                        'cust': cust,
                        'rem30d': rem30d,
                        'remmth': remmth,
                        'ecp': '00',
                        'product': row.get('product', 0),
                        'billerind': row.get('billerind', 'N'),
                        'pbmerch': row.get('pbmerch', 'N'),
                        'intrate': row.get('intrate', 0),
                        'oprrate': row.get('oprrate', 0),
                        'source': row.get('source', ''),
                        'dtsigned': row.get('dtsigned'),
                        'intplan': row.get('intplan', 0),
                        'sme_tag': row.get('sme_tag', ''),
                        'fdhold': row.get('fdhold', 'N'),
                        'trx': row.get('trx', 0),
                        'sign': ''
                    })
    except Exception as e:
        print(f"  Core banking warning: {e}")
        import traceback
        traceback.print_exc()

    return records


def process_cis_info():
    """Process CIS info from CISDP.DEPOSIT and CISCA.DEPOSIT"""
    records = {}
    try:
        for deptype in ['cisdp', 'cisca']:
            file_pattern = f"{PATHS[deptype]}deposit*.sas7bdat"
            files = glob.glob(file_pattern)
            if not files:
                print(f"    No files matching 'deposit*.sas7bdat' found in {PATHS[deptype]}")
                debug_directory(PATHS[deptype])
                continue
            for filepath in files:
                df = read_sas_file(filepath, ['acctno', 'custno', 'seccust', 'newic', 'oldic', 'custname'])
                if df is not None:
                    if 'seccust' not in df.columns:
                        print(f"    !! WARNING [{deptype}]: 'seccust' column not found - "
                              f"columns: {df.columns}. Skipping filter for this file.")
                        continue
                    # FIX: seccust may be numeric (e.g. 901.0, which would
                    # break a direct '901' string-cast comparison the same
                    # way prisec did) OR genuinely character per the
                    # original SAS (WHERE SECCUST='901'). Branch on the
                    # actual dtype instead of assuming either.
                    if df['seccust'].dtype in (pl.Utf8, pl.String):
                        df = df.with_columns(pl.col('seccust').str.strip_chars())
                        df = df.filter(pl.col('seccust') == '901')
                    else:
                        df = df.with_columns(
                            pl.col('seccust').cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
                        )
                        df = df.filter(pl.col('seccust') == 901)
                    for row in df.rows(named=True):
                        if row.get('acctno'):
                            records[row['acctno']] = row
    except Exception as e:
        print(f"  CIS info warning: {e}")
        import traceback
        traceback.print_exc()

    return records


def process_ecp():
    """Process LCR_ECP from LIST.LCR_ECP"""
    records = {}
    try:
        file_pattern = f"{PATHS['list']}lcr_ecp*.sas7bdat"
        files = glob.glob(file_pattern)
        for filepath in files:
            df = read_sas_file(filepath)  # columns now normalized to lowercase
            if df is not None:
                for row in df.rows(named=True):
                    if row.get('acctno'):
                        records[row['acctno']] = row.get('ecp', '00')
    except Exception as e:
        print(f"  ECP warning: {e}")
        import traceback
        traceback.print_exc()

    return records


def read_walk_and_templ():
    """Read WALK.TXT and TEMPL.TXT files"""
    walk_records = []
    templ_records = []

    walk_files = glob.glob(f"{PATHS['list']}walk*.txt")
    if walk_files:
        walk_records = read_walk_file(walk_files[0])
    else:
        print(f"  No WALK.TXT files found in {PATHS['list']}")

    templ_files = glob.glob(f"{PATHS['list']}templ*.txt")
    if templ_files:
        templ_records = read_templ_file(templ_files[0])
    else:
        print(f"  No TEMPL.TXT files found in {PATHS['list']}")

    return walk_records, templ_records

# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def apply_insurance_split(records, walk_records, templ_records):
    """Split insured/uninsured portions for amounts > 250K"""
    result = []

    templ_tags = {r['tag']: r['desc'] for r in templ_records if r.get('tag')}

    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']

    for r in records:
        icgrp = r.get('icgrp', '')
        toticbal = icgrp_totals.get(icgrp, 0)

        if toticbal > 250000 and r.get('bic') not in ['9531X']:
            curbal = r['amt']
            insured_amt = (curbal / toticbal) * 250000
            uninsured_amt = curbal - insured_amt

            if r['bnmcode'][5:7] in ['29', '39'] and r.get('ecp') != '01':
                r1 = r.copy()
                r1['amt'] = curbal
                r1['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r1)
            else:
                r1 = r.copy()
                r1['amt'] = insured_amt
                result.append(r1)

                r2 = r.copy()
                r2['amt'] = uninsured_amt
                r2['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r2)
        else:
            result.append(r)

    return result

# =============================================================================
# CONSOLIDATION AND REPORTING
# =============================================================================
def consolidate_data(all_records):
    """Consolidate all records into summary by BNMCODE"""
    if not all_records:
        return pl.DataFrame()

    df = pl.DataFrame(all_records)
    df = df.with_columns([
        (pl.col('amt') / 1000).round(2).alias('amt_k')
    ])

    summary = df.group_by(['bnmcode', 'cur']).agg([
        pl.col('amt_k').sum()
    ])

    return summary


def apply_column_mapping(row, is_banking):
    """Apply column mapping logic"""
    bnmcode = row['bnmcode']
    bic = bnmcode[:5]

    col_map = {
        '95311': 'fd95311rm',
        '95312': 'sa95312rm',
        '95313': 'ca95313rm',
        '95830': 'std95830',
        '95840': 'nid95840',
        '9x810': 'ibb9x810',
        '9x329': 'dci9x329',
        '95820': 'ibr95820',
        '95850': 'bap95850',
        '9531x': 'gld9531x'
    }
    colname = col_map.get(bic[:5].lower(), '')

    if is_banking:
        item = bnmcode[5:9]
        remmth = bnmcode[9:11]
    else:
        item = bnmcode[5:7]
        if bic == '95820':
            item = 'C1.11'
        remmth = bnmcode[7:9]
        orimth = bnmcode[9:11]
        if item == 'B3.30' and orimth == '02':
            item = 'B6.30'

    if colname[:3].lower() in ['fd9', 'std']:
        colname = f"{colname}{'1' if remmth == '1' else '2'}"
    elif colname[:3].lower() in ['nid', 'dci', 'ibb', 'ibr', 'bap']:
        for i in range(1, 7):
            if str(i) == remmth:
                colname = f"{colname}v{i}"
                break

    return item, colname, row['amt_k']


def write_text_report(report_data, rep_date):
    """Write report to text files"""
    if not report_data:
        print("  No report data to write")
        return

    output_dir = PATHS['output']
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    report_df = pl.DataFrame(report_data)
    final = report_df.group_by(['item', 'colname']).agg([
        pl.col('amount').sum()
    ])

    items = sorted(final['item'].unique().to_list())
    columns = sorted(final['colname'].unique().to_list())

    filename = f"lcr{rep_date['day']}.txt"
    filepath = f"{output_dir}{filename}"

    with open(filepath, 'w') as f:
        f.write("item\t" + "\t".join(columns) + "\n")
        for item in items:
            row_data = [item]
            for col in columns:
                mask = (final['item'] == item) & (final['colname'] == col)
                if mask.any():
                    amount = final.filter(mask)['amount'].sum()
                    row_data.append(f"{amount:.2f}")
                else:
                    row_data.append("0.00")
            f.write("\t".join(row_data) + "\n")

    print(f"  ✓ {filename}: {len(items)} items x {len(columns)} columns")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBDLCRM - BNM LCR Reporting (Conventional Banking)")
    print("=" * 60)
    print("\nNOTE: KALMLIQ logic integrated directly")
    print("      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}")
    print("      - Using hardcoded FX rates")
    print("      - Column names normalized to lowercase on read (fix applied)")
    print("=" * 60)

    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    print(f"Expected K1/K3 files: k1tbl{rep_date['mon']}{rep_date['nowk']}.sas7bdat")
    print(f"Expected UTSAS files: utms{rep_date['rptdt']}.sas7bdat, utfx{rep_date['rptdt']}.sas7bdat, utrp{rep_date['rptdt']}.sas7bdat")

    print("\n" + "=" * 60)
    print("LOADING INPUTS")
    print("=" * 60)

    print("\n1. FX Rates (HARDCODED)...")
    print(f"  Loaded {len(FX_RATES)} currencies: {list(FX_RATES.keys())}")

    print("\n2. Loading WALK.TXT and TEMPL.TXT...")
    walk_records, templ_records = read_walk_and_templ()
    print(f"  WALK: {len(walk_records)} records")
    print(f"  TEMPL: {len(templ_records)} records")

    print("\n3. Processing KALMLIQ (K1TBL and K3TBL)...")
    k1_records = process_k1tbl(rep_date)
    print(f"  K1TBL records: {len(k1_records):,}")
    k3_records = process_k3tbl(rep_date)
    print(f"  K3TBL records: {len(k3_records):,}")

    treasury_records = build_ktblall(k1_records, k3_records, rep_date)
    print(f"  Total treasury records: {len(treasury_records):,}")

    print("\n4. Processing DCIWH.DCID...")
    dci_records = process_dci(rep_date)
    print(f"  DCI records: {len(dci_records):,}")

    print("\n5. Processing CIS.CUSTDLY (parquet)...")
    cis_records = process_cis_equity()
    cis_dict = {r['acctno']: r for r in cis_records if r.get('acctno')}
    print(f"  CIS records: {len(cis_dict):,}")

    print("\n6. Processing EQUA.UTMS/UTFX/UTRP...")
    utsas_records = process_utsas(rep_date)
    utsas_dict = {r['dealref']: r for r in utsas_records if r.get('dealref')}
    print(f"  UTSAS records: {len(utsas_dict):,}")

    print("\n7. Processing LCR.FD/SA/CA/FCYCA...")
    banking_records = process_core_banking(rep_date)
    print(f"  Banking records: {len(banking_records):,}")

    print("\n8. Processing CISDP/CISCA.DEPOSIT...")
    cis_info_dict = process_cis_info()
    print(f"  CIS info records: {len(cis_info_dict):,}")

    print("\n9. Processing LIST.LCR_ECP...")
    ecp_dict = process_ecp()
    print(f"  ECP records: {len(ecp_dict):,}")

    print("\n" + "=" * 60)
    print("PROCESSING DATA")
    print("=" * 60)

    all_treasury = treasury_records + dci_records
    print(f"\nCombined treasury + DCI: {len(all_treasury):,} records")

    enhanced_treasury = []
    for r in all_treasury:
        dealref = r.get('dealref')
        if dealref and dealref in utsas_dict:
            ut = utsas_dict[dealref]
            r.update(ut)

        acctno = r.get('acctno') or r.get('custeqno')
        if acctno and acctno in cis_dict:
            ci = cis_dict[acctno]
            r['cisno'] = ci.get('cisno')
            r['cisname'] = ci.get('cisname')
            r['icno'] = ci.get('icno')

        custfiss = r.get('custfiss', 0)
        if custfiss:
            try:
                custfiss = int(custfiss)
            except:
                custfiss = 0

        custno = r.get('custno', '')
        cust = get_customer_category(custfiss, cust_map, special_cust,
                                     is_custno=(custno in special_cust.get('39', [])))

        bic = r['bnmcode'][:5]
        if bic == '95830' and r.get('dealtype') in ['BCQ', 'BCT', 'BCW']:
            bic = '9583X'

        rem30d = r.get('rem30d', r.get('remmth', 1))
        remmth = r.get('remmth', 1)

        if rem30d is None:
            rem30d = remmth

        bnmcode = f"{bic}{cust}{format_day_bucket(rem30d)}0000Y"
        cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}0000Y"

        if custno in special_cust.get('49', []) and cust == '49' and bic in ['95840', '96840']:
            ori30d = r.get('ori30d', 0)
            if format_day_bucket(ori30d) > '05' and format_day_bucket(rem30d) > '01':
                bnmcode = bnmcode[:9] + '0200Y'

        icgrp = r.get('custid', r.get('icno', '')).replace(' ', '') if isinstance(r.get('custid', r.get('icno', '')), str) else ''

        enhanced_treasury.append({
            'src': r['src'],
            'bic': bic,
            'bnmcode': bnmcode,
            'cmmcode': cmmcode,
            'cur': r.get('cur', 'MYR'),
            'amt': r.get('amt', 0),
            'dealref': dealref,
            'custno': custno,
            'icgrp': icgrp,
            'rem30d': rem30d,
            'remmth': remmth,
            'acctno': acctno,
            'ori30d': r.get('ori30d', 0)
        })

    print(f"Enhanced treasury: {len(enhanced_treasury):,} records")

    enhanced_banking = []
    for r in banking_records:
        acctno = r['acctno']

        if acctno in cis_info_dict:
            ci = cis_info_dict[acctno]
            r['newic'] = ci.get('newic')
            r['oldic'] = ci.get('oldic')
            r['custname'] = ci.get('custname')

        if acctno in ecp_dict:
            r['ecp'] = ecp_dict[acctno]

        if r['ecp'] == '':
            r['ecp'] = '00'
        if r['ecp'] == '01':
            if r['intrate'] < r['oprrate']:
                r['ecp'] = '01'
            else:
                r['ecp'] = '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'

        product_list = [106, 151, 158, 97, 164, 201, 215]
        intplan_ranges = list(range(400,420)) + list(range(600,659)) + \
                         list(range(720,741)) + list(range(864,891)) + \
                         list(range(941,968))

        if (r['product'] in product_list or
            r['intplan'] in intplan_ranges or
            (r['source'] != 'PGD' and r['dtsigned'] and
             r['dtsigned'] > 0 and
             (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '

        special_39 = [4391161,2115999,12579649,13468207,14300254,
                     14675929,15327497,17104931,12677444,3703533,
                     5978659,16185090,2558344,10819745]

        # FIX: 'cust' must default to the category already computed in
        # process_core_banking() (r['cust']) - previously this key was
        # only ever set for the 14 special customer numbers above and
        # was otherwise missing entirely, which would KeyError (or, in
        # the original silently-defaulting style, blow up) whenever a
        # gold/XAU record was hit downstream.
        if r['custno'] in special_39:
            r['cust'] = '39'

        if r['cur'] == 'XAU':
            r['bic'] = '9531X'
            r['bnmcode'] = f"9531X{r['cust']}100000Y"
            r['cmmcode'] = f"9531X{r['cust']}{format_mth_bucket(r['remmth'])}0000Y"
            r['amt'] = r['amt'] * FX_RATES.get('XAU', 200.0)
            r['cur'] = 'MYR'

        enhanced_banking.append(r)

    print(f"Enhanced banking: {len(enhanced_banking):,} records")

    icgrp_totals = {}
    for r in enhanced_banking:
        newic = r.get('newic') or ''
        oldic = r.get('oldic') or ''
        icgrp = (newic or oldic).replace(' ', '') if isinstance(newic or oldic, str) else ''
        r['icgrp'] = icgrp
        icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']

    exclude_cust = [14094942,16557696,3728510,11335374,16265490,
                    3523050,11880426,16771972,15241330,16500538]

    for r in enhanced_banking:
        icgrp = r['icgrp']
        toticbal = icgrp_totals.get(icgrp, 0)
        r['toticbal'] = toticbal

        if (r['custno'] not in exclude_cust and r['bnmcode'][5:7] == '29') or r['custcd'] in [72,73,74]:
            totdpbal = toticbal + 0
            if totdpbal < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdpbal = toticbal + 0
            if totdpbal >= 5000000:
                r['bnmcode'] = f"{r['bic']}29{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}29{r['cmmcode'][7:]}"

        if r['bnmcode'][5:7] in ['08', '19'] and r['bic'] != '9531X':
            if r.get('trx') == 1:
                tag = '01'
            elif r.get('sign') in ['R', 'R ']:
                tag = '02'
            else:
                tag = '03'
            r['bnmcode'] = r['bnmcode'][:7] + tag + '0000Y'

        if r['bic'] in ['95313', '96313']:
            r['bnmcode'] = r['bnmcode'][:9] + r['ecp'] + '00Y'
            r['cmmcode'] = r['cmmcode'][:9] + r['ecp'] + '00Y'

    print("\nApplying insurance split...")
    banking_split = apply_insurance_split(enhanced_banking, walk_records, templ_records)
    print(f"Banking after insurance split: {len(banking_split):,} records")

    all_data = enhanced_treasury + banking_split
    print(f"\nTotal records before consolidation: {len(all_data):,}")

    print("\nConsolidating...")
    summary = consolidate_data(all_data)
    print(f"  Consolidated to {len(summary):,} BNM code x currency combinations")

    print("\nGenerating LCR report (text format)...")
    report_data = []
    for row in summary.rows(named=True):
        is_banking = row['bnmcode'][5] != '9'
        item, colname, amount = apply_column_mapping(row, is_banking)
        report_data.append({
            'item': item,
            'colname': colname,
            'amount': amount,
            'cur': row['cur']
        })

    if report_data:
        write_text_report(report_data, rep_date)
    else:
        print("  No report data to write")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if all_data:
        df_all = pl.DataFrame(all_data)
        total = df_all['amt'].sum() / 1000
        by_src = df_all.group_by('src').agg([(pl.col('amt').sum() / 1000).alias('amt_k')])

        print(f"\nTotal: RM {total:,.0f}K")
        print(f"\nBy Source:")
        for row in by_src.sort('amt_k', descending=True).iter_rows():
            print(f"  {row[0]}: RM {row[1]:,.0f}K")
    else:
        print("\n  No data processed!")

    print("\n" + "=" * 60)
    print("✓ EIBDLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
