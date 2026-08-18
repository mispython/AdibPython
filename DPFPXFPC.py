"""
EIBDLCRM.py - BNM LCR (Liquidity Coverage Ratio) Reporting for Conventional Banking

Self-contained: all paths, FX rates, customer maps, and shared utility
functions live in this one file. The only separate module is kalmliq.py,
which mirrors the original SAS program's structure of keeping treasury
processing (%INC PGM(KALMLIQ)) as its own include, separate from the
main program.

Run with:  python3 EIBDLCRM.py
Requires:  kalmliq.py in the same directory.
"""

import os
import glob
from pathlib import Path
from datetime import date, datetime, timedelta

import polars as pl
import pyreadstat

from kalmliq import process_k1tbl, process_k3tbl, process_k1tbx, process_k3tbl3, build_ktblall

# TODO: populate with the real $CTYPE. PROC FORMAT mapping (UTCTP -> 2-digit
# CUST code) once available. Until then process_k3tbl3() will correctly
# report 0 records - see kalmliq.py's module docstring for details.
CTYPE_LOOKUP = {}


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

for _path in PATHS.values():
    Path(_path).mkdir(parents=True, exist_ok=True)

inst = 'PBB'  # Institution code (&INST macro variable)

# Customer category mappings (LCR)
cust_map = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69],  # SME
    '29': [0, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99],  # Other retail
    '39': [1, 71, 72, 73, 74, 90, 91, 92],  # Sovereign funds
    '49': [2, 3, 7, 12, 81, 82, 83, 84],  # Financial institutions
    '59': [4, 5, 6, 13, 20] + list(range(30, 41)) + [17]  # Corporate
}

# Special customers
special_cust = {
    '39': ['kwsp', 'kwap', 'kwan', 'lemtab'],
    '49': ['aim', 'pbl', 'pbleur', 'pblnid', 'pblusd', 'pivmyr', 'ipbb']
}

# Hardcoded FX rates (replaces FOFMT / $FORATE. format)
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
# SHARED UTILITIES
# =============================================================================
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_pydate(val):
    """
    Convert a SAS date value to a python date.

    pyreadstat does not always auto-convert SAS date-formatted numeric
    columns to python date/datetime - in practice it comes back as a raw
    float (days since 1960-01-01, e.g. 24321.0). This handles all the
    shapes we might get: already-a-date, already-a-datetime, or a raw
    SAS numeric serial.
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
    SAS variable names come back UPPERCASE from pyreadstat (and the
    parquet extract preserves whatever case it was written with, which
    is also uppercase in this environment). Downstream code assumes
    lowercase column names, so normalize immediately on read - otherwise
    every 'col_name' in df.columns / row.get('col_name') check silently
    fails and returns default values (0 / '' / None) instead of the
    real data.
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
    with columns normalized to lowercase."""
    try:
        if columns:
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
    normalized to lowercase."""
    try:
        df = pl.read_parquet(filepath)
        print(f"    Successfully read: {os.path.basename(filepath)} ({len(df)} rows, {len(df.columns)} columns)")
        df = _normalize_columns(df)
        return df
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
        return None


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


def get_report_date():
    """Get report date. Equivalent to the REPTDATE data step + NOWK/REPTMON/
    REPTDAY/RPTDT/RDATE/TDATE macro variables at the top of the SAS program."""
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


def calculate_remaining_months(matdt, reptdate, days_in_month):
    """Calculate REMMTH and REM30D (equivalent to the %REMMTH macro)."""
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
    """Format months into bucket (01-10). Approximates PUT(REMMTH, REMFMT.)."""
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
    """Format days into bucket (01=<=30, 02=>30). Approximates a day-based format."""
    return '01' if days <= 1 else '02'


def get_customer_category(code, mapping, special=None, is_custno=False):
    """Get customer category from code"""
    if is_custno and special and code in special:
        return next((cat for cat, vals in special.items() if code in vals), '29')

    for cat, codes in mapping.items():
        if code in codes:
            return cat
    return '29'


# =============================================================================
# WALK.TXT / TEMPL.TXT
# =============================================================================
def read_walk_file(filepath):
    """
    Read WALK.TXT fixed-width file.

    SAS layout (from the main EIBDLCRM.sas program):
        INFILE WALK;
        INPUT @002 SET_ID  $19.
              @042 AMOUNT  COMMA20.2
              @062 SIGN    $1.
        IF SIGN = '' THEN AMOUNT = -1*AMOUNT;
        ITEM = PUT(SET_ID,$LCRCDGL.);

    SAS column positions are 1-indexed; @002 means "start at column 2"
    which is index 1 in a 0-indexed python string.

    NOTE: ITEM = PUT(SET_ID,$LCRCDGL.) applies a SAS format (a lookup
    table named LCRCDGL) mapping SET_ID -> report ITEM code. That format
    table hasn't been provided, so `item` is left blank here; if you can
    get the $LCRCDGL PROC FORMAT definitions (fmtname/start/label), plug
    them into ITEM_LOOKUP below and this will produce real ITEM values.
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
# DCI PROCESSING
# =============================================================================
def process_dci(rep_date):
    """Process DCI (Dual Currency Investments) from DCIWH.DCID"""
    records = []

    try:
        dci_pattern = f"{PATHS['dciwh']}dcid*.sas7bdat"
        dci_files = glob.glob(dci_pattern)
        if not dci_files:
            print(f"  No DCI files found")
            return records

        dci_file = max(dci_files)
        print(f"  Using DCI file: {os.path.basename(dci_file)}")
        df = read_sas_file(dci_file)  # columns normalized to lowercase

        if df is None:
            return records

        print(f"    Columns ({len(df.columns)}): {df.columns}")

        # DCI file's real columns confirmed: matdt, startdt, invamt,
        # invcurr, custcode, product, ticketno all present directly.
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
            # These come back as raw SAS date-serial floats (e.g.
            # 24321.0), not python dates - convert before comparing.
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

                invamt = row.get(invamt_col, 0) or 0
                invccy = str(row.get(invcurr_col, 'MYR') or 'MYR').upper()
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
                    'custfiss': f"{int(row.get('custcode', 0) or 0):02d}" if 'custcode' in df.columns else '00',
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
# UTSAS PROCESSING (EQUA.UTMS/UTFX/UTRP)
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

                    df = read_sas_file(filepath)  # columns normalized to lowercase
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
# CIS EQUITY PROCESSING (CIS.CUSTDLY, parquet)
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
        df = read_parquet_file(cis_file)  # columns normalized to lowercase

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

        # prisec is numeric (e.g. 901.0). Casting a float straight to
        # Utf8 gives "901.0", which never equals the string "901" - that
        # would silently zero out the filter. Cast through Float64 ->
        # Int64 instead so 901.0, 901, and "901" all normalize the same.
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
# CORE BANKING PROCESSING (LCR.FD/SA/CA/FCYCA)
# =============================================================================
def process_core_banking(rep_date):
    """Process core banking data: FD, SA, CA, FCYCA"""
    records = []

    # 'fd*.sas7bdat' would also match fdhold.sas7bdat (a separate,
    # already-aggregated pledge summary with a completely different
    # schema - bnmcode/curcode/amount/item/fdpledge*/fxpledge* -
    # corresponding to the SAS's LCR.FDHOLD, used later for the "FD
    # PLEDGED" report columns, not core banking at all). Excluded below.
    EXCLUDE_SUBSTRINGS = {'fd': ['hold']}

    try:
        for tbl in ['fd', 'sa', 'ca', 'fcyca']:
            file_pattern = f"{PATHS['lcr']}{tbl}*.sas7bdat"
            files = glob.glob(file_pattern)
            excludes = EXCLUDE_SUBSTRINGS.get(tbl, [])
            files = [f for f in files if not any(x in os.path.basename(f).lower() for x in excludes)]

            for filepath in files:
                df = read_sas_file(filepath)  # columns normalized to lowercase
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
                    # 'custcdx' was only a thing inside the original
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
                    # seccust may be numeric (e.g. 901.0, which would
                    # break a direct '901' string-cast comparison the
                    # same way prisec did) OR genuinely character per the
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
            df = read_sas_file(filepath)  # columns normalized to lowercase
            if df is not None:
                for row in df.rows(named=True):
                    if row.get('acctno'):
                        records[row['acctno']] = row.get('ecp', '00')
    except Exception as e:
        print(f"  ECP warning: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def apply_insurance_split(records, walk_records, templ_records):
    """Split insured/uninsured portions for amounts > 250K"""
    result = []

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
    print("\nNOTE: KALMLIQ logic lives in kalmliq.py (separate module,")
    print("      mirrors the original %INC PGM(KALMLIQ) SAS structure)")
    print("      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}")
    print("      - Using hardcoded FX rates")
    print("      - Column names normalized to lowercase on read")
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
    k1_records = process_k1tbl(rep_date, PATHS['bnmk'])
    print(f"  K1TBL records: {len(k1_records):,}")
    k3_records = process_k3tbl(rep_date, PATHS['bnmk'], inst=inst)
    print(f"  K3TBL records: {len(k3_records):,}")

    print("\n3b. Processing K1TBX (from KAMLIQX - FX swap items 711/911)...")
    k1tbx_records = process_k1tbx(rep_date, PATHS['bnmk'])
    print(f"  K1TBX records: {len(k1tbx_records):,}")

    print("\n3c. Processing K3TBL3 (from KALMLIQ4 - repo items 820/830)...")
    k3tbl3_records = process_k3tbl3(rep_date, PATHS['bnmk'], ctype_lookup=CTYPE_LOOKUP)
    print(f"  K3TBL3 records: {len(k3tbl3_records):,}")

    # K1TBX is stacked alongside K1TBL/K3TBL per the original SAS
    # (SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;). K3TBL3's exact merge point
    # wasn't shown in the source we have, but it shares K3TBL's record
    # shape and KALMLIQ4 runs immediately after the K3TBL step, so it's
    # merged into the K3TBL list here (see kalmliq.py docstring "REMAINING
    # KNOWN GAPS" #1 for the reasoning/caveat).
    k1_all = k1_records + k1tbx_records
    k3_all = k3_records + k3tbl3_records

    treasury_records = build_ktblall(k1_all, k3_all, rep_date)
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
            except (ValueError, TypeError):
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

        icgrp_src = r.get('custid', r.get('icno', ''))
        icgrp = icgrp_src.replace(' ', '') if isinstance(icgrp_src, str) else ''

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
        intplan_ranges = list(range(400, 420)) + list(range(600, 659)) + \
                          list(range(720, 741)) + list(range(864, 891)) + \
                          list(range(941, 968))

        if (r['product'] in product_list or
                r['intplan'] in intplan_ranges or
                (r['source'] != 'PGD' and r['dtsigned'] and
                 r['dtsigned'] > 0 and
                 (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '

        special_39 = [4391161, 2115999, 12579649, 13468207, 14300254,
                      14675929, 15327497, 17104931, 12677444, 3703533,
                      5978659, 16185090, 2558344, 10819745]

        # 'cust' defaults to the category already computed in
        # process_core_banking() (r['cust']); overridden only for the
        # 14 special customer numbers.
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
        raw_icgrp = newic or oldic
        icgrp = raw_icgrp.replace(' ', '') if isinstance(raw_icgrp, str) else ''
        r['icgrp'] = icgrp
        icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']

    exclude_cust = [14094942, 16557696, 3728510, 11335374, 16265490,
                     3523050, 11880426, 16771972, 15241330, 16500538]

    for r in enhanced_banking:
        icgrp = r['icgrp']
        toticbal = icgrp_totals.get(icgrp, 0)
        r['toticbal'] = toticbal

        if (r['custno'] not in exclude_cust and r['bnmcode'][5:7] == '29') or r['custcd'] in [72, 73, 74]:
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
