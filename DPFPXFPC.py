"""
kalmliq.py - Treasury (K1TBL / K3TBL) processing for BNM LCR reporting.

Self-contained module - mirrors the original SAS structure where KALMLIQ
is its own include (%INC PGM(KALMLIQ)) separate from the main program.
It has no dependency on any other project file: it takes the BNMK folder
path and institution code as plain arguments from EIBDLCRM.py, rather
than importing a shared config/common module.

=====================================================================
RESOLVED (previously listed as gaps, now implemented from the real
KAMLIQX / KALMLIQ4 source):

- K1TBX is now implemented in process_k1tbx() below, ported from
  KAMLIQX.sas. Its output should be merged into the K1TBL record list
  before calling build_ktblall() - see EIBDLCRM.py's main().

- K3TBL3 is now implemented in process_k3tbl3() below, ported from
  KALMLIQ4.sas. Its output should be merged into the K3TBL record list
  before calling build_ktblall() - see EIBDLCRM.py's main().

REMAINING KNOWN GAPS:

1. Exactly how K3TBL3 gets combined with K3TBL is not shown in what
   we've been given - the KTBLALL SET statement we've seen only lists
   K1TBL(IN=A) K3TBL(IN=B) K1TBX, not K3TBL3. Since KALMLIQ4 runs
   immediately after the K3TBL DATA step, and K3TBL3 shares the same
   PART/ITEM/MATDT/AMOUNT/AMTUSD/AMTSGD shape as K3TBL, the working
   assumption here is a PROC APPEND BASE=K3TBL DATA=K3TBL3 that wasn't
   in the excerpt (this codebase uses that exact append pattern
   elsewhere, e.g. "PROC APPEND BASE=K1TBL DATA=K3TBL" in the
   DISTRIBUTION PROFILE section). If that assumption is wrong, let me
   know and I'll adjust.

2. process_k3tbl3() needs the $CTYPE. SAS format (CUST=PUT(UTCTP,
   $CTYPE.)) to classify UTCTP into the 2-digit codes checked against
   IREP/NREP. That format hasn't been provided (same kind of gap as
   $LCRCDGL for WALK.TXT) - pass a populated `ctype_lookup` dict to
   process_k3tbl3() once you have the real PROC FORMAT definitions;
   until then it will correctly produce 0 records (matching what the
   SAS source itself would do with an unresolved format - the source's
   own "IF CUST NE '  '" filter drops everything when CUST is blank).

3. The later "DISTRIBUTION PROFILE OF CUSTOMER DEPOSITS (PART 3)"
   block (NON-INTERBANK REPOS / NON-INTERBANK NIDS, re-reading
   BNMK.K1TBL/K3TBL into a CAT/NAME/AMOUNT summary) is a separate
   report section that does NOT feed into KTBLALL/the main LCR figures.
   Not implemented here since it's a distinct output. Let me know if
   you need it and I'll add it as its own function.
=====================================================================
"""

import os
import glob
from datetime import date, datetime, timedelta

import polars as pl
import pyreadstat


# =============================================================================
# SELF-CONTAINED UTILITIES
# (small, deliberately duplicated here rather than imported from a shared
#  module, so this file has zero dependency on the rest of the project)
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
    SAS variable names come back UPPERCASE from pyreadstat. Downstream
    code here is written assuming lowercase column names, so normalize
    immediately on read - otherwise every 'col_name' in df.columns /
    row.get('col_name') check silently fails and returns default values
    (0 / '' / None) instead of the real data.
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


# =============================================================================
# FILE DISCOVERY
# =============================================================================
def find_k1tbl_file(rep_date, bnmk_path):
    """Find K1TBL file (BNMK.K1TBL&REPTMON&NOWK) with debugging."""
    base_path = bnmk_path
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


def find_k3tbl_file(rep_date, bnmk_path):
    """Find K3TBL file (BNMK.K3TBL&REPTMON&NOWK) with debugging."""
    base_path = bnmk_path
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


# =============================================================================
# K1TBL - direct port of:
#   DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT GWCCY
#                    GWSHN GWC2R GWDLP GWDLR);
#      SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWMDT=MATDT GWBALC=AMOUNT
#                                           GWSDT=ISSDT));
#      IF GWMVT = 'P';
#      IF GWOCY IN ('XAU','XAT') OR GWCCY IN ('XAU','XAT') THEN DELETE;
#      ... (see kalmliq.sas for full logic)
# =============================================================================
def process_k1tbl(rep_date, bnmk_path):
    """Process K1TBL from BNMK.K1TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k1_filepath = find_k1tbl_file(rep_date, bnmk_path)

        if k1_filepath is None:
            print(f"  No K1TBL file found")
            return records

        print(f"  Using K1TBL file: {k1_filepath}")
        df = read_sas_file(k1_filepath)  # columns normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K1TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['gwmvt', 'gwccy', 'gwocy', 'gwmvts', 'gwctp', 'gwdlp', 'gwmdt',
             'gwsdt', 'gwbalc', 'gwshn', 'gwc2r', 'gwdlr'],
            'K1TBL'
        )

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
            for key in ['gwmvt', 'gwccy', 'gwocy', 'gwmvts', 'gwctp', 'gwdlp', 'gwmdt', 'gwbalc']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        filtered_out = 0
        gwmvt_p = 0
        excluded_currency = 0
        item_assigned = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            gwmvt = str(row.get(gwmvt_col, '') or '').upper()

            # IF GWMVT = 'P';
            if gwmvt != 'P':
                filtered_out += 1
                continue
            gwmvt_p += 1

            gwccy = str(row.get('gwccy', '') or '').upper() if 'gwccy' in df.columns else ''
            gwocy = str(row.get('gwocy', '') or '').upper() if 'gwocy' in df.columns else ''

            # IF GWOCY='XAU' THEN DELETE; IF GWCCY='XAU' THEN DELETE;
            # IF GWOCY='XAT' THEN DELETE; IF GWCCY='XAT' THEN DELETE;
            if gwocy in ['XAU', 'XAT'] or gwccy in ['XAU', 'XAT']:
                excluded_currency += 1
                continue

            gwmvts = str(row.get('gwmvts', '') or '').upper() if 'gwmvts' in df.columns else ''
            gwctp = str(row.get('gwctp', '') or '').upper() if 'gwctp' in df.columns else ''
            gwdlp = str(row.get('gwdlp', '') or '').upper() if 'gwdlp' in df.columns else ''

            # RENAME=(GWMDT=MATDT GWBALC=AMOUNT GWSDT=ISSDT)
            matdt = sas_date_to_pydate(row.get('gwmdt')) if 'gwmdt' in df.columns else None
            issdt = sas_date_to_pydate(row.get('gwsdt')) if 'gwsdt' in df.columns else None
            amount = (row.get('gwbalc', 0) or 0) if 'gwbalc' in df.columns else 0
            gwshn = (row.get('gwshn', '') or '') if 'gwshn' in df.columns else ''
            gwc2r = (row.get('gwc2r', 0) or 0) if 'gwc2r' in df.columns else 0
            gwdlr = (row.get('gwdlr', '') or '') if 'gwdlr' in df.columns else ''

            if gwccy == 'MYR':
                # ----- PART = '95' branch -----
                part = '95'
                amtusd = 0
                amtsgd = 0

                if gwmvts == 'M':
                    # IF GWDLP IN ('BCD','BCI','BCS','BCQ','BCT','BCW','BQD') THEN ITEM='830'
                    if gwdlp in ['BCD', 'BCI', 'BCS', 'BCQ', 'BCT', 'BCW', 'BQD']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '830', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })

                    # IF SUBSTR(GWCTP,1,1) = 'B' THEN SELECT (GWDLP) ...
                    if gwctp[:1] == 'B':
                        if gwdlp in ['LO', 'LC', 'LF', 'LS', 'LOI', 'LSI', 'LSC', 'LSW',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOC', 'LOW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BO', 'BF', 'BOI', 'BFI', 'BSC', 'BSW', 'BOC', 'BOW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        # OTHERWISE; -> no output

                    # SELECT (SUBSTR(GWDLP,2,2)) - independent of the GWCTP check above
                    dlp23 = gwdlp[1:3] if len(gwdlp) >= 2 else ''
                    if dlp23 in ['MI', 'MT']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '820', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                    elif dlp23 in ['XI', 'XT']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '620', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                # (the FXS/FXO/... block is commented out in the SAS source - not ported)

            else:
                # ----- PART = '96' branch (foreign currency) -----
                part = '96'
                amtusd = amount if gwccy == 'USD' else 0
                amtsgd = amount if gwccy == 'SGD' else 0

                if gwmvts == 'M':
                    if gwctp[:1] == 'B' and gwctp != 'BW':
                        if gwdlp in ['LO', 'LC', 'LS', 'LF', 'LOI', 'LSI', 'LSC', 'LOC',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOW', 'LSW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BC', 'BF', 'BO', 'BSC', 'BOW', 'BSW']:
                            # IF SUBSTR(GWSHN,1,6) ^= 'FCY-FD' THEN ITEM='810'
                            if gwshn[:6] != 'FCY-FD':
                                item_assigned += 1
                                records.append({
                                    'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                    'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                    'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                                })
                        elif gwdlp == 'BOC':
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        # OTHERWISE; -> no output
                # (the FXS/FXO/... block is commented out in the SAS source - not ported)

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


# =============================================================================
# K3TBL - direct port of:
#   DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT UTCCY
#                    UTCUS UTCTP UTSTY UTDLR UTDLP);
#      RETAIN PART '95';
#      SET BNMK.K3TBL&REPTMON&NOWK;
#      ... (see kalmliq.sas for full logic)
#
# NOTE: unlike K1TBL, K3TBL's source table already has native MATDT/ISSDT
# columns (no RENAME needed) - confirmed against the real file's column
# dump ('matdt', 'issdt' present directly).
# =============================================================================
def process_k3tbl(rep_date, bnmk_path, inst='PBB'):
    """Process K3TBL from BNMK.K3TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k3_filepath = find_k3tbl_file(rep_date, bnmk_path)

        if k3_filepath is None:
            print(f"  No K3TBL file found")
            return records

        print(f"  Using K3TBL file: {k3_filepath}")
        df = read_sas_file(k3_filepath)  # columns normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K3TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['utref', 'utsty', 'utdlp', 'utcus', 'utclc', 'utctp', 'matdt',
             'issdt', 'utamoc', 'utdpf', 'utccy', 'utdlr', 'utaict', 'utpcp',
             'utdpey', 'utdpe', 'utaicy', 'utait', 'utmm1'],
            'K3TBL'
        )

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

        matdt_col = 'matdt' if 'matdt' in df.columns else None
        issdt_col = 'issdt' if 'issdt' in df.columns else None
        if matdt_col is None:
            print("    !! WARNING [K3TBL]: no maturity date column found - "
                  "all K3TBL records will be dropped in build_ktblall().")

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in ['utref', 'utsty', 'utdlp', 'utcus', 'utctp', 'matdt', 'utamoc', 'utdpf']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        utref_match = 0
        item_assigned = 0
        matdt_missing = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            # AMOUNT = UTAMOC - UTDPF; IF UTSTY='IDC' THEN AMOUNT=UTAMOC + UTDPF;
            utamoc = (row.get('utamoc', 0) or 0) if 'utamoc' in df.columns else 0
            utdpf = (row.get('utdpf', 0) or 0) if 'utdpf' in df.columns else 0
            utsty = str(row.get(utsty_col, '') or '').upper() if utsty_col else ''
            amount = (utamoc + utdpf) if utsty == 'IDC' else (utamoc - utdpf)

            # IF &INST='PBB' THEN ...
            utccy = str(row.get('utccy', 'MYR') or 'MYR').upper() if 'utccy' in df.columns else 'MYR'
            amtusd = amount if (inst == 'PBB' and utccy == 'USD') else 0
            amtsgd = amount if (inst == 'PBB' and utccy == 'SGD') else 0

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

            matdt = sas_date_to_pydate(row.get(matdt_col)) if matdt_col else None
            # ISSDT is extracted here (used by build_ktblall for ORI30D)
            # since K3TBL's KEEP list includes it, exactly like K1TBL.
            issdt = sas_date_to_pydate(row.get(issdt_col)) if issdt_col else None
            if matdt is None:
                matdt_missing += 1

            part = '95'  # RETAIN PART '95';

            def emit(it, amt):
                records.append({
                    'part': part, 'item': it, 'matdt': matdt, 'issdt': issdt,
                    'amount': amt, 'amtusd': amtusd, 'amtsgd': amtsgd,
                    'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                    'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                })

            # IF UTREF IN ('INV','DRI','DLG','AFSLIQ','AFSBOND','IAFSLIQ','AFS','IAFS') THEN DO;
            if utref in ['INV', 'DRI', 'DLG', 'AFSLIQ', 'AFSBOND', 'IAFSLIQ', 'AFS', 'IAFS']:
                utref_match += 1
                if utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGS', 'MTB', 'BNB', 'BNN',
                             'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM', 'CMB', 'MGI', 'SMC']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('631', amt)
                elif utsty == 'SDC':
                    amt = (utamoc * (utpcp / 100)) + utdpey + utdpe if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty == 'LDC':
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty in ['SLD', 'SSD']:
                    amt = (utamoc * (utpcp / 100)) + utaicy + utait if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty in ['SFD', 'SZD']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty == 'SBA':
                    if utdlp not in ['MOS', 'MSS']:
                        item_assigned += 1
                        emit('633', amount)
                elif utsty in ['ISB', 'DHB', 'KHA', 'PNB']:
                    item_assigned += 1
                    emit('636', amount)
                elif utsty == 'IDS':
                    item_assigned += 1
                    emit('635', amount)
                elif utsty == 'DBD':
                    # NOTE: SAS has WHEN('DBD')->'634' listed BEFORE
                    # WHEN('DMB','DBD','GRL','MTL','RUL')->'635'. SELECT/WHEN
                    # stops at the first match, so DBD always resolves to
                    # '634' here; the second WHEN's 'DBD' is unreachable.
                    item_assigned += 1
                    emit('634', amount)
                elif utsty in ['DMB', 'GRL', 'MTL', 'RUL']:
                    item_assigned += 1
                    emit('635', amount)
                elif utsty == 'PBA':
                    if utdlp in ['MOS', 'MSS']:
                        item_assigned += 1
                        emit('850', amount)
                # OTHERWISE; -> no output

            # ELSE IF UTREF IN ('PFD','PLD','PSD','PZD','PDC') THEN DO;
            elif utref in ['PFD', 'PLD', 'PSD', 'PZD', 'PDC']:
                utref_match += 1
                if utsty in ['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']:
                    item_assigned += 1
                    emit('840', amount)

            # ELSE IF UTREF IN ('IINV','IDRI','IDLG') THEN DO;
            elif utref in ['IINV', 'IDRI', 'IDLG']:
                utref_match += 1
                if utsty == 'SBA' and utdlp == 'IOP':
                    item_assigned += 1
                    emit('633', amount)
                elif utsty in ['SDC', 'LDC']:
                    item_assigned += 1
                    emit('632', amount)
                elif utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGI', 'ITB', 'SAC', 'BMN',
                               'BMC', 'BMF', 'SCD', 'SCM', 'MGS', 'MTB', 'BNB', 'BNN', 'CMB', 'SMC']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('631', amt)
                elif utsty in ['ISB', 'IDS', 'IBZ', 'ICN']:
                    # SAS: IF UTMM1='GGB' THEN ITEM='636';
                    #      ELSE IF UTMM1='NGB' THEN ITEM='635';
                    #      AMOUNT = AMOUNT + UTAICT; OUTPUT;
                    # If neither GGB nor NGB, SAS would retain whatever ITEM
                    # held from a prior loop iteration (an edge case we
                    # don't replicate) - we simply skip emitting a record,
                    # since a genuinely blank ITEM is dropped downstream
                    # anyway by build_ktblall's "IF ITEM ^= ' '" filter.
                    if utmm1 == 'GGB':
                        item_assigned += 1
                        emit('636', amount + utaict)
                    elif utmm1 == 'NGB':
                        item_assigned += 1
                        emit('635', amount + utaict)
                elif utsty in ['DHB', 'KHA']:
                    item_assigned += 1
                    emit('636', amount)
                elif utsty == 'DBD':
                    item_assigned += 1
                    emit('634', amount)

            # IF UTSTY IN ('SIP') THEN DO; ITEM='610'; OUTPUT; END;
            # (unconditional - outside/after the UTREF if/elif chain above,
            # exactly as in the SAS source, so it can fire in addition to
            # one of the branches above for the same row)
            if utsty == 'SIP':
                item_assigned += 1
                emit('610', amount)

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


# =============================================================================
# K1TBX - direct port of KAMLIQX.sas (K1TBX / K1TBX1 / K1TBX2 / final K1TBX).
#
# Produces FX-swap-related BNM items (711/911) that get stacked into
# KTBLALL alongside K1TBL/K3TBL - this is the 'K1TBX' dataset referenced
# in "SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;".
#
# Re-reads the same BNMK.K1TBL{REPTMON}{NOWK} file as process_k1tbl(),
# since that's what the SAS source does - a second SET of the same table
# with a different RENAME/filter/derivation than the main K1TBL step.
# =============================================================================
def _k1tbx_select_gwctp(gwctp, code, gwcnal, gwsac, has_ce_when=False, otherwise_extra_ce=False):
    """
    Port of the repeated:
        SELECT(GWCTP);
          WHEN('BC') BNMCODE=code;
          WHEN('BB') BNMCODE=code;
          WHEN('BI') BNMCODE=code;
          WHEN('BM') BNMCODE=code;
          [WHEN('CE') BNMCODE=code;]                    <- only if has_ce_when
          WHEN('BA','BW','BE') BNMCODE=code;
          OTHERWISE DO;
            IF NOT('BA' <= GWCTP <= 'BZ') AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN BNMCODE=code;
            [IF GWCTP='CE' THEN BNMCODE=code;]           <- only if otherwise_extra_ce
            IF GWSAC EQ 'UF' THEN BNMCODE=code;
          END;
        END;
    block used across the FXS / FXO,FXF / SF1,SF2,TS1,TS2,FF1,FF2
    branches. The two boolean flags capture the small differences
    between branches (see call sites below for exactly which branch
    uses which flag combination - verified line-by-line against the
    KAMLIQX source).
    """
    if gwctp in ('BC', 'BB', 'BI', 'BM'):
        return code
    if has_ce_when and gwctp == 'CE':
        return code
    if gwctp in ('BA', 'BW', 'BE'):
        return code
    # OTHERWISE
    if not ('BA' <= gwctp <= 'BZ') and gwcnal == 'MY' and gwsac != 'UF':
        return code
    if otherwise_extra_ce and gwctp == 'CE':
        return code
    if gwsac == 'UF':
        return code
    return None


def process_k1tbx(rep_date, bnmk_path):
    """Process K1TBX (FX swap items) from BNMK.K1TBL{REPTMON}{NOWK}."""
    records = []

    try:
        k1_filepath = find_k1tbl_file(rep_date, bnmk_path)
        if k1_filepath is None:
            print("  No K1TBL file found (needed for K1TBX)")
            return records

        print(f"  Using K1TBL file for K1TBX: {k1_filepath}")
        df = read_sas_file(k1_filepath)
        if df is None:
            return records

        warn_missing_columns(
            df,
            ['gwmvt', 'gwocy', 'gwccy', 'gwmdt', 'gwbalc', 'gwdlp', 'gwmvts',
             'gwctp', 'gwcnal', 'gwsac'],
            'K1TBX'
        )

        # ----- DATA K1TBX; (base filter, shared by K1TBX1 and K1TBX2) -----
        base_rows = []
        for row in df.iter_rows(named=True):
            gwmvt = str(row.get('gwmvt', '') or '').upper()
            # IF GWMVT = 'P';
            if gwmvt != 'P':
                continue

            gwocy = str(row.get('gwocy', '') or '').upper()
            gwccy = str(row.get('gwccy', '') or '').upper()
            # IF GWOCY='XAU' THEN DELETE; IF GWCCY='XAU' THEN DELETE;
            # (NOTE: unlike process_k1tbl, XAT is NOT excluded here -
            # faithful to KAMLIQX, which only excludes XAU, not XAT)
            if gwocy == 'XAU' or gwccy == 'XAU':
                continue

            gwdlp = str(row.get('gwdlp', '') or '').upper()
            # IF GWDLP IN ('FXS','FXO','FXF','SF1','SF2','TS1','TS2','FBP','FF1','FF2');
            if gwdlp not in ['FXS', 'FXO', 'FXF', 'SF1', 'SF2', 'TS1', 'TS2', 'FBP', 'FF1', 'FF2']:
                continue

            matdt = sas_date_to_pydate(row.get('gwmdt'))  # RENAME=(GWMDT=MATDT)
            amount = row.get('gwbalc', 0) or 0            # RENAME=(GWBALC=AMOUNT)
            amtusd = amount if gwccy == 'USD' else 0.0
            amtsgd = amount if gwccy == 'SGD' else 0.0

            base_rows.append({
                'gwmvt': gwmvt, 'gwocy': gwocy, 'gwccy': gwccy, 'gwdlp': gwdlp,
                'gwmvts': str(row.get('gwmvts', '') or '').upper(),
                'gwctp': str(row.get('gwctp', '') or '').upper(),
                'gwcnal': str(row.get('gwcnal', '') or '').upper(),
                'gwsac': str(row.get('gwsac', '') or '').upper(),
                'matdt': matdt, 'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
            })

        print(f"  K1TBX base rows (GWMVT='P', not XAU, GWDLP in swap set): {len(base_rows):,}")

        # ----- DATA K1TBX1; (domestic-leg swap classification) -----
        k1tbx1 = []
        for r in base_rows:
            bnmcode = None
            gwdlp = r['gwdlp']
            gwctp, gwcnal, gwsac = r['gwctp'], r['gwcnal'], r['gwsac']

            # IF GWOCY EQ 'MYR' AND GWMVT EQ 'P' AND GWMVTS EQ 'P' THEN ... (-> '57100')
            if r['gwocy'] == 'MYR' and r['gwmvt'] == 'P' and r['gwmvts'] == 'P':
                if gwdlp == 'FXS' and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57100', gwcnal, gwsac, has_ce_when=True)
                elif gwdlp == 'FBP' and r['gwccy'] != 'MYR':
                    bnmcode = '57100'
                elif gwdlp in ('FXO', 'FXF') and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57100', gwcnal, gwsac)
                elif gwdlp in ('SF1', 'SF2', 'TS1', 'TS2', 'FF1', 'FF2') and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57100', gwcnal, gwsac)

            # IF GWOCY EQ 'MYR' AND GWMVT EQ 'P' AND GWMVTS EQ 'S' THEN ... (-> '57400')
            # (note: no FBP branch in this block - asymmetric vs the 'P' block above,
            # faithful to the source)
            if r['gwocy'] == 'MYR' and r['gwmvt'] == 'P' and r['gwmvts'] == 'S':
                if gwdlp == 'FXS' and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57400', gwcnal, gwsac, has_ce_when=True)
                elif gwdlp in ('FXO', 'FXF') and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57400', gwcnal, gwsac, otherwise_extra_ce=True)
                elif gwdlp in ('SF1', 'SF2', 'TS1', 'TS2', 'FF1', 'FF2') and r['gwccy'] != 'MYR':
                    bnmcode = _k1tbx_select_gwctp(gwctp, '57400', gwcnal, gwsac)

            if bnmcode:
                k1tbx1.append({**r, 'bnmcode': bnmcode})

        # ----- DATA K1TBX2; (both legs foreign currency -> '57600') -----
        k1tbx2 = []
        for r in base_rows:
            if (r['gwccy'] != 'MYR' and r['gwocy'] != 'MYR' and
                    r['gwmvt'] == 'P' and r['gwmvts'] == 'P' and
                    r['gwdlp'] in ('FXS', 'FXO', 'FXF', 'SF2', 'FF1', 'FF2', 'SF1', 'TS1', 'TS2')):
                k1tbx2.append({**r, 'bnmcode': '57600'})

        print(f"  K1TBX1 (domestic-leg matches): {len(k1tbx1):,}, K1TBX2 (both-foreign matches): {len(k1tbx2):,}")

        # ----- final DATA K1TBX; (expand each match into PART/ITEM pairs) -----
        for r in k1tbx1 + k1tbx2:
            amt = abs(r['amount']) if r['amount'] < 0 else r['amount']
            bnmcode = r['bnmcode']
            matdt = r['matdt']

            if bnmcode == '57100':
                records.append({'part': '95', 'item': '911', 'matdt': matdt,
                                 'amount': amt, 'amtusd': 0.0, 'amtsgd': 0.0, 'src': 'k1tbx'})
                records.append({'part': '96', 'item': '711', 'matdt': matdt,
                                 'amount': amt, 'amtusd': 0.0, 'amtsgd': 0.0, 'src': 'k1tbx'})
            elif bnmcode == '57400':
                records.append({'part': '95', 'item': '711', 'matdt': matdt,
                                 'amount': amt, 'amtusd': 0.0, 'amtsgd': 0.0, 'src': 'k1tbx'})
                records.append({'part': '96', 'item': '911', 'matdt': matdt,
                                 'amount': amt, 'amtusd': 0.0, 'amtsgd': 0.0, 'src': 'k1tbx'})
            elif bnmcode == '57600':
                records.append({'part': '96', 'item': '711', 'matdt': matdt,
                                 'amount': amt, 'amtusd': r['amtusd'], 'amtsgd': r['amtsgd'], 'src': 'k1tbx'})
                records.append({'part': '96', 'item': '911', 'matdt': matdt,
                                 'amount': amt, 'amtusd': r['amtusd'], 'amtsgd': r['amtsgd'], 'src': 'k1tbx'})

        print(f"  K1TBX final records (each match expands to 2 PART/ITEM rows): {len(records):,}")

    except Exception as e:
        print(f"  K1TBX warning: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# K3TBL3 - direct port of KALMLIQ4.sas.
#
# Produces additional repo-related treasury records (BNM items 820/830)
# from BNMK.K3TBL{REPTMON}{NOWK}, filtered to UTREF='RRS'/UTSTY='MGS'/
# UTDLP='MSS' (repo sales of MGS securities).
#
# See the "REMAINING KNOWN GAPS" note at the top of this file re: how
# this merges with K3TBL, and the $CTYPE. format dependency.
# =============================================================================
IREP_CODES = {'01', '02', '11', '12', '81'}
NREP_CODES = {'13', '17', '20', '60', '71', '72', '74', '76', '79', '85'}


def process_k3tbl3(rep_date, bnmk_path, ctype_lookup=None):
    """
    Process K3TBL3 (repo item 820/830 records) from BNMK.K3TBL{REPTMON}{NOWK}.

    ctype_lookup: dict mapping raw UTCTP values -> 2-digit CUST codes,
    equivalent to the $CTYPE. SAS format. Pass the real mapping once you
    have the PROC FORMAT definitions; until then this returns 0 records
    (matching what the SAS source itself does with CUST unresolved).
    """
    records = []
    ctype_lookup = ctype_lookup or {}

    try:
        k3_filepath = find_k3tbl_file(rep_date, bnmk_path)
        if k3_filepath is None:
            print("  No K3TBL file found (needed for K3TBL3)")
            return records

        print(f"  Using K3TBL file for K3TBL3: {k3_filepath}")
        df = read_sas_file(k3_filepath)
        if df is None:
            return records

        warn_missing_columns(
            df,
            ['utref', 'utsty', 'utdlp', 'issdt', 'utpcp', 'utfcv', 'utaict',
             'utctp', 'utidt', 'utccy', 'utcus', 'utdlr', 'matdt'],
            'K3TBL3'
        )

        if not ctype_lookup:
            print("    NOTE [K3TBL3]: $CTYPE. format lookup not populated - "
                  "'cust' will be blank for every row, so K3TBL3 will produce "
                  "0 records until ctype_lookup is filled in with the real "
                  "PROC FORMAT mapping.")

        total_rows = 0
        matched = 0
        item_assigned = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            utref = str(row.get('utref', '') or '').upper()
            utsty = str(row.get('utsty', '') or '').upper()
            utdlp = str(row.get('utdlp', '') or '').upper()

            # IF UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS';
            if not (utref == 'RRS' and utsty == 'MGS' and utdlp == 'MSS'):
                continue
            matched += 1

            issdt = sas_date_to_pydate(row.get('issdt'))
            # IF ISSDT > REPTDATE THEN DELETE;
            if issdt and issdt > rep_date['date']:
                continue

            utpcp = row.get('utpcp', 0) or 0
            utfcv = row.get('utfcv', 0) or 0
            utaict = row.get('utaict', 0) or 0
            # AMOUNT=(UTPCP*UTFCV)*0.01; AMOUNT=SUM(AMOUNT,UTAICT); /* SALES PROCEEDS */
            amount = (utpcp * utfcv) * 0.01
            amount = amount + utaict

            # CUST=PUT(UTCTP,$CTYPE.);
            utctp_raw = row.get('utctp', '')
            cust = ctype_lookup.get(utctp_raw, '')

            matdt = sas_date_to_pydate(row.get('matdt'))
            utidt_raw = row.get('utidt')
            # IF UTIDT NE ' ' THEN MATDT=INPUT(UTIDT,YYMMDD10.);
            if utidt_raw not in (None, '', ' '):
                parsed = None
                if isinstance(utidt_raw, str):
                    try:
                        parsed = datetime.strptime(utidt_raw.strip(), '%Y-%m-%d').date()
                    except ValueError:
                        parsed = None
                else:
                    parsed = sas_date_to_pydate(utidt_raw)
                if parsed is not None:
                    matdt = parsed

            # IF CUST IN &NREP THEN ITEM='830'; ELSE IF CUST IN &IREP THEN ITEM='820';
            item = None
            if cust in NREP_CODES:
                item = '830'
            elif cust in IREP_CODES:
                item = '820'

            # IF CUST NE '  '; (drop rows with blank CUST - includes the
            # unresolved-format case where cust defaults to '')
            if not cust or not cust.strip():
                continue

            if item is None:
                # CUST resolved but isn't in either list. SAS would still
                # output the row with ITEM blank; build_ktblall's own
                # "IF ITEM ^= ' '" filter would drop it downstream anyway,
                # so we just don't emit it here.
                continue

            item_assigned += 1
            records.append({
                'part': '95', 'item': item, 'matdt': matdt, 'issdt': issdt,
                'amount': amount, 'amtusd': 0, 'amtsgd': 0,
                'utccy': row.get('utccy', 'MYR'), 'utcus': row.get('utcus', ''),
                'utctp': row.get('utctp', 0),
                'utdlr': row.get('utdlr', ''), 'utdlp': utdlp,
                'src': 'k3tbl3'
            })

        print(f"  K3TBL3 processing stats:")
        print(f"    Total rows: {total_rows}")
        print(f"    Rows matching UTREF=RRS/UTSTY=MGS/UTDLP=MSS: {matched}")
        print(f"    Records with item assigned: {item_assigned}")

    except Exception as e:
        print(f"  K3TBL3 warning: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# KTBLALL - direct port of:
#   DATA KTBL (KEEP=BNMCODE AMOUNT AMTUSD AMTSGD) KTBLALL;
#      SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;    <- K1TBX not available, see module docstring
#      IF ITEM ^= ' ';
#      IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1; ELSE %REMMTH;
#      IF MATDT - ISSDT    < 8 THEN ORI30D = 0.1; ELSE ORI30D = (MATDT-ISSDT)/30;
#      BNMCODE = PART||ITEM||'00'||PUT(REMMTH,REMFMT.)||'0000Y';
#      OUTPUT;
#      IF PART = '95' THEN SUBSTR(BNMCODE,1,2) = '93'; ELSE SUBSTR(BNMCODE,1,2)='94';
#      OUTPUT;
# =============================================================================
def build_ktblall(k1_records, k3_records, rep_date):
    """
    Build KTBLALL from K1 and K3 records. Applies identically to both
    sources (both are normalized to have part/item/matdt/issdt/amount by
    the time they get here) - matching how the SAS KTBLALL step treats
    the stacked K1TBL+K3TBL(+K1TBX) the same way regardless of source.
    """
    all_records = []

    def process_source(src_records, ccy_key, custfiss_key, custno_val_fn, dealtype_key, dealref_key):
        for r in src_records:
            if not (r.get('item') and r.get('matdt')):
                continue

            matdt = r['matdt']
            issdt = r.get('issdt')

            # IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1; ELSE %REMMTH;
            if (matdt - rep_date['date']).days < 8:
                remmth = 0.1
                rem30d = 0
            else:
                remmth, rem30d = calculate_remaining_months(
                    matdt, rep_date['date'], rep_date['days_in_month']
                )

            # IF MATDT - ISSDT < 8 THEN ORI30D = 0.1; ELSE ORI30D = (MATDT-ISSDT)/30;
            if issdt and (matdt - issdt).days < 8:
                ori30d = 0.1
            elif issdt:
                ori30d = (matdt - issdt).days / 30
            else:
                ori30d = 0

            part = r['part']
            item = r['item']
            bnmcode = f"{part}{item}00{format_mth_bucket(remmth)}0000Y"

            base = {
                'src': r['src'], 'bnmcode': bnmcode, 'part': part, 'item': item,
                'cur': r.get(ccy_key, 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get(custfiss_key, 0), 'custno': custno_val_fn(r),
                'dealtype': r.get(dealtype_key, ''), 'dealref': r.get(dealref_key, ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': ori30d, 'matdt': matdt
            }
            all_records.append(base)

            # PART 1 duplicate: 95->93, else->94
            new_part = '93' if part == '95' else '94'
            dup = dict(base)
            dup['src'] = r['src'] + '_part1'
            dup['bnmcode'] = f"{new_part}{item}00{format_mth_bucket(remmth)}0000Y"
            dup['part'] = new_part
            all_records.append(dup)

    process_source(
        k1_records, ccy_key='gwccy', custfiss_key='gwc2r',
        custno_val_fn=lambda r: None,
        dealtype_key='gwdlp', dealref_key='gwdlr'
    )
    process_source(
        k3_records, ccy_key='utccy', custfiss_key='utctp',
        custno_val_fn=lambda r: r.get('utcus'),
        dealtype_key='utdlp', dealref_key='utdlr'
    )

    return all_records
