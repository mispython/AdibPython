"""
File Name: EIBMTCOF
LCR Concentration of Funding Report
Generates LCR Table 4 - Concentration of Funding report with ASA carriage control
"""

import duckdb
import polars as pl
import pyreadstat
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sys

# All monetary columns (AMOUNT, BUC1-BUC7) are carried as INTEGER CENTS
# (Int64) from the moment they're read until the very last formatting step.
# SAS numeric variables are IEEE-754 doubles, same as Python floats, and
# summing thousands of float64 values in a different grouping/sort order
# than SAS's PROC SUMMARY can drift by a few cents on multi-billion-ringgit
# totals. Integer-cent arithmetic is exact addition -> zero drift,
# regardless of summation order.
CENTS = 100


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTCOF")
LIST_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/list")

# Input files
# NOTE: REPTDATE is no longer read from a file. It is derived as (today - 1 day).
# All SAS dataset inputs are lowercase .sas7bdat files, read via pyreadstat.
CMM_PATH_TEMPLATE = INPUT_DIR / "cmm{}.sas7bdat"
EQU_PATH_TEMPLATE = INPUT_DIR / "equ{}.sas7bdat"
TEMPLATE_PATH = INPUT_DIR / "templ.txt"
WALK_PATH = INPUT_DIR / "walk.txt"
VOSTRO_PATH = INPUT_DIR / "vostro.sas7bdat"
CISINFO_PATH = INPUT_DIR / "cisinfo.sas7bdat"

# List files (lowercase .sas7bdat)
INTRA_GROUP_PATH = LIST_DIR / "cof_mni_intra_group.sas7bdat"
RELATED_PARTY_PATH = LIST_DIR / "cof_mni_related_party.sas7bdat"
EQU_INTRA_GROUP_PATH = LIST_DIR / "cof_equ_intra_group.sas7bdat"
EQU_RELATED_PARTY_PATH = LIST_DIR / "cof_equ_related_party.sas7bdat"

# Output files
COF_OUTPUT_PATH = OUTPUT_DIR / "COF_OUTPUT.txt"
SFTP_SCRIPT_PATH = OUTPUT_DIR / "SFTP_SCRIPT.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

COFF1FMT = {
    '95311': '1.01I   ',
    '95312': '1.02I   ',
    '95313': '1.03I   ',
    '95810': '1.04I   ',
    '95820': '1.05I   ',
    '95830': '1.06I   ',
    '9583X': '1.06I   ',
    '95840': '1.07I   ',
    '95329': '1.08I   ',
    '95850': '1.09I   ',
    '9531X': '1.14I   ',
    '96311': '1.19II  ',
    '96313': '1.20II  ',
    '96810': '1.21II  ',
    '96820': '1.22II  ',
    '96830': '1.23II  ',
    '9683X': '1.23II  ',
    '96840': '1.24II  ',
    '96329': '1.25II  ',
    '96850': '1.26II  ',
}

COFF2FMT = {
    '95311': '2.01I   ',
    '95312': '2.02I   ',
    '95313': '2.03I   ',
    '95810': '2.04I   ',
    '95820': '2.05I   ',
    '95830': '2.06I   ',
    '9583X': '2.06I   ',
    '95840': '2.07I   ',
    '95329': '2.08I   ',
    '96311': '2.12II  ',
    '96313': '2.13II  ',
    '96810': '2.14II  ',
    '96820': '2.15II  ',
    '96830': '2.16II  ',
    '9683X': '2.16II  ',
    '96840': '2.17II  ',
    '96329': '2.18II  ',
}

COFF3FMT = {
    '95311': '3.01I   ',
    '95312': '3.02I   ',
    '95313': '3.03I   ',
    '95830': '3.04I   ',
    '9583X': '3.04I   ',
    '95840': '3.05I   ',
    '95329': '3.06I   ',
    '9531X': '3.07I   ',
    '96311': '3.11II  ',
    '96313': '3.12II  ',
    '96830': '3.13II  ',
    '9683X': '3.13II  ',
    '96840': '3.14II  ',
    '96329': '3.15II  ',
}

COFF4FMT = {
    '95311': '4.01    ',
    '95312': '4.02    ',
    '95313': '4.03    ',
}

COFF5FMT = {
    '95311': '5.01    ',
    '95313': '5.02    ',
    '95810': '5.04I   ',
    '95820': '5.04II  ',
    '95830': '5.04III ',
    '9583X': '5.04III ',
    '95840': '5.04IV  ',
    '95329': '5.04V   ',
    '95850': '5.04VI  ',
    '9531X': '5.04VII ',
    '96810': '5.05I   ',
    '96820': '5.05II  ',
    '96830': '5.05III ',
    '9683X': '5.05III ',
    '96840': '5.05IV  ',
    '96329': '5.05V   ',
    '96850': '5.05VI  ',
}

GLCOFFMT = {
    'F143110VLB': '1.10I   ',
    'F143110VIB': '1.10I   ',
    'F143110VFBI': '1.10I   ',
    'F143130': '1.11I   ',
    'F144111RM': '1.12I   ',
    'F141301': '1.12I   ',
    'F147100': '1.12I   ',
    'F144140CAGA': '1.13I   ',
    'F249120BP': '1.15I   ',
    'F142199A': '1.16I   ',
    'F142199B': '1.16I   ',
    'F142199C': '1.16I   ',
    'F142199D': '1.16I   ',
    'F142199E': '1.16I   ',
    'F142510FDA': '1.17I   ',
    'F142599OELED': '1.17I   ',
    'F142600FBI': '1.27II  ',
    'F143620FNFBI': '1.28II  ',
    'F247610': '1.29II  ',
    'F142699ALL': '1.30II  ',
    'F133110ODVIB': '2.09I   ',
    'F142699': '2.20II  ',
    'F142600PBB': '2.22II  ',
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_sas(path):
    """
    Read a SAS7BDAT dataset into a polars DataFrame using pyreadstat.
    SAS fixed-width character columns are stripped of trailing/leading
    whitespace so downstream string comparisons (e.g. filtering on '',
    list membership) behave the same as in the original SAS code.
    """
    print(f"Reading SAS dataset: {path}")
    print("Exists?", path.exists())

    df_pd, _meta = pyreadstat.read_sas7bdat(str(path))
    df = pl.from_pandas(df_pd)

    str_cols = [c for c, dt in zip(df.columns, df.dtypes) if dt == pl.Utf8]
    if str_cols:
        df = df.with_columns([pl.col(c).str.strip_chars().alias(c) for c in str_cols])

    return df


def apply_format(value, format_dict):
    """
    Apply SAS-style format mapping.

    SAS $8. formatted values are right-padded with blanks, but SAS trims
    trailing blanks automatically on character comparison/merge. Python
    does not, so we return the STRIPPED value here and keep every ITEM
    key stripped everywhere downstream (read_template, read_gl, this
    function). Mixing padded and unpadded ITEM strings breaks both the
    template join and the PART2-based subtotal keys.
    """
    return format_dict.get(value, '').strip()


def parse_cents(amount_str, sign_str):
    """
    Parse a COMMA20.2-style amount string plus its sign column into
    integer cents. Uses Decimal (not float) for the string->number step
    so there's no binary-float rounding noise introduced at the very
    first point the value enters the pipeline. A blank sign column means
    negative, matching the original SAS convention (`IF SIGN = ' ' THEN
    AMOUNT = -1*AMOUNT;`).
    """
    try:
        amt = Decimal(amount_str.replace(',', '').strip())
    except (InvalidOperation, AttributeError):
        return 0
    if sign_str == ' ':
        amt = -amt
    return int((amt * CENTS).to_integral_value())


def to_cents_expr(col_name):
    """Polars expression: convert a float64 dollar column to Int64 cents."""
    return (pl.col(col_name) * CENTS).round(0).cast(pl.Int64).alias(col_name)


def sum_null_aware(col_name):
    """
    Polars agg expression: sum a cents column, but return NULL (not 0)
    when every value contributing to the group is null. This mirrors
    SAS's SUM statistic, which returns MISSING (not 0) when there is no
    non-missing value to sum. Without this, a bucket that was never
    allocated for a given item prints "0.00" instead of staying blank,
    which is not what production output does.
    """
    return (
        pl.when(pl.col(col_name).count() == 0)
        .then(None)
        .otherwise(pl.col(col_name).sum())
        .alias(col_name)
    )


def format_number(value):
    """
    Format an integer-cents value as a comma-separated currency string
    with 2 decimals. Returns '' for missing (None) values -- matching
    SAS's behavior of printing MISSING numerics as blank.
    """
    if value is None:
        return ''
    try:
        cents = int(value)
    except (TypeError, ValueError):
        return ''
    dollars = Decimal(cents) / Decimal(CENTS)
    return f"{dollars:,.2f}"


# ============================================================================
# CALCULATE REPORTING PARAMETERS (REPTDATE = TODAY - 1 DAY)
# ============================================================================

def get_reptdate():
    """
    Calculate the reporting date and derived parameters.
    REPTDATE is no longer read from an input file; it is calculated as
    (current date - 1 day), matching a SAS `REPTDATE = TODAY() - 1` approach.
    """
    print("Calculating REPTDATE as (today - 1 day)...")

    reptdate = datetime.now().date() - timedelta(days=1)

    reptmon = f"{reptdate.month:02d}"
    fildt = reptdate.strftime("%d%m%y")
    rdate = reptdate.strftime("%d/%m/%Y")

    print(f"Report Date: {rdate}")
    print(f"Report Month: {reptmon}")

    return reptdate, reptmon, fildt, rdate


# ============================================================================
# READ TEMPLATE FILE
# ============================================================================

def read_template():
    """Read template file with item descriptions."""
    print("Reading template...")

    template_data = []
    recno = 1

    with open(TEMPLATE_PATH, 'r') as f:
        for line in f:
            if len(line) >= 10:
                item = line[0:8].strip().upper()
                idesc = line[9:129] if len(line) >= 129 else line[9:].rstrip('\n')
                template_data.append({
                    'ITEM': item,
                    'IDESC': idesc,
                    'RECNO': recno,
                    'AMOUNT': None,
                    'BUC1': None,
                    'BUC2': None,
                    'BUC3': None,
                    'BUC4': None,
                    'BUC5': None,
                    'BUC6': None,
                    'BUC7': None,
                })
                recno += 1

    df = pl.DataFrame(template_data)
    # AMOUNT/BUC1-7 start as all-None; pin them to Int64 (cents) so later
    # joins/coalesces against the cents-typed summary tables don't hit a
    # dtype mismatch.
    df = df.with_columns([
        pl.col(c).cast(pl.Int64) for c in
        ['AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']
    ])
    print(f"Template records: {len(df)}")
    return df


# ============================================================================
# READ AND PROCESS GL (WALK FILE)
# ============================================================================

def read_gl():
    """Read and process GL data from WALK file."""
    print("Reading GL data...")

    gl_data = []

    with open(WALK_PATH, 'r') as f:
        for line in f:
            if len(line) >= 62:
                set_id = line[1:20].strip()
                amount_str = line[41:61].strip()
                sign = line[61:62]

                amount = parse_cents(amount_str, sign)

                item = apply_format(set_id, GLCOFFMT)

                if item.strip():
                    buc5 = amount if set_id in ('F142199C', 'F142199D') else None
                    buc6 = amount if set_id in ('F144111RM', 'F141301', 'F147100',
                                                'F144140CAGA', 'F247610') else None
                    buc7 = amount if buc5 is None and buc6 is None else None

                    gl_data.append({
                        'SET_ID': set_id,
                        'AMOUNT': amount,
                        'ITEM': item.strip(),
                        'BUC1': None,
                        'BUC2': None,
                        'BUC3': None,
                        'BUC4': None,
                        'BUC5': buc5,
                        'BUC6': buc6,
                        'BUC7': buc7,
                    })

    df = pl.DataFrame(gl_data)

    # BUC1-4 are always None at this stage (GL only ever populates BUC5-7),
    # so Polars infers a Null-typed column; pin it to Int64 (cents) so it
    # matches the other cents-typed tables it will later be concatenated
    # and joined with.
    df = df.with_columns([
        pl.col(c).cast(pl.Int64) for c in
        ['AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']
    ])

    # Add PART1 and PART2
    df = df.with_columns([
        pl.col('ITEM').str.slice(0, 1).alias('PART1'),
        pl.col('ITEM').str.slice(4, 4).alias('PART2'),
    ])

    # Aggregate by ITEM (null-aware sums: a bucket never touched by any
    # contributing GL record stays NULL/blank rather than becoming 0.00)
    df = df.group_by('ITEM').agg([
        sum_null_aware('AMOUNT'),
        sum_null_aware('BUC1'),
        sum_null_aware('BUC2'),
        sum_null_aware('BUC3'),
        sum_null_aware('BUC4'),
        sum_null_aware('BUC5'),
        sum_null_aware('BUC6'),
        sum_null_aware('BUC7'),
        pl.col('PART1').first(),
        pl.col('PART2').first(),
    ])

    print(f"GL records: {len(df)}")
    return df


# ============================================================================
# READ AND PROCESS COF DATA
# ============================================================================

def read_cof_data(reptmon):
    """Read CMM and EQU data and create initial COF dataset."""
    print("Reading COF data (CMM and EQU)...")

    cmm_path = Path(str(CMM_PATH_TEMPLATE).format(reptmon))
    equ_path = Path(str(EQU_PATH_TEMPLATE).format(reptmon))

    # Read CMM data
    df_cmm = read_sas(cmm_path)

    # Read EQU data and rename CUSTNO to CUSTEQNO
    df_equ = read_sas(equ_path)
    df_equ = df_equ.rename({'CUSTNO': 'CUSTEQNO'})

    # Combine datasets
    df = pl.concat([df_cmm, df_equ], how='diagonal_relaxed')

    # Convert AMOUNT to integer cents immediately, before any summing, so
    # every downstream aggregation is exact integer addition.
    df = df.with_columns(to_cents_expr('AMOUNT'))

    # Add TAG = 1 (TOTAL LIABILITIES)
    df = df.with_columns(pl.lit(1).alias('TAG'))

    # Aggregate by CMMCODE and TAG
    df = df.group_by(['CMMCODE', 'TAG']).agg([
        sum_null_aware('AMOUNT')
    ])

    print(f"COF records (TAG=1): {len(df)}")
    return df


# ============================================================================
# READ LIST FILES AND CREATE EXCLUSION/INCLUSION LISTS
# ============================================================================

def read_list_files():
    """Read list files and create customer/IC lists."""
    print("Reading list files...")

    # INTRA GROUP - INTRAIC
    df_intra = read_sas(INTRA_GROUP_PATH)
    intraic = df_intra.filter(pl.col('BUSSREG') != '').select('BUSSREG')['BUSSREG'].to_list()

    # INTRA GROUP - INTRACUS
    intracus = df_intra.filter(pl.col('CUSTNO').is_not_null()).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - RELCUS
    df_related = read_sas(RELATED_PARTY_PATH)
    relcus = df_related.filter(pl.col('CUSTNO').is_not_null()).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - XRELCUS (exclusions)
    xrelcus = df_related.filter(
        pl.col('ICNEW').str.slice(0, 1) == '-'
    ).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - RELIC
    relic = df_related.filter(pl.col('ICNEW') != '').select('ICNEW')['ICNEW'].to_list()

    # EQU INTRA GROUP - INTRAEQ
    df_equ_intra = read_sas(EQU_INTRA_GROUP_PATH)
    intraeq = df_equ_intra.filter(pl.col('CUSTNO') != '').select('CUSTNO')['CUSTNO'].to_list()

    # EQU RELATED PARTY - RELEQ
    df_equ_related = read_sas(EQU_RELATED_PARTY_PATH)
    releq = df_equ_related.filter(pl.col('CUSTNO') != '').select('CUSTNO')['CUSTNO'].to_list()

    print(f"INTRAIC: {len(intraic)}, INTRACUS: {len(intracus)}")
    print(f"RELCUS: {len(relcus)}, XRELCUS: {len(xrelcus)}, RELIC: {len(relic)}")
    print(f"INTRAEQ: {len(intraeq)}, RELEQ: {len(releq)}")

    return {
        'INTRAIC': set(intraic),
        'INTRACUS': set(intracus),
        'RELCUS': set(relcus),
        'XRELCUS': set(xrelcus),
        'RELIC': set(relic),
        'INTRAEQ': set(intraeq),
        'RELEQ': set(releq),
    }


# ============================================================================
# CREATE COF23 (TAG 2 AND 3)
# ============================================================================

def create_cof23(reptmon, lists):
    """Create COF data with TAG 2 (INTRA GROUP) and TAG 3 (RELATED PARTY)."""
    print("Creating COF23 (TAG 2 and 3)...")

    cmm_path = Path(str(CMM_PATH_TEMPLATE).format(reptmon))
    equ_path = Path(str(EQU_PATH_TEMPLATE).format(reptmon))

    # Read CMM data
    df_cmm = read_sas(cmm_path)

    # Read EQU data and rename CUSTNO to CUSTEQNO
    df_equ = read_sas(equ_path)
    df_equ = df_equ.rename({'CUSTNO': 'CUSTEQNO'})

    # Add CUSTEQNO column to CMM records (fill with None), and CUSTNO to EQU records
    if 'CUSTEQNO' not in df_cmm.columns:
        df_cmm = df_cmm.with_columns(pl.lit(None).alias('CUSTEQNO'))
    if 'CUSTNO' not in df_equ.columns:
        df_equ = df_equ.with_columns(pl.lit(None).alias('CUSTNO'))

    # Combine datasets
    df = pl.concat([df_cmm, df_equ], how='diagonal_relaxed')

    # Convert AMOUNT to integer cents immediately, before any summing.
    df = df.with_columns(to_cents_expr('AMOUNT'))

    # Create TAG based on conditions
    def assign_tag(row):
        custno = row.get('CUSTNO')
        custeqno = row.get('CUSTEQNO')
        newic = row.get('NEWIC', '')

        # TAG 3: RELATED PARTY
        if (custno in lists['RELCUS'] or
                newic in lists['RELIC'] or
                custeqno in lists['RELEQ']):
            return 3

        # TAG 2: INTRA GROUP
        if (custno in lists['INTRACUS'] or
                (newic in lists['INTRAIC'] and custno not in lists['XRELCUS']) or
                custeqno in lists['INTRAEQ']):
            return 2

        return None

    # Apply tag assignment
    tags = []
    for row in df.iter_rows(named=True):
        tags.append(assign_tag(row))

    df = df.with_columns(pl.Series('TAG', tags))

    # Filter only records with TAG 2 or 3
    df = df.filter(pl.col('TAG').is_in([2, 3]))

    # Aggregate by CMMCODE and TAG
    df = df.group_by(['CMMCODE', 'TAG']).agg([
        sum_null_aware('AMOUNT')
    ])

    print(f"COF23 records: {len(df)}")
    return df


# ============================================================================
# CREATE COF123 (COMBINE AND PROCESS)
# ============================================================================

def create_cof123(cof, cof23):
    """Combine COF and COF23, apply formatting and bucket allocation."""
    print("Creating COF123...")

    # Combine datasets
    df = pl.concat([cof, cof23], how='diagonal_relaxed')

    # Extract parts of CMMCODE
    df = df.with_columns([
        pl.col('CMMCODE').str.slice(0, 5).alias('BIC'),
        pl.col('CMMCODE').str.slice(5, 2).alias('CUST'),
        pl.col('CMMCODE').str.slice(7, 2).alias('REM'),
        pl.col('CMMCODE').str.slice(9, 2).alias('ECP'),
    ])

    # Apply format based on TAG
    def get_item(bic, tag):
        if tag == 1:
            return apply_format(bic, COFF1FMT)
        elif tag == 2:
            return apply_format(bic, COFF2FMT)
        elif tag == 3:
            return apply_format(bic, COFF3FMT)
        return ''

    items = []
    for row in df.iter_rows(named=True):
        items.append(get_item(row['BIC'], row['TAG']))

    df = df.with_columns(pl.Series('ITEM', items))

    # Filter only records with valid ITEM
    df = df.filter(pl.col('ITEM') != '')

    # Override REM for specific BICs
    df = df.with_columns(
        pl.when(pl.col('BIC').is_in(['95312', '95313', '96313', '9531X']))
        .then(pl.lit('07'))
        .otherwise(pl.col('REM'))
        .alias('REM')
    )

    # AMOUNT is already integer cents (converted at read time in
    # read_cof_data/create_cof23), so no further rounding is needed here.

    # Allocate AMOUNT to buckets based on REM
    df = df.with_columns([
        pl.when(pl.col('REM') == '01').then(pl.col('AMOUNT')).otherwise(None).alias('BUC1'),
        pl.when(pl.col('REM') == '02').then(pl.col('AMOUNT')).otherwise(None).alias('BUC2'),
        pl.when(pl.col('REM') == '03').then(pl.col('AMOUNT')).otherwise(None).alias('BUC3'),
        pl.when(pl.col('REM') == '04').then(pl.col('AMOUNT')).otherwise(None).alias('BUC4'),
        pl.when(pl.col('REM') == '05').then(pl.col('AMOUNT')).otherwise(None).alias('BUC5'),
        pl.when(pl.col('REM') == '06').then(pl.col('AMOUNT')).otherwise(None).alias('BUC6'),
        pl.when(pl.col('REM') == '07').then(pl.col('AMOUNT')).otherwise(None).alias('BUC7'),
    ])

    print(f"COF123 records: {len(df)}")
    return df


# ============================================================================
# CREATE COF45 (RETAIL/WHOLESALE BREAKDOWN)
# ============================================================================

def create_cof45(cof123):
    """Create COF45 for retail/wholesale funding breakdown."""
    print("Creating COF45...")

    # Filter TAG = 1 only
    df = cof123.filter(pl.col('TAG') == 1)

    # Apply format based on CUST
    def get_item_45(bic, cust, ecp):
        if cust == '08':
            item = apply_format(bic, COFF4FMT)
        else:
            item = apply_format(bic, COFF5FMT)

        # Operational adjustment
        if item == '5.02' and ecp == '01':
            item = '5.03'

        return item

    items = []
    for row in df.iter_rows(named=True):
        items.append(get_item_45(row['BIC'], row['CUST'], row['ECP']))

    df = df.with_columns(pl.Series('ITEM', items))

    # Filter only records with valid ITEM
    df = df.filter(pl.col('ITEM') != '')

    print(f"COF45 records: {len(df)}")
    return df


# ============================================================================
# PROCESS VOSTRO DATA
# ============================================================================

def process_vostro(lists):
    """Process VOSTRO data and merge with CISINFO."""
    print("Processing VOSTRO data...")

    df_vostro = read_sas(VOSTRO_PATH)
    df_cisinfo = read_sas(CISINFO_PATH)

    # Merge VOSTRO with CISINFO
    df = df_vostro.join(df_cisinfo, on='ACCTNO', how='left')

    # Filter based on customer lists
    def is_intra_group(row):
        custno = row.get('CUSTNO')
        custeqno = row.get('CUSTEQNO')
        newic = row.get('NEWIC', '')

        return (custno in lists['INTRACUS'] or
                (newic in lists['INTRAIC'] and custno not in lists['XRELCUS']) or
                custeqno in lists['INTRAEQ'])

    keep_flags = []
    for row in df.iter_rows(named=True):
        keep_flags.append(is_intra_group(row))

    df = df.with_columns(pl.Series('_KEEP', keep_flags))
    df = df.filter(pl.col('_KEEP'))

    # Convert AMOUNT to integer cents before it's used as BUC7.
    df = df.with_columns(to_cents_expr('AMOUNT'))

    # Create output columns
    df = df.with_columns([
        pl.lit('2.09I').alias('ITEM'),
        pl.col('AMOUNT').alias('BUC7'),
    ])

    df = df.select(['ITEM', 'BUC7', 'AMOUNT'])

    print(f"VOSTRO records: {len(df)}")
    return df


# ============================================================================
# AGGREGATE AND CREATE SUMMARIES
# ============================================================================

def create_summaries(cof_combined):
    """Create item-level, subtotal, and total summaries."""
    print("Creating summaries...")

    # Add derived columns
    cof_combined = cof_combined.with_columns([
        pl.col('ITEM').str.slice(0, 1).alias('PART1'),
        pl.col('ITEM').str.slice(4, 4).alias('PART2'),
        pl.col('ITEM').str.slice(0, 4).alias('PREFIX'),
    ])

    # COFITEM - Item level summary (null-aware: a bucket untouched by any
    # contributing record for this item stays blank rather than "0.00")
    cofitem = cof_combined.group_by('ITEM').agg([
        sum_null_aware('AMOUNT'),
        sum_null_aware('BUC1'),
        sum_null_aware('BUC2'),
        sum_null_aware('BUC3'),
        sum_null_aware('BUC4'),
        sum_null_aware('BUC5'),
        sum_null_aware('BUC6'),
        sum_null_aware('BUC7'),
    ])

    # COFSUBTOT - Subtotal by PART1 and PART2
    # NOTE: SAS `PROC SUMMARY ... CLASS PART1 PART2;` excludes rows where a
    # CLASS variable is missing/blank, UNLESS the MISSING option is used
    # (it isn't, in the original code). Items with no "I"/"II" suffix
    # (e.g. 4.01-4.03, 5.01-5.03) have a blank PART2, so SAS never produces
    # an "X.00" subtotal for that group. Polars' group_by has no such
    # default exclusion, so we filter blank PART2 out explicitly here to
    # match SAS's behavior. Without this, an "X.00"-coded template row
    # (e.g. a section header) can wrongly pick up a real total.
    cofsubtot = cof_combined.filter(pl.col('PART2') != '').group_by(['PART1', 'PART2']).agg([
        sum_null_aware('AMOUNT'),
        sum_null_aware('BUC1'),
        sum_null_aware('BUC2'),
        sum_null_aware('BUC3'),
        sum_null_aware('BUC4'),
        sum_null_aware('BUC5'),
        sum_null_aware('BUC6'),
        sum_null_aware('BUC7'),
    ])

    # COFTOT - Total by PART1
    coftot = cof_combined.group_by('PART1').agg([
        sum_null_aware('AMOUNT'),
        sum_null_aware('BUC1'),
        sum_null_aware('BUC2'),
        sum_null_aware('BUC3'),
        sum_null_aware('BUC4'),
        sum_null_aware('BUC5'),
        sum_null_aware('BUC6'),
        sum_null_aware('BUC7'),
    ])

    # COFSPCL - Special summary for operational/non-operational
    cofspcl = cof_combined.filter(
        pl.col('PREFIX').is_in(['5.04', '5.05'])
    ).group_by('PREFIX').agg([
        sum_null_aware('AMOUNT'),
        sum_null_aware('BUC1'),
        sum_null_aware('BUC2'),
        sum_null_aware('BUC3'),
        sum_null_aware('BUC4'),
        sum_null_aware('BUC5'),
        sum_null_aware('BUC6'),
        sum_null_aware('BUC7'),
    ])

    # Create ITEM for totals
    coftot = coftot.with_columns(
        (pl.col('PART1') + pl.lit('.99')).alias('ITEM')
    )

    cofsubtot = cofsubtot.with_columns(
        (pl.col('PART1') + pl.lit('.00') + pl.col('PART2')).alias('ITEM')
    )

    cofspcl = cofspcl.with_columns(
        pl.col('PREFIX').alias('ITEM')
    )

    # Combine all totals
    coftot_combined = pl.concat([
        coftot.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
        cofsubtot.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
        cofspcl.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
    ])

    print(f"COFITEM: {len(cofitem)}, COFTOT: {len(coftot_combined)}")
    return cofitem, coftot_combined


# ============================================================================
# GENERATE REPORT
# ============================================================================

def generate_report(template, cofitem, coftot_combined, rdate):
    """Generate final COF report with ASA carriage control."""
    print("Generating report...")

    # Merge template with item and total data
    df = template.join(cofitem, on='ITEM', how='left', suffix='_item')
    df = df.join(coftot_combined, on='ITEM', how='left', suffix='_tot')

    # Coalesce columns (prefer item values, then tot values, then template defaults)
    for col in ['AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']:
        item_col = f'{col}_item' if f'{col}_item' in df.columns else col
        tot_col = f'{col}_tot' if f'{col}_tot' in df.columns else None

        if tot_col and tot_col in df.columns:
            df = df.with_columns(
                pl.coalesce([item_col, tot_col, pl.col(col)]).alias(col)
            )
        elif item_col in df.columns:
            df = df.with_columns(
                pl.coalesce([item_col, pl.col(col)]).alias(col)
            )

    # Sort by RECNO
    df = df.sort('RECNO')

    # Write output as comma-delimited text (matches production output).
    # NOTE: the original SAS used a non-printable ASCII 0x05 (ENQ) byte as
    # the field delimiter (`DLM='05'X`). That control character renders as
    # a stray box/garbled symbol in ordinary text viewers and doesn't match
    # production's actual output, which is comma-delimited with numeric
    # fields quoted (since the numbers themselves contain thousand-separator
    # commas). We use a real comma here and quote only the numeric fields.
    delimiter = ','

    def quoted(value):
        """Wrap a formatted number in quotes if non-empty, else leave blank
        (matches production: quotes appear only where a value is present)."""
        return f'"{value}"' if value else ''

    with open(COF_OUTPUT_PATH, 'w') as f:
        # Header lines with ASA carriage control
        f.write(' PUBLIC BANK BERHAD\n')
        f.write(f' LIQUIDITY COVERAGE RATIO (LCR) TABLE 4 AS AT {rdate}\n')
        f.write(' CONCENTRATION OF FUNDING\n')

        # Data rows
        for row in df.iter_rows(named=True):
            idesc = row['IDESC']

            # Print blank line if IDESC starts with non-blank
            if idesc and len(idesc) >= 2 and idesc[0:2].strip():
                f.write(' \n')

            # Print data line
            line_parts = [
                ' ',  # ASA carriage control (space = single spacing)
                idesc if idesc else '',
                delimiter,
                quoted(format_number(row.get('BUC1'))),
                delimiter,
                quoted(format_number(row.get('BUC2'))),
                delimiter,
                quoted(format_number(row.get('BUC3'))),
                delimiter,
                quoted(format_number(row.get('BUC4'))),
                delimiter,
                quoted(format_number(row.get('BUC5'))),
                delimiter,
                quoted(format_number(row.get('BUC6'))),
                delimiter,
                quoted(format_number(row.get('BUC7'))),
                delimiter,
                quoted(format_number(row.get('AMOUNT'))),
                delimiter,
            ]
            f.write(''.join(line_parts) + '\n')

            # Print column headers if IDESC starts with non-blank
            if idesc and len(idesc) >= 2 and idesc[0:2].strip():
                header_parts = [
                    ' ',  # ASA carriage control
                    'Deposit Type',
                    delimiter,
                    'up to 1 week',
                    delimiter,
                    '> 1 wk - 1 mth',
                    delimiter,
                    '> 1 - 3 mths',
                    delimiter,
                    '> 3 - 6 mths',
                    delimiter,
                    '> 6 mths -  1 yr',
                    delimiter,
                    '> 1 year',
                    delimiter,
                    'No specific maturity',
                    delimiter,
                    'Total',
                ]
                f.write(''.join(header_parts) + '\n')

    print(f"Report written to {COF_OUTPUT_PATH}")


# ============================================================================
# GENERATE SFTP SCRIPT
# ============================================================================

def generate_sftp_script(fildt):
    """Generate SFTP script file."""
    print("Generating SFTP script...")

    with open(SFTP_SCRIPT_PATH, 'w') as f:
        f.write(f"put //SAP.PBB.LCR.COF.TEXT  COF_{fildt}.XLS\n")

    print(f"SFTP script written to {SFTP_SCRIPT_PATH}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("LCR Concentration of Funding Report Generator")
        print("=" * 80)

        # Calculate reptdate (today - 1 day; no longer read from a file)
        reptdate, reptmon, fildt, rdate = get_reptdate()

        # Read template
        template = read_template()

        # Read GL data
        gl = read_gl()

        # Read COF data
        cof = read_cof_data(reptmon)

        # Read list files
        lists = read_list_files()

        # Create COF23
        cof23 = create_cof23(reptmon, lists)

        # Create COF123
        cof123 = create_cof123(cof, cof23)

        # Create COF45
        cof45 = create_cof45(cof123)

        # Process VOSTRO
        vostro = process_vostro(lists)

        # Combine all COF data
        cof_combined = pl.concat([
            cof123.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
            cof45.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
            gl.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
            vostro,
        ], how='diagonal_relaxed')

        print(f"Total COF combined records: {len(cof_combined)}")

        # Create summaries
        cofitem, coftot_combined = create_summaries(cof_combined)

        # Generate report
        generate_report(template, cofitem, coftot_combined, rdate)

        # Generate SFTP script
        generate_sftp_script(fildt)

        print("=" * 80)
        print("Processing completed successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
