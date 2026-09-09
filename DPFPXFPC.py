#!/usr/bin/env python3
"""
File Name: EIBTNPGS
Non-Performing Government Scheme Trade Finance Processing

Corrected version:
  - Step 2 (CRFT) and Step 10 (MICR) now use fixed-column parsing
    (str.slice) to mirror SAS's column-based INFILE/INPUT statements,
    instead of whitespace-splitting which misaligns fields whenever
    padding between values varies from line to line.
  - All ACCTNO / CENSUS / CVAR01 / CVAR06 / BRANCH columns coming out
    of pyreadstat (which always returns SAS numerics as float64) are
    explicitly rounded and cast to Int64 immediately after each read,
    so every downstream join has matching key dtypes.
  - String join keys sourced from .sas7bdat files (SUBACCT, TRANSREF,
    TRANSREX, BRANCH-as-text where relevant) are stripped of the
    trailing padding SAS stores them with, so they compare equal to
    the stripped values produced from the fixed-width text parses.
  - The SUBA1/SUBA2 -> SUBALMT merge uses how='full' with
    coalesce=True (polars' current name for a two-sided SAS MERGE),
    since 'outer' is deprecated and previously left duplicate
    ACCTNO/ACCTNO_right columns behind.
"""

import duckdb
import polars as pl
import pyreadstat
from datetime import datetime, timedelta
from pathlib import Path
import calendar
import saspy


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
CRFTABL_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/crftabl.txt")
BTRSA_MAST_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/mast{reptday}{reptmon}.sas7bdat")
BTRSA_CRED_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/cred{reptday}{reptmon}.sas7bdat")
BTRSA_PROV_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/prov{reptday}{reptmon}.sas7bdat")
BTRSA_SUBA_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/suba{reptday}{reptmon}.sas7bdat")
COLL_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_{reptyear}{reptmon}{reptday}.sas7bdat")
DESC_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_desc_{reptyear}{reptmon}{reptday}.sas7bdat")
MICR_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/BOPESS.txt")
NPLA_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/npla.sas7bdat")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBTNPGS")

OUTPUT_FILE = None  # Set after determining report date


# ============================================================================
# HELPERS
# ============================================================================
def sas_int(col: str) -> pl.Expr:
    """
    pyreadstat returns every SAS numeric as float64. Round (to absorb
    floating point noise) and cast to Int64 so these columns can be
    safely used as join keys against integers parsed elsewhere.
    """
    return pl.col(col).round(0).cast(pl.Int64, strict=False)


def strip_str(col: str) -> pl.Expr:
    """
    SAS character variables are fixed-width and space padded.
    pyreadstat preserves that padding, so strip it before using the
    column as a join key or comparing it to a value parsed from a
    stripped, fixed-column text extraction.
    """
    return pl.col(col).cast(pl.Utf8).str.strip_chars()


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# STEP 1: SET REPORT DATE (using yesterday's date)
# ============================================================================
print("Step 1: Setting report date...")

reptdate = datetime.now() - timedelta(days=1)

REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"
RDATE = (reptdate - datetime(1960, 1, 1)).days  # SAS date value

print(f"Report Date: {reptdate}, RDATE: {RDATE}")

OUTPUT_FILE = OUTPUT_DIR / f"btnpgs{REPTMON}.sas7bdat"

BTRSA_MAST_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/mast{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_CRED_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/cred{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_PROV_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/prov{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_SUBA_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/suba{REPTDAY}{REPTMON}.sas7bdat")

COLL_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat")
DESC_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_desc_{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat")


# ============================================================================
# STEP 2: PROCESS CRFTABL (Credit Facility Table) - FIXED-COLUMN PARSING
# ============================================================================
print("Step 2: Processing credit facility table...")

# SAS:
#   INPUT @001  RECTYP1 $1. @;
#   IF RECTYP1='1' THEN DELETE;    ELSE
#   INPUT  @004 TFID        $8.
#          @012 SUBACCT     $5.
#          @365 PREIND      $1.
#          @368 CENSUST      1.
#          @377 ACCTNO      10.;
# SAS column N is 1-indexed; Python str.slice offsets are 0-indexed,
# so offset = N - 1.

with open(CRFTABL_FILE, 'r') as f:
    lines = f.readlines()

print("First few lines of crftabl.txt:")
for i, line in enumerate(lines[:5]):
    print(f"Line {i}: '{line.rstrip()}'")

crft_data = pl.DataFrame({'data': [line.rstrip('\n') for line in lines]})

crft_data = crft_data.with_columns([
    pl.col('data').str.slice(0, 1).alias('RECTYP1'),                                     # @001 $1.
    pl.col('data').str.slice(3, 8).str.strip_chars().alias('TFID'),                       # @004 $8.
    pl.col('data').str.slice(11, 5).str.strip_chars().alias('SUBACCT'),                   # @012 $5.
    pl.col('data').str.slice(364, 1).alias('PREIND'),                                     # @365 $1.
    pl.col('data').str.slice(367, 1).cast(pl.Int64, strict=False).alias('CENSUST'),       # @368 1.
    pl.col('data').str.slice(376, 10).str.strip_chars().cast(pl.Int64, strict=False).alias('ACCTNO'),  # @377 10.
]).select(['RECTYP1', 'TFID', 'SUBACCT', 'PREIND', 'CENSUST', 'ACCTNO'])

# Filter out header/record-type '1' rows
crft_data = crft_data.filter(pl.col('RECTYP1') != '1')


def assign_sch(censust):
    """Assign scheme code based on census type"""
    if censust == 3:
        return 'P51'
    elif censust == 4:
        return 'P72'
    elif censust == 5:
        return 'P65'
    elif censust == 6:
        return 'P85'
    elif censust == 7:
        return 'P53'
    else:
        return '   '


crft_data = crft_data.with_columns([
    pl.struct(['CENSUST']).map_elements(
        lambda x: assign_sch(x['CENSUST']),
        return_dtype=pl.Utf8
    ).alias('SCH')
])

crft_data = crft_data.filter(pl.col('SCH') != '   ')

crft_data = crft_data.unique(subset=['ACCTNO', 'CENSUST', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 3: MERGE WITH MAST (Master Account Data)
# ============================================================================
print("Step 3: Merging with master account data...")

mast_df, mast_meta = pyreadstat.read_sas7bdat(BTRSA_MAST_FILE)
mast_data = pl.from_pandas(mast_df).select(['ACCTNO', 'FICODE', 'NAME', 'BUSREGN'])

mast_data = mast_data.with_columns([
    sas_int('ACCTNO'),
    sas_int('FICODE'),
    strip_str('NAME').alias('NAME'),
    strip_str('BUSREGN').alias('BUSREGN'),
]).unique(subset=['ACCTNO'], keep='first')

crft_merged = crft_data.join(mast_data, on='ACCTNO', how='inner')

crft_merged = crft_merged.with_columns([
    pl.col('FICODE').alias('BRANCH')
])

crft_merged = crft_merged.filter(pl.col('ACCTNO') > 0)

crft_final = crft_merged.select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

crft1_data = crft_merged.with_columns([
    (pl.lit('FAC') + pl.col('SUBACCT').str.slice(0, 1)).alias('SUBACCT')
]).select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 4: PROCESS CREDIT DATA
# ============================================================================
print("Step 4: Processing credit data...")

cred_df, cred_meta = pyreadstat.read_sas7bdat(BTRSA_CRED_FILE)
cred_data = pl.from_pandas(cred_df)

cred_data = cred_data.with_columns([
    sas_int('ACCTNO'),
    strip_str('SUBACCT').alias('SUBACCT'),
    strip_str('TRANSREF').alias('TRANSREF'),
])

cred_data = cred_data.join(crft_final, on=['ACCTNO', 'SUBACCT'], how='inner')

# SAS compares TRANSREF to two literal blanks - i.e. "not blank" once stripped
cred_data = cred_data.filter(
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('TRANSREF') != '')
)

cred_data = cred_data.with_columns([
    pl.col('TRANSREF').str.slice(0, 7).alias('TRANSREX')
])

cred_data = cred_data.unique(subset=['ACCTNO', 'TRANSREF'], keep='first')


# ============================================================================
# STEP 5: SUMMARIZE CREDIT OUTSTAND (CRED1)
# ============================================================================
print("Step 5: Summarizing credit outstanding...")

cred1_data = cred_data.filter(
    pl.col('SUBACCT').str.slice(1, 3) != 'SGL'
).group_by('ACCTNO').agg([
    pl.col('OUTSTAND').sum().alias('OUTSTAND')
])


# ============================================================================
# STEP 6: PROCESS PROVISION DATA (CRED2)
# ============================================================================
print("Step 6: Processing provision data...")

prov_df, prov_meta = pyreadstat.read_sas7bdat(BTRSA_PROV_FILE)
prov_data = pl.from_pandas(prov_df)

prov_data = prov_data.with_columns([
    sas_int('ACCTNO'),
    strip_str('TRANSREX').alias('TRANSREX'),
    strip_str('NPLIND').alias('NPLIND'),
]).filter(~pl.col('NPLIND').is_in(['P', 'F']))

cred2_data = prov_data.join(
    cred_data.select(['ACCTNO', 'TRANSREX', 'SUBACCT', 'OUTSTAND']),
    on=['ACCTNO', 'TRANSREX'],
    how='inner'
)

cred2_data = cred2_data.filter(
    ~pl.col('SUBACCT').str.slice(0, 3).is_in(['OV ', 'FAC'])
)

cred2_data = cred2_data.sort(['ACCTNO', 'MATUREDS']).unique(
    subset=['ACCTNO'], keep='first'
)


# SAS: ARREARS = INPUT(NODAYS, NDAYS.)
# NDAYS. is a custom informat defined in PBBLNFMT (PROC FORMAT/INVALUE),
# confirmed contents below. It is NOT a simple 30/60/90/120/150/180/365
# ladder - it's a ~monthly (30-31 day) ladder from bucket 0 (<=30 days)
# up through bucket 24 (>=730 days), with SAS ranges being INCLUSIVE on
# both ends (SAS INVALUE '-' ranges are inclusive).
_NDAYS_TABLE = [
    (None, 30, 0),
    (31, 59, 1),
    (60, 89, 2),
    (90, 121, 3),
    (122, 151, 4),
    (152, 182, 5),
    (183, 213, 6),
    (214, 243, 7),
    (244, 273, 8),
    (274, 303, 9),
    (304, 333, 10),
    (334, 364, 11),
    (365, 394, 12),
    (395, 424, 13),
    (425, 456, 14),
    (457, 486, 15),
    (487, 516, 16),
    (517, 547, 17),
    (548, 577, 18),
    (578, 608, 19),
    (609, 638, 20),
    (639, 668, 21),
    (669, 698, 22),
    (699, 729, 23),
    (730, None, 24),
]


def calculate_arrears(nodays):
    """
    Translate NODAYS into an arrears bucket using the verified NDAYS.
    informat table from PBBLNFMT (LOW-30=0 ... 730-HIGH=24).
    """
    for low, high, result in _NDAYS_TABLE:
        lo_ok = (low is None) or (nodays >= low)
        hi_ok = (high is None) or (nodays <= high)
        if lo_ok and hi_ok:
            return result
    return 24  # fall back to the open-ended top bucket, defensive only


cred2_data = cred2_data.with_columns([
    pl.when((pl.col('MATUREDS').is_not_null()) & (pl.col('MATUREDS') > 0) & (pl.lit(RDATE) > pl.col('MATUREDS')))
    .then((pl.lit(RDATE) - pl.col('MATUREDS')) + 1)
    .otherwise(0).alias('NODAYS')
])

cred2_data = cred2_data.with_columns([
    pl.when(pl.col('NODAYS') > 0)
    .then(pl.struct(['NODAYS']).map_elements(
        lambda x: calculate_arrears(x['NODAYS']),
        return_dtype=pl.Int64
    ))
    .otherwise(0).alias('ARREARS')
])

# SAS: IF ARREARS=24 THEN ARREARS=ROUND(NODAYS/30);
# The top NDAYS. bucket (730+ days) is open-ended, so once a record
# lands in bucket 24 the SAS code recomputes ARREARS as an actual
# month count instead of leaving it pinned at 24.
cred2_data = cred2_data.with_columns([
    pl.when(pl.col('ARREARS') == 24)
    .then((pl.col('NODAYS') / 30).round(0).cast(pl.Int64))
    .otherwise(pl.col('ARREARS')).alias('ARREARS')
])

cred2_final = cred2_data.select(['ACCTNO', 'ARREARS', 'MATUREDS', 'NODAYS'])


# ============================================================================
# STEP 7: PROCESS SUBACCOUNT DATA (SUBA)
# ============================================================================
print("Step 7: Processing subaccount data...")

suba_df, suba_meta = pyreadstat.read_sas7bdat(BTRSA_SUBA_FILE)
suba_data = pl.from_pandas(suba_df)

suba_data = suba_data.with_columns([
    sas_int('ACCTNO'),
    strip_str('SUBACCT').alias('SUBACCT'),
    strip_str('TRANSREF').alias('TRANSREF'),
])

suba_data = suba_data.join(crft1_data, on=['ACCTNO', 'SUBACCT'], how='inner')

suba1_data = suba_data.filter(
    pl.col('SUBACCT').str.slice(0, 3) == 'FAC'
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

suba1_summary = suba1_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMTCURM')
])

# SAS compares TRANSREF to two literal blanks here too
suba2_data = suba_data.filter(
    (pl.col('TRANSREF') == '') &
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('SUBACCT').str.slice(1, 3) != 'SGL')
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

suba2_summary = suba2_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMITS')
])

# SAS "MERGE SUBA1 SUBA2; BY ACCTNO;" is a two-way merge (outer join).
# 'outer' is deprecated in current polars; use 'full' with coalesce=True
# so a single ACCTNO column comes out instead of ACCTNO/ACCTNO_right.
subalmt_data = suba1_summary.join(
    suba2_summary, on='ACCTNO', how='full', coalesce=True
)

subalmt_data = subalmt_data.with_columns([
    pl.when(pl.col('LIMTCURM').is_null())
    .then(pl.col('LIMITS'))
    .otherwise(pl.col('LIMTCURM')).alias('LIMTCURM')
])

# SAS: IF TRANSREF NE '   ' (three literal blanks) -> "not blank" once stripped
suba_issue = suba_data.filter(pl.col('TRANSREF') != '')


def calculate_issue_date(creatds, transref):
    """Calculate issue date from creation date"""
    if creatds is None or creatds <= 0:
        return None, 99999

    date_str = str(int(creatds)).zfill(6)
    try:
        year = int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])

        if year >= 40:
            year += 1900
        else:
            year += 2000

        issue_date = datetime(year, month, day).date()
        issue_sas = (datetime.combine(issue_date, datetime.min.time()) - datetime(1960, 1, 1)).days

        if transref and len(transref) > 0 and transref[0] == 'Y':
            matured1 = issue_sas
        else:
            matured1 = 99999

        return issue_sas, matured1
    except Exception:
        return None, 99999


suba_issue = suba_issue.with_columns([
    pl.struct(['CREATDS', 'TRANSREF']).map_elements(
        lambda x: calculate_issue_date(x['CREATDS'], x['TRANSREF']),
        return_dtype=pl.Struct([pl.Field('ISSUEDT', pl.Int64), pl.Field('MATURED1', pl.Int64)])
    ).alias('_dates')
])

suba_issue = suba_issue.with_columns([
    pl.col('_dates').struct.field('ISSUEDT').alias('ISSUEDT'),
    pl.col('_dates').struct.field('MATURED1').alias('MATURED1')
]).drop('_dates')

suba_issue = suba_issue.sort(['ACCTNO', 'ISSUEDT']).unique(
    subset=['ACCTNO'], keep='first'
)

suba_final = suba_issue.select(['ACCTNO', 'ISSUEDT', 'MATURED1'])


# ============================================================================
# STEP 8: PROCESS COLLATERAL DATA
# ============================================================================
print("Step 8: Processing collateral data...")

coll_df, coll_meta = pyreadstat.read_sas7bdat(COLL_FILE)
coll_data = pl.from_pandas(coll_df).select(['CCOLLNO', 'ACCTNO'])
coll_data = coll_data.with_columns([
    sas_int('CCOLLNO'),
    sas_int('ACCTNO'),
])

desc_df, desc_meta = pyreadstat.read_sas7bdat(DESC_FILE)
desc_data = pl.from_pandas(desc_df).select(['CCOLLNO', 'CINSTCL', 'NATGUAR', 'CENSUS'])
desc_data = desc_data.with_columns([
    sas_int('CCOLLNO'),
    strip_str('CINSTCL').alias('CINSTCL'),
    strip_str('NATGUAR').alias('NATGUAR'),
])


def assign_cr(census):
    """Assign CR code based on census value"""
    if census is None:
        return '  '
    census_int = int(census)
    if 51000000 <= census_int <= 51999999:
        return '51'
    elif 72000000 <= census_int <= 72999999:
        return '72'
    elif 1000000000 <= census_int <= 1099999999:
        return '10'
    else:
        return '  '


desc_data = desc_data.with_columns([
    pl.struct(['CENSUS']).map_elements(
        lambda x: assign_cr(x['CENSUS']),
        return_dtype=pl.Utf8
    ).alias('CR')
])

desc_data = desc_data.filter(pl.col('CR') != '  ')

coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')

coll_combined = coll_combined.filter(
    (pl.col('CINSTCL') == '18') & (pl.col('NATGUAR') == '06')
)


# ============================================================================
# STEP 9: MERGE MAST WITH COLL
# ============================================================================
print("Step 9: Merging master with collateral...")

mast_final = crft_final.join(coll_combined, on='ACCTNO', how='inner')

mast_final = mast_final.unique(subset=['ACCTNO', 'CENSUS'], keep='first')


# ============================================================================
# STEP 10: MERGE WITH MICR DATA - FIXED-COLUMN PARSING
# ============================================================================
print("Step 10: Merging MICR codes...")

# SAS:
#   INPUT @001 BRANCH     3.     (numeric, 3-digit)
#         @040 MICRCD    $5.     (character, 5 chars)

with open(MICR_FILE, 'r') as f:
    micr_lines = f.readlines()

micr_data = pl.DataFrame({'data': [line.rstrip('\n') for line in micr_lines]})

micr_data = micr_data.with_columns([
    pl.col('data').str.slice(0, 3).str.strip_chars().cast(pl.Int64, strict=False).alias('BRANCH'),  # @001 3.
    pl.col('data').str.slice(39, 5).str.strip_chars().alias('MICRCD'),                                # @040 $5.
]).select(['BRANCH', 'MICRCD'])

mast_final = mast_final.join(micr_data, on='BRANCH', how='left')


# ============================================================================
# STEP 11: MERGE ALL DATA TO CREATE NPGS
# ============================================================================
print("Step 11: Merging all data...")

npgs_data = mast_final.join(cred1_data, on='ACCTNO', how='left')
npgs_data = npgs_data.join(cred2_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(suba_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(subalmt_data, on='ACCTNO', how='left')


# ============================================================================
# STEP 12: ASSIGN CVAR02 BASED ON SCH AND CR
# ============================================================================
print("Step 12: Assigning CVAR02...")


def assign_cvar02(sch, cr):
    """Assign CVAR02 based on scheme and CR"""
    if sch == 'P51' and cr in ['10', '51']:
        return '51'
    elif sch == 'P72' and cr in ['10', '72']:
        return '72'
    elif sch == 'P85' and cr == '10':
        return '85'
    elif sch == 'P53' and cr == '10':
        return '53'
    elif sch == 'P65' and cr == '10':
        return '65'
    else:
        return '  '


npgs_data = npgs_data.with_columns([
    pl.struct(['SCH', 'CR']).map_elements(
        lambda x: assign_cvar02(x['SCH'], x['CR']),
        return_dtype=pl.Utf8
    ).alias('CVAR02')
])

npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')


# ============================================================================
# STEP 13: CREATE FINAL CVAR COLUMNS
# ============================================================================
print("Step 13: Creating final output columns...")


def format_date(date_obj):
    """Format date as DD/MM/YYYY"""
    if date_obj is None:
        return '          '
    if isinstance(date_obj, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        date_obj = base_date + timedelta(days=int(date_obj))
    return date_obj.strftime('%d/%m/%Y')


normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

npgs_data = npgs_data.with_columns([
    pl.when((pl.col('MATURED1').is_not_null()) & (pl.col('MATUREDS').is_not_null()) & (
                pl.col('MATURED1') < pl.col('MATUREDS')))
    .then(pl.col('MATURED1'))
    .otherwise(pl.col('MATUREDS')).alias('MATUREDS'),
    pl.when(pl.col('ARREARS').is_null())
    .then(0)
    .otherwise(pl.col('ARREARS')).alias('ARREARS')
])


def calculate_npl_info(matureds, nodays, rdate):
    """Calculate NPL date and return formatted string"""
    if nodays is None or nodays <= 89:
        return None, '   '

    if matureds is None or matureds <= 0:
        return None, '   '

    base_date = datetime(1960, 1, 1).date()
    mature_date = base_date + timedelta(days=int(matureds))
    npl_date = mature_date + timedelta(days=89)

    return npl_date, 'NPL'


npgs_data = npgs_data.with_columns([
    pl.struct(['MATUREDS', 'NODAYS']).map_elements(
        lambda x: calculate_npl_info(x['MATUREDS'], x['NODAYS'], RDATE),
        return_dtype=pl.Struct([pl.Field('NPLDATE', pl.Date), pl.Field('NPL_STATUS', pl.Utf8)])
    ).alias('_npl_info')
])

npgs_data = npgs_data.with_columns([
    pl.col('_npl_info').struct.field('NPLDATE').alias('NPLDATE'),
    pl.col('_npl_info').struct.field('NPL_STATUS').alias('NPL_STATUS')
]).drop('_npl_info')

npgs_data = npgs_data.with_columns([
    pl.lit(0).alias('PRODUCT'),
    pl.col('CENSUS').cast(pl.Int64).alias('CVAR01'),
    pl.col('BUSREGN').cast(pl.Utf8).alias('CVAR03'),
    pl.col('NAME').cast(pl.Utf8).alias('CVAR04'),
    pl.col('ISSUEDT').alias('CVAR05'),
    pl.col('ACCTNO').cast(pl.Int64).alias('CVAR06'),
    pl.lit('TF').alias('CVAR07'),
    pl.col('LIMTCURM').cast(pl.Float64).alias('CVAR08'),
    pl.col('OUTSTAND').cast(pl.Float64).alias('CVAR09'),
    pl.lit(0.00).alias('CVAR10'),
    pl.col('ARREARS').cast(pl.Int64).alias('CVAR11'),
    pl.when((pl.col('ARREARS') >= 3) & (pl.col('NPLDATE').is_not_null()))
    .then(pl.lit('NPL'))
    .otherwise(pl.col('NPL_STATUS')).alias('CVAR12'),
    pl.struct(['NPLDATE']).map_elements(
        lambda x: format_date(x['NPLDATE']),
        return_dtype=pl.Utf8
    ).alias('CVAR13'),
    pl.lit('0233').alias('CVAR14'),
    pl.col('MICRCD').alias('CVAR15')
])

npgs_data = npgs_data.with_columns([
    pl.when(pl.col('CVAR12').is_null())
    .then(pl.lit('   '))
    .otherwise(pl.col('CVAR12')).alias('CVAR12')
])

npgs_data = npgs_data.filter(pl.col('OUTSTAND').is_not_null())


# ============================================================================
# STEP 14: MERGE WITH NPLA (Previous NPL Status)
# ============================================================================
print("Step 14: Merging with NPLA...")

try:
    npla_df, npla_meta = pyreadstat.read_sas7bdat(NPLA_FILE)
    npla_data = pl.from_pandas(npla_df).select(['CVAR06', 'CVAR01', 'STATUS', 'NDATE'])
    npla_data = npla_data.with_columns([
        sas_int('CVAR06'),
        sas_int('CVAR01'),
        strip_str('STATUS').alias('STATUS'),
        strip_str('NDATE').alias('NDATE'),
    ])

    npgs_data = npgs_data.join(npla_data, on=['CVAR06', 'CVAR01'], how='left')

    npgs_data = npgs_data.with_columns([
        pl.when((pl.col('CVAR12') == 'NPL') & (pl.col('STATUS') == 'NPL'))
        .then(pl.col('NDATE'))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') == 'NPL'))
        .then(pl.lit(normdt))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') != 'NPL') & (pl.col('NDATE').is_not_null()) & (
                    pl.col('NDATE') != ''))
        .then(pl.col('NDATE'))
        .otherwise(pl.col('CVAR13')).alias('CVAR13')
    ])
except Exception as e:
    print(f"Warning: Could not read NPLA file: {e}")


# ============================================================================
# STEP 15: FINAL OUTPUT
# ============================================================================
print("Step 15: Writing output...")

final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'SCH', 'CR', 'BRANCH', 'CVAR15', 'CENSUST', 'NATGUAR', 'CINSTCL', 'PRODUCT'
]

output_data = npgs_data.select([col for col in final_columns if col in npgs_data.columns])

output_data = output_data.sort('CVAR01')

print("Writing SAS output...")

sas = saspy.SASsession(cfgname='default')  # Adjust cfgname as needed

output_pd = output_data.to_pandas()

sas_df = sas.df2sd(output_pd, 'npgs_output')

sas_code = f"""
    LIBNAME outlib "{OUTPUT_DIR}";
    DATA outlib.btnpgs{REPTMON};
        SET npgs_output;
    RUN;
"""

sas.submit(sas_code)

sas.endsas()

print(f"Output written to: {OUTPUT_FILE}")
print(f"Total records: {len(output_data)}")
print("\nProcessing complete!")

con.close()
