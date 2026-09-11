#!/usr/bin/env python3
"""
File Name: EIBLNPGS
Non-Performing Government Scheme Loan Processing
"""

from datetime import datetime, timedelta, date
from pathlib import Path
import calendar

import pyreadstat
import polars as pl
import saspy


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLNPGS")

# Input file templates. REPTMON will be filled after report date is calculated.
LOAN_NOTE_TMPL = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{REPTMON}.sas7bdat"
LOAN_COMM_TMPL = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m{REPTMON}.sas7bdat"

MICR_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/BOPESS.txt")
COLL_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
CISLN_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")
NPLA_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/npla.sas7bdat")

# Output file - determined after report month
OUTPUT_FILE = None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_sas(path, columns=None):
    """Read a SAS7BDAT file with pyreadstat and return a Polars DataFrame."""
    df, meta = pyreadstat.read_sas7bdat(
        str(path),
        usecols=columns,
        disable_datetime_conversion=True
    )
    return pl.from_pandas(df)


# ============================================================================
# STEP 1: SET REPORT DATE / MACRO VARIABLES
# ============================================================================
print("Step 1: Setting report date...")

# Remove REPTDATE dependency. Use yesterday's date.
reptdate = datetime.today().date() - timedelta(days=1)

REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"

# SAS date value
SDATE = (reptdate - date(1960, 1, 1)).days

print(f"Report Date: {reptdate}, SDATE: {SDATE}")

# Resolve input files that depend on report month
LOAN_NOTE_FILE = Path(LOAN_NOTE_TMPL.format(REPTMON=REPTMON))
LOANI_NOTE_FILE = LOAN_NOTE_FILE

LOAN_COMM_FILE = Path(LOAN_COMM_TMPL.format(REPTMON=REPTMON))
LOANI_COMM_FILE = LOAN_COMM_FILE

# Output file
OUTPUT_FILE = OUTPUT_DIR / f"lnnpgs{REPTMON}.sas7bdat"


# ============================================================================
# STEP 2: PROCESS LOAN DATA (CONVENTIONAL + ISLAMIC)
# ============================================================================
print("Step 2: Processing loan data...")

loan_cols = [
    'LOANTYPE', 'CENSUS', 'ACCTNO', 'NOTENO', 'COMMNO', 'ISSUEDT',
    'BLDATE', 'COSTCTR', 'PENDBRH', 'BALANCE', 'CUSTCODE', 'ENTITY_CD'
]

# Conventional: ENTITY_CD != 'PIBB'
loan_conv = read_sas(LOAN_NOTE_FILE, loan_cols).filter(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars() != 'PIBB'
)

# Islamic: ENTITY_CD = 'PIBB'
loan_islam = read_sas(LOANI_NOTE_FILE, loan_cols).filter(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars() == 'PIBB'
)

loan_data = pl.concat([loan_conv, loan_islam], how='vertical')

loan_data = loan_data.with_columns([
    pl.col('LOANTYPE').alias('PRODUCT'),
    pl.col('CENSUS').alias('CENSUST')
])


def assign_sch(loantype, census):
    """Assign scheme code based on loantype and census."""
    if loantype is None or census is None:
        return '   '

    lt = int(loantype)
    cs = float(census)

    if lt == 575 and cs == 575.00:
        return 'PS2'
    elif lt == 144 and cs == 144.00:
        return 'PS3'
    elif lt == 575 and cs == 575.01:
        return 'PH4'
    elif lt == 144 and cs == 144.01:
        return 'PH5'
    elif lt == 575 and cs == 575.02:
        return 'PH6'
    elif lt == 169 and cs == 169.01:
        return 'PH7'
    elif lt == 510 and cs in [510.02, 510.03]:
        return 'P70'
    elif lt == 301 and cs == 301.04:
        return 'P85'
    elif lt == 532 and cs == 532.00:
        return 'P51'
    elif lt == 532 and cs == 532.01:
        return 'P53'
    elif lt == 532 and cs == 532.03:
        return 'E6'
    elif lt == 524 and cs == 524.01:
        return 'P72'
    elif lt == 527 and cs == 527.01:
        return 'P72'
    elif lt == 529 and cs == 529.00:
        return 'P81'
    elif lt == 529 and cs == 529.01:
        return 'P83'
    elif lt == 531 and cs == 531.02:
        return 'P63'
    elif lt == 533 and cs == 533.01:
        return 'P64'
    elif lt == 533 and cs == 533.00:
        return 'P65'
    elif lt == 574 and cs == 574.00:
        return 'P57'
    elif lt == 900 and cs == 900.01:
        return 'P81'
    elif lt == 900 and cs == 900.02:
        return 'P83'
    elif lt == 568 and cs in [568.02, 568.06, 568.14]:
        return 'F5'
    elif lt == 438 and cs in [438.02, 438.03]:
        return 'F6'
    elif lt == 575 and cs == 575.06:
        return '1Z'
    elif lt == 575 and cs == 575.08:
        return '5S'
    elif lt == 434 and cs == 434.04:
        return '2Z'
    elif lt == 434 and cs == 434.06:
        return '6S'
    elif lt == 575 and cs == 575.10:
        return '1H'
    elif lt == 434 and cs == 434.07:
        return '2H'
    elif lt == 578 and cs == 578.00:
        return '5Z'
    elif lt == 570 and cs == 570.02:
        return '5H'
    elif lt == 172 and cs == 172.04:
        return '6H'
    else:
        return '   '


loan_data = loan_data.with_columns([
    pl.struct(['LOANTYPE', 'CENSUS']).map_elements(
        lambda x: assign_sch(x['LOANTYPE'], x['CENSUS']),
        return_dtype=pl.Utf8
    ).alias('SCH')
])

loan_data = loan_data.filter(pl.col('SCH') != '   ')

# Split into LOAN0 and LOAN1
loan0 = loan_data.filter((pl.col('COMMNO').is_null()) | (pl.col('COMMNO') <= 0))
loan1 = loan_data.filter(pl.col('COMMNO') > 0)


# ============================================================================
# STEP 3: PROCESS COMMISSION DATA (CONVENTIONAL + ISLAMIC)
# ============================================================================
print("Step 3: Processing commission data...")

comm_cols = ['ACCTNO', 'COMMNO', 'CORGAMT', 'INTAMT', 'ENTITY_CD']

comm_conv = read_sas(LOAN_COMM_FILE, comm_cols).filter(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars() != 'PIBB'
)

comm_islam = read_sas(LOANI_COMM_FILE, comm_cols).filter(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars() == 'PIBB'
)

comm_data = pl.concat([comm_conv, comm_islam], how='vertical')

comm_data = comm_data.with_columns([
    (pl.col('CORGAMT').fill_null(0.0) - pl.col('INTAMT').fill_null(0.0)).alias('NETPROC')
]).select(['ACCTNO', 'COMMNO', 'NETPROC'])

# Merge LOAN1 with COMM
loan1 = loan1.join(comm_data, on=['ACCTNO', 'COMMNO'], how='inner')

# Combine LOAN0 and LOAN1
loan_combined = pl.concat([
    loan0.with_columns(pl.lit(None).cast(pl.Float64).alias('NETPROC')),
    loan1
], how='vertical')


# ============================================================================
# STEP 4: CALCULATE ARREARS AND NPL DATE
# ============================================================================
print("Step 4: Calculating arrears...")


def calculate_arrears(nodays):
    """Calculate arrears based on number of days."""
    if nodays < 30:
        return 0
    elif nodays < 60:
        return 1
    elif nodays < 90:
        return 2
    elif nodays < 120:
        return 3
    elif nodays < 150:
        return 4
    elif nodays < 180:
        return 5
    elif nodays < 365:
        return 6
    else:
        return round((nodays / 365) * 12)


def sas_datetime_to_date(value):
    """Convert SAS datetime value to Python date."""
    if value is None or value <= 0:
        return None
    return (datetime(1960, 1, 1) + timedelta(seconds=int(value))).date()


def calculate_npl_date(bldate, sdate):
    """Calculate NPL date (90 days after BLDATE)."""
    if bldate is None or bldate <= 0 or sdate <= bldate:
        return None

    nodays = sdate - bldate
    if nodays <= 89:
        return None

    base_date = datetime(1960, 1, 1).date()
    bl_date = base_date + timedelta(days=int(bldate))

    npl_date = bl_date + timedelta(days=90)
    last_day = calendar.monthrange(npl_date.year, npl_date.month)[1]
    npl_date = npl_date.replace(day=last_day)

    return npl_date


loan_combined = loan_combined.with_columns([
    pl.struct(['ISSUEDT']).map_elements(
        lambda x: sas_datetime_to_date(x['ISSUEDT']),
        return_dtype=pl.Date
    ).alias('ISSUED'),

    pl.when(
        (pl.col('BLDATE').is_not_null()) &
        (pl.col('BLDATE') > 0) &
        (pl.lit(SDATE) > pl.col('BLDATE'))
    )
    .then(pl.lit(SDATE) - pl.col('BLDATE'))
    .otherwise(0)
    .alias('NODAYS')
])

loan_combined = loan_combined.with_columns([
    pl.struct(['NODAYS']).map_elements(
        lambda x: calculate_arrears(x['NODAYS']),
        return_dtype=pl.Int64
    ).alias('ARREARS'),

    pl.struct(['BLDATE']).map_elements(
        lambda x: calculate_npl_date(x['BLDATE'], SDATE),
        return_dtype=pl.Date
    ).alias('NPLDATE')
])


# ============================================================================
# STEP 5: MERGE WITH CISLN (CUSTOMER INFORMATION)
# ============================================================================
print("Step 5: Merging customer information...")

cisln_cols = ['ACCTNO', 'NEWIC', 'CUSTNAME', 'SECCUST']

cisln_data = read_sas(CISLN_FILE, cisln_cols).filter(
    pl.col('SECCUST').cast(pl.Utf8).str.strip_chars() == '901'
).select(['ACCTNO', 'NEWIC', 'CUSTNAME']).unique()

loan_combined = loan_combined.join(cisln_data, on='ACCTNO', how='left')


# ============================================================================
# STEP 6: PROCESS COLLATERAL DATA
# ============================================================================
print("Step 6: Processing collateral data...")

coll_data = read_sas(COLL_FILE, ['CCOLLNO', 'ACCTNO', 'NOTENO'])
desc_data = read_sas(DESC_FILE, ['CCOLLNO', 'CINSTCL', 'NATGUAR', 'GRNTCVR', 'CENSUS'])


def assign_cr(census):
    """Assign CR code based on census value."""
    if census is None:
        return '  '

    census_int = int(census)

    if 51000000 <= census_int <= 51999999:
        return '51'
    elif 63000000 <= census_int <= 63999999:
        return '63'
    elif 70000000 <= census_int <= 70999999:
        return '70'
    elif 71000000 <= census_int <= 71999999:
        return '71'
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
desc_data = desc_data.filter(
    (pl.col('CINSTCL').cast(pl.Utf8).str.strip_chars() == '18') &
    (pl.col('NATGUAR').cast(pl.Utf8).str.strip_chars() == '06')
)

coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')

npgs_data = loan_combined.join(coll_combined, on=['ACCTNO', 'NOTENO'], how='inner')


# ============================================================================
# STEP 7: MERGE WITH MICR DATA
# ============================================================================
print("Step 7: Merging MICR codes...")

micr_data = read_sas(MICR_FILE, ['PENDBRH', 'BRCHCDI', 'MICRCDC', 'MICRCDI'])

npgs_data = npgs_data.join(micr_data, on='PENDBRH', how='left')

npgs_data = npgs_data.with_columns([
    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.col('MICRCDI'))
    .otherwise(pl.col('MICRCDC'))
    .alias('MICRCD'),

    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.lit('0351'))
    .otherwise(pl.lit('0233'))
    .alias('FICODE')
])


# ============================================================================
# STEP 8: ASSIGN CVAR02 BASED ON SCH AND CR
# ============================================================================
print("Step 8: Assigning CVAR02...")


def assign_cvar02(sch, cr, grntcvr, custcode):
    """Assign CVAR02 based on scheme, CR, and other factors."""
    if sch == 'P51' and cr in ['10', '51']:
        return '51'
    elif sch == 'P63' and cr in ['10', '63']:
        return '63'
    elif sch == 'P53' and cr == '10':
        return '53'
    elif sch == 'P64' and cr == '10':
        return '64'
    elif sch == 'P65' and cr == '10':
        return '65'
    elif sch == 'P85' and cr == '10':
        return '85'
    elif sch == 'P70' and cr == '70':
        return '70'
    elif sch == 'P70' and cr == '71':
        return '71'
    elif sch == 'P72' and cr in ['10', '72']:
        return '72'
    elif sch == 'P70' and cr == '10':
        return 'XX'
    elif sch == 'P81' and cr == '10':
        return '81'
    elif sch == 'P83' and cr == '10':
        return '83'
    elif sch == 'P57' and cr == '10':
        return 'YY'
    elif sch == 'PS2' and cr == '10':
        return 'S2'
    elif sch == 'PS3' and cr == '10':
        return 'S3'
    elif sch == 'PH4' and cr == '10':
        return 'H4'
    elif sch == 'PH5' and cr == '10':
        return 'H5'
    elif sch == 'PH6' and cr == '10':
        return 'H6'
    elif sch == 'PH7' and cr == '10':
        return 'H7'
    elif sch == 'F5' and cr == '10':
        return 'F5'
    elif sch == 'F6' and cr == '10':
        return 'F6'
    elif sch == '1Z' and cr == '10':
        gv = float(grntcvr) if grntcvr is not None else None
        if gv == 80:
            return '1Z'
        elif gv == 90:
            return '3Z'
    elif sch == '2Z' and cr == '10':
        gv = float(grntcvr) if grntcvr is not None else None
        if gv == 80:
            return '2Z'
        elif gv == 90:
            return '4Z'
    elif sch == '5S' and cr == '10':
        return '5S'
    elif sch == '6S' and cr == '10':
        return '6S'
    elif sch == '1H' and cr == '10':
        cc = int(custcode) if custcode is not None else None
        if cc in [42, 43, 46, 47, 49, 51, 53, 54]:
            return '1H'
        elif cc in [41, 44, 48, 52]:
            return '3H'
    elif sch == '2H' and cr == '10':
        cc = int(custcode) if custcode is not None else None
        if cc in [42, 43, 46, 47, 49, 51, 53, 54]:
            return '2H'
        elif cc in [41, 44, 48, 52]:
            return '4H'
    elif sch == 'E6' and cr == '10':
        return 'E6'
    elif sch == '5Z' and cr == '10':
        return '5Z'
    elif sch == '5H' and cr == '10':
        return '5H'
    elif sch == '6H' and cr == '10':
        return '6H'

    return '  '


npgs_data = npgs_data.with_columns([
    pl.struct(['SCH', 'CR', 'GRNTCVR', 'CUSTCODE']).map_elements(
        lambda x: assign_cvar02(x['SCH'], x['CR'], x['GRNTCVR'], x['CUSTCODE']),
        return_dtype=pl.Utf8
    ).alias('CVAR02')
])

npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')


# ============================================================================
# STEP 9: CREATE FINAL CVAR COLUMNS
# ============================================================================
print("Step 9: Creating final output columns...")


def format_date(date_obj):
    """Format date as DD/MM/YYYY."""
    if date_obj is None:
        return '          '
    return date_obj.strftime('%d/%m/%Y')


normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

npgs_data = npgs_data.with_columns([
    pl.col('CENSUS').cast(pl.Int64).alias('CVAR01'),
    pl.col('NEWIC').cast(pl.Utf8).alias('CVAR03'),
    pl.col('CUSTNAME').cast(pl.Utf8).alias('CVAR04'),
    pl.col('ISSUED').alias('CVAR05'),
    pl.col('ACCTNO').cast(pl.Int64).alias('CVAR06'),
    pl.lit('FL').alias('CVAR07'),
    pl.col('NETPROC').cast(pl.Float64).alias('CVAR08'),
    pl.col('BALANCE').cast(pl.Float64).alias('CVAR09'),
    pl.lit(0.00).alias('CVAR10'),
    pl.col('ARREARS').cast(pl.Int64).alias('CVAR11'),

    pl.when(pl.col('ARREARS') >= 3)
    .then(pl.lit('NPL'))
    .otherwise(pl.lit('   '))
    .alias('CVAR12'),

    pl.struct(['NPLDATE']).map_elements(
        lambda x: format_date(x['NPLDATE']),
        return_dtype=pl.Utf8
    ).alias('CVAR13'),

    pl.col('FICODE').alias('CVAR14'),
    pl.col('MICRCD').alias('CVAR15'),
    pl.col('PENDBRH').alias('BRANCH')
])


# ============================================================================
# STEP 10: MERGE WITH NPLA (PREVIOUS NPL STATUS)
# ============================================================================
print("Step 10: Merging with NPLA...")

try:
    npla_data = read_sas(NPLA_FILE, ['CVAR06', 'CVAR01', 'STATUS', 'NDATE'])
    npgs_data = npgs_data.join(npla_data, on=['CVAR06', 'CVAR01'], how='left')

    npgs_data = npgs_data.with_columns([
        pl.when((pl.col('CVAR12') == 'NPL') & (pl.col('STATUS') == 'NPL'))
        .then(pl.col('NDATE'))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') == 'NPL'))
        .then(pl.lit(normdt))
        .when(
            (pl.col('CVAR12') == '   ') &
            (pl.col('STATUS') != 'NPL') &
            (pl.col('NDATE').is_not_null()) &
            (pl.col('NDATE') != '          ')
        )
        .then(pl.col('NDATE'))
        .otherwise(pl.col('CVAR13'))
        .alias('CVAR13')
    ])
except Exception as e:
    print(f"Warning: Could not read NPLA file: {e}")


# ============================================================================
# STEP 11: FINAL OUTPUT TO SAS7BDAT VIA SASPY
# ============================================================================
print("Step 11: Writing output...")

final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'BRANCH', 'CVAR15', 'CENSUST', 'PRODUCT', 'NATGUAR', 'CINSTCL', 'CR', 'SCH'
]

output_data = npgs_data.select([col for col in final_columns if col in npgs_data.columns])
output_data = output_data.sort('CVAR01')

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Convert to pandas for SASPy
output_pd = output_data.to_pandas()

# Start SAS session. Adjust cfgname if required in your environment.
sas = saspy.SASsession()  # e.g. saspy.SASsession(cfgname='default')

libref = 'OUTNPGS'
table_name = f"lnnpgs{REPTMON}"

sas.submit(f"libname {libref} '{OUTPUT_DIR.as_posix()}';")
sas.df2sd(output_pd, table=table_name, libref=libref)
sas.submit(f"libname {libref} clear;")
sas.close()

print(f"Output written to: {OUTPUT_FILE}")
print(f"Total records: {len(output_data)}")
print("\nProcessing complete!")
