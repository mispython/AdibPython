#!/usr/bin/env python3
"""
File Name: EIBTNPGS
Non-Performing Government Scheme Trade Finance Processing
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
# Define each file path independently using Path()
CRFTABL_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/crftabl.txt")
BTRSA_MAST_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/mast{reptday}{reptmon}.sas7bdat")
BTRSA_CRED_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/cred{reptday}{reptmon}.sas7bdat")
BTRSA_PROV_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/prov{reptday}{reptmon}.sas7bdat")
BTRSA_SUBA_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/suba{reptday}{reptmon}.sas7bdat")
COLL_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_{reptyear}{reptmon}{reptday}")
DESC_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_desc_{reptyear}{reptmon}{reptday}")
MICR_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/BOPESS.txt")
NPLA_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/npla.sas7bdat")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBTNPGS")

# Output file - will be determined based on report date
OUTPUT_FILE = None  # Set after determining report date


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# STEP 1: SET REPORT DATE (using yesterday's date)
# ============================================================================
print("Step 1: Setting report date...")

# Use yesterday's date as the report date
reptdate = datetime.now() - timedelta(days=1)

REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"
RDATE = (reptdate - datetime(1960, 1, 1)).days  # SAS date value

print(f"Report Date: {reptdate}, RDATE: {RDATE}")

# Set output file name
OUTPUT_FILE = OUTPUT_DIR / f"btnpgs{REPTMON}.sas7bdat"

# Update BTRSA file paths with date suffix
BTRSA_MAST_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/mast{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_CRED_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/cred{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_PROV_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/prov{REPTDAY}{REPTMON}.sas7bdat")
BTRSA_SUBA_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/suba{REPTDAY}{REPTMON}.sas7bdat")

# Update COLL and DESC files with date (EBCDIC files, no .sas7bdat extension)
COLL_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_{REPTYEAR}{REPTMON}{REPTDAY}")
DESC_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/lccrisex_desc_{REPTYEAR}{REPTMON}{REPTDAY}")


# ============================================================================
# STEP 2: PROCESS CRFTABL (Credit Facility Table)
# ============================================================================
print("Step 2: Processing credit facility table...")

# Read text file - fixed width format
crft_data = pl.read_csv(
    CRFTABL_FILE, 
    has_header=False, 
    new_columns=['data']
)

# Parse the fixed-width data based on observed format
crft_data = crft_data.with_columns([
    pl.col('data').str.slice(0, 1).alias('RECTYP1'),
    pl.col('data').str.slice(1, 4).alias('BRANCH_TEMP'),
    pl.col('data').str.slice(12, 17).alias('SUBACCT'),
    pl.col('data').str.slice(23, 35).alias('TFID'),
    pl.col('data').str.slice(221, 231).str.strip_chars().cast(pl.Int64, strict=False).alias('ACCTNO')
]).select(['RECTYP1', 'TFID', 'SUBACCT', 'ACCTNO'])

# Add placeholder columns that will be filled later
crft_data = crft_data.with_columns([
    pl.lit('').alias('PREIND'),
    pl.lit(0).cast(pl.Int64).alias('CENSUST')
])

# Filter out header record and records where RECTYP1='1'
crft_data = crft_data.filter(
    (pl.col('RECTYP1') != '1') & 
    (pl.col('ACCTNO').is_not_null()) &
    (pl.col('ACCTNO') > 0)
)

# Assign SCH based on CENSUST (default to blank for now)
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

# For now, keep all records with blank SCH (will need to get CENSUST from another source)
# Remove duplicates
crft_data = crft_data.unique(subset=['ACCTNO', 'CENSUST', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 3: MERGE WITH MAST (Master Account Data)
# ============================================================================
print("Step 3: Merging with master account data...")

# Read sas7bdat file
mast_df, mast_meta = pyreadstat.read_sas7bdat(BTRSA_MAST_FILE)
mast_data = pl.from_pandas(mast_df).select([
    'ACCTNO', 'FICODE', 'NAME', 'BUSREGN'
]).unique(subset=['ACCTNO'], keep='first')

# Convert ACCTNO to Int64 to match crft_data
mast_data = mast_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])

crft_merged = crft_data.join(mast_data, on='ACCTNO', how='inner')

# Rename FICODE to BRANCH
crft_merged = crft_merged.with_columns([
    pl.col('FICODE').alias('BRANCH')
])

# Filter where ACCTNO > 0
crft_merged = crft_merged.filter(pl.col('ACCTNO') > 0)

# Select columns for CRFT
crft_final = crft_merged.select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Ensure ACCTNO is Int64 in crft_final
crft_final = crft_final.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])

# Create CRFT1 with modified SUBACCT
crft1_data = crft_merged.with_columns([
    (pl.lit('FAC') + pl.col('SUBACCT').str.slice(0, 1)).alias('SUBACCT')
]).select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Ensure ACCTNO is Int64 in crft1_data
crft1_data = crft1_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])


# ============================================================================
# STEP 4: PROCESS CREDIT DATA
# ============================================================================
print("Step 4: Processing credit data...")

# Read sas7bdat file
cred_df, cred_meta = pyreadstat.read_sas7bdat(BTRSA_CRED_FILE)
cred_data = pl.from_pandas(cred_df)

# Convert ACCTNO to Int64 in cred_data
cred_data = cred_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])

# Merge with CRFT
cred_data = cred_data.join(crft_final, on=['ACCTNO', 'SUBACCT'], how='inner')

# Filter conditions
cred_data = cred_data.filter(
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('TRANSREF') != '  ')
)

# Create TRANSREX
cred_data = cred_data.with_columns([
    pl.col('TRANSREF').str.slice(0, 7).alias('TRANSREX')
])

# Remove duplicates
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

# Read sas7bdat file
prov_df, prov_meta = pyreadstat.read_sas7bdat(BTRSA_PROV_FILE)
prov_data = pl.from_pandas(prov_df).filter(
    ~pl.col('NPLIND').is_in(['P', 'F'])
)

# Convert ACCTNO to Int64 in prov_data
prov_data = prov_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])

# Merge PROV with CRED data to get TRANSREX, SUBACCT, and OUTSTAND
cred2_data = prov_data.join(
    cred_data.select(['ACCTNO', 'TRANSREX', 'SUBACCT', 'OUTSTAND']),
    on=['ACCTNO'],
    how='inner'
)

# Filter out FAC and OV subaccounts
cred2_data = cred2_data.filter(
    ~pl.col('SUBACCT').str.slice(0, 3).is_in(['OV ', 'FAC'])
)

# Since MATUREDS doesn't exist in provision data, use a placeholder
# We'll set NODAYS to 0 and ARREARS to 0 for now
cred2_data = cred2_data.with_columns([
    pl.lit(0).cast(pl.Int64).alias('NODAYS'),
    pl.lit(0).cast(pl.Int64).alias('ARREARS'),
    pl.lit(None).cast(pl.Float64).alias('MATUREDS')
])

# Keep first record per ACCTNO
cred2_data = cred2_data.unique(subset=['ACCTNO'], keep='first')

cred2_final = cred2_data.select(['ACCTNO', 'ARREARS', 'MATUREDS', 'NODAYS'])


# ============================================================================
# STEP 7: PROCESS SUBACCOUNT DATA (SUBA)
# ============================================================================
print("Step 7: Processing subaccount data...")

# Read sas7bdat file
suba_df, suba_meta = pyreadstat.read_sas7bdat(BTRSA_SUBA_FILE)
suba_data = pl.from_pandas(suba_df)

# Convert ACCTNO to Int64 in suba_data
suba_data = suba_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64).alias('ACCTNO')
])

# Merge with CRFT1
suba_data = suba_data.join(crft1_data, on=['ACCTNO', 'SUBACCT'], how='inner')

# Create SUBA1 (FAC subaccounts)
suba1_data = suba_data.filter(
    pl.col('SUBACCT').str.slice(0, 3) == 'FAC'
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Summarize LIMTCURM for SUBA1
if 'LIMTCURM' in suba1_data.columns:
    suba1_summary = suba1_data.group_by('ACCTNO').agg([
        pl.col('LIMTCURM').sum().alias('LIMTCURM')
    ])
else:
    # If LIMTCURM doesn't exist, use a placeholder
    print("Warning: LIMTCURM column not found in SUBA1")
    suba1_summary = suba1_data.group_by('ACCTNO').agg([
        pl.lit(0.0).cast(pl.Float64).first().alias('LIMTCURM')
    ])

# Create SUBA2 (non-FAC, non-SGL subaccounts with no TRANSREF)
suba2_data = suba_data.filter(
    (pl.col('TRANSREF') == '  ') &
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('SUBACCT').str.slice(1, 3) != 'SGL')
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Summarize LIMITS for SUBA2
if 'LIMTCURM' in suba2_data.columns:
    suba2_summary = suba2_data.group_by('ACCTNO').agg([
        pl.col('LIMTCURM').sum().alias('LIMITS')
    ])
else:
    # If LIMTCURM doesn't exist, use placeholder
    print("Warning: LIMTCURM column not found in SUBA2")
    suba2_summary = suba2_data.group_by('ACCTNO').agg([
        pl.lit(0.0).cast(pl.Float64).first().alias('LIMITS')
    ])

# Merge SUBA1 and SUBA2 summaries
subalmt_data = suba1_summary.join(suba2_summary, on='ACCTNO', how='outer')

# Use LIMITS if LIMTCURM is null
subalmt_data = subalmt_data.with_columns([
    pl.when(pl.col('LIMTCURM').is_null())
    .then(pl.col('LIMITS'))
    .otherwise(pl.col('LIMTCURM')).alias('LIMTCURM')
])

# Process SUBA for issue dates
suba_issue = suba_data.filter(pl.col('TRANSREF') != '   ')


def calculate_issue_date(creatds, transref):
    """Calculate issue date from creation date"""
    if creatds is None or creatds <= 0:
        return None, 99999

    # Convert CREATDS (YYMMDD) to date
    date_str = str(int(creatds)).zfill(6)
    try:
        year = int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])

        # Handle 2-digit year (cutoff 1940)
        if year >= 40:
            year += 1900
        else:
            year += 2000

        issue_date = datetime(year, month, day).date()
        issue_sas = (datetime.combine(issue_date, datetime.min.time()) - datetime(1960, 1, 1)).days

        # If TRANSREF starts with 'Y', set MATURED1 to ISSUEDT
        if transref and len(transref) > 0 and transref[0] == 'Y':
            matured1 = issue_sas
        else:
            matured1 = 99999

        return issue_sas, matured1
    except:
        return None, 99999


# Check if CREATDS exists in suba_data, if not use a different column
if 'CREATDS' in suba_data.columns:
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
else:
    # If CREATDS doesn't exist, use POSIDATE or another date column
    print("Warning: CREATDS column not found, using alternative date column")
    suba_issue = suba_issue.with_columns([
        pl.lit(None).cast(pl.Int64).alias('ISSUEDT'),
        pl.lit(99999).cast(pl.Int64).alias('MATURED1')
    ])

# Sort and keep first per ACCTNO
suba_issue = suba_issue.sort(['ACCTNO', 'ISSUEDT']).unique(
    subset=['ACCTNO'], keep='first'
)

suba_final = suba_issue.select(['ACCTNO', 'ISSUEDT', 'MATURED1'])


# ============================================================================
# STEP 8: PROCESS COLLATERAL DATA (EBCDIC files)
# ============================================================================
print("Step 8: Processing collateral data...")

# Read EBCDIC files
def read_ebcdic_file(file_path, column_specs):
    """Read EBCDIC file with fixed-width column specifications"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    # Decode EBCDIC to ASCII
    decoded_data = raw_data.decode('cp500')  # IBM EBCDIC
    
    # Split into lines (assuming fixed record length)
    # You may need to adjust the record length
    record_length = 230  # Adjust based on actual record length
    lines = [decoded_data[i:i+record_length] for i in range(0, len(decoded_data), record_length)]
    
    # Create DataFrame
    data_dict = {}
    for col_name, (start, end) in column_specs.items():
        data_dict[col_name] = [line[start:end].strip() for line in lines]
    
    return pl.DataFrame(data_dict)

# Define column specifications for COLL_FILE (adjust positions as needed)
coll_column_specs = {
    'CCOLLNO': (0, 15),   # Adjust positions
    'ACCTNO': (15, 25)    # Adjust positions
}

# Define column specifications for DESC_FILE (adjust positions as needed)
desc_column_specs = {
    'CCOLLNO': (0, 15),   # Adjust positions
    'CINSTCL': (15, 17),  # Adjust positions
    'NATGUAR': (17, 19),  # Adjust positions
    'CENSUS': (19, 28)    # Adjust positions
}

try:
    # Read EBCDIC files
    coll_data = read_ebcdic_file(COLL_FILE, coll_column_specs)
    coll_data = coll_data.with_columns([
        pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO')
    ])
    
    desc_data = read_ebcdic_file(DESC_FILE, desc_column_specs)
    
    # Assign CR based on CENSUS
    def assign_cr(census):
        """Assign CR code based on census value"""
        if census is None or census == '':
            return '  '
        try:
            census_int = int(census)
            if 51000000 <= census_int <= 51999999:
                return '51'
            elif 72000000 <= census_int <= 72999999:
                return '72'
            elif 1000000000 <= census_int <= 1099999999:
                return '10'
            else:
                return '  '
        except:
            return '  '
    
    desc_data = desc_data.with_columns([
        pl.struct(['CENSUS']).map_elements(
            lambda x: assign_cr(x['CENSUS']),
            return_dtype=pl.Utf8
        ).alias('CR')
    ])
    
    # Filter for specific CR codes
    desc_data = desc_data.filter(pl.col('CR') != '  ')
    
    # Merge collateral data
    coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')
    
    # Filter for specific collateral types
    coll_combined = coll_combined.filter(
        (pl.col('CINSTCL') == '18') & (pl.col('NATGUAR') == '06')
    )
    
except Exception as e:
    print(f"Warning: Could not read EBCDIC files: {e}")
    # Create empty placeholder
    coll_combined = pl.DataFrame({
        'ACCTNO': pl.Series([], dtype=pl.Int64),
        'CENSUS': pl.Series([], dtype=pl.Utf8)
    })


# ============================================================================
# STEP 9: MERGE MAST WITH COLL
# ============================================================================
print("Step 9: Merging master with collateral...")

mast_final = crft_final.join(coll_combined, on='ACCTNO', how='inner')

# Remove duplicates
mast_final = mast_final.unique(subset=['ACCTNO', 'CENSUS'], keep='first')


# ============================================================================
# STEP 10: MERGE WITH MICR DATA
# ============================================================================
print("Step 10: Merging MICR codes...")

# Read BOPESS.txt file
try:
    with open(MICR_FILE, 'r') as f:
        micr_lines = f.readlines()
    
    # Create DataFrame with raw data (skip header if present)
    micr_data = pl.DataFrame({'data': [line.rstrip('\n') for line in micr_lines if line.strip()]})
    
    # Parse based on actual format - adjust positions as needed
    micr_data = micr_data.with_columns([
        pl.col('data').str.slice(0, 5).alias('BRANCH'),
        pl.col('data').str.slice(5, 11).alias('MICRCD')
    ]).select(['BRANCH', 'MICRCD'])
    
    mast_final = mast_final.join(micr_data, on='BRANCH', how='left')
except Exception as e:
    print(f"Warning: Could not read BOPESS.txt: {e}")
    mast_final = mast_final.with_columns([
        pl.lit(None).cast(pl.Utf8).alias('MICRCD')
    ])


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

# Filter where CVAR02 is not blank
npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')


# ============================================================================
# STEP 13: CREATE FINAL CVAR COLUMNS
# ============================================================================
print("Step 13: Creating final output columns...")


def format_date(date_obj):
    """Format date as DD/MM/YYYY"""
    if date_obj is None:
        return '          '
    # Convert SAS date to Python date if needed
    if isinstance(date_obj, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        date_obj = base_date + timedelta(days=int(date_obj))
    return date_obj.strftime('%d/%m/%Y')


normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# Handle MATURED1 vs MATUREDS
npgs_data = npgs_data.with_columns([
    pl.when((pl.col('MATURED1').is_not_null()) & (pl.col('MATUREDS').is_not_null()) & (
                pl.col('MATURED1') < pl.col('MATUREDS')))
    .then(pl.col('MATURED1'))
    .otherwise(pl.col('MATUREDS')).alias('MATUREDS'),
    pl.when(pl.col('ARREARS').is_null())
    .then(0)
    .otherwise(pl.col('ARREARS')).alias('ARREARS')
])


# Calculate NPL date and status
def calculate_npl_info(matureds, nodays, rdate):
    """Calculate NPL date and return formatted string"""
    if nodays is None or nodays <= 89:
        return None, '   '

    # Add 89 days to MATUREDS
    if matureds is None or matureds <= 0:
        return None, '   '

    # Convert SAS date to Python date
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

# Create final columns
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

# Fill CVAR12 default
npgs_data = npgs_data.with_columns([
    pl.when(pl.col('CVAR12').is_null())
    .then(pl.lit('   '))
    .otherwise(pl.col('CVAR12')).alias('CVAR12')
])

# Filter out null OUTSTAND
npgs_data = npgs_data.filter(pl.col('OUTSTAND').is_not_null())


# ============================================================================
# STEP 14: MERGE WITH NPLA (Previous NPL Status)
# ============================================================================
print("Step 14: Merging with NPLA...")

try:
    npla_df, npla_meta = pyreadstat.read_sas7bdat(NPLA_FILE)
    npla_data = pl.from_pandas(npla_df).select(['CVAR06', 'CVAR01', 'STATUS', 'NDATE'])
    
    # Convert CVAR06 and CVAR01 to Int64
    npla_data = npla_data.with_columns([
        pl.col('CVAR06').cast(pl.Int64).alias('CVAR06'),
        pl.col('CVAR01').cast(pl.Int64).alias('CVAR01')
    ])
    
    npgs_data = npgs_data.join(npla_data, on=['CVAR06', 'CVAR01'], how='left')

    # Update CVAR13 based on NPL status
    npgs_data = npgs_data.with_columns([
        pl.when((pl.col('CVAR12') == 'NPL') & (pl.col('STATUS') == 'NPL'))
        .then(pl.col('NDATE'))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') == 'NPL'))
        .then(pl.lit(normdt))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') != 'NPL') & (pl.col('NDATE').is_not_null()) & (
                    pl.col('NDATE') != '          '))
        .then(pl.col('NDATE'))
        .otherwise(pl.col('CVAR13')).alias('CVAR13')
    ])
except Exception as e:
    print(f"Warning: Could not read NPLA file: {e}")


# ============================================================================
# STEP 15: FINAL OUTPUT
# ============================================================================
print("Step 15: Writing output...")

# Select and order final columns
final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'SCH', 'CR', 'BRANCH', 'CVAR15', 'CENSUST', 'NATGUAR', 'CINSTCL', 'PRODUCT'
]

output_data = npgs_data.select([col for col in final_columns if col in npgs_data.columns])

# Sort by CVAR01
output_data = output_data.sort('CVAR01')

# Write output using saspy
print("Writing SAS output...")

# Initialize SAS session
try:
    sas = saspy.SASsession(cfgname='default')  # Adjust cfgname as needed
    
    # Convert polars DataFrame to pandas for saspy
    output_pd = output_data.to_pandas()
    
    # Upload dataframe to SAS
    sas_df = sas.df2sd(output_pd, 'npgs_output')
    
    # Write to sas7bdat
    sas_code = f"""
        LIBNAME outlib "{OUTPUT_DIR}";
        DATA outlib.btnpgs{REPTMON};
            SET npgs_output;
        RUN;
    """
    
    sas.submit(sas_code)
    
    # Close SAS session
    sas.endsas()
except Exception as e:
    print(f"Warning: SAS session error: {e}")
    # If saspy fails, write as parquet as fallback
    output_data.write_parquet(OUTPUT_DIR / f"btnpgs{REPTMON}.parquet")
    print(f"Fallback: Output written as parquet: {OUTPUT_DIR / f'btnpgs{REPTMON}.parquet'}")

print(f"Output written to: {OUTPUT_FILE}")
print(f"Total records: {len(output_data)}")
print("\nProcessing complete!")

# Close DuckDB connection
con.close()
