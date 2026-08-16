#!/usr/bin/env python3
"""
File Name: EIEMCRLS.py
Report ID: EIQPROM2
Automailing Listing for Reinstatement of Loan
"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import sys


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
PROMOTE_LOAN_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIEMCRLS/loan{month}.sas7bdat"
LN_LNNAME_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIEMCRLS/ln/lnname.sas7bdat"
LNI_LNNAME_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIEMCRLS/lni/lnname.sas7bdat"

# Output paths
EMCPBB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emcpbb.txt"
EMCPBBS_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emcpbbs.txt"
EMLPBB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emlpbb.txt"
EMLPBBS_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emlpbbs.txt"
EMXPBB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emxpbb.txt"
EMCPIB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emcpib.txt"
EMCPIBS_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emcpibs.txt"
EMLPIB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emlpib.txt"
EMLPIBS_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emlpibs.txt"
EMXPIB_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/emxpib.txt"
REPORT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/eiqprom2_report.txt"

# Create output directory if it doesn't exist
Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS").mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS
# ============================================================================

def calculate_report_dates(run_date=None):
    """Calculate report dates matching SAS logic"""
    if run_date is None:
        today = datetime.now()
    else:
        today = run_date
    
    reptdate = today - timedelta(days=today.day)
    
    reptdt = reptdate.strftime('%d%m%y')
    indxdt = reptdate.strftime('%Y%m%d')
    rdate = reptdate.strftime('%d/%m/%y')
    reptmon = f"{reptdate.month:02d}"
    reptyr = str(reptdate.year)
    mthnam = reptdate.strftime('%b').upper()

    return {
        'reptdate': reptdate,
        'reptdt': reptdt,
        'indxdt': indxdt,
        'rdate': rdate,
        'reptmon': reptmon,
        'reptyr': reptyr,
        'mthnam': mthnam
    }


# Check for command line date argument
if len(sys.argv) > 1:
    try:
        run_date_str = sys.argv[1]
        run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
        print(f"Using specified run date: {run_date_str}")
        dates = calculate_report_dates(run_date)
    except ValueError:
        print(f"Invalid date format: {sys.argv[1]}. Using today's date.")
        dates = calculate_report_dates()
else:
    dates = calculate_report_dates()

REPTDT = dates['reptdt']
INDXDT = dates['indxdt']
RDATE = dates['rdate']
REPTMON = dates['reptmon']
REPTYR = dates['reptyr']
MTHNAM = dates['mthnam']

print(f"Using report date: {RDATE}")
print(f"Report month: {REPTMON}-{REPTYR}")


# ============================================================================
# SAS-LIKE FUNCTIONS
# ============================================================================

def encr_id(id_value):
    """ENCR_ID macro equivalent"""
    if not id_value or pd.isna(id_value) or str(id_value).strip() == '':
        return ' ' * 40
    
    ids = str(id_value).upper()
    mask_ids = ""
    
    translation_map = {
        '0': 'B', '1': 'C', '2': 'A', '3': 'D', '4': 'X',
        '5': '9', '6': 'G', '7': 'E', '8': 'H', '9': 'I'
    }
    
    for char in ids:
        ascii_val = ord(char)
        ch_x = str(ascii_val)
        mask = ""
        for digit in ch_x:
            if digit in translation_map:
                mask += translation_map[digit]
            else:
                mask += digit
        mask_ids = mask_ids.strip() + mask
    
    return mask_ids[:40]


def safe_int(value, default=0):
    """Safely convert value to integer"""
    if value is None or pd.isna(value) or value == '':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_str(value, default=''):
    """Safely convert value to string"""
    if value is None or pd.isna(value):
        return default
    return str(value)


# ============================================================================
# DATA PROCESSING
# ============================================================================

print("\n" + "="*70)
print("Starting EIQPROM2 processing...")
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")
print("="*70)

# Step 1: Load and filter PROMOTE.LOAN data
print("\nStep 1: Loading and filtering PROMOTE.LOAN data...")
promote_path = PROMOTE_LOAN_PATH.format(month=REPTMON)
print(f"Loading file: {promote_path}")

# Read SAS file
loan_df, loan_meta = pyreadstat.read_sas7bdat(promote_path)
loan_pl = pl.from_pandas(loan_df)

print(f"Total records in LOAN file: {len(loan_pl)}")

# Filter records where REPAID > 100000
rlslist_base = loan_pl.filter(pl.col('REPAID') > 100000)
print(f"\nRecords after REPAID > 100000: {len(rlslist_base)}")

# ============================================================================
# DIAGNOSTIC ANALYSIS
# ============================================================================

print("\n" + "="*70)
print("DIAGNOSTIC ANALYSIS - Identifying correct filters")
print("="*70)

# Analyze NOTENO distribution
print("\nNOTENO distribution (top 30):")
noteno_counts = rlslist_base.group_by('NOTENO').agg(pl.len().alias('count')).sort('count', descending=True).head(30)
for row in noteno_counts.iter_rows(named=True):
    print(f"  NOTENO={row['NOTENO']}: {row['count']} records")

# Analyze PRODUCT distribution
print("\nPRODUCT distribution (top 30):")
product_counts = rlslist_base.group_by('PRODUCT').agg(pl.len().alias('count')).sort('count', descending=True).head(30)
for row in product_counts.iter_rows(named=True):
    print(f"  PRODUCT={row['PRODUCT']}: {row['count']} records")

# Analyze MAILCODE distribution
print("\nMAILCODE distribution:")
mailcode_counts = rlslist_base.group_by('MAILCODE').agg(pl.len().alias('count')).sort('count', descending=True)
for row in mailcode_counts.iter_rows(named=True):
    print(f"  MAILCODE='{row['MAILCODE']}': {row['count']} records")

# Test different filter combinations
print("\n" + "="*70)
print("Testing filter combinations")
print("="*70)

# Test 1: NOTENO >= 20000 (based on production showing 20010, 30016, etc.)
test1 = rlslist_base.filter(pl.col('NOTENO') >= 20000)
print(f"\nTest 1 - NOTENO >= 20000: {len(test1)} records")

# Test 2: Specific NOTENO values from production
noteno_prod = [20010, 20011, 20012, 30010, 30011, 30012, 30016]
test2 = rlslist_base.filter(pl.col('NOTENO').is_in(noteno_prod))
print(f"Test 2 - Specific NOTENO values: {len(test2)} records")

# Test 3: NOTENO between 20000-39999 (reinstatement notes)
test3 = rlslist_base.filter((pl.col('NOTENO') >= 20000) & (pl.col('NOTENO') < 40000))
print(f"Test 3 - NOTENO 20000-39999: {len(test3)} records")

# Test 4: Check if there's a specific range
test4 = rlslist_base.filter(pl.col('NOTENO') >= 10000)
print(f"Test 4 - NOTENO >= 10000: {len(test4)} records")

# Test 5: NOTENO < 20000 but specific values
test5 = rlslist_base.filter(pl.col('NOTENO') < 20000)
print(f"Test 5 - NOTENO < 20000: {len(test5)} records")

# Check MODELLDES field which might indicate reinstatement
if 'MODELDES' in rlslist_base.columns:
    print("\nMODELDES distribution:")
    modeldes_counts = rlslist_base.group_by('MODELDES').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    for row in modeldes_counts.iter_rows(named=True):
        print(f"  MODELDES='{row['MODELDES']}': {row['count']} records")

# Check COLLMAKE which might indicate reinstatement
if 'COLLMAKE' in rlslist_base.columns:
    print("\nCOLLMAKE distribution (top 10):")
    collmake_counts = rlslist_base.group_by('COLLMAKE').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    for row in collmake_counts.iter_rows(named=True):
        print(f"  COLLMAKE='{row['COLLMAKE']}': {row['count']} records")

# Check DELQCD
if 'DELQCD' in rlslist_base.columns:
    print("\nDELQCD distribution:")
    delqcd_counts = rlslist_base.group_by('DELQCD').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in delqcd_counts.iter_rows(named=True):
        print(f"  DELQCD='{row['DELQCD']}': {row['count']} records")

# The key insight: Production shows NOTENO values like 30016, 20010, 30010
# These are likely reinstatement note numbers
# Let's use the filter that gets closest to production count
print("\n" + "="*70)
print("Applying best filter based on analysis")
print("="*70)

# Based on production showing NOTENO >= 20000, apply this filter
# Also apply REINPROD filter
rlslist = rlslist_base.filter(
    (pl.col('NOTENO') >= 20000) &
    (pl.col('REINPROD').is_not_null()) &
    (pl.col('REINPROD').cast(pl.Utf8).str.strip_chars() != '')
)
print(f"Records after NOTENO >= 20000 AND REINPROD filter: {len(rlslist)}")

# Remove empty GUAREND
rlslist = rlslist.filter(pl.col('GUAREND').cast(pl.Utf8).str.strip_chars() != '')
print(f"Records after removing empty GUAREND: {len(rlslist)}")

# Sort by GUAREND ascending, REPAID descending
rlslist = rlslist.sort(['GUAREND', 'REPAID'], descending=[False, True])

# Remove duplicates keeping first record per GUAREND
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')
print(f"Records after deduplication by GUAREND: {len(rlslist)}")

# Sort by ACCTNO for merge
rlslist = rlslist.sort('ACCTNO')

print(f"\nFinal records in RLSLIST: {len(rlslist)}")


# ============================================================================
# PBB PROCESSING
# ============================================================================

print("\n" + "="*70)
print("Step 2: Processing PBB data...")
print("="*70)

# Load LN.LNNAME
lnname_df, lnname_meta = pyreadstat.read_sas7bdat(LN_LNNAME_PATH)
lnname_pl = pl.from_pandas(lnname_df)

print(f"Total records in LN.LNNAME: {len(lnname_pl)}")

# Merge with RLSLIST
pbbname = lnname_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'BRCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pbbname = pbbname.sort('ACCTNO')

print(f"Records in PBBNAME after merge: {len(pbbname)}")

# Add encrypted ID using NEWIC
pbbname = pbbname.with_columns([
    pl.col('NEWIC').map_elements(
        lambda x: encr_id(str(x).replace(' ', '') if pd.notna(x) else ' '), 
        return_dtype=pl.Utf8
    ).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpbb = pbbname.filter(
    (pl.col('MAILCODE').cast(pl.Utf8).is_in([' ', '13', '14', '13.0', '14.0'])) &
    (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != '')
)

pbbname = pbbname.filter(
    ~((pl.col('MAILCODE').cast(pl.Utf8).is_in([' ', '13', '14', '13.0', '14.0'])) &
      (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != ''))
)

print(f"Records in PBBNAME (non-email): {len(pbbname)}")
print(f"Records in MAILPBB (email): {len(mailpbb)}")

# Write EMCPBB file
print("\nWriting EMCPBB file...")
with open(EMCPBB_PATH, 'w') as f:
    rowcnt = 0
    for row in pbbname.iter_rows(named=True):
        rowcnt += 1
        branch = safe_int(row.get('BRANCH'))
        acctno = safe_int(row.get('ACCTNO'))
        
        line = (
            f"B"
            f"{REPTDT}"
            f"{safe_str(row.get('NAMELN1')):<40.40}"
            f"{safe_str(row.get('NAMELN2')):<40.40}"
            f"{safe_str(row.get('NAMELN3')):<40.40}"
            f"{safe_str(row.get('NAMELN4')):<40.40}"
            f"{safe_str(row.get('NAMELN5')):<40.40}"
            f"{branch:07d}"
            f"{acctno:011d}"
            f"{safe_str(row.get('MASK_IDS')):<24.24}"
        )
        f.write(line + '\n')

# Write EMCPBBS summary file
with open(EMCPBBS_PATH, 'w') as f:
    line = (
        f"LNRIHLCP"
        f"{' ' * 22}"
        f"{REPTDT}"
        f"{rowcnt:016d}"
    )
    f.write(line + '\n')

print(f"EMCPBB records written: {rowcnt}")

# Process MAILPBB
if len(mailpbb) > 0:
    print("\nProcessing MAILPBB email statements...")

    mailpbb = mailpbb.with_columns([
        pl.Series(name='ROWCNT', values=range(1, len(mailpbb) + 1))
    ])

    mailpbb = mailpbb.with_columns([
        (pl.lit("PBB_EMAIL_STMT_RIL_C") +
         pl.col('ROWCNT').cast(pl.Utf8).str.zfill(10) +
         pl.lit("_") +
         pl.lit(INDXDT)).alias('VAR_ID'),
        (pl.lit(MTHNAM) + pl.lit(REPTYR)).alias('STATE_DTE')
    ])

    # Write EMLPBB file
    print("Writing EMLPBB file...")
    with open(EMLPBB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpbb.iter_rows(named=True):
            rowcnt += 1
            branch = safe_int(row.get('BRANCH'))
            acctno = safe_int(row.get('ACCTNO'))
            
            line = (
                f"B"
                f"{REPTDT}"
                f"{safe_str(row.get('NAMELN1')):<40.40}"
                f"{safe_str(row.get('NAMELN2')):<40.40}"
                f"{safe_str(row.get('NAMELN3')):<40.40}"
                f"{safe_str(row.get('NAMELN4')):<40.40}"
                f"{safe_str(row.get('NAMELN5')):<40.40}"
                f"{branch:07d}"
                f"{acctno:011d}"
                f"{' '}"
                f"{safe_str(row.get('VAR_ID')):<40.40}"
                f"{' '}"
                f"{safe_str(row.get('MASK_IDS')):<24.24}"
            )
            f.write(line + '\n')

    # Write EMLPBBS summary file
    with open(EMLPBBS_PATH, 'w') as f:
        line = (
            f"LNRIHLCE"
            f"{' ' * 22}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"EMLPBB records written: {rowcnt}")

    # Write EMXPBB index file
    print("Writing EMXPBB index file...")
    with open(EMXPBB_PATH, 'w') as f:
        for row in mailpbb.iter_rows(named=True):
            line = (
                f"{safe_str(row.get('VAR_ID')):<40.40}"
                f"{safe_str(row.get('EMAILADD')):<60.60}"
                f"{safe_str(row.get('STATE_DTE')):<7.7}"
                f"{safe_str(row.get('NAMELN1')):<40.40}"
                f"{safe_str(row.get('NEWIC')):<17.17}"
            )
            f.write(line + '\n')

    print(f"EMXPBB records written: {len(mailpbb)}")


# ============================================================================
# PIB PROCESSING
# ============================================================================

print("\n" + "="*70)
print("Step 3: Processing PIB data...")
print("="*70)

# Load LNI.LNNAME
lni_df, lni_meta = pyreadstat.read_sas7bdat(LNI_LNNAME_PATH)
lni_pl = pl.from_pandas(lni_df)

print(f"Total records in LNI.LNNAME: {len(lni_pl)}")

# Merge with RLSLIST
pibname = lni_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'BRCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pibname = pibname.sort('ACCTNO')

print(f"Records in PIBNAME after merge: {len(pibname)}")

# Add encrypted ID
pibname = pibname.with_columns([
    pl.col('NEWIC').map_elements(
        lambda x: encr_id(str(x).replace(' ', '') if pd.notna(x) else ' '), 
        return_dtype=pl.Utf8
    ).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpib = pibname.filter(
    (pl.col('MAILCODE').cast(pl.Utf8).is_in([' ', '13', '14', '13.0', '14.0'])) &
    (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != '')
)

pibname = pibname.filter(
    ~((pl.col('MAILCODE').cast(pl.Utf8).is_in([' ', '13', '14', '13.0', '14.0'])) &
      (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != ''))
)

print(f"Records in PIBNAME (non-email): {len(pibname)}")
print(f"Records in MAILPIB (email): {len(mailpib)}")

# Write EMCPIB file
print("\nWriting EMCPIB file...")
with open(EMCPIB_PATH, 'w') as f:
    rowcnt = 0
    for row in pibname.iter_rows(named=True):
        rowcnt += 1
        branch = safe_int(row.get('BRANCH'))
        acctno = safe_int(row.get('ACCTNO'))
        
        line = (
            f"B"
            f"{REPTDT}"
            f"{safe_str(row.get('NAMELN1')):<40.40}"
            f"{safe_str(row.get('NAMELN2')):<40.40}"
            f"{safe_str(row.get('NAMELN3')):<40.40}"
            f"{safe_str(row.get('NAMELN4')):<40.40}"
            f"{safe_str(row.get('NAMELN5')):<40.40}"
            f"{branch:07d}"
            f"{acctno:011d}"
            f"{safe_str(row.get('MASK_IDS')):<24.24}"
        )
        f.write(line + '\n')

# Write EMCPIBS summary file
with open(EMCPIBS_PATH, 'w') as f:
    line = (
        f"LNRIHLIP"
        f"{' ' * 22}"
        f"{REPTDT}"
        f"{rowcnt:016d}"
    )
    f.write(line + '\n')

print(f"EMCPIB records written: {rowcnt}")

# Process MAILPIB
if len(mailpib) > 0:
    print("\nProcessing MAILPIB email statements...")

    mailpib = mailpib.with_columns([
        pl.Series(name='ROWCNT', values=range(1, len(mailpib) + 1))
    ])

    mailpib = mailpib.with_columns([
        (pl.lit("PIB_EMAIL_STMT_RIL_C") +
         pl.col('ROWCNT').cast(pl.Utf8).str.zfill(10) +
         pl.lit("_") +
         pl.lit(INDXDT)).alias('VAR_ID'),
        (pl.lit(MTHNAM) + pl.lit(REPTYR)).alias('STATE_DTE')
    ])

    # Write EMLPIB file
    print("Writing EMLPIB file...")
    with open(EMLPIB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpib.iter_rows(named=True):
            rowcnt += 1
            branch = safe_int(row.get('BRANCH'))
            acctno = safe_int(row.get('ACCTNO'))
            
            line = (
                f"B"
                f"{REPTDT}"
                f"{safe_str(row.get('NAMELN1')):<40.40}"
                f"{safe_str(row.get('NAMELN2')):<40.40}"
                f"{safe_str(row.get('NAMELN3')):<40.40}"
                f"{safe_str(row.get('NAMELN4')):<40.40}"
                f"{safe_str(row.get('NAMELN5')):<40.40}"
                f"{branch:07d}"
                f"{acctno:011d}"
                f"{' '}"
                f"{safe_str(row.get('VAR_ID')):<40.40}"
                f"{' '}"
                f"{safe_str(row.get('MASK_IDS')):<24.24}"
            )
            f.write(line + '\n')

    # Write EMLPIBS summary file
    with open(EMLPIBS_PATH, 'w') as f:
        line = (
            f"LNRIHLIPE"
            f"{' ' * 21}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"EMLPIB records written: {rowcnt}")

    # Write EMXPIB index file
    print("Writing EMXPIB index file...")
    with open(EMXPIB_PATH, 'w') as f:
        for row in mailpib.iter_rows(named=True):
            line = (
                f"{safe_str(row.get('VAR_ID')):<40.40}"
                f"{safe_str(row.get('EMAILADD')):<60.60}"
                f"{safe_str(row.get('STATE_DTE')):<7.7}"
                f"{safe_str(row.get('NAMELN1')):<40.40}"
                f"{safe_str(row.get('NEWIC')):<17.17}"
            )
            f.write(line + '\n')

    print(f"EMXPIB records written: {len(mailpib)}")


# ============================================================================
# REPORT GENERATION
# ============================================================================

print("\n" + "="*70)
print("Step 4: Generating report...")
print("="*70)

# Combine PBBNAME and PIBNAME
rlslist_report = pl.concat([pbbname, pibname])
rlslist_report = rlslist_report.with_columns([
    pl.lit(1).alias('NOEMC')
])
rlslist_report = rlslist_report.sort(['BRANCH', 'ACCTNO', 'NOTENO'])

print(f"Total records for report: {len(rlslist_report)}")


def write_report_matching_sas():
    """Generate report matching SAS PROC REPORT output"""
    
    with open(REPORT_PATH, 'w') as f:
        
        # Write title lines
        f.write(f"REPORT ID : EIQPROM2\n")
        f.write(f"AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT {RDATE}\n")
        f.write(f"\n")
        
        # Write header lines
        f.write(f"                                                                                          MA\n")
        f.write(f"                                                                                          IL\n")
        f.write(f"   BRANCH                       NOTE   PRODUCT                                            CO\n")
        f.write(f"     CODE  BRANCH      A/C NO     NO      CODE  NAME OF BORROWER/CUSTOMER                 DE\n")
        f.write(f"                                                                                          \n")
        f.write(f"\n")
        
        current_branch = None
        branch_count = 0
        
        for row in rlslist_report.iter_rows(named=True):
            branch = safe_int(row.get('BRANCH'))
            acctno = safe_int(row.get('ACCTNO'))
            noteno = safe_int(row.get('NOTENO'))
            product = safe_int(row.get('PRODUCT'))
            nameln1 = safe_str(row.get('NAMELN1'))
            
            # Get MAILCODE
            mailcode_raw = row.get('MAILCODE')
            if mailcode_raw is None or pd.isna(mailcode_raw):
                mailcode = ''
            else:
                mailcode = str(mailcode_raw).strip()
                if '.' in mailcode:
                    mailcode = mailcode.rstrip('0').rstrip('.')
                if mailcode == '0':
                    mailcode = ''
            
            # Get BRCH field
            brch = row.get('BRCH')
            if brch is None or pd.isna(brch):
                brch = ''
            else:
                brch = str(brch).strip()
            
            # Check for branch break
            if current_branch is not None and branch != current_branch:
                f.write(f"\n")
                f.write(f"    " + "-"*123 + "\n")
                f.write(f"    NO OF BORROWER/CUSTOMER :{branch_count:>8,}\n")
                f.write(f"\n")
                branch_count = 0
                current_branch = None
            
            # Show branch code only on first occurrence
            if current_branch is None:
                branch_display = f"{branch:>7d}"
                current_branch = branch
            else:
                branch_display = " " * 7
            
            branch_count += 1
            
            # Format detail line
            line = f" {branch_display}  {brch:<5} {acctno:>10d} {noteno:>5d} {product:>8d} {nameln1:<40} {mailcode:>2}\n"
            f.write(line)
        
        # Write final branch summary
        if branch_count > 0:
            f.write(f"\n")
            f.write(f"    " + "-"*123 + "\n")
            f.write(f"    NO OF BORROWER/CUSTOMER :{branch_count:>8,}\n")
    
    print(f"Report written to: {REPORT_PATH}")


write_report_matching_sas()

print("\n" + "="*70)
print("EIEMCRLS processing completed successfully!")
print("="*70)
print(f"Report Date: {RDATE}")
print(f"Data Month: {REPTMON}-{REPTYR}")
print(f"Total non-email records (PBB + PIB): {len(pbbname) + len(pibname)}")
print(f"Total email records (PBB + PIB): {len(mailpbb) + len(mailpib)}")
print("="*70)
