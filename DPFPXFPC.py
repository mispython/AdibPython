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
import math


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

def calculate_report_dates():
    """
    Calculate report dates matching SAS logic:
    REPTDATE = TODAY() - DAY(TODAY())
    This gives the last day of the previous month
    """
    today = datetime.now()
    # Subtract the day of month to get last day of previous month
    reptdate = today - timedelta(days=today.day)
    
    reptdt = reptdate.strftime('%d%m%y')  # DDMMYY6
    indxdt = reptdate.strftime('%Y%m%d')  # YYMMDDN8
    rdate = reptdate.strftime('%d/%m/%y')  # DDMMYY8 with slashes
    reptmon = f"{reptdate.month:02d}"  # Z2
    reptyr = str(reptdate.year)  # YEAR4
    mthnam = reptdate.strftime('%b').upper()  # MONNAME3 uppercase

    return {
        'reptdate': reptdate,
        'reptdt': reptdt,
        'indxdt': indxdt,
        'rdate': rdate,
        'reptmon': reptmon,
        'reptyr': reptyr,
        'mthnam': mthnam
    }


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

def validate_id(ar_guar, valid_chars):
    """
    VALIDATE_ID macro equivalent
    Validates AR_GUAR field and removes invalid characters
    """
    if ar_guar is None or pd.isna(ar_guar) or str(ar_guar).strip() == '':
        return ' '
    
    ar_guar = str(ar_guar).strip()
    
    # Find invalid characters
    invalid_chars = [c for c in ar_guar if c not in valid_chars]
    
    # Check if there are any invalid characters
    if invalid_chars:
        # Translate: replace invalid chars with space
        translation_table = str.maketrans({c: ' ' for c in invalid_chars})
        id_value = ar_guar.translate(translation_table)
        return id_value
    else:
        return ar_guar


def encr_id(id_value):
    """
    ENCR_ID macro equivalent
    Encrypts ID field using character substitution
    """
    if not id_value or pd.isna(id_value) or str(id_value).strip() == '' or str(id_value).strip() == ' ':
        return ' ' * 40
    
    ids = str(id_value).upper().strip()
    mask_ids = ""
    
    # Translation table for encryption
    translation_map = {
        '0': 'B', '1': 'C', '2': 'A', '3': 'D', '4': 'X',
        '5': '9', '6': 'G', '7': 'E', '8': 'H', '9': 'I'
    }
    
    for char in ids:
        # Get ASCII value
        ascii_val = ord(char)
        ch_x = str(ascii_val)
        
        # Translate digits to letters
        mask = ""
        for digit in ch_x:
            if digit in translation_map:
                mask += translation_map[digit]
            else:
                mask += digit
        
        mask_ids = mask_ids.strip() + mask
    
    return mask_ids[:40]


def safe_int(value, default=0):
    """Safely convert value to integer, handling NaN and None"""
    if value is None or pd.isna(value) or value == '':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_str(value, default=''):
    """Safely convert value to string, handling NaN and None"""
    if value is None or pd.isna(value):
        return default
    return str(value)


# ============================================================================
# DATA PROCESSING
# ============================================================================

print("Starting EIQPROM2 processing...")
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")

# Step 1: Load and filter PROMOTE.LOAN data
print("\nStep 1: Loading and filtering PROMOTE.LOAN data...")
promote_path = PROMOTE_LOAN_PATH.format(month=REPTMON)
print(f"Loading file: {promote_path}")

# Read SAS file using pyreadstat
loan_df, loan_meta = pyreadstat.read_sas7bdat(promote_path)
loan_pl = pl.from_pandas(loan_df)

print(f"Total records in LOAN file: {len(loan_pl)}")

# Filter records where REPAID > 100000
rlslist = loan_pl.filter(pl.col('REPAID') > 100000)
print(f"Records after REPAID > 100000 filter: {len(rlslist)}")

# Sort by GUAREND, REPAID DESC
rlslist = rlslist.sort(['GUAREND', 'REPAID'], descending=[False, True])

# Remove duplicates keeping first record per GUAREND (SAS NODUPKEY equivalent)
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')
print(f"Records after deduplication by GUAREND: {len(rlslist)}")

# Sort by ACCTNO for merge
rlslist = rlslist.sort('ACCTNO')

print(f"Final records in RLSLIST: {len(rlslist)}")


# ============================================================================
# PBB PROCESSING
# ============================================================================

print("\nStep 2: Processing PBB data...")

# Load LN.LNNAME
lnname_df, lnname_meta = pyreadstat.read_sas7bdat(LN_LNNAME_PATH)
lnname_pl = pl.from_pandas(lnname_df)
print(f"Total records in LN.LNNAME: {len(lnname_pl)}")

# Merge with RLSLIST - include NEWIC for encryption (as in SAS code)
pbbname = lnname_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pbbname = pbbname.sort('ACCTNO')

print(f"Records in PBBNAME after merge: {len(pbbname)}")

# Define valid characters for ID validation (alphanumeric and space)
VALID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 '

# Add validated and encrypted ID using NEWIC (as in SAS: ID = COMPRESS(NEWIC))
pbbname = pbbname.with_columns([
    pl.col('NEWIC').map_elements(lambda x: validate_id(x, VALID_CHARS), return_dtype=pl.Utf8).alias('ID')
])

pbbname = pbbname.with_columns([
    pl.col('ID').map_elements(encr_id, return_dtype=pl.Utf8).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD (SAS logic)
# Handle potential float storage of MAILCODE
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
                f"{safe_str(row.get('NEWIC')):<17.17}"  # Use NEWIC as in SAS
            )
            f.write(line + '\n')

    print(f"EMXPBB records written: {len(mailpbb)}")


# ============================================================================
# PIB PROCESSING
# ============================================================================

print("\nStep 3: Processing PIB data...")

# Load LNI.LNNAME
lni_df, lni_meta = pyreadstat.read_sas7bdat(LNI_LNNAME_PATH)
lni_pl = pl.from_pandas(lni_df)
print(f"Total records in LNI.LNNAME: {len(lni_pl)}")

# Merge with RLSLIST - include NEWIC for encryption
pibname = lni_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pibname = pibname.sort('ACCTNO')

print(f"Records in PIBNAME after merge: {len(pibname)}")

# Add validated and encrypted ID using NEWIC
pibname = pibname.with_columns([
    pl.col('NEWIC').map_elements(lambda x: validate_id(x, VALID_CHARS), return_dtype=pl.Utf8).alias('ID')
])

pibname = pibname.with_columns([
    pl.col('ID').map_elements(encr_id, return_dtype=pl.Utf8).alias('MASK_IDS')
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
                f"{safe_str(row.get('NEWIC')):<17.17}"  # Use NEWIC as in SAS
            )
            f.write(line + '\n')

    print(f"EMXPIB records written: {len(mailpib)}")


# ============================================================================
# REPORT GENERATION
# ============================================================================

print("\nStep 4: Generating report...")

# Combine PBBNAME and PIBNAME, add NOEMC flag, and sort
rlslist_report = pl.concat([pbbname, pibname])
rlslist_report = rlslist_report.with_columns([
    pl.lit(1).alias('NOEMC')
])
rlslist_report = rlslist_report.sort(['BRANCH', 'ACCTNO', 'NOTENO'])

print(f"Total records for report: {len(rlslist_report)}")


def format_branch_code(branch_val):
    """Format branch as 3-digit string"""
    branch_int = safe_int(branch_val, default=0)
    return f"{branch_int:3d}"


def write_report_with_asa():
    """Generate report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0
    page_number = 1

    with open(REPORT_PATH, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def new_page():
            """Start a new page"""
            nonlocal line_count, page_number
            write_line('1', ' ' * 130)  # Form feed
            write_line(' ', f"{'REPORT ID : EIQPROM2':^130}")
            write_line(' ', f"AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT {RDATE}".center(130))
            write_line(' ', ' ' * 130)
            write_line(' ',
                       f"{'BRANCH':<7} {'BRANCH':<6} {'A/C NO':>10} {'NOTE':>5} {'PRODUCT':<8} {'NAME OF BORROWER/CUSTOMER':<40} {'MAIL CODE':>2}")
            write_line(' ', f"{'CODE':<7} {'':<6} {'':<10} {'NO':>5} {'CODE':<8} {'':<40} {'':<2}")
            write_line(' ', '-' * 130)
            line_count = 7
            page_number += 1

        # Write first page header
        new_page()

        current_branch = None
        branch_count = 0

        for row in rlslist_report.iter_rows(named=True):

            branch = safe_int(row.get('BRANCH'))
            
            # Check for branch break
            if current_branch is not None and branch != current_branch:
                # Write branch summary
                write_line(' ', ' ' * 130)
                write_line(' ', '   ' + '-' * 123)
                write_line(' ', f"   NO OF BORROWER/CUSTOMER :{branch_count:8,}")
                write_line(' ', ' ' * 130)
                line_count += 4
                branch_count = 0

            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 5:
                new_page()

            current_branch = branch
            branch_count += 1

            # Format and write detail line
            branch_code = f"{branch:3d}"
            brch = format_branch_code(row.get('BRANCH'))
            acctno = safe_int(row.get('ACCTNO'))
            noteno = safe_int(row.get('NOTENO'))
            product = f"{safe_str(row.get('PRODUCT')):<8.8}"
            nameln1 = f"{safe_str(row.get('NAMELN1')):<40.40}"
            mailcode = f"{safe_str(row.get('MAILCODE')):>2.2}"

            line = f" {branch_code} {brch} {acctno:10d} {noteno:5d} {product} {nameln1} {mailcode}"
            write_line(' ', line)

        # Write final branch summary
        if branch_count > 0:
            write_line(' ', ' ' * 130)
            write_line(' ', '   ' + '-' * 123)
            write_line(' ', f"   NO OF BORROWER/CUSTOMER :{branch_count:8,}")
            write_line(' ', ' ' * 130)

    print(f"Report written to: {REPORT_PATH}")
    print(f"Total pages: {page_number - 1}")


write_report_with_asa()

print("\n" + "=" * 70)
print("EIEMCRLS processing completed successfully!")
print("=" * 70)
print(f"Report Date: {RDATE}")
print(f"Data Month: {REPTMON}-{REPTYR}")
print("=" * 70)
