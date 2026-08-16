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
    """Calculate report dates based on today's date minus 1 day"""
    today = datetime.now()
    reptdate = today - timedelta(days=1)
    
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


# ============================================================================
# DATA PROCESSING
# ============================================================================

print("Starting EIQPROM2 processing...")
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")

# Step 1: Load and filter PROMOTE.LOAN data
print("\nStep 1: Loading and filtering PROMOTE.LOAN data...")
promote_path = PROMOTE_LOAN_PATH.format(month=REPTMON)

# Read SAS file using pyreadstat
loan_df, loan_meta = pyreadstat.read_sas7bdat(promote_path)
loan_pl = pl.from_pandas(loan_df)

# Filter records where REPAID > 100000
rlslist = loan_pl.filter(pl.col('REPAID') > 100000)

# Sort by GUAREND, REPAID DESC
rlslist = rlslist.sort(['GUAREND', 'REPAID'], descending=[False, True])

# Remove duplicates keeping first record per GUAREND
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')

# Sort by ACCTNO for merge
rlslist = rlslist.sort('ACCTNO')

print(f"  Records in RLSLIST: {len(rlslist)}")


# ============================================================================
# PBB PROCESSING
# ============================================================================

print("\nStep 2: Processing PBB data...")

# Load LN.LNNAME
lnname_df, lnname_meta = pyreadstat.read_sas7bdat(LN_LNNAME_PATH)
lnname_pl = pl.from_pandas(lnname_df)

# Merge with RLSLIST - include necessary fields from rlslist
pbbname = lnname_pl.join(
    rlslist.select(['ACCTNO', 'GUAREND', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pbbname = pbbname.sort('ACCTNO')

print(f"  Records in PBBNAME after merge: {len(pbbname)}")

# Define valid characters for ID validation (alphanumeric and space)
VALID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 '

# Add validated and encrypted ID using GUAREND (AR_GUAR in SAS)
pbbname = pbbname.with_columns([
    pl.col('GUAREND').map_elements(lambda x: validate_id(x, VALID_CHARS), return_dtype=pl.Utf8).alias('ID')
])

pbbname = pbbname.with_columns([
    pl.col('ID').map_elements(encr_id, return_dtype=pl.Utf8).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpbb = pbbname.filter(
    (pl.col('MAILCODE').is_in([' ', '13', '14'])) &
    (pl.col('EMAILADD').str.strip_chars() != '')
)

pbbname = pbbname.filter(
    ~((pl.col('MAILCODE').is_in([' ', '13', '14'])) &
      (pl.col('EMAILADD').str.strip_chars() != ''))
)

print(f"  Records in PBBNAME (non-email): {len(pbbname)}")
print(f"  Records in MAILPBB (email): {len(mailpbb)}")

# Write EMCPBB file
print("\n  Writing EMCPBB file...")
with open(EMCPBB_PATH, 'w') as f:
    rowcnt = 0
    for row in pbbname.iter_rows(named=True):
        rowcnt += 1
        line = (
            f"B"
            f"{REPTDT}"
            f"{str(row['NAMELN1'] or ''):<40.40}"
            f"{str(row['NAMELN2'] or ''):<40.40}"
            f"{str(row['NAMELN3'] or ''):<40.40}"
            f"{str(row['NAMELN4'] or ''):<40.40}"
            f"{str(row['NAMELN5'] or ''):<40.40}"
            f"{row['BRANCH']:07d}"
            f"{row['ACCTNO']:011d}"
            f"{row['MASK_IDS']:<24.24}"
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

print(f"  EMCPBB records written: {rowcnt}")

# Process MAILPBB
if len(mailpbb) > 0:
    print("\n  Processing MAILPBB email statements...")

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
    print("  Writing EMLPBB file...")
    with open(EMLPBB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpbb.iter_rows(named=True):
            rowcnt += 1
            line = (
                f"B"
                f"{REPTDT}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NAMELN2'] or ''):<40.40}"
                f"{str(row['NAMELN3'] or ''):<40.40}"
                f"{str(row['NAMELN4'] or ''):<40.40}"
                f"{str(row['NAMELN5'] or ''):<40.40}"
                f"{row['BRANCH']:07d}"
                f"{row['ACCTNO']:011d}"
                f"{' '}"
                f"{row['VAR_ID']:<40.40}"
                f"{' '}"
                f"{row['MASK_IDS']:<24.24}"
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

    print(f"  EMLPBB records written: {rowcnt}")

    # Write EMXPBB index file
    print("  Writing EMXPBB index file...")
    with open(EMXPBB_PATH, 'w') as f:
        for row in mailpbb.iter_rows(named=True):
            line = (
                f"{row['VAR_ID']:<40.40}"
                f"{str(row['EMAILADD'] or ''):<60.60}"
                f"{row['STATE_DTE']:<7.7}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['ID'] or ''):<17.17}"  # Use validated ID
            )
            f.write(line + '\n')

    print(f"  EMXPBB records written: {len(mailpbb)}")


# ============================================================================
# PIB PROCESSING
# ============================================================================

print("\nStep 3: Processing PIB data...")

# Load LNI.LNNAME
lni_df, lni_meta = pyreadstat.read_sas7bdat(LNI_LNNAME_PATH)
lni_pl = pl.from_pandas(lni_df)

# Merge with RLSLIST - include necessary fields from rlslist
pibname = lni_pl.join(
    rlslist.select(['ACCTNO', 'GUAREND', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pibname = pibname.sort('ACCTNO')

print(f"  Records in PIBNAME after merge: {len(pibname)}")

# Add validated and encrypted ID using GUAREND (AR_GUAR in SAS)
pibname = pibname.with_columns([
    pl.col('GUAREND').map_elements(lambda x: validate_id(x, VALID_CHARS), return_dtype=pl.Utf8).alias('ID')
])

pibname = pibname.with_columns([
    pl.col('ID').map_elements(encr_id, return_dtype=pl.Utf8).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpib = pibname.filter(
    (pl.col('MAILCODE').is_in([' ', '13', '14'])) &
    (pl.col('EMAILADD').str.strip_chars() != '')
)

pibname = pibname.filter(
    ~((pl.col('MAILCODE').is_in([' ', '13', '14'])) &
      (pl.col('EMAILADD').str.strip_chars() != ''))
)

print(f"  Records in PIBNAME (non-email): {len(pibname)}")
print(f"  Records in MAILPIB (email): {len(mailpib)}")

# Write EMCPIB file
print("\n  Writing EMCPIB file...")
with open(EMCPIB_PATH, 'w') as f:
    rowcnt = 0
    for row in pibname.iter_rows(named=True):
        rowcnt += 1
        line = (
            f"B"
            f"{REPTDT}"
            f"{str(row['NAMELN1'] or ''):<40.40}"
            f"{str(row['NAMELN2'] or ''):<40.40}"
            f"{str(row['NAMELN3'] or ''):<40.40}"
            f"{str(row['NAMELN4'] or ''):<40.40}"
            f"{str(row['NAMELN5'] or ''):<40.40}"
            f"{row['BRANCH']:07d}"
            f"{row['ACCTNO']:011d}"
            f"{row['MASK_IDS']:<24.24}"
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

print(f"  EMCPIB records written: {rowcnt}")

# Process MAILPIB
if len(mailpib) > 0:
    print("\n  Processing MAILPIB email statements...")

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
    print("  Writing EMLPIB file...")
    with open(EMLPIB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpib.iter_rows(named=True):
            rowcnt += 1
            line = (
                f"B"
                f"{REPTDT}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NAMELN2'] or ''):<40.40}"
                f"{str(row['NAMELN3'] or ''):<40.40}"
                f"{str(row['NAMELN4'] or ''):<40.40}"
                f"{str(row['NAMELN5'] or ''):<40.40}"
                f"{row['BRANCH']:07d}"
                f"{row['ACCTNO']:011d}"
                f"{' '}"
                f"{row['VAR_ID']:<40.40}"
                f"{' '}"
                f"{row['MASK_IDS']:<24.24}"
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

    print(f"  EMLPIB records written: {rowcnt}")

    # Write EMXPIB index file
    print("  Writing EMXPIB index file...")
    with open(EMXPIB_PATH, 'w') as f:
        for row in mailpib.iter_rows(named=True):
            line = (
                f"{row['VAR_ID']:<40.40}"
                f"{str(row['EMAILADD'] or ''):<60.60}"
                f"{row['STATE_DTE']:<7.7}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['ID'] or ''):<17.17}"  # Use validated ID
            )
            f.write(line + '\n')

    print(f"  EMXPIB records written: {len(mailpib)}")


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

print(f"  Total records for report: {len(rlslist_report)}")


def format_branch_code(branch_val):
    """Format branch as 3-digit string"""
    if branch_val is None or pd.isna(branch_val):
        return '   '
    return f"{int(branch_val):3d}"


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

            # Check for branch break
            if current_branch is not None and row['BRANCH'] != current_branch:
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

            current_branch = row['BRANCH']
            branch_count += 1

            # Format and write detail line
            branch_code = f"{row['BRANCH']:3d}" if row['BRANCH'] is not None else '   '
            brch = format_branch_code(row['BRANCH'])
            acctno = f"{row['ACCTNO']:10d}" if row['ACCTNO'] is not None else ' ' * 10
            noteno = f"{row['NOTENO']:5d}" if row['NOTENO'] is not None else ' ' * 5
            product = f"{str(row['PRODUCT'] or ''):<8.8}"
            nameln1 = f"{str(row['NAMELN1'] or ''):<40.40}"
            mailcode = f"{str(row['MAILCODE'] or ''):>2.2}"

            line = f" {branch_code} {brch} {acctno} {noteno} {product} {nameln1} {mailcode}"
            write_line(' ', line)

        # Write final branch summary
        if branch_count > 0:
            write_line(' ', ' ' * 130)
            write_line(' ', '   ' + '-' * 123)
            write_line(' ', f"   NO OF BORROWER/CUSTOMER :{branch_count:8,}")
            write_line(' ', ' ' * 130)

    print(f"  Report written to: {REPORT_PATH}")
    print(f"  Total pages: {page_number - 1}")


write_report_with_asa()

print("\n" + "=" * 70)
print("EIEMCRLS processing completed successfully!")
print("=" * 70)
