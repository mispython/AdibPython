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
    """
    Calculate report dates matching SAS logic:
    REPTDATE = TODAY() - DAY(TODAY())
    This gives the last day of the previous month
    
    Args:
        run_date: Optional datetime object. If provided, use this as "today"
                 Otherwise use actual today's date
    """
    if run_date is None:
        today = datetime.now()
    else:
        today = run_date
    
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


# Check if a specific run date is provided as command line argument
# Usage: python EIEMCRLS.py 2026-04-15  (to simulate running on April 15, 2026)
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

def validate_id(ar_guar, valid_chars):
    """VALIDATE_ID macro equivalent"""
    if ar_guar is None or pd.isna(ar_guar) or str(ar_guar).strip() == '':
        return ' '
    
    ar_guar = str(ar_guar).strip()
    invalid_chars = [c for c in ar_guar if c not in valid_chars]
    
    if invalid_chars:
        translation_table = str.maketrans({c: ' ' for c in invalid_chars})
        return ar_guar.translate(translation_table)
    else:
        return ar_guar


def encr_id(id_value):
    """ENCR_ID macro equivalent"""
    if not id_value or pd.isna(id_value) or str(id_value).strip() == '' or str(id_value).strip() == ' ':
        return ' ' * 40
    
    ids = str(id_value).upper().strip()
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

# Remove duplicates keeping first record per GUAREND
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')
print(f"Records after deduplication: {len(rlslist)}")

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

# Merge with RLSLIST
pbbname = lnname_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pbbname = pbbname.sort('ACCTNO')

print(f"Records in PBBNAME after merge: {len(pbbname)}")

# Define valid characters
VALID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 '

# Add validated and encrypted ID
pbbname = pbbname.with_columns([
    pl.col('NEWIC').map_elements(lambda x: validate_id(x, VALID_CHARS), return_dtype=pl.Utf8).alias('ID')
])

pbbname = pbbname.with_columns([
    pl.col('ID').map_elements(encr_id, return_dtype=pl.Utf8).alias('MASK_IDS')
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

print("\nStep 3: Processing PIB data...")

# Load LNI.LNNAME
lni_df, lni_meta = pyreadstat.read_sas7bdat(LNI_LNNAME_PATH)
lni_pl = pl.from_pandas(lni_df)

# Merge with RLSLIST
pibname = lni_pl.join(
    rlslist.select(['ACCTNO', 'NEWIC', 'MAILCODE', 'EMAILADD', 'BRANCH', 'NOTENO', 'PRODUCT']), 
    on='ACCTNO', 
    how='inner'
)
pibname = pibname.sort('ACCTNO')

print(f"Records in PIBNAME after merge: {len(pibname)}")

# Add validated and encrypted ID
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
                f"{safe_str(row.get('NEWIC')):<17.17}"
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


def write_report_matching_sas():
    """
    Generate report matching SAS PROC REPORT output format
    """
    
    with open(REPORT_PATH, 'w') as f:
        
        # Write title lines
        f.write(f"REPORT ID : EIQPROM2\n")
        f.write(f"AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT {RDATE}\n")
        f.write(f"\n")
        
        # Write header lines matching SAS PROC REPORT format
        f.write(f" {'':<3} {'':<5} {'':<10} {'':<5} {'':<8} {'':<40} {'MA':<2}\n")
        f.write(f" {'':<3} {'':<5} {'':<10} {'':<5} {'':<8} {'':<40} {'IL':<2}\n")
        f.write(f" {'BRANCH':<3} {'':<5} {'':<10} {'NOTE':<5} {'PRODUCT':<8} {'':<40} {'CO':<2}\n")
        f.write(f" {'CODE':<3} {'BRANCH':<5} {'A/C NO':<10} {'NO':<5} {'CODE':<8} {'NAME OF BORROWER/CUSTOMER':<40} {'DE':<2}\n")
        f.write(f" {'':<3} {'':<5} {'':<10} {'':<5} {'':<8} {'':<40} {'':<2}\n")
        f.write(f"\n")
        
        current_branch = None
        branch_count = 0
        
        for row in rlslist_report.iter_rows(named=True):
            branch = safe_int(row.get('BRANCH'))
            
            # Check for branch break
            if current_branch is not None and branch != current_branch:
                # Write branch summary
                f.write(f"    {'-'*123}\n")
                f.write(f"    NO OF BORROWER/CUSTOMER :{branch_count:>8,}\n")
                f.write(f"\n")
                branch_count = 0
            
            # Only show branch code on first occurrence
            if current_branch != branch:
                branch_display = f"{branch:>3d}"
                current_branch = branch
            else:
                branch_display = "   "
            
            branch_count += 1
            
            # Format fields
            acctno = safe_int(row.get('ACCTNO'))
            noteno = safe_int(row.get('NOTENO'))
            product = safe_str(row.get('PRODUCT'))
            nameln1 = safe_str(row.get('NAMELN1'))
            mailcode = safe_str(row.get('MAILCODE'))
            
            # BRCH field - need to get from data if available
            brch = safe_str(row.get('BRCH'), 'JSS')  # Default to JSS if not in data
            
            # Write detail line in SAS PROC REPORT format
            line = f" {branch_display} {brch:<5} {acctno:>10d} {noteno:>5d} {product:<8} {nameln1:<40} {mailcode:>2}\n"
            f.write(line)
        
        # Write final branch summary
        if branch_count > 0:
            f.write(f"    {'-'*123}\n")
            f.write(f"    NO OF BORROWER/CUSTOMER :{branch_count:>8,}\n")
    
    print(f"Report written to: {REPORT_PATH}")


write_report_matching_sas()

print("\n" + "=" * 70)
print("EIEMCRLS processing completed successfully!")
print("=" * 70)
print(f"Report Date: {RDATE}")
print(f"Data Month: {REPTMON}-{REPTYR}")
print("=" * 70)
