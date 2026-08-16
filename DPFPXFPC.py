#!/usr/bin/env python3
"""
File Name: EIEMCRLS.py
Report ID: EIQPROM2
Automailing Listing for Reinstatement of Loan
Exact SAS logic translation
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
    SAS: REPTDATE = TODAY()-DAY(TODAY());
    """
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
    """
    SAS: %ENCR_ID macro
    ID = COMPRESS(NEWIC); then encrypt
    """
    if not id_value or pd.isna(id_value) or str(id_value).strip() == '':
        return ' ' * 40
    
    # SAS: IDS = UPCASE(ID)
    ids = str(id_value).upper()
    
    mask_ids = ""
    
    # SAS: TRANSLATE(CH_X,'BCADX9GEHI','0123456789')
    translation_map = {
        '0': 'B', '1': 'C', '2': 'A', '3': 'D', '4': 'X',
        '5': '9', '6': 'G', '7': 'E', '8': 'H', '9': 'I'
    }
    
    # SAS: DO I=1 TO LENGTH(IDS)
    for char in ids:
        # SAS: CH_X = RANK(PUT(CH,$ASCII.))
        ascii_val = ord(char)
        ch_x = str(ascii_val)
        
        # SAS: MASK = TRANSLATE(CH_X,'BCADX9GEHI','0123456789')
        mask = ""
        for digit in ch_x:
            if digit in translation_map:
                mask += translation_map[digit]
            else:
                mask += digit
        
        # SAS: MASK_IDS = STRIP(MASK_IDS)|| MASK
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
# DATA PROCESSING - EXACT SAS LOGIC
# ============================================================================

print("\n" + "="*70)
print("Starting EIQPROM2 processing...")
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")
print("="*70)

# Step 1: PROC SORT DATA=PROMOTE.LOAN&REPTMON OUT=RLSLIST;
#         BY GUAREND DESCENDING REPAID;
#         WHERE REPAID > 100000;
print("\nStep 1: Loading and filtering PROMOTE.LOAN data...")
promote_path = PROMOTE_LOAN_PATH.format(month=REPTMON)
print(f"Loading file: {promote_path}")

# Read SAS file
loan_df, loan_meta = pyreadstat.read_sas7bdat(promote_path)
loan_pl = pl.from_pandas(loan_df)

print(f"Total records in LOAN file: {len(loan_pl)}")

# WHERE REPAID > 100000
rlslist = loan_pl.filter(pl.col('REPAID') > 100000)
print(f"Records after WHERE REPAID > 100000: {len(rlslist)}")

# BY GUAREND DESCENDING REPAID
rlslist = rlslist.sort(['GUAREND', 'REPAID'], descending=[False, True])

# PROC SORT DATA=RLSLIST NODUPKEY; BY GUAREND;
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')
print(f"Records after NODUPKEY BY GUAREND: {len(rlslist)}")

# PROC SORT DATA=RLSLIST; BY ACCTNO;
rlslist = rlslist.sort('ACCTNO')
print(f"Final RLSLIST records: {len(rlslist)}")


# ============================================================================
# Step 2: PBB PROCESSING
# ============================================================================

print("\n" + "="*70)
print("Step 2: Processing PBB data (LN.LNNAME)")
print("="*70)

# PROC SORT DATA=LN.LNNAME OUT=PBBNAME; BY ACCTNO;
lnname_df, lnname_meta = pyreadstat.read_sas7bdat(LN_LNNAME_PATH)
lnname_pl = pl.from_pandas(lnname_df)
print(f"Total records in LN.LNNAME: {len(lnname_pl)}")

# DATA PBBNAME; MERGE PBBNAME(IN=A) RLSLIST(IN=B); BY ACCTNO; IF A AND B;
pbbname = lnname_pl.join(
    rlslist,  # Merge all columns from RLSLIST
    on='ACCTNO', 
    how='inner'
)
print(f"Records after MERGE (A AND B): {len(pbbname)}")

# DATA PBBNAME MAILPBB; SET PBBNAME;
# ID = COMPRESS(NEWIC);
# %ENCR_ID;
pbbname = pbbname.with_columns([
    pl.col('NEWIC').map_elements(
        lambda x: encr_id(str(x).replace(' ', '') if pd.notna(x) else ' '), 
        return_dtype=pl.Utf8
    ).alias('MASK_IDS')
])

# IF MAILCODE IN (' ','13','14') AND EMAILADD NE '' THEN OUTPUT MAILPBB;
# ELSE OUTPUT PBBNAME;
mailpbb = pbbname.filter(
    (pl.col('MAILCODE').cast(pl.Utf8).str.strip_chars().is_in(['', '13', '14'])) &
    (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != '')
)

pbbname = pbbname.filter(
    ~((pl.col('MAILCODE').cast(pl.Utf8).str.strip_chars().is_in(['', '13', '14'])) &
      (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != ''))
)

print(f"PBBNAME (non-email): {len(pbbname)}")
print(f"MAILPBB (email): {len(mailpbb)}")

# Write EMCPBB - DATA _NULL_; SET PBBNAME; FILE EMCPBB;
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

# Write EMCPBBS summary
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

    # DATA MAILPBB; SET MAILPBB; ROWCNT+1;
    mailpbb = mailpbb.with_columns([
        pl.Series(name='ROWCNT', values=range(1, len(mailpbb) + 1))
    ])

    # VAR_ID = COMPRESS("PBB_EMAIL_STMT_RIL_C"||PUT(ROWCNT,Z10.)||"_&INDXDT");
    mailpbb = mailpbb.with_columns([
        (pl.lit("PBB_EMAIL_STMT_RIL_C") +
         pl.col('ROWCNT').cast(pl.Utf8).str.zfill(10) +
         pl.lit("_") +
         pl.lit(INDXDT)).alias('VAR_ID'),
        (pl.lit(MTHNAM) + pl.lit(REPTYR)).alias('STATE_DTE')
    ])

    # Write EMLPBB
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

    # Write EMLPBBS summary
    with open(EMLPBBS_PATH, 'w') as f:
        line = (
            f"LNRIHLCE"
            f"{' ' * 22}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"EMLPBB records written: {rowcnt}")

    # Write EMXPBB index
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
# Step 3: PIB PROCESSING
# ============================================================================

print("\n" + "="*70)
print("Step 3: Processing PIB data (LNI.LNNAME)")
print("="*70)

# Load LNI.LNNAME
lni_df, lni_meta = pyreadstat.read_sas7bdat(LNI_LNNAME_PATH)
lni_pl = pl.from_pandas(lni_df)
print(f"Total records in LNI.LNNAME: {len(lni_pl)}")

# Merge with RLSLIST
pibname = lni_pl.join(
    rlslist,
    on='ACCTNO', 
    how='inner'
)
print(f"Records after MERGE: {len(pibname)}")

# Add encrypted ID
pibname = pibname.with_columns([
    pl.col('NEWIC').map_elements(
        lambda x: encr_id(str(x).replace(' ', '') if pd.notna(x) else ' '), 
        return_dtype=pl.Utf8
    ).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpib = pibname.filter(
    (pl.col('MAILCODE').cast(pl.Utf8).str.strip_chars().is_in(['', '13', '14'])) &
    (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != '')
)

pibname = pibname.filter(
    ~((pl.col('MAILCODE').cast(pl.Utf8).str.strip_chars().is_in(['', '13', '14'])) &
      (pl.col('EMAILADD').cast(pl.Utf8).str.strip_chars() != ''))
)

print(f"PIBNAME (non-email): {len(pibname)}")
print(f"MAILPIB (email): {len(mailpib)}")

# Write EMCPIB
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

# Write EMCPIBS summary
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

    # Write EMLPIB
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

    # Write EMLPIBS summary
    with open(EMLPIBS_PATH, 'w') as f:
        line = (
            f"LNRIHLIPE"
            f"{' ' * 21}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"EMLPIB records written: {rowcnt}")

    # Write EMXPIB index
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
# Step 4: REPORT GENERATION
# ============================================================================

print("\n" + "="*70)
print("Step 4: Generating report...")
print("="*70)

# DATA RLSLIST; SET PBBNAME PIBNAME; NOEMC = 1;
rlslist_report = pl.concat([pbbname, pibname])
rlslist_report = rlslist_report.with_columns([
    pl.lit(1).alias('NOEMC')
])

# PROC SORT DATA=RLSLIST; BY BRANCH ACCTNO NOTENO;
rlslist_report = rlslist_report.sort(['BRANCH', 'ACCTNO', 'NOTENO'])

print(f"Total records for report: {len(rlslist_report)}")


def write_report_matching_sas():
    """Generate report matching SAS PROC REPORT output"""
    
    with open(REPORT_PATH, 'w') as f:
        
        # TITLE "REPORT ID : EIQPROM2";
        # TITLE2 "AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT &RDATE";
        f.write(f"REPORT ID : EIQPROM2\n")
        f.write(f"AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT {RDATE}\n")
        f.write(f"\n")
        
        # Column headers matching PROC REPORT with split
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
            
            # MAILCODE formatting
            mailcode_raw = row.get('MAILCODE')
            if mailcode_raw is None or pd.isna(mailcode_raw):
                mailcode = ''
            else:
                mailcode = str(mailcode_raw).strip()
                if '.' in mailcode:
                    mailcode = mailcode.rstrip('0').rstrip('.')
                if mailcode == '0':
                    mailcode = ''
            
            # BRCH field
            brch = row.get('BRCH')
            if brch is None or pd.isna(brch):
                brch = ''
            else:
                brch = str(brch).strip()
            
            # BREAK AFTER BRANCH
            if current_branch is not None and branch != current_branch:
                f.write(f"\n")
                f.write(f"    " + "-"*123 + "\n")
                f.write(f"    NO OF BORROWER/CUSTOMER :{branch_count:>8,}\n")
                f.write(f"\n")
                branch_count = 0
                current_branch = None
            
            # GROUP behavior - show branch only on first occurrence
            if current_branch is None:
                branch_display = f"{branch:>7d}"
                current_branch = branch
            else:
                branch_display = " " * 7
            
            branch_count += 1
            
            # Detail line
            line = f" {branch_display}  {brch:<5} {acctno:>10d} {noteno:>5d} {product:>8d} {nameln1:<40} {mailcode:>2}\n"
            f.write(line)
        
        # Final branch summary
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
