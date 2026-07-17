import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

# Input paths
BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"

# Output paths
OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

# File extensions
SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

# Date configuration
USE_CURRENT_DATE = False
CUSTOM_DATE = datetime(2026, 7, 16)
DAYS_OFFSET = 1

# ============================================
# FUNCTIONS
# ============================================

def read_sas7bdat(file_path):
    """Read SAS7BDAT file and return as Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

def format_number(num):
    """Format number with commas and 2 decimal places"""
    if num is None or pd.isna(num):
        return "0.00"
    try:
        return f"{float(num):,.2f}"
    except:
        return "0.00"

def write_formatted_report(df, filename, title, report_date, total_label="TOTAL"):
    """Write formatted report with headers and footers matching SAS output"""
    
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas for easier manipulation
    pdf = df.to_pandas()
    
    # Group by FMTNAME if exists, otherwise use a default
    if 'FMTNAME' in pdf.columns:
        groups = pdf.groupby('FMTNAME')
    else:
        # Create a dummy group
        pdf['FMTNAME'] = 'DETAIL'
        groups = pdf.groupby('FMTNAME')
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'FMTNAME':<10} {'BNMCODE':<20} {'DESC':<50} {'SIGN':<6} {'AMOUNT':>20} {'TOTAL':>20}\n")
        f.write("-" * 118 + "\n")
        
        # Process each group
        for fmtname, group in groups:
            total_amount = 0
            
            # Write each row
            for _, row in group.iterrows():
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                desc = str(row.get('DESC', ''))[:50]
                sign = str(row.get('SIGN', '+'))[:6]
                amount = format_number(row.get('AMOUNT', 0))
                
                # For total column, show same as amount for now (will sum at group level)
                f.write(f"{fmtname:<10} {bnmcode:<20} {desc:<50} {sign:<6} {amount:>20} {amount:>20}\n")
                total_amount += float(row.get('AMOUNT', 0) or 0)
            
            # Write group total
            f.write("-" * 118 + "\n")
            f.write(f"{'TOTAL FOR':<10} {fmtname:<20} {'':<50} {'':<6} {format_number(total_amount):>20} {format_number(total_amount):>20}\n")
            f.write(" " * 73 + "-" * 45 + "\n")
            f.write("\n")
        
        # Write grand total if multiple groups
        if len(groups) > 1:
            grand_total = pdf['AMOUNT'].sum()
            f.write("\n" + "=" * 118 + "\n")
            f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<50} {'':<6} {format_number(grand_total):>20} {format_number(grand_total):>20}\n")
            f.write("=" * 118 + "\n")

def write_detailed_report(df, filename, title, report_date):
    """Write detailed report with ELDAY grouping"""
    
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas
    pdf = df.to_pandas()
    
    # Group by ELDAY
    if 'ELDAY' in pdf.columns:
        groups = pdf.groupby('ELDAY')
    else:
        pdf['ELDAY'] = 'TOTAL'
        groups = pdf.groupby('ELDAY')
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'ELDAY':<10} {'BNMCODE':<20} {'UTSTY':<10} {'UTREF':<10} {'AMOUNT':>20} {'BNMCODG':<20}\n")
        f.write("-" * 100 + "\n")
        
        # Process each group
        for elday, group in groups:
            total_amount = 0
            
            # Write each row
            for _, row in group.iterrows():
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                utsty = str(row.get('UTSTY', ''))[:10]
                utref = str(row.get('UTREF', ''))[:10]
                amount = format_number(row.get('AMOUNT', 0))
                bnmcodg = str(row.get('BNMCODG', ''))[:20]
                
                f.write(f"{elday:<10} {bnmcode:<20} {utsty:<10} {utref:<10} {amount:>20} {bnmcodg:<20}\n")
                total_amount += float(row.get('AMOUNT', 0) or 0)
            
            # Write group total
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL FOR':<10} {elday:<20} {'':<10} {'':<10} {format_number(total_amount):>20} {'':<20}\n")
            f.write("\n")

def write_variance_report(df, filename, report_date):
    """Write variance report with formatted output"""
    
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas
    pdf = df.to_pandas()
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"KAPITI WALKER VARIANCE REPORT\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'BNMCODE':<20} {'ELDAY':<10} {'AMOUNT_SUM':>20} {'WALWAMT':>20} {'VARIANC':>20}\n")
        f.write("-" * 100 + "\n")
        
        # Write each row
        total_variance = 0
        for _, row in pdf.iterrows():
            bnmcode = str(row.get('BNMCODE', ''))[:20]
            elday = str(row.get('ELDAY', ''))[:10]
            amount_sum = format_number(row.get('AMOUNT_SUM', 0))
            walwamt = format_number(row.get('WALWAMT', 0))
            varianc = format_number(row.get('VARIANC', 0))
            
            f.write(f"{bnmcode:<20} {elday:<10} {amount_sum:>20} {walwamt:>20} {varianc:>20}\n")
            total_variance += float(row.get('VARIANC', 0) or 0)
        
        # Write total
        f.write("-" * 100 + "\n")
        f.write(f"{'TOTAL VARIANCE':<30} {'':<10} {'':<20} {'':<20} {format_number(total_variance):>20}\n")
        f.write("=" * 100 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

# Determine the reporting date
if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

# Process dates
SDESC = "PUBLIC BANK BERHAD"
RPDATE = REPTDATE_LOAN.strftime("%d%m%y")
RDATE = REPTDATE_LOAN.strftime("%d%m%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
MTHEND = f"{REPTDATE_LOAN.day:02d}"

# Process BNMK date
REPTDATE_BNMK = REPTDATE_LOAN
MM = REPTDATE_BNMK.month
DAY = REPTDATE_BNMK.day

# Determine week
if 1 <= DAY <= 8:
    WK = '4'
elif 9 <= DAY <= 15:
    WK = '1'
elif 16 <= DAY <= 22:
    WK = '2'
else:
    WK = '3'

# Adjust month for week 4
if WK == '4':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
    MM = MM1
    if MM == 12:
        SXDATE = REPTDATE_BNMK.replace(month=1, day=1) - timedelta(days=365)
    else:
        SXDATE = REPTDATE_BNMK.replace(month=MM, day=1) - timedelta(days=1)
else:
    SXDATE = REPTDATE_BNMK

NOWK = WK
REPTMON = f"{MM:02d}"
RPDATE_BNMK = SXDATE.strftime("%d%m%y")
RPYEAR = SXDATE.strftime("%Y")
REPTYEAR = SXDATE.strftime("%Y")
YEAR_SHORT = SXDATE.strftime("%y")

print(f"Report Period: Week {WK}, Month {REPTMON}")
print(f"Reading files: rep2{REPTMON}{WK}, rep4{REPTMON}{WK}, elw{REPTMON}{WK}")

# ============================================
# PROCESS DATA FILES
# ============================================

# Process REP2
rep2_file = f"{BNMK_INPUT_PATH}/rep2{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep2_file}")
REP2_DF = read_sas7bdat(rep2_file)

if REP2_DF.height > 0:
    print(f"REP2 record count: {REP2_DF.height}")
    REP2_FILTERED = REP2_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
    print(f"REP2 after filter: {REP2_FILTERED.height} records")
else:
    REP2_FILTERED = pl.DataFrame()
    print(f"Warning: {rep2_file} not found or empty")

# Process REP4
rep4_file = f"{BNMK_INPUT_PATH}/rep4{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep4_file}")
REP4_DF = read_sas7bdat(rep4_file)

if REP4_DF.height > 0:
    print(f"REP4 record count: {REP4_DF.height}")
    REP4_FILTERED = REP4_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
    print(f"REP4 after filter: {REP4_FILTERED.height} records")
else:
    REP4_FILTERED = pl.DataFrame()
    print(f"Warning: {rep4_file} not found or empty")

# Combine REP2 and REP4
if REP2_FILTERED.height > 0 and REP4_FILTERED.height > 0:
    REP2_COMBINED = pl.concat([REP2_FILTERED, REP4_FILTERED])
    print(f"Combined REP2+REP4: {REP2_COMBINED.height} records")
elif REP2_FILTERED.height > 0:
    REP2_COMBINED = REP2_FILTERED
elif REP4_FILTERED.height > 0:
    REP2_COMBINED = REP4_FILTERED
else:
    REP2_COMBINED = pl.DataFrame()
    print("Error: No data available from REP2 or REP4")

# ============================================
# TRANSFORM DATA
# ============================================

if REP2_COMBINED.height > 0:
    # Check if NETAMT exists
    if "NETAMT" in REP2_COMBINED.columns:
        amount_col = "NETAMT"
        print("Using 'NETAMT' column for amount")
    else:
        amount_col = "AMOUNT"
        print("Note: 'NETAMT' column not found, using 'AMOUNT' instead")
    
    REP2_TRANSFORMED = REP2_COMBINED.with_columns([
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.lit('REV'))
          .otherwise(pl.col("UTSTY"))
          .alias("UTSTY"),
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.lit('REPO '))
          .otherwise(pl.col("UTREF"))
          .alias("UTREF"),
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.col(amount_col))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT"),
        pl.when(pl.col("BNMCODE") == '3752000000000Y')
          .then(pl.lit('3552000000000Y'))
          .otherwise(pl.col("BNMCODE"))
          .alias("BNMCODE"),
        (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
    ])

    # Sort data
    REP2_SORTED = REP2_TRANSFORMED.sort("BNMCODG")
    print(f"Transformed and sorted: {REP2_SORTED.height} records")

    # Create summary
    SUMMARY_DF = REP2_SORTED.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT_SUM"))
    print(f"Summary records: {SUMMARY_DF.height}")

    # Process WALW
    walw_file = f"{BNM_INPUT_PATH}/elw{REPTMON}{WK}{SAS_EXTENSION}"
    print(f"Reading: {walw_file}")
    WALW_DF = read_sas7bdat(walw_file)

    if WALW_DF.height > 0:
        print(f"WALW record count: {WALW_DF.height}")
        WALW_PROCESSED = WALW_DF.with_columns([
            pl.when(pl.col("BNMCODE") == '3250001000000Y')
              .then(pl.lit('3250000000000Y'))
              .otherwise(pl.col("BNMCODE"))
              .alias("BNMCODE")
        ])

        # Duplicate records
        WALW_DUPLICATED = WALW_PROCESSED.filter(pl.col("BNMCODE") == '3551000000000Y').with_columns(
            pl.lit('3552000000000Y').alias("BNMCODE")
        )
        WALW_FINAL = pl.concat([WALW_PROCESSED, WALW_DUPLICATED])
        print(f"WALW after processing: {WALW_FINAL.height} records")

        # Create WALW summary
        WALW_SUMMARY = WALW_FINAL.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))
        print(f"WALW summary records: {WALW_SUMMARY.height}")

        # Merge and calculate variance
        MERGED_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left")
        VARIANCE_DF = MERGED_DF.with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
        print(f"Variance records: {VARIANCE_DF.height}")
    else:
        VARIANCE_DF = pl.DataFrame()
        print(f"Warning: {walw_file} not found or empty")

    # Create REP0 data
    if REP2_DF.height > 0:
        REP0_DF = REP2_DF.filter(pl.col("BNMCODE") == '3250000000000Y').with_columns(
            (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
        )
        print(f"Reverse Repo records: {REP0_DF.height}")
    else:
        REP0_DF = pl.DataFrame()

    # ============================================
    # CREATE OUTPUT DIRECTORIES
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")

    # ============================================
    # WRITE OUTPUT FILES - FORMATTED TEXT
    # ============================================
    
    print("\nWriting formatted output files...")
    
    report_date_str = REPTDATE_LOAN.strftime("%d/%m/%y")
    
    # 1. Write Stock Report - Detailed format
    stock_report_file = f"{REPORTS_OUTPUT_PATH}/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}{TEXT_EXTENSION}"
    write_detailed_report(
        REP2_SORTED, 
        stock_report_file,
        "DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR :  DAYA",
        report_date_str
    )
    
    # 2. Write Variance Report
    variance_report_file = f"{REPORTS_OUTPUT_PATH}/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}{TEXT_EXTENSION}"
    write_variance_report(
        VARIANCE_DF,
        variance_report_file,
        report_date_str
    )
    
    # 3. Write Reverse Repo Report
    if REP0_DF.height > 0:
        reverse_repo_file = f"{REPORTS_OUTPUT_PATH}/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}{TEXT_EXTENSION}"
        write_detailed_report(
            REP0_DF,
            reverse_repo_file,
            "REVERSE REPO PURCHASE PROCEEDS",
            report_date_str
        )
    
    # Also save as Parquet for data processing
    if REP2_SORTED.height > 0:
        REP2_SORTED.write_parquet(f"{REPORTS_OUTPUT_PATH}/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}{PARQUET_EXTENSION}")
    
    if VARIANCE_DF.height > 0:
        VARIANCE_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}{PARQUET_EXTENSION}")
    
    if REP0_DF.height > 0:
        REP0_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}{PARQUET_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    if REP2_SORTED.height > 0:
        print(f"Stock Report: {REP2_SORTED.height} records")
    if VARIANCE_DF.height > 0:
        print(f"Variance Report: {VARIANCE_DF.height} records")
    if REP0_DF.height > 0:
        print(f"Reverse Repo: {REP0_DF.height} records")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")
