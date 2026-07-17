import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"

OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

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

def apply_utsty_filter(df):
    """Mirrors SAS filter logic"""
    if df.height == 0:
        return df
    return df.filter(
        ~(
            (pl.col("UTSTY").is_in(['CB1', 'CF1', 'CNT', 'SAC', 'SMC', 'ISB'])) &
            (~pl.col("UTREF").is_in(['DLG', 'IDLG']))
        )
    )

def write_formatted_report(df, filename, title, report_date):
    """
    Write a single formatted text file matching SAS output format
    """
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas
    pdf = df.to_pandas()
    
    # Group by ELDAY (which appears to be the primary grouping in your data)
    if 'ELDAY' in pdf.columns:
        groups = pdf.groupby('ELDAY')
    else:
        pdf['ELDAY'] = 'TOTAL'
        groups = pdf.groupby('ELDAY')
    
    with open(filename, 'w') as f:
        # Write main header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'ELDAY':<10} {'BNMCODE':<20} {'UTSTY':<10} {'UTREF':<10} {'AMOUNT':>20} {'BNMCODG':<20}\n")
        f.write("-" * 100 + "\n")
        
        # Process each group
        grand_total = 0
        
        for elday, group in groups:
            group_total = 0
            
            # Write each row in the group
            for _, row in group.iterrows():
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                utsty = str(row.get('UTSTY', ''))[:10]
                utref = str(row.get('UTREF', ''))[:10]
                amount = format_number(row.get('AMOUNT', 0))
                bnmcodg = str(row.get('BNMCODG', ''))[:20]
                
                f.write(f"{elday:<10} {bnmcode:<20} {utsty:<10} {utref:<10} {amount:>20} {bnmcodg:<20}\n")
                group_total += float(row.get('AMOUNT', 0) or 0)
            
            # Write group separator and total
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL FOR':<10} {elday:<20} {'':<10} {'':<10} {format_number(group_total):>20} {'':<20}\n")
            f.write("\n")
            
            grand_total += group_total
        
        # Write grand total
        f.write("=" * 100 + "\n")
        f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<10} {'':<10} {format_number(grand_total):>20} {'':<20}\n")
        f.write("=" * 100 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

# Process dates
SDESC = "PUBLIC BANK BERHAD"
RDATE = REPTDATE_LOAN.strftime("%d/%m/%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]

REPTDATE_BNMK = REPTDATE_LOAN
MM = REPTDATE_BNMK.month
DAY = REPTDATE_BNMK.day

if 1 <= DAY <= 8:
    WK = '4'
elif 9 <= DAY <= 15:
    WK = '1'
elif 16 <= DAY <= 22:
    WK = '2'
else:
    WK = '3'

if WK == '4':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
    MM = MM1
    if MM == 12:
        SXDATE = REPTDATE_BNMK.replace(month=1, day=1) - timedelta(days=1)
    else:
        SXDATE = REPTDATE_BNMK.replace(day=1) - timedelta(days=1)
        SXDATE = SXDATE.replace(month=MM) if SXDATE.month != MM else SXDATE
else:
    SXDATE = REPTDATE_BNMK

NOWK = WK
REPTMON = f"{MM:02d}"
RPYEAR = SXDATE.strftime("%Y")
REPTYEAR = SXDATE.strftime("%Y")

print(f"Report Period: Week {WK}, Month {REPTMON}")
print(f"Reading files: rep2{REPTMON}{WK}, rep4{REPTMON}{WK}, elw{REPTMON}{WK}")

# ============================================
# LOAD AND PROCESS DATA
# ============================================

rep2_file = f"{BNMK_INPUT_PATH}/rep2{REPTMON}{WK}{SAS_EXTENSION}"
rep4_file = f"{BNMK_INPUT_PATH}/rep4{REPTMON}{WK}{SAS_EXTENSION}"

print(f"Reading: {rep2_file}")
REP2_RAW = read_sas7bdat(rep2_file)
print(f"Reading: {rep4_file}")
REP4_RAW = read_sas7bdat(rep4_file)

REP2_FILTERED = apply_utsty_filter(REP2_RAW)
REP4_FILTERED = apply_utsty_filter(REP4_RAW)

print(f"REP2 raw: {REP2_RAW.height} | filtered: {REP2_FILTERED.height}")
print(f"REP4 raw: {REP4_RAW.height} | filtered: {REP4_FILTERED.height}")

# Combine REP2 + REP4
frames = [d for d in (REP2_FILTERED, REP4_FILTERED) if d.height > 0]
REP2_COMBINED = pl.concat(frames) if frames else pl.DataFrame()
print(f"Combined REP2+REP4 (filtered): {REP2_COMBINED.height} records")

# ============================================
# TRANSFORM DATA
# ============================================

if REP2_COMBINED.height > 0:
    amount_col = "NETAMT" if "NETAMT" in REP2_COMBINED.columns else "AMOUNT"

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
    ]).with_columns(
        (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
    )

    REP2_SORTED = REP2_TRANSFORMED.sort("BNMCODG")
    print(f"Transformed and sorted: {REP2_SORTED.height} records")

    # ============================================
    # CREATE OUTPUT DIRECTORY
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print("\nWriting output files...")

    # ============================================
    # WRITE FORMATTED TEXT FILE (SINGLE OUTPUT)
    # ============================================
    
    report_date_str = REPTDATE_LOAN.strftime("%d/%m/%y")
    filename_base = f"DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}"
    
    # Write the main formatted report
    write_formatted_report(
        REP2_SORTED,
        f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}",
        "DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS",
        report_date_str
    )
    
    # Also save as Parquet for data processing
    REP2_SORTED.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    print(f"Records processed: {REP2_SORTED.height}")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")
