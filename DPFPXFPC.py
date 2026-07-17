import pyreadstat
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

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

def write_kapiti_report(df, filename, title, report_date):
    """
    Write KAPITI STOCK REPORT format (for securities data)
    """
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas
    pdf = df.to_pandas()
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'ELDAY':<10} {'BNMCODE':<20} {'UTSTY':<10} {'UTREF':<10} {'AMOUNT':>20} {'BNMCODG':<25}\n")
        f.write("-" * 100 + "\n")
        
        # Group by ELDAY
        if 'ELDAY' in pdf.columns:
            groups = pdf.groupby('ELDAY')
        else:
            pdf['ELDAY'] = 'TOTAL'
            groups = pdf.groupby('ELDAY')
        
        grand_total = 0
        
        for elday, group in groups:
            group_total = 0
            
            # Sort by BNMCODG within each group
            group = group.sort_values('BNMCODG')
            
            for _, row in group.iterrows():
                elday_val = str(row.get('ELDAY', ''))[:10]
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                utsty = str(row.get('UTSTY', ''))[:10]
                utref = str(row.get('UTREF', ''))[:10]
                amount = format_number(row.get('AMOUNT', 0))
                bnmcodg = str(row.get('BNMCODG', ''))[:25]
                
                f.write(f"{elday_val:<10} {bnmcode:<20} {utsty:<10} {utref:<10} {amount:>20} {bnmcodg:<25}\n")
                group_total += float(row.get('AMOUNT', 0) or 0)
            
            # Write group total
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL FOR':<10} {elday:<20} {'':<10} {'':<10} {format_number(group_total):>20} {'':<25}\n")
            f.write("\n")
            grand_total += group_total
        
        # Write grand total
        f.write("=" * 100 + "\n")
        f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<10} {'':<10} {format_number(grand_total):>20} {'':<25}\n")
        f.write("=" * 100 + "\n")

def write_eligible_liabilities_report(df, filename, title, report_date):
    """
    Write ELIGIBLE LIABILITIES report format (with FMTNAME, DESC, SIGN)
    """
    if df.empty:
        print(f"Warning: No data to write to {filename}")
        return
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'FMTNAME':<10} {'BNMCODE':<20} {'DESC':<50} {'SIGN':<6} {'AMOUNT':>20} {'TOTAL':>20}\n")
        f.write("-" * 128 + "\n")
        
        # Process each FMTNAME group
        total_grand = 0
        
        for fmtname in df['FMTNAME'].unique():
            group = df[df['FMTNAME'] == fmtname]
            group_total = 0
            
            for _, row in group.iterrows():
                bnmcode = str(row['BNMCODE'])[:20]
                desc = str(row['DESC'])[:50]
                sign = str(row['SIGN'])[:6]
                amount = format_number(row.get('AMOUNT', 0))
                total = format_number(row.get('TOTAL', row.get('AMOUNT', 0)))
                
                # Special handling for O/W RM IBB FROM CAGAMAS
                if 'O/W' in desc:
                    desc = f"{desc} {format_number(row.get('AMOUNT', 0))}"
                    amount = "0.00"
                    total = "0.00"
                
                f.write(f"{fmtname:<10} {bnmcode:<20} {desc:<50} {sign:<6} {amount:>20} {total:>20}\n")
                group_total += float(row.get('AMOUNT', 0) or 0)
            
            # Write group total
            f.write("-" * 128 + "\n")
            f.write(f"{'TOTAL FOR':<10} {fmtname:<20} {'':<50} {'':<6} {format_number(group_total):>20} {format_number(group_total):>20}\n")
            f.write(" " * 73 + "-" * 55 + "\n")
            f.write("\n")
            
            total_grand += group_total
        
        # Write grand total
        f.write("=" * 128 + "\n")
        f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<50} {'':<6} {format_number(total_grand):>20} {format_number(total_grand):>20}\n")
        f.write("=" * 128 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

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

REPTMON = f"{MM:02d}"
RYEAR = SXDATE.strftime("%Y")
RDATE = SXDATE.strftime("%d/%m/%y")
MTHNAM = SXDATE.strftime("%B")

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
print(f"Combined REP2+REP4: {REP2_COMBINED.height} records")

# ============================================
# TRANSFORM DATA (SAS logic)
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

    # Convert to pandas for easier handling
    df_pd = REP2_TRANSFORMED.to_pandas()
    
    # ============================================
    # CREATE OUTPUT DIRECTORY
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print("\nWriting output files...")

    # ============================================
    # WRITE KAPITI STOCK REPORT (since we have securities data)
    # ============================================
    
    report_date_str = SXDATE.strftime("%d/%m/%y")
    filename_base = f"DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}"
    
    # Write the Kapiti Stock Report (this is what your data contains)
    write_kapiti_report(
        REP2_TRANSFORMED,
        f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}",
        "SPECIFIED & NON-SPECIFIED RENTAS SECURITIES FROM TRADING BOOK",
        report_date_str
    )
    
    # Also save as Parquet
    REP2_TRANSFORMED.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
    
    print(f"✓ Stock Report written: {filename_base}{TEXT_EXTENSION}")

    # ============================================
    # WRITE VARIANCE REPORT (if WALW data exists)
    # ============================================
    
    # Create summary by BNMCODE and ELDAY
    SUMMARY_DF = REP2_TRANSFORMED.group_by(["BNMCODE", "ELDAY"]).agg(
        pl.col("AMOUNT").sum().alias("AMOUNT_SUM")
    )
    print(f"Summary records: {SUMMARY_DF.height}")
    
    # Process WALW data
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
        
        WALW_DUPLICATED = WALW_PROCESSED.filter(
            pl.col("BNMCODE") == '3551000000000Y'
        ).with_columns(pl.lit('3552000000000Y').alias("BNMCODE"))
        
        WALW_FINAL = pl.concat([WALW_PROCESSED, WALW_DUPLICATED])
        print(f"WALW after processing: {WALW_FINAL.height} records")
        
        WALW_SUMMARY = WALW_FINAL.group_by(["BNMCODE", "ELDAY"]).agg(
            pl.col("AMOUNT").sum().alias("WALWAMT")
        )
        
        MERGED_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left")
        VARIANCE_DF = MERGED_DF.with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
        
        # Write variance report
        variance_filename = f"KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}"
        
        # Convert to pandas and write formatted variance report
        var_pd = VARIANCE_DF.to_pandas()
        with open(f"{REPORTS_OUTPUT_PATH}/{variance_filename}{TEXT_EXTENSION}", 'w') as f:
            f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
            f.write(f"VARIANCE BETWEEN KAPITI AND WALKER\n")
            f.write(f"REPORT DATE :  {report_date_str}\n")
            f.write("\n")
            f.write(f"{'BNMCODE':<20} {'ELDAY':<10} {'KAPITI':>20} {'WALKER':>20} {'VARIANCE':>20}\n")
            f.write("-" * 100 + "\n")
            
            total_kapiti = 0
            total_walker = 0
            total_variance = 0
            
            for _, row in var_pd.iterrows():
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                elday = str(row.get('ELDAY', ''))[:10]
                kapiti = row.get('AMOUNT_SUM', 0)
                walker = row.get('WALWAMT', 0)
                variance = row.get('VARIANC', 0)
                
                f.write(f"{bnmcode:<20} {elday:<10} {format_number(kapiti):>20} {format_number(walker):>20} {format_number(variance):>20}\n")
                total_kapiti += kapiti if not pd.isna(kapiti) else 0
                total_walker += walker if not pd.isna(walker) else 0
                total_variance += variance if not pd.isna(variance) else 0
            
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL':<20} {'':<10} {format_number(total_kapiti):>20} {format_number(total_walker):>20} {format_number(total_variance):>20}\n")
            f.write("=" * 100 + "\n")
        
        VARIANCE_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{variance_filename}{PARQUET_EXTENSION}")
        print(f"✓ Variance Report written: {variance_filename}{TEXT_EXTENSION}")

    # ============================================
    # WRITE REVERSE REPO REPORT
    # ============================================
    
    REP2_REFILTERED = apply_utsty_filter(REP2_RAW)
    if REP2_REFILTERED.height > 0:
        REP0_DF = REP2_REFILTERED.filter(
            pl.col("BNMCODE") == '3250000000000Y'
        ).with_columns(
            (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
        )
        
        if REP0_DF.height > 0:
            repo_filename = f"REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}"
            
            # Write reverse repo report
            with open(f"{REPORTS_OUTPUT_PATH}/{repo_filename}{TEXT_EXTENSION}", 'w') as f:
                f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
                f.write(f"REV REPO AT PURCHASE PROCEEDS\n")
                f.write(f"REPORT DATE :  {report_date_str}\n")
                f.write("\n")
                f.write(f"{'BNMCODG':<40} {'ELDAY':<10} {'AMOUNT':>20}\n")
                f.write("-" * 70 + "\n")
                
                # Group by BNMCODG and ELDAY
                repo_pd = REP0_DF.to_pandas()
                repo_grouped = repo_pd.groupby(['BNMCODG', 'ELDAY'])['AMOUNT'].sum().reset_index()
                repo_grouped = repo_grouped.sort_values(['BNMCODG', 'ELDAY'])
                
                total_repo = 0
                for _, row in repo_grouped.iterrows():
                    bnmcodg = str(row['BNMCODG'])[:40]
                    elday = str(row['ELDAY'])[:10]
                    amount = row['AMOUNT']
                    f.write(f"{bnmcodg:<40} {elday:<10} {format_number(amount):>20}\n")
                    total_repo += amount if not pd.isna(amount) else 0
                
                f.write("-" * 70 + "\n")
                f.write(f"{'TOTAL':<40} {'':<10} {format_number(total_repo):>20}\n")
                f.write("=" * 70 + "\n")
            
            REP0_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{repo_filename}{PARQUET_EXTENSION}")
            print(f"✓ Reverse Repo Report written: {repo_filename}{TEXT_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {SXDATE.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, {MTHNAM} {RYEAR}")
    print(f"Records processed: {REP2_TRANSFORMED.height}")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")
