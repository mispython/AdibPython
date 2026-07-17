"""
EIBWKAPE - DAILY KAPITI STOCK / VARIANCE / REV REPO REPORTS
Produces ONE formatted text file output
"""

import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"

OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"

SAS_EXTENSION = ".sas7bdat"
TEXT_EXTENSION = ".TXT"  # Single output file

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

def apply_utsty_filter(df):
    """
    Mirrors SAS:
      IF UTSTY IN ('CB1','CF1','CNT','SAC','SMC','ISB') THEN DO;
          IF UTREF NOT IN ('DLG','IDLG') THEN DELETE;
      END;
    """
    if df.height == 0:
        return df
    return df.filter(
        ~(
            (pl.col("UTSTY").is_in(['CB1', 'CF1', 'CNT', 'SAC', 'SMC', 'ISB'])) &
            (~pl.col("UTREF").is_in(['DLG', 'IDLG']))
        )
    )

def write_formatted_report(df_stock, df_variance, df_repo, output_file, title, report_date, period):
    """
    Write a single formatted text file with all report sections
    Similar to SAS PROC REPORT output
    """
    
    with open(output_file, 'w') as f:
        # ========================================
        # HEADER
        # ========================================
        f.write("=" * 120 + "\n")
        f.write(f"{title}\n")
        f.write(f"Report Date: {report_date}\n")
        f.write(f"Report Period: {period}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 120 + "\n\n")
        
        # ========================================
        # SECTION 1: STOCK REPORT (REP2/REP4)
        # ========================================
        if df_stock.height > 0:
            f.write("=" * 120 + "\n")
            f.write("SECTION 1: DAILY KAPITI STOCK REPORT\n")
            f.write("=" * 120 + "\n\n")
            
            # Get display columns
            display_cols = ['BNMCODG', 'BNMCODE', 'AMOUNT', 'UTSTY', 'UTREF', 'ELDAY']
            available_cols = [col for col in display_cols if col in df_stock.columns]
            
            # Calculate column widths
            col_widths = {}
            for col in available_cols:
                max_width = max(len(col), 12)
                if col in df_stock.columns:
                    pdf_temp = df_stock[col].to_pandas()
                    max_data = pdf_temp.astype(str).str.len().max()
                    if pd.notna(max_data):
                        max_width = max(max_width, min(max_data + 2, 25))
                col_widths[col] = max_width
            
            # Header
            header_line = ""
            for col in available_cols:
                header_line += f"{col:<{col_widths[col]}}"
            f.write(header_line + "\n")
            f.write("-" * len(header_line) + "\n")
            
            # Data rows
            pdf_stock = df_stock[available_cols].to_pandas()
            for _, row in pdf_stock.iterrows():
                line = ""
                for col in available_cols:
                    val = row[col]
                    if col == 'AMOUNT':
                        val_str = f"{val:,.2f}" if pd.notna(val) else ""
                    else:
                        val_str = str(val) if pd.notna(val) else ""
                    line += f"{val_str:<{col_widths[col]}}"
                f.write(line + "\n")
            
            # Summary
            f.write("-" * len(header_line) + "\n")
            total_amount = df_stock['AMOUNT'].sum() if 'AMOUNT' in df_stock.columns else 0
            f.write(f"TOTAL RECORDS: {df_stock.height:>10}\n")
            f.write(f"TOTAL AMOUNT: {total_amount:>30,.2f}\n")
            f.write("-" * len(header_line) + "\n\n")
        
        # ========================================
        # SECTION 2: VARIANCE REPORT
        # ========================================
        if df_variance.height > 0:
            f.write("=" * 120 + "\n")
            f.write("SECTION 2: KAPITI WALKER VARIANCE REPORT\n")
            f.write("=" * 120 + "\n\n")
            
            # Get display columns
            display_cols = ['BNMCODE', 'ELDAY', 'AMOUNT_SUM', 'WALWAMT', 'VARIANC']
            available_cols = [col for col in display_cols if col in df_variance.columns]
            
            # Calculate column widths
            col_widths = {}
            for col in available_cols:
                max_width = max(len(col), 12)
                if col in df_variance.columns:
                    pdf_temp = df_variance[col].to_pandas()
                    max_data = pdf_temp.astype(str).str.len().max()
                    if pd.notna(max_data):
                        max_width = max(max_width, min(max_data + 2, 25))
                col_widths[col] = max_width
            
            # Header
            header_line = ""
            for col in available_cols:
                header_line += f"{col:<{col_widths[col]}}"
            f.write(header_line + "\n")
            f.write("-" * len(header_line) + "\n")
            
            # Data rows
            pdf_var = df_variance[available_cols].to_pandas()
            for _, row in pdf_var.iterrows():
                line = ""
                for col in available_cols:
                    val = row[col]
                    if col in ['AMOUNT_SUM', 'WALWAMT', 'VARIANC']:
                        val_str = f"{val:,.2f}" if pd.notna(val) else ""
                    else:
                        val_str = str(val) if pd.notna(val) else ""
                    line += f"{val_str:<{col_widths[col]}}"
                f.write(line + "\n")
            
            # Summary
            f.write("-" * len(header_line) + "\n")
            if 'AMOUNT_SUM' in df_variance.columns:
                total_sum = df_variance['AMOUNT_SUM'].sum()
                f.write(f"TOTAL AMOUNT_SUM: {total_sum:>27,.2f}\n")
            if 'WALWAMT' in df_variance.columns:
                total_walw = df_variance['WALWAMT'].sum()
                f.write(f"TOTAL WALWAMT:    {total_walw:>27,.2f}\n")
            if 'VARIANC' in df_variance.columns:
                total_var = df_variance['VARIANC'].sum()
                f.write(f"TOTAL VARIANCE:   {total_var:>27,.2f}\n")
            f.write(f"TOTAL RECORDS: {df_variance.height:>30}\n")
            f.write("-" * len(header_line) + "\n\n")
        
        # ========================================
        # SECTION 3: REVERSE REPO REPORT
        # ========================================
        if df_repo.height > 0:
            f.write("=" * 120 + "\n")
            f.write("SECTION 3: REVERSE REPO PURCHASE PROCEEDS\n")
            f.write("=" * 120 + "\n\n")
            
            # Get display columns
            display_cols = ['BNMCODG', 'AMOUNT']
            if 'NETAMT' in df_repo.columns:
                display_cols.append('NETAMT')
            if 'COSTDED' in df_repo.columns:
                display_cols.append('COSTDED')
            
            available_cols = [col for col in display_cols if col in df_repo.columns]
            
            # Calculate column widths
            col_widths = {}
            for col in available_cols:
                max_width = max(len(col), 12)
                if col in df_repo.columns:
                    pdf_temp = df_repo[col].to_pandas()
                    max_data = pdf_temp.astype(str).str.len().max()
                    if pd.notna(max_data):
                        max_width = max(max_width, min(max_data + 2, 25))
                col_widths[col] = max_width
            
            # Header
            header_line = ""
            for col in available_cols:
                header_line += f"{col:<{col_widths[col]}}"
            f.write(header_line + "\n")
            f.write("-" * len(header_line) + "\n")
            
            # Data rows
            pdf_repo = df_repo[available_cols].to_pandas()
            for _, row in pdf_repo.iterrows():
                line = ""
                for col in available_cols:
                    val = row[col]
                    if col in ['AMOUNT', 'NETAMT', 'COSTDED']:
                        val_str = f"{val:,.2f}" if pd.notna(val) else ""
                    else:
                        val_str = str(val) if pd.notna(val) else ""
                    line += f"{val_str:<{col_widths[col]}}"
                f.write(line + "\n")
            
            # Summary
            f.write("-" * len(header_line) + "\n")
            if 'AMOUNT' in df_repo.columns:
                total_amount = df_repo['AMOUNT'].sum()
                f.write(f"TOTAL AMOUNT: {total_amount:>30,.2f}\n")
            if 'NETAMT' in df_repo.columns:
                total_netamt = df_repo['NETAMT'].sum()
                f.write(f"TOTAL NETAMT: {total_netamt:>30,.2f}\n")
            if 'COSTDED' in df_repo.columns:
                total_costded = df_repo['COSTDED'].sum()
                f.write(f"TOTAL COSTDED: {total_costded:>30,.2f}\n")
            f.write(f"TOTAL RECORDS: {df_repo.height:>30}\n")
            f.write("-" * len(header_line) + "\n\n")
        
        # ========================================
        # FOOTER
        # ========================================
        f.write("=" * 120 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 120 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

SDESC = "PUBLIC BANK BERHAD"
RDATE = REPTDATE_LOAN.strftime("%d/%m/%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")

# Calculate week and month
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

print(f"Report Period: Week {WK}, Month {REPTMON}")

# ============================================
# LOAD AND PROCESS DATA
# ============================================

# Load REP2 and REP4
rep2_file = f"{BNMK_INPUT_PATH}/rep2{REPTMON}{WK}{SAS_EXTENSION}"
rep4_file = f"{BNMK_INPUT_PATH}/rep4{REPTMON}{WK}{SAS_EXTENSION}"

print(f"Reading: {rep2_file}")
REP2_RAW = read_sas7bdat(rep2_file)
print(f"Reading: {rep4_file}")
REP4_RAW = read_sas7bdat(rep4_file)

# Apply filters
REP2_FILTERED = apply_utsty_filter(REP2_RAW)
REP4_FILTERED = apply_utsty_filter(REP4_RAW)

print(f"REP2: {REP2_RAW.height} raw → {REP2_FILTERED.height} filtered")
print(f"REP4: {REP4_RAW.height} raw → {REP4_FILTERED.height} filtered")

# Combine
frames = [d for d in (REP2_FILTERED, REP4_FILTERED) if d.height > 0]
REP2_COMBINED = pl.concat(frames) if frames else pl.DataFrame()
print(f"Combined: {REP2_COMBINED.height} records")

if REP2_COMBINED.height > 0:
    # Transform data
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
    
    # Summary for variance
    SUMMARY_DF = REP2_SORTED.group_by(["BNMCODE", "ELDAY"]).agg(
        pl.col("AMOUNT").sum().alias("AMOUNT_SUM")
    )
    
    # Load Walker (ELW)
    walw_file = f"{BNM_INPUT_PATH}/elw{REPTMON}{WK}{SAS_EXTENSION}"
    print(f"Reading: {walw_file}")
    WALW_DF = read_sas7bdat(walw_file)
    
    if WALW_DF.height > 0:
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
        
        WALW_SUMMARY = WALW_FINAL.group_by(["BNMCODE", "ELDAY"]).agg(
            pl.col("AMOUNT").sum().alias("WALWAMT")
        )
        
        VARIANCE_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left").with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
    else:
        VARIANCE_DF = pl.DataFrame()
    
    # Reverse Repo (REP0)
    REP2_REFILTERED = apply_utsty_filter(REP2_RAW)
    if REP2_REFILTERED.height > 0:
        REP0_DF = REP2_REFILTERED.filter(
            pl.col("BNMCODE") == '3250000000000Y'
        ).with_columns(
            (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
        )
    else:
        REP0_DF = pl.DataFrame()
    
    # ============================================
    # CREATE OUTPUT DIRECTORY
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    
    # ============================================
    # GENERATE SINGLE FORMATTED TEXT FILE
    # ============================================
    
    output_filename = f"DAILY_KAPITI_REPORT_{REPTMON}{WK}_{RYEAR}{TEXT_EXTENSION}"
    output_file = f"{REPORTS_OUTPUT_PATH}/{output_filename}"
    
    write_formatted_report(
        df_stock=REP2_SORTED,
        df_variance=VARIANCE_DF,
        df_repo=REP0_DF,
        output_file=output_file,
        title=f"{SDESC} - DAILY KAPITI STOCK / VARIANCE / REV REPO REPORT",
        report_date=RDATE,
        period=f"Week {WK}, Month {REPTMON}"
    )
    
    # ============================================
    # COMPLETION MESSAGE
    # ============================================
    
    print("\n" + "=" * 50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    print(f"Output file: {output_file}")
    print(f"File size: {Path(output_file).stat().st_size:,} bytes")
    if REP2_SORTED.height > 0:
        print(f"Stock Records: {REP2_SORTED.height}")
    if VARIANCE_DF.height > 0:
        print(f"Variance Records: {VARIANCE_DF.height}")
    if REP0_DF.height > 0:
        print(f"Reverse Repo Records: {REP0_DF.height}")
    print("=" * 50)

else:
    print("Error: No data available to process")
