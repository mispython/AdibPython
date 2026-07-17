import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

# Input paths
BNMK_INPUT_PATH = "BNMK"  # Path for BNMK files (REP2, REP4)
BNM_INPUT_PATH = "BNM"    # Path for BNM files (ELW)

# Output paths
OUTPUT_BASE_PATH = "OUTPUT"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

# File extensions
SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

# Date configuration (for REPTDATE)
# Use current date or specify a specific date
USE_CURRENT_DATE = True  # Set to False to use custom date
CUSTOM_DATE = datetime(2026, 7, 17)  # Only used if USE_CURRENT_DATE is False
DAYS_OFFSET = 1  # Days to subtract from current date (1 = yesterday)

# Text file separator (tab or comma or other)
TEXT_SEPARATOR = '\t'  # '\t' for tab, ',' for comma, '|' for pipe

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

def write_to_text(df, filename, separator=TEXT_SEPARATOR):
    """Write DataFrame to text file with specified separator"""
    if df.height > 0:
        pdf = df.to_pandas()
        pdf.to_csv(filename, sep=separator, index=False)
        print(f"Written: {filename} ({df.height} records)")
    else:
        print(f"Warning: No data to write to {filename}")

def write_to_fwf(df, filename, column_widths=None):
    """Write DataFrame to fixed-width text file"""
    if df.height > 0:
        pdf = df.to_pandas()
        if column_widths is None:
            column_widths = {}
            for col in pdf.columns:
                max_val_len = pdf[col].astype(str).str.len().max()
                max_header_len = len(str(col))
                column_widths[col] = max(max_val_len, max_header_len) + 2
        
        with open(filename, 'w') as f:
            # Write header
            header_line = ''
            for col in pdf.columns:
                header_line += str(col).ljust(column_widths[col])
            f.write(header_line + '\n')
            
            # Write data
            for _, row in pdf.iterrows():
                line = ''
                for col in pdf.columns:
                    val = str(row[col]) if pd.notna(row[col]) else ''
                    line += val.ljust(column_widths[col])
                f.write(line + '\n')
        print(f"Written (fixed-width): {filename} ({df.height} records)")
    else:
        print(f"Warning: No data to write to {filename}")

# ============================================
# DATE PROCESSING
# ============================================

# Determine the reporting date
if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

# PROCESS REPTDAT1 FROM LOAN.REPTDATE
SDESC = "PUBLIC BANK BERHAD"
RPDATE = REPTDATE_LOAN.strftime("%d%m%y")
RDATE = REPTDATE_LOAN.strftime("%d%m%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]
DATE_VAR = REPTDATE_LOAN
MTHEND = f"{REPTDATE_LOAN.day:02d}"

# PROCESS REPTDATE FROM BNMK.REPTDATE
REPTDATE_BNMK = REPTDATE_LOAN  # Using same date as loan
MM = REPTDATE_BNMK.month
DAY = REPTDATE_BNMK.day

# Determine week number based on day
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
WK_VAR = WK
REPTMON = f"{MM:02d}"
RPDATE_BNMK = SXDATE.strftime("%d%m%y")
RPYEAR = SXDATE.strftime("%Y")
REPTYEAR = SXDATE.strftime("%Y")
YEAR_SHORT = SXDATE.strftime("%y")

print(f"Report Period: Week {WK}, Month {REPTMON}")
print(f"Reading files: REP2{REPTMON}{WK}, REP4{REPTMON}{WK}, ELW{REPTMON}{WK}")

# ============================================
# PROCESS DATA FILES
# ============================================

# PROCESS REP2 DATA from SAS7BDAT
rep2_file = f"{BNMK_INPUT_PATH}/REP2{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep2_file}")
REP2_DF = read_sas7bdat(rep2_file)

if REP2_DF.height > 0:
    REP2_FILTERED = REP2_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
else:
    REP2_FILTERED = pl.DataFrame()
    print(f"Warning: {rep2_file} not found or empty")

# PROCESS REP4 DATA from SAS7BDAT
rep4_file = f"{BNMK_INPUT_PATH}/REP4{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep4_file}")
REP4_DF = read_sas7bdat(rep4_file)

if REP4_DF.height > 0:
    REP4_FILTERED = REP4_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
else:
    REP4_FILTERED = pl.DataFrame()
    print(f"Warning: {rep4_file} not found or empty")

# COMBINE REP2 AND REP4
if REP2_FILTERED.height > 0 and REP4_FILTERED.height > 0:
    REP2_COMBINED = pl.concat([REP2_FILTERED, REP4_FILTERED])
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
          .then(pl.col("NETAMT"))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT"),
        pl.when(pl.col("BNMCODE") == '3752000000000Y')
          .then(pl.lit('3552000000000Y'))
          .otherwise(pl.col("BNMCODE"))
          .alias("BNMCODE"),
        (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
    ])

    # SORT DATA
    REP2_SORTED = REP2_TRANSFORMED.sort("BNMCODG")

    # CREATE SUMMARY
    SUMMARY_DF = REP2_SORTED.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT_SUM"))

    # PROCESS WALW DATA from SAS7BDAT
    walw_file = f"{BNM_INPUT_PATH}/ELW{REPTMON}{WK}{SAS_EXTENSION}"
    print(f"Reading: {walw_file}")
    WALW_DF = read_sas7bdat(walw_file)

    if WALW_DF.height > 0:
        WALW_PROCESSED = WALW_DF.with_columns([
            pl.when(pl.col("BNMCODE") == '3250001000000Y')
              .then(pl.lit('3250000000000Y'))
              .otherwise(pl.col("BNMCODE"))
              .alias("BNMCODE")
        ])

        # DUPLICATE RECORDS FOR SPECIFIC CONDITION
        WALW_DUPLICATED = WALW_PROCESSED.filter(pl.col("BNMCODE") == '3551000000000Y').with_columns(
            pl.lit('3552000000000Y').alias("BNMCODE")
        )
        WALW_FINAL = pl.concat([WALW_PROCESSED, WALW_DUPLICATED])

        # CREATE WALW SUMMARY
        WALW_SUMMARY = WALW_FINAL.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))

        # MERGE AND CALCULATE VARIANCE
        MERGED_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left")
        VARIANCE_DF = MERGED_DF.with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
    else:
        VARIANCE_DF = pl.DataFrame()
        print(f"Warning: {walw_file} not found or empty")

    # CREATE REP0 DATA FOR REVERSE REPO
    if REP2_DF.height > 0:
        REP0_DF = REP2_DF.filter(pl.col("BNMCODE") == '3250000000000Y').with_columns(
            (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
        )
    else:
        REP0_DF = pl.DataFrame()

    # ============================================
    # CREATE OUTPUT DIRECTORIES
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print(f"SFTP upload directory: {SFTP_UPLOAD_PATH}")

    # ============================================
    # WRITE OUTPUT FILES
    # ============================================
    
    print("\nWriting output files...")

    # Stock Report
    if REP2_SORTED.height > 0:
        filename_base = f"DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}"
        REP2_SORTED.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(REP2_SORTED, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")
        # Optionally write fixed-width
        # write_to_fwf(REP2_SORTED, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    # Variance Report
    if VARIANCE_DF.height > 0:
        filename_base = f"KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}"
        VARIANCE_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(VARIANCE_DF, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    # Reverse Repo Report
    if REP0_DF.height > 0:
        filename_base = f"REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}"
        REP0_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(REP0_DF, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    print(f"Stock Report: {REP2_SORTED.height} records")
    print(f"Variance Report: {VARIANCE_DF.height} records")
    print(f"Reverse Repo: {REP0_DF.height} records")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")
