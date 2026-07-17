import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

def read_sas7bdat(file_path):
    """Read SAS7BDAT file and return as Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

# PROCESS REPTDAT1 FROM LOAN.REPTDATE (using timedelta)
# Adjust the timedelta based on your requirements
REPTDATE_LOAN = datetime.now() - timedelta(days=1)  # Example: yesterday
SDESC = "PUBLIC BANK BERHAD"
RPDATE = REPTDATE_LOAN.strftime("%d%m%y")
RDATE = REPTDATE_LOAN.strftime("%d%m%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]
DATE_VAR = REPTDATE_LOAN
MTHEND = f"{REPTDATE_LOAN.day:02d}"

# PROCESS REPTDATE FROM BNMK.REPTDATE (using timedelta)
REPTDATE_BNMK = datetime.now() - timedelta(days=1)  # Example: yesterday
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

# PROCESS REP2 DATA from SAS7BDAT
rep2_file = f'BNMK/REP2{REPTMON}{WK}.sas7bdat'
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
rep4_file = f'BNMK/REP4{REPTMON}{WK}.sas7bdat'
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

# TRANSFORM DATA
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
    walw_file = f'BNM/ELW{REPTMON}{WK}.sas7bdat'
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

    # CREATE OUTPUT DIRECTORIES
    Path("OUTPUT/REPORTS").mkdir(parents=True, exist_ok=True)
    Path("SFTP_UPLOAD").mkdir(exist_ok=True)

    # WRITE OUTPUT FUNCTIONS
    def write_to_text(df, filename, separator='\t'):
        """Write DataFrame to text file with specified separator"""
        if df.height > 0:
            # Convert to pandas for easier text writing
            pdf = df.to_pandas()
            # Write header
            pdf.to_csv(filename, sep=separator, index=False)
        else:
            print(f"Warning: No data to write to {filename}")

    def write_to_fwf(df, filename, column_widths=None):
        """Write DataFrame to fixed-width text file"""
        if df.height > 0:
            pdf = df.to_pandas()
            if column_widths is None:
                # Auto-calculate column widths
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
        else:
            print(f"Warning: No data to write to {filename}")

    # WRITE OUTPUT FILES - Parquet and Text files
    if REP2_SORTED.height > 0:
        REP2_SORTED.write_parquet(f"OUTPUT/REPORTS/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}.PARQUET")
        # Write as tab-delimited text
        write_to_text(REP2_SORTED, f"OUTPUT/REPORTS/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}.TXT")
        # Optionally write as fixed-width
        # write_to_fwf(REP2_SORTED, f"OUTPUT/REPORTS/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}.TXT")

    if VARIANCE_DF.height > 0:
        VARIANCE_DF.write_parquet(f"OUTPUT/REPORTS/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}.PARQUET")
        write_to_text(VARIANCE_DF, f"OUTPUT/REPORTS/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}.TXT")

    if REP0_DF.height > 0:
        REP0_DF.write_parquet(f"OUTPUT/REPORTS/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}.PARQUET")
        write_to_text(REP0_DF, f"OUTPUT/REPORTS/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}.TXT")

    print("Processing completed successfully!")
    print(f"Stock Report: {REP2_SORTED.height} records")
    print(f"Variance Report: {VARIANCE_DF.height} records")
    print(f"Reverse Repo: {REP0_DF.height} records")

else:
    print("Error: No data available to process")
