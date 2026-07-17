program:

import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

# Input paths
BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"  # Path for BNMK files (REP2, REP4)
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"   # Path for BNM files (ELW)

# Output paths
OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

# File extensions
SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

# Date configuration (for REPTDATE)
# Use current date or specify a specific date
USE_CURRENT_DATE = False  # Set to False to use custom date
CUSTOM_DATE = datetime(2026, 7, 16)  # Using July 16 as in your error
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
print(f"Reading files: rep2{REPTMON}{WK}, rep4{REPTMON}{WK}, elw071")

# ============================================
# PROCESS DATA FILES
# ============================================

# PROCESS REP2 DATA from SAS7BDAT (lowercase, format: rep2{REPTMON}{WK})
rep2_file = f"{BNMK_INPUT_PATH}/rep2{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep2_file}")
REP2_DF = read_sas7bdat(rep2_file)

if REP2_DF.height > 0:
    print(f"REP2 columns: {REP2_DF.columns}")
    print(f"REP2 record count: {REP2_DF.height}")
    REP2_FILTERED = REP2_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
    print(f"REP2 after filter: {REP2_FILTERED.height} records")
else:
    REP2_FILTERED = pl.DataFrame()
    print(f"Warning: {rep2_file} not found or empty")

# PROCESS REP4 DATA from SAS7BDAT (lowercase, format: rep4{REPTMON}{WK})
rep4_file = f"{BNMK_INPUT_PATH}/rep4{REPTMON}{WK}{SAS_EXTENSION}"
print(f"Reading: {rep4_file}")
REP4_DF = read_sas7bdat(rep4_file)

if REP4_DF.height > 0:
    print(f"REP4 columns: {REP4_DF.columns}")
    print(f"REP4 record count: {REP4_DF.height}")
    REP4_FILTERED = REP4_DF.filter(
        ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
          (~pl.col("UTREF").is_in(['DLG','IDLG'])))
    )
    print(f"REP4 after filter: {REP4_FILTERED.height} records")
else:
    REP4_FILTERED = pl.DataFrame()
    print(f"Warning: {rep4_file} not found or empty")

# COMBINE REP2 AND REP4
if REP2_FILTERED.height > 0 and REP4_FILTERED.height > 0:
    REP2_COMBINED = pl.concat([REP2_FILTERED, REP4_FILTERED])
    print(f"Combined REP2+REP4: {REP2_COMBINED.height} records")
elif REP2_FILTERED.height > 0:
    REP2_COMBINED = REP2_FILTERED
    print(f"Using only REP2: {REP2_COMBINED.height} records")
elif REP4_FILTERED.height > 0:
    REP2_COMBINED = REP4_FILTERED
    print(f"Using only REP4: {REP2_COMBINED.height} records")
else:
    REP2_COMBINED = pl.DataFrame()
    print("Error: No data available from REP2 or REP4")

# ============================================
# TRANSFORM DATA
# ============================================

if REP2_COMBINED.height > 0:
    print(f"Combined columns: {REP2_COMBINED.columns}")
    
    # Check if NETAMT exists, if not use AMOUNT
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
          .then(pl.col(amount_col))  # Use the appropriate column
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
    print(f"Transformed and sorted: {REP2_SORTED.height} records")

    # CREATE SUMMARY
    SUMMARY_DF = REP2_SORTED.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT_SUM"))
    print(f"Summary records: {SUMMARY_DF.height}")

    # PROCESS WALW DATA from SAS7BDAT (lowercase, format: elw{REPTMON}{WK})
    walw_file = f"{BNM_INPUT_PATH}/elw071{SAS_EXTENSION}"
    print(f"Reading: {walw_file}")
    WALW_DF = read_sas7bdat(walw_file)

    if WALW_DF.height > 0:
        print(f"WALW columns: {WALW_DF.columns}")
        print(f"WALW record count: {WALW_DF.height}")
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
        print(f"WALW after processing: {WALW_FINAL.height} records")

        # CREATE WALW SUMMARY
        WALW_SUMMARY = WALW_FINAL.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))
        print(f"WALW summary records: {WALW_SUMMARY.height}")

        # MERGE AND CALCULATE VARIANCE
        MERGED_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left")
        VARIANCE_DF = MERGED_DF.with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
        print(f"Variance records: {VARIANCE_DF.height}")
    else:
        VARIANCE_DF = pl.DataFrame()
        print(f"Warning: {walw_file} not found or empty")

    # CREATE REP0 DATA FOR REVERSE REPO
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


python output 1:

PUBLIC BANK BERHAD                                                                                  
DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR :  DAYA                                                 
REPORT DATE :  16/07/26

ELDAY      BNMCODE              UTSTY      UTREF                    AMOUNT BNMCODG             
----------------------------------------------------------------------------------------------------
DAYA       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYA       3721000000000Y       MGS        AFSLIQ         6,136,116,580.40 3721000000000Y-MGS A
DAYA       3721000000000Y       MGS        DLG            1,157,243,500.00 3721000000000Y-MGS D
DAYA       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYA       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYA       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYA       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYA       3723000000000Y       MGI        AFSLIQ         5,285,092,981.40 3723000000000Y-MGI A
DAYA       3723000000000Y       MGI        DLG              927,171,200.00 3723000000000Y-MGI D
DAYA       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYA                                          36,893,829,812.72                     

DAYB       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYB       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYB       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYB       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYB       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYB       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYB       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYB       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYB       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYB       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYB                                          37,017,498,212.72                     

DAYC       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYC       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYC       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYC       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYC       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYC       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYC       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYC       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYC       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYC       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYC                                          37,017,498,212.72                     

DAYD       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYD       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYD       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYD       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYD       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYD       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYD       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYD       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYD       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYD       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYD                                          37,017,498,212.72                     

DAYE       3721000000000Y       MGS        AFS            7,851,922,162.00 3721000000000Y-MGS A
DAYE       3721000000000Y       MGS        AFSLIQ         6,309,132,980.40 3721000000000Y-MGS A
DAYE       3721000000000Y       MGS        DLG            1,033,920,600.00 3721000000000Y-MGS D
DAYE       3721000000000Y       MGS        INV            4,955,388,116.60 3721000000000Y-MGS I
DAYE       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYE       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYE       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYE       3723000000000Y       MGI        AFSLIQ         5,048,395,481.40 3723000000000Y-MGI A
DAYE       3723000000000Y       MGI        DLG              806,327,000.00 3723000000000Y-MGI D
DAYE       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYE                                          36,742,891,012.72                     

DAYF       3721000000000Y       MGS        AFS            7,500,978,162.00 3721000000000Y-MGS A
DAYF       3721000000000Y       MGS        AFSLIQ         6,152,916,980.40 3721000000000Y-MGS A
DAYF       3721000000000Y       MGS        DLG            1,033,920,600.00 3721000000000Y-MGS D
DAYF       3721000000000Y       MGS        INV            4,955,388,116.60 3721000000000Y-MGS I
DAYF       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYF       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYF       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYF       3723000000000Y       MGI        AFSLIQ         5,048,395,481.40 3723000000000Y-MGI A
DAYF       3723000000000Y       MGI        DLG              806,327,000.00 3723000000000Y-MGI D
DAYF       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYF                                          36,235,731,012.72                     

DAYI       3721000000000Y       MGS        AFS            7,500,978,162.00 3721000000000Y-MGS A
DAYI       3721000000000Y       MGS        AFSLIQ         5,961,140,480.40 3721000000000Y-MGS A
DAYI       3721000000000Y       MGS        DLG              733,603,600.00 3721000000000Y-MGS D
DAYI       3721000000000Y       MGS        INV            3,324,518,116.60 3721000000000Y-MGS I
DAYI       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYI       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYI       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYI       3723000000000Y       MGI        AFSLIQ         5,095,963,497.60 3723000000000Y-MGI A
DAYI       3723000000000Y       MGI        DLG              872,918,230.40 3723000000000Y-MGI D
DAYI       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYI                                          34,226,926,759.32                     



python output 2:

BNMCODE	ELDAY	AMOUNT_SUM	WALWAMT	VARIANC
3723000000000Y	DAYA	16521246914.299847	21888486525.319996	-5367239611.020149
3723000000000Y	DAYC	16524518414.299847	22045561694.13	-5521043279.830154
3723000000000Y	DAYB	16524518414.299847	21887732081.17	-5363213666.8701515
3722000000000Y	DAYC	428821939.4195999	233563468.89999998	195258470.5195999
3721000000000Y	DAYD	20064157858.999794	25978206550.070004	-5914048691.0702095
3721000000000Y	DAYE	20150363858.999794	25981740748.020008	-5831376889.020214
3721000000000Y	DAYF	19643203858.999805	25966983744.41001	-6323779885.410206
3721000000000Y	DAYI	17520240358.999832	25995092585.780018	-8474852226.780186
3722000000000Y	DAYB	428821939.4195999	233494596.42999998	195327342.9895999
3723000000000Y	DAYE	16163705214.299856	22050026535.40001	-5886321321.100153
3722000000000Y	DAYD	428821939.4195999	233563468.89	195258470.5295999
3722000000000Y	DAYE	428821939.4195999	233563468.89999998	195258470.5195999
3723000000000Y	DAYF	16163705214.299856	22052621875.440014	-5888916661.140158
3722000000000Y	DAYF	428821939.4195999	233584753.90999997	195237185.50959992
3723000000000Y	DAYD	16524518414.299847	22047794084.250008	-5523275669.950161
3721000000000Y	DAYA	19943760958.999798	25756115073.279995	-5812354114.280197
3723000000000Y	DAYI	16277864460.899855	22157190651.18002	-5879326190.280165
3722000000000Y	DAYI	428821939.4195999	233625677.55999997	195196261.85959992
3721000000000Y	DAYB	20064157858.999794	25781731033.86	-5717573174.860207
3722000000000Y	DAYA	428821939.4195999	233474892.45	195347046.9695999
3721000000000Y	DAYC	20064157858.999794	25975629214.04	-5911471355.040207


actual production output:

PUBLIC BANK BERHAD                                                                                                                   
DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR :  DAYA                                                                                  
REPORT DATE :  08/07/26                                                                                                              
                                                                                                                                     
  FMTNAME  BNMCODE         DESC                                      SIGN                  AMOUNT                   TOTAL            
  -----------------------------------------------------------------------------------------------------------------------            
  A-RMEL   4211000000000Y  RM DEMAND DEPOSITS ACCEPTED               +          58,355,074,524.53       58,355,074,524.53            
           4212000000000Y  RM SAVINGS DEPOSITS ACCEPTED              +          34,788,322,240.58       34,788,322,240.58            
           4213000000000Y  RM FIXED DEPOSITS ACCEPTED                +         150,458,756,066.17      150,458,756,066.17            
           4213100000000Y  RM SPECIAL INVESTMENT DEPOSIT ACCEPTED    +                       0.00                    0.00            
           4213200000000Y  RM GENERAL INVESTMENT DEPOSIT ACCEPTED    +                       0.00                    0.00            
           4213300000000Y  RM COMMODITY MURABAHAH                    +                       0.00                    0.00            
           4215000000000Y  RM NID ISSUED                             +             900,000,000.00          900,000,000.00            
           4216000000000Y  RM REPURCHASE AGREEMENTS                  +          11,622,078,998.66       11,622,078,998.66            
           4217071000000Y  RM SPECIAL DEPOSITS                       +                       0.00                    0.00            
           4218000000000Y  RM HOUSING DEVELOPMENT ACCOUNTS           +           1,712,725,630.10        1,712,725,630.10            
           4219000000000Y  RM SHORT TERM DEPOSIT ACCEPTED            +          61,770,782,382.26       61,770,782,382.26            
           4219100000000Y  RM INVESTMENT LINKED TO DERIVATIVES       +             386,914,369.00          386,914,369.00            
           4219900000000Y  RM OTHER DEPOSITS ACCEPTED                +              37,646,848.06           37,646,848.06            
           4310000000000Y  RM AMOUNT DUE TO DESIGNATED FI            +                       0.00                    0.00            
           4311002000000Y  RM VOSTRO ACCOUNTS OF CB                  +                 486,854.46              486,854.46            
           4311003000000Y  RM VOSTRO ACCOUNTS OF IB                  +              54,444,827.40           54,444,827.40            
           4311081000000Y  RM VOSTRO ACCOUNTS OF FBI                 +             100,012,977.00          100,012,977.00            
           4312002000000Y  RM OVERDRAWN NOSTRO ACCOUNTS WITH CB      +                       0.00                    0.00            
           4312003000000Y  RM OVERDRAWN NOSTRO ACCOUNTS WITH IB      +                       0.00                    0.00            
           4313000000000Y  RM DEFICIT IN SPICK                       +                       0.00                    0.00            
           4313002000000Y  RM AMOUNT BORROWING FROM SPICK POOL CB    +                       0.00                    0.00            
           4313003000000Y  RM AMOUNT BORROWING FROM SPICK POOL IB    +                       0.00                    0.00            
           4314001000000Y  RM INTERBANK BORROWINGS FROM BNM          +             113,171,358.83          113,171,358.83            
           4314002000000Y  RM INTERBANK BORROWINGS FROM CB           +           2,255,000,000.00        2,255,000,000.00            
           4314003000000Y  RM INTERBANK BORROWINGS FROM IB           +                       0.00                    0.00            
           4314011000000Y  RM INTERBANK BORROWINGS FROM FC           +                       0.00                    0.00            
           4314012000000Y  RM INTERBANK BORROWINGS FROM MB           +                       0.00                    0.00            
           4314013000000Y  RM INTERBANK BORROWINGS FROM DH           +                       0.00                    0.00            
           4314017000000Y  O/W RM IBB FROM CAGAMAS           0.00    +                       0.00                    0.00            
           4314020000000Y  RM INTERBANK BORROWINGS FROM DNBFI        +                       0.00                    0.00            
           4314081100000Y  RM INTERBANK BORROWINGS FROM FBI <= 1 YR  +             150,000,000.00          150,000,000.00            
           4410000000000Y  RM MISC BORROWINGS                        +          10,197,952,678.54       10,197,952,678.54            
           4911080000000Y  RM INTEREST PAYABLE TO NON-RESIDENTS      +              53,372,766.43           53,372,766.43            
           4911095000000Y  RM INTEREST PAYABLE TO NON-RES - DCI/CRA  +                  28,349.42               28,349.42            
           4912080000000Y  RM BILLS PAYABLE TO NON-RESIDENTS         +                       0.00                    0.00            
           4929980000000Y  OTHER RM MISC LIAB NIE DUE TO NON-RES     +                       0.00                    0.00            
           4929995000000Y  RM GOLD INVESTMENT FROM NON-RESIDENTS     +              38,161,231.95           38,161,231.95            
           4929996000000Y  OTHR RM MISC LIAB NIE DUE TO NON-RES-DCI  +                     583.12                  583.12            
           4411100000000Y  RM SUBORDINATED DEBT CAPITAL              -           4,997,935,844.48        4,997,935,844.48            
           4411200000000Y  RM EXEMPT SUBORDINATED DEBT CAPITAL       -                       0.00                    0.00            
           4411300000000Y  RM SUBORDIN DEBT CAPITAL W APPR FR BNM    -                       0.00                    0.00            
           4414000000000Y  RM RESOURCE OBLIQ ON LN SOLD TO CAGAMAS   -           5,200,016,834.06        5,200,016,834.06            
  -----------------------------------------------------------------------------------------------------------------------            
           TOTAL FOR A-RMEL                                                    322,796,980,007.97      322,796,980,007.97            



a bit different compared to python output. can you refix the full code so i can have the correct formatting as per production output?

