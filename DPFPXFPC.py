"""
EIBWKAPE - DAILY KAPITI STOCK / VARIANCE / REV REPO REPORTS
Fixed version - corrects two bugs found against the original SAS source:

  1. WALW (Walker) file name was hardcoded as 'elw071' instead of being
     built dynamically as elw{REPTMON}{WK}, per SAS: BNM.ELW&REPTMON&WK
  2. REP0 (Reverse Repo report) was being built from the RAW, unfiltered
     REP2 read. SAS re-reads REP2 fresh and RE-APPLIES the UTSTY/UTREF
     exclusion filter before building REP0. Python now does the same.

NOTE: This program does NOT include the PBBELQ "Detail Total Eligible
Liabilities" report (%INC PGM(PBBELQ) in the SAS main program). That
report needs a BNMCODE -> FMTNAME/DESC/SIGN/IDX lookup table (built by
PBBELF, which references EL/ELI datasets not yet provided) plus TBL1,
DCI, and GOLD source files. That is a separate deliverable once those
are available.
"""

import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"  # BNMK.* (REP2, REP4)
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"   # BNM.*  (ELW / Walker)

OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

USE_CURRENT_DATE = False
CUSTOM_DATE = datetime(2026, 7, 16)
DAYS_OFFSET = 1

TEXT_SEPARATOR = '\t'

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


# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

# REPTDAT1 FROM LOAN.REPTDATE
SDESC = "PUBLIC BANK BERHAD"
RDATE = REPTDATE_LOAN.strftime("%d/%m/%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]

# REPTDATE FROM BNMK.REPTDATE
# NOTE: SAS reads a SEPARATE date value (SXDATE) from BNMK.REPTDATE here,
# distinct from LOAN.REPTDATE. That source file has not been provided,
# so this still assumes the same reporting date drives both. Flag this
# if BNMK.REPTDATE ever diverges from LOAN.REPTDATE in production.
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
# LOAD REP2 / REP4 (raw, kept for REP0 later)
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

# COMBINE REP2 + REP4 (filtered) for the stock/variance pipeline
frames = [d for d in (REP2_FILTERED, REP4_FILTERED) if d.height > 0]
REP2_COMBINED = pl.concat(frames) if frames else pl.DataFrame()
print(f"Combined REP2+REP4 (filtered): {REP2_COMBINED.height} records")

# ============================================
# TRANSFORM DATA (DATA REP2; SET REP2 REP4; ... BNMCODG=...)
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
    # SUMMARY (PROC SUMMARY BY BNMCODE ELDAY)
    # ============================================

    SUMMARY_DF = REP2_SORTED.group_by(["BNMCODE", "ELDAY"]).agg(
        pl.col("AMOUNT").sum().alias("AMOUNT_SUM")
    )
    print(f"Summary records: {SUMMARY_DF.height}")

    # ============================================
    # WALW (Walker) - FIXED: dynamic file name, not hardcoded elw071
    # ============================================

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
        print(f"WALW summary records: {WALW_SUMMARY.height}")

        MERGED_DF = SUMMARY_DF.join(WALW_SUMMARY, on=["BNMCODE", "ELDAY"], how="left")
        VARIANCE_DF = MERGED_DF.with_columns(
            (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
        )
        print(f"Variance records: {VARIANCE_DF.height}")
    else:
        VARIANCE_DF = pl.DataFrame()
        print(f"Warning: {walw_file} not found or empty")

    # ============================================
    # REP0 - REVERSE REPO AT PURCHASE PROCEEDS
    # FIXED: SAS re-reads REP2 fresh and RE-APPLIES the UTSTY/UTREF
    # filter before subsetting to BNMCODE='3250000000000Y'. Previously
    # this used the raw, unfiltered REP2 read.
    # ============================================

    REP2_REFILTERED = apply_utsty_filter(REP2_RAW)

    if REP2_REFILTERED.height > 0:
        REP0_DF = REP2_REFILTERED.filter(
            pl.col("BNMCODE") == '3250000000000Y'
        ).with_columns(
            (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
        )
        # SAS report includes AMOUNT, COSTDED ("(-) PURC PROC."), NETAMT ("MARKET SEC")
        missing = [c for c in ("COSTDED", "NETAMT") if c not in REP0_DF.columns]
        if missing:
            print(f"Warning: REP0 source is missing expected column(s) {missing}; "
                  f"REV REPO report will omit them.")
        print(f"Reverse Repo records: {REP0_DF.height}")
    else:
        REP0_DF = pl.DataFrame()

    # ============================================
    # OUTPUT
    # ============================================

    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print(f"SFTP upload directory: {SFTP_UPLOAD_PATH}")
    print("\nWriting output files...")

    if REP2_SORTED.height > 0:
        filename_base = f"DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}"
        REP2_SORTED.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(REP2_SORTED, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    if VARIANCE_DF.height > 0:
        filename_base = f"KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}"
        VARIANCE_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(VARIANCE_DF, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    if REP0_DF.height > 0:
        filename_base = f"REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}"
        REP0_DF.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")
        write_to_text(REP0_DF, f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}")

    print("\n" + "=" * 50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    if REP2_SORTED.height > 0:
        print(f"Stock Report: {REP2_SORTED.height} records")
    if VARIANCE_DF.height > 0:
        print(f"Variance Report: {VARIANCE_DF.height} records")
    if REP0_DF.height > 0:
        print(f"Reverse Repo: {REP0_DF.height} records")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("=" * 50)

else:
    print("Error: No data available to process")
