import duckdb
import polars as pl
from pathlib import Path

# PROCESS REPTDAT1 FROM LOAN.REPTDATE
REPTDATE_LOAN_DF = duckdb.connect().execute("SELECT * FROM READ_PARQUET('LOAN/REPTDATE.PARQUET')").pl()
REPTDATE_LOAN = REPTDATE_LOAN_DF["REPTDATE"][0]

SDESC = "PUBLIC BANK BERHAD"
RPDATE = REPTDATE_LOAN.strftime("%d%m%y")
RDATE = REPTDATE_LOAN.strftime("%d%m%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]
DATE_VAR = REPTDATE_LOAN
MTHEND = f"{REPTDATE_LOAN.day:02d}"

# PROCESS REPTDATE FROM BNMK.REPTDATE
REPTDATE_BNMK_DF = duckdb.connect().execute("SELECT * FROM READ_PARQUET('BNMK/REPTDATE.PARQUET')").pl()
REPTDATE_BNMK = REPTDATE_BNMK_DF["SXDATE"][0]

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
        SXDATE = REPTDATE_BNMK.replace(month=1, day=1) - pl.duration(years=1)
else:
    SXDATE = REPTDATE_BNMK

NOWK = WK
WK_VAR = WK
REPTMON = f"{MM:02d}"
RPDATE_BNMK = SXDATE.strftime("%d%m%y")
RPYEAR = SXDATE.strftime("%Y")
REPTYEAR = SXDATE.strftime("%Y")
YEAR_SHORT = SXDATE.strftime("%y")

# PROCESS REP2 DATA
REP2_DF = duckdb.connect().execute(f"SELECT * FROM READ_PARQUET('BNMK/REP2{REPTMON}{WK}.PARQUET')").pl()
REP2_FILTERED = REP2_DF.filter(
    ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
      (~pl.col("UTREF").is_in(['DLG','IDLG'])))
)

# PROCESS REP4 DATA
REP4_DF = duckdb.connect().execute(f"SELECT * FROM READ_PARQUET('BNMK/REP4{REPTMON}{WK}.PARQUET')").pl()
REP4_FILTERED = REP4_DF.filter(
    ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
      (~pl.col("UTREF").is_in(['DLG','IDLG'])))
)

# COMBINE REP2 AND REP4
REP2_COMBINED = pl.concat([REP2_FILTERED, REP4_FILTERED])

# TRANSFORM DATA
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

# PROCESS WALW DATA
WALW_DF = duckdb.connect().execute(f"SELECT * FROM READ_PARQUET('BNM/ELW{REPTMON}{WK}.PARQUET')").pl()
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

# CREATE REP0 DATA FOR REVERSE REPO
REP0_DF = REP2_DF.filter(pl.col("BNMCODE") == '3250000000000Y').with_columns(
    (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)

# CREATE OUTPUT DIRECTORIES
Path("OUTPUT/REPORTS").mkdir(parents=True, exist_ok=True)
Path("SFTP_UPLOAD").mkdir(exist_ok=True)

# WRITE OUTPUT FILES - PARQUET AND CSV
REP2_SORTED.write_parquet(f"OUTPUT/REPORTS/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}.PARQUET")
REP2_SORTED.write_csv(f"OUTPUT/REPORTS/DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}.CSV")

VARIANCE_DF.write_parquet(f"OUTPUT/REPORTS/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}.PARQUET")
VARIANCE_DF.write_csv(f"OUTPUT/REPORTS/KAPITI_WALKER_VARIANCE_REPORT_{REPTMON}{WK}_{RYEAR}.CSV")

REP0_DF.write_parquet(f"OUTPUT/REPORTS/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}.PARQUET")
REP0_DF.write_csv(f"OUTPUT/REPORTS/REVERSE_REPO_PURCHASE_PROCEEDS_{REPTMON}{WK}_{RYEAR}.CSV")
