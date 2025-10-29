import duckdb
import polars as pl
from pathlib import Path

# Process REPTDATE from LOAN
reptdate_loan_df = duckdb.connect().execute("SELECT * FROM read_parquet('LOAN/REPTDATE.parquet')").pl()
reptdate_loan = reptdate_loan_df["REPTDATE"][0]

sdesc = "PUBLIC BANK BERHAD"
rpdate = reptdate_loan.strftime("%d%m%y")
rdate = reptdate_loan.strftime("%d%m%y")
ryear = reptdate_loan.strftime("%Y")
mthnam = reptdate_loan.strftime("%B")
sdesc_formatted = sdesc.ljust(26)[:26]
date_var = reptdate_loan
mthend = f"{reptdate_loan.day:02d}"

# Process REPTDATE from BNMK
reptdate_bnmk_df = duckdb.connect().execute("SELECT * FROM read_parquet('BNMK/REPTDATE.parquet')").pl()
reptdate_bnmk = reptdate_bnmk_df["SXDATE"][0]

mm = reptdate_bnmk.month
day = reptdate_bnmk.day

if 1 <= day <= 8:
    wk = '4'
elif 9 <= day <= 15:
    wk = '1'
elif 16 <= day <= 22:
    wk = '2'
else:
    wk = '3'

if wk == '4':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
    mm = mm1
    if mm == 12:
        sxdate = reptdate_bnmk.replace(month=1, day=1) - pl.duration(years=1)

nowk = wk
wk_var = wk
reptmon = f"{mm:02d}"
rpdate_bnmk = sxdate.strftime("%d%m%y")
rpyear = sxdate.strftime("%Y")
reptyear = sxdate.strftime("%Y")
year_short = sxdate.strftime("%y")

# Load and filter REP2 data
rep2_df = duckdb.connect().execute(f"SELECT * FROM read_parquet('BNMK/REP2{reptmon}{wk}.parquet')").pl()
rep2_filtered = rep2_df.filter(
    ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
      (~pl.col("UTREF").is_in(['DLG','IDLG'])))
)

# Load and filter REP4 data
rep4_df = duckdb.connect().execute(f"SELECT * FROM read_parquet('BNMK/REP4{reptmon}{wk}.parquet')").pl()
rep4_filtered = rep4_df.filter(
    ~((pl.col("UTSTY").is_in(['CB1','CF1','CNT','SAC','SMC','ISB'])) & 
      (~pl.col("UTREF").is_in(['DLG','IDLG'])))
)

# Combine REP2 and REP4
rep2_combined = pl.concat([rep2_filtered, rep4_filtered])

# Transform data
rep2_transformed = rep2_combined.with_columns([
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

# Sort and create summary
rep2_sorted = rep2_transformed.sort("BNMCODG")
summary_df = rep2_sorted.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT_SUM"))

# Load and process WALW data
walw_df = duckdb.connect().execute(f"SELECT * FROM read_parquet('BNM/ELW{reptmon}{wk}.parquet')").pl()
walw_processed = walw_df.with_columns([
    pl.when(pl.col("BNMCODE") == '3250001000000Y')
      .then(pl.lit('3250000000000Y'))
      .otherwise(pl.col("BNMCODE"))
      .alias("BNMCODE")
])

# Duplicate records for specific condition
walw_duplicated = walw_processed.filter(pl.col("BNMCODE") == '3551000000000Y').with_columns(
    pl.lit('3552000000000Y').alias("BNMCODE")
)
walw_final = pl.concat([walw_processed, walw_duplicated])

# Create WALW summary
walw_summary = walw_final.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))

# Merge and calculate variance
merged_df = summary_df.join(walw_summary, on=["BNMCODE", "ELDAY"], how="left")
variance_df = merged_df.with_columns(
    (pl.col("AMOUNT_SUM") - pl.col("WALWAMT")).alias("VARIANC")
)

# Create REP0 data for reverse repo
rep0_df = rep2_df.filter(pl.col("BNMCODE") == '3250000000000Y').with_columns(
    (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)

# Create output directories
Path("output/reports").mkdir(parents=True, exist_ok=True)

# Write output files
rep2_sorted.write_parquet(f"output/reports/securities_report_{reptmon}{wk}_{rpyear}.parquet")
rep2_sorted.write_csv(f"output/reports/securities_report_{reptmon}{wk}_{rpyear}.csv")

variance_df.write_parquet(f"output/reports/variance_report_{reptmon}{wk}_{rpyear}.parquet")
variance_df.write_csv(f"output/reports/variance_report_{reptmon}{wk}_{rpyear}.csv")

rep0_df.write_parquet(f"output/reports/reverse_repo_report_{reptmon}{wk}_{rpyear}.parquet")
rep0_df.write_csv(f"output/reports/reverse_repo_report_{reptmon}{wk}_{rpyear}.csv")
