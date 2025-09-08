# EIIWCCR6.py
# Translation of EIIWCCR6 SAS job into Python + Polars
# ----------------------------------------------------

import polars as pl
from datetime import datetime
from pathlib import Path

# ======================================
# 1. CONFIGURATION
# ======================================
BASE_INPUT = Path("/sas/parquet/input")     # replace with actual input parquet directory
BASE_OUTPUT = Path("/sas/parquet/output")   # replace with actual output directory
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Input datasets (converted from raw mainframe files)
REPTDATE_FILE = BASE_INPUT / "LOAN_REPTDATE.parquet"
LOAN_FILE     = BASE_INPUT / "LOAN.parquet"
LIMIT_FILE    = BASE_INPUT / "LIMIT.parquet"
OLIMIT_FILE   = BASE_INPUT / "OLIMIT.parquet"
LNHIST_FILE   = BASE_INPUT / "LNHIST.parquet"
CISLN_FILE    = BASE_INPUT / "CISLN.parquet"
CISDP_FILE    = BASE_INPUT / "CISDP.parquet"
FCYCA_FILE    = BASE_INPUT / "FCYCA.parquet"

# ======================================
# 2. READ REPORT DATE (equivalent to DATA REPTDATE)
# ======================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate_value = reptdate_df["REPTDATE"][0]

day_val   = reptdate_value.day
month_val = reptdate_value.month
year_val  = reptdate_value.year
startdte  = datetime(year_val, month_val, 1)

if day_val == 8:
    day = "10"; sdd = 1
elif day_val == 15:
    day = "17"; sdd = 9
elif day_val == 22:
    day = "24"; sdd = 16
else:
    day = "02"; sdd = 23

currdate = datetime(year_val, month_val, sdd)
today    = datetime.today()
nowmonth, nowyear = today.month, today.year

RDATE = reptdate_value.strftime("%Y%m%d")
SDATE = startdte.strftime("%Y%m%d")
CDATE = currdate.strftime("%Y%m%d")

# ======================================
# 3. CISLN extract (WHERE SECCUST='901')
# ======================================
cisln = (
    pl.read_parquet(CISLN_FILE)
    .filter(pl.col("SECCUST") == "901")
    .select(["ACCTNO", "INDORG"])
    .unique(subset=["ACCTNO"])
)

# ======================================
# 4. LNHIST filter (TRANCODE=400, RATECHG!=0, date range)
# ======================================
lnhist = (
    pl.read_parquet(LNHIST_FILE)
    .filter(
        (pl.col("TRANCODE") == 400) &
        (pl.col("RATECHG") != 0) &
        (pl.col("EFFDATE") >= int(CDATE)) &
        (pl.col("EFFDATE") <= int(RDATE))
    )
    .select(["ACCTNO", "NOTENO", "EFFDATE"])
    .sort(["ACCTNO", "NOTENO", "EFFDATE"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"])
)

# ======================================
# 5. FCYCA foreign exchange rates
# ======================================
fcyca = (
    pl.read_parquet(FCYCA_FILE)
    .filter(pl.col("REPTDATE") <= int(RDATE))
    .sort(["CURCODE", "REPTDATE"], descending=[False, True])
    .unique(subset=["CURCODE"])
    .select(["CURCODE", "SPOTRATE"])
)

# ======================================
# 6. LNNOTE filtering (PRODCD 34190/34690)
# ======================================
lnnote = (
    pl.read_parquet(LOAN_FILE)
    .select(["ACCTNO", "NOTENO", "NTBRCH", "CURBAL", "DELQCD", "ISSUEDT", "LOANTYPE", "CURCODE"])
    .with_columns([
        pl.when((pl.col("ISSUEDT").is_not_null()) & (pl.col("ISSUEDT") != 0))
          .then(pl.col("ISSUEDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
          .alias("ISSDTE"),
        pl.col("LOANTYPE").cast(pl.Utf8).alias("PRODCD")
    ])
    .filter(pl.col("PRODCD").is_in(["34190", "34690"]))
    .join(fcyca, left_on="CURCODE", right_on="CURCODE", how="left")
    .with_columns(
        pl.when(pl.col("CURCODE") != "MYR")
          .then((pl.col("CURBAL") * pl.col("SPOTRATE")).round(2))
          .otherwise(pl.col("CURBAL"))
          .alias("CURBAL")
    )
    .drop("SPOTRATE")
)

lnnote = lnnote.sort(["ACCTNO", "NOTENO"])

# ======================================
# 7. Merge LNNOTE + LNHIST
# ======================================
lnnote_hist = (
    lnnote.join(lnhist, on=["ACCTNO", "NOTENO"], how="inner")
    .rename({"NTBRCH": "BRANCH"})
    .filter((pl.col("DELQCD").is_null()) & (pl.col("ISSDTE") < datetime.strptime(SDATE, "%Y%m%d")) & (pl.col("CURBAL") != 0))
)

# ======================================
# 8. Join with CISLN and write OUTPUT1
# ======================================
out1 = (
    lnnote_hist.join(cisln, on="ACCTNO", how="inner")
    .with_columns([
        (pl.col("CURBAL") * 100).alias("CURBAL100"),
        pl.col("EFFDATE").cast(pl.Date).dt.year().cast(pl.Utf8).alias("EFFDATEYY"),
        pl.col("EFFDATE").cast(pl.Date).dt.month().cast(pl.Utf8).alias("EFFDATEMM"),
        pl.col("EFFDATE").cast(pl.Date).dt.day().cast(pl.Utf8).alias("EFFDATEDD"),
    ])
    .select(["BRANCH", "ACCTNO", "NOTENO", "INDORG", "EFFDATEYY", "EFFDATEMM", "EFFDATEDD", "CURBAL100"])
)
out1.write_parquet(BASE_OUTPUT / f"EIIWCCR6_ROLLOVER_{RDATE}.parquet")
out1.write_csv(BASE_OUTPUT / f"EIIWCCR6_ROLLOVER_{RDATE}.csv")

# ======================================
# 9. ODREVIEW processing
# ======================================
odrev = (
    pl.read_parquet(LIMIT_FILE)
    .filter((pl.col("APPRLIMT") > 1) & (pl.col("LMTAMT") > 1))
    .select(["BRANCH", "ACCTNO", "LMTDESC", "LMTID", "REVIEWDT", "LMTAMT"])
    .sort(["ACCTNO", "LMTID"])
)

prevodrev = (
    pl.read_parquet(OLIMIT_FILE)
    .filter((pl.col("APPRLIMT") > 1) & (pl.col("REVIEWDT").is_not_null()) & (pl.col("REVIEWDT") != 0))
    .select(["ACCTNO", "LMTID", "REVIEWDT"])
    .rename({"REVIEWDT": "PREVIEWDT"})
    .sort(["ACCTNO", "LMTID"])
)

odreview = (
    odrev.join(prevodrev, on=["ACCTNO", "LMTID"], how="inner")
    .with_columns([
        pl.when((pl.col("REVIEWDT") != 0) & (pl.col("REVIEWDT").is_not_null()))
          .then(pl.col("REVIEWDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
          .otherwise(None)
          .alias("REVIEWDT"),
        pl.when((pl.col("PREVIEWDT") != 0) & (pl.col("PREVIEWDT").is_not_null()))
          .then(pl.col("PREVIEWDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
          .otherwise(None)
          .alias("PREVIEWDT"),
    ])
    .filter(pl.col("REVIEWDT") > pl.col("PREVIEWDT"))
)

odlimit = (
    odreview.groupby(["ACCTNO", "LMTDESC"])
    .agg(pl.sum("LMTAMT").alias("LMTAMT"))
)

odreview = (
    odreview.join(odlimit, on=["ACCTNO", "LMTDESC"], how="left")
    .unique(subset=["ACCTNO", "LMTDESC"])
)

# ======================================
# 10. Join with CISDP and write OUTPUT2
# ======================================
cisdp = (
    pl.read_parquet(CISDP_FILE)
    .filter(pl.col("SECCUST") == "901")
    .select(["ACCTNO", "INDORG"])
    .unique(subset=["ACCTNO"])
)

out2 = (
    odreview.join(cisdp, on="ACCTNO", how="inner")
    .with_columns([
        (pl.col("LMTAMT") * 100).alias("LMTAMT100"),
        pl.lit("R").alias("APPLTYPE"),
        pl.lit(RDATE).alias("APPLDATE"),
        pl.col("REVIEWDT").dt.strftime("%Y%m%d").alias("REVDATE"),
        pl.col("PREVIEWDT").dt.strftime("%Y%m%d").alias("PREVDATE"),
    ])
    .select(["BRANCH", "ACCTNO", "LMTDESC", "INDORG", "PREVDATE", "REVDATE", "LMTAMT100", "APPLDATE", "APPLTYPE"])
)

out2.write_parquet(BASE_OUTPUT / f"EIIWCCR6_ODREVIEW_{RDATE}.parquet")
out2.write_csv(BASE_OUTPUT / f"EIIWCCR6_ODREVIEW_{RDATE}.csv")

print(f"✅ Completed EIIWCCR6 job for {RDATE}")
