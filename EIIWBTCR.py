import polars as pl
from pathlib import Path
from datetime import datetime

# === CONFIG ===
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def save(df: pl.DataFrame, name: str):
    """Save dataframe to parquet and csv"""
    df.write_parquet(OUTPUT_DIR / f"{name}.parquet")
    df.write_csv(OUTPUT_DIR / f"{name}.csv")

# === INPUTS (replace with actual .parquet paths) ===
loan = pl.read_parquet("input/loan_reptdate.parquet")
lnhist = pl.read_parquet("input/lnhist.parquet")
limit = pl.read_parquet("input/limit.parquet")
olimit = pl.read_parquet("input/olimit.parquet")
fcyca = pl.read_parquet("input/fcyca.parquet")
cisln = pl.read_parquet("input/cisln.parquet")
cisdp = pl.read_parquet("input/cisdp.parquet")
lnnote = pl.read_parquet("input/lnnote.parquet")

# --- STEP 1: REPTDATE processing ---
reptdate = loan.with_columns([
    pl.col("REPTDATE").cast(pl.Datetime).dt.day().alias("DAY"),
    pl.col("REPTDATE").cast(pl.Datetime).dt.month().alias("MONTH"),
    pl.col("REPTDATE").cast(pl.Datetime).dt.year().alias("YEAR"),
])

def classify_day(d: int):
    if d == 8:
        return ("10","01")
    elif d == 15:
        return ("17","09")
    elif d == 22:
        return ("24","16")
    else:
        return ("02","23")

day_map = reptdate.select("DAY").to_series().apply(lambda d: classify_day(d))
reptdate = reptdate.with_columns([
    pl.Series(name="DAYSTR", values=[x[0] for x in day_map]),
    pl.Series(name="SDD", values=[int(x[1]) for x in day_map])
])

currdate = reptdate.select(
    (pl.datetime(
        year=pl.col("YEAR"),
        month=pl.col("MONTH"),
        day=pl.col("SDD")
    )).alias("CURRDATE")
)

RDATE = reptdate["REPTDATE"][0]
SDATE = datetime(reptdate["YEAR"][0], reptdate["MONTH"][0], 1)
CDATE = currdate["CURRDATE"][0]

# --- STEP 2: CISLN subset ---
cisln = cisln.filter(pl.col("SECCUST")=="901").select(["ACCTNO","INDORG"]).unique()

# --- STEP 3: LNHIST subset ---
lnhist = (
    lnhist.filter(
        (pl.col("TRANCODE")==400) & 
        (pl.col("RATECHG")!=0) &
        (pl.col("EFFDATE")>=CDATE) & 
        (pl.col("EFFDATE")<=RDATE)
    )
    .select(["ACCTNO","NOTENO","EFFDATE"])
    .sort(["ACCTNO","NOTENO","EFFDATE"], descending=[False,False,True])
    .unique(subset=["ACCTNO","NOTENO"], keep="first")
)

# --- STEP 4: FCYCA exchange rates ---
forate = (
    fcyca.filter(pl.col("REPTDATE")<=RDATE)
    .sort(["CURCODE","REPTDATE"], descending=[False,True])
    .unique(subset=["CURCODE"], keep="first")
)

# --- STEP 5: LNNOTE join with LNHIST ---
lnnote = (
    lnnote.with_columns([
        pl.when((pl.col("ISSUEDT")!=0) & pl.col("ISSUEDT").is_not_null())
          .then(pl.col("ISSUEDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%Y%m%d"))
          .otherwise(None)
          .alias("ISSDTE")
    ])
    .select(["ACCTNO","NOTENO","NTBRCH","CURBAL","DELQCD","ISSDTE","LOANTYPE","CURCODE"])
    .join(lnhist, on=["ACCTNO","NOTENO"], how="inner")
    .rename({"NTBRCH":"BRANCH"})
)

lnnote = lnnote.filter(
    (pl.col("DELQCD").is_null()) & 
    (pl.col("ISSDTE") < SDATE) &
    (pl.col("CURBAL") != 0)
)

# --- STEP 6: OUTPUT1 (ROLLOVER accounts) ---
output1 = lnnote.join(cisln, on="ACCTNO", how="inner").with_columns([
    (pl.col("CURBAL")*100).alias("CURBAL100"),
    pl.col("EFFDATE").dt.year().cast(pl.Utf8).alias("EFFDATEYY"),
    pl.col("EFFDATE").dt.month().cast(pl.Utf8).str.zfill(2).alias("EFFDATEMM"),
    pl.col("EFFDATE").dt.day().cast(pl.Utf8).str.zfill(2).alias("EFFDATEDD"),
])

save(output1, "rollover")

# --- STEP 7: ODREVIEW (overdraft review merge) ---
odrev = (
    limit.filter((pl.col("APPRLIMT")>1) & (pl.col("LMTAMT")>1))
    .select(["BRANCH","ACCTNO","LMTDESC","LMTID","REVIEWDT","LMTAMT"])
    .sort(["ACCTNO","LMTID"])
)

prevodrev = (
    olimit.filter((pl.col("APPRLIMT")>1) & (pl.col("REVIEWDT")>0))
    .select(["ACCTNO","LMTID","REVIEWDT"])
    .rename({"REVIEWDT":"PREVIEWDT"})
)

odreview = odrev.join(prevodrev, on=["ACCTNO","LMTID"], how="inner")

odreview = odreview.with_columns([
    pl.when((pl.col("REVIEWDT")>0) & pl.col("REVIEWDT").is_not_null())
      .then(pl.col("REVIEWDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%Y%m%d"))
      .otherwise(None)
      .alias("REVIEWDT"),
    pl.when((pl.col("PREVIEWDT")>0) & pl.col("PREVIEWDT").is_not_null())
      .then(pl.col("PREVIEWDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%Y%m%d"))
      .otherwise(None)
      .alias("PREVIEWDT")
])

odreview = odreview.filter(pl.col("REVIEWDT") > pl.col("PREVIEWDT"))

# aggregate limits
odlimit = odreview.groupby(["ACCTNO","LMTDESC"]).agg(pl.sum("LMTAMT").alias("LMTAMT_SUM"))

odreview = odreview.join(odlimit, on=["ACCTNO","LMTDESC"], how="left")

# --- STEP 8: OUTPUT2 (ODREVIEW accounts) ---
cisdp = cisdp.filter(pl.col("SECCUST")=="901").select(["ACCTNO","INDORG"]).unique()

output2 = odreview.join(cisdp, on="ACCTNO", how="inner").with_columns([
    (pl.col("LMTAMT_SUM")*100).alias("LMTAMT100"),
    pl.lit("R").alias("APPLTYPE"),
    pl.lit(int(RDATE.strftime("%Y%m%d"))).alias("APPLDATE"),
    pl.when(pl.col("REVIEWDT").is_not_null())
      .then(pl.col("REVIEWDT").dt.strftime("%Y%m%d"))
      .otherwise("0").alias("REVDATE"),
    pl.when(pl.col("PREVIEWDT").is_not_null())
      .then(pl.col("PREVIEWDT").dt.strftime("%Y%m%d"))
      .otherwise("0").alias("PREVDATE"),
])

save(output2, "odreview")

print("✅ Outputs written to", OUTPUT_DIR)
