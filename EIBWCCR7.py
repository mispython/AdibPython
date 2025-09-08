import polars as pl
from datetime import date, datetime
from pathlib import Path

# ======================
# CONFIG / INPUTS
# ======================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Replace with your real datasets
loan_reptdate = pl.read_parquet(BASE_INPUT / "LOAN_REPTDATE.parquet")
cisln_loan    = pl.read_parquet(BASE_INPUT / "CISLN_LOAN.parquet")
lnhist_week   = pl.read_parquet(BASE_INPUT / "LNHIST_WEEK.parquet")
forate_bkp    = pl.read_parquet(BASE_INPUT / "FCYCA_FORATEBKP.parquet")
lnnote        = pl.read_parquet(BASE_INPUT / "LOAN_LNNOTE.parquet")
limit_overdft = pl.read_parquet(BASE_INPUT / "LIMIT_OVERDFT.parquet")
olimit_overdft= pl.read_parquet(BASE_INPUT / "OLIMIT_OVERDFT.parquet")
cisdp_deposit = pl.read_parquet(BASE_INPUT / "CISDP_DEPOSIT.parquet")

# ======================
# REPTDATE handling
# ======================
def week_logic(day: int):
    if day == 8:   return ("10", 1)
    if day == 15:  return ("17", 9)
    if day == 22:  return ("24",16)
    return ("02",23)

rept = loan_reptdate.with_columns([
    pl.col("REPTDATE").dt.year().alias("YEAR"),
    pl.col("REPTDATE").dt.month().alias("MONTH"),
    pl.col("REPTDATE").dt.day().alias("DAY")
])

wkvals = rept["DAY"].map_elements(lambda d: week_logic(d), return_dtype=pl.Struct)
rept = rept.with_columns([
    wkvals.struct.field("field_0").alias("DAYTXT"),
    wkvals.struct.field("field_1").alias("SDD")
])

rept = rept.with_columns([
    pl.date(rept["YEAR"], rept["MONTH"], rept["SDD"]).alias("CURRDATE"),
    pl.date(rept["YEAR"], rept["MONTH"], 1).alias("STARTDTE")
])

RDATE = rept["REPTDATE"][0]
SDATE = rept["STARTDTE"][0]
CDATE = rept["CURRDATE"][0]

# Write header lines into flat outputs (ROLLOVER + ODREVIEW)
with open(BASE_OUTPUT / "ROLLOVER.txt","w") as f1, \
     open(BASE_OUTPUT / "ODREVIEW.txt","w") as f2:
    f1.write(f"{rept['YEAR'][0]:04d}{rept['MONTH'][0]:02d}{rept['DAYTXT'][0]:>2}\n")
    f2.write(f"{rept['YEAR'][0]:04d}{rept['MONTH'][0]:02d}{rept['DAYTXT'][0]:>2}\n")

# ======================
# CISLN loans
# ======================
cisln = cisln_loan.filter(pl.col("SECCUST")=="901").select(["ACCTNO","INDORG"])

# ======================
# LNHIST (recent rollovers)
# ======================
lnhist = lnhist_week.filter(
    (pl.col("TRANCODE")==400) & (pl.col("RATECHG")!=0) &
    (pl.col("EFFDATE")>=CDATE) & (pl.col("EFFDATE")<=RDATE)
).select(["ACCTNO","NOTENO","EFFDATE"])

lnhist = lnhist.sort(["ACCTNO","NOTENO","EFFDATE"], descending=[False,False,True])
lnhist = lnhist.unique(subset=["ACCTNO","NOTENO"], keep="first")

# ======================
# FX RATES
# ======================
fxrate = forate_bkp.filter(pl.col("REPTDATE")<=RDATE)
fxrate = fxrate.sort(["CURCODE","REPTDATE"], descending=[False,True])
fxrate = fxrate.unique("CURCODE", keep="first")
fxdict = dict(zip(fxrate["CURCODE"], fxrate["SPOTRATE"]))

# ======================
# LNNOTE (eligible revolving credit)
# ======================
lnnote = lnnote.with_columns([
    pl.when(pl.col("ISSUEDT").is_not_null() & (pl.col("ISSUEDT")!=0))
      .then(pl.col("ISSUEDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%m%d%Y"))
      .otherwise(None).alias("ISSDTE")
])
lnnote = lnnote.with_columns([
    pl.col("LOANTYPE").cast(pl.Utf8).alias("PRODCD")  # in SAS: PUT(LOANTYPE,LNPROD.)
])
lnnote = lnnote.filter(pl.col("PRODCD").is_in(["34190","34690"]))
lnnote = lnnote.with_columns([
    pl.when(pl.col("CURCODE")!="MYR")
      .then(pl.col("CURBAL")*pl.col("CURCODE").replace(fxdict))
      .otherwise(pl.col("CURBAL")).round(2).alias("CURBAL")
])
lnnote = lnnote.select(["ACCTNO","NOTENO","NTBRCH","CURBAL","DELQCD","ISSDTE","PRODCD","CURCODE"])

# ======================
# Join LNNOTE + LNHIST
# ======================
lnnote = lnnote.join(lnhist,on=["ACCTNO","NOTENO"],how="inner")
lnnote = lnnote.filter((pl.col("DELQCD")==" ") & (pl.col("ISSDTE")<SDATE) & (pl.col("CURBAL")!=0))

# ======================
# Write rollover accounts (OUTPUT1)
# ======================
with open(BASE_OUTPUT / "ROLLOVER.txt","a") as f:
    for row in lnnote.join(cisln,on="ACCTNO",how="left").iter_rows(named=True):
        effdate = row["EFFDATE"]
        effYYYY,effMM,effDD = effdate.year,effdate.month,effdate.day
        f.write(f"{int(row['NTBRCH']):03d}{row['ACCTNO']:>10}{row['NOTENO']:>10}"
                f"{row['INDORG'] or ' '}"+
                f"{effYYYY:04d}{effMM:02d}{effDD:02d}{int(row['CURBAL']*100):016d}\n")

# ======================
# OD REVIEW extraction
# ======================
odrev = limit_overdft.filter((pl.col("APPRLIMT")>1)&(pl.col("LMTAMT")>1))\
    .select(["BRANCH","ACCTNO","LMTDESC","LMTID","REVIEWDT","LMTAMT"])

prevod = olimit_overdft.filter((pl.col("APPRLIMT")>1)&(pl.col("REVIEWDT")>0))\
    .select(["ACCTNO","LMTID","REVIEWDT"]).rename({"REVIEWDT":"PREVIEWDT"})

odreview = odrev.join(prevod,on=["ACCTNO","LMTID"],how="inner")
odreview = odreview.with_columns([
    pl.when(pl.col("REVIEWDT")>0)
      .then(pl.col("REVIEWDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date,"%m%d%Y"))
      .otherwise(None).alias("REVIEWDT"),
    pl.when(pl.col("PREVIEWDT")>0)
      .then(pl.col("PREVIEWDT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date,"%m%d%Y"))
      .otherwise(None).alias("PREVIEWDT")
])
odreview = odreview.filter(pl.col("REVIEWDT")>pl.col("PREVIEWDT"))

# Aggregate (PROC SUMMARY)
odlimit = odreview.group_by(["ACCTNO","LMTDESC"]).agg(pl.sum("LMTAMT").alias("LMTAMT"))

odreview = odreview.join(odlimit,on=["ACCTNO","LMTDESC"],how="left").unique(["ACCTNO","LMTDESC"])

# ======================
# Join CISDP
# ======================
cisdp = cisdp_deposit.filter(pl.col("SECCUST")=="901").select(["ACCTNO","INDORG"])
odreview = odreview.join(cisdp,on="ACCTNO",how="left")

# ======================
# Write OD review (OUTPUT2)
# ======================
with open(BASE_OUTPUT / "ODREVIEW.txt","a") as f:
    for row in odreview.iter_rows(named=True):
        appldate = f"{RDATE.year:04d}{RDATE.month:02d}{RDATE.day:02d}"
        revdate  = row["REVIEWDT"].strftime("%Y%m%d") if row["REVIEWDT"] else "00000000"
        prevdate = row["PREVIEWDT"].strftime("%Y%m%d") if row["PREVIEWDT"] else "00000000"
        f.write(f"{int(row['BRANCH']):03d}{row['ACCTNO']:>10}{row['LMTDESC']:<20}"
                f"{row['INDORG'] or ' '}{prevdate}{revdate}{int(row['LMTAMT']*100):017d}"
                f"{appldate}R\n")

print("EIBWCCR7 pipeline completed.")
