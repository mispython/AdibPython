import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# =========================
# 1. CONFIGURATION
# =========================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Input parquet datasets (replace with your actual converted files)
BNM_REPTDATE   = pl.read_parquet(BASE_INPUT / "BNM_REPTDATE.parquet")
CCRISP_ELDS    = pl.read_parquet(BASE_INPUT / "CCRISP_ELDS.parquet")
HPREJ_EHPA123  = pl.read_parquet(BASE_INPUT / "HPREJ_EHPA123.parquet")
BNMD_RVNOTE    = pl.read_parquet(BASE_INPUT / "BNMD_RVNOTE.parquet")
FORATE_BKP     = pl.read_parquet(BASE_INPUT / "FORATE_BKP.parquet")

# =========================
# 2. REPORT DATE LOGIC
# =========================
dates = BNM_REPTDATE.with_columns([
    pl.col("REPTDATE").dt.day().alias("DAY"),
    pl.col("REPTDATE").dt.month().alias("MM"),
    pl.col("REPTDATE").dt.year().alias("YEARS")
])

def week_logic(day: int):
    if day == 8:   return (1, "1", "4")
    if day == 15:  return (9, "2", "1")
    if day == 22:  return (16,"3","2")
    return (23,"4","3")

wkvals = dates["DAY"].map_elements(lambda d: week_logic(d), return_dtype=pl.Struct)
dates = dates.with_columns([
    wkvals.struct.field("field_0").alias("SDD"),
    wkvals.struct.field("field_1").alias("WK"),
    wkvals.struct.field("field_2").alias("WK1"),
])

dates = dates.with_columns([
    pl.datetime(dates["YEARS"], dates["MM"], dates["DAY"]).alias("SDATE"),
    pl.datetime(dates["YEARS"], dates["MM"], 1).alias("IDATE"),
])

IDATE = dates["IDATE"][0]
SDATE = dates["SDATE"][0]
XDATE = IDATE.replace(day=1)  # start of month
RDATE1 = dates["REPTDATE"][0]

# =========================
# 3. LOAN + LNRJ
# =========================
loan = CCRISP_ELDS.with_columns([
    pl.lit("LN").alias("ACTY")
])

# Fix branches & custcode
loan = loan.with_columns([
    pl.when(pl.col("BRANCH")==902).then(904).otherwise(pl.col("BRANCH")).alias("BRANCH"),
    pl.when(pl.col("TRANBRNO").is_not_null() & (pl.col("TRANBRNO")!=0))
      .then(pl.col("TRANBRNO")).otherwise(pl.col("BRANCH")).alias("BRANCH"),
    pl.when(pl.col("SMESIZE").is_not_null() & (pl.col("SMESIZE")!=0))
      .then(pl.col("SMESIZE")).otherwise(pl.col("CUSTCODE")).alias("CUSTCODE")
])

loan_df = loan.filter((pl.col("AADATE")>=IDATE) & (pl.col("AADATE")<=SDATE))
lnrj_df = loan.filter((pl.col("ICDATE")>=XDATE) & (pl.col("ICDATE")<=SDATE))

# =========================
# 4. HPRJ + RVNOTE
# =========================
hprj = HPREJ_EHPA123.filter(
    (pl.col("CCRIS_STAT")=="D") &
    (pl.col("ICDATE")>=XDATE) & (pl.col("ICDATE")<=SDATE)
).select(["AANO","ICDATE","CCRIS_STAT"]).sort("AANO")

rvnote = BNMD_RVNOTE.with_columns([
    pl.col("VINNO").cast(pl.Utf8).str.strip_chars().str.slice(0,13).alias("AANO1")
])
rvnote = rvnote.with_columns([
    rvnote["AANO1"].str.replace_all(r"[@*(#)-]", "").alias("COMP")
])
rvnote = rvnote.with_columns([
    pl.when(rvnote["COMP"].str.len_chars()==13).then(rvnote["COMP"]).otherwise(" ").alias("AANO")
])
rvnote = rvnote.with_columns([
    pl.col("NTBRCH").alias("BRANCH"),
    pl.when(pl.col("COSTCTR")==8044).then(902)
      .when(pl.col("COSTCTR")==8048).then(903)
      .otherwise(pl.col("NTBRCH")).alias("BRANCH"),
    pl.col("LASTTRAN").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%m%d%Y").alias("ICDATE")
])
rvnote = rvnote.filter(pl.col("ICDATE")>=XDATE).select(
    ["AANO","ACCTNO","NOTENO","BRANCH","COSTCTR","ICDATE","REVERSED"]
)

# Outer join HPRJ+RVNOTE
hprj = hprj.join(rvnote, on="AANO", how="outer")

lnhprj = pl.concat([lnrj_df, hprj], how="vertical").with_columns(
    pl.col("NOTENO").fill_null(0)
)

# =========================
# 5. FX RATES
# =========================
fxrate = FORATE_BKP.filter(pl.col("REPTDATE")<=RDATE1)
fxrate = fxrate.sort(["CURCODE","REPTDATE"], descending=[False,True])
fxrate = fxrate.unique(subset="CURCODE", keep="first")
fx_dict = dict(zip(fxrate["CURCODE"], fxrate["SPOTRATE"]))

# =========================
# 6. ACCTCRED logic
# =========================
acctcred = loan_df.with_columns([
    pl.lit(0).alias("OLDBRH"),
    pl.lit(0).alias("ARREARS"),
    pl.lit(0).alias("NODAYS"),
    pl.lit(0).alias("INSTALM"),
    pl.col("AMOUNT").alias("UNDRAWN"),
    pl.lit("O").alias("ACCTSTAT"),
    pl.col("BRANCH").alias("FICODE")
])

# OD accounts (ACTY='OD')
od_mask = acctcred["ACTY"]=="OD"
acctcred = acctcred.with_columns([
    pl.when(od_mask).then(pl.col("OUTBAL")).otherwise(pl.col("UNDRAWN")).alias("UNDRAWN"),
    pl.when(od_mask).then(pl.lit("O")).otherwise(pl.col("ACCTSTAT")).alias("ACCTSTAT")
])

# Non-OD accounts → compute INSTALM
acctcred = acctcred.with_columns([
    pl.when((acctcred["ACTY"]!="OD") & (acctcred["OUTBAL"]>0))
      .then(acctcred["OUTBAL"] / pl.when(acctcred["NTERM"]>0).then(acctcred["NTERM"]).otherwise(1))
      .otherwise(0).alias("INSTALM")
])

# Undrawn for non-OD
acctcred = acctcred.with_columns([
    pl.when((acctcred["ACTY"]!="OD") & (acctcred["AMOUNT"]>0))
      .then(acctcred["AMOUNT"]-acctcred["OUTBAL"])
      .otherwise(pl.col("UNDRAWN")).alias("UNDRAWN")
])

# =========================
# 7. SAVE OUTPUT
# =========================
acctcred.write_parquet(BASE_OUTPUT / "ACCTCRED.parquet")
lnhprj.write_parquet(BASE_OUTPUT / "LNHPRJ.parquet")

print("✅ EIBWCC5L pipeline complete.")
