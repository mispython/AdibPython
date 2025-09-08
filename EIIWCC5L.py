import polars as pl
import datetime as dt
from pathlib import Path

# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
DATA_DIR = Path("data")    # <-- replace with your input folder
OUTPUT_DIR = Path("out")   # <-- replace with your output folder
OUTPUT_DIR.mkdir(exist_ok=True)

# Input files (replace with real .parquet paths)
btrd            = pl.read_parquet(DATA_DIR / "BTRADE.parquet")
bnm_lnacct      = pl.read_parquet(DATA_DIR / "LNACCT.parquet")
reptdate_df     = pl.read_parquet(DATA_DIR / "REPTDATE.parquet")
collater_src    = pl.read_parquet(DATA_DIR / "COLLATER.parquet")
elds            = pl.read_parquet(DATA_DIR / "CCRISP_ELDS.parquet")
overdfs         = pl.read_parquet(DATA_DIR / "CCRISP_OVERDFS.parquet")
loan            = pl.read_parquet(DATA_DIR / "CCRISP_LOAN.parquet")
summ1           = pl.read_parquet(DATA_DIR / "CCRISP_SUMM1.parquet")

# -------------------------------------------------------------------
# Build DATES dataset
# -------------------------------------------------------------------
reptdate = reptdate_df["REPTDATE"].max()
day = reptdate.day
month = reptdate.month
year = reptdate.year

if day == 8:
    sdd, wk, wk1 = 1, "1", "4"
elif day == 15:
    sdd, wk, wk1 = 9, "2", "1"
elif day == 22:
    sdd, wk, wk1 = 16, "3", "2"
else:
    sdd, wk, wk1 = 23, "4", "3"

mm1 = month - 1 if wk == "1" else month
if mm1 == 0:
    mm1 = 12

mm2 = month if wk == "4" else month - 1
if mm2 == 0:
    mm2 = 12

sdate = dt.date(year, month, 1)
cutoff = dt.date(2018, 7, 1)

# -------------------------------------------------------------------
# Join BTRD with LNACCT (BTRL)
# -------------------------------------------------------------------
btrl = (
    btrd.join(bnm_lnacct, on="ACCTNO", how="inner")
    .filter((pl.col("ACCTNO") > 0) & (pl.col("SETTLED") != "S"))
    .with_columns([
        pl.col("FICODE").alias("BRANCH"),
        pl.lit(0).alias("LOANTYPE"),
    ])
)

btrl = btrl.with_columns(
    pl.when(pl.col("RETAILID") == "C").then(999).otherwise(pl.col("LOANTYPE")).alias("LOANTYPE")
)

# -------------------------------------------------------------------
# Clean CCOLLAT
# -------------------------------------------------------------------
ccollat = collater_src.clone()

ccollat = ccollat.with_columns([
    pl.when(~pl.col("COLLATERAL_EFF_DT").is_null() & (pl.col("COLLATERAL_EFF_DT") != 0))
      .then(pl.col("COLLATERAL_EFF_DT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
      .otherwise(None)
      .alias("EFF_DX"),

    pl.when(pl.col("COLLATERAL_START_DT") != "00000000")
      .then(pl.col("COLLATERAL_START_DT").str.strptime(pl.Date, "%d%m%Y"))
      .otherwise(None)
      .alias("START_DX")
])

ccollat = ccollat.filter(~((pl.col("EFF_DX") >= cutoff) & (pl.col("START_DX").is_null())))

# -------------------------------------------------------------------
# Merge CCRISP datasets
# -------------------------------------------------------------------
loan    = loan.join(ccollat.drop(["BRANCH"]), on=["ACCTNO","NOTENO"], how="inner")
overdfs = overdfs.join(ccollat, on="ACCTNO", how="inner")
btrl    = btrl.join(ccollat, on="ACCTNO", how="inner")
elds    = elds.join(ccollat.drop(["BRANCH","AANO","NOTENO"]), on="ACCTNO", how="inner")

collater = (
    pl.concat([loan, overdfs, btrl, elds], how="diagonal_relaxed")
    .with_columns(pl.col("NOTENO").fill_null(0))
    .join(summ1, on="AANO", how="inner")
)

# -------------------------------------------------------------------
# CCLASSC → COLLATER classification
# -------------------------------------------------------------------
def classify_cclassc(df: pl.DataFrame) -> pl.DataFrame:
    mapping = {
        "29": ["001","006","007","014","016","024","112","113","114","115","116","117",
               "025","026","046","048","049","149"],
        "70": ["000","011","012","013","017","018","019","021","027","028","029","030",
               "124","031","105","106"],
        "90": ["002","003","041","042","043","058","059","067","068","069","070","111","123",
               "071","072","078","079","084","107"],
        "30": ["004","005","127","128","129","142","143"],
        "10": ["032","033","034","035","036","037","038","039","040","044","050","051","052",
               "053","054","055","056","057","118","119","121","122","060","061","062"],
        "40": ["065","066","075","076","082","083","093","094","095","096","097","098",
               "131","132","133","134","135","136","137","138","139","141","101","102","103","104"],
        "60": ["063","064","073","074","080","081"],
        "50": ["010","085","086","087","088","089","090","091","092","111","125","126","144"],
        "00": ["009","022","023"],
        "21": ["008"],
        "22": ["045","047"],
        "23": ["015"],
        "80": ["020"],
        "81": ["108","109"],
        "99": ["077"],
    }

    expr = pl.lit(None)
    for val, codes in mapping.items():
        expr = pl.when(pl.col("CCLASSC").is_in(codes)).then(val).otherwise(expr)
    return df.with_columns(expr.alias("COLLATER"))

collater = classify_cclassc(collater)

# -------------------------------------------------------------------
# Split categories
# -------------------------------------------------------------------
cpropety = collater.filter(pl.col("COLLATER") == "10")
cmtorveh = collater.filter(pl.col("COLLATER") == "30")
cothvehi = collater.filter(pl.col("COLLATER") == "40")
cplantma = collater.filter(pl.col("COLLATER") == "60")
cconcess = collater.filter(pl.col("COLLATER") == "50")
cfinasst = collater.filter(pl.col("COLLATER") == "29")
cothasst = collater.filter(pl.col("COLLATER") == "90")
cfinguar = collater.filter(pl.col("COLLATER") == "70")
dccms    = collater.filter(pl.col("COLLATER") == "10")

# -------------------------------------------------------------------
# Property handling (split land, building, house)
# -------------------------------------------------------------------
cpropety = cpropety.with_columns([
    pl.when(pl.col("CCLASSC").is_in(["032","033","034"])).then("LAND")
      .when(pl.col("CCLASSC").is_in(["035","036","037"])).then("BUILDING")
      .when(pl.col("CCLASSC").is_in(["038","039","040"])).then("HOUSE")
      .otherwise("OTHER")
      .alias("PROP_TYPE")
])

# -------------------------------------------------------------------
# Vehicle cleanup
# -------------------------------------------------------------------
cmtorveh = cmtorveh.with_columns([
    pl.when(pl.col("CCLASSC").is_in(["004","005"])).then("MOTOR")
      .otherwise("OTHER")
      .alias("VEH_TYPE")
])

cothvehi = cothvehi.with_columns([
    pl.when(pl.col("CCLASSC").is_in(["065","066"])).then("HEAVY")
      .otherwise("OTHER")
      .alias("VEH_TYPE")
])

# -------------------------------------------------------------------
# Insurance handling
# -------------------------------------------------------------------
insure = collater.filter(pl.col("COLLATER") == "29")
insure2 = collater.filter(pl.col("COLLATER") == "90")

# Example cleanups (replace with SAS INSURE logic)
insure = insure.with_columns([
    pl.when(pl.col("SUMINSURE") < 0).then(0).otherwise(pl.col("SUMINSURE")).alias("SUMINSURE_CLEAN")
])

# -------------------------------------------------------------------
# Write outputs (Parquet + CSV + fixed width)
# -------------------------------------------------------------------
def write_all(df: pl.DataFrame, name: str):
    df.write_parquet(OUTPUT_DIR / f"{name}.parquet")
    df.write_csv(OUTPUT_DIR / f"{name}.csv")
    # also fixed width example
    fw = df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])
    with open(OUTPUT_DIR / f"{name}.txt", "w") as f:
        for row in fw.iter_rows():
            line = "".join(str(v).ljust(12) for v in row)
            f.write(line + "\n")

write_all(cpropety, "CPROPETY")
write_all(cmtorveh, "CMTORVEH")
write_all(cothvehi, "COTHVEHI")
write_all(cplantma, "CPLANTMA")
write_all(cconcess, "CCONCESS")
write_all(cfinasst, "CFINASST")
write_all(cothasst, "COTHASST")
write_all(cfinguar, "CFINGUAR")
write_all(dccms, "DCCMS")
write_all(insure, "INSURE")
write_all(insure2, "INSURE2")

print("✅ All collateral datasets written (parquet + csv + fixed width)")
