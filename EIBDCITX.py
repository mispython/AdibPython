# eibdcitx.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date, datetime

# -------------------------------------------------------------------
# Step 1: Reporting Date Setup
# -------------------------------------------------------------------
today = date.today()
REPTDAY = f"{today.day:02d}"
REPTMON = f"{today.month:02d}"
REPTYEAR = f"{today.year % 100:02d}"
RDATE = today.strftime("%d/%m/%Y")

day = today.day
if 1 <= day <= 8:
    WK = "1"
elif 9 <= day <= 15:
    WK = "2"
elif 16 <= day <= 22:
    WK = "3"
else:
    WK = "4"

print(f"Running EIBDCITX for {RDATE} (WK={WK})")

# -------------------------------------------------------------------
# Step 2: Load raw datasets (replace with actual paths)
# -------------------------------------------------------------------
dpfl = pl.read_csv("DPFL.csv")   # DCI Daily raw
eqfl = pl.read_csv("EQFL.csv", separator="|")
cra  = pl.read_csv("CRA.csv")
eqrt = pl.read_csv("EQRATE.csv")
mnitb_saving = pl.read_csv("MNITB_SAVING.csv")
mnitb_current = pl.read_csv("MNITB_CURRENT.csv")

# -------------------------------------------------------------------
# Step 3: DPST dataset
# -------------------------------------------------------------------
dpst = (
    dpfl
    .rename({"NEWIC":"ICNO"})  # rename if needed
    .with_columns([
        pl.col("ACCINT").cast(pl.Float64)
    ])
)

# Simulate join with DCID
dcid = pl.read_csv("DCID.csv").select(["TICKETNO","CUSTCODE"])
dpst = dpst.join(dcid, on="TICKETNO", how="left")

# -------------------------------------------------------------------
# Step 4: EQC / EQI split
# -------------------------------------------------------------------
eq = eqfl.with_columns([
    pl.col("ACCINTRM").abs(),
    pl.col("ACCINTAMT").abs(),
    pl.col("TOTINTAMT").abs(),
    pl.col("PREMPAID").abs(),
    pl.col("PREMREC").abs()
])

eq = eq.filter((pl.col("STARTDT") <= str(today)) & (pl.col("MATDT") >= str(today)))

eqc = eq.filter(pl.col("TYPE")=="C")
eqi = eq.filter(pl.col("TYPE")!="C")

# -------------------------------------------------------------------
# Step 5: Customer leg (EQC join DPST, CRA, DEPO)
# -------------------------------------------------------------------
eqdci = dpst.join(eqc, on="TICKETNO", how="inner")
eqdci = eqdci.filter(pl.col("CUSTCODE") >= 80)

# CRA feed
dp_cra = cra.filter(pl.col("INV_STATUS").is_in(["ACT","CEP","CEU","CCU","CMU"]))
dp_cra = dp_cra.with_columns([
    pl.lit("Outstanding").alias("STATUSIND"),
    pl.lit("MYR").alias("INVCURR"),
    pl.lit(0).alias("PREMPAID")
])

# Merge deposits
depo = (
    pl.concat([mnitb_saving, mnitb_current])
    .rename({"ACCTNO":"INVCURAC"})
)
dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner").filter(pl.col("CUSTCODE")>=80)

# Append
eqdci = pl.concat([eqdci, dp_cra])

# FX enrichment
eqdci = eqdci.join(eqrt.rename({"CURRENCY":"INVCURR","SPOTRATE":"SPOTRT"}),
                   on="INVCURR", how="left")

eqdci = eqdci.with_columns([
    (pl.col("ACCINT").round(0 if pl.col("INVCURR")=="JPY" else 2)).alias("ACCINTX"),
    (pl.col("PREMPAID").round(0 if pl.col("INVCURR")=="JPY" else 2)).alias("PREMPAI"),
])

eqdci = eqdci.with_columns([
    (pl.col("ACCINTX")*pl.col("SPOTRT")).alias("ACCINTRM"),
    (pl.col("PREMPAI")*pl.col("SPOTRT")).alias("PREMPAIDRM")
])

cusmyr = eqdci.filter(pl.col("INVCURR")=="MYR")
cusfcy = eqdci.filter(pl.col("INVCURR")!="MYR")

# -------------------------------------------------------------------
# Step 6: Interbank leg
# -------------------------------------------------------------------
eqdci_ib = eqi.filter(pl.col("FISSCODE") >= "80")
eqdci_ib = eqdci_ib.join(eqrt.rename({"CURRENCY":"INVCURR","SPOTRATE":"SPOTRT"}),
                         on="INVCURR", how="left")
eqdci_ib = eqdci_ib.with_columns([
    (pl.col("PREMREC").round(0 if pl.col("INVCURR")=="JPY" else 2)).alias("PREMREX"),
    (pl.col("PREMREX")*pl.col("SPOTRT")).alias("PREMRECRM")
])
ibnmyr = eqdci_ib.filter(pl.col("INVCURR")=="MYR")
ibnfcy = eqdci_ib.filter(pl.col("INVCURR")!="MYR")

# -------------------------------------------------------------------
# Step 7: Build DCI
# -------------------------------------------------------------------
dcimyr = (
    pl.concat([cusmyr, ibnmyr])
    .with_columns([
        pl.when(pl.col("TYPE")=="C").then(pl.col("PREMPAID")).otherwise(pl.col("PREMREC")).alias("PREMIUM"),
        pl.lit(today).alias("REPTDATS")
    ])
)

# Derive ELDAY
def elday(d):
    dd = d.day
    mm = d.month
    yy = d.year
    if dd in (1,9,16,23): return "DAYA"
    if dd in (2,10,17,24): return "DAYB"
    if dd in (3,11,18,25): return "DAYC"
    if dd in (4,12,19,26): return "DAYD"
    if dd in (5,13,20,27): return "DAYE"
    if dd in (6,14,21,28): return "DAYF"
    if dd in (7,29): return "DAYG"
    if dd == 30: return "DAYH"
    if dd in (8,15,22,31): return "DAYI"
    return "DAYX"

dcimyr = dcimyr.with_columns(pl.col("REPTDATS").apply(elday).alias("ELDAY"))

# Map to BNM codes
records = []
for row in dcimyr.iter_rows(named=True):
    if row.get("ACCINTRM",0) not in (None,0):
        records.append({"BNMCODE":"4911095000000Y","ELDAY":row["ELDAY"],
                        "REPTDATS":row["REPTDATS"],"AMOUNT":row["ACCINTRM"]})
    if row.get("PREMIUM",0) not in (None,0):
        records.append({"BNMCODE":"4929996000000Y","ELDAY":row["ELDAY"],
                        "REPTDATS":row["REPTDATS"],"AMOUNT":row["PREMIUM"]})

dci_final = pl.DataFrame(records)

# Aggregate
dci_final = dci_final.group_by(["BNMCODE","ELDAY","REPTDATS"]).agg(
    pl.sum("AMOUNT").alias("AMOUNT")
)

# -------------------------------------------------------------------
# Step 8: Write outputs (Parquet)
# -------------------------------------------------------------------
out_file = f"DCI_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
dci_final.write_parquet(out_file)
print(f"Final DCI Parquet written: {out_file}")

# -------------------------------------------------------------------
# Step 9: Register in DuckDB for analytics
# -------------------------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE TABLE dci AS SELECT * FROM read_parquet('{out_file}')")
print("DuckDB table created. Rowcount:", duckdb.sql("SELECT COUNT(*) FROM dci").fetchall())
