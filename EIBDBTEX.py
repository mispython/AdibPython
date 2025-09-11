import polars as pl
import duckdb
import datetime as dt

# -----------------------------
# CONFIG
# -----------------------------
btrsa_path = "/sasdata/SAP.PBB.BTRADE.SASDATA.DAILY.parquet"
bt_path    = "/sasdata/SAP.BT.SASDATA.parquet"
forate_fmt = "/sasdata/SAP.PBB.FCYCA.parquet"
tfdpd_path = "/sasdata/RBP2.B033.LN.TFS.CBDPD.parquet"
out_file   = "/sasdata/SAP.BT.SASDATA.DAILY_OUT.parquet"

# -----------------------------
# STEP 1: Report Date + Weekly Logic
# -----------------------------
reptdate = dt.date.today()
day = reptdate.day

if day == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MM = reptdate.month
MM2 = MM - 1 if MM > 1 else 12
MM1 = MM - 1 if WK == "1" and MM > 1 else (MM if WK != "1" else 12)

SDATE = dt.date(reptdate.year, reptdate.month, 1)
RDATEX = f"{reptdate.month:02d}{reptdate.year%100:02d}"

print("REPTDATE:", reptdate, "WK:", WK, "WK1:", WK1, "RDATEX:", RDATEX)

# -----------------------------
# STEP 2: Load source tables
# -----------------------------
btrsa = pl.read_parquet(btrsa_path)
bt    = pl.read_parquet(bt_path)
tfdpd = pl.read_parquet(tfdpd_path)

# assume BTRSA.SUBA, CRED, MAST, PROV subsets exist in partitions/files
suba = btrsa.filter(pl.col("TYPE")=="SUBA")
cred = btrsa.filter(pl.col("TYPE")=="CRED")
mast = btrsa.filter(pl.col("TYPE")=="MAST")
prov = btrsa.filter(pl.col("TYPE")=="PROV")

# -----------------------------
# STEP 3: Build ORIGINAL & CROP
# -----------------------------
original = suba.filter(pl.col("TRANSREF")!="").rename({"BTREL":"BTREL1"})
crop     = suba.filter(pl.col("TRANSREF")!="").rename({"BTREL":"BTREL2"})

# -----------------------------
# STEP 4: Layered joins NEW → NEW2 → NEW3
# -----------------------------
new  = original.join(crop, on="TRANSREF", how="left", suffix="_c")
new2 = new.join(crop.rename({"BTREL2":"BTREL3"}), on="TRANSREF", how="left", suffix="_c2")
new3 = new2.join(crop.rename({"BTREL3":"BTREL4"}), on="TRANSREF", how="left", suffix="_c3")

# -----------------------------
# STEP 5: Build LEVELS + Ranks
# -----------------------------
levels = new3.with_columns([
    (pl.col("SUBACCT")!="").cast(pl.Int8).alias("NUM1"),
    (pl.col("BTREL1")!="").cast(pl.Int8).alias("NUM2"),
    (pl.col("BTREL2")!="").cast(pl.Int8).alias("NUM3"),
    (pl.col("BTREL3")!="").cast(pl.Int8).alias("NUM4"),
    (pl.col("BTREL4")!="").cast(pl.Int8).alias("NUM5")
])
level  = levels.with_columns((pl.col("NUM1")+pl.col("NUM2")+pl.col("NUM3")+pl.col("NUM4")+pl.col("NUM5")).alias("NOLEVEL"))
level2 = level.with_columns(pl.col("NOLEVEL").rank("dense", descending=True).alias("RANK2"))
unique = level2.filter(pl.col("RANK2")==1)

btrsa_final = level2.join(unique, on=["TRANSREF","SUBACCT"], how="inner")

# -----------------------------
# STEP 6: Join with CRED
# -----------------------------
cred = cred.rename({"MATUREDX":"MATURED"}).with_columns(pl.col("MATURED").cast(pl.Int32))
next_df = btrsa_final.join(cred, on=["TRANSREF","SUBACCT"], how="full")

# -----------------------------
# STEP 7: Join with MAST
# -----------------------------
mast = mast.rename({"ACCTNO2":"ACCTNOX"})
next2 = next_df.join(mast, left_on="ACCTNOX", right_on="ACCTNOX", how="full")

# -----------------------------
# STEP 8: Join with PROV
# -----------------------------
next3 = next2.join(prov, on=["TRANSREX","SUBACCT"], how="full")

# -----------------------------
# STEP 9: Compare from BT.BTRAD (prior month snapshot)
# -----------------------------
compare = bt.filter(pl.col("TYPE")=="BTRAD").select(["TRANSREX","IRIA"]).rename({"TRANSREX":"TRANSREF"})
next3   = next3.join(compare, on="TRANSREF", how="left")

# -----------------------------
# STEP 10: Build BTDTL
# -----------------------------
btdtl = next3.with_columns([
    pl.lit(dt.date.today()).alias("REPTDAT1"),
    (pl.col("APPRLIM2").fill_null(0)).alias("APPRLIMT"),
    (pl.col("INTRATE").fill_null(0)).alias("INTRATE"),
    (pl.col("SPREAD").fill_null(0)).alias("SPREAD"),
    (pl.col("PRINAMT").fill_null(0)).alias("PRINAMT")
])

# Example IRIA calc logic
btdtl = btdtl.with_columns([
    pl.when((pl.col("PRINAMT")>0) & (~pl.col("LIABCODE").is_in(["MCF","MFL","MOF"])))
      .then(pl.col("INTYTD") - pl.col("DISCNTB"))
      .otherwise(pl.col("IRIA"))
      .alias("IRIA")
])

# OUTSTAND update
btdtl = btdtl.with_columns((pl.col("OUTSTAND")+pl.col("IRIA")).alias("OUTSTAND"))

# -----------------------------
# STEP 11: Join TFDPD Delinquency
# -----------------------------
final = btdtl.join(tfdpd, on=["ACCTNO","TRANSREF"], how="left")

# FX adjust if LIABCODE ∈ ('FTI','FTL')
final = final.with_columns(
    pl.when(pl.col("LIABCODE").is_in(["FTI","FTL"]))
      .then((pl.col("PRINAMT")+pl.col("INTAMT"))*pl.col("FORATE"))
      .otherwise(pl.col("OUTSTAND"))
      .alias("OUTSTAND")
)

# -----------------------------
# STEP 12: Save Output
# -----------------------------
final.write_parquet(out_file)
final.write_csv(out_file.replace(".parquet",".csv"))

print("✅ EIBDBTEX completed →", out_file)
