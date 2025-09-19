# eiidbtex.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date

# -------------------------------------------------------------------
# Step 1: Report date setup (equivalent to SAS DATA REPTDATE)
# -------------------------------------------------------------------
today = date.today()
REPTYEAR = today.strftime("%y")
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
RDATE = today.strftime("%Y-%m-%d")

# SAS logic for week/day bucket
day = today.day
if day == 8:
    WK, WK1, SDD = "1", "4", 1
elif day == 15:
    WK, WK1, SDD = "2", "1", 9
elif day == 22:
    WK, WK1, SDD = "3", "2", 16
else:
    WK, WK1, SDD = "4", "3", 23

NOWK, NOWK1, NOWKS = WK, WK1, "4"

print(f"EIIDBTEX Run Date={RDATE}  WK={NOWK}  MON={REPTMON}  DAY={REPTDAY}")

# -------------------------------------------------------------------
# Step 2: Load source datasets (Parquet assumed)
# -------------------------------------------------------------------
# Replace with your paths
btrsa_file = f"BTRSA_ISUBA_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
cred_file  = f"BTRSA_ICRED_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
mast_file  = f"BTRSA_IMAST_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
prov_file  = f"BTRSA_IPROV_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
tfdpd_file = "TFDPD.parquet"

isuba = pl.read_parquet(btrsa_file)
cred  = pl.read_parquet(cred_file)
mast  = pl.read_parquet(mast_file)
prov  = pl.read_parquet(prov_file)
tfdpd = pl.read_parquet(tfdpd_file)

# -------------------------------------------------------------------
# Step 3: ORIGINAL and CROP datasets (renaming BTREL)
# -------------------------------------------------------------------
original = isuba.filter(pl.col("TRANSREF") != "").rename({"BTREL": "BTREL1"})
crop     = isuba.filter(pl.col("TRANSREF") != "").rename({"BTREL": "BTREL2"})

# join chain (simulate PROC SQL left joins)
new  = original.join(crop, on=["TRANSREF","SUBACCT"], how="left")
crop2 = crop.rename({"BTREL2": "BTREL3"})
new2 = new.join(crop2, on=["TRANSREF","SUBACCT"], how="left")
crop3 = crop2.rename({"BTREL3": "BTREL4"})
new3 = new2.join(crop3, on=["TRANSREF","SUBACCT"], how="left")

# -------------------------------------------------------------------
# Step 4: Add NUM flags and NOLEVEL
# -------------------------------------------------------------------
levels = new3.with_columns([
    pl.when(pl.col("SUBACCT")!="").then(1).otherwise(0).alias("NUM1"),
    pl.when(pl.col("BTREL1")!="").then(1).otherwise(0).alias("NUM2"),
    pl.when(pl.col("BTREL2")!="").then(1).otherwise(0).alias("NUM3"),
    pl.when(pl.col("BTREL3")!="").then(1).otherwise(0).alias("NUM4"),
    pl.when(pl.col("BTREL4")!="").then(1).otherwise(0).alias("NUM5"),
])
levels = levels.with_columns((pl.col("NUM1")+pl.col("NUM2")+pl.col("NUM3")+pl.col("NUM4")+pl.col("NUM5")).alias("NOLEVEL"))

# Rank by TRANSREF, descending NOLEVEL
duckdb.register("levels", levels.to_arrow())
level2 = duckdb.sql("""
    SELECT *, 
           RANK() OVER (PARTITION BY TRANSREF ORDER BY NOLEVEL DESC) as RANK2
    FROM levels
""").pl()
level2 = level2.with_columns(pl.col("RANK2").cast(int).alias("RANKING"))

# Keep only RANKING=1
unique = level2.filter(pl.col("RANKING")==1)

# Merge back unique
btrsa = level2.join(unique, on=["TRANSREF","SUBACCT"], how="inner")

# -------------------------------------------------------------------
# Step 5: Merge with CRED
# -------------------------------------------------------------------
cred = cred.rename({"MATUREDX":"MATURED"}).with_columns(pl.col("MATURED").cast(int).alias("MATUREDX"))
next_ = btrsa.join(cred, on=["TRANSREF","SUBACCT"], how="full")

# -------------------------------------------------------------------
# Step 6: Merge with MAST
# -------------------------------------------------------------------
next2 = next_.join(mast, left_on="ACCTNOX", right_on="ACCTNO2", how="full")
next2 = next2.with_columns(pl.col("ACCTNO2").cast(pl.Int64).alias("ACCTNO4"))

# -------------------------------------------------------------------
# Step 7: Merge with PROV
# -------------------------------------------------------------------
next3 = next2.join(prov, on=["TRANSREX","SUBACCT"], how="full")

# Dedup by TRANSREF, ACCTNOX, SUBACCT, OUTSTAND
next3 = next3.unique(subset=["TRANSREF","ACCTNOX","SUBACCT","OUTSTAND"])

# -------------------------------------------------------------------
# Step 8: Final IBTDTL dataset with calculations
# -------------------------------------------------------------------
ibt = next3.with_columns([
    pl.lit(date.today()).alias("REPTDAT1"),
    pl.when(pl.col("APPRLIM2").is_null()).then(0).otherwise(pl.col("APPRLIM2")).alias("APPRLIM2"),
    pl.when(pl.col("INTRATE").is_null()).then(0).otherwise(pl.col("INTRATE")).alias("INTRATE"),
    pl.when(pl.col("SPREAD").is_null()).then(0).otherwise(pl.col("SPREAD")).alias("SPREAD"),
])

# Simple IRIA logic example (SAS has nested IF/ELSE, here simplified)
ibt = ibt.with_columns([
    pl.when((pl.col("PRINAMT")==0) | (pl.col("DISCNTB")==0))
      .then(0)
      .otherwise(pl.col("INTYTD") - pl.col("DISCNTB"))
      .alias("IRIA")
])
ibt = ibt.with_columns((pl.col("OUTSTAND")+pl.col("IRIA")).alias("OUTSTAND"))

# Rename like SAS final
ibt = ibt.rename({"ACCTNOX":"AA","ACCTNO4":"BB","TRANSREF":"TX","TRANSREX":"TF"})

# -------------------------------------------------------------------
# Step 9: Merge with TFDPD (delinquency file)
# -------------------------------------------------------------------
ibtdup = ibt.with_row_count("COUNT")  # fake duplicate marker
final = ibtdup.join(tfdpd, on=["ACCTNO","TRANSREF"], how="left")

# Keep conditions like SAS (COUNT null OR (APPRLIMT!=0 & OUTSTAND!=0))
final = final.filter((pl.col("COUNT").is_null()) | ((pl.col("APPRLIMT")!=0) & (pl.col("OUTSTAND")!=0)))

# -------------------------------------------------------------------
# Step 10: Save output
# -------------------------------------------------------------------
out_file = f"IBTDTL_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
final.write_parquet(out_file)
print(f"Final IBTDTL written to {out_file}")

# Register in DuckDB for SQL analysis
duckdb.register("ibt", final.to_arrow())
print(duckdb.sql("SELECT COUNT(*) AS rows, SUM(OUTSTAND) AS total_outstand FROM ibt").df())
