# eibdbtrd.py
import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta

# -------------------------------------------------------------------
# Step 1: Setup report date
# -------------------------------------------------------------------
today = date.today()
REPTDAY = f"{today.day:02d}"
REPTMON = f"{today.month:02d}"
REPTYEAR = f"{today.year % 100:02d}"
REPTDT = today.strftime("%y%m%d")
PREVDT = (today - timedelta(days=1)).strftime("%y%m%d")
STARTDT = today.replace(day=1)

print(f"Running EIBDBTRD {REPTDT} (prev={PREVDT})")

# -------------------------------------------------------------------
# Step 2: Load source datasets (replace paths with your Parquet/CSV)
# -------------------------------------------------------------------
btdtl = pl.read_parquet(f"BNM_BTDTL_{REPTDT}.parquet")
mast  = pl.read_parquet(f"BNM_MAST_{REPTDT}.parquet")
btrsa_suba = pl.read_parquet(f"BTRSA_SUBA_{REPTDT}.parquet")
btrsa_mast = pl.read_parquet(f"BTRSA_MAST_{REPTDT}.parquet")
apprlimt   = pl.read_parquet(f"BNM_BTMAST_{REPTDT}.parquet")
pba01      = pl.read_parquet(f"PBA01_{REPTDT}.parquet")
mtd_avg    = pl.read_parquet(f"MTD_BTAVG_{REPTMON}.parquet")

# -------------------------------------------------------------------
# Step 3: Base BTRADE = BTDTL + MAST
# -------------------------------------------------------------------
btrade = btdtl.join(mast.drop("INTRECV"), on="ACCTNO", how="left")

# -------------------------------------------------------------------
# Step 4: Apply sector/customer/product mappings
# (Example, expand with your business rules from SAS)
# -------------------------------------------------------------------
btrade = btrade.with_columns([
    pl.when(pl.col("LIABCODE") != "")
      .then(pl.when(pl.col("DIRCTIND")=="D")
              .then(pl.col("LIABCODE"))  # replace with lookup mapping
              .otherwise(pl.col("LIABCODE")))
      .otherwise(pl.lit(""))
      .alias("PRODCD"),
    pl.lit(today).alias("REPTDATE"),
])

# TODO: replicate all SECTORCD / CUSTCD mappings from SAS with .when/.then

# -------------------------------------------------------------------
# Step 5: Merge with SUBA + MAST for SUBNEW
# -------------------------------------------------------------------
suba = btrsa_suba.filter(pl.col("SINDICAT")!="S")
mast2 = btrsa_mast.select(["ACCTNOX","TFID"])
subnew = suba.join(mast2, on="ACCTNOX", how="left")
subnew = subnew.with_columns([
    pl.col("TRANSREF").str.slice(0,7).alias("TRANSREF"),
])

# -------------------------------------------------------------------
# Step 6: Merge with APPRLIMT (limit enrichment)
# -------------------------------------------------------------------
apprlimt = apprlimt.filter(pl.col("LEVELS")==1).select(["ACCTNO","APPRLIMT","ISSDTE"])
btrade = btrade.join(apprlimt.rename({"ISSDTE":"ISSDTE2"}), on="ACCTNO", how="left")

# -------------------------------------------------------------------
# Step 7: Merge with PBA (unearned)
# -------------------------------------------------------------------
pba = pba01.with_columns([
    pl.col("TRANSREF").str.slice(1,7).alias("TRANSREF")
]).select(["TRANSREF","UNEARNED"])

unearned = pba.group_by("TRANSREF").agg(pl.sum("UNEARNED").alias("UNEARNED"))
btrade = btrade.join(unearned, on="TRANSREF", how="left")

# -------------------------------------------------------------------
# Step 8: Split TB / BA / APNBT
# -------------------------------------------------------------------
tb   = btdtl.filter((pl.col("OUTSTAND")>0) & (~pl.col("LIABCODE").is_in(["BAI","BAP","BAS","BAE"])))
ba   = btdtl.filter((pl.col("OUTSTAND")>0) & (pl.col("LIABCODE").is_in(["BAI","BAP","BAS","BAE"])))
apnbt= btdtl.filter(pl.col("OUTSTAND")<=0)

# Later join logic: tbba = concat([tb, ba, apnbt])

# -------------------------------------------------------------------
# Step 9: Compute disbursements/repayments
# -------------------------------------------------------------------
prngl = pl.read_parquet(f"BTRSA_COMM_{REPTDAY}{REPTMON}.parquet")

disburse = prngl.filter((pl.col("GLMNEMO").is_in(["08000","09000"])) &
                        (pl.col("ISSDTE")>=STARTDT) &
                        (pl.col("ISSDTE")<=today))

repaid = prngl.filter((pl.col("GLMNEMO").is_in(["01010","01140"])) &
                      (pl.col("ISSDTE")>=STARTDT) &
                      (pl.col("ISSDTE")<=today) &
                      (pl.col("TRANSAMT")<0))

repaid = repaid.group_by(["ACCTNO","TRANSREF"]).agg(pl.sum("TRANSAMT").alias("REPAID"))

# TODO: merge disburse + repaid like SAS DISBPAY logic

# -------------------------------------------------------------------
# Step 10: Merge with MTD and Previous Month
# -------------------------------------------------------------------
btrade = btrade.join(mtd_avg, on=["ACCTNO","TRANSREX"], how="left")
prevbtd = pl.read_parquet(f"BNMX_D_BTRAD_{PREVDT}.parquet").rename({"TRANSREF_PBA":"TRANSPBA_PREV"})
btrade = btrade.join(prevbtd, on=["ACCTNO","TRANSREX"], how="left")

# -------------------------------------------------------------------
# Step 11: Final BTRAD + BTDTL datasets
# -------------------------------------------------------------------
btrad = btrade  # after all remapping and merging
btdtl_out = btdtl  # with BT variable drops applied

# -------------------------------------------------------------------
# Step 12: Save as Parquet
# -------------------------------------------------------------------
out_btrad = f"BTRAD_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
out_btdtl = f"BTDTL_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

btrad.write_parquet(out_btrad)
btdtl_out.write_parquet(out_btdtl)

print(f"Written {out_btrad}, {out_btdtl}")

# -------------------------------------------------------------------
# Step 13: Register in DuckDB
# -------------------------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE TABLE btrad AS SELECT * FROM read_parquet('{out_btrad}')")
duckdb.sql(f"CREATE TABLE btdtl AS SELECT * FROM read_parquet('{out_btdtl}')")
print("DuckDB tables available")
