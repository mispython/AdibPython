# eIIDBTRD.py
import polars as pl
import pyarrow as pa
import duckdb
from datetime import date, timedelta

# -----------------------------
# Step 1: Setup Report Date
# -----------------------------
today = date.today()
REPTDATE = today
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
REPTYEAR = today.strftime("%y")
RDATE = today.strftime("%d/%m/%Y")
TDATE = today.strftime("%Y%m%d")
STARTDT = today.replace(day=1)
NOWK = "4"
REPTMON1 = f"{today.month-1 if today.month>1 else 12:02d}"
REPTDT = today.strftime("%y%m%d")
PREVDT = (today - timedelta(days=1)).strftime("%y%m%d")

print(f"Running EIIDBTRD for {RDATE}, REPTDT={REPTDT}, MON={REPTMON}, DAY={REPTDAY}")

# -----------------------------
# Step 2: Load Input Datasets
# -----------------------------
# You would replace these with actual parquet/CSV/DB connectors
IBTDTL = pl.read_parquet(f"BNM_IBTDTL_{REPTDT}.parquet")
IMAST = pl.read_parquet(f"BNM_IMAST_{REPTDT}.parquet")
BTRSA_REPTDATE = pl.read_parquet("BTRSA_REPTDATE.parquet")

# -----------------------------
# Step 3: Merge IBTRADE
# -----------------------------
IBTRADE = IBTDTL.join(IMAST, on="ACCTNO", how="left")

# -----------------------------
# Step 4: Transformations (Sector, Customer, etc.)
# -----------------------------
# Example sector code mapping (from SAS PUT/FORMAT)
def map_sector(custcd, sector):
    if custcd in ["02","03","04","05","06","11","12","13","17","30","31","32","33","34","35","37","38","39","45"]:
        return "8110"
    elif custcd == "40":
        return "8130"
    elif custcd == "79":
        return "9203"
    elif custcd in ["71","72","73","74"]:
        return "9101"
    else:
        return sector or "9999"

IBTRADE = IBTRADE.with_columns([
    pl.col("CUSTCD"),
    pl.col("SECTOR").map_elements(lambda s: str(s).zfill(4)).alias("SECTORCD")
])
IBTRADE = IBTRADE.with_columns(
    IBTRADE.apply(lambda row: map_sector(row["CUSTCD"], row["SECTORCD"]))
            .alias("SECTORCD")
)

# -----------------------------
# Step 5: Aggregate (PROC SUMMARY equivalent)
# -----------------------------
agg_tbl = (
    IBTRADE.group_by(["ACCTNO"])
           .agg([
               pl.sum("OUTSTAND").alias("TOT_OUTSTAND"),
               pl.mean("INTRATE").alias("AVG_INTRATE")
           ])
)

# -----------------------------
# Step 6: Disbursement & Repayment (DISBPAY logic)
# -----------------------------
DISBPAY = pl.read_parquet(f"PRNGL_{REPTDAY}{REPTMON}.parquet")

# Example conditional transformation
DISBPAY = DISBPAY.with_columns([
    pl.when((pl.col("TRANSAMT") > 0) & (~pl.col("GLMNEMO").is_in(["08000","09000"])))
      .then(pl.col("TRANSAMT")).otherwise(0).alias("DISBURSE"),
    pl.when(pl.col("TRANSAMT") <= 0)
      .then(-pl.col("TRANSAMT")).otherwise(0).alias("REPAID")
])

DISBPAY = DISBPAY.group_by(["ACCTNO","TRANSREF"]).agg([
    pl.sum("DISBURSE").alias("DISBURSE"),
    pl.sum("REPAID").alias("REPAID")
])

# -----------------------------
# Step 7: Merge with IBTRADE
# -----------------------------
IBTRADE = IBTRADE.join(DISBPAY, on=["ACCTNO","TRANSREF"], how="left")

# -----------------------------
# Step 8: Save Outputs (Parquet instead of PROC CPORT)
# -----------------------------
output_ibtrade = f"IBTRADE_{REPTDT}.parquet"
output_ibtdtl = f"IBTDTL_{REPTDT}.parquet"

IBTRADE.write_parquet(output_ibtrade)
IBTDTL.write_parquet(output_ibtdtl)

print(f"Outputs written: {output_ibtrade}, {output_ibtdtl}")

# -----------------------------
# Step 9: Optional DuckDB SQL
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE OR REPLACE TABLE ibtrade AS SELECT * FROM read_parquet('{output_ibtrade}')")
print(duckdb.sql("SELECT COUNT(*) FROM ibtrade").fetchall())
