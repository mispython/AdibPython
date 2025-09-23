# EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime

# -----------------------------
# Step 1: Load DATEFILE to get REPTDATE
# -----------------------------
# In SAS: EXTDATE from DATEFILE, derive REPTDATE
# Here: assume DATEFILE is a one-line text file
with open("DATEFILE.txt") as f:
    extdate = f.readline().strip()

# Convert EXTDATE (e.g., "20240915") into REPTDATE
reptdate = datetime.strptime(extdate[:8], "%m%d%Y")
reptyear = reptdate.strftime("%y")
reptmon = reptdate.strftime("%m")
reptday = reptdate.strftime("%d")
reptdte = reptdate.strftime("%y%m%d")

print(f"REPTDATE = {reptdate}, MON={reptmon}, DAY={reptday}, YEAR={reptyear}")

# -----------------------------
# Step 2: Load deposit saving dataset
# -----------------------------
deposit = pl.read_parquet("DEPOSIT_SAVING.parquet")

bright = (
    deposit.filter(pl.col("PRODUCT") == 208)
    .with_columns(
        pl.col("OPENDT").cast(pl.Int64),
    )
    .filter(pl.col("OPENDT") > 0)
)

# Keep only records where OPENDT = REPTDATE
bright = bright.filter(
    pl.col("OPENDT") == int(reptdte)
).select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])

# -----------------------------
# Step 3: Load CIS file
# -----------------------------
cis = pl.read_parquet("CIS_CUSTDLY.parquet")

cis = (
    cis.with_columns(
        pl.col("CUSTOPENDATE").cast(pl.Int64),
    )
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901).then("N").otherwise("Y").alias("JOINT"),
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

# -----------------------------
# Step 4: Join BRIGHT with CIS
# -----------------------------
new = bright.join(cis, on="ACCTNO", how="inner")

# -----------------------------
# Step 5: Convert columns (simulate SAS "convert" dataset)
# -----------------------------
convert = new.select(
    [
        pl.col("BRANCH").cast(pl.Utf8).alias("BRANCH"),
        pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO"),
        pl.col("ALIAS").alias("NEWIC"),
        pl.col("JOINT"),
        pl.col("CUSTNAME"),
        pl.col("OPENDT").cast(pl.Utf8).alias("OPENDT"),
        pl.col("OPENIND"),
        pl.col("CUSTOPENDATE").cast(pl.Utf8).alias("CUSTOPDT"),
        pl.col("CURBAL").cast(pl.Float64).alias("CURBAL"),
    ]
)

# -----------------------------
# Step 6: Save as Parquet (instead of SAS flat file)
# -----------------------------
output_file = f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 7: Register in DuckDB for SQL queries
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE OR REPLACE TABLE brightstar_savings AS SELECT * FROM read_parquet('{output_file}')")

print("DuckDB table 'brightstar_savings' ready")
print(duckdb.sql("SELECT COUNT(*) FROM brightstar_savings").fetchall())
