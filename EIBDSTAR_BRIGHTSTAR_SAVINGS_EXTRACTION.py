from pathlib import Path
import duckdb
import polars as pl
from datetime import datetime

# Define paths clearly
DATA_DIR = Path("data")
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)  # Ensure output directory exists

# -----------------------------
# Step 1: Load DATEFILE using pathlib
# -----------------------------
datefile_path = INPUT_DIR / "DATEFILE.txt"
with open(datefile_path) as f:
    extdate = f.readline().strip()

# Convert EXTDATE
reptdate = datetime.strptime(extdate[:8], "%m%d%Y")
reptyear = reptdate.strftime("%y")
reptmon = reptdate.strftime("%m")
reptday = reptdate.strftime("%d")
reptdte = reptdate.strftime("%y%m%d")

print(f"REPTDATE = {reptdate}, MON={reptmon}, DAY={reptday}, YEAR={reptyear}")

# -----------------------------
# Step 2: Load datasets with clear paths
# -----------------------------
deposit_path = DATA_DIR / "DEPOSIT_SAVING.parquet"
cis_path = DATA_DIR / "CIS_CUSTDLY.parquet"

# Check if files exist before processing
if not deposit_path.exists():
    raise FileNotFoundError(f"Deposit file not found: {deposit_path}")
if not cis_path.exists():
    raise FileNotFoundError(f"CIS file not found: {cis_path}")

deposit = pl.read_parquet(deposit_path)
cis = pl.read_parquet(cis_path)

# -----------------------------
# Steps 3-5: Your processing logic (same as before)
# -----------------------------
bright = (
    deposit.filter(pl.col("PRODUCT") == 208)
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

cis_processed = (
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901).then("N").otherwise("Y").alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

new = bright.join(cis_processed, on="ACCTNO", how="inner")

convert = new.select([
    pl.col("BRANCH").cast(pl.Utf8).alias("BRANCH"),
    pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO"),
    pl.col("ALIAS").alias("NEWIC"),
    pl.col("JOINT"),
    pl.col("CUSTNAME"),
    pl.col("OPENDT").cast(pl.Utf8).alias("OPENDT"),
    pl.col("OPENIND"),
    pl.col("CUSTOPENDATE").cast(pl.Utf8).alias("CUSTOPDT"),
    pl.col("CURBAL").cast(pl.Float64).alias("CURBAL"),
])

# -----------------------------
# Step 6: Save with pathlib - much cleaner
# -----------------------------
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 7: DuckDB with pathlib path conversion
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")

# Convert Path object to string for DuckDB
output_file_str = str(output_file)
duckdb.sql(f"""
    CREATE OR REPLACE TABLE brightstar_savings 
    AS SELECT * FROM read_parquet('{output_file_str}')
""")

print("DuckDB table 'brightstar_savings' ready")
print(duckdb.sql("SELECT COUNT(*) FROM brightstar_savings").fetchall())
