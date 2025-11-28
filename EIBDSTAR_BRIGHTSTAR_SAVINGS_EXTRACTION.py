from pathlib import Path
import duckdb
import polars as pl
from datetime import datetime, timedelta

# Define paths
SAVING_PATH = Path("/path/to/your/SAVING.csv")  # Update this path
CIS_PATH = Path("/path/to/your/CIS_CUSTDLY.parquet")  # Update this path
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Date setup
REPTDATE = datetime.today() - timedelta(days=1)
reptyear = REPTDATE.strftime("%y")
reptmon = REPTDATE.strftime("%m")  
reptday = REPTDATE.strftime("%d")
reptdte = REPTDATE.strftime("%y%m%d")

# Load datasets with proper schema handling
saving = pl.read_csv(
    SAVING_PATH,
    infer_schema_length=10000,
    schema_overrides={
        'PRODUCT': pl.Utf8,
        'OPENDT': pl.Utf8,
        'CURBAL': pl.Float64,
        'ACCTNO': pl.Utf8,
        'BRANCH': pl.Utf8,
        'BONUSANO': pl.Float64,  # Handle scientific notation
        'ODXSAMT': pl.Float64,   # Handle scientific notation
    },
    null_values=["", "NA", "N/A", "null", "NULL"]
)

cis = pl.read_parquet(CIS_PATH)

# Process BRIGHT data - savings accounts opened on reporting date
bright = (
    saving
    .filter(pl.col("PRODUCT") == "208")
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

# Process CIS data with joint account logic
cis_processed = (
    cis
    .with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901)
        .then(pl.lit("N"))
        .otherwise(pl.lit("Y"))
        .alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

# Merge datasets
new = bright.join(cis_processed, on="ACCTNO", how="inner")

# Format final output
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

# Save output
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

# Load to DuckDB
duckdb.sql("LOAD parquet;")
output_file_str = str(output_file)
duckdb.sql(f"""
    CREATE OR REPLACE TABLE brightstar_savings 
    AS SELECT * FROM read_parquet('{output_file_str}')
""")

print(f"Processing completed. Output: {output_file}")
print(f"Records processed: {len(convert)}")
