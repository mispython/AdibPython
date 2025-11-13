import polars as pl
import duckdb
from pathlib import Path

# Configuration
cisfile_path = Path("CISFILE")

# Read the source data
custdly_df = pl.read_parquet(cisfile_path / "CUSTDLY.parquet")

# DATA CISCARD - Filter and transform
ciscard_df = (
    custdly_df
    .filter(
        ~pl.col("ACCTCODE").is_in(['DP   ', 'LN   ', 'EQC  ', 'FSF  ']) &
        (pl.col("PRISEC") == 901)
    )
    .with_columns(
        CUSTNO1=pl.col("CUSTNO").cast(pl.Int64)
    )
    .select([
        pl.col("CUSTNO1").alias("CUSTNO"),
        "ACCTNOC",
        "ACCTCODE", 
        "PRISEC",
        "ALIASKEY",
        "ALIAS"
    ])
)

# PROC SORT equivalent - Remove duplicates by ACCTNOC
ciscard_final = ciscard_df.unique(subset=["ACCTNOC"])

# Save the result
ciscard_final.write_parquet("CISCARD.parquet")
