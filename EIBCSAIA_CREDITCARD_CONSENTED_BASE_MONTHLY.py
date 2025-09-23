# EIBCSAIA_CREDITCARD_CONSENTED_BASE_MONTHLY.py
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date

# -----------------------------
# Step 1: Load CIS Customer Daily file
# -----------------------------
# In SAS: SET CISFILE.CUSTDLY
# Here: assume CIS file is stored as Parquet or CSV
cis_file = "CIS_CUSTDLY.parquet"  # adjust path if CSV or other format

try:
    cis = pl.read_parquet(cis_file)
except FileNotFoundError:
    raise FileNotFoundError(f"Missing CIS input file: {cis_file}")

# -----------------------------
# Step 2: Filter by conditions
# -----------------------------
# SAS logic:
#   IF ACCTCODE NOT IN ('DP   ','LN   ','EQC  ','FSF  ')
#   IF PRISEC = 901
#   CUSTNO1 = CUSTNO*1
ciscard = (
    cis.filter(
        (~pl.col("ACCTCODE").is_in(["DP   ", "LN   ", "EQC  ", "FSF  "]))
        & (pl.col("PRISEC") == 901)
    )
    .with_columns(pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO1"))
    .select(
        [
            pl.col("CUSTNO1").alias("CUSTNO"),
            "ACCTNOC",
            "ACCTCODE",
            "PRISEC",
            "ALIASKEY",
            "ALIAS",
        ]
    )
)

# -----------------------------
# Step 3: Deduplicate by ACCTNOC
# -----------------------------
ciscard_unique = ciscard.unique(subset=["ACCTNOC"])

# -----------------------------
# Step 4: Save output as Parquet
# -----------------------------
output_file = "AIA_CONSENTED_BASE.parquet"
ciscard_unique.write_parquet(output_file)
print(f"Output written to {output_file}")

# -----------------------------
# Step 5: Make available in DuckDB for downstream queries
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE OR REPLACE TABLE aia_consented_base AS SELECT * FROM read_parquet('{output_file}')")

print("DuckDB table 'aia_consented_base' is ready for queries")
print(duckdb.sql("SELECT COUNT(*) FROM aia_consented_base").fetchall())
