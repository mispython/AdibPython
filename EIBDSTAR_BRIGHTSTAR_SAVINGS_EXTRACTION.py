import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime

# Configuration
REPORT_DATE = "11272024"  # MMDDYYYY format - change this as needed

# File paths
deposit_saving_path = Path("deposit_saving.csv")
cis_custdly_path = Path("cis_custdly.parquet")
output_path = Path("bstar_output.parquet")

# Convert report date to SAS date format
reptdate = datetime.strptime(REPORT_DATE, '%m%d%Y')
reptdte = (reptdate - datetime(1960, 1, 1)).days  # SAS date format

# Read and filter deposit saving data
deposit = pl.read_csv(deposit_saving_path)

bright = (deposit
    .filter(pl.col('PRODUCT') == 208)
    .filter(pl.col('OPENDT') > 0)
    .with_columns([
        pl.col('OPENDT').cast(pl.Utf8).str.zfill(11).alias('OPENDT_STR')
    ])
    .with_columns([
        pl.col('OPENDT_STR').str.slice(2, 2).alias('OPENDD'),
        pl.col('OPENDT_STR').str.slice(0, 2).alias('OPENMM'),
        pl.col('OPENDT_STR').str.slice(4, 4).alias('OPENYY')
    ])
    .with_columns([
        (pl.datetime(
            pl.col('OPENYY').cast(pl.Int32),
            pl.col('OPENMM').cast(pl.Int32),
            pl.col('OPENDD').cast(pl.Int32)
        ) - datetime(1960, 1, 1)).dt.total_days().alias('OPENDT_SAS')
    ])
    .filter(pl.col('OPENDT_SAS') == reptdte)
    .select(['BRANCH', 'ACCTNO', 'OPENDT_SAS', 'CURBAL', 'OPENIND'])
    .rename({'OPENDT_SAS': 'OPENDT'})
    .sort('ACCTNO')
)

# Read and process CIS customer data
cis_raw = pl.read_parquet(cis_custdly_path)

cis = (cis_raw
    .filter(pl.col('CUSTOPENDATE') > 0)
    .with_columns([
        pl.col('CUSTOPENDATE').cast(pl.Utf8).str.zfill(11).alias('CUSTDT_STR')
    ])
    .with_columns([
        pl.col('CUSTDT_STR').str.slice(2, 2).alias('CUSTDD'),
        pl.col('CUSTDT_STR').str.slice(0, 2).alias('CUSTMM'),
        pl.col('CUSTDT_STR').str.slice(4, 4).alias('CUSTYY')
    ])
    .with_columns([
        (pl.datetime(
            pl.col('CUSTYY').cast(pl.Int32),
            pl.col('CUSTMM').cast(pl.Int32),
            pl.col('CUSTDD').cast(pl.Int32)
        ) - datetime(1960, 1, 1)).dt.total_days().alias('CUSTOPDT')
    ])
    .with_columns([
        pl.when(pl.col('PRISEC') == 901)
        .then(pl.lit('N'))
        .otherwise(pl.lit('Y'))
        .alias('JOINT')
    ])
    .select(['ACCTNO', 'CUSTNAME', 'JOINT', 'ALIASKEY', 'ALIAS', 'CUSTOPDT'])
    .sort('ACCTNO')
)

# Merge datasets
merged = bright.join(cis, on='ACCTNO', how='inner')

# Final output with renamed columns
final = (merged
    .rename({
        'ALIASKEY': 'PM_SC',
        'ALIAS': 'NEWIC'
    })
    .select([
        'BRANCH', 'ACCTNO', 'NEWIC', 'JOINT', 'CUSTNAME',
        'OPENDT', 'OPENIND', 'CUSTOPDT', 'CURBAL'
    ])
    .sort('ACCTNO')
)

# Write to parquet
final.write_parquet(output_path)

print(f"Processing complete. Output saved to {output_path}")
print(f"Records processed: {len(final)}")
print("\nSample output:")
print(final.head())
