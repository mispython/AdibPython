import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# Use yesterday's date for processing
TODAY = datetime.today()
REPTDATE = TODAY - timedelta(days=1)  # Process yesterday's data
REPTMON = f"{REPTDATE.month:02d}"
REPTYEAR = str(REPTDATE.year)[-2:]

# Paths
BASE_PATH = Path("/host/cis/parquet")
OUTPUT_PATH = Path("/pythonITD/mis_dev/sas_migration/CIGR/output")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Today: {TODAY.date()}")
print(f"Processing date: {REPTDATE.date()} (yesterday)")
print(f"Report month: {REPTMON}, Report year: {REPTYEAR}")

# Construct file paths
CIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_OUT.parquet"
RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"

print(f"\nCIS file: {CIS_FILE}")
print(f"RCIS file: {RCIS_FILE}")

# Read and process ICISBASEL - EXACT SAME as conventional version
print(f"\nReading ICISBASEL data from: {CIS_FILE}")
icisbasel_df = pl.read_parquet(CIS_FILE)

# Convert data types - EXACT SAME LOGIC
icisbasel_df = icisbasel_df.with_columns([
    # BALAMT: string to float, dot '.' becomes null
    pl.when(pl.col("BALAMT") == ".")
        .then(None)
        .otherwise(pl.col("BALAMT").cast(pl.Float64, strict=False))
        .alias("BALAMT"),
    # TOTAMT: might already be float, use strict=False to handle either
    pl.col("TOTAMT").cast(pl.Float64, strict=False),
    # Convert integer columns
    pl.col("ACCTNO").cast(pl.Int64, strict=False),
    pl.col("NOTENO").cast(pl.Int64, strict=False),
])

# Select and order columns
icisbasel_df = icisbasel_df.select([
    pl.col("GRPING"),
    pl.col("GROUPNO"),
    pl.col("CUSTNO"),
    pl.col("FULLNAME"),
    pl.col("ACCTNO"),
    pl.col("NOTENO"),
    pl.col("PRODUCT"),
    pl.col("AMTINDC"),
    pl.col("BALAMT"),
    pl.col("TOTAMT"),
    pl.col("RLENCODE"),
    pl.col("PRIMSEC")
])

# Save output - DIFFERENT NAMES ONLY
output_basename = f"ILOAN_ICISBASEL{REPTMON}{REPTYEAR}"
icisbasel_df.write_parquet(OUTPUT_PATH / f"{output_basename}.parquet")
icisbasel_df.write_csv(OUTPUT_PATH / f"{output_basename}.csv")

print(f"\nICISBASEL saved to {output_basename}")
print(f"Shape: {icisbasel_df.shape}")

# Read and process ICISRPTCC - EXCEPT GRPING is numeric in SAS
print(f"\nReading ICISRPTCC data from: {RCIS_FILE}")
icisrptcc_df = pl.read_parquet(RCIS_FILE)

# Convert GRPING to integer (SAS has GRPING 11. not $11.)
icisrptcc_df = icisrptcc_df.with_columns([
    pl.col("GRPING").cast(pl.Int64, strict=False)
])

# Select and order columns
icisrptcc_df = icisrptcc_df.select([
    pl.col("GRPING"),
    pl.col("C1CUST"),
    pl.col("C1TYPE"),
    pl.col("C1CODE"),
    pl.col("C1DESC"),
    pl.col("C2CUST"),
    pl.col("C2TYPE"),
    pl.col("C2CODE"),
    pl.col("C2DESC")
])

# Save output - DIFFERENT NAMES ONLY
output_rptcc = f"ILOAN_ICISRPTCC{REPTMON}{REPTYEAR}"
icisrptcc_df.write_parquet(OUTPUT_PATH / f"{output_rptcc}.parquet")
icisrptcc_df.write_csv(OUTPUT_PATH / f"{output_rptcc}.csv")

print(f"ICISRPTCC saved to {output_rptcc}")
print(f"Shape: {icisrptcc_df.shape}")

print(f"\nProcessing completed for Islamic version (EIIWCIGR)")
print(f"Output saved to: {OUTPUT_PATH}")
