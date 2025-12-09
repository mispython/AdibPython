import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# Get yesterday's date for processing (today minus 1 day)
TODAY = datetime.today()
REPTDATE = TODAY - timedelta(days=1)  # Process yesterday's data
REPTMON = f"{REPTDATE.month:02d}"
REPTYEAR = str(REPTDATE.year)[-2:]

# Paths
BASE_PATH = Path("/host/cis/parquet")
OUTPUT_PATH = Path("/pythonITD/mis_dev/sas_migration/CIGR/output")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Construct file paths for yesterday's data
CIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_OUT.parquet"
RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"

print(f"Today: {TODAY.date()}")
print(f"Processing date: {REPTDATE.date()} (yesterday)")
print(f"CIS file: {CIS_FILE}")
print(f"RCIS file: {RCIS_FILE}")

# Read CISBASEL data
print(f"\nReading CISBASEL data from: {CIS_FILE}")
cisbasel_df = pl.read_parquet(CIS_FILE)

print(f"Columns in CIS file: {cisbasel_df.columns}")
print(f"Data types: {cisbasel_df.dtypes}")
print(f"\nSample of BALAMT values:")
print(cisbasel_df.select("BALAMT").head(10))

# Handle BALAMT conversion - dot '.' might represent decimal or null
# First, check if BALAMT contains dots that should be decimal points
cisbasel_df = cisbasel_df.with_columns([
    # Convert BALAMT from string to float, treating '.' as decimal point
    # If it's just a single dot '.', convert to null
    pl.when(pl.col("BALAMT") == ".")
        .then(None)
        .otherwise(pl.col("BALAMT").cast(pl.Float64, strict=False))
        .alias("BALAMT"),
    # TOTAMT is already Float64, no conversion needed
    # Convert integer columns
    pl.col("ACCTNO").cast(pl.Int64, strict=False),
    pl.col("NOTENO").cast(pl.Int64, strict=False),
])

# Select and order columns to match SAS output
cisbasel_df = cisbasel_df.select([
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

# Check results
print(f"\nAfter conversion - BALAMT data type: {cisbasel_df['BALAMT'].dtype}")
print(f"BALAMT null count: {cisbasel_df['BALAMT'].null_count()}")
print(f"Sample after conversion:")
print(cisbasel_df.select(["BALAMT", "TOTAMT", "ACCTNO", "NOTENO"]).head(10))

# Save outputs
output_basename = f"LOAN_CISBASEL{REPTMON}{REPTYEAR}"
cisbasel_df.write_parquet(OUTPUT_PATH / f"{output_basename}.parquet")
cisbasel_df.write_csv(OUTPUT_PATH / f"{output_basename}.csv")

print(f"\nCISBASEL saved to {output_basename}")
print(f"Shape: {cisbasel_df.shape}")

# Process CISRPTCC if file exists
if RCIS_FILE.exists():
    print(f"\nReading CISRPTCC data from: {RCIS_FILE}")
    cisrptcc_df = pl.read_parquet(RCIS_FILE)
    
    # Check column types
    print(f"CISRPTCC columns: {cisrptcc_df.columns}")
    print(f"CISRPTCC data types: {cisrptcc_df.dtypes}")
    
    # Convert GRPING to integer (handle potential nulls)
    cisrptcc_df = cisrptcc_df.with_columns([
        pl.col("GRPING").cast(pl.Int64, strict=False)
    ])
    
    # Select and order columns
    cisrptcc_df = cisrptcc_df.select([
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
    
    # Save outputs
    output_rptcc = f"LOAN_CISRPTCC{REPTMON}{REPTYEAR}"
    cisrptcc_df.write_parquet(OUTPUT_PATH / f"{output_rptcc}.parquet")
    cisrptcc_df.write_csv(OUTPUT_PATH / f"{output_rptcc}.csv")
    
    print(f"CISRPTCC saved to {output_rptcc}")
    print(f"Shape: {cisrptcc_df.shape}")
else:
    print(f"\nWARNING: CISRPTCC file not found at {RCIS_FILE}")

print(f"\nProcessing completed for date: {REPTDATE.date()}")
