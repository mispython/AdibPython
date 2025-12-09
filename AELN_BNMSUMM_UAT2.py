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

# Check if files exist
if not CIS_FILE.exists():
    print(f"WARNING: CIS file not found at {CIS_FILE}")
    print("Checking for available dates...")
    # Find the most recent available file
    available_dates = sorted(BASE_PATH.glob("year=*/month=*/day=*/CUST_GROUPING_OUT.parquet"))
    if available_dates:
        CIS_FILE = available_dates[-1]  # Use most recent file
        print(f"Using most recent file: {CIS_FILE}")
        # Extract date from path
        parts = str(CIS_FILE).split('/')
        for part in parts:
            if part.startswith('year='):
                year = int(part.replace('year=', ''))
            elif part.startswith('month='):
                month = int(part.replace('month=', ''))
            elif part.startswith('day='):
                day = int(part.replace('day=', ''))
        REPTDATE = datetime(year, month, day)
        REPTMON = f"{REPTDATE.month:02d}"
        REPTYEAR = str(REPTDATE.year)[-2:]
    else:
        print("ERROR: No CIS files found!")
        exit(1)

# Read CISBASEL data first, then process
print(f"\nReading CISBASEL data from: {CIS_FILE}")
cisbasel_df = pl.read_parquet(CIS_FILE)

# Check column names and data types
print(f"Columns in CIS file: {cisbasel_df.columns}")
print(f"Data types: {cisbasel_df.dtypes}")

# Clean and convert numeric columns - handle dot '.' as null
cisbasel_df = cisbasel_df.with_columns([
    # Replace '.' with null and convert to float
    pl.col("BALAMT").str.replace_all(r"\.", "").cast(pl.Float64),
    pl.col("TOTAMT").str.replace_all(r"\.", "").cast(pl.Float64),
    # Convert integer columns
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("NOTENO").cast(pl.Int64),
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

# Save outputs
output_basename = f"LOAN_CISBASEL{REPTMON}{REPTYEAR}"
cisbasel_df.write_parquet(OUTPUT_PATH / f"{output_basename}.parquet")
cisbasel_df.write_csv(OUTPUT_PATH / f"{output_basename}.csv")

print(f"\nCISBASEL saved to {output_basename}")
print(f"Shape: {cisbasel_df.shape}")
print(f"Sample data:")
print(cisbasel_df.head())

# Process CISRPTCC if file exists
RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"

if RCIS_FILE.exists():
    print(f"\nReading CISRPTCC data from: {RCIS_FILE}")
    cisrptcc_df = pl.read_parquet(RCIS_FILE)
    
    # Convert GRPING to integer (handle potential nulls)
    cisrptcc_df = cisrptcc_df.with_columns([
        pl.col("GRPING").cast(pl.Int64)
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
    print(f"Sample data:")
    print(cisrptcc_df.head())
else:
    print(f"\nWARNING: CISRPTCC file not found at {RCIS_FILE}")

print(f"\nProcessing completed for date: {REPTDATE.date()}")
