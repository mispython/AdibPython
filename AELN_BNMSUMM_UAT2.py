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
    print(f"ERROR: CIS file not found at {CIS_FILE}")
    # Try alternative: previous day if file not found
    REPTDATE = TODAY - timedelta(days=2)
    CIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_OUT.parquet"
    print(f"Trying alternative date: {REPTDATE.date()}")
    print(f"New CIS file: {CIS_FILE}")

if not RCIS_FILE.exists():
    print(f"ERROR: RCIS file not found at {RCIS_FILE}")
    # Try alternative: previous day if file not found
    REPTDATE = TODAY - timedelta(days=2)
    RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"
    print(f"Trying alternative date: {REPTDATE.date()}")
    print(f"New RCIS file: {RCIS_FILE}")

# Update REPTMON and REPTYEAR after potential date change
REPTMON = f"{REPTDATE.month:02d}"
REPTYEAR = str(REPTDATE.year)[-2:]

# Read and process CISBASEL
cisbasel_df = pl.read_parquet(CIS_FILE).select([
    pl.col("GRPING"),
    pl.col("GROUPNO"),
    pl.col("CUSTNO"),
    pl.col("FULLNAME"),
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("NOTENO").cast(pl.Int64),
    pl.col("PRODUCT"),
    pl.col("AMTINDC"),
    pl.col("BALAMT").cast(pl.Float64),
    pl.col("TOTAMT").cast(pl.Float64),
    pl.col("RLENCODE"),
    pl.col("PRIMSEC")
])

# Save outputs
cisbasel_df.write_parquet(OUTPUT_PATH / f"LOAN_CISBASEL{REPTMON}{REPTYEAR}.parquet")
cisbasel_df.write_csv(OUTPUT_PATH / f"LOAN_CISBASEL{REPTMON}{REPTYEAR}.csv")

# Read and process CISRPTCC
cisrptcc_df = pl.read_parquet(RCIS_FILE).select([
    pl.col("GRPING").cast(pl.Int64),
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
cisrptcc_df.write_parquet(OUTPUT_PATH / f"LOAN_CISRPTCC{REPTMON}{REPTYEAR}.parquet")
cisrptcc_df.write_csv(OUTPUT_PATH / f"LOAN_CISRPTCC{REPTMON}{REPTYEAR}.csv")

print(f"\nFiles saved to {OUTPUT_PATH}")
print(f"CISBASEL shape: {cisbasel_df.shape}")
print(f"CISRPTCC shape: {cisrptcc_df.shape}")
