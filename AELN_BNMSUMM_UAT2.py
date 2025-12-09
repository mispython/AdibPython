import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# Get last day of previous month (matches SAS: MDY(MONTH(TODAY()),1,YEAR(TODAY()))-1)
TODAY = datetime.today()
first_day_this_month = TODAY.replace(day=1)
REPTDATE = first_day_this_month - timedelta(days=1)  # Last day of previous month
REPTMON = f"{REPTDATE.month:02d}"
REPTYEAR = str(REPTDATE.year)[-2:]

# Paths
BASE_PATH = Path("/host/cis/parquet")
OUTPUT_PATH = Path("/pythonITD/mis_dev/sas_migration/CIGR/output")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Today: {TODAY.date()}")
print(f"Processing last day of previous month: {REPTDATE.date()}")
print(f"Report month: {REPTMON}, Report year: {REPTYEAR}")

# Construct file paths - assuming same file structure but for Islamic data
# Adjust file names if different for Islamic version
CIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_OUT.parquet"
RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"

print(f"\nCIS file: {CIS_FILE}")
print(f"RCIS file: {RCIS_FILE}")

# Check if files exist - if not, try to find most recent
if not CIS_FILE.exists():
    print(f"WARNING: CIS file not found at {CIS_FILE}")
    available_files = sorted(BASE_PATH.glob("year=*/month=*/day=*/CUST_GROUPING_OUT.parquet"))
    if available_files:
        CIS_FILE = available_files[-1]  # Use most recent
        print(f"Using most recent file: {CIS_FILE}")
        # Extract date from path
        path_str = str(CIS_FILE)
        year = int(path_str.split("year=")[1].split("/")[0])
        month = int(path_str.split("month=")[1].split("/")[0])
        day = int(path_str.split("day=")[1].split("/")[0])
        REPTDATE = datetime(year, month, day)
        REPTMON = f"{REPTDATE.month:02d}"
        REPTYEAR = str(REPTDATE.year)[-2:]
    else:
        print("ERROR: No CIS files found!")
        exit(1)

# Process ICISBASEL (Islamic CISBASEL)
print(f"\nReading ICISBASEL data from: {CIS_FILE}")
icisbasel_df = pl.read_parquet(CIS_FILE)

print(f"Columns: {icisbasel_df.columns}")
print(f"Data types: {icisbasel_df.dtypes}")

# Convert data types to match SAS INPUT specifications
icisbasel_df = icisbasel_df.with_columns([
    # Handle BALAMT conversion - string to float64, dot '.' as null
    pl.when(pl.col("BALAMT") == ".")
        .then(None)
        .otherwise(pl.col("BALAMT").cast(pl.Float64, strict=False))
        .alias("BALAMT"),
    # Handle TOTAMT (might already be float or string)
    pl.when(pl.col("TOTAMT") == ".")
        .then(None)
        .otherwise(pl.col("TOTAMT").cast(pl.Float64, strict=False))
        .alias("TOTAMT"),
    # Convert integer columns
    pl.col("ACCTNO").cast(pl.Int64, strict=False),
    pl.col("NOTENO").cast(pl.Int64, strict=False),
])

# Select and order columns exactly as SAS INPUT statement
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

# Save output as ILOAN.ICISBASELmmYY (matching SAS output)
output_basename = f"ILOAN_ICISBASEL{REPTMON}{REPTYEAR}"
icisbasel_df.write_parquet(OUTPUT_PATH / f"{output_basename}.parquet")
icisbasel_df.write_csv(OUTPUT_PATH / f"{output_basename}.csv")

print(f"\nICISBASEL saved to {output_basename}")
print(f"Shape: {icisbasel_df.shape}")
print(f"Sample data:")
print(icisbasel_df.head())

# Process ICISRPTCC (Islamic CISRPTCC)
# Update RCIS_FILE path with potentially new REPTDATE
RCIS_FILE = BASE_PATH / f"year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}/CUST_GROUPING_RPTCC.parquet"

if RCIS_FILE.exists():
    print(f"\nReading ICISRPTCC data from: {RCIS_FILE}")
    icisrptcc_df = pl.read_parquet(RCIS_FILE)
    
    print(f"Columns: {icisrptcc_df.columns}")
    print(f"Data types: {icisrptcc_df.dtypes}")
    
    # Convert GRPING from string to integer (matches SAS: GRPING 11.)
    # Note: SAS has GRPING as numeric (11.) not string ($11.) in ICISRPTCC
    icisrptcc_df = icisrptcc_df.with_columns([
        pl.col("GRPING").cast(pl.Int64, strict=False)
    ])
    
    # Select and order columns exactly as SAS INPUT statement
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
    
    # Save output as ILOAN.ICISRPTCCmmYY (matching SAS output)
    output_rptcc = f"ILOAN_ICISRPTCC{REPTMON}{REPTYEAR}"
    icisrptcc_df.write_parquet(OUTPUT_PATH / f"{output_rptcc}.parquet")
    icisrptcc_df.write_csv(OUTPUT_PATH / f"{output_rptcc}.csv")
    
    print(f"ICISRPTCC saved to {output_rptcc}")
    print(f"Shape: {icisrptcc_df.shape}")
    print(f"Sample data:")
    print(icisrptcc_df.head())
else:
    print(f"\nWARNING: ICISRPTCC file not found at {RCIS_FILE}")
    
    # Try to find most recent RCIS file
    available_rcis = sorted(BASE_PATH.glob("year=*/month=*/day=*/CUST_GROUPING_RPTCC.parquet"))
    if available_rcis:
        RCIS_FILE = available_rcis[-1]
        print(f"Using most recent RCIS file: {RCIS_FILE}")
        
        icisrptcc_df = pl.read_parquet(RCIS_FILE)
        icisrptcc_df = icisrptcc_df.with_columns([
            pl.col("GRPING").cast(pl.Int64, strict=False)
        ])
        
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
        
        output_rptcc = f"ILOAN_ICISRPTCC{REPTMON}{REPTYEAR}"
        icisrptcc_df.write_parquet(OUTPUT_PATH / f"{output_rptcc}.parquet")
        icisrptcc_df.write_csv(OUTPUT_PATH / f"{output_rptcc}.csv")
        
        print(f"ICISRPTCC saved to {output_rptcc}")
        print(f"Shape: {icisrptcc_df.shape}")

print(f"\nProcessing completed for Islamic version (EIIWCIGR)")
print(f"Report date: {REPTDATE.date()} (last day of previous month)")
print(f"Output saved to: {OUTPUT_PATH}")
