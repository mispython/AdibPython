import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/gold") # Folder for source files
BASE_OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/GOLD/EIBDEGLD") # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
EGOLD_FILE = BASE_INPUT_PATH / "EGOLD_TRX.txt"
OTHER_FILE = BASE_INPUT_PATH / "EGOLD_OTHR.txt"

# Use test date for testing purposes (June 16, 2026)
# To use current date, uncomment the line below and comment the test date line
# REPTDATE = datetime.now().date()
REPTDATE = datetime(2026, 6, 16).date()  # Test date: June 16, 2026

day = REPTDATE.day
month = REPTDATE.month
year = REPTDATE.year

# Equivalent to CALL SYMPUT logic
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]
REPTMON = f"{month:02d}" # year2.
REPTDAY = f"{day:02d}" # z2.
REPTDT = REPTDATE.strftime("%Y%m%d") # 8.

print(f"Testing with REPTDATE: {REPTDATE}")
print(f"REPTMON: {REPTMON}, NOWK: {NOWK}, REPTDAY: {REPTDAY}, REPTDT: {REPTDT}")

def date_to_sas_days(date_obj):
    """Convert Python date to SAS date (days since 1960-01-01)"""
    sas_epoch = datetime(1960, 1, 1).date()
    delta = date_obj - sas_epoch
    return delta.days

def read_fixed_width_file(filepath, columns_spec):
    """
    Read a fixed-width file based on column specifications
    columns_spec: list of tuples (name, start, length, dtype)
    start is 1-indexed as in SAS
    """
    # Read the entire file as strings
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    data = []
    for line in lines:
        # Remove trailing newline but keep spaces
        line = line.rstrip('\n')
        if len(line) < max([start + length - 1 for _, start, length, _ in columns_spec]):
            # Pad line if too short
            line = line.ljust(max([start + length - 1 for _, start, length, _ in columns_spec]))
        
        row = {}
        for col_name, start, length, dtype in columns_spec:
            # SAS uses 1-indexed positions, Python uses 0-indexed
            value = line[start-1:start-1+length].strip()
            
            if dtype == 'int':
                row[col_name] = int(value) if value else None
            elif dtype == 'float':
                # Handle decimal places - SAS format like 11.6 means 6 decimal places
                # The value in the file might not have a decimal point
                if '.' in value:
                    row[col_name] = float(value)
                else:
                    # If no decimal point, divide by 10^decimal_places
                    # Extract decimal places from dtype string
                    if isinstance(dtype, str) and '.' in dtype:
                        decimals = int(dtype.split('.')[1])
                        row[col_name] = float(value) / (10 ** decimals)
                    else:
                        row[col_name] = float(value)
            else:  # string
                row[col_name] = value
        
        data.append(row)
    
    return pl.DataFrame(data)

# Define column specifications based on SAS code
# Format: (name, start_position, length, dtype)
egold_columns = [
    ("TRXNYY", 2, 4, 'int'),      # Year
    ("TRXNMM", 6, 2, 'int'),      # Month
    ("TRXNDD", 8, 2, 'int'),      # Day
    ("ACCTNO", 13, 10, 'str'),    # Account number
    ("MPURCGM", 26, 10, 'float'), # Purchase grams
    ("MSALEGM", 42, 10, 'float'), # Sale grams
    ("BRANCH", 58, 3, 'int'),     # Branch
    ("MPURCPR", 64, 11, 'float.6'), # Selling price (11.6 format)
    ("MPURCAMT", 78, 14, 'float.2'), # Purchase amount (14.2 format)
    ("MSALEPR", 95, 11, 'float.6'),  # Buying price (11.6 format)
    ("MSALEAMT", 109, 14, 'float.2') # Sale amount (14.2 format)
]

other_columns = [
    ("TRXNYY", 2, 4, 'int'),
    ("TRXNMM", 6, 2, 'int'),
    ("TRXNDD", 8, 2, 'int'),
    ("ACCTNO", 13, 10, 'str'),
    ("MPURCGM", 26, 10, 'float'),
    ("MSALEGM", 42, 10, 'float'),
    ("BRANCH", 58, 3, 'int'),
    ("MPURCPR", 64, 11, 'float.6'),
    ("MPURCAMT", 78, 14, 'float.2'),
    ("MSALEPR", 95, 11, 'float.6'),
    ("MSALEAMT", 109, 14, 'float.2'),
    ("TRANCODE", 125, 3, 'str'),   # Transaction code
    ("CHANNEL", 128, 3, 'str')     # Channel
]

# Read EGOLD flat file
print("Reading EGOLD file...")
EGOLD = read_fixed_width_file(EGOLD_FILE, egold_columns)

# Add TRANCODE and CHANNEL columns to EGOLD (as null since they don't exist)
EGOLD = EGOLD.with_columns([
    pl.lit(None).cast(pl.Utf8).alias("TRANCODE"),
    pl.lit(None).cast(pl.Utf8).alias("CHANNEL")
])

# Create TRXNDATE as SAS date (days since 1960-01-01)
EGOLD = EGOLD.with_columns([
    # Convert Python date to SAS date number
    pl.lit(date_to_sas_days(REPTDATE)).alias("REPTDATE_NUM"),
    # For TRXNDATE, create from TRXNYY, TRXNMM, TRXNDD
    pl.struct(["TRXNYY", "TRXNMM", "TRXNDD"])
      .map_elements(lambda x: date_to_sas_days(datetime(x['TRXNYY'], x['TRXNMM'], x['TRXNDD']).date()), return_dtype=pl.Int64)
      .alias("TRXNDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])

# Convert REPTDATE to SAS date number
EGOLD = EGOLD.with_columns([
    pl.lit(date_to_sas_days(REPTDATE)).alias("REPTDATE")
])

# Drop the temporary REPTDATE_NUM column if it exists
if "REPTDATE_NUM" in EGOLD.columns:
    EGOLD = EGOLD.drop("REPTDATE_NUM")

print(f"EGOLD records: {EGOLD.height}")

# Read OTHER flat file
print("Reading OTHER file...")
OTHER = read_fixed_width_file(OTHER_FILE, other_columns)

# Create TRXNDATE as SAS date (days since 1960-01-01) for OTHER
OTHER = OTHER.with_columns([
    pl.struct(["TRXNYY", "TRXNMM", "TRXNDD"])
      .map_elements(lambda x: date_to_sas_days(datetime(x['TRXNYY'], x['TRXNMM'], x['TRXNDD']).date()), return_dtype=pl.Int64)
      .alias("TRXNDATE"),
    pl.lit(date_to_sas_days(REPTDATE)).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

print(f"OTHER records: {OTHER.height}")

# Reorder columns in OTHER to match EGOLD (for consistent concatenation)
# Get the column order from EGOLD
column_order = EGOLD.columns
OTHER = OTHER.select(column_order)

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER])

print(f"Total combined records: {GOLDTRAN.height}")

# Reorder columns to match SAS production output order
# Production output order: TRXNYY, TRXNMM, TRXNDD, ACCTNO, MPURCGM, MSALEGM, BRANCH, 
# MPURCPR, MPURCAMT, MSALEPR, MSALEAMT, TRXNDATE, REPTDATE, CHANNELIND, TRANCODE, CHANNEL
output_column_order = [
    "TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO", "MPURCGM", "MSALEGM", "BRANCH",
    "MPURCPR", "MPURCAMT", "MSALEPR", "MSALEAMT", "TRXNDATE", "REPTDATE", 
    "CHANNELIND", "TRANCODE", "CHANNEL"
]

# Select columns in the desired order
GOLDTRAN = GOLDTRAN.select(output_column_order)

# Replace null TRANCODE with "." to match production output
GOLDTRAN = GOLDTRAN.with_columns([
    pl.col("TRANCODE").fill_null(".").alias("TRANCODE")
])

# Round numeric columns to match production format
GOLDTRAN = GOLDTRAN.with_columns([
    pl.col("MPURCGM").round(2).alias("MPURCGM"),
    pl.col("MSALEGM").round(2).alias("MSALEGM"),
    pl.col("MPURCPR").round(4).alias("MPURCPR"),
    pl.col("MPURCAMT").round(2).alias("MPURCAMT"),
    pl.col("MSALEPR").round(4).alias("MSALEPR"),
    pl.col("MSALEAMT").round(2).alias("MSALEAMT"),
])

print(f"Final column order: {GOLDTRAN.columns}")

# Append Logic
target_name = f"MIS_GOLDTRAN{REPTMON}{NOWK}"
parquet_file = BASE_OUTPUT_PATH / f"{target_name}.parquet"
text_file = BASE_OUTPUT_PATH / f"{target_name}.txt"

# Determine if we should start new dataset (SAS logic: day 01, 09, 16, 23)
# These are the first day of each week
if REPTDAY in ["01", "09", "16", "23"]:
    print(f"Starting new dataset for week {NOWK}")
    MIS_GOLDTRAN = GOLDTRAN
else:
    print(f"Appending to existing dataset for week {NOWK}")
    # Load existing dataset if it exists
    if parquet_file.exists():
        MIS_GOLDTRAN = pl.read_parquet(parquet_file)
        # Remove duplicates for same REPTDATE
        MIS_GOLDTRAN = MIS_GOLDTRAN.filter(pl.col("REPTDATE") != date_to_sas_days(REPTDATE))
        # Append new
        MIS_GOLDTRAN = pl.concat([MIS_GOLDTRAN, GOLDTRAN])
        print(f"Appended {GOLDTRAN.height} records to existing {MIS_GOLDTRAN.height - GOLDTRAN.height} records")
    else:
        print(f"File doesn't exist, creating new dataset")
        MIS_GOLDTRAN = GOLDTRAN

# Ensure final column order is maintained after append
MIS_GOLDTRAN = MIS_GOLDTRAN.select(output_column_order)

# Sort the data by TRXNYY, TRXNMM, TRXNDD, ACCTNO (like SAS would typically sort)
MIS_GOLDTRAN = MIS_GOLDTRAN.sort(["TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO"])

# Save as parquet
print(f"Saving to {parquet_file}")
MIS_GOLDTRAN.write_parquet(parquet_file)

# Save as text file (pipe-delimited)
print(f"Saving to {text_file}")
MIS_GOLDTRAN.write_csv(text_file, separator="|")

print(f"Processing complete. Total records: {MIS_GOLDTRAN.height}")
print(f"Files saved as {target_name}.parquet and {target_name}.txt")
print(f"Output columns: {MIS_GOLDTRAN.columns}")
print(f"Week number: {NOWK}")

# Show sample of output
print("\nSample of output data:")
print(MIS_GOLDTRAN.head(10))
