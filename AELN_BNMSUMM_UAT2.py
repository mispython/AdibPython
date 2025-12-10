import polars as pl
from datetime import datetime
from pathlib import Path

# Base paths
BASE_INPUT_PATH = Path("Data_Warehouse/MIS/XMIS/Outsource/input")
BASE_OUTPUT_PATH = Path("Data_Warehouse/MIS/XMIS/output")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
REPTDATE_FILE = BASE_INPUT_PATH / "DEPOSIT_REPTDATE.sas7bdat"
EGOLD_FILE = BASE_INPUT_PATH / "GOLD_ETRX_20250715.txt"
OTHER_FILE = BASE_INPUT_PATH / "GOLD_ETRX_OTH_20250715.txt"
MIS_DATA_PATH = BASE_OUTPUT_PATH / "MIS.sas7bdat"

# Read REPTDATE from SAS file
# Note: Polars' read_sas() uses pyreadstat under the hood
# If you don't have pyreadstat installed, install it: pip install pyreadstat
REPTDATE_df = pl.read_sas(REPTDATE_FILE)
REPTDATE_value = REPTDATE_df["REPTDATE"][0]
REPTDATE = datetime.strptime(str(REPTDATE_value), "%Y-%m-%d")

day = REPTDATE.day
month = REPTDATE.month
year = REPTDATE.year

# Determine week number
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]
REPTMON = f"{month:02d}"
REPTDAY = f"{day:02d}"
REPTDT = REPTDATE.strftime("%Y%m%d")

# Read EGOLD text file
EGOLD = pl.read_csv(
    EGOLD_FILE,
    has_header=False,
    separator="|",  # Adjust delimiter as needed
    new_columns=[
        "TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO", "MPURCGM", "MSALEGM", 
        "BRANCH", "MPURCPR", "MPURCAMT", "MSALEPR", "MSALEAMT"
    ],
    schema_overrides={
        "TRXNYY": pl.Int32,
        "TRXNMM": pl.Int32,
        "TRXNDD": pl.Int32
    }
)

# Create derived columns for EGOLD
EGOLD = EGOLD.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])

# Read OTHER text file
OTHER = pl.read_csv(
    OTHER_FILE,
    has_header=False,
    separator="|",  # Adjust delimiter as needed
    new_columns=[
        "TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO", "MPURCGM", "MSALEGM",
        "BRANCH", "MPURCPR", "MPURCAMT", "MSALEPR", "MSALEAMT", 
        "TRANCODE", "CHANNEL"
    ],
    schema_overrides={
        "TRXNYY": pl.Int32,
        "TRXNMM": pl.Int32,
        "TRXNDD": pl.Int32
    }
)

# Create derived columns for OTHER
OTHER = OTHER.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER], how="diagonal")

# Append logic
target_name = f"MIS_GOLDTRAN{REPTMON}{NOWK}"

if REPTDAY == "01":
    # Start new dataset
    MIS_GOLDTRAN = GOLDTRAN
else:
    # Load existing MIS data if it exists
    if MIS_DATA_PATH.exists():
        MIS_GOLDTRAN = pl.read_sas(MIS_DATA_PATH)
        # Remove duplicates for same REPTDATE
        MIS_GOLDTRAN = MIS_GOLDTRAN.filter(pl.col("REPTDATE") != REPTDT)
        # Append new data
        MIS_GOLDTRAN = pl.concat([MIS_GOLDTRAN, GOLDTRAN], how="diagonal")
    else:
        MIS_GOLDTRAN = GOLDTRAN

# Create Hive partition structure: /year/month/day/
hive_partition_path = BASE_OUTPUT_PATH / f"year={year}" / f"month={REPTMON}" / f"day={REPTDAY}"
hive_partition_path.mkdir(parents=True, exist_ok=True)

# Save to Hive partition
output_file = hive_partition_path / f"{target_name}.parquet"
MIS_GOLDTRAN.write_parquet(output_file)

# Save TEMP copy in Hive partition
temp_name = f"TEMP_GOLDTRAN{REPTMON}{NOWK}{REPTYEAR}"
temp_file = hive_partition_path / f"{temp_name}.parquet"
MIS_GOLDTRAN.write_parquet(temp_file)

# Export CSV with compression (optional)
csv_file = hive_partition_path / "TRANFILE.csv.gz"
MIS_GOLDTRAN.write_csv(csv_file, separator=",")

print(f"Processing complete!")
print(f"Report Date: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Week: {NOWK}")
print(f"Output saved to: {hive_partition_path}")
print(f"Records processed: {len(MIS_GOLDTRAN)}")
