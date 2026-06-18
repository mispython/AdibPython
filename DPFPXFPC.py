import polars as pl
from datetime import datetime
from pathlib import Path

BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/gold") # Folder for source files
BASE_OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/GOLD/EIBDEGLD") # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
EGOLD_FILE = BASE_INPUT_PATH / "EGOLD_TRX.txt"
OTHER_FILE = BASE_INPUT_PATH / "EGOLD_OTHR.txt"

# Use current datetime instead of reading from file
REPTDATE = datetime.now().date()

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

# Read EGOLD flat file - using read_csv with proper schema
EGOLD = pl.read_csv(
    EGOLD_FILE,
    separator=" ",  # space separated
    has_header=False,
    infer_schema_length=10000,  # Increase schema inference
    schema_overrides={
        "column_1": pl.Utf8,    # TRXNYY (date or identifier)
        "column_2": pl.Utf8,    # ACCTNO (account number - could be large)
        "column_3": pl.Float64, # MPURCGM
        "column_4": pl.Float64, # MSALEGM
        "column_5": pl.Utf8,    # BRANCH (was causing overflow - use string)
        "column_6": pl.Float64, # MPURCPR
        "column_7": pl.Float64, # MPURCAMT
        "column_8": pl.Float64, # MSALEPR
        "column_9": pl.Float64, # MSALEAMT
        "column_10": pl.Float64, # Some other field
    },
    truncate_ragged_lines=True  # Handle lines of varying lengths
)

# Rename columns based on sample data structure
EGOLD = EGOLD.rename({
    "column_1": "TRXNYY",
    "column_2": "ACCTNO",
    "column_3": "MPURCGM",
    "column_4": "MSALEGM",
    "column_5": "BRANCH",
    "column_6": "MPURCPR",
    "column_7": "MPURCAMT",
    "column_8": "MSALEPR",
    "column_9": "MSALEAMT",
})

# Clean up TRXNYY - extract the date part
# Based on sample: "120260615" or "20260615" - seems to be date in some format
EGOLD = EGOLD.with_columns([
    # Remove whitespace and convert to string
    pl.col("TRXNYY").str.replace_all(r"\s", "").alias("TRXNYY_clean")
])

# Try to extract year, month, day from TRXNYY
# If TRXNYY is like "20260615" (8 digits) or "120260615" (9 digits)
EGOLD = EGOLD.with_columns([
    # If length is 8, it's YYYYMMDD
    # If length is 9, first digit might be something else
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(0, 4).cast(pl.Int32))
      .otherwise(
          # For 9 digit, try to parse as date
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(0, 4).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_YEAR"),
    
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(4, 2).cast(pl.Int32))
      .otherwise(
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(3, 2).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_MONTH"),
    
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(6, 2).cast(pl.Int32))
      .otherwise(
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(5, 2).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_DAY")
])

# Create TRXNDATE
EGOLD = EGOLD.with_columns([
    pl.date(
        pl.col("TRXN_YEAR"),
        pl.col("TRXN_MONTH"),
        pl.col("TRXN_DAY")
    ).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])

# Read OTHER flat file with similar approach
OTHER = pl.read_csv(
    OTHER_FILE,
    separator=" ",
    has_header=False,
    infer_schema_length=10000,
    schema_overrides={
        "column_1": pl.Utf8,
        "column_2": pl.Utf8,
        "column_3": pl.Float64,
        "column_4": pl.Float64,
        "column_5": pl.Utf8,    # BRANCH - use string to avoid overflow
        "column_6": pl.Float64,
        "column_7": pl.Float64,
        "column_8": pl.Float64,
        "column_9": pl.Float64,
        "column_10": pl.Utf8,   # TRANCODE
        "column_11": pl.Utf8,   # CHANNEL
    },
    truncate_ragged_lines=True
)

# Rename columns for OTHER
OTHER = OTHER.rename({
    "column_1": "TRXNYY",
    "column_2": "ACCTNO",
    "column_3": "MPURCGM",
    "column_4": "MSALEGM",
    "column_5": "BRANCH",
    "column_6": "MPURCPR",
    "column_7": "MPURCAMT",
    "column_8": "MSALEPR",
    "column_9": "MSALEAMT",
    "column_10": "TRANCODE",
    "column_11": "CHANNEL"
})

# Clean up TRXNYY for OTHER
OTHER = OTHER.with_columns([
    pl.col("TRXNYY").str.replace_all(r"\s", "").alias("TRXNYY_clean")
])

# Extract date components for OTHER
OTHER = OTHER.with_columns([
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(0, 4).cast(pl.Int32))
      .otherwise(
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(0, 4).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_YEAR"),
    
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(4, 2).cast(pl.Int32))
      .otherwise(
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(3, 2).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_MONTH"),
    
    pl.when(pl.col("TRXNYY_clean").str.lengths() == 8)
      .then(pl.col("TRXNYY_clean").str.slice(6, 2).cast(pl.Int32))
      .otherwise(
          pl.when(pl.col("TRXNYY_clean").str.lengths() == 9)
            .then(pl.col("TRXNYY_clean").str.slice(5, 2).cast(pl.Int32))
            .otherwise(pl.lit(None))
      ).alias("TRXN_DAY")
])

# Create TRXNDATE and REPTDATE for OTHER
OTHER = OTHER.with_columns([
    pl.date(
        pl.col("TRXN_YEAR"),
        pl.col("TRXN_MONTH"),
        pl.col("TRXN_DAY")
    ).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER])

# Drop intermediate columns if needed
GOLDTRAN = GOLDTRAN.drop(["TRXNYY_clean", "TRXN_YEAR", "TRXN_MONTH", "TRXN_DAY"])

# Append Logic
target_name = f"MIS_GOLDTRAN{REPTMON}{NOWK}"
parquet_file = BASE_OUTPUT_PATH / f"{target_name}.parquet"
text_file = BASE_OUTPUT_PATH / f"{target_name}.txt"

if REPTDAY == "01":
    # Start new dataset
    MIS_GOLDTRAN = GOLDTRAN
else:
    # Load existing dataset if it exists
    if parquet_file.exists():
        MIS_GOLDTRAN = pl.read_parquet(parquet_file)
        # Remove duplicates for same REPTDATE
        MIS_GOLDTRAN = MIS_GOLDTRAN.filter(pl.col("REPTDATE") != REPTDT)
        # Append new
        MIS_GOLDTRAN = pl.concat([MIS_GOLDTRAN, GOLDTRAN])
    else:
        MIS_GOLDTRAN = GOLDTRAN

# Save as parquet
MIS_GOLDTRAN.write_parquet(parquet_file)

# Save as text file (pipe-delimited)
MIS_GOLDTRAN.write_csv(text_file, separator="|")

print(f"Processing complete. Files saved as {target_name}.parquet and {target_name}.txt")
print(f"Total records: {MIS_GOLDTRAN.height}")
