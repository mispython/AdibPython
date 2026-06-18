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

# Read EGOLD flat file - using whitespace separator
EGOLD = pl.read_csv(
    EGOLD_FILE,
    separator=" ",
    has_header=False,
    schema_overrides={
        "column_1": pl.Utf8,  # TRXNYY (might be stored as string with leading spaces)
        "column_2": pl.Utf8,  # ACCTNO
        "column_3": pl.Float64,  # MPURCGM
        "column_4": pl.Float64,  # MSALEGM
        "column_5": pl.Int32,  # BRANCH
        "column_6": pl.Float64,  # MPURCPR
        "column_7": pl.Float64,  # MPURCAMT
        "column_8": pl.Float64,  # MSALEPR
        "column_9": pl.Float64,  # MSALEAMT
        "column_10": pl.Float64,  # Some other field
    }
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

# Convert TRXNYY to Int32 by extracting digits
EGOLD = EGOLD.with_columns([
    pl.col("TRXNYY").str.replace_all(r"\s", "").cast(pl.Int32).alias("TRXNYY")
])

# Create TRXNDATE and REPTDATE
EGOLD = EGOLD.with_columns([
    pl.date(
        pl.col("TRXNYY") + 2000,  # Assuming year is 2-digit (like 20, 21, etc.)
        (datetime.now().month),  # Use current month as placeholder if not in data
        (datetime.now().day)     # Use current day as placeholder if not in data
    ).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])

# Read OTHER flat file with similar approach
OTHER = pl.read_csv(
    OTHER_FILE,
    separator=" ",
    has_header=False,
    schema_overrides={
        "column_1": pl.Utf8,
        "column_2": pl.Utf8,
        "column_3": pl.Float64,
        "column_4": pl.Float64,
        "column_5": pl.Int32,
        "column_6": pl.Float64,
        "column_7": pl.Float64,
        "column_8": pl.Float64,
        "column_9": pl.Float64,
        "column_10": pl.Utf8,  # TRANCODE
        "column_11": pl.Utf8,  # CHANNEL
    }
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

# Convert TRXNYY to Int32
OTHER = OTHER.with_columns([
    pl.col("TRXNYY").str.replace_all(r"\s", "").cast(pl.Int32).alias("TRXNYY")
])

# Create TRXNDATE and REPTDATE for OTHER
OTHER = OTHER.with_columns([
    pl.date(
        pl.col("TRXNYY") + 2000,
        (datetime.now().month),
        (datetime.now().day)
    ).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER])

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
