import polars as pl
from datetime import datetime
from pathlib import Path

BASE_INPUT_PATH = Path("Data_Warehouse/MIS/XMIS/Outsource/input") # Folder for source files
BASE_OUTPUT_PATH = Path("Data_Warehouse/MIS/XMIS/output") # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
REPTDATE_FILE = BASE_INPUT_PATH / "DEPOSIT_REPTDATE.txt"
EGOLD_FILE = BASE_INPUT_PATH / "GOLD_ETRX_20250715"
OTHER_FILE = BASE_INPUT_PATH / "GOLD_ETRX_OTH_20250715"

# Existing MIS data for path (for appending)
MIS_DATA_PATH = BASE_OUTPUT_PATH / "MIS"

# Read REPTDATE and set variables
REPTDATE_df = pl.read_csv("data/input/DEPOSIT_REPTDATE.txt")
REPTDATE_value = REPTDATE_df["REPTDATE"][0]
REPTDATE = datetime.strptime(REPTDATE_value, "%Y-%m-%d")

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

# Read EGOLD flat file
EGOLD = pl.read_csv(
    "GOLD_ETRX_20250715",
    has_header=False,
    columns=[
        "TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO ", "MPURCGM ", "MSALEGM", "BRANCH", "MPURCPR", "MPURCAMT", "MSALEPR", "MSALEAMT"
    ],
    dtypes={
        "TRXNYY": pl.Int32,
        "TRXNMM": pl.Int32,  
        "TRXNDD": pl.Int32
    }                                              
)

# Create TRXNDATE and REPTDATE
EGOLD = EGOLD.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])
    

# Read OTHER flat file
OTHER = pl.read_csv(
    "GOLD_ETRX_OTH_20250715",
    has_header=False,
    columns=[
        "TRXNYY", "TRXNMM", "TRXNDD", "ACCTNO ", "MPURCGM ", "MSALEGM", "BRANCH", "MPURCPR", "MPURCAMT", "MSALEPR", "MSALEAMT", "TRANCODE", "CHANNEL" 
    ],
    dtypes={
        "TRXNYY": pl.Int32,
        "TRXNMM": pl.Int32,  
        "TRXNDD": pl.Int32
    }    
)

OTHER = OTHER.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(REPTDT).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER])

# Append Logic
target_name = f"MIS_GOLDTRAN{REPTMON}{NOWK}"

if{REPTDAY} == "01":
    # Start new dataset
    MIS_GOLDTRAN = GOLDTRAN
else:
    # Load existing dataset
    MIS_GOLDTRAN = pl.read.parquet(f"{target_name}.parquet")
    # Remove duplicates for same REPTDATE
    MIS_GOLDTRAN = MIS_GOLDTRAN.filter(pl.col("REPTDATE") != REPTDT)
    # Append new
    MIS_GOLDTRAN = pl.concat([MIS_GOLDTRAN, GOLDTRAN])

# Save back
MIS_GOLDTRAN.write_parquet(f"{target_name}.parquet")

# Save TEMP copy
TEMP = f"TEMP_GOLDTRAN{REPTMON}{NOWK}{REPTYEAR}"
MIS_GOLDTRAN.write.parquet(f"{TEMP}.parquet")

# Export
MIS_GOLDTRAN.write.csv("TRANFILE.csv.gz")
