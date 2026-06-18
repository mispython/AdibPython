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

# Read EGOLD flat file
EGOLD = pl.read_csv(
    EGOLD_FILE,
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
    OTHER_FILE,
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
