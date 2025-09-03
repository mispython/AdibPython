EIBMCCR

import polars as pl
from pathlib import Path
from datetime import datetime

# ======================================
# 1. CONFIGURATION
# ======================================
BASE_INPUT_PATH = Path("data/input")
BASE_OUTPUT_PATH = Path("data/output")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Input datasets (as parquet)
REPTDATE_FILE = BASE_INPUT_PATH / "BNM_REPTDATE.parquet"
RAW1_FILE     = BASE_INPUT_PATH / "CCRIS2_PROVISIO.parquet"

# Output datasets
CCRIS_PATH = BASE_OUTPUT_PATH / "CCRIS"
CCRIS_PATH.mkdir(parents=True, exist_ok=True)

TRANFILE_PARQUET = CCRIS_PATH / "CCRIS8_CCR8FTP.parquet"
TRANFILE_CSV     = CCRIS_PATH / "CCRIS8_CCR8FTP.csv"

# ======================================
# 2. READ REPTDATE
# ======================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate_value = reptdate_df["REPTDATE"][0]   # assume single row
# SAS: DDMMYY8. format (e.g., 20-08-25)
RDATE = datetime.strptime(str(reptdate_value), "%Y-%m-%d").strftime("%d%m%y")

# ======================================
# 3. READ RAW1 (already parquet)
# ======================================
raw1 = pl.read_parquet(RAW1_FILE)

# Columns expected in CCRIS.TABLE8 (based on INPUT statement in SAS)
columns = [
    "MICR", "ACCTNO", "NOTENO", "RDDMMYY", "CLASSIFI", "ARREARS",
    "CURBAL", "INTEREST", "OTHERCHG", "SECURITY", "IISOPBAL", "IISSUSP",
    "IISWB", "IISWO", "IISDANAH", "IISPROV", "SPOPBAL", "SPCHARGE",
    "SPWBAMT", "SPWOAMT", "SPDANAH", "SPPROV"
]

# Select only relevant columns
ccris_table8 = raw1.select([col for col in columns if col in raw1.columns])

# Add REPTDT column (from REPTDATE dataset)
ccris_table8 = ccris_table8.with_columns(
    pl.lit(RDATE).alias("REPTDT")
)

# ======================================
# 4. SAVE OUTPUT (PROC CPORT equivalent)
# ======================================
ccris_table8.write_parquet(TRANFILE_PARQUET)
ccris_table8.write_csv(TRANFILE_CSV)

print(" CCRIS.TABLE8 extraction completed")
