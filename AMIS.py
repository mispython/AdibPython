import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# --- Paths ---
BASE_INPUT_PATH  = Path("/host/cis/parquet/")  
BASE_OUTPUT_PATH = Path("/pythonITD/mis_dev/OUTPUT/") 
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Source files
ALL_SUMMARY_SRC = BASE_INPUT_PATH / "year=2025/month=11/day=27/CIPHONET_ALL_SUMMARY.parquet"   # CHANNEL level data
OTC_SUMMARY_SRC = BASE_INPUT_PATH / "year=2025/month=11/day=27/CIPHONET_OTC_SUMMARY.parquet"    # BRANCH level data  
BCODE_SRC       = BASE_INPUT_PATH / "sas_parquet/PBBBRCH.parquet"                              # Branch codes

# CRMWH library targets
CRMWH_DIR = BASE_OUTPUT_PATH / "CRMWH"
CRMWH_DIR.mkdir(parents=True, exist_ok=True)
CHANNEL_SUM_PATH = CRMWH_DIR / "CHANNEL_SUM.parquet"

# --- SAS Date Format Calculations ---
SAS_ORIGIN = datetime(1960, 1, 1).date()
REPTDATE = datetime.today().date() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# --- Formatted date variables (matching SAS SYMPUT) ---
REPTYEAR = f"{REPTDATE.year % 100:02d}"          # YEAR2.
REPTMON  = f"{REPTDATE.month:02d}"               # Z2.
REPTDAY  = f"{REPTDATE.day:02d}"                 # Z2.
REPTDATE_STR = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year % 100:02d}"  # DDMMYY8.

print(f"REPTDATE: {REPTDATE}")
print(f"PREVDATE: {PREVDATE}")
print(f"RDATE (SAS date format): {RDATE}")
print(f"REPTDATE_STR: {REPTDATE_STR}")

# --- CHANNEL step (1:1 SAS migration) ---
# SAS: DATA CHANNEL; INFILE CHANNEL; INPUT @001 CHANNEL $UPCASE5. @006 TOLPROMPT 8. @014 TOLUPDATE 8.;
channel_df = (
    pl.read_parquet(ALL_SUMMARY_SRC)
    .with_columns([
        # Convert CHANNEL to uppercase (matching $UPCASE5.)
        pl.col("CHANNEL").str.to_uppercase(),
        # SAS: REPTDATE=TODAY()-2
        pl.lit(PREVDATE).alias("REPTDATE"),
    ])
    .with_columns([
        # SAS: MONTH=PUT(REPTDATE,MONYY5.)
        pl.col("REPTDATE").map_elements(
            lambda d: d.strftime("%b%y").upper(),
            return_dtype=pl.Utf8,
        ).alias("MONTH")
    ])
    .drop("REPTDATE")
    # Rename to match SAS variable names
    .rename({
        "PROMPT": "TOLPROMPT",
        "UPDATED": "TOLUPDATE"
    })
    # Select only the final columns to ensure consistent schema
    .select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"])
)

print("CHANNEL (head):")
print(channel_df.head(20))
print("CHANNEL schema:")
print(channel_df.schema)

# --- PROC APPEND DATA=CHANNEL BASE=CRMWH.CHANNEL_SUM FORCE ---
def parquet_exists(path: Path) -> bool:
    try:
        pq.read_schema(path)
        return True
    except Exception:
        return path.exists()

if parquet_exists(CHANNEL_SUM_PATH):
    existing = pl.read_parquet(CHANNEL_SUM_PATH)
    print("Existing CHANNEL_SUM schema:")
    print(existing.schema)
    
    # Ensure both DataFrames have the same column names and types
    # Rename columns in existing data if they don't match
    if "PROMPT" in existing.columns and "TOLPROMPT" not in existing.columns:
        existing = existing.rename({"PROMPT": "TOLPROMPT"})
    if "UPDATED" in existing.columns and "TOLUPDATE" not in existing.columns:
        existing = existing.rename({"UPDATED": "TOLUPDATE"})
    
    # FORCE option - append regardless of column type differences
    channel_sum = pl.concat([existing, channel_df], how="vertical_relaxed")
else:
    channel_sum = channel_df

# Ensure final schema consistency
channel_sum = channel_sum.select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"])

channel_sum.write_parquet(CHANNEL_SUM_PATH)
print("CRMWH.CHANNEL_SUM (head):")
print(pl.read_parquet(CHANNEL_SUM_PATH).head(20))
print("CRMWH.CHANNEL_SUM schema:")
print(pl.read_parquet(CHANNEL_SUM_PATH).schema)

# --- BRANCH step (1:1 SAS migration) ---
# SAS: DATA BRANCH; INFILE BRANCH; INPUT @001 BRANCHNO 5. @006 TOLPROMPT 8. @014 TOLUPDATE 8.;
branch_source = pl.read_parquet(OTC_SUMMARY_SRC)

print("BRANCH source data structure:")
print(branch_source.head())
print("BRANCH source shape:", branch_source.shape)
print("BRANCH source columns:", branch_source.columns)

# Check if OTC_SUMMARY file is empty
if branch_source.height == 0:
    print("WARNING: OTC_SUMMARY file is empty! Creating empty BRANCH dataframe.")
    # Create empty dataframe with correct schema
    branch_df = pl.DataFrame({
        'BRANCHNO': pl.Series([], dtype=pl.Int64),
        'TOLPROMPT': pl.Series([], dtype=pl.Int64),
        'TOLUPDATE': pl.Series([], dtype=pl.Int64)
    })
else:
    # Transform OTC summary to match SAS BRANCH structure
    branch_df = (
        branch_source
        .filter(pl.col("CHANNEL") == "OTC")  # Keep only OTC records
        .with_columns([
            # Convert CHANNEL to BRANCHNO - YOU NEED TO ADJUST THIS LOGIC
            # This is a placeholder - replace with your actual branch mapping
            pl.lit(999).cast(pl.Int64).alias("BRANCHNO")  # Use actual branch number
        ])
        .rename({
            "PROMPT": "TOLPROMPT",
            "UPDATED": "TOLUPDATE"
        })
        .select(["BRANCHNO", "TOLPROMPT", "TOLUPDATE"])  # Keep only needed columns
    )

print("BRANCH (after transformation):")
print(branch_df.head())
print("BRANCH schema:")
print(branch_df.schema)

# --- BCODE step (1:1 SAS migration) ---
# SAS: DATA BCODE; INFILE BCODE; INPUT @002 BRANCHNO 3.;
bcode_df = pl.read_parquet(BCODE_SRC)
print("BCODE (head):")
print(bcode_df.head())
print("BCODE schema:")
print(bcode_df.schema)

# Convert BRANCHNO to integer to match BRANCH data type
bcode_df = bcode_df.with_columns([
    pl.col("BRANCHNO").cast(pl.Int64).alias("BRANCHNO")
]).select(["BRANCHNO"])  # Keep only BRANCHNO column as per SAS input

print("BCODE (after conversion):")
print(bcode_df.head())
print("BCODE schema after conversion:")
print(bcode_df.schema)

# --- PROC SORT (1:1 SAS migration) ---
# SAS: PROC SORT DATA=BCODE; BY BRANCHNO; RUN;
# SAS: PROC SORT DATA=BRANCH; BY BRANCHNO; RUN;
bcode_sorted = bcode_df.sort("BRANCHNO")
branch_sorted = branch_df.sort("BRANCHNO")

print("BCODE sorted (head):")
print(bcode_sorted.head())
print("BRANCH sorted (head):")
print(branch_sorted.head())

# --- MERGE (1:1 SAS migration) ---
# SAS: DATA CRMWH.OTC_DETAIL_&REPTMON&REPTYEAR;
#      MERGE BCODE(IN=A) BRANCH(IN=B); BY BRANCHNO;
#      IF TOLPROMPT=. THEN DO TOLPROMPT=0; END;
#      IF TOLUPDATE=. THEN DO TOLUPDATE=0; END;
#      IF A;
#      RUN;
otc_detail = (
    bcode_sorted.join(
        branch_sorted,
        on="BRANCHNO",
        how="left"  # This matches "IF A" - keep all BCODE rows
    )
    .with_columns([
        # SAS: IF TOLPROMPT=. THEN DO TOLPROMPT=0; END;
        pl.col("TOLPROMPT").fill_null(0),
        # SAS: IF TOLUPDATE=. THEN DO TOLUPDATE=0; END;
        pl.col("TOLUPDATE").fill_null(0),
    ])
)

print("OTC_DETAIL (after merge):")
print(otc_detail.head())
print(f"OTC_DETAIL shape: {otc_detail.shape}")

# Output name: CRMWH.OTC_DETAIL_&REPTMON&REPTYEAR;
otc_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
otc_path = CRMWH_DIR / f"{otc_name}.parquet"
otc_detail.write_parquet(otc_path)
print(f"OTC detail written to: {otc_path}")

# --- DuckDB sanity check ---
con = duckdb.connect(database=':memory:')
con.register('CHANNEL_SUM', channel_sum.to_arrow())
result = con.execute("""
    SELECT CHANNEL, SUM(TOLPROMPT) AS SUM_PROMPT, SUM(TOLUPDATE) AS SUM_UPDATE
    FROM CHANNEL_SUM
    GROUP BY CHANNEL
    ORDER BY CHANNEL
""").fetchdf()
print("DuckDB sanity check (CHANNEL_SUM grouped):")
print(result)













import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# Base folders (adjust to your environment)
BASE_INPUT_PATH  = Path("/host/cis/parquet/")     # holds input Parquet
BASE_OUTPUT_PATH = Path("/pythonITD/mis_dev/OUTPUT")    # holds output Parquet
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Map SAS DDs to Parquet paths
# CHANNEL DD -> RBP2.B033.CIPHONET.FULL.SUMMARY (assumed pre-parsed to Parquet)
CHANNEL_PARQUET = BASE_INPUT_PATH / "year=2025/month=11/day=27/CIPHONET_FULL_SUMMARY.parquet"

# SAS LIB CHN -> a folder of Parquets; target table we append to
CHN_DIR = BASE_OUTPUT_PATH / "CHN"
CHN_DIR.mkdir(parents=True, exist_ok=True)
CHN_CHANNEL_UPDATE = CHN_DIR / "CHANNEL_UPDATE.parquet"

# --- OPTIONS NOCENTER YEARCUTOFF=1950 (no action in Python) ---

# --- SAS Date Format Calculations ---
SAS_ORIGIN = datetime(1960, 1, 1).date()
REPTDATE = datetime.today().date() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# --- Formatted date variables ---
REPTYEAR = f"{REPTDATE.year % 100:02d}"  # YEAR2.
REPTMON  = f"{REPTDATE.month:02d}"       # Z2.
REPTDAY  = f"{REPTDATE.day:02d}"         # Z2.
REPTDATE_STR = REPTDATE.strftime("%d/%m/%y") # DDMMYY8. -> "dd/mm/yy"

print(f"REPTDATE: {REPTDATE}")
print(f"PREVDATE: {PREVDATE}")
print(f"RDATE (SAS date format): {RDATE}")
print(f"REPTDATE_STR: {REPTDATE_STR}")

# --- DATA SUM: read from Parquet with required columns only ---
SUM = (
    pl.read_parquet(CHANNEL_PARQUET)
      .select(["ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"])
)

# --- DATA UAT(KEEP=DESC ATM EBK OTC TOTAL DATE) with SAS _N_ logic ---
UAT = (
    SUM
    .with_row_index(name="_N_", offset=1)  # FIXED: using with_row_index instead of with_row_count
    .with_columns(
        pl.when(pl.col("_N_") == 1).then(pl.lit("TOTAL PROMPT BASE"))
         .when(pl.col("_N_") == 2).then(pl.lit("TOTAL UPDATED"))
         .otherwise(pl.lit(None))
         .alias("LINE")
    )
    .select(["LINE", "ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"])  # KEEP/order same as SAS
)

# --- PROC SQL SELECT (no-op) executed via DuckDB for fidelity ---
con = duckdb.connect()
con.register("UAT", UAT.to_arrow())
UAT_SQL = pl.from_arrow(
    con.execute("""
        SELECT LINE, ATM, EBK, OTC, TOTAL, REPORT_DATE
        FROM UAT
    """).arrow()
)

# --- PROC APPEND DATA=UAT BASE=CHN.CHANNEL_UPDATE FORCE ---
if CHN_CHANNEL_UPDATE.exists():
    existing = pl.read_parquet(CHN_CHANNEL_UPDATE).select(UAT_SQL.columns)
    combined = pl.concat([existing, UAT_SQL], how="vertical_relaxed")
else:
    combined = UAT_SQL

combined.write_parquet(CHN_CHANNEL_UPDATE)

# - PROC CPORT step removed intentionally (not needed in Parquet-native pipeline).
# - If you require packaging the CHN library for transfer, we can optionally zip CHN_DIR.
