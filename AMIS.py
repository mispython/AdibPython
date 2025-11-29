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
FULL_SUMMARY_SRC = BASE_INPUT_PATH / "year=2025/month=11/day=27/CIPHONET_FULL_SUMMARY.parquet"  # Full summary for EIBMCHN2

# CRMWH library targets
CRMWH_DIR = BASE_OUTPUT_PATH / "CRMWH"
CRMWH_DIR.mkdir(parents=True, exist_ok=True)
CHANNEL_SUM_PATH = CRMWH_DIR / "CHANNEL_SUM.parquet"

# CHN library targets (from EIBMCHN2)
CHN_DIR = BASE_OUTPUT_PATH / "CHN"
CHN_DIR.mkdir(parents=True, exist_ok=True)
CHN_CHANNEL_UPDATE = CHN_DIR / "CHANNEL_UPDATE.parquet"

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

print("=" * 80)
print("DATE CALCULATIONS")
print("=" * 80)
print(f"REPTDATE: {REPTDATE}")
print(f"PREVDATE: {PREVDATE}")
print(f"RDATE (SAS date format): {RDATE}")
print(f"REPTDATE_STR: {REPTDATE_STR}")
print()

# =============================================================================
# EIBMCHNL PROCESSING
# =============================================================================
print("=" * 80)
print("EIBMCHNL PROCESSING - CHANNEL SUMMARY")
print("=" * 80)

# --- CHANNEL step (1:1 SAS migration) ---
channel_df = (
    pl.read_parquet(ALL_SUMMARY_SRC)
    .with_columns([
        pl.col("CHANNEL").str.to_uppercase(),
        pl.lit(PREVDATE).alias("REPTDATE"),
    ])
    .with_columns([
        pl.col("REPTDATE").map_elements(
            lambda d: d.strftime("%b%y").upper(),
            return_dtype=pl.Utf8,
        ).alias("MONTH")
    ])
    .drop("REPTDATE")
    .rename({
        "PROMPT": "TOLPROMPT",
        "UPDATED": "TOLUPDATE"
    })
    .select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"])
)

print("CHANNEL (head):")
print(channel_df.head(20))
print("CHANNEL schema:")
print(channel_df.schema)
print()

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
    
    if "PROMPT" in existing.columns and "TOLPROMPT" not in existing.columns:
        existing = existing.rename({"PROMPT": "TOLPROMPT"})
    if "UPDATED" in existing.columns and "TOLUPDATE" not in existing.columns:
        existing = existing.rename({"UPDATED": "TOLUPDATE"})
    
    channel_sum = pl.concat([existing, channel_df], how="vertical_relaxed")
else:
    channel_sum = channel_df

channel_sum = channel_sum.select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"])
channel_sum.write_parquet(CHANNEL_SUM_PATH)

print("CRMWH.CHANNEL_SUM (head):")
print(pl.read_parquet(CHANNEL_SUM_PATH).head(20))
print("CRMWH.CHANNEL_SUM schema:")
print(pl.read_parquet(CHANNEL_SUM_PATH).schema)
print()

# --- BRANCH step ---
print("=" * 80)
print("BRANCH PROCESSING")
print("=" * 80)

branch_source = pl.read_parquet(OTC_SUMMARY_SRC)
print("BRANCH source data structure:")
print(branch_source.head())
print("BRANCH source shape:", branch_source.shape)
print("BRANCH source columns:", branch_source.columns)
print()

if branch_source.height == 0:
    print("WARNING: OTC_SUMMARY file is empty! Creating empty BRANCH dataframe.")
    branch_df = pl.DataFrame({
        'BRANCHNO': pl.Series([], dtype=pl.Int64),
        'TOLPROMPT': pl.Series([], dtype=pl.Int64),
        'TOLUPDATE': pl.Series([], dtype=pl.Int64)
    })
else:
    branch_df = (
        branch_source
        .filter(pl.col("CHANNEL") == "OTC")
        .with_columns([
            pl.lit(999).cast(pl.Int64).alias("BRANCHNO")  # Adjust this with actual branch mapping
        ])
        .rename({
            "PROMPT": "TOLPROMPT",
            "UPDATED": "TOLUPDATE"
        })
        .select(["BRANCHNO", "TOLPROMPT", "TOLUPDATE"])
    )

print("BRANCH (after transformation):")
print(branch_df.head())
print("BRANCH schema:")
print(branch_df.schema)
print()

# --- BCODE step ---
bcode_df = pl.read_parquet(BCODE_SRC)
print("BCODE (head):")
print(bcode_df.head())
print("BCODE schema:")
print(bcode_df.schema)
print()

bcode_df = bcode_df.with_columns([
    pl.col("BRANCHNO").cast(pl.Int64).alias("BRANCHNO")
]).select(["BRANCHNO"])

print("BCODE (after conversion):")
print(bcode_df.head())
print("BCODE schema after conversion:")
print(bcode_df.schema)
print()

# --- PROC SORT ---
bcode_sorted = bcode_df.sort("BRANCHNO")
branch_sorted = branch_df.sort("BRANCHNO")

# --- MERGE ---
otc_detail = (
    bcode_sorted.join(
        branch_sorted,
        on="BRANCHNO",
        how="left"
    )
    .with_columns([
        pl.col("TOLPROMPT").fill_null(0),
        pl.col("TOLUPDATE").fill_null(0),
    ])
)

print("OTC_DETAIL (after merge):")
print(otc_detail.head())
print(f"OTC_DETAIL shape: {otc_detail.shape}")
print()

otc_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
otc_path = CRMWH_DIR / f"{otc_name}.parquet"
otc_detail.write_parquet(otc_path)
print(f"OTC detail written to: {otc_path}")
print()

# =============================================================================
# EIBMCHN2 PROCESSING
# =============================================================================
print("=" * 80)
print("EIBMCHN2 PROCESSING - CHANNEL UPDATE")
print("=" * 80)

# --- DATA SUM: read from Parquet ---
SUM = (
    pl.read_parquet(FULL_SUMMARY_SRC)
      .select(["ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"])
)

print("SUM data (head):")
print(SUM.head())
print()

# --- DATA UAT ---
UAT = (
    SUM
    .with_row_index(name="_N_", offset=1)
    .with_columns(
        pl.when(pl.col("_N_") == 1).then(pl.lit("TOTAL PROMPT BASE"))
         .when(pl.col("_N_") == 2).then(pl.lit("TOTAL UPDATED"))
         .otherwise(pl.lit(None))
         .alias("LINE")
    )
    .select(["LINE", "ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"])
)

print("UAT data (head):")
print(UAT.head())
print()

# --- PROC SQL SELECT ---
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
print(f"CHN.CHANNEL_UPDATE written to: {CHN_CHANNEL_UPDATE}")
print("CHN.CHANNEL_UPDATE (head):")
print(pl.read_parquet(CHN_CHANNEL_UPDATE).head())
print()

# =============================================================================
# FINAL SANITY CHECKS
# =============================================================================
print("=" * 80)
print("SANITY CHECKS")
print("=" * 80)

# DuckDB sanity check for CHANNEL_SUM
con.register('CHANNEL_SUM', channel_sum.to_arrow())
result = con.execute("""
    SELECT CHANNEL, SUM(TOLPROMPT) AS SUM_PROMPT, SUM(TOLUPDATE) AS SUM_UPDATE
    FROM CHANNEL_SUM
    GROUP BY CHANNEL
    ORDER BY CHANNEL
""").fetchdf()
print("DuckDB sanity check (CHANNEL_SUM grouped):")
print(result)
print()

print("=" * 80)
print("PROCESSING COMPLETE")
print("=" * 80)
print(f"Output directories:")
print(f"  CRMWH: {CRMWH_DIR}")
print(f"  CHN:   {CHN_DIR}")
