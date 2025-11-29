import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow.parquet as pq

# =============================================================================
# DATE CALCULATIONS (Batch Date Logic)
# =============================================================================
# Batch date is TODAY - 1
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# Formatted date variables
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# Calculate last month (BASE) and current month (NEW BASE)
current_month_date = REPTDATE.replace(day=1)
last_month_date = current_month_date - timedelta(days=1)

LAST_MONTH_YEAR = last_month_date.year
LAST_MONTH_NUM = last_month_date.month

CURRENT_MONTH_YEAR = current_month_date.year
CURRENT_MONTH_NUM = current_month_date.month

print("=" * 80)
print("BATCH DATE CALCULATIONS")
print("=" * 80)
print(f"Batch Date (REPTDATE): {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Previous Date (PREVDATE): {PREVDATE.strftime('%Y-%m-%d')}")
print(f"SAS Date (RDATE): {RDATE}")
print()
print(f"LAST MONTH (BASE):        {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}")
print(f"CURRENT MONTH (NEW BASE): {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
print("=" * 80)
print()

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"

# Last month paths (BASE)
LAST_MONTH_PATH = f"{BASE_PATH}/year={LAST_MONTH_YEAR}/month={LAST_MONTH_NUM:02d}"
LAST_CHANNEL_SUM = f"{LAST_MONTH_PATH}/CHANNEL_SUM.parquet"
LAST_CHANNEL_UPDATE = f"{LAST_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# Current month paths (NEW BASE)
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={CURRENT_MONTH_YEAR}/month={CURRENT_MONTH_NUM:02d}"
os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
CURRENT_CHANNEL_SUM = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
CURRENT_CHANNEL_UPDATE = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# Current processing output (from main script)
CRMWH_DIR = Path("/pythonITD/mis_dev/OUTPUT/CRMWH")
CHN_DIR = Path("/pythonITD/mis_dev/OUTPUT/CHN")
TODAY_CHANNEL_SUM = CRMWH_DIR / "CHANNEL_SUM.parquet"
TODAY_CHANNEL_UPDATE = CHN_DIR / "CHANNEL_UPDATE.parquet"

print(f"Last Month (BASE):     {LAST_MONTH_PATH}")
print(f"Current Month (NEW):   {CURRENT_MONTH_PATH}")
print()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def parquet_exists(path):
    """Check if parquet file exists and is readable"""
    try:
        if isinstance(path, Path):
            path = str(path)
        pq.read_schema(path)
        return True
    except Exception:
        return False

def read_or_create_empty(path, schema_dict):
    """Read parquet if exists, otherwise create empty DataFrame with schema"""
    if parquet_exists(path):
        return pl.read_parquet(path)
    else:
        print(f"⚠ File not found: {path}, creating empty DataFrame")
        return pl.DataFrame(schema_dict)

# =============================================================================
# PROCESS CHANNEL_SUM (CRMWH)
# =============================================================================
print("=" * 80)
print("PROCESSING CHANNEL_SUM")
print("=" * 80)

# Define schema
channel_sum_schema = {
    'CHANNEL': pl.Utf8,
    'TOLPROMPT': pl.Int64,
    'TOLUPDATE': pl.Int64,
    'MONTH': pl.Utf8
}

# Read last month (BASE)
print(f"Reading last month base: {LAST_CHANNEL_SUM}")
last_month_channel = read_or_create_empty(LAST_CHANNEL_SUM, channel_sum_schema)
print(f"  Records: {len(last_month_channel)}")
if len(last_month_channel) > 0:
    print(f"  Date range: {last_month_channel['MONTH'].unique().to_list()}")
print()

# Read today's data
print(f"Reading today's data: {TODAY_CHANNEL_SUM}")
if parquet_exists(TODAY_CHANNEL_SUM):
    today_channel = pl.read_parquet(TODAY_CHANNEL_SUM)
    print(f"  Records: {len(today_channel)}")
    if len(today_channel) > 0:
        print(f"  Date range: {today_channel['MONTH'].unique().to_list()}")
else:
    print("  ⚠ Today's data not found!")
    today_channel = pl.DataFrame(schema=channel_sum_schema)
print()

# Combine last month + today = current month (NEW BASE)
print("Combining data...")
combined_channel_sum = pl.concat(
    [last_month_channel, today_channel],
    how="vertical_relaxed"
).select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"])

print(f"  Combined records: {len(combined_channel_sum)}")
print()

# Save current month (NEW BASE)
combined_channel_sum.write_parquet(CURRENT_CHANNEL_SUM)
print(f"✓ Saved NEW BASE: {CURRENT_CHANNEL_SUM}")
print()

# Show summary
print("CHANNEL_SUM Summary by Channel:")
summary = (
    combined_channel_sum
    .group_by("CHANNEL")
    .agg([
        pl.col("TOLPROMPT").sum().alias("TOTAL_PROMPT"),
        pl.col("TOLUPDATE").sum().alias("TOTAL_UPDATE"),
        pl.col("MONTH").n_unique().alias("MONTHS")
    ])
    .sort("CHANNEL")
)
print(summary)
print()

# =============================================================================
# PROCESS CHANNEL_UPDATE (CHN)
# =============================================================================
print("=" * 80)
print("PROCESSING CHANNEL_UPDATE")
print("=" * 80)

# Define schema
channel_update_schema = {
    'LINE': pl.Utf8,
    'ATM': pl.Int64,
    'EBK': pl.Int64,
    'OTC': pl.Int64,
    'TOTAL': pl.Int64,
    'REPORT_DATE': pl.Date
}

# Read last month (BASE)
print(f"Reading last month base: {LAST_CHANNEL_UPDATE}")
last_month_update = read_or_create_empty(LAST_CHANNEL_UPDATE, channel_update_schema)
print(f"  Records: {len(last_month_update)}")
if len(last_month_update) > 0 and 'REPORT_DATE' in last_month_update.columns:
    dates = last_month_update['REPORT_DATE'].drop_nulls()
    if len(dates) > 0:
        print(f"  Date range: {dates.min()} to {dates.max()}")
print()

# Read today's data
print(f"Reading today's data: {TODAY_CHANNEL_UPDATE}")
if parquet_exists(TODAY_CHANNEL_UPDATE):
    today_update = pl.read_parquet(TODAY_CHANNEL_UPDATE)
    print(f"  Records: {len(today_update)}")
    if len(today_update) > 0 and 'REPORT_DATE' in today_update.columns:
        dates = today_update['REPORT_DATE'].drop_nulls()
        if len(dates) > 0:
            print(f"  Date range: {dates.min()} to {dates.max()}")
else:
    print("  ⚠ Today's data not found!")
    today_update = pl.DataFrame(schema=channel_update_schema)
print()

# Combine last month + today = current month (NEW BASE)
print("Combining data...")
combined_channel_update = pl.concat(
    [last_month_update, today_update],
    how="vertical_relaxed"
).select(["LINE", "ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"])

print(f"  Combined records: {len(combined_channel_update)}")
print()

# Save current month (NEW BASE)
combined_channel_update.write_parquet(CURRENT_CHANNEL_UPDATE)
print(f"✓ Saved NEW BASE: {CURRENT_CHANNEL_UPDATE}")
print()

# Show summary
print("CHANNEL_UPDATE Summary by Line:")
summary = (
    combined_channel_update
    .group_by("LINE")
    .agg([
        pl.col("ATM").sum().alias("TOTAL_ATM"),
        pl.col("EBK").sum().alias("TOTAL_EBK"),
        pl.col("OTC").sum().alias("TOTAL_OTC"),
        pl.col("TOTAL").sum().alias("GRAND_TOTAL"),
        pl.col("REPORT_DATE").count().alias("DAYS")
    ])
)
print(summary)
print()

# =============================================================================
# VERIFICATION WITH DUCKDB
# =============================================================================
print("=" * 80)
print("VERIFICATION")
print("=" * 80)

con = duckdb.connect()

# Verify CHANNEL_SUM
print("CHANNEL_SUM Verification:")
con.register('channel_sum', combined_channel_sum.to_arrow())
result = con.execute("""
    SELECT 
        CHANNEL,
        COUNT(*) as record_count,
        SUM(TOLPROMPT) as total_prompt,
        SUM(TOLUPDATE) as total_update,
        COUNT(DISTINCT MONTH) as unique_months
    FROM channel_sum
    GROUP BY CHANNEL
    ORDER BY CHANNEL
""").fetchdf()
print(result)
print()

# Verify CHANNEL_UPDATE
print("CHANNEL_UPDATE Verification:")
con.register('channel_update', combined_channel_update.to_arrow())
result = con.execute("""
    SELECT 
        LINE,
        COUNT(*) as record_count,
        SUM(ATM) as total_atm,
        SUM(EBK) as total_ebk,
        SUM(OTC) as total_otc,
        SUM(TOTAL) as grand_total
    FROM channel_update
    WHERE LINE IS NOT NULL
    GROUP BY LINE
    ORDER BY LINE
""").fetchdf()
print(result)
print()

con.close()

# =============================================================================
# FINAL SUMMARY
# =============================================================================
print("=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
print(f"Batch Date: {REPTDATE.strftime('%Y-%m-%d')}")
print()
print("FILES CREATED:")
print(f"  1. {CURRENT_CHANNEL_SUM}")
print(f"     Records: {len(combined_channel_sum)}")
print()
print(f"  2. {CURRENT_CHANNEL_UPDATE}")
print(f"     Records: {len(combined_channel_update)}")
print()
print("MONTH BREAKDOWN:")
print(f"  Last Month (BASE):     {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}")
print(f"  Current Month (NEW):   {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
print(f"  Today's batch added:   {REPTDATE.strftime('%Y-%m-%d')}")
print("=" * 80)
