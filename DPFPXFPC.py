from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/RBP2.B033.ODPA.EXT.FILE.MIS.txt")  # Input text file

# Output directory
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 1) DATA LMTDET.LMTDET ----------
# Read directly from the txt file
req = [
 "AANO","APRVDT","APRVAMT","ACCTNO","TOTLMTAMT","LASTMNTDT","LMTID","LMTAMT",
 "LMTSTARTDT","LMTENDDT","LMTTERM","LMTTERMID","LMTPAIDIND",
 "COLL1","COLL2","COLL3","COLL4","COLL5","COLL6","COLL7","COLL8","COLL9","COLL10"
]

# Check if text file exists
if not DPAA_TXT.exists():
    raise FileNotFoundError(f"Text file not found: {DPAA_TXT}")

print(f"Reading from text file: {DPAA_TXT}")

# Try different delimiters
delimiters_to_try = ['\t', '|', ',', ' ']
df = None

for delimiter in delimiters_to_try:
    try:
        print(f"  Trying delimiter: '{delimiter}'")
        df = pl.read_csv(
            DPAA_TXT, 
            separator=delimiter,
            try_parse_dates=True,
            infer_schema_length=1000,
            ignore_errors=False
        )
        
        if len(df.columns) > 5:
            print(f"  Success! Found {len(df.columns)} columns with delimiter '{delimiter}'")
            break
    except Exception as e:
        print(f"  Failed with delimiter '{delimiter}': {str(e)[:100]}")
        continue

if df is None:
    print("  Trying auto-detection...")
    df = pl.read_csv(
        DPAA_TXT,
        try_parse_dates=True,
        infer_schema_length=1000
    )
    print(f"  Auto-detection found {len(df.columns)} columns")

if df is None or len(df.columns) == 0:
    raise ValueError("Could not read the text file with any delimiter")

print(f"\nSuccessfully read {df.height} rows with {df.width} columns")
print(f"First few column names: {df.columns[:10]}")

# Check for missing columns and add them as nulls
miss = [c for c in req if c not in df.columns]
if miss:
    print(f"Adding missing columns: {miss}")
    df = df.with_columns([pl.lit(None).alias(c) for c in miss])

# Select only required columns
available_req = [c for c in req if c in df.columns]
LMTDET_LMTDET = df.select(available_req)

print(f"Selected {len(available_req)} columns for output")

# Write to both Parquet and CSV in output directory
parquet_path = OUTPUT_DIR / "LMTDET.parquet"
csv_path = OUTPUT_DIR / "LMTDET.csv"

LMTDET_LMTDET.write_parquet(parquet_path)
LMTDET_LMTDET.write_csv(csv_path)

print(f"Written to Parquet: {parquet_path}")
print(f"Written to CSV: {csv_path}")

# ---------- 2) DATA LMTDET.REPTDATE (KEEP=EXTDATE REPTDATE) ----------
today = date.today()
REPTDATE = today - timedelta(days=1)
YYYY = f"{REPTDATE.year:04d}"
MM   = f"{REPTDATE.month:02d}"
DD   = f"{REPTDATE.day:02d}"
DAY1 = date(REPTDATE.year, 1, 1)
DAYS = (today - DAY1).days
TEMPDATE = f"{MM}{DD}{YYYY}{DAYS}"
EXTDATE = int(TEMPDATE)

reptdate_df = pl.DataFrame({"EXTDATE":[EXTDATE], "REPTDATE":[REPTDATE]})

# Write REPTDATE to both Parquet and CSV
reptdate_parquet_path = OUTPUT_DIR / "REPTDATE.parquet"
reptdate_csv_path = OUTPUT_DIR / "REPTDATE.csv"

reptdate_df.write_parquet(reptdate_parquet_path)
reptdate_df.write_csv(reptdate_csv_path)

print(f"Written REPTDATE to Parquet: {reptdate_parquet_path}")
print(f"Written REPTDATE to CSV: {reptdate_csv_path}")

print("\n" + "="*50)
print("DONE - All outputs generated:")
print("="*50)
print(f"\nOutput directory: {OUTPUT_DIR}")
print(f"\nFiles created:")
print(f"  • {parquet_path}")
print(f"  • {csv_path}")
print(f"  • {reptdate_parquet_path}")
print(f"  • {reptdate_csv_path}")
print(f"\nREPTDATE: {REPTDATE}")
print(f"EXTDATE: {EXTDATE}")
