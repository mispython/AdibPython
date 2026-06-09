from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat")  # Changed: directory path, not file
ODPA_FILE = DPAA / "RBP2.B033.ODPA.EXT.FILE.MIS.txt"  # Source text file (if needed)
PARQUET_DIR = DPAA  # Or wherever the parquet file actually is
LMTDET = Path("SAP.PBB.DPDET.parquet_lib")                 # target lib
LMTDET.mkdir(parents=True, exist_ok=True)

# ---------- 1) DATA LMTDET.LMTDET ----------
# DPAA Parquet must already expose the SAS fields from INPUT.
req = [
 "AANO","APRVDT","APRVAMT","ACCTNO","TOTLMTAMT","LASTMNTDT","LMTID","LMTAMT",
 "LMTSTARTDT","LMTENDDT","LMTTERM","LMTTERMID","LMTPAIDIND",
 "COLL1","COLL2","COLL3","COLL4","COLL5","COLL6","COLL7","COLL8","COLL9","COLL10"
]

# Check if parquet file exists in the expected location
parquet_path = PARQUET_DIR / "ODPA_EXT_FILE_MIS.parquet"

# If not found, try to convert the txt file to parquet first (if that's the source)
if not parquet_path.exists():
    # Check if the text file exists
    if ODPA_FILE.exists():
        print(f"Parquet not found, reading from text file: {ODPA_FILE}")
        # Read from text file (adjust delimiter as needed - assuming tab or comma)
        df = pl.read_csv(ODPA_FILE, separator='\t', try_parse_dates=True)
        # Save as parquet for future runs
        df.write_parquet(parquet_path)
        print(f"Saved parquet to: {parquet_path}")
    else:
        raise FileNotFoundError(f"Neither parquet nor text file found. Checked: {parquet_path} and {ODPA_FILE}")
else:
    print(f"Reading from parquet: {parquet_path}")
    df = pl.read_parquet(parquet_path)

# Check for missing columns and add them as nulls
miss = [c for c in req if c not in df.columns]
if miss:
    print(f"Adding missing columns: {miss}")
    df = df.with_columns([pl.lit(None).alias(c) for c in miss])

# Select only required columns
LMTDET_LMTDET = df.select(req)
LMTDET_LMTDET.write_parquet(LMTDET / "LMTDET.parquet")
print(f"Written: {LMTDET / 'LMTDET.parquet'}")

# ---------- 2) DATA LMTDET.REPTDATE (KEEP=EXTDATE REPTDATE) ----------
today = date.today()
REPTDATE = today - timedelta(days=1)
YYYY = f"{REPTDATE.year:04d}"
MM   = f"{REPTDATE.month:02d}"
DD   = f"{REPTDATE.day:02d}"
DAY1 = date(REPTDATE.year, 1, 1)
DAYS = (today - DAY1).days  # SAS: DAYS = TODAY() - DAY1;
TEMPDATE = f"{MM}{DD}{YYYY}{DAYS}"
EXTDATE = int(TEMPDATE)  # COMPRESS(...)*1

pl.DataFrame({"EXTDATE":[EXTDATE], "REPTDATE":[REPTDATE]}).write_parquet(LMTDET / "REPTDATE.parquet")

print("\nDONE:")
print(" - LMTDET/LMTDET.parquet")
print(" - LMTDET/REPTDATE.parquet")
print(f"\nREPTDATE: {REPTDATE}")
print(f"EXTDATE: {EXTDATE}")
