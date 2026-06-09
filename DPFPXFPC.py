from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat")  # Input directory
ODPA_FILE = DPAA / "RBP2.B033.ODPA.EXT.FILE.MIS.txt"  # Source text file
PARQUET_DIR = DPAA  # Input parquet location

# Output directories - different from input
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output")  # Main output directory
OUTPUT_PARQUET = OUTPUT_DIR / "parquet"  # Parquet output subdirectory
OUTPUT_CSV = OUTPUT_DIR / "csv"  # CSV output subdirectory

# Create output directories
OUTPUT_PARQUET.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV.mkdir(parents=True, exist_ok=True)

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

# Write to both Parquet and CSV in output directories
parquet_output_path = OUTPUT_PARQUET / "LMTDET.parquet"
csv_output_path = OUTPUT_CSV / "LMTDET.csv"

LMTDET_LMTDET.write_parquet(parquet_output_path)
LMTDET_LMTDET.write_csv(csv_output_path)

print(f"Written to Parquet: {parquet_output_path}")
print(f"Written to CSV: {csv_output_path}")

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

reptdate_df = pl.DataFrame({"EXTDATE":[EXTDATE], "REPTDATE":[REPTDATE]})

# Write REPTDATE to both Parquet and CSV
reptdate_parquet_path = OUTPUT_PARQUET / "REPTDATE.parquet"
reptdate_csv_path = OUTPUT_CSV / "REPTDATE.csv"

reptdate_df.write_parquet(reptdate_parquet_path)
reptdate_df.write_csv(reptdate_csv_path)

print(f"Written REPTDATE to Parquet: {reptdate_parquet_path}")
print(f"Written REPTDATE to CSV: {reptdate_csv_path}")

# Optional: Also create the LMTDET directory structure if needed for compatibility
LMTDET_COMPAT = Path("SAP.PBB.DPDET.parquet_lib")
LMTDET_COMPAT.mkdir(parents=True, exist_ok=True)
LMTDET_LMTDET.write_parquet(LMTDET_COMPAT / "LMTDET.parquet")
reptdate_df.write_parquet(LMTDET_COMPAT / "REPTDATE.parquet")
print(f"\nCompatibility output (original path): {LMTDET_COMPAT}")

print("\n" + "="*50)
print("DONE - All outputs generated:")
print("="*50)
print(f"\nMain outputs in: {OUTPUT_DIR}")
print(f"  - Parquet files: {OUTPUT_PARQUET}")
print(f"  - CSV files: {OUTPUT_CSV}")
print(f"\nFiles created:")
print(f"  • {parquet_output_path}")
print(f"  • {csv_output_path}")
print(f"  • {reptdate_parquet_path}")
print(f"  • {reptdate_csv_path}")
print(f"\nCompatibility outputs in: {LMTDET_COMPAT}")
print(f"  • {LMTDET_COMPAT / 'LMTDET.parquet'}")
print(f"  • {LMTDET_COMPAT / 'REPTDATE.parquet'}")
print(f"\nREPTDATE: {REPTDATE}")
print(f"EXTDATE: {EXTDATE}")
