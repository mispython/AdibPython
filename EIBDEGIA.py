# eibdegia.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date, datetime

# -----------------------------
# Step 1: Define today's date (like SAS TODAY())
# -----------------------------
today = date.today()
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
RDATE = today.strftime("%Y-%m-%d")  # ISO format

print(f"Running EIBDEGIA for {RDATE}")

# -----------------------------
# Step 2: Read ERATE flat file
# -----------------------------
# SAS uses fixed columns: we replicate with Polars scan_csv and column slicing
# You will need to adjust `erate_file` to the correct raw data path.
erate_file = "RBP2.B033.EBNK.RATES.DATA.txt"

# Read whole line, then slice columns
raw = pl.read_csv(
    erate_file,
    has_header=False,
    infer_schema_length=0,
    separator="|",  # dummy sep so whole line is col0
    new_columns=["line"],
)

# Slice fixed positions based on SAS INPUT
df = raw.with_columns([
    pl.col("line").str.slice(2, 3).alias("CURCODE"),
    pl.col("line").str.slice(0, 1).alias("RECTYPE"),
    pl.col("line").str.slice(1, 1).alias("BANKNO"),
    pl.col("line").str.slice(5, 4).cast(pl.Int32).alias("YY"),
    pl.col("line").str.slice(9, 2).cast(pl.Int32).alias("MM"),
    pl.col("line").str.slice(11, 2).cast(pl.Int32).alias("DD"),
    pl.col("line").str.slice(13, 13).cast(pl.Float64).alias("SELLRATE"),
    pl.col("line").str.slice(26, 13).cast(pl.Float64).alias("BUYRATE"),
    pl.col("line").str.slice(39, 13).cast(pl.Float64).alias("WITHHOLD"),
])

# Filter only CURCODE='XAU'
df = df.filter(pl.col("CURCODE") == "XAU")

# Construct REPTDATE
df = df.with_columns(
    pl.concat_str(
        [pl.col("YY").cast(pl.Utf8),
         pl.col("MM").cast(pl.Utf8).str.zfill(2),
         pl.col("DD").cast(pl.Utf8).str.zfill(2)],
        separator="-"
    ).str.strptime(pl.Date, fmt="%Y-%m-%d").alias("REPTDATE")
)

# Keep only records for today
df = df.filter(pl.col("REPTDATE") == datetime.strptime(RDATE, "%Y-%m-%d").date())

print("Filtered ERATE (XAU):")
print(df)

# -----------------------------
# Step 3: Handle MIS dataset (EGOLD) using DuckDB + Parquet
# -----------------------------
# In SAS: MIS.EFORATE&REPTMON is a monthly dataset
# Here: store in Parquet partitioned by month

out_file = f"MIS_EFORATE_{REPTMON}.parquet"

# If first of month → overwrite
if REPTDAY == "01":
    df.write_parquet(out_file)
else:
    # Append logic with DuckDB
    duckdb.sql("INSTALL parquet; LOAD parquet;")
    try:
        existing = pl.read_parquet(out_file)
        # Remove duplicate REPTDATE
        existing = existing.filter(pl.col("REPTDATE") != datetime.strptime(RDATE, "%Y-%m-%d").date())
        combined = pl.concat([existing, df])
    except FileNotFoundError:
        combined = df

    # Sort by REPTDATE
    combined = combined.sort("REPTDATE")
    combined.write_parquet(out_file)

print(f"Updated MIS dataset written to {out_file}")

# -----------------------------
# Step 4: Optional CSV export
# -----------------------------
df.write_csv(f"EFORATE_{REPTMON}_{REPTDAY}.csv")
print("CSV export done.")
