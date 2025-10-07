import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
input_parquet = "input/deposit_data.parquet"   # Parquet input (replaces INFILE DEPOSIT)
output_dir = "output/"
run_date = datetime.now()

# Connect DuckDB
con = duckdb.connect()

# -----------------------------
# STEP 1: Read from Parquet
# -----------------------------
query = """
SELECT 
    BANKNO,
    REPTNO,
    FMTCODE,
    BRANCH,
    ACCTNO,
    OPENIND,
    CURBAL,
    INTPLAN,
    LMATDATE
FROM read_parquet(?)
"""
df = con.execute(query, [input_parquet]).arrow()
pl_fd = pl.from_arrow(df)

# -----------------------------
# STEP 2: Filter valid deposits (same logic as SAS)
# -----------------------------
pl_fd = pl_fd.filter(
    (pl.col("BANKNO") == 33) &
    (pl.col("REPTNO") == 4001) &
    (pl.col("FMTCODE").is_in([1, 2])) &
    (pl.col("OPENIND").is_in(["D", "O"])) &
    (
        ((pl.col("INTPLAN") >= 340) & (pl.col("INTPLAN") <= 359)) |
        ((pl.col("INTPLAN") >= 448) & (pl.col("INTPLAN") <= 459)) |
        ((pl.col("INTPLAN") >= 461) & (pl.col("INTPLAN") <= 469)) |
        ((pl.col("INTPLAN") >= 580) & (pl.col("INTPLAN") <= 599)) |
        ((pl.col("INTPLAN") >= 660) & (pl.col("INTPLAN") <= 740))
    )
)

# Convert LMATDATE (numeric YYYYMMDD or 0) to datetime
def parse_lmatdate(val):
    try:
        s = str(int(val)).zfill(8)
        return datetime.strptime(s[:8], "%Y%m%d")
    except Exception:
        return None

pl_fd = pl_fd.with_columns([
    pl.col("LMATDATE").map_elements(parse_lmatdate, return_dtype=pl.Datetime).alias("LMATDT")
])

# -----------------------------
# STEP 3: Helper - Summarize by period
# -----------------------------
def summarise_period(df, label, condition):
    subset = df.filter(condition)
    grouped = (
        subset
        .groupby(["BRANCH", "INTPLAN"])
        .agg([
            pl.count("ACCTNO").alias("FDINO"),
            pl.sum("CURBAL").alias("FDI")
        ])
        .with_columns(pl.lit(run_date).alias("REPTDATE"))
    )
    print(f"{label}: {len(grouped)} rows summarized.")
    return grouped

# -----------------------------
# STEP 4: Period definitions
# -----------------------------
DYIBUF = summarise_period(pl_fd, "DYIBUF", pl.col("LMATDT").is_not_null())
DYIBUB = summarise_period(pl_fd, "DYIBUB", pl.col("LMATDT") < datetime(2004, 9, 4))
DYIBUA = summarise_period(pl_fd, "DYIBUA", (pl.col("LMATDT") >= datetime(2004, 9, 4)) & (pl.col("LMATDT") <= datetime(2006, 4, 15)))
DYIBUN = summarise_period(pl_fd, "DYIBUN", (pl.col("LMATDT") >= datetime(2006, 4, 16)) & (pl.col("LMATDT") <= datetime(2008, 9, 15)))
DYIBUY = summarise_period(pl_fd, "DYIBUY", pl.col("LMATDT") >= datetime(2008, 9, 16))

# -----------------------------
# STEP 5: Export results (both CSV + Parquet)
# -----------------------------
def save_outputs(df: pl.DataFrame, name: str):
    csv_path = f"{output_dir}{name}.csv"
    parquet_path = f"{output_dir}{name}.parquet"

    # Save to CSV
    df.write_csv(csv_path)

    # Convert to Arrow + save to Parquet
    pq.write_table(df.to_arrow(), parquet_path)
    print(f"✅ Saved {name} → CSV + Parquet")

for name, data in {
    "DYIBUF": DYIBUF,
    "DYIBUB": DYIBUB,
    "DYIBUA": DYIBUA,
    "DYIBUN": DYIBUN,
    "DYIBUY": DYIBUY
}.items():
    save_outputs(data, name)

print("🎯 All summaries successfully exported.")
