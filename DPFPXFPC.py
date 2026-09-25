import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
import saspy
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# CONFIGURATION
# -----------------------------
input_deposit = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDFALE"
input_dyibu   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDFALE"
output_dir    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDFALE"

# REPTDATE = yesterday
run_date = datetime.now() - timedelta(days=1)

# SAS session for writing SAS7BDAT
sas = saspy.SASsession()

# -----------------------------
# STEP 1: Read flat file (fixed-width, packed decimals)
# -----------------------------
# SAS: LRECL=1693
# Positions (1-based in SAS, 0-based in Python):
#   @3   BANKNO   PD2.   -> bytes 2:4
#   @24  REPTNO   PD3.   -> bytes 23:26
#   @27  FMTCODE  PD2.   -> bytes 26:28
#   @106 BRANCH   PD4.   -> bytes 105:109
#   @110 ACCTNO   PD6.   -> bytes 109:115
#   @155 OPENIND  $1.    -> byte  154:155
#   @156 CURBAL   PD6.2  -> bytes 155:161
#   @208 INTPLAN  PD2.   -> bytes 207:209
#   @392 LMATDATE PD6.   -> bytes 391:397

def read_packed_decimal(raw: bytes, scale: int = 0) -> float:
    """Decode a packed decimal (COMP-3) field."""
    # Last nibble is sign; remaining nibbles are digits
    digits = ""
    for i, b in enumerate(raw):
        high = (b >> 4) & 0x0F
        low  = b & 0x0F
        if i == len(raw) - 1:
            # Last byte: high nibble is a digit, low nibble is sign
            digits += str(high)
            sign = low
        else:
            digits += str(high) + str(low)
    val = int(digits) if digits else 0
    if sign in (0x0D, 0x0B):  # negative
        val = -val
    return val / (10 ** scale)

def read_flat_file(path: str) -> pl.DataFrame:
    records = []
    with open(path, "rb") as f:
        for line in f:
            if len(line) < 397:
                continue
            bankno   = read_packed_decimal(line[2:4])
            reptno   = read_packed_decimal(line[23:26])
            fmtcode  = read_packed_decimal(line[26:28])
            branch   = read_packed_decimal(line[105:109])
            acctno   = read_packed_decimal(line[109:115])
            openind  = line[154:155].decode("ascii", errors="ignore")
            curbal   = read_packed_decimal(line[155:161], scale=2)
            intplan  = read_packed_decimal(line[207:209])
            lmatdate = read_packed_decimal(line[391:397])
            records.append({
                "BANKNO": bankno, "REPTNO": reptno, "FMTCODE": fmtcode,
                "BRANCH": branch, "ACCTNO": acctno, "OPENIND": openind,
                "CURBAL": curbal, "INTPLAN": intplan, "LMATDATE": lmatdate,
            })
    return pl.DataFrame(records)

pl_fd = read_flat_file(input_deposit)

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
        if val is None or val == 0:
            return None
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
def summarise_period(df: pl.DataFrame, label: str, condition) -> pl.DataFrame:
    subset = df.filter(condition)
    grouped = (
        subset
        .group_by(["BRANCH", "INTPLAN"])
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
DYIBUA = summarise_period(pl_fd, "DYIBUA",
                          (pl.col("LMATDT") >= datetime(2004, 9, 4)) &
                          (pl.col("LMATDT") <= datetime(2006, 4, 15)))
DYIBUN = summarise_period(pl_fd, "DYIBUN",
                          (pl.col("LMATDT") >= datetime(2006, 4, 16)) &
                          (pl.col("LMATDT") <= datetime(2008, 9, 15)))
DYIBUY = summarise_period(pl_fd, "DYIBUY", pl.col("LMATDT") >= datetime(2008, 9, 16))

# -----------------------------
# STEP 5: Export results as SAS7BDAT (lowercase columns)
# -----------------------------
def save_sas7bdat(df: pl.DataFrame, name: str):
    pdf = df.to_pandas()
    # Lowercase all column names
    pdf.columns = [c.lower() for c in pdf.columns]

    sas_path = f"{output_dir}/{name}.sas7bdat"

    # Option A: write locally with pyreadstat
    pyreadstat.write_sas7bdat(pdf, sas_path, file_format="sas7bdat")
    print(f"Saved {name} -> {sas_path}")

    # Option B: write via saspy (upload to SAS session)
    # sas.df2sd(pdf, table=name, libref="WORK")

for name, data in {
    "DYIBUF": DYIBUF,
    "DYIBUB": DYIBUB,
    "DYIBUA": DYIBUA,
    "DYIBUN": DYIBUN,
    "DYIBUY": DYIBUY,
}.items():
    save_sas7bdat(data, name)

print("All summaries successfully exported as SAS7BDAT.")
