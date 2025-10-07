"""
Program Name : EIBDRPGX_FD_DEPOSIT_PARSER.py
Purpose      : Parse fixed deposit records from mainframe-style DEPOSIT file,
               transform maturity and customer codes, and output FD, IFD.FD,
               IFD.TIA, and FD.REPTDATE datasets in CSV + Parquet format.

"""

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
INPUT_PARQUET = "DEPOSIT.parquet"      # Input dataset (converted from EBCDIC/PD6 etc.)
OUTPUT_DIR = "./output"

# ----------------------------------------------------------------------------
# STEP 1 - READ INPUT FILE (equivalent to INFILE DEPOSIT)
# ----------------------------------------------------------------------------
# Assuming the input parquet already has columns aligned to the parsed SAS fields

df = pl.read_parquet(INPUT_PARQUET)

# If needed, filter first record to derive REPTDATE
first_row = df.head(1)

# Equivalent of @106 TBDATE PD6. → Extract REPTDATE from TBDATE
tbd = int(first_row["TBDATE"][0])
tbd_str = f"{tbd:011d}"[:8]  # Take first 8 digits
reptdate = datetime.strptime(tbd_str, "%m%d%Y")
reptdatx = reptdate + timedelta(days=1)
XDATE = int(reptdatx.strftime("%y%j"))  # SAS Z5. date format (YYDDD)
print(f"REPTDATE = {reptdate.date()}, XDATE = {XDATE}")

# ----------------------------------------------------------------------------
# STEP 2 - FILTER FD4001 records
# ----------------------------------------------------------------------------
# Equivalent SAS conditions:
# IF BANKNO = 33 AND REPTNO = 4001 AND FMTCODE IN (1,2)

fd4001 = (
    df.filter((pl.col("BANKNO") == 33) & 
              (pl.col("REPTNO") == 4001) &
              (pl.col("FMTCODE").is_in([1, 2])))
)

# ----------------------------------------------------------------------------
# STEP 3 - Adjust Customer Code (CUSTCD logic)
# ----------------------------------------------------------------------------
def normalize_custcd(val: int) -> int:
    if 0 < val <= 99:
        return val
    elif 100 <= val <= 999:
        return int(str(val)[1:3])
    elif 1000 <= val <= 9999:
        return int(str(val)[2:4])
    elif 10000 <= val <= 99999:
        return int(str(val)[3:5])
    return 0

fd4001 = fd4001.with_columns(
    pl.col("CUSTCD").map_elements(normalize_custcd, return_dtype=pl.Int32)
)

# ----------------------------------------------------------------------------
# STEP 4 - Handle LMATDATE → MATDATE (renewal = 'N' & same as XDATE)
# ----------------------------------------------------------------------------
def derive_matdate(lmatdate: int, renewal: str) -> str | None:
    if lmatdate > 0:
        lmat_str = f"{lmatdate:011d}"[:8]
        d = datetime.strptime(lmat_str, "%m%d%Y")
        if renewal == 'N' and int(d.strftime("%y%j")) == XDATE:
            return d.strftime("%Y%m%d")
    return None

fd4001 = fd4001.with_columns(
    pl.struct(["LMATDATE", "RENEWAL"]).map_elements(
        lambda s: derive_matdate(s["LMATDATE"], s["RENEWAL"]), return_dtype=pl.Utf8
    ).alias("MATDATE")
)

# ----------------------------------------------------------------------------
# STEP 5 - Add AMTIND (FDDENOM format mapping)
# ----------------------------------------------------------------------------
# Dummy mapping (since SAS uses FDDENOM. format, supply mapping manually)
fd_denom_map = {
    101: "SAVINGS",
    102: "CURRENT",
    201: "FD_SHORT",
    202: "FD_LONG",
}
fd4001 = fd4001.with_columns(
    pl.col("INTPLAN").map_elements(lambda x: fd_denom_map.get(x, "UNKNOWN"), return_dtype=pl.Utf8).alias("AMTIND")
)

# ----------------------------------------------------------------------------
# STEP 6 - Split outputs into FD, IFD.TIA, IFD.FD
# ----------------------------------------------------------------------------
def classify_outputs(row):
    costctr = row["COSTCTR"]
    accttype = row["ACCTTYPE"]
    if 3000 <= costctr <= 4999:
        if accttype == 318:
            return "IFD_TIA"
        else:
            return "IFD_FD"
    else:
        return "FD_FD"

# Apply classification
fd4001 = fd4001.with_columns(
    pl.struct(["COSTCTR", "ACCTTYPE"]).map_elements(classify_outputs, return_dtype=pl.Utf8).alias("OUTPUT_CLASS")
)

fd_fd = fd4001.filter(pl.col("OUTPUT_CLASS") == "FD_FD")
ifd_fd = fd4001.filter(pl.col("OUTPUT_CLASS") == "IFD_FD")
ifd_tia = fd4001.filter(pl.col("OUTPUT_CLASS") == "IFD_TIA")

# ----------------------------------------------------------------------------
# STEP 7 - Export outputs to CSV + Parquet
# ----------------------------------------------------------------------------
fd_fd.write_csv(f"{OUTPUT_DIR}/FD_FD.csv")
ifd_fd.write_csv(f"{OUTPUT_DIR}/IFD_FD.csv")
ifd_tia.write_csv(f"{OUTPUT_DIR}/IFD_TIA.csv")

fd_fd.write_parquet(f"{OUTPUT_DIR}/FD_FD.parquet")
ifd_fd.write_parquet(f"{OUTPUT_DIR}/IFD_FD.parquet")
ifd_tia.write_parquet(f"{OUTPUT_DIR}/IFD_TIA.parquet")

# ----------------------------------------------------------------------------
# STEP 8 - Generate FD.REPTDATE equivalent
# ----------------------------------------------------------------------------
reptdate_tbl = pl.DataFrame(
    {"REPTDATE": [reptdate.strftime("%Y-%m-%d")], "XDATE": [XDATE]}
)
reptdate_tbl.write_csv(f"{OUTPUT_DIR}/FD_REPTDATE.csv")
reptdate_tbl.write_parquet(f"{OUTPUT_DIR}/FD_REPTDATE.parquet")

# ----------------------------------------------------------------------------
# STEP 9 - Optional: Use DuckDB for verification or join operations
# ----------------------------------------------------------------------------
con = duckdb.connect(database=':memory:')
con.register("fd_fd", fd_fd.to_arrow())
result = con.execute("SELECT COUNT(*) AS FD_COUNT FROM fd_fd").fetch_df()
print(result)

print(" Conversion completed successfully.")
