# EIMNFISS_FISS_MONTHLY_EXTRACTION.py
import duckdb
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime, timedelta

# -----------------------------
# Step 1: Derive REPTDATE (last day of previous month)
# -----------------------------
today = datetime.today()
first_of_month = today.replace(day=1)
reptdate = first_of_month - timedelta(days=1)

reptday = reptdate.strftime("%d")
reptmon = reptdate.strftime("%m")
reptyear = reptdate.strftime("%Y")

# Work out week code logic (WK, WK1, WK2, WK3)
day = reptdate.day
if day == 8:
    wk, wk1 = "1", "4"
elif day == 15:
    wk, wk1 = "2", "1"
elif day == 22:
    wk, wk1 = "3", "2"
else:
    wk, wk1, wk2, wk3 = "4", "3", "2", "1"

print(f"REPTDATE = {reptdate}, WK={wk}, MON={reptmon}, YEAR={reptyear}")

# -----------------------------
# Step 2: Load source datasets (assuming converted to Parquet beforehand)
# -----------------------------
# These replace SAS libraries LN, ILN, BT, IBT
ln_util = pl.read_parquet(f"LN_B80510_{reptyear}{reptmon}.parquet")
ln_unutil = pl.read_parquet(f"LN_B80200_{reptyear}{reptmon}.parquet")

iln_util = pl.read_parquet(f"ILN_B80510_{reptyear}{reptmon}.parquet")
iln_unutil = pl.read_parquet(f"ILN_B80200_{reptyear}{reptmon}.parquet")

bt_util = pl.read_parquet("BT_B80510.parquet")
bt_unutil = pl.read_parquet("BT_B80200.parquet")

ibt_util = pl.read_parquet("IBT_B80510.parquet")
ibt_unutil = pl.read_parquet("IBT_B80200.parquet")

# -----------------------------
# Step 3: Register into DuckDB for SQL joins / transformations
# -----------------------------
con = duckdb.connect()
con.register("LN_UTIL", ln_util.to_arrow())
con.register("LN_UNUTIL", ln_unutil.to_arrow())
con.register("ILN_UTIL", iln_util.to_arrow())
con.register("ILN_UNUTIL", iln_unutil.to_arrow())
con.register("BT_UTIL", bt_util.to_arrow())
con.register("BT_UNUTIL", bt_unutil.to_arrow())
con.register("IBT_UTIL", ibt_util.to_arrow())
con.register("IBT_UNUTIL", ibt_unutil.to_arrow())

# -----------------------------
# Step 4: Example query (like SAS DATA step copies)
# -----------------------------
# Just validate row counts
print("LN_UTIL rows:", con.execute("SELECT COUNT(*) FROM LN_UTIL").fetchone())
print("ILN_UTIL rows:", con.execute("SELECT COUNT(*) FROM ILN_UTIL").fetchone())
print("BT_UTIL rows:", con.execute("SELECT COUNT(*) FROM BT_UTIL").fetchone())
print("IBT_UTIL rows:", con.execute("SELECT COUNT(*) FROM IBT_UTIL").fetchone())

# -----------------------------
# Step 5: Save all temp datasets as Parquet (instead of TEMP library in SAS)
# -----------------------------
output_dir = "NFISS_OUTPUT"
ln_util.write_parquet(f"{output_dir}/LN_UTIL_{reptyear}{reptmon}.parquet")
ln_unutil.write_parquet(f"{output_dir}/LN_UNUTIL_{reptyear}{reptmon}.parquet")
iln_util.write_parquet(f"{output_dir}/ILN_UTIL_{reptyear}{reptmon}.parquet")
iln_unutil.write_parquet(f"{output_dir}/ILN_UNUTIL_{reptyear}{reptmon}.parquet")
bt_util.write_parquet(f"{output_dir}/BT_UTIL_{reptyear}{reptmon}.parquet")
bt_unutil.write_parquet(f"{output_dir}/BT_UNUTIL_{reptyear}{reptmon}.parquet")
ibt_util.write_parquet(f"{output_dir}/IBT_UTIL_{reptyear}{reptmon}.parquet")
ibt_unutil.write_parquet(f"{output_dir}/IBT_UNUTIL_{reptyear}{reptmon}.parquet")

print(f"✅ All NFISS extracts saved in {output_dir}/")
