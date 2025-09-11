# eibmfeex.py
# Translation of EIBMFEEX SAS job into Python (Polars + DuckDB + PyArrow)

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# --------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------
today = datetime.date.today()
dd, mm, yy = today.day, today.month, today.year

# SAS cutoff logic for report date
if dd < 8:
    reptdate = datetime.date(yy, mm, 1) - datetime.timedelta(days=1)
elif dd < 15:
    reptdate = datetime.date(yy, mm, 8)
elif dd < 22:
    reptdate = datetime.date(yy, mm, 15)
else:
    reptdate = datetime.date(yy, mm, 22)

# week selection
day = reptdate.day
if day == 8:
    sdd, wk, wk1 = 1, "1", "4"
elif day == 15:
    sdd, wk, wk1 = 9, "2", "1"
elif day == 22:
    sdd, wk, wk1 = 16, "3", "2"
else:
    sdd, wk, wk1 = 23, "4", "3"

reptmon = f"{reptdate.month:02d}"
reptyear = str(reptdate.year)[-2:]
rdate = reptdate

# month-end vs normal
filenm = "FEEFILEM" if wk == "4" else "FEEFILE"

print(f"[INFO] Report Date: {rdate}, WK={wk}, File={filenm}")

# --------------------------------------------------------------------
# INPUTS (replace with actual .parquet or .csv paths in your system)
# --------------------------------------------------------------------
# Fee transactions
fee_path = f"input/{filenm}.parquet"
# Fee plan table
bfee_path = "input/BFEEFILE.parquet"
# Account file
acct_path = "input/ACCTFILE.parquet"

df_fee = pl.read_parquet(fee_path)
df_bfee = pl.read_parquet(bfee_path)
df_acct = pl.read_parquet(acct_path)

# --------------------------------------------------------------------
# MERGE with BFEEFILE (new fee plan basis, effective date cutoff)
# --------------------------------------------------------------------
df_bfee = (
    df_bfee
    .with_columns([
        (pl.col("EFFDATE").cast(pl.Int64)).alias("EFFDATE"),
        (pl.col("BFEEBASIS").cast(pl.Int64)).alias("FEEBASIS"),
    ])
    .with_columns(
        pl.when(pl.col("EFFDATE") > 0)
          .then(pl.from_epoch(pl.col("EFFDATE"), time_unit="s"))
          .otherwise(None)
          .alias("EFFDT")
    )
    .filter(pl.col("EFFDT") < pl.lit(rdate))
    .sort(["FEEPLAN", "EFFDT"], descending=[False, True])
    .unique(subset=["FEEPLAN"], keep="first")
)

df_fee = df_fee.join(df_bfee, on="FEEPLAN", how="left")

# --------------------------------------------------------------------
# Merge with ACCTFILE (bring FEECOMBIND etc.)
# --------------------------------------------------------------------
df_acct = df_acct.select(["ACCTNO", "NOTENO", "FEECOMBIND"])
df_fee = df_fee.join(df_acct, on=["ACCTNO", "NOTENO"], how="left")

# --------------------------------------------------------------------
# Split into PBB vs PIBB by COSTCTR ranges
# --------------------------------------------------------------------
df_pbb = df_fee.filter(~((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 4999)))
df_pibb = df_fee.filter((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 4999))

# --------------------------------------------------------------------
# OUTPUT (modern formats: parquet + csv)
# --------------------------------------------------------------------
out_prefix = f"{reptyear}{reptmon}_wk{wk}"

df_pbb.write_parquet(f"output/LNFEE_PBB_{out_prefix}.parquet")
df_pbb.write_csv(f"output/LNFEE_PBB_{out_prefix}.csv")

df_pibb.write_parquet(f"output/LNFEE_PIBB_{out_prefix}.parquet")
df_pibb.write_csv(f"output/LNFEE_PIBB_{out_prefix}.csv")

print("[INFO] Outputs written to parquet and csv successfully.")

# --------------------------------------------------------------------
# OPTIONAL: Use DuckDB for further SQL style queries
# --------------------------------------------------------------------
con = duckdb.connect()
con.register("pbb", df_pbb.to_pandas())
con.register("pibb", df_pibb.to_pandas())

res = con.execute("""
    SELECT FEECAT, COUNT(*) as cnt, SUM(ASSESCUR) as total_assess
    FROM pbb
    GROUP BY FEECAT
    ORDER BY cnt DESC
""").df()
print(res)
