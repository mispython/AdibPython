import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import datetime as dt
import sys
import os

# -------------------------
# CONFIG / PATHS
# -------------------------
loan_file = "/sasdata/SAP.PBB.MNILN.DAILY.parquet"
walktxt_file = "/sasdata/FDP.APPL.DAILY.LOAN.txt"
walkdtl_file = "/sasdata/FDP.APPL.DAILYFCY.LOAN.txt"
prewalk_file = "/sasdata/SAP.PBB.DAILY.WALKER.parquet"   # previous day
output_walker_file = "/sasdata/SAP.PBB.DAILY.WALKER_out.parquet"

# -------------------------
# STEP 1: REPTDATE
# -------------------------
loan = pl.read_parquet(loan_file)
reptdate = loan.select("REPTDATE").item()

reptdate_dt = dt.datetime.strptime(str(reptdate), "%Y%m%d")
prevdate_dt = reptdate_dt - dt.timedelta(days=1)

REPTYEAR = reptdate_dt.strftime("%y")
REPTMON = reptdate_dt.strftime("%m")
REPTDAY = reptdate_dt.strftime("%d")

PREVYEAR = prevdate_dt.strftime("%y")
PREVMON = prevdate_dt.strftime("%m")
PREVDAY = prevdate_dt.strftime("%d")

RDATE = reptdate_dt.strftime("%d%m%y")

# -------------------------
# STEP 2: Read WALKTXT (get WDTE)
# -------------------------
walktxt_head = pl.read_csv(
    walktxt_file,
    has_header=False,
    new_columns=["RAW"],
    n_rows=1
)

dd = int(walktxt_head[0, "RAW"][1:3])
mm = int(walktxt_head[0, "RAW"][4:6])
yy = int(walktxt_head[0, "RAW"][7:9])
WDTE = dt.date(2000 + yy, mm, dd).strftime("%d%m%y")

# -------------------------
# STEP 3: Read WALKDTL (get XDTE)
# -------------------------
walkdtl_head = pl.read_csv(
    walkdtl_file,
    has_header=False,
    new_columns=["RAW"],
    n_rows=1
)

dd = int(walkdtl_head[0, "RAW"][1:3])
mm = int(walkdtl_head[0, "RAW"][4:6])
yy = int(walkdtl_head[0, "RAW"][7:9])
XDTE = dt.date(2000 + yy, mm, dd).strftime("%d%m%y")

# -------------------------
# MACRO PROCESS equivalent
# -------------------------
if WDTE == RDATE and XDTE == RDATE:

    # -------------------------
    # STEP 4: WALKDTL data
    # -------------------------
    walkdtl = pl.read_fwf(
        walkdtl_file,
        colspecs=[(0,1),(1,14),(20,24),(21,24),(27,30),(36,42),(49,71)],
        infer_schema_length=0,
        new_columns=["INDICODE","PROD","ORIBR","NTBRCH","ACK","CURR","CURBAL"]
    )

    walkdtl = walkdtl.filter(pl.col("INDICODE") == "+").drop("INDICODE")

    # -------------------------
    # STEP 5: WALKTXT data
    # -------------------------
    walktxt = pl.read_fwf(
        walktxt_file,
        colspecs=[(0,1),(1,5),(2,5),(6,9),(19,32)],
        infer_schema_length=0,
        new_columns=["INDICODE","ORIBR","NTBRCH","PROD","CURBAL"]
    )

    walktxt = walktxt.filter(pl.col("INDICODE") == "+").drop("INDICODE")

    # Loan type classification
    walktxt = walktxt.with_columns([
        pl.when(pl.col("PROD") == "ML34170").then("FS")
          .when(pl.col("PROD") == "S-FCYSYNLN").then("SL")
          .when(pl.col("PROD") == "S-FCYRC").then("SR")
          .when(pl.col("PROD") == "ZIIPSUSP").then("ZS")
          .when(pl.col("PROD").str.slice(0,5) == "34200").then("CC")
          .when(pl.col("PROD").str.slice(0,5) == "54120").then("HC")
          .otherwise(None)
          .alias("LNTYPE"),
        pl.when(pl.col("PROD") == "ZIIPSUSP").then(pl.col("CURBAL")).otherwise(0).alias("ZSBAL")
    ])

    # ORIBR handling (stub for %INC NPLNTB1)
    walktxt = walktxt.with_columns([
        pl.when(pl.col("ORIBR").str.slice(0,1).is_in(["3","I"]))
          .then("I")
          .otherwise(None)
          .alias("AMTIND")
    ])

    # -------------------------
    # STEP 6: PREVIOUS WALKER
    # -------------------------
    prewk = pl.read_parquet(prewalk_file)
    prewk = prewk.rename({"BALANCE":"PREBAL"}).select(["ORIBR","LNTYPE","PREBAL"]).filter(pl.col("LNTYPE")=="ZS")

    # -------------------------
    # STEP 7: Merge PREWK and WK
    # -------------------------
    wk = walktxt
    merged = wk.join(prewk, on=["ORIBR","LNTYPE"], how="left")

    merged = merged.with_columns([
        pl.when((pl.col("LNTYPE")=="ZS") & (pl.col("CURBAL")==0))
          .then(pl.col("PREBAL"))
          .otherwise(pl.col("CURBAL"))
          .alias("BALANCE")
    ])

    # -------------------------
    # STEP 8: Save final WALKER dataset (Polars + PyArrow)
    # -------------------------
    merged.write_parquet(output_walker_file)
    merged.write_csv(output_walker_file.replace(".parquet",".csv"))

    # PyArrow Table
    arrow_table = merged.to_arrow()
    pq.write_table(arrow_table, output_walker_file.replace(".parquet","_arrow.parquet"))

    # -------------------------
    # STEP 9: Query with DuckDB
    # -------------------------
    con = duckdb.connect()
    con.register("walker", arrow_table)
    print("DuckDB check → COUNT rows:")
    print(con.execute("SELECT COUNT(*) FROM walker").fetchall())
    print("DuckDB sample → TOP 5:")
    print(con.execute("SELECT * FROM walker LIMIT 5").fetchdf())

    print("✅ Walker job complete")

else:
    if XDTE != RDATE:
        print(f"THE WALKER - FDP.APPL.DAILYFCY.LOAN IS NOT DATED {RDATE}")
    if WDTE != RDATE:
        print(f"THE WALKER - FDP.APPL.DAILY.LOAN IS NOT DATED {RDATE}")
    print("THE WALKER JOB - FDPGLRDL; JOB IS NOT DONE !!")
    sys.exit(77)
