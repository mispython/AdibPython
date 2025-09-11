# eibmstaf.py
# Conversion of EIBMSTAF.sas to Python (Polars + DuckDB + PyArrow)

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import duckdb
import datetime
import os

# --------------------------------------------------------------------
# DATE LOGIC
# --------------------------------------------------------------------
today = datetime.date.today()
reptdate = today   # SAS used MNILN.REPTDATE; here assume today’s snapshot
day = reptdate.day

if day == 8:
    sdd, wk = 1, "1"
elif day == 15:
    sdd, wk = 9, "2"
elif day == 22:
    sdd, wk = 16, "3"
else:
    sdd, wk = 23, "4"

reptmon = f"{reptdate.month:02d}"
reptyear = reptdate.year
rdate = reptdate.strftime("%d/%m/%Y")

print(f"[INFO] Report Date: {rdate}, WK={wk}, MON={reptmon}, YEAR={reptyear}")

# --------------------------------------------------------------------
# INPUT PATHS (replace with actual source exports)
# --------------------------------------------------------------------
lnintgr_path = "input/LNINTGR.parquet"
lnnote_mni   = "input/MNILN_LNNOTE.parquet"
lnnote_imni  = "input/IMNILN_LNNOTE.parquet"
lncomm_mni   = "input/MNILN_LNCOMM.parquet"
lncomm_imni  = "input/IMNILN_LNCOMM.parquet"

# --------------------------------------------------------------------
# LOAD HELPERS
# --------------------------------------------------------------------
def load_arrow_to_polars(path: str) -> pl.DataFrame:
    dataset = ds.dataset(path, format="parquet")
    return pl.from_arrow(dataset.to_table())

df_lnintgr = load_arrow_to_polars(lnintgr_path)
df_lnnote  = pl.concat([
    load_arrow_to_polars(lnnote_mni),
    load_arrow_to_polars(lnnote_imni)
])
df_lncomm  = pl.concat([
    load_arrow_to_polars(lncomm_mni),
    load_arrow_to_polars(lncomm_imni)
])

# --------------------------------------------------------------------
# LNSTAFF (from LNINTGR)
# --------------------------------------------------------------------
df_lnstaff = (
    df_lnintgr
    .select(["ACCTNO","NOTENO","PAYAMT"])
    .sort(["ACCTNO","NOTENO"])
)

# --------------------------------------------------------------------
# FILTER LNNOTE LOANTYPE <= 61 or in (100,102,103,104,105)
# --------------------------------------------------------------------
df_lnnote = df_lnnote.filter(
    (pl.col("LOANTYPE") <= 61) | (pl.col("LOANTYPE").is_in([100,102,103,104,105]))
)

df_lncomm = df_lncomm.sort(["ACCTNO","COMMNO"])
df_lnnote = df_lnnote.join(df_lncomm, on=["ACCTNO","COMMNO"], how="left")

df_lnnote = df_lnnote.with_columns(
    pl.when(pl.col("COMMNO") > 0)
      .then(
        pl.when(pl.col("REVOVLI") == "N")
          .then(pl.col("CORGAMT"))
          .otherwise(pl.col("CCURAMT"))
      )
      .otherwise(pl.col("ORGBAL"))
      .alias("APPRLIMT")
)

# --------------------------------------------------------------------
# FINAL LOAN DATA (apply filters like SAS DATA LOAN step)
# --------------------------------------------------------------------
df_loan = df_lnnote.with_columns([
    pl.lit(1).alias("NOOFAC")
])

df_loan = df_loan.filter(
    (
      ((pl.col("LASTTRAN").dt.month() == int(reptmon)) &
       (pl.col("LASTTRAN").dt.year() == reptyear) &
       pl.col("LSTTRNCD").is_in([310,650,652,610,614,615,660)))
      |
      (pl.col("PAIDIND").is_in(["P","C"]).not_() | (pl.col("REVERSED") != "Y"))
    )
    &
    ((pl.col("SENDNBL") == "D") & (pl.col("BLDATE") > 0) | (pl.col("ORGTYPE") == "N"))
    &
    (pl.col("ORGTYPE") != "S")
)

df_loan = df_loan.sort(["ACCTNO","NOTENO"])

# --------------------------------------------------------------------
# EXCEPTION REPORT LOGIC
# --------------------------------------------------------------------
df_lnexpt = df_loan.filter(pl.col("CURBAL") > 0)

df_rpt3 = df_lnexpt.join(df_lnstaff, on=["ACCTNO","NOTENO"], how="outer", suffix="_staff")

df_rpt3a = df_rpt3.filter((pl.col("PAYAMT_staff").is_null()) & (pl.col("ACCTNO").is_not_null()))
df_rpt3b = df_rpt3.filter((pl.col("PAYAMT").is_null()) & (pl.col("PAYAMT_staff").is_not_null()))

# --------------------------------------------------------------------
# REPORTING (PROC REPORT → DuckDB)
# --------------------------------------------------------------------
con = duckdb.connect()
con.register("rpt3a", df_rpt3a.to_arrow())

query = """
SELECT 
  LOANTYPE,
  NTBRCH,
  ORGTYPE,
  STAFFNO,
  ACCTNO,
  NOTENO,
  NAME,
  SUM(APPRLIMT) AS APPROVED_LIMIT,
  SUM(PAYAMT) AS PAYMENT_AMOUNT,
  COUNT(*) AS NOOFAC
FROM rpt3a
GROUP BY LOANTYPE, NTBRCH, ORGTYPE, STAFFNO, ACCTNO, NOTENO, NAME
ORDER BY LOANTYPE, NTBRCH
"""
report_df = pl.from_arrow(con.execute(query).arrow())

# --------------------------------------------------------------------
# OUTPUT DIRECTORY
# --------------------------------------------------------------------
os.makedirs("output", exist_ok=True)
out_prefix = f"{reptyear}{reptmon}_wk{wk}"

# Save reports
report_df.write_parquet(f"output/EIBMSTAF_report_{out_prefix}.parquet")
report_df.write_csv(f"output/EIBMSTAF_report_{out_prefix}.csv")

# Arrow-native output
pa.parquet.write_table(report_df.to_arrow(), f"output/EIBMSTAF_report_{out_prefix}_arrow.parquet")

print(f"[INFO] EIBMSTAF Exception Report generated for {rdate}")
