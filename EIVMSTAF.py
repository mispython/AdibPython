# eivmstaf.py
# Translation of EIVMSTAF.sas job into Python (Polars + DuckDB + PyArrow)

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import datetime

# --------------------------------------------------------------------
# DATE LOGIC (similar to SAS)
# --------------------------------------------------------------------
today = datetime.date.today()
reptdate = today  # SAS used MNILN.REPTDATE, here assume daily snapshot
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
# INPUT PATHS (replace with your actual datasets / exports)
# --------------------------------------------------------------------
lnintgr_path = "input/LNINTGR.parquet"
lnnote_path  = "input/LNNOTE.parquet"
lncomm_path  = "input/LNCOMM.parquet"

# --------------------------------------------------------------------
# LOAD WITH PYARROW (then convert to Polars)
# --------------------------------------------------------------------
def load_arrow_to_polars(path: str) -> pl.DataFrame:
    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table()
    return pl.from_arrow(table)

df_lnintgr = load_arrow_to_polars(lnintgr_path)
df_lnnote  = load_arrow_to_polars(lnnote_path)
df_lncomm  = load_arrow_to_polars(lncomm_path)

# --------------------------------------------------------------------
# STAFF PAYMENTS (LNINTGR)
# --------------------------------------------------------------------
df_staff = (
    df_lnintgr
    .select(["ACCTNO", "NOTENO", "PAYAMT"])
    .sort(["ACCTNO", "NOTENO"])
)

# --------------------------------------------------------------------
# LOAN NOTE + COMM merge, derive APPRLIMT
# --------------------------------------------------------------------
df_lnnote = df_lnnote.filter(
    pl.col("LOANTYPE").is_in([62,70,71,72,73,74,75,76,77,78,106,107,108])
)
df_lncomm = df_lncomm.sort(["ACCTNO", "COMMNO"])

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
# FINAL LOAN DATA (equivalent to SAS DATA LOAN)
# --------------------------------------------------------------------
df_loan = df_lnnote.select([
    "LOANTYPE","NTBRCH","ORGTYPE","ACCTNO","CURBAL","REVERSED","NOTENO","NAME",
    "APPRLIMT","ISSDTE","PAIDIND","BLDATE","BILPAY","PAYAMT","INTRATE","STAFFNO",
    "SENDNBL","ORGBAL","LASTTRAN","LSTTRNAM","LSTTRNCD","NOOFAC","PAYEFF"
])

df_loan = df_loan.filter(
    (pl.col("ORGTYPE") == "N") |
    ((pl.col("SENDNBL") == "D") & (pl.col("BLDATE") > 0))
)

# --------------------------------------------------------------------
# EXCEPTION REPORT PREP (LNRPT3A and LNRPT3B)
# --------------------------------------------------------------------
df_lnexpt = df_loan.filter(pl.col("CURBAL") > 0)

df_rpt3 = df_lnexpt.join(
    df_staff, on=["ACCTNO","NOTENO"], how="outer", suffix="_staff"
)

# A only (in MNILN but not in LNINTGR)
df_rpt3a = df_rpt3.filter(
    (pl.col("PAYAMT_staff").is_null()) & (pl.col("ACCTNO").is_not_null())
)

# B only (in LNINTGR but not in MNILN)
df_rpt3b = df_rpt3.filter(
    (pl.col("PAYAMT").is_null()) & (pl.col("PAYAMT_staff").is_not_null())
)

# --------------------------------------------------------------------
# REPORTING WITH DUCKDB (PROC REPORT equivalent)
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
# OUTPUTS: PARQUET + CSV (via Polars / PyArrow)
# --------------------------------------------------------------------
out_prefix = f"{reptyear}{reptmon}_wk{wk}"

# Save via Polars
report_df.write_parquet(f"output/EIVMSTAF_report_{out_prefix}.parquet")
report_df.write_csv(f"output/EIVMSTAF_report_{out_prefix}.csv")

# Save also via PyArrow for full Arrow pipeline compatibility
arrow_table = report_df.to_arrow()
pa.parqu
