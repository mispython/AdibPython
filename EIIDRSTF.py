# eiidrstf.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date

# -------------------------------------------------------------------
# Step 1: Report date setup (equivalent to DATA REPTDATE in SAS)
# -------------------------------------------------------------------
today = date.today()
REPTYEAR = today.strftime("%y")   # SAS YEAR2.
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
TDATE = today.strftime("%d/%m/%Y")
RDATE = today.strftime("%Y-%m-%d")

# Weekly bucket (NOWK)
day = today.day
if 1 <= day <= 8:
    NOWK = "01"
elif 9 <= day <= 15:
    NOWK = "02"
elif 16 <= day <= 22:
    NOWK = "03"
else:
    NOWK = "04"

print(f"EIIDRSTF run for {RDATE} (Week {NOWK}, Mon={REPTMON}, Day={REPTDAY})")

# -------------------------------------------------------------------
# Step 2: Load input datasets (Parquet assumed for conversion)
# -------------------------------------------------------------------
lndly_lnnotefile = "PIBB_LNDLY_LNNOTE.parquet"
cis_custfile = "CIS_CUSTDLY.parquet"
lntr_file = f"PBB_LNTR_LNBTRAN_{REPTYEAR}{REPTMON}{NOWK}.parquet"

# Load with Polars
lndly_lnnote = pl.read_parquet(lndly_lnnotefile)
cis_cust = pl.read_parquet(cis_custfile)
lntr = pl.read_parquet(lntr_file)

# -------------------------------------------------------------------
# Step 3: Process STAFF LOANS
# -------------------------------------------------------------------
# Equivalent to DATA LNNOTE (with filters and derived columns)
lnnote = (
    lndly_lnnote.filter((pl.col("COSTCTR") != 8044) & (~pl.col("PAIDIND").is_in(["C", "P"])))
    .with_columns([
        # SAS PUT(LOANTYPE,SLTYPE.) → here assume LOANTYPE already mapped, else use dictionary
        pl.col("LOANTYPE").alias("PRODUCT"),
        pl.when(pl.col("NTBRCH") != 0).then(pl.col("NTBRCH")).otherwise(pl.col("ACCBRCH")).alias("BRANCH"),
    ])
    .select(["ACCTNO", "NOTENO", "PRODUCT", "NAME", "BRANCH"])
)

# -------------------------------------------------------------------
# Step 4: Prepare customer IC numbers
# -------------------------------------------------------------------
cust = (
    cis_cust.filter(pl.col("PRISEC") == "901")
    .with_columns(
        pl.when(pl.col("ALIAS").str.strip_chars() != "")
        .then(pl.col("ALIAS"))
        .otherwise(pl.col("TAXID"))
        .alias("ICNO")
    )
    .select(["ACCTNO", "CUSTNAME", "ICNO"])
)

# -------------------------------------------------------------------
# Step 5: Merge Loan + Customer
# -------------------------------------------------------------------
loan = (
    lnnote.join(cust, on="ACCTNO", how="left")
    .with_columns(
        pl.when(pl.col("CUSTNAME").is_null() | (pl.col("CUSTNAME").str.strip_chars() == ""))
        .then(pl.col("NAME"))
        .otherwise(pl.col("CUSTNAME"))
        .alias("CUSTNAME")
    )
)

# -------------------------------------------------------------------
# Step 6: Filter LNTR transactions
# -------------------------------------------------------------------
lntr_filt = lntr.filter(pl.col("REPTDATE") == RDATE).select(["ACCTNO", "NOTENO", "TRANCODE", "TRANAMT"])

loan = loan.join(lntr_filt, on=["ACCTNO", "NOTENO"], how="inner")

# -------------------------------------------------------------------
# Step 7: Split datasets by PROD
# -------------------------------------------------------------------
lnhl = loan.filter(pl.col("PRODUCT") == "HL")
lnhp = loan.filter(pl.col("PRODUCT") == "HP")
lnfl = loan.filter(pl.col("PRODUCT") == "FL")

# -------------------------------------------------------------------
# Step 8: Write reports (Parquet + CSV, simulate PROC PRINTTO)
# -------------------------------------------------------------------
def write_report(df: pl.DataFrame, name: str, title: str):
    if df.is_empty():
        print(f"No data for {name}")
        return
    out_parquet = f"{name}_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
    out_csv = f"{name}_{REPTYEAR}{REPTMON}{REPTDAY}.csv"
    df.write_parquet(out_parquet)
    df.write_csv(out_csv)
    print(f"{title} → {out_parquet}, {out_csv}")
    print(df.head())

write_report(lnhl, "PIBB_STAFTRAN_HL_DAILY", "Daily Transaction for Staff Housing Loan")
write_report(lnhp, "PIBB_STAFTRAN_HP_DAILY", "Daily Transaction for Staff Hire Purchase Loan")
write_report(lnfl, "PIBB_STAFTRAN_FL_DAILY", "Daily Transaction for Staff Fixed Loan")

# -------------------------------------------------------------------
# Step 9: Optional: DuckDB queries
# -------------------------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.register("loan", loan.to_arrow())

print("DuckDB summary (by PRODUCT):")
print(duckdb.sql("SELECT PRODUCT, COUNT(*) AS n, SUM(TRANAMT) AS total_amt FROM loan GROUP BY PRODUCT").df())

