# eibdrstf.py
import polars as pl
import duckdb
from datetime import date

# =======================================================
# Step 1: Reporting Date Setup (like SAS DATA REPTDATE)
# =======================================================
today = date.today()
REPTYEAR = str(today.year)[-2:]     # YY
REPTMON = f"{today.month:02d}"      # 2-digit month
REPTDAY = f"{today.day:02d}"        # 2-digit day
RDATE = today.strftime("%Y-%m-%d")  # YYYY-MM-DD
TDATE = today.strftime("%d-%m-%Y")  # dd-mm-yyyy

if 1 <= today.day <= 8:
    NOWK = "01"
elif 9 <= today.day <= 15:
    NOWK = "02"
elif 16 <= today.day <= 22:
    NOWK = "03"
else:
    NOWK = "04"

print(f"Report Date: {TDATE}, Week={NOWK}, Month={REPTMON}, Year={REPTYEAR}")

# =======================================================
# Step 2: Load Source Datasets
# =======================================================
# Replace with actual file locations / formats
lndly_lnnote = pl.read_parquet("LNDLY_LNNOTE.parquet")
cis_custdly = pl.read_parquet("CIS_CUSTDLY.parquet")
lntr = pl.read_parquet(f"LNTR_LNBTRAN_{REPTYEAR}{REPTMON}{NOWK}.parquet")
dpdly_current = pl.read_parquet("DPDLY_CURRENT.parquet")
dptr = pl.read_parquet(f"DPTR_DPBTRAN_{REPTYEAR}{REPTMON}{NOWK}.parquet")

# =======================================================
# Step 3: Staff Loans Processing
# =======================================================
# Filter loan notes
pivb = list(range(62,71)) + [106,107,108]

lnnote = (
    lndly_lnnote
    .filter((pl.col("COSTCTR") != 8044) & (~pl.col("PAIDIND").is_in(["C","P"])))
    .with_columns([
        pl.col("LOANTYPE").cast(pl.Utf8).alias("PRODUCT"),
        pl.when(pl.col("NTBRCH") != 0)
          .then(pl.col("NTBRCH"))
          .otherwise(pl.col("ACCBRCH"))
          .alias("BRANCH")
    ])
    .with_columns([
        # Map loan type to product group (HL/HP/FL) using custom logic
        pl.when(pl.col("PRODUCT").is_in(["HL","HP","FL"]))
          .then(pl.col("PRODUCT"))
          .alias("PROD")
    ])
    .filter(~pl.col("LOANTYPE").cast(int).is_in(pivb))
    .select(["ACCTNO","NOTENO","PRODUCT","NAME","BRANCH","PROD"])
    .sort(["ACCTNO","NOTENO"])
)

# Customer info
cust = (
    cis_custdly
    .filter(pl.col("PRISEC") == "901")
    .with_columns(
        pl.when(pl.col("ALIAS").str.strip_chars() != "")
          .then(pl.col("ALIAS"))
          .otherwise(pl.col("TAXID"))
          .alias("ICNO")
    )
    .select(["ACCTNO","CUSTNAME","ICNO"])
    .sort("ACCTNO")
)

# Merge loans with customers
loan = (
    lnnote.join(cust, on="ACCTNO", how="left")
    .with_columns([
        pl.when(pl.col("CUSTNAME").is_null() | (pl.col("CUSTNAME")==""))
          .then(pl.col("NAME"))
          .otherwise(pl.col("CUSTNAME"))
          .alias("CUSTNAME")
    ])
)

# Filter loan transactions
lntr_f = (
    lntr.filter(pl.col("REPTDATE") == RDATE)
        .select(["ACCTNO","NOTENO","TRANCODE","TRANAMT"])
        .sort(["ACCTNO","NOTENO"])
)

loan = loan.join(lntr_f, on=["ACCTNO","NOTENO"], how="inner").sort(["ICNO","ACCTNO"])

# Split HL / HP / FL datasets
lnhl = loan.filter(pl.col("PROD")=="HL")
lnhp = loan.filter(pl.col("PROD")=="HP")
lnfl = loan.filter(pl.col("PROD")=="FL")

# Export reports
lnhl.write_csv("HLTXT.csv")
lnhp.write_csv("HPTXT.csv")
lnfl.write_csv("FLTXT.csv")

# =======================================================
# Step 4: Staff Sharelink Investment
# =======================================================
current = (
    dpdly_current
    .filter((pl.col("PRODUCT")==133) & (~pl.col("OPENIND").is_in(["B","C","P"])))
    .filter(pl.col("NAME").str.ends_with("*"))
    .select(["ACCTNO","PRODUCT","NAME","BRANCH"])
    .sort("ACCTNO")
)

depo = (
    current.join(cust, on="ACCTNO", how="left")
    .with_columns([
        pl.when(pl.col("CUSTNAME").is_null() | (pl.col("CUSTNAME")==""))
          .then(pl.col("NAME"))
          .otherwise(pl.col("CUSTNAME"))
          .alias("CUSTNAME")
    ])
)

dptr_f = (
    dptr.filter(pl.col("REPTDATE")==RDATE)
        .select(["ACCTNO","TRANCODE","TRANAMT"])
        .sort("ACCTNO")
)

dpsi = depo.join(dptr_f, on="ACCTNO", how="inner").sort(["ICNO","ACCTNO"])

# Export report
dpsi.write_csv("SITXT.csv")

# =======================================================
# Step 5: Optional DuckDB for ad-hoc queries
# =======================================================
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.register("loan_hl", lnhl.to_arrow())
duckdb.register("loan_hp", lnhp.to_arrow())
duckdb.register("loan_fl", lnfl.to_arrow())
duckdb.register("loan_si", dpsi.to_arrow())

print("DuckDB counts:")
print(duckdb.sql("SELECT 'HL' AS PROD, COUNT(*) FROM loan_hl UNION ALL \
                  SELECT 'HP', COUNT(*) FROM loan_hp UNION ALL \
                  SELECT 'FL', COUNT(*) FROM loan_fl UNION ALL \
                  SELECT 'SI', COUNT(*) FROM loan_si").fetchall())
