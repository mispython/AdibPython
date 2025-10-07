# ===============================================================
#  PROGRAM ID  : EIBDCA01_CA_NEGATIVE_BALANCE_REPORT.py
#  DESCRIPTION : Daily listing of current account customers
#                without OD facility and negative balances ≥ RM500,000
#  TECHNOLOGY  : DuckDB + PyArrow + Polars (No Pandas)
# ===============================================================

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# ===============================================================
# 1. READ MAIN DATASETS
# ===============================================================
# Input files should be provided as Parquet or CSV.
# You can replace paths as needed.
dptrbl = pl.read_parquet("DPTRBL.parquet")
brhfile = pl.read_parquet("BRHFILE.parquet")
iptrblp = pl.read_parquet("IPTRBLP_CURRENT.parquet")
dptrblp = pl.read_parquet("DPTRBLP_CURRENT.parquet")

# ===============================================================
# 2. INITIALIZE DUCKDB CONNECTION
# ===============================================================
con = duckdb.connect()

# Register Polars DataFrames as DuckDB relations
con.register("dptrbl", dptrbl.to_arrow())
con.register("brhfile", brhfile.to_arrow())
con.register("iptrblp", iptrblp.to_arrow())
con.register("dptrblp", dptrblp.to_arrow())

# ===============================================================
# 3. EXTRACT REPORT DATE
# ===============================================================
first_date = str(dptrbl["TBDATE"][0])[:8]
report_date = datetime.strptime(first_date, "%Y%m%d")
reptyear = report_date.year
reptmon = f"{report_date.month:02d}"
reptday = f"{report_date.day:02d}"
rdate_str = report_date.strftime("%d/%m/%Y")

# ===============================================================
# 4. FILTER CURRENT RECORDS (Equivalent to SAS DATA DPTRBLGS + CURRENT)
# ===============================================================
query_filter = """
    SELECT 
        BANKNO, REPTNO, FMTCODE, BRANCH, ACCTNO, NAME, ODPLAN,
        OPENIND, PRODUCT, DEPTYPE, CURBAL, APPRLIMT
    FROM dptrbl
    WHERE BANKNO = 33
      AND REPTNO = 1001
      AND FMTCODE = 1
      AND PRODUCT NOT IN (297, 298)
      AND DEPTYPE IN ('D', 'N')
      AND CURBAL <= -500000
      AND OPENIND NOT IN ('B', 'C', 'P')
"""

filtered_arrow = con.execute(query_filter).arrow()
filtered = pl.from_arrow(filtered_arrow)

# ===============================================================
# 5. PRODUCT & DENOMINATION MAPPINGS
# ===============================================================
CAPROD_MAP = {
    297: "ISLAMIC CA",
    298: "CONVENTIONAL CA"
}
CADENOM_MAP = {
    297: "I",
    298: "D"
}

filtered = filtered.with_columns([
    pl.col("PRODUCT").map_elements(lambda x: CAPROD_MAP.get(x, "UNKNOWN"), return_dtype=pl.Utf8).alias("PRODCD"),
    pl.col("PRODUCT").map_elements(lambda x: CADENOM_MAP.get(x, "D"), return_dtype=pl.Utf8).alias("AMTIND")
])

# ===============================================================
# 6. JOIN WITH BRANCH FILE
# ===============================================================
joined = (
    con.execute("""
        SELECT f.*, b.BRHCODE
        FROM filtered f
        LEFT JOIN brhfile b
        ON f.BRANCH = b.BRANCH
    """).arrow()
)
joined = pl.from_arrow(joined)

# ===============================================================
# 7. MERGE WITH PREVIOUS BALANCES
# ===============================================================
prev_query = """
    SELECT ACCTNO, PRODUCT, CURBAL AS PREVBAL
    FROM (
        SELECT * FROM iptrblp
        UNION ALL
        SELECT * FROM dptrblp
    )
    WHERE FMTCODE = 1
"""
prev_arrow = con.execute(prev_query).arrow()
prev_bal = pl.from_arrow(prev_arrow)

merged_final = joined.join(prev_bal, on="ACCTNO", how="left")

# ===============================================================
# 8. SPLIT INTO CONVENTIONAL AND ISLAMIC
# ===============================================================
cur1 = (
    merged_final
    .filter((pl.col("AMTIND") == "D") &
            (pl.col("ODPLAN") == 100) &
            (pl.col("APPRLIMT") == 0))
    .select(["BRANCH", "BRHCODE", "NAME", "ACCTNO", "CURBAL", "PREVBAL"])
    .sort(["BRANCH", "ACCTNO"])
)

cur2 = (
    merged_final
    .filter(pl.col("AMTIND") == "I")
    .select(["BRANCH", "BRHCODE", "NAME", "ACCTNO", "CURBAL", "PREVBAL"])
    .sort(["BRANCH", "ACCTNO"])
)

# ===============================================================
# 9. WRITE OUTPUT FILES (PyArrow backend)
# ===============================================================
pq.write_table(cur1.to_arrow(), "CONVENTIONAL_CA_NEG_BAL_REPORT.parquet")
cur1.write_csv("CONVENTIONAL_CA_NEG_BAL_REPORT.csv")

pq.write_table(cur2.to_arrow(), "ISLAMIC_CA_NEG_BAL_REPORT.parquet")
cur2.write_csv("ISLAMIC_CA_NEG_BAL_REPORT.csv")

# ===============================================================
# 10. DISPLAY REPORT SUMMARY
# ===============================================================
print("=======================================================================")
print("REPORT ID  : EIBDCA01")
print("PUBLIC BANK BERHAD - PRODUCT DEVELOPMENT & MARKETING")
print("DAILY LISTING OF CURRENT ACCOUNT CUSTOMERS WITHOUT OD FACILITY")
print("NEGATIVE BALANCES RM500,000 & ABOVE AS AT", rdate_str)
print("=======================================================================")
print(f"Conventional Accounts : {cur1.height}")
print(f"Islamic Accounts       : {cur2.height}")
print("Output files generated:")
print(" - CONVENTIONAL_CA_NEG_BAL_REPORT.parquet / .csv")
print(" - ISLAMIC_CA_NEG_BAL_REPORT.parquet / .csv")
print("=======================================================================")
