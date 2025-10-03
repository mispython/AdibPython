# JOB NAME: EIBACISE_CUSTOMER_ETHNIC_VALIDATION
# PURPOSE: Merge FD and CIS customer data, validate ethnicity differences

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from datetime import date

# ---------------------------
# Step 1: Generate report date (like SAS symput vars)
# ---------------------------
reptdate = date.today().toordinal() - 1  # yesterday
today = date.today()
rept_day = today.day
rept_mon = today.month
rept_year = today.year
rdate_str = today.strftime("%d%m%Y")

print(f"REPORT DATE: {rdate_str}")

# ---------------------------
# Step 2: Create DuckDB connection
# ---------------------------
con = duckdb.connect()

# ---------------------------
# Step 3: Load parquet sources
# ---------------------------
con.execute("""
    CREATE TABLE cd_fd AS 
    SELECT ACCTNO, INTPLAN 
    FROM read_parquet('CD_FD.parquet')
""")

con.execute("""
    CREATE TABLE fd_fd AS
    SELECT BRANCH, ACCTNO, RACE AS USER1, PRODUCT, OPENDT, NAME
    FROM read_parquet('FD_FD.parquet')
""")

con.execute("""
    CREATE TABLE cis AS
    SELECT *
    FROM read_parquet('CUST.parquet')
""")

# ---------------------------
# Step 4: Deduplicate CD on ACCTNO
# ---------------------------
con.execute("""
    CREATE TABLE cd_unique AS
    SELECT DISTINCT ACCTNO, INTPLAN FROM cd_fd
""")

# ---------------------------
# Step 5: Convert OPENDT (SAS date fix)
# ---------------------------
con.execute("""
    CREATE TABLE fd_fixed AS
    SELECT BRANCH, ACCTNO, USER1, PRODUCT,
           STRFTIME(OPENDT, '%Y-%m-%d')::DATE AS OPENDT,
           NAME
    FROM fd_fd
""")

# ---------------------------
# Step 6: Merge FD + CD
# ---------------------------
con.execute("""
    CREATE TABLE fd_merged AS
    SELECT f.*
    FROM fd_fixed f
    JOIN cd_unique c
      ON f.ACCTNO = c.ACCTNO
    WHERE NOT (
        (c.INTPLAN BETWEEN 139 AND 149) OR
        (c.INTPLAN BETWEEN 171 AND 173) OR
        (c.INTPLAN BETWEEN 126 AND 129)
    )
""")

# ---------------------------
# Step 7: Clean CIS (exclude orgs and special SECCUST)
# ---------------------------
con.execute("""
    CREATE TABLE cis_clean AS
    SELECT ACCTNO, INDORG, CUSTNO, SECCUST, CISRACE
    FROM cis
    WHERE INDORG <> 'O'
      AND SECCUST = '901'
""")

# Deduplicate
con.execute("""
    CREATE TABLE cis_unique AS
    SELECT DISTINCT ACCTNO, INDORG, CUSTNO, SECCUST, CISRACE
    FROM cis_clean
""")

# ---------------------------
# Step 8: Merge FD and CIS
# ---------------------------
con.execute("""
    CREATE TABLE deposit_ethnic AS
    SELECT f.*, c.CISRACE
    FROM fd_merged f
    JOIN cis_unique c
      ON f.ACCTNO = c.ACCTNO
    WHERE c.CISRACE <> f.USER1
""")

# ---------------------------
# Step 9: Export results
# ---------------------------
arrow_tbl = con.execute("SELECT * FROM deposit_ethnic").arrow()

# Save Parquet
pq.write_table(arrow_tbl, "DEPOSIT_ETHNIC.parquet")

# Save CSV
with open("DEPOSIT_ETHNIC.csv", "wb") as f:
    pacsv.write_csv(arrow_tbl, f)

print(" Job EIBACISE_CUSTOMER_ETHNIC_VALIDATION completed. Outputs written to Parquet & CSV.")
