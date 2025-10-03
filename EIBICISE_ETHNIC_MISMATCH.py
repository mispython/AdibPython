# JOB NAME: EIBICISE_ETHNIC_MISMATCH
# PURPOSE: Identify mismatches between CISRACE and USER1 (RACE) from FD

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from datetime import date

# ---------------------------
# Step 1: Load REPTDATE
# ---------------------------
# Assume REPTDATE.parquet exists with REPTDATE column
rept_tbl = pq.read_table("FD_REPTDATE.parquet")
reptdate = rept_tbl.column("REPTDATE")[0].as_py()

NODAY     = f"{reptdate.day:02d}"
REPTYEAR  = reptdate.year
REPTMON   = f"{reptdate.month:02d}"
REPTDAY   = f"{reptdate.day:02d}"
RDATE     = reptdate.strftime("%d/%m/%y")

print(f"RUN DATE: {RDATE}")

# ---------------------------
# Step 2: Connect DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------
# Step 3: Load CD (INTPLAN)
# ---------------------------
con.execute("""
    CREATE OR REPLACE TABLE cd AS
    SELECT ACCTNO, INTPLAN
    FROM read_parquet('CD_FD.parquet')
""")
con.execute("CREATE OR REPLACE TABLE cd AS SELECT DISTINCT * FROM cd")

# ---------------------------
# Step 4: Load FD (branch, acct, race)
# ---------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE fd AS
    SELECT 
        BRANCH,
        ACCTNO,
        RACE AS USER1,
        PRODUCT,
        CAST(SUBSTR(LPAD(CAST(OPENDT AS VARCHAR), 11, '0'),1,8) AS DATE) AS OPENDT,
        NAME
    FROM read_parquet('FD_FD.parquet')
""")
con.execute("CREATE OR REPLACE TABLE fd AS SELECT DISTINCT * FROM fd")

# ---------------------------
# Step 5: Merge FD + CD and filter INTPLAN exclusions
# ---------------------------
con.execute("""
    CREATE OR REPLACE TABLE fd_cd AS
    SELECT f.*
    FROM fd f
    JOIN cd c USING (ACCTNO)
    WHERE NOT (
        (c.INTPLAN BETWEEN 139 AND 149) OR
        (c.INTPLAN BETWEEN 171 AND 173) OR
        (c.INTPLAN BETWEEN 126 AND 129)
    )
""")

# ---------------------------
# Step 6: Load CIS flat file (converted to Parquet beforehand)
# ---------------------------
# Expected columns: ACCTNO, INDORG, CUSTNO, SECCUST, CISRACE
con.execute("""
    CREATE OR REPLACE TABLE cis AS
    SELECT *
    FROM read_parquet('CUST_CIS.parquet')
    WHERE INDORG <> 'O'
      AND SECCUST = '901'
""")
con.execute("CREATE OR REPLACE TABLE cis AS SELECT DISTINCT * FROM cis")

# ---------------------------
# Step 7: Merge FD + CIS and find mismatches
# ---------------------------
con.execute("""
    CREATE OR REPLACE TABLE deposit_ethnic AS
    SELECT f.*, c.CISRACE
    FROM fd_cd f
    JOIN cis c USING (ACCTNO)
    WHERE c.CISRACE <> f.USER1
""")

# ---------------------------
# Step 8: Export results
# ---------------------------
ethnic_tbl = con.execute("SELECT * FROM deposit_ethnic").arrow()

pq.write_table(ethnic_tbl, "DEPOSIT_ETHNIC.parquet")

with open("DEPOSIT_ETHNIC.csv", "wb") as f:
    pacsv.write_csv(ethnic_tbl, f)

print("✅ Job EIBICISE_ETHNIC_MISMATCH completed. Outputs: DEPOSIT_ETHNIC.parquet, DEPOSIT_ETHNIC.csv")
