# EIBDDPCA_DEPOSIT_EXTRACTION_CURRENT_ACCOUNT

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime

# Connect DuckDB
con = duckdb.connect()

# -----------------------------------------------------------------
# Load CIS (Deposit Customer Information)
# -----------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE cis AS
    SELECT 
        ACCTNO,
        CITIZEN AS COUNTRY,
        CAST(SUBSTR(BIRTHDAT, 5, 4) || '-' || SUBSTR(BIRTHDAT, 3, 2) || '-' || SUBSTR(BIRTHDAT, 1, 2) AS DATE) AS dobcis,
        RACE,
        CUSTNAM1 AS NAME
    FROM read_parquet('CIS/DEPOSIT.parquet')
    WHERE SECCUST = '901'
""")

# Remove duplicates by ACCTNO
con.execute("""
    CREATE OR REPLACE TABLE cis AS
    SELECT * FROM cis
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
""")

# -----------------------------------------------------------------
# REPTDATE handling
# -----------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE reptdate AS
    SELECT *,
           CASE 
               WHEN DAY(REPTDATE) BETWEEN 1 AND 8 THEN '1'
               WHEN DAY(REPTDATE) BETWEEN 9 AND 15 THEN '2'
               WHEN DAY(REPTDATE) BETWEEN 16 AND 22 THEN '3'
               ELSE '4'
           END AS wk
    FROM read_parquet('DPTRBL/REPTDATE.parquet')
""")

# Derive reporting variables
repdate_tbl = con.execute("SELECT * FROM reptdate LIMIT 1").fetchdf()
repdate = repdate_tbl['reptdate'][0]
wk = repdate_tbl['wk'][0]

if wk != '4':
    mm = repdate.month - 1
else:
    mm = repdate.month
if mm == 0:
    mm = 12

reptyear = repdate.strftime("%y")
repmon = repdate.strftime("%m")
repday = repdate.strftime("%d")
repmon1 = f"{mm:02d}"
rdate = repdate.strftime("%d/%m/%y")

# -----------------------------------------------------------------
# Monthly logic
# -----------------------------------------------------------------
if wk != '4' and repmon == "01":
    con.execute("""
        CREATE OR REPLACE TABLE ca AS
        SELECT *, 0 AS feeytd
        FROM read_parquet('DPTRBL/CURRENT.parquet')
    """)
else:
    con.execute(f"""
        CREATE OR REPLACE TABLE crmca AS
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) AS rn
            FROM read_parquet('MNICRM/CA{repmon1}.parquet')
        )
        WHERE rn = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE ca AS
        SELECT a.*
        FROM read_parquet('DPTRBL/CURRENT.parquet') a
        LEFT JOIN crmca b ON a.ACCTNO = b.ACCTNO
    """)

# -----------------------------------------------------------------
# OD GP3 merge (if WK = 4)
# -----------------------------------------------------------------
if wk == '4':
    con.execute("""
        CREATE OR REPLACE TABLE ca AS
        SELECT a.*, b.*
        FROM ca a
        LEFT JOIN read_parquet('OD/GP3.parquet') b
        ON a.ACCTNO = b.ACCTNO
    """)

# -----------------------------------------------------------------
# Overdraft Table
# -----------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE overdf AS
    SELECT ACCTNO,
           CRRINI AS faacrr,
           CRRNOW AS crrcode,
           ASCORE_PERM,
           ASCORE_LTST,
           ASCORE_COMM
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) AS rn
        FROM read_parquet('OD/OVERDFT.parquet')
    )
    WHERE rn = 1
""")

# -----------------------------------------------------------------
# Current Accounts Processing
# -----------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE curr AS
    SELECT a.*,
           b.faacrr, b.crrcode, b.ascore_perm, b.ascore_ltst, b.ascore_comm,
           SUBSTR(statcd,4,1) AS acstatus4,
           SUBSTR(statcd,5,1) AS acstatus5,
           SUBSTR(statcd,6,1) AS acstatus6,
           SUBSTR(statcd,7,1) AS acstatus7,
           SUBSTR(statcd,8,1) AS acstatus8,
           SUBSTR(statcd,9,1) AS acstatus9,
           SUBSTR(statcd,10,1) AS acstatus10
    FROM ca a
    LEFT JOIN overdf b ON a.ACCTNO = b.ACCTNO
""")

# -----------------------------------------------------------------
# Final Merge with CIS, SMSACC, MISMTD
# -----------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE current_ca AS
    SELECT 
        cis.*,
        curr.*,
        saacc.ESIGNATURE,
        mis.*
    FROM cis
    LEFT JOIN curr ON cis.ACCTNO = curr.ACCTNO
    LEFT JOIN read_parquet('SIGNA/SMSACC.parquet') saacc ON cis.ACCTNO = saacc.ACCTNO
    LEFT JOIN read_parquet('MISMTD/CAVG{repmon}.parquet') mis ON cis.ACCTNO = mis.ACCTNO
""")

# Handle ESIGNATURE defaulting to 'N'
con.execute("""
    UPDATE current_ca
    SET ESIGNATURE = 'N'
    WHERE ESIGNATURE IS NULL OR ESIGNATURE = ''
""")

# -----------------------------------------------------------------
# Save final output
# -----------------------------------------------------------------
out_name = f"CURRENT_CA{reptyear}{repmon}{repday}.parquet"
con.execute(f"COPY current_ca TO '{out_name}' (FORMAT 'parquet')")

print(f"✅ Extraction completed. Output: {out_name}")
