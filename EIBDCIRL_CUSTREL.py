import duckdb
import pyarrow.parquet as pq
import os
from datetime import datetime

# -----------------------------
# Step 1. Read REPTDATE from SRSDATA (first 8 chars = YYYYMMDD)
# -----------------------------
with open("SRSDATA.txt") as f:
    first_line = f.readline().strip()

tbdate = first_line[:8]
reptdate = datetime.strptime(tbdate, "%Y%m%d")

RDATE = reptdate.strftime("%Y-%m-%d")
RPTDT = reptdate.strftime("%Y%m%d")
print("Reporting Date:", RDATE, "Report Tag:", RPTDT)

# -----------------------------
# Step 2. Read CCRL file (fixed width) into DuckDB
# -----------------------------
duckdb.sql("""
    CREATE TABLE cisrl AS
    SELECT
        TRY_CAST(substr(line,1,11) AS BIGINT)      AS custno,
        substr(line,15,1)                          AS indorg1,
        substr(line,20,3)                          AS relatcd1,
        trim(substr(line,25,15))                   AS desc1,
        TRY_CAST(substr(line,40,11) AS BIGINT)     AS custno2,
        substr(line,55,1)                          AS indorg2,
        substr(line,60,3)                          AS relatcd2,
        trim(substr(line,65,15))                   AS desc2,
        substr(line,80,8)                          AS exp_raw,
        substr(line,95,40)                         AS custname1,
        substr(line,135,3)                         AS newicind1,
        substr(line,138,17)                        AS newic1,
        substr(line,155,40)                        AS custname2,
        substr(line,195,3)                         AS newicind2,
        substr(line,198,17)                        AS newic2,
        substr(line,215,9)                         AS oldic1,
        substr(line,226,2)                         AS bgc1,
        substr(line,228,9)                         AS oldic2,
        substr(line,239,2)                         AS bgc2
    FROM read_text('CCRL.txt', AUTO_DETECT=FALSE)
""")

# Handle business IC fields like SAS logic
duckdb.sql("""
    CREATE TABLE cisrl_clean AS
    SELECT *,
        CASE WHEN newicind1 <> 'IC' THEN newicind1 ELSE NULL END AS bussind1,
        CASE WHEN newicind1 <> 'IC' THEN newic1    ELSE NULL END AS bussreg1,
        CASE WHEN newicind2 <> 'IC' THEN newicind2 ELSE NULL END AS bussind2,
        CASE WHEN newicind2 <> 'IC' THEN newic2    ELSE NULL END AS bussreg2,
        TRY_CAST(substr(exp_raw,1,4)||'-'||substr(exp_raw,5,2)||'-'||substr(exp_raw,7,2) AS DATE) AS expdte
    FROM cisrl
""")

# -----------------------------
# Step 3. Read CISNAME file (fixed width) into DuckDB
# -----------------------------
duckdb.sql("""
    CREATE TABLE cisname AS
    SELECT
        TRY_CAST(substr(line,1,11) AS BIGINT) AS custno,
        trim(substr(line,137,150)) AS nmelong
    FROM read_text('CISNAME.txt', AUTO_DETECT=FALSE)
""")

# -----------------------------
# Step 4. Merge CISRL + CISNAME by custno
# -----------------------------
duckdb.sql("""
    CREATE TABLE cisrl_merged AS
    SELECT r.*, n.nmelong
    FROM cisrl_clean r
    LEFT JOIN cisname n USING(custno)
""")

# -----------------------------
# Step 5. Sort + Deduplicate (like PROC SORT with NODUPKEY)
# -----------------------------
cisrl_final = duckdb.sql("""
    SELECT DISTINCT *
    FROM cisrl_merged
    ORDER BY expdte DESC
""").arrow()

# -----------------------------
# Step 6. Save with PyArrow
# -----------------------------
os.makedirs("output", exist_ok=True)
out_path = f"output/CISRLCC_{RPTDT}.parquet"
pq.write_table(cisrl_final, out_path)

print("Output saved to:", out_path)
