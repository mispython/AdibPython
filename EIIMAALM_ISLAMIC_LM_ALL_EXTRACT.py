# Program: EIIMAALM_LM_ALL_EXTRACT
# Purpose: Consolidate loan/limit master data (OVERDFT, LMTDET, ELNEND, ELNMAX) into LM_ALL
# Tools: DuckDB + PyArrow

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os
import datetime

# -----------------------------
# Step 1. Reporting Month (batch_date = yesterday)
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
REPTMON = f"{batch_date.month:02d}"
print("Batch Month:", REPTMON)

# -----------------------------
# Step 2. Read base datasets (assume already exported into Parquet files)
# -----------------------------
duckdb.sql("""
    CREATE TABLE LM AS
    SELECT
        ACCTNO,
        BRANCH,
        LMTID,
        AANO AS LMTDESC,
        LMTAMT,
        LMTRATE,
        LMTINCR,
        LMTCOLL,
        LMTINDEX,
        LMTBASER,
        CASE
            WHEN LMTINDEX IN (1,30) THEN CASE WHEN LMTRATE >= LMTBASER THEN 41 ELSE 42 END
            WHEN LMTINDEX IN (3,4,5,12,13,18,19,22,27) THEN 81
            ELSE 59
        END AS TYPRIC,
        strftime(LMTSTART, '%Y%m%d') AS STDT,
        CASE
            WHEN LMTENDDT <> 99999999999 THEN strftime(LMTENDDT, '%Y%m%d')
            ELSE NULL
        END AS ENDDT
    FROM read_parquet('LIMIT_OVERDFT.parquet')
    WHERE LMTAMT > 1 AND APPRLIMT > 1
""")

duckdb.sql("""
    CREATE TABLE LMTDET AS
    SELECT
        ACCTNO,
        AANO AS LMTDESC,
        strftime(APRVDT, '%Y%m%d') AS DT,
        strftime(LASTMNTDT, '%Y%m%d') AS LSTDT,
        COLL1, COLL2, COLL3, COLL4, COLL5,
        COLL6, COLL7, COLL8, COLL9, COLL10
    FROM read_parquet('LMTDET_LMTDET.parquet')
""")

duckdb.sql("""
    CREATE TABLE ELNEND AS
    SELECT
        MAANO AS LMTDESC,
        CACCRSCK, CACRSPB, CADCHQCK, CADCHEQS, GEAR, CALGLACT
    FROM read_parquet('ELDSRV_ELNEND.parquet')
""")

duckdb.sql("""
    CREATE TABLE ELNMAX AS
    SELECT
        AANO AS LMTDESC,
        PCODCRIS, CRRSCORE
    FROM read_parquet('ELDSRV_ELNMAX.parquet')
    WHERE AANO IS NOT NULL AND AANO <> ''
""")

# -----------------------------
# Step 3. Merge data step by step
# -----------------------------
duckdb.sql("""
    CREATE TABLE LM_DET AS
    SELECT A.*, B.DT, B.LSTDT,
           B.COLL1, B.COLL2, B.COLL3, B.COLL4, B.COLL5,
           B.COLL6, B.COLL7, B.COLL8, B.COLL9, B.COLL10
    FROM LM A
    LEFT JOIN LMTDET B USING (ACCTNO, LMTDESC)
""")

duckdb.sql("""
    CREATE TABLE LM_END AS
    SELECT A.*, B.CACCRSCK, B.CACRSPB, B.CADCHQCK, B.CADCHEQS, B.GEAR, B.CALGLACT
    FROM LM_DET A
    LEFT JOIN ELNEND B USING (LMTDESC)
""")

duckdb.sql("""
    CREATE TABLE LM_MAX AS
    SELECT A.*, B.PCODCRIS, B.CRRSCORE
    FROM LM_END A
    LEFT JOIN ELNMAX B USING (LMTDESC)
""")

# -----------------------------
# Step 4. Final LM_ALL sorted
# -----------------------------
lm_all = duckdb.sql("""
    SELECT *
    FROM LM_MAX
    ORDER BY ACCTNO, LMTID, LMTDESC
""").arrow()

# -----------------------------
# Step 5. Save to Parquet + CSV
# -----------------------------
os.makedirs("output", exist_ok=True)
out_parquet = f"output/LM_ALL_{REPTMON}.parquet"
out_csv = f"output/LM_ALL_{REPTMON}.csv"

pq.write_table(lm_all, out_parquet)

import pyarrow.csv as pacsv
with open(out_csv, "wb") as f:
    pacsv.write_csv(lm_all, f)

print(f"Program EIIMAALM_LM_ALL_EXTRACT completed.\nParquet: {out_parquet}\nCSV: {out_csv}")

# -----------------------------
# (Optional) Step 6: Fixed-width file like FILE ELNLMT
# -----------------------------
# If you need the fixed-width file output, we can map each field with widths and format using Python string formatting.
