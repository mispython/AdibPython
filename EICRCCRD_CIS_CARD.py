# Program: EICRCCRD_CIS_CARD
# Purpose: Consolidate CIS Card + Customer + Tax + Address datasets
# Tools: DuckDB + PyArrow

import duckdb
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os
import datetime

# -----------------------------
# Step 1. Batch date
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
tag = batch_date.strftime("%Y%m%d")
print("EICRCCRD Report Date:", batch_date, "Tag:", tag)

# -----------------------------
# Step 2. Register Parquet inputs
# (Assume preprocessing step: fixed-width -> parquet already done)
# -----------------------------
duckdb.sql("CREATE TABLE CISADDR   AS SELECT * FROM read_parquet('CISADDR.parquet')")
duckdb.sql("CREATE TABLE CISNAME   AS SELECT * FROM read_parquet('CISNAME.parquet')")
duckdb.sql("CREATE TABLE CISTAXID  AS SELECT * FROM read_parquet('CISTAXID.parquet')")
duckdb.sql("CREATE TABLE CISCARD0  AS SELECT * FROM read_parquet('CISCARD.parquet')")
duckdb.sql("CREATE TABLE CISBNMID  AS SELECT * FROM read_parquet('CISBNMID.parquet')")
duckdb.sql("CREATE TABLE CISSSMID  AS SELECT * FROM read_parquet('CISSSMID.parquet')")
duckdb.sql("CREATE TABLE CISRMRK   AS SELECT * FROM read_parquet('CISRMRK.parquet')")
duckdb.sql("CREATE TABLE CISSALE   AS SELECT * FROM read_parquet('CISSALE.parquet')")
duckdb.sql("CREATE TABLE CISEMPL   AS SELECT * FROM read_parquet('CISEMPL.parquet')")
duckdb.sql("CREATE TABLE CISLARGE  AS SELECT * FROM read_parquet('CISLARGE.parquet')")
duckdb.sql("CREATE TABLE CISMMTOH  AS SELECT * FROM read_parquet('CISMMTOH.parquet')")

# -----------------------------
# Step 3. Transformations per SAS logic
# -----------------------------

# Deduplicate where needed
def nodup(table, keycols):
    return duckdb.sql(f"SELECT DISTINCT * FROM {table}").execute()

nodup("CISBNMID", "CUSTNO")
nodup("CISSSMID", "CUSTNO")
nodup("CISRMRK",  "CUSTNO")
nodup("CISSALE",  "CUSTNO")
nodup("CISEMPL",  "CUSTNO")
nodup("CISLARGE", "CUSTNO")
nodup("CISMMTOH", "CUSTNO")

# SALE: numeric conversion
duckdb.sql("""
    CREATE TABLE CISSALE_CLEAN AS
    SELECT *, TRY_CAST(TURNOVER2 AS DOUBLE) AS TURNOVER
    FROM CISSALE
""")

# EMPL: numeric conversion
duckdb.sql("""
    CREATE TABLE CISEMPL_CLEAN AS
    SELECT *, TRY_CAST(NOEMPLO2 AS DOUBLE) AS NOEMPLO
    FROM CISEMPL
""")

# LARGE flag
duckdb.sql("""
    CREATE TABLE CISLARGE_CLEAN AS
    SELECT *,
           CASE WHEN LARGECO='LARGECO' AND LARGECO_IND='YES' THEN 'Y' ELSE NULL END AS LARGE_CORP_FLG
    FROM CISLARGE
""")

# MMTOH flag
duckdb.sql("""
    CREATE TABLE CISMMTOH_CLEAN AS
    SELECT *,
           CASE WHEN MMTOH='MMTOH' THEN 'Y' ELSE NULL END AS MM2H_FLG
    FROM CISMMTOH
""")

# -----------------------------
# Step 4. Merge by CUSTNO
# -----------------------------
duckdb.sql("""
    CREATE TABLE CISCARD1 AS
    SELECT c0.*, t.*, b.*
    FROM CISCARD0 c0
    LEFT JOIN CISTAXID t USING (CUSTNO)
    LEFT JOIN CISBNMID b USING (CUSTNO)
""")

duckdb.sql("""
    CREATE TABLE CISCARD2 AS
    SELECT c1.*, n.*
    FROM CISCARD1 c1
    LEFT JOIN CISNAME n USING (CUSTNO)
""")

duckdb.sql("""
    CREATE TABLE CISCARD3 AS
    SELECT c2.*, r.EMAILADD, s.TURNOVER, e.NOEMPLO,
           sm.NEW_BUSS_REG_ID_TYPE, sm.NEW_BUSS_REG_ID,
           l.LARGE_CORP_FLG, m.MM2H_FLG
    FROM CISCARD2 c2
    LEFT JOIN CISRMRK r   USING (CUSTNO)
    LEFT JOIN CISSALE_CLEAN s USING (CUSTNO)
    LEFT JOIN CISEMPL_CLEAN e USING (CUSTNO)
    LEFT JOIN CISSSMID sm USING (CUSTNO)
    LEFT JOIN CISLARGE_CLEAN l USING (CUSTNO)
    LEFT JOIN CISMMTOH_CLEAN m USING (CUSTNO)
""")

# -----------------------------
# Step 5. Merge with Address (BY ADDREF)
# -----------------------------
duckdb.sql("""
    CREATE TABLE CISCARD_FINAL AS
    SELECT c3.*, a.ADDRLN1, a.ADDRLN2, a.ADDRLN3, a.ADDRLN4, a.ADDRLN5,
           a.MAILCODE, a.MAILSTAT
    FROM CISCARD3 c3
    LEFT JOIN CISADDR a USING (ADDREF)
""")

# -----------------------------
# Step 6. Save output
# -----------------------------
result = duckdb.sql("SELECT * FROM CISCARD_FINAL").arrow()

os.makedirs("output", exist_ok=True)
out_parquet = f"output/CIS_CARD_{tag}.parquet"
out_csv = f"output/CIS_CARD_{tag}.csv"

pq.write_table(result, out_parquet)
with open(out_csv, "wb") as f:
    pacsv.write_csv(result, f)

print(f"EICRCCRD_CIS_CARD completed.\nParquet: {out_parquet}\nCSV: {out_csv}")
