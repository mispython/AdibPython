# Program: EICRCREQ_CIS_DEPOSIT_REQUEST
# Purpose: Consolidate CIS Deposit Requests with Customer, Tax, Address, and Enrichment tables
# Tools: DuckDB + PyArrow

import duckdb
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os
import datetime

# -----------------------------
# Step 1. Batch Date (yesterday)
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
tag = batch_date.strftime("%Y%m%d")
print("EICRCREQ Report Date:", batch_date, "Tag:", tag)

# -----------------------------
# Step 2. Register Parquet Inputs
# (assume FW → Parquet already done upstream)
# -----------------------------
duckdb.sql("CREATE TABLE CISADDR   AS SELECT * FROM read_parquet('CISADDR.parquet')")
duckdb.sql("CREATE TABLE CISNAME   AS SELECT * FROM read_parquet('CISNAME.parquet')")
duckdb.sql("CREATE TABLE CISTAXID  AS SELECT * FROM read_parquet('CISTAXID.parquet')")
duckdb.sql("CREATE TABLE CISDP0    AS SELECT * FROM read_parquet('CISDP.parquet')")
duckdb.sql("CREATE TABLE CISSSMID  AS SELECT * FROM read_parquet('CISSSMID.parquet')")
duckdb.sql("CREATE TABLE CISLARGE  AS SELECT * FROM read_parquet('CISLARGE.parquet')")
duckdb.sql("CREATE TABLE CISMMTOH  AS SELECT * FROM read_parquet('CISMMTOH.parquet')")
duckdb.sql("CREATE TABLE CISRMRK   AS SELECT * FROM read_parquet('CISRMRK.parquet')")
duckdb.sql("CREATE TABLE CISSALE   AS SELECT * FROM read_parquet('CISSALE.parquet')")
duckdb.sql("CREATE TABLE CISEMPL   AS SELECT * FROM read_parquet('CISEMPL.parquet')")

# -----------------------------
# Step 3. Transformations
# -----------------------------

# SALE: numeric turnover
duckdb.sql("""
    CREATE TABLE CISSALE_CLEAN AS
    SELECT *, TRY_CAST(TURNOVER2 AS DOUBLE) AS TURNOVER
    FROM CISSALE
""")

# EMPL: numeric employees
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
    CREATE TABLE CISDP1 AS
    SELECT d.*, t.OLDIC, t.NEWICIND, t.NEWIC, t.BUSSIND, t.BUSSREG, t.BRANCH, t.RHOLD_IND
    FROM CISDP0 d
    LEFT JOIN CISTAXID t USING (CUSTNO)
""")

duckdb.sql("""
    CREATE TABLE CISDP2 AS
    SELECT d1.*, n.CUSTNAME, n.ADDREF AS NAME_ADDRREF, n.PRIPHONE, n.SECPHONE,
           n.CUSTNAM1, n.MOBIPHON, n.LONGNAME
    FROM CISDP1 d1
    LEFT JOIN CISNAME n USING (CUSTNO)
""")

duckdb.sql("""
    CREATE TABLE CISDP3 AS
    SELECT d2.*, r.EMAILADD, s.TURNOVER, e.NOEMPLO,
           sm.NEW_BUSS_REG_ID_TYPE, sm.NEW_BUSS_REG_ID,
           l.LARGE_CORP_FLG, m.MM2H_FLG
    FROM CISDP2 d2
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
    CREATE TABLE CISDEPOSIT_REQ_FINAL AS
    SELECT d3.*, a.ADDRLN1, a.ADDRLN2, a.ADDRLN3, a.ADDRLN4, a.ADDRLN5,
           a.MAILCODE, a.MAILSTAT
    FROM CISDP3 d3
    LEFT JOIN CISADDR a USING (ADDREF)
""")

# -----------------------------
# Step 6. Save Output
# -----------------------------
final_tbl = duckdb.sql("SELECT * FROM CISDEPOSIT_REQ_FINAL").arrow()

os.makedirs("output", exist_ok=True)
out_parquet = f"output/CIS_DEPOSIT_REQ_{tag}.parquet"
out_csv = f"output/CIS_DEPOSIT_REQ_{tag}.csv"

pq.write_table(final_tbl, out_parquet)
with open(out_csv, "wb") as f:
    pacsv.write_csv(final_tbl, f)

print(f"EICRCREQ_CIS_DEPOSIT_REQUEST completed.\nParquet: {out_parquet}\nCSV: {out_csv}")
