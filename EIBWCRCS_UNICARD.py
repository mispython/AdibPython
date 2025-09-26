# Program: EIBWCRCS_UNICARD
# Purpose: Consolidate Card, Customer, and Account data into a CRM UNICARD dataset
# Tools: DuckDB + PyArrow

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os
import datetime

# -----------------------------
# Step 1. Reporting Date (simulate SAS REPTDATE)
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
day = batch_date.day
month = batch_date.month
year = batch_date.year

if day == 8:
    SDD, WK, WK1 = 1, "01", "04"
elif day == 15:
    SDD, WK, WK1 = 9, "02", "01"
elif day == 22:
    SDD, WK, WK1 = 16, "03", "02"
else:
    SDD, WK, WK1 = 23, "04", "03"

REPTMON = f"{month:02d}"
REPTYR2 = str(year)[-2:]
REPTYEAR = str(year)
NOWK = WK

print("Report Date:", batch_date, "Tag:", REPTYEAR+REPTMON+NOWK)

# -----------------------------
# Step 2. Card Type Mapping (BIN prefix)
# -----------------------------
CARDTYP_MAP = {
    "455358": "VISA CLASSIC",
    "455357": "VISA GOLD",
    "467950": "MU VISA CREDIT",
    "467951": "MU VISA DEBIT",
    "491253": "ESSO CLASSIC",
    "491254": "ESSO GOLD",
    "455388": "VISA PLATINUM",
    "447764": "VISA ELECTRON",
    "542634": "PBB MSTR STD",
    "547231": "PBB MSTR GOLD",
    "543760": "PFB MSTR STD",
    "540602": "PFB MSTR GOLD",
    "520000": "PBB MSTR SMRT",
    "467894": "ESSO MOBIL VISA CREDIT",
    "467893": "ESSO MOBIL VISA DEBIT",
    "559589": "PB WORLD MASTERCARD CREDIT CARD",
    "537992": "PB MASTERCARD QUANTUM PLATINUM CREDIT CARD",
    "493531": "PB VISA QUANTUM PLATINUM CREDIT CARD"
}

def map_cardtype(cardno):
    prefix = str(cardno)[:6]
    return CARDTYP_MAP.get(prefix, "UNKNOWN")

# -----------------------------
# Step 3. Read source files (assumed already exported to Parquet)
# -----------------------------
duckdb.sql("CREATE TABLE ACCT AS SELECT * FROM read_parquet('ACCTFILE.parquet')")
duckdb.sql("CREATE TABLE CARD AS SELECT * FROM read_parquet('CARDFILE.parquet')")
duckdb.sql("CREATE TABLE CUST AS SELECT * FROM read_parquet('CUSTFILE.parquet')")
duckdb.sql("CREATE TABLE STMT AS SELECT * FROM read_parquet('STMTFILE.parquet')")

# -----------------------------
# Step 4. Transformations
# -----------------------------

# Fix balances & overdue
duckdb.sql("""
    CREATE TABLE ACCT_CLEAN AS
    SELECT *,
           CASE WHEN SIGN='-' THEN -CURBAL ELSE CURBAL END AS CURBAL2,
           CASE WHEN STATSIGN='-' THEN -STATEINT ELSE STATEINT END AS STATEINT2,
           CASE WHEN CITIZEN='MY' THEN '1' ELSE RESIDENT END AS RESIDENT2,
           CASE WHEN BUCKET1>0 OR BUCKET2>0 OR BUCKET3>0 OR BUCKET4>0 OR BUCKET5>0 OR BUCKET6>0
                THEN 'Y' ELSE 'N' END AS OVRDUE
    FROM ACCT
""")

# Add card type + issue date
duckdb.sql("""
    CREATE TABLE CARD_CLEAN AS
    SELECT *,
           CARDNO,
           CASE WHEN ISSUEDT='000000' THEN NULL
                ELSE STRPTIME(ISSUEDT,'%d%m%y') END AS ISSDTE,
           CARDNO AS CARDNO_STR
    FROM CARD
""")

# Add cardtype mapping (via Python UDF)
duckdb.create_function("map_cardtype", map_cardtype, [str], str)
duckdb.sql("ALTER TABLE CARD_CLEAN ADD COLUMN CARDTYPE VARCHAR")
duckdb.sql("UPDATE CARD_CLEAN SET CARDTYPE = map_cardtype(CARDNO_STR)")

# Handle IC logic + birthdate + age
duckdb.sql(f"""
    CREATE TABLE CUST_CLEAN AS
    SELECT *,
           CASE
               WHEN SUBSTR(CUSTNBR,12,1)=' ' AND SUBSTR(CUSTNBR,1,1) IN ('A','H','K') THEN CUSTNBR
               WHEN SUBSTR(CUSTNBR,12,1)=' ' AND SUBSTR(CUSTNBR,1,1)='T' THEN NULL
               ELSE NULL
           END AS OLDIC,
           CASE
               WHEN SUBSTR(CUSTNBR,12,1)=' ' AND SUBSTR(CUSTNBR,1,1)='T' THEN CUSTNBR
               ELSE NULL
           END AS NEWIC,
           CASE WHEN LENGTH(BIRTHDAT)>0 THEN CAST(SUBSTR(BIRTHDAT,6,2) AS INT) END AS YR,
           CASE
               WHEN LENGTH(BIRTHDAT)>0 THEN 
                   CASE WHEN CAST(SUBSTR(BIRTHDAT,6,2) AS INT) < 20 
                        THEN 2000+CAST(SUBSTR(BIRTHDAT,6,2) AS INT)
                        ELSE 1900+CAST(SUBSTR(BIRTHDAT,6,2) AS INT)
                   END
               ELSE NULL
           END AS BIRTHYEAR,
           {REPTYEAR} - (
                   CASE WHEN LENGTH(BIRTHDAT)>0 THEN 
                       CASE WHEN CAST(SUBSTR(BIRTHDAT,6,2) AS INT) < 20 
                            THEN 2000+CAST(SUBSTR(BIRTHDAT,6,2) AS INT)
                            ELSE 1900+CAST(SUBSTR(BIRTHDAT,6,2) AS INT)
                       END
                   ELSE NULL END
           ) AS AGE
    FROM CUST
""")

# -----------------------------
# Step 5. Merge (CARD + CUST -> CARDCUST)
# -----------------------------
duckdb.sql("""
    CREATE TABLE CARDCUST AS
    SELECT c.*, cu.*
    FROM CARD_CLEAN c
    LEFT JOIN CUST_CLEAN cu USING (CUSTNBR)
""")

# Merge with ACCT
duckdb.sql("""
    CREATE TABLE UNICARD AS
    SELECT a.*, cc.*
    FROM CARDCUST cc
    LEFT JOIN ACCT_CLEAN a USING (ACCTNO)
""")

# Merge with STMT
duckdb.sql("""
    CREATE TABLE UNICARD_FINAL AS
    SELECT u.*, s.INTEREST
    FROM UNICARD u
    LEFT JOIN STMT s USING (CARDNO)
""")

# -----------------------------
# Step 6. Save output
# -----------------------------
unicard_final = duckdb.sql("SELECT * FROM UNICARD_FINAL").arrow()

os.makedirs("output", exist_ok=True)
out_parquet = f"output/UNICARD_{REPTYEAR}{REPTMON}{NOWK}.parquet"
out_csv = f"output/UNICARD_{REPTYEAR}{REPTMON}{NOWK}.csv"

pq.write_table(unicard_final, out_parquet)
with open(out_csv, "wb") as f:
    pacsv.write_csv(unicard_final, f)

print(f"EIBWCRCS_UNICARD completed.\nParquet: {out_parquet}\nCSV: {out_csv}")
