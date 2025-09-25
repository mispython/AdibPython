import duckdb
import pyarrow.parquet as pq
from datetime import datetime

# --- Step 1: Read report date ---
with open("SRSDATA.txt") as f:
    tdate = f.readline().strip()

reptdate = datetime.strptime(tdate, "%Y%m%d")
rptdt = reptdate.strftime("%Y%m%d")  # SAS: YYMMDDN8.

# --- Step 2: Load ADDRFL (fixed-width address file) ---
duckdb.sql("""
    CREATE TABLE CISLADDR AS
    SELECT
        SUBSTR(line,1,11) AS CUSTNO,
        CAST(SUBSTR(line,13,11) AS BIGINT) AS ADDREF,
        SUBSTR(line,24,1)  AS LINE1IND,
        SUBSTR(line,25,40) AS LINE1ADR,
        SUBSTR(line,65,1)  AS LINE2IND,
        SUBSTR(line,66,40) AS LINE2ADR,
        SUBSTR(line,106,1) AS LINE3IND,
        SUBSTR(line,107,40) AS LINE3ADR,
        SUBSTR(line,147,1) AS LINE4IND,
        SUBSTR(line,148,40) AS LINE4ADR,
        SUBSTR(line,188,1) AS LINE5IND,
        SUBSTR(line,189,40) AS LINE5ADR,
        SUBSTR(line,229,15) AS STREET,
        SUBSTR(line,244,25) AS CITY,
        SUBSTR(line,269,3)  AS STATEX,
        SUBSTR(line,272,2)  AS STATEID,
        SUBSTR(line,274,5)  AS ZIP,
        SUBSTR(line,279,4)  AS ZIP2,
        SUBSTR(line,283,10) AS COUNTRY
    FROM read_csv('ADDRFL.txt', delim='|', columns={'line':'VARCHAR'}, header=False)
""")

# --- Step 3: Deduplicate (PROC SORT NODUP) ---
result = duckdb.sql("""
    SELECT DISTINCT *
    FROM CISLADDR
""").arrow()

# --- Step 4: Save output with date suffix ---
output_path = f"output/CISLADDR_{rptdt}.parquet"
pq.write_table(result, output_path)

print(f"Program EIBDADDR_CUSTADDR completed. Output saved to {output_path}")
