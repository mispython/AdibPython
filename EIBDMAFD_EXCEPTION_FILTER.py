import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# ------------------------------------------------------------------
# Job Name : EIBDMAFD
# Purpose  : Extract MAFD records where CMPIND = 'E1'
# ------------------------------------------------------------------

# Connect DuckDB in-memory
con = duckdb.connect()

# ------------------------------------------------------------------
# Step 1: REPTDATE logic
# ------------------------------------------------------------------
reptdate = datetime.date.today() - datetime.timedelta(days=1)

REPTDAY  = str(reptdate.day).zfill(2)
REPTMON  = str(reptdate.month).zfill(2)
REPTYEAR = str(reptdate.year)[-2:]
RDATE    = reptdate.strftime("%Y-%m-%d")

# ------------------------------------------------------------------
# Step 2: Load MAFDFL input (replace with actual Parquet path)
# ------------------------------------------------------------------
con.execute("CREATE OR REPLACE TABLE mafdfl AS SELECT * FROM 'MAFDFL.parquet'")

# ------------------------------------------------------------------
# Step 3: Apply filter CMPIND = 'E1'
# ------------------------------------------------------------------
mafd_tbl = con.execute("""
    SELECT 
        ACCTNO,
        CDNO,
        TIMESTAMP,
        CMPIND,
        ACE_ACCTNO
    FROM mafdfl
    WHERE CMPIND = 'E1'
""").arrow()

# ------------------------------------------------------------------
# Step 4: Save results (Parquet + CSV)
# ------------------------------------------------------------------
output_parquet = f"MAFD{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
output_csv     = f"MAFD{REPTDAY}{REPTMON}{REPTYEAR}.csv"

pq.write_table(mafd_tbl, output_parquet)
con.register("mafd_tbl", mafd_tbl)
con.execute(f"COPY mafd_tbl TO '{output_csv}' (HEADER, DELIMITER ',')")

print(f"EIBDMAFD job completed. Outputs: {output_parquet}, {output_csv}")
