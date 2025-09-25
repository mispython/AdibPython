import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# --- Step 1. Read REPTDATE from SRSDATA ---
with open("SRSDATA.txt") as f:
    first_line = f.readline().strip()

tbdate = first_line[:8]  # first 8 chars = YYYYMMDD
reptdate = datetime.strptime(tbdate, "%Y%m%d")

RDATE = reptdate.strftime("%Y-%m-%d")
RPTDT = reptdate.strftime("%Y%m%d")

print("Reporting date:", RDATE, "Report tag:", RPTDT)

# --- Step 2. Use DuckDB to read CISRHOL fixed-width file ---
# Register file as a virtual table
duckdb.sql(f"""
    CREATE TABLE cisrhol AS
    SELECT
        substr(line, 501, 1)  AS INDORG,
        trim(substr(line, 505, 40)) AS NAME,
        trim(substr(line, 545, 20)) AS ID1,
        trim(substr(line, 565, 20)) AS ID2,
        trim(substr(line, 835, 50)) AS CONTACT1,
        trim(substr(line, 885, 50)) AS CONTACT2,
        trim(substr(line, 935, 50)) AS CONTACT3
    FROM read_text('CISRHOL.txt', AUTO_DETECT=FALSE)
""")

# --- Step 3. Deduplicate (like PROC SORT NODUP BY _ALL_) ---
dedup_df = duckdb.sql("""
    SELECT DISTINCT * FROM cisrhol
""").arrow()

# --- Step 4. Save output as Parquet (PyArrow) ---
output_path = f"output/CISRHOLD_{RPTDT}.parquet"
pq.write_table(dedup_df, output_path)

print("Saved:", output_path)

## ALTERNATIVE

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# --- Step 1. Use system date (yesterday) instead of SRSDATA ---
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day

RDATE = batch_date.strftime("%Y-%m-%d")
RPTDT = batch_date.strftime("%Y%m%d")

print("Reporting date:", RDATE, "Report tag:", RPTDT)

# --- Step 2. Use DuckDB to read CISRHOL fixed-width file ---
duckdb.sql(f"""
    CREATE TABLE cisrhol AS
    SELECT
        substr(line, 501, 1)  AS INDORG,
        trim(substr(line, 505, 40)) AS NAME,
        trim(substr(line, 545, 20)) AS ID1,
        trim(substr(line, 565, 20)) AS ID2,
        trim(substr(line, 835, 50)) AS CONTACT1,
        trim(substr(line, 885, 50)) AS CONTACT2,
        trim(substr(line, 935, 50)) AS CONTACT3
    FROM read_text('CISRHOL.txt', AUTO_DETECT=FALSE)
""")

# --- Step 3. Deduplicate (like PROC SORT NODUP BY _ALL_) ---
dedup_df = duckdb.sql("""
    SELECT DISTINCT * FROM cisrhol
""").arrow()

# --- Step 4. Save output as Parquet (PyArrow) ---
output_path = f"output/CISRHOLD_{RPTDT}.parquet"
pq.write_table(dedup_df, output_path)

print("Saved:", output_path)
