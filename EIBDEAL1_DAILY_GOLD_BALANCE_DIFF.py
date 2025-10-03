# JOB NAME: EIBDEAL1_DAILY_GOLD_BALANCE_DIFF
# PURPOSE: Compare today's and yesterday's GOLD balances and compute differences

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from datetime import date, timedelta

# ---------------------------
# Step 1: Prepare dates
# ---------------------------
today = date.today()
reptdate = today - timedelta(days=1)   # yesterday
reptdat1 = today - timedelta(days=2)   # day before yesterday

YYD, MMD, DDD = reptdate.year % 100, reptdate.month, reptdate.day
YYP, MMP, DDP = reptdat1.year % 100, reptdat1.month, reptdat1.day

print(f"RUN DATE: {reptdate}, FILE KEYS: {YYD}{MMD:02d}{DDD:02d}, {YYP}{MMP:02d}{DDP:02d}")

# ---------------------------
# Step 2: Connect DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------
# Step 3: Load today's GOLD parquet
# ---------------------------
gold_today_path = f"GOLD_GOLD{YYD:02d}{MMD:02d}{DDD:02d}.parquet"
con.execute(f"""
    CREATE TABLE gold_today AS
    SELECT *, CURCODE AS CURCD
    FROM read_parquet('{gold_today_path}')
""")

# ---------------------------
# Step 4: Load yesterday's GOLDP parquet
# ---------------------------
gold_prev_path = f"GOLDP_GOLD{YYP:02d}{MMP:02d}{DDP:02d}.parquet"
con.execute(f"""
    CREATE TABLE gold_prev AS
    SELECT ACCTNO, CURBALGMS AS CURBALP
    FROM read_parquet('{gold_prev_path}')
""")

# ---------------------------
# Step 5: Merge + compute differences
# ---------------------------
con.execute("""
    CREATE TABLE gold_diff AS
    SELECT 
        COALESCE(g.ACCTNO, p.ACCTNO) AS ACCTNO,
        g.CURBALGMS,
        p.CURBALP,
        g.CURCD,
        CASE 
            WHEN g.ACCTNO IS NOT NULL AND p.ACCTNO IS NOT NULL AND g.CURBALGMS = p.CURBALP 
                THEN 0
            WHEN g.ACCTNO IS NOT NULL AND p.ACCTNO IS NOT NULL AND g.CURBALGMS > p.CURBALP
                THEN g.CURBALGMS - p.CURBALP
            WHEN g.ACCTNO IS NOT NULL AND p.ACCTNO IS NOT NULL AND g.CURBALGMS < p.CURBALP
                THEN 0
            WHEN g.ACCTNO IS NOT NULL AND p.ACCTNO IS NULL
                THEN g.CURBALGMS
            ELSE 0
        END AS CREDITFCY,

        CASE 
            WHEN g.ACCTNO IS NOT NULL AND p.ACCTNO IS NOT NULL AND g.CURBALGMS < p.CURBALP
                THEN p.CURBALP - g.CURBALGMS
            ELSE 0
        END AS DEBITFCY,

        0 AS CREDITMYR,
        0 AS DEBITMYR
    FROM gold_today g
    FULL OUTER JOIN gold_prev p
    ON g.ACCTNO = p.ACCTNO
""")

# ---------------------------
# Step 6: Deduplicate ACCTNO
# ---------------------------
con.execute("""
    CREATE TABLE gold_final AS
    SELECT DISTINCT ON (ACCTNO) *
    FROM gold_diff
""")

# ---------------------------
# Step 7: Export results
# ---------------------------
arrow_tbl = con.execute("SELECT * FROM gold_final").arrow()

# Save Parquet
pq.write_table(arrow_tbl, f"GOLD_DIFF_{MMD:02d}.parquet")

# Save CSV
with open(f"GOLD_DIFF_{MMD:02d}.csv", "wb") as f:
    pacsv.write_csv(arrow_tbl, f)

print("✅ Job EIBDEAL1_DAILY_GOLD_BALANCE_DIFF completed. Outputs written to Parquet & CSV.")
