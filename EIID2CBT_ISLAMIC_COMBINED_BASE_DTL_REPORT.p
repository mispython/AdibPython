# EIID2CBT_ISLAMIC_COMBINED_BASE_DTL_REPORT

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta

# -------------------------
# Step 1: Prepare report dates
# -------------------------
today = date.today()
reptdate = today
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# Next month start
if reptdate.month + 1 == 13:
    mm = 1
    yy = reptdate.year + 1
else:
    mm = reptdate.month + 1
    yy = reptdate.year
sdate = date(yy, mm, 1)

# Equivalent macro values
reptyear = reptdate.strftime("%y")
reptmon = reptdate.strftime("%m")
reptday = reptdate.strftime("%d")
rdate = reptdate.strftime("%Y%m%d")
prevmon = prevdate.strftime("%m")
sdate_str = sdate.strftime("%Y%m%d")

# -------------------------
# Step 2: Load Islamic BASE & BTDTL data
# -------------------------
con = duckdb.connect()

# Replace with actual Islamic parquet file locations
base_tbl = f"BASE_IBTBASE{prevmon}.parquet"
btdtl_tbl = f"BT_IBTDTL{reptyear}{reptmon}{reptday}.parquet"

# Read into Arrow Tables
base = pq.read_table(base_tbl)
btdtl = pq.read_table(btdtl_tbl)

# Register in DuckDB
con.register("base", base)
con.register("btdtl", btdtl)

# -------------------------
# Step 3: Transform BASE (PREOUTSTD)
# -------------------------
base_trans = con.execute("""
    SELECT ACCTNO, TRANSREF, OUTSTAND AS PREOUTSTD
    FROM base
    GROUP BY ACCTNO, TRANSREF, OUTSTAND
""").arrow()

# -------------------------
# Step 4: Transform BTDTL (assign PRODTYPE)
# -------------------------
btdtl_trans = con.execute("""
    SELECT BRANCH, ACCTNO, TRANSREF,
           CASE 
             WHEN RETAILID = 'R' THEN 0
             WHEN RETAILID = 'C' THEN 999
             ELSE CAST(PRODTYPE AS INT)
           END AS PRODTYPE,
           OUTSTAND, MATDATE, FACILITY
    FROM btdtl
    GROUP BY BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY, RETAILID
""").arrow()

# -------------------------
# Step 5: Merge BASE + BTDTL
# -------------------------
con.register("base_trans", base_trans)
con.register("btdtl_trans", btdtl_trans)

combt = con.execute(f"""
    SELECT 
        b.BRANCH,
        b.ACCTNO,
        b.TRANSREF,
        b.PRODTYPE,
        base.PREOUTSTD,
        b.OUTSTAND,
        CAST(({sdate.toordinal()} - b.MATDATE) AS INT) AS OVERDUE,
        CAST((base.PREOUTSTD - b.OUTSTAND) AS DOUBLE) AS RECOVAMT,
        b.FACILITY
    FROM base_trans base
    INNER JOIN btdtl_trans b
    ON base.ACCTNO = b.ACCTNO
   AND base.TRANSREF = b.TRANSREF
""").arrow()

# -------------------------
# Step 6: Export to fixed-width report (like SAS PUT)
# -------------------------
output_file = "DAYBTRD_ISLAMIC.txt"

with open(output_file, "w") as f:
    for row in combt.to_pylist():
        f.write(
            f"{str(row['BRANCH']).rjust(5)}"
            f"{str(row['ACCTNO']).rjust(10)}"
            f"{str(row['TRANSREF']).rjust(10)}"
            f"{str(row['PRODTYPE']).zfill(3)}"
            f"{row['PREOUTSTD']:017.2f}"
            f"{row['OUTSTAND']:017.2f}"
            f"{str(row['OVERDUE']).rjust(10)}"
            f"{row['RECOVAMT']:017.2f}"
            f"{str(row['FACILITY']).ljust(5)}\n"
        )

print(f"✅ Islamic BT Report generated: {output_file}")
