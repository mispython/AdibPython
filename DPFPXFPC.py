# EIBD2CBT_COMBINED_BASE_DTL_REPORT

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import date, timedelta
import os

# -------------------------
# Configuration Paths
# -------------------------
INPUT_PATHS = {
    'btbase': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/', 
    'btdtl': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/' 
}

OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/output'  

# -------------------------
# Step 1: Prepare report dates
# -------------------------
today = date.today()
reptdate = today - timedelta(days=1)  # Using yesterday's date
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

print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
print(f"Previous Month: {prevmon}")
print(f"Next Month Start: {sdate_str}")

# -------------------------
# Step 2: Load BASE & BTDTL data
# -------------------------
con = duckdb.connect()

# Input file paths
base_file = os.path.join(INPUT_PATHS['btbase'], f"btbase{prevmon}.sas7bdat")
btdtl_file = os.path.join(INPUT_PATHS['btdtl'], f"btdtl{reptyear}{reptmon}{reptday}.sas7bdat")

# Check if files exist
if not os.path.exists(base_file):
    raise FileNotFoundError(f"BASE file not found: {base_file}")
if not os.path.exists(btdtl_file):
    raise FileNotFoundError(f"BTDTL file not found: {btdtl_file}")

# Read BTBASE SAS file using pyreadstat
df_base, base_meta = pyreadstat.read_sas7bdat(base_file)
base = pa.Table.from_pandas(df_base)
print(f"Loaded BTBASE file: {base_file}")
print(f"BTBASE metadata: {base_meta.column_names}")
print(f"BTBASE rows: {base.num_rows}")

# Read BTDTL SAS file using pyreadstat
df_btdtl, btdtl_meta = pyreadstat.read_sas7bdat(btdtl_file)
btdtl = pa.Table.from_pandas(df_btdtl)
print(f"Loaded BTDTL file: {btdtl_file}")
print(f"BTDTL metadata: {btdtl_meta.column_names}")
print(f"BTDTL rows: {btdtl.num_rows}")

# Register in DuckDB
con.register("btbase", base)
con.register("btdtl", btdtl)

# -------------------------
# Step 3: Transform BASE (PREOUTSTD)
# -------------------------
base_trans = con.execute("""
    SELECT ACCTNO, TRANSREF, OUTSTAND AS PREOUTSTD
    FROM btbase
    GROUP BY ACCTNO, TRANSREF, OUTSTAND
""").fetch_arrow_table()

print(f"BASE transformation complete. Rows: {base_trans.num_rows}")

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
""").fetch_arrow_table()

print(f"BTDTL transformation complete. Rows: {btdtl_trans.num_rows}")

# -------------------------
# Step 5: Merge BASE + BTDTL
# -------------------------
con.register("btbase_trans", base_trans)
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
    FROM btbase_trans base
    INNER JOIN btdtl_trans b
    ON base.ACCTNO = b.ACCTNO
   AND base.TRANSREF = b.TRANSREF
""").fetch_arrow_table()

print(f"Merge complete. Combined rows: {combt.num_rows}")

# -------------------------
# Step 6: Export to fixed-width style file
# -------------------------
# Create output directory if it doesn't exist
os.makedirs(OUTPUT_PATH, exist_ok=True)

output_file = os.path.join(OUTPUT_PATH, f"DAYBTRD_{rdate}.txt")

with open(output_file, "w") as f:
    for row in combt.to_pylist():
        # Format each field according to specifications
        branch = str(row['BRANCH']).rjust(5)
        acctno = str(row['ACCTNO']).rjust(10)
        transref = str(row['TRANSREF']).rjust(10)
        prodtype = str(row['PRODTYPE']).zfill(3)
        preoutstd = f"{row['PREOUTSTD']:017.2f}"
        outstanding = f"{row['OUTSTAND']:017.2f}"
        overdue = str(row['OVERDUE']).rjust(10)
        recovamt = f"{row['RECOVAMT']:017.2f}"
        facility = str(row['FACILITY']).ljust(5)
        
        f.write(f"{branch}{acctno}{transref}{prodtype}{preoutstd}{outstanding}{overdue}{recovamt}{facility}\n")

print(f"Report generated: {output_file}")
print(f"Total records written: {combt.num_rows}")

# Optional: Display sample of output
if combt.num_rows > 0:
    print("\nSample output (first 5 rows):")
    sample = combt.slice(0, min(5, combt.num_rows))
    for row in sample.to_pylist():
        print(f"ACCTNO: {row['ACCTNO']}, PRODTYPE: {row['PRODTYPE']}, "
              f"PREOUTSTD: {row['PREOUTSTD']:.2f}, OUTSTAND: {row['OUTSTAND']:.2f}")

# Close connection
con.close()
