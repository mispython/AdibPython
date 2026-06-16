# EIID2CBT_ISLAMIC_COMBINED_BASE_DTL_REPORT

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
    'ibtbase': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/',  # Islamic BASE
    'ibtdtl': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/'    # Islamic BTDTL
}

OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/output'  

# -------------------------
# Step 1: Prepare report dates (matching SAS logic)
# -------------------------
today = date.today()
reptdate = today - timedelta(days=1)  # Using yesterday's date

# PREVDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))-1
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# Next month start (matching SAS logic)
if reptdate.month + 1 == 13:
    mm = 1
    yy = reptdate.year + 1
else:
    mm = reptdate.month + 1
    yy = reptdate.year
sdate = date(yy, mm, 1)

# Equivalent macro values (matching SAS CALL SYMPUT)
reptyear = reptdate.strftime("%y")  # YEAR2. format
reptmon = reptdate.strftime("%m")   # Z2. format
reptday = reptdate.strftime("%d")   # Z2. format
rdate = reptdate.strftime("%Y%m%d") # YYMMDDN. format
prevmon = prevdate.strftime("%m")   # Z2. format
sdate_ordinal = sdate.toordinal()   # For calculations

print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
print(f"Previous Month: {prevmon}")
print(f"Next Month Start: {sdate.strftime('%Y-%m-%d')}")

# -------------------------
# Step 2: Load Islamic BASE & BTDTL data
# -------------------------
con = duckdb.connect()

# Input file paths - matching SAS naming convention
# BASE.IBTBASE&PREVMON -> ibtbase{prevmon}.sas7bdat
base_file = os.path.join(INPUT_PATHS['ibtbase'], f"ibtbase{prevmon}.sas7bdat")
# BT.IBTDTL&REPTYEAR&REPTMON&REPTDAY -> ibtdtl{reptyear}{reptmon}{reptday}.sas7bdat
btdtl_file = os.path.join(INPUT_PATHS['ibtdtl'], f"ibtdtl{reptyear}{reptmon}{reptday}.sas7bdat")

# Check if files exist
if not os.path.exists(base_file):
    raise FileNotFoundError(f"Islamic BASE file not found: {base_file}")
if not os.path.exists(btdtl_file):
    raise FileNotFoundError(f"Islamic BTDTL file not found: {btdtl_file}")

# Read Islamic BTBASE SAS file using pyreadstat
# Keeping only ACCTNO, TRANSREF, OUTSTAND (renamed to PREOUTSTD) - matching SAS DATA BASE step
df_base = pyreadstat.read_sas7bdat(base_file)[0]
# Select only needed columns and rename OUTSTAND to PREOUTSTD
df_base = df_base[['ACCTNO', 'TRANSREF', 'OUTSTAND']].rename(columns={'OUTSTAND': 'PREOUTSTD'})
base = pa.Table.from_pandas(df_base)
print(f"Loaded Islamic BTBASE file: {base_file}")
print(f"Islamic BTBASE columns: {base.column_names}")
print(f"Islamic BTBASE rows: {base.num_rows}")

# Read Islamic BTDTL SAS file using pyreadstat
# Keeping BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY - matching SAS DATA BTDTL step
df_btdtl = pyreadstat.read_sas7bdat(btdtl_file)[0]
# Select needed columns
df_btdtl = df_btdtl[['BRANCH', 'ACCTNO', 'TRANSREF', 'PRODTYPE', 'OUTSTAND', 'MATDATE', 'FACILITY', 'RETAILID']].copy()
# Apply RETAILID logic: IF RETAILID = 'R' THEN PRODTYPE=000; IF RETAILID = 'C' THEN PRODTYPE=999
df_btdtl.loc[df_btdtl['RETAILID'] == 'R', 'PRODTYPE'] = 0
df_btdtl.loc[df_btdtl['RETAILID'] == 'C', 'PRODTYPE'] = 999
# Drop RETAILID as it's no longer needed
df_btdtl = df_btdtl.drop('RETAILID', axis=1)
btdtl = pa.Table.from_pandas(df_btdtl)
print(f"Loaded Islamic BTDTL file: {btdtl_file}")
print(f"Islamic BTDTL columns: {btdtl.column_names}")
print(f"Islamic BTDTL rows: {btdtl.num_rows}")

# Register in DuckDB
con.register("base", base)
con.register("btdtl", btdtl)

# -------------------------
# Step 3: Transform BASE - Remove duplicates (matching PROC SORT NODUPKEY)
# -------------------------
base_trans = con.execute("""
    SELECT ACCTNO, TRANSREF, PREOUTSTD
    FROM base
    GROUP BY ACCTNO, TRANSREF, PREOUTSTD
""").fetch_arrow_table()

print(f"Islamic BASE deduplicated. Rows: {base_trans.num_rows}")

# -------------------------
# Step 4: Transform BTDTL - Remove duplicates (matching PROC SORT NODUPKEY)
# -------------------------
btdtl_trans = con.execute("""
    SELECT BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY
    FROM btdtl
    GROUP BY BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY
""").fetch_arrow_table()

print(f"Islamic BTDTL deduplicated. Rows: {btdtl_trans.num_rows}")

# -------------------------
# Step 5: Merge BASE + BTDTL (matching MERGE BASE(IN=A) BTDTL(IN=B); IF A;)
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
        CAST(({sdate_ordinal} - b.MATDATE) AS INT) AS OVERDUE,
        CAST((base.PREOUTSTD - b.OUTSTAND) AS DOUBLE) AS RECOVAMT,
        b.FACILITY
    FROM base_trans base
    INNER JOIN btdtl_trans b
    ON base.ACCTNO = b.ACCTNO
   AND base.TRANSREF = b.TRANSREF
""").fetch_arrow_table()

print(f"Islamic merge complete. Combined rows: {combt.num_rows}")

# -------------------------
# Step 6: Export to fixed-width file (matching SAS PUT format)
# -------------------------
# Create output directory if it doesn't exist
os.makedirs(OUTPUT_PATH, exist_ok=True)

output_file = os.path.join(OUTPUT_PATH, f"DAYBTRD_{rdate}.txt")

with open(output_file, "w") as f:
    for row in combt.to_pylist():
        # Format according to SAS PUT specifications:
        # @001 BRANCH      5.    (right-aligned, 5 characters)
        # @007 ACCTNO     10.    (right-aligned, 10 characters)
        # @018 TRANSREF   10.    (right-aligned, 10 characters)
        # @029 PRODTYPE   Z3.    (zero-padded, 3 characters)
        # @033 PREOUTSTD  17.2   (17 chars with 2 decimals, right-aligned)
        # @051 OUTSTAND   17.2   (17 chars with 2 decimals, right-aligned)
        # @069 OVERDUE    10.    (right-aligned, 10 characters)
        # @080 RECOVAMT   17.2   (17 chars with 2 decimals, right-aligned)
        # @098 FACILITY   $5.    (left-aligned, 5 characters)
        
        branch = str(row['BRANCH']).rjust(5)
        acctno = str(row['ACCTNO']).rjust(10)
        transref = str(row['TRANSREF']).rjust(10)
        prodtype = str(row['PRODTYPE']).zfill(3)
        preoutstd = f"{row['PREOUTSTD']:017.2f}"
        outstanding = f"{row['OUTSTAND']:017.2f}"
        overdue = str(row['OVERDUE']).rjust(10)
        recovamt = f"{row['RECOVAMT']:017.2f}"
        facility = str(row['FACILITY']).ljust(5) if row['FACILITY'] is not None else "     "
        
        f.write(f"{branch}{acctno}{transref}{prodtype}{preoutstd}{outstanding}{overdue}{recovamt}{facility}\n")

print(f"Islamic BT Report generated: {output_file}")
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
