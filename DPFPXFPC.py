# EIID2CBT_ISLAMIC_COMBINED_BASE_DTL_REPORT

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import date, datetime, timedelta
import os

# -------------------------
# SAS epoch: SAS day numbers count days since 1960-01-01
# -------------------------
SAS_EPOCH = date(1960, 1, 1)

# -------------------------
# SUBPROD -> PRODTYPE mapping (Option 2)
# ---------------------------------------------------------------
# SAS reads PRODTYPE directly from BT.IBTDTL..., then overrides for
# RETAILID='R' (->0) and RETAILID='C' (->999). The raw sas7bdat only
# carries SUBPROD (character). To reproduce SAS output we map SUBPROD
# codes to the numeric PRODTYPE values that SAS's BT.IBTDTL dataset
# uses.
#
# >>> REPLACE the numeric values below with the real Islamic mapping <<<
# Extract with:
#   PROC SQL;
#     SELECT DISTINCT SUBPROD, PRODTYPE
#     FROM BT.IBTDTL&REPTYEAR&REPTMON&REPTDAY
#     WHERE RETAILID NOT IN ('R','C')
#     ORDER BY SUBPROD;
#   QUIT;
# ---------------------------------------------------------------
SUBPROD_TO_PRODTYPE = {
    'BA':       100,   # <-- replace with real Islamic values
    'BG':       101,
    'BGF':      102,
    'TR':       103,
    'PDB':      104,
    'LC BL':    105,
    'BRULC':    106,
    'SG':       107,
    'FCTL':     108,
    'SBL':      109,
    'ECR PRE':  110,
    'LC DO':    111,
    'FBEP/AP':  112,
    'TBL':      113,
    'ECR POST': 114,
    'SBLC':     115,
    'FBEP/DOC': 116,
}

_default_prodtype = 999
_case_lines = "\n".join(
    f"        WHEN '{k}' THEN {v}" for k, v in SUBPROD_TO_PRODTYPE.items()
)
prodtype_expr_subprod = f"""CASE SUBPROD
{_case_lines}
        ELSE {_default_prodtype}
    END"""

# -------------------------
# Configuration Paths
# -------------------------
INPUT_PATHS = {
    'ibtbase': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/',  # Islamic BASE
    'ibtdtl': '/dwh/ibtrade_d'    # Islamic BTDTL
}

OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIID2CBT'

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
sdate_sas = (sdate - SAS_EPOCH).days   # SAS day number for SDATE

# Equivalent macro values (matching SAS CALL SYMPUT)
reptyear = reptdate.strftime("%y")  # YEAR2. format
reptmon = reptdate.strftime("%m")   # Z2. format
reptday = reptdate.strftime("%d")   # Z2. format
rdate = reptdate.strftime("%Y%m%d") # YYMMDDN. format
prevmon = prevdate.strftime("%m")   # Z2. format

print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
print(f"Previous Month: {prevmon}")
print(f"Next Month Start: {sdate.strftime('%Y-%m-%d')}")
print(f"SDATE (SAS day number): {sdate_sas}")

# -------------------------
# Step 2: Load Islamic BASE & BTDTL data
# -------------------------
con = duckdb.connect()

# Input file paths - matching SAS naming convention
base_file = os.path.join(INPUT_PATHS['ibtbase'], f"ibtbase{prevmon}.sas7bdat")
btdtl_file = os.path.join(INPUT_PATHS['ibtdtl'], f"ibtdtl{reptyear}{reptmon}{reptday}.sas7bdat")

# Check if files exist
if not os.path.exists(base_file):
    raise FileNotFoundError(f"Islamic BASE file not found: {base_file}")
if not os.path.exists(btdtl_file):
    raise FileNotFoundError(f"Islamic BTDTL file not found: {btdtl_file}")

# Read Islamic BTBASE SAS file
df_base, base_meta = pyreadstat.read_sas7bdat(base_file)
base = pa.Table.from_pandas(df_base)
print(f"Loaded Islamic BTBASE file: {base_file}")
print(f"Islamic BTBASE metadata: {base_meta.column_names}")
print(f"Islamic BTBASE rows: {base.num_rows}")

# Read Islamic BTDTL SAS file
df_btdtl, btdtl_meta = pyreadstat.read_sas7bdat(btdtl_file)
btdtl = pa.Table.from_pandas(df_btdtl)
print(f"Loaded Islamic BTDTL file: {btdtl_file}")
print(f"Islamic BTDTL metadata: {btdtl_meta.column_names}")
print(f"Islamic BTDTL rows: {btdtl.num_rows}")

# Debug: dtypes
print(f"BASE ACCTNO dtype: {base.schema.field('ACCTNO').type}")
print(f"BTDTL ACCTNO dtype: {btdtl.schema.field('ACCTNO').type}")
print(f"BASE TRANSREF dtype: {base.schema.field('TRANSREF').type}")
print(f"BTDTL TRANSREF dtype: {btdtl.schema.field('TRANSREF').type}")
print(f"BTDTL MATDATE dtype: {btdtl.schema.field('MATDATE').type}")

# Register in DuckDB
con.register("ibtbase", base)
con.register("ibtdtl", btdtl)

# -------------------------
# Step 3: Transform BASE (PREOUTSTD) with normalized keys
# SAS: KEEP=ACCTNO TRANSREF OUTSTAND RENAME=(OUTSTAND=PREOUTSTD)
# -------------------------
base_trans = con.execute("""
    SELECT 
        TRIM(ACCTNO)  AS ACCTNO,
        TRIM(TRANSREF) AS TRANSREF,
        OUTSTAND AS PREOUTSTD
    FROM ibtbase
""").fetch_arrow_table()

print(f"\nIslamic BASE transformation complete. Rows: {base_trans.num_rows}")

# -------------------------
# Step 4: Transform BTDTL (assign PRODTYPE) - Option 2
# SAS: KEEP BRANCH ACCTNO TRANSREF PRODTYPE OUTSTAND MATDATE FACILITY
#      IF RETAILID = 'R' THEN PRODTYPE=000;
#      IF RETAILID = 'C' THEN PRODTYPE=999;
# -------------------------
btdtl_cols = set(btdtl.column_names)

if 'PRODTYPE' in btdtl_cols:
    print("Using numeric PRODTYPE column from Islamic BTDTL")
    prodtype_expr = "COALESCE(TRY_CAST(PRODTYPE AS INT), 999)"
elif 'SUBPROD' in btdtl_cols:
    print("Reconstructing PRODTYPE from SUBPROD mapping (Islamic)")
    prodtype_expr = prodtype_expr_subprod
elif 'PRODGRP' in btdtl_cols:
    print("Using PRODGRP column for PRODTYPE (with safe TRY_CAST)")
    prodtype_expr = "COALESCE(TRY_CAST(PRODGRP AS INT), 999)"
else:
    print("No product-type column found; defaulting PRODTYPE to 999")
    prodtype_expr = "999"

btdtl_trans = con.execute(f"""
    SELECT 
        BRANCH,
        TRIM(ACCTNO)  AS ACCTNO,
        TRIM(TRANSREF) AS TRANSREF,
        CASE 
            WHEN UPPER(TRIM(RETAILID)) = 'R' THEN 0
            WHEN UPPER(TRIM(RETAILID)) = 'C' THEN 999
            ELSE {prodtype_expr}
        END AS PRODTYPE,
        OUTSTAND,
        MATDATE,
        FACILITY,
        UPPER(TRIM(RETAILID)) AS RETAILID_NORM
    FROM ibtdtl
""").fetch_arrow_table()

print(f"Islamic BTDTL transformation complete. Rows: {btdtl_trans.num_rows}")

# -------------------------
# Step 5: Deduplicate by ACCTNO, TRANSREF (matching PROC SORT NODUPKEY)
# -------------------------
base_dedup = con.execute("""
    SELECT ACCTNO, TRANSREF, PREOUTSTD
    FROM (
        SELECT ACCTNO, TRANSREF, PREOUTSTD,
               ROW_NUMBER() OVER (PARTITION BY ACCTNO, TRANSREF ORDER BY PREOUTSTD) AS rn
        FROM base_trans
    ) WHERE rn = 1
""").fetch_arrow_table()

print(f"Islamic BASE deduplicated. Rows: {base_dedup.num_rows}")

con.register("btdtl_trans", btdtl_trans)

btdtl_dedup = con.execute("""
    SELECT BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY
    FROM (
        SELECT BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY,
               ROW_NUMBER() OVER (
                   PARTITION BY ACCTNO, TRANSREF
                   ORDER BY CASE WHEN RETAILID_NORM = 'R' THEN 1 ELSE 0 END, BRANCH
               ) AS rn
        FROM btdtl_trans
    ) WHERE rn = 1
""").fetch_arrow_table()

print(f"Islamic BTDTL deduplicated. Rows: {btdtl_dedup.num_rows}")

print("\n--- PRODTYPE distribution after dedup ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt FROM btdtl_dedup GROUP BY PRODTYPE ORDER BY cnt DESC LIMIT 30
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 6: Merge BASE + BTDTL (LEFT JOIN, IF A semantics)
# -------------------------
con.register("base_dedup", base_dedup)
con.register("btdtl_dedup", btdtl_dedup)

combt = con.execute("""
    SELECT 
        b.BRANCH,
        base.ACCTNO,
        base.TRANSREF,
        b.PRODTYPE,
        base.PREOUTSTD,
        b.OUTSTAND,
        b.MATDATE,
        b.FACILITY
    FROM base_dedup base
    LEFT JOIN btdtl_dedup b
    ON base.ACCTNO = b.ACCTNO
   AND base.TRANSREF = b.TRANSREF
""").fetch_arrow_table()

print(f"\nIslamic merge complete. Combined rows: {combt.num_rows}")

print("\n--- PRODTYPE distribution after merge ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt FROM combt GROUP BY PRODTYPE ORDER BY cnt DESC LIMIT 30
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 7: Export to fixed-width file (matching SAS PUT + MISSING=0)
# -------------------------
# SAS PUT positions:
#   @001 BRANCH      5.
#   @007 ACCTNO     10.
#   @018 TRANSREF   10.
#   @029 PRODTYPE   Z3.
#   @033 PREOUTSTD  17.2
#   @051 OUTSTAND   17.2
#   @069 OVERDUE    10.
#   @080 RECOVAMT   17.2
#   @098 FACILITY   $5.
# -------------------------
os.makedirs(OUTPUT_PATH, exist_ok=True)

output_file = os.path.join(OUTPUT_PATH, f"DAYBTRD_{rdate}.txt")

with open(output_file, "w") as f:
    for row in combt.to_pylist():
        branch = str(row['BRANCH']).rjust(5) if row['BRANCH'] is not None else "     "
        acctno = str(row['ACCTNO']).rjust(10)
        transref = str(row['TRANSREF']).rjust(10)

        # PRODTYPE: None -> 000 (SAS MISSING=0 with Z3. format)
        prodtype = str(row['PRODTYPE']).zfill(3) if row['PRODTYPE'] is not None else "000"

        preoutstd_val = row['PREOUTSTD'] if row['PREOUTSTD'] is not None else 0.0
        preoutstd = f"{preoutstd_val:017.2f}"

        outstand_val = row['OUTSTAND'] if row['OUTSTAND'] is not None else 0.0
        outstanding = f"{outstand_val:017.2f}"

        # OVERDUE = SDATE - MATDATE; MATDATE missing -> 0
        matdate = row['MATDATE']
        if matdate is None:
            overdue_val = sdate_sas - 0
        elif isinstance(matdate, (date, datetime)):
            md = matdate.date() if isinstance(matdate, datetime) else matdate
            matdate_sas = (md - SAS_EPOCH).days
            overdue_val = sdate_sas - matdate_sas
        else:
            overdue_val = sdate_sas - int(matdate)
        overdue = str(int(overdue_val)).rjust(10)

        # RECOVAMT = PREOUTSTD - OUTSTAND; OUTSTAND missing -> 0
        recovamt_val = preoutstd_val - outstand_val
        recovamt = f"{recovamt_val:017.2f}"

        facility = str(row['FACILITY']).ljust(5) if row['FACILITY'] is not None else "     "

        f.write(f"{branch}{acctno}{transref}{prodtype}{preoutstd}{outstanding}{overdue}{recovamt}{facility}\n")

print(f"\nIslamic BT Report generated: {output_file}")
print(f"Total records written: {combt.num_rows}")

# Optional: Display sample of output
if combt.num_rows > 0:
    print("\nSample output (first 5 rows):")
    sample = combt.slice(0, min(5, combt.num_rows))
    for row in sample.to_pylist():
        print(f"ACCTNO: {row['ACCTNO']}, PRODTYPE: {row['PRODTYPE']}, "
              f"PREOUTSTD: {row['PREOUTSTD']:.2f}, OUTSTAND: {row['OUTSTAND']:.2f}, "
              f"MATDATE: {row['MATDATE']}")

# Close connection
con.close()
