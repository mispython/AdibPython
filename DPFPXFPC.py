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
    'btbase': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/btrade', 
    'btdtl': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/btrade'
}

OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBD2CBT'

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

# Debug: check join key dtypes to catch silent type-mismatch issues
print(f"BASE ACCTNO dtype: {base.schema.field('ACCTNO').type}")
print(f"BTDTL ACCTNO dtype: {btdtl.schema.field('ACCTNO').type}")
print(f"BASE TRANSREF dtype: {base.schema.field('TRANSREF').type}")
print(f"BTDTL TRANSREF dtype: {btdtl.schema.field('TRANSREF').type}")

# Register in DuckDB
con.register("btbase", base)
con.register("btdtl", btdtl)

# -------------------------
# Diagnostic: inspect RETAILID / SUBPROD distributions
# -------------------------
print("\n--- RETAILID distribution in BTDTL (raw) ---")
for r in con.execute("""
    SELECT RETAILID, LENGTH(RETAILID) AS len, COUNT(*) AS cnt
    FROM btdtl
    GROUP BY RETAILID, LENGTH(RETAILID)
    ORDER BY cnt DESC
    LIMIT 20
""").fetchall():
    print(f"  RETAILID={r[0]!r} len={r[1]} count={r[2]}")

print("\n--- SUBPROD sample values in BTDTL ---")
for r in con.execute("""
    SELECT SUBPROD, COUNT(*) AS cnt
    FROM btdtl
    GROUP BY SUBPROD
    ORDER BY cnt DESC
    LIMIT 20
""").fetchall():
    print(f"  SUBPROD={r[0]!r} count={r[1]}")

# -------------------------
# Step 3: Transform BASE (PREOUTSTD) - matching DATA BASE step
# SAS: KEEP=ACCTNO TRANSREF OUTSTAND RENAME=(OUTSTAND=PREOUTSTD)
# -------------------------
base_trans = con.execute("""
    SELECT ACCTNO, TRANSREF, OUTSTAND AS PREOUTSTD
    FROM btbase
""").fetch_arrow_table()

print(f"\nBASE transformation complete. Rows: {base_trans.num_rows}")

# -------------------------
# Step 4: Transform BTDTL (assign PRODTYPE) - matching DATA BTDTL step
# SAS: KEEP BRANCH ACCTNO TRANSREF PRODTYPE OUTSTAND MATDATE FACILITY
#      IF RETAILID = 'R' THEN PRODTYPE=000;
#      IF RETAILID = 'C' THEN PRODTYPE=999;
#
# Notes:
#  - RETAILID is normalized with UPPER(TRIM()) to handle SAS trailing blanks.
#  - RETAILID kept in output for the dedup tiebreaker (dropped before merge).
#  - TRY_CAST prevents crashes on non-numeric SUBPROD/PRODGRP codes.
# -------------------------
btdtl_cols = set(btdtl.column_names)

if 'PRODTYPE' in btdtl_cols:
    prodtype_expr = "COALESCE(TRY_CAST(PRODTYPE AS INT), 999)"
    print("Using PRODTYPE column from BTDTL (with safe TRY_CAST)")
elif 'SUBPROD' in btdtl_cols:
    prodtype_expr = "COALESCE(TRY_CAST(SUBPROD AS INT), 999)"
    print("Using SUBPROD column for PRODTYPE (with safe TRY_CAST)")
elif 'PRODGRP' in btdtl_cols:
    prodtype_expr = "COALESCE(TRY_CAST(PRODGRP AS INT), 999)"
    print("Using PRODGRP column for PRODTYPE (with safe TRY_CAST)")
else:
    prodtype_expr = "999"
    print("No product-type column found; defaulting PRODTYPE to 999")

btdtl_trans = con.execute(f"""
    SELECT 
        BRANCH, 
        ACCTNO, 
        TRANSREF,
        CASE 
            WHEN UPPER(TRIM(RETAILID)) = 'R' THEN 0
            WHEN UPPER(TRIM(RETAILID)) = 'C' THEN 999
            ELSE {prodtype_expr}
        END AS PRODTYPE,
        OUTSTAND, 
        MATDATE, 
        FACILITY,
        UPPER(TRIM(RETAILID)) AS RETAILID_NORM
    FROM btdtl
""").fetch_arrow_table()

print(f"BTDTL transformation complete. Rows: {btdtl_trans.num_rows}")

# -------------------------
# Step 5: Deduplicate by ACCTNO, TRANSREF (matching PROC SORT NODUPKEY)
# Tiebreaker: prefer non-'R' rows so PRODTYPE reflects the real product row.
# -------------------------
base_dedup = con.execute("""
    SELECT ACCTNO, TRANSREF, PREOUTSTD
    FROM (
        SELECT ACCTNO, TRANSREF, PREOUTSTD,
               ROW_NUMBER() OVER (PARTITION BY ACCTNO, TRANSREF ORDER BY PREOUTSTD) AS rn
        FROM base_trans
    )
    WHERE rn = 1
""").fetch_arrow_table()

print(f"BASE deduplicated by ACCTNO/TRANSREF. Rows: {base_dedup.num_rows}")

con.register("btdtl_trans", btdtl_trans)

btdtl_dedup = con.execute("""
    SELECT BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY
    FROM (
        SELECT BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY,
               ROW_NUMBER() OVER (
                   PARTITION BY ACCTNO, TRANSREF
                   ORDER BY 
                       CASE WHEN RETAILID_NORM = 'R' THEN 1 ELSE 0 END,
                       BRANCH
               ) AS rn
        FROM btdtl_trans
    )
    WHERE rn = 1
""").fetch_arrow_table()

print(f"BTDTL deduplicated by ACCTNO/TRANSREF. Rows: {btdtl_dedup.num_rows}")

# Diagnostic: PRODTYPE distribution after dedup
print("\n--- PRODTYPE distribution after dedup ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt
    FROM btdtl_dedup
    GROUP BY PRODTYPE
    ORDER BY cnt DESC
    LIMIT 20
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 6: Merge BASE + BTDTL - matching MERGE BASE(IN=A) BTDTL(IN=B); IF A;
# LEFT JOIN from BASE keeps all BASE records (IF A semantics)
# -------------------------
con.register("base_dedup", base_dedup)
con.register("btdtl_dedup", btdtl_dedup)

combt = con.execute(f"""
    SELECT 
        b.BRANCH,
        base.ACCTNO,
        base.TRANSREF,
        b.PRODTYPE,
        base.PREOUTSTD,
        b.OUTSTAND,
        CAST(({sdate.toordinal()} - b.MATDATE) AS INT) AS OVERDUE,
        CAST((base.PREOUTSTD - b.OUTSTAND) AS DOUBLE) AS RECOVAMT,
        b.FACILITY
    FROM base_dedup base
    LEFT JOIN btdtl_dedup b
    ON base.ACCTNO = b.ACCTNO
   AND base.TRANSREF = b.TRANSREF
""").fetch_arrow_table()

print(f"\nMerge complete (BASE left join BTDTL). Combined rows: {combt.num_rows}")

# Diagnostic: PRODTYPE distribution after merge
print("\n--- PRODTYPE distribution after merge ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt
    FROM combt
    GROUP BY PRODTYPE
    ORDER BY cnt DESC
    LIMIT 20
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 7: Export to fixed-width style file (matching SAS PUT)
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
        prodtype = str(row['PRODTYPE']).zfill(3) if row['PRODTYPE'] is not None else "999"
        preoutstd = f"{row['PREOUTSTD']:017.2f}" if row['PREOUTSTD'] is not None else f"{0:017.2f}"
        outstanding = f"{row['OUTSTAND']:017.2f}" if row['OUTSTAND'] is not None else f"{0:017.2f}"
        overdue = str(row['OVERDUE']).rjust(10) if row['OVERDUE'] is not None else "         0"
        recovamt = f"{row['RECOVAMT']:017.2f}" if row['RECOVAMT'] is not None else f"{0:017.2f}"
        facility = str(row['FACILITY']).ljust(5) if row['FACILITY'] is not None else "     "
        
        f.write(f"{branch}{acctno}{transref}{prodtype}{preoutstd}{outstanding}{overdue}{recovamt}{facility}\n")

print(f"\nReport generated: {output_file}")
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
