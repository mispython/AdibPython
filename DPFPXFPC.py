# EIBD2CBT_COMBINED_BASE_DTL_REPORT

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import date, datetime, timedelta
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

# Debug: check join key dtypes
print(f"BASE ACCTNO dtype: {base.schema.field('ACCTNO').type}")
print(f"BTDTL ACCTNO dtype: {btdtl.schema.field('ACCTNO').type}")
print(f"BASE TRANSREF dtype: {base.schema.field('TRANSREF').type}")
print(f"BTDTL TRANSREF dtype: {btdtl.schema.field('TRANSREF').type}")
print(f"BTDTL MATDATE dtype: {btdtl.schema.field('MATDATE').type}")

# Register in DuckDB
con.register("btbase", base)
con.register("btdtl", btdtl)

# -------------------------
# Diagnostic Block: key match analysis
# -------------------------
print("\n--- BASE sample keys (first 10) ---")
for r in con.execute("SELECT ACCTNO, TRANSREF FROM btbase LIMIT 10").fetchall():
    print(f"  ACCTNO={r[0]!r} TRANSREF={r[1]!r}")

print("\n--- BTDTL sample keys (first 10) ---")
for r in con.execute("SELECT ACCTNO, TRANSREF FROM btdtl LIMIT 10").fetchall():
    print(f"  ACCTNO={r[0]!r} TRANSREF={r[1]!r}")

print("\n--- BASE key length distribution ---")
for r in con.execute("""
    SELECT LENGTH(ACCTNO) AS acct_len, LENGTH(TRANSREF) AS ref_len, COUNT(*) AS cnt
    FROM btbase GROUP BY 1,2 ORDER BY cnt DESC
""").fetchall():
    print(f"  acct_len={r[0]} ref_len={r[1]} count={r[2]}")

print("\n--- BTDTL key length distribution ---")
for r in con.execute("""
    SELECT LENGTH(ACCTNO) AS acct_len, LENGTH(TRANSREF) AS ref_len, COUNT(*) AS cnt
    FROM btdtl GROUP BY 1,2 ORDER BY cnt DESC LIMIT 10
""").fetchall():
    print(f"  acct_len={r[0]} ref_len={r[1]} count={r[2]}")

print("\n--- Exact key match count ---")
print(con.execute("""
    SELECT COUNT(*) FROM btbase b
    WHERE EXISTS (SELECT 1 FROM btdtl d 
                  WHERE d.ACCTNO = b.ACCTNO AND d.TRANSREF = b.TRANSREF)
""").fetchone())

print("\n--- TRIM-normalized key match count ---")
print(con.execute("""
    SELECT COUNT(*) FROM btbase b
    WHERE EXISTS (SELECT 1 FROM btdtl d 
                  WHERE TRIM(d.ACCTNO) = TRIM(b.ACCTNO) 
                    AND TRIM(d.TRANSREF) = TRIM(b.TRANSREF))
""").fetchone())

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
# Step 3: Transform BASE (PREOUTSTD) with normalized keys
# SAS: KEEP=ACCTNO TRANSREF OUTSTAND RENAME=(OUTSTAND=PREOUTSTD)
# -------------------------
base_trans = con.execute("""
    SELECT 
        TRIM(ACCTNO)  AS ACCTNO,
        TRIM(TRANSREF) AS TRANSREF,
        OUTSTAND AS PREOUTSTD
    FROM btbase
""").fetch_arrow_table()

print(f"\nBASE transformation complete. Rows: {base_trans.num_rows}")

# -------------------------
# Step 4: Transform BTDTL (assign PRODTYPE) with normalized keys
# SAS: KEEP BRANCH ACCTNO TRANSREF PRODTYPE OUTSTAND MATDATE FACILITY
#      IF RETAILID = 'R' THEN PRODTYPE=000;
#      IF RETAILID = 'C' THEN PRODTYPE=999;
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
    FROM btdtl
""").fetch_arrow_table()

print(f"BTDTL transformation complete. Rows: {btdtl_trans.num_rows}")

# -------------------------
# Step 5: Deduplicate by ACCTNO, TRANSREF
# -------------------------
base_dedup = con.execute("""
    SELECT ACCTNO, TRANSREF, PREOUTSTD
    FROM (
        SELECT ACCTNO, TRANSREF, PREOUTSTD,
               ROW_NUMBER() OVER (PARTITION BY ACCTNO, TRANSREF ORDER BY PREOUTSTD) AS rn
        FROM base_trans
    ) WHERE rn = 1
""").fetch_arrow_table()

print(f"BASE deduplicated. Rows: {base_dedup.num_rows}")

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

print(f"BTDTL deduplicated. Rows: {btdtl_dedup.num_rows}")

print("\n--- PRODTYPE distribution after dedup ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt FROM btdtl_dedup GROUP BY PRODTYPE ORDER BY cnt DESC LIMIT 20
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 6: Merge BASE + BTDTL (LEFT JOIN, IF A semantics)
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

print(f"\nMerge complete. Combined rows: {combt.num_rows}")

print("\n--- PRODTYPE distribution after merge ---")
for r in con.execute("""
    SELECT PRODTYPE, COUNT(*) AS cnt FROM combt GROUP BY PRODTYPE ORDER BY cnt DESC LIMIT 20
""").fetchall():
    print(f"  PRODTYPE={r[0]} count={r[1]}")

# -------------------------
# Step 7: Export to fixed-width style file (matching SAS PUT + MISSING=0)
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
#
# MISSING=0 semantics:
#   - Numeric missing -> 0
#   - OVERDUE = SDATE - MATDATE (MATDATE missing -> 0)
#   - RECOVAMT = PREOUTSTD - OUTSTAND (OUTSTAND missing -> 0)
#   - Character missing -> blanks
# -------------------------
os.makedirs(OUTPUT_PATH, exist_ok=True)

output_file = os.path.join(OUTPUT_PATH, f"DAYBTRD_{rdate}.txt")

with open(output_file, "w") as f:
    for row in combt.to_pylist():
        # BRANCH: char, blanks if missing
        branch = str(row['BRANCH']).rjust(5) if row['BRANCH'] is not None else "     "

        # ACCTNO / TRANSREF: always present from BASE
        acctno = str(row['ACCTNO']).rjust(10)
        transref = str(row['TRANSREF']).rjust(10)

        # PRODTYPE: None -> 000 (SAS MISSING=0 with Z3. format)
        prodtype = str(row['PRODTYPE']).zfill(3) if row['PRODTYPE'] is not None else "000"

        # PREOUTSTD: always present from BASE
        preoutstd = f"{row['PREOUTSTD']:017.2f}" if row['PREOUTSTD'] is not None else f"{0:017.2f}"

        # OUTSTAND: None -> 0 (SAS MISSING=0)
        outstand_val = row['OUTSTAND'] if row['OUTSTAND'] is not None else 0.0
        outstanding = f"{outstand_val:017.2f}"

        # OVERDUE = SDATE - MATDATE; MATDATE missing -> 0
        matdate = row['MATDATE']
        if matdate is None:
            overdue_val = sdate.toordinal() - 0
        elif isinstance(matdate, (date, datetime)):
            # pyreadstat returned a Python date object
            overdue_val = (sdate - (matdate.date() if isinstance(matdate, datetime) else matdate)).days
        else:
            # pyreadstat returned a raw SAS day number
            overdue_val = sdate.toordinal() - int(matdate)
        overdue = str(int(overdue_val)).rjust(10)

        # RECOVAMT = PREOUTSTD - OUTSTAND; OUTSTAND missing -> 0
        preoutstd_val = row['PREOUTSTD'] if row['PREOUTSTD'] is not None else 0.0
        recovamt_val = preoutstd_val - outstand_val
        recovamt = f"{recovamt_val:017.2f}"

        # FACILITY: char, blanks if missing
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
              f"PREOUTSTD: {row['PREOUTSTD']}, OUTSTAND: {row['OUTSTAND']}")

# Close connection
con.close()
