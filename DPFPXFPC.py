# EIIDBT12_BANKTRADE_PM12.py
# Production-Ready Islamic Version - Auto-determines PREVMON

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import glob

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
INPUT_BTFILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/BTPM12.txt"

# BASE file pattern - {PREVMON} will be auto-detected
INPUT_BASE_FILE_PATTERN = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/ibtbase{PREVMON}.sas7bdat"

OUTPUT_TEXT_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.parquet"

# Optional: Override for testing (set to None for auto-detection)
# If you want to test with a specific month, set this to e.g., "04"
FORCE_PREVMON = None  # Set to None for auto-detection, or "04", "05", etc.

# SAS epoch (days since 1960-01-01)
SAS_EPOCH = date(1960, 1, 1)

def to_sas_date(dt):
    return (dt - SAS_EPOCH).days

# --------------------------------------------------------------------
# Step 1: Auto-determine dates (EXACTLY matching SAS logic)
# --------------------------------------------------------------------
# Use TODAY() as SAS does
today = date.today()

# REPTDATE = TODAY()-1
reptdate = today - timedelta(days=1)

# PREVDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))-1
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# Calculate PREVMON
prevmon = f"{prevdate.month:02d}"

# Allow override for testing
if FORCE_PREVMON is not None:
    prevmon = FORCE_PREVMON
    print(f"*** FORCED PREVMON = {prevmon} (for testing) ***")

# SDATE = First day of NEXT month
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

sdate_sas = to_sas_date(sdate)

params = {
    "TODAY": today,
    "REPTDATE": reptdate,
    "PREVDATE": prevdate,
    "PREVMON": prevmon,
    "SDATE": sdate,
    "SDATE_SAS": sdate_sas,
}

print("=" * 80)
print("EIIDBT12 - Islamic Bank Trade Report")
print("=" * 80)
print(f"TODAY: {params['TODAY']}")
print(f"REPTDATE: {params['REPTDATE']}")
print(f"PREVDATE: {params['PREVDATE']}")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE: {params['SDATE']}")
print(f"SDATE_SAS: {params['SDATE_SAS']}")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Find the BASE file
# --------------------------------------------------------------------
def find_base_file(prevmon):
    """Find the BASE file for the given month"""
    base_dir = os.path.dirname(INPUT_BASE_FILE_PATTERN)
    
    # Try different naming patterns
    patterns = [
        INPUT_BASE_FILE_PATTERN.format(PREVMON=prevmon),
        os.path.join(base_dir, f"ibtbase{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"ibtbase_{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"IBTBASE{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"IBTBASE_{prevmon}.sas7bdat"),
    ]
    
    for pattern in patterns:
        if os.path.exists(pattern):
            return pattern
    
    # If not found, try to find any ibtbase file
    all_files = glob.glob(os.path.join(base_dir, "ibtbase*.sas7bdat"))
    if all_files:
        print(f"\nAvailable IBTBASE files in {base_dir}:")
        for f in sorted(all_files):
            print(f"  - {os.path.basename(f)}")
        
        # If the specific month isn't found, use the latest one
        latest = sorted(all_files)[-1]
        print(f"\nUsing latest available: {os.path.basename(latest)}")
        return latest
    
    raise FileNotFoundError(f"No IBTBASE file found for month {prevmon} in {base_dir}")

base_file = find_base_file(prevmon)
print(f"\nUsing BASE file: {base_file}")

# --------------------------------------------------------------------
# Step 3: Read BTDTL input
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    data = []
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n').rstrip('\r')
            if not line.strip():
                continue
            if line_num == 1 and line.strip().startswith('1BKT'):
                continue
            
            try:
                branch_str = line[1:5].strip()
                acctno_str = line[5:15].strip()
                transref = line[15:22].strip()
                outstanding_str = line[22:37].strip()
                matdt = line[37:43].strip()
                liabcode = line[42:45].strip()
                
                if not acctno_str.isdigit() or len(acctno_str) != 10:
                    continue
                if not matdt.isdigit() or len(matdt) != 6:
                    continue
                
                branch = int(branch_str) if branch_str.isdigit() else 0
                acctno = int(acctno_str)
                outstanding = float(outstanding_str) if outstanding_str else 0.0
                
                month = int(matdt[2:4])
                day = int(matdt[4:6])
                year = 2000 + int(matdt[0:2])
                
                matdate = date(year, month, day)
                matdate_sas = to_sas_date(matdate)
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref,
                    'OUTSTAND': outstanding,
                    'MATDATE_SAS': matdate_sas,
                    'LIABCODE': liabcode,
                    'FACILITY': '99999',
                })
            except Exception:
                continue
    
    print(f"BTDTL: Parsed {len(data)} records")
    return pl.DataFrame(data)

btdtl = read_btdtl_text(INPUT_BTFILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

# --------------------------------------------------------------------
# Step 4: Apply ISLAMIC filter
# --------------------------------------------------------------------
btdtl = btdtl.filter(
    (pl.col("BRANCH") > 3000) &
    (pl.col("ACCTNO") >= 2850000000) &
    (pl.col("ACCTNO") <= 2859999999)
)

print(f"BTDTL after Islamic filter: {len(btdtl)} records")

# --------------------------------------------------------------------
# Step 5: Read Islamic BASE dataset
# --------------------------------------------------------------------
def read_base_sas(filepath):
    print(f"\nReading Islamic BASE: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(filepath)
    base = pl.from_pandas(df)
    
    print(f"BASE columns: {base.columns}")
    print(f"BASE records: {len(base)}")
    
    col_names = base.columns
    
    # IBTBASE columns: TRANSREX, BRANCH, ACCTNO, OUTSTAND, TRANSREF, PRODTYPE, DAYS
    base = base.select([
        pl.col(col_names[2]).alias("ACCTNO"),
        pl.col(col_names[4]).alias("TRANSREF"),
        pl.col(col_names[3]).alias("PREOUTSTD"),
        pl.col(col_names[5]).alias("PRODTYPE"),
        pl.col(col_names[6]).alias("DAYS"),
    ])
    
    base = base.with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("TRANSREF").cast(pl.Utf8),
        pl.col("PREOUTSTD").cast(pl.Float64),
        pl.col("PRODTYPE").cast(pl.Int64),
        pl.col("DAYS").cast(pl.Int64),
    ])
    
    print(f"BASE after mapping: {len(base)} records")
    print("BASE sample:")
    print(base.head(5))
    
    return base

base = read_base_sas(base_file)

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nAfter deduplication:")
print(f"  BASE: {len(base)}")
print(f"  BTDTL: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 6: Sort to match SAS order
# --------------------------------------------------------------------
base = base.sort(["ACCTNO", "TRANSREF"])
btdtl = btdtl.sort(["ACCTNO", "TRANSREF"])

# --------------------------------------------------------------------
# Step 7: Merge and calculate
# --------------------------------------------------------------------
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

print(f"\nAfter merge: {len(combt)} records")

# Fill nulls
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
    pl.col("DAYS").fill_null(0),
])

# Calculate OVERDUE and RECOVAMT
combt = combt.with_columns([
    (pl.col("DAYS") + 1).alias("OVERDUE"),
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    pl.when(pl.col("PRODTYPE") == 0)
    .then(pl.lit("R"))
    .otherwise(pl.lit(None))
    .alias("RETAILID"),
])

# Sort final output
combt = combt.sort(["ACCTNO", "TRANSREF"])

print(f"\nCalculations complete")
print(f"  Records with OVERDUE > 0: {(combt['OVERDUE'] > 0).sum()}")

# --------------------------------------------------------------------
# Step 8: Write output
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    records = []
    
    for row in df.iter_rows(named=True):
        try:
            branch = row.get('BRANCH')
            acctno = row.get('ACCTNO', 0) or 0
            transref = str(row.get('TRANSREF', '') or '')[:10]
            prodtype = row.get('PRODTYPE', 0) or 0
            preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
            outstanding = row.get('OUTSTAND', 0.0) or 0.0
            overdue = row.get('OVERDUE', 0) or 0
            recovamt = row.get('RECOVAMT', 0.0) or 0.0
            facility = str(row.get('FACILITY', '99999') or '99999')[:5]
            
            record = [' '] * 103
            
            if branch is not None and branch != 0:
                record[0:5] = f"{int(branch):5d}"
            else:
                record[0:5] = '     '
            
            record[6:16] = f"{int(acctno):10d}"
            record[17:27] = f"{transref:<10}"
            record[28:31] = f"{int(prodtype):03d}"
            
            if preoutstd != 0:
                record[32:49] = f"{float(preoutstd):17.2f}"
            else:
                record[32:49] = ' ' * 17
            
            if outstanding != 0:
                record[50:67] = f"{float(outstanding):17.2f}"
            else:
                record[50:67] = ' ' * 17
            
            if overdue != 0:
                record[68:78] = f"{int(overdue):10d}"
            else:
                record[68:78] = ' ' * 10
            
            if recovamt != 0:
                record[79:96] = f"{float(recovamt):17.2f}"
            else:
                record[79:96] = ' ' * 17
            
            record[97:102] = f"{facility:<5}"
            
            records.append(''.join(record))
        except:
            continue
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nOutput written: {filepath} ({len(records)} records)")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save Parquet
try:
    os.makedirs(os.path.dirname(OUTPUT_PARQUET_FILE), exist_ok=True)
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet saved: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# --------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Run Date (TODAY): {params['TODAY']}")
print(f"Report Date (REPTDATE): {params['REPTDATE']}")
print(f"Previous Month (PREVMON): {params['PREVMON']}")
print(f"BASE File Used: {base_file}")
print(f"Total Records: {len(combt)}")
print("=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
