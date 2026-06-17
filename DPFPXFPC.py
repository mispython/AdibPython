# EIBDBT12_BANKTRADE_PM12_FIXED.py
# Complete fix for all issues

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import re

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "input/prod/btbase{PREVMON}.sas7bdat"
OUTPUT_TEXT_FILE = "DAYBTRD_PM12_FIXED.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12_FIXED.parquet"

# --------------------------------------------------------------------
# Step 1: Reporting date logic - EXACTLY MATCHING SAS
# --------------------------------------------------------------------
def sas_date_to_python(sas_date):
    """Convert SAS date (days since 1960-01-01) to Python date"""
    return date(1960, 1, 1) + timedelta(days=sas_date)

def python_date_to_sas(dt):
    """Convert Python date to SAS date (days since 1960-01-01)"""
    return (dt - date(1960, 1, 1)).days

# Calculate dates
today = date.today()
reptdate = today - timedelta(days=1)  # 2026-06-16

# PREVDATE = First day of current month - 1
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)  # 2026-05-31

# SDATE = First day of next month
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)  # 2026-07-01

# Convert to SAS dates
reptdate_sas = python_date_to_sas(reptdate)
prevdate_sas = python_date_to_sas(prevdate)
sdate_sas = python_date_to_sas(sdate)

params = {
    "REPTDATE": reptdate,
    "REPTDATE_SAS": reptdate_sas,
    "PREVDATE": prevdate,
    "PREVDATE_SAS": prevdate_sas,
    "PREVMON": f"{prevdate.month:02d}",
    "SDATE": sdate,
    "SDATE_SAS": sdate_sas,
    "SDATE_Z5": f"{sdate_sas:05d}",
}

print("=" * 80)
print("DATE PARAMETERS")
print("=" * 80)
print(f"Today: {today}")
print(f"REPTDATE: {params['REPTDATE']} (SAS: {params['REPTDATE_SAS']})")
print(f"PREVDATE: {params['PREVDATE']} (SAS: {params['PREVDATE_SAS']})")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE: {params['SDATE']} (SAS: {params['SDATE_SAS']}, Z5: {params['SDATE_Z5']})")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """Read BTPM12 with exact SAS positions"""
    data = []
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n').rstrip('\r')
            
            if not line.strip() or line.strip().startswith('1BKT'):
                continue
            
            try:
                # SAS positions (1-based) -> Python (0-based)
                # @002 -> index 1, @006 -> index 5, @016 -> index 15
                # @023 -> index 22, @038 -> index 37, @043 -> index 42
                
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
                
                # Parse MATDT: DDMMYY
                day = int(matdt[0:2])
                month = int(matdt[2:4])
                year = 2000 + int(matdt[4:6])
                
                # Create date
                matdate = date(year, month, day)
                matdate_sas = python_date_to_sas(matdate)
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref,
                    'OUTSTAND': outstanding,
                    'MATDT': matdt,
                    'MATDATE': matdate,
                    'MATDATE_SAS': matdate_sas,
                    'LIABCODE': liabcode,
                })
            except Exception as e:
                if line_num <= 5:
                    print(f"Warning: Line {line_num} error: {e}")
                continue
    
    print(f"Parsed {len(data)} records from BTPM12")
    return pl.DataFrame(data)

btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

# Apply filter
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")

# --------------------------------------------------------------------
# Step 3: Read BASE dataset
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    """Read BTBASE dataset"""
    # Try multiple possible paths
    possible_paths = [
        INPUT_BTBASE_FILE.format(PREVMON=prevmon),
        f"input/prod/btbase_{prevmon}.sas7bdat",
        f"input/prod/BTBASE{prevmon}.sas7bdat",
        f"input/prod/BTBASE_{prevmon}.sas7bdat",
        f"input/prod/btbase{prevmon}.sas7bdat",
    ]
    
    filepath = None
    for path in possible_paths:
        if os.path.exists(path):
            filepath = path
            break
    
    if filepath is None:
        print(f"ERROR: BASE dataset not found. Tried:")
        for path in possible_paths:
            print(f"  - {path}")
        raise FileNotFoundError("BASE dataset not found")
    
    print(f"Reading BASE: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(filepath)
    base = pl.from_pandas(df)
    
    print(f"BASE columns: {base.columns}")
    print(f"BASE records: {len(base)}")
    
    # Rename OUTSTAND to PREOUTSTD if needed
    if "OUTSTAND" in base.columns:
        base = base.rename({"OUTSTAND": "PREOUTSTD"})
    
    # Keep only needed columns
    required = ["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE"]
    available = [col for col in required if col in base.columns]
    if len(available) < 4:
        print(f"WARNING: Missing columns. Available: {available}")
    base = base.select(available)
    
    print(f"BASE after selection: {len(base)} records")
    return base

base = read_base_sas(params['PREVMON'])

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"BASE after dedup: {len(base)}")
print(f"BTDTL after dedup: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge and calculate - USING SAS DATE SYSTEM
# --------------------------------------------------------------------
# Ensure types match
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

# Merge - KEEP ALL BASE RECORDS (IN=A)
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

print(f"After merge: {len(combt)} records")

# Fill nulls for calculations
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
])

# Calculate OVERDUE using SAS date system
# OVERDUE = (SDATE + 1) - MATDATE_SAS
sdate_sas = params["SDATE_SAS"]

combt = combt.with_columns([
    # Calculate OVERDUE
    pl.when(pl.col("MATDATE_SAS").is_not_null())
    .then((sdate_sas + 1) - pl.col("MATDATE_SAS"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD - OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
])

# Fill nulls for output
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

# Check for records with missing BTDTL (these should have BRANCH = '.' in output)
combt = combt.with_columns([
    pl.when(pl.col("BRANCH").is_null())
    .then(pl.lit(None))
    .otherwise(pl.col("BRANCH"))
    .alias("BRANCH"),
])

print(f"Final records: {len(combt)}")

# --------------------------------------------------------------------
# Step 5: Write output
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write matching SAS PUT statement exactly"""
    records = []
    skipped = 0
    
    for row in df.iter_rows(named=True):
        try:
            # Get values
            branch = row.get('BRANCH')
            acctno = row.get('ACCTNO', 0) or 0
            transref = str(row.get('TRANSREF', '') or '')[:10]
            prodtype = row.get('PRODTYPE', 0) or 0
            preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
            outstanding = row.get('OUTSTAND', 0.0) or 0.0
            overdue = row.get('OVERDUE', 0) or 0
            recovamt = row.get('RECOVAMT', 0.0) or 0.0
            facility = '99999'
            
            # Build with exact SAS positions
            record = [' '] * 103
            
            # @001 BRANCH 5. - if branch is None, output spaces ('.' in SAS)
            if branch is not None and branch != 0:
                record[0:5] = f"{int(branch):5d}"
            else:
                record[0:5] = '     '  # Blank for missing
            
            # @007 ACCTNO 10.
            record[6:16] = f"{int(acctno):10d}"
            
            # @018 TRANSREF 10.
            record[17:27] = f"{transref:<10}"
            
            # @029 PRODTYPE Z3.
            record[28:31] = f"{int(prodtype):03d}"
            
            # @033 PREOUTSTD 17.2
            if preoutstd != 0:
                record[32:49] = f"{float(preoutstd):17.2f}"
            else:
                record[32:49] = ' ' * 17  # Blank for zero
            
            # @051 OUTSTAND 17.2
            if outstanding != 0:
                record[50:67] = f"{float(outstanding):17.2f}"
            else:
                record[50:67] = ' ' * 17  # Blank for zero
            
            # @069 OVERDUE 10.
            if overdue != 0:
                record[68:78] = f"{int(overdue):10d}"
            else:
                record[68:78] = ' ' * 10  # Blank for zero
            
            # @080 RECOVAMT 17.2
            if recovamt != 0:
                record[79:96] = f"{float(recovamt):17.2f}"
            else:
                record[79:96] = ' ' * 17  # Blank for zero
            
            # @098 FACILITY $5.
            record[97:102] = f"{facility:<5}"
            
            records.append(''.join(record))
        except Exception as e:
            skipped += 1
            if skipped <= 5:
                print(f"Warning: Error formatting row: {e}")
            continue
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"Output written: {filepath} ({len(records)} records)")
    if skipped > 0:
        print(f"  Skipped {skipped} rows")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save Parquet
try:
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet output: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# --------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("VALIDATION")
print("=" * 80)

# Test specific records
test_cases = [
    {"ACCTNO": 2500667206, "TRANSREF": "Y090778", "EXPECTED_OVERDUE": 67, "EXPECTED_RECOVAMT": -413.70},
    {"ACCTNO": 2505707731, "TRANSREF": "Y080273", "EXPECTED_OVERDUE": 1180, "EXPECTED_RECOVAMT": -562.80},
    {"ACCTNO": 2501873900, "TRANSREF": "Y011618", "EXPECTED_OVERDUE": 7074, "EXPECTED_RECOVAMT": 0.00},
    {"ACCTNO": 2501466705, "TRANSREF": "Y081465", "EXPECTED_OVERDUE": 1048, "EXPECTED_RECOVAMT": -597.00},
]

print("\nComparing with production values:")
for test in test_cases:
    match = combt.filter(
        (pl.col("ACCTNO") == test["ACCTNO"]) & 
        (pl.col("TRANSREF") == test["TRANSREF"])
    )
    
    if len(match) > 0:
        row = match.row(0, named=True)
        print(f"\nACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']}")
        print(f"  OVERDUE: Expected={test['EXPECTED_OVERDUE']}, Got={row['OVERDUE']}")
        print(f"  RECOVAMT: Expected={test['EXPECTED_RECOVAMT']:.2f}, Got={row['RECOVAMT']:.2f}")
        
        if row['OVERDUE'] == test['EXPECTED_OVERDUE']:
            print("  ✓ OVERDUE matches!")
        else:
            print(f"  ✗ OVERDUE differs by {row['OVERDUE'] - test['EXPECTED_OVERDUE']}")
        
        if abs(row['RECOVAMT'] - test['EXPECTED_RECOVAMT']) < 0.01:
            print("  ✓ RECOVAMT matches!")
        else:
            print(f"  ✗ RECOVAMT differs by {row['RECOVAMT'] - test['EXPECTED_RECOVAMT']:.2f}")
    else:
        print(f"\nACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']} - NOT FOUND")

print("\n" + "=" * 80)
