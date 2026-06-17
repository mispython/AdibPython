# EIBDBT12_BANKTRADE_PM12_FIXED.py
# Fixed version with correct date calculations

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
OUTPUT_TEXT_FILE = "DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12.parquet"

# --------------------------------------------------------------------
# Step 1: Reporting date logic - MATCHING SAS EXACTLY
# --------------------------------------------------------------------
def sas_date_to_ordinal(dt):
    """Convert datetime to SAS date (days since 1960-01-01)"""
    sas_epoch = date(1960, 1, 1)
    return (dt - sas_epoch).days

def ordinal_to_sas_date(ordinal):
    """Convert ordinal to SAS date"""
    sas_epoch = date(1960, 1, 1)
    return sas_epoch + timedelta(days=ordinal)

# Calculate dates
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# Calculate SDATE (first day of next month)
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

# Convert to SAS dates (days since 1960-01-01)
sdate_sas = sas_date_to_ordinal(sdate)
reptdate_sas = sas_date_to_ordinal(reptdate)
prevdate_sas = sas_date_to_ordinal(prevdate)

params = {
    "REPTDATE": reptdate.strftime("%Y-%m-%d"),
    "REPTDATE_SAS": reptdate_sas,
    "PREVDATE": prevdate.strftime("%Y-%m-%d"),
    "PREVDATE_SAS": prevdate_sas,
    "PREVMON": f"{prevdate.month:02d}",
    "SDATE": sdate.strftime("%Y-%m-%d"),
    "SDATE_SAS": sdate_sas,
    "SDATE_Z5": f"{sdate_sas:05d}",  # Z5. format
}

print("=" * 80)
print("Date Parameters (SAS compatible)")
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
        for line in f:
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
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref,
                    'OUTSTAND': outstanding,
                    'MATDT': matdt,
                    'LIABCODE': liabcode,
                })
            except:
                continue
    
    print(f"Parsed {len(data)} records from BTPM12")
    return pl.DataFrame(data)

btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

# Parse MATDATE: DDMMYY
btdtl = btdtl.with_columns([
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32).alias("year2")
])

btdtl = btdtl.filter(
    (pl.col("day") >= 1) & (pl.col("day") <= 31) &
    (pl.col("month") >= 1) & (pl.col("month") <= 12)
)

btdtl = btdtl.with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.date("year", "month", "day").alias("MATDATE")
)

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
    filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
    
    if not os.path.exists(filepath):
        # Try alternative naming
        alt_paths = [
            f"input/prod/btbase_{prevmon}.sas7bdat",
            f"input/prod/BTBASE{prevmon}.sas7bdat",
        ]
        for alt in alt_paths:
            if os.path.exists(alt):
                filepath = alt
                break
        else:
            raise FileNotFoundError(f"BASE dataset not found: {filepath}")
    
    print(f"Reading BASE: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(filepath)
    base = pl.from_pandas(df)
    
    # Rename OUTSTAND to PREOUTSTD
    if "OUTSTAND" in base.columns:
        base = base.rename({"OUTSTAND": "PREOUTSTD"})
    
    # Keep only needed columns
    required = ["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE"]
    available = [col for col in required if col in base.columns]
    base = base.select(available)
    
    print(f"BASE records: {len(base)}")
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

# Merge
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Fill nulls
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
])

# Convert MATDATE to SAS date (days since 1960-01-01)
# Then calculate OVERDUE using SAS date system
sas_epoch = date(1960, 1, 1)

def to_sas_date(dt):
    """Convert date to SAS date (days since 1960-01-01)"""
    return (dt - sas_epoch).days

# Calculate OVERDUE using SAS date system
sdate_sas = params["SDATE_SAS"]

combt = combt.with_columns([
    # Convert MATDATE to SAS date
    pl.when(pl.col("MATDATE").is_not_null())
    .then((pl.col("MATDATE").cast(pl.Date) - date(1960, 1, 1)).dt.total_days().cast(pl.Int64))
    .otherwise(pl.lit(None))
    .alias("MATDATE_SAS"),
])

# Calculate OVERDUE = (SDATE + 1) - MATDATE_SAS
combt = combt.with_columns([
    pl.when(pl.col("MATDATE_SAS").is_not_null())
    .then((sdate_sas + 1) - pl.col("MATDATE_SAS"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD - OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
])

# Fill nulls
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

print(f"Merged records: {len(combt)}")

# --------------------------------------------------------------------
# Step 5: Write output
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write matching SAS PUT statement exactly"""
    records = []
    
    for row in df.iter_rows(named=True):
        try:
            branch = row.get('BRANCH', 0) or 0
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
            
            record[0:5] = f"{int(branch):5d}"
            record[6:16] = f"{int(acctno):10d}"
            record[17:27] = f"{transref:<10}"
            record[28:31] = f"{int(prodtype):03d}"
            record[32:49] = f"{float(preoutstd):17.2f}"
            record[50:67] = f"{float(outstanding):17.2f}"
            record[68:78] = f"{int(overdue):10d}"
            record[79:96] = f"{float(recovamt):17.2f}"
            record[97:102] = f"{facility:<5}"
            
            records.append(''.join(record))
        except:
            continue
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"Output written: {filepath} ({len(records)} records)")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save Parquet
try:
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet output: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# --------------------------------------------------------------------
# Validation with production data
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("VALIDATION: Comparing with production values")
print("=" * 80)

# Test specific records from production
test_cases = [
    {"ACCTNO": 2501466705, "TRANSREF": "Y081465", "EXPECTED_OVERDUE": 1048, "EXPECTED_RECOVAMT": -597.00},
    {"ACCTNO": 2505707731, "TRANSREF": "Y080273", "EXPECTED_OVERDUE": 1180, "EXPECTED_RECOVAMT": -562.80},
    {"ACCTNO": 2501873900, "TRANSREF": "Y011618", "EXPECTED_OVERDUE": 7074, "EXPECTED_RECOVAMT": 0.00},
]

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
        print(f"\nACCTNO: {test['ACCTNO']} - NOT FOUND")

print("\n" + "=" * 80)
