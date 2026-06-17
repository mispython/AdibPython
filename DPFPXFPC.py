# EIBDBT12_BANKTRADE_PM12.py
# FINAL FIXED VERSION - Using DAYS from BTBASE05

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
INPUT_BTPM12_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/btbase{PREVMON}.sas7bdat"
OUTPUT_TEXT_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.parquet"

# SAS epoch (days since 1960-01-01) - USE CORRECT REFERENCE
SAS_EPOCH = date(1960, 1, 1)

def to_sas_date(dt):
    """Convert Python date to SAS date (days since 1960-01-01)"""
    return (dt - SAS_EPOCH).days

# --------------------------------------------------------------------
# Step 1: Reporting date logic
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

sdate_sas = to_sas_date(sdate)

params = {
    "REPTDATE": reptdate,
    "PREVDATE": prevdate,
    "PREVMON": f"{prevdate.month:02d}",
    "SDATE": sdate,
    "SDATE_SAS": sdate_sas,
}

print("=" * 80)
print("EIBDBT12 - Bank Trade Report")
print("=" * 80)
print(f"REPTDATE: {params['REPTDATE']}")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE_SAS: {params['SDATE_SAS']}")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input
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
                
                # MATDATE = MDY(SUBSTR(MATDT,3,2), SUBSTR(MATDT,5,2), SUBSTR(MATDT,1,2))
                # MATDT is DDMMYY, SAS interprets as MDY(MM, DD, YY)
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

btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (BTBASE05) - INCLUDE DAYS COLUMN
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    base_dir = os.path.dirname(INPUT_BTBASE_FILE)
    patterns = [
        INPUT_BTBASE_FILE.format(PREVMON=prevmon),
        os.path.join(base_dir, f"btbase{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"btbase_{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"BTBASE{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"BTBASE_{prevmon}.sas7bdat"),
    ]
    
    filepath = None
    for pattern in patterns:
        if os.path.exists(pattern):
            filepath = pattern
            break
    
    if filepath is None:
        all_files = glob.glob(os.path.join(base_dir, "*.sas7bdat"))
        if all_files:
            print(f"\nAvailable SAS files in {base_dir}:")
            for f in all_files:
                print(f"  - {os.path.basename(f)}")
        raise FileNotFoundError(f"BASE file not found for month {prevmon}")
    
    print(f"\nReading BASE: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(filepath)
    base = pl.from_pandas(df)
    
    print(f"BASE columns: {base.columns}")
    print(f"BASE records: {len(base)}")
    
    col_names = base.columns
    
    # Select ALL columns we need including DAYS
    base = base.select([
        pl.col(col_names[2]).alias("ACCTNO"),      # ACCTNO
        pl.col(col_names[4]).alias("TRANSREF"),    # TRANSREF
        pl.col(col_names[3]).alias("PREOUTSTD"),   # OUTSTAND -> PREOUTSTD
        pl.col(col_names[6]).alias("PRODTYPE"),    # PRODTYPE
        pl.col(col_names[7]).alias("DAYS"),        # DAYS - THIS IS THE KEY!
    ])
    
    base = base.with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("TRANSREF").cast(pl.Utf8),
        pl.col("PREOUTSTD").cast(pl.Float64),
        pl.col("PRODTYPE").cast(pl.Int64),
        pl.col("DAYS").cast(pl.Int64),
    ])
    
    print(f"BASE after mapping: {len(base)} records")
    print("BASE sample (including DAYS):")
    print(base.head(5))
    
    return base

base = read_base_sas(params['PREVMON'])

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nAfter deduplication:")
print(f"  BASE: {len(base)}")
print(f"  BTDTL: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge and calculate
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

# CRITICAL FIX: Use DAYS + 1 for OVERDUE (matches production)
# The DAYS column in BTBASE05 is the previous month's overdue days
# Production output uses DAYS + 1 as the OVERDUE
combt = combt.with_columns([
    # OVERDUE = DAYS + 1 (matching production)
    (pl.col("DAYS") + 1).alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD - OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    
    # RETAILID = 'R' if PRODTYPE = 0
    pl.when(pl.col("PRODTYPE") == 0)
    .then(pl.lit("R"))
    .otherwise(pl.lit(None))
    .alias("RETAILID"),
])

print(f"\nCalculations complete")
print(f"  Records with OVERDUE > 0: {(combt['OVERDUE'] > 0).sum()}")

# --------------------------------------------------------------------
# Step 5: Write output
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
# Validation against Production
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("VALIDATION AGAINST PRODUCTION")
print("=" * 80)

test_cases = [
    {"ACCTNO": 2500667206, "TRANSREF": "Y090778", "EXP_OVERDUE": 67, "EXP_RECOVAMT": -413.70},
    {"ACCTNO": 2505707731, "TRANSREF": "Y080273", "EXP_OVERDUE": 1180, "EXP_RECOVAMT": -562.80},
    {"ACCTNO": 2501873900, "TRANSREF": "Y011618", "EXP_OVERDUE": 7074, "EXP_RECOVAMT": 0.00},
]

print("\nComparing specific records:")
matches = 0
total = 0

for test in test_cases:
    match = combt.filter(
        (pl.col("ACCTNO") == test["ACCTNO"]) & 
        (pl.col("TRANSREF") == test["TRANSREF"])
    )
    
    if len(match) > 0:
        row = match.row(0, named=True)
        total += 1
        
        overdue_match = row['OVERDUE'] == test["EXP_OVERDUE"]
        recovamt_match = abs(row['RECOVAMT'] - test["EXP_RECOVAMT"]) < 0.01
        
        if overdue_match and recovamt_match:
            matches += 1
            status = "✓ PASS"
        else:
            status = "✗ FAIL"
        
        print(f"\n{status} ACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']}")
        print(f"  OVERDUE:   Expected={test['EXP_OVERDUE']:>6d}, Got={row['OVERDUE']:>6d} {'✓' if overdue_match else '✗'}")
        print(f"  RECOVAMT:  Expected={test['EXP_RECOVAMT']:>12.2f}, Got={row['RECOVAMT']:>12.2f} {'✓' if recovamt_match else '✗'}")
        print(f"  DAYS from BASE: {row['DAYS']}")
    else:
        print(f"\n✗ NOT FOUND: ACCTNO={test['ACCTNO']}, TRANSREF={test['TRANSREF']}")

print(f"\nValidation Summary:")
print(f"  Matches: {matches}/{total}")

if matches == total:
    print("  ✓ ALL TESTS PASSED!")
else:
    print("  ✗ SOME TESTS FAILED - Check the differences above")

print("\n" + "=" * 80)
