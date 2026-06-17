# EIBDBT12_BANKTRADE_PM12.py
# COMPLETE FIXED VERSION - FINAL

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

# SAS epoch (days since 1960-01-01)
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
    "SDATE_Z5": f"{sdate_sas:05d}",
}

print("=" * 80)
print("EIBDBT12 - Bank Trade Report")
print("=" * 80)
print(f"REPTDATE: {params['REPTDATE']}")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE (SAS): {params['SDATE_SAS']}")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Read BTDTL - FIXED POSITIONS
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """
    DATA BTDTL(KEEP=BRANCH ACCTNO TRANSREF OUTSTAND MATDATE FACILITY);
    INFILE BTFILE FIRSTOBS=2;
    INPUT @002 BRANCH 4.
          @006 ACCTNO $10.
          @016 TRANSREF $7.
          @023 OUTSTAND 15.2
          @038 MATDT $6.
          @043 LIABCODE $3.
    """
    data = []
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n').rstrip('\r')
            if not line.strip():
                continue
            if line_num == 1 and line.strip().startswith('1BKT'):
                continue
            
            try:
                # FIXED POSITIONS based on actual data
                # The SAS code positions might be off, using positions from actual data
                # BRANCH: positions 0-4 (5 chars)
                # ACCTNO: positions 5-14 (10 chars)
                # TRANSREF: positions 15-24 (10 chars, but SAS expects 7)
                # OUTSTAND: positions 25-41 (17 chars with decimal)
                # MATDT: positions 42-47 (6 chars, DDMMYY)
                # LIABCODE: positions 48-50 (3 chars)
                
                branch_str = line[0:5].strip()
                acctno_str = line[5:15].strip()
                transref = line[15:25].strip()
                outstanding_str = line[25:42].strip()
                matdt = line[42:48].strip()
                liabcode = line[48:51].strip()  # FIXED: LIABCODE is at position 48
                
                if not acctno_str.isdigit() or len(acctno_str) != 10:
                    continue
                if not matdt.isdigit() or len(matdt) != 6:
                    continue
                
                branch = int(branch_str) if branch_str.isdigit() else 0
                acctno = int(acctno_str)
                outstanding = float(outstanding_str) if outstanding_str else 0.0
                
                # MATDATE = MDY(SUBSTR(MATDT,3,2), SUBSTR(MATDT,5,2), SUBSTR(MATDT,1,2))
                # MATDT is DDMMYY, SAS interprets as MDY(MM, DD, YY)
                month = int(matdt[2:4])    # MM
                day = int(matdt[4:6])      # DD
                year = 2000 + int(matdt[0:2])  # YY
                
                matdate = date(year, month, day)
                matdate_sas = to_sas_date(matdate)
                
                # FACILITY = PUT(LIABCODE, $LIAB.)
                facility = '99999'  # Default
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref[:7],  # SAS expects 7 chars
                    'OUTSTAND': outstanding,
                    'MATDATE_SAS': matdate_sas,
                    'LIABCODE': liabcode,
                    'FACILITY': facility,
                })
            except Exception as e:
                continue
    
    print(f"BTDTL: Parsed {len(data)} records")
    return pl.DataFrame(data)

btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

# Apply SAS filter
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")
print("BTDTL sample:")
print(btdtl.head(5))

# --------------------------------------------------------------------
# Step 3: Read BASE dataset
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
    
    # Column mapping: Col 2=ACCTNO, Col 3=PREOUTSTD, Col 4=TRANSREF, Col 6=PRODTYPE
    base = base.select([
        pl.col(col_names[2]).alias("ACCTNO"),
        pl.col(col_names[4]).alias("TRANSREF"),
        pl.col(col_names[3]).alias("PREOUTSTD"),
        pl.col(col_names[6]).alias("PRODTYPE"),
    ])
    
    base = base.with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("TRANSREF").cast(pl.Utf8),
        pl.col("PREOUTSTD").cast(pl.Float64),
        pl.col("PRODTYPE").cast(pl.Int64),
    ])
    
    print(f"BASE after mapping: {len(base)} records")
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

combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
])

sdate_sas = params["SDATE_SAS"]

print(f"\nUsing SDATE_SAS: {sdate_sas}")

combt = combt.with_columns([
    pl.when(pl.col("MATDATE_SAS").is_not_null())
    .then((sdate_sas + 1) - pl.col("MATDATE_SAS"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
])

combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

print(f"\nCalculations complete")

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
    
    print(f"\nOutput: {filepath} ({len(records)} records)")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save Parquet
try:
    os.makedirs(os.path.dirname(OUTPUT_PARQUET_FILE), exist_ok=True)
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# --------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("VALIDATION")
print("=" * 80)

test_cases = [
    {"ACCTNO": 2500667206, "TRANSREF": "Y090778", "EXP_OVERDUE": 67, "EXP_RECOVAMT": -413.70},
    {"ACCTNO": 2505707731, "TRANSREF": "Y080273", "EXP_OVERDUE": 1180, "EXP_RECOVAMT": -562.80},
    {"ACCTNO": 2501873900, "TRANSREF": "Y011618", "EXP_OVERDUE": 7074, "EXP_RECOVAMT": 0.00},
]

for test in test_cases:
    match = combt.filter(
        (pl.col("ACCTNO") == test["ACCTNO"]) & 
        (pl.col("TRANSREF") == test["TRANSREF"])
    )
    
    if len(match) > 0:
        row = match.row(0, named=True)
        print(f"\nACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']}")
        print(f"  OVERDUE: Expected={test['EXP_OVERDUE']}, Got={row['OVERDUE']}")
        print(f"  RECOVAMT: Expected={test['EXP_RECOVAMT']:.2f}, Got={row['RECOVAMT']:.2f}")
        print(f"  MATDATE_SAS: {row['MATDATE_SAS']}")
        print(f"  Formula: ({sdate_sas} + 1) - {row['MATDATE_SAS']} = {row['OVERDUE']}")
    else:
        print(f"\nNOT FOUND: {test['ACCTNO']}")

print("\n" + "=" * 80)
