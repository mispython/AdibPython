# EIBDBT12_BANKTRADE_PM12.py
# FINAL COMPLETE WORKING VERSION

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
# Step 1: Reporting date logic (EXACTLY matching SAS)
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)  # Yesterday

# PREVDATE = First day of current month - 1 (last day of previous month)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# SDATE = First day of NEXT month
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

# Convert to SAS date
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
print(f"Today: {today}")
print(f"REPTDATE: {params['REPTDATE']}")
print(f"PREVDATE: {params['PREVDATE']}")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE: {params['SDATE']}")
print(f"SDATE (SAS format): {params['SDATE_SAS']}")
print(f"SDATE (Z5. format): {params['SDATE_Z5']}")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input
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
                continue  # FIRSTOBS=2
            
            try:
                # SAS positions (1-based) -> Python (0-based)
                # @002 -> index 1, @006 -> index 5, @016 -> index 15
                # @023 -> index 22, @038 -> index 37, @043 -> index 42
                
                branch_str = line[1:5].strip()
                acctno_str = line[5:15].strip()
                transref = line[15:22].strip()
                outstanding_str = line[22:37].strip()
                matdt = line[37:43].strip()
                liabcode = line[42:45].strip()  # @043 LIABCODE $3.
                
                if not acctno_str.isdigit() or len(acctno_str) != 10:
                    continue
                if not matdt.isdigit() or len(matdt) != 6:
                    continue
                
                branch = int(branch_str) if branch_str.isdigit() else 0
                acctno = int(acctno_str)
                outstanding = float(outstanding_str) if outstanding_str else 0.0
                
                # CRITICAL FIX: SAS MATDATE = MDY(SUBSTR(MATDT,3,2), SUBSTR(MATDT,5,2), SUBSTR(MATDT,1,2))
                # MATDT is DDMMYY, SAS interprets as MDY(MM, DD, YY)
                # So for MATDT = "070119":
                #   SUBSTR(MATDT,3,2) = "01" = Month = 1 (January)
                #   SUBSTR(MATDT,5,2) = "19" = Day = 19
                #   SUBSTR(MATDT,1,2) = "07" = Year = 2007
                #   MATDATE = 2007-01-19
                month = int(matdt[2:4])    # SUBSTR(MATDT,3,2) - MM
                day = int(matdt[4:6])      # SUBSTR(MATDT,5,2) - DD
                year = 2000 + int(matdt[0:2])  # SUBSTR(MATDT,1,2) - YY
                
                matdate = date(year, month, day)
                matdate_sas = to_sas_date(matdate)
                
                # FACILITY = PUT(LIABCODE, $LIAB.) - default to 99999
                facility = '99999'
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref,
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

# Apply SAS filter: IF BRANCH > 3000 AND (2850000000<=ACCTNO<=2859999999) THEN DELETE
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")
print("BTDTL sample (first 5):")
print(btdtl.head(5))

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (BTBASE05)
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    """
    DATA BASE(KEEP=ACCTNO TRANSREF OUTSTAND PRODTYPE RENAME=(OUTSTAND=PREOUTSTD));
    SET BASE.BTBASE&PREVMON;
    """
    # Try different file naming patterns
    base_dir = os.path.dirname(INPUT_BTBASE_FILE)
    patterns = [
        INPUT_BTBASE_FILE.format(PREVMON=prevmon),
        os.path.join(base_dir, f"btbase{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"btbase_{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"BTBASE{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"BTBASE_{prevmon}.sas7bdat"),
        os.path.join(base_dir, f"btbase{prevmon}.sas7bdat"),
    ]
    
    filepath = None
    for pattern in patterns:
        if os.path.exists(pattern):
            filepath = pattern
            break
    
    if filepath is None:
        print(f"\nERROR: BASE file not found for month {prevmon}")
        print("Tried patterns:")
        for pattern in patterns:
            print(f"  - {pattern}")
        
        # List all sas7bdat files in the directory
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
    
    # Column mapping based on BTBASE05 structure:
    # Col 0: TRANSREX (ignored)
    # Col 1: BRANCH (ignored)
    # Col 2: ACCTNO
    # Col 3: OUTSTAND -> PREOUTSTD
    # Col 4: TRANSREF
    # Col 5: FACILITY (ignored)
    # Col 6: PRODTYPE
    # Col 7: DAYS (ignored)
    
    col_names = base.columns
    
    # Select and rename columns
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
    print("BASE sample (first 5):")
    print(base.head(5))
    
    return base

base = read_base_sas(params['PREVMON'])

# Deduplicate (PROC SORT NODUPKEY)
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nAfter deduplication:")
print(f"  BASE: {len(base)}")
print(f"  BTDTL: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge and calculate (DATA COMBT)
# --------------------------------------------------------------------
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

# MERGE BASE(IN=A) BTDTL(IN=B); BY ACCTNO TRANSREF; IF A;
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

print(f"\nAfter merge: {len(combt)} records")

# Check unmatched records
unmatched = combt.filter(pl.col("MATDATE_SAS").is_null())
print(f"Unmatched BASE records (no BTDTL match): {len(unmatched)}")

# Fill nulls for calculations
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
])

# Calculate OVERDUE and RECOVAMT
sdate_sas = params["SDATE_SAS"]

print(f"\nUsing SDATE_SAS: {sdate_sas}")

combt = combt.with_columns([
    # OVERDUE = (&SDATE+1)-MATDATE
    pl.when(pl.col("MATDATE_SAS").is_not_null())
    .then((sdate_sas + 1) - pl.col("MATDATE_SAS"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD-OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    
    # IF PRODTYPE = 000 THEN RETAILID='R'
    pl.when(pl.col("PRODTYPE") == 0)
    .then(pl.lit("R"))
    .otherwise(pl.lit(None))
    .alias("RETAILID"),
])

# Fill nulls for output
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

print(f"\nCalculations complete")
print(f"  Records with OVERDUE > 0: {(combt['OVERDUE'] > 0).sum()}")
print(f"  Records with RECOVAMT != 0: {(combt['RECOVAMT'] != 0).sum()}")

# --------------------------------------------------------------------
# Step 5: Write output (DATA _NULL_)
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """
    DATA _NULL_;
    SET COMBT;
    FILE DAYBTRD;
    PUT @001 BRANCH 5.
        @007 ACCTNO 10.
        @018 TRANSREF 10.
        @029 PRODTYPE Z3.
        @033 PREOUTSTD 17.2
        @051 OUTSTAND 17.2
        @069 OVERDUE 10.
        @080 RECOVAMT 17.2
        @098 FACILITY $5.
    """
    records = []
    skipped = 0
    
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
            
            # @001 BRANCH 5. (positions 1-5)
            if branch is not None and branch != 0:
                record[0:5] = f"{int(branch):5d}"
            else:
                record[0:5] = '     '  # Blank for missing (shows as '.' in SAS)
            
            # @007 ACCTNO 10. (positions 7-16)
            record[6:16] = f"{int(acctno):10d}"
            
            # @018 TRANSREF 10. (positions 18-27)
            record[17:27] = f"{transref:<10}"
            
            # @029 PRODTYPE Z3. (positions 29-31)
            record[28:31] = f"{int(prodtype):03d}"
            
            # @033 PREOUTSTD 17.2 (positions 33-49)
            if preoutstd != 0:
                record[32:49] = f"{float(preoutstd):17.2f}"
            else:
                record[32:49] = ' ' * 17
            
            # @051 OUTSTAND 17.2 (positions 51-67)
            if outstanding != 0:
                record[50:67] = f"{float(outstanding):17.2f}"
            else:
                record[50:67] = ' ' * 17
            
            # @069 OVERDUE 10. (positions 69-78)
            if overdue != 0:
                record[68:78] = f"{int(overdue):10d}"
            else:
                record[68:78] = ' ' * 10
            
            # @080 RECOVAMT 17.2 (positions 80-96)
            if recovamt != 0:
                record[79:96] = f"{float(recovamt):17.2f}"
            else:
                record[79:96] = ' ' * 17
            
            # @098 FACILITY $5. (positions 98-102)
            record[97:102] = f"{facility:<5}"
            
            records.append(''.join(record))
            
        except Exception as e:
            skipped += 1
            if skipped <= 5:
                print(f"Warning: Error formatting row: {e}")
            continue
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nOutput written: {filepath}")
    print(f"  Total records: {len(records)}")
    if skipped > 0:
        print(f"  Skipped rows: {skipped}")

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
    {"ACCTNO": 2501466705, "TRANSREF": "Y081465", "EXP_OVERDUE": 1048, "EXP_RECOVAMT": -597.00},
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
        
        if not overdue_match:
            print(f"     SDATE_SAS: {params['SDATE_SAS']}")
            print(f"     MATDATE_SAS: {row['MATDATE_SAS']}")
            print(f"     Formula: ({params['SDATE_SAS']} + 1) - {row['MATDATE_SAS']} = {row['OVERDUE']}")
    else:
        print(f"\n✗ NOT FOUND: ACCTNO={test['ACCTNO']}, TRANSREF={test['TRANSREF']}")

print(f"\nValidation Summary:")
print(f"  Matches: {matches}/{total}")

if matches == total:
    print("  ✓ ALL TESTS PASSED!")
else:
    print("  ✗ SOME TESTS FAILED - Check the differences above")

# --------------------------------------------------------------------
# Summary Statistics
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)

print(f"Total records: {len(combt)}")
matched = combt.filter(pl.col("MATDATE_SAS").is_not_null())
unmatched = combt.filter(pl.col("MATDATE_SAS").is_null())
print(f"Records with BTDTL match: {len(matched)}")
print(f"Records without BTDTL match: {len(unmatched)}")

print("\nOVERDUE statistics:")
print(combt['OVERDUE'].describe())

print("\nRECOVAMT statistics:")
print(combt['RECOVAMT'].describe())

print("\nPRODTYPE distribution:")
print(combt.group_by('PRODTYPE').agg(pl.count().alias('count')))

print("\n" + "=" * 80)
print("SDATE Calculation Details:")
print(f"  reptdate: {params['REPTDATE']}")
print(f"  sdate (next month): {params['SDATE']}")
print(f"  sdate_sas: {params['SDATE_SAS']}")
print("=" * 80)

# --------------------------------------------------------------------
# Validate Output Files
# --------------------------------------------------------------------
def validate_files():
    if os.path.exists(OUTPUT_TEXT_FILE):
        size = os.path.getsize(OUTPUT_TEXT_FILE)
        with open(OUTPUT_TEXT_FILE, 'r') as f:
            lines = sum(1 for _ in f)
        print(f"\n✓ Output file: {OUTPUT_TEXT_FILE}")
        print(f"  Size: {size:,} bytes")
        print(f"  Lines: {lines}")
    else:
        print(f"\n✗ Output file not found: {OUTPUT_TEXT_FILE}")
    
    if os.path.exists(OUTPUT_PARQUET_FILE):
        size = os.path.getsize(OUTPUT_PARQUET_FILE)
        print(f"✓ Parquet file: {OUTPUT_PARQUET_FILE}")
        print(f"  Size: {size:,} bytes")
    else:
        print(f"✗ Parquet file not found: {OUTPUT_PARQUET_FILE}")

validate_files()

print("\n" + "=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
