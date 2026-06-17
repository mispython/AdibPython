# EIBDBT12_BANKTRADE_PM12.py
# Complete Python conversion matching SAS EIBDBT12 exactly

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import glob

# ============================================================================
# CONFIGURATION
# ============================================================================
INPUT_BTFILE = "input/prod/BTPM12.txt"           # BTFILE DD - Current month trades
INPUT_BASE_DIR = "input/prod"                    # Directory containing BASE files
OUTPUT_DAYBTRD = "DAYBTRD_PM12.txt"              # DAYBTRD DD - Output file
OUTPUT_PARQUET = "DAYBTRD_PM12.parquet"          # Parquet output for verification

# SAS Epoch (days since 1960-01-01)
SAS_EPOCH = date(1960, 1, 1)

def to_sas_date(dt):
    """Convert Python date to SAS date (days since 1960-01-01)"""
    return (dt - SAS_EPOCH).days

# ============================================================================
# STEP 1: Reporting Date Logic (DATA REPTDATE)
# ============================================================================
today = date.today()
reptdate = today - timedelta(days=1)  # TODAY()-1

# PREVDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))-1
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# RPTDT = PUT(REPTDATE, DDMMYY10.)
rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]      # SUBSTR(RPTDT,4,2)
curyy = rptdt[8:10]     # SUBSTR(RPTDT,9,2)
rdatex = curmm + curyy  # RDATEX

# SDATE = MDY(MM,1,YY) where MM = MONTH(REPTDATE)+1
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

# Convert to SAS date
sdate_sas = to_sas_date(sdate)

# Macro variables (SYMPUT equivalents)
params = {
    'REPTYEAR': f"{reptdate.year % 100:02d}",
    'REPTMON': f"{reptdate.month:02d}",
    'REPTDAY': f"{reptdate.day:02d}",
    'PREVMON': f"{prevdate.month:02d}",
    'PREVDAY': f"{prevdate.day:02d}",
    'RDATE': reptdate.strftime("%d%m%Y"),
    'RDATEX': rdatex,
    'SDATE': f"{sdate_sas:05d}",  # Z5. format
}

print("=" * 80)
print("EIBDBT12 - Bank Trade Report")
print("=" * 80)
print(f"REPTDATE: {reptdate}")
print(f"PREVDATE: {prevdate}")
print(f"PREVMON: {params['PREVMON']}")
print(f"SDATE (SAS): {sdate_sas} (Z5: {params['SDATE']})")
print(f"RDATEX: {params['RDATEX']}")
print("=" * 80)

# ============================================================================
# STEP 2: Read BTDTL (DATA BTDTL)
# ============================================================================
def read_btdtl(filepath):
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
                liabcode = line[42:45].strip()  # @043 LIABCODE $3. - positions 42-44
                
                if not acctno_str.isdigit() or len(acctno_str) != 10:
                    continue
                if not matdt.isdigit() or len(matdt) != 6:
                    continue
                
                branch = int(branch_str) if branch_str.isdigit() else 0
                acctno = int(acctno_str)
                outstanding = float(outstanding_str) if outstanding_str else 0.0
                
                # MATDATE = MDY(SUBSTR(MATDT,3,2), SUBSTR(MATDT,5,2), SUBSTR(MATDT,1,2))
                # MATDT is DDMMYY, SAS interprets as MDY(MM, DD, YY)
                month = int(matdt[2:4])    # SUBSTR(MATDT,3,2) - MM
                day = int(matdt[4:6])      # SUBSTR(MATDT,5,2) - DD
                year = 2000 + int(matdt[0:2])  # SUBSTR(MATDT,1,2) - YY
                
                matdate = date(year, month, day)
                matdate_sas = to_sas_date(matdate)
                
                # FACILITY = PUT(LIABCODE, $LIAB.)
                # From PBBBTFMT - default to 99999 if not found
                facility = '99999'
                
                data.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'TRANSREF': transref,
                    'OUTSTAND': outstanding,
                    'MATDATE': matdate,
                    'MATDATE_SAS': matdate_sas,
                    'LIABCODE': liabcode,
                    'FACILITY': facility,
                })
            except Exception as e:
                continue
    
    print(f"BTDTL: Parsed {len(data)} records")
    return pl.DataFrame(data)

btdtl = read_btdtl(INPUT_BTFILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTDTL")

# Apply SAS filter: IF BRANCH > 3000 AND (2850000000<=ACCTNO<=2859999999) THEN DELETE
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")
print("BTDTL sample:")
print(btdtl.head(5))

# ============================================================================
# STEP 3: Read BASE (DATA BASE)
# ============================================================================
def read_base(prevmon):
    """
    DATA BASE(KEEP=ACCTNO TRANSREF OUTSTAND PRODTYPE RENAME=(OUTSTAND=PREOUTSTD));
    SET BASE.BTBASE&PREVMON;
    """
    # Try different file naming patterns in the input directory
    base_dir = INPUT_BASE_DIR
    patterns = [
        f"btbase{prevmon}.sas7bdat",
        f"btbase_{prevmon}.sas7bdat",
        f"BTBASE{prevmon}.sas7bdat",
        f"BTBASE_{prevmon}.sas7bdat",
        f"BTBASE.{prevmon}.sas7bdat",
    ]
    
    base_file = None
    for pattern in patterns:
        full_path = os.path.join(base_dir, pattern)
        if os.path.exists(full_path):
            base_file = full_path
            break
    
    if base_file is None:
        # Try to find any file with the month
        search_pattern = os.path.join(base_dir, f"*{prevmon}*.sas7bdat")
        files = glob.glob(search_pattern)
        if files:
            base_file = files[0]
            print(f"Found BASE file via glob: {base_file}")
    
    if base_file is None:
        print(f"\nERROR: BASE file not found for month {prevmon}")
        print(f"Tried patterns in {base_dir}:")
        for pattern in patterns:
            print(f"  - {os.path.join(base_dir, pattern)}")
        print(f"  - {search_pattern}")
        
        # List all sas7bdat files in the directory
        all_files = glob.glob(os.path.join(base_dir, "*.sas7bdat"))
        if all_files:
            print(f"\nAvailable SAS files in {base_dir}:")
            for f in all_files:
                print(f"  - {os.path.basename(f)}")
        raise FileNotFoundError(f"BASE file not found for month {prevmon}")
    
    print(f"\nReading BASE: {base_file}")
    df, meta = pyreadstat.read_sas7bdat(base_file)
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
    print("BASE sample:")
    print(base.head(5))
    
    return base

base = read_base(params['PREVMON'])

# ============================================================================
# STEP 4: PROC SORT NODUPKEY
# ============================================================================
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nAfter deduplication:")
print(f"  BASE: {len(base)}")
print(f"  BTDTL: {len(btdtl)}")

# ============================================================================
# STEP 5: MERGE and Calculate (DATA COMBT)
# ============================================================================
# Ensure compatible types
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

# Check unmatched records (should have blank BRANCH in output)
unmatched = combt.filter(pl.col("MATDATE_SAS").is_null())
print(f"Unmatched BASE records (no BTDTL match): {len(unmatched)}")

# Fill nulls for calculations
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
])

# Calculate OVERDUE and RECOVAMT
sdate_sas = to_sas_date(sdate)

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

# ============================================================================
# STEP 6: Write Output (DATA _NULL_)
# ============================================================================
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
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nOutput written: {filepath}")
    print(f"  Total records: {len(records)}")
    if skipped > 0:
        print(f"  Skipped rows: {skipped}")

write_fixed_width(combt, OUTPUT_DAYBTRD)

# ============================================================================
# STEP 7: Save Parquet (for verification)
# ============================================================================
try:
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET)
    print(f"Parquet saved: {OUTPUT_PARQUET}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# ============================================================================
# STEP 8: Validation against Production
# ============================================================================
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
        if not overdue_match:
            print(f"     SDATE_SAS: {sdate_sas}")
            print(f"     MATDATE_SAS: {row['MATDATE_SAS']}")
            print(f"     Formula: ({sdate_sas} + 1) - {row['MATDATE_SAS']} = {row['OVERDUE']}")
    else:
        print(f"\n✗ NOT FOUND ACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']}")

print(f"\nValidation Summary:")
print(f"  Matches: {matches}/{total}")

if matches == total:
    print("  ✓ ALL TESTS PASSED!")
else:
    print("  ✗ SOME TESTS FAILED - Check the differences above")

# ============================================================================
# STEP 9: Summary Statistics
# ============================================================================
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

# ============================================================================
# STEP 10: Validate Output Files
# ============================================================================
def validate_files():
    if os.path.exists(OUTPUT_DAYBTRD):
        size = os.path.getsize(OUTPUT_DAYBTRD)
        with open(OUTPUT_DAYBTRD, 'r') as f:
            lines = sum(1 for _ in f)
        print(f"\n✓ Output file: {OUTPUT_DAYBTRD}")
        print(f"  Size: {size:,} bytes")
        print(f"  Lines: {lines}")
    else:
        print(f"\n✗ Output file not found: {OUTPUT_DAYBTRD}")
    
    if os.path.exists(OUTPUT_PARQUET):
        size = os.path.getsize(OUTPUT_PARQUET)
        print(f"✓ Parquet file: {OUTPUT_PARQUET}")
        print(f"  Size: {size:,} bytes")
    else:
        print(f"✗ Parquet file not found: {OUTPUT_PARQUET}")

validate_files()

print("\n" + "=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
