# EIBDBT12_BANKTRADE_PM12.py
# Complete Python conversion with PBBBTFMT and PBBELF integration

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os

# Import format libraries
try:
    from PBBBTFMT import (
        LIAB_MAPPING, DIRCT_MAPPING, BTFCEPT_MAPPING,
        PRCTYPE_MAPPING, PRCTYPESFS_MAPPING, NSRSLIAB_MAPPING,
        dayr_vectorized, map_liab, map_direct, map_btfcept,
        map_prctype, map_prctypesfs, map_nsrsliab
    )
    print("PBBBTFMT loaded successfully")
except ImportError as e:
    print(f"Warning: PBBBTFMT not available: {e}")
    # Define fallback mappings
    LIAB_MAPPING = {}
    def map_liab(code): return '99999'
    def map_direct(code): return ' '
    def map_btfcept(code): return '99'
    def map_prctype(code): return '99'
    def map_prctypesfs(code): return '99'
    def map_nsrsliab(code): return '99999'

try:
    from PBBELF import (
        format_brchcd, format_cacbrch, format_cacname,
        format_regioff, format_regnew, format_ctype,
        is_perak_branch, is_penang_branch, is_johor_branch,
        is_klang_branch, is_melaka_branch, is_kuching_branch,
        is_kk_branch, is_sro_branch, is_sp_branch, is_srb_branch
    )
    print("PBBELF loaded successfully")
except ImportError as e:
    print(f"Warning: PBBELF not available: {e}")
    # Define fallback functions
    def format_brchcd(code): return ''
    def format_cacbrch(code): return '000'
    def format_cacname(code): return 'NON CAC'
    def format_regioff(code): return 'NON REGION'
    def format_regnew(code): return 'OFF'
    def format_ctype(code): return '  '
    def is_perak_branch(code): return False
    def is_penang_branch(code): return False
    def is_johor_branch(code): return False
    def is_klang_branch(code): return False
    def is_melaka_branch(code): return False
    def is_kuching_branch(code): return False
    def is_kk_branch(code): return False
    def is_sro_branch(code): return False
    def is_sp_branch(code): return False
    def is_srb_branch(code): return False

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "input/prod/btbase05.sas7bdat"
OUTPUT_TEXT_FILE = "DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12.parquet"

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
    Read BTPM12 text file matching SAS INPUT:
    @002 BRANCH 4.      (positions 1-4, 0-based: 1-4)
    @006 ACCTNO $10.    (positions 5-14, 0-based: 5-14)
    @016 TRANSREF $7.   (positions 15-21, 0-based: 15-21)
    @023 OUTSTAND 15.2  (positions 22-36, 0-based: 22-36)
    @038 MATDT $6.      (positions 37-42, 0-based: 37-42)
    @043 LIABCODE $3.   (positions 42-44, 0-based: 42-44)
    """
    data = []
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n').rstrip('\r')
            if not line.strip() or line.strip().startswith('1BKT'):
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
                
                # Parse MATDT: DDMMYY
                day = int(matdt[0:2])
                month = int(matdt[2:4])
                year = 2000 + int(matdt[4:6])
                
                matdate = date(year, month, day)
                matdate_sas = to_sas_date(matdate)
                
                # Apply LIAB format - THIS IS WHERE PBBBTFMT IS USED
                # FACILITY = PUT(LIABCODE, $LIAB.)
                # Using the LIAB_MAPPING from PBBBTFMT
                facility = map_liab(liabcode)
                
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

# Apply SAS filter
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"BTDTL after filter: {len(btdtl)} records")

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (BTBASE05)
# --------------------------------------------------------------------
def read_base_sas(filepath):
    """Read BTBASE05 SAS dataset"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"BASE file not found: {filepath}")
    
    print(f"\nReading BASE: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(filepath)
    base = pl.from_pandas(df)
    
    print(f"BASE columns: {base.columns}")
    print(f"BASE records: {len(base)}")
    
    # Column mapping based on actual structure:
    # Col 0: TRANSREX (ignored)
    # Col 1: BRANCH (ignored)
    # Col 2: ACCTNO
    # Col 3: OUTSTAND (becomes PREOUTSTD)
    # Col 4: TRANSREF
    # Col 5: FACILITY (ignored for now)
    # Col 6: PRODTYPE
    # Col 7: DAYS (ignored)
    
    col_names = base.columns
    
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

base = read_base_sas(INPUT_BTBASE_FILE)

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

# Calculate OVERDUE and RECOVAMT
sdate_sas = params["SDATE_SAS"]

print(f"\nUsing SDATE_SAS: {sdate_sas}")

combt = combt.with_columns([
    # OVERDUE = (SDATE + 1) - MATDATE
    pl.when(pl.col("MATDATE_SAS").is_not_null())
    .then((sdate_sas + 1) - pl.col("MATDATE_SAS"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD - OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
])

combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

print(f"\nCalculations complete")
print(f"  Records with OVERDUE > 0: {(combt['OVERDUE'] > 0).sum()}")

# --------------------------------------------------------------------
# Step 5: Write output
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write matching SAS PUT statement exactly"""
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
            facility = row.get('FACILITY', '99999') or '99999'
            
            # Ensure facility is 5 chars max
            facility = str(facility)[:5]
            
            record = [' '] * 103
            
            # @001 BRANCH 5.
            if branch is not None and branch != 0:
                record[0:5] = f"{int(branch):5d}"
            else:
                record[0:5] = '     '
            
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
                record[32:49] = ' ' * 17
            
            # @051 OUTSTAND 17.2
            if outstanding != 0:
                record[50:67] = f"{float(outstanding):17.2f}"
            else:
                record[50:67] = ' ' * 17
            
            # @069 OVERDUE 10.
            if overdue != 0:
                record[68:78] = f"{int(overdue):10d}"
            else:
                record[68:78] = ' ' * 10
            
            # @080 RECOVAMT 17.2
            if recovamt != 0:
                record[79:96] = f"{float(recovamt):17.2f}"
            else:
                record[79:96] = ' ' * 17
            
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
    
    print(f"\nOutput: {filepath} ({len(records)} records)")
    if skipped > 0:
        print(f"  Skipped rows: {skipped}")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save Parquet
try:
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
    {"ACCTNO": 2500667206, "TRANSREF": "Y090778", "EXP_OVERDUE": 67},
    {"ACCTNO": 2505707731, "TRANSREF": "Y080273", "EXP_OVERDUE": 1180},
    {"ACCTNO": 2501873900, "TRANSREF": "Y011618", "EXP_OVERDUE": 7074},
]

print("\nComparing with production:")
for test in test_cases:
    match = combt.filter(
        (pl.col("ACCTNO") == test["ACCTNO"]) & 
        (pl.col("TRANSREF") == test["TRANSREF"])
    )
    
    if len(match) > 0:
        row = match.row(0, named=True)
        print(f"\nACCTNO: {test['ACCTNO']}, TRANSREF: {test['TRANSREF']}")
        print(f"  OVERDUE: Expected={test['EXP_OVERDUE']}, Got={row['OVERDUE']}")
        print(f"  FACILITY: {row.get('FACILITY', 'N/A')}")
        
        if row['OVERDUE'] == test['EXP_OVERDUE']:
            print("  ✓ OVERDUE matches!")
        else:
            diff = row['OVERDUE'] - test['EXP_OVERDUE']
            print(f"  ✗ OVERDUE differs by {diff}")
            print(f"     SDATE_SAS: {params['SDATE_SAS']}")
            print(f"     MATDATE_SAS: {row['MATDATE_SAS']}")
            print(f"     Formula: ({params['SDATE_SAS']} + 1) - {row['MATDATE_SAS']} = {row['OVERDUE']}")
    else:
        print(f"\nNOT FOUND: {test['ACCTNO']}")

print("\n" + "=" * 80)
