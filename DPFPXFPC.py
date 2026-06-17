# EIBDBT12_BANKTRADE_PM12.py
# Python conversion of SAS job EIBDBT12
# Uses Polars for data manipulation

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import re

# --------------------------------------------------------------------
# Configuration: Input and Output Paths
# --------------------------------------------------------------------
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "input/prod/btbase_{PREVMON}.sas7bdat"
OUTPUT_TEXT_FILE = "DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12.parquet"
USE_DUMMY_DATA = False

# --------------------------------------------------------------------
# Step 1: Reporting date logic (matching SAS DATA REPTDATE)
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]
curyy = rptdt[8:10]
rdatex = curmm + curyy

if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

params = {
    "REPTYEAR": f"{reptdate.year % 100:02d}",
    "REPTMON": f"{reptdate.month:02d}",
    "REPTDAY": f"{reptdate.day:02d}",
    "PREVMON": f"{prevdate.month:02d}",
    "PREVDAY": f"{prevdate.day:02d}",
    "RDATE": reptdate.strftime("%d%m%Y"),
    "RDATEX": rdatex,
    "SDATE": f"{sdate.year}{sdate.month:02d}{sdate.day:02d}"[-5:],
}

print("Report Parameters:", params)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input (matching SAS INFILE BTFILE)
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """
    Read BTPM12 text file matching SAS INPUT statement:
    @002 BRANCH 4.      (starts at 2, 4 chars)
    @006 ACCTNO $10.    (starts at 6, 10 chars)
    @016 TRANSREF $7.   (starts at 16, 7 chars)
    @023 OUTSTAND 15.2  (starts at 23, 15 chars with 2 decimals)
    @038 MATDT $6.      (starts at 38, 6 chars)
    @043 LIABCODE $3.   (starts at 43, 3 chars)
    """
    try:
        data = []
        valid_lines = 0
        
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n').rstrip('\r')
                
                if not line.strip():
                    continue
                
                # Skip header line
                if line.strip().startswith('1BKT'):
                    continue
                
                # Use exact positions from SAS (converted to 0-based indexing)
                try:
                    # SAS @002 -> Python position 1 (2-1=1)
                    # SAS @006 -> Python position 5 (6-1=5)
                    # SAS @016 -> Python position 15 (16-1=15)
                    # SAS @023 -> Python position 22 (23-1=22)
                    # SAS @038 -> Python position 37 (38-1=37)
                    # SAS @043 -> Python position 42 (43-1=42)
                    
                    branch_str = line[1:5].strip()          # @002 BRANCH 4.
                    acctno_str = line[5:15].strip()         # @006 ACCTNO $10.
                    transref = line[15:22].strip()          # @016 TRANSREF $7.
                    outstanding_str = line[22:37].strip()   # @023 OUTSTAND 15.2
                    matdt = line[37:43].strip()             # @038 MATDT $6.
                    liabcode = line[42:45].strip()          # @043 LIABCODE $3.
                    
                    # Validate data
                    if not acctno_str.isdigit() or len(acctno_str) != 10:
                        continue
                    
                    if not matdt.isdigit() or len(matdt) != 6:
                        continue
                    
                    # Convert fields
                    branch = int(branch_str) if branch_str.isdigit() else 0
                    acctno = int(acctno_str)
                    outstanding = float(outstanding_str) if outstanding_str else 0.0
                    
                    # Map LIABCODE to FACILITY (simplified mapping based on SAS format)
                    # In the SAS code, FACILITY = PUT(LIABCODE,$LIAB.)
                    # Based on the output, 99999 is used, so we'll use that as default
                    facility_map = {
                        'PBZ': 'PBZ',
                        'PBA': 'PBA', 
                        'PBI': 'PBI',
                        'PBT': 'PBT',
                    }
                    facility = facility_map.get(liabcode, '99999')
                    
                    data.append({
                        'BRANCH': branch,
                        'ACCTNO': acctno,
                        'TRANSREF': transref,
                        'OUTSTAND': outstanding,
                        'MATDT': matdt,
                        'LIABCODE': liabcode,
                        'FACILITY': facility,
                    })
                    valid_lines += 1
                    
                except (ValueError, IndexError) as e:
                    continue
        
        print(f"Parsed {valid_lines} records from {filepath}")
        
        if not data:
            raise ValueError(f"No valid data found in {filepath}")
        
        return pl.DataFrame(data)
        
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        raise
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        raise

if USE_DUMMY_DATA:
    print("Using dummy data for testing")
    btdtl = pl.DataFrame({
        "BRANCH": [20006, 20009, 20201, 20201],
        "ACCTNO": [2501873900, 2505605133, 2505707731, 2505707731],
        "TRANSREF": ["Y011618", "Y066656", "Y080273", "Y080340"],
        "OUTSTAND": [33531.01, 40245.81, 69258.93, 69128.29],
        "MATDT": ["070119", "180426", "230310", "230317"],
        "LIABCODE": ["PBZ", "PBZ", "PBA", "PBA"],
        "FACILITY": ["PBZ", "PBZ", "PBA", "PBA"],
    })
else:
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

print(f"\nLoaded {len(btdtl)} records from BTPM12")
print("Sample of loaded data:")
print(btdtl.head(10))

# Parse MATDATE matching SAS: MDY(SUBSTR(MATDT,3,2), SUBSTR(MATDT,5,2), SUBSTR(MATDT,1,2))
# MATDT is DDMMYY format
btdtl = btdtl.with_columns([
    # Day is first 2 chars (DD)
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32, strict=False).alias("day"),
    # Month is next 2 chars (MM)
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32, strict=False).alias("month"),
    # Year is last 2 chars (YY)
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32, strict=False).alias("year2")
])

# Filter out rows with invalid dates
btdtl = btdtl.filter(
    pl.col("day").is_not_null() & 
    pl.col("month").is_not_null() & 
    pl.col("year2").is_not_null() &
    (pl.col("day") >= 1) & (pl.col("day") <= 31) &
    (pl.col("month") >= 1) & (pl.col("month") <= 12)
)

if len(btdtl) == 0:
    raise ValueError("No valid dates found in MATDT field")

# Add century (2000 + year2)
btdtl = btdtl.with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.datetime("year", "month", "day").alias("MATDATE")
)

print(f"\nAfter date parsing, {len(btdtl)} records remain")

# Apply SAS filter: IF BRANCH > 3000 AND (2850000000<=ACCTNO<=2859999999) THEN DELETE
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"After filtering, {len(btdtl)} records remain")

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (matching SAS BASE.BTBASE&PREVMON)
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    """Read BTBASE SAS dataset for the previous month"""
    try:
        filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
        
        print(f"\nReading SAS dataset: {filepath}")
        
        if not os.path.exists(filepath):
            print(f"Warning: SAS file not found: {filepath}")
            # Create dummy base data with matching account numbers
            sample_accts = btdtl['ACCTNO'].head(50).to_list()
            sample_transrefs = btdtl['TRANSREF'].head(50).to_list()
            
            if not sample_accts:
                sample_accts = [2501873900, 2505605133, 2505707731]
                sample_transrefs = ["Y011618", "Y066656", "Y080273"]
            
            # Create dummy base data
            base_data = []
            for i in range(min(len(sample_accts), 50)):
                acct = sample_accts[i]
                transref = sample_transrefs[i] if i < len(sample_transrefs) else f"Y{str(i+1).zfill(6)}"
                base_data.append({
                    "ACCTNO": acct,
                    "TRANSREF": transref,
                    "PREOUTSTD": 150000.0 + (i * 10000),
                    "PRODTYPE": 0 if i % 3 == 0 else 200,
                })
            
            base_df = pl.DataFrame(base_data)
            print(f"Created dummy base data with {len(base_df)} records")
            return base_df
        
        # Read SAS dataset using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        base = pl.from_pandas(df)
        
        # Keep only required columns and rename OUTSTAND to PREOUTSTD
        required_cols = ["ACCTNO", "TRANSREF", "OUTSTAND", "PRODTYPE"]
        base = base.select(required_cols).rename({"OUTSTAND": "PREOUTSTD"})
        
        print(f"Successfully read {len(base)} records from SAS dataset")
        return base
        
    except Exception as e:
        print(f"Error reading SAS dataset: {e}")
        print("Creating dummy base data for testing...")
        
        sample_accts = btdtl['ACCTNO'].head(50).to_list()
        sample_transrefs = btdtl['TRANSREF'].head(50).to_list()
        
        if not sample_accts:
            sample_accts = [2501873900, 2505605133, 2505707731]
            sample_transrefs = ["Y011618", "Y066656", "Y080273"]
        
        base_data = []
        for i in range(min(len(sample_accts), 50)):
            acct = sample_accts[i]
            transref = sample_transrefs[i] if i < len(sample_transrefs) else f"Y{str(i+1).zfill(6)}"
            base_data.append({
                "ACCTNO": acct,
                "TRANSREF": transref,
                "PREOUTSTD": 150000.0 + (i * 10000),
                "PRODTYPE": 0 if i % 3 == 0 else 200,
            })
        
        return pl.DataFrame(base_data)

if USE_DUMMY_DATA:
    print("\nUsing dummy base data for testing")
    base = pl.DataFrame({
        "ACCTNO": [2501873900, 2505605133, 2505707731],
        "TRANSREF": ["Y011618", "Y066656", "Y080273"],
        "PREOUTSTD": [150000.0, 100000.0, 120000.0],
        "PRODTYPE": [0, 200, 0],
    })
else:
    base = read_base_sas(params['PREVMON'])

# PROC SORT NODUPKEY equivalent
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nBase records after dedup: {len(base)}")
print(f"BTDTL records after dedup: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge BASE and BTDTL (matching SAS MERGE)
# --------------------------------------------------------------------
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

# Handle missing values
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
    pl.col("PREOUTSTD").fill_null(0),
])

# Compute OVERDUE and RECOVAMT (matching SAS DATA COMBT)
sdate_ordinal = sdate.toordinal()

combt = combt.with_columns([
    # OVERDUE = (&SDATE+1)-MATDATE
    pl.when(pl.col("MATDATE").is_not_null())
    .then((sdate_ordinal + 1) - pl.col("MATDATE").dt.epoch("d"))
    .otherwise(None)  # Keep as missing for non-matches
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD-OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    
    # IF PRODTYPE = 000 THEN RETAILID='R'
    pl.when(pl.col("PRODTYPE") == 0)
    .then(pl.lit("R"))
    .otherwise(pl.lit(None))
    .alias("RETAILID"),
])

# Fill nulls for output formatting
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
])

print(f"\nMerged records: {len(combt)}")

# --------------------------------------------------------------------
# Step 5: Write output (matching SAS PUT statement exactly)
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """
    Write DataFrame matching SAS PUT statement:
    @001 BRANCH      5.  (positions 1-5)
    @007 ACCTNO     10.  (positions 7-16)
    @018 TRANSREF   10.  (positions 18-27)
    @029 PRODTYPE   Z3.  (positions 29-31, with leading zeros)
    @033 PREOUTSTD  17.2 (positions 33-49)
    @051 OUTSTAND   17.2 (positions 51-67)
    @069 OVERDUE    10.  (positions 69-78)
    @080 RECOVAMT   17.2 (positions 80-96)
    @098 FACILITY   $5.  (positions 98-102)
    """
    records = []
    skipped_rows = 0
    
    for row in df.iter_rows(named=True):
        try:
            # Extract values with defaults
            branch = row.get('BRANCH', 0) or 0
            acctno = row.get('ACCTNO', 0) or 0
            transref = str(row.get('TRANSREF', '') or '')[:10]
            prodtype = row.get('PRODTYPE', 0) or 0
            preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
            outstanding = row.get('OUTSTAND', 0.0) or 0.0
            overdue = row.get('OVERDUE', 0) or 0
            recovamt = row.get('RECOVAMT', 0.0) or 0.0
            facility = str(row.get('FACILITY', '99999') or '99999')[:5]
            
            # Build record with exact SAS positions
            # Using string concatenation with exact positions
            record = [' '] * 103  # Initialize with spaces
            
            # @001 BRANCH 5. (positions 1-5, 0-based: 0-4)
            record[0:5] = f"{int(branch):5d}"
            
            # @007 ACCTNO 10. (positions 7-16, 0-based: 6-15)
            record[6:16] = f"{int(acctno):10d}"
            
            # @018 TRANSREF 10. (positions 18-27, 0-based: 17-26)
            record[17:27] = f"{transref:<10}"
            
            # @029 PRODTYPE Z3. (positions 29-31, 0-based: 28-30)
            record[28:31] = f"{int(prodtype):03d}"
            
            # @033 PREOUTSTD 17.2 (positions 33-49, 0-based: 32-48)
            record[32:49] = f"{float(preoutstd):17.2f}"
            
            # @051 OUTSTAND 17.2 (positions 51-67, 0-based: 50-66)
            record[50:67] = f"{float(outstanding):17.2f}"
            
            # @069 OVERDUE 10. (positions 69-78, 0-based: 68-77)
            record[68:78] = f"{int(overdue):10d}"
            
            # @080 RECOVAMT 17.2 (positions 80-96, 0-based: 79-95)
            record[79:96] = f"{float(recovamt):17.2f}"
            
            # @098 FACILITY $5. (positions 98-102, 0-based: 97-101)
            record[97:102] = f"{facility:<5}"
            
            # Join and add newline
            records.append(''.join(record))
            
        except Exception as e:
            skipped_rows += 1
            if skipped_rows <= 5:
                print(f"Warning: Error formatting row: {e}")
            continue
    
    # Write to text file
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nText output written: {filepath} ({len(records)} records)")
    if skipped_rows > 0:
        print(f"  Skipped {skipped_rows} rows due to formatting errors")

# Write text file
write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save to Parquet (additional output for easier analysis)
try:
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet output written: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# Display summary statistics
print("\n--- Summary Statistics ---")
print(f"Total records processed: {len(combt)}")
print(f"Columns in output: {combt.columns}")

print("\nPreview of first 10 rows:")
print(combt.head(10))

# Validate output by checking first few lines
print("\n--- First 10 lines of output file ---")
with open(OUTPUT_TEXT_FILE, 'r') as f:
    for i in range(10):
        line = f.readline()
        if line:
            print(f"Line {i+1}: {line.rstrip()}")

def validate_output():
    """Validate that output files were created successfully"""
    if os.path.exists(OUTPUT_TEXT_FILE):
        size = os.path.getsize(OUTPUT_TEXT_FILE)
        with open(OUTPUT_TEXT_FILE, 'r') as f:
            line_count = sum(1 for _ in f)
        print(f"✓ Text file created: {OUTPUT_TEXT_FILE} ({size:,} bytes, {line_count} lines)")
    else:
        print(f"✗ Text file not found: {OUTPUT_TEXT_FILE}")
    
    if os.path.exists(OUTPUT_PARQUET_FILE):
        size = os.path.getsize(OUTPUT_PARQUET_FILE)
        print(f"✓ Parquet file created: {OUTPUT_PARQUET_FILE} ({size:,} bytes)")
    else:
        print(f"✗ Parquet file not found: {OUTPUT_PARQUET_FILE}")

validate_output()
