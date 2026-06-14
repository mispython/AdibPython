import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import datetime
import os
from pathlib import Path
from functools import lru_cache

# ============================================
# CONFIGURATION - Only 3 input files
# ============================================
CFG = {
    "reptdate": datetime.date.today(),  # Or read from file
    "input": {
        "saca": "/path/to/input/SACA.txt",      # INFILE SACA
        "plusfd": "/path/to/input/PLUSFD.txt",  # INFILE PLUSFD  
        "fcyfd": "/path/to/input/FCYFD.txt"     # INFILE FCYFD
    },
    "output": "/path/to/output"
}

# Create output directories
for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
    Path(os.path.join(CFG["output"], name)).mkdir(parents=True, exist_ok=True)

# ============================================
# HELPER FUNCTIONS
# ============================================
@lru_cache(maxsize=1)
def get_report_metadata():
    """Calculate report date metadata (once, cached)"""
    dt = CFG["reptdate"]
    day = dt.day
    return {
        'dt': dt,
        'dayone': datetime.date(dt.year, dt.month, 1),
        'nowk': "1" if day <= 8 else "2" if day <= 15 else "3" if day <= 22 else "4",
        'rdate': dt.strftime("%d%m%y")
    }

def clean_value(val, dtype):
    """Clean and convert values, handling special cases like '.' for missing data"""
    if not val or val.strip() == '' or val.strip() == '.':
        return None if dtype != float else 0.0
    
    val = val.strip()
    
    if dtype == int:
        try:
            return int(float(val))  # Handle decimal points in integer fields
        except:
            return None
    elif dtype == float:
        try:
            return float(val)
        except:
            return 0.0
    return val

def read_fixed_width(filepath, cols):
    """Read fixed-width text file based on column definitions"""
    if not os.path.exists(filepath):
        print(f"  ⚠ File not found: {filepath}")
        return pa.Table.from_pylist([])
    
    data = []
    line_count = 0
    
    with open(filepath, 'r') as f:
        for line in f:
            line_count += 1
            if len(line) >= cols[-1][1]:
                record = {}
                for name, start, end, dtype in cols:
                    raw_val = line[start-1:end] if end <= len(line) else line[start-1:]
                    cleaned_val = clean_value(raw_val, dtype)
                    record[name] = cleaned_val
                data.append(record)
            
            # Optional: show progress for large files
            if line_count % 100000 == 0:
                print(f"    Read {line_count:,} lines...")
    
    print(f"  Read {len(data):,} records from {os.path.basename(filepath)}")
    return pa.Table.from_pylist(data)

def merge_and_export(flatfile_tbl, output_dir, filename):
    """
    Process flatfile, calculate nodays, export to CSV & Parquet
    """
    if not flatfile_tbl.num_rows:
        print(f"  ⚠ No data for {filename}")
        return
    
    md = get_report_metadata()
    
    # Add required fields
    result = flatfile_tbl.append_column(
        "reptdate",
        pa.scalar(md['dt'])
    )
    
    # Calculate NODAYS (SAS logic: default to day of month)
    # Since we don't have opendate from deposit tables, use reptdate.day
    result = result.append_column("nodays",
        pa.scalar(md['dt'].day)
    )
    
    # Drop reptdate (as in SAS DROP REPTDATE)
    result = result.drop(['reptdate'])
    
    # Export to CSV
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    parquet_path = os.path.join(output_dir, f"{filename}.parquet")
    
    # Convert to pandas for CSV (handle large files in chunks if needed)
    df = result.to_pandas()
    df.to_csv(csv_path, index=False)
    
    # Export to Parquet (more efficient)
    pa.parquet.write_table(result, parquet_path)
    
    print(f"  ✓ {filename}: {result.num_rows:,} rows exported")
    print(f"    - CSV: {csv_path}")
    print(f"    - Parquet: {parquet_path}")
    return result

# ============================================
# MAIN PROCESSING
# ============================================
def main():
    md = get_report_metadata()
    print(f"\n{'='*60}")
    print(f"Processing Report Date: {md['dt']} (Week {md['nowk']})")
    print(f"{'='*60}\n")
    
    # Define column specifications based on SAS INPUT statements
    # @001 ACCTNO 11., @012 CDNO 11., @023 BRANCH 3., @026 PRODUCT 3., 
    # @029 INTPLAN IB2., @032 CURCUM 16.2, @048 MTDBAL 16.2
    flatfile_cols = [
        ('acctno', 1, 11, int),      # @001 ACCTNO 11.
        ('cdno', 12, 22, int),       # @012 CDNO 11.
        ('branch', 23, 25, int),     # @023 BRANCH 3.
        ('product', 26, 28, int),    # @026 PRODUCT 3.
        ('intplan', 29, 30, int),    # @029 INTPLAN IB2. (2-byte integer)
        ('curcum', 32, 47, float),   # @032 CURCUM 16.2 (16 chars, 2 decimals)
        ('mtdbal', 48, 63, float)    # @048 MTDBAL 16.2 (16 chars, 2 decimals)
    ]
    
    # 1. Process SACA
    print("1. Processing FD_SACA...")
    if os.path.exists(CFG["input"]["saca"]):
        saca_data = read_fixed_width(CFG["input"]["saca"], flatfile_cols)
        merge_and_export(saca_data, 
                        os.path.join(CFG["output"], "FD_SACA"), 
                        "FD_SACA")
    else:
        print(f"  ⚠ Input file not found: {CFG['input']['saca']}")
    
    # 2. Process PLUSFD
    print("\n2. Processing FD_PLUSFD...")
    if os.path.exists(CFG["input"]["plusfd"]):
        plusfd_data = read_fixed_width(CFG["input"]["plusfd"], flatfile_cols)
        merge_and_export(plusfd_data, 
                        os.path.join(CFG["output"], "FD_PLUSFD"), 
                        "FD_PLUSFD")
    else:
        print(f"  ⚠ Input file not found: {CFG['input']['plusfd']}")
    
    # 3. Process FCYFD
    print("\n3. Processing FD_FCYFD...")
    if os.path.exists(CFG["input"]["fcyfd"]):
        fcyfd_data = read_fixed_width(CFG["input"]["fcyfd"], flatfile_cols)
        merge_and_export(fcyfd_data, 
                        os.path.join(CFG["output"], "FD_FCYFD"), 
                        "FD_FCYFD")
    else:
        print(f"  ⚠ Input file not found: {CFG['input']['fcyfd']}")
    
    # Save metadata
    print("\n4. Saving metadata...")
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        meta_path = os.path.join(CFG["output"], name, "metadata.txt")
        with open(meta_path, 'w') as f:
            f.write(f"Report Date: {md['dt']}\n")
            f.write(f"RDATE: {md['rdate']}\n")
            f.write(f"NOWK: {md['nowk']}\n")
            f.write(f"DAYONE: {md['dayone']}\n")
            f.write(f"Processing Time: {datetime.datetime.now()}\n")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE! Output saved to: {CFG['output']}")
    print(f"{'='*60}")
    
    # Show output files
    print("\nOutput files created:")
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        out_dir = os.path.join(CFG["output"], name)
        csv_file = os.path.join(out_dir, f"{name}.csv")
        parquet_file = os.path.join(out_dir, f"{name}.parquet")
        if os.path.exists(csv_file):
            print(f"  ✓ {name}.csv")
            print(f"  ✓ {name}.parquet")

if __name__ == "__main__":
    main()
