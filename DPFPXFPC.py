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
            # Handle decimal points in integer fields
            return int(float(val))
        except:
            return None
    elif dtype == float:
        try:
            return float(val)
        except:
            return 0.0
    return val

def read_fixed_width_fast(filepath, cols):
    """Read fixed-width text file efficiently using DuckDB"""
    if not os.path.exists(filepath):
        print(f"  ⚠ File not found: {filepath}")
        return pa.Table.from_pylist([])
    
    # Use DuckDB to read fixed-width file efficiently
    # Create a temporary view with substring extraction
    col_defs = []
    for name, start, end, dtype in cols:
        # DuckDB substring is 1-indexed
        col_defs.append(f"SUBSTRING(line, {start}, {end - start + 1}) AS {name}")
    
    # Read file line by line using DuckDB
    col_list = ", ".join(col_defs)
    query = f"""
    SELECT {col_list}
    FROM read_csv_text('{filepath}')
    """
    
    try:
        df = duckdb.sql(query).to_arrow()
        # Clean the data types
        for name, start, end, dtype in cols:
            if dtype == int:
                # Convert to int, handling errors
                df = df.set_column(df.column_names.index(name), 
                                  pa.array([clean_value(str(v), dtype) for v in df[name]], type=pa.int64()))
            elif dtype == float:
                df = df.set_column(df.column_names.index(name), 
                                  pa.array([clean_value(str(v), dtype) for v in df[name]], type=pa.float64()))
        
        print(f"  Read {df.num_rows:,} records from {os.path.basename(filepath)}")
        return df
    except Exception as e:
        print(f"  Error reading with DuckDB, falling back to Python: {e}")
        return read_fixed_width_python(filepath, cols)

def read_fixed_width_python(filepath, cols):
    """Fallback: Read fixed-width text file using Python (slower but reliable)"""
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
            
            # Show progress for large files
            if line_count % 500000 == 0:
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
    
    # Create arrays (not scalars) for append_column
    reptdate_array = pa.array([md['dt']] * flatfile_tbl.num_rows, type=pa.date32())
    nodays_array = pa.array([md['dt'].day] * flatfile_tbl.num_rows, type=pa.int64())
    
    # Add columns
    result = flatfile_tbl.append_column("reptdate", reptdate_array)
    result = result.append_column("nodays", nodays_array)
    
    # Drop reptdate (as in SAS DROP REPTDATE)
    result = result.drop(['reptdate'])
    
    # Export to Parquet first (more efficient)
    parquet_path = os.path.join(output_dir, f"{filename}.parquet")
    pa.parquet.write_table(result, parquet_path, compression='snappy')
    
    # Export to CSV in chunks to avoid memory issues
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    df = result.to_pandas()
    df.to_csv(csv_path, index=False)
    
    # Get file sizes
    parquet_size = os.path.getsize(parquet_path) / (1024**2)  # MB
    csv_size = os.path.getsize(csv_path) / (1024**2)  # MB
    
    print(f"  ✓ {filename}: {result.num_rows:,} rows exported")
    print(f"    - CSV: {csv_path} ({csv_size:.1f} MB)")
    print(f"    - Parquet: {parquet_path} ({parquet_size:.1f} MB)")
    return result

# ============================================
# MAIN PROCESSING
# ============================================
def main():
    start_time = datetime.datetime.now()
    md = get_report_metadata()
    
    print(f"\n{'='*60}")
    print(f"Processing Report Date: {md['dt']} (Week {md['nowk']})")
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Define column specifications based on SAS INPUT statements
    flatfile_cols = [
        ('acctno', 1, 11, int),      # @001 ACCTNO 11.
        ('cdno', 12, 22, int),       # @012 CDNO 11.
        ('branch', 23, 25, int),     # @023 BRANCH 3.
        ('product', 26, 28, int),    # @026 PRODUCT 3.
        ('intplan', 29, 30, int),    # @029 INTPLAN IB2.
        ('curcum', 32, 47, float),   # @032 CURCUM 16.2
        ('mtdbal', 48, 63, float)    # @048 MTDBAL 16.2
    ]
    
    # Process each file
    for name, input_key in [("FD_SACA", "saca"), ("FD_PLUSFD", "plusfd"), ("FD_FCYFD", "fcyfd")]:
        print(f"\n{'-'*60}")
        print(f"Processing {name}...")
        print(f"{'-'*60}")
        
        input_file = CFG["input"][input_key]
        if os.path.exists(input_file):
            file_start = datetime.datetime.now()
            data = read_fixed_width_fast(input_file, flatfile_cols)
            merge_and_export(data, os.path.join(CFG["output"], name), name)
            elapsed = (datetime.datetime.now() - file_start).total_seconds()
            print(f"  Time: {elapsed:.1f} seconds")
        else:
            print(f"  ⚠ Input file not found: {input_file}")
    
    # Save metadata
    print(f"\n{'='*60}")
    print("Saving metadata...")
    print(f"{'='*60}")
    
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        meta_path = os.path.join(CFG["output"], name, "metadata.txt")
        with open(meta_path, 'w') as f:
            f.write(f"Report Date: {md['dt']}\n")
            f.write(f"RDATE: {md['rdate']}\n")
            f.write(f"NOWK: {md['nowk']}\n")
            f.write(f"DAYONE: {md['dayone']}\n")
            f.write(f"Processing Date: {datetime.datetime.now()}\n")
    
    # Summary
    total_elapsed = (datetime.datetime.now() - start_time).total_seconds()
    print(f"\n{'='*60}")
    print(f"✅ PROCESSING COMPLETE!")
    print(f"Total time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes)")
    print(f"Output directory: {CFG['output']}")
    print(f"{'='*60}")
    
    # Show output files
    print("\nOutput files created:")
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        out_dir = os.path.join(CFG["output"], name)
        csv_file = os.path.join(out_dir, f"{name}.csv")
        parquet_file = os.path.join(out_dir, f"{name}.parquet")
        if os.path.exists(csv_file):
            csv_size = os.path.getsize(csv_file) / (1024**2)
            parquet_size = os.path.getsize(parquet_file) / (1024**2)
            print(f"  ✓ {name}:")
            print(f"      CSV: {csv_size:.1f} MB")
            print(f"      Parquet: {parquet_size:.1f} MB")

if __name__ == "__main__":
    main()
