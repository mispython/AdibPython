import duckdb
import pyarrow as pa
import pyarrow.csv as csv
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

def read_fixed_width_python(filepath, cols):
    """Read fixed-width text file using Python (reliable for large files)"""
    data = []
    line_count = 0
    error_count = 0
    
    print(f"  Reading {filepath}...")
    
    with open(filepath, 'r') as f:
        for line in f:
            line_count += 1
            line = line.rstrip('\n\r')
            
            # Skip empty lines
            if not line.strip():
                continue
                
            # Check if line has minimum required length
            if len(line) >= cols[-1][1]:
                try:
                    record = {}
                    for name, start, end, dtype in cols:
                        # Adjust for 0-based indexing
                        start_idx = start - 1
                        end_idx = min(end, len(line))
                        raw_val = line[start_idx:end_idx] if end_idx > start_idx else ""
                        cleaned_val = clean_value(raw_val, dtype)
                        record[name] = cleaned_val
                    data.append(record)
                except Exception as e:
                    error_count += 1
                    if error_count <= 10:  # Show first 10 errors only
                        print(f"    Warning: Error processing line {line_count}: {e}")
            elif line_count <= 10:  # Show first 10 short lines as warning
                print(f"    Warning: Line {line_count} too short ({len(line)} chars)")
            
            # Show progress for large files
            if line_count % 500000 == 0:
                print(f"    Processed {line_count:,} lines, {len(data):,} records...")
    
    print(f"  Read {len(data):,} records from {os.path.basename(filepath)} (processed {line_count:,} lines)")
    if error_count > 0:
        print(f"  Warnings: {error_count} lines had errors")
    
    if not data:
        return pa.Table.from_pylist([])
    
    # Convert to Arrow Table
    return pa.Table.from_pylist(data)

def write_csv_parquet(table, csv_path, parquet_path):
    """Write table to CSV and Parquet formats"""
    # Write to Parquet using DuckDB (more reliable)
    try:
        # Register table in DuckDB
        duckdb.register("temp_table", table)
        # Export to Parquet using DuckDB
        duckdb.execute(f"COPY temp_table TO '{parquet_path}' (FORMAT PARQUET)")
        print(f"    ✓ Parquet: {parquet_path}")
    except Exception as e:
        print(f"    Warning: Parquet export failed: {e}")
        # Fallback: try pyarrow.parquet if available
        try:
            import pyarrow.parquet as pq
            pq.write_table(table, parquet_path, compression='snappy')
            print(f"    ✓ Parquet: {parquet_path}")
        except:
            print(f"    ✗ Parquet export not available")
    
    # Write to CSV using pandas (reliable)
    try:
        df = table.to_pandas()
        df.to_csv(csv_path, index=False)
        print(f"    ✓ CSV: {csv_path}")
    except Exception as e:
        print(f"    ✗ CSV export failed: {e}")

def process_and_export(flatfile_tbl, output_dir, filename):
    """
    Process flatfile, calculate nodays, export to CSV & Parquet
    """
    if not flatfile_tbl or flatfile_tbl.num_rows == 0:
        print(f"  ⚠ No data for {filename}")
        return None
    
    md = get_report_metadata()
    
    print(f"  Processing {filename} with {flatfile_tbl.num_rows:,} records...")
    
    # Create arrays (not scalars) for append_column
    reptdate_array = pa.array([md['dt']] * flatfile_tbl.num_rows, type=pa.date32())
    nodays_array = pa.array([md['dt'].day] * flatfile_tbl.num_rows, type=pa.int64())
    
    # Add columns
    try:
        result = flatfile_tbl.append_column("reptdate", reptdate_array)
        result = result.append_column("nodays", nodays_array)
        
        # Drop reptdate (as in SAS DROP REPTDATE)
        if 'reptdate' in result.column_names:
            result = result.drop(['reptdate'])
        
        # Export
        csv_path = os.path.join(output_dir, f"{filename}.csv")
        parquet_path = os.path.join(output_dir, f"{filename}.parquet")
        
        write_csv_parquet(result, csv_path, parquet_path)
        
        # Get file sizes
        csv_size = os.path.getsize(csv_path) / (1024**2) if os.path.exists(csv_path) else 0
        parquet_size = os.path.getsize(parquet_path) / (1024**2) if os.path.exists(parquet_path) else 0
        
        print(f"    Summary: {result.num_rows:,} rows, CSV: {csv_size:.1f} MB, Parquet: {parquet_size:.1f} MB")
        return result
        
    except Exception as e:
        print(f"  ✗ Error processing {filename}: {e}")
        import traceback
        traceback.print_exc()
        return None

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
    
    results = {}
    
    # Process each file
    for name, input_key in [("FD_SACA", "saca"), ("FD_PLUSFD", "plusfd"), ("FD_FCYFD", "fcyfd")]:
        print(f"\n{'-'*60}")
        print(f"Processing {name}...")
        print(f"{'-'*60}")
        
        input_file = CFG["input"][input_key]
        if os.path.exists(input_file):
            file_start = datetime.datetime.now()
            
            # Read the fixed-width file
            data = read_fixed_width_python(input_file, flatfile_cols)
            
            # Process and export
            if data.num_rows > 0:
                result = process_and_export(data, os.path.join(CFG["output"], name), name)
                if result:
                    results[name] = result
            
            elapsed = (datetime.datetime.now() - file_start).total_seconds()
            print(f"  Time: {elapsed:.1f} seconds")
        else:
            print(f"  ⚠ Input file not found: {input_file}")
            print(f"  Please update the path in CFG['input']['{input_key}']")
    
    # Save metadata
    print(f"\n{'='*60}")
    print("Saving metadata...")
    print(f"{'='*60}")
    
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        output_subdir = os.path.join(CFG["output"], name)
        if os.path.exists(output_subdir):
            meta_path = os.path.join(output_subdir, "metadata.txt")
            with open(meta_path, 'w') as f:
                f.write(f"Report Date: {md['dt']}\n")
                f.write(f"RDATE: {md['rdate']}\n")
                f.write(f"NOWK: {md['nowk']}\n")
                f.write(f"DAYONE: {md['dayone']}\n")
                f.write(f"Processing Date: {datetime.datetime.now()}\n")
                if name in results:
                    f.write(f"Record Count: {results[name].num_rows:,}\n")
            print(f"  ✓ Metadata saved: {meta_path}")
    
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
        if os.path.exists(out_dir):
            csv_file = os.path.join(out_dir, f"{name}.csv")
            parquet_file = os.path.join(out_dir, f"{name}.parquet")
            if os.path.exists(csv_file):
                csv_size = os.path.getsize(csv_file) / (1024**2)
                print(f"  ✓ {name}:")
                print(f"      CSV: {csv_file} ({csv_size:.1f} MB)")
                if os.path.exists(parquet_file):
                    parquet_size = os.path.getsize(parquet_file) / (1024**2)
                    print(f"      Parquet: {parquet_file} ({parquet_size:.1f} MB)")
            else:
                print(f"  ✗ {name}: No output files created")

if __name__ == "__main__":
    main()
