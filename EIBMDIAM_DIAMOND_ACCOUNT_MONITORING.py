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

def read_fixed_width(filepath, cols):
    """Read fixed-width text file based on column definitions"""
    if not os.path.exists(filepath):
        print(f"  ⚠ File not found: {filepath}")
        return pa.Table.from_pylist([])
    
    data = []
    with open(filepath, 'r') as f:
        for line in f:
            if len(line) >= cols[-1][1]:
                record = {}
                for name, start, end, dtype in cols:
                    val = line[start-1:end].strip()
                    if val:
                        record[name] = dtype(val) if dtype != float else float(val)
                    else:
                        record[name] = None if dtype != float else 0.0
                data.append(record)
    print(f"  Read {len(data):,} records from {os.path.basename(filepath)}")
    return pa.Table.from_pylist(data)

def merge_and_export(flatfile_tbl, output_dir, filename):
    """
    Merge flatfile with itself (since no separate deposit tables),
    calculate nodays, export to CSV & Parquet
    """
    if not flatfile_tbl.num_rows:
        print(f"  ⚠ No data for {filename}")
        return
    
    md = get_report_metadata()
    
    # Add opendate and reptdate based on SAS logic
    # In original SAS, this came from DEPOSIT.SAVING/CURRENT/FD
    # Here we need to derive or use default values
    
    # For now, add placeholder opendate (you'll need to adjust based on your data)
    # If your flatfile has an opendate field, use that instead
    result = flatfile_tbl.append_column(
        "opendate",
        pa.scalar(md['dayone'])  # Placeholder - adjust based on your actual data
    ).append_column(
        "reptdate",
        pa.scalar(md['dt'])
    )
    
    # Calculate NODAYS (SAS logic: if opendate>0 then (reptdate-opendate)+1 else day(reptdate))
    result = result.append_column("nodays",
        pc.if_else(
            pc.field("opendate").is_valid(),
            pc.add(
                pc.subtract(pa.scalar(md['dt']), pc.field("opendate")).cast(pa.int64()),
                pa.scalar(1)
            ),
            pa.scalar(md['dt'].day)
        )
    )
    
    # Drop reptdate (as in SAS DROP REPTDATE)
    result = result.drop(['reptdate'])
    
    # Export
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    parquet_path = os.path.join(output_dir, f"{filename}.parquet")
    
    result.to_pandas().to_csv(csv_path, index=False)
    pa.parquet.write_table(result, parquet_path)
    
    print(f"  ✓ {filename}: {result.num_rows:,} rows exported")
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
    # @001 ACCTNO 11., @012 CDNO 11., @023 BRANCH 3., etc.
    flatfile_cols = [
        ('acctno', 1, 11, int),      # @001 ACCTNO 11.
        ('cdno', 12, 22, int),       # @012 CDNO 11.
        ('branch', 23, 25, int),     # @023 BRANCH 3.
        ('product', 26, 28, int),    # @026 PRODUCT 3.
        ('intplan', 29, 30, int),    # @029 INTPLAN IB2.
        ('curcum', 32, 47, float),   # @032 CURCUM 16.2
        ('mtdbal', 48, 63, float)    # @048 MTDBAL 16.2
    ]
    
    # 1. Process SACA
    print("1. Processing FD_SACA...")
    saca_data = read_fixed_width(CFG["input"]["saca"], flatfile_cols)
    merge_and_export(saca_data, 
                    os.path.join(CFG["output"], "FD_SACA"), 
                    "FD_SACA")
    
    # 2. Process PLUSFD
    print("\n2. Processing FD_PLUSFD...")
    plusfd_data = read_fixed_width(CFG["input"]["plusfd"], flatfile_cols)
    merge_and_export(plusfd_data, 
                    os.path.join(CFG["output"], "FD_PLUSFD"), 
                    "FD_PLUSFD")
    
    # 3. Process FCYFD
    print("\n3. Processing FD_FCYFD...")
    fcyfd_data = read_fixed_width(CFG["input"]["fcyfd"], flatfile_cols)
    merge_and_export(fcyfd_data, 
                    os.path.join(CFG["output"], "FD_FCYFD"), 
                    "FD_FCYFD")
    
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
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE! Output saved to: {CFG['output']}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
