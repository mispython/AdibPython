import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import datetime
import os
from pathlib import Path
from functools import lru_cache

# ============================================
# CONFIGURATION
# ============================================
CFG = {
    "reptdate": datetime.date.today(),  # Or read from file
    "input": {
        "saca_data": "/path/to/input/SACA.txt",
        "plusfd_data": "/path/to/input/PLUSFD.txt",
        "fcyfd_data": "/path/to/input/FCYFD.txt",
        "saving": "/path/to/input/deposit_saving.txt",
        "current": "/path/to/input/deposit_current.txt",
        "fd": "/path/to/input/deposit_fd.txt"
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

def parse_opendate(val):
    """Parse OPENDATE from OPENDT (SAS logic: SUBSTR(PUT(OPENDT,Z11.),1,8))"""
    if not val:
        return None
    s = str(val).strip()[:8]
    try:
        return datetime.date(int(s[4:8]) + (2000 if int(s[4:8]) < 100 else 0), 
                           int(s[:2]), int(s[2:4]))
    except:
        return None

def read_fixed_width(filepath, cols):
    """Read fixed-width text file based on column definitions"""
    if not os.path.exists(filepath):
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
    return pa.Table.from_pylist(data)

def process_accounts(source_tbl, date_col='opendt', min_date=None, max_date=None):
    """Generic account processing: filter by date and remove duplicates"""
    accounts = []
    for row in source_tbl.to_pylist():
        opendate = parse_opendate(row.get(date_col))
        if opendate and min_date <= opendate <= max_date:
            accounts.append({'acctno': row['acctno'], 'opendate': opendate})
    
    unique = {row['acctno']: row['opendate'] for row in accounts}
    return pa.Table.from_pylist([{'acctno': k, 'opendate': v} for k, v in unique.items()])

def merge_and_export(accounts_tbl, flatfile_tbl, output_dir, filename):
    """Merge accounts with flatfile, calculate nodays, export to CSV & Parquet"""
    if not accounts_tbl.num_rows or not flatfile_tbl.num_rows:
        print(f"  ⚠ No data for {filename}")
        return
    
    md = get_report_metadata()
    
    # Merge using DuckDB
    duckdb.register("accounts", accounts_tbl)
    duckdb.register("flatfile", flatfile_tbl)
    
    result = duckdb.sql("""
        SELECT f.*, a.opendate
        FROM flatfile f
        INNER JOIN accounts a ON f.acctno = a.acctno
    """).to_arrow()
    
    # Calculate NODAYS (vectorized)
    result = result.append_column("nodays",
        pc.if_else(pc.field("opendate").is_valid(),
                  pc.add(pc.subtract(pa.scalar(md['dt']), pc.field("opendate")).cast(pa.int64()), 1),
                  pa.scalar(md['dt'].day)))
    
    # Export
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    parquet_path = os.path.join(output_dir, f"{filename}.parquet")
    
    result.to_pandas().to_csv(csv_path, index=False)
    pa.parquet.write_table(result, parquet_path)
    
    print(f"  ✓ {filename}: {result.num_rows:,} rows")
    return result

# ============================================
# MAIN PROCESSING
# ============================================
def main():
    md = get_report_metadata()
    print(f"\n=== Processing {md['dt']} (Week {md['nowk']}) ===\n")
    
    # Define column specifications (name, start, end, type)
    flatfile_cols = [
        ('acctno', 1, 11, int), ('cdno', 12, 22, int), ('branch', 23, 25, int),
        ('product', 26, 28, int), ('intplan', 29, 30, int),
        ('curcum', 32, 47, float), ('mtdbal', 48, 63, float)
    ]
    
    # 1. Process SACA (Saving + Current)
    print("1. Processing FD_SACA...")
    saving = duckdb.read_csv(CFG["input"]["saving"], header=True, auto_detect=True).to_arrow()
    current = duckdb.read_csv(CFG["input"]["current"], header=True, auto_detect=True).to_arrow()
    
    saca_raw = pa.concat_tables([saving, current])
    saca_accounts = process_accounts(saca_raw, min_date=md['dayone'], max_date=md['dt'])
    
    saca_flatfile = read_fixed_width(CFG["input"]["saca_data"], flatfile_cols)
    merge_and_export(saca_accounts, saca_flatfile, 
                    os.path.join(CFG["output"], "FD_SACA"), "FD_SACA")
    
    # 2. Process FD accounts (shared for PLUSFD & FCYFD)
    print("\n2. Processing FD_PLUSFD & FD_FCYFD...")
    fd_raw = duckdb.read_csv(CFG["input"]["fd"], header=True, auto_detect=True).to_arrow()
    fd_accounts = process_accounts(fd_raw, min_date=md['dayone'], max_date=md['dt'])
    
    # Process PLUSFD
    plusfd_flatfile = read_fixed_width(CFG["input"]["plusfd_data"], flatfile_cols)
    merge_and_export(fd_accounts, plusfd_flatfile,
                    os.path.join(CFG["output"], "FD_PLUSFD"), "FD_PLUSFD")
    
    # Process FCYFD
    fcyfd_flatfile = read_fixed_width(CFG["input"]["fcyfd_data"], flatfile_cols)
    merge_and_export(fd_accounts, fcyfd_flatfile,
                    os.path.join(CFG["output"], "FD_FCYFD"), "FD_FCYFD")
    
    # Save metadata
    print("\n3. Saving metadata...")
    for name in ["FD_SACA", "FD_PLUSFD", "FD_FCYFD"]:
        meta_path = os.path.join(CFG["output"], name, "metadata.txt")
        with open(meta_path, 'w') as f:
            f.write(f"Report Date: {md['dt']}\nRDATE: {md['rdate']}\nNOWK: {md['nowk']}\nDAYONE: {md['dayone']}")
    
    print(f"\n✅ Complete! Output in: {CFG['output']}")

if __name__ == "__main__":
    main()
