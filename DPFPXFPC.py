import pyreadstat
import pandas as pd
from pathlib import Path
import os
import sys

# Configuration
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")

def diagnose_sas_file(filepath):
    """Diagnose SAS file reading issues"""
    print(f"\n{'='*80}")
    print(f"DIAGNOSING: {filepath}")
    print(f"{'='*80}")
    
    # 1. Check if file exists
    if not filepath.exists():
        print(f"❌ File does not exist: {filepath}")
        return
    
    # 2. Check file size
    file_size = filepath.stat().st_size
    print(f"📁 File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    
    # 3. Check file permissions
    print(f"🔒 Readable: {os.access(filepath, os.R_OK)}")
    print(f"🔒 Writable: {os.access(filepath, os.W_OK)}")
    
    # 4. Check file extension and magic bytes
    with open(filepath, 'rb') as f:
        magic_bytes = f.read(8)
        print(f"🔢 Magic bytes: {magic_bytes.hex()}")
        
        # SAS7BDAT files start with specific magic bytes
        if magic_bytes[:3] == b'\x00\x00\x00':
            print("✓ Looks like a SAS7BDAT file (starts with null bytes)")
        else:
            print("⚠️  May not be a standard SAS7BDAT file")
    
    # 5. Try reading with different methods
    print("\n📊 Attempting to read file...")
    
    # Method 1: Standard pyreadstat
    try:
        print("Method 1: pyreadstat.read_sas7bdat()")
        df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"✅ SUCCESS! Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"   Columns: {list(df.columns)[:10]}...")
        return df
    except Exception as e:
        print(f"❌ Failed: {type(e).__name__}: {e}")
    
    # Method 2: Try reading with pandas directly
    try:
        print("\nMethod 2: pandas.read_sas()")
        df = pd.read_sas(filepath, format='sas7bdat')
        print(f"✅ SUCCESS! Rows: {len(df)}, Columns: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ Failed: {type(e).__name__}: {e}")
    
    # Method 3: Try with different encoding
    try:
        print("\nMethod 3: pyreadstat with encoding='latin1'")
        df, meta = pyreadstat.read_sas7bdat(filepath, encoding='latin1')
        print(f"✅ SUCCESS! Rows: {len(df)}, Columns: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ Failed: {type(e).__name__}: {e}")
    
    # Method 4: Try reading only first few rows
    try:
        print("\nMethod 4: pyreadstat with rows_limit=1000")
        df, meta = pyreadstat.read_sas7bdat(filepath, rows_limit=1000)
        print(f"✅ SUCCESS! First 1000 rows read")
        return df
    except Exception as e:
        print(f"❌ Failed: {type(e).__name__}: {e}")
    
    # Method 5: Try with chunking
    try:
        print("\nMethod 5: pyreadstat read in chunks")
        reader = pyreadstat.read_sas7bdat(filepath, chunksize=10000)
        chunk_count = 0
        for chunk in reader:
            chunk_count += 1
            print(f"   Read chunk {chunk_count}: {len(chunk[0])} rows")
            if chunk_count >= 3:
                break
        print(f"✅ SUCCESS! Read {chunk_count} chunks")
        return None
    except Exception as e:
        print(f"❌ Failed: {type(e).__name__}: {e}")
    
    print("\n" + "="*80)
    print("DIAGNOSIS COMPLETE - Please check the errors above")
    print("="*80)
    return None

# List all files in directory
print("📂 FILES IN DIRECTORY:")
print("-" * 60)
for file in sorted(pidmfin_path.iterdir()):
    if file.suffix in ['.sas7bdat', '.sas7bcat', '.sas']:
        size_mb = file.stat().st_size / 1024 / 1024
        print(f"  {file.name} - {size_mb:.2f} MB")
print("-" * 60)

# Diagnose each SAS file
for filename in ['cisdepxn.sas7bdat', 'cisdepd.sas7bdat']:
    filepath = pidmfin_path / filename
    diagnose_sas_file(filepath)
