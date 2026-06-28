import pyreadstat
import pandas as pd
from pathlib import Path
import os
import sys
import struct

# Configuration
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")

def diagnose_sas_file_detailed(filepath):
    """Detailed diagnosis of SAS file"""
    print(f"\n{'='*80}")
    print(f"DIAGNOSING: {filepath.name}")
    print(f"{'='*80}")
    
    if not filepath.exists():
        print(f"❌ File does not exist")
        return
    
    # File statistics
    stat = filepath.stat()
    print(f"📁 File size: {stat.st_size:,} bytes ({stat.st_size/1024/1024:.2f} MB)")
    print(f"🔒 Readable: {os.access(filepath, os.R_OK)}")
    print(f"🔒 Writable: {os.access(filepath, os.W_OK)}")
    print(f"📅 Last modified: {datetime.datetime.fromtimestamp(stat.st_mtime)}")
    
    # Read file header (first 1024 bytes)
    try:
        with open(filepath, 'rb') as f:
            header = f.read(1024)
            print(f"\n📊 File header analysis:")
            print(f"   Magic bytes: {header[:16].hex()}")
            
            # Check SAS7BDAT signature
            # SAS7BDAT files typically start with null bytes
            if header[:3] == b'\x00\x00\x00':
                print(f"   ✓ Valid SAS7BDAT signature detected")
            
            # Check for compression
            # Byte 132 often indicates compression type
            if len(header) > 132:
                compression_byte = header[132]
                if compression_byte == 0:
                    print(f"   ✓ No compression")
                elif compression_byte == 1:
                    print(f"   ⚠️  Compressed file (SAS COMPRESS=YES)")
                else:
                    print(f"   ℹ️  Unknown compression: {compression_byte}")
    except Exception as e:
        print(f"   ❌ Error reading header: {e}")
    
    # Try different reading methods
    print(f"\n📖 Attempting to read file...")
    
    methods = [
        ("Standard read", lambda: pyreadstat.read_sas7bdat(filepath)),
        ("With encoding='latin1'", lambda: pyreadstat.read_sas7bdat(filepath, encoding='latin1')),
        ("With encoding='utf-8'", lambda: pyreadstat.read_sas7bdat(filepath, encoding='utf-8')),
        ("With rows_limit=1000", lambda: pyreadstat.read_sas7bdat(filepath, rows_limit=1000)),
        ("With low_memory=True", lambda: pyreadstat.read_sas7bdat(filepath, low_memory=True)),
        ("With formats as pandas", lambda: pyreadstat.read_sas7bdat(filepath, formats_as_dataframe=False)),
    ]
    
    success = False
    for method_name, method in methods:
        try:
            print(f"   Trying: {method_name}...", end=" ")
            df, meta = method()
            print(f"✅ SUCCESS!")
            print(f"      Rows: {len(df):,}")
            print(f"      Columns: {len(df.columns)}")
            print(f"      First 5 columns: {list(df.columns)[:5]}")
            success = True
            break
        except Exception as e:
            print(f"❌ Failed: {str(e)[:100]}")
    
    if not success:
        print(f"\n❌ All methods failed to read the file")
        
        # Try reading with pandas as fallback
        try:
            print(f"   Trying: pandas.read_sas()...", end=" ")
            df = pd.read_sas(filepath, format='sas7bdat')
            print(f"✅ SUCCESS!")
            print(f"      Rows: {len(df):,}")
            print(f"      Columns: {len(df.columns)}")
            success = True
        except Exception as e:
            print(f"❌ Failed: {str(e)[:100]}")
    
    # Check for associated files
    print(f"\n📁 Checking for associated files:")
    base_name = filepath.stem
    for ext in ['.sas7bcat', '.sas7bdat', '.sas']:
        check_file = filepath.parent / f"{base_name}{ext}"
        if check_file.exists() and check_file != filepath:
            size_mb = check_file.stat().st_size / 1024 / 1024
            print(f"   Found: {check_file.name} ({size_mb:.2f} MB)")
    
    # Check directory permissions
    print(f"\n📁 Directory information:")
    print(f"   Directory: {filepath.parent}")
    print(f"   Directory readable: {os.access(filepath.parent, os.R_OK)}")
    print(f"   Directory writable: {os.access(filepath.parent, os.W_OK)}")
    
    # List all SAS files in directory
    print(f"\n📋 All SAS files in directory:")
    for f in sorted(filepath.parent.glob("*.sas7bdat")):
        size_mb = f.stat().st_size / 1024 / 1024
        readable = "✓" if os.access(f, os.R_OK) else "✗"
        print(f"   {readable} {f.name} ({size_mb:.2f} MB)")

# Run diagnosis
import datetime

print("🔍 SAS FILE DIAGNOSTIC TOOL")
print("="*80)

# Diagnose the problematic file
diagnose_sas_file_detailed(pidmfin_path / "cisdepxn.sas7bdat")

# Also check the working file for comparison
print("\n" + "="*80)
print("COMPARISON WITH WORKING FILE")
print("="*80)
diagnose_sas_file_detailed(pidmfin_path / "cisdepd.sas7bdat")
