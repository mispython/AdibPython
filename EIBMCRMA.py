"""
EIBMCRMA - CRM ATM Data Extract

Purpose: Extract customer contact data from equity/TF file for CRM system

Input: EQUTFX file
Output: CRM_ATM.parquet (sorted by ACCTNO)

Fields:
- ACCTNO: Account number
- HOMEPH: Home phone
- BUSSPH: Business phone  
- TINOWN: TIN owner flag
- DEPTNO: Department number
- PURPOSE: Purpose code
"""

import polars as pl
import os

# Directories
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/eq1/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMCRMA - CRM ATM Data Extract")

# Read EQUTFX file
try:
    data = []
    with open(f'{INPUT_DIR}EQUTFX', 'r') as f:
        for line in f:
            data.append({
                'ACCTNO': int(line[1:11]) if line[1:11].strip() else 0,
                'HOMEPH': line[12:23].strip(),
                'BUSSPH': line[24:35].strip(),
                'TINOWN': int(line[36:37]) if line[36:37].strip() else 0,
                'DEPTNO': int(line[38:43]) if line[38:43].strip() else 0,
                'PURPOSE': line[44:45].strip()
            })
    
    # Create DataFrame and sort by ACCTNO
    df = pl.DataFrame(data).sort('ACCTNO')
    
    # Save output
    df.write_parquet(f'{OUTPUT_DIR}CRM_ATM.parquet')
    
    print(f"✓ Extracted {len(df):,} records")
    print(f"✓ Saved to CRM_ATM.parquet")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)
