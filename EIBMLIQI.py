"""
EIBMLIQI - Liquidity Framework Master Controller

Purpose: Orchestrate liquidity framework reporting
1. Set REPTDATE and week
2. Run deposit manipulation (DALWPBBD)
3. Run Islamic FISS report (EIBMRLFI)

Components:
- DALWPBBD: Process deposits with format mappings
- EIBMRLFI: Generate BNM Islamic liquidity framework report

This is the master controller for liquidity reporting.
"""

import subprocess
import polars as pl
from datetime import datetime
import os
import sys

# Directories
LOAN_DIR = 'data/loan/'
BNM_DIR = 'data/bnm/'

print("EIBMLIQI - Liquidity Framework Master Controller")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    # Determine week
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    # Format variables
    reptyear = str(reptdate.year)
    reptmon = f'{reptdate.month:02d}'
    reptday = f'{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Month: {reptmon}, Year: {reptyear}")
    
    # Save to BNM directory
    os.makedirs(BNM_DIR, exist_ok=True)
    df_reptdate.write_parquet(f'{BNM_DIR}REPTDATE.parquet')
    print(f"✓ Saved BNM.REPTDATE")

except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    sys.exit(1)

print("=" * 60)

# Set environment variables
env = os.environ.copy()
env.update({
    'NOWK': nowk,
    'REPTYEAR': reptyear,
    'REPTMON': reptmon,
    'REPTDAY': reptday,
    'RDATE': rdate
})

# Run DALWPBBD (Deposit Manipulation)
print("\n1. Running DALWPBBD (Deposit Manipulation)...")
print("-" * 60)

try:
    result = subprocess.run(
        ['python', 'dalwpbbd.py'],
        env=env,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✓ DALWPBBD completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"✗ DALWPBBD failed with code {result.returncode}")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)

except Exception as e:
    print(f"✗ Error running DALWPBBD: {e}")
    sys.exit(1)

print("=" * 60)

# Run EIBMRLFI (Islamic FISS Report)
print("\n2. Running EIBMRLFI (Islamic FISS Report)...")
print("-" * 60)

try:
    result = subprocess.run(
        ['python', 'eimbrlfi.py'],
        env=env,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✓ EIBMRLFI completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"✗ EIBMRLFI failed with code {result.returncode}")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)

except Exception as e:
    print(f"✗ Error running EIBMRLFI: {e}")
    sys.exit(1)

print("=" * 60)
print("\n✓ EIBMLIQI Complete!")
print("=" * 60)
print(f"\nLiquidity Framework Reporting Summary:")
print(f"  Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"  Week: {nowk}")
print(f"\nPrograms Executed:")
print(f"  1. DALWPBBD - Deposit manipulation")
print(f"     - Processed savings and current accounts")
print(f"     - Applied format mappings (PBBDPFMT)")
print(f"     - Generated BNM deposit datasets")
print(f"\n  2. EIBMRLFI - Islamic FISS report")
print(f"     - Generated BNM liquidity framework")
print(f"     - Maturity profile by bucket")
print(f"     - Customer type analysis")
print(f"\nOutputs:")
print(f"  - BNM.SAVG{reptmon}{nowk}")
print(f"  - BNM.CURN{reptmon}{nowk}")
print(f"  - BNM.FCY{reptmon}{nowk}")
print(f"  - BNM.DEPT{reptmon}{nowk}")
print(f"  - FISS_ISLAMIC.csv")

sys.exit(0)
