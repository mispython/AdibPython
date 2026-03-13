"""
EIIMLIQ3 - Islamic Bank Master Orchestrator: All 5 Liquidity Ratios
"""

import subprocess
import sys
from datetime import datetime
from calendar import monthrange
import os

print("=" * 70)
print("EIIMLIQ3 - ISLAMIC BANK LIQUIDITY MONITORING")
print("PART 3: CONCENTRATION OF FUNDING SOURCES (PIBB)")
print("=" * 70)

BNMTBL1_PATH = 'data/bnm1/BNMTBL1.parquet'
SCRIPT_DIR = '/mnt/user-data/outputs'

try:
    import polars as pl
    
    print("\nReading report date from BNMTBL1...")
    df_bnm = pl.read_parquet(BNMTBL1_PATH)
    reptdate = df_bnm['REPTDATE'][0]
    
    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d')
    
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptmon = str(reptdate.month).zfill(2)
    reptyear = str(reptdate.year)
    year = reptyear[-2:]
    mthend = str(day).zfill(2)
    rdate = reptdate.strftime('%d/%m/%y')
    
    print(f"\n  Report Date: {reptdate.strftime('%d %B %Y')}")
    print(f"  Week: {nowk}")
    print(f"  Month: {reptmon}")
    print(f"  Year: {reptyear}")
    print(f"  Month End Day: {mthend}")
    
except Exception as e:
    print(f"\n✗ ERROR reading BNMTBL1: {e}")
    print("Using default values...")
    reptmon = '12'
    reptyear = '2024'
    nowk = '4'
    year = '24'
    mthend = str(monthrange(int(reptyear), int(reptmon))[1]).zfill(2)
    reptdate = datetime(int(reptyear), int(reptmon), int(mthend))
    rdate = reptdate.strftime('%d/%m/%y')

print("\n" + "=" * 70)
print("EXECUTING LIQUIDITY RATIO PROGRAMS")
print("=" * 70)

programs = [
    ('EIILIQ301', 'Ratio 1: Adjusted Financing / Adjusted Deposits'),
    ('EIILIQ302', 'Ratio 2: Net Offshore Borrowing / Domestic Deposits'),
    ('EIILQ303', 'Ratio 3: Net Domestic IB / Domestic Deposits'),
    ('EIILQ304', 'Ratio 4: Net Overnight IB / Gross Domestic IB'),
    ('EIILQ305', 'Ratio 5: ST IB Borrowing / ST Total Funding')
]

results = []

for i, (program, description) in enumerate(programs, 1):
    print(f"\n[{i}/5] Executing {program}...")
    print(f"      {description}")
    print("-" * 70)
    
    script_path = f'{SCRIPT_DIR}/{program.lower()}.py'
    
    if not os.path.exists(script_path):
        print(f"  ✗ ERROR: Script not found: {script_path}")
        results.append((program, 'FAILED', 'Script not found'))
        continue
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            print(f"  ✓ {program} completed successfully")
            results.append((program, 'SUCCESS', ''))
            if result.stdout:
                for line in result.stdout.split('\n'):
                    if 'Ratio' in line or 'RM' in line or '✓' in line:
                        print(f"    {line}")
        else:
            print(f"  ✗ {program} failed with return code {result.returncode}")
            results.append((program, 'FAILED', result.stderr[:200]))
            if result.stderr:
                print(f"    Error: {result.stderr[:200]}")
    
    except subprocess.TimeoutExpired:
        print(f"  ✗ {program} timed out after 300 seconds")
        results.append((program, 'TIMEOUT', 'Execution exceeded 5 minutes'))
    
    except Exception as e:
        print(f"  ✗ {program} error: {str(e)[:200]}")
        results.append((program, 'ERROR', str(e)[:200]))

print("\n" + "=" * 70)
print("EXECUTION SUMMARY")
print("=" * 70)

success_count = sum(1 for _, status, _ in results if status == 'SUCCESS')
failed_count = len(results) - success_count

print(f"\nReport Date: {rdate}")
print(f"Week: WK{nowk}")
print(f"\nPrograms Executed: {len(results)}")
print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")

print("\nDetailed Results:")
print("-" * 70)
for program, status, error in results:
    status_symbol = "✓" if status == "SUCCESS" else "✗"
    print(f"  {status_symbol} {program:12s} : {status:8s}", end="")
    if error:
        print(f" - {error[:50]}")
    else:
        print()

if failed_count > 0:
    print("\n⚠ WARNING: Some programs failed. Check errors above.")
    print("=" * 70)
    sys.exit(1)
else:
    print("\n✓ ALL LIQUIDITY RATIOS COMPLETED SUCCESSFULLY")
    print("=" * 70)
    
    print("\nOutput Files:")
    print("  - ISLAMIC_FUNDING_RATIO.csv")
    print("  - ISLAMIC_OFFSHORE_RATIO.csv")
    print("  - ISLAMIC_INTERBANK_RATIO.csv")
    print("  - ISLAMIC_OVERNIGHT_RATIO.csv")
    print("  - ISLAMIC_SHORT_TERM_RATIO.csv")
    
    print("\n" + "=" * 70)
    print("EIIMLIQ3 - EXECUTION COMPLETE")
    print("=" * 70)
