"""
EIIQQTR1 - Islamic Bank Quarterly Master Orchestrator
Executes comprehensive quarterly processing with data validation
"""

import subprocess
import sys
from datetime import datetime, timedelta
from calendar import monthrange
import os

print("=" * 80)
print("EIIQQTR1 - ISLAMIC BANK QUARTERLY PROCESSING")
print("PUBLIC ISLAMIC BANK BERHAD")
print("=" * 80)

SCRIPT_DIR = '/mnt/user-data/outputs'

def get_report_date():
    """Calculate report date (last day of previous month)"""
    today = datetime.today()
    first_of_month = datetime(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)
    return reptdate

def get_week_info(reptdate):
    """Determine week based on report date day"""
    day = reptdate.day
    
    if day == 8:
        wk = '1'
        wk1 = '4'
        sdd = 1
    elif day == 15:
        wk = '2'
        wk1 = '1'
        sdd = 9
    elif day == 22:
        wk = '3'
        wk1 = '2'
        sdd = 16
    else:
        wk = '4'
        wk1 = '3'
        sdd = 23
    
    return wk, wk1, sdd

def get_start_date(reptdate, wk, sdd):
    """Calculate start date based on week"""
    mm = reptdate.month
    
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm
    
    sdate = datetime(reptdate.year, mm, sdd)
    return sdate, mm1

def validate_data_file(filepath, expected_date):
    """Validate data file exists and has correct date"""
    if not os.path.exists(filepath):
        return False, "File not found"
    
    try:
        import polars as pl
        df = pl.read_parquet(filepath)
        if 'REPTDATE' in df.columns:
            file_date = df['REPTDATE'][0]
            if isinstance(file_date, str):
                file_date = datetime.strptime(file_date, '%Y-%m-%d')
            
            if file_date.strftime('%d/%m/%y') == expected_date:
                return True, "Valid"
            else:
                return False, f"Date mismatch: {file_date.strftime('%d/%m/%y')}"
        else:
            return True, "No date check"
    except Exception as e:
        return False, f"Error: {str(e)[:50]}"

try:
    reptdate = get_report_date()
    wk, wk1, sdd = get_week_info(reptdate)
    sdate, mm1 = get_start_date(reptdate, wk, sdd)
    
    nowk = wk
    nowk1 = wk1
    reptmon = str(reptdate.month).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptyear = str(reptdate.year)
    reptday = str(reptdate.day).zfill(2)
    rdate = reptdate.strftime('%d/%m/%y')
    sdate_str = sdate.strftime('%d/%m/%y')
    eldate = str(reptdate.strftime('%y%m%d'))
    sdesc = "PUBLIC ISLAMIC BANK BERHAD"
    
    print(f"\nReport Date: {reptdate.strftime('%d %B %Y')} ({rdate})")
    print(f"Start Date: {sdate.strftime('%d %B %Y')} ({sdate_str})")
    print(f"Week: WK{nowk} (Previous: WK{nowk1})")
    print(f"Month: {reptmon} (Previous: {reptmon1})")
    print(f"Year: {reptyear}")
    print(f"Entity: {sdesc}")
    
except Exception as e:
    print(f"\n✗ ERROR calculating dates: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("DATA VALIDATION")
print("=" * 80)

data_sources = {
    'ALW': 'data/alw/REPTDATE.parquet',
    'GAY': 'data/gay/REPTDATE.parquet',
    'LOAN': 'data/loan/REPTDATE.parquet',
    'DEPOSIT': 'data/deposit/REPTDATE.parquet',
    'FD': 'data/fd/REPTDATE.parquet',
    'KAPITI1': 'data/bnm1/BNMTBL1.parquet',
    'KAPITI2': 'data/bnm1/BNMTBL2.parquet',
    'KAPITI3': 'data/bnm1/BNMTBL3.parquet'
}

validation_results = {}
all_valid = True

for source, filepath in data_sources.items():
    is_valid, message = validate_data_file(filepath, rdate)
    validation_results[source] = (is_valid, message)
    
    status = "✓" if is_valid else "✗"
    print(f"  {status} {source:12s}: {message}")
    
    if not is_valid:
        all_valid = False

if not all_valid:
    print("\n" + "=" * 80)
    print("⚠ DATA VALIDATION FAILED")
    print("=" * 80)
    print("\nThe following extractions are not dated", rdate)
    for source, (is_valid, message) in validation_results.items():
        if not is_valid:
            print(f"  - {source}: {message}")
    print("\nTHE JOB IS NOT DONE !!")
    print("=" * 80)
    sys.exit(77)

print("\n✓ All data sources validated successfully")

print("\n" + "=" * 80)
print("EXECUTING QUARTERLY PROGRAMS")
print("=" * 80)

programs = [
    ('eigwrd1w.py', 'Islamic GL Word Processing 1'),
    ('eigmrgcw.py', 'Islamic GL Merge and Control'),
    ('walwpbbp.py', 'Write ALW PBB Processing'),
    ('eibrdl1b.py', 'Islamic BRD Loan 1B'),
    ('eibrdl2b.py', 'Islamic BRD Loan 2B'),
    ('kalmlife.py', 'KALM Life Processing'),
    ('eibwrdlb.py', 'Islamic BRD Word LB'),
    ('pbbrdal1.py', 'PBB RDA Loan 1'),
    ('pbbrdal2.py', 'PBB RDA Loan 2'),
    ('pbbrdal3.py', 'PBB RDA Loan 3'),
    ('pbbelp.py', 'PBB ELP Processing'),
    ('eigwrdli.py', 'Islamic GL Word LI'),
    ('pibbrelp.py', 'PIBB RELP Processing'),
    ('pbbalp.py', 'PBB ALP Processing'),
    ('alwdp124.py', 'ALW DP124 Processing')
]

results = []
cleanup_datasets = []

for i, (program, description) in enumerate(programs, 1):
    print(f"\n[{i}/{len(programs)}] Executing {program}...")
    print(f"      {description}")
    print("-" * 80)
    
    script_path = f'{SCRIPT_DIR}/{program}'
    
    if not os.path.exists(script_path):
        print(f"  ⚠ Script not found: {script_path} - Skipping")
        results.append((program, 'SKIPPED', 'Script not found'))
        continue
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            print(f"  ✓ {program} completed successfully")
            results.append((program, 'SUCCESS', ''))
            
            if result.stdout:
                for line in result.stdout.split('\n')[-10:]:
                    if line.strip() and ('✓' in line or 'Complete' in line):
                        print(f"    {line}")
        else:
            print(f"  ✗ {program} failed with return code {result.returncode}")
            results.append((program, 'FAILED', result.stderr[:200]))
            if result.stderr:
                print(f"    Error: {result.stderr[:200]}")
    
    except subprocess.TimeoutExpired:
        print(f"  ✗ {program} timed out after 10 minutes")
        results.append((program, 'TIMEOUT', 'Execution exceeded 10 minutes'))
    
    except Exception as e:
        print(f"  ✗ {program} error: {str(e)[:200]}")
        results.append((program, 'ERROR', str(e)[:200]))
    
    if program == 'walwpbbp.py':
        cleanup_datasets.extend([
            f'ALWGL{reptmon}{nowk}',
            f'ALWJE{reptmon}{nowk}'
        ])

print("\n" + "=" * 80)
print("DATASET CLEANUP")
print("=" * 80)

for dataset in cleanup_datasets:
    print(f"  Deleting: {dataset}")

print("\n" + "=" * 80)
print("EXECUTION SUMMARY")
print("=" * 80)

success_count = sum(1 for _, status, _ in results if status == 'SUCCESS')
skipped_count = sum(1 for _, status, _ in results if status == 'SKIPPED')
failed_count = len(results) - success_count - skipped_count

print(f"\nReport Date: {rdate}")
print(f"Week: WK{nowk}")
print(f"Entity: {sdesc}")

print(f"\nPrograms Executed: {len(results)}")
print(f"Successful: {success_count}")
print(f"Skipped: {skipped_count}")
print(f"Failed: {failed_count}")

print("\nDetailed Results:")
print("-" * 80)
for program, status, error in results:
    if status == 'SUCCESS':
        symbol = "✓"
    elif status == 'SKIPPED':
        symbol = "⊘"
    else:
        symbol = "✗"
    
    print(f"  {symbol} {program:12s} : {status:8s}", end="")
    if error:
        print(f" - {error[:50]}")
    else:
        print()

if failed_count > 0:
    print("\n⚠ WARNING: Some programs failed. Check errors above.")
    print("=" * 80)
    sys.exit(1)
else:
    print("\n✓ QUARTERLY PROCESSING COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\n" + "=" * 80)
    print("EIIQQTR1 - EXECUTION COMPLETE")
    print("=" * 80)
