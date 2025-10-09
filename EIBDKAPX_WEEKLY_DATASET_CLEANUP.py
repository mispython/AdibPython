#!/usr/bin/env python3
"""
EIBDKAPX - Weekly Dataset Cleanup
1:1 conversion from SAS to Python
Deletes weekly datasets on specific days of the month (1st, 9th, 16th, 23rd)
for BNM reporting periods
"""

import duckdb
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# Initialize paths
OUTPUT_DIR = Path("output")
BNMK_DIR = OUTPUT_DIR / "bnmk"
BNMKI_DIR = OUTPUT_DIR / "bnmki"

# Create directories
BNMK_DIR.mkdir(parents=True, exist_ok=True)
BNMKI_DIR.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
con = duckdb.connect(":memory:")

print("Processing EIBDKAPX - Weekly Dataset Cleanup")

# ============================================================================
# DATA REPTDATE - Calculate report date and week number
# ============================================================================
SXDATE = datetime.today().date()
MM = SXDATE.month
TDD = SXDATE.day

# Determine week number based on day of month
WK1 = None
if TDD == 1:
    WK1 = '1'
elif TDD == 9:
    WK1 = '2'
elif TDD == 16:
    WK1 = '3'
elif TDD == 23:
    WK1 = '4'

# Create macro variables
WK = WK1 if WK1 else ''
REPTMON = str(MM).zfill(2)
REPTDAY = str(TDD).zfill(2)

print(f"Current Date: {SXDATE}")
print(f"Report Month: {REPTMON}")
print(f"Report Day: {REPTDAY}")
print(f"Week Number: {WK if WK else 'N/A (not a reporting day)'}")

# ============================================================================
# MACRO APPEND - Delete datasets on specific reporting days
# Days: 01, 09, 16, 23 (start of each weekly reporting period)
# ============================================================================

if REPTDAY in ["01", "09", "16", "23"]:
    print(f"\nReporting day detected (Day {REPTDAY}) - Deleting weekly datasets...")
    
    # Define datasets to delete
    datasets_to_delete = [
        f"TBL1{REPTMON}{WK}",
        f"REP2{REPTMON}{WK}",
        f"REP3{REPTMON}{WK}",
        f"REP4{REPTMON}{WK}"
    ]
    
    print("\n" + "="*80)
    print("PROC DATASETS LIB=BNMK - Deleting datasets")
    print("="*80)
    
    # Delete from BNMK directory
    deleted_bnmk = []
    for dataset in datasets_to_delete:
        parquet_file = BNMK_DIR / f"{dataset}.parquet"
        csv_file = BNMK_DIR / f"{dataset}.csv"
        
        if parquet_file.exists():
            parquet_file.unlink()
            deleted_bnmk.append(f"{dataset}.parquet")
            print(f"  Deleted: {parquet_file}")
        
        if csv_file.exists():
            csv_file.unlink()
            deleted_bnmk.append(f"{dataset}.csv")
            print(f"  Deleted: {csv_file}")
    
    if not deleted_bnmk:
        print("  No files found to delete in BNMK")
    
    print("\n" + "="*80)
    print("PROC DATASETS LIB=BNMKI - Deleting datasets")
    print("="*80)
    
    # Delete from BNMKI directory (Islamic)
    deleted_bnmki = []
    for dataset in datasets_to_delete:
        parquet_file = BNMKI_DIR / f"{dataset}.parquet"
        csv_file = BNMKI_DIR / f"{dataset}.csv"
        
        if parquet_file.exists():
            parquet_file.unlink()
            deleted_bnmki.append(f"{dataset}.parquet")
            print(f"  Deleted: {parquet_file}")
        
        if csv_file.exists():
            csv_file.unlink()
            deleted_bnmki.append(f"{dataset}.csv")
            print(f"  Deleted: {csv_file}")
    
    if not deleted_bnmki:
        print("  No files found to delete in BNMKI")
    
    # Summary
    print("\n" + "="*80)
    print("DELETION SUMMARY")
    print("="*80)
    print(f"Reporting Period: Month {REPTMON}, Week {WK}")
    print(f"BNMK files deleted: {len(deleted_bnmk)}")
    print(f"BNMKI files deleted: {len(deleted_bnmki)}")
    print("="*80)
    
    if deleted_bnmk or deleted_bnmki:
        print("\nDatasets deleted:")
        for dataset in datasets_to_delete:
            print(f"  - {dataset}")
    else:
        print("\nNo existing datasets found to delete")
        print("(This is normal for the first run of a new reporting period)")

else:
    print(f"\nDay {REPTDAY} is not a reporting day")
    print("No datasets will be deleted")
    print("\nReporting days are: 01, 09, 16, 23")
    print("Current datasets will remain intact")

# ============================================================================
# Additional Information
# ============================================================================
print("\n" + "="*80)
print("WEEKLY REPORTING SCHEDULE")
print("="*80)
print("Week 1: Day 01-08")
print("Week 2: Day 09-15")
print("Week 3: Day 16-22")
print("Week 4: Day 23-End of Month")
print("="*80)
print("\nDatasets affected:")
print("  - TBL1{MONTH}{WEEK} (Table 1)")
print("  - REP2{MONTH}{WEEK} (Report 2)")
print("  - REP3{MONTH}{WEEK} (Report 3)")
print("  - REP4{MONTH}{WEEK} (Report 4)")
print("\nDirectories:")
print(f"  - BNMK:  {BNMK_DIR}  (Conventional Banking)")
print(f"  - BNMKI: {BNMKI_DIR} (Islamic Banking)")

# Close connection
con.close()
print("\nProcessing complete!")
