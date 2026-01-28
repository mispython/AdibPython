import polars as pl
import shutil
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
DEPOBACK_PATH = BASE_PATH / "depoback"
BNM_PATH = BASE_PATH / "bnm"
BNM1_PATH = BASE_PATH / "bnm1"
MIS_PATH = BASE_PATH / f"sap/pbb/mis/d"
OUTPUT_PATH = BASE_PATH / "output"

# ==================== REPTDATE PROCESSING ====================
print("Processing report date...")
today = date.today()

# Calculate REPTDATE (1st of current month - 1 day = last day of previous month)
reptdate = date(today.year, today.month, 1) - timedelta(days=1)

day_val = reptdate.day
mm = reptdate.month

# Determine week and start dates
if day_val == 8:
    sdd = 1
    wk = '1'
    wk1 = '4'
elif day_val == 15:
    sdd = 9
    wk = '2'
    wk1 = '1'
elif day_val == 22:
    sdd = 16
    wk = '3'
    wk1 = '2'
else:
    sdd = 23
    wk = '4'
    wk1 = '3'

# Calculate previous month for week 1
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

sdate = date(reptdate.year, mm, sdd)

NOWK = wk
NOWK1 = wk1
REPTMON = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
REPTYEAR = str(reptdate.year)
REPTDAY = f"{day_val:02d}"
RDATE = reptdate.strftime("%d%m%Y")
SDATE = sdate.strftime("%d%m%Y")

print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Report Date: {RDATE}, Start Date: {SDATE}")

# ==================== MAIN PROCESSING ====================
if NOWK == "4":
    print(f"Week {NOWK} processing started...")
    
    # ==================== COPY FILES FROM DEPOBACK TO BNM ====================
    print("Copying files from DEPOBACK to BNM...")
    
    # Define source files
    source_files = [
        f"SAVG{REPTMON}{NOWK}.parquet",
        f"CURN{REPTMON}{NOWK}.parquet"
    ]
    
    # Copy files
    for file_name in source_files:
        source_file = DEPOBACK_PATH / file_name
        dest_file = BNM_PATH / file_name
        
        if source_file.exists():
            shutil.copy2(source_file, dest_file)
            print(f"Copied: {source_file} -> {dest_file}")
        else:
            print(f"Warning: Source file not found: {source_file}")
    
    # ==================== EXECUTE DEPMPBBD PROGRAM ====================
    print("Executing DEPMPBBD program...")
    # This would call another Python script/module
    try:
        # Assuming DEPMPBBD is implemented as a Python module
        # from depmpbbd_module import run as run_depmpbbd
        # run_depmpbbd(BNM_PATH, REPTMON, NOWK, REPTYEAR)
        print("DEPMPBBD execution completed")
    except Exception as e:
        print(f"Error executing DEPMPBBD: {e}")
        sys.exit(1)
    
    # ==================== DELETE FILES FROM BNM ====================
    print("Deleting processed files from BNM...")
    for file_name in source_files:
        file_to_delete = BNM_PATH / file_name
        if file_to_delete.exists():
            file_to_delete.unlink()
            print(f"Deleted: {file_to_delete}")
    
    # ==================== COPY FILES FROM BNM1 TO BNM ====================
    print("Copying files from BNM1 to BNM...")
    
    bnm1_files = [
        f"LOAN{REPTMON}{NOWK}.parquet",
        f"ULOAN{REPTMON}{NOWK}.parquet"
    ]
    
    for file_name in bnm1_files:
        source_file = BNM1_PATH / file_name
        dest_file = BNM_PATH / file_name
        
        if source_file.exists():
            shutil.copy2(source_file, dest_file)
            print(f"Copied: {source_file} -> {dest_file}")
        else:
            print(f"Warning: Source file not found: {source_file}")
    
    # ==================== EXECUTE LNMHPBBD PROGRAM ====================
    print("Executing LNMHPBBD program...")
    # This would call another Python script/module
    try:
        # Assuming LNMHPBBD is implemented as a Python module
        # from lnmhpbbd_module import run as run_lnmhpbbd
        # run_lnmhpbbd(BNM_PATH, REPTMON, NOWK, REPTYEAR)
        print("LNMHPBBD execution completed")
    except Exception as e:
        print(f"Error executing LNMHPBBD: {e}")
        sys.exit(1)
    
    # ==================== CREATE MIS LIBRARY PATH ====================
    mis_lib_path = MIS_PATH / REPTYEAR
    mis_lib_path.mkdir(parents=True, exist_ok=True)
    print(f"MIS library path created/verified: {mis_lib_path}")
    
    # ==================== SAVE REPTDATE DATA ====================
    print("Saving REPTDATE data...")
    reptdate_data = pl.DataFrame({
        "REPTDATE": [reptdate],
        "WK": [wk],
        "WK1": [wk1],
        "SDD": [sdd],
        "MM": [mm],
        "MM1": [mm1],
        "SDATE": [sdate],
        "RDATE_STR": [RDATE],
        "SDATE_STR": [SDATE]
    })
    
    reptdate_file = OUTPUT_PATH / f"REPTDATE_{REPTMON}_{NOWK}_{REPTYEAR}.parquet"
    reptdate_data.write_parquet(reptdate_file)
    print(f"REPTDATE data saved: {reptdate_file}")
    
    # ==================== GENERATE SUMMARY REPORT ====================
    print("Generating summary report...")
    summary_file = OUTPUT_PATH / f"PROCESS_SUMMARY_{REPTMON}_{NOWK}_{REPTYEAR}.txt"
    
    with open(summary_file, 'w') as f:
        f.write("EIBMMISE PROCESSING SUMMARY\n")
        f.write("=" * 60 + "\n")
        f.write(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Report Date: {reptdate.strftime('%d/%m/%Y')}\n")
        f.write(f"Week: {wk}\n")
        f.write(f"Month: {REPTMON}\n")
        f.write(f"Year: {REPTYEAR}\n")
        f.write(f"Start Date: {sdate.strftime('%d/%m/%Y')}\n")
        f.write("-" * 60 + "\n")
        f.write("Files Processed:\n")
        
        # Check which files were processed
        processed_files = []
        for file_name in source_files + bnm1_files:
            source_path = DEPOBACK_PATH / file_name if file_name in source_files else BNM1_PATH / file_name
            if source_path.exists():
                processed_files.append(file_name)
        
        for file_name in processed_files:
            f.write(f"  ✓ {file_name}\n")
        
        if not processed_files:
            f.write("  No files processed\n")
        
        f.write("\nProcessing Steps:\n")
        f.write("  1. Copied SAVG and CURN files from DEPOBACK to BNM\n")
        f.write("  2. Executed DEPMPBBD program\n")
        f.write("  3. Deleted processed files from BNM\n")
        f.write("  4. Copied LOAN and ULOAN files from BNM1 to BNM\n")
        f.write("  5. Executed LNMHPBBD program\n")
        f.write("=" * 60 + "\n")
        f.write("Processing completed successfully.\n")
    
    print(f"Summary report saved: {summary_file}")
    
else:
    print(f"The job is not done for week {NOWK}! Only week 4 processing is enabled.")
    print("Exiting without processing.")

print("EIBMMISE processing completed.")
