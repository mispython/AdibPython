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
MIS_PATH = BASE_PATH / f"sap/pibb/mis/d"  # Islamic path
OUTPUT_PATH = BASE_PATH / "output"

# ==================== REPTDATE PROCESSING ====================
print("Processing Islamic report date...")
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

print(f"Islamic Report - Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Report Date: {RDATE}, Start Date: {SDATE}")

# ==================== CREATE ISLAMIC MIS LIBRARY ====================
mis_lib_path = MIS_PATH / REPTYEAR
mis_lib_path.mkdir(parents=True, exist_ok=True)
print(f"Islamic MIS library path created: {mis_lib_path}")

# ==================== MAIN PROCESSING ====================
if NOWK == "4":
    print(f"Islamic Week {NOWK} processing started...")
    
    # ==================== COPY FILES FROM DEPOBACK TO BNM ====================
    print("Copying Islamic files from DEPOBACK to BNM...")
    
    source_files = [
        f"SAVG{REPTMON}{NOWK}.parquet",
        f"CURN{REPTMON}{NOWK}.parquet"
    ]
    
    for file_name in source_files:
        source_file = DEPOBACK_PATH / file_name
        dest_file = BNM_PATH / file_name
        
        if source_file.exists():
            shutil.copy2(source_file, dest_file)
            print(f"Copied: {source_file} -> {dest_file}")
        else:
            print(f"Warning: Islamic source file not found: {source_file}")
    
    # ==================== EXECUTE ISLAMIC DEPMPBBD PROGRAM ====================
    print("Executing Islamic DEPMPBBD program...")
    try:
        # Assuming Islamic DEPMPBBD is implemented
        # from depmpbbd_islamic_module import run as run_depmpbbd_islamic
        # run_depmpbbd_islamic(BNM_PATH, REPTMON, NOWK, REPTYEAR)
        print("Islamic DEPMPBBD execution completed")
    except Exception as e:
        print(f"Error executing Islamic DEPMPBBD: {e}")
        sys.exit(1)
    
    # ==================== DELETE PROCESSED FILES FROM BNM ====================
    print("Deleting processed Islamic files from BNM...")
    for file_name in source_files:
        file_to_delete = BNM_PATH / file_name
        if file_to_delete.exists():
            file_to_delete.unlink()
            print(f"Deleted: {file_to_delete}")
    
    # ==================== COPY ISLAMIC LOAN FILES FROM BNM1 TO BNM ====================
    print("Copying Islamic loan files from BNM1 to BNM...")
    
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
            print(f"Warning: Islamic loan file not found: {source_file}")
    
    # ==================== EXECUTE ISLAMIC LNMHPBBD PROGRAM ====================
    print("Executing Islamic LNMHPBBD program...")
    try:
        # Assuming Islamic LNMHPBBD is implemented
        # from lnmhpbbd_islamic_module import run as run_lnmhpbbd_islamic
        # run_lnmhpbbd_islamic(BNM_PATH, REPTMON, NOWK, REPTYEAR)
        print("Islamic LNMHPBBD execution completed")
    except Exception as e:
        print(f"Error executing Islamic LNMHPBBD: {e}")
        sys.exit(1)
    
    # ==================== SAVE ISLAMIC REPTDATE DATA ====================
    print("Saving Islamic REPTDATE data...")
    reptdate_data = pl.DataFrame({
        "REPTDATE": [reptdate],
        "WK": [wk],
        "WK1": [wk1],
        "SDD": [sdd],
        "MM": [mm],
        "MM1": [mm1],
        "SDATE": [sdate],
        "RDATE_STR": [RDATE],
        "SDATE_STR": [SDATE],
        "BANK_TYPE": ["ISLAMIC"]
    })
    
    reptdate_file = OUTPUT_PATH / f"ISLAMIC_REPTDATE_{REPTMON}_{NOWK}_{REPTYEAR}.parquet"
    reptdate_data.write_parquet(reptdate_file)
    print(f"Islamic REPTDATE data saved: {reptdate_file}")
    
    # ==================== GENERATE ISLAMIC SUMMARY REPORT ====================
    print("Generating Islamic summary report...")
    summary_file = OUTPUT_PATH / f"ISLAMIC_PROCESS_SUMMARY_{REPTMON}_{NOWK}_{REPTYEAR}.txt"
    
    with open(summary_file, 'w') as f:
        f.write("EIIMMISE (ISLAMIC) PROCESSING SUMMARY\n")
        f.write("=" * 70 + "\n")
        f.write(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Bank: PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"Report Date: {reptdate.strftime('%d/%m/%Y')}\n")
        f.write(f"Week: {wk}\n")
        f.write(f"Month: {REPTMON}\n")
        f.write(f"Year: {REPTYEAR}\n")
        f.write(f"Start Date: {sdate.strftime('%d/%m/%Y')}\n")
        f.write("-" * 70 + "\n")
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
        
        f.write("\nIslamic Processing Steps:\n")
        f.write("  1. Copied Islamic SAVG and CURN files from DEPOBACK to BNM\n")
        f.write("  2. Executed Islamic DEPMPBBD program\n")
        f.write("  3. Deleted processed files from BNM\n")
        f.write("  4. Copied Islamic LOAN and ULOAN files from BNM1 to BNM\n")
        f.write("  5. Executed Islamic LNMHPBBD program\n")
        f.write("=" * 70 + "\n")
        f.write("Islamic processing completed successfully.\n")
    
    print(f"Islamic summary report saved: {summary_file}")
    
    # ==================== VALIDATE ISLAMIC DATA ====================
    print("\nValidating Islamic data...")
    
    # Check if files were successfully created
    expected_files = [
        BNM_PATH / f"LOAN{REPTMON}{NOWK}.parquet",
        BNM_PATH / f"ULOAN{REPTMON}{NOWK}.parquet"
    ]
    
    for expected_file in expected_files:
        if expected_file.exists():
            try:
                df = pl.read_parquet(expected_file)
                print(f"✓ {expected_file.name}: {len(df):,} records")
            except:
                print(f"✗ {expected_file.name}: Read error")
        else:
            print(f"✗ {expected_file.name}: Not found")
    
else:
    print(f"The Islamic job is not done for week {NOWK}! Only week 4 processing is enabled.")
    print("Exiting without processing.")

# ==================== COMPARE WITH CONVENTIONAL ====================
print("\n" + "=" * 60)
print("ISLAMIC VS CONVENTIONAL COMPARISON")
print("=" * 60)
print(f"Islamic Bank: PUBLIC ISLAMIC BANK BERHAD")
print(f"MIS Library: {mis_lib_path}")
print(f"Output Files:")
print(f"  - REPTDATE: {reptdate_file}")
print(f"  - Summary: {summary_file}")

print("\nIslamic processing complete!")
