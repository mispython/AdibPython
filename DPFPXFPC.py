import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP01"
OUTPUT_DIR = BASE_DIR / "output/EIBWHP01"

JOB_NAME = "EIBWHP01"

# Report date = yesterday (removed external reptdate input)
REPT_DATE = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

# Generate REPTMON and NOWK based on current date
REPTMON = (datetime.now() - timedelta(days=1)).strftime("%Y%m")  # YYYYMM format
NOWK = (datetime.now() - timedelta(days=1)).strftime("%W")  # Week number

print(f"[CONFIG] REPTMON: {REPTMON}, NOWK: {NOWK}")

# Input datasets (sas7bdat format) - with dynamic naming
INPUT_DATASETS = {
    "BNM": INPUT_DIR / f"loan{REPTMON}{NOWK}.sas7bdat",
    "LOAN": INPUT_DIR / "lnnote.sas7bdat"
}

OUTPUT_DATASET = OUTPUT_DIR / "EIBWHP01.txt"

# =====================================================
# IMPORT EXISTING FORMAT MODULE
# =====================================================

try:
    import PBBLNFMT
except ImportError:
    print("[WARN] PBBLNFMT.py not found in PYTHONPATH. Proceeding without format module.")
    PBBLNFMT = None

# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def determine_reptmon_nowk():
    """
    Determine REPTMON and NOWK values with fallback options.
    Returns tuple (reptmon, nowk)
    """
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    # REPTMON: YYYYMM format
    reptmon = yesterday.strftime("%Y%m")
    
    # NOWK: Week number (various formats possible)
    # Option 1: ISO week number (01-53)
    nowk_iso = yesterday.strftime("%V")
    
    # Option 2: Simple week number (0-53, where Monday is first day of week)
    nowk_simple = yesterday.strftime("%W")
    
    # Option 3: SAS week number (1-53, where week starts on Sunday)
    # You might need to calculate this differently based on your SAS logic
    
    # For now, using ISO week number
    nowk = nowk_iso
    
    # Log the determined values
    print(f"[DATE] Report date: {REPT_DATE}")
    print(f"[DATE] REPTMON: {reptmon}")
    print(f"[DATE] NOWK: {nowk}")
    
    return reptmon, nowk


# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(dataset_path):
    """
    Simulates DISP=(MOD,DELETE,DELETE)
    Delete dataset if exists.
    """
    if dataset_path.exists():
        dataset_path.unlink()
        print(f"[DELETE] Removed existing dataset: {dataset_path}")
    else:
        print(f"[DELETE] Dataset not found (OK): {dataset_path}")


def disp_new(dataset_path):
    """
    Simulates DISP=(NEW,CATLG,DELETE)
    - Must not exist before run
    """
    if dataset_path.exists():
        raise FileExistsError(
            f"[DISP ERROR] Dataset already exists: {dataset_path}"
        )


def disp_shr(dataset_path):
    """
    Simulates DISP=SHR
    - Must exist
    """
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"[DISP ERROR] Required input dataset missing: {dataset_path}"
        )


# =====================================================
# SAS7BDAT READER (Direct pyreadstat)
# =====================================================

def read_sas_dataset(dataset_path):
    """
    Reads a .sas7bdat file directly using pyreadstat.
    Returns (pandas.DataFrame, metadata)
    """
    try:
        import pyreadstat
    except ImportError:
        raise ImportError(
            "pyreadstat is required to read .sas7bdat files. "
            "Install it via: pip install pyreadstat"
        )
    
    print(f"[READ] Reading {dataset_path.name}...")
    start_time = datetime.now()
    
    # Direct read using pyreadstat
    df, meta = pyreadstat.read_sas7bdat(str(dataset_path))
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"[READ] Loaded {len(df)} records from {dataset_path.name} in {elapsed:.2f} seconds")
    
    return df, meta


# =====================================================
# FILE WRITER (Removed LRECL enforcement)
# =====================================================

def write_file(path, records):
    """
    Writes output file without fixed block formatting.
    Accepts list of strings.
    """
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(record + "\n")
    
    print(f"[WRITE] Output dataset created: {path}")


# =====================================================
# BUSINESS LOGIC (SAS Migration)
# =====================================================

def execute_sas_program():
    """
    Replaces EXEC SAS609 step.
    Reads input SAS datasets, applies business logic,
    and returns list of output text records.
    """
    print("[EXEC] Starting SAS logic replacement...")

    # 1. Load input datasets
    print(f"[INFO] Loading BNM dataset: {INPUT_DATASETS['BNM']}")
    bnm_df, bnm_meta = read_sas_dataset(INPUT_DATASETS["BNM"])
    
    print(f"[INFO] Loading LOAN dataset: {INPUT_DATASETS['LOAN']}")
    loan_df, loan_meta = read_sas_dataset(INPUT_DATASETS["LOAN"])

    # 2. Apply PBBLNFMT formats if available
    if PBBLNFMT is not None:
        print("[FMT] Applying PBBLNFMT formats...")
        # Example: apply custom formats via PBBLNFMT module
        # Adjust based on actual PBBLNFMT.py interface
        if hasattr(PBBLNFMT, 'apply_formats'):
            bnm_df = PBBLNFMT.apply_formats(bnm_df)
            loan_df = PBBLNFMT.apply_formats(loan_df)
        elif hasattr(PBBLNFMT, 'format_dict'):
            # If PBBLNFMT contains format mappings
            for col, fmt in getattr(PBBLNFMT, 'format_dict', {}).items():
                if col in bnm_df.columns:
                    bnm_df[col] = bnm_df[col].map(fmt)
                if col in loan_df.columns:
                    loan_df[col] = loan_df[col].map(fmt)

    # 3. Build output records (placeholder logic)
    # Replace this section with actual migrated SAS business logic
    output_records = []
    output_records.append(f"EIBWHP01 REPORT GENERATED {REPT_DATE}")
    output_records.append(f"REPTMON: {REPTMON}, NOWK: {NOWK}")
    output_records.append(f"{'='*80}")
    output_records.append(f"BNM RECORDS: {len(bnm_df):>10}")
    output_records.append(f"LOAN RECORDS: {len(loan_df):>10}")
    output_records.append(f"REPORT DATE: {REPT_DATE}")
    output_records.append(f"{'='*80}")

    # TODO: Add actual data processing logic here
    # Example: iterate over merged/joined data and format each record
    # for idx, row in merged_df.iterrows():
    #     output_records.append(format_record(row))

    return output_records


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    global REPTMON, NOWK  # Allow modification of globals
    
    print(f"========== START JOB {JOB_NAME} ==========")
    
    # Determine REPTMON and NOWK
    REPTMON, NOWK = determine_reptmon_nowk()
    
    # Update INPUT_DATASETS with actual values
    INPUT_DATASETS["BNM"] = INPUT_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"
    
    print(f"[INFO] Looking for BNM file: {INPUT_DATASETS['BNM']}")
    print(f"[INFO] Report date (yesterday): {REPT_DATE}")

    # 1. DELETE STEP
    disp_delete(OUTPUT_DATASET)

    # 2. VALIDATE INPUT DATASETS (DISP=SHR)
    # For BNM, try to find the file with fallback logic
    bnm_path = INPUT_DATASETS["BNM"]
    if not bnm_path.exists():
        print(f"[WARN] BNM file not found at expected path: {bnm_path}")
        # Try to find any loan file
        loan_files = list(INPUT_DIR.glob("loan*.sas7bdat"))
        if loan_files:
            latest_loan = max(loan_files, key=lambda x: x.stat().st_mtime)
            print(f"[WARN] Using alternative file: {latest_loan.name}")
            INPUT_DATASETS["BNM"] = latest_loan
        else:
            raise FileNotFoundError(f"No loan*.sas7bdat files found in {INPUT_DIR}")
    
    # Validate all input datasets
    for name, path in INPUT_DATASETS.items():
        try:
            disp_shr(path)
            print(f"[SHR] Input dataset validated: {name} -> {path.name}")
        except FileNotFoundError as e:
            if name == "BNM":
                print(f"[WARN] {e}")
                print("[WARN] Attempting to continue with available BNM file...")
                # Try one more time with different pattern
                loan_files = list(INPUT_DIR.glob("loan*.sas7bdat"))
                if loan_files:
                    INPUT_DATASETS["BNM"] = max(loan_files, key=lambda x: x.stat().st_mtime)
                    disp_shr(INPUT_DATASETS["BNM"])
                    print(f"[SHR] Using {INPUT_DATASETS['BNM'].name} as BNM")
                else:
                    raise
            else:
                raise

    # 3. NEW OUTPUT VALIDATION
    disp_new(OUTPUT_DATASET)

    # 4. EXECUTE PROGRAM LOGIC
    output_records = execute_sas_program()

    # 5. WRITE OUTPUT (Fixed block removed)
    write_file(OUTPUT_DATASET, output_records)

    print(f"========== END JOB {JOB_NAME} ==========")


# =====================================================
# PRODUCTION ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)  # Simulate JCL ABEND return code
