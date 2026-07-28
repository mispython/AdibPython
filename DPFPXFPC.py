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

# Input datasets (sas7bdat format)
INPUT_DATASETS = {
    "BNM": INPUT_DIR / "loan{REPTMON}{NOWK}.sas7bdat",
    "LOAN": INPUT_DIR / "lnnote.sas7bdat"
}

# Parquet cache directory for large files
PARQUET_CACHE_DIR = BASE_DIR / "cache/parquet"
PARQUET_CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DATASET = OUTPUT_DIR / "EIBWHP01.txt"

# Report date = yesterday (removed external reptdate input)
REPT_DATE = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

# =====================================================
# IMPORT EXISTING FORMAT MODULE
# =====================================================

try:
    import PBBLNFMT
except ImportError:
    print("[WARN] PBBLNFMT.py not found in PYTHONPATH. Proceeding without format module.")
    PBBLNFMT = None

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
# SAS7BDAT TO PARQUET CONVERTER (for large files)
# =====================================================

def convert_sas_to_parquet(sas_path, parquet_path, force_rebuild=False):
    """
    Converts a .sas7bdat file to parquet format for faster subsequent reads.
    If parquet exists and force_rebuild=False, skip conversion.
    """
    if parquet_path.exists() and not force_rebuild:
        print(f"[CACHE] Using cached parquet: {parquet_path.name}")
        return parquet_path
    
    try:
        import pyreadstat
    except ImportError:
        raise ImportError(
            "pyreadstat is required to read .sas7bdat files. "
            "Install it via: pip install pyreadstat"
        )
    
    print(f"[CONVERT] Converting {sas_path.name} to parquet...")
    start_time = datetime.now()
    
    # Read SAS file in chunks for large files
    try:
        # Try chunked reading first (more memory efficient)
        reader = pyreadstat.read_sas7bdat(
            str(sas_path),
            chunksize=100000  # Read 100k rows at a time
        )
        
        first_chunk = True
        for df_chunk, meta in reader:
            if first_chunk:
                df_chunk.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
                first_chunk = False
            else:
                # Append to existing parquet
                df_chunk.to_parquet(
                    parquet_path, 
                    engine='pyarrow', 
                    compression='snappy',
                    append=True
                )
            print(f"[CONVERT] Processed {len(df_chunk)} rows...")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"[CONVERT] Completed in {elapsed:.2f} seconds")
        return parquet_path
        
    except Exception as e:
        print(f"[WARN] Chunked conversion failed: {e}")
        print("[WARN] Attempting single-pass conversion...")
        
        # Fallback to single read
        df, meta = pyreadstat.read_sas7bdat(str(sas_path))
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"[CONVERT] Completed in {elapsed:.2f} seconds")
        return parquet_path


def read_sas_dataset(dataset_path, use_cache=True):
    """
    Reads a .sas7bdat file using pyreadstat.
    For large files, converts to parquet first for faster subsequent reads.
    Returns (pandas.DataFrame, metadata)
    """
    # Check if this is a large file (lnnote)
    if "lnnote" in str(dataset_path).lower():
        parquet_path = PARQUET_CACHE_DIR / f"{dataset_path.stem}.parquet"
        
        if use_cache:
            try:
                # Convert to parquet if needed
                parquet_path = convert_sas_to_parquet(dataset_path, parquet_path)
                
                # Read from parquet (much faster)
                print(f"[READ] Loading from parquet cache: {parquet_path.name}")
                df = pd.read_parquet(parquet_path)
                print(f"[READ] Loaded {len(df)} records from cache")
                
                # Return empty metadata (not available from parquet)
                return df, None
            except Exception as e:
                print(f"[WARN] Parquet cache read failed: {e}")
                print("[WARN] Falling back to direct SAS read...")
    
    # Direct SAS read (for small files or if cache fails)
    try:
        import pyreadstat
    except ImportError:
        raise ImportError(
            "pyreadstat is required to read .sas7bdat files. "
            "Install it via: pip install pyreadstat"
        )
    
    print(f"[READ] Direct read of {dataset_path.name}")
    df, meta = pyreadstat.read_sas7bdat(str(dataset_path))
    print(f"[READ] Loaded {len(df)} records from {dataset_path.name}")
    return df, meta


# =====================================================
# FIXED BLOCK FILE WRITER (Removed LRECL enforcement)
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
    bnm_df, bnm_meta = read_sas_dataset(INPUT_DATASETS["BNM"])
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
    print(f"========== START JOB {JOB_NAME} ==========")
    print(f"[INFO] Report date (yesterday): {REPT_DATE}")

    # 1. DELETE STEP
    disp_delete(OUTPUT_DATASET)

    # 2. VALIDATE INPUT DATASETS (DISP=SHR)
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Input dataset validated: {name}")

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
        sys.exit(8)  # Simulate JCL ABEND return code


adjust the loan reptmon and nowk
