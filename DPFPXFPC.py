import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(".")
INPUT_DIR = BASE_DIR / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04"
OUTPUT_DIR = BASE_DIR / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04"

# Calculate previous business day
PREV_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

INPUT_DATASETS = {
    "LOAN": INPUT_DIR / "SAP.PBB.MNILN_0.sas7bdat",
    "BNM": INPUT_DIR / "SAP.PBB.SASDATA.sas7bdat"
}

OUTPUT_DATASET = OUTPUT_DIR / f"EIBWHP04_{PREV_DATE}.txt"
LOG_FILE = OUTPUT_DIR / f"{JOB_NAME}_{PREV_DATE}.log"

# =====================================================
# LOGGING
# =====================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(path: Path):
    if path.exists():
        path.unlink()
        logging.info(f"Deleted dataset: {path}")

def disp_shr(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"DISP=SHR failed: {path}")
    logging.info(f"Validated SHR dataset: {path}")

def disp_new(path: Path):
    if path.exists():
        raise FileExistsError(f"DISP=NEW failed (already exists): {path}")
    logging.info(f"Validated NEW dataset: {path}")

# =====================================================
# SAS7BDAT READER
# =====================================================

def read_sas7bdat(path: Path):
    """Read SAS7BDAT file and return pandas DataFrame"""
    logging.info(f"Reading SAS7BDAT: {path}")
    df = pd.read_sas(path, format='sas7bdat', encoding='utf-8')
    logging.info(f"Read {len(df)} rows from {path}")
    return df

# =====================================================
# TEXT FILE WRITER
# =====================================================

def write_text_file(path: Path, records):
    """Write records to text file"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            if isinstance(record, str):
                f.write(record + "\n")
            else:
                f.write(str(record) + "\n")
    
    logging.info(f"Text file created: {path}")

# =====================================================
# BUSINESS LOGIC (Using PBBLNFMT.py)
# =====================================================

def execute_business_logic():
    """
    Execute EIBWHP04 business logic using PBBLNFMT formatting.
    Reads SAS7BDAT inputs and applies formatting rules.
    """
    
    logging.info("Executing EIBWHP04 business logic...")
    
    # Import PBBLNFMT formatting module
    sys.path.append(str(Path(__file__).parent))
    from PBBLNFMT import format_loan_record  # Assuming this function exists
    
    # Read input datasets
    loan_df = read_sas7bdat(INPUT_DATASETS["LOAN"])
    bnm_df = read_sas7bdat(INPUT_DATASETS["BNM"])
    
    # Process records (placeholder - replace with actual logic)
    records = []
    
    for _, row in loan_df.iterrows():
        try:
            # Apply PBBLNFMT formatting
            formatted_record = format_loan_record(row, bnm_df)
            records.append(formatted_record)
        except Exception as e:
            logging.warning(f"Skipping record due to error: {e}")
            continue
    
    logging.info(f"Processed {len(records)} records")
    return records

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    logging.info(f"========== START JOB {JOB_NAME} ==========")
    logging.info(f"Processing date: {PREV_DATE}")
    
    # DELETE STEP
    disp_delete(OUTPUT_DATASET)
    
    # SHR VALIDATION
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
    
    # NEW VALIDATION
    disp_new(OUTPUT_DATASET)
    
    # EXECUTE LOGIC
    records = execute_business_logic()
    
    # WRITE TEXT FILE
    write_text_file(OUTPUT_DATASET, records)
    
    logging.info(f"========== END JOB {JOB_NAME} ==========")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
        sys.exit(0)   # RC=0 success
    except Exception as e:
        logging.error(f"JOB FAILED: {e}", exc_info=True)
        sys.exit(8)   # RC=8 failure
