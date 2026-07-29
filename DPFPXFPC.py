
import sys
import logging
from pathlib import Path
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(".")
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04"


INPUT_DATASETS = {
    "LOAN": INPUT_DIR / "SAP.PBB.MNILN_0",
    "BNM": INPUT_DIR / "SAP.PBB.SASDATA"
}

OUTPUT_DATASET = OUTPUT_DIR / "EIBWHP04.txt"
LOG_FILE = LOG_DIR / f"{JOB_NAME}_{datetime.now():%Y%m%d}.log"

# =====================================================
# LOGGING
# =====================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

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
# FIXED BLOCK WRITER (FB LRECL=80)
# =====================================================

def write_fixed_block(path: Path, records, lrecl: int):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        for record in records:
            if isinstance(record, str):
                record = record.encode("utf-8")

            if len(record) > lrecl:
                record = record[:lrecl]
            elif len(record) < lrecl:
                record = record.ljust(lrecl, b" ")

            f.write(record)

    logging.info(f"FB dataset created: {path}")

# =====================================================
# BUSINESS LOGIC PLACEHOLDER
# =====================================================

def execute_business_logic():
    """
    Replace with full migrated Python logic later.
    Must return FB record list (semicolon separated).
    """

    logging.info("Executing EIBWHP04 business logic...")

    # Simulated output records (semicolon-separated)
    records = [
        "6734000000000Y;100;40;10;5",
        "7734000000000Y;20;5;2;1",
        "8715000000000Y;300;0;15;0"
    ]

    return records

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    logging.info(f"========== START JOB {JOB_NAME} ==========")

    # DELETE STEP
    disp_delete(OUTPUT_DATASET)

    # SHR VALIDATION
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)

    # NEW VALIDATION
    disp_new(OUTPUT_DATASET)

    # EXECUTE LOGIC
    records = execute_business_logic()

    # WRITE FB DATASET
    write_fixed_block(OUTPUT_DATASET, records, LRECL)

    logging.info(f"========== END JOB {JOB_NAME} ==========")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
        sys.exit(0)   # RC=0 success
    except Exception as e:
        logging.error(f"JOB FAILED: {e}")
        sys.exit(8)   # RC=8 failure



remove lrecl and logs dir, rmeove reptdate, replace with datetime timedelta -1, all inputs are in sas7bdat. output in text file. include the PBBLNFMT.py
