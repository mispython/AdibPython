import os
import sys
from pathlib import Path
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP01"
OUTPUT_DIR = BASE_DIR / "output/EIBWHP01"


JOB_NAME = "EIBWHP01"

INPUT_DATASETS = {
    "BNM": INPUT_DIR / "SAP.PBB.SASDATA",
    "LOAN": INPUT_DIR / "SAP.PBB.MNILN_0"
}

OUTPUT_DATASET = OUTPUT_DIR / "EIBWHP01.txt"


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
# FIXED BLOCK FILE WRITER
# =====================================================

def write_fixed_block(path, records, lrecl):
    """
    Writes FB dataset with enforced LRECL
    """
    with open(path, "wb") as f:
        for record in records:
            if isinstance(record, str):
                record = record.encode("utf-8")

            if len(record) > lrecl:
                record = record[:lrecl]
            elif len(record) < lrecl:
                record = record.ljust(lrecl, b" ")

            f.write(record)

    print(f"[WRITE] Output dataset created: {path}")


# =====================================================
# SAS EXECUTION WRAPPER (SIMULATED)
# =====================================================

def execute_sas_program():
    """
    Simulates EXEC SAS609 step.
    Replace this with actual Python business logic
    if SAS logic is migrated.
    """

    print("[EXEC] Starting SAS logic replacement...")

    # Placeholder output record (for structure preservation)
    sample_output = [
        "EIBWHP01 REPORT GENERATED " + datetime.now().strftime("%d-%m-%Y")
    ]

    return sample_output


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():

    print(f"========== START JOB {JOB_NAME} ==========")

    # 1️⃣ DELETE STEP
    disp_delete(OUTPUT_DATASET)

    # 2️⃣ VALIDATE INPUT DATASETS (DISP=SHR)
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Input dataset validated: {name}")

    # 3️⃣ NEW OUTPUT VALIDATION
    disp_new(OUTPUT_DATASET)

    # 4️⃣ EXECUTE PROGRAM LOGIC
    output_records = execute_sas_program()

    # 5️⃣ WRITE FIXED BLOCK OUTPUT
    write_fixed_block(OUTPUT_DATASET, output_records, LRECL)

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


all inputs are in sas7bdat. may use pyreadstat. output in text file. remove reptdate input, use datetime timedelta - 1 instead. include PBBLNFMT.py (the program already existed)
