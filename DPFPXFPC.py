import os
import sys
from pathlib import Path
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP02"

SPOOL_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP02"

JOB_NAME = "EIBWHP02"

INPUT_DATASETS = {
    "BNM": INPUT_DIR / "SAP.PBB.SASDATA"
}

# SYSOUT simulation file
SPOOL_FILE = SPOOL_DIR / f"{JOB_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.lst"

# =====================================================
# DISP SIMULATION
# =====================================================

def disp_shr(dataset_path):
    """
    Simulates DISP=SHR
    Must exist before execution.
    """
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"[DISP ERROR] Required dataset missing: {dataset_path}"
        )


# =====================================================
# SYSOUT SIMULATION
# =====================================================

def write_sysout(records):
    """
    Simulates SASLIST DD SYSOUT
    Writes to spool file.
    """
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    with open(SPOOL_FILE, "w", encoding="utf-8") as f:
        for line in records:
            f.write(line + "\n")

    print(f"[SYSOUT] Report written to spool: {SPOOL_FILE}")


# =====================================================
# SAS EXECUTION WRAPPER (PLACEHOLDER)
# =====================================================

def execute_sas_program():
    """
    Simulates SAS logic execution.
    Replace this with actual migrated Python logic later.
    """

    print("[EXEC] Running business logic...")

    # Placeholder report output
    report_lines = [
        "EIBWHP02: SMI (CUSTCD 66,67,68,69) BY BRANCH",
        f"Generated at {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}",
        "-" * 80,
        "BRANCH      DISBURSE",
        "001         1000000.00",
        "002         850000.00",
        "-" * 80,
        "END OF REPORT"
    ]

    return report_lines


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():

    print(f"========== START JOB {JOB_NAME} ==========")

    # Validate DISP=SHR datasets
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {name}")

    # Execute SAS replacement logic
    report_output = execute_sas_program()

    # Write SYSOUT spool file
    write_sysout(report_output)

    print(f"========== END JOB {JOB_NAME} ==========")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        sys.exit(8)  # Simulate mainframe ABEND


inputs are in sas7bdat. read by pyreadstat. loan{reptmon}{nowk}.sas7bdat and uloan{reptmon}{nowk}.sas7bdat. use datetime timedelta - 1 instead of reptdate input
