import os
import sys
from pathlib import Path
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/data/mainframe/")
INPUT_DIR = BASE_DIR / "input"
PROGRAM_DIR = BASE_DIR / "program"
OUTPUT_DIR = BASE_DIR / "output"
SPOOL_DIR = BASE_DIR / "spool"

JOB_NAME = "EIBWHP03"

LRECL = 80
RECFM = "FB"

INPUT_DATASETS = {
    "PGM": PROGRAM_DIR / "SAP.BNM.PROGRAM",
    "BNM": INPUT_DIR / "SAP.PBB.SASDATA"
}

OUTPUT_DATASET = OUTPUT_DIR / "SAP.PBB.FISS.HP03"

SPOOL_FILE = SPOOL_DIR / f"{JOB_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.lst"


# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(path):
    if path.exists():
        path.unlink()
        print(f"[DELETE] Removed dataset: {path}")


def disp_shr(path):
    if not path.exists():
        raise FileNotFoundError(f"[DISP ERROR] Missing input dataset: {path}")


def disp_new(path):
    if path.exists():
        raise FileExistsError(f"[DISP ERROR] Dataset already exists: {path}")


# =====================================================
# FIXED BLOCK WRITER (FB LRECL=80)
# =====================================================

def write_fixed_block(path, records, lrecl):
    with open(path, "wb") as f:
        for record in records:
            if isinstance(record, str):
                record = record.encode("utf-8")

            if len(record) > lrecl:
                record = record[:lrecl]
            elif len(record) < lrecl:
                record = record.ljust(lrecl, b" ")

            f.write(record)

    print(f"[WRITE] FB dataset created: {path}")


# =====================================================
# SYSOUT WRITER
# =====================================================

def write_sysout(lines):
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    with open(SPOOL_FILE, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"[SYSOUT] Spool file created: {SPOOL_FILE}")


# =====================================================
# SAS BUSINESS LOGIC PLACEHOLDER
# =====================================================

def execute_business_logic():
    """
    Replace with full migrated Python logic later.
    """

    print("[EXEC] Executing EIBWHP03 business logic...")

    # Simulated SP dataset output records (semicolon separated)
    sp_records = [
        "6734061000000Y;120;45;12;3",
        "6734065000000Y;500;200;50;20"
    ]

    # Simulated SYSOUT lines
    spool_lines = [
        "EIBWHP03 REPORT",
        "SMI (CUSTCD 66,67,68,69)",
        "-" * 60,
        "BNMCODE         AMOUNT",
        "6734061000000Y  120000",
        "6734065000000Y  500000",
        "-" * 60,
        "END OF REPORT"
    ]

    return sp_records, spool_lines


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():

    print(f"========== START JOB {JOB_NAME} ==========")

    # 1️⃣ DELETE old dataset
    disp_delete(OUTPUT_DATASET)

    # 2️⃣ Validate input datasets (DISP=SHR)
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {name}")

    # 3️⃣ Validate NEW dataset
    disp_new(OUTPUT_DATASET)

    # 4️⃣ Execute logic
    sp_records, spool_lines = execute_business_logic()

    # 5️⃣ Write FB dataset (LRECL=80)
    write_fixed_block(OUTPUT_DATASET, sp_records, LRECL)

    # 6️⃣ Write spool report
    write_sysout(spool_lines)

    print(f"========== END JOB {JOB_NAME} ==========")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        sys.exit(8)  # Simulate ABEND
