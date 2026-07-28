import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP02"
SPOOL_DIR = BASE_DIR / "output/EIBWHP02"

JOB_NAME = "EIBWHP02"

# Date calculation: use yesterday's date for file naming
REPORT_DATE = datetime.now() - timedelta(days=1)
REPTMON = REPORT_DATE.strftime("%Y%m")  # YYYYMM format
NOWK = REPORT_DATE.strftime("%W")       # Week number

# Input datasets with dynamic naming
INPUT_DATASETS = {
    "LOAN": INPUT_DIR / f"loan{REPTMON}{NOWK}.sas7bdat",
    "ULOAN": INPUT_DIR / f"uloan{REPTMON}{NOWK}.sas7bdat"
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
# DATA READING
# =====================================================

def read_sas_dataset(file_path):
    """
    Reads SAS7BDAT file using pyreadstat
    Returns pandas DataFrame
    """
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path)
        print(f"[READ] Successfully read {file_path.name}: {len(df)} records, {len(df.columns)} columns")
        return df, meta
    except Exception as e:
        raise Exception(f"Error reading {file_path}: {e}")

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
# BUSINESS LOGIC
# =====================================================

def execute_sas_program(loan_df, uloan_df):
    """
    Executes the business logic equivalent to EIBWHP02
    Filters CUSTCD 66,67,68,69 and aggregates by branch
    """
    print("[EXEC] Running business logic...")
    
    # Combine loan and uloan datasets
    # Assuming both have similar structure with CUSTCD and BRANCH columns
    combined_df = pd.concat([loan_df, uloan_df], ignore_index=True)
    
    # Filter for CUSTCD 66,67,68,69 (SMI customers)
    cust_codes = [66, 67, 68, 69]
    filtered_df = combined_df[combined_df['CUSTCD'].isin(cust_codes)]
    
    print(f"[FILTER] Filtered to {len(filtered_df)} records with CUSTCD in {cust_codes}")
    
    # Group by BRANCH and aggregate disbursement amounts
    # Assuming DISBURSE column exists
    branch_summary = filtered_df.groupby('BRANCH')['DISBURSE'].sum().reset_index()
    
    # Sort by branch
    branch_summary = branch_summary.sort_values('BRANCH')
    
    # Generate report lines
    report_lines = [
        f"{JOB_NAME}: SMI (CUSTCD 66,67,68,69) BY BRANCH",
        f"Report Date: {REPORT_DATE.strftime('%d-%m-%Y')}",
        f"Generated at {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}",
        f"Data Files: loan{REPTMON}{NOWK}.sas7bdat, uloan{REPTMON}{NOWK}.sas7bdat",
        "-" * 80,
        f"{'BRANCH':<15} {'DISBURSE':>20} {'COUNT':>10}",
        "-" * 80
    ]
    
    # Add data rows
    total_disburse = 0
    total_count = 0
    
    for _, row in branch_summary.iterrows():
        branch = str(row['BRANCH']).zfill(3)
        disburse = row['DISBURSE']
        count = filtered_df[filtered_df['BRANCH'] == row['BRANCH']].shape[0]
        
        report_lines.append(f"{branch:<15} {disburse:>20,.2f} {count:>10}")
        total_disburse += disburse
        total_count += count
    
    # Add totals
    report_lines.append("-" * 80)
    report_lines.append(f"{'TOTAL':<15} {total_disburse:>20,.2f} {total_count:>10}")
    report_lines.append("-" * 80)
    report_lines.append("END OF REPORT")
    
    return report_lines

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    print(f"========== START JOB {JOB_NAME} ==========")
    print(f"[INFO] Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"[INFO] File pattern: loan{REPTMON}{NOWK}.sas7bdat")
    
    # Validate DISP=SHR datasets
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {name} -> {path.name}")
    
    # Read input datasets
    print("[READ] Reading input datasets...")
    loan_df, loan_meta = read_sas_dataset(INPUT_DATASETS["LOAN"])
    uloan_df, uloan_meta = read_sas_dataset(INPUT_DATASETS["ULOAN"])
    
    # Execute SAS replacement logic
    report_output = execute_sas_program(loan_df, uloan_df)
    
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
        import traceback
        traceback.print_exc()
        sys.exit(8)  # Simulate mainframe ABEND
