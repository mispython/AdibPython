#!/usr/bin/env python3
"""
EIIMNPLE - Job Runner (Calling Program)
Original JCL: //EIIMNPLE JOB MISEIS,EIFMNPL0,COND=(0,LT),CLASS=A,MSGCLASS=X
Executes: EIFMNP02
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Job parameters (from JCL)
JOB_NAME = "EIIMNPLE"
JOB_ID = "MISEIS"
PROGRAM = "eifmnp02.py"

# Dataset assignments (DD statements)
LOAN_DD = Path("Data_Warehouse/NPL/input/LOAN")      # //LOAN DD DSN=SAP.PIBB.MNILN(0)
NPL6_DD = Path("Data_Warehouse/NPL/input/NPL6")      # //NPL6 DD DSN=SAP.PIBB.NPL.HP.SASDATA
OUTPUT_DD = Path("Data_Warehouse/NPL/output")        # Output directory

# Log file
LOG_FILE = Path(f"logs/{JOB_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

def log(msg):
    """Write to log file and console"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {msg}"
    print(log_msg)
    with open(LOG_FILE, 'a') as f:
        f.write(log_msg + '\n')

def validate():
    """Validate prerequisites"""
    log(f"Job: {JOB_NAME} ({JOB_ID})")
    log(f"Program: {PROGRAM}")
    log("="*70)
    
    if not Path(PROGRAM).exists():
        log(f"✗ Program not found: {PROGRAM}")
        return False
    
    required_files = [
        LOAN_DD / "lnnote.parquet",
        LOAN_DD / "reptdate.parquet",
        NPL6_DD / "nplobal.parquet",
        NPL6_DD / "wiis.parquet",
        NPL6_DD / "waq.parquet",
        NPL6_DD / "wsp2.parquet"
    ]
    
    for f in required_files:
        if not f.exists():
            log(f"✗ Required file missing: {f}")
            return False
        log(f"✓ {f.name} ({f.stat().st_size / 1024 / 1024:.2f} MB)")
    
    OUTPUT_DD.mkdir(parents=True, exist_ok=True)
    return True

def execute():
    """Execute EIFMNP02 (equivalent to //EIIMNPLE EXEC SAS609)"""
    log("\n" + "="*70)
    log(f"Executing: {PROGRAM}")
    log("="*70)
    
    try:
        result = subprocess.run(
            [sys.executable, PROGRAM],
            env={
                **subprocess.os.environ,
                'LOAN_LIBRARY': str(LOAN_DD),
                'NPL6_LIBRARY': str(NPL6_DD),
                'OUTPUT_PATH': str(OUTPUT_DD)
            },
            capture_output=True,
            text=True,
            timeout=3600  # REGION=8M equivalent (1 hour)
        )
        
        if result.stdout:
            log(result.stdout)
        if result.stderr:
            log(f"STDERR: {result.stderr}")
        
        if result.returncode == 0:
            log("="*70)
            log(f"✓ {PROGRAM} completed successfully")
            return 0
        else:
            log("="*70)
            log(f"✗ {PROGRAM} failed (RC={result.returncode})")
            return result.returncode
            
    except subprocess.TimeoutExpired:
        log(f"✗ {PROGRAM} timeout (>1 hour)")
        return 1
    except Exception as e:
        log(f"✗ Error: {str(e)}")
        return 1

def main():
    """Main execution"""
    start = datetime.now()
    
    # COND=(0,LT) - Abort if any step fails
    if not validate():
        log("\n✗ Job aborted - validation failed")
        sys.exit(1)
    
    rc = execute()
    
    duration = datetime.now() - start
    log("\n" + "="*70)
    log(f"Job: {JOB_NAME}")
    log(f"Duration: {duration}")
    log(f"Return Code: {rc}")
    log(f"Log: {LOG_FILE}")
    log("="*70)
    
    sys.exit(rc)

if __name__ == "__main__":
    main()
