# EIIDWKLY_WEEKLY_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
import sys
import subprocess

def main():
    # Configuration using pathlib
    base_path = Path(".")
    bnm_path = base_path / "BNM"
    loan_path = base_path / "LOAN"
    deposit_path = base_path / "DEPOSIT"
    
    # Create output directory if it doesn't exist
    bnm_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters
    reptdate_query = f"""
    SELECT * FROM read_parquet('{loan_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        wk = reptdate.day
        mm = reptdate.month
        nowk = f"{wk:02d}"
        reptmon = f"{mm:02d}"
        reperyear = reptdate.strftime('%Y')
        reperyr = reptdate.strftime('%y')
        rdate = reptdate.strftime('%d%m%y')
        sdate = reptdate.strftime('%d%m%y')
        tdate = reptdate.strftime('%j')  # Julian day
        
        print(f"Processing date: {reptdate}")
        print(f"Week: {nowk}, Month: {reptmon}, Year: {reperyear}")
        print(f"Formatted dates - RDate: {rdate}, SDate: {sdate}, TDate: {tdate}")
    else:
        raise ValueError("No data found in LOAN.REPTDATE table")
    
    # Save BNM.REPTDATE
    bnm_reptdate_query = f"""
    SELECT 
        REPTDATE,
        {wk} as WK,
        {mm} as MM
    FROM read_parquet('{loan_path / "REPTDATE.parquet"}')
    """
    bnm_reptdate_df = conn.execute(bnm_reptdate_query).arrow()
    pq.write_table(bnm_reptdate_df, bnm_path / "REPTDATE.parquet")
    
    # Step 2: Get LOAN REPTDATE
    loan_date = None
    try:
        loan_reptdate_query = f"""
        SELECT * FROM read_parquet('{loan_path / "REPTDATE.parquet"}')
        """
        loan_reptdate_df = conn.execute(loan_reptdate_query).arrow()
        if len(loan_reptdate_df) > 0:
            loan_date = loan_reptdate_df['REPTDATE'][0].as_py().strftime('%d%m%y')
    except Exception as e:
        print(f"Error reading LOAN REPTDATE: {e}")
    
    # Step 3: Get DEPOSIT REPTDATE
    deposit_date = None
    try:
        deposit_reptdate_query = f"""
        SELECT * FROM read_parquet('{deposit_path / "REPTDATE.parquet"}')
        """
        deposit_reptdate_df = conn.execute(deposit_reptdate_query).arrow()
        if len(deposit_reptdate_df) > 0:
            deposit_date = deposit_reptdate_df['REPTDATE'][0].as_py().strftime('%d%m%y')
    except Exception as e:
        print(f"Error reading DEPOSIT REPTDATE: {e}")
    
    # Step 4: Implement the PROCESS macro logic
    def process_macro():
        """Equivalent to SAS %PROCESS macro"""
        if loan_date == rdate and deposit_date == rdate:
            print("Both LOAN and DEPOSIT extractions are dated correctly. Running processing programs...")
            
            # Execute the equivalent Python programs
            programs_to_run = [
                "LALWPBBD.py",  # Equivalent to PGM(LALWPBBD)
                "LALWPBBI.py",  # Equivalent to PGM(LALWPBBI)
                "LALWPBBU.py",  # Equivalent to PGM(LALWPBBU)
                "LALWEIRC.py"   # Equivalent to PGM(LALWEIRC)
            ]
            
            for program in programs_to_run:
                program_path = base_path / program
                if program_path.exists():
                    print(f"Executing {program}...")
                    try:
                        # Run the Python program as a subprocess
                        result = subprocess.run([sys.executable, str(program_path)], 
                                              capture_output=True, text=True)
                        if result.returncode == 0:
                            print(f"✓ {program} completed successfully")
                        else:
                            print(f"✗ {program} failed with return code {result.returncode}")
                            print(f"Error: {result.stderr}")
                    except Exception as e:
                        print(f"Error executing {program}: {e}")
                else:
                    print(f"Warning: Program {program} not found")
            
            # Clean up WORK directory (equivalent to PROC DATASETS LIB=WORK KILL)
            clean_work_directory()
            
        else:
            # Handle mismatch cases
            if loan_date != rdate:
                print(f"THE LOAN EXTRACTION IS NOT DATED {rdate} - it is dated {loan_date}")
            if deposit_date != rdate:
                print(f"THE DEPOSIT EXTRACTION IS NOT DATED {rdate} - it is dated {deposit_date}")
            print("THE JOB IS NOT DONE !!")
            
            # Equivalent to DATA A; ABORT 77;
            sys.exit(77)
    
    def clean_work_directory():
        """Clean up temporary files in WORK directory equivalent to PROC DATASETS KILL"""
        work_path = base_path / "WORK"
        if work_path.exists():
            print("Cleaning up WORK directory...")
            try:
                for file in work_path.glob("*"):
                    if file.is_file():
                        file.unlink()
                print("WORK directory cleaned successfully")
            except Exception as e:
                print(f"Error cleaning WORK directory: {e}")
        else:
            print("WORK directory does not exist, nothing to clean")
    
    # Execute the main processing logic
    process_macro()
    
    # Print final summary
    print(f"\nWeekly processing completed successfully!")
    print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
    print(f"Parameters extracted:")
    print(f"  NOWK: {nowk}")
    print(f"  REPTMON: {reptmon}")
    print(f"  REPTYEAR: {reperyear}")
    print(f"  REPTYR: {reperyr}")
    print(f"  RDATE: {rdate}")
    print(f"  SDATE: {sdate}")
    print(f"  TDATE: {tdate}")
    
    conn.close()

if __name__ == "__main__":
    main()
