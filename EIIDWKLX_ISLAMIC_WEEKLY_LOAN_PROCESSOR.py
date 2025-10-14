# EIIDWKLX_WEEKLY_LOAN_PROCESSOR.py

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
    bnm1_path = base_path / "BNM1"
    loan_path = base_path / "LOAN"
    
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
    
    # Step 3: Process CAGAMAS data (SMR 2021-4114)
    caga_list = "999999999"  # Default value
    try:
        caga_file = base_path / "CAGA.csv"
        if caga_file.exists():
            # Read CAGAMAS data (skip first row, read CAGATAG column)
            caga_query = f"""
            WITH caga_data AS (
                SELECT 
                    CAST(SUBSTR(line, 1, 9) as BIGINT) as CAGATAG
                FROM read_csv('{caga_file}', skip=1, header=false, columns={{'line': 'VARCHAR'}})
                WHERE LENGTH(line) >= 9
            )
            SELECT DISTINCT CAGATAG 
            FROM caga_data 
            WHERE CAGATAG > 0
            ORDER BY CAGATAG
            """
            caga_df = conn.execute(caga_query).arrow()
            
            if len(caga_df) > 0:
                caga_values = [str(caga_df['CAGATAG'][i].as_py()) for i in range(len(caga_df))]
                caga_list = ','.join(caga_values)
                print(f"CAGATAG values found: {caga_list}")
            else:
                print("No valid CAGATAG values found, using default")
        else:
            print("CAGA file not found, using default CAGALIST")
    except Exception as e:
        print(f"Error processing CAGAMAS data: {e}")
        print("Using default CAGALIST")
    
    print(f"CAGATAG=({caga_list})")
    
    # Step 4: Implement the PROCESS macro logic
    def process_macro():
        """Equivalent to SAS %PROCESS macro"""
        if loan_date == rdate:
            print("LOAN extraction is dated correctly. Running processing...")
            
            # Copy BNM1.SME08 to BNM.SME08
            try:
                sme08_source = bnm1_path / "SME08.parquet"
                sme08_target = bnm_path / "SME08.parquet"
                if sme08_source.exists():
                    sme08_df = pq.read_table(sme08_source)
                    pq.write_table(sme08_df, sme08_target)
                    print("✓ Copied BNM1.SME08 to BNM.SME08")
                else:
                    print("✗ BNM1.SME08.parquet not found")
            except Exception as e:
                print(f"Error copying SME08 data: {e}")
            
            # Execute LALWPBBC program
            program_to_run = "LALWPBBC.py"
            program_path = base_path / program_to_run
            if program_path.exists():
                print(f"Executing {program_to_run}...")
                try:
                    result = subprocess.run([sys.executable, str(program_path)], 
                                          capture_output=True, text=True)
                    if result.returncode == 0:
                        print(f"✓ {program_to_run} completed successfully")
                    else:
                        print(f"✗ {program_to_run} failed with return code {result.returncode}")
                        print(f"Error: {result.stderr}")
                except Exception as e:
                    print(f"Error executing {program_to_run}: {e}")
            else:
                print(f"Warning: Program {program_to_run} not found")
            
            # Process NOTE data with CAGATAG logic
            note_filename = f"NOTE{reptmon}{nowk}"
            note_source = bnm_path / f"{note_filename}.parquet"
            
            if note_source.exists():
                try:
                    # Read the NOTE data
                    note_query = f"""
                    SELECT * FROM read_parquet('{note_source}')
                    """
                    note_df = conn.execute(note_query).arrow()
                    
                    # Apply CAGATAG logic
                    if caga_list != "999999999":
                        # Convert string list to Python list of integers
                        caga_values = [int(x.strip()) for x in caga_list.split(',')]
                        
                        # Process records
                        records = note_df.to_pylist()
                        for record in records:
                            if record.get('PZIPCODE') in caga_values:
                                record['PRODCD'] = '54120'
                        
                        # Convert back to Arrow table
                        note_df = pa.Table.from_pylist(records)
                    
                    # Write updated data back
                    pq.write_table(note_df, note_source)
                    print(f"✓ Updated {note_filename} with CAGATAG logic")
                    
                except Exception as e:
                    print(f"Error processing {note_filename}: {e}")
            else:
                print(f"Warning: {note_filename}.parquet not found")
            
            # Clean up WORK directory
            clean_work_directory()
            
        else:
            # Handle mismatch case
            if loan_date != rdate:
                print(f"THE LOAN EXTRACTION IS NOT DATED {rdate} - it is dated {loan_date}")
            print("THE JOB IS NOT DONE !!")
            
            # Equivalent to DATA A; ABORT 77;
            sys.exit(77)
    
    def clean_work_directory():
        """Clean up temporary files in WORK directory"""
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
    print(f"\nWeekly loan processing completed successfully!")
    print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
    print(f"Parameters:")
    print(f"  NOWK: {nowk}")
    print(f"  REPTMON: {reptmon}") 
    print(f"  REPTYEAR: {reperyear}")
    print(f"  RDATE: {rdate}")
    print(f"  SDATE: {sdate}")
    print(f"  TDATE: {tdate}")
    print(f"  CAGALIST: {caga_list}")
    
    conn.close()

if __name__ == "__main__":
    main()
