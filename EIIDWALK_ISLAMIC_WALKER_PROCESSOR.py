# EIIDWALK_WALKER_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
import sys
import subprocess
from datetime import datetime, timedelta

def main():
    # Configuration using pathlib
    base_path = Path(".")
    loan_path = base_path / "LOAN"
    prewalk_path = base_path / "PREWALK"
    walk_path = base_path / "WALK"
    
    # Create output directories if they don't exist
    walk_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters
    reptdate_query = f"""
    SELECT * FROM read_parquet('{loan_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        prevdate = reptdate - timedelta(days=1)
        
        rept_year = reptdate.strftime('%y')
        rept_month = reptdate.strftime('%m')
        rept_day = reptdate.strftime('%d')
        prev_year = prevdate.strftime('%y')
        prev_month = prevdate.strftime('%m')
        prev_day = prevdate.strftime('%d')
        rdate = reptdate.strftime('%d%m%y')
        
        print(f"Processing date: {reptdate}")
        print(f"Previous date: {prevdate}")
        print(f"Current: {rept_year}-{rept_month}-{rept_day}")
        print(f"Previous: {prev_year}-{prev_month}-{prev_day}")
    else:
        raise ValueError("No data found in LOAN.REPTDATE table")
    
    # Step 2: Process WALKTXT file to get walker date
    walktxt_file = base_path / "WALKTXT.txt"
    wdate = None
    
    if walktxt_file.exists():
        try:
            # Read first line to get date
            with open(walktxt_file, 'r') as f:
                first_line = f.readline().strip()
                
            if len(first_line) >= 8:
                dd = first_line[1:3]  # positions 2-3
                mm = first_line[4:6]  # positions 5-6  
                yy = first_line[7:9]  # positions 8-9
                
                # Create date object (assuming 20yy for year)
                wdate = datetime.strptime(f"{dd}/{mm}/20{yy}", "%d/%m/%Y")
                wdte = wdate.strftime('%d%m%y')
                print(f"Walker file date: {wdate.strftime('%Y-%m-%d')}")
                print(f"Formatted WDTE: {wdte}")
            else:
                raise ValueError("Invalid WALKTXT format")
                
        except Exception as e:
            print(f"Error processing WALKTXT file: {e}")
            sys.exit(77)
    else:
        print("WALKTXT file not found")
        sys.exit(77)
    
    # Step 3: Implement the PROCESS macro logic
    def process_macro():
        """Equivalent to SAS %PROCESS macro"""
        if wdte == rdate:
            print("Walker file date matches reporting date. Processing data...")
            
            # Process WALKTXT data
            wk_data = []
            try:
                with open(walktxt_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if len(line) >= 42:  # Ensure minimum length
                            indicode = line[0:1] if len(line) > 0 else ''
                            
                            # Only process lines with '+' indicator
                            if indicode == '+':
                                oribr = line[1:5] if len(line) >= 5 else ''
                                ntbrch = line[4:7] if len(line) >= 7 else ''
                                prod = line[6:19] if len(line) >= 19 else ''
                                
                                # Parse CURBAL (comma formatted number)
                                curbal_str = line[19:41] if len(line) >= 41 else '0'
                                try:
                                    # Remove commas and convert to float
                                    curbal = float(curbal_str.replace(',', ''))
                                except:
                                    curbal = 0.0
                                
                                # Determine LNTYPE based on PROD
                                lntype = None
                                zsbal = None
                                
                                if prod == 'ML34170':
                                    lntype = 'FS'
                                elif prod == 'S-FCYSYNLN':
                                    lntype = 'SL' 
                                elif prod == 'S-FCYRC':
                                    lntype = 'SR'
                                elif prod == 'ZIIPSUSP':
                                    lntype = 'ZS'
                                    zsbal = curbal
                                elif prod.startswith('34200'):
                                    lntype = 'CC'
                                elif prod.startswith('54120'):
                                    lntype = 'HC'
                                
                                if lntype:  # Only include records with valid LNTYPE
                                    record = {
                                        'ORIBR': oribr,
                                        'NTBRCH': int(ntbrch) if ntbrch.strip().isdigit() else 0,
                                        'PROD': prod,
                                        'CURBAL': curbal,
                                        'LNTYPE': lntype,
                                        'ZSBAL': zsbal
                                    }
                                    wk_data.append(record)
                
                print(f"Processed {len(wk_data)} records from WALKTXT")
                
            except Exception as e:
                print(f"Error reading WALKTXT file: {e}")
                sys.exit(77)
            
            # Convert to DataFrame and apply additional logic
            if wk_data:
                wk_df = pa.Table.from_pylist(wk_data)
                
                # Apply branch logic and IPLNTB1 program
                processed_records = []
                for record in wk_df.to_pylist():
                    oribr = record['ORIBR']
                    
                    # Apply branch-specific logic
                    if oribr and oribr[0] in ['H', 'I']:
                        # Execute IPLNTB1 program
                        iplntb1_program = base_path / "IPLNTB1.py"
                        if iplntb1_program.exists():
                            try:
                                result = subprocess.run([sys.executable, str(iplntb1_program)], 
                                                      capture_output=True, text=True)
                                if result.returncode == 0:
                                    print("✓ IPLNTB1 executed successfully")
                                else:
                                    print(f"✗ IPLNTB1 failed: {result.stderr}")
                            except Exception as e:
                                print(f"Error executing IPLNTB1: {e}")
                    
                    # Set AMTIND based on branch
                    if oribr and oribr[0] in ['3', 'I']:
                        record['AMTIND'] = 'I'
                    else:
                        record['AMTIND'] = None
                    
                    # Rename NTBRCH to BRANCH
                    record['BRANCH'] = record.pop('NTBRCH')
                    processed_records.append(record)
                
                wk_df = pa.Table.from_pylist(processed_records)
                
                # Process previous day's data
                prev_filename = f"WK{prev_year}{prev_month}{prev_day}.parquet"
                prev_file = prewalk_path / prev_filename
                
                prewk_data = []
                if prev_file.exists():
                    try:
                        prewk_df = pq.read_table(prev_file)
                        # Filter for ZS records and rename BALANCE to PREBAL
                        for record in prewk_df.to_pylist():
                            if record.get('LNTYPE') == 'ZS':
                                prewk_data.append({
                                    'ORIBR': record.get('ORIBR'),
                                    'LNTYPE': record.get('LNTYPE'),
                                    'PREBAL': record.get('BALANCE', 0)
                                })
                        print(f"Loaded {len(prewk_data)} previous ZS records")
                    except Exception as e:
                        print(f"Error reading previous walk data: {e}")
                
                # Merge current and previous data
                merged_data = []
                
                # Group current data by ORIBR and LNTYPE
                current_by_key = {}
                for record in wk_df.to_pylist():
                    key = (record['ORIBR'], record['LNTYPE'])
                    current_by_key[key] = record
                
                # Group previous data by ORIBR and LNTYPE  
                previous_by_key = {}
                for record in prewk_data:
                    key = (record['ORIBR'], record['LNTYPE'])
                    previous_by_key[key] = record
                
                # Merge logic
                all_keys = set(current_by_key.keys()) | set(previous_by_key.keys())
                
                for key in all_keys:
                    oribr, lntype = key
                    current_rec = current_by_key.get(key, {})
                    previous_rec = previous_by_key.get(key, {})
                    
                    merged_record = {
                        'ORIBR': oribr,
                        'LNTYPE': lntype,
                        'BRANCH': current_rec.get('BRANCH'),
                        'PROD': current_rec.get('PROD'),
                        'CURBAL': current_rec.get('CURBAL', 0),
                        'ZSBAL': current_rec.get('ZSBAL'),
                        'AMTIND': current_rec.get('AMTIND'),
                        'PREBAL': previous_rec.get('PREBAL', 0)
                    }
                    
                    # Apply ZS balance logic
                    if lntype == 'ZS' and merged_record['CURBAL'] == 0:
                        merged_record['BALANCE'] = merged_record['PREBAL']
                    else:
                        merged_record['BALANCE'] = merged_record['CURBAL']
                    
                    merged_data.append(merged_record)
                
                # Create final output
                final_df = pa.Table.from_pylist(merged_data)
                
                # Save final output
                output_filename = f"WK{rept_year}{rept_month}{rept_day}.parquet"
                output_file = walk_path / output_filename
                pq.write_table(final_df, output_file)
                
                # Also create CSV for easy viewing
                csv.write_csv(final_df, walk_path / f"WK{rept_year}{rept_month}{rept_day}.csv")
                
                # Print summary (equivalent to PROC PRINT)
                print(f"\nFinal output summary - {len(final_df)} records:")
                print(f"File: {output_filename}")
                
                # Print first few records
                if len(final_df) > 0:
                    print("\nFirst 5 records:")
                    for i, record in enumerate(final_df.to_pylist()[:5]):
                        print(f"  {i+1}: ORIBR={record['ORIBR']}, LNTYPE={record['LNTYPE']}, "
                              f"BALANCE={record['BALANCE']:,.2f}")
                
            else:
                print("No valid data processed from WALKTXT")
                sys.exit(77)
                
        else:
            # Handle date mismatch
            print(f"THE WALKER - FDP.APPL.DAILY.LOAN IS NOT DATED {rdate} - it is dated {wdte}")
            print("THE WALKER JOB - FDPGLRDL")
            print("THE JOB IS NOT DONE !!")
            sys.exit(77)
    
    # Execute the main processing logic
    process_macro()
    
    print(f"\nWalker processing completed successfully!")
    print(f"Output saved to: {walk_path / f'WK{rept_year}{rept_month}{rept_day}.parquet'}")
    
    conn.close()

if __name__ == "__main__":
    main()
