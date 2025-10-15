# EIMREPOI_REPO_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from datetime import datetime

def main():
    # Configuration using pathlib
    base_path = Path(".")
    loan_path = base_path / "LOAN"
    arrear_path = base_path / "ARREAR"
    output_path = base_path / "OUTPUT"
    
    # Create output directory if it doesn't exist
    output_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters with conditional logic
    reptdate_query = f"""
    SELECT * FROM read_parquet('{loan_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        day = reptdate.day
        month = reptdate.month
        year = reptdate.year
        
        # Implement SELECT(DAY(REPTDATE)) logic
        if day == 8:
            sdd = 1
            wk = '1'
            wk1 = '4'
        elif day == 15:
            sdd = 9
            wk = '2'
            wk1 = '1'
        elif day == 22:
            sdd = 16
            wk = '3'
            wk1 = '2'
        else:
            sdd = 23
            wk = '4'
            wk1 = '3'
        
        # Calculate MM1
        if wk == '1':
            mm1 = month - 1
            if mm1 == 0:
                mm1 = 12
        else:
            mm1 = month
        
        # Calculate SDATE
        sdate = datetime(year, month, sdd)
        
        # Set macro variables equivalent
        nowk = wk
        nowk1 = wk1
        reptmon = f"{month:02d}"
        reptmon1 = f"{mm1:02d}"
        reperyear = reptdate.strftime('%Y')
        reptday = f"{day:02d}"
        rdate = reptdate.strftime('%d%m%y')
        sdate_str = sdate.strftime('%d%m%y')
        
        print(f"Processing date: {reptdate}")
        print(f"Week: {nowk}, Previous Week: {nowk1}")
        print(f"Month: {reptmon}, Previous Month: {reptmon1}")
        print(f"RDate: {rdate}, SDate: {sdate_str}")
    else:
        raise ValueError("No data found in LOAN.REPTDATE table")
    
    # Step 2: Process LNNOTE data with filters
    # Note: &HP macro variable would need to be defined - using common values as example
    hp_values = "('983', '993', '984', '994')"  # Example HP loan types
    
    lnnote_query = f"""
    SELECT 
        ACCTNO, 
        LOANTYPE,
        NTBRCH,
        COLLDESC,
        COLLYEAR
    FROM read_parquet('{loan_path / "LNNOTE.parquet"}')
    WHERE LOANTYPE IN {hp_values}
      AND BALANCE > 0
      AND BORSTAT NOT IN ('F', 'I', 'R')
    """
    
    lnnote_df = conn.execute(lnnote_query).arrow()
    print(f"LNNOTE records after filtering: {len(lnnote_df)}")
    
    # Step 3: Process NAME8 data
    name8_query = f"""
    SELECT 
        ACCTNO,
        LINETHRE as ENGINE,
        LINEFOUR as CHASSIS
    FROM read_parquet('{loan_path / "NAME8.parquet"}')
    """
    
    name8_df = conn.execute(name8_query).arrow()
    print(f"NAME8 records: {len(name8_df)}")
    
    # Step 4: Process ARREAR data
    arrear_query = f"""
    SELECT 
        ACCTNO,
        ARREAR
    FROM read_parquet('{arrear_path / "LOANTEMP.parquet"}')
    """
    
    arrear_df = conn.execute(arrear_query).arrow()
    print(f"ARREAR records: {len(arrear_df)}")
    
    # Step 5: Merge datasets
    merge_query = f"""
    WITH merged_data AS (
        SELECT 
            COALESCE(l.ACCTNO, n.ACCTNO, a.ACCTNO) as ACCTNO,
            l.LOANTYPE,
            l.NTBRCH,
            l.COLLDESC,
            l.COLLYEAR,
            n.ENGINE,
            n.CHASSIS,
            a.ARREAR
        FROM lnnote_df l
        LEFT JOIN name8_df n ON l.ACCTNO = n.ACCTNO
        LEFT JOIN arrear_df a ON l.ACCTNO = a.ACCTNO
    )
    SELECT * FROM merged_data
    """
    
    repo_df = conn.execute(merge_query).arrow()
    print(f"Merged REPO records: {len(repo_df)}")
    
    # Step 6: Process REPO data - add derived fields
    # This requires Python processing for string operations
    repo_records = repo_df.to_pylist()
    processed_records = []
    
    for record in repo_records:
        # Extract BRABBR and CAC (would need lookup tables - using NTBRCH as placeholder)
        brabbr = str(record['NTBRCH'])[:3] if record['NTBRCH'] else "000"
        cac = f"BRANCH_{record['NTBRCH']}" if record['NTBRCH'] else "UNKNOWN"
        
        # Extract vehicle details from COLLDESC
        coll_desc = record.get('COLLDESC', '')
        make = coll_desc[:16] if len(coll_desc) >= 16 else coll_desc.ljust(16)
        model = coll_desc[16:37] if len(coll_desc) >= 37 else ""
        regno = coll_desc[39:52] if len(coll_desc) >= 52 else ""
        
        processed_record = {
            'ACCTNO': record['ACCTNO'],
            'LOANTYPE': record['LOANTYPE'],
            'NTBRCH': record['NTBRCH'],
            'BRABBR': brabbr,
            'CAC': cac,
            'MAKE': make,
            'MODEL': model,
            'REGNO': regno,
            'ENGINE': record.get('ENGINE', ''),
            'CHASSIS': record.get('CHASSIS', ''),
            'COLLYEAR': record.get('COLLYEAR', ''),
            'ARREAR': record.get('ARREAR', 0)
        }
        processed_records.append(processed_record)
    
    repo_processed_df = pa.Table.from_pylist(processed_records)
    
    # Step 7: Split into REPO and REPO1 based on conditions
    repo_filtered_records = []
    repo1_filtered_records = []
    
    for record in processed_records:
        if record['ARREAR'] >= 10:
            repo_filtered_records.append(record)
            if record['LOANTYPE'] in [983, 993]:
                repo1_filtered_records.append(record)
    
    repo_final_df = pa.Table.from_pylist(repo_filtered_records)
    repo1_final_df = pa.Table.from_pylist(repo1_filtered_records)
    
    print(f"REPO records (ARREAR >= 10): {len(repo_final_df)}")
    print(f"REPO1 records (LOANTYPE 983,993): {len(repo1_final_df)}")
    
    # Step 8: Sort by REGNO
    if len(repo_final_df) > 0:
        repo_sorted_query = """
        SELECT * FROM repo_final_df 
        ORDER BY REGNO
        """
        repo_final_df = conn.execute(repo_sorted_query).arrow()
    
    if len(repo1_final_df) > 0:
        repo1_sorted_query = """
        SELECT * FROM repo1_final_df 
        ORDER BY REGNO
        """
        repo1_final_df = conn.execute(repo1_sorted_query).arrow()
    
    # Step 9: Create fixed-width text output for REPO
    repotxt_file = output_path / "REPOTXT.txt"
    with open(repotxt_file, 'w') as f:
        # Write header for first record
        if len(repo_final_df) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING\n")
        
        # Write data records
        for i, record in enumerate(repo_final_df.to_pylist()):
            line = (f"{record['BRABBR']:3}"
                   f"{record['CAC']:20}"
                   f"{record['REGNO']:13}"
                   f"{record['MAKE']:16}"
                   f"{record['MODEL']:21}"
                   f"{record['ENGINE']:40}"
                   f"{record['CHASSIS']:40}"
                   f"{str(record['COLLYEAR'])[:4]:4}")
            f.write(line + '\n')
    
    print(f"Created REPOTXT file: {repotxt_file}")
    
    # Step 10: Create fixed-width text output for REPO1
    repotxt1_file = output_path / "REPOTXT1.txt"
    with open(repotxt1_file, 'w') as f:
        # Write header for first record
        if len(repo1_final_df) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")
        
        # Write data records
        for i, record in enumerate(repo1_final_df.to_pylist()):
            line = (f"{record['BRABBR']:3}"
                   f"{record['CAC']:20}"
                   f"{record['REGNO']:13}"
                   f"{record['MAKE']:16}"
                   f"{record['MODEL']:21}"
                   f"{record['ENGINE']:40}"
                   f"{record['CHASSIS']:40}"
                   f"{str(record['COLLYEAR'])[:4]:4}")
            f.write(line + '\n')
    
    print(f"Created REPOTXT1 file: {repotxt1_file}")
    
    # Step 11: Also save as Parquet and CSV for reference
    pq.write_table(repo_final_df, output_path / "REPO.parquet")
    csv.write_csv(repo_final_df, output_path / "REPO.csv")
    
    pq.write_table(repo1_final_df, output_path / "REPO1.parquet")
    csv.write_csv(repo1_final_df, output_path / "REPO1.csv")
    
    # Print summary statistics
    print(f"\nProcessing completed successfully!")
    print(f"Summary:")
    print(f"  Total LNNOTE records: {len(lnnote_df)}")
    print(f"  REPO records (ARREAR >= 10): {len(repo_final_df)}")
    print(f"  REPO1 records (983,993): {len(repo1_final_df)}")
    
    # Loan type distribution
    if len(repo_final_df) > 0:
        loantype_summary = {}
        for record in repo_final_df.to_pylist():
            lt = record['LOANTYPE']
            loantype_summary[lt] = loantype_summary.get(lt, 0) + 1
        
        print(f"\nLoan type distribution in REPO:")
        for lt, count in sorted(loantype_summary.items()):
            print(f"  {lt}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    main()
