# EIMREPOI_REPO_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from datetime import datetime, timedelta
import pyreadstat

def main():
    # Configuration using pathlib
    base_path = Path(".")
    loan_path = base_path / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI"
    arrear_path = base_path / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI"
    output_path = base_path / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI"
    
    # Create output directory if it doesn't exist
    output_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Calculate REPTDATE using current date minus 1 day
    reptdate = datetime.now() - timedelta(days=1)
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
    
    # Step 2: Read LNNOTE.sas7bdat with pyreadstat
    lnnote_file = loan_path / "lnnote.sas7bdat"
    print("Reading LNNOTE.sas7bdat...")
    lnnote_df, lnnote_meta = pyreadstat.read_sas7bdat(str(lnnote_file))
    print(f"LNNOTE records before filtering: {len(lnnote_df)}")
    
    # Limit to 1000 rows for testing
    lnnote_df = lnnote_df.head(1000)
    print(f"LNNOTE records limited to 1000 for testing: {len(lnnote_df)}")
    
    # Convert column names to lowercase for consistency
    lnnote_df.columns = lnnote_df.columns.str.lower()
    
    # Note: &HP macro variable would need to be defined - using common values as example
    hp_values = ['983', '993', '984', '994']  # Example HP loan types
    
    # Filter LNNOTE data
    lnnote_filtered = lnnote_df[
        (lnnote_df['loantype'].astype(str).isin(hp_values)) &
        (lnnote_df['balance'] > 0) &
        (~lnnote_df['borstat'].isin(['F', 'I', 'R']))
    ]
    
    print(f"LNNOTE records after filtering: {len(lnnote_filtered)}")
    
    # Step 3: Read NAME8.sas7bdat with pyreadstat
    name8_file = loan_path / "name8.sas7bdat"
    print("Reading NAME8.sas7bdat...")
    name8_df, name8_meta = pyreadstat.read_sas7bdat(str(name8_file))
    name8_df.columns = name8_df.columns.str.lower()
    print(f"NAME8 records: {len(name8_df)}")
    
    # Limit NAME8 to 1000 rows for testing
    name8_df = name8_df.head(1000)
    print(f"NAME8 records limited to 1000 for testing: {len(name8_df)}")
    
    # Step 4: Read ARREAR data from LOANTEMP.sas7bdat
    arrear_file = arrear_path / "loantemp.sas7bdat"
    print("Reading LOANTEMP.sas7bdat...")
    arrear_df, arrear_meta = pyreadstat.read_sas7bdat(str(arrear_file))
    arrear_df.columns = arrear_df.columns.str.lower()
    print(f"ARREAR records: {len(arrear_df)}")
    
    # Limit ARREAR to 1000 rows for testing
    arrear_df = arrear_df.head(1000)
    print(f"ARREAR records limited to 1000 for testing: {len(arrear_df)}")
    
    # Step 5: Merge datasets using DuckDB for efficient joining
    # Register DataFrames with DuckDB
    conn.register('lnnote_filtered', lnnote_filtered)
    conn.register('name8_df', name8_df)
    conn.register('arrear_df', arrear_df)
    
    merge_query = f"""
    WITH merged_data AS (
        SELECT 
            COALESCE(l.acctno, n.acctno, a.acctno) as acctno,
            l.loantype,
            l.ntbrch,
            l.colldesc,
            l.collyear,
            n.linethre as engine,
            n.linefour as chassis,
            a.arrear
        FROM lnnote_filtered l
        LEFT JOIN name8_df n ON l.acctno = n.acctno
        LEFT JOIN arrear_df a ON l.acctno = a.acctno
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
        brabbr = str(record['ntbrch'])[:3] if record['ntbrch'] else "000"
        cac = f"BRANCH_{record['ntbrch']}" if record['ntbrch'] else "UNKNOWN"
        
        # Extract vehicle details from COLLDESC
        coll_desc = record.get('colldesc', '')
        if coll_desc is None:
            coll_desc = ''
        
        make = str(coll_desc)[:16] if len(str(coll_desc)) >= 16 else str(coll_desc).ljust(16)
        model = str(coll_desc)[16:37] if len(str(coll_desc)) >= 37 else ""
        regno = str(coll_desc)[39:52] if len(str(coll_desc)) >= 52 else ""
        
        # Handle None values
        engine = record.get('engine', '') or ''
        chassis = record.get('chassis', '') or ''
        collyear = record.get('collyear', '') or ''
        arrear = record.get('arrear', 0) or 0
        
        processed_record = {
            'acctno': record['acctno'],
            'loantype': record['loantype'],
            'ntbrch': record['ntbrch'],
            'brabbr': brabbr,
            'cac': cac,
            'make': make,
            'model': model,
            'regno': regno,
            'engine': engine,
            'chassis': chassis,
            'collyear': collyear,
            'arrear': arrear
        }
        processed_records.append(processed_record)
    
    repo_processed_df = pa.Table.from_pylist(processed_records)
    
    # Step 7: Split into REPO and REPO1 based on conditions
    repo_filtered_records = []
    repo1_filtered_records = []
    
    for record in processed_records:
        if record['arrear'] >= 10:
            repo_filtered_records.append(record)
            # Convert to int for comparison (handle potential string values)
            loantype_val = record['loantype']
            if loantype_val in [983, 993] or str(loantype_val) in ['983', '993']:
                repo1_filtered_records.append(record)
    
    repo_final_df = pa.Table.from_pylist(repo_filtered_records)
    repo1_final_df = pa.Table.from_pylist(repo1_filtered_records)
    
    print(f"REPO records (ARREAR >= 10): {len(repo_final_df)}")
    print(f"REPO1 records (LOANTYPE 983,993): {len(repo1_final_df)}")
    
    # Step 8: Sort by REGNO
    if len(repo_final_df) > 0:
        conn.register('repo_final_df', repo_final_df)
        repo_sorted_query = """
        SELECT * FROM repo_final_df 
        ORDER BY regno
        """
        repo_final_df = conn.execute(repo_sorted_query).arrow()
    
    if len(repo1_final_df) > 0:
        conn.register('repo1_final_df', repo1_final_df)
        repo1_sorted_query = """
        SELECT * FROM repo1_final_df 
        ORDER BY regno
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
            line = (f"{record['brabbr']:3}"
                   f"{record['cac']:20}"
                   f"{record['regno']:13}"
                   f"{record['make']:16}"
                   f"{record['model']:21}"
                   f"{record['engine']:40}"
                   f"{record['chassis']:40}"
                   f"{str(record['collyear'])[:4]:4}")
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
            line = (f"{record['brabbr']:3}"
                   f"{record['cac']:20}"
                   f"{record['regno']:13}"
                   f"{record['make']:16}"
                   f"{record['model']:21}"
                   f"{record['engine']:40}"
                   f"{record['chassis']:40}"
                   f"{str(record['collyear'])[:4]:4}")
            f.write(line + '\n')
    
    print(f"Created REPOTXT1 file: {repotxt1_file}")
    
    # Step 11: Also save as Parquet and CSV for reference
    if len(repo_final_df) > 0:
        pq.write_table(repo_final_df, output_path / "REPO.parquet")
        csv.write_csv(repo_final_df, output_path / "REPO.csv")
    
    if len(repo1_final_df) > 0:
        pq.write_table(repo1_final_df, output_path / "REPO1.parquet")
        csv.write_csv(repo1_final_df, output_path / "REPO1.csv")
    
    # Print summary statistics
    print(f"\nProcessing completed successfully!")
    print(f"Summary:")
    print(f"  Total LNNOTE records: {len(lnnote_filtered)}")
    print(f"  REPO records (ARREAR >= 10): {len(repo_final_df)}")
    print(f"  REPO1 records (983,993): {len(repo1_final_df)}")
    
    # Loan type distribution
    if len(repo_final_df) > 0:
        loantype_summary = {}
        for record in repo_final_df.to_pylist():
            lt = record['loantype']
            loantype_summary[lt] = loantype_summary.get(lt, 0) + 1
 
        print(f"\nLoan type distribution in REPO:")
        for lt, count in sorted(loantype_summary.items()):
            print(f"  {lt}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    main()
