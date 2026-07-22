# EIMREPOI_REPO_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd

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
    # In SAS, REPTDATE is from LOAN.REPTDATE - we use yesterday
    reptdate = datetime.now() - timedelta(days=1)
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year
    
    # Implement SELECT(DAY(REPTDATE)) logic from SAS
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
    
    # Calculate MM1 (SAS logic)
    if wk == '1':
        mm1 = month - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = month
    
    # Calculate SDATE
    sdate = datetime(year, month, sdd)
    
    # Set macro variables equivalent (SAS SYMPUT)
    nowk = wk
    nowk1 = wk1
    reptmon = f"{month:02d}"
    reptmon1 = f"{mm1:02d}"
    reperyear = str(year)
    reptday = f"{day:02d}"
    rdate = reptdate.strftime('%d%m%y')  # DDMMYY format
    sdate_str = sdate.strftime('%d%m%y')
    
    print(f"Processing date: {reptdate}")
    print(f"Week: {nowk}, Previous Week: {nowk1}")
    print(f"Month: {reptmon}, Previous Month: {reptmon1}")
    print(f"RDate: {rdate}, SDate: {sdate_str}")
    
    # Step 2: Read and process LNNOTE with filters (SAS PROC SORT with WHERE)
    lnnote_file = loan_path / "lnnote.sas7bdat"
    print("Reading LNNOTE.sas7bdat...")
    lnnote_df, lnnote_meta = pyreadstat.read_sas7bdat(str(lnnote_file))
    print(f"LNNOTE records before filtering: {len(lnnote_df)}")
    
    # Convert column names to lowercase
    lnnote_df.columns = lnnote_df.columns.str.lower()
    
    # HP loan types - these would come from &HP macro variable in SAS
    hp_values = ['983', '993', '984', '994']
    
    # Apply SAS WHERE conditions: LOANTYPE IN &HP AND BALANCE GT 0 AND BORSTAT NOT IN ('F','I','R')
    lnnote_filtered = lnnote_df[
        (lnnote_df['loantype'].astype(str).isin(hp_values)) &
        (lnnote_df['balance'] > 0) &
        (~lnnote_df['borstat'].isin(['F', 'I', 'R']))
    ]
    
    # Keep only needed columns (KEEP=ACCTNO LOANTYPE NTBRCH COLLDESC COLLYEAR)
    lnnote_filtered = lnnote_filtered[['acctno', 'loantype', 'ntbrch', 'colldesc', 'collyear']]
    
    print(f"LNNOTE records after filtering: {len(lnnote_filtered)}")
    
    # Step 3: Read NAME8 (KEEP=ACCTNO LINETHRE LINEFOUR)
    name8_file = loan_path / "name8.sas7bdat"
    print("Reading NAME8.sas7bdat...")
    name8_df, name8_meta = pyreadstat.read_sas7bdat(str(name8_file))
    name8_df.columns = name8_df.columns.str.lower()
    
    # Keep only needed columns
    name8_df = name8_df[['acctno', 'linethre', 'linefour']]
    # Rename columns (SAS RENAME=(LINETHRE=ENGINE LINEFOUR=CHASSIS))
    name8_df = name8_df.rename(columns={'linethre': 'engine', 'linefour': 'chassis'})
    
    print(f"NAME8 records: {len(name8_df)}")
    
    # Step 4: Read ARREAR (KEEP=ACCTNO ARREAR)
    arrear_file = arrear_path / "loantemp.sas7bdat"
    print("Reading LOANTEMP.sas7bdat...")
    arrear_df, arrear_meta = pyreadstat.read_sas7bdat(str(arrear_file))
    arrear_df.columns = arrear_df.columns.str.lower()
    
    # Keep only needed columns
    arrear_df = arrear_df[['acctno', 'arrear']]
    
    print(f"ARREAR records: {len(arrear_df)}")
    
    # Step 5: Merge datasets (SAS DATA REPO with MERGE and BY ACCTNO)
    # Merge LNNOTE(IN=AA) with NAME8 and ARREAR
    repo_df = pd.merge(
        lnnote_filtered, 
        name8_df, 
        on='acctno', 
        how='left',  # LEFT JOIN (IN=AA ensures only LNNOTE records kept)
        indicator=True
    )
    
    # Keep only records from LNNOTE (IN=AA condition)
    repo_df = repo_df[repo_df['_merge'] == 'left_only']
    repo_df = repo_df.drop(columns=['_merge'])
    
    # Merge with ARREAR
    repo_df = pd.merge(
        repo_df, 
        arrear_df, 
        on='acctno', 
        how='left'
    )
    
    print(f"Merged REPO records: {len(repo_df)}")
    
    # Step 6: Process REPO data - add derived fields (SAS DATA REPO step)
    # In SAS: BRABBR = PUT(NTBRCH,BRCHCD.);
    # CAC = PUT(NTBRCH,CACNAME.);
    # MAKE = SUBSTR(COLLDESC,1,16);  - SAS uses 1-indexed
    # MODEL = SUBSTR(COLLDESC,16,21); - length 21 starting at position 16
    # REGNO = SUBSTR(COLLDESC,40,13); - length 13 starting at position 40
    
    # Note: Need format lookup tables for BRCHCD and CACNAME
    # For now, using placeholder logic
    processed_records = []
    
    for _, record in repo_df.iterrows():
        # BRABBR - would need format lookup, using NTBRCH as placeholder
        ntbrch_val = record['ntbrch'] if pd.notna(record['ntbrch']) else None
        # In production, you'd have a format lookup dictionary
        brabbr = f"BR{str(ntbrch_val)[:3]}" if ntbrch_val else "000"
        
        # CAC - would need format lookup
        cac = f"CAC{str(ntbrch_val)[:5]}" if ntbrch_val else "UNKNOWN"
        
        # Extract vehicle details from COLLDESC (SAS 1-indexed positions)
        coll_desc = str(record['colldesc']) if pd.notna(record['colldesc']) else ''
        
        # SAS SUBSTR(COLLDESC,1,16) - positions 1 to 16 (length 16)
        make = coll_desc[0:16] if len(coll_desc) >= 16 else coll_desc.ljust(16)
        
        # SAS SUBSTR(COLLDESC,16,21) - starting at position 16, length 21
        # In Python (0-indexed): start at 15, take 21 characters
        model = coll_desc[15:36] if len(coll_desc) >= 36 else coll_desc[15:] if len(coll_desc) > 15 else ''
        
        # SAS SUBSTR(COLLDESC,40,13) - starting at position 40, length 13
        # In Python (0-indexed): start at 39, take 13 characters
        regno = coll_desc[39:52] if len(coll_desc) >= 52 else coll_desc[39:] if len(coll_desc) > 39 else ''
        
        # Handle None values
        engine = str(record['engine']) if pd.notna(record['engine']) else ''
        chassis = str(record['chassis']) if pd.notna(record['chassis']) else ''
        collyear = str(record['collyear'])[:4] if pd.notna(record['collyear']) else ''
        arrear = float(record['arrear']) if pd.notna(record['arrear']) else 0
        
        processed_record = {
            'acctno': record['acctno'],
            'loantype': record['loantype'],
            'ntbrch': ntbrch_val,
            'brabbr': brabbr[:3],  # Ensure 3 characters
            'cac': cac[:20],       # Ensure 20 characters
            'make': make[:16],     # Ensure 16 characters
            'model': model[:21],   # Ensure 21 characters
            'regno': regno[:13],   # Ensure 13 characters
            'engine': engine[:40], # Ensure 40 characters
            'chassis': chassis[:40], # Ensure 40 characters
            'collyear': collyear[:4], # Ensure 4 characters
            'arrear': arrear
        }
        processed_records.append(processed_record)
    
    repo_df_processed = pd.DataFrame(processed_records)
    
    # Step 7: Split into REPO and REPO1 (SAS DATA REPO REPO1)
    # First filter: IF ARREAR GE 10
    repo_filtered = repo_df_processed[repo_df_processed['arrear'] >= 10].copy()
    
    # REPO1: IF LOANTYPE IN (983,993)
    repo1_filtered = repo_filtered[
        (repo_filtered['loantype'].astype(str).isin(['983', '993'])) |
        (repo_filtered['loantype'].isin([983, 993]))
    ].copy()
    
    print(f"REPO records (ARREAR >= 10): {len(repo_filtered)}")
    print(f"REPO1 records (LOANTYPE 983,993): {len(repo1_filtered)}")
    
    # Step 8: Sort by REGNO (PROC SORT BY REGNO)
    repo_filtered = repo_filtered.sort_values('regno').reset_index(drop=True)
    repo1_filtered = repo1_filtered.sort_values('regno').reset_index(drop=True)
    
    # Step 9: Create fixed-width text output for REPO
    # SAS DATA _NULL_ with FILE REPOTXT
    repotxt_file = output_path / "REPOTXT.txt"
    with open(repotxt_file, 'w') as f:
        # Write header for first record (IF _N_ = 1)
        if len(repo_filtered) > 0:
            f.write(f"{' ':<{1}}")  # Start at position 1 (SAS @001)
            f.write(f"{rdate}-REPOSSESSION LISTING\n")
        
        # Write data records with exact SAS column positions
        for _, record in repo_filtered.iterrows():
            # SAS PUT positions: @001 BRABBR $3. @009 CAC $20. @029 REGNO $13. 
            # @043 MAKE $16. @060 MODEL $21. @082 ENGINE $40. @123 CHASSIS $40. @164 COLLYEAR $4.
            # In Python, these are 0-indexed positions: 0, 8, 28, 42, 59, 81, 122, 163
            line = (' ' * 0)  # Start at position 1 (0-indexed position 0)
            line += f"{record['brabbr']:<3}"     # @001 (0-index:0)
            line += ' ' * 5                      # Padding to @009 (0-index:8)
            line += f"{record['cac']:<20}"       # @009 (0-index:8)
            line += ' ' * 7                      # Padding to @029 (0-index:28)
            line += f"{record['regno']:<13}"     # @029 (0-index:28)
            line += ' ' * 1                      # Padding to @043 (0-index:42)
            line += f"{record['make']:<16}"      # @043 (0-index:42)
            line += ' ' * 0                      # Padding to @060 (0-index:59)
            line += f"{record['model']:<21}"     # @060 (0-index:59)
            line += ' ' * 1                      # Padding to @082 (0-index:81)
            line += f"{record['engine']:<40}"    # @082 (0-index:81)
            line += ' ' * 1                      # Padding to @123 (0-index:122)
            line += f"{record['chassis']:<40}"   # @123 (0-index:122)
            line += ' ' * 1                      # Padding to @164 (0-index:163)
            line += f"{record['collyear']:<4}"   # @164 (0-index:163)
            f.write(line + '\n')
    
    print(f"Created REPOTXT file: {repotxt_file}")
    
    # Step 10: Create fixed-width text output for REPO1
    repotxt1_file = output_path / "REPOTXT1.txt"
    with open(repotxt1_file, 'w') as f:
        # Write header for first record (IF _N_ = 1)
        if len(repo1_filtered) > 0:
            f.write(f"{' ':<{1}}")  # Start at position 1
            f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")
        
        # Write data records with exact SAS column positions
        for _, record in repo1_filtered.iterrows():
            line = (' ' * 0)  # Start at position 1 (0-indexed position 0)
            line += f"{record['brabbr']:<3}"     # @001 (0-index:0)
            line += ' ' * 5                      # Padding to @009 (0-index:8)
            line += f"{record['cac']:<20}"       # @009 (0-index:8)
            line += ' ' * 7                      # Padding to @029 (0-index:28)
            line += f"{record['regno']:<13}"     # @029 (0-index:28)
            line += ' ' * 1                      # Padding to @043 (0-index:42)
            line += f"{record['make']:<16}"      # @043 (0-index:42)
            line += ' ' * 0                      # Padding to @060 (0-index:59)
            line += f"{record['model']:<21}"     # @060 (0-index:59)
            line += ' ' * 1                      # Padding to @082 (0-index:81)
            line += f"{record['engine']:<40}"    # @082 (0-index:81)
            line += ' ' * 1                      # Padding to @123 (0-index:122)
            line += f"{record['chassis']:<40}"   # @123 (0-index:122)
            line += ' ' * 1                      # Padding to @164 (0-index:163)
            line += f"{record['collyear']:<4}"   # @164 (0-index:163)
            f.write(line + '\n')
    
    print(f"Created REPOTXT1 file: {repotxt1_file}")
    
    # Step 11: Also save as Parquet and CSV for reference
    if len(repo_filtered) > 0:
        repo_arrow = pa.Table.from_pandas(repo_filtered)
        pq.write_table(repo_arrow, output_path / "REPO.parquet")
        csv.write_csv(repo_arrow, output_path / "REPO.csv")
    
    if len(repo1_filtered) > 0:
        repo1_arrow = pa.Table.from_pandas(repo1_filtered)
        pq.write_table(repo1_arrow, output_path / "REPO1.parquet")
        csv.write_csv(repo1_arrow, output_path / "REPO1.csv")
    
    # Print summary statistics
    print(f"\nProcessing completed successfully!")
    print(f"Summary:")
    print(f"  Total LNNOTE records after filtering: {len(lnnote_filtered)}")
    print(f"  REPO records (ARREAR >= 10): {len(repo_filtered)}")
    print(f"  REPO1 records (983,993): {len(repo1_filtered)}")
    
    # Loan type distribution
    if len(repo_filtered) > 0:
        loantype_summary = repo_filtered['loantype'].value_counts()
        print(f"\nLoan type distribution in REPO:")
        for lt, count in loantype_summary.items():
            print(f"  {lt}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    main()
  
