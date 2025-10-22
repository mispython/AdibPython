# EIBDLIME_CRMS_LIMIT_EXTRACTOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime, timedelta
import os

def main():
    """
    EIBDLIME - CRMS & PBB Limit Data Warehouse Extractor
    Extracts, processes, and distributes limit information for CRMS and PBB data warehouse systems
    """
    
    # Configuration using pathlib
    base_path = Path(".")
    dplimit_path = base_path / "DPLIMIT"
    depos_path = base_path / "DEPOS" 
    dpcbr_path = base_path / "DPCBR"
    limit_path = base_path / "LIMIT"
    crms_output_path = base_path / "CRMS_LMDATAWH"
    pbb_output_path = base_path / "PBB_LMDATAWH"
    
    # Create output directories
    limit_path.mkdir(exist_ok=True)
    crms_output_path.mkdir(parents=True, exist_ok=True)
    pbb_output_path.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    print("EIBDLIME - CRMS & PBB Limit Data Warehouse Extractor")
    print("=" * 70)
    
    # Step 1: Clean up previous outputs (equivalent to DELETE step)
    print("Cleaning up previous output files...")
    for output_file in crms_output_path.glob("*"):
        if output_file.is_file():
            output_file.unlink()
    for output_file in pbb_output_path.glob("*"):
        if output_file.is_file():
            output_file.unlink()
    
    # Step 2: Process REPTDATE and calculate date parameters
    print("Processing reporting date parameters...")
    
    reptdate_query = f"""
    SELECT * FROM read_parquet('{dplimit_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        rept_day = reptdate.day
        rept_month = reptdate.month
        rept_year = reptdate.year
        
        # Calculate week parameters based on day of month
        if rept_day == 8:
            sdd, wk, wk1 = 1, '1', '4'
        elif rept_day == 15:
            sdd, wk, wk1 = 9, '2', '1'
        elif rept_day == 22:
            sdd, wk, wk1 = 16, '3', '2'
        else:
            sdd, wk, wk1 = 23, '4', '3'
        
        # Calculate month parameters
        if wk == '1':
            mm1 = rept_month - 1
            if mm1 == 0:
                mm1 = 12
        else:
            mm1 = rept_month
        
        # Calculate SDATE
        sdate = datetime(rept_year, rept_month, sdd)
        
        # Format date parameters
        rept_mon = f"{rept_month:02d}"
        rept_mon1 = f"{mm1:02d}"
        rept_year_short = reptdate.strftime('%y')
        rept_day_str = f"{rept_day:02d}"
        rdate = reptdate.strftime('%y%m%d')
        sdate_str = sdate.strftime('%d%m%y')
        rept_dt = reptdate.strftime('%Y%m%d')
        
        print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
        print(f"Week: {wk}, Previous Week: {wk1}")
        print(f"Month: {rept_mon}, Previous Month: {rept_mon1}")
        print(f"RDate: {rdate}, ReportDT: {rept_dt}")
        
    else:
        raise ValueError("No data found in DPLIMIT.REPTDATE")
    
    # Save processed REPTDATE
    reptdate_output = reptdate_df.select(['REPTDATE'])
    pq.write_table(reptdate_output, limit_path / "REPTDATE.parquet")
    csv.write_csv(reptdate_output, limit_path / "REPTDATE.csv")
    
    # Step 3: Process OVERDFT and CURRENT data
    print("Processing overdraft and current account data...")
    
    # Sort OVERDFT data
    odlm_query = f"""
    SELECT * FROM read_parquet('{dplimit_path / "OVERDFT.parquet"}')
    ORDER BY ACCTNO
    """
    odlm_df = conn.execute(odlm_query).arrow()
    
    # Sort CURRENT data with selected columns
    curr_query = f"""
    SELECT ACCTNO, CURBAL, OMNILOAN, OMNITRDB 
    FROM read_parquet('{depos_path / "CURRENT.parquet"}')
    ORDER BY ACCTNO
    """
    curr_df = conn.execute(curr_query).arrow()
    
    # Merge OVERDFT with CURRENT data
    odlm_merged_query = """
    SELECT 
        o.*,
        COALESCE(c.CURBAL, 0) as CURBAL,
        c.OMNILOAN,
        c.OMNITRDB,
        CASE 
            WHEN o.LMTRATE > 0 THEN (o.INTINCGR * o.LMTADJF) / o.LMTRATE
            ELSE 0 
        END as INTINCNT
    FROM odlm_df o
    LEFT JOIN curr_df c ON o.ACCTNO = c.ACCTNO
    WHERE o.ACCTNO IS NOT NULL
    """
    odlm_merged = conn.execute(odlm_merged_query).arrow()
    
    # Step 4: Process DPCBR fixed-width data
    print("Processing CBR income data...")
    
    # Read fixed-width DPCBR file
    dpcbr_file = dpcbr_path / "CORDATA.txt"
    if dpcbr_file.exists():
        # Read as fixed-width text file
        dpcbr_query = f"""
        WITH raw_data AS (
            SELECT 
                SUBSTR(line, 1, 6) as ACCTNO_STR,
                SUBSTR(line, 35, 6) as INTINCME_STR,
                SUBSTR(line, 41, 6) as COMMTFEE_STR
            FROM read_csv('{dpcbr_file}', header=false, columns={{'line': 'VARCHAR'}})
            WHERE LENGTH(line) >= 46
        )
        SELECT 
            CAST(ACCTNO_STR as INTEGER) as ACCTNO,
            CAST(INTINCME_STR as DOUBLE) as INTINCME,
            CAST(COMMTFEE_STR as DOUBLE) as COMMTFEE
        FROM raw_data
        WHERE ACCTNO_STR ~ '^[0-9]+$'
        """
        odf_df = conn.execute(dpcbr_query).arrow()
        
        # Sort by ACCTNO
        odf_sorted_query = """
        SELECT * FROM odf_df ORDER BY ACCTNO
        """
        odf_df = conn.execute(odf_sorted_query).arrow()
    else:
        # Create empty dataframe if file doesn't exist
        odf_df = pa.table({
            'ACCTNO': pa.array([], pa.int32()),
            'INTINCME': pa.array([], pa.float64()),
            'COMMTFEE': pa.array([], pa.float64())
        })
    
    # Step 5: Merge and process final limit data
    print("Creating final limit datasets...")
    
    # Define the columns to keep (equivalent to LIMITVAR macro)
    limit_columns = [
        'ACCTNO', 'BRANCH', 'LMTID', 'LMTDESC', 'LMTINDEX', 'LMTBASER',
        'LMTADJF', 'LMTRATE', 'LMTTYPE', 'LMTAMT', 'LMTCOLL',
        'PRODUCT', 'OPENIND', 'STATE', 'RISKCODE', 'EXCESSDT', 'TODDATE',
        'ODSTATUS', 'APPRLIMT', 'REVIEWDT', 'LMTSTART', 'LMTENDDT',
        'LMTTERM', 'LMTTRMID', 'LMTINCR', 'ODTEMADJ', 'ODFEECTD',
        'COMMTFEE', 'INTINCME', 'CURBAL', 'ODFDIND', 'FDACNO', 'FDRCNO',
        'COMMFERT', 'LINEFEWV', 'LFEEYTD', 'INTBLYTD', 'INTINCGR',
        'ODDATE', 'ODLMTDTE', 'INTINCNT', 'AVRCRBAL', 'AUTOPRIC',
        'REFIN_FLG', 'OLD_FI_CD', 'OLD_MACC_NO', 'OLD_SUBACC_NO',
        'CLIMATE_PRIN_TAXONOMY_CLASS', 'CRISPURP', 'OMNILOAN', 'OMNITRDB'
    ]
    
    # Merge ODF with ODLM data
    final_query = f"""
    WITH merged_data AS (
        SELECT 
            COALESCE(o.ACCTNO, d.ACCTNO) as ACCTNO,
            COALESCE(d.INTINCME, 0.00) as INTINCME,
            COALESCE(d.COMMTFEE, 0.00) as COMMTFEE,
            o.* EXCLUDE (ACCTNO, INTINCME, COMMTFEE)
        FROM odlm_merged o
        LEFT JOIN odf_df d ON o.ACCTNO = d.ACCTNO
        WHERE o.ACCTNO IS NOT NULL
    ),
    processed_data AS (
        SELECT *,
            CASE WHEN LMTAMT = 0 THEN '' ELSE AUTOPRIC END as AUTOPRIC_ADJ,
            
            -- Convert date fields from numeric to proper dates
            CASE WHEN EXCESSDT > 0 THEN 
                CAST(SUBSTR(CAST(EXCESSDT as VARCHAR), 1, 8) as DATE)
                ELSE NULL END as EXCESSDT_CONV,
                
            CASE WHEN TODDATE > 0 THEN 
                CAST(SUBSTR(CAST(TODDATE as VARCHAR), 1, 8) as DATE)
                ELSE NULL END as TODDATE_CONV,
                
            CASE WHEN REVIEWDT > 0 THEN 
                CAST(SUBSTR(CAST(REVIEWDT as VARCHAR), 1, 8) as DATE)
                ELSE NULL END as REVIEWDT_CONV,
                
            CASE WHEN LMTSTART > 0 THEN 
                CAST(SUBSTR(CAST(LMTSTART as VARCHAR), 1, 8) as DATE)
                ELSE NULL END as LMTSTART_CONV,
                
            CASE WHEN LMTENDDT > 0 THEN 
                CAST(SUBSTR(CAST(LMTENDDT as VARCHAR), 1, 8) as DATE)
                ELSE NULL END as LMTENDDT_CONV,
                
            -- Parse ODLMTDTE from DDMMYY8 format
            CASE WHEN ODLMTDTE IS NOT NULL THEN 
                CAST(
                    SUBSTR(CAST(ODLMTDTE as VARCHAR), 5, 4) || '-' ||
                    SUBSTR(CAST(ODLMTDTE as VARCHAR), 3, 2) || '-' ||
                    SUBSTR(CAST(ODLMTDTE as VARCHAR), 1, 2) 
                as DATE)
                ELSE NULL END as ODDATE_CONV
                
        FROM merged_data
        WHERE BRANCH != 0
    )
    SELECT 
        {', '.join([f'{col}' if col != 'AUTOPRIC' else 'AUTOPRIC_ADJ as AUTOPRIC' 
                   for col in limit_columns if col not in ['EXCESSDT', 'TODDATE', 'REVIEWDT', 'LMTSTART', 'LMTENDDT', 'ODDATE']])},
        EXCESSDT_CONV as EXCESSDT,
        TODDATE_CONV as TODDATE,
        REVIEWDT_CONV as REVIEWDT, 
        LMTSTART_CONV as LMTSTART,
        LMTENDDT_CONV as LMTENDDT,
        ODDATE_CONV as ODDATE
    FROM processed_data
    """
    
    final_df = conn.execute(final_query).arrow()
    
    # Step 6: Create output datasets
    print("Generating output files...")
    
    # Create LM_<REPTDT> dataset (CRMS format)
    lm_reptdt_file = f"LM_{rept_dt}"
    lm_reptdt_parquet = limit_path / f"{lm_reptdt_file}.parquet"
    lm_reptdt_csv = limit_path / f"{lm_reptdt_file}.csv"
    
    pq.write_table(final_df, lm_reptdt_parquet)
    csv.write_csv(final_df, lm_reptdt_csv)
    
    # Create LM<RDATE> dataset (PBB format)  
    lm_rdate_file = f"LM{rdate}"
    lm_rdate_parquet = limit_path / f"{lm_rdate_file}.parquet"
    lm_rdate_csv = limit_path / f"{lm_rdate_file}.csv"
    
    pq.write_table(final_df, lm_rdate_parquet)
    csv.write_csv(final_df, lm_rdate_csv)
    
    # Step 7: Distribute to CRMS and PBB locations (equivalent to PROC CPORT)
    print("Distributing files to CRMS and PBB locations...")
    
    # Copy to CRMS location
    crms_lm_parquet = crms_output_path / f"{lm_reptdt_file}.parquet"
    crms_lm_csv = crms_output_path / f"{lm_reptdt_file}.csv"
    pq.write_table(final_df, crms_lm_parquet)
    csv.write_csv(final_df, crms_lm_csv)
    
    # Copy to PBB location
    pbb_lm_parquet = pbb_output_path / f"{lm_rdate_file}.parquet"
    pbb_lm_csv = pbb_output_path / f"{lm_rdate_file}.csv"
    pq.write_table(final_df, pbb_lm_parquet)
    csv.write_csv(final_df, pbb_lm_csv)
    
    # Step 8: Print summary statistics
    print("\n" + "=" * 70)
    print("EXTRACTION SUMMARY - CRMS & PBB LIMIT DATA")
    print("=" * 70)
    print(f"Processing Date: {reptdate.strftime('%Y-%m-%d')}")
    print(f"Total Limit Records: {len(final_df):,}")
    print(f"CRMS Output: {lm_reptdt_file} ({len(final_df):,} records)")
    print(f"PBB Output: {lm_rdate_file} ({len(final_df):,} records)")
    
    # Branch distribution
    branch_summary = conn.execute("""
    SELECT BRANCH, COUNT(*) as ACCOUNT_COUNT, 
           SUM(LMTAMT) as TOTAL_LIMIT_AMOUNT
    FROM final_df 
    GROUP BY BRANCH 
    ORDER BY ACCOUNT_COUNT DESC
    LIMIT 10
    """).arrow()
    
    print(f"\nTop 10 Branches by Account Count:")
    for i in range(len(branch_summary)):
        branch = branch_summary['BRANCH'][i].as_py()
        count = branch_summary['ACCOUNT_COUNT'][i].as_py()
        amount = branch_summary['TOTAL_LIMIT_AMOUNT'][i].as_py()
        print(f"  Branch {branch}: {count:,} accounts, Limit: {amount:,.2f}")
    
    # Product type summary
    product_summary = conn.execute("""
    SELECT PRODUCT, COUNT(*) as COUNT, 
           AVG(LMTAMT) as AVG_LIMIT_AMOUNT
    FROM final_df 
    GROUP BY PRODUCT 
    ORDER BY COUNT DESC
    LIMIT 5
    """).arrow()
    
    print(f"\nTop 5 Product Types:")
    for i in range(len(product_summary)):
        product = product_summary['PRODUCT'][i].as_py()
        count = product_summary['COUNT'][i].as_py()
        avg_amt = product_summary['AVG_LIMIT_AMOUNT'][i].as_py()
        print(f"  {product}: {count:,} accounts, Avg Limit: {avg_amt:,.2f}")
    
    conn.close()
    
    print(f"\n✓ CRMS & PBB Limit Extraction Completed Successfully!")
    print(f"✓ Files distributed to:")
    print(f"  - CRMS: {crms_output_path}")
    print(f"  - PBB: {pbb_output_path}")

if __name__ == "__main__":
    main()
