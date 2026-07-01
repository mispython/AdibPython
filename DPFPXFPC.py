# EIBDMSFX_NLF_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import os
from datetime import datetime

def main():
    # Configuration
    input_path = "MNITB/REPTDATE.parquet"
    output_base_path = "MISFX"
    final_base_path = "FINAL"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_base_path, exist_0)
    
    # Step 1: Process REPTDATE and extract parameters
    conn = duckdb.connect()
    
    # Read REPTDATE data
    reptdate_query = f"""
    SELECT * FROM read_parquet('{input_path}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    # Extract date parameters (assuming single row in REPTDATE)
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        rept_year = reptdate.year
        rept_month = str(reptdate.month).zfill(2)
        rept_day = str(reptdate.day).zfill(2)
        rdate = str(reptdate.strftime('%j')).zfill(3)  # Julian day as Z3 format
        
        print(f"Processing date: {reptdate}")
        print(f"Year: {rept_year}, Month: {rept_month}, Day: {rept_day}, RDate: {rdate}")
    else:
        raise ValueError("No data found in REPTDATE table")
    
    # Step 2: Process NLF data from multiple sources
    nlf_query = f"""
    SELECT 
        DATE as REPTDATE,
        BALANCE as INDFXFDBAL,
        CAST(NULL AS DOUBLE) as NONFXFDBAL,
        CAST(NULL AS DOUBLE) as INDFXCABAL, 
        CAST(NULL AS DOUBLE) as NONFXCABAL
    FROM read_parquet('{final_base_path}/BEHAVEINDFXFD.parquet')
    WHERE DATE = {rdate}
    
    UNION ALL
    
    SELECT 
        DATE as REPTDATE,
        CAST(NULL AS DOUBLE) as INDFXFDBAL,
        BALANCE as NONFXFDBAL,
        CAST(NULL AS DOUBLE) as INDFXCABAL,
        CAST(NULL AS DOUBLE) as NONFXCABAL
    FROM read_parquet('{final_base_path}/BEHAVENONFXFD.parquet') 
    WHERE DATE = {rdate}
    
    UNION ALL
    
    SELECT 
        DATE as REPTDATE,
        CAST(NULL AS DOUBLE) as INDFXFDBAL,
        CAST(NULL AS DOUBLE) as NONFXFDBAL,
        BALANCE as INDFXCABAL,
        CAST(NULL AS DOUBLE) as NONFXCABAL
    FROM read_parquet('{final_base_path}/BEHAVEINDFXCA.parquet')
    WHERE DATE = {rdate}
    
    UNION ALL
    
    SELECT 
        DATE as REPTDATE,
        CAST(NULL AS DOUBLE) as INDFXFDBAL,
        CAST(NULL AS DOUBLE) as NONFXFDBAL,
        CAST(NULL AS DOUBLE) as INDFXCABAL,
        BALANCE as NONFXCABAL
    FROM read_parquet('{final_base_path}/BEHAVENONFXCA.parquet')
    WHERE DATE = {rdate}
    """
    
    nlf_df = conn.execute(nlf_query).arrow()
    
    # Step 3: Summarize data (equivalent to PROC SUMMARY)
    summary_query = """
    SELECT 
        REPTDATE,
        SUM(INDFXFDBAL) as INDFXFDBAL,
        SUM(NONFXFDBAL) as NONFXFDBAL, 
        SUM(INDFXCABAL) as INDFXCABAL,
        SUM(NONFXCABAL) as NONFXCABAL
    FROM nlf_df
    GROUP BY REPTDATE
    """
    
    nlf_summary = conn.execute(summary_query).arrow()
    
    # Step 4: Append logic (equivalent to MACRO APPEND)
    output_filename = f"NLF{rept_month}"
    output_parquet_path = f"{output_base_path}/{output_filename}.parquet"
    output_csv_path = f"{output_base_path}/{output_filename}.csv"
    
    if rept_day == "01":
        # First day of month - create new file
        pq.write_table(nlf_summary, output_parquet_path)
        csv.write_csv(nlf_summary, output_csv_path)
        print(f"Created new files: {output_parquet_path}, {output_csv_path}")
    else:
        # Other days - append to existing file after removing existing record for the date
        if os.path.exists(output_parquet_path):
            # Read existing data
            existing_data = pq.read_table(output_parquet_path)
            
            # Filter out existing record for the current date
            filter_query = f"""
            SELECT * FROM existing_data 
            WHERE REPTDATE != {rdate}
            """
            filtered_data = conn.execute(filter_query).arrow()
            
            # Combine filtered existing data with new data
            combined_data = pa.concat_tables([filtered_data, nlf_summary])
            
            # Write combined data
            pq.write_table(combined_data, output_parquet_path)
            csv.write_csv(combined_data, output_csv_path)
            print(f"Updated files: {output_parquet_path}, {output_csv_path}")
        else:
            # File doesn't exist, create new one
            pq.write_table(nlf_summary, output_parquet_path)
            csv.write_csv(nlf_summary, output_csv_path)
            print(f"Created new files: {output_parquet_path}, {output_csv_path}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Records processed: {len(nlf_summary)}")
    print(f"INDFXFDBAL Total: {nlf_summary.column('INDFXFDBAL').combine_chunks().to_pandas().sum()}")
    print(f"NONFXFDBAL Total: {nlf_summary.column('NONFXFDBAL').combine_chunks().to_pandas().sum()}")
    print(f"INDFXCABAL Total: {nlf_summary.column('INDFXCABAL').combine_chunks().to_pandas().sum()}")
    print(f"NONFXCABAL Total: {nlf_summary.column('NONFXCABAL').combine_chunks().to_pandas().sum()}")
    
    conn.close()

if __name__ == "__main__":
    main()
