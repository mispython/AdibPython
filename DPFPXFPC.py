# EIBDMSFX_NLF_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import os
from datetime import datetime, timedelta
import pyreadstat

def main():
    # Configuration
    output_base_path = "MISFX"
    final_base_path = "FINAL"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_base_path, exist_ok=True)
    
    # Step 1: Get current date parameters
    # Use today's date or a specific date - adjust as needed
    reptdate = datetime.now()  # or use datetime(2026, 7, 1) for a specific date
    rept_year = reptdate.year
    rept_month = str(reptdate.month).zfill(2)
    rept_day = str(reptdate.day).zfill(2)
    rdate = str(reptdate.strftime('%j')).zfill(3)  # Julian day as Z3 format
    
    print(f"Processing date: {reptdate}")
    print(f"Year: {rept_year}, Month: {rept_month}, Day: {rept_day}, RDate: {rdate}")
    
    # Step 2: Process NLF data from multiple SAS7BDAT sources
    conn = duckdb.connect()
    
    # Define SAS file paths (lowercase)
    sas_files = {
        'behaveindfxfd': f'{final_base_path}/behaveindfxfd.sas7bdat',
        'behavenonfxfd': f'{final_base_path}/behavenonfxfd.sas7bdat',
        'behaveindfxca': f'{final_base_path}/behaveindfxca.sas7bdat',
        'behavenonfxca': f'{final_base_path}/behavenonfxca.sas7bdat'
    }
    
    # Track which files have data
    loaded_tables = []
    
    # Create temporary tables for each SAS file
    for name, filepath in sas_files.items():
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found, skipping...")
            continue
            
        try:
            # Read SAS file using pyreadstat
            df, meta = pyreadstat.read_sas7bdat(filepath)
            
            # Check if DATE column exists and filter
            if 'DATE' in df.columns:
                # Convert DATE column to string for comparison with rdate
                df_filtered = df[df['DATE'].astype(str) == rdate]
            else:
                print(f"Warning: DATE column not found in {name}")
                continue
            
            if len(df_filtered) == 0:
                print(f"No data found for RDate {rdate} in {name}")
                continue
            
            # Register as DuckDB table
            conn.register(name, df_filtered)
            loaded_tables.append(name)
            print(f"Loaded {len(df_filtered)} records from {name}")
            
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    
    # Check if we have any data to process
    if not loaded_tables:
        print("No data found for the current date across all sources")
        conn.close()
        return
    
    # Step 3: Build dynamic UNION ALL query based on loaded tables
    union_queries = []
    
    if 'behaveindfxfd' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            BALANCE as INDFXFDBAL,
            CAST(NULL AS DOUBLE) as NONFXFDBAL,
            CAST(NULL AS DOUBLE) as INDFXCABAL, 
            CAST(NULL AS DOUBLE) as NONFXCABAL
        FROM behaveindfxfd
        WHERE CAST(DATE AS VARCHAR) = '{rdate}'
        """)
    
    if 'behavenonfxfd' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            CAST(NULL AS DOUBLE) as INDFXFDBAL,
            BALANCE as NONFXFDBAL,
            CAST(NULL AS DOUBLE) as INDFXCABAL,
            CAST(NULL AS DOUBLE) as NONFXCABAL
        FROM behave nonfxfd
        WHERE CAST(DATE AS VARCHAR) = '{rdate}'
        """)
    
    if 'behaveindfxca' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            CAST(NULL AS DOUBLE) as INDFXFDBAL,
            CAST(NULL AS DOUBLE) as NONFXFDBAL,
            BALANCE as INDFXCABAL,
            CAST(NULL AS DOUBLE) as NONFXCABAL
        FROM behaveindfxca
        WHERE CAST(DATE AS VARCHAR) = '{rdate}'
        """)
    
    if 'behavenonfxca' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            CAST(NULL AS DOUBLE) as INDFXFDBAL,
            CAST(NULL AS DOUBLE) as NONFXFDBAL,
            CAST(NULL AS DOUBLE) as INDFXCABAL,
            BALANCE as NONFXCABAL
        FROM behave nonfxca
        WHERE CAST(DATE AS VARCHAR) = '{rdate}'
        """)
    
    if not union_queries:
        print("No data found for the current date across all sources")
        conn.close()
        return
    
    # Combine all queries with UNION ALL
    nlf_query = " UNION ALL ".join(union_queries)
    
    try:
        nlf_df = conn.execute(nlf_query).arrow()
    except Exception as e:
        print(f"Error executing query: {e}")
        print(f"Query: {nlf_query}")
        conn.close()
        return
    
    if len(nlf_df) == 0:
        print("No data found for the current date across all sources")
        conn.close()
        return
    
    # Step 4: Summarize data (equivalent to PROC SUMMARY)
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
    
    # Step 5: Append logic (equivalent to MACRO APPEND)
    output_filename = f"NLF{rept_month}"
    output_parquet_path = f"{output_base_path}/{output_filename}.parquet"
    output_sas_path = f"{output_base_path}/{output_filename}.sas7bdat"
    
    if rept_day == "01":
        # First day of month - create new files
        pq.write_table(nlf_summary, output_parquet_path)
        # Convert to pandas for SAS export
        df_output = nlf_summary.to_pandas()
        pyreadstat.write_sas7bdat(df_output, output_sas_path)
        print(f"Created new files: {output_parquet_path}, {output_sas_path}")
    else:
        # Other days - append to existing file after removing existing record for the date
        if os.path.exists(output_parquet_path):
            try:
                # Read existing data
                existing_data = pq.read_table(output_parquet_path)
                
                # Filter out existing record for the current date
                # Convert rdate to integer for comparison
                rdate_int = int(rdate)
                filter_query = f"""
                SELECT * FROM existing_data 
                WHERE REPTDATE != {rdate_int}
                """
                filtered_data = conn.execute(filter_query).arrow()
                
                # Combine filtered existing data with new data
                combined_data = pa.concat_tables([filtered_data, nlf_summary])
                
                # Write combined data
                pq.write_table(combined_data, output_parquet_path)
                
                # Convert to pandas for SAS export
                df_output = combined_data.to_pandas()
                pyreadstat.write_sas7bdat(df_output, output_sas_path)
                print(f"Updated files: {output_parquet_path}, {output_sas_path}")
            except Exception as e:
                print(f"Error updating existing files: {e}")
                # Fallback: create new file
                pq.write_table(nlf_summary, output_parquet_path)
                df_output = nlf_summary.to_pandas()
                pyreadstat.write_sas7bdat(df_output, output_sas_path)
                print(f"Created new files (fallback): {output_parquet_path}, {output_sas_path}")
        else:
            # File doesn't exist, create new one
            pq.write_table(nlf_summary, output_parquet_path)
            df_output = nlf_summary.to_pandas()
            pyreadstat.write_sas7bdat(df_output, output_sas_path)
            print(f"Created new files: {output_parquet_path}, {output_sas_path}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Records processed: {len(nlf_summary)}")
    
    # Calculate totals safely (handling NULLs)
    totals = {}
    for col in ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']:
        col_data = nlf_summary.column(col).combine_chunks().to_pandas()
        total = col_data.sum() if len(col_data) > 0 else 0
        totals[col] = total
        print(f"{col} Total: {total:.2f}")
    
    conn.close()
    print(f"\nProcessing complete. Output saved to: {output_base_path}/")

if __name__ == "__main__":
    main()
