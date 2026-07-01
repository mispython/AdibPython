# EIBDMSFX_NLF_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd

def main():
    # Configuration
    output_base_path = "MISFX"
    final_base_path = "FINAL"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_base_path, exist_ok=True)
    
    # Step 1: Get current date parameters
    # Use today's date or a specific date - adjust as needed
    # For SAS dates: use the actual date you want to process
    # If you want to use today's date:
    reptdate = datetime.now()  # or use datetime(2025, 7, 1) for a specific date
    
    # Calculate SAS date (days since 1960-01-01)
    sas_base_date = datetime(1960, 1, 1)
    sas_date_num = (reptdate - sas_base_date).days
    
    rept_year = reptdate.year
    rept_month = str(reptdate.month).zfill(2)
    rept_day = str(reptdate.day).zfill(2)
    rdate = str(reptdate.strftime('%j')).zfill(3)  # Julian day as Z3 format
    
    print(f"Processing date: {reptdate}")
    print(f"SAS date number: {sas_date_num}")
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
            
            # Check if DATE column exists
            if 'DATE' in df.columns:
                # Filter by SAS date number
                df_filtered = df[df['DATE'] == sas_date_num]
            else:
                print(f"Warning: DATE column not found in {name}")
                continue
            
            if len(df_filtered) == 0:
                print(f"No data found for SAS date {sas_date_num} ({reptdate}) in {name}")
                continue
            
            # Register as DuckDB table
            conn.register(name, df_filtered)
            loaded_tables.append(name)
            print(f"Loaded {len(df_filtered)} records from {name} for SAS date {sas_date_num}")
            
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    
    # Check if we have any data to process
    if not loaded_tables:
        print(f"\nNo data found for SAS date {sas_date_num} ({reptdate}) across all sources")
        print("Available SAS dates in your files:")
        
        # Show what dates are available
        for name, filepath in sas_files.items():
            if os.path.exists(filepath):
                try:
                    df, meta = pyreadstat.read_sas7bdat(filepath)
                    if 'DATE' in df.columns:
                        unique_dates = sorted(df['DATE'].unique())
                        print(f"\n{name}:")
                        print(f"  Total records: {len(df)}")
                        print(f"  Date range: {unique_dates[0]} to {unique_dates[-1]}")
                        print(f"  Available dates: {unique_dates[:10]}{'...' if len(unique_dates) > 10 else ''}")
                        # Convert to readable dates
                        print(f"  Date range (readable): {sas_to_date(unique_dates[0])} to {sas_to_date(unique_dates[-1])}")
                except:
                    pass
        
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
        WHERE DATE = {sas_date_num}
        """)
    
    if 'behavenonfxfd' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            CAST(NULL AS DOUBLE) as INDFXFDBAL,
            BALANCE as NONFXFDBAL,
            CAST(NULL AS DOUBLE) as INDFXCABAL,
            CAST(NULL AS DOUBLE) as NONFXCABAL
        FROM behavenonfxfd
        WHERE DATE = {sas_date_num}
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
        WHERE DATE = {sas_date_num}
        """)
    
    if 'behavenonfxca' in loaded_tables:
        union_queries.append(f"""
        SELECT 
            DATE as REPTDATE,
            CAST(NULL AS DOUBLE) as INDFXFDBAL,
            CAST(NULL AS DOUBLE) as NONFXFDBAL,
            CAST(NULL AS DOUBLE) as INDFXCABAL,
            BALANCE as NONFXCABAL
        FROM behavenonfxca
        WHERE DATE = {sas_date_num}
        """)
    
    if not union_queries:
        print(f"No data found for SAS date {sas_date_num} ({reptdate}) across all sources")
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
        print(f"No data found for SAS date {sas_date_num} ({reptdate}) across all sources")
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
                filter_query = f"""
                SELECT * FROM existing_data 
                WHERE REPTDATE != {sas_date_num}
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

def sas_to_date(sas_date_num):
    """Convert SAS date number to readable date"""
    base_date = datetime(1960, 1, 1)
    return base_date + timedelta(days=sas_date_num)

if __name__ == "__main__":
    main()
  
