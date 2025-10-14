# EIIDLNAP_LOAN_APPEND_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os

def main():
    # Configuration using pathlib
    base_path = Path(".")
    sasd_path = base_path / "SASD"
    app_path = base_path / "APP"
    
    # Create output directory if it doesn't exist
    app_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters
    reptdate_query = f"""
    SELECT * FROM read_parquet('{sasd_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        rept_year = reptdate.strftime('%y')
        rept_month = reptdate.strftime('%m')
        rept_day = reptdate.strftime('%d')
        nowk = '4'  # Hardcoded as per SAS program
        
        print(f"Processing date: {reptdate}")
        print(f"Year: {rept_year}, Month: {rept_month}, Day: {rept_day}")
        print(f"NOWK: {nowk}")
    else:
        raise ValueError("No data found in SASD.REPTDATE table")
    
    # Step 2: Implement the APPEND macro logic
    def append_macro():
        """Equivalent to SAS %APPEND macro"""
        output_filename = f"LOAN{rept_month}"
        output_file = app_path / f"{output_filename}.parquet"
        
        if rept_day == "01":
            # First day of the month - create new file
            print("First day of month - creating new LOAN file")
            
            input_filename = f"LOAN{rept_month}{rept_day}"
            input_file = sasd_path / f"{input_filename}.parquet"
            
            if input_file.exists():
                # Read source data and add DATE column
                loan_query = f"""
                SELECT 
                    ACCTNO, NOTENO, FISSPURP, PRODUCT, BALANCE, CENSUS, 
                    DNBFISME, PRODCD, CUSTCD, AMTIND, SECTORCD, BRANCH, 
                    ACCTYPE, CCY, FORATE,
                    DATE '{reptdate.strftime('%Y-%m-%d')}' as DATE
                FROM read_parquet('{input_file}')
                """
                
                loan_df = conn.execute(loan_query).arrow()
                
                # Save to APP directory
                pq.write_table(loan_df, output_file)
                csv.write_csv(loan_df, app_path / f"{output_filename}.csv")
                
                print(f"✓ Created new {output_filename} with {len(loan_df)} records")
            else:
                raise FileNotFoundError(f"Source file {input_filename}.parquet not found")
                
        else:
            # Other days - append to existing file
            print(f"Day {rept_day} of month - appending to existing LOAN file")
            
            input_filename = f"LOAN{rept_month}{rept_day}"
            input_file = sasd_path / f"{input_filename}.parquet"
            
            if not input_file.exists():
                raise FileNotFoundError(f"Source file {input_filename}.parquet not found")
            
            # Step 1: Remove existing records for current date from base file
            if output_file.exists():
                # Read existing data and filter out current date records
                existing_query = f"""
                SELECT * FROM read_parquet('{output_file}')
                WHERE DATE != DATE '{reptdate.strftime('%Y-%m-%d')}'
                """
                existing_df = conn.execute(existing_query).arrow()
                print(f"✓ Removed existing records for date {reptdate} from base file")
            else:
                # Create empty dataframe if base file doesn't exist
                existing_df = pa.table({
                    'ACCTNO': pa.array([], pa.int64()),
                    'NOTENO': pa.array([], pa.int64()),
                    'FISSPURP': pa.array([], pa.string()),
                    'PRODUCT': pa.array([], pa.string()),
                    'BALANCE': pa.array([], pa.float64()),
                    'CENSUS': pa.array([], pa.string()),
                    'DNBFISME': pa.array([], pa.string()),
                    'PRODCD': pa.array([], pa.string()),
                    'CUSTCD': pa.array([], pa.string()),
                    'AMTIND': pa.array([], pa.string()),
                    'SECTORCD': pa.array([], pa.string()),
                    'BRANCH': pa.array([], pa.string()),
                    'ACCTYPE': pa.array([], pa.string()),
                    'CCY': pa.array([], pa.string()),
                    'FORATE': pa.array([], pa.float64()),
                    'DATE': pa.array([], pa.date32())
                })
                print("✓ Base file doesn't exist, creating new structure")
            
            # Step 2: Read today's data and add DATE column
            today_query = f"""
            SELECT 
                ACCTNO, NOTENO, FISSPURP, PRODUCT, BALANCE, CENSUS, 
                DNBFISME, PRODCD, CUSTCD, AMTIND, SECTORCD, BRANCH, 
                ACCTYPE, CCY, FORATE,
                DATE '{reptdate.strftime('%Y-%m-%d')}' as DATE
            FROM read_parquet('{input_file}')
            """
            
            today_df = conn.execute(today_query).arrow()
            print(f"✓ Loaded {len(today_df)} records from today's data")
            
            # Step 3: Combine existing and today's data
            combined_df = pa.concat_tables([existing_df, today_df])
            print(f"✓ Combined data: {len(combined_df)} total records")
            
            # Step 4: Sort by ACCTNO, NOTENO, DATE descending and remove duplicates
            # Using DuckDB for efficient sorting and deduplication
            sort_dedup_query = f"""
            WITH sorted_data AS (
                SELECT *
                FROM combined_df
                ORDER BY ACCTNO, NOTENO, DATE DESC
            ),
            deduped_data AS (
                SELECT 
                    ACCTNO, NOTENO, FISSPURP, PRODUCT, BALANCE, CENSUS,
                    DNBFISME, PRODCD, CUSTCD, AMTIND, SECTORCD, BRANCH,
                    ACCTYPE, CCY, FORATE, DATE
                FROM sorted_data
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY DATE DESC) = 1
            )
            SELECT * FROM deduped_data
            ORDER BY ACCTNO, NOTENO
            """
            
            final_df = conn.execute(sort_dedup_query).arrow()
            print(f"✓ After deduplication: {len(final_df)} unique records")
            
            # Step 5: Save final output
            pq.write_table(final_df, output_file)
            csv.write_csv(final_df, app_path / f"{output_filename}.csv")
            
            print(f"✓ Saved updated {output_filename} with {len(final_df)} records")
    
    # Execute the append logic
    append_macro()
    
    # Print summary statistics
    output_filename = f"LOAN{rept_month}"
    output_file = app_path / f"{output_filename}.parquet"
    
    if output_file.exists():
        final_df = pq.read_table(output_file)
        
        print(f"\nProcessing completed successfully!")
        print(f"Final output: {output_filename}.parquet")
        print(f"Total records: {len(final_df)}")
        
        # Count records by date
        date_summary_query = """
        SELECT 
            DATE,
            COUNT(*) as RECORD_COUNT
        FROM final_df
        GROUP BY DATE
        ORDER BY DATE
        """
        
        date_summary = conn.execute(date_summary_query).arrow()
        
        print(f"\nRecords by date:")
        for i in range(len(date_summary)):
            date_val = date_summary['DATE'][i].as_py()
            count = date_summary['RECORD_COUNT'][i].as_py()
            print(f"  {date_val}: {count} records")
        
        # Account type distribution
        if 'ACCTYPE' in final_df.column_names:
            actype_summary_query = """
            SELECT 
                ACCTYPE,
                COUNT(*) as RECORD_COUNT
            FROM final_df
            GROUP BY ACCTYPE
            ORDER BY RECORD_COUNT DESC
            """
            
            actype_summary = conn.execute(actype_summary_query).arrow()
            
            print(f"\nRecords by account type:")
            for i in range(min(5, len(actype_summary))):  # Top 5 only
                actype = actype_summary['ACCTYPE'][i].as_py()
                count = actype_summary['RECORD_COUNT'][i].as_py()
                print(f"  {actype}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    main()
