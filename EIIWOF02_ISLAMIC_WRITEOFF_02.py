# islamic_writeoff_upload.py

import duckdb
from pathlib import Path
import sys
import datetime

def upload_islamic_writeoff_accounts():
    """
    EIIWOF02 - Islamic Banking Write-off Accounts Upload
    Uploads and processes write-off accounts from CCD text file for Islamic banking
    Filters for Islamic branches only (3000-3999, 4043, 4048)
    """
    
    print(" ISLAMIC WRITE-OFF ACCOUNTS UPLOAD PROCESSING")
    print("=" * 60)
    
    # Configuration
    npl_path = Path("SAP/PIBB/NPL/HP/SASDATA/WOFF")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)
    npl_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection
    conn = duckdb.connect()
    
    try:
        # *** UPLOAD FROM TEXT FILE PROVIDE BY CCD ***
        # *** CHECK THE POSITION OF EACH FIELDS    ***
        print("📁 READING WRITE-OFF DATA FROM CCD TEXT FILE...")
        
        # Multiple data source options as commented in original SAS
        woff_file_paths = [
            "RBP2/B033/LN/WRI2/OFF/MIS.txt",  # Primary source
            "OPER/RESTORE/PBB/NPL/HP/WOFF01.TXT",  # Alternative 1
            "SAP/PBB/IWAPPR.TXT"  # Alternative 2
        ]
        
        woff_file_path = None
        for file_path in woff_file_paths:
            if Path(file_path).exists():
                woff_file_path = file_path
                print(f"   ✅ Found write-off file: {file_path}")
                break
            else:
                print(f"   ⚠️  File not found: {file_path}")
        
        if woff_file_path is None:
            print("❌ ERROR: No write-off data file found")
            sys.exit(1)
        
        # Create temporary table from fixed-width text file
        # Using DuckDB's read_text function to handle fixed-width parsing
        create_temp_table_sql = f"""
        CREATE OR REPLACE TABLE temp_writeoff_raw AS 
        SELECT 
            line as raw_line
        FROM read_text('{woff_file_path}')
        """
        conn.execute(create_temp_table_sql)
        
        # Count raw records
        raw_count_result = conn.execute("SELECT COUNT(*) as raw_count FROM temp_writeoff_raw").fetchone()
        print(f"   📊 Raw records loaded: {raw_count_result[0]:,}")
        
        # DATA NPL.WOFFHP(KEEP=ACCTNO)
        # INPUT @142  ACCTNO   10.
        #       @236  COSTCTR   4.
        print("🔧 EXTRACTING ACCOUNT AND BRANCH DATA...")
        
        # Extract fields using substring and apply Islamic branch filter
        extract_sql = """
        CREATE OR REPLACE TABLE islamic_writeoff AS 
        SELECT 
            TRIM(SUBSTRING(raw_line, 142, 10)) as ACCTNO,
            CAST(TRIM(SUBSTRING(raw_line, 236, 4)) AS INTEGER) as COSTCTR
        FROM temp_writeoff_raw
        WHERE 
            -- Islamic branches filter: ((3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048))
            (COSTCTR BETWEEN 3000 AND 3999) OR COSTCTR IN (4043, 4048)
        """
        conn.execute(extract_sql)
        
        # Count Islamic write-off accounts
        islamic_count_result = conn.execute("SELECT COUNT(*) as islamic_count FROM islamic_writeoff").fetchone()
        print(f"   ✅ Islamic write-off accounts extracted: {islamic_count_result[0]:,}")
        
        # Validate account numbers and remove empty ones
        final_sql = """
        CREATE OR REPLACE TABLE islamic_writeoff_final AS 
        SELECT 
            ACCTNO
        FROM islamic_writeoff
        WHERE LENGTH(TRIM(ACCTNO)) > 0
        ORDER BY ACCTNO
        """
        conn.execute(final_sql)
        
        # Count final valid accounts
        final_count_result = conn.execute("SELECT COUNT(*) as final_count FROM islamic_writeoff_final").fetchone()
        print(f"   ✅ Valid Islamic account numbers: {final_count_result[0]:,}")
        
        # Generate timestamp for output files
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # OUTPUT 1: Save to Parquet file (equivalent to SAS dataset)
        parquet_output_path = npl_path / "WOFFHP.parquet"
        conn.execute(f"""
            COPY islamic_writeoff_final TO '{parquet_output_path}' (FORMAT PARQUET)
        """)
        
        # OUTPUT 2: Save to CSV file for additional processing/reporting
        csv_output_path = output_path / f"islamic_writeoff_{timestamp}.csv"
        conn.execute(f"""
            COPY islamic_writeoff_final TO '{csv_output_path}' (FORMAT CSV, HEADER true)
        """)
        
        print(f"💾 SAVED ISLAMIC WRITE-OFF DATA:")
        print(f"   📁 Parquet: {parquet_output_path}")
        print(f"   📁 CSV: {csv_output_path}")
        print(f"   📊 Accounts: {final_count_result[0]:,}")
        
        # PROC PRINT equivalent - Display sample of results using DuckDB
        print("\n📋 SAMPLE OF ISLAMIC WRITE-OFF ACCOUNTS:")
        print("=" * 50)
        
        sample_result = conn.execute("""
            SELECT ACCTNO 
            FROM islamic_writeoff_final 
            ORDER BY ACCTNO 
            LIMIT 10
        """).fetchall()
        
        if sample_result:
            for i, row in enumerate(sample_result, 1):
                print(f"   {i:2d}. {row[0]}")
            
            total_count = final_count_result[0]
            if total_count > 10:
                print(f"   ... and {total_count - 10} more accounts")
        else:
            print("   No Islamic write-off accounts found")
        
        # Summary statistics using DuckDB
        summary_sql = """
        SELECT 
            COUNT(*) as total_accounts,
            COUNT(DISTINCT SUBSTRING(ACCTNO, 1, 3)) as account_prefixes,
            MIN(LENGTH(ACCTNO)) as min_acct_length,
            MAX(LENGTH(ACCTNO)) as max_acct_length
        FROM islamic_writeoff_final
        """
        summary_result = conn.execute(summary_sql).fetchone()
        
        print(f"\n📈 PROCESSING SUMMARY:")
        print(f"   ✅ Total Islamic write-off accounts: {summary_result[0]:,}")
        print(f"   ✅ Unique account prefixes: {summary_result[1]}")
        print(f"   ✅ Account number length range: {summary_result[2]}-{summary_result[3]} chars")
        print(f"   ✅ Islamic branches coverage: 3000-3999, 4043, 4048")
        
        # Additional: Create a summary report CSV
        summary_csv_path = output_path / f"writeoff_summary_{timestamp}.csv"
        conn.execute(f"""
            COPY (
                SELECT 
                    'Islamic Write-off Accounts Summary' as report_title,
                    COUNT(*) as total_accounts,
                    CURRENT_DATE as processing_date
                FROM islamic_writeoff_final
            ) TO '{summary_csv_path}' (FORMAT CSV, HEADER true)
        """)
        
        print(f"   📊 Summary report: {summary_csv_path}")
        
        return final_count_result[0]
        
    except Exception as e:
        print(f"❌ ERROR in Islamic write-off processing: {e}")
        raise
    finally:
        # Clean up temporary tables and close connection
        conn.execute("DROP TABLE IF EXISTS temp_writeoff_raw")
        conn.execute("DROP TABLE IF EXISTS islamic_writeoff")
        conn.execute("DROP TABLE IF EXISTS islamic_writeoff_final")
        conn.close()

def validate_islamic_branch_coverage():
    """
    Validate Islamic branch coverage and create branch reference files
    """
    conn = duckdb.connect()
    
    try:
        # Create Islamic branch reference data
        islamic_branches_sql = """
        CREATE OR REPLACE TABLE islamic_branches AS
        SELECT 
            branch_code,
            CASE 
                WHEN branch_code BETWEEN 3000 AND 3999 THEN 'Islamic Standard Branch'
                WHEN branch_code IN (4043, 4048) THEN 'Islamic Special Branch'
                ELSE 'Non-Islamic Branch'
            END as branch_type
        FROM (
            SELECT UNNEST([3000, 3001, 4043, 4048, 3999]) as branch_code
        )
        ORDER BY branch_code
        """
        conn.execute(islamic_branches_sql)
        
        # Save branch reference to both Parquet and CSV
        branches_parquet = Path("output/islamic_branches.parquet")
        branches_csv = Path("output/islamic_branches.csv")
        
        conn.execute(f"COPY islamic_branches TO '{branches_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY islamic_branches TO '{branches_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"🏦 Islamic branch reference saved:")
        print(f"   📁 {branches_parquet}")
        print(f"   📁 {branches_csv}")
        
    finally:
        conn.close()

# Main execution
if __name__ == "__main__":
    try:
        print("🕌 STARTING ISLAMIC WRITE-OFF PROCESSING WITH DUCKDB")
        print("=" * 60)
        
        # Process write-off accounts
        accounts_count = upload_islamic_writeoff_accounts()
        
        # Generate branch reference files
        validate_islamic_branch_coverage()
        
        if accounts_count > 0:
            print(f"\n🎉 ISLAMIC WRITE-OFF UPLOAD COMPLETED SUCCESSFULLY")
            print(f"   📄 Accounts processed: {accounts_count:,}")
            print(f"   💾 Output formats: Parquet + CSV")
            print(f"   🕒 Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"\n⚠️  NO ISLAMIC WRITE-OFF ACCOUNTS FOUND")
            
    except Exception as e:
        print(f"\n💥 PROCESSING FAILED: {e}")
        sys.exit(1)
