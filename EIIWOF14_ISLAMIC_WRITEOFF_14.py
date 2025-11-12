# islamic_writeoff_accumulation.py

import duckdb
from pathlib import Path
import datetime
import sys

def accumulate_islamic_writeoff_data():
    """
    EIIWOF14 - Islamic Banking Write-off Data Accumulation
    Processes write-off information from WMIS text file and appends to cumulative dataset
    Filters for Islamic branches only (3000-3999, 4043, 4048)
    """
    
    print(" ISLAMIC WRITE-OFF DATA ACCUMULATION PROCESSING (EIIWOF14)")
    print("=" * 70)
    
    # Configuration
    npl_path = Path("NPL")
    npl1_path = Path("NPL1")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)
    npl1_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection
    conn = duckdb.connect()
    
    try:
        # =========================================================================
        # DATA NPL1.WIIS, WSP2, WAQ - Copy datasets to NPL1
        # =========================================================================
        print("📁 COPYING BASE DATASETS TO NPL1...")
        
        datasets_to_copy = [
            ("WIIS", "NPL/WIIS.parquet"),
            ("WSP2", "NPL/WSP2.parquet"), 
            ("WAQ", "NPL/WAQ.parquet")
        ]
        
        for dataset_name, source_path in datasets_to_copy:
            if Path(source_path).exists():
                copy_sql = f"""
                CREATE OR REPLACE TABLE {dataset_name.lower()}_npl1 AS 
                SELECT * FROM read_parquet('{source_path}')
                """
                conn.execute(copy_sql)
                conn.execute(f"COPY {dataset_name.lower()}_npl1 TO '{npl1_path}/{dataset_name}.parquet' (FORMAT PARQUET)")
                count = conn.execute(f"SELECT COUNT(*) FROM {dataset_name.lower()}_npl1").fetchone()[0]
                print(f"   ✅ {dataset_name}: {count:,} records copied")
            else:
                print(f"   ⚠️  {dataset_name} source not found: {source_path}")
        
        # =========================================================================
        # DATA WOFF - Process WMIS text file with fixed-width parsing
        # =========================================================================
        print("\n📁 PROCESSING WMIS WRITE-OFF DATA...")
        
        # Multiple possible WMIS file locations
        wmis_file_paths = [
            "WMIS.txt",
            "RBP2/B033/LN/WRI2/OFF/MIS.txt",
            "SAP/PIBB/WOFF/WMIS.txt"
        ]
        
        wmis_file_path = None
        for file_path in wmis_file_paths:
            if Path(file_path).exists():
                wmis_file_path = file_path
                print(f"   ✅ Found WMIS file: {file_path}")
                break
        
        if wmis_file_path is None:
            print("❌ ERROR: No WMIS data file found")
            # Create empty structure for demonstration
            wmis_file_path = "WMIS.txt"
            print("   ⚠️  Creating demonstration mode with sample data")
            create_sample_wmis_file()
        
        # Create temporary table from fixed-width WMIS file
        create_wmis_table_sql = f"""
        CREATE OR REPLACE TABLE wmis_raw AS 
        SELECT 
            line as raw_line
        FROM read_text('{wmis_file_path}')
        """
        conn.execute(create_wmis_table_sql)
        
        raw_count = conn.execute("SELECT COUNT(*) FROM wmis_raw").fetchone()[0]
        print(f"   📊 Raw WMIS records loaded: {raw_count:,}")
        
        # Extract fields using fixed-width positions
        extract_woff_sql = """
        CREATE OR REPLACE TABLE woff_extracted AS 
        SELECT 
            -- @142 ACCTNO 10.
            TRIM(SUBSTRING(raw_line, 142, 10)) as ACCTNO,
            -- @152 NOTENO 5.
            TRIM(SUBSTRING(raw_line, 152, 5)) as NOTENO,
            -- @162 IISWOFF 16.2
            CASE 
                WHEN TRIM(SUBSTRING(raw_line, 162, 16)) != '' 
                THEN CAST(TRIM(SUBSTRING(raw_line, 162, 16)) AS DECIMAL(16,2))
                ELSE 0.0 
            END as IISWOFF,
            -- @178 SPWOFF 16.2
            CASE 
                WHEN TRIM(SUBSTRING(raw_line, 178, 16)) != '' 
                THEN CAST(TRIM(SUBSTRING(raw_line, 178, 16)) AS DECIMAL(16,2))
                ELSE 0.0 
            END as SPWOFF,
            -- @210 DDWOFF 2.
            TRIM(SUBSTRING(raw_line, 210, 2)) as DDWOFF,
            -- @213 MMWOFF 2.
            TRIM(SUBSTRING(raw_line, 213, 2)) as MMWOFF,
            -- @216 YYWOFF 4.
            TRIM(SUBSTRING(raw_line, 216, 4)) as YYWOFF,
            -- @220 CAPBAL 16.2
            CASE 
                WHEN TRIM(SUBSTRING(raw_line, 220, 16)) != '' 
                THEN CAST(TRIM(SUBSTRING(raw_line, 220, 16)) AS DECIMAL(16,2))
                ELSE 0.0 
            END as CAPBAL,
            -- @236 COSTCTR 4.
            CASE 
                WHEN TRIM(SUBSTRING(raw_line, 236, 4)) != '' 
                THEN CAST(TRIM(SUBSTRING(raw_line, 236, 4)) AS INTEGER)
                ELSE 0 
            END as COSTCTR
        FROM wmis_raw
        WHERE LENGTH(raw_line) >= 240  -- Ensure line has minimum required length
        """
        conn.execute(extract_woff_sql)
        
        # Apply transformations and Islamic branch filter
        final_woff_sql = """
        CREATE OR REPLACE TABLE woff_final AS 
        SELECT 
            ACCTNO,
            NOTENO,
            IISWOFF,
            CAPBAL as SPWOFF,  -- SPWOFF = CAPBAL
            -- WOFFDT = PUT(MMWOFF,Z2.)||'/'||PUT(YYWOFF,Z4.)
            CASE 
                WHEN LENGTH(TRIM(MMWOFF)) = 2 AND LENGTH(TRIM(YYWOFF)) = 4 
                THEN TRIM(MMWOFF) || '/' || TRIM(YYWOFF)
                ELSE '01/' || CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS VARCHAR)
            END as WOFFDT,
            CAPBAL
        FROM woff_extracted
        WHERE 
            -- Islamic branches filter: ((3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048))
            (COSTCTR BETWEEN 3000 AND 3999) OR COSTCTR IN (4043, 4048)
        """
        conn.execute(final_woff_sql)
        
        woff_count = conn.execute("SELECT COUNT(*) FROM woff_final").fetchone()[0]
        print(f"   ✅ Islamic write-off records processed: {woff_count:,}")
        
        # Save current WOFF data to Parquet and CSV
        current_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        woff_parquet = output_path / f"woff_current_{current_date}.parquet"
        woff_csv = output_path / f"woff_current_{current_date}.csv"
        
        conn.execute(f"COPY woff_final TO '{woff_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY woff_final TO '{woff_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"   💾 Current WOFF data saved:")
        print(f"      📁 Parquet: {woff_parquet}")
        print(f"      📁 CSV: {woff_csv}")
        
        # PROC PRINT equivalent - Display sample
        print("\n📋 CURRENT WOFF DATA SAMPLE (PROC PRINT equivalent):")
        print_sample_data(conn, "woff_final", 10)
        
        # =========================================================================
        # PROC APPEND equivalent - Append to cumulative WOFFTOT
        # =========================================================================
        print("\n🔄 ACCUMULATING TO WOFFTOT DATASET (PROC APPEND equivalent)...")
        
        # Check if WOFFTOT exists, create if not
        wofftot_parquet_path = npl1_path / "WOFFTOT.parquet"
        
        if wofftot_parquet_path.exists():
            # Load existing WOFFTOT
            load_wofftot_sql = f"""
            CREATE OR REPLACE TABLE wofftot_existing AS 
            SELECT * FROM read_parquet('{wofftot_parquet_path}')
            """
            conn.execute(load_wofftot_sql)
            existing_count = conn.execute("SELECT COUNT(*) FROM wofftot_existing").fetchone()[0]
            print(f"   📊 Existing WOFFTOT records: {existing_count:,}")
        else:
            # Create empty WOFFTOT with same structure
            create_wofftot_sql = """
            CREATE OR REPLACE TABLE wofftot_existing AS 
            SELECT 
                ACCTNO,
                NOTENO,
                IISWOFF,
                SPWOFF,
                WOFFDT,
                CAPBAL
            FROM woff_final 
            WHERE 1=0  -- Empty result set with correct schema
            """
            conn.execute(create_wofftot_sql)
            print("   📊 Created new WOFFTOT dataset")
        
        # Append new data (FORCE equivalent - allows duplicates temporarily)
        append_sql = """
        CREATE OR REPLACE TABLE wofftot_combined AS 
        SELECT * FROM wofftot_existing
        UNION ALL
        SELECT * FROM woff_final
        """
        conn.execute(append_sql)
        
        combined_count = conn.execute("SELECT COUNT(*) FROM wofftot_combined").fetchone()[0]
        print(f"   📊 Combined records (before deduplication): {combined_count:,}")
        
        # =========================================================================
        # PROC SORT NODUPKEY equivalent - Remove duplicates by ACCTNO, NOTENO
        # =========================================================================
        print("\n🔍 REMOVING DUPLICATES (PROC SORT NODUPKEY equivalent)...")
        
        deduplicate_sql = """
        CREATE OR REPLACE TABLE wofftot_final AS 
        SELECT 
            ACCTNO,
            NOTENO,
            IISWOFF,
            SPWOFF,
            WOFFDT,
            CAPBAL
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY ACCTNO, NOTENO 
                    ORDER BY 
                        CASE WHEN WOFFDT IS NOT NULL THEN 1 ELSE 0 END DESC,
                        WOFFDT DESC
                ) as rn
            FROM wofftot_combined
        )
        WHERE rn = 1
        ORDER BY ACCTNO, NOTENO
        """
        conn.execute(deduplicate_sql)
        
        final_count = conn.execute("SELECT COUNT(*) FROM wofftot_final").fetchone()[0]
        duplicates_removed = combined_count - final_count
        print(f"   ✅ Final WOFFTOT records: {final_count:,}")
        print(f"   🗑️  Duplicates removed: {duplicates_removed:,}")
        
        # Save final WOFFTOT to Parquet and CSV
        wofftot_parquet = npl1_path / "WOFFTOT.parquet"
        wofftot_csv = output_path / f"wofftot_cumulative_{current_date}.csv"
        
        conn.execute(f"COPY wofftot_final TO '{wofftot_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY wofftot_final TO '{wofftot_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"   💾 Cumulative WOFFTOT saved:")
        print(f"      📁 Parquet: {wofftot_parquet}")
        print(f"      📁 CSV: {wofftot_csv}")
        
        # =========================================================================
        # Generate Comprehensive Summary Report
        # =========================================================================
        print("\n📊 GENERATING COMPREHENSIVE SUMMARY REPORT...")
        
        summary_sql = """
        CREATE OR REPLACE TABLE accumulation_summary AS
        SELECT 
            'CURRENT_BATCH' as dataset,
            COUNT(*) as record_count,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            ROUND(SUM(IISWOFF), 2) as total_iiswoff,
            ROUND(SUM(SPWOFF), 2) as total_spwoff,
            ROUND(SUM(CAPBAL), 2) as total_capbal,
            ROUND(AVG(IISWOFF), 2) as avg_iiswoff,
            ROUND(AVG(SPWOFF), 2) as avg_spwoff
        FROM woff_final
        UNION ALL
        SELECT 
            'CUMULATIVE_TOTAL' as dataset,
            COUNT(*) as record_count,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            ROUND(SUM(IISWOFF), 2) as total_iiswoff,
            ROUND(SUM(SPWOFF), 2) as total_spwoff,
            ROUND(SUM(CAPBAL), 2) as total_capbal,
            ROUND(AVG(IISWOFF), 2) as avg_iiswoff,
            ROUND(AVG(SPWOFF), 2) as avg_spwoff
        FROM wofftot_final
        UNION ALL
        SELECT 
            'BATCH_CONTRIBUTION' as dataset,
            (SELECT COUNT(*) FROM woff_final),
            (SELECT COUNT(DISTINCT ACCTNO) FROM woff_final),
            (SELECT ROUND(SUM(IISWOFF), 2) FROM woff_final),
            (SELECT ROUND(SUM(SPWOFF), 2) FROM woff_final),
            (SELECT ROUND(SUM(CAPBAL), 2) FROM woff_final),
            (SELECT ROUND(AVG(IISWOFF), 2) FROM woff_final),
            (SELECT ROUND(AVG(SPWOFF), 2) FROM woff_final)
        """
        conn.execute(summary_sql)
        
        # Save summary to both formats
        summary_parquet = output_path / f"islamic_woff_accumulation_summary_{current_date}.parquet"
        summary_csv = output_path / f"islamic_woff_accumulation_summary_{current_date}.csv"
        
        conn.execute(f"COPY accumulation_summary TO '{summary_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY accumulation_summary TO '{summary_csv}' (FORMAT CSV, HEADER true)")
        
        # Display comprehensive summary
        summary_results = conn.execute("SELECT * FROM accumulation_summary").fetchall()
        
        print("\n📈 COMPREHENSIVE ACCUMULATION SUMMARY:")
        print("=" * 110)
        print(f"{'Dataset':<18} {'Records':>10} {'Accounts':>10} {'IIS Woff':>12} {'SP Woff':>12} {'Cap Bal':>12} {'Avg IIS':>10} {'Avg SP':>10}")
        print("-" * 110)
        for row in summary_results:
            print(f"{row[0]:<18} {row[1]:>10,} {row[2]:>10,} {row[3]:>12,.2f} {row[4]:>12,.2f} {row[5]:>12,.2f} {row[6]:>10,.2f} {row[7]:>10,.2f}")
        
        print(f"\n💾 Summary files saved:")
        print(f"   📁 Parquet: {summary_parquet}")
        print(f"   📁 CSV: {summary_csv}")
        
        # Additional: Islamic Branch Analysis
        print("\n🏦 ISLAMIC BRANCH ANALYSIS:")
        branch_analysis_sql = """
        SELECT 
            CASE 
                WHEN COSTCTR BETWEEN 3000 AND 3999 THEN 'Islamic Standard'
                WHEN COSTCTR IN (4043, 4048) THEN 'Islamic Special'
                ELSE 'Other'
            END as branch_type,
            COUNT(*) as record_count,
            ROUND(SUM(IISWOFF), 2) as total_iiswoff
        FROM woff_extracted
        GROUP BY branch_type
        ORDER BY total_iiswoff DESC
        """
        
        branch_results = conn.execute(branch_analysis_sql).fetchall()
        for branch_type, count, total in branch_results:
            print(f"   {branch_type:<18}: {count:>6,} records, RM {total:>12,.2f} IIS Write-off")
        
        return {
            'current_batch_count': woff_count,
            'cumulative_count': final_count,
            'duplicates_removed': duplicates_removed,
            'total_iiswoff': summary_results[1][3] if len(summary_results) > 1 else 0,
            'total_spwoff': summary_results[1][4] if len(summary_results) > 1 else 0,
            'output_files': {
                'woff_current_parquet': woff_parquet,
                'woff_current_csv': woff_csv,
                'wofftot_parquet': wofftot_parquet,
                'wofftot_csv': wofftot_csv,
                'summary_parquet': summary_parquet,
                'summary_csv': summary_csv
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR in Islamic write-off accumulation: {e}")
        raise
    finally:
        # Clean up temporary tables
        temp_tables = [
            "wmis_raw", "woff_extracted", "woff_final", 
            "wiis_npl1", "wsp2_npl1", "waq_npl1",
            "wofftot_existing", "wofftot_combined", "wofftot_final", 
            "accumulation_summary"
        ]
        for table in temp_tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass  # Ignore errors if table doesn't exist
        conn.close()

def print_sample_data(conn, table_name, limit=10):
    """Print sample data from specified table (PROC PRINT equivalent)"""
    sample_sql = f"""
    SELECT * FROM {table_name} 
    ORDER BY ACCTNO 
    LIMIT {limit}
    """
    
    sample_data = conn.execute(sample_sql).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    if sample_data:
        # Print header
        header = " | ".join(f"{col:>12}" for col in columns)
        print(f"   {header}")
        print(f"   {'-' * len(header)}")
        
        # Print sample rows
        for row in sample_data:
            row_display = " | ".join(f"{str(val):>12}"[:12] for val in row)
            print(f"   {row_display}")
        
        if conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] > limit:
            print(f"   ... and {conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0] - limit} more records")
    else:
        print(f"   No data available in {table_name}")

def create_sample_wmis_file():
    """Create a sample WMIS file for demonstration purposes"""
    sample_data = [
        " " * 141 + "1234567890" + "67890" + " " * 5 + "150000.00" + " " * 10 + "100000.00" + " " * 12 + "15" + "03" + "2024" + " " * 2 + "120000.00" + " " * 10 + "3500",
        " " * 141 + "2345678901" + "78901" + " " * 5 + "250000.00" + " " * 10 + "180000.00" + " " * 12 + "20" + "03" + "2024" + " " * 2 + "200000.00" + " " * 10 + "4043",
        " " * 141 + "3456789012" + "89012" + " " * 5 + "75000.00" + " " * 11 + "50000.00" + " " * 13 + "10" + "03" + "2024" + " " * 2 + "60000.00" + " " * 11 + "3800",
    ]
    
    with open("WMIS.txt", "w") as f:
        for line in sample_data:
            f.write(line + "\n")
    print("   📝 Created sample WMIS file for demonstration")

# Main execution
if __name__ == "__main__":
    try:
        print("🕌 STARTING ISLAMIC WRITE-OFF DATA ACCUMULATION (EIIWOF14)")
        print("=" * 70)
        
        results = accumulate_islamic_writeoff_data()
        
        if results:
            print(f"\n🎉 ISLAMIC WRITE-OFF ACCUMULATION COMPLETED SUCCESSFULLY")
            print(f"   📄 Current batch records: {results['current_batch_count']:,}")
            print(f"   📄 Cumulative records: {results['cumulative_count']:,}")
            print(f"   🗑️  Duplicates removed: {results['duplicates_removed']:,}")
            print(f"   💰 Total IIS Write-off: RM {results['total_iiswoff']:,.2f}")
            print(f"   💰 Total SP Write-off: RM {results['total_spwoff']:,.2f}")
            print(f"   💾 Output files generated:")
            for file_type, file_path in results['output_files'].items():
                print(f"      📁 {file_type}: {file_path.name}")
            print(f"   🕒 Completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
    except Exception as e:
        print(f"\n💥 PROCESSING FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
