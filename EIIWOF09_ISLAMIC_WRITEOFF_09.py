# islamic_writeoff_consolidation.py

import duckdb
from pathlib import Path
import datetime
import sys

def consolidate_islamic_writeoff_data():
    """
    EIIWOF09 - Islamic Banking Write-off Data Consolidation
    Consolidates IIS, AQ, SP2 data with write-off accounts for Islamic banking
    Processes Parquet inputs and generates both Parquet and CSV outputs
    """
    
    print("🕌 ISLAMIC WRITE-OFF DATA CONSOLIDATION PROCESSING")
    print("=" * 60)
    
    # Configuration
    npla_path = Path("NPLA")
    npl_path = Path("NPL")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)
    npl_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection
    conn = duckdb.connect()
    
    try:
        # =========================================================================
        # DATA REPTDATE Processing
        # =========================================================================
        print("📅 PROCESSING REPORT DATE...")
        
        # Load REPTDATE from Parquet
        reptdate_sql = """
        CREATE OR REPLACE TABLE reptdate AS 
        SELECT 
            REPTDATE,
            CAST(REPTDATE AS VARCHAR) as RDATE_STR
        FROM read_parquet('NPLA/REPTDATE.parquet')
        LIMIT 1
        """
        conn.execute(reptdate_sql)
        
        # Extract RDATE parameter
        rdate_result = conn.execute("SELECT RDATE_STR FROM reptdate").fetchone()
        RDATE = rdate_result[0] if rdate_result else datetime.datetime.now().strftime('%Y%m%d')
        
        print(f"   ✅ Report Date: {RDATE}")
        
        # =========================================================================
        # PROC SORT equivalent - Load and prepare datasets
        # =========================================================================
        print("📁 LOADING ISLAMIC DATASETS...")
        
        # Load IIS dataset
        iis_sql = """
        CREATE OR REPLACE TABLE iis_sorted AS 
        SELECT 
            ACCTNO,
            NOTENO,
            SUSPEND,
            RECOVER,
            RECC,
            OISUSP,
            OIRECV,
            OIRECC
        FROM read_parquet('NPLA/IIS.parquet')
        ORDER BY ACCTNO
        """
        conn.execute(iis_sql)
        
        # Load AQ dataset
        aq_sql = """
        CREATE OR REPLACE TABLE aq_sorted AS 
        SELECT 
            ACCTNO,
            NOTENO,
            NEWNPL,
            ACCRINT,
            RECOVER
        FROM read_parquet('NPLA/AQ.parquet')
        ORDER BY ACCTNO
        """
        conn.execute(aq_sql)
        
        # Load SP2 dataset
        sp2_sql = """
        CREATE OR REPLACE TABLE sp2_sorted AS 
        SELECT 
            ACCTNO,
            NOTENO,
            SPPL,
            RECOVER
        FROM read_parquet('NPLA/SP2.parquet')
        ORDER BY ACCTNO
        """
        conn.execute(sp2_sql)
        
        # Load Write-off HP dataset
        wfhp_sql = """
        CREATE OR REPLACE TABLE wfhp_sorted AS 
        SELECT 
            ACCTNO
        FROM read_parquet('NPL/WOFFHP.parquet')
        ORDER BY ACCTNO
        """
        conn.execute(wfhp_sql)
        
        # Count records in each dataset
        iis_count = conn.execute("SELECT COUNT(*) FROM iis_sorted").fetchone()[0]
        aq_count = conn.execute("SELECT COUNT(*) FROM aq_sorted").fetchone()[0]
        sp2_count = conn.execute("SELECT COUNT(*) FROM sp2_sorted").fetchone()[0]
        wfhp_count = conn.execute("SELECT COUNT(*) FROM wfhp_sorted").fetchone()[0]
        
        print(f"   📊 IIS records: {iis_count:,}")
        print(f"   📊 AQ records: {aq_count:,}")
        print(f"   📊 SP2 records: {sp2_count:,}")
        print(f"   📊 Write-off accounts: {wfhp_count:,}")
        
        # =========================================================================
        # DATA NPL.TIIS - Merge IIS with Write-off accounts
        # =========================================================================
        print("🔄 CREATING TIIS DATASET...")
        
        tiis_sql = f"""
        CREATE OR REPLACE TABLE tiis AS 
        SELECT 
            i.ACCTNO,
            i.NOTENO,
            i.SUSPEND as WSUSPEND,
            ROUND(i.RECOVER, 2) as WRECOVER,
            i.RECC as WRECC,
            i.OISUSP as WOISUSP,
            i.OIRECV as WOIRECV,
            i.OIRECC as WOIRECC,
            '{RDATE}' as WDATE
        FROM iis_sorted i
        INNER JOIN wfhp_sorted w ON i.ACCTNO = w.ACCTNO
        """
        conn.execute(tiis_sql)
        
        tiis_count = conn.execute("SELECT COUNT(*) FROM tiis").fetchone()[0]
        print(f"   ✅ TIIS records created: {tiis_count:,}")
        
        # Save TIIS to Parquet and CSV
        tiis_parquet = npl_path / "TIIS.parquet"
        tiis_csv = output_path / f"islamic_tiis_{RDATE}.csv"
        
        conn.execute(f"COPY tiis TO '{tiis_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY tiis TO '{tiis_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"   💾 TIIS saved:")
        print(f"      📁 Parquet: {tiis_parquet}")
        print(f"      📁 CSV: {tiis_csv}")
        
        # PROC PRINT equivalent - Display sample
        print_sample(conn, "tiis", "TIIS SAMPLE DATA")
        
        # =========================================================================
        # DATA NPL.TAQ - Merge AQ with Write-off accounts
        # =========================================================================
        print("\n🔄 CREATING TAQ DATASET...")
        
        taq_sql = f"""
        CREATE OR REPLACE TABLE taq AS 
        SELECT 
            a.ACCTNO,
            a.NOTENO,
            a.NEWNPL as WNEWNPL,
            a.ACCRINT as WACCRINT,
            a.RECOVER as WRECOVER,
            '{RDATE}' as WDATE
        FROM aq_sorted a
        INNER JOIN wfhp_sorted w ON a.ACCTNO = w.ACCTNO
        """
        conn.execute(taq_sql)
        
        taq_count = conn.execute("SELECT COUNT(*) FROM taq").fetchone()[0]
        print(f"   ✅ TAQ records created: {taq_count:,}")
        
        # Save TAQ to Parquet and CSV
        taq_parquet = npl_path / "TAQ.parquet"
        taq_csv = output_path / f"islamic_taq_{RDATE}.csv"
        
        conn.execute(f"COPY taq TO '{taq_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY taq TO '{taq_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"   💾 TAQ saved:")
        print(f"      📁 Parquet: {taq_parquet}")
        print(f"      📁 CSV: {taq_csv}")
        
        # PROC PRINT equivalent - Display sample
        print_sample(conn, "taq", "TAQ SAMPLE DATA")
        
        # =========================================================================
        # DATA NPL.TSP2 - Merge SP2 with Write-off accounts
        # =========================================================================
        print("\n🔄 CREATING TSP2 DATASET...")
        
        tsp2_sql = f"""
        CREATE OR REPLACE TABLE tsp2 AS 
        SELECT 
            s.ACCTNO,
            s.NOTENO,
            s.SPPL as WSPPL,
            s.RECOVER as WRECOVER,
            '{RDATE}' as WDATE
        FROM sp2_sorted s
        INNER JOIN wfhp_sorted w ON s.ACCTNO = w.ACCTNO
        """
        conn.execute(tsp2_sql)
        
        tsp2_count = conn.execute("SELECT COUNT(*) FROM tsp2").fetchone()[0]
        print(f"   ✅ TSP2 records created: {tsp2_count:,}")
        
        # Save TSP2 to Parquet and CSV
        tsp2_parquet = npl_path / "TSP2.parquet"
        tsp2_csv = output_path / f"islamic_tsp2_{RDATE}.csv"
        
        conn.execute(f"COPY tsp2 TO '{tsp2_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY tsp2 TO '{tsp2_csv}' (FORMAT CSV, HEADER true)")
        
        print(f"   💾 TSP2 saved:")
        print(f"      📁 Parquet: {tsp2_parquet}")
        print(f"      📁 CSV: {tsp2_csv}")
        
        # PROC PRINT equivalent - Display sample
        print_sample(conn, "tsp2", "TSP2 SAMPLE DATA")
        
        # =========================================================================
        # Generate Summary Report
        # =========================================================================
        print("\n📊 GENERATING CONSOLIDATION SUMMARY...")
        
        summary_sql = """
        CREATE OR REPLACE TABLE consolidation_summary AS
        SELECT 
            'TIIS' as dataset_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            ROUND(AVG(WSUSPEND), 2) as avg_suspend,
            ROUND(AVG(WRECOVER), 2) as avg_recover
        FROM tiis
        UNION ALL
        SELECT 
            'TAQ' as dataset_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            ROUND(AVG(WNEWNPL), 2) as avg_suspend,
            ROUND(AVG(WRECOVER), 2) as avg_recover
        FROM taq
        UNION ALL
        SELECT 
            'TSP2' as dataset_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            ROUND(AVG(WSPPL), 2) as avg_suspend,
            ROUND(AVG(WRECOVER), 2) as avg_recover
        FROM tsp2
        """
        conn.execute(summary_sql)
        
        # Save summary to both formats
        summary_parquet = output_path / "islamic_consolidation_summary.parquet"
        summary_csv = output_path / f"islamic_consolidation_summary_{RDATE}.csv"
        
        conn.execute(f"COPY consolidation_summary TO '{summary_parquet}' (FORMAT PARQUET)")
        conn.execute(f"COPY consolidation_summary TO '{summary_csv}' (FORMAT CSV, HEADER true)")
        
        # Display summary
        summary_results = conn.execute("SELECT * FROM consolidation_summary ORDER BY dataset_name").fetchall()
        
        print("\n📈 CONSOLIDATION SUMMARY:")
        print("=" * 80)
        print(f"{'Dataset':<8} {'Records':>10} {'Accounts':>10} {'Avg Susp':>12} {'Avg Recov':>12}")
        print("-" * 80)
        for row in summary_results:
            print(f"{row[0]:<8} {row[1]:>10,} {row[2]:>10,} {row[3]:>12.2f} {row[4]:>12.2f}")
        
        print(f"\n💾 Summary files saved:")
        print(f"   📁 Parquet: {summary_parquet}")
        print(f"   📁 CSV: {summary_csv}")
        
        return {
            'tiis_count': tiis_count,
            'taq_count': taq_count,
            'tsp2_count': tsp2_count,
            'rdate': RDATE
        }
        
    except Exception as e:
        print(f"❌ ERROR in Islamic write-off consolidation: {e}")
        raise
    finally:
        # Clean up temporary tables
        conn.execute("DROP TABLE IF EXISTS reptdate")
        conn.execute("DROP TABLE IF EXISTS iis_sorted")
        conn.execute("DROP TABLE IF EXISTS aq_sorted")
        conn.execute("DROP TABLE IF EXISTS sp2_sorted")
        conn.execute("DROP TABLE IF EXISTS wfhp_sorted")
        conn.execute("DROP TABLE IF EXISTS tiis")
        conn.execute("DROP TABLE IF EXISTS taq")
        conn.execute("DROP TABLE IF EXISTS tsp2")
        conn.execute("DROP TABLE IF EXISTS consolidation_summary")
        conn.close()

def print_sample(conn, table_name, title, limit=5):
    """Print sample data from specified table"""
    sample_sql = f"""
    SELECT * FROM {table_name} 
    ORDER BY ACCTNO 
    LIMIT {limit}
    """
    
    sample_data = conn.execute(sample_sql).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    print(f"   📋 {title}:")
    if sample_data:
        # Print header
        header = " | ".join(f"{col:<12}" for col in columns[:4])
        print(f"      {header}")
        print(f"      {'-' * len(header)}")
        
        # Print sample rows
        for row in sample_data:
            row_display = " | ".join(f"{str(val):<12}"[:12] for val in row[:4])
            print(f"      {row_display}")
        
        if len(columns) > 4:
            print(f"      ... and {len(columns) - 4} more columns")
    else:
        print(f"      No data available")

# Main execution
if __name__ == "__main__":
    try:
        print("🕌 STARTING ISLAMIC WRITE-OFF DATA CONSOLIDATION")
        print("=" * 60)
        
        results = consolidate_islamic_writeoff_data()
        
        if results:
            print(f"\n🎉 ISLAMIC WRITE-OFF CONSOLIDATION COMPLETED SUCCESSFULLY")
            print(f"   📄 TIIS records: {results['tiis_count']:,}")
            print(f"   📄 TAQ records: {results['taq_count']:,}")
            print(f"   📄 TSP2 records: {results['tsp2_count']:,}")
            print(f"   📅 Report Date: {results['rdate']}")
            print(f"   💾 Output formats: Parquet + CSV")
            print(f"   🕒 Completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
    except Exception as e:
        print(f"\n💥 PROCESSING FAILED: {e}")
        sys.exit(1)
