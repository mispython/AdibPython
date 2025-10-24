"""
EIBARPTS - Simple Job Runner for EIBMISLM
Direct execution using DuckDB and Polars
"""

import duckdb
import polars as pl
import os
from datetime import datetime

def run_eibapts():
    """Simple and powerful job runner for EIBMISLM"""
    
    print("🚀 Starting EIBARPTS Job Runner...")
    start_time = datetime.now()
    
    # Configuration
    BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS'
    OUTPUT_DIR = f'{BASE_PATH}/Job/ELDS/output/EIBMISLM'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get reporting date (simple version)
    reptdate = datetime.today()
    reptyear = reptdate.strftime('%Y')
    reptmon = reptdate.strftime('%m') 
    reptday = reptdate.strftime('%d')
    rdate = reptdate.strftime('%d%m%Y')
    edate = int(reptdate.strftime('%j'))  # Julian day
    
    print(f"📅 Reporting Date: {rdate} (Day {edate})")
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    try:
        # Define products to process
        products = [
            ('AWSA', 'AL-WADIAH SAVINGS ACCOUNT'),
            ('AWCB', 'AL-WADIAH CURRENT ACCOUNT'), 
            ('MUDH', 'MUDHARABAH BESTARI SAVINGS'),
            ('AWSB', 'BASIC WADIAH SAVINGS ACCOUNT'),
            ('AWCA', 'BASIC WADIAH CURRENT ACCOUNT')
        ]
        
        reports_generated = 0
        
        for product_code, product_name in products:
            print(f"📊 Processing {product_name} ({product_code})...")
            
            try:
                # Read and process data using DuckDB
                query = f"""
                SELECT 
                    DEPRANGE,
                    RACE, 
                    PURPOSE,
                    SUM(NOACCT / {edate}) as NOACCT_DAILY,
                    SUM(ACCYTD / {edate}) as ACCYTD_DAILY,
                    SUM(CASE WHEN REPTDATE < {edate} THEN 0.00 ELSE CURBAL END) as ADJ_CURBAL,
                    SUM(CURBAL / {edate}) as AVGAMT
                FROM read_parquet('{BASE_PATH}/MIS/{product_code}{reptmon}/*.parquet')
                WHERE REPTDATE <= {edate}
                GROUP BY DEPRANGE, RACE, PURPOSE
                ORDER BY DEPRANGE
                """
                
                df = conn.execute(query).pl()
                
                if not df.is_empty():
                    # Generate simple report
                    report_file = f"{OUTPUT_DIR}/{product_code}_report_{rdate}.txt"
                    
                    with open(report_file, 'w') as f:
                        f.write(f"{product_name} - Report as at {rdate}\n")
                        f.write("=" * 50 + "\n")
                        
                        # Summary statistics
                        total_accounts = df['NOACCT_DAILY'].sum()
                        total_balance = df['ADJ_CURBAL'].sum()
                        
                        f.write(f"Total Accounts: {total_accounts:,.0f}\n")
                        f.write(f"Total Balance: {total_balance:,.2f}\n")
                        f.write(f"Average Balance per Account: {total_balance/total_accounts if total_accounts > 0 else 0:,.2f}\n")
                        
                        f.write("\nBy Deposit Range:\n")
                        for row in df.iter_rows(named=True):
                            f.write(f"{row['DEPRANGE']}: {row['NOACCT_DAILY']:,.0f} accounts, {row['ADJ_CURBAL']:,.2f} balance\n")
                    
                    print(f"✅ Generated: {report_file}")
                    reports_generated += 1
                else:
                    print(f"⚠ No data for {product_code}")
                    
            except Exception as e:
                print(f"❌ Error processing {product_code}: {e}")
                continue
        
        # Execution summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎉 EIBARPTS Completed!")
        print(f"⏱️  Duration: {duration}")
        print(f"📄 Reports Generated: {reports_generated}")
        print(f"📁 Output: {OUTPUT_DIR}")
        
        return True
        
    except Exception as e:
        print(f"💥 Job failed: {e}")
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Simple one-line execution
    success = run_eibapts()
    exit(0 if success else 1)
