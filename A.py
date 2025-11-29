import duckdb
from pathlib import Path
from datetime import datetime, timedelta

def check_historical_records():
    """Simple program to count records in historical LOAN_BILL and ILOAN_BILL files"""
    
    print("="*50)
    print("HISTORICAL RECORDS COUNTER")
    print("="*50)
    
    # Set the date for the historical files you want to check
    CHECK_DATE = datetime.today() - timedelta(days=1)
    
    print(f"Checking date: {CHECK_DATE.strftime('%Y-%m-%d')}")
    print()
    
    # Define historical file paths
    hist_dir = Path(f"/parquet/dwh/LOAN/year={CHECK_DATE.year}/month={CHECK_DATE.month:02d}/day={CHECK_DATE.day:02d}")
    loan_bill_path = hist_dir / "LOAN_BILL.parquet"
    iloan_bill_path = hist_dir / "ILOAN_BILL.parquet"
    
    # Initialize DuckDB connection
    con = duckdb.connect()
    
    # Check LOAN_BILL
    print("LOAN_BILL:")
    print(f"  Path: {loan_bill_path}")
    if loan_bill_path.exists():
        loan_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{loan_bill_path}')").fetchone()[0]
        print(f"  ✓ Records: {loan_count:,}")
    else:
        print(f"  ✗ NOT FOUND")
    
    print()
    
    # Check ILOAN_BILL
    print("ILOAN_BILL:")
    print(f"  Path: {iloan_bill_path}")
    if iloan_bill_path.exists():
        iloan_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{iloan_bill_path}')").fetchone()[0]
        print(f"  ✓ Records: {iloan_count:,}")
    else:
        print(f"  ✗ NOT FOUND")
    
    print()
    
    # Summary
    if loan_bill_path.exists() and iloan_bill_path.exists():
        total_records = loan_count + iloan_count
        print("SUMMARY:")
        print(f"  Total: {total_records:,} records")
        print(f"  LOAN_BILL: {loan_count:,} ({loan_count/total_records*100:.1f}%)")
        print(f"  ILOAN_BILL: {iloan_count:,} ({iloan_count/total_records*100:.1f}%)")
    
    # Close connection
    con.close()
    
    print("="*50)

if __name__ == "__main__":
    check_historical_records()
