import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

def read_packed_decimal(data: bytes, offset: int, length: int) -> int:
    """Read IBM packed decimal (PD) format."""
    packed = data[offset:offset + length]
    result = 0
    for i, byte in enumerate(packed[:-1]):
        result = result * 100 + (byte >> 4) * 10 + (byte & 0x0F)
    last_byte = packed[-1]
    result = result * 10 + (last_byte >> 4)
    sign = last_byte & 0x0F
    if sign in (0x0D, 0x0B):
        result = -result
    return result

def calculate_report_dates(reptdate: datetime) -> dict:
    """
    Calculate REPTDATE and PDATE based on SAS logic.
    If month >= 6: PDATE = (month-5, year-2)
    Else: PDATE = (month+7, year-3)
    """
    month = reptdate.month
    year = reptdate.year
    
    if month >= 6:
        pmth = month - 5
        pyear = year - 2
    else:
        pmth = month + 7
        pyear = year - 3
    
    pdate = datetime(pyear, pmth, 1).date()
    
    print(f"REPTDATE: {reptdate.date()}")
    print(f"PDATE (2.5 years back): {pdate}")
    print("-" * 80)
    
    return reptdate.date(), pdate

def parse_packed_date(packed_value: int) -> datetime.date:
    """
    Convert packed decimal date (YYYYMMDD as integer) to date.
    Example: 20231115 -> 2023-11-15
    """
    if packed_value == 0:
        return None
    
    date_str = str(packed_value).zfill(8)
    try:
        return datetime.strptime(date_str, '%Y%m%d').date()
    except:
        return None

def process_billfile_binary(input_path: Path, reptdate: datetime, output_path: Path) -> None:
    """
    Process bill file from binary format using DuckDB.
    Handles packed decimal data and converts to parquet.
    """
    
    reptdate_val, pdate_val = calculate_report_dates(reptdate)
    
    print("Reading binary bill file...")
    records = []
    with open(input_path, 'rb') as f:
        lines = f.readlines()
    
    # Skip header and footer if present
    start_line = 1 if lines[0][:2] == b'HT' else 0
    end_line = -1 if lines[-1][:2] == b'FT' else None
    
    for line in lines[start_line:end_line]:
        if len(line) < 22:
            continue
        
        acctno = read_packed_decimal(line, 0, 6)
        if acctno == 0:
            continue
        
        noteno = read_packed_decimal(line, 6, 3)
        bldate_packed = read_packed_decimal(line, 9, 6)
        blpddate_packed = read_packed_decimal(line, 15, 6)
        dayslate = read_packed_decimal(line, 21, 2)
        
        bldat = parse_packed_date(bldate_packed)
        blpddat = parse_packed_date(blpddate_packed)
        
        # Calculate DAYS
        if bldat is None:
            days = 0
        elif blpddat is None:
            days = (reptdate_val - bldat).days
        else:
            days = (blpddat - bldat).days
        
        records.append({
            'ACCTNO': acctno,
            'NOTENO': noteno,
            'BLDAT': bldat,
            'BLPDDAT': blpddat,
            'DAYSLATE': dayslate,
            'DAYS': days
        })
    
    print(f"Total records read: {len(records):,}")
    
    # Use DuckDB for filtering and sorting
    con = duckdb.connect(':memory:')
    
    con.execute("CREATE TABLE bill AS SELECT * FROM records")
    
    # Filter: BLDAT >= PDATE AND DAYS > 0
    con.execute(f"""
        CREATE TABLE bill_filtered AS
        SELECT * FROM bill
        WHERE BLDAT >= DATE '{pdate_val}'
          AND DAYS > 0
        ORDER BY ACCTNO, NOTENO
    """)
    
    result = con.execute("SELECT * FROM bill_filtered").pl()
    
    print(f"Records after filtering (BLDAT >= {pdate_val} AND DAYS > 0): {len(result):,}")
    
    result.write_parquet(output_path)
    print(f"Output written to: {output_path}")
    
    print("\nSample output (first 10 rows):")
    print(result.head(10))
    
    con.close()

def process_billfile_parquet(input_path: Path, reptdate: datetime, output_path: Path) -> None:
    """
    Process bill file from parquet format using DuckDB.
    Assumes input already has: ACCTNO, NOTENO, BLDAT, BLPDDAT, DAYSLATE.
    """
    
    reptdate_val, pdate_val = calculate_report_dates(reptdate)
    
    con = duckdb.connect(':memory:')
    
    print("Reading parquet bill file...")
    
    # Read input parquet
    con.execute(f"""
        CREATE TABLE bill AS
        SELECT 
            ACCTNO, NOTENO, BLDAT, BLPDDAT, DAYSLATE,
            CASE 
                WHEN BLDAT IS NULL THEN 0
                WHEN BLPDDAT IS NULL THEN DATEDIFF('day', BLDAT, DATE '{reptdate_val}')
                ELSE DATEDIFF('day', BLDAT, BLPDDAT)
            END AS DAYS
        FROM read_parquet('{input_path}')
    """)
    
    count = con.execute("SELECT COUNT(*) FROM bill").fetchone()[0]
    print(f"Total records read: {count:,}")
    
    # Filter: BLDAT >= PDATE AND DAYS > 0
    con.execute(f"""
        CREATE TABLE bill_filtered AS
        SELECT * FROM bill
        WHERE BLDAT >= DATE '{pdate_val}'
          AND DAYS > 0
    """)
    
    count_filtered = con.execute("SELECT COUNT(*) FROM bill_filtered").fetchone()[0]
    print(f"Records after filtering (BLDAT >= {pdate_val} AND DAYS > 0): {count_filtered:,}")
    
    # Sort by ACCTNO, NOTENO
    con.execute("""
        CREATE TABLE bill_sorted AS
        SELECT * FROM bill_filtered
        ORDER BY ACCTNO, NOTENO
    """)
    
    # Save to parquet
    result = con.execute("SELECT * FROM bill_sorted").pl()
    result.write_parquet(output_path)
    
    print(f"Output written to: {output_path}")
    
    print("\nSample output (first 10 rows):")
    print(result.head(10))
    
    con.close()

def process_billfile_multiple_parquet(
    input_dir: Path,
    reptdate: datetime,
    output_path: Path,
    num_files: int = 10
) -> None:
    """
    Process multiple bill files (MST01-MST10) and combine into single parquet.
    Uses DuckDB for efficient processing.
    """
    
    reptdate_val, pdate_val = calculate_report_dates(reptdate)
    
    con = duckdb.connect(':memory:')
    
    # Define input file pattern
    input_files = [
        input_dir / f"RBP2.B033.MST{i:02d}.BILLFILE.MIS.parquet"
        for i in range(1, num_files + 1)
    ]
    
    # Check existing files
    existing_files = [f for f in input_files if f.exists()]
    
    if not existing_files:
        print(f"ERROR: No input files found in {input_dir}")
        return
    
    print(f"Found {len(existing_files)} input files")
    print("-" * 80)
    
    # Create UNION ALL query for all files
    union_queries = []
    for file_path in existing_files:
        union_queries.append(f"SELECT * FROM read_parquet('{file_path}')")
    
    union_sql = " UNION ALL ".join(union_queries)
    
    print("Reading and combining all parquet files...")
    con.execute(f"""
        CREATE TABLE bill_all AS
        SELECT * FROM ({union_sql})
    """)
    
    count = con.execute("SELECT COUNT(*) FROM bill_all").fetchone()[0]
    print(f"Total records from all files: {count:,}")
    
    # Calculate DAYS if not present
    columns = con.execute("PRAGMA table_info(bill_all)").fetchall()
    column_names = [col[1] for col in columns]
    
    if 'DAYS' not in column_names:
        print("Calculating DAYS...")
        con.execute(f"""
            CREATE TABLE bill AS
            SELECT *,
                CASE 
                    WHEN BLDAT IS NULL THEN 0
                    WHEN BLPDDAT IS NULL THEN DATEDIFF('day', BLDAT, DATE '{reptdate_val}')
                    ELSE DATEDIFF('day', BLDAT, BLPDDAT)
                END AS DAYS
            FROM bill_all
        """)
        con.execute("DROP TABLE bill_all")
    else:
        con.execute("ALTER TABLE bill_all RENAME TO bill")
    
    # Filter: BLDAT >= PDATE AND DAYS > 0
    print("Applying filters...")
    con.execute(f"""
        CREATE TABLE bill_filtered AS
        SELECT * FROM bill
        WHERE BLDAT >= DATE '{pdate_val}'
          AND DAYS > 0
    """)
    
    count_filtered = con.execute("SELECT COUNT(*) FROM bill_filtered").fetchone()[0]
    print(f"Records after filtering (BLDAT >= {pdate_val} AND DAYS > 0): {count_filtered:,}")
    
    # Sort by ACCTNO, NOTENO
    print("Sorting...")
    con.execute("""
        CREATE TABLE bill_sorted AS
        SELECT * FROM bill_filtered
        ORDER BY ACCTNO, NOTENO
    """)
    
    # Save to parquet
    result = con.execute("SELECT * FROM bill_sorted").pl()
    result.write_parquet(output_path)
    
    print(f"\nOutput written to: {output_path}")
    print(f"Final record count: {len(result):,}")
    
    print("\nSample output (first 10 rows):")
    print(result.head(10))
    
    con.close()

if __name__ == "__main__":
    # Set report date (from BNM.REPTDATE)
    report_date = datetime.now()  # Replace with actual REPTDATE
    
    # Option 1: Process from binary file
    # process_billfile_binary(
    #     Path("BILLFILE.dat"),
    #     report_date,
    #     Path("LNBILL_BILL.parquet")
    # )
    
    # Option 2: Process from single parquet
    # process_billfile_parquet(
    #     Path("STG_LN_BILL.parquet"),
    #     report_date,
    #     Path("LNBILL_BILL.parquet")
    # )
    
    # Option 3: Process from multiple parquet files (MST01-MST10)
    process_billfile_multiple_parquet(
        input_dir=Path("."),
        reptdate=report_date,
        output_path=Path("LNBILL_BILL.parquet"),
        num_files=10
    )
