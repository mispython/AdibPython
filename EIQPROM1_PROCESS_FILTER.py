import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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

def calculate_report_dates(reptdate: datetime) -> tuple:
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
    
    # First day of the calculated month
    pdate = datetime(pyear, pmth, 1).date()
    
    print(f"REPTDATE: {reptdate.date()}")
    print(f"PDATE (2.5 years back): {pdate}")
    print("-" * 60)
    
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

def process_billfile(input_path: Path, reptdate: datetime, output_path: Path) -> None:
    """
    Process bill file with date filtering.
    
    Logic:
    1. Calculate PDATE (2.5 years before REPTDATE)
    2. Parse bill records from binary file
    3. Calculate DAYS (days between bill date and paid date or report date)
    4. Filter: BLDAT >= PDATE AND DAYS > 0
    5. Sort by ACCTNO, NOTENO
    """
    
    # Calculate report dates
    reptdate_val, pdate_val = calculate_report_dates(reptdate)
    
    # Read and parse bill records
    records = []
    with open(input_path, 'rb') as f:
        lines = f.readlines()
    
    # Skip header and footer if present
    start_line = 1 if lines[0][:2] == b'HT' else 0
    end_line = -1 if lines[-1][:2] == b'FT' else None
    
    for line in lines[start_line:end_line]:
        if len(line) < 22:
            continue
        
        # Read fields
        acctno = read_packed_decimal(line, 0, 6)
        if acctno == 0:
            continue
        
        noteno = read_packed_decimal(line, 6, 3)
        bldate_packed = read_packed_decimal(line, 9, 6)
        blpddate_packed = read_packed_decimal(line, 15, 6)
        dayslate = read_packed_decimal(line, 21, 2)
        
        # Parse dates
        bldat = parse_packed_date(bldate_packed)
        blpddat = parse_packed_date(blpddate_packed)
        
        # Calculate DAYS
        if bldat is None:
            days = 0
        elif blpddat is None:
            # Use REPTDATE if no paid date
            days = (reptdate_val - bldat).days
        else:
            # Use paid date
            days = (blpddat - bldat).days
        
        record = {
            'ACCTNO': acctno,
            'NOTENO': noteno,
            'BLDAT': bldat,
            'BLPDDAT': blpddat,
            'DAYSLATE': dayslate,
            'DAYS': days
        }
        records.append(record)
    
    print(f"Total records read: {len(records):,}")
    
    # Create DataFrame
    df = pl.DataFrame(records)
    
    # Filter: Bill within 2.5 years and more than 0 days due
    df_filtered = df.filter(
        (pl.col('BLDAT') >= pdate_val) & 
        (pl.col('DAYS') > 0)
    )
    
    print(f"Records after filtering (BLDAT >= {pdate_val} AND DAYS > 0): {len(df_filtered):,}")
    
    # Sort by ACCTNO, NOTENO
    df_sorted = df_filtered.sort(['ACCTNO', 'NOTENO'])
    
    # Write to parquet
    df_sorted.write_parquet(output_path)
    print(f"Output written to: {output_path}")
    
    # Print sample (first 100 rows)
    print("\nSample output (first 10 rows):")
    print(df_sorted.head(10))

def process_from_parquet(input_parquet: Path, reptdate: datetime, output_path: Path) -> None:
    """
    Process bill data from existing parquet file.
    Assumes input already has: ACCTNO, NOTENO, BLDAT, BLPDDAT, DAYSLATE columns.
    """
    
    # Calculate report dates
    reptdate_val, pdate_val = calculate_report_dates(reptdate)
    
    # Read input parquet
    df = pl.read_parquet(input_parquet)
    
    # Ensure date columns are in date format
    if df['BLDAT'].dtype != pl.Date:
        df = df.with_columns(pl.col('BLDAT').cast(pl.Date))
    if df['BLPDDAT'].dtype != pl.Date:
        df = df.with_columns(pl.col('BLPDDAT').cast(pl.Date))
    
    # Calculate DAYS if not present
    if 'DAYS' not in df.columns:
        df = df.with_columns(
            pl.when(pl.col('BLDAT').is_null())
            .then(pl.lit(0))
            .when(pl.col('BLPDDAT').is_null())
            .then((pl.lit(reptdate_val) - pl.col('BLDAT')).dt.total_days())
            .otherwise((pl.col('BLPDDAT') - pl.col('BLDAT')).dt.total_days())
            .alias('DAYS')
        )
    
    print(f"Total records read: {len(df):,}")
    
    # Filter: Bill within 2.5 years and more than 0 days due
    df_filtered = df.filter(
        (pl.col('BLDAT') >= pdate_val) & 
        (pl.col('DAYS') > 0)
    )
    
    print(f"Records after filtering (BLDAT >= {pdate_val} AND DAYS > 0): {len(df_filtered):,}")
    
    # Sort by ACCTNO, NOTENO
    df_sorted = df_filtered.sort(['ACCTNO', 'NOTENO'])
    
    # Write to parquet
    df_sorted.write_parquet(output_path)
    print(f"Output written to: {output_path}")
    
    # Print sample
    print("\nSample output (first 10 rows):")
    print(df_sorted.head(10))

if __name__ == "__main__":
    # Set report date (typically from BNM.REPTDATE dataset)
    # For production, read from your REPTDATE source
    report_date = datetime.now()  # Replace with actual REPTDATE
    
    # Option 1: Process from binary file
    # process_billfile(
    #     Path("BILLFILE.dat"),
    #     report_date,
    #     Path("LNBILL_BILL.parquet")
    # )
    
    # Option 2: Process from existing parquet
    process_from_parquet(
        Path("STG_LN_BILL.parquet"),
        report_date,
        Path("LNBILL_BILL.parquet")
    )
