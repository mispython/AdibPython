import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import struct
import sys

def read_packed_decimal(data: bytes, offset: int, length: int, decimals: int = 0) -> float:
    """Read IBM packed decimal (PD) format."""
    packed = data[offset:offset + length]
    result = 0
    for i, byte in enumerate(packed[:-1]):
        result = result * 100 + (byte >> 4) * 10 + (byte & 0x0F)
    last_byte = packed[-1]
    result = result * 10 + (last_byte >> 4)
    sign = last_byte & 0x0F
    if sign in (0x0D, 0x0B):  # negative signs
        result = -result
    return result / (10 ** decimals)

def validate_billfile(file_path: Path) -> None:
    """Validate header and footer of bill file."""
    with open(file_path, 'rb') as f:
        lines = f.readlines()
    
    if len(lines) < 2:
        raise ValueError("File incomplete - missing header or footer")
    
    # Check header (first line)
    header = lines[0][:2].decode('ascii', errors='ignore')
    if header != 'HT':
        raise ValueError(f"Invalid header: expected 'HT', got '{header}'")
    
    # Parse header date (DDMMYYYY at position 3-10)
    header_date_str = lines[0][2:10].decode('ascii', errors='ignore')
    header_date = datetime.strptime(header_date_str, '%d%m%Y').date()
    
    # Check footer (last line)
    footer = lines[-1][:2].decode('ascii', errors='ignore')
    if footer != 'FT':
        raise ValueError(f"Invalid footer: expected 'FT', got '{footer}'")
    
    # Parse footer count (15 digits at position 3-17)
    footer_count_str = lines[-1][2:17].decode('ascii', errors='ignore')
    footer_count = int(footer_count_str)
    
    # Calculate expected values
    expected_date = (datetime.now() - timedelta(days=1)).date()
    expected_count = len(lines) - 2  # Exclude header and footer
    
    # Validate
    errors = []
    if header_date != expected_date:
        errors.append(f"File Header ERROR: LNBIL_DTE={header_date.strftime('%d/%m/%Y')} <> SAS_DTE={expected_date.strftime('%d/%m/%Y')}")
    if footer_count != expected_count:
        errors.append(f"File Footer ERROR: LNBIL_OBS={footer_count:015d} <> SAS_OBS={expected_count:015d}")
    
    if errors:
        print("DSN=RBP2.B033.BILLFILE.MIS(0)")
        print("+--------+--------+-------+-------+--------+-------+")
        print("|  ERROR MESSAGE(S) STATUS                         |")
        print("+--------+--------+-------+-------+--------+-------+")
        for error in errors:
            print(f"|  {error:<48}  |")
        print("|                                                  |")
        print("+========+========+=======+=======+========+=======+")
        sys.exit(77)

def process_billfile(input_path: Path, output_path: Path) -> None:
    """Process bill file from binary format to parquet."""
    
    # Validate file first
    validate_billfile(input_path)
    
    # Read and parse data records (skip header, stop before footer)
    records = []
    with open(input_path, 'rb') as f:
        lines = f.readlines()
        
    for line in lines[1:-1]:  # Skip first (header) and last (footer)
        if len(line) < 110:
            continue
            
        # Check if valid data record (ACCTNO not null)
        acctno = read_packed_decimal(line, 0, 6)
        if acctno == 0:
            continue
        
        record = {
            'ACCTNO': int(acctno),
            'NOTENO': int(read_packed_decimal(line, 6, 3)),
            'BLDATE': int(read_packed_decimal(line, 9, 6)),
            'BLPDDATE': int(read_packed_decimal(line, 15, 6)),
            'DAYSLATE': int(read_packed_decimal(line, 21, 2)),
            'PRODUCT': int(read_packed_decimal(line, 23, 2)),
            'COSTCTR': int(read_packed_decimal(line, 25, 4)),
            'BILL_AMT': read_packed_decimal(line, 29, 8, 2),
            'BILL_AMT_PRIN': read_packed_decimal(line, 37, 8, 2),
            'BILL_AMT_INT': read_packed_decimal(line, 45, 8, 2),
            'BILL_AMT_ESCROW': read_packed_decimal(line, 53, 8, 2),
            'BILL_AMT_FEE': read_packed_decimal(line, 61, 8, 2),
            'BILL_NOT_PAY_AMT': read_packed_decimal(line, 69, 8, 2),
            'BILL_NOT_PAY_AMT_PRIN': read_packed_decimal(line, 77, 8, 2),
            'BILL_NOT_PAY_AMT_INT': read_packed_decimal(line, 85, 8, 2),
            'BILL_NOT_PAY_AMT_ESCROW': read_packed_decimal(line, 93, 8, 2),
            'BILL_NOT_PAY_AMT_FEE': read_packed_decimal(line, 101, 8, 2),
        }
        records.append(record)
    
    # Create Polars DataFrame
    df = pl.DataFrame(records)
    
    # Write to parquet
    df.write_parquet(output_path)
    print(f"Processed {len(records)} records to {output_path}")

if __name__ == "__main__":
    input_file = Path("BILLFILE.dat")
    output_file = Path("STG_LN_BILL.parquet")
    
    process_billfile(input_file, output_file)
