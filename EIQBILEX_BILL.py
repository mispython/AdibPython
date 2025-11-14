import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
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
        print(f"DSN={file_path.name}")
        print("+--------+--------+-------+-------+--------+-------+")
        print("|  ERROR MESSAGE(S) STATUS                         |")
        print("+--------+--------+-------+-------+--------+-------+")
        for error in errors:
            print(f"|  {error:<48}  |")
        print("|                                                  |")
        print("+========+========+=======+=======+========+=======+")
        sys.exit(77)

def process_single_billfile(file_path: Path) -> pl.DataFrame:
    """Process single bill file from binary format to DataFrame."""
    
    # Validate file first
    validate_billfile(file_path)
    
    # Read and parse data records (skip header, stop before footer)
    records = []
    with open(file_path, 'rb') as f:
        lines = f.readlines()
        
    for line in lines[1:-1]:  # Skip first (header) and last (footer)
        if len(line) < 110:
            continue
            
        # Check if valid data record (BILL_ACCT_NO not null)
        acct_no = read_packed_decimal(line, 0, 6)
        if acct_no == 0:
            continue
        
        record = {
            'BILL_ACCT_NO': int(acct_no),
            'BILL_NOTE_NO': int(read_packed_decimal(line, 6, 3)),
            'BILL_DUE_DATE': int(read_packed_decimal(line, 9, 6)),
            'BILL_PAID_DATE': int(read_packed_decimal(line, 15, 6)),
            'BILL_DAYS_LATE': int(read_packed_decimal(line, 21, 2)),
            'BILL_NOTE_TYPE': int(read_packed_decimal(line, 23, 2)),
            'BILL_COST_CENTER': int(read_packed_decimal(line, 25, 4)),
            'BILL_TOT_BILLED': int(read_packed_decimal(line, 29, 8)),
            'BILL_PRIN_BILLED': int(read_packed_decimal(line, 37, 8)),
            'BILL_INT_BILLED': int(read_packed_decimal(line, 45, 8)),
            'BILL_ESC_BILLED': int(read_packed_decimal(line, 53, 8)),
            'BILL_FEE_BILLED': int(read_packed_decimal(line, 61, 8)),
            'BILL_TOT_BNP': int(read_packed_decimal(line, 69, 8)),
            'BILL_PRIN_BNP': int(read_packed_decimal(line, 77, 8)),
            'BILL_INT_BNP': int(read_packed_decimal(line, 85, 8)),
            'BILL_ESC_BNP': int(read_packed_decimal(line, 93, 8)),
            'BILL_FEE_BNP': int(read_packed_decimal(line, 101, 8)),
        }
        records.append(record)
    
    print(f"Processed {len(records):,} records from {file_path.name}")
    return pl.DataFrame(records) if records else pl.DataFrame()

def process_billfiles(input_dir: Path, output_path: Path) -> None:
    """Process all MST billfiles and combine into single parquet."""
    
    # Define input file pattern
    input_files = [
        input_dir / f"RBP2.B033.MST{i:02d}.BILLFILE.MIS.parquet"
        for i in range(1, 11)
    ]
    
    # Check if files exist
    existing_files = [f for f in input_files if f.exists()]
    
    if not existing_files:
        print(f"ERROR: No input files found in {input_dir}")
        sys.exit(1)
    
    print(f"Found {len(existing_files)} input files")
    print("-" * 60)
    
    # Process each file
    dataframes = []
    for file_path in existing_files:
        try:
            # Read parquet file (assuming it contains binary data column)
            df_input = pl.read_parquet(file_path)
            
            # If parquet contains raw binary data, process it
            # Otherwise, assume it's already processed and just append
            if 'raw_data' in df_input.columns:
                # Process binary data from parquet
                df = process_single_billfile(file_path)
            else:
                # Already processed data
                df = df_input
                print(f"Loaded {len(df):,} records from {file_path.name}")
            
            if len(df) > 0:
                dataframes.append(df)
        except Exception as e:
            print(f"ERROR processing {file_path.name}: {e}")
            sys.exit(1)
    
    # Combine all dataframes
    if dataframes:
        combined_df = pl.concat(dataframes)
        print("-" * 60)
        print(f"Total records: {len(combined_df):,}")
        
        # Write to output parquet
        combined_df.write_parquet(output_path)
        print(f"Output written to: {output_path}")
    else:
        print("ERROR: No data to process")
        sys.exit(1)

if __name__ == "__main__":
    input_directory = Path(".")  # Current directory
    output_file = Path("STG_LN_BILL.parquet")
    
    process_billfiles(input_directory, output_file)
