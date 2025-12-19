from pathlib import Path
from datetime import datetime
import polars as pl
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
INPUT_TEXT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/R6999_BAL_NOTE_20251218.txt"
BASE_OUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output"

# Output columns matching SAS format
OUTPUT_COLUMNS = [
    "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
    "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
    "OTHERFEE", "CTPRINT", "CTPRNNAC"
]

def extract_batch_date(filename: str) -> datetime:
    """Extract batch date from filename"""
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(1)
        return datetime.strptime(date_str, '%Y%m%d')
    return datetime.now()

def clean_number(num_str: str) -> str:
    """Clean and format a number string"""
    if not num_str or num_str == '.' or num_str == '-':
        return '0.00'
    
    cleaned = num_str.replace(',', '')
    if '.' in cleaned:
        parts = cleaned.split('.')
        if len(parts) == 2:
            decimal_places = len(parts[1])
            if decimal_places <= 2:
                return f"{float(cleaned):.2f}"
            elif decimal_places <= 7:
                return f"{float(cleaned):.7f}"
    return cleaned

def parse_line(line: str) -> dict:
    """Parse a fixed-width line from the input file"""
    line = line.rstrip('\n')
    if not line.strip() or len(line) < 11:
        return None
    
    try:
        # Extract COSTCT and NOTETYPE
        costct = line[0:7].strip()
        notetype = line[8:11].strip()
        
        if not costct or not notetype:
            return None
        
        costct_int = int(costct)
        notetype_int = int(notetype)
        
        # Find all numbers in the line
        numbers = re.findall(r'[-\d,.]+', line[11:])
        
        # Create record with defaults
        record = {
            'COSTCT': costct_int,
            'NOTETYPE': notetype_int,
            'PRINBAL': '0.00',
            'PRNNACCR': '0.00',
            'INTACCR': '0.0000000',
            'INTNACCR': '0.0000000',
            'UNERNINT': '0.00',
            'UNERNNON': '0.00',
            'RSRVFIN': '0.00',
            'RSRVDLR': '0.00',
            'NONDDA': '0.00',
            'LATEFEES': '0.00',
            'OTHERFEE': '0.00',
            'CTPRINT': '0',
            'CTPRNNAC': '0'
        }
        
        # Map numbers to fields
        field_mapping = [
            ('PRINBAL', 0), ('PRNNACCR', 1), ('INTACCR', 2), ('INTNACCR', 3),
            ('UNERNINT', 4), ('UNERNNON', 5), ('RSRVFIN', 6), ('RSRVDLR', 7),
            ('NONDDA', 8), ('LATEFEES', 9), ('OTHERFEE', 10), ('CTPRINT', 11),
            ('CTPRNNAC', 12)
        ]
        
        for field_name, idx in field_mapping:
            if idx < len(numbers):
                record[field_name] = clean_number(numbers[idx])
        
        return record
        
    except Exception as e:
        logger.debug(f"Error parsing line: {e}")
        return None

def process_text_file(file_path: Path) -> list:
    """Process the structured text file"""
    logger.info(f"Processing text file: {file_path}")
    
    records = []
    line_count = 0
    
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line_count += 1
                if line_count % 10000 == 0:
                    logger.info(f"Processed {line_count:,} lines...")
                
                record = parse_line(line)
                if record:
                    records.append(record)
        
        logger.info(f"Total records parsed: {len(records):,}")
        return records
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return []

def create_dataframe(records: list) -> pl.DataFrame:
    """Create DataFrame from parsed records with SAS formatting"""
    if not records:
        raise ValueError("No records to create DataFrame")
    
    # Format with proper right alignment (mimics SAS RIGHT() function)
    formatted_data = []
    for record in records:
        formatted_record = {
            'COSTCT': record['COSTCT'],
            'NOTETYPE': record['NOTETYPE'],
            'PRINBAL': str(record['PRINBAL']).rjust(24),
            'PRNNACCR': str(record['PRNNACCR']).rjust(24),
            'INTACCR': str(record['INTACCR']).rjust(21),
            'INTNACCR': str(record['INTNACCR']).rjust(21),
            'UNERNINT': str(record['UNERNINT']).rjust(24),
            'UNERNNON': str(record['UNERNNON']).rjust(24),
            'RSRVFIN': str(record['RSRVFIN']).rjust(24),
            'RSRVDLR': str(record['RSRVDLR']).rjust(24),
            'NONDDA': str(record['NONDDA']).rjust(24),
            'LATEFEES': str(record['LATEFEES']).rjust(24),
            'OTHERFEE': str(record['OTHERFEE']).rjust(24),
            'CTPRINT': str(record['CTPRINT']).rjust(7),
            'CTPRNNAC': str(record['CTPRNNAC']).rjust(7)
        }
        formatted_data.append(formatted_record)
    
    return pl.DataFrame(formatted_data, schema=OUTPUT_COLUMNS)

def save_output(df: pl.DataFrame, output_dir: Path, reptmon: str, reptyear: str):
    """Save DataFrame to parquet and text files"""
    # Create output directory
    out_dir = output_dir / "LNR6999"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Construct output filename
    out_name = f"R6999{reptmon}{reptyear}.parquet"
    output_path = out_dir / out_name
    
    # Write to parquet
    df.write_parquet(output_path)
    logger.info(f"Parquet output written to: {output_path}")
    
    # Also save text version for verification
    txt_path = output_path.with_suffix('.txt')
    with open(txt_path, 'w') as f:
        # Write header matching SAS format
        header = (
            "COSTCT NOTETYPE PRINBAL                PRNNACCR               "
            "INTACCR              INTNACCR              UNERNINT                "
            "UNERNNON                RSRVFIN                 RSRVDLR                 "
            "NONDDA                  LATEFEES                 OTHERFEE                 "
            "CTPRINT CTPRNNAC"
        )
        f.write(header + "\n")
        
        # Write data with proper formatting
        for row in df.iter_rows(named=True):
            line = (
                f"{row['COSTCT']:07d} "
                f"{row['NOTETYPE']:03d} "
                f"{row['PRINBAL']:>24} "
                f"{row['PRNNACCR']:>24} "
                f"{row['INTACCR']:>21} "
                f"{row['INTNACCR']:>21} "
                f"{row['UNERNINT']:>24} "
                f"{row['UNERNNON']:>24} "
                f"{row['RSRVFIN']:>24} "
                f"{row['RSRVDLR']:>24} "
                f"{row['NONDDA']:>24} "
                f"{row['LATEFEES']:>24} "
                f"{row['OTHERFEE']:>24} "
                f"{row['CTPRINT']:>7} "
                f"{row['CTPRNNAC']:>7}"
            )
            f.write(line + "\n")
    
    logger.info(f"Text version saved to: {txt_path}")

def main():
    """Main processing function"""
    logger.info("=" * 60)
    logger.info("Starting Loan Data Processing")
    logger.info(f"Input file: {INPUT_TEXT_PATH}")
    logger.info("=" * 60)
    
    # Validate input
    input_path = Path(INPUT_TEXT_PATH)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        exit(1)
    
    # Extract batch date
    batch_dt = extract_batch_date(input_path.name)
    batch_dt_str = batch_dt.strftime("%Y%m%d")
    reptmon = f"{batch_dt.month:02d}"
    reptyear = f"{batch_dt.year % 100:02d}"
    
    logger.info(f"Processing for batch date: {batch_dt_str}")
    logger.info(f"REPTMON: {reptmon}, REPTYEAR: {reptyear}")
    
    # Process the file
    records = process_text_file(input_path)
    if not records:
        logger.error("No records processed from text file")
        exit(1)
    
    # Create DataFrame
    try:
        df = create_dataframe(records)
        logger.info(f"Created DataFrame with {len(df)} records")
        
        # Show summary
        logger.info(f"Unique COSTCT values: {df['COSTCT'].n_unique()}")
        logger.info(f"Unique NOTETYPE values: {df['NOTETYPE'].n_unique()}")
        
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        exit(1)
    
    # Save output
    output_dir = Path(BASE_OUT_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        save_output(df, output_dir, reptmon, reptyear)
        logger.info("Processing completed successfully")
        logger.info(f"Output file: R6999{reptmon}{reptyear}.parquet")
        
    except Exception as e:
        logger.error(f"Error saving output: {e}")
        exit(1)

if __name__ == "__main__":
    main()
