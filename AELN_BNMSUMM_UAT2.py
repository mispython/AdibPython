from pathlib import Path
from datetime import datetime
import polars as pl
import logging
import re
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoanDataProcessor:
    """Processor for structured loan data files"""
    
    OUTPUT_COLUMNS = [
        "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
        "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
        "OTHERFEE", "CTPRINT", "CTPRNNAC"
    ]
    
    def __init__(self, input_text_path: str, base_output_path: str, 
                 batch_date_str: str = None):
        self.input_text_path = Path(input_text_path)
        self.BASE_OUT = Path(base_output_path)
        
        # Parse batch date from filename
        if batch_date_str is None:
            filename = self.input_text_path.name
            date_match = re.search(r'(\d{8})', filename)
            if date_match:
                date_str = date_match.group(1)
                batch_date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} 00:00:00"
            else:
                batch_date_str = datetime.now().strftime('%Y-%m-%d 00:00:00')
        
        self.batch_dt = datetime.strptime(batch_date_str, '%Y-%m-%d %H:%M:%S')
        self.batch_dt_str = self.batch_dt.strftime("%Y%m%d")
        
        # Report date formatting
        self.REPTDATE = self.batch_dt
        self.REPTMON = f"{self.REPTDATE.month:02d}"
        self.REPTYEAR = f"{self.REPTDATE.year % 100:02d}"
        
        # Ensure output directory exists
        self.BASE_OUT.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized processor for batch date: {self.batch_dt_str}")
    
    def parse_fixed_width_line(self, line: str) -> Optional[dict]:
        """Parse a fixed-width line from the input file"""
        try:
            # Based on the sample, the format appears to be:
            # Positions 1-7: COSTCT (7 chars)
            # Positions 9-11: NOTETYPE (3 chars, with 1 space before)
            # Positions 12-?: Various balances
            
            # Clean the line
            line = line.rstrip('\n')
            
            # Skip empty lines
            if not line.strip():
                return None
            
            # Extract first two fields
            costct = line[0:7].strip()
            notetype = line[8:11].strip()
            
            # Try to parse all numbers from the line
            # The format seems to have 4-6 numeric fields per line
            # Look for all numeric patterns (with commas, decimals, optional minus)
            number_patterns = re.findall(r'[-\d,.]+', line[11:])
            
            logger.debug(f"Line: {repr(line[:80])}")
            logger.debug(f"CostCT: {costct}, NoteType: {notetype}")
            logger.debug(f"Found numbers: {number_patterns}")
            
            if not costct or not notetype:
                return None
            
            # Convert to integers
            costct_int = int(costct)
            notetype_int = int(notetype)
            
            # Create record with default values
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
            
            # Map numbers to fields based on position
            # From the sample, it looks like positions 0-3 are present
            if len(number_patterns) >= 1:
                # First number appears to be PRINBAL
                record['PRINBAL'] = self._clean_number(number_patterns[0])
            
            if len(number_patterns) >= 2:
                # Second number appears to be PRNNACCR
                record['PRNNACCR'] = self._clean_number(number_patterns[1])
            
            if len(number_patterns) >= 3:
                # Third number appears to be INTACCR
                record['INTACCR'] = self._clean_number(number_patterns[2])
            
            if len(number_patterns) >= 4:
                # Fourth number appears to be INTNACCR
                record['INTNACCR'] = self._clean_number(number_patterns[3])
            
            # Check if there are more fields (for other balances)
            # The file might have all 15 fields in different positions
            
            return record
            
        except Exception as e:
            logger.warning(f"Error parsing line: {e}")
            return None
    
    def _clean_number(self, num_str: str) -> str:
        """Clean and format a number string"""
        if not num_str:
            return '0.00'
        
        # Remove commas
        cleaned = num_str.replace(',', '')
        
        # Handle empty or just minus sign
        if not cleaned or cleaned == '-':
            return '0.00'
        
        # Check if it looks like a decimal number
        if '.' in cleaned:
            # Count decimal places
            parts = cleaned.split('.')
            if len(parts) == 2:
                decimal_places = len(parts[1])
                if decimal_places <= 2:
                    return f"{float(cleaned):.2f}"
                elif decimal_places <= 7:
                    return f"{float(cleaned):.7f}"
        
        return cleaned
    
    def process_text_file(self) -> List[dict]:
        """Process the structured text file"""
        logger.info(f"Processing text file: {self.input_text_path}")
        
        if not self.input_text_path.exists():
            logger.error(f"Input text file not found: {self.input_text_path}")
            return []
        
        try:
            records = []
            
            with open(self.input_text_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    # Skip empty lines
                    if not line.strip():
                        continue
                    
                    # Parse the line
                    record = self.parse_fixed_width_line(line)
                    if record:
                        records.append(record)
                    
                    # Log progress
                    if line_num % 10000 == 0:
                        logger.info(f"Processed {line_num:,} lines...")
            
            logger.info(f"Total records parsed: {len(records):,}")
            
            # Show sample of parsed data
            if records:
                logger.info("Sample parsed records:")
                for i, record in enumerate(records[:3]):
                    logger.info(f"Record {i+1}: COSTCT={record['COSTCT']}, NOTETYPE={record['NOTETYPE']}, "
                               f"PRINBAL={record['PRINBAL']}, INTACCR={record['INTACCR']}")
            
            return records
            
        except Exception as e:
            logger.error(f"Error processing text file: {e}", exc_info=True)
            return []
    
    def analyze_file_format(self):
        """Analyze the file format more thoroughly"""
        logger.info("Analyzing file format in detail...")
        
        try:
            with open(self.input_text_path, 'r') as file:
                lines = [line.rstrip('\n') for line in file if line.strip()]
            
            if not lines:
                logger.error("File is empty")
                return
            
            # Analyze first few lines
            logger.info("\n=== Detailed line analysis ===")
            for i, line in enumerate(lines[:10]):
                logger.info(f"\nLine {i+1}:")
                logger.info(f"Full: {repr(line)}")
                logger.info(f"Length: {len(line)}")
                
                # Show character positions
                logger.info("Character positions:")
                for pos in range(0, min(100, len(line)), 10):
                    segment = line[pos:pos+10]
                    logger.info(f"  {pos:3d}-{pos+9:3d}: {repr(segment)}")
                
                # Extract fields based on visual inspection
                costct = line[0:7].strip()
                notetype = line[8:11].strip()
                
                # Try to find field boundaries by looking for number patterns
                # The numbers seem to start around position 12
                numbers = []
                current_num = ""
                in_number = False
                
                for j, char in enumerate(line[11:], 11):
                    if char in '-.0123456789,':
                        if not in_number:
                            start_pos = j
                            in_number = True
                        current_num += char
                    else:
                        if in_number and current_num.strip():
                            numbers.append((start_pos, current_num.strip()))
                            current_num = ""
                        in_number = False
                
                if in_number and current_num.strip():
                    numbers.append((len(line) - len(current_num), current_num.strip()))
                
                logger.info(f"CostCT: {costct} (pos 0-6)")
                logger.info(f"NoteType: {notetype} (pos 8-10)")
                logger.info(f"Found {len(numbers)} numbers:")
                for pos, num in numbers:
                    logger.info(f"  Pos {pos}: {num}")
            
            # Try to determine field widths by analyzing multiple lines
            if len(lines) > 1:
                logger.info("\n=== Field alignment analysis ===")
                
                # Find all number positions across first 20 lines
                all_number_positions = []
                for line in lines[:20]:
                    # Look for number patterns
                    for match in re.finditer(r'[-\d,.]+', line):
                        all_number_positions.append(match.start())
                
                # Count frequency of number starts at each position
                from collections import Counter
                pos_counter = Counter(all_number_positions)
                
                logger.info("Most common number start positions:")
                for pos, count in pos_counter.most_common(10):
                    logger.info(f"  Position {pos}: {count} occurrences")
            
        except Exception as e:
            logger.error(f"Error analyzing file format: {e}")
    
    def process_with_field_positions(self, field_positions: list) -> List[dict]:
        """Process file using specific field positions"""
        logger.info(f"Processing with field positions: {field_positions}")
        
        # Define expected fields in order
        expected_fields = [
            'PRINBAL', 'PRNNACCR', 'INTACCR', 'INTNACCR',
            'UNERNINT', 'UNERNNON', 'RSRVFIN', 'RSRVDLR',
            'NONDDA', 'LATEFEES', 'OTHERFEE', 'CTPRINT', 'CTPRNNAC'
        ]
        
        records = []
        
        try:
            with open(self.input_text_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.rstrip('\n')
                    
                    if not line.strip():
                        continue
                    
                    # Extract COSTCT and NOTETYPE
                    costct = line[0:7].strip()
                    notetype = line[8:11].strip()
                    
                    if not costct or not notetype:
                        continue
                    
                    try:
                        costct_int = int(costct)
                        notetype_int = int(notetype)
                    except ValueError:
                        continue
                    
                    # Create record
                    record = {
                        'COSTCT': costct_int,
                        'NOTETYPE': notetype_int
                    }
                    
                    # Extract other fields based on positions
                    # Start with defaults
                    for field in expected_fields:
                        if 'PRIN' in field or 'RSRV' in field or 'NONDDA' in field or 'LATE' in field or 'OTHER' in field:
                            record[field] = '0.00'
                        elif 'INT' in field:
                            record[field] = '0.0000000'
                        else:
                            record[field] = '0'
                    
                    # Try to extract numbers
                    # Simple approach: take all numbers after position 11
                    numbers = re.findall(r'[-\d,.]+', line[11:])
                    
                    # Map numbers to fields
                    field_mapping = [
                        ('PRINBAL', 0),
                        ('PRNNACCR', 1),
                        ('INTACCR', 2),
                        ('INTNACCR', 3),
                        ('UNERNINT', 4),
                        ('UNERNNON', 5),
                        ('RSRVFIN', 6),
                        ('RSRVDLR', 7),
                        ('NONDDA', 8),
                        ('LATEFEES', 9),
                        ('OTHERFEE', 10),
                        ('CTPRINT', 11),
                        ('CTPRNNAC', 12)
                    ]
                    
                    for field_name, idx in field_mapping:
                        if idx < len(numbers):
                            record[field_name] = self._clean_number(numbers[idx])
                    
                    records.append(record)
                    
                    if line_num % 10000 == 0:
                        logger.info(f"Processed {line_num:,} lines...")
            
            logger.info(f"Processed {len(records)} records")
            return records
            
        except Exception as e:
            logger.error(f"Error processing with field positions: {e}", exc_info=True)
            return []
    
    def create_dataframe(self, records: List[dict]) -> Optional[pl.DataFrame]:
        """Create DataFrame from parsed records"""
        if not records:
            logger.error("No records to create DataFrame")
            return None
        
        try:
            # Format with proper alignment
            processed_data = []
            
            for record in records:
                formatted_record = {
                    'COSTCT': record['COSTCT'],
                    'NOTETYPE': record['NOTETYPE'],
                    'PRINBAL': str(record.get('PRINBAL', '0.00')).rjust(24),
                    'PRNNACCR': str(record.get('PRNNACCR', '0.00')).rjust(24),
                    'INTACCR': str(record.get('INTACCR', '0.0000000')).rjust(21),
                    'INTNACCR': str(record.get('INTNACCR', '0.0000000')).rjust(21),
                    'UNERNINT': str(record.get('UNERNINT', '0.00')).rjust(24),
                    'UNERNNON': str(record.get('UNERNNON', '0.00')).rjust(24),
                    'RSRVFIN': str(record.get('RSRVFIN', '0.00')).rjust(24),
                    'RSRVDLR': str(record.get('RSRVDLR', '0.00')).rjust(24),
                    'NONDDA': str(record.get('NONDDA', '0.00')).rjust(24),
                    'LATEFEES': str(record.get('LATEFEES', '0.00')).rjust(24),
                    'OTHERFEE': str(record.get('OTHERFEE', '0.00')).rjust(24),
                    'CTPRINT': str(record.get('CTPRINT', '0')).rjust(7),
                    'CTPRNNAC': str(record.get('CTPRNNAC', '0')).rjust(7)
                }
                processed_data.append(formatted_record)
            
            df = pl.DataFrame(processed_data, schema=self.OUTPUT_COLUMNS)
            logger.info(f"Created DataFrame with {len(df)} records")
            
            # Show summary
            logger.info(f"DataFrame columns: {df.columns}")
            logger.info(f"Sample data:")
            logger.info(f"  COSTCT: {df['COSTCT'].head(3).to_list()}")
            logger.info(f"  NOTETYPE: {df['NOTETYPE'].head(3).to_list()}")
            logger.info(f"  PRINBAL: {df['PRINBAL'].head(3).to_list()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}", exc_info=True)
            return None
    
    def save_output(self, df: pl.DataFrame) -> bool:
        """Save DataFrame to parquet file"""
        try:
            out_dir = self.BASE_OUT / "LNR6999"
            out_dir.mkdir(parents=True, exist_ok=True)
            
            out_name = f"R6999{self.REPTMON}{self.REPTYEAR}.parquet"
            output_path = out_dir / out_name
            
            df.write_parquet(output_path)
            logger.info(f"Output written to: {output_path}")
            
            # Also save text version
            txt_path = output_path.with_suffix('.txt')
            with open(txt_path, 'w') as f:
                # Write header
                header = "COSTCT NOTETYPE PRINBAL                PRNNACCR               INTACCR              INTNACCR              UNERNINT                UNERNNON                RSRVFIN                 RSRVDLR                 NONDDA                  LATEFEES                 OTHERFEE                 CTPRINT CTPRNNAC"
                f.write(header + "\n")
                
                # Write data
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
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving output: {e}", exc_info=True)
            return False
    
    def run_processing(self):
        """Run the complete processing"""
        logger.info("Starting processing...")
        
        # First, analyze the file format
        self.analyze_file_format()
        
        # Try processing with simple approach first
        logger.info("\n=== Attempting to process file ===")
        records = self.process_text_file()
        
        if not records:
            logger.warning("Simple parsing failed, trying alternative approach...")
            # Try with assumed field positions
            # Based on the sample, numbers start at these approximate positions
            field_positions = [12, 30, 48, 66, 84, 102, 120, 138, 156, 174, 192, 210, 228]
            records = self.process_with_field_positions(field_positions)
        
        if not records:
            logger.error("Failed to parse any records")
            return False
        
        # Create DataFrame
        df = self.create_dataframe(records)
        if df is None:
            logger.error("Failed to create DataFrame")
            return False
        
        # Save output
        success = self.save_output(df)
        
        if success:
            logger.info("Processing completed successfully")
            return True
        else:
            logger.error("Failed to save output")
            return False


def main():
    """Main execution function"""
    
    INPUT_TEXT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/R6999_BAL_NOTE_20251218.txt"
    BASE_OUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output"
    
    logger.info("=" * 60)
    logger.info("Starting Loan Data Processing")
    logger.info(f"Input file: {INPUT_TEXT_PATH}")
    logger.info("=" * 60)
    
    processor = LoanDataProcessor(
        input_text_path=INPUT_TEXT_PATH,
        base_output_path=BASE_OUT_PATH,
        batch_date_str=None
    )
    
    success = processor.run_processing()
    
    if success:
        logger.info("Processing completed successfully")
        exit(0)
    else:
        logger.error("Processing failed")
        exit(1)


if __name__ == "__main__":
    main()
