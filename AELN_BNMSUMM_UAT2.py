from pathlib import Path
from datetime import datetime
import polars as pl
import logging
import re
from typing import List, Optional, Dict

# Configure logging with more detail
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoanDataProcessor:
    """Combined processor for both SAS steps: text file processing and report generation"""
    
    # Columns for the output report
    OUTPUT_COLUMNS = [
        "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
        "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
        "OTHERFEE", "CTPRINT", "CTPRNNAC"
    ]
    
    def __init__(self, input_text_path: str, base_output_path: str, 
                 batch_date_str: str = None):
        self.input_text_path = Path(input_text_path)
        self.BASE_OUT = Path(base_output_path)
        
        # Parse batch date
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
        
        self.all_records = []
        
        logger.info(f"Initialized processor for batch date: {self.batch_dt_str}")
    
    def analyze_file_structure(self):
        """Analyze the file structure to understand the format"""
        logger.info("Analyzing file structure...")
        
        try:
            with open(self.input_text_path, 'r') as file:
                lines = file.readlines()
            
            logger.info(f"Total lines: {len(lines)}")
            
            # Look for patterns in first 100 lines
            logger.info("\n=== Analyzing first 100 lines ===")
            for i, line in enumerate(lines[:100]):
                line = line.rstrip('\n')
                if line.strip():  # Only show non-empty lines
                    logger.info(f"Line {i+1:4d}: {repr(line[:150])}")
            
            # Count occurrences of key patterns
            patterns = {
                'R6999': 0,
                'R-6999': 0,
                'COST CENTER': 0,
                'LOAN TYPE': 0,
                'PRINCIPAL': 0,
                'INTEREST': 0,
                'UNEARNED': 0,
                'RESERVE': 0,
                'LATE FEES': 0,
                'EXTENSION': 0,
                'TOTALS': 0,
                'CONTROL TOTALS': 0
            }
            
            for line in lines:
                for pattern in patterns:
                    if pattern in line:
                        patterns[pattern] += 1
            
            logger.info("\n=== Pattern counts ===")
            for pattern, count in patterns.items():
                if count > 0:
                    logger.info(f"{pattern:20s}: {count}")
            
            # Find sample lines with key patterns
            logger.info("\n=== Sample lines with R6999/R-6999 ===")
            r6999_lines = []
            for i, line in enumerate(lines):
                if 'R6999' in line or 'R-6999' in line:
                    r6999_lines.append((i, line.strip()))
                    if len(r6999_lines) >= 5:
                        break
            
            for i, line in r6999_lines:
                logger.info(f"Line {i+1}: {repr(line[:100])}")
            
            # Look for cost center patterns
            logger.info("\n=== Sample lines with COST CENTER ===")
            cost_center_lines = []
            for i, line in enumerate(lines):
                if 'COST CENTER' in line:
                    cost_center_lines.append((i, line.strip()))
                    if len(cost_center_lines) >= 3:
                        break
            
            for i, line in cost_center_lines:
                logger.info(f"Line {i+1}: {repr(line[:100])}")
            
            return lines
            
        except Exception as e:
            logger.error(f"Error analyzing file: {e}", exc_info=True)
            return []
    
    def parse_file_based_on_analysis(self, lines):
        """Parse file after analyzing its structure"""
        logger.info("\n=== Starting file parsing ===")
        
        records = []
        current_record = {}
        in_record = False
        
        item_patterns = [
            ('PRINCIPAL/LOAN BALANCE', 'PRINBAL'),
            ('PRINCIPAL IN NON-ACCRUAL', 'PRNNACCR'),
            ('INTEREST ACCRUALS', 'INTACCR'),
            ('INTEREST IN NON-ACCRUAL', 'INTNACCR'),
            ('UNEARNED INTEREST', 'UNERNINT'),
            ('RESERVE FINANCED', 'RSRVFIN'),
            ('NON DDA ESCROW FUNDS', 'NONDDA'),
            ('LATE FEES', 'LATEFEES'),
            ('OTHER EARNED FEES', 'OTHERFEE')
        ]
        
        for i, line in enumerate(lines):
            line = line.rstrip('\n')
            
            # Skip empty lines
            if not line.strip():
                continue
            
            # Look for start of a new record (header with cost center)
            if 'COST CENTER:' in line and 'LOAN TYPE:' in line:
                if in_record and current_record:
                    records.append(current_record.copy())
                    logger.debug(f"Completed record at line {i}: {current_record}")
                
                # Start new record
                in_record = True
                current_record = {
                    'COSTCT': 0,
                    'NOTETYPE': 0,
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
                    'C_PRN': '0',
                    'C_PRNNAC': '0'
                }
                
                # Extract cost center and loan type
                costct_match = re.search(r'COST CENTER:\s*(\d+)', line)
                notety_match = re.search(r'LOAN TYPE:\s*(\d+)', line)
                
                if costct_match:
                    try:
                        current_record['COSTCT'] = int(costct_match.group(1))
                    except ValueError:
                        current_record['COSTCT'] = 0
                
                if notety_match:
                    try:
                        current_record['NOTETYPE'] = int(notety_match.group(1))
                    except ValueError:
                        current_record['NOTETYPE'] = 0
                
                logger.debug(f"New record started at line {i}: COSTCT={current_record['COSTCT']}, NOTETYPE={current_record['NOTETYPE']}")
                continue
            
            # Process item lines if we're in a record
            if in_record:
                # Check for item patterns
                for item_text, item_code in item_patterns:
                    if item_text in line:
                        logger.debug(f"Found {item_text} at line {i}")
                        
                        # Check for zero indicator
                        if 'ZERO' in line or '* * *' in line:
                            # Set appropriate fields to zero
                            if item_text == 'PRINCIPAL/LOAN BALANCE':
                                current_record['PRINBAL'] = '0.00'
                                current_record['C_PRN'] = '0'
                            elif item_text == 'PRINCIPAL IN NON-ACCRUAL':
                                current_record['PRNNACCR'] = '0.00'
                                current_record['C_PRNNAC'] = '0'
                            elif item_text == 'INTEREST ACCRUALS':
                                current_record['INTACCR'] = '0.0000000'
                            elif item_text == 'INTEREST IN NON-ACCRUAL':
                                current_record['INTNACCR'] = '0.0000000'
                            elif item_text == 'UNEARNED INTEREST':
                                current_record['UNERNINT'] = '0.00'
                                current_record['UNERNNON'] = '0.00'
                            elif item_text == 'RESERVE FINANCED':
                                current_record['RSRVFIN'] = '0.00'
                                current_record['RSRVDLR'] = '0.00'
                            elif item_text == 'NON DDA ESCROW FUNDS':
                                current_record['NONDDA'] = '0.00'
                            elif item_text == 'LATE FEES':
                                current_record['LATEFEES'] = '0.00'
                            elif item_text == 'OTHER EARNED FEES':
                                current_record['OTHERFEE'] = '0.00'
                        break
                
                # Check for balance amounts (lines starting with =)
                if line.strip().startswith('='):
                    logger.debug(f"Found balance line at {i}: {repr(line[:50])}")
                    
                    # Try to extract numbers
                    numbers = re.findall(r'[-\d.]+', line)
                    if numbers:
                        logger.debug(f"Extracted numbers: {numbers}")
                        # For now, if we find a number and the last item was INTEREST IN NON-ACCRUAL, use it
                        if 'INTEREST IN NON-ACCRUAL' in lines[i-1] if i > 0 else False:
                            current_record['INTNACCR'] = numbers[0]
                
                # Check for end of record (EXTENSION FEES)
                if 'EXTENSION FEES' in line and current_record['COSTCT'] != 0:
                    records.append(current_record.copy())
                    logger.debug(f"Completed record at EXTENSION FEES line {i}")
                    in_record = False
        
        # Add last record if we're still in one
        if in_record and current_record:
            records.append(current_record.copy())
        
        return records
    
    def process_text_file(self) -> List[Dict]:
        """Process the input text file"""
        logger.info(f"Processing text file: {self.input_text_path}")
        
        if not self.input_text_path.exists():
            logger.error(f"Input text file not found: {self.input_text_path}")
            return []
        
        try:
            # First analyze the file structure
            lines = self.analyze_file_structure()
            
            if not lines:
                logger.error("Failed to read file or file is empty")
                return []
            
            # Then parse based on the analysis
            self.all_records = self.parse_file_based_on_analysis(lines)
            
            logger.info(f"Processed {len(self.all_records)} records from text file")
            
            if self.all_records:
                logger.info(f"Sample records:")
                for i, record in enumerate(self.all_records[:3]):
                    logger.info(f"Record {i+1}: COSTCT={record['COSTCT']}, NOTETYPE={record['NOTETYPE']}, "
                               f"PRINBAL={record['PRINBAL']}, INTNACCR={record['INTNACCR']}")
            else:
                logger.warning("No records found. Check if file format matches expected patterns.")
            
            return self.all_records
            
        except Exception as e:
            logger.error(f"Error processing text file: {e}", exc_info=True)
            return []
    
    def create_intermediate_dataframe(self) -> Optional[pl.DataFrame]:
        """Create DataFrame from parsed data"""
        if not self.all_records:
            logger.error("No records processed from text file")
            return None
        
        try:
            processed_data = []
            
            for record in self.all_records:
                output_record = {
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
                    'CTPRINT': str(record['C_PRN']).rjust(7),
                    'CTPRNNAC': str(record['C_PRNNAC']).rjust(7)
                }
                processed_data.append(output_record)
            
            result_df = pl.DataFrame(processed_data, schema=self.OUTPUT_COLUMNS)
            logger.info(f"Created DataFrame with {len(result_df)} records")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}", exc_info=True)
            return None
    
    def save_parquet_output(self, df: pl.DataFrame, output_dir_name: str = "LNR6999") -> bool:
        """Save DataFrame to parquet file"""
        try:
            out_dir = self.BASE_OUT / output_dir_name
            out_dir.mkdir(parents=True, exist_ok=True)
            
            out_name = f"R6999{self.REPTMON}{self.REPTYEAR}.parquet"
            output_path = out_dir / out_name
            
            df.write_parquet(output_path)
            logger.info(f"Output written to: {output_path}")
            
            return True
                
        except Exception as e:
            logger.error(f"Error saving output file: {e}", exc_info=True)
            return False
    
    def run_full_processing(self) -> bool:
        """Run the complete processing pipeline"""
        try:
            logger.info("Starting loan data processing...")
            
            # Step 1: Process text file
            logger.info("Step 1: Processing input text file...")
            records = self.process_text_file()
            if not records:
                logger.error("No records processed from text file")
                logger.error("Please check the analysis output above to understand the file format")
                return False
            
            # Step 2: Create DataFrame
            logger.info("Step 2: Creating formatted DataFrame...")
            df = self.create_intermediate_dataframe()
            if df is None:
                logger.error("Failed to create DataFrame")
                return False
            
            # Step 3: Save to parquet
            logger.info("Step 3: Saving to parquet file...")
            success = self.save_parquet_output(df)
            
            if success:
                logger.info("All processing steps completed successfully")
                logger.info(f"Total records processed: {len(df)}")
            else:
                logger.error("Processing failed during output save")
            
            return success
            
        except Exception as e:
            logger.error(f"Processing pipeline failed: {e}", exc_info=True)
            return False


def main():
    """Main execution function"""
    
    # Configuration
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
    
    success = processor.run_full_processing()
    
    if not success:
        logger.error("Processing failed")
        exit(1)
    else:
        logger.info("Processing completed successfully")
        exit(0)


if __name__ == "__main__":
    main()
