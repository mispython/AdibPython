from pathlib import Path
from datetime import datetime
import polars as pl
import logging
import re
from typing import List, Optional, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoanDataProcessor:
    """Combined processor for both SAS steps: text file processing and report generation"""
    
    # Constants for the first step (text parsing)
    ITEM_TYPES = {
        'PRINCIPAL/LOAN BALANCE': 'PRINBAL',
        'PRINCIPAL IN NON-ACCRUAL': 'PRNNACCR',
        'INTEREST ACCRUALS': 'INTACCR',
        'INTEREST IN NON-ACCRUAL': 'INTNACCR',
        'UNEARNED INTEREST': 'UNERNINT',
        'RESERVE FINANCED': 'RSRVFIN',
        'NON DDA ESCROW FUNDS': 'NONDDA',
        'LATE FEES': 'LATEFEES',
        'OTHER EARNED FEES': 'OTHERFEE'
    }
    
    # Columns for the output report (second step)
    OUTPUT_COLUMNS = [
        "COSTCT", "NOTETYPE", "PRINBAL", "PRNNACCR", "INTACCR", "INTNACCR",
        "UNERNINT", "UNERNNON", "RSRVFIN", "RSRVDLR", "NONDDA", "LATEFEES",
        "OTHERFEE", "CTPRINT", "CTPRNNAC"
    ]
    
    def __init__(self, input_text_path: str, base_output_path: str, 
                 batch_date_str: str = None):
        """
        Initialize the combined processor
        """
        self.input_text_path = Path(input_text_path)
        self.BASE_OUT = Path(base_output_path)
        
        # Parse batch date from filename if not provided
        if batch_date_str is None:
            filename = self.input_text_path.name
            date_match = re.search(r'(\d{8})', filename)
            if date_match:
                date_str = date_match.group(1)
                batch_date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} 00:00:00"
                logger.info(f"Extracted batch date from filename: {batch_date_str}")
            else:
                batch_date_str = datetime.now().strftime('%Y-%m-%d 00:00:00')
        
        # Parse batch date
        try:
            self.batch_dt = datetime.strptime(batch_date_str, '%Y-%m-%d %H:%M:%S')
            self.batch_dt_str = self.batch_dt.strftime("%Y%m%d")
        except ValueError:
            self.batch_dt = datetime.now()
            self.batch_dt_str = self.batch_dt.strftime("%Y%m%d")
        
        # Report date formatting
        self.REPTDATE = self.batch_dt
        self.REPTMON = f"{self.REPTDATE.month:02d}"
        self.REPTYEAR = f"{self.REPTDATE.year % 100:02d}"
        
        # Ensure output directory exists
        self.BASE_OUT.mkdir(parents=True, exist_ok=True)
        
        # Initialize data structures
        self.current_costct = 0
        self.current_notety = 0
        self.current_item = ""
        
        self.current_record = {
            'COSTCT': 0,
            'NOTETY': 0,
            'BAL1': '0.00',
            'BAL2': '0.00',
            'BAL3': '0.0000000',
            'BAL4': '0.0000000',
            'BAL5': '0.00',
            'BAL5A': '0.00',
            'BAL6': '0.00',
            'BAL6A': '0.00',
            'BAL7': '0.00',
            'BAL8': '0.00',
            'BAL9': '0.00',
            'C_PRN': '0',
            'C_PRNNAC': '0'
        }
        
        self.all_records = []
        
        logger.info(f"Initialized processor for batch date: {self.batch_dt_str}")
    
    def _extract_costct_notety(self, line: str) -> tuple:
        """Extract cost center and loan type from header line"""
        costct = 0
        notety = 0
        
        # Look for COST CENTER: pattern
        costct_match = re.search(r'COST CENTER:\s*(\d+)', line)
        if costct_match:
            try:
                costct = int(costct_match.group(1))
            except ValueError:
                costct = 0
        
        # Look for LOAN TYPE: pattern
        notety_match = re.search(r'LOAN TYPE:\s*(\d+)', line)
        if notety_match:
            try:
                notety = int(notety_match.group(1))
            except ValueError:
                notety = 0
        
        return costct, notety
    
    def _extract_balance_from_line(self, line: str) -> str:
        """Extract balance amount from a line with = sign"""
        # Look for the number after "="
        if '=' in line:
            # Find the position of "="
            eq_pos = line.find('=')
            
            # Look for numbers after the "="
            # The balance appears to be in positions like: "0.0049482-"
            remaining = line[eq_pos:]
            
            # Find all numbers with possible minus sign
            matches = re.findall(r'[\d.]+-?', remaining)
            if matches:
                # Usually the first number after "=" is the balance
                return matches[0].strip()
        
        return '0.0000000'
    
    def process_text_file(self) -> List[Dict]:
        """
        Process the input text file
        """
        logger.info(f"Processing text file: {self.input_text_path}")
        
        if not self.input_text_path.exists():
            logger.error(f"Input text file not found: {self.input_text_path}")
            return []
        
        try:
            with open(self.input_text_path, 'r') as file:
                lines = file.readlines()
            
            logger.info(f"Read {len(lines)} lines from text file")
            
            # Track state
            in_r6999_section = False
            found_header = False
            
            for line_num, line in enumerate(lines, 1):
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Check if this is an R-6999 report section
                if 'R-6999' in line and 'CONTROL TOTALS' in line:
                    in_r6999_section = True
                    found_header = False
                    logger.debug(f"Line {line_num}: Entered R-6999 section")
                    continue
                
                # Only process lines within R-6999 sections
                if not in_r6999_section:
                    continue
                
                # Look for header line with TOTALS ACCRUAL DATE
                if 'TOTALS  ACCRUAL DATE:' in line:
                    found_header = True
                    # Extract cost center and loan type
                    self.current_costct, self.current_notety = self._extract_costct_notety(line)
                    logger.debug(f"Line {line_num}: Found header - COSTCT: {self.current_costct}, NOTETY: {self.current_notety}")
                    
                    # Reset current record
                    self.current_record = {
                        'COSTCT': self.current_costct,
                        'NOTETY': self.current_notety,
                        'BAL1': '0.00',
                        'BAL2': '0.00',
                        'BAL3': '0.0000000',
                        'BAL4': '0.0000000',
                        'BAL5': '0.00',
                        'BAL5A': '0.00',
                        'BAL6': '0.00',
                        'BAL6A': '0.00',
                        'BAL7': '0.00',
                        'BAL8': '0.00',
                        'BAL9': '0.00',
                        'C_PRN': '0',
                        'C_PRNNAC': '0'
                    }
                    continue
                
                # Only process item lines after header is found
                if not found_header:
                    continue
                
                # Check for item lines
                item_found = False
                for item_key in self.ITEM_TYPES:
                    if item_key in line:
                        self.current_item = item_key
                        item_found = True
                        logger.debug(f"Line {line_num}: Found item: {item_key}")
                        
                        # Check for ZERO indicator
                        if 'ARE  EQUAL  TO  ZERO' in line or '*  *  *' in line:
                            # Set zeros based on item type
                            if item_key == 'PRINCIPAL/LOAN BALANCE':
                                self.current_record['BAL1'] = '0.00'
                                self.current_record['C_PRN'] = '0'
                            elif item_key == 'PRINCIPAL IN NON-ACCRUAL':
                                self.current_record['BAL2'] = '0.00'
                                self.current_record['C_PRNNAC'] = '0'
                            elif item_key == 'INTEREST ACCRUALS':
                                self.current_record['BAL3'] = '0.0000000'
                            elif item_key == 'INTEREST IN NON-ACCRUAL':
                                # Special case: might have a balance line following
                                self.current_record['BAL4'] = '0.0000000'
                            elif item_key == 'UNEARNED INTEREST':
                                self.current_record['BAL5'] = '0.00'
                                self.current_record['BAL5A'] = '0.00'
                            elif item_key == 'RESERVE FINANCED':
                                self.current_record['BAL6'] = '0.00'
                                self.current_record['BAL6A'] = '0.00'
                            elif item_key == 'NON DDA ESCROW FUNDS':
                                self.current_record['BAL7'] = '0.00'
                            elif item_key == 'LATE FEES':
                                self.current_record['BAL8'] = '0.00'
                            elif item_key == 'OTHER EARNED FEES':
                                self.current_record['BAL9'] = '0.00'
                        break
                
                # Check for balance line (starts with "=") for INTEREST IN NON-ACCRUAL
                if line.strip().startswith('=') and self.current_item == 'INTEREST IN NON-ACCRUAL':
                    balance = self._extract_balance_from_line(line)
                    self.current_record['BAL4'] = balance
                    logger.debug(f"Line {line_num}: Extracted balance for INTNACCR: {balance}")
                
                # Check for EXTENSION FEES to trigger output
                if 'EXTENSION FEES' in line and self.current_costct != 0:
                    # Output current record
                    record_copy = self.current_record.copy()
                    self.all_records.append(record_copy)
                    logger.debug(f"Line {line_num}: Output record - COSTCT: {self.current_costct}, NOTETY: {self.current_notety}")
                    
                    # Reset for next record
                    in_r6999_section = False
                    found_header = False
            
            logger.info(f"Processed {len(self.all_records)} records from text file")
            
            # Log sample records
            if self.all_records:
                logger.info(f"Sample first record: COSTCT={self.all_records[0]['COSTCT']}, NOTETY={self.all_records[0]['NOTETY']}")
                logger.info(f"BAL4 (INTNACCR) in first record: {self.all_records[0]['BAL4']}")
            
            return self.all_records
            
        except Exception as e:
            logger.error(f"Error processing text file: {e}", exc_info=True)
            return []
    
    def create_intermediate_dataframe(self) -> Optional[pl.DataFrame]:
        """
        Create intermediate DataFrame from parsed text data
        """
        if not self.all_records:
            logger.error("No records processed from text file")
            return None
        
        try:
            # Format the data with proper right alignment
            processed_data = []
            
            for i, record in enumerate(self.all_records):
                # Format numbers consistently
                output_record = {
                    'COSTCT': record['COSTCT'],
                    'NOTETYPE': record['NOTETY'],
                    
                    # Format balances with proper alignment
                    'PRINBAL': str(record['BAL1']).rjust(24),
                    'PRNNACCR': str(record['BAL2']).rjust(24),
                    'INTACCR': str(record['BAL3']).rjust(21),
                    'INTNACCR': str(record['BAL4']).rjust(21),
                    'UNERNINT': str(record['BAL5']).rjust(24),
                    'UNERNNON': str(record['BAL5A']).rjust(24),
                    'RSRVFIN': str(record['BAL6']).rjust(24),
                    'RSRVDLR': str(record['BAL6A']).rjust(24),
                    'NONDDA': str(record['BAL7']).rjust(24),
                    'LATEFEES': str(record['BAL8']).rjust(24),
                    'OTHERFEE': str(record['BAL9']).rjust(24),
                    
                    # Counts
                    'CTPRINT': str(record['C_PRN']).rjust(7),
                    'CTPRNNAC': str(record['C_PRNNAC']).rjust(7)
                }
                processed_data.append(output_record)
            
            # Create DataFrame
            result_df = pl.DataFrame(processed_data, schema=self.OUTPUT_COLUMNS)
            
            logger.info(f"Created DataFrame with {len(result_df)} records")
            
            # Show summary
            logger.info(f"Unique COSTCT values: {result_df['COSTCT'].n_unique()}")
            logger.info(f"Unique NOTETYPE values: {result_df['NOTETYPE'].n_unique()}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}", exc_info=True)
            return None
    
    def save_parquet_output(self, df: pl.DataFrame, output_dir_name: str = "LNR6999") -> bool:
        """
        Save the processed DataFrame to parquet file
        """
        try:
            # Create output directory
            out_dir = self.BASE_OUT / output_dir_name
            out_dir.mkdir(parents=True, exist_ok=True)
            
            # Construct output filename
            out_name = f"R6999{self.REPTMON}{self.REPTYEAR}.parquet"
            output_path = out_dir / out_name
            
            # Write to parquet
            df.write_parquet(output_path)
            
            logger.info(f"Output successfully written to: {output_path}")
            
            # Also save text version for verification
            txt_path = output_path.with_suffix('.txt')
            with open(txt_path, 'w') as f:
                # Write header
                header = (
                    "COSTCT NOTETY PRINBAL                PRNNACCR               "
                    "INTACCR              INTNACCR              UNERNINT                "
                    "UNERNNON                RSRVFIN                 RSRVDLR                 "
                    "NONDDA                  LATEFEES                 OTHERFEE                 "
                    "CTPRINT CTPRNNAC"
                )
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
            logger.error(f"Error saving output file: {e}", exc_info=True)
            return False
    
    def run_full_processing(self) -> bool:
        """
        Run the complete processing pipeline
        """
        try:
            logger.info("Starting loan data processing...")
            
            # Step 1: Process text file
            logger.info("Step 1: Processing input text file...")
            records = self.process_text_file()
            if not records:
                logger.error("No records processed from text file")
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
                # Show final summary
                logger.info(f"Total records processed: {len(df)}")
                logger.info(f"Output file: R6999{self.REPTMON}{self.REPTYEAR}.parquet")
            else:
                logger.error("Processing failed during output save")
            
            return success
            
        except Exception as e:
            logger.error(f"Processing pipeline failed: {e}", exc_info=True)
            return False


def main():
    """Main execution function"""
    
    # Configuration
    INPUT_TEXT_PATH = "/sasdata/rawdata/ln/R6999_BAL_ITEM_20251204.txt"
    BASE_OUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output"
    
    logger.info("=" * 60)
    logger.info("Starting Loan Data Processing")
    logger.info(f"Input file: {INPUT_TEXT_PATH}")
    logger.info("=" * 60)
    
    # Initialize and run processor
    processor = LoanDataProcessor(
        input_text_path=INPUT_TEXT_PATH,
        base_output_path=BASE_OUT_PATH,
        batch_date_str=None  # Extract from filename
    )
    
    # Run full processing
    success = processor.run_full_processing()
    
    if not success:
        logger.error("Processing failed")
        exit(1)
    else:
        logger.info("Processing completed successfully")
        exit(0)


if __name__ == "__main__":
    main()
