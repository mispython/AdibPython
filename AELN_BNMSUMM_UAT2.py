from pathlib import Path
from datetime import datetime
import polars as pl
import logging
import re
from typing import List, Optional, Dict, Tuple
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
                 batch_date_str: str = "2025-11-20 00:00:00"):
        """
        Initialize the combined processor
        
        Args:
            input_text_path: Path to the input text file (INSFILE)
            base_output_path: Base directory for output files
            batch_date_str: Batch date in 'YYYY-MM-DD HH:MM:SS' format
        """
        self.input_text_path = Path(input_text_path)
        self.BASE_OUT = Path(base_output_path)
        
        # Parse batch date
        self.batch_dt = datetime.strptime(batch_date_str, '%Y-%m-%d %H:%M:%S')
        self.batch_dt_str = self.batch_dt.strftime("%Y%m%d")
        
        # Report date formatting
        self.REPTDATE = self.batch_dt
        self.REPTMON = f"{self.REPTDATE.month:02d}"
        self.REPTYEAR = f"{self.REPTDATE.year % 100:02d}"
        
        # Ensure output directory exists
        self.BASE_OUT.mkdir(parents=True, exist_ok=True)
        
        # Data storage for intermediate processing
        self.current_costct = 0
        self.current_notety = 0
        self.current_item = ""
        
        # Dictionary to store parsed values for current record
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
        
        # List to store all processed records
        self.all_records = []
        
        logger.info(f"Initialized combined processor for batch date: {self.batch_dt_str}")
    
    def _parse_fixed_width(self, line: str, start: int, length: int) -> str:
        """Parse fixed width field from line"""
        # SAS uses 1-based indexing, Python uses 0-based
        start_idx = start - 1
        end_idx = start_idx + length
        if end_idx > len(line):
            return line[start_idx:].strip()
        return line[start_idx:end_idx].strip()
    
    def _clean_number(self, value_str: str) -> str:
        """Clean and format numeric strings"""
        if not value_str:
            return '0'
        
        # Remove non-numeric characters except decimal point and minus sign
        cleaned = re.sub(r'[^\d.\-]', '', value_str)
        
        # Handle empty result
        if not cleaned or cleaned == '.' or cleaned == '-':
            return '0'
        
        return cleaned
    
    def _format_balance(self, sign: str, balance: str, item_type: str) -> str:
        """Format balance with sign based on item type"""
        if not balance or balance == '0':
            return '0.00'
        
        # Clean the balance
        cleaned = self._clean_number(balance)
        
        # Handle sign logic
        if sign == '-':
            if cleaned.startswith('-'):
                return cleaned
            else:
                return f"-{cleaned}"
        else:
            # Remove any leading minus if sign is positive
            return cleaned.lstrip('-')
    
    def process_text_file(self) -> List[Dict]:
        """
        Process the input text file (first SAS DATA step)
        
        Returns:
            List of dictionaries with parsed data
        """
        logger.info(f"Processing text file: {self.input_text_path}")
        
        if not self.input_text_path.exists():
            logger.error(f"Input text file not found: {self.input_text_path}")
            return []
        
        try:
            with open(self.input_text_path, 'r') as file:
                lines = file.readlines()
            
            logger.info(f"Read {len(lines)} lines from text file")
            
            for line_num, line in enumerate(lines, 1):
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Parse fields at specific positions
                d_item = self._parse_fixed_width(line, 2, 25)
                d_sign = self._parse_fixed_width(line, 2, 1)
                sign2 = self._parse_fixed_width(line, 62, 1)
                sign7 = self._parse_fixed_width(line, 67, 1)
                signr = self._parse_fixed_width(line, 97, 1)
                d_rptno = self._parse_fixed_width(line, 97, 6)
                
                # Check if this is R-6999 report
                if d_rptno == 'R-6999':
                    d_rptset = self._parse_fixed_width(line, 97, 10)
                    
                    if d_rptset == 'R-6999-001':
                        d_header = self._parse_fixed_width(line, 2, 21)
                        
                        if d_header == 'TOTALS  ACCRUAL DATE:':
                            d_costct = self._parse_fixed_width(line, 33, 12)
                            d_notety = self._parse_fixed_width(line, 55, 10)
                            d_desc = self._parse_fixed_width(line, 70, 20)
                            
                            if d_costct == 'COST CENTER:' and d_notety == 'LOAN TYPE:' and d_desc == '':
                                costct_str = self._parse_fixed_width(line, 46, 7)
                                notety_str = self._parse_fixed_width(line, 66, 3)
                                
                                try:
                                    self.current_costct = int(costct_str) if costct_str.strip() else 0
                                    self.current_notety = int(notety_str) if notety_str.strip() else 0
                                except ValueError:
                                    self.current_costct = 0
                                    self.current_notety = 0
                                logger.debug(f"Found cost center: {self.current_costct}, loan type: {self.current_notety}")
                            else:
                                self.current_costct = 0
                                self.current_notety = 0
                        
                        elif d_item in self.ITEM_TYPES:
                            # Check for ZERO indicator
                            d_zero = self._parse_fixed_width(line, 94, 4)
                            self.current_item = d_item
                            
                            if d_zero == 'ZERO':
                                # Set zeros based on item type
                                item_key = self.ITEM_TYPES[d_item]
                                
                                if d_item == 'PRINCIPAL/LOAN BALANCE':
                                    self.current_record['BAL1'] = '0.00'
                                    self.current_record['C_PRN'] = '0'
                                elif d_item == 'PRINCIPAL IN NON-ACCRUAL':
                                    self.current_record['BAL2'] = '0.00'
                                    self.current_record['C_PRNNAC'] = '0'
                                elif d_item == 'INTEREST ACCRUALS':
                                    self.current_record['BAL3'] = '0.0000000'
                                elif d_item == 'INTEREST IN NON-ACCRUAL':
                                    self.current_record['BAL4'] = '0.0000000'
                                elif d_item == 'UNEARNED INTEREST':
                                    self.current_record['BAL5'] = '0.00'
                                    self.current_record['BAL5A'] = '0.00'
                                elif d_item == 'RESERVE FINANCED':
                                    self.current_record['BAL6'] = '0.00'
                                    self.current_record['BAL6A'] = '0.00'
                                elif d_item == 'NON DDA ESCROW FUNDS':
                                    self.current_record['BAL7'] = '0.00'
                                elif d_item == 'LATE FEES':
                                    self.current_record['BAL8'] = '0.00'
                                elif d_item == 'OTHER EARNED FEES':
                                    self.current_record['BAL9'] = '0.00'
                            
                        elif d_sign == '=':
                            # Parse balance values
                            balance2 = self._parse_fixed_width(line, 39, 23)
                            balance7 = self._parse_fixed_width(line, 47, 20)
                            balancer = self._parse_fixed_width(line, 74, 23)
                            count = self._parse_fixed_width(line, 30, 7)
                            
                            # Format balances with signs
                            bal_2 = self._format_balance(sign2, balance2, self.current_item)
                            bal_7 = self._format_balance(sign7, balance7, self.current_item)
                            bal_r = self._format_balance(signr, balancer, self.current_item)
                            
                            # Assign to appropriate fields based on current item
                            if self.current_item == 'PRINCIPAL/LOAN BALANCE':
                                self.current_record['BAL1'] = bal_2
                                self.current_record['C_PRN'] = count if count else '0'
                            elif self.current_item == 'PRINCIPAL IN NON-ACCRUAL':
                                self.current_record['BAL2'] = bal_2
                                self.current_record['C_PRNNAC'] = count if count else '0'
                            elif self.current_item == 'INTEREST ACCRUALS':
                                self.current_record['BAL3'] = bal_7
                            elif self.current_item == 'INTEREST IN NON-ACCRUAL':
                                self.current_record['BAL4'] = bal_7
                            elif self.current_item == 'UNEARNED INTEREST':
                                self.current_record['BAL5'] = bal_2
                                self.current_record['BAL5A'] = bal_r
                            elif self.current_item == 'RESERVE FINANCED':
                                self.current_record['BAL6'] = bal_2
                                self.current_record['BAL6A'] = bal_r
                            elif self.current_item == 'NON DDA ESCROW FUNDS':
                                self.current_record['BAL7'] = bal_2
                            elif self.current_item == 'LATE FEES':
                                self.current_record['BAL8'] = bal_2
                            elif self.current_item == 'OTHER EARNED FEES':
                                self.current_record['BAL9'] = bal_2
                        
                        elif d_item == 'EXTENSION FEES' and self.current_costct != 0:
                            # Output current record
                            self.current_record['COSTCT'] = self.current_costct
                            self.current_record['NOTETY'] = self.current_notety
                            
                            # Make a copy to avoid reference issues
                            record_copy = self.current_record.copy()
                            self.all_records.append(record_copy)
                            
                            # Reset current record for next
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
            
            logger.info(f"Processed {len(self.all_records)} records from text file")
            return self.all_records
            
        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            return []
    
    def create_intermediate_dataframe(self) -> Optional[pl.DataFrame]:
        """
        Create intermediate DataFrame from parsed text data (mimics SAS DATA _NULL_ step)
        
        Returns:
            Polars DataFrame with formatted data
        """
        if not self.all_records:
            logger.error("No records processed from text file")
            return None
        
        try:
            # Convert to DataFrame for easier manipulation
            df = pl.DataFrame(self.all_records)
            
            # Right-align string values (mimics SAS RIGHT() function)
            # For balance fields, we'll convert to numeric first
            processed_data = []
            
            for record in self.all_records:
                # Create output record with proper formatting
                output_record = {
                    'COSTCT': record['COSTCT'],
                    'NOTETYPE': record['NOTETY'],
                    'PRINBAL': record['BAL1'].rjust(24),
                    'PRNNACCR': record['BAL2'].rjust(24),
                    'INTACCR': record['BAL3'].rjust(21),
                    'INTNACCR': record['BAL4'].rjust(21),
                    'UNERNINT': record['BAL5'].rjust(24),
                    'UNERNNON': record['BAL5A'].rjust(24),
                    'RSRVFIN': record['BAL6'].rjust(24),
                    'RSRVDLR': record['BAL6A'].rjust(24),
                    'NONDDA': record['BAL7'].rjust(24),
                    'LATEFEES': record['BAL8'].rjust(24),
                    'OTHERFEE': record['BAL9'].rjust(24),
                    'CTPRINT': record['C_PRN'].rjust(7),
                    'CTPRNNAC': record['C_PRNNAC'].rjust(7)
                }
                processed_data.append(output_record)
            
            # Create final DataFrame
            result_df = pl.DataFrame(processed_data, schema=self.OUTPUT_COLUMNS)
            
            logger.info(f"Created intermediate DataFrame with {len(result_df)} records")
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating intermediate DataFrame: {e}")
            return None
    
    def save_parquet_output(self, df: pl.DataFrame, output_dir_name: str = "LNR6999") -> bool:
        """
        Save the processed DataFrame to parquet file (second step)
        
        Args:
            df: DataFrame to save
            output_dir_name: Output directory name
            
        Returns:
            True if successful, False otherwise
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
            
            # Verify the file was created
            if output_path.exists():
                file_size = output_path.stat().st_size / (1024 * 1024)  # Size in MB
                logger.info(f"Output file created successfully: {file_size:.2f} MB")
                
                # Also save a text version for comparison with SAS output
                self._save_text_version(df, output_path)
                return True
            else:
                logger.error("Output file was not created")
                return False
                
        except Exception as e:
            logger.error(f"Error saving output file: {e}")
            return False
    
    def _save_text_version(self, df: pl.DataFrame, parquet_path: Path):
        """Save a text version of the data for debugging/verification"""
        try:
            txt_path = parquet_path.with_suffix('.txt')
            
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
                
                # Write each record
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
            
        except Exception as e:
            logger.warning(f"Could not save text version: {e}")
    
    def run_full_processing(self) -> bool:
        """
        Run the complete two-step processing pipeline
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting combined loan data processing...")
            
            # Step 1: Process text file (SAS DATA A step)
            logger.info("Step 1: Processing input text file...")
            records = self.process_text_file()
            if not records:
                logger.error("No records processed from text file")
                return False
            
            # Step 2: Create DataFrame (SAS DATA _NULL_ step)
            logger.info("Step 2: Creating formatted DataFrame...")
            df = self.create_intermediate_dataframe()
            if df is None:
                logger.error("Failed to create DataFrame")
                return False
            
            # Step 3: Save to parquet (EIBMLNRP conversion step)
            logger.info("Step 3: Saving to parquet file...")
            success = self.save_parquet_output(df)
            
            if success:
                logger.info("All processing steps completed successfully")
                
                # Log summary statistics
                self._log_summary_statistics(df)
            else:
                logger.error("Processing failed during output save")
            
            return success
            
        except Exception as e:
            logger.error(f"Processing pipeline failed: {e}")
            return False
    
    def _log_summary_statistics(self, df: pl.DataFrame):
        """Log summary statistics about the processed data"""
        try:
            logger.info("=== Processing Summary ===")
            logger.info(f"Total records: {len(df):,}")
            
            # Check for non-zero values in key columns
            numeric_cols = ['PRINBAL', 'PRNNACCR', 'INTACCR', 'INTNACCR']
            
            for col in numeric_cols:
                if col in df.columns:
                    # Count non-zero values (after cleaning)
                    non_zero_count = df.filter(
                        pl.col(col).str.strip_chars().str.replace(",", "").str.replace("$", "")
                        .cast(pl.Float64, strict=False) != 0
                    ).height
                    logger.info(f"{col}: {non_zero_count:,} non-zero records")
            
            # Unique cost centers and loan types
            if 'COSTCT' in df.columns:
                unique_costct = df['COSTCT'].n_unique()
                logger.info(f"Unique cost centers: {unique_costct}")
            
            if 'NOTETYPE' in df.columns:
                unique_notety = df['NOTETYPE'].n_unique()
                logger.info(f"Unique loan types: {unique_notety}")
                
        except Exception as e:
            logger.warning(f"Could not generate summary statistics: {e}")


def main():
    """Main execution function"""
    
    # Configuration
    INPUT_TEXT_PATH = "/path/to/your/INSFILE.txt"  # Update this path
    BASE_OUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output"
    BATCH_DATE = "2025-11-20 00:00:00"
    
    # Initialize and run processor
    processor = LoanDataProcessor(
        input_text_path=INPUT_TEXT_PATH,
        base_output_path=BASE_OUT_PATH,
        batch_date_str=BATCH_DATE
    )
    
    # Run full processing
    success = processor.run_full_processing()
    
    # Exit with appropriate code
    if not success:
        logger.error("Processing failed - check logs for details")
        exit(1)
    else:
        logger.info("Combined processing completed successfully")
        exit(0)


if __name__ == "__main__":
    main()
