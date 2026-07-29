import sys
import logging
import pandas as pd
import traceback
from pathlib import Path
from datetime import datetime, timedelta

# =====================================================
# CONFIGURATION
# =====================================================

JOB_NAME = "EIBWHP04"

BASE_DIR = Path(".")
INPUT_DIR = BASE_DIR / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04"
OUTPUT_DIR = BASE_DIR / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04"

# Calculate dates for file naming
PREV_DATE = datetime.now() - timedelta(days=1)
REPTMON = PREV_DATE.strftime("%Y%m")  # YYYYMM format
NOWK = PREV_DATE.strftime("%W")       # Week number

# Calculate previous month and its week
if PREV_DATE.month == 1:
    PREV_MONTH_DATE = PREV_DATE.replace(year=PREV_DATE.year - 1, month=12)
else:
    PREV_MONTH_DATE = PREV_DATE.replace(month=PREV_DATE.month - 1)

REPTMON1 = PREV_MONTH_DATE.strftime("%Y%m")
NOWK1 = PREV_MONTH_DATE.strftime("%W")

# Input files with dynamic naming
INPUT_DATASETS = {
    "LOAN_CURRENT": INPUT_DIR / f"loan{REPTMON}{NOWK}.sas7bdat",
    "LOAN_PREVIOUS": INPUT_DIR / f"loan{REPTMON1}{NOWK1}.sas7bdat",
    "ULOAN": INPUT_DIR / f"uloan{REPTMON}{NOWK}.sas7bdat"
}

OUTPUT_DATASET = OUTPUT_DIR / f"EIBWHP04_{PREV_DATE.strftime('%Y%m%d')}.txt"
LOG_FILE = OUTPUT_DIR / f"{JOB_NAME}_{PREV_DATE.strftime('%Y%m%d')}.log"

# =====================================================
# LOGGING SETUP
# =====================================================

# Ensure output directory exists first
try:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"DEBUG: Created/verified output directory: {OUTPUT_DIR}")
except Exception as e:
    print(f"ERROR: Failed to create output directory: {e}")
    sys.exit(8)

# Setup logging with both file and console output for debugging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for maximum detail
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)

logger = logging.getLogger(__name__)

# =====================================================
# DEBUG INFORMATION
# =====================================================

def print_debug_info():
    """Print debug information about the environment and configuration"""
    logger.debug("=" * 60)
    logger.debug("DEBUG INFORMATION:")
    logger.debug(f"Python version: {sys.version}")
    logger.debug(f"Current working directory: {Path.cwd()}")
    logger.debug(f"Script location: {Path(__file__).absolute()}")
    logger.debug(f"BASE_DIR: {BASE_DIR.absolute()}")
    logger.debug(f"INPUT_DIR: {INPUT_DIR.absolute()}")
    logger.debug(f"OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.debug(f"PREV_DATE: {PREV_DATE}")
    logger.debug(f"REPTMON: {REPTMON}, NOWK: {NOWK}")
    logger.debug(f"REPTMON1: {REPTMON1}, NOWK1: {NOWK1}")
    logger.debug(f"LOG_FILE: {LOG_FILE}")
    logger.debug(f"OUTPUT_DATASET: {OUTPUT_DATASET}")
    logger.debug("Input datasets:")
    for name, path in INPUT_DATASETS.items():
        logger.debug(f"  {name}: {path}")
        logger.debug(f"    Exists: {path.exists()}")
        logger.debug(f"    Size: {path.stat().st_size if path.exists() else 'N/A'} bytes")
    logger.debug("=" * 60)

# =====================================================
# DISP SIMULATION WITH DEBUG
# =====================================================

def disp_delete(path: Path):
    """Delete dataset if exists"""
    logger.debug(f"DISP DELETE - Attempting to delete: {path}")
    if path.exists():
        try:
            logger.debug(f"File exists, size: {path.stat().st_size} bytes")
            path.unlink()
            logger.info(f"Deleted dataset: {path}")
        except PermissionError as e:
            logger.error(f"Permission denied deleting {path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error deleting {path}: {e}")
            raise
    else:
        logger.debug(f"File does not exist, nothing to delete: {path}")

def disp_shr(path: Path):
    """Validate SHR dataset exists"""
    logger.debug(f"DISP SHR - Validating: {path}")
    logger.debug(f"  Absolute path: {path.absolute()}")
    logger.debug(f"  Parent directory exists: {path.parent.exists()}")
    
    if not path.parent.exists():
        error_msg = f"Parent directory does not exist: {path.parent}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    if not path.exists():
        # List files in directory for debugging
        if path.parent.exists():
            files_in_dir = list(path.parent.glob("*"))
            logger.debug(f"  Files in directory: {[f.name for f in files_in_dir]}")
        
        error_msg = f"DISP=SHR failed - File not found: {path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    logger.debug(f"  File exists, size: {path.stat().st_size} bytes")
    logger.info(f"Validated SHR dataset: {path}")

def disp_new(path: Path):
    """Validate NEW dataset doesn't exist"""
    logger.debug(f"DISP NEW - Validating: {path}")
    if path.exists():
        error_msg = f"DISP=NEW failed - File already exists: {path} (size: {path.stat().st_size} bytes)"
        logger.error(error_msg)
        raise FileExistsError(error_msg)
    
    # Check if parent directory is writable
    if not path.parent.exists():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created parent directory: {path.parent}")
        except Exception as e:
            error_msg = f"Cannot create parent directory for output: {e}"
            logger.error(error_msg)
            raise
    
    logger.info(f"Validated NEW dataset: {path}")

# =====================================================
# SAS7BDAT READER WITH DEBUG
# =====================================================

def read_sas7bdat(path: Path):
    """Read SAS7BDAT file and return pandas DataFrame"""
    logger.debug(f"READ SAS7BDAT - Starting to read: {path}")
    logger.debug(f"  File size: {path.stat().st_size} bytes")
    
    try:
        logger.debug("  Attempting pd.read_sas()...")
        df = pd.read_sas(path, format='sas7bdat', encoding='utf-8')
        
        logger.debug(f"  Successfully read DataFrame:")
        logger.debug(f"    Shape: {df.shape}")
        logger.debug(f"    Columns: {df.columns.tolist()}")
        logger.debug(f"    Dtypes:\n{df.dtypes}")
        logger.debug(f"    First 5 rows:\n{df.head()}")
        logger.debug(f"    Null counts:\n{df.isnull().sum()}")
        
        logger.info(f"Read {len(df)} rows from {path}")
        return df
        
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty SAS file: {path} - {e}")
        raise
    except UnicodeDecodeError as e:
        logger.error(f"Encoding error reading {path}: {e}")
        logger.debug("Attempting with different encoding...")
        try:
            df = pd.read_sas(path, format='sas7bdat', encoding='latin1')
            logger.info(f"Successfully read with latin1 encoding")
            return df
        except Exception as e2:
            logger.error(f"Failed with alternative encoding: {e2}")
            raise
    except Exception as e:
        logger.error(f"Error reading SAS file {path}: {e}")
        logger.debug(f"Full traceback:\n{traceback.format_exc()}")
        raise

# =====================================================
# TEXT FILE WRITER WITH DEBUG
# =====================================================

def write_text_file(path: Path, records):
    """Write records to text file"""
    logger.debug(f"WRITE TEXT FILE - Starting to write: {path}")
    logger.debug(f"  Number of records to write: {len(records)}")
    
    if not records:
        logger.warning("  No records to write!")
    
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            for i, record in enumerate(records):
                try:
                    if isinstance(record, str):
                        f.write(record + "\n")
                    else:
                        f.write(str(record) + "\n")
                    
                    # Debug first 5 records
                    if i < 5:
                        logger.debug(f"  Record {i}: {record}")
                        
                except Exception as e:
                    logger.error(f"  Error writing record {i}: {e}")
                    logger.debug(f"  Problematic record: {record}")
                    continue
        
        logger.debug(f"  File written successfully, size: {path.stat().st_size} bytes")
        logger.info(f"Text file created: {path}")
        
    except PermissionError as e:
        logger.error(f"Permission denied writing to {path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error writing text file {path}: {e}")
        logger.debug(f"Full traceback:\n{traceback.format_exc()}")
        raise

# =====================================================
# BUSINESS LOGIC WITH DEBUG
# =====================================================

def execute_business_logic():
    """
    Execute EIBWHP04 business logic using PBBLNFMT formatting.
    """
    logger.debug("EXECUTE BUSINESS LOGIC - Starting")
    
    # Try to import PBBLNFMT
    try:
        sys.path.append(str(Path(__file__).parent))
        logger.debug(f"Added to sys.path: {Path(__file__).parent}")
        
        import PBBLNFMT
        logger.debug(f"PBBLNFMT module imported from: {PBBLNFMT.__file__}")
        logger.debug(f"PBBLNFMT attributes: {dir(PBBLNFMT)}")
        
        # Check if format_loan_record exists
        if not hasattr(PBBLNFMT, 'format_loan_record'):
            logger.error("PBBLNFMT module does not have 'format_loan_record' function")
            raise AttributeError("format_loan_record function not found in PBBLNFMT")
        
        format_loan_record = PBBLNFMT.format_loan_record
        logger.debug("format_loan_record function found")
        
    except ImportError as e:
        logger.error(f"Failed to import PBBLNFMT: {e}")
        logger.debug(f"sys.path: {sys.path}")
        logger.debug(f"Current directory contents: {list(Path.cwd().glob('*'))}")
        raise
    except Exception as e:
        logger.error(f"Error with PBBLNFMT module: {e}")
        logger.debug(f"Full traceback:\n{traceback.format_exc()}")
        raise
    
    # Read input datasets
    dataframes = {}
    for name, path in INPUT_DATASETS.items():
        logger.debug(f"Reading {name} from {path}")
        try:
            dataframes[name] = read_sas7bdat(path)
            logger.debug(f"  {name} shape: {dataframes[name].shape}")
        except Exception as e:
            logger.error(f"Failed to read {name}: {e}")
            raise
    
    # Process records
    records = []
    logger.debug("Starting record processing...")
    
    loan_current_df = dataframes["LOAN_CURRENT"]
    loan_previous_df = dataframes["LOAN_PREVIOUS"]
    uloan_df = dataframes["ULOAN"]
    
    for idx, row in loan_current_df.iterrows():
        try:
            logger.debug(f"Processing row {idx}: {row.to_dict()}")
            
            # Apply PBBLNFMT formatting
            formatted_record = format_loan_record(
                row, 
                loan_current_df=loan_current_df,
                loan_previous_df=loan_previous_df,
                uloan_df=uloan_df
            )
            
            if formatted_record:
                records.append(formatted_record)
                logger.debug(f"  Formatted record: {formatted_record}")
            else:
                logger.warning(f"  Empty formatted record for row {idx}")
                
        except Exception as e:
            logger.warning(f"Skipping record {idx} due to error: {e}")
            logger.debug(f"  Row data: {row.to_dict()}")
            logger.debug(f"  Full traceback:\n{traceback.format_exc()}")
            continue
    
    logger.debug(f"Total records processed: {len(records)}")
    logger.info(f"Processed {len(records)} records")
    return records

# =====================================================
# JOB EXECUTION WITH DEBUG
# =====================================================

def run_job():
    """Main job execution with comprehensive error handling"""
    logger.info(f"========== START JOB {JOB_NAME} ==========")
    
    try:
        # Print debug information
        print_debug_info()
        
        logger.info(f"Processing date: {PREV_DATE.strftime('%Y-%m-%d')}")
        logger.info(f"Current month: {REPTMON}, Week: {NOWK}")
        logger.info(f"Previous month: {REPTMON1}, Week: {NOWK1}")
        
        # DELETE STEP
        logger.debug("Starting DELETE STEP")
        disp_delete(OUTPUT_DATASET)
        logger.debug("DELETE STEP completed")
        
        # SHR VALIDATION
        logger.debug("Starting SHR VALIDATION")
        for name, path in INPUT_DATASETS.items():
            logger.debug(f"Validating {name}...")
            disp_shr(path)
            logger.debug(f"{name} validation passed")
        logger.debug("SHR VALIDATION completed")
        
        # NEW VALIDATION
        logger.debug("Starting NEW VALIDATION")
        disp_new(OUTPUT_DATASET)
        logger.debug("NEW VALIDATION completed")
        
        # EXECUTE LOGIC
        logger.debug("Starting BUSINESS LOGIC execution")
        records = execute_business_logic()
        logger.debug(f"Business logic returned {len(records)} records")
        
        # WRITE TEXT FILE
        logger.debug("Starting WRITE STEP")
        write_text_file(OUTPUT_DATASET, records)
        logger.debug("WRITE STEP completed")
        
        logger.info(f"========== END JOB {JOB_NAME} SUCCESSFULLY ==========")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"FILE NOT FOUND ERROR: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8
        
    except FileExistsError as e:
        logger.error(f"FILE EXISTS ERROR: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8
        
    except pd.errors.EmptyDataError as e:
        logger.error(f"EMPTY DATA ERROR: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8
        
    except ImportError as e:
        logger.error(f"IMPORT ERROR: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8
        
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR: {type(e).__name__}: {e}")
        logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        return 8

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info(f"Starting {JOB_NAME} script execution")
        logger.info("=" * 60)
        
        exit_code = run_job()
        
        logger.info(f"Script completed with exit code: {exit_code}")
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        sys.exit(8)
        
    except Exception as e:
        # Catch any unexpected errors in the main block
        print(f"CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        
        # Try to log if logger is available
        try:
            logger.critical(f"CRITICAL ERROR in main: {e}")
            logger.critical(f"Stack trace:\n{traceback.format_exc()}")
        except:
            pass
            
        sys.exit(8)
