import pyreadstat
import polars as pl
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_sas_with_fallback(filepath: Path) -> pl.DataFrame:
    """
    Try multiple methods to read a SAS file
    """
    if not filepath.exists():
        logger.warning(f"File not found: {filepath}")
        return pl.DataFrame()
    
    methods = [
        # Method 1: Standard pyreadstat
        lambda: pyreadstat.read_sas7bdat(filepath),
        
        # Method 2: With encoding
        lambda: pyreadstat.read_sas7bdat(filepath, encoding='latin1'),
        
        # Method 3: With different date handling
        lambda: pyreadstat.read_sas7bdat(filepath, datetime_conversion='datetime'),
        
        # Method 4: Without formats
        lambda: pyreadstat.read_sas7bdat(filepath, formats_as_dataframe=False),
        
        # Method 5: With custom date format
        lambda: pyreadstat.read_sas7bdat(filepath, date_dtype=pd.Timestamp),
        
        # Method 6: Using pandas directly
        lambda: (pd.read_sas(filepath, format='sas7bdat'), None),
    ]
    
    for i, method in enumerate(methods, 1):
        try:
            logger.info(f"Attempting method {i}...")
            result = method()
            
            if isinstance(result, tuple) and len(result) == 2:
                df, meta = result
            else:
                df, meta = result, None
            
            if df is not None and len(df) > 0:
                pl_df = pl.from_pandas(df)
                logger.info(f"✅ Success with method {i}: {len(pl_df):,} rows, {len(pl_df.columns)} columns")
                return pl_df
                
        except Exception as e:
            logger.warning(f"Method {i} failed: {str(e)[:100]}")
            continue
    
    logger.error(f"All methods failed for {filepath}")
    return pl.DataFrame()

# Use the fallback function
def process_trust_data(pidmfin_path: Path) -> pl.DataFrame:
    """Process TRUST data with fallback reading"""
    logger.info("Processing TRUST data from cisdepxn.sas7bdat")
    
    # Try multiple methods to read the file
    cisdepxn_df = read_sas_with_fallback(pidmfin_path / "cisdepxn.sas7bdat")
    
    if cisdepxn_df.is_empty():
        logger.warning("PIDMFIN.cisdepxn is empty or not found")
        return pl.DataFrame()
    
    # Rest of the processing...
    # [Keep the same processing logic as before]
