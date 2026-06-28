#!/usr/bin/env python3
"""
Program : EIBQFAR2_CONV_INSURANCE
Function: Convert deposit and trust data for insurance reporting
Purpose : Generate base dataset for EIBQFAR2 report with format mappings
"""

import os
import sys
import logging
import datetime
from typing import Dict, List, Optional, Any

import polars as pl
import pandas as pd

# Import format definitions from PBBDPFMT
from PBBDPFMT import (
    SADenomFormat,
    SAProductFormat,
    FDDenomFormat,
    FDProductFormat,
    CADenomFormat,
    CAProductFormat,
    FCYTermFormat,
    ProductLists,
    fdorgmt_format,
    get_format,
    apply_format
)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# Report Parameters
REPORT_DATE = datetime.datetime(2025, 12, 31)
REPTMON = REPORT_DATE.month
REPTYEAR = REPORT_DATE.year % 100
SDESC = "PUBLIC BANK BERHAD"

# Input/Output Paths
BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS"
INPUT_PATH = f"{BASE_PATH}/input/prod/EIBQFAR2"
OUTPUT_PATH = f"{BASE_PATH}/output/EIBQFAR2"

# Input Files
DEPOSIT_FILE = f"{INPUT_PATH}/cisdepd.sas7bdat"
TRUST_FILE = f"{INPUT_PATH}/cisdepxn.sas7bdat"

# Output Files
OUTPUT_FILE = f"{OUTPUT_PATH}/EIBQFAR2_BASE.sas7bdat"
REPORT_FILE = f"{OUTPUT_PATH}/EIBQFAR2_REPORT.csv"

# ============================================================================
# FORMAT MAPPING DICTIONARIES
# ============================================================================

def build_product_mappings() -> Dict[int, str]:
    """Build product code mapping dictionary from format classes."""
    mapping = {}
    
    # FD Product Mappings
    mapping.update(FDProductFormat.MAPPINGS)
    
    # SA Product Mappings
    mapping.update(SAProductFormat.MAPPINGS)
    
    # CA Product Mappings
    mapping.update(CAProductFormat.MAPPINGS)
    
    return mapping

def build_customer_mappings() -> Dict[int, str]:
    """Build customer code mapping dictionaries."""
    # Import customer format classes
    from PBBDPFMT import (
        DPCustCD, SACustCD, FDCustCD, IFDCusCD, DDCustCD
    )
    
    mappings = {}
    
    # FD Customer Mappings (most commonly used)
    mappings.update(FDCustCD.MAPPINGS)
    
    return mappings

def build_denomination_mappings() -> Dict[int, str]:
    """Build denomination mapping dictionaries."""
    mappings = {}
    
    # FD Denomination
    fd_islamic = FDDenomFormat.ISLAMIC_RANGES
    fd_singles = FDDenomFormat.ISLAMIC_SINGLES
    
    # Add FD Islamic products
    for start, end in fd_islamic:
        for code in range(start, end + 1):
            mappings[code] = 'I'
    
    for code in fd_singles:
        mappings[code] = 'I'
    
    # SA Denomination
    for code in SADenomFormat.ISLAMIC_PRODUCTS:
        mappings[code] = 'I'
    
    # CA Denomination
    for code in CADenomFormat.ISLAMIC_PRODUCTS:
        mappings[code] = 'I'
    
    return mappings

# Build mapping dictionaries
PRODUCT_MAP = build_product_mappings()
CUSTOMER_MAP = build_customer_mappings()
DENOM_MAP = build_denomination_mappings()

# ============================================================================
# DERIVED COLUMN FUNCTIONS
# ============================================================================

def calculate_fdorgmt(remmth: pl.Expr) -> pl.Expr:
    """Calculate FD Original Term bucket code."""
    return pl.when(remmth <= 0).then(pl.lit('11')) \
        .when(remmth <= 1).then(pl.lit('12')) \
        .when(remmth <= 2).then(pl.lit('13')) \
        .when(remmth <= 3).then(pl.lit('14')) \
        .when(remmth <= 6).then(pl.lit('15')) \
        .when(remmth <= 9).then(pl.lit('16')) \
        .when(remmth <= 12).then(pl.lit('17')) \
        .when(remmth <= 18).then(pl.lit('35')) \
        .when(remmth <= 24).then(pl.lit('36')) \
        .otherwise(pl.lit('37'))

def calculate_fdrmmt(remmth: pl.Expr) -> pl.Expr:
    """Calculate FD Remaining Term bucket code."""
    return pl.when(remmth < 0).then(pl.lit('51')) \
        .when(remmth <= 1).then(pl.lit('52')) \
        .when(remmth <= 2).then(pl.lit('53')) \
        .when(remmth <= 3).then(pl.lit('54')) \
        .when(remmth <= 6).then(pl.lit('55')) \
        .when(remmth <= 9).then(pl.lit('56')) \
        .when(remmth <= 12).then(pl.lit('57')) \
        .when(remmth <= 24).then(pl.lit('61')) \
        .when(remmth <= 36).then(pl.lit('62')) \
        .when(remmth <= 48).then(pl.lit('63')) \
        .when(remmth <= 60).then(pl.lit('64')) \
        .otherwise(pl.lit('70'))

def calculate_rmfdorgmt(prodcd: pl.Expr) -> pl.Expr:
    """Calculate RMFD Original Term bucket code based on product code."""
    return pl.when(prodcd.is_in([272, 284])).then(pl.lit('12')) \
        .when(prodcd.is_in([273, 285])).then(pl.lit('13')) \
        .when(prodcd.is_in([274, 286])).then(pl.lit('14')) \
        .when(prodcd.is_in([275, 276, 277, 287, 288, 289])).then(pl.lit('15')) \
        .when(prodcd.is_in([278, 279, 280, 290, 291, 292])).then(pl.lit('16')) \
        .when(prodcd.is_in([281, 282, 283, 293, 294, 295])).then(pl.lit('17')) \
        .otherwise(pl.lit(''))

def calculate_ace_flag(prodcd: pl.Expr) -> pl.Expr:
    """Calculate ACE product flag."""
    ace_products = ProductLists.ACE_PRODUCTS
    return pl.when(prodcd.is_in(list(ace_products))).then(pl.lit(1)).otherwise(pl.lit(0))

def calculate_fcy_flag(prodcd: pl.Expr) -> pl.Expr:
    """Calculate FCY product flag."""
    fcy_products = ProductLists.FCY_PRODUCTS
    return pl.when(prodcd.is_in(list(fcy_products))).then(pl.lit(1)).otherwise(pl.lit(0))

# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def read_sas_file(file_path: str) -> pl.DataFrame:
    """
    Read SAS file and return as Polars DataFrame.
    
    Args:
        file_path: Path to SAS file (.sas7bdat)
    
    Returns:
        Polars DataFrame with data from SAS file
    """
    try:
        logger.info(f"Reading SAS file: {file_path}")
        
        # Use Polars to read SAS file
        df = pl.read_sas(file_path)
        
        logger.info(f"Successfully read {file_path}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error reading SAS file {file_path}: {e}")
        raise

def process_deposit_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process deposit data with format mappings and derived columns.
    
    Args:
        df: Raw deposit DataFrame
    
    Returns:
        Processed deposit DataFrame
    """
    logger.info("Processing deposit data...")
    
    # Ensure required columns exist
    required_cols = ['PRODCD', 'CUSTCD', 'AMOUNT', 'CURCD', 'INTCODE']
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in deposit data")
            df = df.with_columns(pl.lit(None).alias(col))
    
    # Apply format mappings using replace() instead of map_dict()
    processed_df = df.with_columns([
        # Product Code formatting
        pl.col('PRODCD').replace(PRODUCT_MAP).alias('PRODCD_FORMATTED'),
        
        # Customer Code formatting
        pl.col('CUSTCD').replace(CUSTOMER_MAP).alias('CUSTCD_FORMATTED'),
        
        # Denomination formatting
        pl.col('PRODCD').replace(DENOM_MAP).alias('DENOM_FORMATTED'),
        
        # FCY Term formatting (for interest plan codes)
        pl.col('INTCODE').replace(FCYTermFormat.MAPPINGS).alias('FCYTERM_FORMATTED'),
    ])
    
    # Fill null values with defaults
    processed_df = processed_df.with_columns([
        pl.col('PRODCD_FORMATTED').fill_null('42130'),
        pl.col('CUSTCD_FORMATTED').fill_null('78'),
        pl.col('DENOM_FORMATTED').fill_null('D'),
        pl.col('FCYTERM_FORMATTED').fill_null(0),
    ])
    
    # Add derived columns
    processed_df = processed_df.with_columns([
        # ACE flag
        calculate_ace_flag(pl.col('PRODCD')).alias('ACE_FLAG'),
        
        # FCY flag
        calculate_fcy_flag(pl.col('PRODCD')).alias('FCY_FLAG'),
        
        # FD Original Term buckets (if REMMTH exists)
        pl.when(pl.col('REMMTH').is_not_null())
        .then(calculate_fdorgmt(pl.col('REMMTH')))
        .otherwise(pl.lit(None))
        .alias('FDORGMT_FORMATTED'),
        
        # FD Remaining Term buckets (if REMMTH exists)
        pl.when(pl.col('REMMTH').is_not_null())
        .then(calculate_fdrmmt(pl.col('REMMTH')))
        .otherwise(pl.lit(None))
        .alias('FDRMMT_FORMATTED'),
        
        # RMFD Original Term (based on product code)
        calculate_rmfdorgmt(pl.col('PRODCD')).alias('RMFDORGMT_FORMATTED'),
        
        # Additional derived fields
        pl.when(pl.col('AMOUNT') > 0)
        .then(pl.col('AMOUNT'))
        .otherwise(0)
        .alias('POSITIVE_AMOUNT'),
        
        pl.when(pl.col('AMOUNT') < 0)
        .then(pl.col('AMOUNT').abs())
        .otherwise(0)
        .alias('NEGATIVE_AMOUNT'),
    ])
    
    logger.info(f"Deposit data processed: {processed_df.shape[0]} rows")
    return processed_df

def process_trust_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process trust data with format mappings.
    
    Args:
        df: Raw trust DataFrame
    
    Returns:
        Processed trust DataFrame
    """
    logger.info("Processing trust data...")
    
    # Ensure required columns exist
    required_cols = ['PRODCD', 'CUSTCD', 'AMOUNT', 'CURCD']
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in trust data")
            df = df.with_columns(pl.lit(None).alias(col))
    
    # Apply format mappings
    processed_df = df.with_columns([
        # Product Code formatting
        pl.col('PRODCD').replace(PRODUCT_MAP).alias('PRODCD_FORMATTED'),
        
        # Customer Code formatting
        pl.col('CUSTCD').replace(CUSTOMER_MAP).alias('CUSTCD_FORMATTED'),
        
        # Denomination formatting
        pl.col('PRODCD').replace(DENOM_MAP).alias('DENOM_FORMATTED'),
        
        # Derived fields
        calculate_ace_flag(pl.col('PRODCD')).alias('ACE_FLAG'),
        calculate_fcy_flag(pl.col('PRODCD')).alias('FCY_FLAG'),
    ])
    
    # Fill null values with defaults
    processed_df = processed_df.with_columns([
        pl.col('PRODCD_FORMATTED').fill_null('42130'),
        pl.col('CUSTCD_FORMATTED').fill_null('78'),
        pl.col('DENOM_FORMATTED').fill_null('D'),
    ])
    
    logger.info(f"Trust data processed: {processed_df.shape[0]} rows")
    return processed_df

def combine_data(deposit_df: pl.DataFrame, trust_df: pl.DataFrame) -> pl.DataFrame:
    """
    Combine deposit and trust data into a single dataset.
    
    Args:
        deposit_df: Processed deposit DataFrame
        trust_df: Processed trust DataFrame
    
    Returns:
        Combined DataFrame
    """
    logger.info("Combining deposit and trust data...")
    
    # Add source indicator
    deposit_df = deposit_df.with_columns(pl.lit('DEPOSIT').alias('SOURCE'))
    trust_df = trust_df.with_columns(pl.lit('TRUST').alias('SOURCE'))
    
    # Concatenate
    combined = pl.concat([deposit_df, trust_df], how='vertical')
    
    logger.info(f"Combined data: {combined.shape[0]} rows")
    return combined

def apply_aggregations(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply aggregations to the combined dataset.
    
    Args:
        df: Combined DataFrame
    
    Returns:
        Aggregated DataFrame
    """
    logger.info("Applying aggregations...")
    
    # Group by key dimensions and aggregate
    agg_df = df.group_by([
        'PRODCD_FORMATTED',
        'CUSTCD_FORMATTED',
        'DENOM_FORMATTED',
        'FDORGMT_FORMATTED',
        'FDRMMT_FORMATTED',
        'RMFDORGMT_FORMATTED',
        'ACE_FLAG',
        'FCY_FLAG',
        'CURCD',
        'SOURCE'
    ]).agg([
        pl.col('AMOUNT').sum().alias('TOTAL_AMOUNT'),
        pl.col('POSITIVE_AMOUNT').sum().alias('TOTAL_POSITIVE'),
        pl.col('NEGATIVE_AMOUNT').sum().alias('TOTAL_NEGATIVE'),
        pl.count().alias('RECORD_COUNT'),
    ])
    
    logger.info(f"Aggregated data: {agg_df.shape[0]} rows")
    return agg_df

def add_report_parameters(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add report parameters as constant columns.
    
    Args:
        df: DataFrame to add parameters to
    
    Returns:
        DataFrame with report parameters
    """
    return df.with_columns([
        pl.lit(REPORT_DATE.strftime('%Y-%m-%d')).alias('REPORT_DATE'),
        pl.lit(REPTMON).alias('REPTMON'),
        pl.lit(REPTYEAR).alias('REPTYEAR'),
        pl.lit(SDESC).alias('SDESC'),
        pl.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('RUN_DATETIME'),
    ])

# ============================================================================
# MAIN PROCESSING PIPELINE
# ============================================================================

def main():
    """Main processing pipeline."""
    try:
        logger.info("=" * 80)
        logger.info("Starting EIBQFAR2_CONV_INSURANCE processing")
        logger.info("=" * 80)
        
        # Log report parameters
        logger.info(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
        logger.info(f"REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
        logger.info(f"SDESC: {SDESC}")
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        
        # Save report parameters to file
        params_file = f"{OUTPUT_PATH}/report_params.txt"
        with open(params_file, 'w') as f:
            f.write(f"Report Date: {REPORT_DATE}\n")
            f.write(f"REPTMON: {REPTMON}\n")
            f.write(f"REPTYEAR: {REPTYEAR}\n")
            f.write(f"SDESC: {SDESC}\n")
            f.write(f"Run Time: {datetime.datetime.now()}\n")
        logger.info(f"Report parameters saved to {params_file}")
        
        # Process Trust data
        logger.info("Processing TRUST data from cisdepxn.sas7bdat")
        trust_df = read_sas_file(TRUST_FILE)
        logger.info(f"TRUST records: {trust_df.shape[0]}")
        
        trust_processed = process_trust_data(trust_df)
        
        # Process Deposit data
        logger.info("Processing deposit data from cisdepd.sas7bdat")
        deposit_df = read_sas_file(DEPOSIT_FILE)
        logger.info(f"DEPOSIT records: {deposit_df.shape[0]}")
        
        deposit_processed = process_deposit_data(deposit_df)
        
        # Combine data
        combined_df = combine_data(deposit_processed, trust_processed)
        
        # Apply aggregations
        aggregated_df = apply_aggregations(combined_df)
        
        # Add report parameters
        final_df = add_report_parameters(aggregated_df)
        
        # Save output
        logger.info(f"Saving final report to {OUTPUT_FILE}")
        final_df.write_sas(OUTPUT_FILE)
        
        # Also save as CSV for verification
        csv_file = f"{OUTPUT_PATH}/EIBQFAR2_BASE.csv"
        logger.info(f"Saving CSV to {csv_file}")
        final_df.write_csv(csv_file)
        
        # Log summary statistics
        logger.info("=" * 80)
        logger.info("Processing Summary:")
        logger.info(f"  Total records processed: {combined_df.shape[0]}")
        logger.info(f"  Total aggregated records: {final_df.shape[0]}")
        logger.info(f"  Output file: {OUTPUT_FILE}")
        logger.info(f"  CSV file: {csv_file}")
        logger.info("=" * 80)
        logger.info("EIBQFAR2_CONV_INSURANCE completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

# ============================================================================
# FUNCTION TO APPLY FORMAT MAPPINGS (Exported for external use)
# ============================================================================

def apply_format_mappings(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply format mappings to a DataFrame.
    This function is maintained for backward compatibility.
    
    Args:
        df: DataFrame with columns to map
    
    Returns:
        DataFrame with format mappings applied
    """
    logger.info("Applying format mappings...")
    
    # Ensure required columns exist
    if 'PRODCD' in df.columns:
        df = df.with_columns([
            pl.col('PRODCD').replace(PRODUCT_MAP).alias('PRODCD_FORMATTED')
        ])
    
    if 'CUSTCD' in df.columns:
        df = df.with_columns([
            pl.col('CUSTCD').replace(CUSTOMER_MAP).alias('CUSTCD_FORMATTED')
        ])
    
    # Fill null values
    if 'PRODCD_FORMATTED' in df.columns:
        df = df.with_columns([
            pl.col('PRODCD_FORMATTED').fill_null('42130')
        ])
    
    if 'CUSTCD_FORMATTED' in df.columns:
        df = df.with_columns([
            pl.col('CUSTCD_FORMATTED').fill_null('78')
        ])
    
    return df

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
  
