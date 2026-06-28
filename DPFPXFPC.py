import polars as pl
import duckdb
from pathlib import Path
import datetime
import pandas as pd
import pyreadstat
import logging
from typing import Optional, Dict, Any
import warnings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mdic_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore')

# Configuration
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
deposit1_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2")
output_path.mkdir(exist_ok=True, parents=True)

# PROC FORMAT equivalent - Create mapping dictionaries with consistent naming
FORMATS = {
    'JOINFMT': {
        0: 'ORGANISATION',
        1: 'PERSONAL',
        2: 'JOIN 2',
        3: 'JOIN 3',
        4: 'JOIN 4',
        5: 'JOIN 5',
        6: 'JOIN 6',
        7: 'JOIN 7',
        8: 'JOIN 8',
        9: 'JOIN 9',
        10: 'JOIN 10',
        11: 'JOIN 11'
    },
    'PRODFMD': {
        '42110': 'CA (A)',
        '42310': 'CA (A)',
        '34180': 'CA (A)',
        '42610': 'FX CA (A)',
        '42120': 'SA (B)',
        '42320': 'SA (B)',
        '42130': 'FD (C)',
        '42630': 'FX FD (C)',
        '42132': 'GID',
        '42133': 'GID',
        '42180': 'HOUSING DEV (D)',
        '42XXX': 'ATM/SI (E)',
        '46795': 'DEBIT CARD (E)',
        '42199': 'OD CA ',
        '42699': 'FX ODCA'
    },
    'PRODBRH': {
        '42110': 'DDMAND',
        '42310': 'DDMAND',
        '34180': 'DDMAND',
        '42199': 'DDMAND',
        '42120': 'DSVING',
        '42320': 'DSVING',
        '42130': 'DFIXED',
        '42132': 'DFIXED',
        '42133': 'DFIXED',
        '42180': 'DDMAND',
        '42610': 'FDMAND',
        '42699': 'FDMAND',
        '42630': 'FFIXED',
        '42XXX': 'ATM/SI (E)',
        '46795': 'DEBIT CARD (E)',
        'TRUST': 'TRUST ACCT'
    },
    'PRODFMI': {
        '42110': 'CA (A)',
        '42310': 'CA (A)',
        '34180': 'CA (A)',
        '42120': 'SA (D)',
        '42320': 'SA (D)',
        '42130': 'FD',
        '42610': 'FXCA',
        '42630': 'FXFD',
        '42132': 'GID (B)',
        '42133': 'GID (B)',
        '42180': 'HOUSING DEV (C)',
        '42XXX': 'ATM/SI (E)'
    }
}

logger.info("Format mappings created successfully")

# Enhanced function to read SAS file with metadata
def read_sas_to_polars(filepath: Path) -> tuple[pl.DataFrame, Optional[Dict[str, Any]]]:
    """
    Read a SAS .sas7bdat file using pyreadstat and return as Polars DataFrame with metadata
    
    Args:
        filepath: Path to SAS file
        
    Returns:
        Tuple of (DataFrame, metadata dictionary)
    """
    try:
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return pl.DataFrame(), None
            
        df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        pl_df = pl.from_pandas(df)
        
        # Extract metadata
        metadata = {
            'file_label': meta.file_label,
            'table_name': meta.table_name,
            'number_rows': meta.number_rows,
            'number_columns': meta.number_columns,
            'column_names': meta.column_names,
            'column_labels': meta.column_labels_to_dict(),
            'variable_display_width': meta.variable_display_width,
            'variable_format': meta.variable_format,
            'variable_types': meta.variable_types,
            'value_labels': meta.value_labels,
            'variable_value_labels': meta.variable_value_labels
        }
        
        logger.info(f"Successfully read {filepath.name}: {len(pl_df)} rows, {len(pl_df.columns)} columns")
        return pl_df, metadata
        
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return pl.DataFrame(), None

# OPTIONS equivalent
# YEARCUTOFF=1950 - handled by Python's datetime
# COMPRESS=YES - handled by Parquet compression

def calculate_report_date() -> datetime.date:
    """Calculate report date based on SAS logic"""
    today = datetime.date.today()
    date_string = f"0101{today.year}"
    reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)
    
    # IF MONTH(TODAY()) > 6 THEN REPTDATE = TODAY();
    if today.month > 6:
        reptdate = today
        
    return reptdate

# Data processing functions
def process_trust_data(pidmfin_path: Path) -> pl.DataFrame:
    """Process TRUST data from CISDEPXN"""
    logger.info("Processing TRUST data")
    cisdepxn_df, metadata = read_sas_to_polars(pidmfin_path / "CISDEPXN.sas7bdat")
    
    if cisdepxn_df.is_empty():
        logger.warning("PIDMFIN.CISDEPXN is empty or not found")
        return pl.DataFrame()
    
    # Verify required columns exist
    required_cols = ['ACCTYPE2', 'BENEINT', 'BRANCH', 'PRODCD', 'INSURED']
    missing_cols = [col for col in required_cols if col not in cisdepxn_df.columns]
    if missing_cols:
        logger.warning(f"Missing columns in CISDEPXN: {missing_cols}")
        return pl.DataFrame()
    
    trust_df = cisdepxn_df.filter(
        (pl.col('ACCTYPE2').is_in([3, 7])) & 
        (pl.col('BENEINT').is_not_null())
    ).select([
        'BRANCH', 'PRODCD', 'INSURED'
    ]).rename({'INSURED': 'INSUREBR'})
    
    # Add data source flag
    trust_df = trust_df.with_columns(pl.lit('TRUST').alias('DATA_SOURCE'))
    
    logger.info(f"TRUST records: {trust_df.height}")
    return trust_df

def process_deposit_data(deposit1_path: Path) -> pl.DataFrame:
    """Process deposit data from CISDEPD"""
    logger.info("Processing deposit data")
    cisdepd_df, metadata = read_sas_to_polars(deposit1_path / "CISDEPD.sas7bdat")
    
    if cisdepd_df.is_empty():
        logger.warning("DEPOSIT1.CISDEPD is empty or not found")
        return pl.DataFrame()
    
    # Verify required columns exist
    required_cols = ['BRANCH', 'PRODCD', 'INSUREBR']
    missing_cols = [col for col in required_cols if col not in cisdepd_df.columns]
    if missing_cols:
        logger.warning(f"Missing columns in CISDEPD: {missing_cols}")
        return pl.DataFrame()
    
    # Add data source flag
    cisdepd_df = cisdepd_df.with_columns(pl.lit('DEPOSIT').alias('DATA_SOURCE'))
    
    logger.info(f"DEPOSIT records: {cisdepd_df.height}")
    return cisdepd_df

def apply_format_mappings(df: pl.DataFrame, format_mappings: Dict[str, Dict]) -> pl.DataFrame:
    """Apply format mappings to DataFrame columns"""
    for col_name, mapping in format_mappings.items():
        if col_name in df.columns:
            df = df.with_columns([
                pl.col(col_name).map_dict(mapping).alias(f"{col_name}_FORMATTED")
            ])
    return df

def generate_tabular_report(summary_df: pl.DataFrame, output_path: Path) -> None:
    """Generate and display tabular report similar to PROC TABULATE"""
    if summary_df.is_empty():
        logger.warning("No data available for tabulation")
        return
    
    # Create pivot table
    product_categories = summary_df['PRODCD_FORMATTED'].unique().sort()
    
    pivot_table = summary_df.pivot(
        index='BRANCH',
        columns='PRODCD_FORMATTED', 
        values='INSUREBR_SUM',
        aggregate_function='sum'
    ).fill_null(0)
    
    # Calculate grand total
    grand_total = summary_df.select(pl.col('INSUREBR_SUM').sum()).row(0)[0]
    
    # Format numbers
    formatted_table = pivot_table.clone()
    for col in formatted_table.columns:
        if col != 'BRANCH' and formatted_table[col].dtype in [pl.Float64, pl.Int64]:
            formatted_table = formatted_table.with_columns([
                pl.col(col).map_elements(
                    lambda x: f"{x:,.2f}" if x is not None else "0.00", 
                    return_dtype=pl.String
                ).alias(col)
            ])
    
    # Display report
    print("\n" + "="*120)
    print("APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (CONVENTIONAL)")
    print("="*120)
    
    # Print header
    header = "BRANCH".ljust(20)
    for product in product_categories:
        header += f"{product:<20}"
    print(header)
    print("-" * 120)
    
    # Print rows
    for row in formatted_table.iter_rows(named=True):
        line = f"{row['BRANCH']:<20}"
        for product in product_categories:
            value = row.get(product, "0.00")
            line += f"{value:<20}"
        print(line)
    
    print("-" * 120)
    print(f"{'GRAND TOTAL:':<20}{grand_total:,.2f}")
    print("="*120 + "\n")
    
    # Save detailed results
    summary_df.write_csv(output_path / "CONVENTIONAL_INSURANCE_APPORTIONMENT.csv")
    summary_df.write_parquet(output_path / "CONVENTIONAL_INSURANCE_APPORTIONMENT.parquet")
    pivot_table.write_csv(output_path / "CONVENTIONAL_PIVOT_TABLE.csv")
    pivot_table.write_parquet(output_path / "CONVENTIONAL_PIVOT_TABLE.parquet")
    
    logger.info(f"Report saved to {output_path}")

def main():
    """Main processing function"""
    try:
        # Calculate report date
        reptdate = calculate_report_date()
        SDESC = 'PUBLIC BANK BERHAD'
        REPTMON = f"{reptdate.month:02d}"
        REPTYEAR = reptdate.strftime('%y')
        
        logger.info(f"Report Date: {reptdate}")
        logger.info(f"REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
        logger.info(f"SDESC: {SDESC}")
        
        # Create REPTDATE DataFrame
        reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
        reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
        reptdate_df.write_csv(output_path / "REPTDATE.csv")
        
        # Process data sources
        trust_df = process_trust_data(pidmfin_path)
        deposit_df = process_deposit_data(deposit1_path)
        
        # Combine datasets
        dataframes = [df for df in [trust_df, deposit_df] if not df.is_empty()]
        
        if dataframes:
            rpt_base = pl.concat(dataframes, how="diagonal")
            
            # Apply format mappings
            rpt_base = apply_format_mappings(
                rpt_base, 
                {'PRODCD': FORMATS['PRODBRH']}
            )
            
            # Save base dataset
            rpt_base.write_parquet(output_path / "RPT_BASE.parquet")
            rpt_base.write_csv(output_path / "RPT_BASE.csv")
            
            logger.info(f"RPT_BASE records: {rpt_base.height}")
            
            # Generate summary
            summary = rpt_base.group_by(['BRANCH', 'PRODCD_FORMATTED']).agg([
                pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
            ]).sort(['BRANCH', 'PRODCD_FORMATTED'])
            
            # Calculate total
            total_summary = rpt_base.group_by(['PRODCD_FORMATTED']).agg([
                pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
            ]).with_columns([
                pl.lit('TOTAL').alias('BRANCH')
            ])
            
            # Combine and generate report
            final_summary = pl.concat([summary, total_summary], how="diagonal")
            generate_tabular_report(final_summary, output_path)
            
        else:
            logger.warning("No data available for processing")
        
        logger.info("PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
