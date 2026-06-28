import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import logging

# Import format definitions from PBBDPFMT
import PBBDPFMT
from PBBDPFMT import (
    SADenomFormat, SAProductFormat,
    FDDenomFormat, FDProductFormat,
    CADenomFormat, CAProductFormat,
    FCYTermFormat, fdorgmt_format,
    ProductLists
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('eiq_fisf_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
deposit1_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2")
output_path.mkdir(exist_ok=True, parents=True)

logger.info("PBBDPFMT formats loaded successfully")

def read_sas_to_polars(filepath: Path) -> pl.DataFrame:
    """
    Read a SAS .sas7bdat file using pyreadstat and return as Polars DataFrame
    """
    try:
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return pl.DataFrame()
        
        logger.info(f"Reading SAS file: {filepath}")
        
        # Read the full file
        df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        pl_df = pl.from_pandas(df)
        
        logger.info(f"Successfully read {filepath.name}: {len(pl_df):,} rows, {len(pl_df.columns)} columns")
        return pl_df
        
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return pl.DataFrame()

def calculate_report_date() -> datetime.date:
    """Calculate report date based on SAS logic"""
    today = datetime.date.today()
    date_string = f"0101{today.year}"
    reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)
    
    # IF MONTH(TODAY()) > 6 THEN REPTDATE = TODAY();
    if today.month > 6:
        reptdate = today
        
    return reptdate

def process_islamic_deposit_data(deposit1_path: Path) -> pl.DataFrame:
    """
    Process Islamic deposit data from CISDEPD
    """
    logger.info("Processing Islamic deposit data from cisdepd.sas7bdat")
    
    # Read SAS file
    cisdepd_df = read_sas_to_polars(deposit1_path / "cisdepd.sas7bdat")
    
    if cisdepd_df.is_empty():
        logger.warning("DEPOSIT1.cisdepd is empty or not found")
        return pl.DataFrame()
    
    # Check if required columns exist (case insensitive)
    required_cols = ['BRANCH', 'PRODCD', 'INSUREBR']
    
    # Create a mapping of lowercase column names to actual column names
    col_mapping = {col.lower(): col for col in cisdepd_df.columns}
    
    # Check if all required columns exist (case insensitive)
    missing_cols = []
    for col in required_cols:
        if col.lower() not in col_mapping:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"Missing columns in cisdepd: {missing_cols}")
        logger.info(f"Available columns: {list(cisdepd_df.columns)[:10]}...")
        return pl.DataFrame()
    
    # Rename columns to uppercase for consistency
    rename_dict = {}
    for col in required_cols:
        actual_col = col_mapping[col.lower()]
        if actual_col != col:
            rename_dict[actual_col] = col
    
    if rename_dict:
        cisdepd_df = cisdepd_df.rename(rename_dict)
    
    # Convert PRODCD to integer if it's string
    if 'PRODCD' in cisdepd_df.columns:
        if cisdepd_df['PRODCD'].dtype == pl.String:
            cisdepd_df = cisdepd_df.with_columns([
                pl.col('PRODCD').cast(pl.Int64).alias('PRODCD')
            ])
    
    # Select required columns
    cisdepd_df = cisdepd_df.select(['BRANCH', 'PRODCD', 'INSUREBR'])
    
    logger.info(f"Islamic DEPOSIT records: {cisdepd_df.height:,}")
    return cisdepd_df

def apply_format_mappings(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply format mappings to PRODCD column using PBBDPFMT formats
    """
    if 'PRODCD' in df.columns:
        # Ensure PRODCD is integer type
        if df['PRODCD'].dtype != pl.Int64:
            df = df.with_columns([
                pl.col('PRODCD').cast(pl.Int64).alias('PRODCD')
            ])
        
        # Apply product format to get BNM codes
        logger.info("Applying product format mappings from PBBDPFMT")
        
        # Apply format using map_elements
        df = df.with_columns([
            pl.col('PRODCD').map_elements(
                lambda x: _get_product_format(x),
                return_dtype=pl.String
            ).alias('PRODCD_FORMATTED')
        ])
        
        # Also add denomination (Islamic/Domestic) classification
        df = df.with_columns([
            pl.col('PRODCD').map_elements(
                lambda x: _get_denomination(x),
                return_dtype=pl.String
            ).alias('DENOMINATION')
        ])
    
    return df

def _get_product_format(prodcd) -> str:
    """
    Get product format from PBBDPFMT based on product code
    """
    # Handle None or null values
    if prodcd is None or prodcd == '':
        return 'UNKNOWN'
    
    # Convert to int if it's a string
    try:
        prodcd_int = int(prodcd) if not isinstance(prodcd, int) else prodcd
    except (ValueError, TypeError):
        return str(prodcd)
    
    # Check if it's a Current Account product
    if prodcd_int in ProductLists.CURX_PRODUCTS:
        return CAProductFormat.format(prodcd_int)
    
    # Check if it's a Savings product (typically 200-300 range)
    if 200 <= prodcd_int <= 300:
        return SAProductFormat.format(prodcd_int)
    
    # Check if it's a Fixed Deposit product (typically 229-997 range)
    if 229 <= prodcd_int <= 997:
        return FDProductFormat.format(prodcd_int)
    
    # Default - return the product code as string
    return str(prodcd_int)

def _get_denomination(prodcd) -> str:
    """
    Get Islamic/Domestic denomination from PBBDPFMT
    """
    # Handle None or null values
    if prodcd is None or prodcd == '':
        return 'UNKNOWN'
    
    # Convert to int if it's a string
    try:
        prodcd_int = int(prodcd) if not isinstance(prodcd, int) else prodcd
    except (ValueError, TypeError):
        return 'UNKNOWN'
    
    # Check if it's a Current Account product
    if prodcd_int in ProductLists.CURX_PRODUCTS:
        return CADenomFormat.format(prodcd_int)
    
    # Check if it's a Savings product
    if 200 <= prodcd_int <= 300:
        return SADenomFormat.format(prodcd_int)
    
    # Check if it's a Fixed Deposit product
    if 229 <= prodcd_int <= 997:
        return FDDenomFormat.format(prodcd_int)
    
    return 'UNKNOWN'

def generate_islamic_tabular_report(summary_df: pl.DataFrame, output_path: Path) -> None:
    """
    Generate and display tabular report for Islamic banking
    """
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
    
    # Handle null values in BRANCH column - replace with 'UNKNOWN'
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').fill_null('UNKNOWN').cast(pl.String).alias('BRANCH')
    ])
    
    # Format numbers for display
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
    print("APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (ISLAMIC)")
    print("="*120)
    
    # Print header
    header = "BRANCH".ljust(20)
    for product in product_categories:
        header += f"{product:<20}"
    print(header)
    print("-" * 120)
    
    # Print rows - handle None/Null values safely
    for row in formatted_table.iter_rows(named=True):
        # Ensure BRANCH is not None
        branch = row.get('BRANCH', 'UNKNOWN')
        if branch is None:
            branch = 'UNKNOWN'
        line = f"{str(branch):<20}"
        
        for product in product_categories:
            value = row.get(product, "0.00")
            if value is None:
                value = "0.00"
            line += f"{str(value):<20}"
        print(line)
    
    print("-" * 120)
    print(f"{'GRAND TOTAL:':<20}{grand_total:,.2f}")
    print("="*120 + "\n")
    
    # Save detailed results
    summary_df.write_csv(output_path / "ISLAMIC_INSURANCE_APPORTIONMENT.csv")
    summary_df.write_parquet(output_path / "ISLAMIC_INSURANCE_APPORTIONMENT.parquet")
    pivot_table.write_csv(output_path / "ISLAMIC_PIVOT_TABLE.csv")
    pivot_table.write_parquet(output_path / "ISLAMIC_PIVOT_TABLE.parquet")
    
    logger.info(f"Islamic report saved to {output_path}")

def main():
    """Main processing function for EIIQFISF - Islamic banking report"""
    try:
        # Calculate report date
        reptdate = calculate_report_date()
        SDESC = 'PUBLIC BANK BERHAD (ISLAMIC)'
        REPTMON = f"{reptdate.month:02d}"
        REPTYEAR = reptdate.strftime('%y')
        
        logger.info(f"Report Date: {reptdate}")
        logger.info(f"REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
        logger.info(f"SDESC: {SDESC}")
        
        # Create REPTDATE DataFrame
        reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
        reptdate_df.write_parquet(output_path / "REPTDATE_ISLAMIC.parquet")
        reptdate_df.write_csv(output_path / "REPTDATE_ISLAMIC.csv")
        logger.info(f"REPTDATE saved to {output_path}")
        
        # Process Islamic data sources
        deposit_df = process_islamic_deposit_data(deposit1_path)
        
        if deposit_df.is_empty():
            logger.warning("No Islamic data available for processing")
            return
        
        # Apply format mappings from PBBDPFMT
        rpt_base = apply_format_mappings(deposit_df)
        
        # Save base dataset
        rpt_base.write_parquet(output_path / "RPT_BASE_ISLAMIC.parquet")
        rpt_base.write_csv(output_path / "RPT_BASE_ISLAMIC.csv")
        
        logger.info(f"RPT_BASE records: {rpt_base.height:,}")
        
        # Generate summary - ensure BRANCH is string type for consistency
        summary = rpt_base.group_by(['BRANCH', 'PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ]).sort(['BRANCH', 'PRODCD_FORMATTED'])
        
        # Convert BRANCH to string and handle nulls
        summary = summary.with_columns([
            pl.col('BRANCH').fill_null('UNKNOWN').cast(pl.String).alias('BRANCH')
        ])
        
        # Calculate total - ensure BRANCH is string type
        total_summary = rpt_base.group_by(['PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ]).with_columns([
            pl.lit('TOTAL').cast(pl.String).alias('BRANCH')
        ])
        
        # Combine and generate report
        final_summary = pl.concat([summary, total_summary], how="diagonal")
        generate_islamic_tabular_report(final_summary, output_path)
        
        logger.info("ISLAMIC PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error in Islamic processing: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
