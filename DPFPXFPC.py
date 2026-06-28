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
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQFISF")
deposit1_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQFISF")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQFISF")
output_path.mkdir(exist_ok=True, parents=True)

# Islamic product format mappings
ISLAMIC_PROD_FORMATS = {
    'IR070': 'IR070',
    '42110': 'DDMAND',
    '42310': 'DDMAND',
    '34180': 'DDMAND',
    '42199': 'DDMAND',
    '42610': 'FDMAND',
    '42699': 'FDMAND',
    '42120': 'DSVING',
    '42320': 'DSVING',
    '42130': 'DFIXED',
    '42132': 'DFIXED',
    '42133': 'DFIXED',
    '42630': 'FFIXED',
    '42180': 'DDMAND',
    '42XXX': 'ATM/SI (E)',
    '46795': 'DEBIT CARD (E)',
}

# Define the expected product categories in order
PRODUCT_CATEGORIES = ['IR070', 'DDMAND', 'DSVING', 'DFIXED', 'FDMAND']

logger.info("PBBDPFMT formats loaded successfully")

def read_sas_to_polars(filepath: Path) -> pl.DataFrame:
    """Read a SAS .sas7bdat file using pyreadstat and return as Polars DataFrame"""
    try:
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return pl.DataFrame()
        
        logger.info(f"Reading SAS file: {filepath}")
        df, meta = pyreadstat.read_sas7bdat(filepath)
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
    if today.month > 6:
        reptdate = today
    return reptdate

def process_islamic_deposit_data(deposit1_path: Path) -> pl.DataFrame:
    """Process Islamic deposit data from CISDEPI"""
    logger.info("Processing Islamic deposit data from cisdepi.sas7bdat")
    cisdepi_df = read_sas_to_polars(deposit1_path / "cisdepi.sas7bdat")
    
    if cisdepi_df.is_empty():
        logger.warning("DEPOSIT1.cisdepi is empty or not found")
        return pl.DataFrame()
    
    required_cols = ['BRANCH', 'PRODCD', 'INSUREBR']
    col_mapping = {col.lower(): col for col in cisdepi_df.columns}
    missing_cols = []
    for col in required_cols:
        if col.lower() not in col_mapping:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"Missing columns in cisdepi: {missing_cols}")
        return pl.DataFrame()
    
    rename_dict = {}
    for col in required_cols:
        actual_col = col_mapping[col.lower()]
        if actual_col != col:
            rename_dict[actual_col] = col
    
    if rename_dict:
        cisdepi_df = cisdepi_df.rename(rename_dict)
    
    # Keep PRODCD as string
    cisdepi_df = cisdepi_df.select(['BRANCH', 'PRODCD', 'INSUREBR'])
    
    # Convert BRANCH to integer for proper sorting
    if 'BRANCH' in cisdepi_df.columns:
        if cisdepi_df['BRANCH'].dtype == pl.Float64:
            cisdepi_df = cisdepi_df.with_columns([
                pl.col('BRANCH').cast(pl.Int64).alias('BRANCH')
            ])
    
    logger.info(f"Islamic DEPOSIT records: {cisdepi_df.height:,}")
    return cisdepi_df

def apply_format_mappings(df: pl.DataFrame) -> pl.DataFrame:
    """Apply format mappings to PRODCD column using Islamic product mappings"""
    if 'PRODCD' in df.columns:
        logger.info("Applying product format mappings")
        
        # Apply format using map_elements
        df = df.with_columns([
            pl.col('PRODCD').map_elements(
                lambda x: _get_product_format(x),
                return_dtype=pl.String
            ).alias('PRODCD_FORMATTED')
        ])
        
        # Debug: Show unique formatted values
        unique_formats = df['PRODCD_FORMATTED'].unique().to_list()
        logger.info(f"Unique PRODCD_FORMATTED values: {unique_formats}")
    
    return df

def _get_product_format(prodcd) -> str:
    """Get product format from Islamic product mappings"""
    if prodcd is None or prodcd == '':
        return 'UNKNOWN'
    
    # Convert to string for consistent handling
    prodcd_str = str(prodcd).strip()
    
    # Check if it's an Islamic product code (like "IR070")
    if prodcd_str in ISLAMIC_PROD_FORMATS:
        return ISLAMIC_PROD_FORMATS[prodcd_str]
    
    # Try to convert to int for numeric product codes
    try:
        prodcd_int = int(prodcd_str)
    except (ValueError, TypeError):
        return prodcd_str
    
    # Check numeric product codes
    if prodcd_int in ProductLists.CURX_PRODUCTS:
        return 'DDMAND'
    
    if 200 <= prodcd_int <= 300:
        return 'DSVING'
    
    if 229 <= prodcd_int <= 997:
        return 'DFIXED'
    
    return prodcd_str

def format_islamic_number(x):
    """Format number for Islamic report - empty cells show dots"""
    if x is None or x == 0:
        return "                 ."
    return f"{x:>18,.2f}"

def generate_islamic_txt_report(summary_df: pl.DataFrame, output_path: Path, report_date: datetime.date) -> None:
    """Generate TXT report for Islamic banking in the exact format from the example"""
    if summary_df.is_empty():
        logger.warning("No data available for report generation")
        return
    
    # Create pivot table
    pivot_table = summary_df.pivot(
        index='BRANCH',
        on='PRODCD_FORMATTED',
        values='INSUREBR_SUM',
        aggregate_function='sum'
    ).fill_null(0)
    
    # Ensure all product categories exist
    for col in PRODUCT_CATEGORIES:
        if col not in pivot_table.columns:
            pivot_table = pivot_table.with_columns(pl.lit(0.0).alias(col))
    
    # Calculate grand total
    grand_total = summary_df.select(pl.col('INSUREBR_SUM').sum()).row(0)[0]
    
    # Handle null values in BRANCH and convert to integer string
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').fill_null(0).cast(pl.Int64).cast(pl.String).alias('BRANCH')
    ])
    
    # Sort by BRANCH as integer (numeric order)
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').cast(pl.Int64).alias('BRANCH_NUM')
    ]).sort('BRANCH_NUM').drop('BRANCH_NUM')
    
    # Get current time for header, but use the report date
    now = datetime.datetime.now()
    time_str = now.strftime("%I:%M")  # Time only (e.g., "15:08")
    
    # Format the report date for the header
    date_str = report_date.strftime("%A, %B %d, %Y")
    
    # Combine time and date for header
    header_date_str = f"{time_str} {date_str}"
    
    # Prepare TXT file
    txt_file = output_path / "EIIQFISF_ISLAMIC_REPORT.txt"
    
    with open(txt_file, 'w') as f:
        # Header - Page 1 - Use the report date, not current date
        f.write(" " * 38 + "APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(ISLAMIC)        ")
        f.write(f"{header_date_str}   1\n")
        f.write("\n" * 2)
        f.write(" " * 13 + "-" * 73 + "\n")
        
        # Main table header
        f.write(" " * 13 + "|BRANCH  |" + " " * 46 + "PRODUCT" + " " * 46 + "|\n")
        f.write(" " * 13 + "|        |" + "-" * 46 + "+" + "-" * 46 + "|\n")
        
        # Product columns header - first row
        f.write(" " * 13 + "|        |      IR070       |      DDMAND      |      DSVING      |      DFIXED      |      FDMAND      |\n")
        
        # Product columns header - second row (subheaders)
        f.write(" " * 13 + "|        |------------------+------------------+------------------+------------------+------------------|\n")
        f.write(" " * 13 + "|        |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |\n")
        f.write(" " * 13 + "|        |     INSURED      |     INSURED      |     INSURED      |     INSURED      |     INSURED      |\n")
        f.write(" " * 13 + "|--------+------------------+------------------+------------------+------------------+------------------|\n")
        
        # Data rows
        row_count = 0
        total_rows = len(pivot_table)
        
        for row in pivot_table.iter_rows(named=True):
            branch = str(row.get('BRANCH', '0'))
            
            # Format values - empty cells show dots
            values = []
            for col in PRODUCT_CATEGORIES:
                val = row.get(col, 0)
                if val == 0:
                    values.append("                 .")
                else:
                    values.append(f"{val:>18,.2f}")
            
            # Write row - branch as integer (no decimal point)
            f.write(" " * 13 + f"|{branch:<8}|{values[0]}|{values[1]}|{values[2]}|")
            f.write(f"{values[3]}|{values[4]}|\n")
            f.write(" " * 13 + "|--------+------------------+------------------+------------------+------------------+------------------|\n")
            
            row_count += 1
            
            # Page break every 24 rows (as in example)
            if row_count % 24 == 0 and row_count < total_rows:
                # Footer for current page - Use the report date
                f.write("\n" + " " * 45 + "(Continued)\n")
                f.write(" " * 38 + "APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(ISLAMIC)        ")
                f.write(f"{header_date_str}   {row_count//24 + 1}\n")
                f.write("\n" * 2)
                f.write(" " * 13 + "-" * 73 + "\n")
                
                # Header repeated
                f.write(" " * 13 + "|BRANCH  |" + " " * 46 + "PRODUCT" + " " * 46 + "|\n")
                f.write(" " * 13 + "|        |" + "-" * 46 + "+" + "-" * 46 + "|\n")
                f.write(" " * 13 + "|        |      IR070       |      DDMAND      |      DSVING      |      DFIXED      |      FDMAND      |\n")
                f.write(" " * 13 + "|        |------------------+------------------+------------------+------------------+------------------|\n")
                f.write(" " * 13 + "|        |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |\n")
                f.write(" " * 13 + "|        |     INSURED      |     INSURED      |     INSURED      |     INSURED      |     INSURED      |\n")
                f.write(" " * 13 + "|--------+------------------+------------------+------------------+------------------+------------------|\n")
        
        # Grand total row
        total_values = []
        for col in PRODUCT_CATEGORIES:
            total_val = pivot_table.select(pl.col(col).sum()).row(0)[0]
            total_values.append(f"{total_val:>18,.2f}")
        
        f.write(" " * 13 + f"|TOTAL   |{total_values[0]}|{total_values[1]}|{total_values[2]}|")
        f.write(f"{total_values[3]}|{total_values[4]}|\n")
        f.write(" " * 13 + "-" * 73 + "\n")
    
    logger.info(f"Islamic TXT report saved to: {txt_file}")
    
    # Also save Parquet for data analysis
    pivot_table.write_parquet(output_path / "EIIQFISF_ISLAMIC_PIVOT.parquet")
    summary_df.write_parquet(output_path / "EIIQFISF_ISLAMIC_SUMMARY.parquet")
    
    logger.info(f"Parquet files saved to: {output_path}")

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
        
        # Generate summary - group by BRANCH and PRODCD_FORMATTED
        summary = rpt_base.group_by(['BRANCH', 'PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ])
        
        # Convert BRANCH to string for consistent concatenation
        summary = summary.with_columns([
            pl.col('BRANCH').cast(pl.String).alias('BRANCH')
        ])
        
        # Calculate total - BRANCH is already string 'TOTAL'
        total_summary = rpt_base.group_by(['PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ]).with_columns([
            pl.lit('TOTAL').cast(pl.String).alias('BRANCH')
        ])
        
        # Now both have BRANCH as String type, so they can be concatenated
        final_summary = pl.concat([summary, total_summary], how="diagonal")
        
        generate_islamic_txt_report(final_summary, output_path, reptdate)
        
        logger.info("ISLAMIC PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error in Islamic processing: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
