import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import logging

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

# Configuration
pidmfin_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
deposit1_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2")
output_path.mkdir(exist_ok=True, parents=True)

# PROC FORMAT equivalent - Create mapping dictionaries
PRODBRH = {
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
}

logger.info("Format mappings created successfully")

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

def process_trust_data(pidmfin_path: Path) -> pl.DataFrame:
    """Process TRUST data from CISDEPXN"""
    logger.info("Processing TRUST data from cisdepxn.sas7bdat")
    
    # Read SAS file
    cisdepxn_df = read_sas_to_polars(pidmfin_path / "cisdepxn.sas7bdat")
    
    if cisdepxn_df.is_empty():
        logger.warning("PIDMFIN.cisdepxn is empty or not found - skipping TRUST data")
        return pl.DataFrame()
    
    # Check if required columns exist (case insensitive)
    required_cols = ['ACCTYPE2', 'BENEINT', 'BRANCH', 'PRODCD', 'INSURED']
    
    # Create a mapping of lowercase column names to actual column names
    col_mapping = {col.lower(): col for col in cisdepxn_df.columns}
    
    # Check if all required columns exist (case insensitive)
    missing_cols = []
    for col in required_cols:
        if col.lower() not in col_mapping:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"Missing columns in cisdepxn: {missing_cols}")
        logger.info(f"Available columns: {list(cisdepxn_df.columns)[:10]}...")
        return pl.DataFrame()
    
    # Rename columns to uppercase for consistency
    rename_dict = {}
    for col in required_cols:
        actual_col = col_mapping[col.lower()]
        if actual_col != col:
            rename_dict[actual_col] = col
    
    if rename_dict:
        cisdepxn_df = cisdepxn_df.rename(rename_dict)
    
    # Filter and select required columns
    trust_df = cisdepxn_df.filter(
        (pl.col('ACCTYPE2').is_in([3, 7])) & 
        (pl.col('BENEINT').is_not_null())
    ).select([
        'BRANCH', 'PRODCD', 'INSURED'
    ]).rename({'INSURED': 'INSUREBR'})
    
    logger.info(f"TRUST records: {trust_df.height:,}")
    return trust_df

def process_deposit_data(deposit1_path: Path) -> pl.DataFrame:
    """Process deposit data from CISDEPD"""
    logger.info("Processing deposit data from cisdepd.sas7bdat")
    
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
    
    # Select required columns
    cisdepd_df = cisdepd_df.select(['BRANCH', 'PRODCD', 'INSUREBR'])
    
    logger.info(f"DEPOSIT records: {cisdepd_df.height:,}")
    return cisdepd_df

def apply_format_mappings(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply PRODBRH format mapping to PRODCD column
    """
    if 'PRODCD' in df.columns:
        try:
            logger.info("Applying format mappings using replace() method")
            df = df.with_columns([
                pl.col('PRODCD').replace(PRODBRH).alias('PRODCD_FORMATTED')
            ])
        except AttributeError:
            logger.info("Falling back to map_elements() method")
            df = df.with_columns([
                pl.col('PRODCD').map_elements(
                    lambda x: PRODBRH.get(x, str(x)), 
                    return_dtype=pl.String
                ).alias('PRODCD_FORMATTED')
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
    print("APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (CONVENTIONAL)")
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
        logger.info(f"REPTDATE saved to {output_path}")
        
        # Process data sources
        trust_df = process_trust_data(pidmfin_path)
        deposit_df = process_deposit_data(deposit1_path)
        
        # Combine datasets
        dataframes = [df for df in [trust_df, deposit_df] if not df.is_empty()]
        
        if not dataframes:
            logger.warning("No data available for processing")
            return
        
        rpt_base = pl.concat(dataframes, how="diagonal")
        
        # Apply format mappings
        rpt_base = apply_format_mappings(rpt_base)
        
        # Save base dataset
        rpt_base.write_parquet(output_path / "RPT_BASE.parquet")
        rpt_base.write_csv(output_path / "RPT_BASE.csv")
        
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
        generate_tabular_report(final_summary, output_path)
        
        logger.info("PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
