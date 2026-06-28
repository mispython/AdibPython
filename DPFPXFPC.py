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
        logging.FileHandler('eibqfar2_processing.log'),
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

# Define product categories in order
PRODUCT_CATEGORIES = ['DDMAND', 'DSVING', 'DFIXED', 'FDMAND', 'FFIXED', 'DEBIT CARD (E)']

logger.info("Format mappings created successfully")

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

def process_trust_data(pidmfin_path: Path) -> pl.DataFrame:
    """Process TRUST data from CISDEPXN"""
    logger.info("Processing TRUST data from cisdepxn.sas7bdat")
    cisdepxn_df = read_sas_to_polars(pidmfin_path / "cisdepxn.sas7bdat")
    
    if cisdepxn_df.is_empty():
        logger.warning("PIDMFIN.cisdepxn is empty or not found")
        return pl.DataFrame()
    
    required_cols = ['ACCTYPE2', 'BENEINT', 'BRANCH', 'PRODCD', 'INSURED']
    col_mapping = {col.lower(): col for col in cisdepxn_df.columns}
    missing_cols = []
    for col in required_cols:
        if col.lower() not in col_mapping:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"Missing columns in cisdepxn: {missing_cols}")
        return pl.DataFrame()
    
    rename_dict = {}
    for col in required_cols:
        actual_col = col_mapping[col.lower()]
        if actual_col != col:
            rename_dict[actual_col] = col
    
    if rename_dict:
        cisdepxn_df = cisdepxn_df.rename(rename_dict)
    
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
    cisdepd_df = read_sas_to_polars(deposit1_path / "cisdepd.sas7bdat")
    
    if cisdepd_df.is_empty():
        logger.warning("DEPOSIT1.cisdepd is empty or not found")
        return pl.DataFrame()
    
    required_cols = ['BRANCH', 'PRODCD', 'INSUREBR']
    col_mapping = {col.lower(): col for col in cisdepd_df.columns}
    missing_cols = []
    for col in required_cols:
        if col.lower() not in col_mapping:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"Missing columns in cisdepd: {missing_cols}")
        return pl.DataFrame()
    
    rename_dict = {}
    for col in required_cols:
        actual_col = col_mapping[col.lower()]
        if actual_col != col:
            rename_dict[actual_col] = col
    
    if rename_dict:
        cisdepd_df = cisdepd_df.rename(rename_dict)
    
    cisdepd_df = cisdepd_df.select(['BRANCH', 'PRODCD', 'INSUREBR'])
    logger.info(f"DEPOSIT records: {cisdepd_df.height:,}")
    return cisdepd_df

def apply_format_mappings(df: pl.DataFrame) -> pl.DataFrame:
    """Apply PRODBRH format mapping to PRODCD column"""
    if 'PRODCD' in df.columns:
        try:
            df = df.with_columns([
                pl.col('PRODCD').replace(PRODBRH).alias('PRODCD_FORMATTED')
            ])
        except AttributeError:
            df = df.with_columns([
                pl.col('PRODCD').map_elements(
                    lambda x: PRODBRH.get(x, str(x)), 
                    return_dtype=pl.String
                ).alias('PRODCD_FORMATTED')
            ])
    return df

def generate_conventional_txt_report(summary_df: pl.DataFrame, output_path: Path, report_date: datetime.date) -> None:
    """
    Generate TXT report in the exact format from the example
    """
    if summary_df.is_empty():
        logger.warning("No data available for report generation")
        return
    
    # Separate TOTAL from regular branches
    total_rows = summary_df.filter(pl.col('BRANCH') == 'TOTAL')
    regular_rows = summary_df.filter(pl.col('BRANCH') != 'TOTAL')
    
    # Create pivot table for regular rows (excluding TOTAL)
    pivot_table = regular_rows.pivot(
        index='BRANCH',
        on='PRODCD_FORMATTED',
        values='INSUREBR_SUM',
        aggregate_function='sum'
    ).fill_null(0)
    
    # Ensure all product categories exist
    for col in PRODUCT_CATEGORIES:
        if col not in pivot_table.columns:
            pivot_table = pivot_table.with_columns(pl.lit(0.0).alias(col))
    
    # Convert BRANCH to string and handle nulls
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').fill_null('0').cast(pl.String).alias('BRANCH')
    ])
    
    # Sort by BRANCH as integer (numeric order)
    # Create a temporary numeric column for sorting
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').cast(pl.Int64).alias('BRANCH_NUM')
    ]).sort('BRANCH_NUM').drop('BRANCH_NUM')
    
    # Convert BRANCH back to string for display (no decimal point)
    pivot_table = pivot_table.with_columns([
        pl.col('BRANCH').cast(pl.String).alias('BRANCH')
    ])
    
    # Get TOTAL row summary
    if not total_rows.is_empty():
        total_pivot = total_rows.pivot(
            index='BRANCH',
            on='PRODCD_FORMATTED',
            values='INSUREBR_SUM',
            aggregate_function='sum'
        ).fill_null(0)
        
        # Ensure all product categories exist in total
        for col in PRODUCT_CATEGORIES:
            if col not in total_pivot.columns:
                total_pivot = total_pivot.with_columns(pl.lit(0.0).alias(col))
        
        # Add TOTAL row to pivot table
        total_pivot = total_pivot.with_columns([
            pl.col('BRANCH').fill_null('TOTAL').cast(pl.String).alias('BRANCH')
        ])
        
        # Append TOTAL row
        pivot_table = pl.concat([pivot_table, total_pivot], how="diagonal")
    
    # Get current time for header
    now = datetime.datetime.now()
    time_str = now.strftime("%I:%M")
    date_str = report_date.strftime("%A, %B %d, %Y")
    header_date_str = f"{time_str} {date_str}"
    
    # Prepare TXT file
    txt_file = output_path / "EIBQFAR2_CONVENTIONAL_REPORT.txt"
    
    with open(txt_file, 'w') as f:
        # Header - Page 1
        f.write(" " * 35 + "APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(CONVENTIONAL)     ")
        f.write(f"{header_date_str}   1\n")
        f.write("\n" * 2)
        f.write(" " * 60 + "-" * 60 + "\n")
        
        # Main table header
        f.write("|BRANCH  |" + " " * 49 + "PRODUCT" + " " * 49 + "|\n")
        f.write("|        |" + "-" * 49 + "+" + "-" * 49 + "|\n")
        
        # Product columns header - first row
        f.write("|        |      DDMAND      |      DSVING      |      DFIXED      |")
        f.write("      FDMAND      |      FFIXED      |  DEBIT CARD (E)  |\n")
        
        # Product columns header - second row (subheaders)
        f.write("|        |------------------+------------------+------------------+")
        f.write("------------------+------------------+------------------|\n")
        f.write("|        |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |")
        f.write("   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |\n")
        f.write("|        |     INSURED      |     INSURED      |     INSURED      |")
        f.write("     INSURED      |     INSURED      |     INSURED      |\n")
        f.write("|--------+------------------+------------------+------------------+")
        f.write("------------------+------------------+------------------|\n")
        
        # Data rows
        row_count = 0
        total_rows_count = len(pivot_table)
        
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
            
            # Write row - branch as string (no decimal point)
            f.write(f"|{branch:<8}|{values[0]}|{values[1]}|{values[2]}|")
            f.write(f"{values[3]}|{values[4]}|{values[5]}|\n")
            f.write("|--------+------------------+------------------+------------------+")
            f.write("------------------+------------------+------------------|\n")
            
            row_count += 1
            
            # Page break every 30 rows (as in example)
            if row_count % 30 == 0 and row_count < total_rows_count:
                # Footer for current page
                f.write("\n" + " " * 60 + "(Continued)\n")
                f.write(" " * 35 + "APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(CONVENTIONAL)     ")
                f.write(f"{header_date_str}   {row_count//30 + 1}\n")
                f.write("\n" * 2)
                f.write(" " * 60 + "-" * 60 + "\n")
                
                # Header repeated
                f.write("|BRANCH  |" + " " * 49 + "PRODUCT" + " " * 49 + "|\n")
                f.write("|        |" + "-" * 49 + "+" + "-" * 49 + "|\n")
                f.write("|        |      DDMAND      |      DSVING      |      DFIXED      |")
                f.write("      FDMAND      |      FFIXED      |  DEBIT CARD (E)  |\n")
                f.write("|        |------------------+------------------+------------------+")
                f.write("------------------+------------------+------------------|\n")
                f.write("|        |   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |")
                f.write("   AMOUNT TO BE   |   AMOUNT TO BE   |   AMOUNT TO BE   |\n")
                f.write("|        |     INSURED      |     INSURED      |     INSURED      |")
                f.write("     INSURED      |     INSURED      |     INSURED      |\n")
                f.write("|--------+------------------+------------------+------------------+")
                f.write("------------------+------------------+------------------|\n")
    
    logger.info(f"TXT report saved to: {txt_file}")
    
    # Also save Parquet for data analysis
    pivot_table.write_parquet(output_path / "EIBQFAR2_CONVENTIONAL_PIVOT.parquet")
    summary_df.write_parquet(output_path / "EIBQFAR2_CONVENTIONAL_SUMMARY.parquet")
    
    logger.info(f"Parquet files saved to: {output_path}")

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
        
        # Generate summary
        summary = rpt_base.group_by(['BRANCH', 'PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ])
        
        # Convert BRANCH to string for consistent concatenation
        summary = summary.with_columns([
            pl.col('BRANCH').cast(pl.String).alias('BRANCH')
        ])
        
        # Calculate total
        total_summary = rpt_base.group_by(['PRODCD_FORMATTED']).agg([
            pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
        ]).with_columns([
            pl.lit('TOTAL').cast(pl.String).alias('BRANCH')
        ])
        
        # Combine
        final_summary = pl.concat([summary, total_summary], how="diagonal")
        
        # Generate TXT report
        generate_conventional_txt_report(final_summary, output_path, reptdate)
        
        logger.info("PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
