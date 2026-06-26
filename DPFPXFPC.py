import polars as pl
import duckdb
from pathlib import Path
import datetime
import pandas as pd
import pyreadstat  # pip install pyreadstat

# Configuration
pidmfin_path = Path("PIDMFIN")
deposit1_path = Path("DEPOSIT1")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# PROC FORMAT equivalent - Create mapping dictionaries
JOINFMT = {
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
}

PRODFMD = {
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
}

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

PRODFMI = {
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

print("Formats created: JOINFMT, PRODFMD, PRODBRH, PRODFMI")

# Function to read SAS file using pyreadstat and convert to Polars DataFrame
def read_sas_to_polars(filepath):
    """Read a SAS .sas7bdat file using pyreadstat and return as Polars DataFrame"""
    try:
        # Read SAS file with pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        return pl.from_pandas(df)
    except FileNotFoundError:
        print(f"NOTE: {filepath} not found")
        return pl.DataFrame()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return pl.DataFrame()

# OPTIONS equivalent
# YEARCUTOFF=1950 - handled by Python's datetime
# COMPRESS=YES - handled by Parquet compression

# DATA REPTDATE (KEEP=REPTDATE);
today = datetime.date.today()
date_string = f"0101{today.year}"  # Fixed '0101' + current year
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# IF MONTH(TODAY()) > 6 THEN REPTDATE = TODAY();
if today.month > 6:
    reptdate = today

SDESC = 'PUBLIC BANK BERHAD'

# CALL SYMPUT equivalent
REPTMON = f"{reptdate.month:02d}"
REPTYEAR = reptdate.strftime('%y')  # YEAR2.

print(f"REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
print(f"SDESC: {SDESC}")

# Create REPTDATE DataFrame
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

# DATA TRUST(KEEP=BRANCH PRODCD INSURED RENAME=(INSURED=INSUREBR));
try:
    # Read SAS file using pyreadstat
    cisdepxn_df = read_sas_to_polars(pidmfin_path / "CISDEPXN.sas7bdat")
    
    if not cisdepxn_df.is_empty():
        trust_df = cisdepxn_df.filter(
            (pl.col('ACCTYPE2').is_in([3, 7])) & 
            (pl.col('BENEINT').is_not_null())
        ).select([
            'BRANCH', 'PRODCD', 'INSURED'
        ]).rename({'INSURED': 'INSUREBR'})
        
        trust_df.write_parquet(output_path / "TRUST.parquet")
        trust_df.write_csv(output_path / "TRUST.csv")
        print(f"TRUST records: {trust_df.height}")
    else:
        trust_df = pl.DataFrame()
        print("NOTE: PIDMFIN.CISDEPXN is empty")
        
except FileNotFoundError:
    print("NOTE: PIDMFIN.CISDEPXN.sas7bdat not found")
    trust_df = pl.DataFrame()

# DATA RPT_BASE; SET TRUST DEPOSIT1.CISDEPD;
try:
    # Read SAS file using pyreadstat
    cisdepd_df = read_sas_to_polars(deposit1_path / "CISDEPD.sas7bdat")
    if cisdepd_df.is_empty():
        print("NOTE: DEPOSIT1.CISDEPD is empty")
except FileNotFoundError:
    print("NOTE: DEPOSIT1.CISDEPD.sas7bdat not found")
    cisdepd_df = pl.DataFrame()

# Combine datasets
if not trust_df.is_empty() or not cisdepd_df.is_empty():
    rpt_base = pl.concat([trust_df, cisdepd_df], how="diagonal")
    rpt_base.write_parquet(output_path / "RPT_BASE.parquet")
    rpt_base.write_csv(output_path / "RPT_BASE.csv")
    print(f"RPT_BASE records: {rpt_base.height}")
else:
    rpt_base = pl.DataFrame()
    print("No data for RPT_BASE")

# TITLE1 'APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(CONVENTIONAL)';
print("\n" + "="*80)
print("APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (CONVENTIONAL)")
print("="*80)

# PROC TABULATE equivalent
if not rpt_base.is_empty():
    # Apply PRODBRH format to PRODCD
    rpt_formatted = rpt_base.with_columns([
        pl.col('PRODCD').map_dict(PRODBRH).alias('PRODCD_FORMATTED')
    ])
    
    # Group by BRANCH and PRODCD_FORMATTED, sum INSUREBR
    summary = rpt_formatted.group_by(['BRANCH', 'PRODCD_FORMATTED']).agg([
        pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
    ]).sort(['BRANCH', 'PRODCD_FORMATTED'])
    
    # Calculate total across all branches
    total_summary = rpt_formatted.group_by(['PRODCD_FORMATTED']).agg([
        pl.col('INSUREBR').sum().alias('INSUREBR_SUM')
    ]).with_columns([
        pl.lit('TOTAL').alias('BRANCH')
    ])
    
    # Combine branch details with total
    final_summary = pl.concat([summary, total_summary], how="diagonal")
    
    # Create pivot table for tabular display (similar to PROC TABULATE)
    # Get unique product categories for columns
    product_categories = final_summary['PRODCD_FORMATTED'].unique().sort()
    
    # Create pivot table
    pivot_table = final_summary.pivot(
        index='BRANCH',
        columns='PRODCD_FORMATTED', 
        values='INSUREBR_SUM',
        aggregate_function='sum'
    ).fill_null(0)
    
    # Format numbers with commas (similar to F=COMMA18.2)
    formatted_table = pivot_table.clone()
    for col in formatted_table.columns:
        if col != 'BRANCH' and formatted_table[col].dtype in [pl.Float64, pl.Int64]:
            formatted_table = formatted_table.with_columns([
                pl.col(col).map_elements(lambda x: f"{x:,.2f}" if x is not None else "0.00", return_dtype=pl.String).alias(col)
            ])
    
    # Display the table with proper formatting
    print("\nBRANCH vs PRODUCT - AMOUNT TO BE INSURED")
    print("-" * 100)
    
    # Print header
    header = "BRANCH".ljust(15)
    for product in product_categories:
        header += f"{product:<20}"
    print(header)
    print("-" * 100)
    
    # Print rows
    for row in formatted_table.iter_rows(named=True):
        line = f"{row['BRANCH']:<15}"
        for product in product_categories:
            value = row.get(product, "0.00")
            line += f"{value:<20}"
        print(line)
    
    # Calculate and display grand total
    grand_total = rpt_formatted.select(pl.col('INSUREBR').sum()).row(0)[0]
    print("-" * 100)
    print(f"{'GRAND TOTAL:':<15}{grand_total:,.2f}")
    
    # Save detailed results for reporting
    final_summary.write_csv(output_path / "CONVENTIONAL_INSURANCE_APPORTIONMENT.csv")
    final_summary.write_parquet(output_path / "CONVENTIONAL_INSURANCE_APPORTIONMENT.parquet")
    
    # Also save the pivot table
    pivot_table.write_csv(output_path / "CONVENTIONAL_PIVOT_TABLE.csv")
    
    print(f"\nDetailed results saved to: {output_path / 'CONVENTIONAL_INSURANCE_APPORTIONMENT.csv'}")
    
else:
    print("No data available for tabulation")

print("\nPROCESSING COMPLETED SUCCESSFULLY")
