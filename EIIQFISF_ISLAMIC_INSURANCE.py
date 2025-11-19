import polars as pl
import duckdb
from pathlib import Path
import datetime
import importlib

# Configuration
pidmfin_path = Path("PIDMFIN")
deposit1_path = Path("DEPOSIT1")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# %INC PGM(PBBDPFMT);
try:
    pbbdpfmt = importlib.import_module('PBBDPFMT')
    pbbdpfmt.process()
except ImportError:
    print("NOTE: PBBDPFMT not available")

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
    11: 'JOIN 11',
    12: 'JOIN 12',
    13: 'JOIN 13',
    14: 'JOIN 14'
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
    '46795': 'DEBIT CARD (E)',
    '42XXX': 'ATM/SI (E)',
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

# DATA TRUST(KEEP=BRANCH PRODCD INSURED RENAME=(INSURED=INSUREBR));
try:
    trust_df = pl.read_parquet(pidmfin_path / "CISDEPXN.parquet").filter(
        (pl.col('ACCTYPE2').is_in([3, 7])) & 
        (pl.col('BENEINT').is_not_null())
    ).select([
        'BRANCH', 'PRODCD', 'INSURED'
    ]).rename({'INSURED': 'INSUREBR'})
    
    trust_df.write_parquet(output_path / "TRUST.parquet")
    print(f"TRUST records: {trust_df.height}")
    
except FileNotFoundError:
    print("NOTE: PIDMFIN.CISDEPXN not found")
    trust_df = pl.DataFrame()

# DATA RPT_BASE; SET TRUST DEPOSIT1.CISDEPI;
try:
    cisdepi_df = pl.read_parquet(deposit1_path / "CISDEPI.parquet")
except FileNotFoundError:
    print("NOTE: DEPOSIT1.CISDEPI not found")
    cisdepi_df = pl.DataFrame()

# Combine datasets
if not trust_df.is_empty() or not cisdepi_df.is_empty():
    rpt_base = pl.concat([trust_df, cisdepi_df], how="diagonal")
    rpt_base.write_parquet(output_path / "RPT_BASE.parquet")
    print(f"RPT_BASE records: {rpt_base.height}")
else:
    rpt_base = pl.DataFrame()
    print("No data for RPT_BASE")

# TITLE1 'APPORTIONMENT OF PREMIUN PAID TO MDIC BY BRANCH(ISLAMIC)';
print("\n" + "="*80)
print("APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (ISLAMIC)")
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
    
    # Pivot for tabular display (similar to PROC TABULATE)
    pivot_table = final_summary.pivot(
        index='BRANCH',
        columns='PRODCD_FORMATTED', 
        values='INSUREBR_SUM',
        aggregate_function='sum'
    ).fill_null(0)
    
    # Format numbers with commas (similar to F=COMMA18.2)
    for col in pivot_table.columns:
        if col != 'BRANCH' and pivot_table[col].dtype in [pl.Float64, pl.Int64]:
            pivot_table = pivot_table.with_columns([
                pl.col(col).map_elements(lambda x: f"{x:,.2f}" if x is not None else "0.00").alias(col)
            ])
    
    print("BRANCH vs PRODUCT - AMOUNT TO BE INSURED")
    print("-" * 100)
    print(pivot_table)
    
    # Calculate grand total
    grand_total = rpt_formatted.select(pl.col('INSUREBR').sum()).row(0)[0]
    print(f"\nGRAND TOTAL: {grand_total:,.2f}")
    
    # Save results
    final_summary.write_csv(output_path / "INSURANCE_APPORTIONMENT.csv")
    final_summary.write_parquet(output_path / "INSURANCE_APPORTIONMENT.parquet")
    
else:
    print("No data available for tabulation")

print("\nPROCESSING COMPLETED SUCCESSFULLY")
