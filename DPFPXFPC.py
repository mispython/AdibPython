#!/usr/bin/env python3
"""
File Name: EIPWRDAL
RDAL PBCS Data Processing
Processes banking data and generates RDAL and NSRS output files
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
from pathlib import Path
import sys
import logging
import gc
import os

# Import PBBMRDLF reference data - try different ways
try:
    # Try direct import from current directory
    from PBBMRDLF import df as PBBMRDLF_df
    PBBMRDLF = PBBMRDLF_df
    print(f"Successfully imported PBBMRDLF with {len(PBBMRDLF)} records")
except ImportError:
    try:
        # Try importing from the file path
        import importlib.util
        spec = importlib.util.spec_from_file_location("PBBMRDLF", "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/PBBMRDLF.py")
        if spec and spec.loader:
            pbbmrdlf_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(pbbmrdlf_module)
            PBBMRDLF = pbbmrdlf_module.df
            print(f"Successfully imported PBBMRDLF from path with {len(PBBMRDLF)} records")
        else:
            PBBMRDLF = None
            print("Warning: PBBMRDLF.py not found. Creating empty reference data.")
    except Exception as e:
        print(f"Warning: Could not import PBBMRDLF: {e}")
        PBBMRDLF = None

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# Input paths
INPUT_BASE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod")
LOAN_PATH = INPUT_BASE_PATH / "EIPWRDAL"
PBCS_PATH = INPUT_BASE_PATH / "EIPWRDAL"
BNM_BASE_PATH = INPUT_BASE_PATH / "EIPWRDAL"

# Output paths
OUTPUT_BASE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPWRDAL")
RDAL_OUTPUT = OUTPUT_BASE_PATH / "rdal_pbcs.txt"
NSRS_OUTPUT = OUTPUT_BASE_PATH / "nsrs_rdal_pbcs.txt"

# Ensure output directory exists
OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS
# ============================================================================

def calculate_report_dates():
    """
    Calculate reporting dates based on current date
    Mimics SAS REPTDATE logic
    """
    today = datetime.now()

    # First day of current month
    first_of_month = datetime(today.year, today.month, 1)

    # Last day of previous month
    reptdate = first_of_month - timedelta(days=1)

    day = reptdate.day
    month = reptdate.month
    year = reptdate.year

    # Determine week and start day based on day of month
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:
        sdd = 23
        wk = '4'
        wk1 = '3'

    mm = month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate = datetime(year, mm, sdd)

    return {
        'reptdate': reptdate,
        'nowk': wk,
        'nowk1': wk1,
        'reptmon': f'{mm:02d}',
        'reptmon1': f'{mm1:02d}',
        'reptyear': str(year),
        'reptday': f'{day:02d}',
        'rdate': reptdate.strftime('%d%m%Y'),
        'fdate': reptdate.strftime('%d%m%Y'),
        'sdate': sdate.strftime('%d%m%Y'),
        'sdesc': 'PUBLIC BANK BERHAD'
    }


# Calculate dates
dates = calculate_report_dates()
logger.info(f"Processing for date: {dates['rdate']}")
logger.info(f"Report year: {dates['reptyear']}, Month: {dates['reptmon']}, Week: {dates['nowk']}")


# ============================================================================
# DATA LOADING
# ============================================================================

def load_pbbrdal_data():
    """
    Load PBBRDAL reference data from PBBMRDLF
    """
    if PBBMRDLF is not None:
        try:
            logger.info("Loading PBBRDAL reference data from PBBMRDLF")
            df_ref = PBBMRDLF
            logger.info(f"PBBRDAL data loaded: {len(df_ref)} rows")
            logger.info(f"PBBRDAL columns: {df_ref.columns}")
            return df_ref
        except Exception as e:
            logger.error(f"Error loading PBBMRDLF: {e}")
            return pl.DataFrame({
                'ITCODE': pl.Series([], dtype=pl.Utf8),
                'AMTIND': pl.Series([], dtype=pl.Utf8),
                'AMOUNT': pl.Series([], dtype=pl.Float64)
            })
    else:
        logger.warning("PBBMRDLF not available. Creating empty DataFrame.")
        return pl.DataFrame({
            'ITCODE': pl.Series([], dtype=pl.Utf8),
            'AMTIND': pl.Series([], dtype=pl.Utf8),
            'AMOUNT': pl.Series([], dtype=pl.Float64)
        })


def load_alw_data(dates):
    """Load ALW data from BNM and PBCS sources"""
    reptmon = dates['reptmon']
    nowk = dates['nowk']
    reptyear = dates['reptyear']

    # Try different possible file name formats (lowercase as per logs)
    bnm_paths = [
        BNM_BASE_PATH / f"alw{reptmon}{nowk}.sas7bdat",
        BNM_BASE_PATH / f"ALW{reptmon}{nowk}.sas7bdat",
        BNM_BASE_PATH / f"D{reptyear}" / f"ALW{reptmon}{nowk}.sas7bdat",
    ]
    
    pbcs_paths = [
        PBCS_PATH / f"cclw{reptmon}{nowk}.sas7bdat",
        PBCS_PATH / f"CCLW{reptmon}{nowk}.sas7bdat",
    ]

    dfs = []

    # Try BNM files
    for bnm_path in bnm_paths:
        if bnm_path.exists():
            logger.info(f"Loading BNM data from: {bnm_path}")
            try:
                df_bnm, meta_bnm = pyreadstat.read_sas7bdat(bnm_path)
                df_bnm = pl.from_pandas(df_bnm)
                logger.info(f"BNM data loaded: {len(df_bnm)} rows, columns: {df_bnm.columns}")
                dfs.append(df_bnm)
                break
            except Exception as e:
                logger.error(f"Error loading BNM file {bnm_path}: {e}")
    
    # Try PBCS files
    for pbcs_path in pbcs_paths:
        if pbcs_path.exists():
            logger.info(f"Loading PBCS data from: {pbcs_path}")
            try:
                df_pbcs, meta_pbcs = pyreadstat.read_sas7bdat(pbcs_path)
                df_pbcs = pl.from_pandas(df_pbcs)
                logger.info(f"PBCS data loaded: {len(df_pbcs)} rows, columns: {df_pbcs.columns}")
                dfs.append(df_pbcs)
                break
            except Exception as e:
                logger.error(f"Error loading PBCS file {pbcs_path}: {e}")

    if dfs:
        # Standardize column names to uppercase
        combined = pl.concat(dfs)
        if 'itcode' in combined.columns:
            combined = combined.rename({'itcode': 'ITCODE', 'amtind': 'AMTIND', 'amount': 'AMOUNT'})
        logger.info(f"Combined data: {len(combined)} rows")
        return combined
    else:
        logger.error("No ALW data files found!")
        return pl.DataFrame({
            'ITCODE': pl.Series([], dtype=pl.Utf8),
            'AMTIND': pl.Series([], dtype=pl.Utf8),
            'AMOUNT': pl.Series([], dtype=pl.Float64)
        })


def load_loan_data_filtered(limit_rows=50000):
    """
    Load only the records we need from loan data using filtering
    This is more efficient than loading everything
    """
    loan_paths = [
        LOAN_PATH / "lnnote.sas7bdat",
        LOAN_PATH / "LNNOTE.sas7bdat",
    ]

    # Target zip codes for filtering
    cag_zipcodes = [2002, 2013, 3039, 3047, 800003098, 800003114,
                    800004016, 800004022, 800004029, 800040050,
                    800040053, 800050024, 800060024, 800060045,
                    800060081, 80060085]

    for loan_path in loan_paths:
        if loan_path.exists():
            logger.info(f"Loading loan data from: {loan_path}")
            try:
                # Try reading with pandas first with nrows limit (more reliable)
                import pandas as pd
                logger.info(f"Reading first {limit_rows} rows with pandas...")
                df_pd = pd.read_sas(loan_path, nrows=limit_rows)
                df_loan = pl.from_pandas(df_pd)
                logger.info(f"Loan data loaded: {len(df_loan)} rows (limited to {limit_rows})")
                logger.info(f"Loan columns: {df_loan.columns}")
                
                # Try to find the right column names
                zipcode_col = None
                loantype_col = None
                balance_col = None
                
                for col in df_loan.columns:
                    col_upper = col.upper()
                    if col_upper == 'PZIPCODE' or col_upper == 'ZIPCODE':
                        zipcode_col = col
                    elif col_upper == 'LOANTYPE' or col_upper == 'LOAN_TYPE':
                        loantype_col = col
                    elif col_upper == 'BALANCE' or col_upper == 'BAL' or col_upper == 'AMOUNT':
                        balance_col = col
                
                if zipcode_col:
                    logger.info(f"Found zipcode column: {zipcode_col}")
                    # Filter to only the zip codes we need
                    df_loan = df_loan.filter(pl.col(zipcode_col).is_in(cag_zipcodes))
                    logger.info(f"Filtered loan data: {len(df_loan)} rows matching target zip codes")
                else:
                    logger.warning(f"PZIPCODE column not found. Available columns: {df_loan.columns}")
                
                return df_loan
                
            except Exception as e:
                logger.error(f"Error loading loan file {loan_path}: {e}")
                # Fallback to pyreadstat without rows_limit
                try:
                    logger.info("Trying pyreadstat without row limit...")
                    df_loan, meta_loan = pyreadstat.read_sas7bdat(loan_path)
                    df_loan = pl.from_pandas(df_loan)
                    
                    # Limit rows manually
                    if len(df_loan) > limit_rows:
                        df_loan = df_loan.head(limit_rows)
                        logger.info(f"Limited to {limit_rows} rows")
                    
                    logger.info(f"Loan data loaded: {len(df_loan)} rows")
                    return df_loan
                except Exception as e2:
                    logger.error(f"Error with pyreadstat: {e2}")

    logger.warning("Loan file not found")
    return pl.DataFrame({
        'PZIPCODE': pl.Series([], dtype=pl.Int64),
        'LOANTYPE': pl.Series([], dtype=pl.Utf8),
        'BALANCE': pl.Series([], dtype=pl.Float64)
    })


# ============================================================================
# FORMAT MAPPINGS (from PBBLNFMT)
# ============================================================================

def apply_lnprod_format(loantype):
    """Apply LNPROD format - placeholder implementation"""
    return loantype


def apply_lndenom_format(loantype):
    """Apply LNDENOM format - placeholder implementation"""
    return 'D'


# ============================================================================
# DATA PROCESSING
# ============================================================================

# Load PBBRDAL reference data
pbbrdal = load_pbbrdal_data()

# Debug: Check if we have data
if len(pbbrdal) > 0:
    logger.info(f"PBBRDAL data loaded: {len(pbbrdal)} rows")
    logger.info(f"PBBRDAL sample:\n{pbbrdal.head(5)}")
else:
    logger.warning("PBBRDAL data is empty!")

# Process PBBRDAL1 - set amount indicators and zero amounts
if len(pbbrdal) > 0:
    if 'ITCODE' in pbbrdal.columns:
        # Add AMTIND and AMOUNT columns if they don't exist
        if 'AMTIND' not in pbbrdal.columns:
            pbbrdal = pbbrdal.with_columns([
                pl.lit('D').alias('AMTIND')
            ])
        if 'AMOUNT' not in pbbrdal.columns:
            pbbrdal = pbbrdal.with_columns([
                pl.lit(0.0).alias('AMOUNT')
            ])
        
        pbbrdal1 = pbbrdal.with_columns([
            pl.when(pl.col('ITCODE').str.slice(1, 1) == '0')
            .then(pl.lit(' '))
            .otherwise(pl.lit('D'))
            .alias('AMTIND'),
            pl.lit(0.0).alias('AMOUNT')
        ])
    else:
        logger.error("PBBRDAL missing required column: ITCODE")
        pbbrdal1 = pl.DataFrame({
            'ITCODE': pl.Series([], dtype=pl.Utf8),
            'AMTIND': pl.Series([], dtype=pl.Utf8),
            'AMOUNT': pl.Series([], dtype=pl.Float64)
        })
else:
    pbbrdal1 = pl.DataFrame({
        'ITCODE': pl.Series([], dtype=pl.Utf8),
        'AMTIND': pl.Series([], dtype=pl.Utf8),
        'AMOUNT': pl.Series([], dtype=pl.Float64)
    })

# Load ALW data
alw = load_alw_data(dates)

# Debug: Check ALW data
if len(alw) > 0:
    logger.info(f"ALW data loaded: {len(alw)} rows")
    logger.info(f"ALW columns: {alw.columns}")
    logger.info(f"ALW sample:\n{alw.head(5)}")
else:
    logger.warning("ALW data is empty!")

# Merge ALW and PBBRDAL1
if len(alw) > 0 and len(pbbrdal1) > 0:
    logger.info("Merging ALW and PBBRDAL1 data")
    
    alw_cols = alw.columns
    pbbrdal_cols = pbbrdal1.columns
    
    if 'itcode' in alw_cols and 'ITCODE' not in alw_cols:
        logger.info("Converting column names to uppercase")
        alw = alw.rename({col: col.upper() for col in alw_cols})
        pbbrdal1 = pbbrdal1.rename({col: col.upper() for col in pbbrdal_cols})
    
    rdal = alw.join(
        pbbrdal1,
        on=['ITCODE', 'AMTIND'],
        how='outer',
        suffix='_pbb'
    ).with_columns([
        pl.coalesce(['AMOUNT', 'AMOUNT_pbb', pl.lit(0.0)]).alias('AMOUNT')
    ]).select(['ITCODE', 'AMTIND', 'AMOUNT'])
    
    logger.info(f"After merge: {len(rdal)} rows")
elif len(alw) > 0:
    logger.info("Using ALW data only")
    rdal = alw
elif len(pbbrdal1) > 0:
    logger.info("Using PBBRDAL1 data only")
    rdal = pbbrdal1
else:
    logger.error("No data available for RDAL processing!")
    rdal = pl.DataFrame({
        'ITCODE': pl.Series([], dtype=pl.Utf8),
        'AMTIND': pl.Series([], dtype=pl.Utf8),
        'AMOUNT': pl.Series([], dtype=pl.Float64)
    })

# Free up memory
del alw, pbbrdal, pbbrdal1
gc.collect()

# Debug: Check RDAL data
logger.info(f"RDAL before filtering: {len(rdal)} rows")
if len(rdal) > 0:
    logger.info(f"RDAL sample:\n{rdal.head(5)}")

# Remove unwanted items
rdal = rdal.filter(
    ~(
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30221'), pl.lit('30228'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30231'), pl.lit('30238'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30091'), pl.lit('30098'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('40151'), pl.lit('40158'))) |
        (pl.col('ITCODE').str.slice(0, 5) == 'NSSTS')
    )
)

logger.info(f"RDAL after filtering unwanted: {len(rdal)} rows")


# ============================================================================
# CAG PROCESSING (Loan Data) - Optimized
# ============================================================================

# Load only the loan data we need (filtered and limited)
logger.info("Loading loan data with filtering...")
loan_data = load_loan_data_filtered(limit_rows=50000)

if len(loan_data) > 0:
    logger.info("Processing CAG loan data")
    logger.info(f"Loan data columns: {loan_data.columns}")
    
    # Find column names
    zipcode_col = None
    loantype_col = None
    balance_col = None
    
    for col in loan_data.columns:
        col_upper = col.upper()
        if col_upper == 'PZIPCODE' or col_upper == 'ZIPCODE':
            zipcode_col = col
        elif col_upper == 'LOANTYPE' or col_upper == 'LOAN_TYPE':
            loantype_col = col
        elif col_upper == 'BALANCE' or col_upper == 'BAL' or col_upper == 'AMOUNT':
            balance_col = col
    
    if zipcode_col and loantype_col and balance_col:
        logger.info(f"Found columns - ZIP: {zipcode_col}, LOAN: {loantype_col}, BAL: {balance_col}")
        
        # Rename to standard names
        loan_data = loan_data.rename({
            zipcode_col: 'PZIPCODE',
            loantype_col: 'LOANTYPE',
            balance_col: 'BALANCE'
        })
        
        # Filter for specific zip codes (already filtered, but double-check)
        cag_zipcodes = [2002, 2013, 3039, 3047, 800003098, 800003114,
                        800004016, 800004022, 800004029, 800040050,
                        800040053, 800050024, 800060024, 800060045,
                        800060081, 80060085]

        cag = loan_data.filter(pl.col('PZIPCODE').is_in(cag_zipcodes))
        logger.info(f"CAG filtered data: {len(cag)} rows")

        if len(cag) > 0:
            # Apply formats and set ITCODE
            cag = cag.with_columns([
                pl.col('LOANTYPE').map_elements(apply_lnprod_format, return_dtype=pl.Utf8).alias('PRODCD'),
                pl.col('LOANTYPE').map_elements(apply_lndenom_format, return_dtype=pl.Utf8).alias('AMTIND'),
                pl.lit('7511100000000Y').alias('ITCODE')
            ])

            # Summarize by ITCODE and AMTIND
            cag_summary = cag.group_by(['ITCODE', 'AMTIND']).agg([
                pl.col('BALANCE').sum().alias('AMOUNT')
            ])

            # Combine with RDAL
            rdal = pl.concat([rdal, cag_summary])
            logger.info(f"RDAL after CAG processing: {len(rdal)} rows")
            
            # Free memory
            del cag, cag_summary, loan_data
            gc.collect()
        else:
            logger.warning("No CAG records found for the specified zip codes")
    else:
        logger.warning(f"Loan data missing required columns. Found: {loan_data.columns}")
else:
    logger.info("No loan data available")

# Remove specific item codes
rdal = rdal.filter(pl.col('ITCODE') != '4364008110000Y')

# Apply absolute value except for specific item
rdal = rdal.with_columns([
    pl.when(pl.col('ITCODE') != '3400061006120Y')
    .then(pl.col('AMOUNT').abs())
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
])

logger.info(f"RDAL final row count: {len(rdal)}")


# ============================================================================
# SPLIT DATA INTO AL, OB, SP
# ============================================================================

# Filter out F and # records for initial split
rdal_filtered = rdal.filter(
    ~pl.col('ITCODE').str.slice(13, 1).is_in(['F', '#'])
)

logger.info(f"RDAL filtered (no F/#): {len(rdal_filtered)} rows")

if len(rdal_filtered) == 0:
    logger.warning("No data after filtering! Check if ITCODE has proper length or data format.")
    if len(rdal) > 0:
        logger.info(f"RDAL ITCODE samples: {rdal['ITCODE'].head(10).to_list()}")
        logger.info(f"RDAL ITCODE lengths: {rdal['ITCODE'].str.len_chars().head(10).to_list()}")

# Split into AL, OB, SP based on conditions
al_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') &
    ~(pl.col('ITCODE').str.slice(0, 3) == '307') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40190') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40191') &
    ~(pl.col('ITCODE').str.slice(0, 4) == 'SSTS') &
    (pl.col('ITCODE').str.slice(0, 1) != '5') &
    ~(pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))
)

ob_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') &
    (pl.col('ITCODE').str.slice(0, 1) == '5')
)

# SP data - complex conditions
sp_conditions = (
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3) == '307')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40190')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40191')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))) |
        (pl.col('ITCODE').str.slice(1, 1) == '0')
)

sp_data = rdal_filtered.filter(sp_conditions)

# Handle SSTS special case
ssts_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
).with_columns([
    pl.lit('4017000000000Y').alias('ITCODE')
])

if len(ssts_data) > 0:
    sp_data = pl.concat([sp_data, ssts_data])

logger.info(f"AL data rows: {len(al_data)}")
logger.info(f"OB data rows: {len(ob_data)}")
logger.info(f"SP data rows: {len(sp_data)}")


# ============================================================================
# WRITE RDAL OUTPUT FILE
# ============================================================================

def write_rdal_file(al_data, ob_data, sp_data, dates, output_path):
    """Write RDAL output file with proper formatting"""

    with open(output_path, 'w') as f:
        # Write header
        phead = f"RDAL{dates['reptday']}{dates['reptmon']}{dates['reptyear']}"
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")

        if len(al_data) > 0:
            # Sort and aggregate AL data
            al_sorted = al_data.sort(['ITCODE', 'AMTIND'])

            # Group by ITCODE and aggregate
            al_grouped = al_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'I')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTI'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in al_grouped.iter_rows(named=True):
                itcode = row['ITCODE']

                # Skip certain items on specific days
                proceed = True
                if dates['reptday'] in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[0:2] in ['68', '78']:
                        proceed = False
                if itcode == '4966000000000F':
                    proceed = False

                if proceed:
                    amountd = round(row['AMOUNTD'] / 1000)
                    amounti = round(row['AMOUNTI'] / 1000)
                    amountf = round(row['AMOUNTF'] / 1000)
                    amountd_total = amountd + amounti + amountf

                    f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")
        else:
            logger.warning("No AL data to write")

        # Write OB section
        f.write("OB\n")

        if len(ob_data) > 0:
            ob_sorted = ob_data.sort(['ITCODE', 'AMTIND'])

            ob_grouped = ob_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'I')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTI'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in ob_grouped.iter_rows(named=True):
                amountd = round(row['AMOUNTD'] / 1000)
                amounti = round(row['AMOUNTI'] / 1000)
                amountf = round(row['AMOUNTF'] / 1000)
                amountd_total = amountd + amounti

                f.write(f"{row['ITCODE']};{amountd_total};{amounti};{amountf}\n")
        else:
            logger.warning("No OB data to write")

        # Write SP section
        f.write("SP\n")

        if len(sp_data) > 0:
            sp_sorted = sp_data.sort('ITCODE')

            sp_grouped = sp_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in sp_grouped.iter_rows(named=True):
                amountd = round(row['AMOUNTD'] / 1000)
                amountf = round(row['AMOUNTF'] / 1000)
                amountd_total = amountd + amountf

                f.write(f"{row['ITCODE']};{amountd_total};{amountf}\n")
        else:
            logger.warning("No SP data to write")


# Write first RDAL file
write_rdal_file(al_data, ob_data, sp_data, dates, RDAL_OUTPUT)
logger.info(f"RDAL file written to: {RDAL_OUTPUT}")


# ============================================================================
# PROCESS DATA FOR NSRS (Second Processing)
# ============================================================================

# Handle # records by converting to Y and negating amount
rdal_processed = rdal.with_columns([
    pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
    .then(
        pl.col('ITCODE').str.slice(0, 13) + 'Y'
    )
    .otherwise(pl.col('ITCODE'))
    .alias('ITCODE'),
    pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
    .then(pl.col('AMOUNT') * -1)
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
])

# Re-aggregate after the transformation
rdal_agg = rdal_processed.group_by(['ITCODE', 'AMTIND']).agg([
    pl.col('AMOUNT').sum()
])

# Re-split into AL, OB, SP for NSRS
rdal_filtered2 = rdal_agg

al_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') &
    ~(pl.col('ITCODE').str.slice(0, 3) == '307') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40190') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40191') &
    ~(pl.col('ITCODE').str.slice(0, 4) == 'SSTS') &
    (pl.col('ITCODE').str.slice(0, 1) != '5') &
    ~(pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))
)

ob_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') &
    (pl.col('ITCODE').str.slice(0, 1) == '5')
)

sp_conditions2 = (
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3) == '307')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40190')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40191')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))) |
        (pl.col('ITCODE').str.slice(1, 1) == '0')
)

sp_data2 = rdal_filtered2.filter(sp_conditions2)

ssts_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
).with_columns([
    pl.lit('4017000000000Y').alias('ITCODE')
])

if len(ssts_data2) > 0:
    sp_data2 = pl.concat([sp_data2, ssts_data2])


# ============================================================================
# WRITE NSRS OUTPUT FILE
# ============================================================================

def write_nsrs_file(al_data, ob_data, sp_data, dates, output_path):
    """Write NSRS output file with proper formatting"""

    with open(output_path, 'w') as f:
        # Write header
        phead = f"RDAL{dates['reptday']}{dates['reptmon']}{dates['reptyear']}"
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")

        if len(al_data) > 0:
            al_sorted = al_data.sort(['ITCODE', 'AMTIND'])

            al_grouped = al_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'I')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTI'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in al_grouped.iter_rows(named=True):
                itcode = row['ITCODE']

                # Skip certain items on specific days
                proceed = True
                if dates['reptday'] in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[0:2] in ['68', '78']:
                        proceed = False

                if proceed:
                    amountd_raw = row['AMOUNTD']
                    amounti_raw = row['AMOUNTI']
                    amountf_raw = row['AMOUNTF']

                    # Scale down if ITCODE starts with '80'
                    if itcode[0:2] == '80':
                        amountd = round(amountd_raw / 1000)
                        amounti = round(amounti_raw / 1000)
                        amountf = round(amountf_raw / 1000)
                    else:
                        amountd = round(amountd_raw)
                        amounti = round(amounti_raw)
                        amountf = round(amountf_raw)

                    amountd_total = amountd + amounti + amountf

                    f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")
        else:
            logger.warning("No AL data for NSRS")

        # Write OB section
        f.write("OB\n")

        if len(ob_data) > 0:
            ob_sorted = ob_data.sort(['ITCODE', 'AMTIND'])

            ob_grouped = ob_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'I')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTI'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in ob_grouped.iter_rows(named=True):
                itcode = row['ITCODE']

                amountd_raw = row['AMOUNTD']
                amounti_raw = row['AMOUNTI']
                amountf_raw = row['AMOUNTF']

                # Scale down if ITCODE starts with '80'
                if itcode[0:2] == '80':
                    amountd = round(amountd_raw / 1000)
                    amounti = round(amounti_raw / 1000)
                    amountf = round(amountf_raw / 1000)
                else:
                    amountd = round(amountd_raw)
                    amounti = round(amounti_raw)
                    amountf = round(amountf_raw)

                amountd_total = amountd + amounti

                f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")
        else:
            logger.warning("No OB data for NSRS")

        # Write SP section
        f.write("SP\n")

        if len(sp_data) > 0:
            sp_sorted = sp_data.sort('ITCODE')

            sp_grouped = sp_sorted.group_by('ITCODE', maintain_order=True).agg([
                pl.when(pl.col('AMTIND') == 'D')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTD'),
                pl.when(pl.col('AMTIND') == 'F')
                .then(pl.col('AMOUNT'))
                .otherwise(0.0)
                .sum()
                .alias('AMOUNTF')
            ])

            for row in sp_grouped.iter_rows(named=True):
                itcode = row['ITCODE']

                amountd_raw = row['AMOUNTD']
                amountf_raw = row['AMOUNTF']

                amountd_total = amountd_raw + amountf_raw

                # Scale down if ITCODE starts with '80'
                if itcode[0:2] == '80':
                    amountd = round(amountd_total / 1000)
                else:
                    amountd = round(amountd_total)

                amountf = round(amountf_raw)

                f.write(f"{itcode};{amountd};{amountf}\n")
        else:
            logger.warning("No SP data for NSRS")


# Write NSRS file
write_nsrs_file(al_data2, ob_data2, sp_data2, dates, NSRS_OUTPUT)
logger.info(f"NSRS file written to: {NSRS_OUTPUT}")

print("\n" + "=" * 70)
print("Processing complete!")
print("=" * 70)
print(f"Output files:")
print(f"  - RDAL: {RDAL_OUTPUT}")
print(f"  - NSRS: {NSRS_OUTPUT}")
