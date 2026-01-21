"""
EIYPISIP - PIDM Deposit Report Conversion
Converts SAS program to Python using DuckDB and Polars
Output: Partitioned Parquet files (year=yyyy/month=mm/day=dd)
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_reptdate(reptdate_path: str) -> dict:
    """Read reporting date and extract year, month parameters"""
    df = pl.read_parquet(reptdate_path)
    reptdate = df['REPTDATE'][0]
    
    params = {
        'REPTYEAR': reptdate.strftime('%y'),
        'REPTMON': reptdate.strftime('%m'),
        'NOWK': '4',
        'RDATE': reptdate,
        'REPTDATE': reptdate
    }
    logger.info(f"Report Date: {reptdate}, Year: {params['REPTYEAR']}, Month: {params['REPTMON']}")
    return params


def read_dpmas_file(filepath: str, rdate) -> pl.DataFrame:
    """Read DPMAS fixed-width file and filter records"""
    logger.info(f"Reading DPMAS file: {filepath}")
    
    # Define column specifications (position, width, name, type)
    col_specs = [
        (0, 5, 'CNT', int),
        (6, 7, 'CONTRACT_NO', str),
        (14, 26, 'B_CUST_NAME', str),
        (41, 26, 'A_CUST_NAME', str),
        (68, 20, 'CUST_ID1', str),
        (89, 20, 'CUST_ID2', str),
        (110, 8, 'PFE_ID', str),
        (119, 8, 'TREASURY_ID', str),
        (128, 10, 'TRADING_DATE', str),
        (139, 10, 'START_DATE', str),
        (150, 10, 'MATURITY_DATE', str),
        (161, 10, 'FIXING_DATE', str),
        (172, 3, 'B_CURR_TYPE', str),
        (176, 11, 'B_CURR_ACC_NO', int),
        (188, 13, 'B_CURR_AMOUNT', float),
        (202, 3, 'A_CURR_TYPE', str),
        (206, 11, 'A_CURR_ACC_NO', int),
        (218, 5, 'BRANCH', str),
        (224, 3, 'BRCHABRV', str),
        (228, 3, 'TENURE', int),
        (232, 9, 'INTEREST', float),
        (242, 13, 'STRIKE_RATE', float),
        (256, 13, 'FIXING_RATE', float),
        (270, 15, 'ACCRUAL_INT', float),
        (286, 2, 'DCISTATS', str),
        (289, 1, 'DCICONV', str),
        (291, 1, 'DEAL_CONFIRM', str),
        (293, 1, 'ROLLOVER_IND', str),
        (295, 7, 'RO_PREV_CONTR', str),
        (303, 26, 'CREATE_DATE', str),
        (330, 13, 'CHARGES_AMT', float),
        (344, 13, 'OPTION_PREM', float),
        (358, 8, 'CONV_MAKER', str),
        (367, 8, 'CONV_CHECKER', str),
        (376, 7, 'BUY_TICKET_NO', str),
        (384, 3, 'DEPT_NBR', int),
        (388, 7, 'CONV_CONTRACT', str),
        (396, 9, 'SPOT_RATE', float),
        (406, 1, 'POSTING_STATUS', str),
        (408, 13, 'OPTION_PREM_IB', float),
        (422, 60, 'CONV_ERR_CODE', str)
    ]
    
    # Read fixed-width file (skip first 2 lines)
    with open(filepath, 'r') as f:
        lines = f.readlines()[2:]  # Skip header lines
    
    data = []
    for line in lines:
        row = {}
        for pos, width, name, dtype in col_specs:
            value = line[pos:pos+width].strip()
            if dtype == int:
                row[name] = int(value) if value else 0
            elif dtype == float:
                row[name] = float(value) if value else 0.0
            else:
                row[name] = value
        data.append(row)
    
    df = pl.DataFrame(data)
    
    # Filter records
    df = df.filter(~pl.col('DCISTATS').is_in(['C', 'P', 'M']))
    
    # Parse dates
    df = df.with_columns([
        pl.col('START_DATE').str.strptime(pl.Date, '%Y-%m-%d', strict=False).alias('CHKDATE'),
        pl.col('MATURITY_DATE').str.strptime(pl.Date, '%Y-%m-%d', strict=False).alias('MATDATE')
    ])
    
    # Filter by date
    df = df.filter(
        (pl.col('CHKDATE') <= rdate) & (pl.col('MATDATE') > rdate)
    )
    
    df = df.rename({'B_CURR_ACC_NO': 'ACCNUM'})
    
    logger.info(f"DPMAS records after filtering: {len(df)}")
    return df


def process_deposit_data(deposit_paths: list) -> pl.DataFrame:
    """Process and combine deposit transaction files"""
    logger.info("Processing deposit transaction files")
    
    dfs = []
    for path in deposit_paths:
        if Path(path).exists():
            df = pl.read_parquet(path).select(['ACCTNO', 'KEY', 'JOIN'])
            dfs.append(df)
    
    df = pl.concat(dfs) if dfs else pl.DataFrame()
    df = df.unique(subset=['ACCTNO'])
    df = df.rename({'ACCTNO': 'ACCNUM'})
    
    logger.info(f"Deposit records: {len(df)}")
    return df


def process_pidm_master(pidm_path: str, depocis: pl.DataFrame) -> pl.DataFrame:
    """Process PIDM master data and merge with deposit data"""
    logger.info("Processing PIDM master data")
    
    df = pl.read_parquet(pidm_path)
    df = df.select([
        'ACCTNO', 'SPECIALACC', 'CUSTNAME', 'STAFFCODE', 'ACCTYPE',
        'PRIPHONE', 'SECPHONE', 'MOBIPHON', 'NEWIC', 'NEWICIND'
    ])
    
    df = df.rename({
        'ACCTNO': 'ACCNUM',
        'CUSTNAME': 'ACCNAME',
        'PRIPHONE': 'TELHOME',
        'SECPHONE': 'TELOFFICE',
        'MOBIPHON': 'TELHP',
        'NEWIC': 'REGNUM'
    })
    
    # Merge with deposit data
    df = df.join(depocis, on='ACCNUM', how='inner')
    
    # Process ACCTYPE logic
    df = df.with_columns([
        pl.when((pl.col('KEY') != '') & (pl.col('JOIN') == 1) & (pl.col('ACCTYPE') == 6))
          .then(6)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') == 1))
          .then(1)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') == 0) & (pl.col('ACCTYPE') == 4))
          .then(4)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') == 0) & (pl.col('ACCTYPE') == 5))
          .then(5)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') == 0) & (pl.col('ACCTYPE') == 6))
          .then(6)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') == 0))
          .then(6)
        .when((pl.col('KEY') != '') & (pl.col('JOIN') > 1))
          .then(2)
        .when((pl.col('KEY') == '') & (pl.col('JOIN') == 1) & (pl.col('ACCTYPE') == 6))
          .then(6)
        .when((pl.col('KEY') == '') & (pl.col('JOIN') == 1))
          .then(1)
        .when((pl.col('KEY') == '') & (pl.col('JOIN') == 0) & (pl.col('ACCTYPE').is_in([4, 5, 6])))
          .then(pl.col('ACCTYPE'))
        .when((pl.col('KEY') == '') & (pl.col('JOIN') == 0))
          .then(6)
        .otherwise(pl.col('ACCTYPE'))
        .alias('ACCTYPE2')
    ])
    
    # Additional logic for NEWICIND = 'SA' and ACCTYPE2 = 1
    df = df.with_columns([
        pl.when((pl.col('NEWICIND') == 'SA') & (pl.col('ACCTYPE2') == 1))
          .then(6)
        .otherwise(pl.col('ACCTYPE2'))
        .alias('ACCTYPE2')
    ])
    
    df = df.drop(['ACCTYPE', 'KEY', 'JOIN', 'NEWICIND']).rename({'ACCTYPE2': 'ACCTYPE'})
    
    logger.info(f"PIDM master records: {len(df)}")
    return df


def get_exchange_rates(eqrate_path: str) -> pl.DataFrame:
    """Get exchange rates"""
    logger.info("Loading exchange rates")
    df = pl.read_parquet(eqrate_path)
    df = df.select(['CURRENCY', 'SPOTRATE']).unique(subset=['CURRENCY'])
    return df


def create_master_dataset(dppidm: pl.DataFrame, pidmmast: pl.DataFrame, 
                          eqrate: pl.DataFrame) -> pl.DataFrame:
    """Create master dataset with all calculations"""
    logger.info("Creating master dataset")
    
    # Merge datasets
    master = dppidm.join(pidmmast, on='ACCNUM', how='left')
    master = master.rename({'B_CURR_TYPE': 'CURRENCY'})
    master = master.join(eqrate, on='CURRENCY', how='left')
    
    # Calculate fields
    master = master.with_columns([
        # Basic fields
        pl.col('CONTRACT_NO').alias('ACCNUM_OUT'),
        pl.lit(2).alias('BUSSTYPE'),
        pl.col('BRCHABRV').alias('BRANCHCODE'),  # Would need $BRCHRVR format
        pl.lit(12).alias('DEPTYPE'),
        pl.col('CURRENCY').alias('CURRENCYCODE'),
        pl.lit(2).alias('INSURABILITY'),
        pl.lit('0').alias('DEPCODE'),
        pl.lit('0').alias('CIFNUM'),
        
        # Account open date
        pl.when(pl.col('START_DATE').is_null())
          .then(pl.lit('19000101'))
        .otherwise(pl.col('START_DATE').str.replace_all('-', ''))
        .alias('ACCOPENDATE'),
        
        pl.lit('0').alias('APPROVERACCOPEN'),
        
        # Balance calculations
        (pl.col('B_CURR_AMOUNT') * pl.col('SPOTRATE')).round(2).alias('LEDGERBAL'),
        pl.col('SPOTRATE').alias('FXRATE'),
        
        pl.when(pl.col('CURRENCY') == 'MYR')
          .then(0.0)
        .otherwise(pl.col('B_CURR_AMOUNT').round(2))
        .alias('FXLEDGERBAL'),
        
        # Accrued interest
        (pl.col('ACCRUAL_INT') * pl.col('SPOTRATE')).round(2).alias('ACCRUEDINT'),
        
        pl.when(pl.col('CURRENCY') == 'MYR')
          .then(0.0)
        .otherwise(pl.col('ACCRUAL_INT').round(2))
        .alias('FXACCRUEDINT'),
        
        # Default values
        pl.lit(0.0).alias('BILLPAYABLE'),
        pl.lit(0.0).alias('FXBILLPAYABLE'),
        pl.lit(0.0).alias('INTPROFTODATE'),
        pl.lit(0.0).alias('FXINTPROFTODATE'),
        pl.lit(0).alias('TAGGING'),
        pl.lit('0').alias('CONDOFTAGGING'),
        pl.lit(2).alias('UCLAIMEDMONIES'),
        pl.lit(4).alias('ATMCARD'),
        
        # Maturity date
        pl.when(pl.col('MATURITY_DATE').is_null())
          .then(pl.lit('19000101'))
        .otherwise(pl.col('MATURITY_DATE').str.replace_all('-', ''))
        .alias('MATURITYDATE'),
        
        # Interest rate
        (pl.col('INTEREST') / 100).alias('RATE')
    ])
    
    # Calculate derived fields
    master = master.with_columns([
        pl.col('LEDGERBAL').alias('AVAILABLEBAL'),
        pl.col('FXLEDGERBAL').alias('FXAVAILABLEBAL'),
        pl.col('LEDGERBAL').alias('NOMINALVALUES'),
        pl.col('FXLEDGERBAL').alias('FXNOMINALVALUE'),
        (pl.col('LEDGERBAL') + pl.col('ACCRUEDINT')).alias('PROCEEDVALUE'),
        (pl.col('FXLEDGERBAL') + pl.col('FXACCRUEDINT')).alias('FXPROCEEDVALUE')
    ])
    
    # Handle phone numbers
    master = master.with_columns([
        pl.when(pl.col('TELHOME').is_null() | (pl.col('TELHOME') == ''))
          .then(pl.lit('0')).otherwise(pl.col('TELHOME')).alias('TELHOME'),
        pl.when(pl.col('TELOFFICE').is_null() | (pl.col('TELOFFICE') == ''))
          .then(pl.lit('0')).otherwise(pl.col('TELOFFICE')).alias('TELOFFICE'),
        pl.when(pl.col('TELHP').is_null() | (pl.col('TELHP') == ''))
          .then(pl.lit('0')).otherwise(pl.col('TELHP')).alias('TELHP')
    ])
    
    # Handle REGNUM for business accounts
    master = master.with_columns([
        pl.when(pl.col('ACCTYPE').is_in([1, 2, 3]))
          .then(pl.lit('0'))
        .otherwise(pl.col('REGNUM'))
        .alias('REGNUM')
    ])
    
    logger.info(f"Master records created: {len(master)}")
    return master


def write_partitioned_parquet(df: pl.DataFrame, base_path: str, 
                               table_name: str, report_date: datetime):
    """Write DataFrame to partitioned Parquet files"""
    year = report_date.strftime('%Y')
    month = report_date.strftime('%m')
    day = report_date.strftime('%d')
    
    output_path = Path(base_path) / table_name / f'year={year}' / f'month={month}' / f'day={day}'
    output_path.mkdir(parents=True, exist_ok=True)
    
    output_file = output_path / f'{table_name}.parquet'
    df.write_parquet(output_file, compression='snappy')
    
    logger.info(f"Written {len(df)} records to {output_file}")
    return output_file


def write_delimited_output(df: pl.DataFrame, output_path: str, delimiter: str = '|'):
    """Write delimited output file (for compatibility with original format)"""
    df.write_csv(output_path, separator=delimiter)
    logger.info(f"Written delimited file: {output_path}")


def main(config: dict):
    """
    Main processing function
    
    config should contain:
    - reptdate_path: Path to REPTDATE file
    - dpmas_path: Path to DPMAS fixed-width file
    - deposit_paths: List of deposit transaction file paths
    - pidm_path: Path to PIDM CISDEPOSIT file
    - eqrate_path: Path to EQRATE file
    - holder_path: Path to HOLDER file
    - address_path: Path to ADDRESS file
    - output_base: Base output directory
    """
    
    # Read reporting date
    params = read_reptdate(config['reptdate_path'])
    
    # Read and process data
    dppidm = read_dpmas_file(config['dpmas_path'], params['RDATE'])
    depocis = process_deposit_data(config['deposit_paths'])
    pidmmast = process_pidm_master(config['pidm_path'], depocis)
    eqrate = get_exchange_rates(config['eqrate_path'])
    
    # Create master dataset
    master = create_master_dataset(dppidm, pidmmast, eqrate)
    
    # Write master output
    master_file = write_partitioned_parquet(
        master, 
        config['output_base'], 
        'pidm_deposit_master',
        params['REPTDATE']
    )
    
    # Process holder data
    if 'holder_path' in config and Path(config['holder_path']).exists():
        holder = pl.read_parquet(config['holder_path'])
        holder = holder.join(
            master.select(['ACCNUM_OUT', 'ACCNAME', 'ACCTYPE']),
            left_on='ACCNUM',
            right_on='ACCNUM_OUT',
            how='inner'
        )
        
        # Process holder logic (simplified)
        holder = holder.with_columns([
            pl.when(pl.col('ACCTYPE').is_in([4, 5, 6, 7]))
              .then(pl.lit('0'))
            .otherwise(pl.col('HOLDERNAME'))
            .alias('HOLDERNAME')
        ])
        
        write_partitioned_parquet(
            holder,
            config['output_base'],
            'pidm_deposit_holder',
            params['REPTDATE']
        )
    
    # Process address data
    if 'address_path' in config and Path(config['address_path']).exists():
        address = pl.read_parquet(config['address_path'])
        address = address.join(
            master.select(['ACCNUM_OUT']),
            left_on='ACCNUM',
            right_on='ACCNUM_OUT',
            how='inner'
        )
        
        write_partitioned_parquet(
            address,
            config['output_base'],
            'pidm_deposit_address',
            params['REPTDATE']
        )
    
    logger.info("Processing completed successfully")
    
    return {
        'master_records': len(master),
        'report_date': params['REPTDATE'],
        'output_path': config['output_base']
    }


if __name__ == "__main__":
    # Example configuration
    config = {
        'reptdate_path': 'data/mnitb/reptdate.parquet',
        'dpmas_path': 'data/dpmas.txt',
        'deposit_paths': [
            'data/deposit/trans02.parquet',
            'data/deposit/trans03.parquet',
            'data/deposit/trans04.parquet',
            'data/deposit/trans05.parquet',
            'data/deposit/trans06.parquet',
            'data/deposit/trans07.parquet',
            'data/deposit/trans11.parquet',
            'data/deposit/trans14.parquet',
            'data/deposit/trans08.parquet',
            'data/deposit/trans01.parquet',
            'data/deposit/tranorg.parquet',
            'data/deposit/tran999.parquet'
        ],
        'pidm_path': 'data/pidm/cisdeposit.parquet',
        'eqrate_path': 'data/eq/eqrate.parquet',
        'holder_path': 'data/pidmwh/holder.parquet',
        'address_path': 'data/pidmwh/address.parquet',
        'output_base': 'output/pidm'
    }
    
    result = main(config)
    print(f"Processed {result['master_records']} records for {result['report_date']}")
