"""
SAS to Python Translation Program - EIBDPBIF
Processes parquet files and generates output matching SAS format specifications
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import re

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Define all paths at the beginning for easy modification
INPUT_DIR = Path("./input")
OUTPUT_DIR = Path("./output")
TEMP_DIR = Path("./temp")
FACWH_DIR = Path("./facwh")
SASD_DIR = Path("./sasd")

# Create directories if they don't exist
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR, FACWH_DIR, SASD_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Input parquet files (assumed to exist with SAS column specifications)
INPUT_FILES = {
    'client': INPUT_DIR / 'client.parquet',
    'cclink': INPUT_DIR / 'cclink.parquet',
    'crnote': INPUT_DIR / 'crnote.parquet',
    'crnote1': INPUT_DIR / 'crnote1.parquet',
    'custmr': INPUT_DIR / 'custmr.parquet',
    'invois': INPUT_DIR / 'invois.parquet',
    'paymnt': INPUT_DIR / 'paymnt.parquet',
    'journl': INPUT_DIR / 'journl.parquet',
    'receip': INPUT_DIR / 'receip.parquet',
    'advanc': INPUT_DIR / 'advanc.parquet',
    'ischedu': INPUT_DIR / 'ischedu.parquet',
    'history': INPUT_DIR / 'history.parquet'
}

# =============================================================================
# SAS FORMAT DEFINITIONS
# =============================================================================

# Define SAS format equivalents
SECTORCD_FORMATS = {
    '0101': 'MANUFACTURING',
    '0102': 'TRADING',
    '0103': 'SERVICES',
    '0104': 'CONSTRUCTION',
    '0105': 'TRANSPORT',
    '0106': 'FINANCIAL',
    '0199': 'OTHERS'
}

def apply_sectorcd_format(sectorcd):
    """Apply $SECTCD format to sector code"""
    return SECTORCD_FORMATS.get(sectorcd, 'UNKNOWN')

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def sas_date_to_datetime(sas_date):
    """
    Convert SAS numeric date (days since 1960-01-01) to datetime
    SAS date 0 = 1960-01-01
    """
    if sas_date is None or sas_date <= 0 or pd.isna(sas_date):
        return None
    try:
        return datetime(1960, 1, 1) + timedelta(days=int(sas_date))
    except:
        return None

def datetime_to_sas_date(dt):
    """Convert datetime to SAS numeric date"""
    if dt is None:
        return 0
    reference = datetime(1960, 1, 1)
    return (dt - reference).days

def format_sas_z8_date(sas_date):
    """Format SAS date as Z8. format (yyyymmdd)"""
    if sas_date is None or sas_date <= 0:
        return '0' * 8
    dt = sas_date_to_datetime(sas_date)
    if dt:
        return dt.strftime('%Y%m%d')
    return '0' * 8

def parse_yyyymmdd_to_sas(date_str):
    """Parse YYYYMMDD string to SAS date"""
    if not date_str or date_str == '0' * 8:
        return 0
    try:
        dt = datetime.strptime(date_str, '%Y%m%d')
        return datetime_to_sas_date(dt)
    except:
        return 0

def calculate_report_date():
    """Calculate report date (today - 1) and create macro variables"""
    reptdate = datetime.now().date() - timedelta(days=1)
    
    # Create macro variables equivalent
    macro_vars = {
        'REPTYEAR': reptdate.strftime('%y'),
        'REPTMON': reptdate.strftime('%m'),
        'REPTDAY': reptdate.strftime('%d'),
        'RDATE': reptdate.strftime('%d%m%y'),
        'CDATE': str(datetime_to_sas_date(datetime.combine(reptdate, datetime.min.time()))),
        'PDATE': ''
    }
    
    # Calculate previous date
    mm = reptdate.month
    yy = reptdate.year
    stdate = datetime(yy, mm, 1).date()
    prevdate = stdate - timedelta(days=1)
    macro_vars['PDATE'] = str(datetime_to_sas_date(datetime.combine(prevdate, datetime.min.time())))
    
    return macro_vars

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_client_data(macro_vars):
    """Process CLIENT data - Only PBBH accounts"""
    
    # Read client data using Polars
    client_df = pl.read_parquet(INPUT_FILES['client'])
    
    # Apply SAS-like transformations
    client_df = client_df.filter(
        (pl.col('ENTITY') == 'PBBH') & 
        (pl.col('CLIENTNO').is_not_null()) &
        (pl.col('CLIENTNO') != ' ')
    ).with_columns([
        pl.col('CUSTCD').cast(pl.Utf8).alias('CUSTCX'),
        pl.col('SECTORX').str.slice(0, 4).alias('SECTORCD'),
        pl.col('SECTORX').str.slice(0, 4).apply(apply_sectorcd_format).alias('SECTCD'),
        pl.lit('D').alias('AMTIND'),
        (pl.col('FIU') + pl.col('PRMTHFIU')).alias('BALANCE'),
        pl.when(
            pl.col('INLIMIT') > 0
        ).then(
            (pl.col('INLIMIT') - (pl.col('FIU') + pl.col('PRMTHFIU'))) * 0.2
        ).otherwise(0).alias('UNDRAWN')
    ])
    
    # Fix negative UNDRAWN
    client_df = client_df.with_columns(
        pl.when(pl.col('UNDRAWN') < 0).then(0).otherwise(pl.col('UNDRAWN')).alias('UNDRAWN')
    )
    
    # Process date fields
    for date_field in ['CFDATE', 'MUPDATE', 'MCREATE', 'EFFECDT', 'STDATE']:
        new_field = f"{date_field}S"
        client_df = client_df.with_columns([
            pl.when(
                pl.col(date_field) > 0
            ).then(
                pl.col(date_field).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new_field)
        ])
    
    # Add STATECD based on CLIENTNO
    client_df = client_df.with_columns([
        pl.when(
            pl.col('CLIENTNO').str.slice(7, 2) == 'KL'
        ).then(pl.lit('W')).when(
            pl.col('CLIENTNO').str.slice(7, 2) == 'PG'
        ).then(pl.lit('P')).when(
            pl.col('CLIENTNO').str.slice(7, 2) == 'JB'
        ).then(pl.lit('J')).otherwise(None).alias('STATECD'),
        
        pl.col('PROFITCT').alias('BRANCH')
    ])
    
    # Sort and save
    client_df = client_df.sort('CLIENTNO')
    
    # Save to FACWH
    output_filename = f"CLIEN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    client_df.write_parquet(FACWH_DIR / output_filename)
    
    # Create deduplicated client list
    client_list_df = client_df.select('CLIENTNO').unique()
    client_list_df.write_parquet(TEMP_DIR / 'client_dedup.parquet')
    
    return client_df, client_list_df

def process_cclink_data(client_list_df, macro_vars):
    """Process CCLINK data"""
    
    cclink_df = pl.read_parquet(INPUT_FILES['cclink'])
    
    # Process date fields
    date_mappings = [
        ('EXDATE1', 'EXDATS1'),
        ('APDATE1', 'APDATS1'),
        ('MUPDATE', 'MUPDATS'),
        ('MCREATE', 'MCREATS'),
        ('EXPRDTE', 'EXPRDTS'),
        ('APPRDTE', 'APPRDTS')
    ]
    
    for orig, new in date_mappings:
        cclink_df = cclink_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    cclink_df = cclink_df.sort('CLIENTNO')
    
    # Merge with client list (equivalent to DATA step merge)
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute(f"CREATE OR REPLACE VIEW cclink_view AS SELECT * FROM cclink_df")
    
    query = """
    SELECT cclink_view.* 
    FROM cclink_view 
    INNER JOIN client_view ON cclink_view.CLIENTNO = client_view.CLIENTNO
    WHERE cclink_view.CUSTNO IS NOT NULL AND cclink_view.CUSTNO != ' '
    """
    
    cclink_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    cclink_df = cclink_df.sort(['CLIENTNO', 'CUSTNO'])
    output_filename = f"CCLNK{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    cclink_df.write_parquet(FACWH_DIR / output_filename)
    
    # Create deduplicated custno list
    cust_list_df = cclink_df.select('CUSTNO').unique()
    cust_list_df.write_parquet(TEMP_DIR / 'cclink_dedup.parquet')
    
    return cclink_df, cust_list_df

def process_crnote_data(client_list_df, macro_vars):
    """Process CRNOTE data"""
    
    crnote_df = pl.read_parquet(INPUT_FILES['crnote'])
    
    # Process date fields
    date_mappings = [
        ('ADATE', 'ADATES'),
        ('TRXDATE', 'TRXDATS'),
        ('CRDATE', 'CRDATES')
    ]
    
    for orig, new in date_mappings:
        crnote_df = crnote_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    crnote_df = crnote_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW crnote_view AS SELECT * FROM crnote_df")
    
    query = """
    SELECT crnote_view.* 
    FROM crnote_view 
    INNER JOIN client_view ON crnote_view.CLIENTNO = client_view.CLIENTNO
    """
    
    crnote_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    crnote_df = crnote_df.sort('CUSTNO')
    output_filename = f"CRNOT{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    crnote_df.write_parquet(FACWH_DIR / output_filename)
    
    return crnote_df

def process_crnote1_data(client_list_df, macro_vars):
    """Process CRNOTE1 data"""
    
    crnote1_df = pl.read_parquet(INPUT_FILES['crnote1'])
    
    # Process date fields
    date_mappings = [
        ('TRXDATE', 'TRXDATS'),
        ('CRDATE', 'CDATES')
    ]
    
    for orig, new in date_mappings:
        crnote1_df = crnote1_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    crnote1_df = crnote1_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW crnote1_view AS SELECT * FROM crnote1_df")
    
    query = """
    SELECT crnote1_view.* 
    FROM crnote1_view 
    INNER JOIN client_view ON crnote1_view.CLIENTNO = client_view.CLIENTNO
    """
    
    crnote1_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    crnote1_df = crnote1_df.sort('CLIENTNO')
    output_filename = f"CRNT1{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    crnote1_df.write_parquet(FACWH_DIR / output_filename)
    
    return crnote1_df

def process_custmr_data(cust_list_df, macro_vars):
    """Process CUSTMR data"""
    
    custmr_df = pl.read_parquet(INPUT_FILES['custmr'])
    
    # Process date fields
    date_mappings = [
        ('MCREATE', 'MCREATS'),
        ('MUPDATE', 'MUPDATS')
    ]
    
    for orig, new in date_mappings:
        custmr_df = custmr_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with cclink list
    custmr_df = custmr_df.sort('CUSTNO')
    
    cclink_parquet = TEMP_DIR / 'cclink_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW cclink_view AS SELECT * FROM '{cclink_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW custmr_view AS SELECT * FROM custmr_df")
    
    query = """
    SELECT custmr_view.* 
    FROM custmr_view 
    INNER JOIN cclink_view ON custmr_view.CUSTNO = cclink_view.CUSTNO
    """
    
    custmr_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    custmr_df = custmr_df.sort('CUSTNO')
    output_filename = f"CUSTM{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    custmr_df.write_parquet(FACWH_DIR / output_filename)
    
    return custmr_df

def process_invois_data(client_list_df, macro_vars):
    """Process INVOIS data"""
    
    invois_df = pl.read_parquet(INPUT_FILES['invois'])
    
    # Process date fields
    date_mappings = [
        ('ADJDTE', 'ADATES'),
        ('TRXDATE', 'TRXDATS'),
        ('INVDATE', 'INVDATS')
    ]
    
    for orig, new in date_mappings:
        invois_df = invois_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    invois_df = invois_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW invois_view AS SELECT * FROM invois_df")
    
    query = """
    SELECT invois_view.* 
    FROM invois_view 
    INNER JOIN client_view ON invois_view.CLIENTNO = client_view.CLIENTNO
    """
    
    invois_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    invois_df = invois_df.sort('CUSTNO')
    output_filename = f"INVOI{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    invois_df.write_parquet(FACWH_DIR / output_filename)
    
    return invois_df

def process_paymnt_data(client_list_df, macro_vars):
    """Process PAYMNT data"""
    
    paymnt_df = pl.read_parquet(INPUT_FILES['paymnt'])
    
    # Process date fields
    paymnt_df = paymnt_df.with_columns([
        pl.when(
            pl.col('DATEIN') > 0
        ).then(
            pl.col('DATEIN').apply(lambda x: parse_yyyymmdd_to_sas(
                format_sas_z8_date(x)[:8]
            ))
        ).otherwise(0).alias('DATEIS')
    ])
    
    # Sort and merge with client list
    paymnt_df = paymnt_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW paymnt_view AS SELECT * FROM paymnt_df")
    
    query = """
    SELECT paymnt_view.* 
    FROM paymnt_view 
    INNER JOIN client_view ON paymnt_view.CLIENTNO = client_view.CLIENTNO
    """
    
    paymnt_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    paymnt_df = paymnt_df.sort('CUSTNO')
    output_filename = f"PAYMN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    paymnt_df.write_parquet(FACWH_DIR / output_filename)
    
    return paymnt_df

def process_journl_data(client_list_df, macro_vars):
    """Process JOURNL data"""
    
    journl_df = pl.read_parquet(INPUT_FILES['journl'])
    
    # Process date fields
    date_mappings = [
        ('ADATE', 'ADATES'),
        ('TRXDATE', 'TRXDATS')
    ]
    
    for orig, new in date_mappings:
        journl_df = journl_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    journl_df = journl_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW journl_view AS SELECT * FROM journl_df")
    
    query = """
    SELECT journl_view.* 
    FROM journl_view 
    INNER JOIN client_view ON journl_view.CLIENTNO = client_view.CLIENTNO
    """
    
    journl_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    journl_df = journl_df.sort('CUSTNO')
    output_filename = f"JOURN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    journl_df.write_parquet(FACWH_DIR / output_filename)
    
    return journl_df

def process_receip_data(client_list_df, macro_vars):
    """Process RECEIP data"""
    
    receip_df = pl.read_parquet(INPUT_FILES['receip'])
    
    # Process date fields
    date_mappings = [
        ('VDATE', 'VDATES'),
        ('ATDATE', 'ATDATES'),
        ('ADATE', 'ADATES'),
        ('TRXDATE', 'TRXDATS')
    ]
    
    for orig, new in date_mappings:
        receip_df = receip_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    receip_df = receip_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW receip_view AS SELECT * FROM receip_df")
    
    query = """
    SELECT receip_view.* 
    FROM receip_view 
    INNER JOIN client_view ON receip_view.CLIENTNO = client_view.CLIENTNO
    """
    
    receip_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    receip_df = receip_df.sort('CUSTNO')
    output_filename = f"RECEI{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    receip_df.write_parquet(FACWH_DIR / output_filename)
    
    return receip_df

def process_advanc_data(client_list_df, macro_vars):
    """Process ADVANC data"""
    
    advanc_df = pl.read_parquet(INPUT_FILES['advanc'])
    
    # Process date fields
    advanc_df = advanc_df.with_columns([
        pl.when(
            pl.col('ADATE') > 0
        ).then(
            pl.col('ADATE').apply(lambda x: parse_yyyymmdd_to_sas(
                format_sas_z8_date(x)[:8]
            ))
        ).otherwise(0).alias('ADATES')
    ])
    
    # Sort and merge with client list
    advanc_df = advanc_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW advanc_view AS SELECT * FROM advanc_df")
    
    query = """
    SELECT advanc_view.* 
    FROM advanc_view 
    INNER JOIN client_view ON advanc_view.CLIENTNO = client_view.CLIENTNO
    """
    
    advanc_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    advanc_df = advanc_df.sort(['CLIENTNO', 'VOUCHER'])
    output_filename = f"ADVAN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    advanc_df.write_parquet(FACWH_DIR / output_filename)
    
    return advanc_df

def process_ischedu_data(client_list_df, macro_vars):
    """Process ISCHED data"""
    
    ischedu_df = pl.read_parquet(INPUT_FILES['ischedu'])
    
    # Process date fields
    date_mappings = [
        ('TRXDATE', 'ITRXDT'),
        ('VALDTE', 'VALDTS')
    ]
    
    for orig, new in date_mappings:
        ischedu_df = ischedu_df.with_columns([
            pl.when(
                pl.col(orig) > 0
            ).then(
                pl.col(orig).apply(lambda x: parse_yyyymmdd_to_sas(
                    format_sas_z8_date(x)[:8]
                ))
            ).otherwise(0).alias(new)
        ])
    
    # Sort and merge with client list
    ischedu_df = ischedu_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW ischedu_view AS SELECT * FROM ischedu_df")
    
    query = """
    SELECT ischedu_view.* 
    FROM ischedu_view 
    INNER JOIN client_view ON ischedu_view.CLIENTNO = client_view.CLIENTNO
    """
    
    ischedu_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    ischedu_df = ischedu_df.sort(['CLIENTNO', 'SCHEDNO'])
    output_filename = f"ISCHE{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    ischedu_df.write_parquet(FACWH_DIR / output_filename)
    
    return ischedu_df

def process_history_data(macro_vars):
    """Process HISTORY data"""
    
    history_df = pl.read_parquet(INPUT_FILES['history'])
    
    # Process date fields
    history_df = history_df.with_columns([
        pl.when(
            pl.col('HSDATE') > 0
        ).then(
            pl.col('HSDATE').apply(lambda x: parse_yyyymmdd_to_sas(
                format_sas_z8_date(x)[:8]
            ))
        ).otherwise(0).alias('HSDATS')
    ])
    
    # Filter out blank client numbers
    history_df = history_df.filter(
        pl.col('CLIENTNO') != '   '
    )
    
    # Filter by date (HSDATS >= &PDATE)
    pdate = int(macro_vars['PDATE'])
    history_df = history_df.filter(pl.col('HSDATS') >= pdate)
    
    # Sort and merge with client list
    history_df = history_df.sort('CLIENTNO')
    
    client_parquet = TEMP_DIR / 'client_dedup.parquet'
    
    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW client_view AS SELECT * FROM '{client_parquet}'")
    conn.execute("CREATE OR REPLACE VIEW history_view AS SELECT * FROM history_df")
    
    query = """
    SELECT history_view.* 
    FROM history_view 
    INNER JOIN client_view ON history_view.CLIENTNO = client_view.CLIENTNO
    """
    
    history_df = conn.execute(query).pl()
    conn.close()
    
    # Sort and save
    history_df = history_df.sort('CLIENTNO')
    output_filename = f"HISTR{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    history_df.write_parquet(FACWH_DIR / output_filename)
    
    return history_df

def process_final_advance(macro_vars):
    """Process final ADVAN and CLIENT merge for CLADV"""
    
    # Read the saved files
    client_filename = f"CLIEN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    advan_filename = f"ADVAN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    
    client_df = pl.read_parquet(FACWH_DIR / client_filename)
    advan_df = pl.read_parquet(FACWH_DIR / advan_filename)
    
    # Filter client
    client_df = client_df.filter(pl.col('CUSTCD') > 0)
    client_df = client_df.with_columns(pl.lit('D').alias('AMTIND'))
    client_df = client_df.sort('CLIENTNO')
    
    # Keep only required columns from advan
    advan_df = advan_df.select(['CLIENTNO', 'RLSEAMT']).sort('CLIENTNO')
    
    # Merge and calculate REBATE and APPRLIMX
    conn = duckdb.connect()
    conn.execute("CREATE OR REPLACE VIEW client_view AS SELECT * FROM client_df")
    conn.execute("CREATE OR REPLACE VIEW advan_view AS SELECT * FROM advan_df")
    
    query = """
    SELECT 
        client_view.*,
        COALESCE(advan_view.RLSEAMT, 0) as RLSEAMT,
        COALESCE(advan_view.RLSEAMT, 0) - client_view.BALANCE as REBATE,
        client_view.INLIMIT - (COALESCE(advan_view.RLSEAMT, 0) - client_view.BALANCE) as APPRLIMX
    FROM client_view
    LEFT JOIN advan_view ON client_view.CLIENTNO = advan_view.CLIENTNO
    """
    
    final_df = conn.execute(query).pl()
    conn.close()
    
    # Save final output
    output_filename = f"CLADV{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    final_df.write_parquet(FACWH_DIR / output_filename)
    
    print(f"Final CLADV file created: {output_filename}")
    
    return final_df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function following SAS program flow"""
    
    print(f"Starting EIBDPBIF translation at {datetime.now()}")
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {FACWH_DIR}")
    
    # Calculate report date and macro variables
    macro_vars = calculate_report_date()
    print(f"Report Date: {macro_vars['RDATE']}")
    print(f"Report Year: {macro_vars['REPTYEAR']}")
    print(f"Report Month: {macro_vars['REPTMON']}")
    print(f"Report Day: {macro_vars['REPTDAY']}")
    
    # Verify input files exist
    missing_files = [name for name, path in INPUT_FILES.items() if not path.exists()]
    if missing_files:
        print(f"Warning: Missing input files: {missing_files}")
        print("Continuing with available files...")
    
    try:
        # Step 1: Process CLIENT data
        print("\nStep 1: Processing CLIENT data...")
        client_df, client_list_df = process_client_data(macro_vars)
        
        # Step 2: Process CCLINK data
        print("Step 2: Processing CCLINK data...")
        cclink_df, cust_list_df = process_cclink_data(client_list_df, macro_vars)
        
        # Step 3: Process CRNOTE data
        print("Step 3: Processing CRNOTE data...")
        crnote_df = process_crnote_data(client_list_df, macro_vars)
        
        # Step 4: Process CRNOTE1 data
        print("Step 4: Processing CRNOTE1 data...")
        crnote1_df = process_crnote1_data(client_list_df, macro_vars)
        
        # Step 5: Process CUSTMR data
        print("Step 5: Processing CUSTMR data...")
        custmr_df = process_custmr_data(cust_list_df, macro_vars)
        
        # Step 6: Process INVOIS data
        print("Step 6: Processing INVOIS data...")
        invois_df = process_invois_data(client_list_df, macro_vars)
        
        # Step 7: Process PAYMNT data
        print("Step 7: Processing PAYMNT data...")
        paymnt_df = process_paymnt_data(client_list_df, macro_vars)
        
        # Step 8: Process JOURNL data
        print("Step 8: Processing JOURNL data...")
        journl_df = process_journl_data(client_list_df, macro_vars)
        
        # Step 9: Process RECEIP data
        print("Step 9: Processing RECEIP data...")
        receip_df = process_receip_data(client_list_df, macro_vars)
        
        # Step 10: Process ADVANC data
        print("Step 10: Processing ADVANC data...")
        advanc_df = process_advanc_data(client_list_df, macro_vars)
        
        # Step 11: Process ISCHED data
        print("Step 11: Processing ISCHED data...")
        ischedu_df = process_ischedu_data(client_list_df, macro_vars)
        
        # Step 12: Process HISTORY data
        print("Step 12: Processing HISTORY data...")
        history_df = process_history_data(macro_vars)
        
        # Step 13: Final CLADV processing
        print("Step 13: Creating final CLADV dataset...")
        final_df = process_final_advance(macro_vars)
        
        print(f"\nProcessing completed successfully at {datetime.now()}")
        print(f"Output files created in {FACWH_DIR}:")
        print(f"  - CLIEN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - CCLNK{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - CRNOT{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - CRNT1{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - CUSTM{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - INVOI{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - PAYMN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - JOURN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - RECEI{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - ADVAN{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - ISCHE{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - HISTR{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        print(f"  - CLADV{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet")
        
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
