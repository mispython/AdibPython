"""
SAS to Python Translation - EIBDKAPD
Processes securities data and generates various report tables
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

# Create directories
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Libraries (SAS libnames)
EQ_DIR = INPUT_DIR / "eq"
IEQ_DIR = INPUT_DIR / "ieq"
BNMK_DIR = OUTPUT_DIR / "bnmk"
BNMKI_DIR = OUTPUT_DIR / "bnmki"

for dir_path in [BNMK_DIR, BNMKI_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE HANDLING AND MACRO VARIABLES
# =============================================================================

def setup_date_variables():
    """Calculate report date and create macro variable equivalents"""
    sas_epoch = datetime(1960, 1, 1)
    today = datetime.now().date()
    sasdate = (today - sas_epoch.date()).days - 1  # TODAY()-1
    
    mm = today.month
    day = today.day
    yy = today.year
    
    # Determine week (WK) based on day of month
    if 1 <= day <= 8:
        wk = '1'
    elif 9 <= day <= 15:
        wk = '2'
    elif 16 <= day <= 22:
        wk = '3'
    else:
        wk = '4'
    
    return {
        'SXDATE': sasdate,
        'MM': mm,
        'WK': wk,
        'INDATES': str(sasdate).zfill(5),
        'REPTMON': str(mm).zfill(2),
        'REPTDAY': str(day).zfill(2),
        'REPTYEAR': str(yy).zfill(4),
        'REPTYR': str(yy)[-2:],
        'YYMMDD': today.strftime('%y%m%d')
    }

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def sas_date_to_datetime(sas_date):
    """Convert SAS numeric date to datetime"""
    if sas_date is None or sas_date <= 0:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def get_elday(reptdats):
    """Determine ELDAY based on day of month"""
    if not reptdats:
        return None
    
    dt = sas_date_to_datetime(reptdats)
    if not dt:
        return None
    
    day = dt.day
    month = dt.month
    year = dt.year
    
    # Base mapping
    if day in (1, 9, 16, 23):
        elday = 'DAYA'
    elif day in (2, 10, 17, 24):
        elday = 'DAYB'
    elif day in (3, 11, 18, 25):
        elday = 'DAYC'
    elif day in (4, 12, 19, 26):
        elday = 'DAYD'
    elif day in (5, 13, 20, 27):
        elday = 'DAYE'
    elif day in (6, 14, 21, 28):
        elday = 'DAYF'
    elif day in (7, 29):
        elday = 'DAYG'
    elif day == 30:
        elday = 'DAYH'
    elif day in (8, 15, 22, 31):
        elday = 'DAYI'
    else:
        elday = None
    
    # Adjustments for months with 30 days
    if month in (4, 6, 9, 11) and day == 30:
        elday = 'DAYI'
    
    # Adjustments for February
    if month == 2:
        if day == 28:
            elday = 'DAYI'
        if year % 4 == 0:  # Leap year
            if day == 28:
                elday = 'DAYF'
            elif day == 29:
                elday = 'DAYI'
    
    return elday

# =============================================================================
# SECURITY TYPE LISTS
# =============================================================================

SREPO = ['MGS', 'MTB', 'ITB', 'MGI', 'BNN', 'BNB', 'KHA', 'CNT', 'CB1',
         'SCD', 'BMN', 'BMC', 'CF1', 'SAC', 'SMC', 'BMF', 'SCM']

NREPO = ['DHB', 'DMB', 'DBD', 'DBZ', 'IBM', 'IBZ', 'ISB', 'PNB', 'MTN', 'IDS']

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_utfx(macro_vars):
    """Process UTFX data"""
    input_file = EQ_DIR / f"UTFX{macro_vars['REPTYR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    
    if not input_file.exists():
        print(f"Warning: {input_file} not found")
        return None
    
    utfx_df = pl.read_parquet(input_file)
    
    # Apply filters
    utfx_df = utfx_df.filter(
        pl.col('DEALTYPE').is_in(['BCD', 'BCQ']) &
        (pl.col('CUSTFISS') >= '80') &
        (pl.col('STRTDATE') <= int(macro_vars['INDATES']))
    )
    
    # Process TBL
    tbl_df = utfx_df.with_columns([
        pl.lit(int(macro_vars['INDATES'])).alias('REPTDATS'),
        pl.col('INTLSNBD').alias('AMOUNT')
    ])
    
    # Add ELDAY
    elday_series = []
    for row in tbl_df.select('REPTDATS').rows():
        elday_series.append(get_elday(row[0]))
    
    tbl_df = tbl_df.with_columns(pl.Series('ELDAY', elday_series))
    
    # Add BNMCODE
    tbl_df = tbl_df.with_columns(pl.lit('4911080000000Y').alias('BNMCODE'))
    
    # Summarize
    summary = tbl_df.group_by(['BNMCODE', 'ELDAY']).agg(pl.col('AMOUNT').sum())
    
    # Get ELBATCH from last record
    if len(tbl_df) > 0:
        last_elday = tbl_df.select('ELDAY').tail(1).item()
        macro_vars['ELBATCH'] = last_elday
    
    return summary

def process_k3tbl(macro_vars):
    """Process K3TBL data from BNMTBL3 file"""
    tbl_file = INPUT_DIR / "BNMTBL3.parquet"
    
    if not tbl_file.exists():
        print(f"Warning: {tbl_file} not found")
        return None
    
    k3tbl_df = pl.read_parquet(tbl_file)
    
    # Process date fields
    k3tbl_df = k3tbl_df.with_columns([
        pl.when(pl.col('UTMDT') != ' ').then(
            pl.col('UTMDT').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('MATDT'),
        pl.when(pl.col('UTOSD') != ' ').then(
            pl.col('UTOSD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('ISSDT'),
        pl.when(pl.col('UTCBD') != ' ').then(
            pl.col('UTCBD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('REPTDATE'),
        pl.when(pl.col('UTCBD') != ' ').then(
            pl.col('UTCBD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('DDATE'),
        pl.when(pl.col('UTIDT') != ' ').then(
            pl.col('UTIDT').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('XDATE')
    ])
    
    # Adjust IZD
    k3tbl_df = k3tbl_df.with_columns([
        pl.when(pl.col('UTSTY') == 'IZD').then(pl.col('UTQDS')).otherwise(pl.col('UTCPR')).alias('UTCPR')
    ])
    
    # Filter IFD/ILD/ISD/IZD
    k3tbl_df = k3tbl_df.filter(
        ~((pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD'])) & 
          (pl.col('XDATE') > pl.col('DDATE')))
    )
    
    # Add REPTDATS
    k3tbl_df = k3tbl_df.with_columns(pl.lit(int(macro_vars['INDATES'])).alias('REPTDATS'))
    
    # Filter by security type
    k3tbl_df = k3tbl_df.filter(
        pl.col('UTSTY').is_in(SREPO + NREPO)
    )
    
    # Add CATG
    k3tbl_df = k3tbl_df.with_columns([
        pl.when(pl.col('UTSTY').is_in(SREPO)).then(pl.lit('    SPECIFIED RENTAS'))
         .when(pl.col('UTSTY').is_in(NREPO)).then(pl.lit('NON SPECIFIED RENTAS'))
         .otherwise(None).alias('CATG')
    ])
    
    # Filter ISSDT > REPTDATE
    k3tbl_df = k3tbl_df.filter(
        ~((pl.col('ISSDT').is_not_null()) & (pl.col('REPTDATE').is_not_null()) & 
          (pl.col('ISSDT') > pl.col('REPTDATE')))
    )
    
    # Add ELDAY
    elday_series = []
    for row in k3tbl_df.select('REPTDATS').rows():
        elday_series.append(get_elday(row[0]))
    
    k3tbl_df = k3tbl_df.with_columns([
        pl.Series('ELDAY', elday_series),
        (pl.col('UTAMOC') * pl.col('UTPCP') * 0.01).alias('AMOUNT')
    ])
    
    return k3tbl_df

def process_k3tbl1(k3tbl_df, macro_vars):
    """Process K3TBL1 for REP2 and REP3 reports"""
    if k3tbl_df is None or len(k3tbl_df) == 0:
        return None, None
    
    k3tbl1_df = k3tbl_df.with_columns(pl.lit('  ').alias('BNMCODE'))
    
    # Filter for specific UTSTY and UTDLP patterns
    mask = (
        pl.col('UTSTY').is_in(['MGS', 'ISB', 'BMN', 'MGI', 'BMC', 'BMF', 'ITB', 'MTB', 'SCD', 'SCM']) &
        pl.col('UTDLP').str.slice(1, 2).is_in(['VA', 'VB', 'VQ', 'VI', 'VT', 'XI', 'XB', 'XS'])
    )
    
    k3tbl1_df = k3tbl1_df.filter(mask).with_columns([
        pl.lit('3250000000000Y').alias('BNMCODE'),
        pl.col('UTAMTS').alias('AMOUNT'),
        (pl.col('UTAMOC') / pl.col('UTFCV') * pl.col('UTAMTS')).alias('COSTDED')
    ]).with_columns([
        (pl.col('AMOUNT') - pl.col('COSTDED')).alias('NETAMT')
    ]).filter(
        (pl.col('BNMCODE') != '  ') & (pl.col('AMOUNT') > 0)
    )
    
    # Summarize for REP2
    rep2_df = k3tbl1_df.group_by(['ELDAY', 'BNMCODE', 'UTSTY', 'UTREF', 'UTDLP']).agg([
        pl.col('AMOUNT').sum(),
        pl.col('COSTDED').sum(),
        pl.col('NETAMT').sum()
    ])
    
    # Summarize for REP3
    rep3_df = k3tbl1_df.group_by(['ELDAY', 'BNMCODE']).agg([
        pl.col('AMOUNT').sum()
    ])
    
    return rep2_df, rep3_df

def process_provalue(macro_vars):
    """Process PROVALUE from UTMS and UTRP"""
    utms_file = EQ_DIR / f"UTMS{macro_vars['YYMMDD']}.parquet"
    utrp_file = EQ_DIR / f"UTRP_UNDL_SEC{macro_vars['YYMMDD']}.parquet"
    
    if not utms_file.exists() or not utrp_file.exists():
        print("Warning: UTMS or UTRP files not found")
        return None
    
    utms_df = pl.read_parquet(utms_file).select(['DEALREF', 'CAMTOWNE', 'BOOKVALU', 'FACEVALU'])
    utrp_df = pl.read_parquet(utrp_file).filter(
        pl.col('DEALDATE') <= pl.col('BUSDATE')
    ).select([
        pl.col('DEALREF_SEC').alias('DEALREF'),
        'SEC_PLEDGE_AMT'
    ])
    
    # Summarize UTRP
    utrp_sum = utrp_df.group_by('DEALREF').agg(pl.col('SEC_PLEDGE_AMT').sum())
    
    # Merge and calculate PROAMOUNT
    provalue_df = utms_df.join(utrp_sum, on='DEALREF', how='inner')
    provalue_df = provalue_df.with_columns([
        ((pl.col('SEC_PLEDGE_AMT') / pl.col('CAMTOWNE')) * 
         (pl.col('CAMTOWNE') / pl.col('FACEVALU') * pl.col('BOOKVALU'))).alias('PROAMOUNT')
    ]).select([
        pl.col('DEALREF').alias('UTDLR'),
        'PROAMOUNT'
    ])
    
    return provalue_df

def process_k3tblf(k3tbl_df, provalue_df, macro_vars):
    """Process K3TBLF for REP4 report"""
    if k3tbl_df is None:
        return None
    
    # Merge with PROVALUE
    if provalue_df is not None:
        k3tblf_df = k3tbl_df.join(provalue_df, on='UTDLR', how='left')
        k3tblf_df = k3tblf_df.with_columns([
            (pl.col('UTAMOC') * pl.col('UTPCP') * 0.01 - pl.col('PROAMOUNT').fill_null(0)).alias('AMOUNT')
        ])
    else:
        k3tblf_df = k3tbl_df.with_columns([
            (pl.col('UTAMOC') * pl.col('UTPCP') * 0.01).alias('AMOUNT')
        ])
    
    k3tblf_df = k3tblf_df.with_columns(pl.lit('  ').alias('BNMCODE'))
    
    # Specified rentas securities
    srepo_mask = (
        pl.col('UTSTY').is_in(SREPO) &
        ~pl.col('UTREF').is_in(['RRB', 'RRE', 'RRS']) &
        ~pl.col('UTDLP').str.slice(1, 2).is_in(['RA', 'RB', 'RQ', 'RI'])
    )
    
    # Apply BNMCODE based on UTSTY
    k3tblf_df = k3tblf_df.with_columns([
        pl.when(srepo_mask & (pl.col('UTSTY') == 'MGS')).then(pl.lit('3721000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['MTB', 'ITB'])).then(pl.lit('3722000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'MGI')).then(pl.lit('3723000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['BNN', 'BNB', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM'])).then(pl.lit('3725000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'KHA')).then(pl.lit('3726000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'CNT')).then(pl.lit('3751000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['CB1', 'CF1'])).then(pl.lit('3752000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['CB2', 'CF2'])).then(pl.lit('3753000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['SAC', 'SMC'])).then(pl.lit('3755000000000Y'))
        .otherwise(pl.col('BNMCODE')).alias('BNMCODE')
    ])
    
    # Non-specified rentas securities with UTREF='DLG'
    nrepo_mask = pl.col('UTSTY').is_in(NREPO) & (pl.col('UTREF') == 'DLG')
    
    k3tblf_df = k3tblf_df.with_columns([
        pl.when(nrepo_mask & (pl.col('UTSTY') == 'DBD')).then(pl.lit('3542000000000Y'))
        .when(nrepo_mask & (pl.col('UTSTY') == 'DHB')).then(pl.lit('3527000000000Y'))
        .when(nrepo_mask & (pl.col('UTSTY') == 'DMB')).then(pl.lit('3528000000000Y'))
        .when(nrepo_mask & pl.col('UTSTY').is_in(['DBZ', 'IBZ', 'ISB', 'PNB'])).then(pl.lit('3542000000000Y'))
        .when(nrepo_mask & pl.col('UTSTY').is_in(['MTN', 'IDS'])).then(pl.lit('3543000000000Y'))
        .otherwise(pl.col('BNMCODE')).alias('BNMCODE')
    ])
    
    # Filter out empty BNMCODE
    k3tblf_df = k3tblf_df.filter(pl.col('BNMCODE') != '  ')
    
    # Summarize for REP4
    rep4_df = k3tblf_df.group_by(['ELDAY', 'BNMCODE', 'UTSTY', 'UTREF']).agg([
        pl.col('AMOUNT').sum()
    ])
    
    return rep4_df

def process_iutfx(macro_vars):
    """Process IUTFX data (Islamic)"""
    input_file = IEQ_DIR / f"IUTFX{macro_vars['REPTYR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}.parquet"
    
    if not input_file.exists():
        print(f"Warning: {input_file} not found")
        return None
    
    iutfx_df = pl.read_parquet(input_file)
    
    # Apply filters
    iutfx_df = iutfx_df.filter(
        pl.col('DEALTYPE').is_in(['BCI', 'BCS', 'BCT', 'BCW', 'BQD']) &
        (pl.col('CUSTFISS') >= '80') &
        (pl.col('STRTDATE') <= int(macro_vars['INDATES']))
    )
    
    # Process TBLI
    tbli_df = iutfx_df.with_columns([
        pl.lit(int(macro_vars['INDATES'])).alias('REPTDATS'),
        pl.col('INTLSNBD').alias('AMOUNT')
    ])
    
    # Add ELDAY
    elday_series = []
    for row in tbli_df.select('REPTDATS').rows():
        elday_series.append(get_elday(row[0]))
    
    tbli_df = tbli_df.with_columns(pl.Series('ELDAY', elday_series))
    
    # Add BNMCODE
    tbli_df = tbli_df.with_columns(pl.lit('4911080000000Y').alias('BNMCODE'))
    
    # Summarize
    summary = tbli_df.group_by(['BNMCODE', 'ELDAY']).agg(pl.col('AMOUNT').sum())
    
    return summary

def process_k3tbli(macro_vars):
    """Process K3TBLI data from BNMTBL3I file (Islamic)"""
    tbl_file = INPUT_DIR / "BNMTBL3I.parquet"
    
    if not tbl_file.exists():
        print(f"Warning: {tbl_file} not found")
        return None
    
    k3tbli_df = pl.read_parquet(tbl_file)
    
    # Process date fields
    k3tbli_df = k3tbli_df.with_columns([
        pl.when(pl.col('UTMDT') != ' ').then(
            pl.col('UTMDT').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('MATDT'),
        pl.when(pl.col('UTOSD') != ' ').then(
            pl.col('UTOSD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('ISSDT'),
        pl.when(pl.col('UTCBD') != ' ').then(
            pl.col('UTCBD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('REPTDATE'),
        pl.when(pl.col('UTCBD') != ' ').then(
            pl.col('UTCBD').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('DDATE'),
        pl.when(pl.col('UTIDT') != ' ').then(
            pl.col('UTIDT').str.strptime(pl.Date, format='%d%m%Y')
        ).otherwise(None).alias('XDATE')
    ])
    
    # Adjust IZD
    k3tbli_df = k3tbli_df.with_columns([
        pl.when(pl.col('UTSTY') == 'IZD').then(pl.col('UTQDS')).otherwise(pl.col('UTCPR')).alias('UTCPR')
    ])
    
    # Filter IFD/ILD/ISD/IZD
    k3tbli_df = k3tbli_df.filter(
        ~((pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD'])) & 
          (pl.col('XDATE') > pl.col('DDATE')))
    )
    
    # Add REPTDATS
    k3tbli_df = k3tbli_df.with_columns(pl.lit(int(macro_vars['INDATES'])).alias('REPTDATS'))
    
    # Filter by security type
    k3tbli_df = k3tbli_df.filter(
        pl.col('UTSTY').is_in(SREPO + NREPO)
    )
    
    # Add CATG
    k3tbli_df = k3tbli_df.with_columns([
        pl.when(pl.col('UTSTY').is_in(SREPO)).then(pl.lit('    SPECIFIED RENTAS'))
         .when(pl.col('UTSTY').is_in(NREPO)).then(pl.lit('NON SPECIFIED RENTAS'))
         .otherwise(None).alias('CATG')
    ])
    
    # Filter ISSDT > REPTDATE
    k3tbli_df = k3tbli_df.filter(
        ~((pl.col('ISSDT').is_not_null()) & (pl.col('REPTDATE').is_not_null()) & 
          (pl.col('ISSDT') > pl.col('REPTDATE')))
    )
    
    # Add ELDAY and AMOUNT
    elday_series = []
    for row in k3tbli_df.select('REPTDATS').rows():
        elday_series.append(get_elday(row[0]))
    
    k3tbli_df = k3tbli_df.with_columns([
        pl.Series('ELDAY', elday_series),
        (pl.col('UTAMOC') * pl.col('UTPCP') * 0.01).alias('AMOUNT')
    ])
    
    return k3tbli_df

def process_k3tbl1i(k3tbli_df, macro_vars):
    """Process K3TBL1I for REP2 and REP3 reports (Islamic)"""
    if k3tbli_df is None or len(k3tbli_df) == 0:
        return None, None
    
    k3tbl1i_df = k3tbli_df.with_columns(pl.lit('  ').alias('BNMCODE'))
    
    # Filter for MGS/ISB with specific UTDLP patterns
    mask = (
        pl.col('UTSTY').is_in(['MGS', 'ISB']) &
        pl.col('UTDLP').str.slice(1, 2).is_in(['VI', 'VT'])
    )
    
    k3tbl1i_df = k3tbl1i_df.filter(mask).with_columns([
        pl.lit('3250000000000Y').alias('BNMCODE'),
        pl.col('UTAMTS').alias('AMOUNT'),
        (pl.col('UTAMOC') / pl.col('UTFCV') * pl.col('UTAMTS')).alias('COSTDED')
    ]).with_columns([
        (pl.col('AMOUNT') - pl.col('COSTDED')).alias('NETAMT')
    ]).filter(
        (pl.col('BNMCODE') != '  ') & (pl.col('AMOUNT') > 0)
    )
    
    # Summarize for REP2I
    rep2i_df = k3tbl1i_df.group_by(['ELDAY', 'BNMCODE', 'UTSTY', 'UTREF', 'UTDLP']).agg([
        pl.col('AMOUNT').sum(),
        pl.col('COSTDED').sum(),
        pl.col('NETAMT').sum()
    ])
    
    # Summarize for REP3I
    rep3i_df = k3tbl1i_df.group_by(['ELDAY', 'BNMCODE']).agg([
        pl.col('AMOUNT').sum()
    ])
    
    return rep2i_df, rep3i_df

def process_k3tblfi(k3tbli_df, macro_vars, variant='standard'):
    """Process K3TBLFI for REP4 reports (Islamic)"""
    if k3tbli_df is None:
        return None
    
    k3tblfi_df = k3tbli_df.with_columns([
        (pl.col('UTAMOC') * pl.col('UTPCP') * 0.01).alias('AMOUNT'),
        pl.lit('  ').alias('BNMCODE')
    ])
    
    if variant == 'standard':
        # REP4 - Standard
        srepo_mask = (
            pl.col('UTSTY').is_in(SREPO) &
            ~pl.col('UTREF').is_in(['RRB', 'RRE', 'RRS'])
        )
        
        nrepo_mask = (
            pl.col('UTSTY').is_in(NREPO) &
            ~pl.col('UTREF').is_in(['DLG', 'IDLG'])
        )
    else:
        # REP4X - Alternative
        srepo_mask = (
            pl.col('UTSTY').is_in(SREPO) &
            ~pl.col('UTREF').is_in(['RRB', 'RRE', 'RRS'])
        )
        
        nrepo_mask = (
            pl.col('UTSTY').is_in(NREPO) &
            pl.col('UTREF').is_in(['DLG', 'IDLG'])
        )
    
    # Apply BNMCODE for specified rentas
    k3tblfi_df = k3tblfi_df.with_columns([
        pl.when(srepo_mask & (pl.col('UTSTY') == 'MGS')).then(pl.lit('3721000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['MTB', 'ITB'])).then(pl.lit('3722000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'MGI')).then(pl.lit('3723000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['BNN', 'BNB', 'BMN', 'BMC', 'BMF'])).then(pl.lit('3725000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['SCD', 'SCM'])).then(pl.lit('3525000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'KHA')).then(pl.lit('3726000000000Y'))
        .when(srepo_mask & (pl.col('UTSTY') == 'CNT')).then(pl.lit('3751000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['CB1', 'CF1'])).then(pl.lit('3752000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['CB2', 'CF2'])).then(pl.lit('3753000000000Y'))
        .when(srepo_mask & pl.col('UTSTY').is_in(['SAC', 'SMC'])).then(pl.lit('3755000000000Y'))
        .otherwise(pl.col('BNMCODE')).alias('BNMCODE')
    ])
    
    # Apply BNMCODE for non-specified rentas
    k3tblfi_df = k3tblfi_df.with_columns([
        pl.when(nrepo_mask & (pl.col('UTSTY') == 'DBD')).then(pl.lit('3542000000000Y'))
        .when(nrepo_mask & (pl.col('UTSTY') == 'DHB')).then(pl.lit('3527000000000Y'))
        .when(nrepo_mask & (pl.col('UTSTY') == 'DMB')).then(pl.lit('3528000000000Y'))
        .when(nrepo_mask & pl.col('UTSTY').is_in(['DBZ', 'IBZ', 'ISB', 'PNB'])).then(pl.lit('3542000000000Y'))
        .when(nrepo_mask & pl.col('UTSTY').is_in(['MTN', 'IDS'])).then(pl.lit('3543000000000Y'))
        .otherwise(pl.col('BNMCODE')).alias('BNMCODE')
    ])
    
    # Filter out empty BNMCODE
    k3tblfi_df = k3tblfi_df.filter(pl.col('BNMCODE') != '  ')
    
    # Summarize
    rep_df = k3tblfi_df.group_by(['ELDAY', 'BNMCODE', 'UTSTY', 'UTREF']).agg([
        pl.col('AMOUNT').sum()
    ])
    
    return rep_df

def append_or_create_table(df, base_path, macro_vars, table_suffix, elday_field='ELDAY'):
    """Append data to existing table or create new one"""
    if df is None or len(df) == 0:
        return
    
    filename = f"{table_suffix}{macro_vars['REPTMON']}{macro_vars['WK']}.parquet"
    filepath = base_path / filename
    
    if filepath.exists():
        # Read existing, filter out current ELBATCH, then append
        existing_df = pl.read_parquet(filepath)
        if 'ELBATCH' in macro_vars:
            existing_df = existing_df.filter(pl.col(elday_field) != macro_vars['ELBATCH'])
        
        # Combine
        combined_df = pl.concat([existing_df, df])
        combined_df.write_parquet(filepath)
        print(f"Appended to {filename}")
    else:
        # Create new
        df.write_parquet(filepath)
        print(f"Created {filename}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    print(f"Starting EIBDKAPD translation at {datetime.now()}")
    
    # Setup date variables
    macro_vars = setup_date_variables()
    print(f"Report Date (SAS): {macro_vars['SXDATE']}, Week: {macro_vars['WK']}")
    print(f"INDATES: {macro_vars['INDATES']}, YYMMDD: {macro_vars['YYMMDD']}")
    
    try:
        # ===== PART 1: Conventional Securities =====
        print("\n" + "="*60)
        print("PART 1: Conventional Securities")
        print("="*60)
        
        # Process UTFX -> TBL -> TBL1
        print("\n1. Processing UTFX data...")
        tbl_summary = process_utfx(macro_vars)
        if tbl_summary is not None:
            print(f"   UTFX summary: {len(tbl_summary)} rows")
            append_or_create_table(tbl_summary, BNMK_DIR, macro_vars, 'TBL1')
        
        # Process K3TBL
        print("\n2. Processing K3TBL data...")
        k3tbl_df = process_k3tbl(macro_vars)
        if k3tbl_df is not None:
            print(f"   K3TBL: {len(k3tbl_df)} rows")
        
        # Process K3TBL1 for REP2 and REP3
        print("\n3. Processing K3TBL1 for REP2/REP3...")
        rep2_df, rep3_df = process_k3tbl1(k3tbl_df, macro_vars)
        if rep2_df is not None:
            print(f"   REP2: {len(rep2_df)} rows")
            append_or_create_table(rep2_df, BNMK_DIR, macro_vars, 'REP2')
        if rep3_df is not None:
            print(f"   REP3: {len(rep3_df)} rows")
            append_or_create_table(rep3_df, BNMK_DIR, macro_vars, 'REP3')
        
        # Process PROVALUE
        print("\n4. Processing PROVALUE...")
        provalue_df = process_provalue(macro_vars)
        if provalue_df is not None:
            print(f"   PROVALUE: {len(provalue_df)} rows")
        
        # Process K3TBLF for REP4
        print("\n5. Processing K3TBLF for REP4...")
        rep4_df = process_k3tblf(k3tbl_df, provalue_df, macro_vars)
        if rep4_df is not None:
            print(f"   REP4: {len(rep4_df)} rows")
            append_or_create_table(rep4_df, BNMK_DIR, macro_vars, 'REP4')
        
        # ===== PART 2: Islamic Securities =====
        print("\n" + "="*60)
        print("PART 2: Islamic Securities")
        print("="*60)
        
        # Process IUTFX -> TBLI -> TBL1I
        print("\n1. Processing IUTFX data...")
        tbli_summary = process_iutfx(macro_vars)
        if tbli_summary is not None:
            print(f"   IUTFX summary: {len(tbli_summary)} rows")
            append_or_create_table(tbli_summary, BNMKI_DIR, macro_vars, 'TBL1')
        
        # Process K3TBLI
        print("\n2. Processing K3TBLI data...")
        k3tbli_df = process_k3tbli(macro_vars)
        if k3tbli_df is not None:
            print(f"   K3TBLI: {len(k3tbli_df)} rows")
        
        # Process K3TBL1I for REP2I and REP3I
        print("\n3. Processing K3TBL1I for REP2I/REP3I...")
        rep2i_df, rep3i_df = process_k3tbl1i(k3tbli_df, macro_vars)
        if rep2i_df is not None:
            print(f"   REP2I: {len(rep2i_df)} rows")
            append_or_create_table(rep2i_df, BNMKI_DIR, macro_vars, 'REP2')
        if rep3i_df is not None:
            print(f"   REP3I: {len(rep3i_df)} rows")
            append_or_create_table(rep3i_df, BNMKI_DIR, macro_vars, 'REP3')
        
        # Process K3TBLFI for REP4I
        print("\n4. Processing K3TBLFI for REP4I (standard)...")
        rep4i_df = process_k3tblfi(k3tbli_df, macro_vars, variant='standard')
        if rep4i_df is not None:
            print(f"   REP4I: {len(rep4i_df)} rows")
            append_or_create_table(rep4i_df, BNMKI_DIR, macro_vars, 'REP4')
        
        # Process K3TBLFI for REP4X
        print("\n5. Processing K3TBLFI for REP4X (alternative)...")
        rep4x_df = process_k3tblfi(k3tbli_df, macro_vars, variant='alternative')
        if rep4x_df is not None:
            print(f"   REP4X: {len(rep4x_df)} rows")
            append_or_create_table(rep4x_df, BNMKI_DIR, macro_vars, f"REP4X{macro_vars['REPTYEAR']}")
        
        # ===== Summary =====
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        print(f"\nOutput files saved in:")
        print(f"  - Conventional: {BNMK_DIR}")
        print(f"  - Islamic: {BNMKI_DIR}")
        print(f"\nELBATCH value: {macro_vars.get('ELBATCH', 'Not set')}")
        
        # List output files
        print("\nGenerated files:")
        for dir_path in [BNMK_DIR, BNMKI_DIR]:
            if dir_path.exists():
                files = list(dir_path.glob("*.parquet"))
                for f in sorted(files):
                    print(f"  {dir_path.name}/{f.name}")
        
        print(f"\nProcessing completed successfully at {datetime.now()}")
        
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
