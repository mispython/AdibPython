import polars as pl
from pathlib import Path
import datetime
from dateutil.relativedelta import relativedelta

def process_eiikalwe():
    """Simple and compact EIIKALWE conversion"""
    
    # Read REPTDATE from first line of BNMTBL3
    with open('BNMTBL3', 'r') as f:
        first_line = f.readline().strip()
    
    # Parse REPTDATE (assuming format: YYYYMMDD)
    reptdate = datetime.datetime.strptime(first_line.split('|')[0], '%Y%m%d').date()
    
    # Determine week based on day
    day = reptdate.day
    if 1 <= day <= 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif 9 <= day <= 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif 16 <= day <= 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
    
    mm = reptdate.month
    mm1 = mm
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    
    # Set macro variables
    NOWK = wk
    REPTYEAR = str(reptdate.year)
    REPTMON = f"{mm:02d}"
    REPTDAY = f"{day:02d}"
    RDATE = reptdate.strftime('%d%m%y')
    REPTDATE_STR = str(reptdate)
    
    # Read K3TBL data
    df = pl.read_csv(
        'BNMTBL3',
        separator='|',
        has_header=False,
        null_values=['', ' '],
        dtypes={
            'column_0': pl.Utf8,    # First column (date) is actually parsed above
            'column_1': pl.Utf8,    # UTSTY
            'column_2': pl.Utf8,    # UTREF
            'column_3': pl.Utf8,    # UTBRNM
            'column_4': pl.Utf8,    # UTDLP
            'column_5': pl.Utf8,    # UTDLR
            'column_6': pl.Utf8,    # UTSMN
            'column_7': pl.Utf8,    # UTCUS
            'column_8': pl.Utf8,    # UTCLC
            'column_9': pl.Utf8,    # UTCTP
            'column_10': pl.Float64,  # UTFCV
            'column_11': pl.Utf8,     # UTIDT
            'column_12': pl.Utf8,     # UTLCD
            'column_13': pl.Utf8,     # UTNCD
            'column_14': pl.Utf8,     # UTMDT
            'column_15': pl.Utf8,     # UTCBD
            'column_16': pl.Float64,  # UTCPR
            'column_17': pl.Float64,  # UTQDS
            'column_18': pl.Float64,  # UTPCP
            'column_19': pl.Float64,  # UTAMOC
            'column_20': pl.Float64,  # UTDPF
            'column_21': pl.Float64,  # UTAICT
            'column_22': pl.Float64,  # UTAICY
            'column_23': pl.Float64,  # UTAIT
            'column_24': pl.Float64,  # UTDPET
            'column_25': pl.Float64,  # UTDPEY
            'column_26': pl.Float64,  # UTDPE
            'column_27': pl.Int64,    # UTASN
            'column_28': pl.Utf8,     # UTOSD
            'column_29': pl.Utf8,     # UTCA2
            'column_30': pl.Utf8,     # UTSAC
            'column_31': pl.Utf8,     # UTCNAP
            'column_32': pl.Utf8,     # UTCNAR
            'column_33': pl.Utf8,     # UTCNAL
            'column_34': pl.Utf8,     # UTCCY
            'column_35': pl.Float64,  # UTAMTS
            'column_36': pl.Utf8,     # UTMM1
        }
    )
    
    # Rename columns to match SAS variables
    column_names = [
        'REPTDATE_RAW', 'UTSTY', 'UTREF', 'UTBRNM', 'UTDLP', 'UTDLR', 'UTSMN',
        'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD', 'UTMDT',
        'UTCBD', 'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF', 'UTAICT',
        'UTAICY', 'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN', 'UTOSD',
        'UTCA2', 'UTSAC', 'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY', 'UTAMTS', 'UTMM1'
    ]
    df = df.rename(dict(zip(df.columns, column_names)))
    
    # Skip first row as we already parsed the date separately
    df = df.slice(1)
    
    # Add parsed REPTDATE column
    df = df.with_columns(pl.lit(reptdate).alias('REPTDATE'))
    
    # Apply transformations
    df = df.with_columns([
        # Delete if UTDLP 2nd/3rd chars are 'RT' or 'RI'
        pl.when(
            pl.col('UTDLP').str.slice(1, 2).is_in(['RT', 'RI'])
        ).then(pl.lit(True)).otherwise(pl.lit(False)).alias('to_delete')
    ]).filter(pl.col('to_delete') == False).drop('to_delete')
    
    # Parse dates
    for date_col, new_col in [
        ('UTMDT', 'MATDT'),
        ('UTOSD', 'ISSDT'),
        ('UTCBD', 'REPTDATE2'),
        ('UTCBD', 'DDATE'),
        ('UTIDT', 'XDATE')
    ]:
        df = df.with_columns(
            pl.when(pl.col(date_col) != '')
            .then(
                pl.col(date_col).str.strptime(pl.Date, format='%d/%m/%Y', strict=False)
            ).alias(new_col)
        )
    
    # Filter based on UTSTY
    df = df.with_columns([
        pl.when(
            pl.col('UTSTY').is_in(['IDC', 'IDP', 'ABP', 'ABS']) &
            (pl.col('XDATE') > pl.col('DDATE'))
        ).then(pl.lit(True)).otherwise(pl.lit(False)).alias('filter_out')
    ]).filter(pl.col('filter_out') == False).drop('filter_out')
    
    # Add EXTDATE
    df = df.with_columns(pl.lit(datetime.date.today()).alias('EXTDATE'))
    
    # Handle existing file - delete today's records if file exists
    output_file = Path(f'BNMKD/K3TBL{REPTMON}{NOWK}.parquet')
    
    if output_file.exists():
        existing_df = pl.read_parquet(output_file)
        # Remove records with today's EXTDATE
        existing_df = existing_df.filter(
            pl.col('EXTDATE') != datetime.date.today()
        )
        # Append new data
        final_df = pl.concat([existing_df, df])
    else:
        final_df = df
    
    # Save to parquet
    output_file.parent.mkdir(parents=True, exist_ok=True)
    final_df.write_parquet(output_file)
    
    print(f"Processed {len(df)} records")
    print(f"Saved to: {output_file}")
    
    return {
        'NOWK': NOWK,
        'REPTYEAR': REPTYEAR,
        'REPTMON': REPTMON,
        'REPTDAY': REPTDAY,
        'RDATE': RDATE,
        'REPTDATE': REPTDATE_STR,
        'dataframe': df
    }

# Run the processing
if __name__ == "__main__":
    result = process_eiikalwe()
    print(f"Week: {result['NOWK']}, Month: {result['REPTMON']}, Year: {result['REPTYEAR']}")
