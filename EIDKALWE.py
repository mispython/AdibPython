import polars as pl
from pathlib import Path
import datetime

INPUT_BASE_PATH = Path(r"C:\Your\Input\Path")
OUTPUT_BASE_PATH = Path(r"C:\Your\Output\Path")

def process_eidkalwe():
    with open(INPUT_BASE_PATH / "BNMTBL3", 'r') as f:
        first_line = f.readline().strip()
    
    date_str = first_line.split('|')[0]
    reptdate = datetime.datetime.strptime(date_str, '%Y%m%d').date()
    
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
    
    NOWK = wk
    REPTYEAR = str(reptdate.year)
    REPTMON = f"{mm:02d}"
    REPTDAY = f"{day:02d}"
    RDATE = reptdate.strftime('%d%m%y')
    
    df = pl.read_csv(
        INPUT_BASE_PATH / "BNMTBL3",
        separator='|',
        has_header=False,
        null_values=['', ' '],
        dtypes={
            'column_0': pl.Utf8,
            'column_1': pl.Utf8, 'column_2': pl.Utf8, 'column_3': pl.Utf8,
            'column_4': pl.Utf8, 'column_5': pl.Utf8, 'column_6': pl.Utf8,
            'column_7': pl.Utf8, 'column_8': pl.Utf8, 'column_9': pl.Float64,
            'column_10': pl.Utf8, 'column_11': pl.Utf8, 'column_12': pl.Utf8,
            'column_13': pl.Utf8, 'column_14': pl.Utf8, 'column_15': pl.Float64,
            'column_16': pl.Float64, 'column_17': pl.Float64, 'column_18': pl.Float64,
            'column_19': pl.Float64, 'column_20': pl.Float64, 'column_21': pl.Float64,
            'column_22': pl.Float64, 'column_23': pl.Float64, 'column_24': pl.Float64,
            'column_25': pl.Int64, 'column_26': pl.Utf8, 'column_27': pl.Utf8,
            'column_28': pl.Utf8, 'column_29': pl.Utf8, 'column_30': pl.Utf8,
            'column_31': pl.Utf8, 'column_32': pl.Utf8, 'column_33': pl.Float64,
        }
    )
    
    column_names = [
        'REPTDATE_RAW', 'UTSTY', 'UTREF', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS',
        'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD', 'UTMDT', 'UTCBD',
        'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF', 'UTAICT', 'UTAICY',
        'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN', 'UTOSD', 'UTCA2', 'UTSAC',
        'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY', 'UTAMTS'
    ]
    
    df = df.rename(dict(zip(df.columns, column_names)))
    df = df.slice(1)
    df = df.with_columns(pl.lit(reptdate).alias('REPTDATE'))
    
    df = df.with_columns([
        pl.when(pl.col('UTDLP').str.slice(1, 2).is_in(['RT', 'RI']))
        .then(pl.lit(True)).otherwise(pl.lit(False)).alias('to_delete')
    ]).filter(pl.col('to_delete') == False).drop('to_delete')
    
    for date_col, new_col in [
        ('UTMDT', 'MATDT'),
        ('UTOSD', 'ISSDT'),
        ('UTCBD', 'REPTDATE2'),
        ('UTCBD', 'DDATE'),
        ('UTIDT', 'XDATE')
    ]:
        df = df.with_columns(
            pl.when(pl.col(date_col) != '')
            .then(pl.col(date_col).str.strptime(pl.Date, format='%d/%m/%Y', strict=False))
            .alias(new_col)
        )
    
    df = df.with_columns([
        pl.when(pl.col('UTSTY') == 'IZD')
        .then(pl.col('UTQDS'))
        .otherwise(pl.col('UTCPR'))
        .alias('UTCPR')
    ])
    
    df = df.with_columns([
        pl.when(
            pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD']) &
            (pl.col('XDATE') > pl.col('DDATE'))
        ).then(pl.lit(True)).otherwise(pl.lit(False)).alias('filter_out')
    ]).filter(pl.col('filter_out') == False).drop('filter_out')
    
    df = df.with_columns(pl.lit(datetime.date.today()).alias('EXTDATE'))
    
    output_file = OUTPUT_BASE_PATH / f"BNMKD/K3TBL{REPTMON}{NOWK}.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if output_file.exists():
        existing_df = pl.read_parquet(output_file)
        existing_df = existing_df.filter(pl.col('EXTDATE') != datetime.date.today())
        final_df = pl.concat([existing_df, df])
    else:
        final_df = df
    
    final_df.write_parquet(output_file, compression="zstd")
    
    return final_df

if __name__ == "__main__":
    result_df = process_eidkalwe()
