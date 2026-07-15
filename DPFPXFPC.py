import polars as pl
from datetime import datetime, timedelta

FD_REPTDATE = 'data/fd/reptdate.parquet'
CISAFD_DEPOSIT = 'data/cisafd/deposit.parquet'
FD_FD = 'data/fd/fd.parquet'
OVER1M = 'data/over1m.txt'

df_reptdate = pl.read_parquet(FD_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

df_cisfd = pl.read_parquet(CISAFD_DEPOSIT)
df_cisfd = df_cisfd.filter(pl.col('SECCUST') == '901').select([
    'ACCTNO', 'CUSTNAM1', 'NEWIC', 'OLDIC', 'BUSSREG', 'CUSTNO', 'SECCUST'
]).sort('ACCTNO').rename({'CUSTNAM1': 'NAME'})

df_fd = pl.read_parquet(FD_FD)
df_fd = df_fd.filter(
    (pl.col('CURBAL') > 0) & 
    (~pl.col('CUSTCD').is_in([77, 78, 95, 96])) & 
    (pl.col('CURCODE') == 'MYR')
)

df_fd = df_fd.with_columns([
    pl.lit(reptdate).alias('REPTDATE')
])

def parse_matdate(matdate_val):
    matdate_str = str(int(matdate_val)).zfill(8)
    year = int(matdate_str[0:4])
    month = int(matdate_str[4:6])
    day = int(matdate_str[6:8])
    return datetime(year, month, day)

df_fd = df_fd.with_columns([
    pl.col('MATDATE').map_elements(parse_matdate, return_dtype=pl.Datetime).dt.date().alias('MATDT')
])

df_fd = df_fd.with_columns([
    (pl.col('MATDT').cast(pl.Date) - pl.lit(reptdate).cast(pl.Date)).dt.total_days().alias('MATURITY')
])

df_fd = df_fd.with_columns([
    pl.when(pl.col('LMATDATE') != 0)
      .then(
          pl.col('LMATDATE').cast(pl.Utf8).str.slice(0, 8).map_elements(
              lambda x: datetime.strptime(x, '%m%d%Y').date() if len(x) == 8 else None,
              return_dtype=pl.Date
          )
      )
      .otherwise(pl.lit(None))
      .alias('LASTMAT')
])

df_fd = df_fd.with_columns([
    pl.when(pl.col('LASTMAT') == reptdate)
      .then(pl.lit(0))
      .otherwise(pl.col('MATURITY'))
      .alias('MATURITY'),
    pl.when(pl.col('LASTMAT') == reptdate)
      .then(pl.col('LASTMAT'))
      .otherwise(pl.col('MATDT'))
      .alias('MATDT')
])

df_fd = df_fd.with_columns([
    pl.when(pl.col('MATURITY') == 0)
      .then(pl.lit('(T)'))
      .otherwise(pl.concat_str([
          pl.lit('(T+'),
          pl.col('MATURITY').cast(pl.Utf8),
          pl.lit(')')
      ]))
      .alias('TMATURITY')
])

df_fd = df_fd.with_columns([
    (pl.col('CURBAL') * pl.col('RATE')).alias('RATEBAL')
])

df_fd = df_fd.drop('NAME')

df_fd = df_fd.join(df_cisfd, on='ACCTNO', how='inner')

df_fd = df_fd.with_columns([
    pl.when(pl.col('BUSSREG').str.strip_chars() != '')
      .then(pl.col('BUSSREG'))
      .when(pl.col('NEWIC').str.strip_chars() != '')
      .then(pl.col('NEWIC'))
      .when(pl.col('OLDIC').str.strip_chars() != '')
      .then(pl.col('OLDIC'))
      .otherwise(pl.col('CUSTNO'))
      .alias('CUSTID')
])

df_fd = df_fd.sort('MATDT')

df_fd = df_fd.sort(['CUSTID', 'MATDT'])

df_fdtotal = df_fd.group_by(['CUSTID', 'MATDT']).agg([
    pl.col('CURBAL').sum().alias('TOTAL'),
    pl.col('RATEBAL').sum().alias('TOTRATEBAL')
])

df_fd_unique = df_fd.unique(subset=['CUSTID', 'MATDT'], keep='first')

df_fdtotal = df_fd_unique.join(df_fdtotal, on=['CUSTID', 'MATDT'], how='inner')

df_fdtotal = df_fdtotal.select([
    'NAME', 'TOTAL', 'MATDT', 'MATURITY', 'TMATURITY', 'REPTDATE', 'TOTRATEBAL'
])

df_fdtotal = df_fdtotal.with_columns([
    (pl.col('TOTRATEBAL') / pl.col('TOTAL')).round(2).alias('AVGRATE')
])

df_fd_all = pl.read_parquet(FD_FD)
df_fd_all = df_fd_all.filter(
    (pl.col('CURBAL') > 0) & 
    (~pl.col('CUSTCD').is_in([77, 78, 95, 96])) & 
    (pl.col('CURCODE') == 'MYR')
)

df_fd_all = df_fd_all.with_columns([
    (pl.col('CURBAL') * pl.col('RATE')).alias('RATEBAL')
])

df_totalfd = df_fd_all.select([
    pl.col('CURBAL').sum(),
    pl.col('RATEBAL').sum()
])

df_totalfd = df_totalfd.with_columns([
    (pl.col('RATEBAL') / pl.col('CURBAL')).round(2).alias('AVGRATE')
])

lines = []
lines.append(' ')
lines.append('TOTAL NON-INDI FD (MIL);AVERAGE RATE;')

total_fd = df_totalfd.to_dicts()[0]
curbal1 = round(total_fd['CURBAL'] / 1000000, 3)
lines.append(f"{curbal1:.3f};{total_fd['AVGRATE']:.2f};")

df_mature = df_fdtotal.with_columns([
    (pl.col('TOTAL') / 1000000).round(3).alias('TOTAL1'),
    pl.concat_str([
        pl.col('MATDT').dt.strftime('%d/%m/%Y'),
        pl.col('TMATURITY')
    ]).alias('REPTDATE1')
])

df_mature_summary = df_mature.filter(
    (pl.col('MATURITY') >= 0) & (pl.col('MATURITY') < 8)
).group_by('REPTDATE1').agg([
    pl.col('TOTAL').sum()
])

lines.append(' ')
lines.append('MATURITY DATE;TOTAL NON-INDI FD (MIL);')

for row in df_mature_summary.iter_rows(named=True):
    curbal1 = round(row['TOTAL'] / 1000000, 3)
    lines.append(f"{row['REPTDATE1']};{curbal1:.3f};")

for i in range(8):
    df_mature_day = df_mature.filter(pl.col('MATURITY') == i)
    
    if len(df_mature_day) > 0:
        df_mature_day = df_mature_day.sort('TOTAL', descending=True)
        
        first_row = df_mature_day.to_dicts()[0]
        tmat = first_row['TMATURITY']
        date_str = first_row['MATDT'].strftime('%d/%m/%Y')
        
        lines.append(' ')
        lines.append(' ')
        lines.append(' ')
        lines.append(f"RM NON-INDI FD DETAILS BY CUSTOMER MATURING {date_str} {tmat}")
        lines.append(' ')
        lines.append('CUSTOMER;SETTLEMENT AMOUNT(MIL.);MATURITY DATE;AVERAGE RATE;')
        
        for row in df_mature_day.iter_rows(named=True):
            lines.append(
                f"{row['NAME']};{row['TOTAL1']:.3f};{row['REPTDATE1']};{row['AVGRATE']:.2f};"
            )

with open(OVER1M, 'w') as f:
    for line in lines:
        f.write(line + '\n')

print(f"Report generated: {OVER1M}")
