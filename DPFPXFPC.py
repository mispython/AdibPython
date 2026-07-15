import polars as pl
import pyreadstat
from datetime import datetime, timedelta

# Hardcode reptdate as yesterday
reptdate = datetime.now().date() - timedelta(days=1)

# SAS dataset paths
CISAFD_DEPOSIT = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDFD1M/deposit.sas7bdat'
FD_FD = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQDISE/fd.sas7bdat'
OVER1M = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDFD1M/over1m.txt'

# Read SAS datasets using pyreadstat - limit to 10000 rows for testing
df_cisfd, meta_cisfd = pyreadstat.read_sas7bdat(CISAFD_DEPOSIT, row_limit=10000)
df_cisfd = pl.from_pandas(df_cisfd)
df_cisfd = df_cisfd.filter(pl.col('SECCUST') == '901').select([
    'ACCTNO', 'CUSTNAM1', 'NEWIC', 'OLDIC', 'BUSSREG', 'CUSTNO', 'SECCUST'
]).sort('ACCTNO').rename({'CUSTNAM1': 'NAME'})

df_fd, meta_fd = pyreadstat.read_sas7bdat(FD_FD, row_limit=10000)
df_fd = pl.from_pandas(df_fd)
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

# Fix LMATDATE parsing - handle it as a numeric with possible decimal places
def parse_lmatdate(val):
    if val == 0 or val is None:
        return None
    # Convert to string and handle decimal places
    val_str = str(val)
    # If it has a decimal, split and take only the integer part
    if '.' in val_str:
        val_str = val_str.split('.')[0]
    # Ensure we have exactly 8 characters (some might be missing leading zeros)
    if len(val_str) < 8:
        val_str = val_str.zfill(8)
    # Take only the last 8 characters in case it's longer
    if len(val_str) > 8:
        val_str = val_str[:8]
    try:
        # Format is MMDDYYYY (8 digits)
        month = int(val_str[0:2])
        day = int(val_str[2:4])
        year = int(val_str[4:8])
        return datetime(year, month, day).date()
    except:
        return None

df_fd = df_fd.with_columns([
    pl.col('LMATDATE').map_elements(parse_lmatdate, return_dtype=pl.Date).alias('LASTMAT')
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

# Read FD again for total calculation - limit to 10000 rows
df_fd_all, meta_fd_all = pyreadstat.read_sas7bdat(FD_FD, row_limit=10000)
df_fd_all = pl.from_pandas(df_fd_all)
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
