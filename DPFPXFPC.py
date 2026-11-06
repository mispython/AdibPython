import polars as pl
import pyreadstat
from datetime import datetime, date, timedelta

# Hardcode reptdate as yesterday
reptdate = datetime.now().date() - timedelta(days=1)

# Format reptday for the dynamic filename (e.g. '07' for day 7)
reptday = reptdate.strftime('%d')

# SAS dataset paths
CISAFD_DEPOSIT = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDFD1M/cisinfo.sas7bdat'
FD_FD = f'/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDFD1M/enrh_dp_fd_cert_d{reptday}.sas7bdat'
OVER1M = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDFD1M/over1m.txt'

# Testing row limit
ROW_LIMIT = 100000

# Chunk size for streaming the huge customer info file
CHUNK_SIZE = 50000


def read_sas_chunked(path, columns=None, row_limit=None, chunk_size=50000):
    """Read a SAS7BDAT file in chunks and return a single Polars DataFrame."""
    chunks = []
    rows_read = 0
    row_offset = 0

    while True:
        if row_limit is not None:
            remaining = row_limit - rows_read
            if remaining <= 0:
                break
            current_limit = min(chunk_size, remaining)
        else:
            current_limit = chunk_size

        try:
            df_chunk, _ = pyreadstat.read_sas7bdat(
                path,
                row_offset=row_offset,
                row_limit=current_limit,
                usecols=columns,
            )
        except Exception:
            break

        if df_chunk is None or len(df_chunk) == 0:
            break

        chunks.append(pl.from_pandas(df_chunk))
        rows_read += len(df_chunk)
        row_offset += len(df_chunk)

        # If we got fewer rows than requested, we've hit EOF
        if len(df_chunk) < current_limit:
            break

    if not chunks:
        return pl.DataFrame()

    return pl.concat(chunks, how="vertical_relaxed")


# --- Customer info: read in chunks, only needed columns ---
df_cisfd = read_sas_chunked(
    CISAFD_DEPOSIT,
    columns=['ACCTNO', 'CUSTNAME', 'NEWIC', 'OLDIC', 'BUSSREG', 'CUSTNO', 'SECCUST'],
    row_limit=ROW_LIMIT,
    chunk_size=CHUNK_SIZE,
)
df_cisfd = df_cisfd.filter(pl.col('SECCUST') == '901').select([
    'ACCTNO', 'CUSTNAME', 'NEWIC', 'OLDIC', 'BUSSREG', 'CUSTNO', 'SECCUST'
]).sort('ACCTNO').rename({'CUSTNAME': 'NAME'})

# --- FD file ---
df_fd, meta_fd = pyreadstat.read_sas7bdat(FD_FD, row_limit=ROW_LIMIT)
df_fd = pl.from_pandas(df_fd)
df_fd = df_fd.filter(
    (pl.col('CURR_BAL') > 0) &
    (~pl.col('CUSTOMER_CD').is_in([77, 78, 95, 96])) &
    (pl.col('CURRENCY_CD') == 'MYR')
)

df_fd = df_fd.with_columns([
    pl.lit(reptdate).alias('REPTDATE')
])


# --- Robust date parser: handles datetime/date already converted by pyreadstat,
#     plus numeric YYYYMMDD strings, and nulls ---
def to_date(val):
    """Convert a pyreadstat value to a Python date, or None."""
    if val is None:
        return None
    # Already a datetime / date (pyreadstat converts SAS dates automatically)
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    # pandas NaT / NaN
    try:
        if pl.Series([val]).is_null().item():
            return None
    except Exception:
        pass
    # Numeric / string path (assume YYYYMMDD)
    try:
        val_str = str(val)
        if '.' in val_str:
            val_str = val_str.split('.')[0]
        if len(val_str) < 8:
            val_str = val_str.zfill(8)
        if len(val_str) > 8:
            val_str = val_str[:8]
        year = int(val_str[0:4])
        month = int(val_str[4:6])
        day = int(val_str[6:8])
        return datetime(year, month, day).date()
    except Exception:
        return None


# --- MATURE_DT -> MATDT ---
df_fd = df_fd.with_columns([
    pl.col('MATURE_DT').map_elements(to_date, return_dtype=pl.Date).alias('MATDT')
])

df_fd = df_fd.with_columns([
    (pl.col('MATDT').cast(pl.Date) - pl.lit(reptdate).cast(pl.Date)).dt.total_days().alias('MATURITY')
])

# --- LAST_MATURE_DT -> LASTMAT ---
df_fd = df_fd.with_columns([
    pl.col('LAST_MATURE_DT').map_elements(to_date, return_dtype=pl.Date).alias('LASTMAT')
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
    (pl.col('CURR_BAL') * pl.col('RT')).alias('RATEBAL')
])

# Rename FD account column before joining (FD uses ACCT_NUM, CIS uses ACCTNO)
df_fd = df_fd.rename({'ACCT_NUM': 'ACCTNO'})
df_fd = df_fd.join(df_cisfd, on='ACCTNO', how='inner')

# CUSTID resolution: BUSSREG -> NEWIC -> OLDIC -> CUSTNO  (matches original SAS)
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
    pl.col('CURR_BAL').sum().alias('TOTAL'),
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

# Read FD again for total calculation
df_fd_all, meta_fd_all = pyreadstat.read_sas7bdat(FD_FD, row_limit=ROW_LIMIT)
df_fd_all = pl.from_pandas(df_fd_all)
df_fd_all = df_fd_all.filter(
    (pl.col('CURR_BAL') > 0) &
    (~pl.col('CUSTOMER_CD').is_in([77, 78, 95, 96])) &
    (pl.col('CURRENCY_CD') == 'MYR')
)

df_fd_all = df_fd_all.with_columns([
    (pl.col('CURR_BAL') * pl.col('RT')).alias('RATEBAL')
])

df_totalfd = df_fd_all.select([
    pl.col('CURR_BAL').sum(),
    pl.col('RATEBAL').sum()
])

df_totalfd = df_totalfd.with_columns([
    (pl.col('RATEBAL') / pl.col('CURR_BAL')).round(2).alias('AVGRATE')
])

lines = []
lines.append(' ')
lines.append('TOTAL NON-INDI FD (MIL);AVERAGE RATE;')

total_fd = df_totalfd.to_dicts()[0]
curbal1 = round(total_fd['CURR_BAL'] / 1000000, 3)
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
