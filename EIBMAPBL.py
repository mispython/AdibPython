import polars as pl
from datetime import datetime, timedelta

PBIF_REPTDATE = 'data/pbif/reptdate.parquet'
PBIF_DIR = 'data/pbif/'
PBIFNLF = 'data/pbifnlf.txt'

df_reptdate = pl.read_parquet(PBIF_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

reptq = 'Y' if (reptdate + timedelta(days=1)).day == 1 else 'N'

day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

rdatx = int(reptdate.strftime('%y%j'))
reptyr = reptdate.year
reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')
mdate = int(reptdate.strftime('%y%j'))

rpyr = reptdate.year
rpmth = int(reptdate.strftime('%m'))
rpday = reptdate.day

def get_days_in_month(year, month):
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

def calculate_remmth(matdte, reptdate_val):
    mdyr = matdte.year
    mdmth = matdte.month
    mdday = matdte.day
    
    rpyr_val = reptdate_val.year
    rpmth_val = reptdate_val.month
    rpday_val = reptdate_val.day
    
    rpdays_current = get_days_in_month(rpyr_val, rpmth_val)
    
    if mdday > rpday_val:
        mdday_adj = rpday_val
    else:
        mdday_adj = mdday
    
    remy = mdyr - rpyr_val
    remm = mdmth - rpmth_val
    remd = mdday_adj - rpday_val
    
    return remy * 12 + remm + remd / rpdays_current

def categorize_remmth(remmth):
    if remmth < 0.255:
        return 'UP TO 1 WK'
    elif remmth < 1:
        return '>1 WK - 1 MTH'
    elif remmth < 3:
        return '>1 MTH - 3 MTHS'
    elif remmth < 6:
        return '>3 - 6 MTHS'
    elif remmth < 12:
        return '>6 MTHS - 1 YR'
    elif remmth < 36:
        return '>1 YR  - 3 YR'
    elif remmth < 60:
        return '>3 YR  - 5 YR'
    else:
        return '>5 YR'

if reptq == 'Y':
    df_pbif = pl.read_parquet(f'{PBIF_DIR}rdlmpbif.parquet')
else:
    df_pbif = pl.read_parquet(f'{PBIF_DIR}rdalpbif.parquet')

df_pbif = df_pbif.with_columns([
    (pl.col('MATDTE').cast(pl.Date) - pl.lit(reptdate).cast(pl.Date)).dt.total_days().alias('DAYS')
])

df_pbif = df_pbif.with_columns([
    pl.when(pl.col('DAYS') < 8)
      .then(pl.lit(0.1))
      .otherwise(
          pl.struct(['MATDTE']).map_elements(
              lambda x: calculate_remmth(x['MATDTE'], reptdate),
              return_dtype=pl.Float64
          )
      )
      .alias('REMMTH')
])

df_pbif = df_pbif.with_columns([
    pl.when(pl.col('CUSTCX').is_in(['77', '78', '95', '96']))
      .then(pl.lit('A1.08'))
      .otherwise(pl.lit('A1.04'))
      .alias('ITEM'),
    pl.col('BALANCE').alias('AMOUNT')
])

records_part2 = []
records_part1 = []

for row in df_pbif.iter_rows(named=True):
    records_part2.append({
        'PART': '2-RM',
        'ITEM': row['ITEM'],
        'REMMTH': row['REMMTH'],
        'AMOUNT': row['AMOUNT']
    })
    
    records_part1.append({
        'PART': '1-RM',
        'ITEM': row['ITEM'],
        'REMMTH': row['REMMTH'],
        'AMOUNT': row['AMOUNT']
    })
    
    undrawn_amt = row['INLIMIT'] - row['BALANCE']
    if undrawn_amt < 0:
        undrawn_amt = 0.00
    
    records_part2.append({
        'PART': '2-RM',
        'ITEM': 'A1.28',
        'REMMTH': row['REMMTH'],
        'AMOUNT': undrawn_amt
    })
    
    undrawn_part1 = undrawn_amt * 0.20
    if undrawn_part1 < 0:
        undrawn_part1 = 0.00
    
    records_part1.append({
        'PART': '1-RM',
        'ITEM': 'A1.28',
        'REMMTH': row['REMMTH'],
        'AMOUNT': undrawn_part1
    })

df_pbif_output = pl.DataFrame(records_part2 + records_part1)

df_pbif_summary = df_pbif_output.group_by(['PART', 'ITEM', 'REMMTH']).agg([
    pl.col('AMOUNT').sum()
])

df_pbif2 = df_pbif_summary.filter(
    (pl.col('PART') == '2-RM') & 
    (pl.col('ITEM') == 'A1.04')
)

df_pbif2 = df_pbif2.with_columns([
    pl.col('REMMTH').map_elements(categorize_remmth, return_dtype=pl.Utf8).alias('REMMTH_FMT')
])

df_pbif3 = df_pbif2.group_by(['ITEM', 'REMMTH_FMT']).agg([
    pl.col('AMOUNT').sum()
])

item_labels = {
    'A1.01': 'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02': 'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03': 'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04': 'A1.04  LOANS: CORP - OTHERS',
    'A1.05': 'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07': 'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08': 'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.12': 'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13': 'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14': 'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15': 'A1.15  DEPOSITS: IND  - FIXED',
    'A1.16': 'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17': 'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.25': 'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.28': 'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A2.01': 'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02': 'A2.02  REVERSE REPO',
    'A2.03': 'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04': 'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05': 'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06': 'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08': 'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09': 'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14': 'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15': 'A2.15  INTERBANK REPOS',
    'A2.16': 'A2.16  NON-INTERBANK REPOS',
    'A2.17': 'A2.17  NIDS ISSUED',
    'A2.18': 'A2.18  BAS PAYABLE',
    'A2.19': 'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.12': 'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15': 'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01': 'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09': 'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14': 'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19': 'B2.19  FX EXCHG CONTRACTS PAYABLE'
}

with open(PBIFNLF, 'w') as f:
    f.write(f"NLFPBIF{reptday}{reptmon}{reptyear}\n")
    
    for row in df_pbif3.iter_rows(named=True):
        item_label = item_labels.get(row['ITEM'], row['ITEM'])
        f.write(f"{item_label};{row['REMMTH_FMT']};{row['AMOUNT']:.2f};\n")

print(f"Report generated: {PBIFNLF}")
print(f"Total records: {len(df_pbif3)}")
