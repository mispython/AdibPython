import polars as pl
from datetime import datetime, date

DEPOSIT_REPTDATE = 'data/deposit/reptdate.parquet'
SIPCUST = 'data/sipcust.txt'
SIPPROD = 'data/sipprod.txt'
SIPCERT = 'data/sipcert.txt'
STORE_DIR = 'data/store/'
STOREM_DIR = 'data/storem/'

df_reptdate = pl.read_parquet(DEPOSIT_REPTDATE)
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

rpyr = reptdate.year
rpmth = reptdate.month
rpday = reptdate.day

def get_days_in_month(year, month):
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

def calculate_remmth(matdt, reptdate_val):
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    
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
    if remmth < 0.1:
        return '01'
    elif remmth < 1:
        return '02'
    elif remmth < 3:
        return '03'
    elif remmth < 6:
        return '04'
    elif remmth < 12:
        return '05'
    else:
        return '06'

cust_schema = {
    'CUSTID': (0, 2, str),
    'PRIMIC': (2, 17, str),
    'BRANCH': (17, 20, int),
    'BRABBR': (20, 35, str),
    'CUSTYPE': (35, 36, int),
    'NAME': (36, 156, str),
    'ICTYPE': (156, 158, str),
    'IC': (158, 173, str),
    'CUSTCODE': (173, 175, str),
    'CUSTDESC': (175, 275, str),
    'INVSTYPE': (275, 278, str),
    'INVSDESC': (278, 378, str),
    'INVSDES1': (378, 478, str),
    'HOMENO': (478, 490, str),
    'OFFNO': (490, 502, str),
    'MOBILE': (502, 514, str),
    'INVSNO': (514, 524, str),
    'DD1': (524, 526, int),
    'MM1': (527, 529, int),
    'YY1': (530, 534, int),
    'GEND': (534, 535, int),
    'ETHN': (535, 536, int),
    'MARI': (536, 537, int),
    'INCOME': (537, 617, str),
    'OCCUPT': (617, 697, str),
    'CERTNO': (697, 705, str)
}

def read_fixed_width_file(filepath, schema):
    records = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            record = {}
            for field, (start, end, dtype) in schema.items():
                value = line[start:end].strip() if end <= len(line) else ''
                if dtype == int:
                    try:
                        record[field] = int(value) if value else 0
                    except ValueError:
                        record[field] = 0
                elif dtype == float:
                    try:
                        record[field] = float(value) if value else 0.0
                    except ValueError:
                        record[field] = 0.0
                else:
                    record[field] = value.upper() if value else ''
            records.append(record)
    return pl.DataFrame(records)

df_cust = read_fixed_width_file(SIPCUST, cust_schema)
df_cust = df_cust.filter(pl.col('IC') != '')

df_cust = df_cust.with_columns([
    pl.when(pl.col('INVSTYPE') == 'OTH')
      .then(pl.col('INVSDES1'))
      .otherwise(pl.col('INVSDESC'))
      .alias('INVSDESC'),
    pl.when(pl.col('GEND') == 1)
      .then(pl.lit('MALE'))
      .otherwise(pl.lit('FEMALE'))
      .alias('GENDER'),
    pl.when(pl.col('ETHN') == 1)
      .then(pl.lit('MALAY'))
      .when(pl.col('ETHN') == 2)
      .then(pl.lit('CHINESE'))
      .when(pl.col('ETHN') == 3)
      .then(pl.lit('INDIAN'))
      .otherwise(pl.lit('OTHERS'))
      .alias('ETHNIC'),
    pl.when(pl.col('MARI') == 1)
      .then(pl.lit('SINGLE'))
      .when(pl.col('MARI') == 2)
      .then(pl.lit('MARRIED'))
      .when(pl.col('MARI') == 3)
      .then(pl.lit('DIVORCED'))
      .when(pl.col('MARI') == 4)
      .then(pl.lit('SEPARATED'))
      .when(pl.col('MARI') == 5)
      .then(pl.lit('WIDOW/WIDOWER'))
      .otherwise(pl.lit(' '))
      .alias('MARITAL'),
    pl.when(pl.col('MM1') > 0)
      .then(
          pl.struct(['YY1', 'MM1', 'DD1']).map_elements(
              lambda x: datetime(x['YY1'], x['MM1'], x['DD1']).date() if x['MM1'] > 0 and x['DD1'] > 0 else None,
              return_dtype=pl.Date
          )
      )
      .otherwise(pl.lit(None))
      .alias('BIRTHDT')
]).drop(['INVSDES1', 'DD1', 'MM1', 'YY1', 'GEND', 'ETHN', 'MARI'])

cert_schema = {
    'BRANCH': (0, 3, int),
    'BRABBR': (3, 18, str),
    'NAME': (18, 138, str),
    'CUSTYPE': (138, 141, int),
    'IC': (141, 156, str),
    'PRODKEY': (156, 163, str),
    'PRODUCT': (163, 166, int),
    'PRODNM': (166, 266, str),
    'ASETCODE': (266, 268, str),
    'TRANCNO': (268, 270, int),
    'DD1': (270, 272, int),
    'MM1': (273, 275, int),
    'YY1': (276, 280, int),
    'PRINAMT': (280, 297, float),
    'SOFUND': (297, 299, str),
    'FUNDET': (299, 399, str),
    'ADD1': (399, 439, str),
    'ADD2': (439, 479, str),
    'ADD3': (479, 519, str),
    'ADD4': (519, 559, str),
    'CERTNO': (559, 567, str),
    'INVSTAT': (567, 570, str),
    'EARAMT': (570, 587, float),
    'AVAIBAL': (587, 604, float),
    'UNSUREA': (604, 634, str),
    'DEBITBY': (634, 734, str),
    'DD2': (734, 736, int),
    'MM2': (737, 739, int),
    'YY2': (740, 744, int),
    'DEBITREA': (744, 844, str),
    'DD3': (844, 846, int),
    'MM3': (847, 849, int),
    'YY3': (850, 854, int),
    'PLAMT': (854, 871, float),
    'UNSUREA2': (871, 971, str),
    'STATUPBY': (971, 1071, str),
    'DD4': (1071, 1073, int),
    'MM4': (1074, 1076, int),
    'YY4': (1077, 1081, int),
    'REMARKS': (1081, 1181, str),
    'SWABBR': (1181, 1189, str),
    'SWAMT': (1189, 1206, float),
    'AMTFD': (1206, 1223, float),
    'AMTOTH': (1223, 1240, float),
    'AMTDEP': (1240, 1257, float),
    'SALENAME': (1257, 1266, str),
    'REDAMT': (1266, 1283, float),
    'OSREDAMT': (1283, 1300, float),
    'CREMTACC': (1300, 1310, int),
    'PLAGSNTLN': (1310, 1311, int),
    'TAGBY': (1311, 1411, str),
    'DD5': (1411, 1413, int),
    'MM5': (1414, 1416, int),
    'YY5': (1417, 1421, int),
    'DD6': (1421, 1423, int),
    'MM6': (1424, 1426, int),
    'YY6': (1427, 1431, int),
    'COUPAYAM': (1431, 1444, float),
    'OUTSDAMT': (1444, 1461, float),
    'POSTCODE': (1461, 1476, str),
    'STATE': (1476, 1526, str),
    'COUNTRY': (1526, 1576, str)
}

df_cert = read_fixed_width_file(SIPCERT, cert_schema)
df_cert = df_cert.filter(pl.col('IC') != '')

cert_date_mappings = [
    ('PLACEDT', 'MM1', 'DD1', 'YY1'),
    ('DEBITDT', 'MM2', 'DD2', 'YY2'),
    ('CREDITDT', 'MM3', 'DD3', 'YY3'),
    ('STATUPDT', 'MM4', 'DD4', 'YY4'),
    ('TAGDATE', 'MM5', 'DD5', 'YY5'),
    ('COUPAYDT', 'MM6', 'DD6', 'YY6')
]

for date_col, mm, dd, yy in cert_date_mappings:
    df_cert = df_cert.with_columns([
        pl.when(pl.col(mm) > 0)
          .then(
              pl.struct([yy, mm, dd]).map_elements(
                  lambda x: datetime(x[yy], x[mm], x[dd]).date() if x[mm] > 0 and x[dd] > 0 else None,
                  return_dtype=pl.Date
              )
          )
          .otherwise(pl.lit(None))
          .alias(date_col)
    ])

df_cert = df_cert.drop([col for _, mm, dd, yy in cert_date_mappings for col in [mm, dd, yy]])

df_cust01 = df_cust.filter(pl.col('INVSNO') != '').select(['IC', 'CUSTCODE', 'CUSTDESC', 'INVSTYPE', 'INVSDESC']).unique(subset=['IC'])

df_cert = df_cert.sort('IC').join(df_cust01.sort('IC'), on='IC', how='left')

df_cert01 = df_cert.select(['IC', 'ADD1', 'ADD2', 'ADD3', 'ADD4']).unique(subset=['IC'])

df_cust = df_cust.sort('IC').join(df_cert01.sort('IC'), on='IC', how='left')

df_cert = df_cert.drop(['ADD1', 'ADD2', 'ADD3', 'ADD4'])

df_default = pl.DataFrame({
    'BALANCE': [0, 0, 0, 0, 0, 0],
    'BNMCODE': [
        '9532900010000Y',
        '9532900020000Y',
        '9532900030000Y',
        '9532900040000Y',
        '9532900050000Y',
        '9532900060000Y'
    ]
})

df_sip = df_cert.with_columns([
    pl.when((pl.col('PRODUCT') == 203) & (pl.col('CERTNO') == 'IN002220'))
      .then(pl.lit(2))
      .otherwise(pl.lit(1))
      .alias('TRANCHE')
])

df_sip = df_sip.with_columns([
    ((pl.col('PRINAMT') - pl.col('REDAMT')) / 1000 * -1).alias('BALANCE')
])

df_sip = df_sip.with_columns([
    pl.when(pl.col('PRODUCT') == 200)
      .then(pl.lit(date(2011, 6, 27)))
      .when(pl.col('PRODUCT') == 202)
      .then(pl.lit(date(2010, 9, 27)))
      .when((pl.col('PRODUCT') == 203) & (pl.col('TRANCHE') == 1))
      .then(pl.lit(date(2014, 6, 5)))
      .when((pl.col('PRODUCT') == 203) & (pl.col('TRANCHE') == 2))
      .then(pl.lit(date(2014, 6, 26)))
      .when(pl.col('PRODUCT') == 204)
      .then(pl.lit(date(2014, 7, 31)))
      .when(pl.col('PRODUCT') == 205)
      .then(pl.lit(date(2013, 7, 9)))
      .otherwise(pl.lit(None))
      .alias('MATDT')
])

df_sip = df_sip.with_columns([
    pl.when((pl.col('BALANCE') > 0) & ((pl.col('MATDT').cast(pl.Date) - pl.lit(reptdate).cast(pl.Date)).dt.total_days() < 8))
      .then(pl.lit(0.1))
      .otherwise(
          pl.struct(['MATDT']).map_elements(
              lambda x: calculate_remmth(x['MATDT'], reptdate) if x['MATDT'] else 0,
              return_dtype=pl.Float64
          )
      )
      .alias('REMMTH')
])

df_sip = df_sip.with_columns([
    (pl.lit('9532900') + pl.col('REMMTH').map_elements(categorize_remmth, return_dtype=pl.Utf8) + pl.lit('0000Y')).alias('BNMCODE')
])

df_sip_summary = df_sip.group_by('BNMCODE').agg([
    pl.col('BALANCE').sum()
])

df_sipp2 = df_default.join(df_sip_summary, on='BNMCODE', how='outer_coalesce').sort('BNMCODE')

df_sipp2 = df_sipp2.with_columns([
    pl.lit('9532900').alias('PROD'),
    pl.lit('INDRMSP').alias('DESC'),
    pl.lit('A1.20').alias('ITEM')
])

df_sipp2.write_parquet(f'{STORE_DIR}SIPP2.parquet')

df_sipp1 = df_sipp2.with_columns([
    pl.col('BNMCODE').str.replace('9532900', '9332900'),
    pl.lit('9332900').alias('PROD')
])

df_sipp1.write_parquet(f'{STORE_DIR}SIPP1.parquet')

df_sipp2_pivot = df_sipp2.select(['PROD', 'DESC', 'ITEM', 'BALANCE']).with_row_index()

balance_values = df_sipp2_pivot['BALANCE'].to_list()
week = balance_values[0] if len(balance_values) > 0 else 0
month = balance_values[1] if len(balance_values) > 1 else 0
qtr = balance_values[2] if len(balance_values) > 2 else 0
halfyr = balance_values[3] if len(balance_values) > 3 else 0
year = balance_values[4] if len(balance_values) > 4 else 0
last = balance_values[5] if len(balance_values) > 5 else 0

df_sipp2_final = pl.DataFrame({
    'PROD': ['9532900'],
    'DESC': ['INDRMSP'],
    'ITEM': ['A1.20'],
    'WEEK': [week],
    'MONTH': [month],
    'QTR': [qtr],
    'HALFYR': [halfyr],
    'YEAR': [year],
    'LAST': [last],
    'BALANCE': [week + month + qtr + halfyr + year + last]
})

df_sipp2_final.write_parquet(f'{STORE_DIR}SIPP2.parquet')
df_sipp2_final.write_parquet(f'{STOREM_DIR}SIPP2{reptyear}{reptmon}{reptday}.parquet')

df_sipp1_pivot = df_sipp1.select(['PROD', 'DESC', 'ITEM', 'BALANCE']).with_row_index()

balance_values = df_sipp1_pivot['BALANCE'].to_list()
week = balance_values[0] if len(balance_values) > 0 else 0
month = balance_values[1] if len(balance_values) > 1 else 0
qtr = balance_values[2] if len(balance_values) > 2 else 0
halfyr = balance_values[3] if len(balance_values) > 3 else 0
year = balance_values[4] if len(balance_values) > 4 else 0
last = balance_values[5] if len(balance_values) > 5 else 0

df_sipp1_final = pl.DataFrame({
    'PROD': ['9332900'],
    'DESC': ['INDRMSP'],
    'ITEM': ['A1.20'],
    'WEEK': [week],
    'MONTH': [month],
    'QTR': [qtr],
    'HALFYR': [halfyr],
    'YEAR': [year],
    'LAST': [last],
    'BALANCE': [week + month + qtr + halfyr + year + last]
})

df_sipp1_final.write_parquet(f'{STORE_DIR}SIPP1.parquet')
df_sipp1_final.write_parquet(f'{STOREM_DIR}SIPP1{reptyear}{reptmon}{reptday}.parquet')

print(f"\nData files generated:")
print(f"  {STORE_DIR}SIPP1.parquet")
print(f"  {STORE_DIR}SIPP2.parquet")
print(f"  {STOREM_DIR}SIPP1{reptyear}{reptmon}{reptday}.parquet")
print(f"  {STOREM_DIR}SIPP2{reptyear}{reptmon}{reptday}.parquet")
print(f"\nSIPP1 Balance: {df_sipp1_final['BALANCE'][0]:,.2f}")
print(f"SIPP2 Balance: {df_sipp2_final['BALANCE'][0]:,.2f}")
