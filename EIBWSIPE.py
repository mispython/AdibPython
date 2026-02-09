import polars as pl
from datetime import datetime, timedelta

SIP_DIR = 'data/sip/'
SIPWH_DIR = 'data/sipwh/'
R1SIPWH_DIR = 'data/r1sipwh/'
SIPCUST = 'data/sipcust.txt'
SIPPROD = 'data/sipprod.txt'
SIPCERT = 'data/sipcert.txt'

today = datetime.now().date()
day = today.day

if 8 <= day <= 14:
    reptdate = datetime(today.year, today.month, 8).date()
    wk = '1'
elif 15 <= day <= 21:
    reptdate = datetime(today.year, today.month, 15).date()
    wk = '2'
elif 22 <= day <= 27:
    reptdate = datetime(today.year, today.month, 22).date()
    wk = '3'
else:
    reptdate = datetime(today.year, today.month, 1).date() - timedelta(days=1)
    wk = '4'

nowk = wk
rdate = reptdate.strftime('%d%m%y')
reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')

df_reptdate = pl.DataFrame({
    'REPTDATE': [reptdate],
    'WK': [wk]
})

df_reptdate.write_parquet(f'{SIP_DIR}REPTDATE.parquet')
df_reptdate.write_parquet(f'{SIPWH_DIR}REPTDATE.parquet')

print(f"Report Date: {reptdate.strftime('%Y-%m-%d')} (Week {wk})")

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
      .alias('MARITAL')
])

df_cust = df_cust.with_columns([
    pl.when(pl.col('MM1') > 0)
      .then(
          pl.struct(['YY1', 'MM1', 'DD1']).map_elements(
              lambda x: datetime(x['YY1'], x['MM1'], x['DD1']).date() if x['MM1'] > 0 and x['DD1'] > 0 else None,
              return_dtype=pl.Date
          )
      )
      .otherwise(pl.lit(None))
      .alias('BIRTHDT')
])

df_cust = df_cust.drop(['INVSDES1', 'DD1', 'MM1', 'YY1', 'GEND', 'ETHN', 'MARI'])

prod_schema = {
    'PRODKEY': (0, 7, str),
    'PRODNM': (7, 107, str),
    'PRODUCT': (107, 110, int),
    'ASETCODE': (110, 112, str),
    'AGENT': (112, 232, str),
    'COLLACC': (232, 242, str),
    'PRTECT': (242, 243, str),
    'COLLABLE': (243, 244, str),
    'TRANCNO': (244, 246, int),
    'VALUFEQ': (246, 247, int),
    'TENURE': (247, 251, int),
    'ISOCURR': (251, 254, str),
    'MINAMT': (254, 271, float),
    'PRATE': (271, 277, float),
    'ISRATE': (277, 285, float),
    'DD1': (287, 289, int),
    'MM1': (290, 292, int),
    'YY1': (293, 297, int),
    'DD2': (297, 299, int),
    'MM2': (300, 302, int),
    'YY2': (303, 307, int),
    'DD3': (307, 309, int),
    'MM3': (310, 312, int),
    'YY3': (313, 317, int),
    'DD4': (317, 319, int),
    'MM4': (320, 322, int),
    'YY4': (323, 327, int),
    'DD5': (327, 329, int),
    'MM5': (330, 332, int),
    'YY5': (333, 337, int),
    'INVAMT': (337, 354, float),
    'DD6': (354, 356, int),
    'MM6': (357, 359, int),
    'YY6': (360, 364, int),
    'TOTNAV': (364, 381, float),
    'TOTNAVP': (381, 389, float),
    'FVALIND': (389, 390, str),
    'INVINTRE': (390, 396, float),
    'INVCSREB': (397, 403, float),
    'INVCSFD': (403, 409, float),
    'CSREBFD': (409, 419, str),
    'SWNAME': (419, 427, str),
    'SWKEY': (427, 434, str),
    'SWCSREB': (434, 440, float),
    'DD7': (440, 442, int),
    'MM7': (443, 445, int),
    'YY7': (446, 450, int),
    'DD8': (450, 452, int),
    'MM8': (453, 455, int),
    'YY8': (456, 460, int),
    'DD9': (460, 462, int),
    'MM9': (463, 465, int),
    'YY9': (466, 470, int),
    'DD10': (470, 472, int),
    'MM10': (473, 475, int),
    'YY10': (476, 480, int),
    'MININVBY': (480, 490, str),
    'LOWBAR': (490, 498, float),
    'UPPBAR': (498, 506, float),
    'COUPAYFR': (506, 508, int),
    'COUPAYRA': (508, 513, float),
    'DISERBCP': (513, 516, int),
    'ALLERBCP': (516, 519, int),
    'LISCOUPD': (519, 999, str)
}

df_prod = read_fixed_width_file(SIPPROD, prod_schema)

df_prod = df_prod.filter(pl.col('PRODKEY') != '')

date_mappings = [
    ('OFSTDT', 'MM1', 'DD1', 'YY1'),
    ('OFENDT', 'MM2', 'DD2', 'YY2'),
    ('TRADDT', 'MM3', 'DD3', 'YY3'),
    ('MATUDT', 'MM4', 'DD4', 'YY4'),
    ('MATPYDT', 'MM5', 'DD5', 'YY5'),
    ('VALUDT', 'MM6', 'DD6', 'YY6'),
    ('EFFTDT', 'MM7', 'DD7', 'YY7'),
    ('INIVALDT', 'MM8', 'DD8', 'YY8'),
    ('FINVALDT', 'MM9', 'DD9', 'YY9'),
    ('FIFXVADT', 'MM10', 'DD10', 'YY10')
]

for date_col, mm, dd, yy in date_mappings:
    df_prod = df_prod.with_columns([
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

df_prod = df_prod.drop([col for _, mm, dd, yy in date_mappings for col in [mm, dd, yy]])

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

df_cust.write_parquet(f'{SIP_DIR}SIPCUST{reptmon}{nowk}{reptyear}.parquet')
df_cust.write_parquet(f'{R1SIPWH_DIR}SIPCUST{reptmon}{nowk}{reptyear}.parquet')

df_prod_sorted = df_prod.sort('TRADDT')
df_prod_sorted.write_parquet(f'{SIP_DIR}SIPPROD{reptmon}{nowk}{reptyear}.parquet')
df_prod_sorted.write_parquet(f'{SIPWH_DIR}SIPPROD{reptmon}{nowk}{reptyear}.parquet')

df_cert.write_parquet(f'{SIP_DIR}SIPCERT{reptmon}{nowk}{reptyear}.parquet')
df_cert.write_parquet(f'{SIPWH_DIR}SIPCERT{reptmon}{nowk}{reptyear}.parquet')

print(f"\nData files generated:")
print(f"  {SIP_DIR}SIPCUST{reptmon}{nowk}{reptyear}.parquet")
print(f"  {R1SIPWH_DIR}SIPCUST{reptmon}{nowk}{reptyear}.parquet")
print(f"  {SIP_DIR}SIPPROD{reptmon}{nowk}{reptyear}.parquet")
print(f"  {SIPWH_DIR}SIPPROD{reptmon}{nowk}{reptyear}.parquet")
print(f"  {SIP_DIR}SIPCERT{reptmon}{nowk}{reptyear}.parquet")
print(f"  {SIPWH_DIR}SIPCERT{reptmon}{nowk}{reptyear}.parquet")
print(f"\nCustomers: {len(df_cust)}")
print(f"Products: {len(df_prod)}")
print(f"Certificates: {len(df_cert)}")
