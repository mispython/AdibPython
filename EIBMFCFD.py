import polars as pl
from datetime import datetime, timedelta
import struct

DPTRBL = 'data/dptrbl.dat'
RAW = 'data/raw.dat'
MIS_DIR = 'data/mis/'
MISB_DIR = 'data/misb/'
CISDP_DIR = 'data/cisdp/'
MNITB_DIR = 'data/mnitb/'
MNIFD_DIR = 'data/mnifd/'

FCY_PRODUCTS = [400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 413, 
                420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434]

FORATE_RATES = {
    'USD': 3.80, 'EUR': 4.50, 'GBP': 5.20, 'JPY': 0.035, 'SGD': 2.85,
    'AUD': 2.60, 'CAD': 2.90, 'CHF': 4.10, 'NZD': 2.40, 'HKD': 0.49, 'MYR': 1.0
}

with open(DPTRBL, 'rb') as f:
    first_record = f.read(1752)

tbdate_bytes = first_record[105:111]
tbdate = struct.unpack('>q', b'\x00\x00' + tbdate_bytes)[0]
tbdate_str = str(tbdate).zfill(11)[:8]
reptdate = datetime.strptime(tbdate_str, '%m%d%Y').date()
reptdatx = reptdate + timedelta(days=1)

reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')
nowk = '4'
reptdt = reptdate
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')
xdate = int(reptdatx.strftime('%y%j'))

dayone = datetime(reptdate.year, reptdate.month, 1).date()

df_averbal = pl.read_parquet(f'{MIS_DIR}FCYFD{reptmon}.parquet')
df_averbal = df_averbal.filter(
    (pl.col('REPTDATE') >= dayone) & 
    (pl.col('REPTDATE') <= reptdt)
).rename({'CURBALUS': 'TOTMTBAL'}).select(['ACCTNO', 'CDNO', 'TOTMTBAL'])

df_averbal = df_averbal.group_by(['ACCTNO', 'CDNO']).agg([
    pl.col('TOTMTBAL').sum()
])

def read_dptrbl_fd():
    records = []
    with open(DPTRBL, 'rb') as f:
        while True:
            record = f.read(1752)
            if len(record) < 1752:
                break
            
            bankno = struct.unpack('>H', record[2:4])[0]
            reptno = struct.unpack('>I', b'\x00' + record[23:26])[0]
            fmtcode = struct.unpack('>H', record[26:28])[0]
            
            if bankno == 33 and reptno == 4001 and fmtcode == 2:
                branch = struct.unpack('>I', record[105:109])[0]
                acctno = struct.unpack('>q', b'\x00\x00' + record[109:115])[0]
                name = record[133:148].decode('ascii', errors='ignore').strip()
                custcode = struct.unpack('>I', b'\x00' + record[123:126])[0]
                cdno = struct.unpack('>q', b'\x00\x00' + record[148:154])[0]
                openind = chr(record[154])
                curbal = struct.unpack('>q', b'\x00\x00' + record[155:161])[0] / 100.0
                orgdate = struct.unpack('>I', b'\x00\x00\x00' + record[173:176])[0]
                matdate = struct.unpack('>I', b'\x00\x00\x00' + record[178:181])[0]
                product = struct.unpack('>H', record[202:204])[0]
                term = struct.unpack('>H', record[204:206])[0]
                intplan = struct.unpack('>H', record[207:209])[0]
                renewal = chr(record[210])
                lastactv = struct.unpack('>I', b'\x00\x00\x00' + record[239:242])[0]
                curcode = record[348:351].decode('ascii', errors='ignore').strip()
                prinbal = struct.unpack('>q', b'\x00\x00' + record[373:379])[0] / 100.0
                lmatdate = struct.unpack('>q', b'\x00\x00' + record[391:397])[0]
                
                records.append({
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'NAME': name,
                    'CUSTCODE': custcode,
                    'CDNO': cdno,
                    'OPENIND': openind,
                    'CURBAL': curbal,
                    'ORGDATE': orgdate,
                    'MATDATE': matdate,
                    'PRODUCT': product,
                    'TERM': term,
                    'INTPLAN': intplan,
                    'RENEWAL': renewal,
                    'LASTACTV': lastactv,
                    'CURCODE': curcode,
                    'PRINBAL': prinbal,
                    'LMATDATE': lmatdate
                })
    
    return pl.DataFrame(records) if records else pl.DataFrame()

df_fcyfd = read_dptrbl_fd()

df_fcyfd = df_fcyfd.with_columns([
    pl.when(pl.col('LMATDATE') > 0)
      .then(
          pl.col('LMATDATE').cast(pl.Utf8).str.zfill(11).str.slice(0, 8).map_elements(
              lambda x: datetime.strptime(x, '%m%d%Y').date(),
              return_dtype=pl.Date
          )
      )
      .otherwise(pl.lit(None))
      .alias('LMDATES')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(
        (pl.col('RENEWAL') == 'N') & 
        (pl.col('LMDATES') == reptdatx) &
        (pl.col('LMATDATE') > 0)
    )
    .then(
        pl.col('LMDATES').dt.strftime('%Y%m%d').cast(pl.Int64)
    )
    .otherwise(pl.col('MATDATE'))
    .alias('MATDATE')
])

df_mnitbfd = pl.read_parquet(f'{MNITB_DIR}FD.parquet').select(['ACCTNO', 'SECTOR', 'PURPOSE'])

df_fcyfd = df_fcyfd.join(df_mnitbfd, on='ACCTNO', how='left')

df_fcyfd = df_fcyfd.filter(pl.col('PRODUCT').is_in(FCY_PRODUCTS))

df_fcyfd = df_fcyfd.with_columns([
    pl.col('CURBAL').alias('FORBAL'),
    pl.col('SECTOR').alias('PURPOSE1'),
    pl.col('MATDATE').cast(pl.Utf8).str.zfill(8).map_elements(
        lambda x: datetime.strptime(x, '%Y%m%d').date() if len(x) == 8 else None,
        return_dtype=pl.Date
    ).alias('MATUREDT'),
    pl.col('ORGDATE').cast(pl.Utf8).str.zfill(9).str.slice(0, 6).map_elements(
        lambda x: datetime.strptime(x, '%m%d%y').date() if len(x) == 6 else None,
        return_dtype=pl.Date
    ).alias('STARTDAT')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(pl.col('CURCODE') != 'MYR')
      .then(pl.col('CURCODE').map_elements(lambda x: FORATE_RATES.get(x, 1.0), return_dtype=pl.Float64))
      .otherwise(pl.lit(1.0))
      .alias('FORATE')
])

df_fcyfd = df_fcyfd.with_columns([
    (pl.col('CURBAL') * pl.col('FORATE')).round(2).alias('CURBAL'),
    (pl.col('CURBAL') / FORATE_RATES['USD']).alias('CURBALUS')
])

custcd_map = {1: '77', 2: '78', 95: '95', 96: '96'}
df_fcyfd = df_fcyfd.with_columns([
    pl.col('CUSTCODE').map_elements(lambda x: custcd_map.get(x, str(x)), return_dtype=pl.Utf8).alias('CUSTCD')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(
        pl.col('CUSTCD').is_in(['77','78','95','96']) &
        ((pl.col('SECTOR').is_in([4,5])) | ((pl.col('SECTOR') >= 40) & (pl.col('SECTOR') <= 59)))
    )
    .then(pl.lit(1))
    .when(
        pl.col('CUSTCD').is_in(['77','78','95','96']) &
        (~((pl.col('SECTOR') >= 10) & (pl.col('SECTOR') <= 59)) | (~pl.col('SECTOR').is_in([1,2,3,4,5])))
    )
    .then(pl.lit(1))
    .when(
        ~pl.col('CUSTCD').is_in(['77','78','95','96']) &
        ((pl.col('SECTOR').is_in([4,5])) | ((pl.col('SECTOR') >= 40) & (pl.col('SECTOR') <= 59)))
    )
    .then(pl.lit(1))
    .when(
        ~pl.col('CUSTCD').is_in(['77','78','95','96']) &
        (~((pl.col('SECTOR') >= 10) & (pl.col('SECTOR') <= 59)) | (~pl.col('SECTOR').is_in([1,2,3,4,5])))
    )
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias('SECTORX')
])

df_fcyfd = df_fcyfd.filter(
    ~pl.col('OPENIND').is_in(['B','C','P']) |
    (
        pl.col('OPENIND').is_in(['B','C','P']) &
        (pl.col('LASTACTV').cast(pl.Utf8).str.zfill(9).str.slice(0, 6).map_elements(
            lambda x: datetime.strptime(x, '%m%d%y').date() > 
                     datetime(datetime.now().year, datetime.now().month, 1).date() - timedelta(days=1)
                     if len(x) == 6 else False,
            return_dtype=pl.Boolean
        ))
    )
)

df_cisdp = pl.read_parquet(f'{CISDP_DIR}CISR1FD{reptmon}{nowk}{reptyear}.parquet')
df_cisdp = df_cisdp.filter(pl.col('SECCUST') == '901').select(['ACCTNO', 'CUSTNO'])

df_limitkey = pl.read_csv(RAW, separator=' ', has_header=False,
                          new_columns=['CUSTNO', 'PERLIMT', 'KEYWORD', 'PURPOSE1'])
df_limitkey = df_limitkey.with_columns([
    pl.col('PURPOSE1').alias('PURPOSEC')
])

df_average = df_fcyfd.join(df_cisdp, on='ACCTNO', how='left')
df_average = df_average.with_columns([
    pl.col('PURPOSE1').alias('PURPOSEM')
])

df_average = df_average.sort(['CUSTNO', 'PURPOSE1']).join(
    df_limitkey.sort(['CUSTNO', 'PURPOSE1']),
    on=['CUSTNO', 'PURPOSE1'],
    how='left'
).sort(['ACCTNO', 'CDNO'])

df_current = df_average.join(df_averbal, on=['ACCTNO', 'CDNO'], how='left')

days = (reptdt - dayone).days + 1
df_current = df_current.with_columns([
    (pl.col('TOTMTBAL') / days).alias('AVERBAL'),
    pl.col('CURBAL').alias('CURBALRM'),
    pl.col('PURPOSE').alias('CLASSIFI')
])

df_current = df_current.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['80','81','82','83','84','85','86','90','91','92','95','96','98','99']))
      .then(pl.lit('N'))
      .otherwise(pl.lit('R'))
      .alias('RESIDIND')
])

df_current = df_current.with_columns([
    pl.when(pl.col('RESIDIND') != 'N')
      .then(
          pl.when(
              ((pl.col('SECTOR').is_in([1,2,3])) | 
               ((pl.col('SECTOR') >= 10) & (pl.col('SECTOR') <= 39)) | 
               (pl.col('SECTORX') == 1)) &
              pl.col('CUSTCD').is_in(['77','78'])
          )
          .then(pl.lit('INDIVIDUAL'))
          .when(
              ((pl.col('SECTOR').is_in([4,5])) | 
               ((pl.col('SECTOR') >= 40) & (pl.col('SECTOR') <= 59)) | 
               (pl.col('SECTORX') == 4)) &
              ~pl.col('CUSTCD').is_in(['77','78'])
          )
          .then(pl.lit('COMPANIES '))
          .otherwise(pl.lit(None))
      )
      .otherwise(
          pl.when(
              ((pl.col('SECTOR').is_in([1,2,3])) | 
               ((pl.col('SECTOR') >= 10) & (pl.col('SECTOR') <= 39)) | 
               (pl.col('SECTORX') == 1)) &
              pl.col('CUSTCD').is_in(['95','96'])
          )
          .then(pl.lit('INDIVIDUAL'))
          .when(
              ((pl.col('SECTOR').is_in([4,5])) | 
               ((pl.col('SECTOR') >= 40) & (pl.col('SECTOR') <= 59)) | 
               (pl.col('SECTORX') == 4)) &
              ~pl.col('CUSTCD').is_in(['95','96'])
          )
          .then(pl.lit('COMPANIES '))
          .otherwise(pl.lit(None))
      )
      .alias('CATEGORY')
])

df_limit = df_current.filter(pl.col('PERLIMT') > 0).select(['CUSTNO', 'PURPOSE1', 'PERLIMT']).sort(['CUSTNO', 'PURPOSE1'])

df_alm1 = df_current.filter(pl.col('CUSTNO').is_not_null()).sort(['CUSTNO', 'PURPOSE1'])
df_alm1 = df_alm1.join(df_limit, on=['CUSTNO', 'PURPOSE1'], how='left', suffix='_limit')

df_alm1 = df_alm1.with_columns([
    pl.when((pl.col('AVERBAL') > pl.col('PERLIMT_limit')) & pl.col('PERLIMT_limit').is_not_null())
      .then(pl.lit('Y'))
      .otherwise(pl.lit(None))
      .alias('FLAG')
])

df_alm1 = df_alm1.with_columns([
    pl.when((pl.col('CUSTNO') != pl.col('CUSTNO').shift(1)) | 
            (pl.col('PURPOSE1') != pl.col('PURPOSE1').shift(1)))
      .then(pl.col('PERLIMT_limit'))
      .otherwise(pl.lit(0))
      .alias('PERLIMT')
]).drop('PERLIMT_limit')

df_alm1 = df_alm1.unique(subset=['ACCTNO', 'CDNO', 'PURPOSE1'], keep='first')

df_alm2 = df_current.filter(pl.col('CUSTNO').is_null())

df_alm = pl.concat([df_alm1, df_alm2])
df_alm = df_alm.with_columns([
    (pl.col('BRANCH').cast(pl.Utf8) + ' ' + pl.col('BRANCH').cast(pl.Utf8).str.zfill(3)).alias('BRANCH1')
]).sort(['CUSTNO', 'PURPOSE1'])

df_alm = df_alm.with_columns([
    pl.when((pl.col('CUSTNO') != pl.col('CUSTNO').shift(1)) | 
            (pl.col('PURPOSE1') != pl.col('PURPOSE1').shift(1)))
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('NOACCT')
]).sort(['ACCTNO', 'CDNO'])

df_fd = pl.read_parquet(f'{MNIFD_DIR}FD.parquet').drop('CUSTCD')

df_fdcd = df_alm.join(df_fd, on=['ACCTNO', 'CDNO'], how='left')

df_fdcd = df_fdcd.with_columns([
    pl.col('STATEC').alias('STATE')
])

df_fdcd.select([
    'ACCTNO', 'BRANCH', 'CDNO', 'CURCODE', 'CUSTCD', 'CLASSIFI',
    'PURPOSEM', 'PURPOSEC', 'CURBALRM', 'FORBAL', 'AVERBAL', 'PERLIMT',
    'STARTDAT', 'MATDATE', 'CURBALUS', 'INTPLAN', 'OPENIND', 'ORGDATE',
    'RENEWAL', 'STATE', 'TERM', 'CUSTNO', 'FORATE', 'NAME', 'PRODUCT', 'KEYWORD'
]).write_parquet(f'{MIS_DIR}FDCD{reptmon}.parquet')

df_resident = df_alm.filter(pl.col('RESIDIND') == 'R').sort(['BRANCH', 'PURPOSE1'])
df_nonresident = df_alm.filter(pl.col('RESIDIND') == 'N').sort(['BRANCH', 'PURPOSE1'])

print(f"\nFOREIGN CURRENCY FIXED DEPOSIT DETAIL LISTING AS AT {rdate}")
print("ATTN : MR.TAI GUAN ONG")
print("\nResident FD Summary:")
print(df_resident.select(['BRANCH', 'CUSTNO', 'ACCTNO', 'CDNO', 'NAME', 'CURCODE', 
                          'CUSTCD', 'PURPOSEM', 'CURBALRM', 'FORBAL', 'AVERBAL', 
                          'FORATE', 'PERLIMT', 'STARTDAT', 'MATUREDT']))

print(f"\nFOREIGN CURRENCY NON-RESIDENT A/C AS AT {rdate}")
print("ATTN : MR.TAI GUAN ONG")
print("\nNon-Resident FD Summary:")
print(df_nonresident.select(['BRANCH', 'CUSTNO', 'ACCTNO', 'CDNO', 'NAME', 'CURCODE',
                             'CUSTCD', 'PURPOSEM', 'CURBALRM', 'FORBAL', 'AVERBAL',
                             'FORATE', 'PERLIMT', 'STARTDAT', 'MATUREDT']))

def calculate_remmth(matdate_val, reptdate_val):
    if matdate_val is None:
        return 0
    
    md_year = matdate_val.year
    md_month = matdate_val.month
    md_day = matdate_val.day
    
    rp_year = reptdate_val.year
    rp_month = reptdate_val.month
    rp_day = reptdate_val.day
    
    days_in_rp_month = (datetime(rp_year, rp_month % 12 + 1, 1) - datetime(rp_year, rp_month, 1)).days
    
    if md_day > rp_day:
        md_day = rp_day
    
    remy = md_year - rp_year
    remm = md_month - rp_month
    remd = md_day - rp_day
    
    return remy * 12 + remm + remd / days_in_rp_month

df_alm = df_alm.with_columns([
    pl.struct(['MATUREDT']).map_elements(
        lambda x: calculate_remmth(x['MATUREDT'], reptdate),
        return_dtype=pl.Float64
    ).alias('REMMTH')
])

df_bodrm = df_alm.group_by('REMMTH').agg([
    pl.col('CURBALRM').sum().alias('AMOUNT')
])

df_bodrm.write_parquet(f'{MISB_DIR}BODRM{reptmon}.parquet')

print(f"\nBOD PAPERS (REMAINING MATURITY) {rdate}")
print(df_bodrm)

df_bodstat = df_alm.group_by(['CURCODE', 'RESIDIND']).agg([
    pl.col('PRINBAL').sum(),
    pl.col('FORBAL').sum()
])

print(f"\nSTATEMENT F REPORTING AS AT {rdate}")
print(df_bodstat.select(['CURCODE', 'RESIDIND', 'FORBAL']))

print(f"\nData saved:")
print(f"  {MIS_DIR}FDCD{reptmon}.parquet")
print(f"  {MISB_DIR}BODRM{reptmon}.parquet")
