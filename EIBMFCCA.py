import polars as pl
from datetime import datetime, timedelta
import struct

DPTRBL = 'data/dptrbl.dat'
RAW = 'data/raw.dat'
MIS_DIR = 'data/mis/'
CISDP_DIR = 'data/cisdp/'
FORATE_RATES = {
    'USD': 3.80,
    'EUR': 4.50,
    'GBP': 5.20,
    'JPY': 0.035,
    'SGD': 2.85,
    'AUD': 2.60,
    'CAD': 2.90,
    'CHF': 4.10,
    'NZD': 2.40,
    'HKD': 0.49
}

with open(DPTRBL, 'rb') as f:
    first_record = f.read(924)

tbdate_bytes = first_record[105:111]
tbdate = struct.unpack('>q', b'\x00\x00' + tbdate_bytes)[0]
tbdate_str = str(tbdate).zfill(11)[:8]
reptdate = datetime.strptime(tbdate_str, '%m%d%Y').date()

reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')
nowk = '4'
reptdt = reptdate
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

dayone = datetime(reptdate.year, reptdate.month, 1).date()

def read_dptrbl():
    records = []
    with open(DPTRBL, 'rb') as f:
        while True:
            record = f.read(924)
            if len(record) < 924:
                break
            
            bankno = struct.unpack('>H', record[2:4])[0]
            reptno = struct.unpack('>I', b'\x00' + record[23:26])[0]
            fmtcode = struct.unpack('>H', record[26:28])[0]
            
            if bankno == 33 and reptno == 1001 and fmtcode == 22:
                branch_bytes = record[105:109]
                branch = struct.unpack('>I', branch_bytes)[0]
                
                acctno_bytes = record[109:115]
                acctno = struct.unpack('>q', b'\x00\x00' + acctno_bytes)[0]
                
                name = record[115:130].decode('ascii', errors='ignore').strip()
                
                closedt_bytes = record[157:163]
                closedt = struct.unpack('>q', b'\x00\x00' + closedt_bytes)[0]
                
                custcode_bytes = record[175:178]
                custcode = struct.unpack('>I', b'\x00' + custcode_bytes)[0]
                
                sector_bytes = record[311:314]
                sector = struct.unpack('>I', b'\x00' + sector_bytes)[0]
                
                product_bytes = record[396:398]
                product = struct.unpack('>H', product_bytes)[0]
                
                deptype = chr(record[430])
                
                curbal_bytes = record[465:472]
                curbal = struct.unpack('>q', curbal_bytes)[0] / 100.0
                
                curcode = record[587:590].decode('ascii', errors='ignore').strip()
                
                if (400 <= product <= 411) or product == 413 or (420 <= product <= 434):
                    forbal = curbal
                    forate = 1.0
                    
                    if curcode != 'MYR':
                        forate = FORATE_RATES.get(curcode, 1.0)
                        curbal = curbal * forate
                    
                    curbalus = curbal / FORATE_RATES.get('USD', 3.80)
                    
                    custcd_map = {1: '77', 2: '78', 95: '95', 96: '96'}
                    custcd = custcd_map.get(custcode, str(custcode))
                    
                    sectorx = None
                    if custcd in ('77', '78', '95', '96'):
                        if sector in (4, 5) or (40 <= sector <= 59):
                            sectorx = 1
                        elif not (10 <= sector <= 59) or sector not in (1, 2, 3, 4, 5):
                            sectorx = 1
                    else:
                        if sector in (1, 2, 3) or (10 <= sector <= 39):
                            sectorx = 4
                        elif not (10 <= sector <= 59) or sector not in (1, 2, 3, 4, 5):
                            sectorx = 4
                    
                    purpose = sector
                    
                    should_output = False
                    if closedt != 0:
                        closedt_str = str(closedt).zfill(11)[:8]
                        closedat = datetime.strptime(closedt_str, '%m%d%Y').date()
                        first_of_month = datetime(datetime.now().year, datetime.now().month, 1).date()
                        if closedat > first_of_month - timedelta(days=1):
                            should_output = True
                    else:
                        if deptype in ('D', 'N'):
                            should_output = True
                    
                    if should_output:
                        records.append({
                            'REPTDATE': reptdate,
                            'BRANCH': branch,
                            'ACCTNO': acctno,
                            'NAME': name,
                            'CUSTCODE': custcode,
                            'CUSTCD': custcd,
                            'SECTOR': sector,
                            'PRODUCT': product,
                            'CURCODE': curcode,
                            'CURBAL': curbal,
                            'FORBAL': forbal,
                            'CURBALUS': curbalus,
                            'FORATE': forate,
                            'PURPOSE': purpose,
                            'PURPOSEM': purpose,
                            'SECTORX': sectorx
                        })
    
    return pl.DataFrame(records) if records else pl.DataFrame()

df_fcy = read_dptrbl()

df_averbal = pl.read_parquet(f'{MIS_DIR}FCY{reptmon}.parquet')
df_averbal = df_averbal.filter(
    (pl.col('REPTDATE') >= dayone) & 
    (pl.col('REPTDATE') <= reptdt)
).rename({'CURBALUS': 'TOTMTBAL'})

df_averbal = df_averbal.group_by('ACCTNO').agg([
    pl.col('TOTMTBAL').sum()
])

df_cisdp = pl.read_parquet(f'{CISDP_DIR}CISR1CA{reptmon}{nowk}{reptyear}.parquet')
df_cisdp = df_cisdp.filter(pl.col('SECCUST') == '901').select(['ACCTNO', 'CUSTNO'])

df_limit = pl.read_csv(RAW, separator=' ', has_header=False, 
                       new_columns=['CUSTNO', 'PERLIMT', 'KEYWORD', 'PURPOSE'])
df_limit = df_limit.with_columns([
    pl.col('PURPOSE').alias('PURPOSEC')
]).unique(subset=['CUSTNO', 'PURPOSE'], keep='first')

df_average = df_fcy.join(df_cisdp, on='ACCTNO', how='left')

df_average = df_average.sort(['CUSTNO', 'PURPOSE']).join(
    df_limit, on=['CUSTNO', 'PURPOSE'], how='left'
).sort('ACCTNO')

df_current = df_average.join(df_averbal, on='ACCTNO', how='left')

days = (reptdt - dayone).days + 1
df_current = df_current.with_columns([
    (pl.col('TOTMTBAL') / days).alias('AVERBAL'),
    pl.col('CURBAL').alias('CURBALRM')
])

df_current = df_current.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['80','81','82','83','84','85','86','90','91','92','95','96','98','99']))
      .then(pl.lit('N'))
      .otherwise(pl.lit('M'))
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

df_limit_valid = df_current.filter(pl.col('PERLIMT') > 0).select(['CUSTNO', 'PURPOSE', 'PERLIMT'])

df_alm1_summary = df_current.filter(pl.col('CUSTNO').is_not_null()).group_by(['CUSTNO', 'PURPOSE']).agg([
    pl.col('AVERBAL').sum()
])

df_alm1 = df_alm1_summary.join(
    df_current.drop(['PERLIMT', 'AVERBAL']).unique(subset=['CUSTNO', 'PURPOSE'], keep='first'),
    on=['CUSTNO', 'PURPOSE'],
    how='left'
).join(df_limit_valid, on=['CUSTNO', 'PURPOSE'], how='left')

df_alm1 = df_alm1.with_columns([
    pl.when((pl.col('AVERBAL') > pl.col('PERLIMT')) & pl.col('PERLIMT').is_not_null())
      .then(pl.lit('Y'))
      .otherwise(pl.lit(None))
      .alias('FLAG')
])

df_alm2 = df_current.filter(pl.col('CUSTNO').is_null())

df_alm = pl.concat([df_alm1, df_alm2])

df_alm = df_alm.with_columns([
    (pl.col('BRANCH').cast(pl.Utf8) + ' ' + pl.col('BRANCH').cast(pl.Utf8).str.zfill(3)).alias('BRANCH1')
])

df_current = df_current.sort(['CUSTNO', 'PURPOSE'])

df_current = df_current.with_columns([
    pl.when((pl.col('CUSTNO') != pl.col('CUSTNO').shift(1)) | 
            (pl.col('PURPOSE') != pl.col('PURPOSE').shift(1)))
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('NOACCT')
])

df_current = df_current.with_columns([
    pl.when(pl.col('NOACCT') == 0)
      .then(pl.lit(0))
      .otherwise(pl.col('PERLIMT'))
      .alias('PERLIMT')
])

df_current.write_parquet(f'{MIS_DIR}FCYCA.parquet')

df_resident = df_current.filter(pl.col('RESIDIND') == 'M').sort(['BRANCH', 'CUSTNO'])
df_nonresident = df_current.filter(pl.col('RESIDIND') == 'N').sort(['BRANCH', 'CUSTNO'])

print(f"\nFOREIGN CURRENCY RESIDENT A/C AS AT {rdate}")
print("ATTN : MS.LIM")
print("\nResident Accounts Summary:")
print(df_resident.select(['BRANCH', 'CUSTNO', 'ACCTNO', 'NAME', 'CURCODE', 'CUSTCD', 
                          'PURPOSEM', 'CURBALRM', 'FORBAL', 'AVERBAL', 'FORATE', 'PERLIMT']))

print(f"\nFOREIGN CURRENCY NON-RESIDENT A/C AS AT {rdate}")
print("ATTN : MS.LIM")
print("\nNon-Resident Accounts Summary:")
print(df_nonresident.select(['BRANCH', 'CUSTNO', 'ACCTNO', 'NAME', 'CURCODE', 'CUSTCD', 
                             'PURPOSEM', 'CURBALRM', 'FORBAL', 'AVERBAL', 'FORATE', 'PERLIMT']))

df_resident_report = df_current.filter(pl.col('RESIDIND') == 'M')
df_resident_summary = df_resident_report.group_by(['CATEGORY', 'KEYWORD']).agg([
    pl.col('PERLIMT').sum().alias('PERMITTED_LIMIT'),
    pl.col('AVERBAL').sum().alias('AVERAGE_BALANCE'),
    pl.col('NOACCT').sum().alias('NO_OF_ACCOUNTS')
])

print(f"\nREPORT ON FOREIGN CURRENCY A/C BALANCES OF RESIDENT {rdate}")
print(df_resident_summary)

df_nonresident_report = df_current.filter(pl.col('RESIDIND') == 'N')
df_nonresident_summary = df_nonresident_report.group_by(['CATEGORY', 'KEYWORD']).agg([
    pl.col('PERLIMT').sum().alias('PERMITTED_LIMIT'),
    pl.col('AVERBAL').sum().alias('AVERAGE_BALANCE'),
    pl.col('NOACCT').sum().alias('NO_OF_ACCOUNTS')
])

print(f"\nREPORT ON FOREIGN CURRENCY A/C BALANCES OF NON-RESIDENT {rdate}")
print(df_nonresident_summary)

print(f"\nData saved to {MIS_DIR}FCYCA.parquet")
