import polars as pl
from datetime import datetime, timedelta
import struct
import pyreadstat
import saspy
import os

# Initialize SAS session
sas = saspy.SASsession(cfgname='default')  # Adjust cfgname as needed

# Paths for SAS datasets
MIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/mis/'
MISB_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/misb/'
CISDP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/cisdp/'
MNITB_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/mnitb/'
MNIFD_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/mnifd/'

# Flat file paths
DPTRBL = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/dptrbl.dat'
RAW = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMFCFD/raw.dat'

# Output directories
OUTPUT_MIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/prod/EIBMFCFD/mis/'
OUTPUT_MISB_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/prod/EIBMFCFD/misb/'

# Create output directories if they don't exist
os.makedirs(OUTPUT_MIS_DIR, exist_ok=True)
os.makedirs(OUTPUT_MISB_DIR, exist_ok=True)

FCY_PRODUCTS = [400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 413, 
                420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434]

FORATE_RATES = {
    'USD': 3.80, 'EUR': 4.50, 'GBP': 5.20, 'JPY': 0.035, 'SGD': 2.85,
    'AUD': 2.60, 'CAD': 2.90, 'CHF': 4.10, 'NZD': 2.40, 'HKD': 0.49, 'MYR': 1.0
}

# Read first record from DPTRBL to get report date
with open(DPTRBL, 'rb') as f:
    first_record = f.read(1752)

tbdate_bytes = first_record[105:111]
tbdate = struct.unpack('>q', b'\x00\x00' + tbdate_bytes)[0]
tbdate_str = str(tbdate).zfill(11)[:8]
reptdate = datetime.strptime(tbdate_str, '%m%d%Y').date()
reptdatx = reptdate + timedelta(days=1)

# Use datetime - 1 day for previous day
prevdate = reptdate - timedelta(days=1)

reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')
nowk = '4'
reptdt = reptdate
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')
xdate = int(reptdatx.strftime('%y%j'))

dayone = datetime(reptdate.year, reptdate.month, 1).date()

# Read SAS datasets using pyreadstat
# FCYFD dataset from MIS directory
df_averbal, meta_averbal = pyreadstat.read_sas7bdat(f'{MIS_DIR}fcyfd{reptmon}.sas7bdat')
df_averbal = pl.from_pandas(df_averbal)
df_averbal = df_averbal.filter(
    (pl.col('reptdate') >= dayone) & 
    (pl.col('reptdate') <= reptdt)
).rename({'curbalus': 'totmtbal'}).select(['acctno', 'cdno', 'totmtbal'])

df_averbal = df_averbal.group_by(['acctno', 'cdno']).agg([
    pl.col('totmtbal').sum()
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
                    'branch': branch,
                    'acctno': acctno,
                    'name': name,
                    'custcode': custcode,
                    'cdno': cdno,
                    'openind': openind,
                    'curbal': curbal,
                    'orgdate': orgdate,
                    'matdate': matdate,
                    'product': product,
                    'term': term,
                    'intplan': intplan,
                    'renewal': renewal,
                    'lastactv': lastactv,
                    'curcode': curcode,
                    'prinbal': prinbal,
                    'lmatdate': lmatdate
                })
    
    return pl.DataFrame(records) if records else pl.DataFrame()

df_fcyfd = read_dptrbl_fd()

df_fcyfd = df_fcyfd.with_columns([
    pl.when(pl.col('lmatdate') > 0)
      .then(
          pl.col('lmatdate').cast(pl.Utf8).str.zfill(11).str.slice(0, 8).map_elements(
              lambda x: datetime.strptime(x, '%m%d%Y').date(),
              return_dtype=pl.Date
          )
      )
      .otherwise(pl.lit(None))
      .alias('lmdates')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(
        (pl.col('renewal') == 'N') & 
        (pl.col('lmdates') == reptdatx) &
        (pl.col('lmatdate') > 0)
    )
    .then(
        pl.col('lmdates').dt.strftime('%Y%m%d').cast(pl.Int64)
    )
    .otherwise(pl.col('matdate'))
    .alias('matdate')
])

# Read MNITB dataset
df_mnitbfd, meta_mnitbfd = pyreadstat.read_sas7bdat(f'{MNITB_DIR}fd.sas7bdat')
df_mnitbfd = pl.from_pandas(df_mnitbfd)
df_mnitbfd = df_mnitbfd.select(['acctno', 'sector', 'purpose'])

df_fcyfd = df_fcyfd.join(df_mnitbfd, on='acctno', how='left')

df_fcyfd = df_fcyfd.filter(pl.col('product').is_in(FCY_PRODUCTS))

df_fcyfd = df_fcyfd.with_columns([
    pl.col('curbal').alias('forbal'),
    pl.col('sector').alias('purpose1'),
    pl.col('matdate').cast(pl.Utf8).str.zfill(8).map_elements(
        lambda x: datetime.strptime(x, '%Y%m%d').date() if len(x) == 8 else None,
        return_dtype=pl.Date
    ).alias('maturedt'),
    pl.col('orgdate').cast(pl.Utf8).str.zfill(9).str.slice(0, 6).map_elements(
        lambda x: datetime.strptime(x, '%m%d%y').date() if len(x) == 6 else None,
        return_dtype=pl.Date
    ).alias('startdat')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(pl.col('curcode') != 'MYR')
      .then(pl.col('curcode').map_elements(lambda x: FORATE_RATES.get(x, 1.0), return_dtype=pl.Float64))
      .otherwise(pl.lit(1.0))
      .alias('forate')
])

df_fcyfd = df_fcyfd.with_columns([
    (pl.col('curbal') * pl.col('forate')).round(2).alias('curbal'),
    (pl.col('curbal') / FORATE_RATES['USD']).alias('curbalus')
])

custcd_map = {1: '77', 2: '78', 95: '95', 96: '96'}
df_fcyfd = df_fcyfd.with_columns([
    pl.col('custcode').map_elements(lambda x: custcd_map.get(x, str(x)), return_dtype=pl.Utf8).alias('custcd')
])

df_fcyfd = df_fcyfd.with_columns([
    pl.when(
        pl.col('custcd').is_in(['77','78','95','96']) &
        ((pl.col('sector').is_in([4,5])) | ((pl.col('sector') >= 40) & (pl.col('sector') <= 59)))
    )
    .then(pl.lit(1))
    .when(
        pl.col('custcd').is_in(['77','78','95','96']) &
        (~((pl.col('sector') >= 10) & (pl.col('sector') <= 59)) | (~pl.col('sector').is_in([1,2,3,4,5])))
    )
    .then(pl.lit(1))
    .when(
        ~pl.col('custcd').is_in(['77','78','95','96']) &
        ((pl.col('sector').is_in([4,5])) | ((pl.col('sector') >= 40) & (pl.col('sector') <= 59)))
    )
    .then(pl.lit(1))
    .when(
        ~pl.col('custcd').is_in(['77','78','95','96']) &
        (~((pl.col('sector') >= 10) & (pl.col('sector') <= 59)) | (~pl.col('sector').is_in([1,2,3,4,5])))
    )
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias('sectorx')
])

df_fcyfd = df_fcyfd.filter(
    ~pl.col('openind').is_in(['B','C','P']) |
    (
        pl.col('openind').is_in(['B','C','P']) &
        (pl.col('lastactv').cast(pl.Utf8).str.zfill(9).str.slice(0, 6).map_elements(
            lambda x: datetime.strptime(x, '%m%d%y').date() > 
                     datetime(prevdate.year, prevdate.month, 1).date() - timedelta(days=1)
                     if len(x) == 6 else False,
            return_dtype=pl.Boolean
        ))
    )
)

# Read CISDP dataset
df_cisdp, meta_cisdp = pyreadstat.read_sas7bdat(f'{CISDP_DIR}cisr1fd{reptmon}{nowk}{reptyear}.sas7bdat')
df_cisdp = pl.from_pandas(df_cisdp)
df_cisdp = df_cisdp.filter(pl.col('seccust') == '901').select(['acctno', 'custno'])

# Read RAW flat file
df_limitkey = pl.read_csv(RAW, separator=' ', has_header=False,
                          new_columns=['custno', 'perlimt', 'keyword', 'purpose1'])
df_limitkey = df_limitkey.with_columns([
    pl.col('purpose1').alias('purposec')
])

df_average = df_fcyfd.join(df_cisdp, on='acctno', how='left')
df_average = df_average.with_columns([
    pl.col('purpose1').alias('purposem')
])

df_average = df_average.sort(['custno', 'purpose1']).join(
    df_limitkey.sort(['custno', 'purpose1']),
    on=['custno', 'purpose1'],
    how='left'
).sort(['acctno', 'cdno'])

df_current = df_average.join(df_averbal, on=['acctno', 'cdno'], how='left')

days = (reptdt - dayone).days + 1
df_current = df_current.with_columns([
    (pl.col('totmtbal') / days).alias('averbal'),
    pl.col('curbal').alias('curbalrm'),
    pl.col('purpose').alias('classifi')
])

df_current = df_current.with_columns([
    pl.when(pl.col('custcd').is_in(['80','81','82','83','84','85','86','90','91','92','95','96','98','99']))
      .then(pl.lit('N'))
      .otherwise(pl.lit('R'))
      .alias('residind')
])

df_current = df_current.with_columns([
    pl.when(pl.col('residind') != 'N')
      .then(
          pl.when(
              ((pl.col('sector').is_in([1,2,3])) | 
               ((pl.col('sector') >= 10) & (pl.col('sector') <= 39)) | 
               (pl.col('sectorx') == 1)) &
              pl.col('custcd').is_in(['77','78'])
          )
          .then(pl.lit('INDIVIDUAL'))
          .when(
              ((pl.col('sector').is_in([4,5])) | 
               ((pl.col('sector') >= 40) & (pl.col('sector') <= 59)) | 
               (pl.col('sectorx') == 4)) &
              ~pl.col('custcd').is_in(['77','78'])
          )
          .then(pl.lit('COMPANIES '))
          .otherwise(pl.lit(None))
      )
      .otherwise(
          pl.when(
              ((pl.col('sector').is_in([1,2,3])) | 
               ((pl.col('sector') >= 10) & (pl.col('sector') <= 39)) | 
               (pl.col('sectorx') == 1)) &
              pl.col('custcd').is_in(['95','96'])
          )
          .then(pl.lit('INDIVIDUAL'))
          .when(
              ((pl.col('sector').is_in([4,5])) | 
               ((pl.col('sector') >= 40) & (pl.col('sector') <= 59)) | 
               (pl.col('sectorx') == 4)) &
              ~pl.col('custcd').is_in(['95','96'])
          )
          .then(pl.lit('COMPANIES '))
          .otherwise(pl.lit(None))
      )
      .alias('category')
])

df_limit = df_current.filter(pl.col('perlimt') > 0).select(['custno', 'purpose1', 'perlimt']).sort(['custno', 'purpose1'])

df_alm1 = df_current.filter(pl.col('custno').is_not_null()).sort(['custno', 'purpose1'])
df_alm1 = df_alm1.join(df_limit, on=['custno', 'purpose1'], how='left', suffix='_limit')

df_alm1 = df_alm1.with_columns([
    pl.when((pl.col('averbal') > pl.col('perlimt_limit')) & pl.col('perlimt_limit').is_not_null())
      .then(pl.lit('Y'))
      .otherwise(pl.lit(None))
      .alias('flag')
])

df_alm1 = df_alm1.with_columns([
    pl.when((pl.col('custno') != pl.col('custno').shift(1)) | 
            (pl.col('purpose1') != pl.col('purpose1').shift(1)))
      .then(pl.col('perlimt_limit'))
      .otherwise(pl.lit(0))
      .alias('perlimt')
]).drop('perlimt_limit')

df_alm1 = df_alm1.unique(subset=['acctno', 'cdno', 'purpose1'], keep='first')

df_alm2 = df_current.filter(pl.col('custno').is_null())

df_alm = pl.concat([df_alm1, df_alm2])
df_alm = df_alm.with_columns([
    (pl.col('branch').cast(pl.Utf8) + ' ' + pl.col('branch').cast(pl.Utf8).str.zfill(3)).alias('branch1')
]).sort(['custno', 'purpose1'])

df_alm = df_alm.with_columns([
    pl.when((pl.col('custno') != pl.col('custno').shift(1)) | 
            (pl.col('purpose1') != pl.col('purpose1').shift(1)))
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('noacct')
]).sort(['acctno', 'cdno'])

# Read MNIFD dataset
df_fd, meta_fd = pyreadstat.read_sas7bdat(f'{MNIFD_DIR}fd.sas7bdat')
df_fd = pl.from_pandas(df_fd)
if 'custcd' in df_fd.columns:
    df_fd = df_fd.drop('custcd')

df_fdcd = df_alm.join(df_fd, on=['acctno', 'cdno'], how='left')

df_fdcd = df_fdcd.with_columns([
    pl.col('statec').alias('state')
])

# Prepare final output
final_columns = [
    'acctno', 'branch', 'cdno', 'curcode', 'custcd', 'classifi',
    'purposem', 'purposec', 'curbalrm', 'forbal', 'averbal', 'perlimt',
    'startdat', 'matdate', 'curbalus', 'intplan', 'openind', 'orgdate',
    'renewal', 'state', 'term', 'custno', 'forate', 'name', 'product', 'keyword'
]

df_output = df_fdcd.select(final_columns)

# Write output to parquet
df_output.write_parquet(f'{OUTPUT_MIS_DIR}fdcd{reptmon}.parquet')

# Write output to SAS7BDAT using saspy
# Convert Polars DataFrame to pandas first
df_output_pandas = df_output.to_pandas()

# Upload to SAS
sas_df = sas.df2sd(df_output_pandas, 'work_fdcd')

# Save as SAS7BDAT
sas.submit(f'''
    LIBNAME outdir '{OUTPUT_MIS_DIR}';
    DATA outdir.fdcd{reptmon};
        SET work_fdcd;
    RUN;
''')

df_resident = df_alm.filter(pl.col('residind') == 'R').sort(['branch', 'purpose1'])
df_nonresident = df_alm.filter(pl.col('residind') == 'N').sort(['branch', 'purpose1'])

print(f"\nFOREIGN CURRENCY FIXED DEPOSIT DETAIL LISTING AS AT {rdate}")
print("ATTN : MR.TAI GUAN ONG")
print("\nResident FD Summary:")
print(df_resident.select(['branch', 'custno', 'acctno', 'cdno', 'name', 'curcode', 
                          'custcd', 'purposem', 'curbalrm', 'forbal', 'averbal', 
                          'forate', 'perlimt', 'startdat', 'maturedt']))

print(f"\nFOREIGN CURRENCY NON-RESIDENT A/C AS AT {rdate}")
print("ATTN : MR.TAI GUAN ONG")
print("\nNon-Resident FD Summary:")
print(df_nonresident.select(['branch', 'custno', 'acctno', 'cdno', 'name', 'curcode',
                             'custcd', 'purposem', 'curbalrm', 'forbal', 'averbal',
                             'forate', 'perlimt', 'startdat', 'maturedt']))

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
    pl.struct(['maturedt']).map_elements(
        lambda x: calculate_remmth(x['maturedt'], reptdate),
        return_dtype=pl.Float64
    ).alias('remmth')
])

df_bodrm = df_alm.group_by('remmth').agg([
    pl.col('curbalrm').sum().alias('amount')
])

# Write BODRM to parquet
df_bodrm.write_parquet(f'{OUTPUT_MISB_DIR}bodrm{reptmon}.parquet')

# Write BODRM to SAS7BDAT
df_bodrm_pandas = df_bodrm.to_pandas()
sas_df_bodrm = sas.df2sd(df_bodrm_pandas, 'work_bodrm')

sas.submit(f'''
    LIBNAME outdir '{OUTPUT_MISB_DIR}';
    DATA outdir.bodrm{reptmon};
        SET work_bodrm;
    RUN;
''')

print(f"\nBOD PAPERS (REMAINING MATURITY) {rdate}")
print(df_bodrm)

df_bodstat = df_alm.group_by(['curcode', 'residind']).agg([
    pl.col('prinbal').sum(),
    pl.col('forbal').sum()
])

print(f"\nSTATEMENT F REPORTING AS AT {rdate}")
print(df_bodstat.select(['curcode', 'residind', 'forbal']))

print(f"\nData saved:")
print(f"  {OUTPUT_MIS_DIR}fdcd{reptmon}.parquet")
print(f"  {OUTPUT_MIS_DIR}fdcd{reptmon}.sas7bdat")
print(f"  {OUTPUT_MISB_DIR}bodrm{reptmon}.parquet")
print(f"  {OUTPUT_MISB_DIR}bodrm{reptmon}.sas7bdat")

# Close SAS session
sas.endsas()
