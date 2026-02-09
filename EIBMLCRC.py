import polars as pl
from datetime import datetime, timedelta

DP_REPTDATE = 'data/dp/reptdate.parquet'
CIS_CUSTDLY = 'data/cis/custdly.parquet'
CISCA_DEPOSIT = 'data/cisca/deposit.parquet'
CISSAFD_DEPOSIT = 'data/cissafd/deposit.parquet'
CISLN_LOAN = 'data/cisln/loan.parquet'
LN_DIR = 'data/ln/'
ILN_DIR = 'data/iln/'
CRM_DIR = 'data/crm/'
SDB_DIR = 'data/sdb/'
LCR_DIR = 'data/lcr/'
ILCR_DIR = 'data/ilcr/'

df_reptdate = pl.read_parquet(DP_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
weekday = str(reptdate.weekday() + 1)
reptdt = reptdate

df_ciseq = pl.read_parquet(CIS_CUSTDLY).filter(
    (pl.col('ACCTCODE') == 'EQC') & (pl.col('PRISEC') == 901)
).with_columns([
    pl.when((pl.col('NEWIC') == '') | (pl.col('NEWIC').str.slice(0, 5) == '99999'))
      .then(pl.concat_str([pl.col('ALIASKEY'), pl.col('CUSTNO').cast(pl.Utf8).str.zfill(20)], separator=''))
      .otherwise(pl.concat_str([pl.col('ALIASKEY'), pl.col('ALIAS')], separator=''))
      .alias('ICNO')
]).select(['ACCTNO', 'CUSTNO', 'PRISEC', 'ALIASKEY', 'ALIAS', 'CUSTNAME', 'ICNO']).rename({
    'CUSTNO': 'CISNO',
    'CUSTNAME': 'CISNAME'
})

df_ciseq.sort('ACCTNO').write_parquet(f'{LCR_DIR}CISEQ.parquet')
df_ciseq.sort('ACCTNO').write_parquet(f'{ILCR_DIR}CISEQ.parquet')

df_cisca = pl.read_parquet(CISCA_DEPOSIT)
df_cissafd = pl.read_parquet(CISSAFD_DEPOSIT)
df_cisdp = pl.concat([df_cisca, df_cissafd])

df_cisdp = df_cisdp.filter(
    ~((pl.col('CACCCODE').is_in(['014', '017', '018', '019', '021'])) & 
      (pl.col('SECCUST') == '902'))
).with_columns([
    pl.when((pl.col('NEWIC') == '') | (pl.col('NEWIC').str.slice(0, 5) == '99999'))
      .then(pl.concat_str([pl.col('NEWICIND'), pl.col('CUSTNO').cast(pl.Utf8).str.zfill(20)], separator=''))
      .otherwise(pl.concat_str([pl.col('NEWICIND'), pl.col('NEWIC')], separator=''))
      .alias('ICNO'),
    pl.lit(1).alias('CNT')
]).select(['ACCTNO', 'CUSTNO', 'SECCUST', 'NEWICIND', 'NEWIC', 'CUSTNAME', 'ICNO', 'CNT']).sort('ACCTNO')

df_cisicno = df_cisdp.group_by('ACCTNO').agg([
    pl.col('CNT').sum().alias('CNTIC')
])

df_cisicno = df_cisicno.join(
    df_cisdp.select(['ACCTNO', 'ICNO', 'CUSTNAME']),
    on='ACCTNO',
    how='left'
).sort(['ACCTNO', 'CNTIC'])

df_trnscisic = df_cisicno.group_by(['ACCTNO', 'CNTIC']).agg([
    pl.col('ICNO').str.concat(delimiter=',').alias('ICGRP')
]).select(['ACCTNO', 'CNTIC', 'ICGRP']).sort('ACCTNO')

df_cisdpacc = df_cisdp.filter(pl.col('SECCUST') == '901').group_by('ICNO').agg([
    pl.col('CNT').sum().alias('CNTDPACC')
])

df_cisln = pl.read_parquet(CISLN_LOAN).filter(
    ~((pl.col('CACCCODE').is_in(['014', '017', '018', '019', '021'])) & 
      (pl.col('SECCUST') == '902'))
).with_columns([
    pl.when((pl.col('NEWIC') == '') | (pl.col('NEWIC').str.slice(0, 5) == '99999'))
      .then(pl.concat_str([pl.col('NEWICIND'), pl.col('CUSTNO').cast(pl.Utf8).str.zfill(20)], separator=''))
      .otherwise(pl.concat_str([pl.col('NEWICIND'), pl.col('NEWIC')], separator=''))
      .alias('ICNO'),
    pl.lit(1).alias('CNT')
]).select(['ACCTNO', 'CUSTNO', 'SECCUST', 'NEWICIND', 'NEWIC', 'CUSTNAME', 'ICNO', 'CNT']).sort('ACCTNO')

df_lnloan = pl.read_parquet(f'{LN_DIR}LOAN{reptmon}4.parquet')
df_lnwof = pl.read_parquet(f'{LN_DIR}LNWOF{reptmon}4.parquet')
df_lnacc = pl.concat([df_lnloan, df_lnwof])

today = datetime.now().date()
first_of_month = datetime(today.year, today.month, 1).date()

df_lnacc = df_lnacc.filter(
    ~((pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)) &
    (pl.col('BRANCH') != 998) &
    ~pl.col('PAIDIND').is_in(['P', 'C']) &
    ~pl.col('PRODUCT').is_in([199, 983]) &
    ((pl.col('ISSDTE').cast(pl.Date) - first_of_month).dt.total_days() / 30.4375 >= 12)
).select(['ACCTNO']).unique()

df_lnacc = df_cisln.join(df_lnacc, on='ACCTNO', how='inner').sort('ICNO')

df_cislnacc = df_lnacc.filter(pl.col('SECCUST') == '901').group_by('ICNO').agg([
    pl.col('CNT').sum().alias('CNTLNACC')
])

df_combinelndp = df_cislnacc.join(df_cisdpacc, on='ICNO', how='outer_coalesce').join(
    df_cisdp.select(['ICNO', 'ACCTNO', 'SECCUST']),
    on='ICNO',
    how='left'
).with_columns([
    pl.when((pl.col('CNTDPACC').fill_null(0) >= 1) & (pl.col('CNTLNACC').fill_null(0) >= 1))
      .then(pl.lit('R '))
      .otherwise(pl.lit('NR'))
      .alias('SIGN')
]).filter(pl.col('SECCUST') == '901').sort('ACCTNO')

df_trnscisic = df_trnscisic.join(
    df_combinelndp,
    on='ACCTNO',
    how='left'
).select(['ACCTNO', 'ICGRP', 'CNTIC', 'SIGN'])

df_ilnloan = pl.read_parquet(f'{ILN_DIR}LOAN{reptmon}4.parquet')
df_ilnwof = pl.read_parquet(f'{ILN_DIR}LNWOF{reptmon}4.parquet')
df_ilnacc = pl.concat([df_ilnloan, df_ilnwof])

df_ilnacc = df_ilnacc.filter(
    ~((pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)) &
    (pl.col('BRANCH') != 998) &
    ~pl.col('PAIDIND').is_in(['P', 'C']) &
    ~pl.col('PRODUCT').is_in([199, 983]) &
    ((pl.col('ISSDTE').cast(pl.Date) - first_of_month).dt.total_days() / 30.4375 >= 12)
).select(['ACCTNO']).unique()

df_ilnacc = df_cisln.join(df_ilnacc, on='ACCTNO', how='inner').sort('ICNO')

df_icislnacc = df_ilnacc.filter(pl.col('SECCUST') == '901').group_by('ICNO').agg([
    pl.col('CNT').sum().alias('CNTLNACC')
])

df_icombinelndp = df_icislnacc.join(df_cisdpacc, on='ICNO', how='outer_coalesce').join(
    df_cisdp.select(['ICNO', 'ACCTNO', 'SECCUST']),
    on='ICNO',
    how='left'
).with_columns([
    pl.when((pl.col('CNTDPACC').fill_null(0) >= 1) & (pl.col('CNTLNACC').fill_null(0) >= 1))
      .then(pl.lit('R '))
      .otherwise(pl.lit('NR'))
      .alias('SIGN')
]).filter(pl.col('SECCUST') == '901').sort('ACCTNO')

df_itrnscisic = df_trnscisic.join(
    df_icombinelndp,
    on='ACCTNO',
    how='left'
).select(['ACCTNO', 'ICGRP', 'CNTIC', 'SIGN'])

transactional_channels = [223, 224, 225, 226, 227, 228, 242, 284, 321, 322, 323, 324,
                          325, 326, 328, 331, 333, 341, 342, 501, 502, 503, 505, 511,
                          512, 513, 515, 526, 527, 645, 648, 669]

df_dpbtran = pl.concat([
    pl.read_parquet(f'{CRM_DIR}DPBTRAN{reptyear}{reptmon}01.parquet'),
    pl.read_parquet(f'{CRM_DIR}DPBTRAN{reptyear}{reptmon}02.parquet'),
    pl.read_parquet(f'{CRM_DIR}DPBTRAN{reptyear}{reptmon}03.parquet'),
    pl.read_parquet(f'{CRM_DIR}DPBTRAN{reptyear}{reptmon}04.parquet')
])

df_tranxacc = df_dpbtran.filter(
    pl.col('CHANNEL').is_in(transactional_channels) |
    ((pl.col('CHANNEL').is_in([290, 296])) & (pl.col('TRANCODE') == 626))
).with_columns([
    pl.lit(1).alias('CNT'),
    pl.lit('Y').alias('INITCUST')
]).select(['ACCTNO', 'CNT', 'INITCUST']).sort(['ACCTNO', 'INITCUST'])

df_tranxacc = df_tranxacc.group_by(['ACCTNO', 'INITCUST']).agg([
    pl.col('CNT').sum()
])

df_sdb = pl.read_parquet(f'{SDB_DIR}SDB{reptmon}4{reptyear}.parquet').rename({'ACCTNO': 'ACCTNOC'})

df_sdb = df_sdb.with_columns([
    pl.col('ACCTNOC').str.replace_all('-', '').cast(pl.Int64).alias('ACCTNO')
]).with_columns([
    pl.when(((reptdt - pl.col('OPENDT').cast(pl.Date)).dt.total_days() / 365.25) >= 1)
      .then(pl.lit('R '))
      .otherwise(pl.lit('NR'))
      .alias('SIGN'),
    pl.lit('Y').alias('SDB')
]).select(['ACCTNO', 'SDB', 'SIGN', 'OPENDT'])

df_sdb.write_parquet(f'{LCR_DIR}SDBM.parquet')

df_sdb = df_sdb.filter(
    (pl.col('ACCTNO') > 0) & (pl.col('SIGN') == 'R ')
).sort(['ACCTNO', 'OPENDT']).select(['ACCTNO', 'SDB', 'SIGN']).unique()

df_lcr_trnscisic = df_trnscisic.join(df_tranxacc, on='ACCTNO', how='left').join(
    df_sdb, on='ACCTNO', how='left'
).with_columns([
    pl.when(pl.col('CNT').fill_null(0) >= 1)
      .then(pl.lit(1))
      .otherwise(pl.lit(None))
      .alias('TRX'),
    pl.when(pl.col('INITCUST').is_null())
      .then(pl.lit('N'))
      .otherwise(pl.col('INITCUST'))
      .alias('INITCUST')
])

df_lcr_trnscisic.write_parquet(f'{LCR_DIR}TRNSCISIC.parquet')

df_ilcr_trnscisic = df_itrnscisic.join(df_tranxacc, on='ACCTNO', how='left').join(
    df_sdb, on='ACCTNO', how='left'
).with_columns([
    pl.when(pl.col('CNT').fill_null(0) >= 1)
      .then(pl.lit(1))
      .otherwise(pl.lit(None))
      .alias('TRX'),
    pl.when(pl.col('INITCUST').is_null())
      .then(pl.lit('N'))
      .otherwise(pl.col('INITCUST'))
      .alias('INITCUST')
])

df_ilcr_trnscisic.write_parquet(f'{ILCR_DIR}TRNSCISIC.parquet')

print(f"Data files generated:")
print(f"  {LCR_DIR}CISEQ.parquet")
print(f"  {ILCR_DIR}CISEQ.parquet")
print(f"  {LCR_DIR}SDBM.parquet")
print(f"  {LCR_DIR}TRNSCISIC.parquet")
print(f"  {ILCR_DIR}TRNSCISIC.parquet")
print(f"\nCustomer equity accounts: {len(df_ciseq)}")
print(f"Deposit accounts with IC grouping: {len(df_lcr_trnscisic)}")
print(f"Islamic deposit accounts with IC grouping: {len(df_ilcr_trnscisic)}")
