import polars as pl
from datetime import datetime, timedelta

LOAN_REPTDATE = 'data/loan/reptdate.parquet'
IMNITB_DIR = 'data/imnitb/'
MNITB_DIR = 'data/mnitb/'
IMNIFD_DIR = 'data/imnifd/'
MNIFD_DIR = 'data/mnifd/'
OCM_DIR = 'data/ocm/'
CISCA_DIR = 'data/cisca/'
CISDP_DIR = 'data/cisdp/'
COLL_DIR = 'data/coll/'
ICOLL_DIR = 'data/icoll/'
MISCA_DIR = 'data/misca/'
RNID_DIR = 'data/rnid/'
IRNID_DIR = 'data/irnid/'
PIDMWH_DIR = 'data/pidmwh/'
PIDM_DIR = 'data/pidm/'
BNM_DIR = 'data/bnm/'
IBNM_DIR = 'data/ibnm/'

reptdate = datetime(datetime.today().year, 1, 1) - timedelta(days=1)
day = reptdate.day

if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = f'{reptdate.year % 100:02d}'
reptmon = f'{reptdate.month:02d}'
tdate = reptdate

df_imnitb_uma = pl.read_parquet(f'{IMNITB_DIR}uma.parquet').with_columns([
    pl.lit('PIBB').alias('BNKIND')
])
df_mnitb_uma = pl.read_parquet(f'{MNITB_DIR}uma.parquet').with_columns([
    pl.lit('PBB ').alias('BNKIND')
])

df_uma_combined = pl.concat([df_imnitb_uma, df_mnitb_uma])

df_uma = df_uma_combined.with_columns([
    pl.when((pl.col('INSTRUCTIONS').str.split(' ').list.first() == 'CHNGE') |
            (pl.col('INSTRUCTIONS').str.split(' ').list.get(-2) == 'TYP'))
    .then(pl.col('INSTRUCTIONS').str.slice(-3, 3))
    .otherwise(pl.lit(None))
    .alias('ORIPRODUCT_STR'),
    pl.lit(1).alias('UCLAIMEDMONIES')
]).with_columns([
    pl.when(pl.col('ORIPRODUCT_STR').is_not_null())
    .then(pl.col('ORIPRODUCT_STR').cast(pl.Int32, strict=False))
    .otherwise(pl.lit(None))
    .alias('PRODUCT1')
]).with_columns([
    pl.when((pl.col('PRODUCT1').is_null()) & (pl.col('BNKIND') == 'PIBB'))
    .then(pl.lit(204))
    .when((pl.col('PRODUCT1').is_null()) & (pl.col('BNKIND') == 'PBB '))
    .then(pl.lit(200))
    .when(pl.col('PRODUCT1') == 297)
    .then(pl.lit(200))
    .when(pl.col('PRODUCT1') == 298)
    .then(pl.lit(204))
    .otherwise(pl.col('PRODUCT1'))
    .alias('PRODUCT')
]).drop(['PRODUCT1', 'ORIPRODUCT_STR'])

df_uma_pbb = df_uma.filter(pl.col('BNKIND') == 'PBB ')
df_uma_pibb = df_uma.filter(pl.col('BNKIND') == 'PIBB')

df_uma_pbb.write_parquet(f'{BNM_DIR}uma.parquet')
df_uma_pibb.write_parquet(f'{IBNM_DIR}uma.parquet')

df_current_i = pl.read_parquet(f'{IMNITB_DIR}current.parquet')
df_saving_i = pl.read_parquet(f'{IMNITB_DIR}saving.parquet')
df_fd_i = pl.read_parquet(f'{IMNIFD_DIR}fd.parquet')
df_uma_i = pl.read_parquet(f'{IBNM_DIR}uma.parquet')

df_current_m = pl.read_parquet(f'{MNITB_DIR}current.parquet')
df_saving_m = pl.read_parquet(f'{MNITB_DIR}saving.parquet')
df_fd_m = pl.read_parquet(f'{MNIFD_DIR}fd.parquet')
df_uma_m = pl.read_parquet(f'{BNM_DIR}uma.parquet')

df_deposit = pl.concat([
    df_current_i, df_saving_i, df_fd_i, df_uma_i,
    df_current_m, df_saving_m, df_fd_m, df_uma_m
])

df_deposit = df_deposit.with_columns([
    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.lit(1))
    .otherwise(pl.lit(2))
    .alias('BUSSTYPE'),
    
    pl.when(pl.col('DEPTYPE').is_in(['D','N']))
    .then(pl.lit(1))
    .when(pl.col('DEPTYPE') == 'S')
    .then(pl.lit(2))
    .when(pl.col('DEPTYPE') == 'C')
    .then(pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
         .then(pl.lit(6))
         .otherwise(pl.lit(3)))
    .otherwise(pl.col('DEPTYPE'))
    .alias('DEPTYPE'),
    
    pl.when(pl.col('UCLAIMEDMONIES') != 1)
    .then(pl.lit(2))
    .otherwise(pl.col('UCLAIMEDMONIES'))
    .alias('UCLAIMEDMONIES'),
    
    pl.lit(1).alias('INSURABILITY'),
    pl.lit(0).alias('SPECIALACC'),
    
    pl.when(pl.col('PURPOSE') == '4')
    .then(pl.lit(1))
    .otherwise(pl.lit(2))
    .alias('STAFFCODE'),
    
    pl.when(pl.col('PURPOSE').is_in(['1','4','I']))
    .then(pl.lit(1))
    .when(pl.col('PURPOSE').is_in(['2','H']))
    .then(pl.lit(2))
    .when(pl.col('PURPOSE').is_in(['5','6']))
    .then(pl.lit(3))
    .when(pl.col('PURPOSE') == '7')
    .then(pl.lit(4))
    .when(pl.col('PURPOSE') == '8')
    .then(pl.lit(5))
    .otherwise(pl.lit(6))
    .alias('ACCTYPE'),
    
    pl.col('STATCD').str.slice(3, 1).alias('ACSTATUS4'),
    pl.col('STATCD').str.slice(4, 1).alias('ACSTATUS5'),
    pl.col('STATCD').str.slice(5, 1).alias('ACSTATUS6')
]).sort('ACCTNO')

df_deposit.write_parquet(f'{PIDM_DIR}deposit.parquet')

df_card = pl.read_parquet(f'{OCM_DIR}BANKCARD{reptmon}{reptyear}.parquet').select([
    pl.col('ACCTNO1').alias('ACCTNO')
]).unique()

df_ebank = pl.DataFrame({
    'ACCTNO': [],
    'EBANK': []
}).cast({'ACCTNO': pl.Int64, 'EBANK': pl.Utf8})

df_deposit = df_deposit.join(df_card, on='ACCTNO', how='left', suffix='_card')
df_deposit = df_deposit.join(df_ebank, on='ACCTNO', how='left', suffix='_ebank')

df_deposit = df_deposit.with_columns([
    pl.when((pl.col('ACCTNO_card').is_not_null()) & (pl.col('EBANK').is_not_null()))
    .then(pl.lit(2))
    .when(pl.col('ACCTNO_card').is_not_null())
    .then(pl.lit(1))
    .when(pl.col('EBANK').is_not_null())
    .then(pl.lit(3))
    .otherwise(pl.lit(4))
    .alias('ATMCARD')
]).drop(['ACCTNO_card', 'EBANK'])

df_cisca = pl.read_parquet(f'{CISCA_DIR}deposit.parquet')
df_cisdp = pl.read_parquet(f'{CISDP_DIR}deposit.parquet')

df_cisinfo = pl.concat([df_cisca, df_cisdp])

df_cisinfo = df_cisinfo.filter(
    ~((pl.col('SECCUST') == '902') & 
      pl.col('CACCCODE').is_in(['014','017','018','019','021']))
).with_columns([
    pl.when(pl.col('GENDER') == 'M')
    .then(pl.lit(1))
    .when(pl.col('GENDER') == 'F')
    .then(pl.lit(2))
    .otherwise(pl.lit(0))
    .alias('GENDER'),
    
    pl.when(pl.col('RACE') == '1')
    .then(pl.lit(1))
    .when(pl.col('RACE') == '2')
    .then(pl.lit(3))
    .when(pl.col('RACE') == '3')
    .then(pl.lit(4))
    .when(pl.col('RACE') == '4')
    .then(pl.lit(2))
    .when(pl.col('RACE') == 'OTHER')
    .then(pl.lit(5))
    .otherwise(pl.lit(6))
    .alias('RACE'),
    
    pl.col('NEWIC').str.replace_all(' ', '').alias('NEWIC')
]).sort('ACCTNO')

df_cisinfo.write_parquet(f'{PIDM_DIR}cisinfo.parquet')

df_cisdeposit = df_deposit.drop(['RACE', 'MAILCODE']).join(
    df_cisinfo, on='ACCTNO', how='left'
).filter(
    (pl.col('SECCUST') == '901') &
    (~pl.col('PRODUCT').is_in([30, 31, 32, 33, 34]))
)

df_cisdeposit.write_parquet(f'{PIDM_DIR}cisdeposit.parquet')

df_forate = pl.read_parquet(f'{MISCA_DIR}foratebkp.parquet').filter(
    pl.col('REPTDATE') <= tdate
).sort(['CURCODE', 'REPTDATE'], descending=[False, True]).unique(subset=['CURCODE'])

forate_dict = dict(zip(df_forate['CURCODE'].to_list(), df_forate['SPOTRATE'].to_list()))

df_fdcd = pl.concat([
    pl.read_parquet(f'{IMNIFD_DIR}fd.parquet'),
    pl.read_parquet(f'{MNIFD_DIR}fd.parquet')
]).with_columns([
    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.lit(1))
    .otherwise(pl.lit(2))
    .alias('BUSSTYPE'),
    
    pl.when(pl.col('MATID') == 'M')
    .then(pl.col('TERM') * 30)
    .otherwise(pl.col('TERM'))
    .alias('TENOR'),
    
    pl.col('CURBAL').alias('AVAILABLEBAL'),
    pl.col('CURBAL').alias('LEDGERBAL'),
    pl.col('CURBAL').alias('FXAVAILABLEBAL'),
    pl.col('RATE').alias('RATENETGIARATE')
]).with_columns([
    pl.when(pl.col('BUSSTYPE') == 1)
    .then((pl.col('RATE') * 100) / pl.col('PRORATIO').str.slice(0, 2).cast(pl.Float64))
    .otherwise(pl.lit(0))
    .alias('GROSSRATE'),
    
    pl.when(pl.col('BUSSTYPE') == 1)
    .then(pl.col('PRORATIO').str.slice(0, 2).cast(pl.Int32))
    .otherwise(pl.lit(0))
    .alias('PSR'),
    
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('CURCODE').map_dict(forate_dict, default=1.0))
    .otherwise(pl.lit(0))
    .alias('FXRATE')
]).select([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('CDNO').alias('CERTNUM'),
    'PLACEMENTRENEWDATE', 'MATURITYDATE', 'GROSSRATE', 'PSR',
    'TENOR', 'LASTINTDIVPMT', 'RATENETGIARATE', 'AVAILABLEBAL',
    'FXAVAILABLEBAL', pl.col('CURCODE').alias('CURRENCYCODE'),
    'FXLEDGERBAL', 'LEDGERBAL'
]).sort(['ACCNUM', 'CERTNUM'])

df_fdcd.write_parquet(f'{PIDM_DIR}fdcd.parquet')

df_coll = pl.concat([
    pl.read_parquet(f'{COLL_DIR}collater.parquet'),
    pl.read_parquet(f'{ICOLL_DIR}collater.parquet')
]).select([
    pl.col('FDACCTNO'),
    pl.col('ACCTNO').alias('COLLACCTNO')
]).unique()

df_master = df_cisdeposit.join(
    df_coll.rename({'FDACCTNO': 'ACCTNO'}), on='ACCTNO', how='left'
).with_columns([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('CURCODE').alias('CURRENCYCODE'),
    pl.col('CUSTNAME').alias('ACCNAME'),
    pl.col('CUSTNO').alias('CIFNUM'),
    pl.col('BRANCH').alias('BRANCHCODE'),
    pl.col('PRODUCT').alias('DEPCODE'),
    pl.col('PRIPHONE').alias('TELHOME'),
    pl.col('SECPHONE').alias('TELOFFICE'),
    pl.col('MOBIPHON').alias('TELHP'),
    
    pl.col('NEWIC').alias('REGNUM'),
    pl.lit('').alias('APPROVERACCOPEN'),
    pl.lit(0).alias('FXRATE'),
    pl.lit(0).alias('FXLEDGERBAL'),
    pl.lit(0).alias('FXAVAILABLEBAL'),
    pl.lit(0).alias('FXBILLPAYABLE'),
    pl.lit(0).alias('FXACCRUEDINT'),
    pl.lit(0).alias('FXINTPROFTODATE'),
    
    pl.col('LEDGBAL').alias('LEDGERBAL'),
    pl.col('CURBAL').alias('AVAILABLEBAL'),
    pl.col('INTPAYBL').alias('BILLPAYABLE'),
    pl.col('INTPD').alias('ACCRUEDINT'),
    pl.col('INTYTD').alias('INTPROFTODATE'),
    
    pl.lit(0).alias('NOMINALVALUES'),
    pl.lit(0).alias('FXNOMINALVALUE'),
    pl.lit(0).alias('PROCEEDVALUE'),
    pl.lit(0).alias('FXPROCEEDVALUE')
]).with_columns([
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('CURCODE').map_dict(forate_dict, default=1.0))
    .otherwise(pl.lit(0))
    .alias('FXRATE')
]).with_columns([
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('LEDGBAL') * pl.col('FXRATE'))
    .otherwise(pl.lit(0))
    .alias('FXLEDGERBAL'),
    
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('CURBAL') / pl.col('FXRATE'))
    .otherwise(pl.lit(0))
    .alias('FXAVAILABLEBAL'),
    
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('INTPAYBL') * pl.col('FXRATE'))
    .otherwise(pl.lit(0))
    .alias('FXBILLPAYABLE'),
    
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('INTPD') * pl.col('FXRATE'))
    .otherwise(pl.lit(0))
    .alias('FXACCRUEDINT'),
    
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(pl.col('INTYTD') * pl.col('FXRATE'))
    .otherwise(pl.lit(0))
    .alias('FXINTPROFTODATE')
]).with_columns([
    pl.when((pl.col('HOLDIND') == 'Y') & pl.col('COLLACCTNO').is_not_null())
    .then(pl.when((pl.col('ACSTATUS4') == 'H') | (pl.col('ACSTATUS5') == 'H') | (pl.col('ACSTATUS6') == 'H'))
         .then(pl.lit(4))
         .otherwise(pl.lit(1)))
    .when((pl.col('HOLDIND') == 'Y') & pl.col('COLLACCTNO').is_null())
    .then(pl.when((pl.col('ACSTATUS4') == 'H') | (pl.col('ACSTATUS5') == 'H') | (pl.col('ACSTATUS6') == 'H'))
         .then(pl.lit(5))
         .otherwise(pl.lit(2)))
    .when(pl.col('POSTIND').is_not_null())
    .then(pl.when((pl.col('ACSTATUS4') == 'H') | (pl.col('ACSTATUS5') == 'H') | (pl.col('ACSTATUS6') == 'H'))
         .then(pl.lit(5))
         .otherwise(pl.lit(2)))
    .when((pl.col('ACSTATUS4') == 'H') | (pl.col('ACSTATUS5') == 'H') | (pl.col('ACSTATUS6') == 'H'))
    .then(pl.lit(3))
    .otherwise(pl.lit(0))
    .alias('TAGGING')
]).with_columns([
    pl.when((pl.col('TAGGING') == 1) | (pl.col('TAGGING') == 4))
    .then(pl.col('COLLACCTNO').cast(pl.Utf8))
    .when(((pl.col('TAGGING') == 2) | (pl.col('TAGGING') == 5)) & (pl.col('HOLDIND') == 'Y'))
    .then(pl.lit('EARMARK'))
    .when(pl.col('TAGGING') == 3)
    .then(pl.lit('DECEASED ACCOUNT'))
    .when(pl.col('POSTIND') == 'A')
    .then(pl.lit('POST NO CHEQUE'))
    .when(pl.col('POSTIND') == 'C')
    .then(pl.lit('POST NO CREDIT'))
    .when(pl.col('POSTIND') == 'X')
    .then(pl.lit('POST NO DEBIT'))
    .when(pl.col('POSTIND') == 'T')
    .then(pl.lit('POST NO TRANSACTION'))
    .otherwise(pl.lit('0'))
    .alias('CONDOFTAGGING')
])

df_master.write_parquet(f'{PIDMWH_DIR}master.parquet')

df_holder = df_deposit.select(['ACCTNO']).join(
    df_cisinfo, on='ACCTNO', how='left'
).with_columns([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('CUSTNAME').alias('ACCNAME'),
    pl.col('CUSTNAM1').alias('HOLDERNAME'),
    pl.col('CUSTNO').alias('CIFNUM'),
    pl.col('NEWIC').alias('NEWIDNUM'),
    pl.col('OLDIC').alias('OLDIDNUM'),
    
    pl.when(pl.col('NEWICIND').is_in(['ML','PL']))
    .then(pl.col('NEWIC'))
    .otherwise(pl.lit('0'))
    .alias('POLICEARMYID'),
    
    pl.when(pl.col('NEWICIND') == 'PP')
    .then(pl.col('NEWIC'))
    .otherwise(pl.lit('0'))
    .alias('PASSPORTNUM'),
    
    pl.when(pl.col('CITIZEN') == 'MY')
    .then(pl.lit(1))
    .otherwise(pl.lit(2))
    .alias('RESIDENT'),
    
    pl.col('BIRTHDAT').cast(pl.Date).alias('DATEOFBIRTH')
])

df_holder.write_parquet(f'{PIDMWH_DIR}holder.parquet')

df_timedepo = df_cisdeposit.filter(
    pl.col('DEPTYPE').is_in([3, 5, 6])
).with_columns([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('CURCODE').alias('CURRENCYCODE'),
    
    pl.lit(0).alias('NOMINALVALUE'),
    pl.lit(0).alias('FXNOMINALVALUE'),
    pl.lit(0).alias('FXRATE'),
    pl.lit(0).alias('FXLEDGERBAL'),
    pl.lit(0).alias('FXAVAILABLEBAL'),
    pl.lit(0).alias('FXACCRUEDINT'),
    pl.lit(0).alias('FXINTPROFTODATE'),
    
    pl.col('LEDGBAL').alias('LEDGERBAL'),
    pl.col('CURBAL').alias('AVAILABLEBAL'),
    pl.col('INTPD').alias('ACCRUEDINT'),
    pl.col('INTYTD').alias('INTPROFTODATE'),
    
    pl.lit(0).alias('TENOR'),
    pl.lit(0).alias('RATENETGIARATE'),
    pl.lit(0).alias('GROSSRATE'),
    pl.lit(0).alias('PSR'),
    pl.lit(0).alias('PROCEEDVALUE'),
    pl.lit(0).alias('FXPROCEEDVALUE')
]).join(df_fdcd, on='ACCNUM', how='left', suffix='_fd')

df_timedepo.write_parquet(f'{PIDMWH_DIR}timedepo.parquet')

df_beneficiary = df_cisdeposit.with_columns([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('CURCODE').alias('CURRENCYCODE'),
    pl.col('CUSTNAME').alias('ACCNAME'),
    pl.lit('').alias('BENEFICIARYNAME'),
    pl.lit('').alias('BENEFICIARYID'),
    pl.lit('').alias('BENEFICIARYADD'),
    pl.lit(0).alias('BENEFICIARYINT')
])

df_beneficiary.write_parquet(f'{PIDMWH_DIR}beneficiary.parquet')

df_address = df_cisdeposit.with_columns([
    pl.col('ACCTNO').alias('ACCNUM'),
    pl.col('ADDRLN1').alias('RESIDENTADD1'),
    pl.col('ADDRLN2').alias('RESIDENTADD2'),
    pl.col('ADDRLN3').alias('RESIDENTADD3'),
    pl.col('ADDRLN4').alias('RESIDENTADD4'),
    pl.col('MAILCODE').alias('POSTCODE'),
    pl.col('ADDRLN1').alias('MAILADD1'),
    pl.col('ADDRLN2').alias('MAILADD2'),
    pl.col('ADDRLN3').alias('MAILADD3'),
    pl.col('ADDRLN4').alias('MAILADD4')
])

df_address.write_parquet(f'{PIDMWH_DIR}address.parquet')

df_rnid = pl.read_parquet(f'{RNID_DIR}RNIDM{reptmon}.parquet')
df_irnid = pl.read_parquet(f'{IRNID_DIR}RNIDM{reptmon}.parquet')

df_rnid.write_parquet(f'{PIDM_DIR}rnid.parquet')
df_irnid.write_parquet(f'{PIDM_DIR}irnid.parquet')

print(f"PIDM Deposit Insurance Reporting Complete")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nOutput files generated:")
print(f"  {PIDMWH_DIR}master.parquet")
print(f"  {PIDMWH_DIR}holder.parquet")
print(f"  {PIDMWH_DIR}timedepo.parquet")
print(f"  {PIDMWH_DIR}beneficiary.parquet")
print(f"  {PIDMWH_DIR}address.parquet")
print(f"  {PIDM_DIR}rnid.parquet")
print(f"  {PIDM_DIR}irnid.parquet")
