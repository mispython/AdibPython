import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

LOAN_REPTDATE = 'data/loan/reptdate.parquet'
BNM_DIR = 'data/bnm/'
BTRSA_DIR = 'data/btrsa/'
PBA01_DIR = 'data/pba01/'
BNMDAILY_DIR = 'data/bnmdaily/'
MTD_DIR = 'data/mtd/'
BT_DIR = 'data/bt/'

df_reptdate = pl.read_parquet(LOAN_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'
    wk2, wk3 = '2', '1'

mm1 = mm - 1 if wk != '1' else (12 if mm == 1 else mm - 1)
mm1 = 12 if mm1 == 0 else mm1

sdate = datetime(year, mm, sdd).date()
startdt = datetime(year, mm, 1).date()
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = f'{year % 100:02d}'
reptday = f'{day:02d}'
nowk = wk

df_ibtdtl = pl.read_parquet(f'{BNM_DIR}IBTDTL{reptmon}{nowk}.parquet')
df_imast = pl.read_parquet(f'{BNM_DIR}IMAST{reptmon}{nowk}.parquet')

df_btradex = df_ibtdtl.filter(
    pl.col('GLMNEMO').is_in(['01010','01020','01070','1101'])
).join(
    df_imast.drop('INTRECV'), on='ACCTNO', how='inner'
).unique(subset=['ACCTNO','LIABCODE','TRANSREF'])

df_intrecv = df_ibtdtl.filter(
    pl.col('GLMNEMO').is_in(['01010','01020','01070','1101'])
).group_by(['ACCTNO','LIABCODE','TRANSREF']).agg([
    pl.col('GLTRNTOT').sum().alias('INTRECV')
])

df_intrecv = df_btradex.drop('GLTRNTOT').join(
    df_intrecv, on=['ACCTNO','LIABCODE','TRANSREF'], how='inner'
)

valid_glmnemo = ['00620','00560','00530','00640','00660','00120','00140','00160',
                 '00590','00470','00490','00440','00420','00340']

df_btrade = df_ibtdtl.filter(
    ~pl.col('GLMNEMO').is_in(valid_glmnemo)
).join(
    df_imast.drop('INTRECV'), on='ACCTNO', how='inner'
).sort(['ACCTNO','LIABCODE','TRANSREF'])

df_ibtrad = df_btrade.join(
    df_intrecv, on=['ACCTNO','LIABCODE','TRANSREF'], how='left'
).with_columns([
    pl.when(pl.col('INTRECV').is_not_null())
    .then(pl.col('GLTRNTOT') + pl.col('INTRECV'))
    .otherwise(pl.col('GLTRNTOT'))
    .alias('BALANCE'),
    pl.col('GLTRNTOT').alias('CURBAL')
])

def calculate_remain_months(exprdate, reptdate_val):
    """Calculate remaining months between two dates"""
    if exprdate is None or exprdate < reptdate_val:
        return '51'
    
    folmonth = reptdate_val
    nummonth = 0
    
    while exprdate > folmonth:
        nummonth += 1
        nextday = reptdate_val.day
        folmonth = folmonth + relativedelta(months=1)
        
        if nextday in [29,30,31] and folmonth.month == 2:
            folmonth = datetime(folmonth.year, 3, 1).date() - timedelta(days=1)
        elif nextday == 31 and folmonth.month in [4,6,9,11]:
            folmonth = folmonth.replace(day=30)
        elif nextday == 30 and folmonth.month in [1,3,5,7,8,10,12]:
            folmonth = folmonth.replace(day=31)
    
    if nummonth <= 12:
        return f'{50 + nummonth}'
    elif nummonth <= 36:
        return f'{60 + ((nummonth - 13) // 12 + 1)}'
    else:
        return f'{70 + ((nummonth - 37) // 12 + 1)}'

df_processed = []

for row in df_ibtrad.iter_rows(named=True):
    new_row = row.copy()
    
    new_row['FISSPURP'] = '0470'
    new_row['PRODUCT'] = 0
    new_row['NOTENO'] = 0
    new_row['AMTIND'] = 'I'
    
    if row['EXPRDATE'] and row['ISSDTE']:
        days_diff = (row['EXPRDATE'] - row['ISSDTE']).days
        new_row['ORIGMT'] = '10' if days_diff < 366 else '20'
    else:
        new_row['ORIGMT'] = '20'
    
    custcd = new_row.get('CUSTCD', '')
    sectorcd = new_row.get('SECTORCD', '')
    
    if custcd not in ['77','78','95','96'] and sectorcd < '1111':
        if custcd in ['02','03','04','05','06','11','12','13','17','30','31','32','33','34','35','37','38','39','45']:
            new_row['SECTORCD'] = '8110'
        elif custcd == '40':
            new_row['SECTORCD'] = '8130'
        elif custcd == '79':
            new_row['SECTORCD'] = '9203'
        elif custcd in ['71','72','73','74']:
            new_row['SECTORCD'] = '9101'
        else:
            new_row['SECTORCD'] = '9999'
    
    if custcd in ['79','61','62','63','75','57','59','41','42','43','44','46','47','48','49','51','52','53','54']:
        if sectorcd in ['8110','8120','8130']:
            new_row['SECTORCD'] = '9999'
    
    if custcd in ['02','03','04','05','06','11','12','13','17','30','31','32','33','34','35','37','38','39','40','45']:
        if sectorcd not in ['8110','8120','8130']:
            new_row['SECTORCD'] = '8110'
    
    if custcd in ['71','72','73','74']:
        if sectorcd and sectorcd[:2] not in ['91','92','93','94','95','96','97','98','99']:
            new_row['SECTORCD'] = '9101'
    
    if custcd in ['77','78'] and '1111' <= sectorcd <= '9999':
        new_row['CUSTCD'] = '41' if custcd == '77' else '44'
    
    if new_row.get('REBIND') == 2:
        if new_row.get('INTAMT') == 0.01:
            new_row['REBATE'] = -1 * new_row.get('REBATE', 0)
            new_row['INTEARN4'] = -1 * new_row.get('INTEARN4', 0)
            new_row['INTAMT'] = 0
            new_row['INTEARN'] = 0
            new_row['ACCRUAL'] = 0
            new_row['INTEARN2'] = 0
            new_row['INTEARN3'] = 0
        else:
            new_row['REBATE'] = 0
            new_row['INTEARN4'] = 0
    
    if new_row.get('CUSTCD') == 'AA':
        orgtype = new_row.get('ORGTYPE', '')
        new_row['CUSTCD'] = '66' if orgtype in ['C','P','I'] else '77'
    
    if row.get('EXPRDATE'):
        remainmt = calculate_remain_months(row['EXPRDATE'], reptdate)
        new_row['REMAINMT'] = remainmt
    else:
        new_row['REMAINMT'] = '51'
    
    if new_row.get('APPRLIM2', 0) < 0:
        balance = new_row.get('BALANCE', 0)
        new_row['APPRLIM2'] = balance if balance > 0 else 0
    
    undrawn = new_row.get('APPRLIM2', 0) - new_row.get('BALANCE', 0)
    new_row['UNDRAWN'] = undrawn if undrawn > 0 else 0
    
    df_processed.append(new_row)
    
    if row.get('PAIDIND') == 'P' and row.get('FEEAMT', 0) > 0:
        paid_row = new_row.copy()
        if row.get('EXPRDATE'):
            remainmt = calculate_remain_months(row['EXPRDATE'], reptdate)
            paid_row['REMAINMT'] = remainmt
        else:
            paid_row['REMAINMT'] = '51'
        df_processed.append(paid_row)

df_ibtrad_processed = pl.DataFrame(df_processed)

df_suba = pl.read_parquet(f'{BTRSA_DIR}ISUBA{reptday}{reptmon}.parquet').filter(
    pl.col('SINDICAT') != 'S'
)

df_subacct = df_suba.join(
    pl.read_parquet(f'{BTRSA_DIR}IMAST{reptday}{reptmon}.parquet').select(['ACCTNO','TFID']),
    on='ACCTNO', how='left'
)

df_subnew = df_subacct.filter(
    (pl.col('CREATDS') > 0) | (pl.col('EXPIRDS') > 0)
).unique(subset=['ACCTNO','TRANSREF']).with_columns([
    pl.col('TRANSREF').alias('TRANSREX'),
    pl.when(pl.col('CREATDS').is_not_null() & (pl.col('CREATDS') != 0))
    .then(pl.col('CREATDS').cast(pl.Date))
    .alias('ISSDTE'),
    pl.when(pl.col('EXPIRDS').is_not_null() & (pl.col('EXPIRDS') != 0))
    .then(pl.col('EXPIRDS').cast(pl.Date))
    .alias('EXPRDATE')
]).select(['ACCTNO','TRANSREF','ISSDTE','EXPRDATE'])

df_ibtrad_merged = df_ibtrad_processed.join(
    df_subnew, on=['ACCTNO','TRANSREF'], how='left', suffix='_new'
).with_columns([
    pl.when(pl.col('ISSDTE_new').is_not_null())
    .then(pl.col('ISSDTE_new'))
    .otherwise(pl.col('ISSDTE'))
    .alias('ISSDTE'),
    pl.when(pl.col('EXPRDATE_new').is_not_null())
    .then(pl.col('EXPRDATE_new'))
    .otherwise(pl.col('EXPRDATE'))
    .alias('EXPRDATE')
]).drop(['ISSDTE_new','EXPRDATE_new'])

df_ibtrad_final = df_ibtrad_merged.with_columns([
    pl.when(pl.col('APPRLIM2') < 0)
    .then(pl.when(pl.col('BALANCE') > 0).then(pl.col('BALANCE')).otherwise(0))
    .otherwise(pl.col('APPRLIM2'))
    .alias('APPRLIM2'),
    (pl.col('APPRLIM2') - pl.col('CURBAL')).alias('UNDRAWN')
]).with_columns([
    pl.when(pl.col('UNDRAWN') < 0).then(0).otherwise(pl.col('UNDRAWN')).alias('UNDRAWN')
])

df_ibtrad_d = df_ibtrad_final.filter(pl.col('DIRCTIND') == 'D')
df_ibtradi = df_ibtrad_final.filter(pl.col('DIRCTIND') == 'I')

df_ibtrad_d.write_parquet(f'{BNM_DIR}IBTRAD{reptmon}{nowk}.parquet')
df_ibtradi.write_parquet(f'{BNM_DIR}IBTRADI{reptmon}{nowk}.parquet')

disbpay_glmnemo = ['00110','00130','00150','00170','00210','00230','00240','00250',
                   '00260','00270','00280','00510','00540','00600','00800','00810',
                   '00820','1011','1101','1054','1060','7101','8101','08000','09000']

df_icomm = pl.read_parquet(f'{BTRSA_DIR}ICOMM{reptday}{reptmon}.parquet')

df_prngl = df_icomm.filter(
    (pl.col('ISSDTE') <= reptdate) &
    (~pl.col('GLMNEMO').is_in(['CONI','CONM','CONP','DDA','HOE','IBS'])) &
    (~pl.col('GLMNEMO').str.starts_with('9'))
).with_columns([
    pl.when(pl.col('GLMNEMO').str.starts_with('0'))
    .then(pl.col('GLMNEMO') + '0')
    .otherwise(pl.col('GLMNEMO'))
    .alias('GLMNEMO'),
    pl.when(pl.col('PAYGRNO').is_not_null())
    .then(pl.col('PAYGRNO').str.slice(0,7))
    .otherwise(pl.col('TRANSREF'))
    .alias('TRANSREF')
])

df_disbpay = df_prngl.filter(
    pl.col('GLMNEMO').is_in(disbpay_glmnemo) &
    (pl.col('ISSDTE') >= startdt) & (pl.col('ISSDTE') <= reptdate)
).with_columns([
    pl.when((pl.col('TRANSAMT') > 0) & (~pl.col('GLMNEMO').is_in(['08000','09000'])))
    .then(pl.when(~pl.col('GLMNEMO').is_in(['1101','7101','8101']))
          .then(pl.col('TRANSAMT')).otherwise(0))
    .otherwise(0)
    .alias('DISBURSE'),
    pl.when((pl.col('TRANSAMT') <= 0) | pl.col('GLMNEMO').is_in(['08000','09000']))
    .then(pl.col('TRANSAMT') * -1)
    .otherwise(0)
    .alias('REPAID'),
    pl.when((pl.col('TRANSAMT') <= 0) | pl.col('GLMNEMO').is_in(['08000','09000']))
    .then(pl.when(~pl.col('GLMNEMO').is_in(['1101','7101','8101']))
          .then(pl.col('TRANSAMT') * -1).otherwise(0))
    .otherwise(0)
    .alias('REPAIDPRIN')
])

df_disbpay_agg = df_disbpay.group_by(['ACCTNO','TRANSREF']).agg([
    pl.col('DISBURSE').sum(),
    pl.col('REPAID').sum(),
    pl.col('REPAIDPRIN').sum()
])

df_ibtrad_with_disbpay = df_ibtrad_d.join(
    df_disbpay_agg, on=['ACCTNO','TRANSREF'], how='left'
)

df_pba = pl.read_parquet(f'{PBA01_DIR}IPBA01{reptyear}{reptmon}{reptday}.parquet').with_columns([
    pl.col('TRANSREF').str.slice(1,8).alias('TRANSREF')
]).select(['TRANSREF','UNEARNED'])

df_transrefx = df_pba.group_by('TRANSREF').agg([pl.col('UNEARNED').sum()])

df_ibtrad_final = df_ibtrad_with_disbpay.join(
    df_transrefx, on='TRANSREF', how='left'
).with_columns([
    pl.when(pl.col('UNEARNED').is_null()).then(0).otherwise(pl.col('UNEARNED')).alias('UNEARNED'),
    pl.when(pl.col('INTRECV').is_null()).then(0).otherwise(pl.col('INTRECV')).alias('INTRECV'),
    (pl.col('BALANCE') - pl.col('UNEARNED')).alias('BALANCE')
])

df_ibtrad_final = df_ibtrad_final.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['77','78','95','96']) & 
            (pl.col('SECTORCD') >= '0111') & (pl.col('SECTORCD') <= '0430'))
    .then(pl.lit('0410'))
    .otherwise(pl.col('SECTORCD'))
    .alias('SECTORCD')
])

sector_custcd_filter = ['01','02','03','11','12','20','13','17','30','31','32','33','34',
                        '35','36','37','38','39','40','45','04','05','06','80','81','82',
                        '83','84','85','86','87','88','90']

df_ibtrad_final = df_ibtrad_final.with_columns([
    pl.when(~pl.col('CUSTCD').is_in(sector_custcd_filter) & 
            pl.col('SECTORCD').is_in(['8110','8120','8130']))
    .then(pl.lit('9999'))
    .otherwise(pl.col('SECTORCD'))
    .alias('SECTORCD')
])

df_ibtrad_final = df_ibtrad_final.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['82','83','84','86','90','91','95','96','98','99']) &
            (pl.col('SECTORCD') >= '1000') & (pl.col('SECTORCD') <= '9999'))
    .then(pl.lit('86'))
    .otherwise(pl.col('CUSTCD'))
    .alias('CUSTCD')
])

df_ibtrad_final = df_ibtrad_final.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['77','78','95','96']))
    .then(pl.lit('9700'))
    .otherwise(pl.col('SECTORCD'))
    .alias('SECTORCD')
])

df_ibtrad_final.write_parquet(f'{BNM_DIR}IBTRAD{reptmon}{nowk}.parquet')

df_ibtmast = pl.read_parquet(f'{BNM_DIR}IBTMAST{reptmon}{nowk}.parquet')

df_ibtmast.write_parquet(f'{BT_DIR}IBTMAST{reptmon}{nowk}{reptyear}.parquet')
df_ibtrad_final.write_parquet(f'{BT_DIR}IBTRAD{reptmon}{nowk}{reptyear}.parquet')

print(f"Islamic Banking Trade Data Processing Complete")
print(f"Report Date: {reptdate.strftime('%d/%m/%y')}")
print(f"Records processed: {len(df_ibtrad_final)}")
print(f"\nOutput files generated:")
print(f"  {BNM_DIR}IBTRAD{reptmon}{nowk}.parquet")
print(f"  {BNM_DIR}IBTRADI{reptmon}{nowk}.parquet")
print(f"  {BT_DIR}IBTMAST{reptmon}{nowk}{reptyear}.parquet")
print(f"  {BT_DIR}IBTRAD{reptmon}{nowk}{reptyear}.parquet")
