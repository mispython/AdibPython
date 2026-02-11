import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

LOAN_DIR = 'data/loan/'
NPL_DIR = 'data/npl/'
NAME_DIR = 'data/name/'
BNM_DIR = 'data/bnm/'
SUBA_DIR = 'data/suba/'
MNICS_DIR = 'data/mnics/'
TRAN_DIR = 'data/tran/'
ELDS2_DIR = 'data/elds2/'
SAVE_DIR = 'data/save/'
OUTPUT_PAGE01 = 'data/sap_pfcriss_page01.txt'
OUTPUT_PAGE02 = 'data/sap_pfcriss_page02.txt'
OUTPUT_PAGE03 = 'data/sap_pfcriss_page03.txt'
OUTPUT_PAGE04 = 'data/sap_pfcriss_page04.txt'

df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
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

wk1, wk2, wk3, wk4 = '01', '02', '03', '04'
reptyear = reptdate.year
ryear = f'{reptyear % 100:02d}'
reptmon = f'{reptdate.month:02d}'
reptday = f'{day:02d}'
prevmon = f'{(reptdate.month - 1) if reptdate.month > 1 else 12:02d}'

reptdte = f"{reptday}{reptmon}{reptyear}"

def calculate_next_bldate(bldate, issdte):
    """Calculate next billing date"""
    dd = issdte.day
    mm = bldate.month + 1
    yy = bldate.year
    
    if mm > 12:
        mm = 1
        yy += 1
    
    if mm == 2:
        if yy % 4 == 0:
            max_day = 29
        else:
            max_day = 28
    elif mm in [4, 6, 9, 11]:
        max_day = 30
    else:
        max_day = 31
    
    if dd > max_day:
        dd = max_day
    
    return datetime(yy, mm, dd).date()

df_prev = pl.read_parquet(f'{NPL_DIR}LOAN{prevmon}.parquet').filter(
    pl.col('LOANTYPE') < 900
).with_columns([
    pl.lit('OLD').alias('PREVIND')
])

df_curr = pl.read_parquet(f'{NPL_DIR}LOAN{reptmon}.parquet').with_columns([
    pl.lit('NEW').alias('PREVIND')
])

df_perform = df_prev.join(
    df_curr.select(['ACCTNO', 'PREVIND']), 
    on='ACCTNO', 
    how='anti'
).with_columns([
    pl.lit(0).alias('MARKETVL')
])

df_iis = pl.read_parquet(f'{NPL_DIR}IIS.parquet').sort(['ACCTNO', 'NOTENO']).with_columns([
    pl.col('RECOVER').alias('RECIIS'),
    pl.col('NTBRCH').alias('BRANCH')
])

df_sp2 = pl.read_parquet(f'{NPL_DIR}SP2.parquet').sort(['ACCTNO', 'NOTENO'])
df_sp1 = pl.read_parquet(f'{NPL_DIR}SP1.parquet').sort(['ACCTNO', 'NOTENO'])
df_sp2open = pl.read_parquet(f'{NPL_DIR}SP2OPEN.parquet').sort(['ACCTNO'])
df_sp1open = pl.read_parquet(f'{NPL_DIR}SP1OPEN.parquet').sort(['ACCTNO'])

df_sp2 = df_sp2.join(df_sp2open.select(['ACCTNO', 'RISKOPEN']), on='ACCTNO', how='left')
df_sp2 = df_sp2.join(df_sp1.select(['ACCTNO', 'NOTENO', 'SPP1']), on=['ACCTNO', 'NOTENO'], how='left')
df_sp1 = df_sp1.join(df_sp1open.select(['ACCTNO', 'RISKOPEN']), on='ACCTNO', how='left')
df_sp1 = df_sp1.join(df_sp2.select(['ACCTNO', 'NOTENO', 'SPP2', 'RISK']), on=['ACCTNO', 'NOTENO'], how='left')

df_sp = df_sp2.with_columns([
    pl.when(pl.col('RISK') == 'SUBSTANDARD-1').then(pl.lit('C')).otherwise(pl.col('RISK')).alias('RISK'),
    pl.col('RISK').str.slice(0, 1).alias('RISKC'),
    pl.col('RISKOPEN').str.slice(0, 1).alias('RISKO')
]).with_columns([
    pl.when(pl.col('RISKO').is_null() | (pl.col('RISKO') == '')).then(pl.col('RISKC')).otherwise(pl.col('RISKO')).alias('RISKO'),
    pl.col('SPP2').alias('SPP')
]).with_columns([
    pl.when(pl.col('SP').is_null()).then(0).otherwise(pl.col('SP')).alias('SP'),
    pl.when(pl.col('SPPW').is_null()).then(0).otherwise(pl.col('SPPW')).alias('SPPW'),
    pl.when(pl.col('SPP1').is_null()).then(0).otherwise(pl.col('SPP1')).alias('SPP1'),
    pl.when(pl.col('SPP2').is_null()).then(0).otherwise(pl.col('SPP2')).alias('SPP2'),
    pl.when(pl.col('SPPL').is_null()).then(0).otherwise(pl.col('SPPL')).alias('SPPL'),
    pl.when(pl.col('SPP').is_null()).then(0).otherwise(pl.col('SPP')).alias('SPP'),
    pl.when(pl.col('RECOVER').is_null()).then(0).otherwise(pl.col('RECOVER')).alias('RECOVER')
]).with_columns([
    pl.col('SPPL').alias('SPMADE'),
    pl.col('RECOVER').alias('SPWRBACK')
]).select(['ACCTNO', 'NOTENO', 'OSPRIN', 'MARKETVL', 'SPP', 'SPPL', 'RECOVER', 'SPPW', 'SP', 'RISKC', 'SPMADE', 'SPWRBACK', 'SPP1', 'SPP2', 'RISKO', 'OTHERFEE'])

df_npl = df_iis.join(df_sp, on=['ACCTNO', 'NOTENO'], how='left').with_columns([
    pl.when(pl.col('OIP').is_null()).then(0).otherwise(pl.col('OIP')).alias('OIP'),
    pl.when(pl.col('OI').is_null()).then(0).otherwise(pl.col('OI')).alias('OI')
]).with_columns([
    (pl.col('IISP') + pl.col('OIP')).alias('IISP'),
    (pl.col('SUSPEND') + pl.col('OISUSP')).alias('SUSPEND'),
    (pl.col('RECIIS') + pl.col('OIRECV') + pl.col('RECC') + pl.col('OIRECC')).alias('RECIIS'),
    (pl.col('IISPW') + pl.col('OIW')).alias('IISPW')
]).with_columns([
    pl.when(pl.col('MARKETVL').is_null()).then(0).otherwise(pl.col('MARKETVL')).alias('MARKETVL'),
    (pl.col('OSPRIN') - pl.col('MARKETVL')).alias('CLASSAMT')
])

df_lnnote = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').sort(['ACCTNO', 'NOTENO']).drop(['MARKETVL', 'BORSTAT'])

df_npl = df_perform.join(
    df_npl, on=['ACCTNO', 'NOTENO'], how='outer'
).with_columns([
    pl.when(pl.col('RISKC').is_null()).then(pl.lit('O')).otherwise(pl.col('RISKC')).alias('RISKC'),
    pl.when(pl.col('MARKETVL').is_null()).then(0).otherwise(pl.col('MARKETVL')).alias('MARKETVL')
])

df_npl = df_npl.join(df_lnnote, on=['ACCTNO', 'NOTENO'], how='left')

df_npl = df_npl.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([983, 993])).then(pl.lit('B')).otherwise(pl.col('RISKC')).alias('RISKC'),
    pl.when(pl.col('LOANTYPE').is_in([983, 993])).then(pl.lit('B')).otherwise(pl.col('RISKC')).alias('RISKD')
])

df_npl = df_npl.with_columns([
    pl.when(
        (pl.col('DAYS') < 90) & 
        (pl.col('TOTIIS') <= 0) & 
        (pl.col('SP') <= 0) & 
        pl.col('RISKC').is_in(['S', 'C']) & 
        (pl.col('PAIDIND') != 'P')
    ).then(
        pl.when(
            pl.col('LOANTYPE').is_in([128, 130, 700, 705, 380, 381]) &
            (~pl.col('BORSTAT').is_in(['F', 'R', 'I']) | (pl.col('USER5') != 'N'))
        ).then(pl.lit('P'))
        .when(~pl.col('LOANTYPE').is_in([128, 130, 700, 705, 380, 381]) & (pl.col('CENSUS7') != '9'))
        .then(pl.lit('P'))
        .otherwise(pl.col('RISKC'))
    ).otherwise(pl.col('RISKC')).alias('RISKC')
])

df_npl = df_npl.with_columns([
    pl.when(pl.col('PAIDIND') == 'P').then(pl.lit('S')).otherwise(pl.col('RISKD')).alias('RISKD'),
    pl.when(pl.col('PAIDIND') == 'P').then(pl.col('NTBRCH')).otherwise(pl.col('BRANCH')).alias('BRANCH')
])

df_lnname = pl.read_parquet(f'{NAME_DIR}LNNAME.parquet').sort('ACCTNO')

df_lnaddr = df_lnnote.select(['ACCTNO']).join(
    df_lnname, on='ACCTNO', how='left'
).select([
    'ACCTNO',
    pl.col('TAXNO').alias('OLDIC'),
    pl.col('GUAREND').alias('NEWIC'),
    'NAMELN1', 'NAMELN2', 'NAMELN3', 'NAMELN4', 'NAMELN5'
]).unique(subset=['ACCTNO'])

df_termchg = df_lnnote.filter(
    (pl.col('REVERSED') != 'Y') &
    pl.col('NOTENO').is_not_null() &
    (pl.col('PAIDIND') != 'P') &
    pl.col('NTBRCH').is_not_null() &
    pl.col('LOANTYPE').is_in([128, 130, 700, 705, 380, 381])
).with_columns([
    pl.when(pl.col('INTAMT') > 0.1)
    .then(pl.col('INTAMT') + pl.col('INTEARN2'))
    .otherwise(0)
    .alias('TERMCHG')
]).select(['ACCTNO', 'NOTENO', 'TERMCHG', 'VINNO'])

df_uloan = pl.read_parquet(f'{BNM_DIR}ULOAN{reptmon}{nowk}.parquet').sort(['ACCTNO', 'COMMNO'])
df_lncomm = pl.read_parquet(f'{LOAN_DIR}LNCOMM.parquet').sort(['ACCTNO', 'COMMNO'])

df_mergecom = df_uloan.join(
    df_lncomm.select(['ACCTNO', 'COMMNO', 'CMBRCH', 'CAPPDATE']), 
    on=['ACCTNO', 'COMMNO'], 
    how='left'
).with_columns([
    pl.when((pl.col('BRANCH') == 0) | pl.col('BRANCH').is_null())
    .then(pl.col('CMBRCH'))
    .otherwise(pl.col('BRANCH'))
    .alias('BRANCH')
]).filter(pl.col('APPRLIMT') >= 1000000)

df_loan = pl.concat([
    pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet'),
    df_mergecom.with_columns([pl.col('COMMNO').alias('NOTENO')])
]).filter(
    ~pl.col('CUSTCODE').is_in([1, 2, 3, 10, 11, 12, 81, 91])
).with_columns([
    pl.when(pl.col('PRODUCT').is_in([128, 130, 700, 705, 380, 381, 709, 710, 750, 752, 760]))
    .then(pl.col('NETPROC'))
    .otherwise(pl.col('ORGBAL'))
    .alias('APPRLIMT')
])

df_temp = df_loan.group_by('ACCTNO').agg([
    pl.col('APPRLIMT').sum().alias('TOTLIMT'),
    pl.len().alias('LOANCNT')
])

df_loan = df_loan.join(df_temp, on='ACCTNO', how='left')

df_loan = df_loan.join(df_npl, on=['ACCTNO', 'NOTENO'], how='left', suffix='_npl')
df_loan = df_loan.join(df_termchg, on=['ACCTNO', 'NOTENO'], how='left')

df_loan = df_loan.filter(
    pl.col('TOTLIMT') >= 1000000
).with_columns([
    pl.when(pl.col('RISKC').is_null()).then(pl.lit('O')).otherwise(pl.col('RISKC')).alias('RISKC'),
    pl.when(pl.col('MARKETVL').is_null()).then(0).otherwise(pl.col('MARKETVL')).alias('MARKETVL')
])

hp_products = [128, 130, 700, 705, 380, 381]

processed = []
for row in df_loan.iter_rows(named=True):
    new_row = row.copy()
    
    if new_row['RISKC'] != 'B':
        new_row['TOTIIS'] = new_row.get('TOTIIS', 0) if new_row.get('TOTIIS') is not None else 0
        new_row['UHC'] = 0
        
        if row.get('PRODUCT') not in hp_products:
            new_row['OSPRIN'] = new_row.get('CURBAL', 0)
        elif row.get('BLDATE'):
            bldate = row['BLDATE']
            issdte = row.get('ISSDTE')
            reptdate_val = reptdate
            
            if row.get('BILPAY', 0) > 0 and row.get('BILTOT', 0) / row['BILPAY'] <= 0.01:
                bldate = calculate_next_bldate(bldate, issdte)
            
            days = (reptdate_val - bldate).days if bldate else 0
            new_row['DAYS'] = days
            
            termchg = row.get('TERMCHG', 0)
            if termchg > 0:
                remmth1 = row.get('EARNTERM', 0) - (
                    (bldate.year - issdte.year) * 12 +
                    bldate.month - issdte.month + 1
                )
                remmth2 = row.get('EARNTERM', 0) - (
                    (reptdate_val.year - issdte.year) * 12 +
                    reptdate_val.month - issdte.month + 1
                )
                
                if remmth2 < 0:
                    remmth2 = 0
                
                if row.get('LOANTYPE') in [128, 130]:
                    remmth1 = remmth1 - 3
                else:
                    remmth1 = remmth1 - 1
                
                if remmth1 >= remmth2:
                    totiis = 0
                    for rm in range(remmth1, remmth2 - 1, -1):
                        totiis += 2 * (rm + 1) * termchg / (row['EARNTERM'] * (row['EARNTERM'] + 1))
                    new_row['TOTIIS'] = totiis
                
                if remmth2 > 0:
                    new_row['UHC'] = remmth2 * (remmth2 + 1) * termchg / (row['EARNTERM'] * (row['EARNTERM'] + 1))
            
            curbal = row.get('CURBAL', 0) if row.get('CURBAL') is not None else 0
            new_row['OSPRIN'] = curbal - new_row.get('UHC', 0) - new_row.get('TOTIIS', 0)
    
    if new_row.get('OSPRIN', 0) < 0:
        new_row['OSPRIN'] = 0
    
    if row.get('LOANTYPE') in [720, 725]:
        new_row['OSPRIN'] = row.get('CURBAL', 0)
    
    if row.get('BORSTAT') == 'W' or row.get('LOANTYPE') in [981, 982, 983, 991, 992, 993]:
        new_row['OSPRIN'] = 0
    
    new_row['TOTBAL'] = new_row.get('OSPRIN', 0) + new_row.get('TOTIIS', 0)
    
    processed.append(new_row)

df_loan = pl.DataFrame(processed)

df_cis = pl.read_parquet(f'{MNICS_DIR}CIS.parquet')

df_dirinfo = df_loan.select(['BRANCH', 'ACCTNO', 'NTBRCH', 'COSTCTR']).unique(subset=['ACCTNO']).join(
    df_cis, on='ACCTNO', how='left'
).with_columns([
    pl.when((pl.col('BRANCH') == 0) | pl.col('BRANCH').is_null())
    .then(pl.col('NTBRCH'))
    .otherwise(pl.col('BRANCH'))
    .alias('BRANCH'),
    pl.when(pl.col('DIRNIC').str.replace_all(' ', '').str.replace_all('0', '') == '')
    .then(pl.lit(' '))
    .otherwise(pl.col('DIRNIC'))
    .alias('DIRNIC'),
    pl.when(pl.col('DIROIC').str.replace_all(' ', '').str.replace_all('0', '') == '')
    .then(pl.lit(' '))
    .otherwise(pl.col('DIROIC'))
    .alias('DIROIC')
]).select(['BRANCH', 'ACCTNO', 'DIRNIC', 'DIROIC', 'DIRNAME', 'COSTCTR'])

df_dirinfo.write_parquet(f'{SAVE_DIR}DIRINFO.parquet')

with open(OUTPUT_PAGE02, 'w') as f:
    for row in df_dirinfo.iter_rows(named=True):
        f.write(f"0234")
        f.write(f"{row['BRANCH']:05d}")
        f.write(f"{row['ACCTNO']:10.0f}")
        f.write(f"{reptdte}")
        f.write(f"{row.get('DIRNIC', ' '):<20}")
        f.write(f"{row.get('DIROIC', ' '):<20}")
        f.write(f"  ")
        f.write(f"{row.get('DIRNAME', ''):<60}")
        f.write(f"MY")
        f.write(f"N")
        f.write(f"{row['COSTCTR']:04d}\n")

df_cis_temp = df_cis.select(['ACCTNO', 'NAME', 'SIC']).unique(subset=['ACCTNO'])

df_save_loan = df_loan.join(df_lnaddr, on='ACCTNO', how='left').join(
    df_cis_temp, on='ACCTNO', how='left', suffix='_cis'
).with_columns([
    pl.when(pl.col('NEWIC').str.replace_all(' ', '').str.replace_all('0', '') == '')
    .then(pl.lit(' '))
    .otherwise(pl.col('NEWIC'))
    .alias('NEWIC'),
    pl.when(pl.col('OLDIC').str.replace_all(' ', '').str.replace_all('0', '') == '')
    .then(pl.lit(' '))
    .otherwise(pl.col('OLDIC'))
    .alias('OLDIC'),
    (pl.col('NAMELN3') + ' ' + pl.col('NAMELN4') + ' ' + pl.col('NAMELN5')).alias('ADDR2')
])

df_save_loan = df_save_loan.with_columns([
    pl.when((pl.col('PRODUCT') >= 100) & (pl.col('PRODUCT') <= 199)).then(pl.lit('I')).otherwise(pl.lit('C')).alias('FITYPE'),
    pl.when(pl.col('LOANCNT') > 1).then(pl.lit('M')).otherwise(pl.lit('S')).alias('LOANOPT'),
    pl.when(pl.col('PAYFREQ').is_null() | (pl.col('PAYFREQ') == '')).then(pl.lit('M')).otherwise(pl.col('PAYFREQ')).alias('PAYTERM'),
    pl.when(pl.col('SECURE') != 'S').then(pl.lit('C')).otherwise(pl.col('SECURE')).alias('SECURE'),
    pl.when(pl.col('BORSTAT') == 'S').then(pl.lit('R')).otherwise(pl.lit('N')).alias('RESTRUC'),
    pl.when(pl.col('PRODUCT').is_in([101, 210, 307, 709, 710, 910])).then(pl.lit('C')).otherwise(pl.lit('N')).alias('CAGAMAS'),
    pl.when(pl.col('RISKC').is_null() | (pl.col('RISKC') == '')).then(pl.lit('P')).otherwise(pl.col('RISKC')).alias('RISKC'),
    (pl.col('DAYS') / 30.00050).floor().alias('MTHARR')
])

df_save_loan = df_save_loan.with_columns([
    pl.when(
        (pl.col('DAYS') >= 90) |
        pl.col('BORSTAT').is_in(['F', 'I', 'R']) |
        pl.col('LOANTYPE').is_in([983, 993, 678, 679, 698, 699]) |
        (pl.col('USER5') == 'N')
    ).then(pl.lit('Y')).otherwise(pl.lit('N')).alias('IMPAIRED')
])

df_sub = pl.read_parquet(f'{SUBA_DIR}LOAN.parquet').select(['ACCTNO', 'NOTENO']).unique()

df_save_loan = df_save_loan.join(df_sub, on=['ACCTNO', 'NOTENO'], how='inner')

df_save_loan.write_parquet(f'{SAVE_DIR}LOAN.parquet')

with open(OUTPUT_PAGE01, 'w') as f:
    for acctno, group in df_save_loan.group_by('ACCTNO'):
        row = group[0]
        f.write(f"0234")
        f.write(f"{row['BRANCH']:05d}")
        f.write(f"{row['ACCTNO']:10.0f}")
        f.write(f"{reptdte}")
        f.write(f"{row.get('NEWIC', ' '):<20}")
        f.write(f"{row.get('OLDIC', ' '):<20}")
        f.write(f"  ")
        f.write(f"{row.get('NAME', ''):<60}")
        f.write(f"{row.get('NAMELN2', ''):<45}")
        f.write(f"{row.get('ADDR2', ''):<90}")
        f.write(f"{row.get('CUSTCD', ''):<2}")
        f.write(f"MY")
        f.write(f"{row.get('SIC', 0):04d}")
        f.write(f"N")
        f.write(f"N")
        f.write(f"{row['FITYPE']}")
        f.write(f"{row['LOANOPT']}")
        f.write(f"{row['TOTLIMT']:12.0f}")
        f.write(f"{row['LOANCNT']:2.0f}")
        f.write(f"N")
        f.write(f"{row['COSTCTR']:04d}\n")

df_prov = df_save_loan.with_columns([
    pl.when(pl.col('RISKC') == 'O').then(pl.lit('P')).otherwise(pl.col('RISKC')).alias('RISKC')
]).filter(
    pl.col('RISKC').is_not_null() & (pl.col('RISKC') != '')
)

with open(OUTPUT_PAGE03, 'w') as f:
    for row in df_prov.iter_rows(named=True):
        f.write(f"0234")
        f.write(f"{row['BRANCH']:05d}")
        f.write(f"{row['ACCTNO']:10.0f}")
        f.write(f"{row['NOTENO']:05d}")
        f.write(f"{reptdte}")
        f.write(f"{row['LOANTYPE']:5d}")
        f.write(f"{row.get('APPRDTE', ''):<8}")
        f.write(f"{row.get('CLOSDTE', ''):<8}")
        f.write(f"        ")
        f.write(f"  ")
        f.write(f"{row.get('SECTORCD', ''):<4}")
        f.write(f"{row.get('NOTETERM', 0):3d}")
        f.write(f"{row['PAYTERM']}")
        f.write(f"{row['MTHARR']:3.0f}")
        f.write(f"{row['MTHARR']:3.0f}")
        f.write(f"{row['RISKC']}")
        f.write(f"{row['SECURE']}")
        f.write(f"N")
        f.write(f"N")
        f.write(f"{row['RESTRUC']}")
        f.write(f"{row['CAGAMAS']}")
        f.write(f"S")
        f.write(f"N")
        f.write(f"N")
        f.write(f"N")
        f.write(f"MYR")
        f.write(f"{row.get('OSPRIN', 0):12.2f}")
        f.write(f"{row.get('TOTIIS', 0):12.2f}")
        f.write(f"{row.get('OTHERFEE', 0):12.2f}")
        f.write(f"{row.get('TOTBAL', 0):12.2f}")
        f.write(f"N")
        f.write(f"{row.get('BORSTAT', ' ')}")
        f.write(f"{row['COSTCTR']:04d}")
        f.write(f"{row.get('AANO', ''):<13}")
        f.write(f"{row.get('FACILITY', 0):5d}")
        f.write(f"{row.get('TOTAL', 0):17.2f}")
        f.write(f"{row.get('TRANAMT', 0):17.2f}")
        f.write(f"{row.get('FACCODE', 0):5d}")
        f.write(f"{row['IMPAIRED']}\n")

df_page04 = df_save_loan.filter(
    (pl.col('RISKC') != 'P') | (pl.col('RISKD') == 'P')
)

with open(OUTPUT_PAGE04, 'w') as f:
    for row in df_page04.iter_rows(named=True):
        f.write(f"0234")
        f.write(f"{row['BRANCH']:05d}")
        f.write(f"{row['ACCTNO']:10.0f}")
        f.write(f"{row['NOTENO']:05d}")
        f.write(f"{reptdte}")
        f.write(f"{row.get('MARKETVL', 0):12.2f}")
        f.write(f"{row.get('CLASSAMT', 0):12.2f}")
        f.write(f"{row.get('IISP', 0):12.2f}")
        f.write(f"{row.get('SUSPEND', 0):12.2f}")
        f.write(f"{row.get('RECIIS', 0):12.2f}")
        f.write(f"{row.get('IISPW', 0):12.2f}")
        f.write(f"{row.get('TOTIIS', 0):12.2f}")
        f.write(f"{row.get('SPP', 0):12.2f}")
        f.write(f"{row.get('SPMADE', 0):12.2f}")
        f.write(f"{row.get('SPWRBACK', 0):12.2f}")
        f.write(f"{row.get('SPPW', 0):12.2f}")
        f.write(f"{row.get('SP', 0):12.2f}")
        f.write(f"            ")
        f.write(f"N")
        f.write(f"{row['COSTCTR']:04d}")
        f.write(f"{row.get('AANO', ''):<13}")
        f.write(f"{row.get('FACILITY', 0):5d}")
        f.write(f"{row.get('TOTAL', 0):17.2f}")
        f.write(f"{row.get('TRANAMT', 0):17.2f}")
        f.write(f"{row.get('FACCODE', 0):5d}")
        f.write(f"{row['IMPAIRED']}\n")

print(f"CRIS Reporting Complete")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_PAGE01} (Borrower/Loan Profile)")
print(f"  {OUTPUT_PAGE02} (Holder/Director)")
print(f"  {OUTPUT_PAGE03} (Loan Details/Currency)")
print(f"  {OUTPUT_PAGE04} (Non-Performing Loan)")
print(f"\nRecords processed: {len(df_save_loan)}")
