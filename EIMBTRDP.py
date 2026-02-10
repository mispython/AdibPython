import polars as pl
from datetime import datetime

BNM2_REPTDATE = 'data/bnm2/reptdate.parquet'
BNM_DIR = 'data/bnm/'
OUTPUT_RDALKM = 'data/rdalkm.txt'
OUTPUT_NSRS = 'data/nsrs.txt'

df_reptdate = pl.read_parquet(BNM2_REPTDATE)
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
mm2 = mm - 1 if mm > 1 else 12

sdate = datetime(year, mm, sdd).date()
rdate = reptdate.strftime('%d%m%y')
reptmon = reptdate.strftime('%m')
reptmon1 = f'{mm2:02d}'
reptyear = reptdate.strftime('%Y')
reptday = reptdate.strftime('%d')

if wk != '4':
    print(f"Report only runs for week 4 (month-end). Current week: {wk}")
    exit()

nowk = wk

df_btrad = pl.read_parquet(f'{BNM_DIR}BTRAD{reptmon}{nowk}.parquet')
df_btmast = pl.read_parquet(f'{BNM_DIR}BTMAST{reptmon}{nowk}.parquet')

def apply_sector_mapping(df):
    """Apply complete BNM sector code hierarchical mapping"""
    
    df = df.with_columns([
        pl.when(pl.col('SECTORCD').str.len_chars() == 0).then(pl.lit('9999'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '1').then(pl.lit('1400'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '2').then(pl.lit('2900'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '3').then(pl.lit('3919'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '4').then(pl.lit('4010'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '5').then(pl.lit('5999'))
        .when(pl.col('SECTORCD').str.slice(0,2) == '61').then(pl.lit('6120'))
        .when(pl.col('SECTORCD').str.slice(0,2) == '62').then(pl.lit('6130'))
        .when(pl.col('SECTORCD').str.slice(0,2) == '63').then(pl.lit('6310'))
        .when(pl.col('SECTORCD').str.slice(0,2).is_in(['64','65','66','67','68','69'])).then(pl.lit('6130'))
        .when(pl.col('SECTORCD').str.slice(0,1) == '7').then(pl.lit('7199'))
        .when(pl.col('SECTORCD').str.slice(0,2).is_in(['81','82'])).then(pl.lit('8110'))
        .when(pl.col('SECTORCD').str.slice(0,2).is_in(['83','84','85','86','87','88','89'])).then(pl.lit('8999'))
        .when(pl.col('SECTORCD').str.slice(0,2) == '91').then(pl.lit('9101'))
        .when(pl.col('SECTORCD').str.slice(0,2) == '92').then(pl.lit('9410'))
        .when(pl.col('SECTORCD').str.slice(0,2).is_in(['93','94','95'])).then(pl.lit('9499'))
        .when(pl.col('SECTORCD').str.slice(0,2).is_in(['96','97','98','99'])).then(pl.lit('9999'))
        .otherwise(pl.col('SECTORCD'))
        .alias('SECTCD')
    ])
    
    expanded = []
    for row in df.iter_rows(named=True):
        s = row['SECTCD']
        base = {k:v for k,v in row.items() if k not in ['SECTORCD','SECTCD']}
        
        if s in ['1111','1112','1113','1114','1115','1116','1117','1119','1120','1130','1140','1150']:
            if s in ['1111','1113','1115','1117','1119']:
                expanded.append({**base, 'SECTORCD': '1110'})
            expanded.append({**base, 'SECTORCD': '1100'})
        elif s in ['2210','2220']:
            expanded.append({**base, 'SECTORCD': '2200'})
        elif s in ['2301','2302','2303']:
            if s in ['2301','2302']:
                expanded.append({**base, 'SECTORCD': '2300'})
            expanded.append({**base, 'SECTORCD': '2300'})
            if s == '2303':
                expanded.append({**base, 'SECTORCD': '2302'})
        elif s in ['3110','3115','3111','3112','3113','3114']:
            if s in ['3110','3113','3114']:
                expanded.append({**base, 'SECTORCD': '3100'})
            if s in ['3115','3111','3112']:
                expanded.append({**base, 'SECTORCD': '3110'})
        elif s in ['3211','3212','3219']:
            expanded.append({**base, 'SECTORCD': '3210'})
        elif s in ['3221','3222']:
            expanded.append({**base, 'SECTORCD': '3220'})
        elif s in ['3231','3232']:
            expanded.append({**base, 'SECTORCD': '3230'})
        elif s in ['3241','3242']:
            expanded.append({**base, 'SECTORCD': '3240'})
        elif s in ['3270','3280','3290','3271','3272','3273','3311','3312','3313']:
            if s in ['3270','3280','3290','3271','3272','3273']:
                expanded.append({**base, 'SECTORCD': '3260'})
            if s in ['3271','3272','3273']:
                expanded.append({**base, 'SECTORCD': '3270'})
            if s in ['3311','3312','3313']:
                expanded.append({**base, 'SECTORCD': '3310'})
        elif s in ['3431','3432','3433']:
            expanded.append({**base, 'SECTORCD': '3430'})
        elif s in ['3551','3552']:
            expanded.append({**base, 'SECTORCD': '3550'})
        elif s in ['3611','3619']:
            expanded.append({**base, 'SECTORCD': '3610'})
        elif s in ['3710','3720','3730','3721','3731','3732']:
            expanded.append({**base, 'SECTORCD': '3700'})
            if s == '3721':
                expanded.append({**base, 'SECTORCD': '3720'})
            if s in ['3731','3732']:
                expanded.append({**base, 'SECTORCD': '3730'})
        elif s in ['3811','3812']:
            expanded.append({**base, 'SECTORCD': '3800'})
        elif s in ['3813','3814','3819']:
            expanded.append({**base, 'SECTORCD': '3812'})
        elif s in ['3832','3834','3835','3833']:
            expanded.append({**base, 'SECTORCD': '3831'})
            if s == '3833':
                expanded.append({**base, 'SECTORCD': '3832'})
        elif s in ['3842','3843','3844']:
            expanded.append({**base, 'SECTORCD': '3841'})
        elif s in ['3851','3852','3853']:
            expanded.append({**base, 'SECTORCD': '3850'})
        elif s in ['3861','3862','3863','3864','3865','3866']:
            expanded.append({**base, 'SECTORCD': '3860'})
        elif s in ['3871','3872']:
            expanded.append({**base, 'SECTORCD': '3870'})
        elif s in ['3891','3892','3893','3894']:
            expanded.append({**base, 'SECTORCD': '3890'})
        elif s in ['3911','3919']:
            expanded.append({**base, 'SECTORCD': '3910'})
        elif s in ['3951','3952','3953','3954','3955','3956','3957']:
            expanded.append({**base, 'SECTORCD': '3950'})
            if s in ['3952','3953']:
                expanded.append({**base, 'SECTORCD': '3951'})
            if s in ['3955','3956','3957']:
                expanded.append({**base, 'SECTORCD': '3954'})
        elif s in ['5001','5002','5003','5004','5005','5006','5008']:
            expanded.append({**base, 'SECTORCD': '5010'})
        elif s in ['6110','6120','6130']:
            expanded.append({**base, 'SECTORCD': '6100'})
        elif s in ['6310','6320']:
            expanded.append({**base, 'SECTORCD': '6300'})
        elif s in ['7111','7112','7117','7113','7114','7115','7116']:
            expanded.append({**base, 'SECTORCD': '7110'})
        elif s in ['7113','7114','7115','7116']:
            expanded.append({**base, 'SECTORCD': '7112'})
        elif s in ['7112','7114']:
            expanded.append({**base, 'SECTORCD': '7113'})
        elif s == '7116':
            expanded.append({**base, 'SECTORCD': '7115'})
        elif s in ['7121','7122','7123','7124']:
            expanded.append({**base, 'SECTORCD': '7120'})
            if s == '7124':
                expanded.append({**base, 'SECTORCD': '7123'})
            if s == '7122':
                expanded.append({**base, 'SECTORCD': '7121'})
        elif s in ['7131','7132','7133','7134']:
            expanded.append({**base, 'SECTORCD': '7130'})
        elif s in ['7191','7192','7193','7199']:
            expanded.append({**base, 'SECTORCD': '7190'})
        elif s in ['7210','7220']:
            expanded.append({**base, 'SECTORCD': '7200'})
        elif s in ['8110','8120','8130']:
            expanded.append({**base, 'SECTORCD': '8100'})
        elif s in ['8310','8330','8340','8320','8331','8332']:
            expanded.append({**base, 'SECTORCD': '8300'})
            if s in ['8320','8331','8332']:
                expanded.append({**base, 'SECTORCD': '8330'})
        elif s == '8321':
            expanded.append({**base, 'SECTORCD': '8320'})
        elif s == '8333':
            expanded.append({**base, 'SECTORCD': '8332'})
        elif s in ['8420','8411','8412','8413','8414','8415','8416']:
            expanded.append({**base, 'SECTORCD': '8400'})
            if s in ['8411','8412','8413','8414','8415','8416']:
                expanded.append({**base, 'SECTORCD': '8410'})
        elif s[:2] == '89':
            expanded.append({**base, 'SECTORCD': '8900'})
            if s in ['8910','8911','8912','8913','8914']:
                if s in ['8911','8912','8913','8914']:
                    expanded.append({**base, 'SECTORCD': '8910'})
                if s == '8910':
                    expanded.append({**base, 'SECTORCD': '8914'})
            if s in ['8921','8922','8920']:
                if s in ['8921','8922']:
                    expanded.append({**base, 'SECTORCD': '8920'})
                if s == '8920':
                    expanded.append({**base, 'SECTORCD': '8922'})
            if s in ['8931','8932']:
                expanded.append({**base, 'SECTORCD': '8930'})
            if s in ['8991','8999']:
                expanded.append({**base, 'SECTORCD': '8990'})
        elif s in ['9101','9102','9103']:
            expanded.append({**base, 'SECTORCD': '9100'})
        elif s in ['9201','9202','9203']:
            expanded.append({**base, 'SECTORCD': '9200'})
        elif s in ['9311','9312','9313','9314']:
            expanded.append({**base, 'SECTORCD': '9300'})
        elif s[:2] == '94':
            expanded.append({**base, 'SECTORCD': '9400'})
            if s in ['9433','9434','9435','9432','9431']:
                if s in ['9433','9434','9435']:
                    expanded.append({**base, 'SECTORCD': '9432'})
                expanded.append({**base, 'SECTORCD': '9430'})
            if s in ['9410','9420','9440','9450']:
                expanded.append({**base, 'SECTORCD': '9499'})
        else:
            expanded.append({**base, 'SECTORCD': s})
    
    df_exp = pl.DataFrame(expanded) if expanded else df.with_columns([pl.col('SECTCD').alias('SECTORCD')])
    
    rollup = []
    for row in df_exp.iter_rows(named=True):
        s = row['SECTORCD']
        rollup.append(row)
        
        if s in ['1100','1200','1300','1400']:
            rollup.append({**row, 'SECTORCD': '1000'})
        elif s in ['2100','2200','2300','2400','2900']:
            rollup.append({**row, 'SECTORCD': '2000'})
        elif s in ['3100','3120','3210','3220','3230','3240','3250','3260','3310','3430','3550','3610','3700','3800','3825','3831','3841','3850','3860','3870','3890','3910','3950','3960']:
            rollup.append({**row, 'SECTORCD': '3000'})
        elif s in ['4010','4020','4030']:
            rollup.append({**row, 'SECTORCD': '4000'})
        elif s in ['5010','5020','5030','5040','5050','5999']:
            rollup.append({**row, 'SECTORCD': '5000'})
        elif s in ['6100','6300']:
            rollup.append({**row, 'SECTORCD': '6000'})
        elif s in ['7110','7120','7130','7190','7200']:
            rollup.append({**row, 'SECTORCD': '7000'})
        elif s in ['8100','8300','8400','8900']:
            rollup.append({**row, 'SECTORCD': '8000'})
        elif s in ['9100','9200','9300','9400','9500','9600']:
            rollup.append({**row, 'SECTORCD': '9000'})
    
    return pl.DataFrame(rollup).with_columns([
        pl.when(pl.col('SECTORCD') == '    ').then(pl.lit('9999'))
        .otherwise(pl.col('SECTORCD')).alias('SECTORCD')
    ])

btrm_records = []

df_alm = df_btrad.filter(
    (pl.col('PRODCD').str.slice(0,2) == '34') | (pl.col('PRODCD') == '54120')
).group_by(['AMTIND','APPRLIM2','CUSTCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    almlimt = f"{int(row['APPRLIM2']):05d}"
    c = row['CUSTCD']
    
    btrm_records.append({'BNMCODE': f"{almlimt}00000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54']:
        btrm_records.append({'BNMCODE': f"{almlimt}{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['41','42','43']:
            btrm_records.append({'BNMCODE': f"{almlimt}66000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['44','46','47']:
            btrm_records.append({'BNMCODE': f"{almlimt}67000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['48','49','51']:
            btrm_records.append({'BNMCODE': f"{almlimt}68000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['52','53','54']:
            btrm_records.append({'BNMCODE': f"{almlimt}69000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    
    if c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': f"{almlimt}61000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c == '77':
        btrm_records.append({'BNMCODE': f"{almlimt}77000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    (pl.col('PRODCD').str.slice(0,2) == '34') | (pl.col('PRODCD') == '54120')
).group_by(['AMTIND','COLLCD','CUSTCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    cd = row['COLLCD']
    c = row['CUSTCD']
    
    if cd in ['30560','30570','30580'] and not c:
        btrm_records.append({'BNMCODE': f"{cd}00000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif cd not in ['30560','30570','30580'] and c:
        if c in ['10','02','03','11','12']:
            btrm_records.append({'BNMCODE': f"{cd}10000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['20','13','17','30','32','33','34','35','36','37','38','39','40']:
            btrm_records.append({'BNMCODE': f"{cd}20000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['04','05','06']:
            btrm_records.append({'BNMCODE': f"{cd}{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            btrm_records.append({'BNMCODE': f"{cd}20000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['60','62','63','44','46','47','48','49','51']:
            btrm_records.append({'BNMCODE': f"{cd}60000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['61','41','42','43']:
            btrm_records.append({'BNMCODE': f"{cd}61000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            btrm_records.append({'BNMCODE': f"{cd}60000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['64','52','53','54','75','57','59']:
            btrm_records.append({'BNMCODE': f"{cd}64000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            btrm_records.append({'BNMCODE': f"{cd}60000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['70','71','72','73','74']:
            btrm_records.append({'BNMCODE': f"{cd}70000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['76','78']:
            btrm_records.append({'BNMCODE': f"{cd}76000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c == '77':
            btrm_records.append({'BNMCODE': f"{cd}77000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            btrm_records.append({'BNMCODE': f"{cd}76000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c == '79':
            btrm_records.append({'BNMCODE': f"{cd}79000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['80','81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99']:
            btrm_records.append({'BNMCODE': f"{cd}80000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            if c in ['86','87','88','89']:
                btrm_records.append({'BNMCODE': f"{cd}86000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_b80510 = df_btmast.filter(
    (pl.col('LEVELS') == 1) & (pl.col('D') == 1) & (pl.col('DBALANCE') > 0) &
    (pl.col('CUSTCD') != '') & (pl.col('ACCTNO').cast(pl.Utf8).str.slice(0,2) == '25')
).sort('ACCTNO')

df_alm = df_b80510.group_by(['D','APPRLIMT','CUSTCD']).agg([pl.len().alias('_FREQ_')])

for row in df_alm.iter_rows(named=True):
    loansize = f"{int(row['APPRLIMT']):05d}"
    amt = row['_FREQ_'] * 1000
    c = row['CUSTCD']
    
    if c in ['10','02','03','11','12']:
        btrm_records.append({'BNMCODE': '8051010000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['20','13','17','04','05','06','30','32','33','34','35','36','37','38','39','40']:
        btrm_records.append({'BNMCODE': '8051020000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': '8051061000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        if c in ['41','42','43']:
            btrm_records.append({'BNMCODE': f"80510{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': '8051066000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['62','44','46','47']:
        btrm_records.append({'BNMCODE': '8051062000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        if c in ['44','46','47']:
            btrm_records.append({'BNMCODE': f"80510{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': '8051067000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['63','48','49','51']:
        btrm_records.append({'BNMCODE': '8051063000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        if c in ['48','49','51']:
            btrm_records.append({'BNMCODE': f"80510{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': '8051068000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['64','52','53','54','75','57','59']:
        btrm_records.append({'BNMCODE': '8051064000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        if c in ['52','53','54']:
            btrm_records.append({'BNMCODE': f"80510{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': '8051069000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
            btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64']:
        btrm_records.append({'BNMCODE': '8051065000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['70','71','72','73','74']:
        btrm_records.append({'BNMCODE': '8051070000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': '8050070000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c == '77':
        btrm_records.append({'BNMCODE': '8051077000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': '8050077000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c == '78':
        btrm_records.append({'BNMCODE': '8051078000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': '8050078000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c == '79':
        btrm_records.append({'BNMCODE': '8051079000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': '8050079000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['80','81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99']:
        btrm_records.append({'BNMCODE': '8051080000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': '8050080000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    
    btrm_records.append({'BNMCODE': f"{loansize}00000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c == '77':
        btrm_records.append({'BNMCODE': f"{loansize}77000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    elif c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': f"{loansize}61000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54']:
        btrm_records.append({'BNMCODE': f"{loansize}{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})

df_alm = df_btrad.filter(pl.col('PRODCD').str.slice(0,2) == '34').group_by(['AMTIND','CUSTCD','FISSPURP']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    c = row['CUSTCD']
    p = row['FISSPURP']
    
    if c == '79':
        btrm_records.append({'BNMCODE': f"34000{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['10','02','03','11','12']:
        btrm_records.append({'BNMCODE': f"340001000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['20','13','17','30','31','32','33','34','35','37','38','39','40','04','05','06','45']:
        if c in ['04','05','06']:
            btrm_records.append({'BNMCODE': f"34000{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        btrm_records.append({'BNMCODE': f"340002000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': f"340006100{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['62','44','46','47']:
        btrm_records.append({'BNMCODE': f"340006200{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['63','48','49','51']:
        btrm_records.append({'BNMCODE': f"340006300{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['64','57','75','59','52','53','54']:
        btrm_records.append({'BNMCODE': f"340006400{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['70','71','72','73','74']:
        btrm_records.append({'BNMCODE': f"340007000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    
    if c and c.isdigit() and 81 <= int(c) <= 99:
        btrm_records.append({'BNMCODE': f"340008000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['77','78']:
        btrm_records.append({'BNMCODE': f"34000{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        btrm_records.append({'BNMCODE': f"340007600{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['41','42','43','44','46','47','48','49','51','52','53','54']:
        btrm_records.append({'BNMCODE': f"34000{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(pl.col('PRODCD').str.slice(0,2) == '34').group_by(['AMTIND','CUSTCD','SECTORCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])
df_alm_sector = apply_sector_mapping(df_alm)

for row in df_alm_sector.iter_rows(named=True):
    c = row['CUSTCD']
    s = row['SECTORCD']
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54']:
        btrm_records.append({'BNMCODE': f"34000{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    if c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': f"340006100{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['62','44','46','47']:
        btrm_records.append({'BNMCODE': f"340006200{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['63','48','49','51']:
        btrm_records.append({'BNMCODE': f"340006300{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['64','57','75','59','52','53','54']:
        btrm_records.append({'BNMCODE': f"340006400{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['70','71','72','73','74']:
        btrm_records.append({'BNMCODE': f"340007000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c == '79':
        btrm_records.append({'BNMCODE': f"340007900{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['20','13','17','30','31','32','33','34','35','37','38','39','40','04','05','06','45']:
        if c in ['04','05','06']:
            btrm_records.append({'BNMCODE': f"34000{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        btrm_records.append({'BNMCODE': f"340002000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['10','02','03','11','12']:
        btrm_records.append({'BNMCODE': f"340001000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    
    if c and c.isdigit() and 81 <= int(c) <= 99:
        btrm_records.append({'BNMCODE': f"340008000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if s == '9700':
            btrm_records.append({'BNMCODE': '3400085009700Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif s in ['1000','2000','3000','4000','5000','6000','7000','8000','9000','9999']:
            btrm_records.append({'BNMCODE': '3400085009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    if c in ['95','96']:
        if s in ['1000','2000','3000','4000','5000','6000','7000','8000','9000','9999','9700']:
            btrm_records.append({'BNMCODE': '3400095000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        btrm_records.append({'BNMCODE': f"340009500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif c in ['77','78']:
        btrm_records.append({'BNMCODE': f"340007600{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        btrm_records.append({'BNMCODE': f"34000{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    (pl.col('PRODCD').str.slice(0,2) == '34') &
    pl.col('CUSTCD').is_in(['41','42','43','44','46','47','48','49','51','52','53','54'])
).group_by(['CUSTCD','STATECD','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': f"34000{row['CUSTCD']}000000{row['STATECD']}", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').is_in(['34152','34159','34160'])
).group_by(['AMTIND','PRODCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': f"{row['PRODCD']}00000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    (pl.col('DIRCTIND') == 'I') &
    (pl.col('ACCTNO').cast(pl.Utf8).str.slice(0,2) == '25')
).group_by(['PRODCD']).agg([pl.col('OUTSTAND').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': f"{row['PRODCD']}00000000Y", 'AMTIND': 'D', 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(pl.col('PRODCD') == '34180').group_by(['AMTIND','PRODCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])
for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': '3418000000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').str.slice(0,3).is_in(['341','342','343','344'])
).group_by(['AMTIND','ORIGMT','CUSTCD']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    c, om = row['CUSTCD'], row['ORIGMT']
    if om in ['10','12','13','14','15','16','17']:
        if c in ['81','82','83','84']:
            btrm_records.append({'BNMCODE': '3410081100000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['85','86','87','88','89','90','91','92','95','96','98','99']:
            btrm_records.append({'BNMCODE': '3410085100000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    if om in ['20','21','22','23','24','25','26','30','31','32','33']:
        if c == '81':
            btrm_records.append({'BNMCODE': '3410081200000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['85','86','87','88','89','90','91','92','95','96','98','99']:
            btrm_records.append({'BNMCODE': '3410085200000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').is_in(['34111','34112','34113','34114','34115','34116','34117','34120','34149'])
).group_by(['AMTIND','PRODCD','FISSPURP']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': f"{row['PRODCD']}00000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    if row['PRODCD'] == '34111':
        fp = row['FISSPURP']
        if fp and fp[:3] == '043':
            btrm_records.append({'BNMCODE': '3411100000430Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if fp and fp[:3] == '021':
            btrm_records.append({'BNMCODE': '3411100000210Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if row['AMTIND'] == 'I':
            btrm_records.append({'BNMCODE': '3070300000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').is_in(['34111','34112','34113','34114','34115','34116','34117','34120','34149'])
).group_by(['AMTIND','ORIGMT']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    om = row['ORIGMT']
    if om in ['10','12','13','14','15','16','17']:
        btrm_records.append({'BNMCODE': '3411000100000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif om in ['21','22','23','24','25','26','31','32','33']:
        btrm_records.append({'BNMCODE': f"3411000{om}0000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').is_in(['34111','34112','34113','34114','34115','34116','34117','34120','34149'])
).group_by(['AMTIND','REMAINMT']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    rm = row['REMAINMT']
    if rm in ['51','52','53','54','55','56','57']:
        btrm_records.append({'BNMCODE': '3411000500000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif rm in ['61','62','63','64','71','72','73']:
        btrm_records.append({'BNMCODE': f"3411000{rm}0000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btmast.filter(
    (pl.col('LEVELS') == 1) & (pl.col('D') == 1) &
    (pl.col('ACCTNO').cast(pl.Utf8).str.slice(0,2) == '25')
).group_by(['AMTIND']).agg([pl.col('APPRLIMT').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    btrm_records.append({'BNMCODE': '8150000000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_b80200 = df_btmast.filter(
    (pl.col('LEVELS') == 1) & (pl.col('D') == 1) & (pl.col('DBALANCE') <= 0) &
    (pl.col('CUSTCD') != '') & (pl.col('ACCTNO').cast(pl.Utf8).str.slice(0,2) == '25')
).sort('ACCTNO')

df_alm = df_b80200.group_by(['CUSTCD']).agg([pl.len().alias('_FREQ_')])

for row in df_alm.iter_rows(named=True):
    amt = row['_FREQ_'] * 1000
    c = row['CUSTCD']
    
    btrm_records.append({'BNMCODE': '8020000000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    
    if c in ['61','41','42','43']:
        btrm_records.append({'BNMCODE': '8020061000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54']:
        btrm_records.append({'BNMCODE': '8020065000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
        btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['70','71','72','73','74']:
        btrm_records.append({'BNMCODE': '8050070000000Y', 'AMTIND': 'D', 'AMOUNT': amt})
    if c in ['77','78','79']:
        btrm_records.append({'BNMCODE': f"80500{c}000000Y", 'AMTIND': 'D', 'AMOUNT': amt})
    if c and c.isdigit() and 81 <= int(c) <= 99:
        btrm_records.append({'BNMCODE': '8050080000000Y', 'AMTIND': 'D', 'AMOUNT': amt})

df_alm_fix = pl.DataFrame(btrm_records).with_columns([
    pl.when(pl.col('BNMCODE') == '8020065000000Y').then(pl.lit('8050065000000Y'))
    .when(pl.col('BNMCODE') == '8020061000000Y').then(pl.lit('8050061000000Y'))
    .when(pl.col('BNMCODE') == '8051065000000Y').then(pl.lit('8050065000000Y'))
    .when(pl.col('BNMCODE') == '8051061000000Y').then(pl.lit('8050061000000Y'))
    .otherwise(pl.col('BNMCODE')).alias('BNMCODE')
])

df_alm_extra = df_alm_fix.filter(
    pl.col('BNMCODE').is_in(['8050065000000Y','8050061000000Y'])
).group_by(['BNMCODE','AMTIND']).agg([pl.col('AMOUNT').sum()])

btrm_records.extend(df_alm_extra.to_dicts())

df_alm = df_btmast.filter(
    (pl.col('SUBACCT') == 'OV') & (pl.col('CUSTCD') != ' ') & (pl.col('DCURBAL').is_not_null())
).with_columns([
    pl.when((pl.col('SECTORCD') == ' ') | (pl.col('SECTORCD').cast(pl.Int32,strict=False) < 1000))
    .then(pl.lit('9999')).otherwise(pl.col('SECTORCD')).alias('SECTORCD')
]).sort('ACCTNO')

df_alm_agg = df_alm.group_by(['AMTIND','SUBACCT','ORIGMT','SECTORCD']).agg([pl.col('DUNDRAWN').sum().alias('AMOUNT')])

for row in df_alm_agg.iter_rows(named=True):
    btrm_records.append({'BNMCODE': '5620000000470Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    om = row['ORIGMT']
    if om in ['10','12','13','14','15','16','17']:
        btrm_records.append({'BNMCODE': '5629900100000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    elif om in ['20','21','22','23','24','25','26','31','32','33']:
        btrm_records.append({'BNMCODE': '5629900200000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm_sector2 = apply_sector_mapping(df_alm_agg)
for row in df_alm_sector2.iter_rows(named=True):
    if row['SECTORCD'] != '    ':
        btrm_records.append({'BNMCODE': f"562000000{row['SECTORCD']}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm = df_btrad.filter(
    pl.col('PRODCD').str.slice(0,3).is_in(['341','342','343','344'])
).group_by(['AMTIND','PRODCD','LIABCODE']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alm.iter_rows(named=True):
    lb = row['LIABCODE']
    bic = '30593' if lb in ['DAU','DDU','DDT','POS'] else \
          '30596' if lb in ['MCF','MDL','MFL','MLL','MOD','MOF','MOL','IEF','BAP','BAI','BAE','BAS','FTL','FTI'] else \
          '30597' if lb in ['VAL','DIL','FIL','TML','TMC','TMO','TNL','TNC','TNO','FFS','FFU','FCS','FCU','FFL'] else '30595'
    btrm_records.append({'BNMCODE': f"{bic}00000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alm_sorted = df_btrad.filter(pl.col('PRODCD').str.slice(0,2) == '34').sort(['ACCTNO','TRANSREF','CUSTCD','FISSPURP','SECTORCD'])
df_almx = df_alm_sorted.group_by(['ACCTNO','TRANSREF','CUSTCD','FISSPURP','SECTORCD']).agg([pl.col('BALANCE').sum()])
df_alm_merged = df_alm_sorted.unique(subset=['ACCTNO','TRANSREF','CUSTCD','FISSPURP','SECTORCD']).join(
    df_almx, on=['ACCTNO','TRANSREF','CUSTCD','FISSPURP','SECTORCD'], how='left', suffix='_sum'
).with_columns([pl.col('BALANCE_sum').alias('BALANCE')])

df_alm_filtered = df_alm_merged.filter(pl.col('APPRDATE') <= reptdate).with_columns([
    pl.when(pl.col('APPRDATE') < sdate).then(pl.lit(0)).otherwise(pl.col('APPRLIM2')).alias('APPRLIM2'),
    pl.when((pl.col('PRODCD') == '34190') & (pl.col('NOTETERM') != pl.col('EARNTERM')) &
            (pl.col('NOTETERM') != pl.col('LASTNOTE'))).then(pl.col('BALANCE')).otherwise(pl.lit(0)).alias('ROLLOVER')
])

df_alm_purp = df_alm_filtered.group_by(['FISSPURP','AMTIND']).agg([
    pl.col('DISBURSE').sum(), pl.col('REPAID').sum(),
    pl.col('APPRLIM2').sum(), pl.col('ROLLOVER').sum()
])

for row in df_alm_purp.iter_rows(named=True):
    p = row['FISSPURP']
    btrm_records.append({'BNMCODE': f"683400000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
    btrm_records.append({'BNMCODE': f"783400000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
    btrm_records.append({'BNMCODE': f"821520000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})

df_alm_smi = df_alm_filtered.group_by(['FISSPURP','CUSTCD','AMTIND']).agg([
    pl.col('DISBURSE').sum(), pl.col('REPAID').sum(),
    pl.col('APPRLIM2').sum(), pl.col('ROLLOVER').sum()
])

smi_exp = []
for row in df_alm_smi.iter_rows(named=True):
    if row['FISSPURP'] in ['0220','0230','0210','0211','0212']:
        smi_exp.append({**row, 'FISSPURP': '0200'})
    smi_exp.append(row)

df_alm_smi = pl.DataFrame(smi_exp) if smi_exp else df_alm_smi

for row in df_alm_smi.iter_rows(named=True):
    c, p = row['CUSTCD'], row['FISSPURP']
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54','77','78','79']:
        btrm_records.append({'BNMCODE': f"68340{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"78340{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"82152{c}00{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['61','41','42','43','62','44','46','47','63','48','49','51','57','59','75','52','53','54']:
        btrm_records.append({'BNMCODE': f"683406000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
    
    if c in ['02','03','11','12']:
        btrm_records.append({'BNMCODE': f"683401000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783401000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821521000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06']:
        btrm_records.append({'BNMCODE': f"683402000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783402000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821522000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['71','72','73','74']:
        btrm_records.append({'BNMCODE': f"683407000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783407000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821527000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99']:
        btrm_records.append({'BNMCODE': f"683408000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783408000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821528000{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['41','42','43','61']:
        btrm_records.append({'BNMCODE': f"683406100{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406100{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526100{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['62','44','46','47']:
        btrm_records.append({'BNMCODE': f"683406200{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406200{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526200{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['63','48','49','51']:
        btrm_records.append({'BNMCODE': f"683406300{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406300{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526300{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['64','52','53','54','59','75','57']:
        btrm_records.append({'BNMCODE': f"683406400{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406400{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526400{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64']:
        btrm_records.append({'BNMCODE': f"683406500{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406500{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526500{p}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})

df_alm_sector_filt = df_alm_filtered.filter(
    (pl.col('PRODCD').str.slice(0,2) == '34') |
    ((pl.col('PRODUCT').is_in([225,226])) & (pl.col('SECTORCD') != '0410'))
).group_by(['SECTORCD','AMTIND']).agg([
    pl.col('DISBURSE').sum(), pl.col('REPAID').sum(),
    pl.col('APPRLIM2').sum(), pl.col('ROLLOVER').sum()
])

df_alm_sector_mapped = apply_sector_mapping(df_alm_sector_filt)

for row in df_alm_sector_mapped.iter_rows(named=True):
    s = row['SECTORCD']
    btrm_records.append({'BNMCODE': f"683400000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
    btrm_records.append({'BNMCODE': f"783400000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
    btrm_records.append({'BNMCODE': f"821520000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})

df_almx_sector = df_alm_filtered.group_by(['SECTORCD','CUSTCD','AMTIND']).agg([
    pl.col('DISBURSE').sum(), pl.col('REPAID').sum(),
    pl.col('APPRLIM2').sum(), pl.col('ROLLOVER').sum()
])

df_almx_sector_mapped = apply_sector_mapping(df_almx_sector)

for row in df_almx_sector_mapped.iter_rows(named=True):
    c, s = row['CUSTCD'], row['SECTORCD']
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54','77','78','79']:
        btrm_records.append({'BNMCODE': f"68340{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"78340{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"82152{c}00{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['61','41','42','43','62','44','46','47','63','48','49','51','57','59','75','52','53','54']:
        btrm_records.append({'BNMCODE': f"683406000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
    
    if c in ['02','03','11','12']:
        btrm_records.append({'BNMCODE': f"683401000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783401000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821521000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06']:
        btrm_records.append({'BNMCODE': f"683402000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783402000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821522000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['71','72','73','74']:
        btrm_records.append({'BNMCODE': f"683407000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783407000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821527000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99'] and s != '9999':
        btrm_records.append({'BNMCODE': f"683408000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783408000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821528000{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
        btrm_records.append({'BNMCODE': f"683408500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783408500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821528500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if s in ['1000','2000','3000','4000','5000','6000','7000','8000','9000','9999','9700']:
        if c in ['81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99']:
            btrm_records.append({'BNMCODE': '6834080000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834080000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215280000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
            btrm_records.append({'BNMCODE': '6834085000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834085000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215285000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
        
        if c in ['81','82','83','84','85','86','87','88','89','90','91','92','98','99']:
            btrm_records.append({'BNMCODE': '6834085009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834085009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215285009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
            btrm_records.append({'BNMCODE': '6834080009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834080009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215280009999Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
        
        if c in ['95','96']:
            btrm_records.append({'BNMCODE': '6834095000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834095000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215295000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
        
        if c in ['77','78']:
            btrm_records.append({'BNMCODE': '6834076000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
            btrm_records.append({'BNMCODE': '7834076000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
            btrm_records.append({'BNMCODE': '8215276000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['95','96'] and s != '9999':
        btrm_records.append({'BNMCODE': f"683409500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783409500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821529500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['77','78']:
        btrm_records.append({'BNMCODE': f"683407600{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783407600{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821527600{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['41','42','43','61']:
        btrm_records.append({'BNMCODE': f"683406100{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406100{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526100{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['62','44','46','47']:
        btrm_records.append({'BNMCODE': f"683406200{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406200{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526200{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['63','48','49','51']:
        btrm_records.append({'BNMCODE': f"683406300{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406300{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526300{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['64','52','53','54','59','75','57']:
        btrm_records.append({'BNMCODE': f"683406400{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406400{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526400{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})
    
    if c in ['41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64']:
        btrm_records.append({'BNMCODE': f"683406500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['DISBURSE']})
        btrm_records.append({'BNMCODE': f"783406500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['REPAID']})
        btrm_records.append({'BNMCODE': f"821526500{s}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['ROLLOVER']})

df_lnaging = df_btrad.filter(
    pl.col('PRODCD').is_in(['34111','34112','34113','34114','34115','34116','34117','34120','34230','34149'])
).group_by(['PRODCD','REMAINMT','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

lnaging_records = []
for row in df_lnaging.iter_rows(named=True):
    lntype = 'S' if row['PRODCD'] == '34230' and row['AMTIND'] == 'D' else 'T' if row['AMTIND'] == 'D' else 'I'
    rm = row['REMAINMT']
    if rm in ['51','52','53','54','55','56','57']:
        lnaging_records.append({'BNMCODE': '3411000500000Y', 'LNTYPE': lntype, 'AMOUNT': row['AMOUNT']})
    elif rm in ['61','62','63','64','71','72','73']:
        lnaging_records.append({'BNMCODE': f"3411000{rm}0000Y", 'LNTYPE': lntype, 'AMOUNT': row['AMOUNT']})

df_btrm = pl.DataFrame(btrm_records).group_by(['BNMCODE','AMTIND']).agg([pl.col('AMOUNT').sum()])
df_btrm.write_parquet(f'{BNM_DIR}BTRM{reptmon}{nowk}.parquet')

print(f"PUBLIC BANK BERHAD")
print(f"REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II - M&I LOAN")
print(f"REPORT DATE: {rdate}\n")
print(f"Records generated: {len(df_btrm)}")

df_al = df_btrm.with_columns([pl.col('BNMCODE').alias('ITCODE')]).sort('ITCODE')

phead = f"RDAL{reptday}{reptmon}{reptyear}"

with open(OUTPUT_RDALKM, 'w') as f:
    f.write(f"{phead}\n")
    f.write("AL\n")
    
    for itcode in df_al['ITCODE'].unique().sort():
        df_code = df_al.filter(pl.col('ITCODE') == itcode)
        amountd = df_code.filter(pl.col('AMTIND') == 'D')['AMOUNT'].sum() if len(df_code.filter(pl.col('AMTIND') == 'D')) > 0 else 0
        amounti = df_code.filter(pl.col('AMTIND') == 'I')['AMOUNT'].sum() if len(df_code.filter(pl.col('AMTIND') == 'I')) > 0 else 0
        amountd_r = round(amountd / 1000)
        amounti_r = round(amounti / 1000)
        total = amountd_r + amounti_r
        f.write(f"{itcode};{total};{amounti_r}\n")

with open(OUTPUT_NSRS, 'w') as f:
    f.write(f"{phead}\n")
    f.write("AL\n")
    
    for itcode in df_al['ITCODE'].unique().sort():
        df_code = df_al.filter(pl.col('ITCODE') == itcode)
        amountd = df_code.filter(pl.col('AMTIND') == 'D')['AMOUNT'].sum() if len(df_code.filter(pl.col('AMTIND') == 'D')) > 0 else 0
        amounti = df_code.filter(pl.col('AMTIND') == 'I')['AMOUNT'].sum() if len(df_code.filter(pl.col('AMTIND') == 'I')) > 0 else 0
        
        if itcode[:2] == '80':
            amountd_r = round(amountd / 1000)
            amounti_r = round(amounti / 1000)
        else:
            amountd_r = round(amountd)
            amounti_r = round(amounti)
        
        total = amountd_r + amounti_r
        f.write(f"{itcode};{total};{amounti_r}\n")

print(f"\nOutput files generated:")
print(f"  {OUTPUT_RDALKM}")
print(f"  {OUTPUT_NSRS}")
print(f"  {BNM_DIR}BTRM{reptmon}{nowk}.parquet")
