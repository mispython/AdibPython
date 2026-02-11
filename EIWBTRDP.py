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

nowk = wk

df_btrad = pl.read_parquet(f'{BNM_DIR}BTRAD{reptmon}{nowk}.parquet')

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

btrw_records = []

df_alw = df_btrad.filter(
    pl.col('PRODCD').str.slice(0,2) == '34'
).group_by(['CUSTCD','PRODCD','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alw.iter_rows(named=True):
    c = row['CUSTCD']
    p = row['PRODCD']
    
    if p[:3] == '346':
        if c in ['02','03','11','12','71','72','73','74','79']:
            btrw_records.append({'BNMCODE': f"34600{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06']:
            btrw_records.append({'BNMCODE': '3460020000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            if c in ['13','17']:
                btrw_records.append({'BNMCODE': f"34600{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['41','42','43','44','46','47','48','49','51','52','53','54','60','61','62','63','64','65','59','75','57']:
            btrw_records.append({'BNMCODE': '3460060000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['76','77','78']:
            btrw_records.append({'BNMCODE': '3460076000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['81','82','83','84']:
            btrw_records.append({'BNMCODE': '3460081000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['85','86','87','88','89','90','91','92','95','96','98','99']:
            btrw_records.append({'BNMCODE': '3460085000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        
        if c in ['41','42','43']:
            btrw_records.append({'BNMCODE': '3460061000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['44','46','47']:
            btrw_records.append({'BNMCODE': '3460062000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['48','49','51']:
            btrw_records.append({'BNMCODE': '3460063000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        if c in ['52','53','54','75','57','59']:
            btrw_records.append({'BNMCODE': '3460064000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        
        btrw_records.append({'BNMCODE': f"34600{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
    
    else:
        if c in ['02','03','11','12','71','72','73','74','79']:
            btrw_records.append({'BNMCODE': f"{p}{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06']:
            btrw_records.append({'BNMCODE': '3410020000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
            if c in ['13','17']:
                btrw_records.append({'BNMCODE': f"34100{c}000000Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['41','42','43','44','46','47','48','49','51','52','53','54','60','61','62','63','64','65','59','75','57']:
            btrw_records.append({'BNMCODE': '3410060000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['76','77','78']:
            btrw_records.append({'BNMCODE': '3410076000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['81','82','83','84']:
            btrw_records.append({'BNMCODE': '3410081000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})
        elif c in ['85','86','87','88','89','90','91','92','95','96','98','99']:
            btrw_records.append({'BNMCODE': '3410085000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alw = df_btrad.filter(
    (pl.col('PRODCD').str.slice(0,2) == '34') | (pl.col('PRODCD') == '54120')
).group_by(['PRODCD','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alw.iter_rows(named=True):
    btrw_records.append({'BNMCODE': '3051000000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alw = df_btrad.filter(
    pl.col('PRODUCT').is_in([225,226])
).group_by(['PRODCD','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

for row in df_alw.iter_rows(named=True):
    btrw_records.append({'BNMCODE': '7511100000000Y', 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_alw = df_btrad.filter(
    pl.col('PRODCD').str.slice(0,2) == '34'
).group_by(['SECTORCD','AMTIND']).agg([pl.col('BALANCE').sum().alias('AMOUNT')])

df_alw_sector = apply_sector_mapping(df_alw)

for row in df_alw_sector.iter_rows(named=True):
    if row['SECTORCD'] != '    ':
        btrw_records.append({'BNMCODE': f"340000000{row['SECTORCD']}Y", 'AMTIND': row['AMTIND'], 'AMOUNT': row['AMOUNT']})

df_btrw = pl.DataFrame(btrw_records).group_by(['BNMCODE','AMTIND']).agg([pl.col('AMOUNT').sum()])
df_btrw.write_parquet(f'{BNM_DIR}BTRW{reptmon}{nowk}.parquet')

print(f"PUBLIC BANK BERHAD")
print(f"REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - BANK TRADE")
print(f"REPORT DATE: {rdate}\n")
print(f"Records generated: {len(df_btrw)}")

df_al = df_btrw.with_columns([pl.col('BNMCODE').alias('ITCODE')]).sort('ITCODE')

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
print(f"  {BNM_DIR}BTRW{reptmon}{nowk}.parquet")
