import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

LOAN_DIR = 'data/loan/'
BNM_DIR = 'data/bnm/'
PBCS_DIR = 'data/pbcs/'
OUTPUT_RDAL = 'data/rdal.txt'
OUTPUT_NSRS = 'data/nsrs.txt'

today = datetime.today().date()
first_of_month = datetime(today.year, today.month, 1).date()
reptdate = first_of_month - timedelta(days=1)

day = reptdate.day
if day == 8:
    sdd = 1
    wk = '1'
    wk1 = '4'
elif day == 15:
    sdd = 9
    wk = '2'
    wk1 = '1'
elif day == 22:
    sdd = 16
    wk = '3'
    wk1 = '2'
else:
    sdd = 23
    wk = '4'
    wk1 = '3'

mm = reptdate.month
if wk == '1':
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm

sdate = datetime(reptdate.year, mm, sdd).date()

nowk = wk
nowk1 = wk1
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = reptdate.year
reptday = f'{reptdate.day:02d}'
rdate = reptdate.strftime('%d%m%y')
fdate = reptdate.strftime('%d%m%Y')
eldate = f'{reptdate.year}{reptdate.month:02d}'
sdate_str = sdate.strftime('%d%m%y')
sdesc = 'PUBLIC BANK BERHAD'

print(f"Processing RDAL Report Generation")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Previous Week: {nowk1}")
print(f"Start Date: {sdate.strftime('%d/%m/%Y')}")

df_pbbrdal = pl.read_parquet(f'{BNM_DIR}PBBRDAL.parquet')

df_pbbrdal1 = df_pbbrdal.with_columns([
    pl.when(pl.col('ITCODE').str.slice(1, 1) == '0')
    .then(pl.lit(' '))
    .otherwise(pl.lit('D'))
    .alias('AMTIND'),
    pl.lit(0).alias('AMOUNT')
]).sort(['ITCODE', 'AMTIND'])

df_alw = pl.concat([
    pl.read_parquet(f'{BNM_DIR}ALW{reptmon}{nowk}.parquet'),
    pl.read_parquet(f'{PBCS_DIR}CCLW{reptmon}{nowk}.parquet')
]).sort(['ITCODE', 'AMTIND'])

df_rdal = df_alw.join(
    df_pbbrdal1, 
    on=['ITCODE', 'AMTIND'], 
    how='outer',
    suffix='_pbb'
).with_columns([
    pl.when(pl.col('AMOUNT').is_not_null() & pl.col('AMOUNT_pbb').is_not_null())
    .then(pl.col('AMOUNT'))
    .when(pl.col('AMOUNT').is_null() & pl.col('AMOUNT_pbb').is_not_null())
    .then(pl.col('AMOUNT_pbb'))
    .when(pl.col('AMOUNT').is_not_null() & pl.col('AMOUNT_pbb').is_null())
    .then(pl.col('AMOUNT'))
    .otherwise(0)
    .alias('AMOUNT')
]).drop('AMOUNT_pbb')

df_rdal = df_rdal.filter(
    ~(
        ((pl.col('ITCODE').str.slice(0, 5) >= '30221') & (pl.col('ITCODE').str.slice(0, 5) <= '30228')) |
        ((pl.col('ITCODE').str.slice(0, 5) >= '30231') & (pl.col('ITCODE').str.slice(0, 5) <= '30238')) |
        ((pl.col('ITCODE').str.slice(0, 5) >= '30091') & (pl.col('ITCODE').str.slice(0, 5) <= '30098')) |
        ((pl.col('ITCODE').str.slice(0, 5) >= '40151') & (pl.col('ITCODE').str.slice(0, 5) <= '40158')) |
        (pl.col('ITCODE').str.slice(0, 5) == 'NSSTS')
    )
)

df_cag = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').filter(
    pl.col('PZIPCODE').is_in([2002, 2013, 3039, 3047, 800003098, 800003114,
                              800004016, 800004022, 800004029, 800040050,
                              800040053, 800050024, 800060024, 800060045,
                              800060081, 80060085])
).with_columns([
    pl.lit('7511100000000Y').alias('ITCODE')
])

df_cag_agg = df_cag.group_by(['ITCODE', 'AMTIND']).agg([
    pl.col('BALANCE').sum().alias('AMOUNT')
])

df_rdal = pl.concat([df_rdal, df_cag_agg])

df_rdal = df_rdal.filter(
    pl.col('ITCODE') != '4364008110000Y'
).with_columns([
    pl.when(pl.col('ITCODE') != '3400061006120Y')
    .then(pl.col('AMOUNT').abs())
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
]).sort(['ITCODE', 'AMTIND'])

df_rdal_filtered = df_rdal.filter(
    ~pl.col('ITCODE').str.slice(13, 1).is_in(['F', '#'])
)

def classify_record(itcode, amtind):
    """Classify record as AL, OB, or SP"""
    if amtind != ' ':
        if itcode[:3] in ['307']:
            return 'SP'
        elif itcode[:5] in ['40190', '40191']:
            return 'SP'
        elif itcode[:4] == 'SSTS':
            return 'SP'
        elif itcode[0] != '5':
            if itcode[:3] in ['685', '785']:
                return 'SP'
            else:
                return 'AL'
        else:
            return 'OB'
    elif itcode[1:2] == '0':
        return 'SP'
    return None

classifications = []
for row in df_rdal_filtered.iter_rows(named=True):
    classification = classify_record(row['ITCODE'], row.get('AMTIND', ' '))
    classifications.append(classification)

df_rdal_filtered = df_rdal_filtered.with_columns([
    pl.Series('CLASSIFICATION', classifications)
])

df_al = df_rdal_filtered.filter(pl.col('CLASSIFICATION') == 'AL')
df_ob = df_rdal_filtered.filter(pl.col('CLASSIFICATION') == 'OB')
df_sp = df_rdal_filtered.filter(pl.col('CLASSIFICATION') == 'SP')

df_sp = df_sp.with_columns([
    pl.when(pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
    .then(pl.lit('4017000000000Y'))
    .otherwise(pl.col('ITCODE'))
    .alias('ITCODE')
])

phead = f"RDAL{reptday}{reptmon}{reptyear}"

with open(OUTPUT_RDAL, 'w') as f:
    f.write(f"{phead}\n")
    f.write("AL\n")
    
    amountd_total = 0
    amounti_total = 0
    amountf_total = 0
    
    for itcode, group in df_al.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amounti = 0
        amountf = 0
        
        proceed = True
        
        if reptday in ['08', '22']:
            if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                proceed = False
        
        if itcode == '4966000000000F':
            proceed = False
        
        if not proceed:
            continue
        
        for row in group.iter_rows(named=True):
            amount = round(row['AMOUNT'] / 1000)
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += amount
            elif amtind == 'I':
                amounti += amount
            elif amtind == 'F':
                amountf += amount
        
        total = amountd + amounti + amountf
        f.write(f"{itcode};{total};{amounti};{amountf}\n")
    
    f.write("OB\n")
    
    for itcode, group in df_ob.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amounti = 0
        amountf = 0
        
        for row in group.iter_rows(named=True):
            amount = row['AMOUNT']
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += round(amount / 1000)
            elif amtind == 'I':
                amounti += round(amount / 1000)
            elif amtind == 'F':
                amountf += round(amount / 1000)
        
        total = amountd + amounti + amountf
        f.write(f"{itcode};{total};{amounti};{amountf}\n")
    
    f.write("SP\n")
    
    df_sp_sorted = df_sp.sort('ITCODE')
    
    for itcode, group in df_sp_sorted.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amountf = 0
        
        for row in group.iter_rows(named=True):
            amount = round(row['AMOUNT'] / 1000)
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += amount
            elif amtind == 'F':
                amountf += amount
        
        total = amountd + amountf
        f.write(f"{itcode};{total};{amountf}\n")

df_rdal_hash = df_rdal.with_columns([
    pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
    .then(
        pl.struct([
            pl.col('ITCODE').str.slice(0, 13) + 'Y',
            pl.col('AMOUNT') * -1
        ])
    )
    .otherwise(
        pl.struct([
            pl.col('ITCODE'),
            pl.col('AMOUNT')
        ])
    )
    .alias('modified')
]).with_columns([
    pl.col('modified').struct.field('field_0').alias('ITCODE'),
    pl.col('modified').struct.field('field_1').alias('AMOUNT')
]).drop('modified')

df_rdal_agg = df_rdal_hash.group_by(['ITCODE', 'AMTIND']).agg([
    pl.col('AMOUNT').sum()
])

classifications2 = []
for row in df_rdal_agg.iter_rows(named=True):
    classification = classify_record(row['ITCODE'], row.get('AMTIND', ' '))
    classifications2.append(classification)

df_rdal_agg = df_rdal_agg.with_columns([
    pl.Series('CLASSIFICATION', classifications2)
])

df_al2 = df_rdal_agg.filter(pl.col('CLASSIFICATION') == 'AL')
df_ob2 = df_rdal_agg.filter(pl.col('CLASSIFICATION') == 'OB')
df_sp2 = df_rdal_agg.filter(pl.col('CLASSIFICATION') == 'SP')

df_sp2 = df_sp2.with_columns([
    pl.when(pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
    .then(pl.lit('4017000000000Y'))
    .otherwise(pl.col('ITCODE'))
    .alias('ITCODE')
])

with open(OUTPUT_NSRS, 'w') as f:
    f.write(f"{phead}\n")
    f.write("AL\n")
    
    for itcode, group in df_al2.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amounti = 0
        amountf = 0
        
        proceed = True
        
        if reptday in ['08', '22']:
            if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                proceed = False
        
        if not proceed:
            continue
        
        for row in group.iter_rows(named=True):
            amount = round(row['AMOUNT'])
            if itcode[:2] == '80':
                amount = round(row['AMOUNT'] / 1000)
            
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += amount
            elif amtind == 'I':
                amounti += amount
            elif amtind == 'F':
                amountf += amount
        
        total = amountd + amounti + amountf
        f.write(f"{itcode};{total};{amounti};{amountf}\n")
    
    f.write("OB\n")
    
    for itcode, group in df_ob2.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amounti = 0
        amountf = 0
        
        for row in group.iter_rows(named=True):
            amount = row['AMOUNT']
            if itcode[:2] == '80':
                amount = round(amount / 1000)
            
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += round(amount)
            elif amtind == 'I':
                amounti += round(amount)
            elif amtind == 'F':
                amountf += round(amount)
        
        total = amountd + amounti
        f.write(f"{itcode};{total};{amounti};{amountf}\n")
    
    f.write("SP\n")
    
    df_sp2_sorted = df_sp2.sort('ITCODE')
    
    for itcode, group in df_sp2_sorted.group_by('ITCODE', maintain_order=True):
        amountd = 0
        amountf = 0
        
        for row in group.iter_rows(named=True):
            amount = round(row['AMOUNT'])
            amtind = row.get('AMTIND', '')
            
            if amtind == 'D':
                amountd += amount
            elif amtind == 'F':
                amountf += amount
        
        total = amountd + amountf
        if itcode[:2] == '80':
            total = round(total / 1000)
        
        f.write(f"{itcode};{total};{amountf}\n")

print(f"\nRDAL Report Generation Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_RDAL} (RDAL - amounts in thousands)")
print(f"  {OUTPUT_NSRS} (NSRS - full amounts for '80' codes)")
print(f"\nReport header: {phead}")
print(f"Classification breakdown:")
print(f"  AL (Assets & Liabilities): {len(df_al)} items")
print(f"  OB (Off-Balance Sheet): {len(df_ob)} items")
print(f"  SP (Specific Provisions): {len(df_sp)} items")
