import polars as pl
from datetime import datetime

DEPOSIT_DIR = 'data/deposit/'
FD_DIR = 'data/fd/'
LOAN_DIR = 'data/loan/'
SASDATA_DIR = 'data/sasdata/'
KAPITI5_FILE = 'data/kapiti5.dat'
OUTPUT_FILE = 'reports/islamic_branch_deposit_position.txt'

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

reptyrs = f'{reptdate.year % 100:02d}'
reptyear = reptdate.year
reptmon = f'{reptdate.month:02d}'
reptday = f'{reptdate.day:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Processing Islamic Branch Deposit Position Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

df_saving_raw = pl.read_parquet(f'{DEPOSIT_DIR}SAVING.parquet').filter(
    ~pl.col('OPENIND').is_in(['B', 'C', 'P', 'Z'])
)

saving_products = []
for row in df_saving_raw.iter_rows(named=True):
    product = row['PRODUCT']
    
    if product in [200, 201]:
        item = '1 PLUS SAVING ACCOUNTS'
    elif product == 202:
        item = '2 YOUNG ACHIEVER S/A'
    elif product == 212:
        item = '3 WISE SAVING ACCOUNTS'
    elif product == 203:
        item = '4 50 PLUS SAVING ACCOUNTS'
    elif product == 205:
        item = '5 BASIC SAVING ACCOUNTS'
    elif product == 206:
        item = '6 BASIC 55 SAVING ACCOUNTS'
    elif product == 213:
        item = '7 PB SAVELINK ACCOUNTS'
    else:
        continue
    
    saving_products.append({
        **row,
        'ITEM': item,
        'GROUP': '(1) DEPOSIT PRODUCTS',
        'CATG': '(A) SAVINGS ACCOUNTS'
    })
    
    if row.get('USER3') in ['2', '4']:
        saving_products.append({
            **row,
            'ITEM': '2 SAVING ACCOUNTS',
            'GROUP': '(2) PB PREMIUM CLUB',
            'CATG': '(A) CLUB MEMBERS ON :'
        })

df_saving = pl.DataFrame(saving_products)

df_current_raw = pl.read_parquet(f'{DEPOSIT_DIR}CURRENT.parquet').filter(
    ~pl.col('OPENIND').is_in(['B', 'C', 'P'])
)

current_products = []
for row in df_current_raw.iter_rows(named=True):
    product = row['PRODUCT']
    curbal = row.get('CURBAL', 0)
    
    if product in [100, 102, 106, 180]:
        if curbal < 0:
            item = '1B PLUS ACCOUNTS (DEBIT)'
        else:
            item = '1A PLUS ACCOUNTS (CREDIT)'
    elif product in [150, 151, 152, 181]:
        item = '2 ACE ACCOUNTS'
    elif product == 90:
        item = '3 BASIC CURRENT ACOUNTS'
    elif product == 91:
        item = '4 BASIC 55 CURRENT ACOUNTS'
    elif product in [156, 157, 158]:
        item = '5 PB CURRENTLINK ACOUNTS'
    else:
        continue
    
    current_products.append({
        **row,
        'ITEM': item,
        'GROUP': '(1) DEPOSIT PRODUCTS',
        'CATG': '(B) CURRENT ACCOUNTS'
    })
    
    if product in [150, 151, 152, 100, 102, 160, 162] and row.get('USER3') in ['2', '4']:
        current_products.append({
            **row,
            'ITEM': '1 CURRENT ACCOUNTS',
            'GROUP': '(2) PB PREMIUM CLUB',
            'CATG': '(A) CLUB MEMBERS ON :'
        })

df_current = pl.DataFrame(current_products)

df_fd_raw = pl.read_parquet(f'{FD_DIR}FD.parquet').filter(
    ~pl.col('OPENIND').is_in(['B', 'C', 'P'])
)

fd_products = []
for row in df_fd_raw.iter_rows(named=True):
    intplan = row['INTPLAN']
    
    if intplan in [360, 361, 362, 363, 364, 365, 366, 460]:
        item = '3 PB GOLDEN 50 PLUS'
    elif intplan in [320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331,
                     420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431]:
        item = '2 FIXED DEPOSIT LIFE'
    elif intplan in list(range(400, 440)) + list(range(600, 640)) + \
                     list(range(300, 320)) + [373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384] + \
                     list(range(500, 540)):
        item = '1 PLUS FIXED DEPOSIT'
    else:
        continue
    
    fd_products.append({
        **row,
        'ITEM': item,
        'GROUP': '(1) DEPOSIT PRODUCTS',
        'CATG': '(C) FIXED DEPOSIT ACCOUNT',
        'PRODUCT': intplan
    })

df_fd = pl.DataFrame(fd_products).sort(['BRANCH', 'ITEM', 'ACCTNO', 'CDNO'])

fd_sorted = []
current_acct = None
amount = 0
noofcd = 0

for row in df_fd.iter_rows(named=True):
    acctno = row['ACCTNO']
    
    if acctno != current_acct:
        if current_acct is not None:
            fd_sorted[-1]['AMOUNT'] = amount
            fd_sorted[-1]['NOOFCD'] = noofcd
        
        current_acct = acctno
        amount = 0
        noofcd = 0
    
    amount += row.get('CURBAL', 0)
    noofcd += 1
    
    fd_sorted.append({**row, 'AMOUNT': amount, 'NOOFCD': noofcd})

if fd_sorted:
    fd_sorted[-1]['AMOUNT'] = amount
    fd_sorted[-1]['NOOFCD'] = noofcd

df_fdsort = pl.DataFrame(fd_sorted)

df_loan = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').filter(
    (pl.col('REVERSED') != 'Y') &
    (pl.col('PAIDIND') != 'P') &
    pl.col('NOTENO').is_not_null() &
    (
        (pl.col('NOTENO') < 100) |
        ((pl.col('NOTENO') >= 20000) & (pl.col('NOTENO') <= 29999)) |
        ((pl.col('NOTENO') >= 30000) & (pl.col('NOTENO') <= 39999) & (pl.col('FLAG1') == 'F'))
    ) &
    pl.col('LOANTYPE').is_in([110, 111, 112, 113, 114, 115, 116, 200, 201, 204, 205,
                               209, 210, 211, 212, 214, 215, 225, 226, 227, 228, 230,
                               231, 232, 233, 234]) &
    ~pl.col('RISKRATE').is_in(['1', '2', '3', '4']) &
    pl.col('CUSTCODE').is_in([77, 78, 95, 96]) &
    pl.col('ORGTYPE').is_in(['E', 'M', 'N', 'S'])
).drop('SECTOR').unique(subset=['ACCTNO', 'NOTENO'])

df_sloan = pl.read_parquet(f'{SASDATA_DIR}LOAN{reptmon}{nowk}.parquet').filter(
    (
        (pl.col('PRODUCT').is_in([200, 201, 204, 205, 209, 210, 211, 212, 214, 215,
                                   225, 226, 227, 228, 230, 231, 232, 233, 234]) &
         (pl.col('APPRLIMT') >= 150000)) |
        (pl.col('PRODUCT').is_in([110, 111, 112, 113, 114, 115, 116]) &
         (pl.col('NETPROC') >= 150000))
    )
).drop('SECTOR').unique(subset=['ACCTNO', 'NOTENO'])

df_pbloan = df_loan.join(df_sloan, on=['ACCTNO', 'NOTENO'], how='inner')

df_pbloans = df_pbloan.with_columns([
    pl.col('BALANCE').alias('LEDGBAL'),
    pl.lit('3 HOUSING LOANS').alias('ITEM'),
    pl.lit('(2) PB PREMIUM CLUB').alias('GROUP'),
    pl.lit('(A) CLUB MEMBERS ON :').alias('CATG')
])

df_pbprem = pl.concat([
    df_saving.filter(pl.col('GROUP') == '(2) PB PREMIUM CLUB'),
    df_current.filter(pl.col('GROUP') == '(2) PB PREMIUM CLUB'),
    df_pbloans.select(['BRANCH', 'ITEM', 'GROUP', 'CATG', 'LEDGBAL'])
])

kapiti5_records = []
with open(KAPITI5_FILE, 'r') as f:
    for line in f:
        try:
            branches = line[208:238].strip()
            branci = ''.join(c for c in branches if c.isdigit())
            branch = int(branci) if branci else 0
            pbborpfb = line[200:203].strip()
            
            if pbborpfb == 'PBB' and branch < 500:
                kapiti5_records.append({
                    'BRANCH': branch,
                    'ACCTNO': line[280:300].strip(),
                    'AMT': 0,
                    'GROUP': '(3) SERVICES',
                    'CATG': '(A) PB TELEBANKING',
                    'ITEM': '1 SUBSCRIBERS'
                })
        except:
            continue

df_kapiti5 = pl.DataFrame(kapiti5_records).sort(['BRANCH', 'ACCTNO'])

df_saving_agg = df_saving.group_by(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
    pl.len().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('AMOUNT')
])

df_current_agg = df_current.group_by(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
    pl.len().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('AMOUNT')
])

df_fd_agg = df_fdsort.group_by(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
    pl.len().alias('NOACCT'),
    pl.col('NOOFCD').sum().alias('NOOFCD'),
    pl.col('AMOUNT').sum().alias('AMOUNT'),
    pl.col('CURBAL').sum().alias('AMOUNT1')
])

df_pbprem_agg = df_pbprem.group_by(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
    pl.len().alias('NOACCT'),
    pl.col('LEDGBAL').sum().alias('AMOUNT')
])

df_telebank_agg = df_kapiti5.group_by(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
    pl.len().alias('NOACCT'),
    pl.col('AMT').sum().alias('AMOUNT')
])

df_pdrdata = pl.concat([
    df_saving_agg,
    df_current_agg,
    df_fd_agg.drop('AMOUNT1'),
    df_pbprem_agg,
    df_telebank_agg
], how='diagonal').sort(['BRANCH', 'GROUP', 'CATG', 'ITEM'])

with open(OUTPUT_FILE, 'w') as f:
    pagecnt = 0
    date_today = datetime.today().strftime('%d%m%Y')
    
    prev_branch = None
    
    for branch, branch_group in df_pdrdata.group_by('BRANCH', maintain_order=True):
        if prev_branch is None or branch != prev_branch:
            pagecnt += 1
            f.write(f"REPORT NAME : BR-DEP-POS :  BRANCH : {branch:3d}         "
                    f"P U B L I C   I S L A M I C   B A N K   B E R H A D"
                    f"{' '*30}DATE : {date_today}\n")
            f.write(f"{' '*50}PRODUCTS POSITION AS AT : {rdate}\n")
            f.write(f"{' '*38}NO. OF ACCOUNTS/\n")
            f.write(f"     PRODUCT TYPES{' '*23}MEMBERS/SUBSCRIBERS{' '*6}NO OF RECEIPTS{' '*8}"
                    f"AMOUNT OUTSTANDING (RM)\n")
            f.write(f"     {'_'*13}{' '*23}{'_'*19}{' '*6}{'_'*14}{' '*8}{'_'*22}\n\n")
            prev_branch = branch
        
        prev_group = None
        prev_catg = None
        
        for row in branch_group.iter_rows(named=True):
            group = row['GROUP']
            catg = row['CATG']
            item = row['ITEM']
            noacct = row.get('NOACCT', 0)
            noofcd = row.get('NOOFCD', 0)
            amount = row.get('AMOUNT', 0)
            
            if group != prev_group:
                f.write(f"\n{group}\n")
                prev_group = group
            
            if catg != prev_catg:
                f.write(f"\n    {catg}\n")
                prev_catg = catg
            
            if item == '3 HOUSING LOANS':
                amount = 0.00
            
            f.write(f"        {item:<34}{noacct:>7,}")
            if noofcd:
                f.write(f"{' '*16}{noofcd:>7,}")
            else:
                f.write(f"{' '*23}")
            f.write(f"{' '*12}{amount:>18,.2f}\n")

print(f"\nIslamic Branch Deposit Position Report Complete")
print(f"\nOutput file generated:")
print(f"  {OUTPUT_FILE}")
print(f"\nBranches processed: {df_pdrdata['BRANCH'].n_unique()}")
print(f"\nProduct categories:")
print(f"  (1) DEPOSIT PRODUCTS")
print(f"      (A) SAVINGS ACCOUNTS - {len(df_saving_agg)} items")
print(f"      (B) CURRENT ACCOUNTS - {len(df_current_agg)} items")
print(f"      (C) FIXED DEPOSIT ACCOUNT - {len(df_fd_agg)} items")
print(f"  (2) PB PREMIUM CLUB - {len(df_pbprem_agg)} items")
print(f"  (3) SERVICES")
print(f"      (A) PB TELEBANKING - {len(df_telebank_agg)} subscribers")
