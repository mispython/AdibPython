"""
EIILQ305 - Islamic Bank Ratio 5: ST IB Borrowing / ST Total Funding
"""

import polars as pl
from datetime import datetime, timedelta
from calendar import monthrange, isleap
import pandas as pd
import os

BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def calculate_remmth(matdate, reptdate, reptyear):
    """Calculate remaining months (REMMTH macro)"""
    if matdate is None or pd.isna(matdate):
        return None
    
    matdate_dt = matdate if isinstance(matdate, datetime) else datetime.strptime(str(matdate), '%Y-%m-%d')
    
    remy = matdate_dt.year - reptdate.year
    remm = matdate_dt.month - reptdate.month
    remd = matdate_dt.day - reptdate.day
    
    mdday = matdate_dt.day
    rpdays = monthrange(reptdate.year, reptdate.month)[1]
    
    if mdday > rpdays:
        mdday = rpdays
    
    if isleap(int(reptyear)):
        rpdays_feb = 29 if reptdate.month == 2 else rpdays
    else:
        rpdays_feb = rpdays
    
    remmth = remy * 12 + remm + (remd / rpdays_feb)
    
    return remmth

print("EIILQ305 - Islamic Bank Short Term Funding Ratio")
print("=" * 60)

try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    year = reptyear
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    sum4c103a = 0.0
    sum4c201a = 0.0
    sum4c103b = 0.0
    sum4c201b = 0.0
    sum4c103c = 0.0
    sum4c103f = 0.0
    sum4c201e = 0.0
    sum4c103h = 0.0
    sum4c201g = 0.0
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

print("\n1. Processing ALW...")
try:
    df_alw = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    df_alw = df_alw.filter(pl.col('AMTIND') != ' ')
    df_alw = df_alw.with_columns([pl.col('AMOUNT').alias('AMOUNT4')])
    
    alw_items = []
    
    single_mappings = {
        '4311002000000Y': ['C5.01B', 'C5.07B'],
        '4311003000000Y': ['C5.01C', 'C5.07C'],
        '4312002000000Y': ['C5.01D', 'C5.07D'],
        '4312003000000Y': ['C5.01E', 'C5.07E'],
        '4369902000000Y': ['C5.01F', 'C5.07F'],
    }
    
    for itcode, items in single_mappings.items():
        df_filtered = df_alw.filter(pl.col('ITCODE') == itcode)
        for item in items:
            df_temp = df_filtered.with_columns([pl.lit(item).alias('ITEM')])
            alw_items.append(df_temp)
    
    gid_codes = ['4213202520000Y', '4213203520000Y', '4213212520000Y',
                 '4213220520000Y', '4213260520000Y', '4213270520000Y',
                 '4213276520000Y', '4213279520000Y', '4213280520000Y',
                 '4213217520000Y', '4213257520000Y', '4213275520000Y']
    df_c504e = df_alw.filter(pl.col('ITCODE').is_in(gid_codes))
    df_c504e = df_c504e.with_columns([pl.lit('C5.04E').alias('ITEM')])
    alw_items.append(df_c504e)
    
    df_c504f = df_alw.filter(pl.col('ITCODE') == '4213280520000Y')
    df_c504f = df_c504f.with_columns([pl.lit('C5.04F').alias('ITEM')])
    alw_items.append(df_c504f)
    
    gid_fe_codes = ['4213202510000Y', '4213203510000Y', '4213212510000Y',
                    '4213220510000Y', '4213260510000Y', '4213270510000Y',
                    '4213276510000Y', '4213279510000Y', '4213280510000Y',
                    '4213217510000Y', '4213257510000Y', '4213275510000Y']
    df_c504g = df_alw.filter(pl.col('ITCODE').is_in(gid_fe_codes))
    df_c504g = df_c504g.with_columns([pl.lit('C5.04G').alias('ITEM')])
    alw_items.append(df_c504g)
    
    df_c504h = df_alw.filter(pl.col('ITCODE') == '4213280510000Y')
    df_c504h = df_c504h.with_columns([pl.lit('C5.04H').alias('ITEM')])
    alw_items.append(df_c504h)
    
    cm_codes = ['4213302520000Y', '4213303520000Y', '4213312520000Y',
                '4213320520000Y', '4213360520000Y', '4213370520000Y',
                '4213376520000Y', '4213379520000Y', '4213380520000Y',
                '4213317520000Y', '4213357520000Y', '4213375520000Y',
                '4213301520000Y']
    df_c504n = df_alw.filter(pl.col('ITCODE').is_in(cm_codes))
    df_c504n = df_c504n.with_columns([pl.lit('C5.04N').alias('ITEM')])
    alw_items.append(df_c504n)
    
    df_c504o = df_alw.filter(pl.col('ITCODE') == '4213380520000Y')
    df_c504o = df_c504o.with_columns([pl.lit('C5.04O').alias('ITEM')])
    alw_items.append(df_c504o)
    
    df_alw_all = pl.concat(alw_items) if alw_items else pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
    print(f"  ✓ ALW: {len(df_alw_all):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n2. Processing IUTFX...")
try:
    df_utfx = pl.read_parquet(f'{EQ1_DIR}IUTFX{reptmon}{wk4}{year}.parquet')
    df_utfx = df_utfx.with_columns([
        ((pl.col('MATDATE') - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    df_utfx = df_utfx.filter(pl.col('MATDATE').is_not_null())
    
    df_c501a = df_utfx.filter(
        (pl.col('BASICTYP') == 'D') &
        (pl.col('DEALTYPE').is_in(['BFI', 'BOC', 'BOI', 'BOW', 'BSW', 'BSC'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        ((pl.col('PURCHCUR') == 'MYR') | ((pl.col('PURCHCUR') != 'MYR') & (pl.col('CUSTRESC') == 'MY'))) &
        (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])) &
        (pl.col('REMMTH') <= 1)
    )
    df_c501a = df_c501a.with_columns([
        pl.lit('C5.01A').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    df_c507a = df_utfx.filter(
        (pl.col('BASICTYP') == 'D') &
        (pl.col('DEALTYPE').is_in(['BFI', 'BOC', 'BOI', 'BOW', 'BSW', 'BSC'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        (pl.col('CUSTRESC') == 'MY') &
        (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])) &
        (pl.col('REMMTH') <= 1)
    )
    df_c507a = df_c507a.with_columns([
        pl.lit('C5.07A').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    df_c504l = df_utfx.filter(
        (pl.col('DEALTYPE').is_in(['BCI', 'BCS', 'BCT', 'BCW', 'BQD'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        (pl.col('REMMTH') <= 1)
    )
    df_c504l = df_c504l.with_columns([
        pl.lit('C5.04L').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    df_c504m = df_utfx.filter(
        (pl.col('DEALTYPE').is_in(['BCI', 'BCS', 'BCT', 'BCW', 'BQD'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        (((pl.col('CUSTFISS') >= '80') & (pl.col('CUSTFISS') <= '99')) |
         (pl.col('CUSTEQTP').is_in(['EB', 'BA', 'CE', 'BE', 'GA']))) &
        (pl.col('REMMTH') <= 1)
    )
    df_c504m = df_c504m.with_columns([
        pl.lit('C5.04M').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    df_utfx_all = pl.concat([df_c501a, df_c507a, df_c504l, df_c504m])
    print(f"  ✓ IUTFX: {len(df_utfx_all):,} records")

except Exception as e:
    print(f"  ⚠ IUTFX: {e}")
    df_utfx_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n3. Processing IUTMS...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}IUTMS{reptmon}{wk4}{year}.parquet')
    df_utms = df_utms.with_columns([
        ((pl.col('MATDATE') - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    df_utms = df_utms.filter(pl.col('MATDATE').is_not_null())
    
    df_c503 = df_utms.filter(
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM']))) &
        (pl.col('REMMTH') <= 1)
    )
    df_c503 = df_c503.with_columns([
        pl.lit('C5.03').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    df_c506 = df_utms.filter(
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        (((pl.col('CUSTFISS') < '80') | (pl.col('CUSTFISS') > '99')) |
         (~pl.col('CUSTEQTP').is_in(['EB', 'BA', 'CE', 'BE', 'GA']))) &
        (pl.col('REMMTH') <= 1)
    )
    df_c506 = df_c506.with_columns([
        pl.lit('C5.06').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    df_utms_all = pl.concat([df_c503, df_c506])
    print(f"  ✓ IUTMS: {len(df_utms_all):,} records")

except Exception as e:
    print(f"  ⚠ IUTMS: {e}")
    df_utms_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n4. Adding previous ratio values...")
df_prev = pl.DataFrame({
    'ITEM': ['C5.04A', 'C5.04B', 'C5.04C', 'C5.04D', 'C5.04I',
             'C5.04J', 'C5.04K', 'C5.04P', 'C5.04Q'],
    'AMOUNT4': [sum4c103a, sum4c201a, sum4c103b, sum4c201b, sum4c103c,
                sum4c103f, sum4c201e, sum4c103h, sum4c201g]
})

print("\n5. Combining and calculating...")
df_itemc5 = pl.concat([df_alw_all, df_utfx_all, df_utms_all, df_prev])

df_itemc5_agg = df_itemc5.group_by('ITEM').agg([pl.col('AMOUNT4').sum()])

sum4c501 = df_itemc5.filter(pl.col('ITEM').str.starts_with('C5.01'))['AMOUNT4'].sum()
sum4c507 = df_itemc5.filter(pl.col('ITEM').str.starts_with('C5.07'))['AMOUNT4'].sum()

c504_plus = ['C5.04A', 'C5.04C', 'C5.04E', 'C5.04G', 'C5.04I',
             'C5.04J', 'C5.04L', 'C5.04N', 'C5.04P']
c504_minus = ['C5.04B', 'C5.04D', 'C5.04F', 'C5.04H', 'C5.04K',
              'C5.04M', 'C5.04O', 'C5.04Q']

pls4c504 = df_itemc5.filter(pl.col('ITEM').is_in(c504_plus))['AMOUNT4'].sum()
min4c504 = df_itemc5.filter(pl.col('ITEM').is_in(c504_minus))['AMOUNT4'].sum()
sum4c504 = pls4c504 - min4c504

sum4c502 = df_itemc5_agg.filter(pl.col('ITEM') == 'C5.02')['AMOUNT4'].sum() if len(df_itemc5_agg.filter(pl.col('ITEM') == 'C5.02')) > 0 else 0
sum4c503 = df_itemc5_agg.filter(pl.col('ITEM') == 'C5.03')['AMOUNT4'].sum() if len(df_itemc5_agg.filter(pl.col('ITEM') == 'C5.03')) > 0 else 0
sum4c505 = df_itemc5_agg.filter(pl.col('ITEM') == 'C5.05')['AMOUNT4'].sum() if len(df_itemc5_agg.filter(pl.col('ITEM') == 'C5.05')) > 0 else 0
sum4c506 = df_itemc5_agg.filter(pl.col('ITEM') == 'C5.06')['AMOUNT4'].sum() if len(df_itemc5_agg.filter(pl.col('ITEM') == 'C5.06')) > 0 else 0

sub5ta4 = sum4c501 + sum4c502 + sum4c503
sub5tb4 = sum4c504 + sum4c505 + sum4c506 + sum4c507
ratio5 = (sub5ta4 / sub5tb4 * 100) if sub5tb4 != 0 else 0

print(f"\n  ST IB Borrowing: RM {sum4c501:,.2f}")
print(f"  IB SBBAS: RM {sum4c502:,.2f}")
print(f"  IB INIS: RM {sum4c503:,.2f}")
print(f"  ST Deposits (net FE): RM {sum4c504:,.2f}")
print(f"  SBBAS: RM {sum4c505:,.2f}")
print(f"  INIS: RM {sum4c506:,.2f}")
print(f"  ST IB Components: RM {sum4c507:,.2f}")
print(f"  Total IB Borrowing: RM {sub5ta4:,.2f}")
print(f"  Total Funding: RM {sub5tb4:,.2f}")
print(f"  Ratio 5: {ratio5:.2f}%")

df_output = pl.DataFrame({
    'METRIC': ['ST IB Borrowing', 'IB SBBAS', 'IB INIS',
               'ST Deposits (net FE)', 'SBBAS', 'INIS', 'ST IB Components',
               'Total IB Borrowing', 'Total Funding', 'Ratio 5'],
    'AMOUNT': [sum4c501, sum4c502, sum4c503, sum4c504, sum4c505,
               sum4c506, sum4c507, sub5ta4, sub5tb4, ratio5]
})

df_output.write_csv(f'{OUTPUT_DIR}ISLAMIC_SHORT_TERM_RATIO.csv')
print(f"\n✓ Saved: ISLAMIC_SHORT_TERM_RATIO.csv")
print(f"\n{'='*60}")
print("✓ EIILQ305 Complete")
print(f"{'='*60}")
