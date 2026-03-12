"""
EIILQ302 - Islamic Bank Ratio 2: Net Offshore Borrowing / Domestic Deposits
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIILQ302 - Islamic Bank Offshore Ratio")
print("=" * 60)

try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    year = reptyear
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    sum4c103 = 0.0
    sum4c105 = 0.0
    sum4c210 = 0.0
    
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
    
    df_alw = df_alw.with_columns([
        pl.when(pl.col('ITCODE') == '4211085000000Y').then(pl.lit('C2.01A'))
        .when(pl.col('ITCODE') == '4212085000000Y').then(pl.lit('C2.01B'))
        .when(pl.col('ITCODE').is_in(['4213281000000Y', '4213285000000Y'])).then(pl.lit('C2.01C'))
        .when(pl.col('ITCODE').is_in(['4219981000000Y', '4219985000000Y'])).then(pl.lit('C2.01E'))
        .when(pl.col('ITCODE').is_in(['4213381000000Y', '4213385000000Y'])).then(pl.lit('C2.01F'))
        .when(pl.col('ITCODE') == '4261085000000Y').then(pl.lit('C2.01G'))
        .when(pl.col('ITCODE').is_in(['4314081100000Y', '4314081200000Y'])).then(pl.lit('C2.04A'))
        .when(pl.col('ITCODE') == '4311081000000Y').then(pl.lit('C2.04B'))
        .when(pl.col('ITCODE') == '4319981000000Y').then(pl.lit('C2.04C'))
        .when(pl.col('ITCODE').is_in(['4364081100000Y', '4364081200000Y'])).then(pl.lit('C2.04D'))
        .when(pl.col('ITCODE') == '4362081000000Y').then(pl.lit('C2.04E'))
        .when(pl.col('ITCODE') == '4369981000000Y').then(pl.lit('C2.04F'))
        .when(pl.col('ITCODE').is_in(['3314081100000Y', '3314081200000Y'])).then(pl.lit('C2.08A'))
        .when(pl.col('ITCODE') == '3311081000000Y').then(pl.lit('C2.08B'))
        .when(pl.col('ITCODE').is_in(['3361408110000Y', '3364081200000Y'])).then(pl.lit('C2.08C'))
        .when(pl.col('ITCODE') == '3362081000000Y').then(pl.lit('C2.08D'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_alw = df_alw.filter(pl.col('ITEM').is_not_null())
    print(f"  ✓ ALW: {len(df_alw):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n2. Processing IUTFX...")
try:
    df_utfx = pl.read_parquet(f'{EQ1_DIR}IUTFX{reptmon}{wk4}{year}.parquet')
    
    df_utfx = df_utfx.filter(
        (pl.col('DEALTYPE').is_in(['BCI', 'BCS', 'BCT', 'BCW', 'BQD'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        (((pl.col('CUSTFISS') >= '80') & (pl.col('CUSTFISS') <= '99')) |
         (pl.col('CUSTEQTP').is_in(['EB', 'BA', 'CE', 'BE', 'GA'])))
    )
    
    df_utfx = df_utfx.with_columns([
        pl.lit('C2.01D').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    print(f"  ✓ IUTFX: {len(df_utfx):,} records")

except Exception as e:
    print(f"  ⚠ IUTFX: {e}")
    df_utfx = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n3. Processing IUTMS...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}IUTMS{reptmon}{wk4}{year}.parquet')
    
    df_utms = df_utms.filter(
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('VALUDATE') <= pl.col('BUSDATE'))
    )
    
    df_utms = df_utms.with_columns([
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    df_c202a = df_utms.filter(
        ((pl.col('CUSTFISS') >= '81') & (pl.col('CUSTFISS') <= '84')) |
        (pl.col('CUSTEQTP').is_in(['BA', 'BE']))
    )
    df_c202a = df_c202a.with_columns([pl.lit('C2.02A').alias('ITEM')])
    
    df_c202b = df_utms.filter(
        ((pl.col('CUSTFISS') >= '85') & (pl.col('CUSTFISS') <= '99')) |
        (pl.col('CUSTEQTP').is_in(['CE', 'EB', 'GA']))
    )
    df_c202b = df_c202b.with_columns([pl.lit('C2.02B').alias('ITEM')])
    
    df_utms_all = pl.concat([df_c202a, df_c202b])
    print(f"  ✓ IUTMS: {len(df_utms_all):,} records")

except Exception as e:
    print(f"  ⚠ IUTMS: {e}")
    df_utms_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n4. Combining and calculating...")
df_itemc2 = pl.concat([df_alw, df_utfx, df_utms_all])

df_itemc2_agg = df_itemc2.group_by('ITEM').agg([pl.col('AMOUNT4').sum()])

sum4c201 = df_itemc2.filter(pl.col('ITEM').str.starts_with('C2.01'))['AMOUNT4'].sum()
sum4c202 = df_itemc2.filter(pl.col('ITEM').str.starts_with('C2.02'))['AMOUNT4'].sum()
sum4c204 = df_itemc2.filter(pl.col('ITEM').str.starts_with('C2.04'))['AMOUNT4'].sum()
sum4c208 = df_itemc2.filter(pl.col('ITEM').str.starts_with('C2.08'))['AMOUNT4'].sum()

df_groups = pl.DataFrame({
    'ITEM': ['C2.01', 'C2.02', 'C2.04', 'C2.08'],
    'AMOUNT4': [sum4c201, sum4c202, sum4c204, sum4c208]
})

df_itemc2_agg = pl.concat([df_itemc2_agg, df_groups])

sum4c202b = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.02B')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.02B')) > 0 else 0
sum4c203 = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.03')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.03')) > 0 else 0
sum4c205 = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.05')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.05')) > 0 else 0
sum4c206 = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.06')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.06')) > 0 else 0
sum4c207 = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.07')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.07')) > 0 else 0

sum4c209 = sum4c103 - sum4c201
sum4c211 = sum4c105 - sum4c202b

sub2ta4 = sum4c201 + sum4c202 + sum4c203 + sum4c204
sub2tb4 = sum4c205 + sum4c206 + sum4c207 + sum4c208
sub2tc4 = sum4c209 + sum4c210 + sum4c211
g2tot4 = sub2ta4 - sub2tb4
ratio2 = (g2tot4 / sub2tc4 * 100) if sub2tc4 != 0 else 0

print(f"\n  Offshore Borrowing: RM {sub2ta4:,.2f}")
print(f"  Offshore Placements: RM {sub2tb4:,.2f}")
print(f"  Net Offshore: RM {g2tot4:,.2f}")
print(f"  Domestic Deposits: RM {sub2tc4:,.2f}")
print(f"  Ratio 2: {ratio2:.2f}%")

df_output = pl.DataFrame({
    'METRIC': ['All Deposits from FE', 'INIS to FE', 'SBBAS with FE',
               'IB Borrowing from FBI', 'Offshore Borrowing',
               'Deposits with FE', 'INIS with FE', 'Reverse SBBAS',
               'IB Placement with FBI', 'Offshore Placements',
               'Net Offshore', 'Domestic Deposits', 'Ratio 2'],
    'AMOUNT': [sum4c201, sum4c202, sum4c203, sum4c204, sub2ta4,
               sum4c205, sum4c206, sum4c207, sum4c208, sub2tb4,
               g2tot4, sub2tc4, ratio2]
})

df_output.write_csv(f'{OUTPUT_DIR}ISLAMIC_OFFSHORE_RATIO.csv')
print(f"\n✓ Saved: ISLAMIC_OFFSHORE_RATIO.csv")
print(f"\n{'='*60}")
print("✓ EIILQ302 Complete")
print(f"{'='*60}")
