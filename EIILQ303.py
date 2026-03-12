"""
EIILQ303 - Islamic Bank Ratio 3: Net Domestic IB / Domestic Deposits
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIILQ303 - Islamic Bank Domestic Interbank Ratio")
print("=" * 60)

try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    year = reptyear
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    sum4c209 = 0.0
    sum4c210 = 0.0
    sum4c211 = 0.0
    
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
        pl.when(pl.col('ITCODE') == '4314001000000Y').then(pl.lit('C3.01A'))
        .when(pl.col('ITCODE') == '4314002000000Y').then(pl.lit('C3.01B'))
        .when(pl.col('ITCODE') == '4314003000000Y').then(pl.lit('C3.01C'))
        .when(pl.col('ITCODE') == '4314012000000Y').then(pl.lit('C3.01D'))
        .when(pl.col('ITCODE') == '4311002000000Y').then(pl.lit('C3.01E'))
        .when(pl.col('ITCODE') == '4311003000000Y').then(pl.lit('C3.01F'))
        .when(pl.col('ITCODE') == '4312002000000Y').then(pl.lit('C3.01G'))
        .when(pl.col('ITCODE') == '4312003000000Y').then(pl.lit('C3.01H'))
        .when(pl.col('ITCODE') == '4364001000000Y').then(pl.lit('C3.01I'))
        .when(pl.col('ITCODE') == '4364002000000Y').then(pl.lit('C3.01J'))
        .when(pl.col('ITCODE') == '4364003000000Y').then(pl.lit('C3.01K'))
        .when(pl.col('ITCODE') == '4364012000000Y').then(pl.lit('C3.01L'))
        .when(pl.col('ITCODE').is_in(['4364007100000Y', '4364007200000Y'])).then(pl.lit('C3.01M'))
        .when(pl.col('ITCODE') == '4369902000000Y').then(pl.lit('C3.01N'))
        .when(pl.col('ITCODE') == '3314001000000Y').then(pl.lit('C3.04A'))
        .when(pl.col('ITCODE') == '3314002000000Y').then(pl.lit('C3.04B'))
        .when(pl.col('ITCODE') == '3314003000000Y').then(pl.lit('C3.04C'))
        .when(pl.col('ITCODE') == '3314012000000Y').then(pl.lit('C3.04D'))
        .when(pl.col('ITCODE') == '3311002000000Y').then(pl.lit('C3.04E'))
        .when(pl.col('ITCODE') == '3311003000000Y').then(pl.lit('C3.04F'))
        .when(pl.col('ITCODE') == '3312002000000Y').then(pl.lit('C3.04G'))
        .when(pl.col('ITCODE') == '3312003000000Y').then(pl.lit('C3.04H'))
        .when(pl.col('ITCODE') == '3364001000000Y').then(pl.lit('C3.04I'))
        .when(pl.col('ITCODE') == '3364002000000Y').then(pl.lit('C3.04J'))
        .when(pl.col('ITCODE') == '3364003000000Y').then(pl.lit('C3.04K'))
        .when(pl.col('ITCODE') == '3364012000000Y').then(pl.lit('C3.04L'))
        .when(pl.col('ITCODE').is_in(['3364007100000F', '3364007200000F'])).then(pl.lit('C3.04M'))
        .when(pl.col('ITCODE') == '3369002000000F').then(pl.lit('C3.04N'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_alw = df_alw.filter(pl.col('ITEM').is_not_null())
    print(f"  ✓ ALW: {len(df_alw):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n2. Processing W1AL...")
try:
    df_w1al = pl.read_parquet(f'{BNM1_DIR}W1AL{reptmon}{wk4}.parquet')
    df_w1al = df_w1al.filter(pl.col('SET_ID') == 'F137030NIDH')
    df_w1al = df_w1al.with_columns([
        pl.lit('C3.06A').alias('ITEM'),
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    print(f"  ✓ W1AL: {len(df_w1al):,} records")

except Exception as e:
    print(f"  ⚠ W1AL: {e}")
    df_w1al = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n3. Processing IUTMS...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}IUTMS{reptmon}{wk4}{year}.parquet')
    
    df_c303 = df_utms.filter(
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])))
    )
    df_c303 = df_c303.with_columns([
        pl.lit('C3.03').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    df_nids = df_utms.filter(
        pl.col('SECTYPE').is_in(['SDC', 'SLD', 'SSD', 'SZD', 'LDC'])
    )
    
    df_nids = df_nids.with_columns([
        (pl.col('CAMTOWNE') + pl.col('INTACR') - pl.col('UNDISCPR') - pl.col('DISCPRMN')).alias('AMOUNT4')
    ])
    
    df_nids = df_nids.with_columns([
        pl.when(pl.col('CUSTFISS') == '17')
          .then(pl.lit('C3.06B'))
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['BKPKSM', 'ISME']))
          .then(pl.lit('C3.06C'))
        .when((pl.col('CUSTFISS') == '33') & (pl.col('CUSTNO') == 'BKRAK'))
          .then(pl.lit('C3.06D'))
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['IBSN', 'BSN']))
          .then(pl.lit('C3.06E'))
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['BPMB', 'IBPMB']))
          .then(pl.lit('C3.06F'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_nids = df_nids.filter(pl.col('ITEM').is_not_null())
    df_utms_all = pl.concat([df_c303, df_nids])
    
    print(f"  ✓ IUTMS: {len(df_utms_all):,} records")

except Exception as e:
    print(f"  ⚠ IUTMS: {e}")
    df_utms_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n4. Combining and calculating...")
df_itemc3 = pl.concat([df_alw, df_w1al, df_utms_all])

df_itemc3_agg = df_itemc3.group_by('ITEM').agg([pl.col('AMOUNT4').sum()])

sum4c301 = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.01'))['AMOUNT4'].sum()
sum4c304 = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.04'))['AMOUNT4'].sum()

c306_items = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.06'))
sum4c306a = c306_items.filter(pl.col('ITEM') == 'C3.06A')['AMOUNT4'].sum()
sum4c306b = c306_items.filter(pl.col('ITEM') != 'C3.06A')['AMOUNT4'].sum()
sum4c306 = sum4c306a - sum4c306b

df_groups = pl.DataFrame({
    'ITEM': ['C3.01', 'C3.04', 'C3.06'],
    'AMOUNT4': [sum4c301, sum4c304, sum4c306]
})

df_itemc3_agg = pl.concat([df_itemc3_agg, df_groups])

sum4c302 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.02')['AMOUNT4'][0] if len(df_itemc3_agg.filter(pl.col('ITEM') == 'C3.02')) > 0 else 0
sum4c303 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.03')['AMOUNT4'][0] if len(df_itemc3_agg.filter(pl.col('ITEM') == 'C3.03')) > 0 else 0
sum4c305 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.05')['AMOUNT4'][0] if len(df_itemc3_agg.filter(pl.col('ITEM') == 'C3.05')) > 0 else 0

sum4c307 = sum4c209
sum4c308 = sum4c210
sum4c309 = sum4c211

sub3ta4 = sum4c301 + sum4c302 + sum4c303
sub3tb4 = sum4c304 + sum4c305 + sum4c306
sub3tc4 = sum4c307 + sum4c308 + sum4c309
g3tot4 = sub3ta4 - sub3tb4
ratio3 = (g3tot4 / sub3tc4 * 100) if sub3tc4 != 0 else 0

print(f"\n  Interbank Borrowing: RM {sub3ta4:,.2f}")
print(f"  Interbank Placements: RM {sub3tb4:,.2f}")
print(f"  Net Domestic Interbank: RM {g3tot4:,.2f}")
print(f"  Domestic Deposits: RM {sub3tc4:,.2f}")
print(f"  Ratio 3: {ratio3:.2f}%")

df_output = pl.DataFrame({
    'METRIC': ['IB Borrowing', 'IB SBBAS', 'IB INIS',
               'IB Placements', 'Reverse SBBAS', 'INIS Held (Net)',
               'Total Borrowing', 'Total Placements',
               'Net Domestic IB', 'Domestic Deposits', 'Ratio 3'],
    'AMOUNT': [sum4c301, sum4c302, sum4c303, sum4c304, sum4c305,
               sum4c306, sub3ta4, sub3tb4, g3tot4, sub3tc4, ratio3]
})

df_output.write_csv(f'{OUTPUT_DIR}ISLAMIC_INTERBANK_RATIO.csv')
print(f"\n✓ Saved: ISLAMIC_INTERBANK_RATIO.csv")
print(f"\n{'='*60}")
print("✓ EIILQ303 Complete")
print(f"{'='*60}")
