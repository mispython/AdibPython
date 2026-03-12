"""
EIILIQ301 - Islamic Bank Ratio 1: Adjusted Financing / Adjusted Deposits
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIILIQ301 - Islamic Bank Funding Concentration Ratio")
print("=" * 60)

try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    year = reptyear
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

layout_data = [
    ('C1.01', 1), ('C1.02', 2), ('C1.03', 3), ('C1.03A', 4),
    ('C1.03B', 5), ('C1.03C', 6), ('C1.03D', 7), ('C1.03E', 8),
    ('C1.03F', 9), ('C1.03G', 10), ('C1.03H', 11), ('C1.04', 12),
    ('C1.05', 13), ('C1.05A', 14), ('C1.05B', 15), ('C1.06', 16),
    ('C1.07', 17), ('C1.08', 18), ('C1.09', 19), ('C1.10', 20)
]

df_layout = pl.DataFrame({
    'ITEM': [x[0] for x in layout_data],
    'ITEM2': [x[1] for x in layout_data]
})

print("\n1. Processing W1AL...")
try:
    df_w1al = pl.read_parquet(f'{BNM1_DIR}W1AL{reptmon}{wk4}.parquet')
    
    df_w1al = df_w1al.with_columns([pl.col('AMOUNT').alias('AMOUNT4')])
    
    set_id_mapping = {
        'F130510LN': 'C1.01',
        'F142110DDEP': 'C1.03A',
        'F142120SA': 'C1.03B',
        'F142180HDAD': 'C1.03C',
        'F142132GID': 'C1.03D',
        'F142190STD': 'C1.03E',
        'F142199ODA': 'C1.03F',
        'F142133CM': 'C1.03G',
        'F142610': 'C1.03H',
        'F142150NIDI': 'C1.05A',
        'F132110SDBNM': 'C1.06',
        'F144111TIER2': 'C1.08',
        'F154121CAGA': 'C1.10'
    }
    
    capital_ids = ['F241101OSC', 'F241102SP', 'F241103SRF', 'F241104GRF',
                   'F241105RPAL', 'F241107CUPL', 'F241109PRR', 'F241400']
    
    df_w1al = df_w1al.with_columns([
        pl.when(pl.col('SET_ID').is_in(list(set_id_mapping.keys())))
          .then(pl.col('SET_ID').replace(set_id_mapping))
        .when(pl.col('SET_ID').is_in(capital_ids))
          .then(pl.lit('C1.07'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_w1al = df_w1al.filter(pl.col('ITEM').is_not_null())
    
    df_w1al = df_w1al.with_columns([
        pl.when(pl.col('ITEM') == 'C1.10')
          .then(pl.col('AMOUNT4') * -1)
          .otherwise(pl.col('AMOUNT4'))
          .alias('AMOUNT4')
    ])
    
    print(f"  ✓ W1AL: {len(df_w1al):,} records")

except Exception as e:
    print(f"  ⚠ W1AL: {e}")
    df_w1al = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n2. Processing IUTMS...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}IUTMS{reptmon}{wk4}{year}.parquet')
    
    df_utms = df_utms.filter(
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12', '81', '82', '83', '84'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM', 'BA', 'BE'])))
    )
    
    df_utms = df_utms.with_columns([
        pl.lit('C1.05B').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    print(f"  ✓ IUTMS: {len(df_utms):,} records")

except Exception as e:
    print(f"  ⚠ IUTMS: {e}")
    df_utms = pl.DataFrame({'ITEM': ['C1.05B'], 'AMOUNT4': [0.0]})

print("\n3. Combining and calculating...")
df_itemc1 = pl.concat([df_w1al, df_utms])

df_itemc1_agg = df_itemc1.group_by('ITEM').agg([pl.col('AMOUNT4').sum()])

sum4c103 = df_itemc1.filter(pl.col('ITEM').str.starts_with('C1.03'))['AMOUNT4'].sum()

df_c103_group = pl.DataFrame({'ITEM': ['C1.03'], 'AMOUNT4': [sum4c103]})
df_itemc1_agg = pl.concat([df_itemc1_agg, df_c103_group])

df_final = df_layout.join(df_itemc1_agg, on='ITEM', how='left')
df_final = df_final.with_columns([pl.col('AMOUNT4').fill_null(0)])

sum4c101 = df_final.filter(pl.col('ITEM') == 'C1.01')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.01')) > 0 else 0
sum4c102 = df_final.filter(pl.col('ITEM') == 'C1.02')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.02')) > 0 else 0
sum4c103 = df_final.filter(pl.col('ITEM') == 'C1.03')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.03')) > 0 else 0
sum4c104 = df_final.filter(pl.col('ITEM') == 'C1.04')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.04')) > 0 else 0
sum4c105a = df_final.filter(pl.col('ITEM') == 'C1.05A')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.05A')) > 0 else 0
sum4c105b = df_final.filter(pl.col('ITEM') == 'C1.05B')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.05B')) > 0 else 0
sum4c106 = df_final.filter(pl.col('ITEM') == 'C1.06')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.06')) > 0 else 0
sum4c107 = df_final.filter(pl.col('ITEM') == 'C1.07')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.07')) > 0 else 0
sum4c108 = df_final.filter(pl.col('ITEM') == 'C1.08')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.08')) > 0 else 0
sum4c109 = df_final.filter(pl.col('ITEM') == 'C1.09')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.09')) > 0 else 0
sum4c110 = df_final.filter(pl.col('ITEM') == 'C1.10')['AMOUNT4'][0] if len(df_final.filter(pl.col('ITEM') == 'C1.10')) > 0 else 0

subta4 = sum4c101 - sum4c102
sum4c105 = sum4c105a - sum4c105b
subtb4 = sum4c103 + sum4c104 + sum4c105 + sum4c107 + sum4c108 + sum4c109 + sum4c110 - sum4c106
ratio = (subta4 / subtb4 * 100) if subtb4 != 0 else 0

print(f"\n  Adjusted Financing: RM {subta4:,.2f}")
print(f"  Adjusted Deposits: RM {subtb4:,.2f}")
print(f"  Ratio 1: {ratio:.2f}%")

df_output = pl.DataFrame({
    'METRIC': ['Gross Financing', 'BA Payable', 'Adjusted Financing',
               'All Deposits', 'SBBAS', 'INIS (Net)', 'Capital', 'Tier-2',
               'Subordinated', 'Cagamas', 'Statutory Deposits',
               'Adjusted Deposits', 'Ratio 1'],
    'AMOUNT': [sum4c101, sum4c102, subta4, sum4c103, sum4c104, sum4c105,
               sum4c107, sum4c108, sum4c109, sum4c110, sum4c106,
               subtb4, ratio]
})

df_output.write_csv(f'{OUTPUT_DIR}ISLAMIC_FUNDING_RATIO.csv')
print(f"\n✓ Saved: ISLAMIC_FUNDING_RATIO.csv")
print(f"\n{'='*60}")
print("✓ EIILIQ301 Complete")
print(f"{'='*60}")
