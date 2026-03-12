"""
EIILQ304 - Islamic Bank Ratio 4: Net Overnight IB / Gross Domestic IB
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIILQ304 - Islamic Bank Overnight Interbank Ratio")
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

print("\n1. Processing IUTFX...")
try:
    df_utfx = pl.read_parquet(f'{EQ1_DIR}IUTFX{reptmon}{wk4}{year}.parquet')
    
    df_utfx = df_utfx.with_columns([pl.col('AMTPAY').alias('AMOUNT4')])
    
    df_c401a = df_utfx.filter(
        (pl.col('BASICTYP') == 'D') &
        (pl.col('DEALTYPE').is_in(['BO', 'BOI', 'BOC', 'BOW'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        ((pl.col('PURCHCUR') == 'MYR') | ((pl.col('PURCHCUR') != 'MYR') & (pl.col('CUSTRESC') == 'MY'))) &
        (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM']))
    )
    df_c401a = df_c401a.with_columns([pl.lit('C4.01A').alias('ITEM')])
    
    df_c402a = df_utfx.filter(
        (pl.col('BASICTYP') == 'L') &
        (pl.col('DEALTYPE').is_in(['LO', 'LOI', 'LOC', 'LOW'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        ((pl.col('PURCHCUR') == 'MYR') | ((pl.col('PURCHCUR') != 'MYR') & (pl.col('CUSTRESC') == 'MY'))) &
        (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM']))
    )
    df_c402a = df_c402a.with_columns([pl.lit('C4.02A').alias('ITEM')])
    
    df_utfx_all = pl.concat([df_c401a, df_c402a])
    print(f"  ✓ IUTFX: {len(df_utfx_all):,} records")

except Exception as e:
    print(f"  ⚠ IUTFX: {e}")
    df_utfx_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n2. Processing ALW...")
try:
    df_alw = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    df_alw = df_alw.filter(pl.col('AMTIND') != ' ')
    df_alw = df_alw.with_columns([pl.col('AMOUNT').alias('AMOUNT4')])
    
    alw_items = []
    
    single_mappings = {
        '4311002000000Y': ['C4.01B', 'C4.03E'],
        '4311003000000Y': ['C4.01C', 'C4.03F'],
        '4312002000000Y': ['C4.01D', 'C4.03G'],
        '4312003000000Y': ['C4.01E', 'C4.03H'],
        '4369902000000Y': ['C4.01F', 'C4.03N'],
        '3311002000000Y': ['C4.02B'],
        '3311003000000Y': ['C4.02C'],
        '3312002000000Y': ['C4.02D'],
        '3312003000000Y': ['C4.02E'],
        '3369002000000F': ['C4.02F'],
        '4314001000000Y': ['C4.03A'],
        '4314002000000Y': ['C4.03B'],
        '4314003000000Y': ['C4.03C'],
        '4314012000000Y': ['C4.03D'],
        '4364001000000Y': ['C4.03I'],
        '4364002000000Y': ['C4.03J'],
        '4364003000000Y': ['C4.03K'],
        '4364012000000Y': ['C4.03L'],
    }
    
    for itcode, items in single_mappings.items():
        df_filtered = df_alw.filter(pl.col('ITCODE') == itcode)
        for item in items:
            df_temp = df_filtered.with_columns([pl.lit(item).alias('ITEM')])
            alw_items.append(df_temp)
    
    df_c403m = df_alw.filter(pl.col('ITCODE').is_in(['4364007100000Y', '4364007200000Y']))
    df_c403m = df_c403m.with_columns([pl.lit('C4.03M').alias('ITEM')])
    alw_items.append(df_c403m)
    
    df_alw_all = pl.concat(alw_items) if alw_items else pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
    print(f"  ✓ ALW: {len(df_alw_all):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

print("\n3. Processing IUTMS...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}IUTMS{reptmon}{wk4}{year}.parquet')
    
    df_utms = df_utms.filter(
        (pl.col('PORTREF') == 'PDC') &
        (pl.col('SECTYPE').is_in(['IDC', 'IDP'])) &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])))
    )
    
    df_utms = df_utms.with_columns([
        pl.lit('C4.05A').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    print(f"  ✓ IUTMS: {len(df_utms):,} records")

except Exception as e:
    print(f"  ⚠ IUTMS: {e}")
    df_utms = pl.DataFrame({'ITEM': ['C4.05A'], 'AMOUNT4': [0.0]})

print("\n4. Combining and calculating...")
df_itemc4 = pl.concat([df_utfx_all, df_alw_all, df_utms])

df_itemc4_agg = df_itemc4.group_by('ITEM').agg([pl.col('AMOUNT4').sum()])

sum4c401 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.01'))['AMOUNT4'].sum()
sum4c402 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.02'))['AMOUNT4'].sum()
sum4c403 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.03'))['AMOUNT4'].sum()
sum4c405 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.05'))['AMOUNT4'].sum()

df_groups = pl.DataFrame({
    'ITEM': ['C4.01', 'C4.02', 'C4.03', 'C4.05'],
    'AMOUNT4': [sum4c401, sum4c402, sum4c403, sum4c405]
})

df_itemc4_agg = pl.concat([df_itemc4_agg, df_groups])

sum4c404 = df_itemc4_agg.filter(pl.col('ITEM') == 'C4.04')['AMOUNT4'][0] if len(df_itemc4_agg.filter(pl.col('ITEM') == 'C4.04')) > 0 else 0
sum4c406 = df_itemc4_agg.filter(pl.col('ITEM') == 'C4.06')['AMOUNT4'][0] if len(df_itemc4_agg.filter(pl.col('ITEM') == 'C4.06')) > 0 else 0

sub4ta4 = sum4c401 - sum4c402
sub4tb4 = sum4c403 + sum4c404 + sum4c405
g4tot4 = sub4tb4 - sum4c402
ratio4 = (sub4ta4 / g4tot4 * 100) if g4tot4 != 0 else 0

print(f"\n  Overnight IB Borrowing: RM {sum4c401:,.2f}")
print(f"  Overnight IB Placement: RM {sum4c402:,.2f}")
print(f"  Net Overnight: RM {sub4ta4:,.2f}")
print(f"  Gross Domestic IB: RM {sub4tb4:,.2f}")
print(f"  Total (Gross - Overnight Placement): RM {g4tot4:,.2f}")
print(f"  Ratio 4: {ratio4:.2f}%")

df_output = pl.DataFrame({
    'METRIC': ['Overnight IB Borrowing', 'Overnight IB Placement', 'Net Overnight',
               'Total IB Borrowing', 'IB SBBAS', 'IB INIS',
               'Gross Domestic IB', 'Total (Gross - ON Placement)', 'Ratio 4'],
    'AMOUNT': [sum4c401, sum4c402, sub4ta4, sum4c403, sum4c404, sum4c405,
               sub4tb4, g4tot4, ratio4]
})

df_output.write_csv(f'{OUTPUT_DIR}ISLAMIC_OVERNIGHT_RATIO.csv')
print(f"\n✓ Saved: ISLAMIC_OVERNIGHT_RATIO.csv")
print(f"\n{'='*60}")
print("✓ EIILQ304 Complete")
print(f"{'='*60}")
