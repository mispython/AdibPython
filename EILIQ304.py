"""
EILIQ304 - Concentration of Funding Sources (PBB) - Ratio 4

Purpose: Calculate Net Overnight Interbank Borrowing / Gross Domestic Interbank
- Part 3 of liquidity monitoring (Ratio 4)
- BNM regulatory reporting
- Overnight liquidity concentration

Formula:
  Ratio 4 = (Net Overnight / Gross Domestic Interbank) × 100
  Net Overnight = Overnight Borrowing - Overnight Placement
  Gross Domestic = Total IB Borrowing + IB Repos + IB NIDS - Overnight Placement
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

# Directories
BNMK_DIR = 'data/bnmk/'
BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EILIQ304 - Net Overnight Interbank Ratio")
print("=" * 60)

# Get report parameters
try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Initialize layout
layout_data = [
    ('C4.01', 1), ('C4.01A', 2), ('C4.01B', 3), ('C4.01C', 4),
    ('C4.01D', 5), ('C4.01E', 6), ('C4.01F', 7), ('C4.02', 8),
    ('C4.02A', 9), ('C4.02B', 10), ('C4.02C', 11), ('C4.02D', 12),
    ('C4.02E', 13), ('C4.02F', 14), ('C4.03', 15), ('C4.03A', 16),
    ('C4.03B', 17), ('C4.03C', 18), ('C4.03D', 19), ('C4.03E', 20),
    ('C4.03F', 21), ('C4.03G', 22), ('C4.03H', 23), ('C4.03I', 24),
    ('C4.03J', 25), ('C4.03K', 26), ('C4.03L', 27), ('C4.03M', 28),
    ('C4.03N', 29), ('C4.04', 30), ('C4.05', 31), ('C4.06', 32)
]

df_layout = pl.DataFrame({
    'ITEM': [x[0] for x in layout_data],
    'ITEM2': [x[1] for x in layout_data]
})

print("\n1. Processing K1TBL (Overnight items)...")
try:
    df_k1tbl = pl.read_parquet(f'{BNMK_DIR}K1TBL{reptmon}{wk4}.parquet')
    
    # C4.01A: Overnight Interbank Borrowing from Residents
    df_c401a = df_k1tbl.filter(
        (pl.col('GWDLP') == 'BO') &
        ((pl.col('GWCCY') == 'MYR') | ((pl.col('GWCCY') != 'MYR') & (pl.col('GWCNAL') == 'MY'))) &
        (pl.col('GWCTP').is_in(['BB', 'BC', 'BI', 'BJ', 'BM'])) &
        (pl.col('GWMVT') == 'P') &
        (pl.col('GWMVTS') == 'M') &
        (~pl.col('GWSHN').str.slice(0, 6).eq('FCY-FD'))
    )
    
    df_c401a = df_c401a.with_columns([
        pl.lit('C4.01A').alias('ITEM'),
        pl.col('GWBALC').alias('AMOUNT4')
    ])
    
    # C4.02A: Overnight Interbank Placement to Residents
    df_c402a = df_k1tbl.filter(
        ((pl.col('GWCCY') == 'MYR') | ((pl.col('GWCCY') != 'MYR') & (pl.col('GWCNAL') == 'MY'))) &
        (pl.col('GWMVT') == 'P') &
        (pl.col('GWMVTS') == 'M') &
        (pl.col('GWDLP') == 'LO') &
        (pl.col('GWCTP').str.slice(0, 1) == 'B') &
        (pl.col('GWCTP').is_in(['BB', 'BC', 'BI', 'BJ', 'BM']))
    )
    
    df_c402a = df_c402a.with_columns([
        pl.lit('C4.02A').alias('ITEM'),
        pl.when(pl.col('GWBALC') < 0)
          .then(pl.col('GWBALC') * -1)
          .otherwise(pl.col('GWBALC'))
          .alias('AMOUNT4')
    ])
    
    df_k1tbl_all = pl.concat([df_c401a, df_c402a])
    
    print(f"  ✓ K1TBL: {len(df_k1tbl_all):,} records")

except Exception as e:
    print(f"  ⚠ K1TBL: {e}")
    df_k1tbl_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

# Process ALW (Multiple items)
print("\n2. Processing ALW (Interbank items)...")
try:
    df_alw = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    
    df_alw = df_alw.filter(pl.col('AMTIND') != ' ')
    
    df_alw = df_alw.with_columns([
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    # Map ITCODE to ITEM (with multiple outputs for some codes)
    alw_items = []
    
    # Create individual DataFrames for each ITCODE mapping
    itcode_mappings = {
        '4311002000000Y': ['C4.01B', 'C4.03E'],
        '4311003000000Y': ['C4.01C', 'C4.03F'],
        '4312002000000Y': ['C4.01D', 'C4.03G'],
        '4312003000000Y': ['C4.01E', 'C4.03H'],
        '3311002000000Y': ['C4.02B'],
        '3311003000000Y': ['C4.02C'],
        '3312002000000Y': ['C4.02D'],
        '3312003000000Y': ['C4.02E'],
        '4314001000000Y': ['C4.03A'],
        '4314002000000Y': ['C4.03B'],
        '4314003000000Y': ['C4.03C'],
        '4314012000000Y': ['C4.03D'],
        '4364001000000Y': ['C4.03I'],
        '4364002000000Y': ['C4.03J'],
        '4364003000000Y': ['C4.03K'],
        '4364012000000Y': ['C4.03L'],
        '4369903000000Y': ['C4.01F', 'C4.03N'],
        '3369003000000F': ['C4.02F'],
    }
    
    # Handle compound ITCODEs
    for itcode, items in itcode_mappings.items():
        df_filtered = df_alw.filter(pl.col('ITCODE') == itcode)
        for item in items:
            df_temp = df_filtered.with_columns([pl.lit(item).alias('ITEM')])
            alw_items.append(df_temp)
    
    # Handle C4.03M (multiple ITCODEs)
    df_c403m = df_alw.filter(
        pl.col('ITCODE').is_in(['4364007100000Y', '4364007200000Y'])
    )
    df_c403m = df_c403m.with_columns([pl.lit('C4.03M').alias('ITEM')])
    alw_items.append(df_c403m)
    
    # Handle C4.04 (multiple ITCODEs)
    df_c404 = df_alw.filter(
        pl.col('ITCODE').is_in(['4216001000000Y', '4216002000000Y', '4216003000000Y',
                                '4216007000000Y', '4216012000000Y'])
    )
    df_c404 = df_c404.with_columns([pl.lit('C4.04').alias('ITEM')])
    alw_items.append(df_c404)
    
    df_alw_all = pl.concat(alw_items) if alw_items else pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
    
    print(f"  ✓ ALW: {len(df_alw_all):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

# Process UTMS (C4.05 - Interbank NIDS)
print("\n3. Processing UTMS (C4.05)...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}UTMS{reptmon}{wk4}{reptyear}.parquet')
    
    df_utms = df_utms.filter(
        (~pl.col('DEALTYPE').is_in(['MOP', 'MSP', 'IUP'])) &
        (pl.col('SECTYPE') != 'DIM') &
        (pl.col('PORTREF') != 'SIPPFD') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])))
    )
    
    df_utms = df_utms.with_columns([
        pl.lit('C4.05').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    print(f"  ✓ UTMS: {len(df_utms):,} records")

except Exception as e:
    print(f"  ⚠ UTMS: {e}")
    df_utms = pl.DataFrame({'ITEM': ['C4.05'], 'AMOUNT4': [0.0]})

# Combine all items
print("\n4. Combining all items...")
df_itemc4 = pl.concat([df_k1tbl_all, df_alw_all, df_utms])

# Aggregate by ITEM
df_itemc4_agg = df_itemc4.group_by('ITEM').agg([
    pl.col('AMOUNT4').sum()
])

# Calculate group totals
sum4c401 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.01'))['AMOUNT4'].sum()
sum4c402 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.02'))['AMOUNT4'].sum()
sum4c403 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.03'))['AMOUNT4'].sum()
sum4c404 = df_itemc4_agg.filter(pl.col('ITEM') == 'C4.04')['AMOUNT4'].sum() if len(df_itemc4_agg.filter(pl.col('ITEM') == 'C4.04')) > 0 else 0
sum4c405 = df_itemc4.filter(pl.col('ITEM').str.starts_with('C4.05'))['AMOUNT4'].sum()

print(f"\n  Group Totals:")
print(f"    C4.01 (Overnight IB Borrowing): RM {sum4c401:,.2f}")
print(f"    C4.02 (Overnight IB Placement): RM {sum4c402:,.2f}")
print(f"    C4.03 (Total IB Borrowing): RM {sum4c403:,.2f}")
print(f"    C4.04 (Interbank Repos): RM {sum4c404:,.2f}")
print(f"    C4.05 (Interbank NIDS): RM {sum4c405:,.2f}")

# Calculate final values
subta4 = sum4c401 - sum4c402  # Net Overnight
subtb4 = sum4c403 + sum4c404 + sum4c405  # Gross Domestic Interbank Borrowing
g4tot4 = subtb4 - sum4c402  # Gross Domestic IB - Overnight Placement

# Calculate ratio
ratio4 = (subta4 / g4tot4 * 100) if g4tot4 != 0 else 0

print(f"\n  Calculations:")
print(f"    Net Overnight (C4.02G): RM {subta4:,.2f}")
print(f"    Gross Domestic IB (C4.05A): RM {subtb4:,.2f}")
print(f"    Total (C4.06A): RM {g4tot4:,.2f}")
print(f"    Ratio 4: {ratio4:.2f}%")

# Merge with layout
df_final = df_layout.join(df_itemc4_agg, on='ITEM', how='left')
df_final = df_final.with_columns([
    pl.col('AMOUNT4').fill_null(0)
])

# Add group totals
df_groups = pl.DataFrame({
    'ITEM': ['C4.01', 'C4.02', 'C4.03', 'C4.05'],
    'SUM4': [sum4c401, sum4c402, sum4c403, sum4c405]
})

df_final = df_final.join(df_groups, on='ITEM', how='left')

# Update C4.06 with C4.02 value
df_final = df_final.with_columns([
    pl.when(pl.col('ITEM') == 'C4.06')
      .then(sum4c402)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

# Prepare final output
df_output = pl.DataFrame({
    'METRIC': ['Net Overnight Borrowing', 'Gross Domestic IB Borrowing',
               'Total (Gross - Overnight Placement)', 'Ratio 4'],
    'AMOUNT': [subta4, subtb4, g4tot4, ratio4]
})

df_output.write_csv(f'{OUTPUT_DIR}OVERNIGHT_RATIO.csv')
print(f"  ✓ Saved: OVERNIGHT_RATIO.csv")

# Save detailed breakdown
df_breakdown = pl.DataFrame({
    'COMPONENT': [
        'C4.01 - Overnight IB Borrowing',
        'C4.02 - Overnight IB Placement',
        'C4.03 - Total IB Borrowing',
        'C4.04 - Interbank Repos',
        'C4.05 - Interbank NIDS'
    ],
    'AMOUNT': [sum4c401, sum4c402, sum4c403, sum4c404, sum4c405]
})

df_breakdown.write_csv(f'{OUTPUT_DIR}OVERNIGHT_BREAKDOWN.csv')

print(f"\n{'='*60}")
print("✓ EILIQ304 Complete")
print(f"{'='*60}")
print(f"\nREPORT ID: EIBMLIQ3")
print(f"DATE: {rdate}")
print(f"PART 3")
print(f"CONCENTRATION OF FUNDING SOURCES (PBB)")
print(f"RATIO 4: TOTAL NET DOMESTIC OVERNIGHT INTERBANK BORROWING (ACCEPTANCE)")
print(f"         / TOTAL GROSS DOMESTIC INTERBANK ACCEPTANCE LESS OVERNIGHT")
print(f"         DOMESTIC INTERBANK LENDING")
print(f"\nSummary:")
print(f"  Net Overnight Borrowing: RM {subta4:,.2f}")
print(f"  Gross Domestic IB (less Overnight Placement): RM {g4tot4:,.2f}")
print(f"  Ratio 4: {ratio4:.2f}%")
print(f"\nComponents:")
print(f"  Overnight Borrowing: RM {sum4c401:,.2f}")
print(f"  Overnight Placement: RM {sum4c402:,.2f}")
print(f"  Net Overnight: RM {subta4:,.2f}")
print(f"\nGross Domestic IB:")
print(f"  Total IB Borrowing: RM {sum4c403:,.2f}")
print(f"  Interbank Repos: RM {sum4c404:,.2f}")
print(f"  Interbank NIDS: RM {sum4c405:,.2f}")
print(f"  Subtotal: RM {subtb4:,.2f}")
print(f"  Less Overnight Placement: RM {sum4c402:,.2f}")
print(f"  Total: RM {g4tot4:,.2f}")
print(f"\nOutputs:")
print(f"  - OVERNIGHT_RATIO.csv (Summary)")
print(f"  - OVERNIGHT_BREAKDOWN.csv (Detailed)")
