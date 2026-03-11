"""
EILIQ303 - Concentration of Funding Sources (PBB) - Ratio 3

Purpose: Calculate Net Domestic Interbank Borrowing / Total Domestic Deposits
- Part 3 of liquidity monitoring (Ratio 3)
- BNM regulatory reporting
- Domestic interbank exposure concentration

Formula:
  Ratio 3 = (Net Domestic Interbank Borrowing / Total Domestic Deposits) × 100
  Net Domestic = Interbank Borrowing - Interbank Placements
  Total Domestic Deposits = C2.09 + C2.10 + C2.11 (from Ratio 2)
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

# Directories
BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EILIQ303 - Net Domestic Interbank Borrowing Ratio")
print("=" * 60)

# Get report parameters
try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    # From EILIQ302 - these would be passed or calculated
    sum4c209 = 0.0  # C2.09 - All Deposits less FE Deposits
    sum4c210 = 0.0  # C2.10 - Non-IB Repos less FE Repos
    sum4c211 = 0.0  # C2.11 - Non-IB NIDS less FE NIDS
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Initialize layout
layout_data = [
    ('C3.01', 1), ('C3.01A', 2), ('C3.01B', 3), ('C3.01C', 4),
    ('C3.01D', 5), ('C3.01E', 6), ('C3.01F', 7), ('C3.01G', 8),
    ('C3.01H', 9), ('C3.01I', 10), ('C3.01J', 11), ('C3.01K', 12),
    ('C3.01L', 13), ('C3.01M', 14), ('C3.01N', 15), ('C3.02', 16),
    ('C3.03', 17), ('C3.04', 18), ('C3.04A', 19), ('C3.04B', 20),
    ('C3.04C', 21), ('C3.04D', 22), ('C3.04E', 23), ('C3.04F', 24),
    ('C3.04G', 25), ('C3.04H', 26), ('C3.04I', 27), ('C3.04J', 28),
    ('C3.04K', 29), ('C3.04L', 30), ('C3.04M', 31), ('C3.04N', 32),
    ('C3.05', 33), ('C3.06', 34), ('C3.06A', 35), ('C3.06B', 36),
    ('C3.06C', 37), ('C3.06D', 38), ('C3.06E', 39), ('C3.06F', 40),
    ('C3.07', 41), ('C3.08', 42), ('C3.09', 43)
]

df_layout = pl.DataFrame({
    'ITEM': [x[0] for x in layout_data],
    'ITEM2': [x[1] for x in layout_data]
})

print("\n1. Processing ALW (Interbank items)...")
try:
    df_alw = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    
    df_alw = df_alw.filter(pl.col('AMTIND') != ' ')
    
    df_alw = df_alw.with_columns([
        pl.col('AMOUNT').alias('AMOUNT4'),
        pl.lit(0.0).alias('AMT4')
    ])
    
    # Map ITCODE to ITEM
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
        .when(pl.col('ITCODE').is_in(['4216001000000Y', '4216002000000Y', '4216003000000Y',
                                       '4216007000000Y', '4216012000000Y'])).then(pl.lit('C3.02'))
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
        .when(pl.col('ITCODE') == '4369903000000Y').then(pl.lit('C3.01N'))
        .when(pl.col('ITCODE') == '3369003000000F').then(pl.lit('C3.04N'))
        .when(pl.col('ITCODE').is_in(['3250001000000Y', '3250020000000Y', '3250081000000Y'])).then(pl.lit('C3.05'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    # Handle C3.05 special case (zero out specific ITCODEs)
    df_alw = df_alw.with_columns([
        pl.when(pl.col('ITCODE').is_in(['3250020000000Y', '3250081000000Y']))
          .then(pl.col('AMOUNT4'))
          .otherwise(0.0)
          .alias('AMT4')
    ])
    
    df_alw = df_alw.filter(pl.col('ITEM').is_not_null())
    
    print(f"  ✓ ALW: {len(df_alw):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw = pl.DataFrame({'ITEM': [], 'AMOUNT4': [], 'AMT4': []})

# Process W1AL (C3.06A)
print("\n2. Processing W1AL (C3.06A - NIDS Held)...")
try:
    df_w1al = pl.read_parquet(f'{BNM1_DIR}W1AL{reptmon}{wk4}.parquet')
    
    df_w1al = df_w1al.filter(pl.col('SET_ID') == 'F137030NIDH')
    
    df_w1al = df_w1al.with_columns([
        pl.lit('C3.06A').alias('ITEM'),
        pl.col('AMOUNT').alias('AMOUNT4'),
        pl.lit(0.0).alias('AMT4')
    ])
    
    print(f"  ✓ W1AL: {len(df_w1al):,} records")

except Exception as e:
    print(f"  ⚠ W1AL: {e}")
    df_w1al = pl.DataFrame({'ITEM': [], 'AMOUNT4': [], 'AMT4': []})

# Process UTMS (C3.03, C3.06B-F)
print("\n3. Processing UTMS (NIDS)...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}UTMS{reptmon}{wk4}{reptyear}.parquet')
    
    # C3.03: Interbank NIDS
    df_utms_c303 = df_utms.filter(
        (~pl.col('DEALTYPE').is_in(['MOP', 'MSP', 'IUP'])) &
        (pl.col('SECTYPE') != 'DIM') &
        (pl.col('PORTREF') != 'SIPPFD') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01', '02', '03', '07', '12'])) |
         (pl.col('CUSTEQTP').is_in(['BC', 'BB', 'BI', 'BJ', 'BM'])))
    )
    
    df_utms_c303 = df_utms_c303.with_columns([
        pl.lit('C3.03').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4'),  # Use CAMTOWNE for C3.03
        pl.lit(0.0).alias('AMT4')
    ])
    
    # C3.06B-F: NIDS held with resident banks
    df_utms_nids = df_utms.filter(
        pl.col('SECTYPE').is_in(['SDC', 'SLD', 'SSD', 'SZD', 'LDC'])
    )
    
    df_utms_nids = df_utms_nids.with_columns([
        pl.when((pl.col('CUSTFISS') == '17'))
          .then(pl.lit('C3.06B'))  # Cagamas
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['BKPKSM', 'ISME']))
          .then(pl.lit('C3.06C'))  # SME Bank
        .when((pl.col('CUSTFISS') == '33') & (pl.col('CUSTNO') == 'BKRAK'))
          .then(pl.lit('C3.06D'))  # Bank Kerjasama Rakyat
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['IBSN', 'BSN']))
          .then(pl.lit('C3.06E'))  # Bank Simpanan Nasional
        .when((pl.col('CUSTFISS') == '33') & pl.col('CUSTNO').is_in(['BPMB', 'IBPMB']))
          .then(pl.lit('C3.06F'))  # Bank Pembangunan Malaysia
        .otherwise(None)
        .alias('ITEM'),
        (pl.col('CAMTOWNE') + pl.col('INTACR') - pl.col('UNDISCPR')).alias('AMOUNT4'),
        pl.lit(0.0).alias('AMT4')
    ])
    
    df_utms_nids = df_utms_nids.filter(pl.col('ITEM').is_not_null())
    
    df_utms_all = pl.concat([df_utms_c303, df_utms_nids])
    
    print(f"  ✓ UTMS: {len(df_utms_all):,} records")

except Exception as e:
    print(f"  ⚠ UTMS: {e}")
    df_utms_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': [], 'AMT4': []})

# Combine all items
print("\n4. Combining all items...")
df_itemc3 = pl.concat([df_alw, df_w1al, df_utms_all])

# Aggregate by ITEM
df_itemc3_agg = df_itemc3.group_by('ITEM').agg([
    pl.col('AMOUNT4').sum(),
    pl.col('AMT4').sum()
])

# Handle C3.05 special calculation
df_c305 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.05')
if len(df_c305) > 0:
    amount4_c305 = df_c305['AMOUNT4'][0]
    amt4_c305 = df_c305['AMT4'][0]
    amount4_new_c305 = amount4_c305 - amt4_c305
else:
    amount4_new_c305 = 0.0

# Calculate group totals
sum4c301 = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.01'))['AMOUNT4'].sum()
sum4c304 = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.04'))['AMOUNT4'].sum()

# Calculate C3.06 (NIDS held net)
c306_items = df_itemc3.filter(pl.col('ITEM').str.starts_with('C3.06'))
sum4c306a = c306_items.filter(pl.col('ITEM') == 'C3.06A')['AMOUNT4'].sum()
sum4c306b = c306_items.filter(pl.col('ITEM') != 'C3.06A')['AMOUNT4'].sum()
sum4c306 = sum4c306a - sum4c306b  # NIDS held - NIDS with resident banks

sum4c302 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.02')['AMOUNT4'].sum() if len(df_itemc3_agg.filter(pl.col('ITEM') == 'C3.02')) > 0 else 0
sum4c303 = df_itemc3_agg.filter(pl.col('ITEM') == 'C3.03')['AMOUNT4'].sum() if len(df_itemc3_agg.filter(pl.col('ITEM') == 'C3.03')) > 0 else 0

print(f"\n  Group Totals:")
print(f"    C3.01 (Interbank Borrowing): RM {sum4c301:,.2f}")
print(f"    C3.02 (Interbank Repos): RM {sum4c302:,.2f}")
print(f"    C3.03 (Interbank NIDS): RM {sum4c303:,.2f}")
print(f"    C3.04 (Interbank Placement): RM {sum4c304:,.2f}")
print(f"    C3.05 (Reverse Repos): RM {amount4_new_c305:,.2f}")
print(f"    C3.06 (NIDS Held Net): RM {sum4c306:,.2f}")

# Calculate final values
sub3ta4 = sum4c301 + sum4c302 + sum4c303  # Interbank Borrowing
sub3tb4 = sum4c304 + amount4_new_c305 + sum4c306  # Interbank Placements
g3tot4 = sub3ta4 - sub3tb4  # Net Domestic Interbank Borrowing

# Calculate domestic deposits (from Ratio 2)
sum4c307 = sum4c209
sum4c308 = sum4c210
sum4c309 = sum4c211
sub3tc4 = sum4c307 + sum4c308 + sum4c309

# Calculate ratio
ratio3 = (g3tot4 / sub3tc4 * 100) if sub3tc4 != 0 else 0

print(f"\n  Calculations:")
print(f"    Interbank Borrowing (C3.03A): RM {sub3ta4:,.2f}")
print(f"    Interbank Placements (C3.06G): RM {sub3tb4:,.2f}")
print(f"    Net Domestic Interbank (C3.06H): RM {g3tot4:,.2f}")
print(f"    Total Domestic Deposits (C3.09A): RM {sub3tc4:,.2f}")
print(f"    Ratio 3: {ratio3:.2f}%")

# Prepare final output
df_output = pl.DataFrame({
    'METRIC': ['Interbank Borrowing', 'Interbank Placements', 
               'Net Domestic Interbank', 'Domestic Deposits', 'Ratio 3'],
    'AMOUNT': [sub3ta4, sub3tb4, g3tot4, sub3tc4, ratio3]
})

df_output.write_csv(f'{OUTPUT_DIR}DOMESTIC_INTERBANK_RATIO.csv')
print(f"  ✓ Saved: DOMESTIC_INTERBANK_RATIO.csv")

# Create detailed report
item_descriptions = {
    1: 'TOTAL INTERBANK BORROWING',
    2: 'RM INTERBANK BORROWING FROM BNM',
    3: 'RM INTERBANK BORROWING FROM CB',
    4: 'RM INTERBANK BORROWING FROM IB',
    5: 'RM INTERBANK BORROWING FROM MB',
    6: 'RM VOSTRO A/C BALANCES OF CB',
    7: 'RM VOSTRO A/C BALANCES OF IB',
    8: 'RM OVERDRAWN NOSTRO A/C WITH CB',
    9: 'RM OVERDRAWN NOSTRO A/C WITH IB',
    10: 'FX INTERBANK BORROWING FROM BNM',
    11: 'FX INTERBANK BORROWING FROM CB',
    12: 'FX INTERBANK BORROWING FROM IB',
    13: 'FX INTERBANK BORROWING FROM MB',
    14: 'FX INTERBANK BORROWING FROM IIB',
    15: 'FX OTHER AMOUNT DUE TO IB',
    16: 'INTERBANK REPOS',
    17: 'INTERBANK NIDS',
    18: 'TOTAL INTERBANK PLACEMENT',
    19: 'RM INTERBANK PLACEMENTS WITH BNM',
    20: 'RM INTERBANK PLACEMENTS WITH CB',
    21: 'RM INTERBANK PLACEMENTS WITH IB',
    22: 'RM INTERBANK PLACEMENTS WITH MB',
    23: 'RM OVERDRAWN VOSTRO A/C OF CB',
    24: 'RM OVERDRAWN VOSTRO A/C OF IB',
    25: 'RM NOSTRO A/C WITH CB',
    26: 'RM NOSTRO A/C WITH IB',
    27: 'FX INTERBANK PLACEMENTS WITH BNM',
    28: 'FX INTERBANK PLACEMENTS WITH CB',
    29: 'FX INTERBANK PLACEMENTS WITH IB',
    30: 'FX INTERBANK PLACEMENTS WITH MB',
    31: 'FX INTERBANK PLACEMENTS WITH IIB',
    32: 'FX OTHER AMOUNT DUE FROM IB',
    33: 'INTERBANK REVERSE REPOS',
    34: 'TOTAL NIDS HELD WITH RESIDENT BANK',
    35: 'TOTAL NIDS HELD',
    36: 'NIDS HELD WITH CAGAMAS',
    37: 'NIDS HELD WITH SME BANK',
    38: 'NIDS HELD WITH BANK KERJASAMA RAKYAT',
    39: 'NIDS HELD WITH BANK SIMPANAN NASIONAL',
    40: 'NIDS HELD WITH BANK PEMBANGUNAN MALAYSIA BHD',
    41: 'ALL DEPOSITS (RATIO 1) LESS ALL DEPOSITS (RATIO 2)',
    42: 'NON-INTERBANK REPOS',
    43: 'NON-INTERBANK NIDS',
}

print(f"\n{'='*60}")
print("✓ EILIQ303 Complete")
print(f"{'='*60}")
print(f"\nREPORT ID: EIBMLIQ3")
print(f"DATE: {rdate}")
print(f"PART 3")
print(f"CONCENTRATION OF FUNDING SOURCES (PBB)")
print(f"RATIO 3: NET DOMESTIC INTERBANK BORROWING (ACCEPTANCE) / TOTAL DOMESTIC DEPOSIT LIABILITIES")
print(f"\nSummary:")
print(f"  Net Domestic Interbank Borrowing: RM {g3tot4:,.2f}")
print(f"  Total Domestic Deposits: RM {sub3tc4:,.2f}")
print(f"  Ratio 3: {ratio3:.2f}%")
print(f"\nBank Types:")
print(f"  BNM: Bank Negara Malaysia")
print(f"  CB: Commercial Banks")
print(f"  IB: Investment Banks")
print(f"  MB: Merchant Banks")
print(f"  IIB: Islamic Investment Banks")
print(f"\nOutput: DOMESTIC_INTERBANK_RATIO.csv")
