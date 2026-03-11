"""
EILIQ301 - Concentration of Funding Sources (PBB)

Purpose: Calculate Adjusted Loan/Deposit Ratio (Ratio 1)
- Part 3 of liquidity monitoring
- BNM regulatory reporting
- Concentration of funding sources analysis

Formula:
  Ratio 1 = (Adjusted Loans / Adjusted Deposits) × 100
  Adjusted Loans = Gross Loans - BA Payable
  Adjusted Deposits = Sum of various deposit categories - Statutory Deposits
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

# Directories
BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
CIS_DIR = 'data/cis/'
FD_DIR = 'data/fd/'
FCY_DIR = 'data/fcy/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EILIQ301 - Concentration of Funding Sources")
print("=" * 60)

# Get report parameters (from environment or config)
try:
    # These would typically come from a config file or parameters
    reptmon = '12'  # Example: December
    reptyear = '2024'
    wk4 = '4'  # Week 4 (month-end)
    nowk = '4'
    
    # Get last day of month
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")
    print(f"Month: {reptmon}, Week: {wk4}")

except Exception as e:
    print(f"✗ ERROR setting parameters: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Initialize layout with all items
layout_data = [
    ('C1.01', 1), ('C1.02', 2), ('C1.03', 3), ('C1.03A', 4),
    ('C1.03B', 5), ('C1.03C', 6), ('C1.03D', 7), ('C1.03E', 8),
    ('C1.03F', 9), ('C1.03G', 10), ('C1.03H', 11), ('C1.03I', 12),
    ('C1.03J', 13), ('C1.03K', 14), ('C1.03L', 15), ('C1.03M', 16),
    ('C1.04', 17), ('C1.05', 18), ('C1.05A', 19), ('C1.05B', 20),
    ('C1.06', 21), ('C1.07', 22), ('C1.08', 23), ('C1.09', 24),
    ('C1.10', 25)
]

df_layout = pl.DataFrame({
    'ITEM': [x[0] for x in layout_data],
    'ITEM2': [x[1] for x in layout_data]
})

print("\n1. Processing K1TBL (C1.04 - Non-interbank repos)...")
try:
    df_k1tbl = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    
    # Filter specific IT codes
    it_codes = ['4216017000000Y', '4216020000000Y', '4216060000000Y',
                '4216071000000Y', '4216072000000Y', '4216073000000Y',
                '4216074000000Y', '4216076000000Y', '4216079000000Y',
                '4216085000000Y']
    
    df_k1tbl = df_k1tbl.filter(
        (pl.col('AMTIND') != ' ') &
        (pl.col('ITCODE').is_in(it_codes))
    )
    
    df_k1tbl = df_k1tbl.with_columns([
        pl.lit('C1.04').alias('ITEM'),
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    print(f"  ✓ K1TBL: {len(df_k1tbl):,} records")

except Exception as e:
    print(f"  ⚠ K1TBL: {e}")
    df_k1tbl = pl.DataFrame({'ITEM': ['C1.04'], 'AMOUNT4': [0.0]})

# Process UTMS (C1.05B - NIDS Issued)
print("\n2. Processing UTMS (C1.05B - NIDS Issued)...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}UTMS{reptmon}{wk4}{reptyear}.parquet')
    
    # Filter conditions
    df_utms = df_utms.filter(
        (~pl.col('DEALTYPE').is_in(['MOP', 'MSP', 'IUP'])) &
        (pl.col('SECTYPE') != 'DIM') &
        (pl.col('PORTREF') != 'SIPPFD') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        ((pl.col('CUSTFISS').is_in(['01','02','03','07','12','81','82','83','84'])) |
         (pl.col('CUSTEQTP').is_in(['BC','BB','BI','BJ','BM','BA','BE'])))
    )
    
    df_utms = df_utms.with_columns([
        pl.lit('C1.05B').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    print(f"  ✓ UTMS: {len(df_utms):,} records")

except Exception as e:
    print(f"  ⚠ UTMS: {e}")
    df_utms = pl.DataFrame({'ITEM': ['C1.05B'], 'AMOUNT4': [0.0]})

# Process W1AL (Multiple items)
print("\n3. Processing W1AL (Multiple items)...")
try:
    df_w1al = pl.read_parquet(f'{BNM1_DIR}W1AL{reptmon}{wk4}.parquet')
    
    # Mapping of SET_ID to ITEM
    set_id_mapping = {
        'F134000LN': 'C1.01',
        'F249130AP': 'C1.02',
        'F142110DDEP': 'C1.03A',
        'F142120SA': 'C1.03B',
        'F142130FD': 'C1.03C',
        'F142180HDAD': 'C1.03D',
        'F142191ILD': 'C1.03E',
        'F142191DCI': 'C1.03F',
        'F142190STD': 'C1.03G',
        'F142199ODA': 'C1.03H',
        'F142600A': 'C1.03I',
        'F142630FXFD': 'C1.03J',
        'F142691FXDCI': 'C1.03K',
        'F142699ALL': 'C1.03L',
        'F142150NIDI': 'C1.05A',
        'F132110SDBNM': 'C1.06',
        'F144111TIER2': 'C1.08',
        'F144111TIER1': 'C1.09',
        'F144140CAGA': 'C1.10',
    }
    
    # C1.07 has multiple SET_IDs
    c107_set_ids = ['F241101OSC', 'F241102SP', 'F241103SRF', 'F241104GRF',
                    'F241111CRR', 'F241105RPAL', 'F241107CUPL', 'F241109PRR',
                    'F241400', 'F240510RR']
    
    # Map SET_ID to ITEM
    df_w1al = df_w1al.with_columns([
        pl.when(pl.col('SET_ID').is_in(c107_set_ids))
          .then(pl.lit('C1.07'))
          .otherwise(
              pl.col('SET_ID').map_elements(
                  lambda x: set_id_mapping.get(x, None),
                  return_dtype=pl.Utf8
              )
          )
          .alias('ITEM'),
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    df_w1al = df_w1al.filter(pl.col('ITEM').is_not_null())
    
    print(f"  ✓ W1AL: {len(df_w1al):,} records")

except Exception as e:
    print(f"  ⚠ W1AL: {e}")
    df_w1al = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

# Process FD/FCY for PB(L) - C1.03M
print("\n4. Processing FD/FCY for PB(L) (C1.03M)...")
try:
    # Read CIS to get PB(L) customers
    df_cis = pl.read_parquet(f'{CIS_DIR}CISR1FD{reptmon}{nowk}{reptyear}.parquet')
    df_cis = df_cis.filter(pl.col('SECCUST') == 901)
    df_cis = df_cis.select(['ACCTNO', 'CUSTNO']).unique()
    
    # Read FD
    df_fd = pl.read_parquet(f'{FD_DIR}FD{reptmon}{nowk}{reptyear}.parquet')
    df_fd = df_fd.join(df_cis, on='ACCTNO', how='left')
    df_fd = df_fd.filter(pl.col('CUSTNO') == 3721354)
    
    # Read FCY
    df_fcy = pl.read_parquet(f'{FCY_DIR}FCY{reptmon}{nowk}{reptyear}.parquet')
    df_fcy = df_fcy.join(df_cis, on='ACCTNO', how='left')
    df_fcy = df_fcy.filter(
        (pl.col('CUSTNO') == 3721354) &
        (pl.col('PRODUCT') >= 300) &
        (pl.col('PRODUCT') <= 399)
    )
    
    # Combine FD and FCY
    df_fdfcy = pl.concat([df_fd, df_fcy])
    
    # Sum balances
    total_curbal = df_fdfcy['CURBAL'].sum()
    total_curbalrm = df_fdfcy.get_column('CURBALRM').sum() if 'CURBALRM' in df_fdfcy.columns else 0
    
    df_fdfcy_sum = pl.DataFrame({
        'ITEM': ['C1.03M'],
        'AMOUNT4': [total_curbal + total_curbalrm]
    })
    
    print(f"  ✓ FD/FCY: RM {(total_curbal + total_curbalrm):,.2f}")

except Exception as e:
    print(f"  ⚠ FD/FCY: {e}")
    df_fdfcy_sum = pl.DataFrame({'ITEM': ['C1.03M'], 'AMOUNT4': [0.0]})

# Combine all items
print("\n5. Combining all items...")
df_allitem = pl.concat([df_k1tbl, df_utms, df_w1al, df_fdfcy_sum])

# Aggregate by ITEM
df_allitem1 = df_allitem.group_by('ITEM').agg([
    pl.col('AMOUNT4').sum()
])

# Merge with layout
df_allitem1 = df_layout.join(df_allitem1, on='ITEM', how='left')
df_allitem1 = df_allitem1.with_columns([
    pl.col('AMOUNT4').fill_null(0)
])

# Extract key balances
bald = df_allitem1.filter(pl.col('ITEM') == 'C1.01')['AMOUNT4'][0]  # Gross loans
bal4 = df_allitem1.filter(pl.col('ITEM') == 'C1.05A')['AMOUNT4'][0] # Non-interbank NIDS
bal4a = df_allitem1.filter(pl.col('ITEM') == 'C1.05B')['AMOUNT4'][0] # NIDS issued
sum4c103m = df_allitem1.filter(pl.col('ITEM') == 'C1.03M')['AMOUNT4'][0] # PB(L) FD

print(f"\n  Key Balances:")
print(f"    C1.01 (Gross Loans): RM {bald:,.2f}")
print(f"    C1.05A (Non-interbank NIDS): RM {bal4:,.2f}")
print(f"    C1.05B (NIDS Issued): RM {bal4a:,.2f}")
print(f"    C1.03M (PB(L) FD): RM {sum4c103m:,.2f}")

# Calculate subtotals and adjustments
print("\n6. Calculating subtotals...")

# C1.02: BA Payable - calculate subtotal
c102_amount = df_allitem1.filter(pl.col('ITEM') == 'C1.02')['AMOUNT4'][0]
subta4 = bald - c102_amount  # Adjusted Loans

# C1.05: Interbank NIDS - calculate subtotal
subtot_c105 = bal4 - bal4a

# C1.03: Total deposits - sum all C1.03x items
df_item03 = df_allitem1.filter(pl.col('ITEM').str.starts_with('C1.03'))

# Zero out C1.03E, C1.03F, C1.03K (not reportable)
df_item03 = df_item03.with_columns([
    pl.when(pl.col('ITEM').is_in(['C1.03E', 'C1.03F', 'C1.03K']))
      .then(0)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

sum403 = df_item03['AMOUNT4'].sum() - (sum4c103m + sum4c103m)  # Subtract PB(L) FD twice

# Extract more balances for final calculation
bal44 = df_allitem1.filter(pl.col('ITEM') == 'C1.04')['AMOUNT4'][0]  # Non-interbank repos
bal64 = df_allitem1.filter(pl.col('ITEM') == 'C1.06')['AMOUNT4'][0]  # Statutory deposits
bal74 = df_allitem1.filter(pl.col('ITEM') == 'C1.07')['AMOUNT4'][0]  # Capital
bal84 = df_allitem1.filter(pl.col('ITEM') == 'C1.08')['AMOUNT4'][0]  # Tier 2
bal94 = df_allitem1.filter(pl.col('ITEM') == 'C1.09')['AMOUNT4'][0]  # Tier 1
bal104 = df_allitem1.filter(pl.col('ITEM') == 'C1.10')['AMOUNT4'][0] # Cagamas

# Calculate adjusted deposits
lastsub4 = sum403 + bal44 + subtot_c105 + bal74 + bal84 + bal94 + bal104 - bal64

# Calculate ratio
ratio = (subta4 / lastsub4 * 100) if lastsub4 != 0 else 0

print(f"\n  Calculations:")
print(f"    Adjusted Loans (C1.02A): RM {subta4:,.2f}")
print(f"    Adjusted Deposits (C1.10A): RM {lastsub4:,.2f}")
print(f"    Ratio 1: {ratio:.2f}%")

# Prepare final output
print("\n7. Preparing final report...")

# Add subtotal rows
subtotal_data = [
    ('C1.02A', 26, subta4),
    ('C1.10A', 27, lastsub4),
    ('C1.11', 28, 0.0)  # Ratio will be displayed as text
]

df_subtot = pl.DataFrame({
    'ITEM': [x[0] for x in subtotal_data],
    'ITEM2': [x[1] for x in subtotal_data],
    'AMOUNT4': [x[2] for x in subtotal_data]
})

# Combine with main data
df_final = pl.concat([df_allitem1, df_subtot])

# Update C1.03 with calculated sum
df_final = df_final.with_columns([
    pl.when(pl.col('ITEM') == 'C1.03')
      .then(sum403)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

# Update C1.05 with subtotal
df_final = df_final.with_columns([
    pl.when(pl.col('ITEM') == 'C1.05')
      .then(subtot_c105)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

# Zero out not reportable items
df_final = df_final.with_columns([
    pl.when(pl.col('ITEM').is_in(['C1.03E', 'C1.03F', 'C1.03K']))
      .then(0)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

# Add grouping and descriptions
item_descriptions = {
    1: 'GROSS LOANS', 2: 'BA PAYABLE', 3: 'TOTAL ALL DEPOSITS',
    4: 'RM DEMAND DEPOSITS ACCEPTED', 5: 'RM SAVINGS DEPOSITS ACCEPTED',
    6: 'RM FIXED DEPOSITS ACCEPTED', 7: 'RM HOUSING DEVELOPMENT ACCOUNT DEPOSITS',
    8: 'RM INVESTMENT LINK TO DERIVATIVES - SIP (NOT REPORTABLE)',
    9: 'RM INVESTMENT LINK TO DERIVATIVES - DCI (NOT REPORTABLE)',
    10: 'RM SHORT-TERM DEPOSITS ACCEPTED - PMM', 11: 'RM OTHER DEPOSITS ACCEPTED',
    12: 'FX DEMAND DEPOSITS ACCEPTED', 13: 'FX FIXED DEPOSITS ACCEPTED',
    14: 'FX INVESTMENT LINK TO DERIVATIVES - DCI (NOT REPORTABLE)',
    15: 'FX OTHER DEPOSITS ACCEPTED', 16: 'FIXED DEPOSITS ACCEPTED-PB(L)',
    17: 'NON-INTERBANK REPOS', 18: 'NON-INTERBANK NIDS', 19: 'NIDS ISSUED',
    20: 'INTERBANK NIDS', 21: 'STATUTORY DEPOSITS', 22: 'CAPITAL',
    23: 'OTHER HYBRID TIER-2 CAPITAL INSTRUMENTS', 24: 'RM SUBORDINATED BORROWING',
    25: 'LOANS SOLD TO CAGAMAS', 26: 'SUB-TOTAL', 27: 'SUB-TOTAL', 28: f'{ratio:.2f}%'
}

df_final = df_final.with_columns([
    pl.col('ITEM2').map_elements(lambda x: item_descriptions.get(x, ''), return_dtype=pl.Utf8).alias('DESCRIPTION')
])

# Add grouping
df_final = df_final.with_columns([
    pl.when(pl.col('ITEM').str.starts_with('C1.01') | pl.col('ITEM').str.starts_with('C1.02'))
      .then(pl.lit('ADJUSTED LOAN:'))
    .when(pl.col('ITEM') == 'C1.11')
      .then(pl.lit('RATIO:'))
    .otherwise(pl.lit('ADJUSTED DEPOSIT:'))
    .alias('GROUP')
])

# Sort by group and item
df_final = df_final.sort(['GROUP', 'ITEM2'])

# Save output
df_final.write_csv(f'{OUTPUT_DIR}FUNDING_CONCENTRATION.csv')
print(f"  ✓ Saved: FUNDING_CONCENTRATION.csv")

print(f"\n{'='*60}")
print("✓ EILIQ301 Complete")
print(f"{'='*60}")
print(f"\nREPORT ID: EIBMLIQ3")
print(f"DATE: {rdate}")
print(f"PART 3")
print(f"CONCENTRATION OF FUNDING SOURCES (PBB)")
print(f"RATIO 1: ADJUSTED LOAN / DEPOSIT RATIO")
print(f"\nSummary:")
print(f"  Adjusted Loans: RM {subta4:,.2f}")
print(f"  Adjusted Deposits: RM {lastsub4:,.2f}")
print(f"  Ratio 1: {ratio:.2f}%")
print(f"\nOutput: FUNDING_CONCENTRATION.csv")
