"""
EILIQ302 - Concentration of Funding Sources (PBB) - Ratio 2

Purpose: Calculate Net Offshore Borrowing / Total Domestic Deposit Liabilities
- Part 3 of liquidity monitoring (Ratio 2)
- BNM regulatory reporting
- Foreign exposure concentration analysis

Formula:
  Ratio 2 = (Net Offshore Borrowing / Total Domestic Deposits) × 100
  Net Offshore Borrowing = Total Offshore Borrowing - Total Offshore Placements
  Total Domestic Deposits = C1.03 - C2.01 + C2.10 + C2.11
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

# Directories
BNM1_DIR = 'data/bnm1/'
EQ1_DIR = 'data/eq1/'
SIP_DIR = 'data/sip/'
CIS_DIR = 'data/cis/'
CA_DIR = 'data/ca/'
FORATE_DIR = 'data/forate/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EILIQ302 - Net Offshore Borrowing Ratio")
print("=" * 60)

# Get report parameters
try:
    reptmon = '12'
    reptyear = '2024'
    wk4 = '4'
    nowk = '4'
    mthend = monthrange(int(reptyear), int(reptmon))[1]
    
    reptdate = datetime(int(reptyear), int(reptmon), mthend)
    rdate = reptdate.strftime('%d %B %Y')
    
    # From EILIQ301 - these would be passed or calculated
    bal34 = 0.0  # C1.03 from Ratio 1
    bal44 = 0.0  # C1.04 from Ratio 1
    bal54 = 0.0  # C1.05 from Ratio 1
    sum4c103m = 0.0  # C1.03M from Ratio 1
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Initialize layout
layout_data = [
    ('C2.01', 1), ('C2.01A', 2), ('C2.01B', 3), ('C2.01C', 4),
    ('C2.01D', 5), ('C2.01E', 6), ('C2.01F', 7), ('C2.01G', 8),
    ('C2.01H', 9), ('C2.01I', 10), ('C2.01J', 11), ('C2.01K', 12),
    ('C2.01L', 13), ('C2.02', 14), ('C2.02A', 15), ('C2.02B', 16),
    ('C2.02C', 17), ('C2.03', 18), ('C2.03A', 19), ('C2.03B', 20),
    ('C2.04', 21), ('C2.04A', 22), ('C2.04B', 23), ('C2.04C', 24),
    ('C2.04D', 25), ('C2.04E', 26), ('C2.04F', 27), ('C2.04G', 28),
    ('C2.04H', 29), ('C2.04I', 30), ('C2.05', 31), ('C2.06', 32),
    ('C2.07', 33), ('C2.08', 34), ('C2.08A', 35), ('C2.08B', 36),
    ('C2.08C', 37), ('C2.08D', 38), ('C2.08E', 39), ('C2.08F', 40),
    ('C2.09', 41), ('C2.10', 42), ('C2.11', 43)
]

df_layout = pl.DataFrame({
    'ITEM': [x[0] for x in layout_data],
    'ITEM2': [x[1] for x in layout_data]
})

print("\n1. Processing W1AL (C2.04E, C2.08D, C2.04H, C2.08E)...")
try:
    df_w1al = pl.read_parquet(f'{BNM1_DIR}W1AL{reptmon}{wk4}.parquet')
    
    # Extract specific items
    df_w1al = df_w1al.with_columns([
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    # Map SET_ID to ITEM
    df_w1al = df_w1al.with_columns([
        pl.when(pl.col('SET_ID') == 'F143620LKR')
          .then(pl.lit('C2.04E'))
        .when(pl.col('SET_ID') == 'F133620LKR')
          .then(pl.lit('C2.08D'))
        .when(pl.col('SET_ID') == 'F242699PBL')
          .then(pl.lit('C2.04H'))
        .when(pl.col('SET_ID') == 'F133690VFBI')
          .then(pl.lit('C2.08E'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_w1al = df_w1al.filter(pl.col('ITEM').is_not_null())
    
    # Get C2.04E and C2.08D for adjustment
    c204e = df_w1al.filter(pl.col('SET_ID') == 'F143620LKR')['AMOUNT4'].sum()
    c208d = df_w1al.filter(pl.col('SET_ID') == 'F133620LKR')['AMOUNT4'].sum()
    
    print(f"  ✓ W1AL: {len(df_w1al):,} records")
    print(f"    C2.04E: RM {c204e:,.2f}")
    print(f"    C2.08D: RM {c208d:,.2f}")

except Exception as e:
    print(f"  ⚠ W1AL: {e}")
    df_w1al = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
    c204e = 0.0
    c208d = 0.0

# Process ALW (Multiple items)
print("\n2. Processing ALW (Main deposit/borrowing items)...")
try:
    df_alw = pl.read_parquet(f'{BNM1_DIR}ALW{reptmon}{wk4}.parquet')
    
    df_alw = df_alw.filter(pl.col('AMTIND') != ' ')
    
    # Create ITEM based on ITCODE
    df_alw = df_alw.with_columns([
        pl.col('AMOUNT').alias('AMOUNT4')
    ])
    
    # Map ITCODE to ITEM
    itcode_map = {
        '4211085000000Y': 'C2.01A',
        '4212085000000Y': 'C2.01B',
        '4215081000000Y': 'C2.02A',
        '4215085000000Y': 'C2.02B',
        '4216081000000Y': 'C2.03A',
        '4216085000000Y': 'C2.03B',
        '4311081000000Y': 'C2.04B',
        '4319981000000Y': 'C2.04C',
        '4369981000000Y': 'C2.04F',
        '3311081000000Y': 'C2.08B',
    }
    
    # Handle compound ITCODE conditions
    df_alw = df_alw.with_columns([
        pl.when(pl.col('ITCODE') == '4211085000000Y').then(pl.lit('C2.01A'))
        .when(pl.col('ITCODE') == '4212085000000Y').then(pl.lit('C2.01B'))
        .when(pl.col('ITCODE').is_in(['4213081000000Y', '4213085000000Y'])).then(pl.lit('C2.01C'))
        .when(pl.col('ITCODE').is_in(['4219181000000Y', '4219185000000Y'])).then(pl.lit('C2.01E'))
        .when(pl.col('ITCODE').is_in(['4219981000000Y', '4219985000000Y'])).then(pl.lit('C2.01G'))
        .when(pl.col('ITCODE') == '4261085000000Y').then(pl.lit('C2.01H'))
        .when(pl.col('ITCODE').is_in(['4263081000000Y', '4263085000000Y'])).then(pl.lit('C2.01I'))
        .when(pl.col('ITCODE').is_in(['4269181000000Y', '4269185000000Y'])).then(pl.lit('C2.01J'))
        .when(pl.col('ITCODE').is_in(['4269981000000Y', '4269985000000Y'])).then(pl.lit('C2.01K'))
        .when(pl.col('ITCODE') == '4215081000000Y').then(pl.lit('C2.02A'))
        .when(pl.col('ITCODE') == '4215085000000Y').then(pl.lit('C2.02B'))
        .when(pl.col('ITCODE') == '4216081000000Y').then(pl.lit('C2.03A'))
        .when(pl.col('ITCODE') == '4216085000000Y').then(pl.lit('C2.03B'))
        .when(pl.col('ITCODE').is_in(['4314081100000Y', '4314081200000Y'])).then(pl.lit('C2.04A'))
        .when(pl.col('ITCODE') == '4311081000000Y').then(pl.lit('C2.04B'))
        .when(pl.col('ITCODE') == '4319981000000Y').then(pl.lit('C2.04C'))
        .when(pl.col('ITCODE').is_in(['4364081100000Y', '4364081200000Y'])).then(pl.lit('C2.04D'))
        .when(pl.col('ITCODE') == '4362081000000Y').then(pl.lit('C2.04E'))
        .when(pl.col('ITCODE') == '4369981000000Y').then(pl.lit('C2.04F'))
        .when(pl.col('ITCODE').is_in(['3314081100000Y', '3314081200000Y'])).then(pl.lit('C2.08A'))
        .when(pl.col('ITCODE') == '3311081000000Y').then(pl.lit('C2.08B'))
        .when(pl.col('ITCODE').is_in(['3364081100000Y', '3364081200000Y'])).then(pl.lit('C2.08C'))
        .when(pl.col('ITCODE') == '3362081000000Y').then(pl.lit('C2.08D'))
        .otherwise(None)
        .alias('ITEM')
    ])
    
    df_alw = df_alw.filter(pl.col('ITEM').is_not_null())
    
    # Adjust C2.04E and C2.08D
    df_alw = df_alw.with_columns([
        pl.when(pl.col('ITEM') == 'C2.04E')
          .then(pl.col('AMOUNT4') - c204e)
        .when(pl.col('ITEM') == 'C2.08D')
          .then(pl.col('AMOUNT4') - c208d)
        .otherwise(pl.col('AMOUNT4'))
        .alias('AMOUNT4')
    ])
    
    print(f"  ✓ ALW: {len(df_alw):,} records")

except Exception as e:
    print(f"  ⚠ ALW: {e}")
    df_alw = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

# Process SIP (C2.01D)
print("\n3. Processing SIP (C2.01D - SIP Certificates)...")
try:
    df_sip = pl.read_parquet(f'{SIP_DIR}SIPCERT{reptmon}{wk4}{reptyear}.parquet')
    
    df_sip = df_sip.filter(
        (pl.col('INVSTAT') != 601) &
        (pl.col('CUSTCODE') >= 80) &
        (pl.col('CUSTCODE') <= 99)
    )
    
    df_sip = df_sip.with_columns([
        pl.lit('C2.01D').alias('ITEM'),
        (pl.col('PRINAMT') - pl.col('REDAMT')).alias('AMOUNT4')
    ])
    
    print(f"  ✓ SIP: {len(df_sip):,} records")

except Exception as e:
    print(f"  ⚠ SIP: {e}")
    df_sip = pl.DataFrame({'ITEM': ['C2.01D'], 'AMOUNT4': [0.0]})

# Process UTFX (C2.01F, C2.04I, C2.08F)
print("\n4. Processing UTFX (FX deals)...")
try:
    df_utfx = pl.read_parquet(f'{EQ1_DIR}UTFX{reptmon}{wk4}{reptyear}.parquet')
    
    # C2.01F: FX deposits from FE
    df_utfx_01f = df_utfx.filter(
        (pl.col('DEALTYPE').is_in(['BCD', 'BCI', 'BCQ'])) &
        (pl.col('STRTDATE') <= pl.col('BUSDATE')) &
        (((pl.col('CUSTFISS') >= '80') & (pl.col('CUSTFISS') <= '99')) |
         (pl.col('CUSTEQTP').is_in(['EB', 'BA', 'CE', 'BE', 'GA'])))
    )
    
    df_utfx_01f = df_utfx_01f.with_columns([
        pl.lit('C2.01F').alias('ITEM'),
        pl.col('AMTPAY').alias('AMOUNT4')
    ])
    
    # C2.04I and C2.08F: PB(L) FX deals (need FX conversion)
    df_utfx_fxconv = df_utfx.filter(
        (pl.col('CUSTID') == 'BR 900002A') &
        (pl.col('STRTDATE') <= pl.col('BUSDATE'))
    )
    
    # C2.04I: Deposit side
    df_utfx_04i = df_utfx_fxconv.filter(
        (~pl.col('DEALTYPE').is_in(['BCC', 'BCD', 'BCQ'])) &
        (pl.col('BASICTYP') == 'D')
    )
    
    # C2.08F: Loan side
    df_utfx_08f = df_utfx_fxconv.filter(
        (~pl.col('DEALTYPE').is_in(['BCD', 'BCQ', 'LCC'])) &
        (pl.col('BASICTYP') == 'L')
    )
    
    # Read FX rates for conversion
    try:
        df_forate = pl.read_parquet(f'{FORATE_DIR}FORATEBKP.parquet')
        df_forate = df_forate.filter(pl.col('REPTDATE') <= reptdate)
        df_forate = df_forate.sort(['CURCODE', 'REPTDATE'], descending=[False, True])
        df_forate = df_forate.unique(subset=['CURCODE'], keep='first')
        
        # Convert C2.04I
        if len(df_utfx_04i) > 0:
            df_utfx_04i = df_utfx_04i.with_columns([
                pl.col('PURCHCUR').alias('CURCODE')
            ])
            df_utfx_04i = df_utfx_04i.join(df_forate.select(['CURCODE', 'SPOTRATE']), 
                                           on='CURCODE', how='left')
            df_utfx_04i = df_utfx_04i.with_columns([
                pl.lit('C2.04I').alias('ITEM'),
                (pl.col('AMTPAY') * pl.col('SPOTRATE')).alias('AMOUNT4')
            ])
        
        # Convert C2.08F
        if len(df_utfx_08f) > 0:
            df_utfx_08f = df_utfx_08f.with_columns([
                pl.col('SALESCUR').alias('CURCODE')
            ])
            df_utfx_08f = df_utfx_08f.join(df_forate.select(['CURCODE', 'SPOTRATE']), 
                                           on='CURCODE', how='left')
            df_utfx_08f = df_utfx_08f.with_columns([
                pl.lit('C2.08F').alias('ITEM'),
                (pl.col('AMTPAY') * pl.col('SPOTRATE')).alias('AMOUNT4')
            ])
            
    except Exception as e:
        print(f"  ⚠ FX rates: {e}")
        df_utfx_04i = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
        df_utfx_08f = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})
    
    df_utfx_all = pl.concat([df_utfx_01f, df_utfx_04i, df_utfx_08f])
    
    print(f"  ✓ UTFX: {len(df_utfx_all):,} records")

except Exception as e:
    print(f"  ⚠ UTFX: {e}")
    df_utfx_all = pl.DataFrame({'ITEM': [], 'AMOUNT4': []})

# Process UTMS (C2.02C - PB(L) NIDS)
print("\n5. Processing UTMS (C2.02C)...")
try:
    df_utms = pl.read_parquet(f'{EQ1_DIR}UTMS{reptmon}{wk4}{reptyear}.parquet')
    
    df_utms = df_utms.filter(
        (~pl.col('DEALTYPE').is_in(['MOP', 'MSP', 'IUP'])) &
        (pl.col('SECTYPE') != 'DIM') &
        (pl.col('PORTREF') != 'SIPPFD') &
        (pl.col('VALUDATE') <= pl.col('BUSDATE')) &
        (pl.col('CUSTNO') == 'PBLNID')
    )
    
    df_utms = df_utms.with_columns([
        pl.lit('C2.02C').alias('ITEM'),
        pl.col('CAMTOWNE').alias('AMOUNT4')
    ])
    
    sumc202c = df_utms['AMOUNT4'].sum()
    
    print(f"  ✓ UTMS: {len(df_utms):,} records, Total: RM {sumc202c:,.2f}")

except Exception as e:
    print(f"  ⚠ UTMS: {e}")
    df_utms = pl.DataFrame({'ITEM': ['C2.02C'], 'AMOUNT4': [0.0]})
    sumc202c = 0.0

# Process CA for PB(L) (C2.04G)
print("\n6. Processing CA for PB(L) (C2.04G)...")
try:
    # Read CIS
    df_cis = pl.read_parquet(f'{CIS_DIR}CISR1CA{reptmon}{nowk}{reptyear}.parquet')
    df_cis = df_cis.filter(pl.col('SECCUST') == 901)
    df_cis = df_cis.select(['ACCTNO', 'CUSTNO']).unique()
    
    # Read CA
    df_ca = pl.read_parquet(f'{CA_DIR}CA{reptmon}{nowk}{reptyear}.parquet')
    df_ca = df_ca.join(df_cis, on='ACCTNO', how='left')
    df_ca = df_ca.filter(
        (pl.col('CUSTNO') == 3721354) &
        (pl.col('PRODUCT').is_in([104, 105, 147]))
    )
    
    df_ca = df_ca.with_columns([
        pl.lit('C2.04G').alias('ITEM'),
        pl.col('CURBAL').alias('AMOUNT4')
    ])
    
    print(f"  ✓ CA: {len(df_ca):,} records")

except Exception as e:
    print(f"  ⚠ CA: {e}")
    df_ca = pl.DataFrame({'ITEM': ['C2.04G'], 'AMOUNT4': [0.0]})

# Add C2.01L (PB(L) FD from Ratio 1)
df_c201l = pl.DataFrame({
    'ITEM': ['C2.01L'],
    'AMOUNT4': [sum4c103m]
})

# Combine all items
print("\n7. Combining all items...")
df_itemc2 = pl.concat([df_alw, df_sip, df_utfx_all, df_utms, df_ca, df_w1al, df_c201l])

# Aggregate by ITEM
df_itemc2_agg = df_itemc2.group_by('ITEM').agg([
    pl.col('AMOUNT4').sum()
])

# Calculate C2.04H+C2.04I total
summic204 = df_itemc2.filter(pl.col('ITEM').is_in(['C2.04G', 'C2.04H', 'C2.04I']))['AMOUNT4'].sum()

# Calculate C2.08F total
summic208f = df_itemc2.filter(pl.col('ITEM') == 'C2.08F')['AMOUNT4'].sum()

# Calculate group totals
sum4c201 = df_itemc2.filter(
    pl.col('ITEM').str.starts_with('C2.01')
)['AMOUNT4'].sum() - sum4c103m

# Zero out not reportable
df_itemc2_zero = df_itemc2.with_columns([
    pl.when(pl.col('ITEM').is_in(['C2.01D', 'C2.01E', 'C2.01J']))
      .then(0)
      .otherwise(pl.col('AMOUNT4'))
      .alias('AMOUNT4')
])

sum4c202 = df_itemc2_zero.filter(pl.col('ITEM').str.starts_with('C2.02'))['AMOUNT4'].sum() - sumc202c
sum4c203 = df_itemc2_zero.filter(pl.col('ITEM').str.starts_with('C2.03'))['AMOUNT4'].sum()
sum4c204 = df_itemc2_zero.filter(pl.col('ITEM').str.starts_with('C2.04'))['AMOUNT4'].sum() - summic204
sum4c208 = df_itemc2_zero.filter(pl.col('ITEM').str.starts_with('C2.08'))['AMOUNT4'].sum() - summic208f

# Calculate final values
sub2ta4 = sum4c201 + sum4c202 + sum4c203 + sum4c204
sub2tb4 = sum4c208
g2tot4 = sub2ta4 - sub2tb4

# Get specific values
c202b = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.02B')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.02B')) > 0 else 0
c203b = df_itemc2_agg.filter(pl.col('ITEM') == 'C2.03B')['AMOUNT4'][0] if len(df_itemc2_agg.filter(pl.col('ITEM') == 'C2.03B')) > 0 else 0

# Calculate domestic deposits
sum4c209 = bal34 - sum4c201  # C1.03 - C2.01
sum4c210 = bal44 - c203b     # C1.04 - C2.03B
sum4c211 = bal54 - c202b     # C1.05 - C2.02B
sub2tc4 = sum4c209 + sum4c210 + sum4c211

# Calculate ratio
ratio2 = (g2tot4 / sub2tc4 * 100) if sub2tc4 != 0 else 0

print(f"\n  Calculations:")
print(f"    Offshore Borrowing: RM {sub2ta4:,.2f}")
print(f"    Offshore Placements: RM {sub2tb4:,.2f}")
print(f"    Net Offshore: RM {g2tot4:,.2f}")
print(f"    Domestic Deposits: RM {sub2tc4:,.2f}")
print(f"    Ratio 2: {ratio2:.2f}%")

# Save output
df_output = pl.DataFrame({
    'METRIC': ['Offshore Borrowing', 'Offshore Placements', 'Net Offshore', 
               'Domestic Deposits', 'Ratio 2'],
    'AMOUNT': [sub2ta4, sub2tb4, g2tot4, sub2tc4, ratio2]
})

df_output.write_csv(f'{OUTPUT_DIR}OFFSHORE_RATIO.csv')
print(f"  ✓ Saved: OFFSHORE_RATIO.csv")

print(f"\n{'='*60}")
print("✓ EILIQ302 Complete")
print(f"{'='*60}")
print(f"\nREPORT ID: EIBMLIQ3")
print(f"DATE: {rdate}")
print(f"PART 3")
print(f"CONCENTRATION OF FUNDING SOURCES (PBB)")
print(f"RATIO 2: NET OFFSHORE BORROWING (ACCEPTANCE) / TOTAL DOMESTIC DEPOSIT LIABILITIES")
print(f"\nSummary:")
print(f"  Net Offshore Borrowing: RM {g2tot4:,.2f}")
print(f"  Total Domestic Deposits: RM {sub2tc4:,.2f}")
print(f"  Ratio 2: {ratio2:.2f}%")
print(f"\nOutput: OFFSHORE_RATIO.csv")
