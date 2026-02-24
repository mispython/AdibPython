"""
EIIM1503 - Islamic NPL Financial Management Report (PIBB) - Production Ready

Purpose:
- Generate Financial Management (FM) NPL reports for PIBB (Islamic banking)
- Same as EIMB1503 but for Islamic banking division
- Process current and previous month Islamic NPL data
- Calculate averages and accumulate totals
- Produce 3 output files: Count, Amount, Average

Key Difference from EIMB1503:
- Entity: 'PIBB' (Islamic) instead of 'PBB' (Conventional)
- TF Account Range: 2580000000-2589999999 (Islamic TF) vs 2500000000-2599999999
- Same processing logic, different entity designation

Account Types:
- OD: Overdraft (3000000000-3999999999)
- LN: Term Loans (others)
- TF: Trade Finance (2580000000-2589999999) - Islamic specific range

LOB Special Handling:
- PCCC: Use DRFMT mapping (branch-specific LOB)
- HPOP: HP Staff loans (branch H* + product LSTAFF)
- CORP: Corporate TF (product 999)
- RETL: Retail TF (default)

Output Files:
- NPLC: Islamic NPL count by branch/product/LOB
- NPLA: Islamic NPL amount by branch/product/LOB
- NPLV: Islamic NPL average (current + previous)/2 + accumulative

Format: FM standard (comma-separated, fixed positions)
Entity: PIBB (Islamic banking)
"""

import polars as pl
from datetime import datetime
import os

# Directories
LOAN_DIR = 'data/loan/'
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

for d in [OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIIM1503 - Islamic NPL Financial Management Report (PIBB)")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    if mm == datetime.today().month:
        mm = mm - 1
    
    rpmon = f'{mm1:02d}'
    rmonth = f'{mm:02d}'
    reptyear = f'{reptdate.year % 100:02d}'
    ryear = reptdate.year
    ym = f'{ryear}{rmonth}'
    
    print(f"Report Period: {rmonth}/{ryear}")
    print(f"Entity: PIBB (Islamic Banking)")

except Exception as e:
    print(f"  ⚠ REPTDATE: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Read previous month NPL (XNPL)
print("\nReading previous month Islamic NPL (XNPL)...")
try:
    xnpl_data = []
    with open(f'{INPUT_DIR}XNPL', 'r') as f:
        for line in f:
            if line[0:1] == 'D':
                bx1 = line[167:168]
                bx2 = line[167:170]
                bx3 = line[167:171]
                lob = line[200:204]
                producx = line[233:239]
                pamount = float(line[299:314]) if line[299:314].strip() else 0.0
                
                if bx1 == 'H':
                    brancx = bx3
                else:
                    brancx = f'A{bx2}'
                
                xnpl_data.append({
                    'BRANCX': brancx,
                    'PRODUCX': producx,
                    'LOB': lob,
                    'PAMOUNT': pamount
                })
    
    df_xnpl = pl.DataFrame(xnpl_data)
    print(f"  ✓ Previous month NPL: {len(df_xnpl):,} records")

except Exception as e:
    print(f"  ⚠ XNPL: {e}")
    df_xnpl = pl.DataFrame([])

# Read current month NPL
print("\nReading current month Islamic NPL...")
try:
    npl_data = []
    with open(f'{INPUT_DIR}NPL', 'r') as f:
        next(f)
        for line in f:
            branch = int(line[0:4]) if line[0:4].strip() else 0
            acctno = int(line[15:25]) if line[15:25].strip() else 0
            noteno = int(line[30:35]) if line[30:35].strip() else 0
            class_code = line[45:46]
            nplmth = int(line[60:64]) if line[60:64].strip() else 0
            amount = float(line[75:88]) if line[75:88].strip() else 0.0
            product = int(line[90:93]) if line[90:93].strip() else 0
            
            if amount == 0 or class_code == 'P':
                continue
            
            if branch > 3000:
                branch = branch - 3000
            
            if 800 <= branch < 900:
                brancx = f'H{branch:03d}'
            else:
                brancx = f'A{branch:03d}'
            
            # Islamic TF range: 2580000000-2589999999
            if 2580000000 <= acctno <= 2589999999:
                acctype = 'TF'
            elif 3000000000 <= acctno <= 3999999999:
                acctype = 'OD'
            else:
                acctype = 'LN'
            
            npl_data.append({
                'BRANCH': branch,
                'ACCTNO': acctno,
                'NOTENO': noteno,
                'CLASS': class_code,
                'NPLMTH': nplmth,
                'AMOUNT': amount,
                'PRODUCT': product,
                'BRANCX': brancx,
                'ACCTYPE': acctype,
                'NOACC': 1
            })
    
    df_npl = pl.DataFrame(npl_data)
    print(f"  ✓ Current month NPL: {len(df_npl):,} records")

except Exception as e:
    print(f"  ⚠ NPL: {e}")
    df_npl = pl.DataFrame([])

# Read product mappings
print("\nReading product mappings...")

try:
    odfmt_data = []
    with open(f'{INPUT_DIR}ODFMT', 'r') as f:
        for line in f:
            product = int(line[5:8]) if line[5:8].strip() else 0
            producx = line[16:23].strip()
            lob = line[30:34].strip()
            odfmt_data.append({
                'ACCTYPE': 'OD',
                'PRODUCT': product,
                'PRODUCX': producx,
                'LOB': lob
            })
    df_odfmt = pl.DataFrame(odfmt_data)
    print(f"  ✓ ODFMT: {len(df_odfmt):,}")
except:
    df_odfmt = pl.DataFrame([])

try:
    lnfmt_data = []
    with open(f'{INPUT_DIR}LNFMT', 'r') as f:
        for line in f:
            product = int(line[5:8]) if line[5:8].strip() else 0
            producx = line[26:33].strip()
            lob = line[39:43].strip()
            lnfmt_data.append({
                'ACCTYPE': 'LN',
                'PRODUCT': product,
                'PRODUCX': producx,
                'LOB': lob
            })
    df_lnfmt = pl.DataFrame(lnfmt_data)
    print(f"  ✓ LNFMT: {len(df_lnfmt):,}")
except:
    df_lnfmt = pl.DataFrame([])

df_plob = pl.concat([df_odfmt, df_lnfmt]) if len(df_odfmt) > 0 or len(df_lnfmt) > 0 else pl.DataFrame([])

# Merge with product mapping
df_npl = df_npl.join(df_plob, on=['ACCTYPE', 'PRODUCT'], how='left')

# Handle TF
df_npl = df_npl.with_columns([
    pl.when(pl.col('ACCTYPE') == 'TF')
      .then(pl.lit('RETL'))
      .otherwise(pl.col('LOB'))
      .alias('LOB'),
    pl.when(pl.col('ACCTYPE') == 'TF')
      .then(pl.lit('LTF_OT'))
      .otherwise(pl.col('PRODUCX'))
      .alias('PRODUCX')
])

df_npl = df_npl.with_columns([
    pl.when((pl.col('ACCTYPE') == 'TF') & (pl.col('PRODUCT') == 999))
      .then(pl.lit('CORP'))
      .otherwise(pl.col('LOB'))
      .alias('LOB')
])

# Read DRFMT
try:
    drfmt_data = []
    with open(f'{INPUT_DIR}DRFMT', 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 3:
                branch1 = int(parts[1]) if parts[1].strip().isdigit() else 0
                fmlob = parts[2].strip()
                drfmt_data.append({'BRANCH1': branch1, 'FMLOB': fmlob})
    df_drfmt = pl.DataFrame(drfmt_data)
    print(f"  ✓ DRFMT: {len(df_drfmt):,}")
except:
    df_drfmt = pl.DataFrame([])

# Split PCCC
df_npl = df_npl.with_columns([pl.col('BRANCH').alias('BRANCH1')])
df_npl1 = df_npl.filter(pl.col('LOB') == 'PCCC')
df_npl2 = df_npl.filter(pl.col('LOB') != 'PCCC')

if len(df_npl1) > 0 and len(df_drfmt) > 0:
    df_npl1 = df_npl1.drop(['LOB']).join(df_drfmt, on='BRANCH1', how='left')
    df_npl1 = df_npl1.rename({'FMLOB': 'LOB'})

df_npl = pl.concat([df_npl1, df_npl2]) if len(df_npl1) > 0 or len(df_npl2) > 0 else df_npl2

# Handle HPOP
df_npl = df_npl.with_columns([
    pl.when((pl.col('BRANCX').str.slice(0, 1) == 'H') & (pl.col('PRODUCX') == 'LSTAFF'))
      .then(pl.lit('HPOP'))
      .otherwise(pl.col('LOB'))
      .alias('LOB')
])

# Summarize
print("\nSummarizing Islamic NPL data...")
df_nplw = df_npl.group_by(['BRANCX', 'PRODUCX', 'LOB']).agg([
    pl.col('AMOUNT').sum().alias('AMOUNT'),
    pl.col('NOACC').sum().alias('NOACC')
])

df_npls = df_nplw.with_columns([
    pl.lit(ym).alias('YM'),
    pl.when(pl.col('BRANCX').str.slice(0, 1) == 'A')
      .then(pl.col('BRANCX').str.slice(1, 3))
      .otherwise(pl.col('BRANCX'))
      .alias('BRANCX')
])

print(f"  ✓ Islamic NPL summary: {len(df_npls):,} records")

# Generate NPLC (Count - PIBB)
print("\nGenerating NPLC (count file - PIBB)...")
totcnt = df_npls['NOACC'].sum()
lcnt = len(df_npls)

with open(f'{OUTPUT_DIR}NPLC', 'w') as f:
    for row in df_npls.iter_rows(named=True):
        line = (
            f"D,PIBB{'':64}C_NPL{'':197}{row['YM']}{row['NOACC']:>15}"
            f"{row['LOB']:>4}{row['PRODUCX']:>6}EXT{'':65}ACTUAL{'':65}"
            f"{row['BRANCX']:>4},{'':66},{'':32},MYR{'':32},"
            f"{'':32},{'':32},{'':32},Y,{'':32},{'':32},{'':32},;"
        )
        f.write(line + '\n')
    f.write(f"T,{lcnt:>10},{totcnt:>15},;\n")

print(f"  ✓ NPLC (PIBB): {lcnt:,} records, Total: {totcnt:,}")

# Generate NPLA (Amount - PIBB)
print("\nGenerating NPLA (amount file - PIBB)...")
totamt = df_npls['AMOUNT'].sum()

with open(f'{OUTPUT_DIR}NPLA', 'w') as f:
    for row in df_npls.iter_rows(named=True):
        line = (
            f"D,PIBB{'':64}B_NPL{'':197}{row['YM']}{row['AMOUNT']:>15.2f}"
            f"{row['LOB']:>4}{row['PRODUCX']:>6}EXT{'':65}ACTUAL{'':65}"
            f"{row['BRANCX']:>4},{'':66},{'':32},MYR{'':32},"
            f"{'':32},{'':32},{'':32},Y,{'':32},{'':32},{'':32},;"
        )
        f.write(line + '\n')
    f.write(f"T,{lcnt:>10},{totamt:>15.2f},;\n")

print(f"  ✓ NPLA (PIBB): {lcnt:,} records, Total: RM {totamt:,.2f}")

# Generate NPLV (Average - PIBB)
print("\nGenerating NPLV (average file - PIBB)...")

df_nplv = df_nplw.join(df_xnpl, on=['BRANCX', 'PRODUCX', 'LOB'], how='outer')

df_nplv = df_nplv.with_columns([
    pl.col('PAMOUNT').fill_null(0),
    pl.col('AMOUNT').fill_null(0)
])

df_nplv = df_nplv.with_columns([
    ((pl.col('AMOUNT') + pl.col('PAMOUNT')) / 2).round(2).alias('AVGAMR'),
    pl.lit(ym).alias('YM'),
    pl.when(pl.col('BRANCX').str.slice(0, 1) == 'A')
      .then(pl.col('BRANCX').str.slice(1, 3))
      .otherwise(pl.col('BRANCX'))
      .alias('BRANCX')
])

tavg = df_nplv['AVGAMR'].sum()
lcnt_v = len(df_nplv)

with open(f'{OUTPUT_DIR}NPLV', 'w') as f:
    for row in df_nplv.iter_rows(named=True):
        # AVG
        line1 = (
            f"D,PIBB{'':64}B_NPL{'':197}{row['YM']}{row['AVGAMR']:>15.2f}"
            f"{row['LOB']:>4}{row['PRODUCX']:>6}EXT{'':65}AVG{'':65}"
            f"{row['BRANCX']:>4},{'':66},{'':32},MYR{'':32},"
            f"{'':32},{'':32},{'':32},Y,{'':32},{'':32},{'':32},;"
        )
        f.write(line1 + '\n')
        
        # ACCM_AVG
        line2 = (
            f"D,PIBB{'':64}B_NPL{'':197}{row['YM']}{row['AVGAMR']:>15.2f}"
            f"{row['LOB']:>4}{row['PRODUCX']:>6}EXT{'':65}ACCM_AVG{'':65}"
            f"{row['BRANCX']:>4},{'':66},{'':32},MYR{'':32},"
            f"{'':32},{'':32},{'':32},N,{'':32},{'':32},{'':32},;"
        )
        f.write(line2 + '\n')
    
    f.write(f"T,{lcnt_v:>10},{tavg:>15.2f},;\n")

print(f"  ✓ NPLV (PIBB): {lcnt_v:,} records, Total: RM {tavg:,.2f}")

print(f"\n{'='*60}")
print(f"✓ EIIM1503 Complete!")
print(f"{'='*60}")
print(f"\nIslamic NPL FM Report Summary:")
print(f"  Period: {rmonth}/{ryear}")
print(f"  Entity: PIBB (Islamic Banking)")
print(f"\nOutput Files:")
print(f"  NPLC (Count):   {lcnt:,} records, Total: {totcnt:,}")
print(f"  NPLA (Amount):  {lcnt:,} records, Total: RM {totamt:,.2f}")
print(f"  NPLV (Average): {lcnt_v:,} records, Total: RM {tavg:,.2f}")
print(f"\nKey Differences from EIMB1503 (PBB):")
print(f"  Entity: PIBB (Islamic) vs PBB (Conventional)")
print(f"  TF Range: 2580000000-2589999999 (Islamic) vs 2500000000-2599999999")
print(f"  Same logic: LOB mapping, PCCC/HPOP handling, average calculation")
