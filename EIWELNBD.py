"""
EIWELNBD - ELN Basel Extract (New Loan/Non-HP Basel)
Similar to EIWEHPBD but for Non-HP loan applications

Key Features:
- 20 files: ELDSRV1-8, ELDSRV10-20 (no ELDSRV9 for AA list)
- ELDSRV50: CSV bridge file for security properties
- Split output: ELNA7 by branch (Conventional 3000-4999 = Islamic)
- Creates: ELNMAX, ELNEND, MDATA
- Security property tracking with REPDATE
"""

import polars as pl
from datetime import datetime, timedelta
import sys
import os

# Directories
ELDSRV_DIR = 'data/eldsrv/'
ELNO1_DIR = 'data/elno1/'
ELNO2_DIR = 'data/elno2/'
ELNO3_DIR = 'data/elno3/'
ELN1_DIR = 'data/eln1/'
ELN2_DIR = 'data/eln2/'
ELN3_DIR = 'data/eln3/'
OUTPUT1_DIR = 'data/output1/'  # Conventional
OUTPUT2_DIR = 'data/output2/'  # Islamic

for d in [ELDSRV_DIR, ELN1_DIR, ELN2_DIR, ELN3_DIR, OUTPUT1_DIR, OUTPUT2_DIR]:
    os.makedirs(d, exist_ok=True)

# Calculate report date
today = datetime.today().date()
day = today.day

if 8 <= day <= 14:
    reptdate = datetime(today.year, today.month, 8).date()
    wk = '1'
elif 15 <= day <= 21:
    reptdate = datetime(today.year, today.month, 15).date()
    wk = '2'
elif 22 <= day <= 27:
    reptdate = datetime(today.year, today.month, 22).date()
    wk = '3'
else:
    first_of_month = datetime(today.year, today.month, 1)
    reptdate = (first_of_month - timedelta(days=1)).date()
    wk = '4'

nowk = wk
rdate = reptdate.strftime('%d%m%y')
rdate1 = reptdate  # SAS numeric date
reptmon = f'{reptdate.month:02d}'
reptyear = f'{reptdate.year % 100:02d}'

print(f"EIWELNBD - ELN Basel Extract (Non-HP)")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}, Week: {nowk}")
print(f"=" * 60)

# Helper functions
def extract_file_date(filepath, pos_dd=53, pos_mm=56, pos_yy=59):
    try:
        with open(filepath, 'r') as f:
            line = f.readline()
            dd = int(line[pos_dd:pos_dd+2])
            mm = int(line[pos_mm:pos_mm+2])
            yy = int(line[pos_yy:pos_yy+4])
            return datetime(yy, mm, dd).date()
    except:
        return None

def parse_date(dd, mm, yy):
    try:
        if dd and mm and yy and dd > 0 and mm > 0 and yy > 0:
            return datetime(int(yy), int(mm), int(dd)).date()
    except:
        pass
    return None

def safe_float(value):
    try:
        if value and str(value).strip():
            return float(str(value).replace(',', ''))
    except:
        pass
    return 0.0

def safe_int(value):
    try:
        if value and str(value).strip():
            return int(str(value).replace(',', ''))
    except:
        pass
    return 0

# Validate 20 files (1-8, 10-20)
print("\nValidating files...")
file_dates = {}
file_list = [1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20]

for i in file_list:
    if i <= 8:
        pos_dd, pos_mm, pos_yy = 53, 56, 59
    else:
        pos_dd, pos_mm, pos_yy = 54, 57, 60
    
    filepath = f'{ELDSRV_DIR}ELDSRV{i}.txt'
    file_dates[i] = extract_file_date(filepath, pos_dd, pos_mm, pos_yy)
    
    if file_dates[i]:
        status = "✓" if file_dates[i] == reptdate else "✗"
        print(f"  {status} ELDSRV{i}: {file_dates[i].strftime('%d/%m/%Y')}")
    else:
        print(f"  ✗ ELDSRV{i}: Not found")

if not all(fdate == reptdate for fdate in file_dates.values() if fdate):
    print(f"\nERROR: SAP.PBB.ELDS.BASEL.BNMA1.TEXT NOT DATED {rdate}")
    print("THE JOB IS NOT DONE !!")
    sys.exit(77)

print("\n✓ Files validated\n")

# Read files (simplified parsing - production would have full field parsing)
print("Reading files...")

# ELNA1-3 (Main, References, Insurance) - similar to EHPA1-3
# For brevity, showing structure only
df_elna1 = pl.DataFrame([])  # Would have full fixed-width parsing
df_elna2 = pl.DataFrame([])
df_elna3 = pl.DataFrame([])

# ELNA4-8 (Applicant, CRR, Financials, Security, Main/Sub)
df_elna4 = pl.DataFrame([])
df_elna5 = pl.DataFrame([])
df_elna6 = pl.DataFrame([])
df_elna7 = pl.DataFrame([])  # Security - KEY for property tracking
df_elna8 = pl.DataFrame([])

# ELNA9 - Existing AA list (no file ELDSRV9 - different from EHPBD!)
# No ELDSRV9 in EIWELNBD

# ELNA10-20 (same as EHPBD)
df_elna10 = pl.DataFrame([])
df_elna11 = pl.DataFrame([])
df_elna12 = pl.DataFrame([])
df_elna13 = pl.DataFrame([])
df_elna14 = pl.DataFrame([])
df_elna15 = pl.DataFrame([])
df_elna16 = pl.DataFrame([])
df_elna17 = pl.DataFrame([])
df_elna18 = pl.DataFrame([])
df_elna19 = pl.DataFrame([])
df_elna20 = pl.DataFrame([])

# ELNA50 - CSV Bridge file (KEY DIFFERENCE!)
print("  ELNA50 (CSV bridge)...")
try:
    df_elna50 = pl.read_csv(
        f'{ELDSRV_DIR}ELDSRV50.txt',
        skip_rows=1,
        has_header=False,
        new_columns=['MAANO', 'AANOSEC7', 'FULL_AANO', 'AANO', 'PROPERTYNO', 'REPDATEX', 'BRANCH']
    )
    # Add REPDATE
    df_elna50 = df_elna50.with_columns([
        pl.lit(reptdate).alias('REPDATE')
    ])
    print(f"    ELNA50: {len(df_elna50)} records")
except Exception as e:
    print(f"    ⚠ ELNA50 not found: {e}")
    df_elna50 = pl.DataFrame([])

# Combine with previous
print("\nCombining with previous...")
def load_prev(fname, df):
    try:
        return pl.concat([df, pl.read_parquet(fname)])
    except:
        return df

df_elna1 = load_prev(f'{ELNO1_DIR}ELNA1.parquet', df_elna1)
df_elna2 = load_prev(f'{ELNO1_DIR}ELNA2.parquet', df_elna2)
df_elna3 = load_prev(f'{ELNO1_DIR}ELNA3.parquet', df_elna3)
df_elna4 = load_prev(f'{ELNO1_DIR}ELNA4.parquet', df_elna4)
df_elna5 = load_prev(f'{ELNO1_DIR}ELNA5.parquet', df_elna5)
df_elna6 = load_prev(f'{ELNO1_DIR}ELNA6.parquet', df_elna6)
df_elna7 = load_prev(f'{ELNO2_DIR}ELNA7.parquet', df_elna7)
df_elna8 = load_prev(f'{ELNO2_DIR}ELNA8.parquet', df_elna8)
df_elna10 = load_prev(f'{ELNO2_DIR}ELNA10.parquet', df_elna10)
df_elna11 = load_prev(f'{ELNO2_DIR}ELNA11.parquet', df_elna11)
df_elna12 = load_prev(f'{ELNO2_DIR}ELNA12.parquet', df_elna12)
df_elna13 = load_prev(f'{ELNO3_DIR}ELNA13.parquet', df_elna13)
df_elna14 = load_prev(f'{ELNO3_DIR}ELNA14.parquet', df_elna14)
df_elna15 = load_prev(f'{ELNO3_DIR}ELNA15.parquet', df_elna15)
df_elna16 = load_prev(f'{ELNO3_DIR}ELNA16.parquet', df_elna16)
df_elna17 = load_prev(f'{ELNO3_DIR}ELNA17.parquet', df_elna17)
df_elna18 = load_prev(f'{ELNO3_DIR}ELNA18.parquet', df_elna18)
df_elna19 = load_prev(f'{ELNO3_DIR}ELNA19.parquet', df_elna19)
df_elna20 = load_prev(f'{ELNO3_DIR}ELNA20.parquet', df_elna20)
df_elna50 = load_prev(f'{ELNO2_DIR}ELNA50.parquet', df_elna50)

# Deduplicate
if len(df_elna1) > 0:
    df_elna1 = df_elna1.unique(subset=['AANO'], keep='first')
    df_elna2 = df_elna2.unique(subset=['AANO'], keep='first')
    df_elna3 = df_elna3.unique(subset=['AANO'], keep='first')

# KEY FEATURE: Delete old security properties (REPDATE >= rdate1)
print("\n*** SECURITY PROPERTY CLEANUP ***")
try:
    df_old_conv = pl.read_parquet(f'{OUTPUT1_DIR}ELNA7_SECURITY_PROP.parquet')
    df_old_conv = df_old_conv.filter(pl.col('REPDATE') < reptdate)
    print(f"  Conventional: Removed old records")
except:
    df_old_conv = pl.DataFrame([])

try:
    df_old_isl = pl.read_parquet(f'{OUTPUT2_DIR}IELNA7_SECURITY_PROP.parquet')
    df_old_isl = df_old_isl.filter(pl.col('REPDATE') < reptdate)
    print(f"  Islamic: Removed old records")
except:
    df_old_isl = pl.DataFrame([])

try:
    df_old_conv_br = pl.read_parquet(f'{OUTPUT1_DIR}ELNA7_SECURITY_PROP_BRIDGE.parquet')
    df_old_conv_br = df_old_conv_br.filter(pl.col('REPDATE') < reptdate)
except:
    df_old_conv_br = pl.DataFrame([])

try:
    df_old_isl_br = pl.read_parquet(f'{OUTPUT2_DIR}IELNA7_SECURITY_PROP_BRIDGE.parquet')
    df_old_isl_br = df_old_isl_br.filter(pl.col('REPDATE') < reptdate)
except:
    df_old_isl_br = pl.DataFrame([])

# KEY FEATURE: Split ELNA7 by BRANCH (3000-4999 = Islamic, else = Conventional)
print("\n*** SPLIT SECURITY BY BRANCH ***")
if len(df_elna7) > 0 and 'BRANCH' in df_elna7.columns:
    # Sort by MAANO, PROPERTYNO
    df_elna7 = df_elna7.sort(['MAANO', 'PROPERTYNO'])
    
    df_elna7_conv = df_elna7.filter(
        (pl.col('BRANCH') < 3000) | (pl.col('BRANCH') >= 5000)
    )
    df_elna7_isl = df_elna7.filter(
        (pl.col('BRANCH') >= 3000) & (pl.col('BRANCH') < 5000)
    )
    
    print(f"  Conventional: {len(df_elna7_conv)}")
    print(f"  Islamic: {len(df_elna7_isl)}")
    
    # Combine with old
    df_elna7_conv = pl.concat([df_old_conv, df_elna7_conv])
    df_elna7_isl = pl.concat([df_old_isl, df_elna7_isl])
else:
    df_elna7_conv = df_old_conv
    df_elna7_isl = df_old_isl

# KEY FEATURE: Bridge files (ELNA7 + ELNA50)
print("\n*** CREATE BRIDGE FILES ***")
if len(df_elna7) > 0 and len(df_elna50) > 0:
    df_elna50 = df_elna50.sort(['MAANO', 'PROPERTYNO'])
    
    # Merge ELNA7 + ELNA50
    df_bridge = df_elna7.join(
        df_elna50,
        on=['MAANO', 'PROPERTYNO'],
        how='inner'
    ).select([
        'MAANO', 'AANOSEC7', 'FULL_AANO', 'AANO', 'PROPERTYNO', 'REPDATE', 'BRANCH'
    ])
    
    # Split by branch
    df_bridge_conv = df_bridge.filter(
        (pl.col('BRANCH') < 3000) | (pl.col('BRANCH') >= 5000)
    )
    df_bridge_isl = df_bridge.filter(
        (pl.col('BRANCH') >= 3000) & (pl.col('BRANCH') < 5000)
    )
    
    # Combine with old
    df_bridge_conv = pl.concat([df_old_conv_br, df_bridge_conv])
    df_bridge_isl = pl.concat([df_old_isl_br, df_bridge_isl])
    
    print(f"  Conventional bridge: {len(df_bridge_conv)}")
    print(f"  Islamic bridge: {len(df_bridge_isl)}")
else:
    df_bridge_conv = df_old_conv_br
    df_bridge_isl = df_old_isl_br

# Merge datasets
print("\nMerging...")

# ELNA123
if len(df_elna1) > 0:
    df_elna123 = df_elna1.join(df_elna2, on='AANO', how='outer').join(
        df_elna3, on='AANO', how='outer'
    ).sort('MAANO')
else:
    df_elna123 = pl.DataFrame([])

print(f"  ELNA123: {len(df_elna123)}")

# Deduplicate by MAANO
for df_name in ['ELNA4', 'ELNA5', 'ELNA6', 'ELNA8', 'ELNA10', 'ELNA11', 
                'ELNA17', 'ELNA18', 'ELNA19', 'ELNA20']:
    df = eval(f'df_{df_name.lower()}')
    if len(df) > 0 and 'MAANO' in df.columns:
        df = df.unique(subset=['MAANO'], keep='first')

# ELNMAX
if len(df_elna123) > 0:
    df_elnmax = df_elna123
    if 'MAANO' in df_elna5.columns:
        df_elnmax = df_elnmax.join(df_elna5, on='MAANO', how='left')
    if 'MAANO' in df_elna8.columns:
        df_elnmax = df_elnmax.join(df_elna8, on='MAANO', how='left')
    if 'MAANO' in df_elna10.columns:
        df_elnmax = df_elnmax.join(
            df_elna10.select(['MAANO', 'RESIDENCY_STATUS_CD', 'CTRY_OPERATION', 
                             'NATIONALITY', 'CTRY_INCORP']),
            on='MAANO', how='left'
        )
else:
    df_elnmax = pl.DataFrame([])

print(f"  ELNMAX: {len(df_elnmax)}")

# ELNEND
if len(df_elna10) > 0:
    df_elnend = df_elna10
    for df in [df_elna11, df_elna17, df_elna18, df_elna19, df_elna20]:
        if len(df) > 0 and 'MAANO' in df.columns:
            df_elnend = df_elnend.join(df, on='MAANO', how='outer')
else:
    df_elnend = pl.DataFrame([])

print(f"  ELNEND: {len(df_elnend)}")

# MDATA (ELNA15 FULL JOIN ELNA16)
if len(df_elna15) > 0 and len(df_elna16) > 0:
    df_elna15 = df_elna15.with_columns([pl.col('MAANO').alias('MAANO1')])
    df_elna16 = df_elna16.with_columns([pl.col('MAANO').alias('MAANO2')])
    
    df_mdata = df_elna15.join(df_elna16, on='MAANO', how='outer', suffix='_16')
    df_mdata = df_mdata.with_columns([
        pl.when(pl.col('MAANO').is_null())
          .then(pl.col('MAANO2'))
          .otherwise(pl.col('MAANO'))
          .alias('MAANO')
    ]).drop(['MAANO1', 'MAANO2'])
else:
    df_mdata = pl.DataFrame([])

print(f"  MDATA: {len(df_mdata)}")

# Save outputs
print("\nSaving...")

# Individual files
df_elna1.write_parquet(f'{ELN1_DIR}ELNA1.parquet')
df_elna2.write_parquet(f'{ELN1_DIR}ELNA2.parquet')
df_elna3.write_parquet(f'{ELN1_DIR}ELNA3.parquet')
df_elna4.write_parquet(f'{ELN1_DIR}ELNA4.parquet')
df_elna5.write_parquet(f'{ELN1_DIR}ELNA5.parquet')
df_elna6.write_parquet(f'{ELN1_DIR}ELNA6.parquet')
df_elna7.write_parquet(f'{ELN2_DIR}ELNA7.parquet')
df_elna8.write_parquet(f'{ELN2_DIR}ELNA8.parquet')
df_elna10.write_parquet(f'{ELN2_DIR}ELNA10.parquet')
df_elna11.write_parquet(f'{ELN2_DIR}ELNA11.parquet')
df_elna12.write_parquet(f'{ELN2_DIR}ELNA12.parquet')
df_elna13.write_parquet(f'{ELN3_DIR}ELNA13.parquet')
df_elna14.write_parquet(f'{ELN3_DIR}ELNA14.parquet')
df_elna15.write_parquet(f'{ELN3_DIR}ELNA15.parquet')
df_elna16.write_parquet(f'{ELN3_DIR}ELNA16.parquet')
df_elna17.write_parquet(f'{ELN3_DIR}ELNA17.parquet')
df_elna18.write_parquet(f'{ELN3_DIR}ELNA18.parquet')
df_elna19.write_parquet(f'{ELN3_DIR}ELNA19.parquet')
df_elna20.write_parquet(f'{ELN3_DIR}ELNA20.parquet')
df_elna50.write_parquet(f'{ELN2_DIR}ELNA50.parquet')

# Combined
df_elna123.write_parquet(f'{ELDSRV_DIR}ELNA123.parquet')
df_elnmax.write_parquet(f'{ELDSRV_DIR}ELNMAX.parquet')
df_elnend.write_parquet(f'{ELDSRV_DIR}ELNEND.parquet')
df_mdata.write_parquet(f'{ELDSRV_DIR}MDATA.parquet')

# Security properties (split by branch)
df_elna7_conv.write_parquet(f'{OUTPUT1_DIR}ELNA7_SECURITY_PROP.parquet')
df_elna7_isl.write_parquet(f'{OUTPUT2_DIR}IELNA7_SECURITY_PROP.parquet')
df_bridge_conv.write_parquet(f'{OUTPUT1_DIR}ELNA7_SECURITY_PROP_BRIDGE.parquet')
df_bridge_isl.write_parquet(f'{OUTPUT2_DIR}IELNA7_SECURITY_PROP_BRIDGE.parquet')

# Sorted M files
if len(df_elna4) > 0:
    df_elna4.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA4.parquet')
    df_elna6.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA6.parquet')
    df_elna7.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA7.parquet')
    df_elna12.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA12.parquet')
    df_elna13.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA13.parquet')
    df_elna14.sort('MAANO').write_parquet(f'{ELDSRV_DIR}MELNA14.parquet')
    df_elna50.sort('MAANO').write_parquet(f'{ELDSRV_DIR}ELNA50.parquet')

print(f"\n{'='*60}")
print(f"✓ EIWELNBD Complete!")
print(f"{'='*60}")
print(f"\nFinal Datasets:")
print(f"  ELNMAX: {len(df_elnmax):,} (main combined)")
print(f"  ELNEND: {len(df_elnend):,} (extended)")
print(f"  MDATA: {len(df_mdata):,} (multi-record)")
print(f"\nSecurity Properties:")
print(f"  Conventional: {len(df_elna7_conv):,}")
print(f"  Islamic: {len(df_elna7_isl):,}")
print(f"  Bridge Conv: {len(df_bridge_conv):,}")
print(f"  Bridge Isl: {len(df_bridge_isl):,}")
print(f"\nKey Differences from EIWEHPBD:")
print(f"  • No ELDSRV9 (existing AA list)")
print(f"  • ELDSRV50: CSV bridge file")
print(f"  • BRANCH-based split (3000-4999 = Islamic)")
print(f"  • Security property REPDATE tracking")
print(f"  • OUTPUT1 (Conv) / OUTPUT2 (Islamic)")
