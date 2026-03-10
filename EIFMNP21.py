"""
EIFMNP21 - NPL Consolidated Report (6+ Months NPL)

Purpose: Consolidated reporting of NPL provisions for accounts 6+ months overdue
- IIS (Interest in Suspense) movements
- SP1 (Specific Provision - Purchase Price less depreciation)
- SP2 (Specific Provision - Depreciated PP for unscheduled goods)

Note: Uses existing datasets from EIFMNP03 and EIFMNP06
"""

import polars as pl
from datetime import datetime
import os

# Directories
NPL_DIR = 'data/npl/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMNP21 - NPL Consolidated Report (6+ Months)")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{NPL_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Read IIS data
print("\n1. Reading IIS Data (Interest in Suspense)...")
try:
    df_iis = pl.read_parquet(f'{NPL_DIR}IIS.parquet')
    
    # Filter 6+ months (182+ days)
    df_iis = df_iis.filter(pl.col('DAYS') > 182)
    
    print(f"  ✓ IIS Records (6+ months): {len(df_iis):,}")

except Exception as e:
    print(f"  ✗ IIS data not found: {e}")
    df_iis = None

# Read SP1 data
print("\n2. Reading SP1 Data (PP less depreciation)...")
try:
    df_sp1 = pl.read_parquet(f'{NPL_DIR}SP1.parquet')
    
    # Filter 6+ months
    df_sp1 = df_sp1.filter(pl.col('DAYS') > 182)
    
    print(f"  ✓ SP1 Records (6+ months): {len(df_sp1):,}")

except Exception as e:
    print(f"  ✗ SP1 data not found: {e}")
    df_sp1 = None

# Read SP2 data
print("\n3. Reading SP2 Data (Depreciated PP for unscheduled)...")
try:
    df_sp2 = pl.read_parquet(f'{NPL_DIR}SP2.parquet')
    
    # Filter 6+ months
    df_sp2 = df_sp2.filter(pl.col('DAYS') > 182)
    
    print(f"  ✓ SP2 Records (6+ months): {len(df_sp2):,}")

except Exception as e:
    print(f"  ✗ SP2 data not found: {e}")
    df_sp2 = None

# Generate IIS Report
if df_iis is not None:
    print("\n4. Generating IIS Report (6+ Months)...")
    
    # Summary by branch
    df_iis_summary = df_iis.group_by('LOANTYP').agg([
        pl.count().alias('COUNT'),
        pl.col('CURBAL').sum(),
        pl.col('UHC').sum(),
        pl.col('NETBAL').sum(),
        pl.col('IISP').sum(),
        pl.col('SUSPEND').sum(),
        pl.col('RECOVER').sum(),
        pl.col('RECC').sum(),
        pl.col('IISPW').sum(),
        pl.col('IIS').sum(),
        pl.col('OIP').sum(),
        pl.col('OISUSP').sum(),
        pl.col('OIRECV').sum(),
        pl.col('OIRECC').sum(),
        pl.col('OIW').sum(),
        pl.col('OI').sum(),
        pl.col('TOTIIS').sum()
    ])
    
    df_iis_summary.write_csv(f'{OUTPUT_DIR}NPL_IIS_6M.csv')
    print(f"  ✓ Saved: NPL_IIS_6M.csv")
    
    # Display summary
    print(f"\n  IIS Summary (6+ Months):")
    for row in df_iis_summary.iter_rows(named=True):
        print(f"    {row['LOANTYP']}: {row['COUNT']:,} accounts")
        print(f"      Total IIS: RM {row['TOTIIS']:,.2f}")

# Generate SP1 Report
if df_sp1 is not None:
    print("\n5. Generating SP1 Report (6+ Months)...")
    
    # Summary by risk and branch
    df_sp1_risk = df_sp1.group_by(['LOANTYP', 'RISK']).agg([
        pl.count().alias('COUNT'),
        pl.col('CURBAL').sum(),
        pl.col('UHC').sum(),
        pl.col('NETBAL').sum(),
        pl.col('IIS').sum(),
        pl.col('OSPRIN').sum(),
        pl.col('MARKETVL').sum(),
        pl.col('NETEXP').sum(),
        pl.col('SPP1').sum(),
        pl.col('SPPL').sum(),
        pl.col('RECOVER').sum(),
        pl.col('SPPW').sum(),
        pl.col('SP').sum()
    ])
    
    # Summary by branch
    df_sp1_branch = df_sp1.group_by('LOANTYP').agg([
        pl.count().alias('COUNT'),
        pl.col('CURBAL').sum(),
        pl.col('UHC').sum(),
        pl.col('NETBAL').sum(),
        pl.col('IIS').sum(),
        pl.col('OSPRIN').sum(),
        pl.col('MARKETVL').sum(),
        pl.col('NETEXP').sum(),
        pl.col('SPP1').sum(),
        pl.col('SPPL').sum(),
        pl.col('RECOVER').sum(),
        pl.col('SPPW').sum(),
        pl.col('SP').sum()
    ])
    
    df_sp1_risk.write_csv(f'{OUTPUT_DIR}NPL_SP1_6M_RISK.csv')
    df_sp1_branch.write_csv(f'{OUTPUT_DIR}NPL_SP1_6M_BRANCH.csv')
    print(f"  ✓ Saved: NPL_SP1_6M_RISK.csv, NPL_SP1_6M_BRANCH.csv")
    
    # Display summary
    print(f"\n  SP1 Summary (6+ Months - PP less depreciation):")
    for row in df_sp1_branch.iter_rows(named=True):
        print(f"    {row['LOANTYP']}: {row['COUNT']:,} accounts")
        print(f"      SP: RM {row['SP']:,.2f}")

# Generate SP2 Report
if df_sp2 is not None:
    print("\n6. Generating SP2 Report (6+ Months)...")
    
    # Summary by risk and branch
    df_sp2_risk = df_sp2.group_by(['LOANTYP', 'RISK']).agg([
        pl.count().alias('COUNT'),
        pl.col('CURBAL').sum(),
        pl.col('UHC').sum(),
        pl.col('NETBAL').sum(),
        pl.col('IIS').sum(),
        pl.col('OSPRIN').sum(),
        pl.col('MARKETVL').sum(),
        pl.col('NETEXP').sum(),
        pl.col('SPP2').sum(),
        pl.col('SPPL').sum(),
        pl.col('RECOVER').sum(),
        pl.col('SPPW').sum(),
        pl.col('SP').sum()
    ])
    
    # Summary by branch
    df_sp2_branch = df_sp2.group_by('LOANTYP').agg([
        pl.count().alias('COUNT'),
        pl.col('CURBAL').sum(),
        pl.col('UHC').sum(),
        pl.col('NETBAL').sum(),
        pl.col('IIS').sum(),
        pl.col('OSPRIN').sum(),
        pl.col('MARKETVL').sum(),
        pl.col('NETEXP').sum(),
        pl.col('SPP2').sum(),
        pl.col('SPPL').sum(),
        pl.col('RECOVER').sum(),
        pl.col('SPPW').sum(),
        pl.col('SP').sum()
    ])
    
    df_sp2_risk.write_csv(f'{OUTPUT_DIR}NPL_SP2_6M_RISK.csv')
    df_sp2_branch.write_csv(f'{OUTPUT_DIR}NPL_SP2_6M_BRANCH.csv')
    print(f"  ✓ Saved: NPL_SP2_6M_RISK.csv, NPL_SP2_6M_BRANCH.csv")
    
    # Display summary
    print(f"\n  SP2 Summary (6+ Months - Depreciated PP):")
    for row in df_sp2_branch.iter_rows(named=True):
        print(f"    {row['LOANTYP']}: {row['COUNT']:,} accounts")
        print(f"      SP: RM {row['SP']:,.2f}")

print(f"\n{'='*60}")
print("✓ EIFMNP21 Complete")
print(f"{'='*60}")
print(f"\nPUBLIC BANK - (NPL FROM 6 MONTHS & ABOVE)")
print(f"CONSOLIDATED REPORTS AS AT {rdate}")
print(f"\nThree Reports Generated:")
print(f"\n1. MOVEMENTS OF INTEREST IN SUSPENSE")
print(f"   - Opening balance (IISP)")
print(f"   - Suspended (SUSPEND)")
print(f"   - Written back (RECOVER)")
print(f"   - Reversal (RECC)")
print(f"   - Written off (IISPW)")
print(f"   - Closing balance (IIS)")
print(f"   - Overdue Interest (OI)")
print(f"   - Total IIS (IIS + OI)")
print(f"\n2. SPECIFIC PROVISION (SP1 - PP less depreciation)")
print(f"   - Market value based on purchase price")
print(f"   - Straight-line depreciation")
print(f"   - Provision by risk category")
print(f"\n3. SPECIFIC PROVISION (SP2 - Depreciated PP unscheduled)")
print(f"   - Depreciated PP for unscheduled goods")
print(f"   - Alternative valuation method")
print(f"   - Provision by risk category")
print(f"\nCriteria:")
print(f"   NPL: 6+ months overdue (DAYS > 182)")
print(f"   Substandard-2, Doubtful, and Bad accounts")
print(f"\nOutputs:")
print(f"   - NPL_IIS_6M.csv")
print(f"   - NPL_SP1_6M_RISK.csv, NPL_SP1_6M_BRANCH.csv")
print(f"   - NPL_SP2_6M_RISK.csv, NPL_SP2_6M_BRANCH.csv")
