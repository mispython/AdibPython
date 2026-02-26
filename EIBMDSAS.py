"""
EIBMDSAS - Dataset Cleanup/Purge Program (Production Ready)

Purpose:
- Clean up old datasets from various libraries
- Calculate retention dates based on current date
- Delete outdated monthly and yearly datasets
- Purge write-off datasets older than 7 months

Retention Periods:
- Standard datasets: 4 months back
- Delete trigger: 7 months back
- Extended delete: 12 months back
- Historical baseline: 7 years back

Libraries Cleaned:
- MTD/MTDI: Monthly deposit data (CA, SA, FD)
- MLN/MLNI: Monthly loan data
- LCR/LCRI: Liquidity Coverage Ratio (CMM, EQU)
- NSFR/NSFRI: Net Stable Funding Ratio (CMM, EQU)
- CCRIS: Credit reporting (4 datasets)
- LNCOMM: Loan commitment (DRAW)
- BASKET: NPL baselines (3 datasets)
- NPL/INPL: NPL data (CAP datasets)
- WOFF/WOFFI: Write-off data (all datasets > 7 months old)

Date Variables:
- REPTDATE: 4 months back (end of month)
- DELEDATE: 7 months back (end of month)
- DELXDATE: 12 months back (end of month)
- DEL7DATE: 7 years back (end of year)

Month/Year Suffixes:
- REPTMON/REPTYEAR: From REPTDATE (4 months back)
- REPTMONS/REPTYRS: From DELEDATE (7 months back)
- REPTMON1/REPTYR1: From DELXDATE (12 months back)
- REPTYR7: From DEL7DATE (7 years back)
"""

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import glob

# Directories
MTD_DIR = 'data/mtd/'
MTDI_DIR = 'data/mtdi/'
MLN_DIR = 'data/mln/'
MLNI_DIR = 'data/mlni/'
LCR_DIR = 'data/lcr/'
LCRI_DIR = 'data/lcri/'
NSFR_DIR = 'data/nsfr/'
NSFRI_DIR = 'data/nsfri/'
CCRIS_DIR = 'data/ccris/'
LNCOMM_DIR = 'data/lncomm/'
BASKET_DIR = 'data/basket/'
NPL_DIR = 'data/npl/'
INPL_DIR = 'data/inpl/'
WOFF_DIR = 'data/woff/'
WOFFI_DIR = 'data/woffi/'

print("EIBMDSAS - Dataset Cleanup/Purge Program")
print("=" * 60)

# Calculate retention dates
print("\nCalculating retention dates...")
today = datetime.today()

# REPTDATE: 4 months back, end of month
reptdate = today - relativedelta(months=4)
reptdate = reptdate.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)

# DELEDATE: 7 months back, end of month
deledate = today - relativedelta(months=7)
deledate = deledate.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)

# DELXDATE: 12 months back, end of month
delxdate = today - relativedelta(months=12)
delxdate = delxdate.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)

# DEL7DATE: 7 years back, end of year
del7date = today - relativedelta(years=7)
del7date = del7date.replace(month=12, day=31)

print(f"  Today:       {today.strftime('%d/%m/%Y')}")
print(f"  REPTDATE:    {reptdate.strftime('%d/%m/%Y')} (4 months back)")
print(f"  DELEDATE:    {deledate.strftime('%d/%m/%Y')} (7 months back)")
print(f"  DELXDATE:    {delxdate.strftime('%d/%m/%Y')} (12 months back)")
print(f"  DEL7DATE:    {del7date.strftime('%d/%m/%Y')} (7 years back)")

# Format variables
reptyear = f'{reptdate.year % 100:02d}'
reptmon = f'{reptdate.month:02d}'
reptyr1 = f'{delxdate.year % 100:02d}'
reptmon1 = f'{delxdate.month:02d}'
reptyrs = f'{deledate.year % 100:02d}'
reptmons = f'{deledate.month:02d}'
reptyr7 = f'{del7date.year % 100:02d}'

print(f"\nFormatted Suffixes:")
print(f"  REPTMON/REPTYEAR:   {reptmon}/{reptyear}")
print(f"  REPTMON1/REPTYR1:   {reptmon1}/{reptyr1}")
print(f"  REPTMONS/REPTYRS:   {reptmons}/{reptyrs}")
print(f"  REPTYR7:            {reptyr7}")

print("=" * 60)

# Helper function to delete datasets
def delete_datasets(library_dir, dataset_names, library_name):
    """Delete specified datasets from library"""
    if not os.path.exists(library_dir):
        print(f"  ⚠ {library_name}: Directory not found, skipping")
        return 0
    
    deleted_count = 0
    for dataset in dataset_names:
        # Support both .parquet and .sas7bdat extensions
        for ext in ['.parquet', '.sas7bdat']:
            filepath = os.path.join(library_dir, f'{dataset}{ext}')
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    print(f"    ✓ Deleted: {dataset}{ext}")
                    deleted_count += 1
                except Exception as e:
                    print(f"    ✗ Error deleting {dataset}{ext}: {e}")
    
    return deleted_count

# Helper function to delete old datasets by date
def delete_old_datasets(library_dir, cutoff_date, library_name):
    """Delete all datasets older than cutoff date"""
    if not os.path.exists(library_dir):
        print(f"  ⚠ {library_name}: Directory not found, skipping")
        return 0
    
    deleted_count = 0
    for ext in ['.parquet', '.sas7bdat']:
        pattern = os.path.join(library_dir, f'*{ext}')
        for filepath in glob.glob(pattern):
            try:
                # Get file creation time
                ctime = datetime.fromtimestamp(os.path.getctime(filepath))
                
                # Delete if older than cutoff
                if ctime < cutoff_date:
                    filename = os.path.basename(filepath)
                    os.remove(filepath)
                    print(f"    ✓ Deleted: {filename} (created {ctime.strftime('%d/%m/%Y')})")
                    deleted_count += 1
            except Exception as e:
                print(f"    ✗ Error processing {filepath}: {e}")
    
    return deleted_count

# Clean MTD library
print("\nCleaning MTD library (Conventional Deposits)...")
datasets = [f'CA{reptmon}', f'SA{reptmon}', f'FD{reptmon}']
count = delete_datasets(MTD_DIR, datasets, 'MTD')
print(f"  Total deleted: {count}")

# Clean MTDI library
print("\nCleaning MTDI library (Islamic Deposits)...")
datasets = [f'CA{reptmon}', f'SA{reptmon}', f'FD{reptmon}']
count = delete_datasets(MTDI_DIR, datasets, 'MTDI')
print(f"  Total deleted: {count}")

# Clean MLN library
print("\nCleaning MLN library (Conventional Loans)...")
datasets = [f'LOAN{reptmon}']
count = delete_datasets(MLN_DIR, datasets, 'MLN')
print(f"  Total deleted: {count}")

# Clean MLNI library
print("\nCleaning MLNI library (Islamic Loans)...")
datasets = [f'LOAN{reptmon}']
count = delete_datasets(MLNI_DIR, datasets, 'MLNI')
print(f"  Total deleted: {count}")

# Clean LCR library
print("\nCleaning LCR library (Liquidity Coverage Ratio)...")
datasets = [f'CMM{reptmon}', f'EQU{reptmon}']
count = delete_datasets(LCR_DIR, datasets, 'LCR')
print(f"  Total deleted: {count}")

# Clean LCRI library
print("\nCleaning LCRI library (LCR Islamic)...")
datasets = [f'CMM{reptmon}', f'EQU{reptmon}']
count = delete_datasets(LCRI_DIR, datasets, 'LCRI')
print(f"  Total deleted: {count}")

# Clean NSFR library
print("\nCleaning NSFR library (Net Stable Funding Ratio)...")
datasets = [f'CMM{reptmon}', f'EQU{reptmon}']
count = delete_datasets(NSFR_DIR, datasets, 'NSFR')
print(f"  Total deleted: {count}")

# Clean NSFRI library
print("\nCleaning NSFRI library (NSFR Islamic)...")
datasets = [f'CMM{reptmon}', f'EQU{reptmon}']
count = delete_datasets(NSFRI_DIR, datasets, 'NSFRI')
print(f"  Total deleted: {count}")

# Clean CCRIS library
print("\nCleaning CCRIS library (Credit Reporting)...")
datasets = [
    f'CREDMSUBAC{reptmon1}{reptyr1}',
    f'ICREDMSUBAC{reptmon1}{reptyr1}',
    f'IPROVSUBAC{reptmon1}{reptyr1}',
    f'PROVSUBAC{reptmon1}{reptyr1}'
]
count = delete_datasets(CCRIS_DIR, datasets, 'CCRIS')
print(f"  Total deleted: {count}")

# Clean LNCOMM library
print("\nCleaning LNCOMM library (Loan Commitment)...")
datasets = [f'DRAW{reptyrs}{reptmons}']
count = delete_datasets(LNCOMM_DIR, datasets, 'LNCOMM')
print(f"  Total deleted: {count}")

# Clean BASKET library
print("\nCleaning BASKET library (NPL Baselines)...")
datasets = [
    f'BASECAX{reptyr7}',
    f'BASECA{reptyr7}',
    f'BASENP{reptyr7}'
]
count = delete_datasets(BASKET_DIR, datasets, 'BASKET')
print(f"  Total deleted: {count}")

# Clean NPL library
print("\nCleaning NPL library (Non-Performing Loans)...")
datasets = [
    f'CAP_STAFF{reptmon}{reptyr7}',
    f'CAPOLD{reptmon}{reptyr7}',
    f'CAP{reptmon}{reptyr7}'
]
count = delete_datasets(NPL_DIR, datasets, 'NPL')
print(f"  Total deleted: {count}")

# Clean INPL library
print("\nCleaning INPL library (Islamic NPL)...")
datasets = [
    f'ICAP_STAFF{reptmon}{reptyr7}',
    f'ICAPOLD{reptmon}{reptyr7}',
    f'ICAP{reptmon}{reptyr7}'
]
count = delete_datasets(INPL_DIR, datasets, 'INPL')
print(f"  Total deleted: {count}")

# Clean WOFF library (write-off - all datasets older than DELEDATE)
print("\nCleaning WOFF library (Conventional Write-Off)...")
print(f"  Deleting datasets created before {deledate.strftime('%d/%m/%Y')}...")
count = delete_old_datasets(WOFF_DIR, deledate, 'WOFF')
# Also delete TEMPDS if exists
count += delete_datasets(WOFF_DIR, ['TEMPDS'], 'WOFF')
print(f"  Total deleted: {count}")

# Clean WOFFI library (Islamic write-off - all datasets older than DELEDATE)
print("\nCleaning WOFFI library (Islamic Write-Off)...")
print(f"  Deleting datasets created before {deledate.strftime('%d/%m/%Y')}...")
count = delete_old_datasets(WOFFI_DIR, deledate, 'WOFFI')
# Also delete TEMPDS if exists
count += delete_datasets(WOFFI_DIR, ['TEMPDS'], 'WOFFI')
print(f"  Total deleted: {count}")

print(f"\n{'='*60}")
print(f"✓ EIBMDSAS Complete!")
print(f"{'='*60}")
print(f"\nDataset Cleanup Summary:")
print(f"  Today's Date: {today.strftime('%d/%m/%Y')}")
print(f"\nRetention Periods Applied:")
print(f"  Standard cleanup:     4 months  (REPTDATE: {reptdate.strftime('%d/%m/%Y')})")
print(f"  Write-off cleanup:    7 months  (DELEDATE: {deledate.strftime('%d/%m/%Y')})")
print(f"  Extended cleanup:     12 months (DELXDATE: {delxdate.strftime('%d/%m/%Y')})")
print(f"  Historical baseline:  7 years   (DEL7DATE: {del7date.strftime('%d/%m/%Y')})")
print(f"\nLibraries Cleaned: 16")
print(f"  Deposits:    MTD, MTDI")
print(f"  Loans:       MLN, MLNI")
print(f"  Ratios:      LCR, LCRI, NSFR, NSFRI")
print(f"  Reporting:   CCRIS, LNCOMM")
print(f"  Monitoring:  BASKET, NPL, INPL")
print(f"  Write-Off:   WOFF, WOFFI")
print(f"\nDataset Types Deleted:")
print(f"  CA, SA, FD - Deposit accounts by type")
print(f"  LOAN - Loan portfolios")
print(f"  CMM, EQU - Ratio components")
print(f"  CREDMSUBAC, PROVSUBAC - Credit reporting")
print(f"  DRAW - Loan commitments")
print(f"  BASECA, BASENP - NPL baselines")
print(f"  CAP - Close attention/NPL data")
print(f"  TEMPDS - Temporary datasets")
print(f"\nCleanup completed successfully.")

import sys
sys.exit(0)
