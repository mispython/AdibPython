"""
EIIWOMOV - Islamic Write-Off Movement Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("🚀 Starting EIIWOMOV - Islamic Write-Off Movement Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/WOMV/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================================
# DATE PROCESSING (equivalent to DATA DATES)
# ========================================
print("📅 Processing reporting dates...")

# Load REPTDATE from parquet
reptdate_df = pl.read_parquet(f'{BASE_PATH}/BNM/REPTDATE/*.parquet')
reptdate = reptdate_df['REPTDATE'][0]

# Determine week (same logic as SAS SELECT/WHEN)
day_of_month = reptdate.day

if 1 <= day_of_month <= 8:
    wk = '1'
    wk1 = '4'
elif 9 <= day_of_month <= 15:
    wk = '2'
    wk1 = '1'
elif 16 <= day_of_month <= 22:
    wk = '3'
    wk1 = '2'
else:
    wk = '4'
    wk1 = '3'

# Calculate previous month end (equivalent to MDY function)
if reptdate.month == 1:
    prevdate = datetime(reptdate.year - 1, 12, 31)
else:
    prevdate = datetime(reptdate.year, reptdate.month - 1, 1) - pl.duration(days=1)

nowk = wk
reptdt = reptdate.strftime('%Y-%m-%d')
reptday = reptdate.strftime('%d')
reptmon = reptdate.strftime('%m')
prevmon = prevdate.strftime('%m')
reptyear = reptdate.strftime('%y')

print(f"📅 Processing date: {reptdt} (Week {nowk})")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # ISLAMIC LOAN WRITE-OFF MOVEMENT
    # ========================================
    print("📊 Processing Islamic Loan Write-Off Movement...")
    
    # Sort and merge Islamic loan data
    ilnwomv_query = """
    WITH ilnwomv AS (
        SELECT ACCTNO, NOTENO 
        FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/WOMOVE/IWOMOVE/*.parquet')
    ),
    lnnote AS (
        SELECT ACCTNO, NOTENO, NTBRCH as BRANCH
        FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM/LNNOTE/*.parquet')
    )
    SELECT ilnwomv.ACCTNO, ilnwomv.NOTENO, lnnote.BRANCH
    FROM ilnwomv
    INNER JOIN lnnote ON ilnwomv.ACCTNO = lnnote.ACCTNO AND ilnwomv.NOTENO = lnnote.NOTENO
    ORDER BY ilnwomv.ACCTNO, ilnwomv.NOTENO
    """
    
    ilnwomv_df = conn.execute(ilnwomv_query).pl()
    print(f"📄 Islamic loan movement records: {len(ilnwomv_df)}")

    # ========================================
    # ISLAMIC OVERDRAFT WRITE-OFF MOVEMENT  
    # ========================================
    print("📊 Processing Islamic Overdraft Write-Off Movement...")
    
    # Current Islamic OD data
    iodwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB_AFT_PAY,
        BDR as BDR_AFT_PAY, 
        CUMNAI as NAI_AFT_PAY,
        CUMRC as RC_AFT_PAY,
        CUMSC as SC_AFT_PAY,
        ORIPRODUCT,
        ACCBRCH as BRANCH
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/ODWOF/IODWOF/*.parquet')
    """
    iodwof_df = conn.execute(iodwof_query).pl()
    
    # Previous Islamic OD data  
    ilmwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB_BFR_PAY,
        BDR as BDR_BFR_PAY,
        CUMNAI as NAI_BFR_PAY, 
        CUMRC as RC_BFR_PAY,
        CUMSC as SC_BFR_PAY,
        ACCBRCH as BRANCH
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/LMWOF/IODWOF/*.parquet')
    """
    ilmwof_df = conn.execute(ilmwof_query).pl()
    
    # Merge and calculate Islamic OD movements
    iodwomv_query = f"""
    SELECT 
        iod.ACCTNO,
        iod.WDB_AFT_PAY,
        (ilm.WDB_BFR_PAY - iod.WDB_AFT_PAY) as PAY_WDB,
        iod.BDR_AFT_PAY,
        (iod.BDR_AFT_PAY - ilm.BDR_BFR_PAY) as PAY_BDR,
        (iod.NAI_AFT_PAY - ilm.NAI_BFR_PAY) as NAI,
        (iod.RC_AFT_PAY - ilm.RC_BFR_PAY) as RC,
        (iod.SC_AFT_PAY - ilm.SC_BFR_PAY) as SC,
        CAST('{reptdt}' as DATE) as TRANDATE,
        ((ilm.WDB_BFR_PAY - iod.WDB_AFT_PAY) + (iod.BDR_AFT_PAY - ilm.BDR_BFR_PAY)) as PAYMENT,
        ilm.WDB_BFR_PAY,
        ilm.BDR_BFR_PAY,
        iod.ORIPRODUCT,
        COALESCE(iod.BRANCH, ilm.BRANCH) as BRANCH
    FROM iodwof_df iod
    INNER JOIN ilmwof_df ilm ON iod.ACCTNO = ilm.ACCTNO
    """
    
    iodwomv_df = conn.execute(iodwomv_query).pl()
    print(f"📄 Islamic OD movement records: {len(iodwomv_df)}")

    # ========================================
    # COMBINE ISLAMIC LOAN AND OD DATA
    # ========================================
    print("🔗 Combining Islamic Loan and OD movement data...")
    
    # Add NOTENO column to OD data for union
    iodwomv_with_noteno = iodwomv_df.with_columns(pl.lit(None).cast(pl.Utf8).alias('NOTENO'))
    
    # Select common columns for union
    iln_selected = ilnwomv_df.select(['ACCTNO', 'NOTENO', 'BRANCH'])
    iod_selected = iodwomv_with_noteno.select(['ACCTNO', 'NOTENO', 'BRANCH', 'TRANDATE', 'PAYMENT'])
    
    # Combine Islamic data
    iwomove_df = pl.concat([iln_selected, iod_selected])
    iwomove_df = iwomove_df.sort(['ACCTNO', 'NOTENO'])
    print(f"📄 Total Islamic movement records: {len(iwomove_df)}")

    # ========================================
    # WEEKLY PROCESSING
    # ========================================
    print("📅 Processing weekly Islamic data...")
    weekly_base = f"IWOMOVE_{reptmon}{nowk}_{reptyear}"
    weekly_parquet = f"{OUTPUT_DIR}/{weekly_base}.parquet"
    weekly_csv = f"{OUTPUT_DIR}/{weekly_base}.csv"
    
    if reptday in ['01', '09', '16', '23']:
        # Create new weekly file
        iwomove_df.write_parquet(weekly_parquet)
        iwomove_df.write_csv(weekly_csv)
        print(f"💾 Created new weekly Islamic file: {weekly_parquet}")
    else:
        # Append to existing weekly file
        if os.path.exists(weekly_parquet):
            existing_df = pl.read_parquet(weekly_parquet)
            # Remove records for current date
            existing_filtered = existing_df.filter(pl.col('TRANDATE') != reptdt)
            # Append new data
            combined_weekly = pl.concat([existing_filtered, iwomove_df])
            combined_weekly.write_parquet(weekly_parquet)
            combined_weekly.write_csv(weekly_csv)
            print(f"💾 Updated weekly Islamic file: {weekly_parquet}")
        else:
            iwomove_df.write_parquet(weekly_parquet)
            iwomove_df.write_csv(weekly_csv)
            print(f"💾 Created weekly Islamic file: {weekly_parquet}")

    # ========================================
    # MONTHLY PROCESSING  
    # ========================================
    print("📅 Processing monthly Islamic data...")
    monthly_base = f"IWOMOVE_{reptmon}_{reptyear}"
    monthly_parquet = f"{OUTPUT_DIR}/{monthly_base}.parquet"
    monthly_csv = f"{OUTPUT_DIR}/{monthly_base}.csv"
    
    if reptday == '01':
        # Create new monthly file
        iwomove_df.write_parquet(monthly_parquet)
        iwomove_df.write_csv(monthly_csv)
        print(f"💾 Created new monthly Islamic file: {monthly_parquet}")
    else:
        # Append to existing monthly file
        if os.path.exists(monthly_parquet):
            existing_monthly = pl.read_parquet(monthly_parquet)
            # Remove records for current date
            existing_filtered = existing_monthly.filter(pl.col('TRANDATE') != reptdt)
            # Append new data
            combined_monthly = pl.concat([existing_filtered, iwomove_df])
            combined_monthly.write_parquet(monthly_parquet)
            combined_monthly.write_csv(monthly_csv)
            print(f"💾 Updated monthly Islamic file: {monthly_parquet}")
        else:
            iwomove_df.write_parquet(monthly_parquet)
            iwomove_df.write_csv(monthly_csv)
            print(f"💾 Created monthly Islamic file: {monthly_parquet}")

    # ========================================
    # MONTHLY SUMMARY
    # ========================================
    print("📈 Generating monthly Islamic summary...")
    
    if os.path.exists(monthly_parquet):
        monthly_df = pl.read_parquet(monthly_parquet)
        
        # Group by ACCTNO and NOTENO, sum specified columns
        isumov_df = monthly_df.group_by(['ACCTNO', 'NOTENO']).agg([
            pl.col('PAY_BDR').sum().alias('BDR_MTH'),
            pl.col('SC').sum().alias('SC_MTH'),
            pl.col('RC').sum().alias('RC_MTH'), 
            pl.col('NAI').sum().alias('NAI_MTH')
        ])
        
        # Add account type
        isumov_df = isumov_df.with_columns(
            pl.when((pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999))
            .then(pl.lit('OD'))
            .otherwise(pl.lit('LN'))
            .alias('ACCTYPE')
        )
        
        # Save Islamic summary
        isumov_base = f"ISUMOV_{reptmon}_{reptyear}"
        isumov_parquet = f"{OUTPUT_DIR}/{isumov_base}.parquet"
        isumov_csv = f"{OUTPUT_DIR}/{isumov_base}.csv"
        
        isumov_df.write_parquet(isumov_parquet)
        isumov_df.write_csv(isumov_csv)
        print(f"💾 Islamic monthly summary: {isumov_parquet}")

    # ========================================
    # TEMPORARY OUTPUT
    # ========================================
    temp_base = f"IWOMOVE_{reptmon}{nowk}_{reptyear}_TEMP"
    temp_parquet = f"{OUTPUT_DIR}/{temp_base}.parquet"
    temp_csv = f"{OUTPUT_DIR}/{temp_base}.csv"
    
    iwomove_df.write_parquet(temp_parquet)
    iwomove_df.write_csv(temp_csv)
    print(f"💾 Temporary Islamic file: {temp_parquet}")

    # ========================================
    # GENERATE MOVEMENT ANALYSIS
    # ========================================
    print("📊 Generating Islamic movement analysis...")
    
    movement_analysis = pl.DataFrame({
        'category': ['Total_Records', 'Loan_Movements', 'OD_Movements', 
                    'Total_Payment', 'Avg_Payment', 'Unique_Accounts'],
        'value': [
            len(iwomove_df),
            len(ilnwomv_df),
            len(iodwomv_df),
            iodwomv_df['PAYMENT'].sum(),
            iodwomv_df['PAYMENT'].mean(),
            iwomove_df['ACCTNO'].n_unique()
        ],
        'week': [nowk] * 6,
        'month': [reptmon] * 6,
        'year': [reptyear] * 6,
        'product_type': ['ISLAMIC'] * 6
    })
    
    analysis_base = f"IWOMOVE_ANALYSIS_{reptmon}{nowk}_{reptyear}"
    analysis_parquet = f"{OUTPUT_DIR}/{analysis_base}.parquet"
    analysis_csv = f"{OUTPUT_DIR}/{analysis_base}.csv"
    
    movement_analysis.write_parquet(analysis_parquet)
    movement_analysis.write_csv(analysis_csv)
    print(f"💾 Islamic movement analysis: {analysis_parquet}")

    # ========================================
    # GENERATE METADATA
    # ========================================
    print("📄 Generating Islamic metadata...")
    
    metadata_data = {
        'parameter': ['PROCESS_NAME', 'REPORT_DATE', 'WEEK', 'MONTH', 'YEAR',
                     'ISLAMIC_LOAN_RECORDS', 'ISLAMIC_OD_RECORDS', 'TOTAL_RECORDS'],
        'value': [
            'EIIWOMOV', reptdt, nowk, reptmon, reptyear,
            str(len(ilnwomv_df)), str(len(iodwomv_df)), str(len(iwomove_df))
        ]
    }
    metadata_df = pl.DataFrame(metadata_data)
    
    metadata_base = f"IWOMOVE_METADATA_{reptmon}{nowk}_{reptyear}"
    metadata_parquet = f"{OUTPUT_DIR}/{metadata_base}.parquet"
    metadata_csv = f"{OUTPUT_DIR}/{metadata_base}.csv"
    
    metadata_df.write_parquet(metadata_parquet)
    metadata_df.write_csv(metadata_csv)
    print(f"💾 Islamic metadata: {metadata_parquet}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIIWOMOV Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Islamic Loan Movements: {len(ilnwomv_df)}")
    print(f"📄 Islamic OD Movements: {len(iodwomv_df)}")
    print(f"📄 Total Islamic Movements: {len(iwomove_df)}")
    print(f"💰 Islamic Total Payment: {iodwomv_df['PAYMENT'].sum():,.2f}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"📊 Files Generated: 14 files (7 Parquet + 7 CSV)")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
