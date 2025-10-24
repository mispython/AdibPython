"""
EIBWOMOV - Write-Off Movement Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("🚀 Starting EIBWOMOV - Write-Off Movement Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/WOMV/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get reporting date and week (same logic as SAS)
reptdate = datetime.today()
day_of_month = reptdate.day

# Determine week (same logic as SAS SELECT/WHEN)
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
    # LOAN WRITE-OFF MOVEMENT
    # ========================================
    print("📊 Processing Loan Write-Off Movement...")
    
    # Sort and merge loan data (equivalent to PROC SORT and MERGE)
    loan_query = """
    WITH lnwomv AS (
        SELECT ACCTNO, NOTENO 
        FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/WOMOVE/WOMOVE/*.parquet')
    ),
    lnnote AS (
        SELECT ACCTNO, NOTENO, NTBRCH as BRANCH
        FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM/LNNOTE/*.parquet')
    )
    SELECT lnwomv.ACCTNO, lnwomv.NOTENO, lnnote.BRANCH
    FROM lnwomv
    INNER JOIN lnnote ON lnwomv.ACCTNO = lnnote.ACCTNO AND lnwomv.NOTENO = lnnote.NOTENO
    ORDER BY lnwomv.ACCTNO, lnwomv.NOTENO
    """
    
    lnwomv_df = conn.execute(loan_query).pl()
    print(f"📄 Loan records: {len(lnwomv_df)}")

    # ========================================
    # OVERDRAFT WRITE-OFF MOVEMENT  
    # ========================================
    print("📊 Processing Overdraft Write-Off Movement...")
    
    # Current OD data
    odwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB_AFT_PAY,
        BDR as BDR_AFT_PAY, 
        CUMNAI as NAI_AFT_PAY,
        CUMRC as RC_AFT_PAY,
        CUMSC as SC_AFT_PAY,
        ORIPRODUCT,
        ACCBRCH as BRANCH
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/ODWOF/ODWOF/*.parquet')
    """
    odwof_df = conn.execute(odwof_query).pl()
    
    # Previous OD data  
    lmwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB_BFR_PAY,
        BDR as BDR_BFR_PAY,
        CUMNAI as NAI_BFR_PAY, 
        CUMRC as RC_BFR_PAY,
        CUMSC as SC_BFR_PAY,
        ACCBRCH as BRANCH
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/LMWOF/ODWOF/*.parquet')
    """
    lmwof_df = conn.execute(lmwof_query).pl()
    
    # Merge and calculate movements (equivalent to SAS MERGE and calculations)
    odwomv_query = """
    SELECT 
        od.ACCTNO,
        od.WDB_AFT_PAY,
        (lm.WDB_BFR_PAY - od.WDB_AFT_PAY) as PAY_WDB,
        od.BDR_AFT_PAY,
        (od.BDR_AFT_PAY - lm.BDR_BFR_PAY) as PAY_BDR,
        (od.NAI_AFT_PAY - lm.NAI_BFR_PAY) as NAI,
        (od.RC_AFT_PAY - lm.RC_BFR_PAY) as RC,
        (od.SC_AFT_PAY - lm.SC_BFR_PAY) as SC,
        CAST('{reptdt}' as DATE) as TRANDATE,
        ((lm.WDB_BFR_PAY - od.WDB_AFT_PAY) + (od.BDR_AFT_PAY - lm.BDR_BFR_PAY)) as PAYMENT,
        lm.WDB_BFR_PAY,
        lm.BDR_BFR_PAY,
        od.ORIPRODUCT,
        COALESCE(od.BRANCH, lm.BRANCH) as BRANCH
    FROM odwof_df od
    INNER JOIN lmwof_df lm ON od.ACCTNO = lm.ACCTNO
    """.format(reptdt=reptdt)
    
    odwomv_df = conn.execute(odwomv_query).pl()
    print(f"📄 Overdraft records: {len(odwomv_df)}")

    # ========================================
    # COMBINE LOAN AND OVERDRAFT DATA
    # ========================================
    print("🔗 Combining Loan and Overdraft data...")
    
    # Add NOTENO column to OD data for union
    odwomv_with_noteno = odwomv_df.with_columns(pl.lit(None).cast(pl.Utf8).alias('NOTENO'))
    
    # Select common columns for union
    ln_selected = lnwomv_df.select(['ACCTNO', 'NOTENO', 'BRANCH'])
    od_selected = odwomv_with_noteno.select(['ACCTNO', 'NOTENO', 'BRANCH'])
    
    # Combine data (equivalent to SAS SET)
    womove_df = pl.concat([ln_selected, od_selected])
    womove_df = womove_df.sort(['ACCTNO', 'NOTENO'])
    print(f"📄 Total combined records: {len(womove_df)}")

    # ========================================
    # WEEKLY PROCESSING
    # ========================================
    print("📅 Processing weekly data...")
    weekly_file = f"{OUTPUT_DIR}/WOMOVE{reptmon}{nowk}.parquet"
    
    if reptday in ['01', '09', '16', '23']:
        # Create new weekly file
        womove_df.write_parquet(weekly_file)
        print(f"💾 Created new weekly file: {weekly_file}")
    else:
        # Append to existing weekly file (delete current date first)
        if os.path.exists(weekly_file):
            existing_df = pl.read_parquet(weekly_file)
            # Remove records for current date (equivalent to SAS DELETE)
            existing_filtered = existing_df.filter(pl.col('TRANDATE') != reptdt)
            # Append new data (equivalent to PROC APPEND)
            combined_weekly = pl.concat([existing_filtered, womove_df])
            combined_weekly.write_parquet(weekly_file)
            print(f"💾 Updated weekly file: {weekly_file}")
        else:
            womove_df.write_parquet(weekly_file)
            print(f"💾 Created weekly file: {weekly_file}")

    # ========================================
    # MONTHLY PROCESSING  
    # ========================================
    print("📅 Processing monthly data...")
    monthly_file = f"{OUTPUT_DIR}/WOMOVE{reptmon}.parquet"
    
    if reptday == '01':
        # Create new monthly file
        womove_df.write_parquet(monthly_file)
        print(f"💾 Created new monthly file: {monthly_file}")
    else:
        # Append to existing monthly file
        if os.path.exists(monthly_file):
            existing_monthly = pl.read_parquet(monthly_file)
            # Remove records for current date
            existing_filtered = existing_monthly.filter(pl.col('TRANDATE') != reptdt)
            # Append new data
            combined_monthly = pl.concat([existing_filtered, womove_df])
            combined_monthly.write_parquet(monthly_file)
            print(f"💾 Updated monthly file: {monthly_file}")
        else:
            womove_df.write_parquet(monthly_file)
            print(f"💾 Created monthly file: {monthly_file}")

    # ========================================
    # MONTHLY SUMMARY
    # ========================================
    print("📈 Generating monthly summary...")
    
    if os.path.exists(monthly_file):
        monthly_df = pl.read_parquet(monthly_file)
        
        # Group by ACCTNO and NOTENO, sum specified columns (equivalent to PROC SUMMARY)
        summary_df = monthly_df.group_by(['ACCTNO', 'NOTENO']).agg([
            pl.col('PAY_BDR').sum().alias('BDR_MTH'),
            pl.col('SC').sum().alias('SC_MTH'),
            pl.col('RC').sum().alias('RC_MTH'), 
            pl.col('NAI').sum().alias('NAI_MTH')
        ])
        
        # Add account type (equivalent to SAS IF/THEN)
        summary_df = summary_df.with_columns(
            pl.when((pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999))
            .then(pl.lit('OD'))
            .otherwise(pl.lit('LN'))
            .alias('ACCTYPE')
        )
        
        # Save summary
        summary_file = f"{OUTPUT_DIR}/SUMOV{reptmon}.parquet"
        summary_df.write_parquet(summary_file)
        print(f"💾 Saved monthly summary: {summary_file}")

    # ========================================
    # TEMPORARY OUTPUT
    # ========================================
    temp_file = f"{OUTPUT_DIR}/WOMOVE{reptmon}{nowk}{reptyear}.parquet"
    womove_df.write_parquet(temp_file)
    print(f"💾 Saved temporary file: {temp_file}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWOMOV Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Loan Records: {len(lnwomv_df)}")
    print(f"📄 OD Records: {len(odwomv_df)}") 
    print(f"📄 Total Records: {len(womove_df)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
