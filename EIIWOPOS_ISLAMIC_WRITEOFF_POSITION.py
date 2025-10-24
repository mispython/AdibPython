"""
EIIWOPOS - Islamic Write-Off Position Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("🚀 Starting EIIWOPOS - Islamic Write-Off Position Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/WOPS/output'
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

nowk = wk
nowk1 = wk1
reptday = reptdate.strftime('%d')
reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')

# Get WDAY from BNM1.REPTDATE (equivalent to second DATA step)
try:
    wday_df = pl.read_parquet(f'{BASE_PATH}/BNM1/REPTDATE/*.parquet')
    wday = wday_df[0,0].strftime('%d')  # Get first record's day
except:
    wday = reptday  # Fallback to current day

print(f"📅 Processing date: {reptdate.strftime('%Y-%m-%d')} (Week {nowk}, WDAY: {wday})")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # ISLAMIC LOAN WRITE-OFF POSITION
    # ========================================
    print("📊 Processing Islamic Loan Write-Off Position...")
    
    # Read and sort ILNWOF data
    ilnwof_query = """
    SELECT 
        ACCTNO, NOTENO, 
        CENSUS_TRT as ORIPRODUCT,
        WRITE_DOWN_BAL as WDB,
        NBDR
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/LNWOF/ILNWOF/*.parquet')
    ORDER BY ACCTNO, NOTENO
    """
    ilnwof_df = conn.execute(ilnwof_query).pl()
    
    # Read and sort IWOMOVE data (get latest BDR_AFT_PAY)
    iwomove_query = """
    SELECT 
        ACCTNO, NOTENO, BDR_AFT_PAY
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/WOMV/IWOMOVE/*.parquet')
    ORDER BY ACCTNO, NOTENO, BDR_AFT_PAY DESC
    """
    iwomove_df = conn.execute(iwomove_query).pl()
    
    # Remove duplicates (equivalent to NODUPKEY)
    iwomove_dedup = iwomove_df.unique(subset=['ACCTNO', 'NOTENO'])
    
    # Merge ILNWOF with IWOMOVE (equivalent to SAS MERGE)
    ilnwof_merged = ilnwof_df.join(
        iwomove_dedup.rename({'BDR_AFT_PAY': 'BDR'}), 
        on=['ACCTNO', 'NOTENO'], 
        how='left'
    )
    
    # Replace missing BDR with NBDR (equivalent to SAS IF BDR=. THEN BDR=NBDR)
    ilnwof_merged = ilnwof_merged.with_columns(
        pl.coalesce(pl.col('BDR'), pl.col('NBDR')).alias('BDR')
    )
    
    # Read LOAN data
    loan_query = f"""
    SELECT ACCTNO, NOTENO, ORIBALANCE, BRANCH, VB
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM/LOAN{reptmon}{reptday}/*.parquet')
    ORDER BY ACCTNO, NOTENO
    """
    loan_df = conn.execute(loan_query).pl()
    
    # Read LNNOTE data
    lnnote_query = """
    SELECT 
        ACCTNO, NOTENO, VB, DIGITAL_RR_STATUS_DT,
        DIGITAL_RR_STATUS_CD, REPAY_PROPOSAL_CD, AKPK_STATUS,
        CLIMATE_PRIN_TAXONOMY_CLASS
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM2/LNNOTE/*.parquet')
    ORDER BY ACCTNO, NOTENO
    """
    lnnote_df = conn.execute(lnnote_query).pl()
    
    # Merge all Islamic loan data
    ilnwof_final = ilnwof_merged.join(loan_df, on=['ACCTNO', 'NOTENO'], how='left')
    ilnwof_final = ilnwof_final.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Process Islamic loan position data (equivalent to DATA ILNWOPS)
    ilnwops_df = ilnwof_final.with_columns([
        pl.col('ORIPRODUCT').alias('CENSUS0'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(0, 2).alias('CENSUS1'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(2, 1).alias('CENSUS3'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(3, 1).alias('CENSUS4'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(5, 2).alias('CENSUS5'),
        pl.when(pl.col('WDB') <= 2.00).then(pl.lit('W')).otherwise(pl.lit('P')).alias('ACSTATUS'),
        pl.col('ORIBALANCE').alias('ACTOWE')
    ]).select([
        'ACCTNO', 'NOTENO', 'PRODUCT', 'ORIPRODUCT', 'WDB', 'BDR', 'ACTOWE', 'BRANCH', 'VB',
        'DIGITAL_RR_STATUS_DT', 'DIGITAL_RR_STATUS_CD', 'REPAY_PROPOSAL_CD', 'AKPK_STATUS',
        'REFNOTENO', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'CENSUS0', 'CENSUS1', 'CENSUS3', 'CENSUS4', 'CENSUS5', 'ACSTATUS'
    ])
    
    print(f"📄 Islamic loan positions: {len(ilnwops_df)}")

    # ========================================
    # ISLAMIC OVERDRAFT WRITE-OFF POSITION
    # ========================================
    print("📊 Processing Islamic Overdraft Write-Off Position...")
    
    # Read IODWOF data
    iodwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB,
        ACSTAT as ACSTATUS,
        CUMWOSP, CUMWOIS, CUMNAI, BDR, CUMRC, CUMSC,
        PRODUCT, ORIPRODUCT, WOTYPE, WOSTAT
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/ODWOF/IODWOF/*.parquet')
    ORDER BY ACCTNO
    """
    iodwof_df = conn.execute(iodwof_query).pl()
    
    # Read OD loan data
    od_loan_query = f"""
    SELECT ACCTNO, ORIBALANCE, BRANCH, VB
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM/LOAN{reptmon}{reptday}/*.parquet')
    ORDER BY ACCTNO
    """
    od_loan_df = conn.execute(od_loan_query).pl()
    
    # Merge Islamic OD data
    iodwof_merged = iodwof_df.join(od_loan_df, on='ACCTNO', how='left')
    
    # Process Islamic OD position data (equivalent to DATA IODWOPS)
    iodwops_df = iodwof_merged.with_columns([
        pl.col('ORIBALANCE').alias('ACTOWE'),
        pl.col('CUMWOSP').alias('CUMWOSP_OD'),
        pl.col('CUMWOIS').alias('CUMWOIS_OD'),
        pl.col('CUMNAI').alias('CUMNAI_OD'),
        pl.col('CUMRC').alias('CUMRC_OD'),
        pl.col('CUMSC').alias('CUMSC_OD')
    ]).select([
        'ACCTNO', 'CUMWOSP_OD', 'CUMWOIS_OD', 'WDB', 'CUMNAI_OD', 'BDR', 'CUMRC_OD', 'CUMSC_OD',
        'PRODUCT', 'ORIPRODUCT', 'ACTOWE', 'WOTYPE', 'WOSTAT', 'ACSTATUS', 'BRANCH', 'VB'
    ])
    
    print(f"📄 Islamic OD positions: {len(iodwops_df)}")

    # ========================================
    # COMBINE ISLAMIC LOAN AND OD POSITIONS
    # ========================================
    print("🔗 Combining Islamic Loan and OD positions...")
    
    # Add NOTENO to OD data for consistent structure
    iodwops_with_noteno = iodwops_df.with_columns(pl.lit(None).cast(pl.Utf8).alias('NOTENO'))
    
    # Select common columns for union
    common_columns = ['ACCTNO', 'NOTENO', 'PRODUCT', 'ORIPRODUCT', 'WDB', 'BDR', 'ACTOWE', 'BRANCH', 'VB', 'ACSTATUS']
    
    iln_selected = ilnwops_df.select(common_columns)
    iod_selected = iodwops_with_noteno.select(common_columns)
    
    # Combine data (equivalent to SAS SET)
    iwopos_df = pl.concat([iln_selected, iod_selected])
    iwopos_df = iwopos_df.sort(['ACCTNO', 'NOTENO'])
    
    # Save combined Islamic positions
    iwopos_file = f"{OUTPUT_DIR}/IWOPOS_{reptmon}{nowk}_{reptyear}.parquet"
    iwopos_df.write_parquet(iwopos_file)
    print(f"💾 Saved Islamic positions: {iwopos_file}")

    # ========================================
    # INCORPORATE IHPWO DATA (equivalent to %APPEND macro)
    # ========================================
    if reptday == wday:
        print("📥 Incorporating Islamic HPWO data...")
        
        try:
            # Read IHPWO data
            ihpwo_df = pl.read_parquet(f'{BASE_PATH}/HP/IHPWO_{reptmon}{nowk}{reptyear}/*.parquet')
            ihpwo_selected = ihpwo_df.select([
                'ACCTNO', 'NOTENO', 'BALANCE', 'PRODUCT', 'CENSUS0', 
                'REFNOTENO', 'CENSUS1', 'CENSUS3', 'CENSUS4', 'CENSUS5', 'BRANCH', 'VB'
            ])
            
            # Merge with existing Islamic positions (equivalent to SAS MERGE)
            iwopos_updated = iwopos_df.join(
                ihpwo_selected.rename({'BALANCE': 'ACTOWE_HP'}), 
                on=['ACCTNO', 'NOTENO'], 
                how='left'
            )
            
            # Update ACTOWE with IHPWO value where available
            iwopos_updated = iwopos_updated.with_columns(
                pl.coalesce(pl.col('ACTOWE_HP'), pl.col('ACTOWE')).alias('ACTOWE')
            ).select(iwopos_df.columns)  # Keep original columns
            
            # Save updated Islamic positions
            iwopos_updated.write_parquet(iwopos_file)
            print(f"💾 Updated with IHPWO data: {iwopos_file}")
            
        except Exception as e:
            print(f"⚠ IHPWO data not available: {e}")

    # ========================================
    # SAVE FULL DETAILED OUTPUTS
    # ========================================
    print("💾 Saving detailed outputs...")
    
    # Save detailed Islamic loan positions
    ilnwops_file = f"{OUTPUT_DIR}/ILNWOPS_{reptmon}{nowk}_{reptyear}.parquet"
    ilnwops_df.write_parquet(ilnwops_file)
    print(f"💾 Islamic loan details: {ilnwops_file}")
    
    # Save detailed Islamic OD positions
    iodwops_file = f"{OUTPUT_DIR}/IODWOPS_{reptmon}{nowk}_{reptyear}.parquet"
    iodwops_df.write_parquet(iodwops_file)
    print(f"💾 Islamic OD details: {iodwops_file}")

    # ========================================
    # SAVE TEMPORARY OUTPUT
    # ========================================
    temp_file = f"{OUTPUT_DIR}/IWOPOS_{reptmon}{nowk}_{reptyear}_TEMP.parquet"
    iwopos_df.write_parquet(temp_file)
    print(f"💾 Temporary file: {temp_file}")

    # ========================================
    # GENERATE SUMMARY STATISTICS
    # ========================================
    print("📈 Generating summary statistics...")
    
    summary_stats = pl.DataFrame({
        'metric': ['Total_Records', 'Loan_Records', 'OD_Records', 'Write_Off_Accounts', 'Performing_Accounts'],
        'count': [
            len(iwopos_df),
            len(ilnwops_df),
            len(iodwops_df),
            iwopos_df.filter(pl.col('ACSTATUS') == 'W').height,
            iwopos_df.filter(pl.col('ACSTATUS') == 'P').height
        ],
        'week': [nowk] * 5,
        'month': [reptmon] * 5,
        'year': [reptyear] * 5
    })
    
    summary_file = f"{OUTPUT_DIR}/IWOPOS_SUMMARY_{reptmon}{nowk}_{reptyear}.parquet"
    summary_stats.write_parquet(summary_file)
    print(f"💾 Summary statistics: {summary_file}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIIWOPOS Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Islamic Loan Positions: {len(ilnwops_df)}")
    print(f"📄 Islamic OD Positions: {len(iodwops_df)}")
    print(f"📄 Total Islamic Positions: {len(iwopos_df)}")
    print(f"📊 Write-Off Accounts: {iwopos_df.filter(pl.col('ACSTATUS') == 'W').height}")
    print(f"📊 Performing Accounts: {iwopos_df.filter(pl.col('ACSTATUS') == 'P').height}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
