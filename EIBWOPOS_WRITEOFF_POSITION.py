"""
EIBWOPOS - Write-Off Position Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("🚀 Starting EIBWOPOS - Write-Off Position Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/WOPS/output'
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
    # LOAN WRITE-OFF POSITION
    # ========================================
    print("📊 Processing Loan Write-Off Position...")
    
    # Read and sort LNWOF data
    lnwof_query = """
    SELECT 
        ACCTNO, NOTENO, 
        CENSUS_TRT as ORIPRODUCT,
        WRITE_DOWN_BAL as WDB,
        NBDR
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/LNWOF/LNWOF/*.parquet')
    ORDER BY ACCTNO, NOTENO
    """
    lnwof_df = conn.execute(lnwof_query).pl()
    
    # Read and sort WOMOVE data (get latest BDR_AFT_PAY)
    womove_query = """
    SELECT 
        ACCTNO, NOTENO, BDR_AFT_PAY
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/WOMV/WOMOVE/*.parquet')
    ORDER BY ACCTNO, NOTENO, BDR_AFT_PAY DESC
    """
    womove_df = conn.execute(womove_query).pl()
    
    # Remove duplicates (equivalent to NODUPKEY)
    womove_dedup = womove_df.unique(subset=['ACCTNO', 'NOTENO'])
    
    # Merge LNWOF with WOMOVE (equivalent to SAS MERGE)
    lnwof_merged = lnwof_df.join(
        womove_dedup.rename({'BDR_AFT_PAY': 'BDR'}), 
        on=['ACCTNO', 'NOTENO'], 
        how='left'
    )
    
    # Replace missing BDR with NBDR (equivalent to SAS IF BDR=. THEN BDR=NBDR)
    lnwof_merged = lnwof_merged.with_columns(
        pl.coalesce(pl.col('BDR'), pl.col('NBDR')).alias('BDR')
    )
    
    # Read LOAN data
    loan_query = f"""
    SELECT ACCTNO, NOTENO, ORIBALANCE, BRANCH
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
    
    # Merge all loan data
    lnwof_final = lnwof_merged.join(loan_df, on=['ACCTNO', 'NOTENO'], how='left')
    lnwof_final = lnwof_final.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Process loan position data (equivalent to DATA LNWOPS)
    lnwops_df = lnwof_final.with_columns([
        pl.col('ORIPRODUCT').alias('CENSUS0'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(0, 2).alias('CENSUS1'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(2, 1).alias('CENSUS3'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(3, 1).alias('CENSUS4'),
        pl.col('ORIPRODUCT').cast(pl.Utf8).str.slice(5, 2).alias('CENSUS5'),
        pl.when(pl.col('WDB') <= 2.00).then(pl.lit('W')).otherwise(pl.lit('P')).alias('ACSTATUS'),
        pl.col('ORIBALANCE').alias('ACTOWE')
    ])
    
    print(f"📄 Loan positions: {len(lnwops_df)}")

    # ========================================
    # OVERDRAFT WRITE-OFF POSITION
    # ========================================
    print("📊 Processing Overdraft Write-Off Position...")
    
    # Read ODWOF data
    odwof_query = """
    SELECT 
        ACCTNO,
        WRITE_DOWN_BAL as WDB,
        ACSTAT as ACSTATUS,
        CUMWOSP, CUMWOIS, CUMNAI, BDR, CUMRC, CUMSC,
        PRODUCT, ORIPRODUCT, WOTYPE, WOSTAT
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/ODWOF/ODWOF/*.parquet')
    ORDER BY ACCTNO
    """
    odwof_df = conn.execute(odwof_query).pl()
    
    # Read OD loan data
    od_loan_query = f"""
    SELECT ACCTNO, ORIBALANCE, BRANCH, VB
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/BNM/LOAN{reptmon}{reptday}/*.parquet')
    ORDER BY ACCTNO
    """
    od_loan_df = conn.execute(od_loan_query).pl()
    
    # Merge OD data
    odwof_merged = odwof_df.join(od_loan_df, on='ACCTNO', how='left')
    
    # Process OD position data (equivalent to DATA ODWOPS)
    odwops_df = odwof_merged.with_columns([
        pl.col('ORIBALANCE').alias('ACTOWE'),
        pl.col('CUMWOSP').alias('CUMWOSP_OD'),
        pl.col('CUMWOIS').alias('CUMWOIS_OD'),
        pl.col('CUMNAI').alias('CUMNAI_OD'),
        pl.col('CUMRC').alias('CUMRC_OD'),
        pl.col('CUMSC').alias('CUMSC_OD')
    ])
    
    print(f"📄 OD positions: {len(odwops_df)}")

    # ========================================
    # COMBINE LOAN AND OD POSITIONS
    # ========================================
    print("🔗 Combining Loan and OD positions...")
    
    # Add NOTENO to OD data for consistent structure
    odwops_with_noteno = odwops_df.with_columns(pl.lit(None).cast(pl.Utf8).alias('NOTENO'))
    
    # Select common columns for union
    common_columns = ['ACCTNO', 'NOTENO', 'PRODUCT', 'ORIPRODUCT', 'WDB', 'BDR', 'ACTOWE', 'BRANCH', 'VB', 'ACSTATUS']
    
    ln_selected = lnwops_df.select(common_columns)
    od_selected = odwops_with_noteno.select(common_columns)
    
    # Combine data (equivalent to SAS SET)
    wopos_df = pl.concat([ln_selected, od_selected])
    wopos_df = wopos_df.sort(['ACCTNO', 'NOTENO'])
    
    # Save combined positions
    wopos_file = f"{OUTPUT_DIR}/WOPOS{reptmon}{nowk}.parquet"
    wopos_df.write_parquet(wopos_file)
    print(f"💾 Saved combined positions: {wopos_file}")

    # ========================================
    # INCORPORATE HPWO DATA (equivalent to %APPEND macro)
    # ========================================
    if reptday == wday:
        print("📥 Incorporating HPWO data...")
        
        try:
            # Read HPWO data
            hpwo_df = pl.read_parquet(f'{BASE_PATH}/HP/HPWO{reptmon}{nowk}{reptyear}/*.parquet')
            hpwo_selected = hpwo_df.select([
                'ACCTNO', 'NOTENO', 'BALANCE', 'PRODUCT', 'CENSUS0', 
                'REFNOTENO', 'CENSUS1', 'CENSUS3', 'CENSUS4', 'CENSUS5', 'BRANCH', 'VB'
            ])
            
            # Merge with existing positions (equivalent to SAS MERGE)
            wopos_updated = wopos_df.join(
                hpwo_selected.rename({'BALANCE': 'ACTOWE_HP'}), 
                on=['ACCTNO', 'NOTENO'], 
                how='left'
            )
            
            # Update ACTOWE with HPWO value where available
            wopos_updated = wopos_updated.with_columns(
                pl.coalesce(pl.col('ACTOWE_HP'), pl.col('ACTOWE')).alias('ACTOWE')
            )
            
            # Save updated positions
            wopos_updated.write_parquet(wopos_file)
            print(f"💾 Updated with HPWO data: {wopos_file}")
            
        except Exception as e:
            print(f"⚠ HPWO data not available: {e}")

    # ========================================
    # SAVE TEMPORARY OUTPUT
    # ========================================
    temp_file = f"{OUTPUT_DIR}/WOPOS{reptmon}{nowk}{reptyear}.parquet"
    wopos_df.write_parquet(temp_file)
    print(f"💾 Saved temporary file: {temp_file}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWOPOS Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Loan Positions: {len(lnwops_df)}")
    print(f"📄 OD Positions: {len(odwops_df)}")
    print(f"📄 Total Positions: {len(wopos_df)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
