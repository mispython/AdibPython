"""
EIBDFDCE - Fixed Deposit Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("🚀 Starting EIBDFDCE - Fixed Deposit Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/FIXEDCD/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get reporting date and week
reptdate = datetime.today()
day_of_month = reptdate.day

# Determine week (same logic as SAS)
if 1 <= day_of_month <= 8:
    nowk = '1'
elif 9 <= day_of_month <= 15:
    nowk = '2'
elif 16 <= day_of_month <= 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

print(f"📅 Processing date: {rdate} (Week {nowk})")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # Process Fixed Deposit data
    query = f"""
    WITH fixed_deposits AS (
        SELECT 
            BRANCH,
            ACCTNO,
            CDNO,
            STATEC,
            CUSTCD,
            OPENIND,
            CURBAL,
            ORGDATE,
            MATDATE,
            RATE,
            RENEWAL,
            INTPLAN,
            LASTACTV,
            DEPDATE,
            TERM,
            PURPOSE,
            ORIGAMT,
            INTPAY,
            LMATDATE,
            PAYMENT,
            INTTFRACCT,
            INTFREQ,
            INTFREQID,
            MATID,
            ACCTTYPE as PRODUCT,
            PENDINT as PEND_INTPLAN,
            PRN_DISP_OPT,
            PRN_RENEW,
            PRN_TFR_ACCT,
            
            -- Date conversions (equivalent to SAS INPUT functions)
            CASE 
                WHEN DEPODTE IS NOT NULL THEN 
                    strptime(substring(cast(DEPODTE as varchar), 1, 8), '%m%d%Y')
                ELSE NULL 
            END as DEPDATE_CONV,
            
            CASE 
                WHEN ORGDATE IS NOT NULL THEN 
                    strptime(substring(cast(ORGDATE as varchar), 1, 6), '%m%d%y')
                ELSE NULL 
            END as ORGDATE_CONV,
            
            CASE 
                WHEN MATDATE IS NOT NULL THEN 
                    strptime(substring(cast(MATDATE as varchar), 1, 8), '%Y%m%d')
                ELSE NULL 
            END as MATDATE_CONV,
            
            CASE 
                WHEN LASTACTV IS NOT NULL THEN 
                    strptime(substring(cast(LASTACTV as varchar), 1, 6), '%m%d%y')
                ELSE NULL 
            END as LASTACTV_CONV,
            
            -- Purpose transformation
            CASE 
                WHEN PURPOSE = '4' THEN '1'
                ELSE PURPOSE 
            END as PURPOSE_ADJ
            
        FROM read_parquet('{BASE_PATH}/DPDARPGS/FD/*.parquet')
        WHERE OPENIND IN ('D', 'O')
          AND NOT (
            (1590000000 <= ACCTNO AND ACCTNO <= 1599999999) OR
            (1689999999 <= ACCTNO AND ACCTNO <= 1699999999) OR
            (1789999999 <= ACCTNO AND ACCTNO <= 1799999999)
          )
    )
    SELECT * FROM fixed_deposits
    """
    
    # Execute query and get results as Polars DataFrame
    df = conn.execute(query).pl()
    
    if df.is_empty():
        print("⚠ No fixed deposit data found")
        exit(1)
    
    print(f"📊 Processed {len(df)} fixed deposit records")
    
    # Save to FIXEDCD output (equivalent to SAS OUTPUT)
    fixedcd_file = f"{OUTPUT_DIR}/FDCD{reptyear}{reptmon}{reptday}.parquet"
    df.write_parquet(fixedcd_file)
    print(f"💾 Saved FIXEDCD data: {fixedcd_file}")
    
    # Save to FIXEDR1 output (subset of columns)
    fixedr1_columns = ['ACCTNO', 'CDNO']
    fixedr1_df = df.select(fixedr1_columns)
    fixedr1_file = f"{OUTPUT_DIR}/R1CD{reptyear}{reptmon}{reptday}.parquet"
    fixedr1_df.write_parquet(fixedr1_file)
    print(f"💾 Saved FIXEDR1 data: {fixedr1_file}")
    
    # Sort FIXEDR1 data by ACCTNO (equivalent to PROC SORT)
    sorted_fixedr1 = fixedr1_df.sort('ACCTNO')
    
    # Merge with CIS data (equivalent to SAS MERGE)
    try:
        cis_query = """
        SELECT 
            ACCTNO,
            CUSTNAM1 as NAME,
            CUSTNO,
            SIC_ISS
        FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/CIS/DEPOSIT/*.parquet')
        WHERE SECCUST = '901'
        """
        
        cis_df = conn.execute(cis_query).pl()
        
        if not cis_df.is_empty():
            # Remove duplicates by ACCTNO (equivalent to NODUPKEY)
            cis_deduped = cis_df.unique(subset=['ACCTNO'])
            
            # Merge with fixed deposit data (equivalent to SAS MERGE)
            merged_df = sorted_fixedr1.join(
                cis_deduped, 
                on='ACCTNO', 
                how='inner'  # Equivalent to IF A in SAS
            )
            
            # Save merged result
            merged_file = f"{OUTPUT_DIR}/R1CD{reptyear}{reptmon}{reptday}_MERGED.parquet"
            merged_df.write_parquet(merged_file)
            print(f"🔗 Merged with CIS data: {merged_file}")
            print(f"📊 Merged records: {len(merged_df)}")
        else:
            print("⚠ No CIS data found for merging")
            
    except Exception as e:
        print(f"⚠ CIS data merge skipped: {e}")
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBDFDCE Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Records Processed: {len(df)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
