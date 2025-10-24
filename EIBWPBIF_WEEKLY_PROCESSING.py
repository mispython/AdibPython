"""
EIBWPBIF - PBIF Weekly Processing
"""

import duckdb
import polars as pl
from datetime import datetime, date
import os

print("🚀 Starting EIBWPBIF - PBIF Weekly Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/PBIF/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================================
# DATE PROCESSING (equivalent to DATA REPTDATE)
# ========================================
print("📅 Processing reporting dates...")

# Load REPTDATE from parquet
reptdate_df = pl.read_parquet(f'{BASE_PATH}/PBIF/REPTDATE/*.parquet')
reptdate = reptdate_df['REPTDATE'][0]

# Determine week and start date (same logic as SAS SELECT/WHEN)
day_of_month = reptdate.day

if day_of_month == 8:
    sdd = 1
    wk = '1'
    wk1 = '4'
    wk2 = None
    wk3 = None
elif day_of_month == 15:
    sdd = 9  
    wk = '2'
    wk1 = '1'
    wk2 = None
    wk3 = None
elif day_of_month == 22:
    sdd = 16
    wk = '3'
    wk1 = '2'
    wk2 = None
    wk3 = None
else:
    sdd = 23
    wk = '4'
    wk1 = '3'
    wk2 = '2'
    wk3 = '1'

mm = reptdate.month

# Calculate MM1 (previous month for week 1)
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

# Calculate start date (equivalent to MDY function)
sdate = date(reptdate.year, mm, sdd)

# Quarter flag (equivalent to QTR calculation)
qtr = 'Y' if mm in [3, 6, 9, 12] else 'N'

# Set parameters (equivalent to CALL SYMPUT)
nowk = wk
nowk1 = wk1
nowk2 = wk2 if wk2 else ''
nowk3 = wk3 if wk3 else ''
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = reptdate.strftime('%y')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%Y')
sdate_str = sdate.strftime('%d%m%Y')

print(f"📅 Report Date: {rdate} (Week {nowk})")
print(f"📅 Start Date: {sdate_str}")
print(f"📅 Quarter Flag: {qtr}")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # PROCESS RDALPBIF DATA
    # ========================================
    print("📊 Processing RDALPBIF data...")
    
    rdalpbif_df = pl.read_parquet(f'{BASE_PATH}/PBIF/RDALPBIF/*.parquet')
    
    # Save as weekly parquet file
    rdalpbif_parquet = f"{OUTPUT_DIR}/RDALPBIF_{reptmon}{nowk}_{reptyear}.parquet"
    rdalpbif_df.write_parquet(rdalpbif_parquet)
    print(f"💾 RDALPBIF parquet: {rdalpbif_parquet}")
    
    # Save as CSV
    rdalpbif_csv = f"{OUTPUT_DIR}/RDALPBIF_{reptmon}{nowk}_{reptyear}.csv"
    rdalpbif_df.write_csv(rdalpbif_csv)
    print(f"💾 RDALPBIF CSV: {rdalpbif_csv}")

    # ========================================
    # PROCESS PBIFLALW DATA  
    # ========================================
    print("📊 Processing PBIFLALW data...")
    
    pbiflalw_df = pl.read_parquet(f'{BASE_PATH}/PBIF/PBIFLALW/*.parquet')
    
    # Save as weekly parquet file
    pbiflalw_parquet = f"{OUTPUT_DIR}/PBIFLALW_{reptmon}{nowk}_{reptyear}.parquet"
    pbiflalw_df.write_parquet(pbiflalw_parquet)
    print(f"💾 PBIFLALW parquet: {pbiflalw_parquet}")
    
    # Save as CSV
    pbiflalw_csv = f"{OUTPUT_DIR}/PBIFLALW_{reptmon}{nowk}_{reptyear}.csv"
    pbiflalw_df.write_csv(pbiflalw_csv)
    print(f"💾 PBIFLALW CSV: {pbiflalw_csv}")

    # ========================================
    # PROCESS PBIFFISS DATA
    # ========================================
    print("📊 Processing PBIFFISS data...")
    
    pbiffiss_df = pl.read_parquet(f'{BASE_PATH}/PBIF/PBIFFISS/*.parquet')
    
    # Save as weekly parquet file
    pbiffiss_parquet = f"{OUTPUT_DIR}/PBIFFISS_{reptmon}{nowk}_{reptyear}.parquet"
    pbiffiss_df.write_parquet(pbiffiss_parquet)
    print(f"💾 PBIFFISS parquet: {pbiffiss_parquet}")
    
    # Save as CSV
    pbiffiss_csv = f"{OUTPUT_DIR}/PBIFFISS_{reptmon}{nowk}_{reptyear}.csv"
    pbiffiss_df.write_csv(pbiffiss_csv)
    print(f"💾 PBIFFISS CSV: {pbiffiss_csv}")

    # ========================================
    # GENERATE WEEKLY SUMMARY
    # ========================================
    print("📈 Generating weekly summary...")
    
    # Create summary using DuckDB
    summary_query = f"""
    WITH rdal_summary AS (
        SELECT 
            'RDALPBIF' as dataset,
            COUNT(*) as record_count,
            SUM(BALANCE) as total_balance,
            AVG(BALANCE) as avg_balance
        FROM read_parquet('{rdalpbif_parquet}')
    ),
    lalw_summary AS (
        SELECT 
            'PBIFLALW' as dataset, 
            COUNT(*) as record_count,
            SUM(BALANCE) as total_balance, 
            AVG(BALANCE) as avg_balance
        FROM read_parquet('{pbiflalw_parquet}')
    ),
    fiss_summary AS (
        SELECT 
            'PBIFFISS' as dataset,
            COUNT(*) as record_count, 
            SUM(BALANCE) as total_balance,
            AVG(BALANCE) as avg_balance
        FROM read_parquet('{pbiffiss_parquet}')
    )
    SELECT * FROM rdal_summary
    UNION ALL SELECT * FROM lalw_summary  
    UNION ALL SELECT * FROM fiss_summary
    """
    
    summary_df = conn.execute(summary_query).pl()
    
    # Save summary as parquet
    summary_parquet = f"{OUTPUT_DIR}/WEEKLY_SUMMARY_{reptmon}{nowk}_{reptyear}.parquet"
    summary_df.write_parquet(summary_parquet)
    print(f"💾 Weekly summary parquet: {summary_parquet}")
    
    # Save summary as CSV
    summary_csv = f"{OUTPUT_DIR}/WEEKLY_SUMMARY_{reptmon}{nowk}_{reptyear}.csv"
    summary_df.write_csv(summary_csv)
    print(f"💾 Weekly summary CSV: {summary_csv}")

    # ========================================
    # GENERATE QUARTERLY OUTPUTS IF APPLICABLE
    # ========================================
    if qtr == 'Y':
        print("📊 Generating quarterly outputs...")
        
        # Quarterly combined data
        quarterly_query = f"""
        SELECT 
            'Q{mm//3}_{reptyear}' as quarter,
            r.ACCTNO, r.BALANCE as RDAL_BALANCE, 
            l.BALANCE as LALW_BALANCE, f.BALANCE as FISS_BALANCE
        FROM read_parquet('{rdalpbif_parquet}') r
        LEFT JOIN read_parquet('{pbiflalw_parquet}') l ON r.ACCTNO = l.ACCTNO
        LEFT JOIN read_parquet('{pbiffiss_parquet}') f ON r.ACCTNO = f.ACCTNO
        """
        
        quarterly_df = conn.execute(quarterly_query).pl()
        
        # Save quarterly data
        quarterly_parquet = f"{OUTPUT_DIR}/QUARTERLY_Q{mm//3}_{reptyear}.parquet"
        quarterly_df.write_parquet(quarterly_parquet)
        print(f"💾 Quarterly parquet: {quarterly_parquet}")
        
        quarterly_csv = f"{OUTPUT_DIR}/QUARTERLY_Q{mm//3}_{reptyear}.csv"
        quarterly_df.write_csv(quarterly_csv)
        print(f"💾 Quarterly CSV: {quarterly_csv}")

    # ========================================
    # GENERATE METADATA FILE
    # ========================================
    print("📄 Generating metadata...")
    
    metadata_data = {
        'parameter': ['REPORT_DATE', 'WEEK', 'QUARTER_FLAG', 'MONTH', 'PREV_MONTH', 'YEAR', 'START_DATE'],
        'value': [rdate, nowk, qtr, reptmon, reptmon1, reptyear, sdate_str]
    }
    metadata_df = pl.DataFrame(metadata_data)
    
    metadata_parquet = f"{OUTPUT_DIR}/METADATA_{reptmon}{nowk}_{reptyear}.parquet"
    metadata_df.write_parquet(metadata_parquet)
    print(f"💾 Metadata parquet: {metadata_parquet}")
    
    metadata_csv = f"{OUTPUT_DIR}/METADATA_{reptmon}{nowk}_{reptyear}.csv"
    metadata_df.write_csv(metadata_csv)
    print(f"💾 Metadata CSV: {metadata_csv}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWPBIF Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 RDALPBIF Records: {len(rdalpbif_df)}")
    print(f"📄 PBIFLALW Records: {len(pbiflalw_df)}")
    print(f"📄 PBIFFISS Records: {len(pbiffiss_df)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    # List generated files
    print(f"\n📁 Generated Files:")
    for file in os.listdir(OUTPUT_DIR):
        if file.startswith(f"{reptmon}{nowk}_{reptyear}") or file.startswith(f"WEEKLY_{reptmon}{nowk}"):
            print(f"   📄 {file}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
