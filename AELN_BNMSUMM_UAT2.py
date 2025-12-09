"""
EIBWTRBL - Billings Transaction Processing
"""

import duckdb
import polars as pl
from datetime import datetime
import os

print("Billings Transaction Processing...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/BTRADE/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================================
# DATE PROCESSING (equivalent to DATA REPTDATE)
# ========================================
print("📅 Processing reporting dates...")

# Load REPTDATE from parquet
reptdate_df = pl.read_parquet(f'{BASE_PATH}/LOAN/REPTDATE/*.parquet')
reptdate = reptdate_df['REPTDATE'][0]

# Determine week (equivalent to SAS SELECT/WHEN)
day_of_month = reptdate.day

if day_of_month == 8:
    nowk = '1'
elif day_of_month == 15:
    nowk = '2'
elif day_of_month == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')

print(f"📅 Processing date: {reptdate.strftime('%Y-%m-%d')} (Week {nowk})")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # PROCESS BILLINGS TRANSACTION DATA
    # ========================================
    print("📊 Processing billings transaction data...")
    
    # Read from parquet file (equivalent to INFILE EXTCOMM)
    # Assuming the external file is available as parquet
    extcomm_query = """
    SELECT 
        SUBSTRING(record, 1, 2) as RECTYPE,
        SUBSTRING(record, 3, 10) as TRANSREF,
        CAST(SUBSTRING(record, 21, 4) as INTEGER) as COSTCTR,
        SUBSTRING(record, 27, 15) as ACCTNO,
        SUBSTRING(record, 42, 13) as SUBACCT,
        SUBSTRING(record, 64, 5) as GLMNEMONIC,
        SUBSTRING(record, 69, 3) as LIABCODE,
        CAST(CONCAT(SUBSTRING(record, 73, 4), '-', SUBSTRING(record, 77, 2), '-', SUBSTRING(record, 79, 2)) as DATE) as TRANDATE,
        CAST(CONCAT('20', SUBSTRING(record, 81, 2), '-', SUBSTRING(record, 83, 2), '-', SUBSTRING(record, 85, 2)) as DATE) as EXPRDATE,
        CAST(SUBSTRING(record, 87, 18) as DOUBLE) as TRANAMT,
        CAST(SUBSTRING(record, 105, 16) as DOUBLE) as EXCHANGE,
        SUBSTRING(record, 121, 4) as CURRENCY,
        SUBSTRING(record, 125, 13) as BTREL,
        SUBSTRING(record, 138, 13) as RELFROM,
        SUBSTRING(record, 169, 10) as TRANSREFPG,
        CAST(SUBSTRING(record, 180, 18) as DOUBLE) as TRANAMT_CCY,
        SUBSTRING(record, 198, 10) as TRANS_NUM,
        SUBSTRING(record, 208, 3) as TRANS_IND,
        SUBSTRING(record, 211, 5) as MNEMONIC_CD,
        SUBSTRING(record, 216, 20) as ACCT_INFO,
        SUBSTRING(record, 236, 1) as CR_DR_IND,
        SUBSTRING(record, 237, 10) as VOUCHER_NUM
    FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/EXTCOMM/*.parquet')
    WHERE LENGTH(record) >= 246  -- Ensure record has minimum length
    """
    
    # Alternative: If data is already in structured parquet format
    try:
        # Try to read as structured parquet first
        btrbl_df = pl.read_parquet(f'{BASE_PATH}/EXTCOMM/structured/*.parquet')
        print("📄 Loaded from structured parquet file")
    except:
        # Fallback to parsing from raw text format
        btrbl_df = conn.execute(extcomm_query).pl()
        print("📄 Loaded and parsed from raw format")
    
    print(f"📄 Transaction records loaded: {len(btrbl_df)}")
    
    # ========================================
    # DATA VALIDATION AND CLEANING
    # ========================================
    print("🔍 Validating and cleaning data...")
    
    # Remove invalid records and clean data
    btrbl_clean = btrbl_df.filter(
        pl.col('ACCTNO').is_not_null() &
        pl.col('TRANDATE').is_not_null() &
        pl.col('TRANAMT').is_not_null()
    ).with_columns([
        # Clean account numbers
        pl.col('ACCTNO').str.strip_chars().alias('ACCTNO_CLEAN'),
        # Ensure numeric fields are properly formatted
        pl.col('TRANAMT').fill_nan(0.0).fill_null(0.0),
        pl.col('EXCHANGE').fill_nan(0.0).fill_null(0.0),
        pl.col('TRANAMT_CCY').fill_nan(0.0).fill_null(0.0),
        # Add processing timestamp
        pl.lit(datetime.now()).alias('PROCESSING_TS')
    ])
    
    print(f"📄 Valid records after cleaning: {len(btrbl_clean)}")

    # ========================================
    # SAVE OUTPUT FILES
    # ========================================
    print("💾 Saving output files...")
    
    # Base filename
    base_filename = f"BILLSTRAN_{reptmon}{nowk}_{reptyear}"
    
    # Save as Parquet
    parquet_file = f"{OUTPUT_DIR}/{base_filename}.parquet"
    btrbl_clean.write_parquet(parquet_file)
    print(f"💾 Parquet output: {parquet_file}")
    
    # Save as CSV
    csv_file = f"{OUTPUT_DIR}/{base_filename}.csv"
    btrbl_clean.write_csv(csv_file)
    print(f"💾 CSV output: {csv_file}")
    
    # ========================================
    # GENERATE SUMMARY STATISTICS
    # ========================================
    print("📈 Generating summary statistics...")
    
    summary_stats = pl.DataFrame({
        'metric': [
            'Total_Records', 'Valid_Records', 'Total_Amount', 
            'Avg_Transaction_Amount', 'Credit_Transactions', 
            'Debit_Transactions', 'Unique_Accounts'
        ],
        'value': [
            len(btrbl_df),
            len(btrbl_clean),
            btrbl_clean['TRANAMT'].sum(),
            btrbl_clean['TRANAMT'].mean(),
            btrbl_clean.filter(pl.col('CR_DR_IND') == 'C').height,
            btrbl_clean.filter(pl.col('CR_DR_IND') == 'D').height,
            btrbl_clean['ACCTNO_CLEAN'].n_unique()
        ],
        'week': [nowk] * 7,
        'month': [reptmon] * 7,
        'year': [reptyear] * 7
    })
    
    # Save summary as Parquet
    summary_parquet = f"{OUTPUT_DIR}/BILLSTRAN_SUMMARY_{reptmon}{nowk}_{reptyear}.parquet"
    summary_stats.write_parquet(summary_parquet)
    print(f"💾 Summary parquet: {summary_parquet}")
    
    # Save summary as CSV
    summary_csv = f"{OUTPUT_DIR}/BILLSTRAN_SUMMARY_{reptmon}{nowk}_{reptyear}.csv"
    summary_stats.write_csv(summary_csv)
    print(f"💾 Summary CSV: {summary_csv}")

    # ========================================
    # GENERATE DAILY BREAKDOWN
    # ========================================
    print("📊 Generating daily breakdown...")
    
    daily_breakdown = btrbl_clean.group_by('TRANDATE').agg([
        pl.col('TRANAMT').sum().alias('DAILY_TOTAL'),
        pl.col('ACCTNO_CLEAN').n_unique().alias('UNIQUE_ACCOUNTS'),
        pl.col('TRANSREF').count().alias('TRANSACTION_COUNT'),
        pl.col('CR_DR_IND').filter(pl.col('CR_DR_IND') == 'C').count().alias('CREDIT_COUNT'),
        pl.col('CR_DR_IND').filter(pl.col('CR_DR_IND') == 'D').count().alias('DEBIT_COUNT')
    ]).sort('TRANDATE')
    
    # Save daily breakdown as Parquet
    daily_parquet = f"{OUTPUT_DIR}/BILLSTRAN_DAILY_{reptmon}{nowk}_{reptyear}.parquet"
    daily_breakdown.write_parquet(daily_parquet)
    print(f"💾 Daily breakdown parquet: {daily_parquet}")
    
    # Save daily breakdown as CSV
    daily_csv = f"{OUTPUT_DIR}/BILLSTRAN_DAILY_{reptmon}{nowk}_{reptyear}.csv"
    daily_breakdown.write_csv(daily_csv)
    print(f"💾 Daily breakdown CSV: {daily_csv}")

    # ========================================
    # GENERATE CURRENCY BREAKDOWN
    # ========================================
    print("💰 Generating currency breakdown...")
    
    currency_breakdown = btrbl_clean.group_by('CURRENCY').agg([
        pl.col('TRANAMT_CCY').sum().alias('TOTAL_AMOUNT_CCY'),
        pl.col('TRANAMT').sum().alias('TOTAL_AMOUNT_LCY'),
        pl.col('EXCHANGE').mean().alias('AVG_EXCHANGE_RATE'),
        pl.col('ACCTNO_CLEAN').n_unique().alias('UNIQUE_ACCOUNTS'),
        pl.col('TRANSREF').count().alias('TRANSACTION_COUNT')
    ]).sort('CURRENCY')
    
    # Save currency breakdown as Parquet
    currency_parquet = f"{OUTPUT_DIR}/BILLSTRAN_CURRENCY_{reptmon}{nowk}_{reptyear}.parquet"
    currency_breakdown.write_parquet(currency_parquet)
    print(f"💾 Currency breakdown parquet: {currency_parquet}")
    
    # Save currency breakdown as CSV
    currency_csv = f"{OUTPUT_DIR}/BILLSTRAN_CURRENCY_{reptmon}{nowk}_{reptyear}.csv"
    currency_breakdown.write_csv(currency_csv)
    print(f"💾 Currency breakdown CSV: {currency_csv}")

    # ========================================
    # GENERATE METADATA FILE
    # ========================================
    print("📄 Generating metadata...")
    
    metadata_data = {
        'parameter': [
            'PROCESS_NAME', 'REPORT_DATE', 'WEEK', 'MONTH', 'YEAR',
            'INPUT_RECORDS', 'OUTPUT_RECORDS', 'PROCESSING_TIME'
        ],
        'value': [
            'EIBWTRBL', reptdate.strftime('%Y-%m-%d'), nowk, reptmon, reptyear,
            str(len(btrbl_df)), str(len(btrbl_clean)), 
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ]
    }
    metadata_df = pl.DataFrame(metadata_data)
    
    # Save metadata as Parquet
    metadata_parquet = f"{OUTPUT_DIR}/BILLSTRAN_METADATA_{reptmon}{nowk}_{reptyear}.parquet"
    metadata_df.write_parquet(metadata_parquet)
    print(f"💾 Metadata parquet: {metadata_parquet}")
    
    # Save metadata as CSV
    metadata_csv = f"{OUTPUT_DIR}/BILLSTRAN_METADATA_{reptmon}{nowk}_{reptyear}.csv"
    metadata_df.write_csv(metadata_csv)
    print(f"💾 Metadata CSV: {metadata_csv}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWTRBL Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Input Records: {len(btrbl_df)}")
    print(f"📄 Output Records: {len(btrbl_clean)}")
    print(f"💰 Total Amount: {btrbl_clean['TRANAMT'].sum():,.2f}")
    print(f"👤 Unique Accounts: {btrbl_clean['ACCTNO_CLEAN'].n_unique()}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"📊 Files Generated: 8 files (4 Parquet + 4 CSV)")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()



EXTDATE	REPTDATE
12082025342	24083
