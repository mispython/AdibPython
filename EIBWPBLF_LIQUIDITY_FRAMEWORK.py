"""
EIBWPBLF - Public Bank Liquidity Framework
"""

import duckdb
import polars as pl
from datetime import datetime, date
import os

print("🚀 Starting EIBWPBLF - Public Bank Liquidity Framework...")
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

# Determine quarter flag (equivalent to REPTQ)
next_day = reptdate + pl.duration(days=1)
reptq = 'Y' if next_day.day == 1 else 'N'

# Determine week (equivalent to SELECT/WHEN)
day_of_month = reptdate.day
if day_of_month == 8:
    nowk = '1'
elif day_of_month == 15:
    nowk = '2'
elif day_of_month == 22:
    nowk = '3'
else:
    nowk = '4'

# Set parameters (equivalent to CALL SYMPUT)
rdatx = int(reptdate.strftime('%j'))  # Julian day
reptyr = reptdate.year
reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%Y')
mdate = rdatx

print(f"📅 Report Date: {rdate} (Week {nowk}, Quarter Flag: {reptq})")

# ========================================
# MATURITY BUCKET FORMATS (equivalent to PROC FORMAT)
# ========================================
def get_remfmt_bucket(remmth):
    """Convert REMMTH to maturity buckets (equivalent to REMFMT format)"""
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'

# Item descriptions (equivalent to $ITEMF format)
item_descriptions = {
    'A1.01': 'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02': 'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03': 'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04': 'A1.04  LOANS: CORP - OTHERS',
    'A1.05': 'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07': 'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08': 'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.12': 'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13': 'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14': 'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15': 'A1.15  DEPOSITS: IND  - FIXED',
    'A1.16': 'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17': 'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.25': 'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.28': 'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A2.01': 'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02': 'A2.02  REVERSE REPO',
    'A2.03': 'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04': 'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05': 'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06': 'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08': 'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09': 'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14': 'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15': 'A2.15  INTERBANK REPOS',
    'A2.16': 'A2.16  NON-INTERBANK REPOS',
    'A2.17': 'A2.17  NIDS ISSUED',
    'A2.18': 'A2.18  BAS PAYABLE',
    'A2.19': 'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.12': 'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15': 'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01': 'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09': 'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14': 'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19': 'B2.19  FX EXCHG CONTRACTS PAYABLE'
}

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # LOAD PBIF DATA (equivalent to %RDAL macro)
    # ========================================
    print("📊 Loading PBIF data...")
    
    if reptq != "Y":
        # Load regular PBIF data
        pbif_query = """
        SELECT * FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/PBIF/RDALPBIF/*.parquet')
        """
    else:
        # Load monthly PBIF data  
        pbif_query = """
        SELECT * FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/PBIF/RDLMPBIF/*.parquet')
        """
    
    pbif_df = conn.execute(pbif_query).pl()
    print(f"📄 PBIF records loaded: {len(pbif_df)}")

    # ========================================
    # MATURITY PROFILE CALCULATIONS
    # ========================================
    print("📈 Calculating maturity profiles...")
    
    # Calculate remaining months (equivalent to %REMMTH macro logic)
    pbif_calc = pbif_df.with_columns([
        # Days to maturity
        (pl.col('MATDTE') - reptdate).dt.total_days().alias('DAYS'),
        
        # Calculate remaining months
        pl.when((pl.col('MATDTE') - reptdate).dt.total_days() < 8)
        .then(pl.lit(0.1))
        .otherwise(
            ((pl.col('MATDTE').dt.year() - reptyr) * 12 + 
             (pl.col('MATDTE').dt.month() - int(reptmon)) + 
             (pl.col('MATDTE').dt.day() - int(reptday)) / 
             pl.when(pl.col('MATDTE').dt.month() == 2)
             .then(pl.when(pl.col('MATDTE').dt.year() % 4 == 0).then(29).otherwise(28))
             .otherwise(pl.col('MATDTE').dt.days_in_month()))
        ).alias('REMMTH'),
        
        # Determine ITEM based on CUSTCX
        pl.when(pl.col('CUSTCX').is_in(['77', '78', '95', '96']))
        .then(pl.lit('A1.08'))
        .otherwise(pl.lit('A1.04')).alias('ITEM'),
        
        # AMOUNT from BALANCE
        pl.col('BALANCE').alias('AMOUNT')
    ])

    # Create PART 1-RM and PART 2-RM records
    part1_df = pbif_calc.with_columns([pl.lit('1-RM').alias('PART')])
    part2_df = pbif_calc.with_columns([pl.lit('2-RM').alias('PART')])
    
    # Undrawn portion for PART 2-RM
    undrawn_part2 = pbif_calc.with_columns([
        pl.lit('2-RM').alias('PART'),
        pl.lit('A1.28').alias('ITEM'),
        pl.when((pl.col('INLIMIT') - pl.col('BALANCE')) > 0)
        .then(pl.col('INLIMIT') - pl.col('BALANCE'))
        .otherwise(0.0).alias('AMOUNT')
    ])
    
    # Undrawn portion for PART 1-RM
    undrawn_part1 = pbif_calc.with_columns([
        pl.lit('1-RM').alias('PART'),
        pl.lit('A1.28').alias('ITEM'),
        pl.when(((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.20) > 0)
        .then((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.20)
        .otherwise(0.0).alias('AMOUNT')
    ])

    # Combine all data
    combined_df = pl.concat([part1_df, part2_df, undrawn_part2, undrawn_part1])
    
    # Select relevant columns
    final_df = combined_df.select(['PART', 'ITEM', 'REMMTH', 'AMOUNT'])
    print(f"📄 Combined records: {len(final_df)}")

    # ========================================
    # SUMMARIZE DATA (equivalent to PROC SUMMARY)
    # ========================================
    print("📊 Summarizing data...")
    
    summary_df = final_df.group_by(['PART', 'ITEM', 'REMMTH']).agg([
        pl.col('AMOUNT').sum().alias('AMOUNT_SUM')
    ])
    
    # Apply maturity bucket formatting
    summary_df = summary_df.with_columns([
        pl.col('REMMTH').map_elements(get_remfmt_bucket, return_dtype=pl.Utf8).alias('REMMTH_BUCKET')
    ])
    
    # Apply item descriptions
    summary_df = summary_df.with_columns([
        pl.col('ITEM').replace(item_descriptions).alias('ITEM_DESC')
    ])

    # ========================================
    # GENERATE REPORTS
    # ========================================
    print("📋 Generating reports...")
    
    # PART 1-RM Report (Behavioral Maturity Profile)
    part1_report = summary_df.filter(
        (pl.col('PART') == '1-RM') & 
        (~pl.col('ITEM').is_in(['B1.12', 'B1.15']))
    )
    
    part1_file = f"{OUTPUT_DIR}/PBLF_PART1_RM_{reptmon}{nowk}_{reptyear}.parquet"
    part1_report.write_parquet(part1_file)
    print(f"💾 PART 1-RM report: {part1_file}")
    
    # PART 2-RM Report (Contractual Maturity Profile)  
    part2_report = summary_df.filter(
        (pl.col('PART') == '2-RM') & 
        (~pl.col('ITEM').is_in(['B1.12', 'B1.15']))
    )
    
    part2_file = f"{OUTPUT_DIR}/PBLF_PART2_RM_{reptmon}{nowk}_{reptyear}.parquet"
    part2_report.write_parquet(part2_file)
    print(f"💾 PART 2-RM report: {part2_file}")
    
    # Save full summary
    summary_file = f"{OUTPUT_DIR}/PBLF_FULL_SUMMARY_{reptmon}{nowk}_{reptyear}.parquet"
    summary_df.write_parquet(summary_file)
    print(f"💾 Full summary: {summary_file}")

    # ========================================
    # GENERATE FORMATTED REPORT OUTPUTS
    # ========================================
    def generate_maturity_report(df, part_name, output_file):
        """Generate formatted maturity profile report"""
        # Group by item and maturity bucket for reporting
        report_data = df.group_by(['ITEM_DESC', 'REMMTH_BUCKET']).agg([
            pl.col('AMOUNT_SUM').sum().alias('AMOUNT')
        ])
        
        # Pivot for better reporting format
        pivot_df = report_data.pivot(
            values='AMOUNT',
            index='ITEM_DESC',
            columns='REMMTH_BUCKET',
            aggregate_function='sum'
        )
        
        # Add total column
        pivot_df = pivot_df.with_columns([
            pl.sum_horizontal(pl.exclude('ITEM_DESC')).alias('TOTAL')
        ])
        
        pivot_df.write_parquet(output_file)
        print(f"💾 {part_name} pivot report: {output_file}")
    
    # Generate pivot reports
    generate_maturity_report(part1_report, "PART1-RM", 
                           f"{OUTPUT_DIR}/PBLF_PART1_PIVOT_{reptmon}{nowk}_{reptyear}.parquet")
    generate_maturity_report(part2_report, "PART2-RM", 
                           f"{OUTPUT_DIR}/PBLF_PART2_PIVOT_{reptmon}{nowk}_{reptyear}.parquet")

    # ========================================
    # METADATA AND CONFIGURATION
    # ========================================
    print("📄 Generating metadata...")
    
    metadata_data = {
        'framework': ['PBLF'],
        'report_date': [rdate],
        'week': [nowk],
        'quarter_flag': [reptq],
        'month': [reptmon],
        'year': [reptyear],
        'processing_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    }
    metadata_df = pl.DataFrame(metadata_data)
    
    metadata_file = f"{OUTPUT_DIR}/PBLF_METADATA_{reptmon}{nowk}_{reptyear}.parquet"
    metadata_df.write_parquet(metadata_file)
    print(f"💾 Metadata: {metadata_file}")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWPBLF Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Input Records: {len(pbif_df)}")
    print(f"📄 Output Records: {len(summary_df)}")
    print(f"📊 PART 1-RM Items: {len(part1_report)}")
    print(f"📊 PART 2-RM Items: {len(part2_report)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
