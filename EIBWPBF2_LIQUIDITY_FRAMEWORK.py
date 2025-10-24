"""
EIBWPBF2 - New Liquidity Framework for PBIF
"""

import duckdb
import polars as pl
from datetime import datetime, date
import os

print("🚀 Starting EIBWPBF2 - New Liquidity Framework for PBIF...")
start_time = datetime.now()

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse'
OUTPUT_DIR = f'{BASE_PATH}/PBIF/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get reporting date (equivalent to SAS REPTDATE processing)
reptdate_df = pl.read_parquet(f'{BASE_PATH}/PBIF/REPTDATE/*.parquet')
reptdate = reptdate_df['REPTDATE'][0]

# Determine week and quarter flags (same logic as SAS)
day_of_month = reptdate.day
if day_of_month == 8:
    nowk = '1'
elif day_of_month == 15:
    nowk = '2'
elif day_of_month == 22:
    nowk = '3'
else:
    nowk = '4'

# Check if next day is 1st of month for quarter flag
next_day = reptdate + pl.duration(days=1)
reptq = 'Y' if next_day.day == 1 else 'N'

# Date parameters (equivalent to CALL SYMPUT)
rdatx = int(reptdate.strftime('%j'))  # Julian day
reptyr = reptdate.year
reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%Y')
mdate = rdatx

print(f"📅 Processing date: {rdate} (Week {nowk}, Quarter Flag: {reptq})")

# Connect to DuckDB
conn = duckdb.connect()

try:
    # ========================================
    # LOAD PBIF DATA (equivalent to %RDAL macro)
    # ========================================
    print("📊 Loading PBIF data...")
    
    if reptq != 'Y':
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
    # MATURITY PROFILE CALCULATIONS (PART 1 & 2)
    # ========================================
    print("📈 Calculating maturity profiles...")
    
    # Calculate days to maturity and remaining months
    pbif_calc = pbif_df.with_columns([
        # Days to maturity
        (pl.col('MATDTE') - reptdate).dt.total_days().alias('DAYS'),
        
        # Adjust CUSTCX (equivalent to SAS IF statement)
        pl.when(pl.col('CUSTCX') == '67').then(pl.lit('44'))
        .otherwise(pl.col('CUSTCX')).alias('CUSTCX_ADJ'),
        
        # Calculate remaining months (equivalent to %REMMTH macro)
        pl.when((pl.col('MATDTE') - reptdate).dt.total_days() < 8)
        .then(pl.lit(0.1))
        .otherwise(
            ((pl.col('MATDTE').dt.year() - reptyr) * 12 + 
             (pl.col('MATDTE').dt.month() - int(reptmon)) + 
             (pl.col('MATDTE').dt.day() - int(reptday)) / 
             pl.col('MATDTE').dt.days_in_month())
        ).alias('REMMTH'),
        
        # Determine ITEM based on CUSTCX (equivalent to SAS IF/THEN)
        pl.when(pl.col('CUSTCX').is_in(['77', '78', '95', '96']))
        .then(pl.lit('A1.08A'))
        .otherwise(pl.lit('A1.02')).alias('ITEM'),
        
        # AMOUNT from BALANCE
        pl.col('BALANCE').alias('AMOUNT')
    ])

    # Create PART 1-RM and PART 2-RM records (equivalent to OUTPUT)
    part1_df = pbif_calc.with_columns([pl.lit('1-RM').alias('PART')])
    part2_df = pbif_calc.with_columns([pl.lit('2-RM').alias('PART')])
    
    # Undrawn portion for PART 2-RM (equivalent to second DATA step)
    undrawn_part2 = pbif_calc.with_columns([
        pl.lit('2-RM').alias('PART'),
        pl.lit('A1.28').alias('ITEM'),
        pl.when((pl.col('INLIMIT') - pl.col('BALANCE')) > 0)
        .then(pl.col('INLIMIT') - pl.col('BALANCE'))
        .otherwise(0.0).alias('AMOUNT')
    ])
    
    # Undrawn portion for PART 1-RM (equivalent to third DATA step)
    undrawn_part1 = pbif_calc.with_columns([
        pl.lit('1-RM').alias('PART'),
        pl.lit('A1.28').alias('ITEM'),
        pl.when(((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.20) > 0)
        .then((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.20)
        .otherwise(0.0).alias('AMOUNT'),
        pl.lit(0.1).alias('REMMTH')
    ])

    # Combine all data (equivalent to SET statement)
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
    
    # Apply maturity bucket formatting (equivalent to REMFMT format)
    summary_df = summary_df.with_columns([
        pl.when(pl.col('REMMTH') <= 0.255).then(pl.lit('UP TO 1 WK'))
        .when(pl.col('REMMTH') <= 1.0).then(pl.lit('>1 WK - 1 MTH'))
        .otherwise(pl.lit('>1 MTH')).alias('REMMTH_BUCKET')
    ])
    
    # Apply item descriptions (equivalent to $ITEMF format)
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
    
    part1_file = f"{OUTPUT_DIR}/PART1_RM_{reptmon}{nowk}.parquet"
    part1_report.write_parquet(part1_file)
    print(f"💾 PART 1-RM report: {part1_file}")
    
    # PART 2-RM Report (Contractual Maturity Profile)  
    part2_report = summary_df.filter(
        (pl.col('PART') == '2-RM') & 
        (~pl.col('ITEM').is_in(['B1.12', 'B1.15']))
    )
    
    part2_file = f"{OUTPUT_DIR}/PART2_RM_{reptmon}{nowk}.parquet"
    part2_report.write_parquet(part2_file)
    print(f"💾 PART 2-RM report: {part2_file}")
    
    # Save full summary
    summary_file = f"{OUTPUT_DIR}/PBIF_SUMMARY_{reptmon}{nowk}.parquet"
    summary_df.write_parquet(summary_file)
    print(f"💾 Full summary: {summary_file}")

    # ========================================
    # GENERATE REPORT FILES (equivalent to PROC TABULATE output)
    # ========================================
    def generate_report_file(df, part_name, output_file):
        """Generate formatted report similar to PROC TABULATE"""
        with open(output_file, 'w') as f:
            f.write("PUBLIC BANK BERHAD\n")
            f.write("NEW LIQUIDITY FRAMEWORK (FACTORING) AS AT " + rdate + "\n")
            f.write("(CONTRACTUAL RUN-OFF)\n")
            f.write(f"BREAKDOWN BY {part_name} MATURITY PROFILE\n")
            f.write("=" * 80 + "\n")
            f.write(f"{'ITEM':<50} {'MATURITY BUCKET':<20} {'AMOUNT':>15}\n")
            f.write("-" * 80 + "\n")
            
            # Group by item and maturity bucket
            for item in df['ITEM_DESC'].unique():
                item_data = df.filter(pl.col('ITEM_DESC') == item)
                f.write(f"{item:<50}\n")
                
                total_amount = 0
                for bucket in ['UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH']:
                    bucket_data = item_data.filter(pl.col('REMMTH_BUCKET') == bucket)
                    amount = bucket_data['AMOUNT_SUM'].sum() if len(bucket_data) > 0 else 0
                    total_amount += amount
                    f.write(f"{'':<10}{bucket:<20} {amount:>15,.2f}\n")
                
                f.write(f"{'':<10}{'TOTAL':<20} {total_amount:>15,.2f}\n")
                f.write("-" * 80 + "\n")
    
    # Generate text reports
    generate_report_file(part1_report, "BEHAVIOURAL", f"{OUTPUT_DIR}/PART1_RM_REPORT_{reptmon}{nowk}.txt")
    generate_report_file(part2_report, "PURE CONTRACTUAL", f"{OUTPUT_DIR}/PART2_RM_REPORT_{reptmon}{nowk}.txt")
    
    print("📄 Generated text reports")

    # ========================================
    # COMPLETION SUMMARY
    # ========================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 EIBWPBF2 Completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📄 Input Records: {len(pbif_df)}")
    print(f"📄 Output Records: {len(summary_df)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    
    exit(0)
    
except Exception as e:
    print(f"💥 Processing failed: {e}")
    exit(1)
    
finally:
    conn.close()
