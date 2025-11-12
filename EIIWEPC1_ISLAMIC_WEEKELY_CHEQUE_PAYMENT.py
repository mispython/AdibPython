import polars as pl
import pandas as pd
from pathlib import Path
import datetime
from tabulate import tabulate

def process_islamic_cheques_report():
    """
    Islamic Bank Cheques Issued Report (EIIWEPC1)
    Processes and reports on cheques issued by Public Islamic Bank Berhad
    """
    
    # Configuration
    loan_path = Path("LOAN")
    bnm_path = Path("BNM")
    dpld_path = Path("DPLD")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)

    # =========================================================================
    # DATA REPTDATE - Date Processing
    # =========================================================================
    print("📅 PROCESSING REPORT DATE...")
    reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")

    # Process REPTDATE with SELECT/WHEN logic for Islamic banking
    processed_reptdate = reptdate_df.with_columns([
        pl.col('REPTDATE').dt.day().alias('day')
    ]).with_columns([
        pl.when(pl.col('day') == 8).then(
            pl.datetime(pl.col('REPTDATE').dt.year(), pl.col('REPTDATE').dt.month(), 1)
        ).when(pl.col('day') == 15).then(
            pl.datetime(pl.col('REPTDATE').dt.year(), pl.col('REPTDATE').dt.month(), 9)
        ).when(pl.col('day') == 22).then(
            pl.datetime(pl.col('REPTDATE').dt.year(), pl.col('REPTDATE').dt.month(), 16)
        ).otherwise(
            pl.datetime(pl.col('REPTDATE').dt.year(), pl.col('REPTDATE').dt.month(), 23)
        ).alias('PREVDATE')
    ])

    # Extract parameters - CALL SYMPUT equivalent
    first_row = processed_reptdate.row(0)
    REPTYEAR = first_row['REPTDATE'].strftime('%Y')
    REPTMON = f"{first_row['REPTDATE'].month:02d}"
    REPTDAY = f"{first_row['REPTDATE'].day:02d}"
    RDATE = first_row['REPTDATE'].strftime('%d/%m/%Y')
    PREVDT = first_row['PREVDATE']
    REPTDT = first_row['REPTDATE']

    print(f"   Report Year: {REPTYEAR}, Month: {REPTMON}, Day: {REPTDAY}")
    print(f"   Date Range: {PREVDT.strftime('%d/%m/%Y')} to {REPTDT.strftime('%d/%m/%Y')}")

    # =========================================================================
    # PROC FORMAT equivalent - Islamic Banking Formats
    # =========================================================================
    print("📊 LOADING ISLAMIC BANKING FORMATS...")
    
    DESC_FORMAT = {
        1: 'CHEQUES ISSUED'
    }

    ISLAMIC_TCODE_FORMAT = {
        310: 'FINANCING DISBURSEMENT',  # Islamic equivalent of LOAN DISBURSEMENT
        750: 'PRINCIPAL INCREASE (PROGRESSIVE FINANCING RELEASE)',  # Islamic terminology
        752: 'DEBITING FOR TAKAFUL PREMIUM',  # Islamic insurance
        753: 'DEBITING FOR LEGAL FEE',
        754: 'DEBITING FOR OTHER PAYMENTS',
        760: 'MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY'
    }

    ISLAMIC_FEEFMT_FORMAT = {
        'QR': 'QUIT RENT',
        'LF': 'LEGAL FEE & DISBURSEMENT',
        'VA': 'VALUATION FEE',
        'IP': 'TAKAFUL PREMIUM',  # Islamic insurance
        'PA': 'PROFESSIONAL/OTHERS',
        'AC': 'ADVERTISEMENT FEE',
        'MC': 'MAINTENANCE CHARGES',
        'RE': 'REPOSSESION CHARGES',
        'RI': 'REPAIR CHARGES',
        'SC': 'STORAGE CHARGES',
        'SF': 'SEARCH FEE',
        'TC': 'TOWING CHARGES',
        '99': 'MISCHELLANEOUS EXPENSES'
    }

    # =========================================================================
    # DATA BNM.DPLD - Islamic Deposit Data
    # =========================================================================
    print("💳 LOADING ISLAMIC DEPOSIT DATA...")
    try:
        dpld_df = pl.read_parquet(dpld_path / f"DPLD{REPTMON}.parquet")
        # Islamic banking date range filter
        dpld_filtered = dpld_df.filter(
            (pl.col('REPTDATE') >= PREVDT) & (pl.col('REPTDATE') <= REPTDT)
        )
        dpld_filtered.write_parquet(bnm_path / "ISLAMIC_DPLD.parquet")
        print(f"   ✅ Loaded Islamic DPLD data for month {REPTMON}")
    except FileNotFoundError:
        print(f"   ⚠️  NOTE: Islamic DPLD{REPTMON}.parquet not found")
        dpld_filtered = pl.DataFrame()

    # =========================================================================
    # DATA BNM.LNLD - Islamic Financing Data
    # =========================================================================
    print("🏦 LOADING ISLAMIC FINANCING DATA...")
    try:
        lnld_df = pl.read_csv("LNLD.csv", null_values=[""], infer_schema_length=10000)
        
        # Apply Islamic banking specific parsing
        lnld_processed = lnld_df.with_columns([
            pl.col('line').str.slice(0, 11).str.strip().cast(pl.Int64).alias('ACCTNO'),
            pl.col('line').str.slice(12, 5).str.strip().cast(pl.Int64).alias('NOTENO'),
            pl.col('line').str.slice(18, 7).str.strip().cast(pl.Int64).alias('COSTCTR'),
            pl.col('line').str.slice(26, 3).str.strip().alias('NOTETYPE'),
            pl.col('line').str.slice(30, 8).str.strip().alias('TRANDT_STR'),
            pl.col('line').str.slice(46, 3).str.strip().cast(pl.Int64).alias('TRANCODE'),
            pl.col('line').str.slice(50, 3).str.strip().cast(pl.Int64).alias('SEQNO')
        ]).with_columns([
            # Convert DDMMYY to date
            pl.when(pl.col('TRANDT_STR').str.len_bytes() == 8)
            .then(pl.col('TRANDT_STR').str.strptime(pl.Date, format='%d%m%y'))
            .otherwise(pl.lit(None)).alias('TRANDT')
        ])
        
        # Conditional input for Islamic fee structure
        lnld_processed = lnld_processed.with_columns([
            pl.lit(None).alias('FEEPLAN'),
            pl.lit(None).alias('FEENO')
        ]).with_columns([
            pl.when(pl.col('TRANCODE') == 760)
            .then(pl.struct([
                pl.col('line').str.slice(54, 2).str.strip().alias('FEEPLAN'),
                pl.col('line').str.slice(56, 3).str.strip().cast(pl.Int64).alias('FEENO')
            ]))
            .otherwise(pl.struct([
                pl.col('FEEPLAN'),
                pl.col('FEENO')
            ])).alias('fee_info')
        ]).with_columns([
            pl.col('fee_info').struct.field('FEEPLAN').alias('FEEPLAN'),
            pl.col('fee_info').struct.field('FEENO').alias('FEENO')
        ]).with_columns([
            # Continue with remaining input
            pl.col('line').str.slice(60, 18).str.strip().cast(pl.Float64).alias('TRANAMT'),
            pl.col('line').str.slice(79, 3).str.strip().cast(pl.Int64).alias('SOURCE')
        ])
        
        # Islamic banking specific filter - ONLY Islamic branches
        lnld_filtered = lnld_processed.filter(
            ((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) |
            (pl.col('COSTCTR').is_in([4043, 4048]))
        ).drop(['line', 'TRANDT_STR', 'fee_info'])
        
        lnld_filtered.write_parquet(bnm_path / "ISLAMIC_LNLD.parquet")
        print("   ✅ Processed Islamic financing data")
        
    except FileNotFoundError:
        print("   ⚠️  NOTE: Islamic LNLD.csv not found")
        lnld_filtered = pl.DataFrame()

    # =========================================================================
    # DATA PROCESSING - Merge Islamic Transactions
    # =========================================================================
    print("🔄 MERGING ISLAMIC TRANSACTION DATA...")
    
    # Sort datasets for merge
    if not dpld_filtered.is_empty():
        dpld_sorted = dpld_filtered.sort(['ACCTNO', 'TRANDT', 'TRANAMT'])
    else:
        dpld_sorted = dpld_filtered

    if not lnld_filtered.is_empty():
        lnld_sorted = lnld_filtered.sort(['ACCTNO', 'TRANDT', 'TRANAMT'])
    else:
        lnld_sorted = lnld_filtered

    # Merge Islamic transactions
    if not lnld_sorted.is_empty() and not dpld_sorted.is_empty():
        islamic_tranx_df = lnld_sorted.join(
            dpld_sorted, 
            on=['ACCTNO', 'TRANDT', 'TRANAMT'], 
            how='inner', 
            suffix='_dpld'
        )
        islamic_tranx_df.write_parquet(bnm_path / "ISLAMIC_TRANX.parquet")
        print("   ✅ Created Islamic transaction dataset")
    else:
        print("   ⚠️  Cannot merge - one or both Islamic datasets are empty")
        islamic_tranx_df = pl.DataFrame()

    # =========================================================================
    # DATA TRANX - Islamic Banking Processing
    # =========================================================================
    print("💰 PROCESSING ISLAMIC TRANSACTIONS...")
    if not islamic_tranx_df.is_empty():
        islamic_tranx_processed = islamic_tranx_df.with_columns([
            (pl.col('TRANAMT') / 1000).alias('TRANAMT1'),  # Convert to RM'000
            pl.lit(1).alias('VALUE'),
            # Apply Islamic transaction code formats
            pl.col('TRANCODE').map_dict(ISLAMIC_TCODE_FORMAT).alias('TRNXDESC')
        ]).with_columns([
            # Apply Islamic fee formats
            pl.when((pl.col('TRANCODE') == 760) & 
                   (pl.col('FEEPLAN').is_not_null()) & 
                   (pl.col('FEEPLAN') != ''))
            .then(pl.col('FEEPLAN').map_dict(ISLAMIC_FEEFMT_FORMAT))
            .otherwise(pl.col('TRNXDESC')).alias('TRNXDESC')
        ])
        print(f"   ✅ Processed {islamic_tranx_processed.height} Islamic transactions")
    else:
        islamic_tranx_processed = pl.DataFrame()
        print("   ⚠️  No Islamic transactions to process")

    # =========================================================================
    # REPORT 1: Summary of Cheques Issued (Islamic)
    # =========================================================================
    if not islamic_tranx_processed.is_empty():
        print("\n" + "="*80)
        print("📋 REPORT ID : EIIQEPC1")
        print("🕌 PUBLIC ISLAMIC BANK BERHAD")
        print(f"📅 CHEQUES ISSUED BY THE BANK AS AT {RDATE}")
        print("="*80)
        
        n_cheques = islamic_tranx_processed.height
        sum_value = islamic_tranx_processed['TRANAMT1'].sum()
        
        report1_data = [
            ["CHEQUES ISSUED", f"{n_cheques:16d}", f"{sum_value:16.2f}"]
        ]
        
        print(tabulate(report1_data, 
                      headers=['', 'NUMBER OF CHEQUES', "VALUE OF CHEQUES (RM'000)"],
                      tablefmt='grid'))

        # =========================================================================
        # REPORT 2: By Number of Cheques (Islamic)
        # =========================================================================
        print("\n" + "="*80)
        print("📋 REPORT ID : EIIQEPC1")
        print("🕌 PUBLIC ISLAMIC BANK BERHAD")
        print(f"📊 ALL PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}")
        print("="*80)
        
        # Group by Islamic transaction description
        tran1_df = islamic_tranx_processed.group_by('TRNXDESC').agg([
            pl.len().alias('UNIT'),
            pl.col('TRANAMT1').sum().alias('TRANAMT1_SUM')
        ])
        
        # Sort by descending unit count
        tran2_df = tran1_df.sort('UNIT', descending=True)
        tran_df = tran2_df.with_columns([
            pl.int_range(1, tran2_df.height + 1).alias('COUNT')
        ])
        
        # Generate report data
        report2_data = []
        for row in tran_df.rows():
            report2_data.append([
                row['COUNT'],
                row['TRNXDESC'],
                f"{row['UNIT']:16d}",
                f"{row['TRANAMT1_SUM']:16.2f}"
            ])
        
        print(tabulate(report2_data,
                      headers=['NO', 'PURPOSE', 'UNIT', "VALUE (RM'000)"],
                      tablefmt='grid'))

        # =========================================================================
        # REPORT 3: By Value of Cheques (Islamic)
        # =========================================================================
        print("\n" + "="*80)
        print("📋 REPORT ID : EIIQEPC1")
        print("🕌 PUBLIC ISLAMIC BANK BERHAD")
        print(f"💵 ALL PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}")
        print("="*80)
        
        # Group by value for Islamic transactions
        tran1b_df = islamic_tranx_processed.group_by('TRNXDESC').agg([
            pl.len().alias('UNIT'),
            pl.col('TRANAMT1').sum().alias('TRANAMT1_SUM')
        ])
        
        # Sort by descending value
        tran2b_df = tran1b_df.sort('TRANAMT1_SUM', descending=True)
        tranb_df = tran2b_df.with_columns([
            pl.int_range(1, tran2b_df.height + 1).alias('COUNT')
        ])
        
        # Generate value-based report
        report3_data = []
        for row in tranb_df.rows():
            report3_data.append([
                row['COUNT'],
                row['TRNXDESC'],
                f"{row['UNIT']:16d}",
                f"{row['TRANAMT1_SUM']:16.2f}"
            ])
        
        print(tabulate(report3_data,
                      headers=['NO', 'PURPOSE', 'UNIT', "VALUE (RM'000)"],
                      tablefmt='grid'))

        # =========================================================================
        # REPORT 4: By Islamic Branch
        # =========================================================================
        print("\n" + "="*80)
        print("📋 REPORT ID : EIIQEPC1")
        print("🕌 PUBLIC ISLAMIC BANK BERHAD")
        print(f"🏢 ALL PAYMENTS BY ISLAMIC BRANCH AS AT {RDATE}")
        print("="*80)
        
        # Group by Islamic branch and transaction type
        islamic_branch_summary = islamic_tranx_processed.group_by(['COSTCTR', 'TRNXDESC']).agg([
            pl.len().alias('UNIT'),
            pl.col('TRANAMT1').sum().alias('VALUE')
        ]).sort(['COSTCTR', 'TRNXDESC'])
        
        report4_data = []
        for row in islamic_branch_summary.rows():
            report4_data.append([
                f"IB{row['COSTCTR']}",  # Prefix with IB for Islamic Branch
                row['TRNXDESC'],
                f"{row['UNIT']:16d}",
                f"{row['VALUE']:16.2f}"
            ])
        
        print(tabulate(report4_data,
                      headers=['ISLAMIC BRANCH', 'PURPOSE', 'UNIT', "VALUE (RM'000)"],
                      tablefmt='grid'))
        
        # =========================================================================
        # Save Islamic Report Summary
        # =========================================================================
        summary_stats = {
            'report_date': RDATE,
            'total_cheques': n_cheques,
            'total_value_000': sum_value,
            'islamic_branches_count': islamic_branch_summary['COSTCTR'].n_unique(),
            'transaction_types_count': islamic_tranx_processed['TRNXDESC'].n_unique()
        }
        
        print(f"\n✅ ISLAMIC REPORT SUMMARY:")
        print(f"   📅 Report Date: {summary_stats['report_date']}")
        print(f"   📄 Total Cheques: {summary_stats['total_cheques']:,}")
        print(f"   💰 Total Value: RM {summary_stats['total_value_000']:,.2f} thousand")
        print(f"   🏢 Islamic Branches: {summary_stats['islamic_branches_count']}")
        print(f"   🔄 Transaction Types: {summary_stats['transaction_types_count']}")
        
    else:
        print("\n⚠️  NO ISLAMIC TRANSACTION DATA AVAILABLE FOR REPORTING")

    print("\n🎉 ISLAMIC CHEQUES ISSUED REPORT COMPLETED SUCCESSFULLY")
    return islamic_tranx_processed if not islamic_tranx_processed.is_empty() else None

# =============================================================================
# Main Execution
# =============================================================================
if __name__ == "__main__":
    print("🕌 STARTING PUBLIC ISLAMIC BANK CHEQUES REPORT")
    print("=" * 60)
    
    try:
        result = process_islamic_cheques_report()
        if result is not None:
            print(f"\n✅ Final Islamic transactions count: {result.height}")
        else:
            print("\n⚠️  No Islamic transactions processed")
            
    except Exception as e:
        print(f"\n❌ ERROR in Islamic Cheques Report: {e}")
        raise
    
    print("=" * 60)
    print("🕌 PUBLIC ISLAMIC BANK REPORTING COMPLETED")
