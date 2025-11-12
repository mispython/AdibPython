import polars as pl
import pandas as pd
from pathlib import Path
import datetime
from tabulate import tabulate

# Configuration
loan_path = Path("LOAN")
bnm_path = Path("BNM")
dpld_path = Path("DPLD")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE;
reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")

# Process REPTDATE with SELECT/WHEN logic
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
REPTYEAR = first_row['REPTDATE'].strftime('%Y')  # YEAR4.
REPTMON = f"{first_row['REPTDATE'].month:02d}"  # Z2.
REPTDAY = f"{first_row['REPTDATE'].day:02d}"  # Z2.
RDATE = first_row['REPTDATE'].strftime('%d/%m/%Y')  # DDMMYY10.
PREVDT = first_row['PREVDATE']
REPTDT = first_row['REPTDATE']

print(f"REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}, REPTDAY: {REPTDAY}")
print(f"RDATE: {RDATE}, PREVDT: {PREVDT}, REPTDT: {REPTDT}")

# PROC FORMAT equivalent - Create format dictionaries
DESC_FORMAT = {
    1: 'CHEQUES ISSUED'
}

TCODE_FORMAT = {
    310: 'LOAN DISBURSEMENT',
    750: 'PRINCIPAL INCREASE (PROGRESSIVE LOAN RELEASE)',
    752: 'DEBITING FOR INSURANCE PREMIUM',
    753: 'DEBITING FOR LEGAL FEE',
    754: 'DEBITING FOR OTHER PAYMENTS',
    760: 'MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY'
}

FEEFMT_FORMAT = {
    'QR': 'QUIT RENT',
    'LF': 'LEGAL FEE & DISBURSEMENT',
    'VA': 'VALUATION FEE',
    'IP': 'INSURANCE PREMIUM',
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

# DATA BNM.DPLD;
try:
    dpld_df = pl.read_parquet(dpld_path / f"DPLD{REPTMON}.parquet")
    # IF (&PREVDT<=REPTDATE<=&REPTDT);
    dpld_filtered = dpld_df.filter(
        (pl.col('REPTDATE') >= PREVDT) & (pl.col('REPTDATE') <= REPTDT)
    )
    dpld_filtered.write_parquet(bnm_path / "DPLD.parquet")
    print(f"Loaded and filtered DPLD data for month {REPTMON}")
except FileNotFoundError:
    print(f"NOTE: DPLD{REPTMON}.parquet not found")
    dpld_filtered = pl.DataFrame()

# DATA BNM.LNLD;
# INFILE LNLD MISSOVER; - Assuming CSV format
try:
    lnld_df = pl.read_csv("LNLD.csv", null_values=[""], infer_schema_length=10000)
    
    # Apply the same input parsing logic
    lnld_processed = lnld_df.with_columns([
        pl.col('line').str.slice(0, 11).str.strip().cast(pl.Int64).alias('ACCTNO'),        # @001 ACCTNO 11.
        pl.col('line').str.slice(12, 5).str.strip().cast(pl.Int64).alias('NOTENO'),        # @013 NOTENO 5.
        pl.col('line').str.slice(18, 7).str.strip().cast(pl.Int64).alias('COSTCTR'),       # @019 COSTCTR 7.
        pl.col('line').str.slice(26, 3).str.strip().alias('NOTETYPE'),                     # @027 NOTETYPE 3.
        pl.col('line').str.slice(30, 8).str.strip().alias('TRANDT_STR'),                   # @031 TRANDT DDMMYY8.
        pl.col('line').str.slice(46, 3).str.strip().cast(pl.Int64).alias('TRANCODE'),      # @047 TRANCODE 3.
        pl.col('line').str.slice(50, 3).str.strip().cast(pl.Int64).alias('SEQNO')          # @051 SEQNO 3.
    ]).with_columns([
        # Convert DDMMYY8 to date
        pl.when(pl.col('TRANDT_STR').str.len_bytes() == 8)
        .then(pl.col('TRANDT_STR').str.strptime(pl.Date, format='%d%m%y'))
        .otherwise(pl.lit(None)).alias('TRANDT')
    ])
    
    # Conditional input based on TRANCODE
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
        pl.col('line').str.slice(60, 18).str.strip().cast(pl.Float64).alias('TRANAMT'),    # @061 TRANAMT 18.2
        pl.col('line').str.slice(79, 3).str.strip().cast(pl.Int64).alias('SOURCE')         # @080 SOURCE 3.
    ])
    
    # Apply filters
    lnld_filtered = lnld_processed.filter(
        ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)) &
        (~pl.col('COSTCTR').is_in([4043, 4048]))
    ).drop(['line', 'TRANDT_STR', 'fee_info'])
    
    lnld_filtered.write_parquet(bnm_path / "LNLD.parquet")
    print("Loaded and processed LNLD data")
    
except FileNotFoundError:
    print("NOTE: LNLD.csv not found")
    lnld_filtered = pl.DataFrame()

# PROC SORT equivalent
if not dpld_filtered.is_empty():
    dpld_sorted = dpld_filtered.sort(['ACCTNO', 'TRANDT', 'TRANAMT'])
else:
    dpld_sorted = dpld_filtered

if not lnld_filtered.is_empty():
    lnld_sorted = lnld_filtered.sort(['ACCTNO', 'TRANDT', 'TRANAMT'])
else:
    lnld_sorted = lnld_filtered

# DATA BNM.TRANX - MERGE equivalent
if not lnld_sorted.is_empty() and not dpld_sorted.is_empty():
    tranx_df = lnld_sorted.join(
        dpld_sorted, 
        on=['ACCTNO', 'TRANDT', 'TRANAMT'], 
        how='inner', 
        suffix='_dpld'
    )
    tranx_df.write_parquet(bnm_path / "TRANX.parquet")
    print("Created TRANX dataset from merged LNLD and DPLD")
else:
    print("NOTE: Cannot merge - one or both datasets are empty")
    tranx_df = pl.DataFrame()

# DATA TRANX - FOR PBB
if not tranx_df.is_empty():
    tranx_processed = tranx_df.with_columns([
        (pl.col('TRANAMT') / 1000).alias('TRANAMT1'),  # TRANAMT1 = TRANAMT/1000;
        pl.lit(1).alias('VALUE'),                      # VALUE = 1;
        # TRNXDESC = PUT(TRANCODE,TCODE.);
        pl.col('TRANCODE').map_dict(TCODE_FORMAT).alias('TRNXDESC')
    ]).with_columns([
        # IF TRANCODE = 760 AND FEEPLAN NE ' ' THEN TRNXDESC = PUT(FEEPLAN,$FEEFMT.);
        pl.when((pl.col('TRANCODE') == 760) & (pl.col('FEEPLAN').is_not_null()) & (pl.col('FEEPLAN') != ''))
        .then(pl.col('FEEPLAN').map_dict(FEEFMT_FORMAT))
        .otherwise(pl.col('TRNXDESC')).alias('TRNXDESC')
    ])
else:
    tranx_processed = pl.DataFrame()

# PROC TABULATE equivalent - Report 1
if not tranx_processed.is_empty():
    print("\n" + "="*80)
    print("REPORT ID : EIBQEPC1")
    print("PUBLIC BANK BERHAD")
    print(f"CHEQUES ISSUED BY THE BANK AS AT {RDATE}")
    print("="*80)
    
    # Calculate summary statistics
    n_cheques = tranx_processed.height
    sum_value = tranx_processed['TRANAMT1'].sum()
    
    report1_data = [
        ["CHEQUES ISSUED", f"{n_cheques:16d}", f"{sum_value:16.2f}"]
    ]
    
    print(tabulate(report1_data, 
                   headers=['', 'NUMBER OF CHEQUES', "VALUE OF CHEQUES (RM'000)"],
                   tablefmt='grid'))
    
    # PROC SUMMARY equivalent - Report 2 preparation
    # Group by TRNXDESC and calculate summary
    tran1_df = tranx_processed.group_by('TRNXDESC').agg([
        pl.len().alias('UNIT'),
        pl.col('TRANAMT1').sum().alias('TRANAMT1_SUM')
    ])
    
    # PROC SORT equivalent - sort by descending UNIT
    tran2_df = tran1_df.sort('UNIT', descending=True)
    
    # DATA TRAN equivalent - add COUNT
    tran_df = tran2_df.with_columns([
        pl.int_range(1, tran2_df.height + 1).alias('COUNT')
    ])
    
    # Merge back with original data
    xxx_df = tran_df.select(['COUNT', 'TRNXDESC'])
    tranx1_df = tranx_processed.join(xxx_df, on='TRNXDESC', how='left').sort('COUNT')
    
    # Report 2 - By number of cheques
    print("\n" + "="*80)
    print("REPORT ID : EIBQEPC1")
    print("PUBLIC BANK BERHAD")
    print(f"ALL PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}")
    print("="*80)
    
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
    
    # PROC SUMMARY equivalent - Report 3 preparation
    # Group by TRNXDESC and calculate summary (by value)
    tran1b_df = tranx_processed.group_by('TRNXDESC').agg([
        pl.len().alias('UNIT'),
        pl.col('TRANAMT1').sum().alias('TRANAMT1_SUM')
    ])
    
    # PROC SORT equivalent - sort by descending TRANAMT1_SUM
    tran2b_df = tran1b_df.sort('TRANAMT1_SUM', descending=True)
    
    # DATA TRAN equivalent - add COUNT
    tranb_df = tran2b_df.with_columns([
        pl.int_range(1, tran2b_df.height + 1).alias('COUNT')
    ])
    
    # Merge back with original data
    xxxb_df = tranb_df.select(['COUNT', 'TRNXDESC'])
    tranx2_df = tranx_processed.join(xxxb_df, on='TRNXDESC', how='left').sort('COUNT')
    
    # Report 3 - By value of cheques
    print("\n" + "="*80)
    print("REPORT ID : EIBQEPC1")
    print("PUBLIC BANK BERHAD")
    print(f"ALL PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}")
    print("="*80)
    
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
    
    # Report 4 - By branch
    print("\n" + "="*80)
    print("REPORT ID : EIBQEPC1")
    print("PUBLIC BANK BERHAD")
    print(f"ALL PAYMENTS BY BRANCH AS AT {RDATE}")
    print("="*80)
    
    # Group by COSTCTR and TRNXDESC
    branch_summary = tranx_processed.group_by(['COSTCTR', 'TRNXDESC']).agg([
        pl.len().alias('UNIT'),
        pl.col('TRANAMT1').sum().alias('VALUE')
    ]).sort(['COSTCTR', 'TRNXDESC'])
    
    report4_data = []
    for row in branch_summary.rows():
        report4_data.append([
            row['COSTCTR'],
            row['TRNXDESC'],
            f"{row['UNIT']:16d}",
            f"{row['VALUE']:16.2f}"
        ])
    
    print(tabulate(report4_data,
                   headers=['COSTCTR', 'PURPOSE', 'UNIT', "VALUE (RM'000)"],
                   tablefmt='grid'))
    
else:
    print("No data available for reporting")

print("\nPROCESSING COMPLETED SUCCESSFULLY")
