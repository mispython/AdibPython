"""
SAS to Python Translation - EIMDISRP
Processes loan disbursement and repayment data for monthly reporting
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, date
import numpy as np

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

# Create directories
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Libraries (SAS libnames)
LOAN_DIR = INPUT_DIR / "loan"
ILOAN_DIR = INPUT_DIR / "iloan"
FORATE_DIR = INPUT_DIR / "forate"
FEE_DIR = INPUT_DIR / "fee"
BNM_DIR = INPUT_DIR / "bnm"
IBNM_DIR = INPUT_DIR / "ibnm"
SASD_DIR = INPUT_DIR / "sasd"
ISASD_DIR = INPUT_DIR / "isasd"
LIMIT_DIR = INPUT_DIR / "limit"
ILIMIT_DIR = INPUT_DIR / "ilimit"
DEPOSIT_DIR = INPUT_DIR / "deposit"
IDEPOSIT_DIR = INPUT_DIR / "ideposit"
DAILY_DIR = INPUT_DIR / "daily"
IDAILY_DIR = INPUT_DIR / "idaily"
DISPAY_DIR = OUTPUT_DIR / "disp ay"
IDISPAY_DIR = OUTPUT_DIR / "idispay"

for dir_path in [DISPAY_DIR, IDISPAY_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE HANDLING AND MACRO VARIABLES
# =============================================================================

def sas_date_to_datetime(sas_date):
    """Convert SAS numeric date to datetime"""
    if sas_date is None or sas_date <= 0:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def datetime_to_sas_date(dt):
    """Convert datetime to SAS numeric date"""
    if dt is None:
        return 0
    reference = datetime(1960, 1, 1)
    return (dt - reference).days

def parse_mmddyyyy(date_str):
    """Parse MMDDYYYY string to date"""
    if not date_str or len(date_str) != 8:
        return None
    try:
        month = int(date_str[0:2])
        day = int(date_str[2:4])
        year = int(date_str[4:8])
        return datetime(year, month, day).date()
    except:
        return None

def date_to_julian(dt):
    """Convert date to SAS Julian date"""
    if dt is None:
        return 0
    year = dt.year
    day_of_year = dt.timetuple().tm_yday
    return year * 1000 + day_of_year

def julian_to_date(julian):
    """Convert SAS Julian date to datetime"""
    if julian is None or julian <= 0:
        return None
    julian_str = str(int(julian)).zfill(7)
    year = int(julian_str[0:4])
    day_of_year = int(julian_str[4:7])
    return datetime(year, 1, 1) + timedelta(days=day_of_year - 1)

def setup_date_variables():
    """Calculate report date and create macro variable equivalents"""
    reptdate_df = pl.read_parquet(LOAN_DIR / "REPTDATE.parquet")
    
    # Get REPTDATE from first row
    reptdate_val = reptdate_df.select(pl.first()).item()
    reptdate = sas_date_to_datetime(reptdate_val)
    
    mm = reptdate.month
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12
    
    stdate = date(reptdate.year, mm, 1)
    styear = date(reptdate.year, 1, 1)
    loopdt = reptdate - timedelta(days=61)
    
    return {
        'REPTDATE': reptdate,
        'REPTDATE_SAS': reptdate_val,
        'MM': mm,
        'MM2': mm2,
        'NOWK': '4',
        'NOWK1': '1',
        'NOWK2': '2',
        'NOWK3': '3',
        'REPTYEAR': reptdate.strftime('%y'),
        'REPTMON': str(mm).zfill(2),
        'REPTDAY': str(reptdate.day).zfill(2),
        'REPTMON2': str(mm2).zfill(2),
        'RDATE': reptdate.strftime('%d%m%Y'),
        'SDATE': stdate,
        'SDATE_SAS': datetime_to_sas_date(datetime.combine(stdate, datetime.min.time())),
        'TDATE': reptdate,
        'TDATE_SAS': reptdate_val,
        'STYEAR': styear,
        'STYEAR_SAS': datetime_to_sas_date(datetime.combine(styear, datetime.min.time())),
        'LOOPDT': loopdt,
        'LOOPDT_SAS': datetime_to_sas_date(datetime.combine(loopdt, datetime.min.time()))
    }

# =============================================================================
# PRODUCT AND PURPOSE CODE LISTS
# =============================================================================

# HP Loan types
HP = ['HPLN', 'HPCS', 'HPSV', 'HPCS2', 'HPLN2', 'HPUS', 
      'AUTOL', 'AUTOL2', 'BOB', 'MBIKE', 'MBIKE2', 'HVYEQ']

# FCY Products
FCY = [110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
       123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135,
       136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148,
       149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161,
       162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174,
       175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187,
       188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]

# =============================================================================
# FOREX RATE PROCESSING
# =============================================================================

def process_forex_rates(macro_vars):
    """Process forex rates and create daily rate format"""
    
    # Create date range
    start_date = macro_vars['LOOPDT']
    end_date = macro_vars['TDATE']
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    date_df = pl.DataFrame({'REPTDATE': [datetime_to_sas_date(datetime.combine(d, datetime.min.time())) 
                                         for d in date_range]})
    
    # Read forex rates
    fxrate_df = pl.read_parquet(FORATE_DIR / "FORATEBKP.parquet")
    
    # Filter by date range
    fxrate_df = fxrate_df.filter(
        (pl.col('REPTDATE') >= macro_vars['LOOPDT_SAS']) &
        (pl.col('REPTDATE') <= macro_vars['TDATE_SAS'])
    )
    
    # Get latest rate per currency
    fxrate_df = fxrate_df.sort(['CURCODE', 'REPTDATE'], descending=True)
    fxrate_df = fxrate_df.unique(subset=['CURCODE'])
    
    # Create working dates with latest available rate
    workdte = date_df.with_columns(pl.lit(None).alias('REPXDATE'))
    
    # Join with fxrate to get available dates
    workdte = workdte.join(
        fxrate_df.select('REPTDATE').unique(),
        on='REPTDATE',
        how='left',
        indicator='_merge'
    )
    
    workdte = workdte.with_columns([
        pl.when(pl.col('_merge') == 'both')
        .then(pl.col('REPTDATE'))
        .otherwise(None)
        .alias('REPXDATE')
    ]).drop('_merge')
    
    # Forward fill REPXDATE
    workdte = workdte.with_columns([
        pl.col('REPXDATE').forward_fill().alias('REPXDATE')
    ])
    
    # Create daily forex rates
    dlyforate = workdte.join(
        fxrate_df.select(['REPTDATE', 'CURCODE', 'SPOTRATE']),
        left_on='REPXDATE',
        right_on='REPTDATE',
        how='left'
    )
    
    dlyforate = dlyforate.drop_nulls('SPOTRATE')
    
    # Create format control dataset
    dlyforate = dlyforate.with_columns([
        (pl.col('CURCODE').cast(pl.Utf8) + pl.col('REPTDATE').cast(pl.Utf8)).alias('KEY'),
        pl.col('SPOTRATE').alias('LABEL')
    ])
    
    return dlyforate

# =============================================================================
# HISTORICAL TRANSACTION PROCESSING
# =============================================================================

def process_histfile(macro_vars):
    """Process HISTFILE data"""
    
    hist_df = pl.read_parquet(INPUT_DIR / "HISTFILE.parquet")
    
    # Filter out specific transactions
    hist_df = hist_df.filter(
        ~((pl.col('TRANCODE') == 660) & (pl.col('CHANNEL') == 5))
    )
    
    # Calculate TOTAMT
    hist_df = hist_df.with_columns([
        pl.when(pl.col('TRANCODE').is_in([666, 726]))
        .then(
            pl.when(pl.col('ACCRADJ') == 0)
            .then(pl.col('TAMT'))
            .otherwise(pl.col('ACCRADJ'))
        )
        .when(pl.col('TRANCODE') == 999)
        .then(pl.col('HISTINT'))
        .otherwise(pl.col('TAMT'))
        .alias('TOTAMT')
    ])
    
    # Adjust for 800-899 codes
    hist_df = hist_df.with_columns([
        pl.when(
            (pl.col('TRANCODE').is_between(800, 899)) & 
            (pl.col('TAMT') == 0)
        )
        .then(pl.col('ACCRADJ'))
        .otherwise(pl.col('TOTAMT'))
        .alias('TOTAMT')
    ])
    
    # Identify negative/positive values
    neg_codes = [310, 726, 750, 751, 752, 753, 754, 756, 758, 760, 761]
    
    hist_df = hist_df.with_columns([
        pl.when(
            pl.col('TRANCODE').is_in(neg_codes) & 
            ~((pl.col('TRANCODE') == 726) & (pl.col('REVERSED') == 'R'))
        )
        .then(pl.col('TOTAMT') * -1)
        .otherwise(pl.col('TOTAMT'))
        .alias('TOTAMT')
    ])
    
    # Convert dates
    hist_df = hist_df.with_columns([
        pl.col('EFFDATE').apply(julian_to_date).alias('EFFDATE'),
        pl.when(pl.col('POSTDT').is_not_null() & (pl.col('POSTDT') != 0))
        .then(
            pl.col('POSTDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format='%m%d%Y')
        )
        .otherwise(None)
        .alias('POSTDATE')
    ])
    
    # Split into two datasets
    hist_yr = hist_df.filter(
        (pl.col('ACCTNO') > 0) & 
        (pl.col('POSTDATE') >= macro_vars['STYEAR'])
    )
    
    hist = hist_df.filter(
        (pl.col('ACCTNO') > 0) & 
        ((pl.col('EFFDATE') >= macro_vars['SDATE']) | 
         (pl.col('POSTDATE') >= macro_vars['SDATE']))
    )
    
    return hist_yr, hist

# =============================================================================
# FEE TRANSACTION PROCESSING
# =============================================================================

def process_feetran(macro_vars):
    """Process fee transactions"""
    
    feetran_file = FEE_DIR / f"FEETRAN{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR']}.parquet"
    
    if not feetran_file.exists():
        print(f"Warning: {feetran_file} not found")
        return None
    
    feetran_df = pl.read_parquet(feetran_file).filter(
        (pl.col('TRANCODE') == 680) &
        (pl.col('REVERSED') != 'R')
    ).with_columns([
        pl.lit('10').alias('REPAY_TYPE_CD'),
        pl.col('ACTDATE').alias('POSTDATE'),
        pl.col('TRANAMT').alias('TOTAMT')
    ])
    
    return feetran_df

# =============================================================================
# DISBURSEMENT PROCESSING FUNCTIONS
# =============================================================================

def process_disb_base(df, trans_type):
    """Base function for disbursement processing"""
    if df is None or len(df) == 0:
        return None
    
    df = df.sort(['ACCTNO', 'TOTAMT', 'EFFDATE'])
    
    # Add count within group
    df = df.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'TOTAMT', 'EFFDATE']).alias('COUNT')
    ])
    
    return df

def process_disb310(disbmthtest, lnnote_df):
    """Process disbursement type 310"""
    
    disb310 = disbmthtest.filter(
        (pl.col('TRANCODE') == 310) &
        (pl.col('REVERSED') != 'R')
    ).with_columns([
        pl.col('NOTENO').alias('NOTENEW')
    ])
    
    disb310 = process_disb_base(disb310)
    
    # Adjust amount for NTINT = 'A'
    disb310 = disb310.with_columns([
        pl.when(pl.col('NTINT') == 'A')
        .then(pl.col('NETPROC') * -1)
        .otherwise(pl.col('TOTAMT'))
        .alias('TOTAMT')
    ])
    
    # Calculate TOTAMT_HP for specific loan types
    hp_loan_types = [700, 705]  # Conventional
    if 'LOANTYPE' in disb310.columns:
        disb310 = disb310.with_columns([
            pl.when(pl.col('LOANTYPE').is_in(hp_loan_types))
            .then((-(pl.col('NETPROC') + pl.col('FEEAMT').fill_null(0))).round(2))
            .otherwise(None)
            .alias('TOTAMT_HP')
        ])
    
    return disb310

def process_disb652(disbmthtest):
    """Process disbursement type 652"""
    
    disb652 = disbmthtest.filter(
        (pl.col('TRANCODE') == 652) &
        (pl.col('REVERSED') != 'R')
    ).with_columns([
        (pl.col('TOTAMT') * -1).alias('TOTAMT')
    ])
    
    disb652 = process_disb_base(disb652)
    
    # Summarize by ACCTNO and EFFDATE
    disb652_sum = disb652.group_by(['ACCTNO', 'EFFDATE']).agg(
        pl.col('TOTAMT').sum().round(2).alias('TOTAMT')
    ).with_columns([
        pl.lit(1).alias('COUNT')
    ])
    
    return disb652, disb652_sum

def process_disb750(disbmthtest):
    """Process disbursement type 750"""
    
    disb750 = disbmthtest.filter(
        (pl.col('TRANCODE') == 750) &
        (pl.col('REVERSED') != 'R')
    ).with_columns([
        pl.col('NOTENO').alias('NOTENEW')
    ])
    
    return disb750

def process_repay610(disbmthtest):
    """Process repayment type 610"""
    
    repay610 = disbmthtest.filter(
        (pl.col('TRANCODE').is_in([610, 614, 615, 619, 650, 660, 661, 668, 680])) |
        ((pl.col('TRANCODE') == 612) & (pl.col('NTBRCH') == 130))
    ).with_columns([
        pl.when(pl.col('REVERSED') == 'R').then(1).otherwise(0).alias('IND_R')
    ])
    
    repay610 = repay610.sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'IND_R'], descending=True)
    
    # Add count within group
    repay610 = repay610.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ]).drop('IND_R')
    
    return repay610

def process_repay652(disbmthtest):
    """Process repayment type 652"""
    
    repay652 = disbmthtest.filter(
        (pl.col('REVERSED') == 'R') &
        ~pl.col('TRANCODE').is_in([610, 612, 614, 615, 619, 650, 660, 661, 668, 680])
    ).sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE'])
    
    # Add count within group
    repay652 = repay652.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE']).alias('COUNT')
    ])
    
    return repay652

def process_repay800(disbmthtest):
    """Process repayment type 800"""
    
    repay800 = disbmthtest.filter(
        pl.col('TRANCODE').is_in([800, 801, 810])
    ).with_columns([
        (pl.col('TOTAMT') * -1).alias('TOTAMT')
    ]).sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE'])
    
    # Add count within group
    repay800 = repay800.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE']).alias('COUNT')
    ])
    
    return repay800

def process_repay754(disbmthtest, disb750):
    """Process repayment type 754"""
    
    repay754 = disbmthtest.filter(
        pl.col('TRANCODE').is_in([726, 754, 757]) &
        (pl.col('REVERSED') != 'R')
    )
    
    if disb750 is not None:
        repay754 = pl.concat([repay754, disb750])
    
    repay754 = repay754.sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT'])
    
    # Add count within group
    repay754 = repay754.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    return repay754

def merge_disbursements(disb310, disb652, disb652_sum, disb750, lnnote_df, macro_vars, prefix=''):
    """Merge all disbursement datasets"""
    
    # Merge 310 with 652_sum
    disb310 = disb310.sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT'])
    disb652_sum = disb652_sum.sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT'])
    
    merged = disb310.join(
        disb652_sum.select(['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT']),
        on=['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT'],
        how='left',
        indicator='_merge'
    )
    
    disb310_out = merged.filter(pl.col('_merge') == 'left_only').drop('_merge')
    disb652t = merged.filter(pl.col('_merge') == 'both').drop('_merge')
    
    # Merge with disb652
    disb652t = disb652t.sort(['ACCTNO', 'EFFDATE'])
    disb652 = disb652.sort(['ACCTNO', 'EFFDATE'])
    
    noteconv1 = disb652t.join(
        disb652.select(['ACCTNO', 'EFFDATE', 'NOTENO']),
        on=['ACCTNO', 'EFFDATE'],
        how='inner'
    ).select(['ACCTNO', 'NOTENO', pl.col('NOTENO_right').alias('NOTENEW')])
    
    disb652 = disb652.join(
        noteconv1.select(['ACCTNO', 'NOTENEW']),
        on='ACCTNO',
        how='anti'
    )
    
    # Second merge
    disb310 = disb310_out.sort(['ACCTNO', 'TOTAMT', 'EFFDATE'])
    disb652 = disb652.sort(['ACCTNO', 'TOTAMT', 'EFFDATE'])
    
    merged2 = disb310.join(
        disb652.select(['ACCTNO', 'TOTAMT', 'EFFDATE']),
        on=['ACCTNO', 'TOTAMT', 'EFFDATE'],
        how='left',
        indicator='_merge'
    )
    
    disb310_out2 = merged2.filter(pl.col('_merge') == 'left_only').drop('_merge')
    noteconv2 = merged2.filter(pl.col('_merge') == 'both').drop('_merge').select(['ACCTNO', 'NOTENO'])
    noteconv2 = noteconv2.with_columns(pl.col('NOTENO').alias('NOTENEW'))
    disb652 = disb652.join(noteconv2.select(['ACCTNO']), on='ACCTNO', how='anti')
    
    # Third merge on TOTAMT_HP
    disb310_out2 = disb310_out2.sort(['ACCTNO', 'TOTAMT_HP'])
    disb652 = disb652.rename({'TOTAMT': 'TOTAMT_HP'}).sort(['ACCTNO', 'TOTAMT_HP'])
    
    merged3 = disb310_out2.join(
        disb652.select(['ACCTNO', 'TOTAMT_HP']),
        on=['ACCTNO', 'TOTAMT_HP'],
        how='left',
        indicator='_merge'
    )
    
    disb310_final = merged3.filter(pl.col('_merge') == 'left_only').drop('_merge')
    noteconv2_1 = merged3.filter(pl.col('_merge') == 'both').drop('_merge').select(['ACCTNO', 'NOTENO'])
    noteconv2_1 = noteconv2_1.with_columns(pl.col('NOTENO').alias('NOTENEW'))
    disb652_final = merged3.filter(pl.col('_merge') == 'both').drop('_merge')
    disb652_final = disb652_final.rename({'TOTAMT_HP': 'TOTAMT'})
    
    # Combine note conversions
    noteconv = pl.concat([
        df for df in [noteconv1, noteconv2, noteconv2_1] if df is not None and len(df) > 0
    ]).unique(subset=['ACCTNO', 'NOTENO'])
    
    return disb310_final, disb652_final, noteconv

# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_conventional(macro_vars):
    """Process conventional loan disbursements and repayments"""
    
    print("\n" + "="*60)
    print("Processing Conventional Loans")
    print("="*60)
    
    # Step 1: Create date sequence for forex
    print("  Creating forex rates...")
    dlyforate = process_forex_rates(macro_vars)
    
    # Step 2: Process HISTFILE
    print("  Processing HISTFILE...")
    hist_yr, hist = process_histfile(macro_vars)
    
    # Step 3: Process FEETRAN
    print("  Processing FEETRAN...")
    feetran = process_feetran(macro_vars)
    
    # Step 4: Create DISBMTHTEST
    print("  Creating DISBMTHTEST...")
    disbmthtest = pl.concat([df for df in [hist_yr, feetran] if df is not None])
    disbmthtest = disbmthtest.filter(
        (pl.col('POSTDATE') >= macro_vars['SDATE']) &
        (pl.col('POSTDATE') <= macro_vars['TDATE'])
    ).with_columns([
        (pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('REBATE'),
        (pl.col('HISTPRIN').fill_null(0) + pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('HISTPRIN'),
        pl.when(pl.col('REPAY_TYPE_CD') == '10').then(pl.col('TOTAMT')).otherwise(None).alias('REPAID10'),
        pl.col('TOTAMT').abs().alias('TOTAMT_CNT')
    ])
    
    # Step 5: Merge with LNNOTE
    print("  Merging with LNNOTE...")
    lnnote_df = pl.read_parquet(LOAN_DIR / "LNNOTE.parquet").select([
        'ACCTNO', 'NOTENO', 'NETPROC', 'NTINT', 'DELQCD', 'NTBRCH',
        'DNBFISME', 'CURCODE', 'LOANTYPE', 'FEEAMT', 'BONUSANO'
    ])
    
    disbmthtest = disbmthtest.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='inner')
    disbmthtest = disbmthtest.sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'BONUSANO'], descending=True)
    
    # Step 6: Process disbursements
    print("  Processing disbursements...")
    disb310 = process_disb310(disbmthtest, lnnote_df)
    disb652, disb652_sum = process_disb652(disbmthtest)
    disb750 = process_disb750(disbmthtest)
    
    # Step 7: Merge disbursements
    disb310_final, disb652_final, noteconv = merge_disbursements(
        disb310, disb652, disb652_sum, disb750, lnnote_df, macro_vars
    )
    
    # Step 8: Process repayments
    print("  Processing repayments...")
    repay610 = process_repay610(disbmthtest)
    repay652 = process_repay652(disbmthtest)
    repay800 = process_repay800(disbmthtest)
    
    # Remove overlapping records
    repay800 = repay800.join(
        repay652.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT'],
        how='anti'
    )
    
    # Recalculate count for repay800
    repay800 = repay800.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    # Merge repay800 with repay610
    repay610 = repay610.join(
        repay800.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'],
        how='anti'
    )
    
    # Process repay754
    repay754 = process_repay754(disbmthtest, disb750)
    
    # Merge repay754 with repay610
    repay754 = repay754.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    repay610 = repay610.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    merged_repay = repay610.join(
        repay754.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'],
        how='left',
        indicator='_merge'
    )
    
    repay_final = merged_repay.filter(pl.col('_merge') == 'left_only').drop('_merge')
    repay754x = merged_repay.filter(pl.col('_merge') == 'both').drop('_merge')
    
    # Extract disb750 from repay754x
    disb750_extra = repay754x.filter(pl.col('TRANCODE') == 750)
    
    # Update NOTENO using noteconv
    repay_final = repay_final.join(
        noteconv.select(['ACCTNO', 'NOTENO', 'NOTENEW']),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    ).with_columns([
        pl.when(pl.col('NOTENEW').is_not_null())
        .then(pl.col('NOTENEW'))
        .otherwise(pl.col('NOTENO'))
        .alias('NOTENO')
    ])
    
    # Step 9: Create disbursed dataset
    print("  Creating disbursed dataset...")
    disbursed = pl.concat([
        disb310_final.with_columns([(pl.col('TOTAMT') * -1).alias('DISBURSE')]),
        disb750_extra.with_columns([(pl.col('TOTAMT') * -1).alias('DISBURSE')])
    ]).filter(pl.col('REVERSED') != 'R')
    
    disbursed = disbursed.join(
        noteconv.select(['ACCTNO', 'NOTENO', 'NOTENEW']),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    ).with_columns([
        pl.when(pl.col('NOTENEW').is_not_null())
        .then(pl.col('NOTENEW'))
        .otherwise(pl.col('NOTENO'))
        .alias('NOTENO')
    ])
    
    # Step 10: Read loan files
    print("  Reading loan files...")
    lnwof = pl.read_parquet(BNM_DIR / f"LNWOF{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet")
    lnwod = pl.read_parquet(BNM_DIR / f"LNWOD{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet")
    plnwof = pl.read_parquet(BNM_DIR / f"LNWOF{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet")
    plnwod = pl.read_parquet(BNM_DIR / f"LNWOD{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet")
    loan2 = pl.read_parquet(BNM_DIR / f"LOAN{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet")
    loan_sasd = pl.read_parquet(SASD_DIR / f"LOAN{macro_vars['REPTMON']}.parquet")
    
    loan_combined = pl.concat([
        plnwof, plnwod, lnwof, lnwod, loan2, loan_sasd
    ]).unique(subset=['ACCTNO', 'NOTENO'])
    
    # Step 11: Merge with disbursed and repay
    print("  Creating final DISPAY dataset...")
    
    # Summarize disbursed
    disbursed_sum = disbursed.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('DISBURSE').sum(),
        pl.col('HISTINT').fill_null(0).sum().alias('HISTINT'),
        pl.col('REBATE').fill_null(0).sum().alias('REBATE'),
        pl.col('REPAID10').fill_null(0).sum().alias('REPAID10')
    ])
    
    # Summarize repay
    repay_sum = repay_final.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('REPAID').sum()
    ])
    
    # Combine
    disburse_sum = disbursed_sum.join(repay_sum, on=['ACCTNO', 'NOTENO'], how='outer')
    
    loan_final = loan_combined.join(disburse_sum, on=['ACCTNO', 'NOTENO'], how='inner')
    
    # Step 12: Create DISPAY for purpose code analysis
    print("  Creating purpose code analysis...")
    
    dispay = loan_final.filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226, 678, 679, 993, 996])
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'BALANCE', 'CENSUS', 'DNBFISME',
        'PRODCD', 'CUSTCD', 'AMTIND', 'SECTORCD', 'DISBURSE', 'REPAID', 'BRANCH',
        'HISTINT', 'REBATE', 'CCY', 'FORATE', 'REPAID10'
    ])
    
    # Step 13: Read ALM files
    print("  Processing ALM data...")
    
    alm1 = pl.read_parquet(BNM_DIR / f"LOAN{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet").filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226])
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD',
        'CUSTCD', 'AMTIND', 'SECTORCD', 'BRANCH', 'CCY', 'FORATE', 'DNBFISME'
    ]).rename({'BALANCE': 'LASTBAL', 'NOTETERM': 'LASTNOTE'})
    
    alm = pl.read_parquet(BNM_DIR / f"LOAN{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet").filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226])
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE',
        'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'SECTORCD', 'BRANCH',
        'CCY', 'FORATE', 'DNBFISME'
    ])
    
    # Step 14: Process rollovers
    print("  Processing rollovers...")
    
    # LNHIST for rate changes
    lnhist = pl.read_parquet(DISPAY_DIR / "LNHIST.parquet").filter(
        (pl.col('TRANCODE') == 400) &
        (pl.col('RATECHG') != 0)
    ).unique(subset=['ACCTNO', 'NOTENO'])
    
    alm = alm.join(lnnote_df.select(['ACCTNO', 'NOTENO', 'DELQCD']), on=['ACCTNO', 'NOTENO'], how='left')
    alm = alm.join(lnhist, on=['ACCTNO', 'NOTENO'], how='left')
    
    alm = alm.with_columns([
        pl.when(
            pl.col('DELQCD').is_null() & 
            (pl.col('ISSDTE') < macro_vars['SDATE_SAS'])
        )
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias('RATECHG')
    ])
    
    alm = alm.with_columns([
        pl.when(
            (pl.col('PRODCD').is_in(['34190', '34690'])) &
            (pl.col('RATECHG') == 'Y')
        )
        .then(pl.col('CURBAL'))
        .otherwise(0)
        .alias('ROLLOVER')
    ])
    
    # Step 15: Process OD rollovers
    over_dft = pl.read_parquet(LIMIT_DIR / "OVERDFT.parquet").filter(
        (pl.col('APPRLIMT') > 1) &
        (pl.col('LMTAMT') > 1)
    ).select(['ACCTNO', 'LMTID', 'REVIEWDT', 'LMTAMT'])
    
    pover_dft = pl.read_parquet(LIMIT_DIR / "POVERDFT.parquet").filter(
        (pl.col('APPRLIMT') > 1) &
        ~pl.col('REVIEWDT').is_in([0, None])
    ).select(['ACCTNO', 'LMTID', 'REVIEWDT'])
    
    limit = over_dft.join(
        pover_dft.rename({'REVIEWDT': 'PREVIEWDT'}),
        on=['ACCTNO', 'LMTID'],
        how='inner'
    )
    
    # Convert dates
    limit = limit.with_columns([
        pl.when(pl.col('REVIEWDT') > 0)
        .then(
            pl.col('REVIEWDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format='%m%d%Y')
        )
        .otherwise(None).alias('REVIEW'),
        pl.when(pl.col('PREVIEWDT') > 0)
        .then(
            pl.col('PREVIEWDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format='%m%d%Y')
        )
        .otherwise(None).alias('PREVIEW')
    ])
    
    limit = limit.with_columns([
        pl.col('REVIEW').alias('REVIEWDT'),
        pl.col('PREVIEW').alias('PREVIEWDT')
    ]).filter(pl.col('REVIEWDT') > pl.col('PREVIEWDT'))
    
    limit_sum = limit.group_by('ACCTNO').agg(pl.col('LMTAMT').sum().alias('ROLLOVER'))
    
    # Step 16: Process current account data
    ovdft = pl.read_parquet(DEPOSIT_DIR / "CURRENT.parquet").rename({'SECTOR': 'SECTORC'}).filter(
        ~pl.col('OPENIND').is_in(['B', 'C', 'P'])
    )
    
    # Complex purpose code mapping - simplified here
    ovdft = ovdft.with_columns([
        pl.col('SECTORC').cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8).str.zfill(4).alias('SECTOR'),
        pl.when(pl.col('PRODUCT') == 104).then(pl.lit('02'))
        .when(pl.col('PRODUCT') == 105).then(pl.lit('81'))
        .otherwise(pl.col('CUSTCODE').cast(pl.Utf8))
        .alias('CUSTCD'),
        pl.col('CURCODE').alias('CCY')
    ])
    
    # Join with limit
    limitx = ovdft.join(limit_sum, on='ACCTNO', how='inner')
    
    # Step 17: Final ALM merge
    alm = alm.join(dispay, on=['ACCTNO', 'NOTENO'], how='left')
    alm = alm.join(limitx, on='ACCTNO', how='left')
    
    # Step 18: Apply forex rates
    forate = pl.read_parquet(FORATE_DIR / "FORATEBKP.parquet").sort(
        ['CURCODE', 'REPTDATE'], descending=True
    ).unique(subset=['CURCODE']).select(['CURCODE', 'SPOTRATE'])
    
    forate_dict = {row['CURCODE']: row['SPOTRATE'] for row in forate.rows()}
    
    # Step 19: Create final DISPAYMTH
    print("  Creating DISPAYMTH...")
    
    dispaymth = alm.filter(
        (pl.col('DISBURSE') > 0) | (pl.col('REPAID') > 0) | (pl.col('ROLLOVER') > 0)
    ).with_columns([
        pl.col('CCY').apply(lambda c: forate_dict.get(c, 1.0)).alias('FORATE')
    ])
    
    # Apply forex for specific products
    dispaymth = dispaymth.with_columns([
        pl.when(
            (pl.col('PRODUCT').is_between(800, 851)) &
            (pl.col('CCY') != 'MYR')
        )
        .then(pl.col('DISBURSE') * pl.col('FORATE'))
        .otherwise(pl.col('DISBURSE'))
        .alias('DISBURSE'),
        pl.when(
            (pl.col('PRODUCT').is_between(800, 851)) &
            (pl.col('CCY') != 'MYR')
        )
        .then(pl.col('REPAID') * pl.col('FORATE'))
        .otherwise(pl.col('REPAID'))
        .alias('REPAID')
    ])
    
    # Set negative values to zero
    dispaymth = dispaymth.with_columns([
        pl.when(pl.col('REPAID') <= 0).then(0).otherwise(pl.col('REPAID')).alias('REPAID'),
        pl.when(pl.col('REPAID10') <= 0).then(0).otherwise(pl.col('REPAID10')).alias('REPAID10')
    ])
    
    # Set DNBFISME
    cust_exclude = ['04', '05', '06', '30', '31', '32', '33', '34', '35', '37', '38', '39', '40', '45']
    dispaymth = dispaymth.with_columns([
        pl.when(~pl.col('CUSTCD').cast(pl.Utf8).is_in(cust_exclude))
        .then(pl.lit('0'))
        .otherwise(pl.col('DNBFISME'))
        .alias('DNBFISME')
    ])
    
    # Save output
    output_file = DISPAY_DIR / f"DISPAYMTH{macro_vars['REPTMON']}.parquet"
    dispaymth.write_parquet(output_file)
    print(f"  Saved: {output_file}")
    
    return dispaymth

def process_islamic(macro_vars):
    """Process Islamic loan disbursements and repayments"""
    
    print("\n" + "="*60)
    print("Processing Islamic Loans")
    print("="*60)
    
    # Similar to conventional but with Islamic-specific paths and product ranges
    # This would follow the same logic but with ILOAN_DIR, IBNM_DIR, etc.
    # For brevity, I've outlined the structure but would implement similarly
    
    # Step 1: Create date sequence for forex (same as conventional)
    dlyforate = process_forex_rates(macro_vars)
    
    # Step 2: Process HISTFILE (same data but filtered differently in later steps)
    hist_yr, hist = process_histfile(macro_vars)
    
    # Step 3: Process FEETRAN (same)
    feetran = process_feetran(macro_vars)
    
    # Step 4: Create DISBMTHTEST
    disbmthtest = pl.concat([df for df in [hist_yr, feetran] if df is not None])
    disbmthtest = disbmthtest.filter(
        (pl.col('POSTDATE') >= macro_vars['SDATE']) &
        (pl.col('POSTDATE') <= macro_vars['TDATE'])
    ).with_columns([
        (pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('REBATE'),
        (pl.col('HISTPRIN').fill_null(0) + pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('HISTPRIN'),
        pl.when(pl.col('REPAY_TYPE_CD') == '10').then(pl.col('TOTAMT')).otherwise(None).alias('REPAID10'),
        pl.col('TOTAMT').abs().alias('TOTAMT_CNT')
    ])
    
    # Step 5: Merge with ILOAN.LNNOTE
    lnnote_df = pl.read_parquet(ILOAN_DIR / "LNNOTE.parquet").select([
        'ACCTNO', 'NOTENO', 'NETPROC', 'NTINT', 'DELQCD', 'NTBRCH',
        'DNBFISME', 'CURCODE', 'LOANTYPE', 'FEEAMT', 'BONUSANO'
    ])
    
    disbmthtest = disbmthtest.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='inner')
    disbmthtest = disbmthtest.sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'BONUSANO'], descending=True)
    
    # Step 6: Process disbursements (similar logic but with Islamic loan types)
    disb310 = process_disb310(disbmthtest, lnnote_df)
    # Override TOTAMT_HP for Islamic loan types
    islamic_loan_types = [128, 130]
    if 'LOANTYPE' in disb310.columns:
        disb310 = disb310.with_columns([
            pl.when(pl.col('LOANTYPE').is_in(islamic_loan_types))
            .then((-(pl.col('NETPROC') + pl.col('FEEAMT').fill_null(0))).round(2))
            .otherwise(pl.col('TOTAMT_HP'))
            .alias('TOTAMT_HP')
        ])
    
    disb652, disb652_sum = process_disb652(disbmthtest)
    disb750 = process_disb750(disbmthtest)
    
    # Step 7: Merge disbursements (same logic)
    disb310_final, disb652_final, noteconv = merge_disbursements(
        disb310, disb652, disb652_sum, disb750, lnnote_df, macro_vars, prefix='I'
    )
    
    # Step 8: Process repayments with Islamic-specific handling
    # IREPAY610 for TRANCODE 610
    irepay610 = disbmthtest.filter(
        pl.col('TRANCODE') == 610
    ).with_columns([
        (pl.col('TOTAMT') * -1).alias('TOTAMT')
    ]).sort(['ACCTNO', 'TOTAMT', 'EFFDATE'])
    
    irepay610 = irepay610.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'TOTAMT', 'EFFDATE']).alias('COUNT')
    ])
    
    # Remove overlapping with disb750
    irepay610 = irepay610.join(
        disb750.select(['ACCTNO', 'NOTENO', 'TOTAMT', 'EFFDATE']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT', 'EFFDATE'],
        how='anti'
    )
    
    # IREPAY664 for TRANCODE 664
    irepay664 = disbmthtest.filter(
        pl.col('TRANCODE') == 664
    ).with_columns([
        (pl.col('TOTAMT') * -1).alias('TOTAMT')
    ]).sort(['ACCTNO', 'TOTAMT', 'EFFDATE'])
    
    irepay664 = irepay664.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'TOTAMT', 'EFFDATE']).alias('COUNT')
    ])
    
    # Remove overlapping with disb750
    disb750 = disb750.join(
        irepay664.select(['ACCTNO', 'NOTENO', 'TOTAMT', 'EFFDATE']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT', 'EFFDATE'],
        how='anti'
    )
    
    # Process repayments (similar to conventional)
    repay610 = process_repay610(disbmthtest)
    repay652 = process_repay652(disbmthtest)
    repay800 = process_repay800(disbmthtest)
    
    # Remove overlapping records
    repay800 = repay800.join(
        repay652.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT'],
        how='anti'
    )
    
    repay800 = repay800.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    repay610 = repay610.join(
        repay800.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'],
        how='anti'
    )
    
    repay754 = process_repay754(disbmthtest, disb750)
    
    # Merge repay754 with repay610
    repay754 = repay754.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    repay610 = repay610.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    merged_repay = repay610.join(
        repay754.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']),
        on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'],
        how='left',
        indicator='_merge'
    )
    
    repay_final = merged_repay.filter(pl.col('_merge') == 'left_only').drop('_merge')
    repay754x = merged_repay.filter(pl.col('_merge') == 'both').drop('_merge')
    
    # Extract disb750 from repay754x
    disb750_extra = repay754x.filter(pl.col('TRANCODE') == 750)
    
    # Merge with irepay610
    irepay610 = irepay610.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    repay_final = repay_final.with_columns([
        pl.cum_count('ACCTNO').over(['ACCTNO', 'TOTAMT_CNT']).alias('COUNT')
    ])
    
    repay_merged = repay_final.join(
        irepay610.select(['ACCTNO', 'TOTAMT_CNT', 'COUNT']),
        on=['ACCTNO', 'TOTAMT_CNT', 'COUNT'],
        how='anti'
    ).with_columns([
        pl.col('TOTAMT').alias('REPAID')
    ])
    
    # Update NOTENO using noteconv
    repay_merged = repay_merged.join(
        noteconv.select(['ACCTNO', 'NOTENO', 'NOTENEW']),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    ).with_columns([
        pl.when(pl.col('NOTENEW').is_not_null())
        .then(pl.col('NOTENEW'))
        .otherwise(pl.col('NOTENO'))
        .alias('NOTENO')
    ])
    
    # Step 9: Create disbursed dataset
    disbursed = pl.concat([
        disb310_final.with_columns([(pl.col('TOTAMT') * -1).alias('DISBURSE')]),
        disb750_extra.with_columns([(pl.col('TOTAMT') * -1).alias('DISBURSE')])
    ]).filter(pl.col('REVERSED') != 'R')
    
    disbursed = disbursed.join(
        noteconv.select(['ACCTNO', 'NOTENO', 'NOTENEW']),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    ).with_columns([
        pl.when(pl.col('NOTENEW').is_not_null())
        .then(pl.col('NOTENEW'))
        .otherwise(pl.col('NOTENO'))
        .alias('NOTENO')
    ])
    
    # Step 10: Read Islamic loan files
    lnwof = pl.read_parquet(IBNM_DIR / f"LNWOF{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet")
    lnwod = pl.read_parquet(IBNM_DIR / f"LNWOD{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet")
    loan2 = pl.read_parquet(IBNM_DIR / f"LOAN{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet")
    loan_sasd = pl.read_parquet(ISASD_DIR / f"LOAN{macro_vars['REPTMON']}.parquet")
    
    loan_combined = pl.concat([
        lnnote_df.rename({'LOANTYPE': 'PRODUCT'}),
        lnwof, lnwod, loan2, loan_sasd
    ]).unique(subset=['ACCTNO', 'NOTENO'])
    
    # Step 11: Merge with disbursed and repay
    disbursed_sum = disbursed.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('DISBURSE').sum(),
        pl.col('HISTINT').fill_null(0).sum().alias('HISTINT'),
        pl.col('REBATE').fill_null(0).sum().alias('REBATE'),
        pl.col('REPAID10').fill_null(0).sum().alias('REPAID10')
    ])
    
    repay_sum = repay_merged.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('REPAID').sum()
    ])
    
    disburse_sum = disbursed_sum.join(repay_sum, on=['ACCTNO', 'NOTENO'], how='outer')
    
    loan_final = loan_combined.join(disburse_sum, on=['ACCTNO', 'NOTENO'], how='inner')
    
    # Step 12: Create DISPAY for purpose code analysis
    dispay = loan_final.filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226, 698, 699, 983]) |
        (pl.col('PRODCD') == '54120')
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'BALANCE', 'CENSUS', 'DNBFISME',
        'PRODCD', 'CUSTCD', 'AMTIND', 'SECTORCD', 'DISBURSE', 'REPAID', 'BRANCH',
        'HISTINT', 'REBATE', 'CCY', 'FORATE', 'REPAID10'
    ])
    
    # Step 13: Read ALM files (Islamic)
    alm1 = pl.read_parquet(IBNM_DIR / f"LOAN{macro_vars['REPTMON2']}{macro_vars['NOWK']}.parquet").filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226]) |
        (pl.col('PRODCD') == '54120')
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD',
        'CUSTCD', 'AMTIND', 'SECTORCD', 'BRANCH', 'CCY', 'FORATE', 'DNBFISME'
    ]).rename({'BALANCE': 'LASTBAL', 'NOTETERM': 'LASTNOTE'})
    
    alm = pl.read_parquet(IBNM_DIR / f"LOAN{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet").filter(
        pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344', '346']) |
        pl.col('PRODUCT').is_in([225, 226]) |
        (pl.col('PRODCD') == '54120')
    ).select([
        'ACCTNO', 'NOTENO', 'FISSPURP', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE',
        'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'SECTORCD', 'BRANCH',
        'CCY', 'FORATE', 'DNBFISME'
    ])
    
    # Step 14: Process rollovers (similar logic)
    lnhist = pl.read_parquet(DISPAY_DIR / "LNHIST.parquet").filter(
        (pl.col('TRANCODE') == 400) &
        (pl.col('RATECHG') != 0)
    ).unique(subset=['ACCTNO', 'NOTENO'])
    
    alm = alm.join(lnnote_df.select(['ACCTNO', 'NOTENO', 'DELQCD']), on=['ACCTNO', 'NOTENO'], how='left')
    alm = alm.join(lnhist, on=['ACCTNO', 'NOTENO'], how='left')
    
    alm = alm.with_columns([
        pl.when(
            pl.col('DELQCD').is_null() & 
            (pl.col('ISSDTE') < macro_vars['SDATE_SAS'])
        )
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias('RATECHG')
    ])
    
    alm = alm.with_columns([
        pl.when(
            (pl.col('PRODCD').is_in(['34190', '34690'])) &
            (pl.col('RATECHG') == 'Y')
        )
        .then(pl.col('CURBAL'))
        .otherwise(0)
        .alias('ROLLOVER')
    ])
    
    # Step 15: Process OD rollovers (Islamic)
    over_dft = pl.read_parquet(ILIMIT_DIR / "OVERDFT.parquet").filter(
        (pl.col('APPRLIMT') > 1) &
        (pl.col('LMTAMT') > 1)
    ).select(['ACCTNO', 'LMTID', 'REVIEWDT', 'LMTAMT'])
    
    pover_dft = pl.read_parquet(ILIMIT_DIR / "POVERDFT.parquet").filter(
        (pl.col('APPRLIMT') > 1) &
        ~pl.col('REVIEWDT').is_in([0, None])
    ).select(['ACCTNO', 'LMTID', 'REVIEWDT'])
    
    limit = over_dft.join(
        pover_dft.rename({'REVIEWDT': 'PREVIEWDT'}),
        on=['ACCTNO', 'LMTID'],
        how='inner'
    )
    
    # Convert dates
    limit = limit.with_columns([
        pl.when(pl.col('REVIEWDT') > 0)
        .then(
            pl.col('REVIEWDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format='%m%d%Y')
        )
        .otherwise(None).alias('REVIEW'),
        pl.when(pl.col('PREVIEWDT') > 0)
        .then(
            pl.col('PREVIEWDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format='%m%d%Y')
        )
        .otherwise(None).alias('PREVIEW')
    ])
    
    limit = limit.with_columns([
        pl.col('REVIEW').alias('REVIEWDT'),
        pl.col('PREVIEW').alias('PREVIEWDT')
    ]).filter(pl.col('REVIEWDT') > pl.col('PREVIEWDT'))
    
    limit_sum = limit.group_by('ACCTNO').agg(pl.col('LMTAMT').sum().alias('ROLLOVER'))
    
    # Step 16: Process current account data (Islamic)
    ovdft = pl.read_parquet(IDEPOSIT_DIR / "CURRENT.parquet").rename({'SECTOR': 'SECTORC'}).filter(
        ~pl.col('OPENIND').is_in(['B', 'C', 'P'])
    )
    
    # Purpose code mapping (similar to conventional)
    ovdft = ovdft.with_columns([
        pl.col('SECTORC').cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8).str.zfill(4).alias('SECTOR'),
        pl.when(pl.col('PRODUCT') == 104).then(pl.lit('02'))
        .when(pl.col('PRODUCT') == 105).then(pl.lit('81'))
        .otherwise(pl.col('CUSTCODE').cast(pl.Utf8))
        .alias('CUSTCD'),
        pl.col('CURCODE').alias('CCY')
    ])
    
    # Join with limit
    limitx = ovdft.join(limit_sum, on='ACCTNO', how='inner')
    
    # Step 17: Final ALM merge
    alm = alm.join(dispay, on=['ACCTNO', 'NOTENO'], how='left')
    alm = alm.join(limitx, on='ACCTNO', how='left')
    
    # Step 18: Apply forex rates
    forate = pl.read_parquet(FORATE_DIR / "FORATEBKP.parquet").sort(
        ['CURCODE', 'REPTDATE'], descending=True
    ).unique(subset=['CURCODE']).select(['CURCODE', 'SPOTRATE'])
    
    forate_dict = {row['CURCODE']: row['SPOTRATE'] for row in forate.rows()}
    
    # Step 19: Create final IDISPAYMTH
    dispaymth = alm.filter(
        (pl.col('DISBURSE') > 0) | (pl.col('REPAID') > 0) | (pl.col('ROLLOVER') > 0)
    ).with_columns([
        pl.col('CCY').apply(lambda c: forate_dict.get(c, 1.0)).alias('FORATE')
    ])
    
    # Apply forex for Islamic products (851-899)
    dispaymth = dispaymth.with_columns([
        pl.when(
            (pl.col('PRODUCT').is_between(851, 900)) &
            (pl.col('CCY') != 'MYR')
        )
        .then(pl.col('DISBURSE') * pl.col('FORATE'))
        .otherwise(pl.col('DISBURSE'))
        .alias('DISBURSE'),
        pl.when(
            (pl.col('PRODUCT').is_between(851, 900)) &
            (pl.col('CCY') != 'MYR')
        )
        .then(pl.col('REPAID') * pl.col('FORATE'))
        .otherwise(pl.col('REPAID'))
        .alias('REPAID')
    ])
    
    # Set negative values to zero
    dispaymth = dispaymth.with_columns([
        pl.when(pl.col('REPAID') <= 0).then(0).otherwise(pl.col('REPAID')).alias('REPAID'),
        pl.when(pl.col('REPAID10') <= 0).then(0).otherwise(pl.col('REPAID10')).alias('REPAID10')
    ])
    
    # Set DNBFISME
    cust_exclude = ['04', '05', '06', '30', '31', '32', '33', '34', '35', '37', '38', '39', '40', '45']
    dispaymth = dispaymth.with_columns([
        pl.when(~pl.col('CUSTCD').cast(pl.Utf8).is_in(cust_exclude))
        .then(pl.lit('0'))
        .otherwise(pl.col('DNBFISME'))
        .alias('DNBFISME')
    ])
    
    # Save output
    output_file = IDISPAY_DIR / f"IDISPAYMTH{macro_vars['REPTMON']}.parquet"
    dispaymth.write_parquet(output_file)
    print(f"  Saved: {output_file}")
    
    return dispaymth

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    print(f"Starting EIMDISRP translation at {datetime.now()}")
    
    # Setup date variables
    macro_vars = setup_date_variables()
    print(f"Report Date: {macro_vars['RDATE']}")
    print(f"SDATE: {macro_vars['SDATE']}, TDATE: {macro_vars['TDATE']}")
    print(f"REPTMON: {macro_vars['REPTMON']}, REPTMON2: {macro_vars['REPTMON2']}")
    
    try:
        # Process conventional loans
        conv_result = process_conventional(macro_vars)
        
        # Process Islamic loans
        islamic_result = process_islamic(macro_vars)
        
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        print(f"\nOutput files saved in:")
        print(f"  - Conventional: {DISPAY_DIR}")
        print(f"  - Islamic: {IDISPAY_DIR}")
        
        # List output files
        print("\nGenerated files:")
        for dir_path in [DISPAY_DIR, IDISPAY_DIR]:
            if dir_path.exists():
                files = list(dir_path.glob("*.parquet"))
                for f in sorted(files):
                    print(f"  {dir_path.name}/{f.name}")
        
        print(f"\nProcessing completed successfully at {datetime.now()}")
        
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
