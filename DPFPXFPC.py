import pyreadstat
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# CONFIGURATION - INPUT/OUTPUT PATHS
# ============================================

BNMK_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"
BNM_INPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE"

OUTPUT_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWKAPE"
REPORTS_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/REPORTS"
SFTP_UPLOAD_PATH = "SFTP_UPLOAD"

SAS_EXTENSION = ".sas7bdat"
PARQUET_EXTENSION = ".PARQUET"
TEXT_EXTENSION = ".TXT"

USE_CURRENT_DATE = False
CUSTOM_DATE = datetime(2026, 7, 16)
DAYS_OFFSET = 1

# ============================================
# EL_ITEM LOOKUP TABLE (from SAS PBBELQ)
# ============================================

EL_ITEM = pd.DataFrame({
    'BNMCODE': [
        '4211000000000Y', '4212000000000Y', '4213000000000Y', '4213100000000Y',
        '4213200000000Y', '4213300000000Y', '4215000000000Y', '4216000000000Y',
        '4217071000000Y', '4218000000000Y', '4219000000000Y', '4219100000000Y',
        '4219900000000Y', '4310000000000Y', '4311002000000Y', '4311003000000Y',
        '4311081000000Y', '4312002000000Y', '4312003000000Y', '4313000000000Y',
        '4313002000000Y', '4313003000000Y', '4314001000000Y', '4314002000000Y',
        '4314003000000Y', '4314011000000Y', '4314012000000Y', '4314013000000Y',
        '4314017000000Y', '4314020000000Y', '4314081100000Y', '4410000000000Y',
        '4911080000000Y', '4911095000000Y', '4912080000000Y', '4929980000000Y',
        '4929995000000Y', '4929996000000Y', '4411100000000Y', '4411200000000Y',
        '4411300000000Y', '4414000000000Y',
        # B-RMEA items
        '3212002000000Y', '3212003000000Y', '3213002000000Y', '3213011000000Y',
        '3213012000000Y', '3213013000000Y', '3213102000000Y', '3213103000000Y',
        '3213111000000Y', '3213112000000Y', '3213113000000Y', '3213202000000Y',
        '3213203000000Y', '3213211000000Y', '3213212000000Y', '3213213000000Y',
        '3219910000000Y', '3250001000000Y', '3250002000000Y', '3250011000000Y',
        '3250012000000Y', '3250013000000Y', '3311002000000Y', '3311003000000Y',
        '3312002000000Y', '3312003000000Y', '3313000000000Y', '3314001000000Y',
        '3314002000000Y', '3314003000000Y', '3314011000000Y', '3314012000000Y',
        '3314013000000Y', '3314017000000Y', '3410002000000Y', '3410003000000Y',
        '3410011000000Y', '3410012000000Y', '3410013000000Y', '3410017000000Y',
        '3703000000000Y', '3803000000000Y', '4015000000000Y', '3314013110000Y'
    ],
    'FMTNAME': [
        'A-RMEL'] * 42 + ['B-RMEA'] * 44,
    'IDX': ['A'] * 42 + ['B'] * 44,
    'SIGN': ['+'] * 37 + ['-'] * 5 + ['+'] * 44,
    'DESC': [
        'RM DEMAND DEPOSITS ACCEPTED',
        'RM SAVINGS DEPOSITS ACCEPTED',
        'RM FIXED DEPOSITS ACCEPTED',
        'RM SPECIAL INVESTMENT DEPOSIT ACCEPTED',
        'RM GENERAL INVESTMENT DEPOSIT ACCEPTED',
        'RM COMMODITY MURABAHAH',
        'RM NID ISSUED',
        'RM REPURCHASE AGREEMENTS',
        'RM SPECIAL DEPOSITS',
        'RM HOUSING DEVELOPMENT ACCOUNTS',
        'RM SHORT TERM DEPOSIT ACCEPTED',
        'RM INVESTMENT LINKED TO DERIVATIVES',
        'RM OTHER DEPOSITS ACCEPTED',
        'RM AMOUNT DUE TO DESIGNATED FI',
        'RM VOSTRO ACCOUNTS OF CB',
        'RM VOSTRO ACCOUNTS OF IB',
        'RM VOSTRO ACCOUNTS OF FBI',
        'RM OVERDRAWN NOSTRO ACCOUNTS WITH CB',
        'RM OVERDRAWN NOSTRO ACCOUNTS WITH IB',
        'RM DEFICIT IN SPICK',
        'RM AMOUNT BORROWING FROM SPICK POOL CB',
        'RM AMOUNT BORROWING FROM SPICK POOL IB',
        'RM INTERBANK BORROWINGS FROM BNM',
        'RM INTERBANK BORROWINGS FROM CB',
        'RM INTERBANK BORROWINGS FROM IB',
        'RM INTERBANK BORROWINGS FROM FC',
        'RM INTERBANK BORROWINGS FROM MB',
        'RM INTERBANK BORROWINGS FROM DH',
        'O/W RM IBB FROM CAGAMAS',
        'RM INTERBANK BORROWINGS FROM DNBFI',
        'RM INTERBANK BORROWINGS FROM FBI <= 1 YR',
        'RM MISC BORROWINGS',
        'RM INTEREST PAYABLE TO NON-RESIDENTS',
        'RM INTEREST PAYABLE TO NON-RES - DCI/CRA',
        'RM BILLS PAYABLE TO NON-RESIDENTS',
        'OTHER RM MISC LIAB NIE DUE TO NON-RES',
        'RM GOLD INVESTMENT FROM NON-RESIDENTS',
        'OTHR RM MISC LIAB NIE DUE TO NON-RES-DCI',
        'RM SUBORDINATED DEBT CAPITAL',
        'RM EXEMPT SUBORDINATED DEBT CAPITAL',
        'RM SUBORDIN DEBT CAPITAL W APPR FR BNM',
        'RM RESOURCE OBLIQ ON LN SOLD TO CAGAMAS',
        # B-RMEA descriptions
        'RM BALANCES IN CURRENT ACCOUNTS WITH CB',
        'RM BALANCES IN CURRENT ACCOUNTS WITH IB',
        'RM FIXED DEPOSITS PLACED WITH CB',
        'RM FIXED DEPOSITS PLACED WITH FC',
        'RM FIXED DEPOSITS PLACED WITH MB',
        'RM FIXED DEPOSITS PLACED WITH DH',
        'RM SPECIAL INV DEP PLACED WITH CB',
        'RM SPECIAL INV DEP PLACED WITH IB',
        'RM SPECIAL INV DEP PLACED WITH FC',
        'RM SPECIAL INV DEP PLACED WITH MB',
        'RM SPECIAL INV DEP PLACED WITH DH',
        'RM GEN INVESTMENT DEP PLACED WITH CB',
        'RM GEN INVESTMENT DEP PLACED WITH IB',
        'RM GEN INVESTMENT DEP PLACED WITH FC',
        'RM GEN INVESTMENT DEP PLACED WITH MB',
        'RM GEN INVESTMENT DEP PLACED WITH DH',
        'RM OTHER DEPOSITS PLACED WITH DBI',
        'RM REVERSE REPOS WITH BNM',
        'RM REVERSE REPOS WITH CB',
        'RM REVERSE REPOS WITH FC',
        'RM REVERSE REPOS WITH MB',
        'RM REVERSE REPOS WITH DH',
        'RM OVERDRAWN VOSTRO ACCOUNTS OF CB',
        'RM OVERDRAWN VOSTRO ACCOUNTS OF IB',
        'RM NOSTRO ACCOUNT BALANCES WITH CB',
        'RM NOSTRO ACCOUNT BALANCES WITH IB',
        'RM SURPLUS IN SPICK',
        'RM INTERBANK PLACEMENTS WITH BNM',
        'RM INTERBANK PLACEMENTS WITH CB',
        'RM INTERBANK PLACEMENTS WITH IB',
        'RM INTERBANK PLACEMENTS WITH FC',
        'RM INTERBANK PLACEMENTS WITH MB',
        'RM INTERBANK PLACEMENTS WITH DH',
        'RM INTERBANK PLACEMENTS WITH CAGAMAS',
        'RM LOANS TO CB',
        'RM LOANS TO IB',
        'RM LOANS TO FC',
        'RM LOANS TO MB',
        'RM LOANS TO DH',
        'RM LOANS TO CAGAMAS',
        'RM NIDS HELD',
        'NIDS SOLD UNDER REPO',
        'ELIGIBLE CAGAMAS TIER-2 BONDS (DAY I)',
        'RM INTERBKS PLACEMENTS WITH DH OVRNIGHT'
    ]
})

# ============================================
# FUNCTIONS
# ============================================

def read_sas7bdat(file_path):
    """Read SAS7BDAT file and return as Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

def format_number(num):
    """Format number with commas and 2 decimal places"""
    if num is None or pd.isna(num):
        return "0.00"
    try:
        return f"{float(num):,.2f}"
    except:
        return "0.00"

def apply_utsty_filter(df):
    """Mirrors SAS filter logic"""
    if df.height == 0:
        return df
    return df.filter(
        ~(
            (pl.col("UTSTY").is_in(['CB1', 'CF1', 'CNT', 'SAC', 'SMC', 'ISB'])) &
            (~pl.col("UTREF").is_in(['DLG', 'IDLG']))
        )
    )

def write_formatted_report(df, filename, title, report_date, is_alternative=False):
    """
    Write formatted report matching SAS PBBELQ output
    """
    if df.empty:
        print(f"Warning: No data to write to {filename}")
        return
    
    with open(filename, 'w') as f:
        # Write header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'FMTNAME':<10} {'BNMCODE':<20} {'DESC':<50} {'SIGN':<6} {'AMOUNT':>20} {'TOTAL':>20}\n")
        f.write("-" * 128 + "\n")
        
        # Process each FMTNAME group
        total_grand = 0
        
        for fmtname in df['FMTNAME'].unique():
            group = df[df['FMTNAME'] == fmtname]
            group_total = 0
            
            for _, row in group.iterrows():
                bnmcode = str(row['BNMCODE'])[:20]
                desc = str(row['DESC'])[:50]
                sign = str(row['SIGN'])[:6]
                amount = format_number(row.get('AMOUNT', 0))
                total = format_number(row.get('TOTAL', row.get('AMOUNT', 0)))
                
                # Special handling for O/W RM IBB FROM CAGAMAS
                if 'O/W' in desc:
                    desc = f"{desc} {format_number(row.get('AMOUNT', 0))}"
                    amount = "0.00"
                    total = "0.00"
                
                f.write(f"{fmtname:<10} {bnmcode:<20} {desc:<50} {sign:<6} {amount:>20} {total:>20}\n")
                group_total += float(row.get('AMOUNT', 0) or 0)
            
            # Write group total
            f.write("-" * 128 + "\n")
            f.write(f"{'TOTAL FOR':<10} {fmtname:<20} {'':<50} {'':<6} {format_number(group_total):>20} {format_number(group_total):>20}\n")
            f.write(" " * 73 + "-" * 55 + "\n")
            f.write("\n")
            
            total_grand += group_total
        
        # Write grand total
        f.write("=" * 128 + "\n")
        f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<50} {'':<6} {format_number(total_grand):>20} {format_number(total_grand):>20}\n")
        f.write("=" * 128 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

REPTDATE_BNMK = REPTDATE_LOAN
MM = REPTDATE_BNMK.month
DAY = REPTDATE_BNMK.day

if 1 <= DAY <= 8:
    WK = '4'
elif 9 <= DAY <= 15:
    WK = '1'
elif 16 <= DAY <= 22:
    WK = '2'
else:
    WK = '3'

if WK == '4':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
    MM = MM1
    if MM == 12:
        SXDATE = REPTDATE_BNMK.replace(month=1, day=1) - timedelta(days=1)
    else:
        SXDATE = REPTDATE_BNMK.replace(day=1) - timedelta(days=1)
        SXDATE = SXDATE.replace(month=MM) if SXDATE.month != MM else SXDATE
else:
    SXDATE = REPTDATE_BNMK

REPTMON = f"{MM:02d}"
RYEAR = SXDATE.strftime("%Y")
RDATE = SXDATE.strftime("%d/%m/%y")
MTHNAM = SXDATE.strftime("%B")

print(f"Report Period: Week {WK}, Month {REPTMON}")

# ============================================
# LOAD AND PROCESS DATA
# ============================================

rep2_file = f"{BNMK_INPUT_PATH}/rep2{REPTMON}{WK}{SAS_EXTENSION}"
rep4_file = f"{BNMK_INPUT_PATH}/rep4{REPTMON}{WK}{SAS_EXTENSION}"

print(f"Reading: {rep2_file}")
REP2_RAW = read_sas7bdat(rep2_file)
print(f"Reading: {rep4_file}")
REP4_RAW = read_sas7bdat(rep4_file)

REP2_FILTERED = apply_utsty_filter(REP2_RAW)
REP4_FILTERED = apply_utsty_filter(REP4_RAW)

print(f"REP2 raw: {REP2_RAW.height} | filtered: {REP2_FILTERED.height}")
print(f"REP4 raw: {REP4_RAW.height} | filtered: {REP4_FILTERED.height}")

# Combine REP2 + REP4
frames = [d for d in (REP2_FILTERED, REP4_FILTERED) if d.height > 0]
REP2_COMBINED = pl.concat(frames) if frames else pl.DataFrame()
print(f"Combined REP2+REP4: {REP2_COMBINED.height} records")

# ============================================
# TRANSFORM DATA (SAS logic)
# ============================================

if REP2_COMBINED.height > 0:
    amount_col = "NETAMT" if "NETAMT" in REP2_COMBINED.columns else "AMOUNT"
    
    REP2_TRANSFORMED = REP2_COMBINED.with_columns([
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.lit('REV'))
          .otherwise(pl.col("UTSTY"))
          .alias("UTSTY"),
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.lit('REPO '))
          .otherwise(pl.col("UTREF"))
          .alias("UTREF"),
        pl.when(pl.col("BNMCODE") == '3250000000000Y')
          .then(pl.col(amount_col))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT"),
        pl.when(pl.col("BNMCODE") == '3752000000000Y')
          .then(pl.lit('3552000000000Y'))
          .otherwise(pl.col("BNMCODE"))
          .alias("BNMCODE"),
    ]).with_columns(
        (pl.col("BNMCODE") + '-' + pl.col("UTSTY") + ' ' + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
    )

    # Convert to pandas for EL_ITEM merge
    df_pd = REP2_TRANSFORMED.to_pandas()
    
    # Group by BNMCODE and ELDAY to get totals
    df_summary = df_pd.groupby(['BNMCODE', 'ELDAY'], as_index=False)['AMOUNT'].sum()
    
    # Merge with EL_ITEM to get descriptions
    df_final = df_summary.merge(EL_ITEM, on='BNMCODE', how='inner')
    
    # Apply sign handling
    df_final['AMOUNX'] = df_final['AMOUNT']
    mask_neg = df_final['SIGN'] == '-'
    df_final.loc[mask_neg, 'AMOUNX'] = -df_final.loc[mask_neg, 'AMOUNT']
    df_final['TOTAL'] = df_final['AMOUNX']
    
    # Special handling for O/W RM IBB FROM CAGAMAS
    mask_cagamas = df_final['BNMCODE'] == '4314017000000Y'
    if mask_cagamas.any():
        df_final.loc[mask_cagamas, 'DESC'] = df_final.loc[mask_cagamas, 'DESC'] + ' ' + df_final.loc[mask_cagamas, 'AMOUNT'].apply(lambda x: f'{x:,.2f}')
        df_final.loc[mask_cagamas, 'AMOUNT'] = 0.00
        df_final.loc[mask_cagamas, 'AMOUNX'] = 0.00
        df_final.loc[mask_cagamas, 'TOTAL'] = 0.00
    
    # Sort by FMTNAME and BNMCODE
    df_final = df_final.sort_values(['FMTNAME', 'BNMCODE']).reset_index(drop=True)
    
    # ============================================
    # CREATE OUTPUT DIRECTORY
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print("\nWriting output files...")

    # ============================================
    # WRITE FORMATTED REPORT
    # ============================================
    
    report_date_str = SXDATE.strftime("%d/%m/%y")
    filename_base = f"ELIGIBLE_LIABILITIES_REPORT_{REPTMON}{WK}_{RYEAR}"
    
    # Write the main formatted report
    write_formatted_report(
        df_final,
        f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}",
        "DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR :  DAYA",
        report_date_str
    )
    
    # Also save as Parquet for data processing
    pl.from_pandas(df_final).write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {SXDATE.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, {MTHNAM} {RYEAR}")
    print(f"Records processed: {len(df_final)}")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")
