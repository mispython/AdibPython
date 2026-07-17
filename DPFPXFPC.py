EIBWKAPE code:

import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

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

def write_formatted_report(df, filename, title, report_date):
    """
    Write a single formatted text file matching SAS output format
    """
    if df.height == 0:
        print(f"Warning: No data to write to {filename}")
        return
    
    # Convert to pandas
    pdf = df.to_pandas()
    
    # Group by ELDAY (which appears to be the primary grouping in your data)
    if 'ELDAY' in pdf.columns:
        groups = pdf.groupby('ELDAY')
    else:
        pdf['ELDAY'] = 'TOTAL'
        groups = pdf.groupby('ELDAY')
    
    with open(filename, 'w') as f:
        # Write main header
        f.write(f"{'PUBLIC BANK BERHAD':<100}\n")
        f.write(f"{title:<100}\n")
        f.write(f"REPORT DATE :  {report_date}\n")
        f.write("\n")
        
        # Column headers
        f.write(f"{'ELDAY':<10} {'BNMCODE':<20} {'UTSTY':<10} {'UTREF':<10} {'AMOUNT':>20} {'BNMCODG':<20}\n")
        f.write("-" * 100 + "\n")
        
        # Process each group
        grand_total = 0
        
        for elday, group in groups:
            group_total = 0
            
            # Write each row in the group
            for _, row in group.iterrows():
                bnmcode = str(row.get('BNMCODE', ''))[:20]
                utsty = str(row.get('UTSTY', ''))[:10]
                utref = str(row.get('UTREF', ''))[:10]
                amount = format_number(row.get('AMOUNT', 0))
                bnmcodg = str(row.get('BNMCODG', ''))[:20]
                
                f.write(f"{elday:<10} {bnmcode:<20} {utsty:<10} {utref:<10} {amount:>20} {bnmcodg:<20}\n")
                group_total += float(row.get('AMOUNT', 0) or 0)
            
            # Write group separator and total
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL FOR':<10} {elday:<20} {'':<10} {'':<10} {format_number(group_total):>20} {'':<20}\n")
            f.write("\n")
            
            grand_total += group_total
        
        # Write grand total
        f.write("=" * 100 + "\n")
        f.write(f"{'GRAND TOTAL':<10} {'':<20} {'':<10} {'':<10} {format_number(grand_total):>20} {'':<20}\n")
        f.write("=" * 100 + "\n")

# ============================================
# DATE PROCESSING
# ============================================

if USE_CURRENT_DATE:
    REPTDATE_LOAN = datetime.now() - timedelta(days=DAYS_OFFSET)
else:
    REPTDATE_LOAN = CUSTOM_DATE

print(f"Reporting Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")

# Process dates
SDESC = "PUBLIC BANK BERHAD"
RDATE = REPTDATE_LOAN.strftime("%d/%m/%y")
RYEAR = REPTDATE_LOAN.strftime("%Y")
MTHNAM = REPTDATE_LOAN.strftime("%B")
SDESC_FORMATTED = SDESC.ljust(26)[:26]

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

NOWK = WK
REPTMON = f"{MM:02d}"
RPYEAR = SXDATE.strftime("%Y")
REPTYEAR = SXDATE.strftime("%Y")

print(f"Report Period: Week {WK}, Month {REPTMON}")
print(f"Reading files: rep2{REPTMON}{WK}, rep4{REPTMON}{WK}, elw{REPTMON}{WK}")

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
print(f"Combined REP2+REP4 (filtered): {REP2_COMBINED.height} records")

# ============================================
# TRANSFORM DATA
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

    REP2_SORTED = REP2_TRANSFORMED.sort("BNMCODG")
    print(f"Transformed and sorted: {REP2_SORTED.height} records")

    # ============================================
    # CREATE OUTPUT DIRECTORY
    # ============================================
    
    Path(REPORTS_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    Path(SFTP_UPLOAD_PATH).mkdir(exist_ok=True)

    print(f"\nOutput directory: {REPORTS_OUTPUT_PATH}")
    print("\nWriting output files...")

    # ============================================
    # WRITE FORMATTED TEXT FILE (SINGLE OUTPUT)
    # ============================================
    
    report_date_str = REPTDATE_LOAN.strftime("%d/%m/%y")
    filename_base = f"DAILY_KAPITI_STOCK_REPORT_{REPTMON}{WK}_{RYEAR}"
    
    # Write the main formatted report
    write_formatted_report(
        REP2_SORTED,
        f"{REPORTS_OUTPUT_PATH}/{filename_base}{TEXT_EXTENSION}",
        "DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS",
        report_date_str
    )
    
    # Also save as Parquet for data processing
    REP2_SORTED.write_parquet(f"{REPORTS_OUTPUT_PATH}/{filename_base}{PARQUET_EXTENSION}")

    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Report Date: {REPTDATE_LOAN.strftime('%Y-%m-%d')}")
    print(f"Report Period: Week {WK}, Month {REPTMON}")
    print(f"Records processed: {REP2_SORTED.height}")
    print(f"Output location: {Path(REPORTS_OUTPUT_PATH).absolute()}")
    print("="*50)

else:
    print("Error: No data available to process")




PUBLIC BANK BERHAD                                                                                  
DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS                                                             
REPORT DATE :  16/07/26

ELDAY      BNMCODE              UTSTY      UTREF                    AMOUNT BNMCODG             
----------------------------------------------------------------------------------------------------
DAYA       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYA       3721000000000Y       MGS        AFSLIQ         6,136,116,580.40 3721000000000Y-MGS A
DAYA       3721000000000Y       MGS        DLG            1,157,243,500.00 3721000000000Y-MGS D
DAYA       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYA       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYA       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYA       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYA       3723000000000Y       MGI        AFSLIQ         5,285,092,981.40 3723000000000Y-MGI A
DAYA       3723000000000Y       MGI        DLG              927,171,200.00 3723000000000Y-MGI D
DAYA       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYA                                          36,893,829,812.72                     

DAYB       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYB       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYB       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYB       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYB       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYB       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYB       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYB       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYB       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYB       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYB                                          37,017,498,212.72                     

DAYC       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYC       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYC       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYC       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYC       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYC       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYC       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYC       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYC       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYC       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYC                                          37,017,498,212.72                     

DAYD       3721000000000Y       MGS        AFS            7,694,996,662.00 3721000000000Y-MGS A
DAYD       3721000000000Y       MGS        AFSLIQ         6,329,126,580.40 3721000000000Y-MGS A
DAYD       3721000000000Y       MGS        DLG            1,084,630,400.00 3721000000000Y-MGS D
DAYD       3721000000000Y       MGS        INV            4,955,404,216.60 3721000000000Y-MGS I
DAYD       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYD       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYD       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYD       3723000000000Y       MGI        AFSLIQ         5,308,719,481.40 3723000000000Y-MGI A
DAYD       3723000000000Y       MGI        DLG              906,816,200.00 3723000000000Y-MGI D
DAYD       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYD                                          37,017,498,212.72                     

DAYE       3721000000000Y       MGS        AFS            7,851,922,162.00 3721000000000Y-MGS A
DAYE       3721000000000Y       MGS        AFSLIQ         6,309,132,980.40 3721000000000Y-MGS A
DAYE       3721000000000Y       MGS        DLG            1,033,920,600.00 3721000000000Y-MGS D
DAYE       3721000000000Y       MGS        INV            4,955,388,116.60 3721000000000Y-MGS I
DAYE       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYE       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYE       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYE       3723000000000Y       MGI        AFSLIQ         5,048,395,481.40 3723000000000Y-MGI A
DAYE       3723000000000Y       MGI        DLG              806,327,000.00 3723000000000Y-MGI D
DAYE       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYE                                          36,742,891,012.72                     

DAYF       3721000000000Y       MGS        AFS            7,500,978,162.00 3721000000000Y-MGS A
DAYF       3721000000000Y       MGS        AFSLIQ         6,152,916,980.40 3721000000000Y-MGS A
DAYF       3721000000000Y       MGS        DLG            1,033,920,600.00 3721000000000Y-MGS D
DAYF       3721000000000Y       MGS        INV            4,955,388,116.60 3721000000000Y-MGS I
DAYF       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYF       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYF       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYF       3723000000000Y       MGI        AFSLIQ         5,048,395,481.40 3723000000000Y-MGI A
DAYF       3723000000000Y       MGI        DLG              806,327,000.00 3723000000000Y-MGI D
DAYF       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYF                                          36,235,731,012.72                     

DAYI       3721000000000Y       MGS        AFS            7,500,978,162.00 3721000000000Y-MGS A
DAYI       3721000000000Y       MGS        AFSLIQ         5,961,140,480.40 3721000000000Y-MGS A
DAYI       3721000000000Y       MGS        DLG              733,603,600.00 3721000000000Y-MGS D
DAYI       3721000000000Y       MGS        INV            3,324,518,116.60 3721000000000Y-MGS I
DAYI       3722000000000Y       ITB        AFS              231,830,158.62 3722000000000Y-ITB A
DAYI       3722000000000Y       MTB        DLG              196,991,780.80 3722000000000Y-MTB D
DAYI       3723000000000Y       MGI        AFS            6,686,568,608.90 3723000000000Y-MGI A
DAYI       3723000000000Y       MGI        AFSLIQ         5,095,963,497.60 3723000000000Y-MGI A
DAYI       3723000000000Y       MGI        DLG              872,918,230.40 3723000000000Y-MGI D
DAYI       3723000000000Y       MGI        INV            3,622,414,124.00 3723000000000Y-MGI I
----------------------------------------------------------------------------------------------------
TOTAL FOR  DAYI                                          34,226,926,759.32                     

====================================================================================================
GRAND TOTAL                                              255,151,873,235.63                     
====================================================================================================




actual production output:

PUBLIC BANK BERHAD                                                                                                                   
DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR :  DAYA                                                                                  
REPORT DATE :  08/07/26                                                                                                              
                                                                                                                                     
  FMTNAME  BNMCODE         DESC                                      SIGN                  AMOUNT                   TOTAL            
  -----------------------------------------------------------------------------------------------------------------------            
  A-RMEL   4211000000000Y  RM DEMAND DEPOSITS ACCEPTED               +          58,355,074,524.53       58,355,074,524.53            
           4212000000000Y  RM SAVINGS DEPOSITS ACCEPTED              +          34,788,322,240.58       34,788,322,240.58            
           4213000000000Y  RM FIXED DEPOSITS ACCEPTED                +         150,458,756,066.17      150,458,756,066.17            
           4213100000000Y  RM SPECIAL INVESTMENT DEPOSIT ACCEPTED    +                       0.00                    0.00            
           4213200000000Y  RM GENERAL INVESTMENT DEPOSIT ACCEPTED    +                       0.00                    0.00            
           4213300000000Y  RM COMMODITY MURABAHAH                    +                       0.00                    0.00            
           4215000000000Y  RM NID ISSUED                             +             900,000,000.00          900,000,000.00            
           4216000000000Y  RM REPURCHASE AGREEMENTS                  +          11,622,078,998.66       11,622,078,998.66            
           4217071000000Y  RM SPECIAL DEPOSITS                       +                       0.00                    0.00            
           4218000000000Y  RM HOUSING DEVELOPMENT ACCOUNTS           +           1,712,725,630.10        1,712,725,630.10            
           4219000000000Y  RM SHORT TERM DEPOSIT ACCEPTED            +          61,770,782,382.26       61,770,782,382.26            
           4219100000000Y  RM INVESTMENT LINKED TO DERIVATIVES       +             386,914,369.00          386,914,369.00            
           4219900000000Y  RM OTHER DEPOSITS ACCEPTED                +              37,646,848.06           37,646,848.06            
           4310000000000Y  RM AMOUNT DUE TO DESIGNATED FI            +                       0.00                    0.00            
           4311002000000Y  RM VOSTRO ACCOUNTS OF CB                  +                 486,854.46              486,854.46            
           4311003000000Y  RM VOSTRO ACCOUNTS OF IB                  +              54,444,827.40           54,444,827.40            
           4311081000000Y  RM VOSTRO ACCOUNTS OF FBI                 +             100,012,977.00          100,012,977.00            
           4312002000000Y  RM OVERDRAWN NOSTRO ACCOUNTS WITH CB      +                       0.00                    0.00            
           4312003000000Y  RM OVERDRAWN NOSTRO ACCOUNTS WITH IB      +                       0.00                    0.00            
           4313000000000Y  RM DEFICIT IN SPICK                       +                       0.00                    0.00            
           4313002000000Y  RM AMOUNT BORROWING FROM SPICK POOL CB    +                       0.00                    0.00            
           4313003000000Y  RM AMOUNT BORROWING FROM SPICK POOL IB    +                       0.00                    0.00            
           4314001000000Y  RM INTERBANK BORROWINGS FROM BNM          +             113,171,358.83          113,171,358.83            
           4314002000000Y  RM INTERBANK BORROWINGS FROM CB           +           2,255,000,000.00        2,255,000,000.00            
           4314003000000Y  RM INTERBANK BORROWINGS FROM IB           +                       0.00                    0.00            
           4314011000000Y  RM INTERBANK BORROWINGS FROM FC           +                       0.00                    0.00            
           4314012000000Y  RM INTERBANK BORROWINGS FROM MB           +                       0.00                    0.00            
           4314013000000Y  RM INTERBANK BORROWINGS FROM DH           +                       0.00                    0.00            
           4314017000000Y  O/W RM IBB FROM CAGAMAS           0.00    +                       0.00                    0.00            
           4314020000000Y  RM INTERBANK BORROWINGS FROM DNBFI        +                       0.00                    0.00            
           4314081100000Y  RM INTERBANK BORROWINGS FROM FBI <= 1 YR  +             150,000,000.00          150,000,000.00            
           4410000000000Y  RM MISC BORROWINGS                        +          10,197,952,678.54       10,197,952,678.54            
           4911080000000Y  RM INTEREST PAYABLE TO NON-RESIDENTS      +              53,372,766.43           53,372,766.43            
           4911095000000Y  RM INTEREST PAYABLE TO NON-RES - DCI/CRA  +                  28,349.42               28,349.42            
           4912080000000Y  RM BILLS PAYABLE TO NON-RESIDENTS         +                       0.00                    0.00            
           4929980000000Y  OTHER RM MISC LIAB NIE DUE TO NON-RES     +                       0.00                    0.00            
           4929995000000Y  RM GOLD INVESTMENT FROM NON-RESIDENTS     +              38,161,231.95           38,161,231.95            
           4929996000000Y  OTHR RM MISC LIAB NIE DUE TO NON-RES-DCI  +                     583.12                  583.12            
           4411100000000Y  RM SUBORDINATED DEBT CAPITAL              -           4,997,935,844.48        4,997,935,844.48            
           4411200000000Y  RM EXEMPT SUBORDINATED DEBT CAPITAL       -                       0.00                    0.00            
           4411300000000Y  RM SUBORDIN DEBT CAPITAL W APPR FR BNM    -                       0.00                    0.00            
           4414000000000Y  RM RESOURCE OBLIQ ON LN SOLD TO CAGAMAS   -           5,200,016,834.06        5,200,016,834.06            
  -----------------------------------------------------------------------------------------------------------------------            
           TOTAL FOR A-RMEL                                                    322,796,980,007.97      322,796,980,007.97            


please fix the code so it can follow exactly as per production
