
# npgs_reports.py - SINGLE MODULE FOR ALL REPORTS
import polars as pl

def generate_report(df, sch_type="", rdate="", output_file=None):
    """One function for all SCH reports (93, 94, 101, MEF)"""
    if df.is_empty():
        print(f"No data for SCH={sch_type}")
        return df
    
    # Auto-detect SCH type from CVAR02 if not specified
    if not sch_type and 'CVAR02' in df.columns and len(df) > 0:
        sch_type = df['CVAR02'][0]
    
    print("=" * 80)
    print(f"PUBLIC BANK BERHAD")
    
    # Set title based on SCH type
    if sch_type in ['93', '94', '101']:
        print(f"DETAIL OF ACCTS (SCH={sch_type}) FOR SUBMISSION TO CGC @ {rdate}")
    else:
        print(f"DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ {rdate}")
    
    print("=" * 80)
    
    # Select and rename columns based on SCH type
    if sch_type == '101':
        # SCH=101 columns
        cols = ['CVAR02','CVAR03','CVAR04','CVAR06','CVARX5','CVAR16',
                'CVAR08','CVAR09','CVAR17','ACCRUALX','CVAR10',
                'CVAR11','CVAR12A','CVAR13','CVARX2','CVARX3','CVAR01']
        rename = {
            'CVAR02': 'SCH', 'CVAR03': 'IC_NO', 'CVAR04': 'CUSTOMER_NAME',
            'CVAR06': 'ACCT_NO', 'CVARX5': 'DISB_DATE', 'CVAR16': 'FAC_TYPE',
            'CVAR08': 'LOAN_AMT', 'CVAR09': 'OS_BALANCE', 'CVAR17': 'PRINCIPAL',
            'ACCRUALX': 'INTEREST', 'CVAR10': 'CREDIT_BAL', 'CVAR11': 'ARREARS',
            'CVAR12A': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVARX2': 'NPL_NOTIFY',
            'CVARX3': 'NPL_REASON', 'CVAR01': 'APP_NO'
        }
    elif sch_type in ['93', '94']:
        # SCH=93/94 columns
        cols = ['CVAR02','CVAR03','CVAR04','CVAR06','CVAR08','CVAR09',
                'CURBAL','ACCRUAL','CVAR11','CVAR12A','CVAR13',
                'CVARX2','CVARX3','CVAR01','TRANCHE']
        rename = {
            'CVAR02': 'SCH', 'CVAR03': 'IC_NO', 'CVAR04': 'CUSTOMER_NAME',
            'CVAR06': 'ACCT_NO', 'CVAR08': 'LOAN_AMT', 'CVAR09': 'OS_BALANCE',
            'CURBAL': 'PRINCIPAL', 'ACCRUAL': 'INTEREST', 'CVAR11': 'ARREARS',
            'CVAR12A': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVARX2': 'NPL_NOTIFY',
            'CVARX3': 'NPL_REASON', 'CVAR01': 'APP_NO', 'TRANCHE': 'TRANCHE'
        }
    else:
        # MEF/NPGS default columns
        cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR05','CVAR06',
                'CVAR07','CVAR08','CVAR09','CVAR10','CVAR11','CVAR12',
                'CVAR13','CVAR14','CVAR15','BRANCH']
        rename = {
            'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
            'CVAR04': 'CUSTOMER_NAME', 'CVAR05': 'DISBURSE_DATE',
            'CVAR06': 'ACCT_NO', 'CVAR07': 'TYPE', 'CVAR08': 'APPROVE_LIMIT',
            'CVAR09': 'DEBIT_BAL', 'CVAR10': 'CREDIT_BAL', 'CVAR11': 'ARREARS',
            'CVAR12': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVAR14': 'FI_CODE',
            'CVAR15': 'MICR_CODE', 'BRANCH': 'BRANCH'
        }
    
    # Select available columns only
    available_cols = [c for c in cols if c in df.columns]
    if not available_cols:
        print("No matching columns found in data")
        return df
    
    display_df = df.select(available_cols)
    
    # Rename available columns
    rename_available = {k:v for k,v in rename.items() if k in display_df.columns}
    if rename_available:
        display_df = display_df.rename(rename_available)
    
    # Show the data
    print(display_df)
    print(f"\nTotal records: {len(df)}")
    
    # Save to file if requested
    if output_file:
        df.write_csv(output_file)
        print(f"Data saved to: {output_file}")
    
    return df

# ALIASES FOR SAS COMPATIBILITY - OTHER PROGRAMS CAN USE THESE
def npgsrpt(df, rdate, output_file=None):
    """For MEF/NPGS reports (EIBRP159)"""
    return generate_report(df, "MEF", rdate, output_file)

def npgs3rpt(df, rdate, output_file=None):
    """For SCH=93 reports (EIBRSMEZ)"""
    return generate_report(df, "93", rdate, output_file)

def npgs4rpt(df, rdate, output_file=None):
    """For SCH=94 reports (EIBRSMEZ)"""
    return generate_report(df, "94", rdate, output_file)

def npgs5rpt(df, rdate, output_file=None):
    """For SCH=101 reports (EIBRSMEZ)"""
    return generate_report(df, "101", rdate, output_file)

# SAS-style alias for %INC PGM(...)
pgm = generate_report  # Generic fallback
