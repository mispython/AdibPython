import polars as pl

def npgs3rpt(df, rdate):
    """Minimal SCH=93 report"""
    if df.is_empty():
        print("No data for SCH=93")
        return df
    
    print(f"SCH=93 Report - {rdate}")
    print("=" * 80)
    
    # Select and rename columns for display
    cols = ['CVAR02','CVAR03','CVAR04','CVAR06','CVAR08','CVAR09',
            'CURBAL','ACCRUAL','CVAR11','CVAR12A','CVAR13',
            'CVARX2','CVARX3','CVAR01','TRANCHE']
    
    display_df = df.select([c for c in cols if c in df.columns])
    
    # Rename for readability
    rename_map = {
        'CVAR02': 'SCH',
        'CVAR03': 'IC_NO',
        'CVAR04': 'CUSTOMER_NAME',
        'CVAR06': 'ACCT_NO',
        'CVAR08': 'LOAN_AMT',
        'CVAR09': 'OS_BALANCE',
        'CURBAL': 'PRINCIPAL',
        'ACCRUAL': 'INTEREST',
        'CVAR11': 'ARREARS',
        'CVAR12A': 'STATUS',
        'CVAR13': 'NPL_DATE',
        'CVARX2': 'NPL_NOTIFY',
        'CVARX3': 'NPL_REASON',
        'CVAR01': 'APP_NO',
        'TRANCHE': 'TRANCHE'
    }
    
    display_df = display_df.rename({k:v for k,v in rename_map.items() if k in display_df.columns})
    print(display_df)
    print(f"\nTotal: {len(df)} records")
    
    return df

def generate_report(df, output_file=None, rdate=""):
    """For other programs to call"""
    result = npgs3rpt(df, rdate)
    if output_file and not df.is_empty():
        df.write_csv(output_file)
    return result

# SAS compatibility
pgm = generate_report
