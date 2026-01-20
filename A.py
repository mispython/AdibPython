# npgs_reports.py - SINGLE UNIFIED REPORT MODULE FOR ALL SAS CONVERSIONS
import polars as pl
from datetime import datetime

def generate_report(df, sch_type="", rdate="", output_file=None, bank_name="PUBLIC BANK BERHAD"):
    """
    Unified report generator for all SCH types
    
    Parameters:
    -----------
    df : polars.DataFrame
        Input data
    sch_type : str
        Report type: '93', '94', '101', 'E5', '7Q', '8Q', 'MEF', 'SRGF', 'NON-PG', 'SRGF'
    rdate : str
        Report date in DDMMYY format
    output_file : str or Path, optional
        Output file path
    bank_name : str, optional
        Bank name (default: 'PUBLIC BANK BERHAD')
    """
    if df.is_empty():
        print(f"No data for {sch_type}")
        return df
    
    # Auto-detect SCH type from CVAR02 if not specified
    if not sch_type and 'CVAR02' in df.columns and len(df) > 0:
        sch_types = df['CVAR02'].unique().to_list()
        sch_type = sch_types[0] if sch_types else ""
    
    print("=" * 80)
    print(bank_name)
    
    # Set title based on SCH type
    if sch_type == 'E5':
        title = f"DETAIL OF ACCTS (SCH={sch_type}) FOR SUBMISSION TO CGC @ {rdate}"
    elif sch_type in ['93', '94', '101', '7Q', '8Q']:
        title = f"DETAIL OF ACCTS (SCH={sch_type}) FOR SUBMISSION TO CGC @ {rdate}"
    elif sch_type == 'SRGF':
        title = f"ACCOUNT DETAILS (SCHEME: SRGF) FOR CGC @ {rdate}"
    elif sch_type == 'NON-PG':
        title = f"DETAIL OF ACCTS NON-PG FOR SUBMISSION TO CGC @ {rdate}"
    elif sch_type == 'MEF':
        title = f"DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ {rdate}"
    else:
        title = f"REPORT @ {rdate}"
    
    print(title)
    print("=" * 80)
    
    # Select and rename columns based on SCH type
    if sch_type == '101' or sch_type in ['7Q', '8Q']:
        # SCH=101, 7Q, 8Q columns (similar format)
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
    elif sch_type == 'E5':
        # SCH=E5 columns
        cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR05','CVAR06',
                'CVAR07','CVAR08','CVAR09','CVAR10','CVAR11','CVAR12',
                'CVAR13','CVAR14','CVAR15','BRANCH']
        rename = {
            'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
            'CVAR04': 'CUSTOMER_NAME', 'CVAR05': 'DISB_DATE',
            'CVAR06': 'ACCT_NO', 'CVAR07': 'TYPE', 'CVAR08': 'LOAN_AMT',
            'CVAR09': 'OS_BALANCE', 'CVAR10': 'CREDIT_BAL', 'CVAR11': 'ARREARS',
            'CVAR12': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVAR14': 'FI_CODE',
            'CVAR15': 'MICR_CODE', 'BRANCH': 'BRANCH'
        }
    elif sch_type == 'SRGF':
        # SRGF columns (CVAR09=CREDIT, CVAR10=DEBIT - swapped from MEF)
        cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR05','CVAR06',
                'CVAR07','CVAR08','CVAR09','CVAR10','CVAR11','CVAR12',
                'CVAR13','CVAR14','CVAR15','BRANCH']
        rename = {
            'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
            'CVAR04': 'CUSTOMER_NAME', 'CVAR05': 'DISB_DATE',
            'CVAR06': 'ACCT_NO', 'CVAR07': 'TYPE', 'CVAR08': 'APPROVE_LIMIT',
            'CVAR09': 'CREDIT_BAL', 'CVAR10': 'DEBIT_BAL', 'CVAR11': 'ARREARS',
            'CVAR12': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVAR14': 'FI_CODE',
            'CVAR15': 'MICR_CODE', 'BRANCH': 'BRANCH'
        }
    else:
        # Default/MEF/NON-PG columns
        cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR05','CVAR06',
                'CVAR07','CVAR08','CVAR09','CVAR10','CVAR11','CVAR12',
                'CVAR13','CVAR14','CVAR15','BRANCH']
        rename = {
            'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
            'CVAR04': 'CUSTOMER_NAME', 'CVAR05': 'DISB_DATE',
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
    
    # Show summary by CVAR02 if available
    if 'CVAR02' in df.columns:
        summary = df.group_by("CVAR02").agg(pl.count().alias("count")).sort("CVAR02")
        if len(summary) > 1:
            print("\nBreakdown by SCH type:")
            for row in summary.iter_rows(named=True):
                print(f"  SCH={row['CVAR02']}: {row['count']}")
    
    # Save to file if requested
    if output_file:
        df.write_csv(output_file)
        print(f"\nData saved to: {output_file}")
    
    return df


# ============================================================================
# SPECIFIC FUNCTIONS FOR EACH SAS PROGRAM (for backward compatibility)
# ============================================================================

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
    """For SCH=101/7Q/8Q reports (EIBRSMEZ, EIBRTRRF)"""
    # Auto-detect which SCH type
    if not df.is_empty() and 'CVAR02' in df.columns:
        sch_type = df['CVAR02'][0] if len(df) > 0 else "101"
        return generate_report(df, sch_type, rdate, output_file)
    return generate_report(df, "101", rdate, output_file)

def cgcrpt(df, rdate, output_file=None):
    """For Islamic bank reports (EIBRTLIO)"""
    return generate_report(df, "", rdate, output_file, "PUBLIC ISLAMIC BANK BERHAD")

# ============================================================================
# SAS-STYLE ALIASES
# ============================================================================

# SAS %INC PGM(...) compatibility
pgm = generate_report  # Generic fallback

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_formatted_report(df, rdate, sch_type=""):
    """Print formatted report similar to SAS PROC REPORT"""
    if df.is_empty():
        return
    
    # Determine column widths based on data
    col_widths = {}
    for col in df.columns:
        if col.startswith('CVAR'):
            # Estimate width based on column name
            if col in ['CVAR04', 'CVAR03']:
                col_widths[col] = max(30, df[col].str.len_chars().max())
            elif col in ['CVAR08', 'CVAR09', 'CVAR10']:
                col_widths[col] = 12  # Numeric columns
            else:
                col_widths[col] = 10
    
    # Print header
    print("\n" + "=" * 80)
    print(f"Formatted Report - {rdate}")
    if sch_type:
        print(f"SCH Type: {sch_type}")
    print("=" * 80)
    
    # Print column headers
    headers = []
    for col in df.columns:
        if col in col_widths:
            headers.append(f"{col:<{col_widths[col]}}")
    print(" ".join(headers))
    print("-" * 80)
    
    # Print data (first 20 rows)
    for row in df.head(20).iter_rows(named=True):
        row_data = []
        for col in df.columns:
            if col in col_widths:
                val = row[col]
                if isinstance(val, (int, float)):
                    row_data.append(f"{val:<{col_widths[col]}.2f}")
                else:
                    row_data.append(f"{str(val):<{col_widths[col]}}")
        print(" ".join(row_data))
    
    if len(df) > 20:
        print(f"... and {len(df) - 20} more rows")


# ============================================================================
# TEST FUNCTION
# ============================================================================

def test_all_reports():
    """Test function to verify all report types work"""
    import pandas as pd
    
    # Create test data for each SCH type
    test_data = {
        'CVAR01': [1001, 1002, 1003],
        'CVAR02': ['93', '94', '101'],  # Different SCH types
        'CVAR03': ['IC123', 'IC456', 'IC789'],
        'CVAR04': ['Test Company A', 'Test Company B', 'Test Company C'],
        'CVAR05': ['2023-01-15', '2023-02-20', '2023-03-10'],
        'CVAR06': [123456, 789012, 345678],
        'CVAR07': ['A', 'B', 'C'],
        'CVAR08': [100000.50, 200000.75, 150000.25],
        'CVAR09': [50000.25, 100000.50, 75000.75],
        'CVAR10': [0.00, 10000.25, 5000.50],
        'CVAR11': [30, 45, 0],
        'CVAR12': ['AP', 'NP', 'AP'],
        'CVAR13': ['', '15/01/2023', ''],
        'CVAR14': ['F001', 'F002', 'F003'],
        'CVAR15': ['MC01', 'MC02', 'MC03'],
        'BRANCH': [101, 102, 103]
    }
    
    df = pl.DataFrame(test_data)
    rdate = "151223"
    
    print("Testing all report types...")
    print("-" * 60)
    
    # Test each report type
    reports_to_test = [
        ("MEF", npgsrpt),
        ("93", npgs3rpt),
        ("94", npgs4rpt),
        ("101", npgs5rpt),
        ("SRGF", generate_report),
    ]
    
    for sch_type, report_func in reports_to_test:
        print(f"\nTesting {sch_type} report:")
        print("-" * 40)
        # Filter data for this SCH type
        if sch_type != "MEF":
            test_df = df.filter(pl.col("CVAR02") == sch_type)
        else:
            test_df = df
        report_func(test_df, rdate)
    
    print("\n" + "=" * 60)
    print("All reports tested successfully!")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_all_reports()
    else:
        print("NPGS Reports Module")
        print("=" * 60)
        print("Available functions:")
        print("  generate_report(df, sch_type, rdate, output_file=None)")
        print("  npgsrpt(df, rdate, output_file=None)       # MEF reports")
        print("  npgs3rpt(df, rdate, output_file=None)      # SCH=93")
        print("  npgs4rpt(df, rdate, output_file=None)      # SCH=94")
        print("  npgs5rpt(df, rdate, output_file=None)      # SCH=101/7Q/8Q")
        print("  cgcrpt(df, rdate, output_file=None)        # Islamic bank")
        print("\nUsage from other programs:")
        print("  from npgs_reports import generate_report as pgm")
        print("  pgm(df, '93', '151223', 'output.txt')")
