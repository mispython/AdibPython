import polars as pl

def npgsrpt(npgs_df, rdate):
    """Simple report generator for NPGS data"""
    if npgs_df.is_empty():
        return ""
    
    # Format columns like SAS DISPLAY FORMAT
    def format_col(col, value):
        if col == 'CVAR01': return f"{value:10.0f}"
        if col == 'CVAR02': return f"{value:>3}"
        if col == 'CVAR03': return f"{value:>15}"
        if col == 'CVAR04': return f"{value:<50}"
        if col == 'CVAR05': 
            try:
                if hasattr(value, 'strftime'): return value.strftime("%d/%m/%Y")
                return str(value).rjust(10)
            except: return " " * 10
        if col == 'CVAR06': return f"{value:10.0f}"
        if col == 'CVAR07': return f"{value:>2}"
        if col in ['CVAR08','CVAR09','CVAR10']: return f"{value:13.2f}"
        if col == 'CVAR11': return f"{value:7.0f}"
        if col == 'CVAR12': return f"{value:>3}"
        if col == 'CVAR13': return f"{value:>10}"
        if col == 'CVAR14': return f"{value:>4}"
        if col == 'CVAR15': return f"{value:>5}"
        if col == 'BRANCH': return f"{value:3.0f}"
        return str(value)
    
    # Build report
    lines = []
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ {rdate}")
    lines.append("="*120)
    
    # Headers
    headers = ["REFER.NUM","SCH","IC /BUSS. NUM.","NAME OF CUSTOMER",
               "DISBURSE"," "*10,"ACCOUNT NUMBER","TY","APPROVE LIMIT",
               "DEBIT BALANCE","CREDIT BALANCE","ARREARS","ST",
               "NPL DATE","FI CODE","MICR CODE","BRH"]
    widths = [10,3,15,50,10,10,10,2,13,13,13,7,3,10,4,5,3]
    
    lines.append(" ".join(f"{h:<{w}}" for h,w in zip(headers, widths)))
    lines.append("-"*120)
    
    # Data rows
    cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR05','CVARXX',
            'CVAR06','CVAR07','CVAR08','CVAR09','CVAR10','CVAR11',
            'CVAR12','CVAR13','CVAR14','CVAR15','BRANCH']
    
    for row in npgs_df.iter_rows(named=True):
        row_vals = []
        for col in cols:
            val = row.get(col, "" if 'CVAR' in col else 0)
            if col == 'CVARXX': val = " " * 10  # Always 10 spaces
            row_vals.append(format_col(col, val))
        lines.append(" ".join(row_vals))
    
    lines.append(f"\nTotal Records: {len(npgs_df)}")
    
    report = "\n".join(lines)
    print(report)
    return report

# For other programs to call (like %INC PGM(NPGSRPT))
def generate_report(npgs_df, output_file=None, rdate=""):
    """Called by EIBRP159, EIBRSMEZ programs"""
    report = npgsrpt(npgs_df, rdate)
    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
    return report

# Quick test if run directly
if __name__ == "__main__":
    # Sample data for testing
    data = {
        'CVAR01': [1001, 1002],
        'CVAR02': ['93', '94'],
        'CVAR03': ['IC123456', 'IC789012'],
        'CVAR04': ['COMPANY A', 'COMPANY B'],
        'CVAR05': ['2023-01-15', '2023-02-20'],
        'CVAR06': [1234567890, 9876543210],
        'CVAR07': ['A', 'B'],
        'CVAR08': [100000.50, 200000.75],
        'CVAR09': [50000.25, 150000.00],
        'CVAR10': [0.00, 50000.75],
        'CVAR11': [30, 45],
        'CVAR12': ['AP', 'NP'],
        'CVAR13': ['', '15/01/2023'],
        'CVAR14': ['F001', 'F002'],
        'CVAR15': ['MC01', 'MC02'],
        'BRANCH': [101, 102]
    }
    
    df = pl.DataFrame(data)
    npgsrpt(df, "151223")
