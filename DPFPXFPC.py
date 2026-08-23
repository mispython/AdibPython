# NPGSRT.py
# Replicates SAS PROC REPORT DATA=NPGS
# This module assumes NPGS DataFrame is already available globally

import pandas as pd
import numpy as np

def pgm_npgsrt():
    """
    Replicates SAS PROC REPORT DATA=NPGS
    Assumes NPGS DataFrame is available in global namespace
    """
    import __main__
    
    # Get NPGS from global namespace (like SAS dataset)
    if hasattr(__main__, 'NPGS'):
        NPGS = __main__.NPGS
    else:
        raise ValueError("NPGS dataset not found in global namespace")
    
    # COLUMN statement
    column_order = [
        'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 
        'CVAR07', 'CVAR08', 'CVARXX', 'CVAR09', 'CVAR10', 'CVAR11', 
        'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15', 'BRANCH'
    ]
    
    # DEFINE statements
    define_specs = {
        'CVAR01':  {'format': '10.',       'label': 'REFER.NUM '},
        'CVAR02':  {'format': '$3.',       'label': 'SCH'},
        'CVAR03':  {'format': '$15.',      'label': 'IC /BUSS. NUM.'},
        'CVAR04':  {'format': '$50.',      'label': 'NAME OF CUSTOMER'},
        'CVAR05':  {'format': 'DDMMYY10.', 'label': 'DISBURSE'},
        'CVARXX':  {'format': '$10.',      'label': '              '},
        'CVAR06':  {'format': '10.',       'label': 'ACCOUNT NUMBER'},
        'CVAR07':  {'format': '$2.',       'label': 'TY'},
        'CVAR08':  {'format': '13.2',      'label': 'APPROVE LIMIT'},
        'CVAR09':  {'format': '13.2',      'label': 'DEBIT  BALANCE'},
        'CVAR10':  {'format': '13.2',      'label': 'CREDIT BALANCE'},
        'CVAR11':  {'format': '7.',        'label': 'ARREARS'},
        'CVAR12':  {'format': '$3.',       'label': 'ST '},
        'CVAR13':  {'format': '$10.',      'label': 'NPL DATE'},
        'CVAR14':  {'format': '$4.',       'label': 'FI  CODE'},
        'CVAR15':  {'format': '$5.',       'label': 'MICR CODE'},
        'BRANCH':  {'format': '3.',        'label': 'BRH'}
    }
    
    # Create report DataFrame
    report_df = pd.DataFrame()
    
    # Apply formatting based on DEFINE statements
    for col in column_order:
        if col in NPGS.columns:
            spec = define_specs[col]
            fmt = spec['format']
            label = spec['label']
            
            # Character format ($)
            if fmt.startswith('$'):
                width = int(fmt.replace('$', '').replace('.', ''))
                report_df[label] = NPGS[col].astype(str).str[:width].str.ljust(width)
            
            # Date format (DDMMYY)
            elif 'DDMMYY' in fmt:
                report_df[label] = pd.to_datetime(NPGS[col], errors='coerce').dt.strftime('%d/%m/%Y')
            
            # Decimal format (13.2)
            elif '.' in fmt and not fmt.endswith('.'):
                width, decimals = fmt.split('.')
                report_df[label] = NPGS[col].apply(
                    lambda x: f"{x:>{int(width)}.{int(decimals)}f}" if pd.notna(x) else " " * int(width)
                )
            
            # Integer format (10., 7., 3.)
            else:
                width = int(fmt.replace('.', ''))
                report_df[label] = NPGS[col].apply(
                    lambda x: f"{x:>{width}.0f}" if pd.notna(x) else " " * width
                )
    
    return report_df


# Auto-execute when run directly
if __name__ == "__main__":
    result = pgm_npgsrt()
    print(result.to_string(index=False))
