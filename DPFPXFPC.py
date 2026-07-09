import polars as pl
from datetime import datetime, timedelta
import sys
import os
import re

# Constants
GLFILE_TXT = 'data/glfile.txt'
STORE_DIR = 'data/store/'

# Use yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

def read_gl_text_file_fixed_width(filepath):
    """Read GL text file with correct fixed-width format"""
    
    with open(filepath, 'r') as f:
        lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
    
    print(f"Total lines: {len(lines)}")
    print(f"First few lines:")
    for i, line in enumerate(lines[:5]):
        print(f"Line {i+1}: '{line}'")
        print(f"  Length: {len(line)}")
        # Show the first 60 characters with positions
        if len(line) > 0:
            chars = list(line[:60])
            print(f"  First 60 chars: {''.join(chars)}")
            # Show character positions
            positions = []
            for j in range(0, min(60, len(line))):
                if j % 10 == 0:
                    positions.append(f"{j:>3}")
            print(f"  Positions:     {' '.join(positions)}")
            # Show the characters with indices
            char_pos = []
            for j in range(0, min(60, len(line))):
                if j % 10 == 0:
                    char_pos.append(f"{j:>3}")
            print(f"  Characters:    {' '.join(chars[:60])}")
        print()
    
    # Parse each line based on the actual format
    data = []
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Extract fields based on fixed positions
        # Format: GLITEM(8) + spaces + DATE(8) + spaces + BALANCE(15) + SIGN(1)
        glitem = line[0:8].strip() if len(line) > 8 else ''
        date = line[20:28].strip() if len(line) > 28 else ''
        balance_str = line[45:60].strip() if len(line) > 60 else ''
        sign = line[-1] if len(line) > 0 else ''
        
        # Clean up balance string (remove commas)
        balance_str = balance_str.replace(',', '')
        
        # Convert balance to float
        try:
            balance = float(balance_str) if balance_str else 0.0
        except ValueError:
            balance = 0.0
        
        # Apply sign
        if sign == '-':
            balance = -balance
        
        # Extract YY, MM, DD from the first line (date)
        if glitem == '20260708':  # This is the header line with date
            yy = glitem[0:2]
            mm = glitem[2:4]
            dd = glitem[4:6]
            continue  # Skip this row as it's just the date header
        
        # For other rows, try to parse YY, MM, DD from the data
        # The GLITEM contains the account code, but we need YY, MM, DD from somewhere
        # Let's use the date from the header line
        if 'yy' not in locals():
            # Use yesterday's date as fallback
            yy = reptyear[2:4]
            mm = reptmon
            dd = reptday
        else:
            # Use the date from the header
            pass
        
        data.append({
            'YY': yy,
            'MM': mm,
            'DD': dd,
            'GLITEM': glitem,
            'DATE': date,
            'BALANCE': balance,
            'SIGN': sign
        })
    
    if data:
        df = pl.DataFrame(data)
        return df, 'fixed-width-corrected'
    else:
        return None, None

def process_gl_data(df_gl, suffix):
    """Process GL data for a given suffix (P1 or P2)"""
    
    print(f"\nProcessing {suffix}...")
    print(f"DataFrame shape: {df_gl.shape}")
    print(f"Columns: {df_gl.columns}")
    print(f"First few rows:")
    print(df_gl.head(10))
    
    # Define conditions based on suffix
    if suffix == 'P1':
        conditions = [
            ('F142630C', 'B1.12'),
            ('42699', 'B1.14'),
            ('44111', 'A1.18'),
            ('F147100', 'A1.18'),
            ('F249299K', 'A1.20'),
            ('49120', 'A1.20'),
            ('42199', 'A1.20'),
            ('49120NLF', 'A1.20'),
            ('42190', 'A1.20'),
            ('F144611FXSDC', 'B1.18'),
            ('F147600', 'B1.18'),
            ('F143110VCB', 'A2.21'),
            ('F143110VFBI', 'A2.21'),
            ('F143120ODNVB', 'A2.21'),
            ('F143120ODNIB', 'A2.21'),
            ('F143620FNFBI', 'B2.21'),
            ('F133110ODVIB', 'A2.01'),
            ('F13312002CB', 'A2.01'),
            ('F132121BBNM', 'A2.01'),
            ('37070', 'A2.08'),
            ('F137610FXSH', 'B2.08'),
            ('F137650FXCDS', 'B2.08'),
            ('F133620FNFBI', 'B2.01')
        ]
    else:  # P2
        conditions = [
            ('F142630C', 'B1.12'),
            ('42699', 'B1.14'),
            ('44111', 'A1.18'),
            ('F147100', 'A1.18'),
            ('F147600', 'B1.18'),
            ('F144611FXSDC', 'B1.18'),
            ('F249299K', 'A1.20'),
            ('49120', 'A1.20'),
            ('42199', 'A1.20'),
            ('49120NLF', 'A1.20'),
            ('F143110VCB', 'A2.21'),
            ('F143110VFBI', 'A2.21'),
            ('F143120ODNVB', 'A2.21'),
            ('F143120ODNIB', 'A2.21'),
            ('F143620FNFBI', 'B2.21'),
            ('F133110ODVIB', 'A2.01'),
            ('F13312002CB', 'A2.01'),
            ('F132121BBNM', 'A2.01'),
            ('37070', 'A2.08'),
            ('F137610FXSH', 'B2.08'),
            ('F137650FXCDS', 'B2.08'),
            ('F133620FNFBI', 'B2.01')
        ]
    
    # Process each GLITEM
    rows = []
    for glitem_code, item_code in conditions:
        filtered = df_gl.filter(pl.col('GLITEM') == glitem_code)
        if len(filtered) > 0:
            balance = filtered['BALANCE'].sum()
            rows.append({
                'ITEM': item_code,
                'BALANCE': balance,
                'WEEK': balance if item_code.startswith('B1') and item_code not in ['B1.12', 'B1.14'] else 0,
                'MONTH': balance if item_code.startswith('B1') and item_code not in ['B1.12', 'B1.14'] else 0,
                'QTR': 0,
                'HALFYR': 0,
                'YEAR': 0,
                'LAST': balance if item_code in ['B1.12', 'B1.14'] else 0,
                'TOTAL': balance if item_code.startswith('A') else 0
            })
    
    if not rows:
        print(f"No data found for {suffix}")
        return None, None, None, None, None
    
    glfilep = pl.DataFrame(rows)
    
    # Calculate balance as sum of all fields
    glfilep = glfilep.with_columns([
        (pl.col('WEEK') + pl.col('MONTH') + pl.col('QTR') + 
         pl.col('HALFYR') + pl.col('YEAR') + pl.col('LAST') + 
         pl.col('TOTAL')).alias('BALANCE_CALC')
    ])
    
    # Group by ITEM and aggregate
    glfilep = glfilep.group_by('ITEM').agg([
        pl.col('WEEK').sum().alias('WEEK'),
        pl.col('MONTH').sum().alias('MONTH'),
        pl.col('QTR').sum().alias('QTR'),
        pl.col('HALFYR').sum().alias('HALFYR'),
        pl.col('YEAR').sum().alias('YEAR'),
        pl.col('LAST').sum().alias('LAST'),
        pl.col('TOTAL').sum().alias('TOTAL'),
        pl.col('BALANCE').sum().alias('BALANCE')
    ])
    
    # Convert to thousands
    glfilep = glfilep.with_columns([
        (pl.col('WEEK') / 1000).round(3).alias('WEEK'),
        (pl.col('MONTH') / 1000).round(3).alias('MONTH'),
        (pl.col('QTR') / 1000).round(3).alias('QTR'),
        (pl.col('HALFYR') / 1000).round(3).alias('HALFYR'),
        (pl.col('YEAR') / 1000).round(3).alias('YEAR'),
        (pl.col('LAST') / 1000).round(3).alias('LAST'),
        (pl.col('TOTAL') / 1000).round(3).alias('TOTAL'),
        (pl.col('BALANCE') / 1000).round(3).alias('BALANCE')
    ])
    
    # Split into categories
    glrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
    glfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glrmfxp = glfilep.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glutrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    glutfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    
    # Save files
    os.makedirs(STORE_DIR, exist_ok=True)
    
    timestamp = f"{reptyear}{reptmon}{reptday}"
    
    for df, name in [(glrmp, f'GLRM{suffix}{timestamp}'),
                     (glfxp, f'GLFX{suffix}{timestamp}'),
                     (glrmfxp, f'GLRMFX{suffix}{timestamp}'),
                     (glutrmp, f'GLUTRM{suffix}{timestamp}'),
                     (glutfxp, f'GLUTFX{suffix}{timestamp}')]:
        if len(df) > 0:
            # Save as parquet
            df.write_parquet(f'{STORE_DIR}{name}.parquet')
            print(f"Saved: {STORE_DIR}{name}.parquet")
            
            # Save as SAS
            save_to_sas(df, name)
            
            # Print
            print(f"\n{name}:")
            print(df)
    
    return glrmp, glfxp, glrmfxp, glutrmp, glutfxp

def save_to_sas(df, dataset_name):
    """Save a Polars DataFrame to SAS dataset using saspy"""
    if len(df) == 0:
        return
    
    try:
        import saspy
        sas = saspy.SASsession()
        df_pandas = df.to_pandas()
        sas.sasdata(df_pandas, table=dataset_name, libref='WORK')
        sas.submit(f"""
            libname out '{STORE_DIR}';
            data out.{dataset_name};
                set {dataset_name};
            run;
        """)
        print(f"SAS dataset saved: {STORE_DIR}{dataset_name}.sas7bdat")
    except ImportError:
        print("saspy not installed. SAS datasets will not be created.")
    except Exception as e:
        print(f"Error creating SAS dataset {dataset_name}: {e}")

# Main execution
if __name__ == "__main__":
    os.makedirs(STORE_DIR, exist_ok=True)
    
    print("Reading GL text file with fixed-width format...")
    df_gl, format_type = read_gl_text_file_fixed_width(GLFILE_TXT)
    
    if df_gl is None:
        print(f"ERROR: Could not read {GLFILE_TXT}")
        sys.exit(77)
    
    print(f"\nSuccessfully read file with {df_gl.height} rows")
    print(f"Columns: {df_gl.columns}")
    print(f"\nData sample:")
    print(df_gl.head(10))
    
    # Process P1
    print("\n" + "="*60)
    print("Processing GL P1...")
    print("="*60)
    results_p1 = process_gl_data(df_gl, 'P1')
    
    # Process P2
    print("\n" + "="*60)
    print("Processing GL P2...")
    print("="*60)
    results_p2 = process_gl_data(df_gl, 'P2')
    
    print("\n" + "="*60)
    print("Processing complete!")
    print("="*60)
