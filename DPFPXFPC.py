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

def read_gl_text_file(filepath):
    """Read GL text file with proper parsing"""
    
    with open(filepath, 'r') as f:
        lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
    
    print(f"Total lines: {len(lines)}")
    
    # Parse each line
    data = []
    header_date = None
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Check if this is the header line (8 digits followed by spaces)
        stripped = line.strip()
        if stripped.isdigit() and len(stripped) == 8:
            header_date = stripped
            print(f"Header date found: {header_date}")
            continue
        
        # Extract GLITEM (positions 0-8)
        glitem = line[0:8].strip() if len(line) > 8 else ''
        
        # Skip if GLITEM is empty or just spaces
        if not glitem or glitem.isspace():
            continue
        
        # Extract DATE (positions 20-28)
        date = line[20:28].strip() if len(line) > 28 else ''
        
        # Extract BALANCE (positions 45-60)
        balance_str = line[45:60].strip() if len(line) > 60 else ''
        
        # Check for sign at the end
        sign = line[-1] if len(line) > 0 else ''
        
        # Clean balance string
        balance_str = balance_str.replace(',', '')
        
        # Convert to float
        try:
            balance = float(balance_str) if balance_str else 0.0
        except ValueError:
            balance = 0.0
        
        # Apply sign
        if sign == '-':
            balance = -balance
        
        # Get date from header
        if header_date:
            yy = header_date[0:2]
            mm = header_date[2:4]
            dd = header_date[4:6]
        else:
            yy = reptyear[2:4]
            mm = reptmon
            dd = reptday
        
        # Only include if GLITEM looks valid (not just numbers that look like dates)
        if glitem and glitem != '08':
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
        return df
    else:
        return None

def match_glitem(file_glitem, condition_glitem):
    """Try to match file GLITEM with condition GLITEM using flexible matching"""
    if not file_glitem or not condition_glitem:
        return False
    
    # Clean both
    file_clean = file_glitem.strip()
    cond_clean = condition_glitem.strip()
    
    # Direct match
    if file_clean == cond_clean:
        return True
    
    # Check if file is a prefix of condition (for truncated values)
    if cond_clean.startswith(file_clean):
        return True
    
    # Check if condition is a prefix of file (for extra characters)
    if file_clean.startswith(cond_clean):
        return True
    
    # Handle '1F' prefix vs 'F' prefix
    if file_clean.startswith('1F') and cond_clean.startswith('F'):
        # Remove prefix from both
        file_no_prefix = file_clean[2:]  # Remove '1F'
        cond_no_prefix = cond_clean[1:]   # Remove 'F'
        if file_no_prefix == cond_no_prefix:
            return True
        if cond_no_prefix.startswith(file_no_prefix):
            return True
        if file_no_prefix.startswith(cond_no_prefix):
            return True
    
    # Handle '1F' prefix vs no prefix in condition
    if file_clean.startswith('1F'):
        file_no_prefix = file_clean[2:]  # Remove '1F'
        if file_no_prefix == cond_clean:
            return True
        if cond_clean.startswith(file_no_prefix):
            return True
        if file_no_prefix.startswith(cond_clean):
            return True
    
    # Handle 'NL' suffix variations
    if file_clean.endswith('NL') and not cond_clean.endswith('NL'):
        file_no_suffix = file_clean[:-2]  # Remove 'NL'
        if file_no_suffix == cond_clean:
            return True
        if cond_clean.startswith(file_no_suffix):
            return True
        if file_no_suffix.startswith(cond_clean):
            return True
    
    # Check if the last 5-6 characters match (for partial matches)
    if len(file_clean) >= 5 and len(cond_clean) >= 5:
        file_suffix = file_clean[-5:]
        cond_suffix = cond_clean[-5:]
        if file_suffix == cond_suffix:
            return True
    
    return False

def process_gl_data(df_gl, suffix):
    """Process GL data for a given suffix (P1 or P2)"""
    
    print(f"\nProcessing {suffix}...")
    print(f"DataFrame shape: {df_gl.shape}")
    
    # Define GLITEM mappings for P1 and P2
    if suffix == 'P1':
        glitem_mappings = [
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
        glitem_mappings = [
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
    
    # Get unique GLITEMs from the file
    file_glitems = df_gl['GLITEM'].unique().to_list()
    print(f"\nUnique GLITEMs in file ({len(file_glitems)}):")
    for glitem in sorted(file_glitems)[:20]:
        print(f"  {glitem}")
    if len(file_glitems) > 20:
        print(f"  ... and {len(file_glitems) - 20} more")
    
    # Create a mapping from file GLITEM to condition GLITEM
    mapping = {}
    for file_glitem in file_glitems:
        for cond_glitem, item_code in glitem_mappings:
            if match_glitem(file_glitem, cond_glitem):
                mapping[file_glitem] = (cond_glitem, item_code)
                print(f"Matched: '{file_glitem}' -> '{cond_glitem}' ({item_code})")
                break
    
    if not mapping:
        print(f"No matches found for {suffix}")
        # Try to find partial matches for debugging
        print("Attempting partial matches for debugging...")
        for file_glitem in file_glitems:
            for cond_glitem, item_code in glitem_mappings:
                if file_glitem[-5:] == cond_glitem[-5:]:
                    print(f"  Partial match: '{file_glitem}' -> '{cond_glitem}' ({item_code})")
                    break
        return None, None, None, None, None
    
    # Process each matched GLITEM
    rows = []
    for file_glitem, (cond_glitem, item_code) in mapping.items():
        filtered = df_gl.filter(pl.col('GLITEM') == file_glitem)
        if len(filtered) > 0:
            balance = filtered['BALANCE'].sum()
            
            # Determine which fields get the balance based on item code
            week = 0
            month = 0
            last = 0
            total = 0
            
            if item_code.startswith('B') and item_code not in ['B1.12', 'B1.14']:
                week = balance
                month = balance
            elif item_code in ['B1.12', 'B1.14']:
                last = balance
            elif item_code.startswith('A'):
                total = balance
            
            rows.append({
                'ITEM': item_code,
                'BALANCE': balance,
                'WEEK': week,
                'MONTH': month,
                'QTR': 0,
                'HALFYR': 0,
                'YEAR': 0,
                'LAST': last,
                'TOTAL': total
            })
    
    if not rows:
        print(f"No data found for {suffix}")
        return None, None, None, None, None
    
    glfilep = pl.DataFrame(rows)
    print(f"Created DataFrame with {len(rows)} rows for {suffix}")
    
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
    
    print(f"\nProcessed data for {suffix}:")
    print(glfilep)
    
    # Split into categories
    glrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
    glfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glrmfxp = glfilep.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glutrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    glutfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    
    # Save files
    os.makedirs(STORE_DIR, exist_ok=True)
    
    timestamp = f"{reptyear}{reptmon}{reptday}"
    
    # Save each dataset
    datasets = [
        (glrmp, f'GLRM{suffix}{timestamp}'),
        (glfxp, f'GLFX{suffix}{timestamp}'),
        (glrmfxp, f'GLRMFX{suffix}{timestamp}'),
        (glutrmp, f'GLUTRM{suffix}{timestamp}'),
        (glutfxp, f'GLUTFX{suffix}{timestamp}')
    ]
    
    for df, name in datasets:
        if len(df) > 0:
            # Save as parquet
            parquet_file = f'{STORE_DIR}{name}.parquet'
            df.write_parquet(parquet_file)
            print(f"Saved: {parquet_file}")
            
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
        
        # Convert Polars DataFrame to pandas
        df_pandas = df.to_pandas()
        
        # Create SAS dataset
        sas.sasdata(df_pandas, table=dataset_name)
        
        # Save to permanent SAS dataset
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
    
    print("Reading GL text file...")
    df_gl = read_gl_text_file(GLFILE_TXT)
    
    if df_gl is None:
        print(f"ERROR: Could not read {GLFILE_TXT}")
        sys.exit(77)
    
    print(f"\nSuccessfully read {df_gl.height} rows")
    print(f"Columns: {df_gl.columns}")
    print(f"\nData sample:")
    print(df_gl.head(10))
    
    # Show unique GLITEMs
    print(f"\nUnique GLITEMs in file ({df_gl['GLITEM'].n_unique()}):")
    for glitem in sorted(df_gl['GLITEM'].unique().to_list())[:30]:
        print(f"  {glitem}")
    
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
