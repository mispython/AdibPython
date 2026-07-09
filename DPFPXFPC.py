import polars as pl
from datetime import datetime, timedelta
import sys
import os
import re
import chardet

# Constants
GLFILE_TXT = 'data/glfile.txt'
STORE_DIR = 'data/store/'

# Use yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

def detect_encoding(filepath):
    """Detect the encoding of the file"""
    with open(filepath, 'rb') as f:
        raw_data = f.read(10000)  # Read first 10KB for detection
        result = chardet.detect(raw_data)
        return result['encoding']

def read_gl_text_file_with_encoding(filepath):
    """Read GL text file with proper encoding detection"""
    
    # First, detect the encoding
    encoding = detect_encoding(filepath)
    print(f"Detected encoding: {encoding}")
    
    # Try different encodings if detection fails
    encodings_to_try = [encoding, 'utf-16', 'utf-16le', 'utf-16be', 'utf-8', 'latin-1', 'cp1252']
    if encoding not in encodings_to_try:
        encodings_to_try.insert(0, encoding)
    
    for enc in encodings_to_try:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
            
            # Check if we got readable content
            if lines and len(lines) > 0:
                # Check if first line looks like a date (e.g., 20260708)
                first_line = lines[0].strip()
                if first_line.isdigit() and len(first_line) == 8:
                    print(f"Successfully read file with encoding: {enc}")
                    return lines, enc
                
                # Check if we have readable characters
                readable_count = sum(1 for c in first_line if c.isprintable() or c.isspace())
                if readable_count > len(first_line) * 0.5:
                    print(f"Successfully read file with encoding: {enc}")
                    return lines, enc
                    
        except Exception as e:
            print(f"Failed with encoding {enc}: {e}")
            continue
    
    # If all encodings fail, try reading as binary and decode manually
    print("All encodings failed, trying binary read...")
    with open(filepath, 'rb') as f:
        raw_data = f.read()
    
    # Try to find the pattern in binary data
    # Look for the date pattern (8 digits)
    date_pattern = rb'\d{8}'
    match = re.search(date_pattern, raw_data)
    if match:
        date_str = match.group().decode('ascii')
        print(f"Found date in binary: {date_str}")
    
    # Try to extract readable text from binary
    try:
        # Remove null bytes and other non-printable characters
        text = raw_data.decode('utf-16le', errors='ignore')
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if lines:
            print("Successfully extracted text using utf-16le with error ignoring")
            return lines, 'utf-16le-ignore'
    except:
        pass
    
    try:
        # Try UTF-16 with BOM
        text = raw_data.decode('utf-16', errors='ignore')
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if lines:
            print("Successfully extracted text using utf-16 with error ignoring")
            return lines, 'utf-16-ignore'
    except:
        pass
    
    print("Could not read file with any encoding")
    return None, None

def parse_gl_lines(lines, encoding):
    """Parse GL lines into structured data"""
    
    print(f"\nParsing {len(lines)} lines...")
    
    # Show first few lines for debugging
    print("\nFirst 5 lines after decoding:")
    for i, line in enumerate(lines[:5]):
        print(f"Line {i+1}: '{line}'")
        print(f"  Length: {len(line)}")
        # Show hex representation for debugging
        hex_repr = ' '.join(f'{ord(c):02x}' for c in line[:20])
        print(f"  Hex (first 20 chars): {hex_repr}")
        print()
    
    # Parse each line
    data = []
    header_date = None
    
    for line in lines:
        # Skip empty lines or lines that are just spaces
        if not line.strip() or line.strip() == '†' or line.strip() == 'ഊ':
            continue
        
        # Clean the line - remove non-printable characters
        clean_line = ''.join(c for c in line if c.isprintable() or c.isspace())
        if not clean_line.strip():
            continue
        
        # Try to extract date from the line (first line should be 8 digits)
        if clean_line.strip().isdigit() and len(clean_line.strip()) == 8:
            header_date = clean_line.strip()
            print(f"Found header date: {header_date}")
            continue
        
        # Try to parse the line based on fixed positions
        # The data appears to have: GLITEM (8 chars), DATE (8 chars), BALANCE, SIGN
        # But due to encoding issues, positions might be off
        
        # Try to find patterns in the line
        # Look for GLITEM pattern (starts with 1F or digits)
        glitem_match = re.search(r'^(1F\d{5}|1F\d{5}[A-Z]|\d{5,8}[A-Z]?)', clean_line)
        if glitem_match:
            glitem = glitem_match.group(1)
            rest = clean_line[len(glitem):]
        else:
            # Try to find GLITEM in the line
            # Common patterns: 1F followed by 5-6 characters, or 5-8 digits
            glitem_match = re.search(r'(1F\d{5,6}[A-Z]?|\d{5,8}[A-Z]?)', clean_line)
            if glitem_match:
                glitem = glitem_match.group(1)
                rest = clean_line.replace(glitem, '', 1)
            else:
                # Try to find any alphanumeric sequence that might be GLITEM
                glitem_match = re.search(r'([A-Z0-9]{6,10})', clean_line)
                if glitem_match:
                    glitem = glitem_match.group(1)
                    rest = clean_line.replace(glitem, '', 1)
                else:
                    continue
        
        # Look for date pattern (DD/MM/YY)
        date_match = re.search(r'(\d{2}/\d{2}/\d{2})', rest)
        if date_match:
            date = date_match.group(1)
            rest = rest.replace(date, '', 1)
        else:
            date = ''
        
        # Look for balance pattern (numbers with commas and decimals)
        balance_match = re.search(r'([\d,]+\.?\d*)', rest)
        if balance_match:
            balance_str = balance_match.group(1)
            # Clean up balance string
            balance_str = balance_str.replace(',', '')
            try:
                balance = float(balance_str)
            except ValueError:
                balance = 0.0
            # Check for negative sign
            if '-' in rest or rest.endswith('-'):
                balance = -balance
        else:
            balance = 0.0
        
        # Check for sign
        sign = ''
        if rest and rest.strip().endswith('-'):
            sign = '-'
            if balance > 0:
                balance = -balance
        
        # Only include rows with valid GLITEM (not empty and not just spaces)
        if glitem and glitem not in ['08', '†', 'ഊ']:
            # Get date from header or use yesterday
            if header_date:
                yy = header_date[0:2]
                mm = header_date[2:4]
                dd = header_date[4:6]
            else:
                yy = reptyear[2:4]
                mm = reptmon
                dd = reptday
            
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

def clean_glitem_for_matching(glitem):
    """Clean GLITEM for matching purposes"""
    if not glitem:
        return glitem
    
    # Remove common prefixes/suffixes that might be encoding artifacts
    glitem = glitem.strip()
    
    # If it starts with '1F', keep it as is
    # If it starts with just 'F', add '1' prefix for matching
    if glitem.startswith('F') and not glitem.startswith('1F'):
        glitem = '1' + glitem
    
    return glitem

def match_glitem(file_glitem, condition_glitem):
    """Try to match file GLITEM with condition GLITEM using flexible matching"""
    if not file_glitem or not condition_glitem:
        return False
    
    # Clean both for matching
    file_clean = clean_glitem_for_matching(file_glitem)
    cond_clean = clean_glitem_for_matching(condition_glitem)
    
    # Direct match
    if file_clean == cond_clean:
        return True
    
    # Check if one contains the other (for truncated values)
    if cond_clean in file_clean or file_clean in cond_clean:
        return True
    
    # Remove '1F' prefix for comparison if both start with it
    if file_clean.startswith('1F') and cond_clean.startswith('1F'):
        file_no_prefix = file_clean[2:]
        cond_no_prefix = cond_clean[2:]
        if file_no_prefix == cond_no_prefix or cond_no_prefix in file_no_prefix or file_no_prefix in cond_no_prefix:
            return True
    
    # Sometimes the condition has 'F' where file has '1F'
    if file_clean.startswith('1F') and cond_clean.startswith('F'):
        file_no_1 = file_clean[1:]  # Remove just the '1'
        cond_no_f = cond_clean[1:]   # Remove just the 'F'
        if file_no_1 == cond_no_f:
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
    for glitem in sorted(file_glitems):
        print(f"  {glitem}")
    
    # Create a mapping from file GLITEM to condition GLITEM
    mapping = {}
    for file_glitem in file_glitems:
        for cond_glitem, item_code in glitem_mappings:
            if match_glitem(file_glitem, cond_glitem):
                mapping[file_glitem] = (cond_glitem, item_code)
                print(f"Matched: '{file_glitem}' -> '{cond_glitem}' ({item_code})")
                break
    
    if not mapping:
        print("No matches found. Trying partial matches...")
        # Try partial matches
        for file_glitem in file_glitems:
            for cond_glitem, item_code in glitem_mappings:
                # Check if any part matches
                file_clean = clean_glitem_for_matching(file_glitem)
                cond_clean = clean_glitem_for_matching(cond_glitem)
                if len(file_clean) > 3 and len(cond_clean) > 3:
                    # Check if last 4-5 characters match
                    if file_clean[-5:] == cond_clean[-5:] or file_clean[-4:] == cond_clean[-4:]:
                        mapping[file_glitem] = (cond_glitem, item_code)
                        print(f"Partial matched: '{file_glitem}' -> '{cond_glitem}' ({item_code})")
                        break
    
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
    
    print("Reading GL text file with encoding detection...")
    
    # Read file with encoding detection
    lines, encoding = read_gl_text_file_with_encoding(GLFILE_TXT)
    
    if lines is None:
        print(f"ERROR: Could not read {GLFILE_TXT}")
        sys.exit(77)
    
    print(f"\nSuccessfully read {len(lines)} lines with encoding: {encoding}")
    
    # Parse the lines into structured data
    df_gl = parse_gl_lines(lines, encoding)
    
    if df_gl is None:
        print("ERROR: Could not parse data from file")
        sys.exit(77)
    
    print(f"\nParsed {df_gl.height} rows of data")
    print(f"Columns: {df_gl.columns}")
    print(f"\nData sample:")
    print(df_gl.head(10))
    
    # Show unique GLITEMs
    print(f"\nUnique GLITEMs in file ({df_gl['GLITEM'].n_unique()}):")
    for glitem in sorted(df_gl['GLITEM'].unique().to_list())[:20]:  # Show first 20
        print(f"  {glitem}")
    if df_gl['GLITEM'].n_unique() > 20:
        print(f"  ... and {df_gl['GLITEM'].n_unique() - 20} more")
    
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
