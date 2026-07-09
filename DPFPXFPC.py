import polars as pl
import duckdb
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

def read_gl_text_file_advanced(filepath):
    """Read GL text file with advanced parsing capabilities"""
    
    # First, try to read the file as raw text to inspect it
    with open(filepath, 'r') as f:
        first_line = f.readline().rstrip('\n')
        print(f"First line of file: '{first_line}'")
        print(f"Length of first line: {len(first_line)}")
        
        # Get all lines to analyze
        f.seek(0)
        lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
        print(f"Total lines: {len(lines)}")
        
        # Show first few lines for analysis
        print("\nFirst 5 lines of data:")
        for i, line in enumerate(lines[:5]):
            print(f"Line {i+1}: '{line}'")
            print(f"  Length: {len(line)}")
            # Show character positions
            if len(line) > 0:
                chars = list(line)
                positions = []
                for j, c in enumerate(chars):
                    if j % 10 == 0:
                        positions.append(f"{j:>3}")
                print(f"  Positions: {' '.join(positions)}")
                print(f"  Characters: {' '.join(chars[:50])}...")
            print()
    
    # Based on the data, let's try different fixed-width formats
    # The data appears to have fields like: YY, MM, DD, DATE, GLITEM, SIGN, BALANCE
    # But they seem to be concatenated
    
    # Try to parse as fixed-width with different column widths
    col_width_options = [
        # Format 1: Based on observed data
        {'widths': [2, 2, 2, 8, 15, 1, 10], 'names': ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']},
        # Format 2: Alternative widths
        {'widths': [2, 2, 2, 10, 20, 1, 15], 'names': ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']},
        # Format 3: With date first
        {'widths': [8, 2, 2, 2, 15, 1, 10], 'names': ['DATE', 'YY', 'MM', 'DD', 'GLITEM', 'SIGN', 'BALANCE']},
        # Format 4: No date field
        {'widths': [2, 2, 2, 20, 1, 15], 'names': ['YY', 'MM', 'DD', 'GLITEM', 'SIGN', 'BALANCE']},
    ]
    
    for option in col_width_options:
        try:
            data = []
            for line in lines:
                row = []
                start = 0
                for width in option['widths']:
                    if start < len(line):
                        row.append(line[start:start+width].strip())
                        start += width
                    else:
                        row.append('')
                if any(row):  # Skip empty rows
                    data.append(row)
            
            if data and len(data[0]) == len(option['names']):
                df = pl.DataFrame(data, schema=option['names'])
                # Check if this looks like valid data
                if df.height > 0:
                    # Check if first row has valid-looking GLITEM
                    sample_glitem = df['GLITEM'][0] if 'GLITEM' in df.columns else ''
                    if sample_glitem and not sample_glitem.startswith('20'):
                        print(f"Successfully parsed with widths: {option['widths']}")
                        print(f"First row: {df.row(0)}")
                        return df, f'fixed-width-{option["widths"]}'
        except Exception as e:
            continue
    
    # If none of the fixed-width options work, try regex parsing
    try:
        data = []
        pattern = r'(\d{2})(\d{2})(\d{2})(\d{2}/\d{2}/\d{2})?(\w+)?([+-]?)([\d,]+\.?\d*)?'
        
        for line in lines:
            matches = re.search(pattern, line)
            if matches:
                row = list(matches.groups())
                # Clean up empty values
                row = ['' if v is None else v for v in row]
                data.append(row)
        
        if data:
            col_names = ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
            # Ensure we have the right number of columns
            max_cols = max(len(row) for row in data)
            if max_cols <= len(col_names):
                # Pad rows with empty strings
                data = [row + [''] * (len(col_names) - len(row)) for row in data]
                df = pl.DataFrame(data, schema=col_names)
                return df, 'regex'
    except Exception as e:
        print(f"Regex parsing failed: {e}")
    
    return None, None

def inspect_dataframe(df):
    """Inspect the dataframe to understand its structure"""
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print(f"Data types: {df.dtypes}")
    print(f"\nFirst few rows:")
    print(df.head(10))
    
    # Show some statistics
    print("\nColumn statistics:")
    for col in df.columns:
        non_null = df[col].null_count()
        unique = df[col].n_unique()
        print(f"  {col}: {df.height - non_null} non-null values, {unique} unique")
        if unique > 0 and unique <= 10:
            print(f"    Values: {df[col].unique().to_list()}")
    
    return df

def clean_and_rename_columns(df):
    """Clean column names and try to identify the correct columns"""
    
    # If there's only one column, try to split it
    if len(df.columns) == 1:
        col_name = df.columns[0]
        print(f"Single column detected: '{col_name[:100]}...'")
        
        # Extract data from the single column
        data = df[col_name].to_list()
        
        # Try to parse each row
        parsed_data = []
        for row in data:
            if row:
                # Try splitting on whitespace
                parts = row.split()
                if len(parts) >= 3:
                    parsed_data.append(parts)
        
        if parsed_data:
            # Determine columns from parsed data
            cols = len(parsed_data[0])
            col_names = [f'col{i}' for i in range(cols)]
            df_new = pl.DataFrame(parsed_data, schema=col_names)
            print(f"Created {cols} columns from single column")
            return df_new
    
    return df

def process_gl_data(df_gl, suffix):
    """Process GL data for a given suffix (P1 or P2)"""
    
    # Define conditions for P1 or P2
    if suffix == 'P1':
        conditions = [
            (pl.col('GLITEM').is_in(['F142630C']), pl.lit('B1.12'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['42699']), pl.lit('B1.14'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['44111', 'F147100']), pl.lit('A1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF', '42190']), pl.lit('A1.20'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F144611FXSDC', 'F147600']), pl.lit('B1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), pl.lit('A2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F143620FNFBI']), pl.lit('B2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), pl.lit('A2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['37070']), pl.lit('A2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), pl.lit('B2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F133620FNFBI']), pl.lit('B2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'))
        ]
    else:  # P2
        conditions = [
            (pl.col('GLITEM').is_in(['F142630C']), pl.lit('B1.12'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['42699']), pl.lit('B1.14'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['44111', 'F147100']), pl.lit('A1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F147600', 'F144611FXSDC']), pl.lit('B1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF']), pl.lit('A1.20'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), pl.lit('A2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F143620FNFBI']), pl.lit('B2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), pl.lit('A2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['37070']), pl.lit('A2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), pl.lit('B2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
            (pl.col('GLITEM').is_in(['F133620FNFBI']), pl.lit('B2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'))
        ]
    
    rows = []
    for condition, item, week, month, qtr, halfyr, year, last, total in conditions:
        try:
            filtered = df_gl.filter(condition)
            if len(filtered) > 0:
                rows.append(
                    filtered.select([
                        pl.lit(item).alias('ITEM'),
                        week.alias('WEEK') if week is not None else pl.lit(None).alias('WEEK'),
                        month.alias('MONTH') if month is not None else pl.lit(None).alias('MONTH'),
                        qtr.alias('QTR') if qtr is not None else pl.lit(None).alias('QTR'),
                        halfyr.alias('HALFYR') if halfyr is not None else pl.lit(None).alias('HALFYR'),
                        year.alias('YEAR') if year is not None else pl.lit(None).alias('YEAR'),
                        last.alias('LAST') if last is not None else pl.lit(None).alias('LAST'),
                        total.alias('TOTAL') if total is not None else pl.lit(None).alias('TOTAL')
                    ])
                )
        except Exception as e:
            print(f"Error processing condition: {e}")
            continue
    
    glfilep = pl.concat(rows) if rows else pl.DataFrame()
    
    if len(glfilep) > 0:
        glfilep = glfilep.with_columns([
            (pl.col('WEEK').fill_null(0) + 
             pl.col('MONTH').fill_null(0) + 
             pl.col('QTR').fill_null(0) + 
             pl.col('HALFYR').fill_null(0) + 
             pl.col('YEAR').fill_null(0) + 
             pl.col('LAST').fill_null(0) + 
             pl.col('TOTAL').fill_null(0)).alias('BALANCE')
        ])
        
        glfilep = glfilep.filter(pl.col('ITEM').is_not_null() & (pl.col('ITEM') != ''))
        
        glfilep = glfilep.group_by('ITEM').agg([
            pl.col('WEEK').sum().alias('WEEK'),
            pl.col('MONTH').sum().alias('MONTH'),
            pl.col('QTR').sum().alias('QTR'),
            pl.col('HALFYR').sum().alias('HALFYR'),
            pl.col('YEAR').sum().alias('YEAR'),
            pl.col('LAST').sum().alias('LAST'),
            pl.col('BALANCE').sum().alias('BALANCE')
        ])
        
        glfilep = glfilep.with_columns([
            (pl.col('WEEK').round(0) / 1000).round(3).alias('WEEK'),
            (pl.col('MONTH').round(0) / 1000).round(3).alias('MONTH'),
            (pl.col('QTR').round(0) / 1000).round(3).alias('QTR'),
            (pl.col('HALFYR').round(0) / 1000).round(3).alias('HALFYR'),
            (pl.col('YEAR').round(0) / 1000).round(3).alias('YEAR'),
            (pl.col('LAST').round(0) / 1000).round(3).alias('LAST'),
            (pl.col('BALANCE').round(0) / 1000).round(3).alias('BALANCE')
        ])
        
        # Split into categories
        try:
            glrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
            glfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
            glrmfxp = glfilep.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
            glutrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
            glutfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
        except Exception as e:
            print(f"Error filtering categories: {e}")
            glrmp = glfxp = glrmfxp = glutrmp = glutfxp = pl.DataFrame()
        
        # Save parquet files
        os.makedirs(STORE_DIR, exist_ok=True)
        
        try:
            if len(glrmp) > 0:
                glrmp.write_parquet(f'{STORE_DIR}GLRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
            if len(glfxp) > 0:
                glfxp.write_parquet(f'{STORE_DIR}GLFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            if len(glrmfxp) > 0:
                glrmfxp.write_parquet(f'{STORE_DIR}GLRMFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            if len(glutrmp) > 0:
                glutrmp.write_parquet(f'{STORE_DIR}GLUTRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
            if len(glutfxp) > 0:
                glutfxp.write_parquet(f'{STORE_DIR}GLUTFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            print(f"Parquet files saved for {suffix}")
        except Exception as e:
            print(f"Error saving parquet files: {e}")
        
        # Save SAS datasets
        try:
            if len(glrmp) > 0:
                save_to_sas(glrmp, f'GLRM{suffix}{reptyear}{reptmon}{reptday}')
            if len(glfxp) > 0:
                save_to_sas(glfxp, f'GLFX{suffix}{reptyear}{reptmon}{reptday}')
            if len(glrmfxp) > 0:
                save_to_sas(glrmfxp, f'GLRMFX{suffix}{reptyear}{reptmon}{reptday}')
            if len(glutrmp) > 0:
                save_to_sas(glutrmp, f'GLUTRM{suffix}{reptyear}{reptmon}{reptday}')
            if len(glutfxp) > 0:
                save_to_sas(glutfxp, f'GLUTFX{suffix}{reptyear}{reptmon}{reptday}')
        except Exception as e:
            print(f"Error saving SAS datasets: {e}")
        
        # Print results
        if len(glrmp) > 0:
            print(f"\nGLRM{suffix}{reptyear}{reptmon}{reptday}:")
            print(glrmp)
        if len(glfxp) > 0:
            print(f"\nGLFX{suffix}{reptyear}{reptmon}{reptday}:")
            print(glfxp)
        if len(glrmfxp) > 0:
            print(f"\nGLRMFX{suffix}{reptyear}{reptmon}{reptday}:")
            print(glrmfxp)
        if len(glutrmp) > 0:
            print(f"\nGLUTRM{suffix}{reptyear}{reptmon}{reptday}:")
            print(glutrmp)
        if len(glutfxp) > 0:
            print(f"\nGLUTFX{suffix}{reptyear}{reptmon}{reptday}:")
            print(glutfxp)
        
        return glrmp, glfxp, glrmfxp, glutrmp, glutfxp
    
    return None, None, None, None, None

def save_to_sas(df, dataset_name):
    """Save a Polars DataFrame to SAS dataset using saspy"""
    if len(df) == 0:
        print(f"Empty DataFrame, skipping SAS save for {dataset_name}")
        return
    
    try:
        import saspy
        # Create SAS session
        sas = saspy.SASsession()
        
        # Convert Polars DataFrame to pandas for saspy compatibility
        df_pandas = df.to_pandas()
        
        # Create SAS dataset
        sas.sasdata(df_pandas, table=dataset_name, libref='WORK')
        
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
    # Ensure store directory exists
    os.makedirs(STORE_DIR, exist_ok=True)
    
    print("Reading GL text file...")
    
    # Try to read the text file
    df_gl, format_type = read_gl_text_file_advanced(GLFILE_TXT)
    
    if df_gl is None:
        print(f"ERROR: Could not read {GLFILE_TXT}")
        sys.exit(77)
    
    print(f"Successfully read file using format: {format_type}")
    df_gl = inspect_dataframe(df_gl)
    
    # Clean and rename columns if needed
    if len(df_gl.columns) == 1:
        df_gl = clean_and_rename_columns(df_gl)
        print("\nAfter cleaning:")
        inspect_dataframe(df_gl)
    
    # Try to manually fix the data based on what we see
    # Looking at the data, it seems like:
    # Row 1: "20", "26", "07", "08", "", "", "" -> This is likely the header/date
    # Row 2: "1F", "14", "76", "00", "08/07/26", "", "" -> GLITEM seems to be "1F147600"
    # Row 3: "1F", "14", "26", "30C", "08/07/26", "", "" -> GLITEM seems to be "1F142630C"
    # Row 4: "14", "26", "99", "", "08/07/26", "", "224" -> GLITEM seems to be "42699", BALANCE=224
    # Row 5: "14", "41", "11", "", "08/07/26", "", "4,997" -> GLITEM seems to be "44111", BALANCE=4,997
    
    # Let's try to reconstruct the data manually
    if df_gl.height > 0:
        # Check if we have misaligned data
        if 'GLITEM' in df_gl.columns:
            # Clean up GLITEM - it seems to be split across columns
            print("\nAttempting to reconstruct GLITEM from data...")
            
            # Create a new column combining YY, MM, DD if they look like GLITEM parts
            # This is a heuristic based on the observed data
            if 'YY' in df_gl.columns and 'MM' in df_gl.columns:
                # Check if YY+MM+DD looks like a GLITEM pattern
                df_gl = df_gl.with_columns([
                    pl.when(
                        pl.col('YY').str.contains(r'^[A-Z0-9]') & 
                        pl.col('MM').str.contains(r'^[A-Z0-9]') &
                        pl.col('DD').str.contains(r'^[A-Z0-9]')
                    ).then(
                        pl.col('YY') + pl.col('MM') + pl.col('DD')
                    ).otherwise(pl.col('GLITEM')).alias('GLITEM_FIXED')
                ])
                
                # Update GLITEM
                df_gl = df_gl.with_columns([
                    pl.when(
                        pl.col('GLITEM_FIXED').is_not_null() & 
                        (pl.col('GLITEM_FIXED') != '')
                    ).then(pl.col('GLITEM_FIXED'))
                    .otherwise(pl.col('GLITEM'))
                    .alias('GLITEM')
                ])
                
                # Remove the temporary column
                df_gl = df_gl.drop('GLITEM_FIXED')
    
    # Convert BALANCE to numeric
    if 'BALANCE' in df_gl.columns:
        try:
            # Remove commas and convert to float
            df_gl = df_gl.with_columns([
                pl.col('BALANCE').str.replace_all(',', '').cast(pl.Float64, strict=False).alias('BALANCE')
            ])
            print("Converted BALANCE to numeric")
        except Exception as e:
            print(f"Error converting BALANCE: {e}")
    
    # Check if required columns exist
    required_cols = ['GLITEM', 'BALANCE']
    missing_cols = [col for col in required_cols if col not in df_gl.columns]
    
    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        print(f"Available columns: {df_gl.columns}")
        sys.exit(77)
    
    # Filter out empty GLITEM rows
    df_gl = df_gl.filter(pl.col('GLITEM').is_not_null() & (pl.col('GLITEM') != ''))
    
    print("\nCleaned data:")
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
