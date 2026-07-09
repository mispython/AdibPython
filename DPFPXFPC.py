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
        first_line = f.readline().strip()
        print(f"First line of file: {first_line[:200]}...")
        print(f"Length of first line: {len(first_line)}")
        
        # Try to detect the delimiter
        if '\t' in first_line:
            delimiter = '\t'
            print("Detected tab delimiter")
        elif '|' in first_line:
            delimiter = '|'
            print("Detected pipe delimiter")
        elif ';' in first_line:
            delimiter = ';'
            print("Detected semicolon delimiter")
        elif ',' in first_line:
            delimiter = ','
            print("Detected comma delimiter")
        else:
            # Likely fixed-width format
            delimiter = None
            print("Could not detect delimiter, trying fixed-width format")
        
        # Check if there's a header row
        f.seek(0)
        lines = f.readlines()
        
        # Try to determine if first line is header
        is_header = False
        if lines and any(word.isalpha() for word in lines[0].split()):
            is_header = True
            print("Detected header row")
    
    # Try reading with detected delimiter
    if delimiter:
        try:
            df = pl.read_csv(
                filepath,
                separator=delimiter,
                infer_schema=True,
                ignore_errors=True,
                has_header=is_header
            )
            if len(df.columns) > 1:
                return df, delimiter
        except Exception as e:
            print(f"Error reading with delimiter '{delimiter}': {e}")
    
    # Try reading as fixed-width format
    try:
        # Read the file and try to parse based on typical GL format
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        # Define column widths based on your file format
        # Adjust these based on your actual file structure
        col_widths = [2, 2, 2, 8, 20, 1, 15]  # Adjust as needed
        col_names = ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
        
        data = []
        for line in lines:
            if line.strip():
                row = []
                start = 0
                for width in col_widths:
                    if start < len(line):
                        row.append(line[start:start+width].strip())
                        start += width
                    else:
                        row.append('')
                if any(row):  # Skip empty rows
                    data.append(row)
        
        if data:
            df = pl.DataFrame(data, schema=col_names)
            return df, 'fixed-width'
            
    except Exception as e:
        print(f"Error parsing as fixed-width: {e}")
    
    # Try reading as space-delimited (multiple spaces)
    try:
        df = pl.read_csv(
            filepath,
            separator=' ',
            infer_schema=True,
            ignore_errors=True,
            has_header=is_header
        )
        if len(df.columns) > 1:
            return df, 'space'
    except Exception as e:
        print(f"Error reading as space-delimited: {e}")
    
    # If all else fails, try to parse each line manually
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        # Remove empty lines
        lines = [line.strip() for line in lines if line.strip()]
        
        # Try to parse each line
        data = []
        for line in lines:
            # Try to split on whitespace
            parts = line.split()
            if len(parts) >= 3:
                data.append(parts)
        
        if data:
            # Determine number of columns from first non-empty row
            cols = len(data[0])
            col_names = [f'col{i}' for i in range(cols)]
            df = pl.DataFrame(data, schema=col_names)
            return df, 'whitespace'
    except Exception as e:
        print(f"Error parsing manually: {e}")
    
    return None, None

def inspect_dataframe(df):
    """Inspect the dataframe to understand its structure"""
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print(f"First few rows:")
    print(df.head(5))
    return df

def clean_and_rename_columns(df):
    """Clean column names and try to identify the correct columns"""
    
    # If there's only one column, try to split it
    if len(df.columns) == 1:
        col_name = df.columns[0]
        print(f"Single column detected: '{col_name[:100]}...'")
        
        # Try to split the single column into multiple columns
        try:
            # Extract data from the single column
            data = df[col_name].to_list()
            
            # Try different parsing strategies
            for row in data[:5]:
                print(f"Sample row: '{row}'")
            
            # Attempt to parse based on patterns
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
        except Exception as e:
            print(f"Error splitting single column: {e}")
    
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
            glrmp.write_parquet(f'{STORE_DIR}GLRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
            glfxp.write_parquet(f'{STORE_DIR}GLFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            glrmfxp.write_parquet(f'{STORE_DIR}GLRMFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            glutrmp.write_parquet(f'{STORE_DIR}GLUTRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
            glutfxp.write_parquet(f'{STORE_DIR}GLUTFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
            print(f"Parquet files saved for {suffix}")
        except Exception as e:
            print(f"Error saving parquet files: {e}")
        
        # Save SAS datasets
        try:
            save_to_sas(glrmp, f'GLRM{suffix}{reptyear}{reptmon}{reptday}')
            save_to_sas(glfxp, f'GLFX{suffix}{reptyear}{reptmon}{reptday}')
            save_to_sas(glrmfxp, f'GLRMFX{suffix}{reptyear}{reptmon}{reptday}')
            save_to_sas(glutrmp, f'GLUTRM{suffix}{reptyear}{reptmon}{reptday}')
            save_to_sas(glutfxp, f'GLUTFX{suffix}{reptyear}{reptmon}{reptday}')
        except Exception as e:
            print(f"Error saving SAS datasets: {e}")
        
        # Print results
        print(f"\nGLRM{suffix}{reptyear}{reptmon}{reptday}:")
        print(glrmp)
        print(f"\nGLFX{suffix}{reptyear}{reptmon}{reptday}:")
        print(glfxp)
        print(f"\nGLRMFX{suffix}{reptyear}{reptmon}{reptday}:")
        print(glrmfxp)
        print(f"\nGLUTRM{suffix}{reptyear}{reptmon}{reptday}:")
        print(glutrmp)
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
    
    # Try to identify columns if they don't have standard names
    if len(df_gl.columns) >= 7:
        # Try to detect which columns are which based on content
        print("\nAttempting to identify columns...")
        
        # Check if columns contain certain patterns
        for col in df_gl.columns:
            sample_values = df_gl[col].head(10).to_list()
            sample_str = ' '.join([str(v) for v in sample_values if v])
            
            # Check for numeric patterns that might be GLITEM
            if any(re.search(r'^F?\d{3,}', str(v)) for v in sample_values if v):
                print(f"Column '{col}' might be GLITEM")
                if 'GLITEM' not in df_gl.columns:
                    df_gl = df_gl.rename({col: 'GLITEM'})
            
            # Check for balance patterns (numbers with decimals)
            if any(re.search(r'^-?\d+\.?\d*$', str(v)) for v in sample_values if v):
                print(f"Column '{col}' might be BALANCE")
                if 'BALANCE' not in df_gl.columns:
                    df_gl = df_gl.rename({col: 'BALANCE'})
            
            # Check for sign patterns
            if any(v in ['-', '+', 'C', 'D'] for v in sample_values if v):
                print(f"Column '{col}' might be SIGN")
                if 'SIGN' not in df_gl.columns:
                    df_gl = df_gl.rename({col: 'SIGN'})
            
            # Check for date patterns
            if any(re.search(r'\d{2}/\d{2}/\d{2,4}', str(v)) for v in sample_values if v):
                print(f"Column '{col}' might be DATE")
                if 'DATE' not in df_gl.columns:
                    df_gl = df_gl.rename({col: 'DATE'})
        
        # If we still don't have standard columns, rename by position
        standard_cols = ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
        current_cols = df_gl.columns
        
        if len(current_cols) >= len(standard_cols):
            rename_map = {}
            for i, col in enumerate(current_cols[:len(standard_cols)]):
                if col not in standard_cols:  # Only rename if not already named
                    rename_map[col] = standard_cols[i]
            df_gl = df_gl.rename(rename_map)
            print(f"\nRenamed columns to: {df_gl.columns}")
    
    # Ensure GLITEM column exists
    if 'GLITEM' not in df_gl.columns:
        print("WARNING: GLITEM column not found. Attempting to identify it...")
        # Try to find column with most string values
        for col in df_gl.columns:
            if df_gl[col].dtype == pl.Utf8:
                df_gl = df_gl.rename({col: 'GLITEM'})
                print(f"Using column '{col}' as GLITEM")
                break
    
    # Convert columns to appropriate types
    try:
        if 'YY' in df_gl.columns:
            df_gl = df_gl.with_columns(pl.col('YY').cast(pl.Int64, strict=False))
        if 'MM' in df_gl.columns:
            df_gl = df_gl.with_columns(pl.col('MM').cast(pl.Int64, strict=False))
        if 'DD' in df_gl.columns:
            df_gl = df_gl.with_columns(pl.col('DD').cast(pl.Int64, strict=False))
        if 'BALANCE' in df_gl.columns:
            df_gl = df_gl.with_columns(pl.col('BALANCE').cast(pl.Float64, strict=False))
    except Exception as e:
        print(f"Error converting column types: {e}")
    
    # Check if required columns exist
    required_cols = ['YY', 'MM', 'DD', 'GLITEM', 'BALANCE']
    missing_cols = [col for col in required_cols if col not in df_gl.columns]
    
    if missing_cols:
        print(f"WARNING: Missing required columns: {missing_cols}")
        print(f"Available columns: {df_gl.columns}")
        print("Attempting to continue with available columns...")
    
    # Process date
    if 'YY' in df_gl.columns and 'MM' in df_gl.columns and 'DD' in df_gl.columns:
        try:
            first_row = df_gl.head(1)
            yy = int(first_row['YY'][0])
            mm = int(first_row['MM'][0])
            dd = int(first_row['DD'][0])
            gl_date = datetime(2000 + yy, mm, dd) if yy < 100 else datetime(yy, mm, dd)
            gl = gl_date.strftime('%d%m%y')
            print(f"GL date from file: {gl}")
        except Exception as e:
            print(f"Error parsing date from file: {e}")
            gl = rdate
            print(f"Using date: {gl}")
    else:
        print("Using date from filename or yesterday's date")
        gl = rdate
        print(f"Using date: {gl}")
    
    # Process BALANCE column with sign adjustment
    if 'BALANCE' in df_gl.columns and 'SIGN' in df_gl.columns:
        try:
            df_gl = df_gl.with_columns([
                pl.when(pl.col('SIGN') == '-')
                  .then(pl.col('BALANCE') * -1)
                  .otherwise(pl.col('BALANCE'))
                  .alias('BALANCE')
            ])
            print("Applied sign adjustment to BALANCE")
        except Exception as e:
            print(f"Error applying sign adjustment: {e}")
    else:
        print("SIGN column not found. Continuing without sign adjustment.")
    
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
