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
    # This is common for GL files from mainframe systems
    try:
        # Read the file and try to parse based on typical GL format
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        # Define column widths based on your file format
        # Adjust these based on your actual file structure
        # Typical GL file format: YYMMDD, DATE, GLITEM, SIGN, BALANCE
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
    print(df.head())
    return df

def clean_and_rename_columns(df):
    """Clean column names and try to identify the correct columns"""
    
    # If there's only one column, try to split it
    if len(df.columns) == 1:
        col_name = df.columns[0]
        print(f"Single column detected: '{col_name[:100]}...'")
        
        # Try to split the single column into multiple columns
        # This is common when the file has fixed-width format
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
        # Rename columns based on position (assuming GL format)
        standard_cols = ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
        rename_map = {}
        for i, col in enumerate(df_gl.columns[:7]):
            rename_map[col] = standard_cols[i]
        df_gl = df_gl.rename(rename_map)
        print(f"\nRenamed columns to: {df_gl.columns}")
        
        # Convert columns to appropriate types
        try:
            df_gl = df_gl.with_columns([
                pl.col('YY').cast(pl.Int64),
                pl.col('MM').cast(pl.Int64),
                pl.col('DD').cast(pl.Int64),
                pl.col('BALANCE').cast(pl.Float64)
            ])
        except Exception as e:
            print(f"Error converting column types: {e}")
    
    # Check if required columns exist
    required_cols = ['YY', 'MM', 'DD', 'GLITEM', 'BALANCE']
    missing_cols = [col for col in required_cols if col not in df_gl.columns]
    
    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        print(f"Available columns: {df_gl.columns}")
        print("Please check the file format and adjust the parsing logic.")
        
        # Continue with what we have
        print("Attempting to continue with available columns...")
    
    # Process date
    if 'YY' in df_gl.columns and 'MM' in df_gl.columns and 'DD' in df_gl.columns:
        try:
            yy = int(df_gl['YY'][0])
            mm = int(df_gl['MM'][0])
            dd = int(df_gl['DD'][0])
            gl_date = datetime(2000 + yy, mm, dd) if yy < 100 else datetime(yy, mm, dd)
            gl = gl_date.strftime('%d%m%y')
            print(f"GL date from file: {gl}")
        except Exception as e:
            print(f"Error parsing date from file: {e}")
            gl = rdate
    else:
        print("Using yesterday's date for GL date")
        gl = rdate
    
    # Process BALANCE column
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
        print("SIGN or BALANCE columns not found. Continuing without sign adjustment.")
    
    # Your existing processing functions here...
    # (I'll show the full code in the next response)
