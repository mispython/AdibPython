import polars as pl
from datetime import datetime, timedelta
import os
import sys

# Configuration
GLFILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNLGL/glfile.txt'
STORE_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNLGL'
OUTPUT = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMNLGL/'

# Calculate reptdate as yesterday
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

# Read GL file header to get date
print("Reading GL file header...")

# First, examine the file to understand its structure
try:
    with open(GLFILE, 'r') as f:
        first_line = f.readline().strip()
        print(f"First line: {first_line}")
        second_line = f.readline().strip()
        print(f"Second line: {second_line}")
        
        # Try to determine the delimiter
        if '\t' in first_line:
            delimiter = '\t'
            print("Detected tab delimiter")
        elif '|' in first_line:
            delimiter = '|'
            print("Detected pipe delimiter")
        elif ',' in first_line:
            delimiter = ','
            print("Detected comma delimiter")
        else:
            # Assume space-delimited
            delimiter = ' '
            print("Assuming space delimiter")
    
    # Try reading with the detected delimiter
    try:
        df_test = pl.read_csv(GLFILE, separator=delimiter, n_rows=5, has_header=True)
        print(f"Successfully read with delimiter: '{delimiter}'")
        print(f"Columns found: {df_test.columns}")
    except Exception as e:
        print(f"Failed with delimiter '{delimiter}': {e}")
        # Try without header
        try:
            df_test = pl.read_csv(GLFILE, separator=delimiter, n_rows=5, has_header=False)
            print("Read without header")
            print(f"First row (potential headers): {df_test.row(0)}")
            print(f"Second row: {df_test.row(1)}")
        except Exception as e2:
            print(f"Failed without header: {e2}")
            sys.exit(1)
    
except Exception as e:
    print(f"Error examining file: {e}")
    sys.exit(1)

# Look for date columns with various possible names
yy_col = None
mm_col = None
dd_col = None

print("\nSearching for date columns...")
for col in df_test.columns:
    col_upper = str(col).upper()
    if col_upper in ['YY', 'YEAR']:
        yy_col = col
        print(f"Found year column: {col}")
    elif col_upper in ['MM', 'MONTH']:
        mm_col = col
        print(f"Found month column: {col}")
    elif col_upper in ['DD', 'DAY']:
        dd_col = col
        print(f"Found day column: {col}")

# If not found by exact match, try partial match
if not all([yy_col, mm_col, dd_col]):
    print("Trying partial matches...")
    for col in df_test.columns:
        col_upper = str(col).upper()
        if not yy_col and ('YY' in col_upper or 'YEAR' in col_upper):
            yy_col = col
            print(f"Found year column (partial): {col}")
        if not mm_col and ('MM' in col_upper or 'MONTH' in col_upper):
            mm_col = col
            print(f"Found month column (partial): {col}")
        if not dd_col and ('DD' in col_upper or 'DAY' in col_upper):
            dd_col = col
            print(f"Found day column (partial): {col}")

if not all([yy_col, mm_col, dd_col]):
    print(f"Could not find all date columns. Available columns: {df_test.columns}")
    print("Please specify the column names for year, month, and day")
    sys.exit(1)

# Read the full GL file
print(f"\nReading full GL file with separator: '{delimiter}'")
df_gl = pl.read_csv(GLFILE, separator=delimiter)

# Get date from first row
yy = int(df_gl[yy_col][0])
mm = int(df_gl[mm_col][0])
dd = int(df_gl[dd_col][0])
gl_date = datetime(yy, mm, dd)
gl = gl_date.strftime('%d%m%y')

print(f"GL date from file: {gl}")
print(f"Expected date: {rdate}")

if gl != rdate:
    print(f"THE GLIFLE EXTRACTION IS NOT DATED {rdate}")
    sys.exit(77)

# Check for required columns
print("\nSearching for required columns...")
glitem_col = None
sign_col = None
balance_col = None

for col in df_gl.columns:
    col_upper = str(col).upper()
    if 'GLITEM' in col_upper or 'ITEM' in col_upper or 'GL_ITEM' in col_upper:
        glitem_col = col
        print(f"Found GL item column: {col}")
    elif 'SIGN' in col_upper:
        sign_col = col
        print(f"Found sign column: {col}")
    elif 'BALANCE' in col_upper or 'BAL' in col_upper or 'AMOUNT' in col_upper:
        balance_col = col
        print(f"Found balance column: {col}")

if not all([glitem_col, sign_col, balance_col]):
    print(f"Could not find all required columns. Available columns: {df_gl.columns}")
    print("Required: GLITEM, SIGN, BALANCE")
    sys.exit(1)

# Rename columns to expected names for consistency
df_gl = df_gl.rename({
    glitem_col: 'GLITEM',
    sign_col: 'SIGN',
    balance_col: 'BALANCE'
})

print("\nColumns after renaming:", df_gl.columns)

# Apply SIGN logic
df_gl = df_gl.with_columns([
    pl.when(pl.col('SIGN') == '-')
      .then(pl.col('BALANCE') * -1)
      .otherwise(pl.col('BALANCE'))
      .alias('BALANCE')
])

def process_gl_data(df_gl, conditions, suffix):
    """Process GL data with given conditions"""
    rows = []
    for condition, item, week, month, qtr, halfyr, year, last, total in conditions:
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
    
    glfile = pl.concat(rows) if rows else pl.DataFrame()
    
    if len(glfile) > 0:
        # Calculate BALANCE
        glfile = glfile.with_columns([
            (pl.col('WEEK').fill_null(0) + 
             pl.col('MONTH').fill_null(0) + 
             pl.col('QTR').fill_null(0) + 
             pl.col('HALFYR').fill_null(0) + 
             pl.col('YEAR').fill_null(0) + 
             pl.col('LAST').fill_null(0) + 
             pl.col('TOTAL').fill_null(0)).alias('BALANCE')
        ])
        
        # Filter and group
        glfile = glfile.filter(pl.col('ITEM').is_not_null() & (pl.col('ITEM') != ''))
        glfile = glfile.group_by('ITEM').agg([
            pl.col('WEEK').sum().alias('WEEK'),
            pl.col('MONTH').sum().alias('MONTH'),
            pl.col('QTR').sum().alias('QTR'),
            pl.col('HALFYR').sum().alias('HALFYR'),
            pl.col('YEAR').sum().alias('YEAR'),
            pl.col('LAST').sum().alias('LAST'),
            pl.col('BALANCE').sum().alias('BALANCE')
        ])
        
        # Round values
        glfile = glfile.with_columns([
            (pl.col('WEEK').round(0) / 1000).round(3).alias('WEEK'),
            (pl.col('MONTH').round(0) / 1000).round(3).alias('MONTH'),
            (pl.col('QTR').round(0) / 1000).round(3).alias('QTR'),
            (pl.col('HALFYR').round(0) / 1000).round(3).alias('HALFYR'),
            (pl.col('YEAR').round(0) / 1000).round(3).alias('YEAR'),
            (pl.col('LAST').round(0) / 1000).round(3).alias('LAST'),
            (pl.col('BALANCE').round(0) / 1000).round(3).alias('BALANCE')
        ])
        
        # Create subsets
        subsets = {
            f'GLRM{suffix}': glfile.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1')),
            f'GLFX{suffix}': glfile.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14'])),
            f'GLRMFX{suffix}': glfile.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14'])),
            f'GLUTRM{suffix}': glfile.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2')),
            f'GLUTFX{suffix}': glfile.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
        }
        
        # Save files as SAS7BDAT
        date_str = f"{reptyear}{reptmon}{reptday}"
        for name, data in subsets.items():
            filename = f"{name}{date_str}"
            sas_path = os.path.join(STORE_DIR, f"{filename}.sas7bdat")
            save_as_sas(data, sas_path, filename)
            
            # Print results
            print(f"\n{filename}:")
            print(data)
        
        return subsets
    
    return {}

def save_as_sas(df_polars, sas_path, dataset_name):
    """Save Polars DataFrame as SAS dataset using saspy"""
    try:
        import saspy
        
        # Convert Polars to Pandas
        df_pandas = df_polars.to_pandas()
        
        # Initialize SAS session
        sas = saspy.SASsession()
        
        # Upload dataframe to SAS
        sas_df = sas.df2sd(df_pandas, dataset_name)
        
        # Save as permanent SAS dataset
        sas.saslib('mylib', path=os.path.dirname(sas_path))
        sas.submit(f"""
            data mylib.{dataset_name};
                set {dataset_name};
            run;
        """)
        
        print(f"Saved SAS dataset: {sas_path}")
        
    except ImportError:
        print("saspy not installed. Saving as CSV instead...")
        # Fallback: save as CSV if saspy is not available
        csv_path = sas_path.replace('.sas7bdat', '.csv')
        df_polars.write_csv(csv_path)
        print(f"Saved CSV file: {csv_path}")
    except Exception as e:
        print(f"Error saving SAS file: {e}")
        # Try saving as CSV as fallback
        try:
            csv_path = sas_path.replace('.sas7bdat', '.csv')
            df_polars.write_csv(csv_path)
            print(f"Saved CSV file as fallback: {csv_path}")
        except:
            print("Could not save file in any format")

# Define conditions for P1 and P2
conditions_p1 = [
    (pl.col('GLITEM').is_in(['F142630C']), pl.lit('B1.12'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE')),
    (pl.col('GLITEM').is_in(['42699']), pl.lit('B1.14'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['44111', 'F147100']), pl.lit('A1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF', '42190']), pl.lit('A1.20'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F144611FXSDC', 'F147600']), pl.lit('B1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), pl.lit('A2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F143620FNFBI']), pl.lit('B2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), pl.lit('A2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['37070']), pl.lit('A2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), pl.lit('B2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F133620FNFBI']), pl.lit('B2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None))
]

conditions_p2 = [
    (pl.col('GLITEM').is_in(['F142630C']), pl.lit('B1.12'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['42699']), pl.lit('B1.14'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['44111', 'F147100']), pl.lit('A1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F147600', 'F144611FXSDC']), pl.lit('B1.18'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.col('BALANCE'), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF']), pl.lit('A1.20'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), pl.lit('A2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F143620FNFBI']), pl.lit('B2.21'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), pl.lit('A2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['37070']), pl.lit('A2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), pl.lit('B2.08'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None)),
    (pl.col('GLITEM').is_in(['F133620FNFBI']), pl.lit('B2.01'), pl.col('BALANCE'), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None), pl.lit(None))
]

# Process both sets of conditions
print("\nProcessing P1 conditions...")
results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')

print("\nProcessing P2 conditions...")
results_p2 = process_gl_data(df_gl, conditions_p2, 'P2')

print("\nProcessing complete!")
