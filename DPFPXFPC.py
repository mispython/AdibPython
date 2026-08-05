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

# Read the file manually to understand structure
print("Reading GL file...")
with open(GLFILE, 'r') as f:
    lines = f.readlines()

# First line contains the date (YYYYMMDD format)
if len(lines) > 0:
    date_str = lines[0].strip()
    print(f"Date from file header: {date_str}")
    
    # Parse date from first line (YYYYMMDD)
    if len(date_str) >= 8:
        yy = int(date_str[0:4])
        mm = int(date_str[4:6])
        dd = int(date_str[6:8])
        gl_date = datetime(yy, mm, dd)
        gl = gl_date.strftime('%d%m%y')
        
        if gl != rdate:
            print(f"THE GLIFLE EXTRACTION IS NOT DATED {rdate}")
            sys.exit(77)
        
        print(f"GL date: {gl}, Expected date: {rdate}")
    else:
        print("First line doesn't contain valid date")
        sys.exit(1)

# Process data lines (skip first line which is the date)
data_lines = lines[1:]
print(f"Processing {len(data_lines)} data lines")

# Parse fixed-width data based on the sample
# From the sample: 
# Column 1: GLITEM (e.g., "149120") - appears to be at position 0
# Column 2: Date (e.g., "31/07/26") - appears to be at position around 20
# Column 3: Balance (e.g., "150,169,113.86-") - appears to be around position 35
# SIGN is indicated by '-' at the end of balance

records = []
for line in data_lines:
    line = line.strip()
    if not line:
        continue
    
    # Parse the line - split by whitespace since fields are space-separated
    parts = line.split()
    
    if len(parts) >= 3:
        glitem = parts[0]
        date_str = parts[1]
        
        # The balance might be split if it contains commas
        # Join the remaining parts to get the full balance
        balance_str = ' '.join(parts[2:])
        
        # Remove commas and determine sign
        sign = '+'
        if balance_str.endswith('-'):
            sign = '-'
            balance_str = balance_str[:-1]  # Remove trailing minus
        elif balance_str.endswith('+'):
            balance_str = balance_str[:-1]  # Remove trailing plus
        
        # Remove commas from balance
        balance_str = balance_str.replace(',', '')
        
        try:
            balance = float(balance_str)
            records.append({
                'GLITEM': glitem,
                'DATE': date_str,
                'SIGN': sign,
                'BALANCE': balance,
                'YY': yy,
                'MM': mm,
                'DD': dd
            })
        except ValueError as e:
            print(f"Could not parse balance: {balance_str} in line: {line}")
            continue

# Create DataFrame
if records:
    df_gl = pl.DataFrame(records)
    print(f"Created DataFrame with {len(df_gl)} records")
    print(f"Columns: {df_gl.columns}")
    print(f"First few rows:")
    print(df_gl.head(3))
else:
    print("No records found in the file")
    sys.exit(1)

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
