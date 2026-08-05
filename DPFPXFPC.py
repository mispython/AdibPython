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

print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
print(f"Report date (yesterday): {reptdate.strftime('%Y-%m-%d')}")
print(f"Expected date format (DDMMYY): {rdate}")

# Read the file manually to understand structure
print("\nReading GL file...")
with open(GLFILE, 'r') as f:
    lines = f.readlines()

# First line contains the date (YYYYMMDD format)
if len(lines) > 0:
    date_str = lines[0].strip()
    
    # Parse date from first line (YYYYMMDD)
    if len(date_str) >= 8:
        try:
            yy = int(date_str[0:4])
            mm = int(date_str[4:6])
            dd = int(date_str[6:8])
            gl_date = datetime(yy, mm, dd)
            gl = gl_date.strftime('%d%m%y')
            
            print(f"File date: {gl_date.strftime('%Y-%m-%d')} (DDMMYY: {gl})")
            print(f"Expected date (DDMMYY): {rdate}")
            
            # Check if the file date matches expected date
            if gl != rdate:
                print(f"WARNING: GL file extraction date ({gl}) does not match expected date ({rdate})")
                print(f"Using file date for processing...")
                reptdate = gl_date
                reptyear = reptdate.strftime('%Y')
                reptmon = reptdate.strftime('%m')
                reptday = reptdate.strftime('%d')
                rdate = reptdate.strftime('%d%m%y')
                
        except ValueError as e:
            print(f"Error parsing date: {e}")
            sys.exit(1)
    else:
        print("First line doesn't contain valid date")
        sys.exit(1)

# Process data lines
data_lines = lines[1:]
print(f"\nProcessing {len(data_lines)} data lines")

# Parse the data
records = []
for line_num, line in enumerate(data_lines, 1):
    line = line.strip()
    if not line:
        continue
    
    parts = line.split()
    
    if len(parts) >= 3:
        glitem = parts[0]
        date_str = parts[1]
        
        # Clean GLITEM - remove leading '1' if it exists
        if glitem.startswith('1') and len(glitem) > 1 and glitem[1].isalpha():
            glitem = glitem[1:]
        
        # Join the remaining parts to get the full balance
        balance_str = ''.join(parts[2:])
        
        # Determine sign
        sign = '+'
        if balance_str.endswith('-'):
            sign = '-'
            balance_str = balance_str[:-1]
        elif balance_str.endswith('+'):
            balance_str = balance_str[:-1]
        
        # Remove commas
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
            print(f"Line {line_num}: Could not parse balance '{balance_str}'")
            continue
    elif len(parts) == 1 and parts[0] == '-':
        continue
    else:
        print(f"Line {line_num}: Not enough parts ({len(parts)}): {line[:100]}")

# Create DataFrame
if records:
    df_gl = pl.DataFrame(records)
    print(f"\nCreated DataFrame with {len(df_gl)} records")
    print(f"Sample GLITEMs: {df_gl['GLITEM'].unique().to_list()[:10]}")
    print(f"\nFirst few rows:")
    print(df_gl.head(5))
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
    for condition, item_name, week_col, month_col, qtr_col, halfyr_col, year_col, last_col, total_col in conditions:
        filtered = df_gl.filter(condition)
        if len(filtered) > 0:
            # Start with ITEM column
            result = filtered.select([
                pl.lit(item_name).alias('ITEM')
            ])
            
            # Add WEEK column
            if week_col is not None:
                # Keep the original BALANCE column and add WEEK
                result = filtered.select([
                    pl.lit(item_name).alias('ITEM'),
                    week_col.alias('WEEK')
                ])
            else:
                result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('WEEK'))
            
            # Add MONTH column
            if month_col is not None:
                # Need to include BALANCE if it's used as MONTH
                temp = filtered.select([
                    pl.lit(item_name).alias('ITEM'),
                    month_col.alias('MONTH')
                ])
                if 'WEEK' in result.columns:
                    result = result.join(temp, on='ITEM', how='left')
                else:
                    result = temp
            else:
                if 'MONTH' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('MONTH'))
            
            # Add QTR column
            if qtr_col is not None:
                if 'QTR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('QTR'))
            else:
                if 'QTR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('QTR'))
            
            # Add HALFYR column
            if halfyr_col is not None:
                if 'HALFYR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('HALFYR'))
            else:
                if 'HALFYR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('HALFYR'))
            
            # Add YEAR column
            if year_col is not None:
                if 'YEAR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('YEAR'))
            else:
                if 'YEAR' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('YEAR'))
            
            # Add LAST column
            if last_col is not None:
                if 'LAST' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('LAST'))
            else:
                if 'LAST' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('LAST'))
            
            # Add TOTAL column
            if total_col is not None:
                if 'TOTAL' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('TOTAL'))
            else:
                if 'TOTAL' not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias('TOTAL'))
            
            # Ensure all required columns exist
            for col_name in ['WEEK', 'MONTH', 'QTR', 'HALFYR', 'YEAR', 'LAST', 'TOTAL']:
                if col_name not in result.columns:
                    result = result.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col_name))
            
            rows.append(result.select(['ITEM', 'WEEK', 'MONTH', 'QTR', 'HALFYR', 'YEAR', 'LAST', 'TOTAL']))
    
    if rows:
        # Concatenate all rows
        glfile = pl.concat(rows, how='vertical')
        
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
        
        # Save files
        date_str = f"{reptyear}{reptmon}{reptday}"
        for name, data in subsets.items():
            if len(data) > 0:
                filename = f"{name}{date_str}"
                sas_path = os.path.join(STORE_DIR, f"{filename}.sas7bdat")
                save_as_sas(data, sas_path, filename)
                
                # Save as Parquet backup
                parquet_path = os.path.join(STORE_DIR, f"{filename}.parquet")
                try:
                    data.write_parquet(parquet_path)
                    print(f"Saved Parquet: {parquet_path}")
                except:
                    pass
                
                print(f"\n{filename}:")
                print(data)
            else:
                print(f"\n{name}{date_str}: No data")
        
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
        csv_path = sas_path.replace('.sas7bdat', '.csv')
        df_polars.write_csv(csv_path)
        print(f"Saved CSV file: {csv_path}")
    except Exception as e:
        print(f"Error saving SAS file: {e}")
        try:
            csv_path = sas_path.replace('.sas7bdat', '.csv')
            df_polars.write_csv(csv_path)
            print(f"Saved CSV file as fallback: {csv_path}")
        except:
            print("Could not save file in any format")

# Define conditions for P1 and P2
conditions_p1 = [
    (pl.col('GLITEM').is_in(['F142630C']), 'B1.12', None, None, None, None, None, None, pl.col('BALANCE')),
    (pl.col('GLITEM').is_in(['42699']), 'B1.14', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['44111', 'F147100']), 'A1.18', None, None, None, None, None, pl.col('BALANCE'), None),
    (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF', '42190']), 'A1.20', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F144611FXSDC', 'F147600']), 'B1.18', None, None, None, None, None, pl.col('BALANCE'), None),
    (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), 'A2.21', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F143620FNFBI']), 'B2.21', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), 'A2.01', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['37070']), 'A2.08', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), 'B2.08', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F133620FNFBI']), 'B2.01', pl.col('BALANCE'), None, None, None, None, None, None)
]

conditions_p2 = [
    (pl.col('GLITEM').is_in(['F142630C']), 'B1.12', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['42699']), 'B1.14', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['44111', 'F147100']), 'A1.18', None, None, None, None, None, pl.col('BALANCE'), None),
    (pl.col('GLITEM').is_in(['F147600', 'F144611FXSDC']), 'B1.18', None, None, None, None, None, pl.col('BALANCE'), None),
    (pl.col('GLITEM').is_in(['F249299K', '49120', '42199', '49120NLF']), 'A1.20', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F143110VCB', 'F143110VFBI', 'F143120ODNVB', 'F143120ODNIB']), 'A2.21', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F143620FNFBI']), 'B2.21', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F133110ODVIB', 'F13312002CB', 'F132121BBNM']), 'A2.01', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['37070']), 'A2.08', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F137610FXSH', 'F137650FXCDS']), 'B2.08', pl.col('BALANCE'), None, None, None, None, None, None),
    (pl.col('GLITEM').is_in(['F133620FNFBI']), 'B2.01', pl.col('BALANCE'), None, None, None, None, None, None)
]

# Process both sets of conditions
print("\nProcessing P1 conditions...")
results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')

print("\nProcessing P2 conditions...")
results_p2 = process_gl_data(df_gl, conditions_p2, 'P2')

print("\nProcessing complete!")
