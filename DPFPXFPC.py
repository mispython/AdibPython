import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import os

# Configuration
GLFILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNLGL/glfile.sas7bdat'
STORE_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNLGL'
OUTPUT = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMNLGL/'

# Calculate reptdate as yesterday
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

# Read GL file using pyreadstat
df_gl, meta = pyreadstat.read_sas7bdat(GLFILE)

# Get date from first row
yy = int(df_gl['YY'][0])
mm = int(df_gl['MM'][0])
dd = int(df_gl['DD'][0])
gl_date = datetime(yy, mm, dd)
gl = gl_date.strftime('%d%m%y')

if gl != rdate:
    print(f"THE GLIFLE EXTRACTION IS NOT DATED {rdate}")
    sys.exit(77)

# Convert to Polars DataFrame
df_gl = pl.from_pandas(df_gl)

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
        
        # Save files
        date_str = f"{reptyear}{reptmon}{reptday}"
        for name, data in subsets.items():
            filename = f"{name}{date_str}"
            
            # Save as Parquet
            parquet_path = os.path.join(STORE_DIR, f"{filename}.parquet")
            data.write_parquet(parquet_path)
            
            # Save as SAS7BDAT using saspy
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
        print("saspy not installed. Skipping SAS output.")
    except Exception as e:
        print(f"Error saving SAS file: {e}")

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
print("Processing P1 conditions...")
results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')

print("\nProcessing P2 conditions...")
results_p2 = process_gl_data(df_gl, conditions_p2, 'P2')

print("\nProcessing complete!")
