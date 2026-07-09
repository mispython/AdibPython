import polars as pl
import duckdb
from datetime import datetime, timedelta
import sys
import os

# Constants
GLFILE_TXT = 'data/glfile.txt'  # Changed to txt file
STORE_DIR = 'data/store/'

# Use yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

# Read GL file directly from text
def read_gl_text_file(filepath):
    """Read GL text file with appropriate format"""
    try:
        # Try reading with tab separator (common for GL files)
        df = pl.read_csv(
            filepath, 
            separator='\t', 
            infer_schema=True,
            ignore_errors=True
        )
        return df
    except:
        # If tab doesn't work, try comma or space
        try:
            df = pl.read_csv(
                filepath, 
                separator=',', 
                infer_schema=True,
                ignore_errors=True
            )
            return df
        except:
            # Try reading as fixed-width or space-separated
            df = pl.read_csv(
                filepath, 
                separator=' ', 
                infer_schema=True,
                ignore_errors=True
            )
            return df

# Read the text file
df_gl = read_gl_text_file(GLFILE_TXT)

if df_gl is None:
    print(f"ERROR: Could not read {GLFILE_TXT}")
    sys.exit(77)

# Check if the data has the expected columns
expected_columns = ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
missing_cols = [col for col in expected_columns if col not in df_gl.columns]

if missing_cols:
    print(f"WARNING: Missing columns: {missing_cols}")
    print(f"Available columns: {df_gl.columns}")
    print("Attempting to continue with available columns...")

# Get GL date from the first row
df_gl_header = df_gl.head(1)
yy = int(df_gl_header['YY'][0]) if 'YY' in df_gl_header.columns else 0
mm = int(df_gl_header['MM'][0]) if 'MM' in df_gl_header.columns else 0
dd = int(df_gl_header['DD'][0]) if 'DD' in df_gl_header.columns else 0

if yy == 0 or mm == 0 or dd == 0:
    print(f"WARNING: Could not parse YY/MM/DD from text file. Using yesterday's date.")
    gl_date = reptdate
else:
    gl_date = datetime(yy, mm, dd)

gl = gl_date.strftime('%d%m%y')

if gl != rdate:
    print(f"THE GLFILE EXTRACTION IS NOT DATED {rdate} (found {gl})")
    sys.exit(77)

# Process BALANCE column
if 'BALANCE' in df_gl.columns and 'SIGN' in df_gl.columns:
    df_gl = df_gl.with_columns([
        pl.when(pl.col('SIGN') == '-')
          .then(pl.col('BALANCE') * -1)
          .otherwise(pl.col('BALANCE'))
          .alias('BALANCE')
    ])
else:
    print("WARNING: SIGN or BALANCE columns not found. Continuing without sign adjustment.")

def process_gl_data(df_gl, suffix):
    """Process GL data for a given suffix (P1 or P2)"""
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
        glrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
        glfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
        glrmfxp = glfilep.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
        glutrmp = glfilep.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
        glutfxp = glfilep.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
        
        # Save parquet files
        os.makedirs(STORE_DIR, exist_ok=True)
        glrmp.write_parquet(f'{STORE_DIR}GLRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
        glfxp.write_parquet(f'{STORE_DIR}GLFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
        glrmfxp.write_parquet(f'{STORE_DIR}GLRMFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
        glutrmp.write_parquet(f'{STORE_DIR}GLUTRM{suffix}{reptyear}{reptmon}{reptday}.parquet')
        glutfxp.write_parquet(f'{STORE_DIR}GLUTFX{suffix}{reptyear}{reptmon}{reptday}.parquet')
        
        # Save SAS datasets
        save_to_sas(glrmp, f'GLRM{suffix}{reptyear}{reptmon}{reptday}')
        save_to_sas(glfxp, f'GLFX{suffix}{reptyear}{reptmon}{reptday}')
        save_to_sas(glrmfxp, f'GLRMFX{suffix}{reptyear}{reptmon}{reptday}')
        save_to_sas(glutrmp, f'GLUTRM{suffix}{reptyear}{reptmon}{reptday}')
        save_to_sas(glutfxp, f'GLUTFX{suffix}{reptyear}{reptmon}{reptday}')
        
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
    
    # Process P1
    print("Processing GL P1...")
    results_p1 = process_gl_data(df_gl, 'P1')
    
    # Process P2
    print("\nProcessing GL P2...")
    results_p2 = process_gl_data(df_gl, 'P2')
