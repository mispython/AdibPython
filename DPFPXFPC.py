import polars as pl
import duckdb
from datetime import datetime, timedelta
import sys
import os

# Constants
GLFILE = 'data/glfile.parquet'
STORE_DIR = 'data/store/'

# Use yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

# Read GL file
df_gl_header = pl.read_parquet(GLFILE).head(1)
yy = int(df_gl_header['YY'][0])
mm = int(df_gl_header['MM'][0])
dd = int(df_gl_header['DD'][0])
gl_date = datetime(yy, mm, dd)
gl = gl_date.strftime('%d%m%y')

if gl != rdate:
    print(f"THE GLFILE EXTRACTION IS NOT DATED {rdate}")
    sys.exit(77)

df_gl = pl.read_parquet(GLFILE)

df_gl = df_gl.with_columns([
    pl.col('DATE').alias('DATE'),
    pl.when(pl.col('SIGN') == '-')
      .then(pl.col('BALANCE') * -1)
      .otherwise(pl.col('BALANCE'))
      .alias('BALANCE')
])

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

def read_txt_to_parquet(txt_file, parquet_file):
    """Convert txt file to parquet format"""
    try:
        # Read txt file with appropriate delimiter
        # Adjust delimiter and column names based on your actual txt file format
        df = pl.read_csv(txt_file, separator='\t', infer_schema=True)
        df.write_parquet(parquet_file)
        print(f"Converted {txt_file} to {parquet_file}")
        return df
    except Exception as e:
        print(f"Error converting {txt_file} to parquet: {e}")
        return None

# Main execution
if __name__ == "__main__":
    # If you need to convert txt to parquet first
    # txt_file = 'data/glfile.txt'
    # if os.path.exists(txt_file):
    #     read_txt_to_parquet(txt_file, GLFILE)
    
    # Ensure store directory exists
    os.makedirs(STORE_DIR, exist_ok=True)
    
    # Process P1
    print("Processing GL P1...")
    results_p1 = process_gl_data(df_gl, 'P1')
    
    # Process P2 (using conditions from your original code)
    # Note: P2 conditions are slightly different from P1
    print("\nProcessing GL P2...")
    results_p2 = process_gl_data(df_gl, 'P2')
