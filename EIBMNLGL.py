import polars as pl
from datetime import datetime
import sys

DEPOSIT_REPTDATE = 'data/deposit/reptdate.parquet'
GLFILE = 'data/glfile.parquet'
STORE_DIR = 'data/store/'

df_reptdate = pl.read_parquet(DEPOSIT_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

df_glfile_header = pl.read_parquet(GLFILE).head(1)
yy = int(df_glfile_header['YY'][0])
mm = int(df_glfile_header['MM'][0])
dd = int(df_glfile_header['DD'][0])
gl_date = datetime(yy, mm, dd)
gl = gl_date.strftime('%d%m%y')

if gl != rdate:
    print(f"THE GLIFLE EXTRACTION IS NOT DATED {rdate}")
    sys.exit(77)

df_gl = pl.read_parquet(GLFILE)

df_gl = df_gl.with_columns([
    pl.col('DATE').alias('DATE'),
    pl.when(pl.col('SIGN') == '-')
      .then(pl.col('BALANCE') * -1)
      .otherwise(pl.col('BALANCE'))
      .alias('BALANCE')
])

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

rows_p1 = []
for condition, item, week, month, qtr, halfyr, year, last, total in conditions_p1:
    filtered = df_gl.filter(condition)
    if len(filtered) > 0:
        rows_p1.append(
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

glfilep1 = pl.concat(rows_p1) if rows_p1 else pl.DataFrame()

if len(glfilep1) > 0:
    glfilep1 = glfilep1.with_columns([
        (pl.col('WEEK').fill_null(0) + 
         pl.col('MONTH').fill_null(0) + 
         pl.col('QTR').fill_null(0) + 
         pl.col('HALFYR').fill_null(0) + 
         pl.col('YEAR').fill_null(0) + 
         pl.col('LAST').fill_null(0) + 
         pl.col('TOTAL').fill_null(0)).alias('BALANCE')
    ])
    
    glfilep1 = glfilep1.filter(pl.col('ITEM').is_not_null() & (pl.col('ITEM') != ''))
    
    glfilep1 = glfilep1.group_by('ITEM').agg([
        pl.col('WEEK').sum().alias('WEEK'),
        pl.col('MONTH').sum().alias('MONTH'),
        pl.col('QTR').sum().alias('QTR'),
        pl.col('HALFYR').sum().alias('HALFYR'),
        pl.col('YEAR').sum().alias('YEAR'),
        pl.col('LAST').sum().alias('LAST'),
        pl.col('BALANCE').sum().alias('BALANCE')
    ])
    
    glfilep1 = glfilep1.with_columns([
        (pl.col('WEEK').round(0) / 1000).round(3).alias('WEEK'),
        (pl.col('MONTH').round(0) / 1000).round(3).alias('MONTH'),
        (pl.col('QTR').round(0) / 1000).round(3).alias('QTR'),
        (pl.col('HALFYR').round(0) / 1000).round(3).alias('HALFYR'),
        (pl.col('YEAR').round(0) / 1000).round(3).alias('YEAR'),
        (pl.col('LAST').round(0) / 1000).round(3).alias('LAST'),
        (pl.col('BALANCE').round(0) / 1000).round(3).alias('BALANCE')
    ])
    
    glrmp1 = glfilep1.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
    glfxp1 = glfilep1.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glrmfxp1 = glfilep1.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glutrmp1 = glfilep1.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    glutfxp1 = glfilep1.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    
    glrmp1.write_parquet(f'{STORE_DIR}GLRMP1{reptyear}{reptmon}{reptday}.parquet')
    glfxp1.write_parquet(f'{STORE_DIR}GLFXP1{reptyear}{reptmon}{reptday}.parquet')
    glrmfxp1.write_parquet(f'{STORE_DIR}GLRMFXP1{reptyear}{reptmon}{reptday}.parquet')
    glutrmp1.write_parquet(f'{STORE_DIR}GLUTRMP1{reptyear}{reptmon}{reptday}.parquet')
    glutfxp1.write_parquet(f'{STORE_DIR}GLUTFXP1{reptyear}{reptmon}{reptday}.parquet')
    
    print(f"\nGLRMP1{reptyear}{reptmon}{reptday}:")
    print(glrmp1)
    
    print(f"\nGLFXP1{reptyear}{reptmon}{reptday}:")
    print(glfxp1)
    
    print(f"\nGLRMFXP1{reptyear}{reptmon}{reptday}:")
    print(glrmfxp1)
    
    print(f"\nGLUTRMP1{reptyear}{reptmon}{reptday}:")
    print(glutrmp1)
    
    print(f"\nGLUTFXP1{reptyear}{reptmon}{reptday}:")
    print(glutfxp1)

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

rows_p2 = []
for condition, item, week, month, qtr, halfyr, year, last, total in conditions_p2:
    filtered = df_gl.filter(condition)
    if len(filtered) > 0:
        rows_p2.append(
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

glfilep2 = pl.concat(rows_p2) if rows_p2 else pl.DataFrame()

if len(glfilep2) > 0:
    glfilep2 = glfilep2.with_columns([
        (pl.col('WEEK').fill_null(0) + 
         pl.col('MONTH').fill_null(0) + 
         pl.col('QTR').fill_null(0) + 
         pl.col('HALFYR').fill_null(0) + 
         pl.col('YEAR').fill_null(0) + 
         pl.col('LAST').fill_null(0) + 
         pl.col('TOTAL').fill_null(0)).alias('BALANCE')
    ])
    
    glfilep2 = glfilep2.filter(pl.col('ITEM').is_not_null() & (pl.col('ITEM') != ''))
    
    glfilep2 = glfilep2.group_by('ITEM').agg([
        pl.col('WEEK').sum().alias('WEEK'),
        pl.col('MONTH').sum().alias('MONTH'),
        pl.col('QTR').sum().alias('QTR'),
        pl.col('HALFYR').sum().alias('HALFYR'),
        pl.col('YEAR').sum().alias('YEAR'),
        pl.col('LAST').sum().alias('LAST'),
        pl.col('BALANCE').sum().alias('BALANCE')
    ])
    
    glfilep2 = glfilep2.with_columns([
        (pl.col('WEEK').round(0) / 1000).round(3).alias('WEEK'),
        (pl.col('MONTH').round(0) / 1000).round(3).alias('MONTH'),
        (pl.col('QTR').round(0) / 1000).round(3).alias('QTR'),
        (pl.col('HALFYR').round(0) / 1000).round(3).alias('HALFYR'),
        (pl.col('YEAR').round(0) / 1000).round(3).alias('YEAR'),
        (pl.col('LAST').round(0) / 1000).round(3).alias('LAST'),
        (pl.col('BALANCE').round(0) / 1000).round(3).alias('BALANCE')
    ])
    
    glrmp2 = glfilep2.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('1'))
    glfxp2 = glfilep2.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('1') & ~pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glrmfxp2 = glfilep2.filter(pl.col('ITEM').is_in(['B1.12', 'B1.14']))
    glutrmp2 = glfilep2.filter(pl.col('ITEM').str.starts_with('A') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    glutfxp2 = glfilep2.filter(pl.col('ITEM').str.starts_with('B') & pl.col('ITEM').str.slice(1, 1).eq('2'))
    
    glrmp2.write_parquet(f'{STORE_DIR}GLRMP2{reptyear}{reptmon}{reptday}.parquet')
    glfxp2.write_parquet(f'{STORE_DIR}GLFXP2{reptyear}{reptmon}{reptday}.parquet')
    glrmfxp2.write_parquet(f'{STORE_DIR}GLRMFXP2{reptyear}{reptmon}{reptday}.parquet')
    glutrmp2.write_parquet(f'{STORE_DIR}GLUTRMP2{reptyear}{reptmon}{reptday}.parquet')
    glutfxp2.write_parquet(f'{STORE_DIR}GLUTFXP2{reptyear}{reptmon}{reptday}.parquet')
    
    print(f"\nGLRMP2{reptyear}{reptmon}{reptday}:")
    print(glrmp2)
    
    print(f"\nGLFXP2{reptyear}{reptmon}{reptday}:")
    print(glfxp2)
    
    print(f"\nGLRMFXP2{reptyear}{reptmon}{reptday}:")
    print(glrmfxp2)
    
    print(f"\nGLUTRMP2{reptyear}{reptmon}{reptday}:")
    print(glutrmp2)
    
    print(f"\nGLUTFXP2{reptyear}{reptmon}{reptday}:")
    print(glutfxp2)
