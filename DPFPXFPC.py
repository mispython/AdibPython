def load_saca_client():
    """Load SA/CA/FD data for CLIENT processing (WITHOUT PURPOSE filter)
       DATA SASA; SET SACA.SAVING DEP.UMA; RUN;
       DATA FD; SET SACA.FD (KEEP=ACCTNO BRANCH PRODUCT PURPOSE CURBAL INTPAYBL CURCODE FORATE); RUN;
       DATA CURRENT; SET SACA.CURRENT; RUN;
       DATA CA; SET CURRENT (KEEP=ACCTNO BRANCH PRODUCT PURPOSE CURBAL INTPAYBL CURCODE FORATE); RUN;
       DATA DEPOSIT; SET SA CA FD; IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01); RUN;"""
    dfs = []
    
    # Load SAVING (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}saving.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # KEEP ACCTNO BRANCH PRODUCT PURPOSE CURBAL INTPAYBL CURCODE FORATE NAME
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading SAVING (client): {e}")
        import traceback
        traceback.print_exc()
    
    # Load FD (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # KEEP ACCTNO BRANCH PRODUCT PURPOSE CURBAL INTPAYBL CURCODE FORATE NAME
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading FD (client): {e}")
        import traceback
        traceback.print_exc()
    
    # Load CURRENT (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # KEEP ACCTNO BRANCH PRODUCT PURPOSE CURBAL INTPAYBL CURCODE FORATE NAME
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading CURRENT (client): {e}")
        import traceback
        traceback.print_exc()
    
    if dfs:
        result = pl.concat(dfs)
        
        # Cast numeric columns
        result = result.with_columns([
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if 'curcode' in result.columns and 'forate' in result.columns:
            result = result.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        # PROC SORT DATA=DEPOSIT NODUPKEYS; BY ACCTNO;
        result = result.unique(subset=['acctno'])
        
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        return result
    else:
        return pl.DataFrame()
