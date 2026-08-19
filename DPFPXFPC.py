def load_remit(d):
    """Load REMIT and UNCLAIM data
       DATA REMIT; SET DEPOSIT.REMIT UNCLAIM.UNCLAIM&REPTYEAR(RENAME=(LEDGBAL=UNCLAIMX)); RUN;"""
    try:
        # Load REMIT
        remit, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}remit.sas7bdat")
        remit = pl.DataFrame(remit)
        remit.columns = [col.lower() for col in remit.columns]
        
        # Load UNCLAIM with reptyear - following SAS logic exactly
        # UNCLAIM.UNCLAIM&REPTYEAR
        unclaim_file = f"{PATHS['UNCLAIM']}unclaim{d['reptyear']}.sas7bdat"
        
        # Check if the file with reptyear exists
        if not Path(unclaim_file).exists():
            print(f"Warning: UNCLAIM file not found at {unclaim_file}")
            # Try alternative locations
            alt_files = [
                f"{PATHS['UNCLAIM']}unclaim.sas7bdat",
                f"{PATHS['DEPOSIT']}unclaim{d['reptyear']}.sas7bdat",
                f"{PATHS['DEPOSIT']}unclaim.sas7bdat"
            ]
            
            unclaim_file = None
            for alt_file in alt_files:
                if Path(alt_file).exists():
                    unclaim_file = alt_file
                    print(f"  Using alternative UNCLAIM file: {alt_file}")
                    break
            
            if unclaim_file is None:
                print("Error: No UNCLAIM file found")
                return pl.DataFrame()
        
        unclaim, meta = pyreadstat.read_sas7bdat(unclaim_file)
        unclaim = pl.DataFrame(unclaim)
        unclaim.columns = [col.lower() for col in unclaim.columns]
        
        # RENAME=(LEDGBAL=UNCLAIMX)
        if 'ledgbal' in unclaim.columns:
            unclaim = unclaim.rename({'ledgbal': 'unclaimx'})
        
        # Ensure both have consistent types for concatenation
        if 'paymode' in remit.columns:
            remit = remit.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip_chars())
        if 'paymode' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip_chars())
        
        if 'ledgbal' in remit.columns:
            remit = remit.with_columns(pl.col('ledgbal').cast(pl.Float64, strict=False).fill_null(0))
        else:
            remit = remit.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        if 'unclaimx' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('unclaimx').cast(pl.Float64, strict=False).fill_null(0))
        else:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        # Add unclaimx to remit if not exists
        if 'unclaimx' not in remit.columns:
            remit = remit.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        # Add ledgbal to unclaim if not exists
        if 'ledgbal' not in unclaim.columns:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        # Select common columns and concatenate
        common_cols = ['paymode', 'ledgbal', 'unclaimx']
        remit_subset = remit.select(common_cols)
        unclaim_subset = unclaim.select(common_cols)
        combined = pl.concat([remit_subset, unclaim_subset])
        
        # PROC SUMMARY DATA=REMIT NWAY; CLASS PAYMODE; VAR LEDGBAL UNCLAIMX; OUTPUT SUM=PLUSBAL UNCLAIM
        summary = combined.group_by('paymode').agg([
            pl.col('ledgbal').sum().alias('plusbal'),
            pl.col('unclaimx').sum().alias('unclaim')
        ])
        
        # PROC SORT DATA=DEPOSIT.REMIT OUT=REMITORI NODUPKEYS; BY PAYMODE
        remitori = remit.unique(subset=['paymode'])
        
        # DATA REMIT; MERGE REMIT REMITORI; BY PAYMODE
        result = summary.join(remitori, on='paymode', how='left')
        
        # DATA REMIT; SET REMIT; ACCTNO = PAYMODE; DROP PAYMODE LEDGBAL UNCLAIMX
        result = result.with_columns(pl.col('paymode').alias('acctno'))
        
        # Standardize acctno
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        # Keep only needed columns
        keep_cols = ['acctno', 'plusbal', 'unclaim']
        for col in keep_cols:
            if col not in result.columns:
                result = result.with_columns(pl.lit(0.0).alias(col))
        
        return result.select(keep_cols)
    except Exception as e:
        print(f"Error loading REMIT/UNCLAIM: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()
