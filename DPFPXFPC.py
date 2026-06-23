import polars as pl
import pyreadstat
from pathlib import Path
import datetime

# Configuration
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDMS")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPCIFLO")
output_path.mkdir(exist_ok=True)

def read_sas_dataset(filepath, columns=None):
    """
    Read a SAS7BDAT dataset and return as Polars DataFrame
    """
    try:
        if columns:
            df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        pl_df = pl.DataFrame(df)
        
        # Convert column names to lowercase
        pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
        
        # Convert date columns if they exist (SAS dates are days since 1960-01-01)
        for col in pl_df.columns:
            if 'date' in col.lower() or 'dt' in col.lower():
                try:
                    # Check if it's a numeric column that might be a date
                    if pl_df[col].dtype in [pl.Int64, pl.Float64]:
                        # SAS dates are days since 1960-01-01
                        pl_df = pl_df.with_columns([
                            pl.when(pl.col(col) > 0)
                            .then(pl.lit(datetime.date(1960, 1, 1)) + pl.duration(days=pl.col(col).cast(pl.Int64)))
                            .otherwise(pl.col(col))
                            .alias(col)
                        ])
                except:
                    pass  # Keep as is if conversion fails
        
        return pl_df, meta
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, None

# PROC SORT DATA=IMNI.FDMTHLY OUT=FDMTHLY;
try:
    fdmthly_df, meta = read_sas_dataset(imni_path / "fdmthly.sas7bdat", 
                                         columns=['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
    if fdmthly_df is not None:
        print(f"Loaded FDMTHLY with {len(fdmthly_df)} records")
        fdmthly_sorted = fdmthly_df.sort('acctno')
        fdmthly_sorted.write_parquet(output_path / "FDMTHLY_SORTED.parquet")
        fdmthly_sorted.write_csv(output_path / "FDMTHLY_SORTED.txt")
        print(f"Saved FDMTHLY_SORTED with {len(fdmthly_sorted)} records")
    else:
        fdmthly_df = pl.DataFrame()
        print("NOTE: IMNI.FDMTHLY could not be loaded")
except FileNotFoundError:
    print("NOTE: IMNI.FDMTHLY not found")
    fdmthly_df = pl.DataFrame()
except Exception as e:
    print(f"Error loading FDMTHLY: {e}")
    fdmthly_df = pl.DataFrame()

# DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
if not fdmthly_df.is_empty():
    fdmthly_processed = fdmthly_df.with_columns([
        pl.col('curbal').alias('ledgbal')
    ])
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
    fdmthly_processed.write_csv(output_path / "FDMTHLY.txt")
    print(f"Saved FDMTHLY with {len(fdmthly_processed)} records")
else:
    fdmthly_processed = pl.DataFrame()

# DATA CURN; SET IMNI.CURN124;
try:
    curn_df, meta = read_sas_dataset(imni_path / "curn124.sas7bdat")
    if curn_df is not None:
        print(f"Loaded CURN124 with {len(curn_df)} records")
        # IF PRODUCT = 139 THEN DELETE;
        curn_filtered = curn_df.filter(pl.col('product') != 139)
        curn_filtered.write_parquet(output_path / "CURN.parquet")
        curn_filtered.write_csv(output_path / "CURN.txt")
        print(f"Saved CURN with {len(curn_filtered)} records (after filtering)")
    else:
        curn_filtered = pl.DataFrame()
        print("NOTE: IMNI.CURN124 could not be loaded")
except FileNotFoundError:
    print("NOTE: IMNI.CURN124 not found")
    curn_filtered = pl.DataFrame()
except Exception as e:
    print(f"Error loading CURN124: {e}")
    curn_filtered = pl.DataFrame()

# DATA DEPOSIT; SET multiple datasets;
datasets_to_combine = []

# IMNI.SAVG124
try:
    savg_df, meta = read_sas_dataset(imni_path / "savg124.sas7bdat", 
                                      columns=['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if savg_df is not None:
        print(f"Loaded SAVG124 with {len(savg_df)} records")
        datasets_to_combine.append(savg_df)
    else:
        print("NOTE: IMNI.SAVG124 could not be loaded")
except FileNotFoundError:
    print("NOTE: IMNI.SAVG124 not found")
except Exception as e:
    print(f"Error loading SAVG124: {e}")

# CURN
if not curn_filtered.is_empty():
    curn_selected = curn_filtered.select([
        'acctno', 'product', 'curbal', 'ledgbal', 'prodcd', 'amtind', 'intpaybl', 'branch'
    ])
    datasets_to_combine.append(curn_selected)
    print(f"Added CURN with {len(curn_selected)} records")

# FDMTHLY with renames
if not fdmthly_processed.is_empty():
    # First, make sure we have all the columns we need
    fdmthly_renamed = fdmthly_processed.with_columns([
        pl.col('intplan').alias('product'),
        pl.col('bic').alias('prodcd'),
        pl.col('intpay').alias('intpaybl')
    ]).select([
        'acctno', 'branch', 'curbal', 'ledgbal', 'amtind',
        'product', 'prodcd', 'intpaybl'
    ])
    datasets_to_combine.append(fdmthly_renamed)
    print(f"Added FDMTHLY with {len(fdmthly_renamed)} records")

# Combine all datasets and apply filters
if datasets_to_combine:
    deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
    print(f"Combined DEPOSIT dataset has {len(deposit_combined)} records")
    
    # Apply filters and transformations (Islamic version has slightly different PRODCD list)
    valid_prodcd = [
        '42110', '42310', '42120', '42320', '42130', '42610',
        '42133', '42132', '42180', '42199', '42699'
    ]
    
    deposit_filtered = deposit_combined.filter(
        pl.col('prodcd').is_in(valid_prodcd)
    ).with_columns([
        # IF PRODUCT = 166 THEN PRODCD = '42310';
        pl.when(pl.col('product') == 166)
        .then(pl.lit('42310'))
        .otherwise(pl.col('prodcd'))
        .alias('prodcd')
    ]).filter(
        # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
        ~(
            pl.col('prodcd').is_in(['42199', '42699']) & 
            ~pl.col('product').is_in([72, 413])
        )
    ).with_columns([
        # IF INTPAYBL < 0 THEN INTPAYBL = 0;
        pl.when(pl.col('intpaybl') < 0)
        .then(0)
        .otherwise(pl.col('intpaybl'))
        .alias('intpaybl')
    ])
    
    deposit_filtered.write_parquet(output_path / "DEPOSIT.parquet")
    deposit_filtered.write_csv(output_path / "DEPOSIT.txt")
    print(f"DEPOSIT records after filtering: {len(deposit_filtered)}")
else:
    deposit_filtered = pl.DataFrame()
    print("No DEPOSIT data created")

# DATA FLOAT; SET PIDMS.FLOAT;
try:
    float_df, meta = read_sas_dataset(pidms_path / "float.sas7bdat")
    if float_df is not None:
        print(f"Loaded FLOAT with {len(float_df)} records")
        float_df.write_parquet(output_path / "FLOAT.parquet")
        float_df.write_csv(output_path / "FLOAT.txt")
        print(f"Saved FLOAT with {len(float_df)} records")
    else:
        float_df = pl.DataFrame()
        print("NOTE: PIDMS.FLOAT could not be loaded")
except FileNotFoundError:
    print("NOTE: PIDMS.FLOAT not found")
    float_df = pl.DataFrame()
except Exception as e:
    print(f"Error loading FLOAT: {e}")
    float_df = pl.DataFrame()

# PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
if not float_df.is_empty():
    float_summary = float_df.group_by('acctno').agg([
        pl.col('float').sum().alias('float')
    ])
    float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
    float_summary.write_csv(output_path / "FLOAT_SUMMARY.txt")
    print(f"FLOAT summary records: {len(float_summary)}")
else:
    float_summary = pl.DataFrame()
    print("No FLOAT data for summary")

# PROC SORT DATA=DEPOSIT; BY ACCTNO;
if not deposit_filtered.is_empty():
    deposit_sorted = deposit_filtered.sort('acctno')
    
    # DATA DEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
    if not float_summary.is_empty():
        # Use 'full' instead of 'outer' to avoid deprecation warning
        deposit_merged = deposit_sorted.join(
            float_summary, on='acctno', how='full', suffix='_float'
        )
        print(f"Merged DEPOSIT with FLOAT: {len(deposit_merged)} records")
        
        # Apply transformations
        deposit_processed = deposit_merged.with_columns([
            # IF CURBAL < 0 THEN CURBAL = 0;
            pl.when(pl.col('curbal') < 0)
            .then(0)
            .otherwise(pl.col('curbal'))
            .alias('curbal'),
            
            # FLOATORI = CURBAL;
            pl.col('curbal').alias('floatori'),
            
            # AVBAL = SUM(CURBAL,(-1)*FLOAT);
            (pl.col('curbal') + (-1) * pl.col('float')).alias('avbal'),
            
            # MINUSFLOAT = SUM(CURBAL,(-1)*FLOAT);
            (pl.col('curbal') + (-1) * pl.col('float')).alias('minusfloat')
        ]).with_columns([
            # IF AVBAL < 0 THEN DO; FLOAT = CURBAL; AVBAL = 0; END;
            pl.when(pl.col('avbal') < 0)
            .then(pl.struct([
                pl.col('curbal').alias('float'),
                pl.lit(0).alias('avbal')
            ]))
            .otherwise(pl.struct([
                pl.col('float').alias('float'),
                pl.col('avbal').alias('avbal')
            ]))
            .alias('adjustment')
        ]).with_columns([
            pl.col('adjustment').struct.field('float').alias('float'),
            pl.col('adjustment').struct.field('avbal').alias('avbal'),
            
            # AVBALTT = SUM(AVBAL,INTPAYBL);
            (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
            
            # CURBALTT = SUM(CURBAL,INTPAYBL);
            (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
        ]).drop('adjustment')
        
        # Split into DEPOSIT and EXCEPT based on conditions
        # IF B AND NOT A THEN OUTPUT EXCEPT;
        except_df = deposit_processed.filter(
            pl.col('float').is_not_null() & 
            (pl.col('curbal').is_null() | pl.col('product').is_null())
        )
        
        # IF A AND B THEN OUTPUT DEPOSIT;
        deposit_final = deposit_processed.filter(
            pl.col('curbal').is_not_null() & 
            pl.col('product').is_not_null() & 
            pl.col('float').is_not_null()
        )
        
        # Save outputs
        deposit_final.write_parquet(output_path / "DEPOSIT_FINAL.parquet")
        deposit_final.write_csv(output_path / "DEPOSIT_FINAL.txt")
        
        except_df.write_parquet(output_path / "EXCEPT.parquet")
        except_df.write_csv(output_path / "EXCEPT.txt")
        
        print(f"DEPOSIT final records: {len(deposit_final)}")
        print(f"EXCEPT records: {len(except_df)}")
        
        # PROC SUMMARY DATA=DEPOSIT NWAY MISSING; CLASS BRANCH;
        if not deposit_final.is_empty():
            summary = deposit_final.group_by('branch').agg([
                pl.col('float').sum().alias('float'),
                pl.col('minusfloat').sum().alias('minusfloat'),
                pl.col('floatori').sum().alias('floatori')
            ])
            
            summary.write_parquet(output_path / "XXX.parquet")
            summary.write_csv(output_path / "XXX.txt")
            
            # PROC PRINT DATA=XXX; SUM FLOAT MINUSFLOAT FLOATORI;
            # Format numbers to show normal decimal format (no scientific notation)
            print("\n" + "="*80)
            print("SUMMARY BY BRANCH (ISLAMIC)")
            print("="*80)
            
            # Create a formatted version for display
            summary_display = summary.with_columns([
                pl.col('float').map_elements(lambda x: f"{x:,.2f}", return_dtype=pl.Utf8).alias('float_formatted'),
                pl.col('minusfloat').map_elements(lambda x: f"{x:,.2f}", return_dtype=pl.Utf8).alias('minusfloat_formatted'),
                pl.col('floatori').map_elements(lambda x: f"{x:,.2f}", return_dtype=pl.Utf8).alias('floatori_formatted')
            ]).select(['branch', 'float_formatted', 'minusfloat_formatted', 'floatori_formatted'])
            
            print(summary_display.sort('branch'))
            
            total_float = summary.select(pl.col('float').sum()).row(0)[0]
            total_minusfloat = summary.select(pl.col('minusfloat').sum()).row(0)[0]
            total_floatori = summary.select(pl.col('floatori').sum()).row(0)[0]
            
            print("\n" + "="*80)
            print("TOTALS:")
            print("="*80)
            print(f"FLOAT: {total_float:,.2f}")
            print(f"MINUSFLOAT: {total_minusfloat:,.2f}")
            print(f"FLOATORI: {total_floatori:,.2f}")
            
            # Save summary to CSV for reporting
            summary.write_csv(output_path / "ISLAMIC_FLOAT_SUMMARY.csv")
            
        else:
            print("No DEPOSIT data for summary")
            
    else:
        print("No FLOAT data for merging")
        
else:
    print("No DEPOSIT data for processing")

print("\n" + "="*80)
print(f"All output files saved to: {output_path}")
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
