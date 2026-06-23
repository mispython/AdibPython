import polars as pl
import duckdb
from pathlib import Path

# Configuration
imni_path = Path("IMNI")
pidms_path = Path("PIDMS")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# PROC SORT DATA=IMNI.FDMTHLY OUT=FDMTHLY;
try:
    fdmthly_df = pl.read_parquet(imni_path / "FDMTHLY.parquet").select([
        'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
    ]).sort('ACCTNO')
    fdmthly_df.write_parquet(output_path / "FDMTHLY_SORTED.parquet")
    print(f"FDMTHLY records: {fdmthly_df.height}")
except FileNotFoundError:
    print("NOTE: IMNI.FDMTHLY not found")
    fdmthly_df = pl.DataFrame()

# DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
if not fdmthly_df.is_empty():
    fdmthly_processed = fdmthly_df.with_columns([
        pl.col('CURBAL').alias('LEDGBAL')
    ])
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
else:
    fdmthly_processed = pl.DataFrame()

# DATA CURN; SET IMNI.CURN124;
try:
    curn_df = pl.read_parquet(imni_path / "CURN124.parquet")
    # IF PRODUCT = 139 THEN DELETE;
    curn_filtered = curn_df.filter(pl.col('PRODUCT') != 139)
    curn_filtered.write_parquet(output_path / "CURN.parquet")
    print(f"CURN records: {curn_filtered.height}")
except FileNotFoundError:
    print("NOTE: IMNI.CURN124 not found")
    curn_filtered = pl.DataFrame()

# DATA DEPOSIT; SET multiple datasets;
datasets_to_combine = []

# IMNI.SAVG124
try:
    savg_df = pl.read_parquet(imni_path / "SAVG124.parquet").select([
        'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
    ])
    datasets_to_combine.append(savg_df)
    print(f"SAVG124 records: {savg_df.height}")
except FileNotFoundError:
    print("NOTE: IMNI.SAVG124 not found")

# CURN
if not curn_filtered.is_empty():
    curn_selected = curn_filtered.select([
        'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
    ])
    datasets_to_combine.append(curn_selected)

# FDMTHLY with renames
if not fdmthly_processed.is_empty():
    fdmthly_renamed = fdmthly_processed.select([
        'ACCTNO', 'BRANCH', 'CURBAL', 'LEDGBAL', 'AMTIND'
    ]).with_columns([
        pl.col('INTPLAN').alias('PRODUCT'),
        pl.col('BIC').alias('PRODCD'),
        pl.col('INTPAY').alias('INTPAYBL')
    ])
    datasets_to_combine.append(fdmthly_renamed)

# Combine all datasets and apply filters
if datasets_to_combine:
    deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
    
    # Apply filters and transformations
    valid_prodcd = [
        '42110', '42310', '42120', '42320', '42130', '42610',
        '42133', '42132', '42180', '42199', '42699'
    ]
    
    deposit_filtered = deposit_combined.filter(
        pl.col('PRODCD').is_in(valid_prodcd)
    ).with_columns([
        # IF PRODUCT = 166 THEN PRODCD = '42310';
        pl.when(pl.col('PRODUCT') == 166)
        .then(pl.lit('42310'))
        .otherwise(pl.col('PRODCD'))
        .alias('PRODCD')
    ]).filter(
        # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
        ~(
            pl.col('PRODCD').is_in(['42199', '42699']) & 
            ~pl.col('PRODUCT').is_in([72, 413])
        )
    ).with_columns([
        # IF INTPAYBL < 0 THEN INTPAYBL = 0;
        pl.when(pl.col('INTPAYBL') < 0)
        .then(0)
        .otherwise(pl.col('INTPAYBL'))
        .alias('INTPAYBL')
    ])
    
    deposit_filtered.write_parquet(output_path / "DEPOSIT.parquet")
    print(f"DEPOSIT records after filtering: {deposit_filtered.height}")
else:
    deposit_filtered = pl.DataFrame()
    print("No DEPOSIT data created")

# DATA FLOAT; SET PIDMS.FLOAT;
try:
    float_df = pl.read_parquet(pidms_path / "FLOAT.parquet")
    float_df.write_parquet(output_path / "FLOAT.parquet")
    print(f"FLOAT records: {float_df.height}")
except FileNotFoundError:
    print("NOTE: PIDMS.FLOAT not found")
    float_df = pl.DataFrame()

# PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
if not float_df.is_empty():
    float_summary = float_df.group_by('ACCTNO').agg([
        pl.col('FLOAT').sum().alias('FLOAT')
    ])
    float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
    print(f"FLOAT summary records: {float_summary.height}")
else:
    float_summary = pl.DataFrame()

# PROC SORT DATA=DEPOSIT; BY ACCTNO;
if not deposit_filtered.is_empty():
    deposit_sorted = deposit_filtered.sort('ACCTNO')
    
    # DATA DEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
    if not float_summary.is_empty():
        deposit_merged = deposit_sorted.join(
            float_summary, on='ACCTNO', how='outer', suffix='_float'
        )
        
        # Apply transformations
        deposit_processed = deposit_merged.with_columns([
            # IF CURBAL < 0 THEN CURBAL = 0;
            pl.when(pl.col('CURBAL') < 0)
            .then(0)
            .otherwise(pl.col('CURBAL'))
            .alias('CURBAL'),
            
            # FLOATORI = CURBAL;
            pl.col('CURBAL').alias('FLOATORI'),
            
            # AVBAL = SUM(CURBAL,(-1)*FLOAT);
            (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('AVBAL'),
            
            # MINUSFLOAT = SUM(CURBAL,(-1)*FLOAT);
            (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('MINUSFLOAT')
        ]).with_columns([
            # IF AVBAL < 0 THEN DO; FLOAT = CURBAL; AVBAL = 0; END;
            pl.when(pl.col('AVBAL') < 0)
            .then(pl.struct([
                pl.col('CURBAL').alias('FLOAT'),
                pl.lit(0).alias('AVBAL')
            ]))
            .otherwise(pl.struct([
                pl.col('FLOAT').alias('FLOAT'),
                pl.col('AVBAL').alias('AVBAL')
            ]))
            .alias('adjustment')
        ]).with_columns([
            pl.col('adjustment').struct.field('FLOAT').alias('FLOAT'),
            pl.col('adjustment').struct.field('AVBAL').alias('AVBAL'),
            
            # AVBALTT = SUM(AVBAL,INTPAYBL);
            (pl.col('AVBAL') + pl.col('INTPAYBL')).alias('AVBALTT'),
            
            # CURBALTT = SUM(CURBAL,INTPAYBL);
            (pl.col('CURBAL') + pl.col('INTPAYBL')).alias('CURBALTT')
        ]).drop('adjustment')
        
        # Split into DEPOSIT and EXCEPT based on conditions
        # IF B AND NOT A THEN OUTPUT EXCEPT;
        except_df = deposit_processed.filter(
            pl.col('FLOAT').is_not_null() & 
            (pl.col('CURBAL').is_null() | pl.col('PRODUCT').is_null())
        )
        
        # IF A AND B THEN OUTPUT DEPOSIT;
        deposit_final = deposit_processed.filter(
            pl.col('CURBAL').is_not_null() & 
            pl.col('PRODUCT').is_not_null() & 
            pl.col('FLOAT').is_not_null()
        )
        
        # Save outputs
        deposit_final.write_parquet(output_path / "DEPOSIT_FINAL.parquet")
        except_df.write_parquet(output_path / "EXCEPT.parquet")
        
        print(f"DEPOSIT final records: {deposit_final.height}")
        print(f"EXCEPT records: {except_df.height}")
        
        # PROC SUMMARY DATA=DEPOSIT NWAY MISSING; CLASS BRANCH;
        if not deposit_final.is_empty():
            summary = deposit_final.group_by('BRANCH').agg([
                pl.col('FLOAT').sum().alias('FLOAT'),
                pl.col('MINUSFLOAT').sum().alias('MINUSFLOAT'),
                pl.col('FLOATORI').sum().alias('FLOATORI')
            ])
            
            summary.write_parquet(output_path / "XXX.parquet")
            
            # PROC PRINT DATA=XXX; SUM FLOAT MINUSFLOAT FLOATORI;
            print("\n" + "="*80)
            print("SUMMARY BY BRANCH (ISLAMIC)")
            print("="*80)
            print(summary)
            
            total_float = summary.select(pl.col('FLOAT').sum()).row(0)[0]
            total_minusfloat = summary.select(pl.col('MINUSFLOAT').sum()).row(0)[0]
            total_floatori = summary.select(pl.col('FLOATORI').sum()).row(0)[0]
            
            print(f"\nTOTALS:")
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

print("\nPROCESSING COMPLETED SUCCESSFULLY")
