import polars as pl
import duckdb
from pathlib import Path

# Configuration
mni_path = Path("MNI")
imni_path = Path("IMNI")
pidms_path = Path("PIDMS")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# PROC SORT DATA=MNI.FDMTHLY OUT=FDMTHLY;
try:
    fdmthly_df = pl.read_parquet(mni_path / "FDMTHLY.parquet").select([
        'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
    ]).sort('ACCTNO')
except FileNotFoundError:
    fdmthly_df = pl.DataFrame()

# PROC SORT DATA=IMNI.FDMTHLY OUT=IFDMTHLY;
try:
    ifdmthly_df = pl.read_parquet(imni_path / "FDMTHLY.parquet").select([
        'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
    ]).sort('ACCTNO')
except FileNotFoundError:
    ifdmthly_df = pl.DataFrame()

# DATA FDMTHLY; SET FDMTHLY IFDMTHLY;
fdmthly_combined = pl.concat([fdmthly_df, ifdmthly_df], how="diagonal")

# LEDGBAL = CURBAL;
if not fdmthly_combined.is_empty():
    fdmthly_processed = fdmthly_combined.with_columns([
        pl.col('CURBAL').alias('LEDGBAL')
    ])
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
else:
    fdmthly_processed = pl.DataFrame()

# DATA CURN; SET MNI.CURN124 IMNI.CURN124;
try:
    curn1_df = pl.read_parquet(mni_path / "CURN124.parquet")
except FileNotFoundError:
    curn1_df = pl.DataFrame()

try:
    curn2_df = pl.read_parquet(imni_path / "CURN124.parquet")
except FileNotFoundError:
    curn2_df = pl.DataFrame()

curn_combined = pl.concat([curn1_df, curn2_df], how="diagonal")

# IF PRODUCT = 139 THEN DELETE;
if not curn_combined.is_empty():
    curn_filtered = curn_combined.filter(pl.col('PRODUCT') != 139)
    curn_filtered.write_parquet(output_path / "CURN.parquet")
else:
    curn_filtered = pl.DataFrame()

# DATA DEPOSIT; SET multiple datasets;
datasets_to_combine = []

# MNI.SAVG124
try:
    savg1_df = pl.read_parquet(mni_path / "SAVG124.parquet").select([
        'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
    ])
    datasets_to_combine.append(savg1_df)
except FileNotFoundError:
    pass

# IMNI.SAVG124
try:
    savg2_df = pl.read_parquet(imni_path / "SAVG124.parquet").select([
        'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
    ])
    datasets_to_combine.append(savg2_df)
except FileNotFoundError:
    pass

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

# Combine all datasets
if datasets_to_combine:
    deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
    
    # Apply filters and transformations
    valid_prodcd = [
        '42110', '42310', '42120', '42320', '42130', '42610',
        '42133', '42132', '42180', '42610', '42630', '34180',
        '42199', '42699'
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
    ).filter(
        # IF PRODUCT IN (30,31,32,33,34) THEN DELETE;
        ~pl.col('PRODUCT').is_in([30, 31, 32, 33, 34])
    ).with_columns([
        # IF INTPAYBL < 0 THEN INTPAYBL = 0;
        pl.when(pl.col('INTPAYBL') < 0)
        .then(0)
        .otherwise(pl.col('INTPAYBL'))
        .alias('INTPAYBL')
    ])
    
    deposit_filtered.write_parquet(output_path / "DEPOSIT.parquet")
else:
    deposit_filtered = pl.DataFrame()

# DATA FLOAT; SET PIDMS.FLOAT;
try:
    float_df = pl.read_parquet(pidms_path / "FLOAT.parquet")
    float_df.write_parquet(output_path / "FLOAT.parquet")
except FileNotFoundError:
    float_df = pl.DataFrame()

# PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
if not float_df.is_empty():
    float_summary = float_df.group_by('ACCTNO').agg([
        pl.col('FLOAT').sum().alias('FLOAT')
    ])
    float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
else:
    float_summary = pl.DataFrame()

# PROC SORT DATA=DEPOSIT; BY ACCTNO;
if not deposit_filtered.is_empty():
    deposit_sorted = deposit_filtered.sort('ACCTNO')
    
    # DATA DEPOSIT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
    if not float_summary.is_empty():
        deposit_merged = deposit_sorted.join(
            float_summary, on='ACCTNO', how='left', suffix='_float'
        ).with_columns([
            # IF CURBAL < 0 THEN CURBAL = 0;
            pl.when(pl.col('CURBAL') < 0)
            .then(0)
            .otherwise(pl.col('CURBAL'))
            .alias('CURBAL'),
            
            # AVBAL = SUM(CURBAL,(-1)*FLOAT);
            (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('AVBAL'),
            
            # AVBALTT = SUM(AVBAL,INTPAYBL);
            (pl.col('AVBAL') + pl.col('INTPAYBL')).alias('AVBALTT'),
            
            # CURBALTT = SUM(CURBAL,INTPAYBL);
            (pl.col('CURBAL') + pl.col('INTPAYBL')).alias('CURBALTT')
        ])
        
        # SAS commented logic:
        /*
        IF AVBAL < 0 THEN DO;
           FLOAT = CURBAL;
           AVBAL = 0;
        END;
        */
        
        # IF B AND NOT A; (keep records that are in FLOAT but not in DEPOSIT)
        float_only = deposit_merged.filter(
            pl.col('FLOAT').is_not_null() & 
            (pl.col('CURBAL').is_null() | pl.col('PRODUCT').is_null())
        )
        
        deposit_merged.write_parquet(output_path / "DEPOSIT_MERGED.parquet")
        float_only.write_parquet(output_path / "FLOAT_ONLY.parquet")
        
        # PROC PRINT DATA=DEPOSIT; SUM FLOAT;
        print("DEPOSIT DATA WITH FLOAT:")
        print(deposit_merged)
        total_float = deposit_merged.select(pl.col('FLOAT').sum()).row(0)[0]
        print(f"TOTAL FLOAT: {total_float:.2f}")
        
        print("\nFLOAT ONLY RECORDS (B AND NOT A):")
        print(float_only)
        
    else:
        print("No FLOAT data found for merging")
        deposit_merged = deposit_sorted
        
else:
    print("No DEPOSIT data found for processing")

print("PROCESSING COMPLETED SUCCESSFULLY")
